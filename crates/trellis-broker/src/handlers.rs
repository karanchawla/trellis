use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::{HashMap, HashSet, VecDeque};
use trellis_core::model::calculate_backoff;
use trellis_core::{FailurePolicy, Run, RunStatus, TaskStatus};
use trellis_protocol::{BrokerMessage, RejectionReason, WorkerMessage};
use uuid::Uuid;

pub fn apply_message(
    run: &mut Run,
    message: &WorkerMessage,
    now: DateTime<Utc>,
) -> (BrokerMessage, bool) {
    match message {
        WorkerMessage::Claim {
            worker_id,
            task_types,
        } => handle_claim(run, worker_id, task_types, now),
        WorkerMessage::Complete {
            task_id,
            lease_id,
            output_ref,
        } => handle_complete(run, task_id, lease_id, output_ref, now),
        WorkerMessage::Fail {
            task_id,
            lease_id,
            error,
            retryable,
        } => handle_fail(run, task_id, lease_id, error, *retryable, now),
        WorkerMessage::Heartbeat { task_id, lease_id } => {
            handle_heartbeat(run, task_id, lease_id, now)
        }
    }
}

pub fn timeout_scan(run: &mut Run, now: DateTime<Utc>) -> bool {
    let expired: Vec<String> = run
        .tasks
        .iter()
        .filter_map(|(task_id, task)| {
            (task.status == TaskStatus::Running
                && task
                    .lease_expires_at
                    .is_some_and(|lease_expires_at| lease_expires_at < now))
            .then_some(task_id.clone())
        })
        .collect();

    let mut changed = false;
    for task_id in expired {
        let lease_id = run
            .tasks
            .get(&task_id)
            .and_then(|task| task.lease_id.clone())
            .unwrap_or_default();
        let (response, mutated) = handle_fail(
            run,
            &task_id,
            &lease_id,
            "lease_timeout".to_string().as_str(),
            true,
            now,
        );
        if matches!(response, BrokerMessage::Ack) {
            changed |= mutated;
        }
    }

    changed
}

pub fn update_current_layer(run: &mut Run) -> bool {
    let next_layer = run
        .tasks
        .values()
        .filter(|task| !task.status.is_terminal())
        .map(|task| task.layer)
        .min()
        .unwrap_or(run.total_layers);

    if run.current_layer == next_layer {
        return false;
    }

    run.current_layer = next_layer;
    true
}

pub fn check_run_completion(run: &mut Run, now: DateTime<Utc>) -> bool {
    if run.status.is_terminal() {
        return false;
    }

    let all_terminal = run.tasks.values().all(|task| task.status.is_terminal());
    if !all_terminal {
        return false;
    }

    let any_failed = run
        .tasks
        .values()
        .any(|task| task.status == TaskStatus::Failed);
    let next = if any_failed {
        RunStatus::Failed
    } else {
        RunStatus::Completed
    };

    let changed = run.status != next || run.completed_at != Some(now);
    run.status = next;
    run.completed_at = Some(now);
    changed
}

fn handle_claim(
    run: &mut Run,
    worker_id: &str,
    task_types: &[String],
    now: DateTime<Utc>,
) -> (BrokerMessage, bool) {
    if run.status != RunStatus::Running {
        return (BrokerMessage::NoWork, false);
    }

    let requested: HashSet<&str> = task_types.iter().map(String::as_str).collect();
    let wildcard = requested.is_empty();

    let next = run
        .tasks
        .iter()
        .filter(|(_, task)| {
            task.status == TaskStatus::Pending
                && task.available_at <= now
                && (wildcard || requested.contains(task.task_type.as_str()))
        })
        .min_by(|(id_a, task_a), (id_b, task_b)| {
            (task_a.layer, task_a.available_at, id_a.as_str()).cmp(&(
                task_b.layer,
                task_b.available_at,
                id_b.as_str(),
            ))
        })
        .map(|(task_id, _)| task_id.clone());

    let Some(task_id) = next else {
        return (BrokerMessage::NoWork, false);
    };

    let task = run.tasks.get_mut(&task_id).expect("task selected from map");
    task.status = TaskStatus::Running;
    if task.started_at.is_none() {
        task.started_at = Some(now);
    }
    task.worker_id = Some(worker_id.to_string());
    let lease_id = Uuid::new_v4().to_string();
    task.lease_id = Some(lease_id.clone());
    task.lease_expires_at = Some(now + ChronoDuration::seconds(task.timeout_seconds as i64));
    task.attempt += 1;

    let input_refs = task
        .input_refs
        .iter()
        .filter_map(|(key, value)| value.as_ref().map(|value| (key.clone(), value.clone())))
        .collect::<HashMap<_, _>>();

    (
        BrokerMessage::Assigned {
            task_id,
            task_type: task.task_type.clone(),
            lease_id,
            timeout_seconds: task.timeout_seconds,
            input_refs,
        },
        true,
    )
}

fn handle_complete(
    run: &mut Run,
    task_id: &str,
    lease_id: &str,
    output_ref: &str,
    now: DateTime<Utc>,
) -> (BrokerMessage, bool) {
    let downstream = {
        let Some(task) = run.tasks.get_mut(task_id) else {
            return reject_lease();
        };

        if task.lease_id.as_deref() != Some(lease_id) || task.status != TaskStatus::Running {
            return reject_lease();
        }

        task.status = TaskStatus::Completed;
        task.output_ref = Some(output_ref.to_string());
        task.completed_at = Some(now);
        task.worker_id = None;
        task.lease_id = None;
        task.lease_expires_at = None;
        task.downstream.clone()
    };

    for child_id in downstream {
        let Some(child) = run.tasks.get_mut(&child_id) else {
            continue;
        };

        if child.status.is_terminal() {
            continue;
        }

        if child.deps_remaining > 0 {
            child.deps_remaining -= 1;
        }
        child
            .input_refs
            .insert(task_id.to_string(), Some(output_ref.to_string()));

        if run.status == RunStatus::Running
            && child.status == TaskStatus::Blocked
            && child.deps_remaining == 0
        {
            child.status = TaskStatus::Pending;
            child.available_at = now;
        }
    }

    (BrokerMessage::Ack, true)
}

fn handle_fail(
    run: &mut Run,
    task_id: &str,
    lease_id: &str,
    error: &str,
    retryable: bool,
    now: DateTime<Utc>,
) -> (BrokerMessage, bool) {
    let on_failure = {
        let Some(task) = run.tasks.get_mut(task_id) else {
            return reject_lease();
        };

        if task.lease_id.as_deref() != Some(lease_id) || task.status != TaskStatus::Running {
            return reject_lease();
        }

        task.last_error = Some(error.to_string());
        task.worker_id = None;
        task.lease_id = None;
        task.lease_expires_at = None;
        if retryable && task.retry_count < task.max_retries {
            task.retry_count += 1;
            task.status = TaskStatus::Pending;
            task.completed_at = None;
            task.available_at =
                now + to_chrono_duration(calculate_backoff(task.retry_count, &task.retry_policy));
            None
        } else {
            task.status = TaskStatus::Failed;
            task.completed_at = Some(now);
            Some(task.on_failure)
        }
    };

    if let Some(on_failure) = on_failure {
        apply_failure_policy(run, task_id, on_failure, now);
    }
    (BrokerMessage::Ack, true)
}

fn handle_heartbeat(
    run: &mut Run,
    task_id: &str,
    lease_id: &str,
    now: DateTime<Utc>,
) -> (BrokerMessage, bool) {
    let Some(task) = run.tasks.get_mut(task_id) else {
        return reject_lease();
    };

    if task.lease_id.as_deref() != Some(lease_id) || task.status != TaskStatus::Running {
        return reject_lease();
    }

    task.lease_expires_at = Some(now + ChronoDuration::seconds(task.timeout_seconds as i64));
    (BrokerMessage::Ack, true)
}

fn apply_failure_policy(
    run: &mut Run,
    failed_task_id: &str,
    policy: FailurePolicy,
    now: DateTime<Utc>,
) {
    match policy {
        FailurePolicy::FailDag => {
            run.status = RunStatus::Failed;
            run.completed_at = Some(now);
            for task in run.tasks.values_mut() {
                if matches!(task.status, TaskStatus::Blocked | TaskStatus::Pending) {
                    task.status = TaskStatus::Skipped;
                    task.completed_at = Some(now);
                }
            }
        }
        FailurePolicy::Continue => {
            propagate_unreachable(run, failed_task_id, now);
        }
        FailurePolicy::SkipDownstream => {
            let mut queue = VecDeque::new();
            if let Some(task) = run.tasks.get(failed_task_id) {
                for child_id in &task.downstream {
                    queue.push_back(child_id.clone());
                }
            }

            while let Some(child_id) = queue.pop_front() {
                let downstream = {
                    let Some(child) = run.tasks.get_mut(&child_id) else {
                        continue;
                    };

                    if matches!(child.status, TaskStatus::Blocked | TaskStatus::Pending) {
                        child.status = TaskStatus::Skipped;
                        child.completed_at = Some(now);
                        child.downstream.clone()
                    } else {
                        Vec::new()
                    }
                };

                for next in downstream {
                    queue.push_back(next);
                }
            }
        }
    }
}

fn propagate_unreachable(run: &mut Run, failed_task_id: &str, now: DateTime<Utc>) {
    let mut queue = VecDeque::new();
    if let Some(task) = run.tasks.get(failed_task_id) {
        for child in &task.downstream {
            queue.push_back(child.clone());
        }
    }

    while let Some(task_id) = queue.pop_front() {
        let should_skip = {
            let Some(task) = run.tasks.get(&task_id) else {
                continue;
            };

            if task.status != TaskStatus::Blocked {
                false
            } else {
                task.depends_on.iter().any(|parent_id| {
                    run.tasks.get(parent_id).is_some_and(|parent| {
                        matches!(parent.status, TaskStatus::Failed | TaskStatus::Skipped)
                    })
                })
            }
        };

        if !should_skip {
            continue;
        }

        let downstream = {
            let task = run
                .tasks
                .get_mut(&task_id)
                .expect("task exists after lookup");
            task.status = TaskStatus::Skipped;
            task.completed_at = Some(now);
            task.downstream.clone()
        };

        for child in downstream {
            queue.push_back(child);
        }
    }
}

fn reject_lease() -> (BrokerMessage, bool) {
    (
        BrokerMessage::Rejected {
            reason: RejectionReason::LeaseMismatch,
        },
        false,
    )
}

fn to_chrono_duration(duration: std::time::Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::zero())
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis_core::DagBuilder;

    fn single_task_run(max_retries: u32) -> Run {
        let mut dag = DagBuilder::new("single");
        dag.task("task")
            .task_type("echo")
            .max_retries(max_retries)
            .timeout_seconds(1);
        let mut run = dag.build().expect("build run");
        run.status = RunStatus::Running;
        run
    }

    #[test]
    fn retryable_fail_requeues_task_with_backoff() {
        let mut run = single_task_run(3);
        let now = Utc::now();
        let task = run.tasks.get_mut("task").expect("task");
        task.status = TaskStatus::Running;
        task.lease_id = Some("lease-1".to_string());
        task.worker_id = Some("w-1".to_string());
        task.lease_expires_at = Some(now + ChronoDuration::seconds(1));

        let (response, changed) = apply_message(
            &mut run,
            &WorkerMessage::Fail {
                task_id: "task".to_string(),
                lease_id: "lease-1".to_string(),
                error: "retry".to_string(),
                retryable: true,
            },
            now,
        );

        assert!(changed);
        assert_eq!(response, BrokerMessage::Ack);
        let task = run.tasks.get("task").expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.retry_count, 1);
        assert!(task.available_at > now);
    }

    #[test]
    fn retries_exhausted_mark_task_failed() {
        let mut run = single_task_run(0);
        let now = Utc::now();
        let task = run.tasks.get_mut("task").expect("task");
        task.status = TaskStatus::Running;
        task.lease_id = Some("lease-1".to_string());
        task.worker_id = Some("w-1".to_string());

        let (response, changed) = apply_message(
            &mut run,
            &WorkerMessage::Fail {
                task_id: "task".to_string(),
                lease_id: "lease-1".to_string(),
                error: "boom".to_string(),
                retryable: true,
            },
            now,
        );

        assert!(changed);
        assert_eq!(response, BrokerMessage::Ack);
        let task = run.tasks.get("task").expect("task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(task.retry_count, 0);
    }

    #[test]
    fn timeout_scan_requeues_expired_lease() {
        let mut run = single_task_run(2);
        run.status = RunStatus::Running;
        let now = Utc::now();
        let task = run.tasks.get_mut("task").expect("task");
        task.status = TaskStatus::Running;
        task.lease_id = Some("lease-1".to_string());
        task.worker_id = Some("w-1".to_string());
        task.lease_expires_at = Some(now - ChronoDuration::seconds(1));

        let changed = timeout_scan(&mut run, now);
        assert!(changed);
        let task = run.tasks.get("task").expect("task");
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.retry_count, 1);
    }

    #[test]
    fn stale_completion_is_rejected_after_retry_reassignment() {
        let mut run = single_task_run(2);
        run.status = RunStatus::Running;
        let now = Utc::now();

        let (first_claim, _) = apply_message(
            &mut run,
            &WorkerMessage::Claim {
                worker_id: "w-a".to_string(),
                task_types: vec!["echo".to_string()],
            },
            now,
        );
        let lease_a = match first_claim {
            BrokerMessage::Assigned { lease_id, .. } => lease_id,
            other => panic!("unexpected claim response: {other:?}"),
        };

        let task = run.tasks.get_mut("task").expect("task");
        task.lease_expires_at = Some(now - ChronoDuration::seconds(1));
        let _ = timeout_scan(&mut run, now);

        let (second_claim, _) = apply_message(
            &mut run,
            &WorkerMessage::Claim {
                worker_id: "w-b".to_string(),
                task_types: vec!["echo".to_string()],
            },
            now + ChronoDuration::seconds(5),
        );
        let lease_b = match second_claim {
            BrokerMessage::Assigned { lease_id, .. } => lease_id,
            other => panic!("unexpected second claim response: {other:?}"),
        };

        let (complete_b, _) = apply_message(
            &mut run,
            &WorkerMessage::Complete {
                task_id: "task".to_string(),
                lease_id: lease_b.clone(),
                output_ref: "data/run/task/output/b".to_string(),
            },
            now,
        );
        assert_eq!(complete_b, BrokerMessage::Ack);

        let (stale_complete, _) = apply_message(
            &mut run,
            &WorkerMessage::Complete {
                task_id: "task".to_string(),
                lease_id: lease_a,
                output_ref: "data/run/task/output/a".to_string(),
            },
            now,
        );

        assert_eq!(
            stale_complete,
            BrokerMessage::Rejected {
                reason: RejectionReason::LeaseMismatch
            }
        );
        assert_eq!(
            run.tasks["task"].output_ref.as_deref(),
            Some("data/run/task/output/b")
        );
    }

    #[test]
    fn fail_dag_marks_remaining_tasks_skipped_and_claims_no_work() {
        let mut dag = DagBuilder::new("fail-dag");
        dag.task("a")
            .task_type("echo")
            .max_retries(0)
            .on_failure(FailurePolicy::FailDag);
        dag.task("b").task_type("echo").depends_on(["a"]);
        let mut run = dag.build().expect("build");
        run.status = RunStatus::Running;

        let now = Utc::now();
        let task = run.tasks.get_mut("a").expect("task a");
        task.status = TaskStatus::Running;
        task.lease_id = Some("lease-a".to_string());
        task.worker_id = Some("w-1".to_string());

        let (response, changed) = apply_message(
            &mut run,
            &WorkerMessage::Fail {
                task_id: "a".to_string(),
                lease_id: "lease-a".to_string(),
                error: "boom".to_string(),
                retryable: false,
            },
            now,
        );

        assert!(changed);
        assert_eq!(response, BrokerMessage::Ack);
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.tasks["a"].status, TaskStatus::Failed);
        assert_eq!(run.tasks["b"].status, TaskStatus::Skipped);

        let (claim, _) = apply_message(
            &mut run,
            &WorkerMessage::Claim {
                worker_id: "w-2".to_string(),
                task_types: vec!["echo".to_string()],
            },
            now,
        );
        assert_eq!(claim, BrokerMessage::NoWork);
    }
}
