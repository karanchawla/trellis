use chrono::{DateTime, Utc};
use std::fmt::{Display, Formatter};
use trellis_core::{Run, RunStatus, TaskStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSnapshot {
    pub run_id: String,
    pub status: RunStatus,
    pub current_layer: u32,
    pub total_layers: u32,
    pub tasks: Vec<TaskSnapshot>,
    pub layers: Vec<LayerSummary>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub layer: u32,
    pub status: TaskStatus,
    pub waiting_on: Vec<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerSummary {
    pub layer: u32,
    pub total_tasks: usize,
    pub blocked: usize,
    pub pending: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub skipped: usize,
}

impl RunSnapshot {
    pub fn from_run(run: &Run) -> Self {
        let mut tasks = run
            .tasks
            .values()
            .map(|task| TaskSnapshot {
                task_id: task.task_id.clone(),
                layer: task.layer,
                status: task.status,
                waiting_on: if task.status == TaskStatus::Blocked {
                    task.depends_on
                        .iter()
                        .filter(|dep| {
                            run.tasks
                                .get(*dep)
                                .map(|parent| !parent.status.is_terminal())
                                .unwrap_or(false)
                        })
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                },
                last_error: task.last_error.clone(),
            })
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            (left.layer, left.task_id.as_str()).cmp(&(right.layer, right.task_id.as_str()))
        });

        let mut layers = (0..run.total_layers)
            .map(|layer| LayerSummary {
                layer,
                total_tasks: 0,
                blocked: 0,
                pending: 0,
                running: 0,
                completed: 0,
                failed: 0,
                skipped: 0,
            })
            .collect::<Vec<_>>();
        for task in run.tasks.values() {
            if let Some(summary) = layers.get_mut(task.layer as usize) {
                summary.total_tasks += 1;
                match task.status {
                    TaskStatus::Blocked => summary.blocked += 1,
                    TaskStatus::Pending => summary.pending += 1,
                    TaskStatus::Running => summary.running += 1,
                    TaskStatus::Completed => summary.completed += 1,
                    TaskStatus::Failed => summary.failed += 1,
                    TaskStatus::Skipped => summary.skipped += 1,
                }
            }
        }

        Self {
            run_id: run.run_id.clone(),
            status: run.status,
            current_layer: run.current_layer,
            total_layers: run.total_layers,
            tasks,
            layers,
            started_at: run.started_at,
            completed_at: run.completed_at,
        }
    }
}

impl Display for RunSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Run {}: {} (layer {} of {})",
            self.run_id,
            render_run_status(self.status),
            self.current_layer + 1,
            self.total_layers
        )?;

        for summary in &self.layers {
            let mut parts = Vec::new();
            if summary.completed > 0 {
                parts.push(format!(
                    "{}/{} completed",
                    summary.completed, summary.total_tasks
                ));
            }
            if summary.running > 0 {
                parts.push(format!(
                    "{}/{} running",
                    summary.running, summary.total_tasks
                ));
            }
            if summary.pending > 0 {
                parts.push(format!(
                    "{}/{} pending",
                    summary.pending, summary.total_tasks
                ));
            }
            if summary.blocked > 0 {
                parts.push(format!(
                    "{}/{} blocked",
                    summary.blocked, summary.total_tasks
                ));
            }
            if summary.failed > 0 {
                parts.push(format!("{}/{} failed", summary.failed, summary.total_tasks));
            }
            if summary.skipped > 0 {
                parts.push(format!(
                    "{}/{} skipped",
                    summary.skipped, summary.total_tasks
                ));
            }
            let status_line = if parts.is_empty() {
                format!("0/{} tasks", summary.total_tasks)
            } else {
                parts.join(", ")
            };
            writeln!(f, "  Layer {}: {}", summary.layer, status_line)?;
        }

        writeln!(f)?;
        writeln!(f, "Tasks:")?;
        for task in &self.tasks {
            if task.waiting_on.is_empty() {
                writeln!(
                    f,
                    "  {:<12} [L{}] {}",
                    task.task_id,
                    task.layer,
                    render_task_status(task.status)
                )?;
            } else {
                writeln!(
                    f,
                    "  {:<12} [L{}] {} (waiting on: {})",
                    task.task_id,
                    task.layer,
                    render_task_status(task.status),
                    task.waiting_on.join(", ")
                )?;
            }
            if let Some(last_error) = &task.last_error {
                writeln!(f, "    error: {last_error}")?;
            }
        }

        Ok(())
    }
}

fn render_run_status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Pending => "pending",
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn render_task_status(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Blocked => "blocked",
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Skipped => "skipped",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use trellis_core::{FailurePolicy, RetryPolicy, Task};

    fn task(task_id: &str, layer: u32, status: TaskStatus, depends_on: Vec<&str>) -> Task {
        Task {
            task_id: task_id.to_string(),
            task_type: task_id.to_string(),
            layer,
            depends_on: depends_on.into_iter().map(str::to_string).collect(),
            deps_remaining: 0,
            downstream: Vec::new(),
            input_refs: HashMap::new(),
            status,
            started_at: None,
            available_at: Utc::now(),
            completed_at: None,
            last_error: None,
            timeout_seconds: 30,
            max_retries: 1,
            retry_count: 0,
            retry_policy: RetryPolicy::default(),
            attempt: 0,
            worker_id: None,
            lease_id: None,
            lease_expires_at: None,
            on_failure: FailurePolicy::FailDag,
            output_ref: None,
        }
    }

    #[test]
    fn mixed_status_snapshot_renders_expected_sections() {
        let mut tasks = HashMap::new();
        tasks.insert(
            "extract".to_string(),
            task("extract", 0, TaskStatus::Completed, vec![]),
        );
        tasks.insert(
            "transform-a".to_string(),
            task("transform-a", 1, TaskStatus::Running, vec!["extract"]),
        );
        tasks.insert(
            "transform-b".to_string(),
            task("transform-b", 1, TaskStatus::Pending, vec!["extract"]),
        );
        tasks.insert(
            "load".to_string(),
            task(
                "load",
                2,
                TaskStatus::Blocked,
                vec!["transform-a", "transform-b"],
            ),
        );

        let run = Run {
            run_id: "run-abc123".to_string(),
            status: RunStatus::Running,
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            completed_at: None,
            broker: None,
            total_layers: 3,
            current_layer: 1,
            tasks,
        };
        let rendered = RunSnapshot::from_run(&run).to_string();

        assert!(rendered.contains("Run run-abc123: running (layer 2 of 3)"));
        assert!(rendered.contains("Layer 0: 1/1 completed"));
        assert!(rendered.contains("Layer 1: 1/2 running, 1/2 pending"));
        assert!(rendered.contains("load"));
        assert!(rendered.contains("waiting on: transform-a, transform-b"));
    }
}
