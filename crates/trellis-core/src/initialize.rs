use crate::model::{Run, RunStatus, TaskStatus};
use chrono::Utc;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RunInitOptions {
    pub root_input_refs: HashMap<String, Option<String>>,
    pub reset_run_metadata: bool,
}

impl Default for RunInitOptions {
    fn default() -> Self {
        Self {
            root_input_refs: HashMap::new(),
            reset_run_metadata: true,
        }
    }
}

pub fn initialize_run(run: &mut Run, options: &RunInitOptions) {
    let now = Utc::now();

    if options.reset_run_metadata {
        run.status = RunStatus::Pending;
        run.started_at = None;
        run.completed_at = None;
        run.broker = None;
    }

    for task in run.tasks.values_mut() {
        task.deps_remaining = task.depends_on.len();
        task.started_at = None;
        task.completed_at = None;
        task.last_error = None;
        task.retry_count = 0;
        task.attempt = 0;
        task.worker_id = None;
        task.lease_id = None;
        task.lease_expires_at = None;
        task.output_ref = None;
        task.available_at = now;

        let mut input_refs = HashMap::with_capacity(task.depends_on.len() + 1);
        for dep in &task.depends_on {
            input_refs.insert(dep.clone(), None);
        }

        if task.depends_on.is_empty() {
            task.status = TaskStatus::Pending;
            task.deps_remaining = 0;
            let input_ref = options
                .root_input_refs
                .get(&task.task_id)
                .cloned()
                .unwrap_or(None);
            input_refs.insert("_input".to_string(), input_ref);
        } else {
            task.status = TaskStatus::Blocked;
        }

        task.input_refs = input_refs;
    }

    run.current_layer = run
        .tasks
        .values()
        .filter(|task| !task.status.is_terminal())
        .map(|task| task.layer)
        .min()
        .unwrap_or(run.total_layers);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DagBuilder, TaskStatus};

    #[test]
    fn initializes_root_and_blocked_tasks_consistently() {
        let mut dag = DagBuilder::new("init");
        dag.task("extract").task_type("extract");
        dag.task("load").task_type("load").depends_on(["extract"]);
        let mut run = dag.build().expect("build run");

        let mut root_input_refs = HashMap::new();
        root_input_refs.insert(
            "extract".to_string(),
            Some("data/run/extract/input".to_string()),
        );

        initialize_run(
            &mut run,
            &RunInitOptions {
                root_input_refs,
                reset_run_metadata: true,
            },
        );

        assert_eq!(run.status, RunStatus::Pending);
        assert_eq!(run.tasks["extract"].status, TaskStatus::Pending);
        assert_eq!(run.tasks["load"].status, TaskStatus::Blocked);
        assert_eq!(
            run.tasks["extract"].input_refs.get("_input"),
            Some(&Some("data/run/extract/input".to_string()))
        );
        assert_eq!(run.tasks["load"].input_refs.get("extract"), Some(&None));
    }
}
