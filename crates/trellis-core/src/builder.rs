use crate::errors::DagValidationError;
use crate::initialize::{RunInitOptions, initialize_run};
use crate::layers::assign_layers;
use crate::model::{FailurePolicy, RetryPolicy, Run, Task, TaskStatus};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct TaskSpec {
    task_id: String,
    task_type: Option<String>,
    depends_on: Vec<String>,
    timeout_seconds: u32,
    max_retries: u32,
    on_failure: FailurePolicy,
    retry_policy: RetryPolicy,
}

impl TaskSpec {
    fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
            task_type: None,
            depends_on: Vec::new(),
            timeout_seconds: 300,
            max_retries: 3,
            on_failure: FailurePolicy::FailDag,
            retry_policy: RetryPolicy::default(),
        }
    }
}

pub struct DagBuilder {
    name: String,
    tasks: Vec<TaskSpec>,
}

pub struct TaskBuilder<'a> {
    spec: &'a mut TaskSpec,
}

impl DagBuilder {
    /// Creates a new DAG builder.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            tasks: Vec::new(),
        }
    }

    /// Starts defining a task with the provided task id.
    pub fn task(&mut self, task_id: &str) -> TaskBuilder<'_> {
        self.tasks.push(TaskSpec::new(task_id));
        let spec = self
            .tasks
            .last_mut()
            .expect("task vector has at least one newly inserted element");
        TaskBuilder { spec }
    }

    /// Builds and validates a DAG, returning an executable run state.
    pub fn build(self) -> Result<Run, DagValidationError> {
        if self.tasks.is_empty() {
            return Err(DagValidationError::EmptyDag);
        }

        let mut seen = HashSet::new();
        let mut ids = HashSet::new();

        for spec in &self.tasks {
            validate_task_id(&spec.task_id)?;
            if !seen.insert(spec.task_id.clone()) {
                return Err(DagValidationError::DuplicateTaskId(spec.task_id.clone()));
            }
            let has_task_type = spec
                .task_type
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty());
            if !has_task_type {
                return Err(DagValidationError::MissingTaskType(spec.task_id.clone()));
            }
            ids.insert(spec.task_id.clone());
        }

        for spec in &self.tasks {
            for dependency in &spec.depends_on {
                if !ids.contains(dependency) {
                    return Err(DagValidationError::MissingDependency(dependency.clone()));
                }
            }
        }

        let now = Utc::now();
        let mut tasks: HashMap<String, Task> = HashMap::with_capacity(self.tasks.len());

        for spec in self.tasks {
            let mut input_refs = HashMap::new();
            for dep in &spec.depends_on {
                input_refs.insert(dep.clone(), None);
            }

            let task = Task {
                task_id: spec.task_id.clone(),
                task_type: spec
                    .task_type
                    .expect("task_type validated to be present before task construction"),
                layer: 0,
                depends_on: spec.depends_on.clone(),
                deps_remaining: spec.depends_on.len(),
                downstream: Vec::new(),
                input_refs,
                status: TaskStatus::Blocked,
                started_at: None,
                available_at: now,
                completed_at: None,
                last_error: None,
                timeout_seconds: spec.timeout_seconds,
                max_retries: spec.max_retries,
                retry_count: 0,
                retry_policy: spec.retry_policy,
                attempt: 0,
                worker_id: None,
                lease_id: None,
                lease_expires_at: None,
                on_failure: spec.on_failure,
                output_ref: None,
            };

            tasks.insert(task.task_id.clone(), task);
        }

        let task_ids: Vec<String> = tasks.keys().cloned().collect();
        for task_id in task_ids {
            let depends_on = tasks
                .get(&task_id)
                .expect("task id collected from map keys")
                .depends_on
                .clone();
            for dep in depends_on {
                tasks
                    .get_mut(&dep)
                    .expect("dependency exists from validated ids")
                    .downstream
                    .push(task_id.clone());
            }
        }

        let total_layers = assign_layers(&mut tasks)?;

        let run_id = format!("{}-{}", sanitize_name(&self.name), Uuid::new_v4());
        let mut run = Run::new(run_id);
        run.total_layers = total_layers;
        run.tasks = tasks;
        initialize_run(&mut run, &RunInitOptions::default());
        Ok(run)
    }
}

impl<'a> TaskBuilder<'a> {
    /// Sets the task type.
    pub fn task_type(self, task_type: impl Into<String>) -> Self {
        self.spec.task_type = Some(task_type.into());
        self
    }

    /// Sets task dependencies by task id.
    pub fn depends_on<I, S>(self, depends_on: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.spec.depends_on = depends_on.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the heartbeat timeout window in seconds.
    pub fn timeout_seconds(self, timeout_seconds: u32) -> Self {
        self.spec.timeout_seconds = timeout_seconds;
        self
    }

    /// Sets the maximum retry count.
    pub fn max_retries(self, max_retries: u32) -> Self {
        self.spec.max_retries = max_retries;
        self
    }

    /// Sets failure policy for this task.
    pub fn on_failure(self, on_failure: FailurePolicy) -> Self {
        self.spec.on_failure = on_failure;
        self
    }

    /// Sets retry policy for this task.
    pub fn retry_policy(self, retry_policy: RetryPolicy) -> Self {
        self.spec.retry_policy = retry_policy;
        self
    }
}

fn validate_task_id(task_id: &str) -> Result<(), DagValidationError> {
    if task_id.is_empty() {
        return Err(DagValidationError::InvalidTaskId(task_id.to_string()));
    }
    if task_id.starts_with('_') {
        return Err(DagValidationError::ReservedTaskId(task_id.to_string()));
    }

    let mut chars = task_id.chars();
    let first = chars.next().expect("task_id emptiness handled above");
    if !first.is_ascii_alphanumeric() {
        return Err(DagValidationError::InvalidTaskId(task_id.to_string()));
    }

    if chars.any(|c| !(c.is_ascii_alphanumeric() || c == '_' || c == '-')) {
        return Err(DagValidationError::InvalidTaskId(task_id.to_string()));
    }

    Ok(())
}

fn sanitize_name(name: &str) -> String {
    let normalized = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>();

    if normalized.is_empty() {
        "run".to_string()
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_diamond_dag_with_correct_wiring() {
        let mut dag = DagBuilder::new("diamond");
        dag.task("a").task_type("extract");
        dag.task("b").task_type("transform").depends_on(["a"]);
        dag.task("c").task_type("transform").depends_on(["a"]);
        dag.task("d").task_type("load").depends_on(["b", "c"]);

        let run = dag.build().expect("build should succeed");

        assert_eq!(run.tasks["a"].downstream.len(), 2);
        assert!(run.tasks["a"].downstream.contains(&"b".to_string()));
        assert!(run.tasks["a"].downstream.contains(&"c".to_string()));
        assert_eq!(run.tasks["b"].deps_remaining, 1);
        assert_eq!(run.tasks["c"].deps_remaining, 1);
        assert_eq!(run.tasks["d"].deps_remaining, 2);
        assert_eq!(run.tasks["a"].status, TaskStatus::Pending);
        assert_eq!(run.tasks["d"].status, TaskStatus::Blocked);
    }

    #[test]
    fn rejects_cycle() {
        let mut dag = DagBuilder::new("cycle");
        dag.task("a").task_type("x").depends_on(["c"]);
        dag.task("b").task_type("x").depends_on(["a"]);
        dag.task("c").task_type("x").depends_on(["b"]);

        let err = dag.build().expect_err("cycle should fail");
        assert_eq!(err, DagValidationError::CycleDetected);
    }

    #[test]
    fn rejects_missing_dependency() {
        let mut dag = DagBuilder::new("missing");
        dag.task("a").task_type("x").depends_on(["does-not-exist"]);

        let err = dag.build().expect_err("missing dependency should fail");
        assert_eq!(
            err,
            DagValidationError::MissingDependency("does-not-exist".to_string())
        );
    }

    #[test]
    fn rejects_duplicate_task_id() {
        let mut dag = DagBuilder::new("duplicate");
        dag.task("a").task_type("x");
        dag.task("a").task_type("y");

        let err = dag.build().expect_err("duplicate task id should fail");
        assert_eq!(err, DagValidationError::DuplicateTaskId("a".to_string()));
    }

    #[test]
    fn rejects_missing_task_type() {
        let mut dag = DagBuilder::new("missing-type");
        dag.task("a");

        let err = dag.build().expect_err("missing task type should fail");
        assert_eq!(err, DagValidationError::MissingTaskType("a".to_string()));
    }

    #[test]
    fn rejects_reserved_task_id() {
        let mut dag = DagBuilder::new("reserved");
        dag.task("_hidden").task_type("x");

        let err = dag.build().expect_err("reserved task id should fail");
        assert_eq!(
            err,
            DagValidationError::ReservedTaskId("_hidden".to_string())
        );
    }

    #[test]
    fn rejects_invalid_task_ids() {
        let mut dag1 = DagBuilder::new("invalid1");
        dag1.task("foo/bar").task_type("x");
        let err1 = dag1.build().expect_err("invalid char should fail");
        assert_eq!(
            err1,
            DagValidationError::InvalidTaskId("foo/bar".to_string())
        );

        let mut dag2 = DagBuilder::new("invalid2");
        dag2.task("a b").task_type("x");
        let err2 = dag2.build().expect_err("space should fail");
        assert_eq!(err2, DagValidationError::InvalidTaskId("a b".to_string()));
    }

    #[test]
    fn rejects_empty_dag() {
        let dag = DagBuilder::new("empty");
        let err = dag.build().expect_err("empty DAG should fail");
        assert_eq!(err, DagValidationError::EmptyDag);
    }

    #[test]
    fn assigns_layers_correctly() {
        let mut dag = DagBuilder::new("layers");
        dag.task("a").task_type("x");
        dag.task("b").task_type("x").depends_on(["a"]);
        dag.task("c").task_type("x").depends_on(["a"]);
        dag.task("d").task_type("x").depends_on(["b", "c"]);

        let run = dag.build().expect("build should succeed");
        assert_eq!(run.total_layers, 3);
        assert_eq!(run.tasks["a"].layer, 0);
        assert_eq!(run.tasks["b"].layer, 1);
        assert_eq!(run.tasks["c"].layer, 1);
        assert_eq!(run.tasks["d"].layer, 2);
    }
}
