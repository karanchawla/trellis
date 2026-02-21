use crate::errors::{ExecutionError, TaskError};
use crate::model::{FailurePolicy, Run, RunStatus, TaskStatus, calculate_backoff};
use crate::state::{deserialize_run, serialize_run};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use trellis_store::{ObjectStore, StoreError, download_data, upload_output};
use uuid::Uuid;

pub type TaskHandler =
    Box<dyn Fn(HashMap<String, Value>) -> Result<Value, TaskError> + Send + Sync + 'static>;

#[async_trait]
pub trait ExecutionBackend: Send {
    async fn prepare_run(&mut self, run: &Run) -> Result<(), ExecutionError>;

    async fn persist_run(&mut self, run: &Run) -> Result<(), ExecutionError>;

    async fn load_inputs(
        &mut self,
        run: &Run,
        task_id: &str,
        depends_on: &[String],
    ) -> Result<HashMap<String, Value>, ExecutionError>;

    async fn store_output(
        &mut self,
        run: &Run,
        task_id: &str,
        lease_id: &str,
        attempt: u32,
        output: &Value,
    ) -> Result<String, ExecutionError>;
}

pub struct MemoryBackend {
    root_inputs: HashMap<String, Value>,
    outputs: HashMap<String, Value>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            root_inputs: HashMap::new(),
            outputs: HashMap::new(),
        }
    }

    pub fn set_root_inputs(&mut self, inputs: HashMap<String, Value>) {
        self.root_inputs = inputs;
        self.outputs.clear();
    }
}

#[async_trait]
impl ExecutionBackend for MemoryBackend {
    async fn prepare_run(&mut self, _run: &Run) -> Result<(), ExecutionError> {
        self.outputs.clear();
        Ok(())
    }

    async fn persist_run(&mut self, _run: &Run) -> Result<(), ExecutionError> {
        Ok(())
    }

    async fn load_inputs(
        &mut self,
        _run: &Run,
        task_id: &str,
        depends_on: &[String],
    ) -> Result<HashMap<String, Value>, ExecutionError> {
        let mut handler_inputs = HashMap::new();

        if depends_on.is_empty() {
            if let Some(root_input) = self.root_inputs.get(task_id) {
                handler_inputs.insert("_input".to_string(), root_input.clone());
            }
            return Ok(handler_inputs);
        }

        for parent_id in depends_on {
            let parent_output = self.outputs.get(parent_id).ok_or_else(|| {
                ExecutionError::InvariantViolation(format!(
                    "missing parent output for {task_id} from {parent_id}"
                ))
            })?;
            handler_inputs.insert(parent_id.clone(), parent_output.clone());
        }

        Ok(handler_inputs)
    }

    async fn store_output(
        &mut self,
        run: &Run,
        task_id: &str,
        _lease_id: &str,
        attempt: u32,
        output: &Value,
    ) -> Result<String, ExecutionError> {
        self.outputs.insert(task_id.to_string(), output.clone());
        Ok(format!(
            "memory://{}/{}/output/attempt-{}",
            run.run_id, task_id, attempt
        ))
    }
}

pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    state_key: Option<String>,
    state_etag: Option<String>,
}

impl ObjectStoreBackend {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            state_key: None,
            state_etag: None,
        }
    }
}

#[async_trait]
impl ExecutionBackend for ObjectStoreBackend {
    async fn prepare_run(&mut self, run: &Run) -> Result<(), ExecutionError> {
        let state_key = format!("runs/{}/state.json", run.run_id);
        let etag = match self.store.get(&state_key).await {
            Ok((body, etag)) => {
                let stored = deserialize_run(&body)
                    .map_err(|e| ExecutionError::StateSerde(format!("invalid stored run: {e}")))?;
                if stored.run_id != run.run_id {
                    return Err(ExecutionError::InvariantViolation(format!(
                        "stored run id {} does not match requested run id {}",
                        stored.run_id, run.run_id
                    )));
                }
                etag
            }
            Err(StoreError::NotFound(_)) => {
                let body = serialize_run(run)
                    .map_err(|e| ExecutionError::StateSerde(format!("serialize run: {e}")))?;
                self.store.put_if_none_match(&state_key, body).await?
            }
            Err(error) => return Err(ExecutionError::Store(error)),
        };

        self.state_key = Some(state_key);
        self.state_etag = Some(etag);
        Ok(())
    }

    async fn persist_run(&mut self, run: &Run) -> Result<(), ExecutionError> {
        let state_key = self.state_key.as_ref().ok_or_else(|| {
            ExecutionError::InvariantViolation("state key was not initialized".to_string())
        })?;
        let etag = self.state_etag.as_ref().ok_or_else(|| {
            ExecutionError::InvariantViolation("state etag was not initialized".to_string())
        })?;

        let body = serialize_run(run)
            .map_err(|e| ExecutionError::StateSerde(format!("serialize run: {e}")))?;
        let next_etag = self.store.put_if_match(state_key, body, etag).await?;
        self.state_etag = Some(next_etag);
        Ok(())
    }

    async fn load_inputs(
        &mut self,
        run: &Run,
        task_id: &str,
        depends_on: &[String],
    ) -> Result<HashMap<String, Value>, ExecutionError> {
        let task = run.tasks.get(task_id).ok_or_else(|| {
            ExecutionError::InvariantViolation(format!("task missing from run map: {task_id}"))
        })?;
        let mut handler_inputs = HashMap::new();

        if depends_on.is_empty() {
            if let Some(root_key) = task.input_refs.get("_input").cloned().flatten() {
                let payload = download_data(self.store.as_ref(), &root_key).await?;
                let value = serde_json::from_slice::<Value>(&payload).map_err(|e| {
                    ExecutionError::StateSerde(format!(
                        "failed to deserialize root input for {task_id}: {e}"
                    ))
                })?;
                handler_inputs.insert("_input".to_string(), value);
            }
            return Ok(handler_inputs);
        }

        for parent_id in depends_on {
            let input_ref = task
                .input_refs
                .get(parent_id)
                .cloned()
                .flatten()
                .ok_or_else(|| {
                    ExecutionError::InvariantViolation(format!(
                        "missing input ref for dependency {parent_id} -> {task_id}"
                    ))
                })?;

            let payload = download_data(self.store.as_ref(), &input_ref).await?;
            let value = serde_json::from_slice::<Value>(&payload).map_err(|e| {
                ExecutionError::StateSerde(format!(
                    "failed to deserialize dependency {parent_id} for {task_id}: {e}"
                ))
            })?;
            handler_inputs.insert(parent_id.clone(), value);
        }

        Ok(handler_inputs)
    }

    async fn store_output(
        &mut self,
        run: &Run,
        task_id: &str,
        lease_id: &str,
        _attempt: u32,
        output: &Value,
    ) -> Result<String, ExecutionError> {
        let encoded = serde_json::to_vec(output).map_err(|e| {
            ExecutionError::StateSerde(format!("failed to serialize output for {task_id}: {e}"))
        })?;
        let key = upload_output(
            self.store.as_ref(),
            &run.run_id,
            task_id,
            lease_id,
            Bytes::from(encoded),
        )
        .await?;
        Ok(key)
    }
}

pub struct Executor<B> {
    backend: B,
    handlers: HashMap<String, TaskHandler>,
}

impl<B> Executor<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            handlers: HashMap::new(),
        }
    }

    pub fn register(&mut self, task_type: &str, handler: TaskHandler) {
        self.handlers.insert(task_type.to_string(), handler);
    }
}

impl<B: ExecutionBackend> Executor<B> {
    pub async fn execute_async(&mut self, run: &mut Run) -> Result<(), ExecutionError> {
        if run.status.is_terminal() {
            return Err(ExecutionError::InvalidRunState(format!(
                "run {} is already terminal: {:?}",
                run.run_id, run.status
            )));
        }

        self.backend.prepare_run(run).await?;

        let mut now = Utc::now();
        if run.started_at.is_none() {
            run.started_at = Some(now);
        }
        run.status = RunStatus::Running;
        self.backend.persist_run(run).await?;

        loop {
            Self::update_current_layer(run);
            Self::check_run_completion(run, now);

            if run.tasks.values().all(|task| task.status.is_terminal()) {
                break;
            }

            if let Some(task_id) = Self::find_next_runnable_task(run, now) {
                self.execute_task(run, &task_id, &mut now).await?;
                self.backend.persist_run(run).await?;
                continue;
            }

            if let Some(next_available) = run
                .tasks
                .values()
                .filter(|task| task.status == TaskStatus::Pending)
                .map(|task| task.available_at)
                .min()
                && next_available > now
            {
                now = next_available;
                continue;
            }

            let changed = Self::propagate_unreachable_from_all_failed(run, now);
            if changed {
                self.backend.persist_run(run).await?;
                continue;
            }

            return Err(ExecutionError::Deadlock(
                "no runnable tasks remain while run is non-terminal".to_string(),
            ));
        }

        Self::update_current_layer(run);
        Self::check_run_completion(run, now);
        self.backend.persist_run(run).await?;

        Ok(())
    }

    async fn execute_task(
        &mut self,
        run: &mut Run,
        task_id: &str,
        now: &mut DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        let (task_type, depends_on, lease_id, attempt) = {
            let task = run.tasks.get_mut(task_id).ok_or_else(|| {
                ExecutionError::InvariantViolation(format!("task missing from run map: {task_id}"))
            })?;

            if task.status != TaskStatus::Pending {
                return Err(ExecutionError::InvariantViolation(format!(
                    "task {task_id} expected pending, found {:?}",
                    task.status
                )));
            }

            task.status = TaskStatus::Running;
            if task.started_at.is_none() {
                task.started_at = Some(*now);
            }
            task.worker_id = Some("executor".to_string());
            let lease_id = Uuid::new_v4().to_string();
            task.lease_id = Some(lease_id.clone());
            task.lease_expires_at =
                Some(*now + ChronoDuration::seconds(task.timeout_seconds as i64));
            task.attempt += 1;

            (
                task.task_type.clone(),
                task.depends_on.clone(),
                lease_id,
                task.attempt,
            )
        };

        let handler_inputs = self.backend.load_inputs(run, task_id, &depends_on).await?;

        let handler = self
            .handlers
            .get(&task_type)
            .ok_or_else(|| ExecutionError::MissingHandler(task_type.clone()))?;

        match handler(handler_inputs) {
            Ok(output) => {
                let output_ref = self
                    .backend
                    .store_output(run, task_id, &lease_id, attempt, &output)
                    .await?;
                Self::complete_task(run, task_id, &lease_id, output_ref, *now)?;
            }
            Err(TaskError::Retryable(error)) => {
                Self::fail_task(run, task_id, &lease_id, error, true, *now)?;
            }
            Err(TaskError::Permanent(error)) => {
                Self::fail_task(run, task_id, &lease_id, error, false, *now)?;
            }
        }

        Ok(())
    }

    fn find_next_runnable_task(run: &Run, now: DateTime<Utc>) -> Option<String> {
        if run.status != RunStatus::Running {
            return None;
        }

        run.tasks
            .iter()
            .filter(|(_, task)| task.status == TaskStatus::Pending && task.available_at <= now)
            .min_by(|(id_a, task_a), (id_b, task_b)| {
                (task_a.layer, task_a.available_at, id_a.as_str()).cmp(&(
                    task_b.layer,
                    task_b.available_at,
                    id_b.as_str(),
                ))
            })
            .map(|(task_id, _)| task_id.clone())
    }

    fn complete_task(
        run: &mut Run,
        task_id: &str,
        lease_id: &str,
        output_ref: String,
        now: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        let downstream = {
            let task = run.tasks.get_mut(task_id).ok_or_else(|| {
                ExecutionError::InvariantViolation(format!("task missing from run map: {task_id}"))
            })?;

            if task.lease_id.as_deref() != Some(lease_id) {
                return Ok(());
            }

            task.status = TaskStatus::Completed;
            task.output_ref = Some(output_ref.clone());
            task.completed_at = Some(now);
            task.worker_id = None;
            task.lease_id = None;
            task.lease_expires_at = None;
            task.downstream.clone()
        };

        for child_id in downstream {
            let child = run.tasks.get_mut(&child_id).ok_or_else(|| {
                ExecutionError::InvariantViolation(format!(
                    "child task missing from run map: {child_id}"
                ))
            })?;

            if child.status.is_terminal() {
                continue;
            }

            if child.deps_remaining > 0 {
                child.deps_remaining -= 1;
            }
            child
                .input_refs
                .insert(task_id.to_string(), Some(output_ref.clone()));

            if run.status == RunStatus::Running
                && child.status == TaskStatus::Blocked
                && child.deps_remaining == 0
            {
                child.status = TaskStatus::Pending;
                child.available_at = now;
            }
        }

        Ok(())
    }

    fn fail_task(
        run: &mut Run,
        task_id: &str,
        lease_id: &str,
        error: String,
        retryable: bool,
        now: DateTime<Utc>,
    ) -> Result<(), ExecutionError> {
        let mut failed_permanently = false;
        let on_failure = {
            let task = run.tasks.get_mut(task_id).ok_or_else(|| {
                ExecutionError::InvariantViolation(format!("task missing from run map: {task_id}"))
            })?;

            if task.lease_id.as_deref() != Some(lease_id) {
                return Ok(());
            }

            task.last_error = Some(error);
            task.worker_id = None;
            task.lease_id = None;
            task.lease_expires_at = None;

            if retryable && task.retry_count < task.max_retries {
                task.status = TaskStatus::Pending;
                task.retry_count += 1;
                task.available_at = now
                    + to_chrono_duration(calculate_backoff(task.retry_count, &task.retry_policy));
            } else {
                task.status = TaskStatus::Failed;
                task.completed_at = Some(now);
                failed_permanently = true;
            }

            task.on_failure
        };

        if failed_permanently {
            Self::apply_failure_policy(run, task_id, on_failure, now);
        }

        Ok(())
    }

    fn apply_failure_policy(
        run: &mut Run,
        failed_task_id: &str,
        on_failure: FailurePolicy,
        now: DateTime<Utc>,
    ) {
        match on_failure {
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
                Self::propagate_unreachable(run, failed_task_id, now);
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
                        let child = match run.tasks.get_mut(&child_id) {
                            Some(child) => child,
                            None => continue,
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

    fn propagate_unreachable(run: &mut Run, failed_task_id: &str, now: DateTime<Utc>) -> bool {
        let mut changed = false;
        let mut queue = VecDeque::new();

        if let Some(task) = run.tasks.get(failed_task_id) {
            for child_id in &task.downstream {
                queue.push_back(child_id.clone());
            }
        }

        while let Some(child_id) = queue.pop_front() {
            let should_skip = {
                let child = match run.tasks.get(&child_id) {
                    Some(child) => child,
                    None => continue,
                };

                if child.status != TaskStatus::Blocked {
                    false
                } else {
                    child.depends_on.iter().any(|dep| {
                        run.tasks
                            .get(dep)
                            .map(|parent| {
                                matches!(parent.status, TaskStatus::Failed | TaskStatus::Skipped)
                            })
                            .unwrap_or(false)
                    })
                }
            };

            if should_skip {
                let downstream = {
                    let child = run
                        .tasks
                        .get_mut(&child_id)
                        .expect("child exists after lookup");
                    child.status = TaskStatus::Skipped;
                    child.completed_at = Some(now);
                    child.downstream.clone()
                };
                changed = true;
                for next in downstream {
                    queue.push_back(next);
                }
            }
        }

        changed
    }

    fn propagate_unreachable_from_all_failed(run: &mut Run, now: DateTime<Utc>) -> bool {
        let failed_ids: Vec<String> = run
            .tasks
            .iter()
            .filter_map(|(task_id, task)| {
                (task.status == TaskStatus::Failed).then_some(task_id.clone())
            })
            .collect();

        let mut changed = false;
        for failed_id in failed_ids {
            changed |= Self::propagate_unreachable(run, &failed_id, now);
        }
        changed
    }

    fn check_run_completion(run: &mut Run, now: DateTime<Utc>) {
        if run.status.is_terminal() {
            return;
        }

        let all_terminal = run.tasks.values().all(|task| task.status.is_terminal());
        if all_terminal {
            let any_failed = run
                .tasks
                .values()
                .any(|task| task.status == TaskStatus::Failed);
            run.status = if any_failed {
                RunStatus::Failed
            } else {
                RunStatus::Completed
            };
            run.completed_at = Some(now);
        }
    }

    fn update_current_layer(run: &mut Run) {
        run.current_layer = run
            .tasks
            .values()
            .filter(|task| !task.status.is_terminal())
            .map(|task| task.layer)
            .min()
            .unwrap_or(run.total_layers);
    }
}

pub struct LocalExecutor {
    executor: Executor<MemoryBackend>,
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalExecutor {
    /// Creates a new local in-process executor.
    pub fn new() -> Self {
        Self {
            executor: Executor::new(MemoryBackend::new()),
        }
    }

    /// Registers a handler for a task type.
    pub fn register(&mut self, task_type: &str, handler: TaskHandler) {
        self.executor.register(task_type, handler);
    }

    /// Executes a run to completion in-process.
    pub fn execute(
        &mut self,
        run: &mut Run,
        inputs: HashMap<String, Value>,
    ) -> Result<(), ExecutionError> {
        self.executor.backend.set_root_inputs(inputs);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ExecutionError::InvariantViolation(format!("runtime init failed: {e}")))?;
        runtime.block_on(self.executor.execute_async(run))
    }
}

pub struct S3Executor {
    executor: Executor<ObjectStoreBackend>,
}

impl S3Executor {
    /// Creates a new single-process executor that persists state and task data in object storage.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            executor: Executor::new(ObjectStoreBackend::new(store)),
        }
    }

    /// Registers a handler for a task type.
    pub fn register(&mut self, task_type: &str, handler: TaskHandler) {
        self.executor.register(task_type, handler);
    }

    /// Executes a run to completion using object storage for all task inputs/outputs.
    pub async fn execute(&mut self, run: &mut Run) -> Result<(), ExecutionError> {
        self.executor.execute_async(run).await
    }
}

fn to_chrono_duration(duration: std::time::Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_else(|_| ChronoDuration::zero())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DagBuilder, FailurePolicy};
    use tempfile::tempdir;
    use trellis_store::{LocalFsStore, ObjectStore, upload_input};

    use std::sync::{Arc, Mutex};

    fn linear_run() -> Run {
        let mut dag = DagBuilder::new("linear");
        dag.task("extract").task_type("extract");
        dag.task("transform")
            .task_type("transform")
            .depends_on(["extract"]);
        dag.task("load").task_type("load").depends_on(["transform"]);
        dag.build().expect("linear build")
    }

    #[test]
    fn executes_linear_chain_with_data_flow() {
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut executor = LocalExecutor::new();

        {
            let order = Arc::clone(&order);
            executor.register(
                "extract",
                Box::new(move |inputs| {
                    order
                        .lock()
                        .expect("lock order")
                        .push("extract".to_string());
                    Ok(serde_json::json!({"records": inputs["_input"]["records"].clone()}))
                }),
            );
        }

        {
            let order = Arc::clone(&order);
            executor.register(
                "transform",
                Box::new(move |inputs| {
                    order
                        .lock()
                        .expect("lock order")
                        .push("transform".to_string());
                    let records = inputs["extract"]["records"].clone();
                    Ok(serde_json::json!({"records": records, "transformed": true}))
                }),
            );
        }

        {
            let order = Arc::clone(&order);
            executor.register(
                "load",
                Box::new(move |inputs| {
                    order.lock().expect("lock order").push("load".to_string());
                    Ok(serde_json::json!({"loaded": inputs["transform"]["transformed"].clone()}))
                }),
            );
        }

        let mut run = linear_run();
        let mut inputs = HashMap::new();
        inputs.insert(
            "extract".to_string(),
            serde_json::json!({"records": [1, 2, 3]}),
        );

        executor
            .execute(&mut run, inputs)
            .expect("execution success");

        let actual_order = order.lock().expect("lock order").clone();
        assert_eq!(actual_order, vec!["extract", "transform", "load"]);
        assert_eq!(run.status, RunStatus::Completed);
        assert!(
            run.tasks
                .values()
                .all(|task| task.status == TaskStatus::Completed)
        );
    }

    #[test]
    fn executes_single_task_dag() {
        let mut dag = DagBuilder::new("single");
        dag.task("extract").task_type("extract");
        let mut run = dag.build().expect("single build");

        let mut executor = LocalExecutor::new();
        executor.register("extract", Box::new(|_| Ok(serde_json::json!({"ok": true}))));

        let mut inputs = HashMap::new();
        inputs.insert("extract".to_string(), serde_json::json!({"source": "unit"}));

        executor
            .execute(&mut run, inputs)
            .expect("execution success");

        assert_eq!(run.status, RunStatus::Completed);
        assert_eq!(run.tasks["extract"].status, TaskStatus::Completed);
    }

    #[test]
    fn continue_policy_skips_unreachable_only() {
        let mut dag = DagBuilder::new("continue");
        dag.task("a").task_type("source");
        dag.task("b")
            .task_type("branch_fail")
            .depends_on(["a"])
            .on_failure(FailurePolicy::Continue);
        dag.task("c")
            .task_type("branch_ok")
            .depends_on(["a"])
            .on_failure(FailurePolicy::FailDag);
        dag.task("d")
            .task_type("join")
            .depends_on(["b", "c"])
            .on_failure(FailurePolicy::FailDag);

        let mut run = dag.build().expect("build continue test run");
        let mut executor = LocalExecutor::new();

        executor.register("source", Box::new(|_| Ok(serde_json::json!({"n": 1}))));
        executor.register(
            "branch_fail",
            Box::new(|_| Err(TaskError::Permanent("fail-b".to_string()))),
        );
        executor.register(
            "branch_ok",
            Box::new(|_| Ok(serde_json::json!({"ok": true}))),
        );
        executor.register(
            "join",
            Box::new(|_| Ok(serde_json::json!({"joined": true}))),
        );

        executor
            .execute(&mut run, HashMap::new())
            .expect("execution handles continue policy");

        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.tasks["b"].status, TaskStatus::Failed);
        assert_eq!(run.tasks["c"].status, TaskStatus::Completed);
        assert_eq!(run.tasks["d"].status, TaskStatus::Skipped);
    }

    #[test]
    fn retryable_error_eventually_succeeds() {
        let mut dag = DagBuilder::new("retry-success");
        dag.task("a")
            .task_type("flaky")
            .max_retries(5)
            .on_failure(FailurePolicy::FailDag);
        let mut run = dag.build().expect("build retry-success run");

        let attempts = Arc::new(Mutex::new(0u32));
        let mut executor = LocalExecutor::new();
        {
            let attempts = Arc::clone(&attempts);
            executor.register(
                "flaky",
                Box::new(move |_| {
                    let mut n = attempts.lock().expect("lock attempts");
                    *n += 1;
                    if *n < 3 {
                        Err(TaskError::Retryable(format!("attempt {n}")))
                    } else {
                        Ok(serde_json::json!({"ok": true}))
                    }
                }),
            );
        }

        executor
            .execute(&mut run, HashMap::new())
            .expect("execution should complete successfully");

        assert_eq!(*attempts.lock().expect("lock attempts"), 3);
        assert_eq!(run.tasks["a"].retry_count, 2);
        assert_eq!(run.tasks["a"].status, TaskStatus::Completed);
        assert_eq!(run.status, RunStatus::Completed);
    }

    #[tokio::test]
    async fn s3_executor_persists_state_and_output_refs() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let mut dag = DagBuilder::new("s3-exec");
        dag.task("extract").task_type("extract");
        dag.task("transform")
            .task_type("transform")
            .depends_on(["extract"]);
        dag.task("load").task_type("load").depends_on(["transform"]);
        let mut run = dag.build().expect("build run");
        run.run_id = "run-s3-exec".to_string();

        let root_key = upload_input(
            store.as_ref(),
            &run.run_id,
            "extract",
            Bytes::from_static(br#"{"records":[1,2,3]}"#),
        )
        .await
        .expect("upload root input");
        run.tasks
            .get_mut("extract")
            .expect("extract task")
            .input_refs
            .insert("_input".to_string(), Some(root_key));

        let mut executor = S3Executor::new(store.clone());
        executor.register(
            "extract",
            Box::new(|inputs| Ok(serde_json::json!({ "records": inputs["_input"]["records"] }))),
        );
        executor.register(
            "transform",
            Box::new(|inputs| {
                let values = inputs["extract"]["records"]
                    .as_array()
                    .expect("records array")
                    .iter()
                    .map(|item| item.as_i64().expect("i64") + 10)
                    .collect::<Vec<_>>();
                Ok(serde_json::json!({ "records": values }))
            }),
        );
        executor.register(
            "load",
            Box::new(|inputs| {
                let count = inputs["transform"]["records"]
                    .as_array()
                    .expect("records array")
                    .len();
                Ok(serde_json::json!({ "loaded": count }))
            }),
        );

        executor.execute(&mut run).await.expect("execute run");
        assert_eq!(run.status, RunStatus::Completed);
        assert!(
            run.tasks
                .values()
                .all(|task| task.status == TaskStatus::Completed)
        );

        let state_key = format!("runs/{}/state.json", run.run_id);
        for task_id in ["extract", "transform", "load"] {
            let output_key = run.tasks[task_id]
                .output_ref
                .clone()
                .expect("output_ref should exist");
            assert!(output_key.starts_with(&format!("data/{}/{task_id}/output/", run.run_id)));
            assert!(store.exists(&output_key).await.expect("output exists"));
        }

        let (stored_state, _) = store.get(&state_key).await.expect("read stored state");
        let stored_run = deserialize_run(&stored_state).expect("deserialize stored state");
        assert_eq!(stored_run.status, RunStatus::Completed);
    }
}
