use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DagValidationError {
    #[error("DAG contains a cycle")]
    CycleDetected,
    #[error("task has missing dependency: {0}")]
    MissingDependency(String),
    #[error("duplicate task id: {0}")]
    DuplicateTaskId(String),
    #[error("task is missing task_type: {0}")]
    MissingTaskType(String),
    #[error("reserved task id prefix '_': {0}")]
    ReservedTaskId(String),
    #[error("invalid task id: {0}")]
    InvalidTaskId(String),
    #[error("DAG must contain at least one task")]
    EmptyDag,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TaskError {
    #[error("retryable task error: {0}")]
    Retryable(String),
    #[error("permanent task error: {0}")]
    Permanent(String),
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("run is not executable in current status: {0}")]
    InvalidRunState(String),
    #[error("no handler registered for task type: {0}")]
    MissingHandler(String),
    #[error("execution deadlocked: {0}")]
    Deadlock(String),
    #[error("executor invariant violated: {0}")]
    InvariantViolation(String),
    #[error(transparent)]
    Store(#[from] trellis_store::StoreError),
    #[error("state serialization error: {0}")]
    StateSerde(String),
}
