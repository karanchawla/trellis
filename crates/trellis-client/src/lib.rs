mod display;

pub use display::{LayerSummary, RunSnapshot, TaskSnapshot};

use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use trellis_core::{
    DagBuilder, DagValidationError, Run, RunInitOptions, initialize_run, serialize_run,
};
use trellis_store::{ObjectStore, StoreError, read_state, upload_input};
use uuid::Uuid;

pub struct Client {
    store: Arc<dyn ObjectStore>,
}

impl Client {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Validates and submits a DAG run, uploading root inputs and atomically creating state.json.
    pub async fn submit(
        &self,
        dag: DagBuilder,
        inputs: HashMap<String, serde_json::Value>,
    ) -> Result<String, SubmitError> {
        let mut run = dag.build().map_err(SubmitError::DagValidation)?;
        let run_id = Uuid::now_v7().to_string();
        run.run_id = run_id.clone();

        let mut root_input_refs = HashMap::new();
        for (task_id, task) in &run.tasks {
            if !task.depends_on.is_empty() {
                continue;
            }
            let input_ref = if let Some(payload) = inputs.get(task_id) {
                let encoded = serde_json::to_vec(payload)
                    .map_err(|e| SubmitError::Serialization(e.to_string()))?;
                Some(
                    upload_input(
                        self.store.as_ref(),
                        &run.run_id,
                        task_id,
                        bytes::Bytes::from(encoded),
                    )
                    .await?,
                )
            } else {
                None
            };
            root_input_refs.insert(task_id.clone(), input_ref);
        }

        initialize_run(
            &mut run,
            &RunInitOptions {
                root_input_refs,
                reset_run_metadata: true,
            },
        );

        let state_key = format!("runs/{run_id}/state.json");
        let body = serialize_run(&run).map_err(|e| SubmitError::Serialization(e.to_string()))?;
        self.store.put_if_none_match(&state_key, body).await?;
        Ok(run_id)
    }

    /// Fetches the latest run status snapshot from persisted state.json.
    pub async fn status(&self, run_id: &str) -> Result<RunSnapshot, StoreError> {
        let cas = read_state::<Run>(self.store.as_ref(), run_id).await?;
        Ok(RunSnapshot::from_run(&cas.run))
    }
}

#[derive(Debug, Error)]
pub enum SubmitError {
    #[error(transparent)]
    DagValidation(#[from] DagValidationError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::tempdir;
    use trellis_core::{FailurePolicy, TaskStatus, deserialize_run};
    use trellis_store::LocalFsStore;

    #[tokio::test]
    async fn submit_writes_state_and_root_inputs() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(store.clone());

        let mut dag = DagBuilder::new("submit");
        dag.task("extract")
            .task_type("extract")
            .on_failure(FailurePolicy::FailDag);
        dag.task("load").task_type("load").depends_on(["extract"]);

        let mut inputs = HashMap::new();
        inputs.insert(
            "extract".to_string(),
            serde_json::json!({"records": [1,2,3]}),
        );
        inputs.insert("load".to_string(), serde_json::json!({"ignored": true}));

        let run_id = client.submit(dag, inputs).await.expect("submit DAG");
        let state_key = format!("runs/{run_id}/state.json");
        let (state_body, _) = store.get(&state_key).await.expect("read state");
        let run = deserialize_run(&state_body).expect("deserialize run");

        let root_ref = run.tasks["extract"]
            .input_refs
            .get("_input")
            .expect("_input set for root")
            .clone();
        assert!(root_ref.is_some());
        assert_eq!(run.tasks["extract"].status, TaskStatus::Pending);
        assert_eq!(run.tasks["load"].status, TaskStatus::Blocked);
        assert!(!run.tasks["load"].input_refs.contains_key("_input"));
    }

    #[tokio::test]
    async fn submit_generates_unique_run_ids() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(store);

        let mut run_ids = HashSet::new();
        for _ in 0..2 {
            let mut dag = DagBuilder::new("submit");
            dag.task("extract").task_type("extract");
            let run_id = client
                .submit(dag, HashMap::new())
                .await
                .expect("submit DAG");
            run_ids.insert(run_id);
        }
        assert_eq!(run_ids.len(), 2);
    }

    #[tokio::test]
    async fn submit_sets_null_input_for_root_without_payload() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(store.clone());

        let mut dag = DagBuilder::new("submit");
        dag.task("extract").task_type("extract");
        let run_id = client
            .submit(dag, HashMap::new())
            .await
            .expect("submit DAG");

        let state_key = format!("runs/{run_id}/state.json");
        let (state_body, _) = store.get(&state_key).await.expect("read state");
        let run = deserialize_run(&state_body).expect("deserialize run");
        assert_eq!(run.tasks["extract"].input_refs["_input"], None);
    }

    #[tokio::test]
    async fn status_returns_snapshot() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(store.clone());

        let mut dag = DagBuilder::new("status");
        dag.task("extract").task_type("extract");
        dag.task("load").task_type("load").depends_on(["extract"]);
        let run_id = client
            .submit(dag, HashMap::new())
            .await
            .expect("submit DAG");

        let snapshot = client.status(&run_id).await.expect("status");
        assert_eq!(snapshot.run_id, run_id);
        assert_eq!(snapshot.total_layers, 2);
        assert_eq!(snapshot.tasks.len(), 2);
    }

    #[tokio::test]
    async fn snapshot_display_renders_status_and_waiting_dependencies() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(store.clone());

        let mut dag = DagBuilder::new("display");
        dag.task("extract").task_type("extract");
        dag.task("transform")
            .task_type("transform")
            .depends_on(["extract"]);
        let run_id = client
            .submit(dag, HashMap::new())
            .await
            .expect("submit DAG");

        let snapshot = client.status(&run_id).await.expect("status");
        let rendered = snapshot.to_string();
        assert!(rendered.contains("Run"));
        assert!(rendered.contains("Layer 0"));
        assert!(rendered.contains("extract"));
    }
}
