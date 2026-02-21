use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;
use trellis_client::Client;
use trellis_core::{DagBuilder, Run, RunStatus, S3Executor, TaskStatus};
use trellis_store::{LocalFsStore, ObjectStore, read_state};

#[tokio::test]
async fn submit_execute_status_end_to_end_with_localfs() {
    let temp = tempdir().expect("create temp dir");
    let store = Arc::new(LocalFsStore::new(temp.path()));
    let client = Client::new(store.clone());

    let mut dag = DagBuilder::new("sprint2-e2e");
    dag.task("extract").task_type("extract");
    dag.task("transform")
        .task_type("transform")
        .depends_on(["extract"]);
    dag.task("validate")
        .task_type("validate")
        .depends_on(["transform"]);
    dag.task("load").task_type("load").depends_on(["validate"]);

    let mut inputs = HashMap::new();
    inputs.insert(
        "extract".to_string(),
        serde_json::json!({ "records": [1, 2, 3, 4] }),
    );
    let run_id = client.submit(dag, inputs).await.expect("submit");

    let mut run = read_state::<Run>(store.as_ref(), &run_id)
        .await
        .expect("read submitted run")
        .run;

    let mut executor = S3Executor::new(store.clone());
    executor.register(
        "extract",
        Box::new(|inputs| {
            std::thread::sleep(Duration::from_millis(30));
            Ok(serde_json::json!({ "records": inputs["_input"]["records"] }))
        }),
    );
    executor.register(
        "transform",
        Box::new(|inputs| {
            std::thread::sleep(Duration::from_millis(30));
            let out = inputs["extract"]["records"]
                .as_array()
                .expect("records array")
                .iter()
                .map(|v| v.as_i64().expect("i64") * 2)
                .collect::<Vec<_>>();
            Ok(serde_json::json!({ "records": out }))
        }),
    );
    executor.register(
        "validate",
        Box::new(|inputs| {
            std::thread::sleep(Duration::from_millis(30));
            let valid = inputs["transform"]["records"]
                .as_array()
                .expect("records array")
                .iter()
                .all(|v| v.as_i64().expect("i64") > 0);
            Ok(serde_json::json!({ "valid": valid, "records": inputs["transform"]["records"] }))
        }),
    );
    executor.register(
        "load",
        Box::new(|inputs| {
            std::thread::sleep(Duration::from_millis(30));
            let count = inputs["validate"]["records"]
                .as_array()
                .expect("records array")
                .len();
            Ok(serde_json::json!({ "loaded_count": count }))
        }),
    );

    let execution = tokio::spawn(async move {
        executor.execute(&mut run).await.expect("execute run");
        run
    });

    let mut saw_running = false;
    while !execution.is_finished() {
        let snapshot = client.status(&run_id).await.expect("poll status");
        if snapshot.status == RunStatus::Running {
            saw_running = true;
        }
        sleep(Duration::from_millis(10)).await;
    }
    let final_run = execution.await.expect("join execution");
    assert!(saw_running);
    assert_eq!(final_run.status, RunStatus::Completed);
    assert!(
        final_run
            .tasks
            .values()
            .all(|task| task.status == TaskStatus::Completed)
    );

    let final_snapshot = client.status(&run_id).await.expect("final status");
    assert_eq!(final_snapshot.status, RunStatus::Completed);
    assert_eq!(final_snapshot.total_layers, 4);

    for task_id in ["extract", "transform", "validate", "load"] {
        let output_key = final_run.tasks[task_id]
            .output_ref
            .clone()
            .expect("output_ref");
        assert!(output_key.starts_with(&format!("data/{run_id}/{task_id}/output/")));
        assert!(store.exists(&output_key).await.expect("output exists"));
    }
}
