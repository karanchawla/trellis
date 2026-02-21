use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use trellis_core::{DagBuilder, FailurePolicy, LocalExecutor, RunStatus, TaskStatus};

#[test]
fn etl_pipeline_executes_in_dependency_order_with_correct_data_flow() {
    let mut dag = DagBuilder::new("etl-pipeline");

    dag.task("extract")
        .task_type("extract")
        .on_failure(FailurePolicy::FailDag);
    dag.task("transform-a")
        .task_type("transform")
        .depends_on(["extract"]);
    dag.task("transform-b")
        .task_type("transform")
        .depends_on(["extract"]);
    dag.task("merge")
        .task_type("merge")
        .depends_on(["transform-a", "transform-b"]);
    dag.task("load").task_type("load").depends_on(["merge"]);

    let mut run = dag.build().expect("build ETL DAG");

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
                let records = inputs["_input"]["records"].clone();
                Ok(serde_json::json!({"records": records}))
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
                let records = inputs["extract"]["records"]
                    .as_array()
                    .expect("records array from extract")
                    .iter()
                    .map(|v| v.as_i64().expect("numeric record") + 1)
                    .collect::<Vec<_>>();
                Ok(serde_json::json!({"records": records}))
            }),
        );
    }

    {
        let order = Arc::clone(&order);
        executor.register(
            "merge",
            Box::new(move |inputs| {
                order.lock().expect("lock order").push("merge".to_string());
                let mut left = inputs["transform-a"]["records"]
                    .as_array()
                    .expect("left records")
                    .clone();
                let mut right = inputs["transform-b"]["records"]
                    .as_array()
                    .expect("right records")
                    .clone();
                left.append(&mut right);
                Ok(serde_json::json!({"records": left}))
            }),
        );
    }

    {
        let order = Arc::clone(&order);
        executor.register(
            "load",
            Box::new(move |inputs| {
                order.lock().expect("lock order").push("load".to_string());
                let count = inputs["merge"]["records"]
                    .as_array()
                    .expect("merged records")
                    .len();
                Ok(serde_json::json!({"loaded_count": count}))
            }),
        );
    }

    let mut inputs = HashMap::new();
    inputs.insert(
        "extract".to_string(),
        serde_json::json!({"records": [1, 2, 3]}),
    );

    executor.execute(&mut run, inputs).expect("execute ETL run");

    let executed = order.lock().expect("lock order").clone();
    assert_eq!(executed.len(), 5);
    assert_eq!(executed[0], "extract");
    assert_eq!(executed[1], "transform");
    assert_eq!(executed[2], "transform");
    assert_eq!(executed[3], "merge");
    assert_eq!(executed[4], "load");

    assert_eq!(run.status, RunStatus::Completed);
    assert!(
        run.tasks
            .values()
            .all(|task| task.status == TaskStatus::Completed)
    );
    assert!(run.tasks.values().all(|task| task.started_at.is_some()));

    assert_eq!(run.tasks["extract"].layer, 0);
    assert_eq!(run.tasks["transform-a"].layer, 1);
    assert_eq!(run.tasks["transform-b"].layer, 1);
    assert_eq!(run.tasks["merge"].layer, 2);
    assert_eq!(run.tasks["load"].layer, 3);
}
