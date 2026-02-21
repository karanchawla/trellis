use crate::model::Run;
use bytes::Bytes;

pub fn serialize_run(run: &Run) -> Result<Bytes, serde_json::Error> {
    serde_json::to_vec(run).map(Bytes::from)
}

pub fn deserialize_run(data: &[u8]) -> Result<Run, serde_json::Error> {
    serde_json::from_slice(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DagBuilder, FailurePolicy, TaskStatus};
    use serde_json::Value;

    #[test]
    fn round_trip_preserves_structure_and_nulls() {
        let mut dag = DagBuilder::new("state-serde");
        dag.task("extract").task_type("extract");
        for idx in 0..9 {
            let task_id = format!("transform-{idx}");
            dag.task(&task_id)
                .task_type("transform")
                .depends_on(["extract"])
                .on_failure(FailurePolicy::Continue);
        }
        let mut run = dag.build().expect("build run");
        run.tasks
            .get_mut("extract")
            .expect("extract task")
            .input_refs
            .insert("_input".to_string(), None);
        run.tasks
            .get_mut("transform-0")
            .expect("transform task")
            .status = TaskStatus::Blocked;

        let encoded = serialize_run(&run).expect("serialize run");
        let decoded = deserialize_run(&encoded).expect("deserialize run");
        assert_eq!(decoded, run);

        let json_value: Value = serde_json::from_slice(&encoded).expect("decode json value");
        assert!(json_value.get("run_id").is_some());
        assert!(json_value.get("created_at").is_some());
        assert!(json_value.get("tasks").is_some());
        assert_eq!(
            json_value["tasks"]["extract"]["input_refs"]["_input"],
            Value::Null
        );
    }
}
