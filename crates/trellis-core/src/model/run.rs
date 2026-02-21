use crate::model::{RunStatus, Task};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub run_id: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub broker: Option<String>,
    pub total_layers: u32,
    pub current_layer: u32,
    pub tasks: HashMap<String, Task>,
}

impl Run {
    pub fn new(run_id: String) -> Self {
        Self {
            run_id,
            status: RunStatus::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            broker: None,
            total_layers: 0,
            current_layer: 0,
            tasks: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FailurePolicy, TaskStatus};

    #[test]
    fn run_round_trip_with_tasks() {
        let mut run = Run::new("run-1".to_string());

        let task = Task {
            task_id: "extract".to_string(),
            task_type: "extract_data".to_string(),
            status: TaskStatus::Pending,
            on_failure: FailurePolicy::FailDag,
            ..Task::default()
        };

        run.tasks.insert(task.task_id.clone(), task.clone());

        let encoded = serde_json::to_string(&run).expect("serialize run");
        let decoded: Run = serde_json::from_str(&encoded).expect("deserialize run");

        assert_eq!(decoded.run_id, run.run_id);
        assert_eq!(decoded.status, RunStatus::Pending);
        assert_eq!(decoded.tasks.len(), 1);
        assert_eq!(decoded.tasks["extract"], task);
    }
}
