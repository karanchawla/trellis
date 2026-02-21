use crate::model::{FailurePolicy, RetryPolicy, TaskStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub task_type: String,
    pub layer: u32,
    pub depends_on: Vec<String>,
    pub deps_remaining: usize,
    pub downstream: Vec<String>,
    pub input_refs: HashMap<String, Option<String>>,
    pub status: TaskStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub timeout_seconds: u32,
    pub max_retries: u32,
    pub retry_count: u32,
    pub retry_policy: RetryPolicy,
    pub attempt: u32,
    pub worker_id: Option<String>,
    pub lease_id: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub on_failure: FailurePolicy,
    pub output_ref: Option<String>,
}

impl Default for Task {
    fn default() -> Self {
        Self {
            task_id: String::new(),
            task_type: String::new(),
            layer: 0,
            depends_on: Vec::new(),
            deps_remaining: 0,
            downstream: Vec::new(),
            input_refs: HashMap::new(),
            status: TaskStatus::Blocked,
            started_at: None,
            available_at: Utc::now(),
            completed_at: None,
            last_error: None,
            timeout_seconds: 300,
            max_retries: 3,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_match_mvp_expectations() {
        let t = Task::default();
        assert_eq!(t.timeout_seconds, 300);
        assert_eq!(t.max_retries, 3);
        assert_eq!(t.on_failure, FailurePolicy::FailDag);
        assert_eq!(t.status, TaskStatus::Blocked);
        assert!(t.started_at.is_none());
    }

    #[test]
    fn serde_round_trip_with_input_refs() {
        let mut t = Task {
            task_id: "extract".to_string(),
            task_type: "extract_data".to_string(),
            status: TaskStatus::Pending,
            ..Task::default()
        };
        t.input_refs.insert(
            "_input".to_string(),
            Some("data/run/extract/input".to_string()),
        );
        t.input_refs.insert("transform_a".to_string(), None);

        let raw = serde_json::to_value(&t).expect("serialize task");
        assert_eq!(raw["input_refs"]["_input"], json!("data/run/extract/input"));
        assert_eq!(raw["input_refs"]["transform_a"], serde_json::Value::Null);

        let back: Task = serde_json::from_value(raw).expect("deserialize task");
        assert_eq!(back.task_id, t.task_id);
        assert_eq!(back.task_type, t.task_type);
        assert_eq!(
            back.input_refs.get("_input"),
            Some(&Some("data/run/extract/input".to_string()))
        );
        assert_eq!(back.input_refs.get("transform_a"), Some(&None));
    }
}
