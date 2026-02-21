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
