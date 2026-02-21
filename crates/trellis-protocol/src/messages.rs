use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    LeaseMismatch,
    BrokerReplaced,
    CasConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Claim {
        worker_id: String,
        task_types: Vec<String>,
    },
    Complete {
        task_id: String,
        lease_id: String,
        output_ref: String,
    },
    Fail {
        task_id: String,
        lease_id: String,
        error: String,
        retryable: bool,
    },
    Heartbeat {
        task_id: String,
        lease_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BrokerMessage {
    Assigned {
        task_id: String,
        task_type: String,
        lease_id: String,
        timeout_seconds: u32,
        input_refs: HashMap<String, String>,
    },
    NoWork,
    Ack,
    Rejected {
        reason: RejectionReason,
    },
}
