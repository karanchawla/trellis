use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Blocked,
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Skipped)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    FailDag,
    Continue,
    SkipDownstream,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_status_terminal_states() {
        assert!(!RunStatus::Pending.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
    }

    #[test]
    fn task_status_terminal_states() {
        assert!(!TaskStatus::Blocked.is_terminal());
        assert!(!TaskStatus::Pending.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Skipped.is_terminal());
    }
}
