use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    RunQueued {
        project_id: String,
        run_id: String,
        component_type: String,
        component_name: String,
        input_data: Vec<u8>,
        submitted_at_ms: i64,
    },
    JobClaimed {
        project_id: String,
        run_id: String,
        worker_id: String,
        lease_id: String,
        lease_expires_at_ms: i64,
    },
    JobReclaimed {
        project_id: String,
        run_id: String,
        worker_id: String,
        previous_lease_id: String,
        lease_id: String,
        lease_expires_at_ms: i64,
    },
    JobLeaseRenewed {
        project_id: String,
        run_id: String,
        lease_id: String,
        lease_expires_at_ms: i64,
    },
    JobCompleted {
        project_id: String,
        run_id: String,
        lease_id: String,
        output_data: Vec<u8>,
        completed_at_ms: i64,
    },
    JobFailed {
        project_id: String,
        run_id: String,
        lease_id: String,
        error_message: String,
        error_code: String,
        completed_at_ms: i64,
    },
}

impl RuntimeEvent {
    pub fn run_id(&self) -> &str {
        match self {
            Self::RunQueued { run_id, .. }
            | Self::JobClaimed { run_id, .. }
            | Self::JobReclaimed { run_id, .. }
            | Self::JobLeaseRenewed { run_id, .. }
            | Self::JobCompleted { run_id, .. }
            | Self::JobFailed { run_id, .. } => run_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunState {
    pub project_id: String,
    pub run_id: String,
    pub component_type: String,
    pub component_name: String,
    pub status: String,
    pub input_data: Vec<u8>,
    pub output_data: Option<Vec<u8>>,
    pub error_message: Option<String>,
    pub error_code: Option<String>,
    pub submitted_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub worker_id: Option<String>,
    pub lease_id: Option<String>,
    pub lease_expires_at_ms: Option<i64>,
    pub attempt: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PendingJob {
    pub project_id: String,
    pub run_id: String,
    pub component_type: String,
    pub component_name: String,
    pub input_data: Vec<u8>,
}
