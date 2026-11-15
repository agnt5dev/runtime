pub mod activations;
pub mod approvals;
pub mod batches;
pub mod events;
pub mod jobs;
pub mod messages;
pub mod runs;
pub mod sessions;
pub mod signals;
pub mod state;
pub mod timers;

pub use engine_core::storage_contract::ProjectionMutation as StoreOp;

/// Result of applying projections to a record.
pub struct ProjectionResult {
    pub ops: Vec<StoreOp>,
}

impl ProjectionResult {
    /// Find the first PutRun op and return the data.
    pub fn run_data(&self) -> Option<&[u8]> {
        self.ops.iter().find_map(|op| match op {
            StoreOp::PutRun(data) => Some(data.as_slice()),
            _ => None,
        })
    }

    /// Find the first PutStepMemo op and return (key, data).
    pub fn step_memo(&self) -> Option<(&str, &[u8])> {
        self.ops.iter().find_map(|op| match op {
            StoreOp::PutStepMemo { step_key, data } => Some((step_key.as_str(), data.as_slice())),
            _ => None,
        })
    }

    /// Check if any PutEvent op exists.
    pub fn has_store_event(&self) -> bool {
        self.ops.iter().any(|op| matches!(op, StoreOp::PutEvent))
    }
}
