//! Stable contracts and domain types shared by the community and managed
//! AGNT5 runtimes.

mod journal;
mod store;

pub use journal::{
    AppendOutcome, JournalError, JournalRecord, NewJournalRecord, Offset, RecordStream, Segment,
};
pub use store::{KvOp, MaterializedStore};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub subject: String,
    pub project_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeIdentity {
    pub project_id: String,
}

impl RuntimeIdentity {
    pub fn new(project_id: impl Into<String>) -> Result<Self, &'static str> {
        let project_id = project_id.into();
        if project_id.trim().is_empty() {
            return Err("project_id must not be empty");
        }
        Ok(Self { project_id })
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeIdentity;

    #[test]
    fn project_id_is_required() {
        assert!(RuntimeIdentity::new("default").is_ok());
        assert!(RuntimeIdentity::new("  ").is_err());
    }
}
