//! Reserved entity identity for runtime-owned run feedback catalogs.
pub const ENTITY_TYPE: &str = "__agnt5_run_feedback_v1";

pub fn is_feedback_record(record: &crate::types::Record) -> bool {
    record
        .metadata
        .get("entity_type")
        .is_some_and(|value| value == ENTITY_TYPE)
}
