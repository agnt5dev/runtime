//! Entity state projection.
//!
//! Materializes entity state from `workflow.state.changed` journal events.
//! Each event carries the full state snapshot (not a delta) along with
//! entity identity and version in metadata.

use engine_core::types::Record;

use crate::projections::StoreOp;

/// Event type emitted when entity state is saved.
const STATE_CHANGED_EVENT: &str = "workflow.state.changed";

/// Project a record into entity state ops.
///
/// Returns a `PutEntityState` op if this is a state change event
/// with the required metadata fields, otherwise returns empty.
pub fn project(record: &Record) -> Vec<StoreOp> {
    if record.event_type != STATE_CHANGED_EVENT {
        return vec![];
    }

    let meta = &record.metadata;

    let entity_type = match meta.get("entity_type") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };
    let entity_key = match meta.get("entity_key") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let scope = meta
        .get("scope")
        .cloned()
        .unwrap_or_else(|| "global".to_string());
    let scope_id = meta.get("scope_id").cloned().unwrap_or_default();

    let version = meta
        .get("new_version")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1);

    vec![StoreOp::PutEntityState {
        entity_type,
        entity_key,
        scope,
        scope_id,
        data: record.data.to_vec(),
        version,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use engine_core::types::Offset;
    use std::collections::HashMap;

    fn make_record(event_type: &str, data: &str, metadata: HashMap<String, String>) -> Record {
        Record {
            offset: Offset(0),
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::from(data.to_string()),
            timestamp_ns: 1_000_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    #[test]
    fn state_changed_produces_put_op() {
        let mut meta = HashMap::new();
        meta.insert("entity_type".to_string(), "ShoppingCart".to_string());
        meta.insert("entity_key".to_string(), "cart-123".to_string());
        meta.insert("scope".to_string(), "global".to_string());
        meta.insert("scope_id".to_string(), String::new());
        meta.insert("new_version".to_string(), "3".to_string());

        let record = make_record(
            "workflow.state.changed",
            r#"{"items":["apple","banana"]}"#,
            meta,
        );

        let ops = project(&record);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutEntityState {
                entity_type,
                entity_key,
                scope,
                scope_id,
                data,
                version,
            } => {
                assert_eq!(entity_type, "ShoppingCart");
                assert_eq!(entity_key, "cart-123");
                assert_eq!(scope, "global");
                assert_eq!(scope_id, "");
                assert_eq!(data, br#"{"items":["apple","banana"]}"#);
                assert_eq!(*version, 3);
            }
            other => panic!("expected PutEntityState, got {:?}", other),
        }
    }

    #[test]
    fn non_state_event_ignored() {
        let ops = project(&make_record("run.queued", "{}", HashMap::new()));
        assert!(ops.is_empty());
    }

    #[test]
    fn missing_entity_type_skipped() {
        let mut meta = HashMap::new();
        meta.insert("entity_key".to_string(), "k1".to_string());

        let record = make_record("workflow.state.changed", "{}", meta);
        assert!(project(&record).is_empty());
    }

    #[test]
    fn missing_entity_key_skipped() {
        let mut meta = HashMap::new();
        meta.insert("entity_type".to_string(), "Cart".to_string());

        let record = make_record("workflow.state.changed", "{}", meta);
        assert!(project(&record).is_empty());
    }

    #[test]
    fn defaults_scope_to_global() {
        let mut meta = HashMap::new();
        meta.insert("entity_type".to_string(), "Agent".to_string());
        meta.insert("entity_key".to_string(), "a1".to_string());
        // No scope or scope_id

        let record = make_record("workflow.state.changed", r#"{"x":1}"#, meta);
        let ops = project(&record);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutEntityState {
                scope, scope_id, ..
            } => {
                assert_eq!(scope, "global");
                assert_eq!(scope_id, "");
            }
            _ => panic!("expected PutEntityState"),
        }
    }

    #[test]
    fn defaults_version_to_one() {
        let mut meta = HashMap::new();
        meta.insert("entity_type".to_string(), "Cart".to_string());
        meta.insert("entity_key".to_string(), "c1".to_string());
        // No new_version

        let record = make_record("workflow.state.changed", "{}", meta);
        let ops = project(&record);
        match &ops[0] {
            StoreOp::PutEntityState { version, .. } => assert_eq!(*version, 1),
            _ => panic!("expected PutEntityState"),
        }
    }

    #[test]
    fn run_scoped_state() {
        let mut meta = HashMap::new();
        meta.insert("entity_type".to_string(), "state".to_string());
        meta.insert("entity_key".to_string(), "kv".to_string());
        meta.insert("scope".to_string(), "run".to_string());
        meta.insert("scope_id".to_string(), "run-abc-123".to_string());
        meta.insert("new_version".to_string(), "1".to_string());

        let record = make_record("workflow.state.changed", r#"{"phase":"research"}"#, meta);

        let ops = project(&record);
        match &ops[0] {
            StoreOp::PutEntityState {
                entity_type,
                entity_key,
                scope,
                scope_id,
                ..
            } => {
                assert_eq!(entity_type, "state");
                assert_eq!(entity_key, "kv");
                assert_eq!(scope, "run");
                assert_eq!(scope_id, "run-abc-123");
            }
            _ => panic!("expected PutEntityState"),
        }
    }
}
