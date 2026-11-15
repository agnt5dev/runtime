//! Session projection.
//!
//! Materializes session state from `session.created` and `session.updated` events.

use engine_core::types::Record;
use engine_proto::Session;
use prost::Message;

use crate::projections::StoreOp;

/// Project a record into session store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        "session.created" => project_created(record),
        "session.updated" => project_updated(record, existing),
        _ => vec![],
    }
}

fn project_created(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let session_id = match meta.get("session_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let component_name = meta.get("component_name").cloned().unwrap_or_default();

    let mut session = Session {
        session_id: session_id.clone(),
        project_id: record.project_id.clone(),
        component_name: component_name.clone(),
        created_at: Some(ns_to_timestamp(record.timestamp_ns)),
        updated_at: Some(ns_to_timestamp(record.timestamp_ns)),
        ..Default::default()
    };

    if let Some(v) = meta.get("session_type") {
        session.session_type = v.clone();
    }
    if let Some(v) = meta.get("service_name") {
        session.service_name = v.clone();
    }

    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(s) = payload.get("state") {
            if let Ok(bytes) = serde_json::to_vec(s) {
                session.state = bytes;
            }
        }
        if let Some(m) = payload.get("metadata") {
            if let Ok(bytes) = serde_json::to_vec(m) {
                session.metadata = bytes;
            }
        }
        if let Some(expires) = payload.get("expires_at_ns").and_then(|v| v.as_i64()) {
            session.expires_at = Some(ns_to_timestamp(expires));
        }
    }

    vec![StoreOp::PutSession {
        session_id,
        component_name,
        data: session.encode_to_vec(),
    }]
}

fn project_updated(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let session_id = match meta.get("session_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut session = existing
        .and_then(|bytes| Session::decode(bytes).ok())
        .unwrap_or_else(|| Session {
            session_id: session_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        });

    session.updated_at = Some(ns_to_timestamp(record.timestamp_ns));
    session.last_used_at = Some(ns_to_timestamp(record.timestamp_ns));

    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(s) = payload.get("state") {
            if let Ok(bytes) = serde_json::to_vec(s) {
                session.state = bytes;
            }
        }
        if let Some(m) = payload.get("metadata") {
            if let Ok(bytes) = serde_json::to_vec(m) {
                session.metadata = bytes;
            }
        }
    }

    vec![StoreOp::PutSession {
        session_id,
        component_name: session.component_name.clone(),
        data: session.encode_to_vec(),
    }]
}

fn ns_to_timestamp(ns: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ns / 1_000_000_000,
        nanos: (ns % 1_000_000_000) as i32,
    }
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
    fn created_initializes_session() {
        let mut meta = HashMap::new();
        meta.insert("session_id".to_string(), "sess-1".to_string());
        meta.insert("component_name".to_string(), "chatbot".to_string());
        meta.insert("session_type".to_string(), "agent".to_string());
        let ops = project(
            &make_record("session.created", r#"{"state":{"turn":0}}"#, meta),
            None,
        );
        match &ops[0] {
            StoreOp::PutSession {
                session_id,
                component_name,
                data,
            } => {
                assert_eq!(session_id, "sess-1");
                assert_eq!(component_name, "chatbot");
                let s = Session::decode(data.as_slice()).unwrap();
                assert_eq!(s.session_type, "agent");
                assert!(!s.state.is_empty());
            }
            other => panic!("expected PutSession, got {:?}", other),
        }
    }

    #[test]
    fn updated_merges_state() {
        let existing = Session {
            session_id: "sess-1".into(),
            project_id: "t1".into(),
            component_name: "chatbot".into(),
            session_type: "agent".into(),
            state: br#"{"turn":0}"#.to_vec(),
            ..Default::default()
        };
        let mut meta = HashMap::new();
        meta.insert("session_id".to_string(), "sess-1".to_string());
        let ops = project(
            &make_record("session.updated", r#"{"state":{"turn":1}}"#, meta),
            Some(&existing.encode_to_vec()),
        );
        match &ops[0] {
            StoreOp::PutSession { data, .. } => {
                let s = Session::decode(data.as_slice()).unwrap();
                assert_eq!(s.session_type, "agent"); // preserved
                assert!(s.last_used_at.is_some());
            }
            other => panic!("expected PutSession, got {:?}", other),
        }
    }
}
