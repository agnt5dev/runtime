//! Message projection.
//!
//! Materializes message state from `message.enqueued` and `message.acked` events.

use engine_core::types::Record;
use engine_proto::ChatMessage;
use prost::Message;

use crate::projections::StoreOp;

/// Project a record into message store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        "message.enqueued" => project_enqueued(record),
        "message.acked" => project_acked(record, existing),
        _ => vec![],
    }
}

fn project_enqueued(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let message_id = match meta.get("message_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let correlation_id = meta.get("correlation_id").cloned().unwrap_or_default();

    let mut msg = ChatMessage {
        message_id: message_id.clone(),
        project_id: record.project_id.clone(),
        status: "pending".to_string(),
        correlation_id: correlation_id.clone(),
        created_at: Some(ns_to_timestamp(record.timestamp_ns)),
        ..Default::default()
    };

    if let Some(v) = meta.get("message_type") {
        msg.message_type = v.clone();
    }
    if let Some(v) = meta.get("message_key") {
        msg.message_key = v.clone();
    }
    if let Some(v) = meta.get("from_service") {
        msg.from_service = v.clone();
    }
    if let Some(v) = meta.get("to_service") {
        msg.to_service = v.clone();
    }
    if let Some(v) = meta.get("reply_to") {
        msg.reply_to = v.clone();
    }
    if let Some(v) = meta.get("trace_id") {
        msg.trace_id = v.clone();
    }
    if let Some(v) = meta.get("priority").and_then(|v| v.parse().ok()) {
        msg.priority = v;
    }

    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(p) = payload.get("payload") {
            if let Ok(bytes) = serde_json::to_vec(p) {
                msg.payload = bytes;
            }
        }
        if let Some(h) = payload.get("headers") {
            if let Ok(bytes) = serde_json::to_vec(h) {
                msg.headers = bytes;
            }
        }
        if let Some(sched) = payload.get("scheduled_at_ns").and_then(|v| v.as_i64()) {
            msg.scheduled_at = Some(ns_to_timestamp(sched));
        }
    }

    vec![StoreOp::PutMessage {
        message_id,
        correlation_id,
        data: msg.encode_to_vec(),
    }]
}

fn project_acked(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let message_id = match meta.get("message_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut msg = existing
        .and_then(|bytes| ChatMessage::decode(bytes).ok())
        .unwrap_or_else(|| ChatMessage {
            message_id: message_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        });

    msg.status = "delivered".to_string();
    msg.delivered_at = Some(ns_to_timestamp(record.timestamp_ns));

    vec![StoreOp::PutMessage {
        message_id,
        correlation_id: msg.correlation_id.clone(),
        data: msg.encode_to_vec(),
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
    fn enqueued_creates_message() {
        let mut meta = HashMap::new();
        meta.insert("message_id".to_string(), "msg-1".to_string());
        meta.insert("from_service".to_string(), "agent-a".to_string());
        meta.insert("to_service".to_string(), "agent-b".to_string());
        meta.insert("correlation_id".to_string(), "corr-1".to_string());
        let ops = project(
            &make_record("message.enqueued", r#"{"payload":{"text":"hello"}}"#, meta),
            None,
        );
        match &ops[0] {
            StoreOp::PutMessage {
                message_id,
                correlation_id,
                data,
            } => {
                assert_eq!(message_id, "msg-1");
                assert_eq!(correlation_id, "corr-1");
                let m = ChatMessage::decode(data.as_slice()).unwrap();
                assert_eq!(m.status, "pending");
                assert_eq!(m.from_service, "agent-a");
                assert!(!m.payload.is_empty());
            }
            other => panic!("expected PutMessage, got {:?}", other),
        }
    }

    #[test]
    fn acked_updates_status() {
        let existing = ChatMessage {
            message_id: "msg-1".into(),
            project_id: "t1".into(),
            status: "pending".into(),
            correlation_id: "corr-1".into(),
            ..Default::default()
        };
        let mut meta = HashMap::new();
        meta.insert("message_id".to_string(), "msg-1".to_string());
        let ops = project(
            &make_record("message.acked", "{}", meta),
            Some(&existing.encode_to_vec()),
        );
        match &ops[0] {
            StoreOp::PutMessage { data, .. } => {
                let m = ChatMessage::decode(data.as_slice()).unwrap();
                assert_eq!(m.status, "delivered");
                assert!(m.delivered_at.is_some());
            }
            other => panic!("expected PutMessage, got {:?}", other),
        }
    }

    #[test]
    fn missing_message_id_skipped() {
        assert!(project(&make_record("message.enqueued", "{}", HashMap::new()), None).is_empty());
    }
}
