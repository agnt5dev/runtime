//! Signal projection.
//!
//! Materializes signal state from `signal.received` and `signal.acked`
//! journal events into the materialized store.

use engine_core::types::Record;
use engine_proto::Signal;
use prost::Message;

use crate::projections::StoreOp;

const SIGNAL_RECEIVED: &str = "signal.received";
const SIGNAL_ACKED: &str = "signal.acked";

/// Project a record into signal store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        SIGNAL_RECEIVED => project_received(record),
        SIGNAL_ACKED => project_acked(record, existing),
        _ => vec![],
    }
}

fn project_received(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let signal_id = match meta.get("signal_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let signal_name = meta.get("signal_name").cloned().unwrap_or_default();
    let waiting_step = meta.get("waiting_step").cloned().unwrap_or_default();

    let mut signal = Signal {
        signal_id: signal_id.clone(),
        run_id: record.run_id.clone(),
        project_id: record.project_id.clone(),
        signal_name: signal_name.clone(),
        waiting_step: waiting_step.clone(),
        status: "received".to_string(),
        received_at: Some(ns_to_timestamp(record.timestamp_ns)),
        ..Default::default()
    };

    if let Some(v) = meta.get("event_id") {
        signal.event_id = v.clone();
    }
    if let Some(v) = meta.get("version").and_then(|v| v.parse::<i32>().ok()) {
        signal.version = v;
    }

    // Parse payload
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(p) = payload.get("payload") {
            if let Ok(bytes) = serde_json::to_vec(p) {
                signal.payload = bytes;
            }
        }
        if let Some(expires) = payload.get("expires_at_ns").and_then(|v| v.as_i64()) {
            signal.expires_at = Some(ns_to_timestamp(expires));
        }
    }

    vec![StoreOp::PutSignal {
        signal_id,
        run_id: record.run_id.clone(),
        signal_name,
        waiting_step,
        data: signal.encode_to_vec(),
    }]
}

fn project_acked(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let signal_id = match meta.get("signal_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut signal = existing
        .and_then(|bytes| Signal::decode(bytes).ok())
        .unwrap_or_else(|| Signal {
            signal_id: signal_id.clone(),
            run_id: record.run_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        });

    signal.status = "acked".to_string();
    signal.acknowledged_at = Some(ns_to_timestamp(record.timestamp_ns));

    if let Some(by) = meta.get("acked_by") {
        signal.acked_by = by.clone();
    }

    vec![StoreOp::PutSignal {
        signal_id,
        run_id: signal.run_id.clone(),
        signal_name: signal.signal_name.clone(),
        waiting_step: signal.waiting_step.clone(),
        data: signal.encode_to_vec(),
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
    fn received_creates_signal() {
        let mut meta = HashMap::new();
        meta.insert("signal_id".to_string(), "sig-1".to_string());
        meta.insert("signal_name".to_string(), "approval_given".to_string());
        meta.insert("waiting_step".to_string(), "step_wait".to_string());

        let record = make_record("signal.received", r#"{"payload":{"approved":true}}"#, meta);
        let ops = project(&record, None);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutSignal {
                signal_id,
                signal_name,
                waiting_step,
                data,
                ..
            } => {
                assert_eq!(signal_id, "sig-1");
                assert_eq!(signal_name, "approval_given");
                assert_eq!(waiting_step, "step_wait");
                let s = Signal::decode(data.as_slice()).unwrap();
                assert_eq!(s.status, "received");
                assert!(!s.payload.is_empty());
            }
            other => panic!("expected PutSignal, got {:?}", other),
        }
    }

    #[test]
    fn acked_updates_status() {
        let existing = Signal {
            signal_id: "sig-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            signal_name: "ready".to_string(),
            waiting_step: "step_wait".to_string(),
            status: "received".to_string(),
            ..Default::default()
        };
        let existing_bytes = existing.encode_to_vec();

        let mut meta = HashMap::new();
        meta.insert("signal_id".to_string(), "sig-1".to_string());
        meta.insert("acked_by".to_string(), "worker-1".to_string());

        let record = make_record("signal.acked", "{}", meta);
        let ops = project(&record, Some(&existing_bytes));
        match &ops[0] {
            StoreOp::PutSignal { data, .. } => {
                let s = Signal::decode(data.as_slice()).unwrap();
                assert_eq!(s.status, "acked");
                assert_eq!(s.acked_by, "worker-1");
                assert_eq!(s.signal_name, "ready"); // preserved
            }
            other => panic!("expected PutSignal, got {:?}", other),
        }
    }

    #[test]
    fn missing_signal_id_skipped() {
        let ops = project(&make_record("signal.received", "{}", HashMap::new()), None);
        assert!(ops.is_empty());
    }

    #[test]
    fn non_signal_event_ignored() {
        let ops = project(&make_record("run.queued", "{}", HashMap::new()), None);
        assert!(ops.is_empty());
    }
}
