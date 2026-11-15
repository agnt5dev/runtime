//! Inbound-event hot-index projection.
//!
//! Materializes a newest-first, project-scoped index of received
//! `event.received` records so the gateway can list "events a project
//! received" without scanning the journal. See `CF_EVENTS_HOT`.

use engine_core::types::Record;
use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::projections::StoreOp;

const EVENT_RECEIVED: &str = "event.received";
const LEGACY_USER_EVENT_RECEIVED: &str = "user_event.received";

/// Compact summary stored as the `CF_EVENTS_HOT` row value. Mirrors the
/// list-item shape the Events page renders; the full event (incl. payload)
/// is hydrated separately via `read_by_run_id`.
#[derive(Debug, Serialize)]
struct EventSummary<'a> {
    event_id: &'a str,
    event_name: &'a str,
    source: &'a str,
    deployment_id: &'a str,
    timestamp_ns: i64,
    matched_count: usize,
    skipped_unsupported_count: usize,
    dispatch_kind: &'a str,
    bound_component_name: &'a str,
    bound_component_type: &'a str,
    status: &'a str,
    error_message: &'a str,
}

/// Project an `event.received` record into an events-hot index op.
/// Every other record type yields no ops.
pub fn project(record: &Record) -> Vec<StoreOp> {
    if record.event_type != EVENT_RECEIVED && record.event_type != LEGACY_USER_EVENT_RECEIVED {
        return Vec::new();
    }

    // The full StoredEvent is the record payload (JSON). Parse only
    // the fields the summary needs; a malformed payload is logged and
    // skipped rather than poisoning the batch.
    let data: Value = match serde_json::from_slice(&record.data) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                run_id = %record.run_id,
                error = %e,
                "event.received payload not JSON; events-hot entry skipped"
            );
            return Vec::new();
        }
    };

    let timestamp_ns = data
        .get("timestamp_ns")
        .and_then(Value::as_i64)
        .unwrap_or(record.timestamp_ns);
    let matched_count = data
        .get("matched_triggers")
        .and_then(Value::as_array)
        .map(|a| a.len())
        .filter(|count| *count > 0)
        .or_else(|| {
            data.get("run_ids")
                .and_then(Value::as_array)
                .map(|a| a.len())
        })
        .unwrap_or(0);
    let summary = EventSummary {
        event_id: data.get("event_id").and_then(Value::as_str).unwrap_or(""),
        event_name: data.get("event_name").and_then(Value::as_str).unwrap_or(""),
        source: data.get("source").and_then(Value::as_str).unwrap_or(""),
        deployment_id: data
            .get("deployment_id")
            .and_then(Value::as_str)
            .unwrap_or(""),
        timestamp_ns,
        matched_count,
        skipped_unsupported_count: data
            .get("skipped_unsupported_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
        dispatch_kind: data
            .get("dispatch_kind")
            .and_then(Value::as_str)
            .unwrap_or("trigger"),
        bound_component_name: data
            .get("bound_component_name")
            .and_then(Value::as_str)
            .unwrap_or(""),
        bound_component_type: data
            .get("bound_component_type")
            .and_then(Value::as_str)
            .unwrap_or(""),
        status: data
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("received"),
        error_message: data
            .get("error_message")
            .and_then(Value::as_str)
            .unwrap_or(""),
    };

    let summary_bytes = match serde_json::to_vec(&summary) {
        Ok(b) => b,
        Err(e) => {
            warn!(run_id = %record.run_id, error = %e, "failed to encode event summary");
            return Vec::new();
        }
    };

    vec![StoreOp::PutEventHotIndex {
        // Inverted-ms ordering in the key is at second-ish granularity is
        // fine; we store ms (ns / 1e6) to match the runs hot index.
        timestamp_ms: timestamp_ns / 1_000_000,
        summary: summary_bytes,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::types::{Offset, Record};

    fn make_record(event_type: &str, data: &str) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "proj-1".to_string(),
            run_id: "evt-run-1".to_string(),
            event_type: event_type.to_string(),
            data: data.as_bytes().to_vec().into(),
            timestamp_ns: 1_711_000_000_000_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: Default::default(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    #[test]
    fn ignores_non_event_records() {
        let ops = project(&make_record("run.queued", "{}"));
        assert!(ops.is_empty());
    }

    #[test]
    fn emits_events_hot_for_event_received() {
        let data = r#"{
            "event_id": "e1",
            "event_name": "user.created",
            "source": "stripe",
            "deployment_id": "dep-1",
            "timestamp_ns": 1711000000000000000,
            "matched_triggers": [{"trigger_id":"t1","component_type":1,"component_name":"c"}],
            "skipped_unsupported_count": 0,
            "dispatch_kind": "trigger",
            "status": "received"
        }"#;
        let ops = project(&make_record("event.received", data));
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutEventHotIndex {
                timestamp_ms,
                summary,
            } => {
                assert_eq!(*timestamp_ms, 1_711_000_000_000);
                let v: Value = serde_json::from_slice(summary).unwrap();
                assert_eq!(v["event_name"], "user.created");
                assert_eq!(v["matched_count"], 1);
                assert_eq!(v["dispatch_kind"], "trigger");
                assert_eq!(v["status"], "received");
            }
            other => panic!("unexpected op: {other:?}"),
        }
    }

    #[test]
    fn emits_events_hot_for_bound_dispatch() {
        let data = r#"{
            "event_id": "e1",
            "event_name": "slack.app_mention",
            "source": "slack",
            "deployment_id": "dep-1",
            "timestamp_ns": 1711000000000000000,
            "matched_triggers": [],
            "run_ids": ["run-1"],
            "dispatch_kind": "binding",
            "bound_component_name": "support_bot",
            "bound_component_type": "agent",
            "skipped_unsupported_count": 0,
            "status": "received"
        }"#;
        let ops = project(&make_record("event.received", data));
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutEventHotIndex { summary, .. } => {
                let v: Value = serde_json::from_slice(summary).unwrap();
                assert_eq!(v["matched_count"], 1);
                assert_eq!(v["dispatch_kind"], "binding");
                assert_eq!(v["bound_component_name"], "support_bot");
            }
            other => panic!("unexpected op: {other:?}"),
        }
    }

    #[test]
    fn accepts_legacy_user_event_received_records() {
        let data = r#"{
            "event_id": "e1",
            "event_name": "user.created",
            "source": "api",
            "deployment_id": "",
            "timestamp_ns": 1711000000000000000,
            "matched_triggers": [],
            "skipped_unsupported_count": 0
        }"#;
        let ops = project(&make_record("user_event.received", data));
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn malformed_payload_yields_no_ops() {
        let ops = project(&make_record("event.received", "not json"));
        assert!(ops.is_empty());
    }
}
