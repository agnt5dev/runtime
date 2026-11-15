//! Approval projection.
//!
//! Materializes approval state from `approval.requested` and `approval.resolved`
//! journal events into the materialized store.

use engine_core::types::Record;
use engine_proto::Approval;
use prost::Message;

use crate::projections::StoreOp;

const APPROVAL_REQUESTED: &str = "approval.requested";
const APPROVAL_RESOLVED: &str = "approval.resolved";

/// Project a record into approval store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        APPROVAL_REQUESTED => project_requested(record),
        APPROVAL_RESOLVED => project_resolved(record, existing),
        _ => vec![],
    }
}

fn project_requested(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let approval_id = match meta.get("approval_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut approval = Approval {
        approval_id: approval_id.clone(),
        run_id: record.run_id.clone(),
        project_id: record.project_id.clone(),
        status: "pending".to_string(),
        requested_at: Some(ns_to_timestamp(record.timestamp_ns)),
        ..Default::default()
    };

    if let Some(v) = meta.get("name") {
        approval.name = v.clone();
    }
    if let Some(v) = meta.get("dedupe_id") {
        approval.dedupe_id = v.clone();
    }

    // Parse payload for structured fields
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(p) = payload.get("payload") {
            if let Ok(bytes) = serde_json::to_vec(p) {
                approval.payload = bytes;
            }
        }
        if let Some(roles) = payload.get("required_roles").and_then(|v| v.as_array()) {
            approval.required_roles = roles
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(channels) = payload.get("notify_channels").and_then(|v| v.as_array()) {
            approval.notify_channels = channels
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(expires) = payload.get("expires_at_ns").and_then(|v| v.as_i64()) {
            approval.expires_at = Some(ns_to_timestamp(expires));
        }
    }

    vec![StoreOp::PutApproval {
        approval_id,
        run_id: record.run_id.clone(),
        data: approval.encode_to_vec(),
    }]
}

fn project_resolved(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let approval_id = match meta.get("approval_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut approval = existing
        .and_then(|bytes| Approval::decode(bytes).ok())
        .unwrap_or_else(|| Approval {
            approval_id: approval_id.clone(),
            run_id: record.run_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        });

    // Extract decision from metadata or payload
    if let Some(status) = meta.get("decision") {
        approval.status = status.clone();
    }
    if let Some(by) = meta.get("decided_by") {
        approval.decided_by = by.clone();
    }
    approval.decided_at = Some(ns_to_timestamp(record.timestamp_ns));

    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(reason) = payload.get("reason").and_then(|v| v.as_str()) {
            approval.decision_reason = reason.to_string();
        }
        if let Some(dp) = payload.get("decision_payload") {
            if let Ok(bytes) = serde_json::to_vec(dp) {
                approval.decision_payload = bytes;
            }
        }
        // Fallback: decision from payload if not in metadata
        if approval.status == "pending" || approval.status.is_empty() {
            if let Some(d) = payload.get("decision").and_then(|v| v.as_str()) {
                approval.status = d.to_string();
            }
        }
    }

    vec![StoreOp::PutApproval {
        approval_id,
        run_id: approval.run_id.clone(),
        data: approval.encode_to_vec(),
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
    fn requested_creates_pending_approval() {
        let mut meta = HashMap::new();
        meta.insert("approval_id".to_string(), "apr-1".to_string());
        meta.insert("name".to_string(), "deploy_prod".to_string());

        let record = make_record(
            "approval.requested",
            r#"{"required_roles":["admin"],"notify_channels":["slack"]}"#,
            meta,
        );
        let ops = project(&record, None);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutApproval {
                approval_id, data, ..
            } => {
                assert_eq!(approval_id, "apr-1");
                let a = Approval::decode(data.as_slice()).unwrap();
                assert_eq!(a.status, "pending");
                assert_eq!(a.name, "deploy_prod");
                assert_eq!(a.required_roles, vec!["admin"]);
                assert_eq!(a.notify_channels, vec!["slack"]);
            }
            other => panic!("expected PutApproval, got {:?}", other),
        }
    }

    #[test]
    fn resolved_updates_status() {
        // Create existing pending approval
        let pending = Approval {
            approval_id: "apr-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            status: "pending".to_string(),
            name: "deploy_prod".to_string(),
            ..Default::default()
        };
        let existing = pending.encode_to_vec();

        let mut meta = HashMap::new();
        meta.insert("approval_id".to_string(), "apr-1".to_string());
        meta.insert("decision".to_string(), "approved".to_string());
        meta.insert("decided_by".to_string(), "user-42".to_string());

        let record = make_record("approval.resolved", r#"{"reason":"looks good"}"#, meta);
        let ops = project(&record, Some(&existing));
        match &ops[0] {
            StoreOp::PutApproval { data, .. } => {
                let a = Approval::decode(data.as_slice()).unwrap();
                assert_eq!(a.status, "approved");
                assert_eq!(a.decided_by, "user-42");
                assert_eq!(a.decision_reason, "looks good");
                assert_eq!(a.name, "deploy_prod"); // preserved from existing
                assert!(a.decided_at.is_some());
            }
            other => panic!("expected PutApproval, got {:?}", other),
        }
    }

    #[test]
    fn missing_approval_id_skipped() {
        let ops = project(
            &make_record("approval.requested", "{}", HashMap::new()),
            None,
        );
        assert!(ops.is_empty());
    }

    #[test]
    fn non_approval_event_ignored() {
        let ops = project(&make_record("run.queued", "{}", HashMap::new()), None);
        assert!(ops.is_empty());
    }
}
