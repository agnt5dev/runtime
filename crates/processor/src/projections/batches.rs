//! Batch orchestration projection.
//!
//! Materializes batch state from journal events:
//! `batch.created`, `batch.started`, `batch.item_completed`, `batch.item_failed`,
//! `batch.completed`, `batch.cancelled`

use engine_core::types::Record;
use engine_proto::Batch;
use prost::Message;

use crate::projections::StoreOp;

/// Project a record into batch store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        "batch.created" => project_created(record),
        "batch.started"
        | "batch.item_completed"
        | "batch.item_failed"
        | "batch.completed"
        | "batch.cancelled" => project_update(record, existing),
        _ => vec![],
    }
}

fn project_created(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let batch_id = match meta.get("batch_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut batch = Batch {
        batch_id: batch_id.clone(),
        project_id: record.project_id.clone(),
        status: "pending".to_string(),
        submitted_at: Some(ns_to_timestamp(record.timestamp_ns)),
        ..Default::default()
    };

    if let Some(v) = meta.get("service_name") {
        batch.service_name = v.clone();
    }
    if let Some(v) = meta.get("component_type") {
        batch.component_type = v.clone();
    }
    if let Some(v) = meta.get("component_name") {
        batch.component_name = v.clone();
    }
    if let Some(v) = meta.get("trace_id") {
        batch.trace_id = v.clone();
    }

    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(v) = payload.get("total_items").and_then(|v| v.as_i64()) {
            batch.total_items = v as i32;
        }
        if let Some(v) = payload.get("max_concurrency").and_then(|v| v.as_i64()) {
            batch.max_concurrency = v as i32;
        }
        if let Some(v) = payload.get("continue_on_failure").and_then(|v| v.as_bool()) {
            batch.continue_on_failure = v;
        }
        if let Some(v) = payload.get("batch_timeout_ms").and_then(|v| v.as_i64()) {
            batch.batch_timeout_ms = v;
        }
        if let Some(v) = payload
            .get("default_item_timeout_ms")
            .and_then(|v| v.as_i64())
        {
            batch.default_item_timeout_ms = v;
        }
        if let Some(m) = payload.get("metadata") {
            if let Ok(bytes) = serde_json::to_vec(m) {
                batch.metadata = bytes;
            }
        }
    }

    vec![StoreOp::PutBatch {
        batch_id,
        data: batch.encode_to_vec(),
    }]
}

fn project_update(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let batch_id = match meta.get("batch_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut batch = existing
        .and_then(|bytes| Batch::decode(bytes).ok())
        .unwrap_or_else(|| Batch {
            batch_id: batch_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        });

    match record.event_type.as_str() {
        "batch.started" => {
            batch.status = "running".to_string();
            batch.started_at = Some(ns_to_timestamp(record.timestamp_ns));
        }
        "batch.item_completed" => {
            batch.completed_items += 1;
        }
        "batch.item_failed" => {
            batch.failed_items += 1;
        }
        "batch.completed" => {
            // Derive final status
            if batch.failed_items == 0 {
                batch.status = "completed".to_string();
            } else if batch.completed_items > 0 {
                batch.status = "partial_failure".to_string();
            } else {
                batch.status = "failed".to_string();
            }
            batch.completed_at = Some(ns_to_timestamp(record.timestamp_ns));
        }
        "batch.cancelled" => {
            batch.status = "cancelled".to_string();
            batch.completed_at = Some(ns_to_timestamp(record.timestamp_ns));
        }
        _ => {}
    }

    vec![StoreOp::PutBatch {
        batch_id,
        data: batch.encode_to_vec(),
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

    fn batch_meta(id: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("batch_id".to_string(), id.to_string());
        m
    }

    #[test]
    fn created_initializes_batch() {
        let mut meta = batch_meta("b1");
        meta.insert("component_name".to_string(), "my_func".to_string());
        let ops = project(
            &make_record(
                "batch.created",
                r#"{"total_items":10,"max_concurrency":5}"#,
                meta,
            ),
            None,
        );
        match &ops[0] {
            StoreOp::PutBatch { data, .. } => {
                let b = Batch::decode(data.as_slice()).unwrap();
                assert_eq!(b.status, "pending");
                assert_eq!(b.total_items, 10);
                assert_eq!(b.max_concurrency, 5);
                assert_eq!(b.component_name, "my_func");
            }
            other => panic!("expected PutBatch, got {:?}", other),
        }
    }

    #[test]
    fn item_completed_increments() {
        let existing = Batch {
            batch_id: "b1".into(),
            completed_items: 3,
            ..Default::default()
        };
        let ops = project(
            &make_record("batch.item_completed", "{}", batch_meta("b1")),
            Some(&existing.encode_to_vec()),
        );
        match &ops[0] {
            StoreOp::PutBatch { data, .. } => {
                assert_eq!(Batch::decode(data.as_slice()).unwrap().completed_items, 4);
            }
            other => panic!("expected PutBatch, got {:?}", other),
        }
    }

    #[test]
    fn completed_derives_status() {
        let existing = Batch {
            batch_id: "b1".into(),
            completed_items: 8,
            failed_items: 2,
            ..Default::default()
        };
        let ops = project(
            &make_record("batch.completed", "{}", batch_meta("b1")),
            Some(&existing.encode_to_vec()),
        );
        match &ops[0] {
            StoreOp::PutBatch { data, .. } => {
                assert_eq!(
                    Batch::decode(data.as_slice()).unwrap().status,
                    "partial_failure"
                );
            }
            other => panic!("expected PutBatch, got {:?}", other),
        }
    }
}
