//! Job queue projection.
//!
//! Materializes job state from journal events:
//! `job.enqueued`, `job.claimed`, `job.started`, `job.completed`, `job.failed`, `job.cancelled`

use engine_core::types::Record;
use engine_proto::Job;
use prost::Message;

use crate::projections::StoreOp;

/// Project a record into job store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        "job.enqueued" => project_enqueued(record),
        "job.claimed" | "job.started" | "job.completed" | "job.failed" | "job.cancelled" => {
            project_update(record, existing)
        }
        _ => vec![],
    }
}

fn project_enqueued(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let job_id = match meta.get("job_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut job = Job {
        job_id: job_id.clone(),
        run_id: record.run_id.clone(),
        project_id: record.project_id.clone(),
        status: "pending".to_string(),
        created_at: Some(ns_to_timestamp(record.timestamp_ns)),
        ..Default::default()
    };

    if let Some(v) = meta.get("component_id") {
        job.component_id = v.clone();
    }
    if let Some(v) = meta.get("component_name") {
        job.component_name = v.clone();
    }
    if let Some(v) = meta.get("batch_id") {
        job.batch_id = v.clone();
    }
    if let Some(v) = meta.get("batch_index").and_then(|v| v.parse().ok()) {
        job.batch_index = v;
    }
    if let Some(v) = meta.get("priority").and_then(|v| v.parse().ok()) {
        job.priority = v;
    }
    if let Some(v) = meta.get("timeout_ms").and_then(|v| v.parse().ok()) {
        job.timeout_ms = v;
    }
    if let Some(v) = meta.get("max_attempts").and_then(|v| v.parse().ok()) {
        job.max_attempts = v;
    }
    if let Some(v) = meta.get("trace_id") {
        job.trace_id = v.clone();
    }

    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(input) = payload.get("input_data") {
            if let Ok(bytes) = serde_json::to_vec(input) {
                job.input_data = bytes;
            }
        }
    }

    vec![StoreOp::PutJob {
        job_id,
        run_id: record.run_id.clone(),
        batch_id: job.batch_id.clone(),
        data: job.encode_to_vec(),
    }]
}

fn project_update(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let job_id = match meta.get("job_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut job = existing
        .and_then(|bytes| Job::decode(bytes).ok())
        .unwrap_or_else(|| Job {
            job_id: job_id.clone(),
            run_id: record.run_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        });

    match record.event_type.as_str() {
        "job.claimed" => {
            job.status = "claimed".to_string();
            job.claimed_at = Some(ns_to_timestamp(record.timestamp_ns));
            if let Some(v) = meta.get("claimed_by") {
                job.claimed_by = v.clone();
            }
            if let Some(v) = meta.get("attempt").and_then(|v| v.parse().ok()) {
                job.attempt = v;
            }
        }
        "job.started" => {
            job.status = "started".to_string();
        }
        "job.completed" => {
            job.status = "completed".to_string();
            job.completed_at = Some(ns_to_timestamp(record.timestamp_ns));
        }
        "job.failed" => {
            job.status = "failed".to_string();
            job.completed_at = Some(ns_to_timestamp(record.timestamp_ns));
            if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
                if let Some(msg) = payload.get("error_message").and_then(|v| v.as_str()) {
                    job.error_message = msg.to_string();
                }
            }
        }
        "job.cancelled" => {
            job.status = "cancelled".to_string();
            job.completed_at = Some(ns_to_timestamp(record.timestamp_ns));
        }
        _ => {}
    }

    vec![StoreOp::PutJob {
        job_id,
        run_id: job.run_id.clone(),
        batch_id: job.batch_id.clone(),
        data: job.encode_to_vec(),
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
    fn enqueued_creates_job() {
        let mut meta = HashMap::new();
        meta.insert("job_id".to_string(), "job-1".to_string());
        meta.insert("component_id".to_string(), "comp-1".to_string());
        meta.insert("batch_id".to_string(), "batch-1".to_string());
        meta.insert("priority".to_string(), "50".to_string());
        let ops = project(
            &make_record("job.enqueued", r#"{"input_data":{"x":1}}"#, meta),
            None,
        );
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutJob {
                job_id,
                batch_id,
                data,
                ..
            } => {
                assert_eq!(job_id, "job-1");
                assert_eq!(batch_id, "batch-1");
                let j = Job::decode(data.as_slice()).unwrap();
                assert_eq!(j.status, "pending");
                assert_eq!(j.priority, 50);
            }
            other => panic!("expected PutJob, got {:?}", other),
        }
    }

    #[test]
    fn completed_updates_status() {
        let existing = Job {
            job_id: "job-1".into(),
            run_id: "r1".into(),
            project_id: "t1".into(),
            status: "started".into(),
            batch_id: "b1".into(),
            ..Default::default()
        };
        let mut meta = HashMap::new();
        meta.insert("job_id".to_string(), "job-1".to_string());
        let ops = project(
            &make_record("job.completed", "{}", meta),
            Some(&existing.encode_to_vec()),
        );
        match &ops[0] {
            StoreOp::PutJob { data, .. } => {
                let j = Job::decode(data.as_slice()).unwrap();
                assert_eq!(j.status, "completed");
            }
            other => panic!("expected PutJob, got {:?}", other),
        }
    }

    #[test]
    fn missing_job_id_skipped() {
        assert!(project(&make_record("job.enqueued", "{}", HashMap::new()), None).is_empty());
    }
}
