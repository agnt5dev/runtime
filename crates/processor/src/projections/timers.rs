//! Timer projection.
//!
//! Materializes timer state from `timer.scheduled`, `timer.fired`,
//! `timer.canceled`, and `timer.expired` journal events.

use engine_core::types::Record;
use engine_proto::Timer;
use prost::Message;

use crate::projections::StoreOp;

const TIMER_SCHEDULED: &str = "timer.scheduled";
const TIMER_FIRED: &str = "timer.fired";
const TIMER_CANCELED: &str = "timer.canceled";
const TIMER_EXPIRED: &str = "timer.expired";

/// Project a record into timer store ops.
pub fn project(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    match record.event_type.as_str() {
        TIMER_SCHEDULED => project_scheduled(record),
        TIMER_FIRED => project_fired(record, existing),
        TIMER_CANCELED => project_canceled(record, existing),
        TIMER_EXPIRED => project_expired(record, existing),
        _ => vec![],
    }
}

fn project_scheduled(record: &Record) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let timer_id = match meta.get("timer_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut timer = Timer {
        timer_id: timer_id.clone(),
        run_id: record.run_id.clone(),
        project_id: record.project_id.clone(),
        status: "scheduled".to_string(),
        ..Default::default()
    };

    if let Some(v) = meta.get("timer_key") {
        timer.timer_key = v.clone();
    }
    if let Some(v) = meta.get("waiting_step") {
        timer.waiting_step = v.clone();
    }
    if let Some(v) = meta.get("component_id") {
        timer.component_id = v.clone();
    }
    if let Some(v) = meta.get("schedule_type") {
        timer.schedule_type = v.clone();
    }
    if let Some(v) = meta.get("recurring") {
        timer.recurring = v == "true";
    }
    if let Some(v) = meta.get("event_id") {
        timer.event_id = v.clone();
    }

    let mut fire_at_ns: i64 = 0;

    // Parse payload for fire_at and schedule_payload
    if let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&record.data) {
        if let Some(fat) = payload.get("fire_at_ns").and_then(|v| v.as_i64()) {
            fire_at_ns = fat;
            timer.fire_at = Some(ns_to_timestamp(fat));
        }
        if let Some(sp) = payload.get("schedule_payload") {
            if let Ok(bytes) = serde_json::to_vec(sp) {
                timer.schedule_payload = bytes;
            }
        }
    }

    vec![StoreOp::PutTimer {
        timer_id,
        run_id: record.run_id.clone(),
        status: "scheduled".to_string(),
        fire_at_ns,
        data: timer.encode_to_vec(),
    }]
}

fn project_fired(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let timer_id = match meta.get("timer_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut timer = load_or_default(&timer_id, record, existing);

    // Idempotent: already fired
    if timer.status == "fired" {
        return vec![];
    }

    let prev_fire_at_ns = timer
        .fire_at
        .as_ref()
        .map_or(0, |ts| ts.seconds * 1_000_000_000 + ts.nanos as i64);

    timer.status = "fired".to_string();
    timer.fired_at = Some(ns_to_timestamp(record.timestamp_ns));

    vec![StoreOp::PutTimer {
        timer_id,
        run_id: timer.run_id.clone(),
        status: "fired".to_string(),
        fire_at_ns: prev_fire_at_ns,
        data: timer.encode_to_vec(),
    }]
}

fn project_canceled(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let timer_id = match meta.get("timer_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut timer = load_or_default(&timer_id, record, existing);

    let prev_fire_at_ns = timer
        .fire_at
        .as_ref()
        .map_or(0, |ts| ts.seconds * 1_000_000_000 + ts.nanos as i64);

    timer.status = "canceled".to_string();
    timer.canceled_at = Some(ns_to_timestamp(record.timestamp_ns));
    if let Some(by) = meta.get("canceled_by") {
        timer.canceled_by = by.clone();
    }

    vec![StoreOp::PutTimer {
        timer_id,
        run_id: timer.run_id.clone(),
        status: "canceled".to_string(),
        fire_at_ns: prev_fire_at_ns,
        data: timer.encode_to_vec(),
    }]
}

fn project_expired(record: &Record, existing: Option<&[u8]>) -> Vec<StoreOp> {
    let meta = &record.metadata;
    let timer_id = match meta.get("timer_id") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => return vec![],
    };

    let mut timer = load_or_default(&timer_id, record, existing);

    let prev_fire_at_ns = timer
        .fire_at
        .as_ref()
        .map_or(0, |ts| ts.seconds * 1_000_000_000 + ts.nanos as i64);

    timer.status = "expired".to_string();
    timer.expired_at = Some(ns_to_timestamp(record.timestamp_ns));

    vec![StoreOp::PutTimer {
        timer_id,
        run_id: timer.run_id.clone(),
        status: "expired".to_string(),
        fire_at_ns: prev_fire_at_ns,
        data: timer.encode_to_vec(),
    }]
}

fn load_or_default(timer_id: &str, record: &Record, existing: Option<&[u8]>) -> Timer {
    existing
        .and_then(|bytes| Timer::decode(bytes).ok())
        .unwrap_or_else(|| Timer {
            timer_id: timer_id.to_string(),
            run_id: record.run_id.clone(),
            project_id: record.project_id.clone(),
            ..Default::default()
        })
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
            timestamp_ns: 2_000_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn timer_meta(timer_id: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("timer_id".to_string(), timer_id.to_string());
        m
    }

    #[test]
    fn scheduled_creates_timer() {
        let mut meta = timer_meta("tmr-1");
        meta.insert("timer_key".to_string(), "sleep_step".to_string());
        meta.insert("schedule_type".to_string(), "delay".to_string());
        meta.insert("waiting_step".to_string(), "step_2".to_string());

        let record = make_record(
            "timer.scheduled",
            r#"{"fire_at_ns":5000000000,"schedule_payload":{"duration_ms":3000}}"#,
            meta,
        );
        let ops = project(&record, None);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            StoreOp::PutTimer {
                timer_id,
                status,
                fire_at_ns,
                data,
                ..
            } => {
                assert_eq!(timer_id, "tmr-1");
                assert_eq!(status, "scheduled");
                assert_eq!(*fire_at_ns, 5_000_000_000);
                let t = Timer::decode(data.as_slice()).unwrap();
                assert_eq!(t.timer_key, "sleep_step");
                assert_eq!(t.schedule_type, "delay");
                assert_eq!(t.waiting_step, "step_2");
                assert!(t.fire_at.is_some());
            }
            other => panic!("expected PutTimer, got {:?}", other),
        }
    }

    #[test]
    fn fired_updates_status() {
        let existing = Timer {
            timer_id: "tmr-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            status: "scheduled".to_string(),
            fire_at: Some(ns_to_timestamp(5_000_000_000)),
            ..Default::default()
        };
        let existing_bytes = existing.encode_to_vec();

        let record = make_record("timer.fired", "{}", timer_meta("tmr-1"));
        let ops = project(&record, Some(&existing_bytes));
        match &ops[0] {
            StoreOp::PutTimer {
                status,
                fire_at_ns,
                data,
                ..
            } => {
                assert_eq!(status, "fired");
                assert_eq!(*fire_at_ns, 5_000_000_000); // for due index removal
                let t = Timer::decode(data.as_slice()).unwrap();
                assert_eq!(t.status, "fired");
                assert!(t.fired_at.is_some());
            }
            other => panic!("expected PutTimer, got {:?}", other),
        }
    }

    #[test]
    fn fired_idempotent() {
        let existing = Timer {
            timer_id: "tmr-1".to_string(),
            status: "fired".to_string(),
            ..Default::default()
        };
        let ops = project(
            &make_record("timer.fired", "{}", timer_meta("tmr-1")),
            Some(&existing.encode_to_vec()),
        );
        assert!(ops.is_empty());
    }

    #[test]
    fn canceled_updates_status() {
        let existing = Timer {
            timer_id: "tmr-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            status: "scheduled".to_string(),
            fire_at: Some(ns_to_timestamp(5_000_000_000)),
            ..Default::default()
        };

        let mut meta = timer_meta("tmr-1");
        meta.insert("canceled_by".to_string(), "user-1".to_string());

        let record = make_record("timer.canceled", "{}", meta);
        let ops = project(&record, Some(&existing.encode_to_vec()));
        match &ops[0] {
            StoreOp::PutTimer { status, data, .. } => {
                assert_eq!(status, "canceled");
                let t = Timer::decode(data.as_slice()).unwrap();
                assert_eq!(t.canceled_by, "user-1");
            }
            other => panic!("expected PutTimer, got {:?}", other),
        }
    }

    #[test]
    fn expired_updates_status() {
        let existing = Timer {
            timer_id: "tmr-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            status: "scheduled".to_string(),
            ..Default::default()
        };

        let record = make_record("timer.expired", "{}", timer_meta("tmr-1"));
        let ops = project(&record, Some(&existing.encode_to_vec()));
        match &ops[0] {
            StoreOp::PutTimer { status, .. } => assert_eq!(status, "expired"),
            other => panic!("expected PutTimer, got {:?}", other),
        }
    }

    #[test]
    fn missing_timer_id_skipped() {
        let ops = project(&make_record("timer.scheduled", "{}", HashMap::new()), None);
        assert!(ops.is_empty());
    }
}
