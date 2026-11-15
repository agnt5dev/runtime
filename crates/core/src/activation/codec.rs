//! Canonical JSON encoding and the journal envelope for activation records.
//!
//! A record's `event_type` is the boundary the user sees, derived from the
//! activation kind: `workflow.step.completed`, `lm.started`,
//! `tool_call.failed`, `agent.suspended`. What makes it an activation record
//! is the metadata marker, never the name. A marker without a valid canonical
//! payload is a contract violation and is rejected at append.

use std::collections::HashMap;

use bytes::Bytes;
use engine_proto::ActivationKind;
use sha2::{Digest, Sha256};

use super::payload::{b64, ActivationJournalRecord, Event, ACTIVATION_JOURNAL_VERSION};
use super::{validate_journal_record_payload, DURABLE_ACTIVATION_V2_CAPABILITY};
use crate::error::ActivationError;
use crate::types::{Offset, Record};

pub const META_ACTIVATION_CONTRACT: &str = "activation_contract";
pub const META_ACTIVATION_ID: &str = "activation_id";
pub const META_ACTIVATION_ATTEMPT: &str = "activation_attempt";
pub const META_ACTIVATION_TRANSITION: &str = "activation_transition";
pub const META_ACTIVATION_KIND: &str = "activation_kind";
const META_DISPLAY_PARENT: &str = "display_parent_correlation_id";

pub const TRANSITION_STARTED: &str = "started";
pub const TRANSITION_RETRY_STARTED: &str = "retry_started";
pub const TRANSITION_COMPLETED: &str = "completed";
pub const TRANSITION_FAILED: &str = "failed";
pub const TRANSITION_SUSPENDED: &str = "suspended";
pub const TRANSITION_CANCELLED: &str = "cancelled";
pub const TRANSITION_UNKNOWN_OUTCOME: &str = "unknown_outcome";
pub const TRANSITION_SNAPSHOT: &str = "snapshot";

/// Every transition label, in the order metrics report them.
pub const ALL_TRANSITIONS: &[&str] = &[
    TRANSITION_STARTED,
    TRANSITION_RETRY_STARTED,
    TRANSITION_COMPLETED,
    TRANSITION_FAILED,
    TRANSITION_SUSPENDED,
    TRANSITION_CANCELLED,
    TRANSITION_UNKNOWN_OUTCOME,
    TRANSITION_SNAPSHOT,
];

/// Kinds that have a producer today. Metrics and diagnostics enumerate these.
pub const LIVE_ACTIVATION_KINDS: &[ActivationKind] = &[
    ActivationKind::Step,
    ActivationKind::Model,
    ActivationKind::Tool,
    ActivationKind::Child,
    ActivationKind::Timer,
];

/// The event-type prefix an activation kind is journaled under.
pub fn event_prefix(kind: i32) -> Result<&'static str, ActivationError> {
    match ActivationKind::try_from(kind).unwrap_or(ActivationKind::Unspecified) {
        ActivationKind::Step | ActivationKind::Timer => Ok("workflow.step"),
        ActivationKind::Function => Ok("function"),
        ActivationKind::Model => Ok("lm"),
        ActivationKind::Tool => Ok("tool_call"),
        ActivationKind::Child => Ok("agent"),
        ActivationKind::Approval => Ok("approval"),
        ActivationKind::Eval => Ok("eval"),
        ActivationKind::Unspecified => Err(ActivationError::InvalidArgument(
            "activation kind is required to name the journal record".to_string(),
        )),
    }
}

/// The metrics label for a transition. A retry is still journaled as
/// `.started`; the label keeps the distinction.
pub fn transition_label(event: &Event) -> &'static str {
    match event {
        Event::Started(started) if started.previous_attempt == 0 => TRANSITION_STARTED,
        Event::Started(_) => TRANSITION_RETRY_STARTED,
        Event::Completed(_) => TRANSITION_COMPLETED,
        Event::Failed(_) => TRANSITION_FAILED,
        Event::Suspended(_) => TRANSITION_SUSPENDED,
        Event::Cancelled(_) => TRANSITION_CANCELLED,
        Event::UnknownOutcome(_) => TRANSITION_UNKNOWN_OUTCOME,
        Event::Snapshot(_) => TRANSITION_SNAPSHOT,
    }
}

fn event_suffix(event: &Event) -> &'static str {
    match event {
        Event::Started(_) => "started",
        Event::Completed(_) => "completed",
        Event::Failed(_) => "failed",
        Event::Suspended(_) => "suspended",
        Event::Cancelled(_) => "cancelled",
        Event::UnknownOutcome(_) => "unknown_outcome",
        Event::Snapshot(_) => "snapshot",
    }
}

/// The journal event type for one activation record.
pub fn event_type(kind: i32, event: &Event) -> Result<String, ActivationError> {
    Ok(format!("{}.{}", event_prefix(kind)?, event_suffix(event)))
}

/// Every event type a live activation kind can be journaled under.
pub fn all_event_types() -> Vec<String> {
    let suffixes = [
        "started",
        "completed",
        "failed",
        "suspended",
        "cancelled",
        "unknown_outcome",
        "snapshot",
    ];
    let mut names = Vec::new();
    for kind in LIVE_ACTIVATION_KINDS {
        let prefix = event_prefix(*kind as i32).expect("live kinds are named");
        for suffix in suffixes {
            let name = format!("{prefix}.{suffix}");
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    names
}

/// Whether a record claims to be an activation record. Cheap; used at every
/// branch site. The payload is validated separately at append.
pub fn has_activation_marker(record: &Record) -> bool {
    record
        .metadata
        .get(META_ACTIVATION_CONTRACT)
        .is_some_and(|value| value == DURABLE_ACTIVATION_V2_CAPABILITY)
}

/// `(activation_id, attempt, transition)` from the envelope of a marked
/// record. `None` for unmarked records.
pub fn activation_record_identity(record: &Record) -> Option<(&str, u32, &str)> {
    if !has_activation_marker(record) {
        return None;
    }
    let activation_id = record.metadata.get(META_ACTIVATION_ID)?;
    let attempt = record
        .metadata
        .get(META_ACTIVATION_ATTEMPT)?
        .parse::<u32>()
        .ok()?;
    let transition = record.metadata.get(META_ACTIVATION_TRANSITION)?;
    Some((activation_id, attempt, transition))
}

/// Sort every object recursively so the encoding is independent of the
/// serializer's field order.
fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<(String, serde_json::Value)> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, canonicalize(value));
            }
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

pub fn encode_journal_record(record: &ActivationJournalRecord) -> Vec<u8> {
    let value = serde_json::to_value(record).expect("activation record serializes");
    serde_json::to_vec(&canonicalize(value)).expect("canonical JSON serializes")
}

/// Decode canonical JSON bytes. Any deviation from the canonical encoding
/// (whitespace, key order, unknown or missing fields, a different serializer)
/// fails, so two records with the same meaning always have the same bytes.
pub fn decode_journal_record(bytes: &[u8]) -> Result<ActivationJournalRecord, ActivationError> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|error| {
        ActivationError::InvalidArgument(format!("invalid activation journal JSON: {error}"))
    })?;
    let decoded: ActivationJournalRecord = serde_json::from_value(value).map_err(|error| {
        ActivationError::InvalidArgument(format!("invalid activation journal payload: {error}"))
    })?;
    if encode_journal_record(&decoded) != bytes {
        return Err(ActivationError::InvalidArgument(
            "activation journal bytes are not canonical JSON".to_string(),
        ));
    }
    Ok(decoded)
}

fn journal_identity(record: &ActivationJournalRecord) -> Result<Envelope, ActivationError> {
    let event = record.event.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument("activation journal event is required".to_string())
    })?;
    let (project_id, run_id, activation_id, attempt, parent, stable_key) = match event {
        Event::Started(s) => (
            &s.project_id,
            &s.run_id,
            &s.activation_id,
            s.attempt,
            &s.parent_activation_id,
            &s.stable_key,
        ),
        Event::Completed(v) => (
            &v.project_id,
            &v.run_id,
            &v.activation_id,
            v.attempt,
            &v.header.parent_activation_id,
            &v.header.stable_key,
        ),
        Event::Failed(v) => (
            &v.project_id,
            &v.run_id,
            &v.activation_id,
            v.attempt,
            &v.header.parent_activation_id,
            &v.header.stable_key,
        ),
        Event::Suspended(v) => (
            &v.project_id,
            &v.run_id,
            &v.activation_id,
            v.attempt,
            &v.header.parent_activation_id,
            &v.header.stable_key,
        ),
        Event::Cancelled(v) => (
            &v.project_id,
            &v.run_id,
            &v.activation_id,
            v.attempt,
            &v.header.parent_activation_id,
            &v.header.stable_key,
        ),
        Event::UnknownOutcome(v) => {
            let f = v.failure.as_ref().ok_or_else(|| {
                ActivationError::InvalidArgument("unknown outcome is missing failure".to_string())
            })?;
            (
                &f.project_id,
                &f.run_id,
                &f.activation_id,
                f.attempt,
                &f.header.parent_activation_id,
                &f.header.stable_key,
            )
        }
        Event::Snapshot(v) => {
            let s = v.current_attempt.as_ref().ok_or_else(|| {
                ActivationError::InvalidArgument("snapshot is missing current_attempt".to_string())
            })?;
            (
                &v.project_id,
                &v.run_id,
                &v.activation_id,
                s.attempt,
                &s.parent_activation_id,
                &s.stable_key,
            )
        }
    };
    let kind = event.activation_kind().ok_or_else(|| {
        ActivationError::InvalidArgument("activation record is missing its kind".to_string())
    })?;
    Ok(Envelope {
        event_type: event_type(kind, event)?,
        transition: transition_label(event),
        kind,
        project_id: project_id.clone(),
        run_id: run_id.clone(),
        activation_id: activation_id.clone(),
        attempt,
        parent_activation_id: parent.clone(),
        stable_key: stable_key.clone(),
    })
}

struct Envelope {
    event_type: String,
    transition: &'static str,
    kind: i32,
    project_id: String,
    run_id: String,
    activation_id: String,
    attempt: u32,
    parent_activation_id: String,
    stable_key: String,
}

/// Wrap a canonical activation payload in its journal envelope.
pub fn to_engine_record(
    record: &ActivationJournalRecord,
    timestamp_ns: i64,
) -> Result<Record, ActivationError> {
    validate_journal_record_payload(record)?;
    let envelope = journal_identity(record)?;
    let encoded = encode_journal_record(record);
    let checksum = Bytes::copy_from_slice(&Sha256::digest(&encoded));
    let mut metadata = HashMap::new();
    metadata.insert(
        META_ACTIVATION_CONTRACT.to_string(),
        DURABLE_ACTIVATION_V2_CAPABILITY.to_string(),
    );
    metadata.insert(
        META_ACTIVATION_ID.to_string(),
        envelope.activation_id.clone(),
    );
    metadata.insert(
        META_ACTIVATION_ATTEMPT.to_string(),
        envelope.attempt.to_string(),
    );
    metadata.insert(
        META_ACTIVATION_TRANSITION.to_string(),
        envelope.transition.to_string(),
    );
    metadata.insert(META_ACTIVATION_KIND.to_string(), envelope.kind.to_string());
    if let Some(parent) = record
        .event
        .as_ref()
        .and_then(Event::display_parent_correlation_id)
    {
        metadata.insert(META_DISPLAY_PARENT.to_string(), parent.to_string());
    }
    if let Some(Event::Completed(completed)) = record.event.as_ref() {
        if ActivationKind::try_from(completed.header.kind).ok() == Some(ActivationKind::Model) {
            if let Some(usage) = completed.usage.as_ref() {
                stamp_model_usage(&mut metadata, usage);
            }
        }
    }
    Ok(Record {
        offset: Offset::ZERO,
        project_id: envelope.project_id,
        run_id: envelope.run_id,
        event_type: envelope.event_type,
        data: Bytes::from(encoded),
        timestamp_ns,
        step_key: Some(envelope.stable_key),
        correlation_id: Some(envelope.activation_id),
        parent_event_id: (!envelope.parent_activation_id.is_empty())
            .then_some(envelope.parent_activation_id),
        metadata,
        data_type: "json".to_string(),
        data_checksum: Some(checksum),
        data_compressed: false,
    })
}

/// Model usage as metadata, which is what pricing enrichment and the run's
/// `model_usage` projection read.
fn stamp_model_usage(
    metadata: &mut HashMap<String, String>,
    usage: &engine_proto::ActivationUsage,
) {
    if !usage.model.is_empty() {
        metadata.insert("model".to_string(), usage.model.clone());
    }
    if !usage.provider.is_empty() {
        metadata.insert("provider".to_string(), usage.provider.clone());
    }
    metadata.insert("input_tokens".to_string(), usage.tokens_in.to_string());
    metadata.insert("output_tokens".to_string(), usage.tokens_out.to_string());
    if usage.cached_tokens > 0 {
        metadata.insert("cached_tokens".to_string(), usage.cached_tokens.to_string());
    }
    if usage.latency_ms > 0 {
        metadata.insert("duration_ms".to_string(), usage.latency_ms.to_string());
    }
}

/// Decode a marked record and check that its envelope matches the canonical
/// payload. `Ok(None)` for unmarked records.
pub fn from_engine_record(
    record: &Record,
) -> Result<Option<ActivationJournalRecord>, ActivationError> {
    if !has_activation_marker(record) {
        return Ok(None);
    }
    let decoded = decode_journal_record(&record.data)?;
    validate_journal_record_payload(&decoded)?;
    let envelope = journal_identity(&decoded)?;
    let expected_parent = (!envelope.parent_activation_id.is_empty())
        .then_some(envelope.parent_activation_id.as_str());
    if envelope.event_type != record.event_type
        || envelope.project_id != record.project_id
        || envelope.run_id != record.run_id
        || record.metadata.get(META_ACTIVATION_ID) != Some(&envelope.activation_id)
        || record
            .metadata
            .get(META_ACTIVATION_ATTEMPT)
            .map(String::as_str)
            != Some(envelope.attempt.to_string().as_str())
        || record
            .metadata
            .get(META_ACTIVATION_TRANSITION)
            .map(String::as_str)
            != Some(envelope.transition)
        || record
            .metadata
            .get(META_ACTIVATION_KIND)
            .map(String::as_str)
            != Some(envelope.kind.to_string().as_str())
        || record.metadata.get(META_DISPLAY_PARENT).map(String::as_str)
            != decoded
                .event
                .as_ref()
                .and_then(Event::display_parent_correlation_id)
        || record.correlation_id.as_deref() != Some(envelope.activation_id.as_str())
        || record.parent_event_id.as_deref() != expected_parent
        || record.step_key.as_deref() != Some(envelope.stable_key.as_str())
        || record.data_type != "json"
        || record.data_compressed
    {
        return Err(ActivationError::InvalidArgument(
            "activation record envelope does not match canonical payload".to_string(),
        ));
    }
    let Some(checksum) = &record.data_checksum else {
        return Err(ActivationError::InvalidArgument(
            "activation record data_checksum is required".to_string(),
        ));
    };
    let actual = Sha256::digest(record.data.as_ref());
    if checksum.as_ref() != actual.as_slice() {
        return Err(ActivationError::PayloadConflict {
            activation_id: envelope.activation_id,
            attempt: envelope.attempt,
        });
    }
    Ok(Some(decoded))
}

/// A marked record must carry a valid canonical payload. Used by append
/// validation so the segment never indexes a forged or corrupt record.
pub fn is_activation_record(record: &Record) -> Result<bool, ActivationError> {
    Ok(from_engine_record(record)?.is_some())
}

/// The reader-facing JSON of an activation record: the stored payload minus
/// the authority secrets, with inline output and error payloads expanded to
/// parsed JSON where they are JSON.
pub fn public_view(record: &Record) -> serde_json::Value {
    let Ok(Some(decoded)) = from_engine_record(record) else {
        return serde_json::Value::Null;
    };
    let mut value = serde_json::to_value(&decoded).unwrap_or(serde_json::Value::Null);
    let Some(event) = value.get_mut("event").and_then(|v| v.as_object_mut()) else {
        return serde_json::Value::Null;
    };
    strip_secrets(event);
    if let Some(failure) = event.get_mut("failure").and_then(|v| v.as_object_mut()) {
        strip_secrets(failure);
        expand_payload(failure, "error_data");
    }
    expand_payload(event, "output");
    expand_payload(event, "error_data");
    if let Some(snapshot_attempt) = event
        .get_mut("current_attempt")
        .and_then(|v| v.as_object_mut())
    {
        strip_secrets(snapshot_attempt);
    }
    for key in ["completion", "failure", "suspension"] {
        if let Some(inner) = event.get_mut(key).and_then(|v| v.as_object_mut()) {
            strip_secrets(inner);
            expand_payload(inner, "output");
            expand_payload(inner, "error_data");
        }
    }
    let event = event.clone();
    serde_json::Value::Object(event)
}

/// The keys an activation record keeps for the fold but never shows a reader.
pub const AUTHORITY_KEYS: &[&str] = &[
    "fence_token",
    "worker_session_id",
    "run_authority",
    "lease_authority",
];

fn strip_secrets(object: &mut serde_json::Map<String, serde_json::Value>) {
    for key in AUTHORITY_KEYS {
        object.remove(*key);
    }
}

/// `output: {inline_b64}` becomes `output_data: <parsed json | string>`;
/// references stay as `output: {reference}`.
fn expand_payload(object: &mut serde_json::Map<String, serde_json::Value>, key: &str) {
    let expanded_key = if key == "output" {
        "output_data".to_string()
    } else {
        key.to_string()
    };
    let Some(inline) = object
        .get(key)
        .and_then(|v| v.get("inline_b64"))
        .and_then(|v| v.as_str())
        .and_then(|v| b64::decode(v).ok())
    else {
        return;
    };
    let expanded = serde_json::from_slice::<serde_json::Value>(&inline)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&inline).into()));
    object.remove(key);
    object.insert(expanded_key, expanded);
}

/// Journal version every new record carries.
pub fn journal_record(event: Event) -> ActivationJournalRecord {
    ActivationJournalRecord {
        version: ACTIVATION_JOURNAL_VERSION,
        event: Some(event),
    }
}
