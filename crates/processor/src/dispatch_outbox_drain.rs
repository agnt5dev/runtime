//! Durable dispatch outbox drainer.
//!
//! The partition processor commits durable dispatch intents as part of normal
//! projection. This drainer runs only on the active partition owner after
//! startup replay is complete. It dispatches rows concurrently so worker
//! holdoff/ack latency does not block the processor's serial projection path.
//!
//! Semantics are intentionally at-least-once: if the runtime crashes after a
//! dispatch reaches a worker but before `run.assigned` is appended, restart will
//! drain the durable row again. Workers/coordinator fencing must continue to use
//! `invocation_id` and leases for idempotency.
//!
//! Durable Activation V1 effect intents live in a separate activation outbox.
//! This drainer consumes only typed `PutDispatchOutbox` rows; it must not infer
//! component dispatch from an arbitrary activation intent `kind`. A versioned
//! adapter may perform that translation explicitly.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use engine_cache::RecordCache;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    DispatchOutboxCursor, DispatchOutboxEntry, DispatchOutboxScanEntry, DispatchOutboxStore,
    OutboxEntryToken,
};
use engine_core::traits::Segment;
use engine_core::types::Record;
use engine_proto::{DispatchComponentRequest, Record as ProtoRecord, Run};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinSet;
use tokio::time::{interval, Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::orchestrator::DispatchEnvelope;
use crate::{DISPATCH_OUTBOX_PARTITION_ID_META, DISPATCH_OUTBOX_QUEUED_OFFSET_META};

const DEFAULT_DISPATCH_OUTBOX_DRAIN_INTERVAL: Duration = Duration::from_millis(100);
const DISPATCH_OUTBOX_SCAN_LIMIT: usize = 512;
const DISPATCH_OUTBOX_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const DISPATCH_OUTBOX_DELAYED_RECHECK_MAX: Duration = Duration::from_secs(60);
const DEFAULT_DISPATCH_MAX_INFLIGHT: usize = 128;
const DEFAULT_DISPATCH_ROUTE_SHARE_DIVISOR: usize = 4;
const DISPATCH_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(60);
const DISPATCH_OUTBOX_DRAIN_INTERVAL_MS_ENV: &str = "AGNT5_DISPATCH_OUTBOX_DRAIN_INTERVAL_MS";
const DISPATCH_OUTBOX_MAX_IN_FLIGHT_ENV: &str = "AGNT5_DISPATCH_OUTBOX_MAX_INFLIGHT_PER_PARTITION";
const DISPATCH_OUTBOX_MAX_IN_FLIGHT_PER_ROUTE_ENV: &str =
    "AGNT5_DISPATCH_OUTBOX_MAX_INFLIGHT_PER_ROUTE";
const DISPATCH_MAX_INFLIGHT_ENV: &str = "AGNT5_DISPATCH_MAX_INFLIGHT";
const ENGINE_PARTITIONS_ENV: &str = "AGNT5_ENGINE_PARTITIONS";
const WORKERLESS_DEBOUNCE_WINDOW_MS_META: &str = "workerless_debounce_window_ms";
const WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META: &str = "workerless_debounce_queued_at_ms";
const GATEWAY_RECEIVED_AT_NS_META: &str = "gateway_received_at_ns";
const WORKERLESS_DEBOUNCE_SUPERSEDED_REASON: &str = "workerless_debounce_superseded";
const DISPATCH_RETRY_SOURCE_CONTEXT_MAX_BYTES: usize = 1_024;
pub(crate) use engine_core::journal_contract::DISPATCH_RETRY_SCHEDULED_EVENT;
pub const DISPATCH_RETRY_ATTEMPT_META: &str = "dispatch_retry_attempt";
pub const DISPATCH_RETRY_NEXT_ATTEMPT_AT_MS_META: &str = "dispatch_retry_next_attempt_at_ms";
pub const DISPATCH_RETRY_SOURCE_EVENT_TYPE_META: &str = "dispatch_retry_source_event_type";
pub const DISPATCH_RETRY_SOURCE_TIMESTAMP_NS_META: &str = "dispatch_retry_source_timestamp_ns";
pub const DISPATCH_RETRY_OUTBOX_INSERTED_AT_MS_META: &str = "dispatch_retry_outbox_inserted_at_ms";
pub const DISPATCH_RETRY_SOURCE_STEP_KEY_META: &str = "dispatch_retry_source_step_key";
pub const DISPATCH_RETRY_SOURCE_CORRELATION_ID_META: &str = "dispatch_retry_source_correlation_id";
pub const DISPATCH_RETRY_SOURCE_PARENT_EVENT_ID_META: &str =
    "dispatch_retry_source_parent_event_id";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchOutboxPayload {
    pub request: Vec<u8>,
    pub queued_timestamp_ns: i64,
    #[serde(default)]
    pub outbox_inserted_at_ms: i64,
    #[serde(default)]
    pub queued_record: Vec<u8>,
    /// Persisted retry schedule. Defaults keep old payloads readable.
    #[serde(default)]
    pub retry_attempt: u32,
    #[serde(default)]
    pub next_attempt_at_ms: i64,
}

pub fn encode_dispatch_outbox_payload(
    request: &DispatchComponentRequest,
    queued_record: &Record,
) -> EngineResult<Vec<u8>> {
    encode_dispatch_outbox_payload_with_schedule(
        request,
        queued_record,
        queued_record.timestamp_ns,
        now_unix_ns() / 1_000_000,
        0,
        0,
    )
}

pub(crate) fn encode_dispatch_outbox_payload_with_schedule(
    request: &DispatchComponentRequest,
    queued_record: &Record,
    queued_timestamp_ns: i64,
    outbox_inserted_at_ms: i64,
    retry_attempt: u32,
    next_attempt_at_ms: i64,
) -> EngineResult<Vec<u8>> {
    let mut request = request.clone();
    crate::orchestrator::strip_workerless_credentials(&mut request.metadata);
    let mut queued_record = queued_record.clone();
    crate::orchestrator::strip_workerless_credentials(&mut queued_record.metadata);
    serde_json::to_vec(&DispatchOutboxPayload {
        request: request.encode_to_vec(),
        queued_timestamp_ns,
        outbox_inserted_at_ms,
        queued_record: queued_record.to_proto().encode_to_vec(),
        retry_attempt,
        next_attempt_at_ms,
    })
    .map_err(|error| EngineError::Storage(format!("encode dispatch outbox payload: {error}")))
}

fn recover_request_metadata_from_source_record(
    request: &mut DispatchComponentRequest,
    source_record: &Record,
) {
    for (key, value) in &source_record.metadata {
        request
            .metadata
            .entry(key.clone())
            .or_insert_with(|| value.clone());
    }
    crate::orchestrator::sanitize_dispatch_generation_metadata(&mut request.metadata);
}

fn stamp_workerless_debounce_queued_at(
    request: &mut DispatchComponentRequest,
    source_record: &mut Record,
) {
    if !request
        .metadata
        .contains_key(WORKERLESS_DEBOUNCE_WINDOW_MS_META)
        && !source_record
            .metadata
            .contains_key(WORKERLESS_DEBOUNCE_WINDOW_MS_META)
    {
        return;
    }
    let Some(queued_at_ms) = workerless_debounce_queued_at_ms(request, source_record) else {
        return;
    };
    let value = queued_at_ms.to_string();
    request
        .metadata
        .entry(WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META.to_string())
        .or_insert_with(|| value.clone());
    source_record
        .metadata
        .entry(WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META.to_string())
        .or_insert(value);
}

fn workerless_debounce_queued_at_ms(
    request: &DispatchComponentRequest,
    source_record: &Record,
) -> Option<i64> {
    metadata_i64(&request.metadata, WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META)
        .or_else(|| {
            metadata_i64(
                &source_record.metadata,
                WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META,
            )
        })
        .or_else(|| {
            metadata_i64(&request.metadata, GATEWAY_RECEIVED_AT_NS_META)
                .or_else(|| metadata_i64(&source_record.metadata, GATEWAY_RECEIVED_AT_NS_META))
                .map(|value| value / 1_000_000)
        })
        .or_else(|| {
            (source_record.timestamp_ns > 0).then_some(source_record.timestamp_ns / 1_000_000)
        })
        .filter(|value| *value > 0)
}

fn metadata_i64(metadata: &HashMap<String, String>, key: &str) -> Option<i64> {
    metadata
        .get(key)
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
}

#[derive(Debug, Clone)]
struct DecodedDispatchOutboxEntry {
    token: OutboxEntryToken,
    partition_id: u32,
    project_id: String,
    queued_offset: u64,
    run_id: String,
    request: DispatchComponentRequest,
    source_record: Record,
    outbox_inserted_at_ms: Option<i64>,
    retry_attempt: u32,
    next_attempt_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DispatchRouteKey {
    project_id: String,
    deployment_id: String,
    component_type: i32,
    component_name: String,
}

impl DecodedDispatchOutboxEntry {
    fn route_key(&self) -> DispatchRouteKey {
        DispatchRouteKey {
            project_id: self.project_id.clone(),
            deployment_id: self
                .request
                .metadata
                .get("deployment_id")
                .or_else(|| self.source_record.metadata.get("deployment_id"))
                .cloned()
                .unwrap_or_default(),
            component_type: self.request.component_type,
            component_name: if self.request.component_name.is_empty() {
                self.source_record
                    .metadata
                    .get("component_name")
                    .cloned()
                    .unwrap_or_default()
            } else {
                self.request.component_name.clone()
            },
        }
    }
}

/// Preserve FIFO within each route while rotating among the routes represented
/// in a scanned page. Low-offset bursts can no longer consume every free slot
/// before another healthy route is considered.
fn fair_route_order(entries: Vec<DecodedDispatchOutboxEntry>) -> Vec<DecodedDispatchOutboxEntry> {
    let mut route_indexes = HashMap::<DispatchRouteKey, usize>::new();
    let mut routes = Vec::<VecDeque<DecodedDispatchOutboxEntry>>::new();
    for entry in entries {
        let route = entry.route_key();
        let route_index = if let Some(index) = route_indexes.get(&route) {
            *index
        } else {
            let index = routes.len();
            route_indexes.insert(route, index);
            routes.push(VecDeque::new());
            index
        };
        routes[route_index].push_back(entry);
    }

    let mut ordered = Vec::with_capacity(routes.iter().map(VecDeque::len).sum());
    loop {
        let mut advanced = false;
        for route in &mut routes {
            if let Some(entry) = route.pop_front() {
                ordered.push(entry);
                advanced = true;
            }
        }
        if !advanced {
            return ordered;
        }
    }
}

#[derive(Debug)]
enum DecodeEntryError {
    MalformedValue {
        token: OutboxEntryToken,
        partition_id: u32,
        project_id: String,
        queued_offset: u64,
        run_id: String,
        reason: String,
    },
}

fn now_unix_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(0)
}

fn age_ms_from_ns(now_ns: i64, timestamp_ns: i64) -> u64 {
    now_ns.saturating_sub(timestamp_ns).max(0) as u64 / 1_000_000
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn dispatch_outbox_max_in_flight() -> usize {
    if let Some(value) = env_usize(DISPATCH_OUTBOX_MAX_IN_FLIGHT_ENV) {
        return value;
    }

    let dispatch_max =
        env_usize(DISPATCH_MAX_INFLIGHT_ENV).unwrap_or(DEFAULT_DISPATCH_MAX_INFLIGHT);
    let partitions = env_usize(ENGINE_PARTITIONS_ENV).unwrap_or(1);
    (dispatch_max / partitions.max(1)).max(1)
}

fn dispatch_outbox_max_in_flight_per_route(max_in_flight: usize) -> usize {
    let isolation_default = max_in_flight
        .div_ceil(DEFAULT_DISPATCH_ROUTE_SHARE_DIVISOR)
        .max(1);
    env_usize(DISPATCH_OUTBOX_MAX_IN_FLIGHT_PER_ROUTE_ENV)
        .unwrap_or(isolation_default)
        .min(max_in_flight.max(1))
        .max(1)
}

fn dispatch_outbox_drain_interval() -> Duration {
    env_u64(DISPATCH_OUTBOX_DRAIN_INTERVAL_MS_ENV)
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_DISPATCH_OUTBOX_DRAIN_INTERVAL)
}

fn record_age_ms(record: &Record) -> Option<u64> {
    if record.timestamp_ns <= 0 {
        return None;
    }
    Some(age_ms_from_ns(now_unix_ns(), record.timestamp_ns))
}

fn synthetic_queued_record(
    partition_id: u32,
    project_id: String,
    queued_offset: u64,
    run_id: String,
    request: &DispatchComponentRequest,
    queued_timestamp_ns: i64,
) -> EngineResult<Record> {
    let mut metadata = request.metadata.clone();
    metadata.insert(
        DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
        partition_id.to_string(),
    );
    metadata.insert(
        DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
        queued_offset.to_string(),
    );

    Ok(Record {
        offset: engine_core::partition_offset::encode_offset(partition_id as usize, queued_offset)?,
        project_id,
        run_id,
        event_type: "run.queued".to_string(),
        data: Bytes::from(request.input_data.clone()),
        timestamp_ns: queued_timestamp_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

fn normalize_source_record(
    mut record: Record,
    partition_id: u32,
    project_id: String,
    queued_offset: u64,
    run_id: String,
) -> EngineResult<Record> {
    record.offset =
        engine_core::partition_offset::encode_offset(partition_id as usize, queued_offset)?;
    record.project_id = project_id;
    record.run_id = run_id;
    // Preserve the event that actually created this dispatch generation.
    // Legacy payloads without an encoded source record are synthesized as
    // run.queued above, while newer retries and lease-expiry redispatches need
    // their source type to derive the authoritative attempt number.
    record.metadata.insert(
        DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
        partition_id.to_string(),
    );
    record.metadata.insert(
        DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
        queued_offset.to_string(),
    );
    Ok(record)
}

/// What the retirement reconciler needs from one outbox row: its project,
/// run, deployment and queued record, or `None` when the row cannot be
/// decoded (the drainer owns those rows).
pub fn outbox_row_for_retirement(
    entry: DispatchOutboxEntry,
) -> Option<(String, String, String, Record)> {
    let decoded = decode_entry(entry).ok()?;
    let deployment_id = decoded.route_key().deployment_id;
    Some((
        decoded.project_id,
        decoded.run_id,
        deployment_id,
        decoded.source_record,
    ))
}

fn decode_entry(
    entry: DispatchOutboxEntry,
) -> Result<DecodedDispatchOutboxEntry, DecodeEntryError> {
    let DispatchOutboxEntry {
        token,
        partition_id,
        project_id,
        queued_offset,
        run_id,
        payload: value,
    } = entry;
    let malformed = |reason: String| DecodeEntryError::MalformedValue {
        token: token.clone(),
        partition_id,
        project_id: project_id.clone(),
        queued_offset,
        run_id: run_id.clone(),
        reason,
    };

    let payload: DispatchOutboxPayload = serde_json::from_slice(&value)
        .map_err(|error| malformed(format!("decode payload json: {error}")))?;
    let request = DispatchComponentRequest::decode(payload.request.as_slice())
        .map_err(|error| malformed(format!("decode dispatch request: {error}")))?;
    let source_record = if payload.queued_record.is_empty() {
        synthetic_queued_record(
            partition_id,
            project_id.clone(),
            queued_offset,
            run_id.clone(),
            &request,
            payload.queued_timestamp_ns,
        )
        .map_err(|error| malformed(format!("encode queued record offset: {error}")))?
    } else {
        let proto = ProtoRecord::decode(payload.queued_record.as_slice())
            .map_err(|error| malformed(format!("decode queued record: {error}")))?;
        normalize_source_record(
            Record::from_proto(proto),
            partition_id,
            project_id.clone(),
            queued_offset,
            run_id.clone(),
        )
        .map_err(|error| malformed(format!("encode queued record offset: {error}")))?
    };

    Ok(DecodedDispatchOutboxEntry {
        token,
        partition_id,
        project_id,
        queued_offset,
        run_id,
        request,
        source_record,
        outbox_inserted_at_ms: (payload.outbox_inserted_at_ms > 0)
            .then_some(payload.outbox_inserted_at_ms),
        retry_attempt: payload.retry_attempt,
        next_attempt_at_ms: payload.next_attempt_at_ms,
    })
}

fn durable_retry_delay(entry: &DecodedDispatchOutboxEntry) -> Duration {
    let exponent = entry.retry_attempt.min(8);
    let base_ms = (DISPATCH_OUTBOX_RETRY_BACKOFF.as_millis() as u64)
        .saturating_mul(1_u64 << exponent)
        .min(DISPATCH_RETRY_BACKOFF_MAX.as_millis() as u64);
    // Stable bounded jitter avoids synchronized retries without process-local
    // randomness making restart behavior unpredictable.
    let jitter_window = (base_ms / 4).max(1);
    let jitter_ms = entry
        .queued_offset
        .wrapping_add(u64::from(entry.retry_attempt))
        % jitter_window;
    Duration::from_millis(
        base_ms
            .saturating_add(jitter_ms)
            .min(DISPATCH_RETRY_BACKOFF_MAX.as_millis() as u64),
    )
}

async fn persist_durable_retry(
    segment: &dyn Segment,
    cache: Option<&Arc<RecordCache>>,
    entry: &DecodedDispatchOutboxEntry,
) -> EngineResult<Instant> {
    let delay = durable_retry_delay(entry);
    let next_attempt_at_ms = (now_unix_ns() / 1_000_000)
        .saturating_add(i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
    let retry_attempt = entry.retry_attempt.saturating_add(1);
    let mut metadata = HashMap::from([
        (
            DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
            entry.partition_id.to_string(),
        ),
        (
            DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
            entry.queued_offset.to_string(),
        ),
        (
            DISPATCH_RETRY_ATTEMPT_META.to_string(),
            retry_attempt.to_string(),
        ),
        (
            DISPATCH_RETRY_NEXT_ATTEMPT_AT_MS_META.to_string(),
            next_attempt_at_ms.to_string(),
        ),
        (
            DISPATCH_RETRY_SOURCE_EVENT_TYPE_META.to_string(),
            entry.source_record.event_type.clone(),
        ),
        (
            DISPATCH_RETRY_SOURCE_TIMESTAMP_NS_META.to_string(),
            entry.source_record.timestamp_ns.to_string(),
        ),
        (
            DISPATCH_RETRY_OUTBOX_INSERTED_AT_MS_META.to_string(),
            entry.outbox_inserted_at_ms.unwrap_or_default().to_string(),
        ),
    ]);
    for (key, value) in [
        (
            DISPATCH_RETRY_SOURCE_STEP_KEY_META,
            entry.source_record.step_key.as_ref(),
        ),
        (
            DISPATCH_RETRY_SOURCE_CORRELATION_ID_META,
            entry.source_record.correlation_id.as_ref(),
        ),
        (
            DISPATCH_RETRY_SOURCE_PARENT_EVENT_ID_META,
            entry.source_record.parent_event_id.as_ref(),
        ),
    ] {
        if let Some(value) =
            value.filter(|value| value.len() <= DISPATCH_RETRY_SOURCE_CONTEXT_MAX_BYTES)
        {
            metadata.insert(key.to_string(), value.clone());
        }
    }
    let retry_record = Record {
        offset: engine_core::types::Offset::ZERO,
        project_id: entry.project_id.clone(),
        run_id: entry.run_id.clone(),
        event_type: DISPATCH_RETRY_SCHEDULED_EVENT.to_string(),
        data: Bytes::new(),
        timestamp_ns: now_unix_ns(),
        step_key: None,
        correlation_id: entry.source_record.correlation_id.clone(),
        parent_event_id: None,
        metadata,
        data_type: "application/vnd.agnt5.dispatch-retry+metadata".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    crate::append_and_cache(segment, retry_record, cache).await?;
    Ok(Instant::now() + delay)
}

async fn delete_raw_entry<M: DispatchOutboxStore + ?Sized>(
    store: &M,
    token: OutboxEntryToken,
) -> EngineResult<()> {
    DispatchOutboxStore::delete_dispatch_intent(store, &token).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunDispatchEligibility {
    Eligible,
    Delayed(Duration),
    Stale,
}

async fn run_dispatch_eligibility<M: DispatchOutboxStore + ?Sized>(
    store: &M,
    project_id: &str,
    run_id: &str,
) -> EngineResult<RunDispatchEligibility> {
    let Some(run_data) = DispatchOutboxStore::load_dispatch_run(store, project_id, run_id).await?
    else {
        return Ok(RunDispatchEligibility::Stale);
    };
    let run = Run::decode(run_data.as_slice())
        .map_err(|error| EngineError::Storage(format!("decode run: {error}")))?;
    if run.status != "queued" {
        return Ok(RunDispatchEligibility::Stale);
    }
    let now_ms = now_unix_ns() / 1_000_000;
    let ready_at_ms = run
        .metadata
        .get(crate::run_state_machine::READY_AT_MS_META)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default();
    if ready_at_ms > now_ms {
        return Ok(RunDispatchEligibility::Delayed(Duration::from_millis(
            ready_at_ms.saturating_sub(now_ms) as u64,
        )));
    }
    Ok(RunDispatchEligibility::Eligible)
}

fn canonical_priority(run: &Run) -> u8 {
    match run.priority {
        0 => 2,
        p => p as u8,
    }
}

fn run_created_at_ms(run: &Run) -> i64 {
    run.submitted_at
        .as_ref()
        .map(|ts| ts.seconds * 1_000 + (ts.nanos as i64) / 1_000_000)
        .unwrap_or(0)
}

async fn ensure_pending_deployment_priority<M: DispatchOutboxStore + ?Sized>(
    store: &M,
    project_id: &str,
    run_id: &str,
) -> EngineResult<bool> {
    let Some(run_data) = DispatchOutboxStore::load_dispatch_run(store, project_id, run_id).await?
    else {
        return Ok(false);
    };
    let run = Run::decode(run_data.as_slice())
        .map_err(|error| EngineError::Storage(format!("decode run: {error}")))?;
    if run.status != "queued" || run.deployment_id.is_empty() {
        return Ok(false);
    }

    let priority = canonical_priority(&run);
    let created_at_ms = run_created_at_ms(&run);
    DispatchOutboxStore::ensure_pending_deployment_run(
        store,
        if run.project_id.is_empty() {
            project_id
        } else {
            &run.project_id
        },
        &run.deployment_id,
        priority,
        created_at_ms,
        created_at_ms,
        &run.run_id,
    )
    .await?;
    Ok(true)
}

/// Dispatch rejection the engine dispatcher returns when every alive worker
/// on the exact route is pull-mode. The drainer keys the pull handoff on it,
/// so the dispatcher must use this constant rather than a free-form message.
pub const PULL_ONLY_ROUTE_MESSAGE: &str = "pull-mode workers available; awaiting PollJob claim";

fn is_pull_mode_dispatch_error(error: &str) -> bool {
    if error == PULL_ONLY_ROUTE_MESSAGE || error.ends_with(PULL_ONLY_ROUTE_MESSAGE) {
        return true;
    }
    // Older peers may wrap or rephrase the message across a forwarded
    // dispatch; keep the lenient match until every node emits the constant.
    let error = error.to_ascii_lowercase();
    error.contains("pull-mode workers available")
        || error.contains("awaiting polljob claim")
        || error.contains("awaiting poll job claim")
}

fn is_workerless_debounce_superseded_error(error: &str) -> bool {
    error
        .to_ascii_lowercase()
        .contains("workerless debounce superseded")
}

fn is_retriable_dispatch_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "dispatch saturated",
        "resource exhausted",
        "capacity exhausted",
        "no headroom",
        "channel full",
        "no worker",
        "no routing worker",
        "routing backend unavailable",
        "target coordinator unavailable",
        "peer coordinator",
        "peer dispatch timed out",
        "coordinator draining",
        "worker disconnected",
        "stream closed",
        "dispatch timeout",
        "deadline",
        "unavailable",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

fn retry_after() -> Instant {
    Instant::now() + DISPATCH_OUTBOX_RETRY_BACKOFF
}

fn build_workerless_debounce_cancelled_record(record: &Record, reason: &str) -> Record {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(0);
    let data = serde_json::json!({
        "reason": WORKERLESS_DEBOUNCE_SUPERSEDED_REASON,
        "message": reason,
    });
    let mut metadata = record.metadata.clone();
    crate::orchestrator::strip_workerless_credentials(&mut metadata);
    metadata.insert(
        "cancel_reason".to_string(),
        WORKERLESS_DEBOUNCE_SUPERSEDED_REASON.to_string(),
    );
    metadata.insert("error_message".to_string(), reason.to_string());

    Record {
        offset: engine_core::types::Offset::ZERO,
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        event_type: "run.cancelled".to_string(),
        data: Bytes::from(serde_json::to_vec(&data).unwrap_or_default()),
        timestamp_ns: now_ns,
        step_key: None,
        correlation_id: record.correlation_id.clone(),
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

fn is_permanent_assigned_append_error(error: &EngineError) -> bool {
    matches!(error, EngineError::InvalidArgument(_))
}

async fn fail_and_delete_permanent_assignment<M: DispatchOutboxStore + ?Sized>(
    entry: &DecodedDispatchOutboxEntry,
    record: &Record,
    store: &M,
    segment: &dyn Segment,
    cache: Option<&Arc<RecordCache>>,
    error: &EngineError,
) -> bool {
    let failed = crate::orchestrator::build_failed_record(
        record,
        &format!("permanent run.assigned validation failure: {error}"),
    );
    if let Err(append_error) = crate::append_and_cache(segment, failed, cache).await {
        error!(
            error = %append_error,
            run_id = %entry.run_id,
            "failed to append run.failed for permanent assignment rejection"
        );
        return false;
    }
    if let Err(delete_error) = delete_raw_entry(store, entry.token.clone()).await {
        warn!(
            error = %delete_error,
            run_id = %entry.run_id,
            "failed to delete dispatch outbox row after permanent assignment rejection"
        );
        return false;
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn dead_letter_malformed_entry(
    segment: Arc<dyn Segment>,
    cache: Option<Arc<RecordCache>>,
    partition_id: u32,
    project_id: String,
    queued_offset: u64,
    run_id: String,
    reason: String,
) -> bool {
    let request = DispatchComponentRequest::default();
    let record = match synthetic_queued_record(
        partition_id,
        project_id.clone(),
        queued_offset,
        run_id.clone(),
        &request,
        now_unix_ns(),
    ) {
        Ok(record) => record,
        Err(error) => {
            warn!(%error, partition_id, queued_offset, "cannot encode malformed dispatch outbox offset");
            return false;
        }
    };
    let failed = crate::orchestrator::build_failed_record(
        &record,
        &format!("malformed dispatch outbox row: {reason}"),
    );
    match crate::append_and_cache(segment.as_ref(), failed, cache.as_ref()).await {
        Ok(_) => true,
        Err(error) => {
            warn!(
                error = %error,
                run_id = %run_id,
                "failed to append run.failed for malformed dispatch outbox row"
            );
            false
        }
    }
}

struct DispatchTaskCompletion {
    token: OutboxEntryToken,
    route: DispatchRouteKey,
    retry_after: Option<Instant>,
}

struct DispatchTaskCompletionGuard {
    token: Option<OutboxEntryToken>,
    route: DispatchRouteKey,
    done_tx: mpsc::UnboundedSender<DispatchTaskCompletion>,
}

impl DispatchTaskCompletionGuard {
    fn complete(mut self, retry_after: Option<Instant>) {
        if let Some(token) = self.token.take() {
            let _ = self.done_tx.send(DispatchTaskCompletion {
                token,
                route: self.route.clone(),
                retry_after,
            });
        }
    }
}

impl Drop for DispatchTaskCompletionGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let _ = self.done_tx.send(DispatchTaskCompletion {
                token,
                route: self.route.clone(),
                retry_after: Some(retry_after()),
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_entry<M: DispatchOutboxStore + ?Sized + 'static>(
    entry: DecodedDispatchOutboxEntry,
    node_id: String,
    store: Arc<M>,
    dispatch_tx: mpsc::Sender<DispatchEnvelope>,
    segment: Arc<dyn Segment>,
    cache: Option<Arc<RecordCache>>,
) -> Option<Instant> {
    let mut record = entry.source_record.clone();
    let row_age_ms = record_age_ms(&record);
    let insert_to_spawn_ms = entry.outbox_inserted_at_ms.map(|inserted_at_ms| {
        (now_unix_ns() / 1_000_000)
            .saturating_sub(inserted_at_ms)
            .max(0) as u64
    });
    if let Some(age_ms) = row_age_ms {
        crate::metrics::record_dispatch_outbox_row_age_at_spawn(
            &record,
            entry.partition_id,
            &node_id,
            "spawned",
            age_ms,
        );
    }
    if let Some(inserted_at_ms) = entry.outbox_inserted_at_ms {
        crate::metrics::record_dispatch_outbox_insert_to_spawn(
            &record,
            entry.partition_id,
            &node_id,
            "spawned",
            inserted_at_ms,
        );
    }
    debug!(
        run_id = %entry.run_id,
        partition_id = entry.partition_id,
        queued_offset = entry.queued_offset,
        row_age_ms = row_age_ms.unwrap_or(0),
        outbox_inserted_at_ms = entry.outbox_inserted_at_ms.unwrap_or(0),
        insert_to_spawn_ms = insert_to_spawn_ms.unwrap_or(0),
        "dispatch outbox row picked up"
    );

    match run_dispatch_eligibility(store.as_ref(), &entry.project_id, &entry.run_id).await {
        Ok(RunDispatchEligibility::Eligible) => {}
        Ok(RunDispatchEligibility::Delayed(delay)) => {
            crate::metrics::record_dispatch_outbox_guardrail(
                &record,
                entry.partition_id,
                &node_id,
                "retry_delayed",
            );
            // The durable wall-clock timestamp remains authoritative. Cap one
            // process-local sleep so malformed/far-future metadata cannot
            // overflow `Instant` and leadership changes recheck promptly.
            return Some(Instant::now() + delay.min(DISPATCH_OUTBOX_DELAYED_RECHECK_MAX));
        }
        Ok(RunDispatchEligibility::Stale) => {
            crate::metrics::record_dispatch_outbox_guardrail(
                &record,
                entry.partition_id,
                &node_id,
                "stale_run_deleted",
            );
            if let Err(error) = delete_raw_entry(store.as_ref(), entry.token.clone()).await {
                warn!(
                    error = %error,
                    run_id = %entry.run_id,
                    "failed to delete stale dispatch outbox row"
                );
                return Some(retry_after());
            }
            return None;
        }
        Err(error) => {
            warn!(
                error = %error,
                run_id = %entry.run_id,
                "failed to inspect run before dispatch outbox drain"
            );
            return Some(retry_after());
        }
    }

    // A push run of a retired deployment is cancelled here, by the only task
    // that dispatches it, so the cancel never races a dispatch.
    let route = entry.route_key();
    if let Some((status, canceller)) = crate::retired_deployments::retired_status_for_dispatch(
        &route.project_id,
        &route.deployment_id,
        record.timestamp_ns / 1_000_000,
        crate::retired_deployments::now_ms(),
    ) {
        let cancelled = crate::retired_deployments::cancellation_record(
            &entry.project_id,
            &entry.run_id,
            &route.deployment_id,
            &route.component_name,
            record
                .metadata
                .get("component_type")
                .map(String::as_str)
                .unwrap_or_default(),
            &record.metadata,
            Some((entry.partition_id, entry.queued_offset)),
            &status,
        );
        if let Err(error) = canceller.cancel_queued_run(cancelled).await {
            warn!(
                error = %error,
                run_id = %entry.run_id,
                "failed to cancel a queued run of a retired deployment; keeping row"
            );
            return Some(retry_after());
        }
        crate::metrics::record_dispatch_outbox_guardrail(
            &record,
            entry.partition_id,
            &node_id,
            "deployment_retired",
        );
        if let Err(error) = delete_raw_entry(store.as_ref(), entry.token.clone()).await {
            warn!(
                error = %error,
                run_id = %entry.run_id,
                "failed to delete dispatch outbox row after cancelling it"
            );
            return Some(retry_after());
        }
        return None;
    }

    let mut request = entry.request.clone();
    recover_request_metadata_from_source_record(&mut request, &record);
    stamp_workerless_debounce_queued_at(&mut request, &mut record);
    request.metadata.insert(
        DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
        entry.partition_id.to_string(),
    );
    request.metadata.insert(
        DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
        entry.queued_offset.to_string(),
    );

    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let envelope = DispatchEnvelope {
        request,
        ack: ack_tx,
    };
    let channel_send_start = std::time::Instant::now();
    match dispatch_tx.send(envelope).await {
        Ok(()) => {
            let channel_send_ms = channel_send_start.elapsed().as_secs_f64() * 1000.0;
            crate::metrics::record_dispatch_channel_send_wait(
                &record,
                entry.partition_id,
                &node_id,
                "sent",
                channel_send_start,
            );
            let ack_wait_start = std::time::Instant::now();
            match ack_rx.await {
                Ok(Ok(lease_fields)) => {
                    let ack_wait_ms = ack_wait_start.elapsed().as_secs_f64() * 1000.0;
                    crate::metrics::record_dispatch_ack_wait(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "accepted",
                        ack_wait_start,
                    );
                    crate::metrics::record_queue_to_dispatch(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "accepted",
                    );
                    info!(
                        run_id = %entry.run_id,
                        partition_id = entry.partition_id,
                        queued_offset = entry.queued_offset,
                        row_age_ms = row_age_ms.unwrap_or(0),
                        insert_to_spawn_ms = insert_to_spawn_ms.unwrap_or(0),
                        channel_send_ms = channel_send_ms,
                        ack_wait_ms = ack_wait_ms,
                        worker_id = lease_fields
                            .as_ref()
                            .map(|fields| fields.lease_worker_id.as_str())
                            .unwrap_or(""),
                        "dispatch outbox accepted"
                    );
                    let assigned =
                        crate::orchestrator::build_assigned_record(&record, lease_fields.as_ref());
                    let assigned_append_start = std::time::Instant::now();
                    match crate::append_and_cache(segment.as_ref(), assigned, cache.as_ref()).await
                    {
                        Ok(_) => {
                            crate::metrics::record_dispatch_assigned_append_wait(
                                &record,
                                entry.partition_id,
                                &node_id,
                                "success",
                                assigned_append_start,
                            );
                            crate::metrics::processor_metrics()
                                .processor_dispatches
                                .add(1, &[]);
                            if let Err(error) =
                                delete_raw_entry(store.as_ref(), entry.token.clone()).await
                            {
                                warn!(
                                    error = %error,
                                    run_id = %entry.run_id,
                                    "failed to delete dispatch outbox after assignment append"
                                );
                                return Some(retry_after());
                            }
                        }
                        Err(error) => {
                            crate::metrics::record_dispatch_assigned_append_wait(
                                &record,
                                entry.partition_id,
                                &node_id,
                                "error",
                                assigned_append_start,
                            );
                            if is_permanent_assigned_append_error(&error) {
                                crate::metrics::record_dispatch_outbox_guardrail(
                                    &record,
                                    entry.partition_id,
                                    &node_id,
                                    "permanent_assignment_rejected",
                                );
                                error!(
                                    error = %error,
                                    run_id = %entry.run_id,
                                    partition_id = entry.partition_id,
                                    queued_offset = entry.queued_offset,
                                    "permanent run.assigned validation failure; failing run and deleting outbox row"
                                );
                                if fail_and_delete_permanent_assignment(
                                    &entry,
                                    &record,
                                    store.as_ref(),
                                    segment.as_ref(),
                                    cache.as_ref(),
                                    &error,
                                )
                                .await
                                {
                                    return None;
                                }
                                return Some(retry_after());
                            }
                            error!(
                                error = %error,
                                run_id = %entry.run_id,
                                "failed to append run.assigned from dispatch outbox"
                            );
                            return Some(retry_after());
                        }
                    }
                }
                Ok(Err(error)) if is_pull_mode_dispatch_error(&error) => {
                    crate::metrics::record_dispatch_ack_wait(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "pull_only_handoff",
                        ack_wait_start,
                    );
                    crate::metrics::record_queue_to_dispatch(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "pull_only_handoff",
                    );
                    match ensure_pending_deployment_priority(
                        store.as_ref(),
                        &entry.project_id,
                        &entry.run_id,
                    )
                    .await
                    {
                        Ok(indexed) => {
                            info!(
                                run_id = %entry.run_id,
                                partition_id = entry.partition_id,
                                queued_offset = entry.queued_offset,
                                indexed,
                                "dispatch outbox handed off to pull worker lane"
                            );
                            if let Err(error) =
                                delete_raw_entry(store.as_ref(), entry.token.clone()).await
                            {
                                warn!(
                                    error = %error,
                                    run_id = %entry.run_id,
                                    "failed to delete dispatch outbox after pull handoff"
                                );
                                return Some(retry_after());
                            }
                        }
                        Err(error) => {
                            warn!(
                                error = %error,
                                run_id = %entry.run_id,
                                "failed to ensure pending deployment priority for pull handoff"
                            );
                            return Some(retry_after());
                        }
                    }
                }
                Ok(Err(error)) if is_workerless_debounce_superseded_error(&error) => {
                    crate::metrics::record_dispatch_ack_wait(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "debounce_superseded",
                        ack_wait_start,
                    );
                    crate::metrics::record_queue_to_dispatch(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "debounce_superseded",
                    );
                    let cancelled = build_workerless_debounce_cancelled_record(&record, &error);
                    if let Err(append_error) =
                        crate::append_and_cache(segment.as_ref(), cancelled, cache.as_ref()).await
                    {
                        error!(
                            error = %append_error,
                            run_id = %entry.run_id,
                            "failed to append run.cancelled after workerless debounce supersession"
                        );
                        return Some(retry_after());
                    } else if let Err(delete_error) =
                        delete_raw_entry(store.as_ref(), entry.token.clone()).await
                    {
                        warn!(
                            error = %delete_error,
                            run_id = %entry.run_id,
                            "failed to delete dispatch outbox after workerless debounce supersession"
                        );
                        return Some(retry_after());
                    }
                }
                Ok(Err(error)) if is_retriable_dispatch_error(&error) => {
                    crate::metrics::record_dispatch_ack_wait(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "retryable_dispatch_error",
                        ack_wait_start,
                    );
                    crate::metrics::record_queue_to_dispatch(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "retryable_dispatch_error",
                    );
                    warn!(
                        error = %error,
                        run_id = %entry.run_id,
                        "dispatch outbox hit retryable dispatch error; keeping row"
                    );
                    return Some(
                        match persist_durable_retry(segment.as_ref(), cache.as_ref(), &entry).await
                        {
                            Ok(retry_at) => retry_at,
                            Err(persist_error) => {
                                warn!(
                                    error = %persist_error,
                                    run_id = %entry.run_id,
                                    "failed to persist dispatch retry schedule"
                                );
                                retry_after()
                            }
                        },
                    );
                }
                Ok(Err(error)) => {
                    crate::metrics::record_dispatch_ack_wait(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "dispatch_error",
                        ack_wait_start,
                    );
                    crate::metrics::record_queue_to_dispatch(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "dispatch_error",
                    );
                    let failed = crate::orchestrator::build_failed_record(&record, &error);
                    if let Err(append_error) =
                        crate::append_and_cache(segment.as_ref(), failed, cache.as_ref()).await
                    {
                        error!(
                            error = %append_error,
                            run_id = %entry.run_id,
                            "failed to append run.failed after dispatch error"
                        );
                        return Some(retry_after());
                    } else if let Err(delete_error) =
                        delete_raw_entry(store.as_ref(), entry.token.clone()).await
                    {
                        warn!(
                            error = %delete_error,
                            run_id = %entry.run_id,
                            "failed to delete dispatch outbox after dispatch error"
                        );
                        return Some(retry_after());
                    }
                }
                Err(_) => {
                    crate::metrics::record_dispatch_ack_wait(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "ack_dropped",
                        ack_wait_start,
                    );
                    crate::metrics::record_queue_to_dispatch(
                        &record,
                        entry.partition_id,
                        &node_id,
                        "ack_dropped",
                    );
                    warn!(
                        run_id = %entry.run_id,
                        "dispatch ack channel dropped; keeping dispatch outbox row"
                    );
                    return Some(retry_after());
                }
            }
        }
        Err(error) => {
            crate::metrics::record_dispatch_channel_send_wait(
                &record,
                entry.partition_id,
                &node_id,
                "channel_closed",
                channel_send_start,
            );
            crate::metrics::record_queue_to_dispatch(
                &record,
                entry.partition_id,
                &node_id,
                "channel_closed",
            );
            error!(
                error = %error,
                run_id = %entry.run_id,
                "dispatch channel closed while draining dispatch outbox"
            );
            return Some(retry_after());
        }
    }

    None
}

#[allow(clippy::too_many_arguments)]
fn spawn_dispatch_task<M: DispatchOutboxStore + ?Sized + 'static>(
    tasks: &mut JoinSet<()>,
    entry: DecodedDispatchOutboxEntry,
    node_id: String,
    store: Arc<M>,
    dispatch_tx: mpsc::Sender<DispatchEnvelope>,
    segment: Arc<dyn Segment>,
    cache: Option<Arc<RecordCache>>,
    done_tx: mpsc::UnboundedSender<DispatchTaskCompletion>,
) {
    let route = entry.route_key();
    let completion = DispatchTaskCompletionGuard {
        token: Some(entry.token.clone()),
        route,
        done_tx,
    };
    tasks.spawn(async move {
        let retry_after = dispatch_entry(entry, node_id, store, dispatch_tx, segment, cache).await;
        completion.complete(retry_after);
    });
}

fn apply_completion(
    completion: DispatchTaskCompletion,
    in_flight: &mut HashSet<OutboxEntryToken>,
    deferred_until: &mut HashMap<OutboxEntryToken, Instant>,
    route_in_flight: &mut HashMap<DispatchRouteKey, usize>,
    route_deferred_until: &mut HashMap<DispatchRouteKey, Instant>,
) {
    in_flight.remove(&completion.token);
    if let Some(count) = route_in_flight.get_mut(&completion.route) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            route_in_flight.remove(&completion.route);
        }
    }
    if let Some(until) = completion.retry_after {
        deferred_until.insert(completion.token, until);
        route_deferred_until
            .entry(completion.route)
            .and_modify(|current| *current = (*current).max(until))
            .or_insert(until);
    } else {
        deferred_until.remove(&completion.token);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainWakeReason {
    Poll,
    Notify,
}

async fn wait_for_drain_wake(tick: &mut Interval, wake: &Notify) -> DrainWakeReason {
    tokio::select! {
        _ = tick.tick() => DrainWakeReason::Poll,
        _ = wake.notified() => DrainWakeReason::Notify,
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_partition_dispatch_outbox_drainer<M: DispatchOutboxStore + ?Sized + 'static>(
    partition_id: u32,
    node_id: String,
    store: Arc<M>,
    dispatch_tx: mpsc::Sender<DispatchEnvelope>,
    segment: Arc<dyn Segment>,
    cache: Option<Arc<RecordCache>>,
    is_leader_rx: watch::Receiver<bool>,
    replay_ready_rx: watch::Receiver<bool>,
    wake: Arc<Notify>,
    cancel: CancellationToken,
) {
    let max_in_flight = dispatch_outbox_max_in_flight();
    let max_in_flight_per_route = dispatch_outbox_max_in_flight_per_route(max_in_flight);
    let drain_interval = dispatch_outbox_drain_interval();
    info!(
        partition_id,
        max_in_flight,
        max_in_flight_per_route,
        drain_interval_ms = drain_interval.as_millis() as u64,
        "dispatch outbox drainer starting"
    );
    let mut tick = interval(drain_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<DispatchTaskCompletion>();
    let mut dispatch_tasks = JoinSet::new();
    let mut in_flight: HashSet<OutboxEntryToken> = HashSet::new();
    let mut deferred_until: HashMap<OutboxEntryToken, Instant> = HashMap::new();
    let mut route_in_flight: HashMap<DispatchRouteKey, usize> = HashMap::new();
    let mut route_deferred_until: HashMap<DispatchRouteKey, Instant> = HashMap::new();
    let mut scan_cursor: Option<DispatchOutboxCursor> = None;
    let mut last_tick_at: Option<Instant> = None;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                dispatch_tasks.shutdown().await;
                info!(partition_id, in_flight = in_flight.len(), "dispatch outbox drainer shutting down");
                return;
            }
            result = dispatch_tasks.join_next(), if !dispatch_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    warn!(
                        partition_id,
                        error = %error,
                        "dispatch outbox child task failed"
                    );
                }
            }
            Some(completion) = done_rx.recv() => {
                apply_completion(
                    completion,
                    &mut in_flight,
                    &mut deferred_until,
                    &mut route_in_flight,
                    &mut route_deferred_until,
                );
            }
            wake_reason = wait_for_drain_wake(&mut tick, &wake) => {
                let tick_started_at = Instant::now();
                if wake_reason == DrainWakeReason::Poll {
                    if let Some(previous_tick_at) = last_tick_at.replace(tick_started_at) {
                        crate::metrics::record_dispatch_outbox_tick_gap(
                            partition_id,
                            &node_id,
                            tick_started_at.duration_since(previous_tick_at),
                        );
                    }
                } else {
                    crate::metrics::record_dispatch_outbox_cycle(
                        partition_id,
                        &node_id,
                        "notified",
                    );
                }

                while let Ok(completion) = done_rx.try_recv() {
                    apply_completion(
                        completion,
                        &mut in_flight,
                        &mut deferred_until,
                        &mut route_in_flight,
                        &mut route_deferred_until,
                    );
                }
                let now = Instant::now();
                deferred_until.retain(|_, until| *until > now);
                route_deferred_until.retain(|_, until| *until > now);
                crate::metrics::record_dispatch_outbox_runtime_state(
                    partition_id,
                    &node_id,
                    in_flight.len() as u64,
                    deferred_until.len() as u64,
                    max_in_flight as u64,
                );

                if !*replay_ready_rx.borrow() {
                    crate::metrics::record_dispatch_outbox_cycle(
                        partition_id,
                        &node_id,
                        "skipped_replay_not_ready",
                    );
                    debug!(partition_id, "skipping dispatch outbox drain: startup replay not complete");
                    continue;
                }
                if !*is_leader_rx.borrow() {
                    crate::metrics::record_dispatch_outbox_cycle(
                        partition_id,
                        &node_id,
                        "skipped_not_leader",
                    );
                    debug!(partition_id, "skipping dispatch outbox drain: not leader");
                    continue;
                }
                if in_flight.len() >= max_in_flight {
                    crate::metrics::record_dispatch_outbox_cycle(
                        partition_id,
                        &node_id,
                        "skipped_in_flight_full",
                    );
                    debug!(
                        partition_id,
                        in_flight = in_flight.len(),
                        max_in_flight,
                        "skipping dispatch outbox drain: max in-flight reached"
                    );
                    continue;
                }

                let list_start = Instant::now();
                let raw = match DispatchOutboxStore::scan_dispatch_intents(
                    store.as_ref(),
                    partition_id,
                    scan_cursor.as_ref(),
                    DISPATCH_OUTBOX_SCAN_LIMIT,
                )
                .await
                {
                    Ok(raw) => {
                        crate::metrics::record_dispatch_outbox_list_duration(
                            partition_id,
                            &node_id,
                            "success",
                            list_start,
                        );
                        raw
                    }
                    Err(error) => {
                        crate::metrics::record_dispatch_outbox_list_duration(
                            partition_id,
                            &node_id,
                            "error",
                            list_start,
                        );
                        crate::metrics::record_dispatch_outbox_cycle(
                            partition_id,
                            &node_id,
                            "scan_error",
                        );
                        error!(error = %error, partition_id, "failed to list dispatch outbox");
                        continue;
                    }
                };
                let scan_len = raw.len();
                let next_scan_cursor = (scan_len == DISPATCH_OUTBOX_SCAN_LIMIT)
                    .then(|| DispatchOutboxCursor::after(raw.last().expect("non-empty full page")));
                scan_cursor = next_scan_cursor;
                let mut decoded = Vec::new();
                let now_ns = now_unix_ns();
                let mut oldest_age_ms = 0;
                let mut skipped_in_flight = 0_u64;
                let mut skipped_deferred = 0_u64;
                let mut skipped_capacity = 0_u64;
                let mut skipped_route_limit = 0_u64;
                let mut malformed_key = 0_u64;
                let mut malformed_value = 0_u64;

                for scan_entry in raw {
                    let entry = match scan_entry {
                        DispatchOutboxScanEntry::Ready(entry) => entry,
                        DispatchOutboxScanEntry::CorruptKey { token } => {
                            malformed_key += 1;
                            warn!(
                                partition_id,
                                "deleting malformed dispatch outbox row with invalid key"
                            );
                            if let Err(error) = delete_raw_entry(store.as_ref(), token).await {
                                warn!(
                                    error = %error,
                                    partition_id,
                                    "failed to delete malformed dispatch outbox key"
                                );
                            }
                            continue;
                        }
                    };
                    if in_flight.contains(&entry.token) {
                        skipped_in_flight += 1;
                        continue;
                    }
                    if let Some(until) = deferred_until.get(&entry.token) {
                        if *until > now {
                            skipped_deferred += 1;
                            continue;
                        }
                    }

                    match decode_entry(entry) {
                        Ok(entry) => {
                            oldest_age_ms = oldest_age_ms
                                .max(age_ms_from_ns(now_ns, entry.source_record.timestamp_ns));
                            deferred_until.remove(&entry.token);
                            decoded.push(entry);
                        }
                        Err(DecodeEntryError::MalformedValue {
                            token,
                            partition_id: row_partition_id,
                            project_id,
                            queued_offset,
                            run_id,
                            reason,
                        }) => {
                            malformed_value += 1;
                            warn!(
                                reason = %reason,
                                run_id = %run_id,
                                "dead-lettering malformed dispatch outbox row"
                            );
                            if dead_letter_malformed_entry(
                                segment.clone(),
                                cache.clone(),
                                row_partition_id,
                                project_id.clone(),
                                queued_offset,
                                run_id.clone(),
                                reason,
                            )
                            .await
                            {
                                if let Err(error) = delete_raw_entry(store.as_ref(), token).await {
                                    warn!(
                                        error = %error,
                                        run_id = %run_id,
                                        "failed to delete raw malformed dispatch outbox row"
                                    );
                                }
                            }
                        }
                    }
                }

                let decoded_len = decoded.len();
                let now_ms = now_ns / 1_000_000;
                // A persisted retry on any row cools down the whole route.
                // Build this map before spawning so later rows in the page
                // cannot sneak around an earlier route failure after restart.
                for entry in &decoded {
                    if entry.next_attempt_at_ms <= now_ms {
                        continue;
                    }
                    let until = now
                        + Duration::from_millis(
                            entry.next_attempt_at_ms.saturating_sub(now_ms) as u64,
                        )
                        .min(DISPATCH_OUTBOX_DELAYED_RECHECK_MAX);
                    deferred_until.insert(entry.token.clone(), until);
                    route_deferred_until
                        .entry(entry.route_key())
                        .and_modify(|current| *current = (*current).max(until))
                        .or_insert(until);
                }
                let decoded = fair_route_order(decoded);
                let mut dispatched = 0_u64;
                for (idx, entry) in decoded.into_iter().enumerate() {
                    if in_flight.len() >= max_in_flight {
                        skipped_capacity += decoded_len.saturating_sub(idx) as u64;
                        break;
                    }
                    let route = entry.route_key();
                    if deferred_until
                        .get(&entry.token)
                        .is_some_and(|until| *until > now)
                        || route_deferred_until
                            .get(&route)
                            .is_some_and(|until| *until > now)
                    {
                        skipped_deferred += 1;
                        continue;
                    }
                    if route_in_flight.get(&route).copied().unwrap_or_default()
                        >= max_in_flight_per_route
                    {
                        skipped_route_limit += 1;
                        continue;
                    }
                    in_flight.insert(entry.token.clone());
                    *route_in_flight.entry(route).or_default() += 1;
                    dispatched += 1;
                    spawn_dispatch_task(
                        &mut dispatch_tasks,
                        entry,
                        node_id.clone(),
                        store.clone(),
                        dispatch_tx.clone(),
                        segment.clone(),
                        cache.clone(),
                        done_tx.clone(),
                    );
                }

                crate::metrics::record_dispatch_outbox_cycle(partition_id, &node_id, "scanned");
                crate::metrics::record_dispatch_outbox_state(
                    partition_id,
                    &node_id,
                    scan_len as u64,
                    oldest_age_ms,
                    in_flight.len() as u64,
                    deferred_until.len() as u64,
                    max_in_flight as u64,
                );
                if scan_len > 0 {
                    crate::metrics::record_dispatch_outbox_scan_rows(
                        partition_id,
                        &node_id,
                        "scanned",
                        scan_len as u64,
                    );
                    crate::metrics::record_dispatch_outbox_scan_rows(
                        partition_id,
                        &node_id,
                        "decoded",
                        decoded_len as u64,
                    );
                    crate::metrics::record_dispatch_outbox_scan_rows(
                        partition_id,
                        &node_id,
                        "dispatched",
                        dispatched,
                    );
                    if skipped_in_flight > 0 {
                        crate::metrics::record_dispatch_outbox_scan_rows(
                            partition_id,
                            &node_id,
                            "skipped_in_flight",
                            skipped_in_flight,
                        );
                    }
                    if skipped_deferred > 0 {
                        crate::metrics::record_dispatch_outbox_scan_rows(
                            partition_id,
                            &node_id,
                            "skipped_deferred",
                            skipped_deferred,
                        );
                    }
                    if skipped_capacity > 0 {
                        crate::metrics::record_dispatch_outbox_scan_rows(
                            partition_id,
                            &node_id,
                            "skipped_capacity",
                            skipped_capacity,
                        );
                    }
                    if skipped_route_limit > 0 {
                        crate::metrics::record_dispatch_outbox_scan_rows(
                            partition_id,
                            &node_id,
                            "skipped_route_limit",
                            skipped_route_limit,
                        );
                    }
                    if malformed_key > 0 {
                        crate::metrics::record_dispatch_outbox_scan_rows(
                            partition_id,
                            &node_id,
                            "malformed_key",
                            malformed_key,
                        );
                    }
                    if malformed_value > 0 {
                        crate::metrics::record_dispatch_outbox_scan_rows(
                            partition_id,
                            &node_id,
                            "malformed_value",
                            malformed_value,
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use engine_core::traits::Segment;
    use engine_core::types::{Offset, Record};
    use engine_journal_memory::MemorySegment;
    use engine_proto::Run;
    use prost::Message;
    use tokio::sync::watch;

    use super::*;
    use crate::memory_work_store::MemoryWorkStore;

    struct FailFirstDeleteStore {
        inner: Arc<MemoryWorkStore>,
        fail_delete: AtomicBool,
    }

    struct RejectAssignedSegment {
        inner: Arc<MemorySegment>,
    }

    #[async_trait::async_trait]
    impl Segment for RejectAssignedSegment {
        async fn append_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>> {
            if records
                .iter()
                .any(|record| record.event_type == "run.assigned")
            {
                return Err(EngineError::InvalidArgument(
                    "injected permanent run.assigned contract failure".to_string(),
                ));
            }
            self.inner.append_batch(records).await
        }

        async fn tail(&self, from: Offset) -> EngineResult<engine_core::traits::RecordStream> {
            self.inner.tail(from).await
        }

        async fn read_range(&self, from: Offset, to: Offset) -> EngineResult<Vec<Record>> {
            self.inner.read_range(from, to).await
        }

        async fn tail_offset(&self) -> EngineResult<Offset> {
            self.inner.tail_offset().await
        }

        async fn seal(&self) -> EngineResult<()> {
            self.inner.seal().await
        }

        async fn trim(&self, through: Offset) -> EngineResult<()> {
            self.inner.trim(through).await
        }
    }

    #[async_trait::async_trait]
    impl DispatchOutboxStore for FailFirstDeleteStore {
        async fn scan_dispatch_intents(
            &self,
            partition_id: u32,
            cursor: Option<&DispatchOutboxCursor>,
            limit: usize,
        ) -> EngineResult<Vec<DispatchOutboxScanEntry>> {
            DispatchOutboxStore::scan_dispatch_intents(
                self.inner.as_ref(),
                partition_id,
                cursor,
                limit,
            )
            .await
        }

        async fn delete_dispatch_intent(&self, token: &OutboxEntryToken) -> EngineResult<()> {
            if self.fail_delete.swap(false, Ordering::SeqCst) {
                return Err(EngineError::Storage(
                    "injected failure after dispatch acknowledgement".to_string(),
                ));
            }
            DispatchOutboxStore::delete_dispatch_intent(self.inner.as_ref(), token).await
        }

        async fn load_dispatch_run(
            &self,
            project_id: &str,
            run_id: &str,
        ) -> EngineResult<Option<Vec<u8>>> {
            DispatchOutboxStore::load_dispatch_run(self.inner.as_ref(), project_id, run_id).await
        }

        async fn ensure_pending_deployment_run(
            &self,
            project_id: &str,
            deployment_id: &str,
            priority: u8,
            ready_at_ms: i64,
            created_at_ms: i64,
            run_id: &str,
        ) -> EngineResult<()> {
            DispatchOutboxStore::ensure_pending_deployment_run(
                self.inner.as_ref(),
                project_id,
                deployment_id,
                priority,
                ready_at_ms,
                created_at_ms,
                run_id,
            )
            .await
        }
    }

    fn make_queued_record_with_timestamp(
        run_id: &str,
        queued_offset: u64,
        timestamp_ns: i64,
    ) -> Record {
        let mut metadata = HashMap::new();
        metadata.insert("component_name".to_string(), "ks_noop".to_string());
        metadata.insert("component_type".to_string(), "function".to_string());
        metadata.insert(
            DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
            "0".to_string(),
        );
        metadata.insert(
            DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
            queued_offset.to_string(),
        );
        Record {
            offset: engine_core::partition_offset::encode_offset(0, queued_offset).unwrap(),
            project_id: "t1".to_string(),
            run_id: run_id.to_string(),
            event_type: "run.queued".to_string(),
            data: Bytes::from_static(b"{\"input\":true}"),
            timestamp_ns,
            step_key: Some("step.dispatch".to_string()),
            correlation_id: Some("corr-1".to_string()),
            parent_event_id: Some("parent-1".to_string()),
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn make_queued_record(run_id: &str, queued_offset: u64) -> Record {
        make_queued_record_with_timestamp(run_id, queued_offset, now_unix_ns())
    }

    fn dispatch_outbox_entry(
        run_id: &str,
        queued_offset: u64,
        payload: Vec<u8>,
    ) -> DispatchOutboxEntry {
        DispatchOutboxEntry {
            token: MemoryWorkStore::token(0, "t1", queued_offset, run_id),
            partition_id: 0,
            project_id: "t1".to_string(),
            queued_offset,
            run_id: run_id.to_string(),
            payload,
        }
    }

    fn decoded_entry_for_route(
        run_id: &str,
        queued_offset: u64,
        deployment_id: &str,
    ) -> DecodedDispatchOutboxEntry {
        let mut record = make_queued_record(run_id, queued_offset);
        record
            .metadata
            .insert("deployment_id".to_string(), deployment_id.to_string());
        let request = crate::orchestrator::build_dispatch_request(&record);
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();
        decode_entry(dispatch_outbox_entry(run_id, queued_offset, payload)).unwrap()
    }

    #[test]
    fn scanned_dispatches_rotate_routes_without_reordering_a_route() {
        let entries = vec![
            decoded_entry_for_route("a-1", 1, "deployment-a"),
            decoded_entry_for_route("a-2", 2, "deployment-a"),
            decoded_entry_for_route("a-3", 3, "deployment-a"),
            decoded_entry_for_route("b-1", 4, "deployment-b"),
            decoded_entry_for_route("b-2", 5, "deployment-b"),
        ];

        let ordered = fair_route_order(entries)
            .into_iter()
            .map(|entry| entry.run_id)
            .collect::<Vec<_>>();

        assert_eq!(ordered, ["a-1", "b-1", "a-2", "b-2", "a-3"]);
    }

    #[tokio::test]
    async fn notification_wakes_before_poll_and_retains_an_early_permit() {
        let mut tick = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(60),
            Duration::from_secs(60),
        );
        let wake = Notify::new();

        // Commit-side notification can happen just before the drainer starts
        // awaiting. Notify must retain that permit so the durable scan begins
        // immediately instead of waiting for the repair poll.
        wake.notify_one();

        let reason = tokio::time::timeout(
            Duration::from_millis(50),
            wait_for_drain_wake(&mut tick, &wake),
        )
        .await
        .expect("notification should beat the distant repair poll");
        assert_eq!(reason, DrainWakeReason::Notify);
    }

    #[tokio::test]
    async fn durable_retry_ready_at_defers_push_dispatch() {
        let store = MemoryWorkStore::new();
        let ready_at_ms = now_unix_ns() / 1_000_000 + 60_000;
        let delayed = Run {
            project_id: "t1".to_string(),
            run_id: "run-delayed".to_string(),
            status: "queued".to_string(),
            metadata: HashMap::from([(
                crate::run_state_machine::READY_AT_MS_META.to_string(),
                ready_at_ms.to_string(),
            )]),
            ..Default::default()
        };
        store.put_run("t1", "run-delayed", delayed.encode_to_vec());

        let eligibility = run_dispatch_eligibility(&store, "t1", "run-delayed")
            .await
            .unwrap();
        assert!(matches!(
            eligibility,
            RunDispatchEligibility::Delayed(delay) if delay > Duration::from_secs(55)
        ));

        let mut ready = delayed;
        ready.metadata.insert(
            crate::run_state_machine::READY_AT_MS_META.to_string(),
            "1".to_string(),
        );
        store.put_run("t1", "run-delayed", ready.encode_to_vec());
        assert_eq!(
            run_dispatch_eligibility(&store, "t1", "run-delayed")
                .await
                .unwrap(),
            RunDispatchEligibility::Eligible
        );
    }

    #[tokio::test]
    async fn old_queued_work_remains_dispatchable() {
        let store = MemoryWorkStore::new();
        let queued = Run {
            project_id: "t1".to_string(),
            run_id: "run-old".to_string(),
            status: "queued".to_string(),
            metadata: HashMap::from([("dispatch_deadline_ms".into(), "1".into())]),
            submitted_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        store.put_run("t1", "run-old", queued.encode_to_vec());

        assert_eq!(
            run_dispatch_eligibility(&store, "t1", "run-old")
                .await
                .unwrap(),
            RunDispatchEligibility::Eligible
        );
    }

    async fn put_outbox_and_run_with_timestamp(
        store: &MemoryWorkStore,
        run_id: &str,
        queued_offset: u64,
        timestamp_ns: i64,
    ) {
        let record = make_queued_record_with_timestamp(run_id, queued_offset, timestamp_ns);
        let request = crate::orchestrator::build_dispatch_request(&record);
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();
        let run = Run {
            project_id: "t1".to_string(),
            run_id: run_id.to_string(),
            component_name: "ks_noop".to_string(),
            component_type: "function".to_string(),
            status: "queued".to_string(),
            input_data: record.data.to_vec(),
            metadata: record.metadata.clone(),
            ..Default::default()
        };

        store.put_dispatch_intent(0, "t1", queued_offset, run_id, payload);
        store.put_run("t1", run_id, run.encode_to_vec());
    }

    async fn put_outbox_and_run(store: &MemoryWorkStore, run_id: &str, queued_offset: u64) {
        put_outbox_and_run_with_timestamp(store, run_id, queued_offset, now_unix_ns()).await;
    }

    fn true_rx() -> watch::Receiver<bool> {
        let (_tx, rx) = watch::channel(true);
        rx
    }

    #[test]
    fn retryable_dispatch_error_classification_covers_capacity_and_workers() {
        assert!(is_retriable_dispatch_error(
            "dispatch saturated (max inflight 128)"
        ));
        assert!(is_retriable_dispatch_error("channel full"));
        assert!(is_retriable_dispatch_error(
            "resource exhausted: worker capacity exhausted for component 'ks_noop'; retry"
        ));
        assert!(is_retriable_dispatch_error(
            "resource exhausted: workerless concurrency exhausted for scope 'component' key 'component-key' (active 1, limit 1); retry"
        ));
        assert!(is_retriable_dispatch_error(
            "resource exhausted: workerless singleton key 'account:alpha' already has active execution; retry"
        ));
        assert!(is_retriable_dispatch_error(
            "resource exhausted: workerless debounce waiting for key 'account:alpha' until 1234; retry"
        ));
        assert!(is_retriable_dispatch_error(
            "no worker available for component 'ks_noop'"
        ));
        assert!(!is_retriable_dispatch_error(
            "invalid argument: bad request"
        ));
        assert!(is_workerless_debounce_superseded_error(
            "failed precondition: workerless debounce superseded run for key 'account:alpha'"
        ));
        assert!(!is_retriable_dispatch_error(
            "failed precondition: workerless debounce superseded run for key 'account:alpha'"
        ));
    }

    #[test]
    fn payload_and_cancellation_do_not_repersist_legacy_credentials() {
        let mut record = make_queued_record("run-legacy", 7);
        record.metadata.insert(
            "workerless_signing_secret".to_string(),
            "old-secret".to_string(),
        );
        record.metadata.insert(
            "workerless_invoke_headers".to_string(),
            "old-headers".to_string(),
        );
        let mut request = crate::orchestrator::build_dispatch_request(&record);
        request.metadata.insert(
            "workerless_signing_secret".to_string(),
            "old-secret".to_string(),
        );
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();
        let decoded = decode_entry(dispatch_outbox_entry("run-legacy", 7, payload)).unwrap();
        let cancelled = build_workerless_debounce_cancelled_record(&record, "superseded");

        for metadata in [
            &decoded.request.metadata,
            &decoded.source_record.metadata,
            &cancelled.metadata,
        ] {
            assert!(!metadata.contains_key("workerless_signing_secret"));
            assert!(!metadata.contains_key("workerless_invoke_headers"));
        }
    }

    #[test]
    fn payload_preserves_source_record_fields() {
        let record = make_queued_record("run-preserve", 7);
        let request = crate::orchestrator::build_dispatch_request(&record);
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();

        let decoded = decode_entry(dispatch_outbox_entry("run-preserve", 7, payload)).unwrap();
        assert_eq!(
            decoded.source_record.step_key.as_deref(),
            Some("step.dispatch")
        );
        assert_eq!(
            decoded.source_record.correlation_id.as_deref(),
            Some("corr-1")
        );
        assert_eq!(
            decoded.source_record.parent_event_id.as_deref(),
            Some("parent-1")
        );
        assert_eq!(
            decoded.source_record.data,
            Bytes::from_static(b"{\"input\":true}")
        );
        assert!(decoded.outbox_inserted_at_ms.unwrap_or_default() > 0);
    }

    #[test]
    fn payload_preserves_lease_expiry_generation_for_assignment() {
        let mut record = make_queued_record("run-expired", 7);
        record.event_type = "run.lease_expired".to_string();
        record.metadata.insert(
            engine_core::lease_fence::META_FENCE_ATTEMPT.to_string(),
            "0".to_string(),
        );
        let request = crate::orchestrator::build_dispatch_request(&record);
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();

        let decoded = decode_entry(dispatch_outbox_entry("run-expired", 7, payload)).unwrap();
        assert_eq!(decoded.source_record.event_type, "run.lease_expired");
        assert_eq!(decoded.request.attempt, 1);

        let assigned = crate::orchestrator::build_assigned_record(&decoded.source_record, None);
        assert_eq!(
            assigned.metadata.get("attempt").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn dispatch_request_recovers_stable_metadata_without_old_authority() {
        let mut record = make_queued_record("run-workerless-resume", 8);
        record.event_type = "run.lease_expired".to_string();
        record.data = Bytes::new();
        record.metadata.remove("component_type");
        record.metadata.remove("deployment_id");
        record.metadata.insert(
            "workerless_checkpoint".to_string(),
            serde_json::json!({"steps": {"fetch": {"ok": true}}}).to_string(),
        );
        record.metadata.insert(
            engine_core::lease_fence::META_FENCE_ATTEMPT.to_string(),
            "0".to_string(),
        );
        record.metadata.insert(
            engine_core::lease_fence::META_FENCE_KIND.to_string(),
            "1".to_string(),
        );
        record.metadata.insert(
            engine_core::lease_fence::META_FENCE_LEASE_ID.to_string(),
            "expired-generation".to_string(),
        );
        record
            .metadata
            .insert("lease_id".to_string(), "expired-lease".to_string());
        let mut run = Run {
            project_id: "t1".to_string(),
            run_id: "run-workerless-resume".to_string(),
            component_name: "ks_noop".to_string(),
            component_type: "function".to_string(),
            deployment_id: "deployment-1".to_string(),
            input_data: br#"{"input":true}"#.to_vec(),
            ..Default::default()
        };
        run.metadata
            .insert("stable_context".to_string(), "preserved".to_string());
        run.metadata
            .insert("lease_id".to_string(), "expired-lease".to_string());

        let record = crate::orchestrator::hydrate_dispatch_source_record(
            &record,
            Some(&run.encode_to_vec()),
        );
        let mut request = crate::orchestrator::build_dispatch_request(&record);
        request.metadata.remove("workerless_checkpoint");
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();

        let decoded =
            decode_entry(dispatch_outbox_entry("run-workerless-resume", 8, payload)).unwrap();
        let mut recovered = decoded.request.clone();
        recover_request_metadata_from_source_record(&mut recovered, &decoded.source_record);

        assert_eq!(
            recovered
                .metadata
                .get("workerless_checkpoint")
                .map(String::as_str),
            Some(r#"{"steps":{"fetch":{"ok":true}}}"#)
        );
        assert_eq!(recovered.attempt, 1);
        assert_eq!(
            recovered.metadata.get("stable_context").map(String::as_str),
            Some("preserved")
        );
        assert_eq!(
            recovered.component_type,
            engine_proto::ComponentType::Function as i32
        );
        assert_eq!(recovered.deployment_id, "deployment-1");
        assert_eq!(recovered.input_data, br#"{"input":true}"#);
        assert!(!recovered.metadata.contains_key("lease_id"));
        assert!(!recovered
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_ATTEMPT));
        assert!(!recovered
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_KIND));

        let mut completion = decoded.source_record;
        completion.event_type = "run.completed".to_string();
        completion.metadata = recovered.metadata;
        assert!(engine_core::journal_contract::record_idempotency_key(&completion).is_none());
    }

    #[test]
    fn dispatch_request_stamps_workerless_debounce_queued_at() {
        let mut record =
            make_queued_record_with_timestamp("run-workerless-debounce", 9, 4_200_000_000);
        record.metadata.insert(
            WORKERLESS_DEBOUNCE_WINDOW_MS_META.to_string(),
            "500".to_string(),
        );
        record.metadata.insert(
            GATEWAY_RECEIVED_AT_NS_META.to_string(),
            "4100000000".to_string(),
        );
        let mut request = crate::orchestrator::build_dispatch_request(&record);
        request
            .metadata
            .remove(WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META);

        stamp_workerless_debounce_queued_at(&mut request, &mut record);

        assert_eq!(
            request
                .metadata
                .get(WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META)
                .map(String::as_str),
            Some("4100")
        );
        assert_eq!(
            record
                .metadata
                .get(WORKERLESS_DEBOUNCE_QUEUED_AT_MS_META)
                .map(String::as_str),
            Some("4100")
        );
    }

    #[tokio::test]
    async fn retryable_dispatch_error_leaves_outbox_row() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        put_outbox_and_run(store.as_ref(), "run-saturated", 1).await;

        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(8);
        let cancel = CancellationToken::new();
        let drainer = tokio::spawn(run_partition_dispatch_outbox_drainer(
            0,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
            true_rx(),
            true_rx(),
            Arc::new(Notify::new()),
            cancel.clone(),
        ));

        let envelope = tokio::time::timeout(Duration::from_secs(1), dispatch_rx.recv())
            .await
            .expect("dispatch should be attempted")
            .expect("dispatch channel should remain open");
        assert_eq!(envelope.request.invocation_id, "run-saturated");
        let _ = envelope
            .ack
            .send(Err("dispatch saturated (max inflight 128)".to_string()));

        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        drainer.await.unwrap();

        let rows = store.dispatch_intents(0);
        assert_eq!(rows.len(), 1, "retryable errors must keep the outbox row");
        let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        let retry_record = records
            .iter()
            .find(|record| record.event_type == DISPATCH_RETRY_SCHEDULED_EVENT)
            .expect("retry schedule must be journaled for HA replay");
        assert!(
            retry_record.data.is_empty(),
            "retry journal records must not duplicate the potentially large input payload"
        );
        assert_eq!(
            retry_record
                .metadata
                .get(DISPATCH_RETRY_ATTEMPT_META)
                .map(String::as_str),
            Some("1")
        );
        assert!(
            retry_record.metadata[DISPATCH_RETRY_NEXT_ATTEMPT_AT_MS_META]
                .parse::<i64>()
                .unwrap()
                > now_unix_ns() / 1_000_000
        );
        assert!(
            records
                .iter()
                .all(|record| record.event_type != "run.failed"),
            "retryable saturation must not fail the run"
        );
    }

    #[tokio::test]
    async fn durable_retry_record_does_not_duplicate_large_dispatch_input() {
        let segment = Arc::new(MemorySegment::new(0));
        let mut source_record = make_queued_record("run-large-retry", 17);
        source_record.data = Bytes::from(vec![
            b'x';
            engine_core::types::MAX_JOURNAL_RECORD_BYTES
                .saturating_sub(32 * 1024)
        ]);
        let request = crate::orchestrator::build_dispatch_request(&source_record);
        let entry = DecodedDispatchOutboxEntry {
            token: MemoryWorkStore::token(0, "t1", 17, "run-large-retry"),
            partition_id: 0,
            project_id: "t1".to_string(),
            queued_offset: 17,
            run_id: "run-large-retry".to_string(),
            request,
            source_record,
            outbox_inserted_at_ms: Some(1_000),
            retry_attempt: 0,
            next_attempt_at_ms: 0,
        };

        persist_durable_retry(segment.as_ref(), None, &entry)
            .await
            .expect("compact retry metadata should fit in the journal");

        let retry = segment
            .read_range(Offset::ZERO, Offset(1))
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(retry.data.is_empty());
        assert!(retry.to_proto().encoded_len() < 32 * 1024);
    }

    #[tokio::test]
    async fn lease_backed_dispatch_appends_contract_valid_assignment() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        put_outbox_and_run(store.as_ref(), "run-leased", 1).await;

        let scanned = store.scan_dispatch_intents(0, None, 10).await.unwrap();
        let entry = match scanned.into_iter().next().unwrap() {
            DispatchOutboxScanEntry::Ready(entry) => decode_entry(entry).unwrap(),
            DispatchOutboxScanEntry::CorruptKey { .. } => panic!("expected a valid row"),
        };
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(1);
        let task = tokio::spawn(dispatch_entry(
            entry,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
        ));
        let envelope = dispatch_rx.recv().await.expect("dispatch should be sent");
        let session_hash = engine_core::lease_fence::worker_session_hash("worker-a");
        envelope
            .ack
            .send(Ok(Some(crate::orchestrator::LeaseFields {
                lease_id: "lease-a".to_string(),
                lease_worker_id: "worker-a".to_string(),
                lease_session_hash: session_hash.clone(),
                lease_expires_at_ms: 10_000,
                lease_timeout_ms: 5_000,
            })))
            .unwrap();

        assert!(task.await.unwrap().is_none());
        assert!(store.dispatch_intents(0).is_empty());
        let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        let assigned = records
            .iter()
            .find(|record| record.event_type == "run.assigned")
            .expect("lease-backed dispatch should append run.assigned");
        assert_eq!(
            assigned
                .metadata
                .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH),
            Some(&session_hash)
        );
    }

    #[tokio::test]
    async fn permanent_assignment_validation_error_fails_once_and_deletes_outbox() {
        let inner = Arc::new(MemorySegment::new(0));
        let segment = Arc::new(RejectAssignedSegment {
            inner: inner.clone(),
        });
        let store = Arc::new(MemoryWorkStore::new());
        put_outbox_and_run(store.as_ref(), "run-poisoned", 1).await;

        let scanned = store.scan_dispatch_intents(0, None, 10).await.unwrap();
        let entry = match scanned.into_iter().next().unwrap() {
            DispatchOutboxScanEntry::Ready(entry) => decode_entry(entry).unwrap(),
            DispatchOutboxScanEntry::CorruptKey { .. } => panic!("expected a valid row"),
        };
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(1);
        let task = tokio::spawn(dispatch_entry(
            entry,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment,
            None,
        ));
        let envelope = dispatch_rx.recv().await.expect("dispatch should be sent");
        envelope.ack.send(Ok(None)).unwrap();

        assert!(
            task.await.unwrap().is_none(),
            "permanent validation errors must not schedule another retry"
        );
        assert!(store.dispatch_intents(0).is_empty());
        let records = inner.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        let failed = records
            .iter()
            .find(|record| record.event_type == "run.failed")
            .expect("permanent validation errors should append one terminal failure");
        let data: serde_json::Value = serde_json::from_slice(&failed.data).unwrap();
        assert!(data["error_message"]
            .as_str()
            .is_some_and(|message| message.contains("permanent run.assigned validation failure")));
    }

    /// Crash boundary: a worker may accept dispatch and `run.assigned` may
    /// reach the journal before deleting the durable outbox row. Once the
    /// assignment projects, replay classifies the row as stale and deletes it
    /// without dispatching the run again.
    #[tokio::test]
    async fn assignment_before_outbox_delete_failure_converges_without_redispatch() {
        let segment = Arc::new(MemorySegment::new(0));
        let rocks = Arc::new(MemoryWorkStore::new());
        put_outbox_and_run(rocks.as_ref(), "run-ack-crash", 1).await;
        let store = Arc::new(FailFirstDeleteStore {
            inner: rocks.clone(),
            fail_delete: AtomicBool::new(true),
        });

        let scanned = store.scan_dispatch_intents(0, None, 10).await.unwrap();
        let entry = match scanned.into_iter().next().unwrap() {
            DispatchOutboxScanEntry::Ready(entry) => decode_entry(entry).unwrap(),
            DispatchOutboxScanEntry::CorruptKey { .. } => panic!("expected a valid row"),
        };
        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(1);
        let task = tokio::spawn(dispatch_entry(
            entry,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
        ));
        let envelope = dispatch_rx.recv().await.expect("dispatch should be sent");
        envelope.ack.send(Ok(None)).unwrap();
        assert!(
            task.await.unwrap().is_some(),
            "failed delete should request retry"
        );
        assert_eq!(
            rocks.dispatch_intents(0).len(),
            1,
            "the durable row must survive the injected delete failure"
        );

        let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        let assigned = records
            .iter()
            .find(|record| record.event_type == "run.assigned")
            .expect("accepted dispatch must append run.assigned");
        let existing = rocks.run(&assigned.project_id, &assigned.run_id).unwrap();
        let projected = crate::projections::runs::project(assigned, Some(existing.as_slice()));
        rocks.apply(assigned, &projected.ops);

        let scanned = store.scan_dispatch_intents(0, None, 10).await.unwrap();
        let replay = match scanned.into_iter().next().unwrap() {
            DispatchOutboxScanEntry::Ready(entry) => decode_entry(entry).unwrap(),
            DispatchOutboxScanEntry::CorruptKey { .. } => panic!("expected a valid row"),
        };
        let (retry_tx, mut retry_rx) = mpsc::channel::<DispatchEnvelope>(1);
        assert!(dispatch_entry(
            replay,
            "node-test".to_string(),
            store,
            retry_tx,
            segment.clone(),
            None,
        )
        .await
        .is_none());
        assert!(
            retry_rx.try_recv().is_err(),
            "projected assignment must suppress duplicate dispatch"
        );
        assert!(rocks.dispatch_intents(0).is_empty());
        assert_eq!(
            segment
                .read_range(Offset::ZERO, Offset(10))
                .await
                .unwrap()
                .iter()
                .filter(|record| record.event_type == "run.assigned")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn cancellation_drops_inflight_ack_waits_before_drainer_returns() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        put_outbox_and_run(store.as_ref(), "run-cancelled-drainer", 1).await;

        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(8);
        let cancel = CancellationToken::new();
        let drainer = tokio::spawn(run_partition_dispatch_outbox_drainer(
            0,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment,
            None,
            true_rx(),
            true_rx(),
            Arc::new(Notify::new()),
            cancel.clone(),
        ));

        let envelope = tokio::time::timeout(Duration::from_secs(1), dispatch_rx.recv())
            .await
            .expect("dispatch should be attempted")
            .expect("dispatch channel should remain open");
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), drainer)
            .await
            .expect("drainer cancellation should be bounded")
            .expect("drainer should join cleanly");

        assert!(
            envelope.ack.send(Ok(None)).is_err(),
            "the drainer must own and cancel every child ack wait before it returns"
        );
        assert_eq!(
            store.dispatch_intents(0).len(),
            1,
            "cancelling an in-flight dispatch must leave its durable row for retry"
        );
    }

    #[tokio::test]
    async fn workerless_debounce_superseded_cancels_and_deletes_outbox_row() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        put_outbox_and_run(store.as_ref(), "run-superseded", 1).await;

        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(8);
        let cancel = CancellationToken::new();
        let drainer = tokio::spawn(run_partition_dispatch_outbox_drainer(
            0,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
            true_rx(),
            true_rx(),
            Arc::new(Notify::new()),
            cancel.clone(),
        ));

        let envelope = tokio::time::timeout(Duration::from_secs(1), dispatch_rx.recv())
            .await
            .expect("dispatch should be attempted")
            .expect("dispatch channel should remain open");
        assert_eq!(envelope.request.invocation_id, "run-superseded");
        let _ = envelope.ack.send(Err(
            "failed precondition: workerless debounce superseded run for key 'tenant:alpha'"
                .to_string(),
        ));

        for _ in 0..50 {
            if store.dispatch_intents(0).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        drainer.await.unwrap();

        assert!(store.dispatch_intents(0).is_empty());
        let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        let cancelled = records
            .iter()
            .find(|record| {
                record.run_id == "run-superseded" && record.event_type == "run.cancelled"
            })
            .expect("superseded debounce row should be cancelled");
        assert_eq!(
            cancelled.metadata.get("cancel_reason").map(String::as_str),
            Some(WORKERLESS_DEBOUNCE_SUPERSEDED_REASON)
        );
        let data: serde_json::Value = serde_json::from_slice(&cancelled.data).unwrap();
        assert_eq!(
            data["reason"].as_str(),
            Some(WORKERLESS_DEBOUNCE_SUPERSEDED_REASON)
        );
    }

    #[tokio::test]
    async fn old_queued_work_keeps_retrying_when_workers_are_unavailable() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        let old_timestamp_ns = now_unix_ns() - (3 * 24 * 60 * 60 * 1_000_000_000);
        put_outbox_and_run_with_timestamp(store.as_ref(), "run-waiting", 1, old_timestamp_ns).await;

        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(8);
        let cancel = CancellationToken::new();
        let drainer = tokio::spawn(run_partition_dispatch_outbox_drainer(
            0,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
            true_rx(),
            true_rx(),
            Arc::new(Notify::new()),
            cancel.clone(),
        ));

        let envelope = tokio::time::timeout(Duration::from_secs(1), dispatch_rx.recv())
            .await
            .expect("dispatch should be attempted")
            .expect("dispatch channel should remain open");
        assert_eq!(envelope.request.invocation_id, "run-waiting");
        let _ = envelope
            .ack
            .send(Err("no routing worker available".to_string()));

        for _ in 0..50 {
            let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
            if records.iter().any(|record| {
                record.event_type == DISPATCH_RETRY_SCHEDULED_EVENT
                    || record.event_type == "run.failed"
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        drainer.await.unwrap();

        assert_eq!(store.dispatch_intents(0).len(), 1);
        let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        assert!(records
            .iter()
            .any(|record| record.event_type == DISPATCH_RETRY_SCHEDULED_EVENT));
        assert!(records
            .iter()
            .all(|record| record.event_type != "run.failed"));
    }

    #[tokio::test]
    async fn a_push_run_of_a_retired_deployment_is_cancelled_not_dispatched() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        let queued_ns = now_unix_ns() - 3_600 * 1_000_000_000;
        let mut record = make_queued_record_with_timestamp("run-orphaned", 4, queued_ns);
        record
            .metadata
            .insert("deployment_id".to_string(), "deploy-gone".to_string());
        let request = crate::orchestrator::build_dispatch_request(&record);
        let payload = encode_dispatch_outbox_payload(&request, &record).unwrap();
        let run = Run {
            project_id: "t1".to_string(),
            run_id: "run-orphaned".to_string(),
            status: "queued".to_string(),
            metadata: record.metadata.clone(),
            ..Default::default()
        };
        store.put_dispatch_intent(0, "t1", 4, "run-orphaned", payload);
        store.put_run("t1", "run-orphaned", run.encode_to_vec());

        let canceller = crate::retired_deployments::test_support::installed_recording_canceller();
        crate::retired_deployments::test_support::mark_retired("t1", "deploy-gone", "terminated");

        let (dispatch_tx, mut dispatch_rx) = mpsc::channel::<DispatchEnvelope>(8);
        let cancel = CancellationToken::new();
        let drainer = tokio::spawn(run_partition_dispatch_outbox_drainer(
            0,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
            true_rx(),
            true_rx(),
            Arc::new(Notify::new()),
            cancel.clone(),
        ));
        for _ in 0..50 {
            if store.dispatch_intents(0).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        drainer.await.unwrap();

        assert!(store.dispatch_intents(0).is_empty());
        assert!(
            dispatch_rx.try_recv().is_err(),
            "a retired run must not be dispatched"
        );
        let cancelled = canceller
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.run_id == "run-orphaned")
            .cloned()
            .expect("the drainer should cancel the run");
        assert_eq!(cancelled.event_type, "run.cancelled");
        assert_eq!(
            cancelled
                .metadata
                .get(DISPATCH_OUTBOX_QUEUED_OFFSET_META)
                .map(String::as_str),
            Some("4")
        );
        assert_eq!(
            cancelled.metadata.get("cancel_reason").map(String::as_str),
            Some(crate::retired_deployments::DEPLOYMENT_RETIRED_REASON)
        );
    }

    #[tokio::test]
    async fn malformed_payload_is_dead_lettered_and_deleted() {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        store.put_dispatch_intent(0, "t1", 7, "run-poison", b"not-json".to_vec());

        let (dispatch_tx, _dispatch_rx) = mpsc::channel::<DispatchEnvelope>(8);
        let cancel = CancellationToken::new();
        let drainer = tokio::spawn(run_partition_dispatch_outbox_drainer(
            0,
            "node-test".to_string(),
            store.clone(),
            dispatch_tx,
            segment.clone(),
            None,
            true_rx(),
            true_rx(),
            Arc::new(Notify::new()),
            cancel.clone(),
        ));

        for _ in 0..50 {
            if store.dispatch_intents(0).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cancel.cancel();
        drainer.await.unwrap();

        assert!(store.dispatch_intents(0).is_empty());
        let records = segment.read_range(Offset::ZERO, Offset(10)).await.unwrap();
        assert!(records
            .iter()
            .any(|record| { record.run_id == "run-poison" && record.event_type == "run.failed" }));
    }
}
