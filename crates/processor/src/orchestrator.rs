//! Processor orchestration decision logic.
//!
//! Evaluates pre-projection run state and decides what action should happen
//! next. When enabled (`AGNT5_PROCESSOR_ORCHESTRATES_RUNS=true`), the processor
//! dispatches runs and writes `run.assigned` after the worker accepts the job.
//!
//! The coordinator engine consumer is disabled for `run.queued` dispatch when
//! this is active, ensuring only one dispatch source at a time.

use std::sync::OnceLock;

use bytes::Bytes;
use engine_core::types::{Offset, Record};
use engine_proto::{DispatchComponentRequest, Run};
use prost::Message;
use tracing::{debug, info};

use crate::run_state_machine::{
    self, RunStatus, READY_AT_MS_META, RETRY_FROM_OFFSET_META, RETRY_INITIAL_BACKOFF_MS_META,
    RETRY_MAX_BACKOFF_MS_META, RETRY_READY_AT_MS_META, RETRY_SOURCE_META, RETRY_SOURCE_RUN_FAILED,
};

/// Action the processor decides after evaluating a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestratorAction {
    /// Run is eligible for dispatch to a worker.
    Dispatch {
        project_id: String,
        run_id: String,
        component_name: String,
    },
    /// Failed run is eligible for retry.
    Retry {
        project_id: String,
        run_id: String,
        attempt: i32,
        max_attempts: i32,
    },
    /// No orchestration action needed.
    NoOp,
}

/// Lease fields returned from the dispatch consumer for PUSH dispatches. The
/// processor uses these to stamp lease metadata on the `run.assigned` record
/// so the projection populates `Run.lease_*` fields and the diff-driven index
/// emitter writes `CF_LEASES_BY_EXPIRY` / `CF_LEASES_BY_WORKER`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseFields {
    pub lease_id: String,
    pub lease_worker_id: String,
    pub lease_session_hash: String,
    pub lease_expires_at_ms: i64,
    pub lease_timeout_ms: i64,
}

/// Envelope for dispatch requests sent through the mpsc channel.
/// The processor sends the request and waits for an ack from the dispatch consumer.
///
/// The ack carries `Option<LeaseFields>`: `Some(fields)` when dispatch minted
/// a lease, `None` only for the explicit legacy opt-out path.
pub struct DispatchEnvelope {
    pub request: DispatchComponentRequest,
    pub ack: tokio::sync::oneshot::Sender<Result<Option<LeaseFields>, String>>,
}

const PINNED_WORKERLESS_DISPATCH_METADATA_KEYS: &[&str] = &[
    "dispatch_mode",
    "workerless_endpoint",
    "workerless_protocol_version",
    "workerless_immutable_ref",
    "workerless_manifest_hash",
    "workerless_request_timeout_ms",
    "workerless_yield_before_timeout_ms",
    "workerless_max_payload_bytes",
    "workerless_payload_ref_threshold_bytes",
    "workerless_concurrency_limit",
    "workerless_concurrency_scope",
    "workerless_concurrency_key",
    "workerless_concurrency_key_expression",
    "workerless_throttle_limit",
    "workerless_throttle_window_ms",
    "workerless_throttle_key",
    "workerless_throttle_key_expression",
    "workerless_rate_limit_limit",
    "workerless_rate_limit_window_ms",
    "workerless_rate_limit_key",
    "workerless_rate_limit_key_expression",
    "workerless_debounce_window_ms",
    "workerless_debounce_key",
    "workerless_debounce_key_expression",
    "workerless_debounce_queued_at_ms",
    "workerless_batch_max_size",
    "workerless_batch_window_ms",
    "workerless_batch_key",
    "workerless_batch_key_expression",
    "workerless_idempotency_key",
    "workerless_idempotency_key_expression",
    "workerless_idempotency_ttl_ms",
    "workerless_singleton_key",
    "workerless_singleton_key_expression",
    "workerless_singleton_mode",
];
const WORKERLESS_CREDENTIAL_METADATA_KEYS: &[&str] =
    &["workerless_signing_secret", "workerless_invoke_headers"];
const STALE_RETRY_ASSIGNMENT_METADATA_KEYS: &[&str] = &[
    "lease_id",
    "worker_id",
    "worker_session_id",
    "lease_worker_id",
    "lease_expires_at_ms",
    "lease_created_at_ms",
    "lease_timeout_ms",
    engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH,
    engine_core::lease_fence::RUN_META_LEASE_ATTEMPT,
    engine_core::lease_fence::RUN_META_GRANT_ID,
    engine_core::lease_fence::RUN_META_GRANT_REQUEST_FINGERPRINT,
];
const WORKERLESS_PAYLOAD_TOO_LARGE_CODE: &str = "WORKERLESS_PAYLOAD_TOO_LARGE";

/// Whether processor orchestration is enabled.
///
/// Reads `AGNT5_PROCESSOR_ORCHESTRATES_RUNS` once and caches the result.
pub fn is_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("AGNT5_PROCESSOR_ORCHESTRATES_RUNS")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
    })
}

/// Whether lease-backed PUSH dispatch is enabled.
///
/// Default is on: the dispatch consumer mints a lease (`lease_id`,
/// `lease_worker_id`, `expires_at_ms`) after worker selection and the processor
/// stamps those fields onto `run.assigned` metadata so the projection
/// populates `Run.lease_*` and the secondary lease indexes.
///
/// `AGNT5_LEASE_BACKED_PUSH=0|false|FALSE|no` is an emergency opt-out for
/// rollback. Reads once and caches the result.
pub fn lease_backed_push_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("AGNT5_LEASE_BACKED_PUSH")
            .map(|v| parse_lease_backed_push_env(&v))
            .unwrap_or(true)
    })
}

fn parse_lease_backed_push_env(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no"
    )
}

/// Default lease timeout for PUSH dispatches (2 minutes).
///
/// Matches the PULL default. Per-dispatch and component-level overrides take
/// precedence over this system default.
pub const DEFAULT_PUSH_LEASE_TIMEOUT_MS: i64 = 120_000;

/// Evaluate what orchestration action a record implies.
///
/// Pure function: no I/O, no async, no side effects. Inspects the event type,
/// record metadata, and **pre-projection** run state to decide the next action.
///
/// `existing_run` is the serialized Run proto *before* this record is applied.
/// This enables replay-safe deduplication: if a run has already progressed past
/// `queued`, the processor won't re-dispatch on replay.
pub fn evaluate(record: &Record, existing_run: Option<&[u8]>) -> OrchestratorAction {
    // An observed run is built from a customer's traces and no worker runs
    // it, so there is nothing to dispatch, retry or resume.
    if engine_core::journal_contract::is_observed_record(record) {
        debug!(run_id = %record.run_id, event_type = %record.event_type, "Orchestrator: skip observed run");
        return OrchestratorAction::NoOp;
    }
    match record.event_type.as_str() {
        "run.queued" => evaluate_queued(record, existing_run),
        "run.lease_expired" => evaluate_lease_expired(record, existing_run),
        "run.resumed" => evaluate_resumed(record, existing_run),
        "run.failed" => evaluate_failed(record, existing_run),
        _ => OrchestratorAction::NoOp,
    }
}

fn evaluate_queued(record: &Record, existing_run: Option<&[u8]>) -> OrchestratorAction {
    let component_name = record
        .metadata
        .get("component_name")
        .cloned()
        .unwrap_or_default();

    if component_name.is_empty() {
        debug!(run_id = %record.run_id, "Orchestrator: skip run.queued without component_name");
        return OrchestratorAction::NoOp;
    }

    // Replay deduplication: check pre-projection state.
    //
    // Only in-flight states short-circuit. A `queued` existing status does
    // NOT — it typically means the processor crashed after the projection
    // applied but before the dispatch acked (e.g. during the worker
    // hold-off window). On restart we replay from the last checkpoint,
    // the projection is idempotent, and the orchestrator must re-dispatch
    // so the run can actually make forward progress. At-least-once
    // delivery is safe because the coordinator fences by `lease_id` and
    // workers are idempotent by `invocation_id`.
    if let Some(data) = existing_run {
        if let Ok(run) = Run::decode(data) {
            if let Some(status) = RunStatus::from_str(&run.status) {
                match status {
                    // Run has already been dispatched or progressed — a live
                    // worker owns it, skip.
                    RunStatus::Assigned | RunStatus::Started | RunStatus::Paused => {
                        debug!(
                            run_id = %record.run_id,
                            status = %status,
                            "Orchestrator: skip run.queued — run already in progress"
                        );
                        return OrchestratorAction::NoOp;
                    }
                    // Queued can be replayed after the projection committed but
                    // before dispatch acknowledged. Failed can be reopened only
                    // by a processor-built, monotonic retry record.
                    RunStatus::Queued => {
                        debug!(
                            run_id = %record.run_id,
                            status = %status,
                            "Orchestrator: (re)dispatch run.queued"
                        );
                        // Fall through to dispatch
                    }
                    RunStatus::RetryDelayed
                        if run_state_machine::is_authorized_failed_retry(
                            status,
                            run.attempt,
                            &record.metadata,
                        ) =>
                    {
                        debug!(
                            run_id = %record.run_id,
                            attempt = run.attempt,
                            "Orchestrator: dispatch authorized failed-run retry"
                        );
                    }
                    RunStatus::RetryDelayed
                    | RunStatus::Terminalizing
                    | RunStatus::Completed
                    | RunStatus::Failed
                    | RunStatus::Cancelled => {
                        debug!(
                            run_id = %record.run_id,
                            status = %status,
                            "Orchestrator: skip unauthorized run.queued after terminal state"
                        );
                        return OrchestratorAction::NoOp;
                    }
                }
            }
        }
    }

    OrchestratorAction::Dispatch {
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        component_name,
    }
}

fn evaluate_resumed(record: &Record, existing_run: Option<&[u8]>) -> OrchestratorAction {
    let component_name = record
        .metadata
        .get("component_name")
        .cloned()
        .unwrap_or_default();

    if component_name.is_empty() {
        debug!(run_id = %record.run_id, "Orchestrator: skip run.resumed without component_name");
        return OrchestratorAction::NoOp;
    }

    let Some(data) = existing_run else {
        debug!(
            run_id = %record.run_id,
            "Orchestrator: skip run.resumed without existing paused run"
        );
        return OrchestratorAction::NoOp;
    };

    let Ok(run) = Run::decode(data) else {
        debug!(run_id = %record.run_id, "Orchestrator: skip run.resumed with undecodable run");
        return OrchestratorAction::NoOp;
    };

    let Some(status) = RunStatus::from_str(&run.status) else {
        debug!(
            run_id = %record.run_id,
            status = %run.status,
            "Orchestrator: skip run.resumed with unknown status"
        );
        return OrchestratorAction::NoOp;
    };

    if !matches!(status, RunStatus::Paused | RunStatus::Queued) {
        debug!(
            run_id = %record.run_id,
            status = %status,
            "Orchestrator: skip run.resumed when run is neither paused nor queued"
        );
        return OrchestratorAction::NoOp;
    }

    OrchestratorAction::Dispatch {
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        component_name,
    }
}

fn evaluate_lease_expired(record: &Record, existing_run: Option<&[u8]>) -> OrchestratorAction {
    let Some(data) = existing_run else {
        debug!(
            run_id = %record.run_id,
            "Orchestrator: skip run.lease_expired without existing active run"
        );
        return OrchestratorAction::NoOp;
    };
    let Ok(run) = Run::decode(data) else {
        debug!(
            run_id = %record.run_id,
            "Orchestrator: skip run.lease_expired with undecodable run"
        );
        return OrchestratorAction::NoOp;
    };
    let Some(status) = RunStatus::from_str(&run.status) else {
        return OrchestratorAction::NoOp;
    };
    if !matches!(
        status,
        RunStatus::Assigned | RunStatus::Started | RunStatus::Queued
    ) {
        debug!(
            run_id = %record.run_id,
            status = %status,
            "Orchestrator: skip run.lease_expired after run progressed"
        );
        return OrchestratorAction::NoOp;
    }

    let component_name = record
        .metadata
        .get("component_name")
        .filter(|name| !name.is_empty())
        .cloned()
        .unwrap_or(run.component_name);
    if component_name.is_empty() {
        debug!(
            run_id = %record.run_id,
            "Orchestrator: skip run.lease_expired without component_name"
        );
        return OrchestratorAction::NoOp;
    }

    OrchestratorAction::Dispatch {
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        component_name,
    }
}

fn evaluate_failed(record: &Record, existing_run: Option<&[u8]>) -> OrchestratorAction {
    let Some((next_attempt, max_attempts)) =
        run_state_machine::next_retry_attempt(&record.metadata)
    else {
        return OrchestratorAction::NoOp;
    };

    // The stored attempt must still be the attempt that failed. This permits
    // replay after the failed projection committed but before its retry record
    // was appended, while rejecting a stale failure after a later attempt has
    // already been materialized.
    if let Some(data) = existing_run {
        if let Ok(run) = Run::decode(data) {
            if let Some(status) = RunStatus::from_str(&run.status) {
                if matches!(
                    status,
                    RunStatus::Terminalizing
                        | RunStatus::Completed
                        | RunStatus::Failed
                        | RunStatus::Cancelled
                ) || run.attempt.saturating_add(1) != next_attempt
                {
                    debug!(
                        run_id = %record.run_id,
                        status = %status,
                        stored_attempt = run.attempt,
                        next_attempt,
                        "Orchestrator: skip stale retry decision"
                    );
                    return OrchestratorAction::NoOp;
                }
            }
        }
    }

    OrchestratorAction::Retry {
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        attempt: next_attempt as i32,
        max_attempts: max_attempts as i32,
    }
}

// ---------------------------------------------------------------------------
// Record builders
// ---------------------------------------------------------------------------

/// Build a `DispatchComponentRequest` from a `run.queued` journal record.
///
/// Field mapping matches `engine_consumer::dispatch_to_worker` in the coordinator.
pub fn build_dispatch_request(record: &Record) -> DispatchComponentRequest {
    let component_name = record
        .metadata
        .get("component_name")
        .cloned()
        .unwrap_or_default();
    let component_type_str = record
        .metadata
        .get("component_type")
        .cloned()
        .unwrap_or_default();
    let service_name = record
        .metadata
        .get("service_name")
        .cloned()
        .unwrap_or_default();

    let component_type = parse_component_type(&component_type_str);

    // Include project_id so the worker writes events scoped to the right
    // project. The metadata key `tenant_id` is reserved for the
    // customer-facing sub-tenant carried by the gateway via
    // `tenant_middleware`.
    let is_streaming = streaming_requested(&record.metadata);
    let attempt = dispatch_attempt(record);
    let mut metadata = derived_dispatch_generation_metadata(record);
    if !record.project_id.is_empty() {
        metadata.insert("project_id".to_string(), record.project_id.clone());
    }
    metadata.insert("attempt".to_string(), attempt.to_string());

    // Derive the default retry policy from the priority the
    // gateway already stamped in `stamp_dispatch_metadata`. Sync
    // endpoints get TERMINAL (fail fast to the waiting caller); async
    // endpoints get BOUNDED(3, 1s, 60s) so the coordinator can absorb
    // transient failures without the client noticing. Legacy ingress
    // without a `priority` key falls through to the NORMAL default.
    let priority = priority_from_metadata(&metadata);
    let retry_policy = Some(default_retry_policy_for_priority(priority));

    // Stamp max_attempts into metadata so the SDK propagates it through
    // events and evaluate_failed can read it from run.failed metadata.
    // The engine_dispatcher may override this with the component's declared
    // retry config (e.g. retries=2) if available.
    if let Some(ref rp) = retry_policy {
        metadata
            .entry("max_attempts".to_string())
            .or_insert_with(|| rp.max_attempts.to_string());
        metadata
            .entry(RETRY_INITIAL_BACKOFF_MS_META.to_string())
            .or_insert_with(|| rp.initial_backoff_ms.to_string());
        metadata
            .entry(RETRY_MAX_BACKOFF_MS_META.to_string())
            .or_insert_with(|| rp.max_backoff_ms.to_string());
    }

    DispatchComponentRequest {
        invocation_id: record.run_id.clone(),
        service_name,
        component_type,
        component_name,
        input_data: record.data.to_vec(),
        deployment_id: metadata.get("deployment_id").cloned().unwrap_or_default(),
        metadata,
        session_id: record
            .metadata
            .get("session_id")
            .cloned()
            .unwrap_or_else(|| record.run_id.clone()),
        is_streaming,
        attempt,
        priority: priority as i32,
        retry_policy,
        ..Default::default()
    }
}

/// Rehydrate a sparse lifecycle record before using it as a dispatch source.
///
/// Lease-expiry and compact durable-retry records intentionally omit the
/// original input. The durable `Run` remains the source of truth for immutable
/// execution context such as deployment, component type, input, and original
/// dispatch metadata.
/// Timer resumes can omit a large continuation retained on the run. Restore
/// only that field so stale answers and lease metadata stay removed.
pub(crate) fn hydrate_dispatch_source_record(
    record: &Record,
    existing_run: Option<&[u8]>,
) -> Record {
    let is_dispatch_retry =
        record.event_type == crate::dispatch_outbox_drain::DISPATCH_RETRY_SCHEDULED_EVENT;
    if record.event_type != "run.lease_expired"
        && record.event_type != "run.resumed"
        && !is_dispatch_retry
    {
        return record.clone();
    }

    let Some(run) = existing_run.and_then(|bytes| Run::decode(bytes).ok()) else {
        return record.clone();
    };

    let mut hydrated = record.clone();
    if record.event_type == "run.resumed" {
        use engine_core::journal_contract::META_CONTINUATION_B64;

        if let Some(continuation) = run.metadata.get(META_CONTINUATION_B64) {
            hydrated
                .metadata
                .entry(META_CONTINUATION_B64.to_string())
                .or_insert_with(|| continuation.clone());
        }
        return hydrated;
    }

    let mut metadata = run.metadata;
    metadata.extend(hydrated.metadata);
    strip_workerless_credentials(&mut metadata);
    hydrated.metadata = metadata;

    for (key, value) in [
        ("component_name", run.component_name),
        ("component_type", run.component_type),
        ("deployment_id", run.deployment_id),
    ] {
        if !value.is_empty()
            && hydrated
                .metadata
                .get(key)
                .is_none_or(|current| current.is_empty())
        {
            hydrated.metadata.insert(key.to_string(), value);
        }
    }
    if hydrated.data.is_empty() && !run.input_data.is_empty() {
        hydrated.data = Bytes::from(run.input_data);
    }

    if is_dispatch_retry {
        use crate::dispatch_outbox_drain::{
            DISPATCH_RETRY_ATTEMPT_META, DISPATCH_RETRY_NEXT_ATTEMPT_AT_MS_META,
            DISPATCH_RETRY_OUTBOX_INSERTED_AT_MS_META, DISPATCH_RETRY_SOURCE_CORRELATION_ID_META,
            DISPATCH_RETRY_SOURCE_EVENT_TYPE_META, DISPATCH_RETRY_SOURCE_PARENT_EVENT_ID_META,
            DISPATCH_RETRY_SOURCE_STEP_KEY_META, DISPATCH_RETRY_SOURCE_TIMESTAMP_NS_META,
        };

        if let Some(event_type) = hydrated
            .metadata
            .get(DISPATCH_RETRY_SOURCE_EVENT_TYPE_META)
            .cloned()
        {
            hydrated.event_type = event_type;
        }
        if let Some(timestamp_ns) = hydrated
            .metadata
            .get(DISPATCH_RETRY_SOURCE_TIMESTAMP_NS_META)
            .and_then(|value| value.parse::<i64>().ok())
        {
            hydrated.timestamp_ns = timestamp_ns;
        }
        hydrated.step_key = hydrated
            .metadata
            .get(DISPATCH_RETRY_SOURCE_STEP_KEY_META)
            .cloned();
        hydrated.correlation_id = hydrated
            .metadata
            .get(DISPATCH_RETRY_SOURCE_CORRELATION_ID_META)
            .cloned();
        hydrated.parent_event_id = hydrated
            .metadata
            .get(DISPATCH_RETRY_SOURCE_PARENT_EVENT_ID_META)
            .cloned();
        for key in [
            DISPATCH_RETRY_ATTEMPT_META,
            DISPATCH_RETRY_NEXT_ATTEMPT_AT_MS_META,
            DISPATCH_RETRY_OUTBOX_INSERTED_AT_MS_META,
            DISPATCH_RETRY_SOURCE_EVENT_TYPE_META,
            DISPATCH_RETRY_SOURCE_TIMESTAMP_NS_META,
            DISPATCH_RETRY_SOURCE_STEP_KEY_META,
            DISPATCH_RETRY_SOURCE_CORRELATION_ID_META,
            DISPATCH_RETRY_SOURCE_PARENT_EVENT_ID_META,
        ] {
            hydrated.metadata.remove(key);
        }
        hydrated.data_type = "json".to_string();
    }

    hydrated
}

fn streaming_requested(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata
        .get("is_streaming")
        .map(|value| value == "true")
        .unwrap_or(false)
        || metadata
            .get("stream_mode")
            .map(|value| value == "full")
            .unwrap_or(false)
}

/// Default retry policy per priority class.
///
/// - `Interactive` → `TERMINAL`. Synchronous `/run`/`/stream` must surface
///   failures immediately to the waiting caller; absorbing them inside the
///   coordinator would leave the client waiting.
/// - `Normal`, `Batch`, `Unspecified` → `BOUNDED(3, 1s, 60s)`. Async
///   `/submit`/`/batch` (and anything that falls back to NORMAL via the
///   projection's canonicalisation) can tolerate transient failures
///   with exponential backoff.
///
/// Keeping this as a pure free function makes the mapping easy to unit
/// test without constructing a full `Record`.
pub fn default_retry_policy_for_priority(
    priority: engine_proto::Priority,
) -> engine_proto::RetryPolicy {
    use engine_proto::retry_policy::Kind;
    match priority {
        engine_proto::Priority::Interactive => engine_proto::RetryPolicy {
            kind: Kind::Terminal as i32,
            max_attempts: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
        // Unspecified canonicalises to Normal upstream (projections/runs.rs),
        // so we give it the same default here for consistency.
        engine_proto::Priority::Unspecified
        | engine_proto::Priority::Normal
        | engine_proto::Priority::Batch => engine_proto::RetryPolicy {
            kind: Kind::Bounded as i32,
            max_attempts: 3,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
        },
    }
}

/// Resolve the retry policy from a `run.queued` record's
/// metadata. Reads the `priority` key (stamped by the gateway as the
/// i32 discriminant) and maps it to the default for that class.
///
/// Falls back to `Unspecified` if the key is missing or unparseable —
/// the default function then canonicalises that to the `Normal`
/// default, matching how the projection handles legacy/untagged
/// records.
fn priority_from_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> engine_proto::Priority {
    metadata
        .get("priority")
        .and_then(|v| v.parse::<i32>().ok())
        .and_then(|i| engine_proto::Priority::try_from(i).ok())
        .unwrap_or(engine_proto::Priority::Unspecified)
}

fn copy_pinned_workerless_dispatch_metadata(
    metadata: &mut std::collections::HashMap<String, String>,
    source: &std::collections::HashMap<String, String>,
) {
    for key in PINNED_WORKERLESS_DISPATCH_METADATA_KEYS {
        if let Some(value) = source.get(*key).filter(|value| !value.trim().is_empty()) {
            metadata.insert((*key).to_string(), value.clone());
        }
    }
}

/// Start metadata for a new dispatch generation without carrying authority
/// from the generation that produced the source record.
///
/// In particular, `run.lease_expired` carries the lease fence of the lease it
/// released. If those fence keys reached the derived dispatch or
/// `run.assigned`, the new generation would look authored by a dead lease.
fn derived_dispatch_generation_metadata(
    record: &Record,
) -> std::collections::HashMap<String, String> {
    let mut metadata = record.metadata.clone();
    sanitize_dispatch_generation_metadata(&mut metadata);
    metadata
}

pub(crate) fn strip_workerless_credentials(
    metadata: &mut std::collections::HashMap<String, String>,
) {
    for key in WORKERLESS_CREDENTIAL_METADATA_KEYS {
        metadata.remove(*key);
    }
}

/// Remove server-owned authority from metadata copied into a new dispatch.
///
/// Dispatch outbox compatibility recovery also uses this after merging fields
/// from the source record. Otherwise an expired lease's fence keys can be
/// resurrected after `build_dispatch_request` stripped them.
pub(crate) fn sanitize_dispatch_generation_metadata(
    metadata: &mut std::collections::HashMap<String, String>,
) {
    engine_core::lease_fence::strip_reserved_fence_metadata(metadata);
    for key in STALE_RETRY_ASSIGNMENT_METADATA_KEYS {
        metadata.remove(*key);
    }
    // Workers never see lease-loss metadata, so they can't echo it back.
    run_state_machine::strip_runtime_lease_loss_metadata(metadata);
    strip_workerless_credentials(metadata);
}

/// Build a `run.assigned` journal record from the original `run.queued` record.
///
/// When `lease` is `Some`, stamps lease metadata keys (`lease_id`,
/// `lease_worker_id`, `lease_session_hash`, `lease_expires_at_ms`,
/// `lease_timeout_ms`) onto the record metadata so the `apply_assigned` projection populates the
/// corresponding `Run.lease_*` fields. When `lease` is `None`, leaves those
/// fields unset for compatibility with dispatches that do not mint leases.
pub fn build_assigned_record(record: &Record, lease: Option<&LeaseFields>) -> Record {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    let attempt = dispatch_attempt(record);
    let mut metadata = derived_dispatch_generation_metadata(record);
    metadata.insert("attempt".to_string(), attempt.to_string());
    if let Some(lf) = lease {
        if !metadata
            .get("dispatch_mode")
            .is_some_and(|mode| mode.eq_ignore_ascii_case("http"))
        {
            metadata.insert("dispatch_mode".to_string(), "push".to_string());
        }
        metadata.insert("lease_id".to_string(), lf.lease_id.clone());
        metadata.insert("lease_worker_id".to_string(), lf.lease_worker_id.clone());
        metadata.insert(
            engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH.to_string(),
            lf.lease_session_hash.clone(),
        );
        metadata.insert(
            "lease_expires_at_ms".to_string(),
            lf.lease_expires_at_ms.to_string(),
        );
        metadata.insert(
            "lease_timeout_ms".to_string(),
            lf.lease_timeout_ms.to_string(),
        );
        metadata.insert(
            engine_core::lease_fence::RUN_META_LEASE_ATTEMPT.to_string(),
            attempt.to_string(),
        );
    }

    Record {
        offset: Offset::ZERO, // assigned by segment on append
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        event_type: "run.assigned".to_string(),
        data: Bytes::from(b"{}".as_slice()),
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

fn dispatch_attempt(record: &Record) -> i32 {
    let attempt = if record.event_type == "run.lease_expired" {
        record
            .metadata
            .get(engine_core::lease_fence::META_FENCE_ATTEMPT)
            .or_else(|| record.metadata.get("attempt"))
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or_default()
            .saturating_add(1)
    } else {
        record
            .metadata
            .get("attempt")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or_default()
    };
    attempt.max(0)
}

/// Build a `run.failed` journal record for dispatch errors.
pub fn build_failed_record(record: &Record, error_message: &str) -> Record {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    let error_code = dispatch_error_code(error_message);

    let error_data = serde_json::json!({
        "error_message": error_message,
        "error_code": error_code,
    });
    let attempt = dispatch_attempt(record);
    let mut metadata = derived_dispatch_generation_metadata(record);
    metadata.insert("attempt".to_string(), attempt.to_string());
    metadata.insert("error_code".to_string(), error_code.to_string());
    if error_code == WORKERLESS_PAYLOAD_TOO_LARGE_CODE {
        metadata.insert("workerless_retryable".to_string(), "false".to_string());
        let attempt = metadata
            .get("attempt")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0);
        metadata.insert(
            "max_attempts".to_string(),
            attempt.saturating_add(1).to_string(),
        );
    }

    Record {
        offset: Offset::ZERO,
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        event_type: "run.failed".to_string(),
        data: Bytes::from(serde_json::to_vec(&error_data).unwrap_or_default()),
        timestamp_ns: now_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

fn dispatch_error_code(error_message: &str) -> &'static str {
    if error_message.contains("workerless input payload exceeds")
        || error_message.contains("workerless output payload exceeds")
    {
        WORKERLESS_PAYLOAD_TOO_LARGE_CODE
    } else {
        "DISPATCH_FAILED"
    }
}

/// Build a new `run.queued` journal record for a retry attempt.
///
/// The caller triggers this when `evaluate_failed` returns `Retry`. Since the
/// failed record's `data` holds error info (not input), we rehydrate the
/// original `input_data` and component/trace metadata from the prior Run
/// state so the retried dispatch is a faithful replay of the original queue
/// event, just with an incremented `attempt` counter.
pub fn build_retry_record(
    failed_record: &Record,
    existing_run: Option<&[u8]>,
    attempt: i32,
    max_attempts: i32,
) -> Record {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    // Start from the failed record's metadata (carries original keys forwarded
    // through the run lifecycle), then overlay anything still needed from the
    // prior Run state that might have been stripped along the way.
    let mut metadata = failed_record.metadata.clone();
    engine_core::lease_fence::strip_reserved_fence_metadata(&mut metadata);
    for key in STALE_RETRY_ASSIGNMENT_METADATA_KEYS {
        metadata.remove(*key);
    }
    // The failed record may carry a stale lease-loss history or an earlier
    // attempt's cause. The projection keeps the run's own history across the
    // retry.
    run_state_machine::strip_runtime_lease_loss_metadata(&mut metadata);
    let mut input_data = Bytes::new();

    if let Some(bytes) = existing_run {
        if let Ok(run) = Run::decode(bytes) {
            if !run.input_data.is_empty() {
                input_data = Bytes::from(run.input_data);
            }
            copy_pinned_workerless_dispatch_metadata(&mut metadata, &run.metadata);
            // Component routing fields — apply_queued rebuilds Run from
            // record.metadata, so these must be present on the retry record.
            if !run.component_name.is_empty() {
                metadata
                    .entry("component_name".to_string())
                    .or_insert(run.component_name);
            }
            if !run.component_type.is_empty() {
                metadata
                    .entry("component_type".to_string())
                    .or_insert(run.component_type);
            }
            if !run.deployment_id.is_empty() {
                metadata
                    .entry("deployment_id".to_string())
                    .or_insert(run.deployment_id);
            }
            // Preserve W3C trace context so the retried run stays on the same
            // trace as the original gateway-initiated execution.
            for key in ["traceparent", "baggage"] {
                if let Some(v) = run.metadata.get(key) {
                    metadata.entry(key.to_string()).or_insert_with(|| v.clone());
                }
            }
        }
    }

    // Bump attempt fencing counters.
    metadata.insert("attempt".to_string(), attempt.to_string());
    metadata.insert("max_attempts".to_string(), max_attempts.to_string());
    metadata.insert(
        RETRY_SOURCE_META.to_string(),
        RETRY_SOURCE_RUN_FAILED.to_string(),
    );
    metadata.insert(
        RETRY_FROM_OFFSET_META.to_string(),
        failed_record.offset.0.to_string(),
    );
    if let Some(ready_at_ms) =
        run_state_machine::retry_ready_at_ms(failed_record.timestamp_ns, &failed_record.metadata)
    {
        metadata.insert(RETRY_READY_AT_MS_META.to_string(), ready_at_ms.to_string());
        metadata.insert(READY_AT_MS_META.to_string(), ready_at_ms.to_string());
    }

    strip_workerless_credentials(&mut metadata);

    Record {
        offset: Offset::ZERO,
        project_id: failed_record.project_id.clone(),
        run_id: failed_record.run_id.clone(),
        event_type: "run.queued".to_string(),
        data: input_data,
        timestamp_ns: now_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

/// Log an orchestrator decision.
pub fn log_action(action: &OrchestratorAction) {
    match action {
        OrchestratorAction::Dispatch {
            run_id,
            component_name,
            ..
        } => {
            info!(
                run_id = %run_id,
                component = %component_name,
                source = "processor",
                "Orchestrator: DISPATCH"
            );
        }
        OrchestratorAction::Retry {
            run_id,
            attempt,
            max_attempts,
            ..
        } => {
            info!(
                run_id = %run_id,
                attempt = attempt,
                max_attempts = max_attempts,
                source = "processor",
                "Orchestrator: RETRY"
            );
        }
        OrchestratorAction::NoOp => {}
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_component_type(s: &str) -> i32 {
    match s.to_lowercase().as_str() {
        "function" | "functions" => engine_proto::ComponentType::Function.into(),
        "workflow" | "workflows" => engine_proto::ComponentType::Workflow.into(),
        "agent" | "agents" => engine_proto::ComponentType::Agent.into(),
        "tool" | "tools" => engine_proto::ComponentType::Tool.into(),
        "entity" | "entities" => engine_proto::ComponentType::Entity.into(),
        "scorer" | "scorers" => engine_proto::ComponentType::Scorer.into(),
        _ => engine_proto::ComponentType::Unspecified.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_record(event_type: &str, metadata: HashMap<String, String>) -> Record {
        make_record_with_data(event_type, metadata, Bytes::new())
    }

    fn make_record_with_data(
        event_type: &str,
        metadata: HashMap<String, String>,
        data: Bytes,
    ) -> Record {
        Record {
            offset: Offset(0),
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            event_type: event_type.to_string(),
            data,
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn make_run(status: &str) -> Vec<u8> {
        Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: status.to_string(),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[test]
    fn observed_run_is_never_dispatched() {
        let meta = HashMap::from([
            ("component_name".to_string(), "my_func".to_string()),
            (
                engine_core::journal_contract::META_ORIGIN.to_string(),
                engine_core::journal_contract::ORIGIN_OTLP.to_string(),
            ),
        ]);
        for event_type in ["run.queued", "run.failed", "run.lease_expired"] {
            let record = make_record(event_type, meta.clone());
            assert_eq!(evaluate(&record, None), OrchestratorAction::NoOp);
        }
    }

    #[test]
    fn queued_new_run_produces_dispatch() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.queued", meta);

        assert_eq!(
            evaluate(&record, None),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_func".to_string(),
            }
        );
    }

    #[test]
    fn queued_with_existing_queued_status_redispatches() {
        // Replay case: processor crashed after apply_queued projection wrote
        // status=queued but before the dispatch ack. On restart we replay
        // from the checkpoint, the projection re-applies idempotently, and
        // the orchestrator must re-dispatch so the run can make progress.
        // Lease fencing + worker-side idempotency by invocation_id keep
        // re-dispatch safe.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.queued", meta);
        let existing = make_run("queued");

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_func".to_string(),
            }
        );
    }

    #[test]
    fn queued_already_assigned_produces_noop() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.queued", meta);
        let existing = make_run("assigned");

        assert_eq!(evaluate(&record, Some(&existing)), OrchestratorAction::NoOp);
    }

    #[test]
    fn queued_already_started_produces_noop() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.queued", meta);
        let existing = make_run("started");

        assert_eq!(evaluate(&record, Some(&existing)), OrchestratorAction::NoOp);
    }

    #[test]
    fn unmarked_queued_after_terminal_produces_noop() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.queued", meta);
        let existing = make_run("failed");

        assert_eq!(evaluate(&record, Some(&existing)), OrchestratorAction::NoOp);
    }

    #[test]
    fn authorized_retry_after_retry_delay_produces_dispatch() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("attempt".to_string(), "1".to_string());
        meta.insert("max_attempts".to_string(), "3".to_string());
        meta.insert(
            RETRY_SOURCE_META.to_string(),
            RETRY_SOURCE_RUN_FAILED.to_string(),
        );
        let record = make_record("run.queued", meta);
        let mut run = Run::decode(make_run("retry_delayed").as_slice()).unwrap();
        run.attempt = 0;
        let existing = run.encode_to_vec();

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_func".to_string(),
            }
        );
    }

    #[test]
    fn batch_queued_dispatches_like_any_other_run() {
        // Batch items go through the unified lease-backed PUSH path
        // alongside /run and /submit. `batch_id` is just a grouping tag for
        // observability — it must not short-circuit dispatch.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("batch_id".to_string(), "b1".to_string());
        let record = make_record("run.queued", meta);

        assert_eq!(
            evaluate(&record, None),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_func".to_string(),
            }
        );
    }

    #[test]
    fn queued_without_component_produces_noop() {
        let record = make_record("run.queued", HashMap::new());
        assert_eq!(evaluate(&record, None), OrchestratorAction::NoOp);
    }

    #[test]
    fn resumed_paused_run_produces_dispatch() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_workflow".to_string());
        let record = make_record("run.resumed", meta);
        let existing = make_run("paused");

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_workflow".to_string(),
            }
        );
    }

    #[test]
    fn resumed_queued_run_produces_dispatch() {
        // Projection may apply run.resumed before the orchestrator evaluates it.
        // In that ordering, the existing run is already queued and still needs
        // dispatch; treating queued as stale loses the resume permanently.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_workflow".to_string());
        let record = make_record("run.resumed", meta);
        let existing = make_run("queued");

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_workflow".to_string(),
            }
        );
    }

    #[test]
    fn resumed_started_run_produces_noop() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_workflow".to_string());
        let record = make_record("run.resumed", meta);
        let existing = make_run("started");

        assert_eq!(evaluate(&record, Some(&existing)), OrchestratorAction::NoOp);
    }

    #[test]
    fn resumed_without_existing_run_produces_noop() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_workflow".to_string());
        let record = make_record("run.resumed", meta);

        assert_eq!(evaluate(&record, None), OrchestratorAction::NoOp);
    }

    #[test]
    fn lease_expiry_dispatches_the_next_attempt() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.lease_expired", meta);
        let existing = make_run("started");

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Dispatch {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                component_name: "my_func".to_string(),
            }
        );
    }

    #[test]
    fn lease_expiry_replay_from_queued_state_recreates_dispatch() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.lease_expired", meta);
        let existing = make_run("queued");

        assert!(matches!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Dispatch { .. }
        ));
    }

    #[test]
    fn stale_lease_expiry_after_terminal_state_does_not_dispatch() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.lease_expired", meta);

        assert_eq!(
            evaluate(&record, Some(&make_run("completed"))),
            OrchestratorAction::NoOp
        );
        assert_eq!(evaluate(&record, None), OrchestratorAction::NoOp);
    }

    #[test]
    fn failed_with_retries_produces_retry() {
        let mut meta = HashMap::new();
        meta.insert("attempt".to_string(), "0".to_string());
        meta.insert("max_attempts".to_string(), "3".to_string());
        let record = make_record("run.failed", meta);
        let existing = make_run("started"); // pre-projection: was started, about to become failed

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Retry {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                attempt: 1,
                max_attempts: 3,
            }
        );
    }

    #[test]
    fn retry_delayed_replay_recreates_missing_retry_decision() {
        let mut meta = HashMap::new();
        meta.insert("attempt".to_string(), "0".to_string());
        meta.insert("max_attempts".to_string(), "3".to_string());
        let record = make_record("run.failed", meta);
        let existing = make_run("retry_delayed");

        assert_eq!(
            evaluate(&record, Some(&existing)),
            OrchestratorAction::Retry {
                project_id: "t1".to_string(),
                run_id: "r1".to_string(),
                attempt: 1,
                max_attempts: 3,
            }
        );
    }

    #[test]
    fn stale_failure_cannot_retry_an_already_advanced_attempt() {
        let mut meta = HashMap::new();
        meta.insert("attempt".to_string(), "0".to_string());
        meta.insert("max_attempts".to_string(), "3".to_string());
        let record = make_record("run.failed", meta);
        let mut run = Run::decode(make_run("failed").as_slice()).unwrap();
        run.attempt = 1;

        assert_eq!(
            evaluate(&record, Some(&run.encode_to_vec())),
            OrchestratorAction::NoOp
        );
    }

    #[test]
    fn failed_exhausted_produces_noop() {
        let mut meta = HashMap::new();
        meta.insert("attempt".to_string(), "2".to_string());
        meta.insert("max_attempts".to_string(), "3".to_string());
        let record = make_record("run.failed", meta);

        assert_eq!(evaluate(&record, None), OrchestratorAction::NoOp);
    }

    #[test]
    fn failed_no_retry_config_produces_noop() {
        let record = make_record("run.failed", HashMap::new());
        assert_eq!(evaluate(&record, None), OrchestratorAction::NoOp);
    }

    #[test]
    fn non_lifecycle_produces_noop() {
        assert_eq!(
            evaluate(&make_record("lm.completed", HashMap::new()), None),
            OrchestratorAction::NoOp
        );
        assert_eq!(
            evaluate(&make_record("run.started", HashMap::new()), None),
            OrchestratorAction::NoOp
        );
        assert_eq!(
            evaluate(&make_record("run.completed", HashMap::new()), None),
            OrchestratorAction::NoOp
        );
    }

    #[test]
    fn resume_dispatch_preserves_explicit_continuation_and_answer() {
        let run = Run {
            metadata: HashMap::from([
                (
                    "continuation_b64".to_string(),
                    "old-continuation".to_string(),
                ),
                ("user_response".to_string(), "old answer".to_string()),
            ]),
            ..Default::default()
        };
        let record = make_record(
            "run.resumed",
            HashMap::from([
                (
                    "continuation_b64".to_string(),
                    "new-continuation".to_string(),
                ),
                ("user_response".to_string(), "new answer".to_string()),
            ]),
        );
        let hydrated = hydrate_dispatch_source_record(&record, Some(&run.encode_to_vec()));
        let request = build_dispatch_request(&hydrated);

        assert_eq!(request.metadata["continuation_b64"], "new-continuation");
        assert_eq!(request.metadata["user_response"], "new answer");
    }

    #[test]
    fn build_dispatch_request_maps_fields() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("component_type".to_string(), "function".to_string());
        meta.insert("service_name".to_string(), "my_svc".to_string());
        meta.insert("session_id".to_string(), "sess-1".to_string());
        meta.insert("is_streaming".to_string(), "true".to_string());
        let record = make_record_with_data("run.queued", meta, Bytes::from(r#"{"input":"hello"}"#));

        let req = build_dispatch_request(&record);
        assert_eq!(req.invocation_id, "r1");
        assert_eq!(req.component_name, "my_func");
        assert_eq!(req.service_name, "my_svc");
        assert_eq!(req.session_id, "sess-1");
        assert!(req.is_streaming);
        assert_eq!(req.attempt, 0);
        assert_eq!(req.input_data, br#"{"input":"hello"}"#);
        assert_eq!(req.metadata.get("project_id").unwrap(), "t1");
        assert_eq!(req.metadata.get("attempt").map(|s| s.as_str()), Some("0"));
    }

    #[test]
    fn build_dispatch_request_maps_stream_mode_full_to_streaming() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("component_type".to_string(), "function".to_string());
        meta.insert("stream_mode".to_string(), "full".to_string());
        let record = make_record_with_data("run.queued", meta, Bytes::from(r#"{"input":"hello"}"#));

        let req = build_dispatch_request(&record);

        assert!(req.is_streaming);
    }

    #[test]
    fn build_dispatch_request_preserves_retry_attempt_metadata() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("attempt".to_string(), "1".to_string());
        meta.insert("max_attempts".to_string(), "2".to_string());
        let record = make_record("run.queued", meta);

        let req = build_dispatch_request(&record);
        assert_eq!(req.attempt, 1);
        assert_eq!(req.metadata.get("attempt").map(|s| s.as_str()), Some("1"));
        assert_eq!(
            req.metadata.get("max_attempts").map(|s| s.as_str()),
            Some("2")
        );
    }

    #[test]
    fn build_dispatch_request_advances_expired_fence_attempt() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("attempt".to_string(), "0".to_string());
        meta.insert("lease_id".to_string(), "stale-lease".to_string());
        meta.insert(
            engine_core::lease_fence::META_FENCE_ATTEMPT.to_string(),
            "0".to_string(),
        );
        meta.insert(
            engine_core::lease_fence::META_FENCE_KIND.to_string(),
            "1".to_string(),
        );
        meta.insert(
            engine_core::lease_fence::META_FENCE_LEASE_ID.to_string(),
            "expiry-fingerprint".to_string(),
        );
        let record = make_record("run.lease_expired", meta);

        let request = build_dispatch_request(&record);
        assert_eq!(request.attempt, 1);
        assert_eq!(
            request.metadata.get("attempt").map(String::as_str),
            Some("1")
        );
        assert!(!request.metadata.contains_key("lease_id"));
        assert!(!request
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_ATTEMPT));
        assert!(!request
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_KIND));
    }

    #[test]
    fn build_dispatch_request_preserves_workerless_checkpoint_metadata() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "workerless".to_string());
        meta.insert(
            "workerless_checkpoint".to_string(),
            serde_json::json!({"steps": {"fetch": {"ok": true}}}).to_string(),
        );
        let record = make_record("run.resumed", meta);

        let req = build_dispatch_request(&record);
        assert_eq!(
            req.metadata
                .get("workerless_checkpoint")
                .map(String::as_str),
            Some(r#"{"steps":{"fetch":{"ok":true}}}"#)
        );
    }

    #[test]
    fn build_dispatch_request_preserves_workerless_target_metadata() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "workerless".to_string());
        meta.insert("dispatch_mode".to_string(), "http".to_string());
        meta.insert(
            "workerless_endpoint".to_string(),
            "https://pinned.example".to_string(),
        );
        meta.insert(
            "workerless_protocol_version".to_string(),
            "workerless.v1".to_string(),
        );
        meta.insert(
            "workerless_manifest_hash".to_string(),
            "manifest-old".to_string(),
        );
        meta.insert(
            "workerless_signing_secret".to_string(),
            "secret-old".to_string(),
        );
        meta.insert(
            "workerless_max_payload_bytes".to_string(),
            "1048576".to_string(),
        );
        let record = make_record("run.resumed", meta);

        let req = build_dispatch_request(&record);

        assert_eq!(
            req.metadata.get("dispatch_mode").map(String::as_str),
            Some("http")
        );
        assert_eq!(
            req.metadata.get("workerless_endpoint").map(String::as_str),
            Some("https://pinned.example")
        );
        assert!(!req.metadata.contains_key("workerless_signing_secret"));
        assert_eq!(
            req.metadata
                .get("workerless_manifest_hash")
                .map(String::as_str),
            Some("manifest-old")
        );
    }

    // ---- Retry-policy default resolution ----

    #[test]
    fn default_retry_policy_interactive_is_terminal() {
        // INTERACTIVE is the sync path — failures must surface immediately.
        let p = default_retry_policy_for_priority(engine_proto::Priority::Interactive);
        assert_eq!(p.kind, engine_proto::retry_policy::Kind::Terminal as i32);
        assert_eq!(p.max_attempts, 0);
        assert_eq!(p.initial_backoff_ms, 0);
        assert_eq!(p.max_backoff_ms, 0);
    }

    #[test]
    fn default_retry_policy_normal_is_bounded() {
        // NORMAL (async /submit) gets the standard bounded default.
        let p = default_retry_policy_for_priority(engine_proto::Priority::Normal);
        assert_eq!(p.kind, engine_proto::retry_policy::Kind::Bounded as i32);
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.initial_backoff_ms, 1_000);
        assert_eq!(p.max_backoff_ms, 60_000);
    }

    #[test]
    fn default_retry_policy_batch_is_bounded() {
        // BATCH (async /batch) shares the NORMAL default: retry policy is
        // decoupled from the priority class for
        // scheduling; both async classes absorb transient failures.
        let p = default_retry_policy_for_priority(engine_proto::Priority::Batch);
        assert_eq!(p.kind, engine_proto::retry_policy::Kind::Bounded as i32);
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.initial_backoff_ms, 1_000);
        assert_eq!(p.max_backoff_ms, 60_000);
    }

    #[test]
    fn default_retry_policy_unspecified_matches_normal() {
        // UNSPECIFIED is canonicalised to NORMAL by the projection, so
        // we give the same default here to keep both canonicalisation
        // points in sync.
        let p = default_retry_policy_for_priority(engine_proto::Priority::Unspecified);
        assert_eq!(p.kind, engine_proto::retry_policy::Kind::Bounded as i32);
        assert_eq!(p.max_attempts, 3);
    }

    #[test]
    fn build_dispatch_request_missing_priority_falls_back_to_bounded_policy() {
        // Legacy record with no priority tag should get the NORMAL
        // default via the UNSPECIFIED canonicalisation path.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let record = make_record("run.queued", meta);

        let req = build_dispatch_request(&record);
        assert_eq!(req.priority(), engine_proto::Priority::Unspecified);
        let p = req.retry_policy.expect("retry_policy stamped");
        assert_eq!(p.kind, engine_proto::retry_policy::Kind::Bounded as i32);
        assert_eq!(p.max_attempts, 3);
        assert_eq!(
            req.metadata
                .get(RETRY_INITIAL_BACKOFF_MS_META)
                .map(String::as_str),
            Some("1000")
        );
        assert_eq!(
            req.metadata
                .get(RETRY_MAX_BACKOFF_MS_META)
                .map(String::as_str),
            Some("60000")
        );
    }

    #[test]
    fn build_dispatch_request_unparseable_priority_falls_back_to_bounded_policy() {
        // Defensive: a metadata value that isn't a number shouldn't
        // panic — fall back to the same NORMAL default.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("priority".to_string(), "not-a-number".to_string());
        let record = make_record("run.queued", meta);

        let req = build_dispatch_request(&record);
        assert_eq!(req.priority(), engine_proto::Priority::Unspecified);
        let p = req.retry_policy.expect("retry_policy stamped");
        assert_eq!(p.kind, engine_proto::retry_policy::Kind::Bounded as i32);
    }

    #[test]
    fn build_dispatch_request_stamps_interactive_terminal_policy() {
        // End-to-end check: a record tagged INTERACTIVE yields a
        // TERMINAL retry policy on the built request.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert(
            "priority".to_string(),
            (engine_proto::Priority::Interactive as i32).to_string(),
        );
        let record = make_record("run.queued", meta);

        let req = build_dispatch_request(&record);
        assert_eq!(req.priority(), engine_proto::Priority::Interactive);
        let rp = req.retry_policy.expect("retry_policy stamped");
        assert_eq!(rp.kind, engine_proto::retry_policy::Kind::Terminal as i32);
    }

    #[test]
    fn build_dispatch_request_stamps_batch_bounded_policy() {
        // End-to-end check: a BATCH record gets BOUNDED defaults.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert(
            "priority".to_string(),
            (engine_proto::Priority::Batch as i32).to_string(),
        );
        let record = make_record("run.queued", meta);

        let req = build_dispatch_request(&record);
        assert_eq!(req.priority(), engine_proto::Priority::Batch);
        let rp = req.retry_policy.expect("retry_policy stamped");
        assert_eq!(rp.kind, engine_proto::retry_policy::Kind::Bounded as i32);
        assert_eq!(rp.max_attempts, 3);
        assert_eq!(rp.initial_backoff_ms, 1_000);
        assert_eq!(rp.max_backoff_ms, 60_000);
    }

    #[test]
    fn build_assigned_record_correct() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        let mut record = make_record("run.queued", meta);
        record.correlation_id = Some("run-span".to_string());

        let assigned = build_assigned_record(&record, None);
        assert_eq!(assigned.event_type, "run.assigned");
        assert_eq!(assigned.run_id, "r1");
        assert_eq!(assigned.project_id, "t1");
        assert_eq!(assigned.correlation_id.as_deref(), Some("run-span"));
        assert!(assigned.timestamp_ns > 0);
        assert_eq!(assigned.metadata.get("component_name").unwrap(), "my_func");
        // Legacy path: no lease metadata stamped.
        assert!(!assigned.metadata.contains_key("lease_id"));
    }

    #[test]
    fn build_assigned_record_stamps_lease_metadata_when_provided() {
        // When the dispatch consumer mints a lease, the processor
        // passes those fields through so `apply_assigned` populates
        // `Run.lease_*` and the diff-driven index emitter writes
        // `CF_LEASES_BY_EXPIRY` / `CF_LEASES_BY_WORKER`.
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert("attempt".to_string(), "3".to_string());
        let record = make_record("run.queued", meta);

        let lease = LeaseFields {
            lease_id: "0190abcd-1234-7890-abcd-ef0123456789".to_string(),
            lease_worker_id: "worker-a".to_string(),
            lease_session_hash: engine_core::lease_fence::worker_session_hash("worker-a"),
            lease_expires_at_ms: 1_700_000_120_000,
            lease_timeout_ms: DEFAULT_PUSH_LEASE_TIMEOUT_MS,
        };

        let assigned = build_assigned_record(&record, Some(&lease));
        assert_eq!(assigned.event_type, "run.assigned");
        assert_eq!(
            assigned.metadata.get("lease_id").map(String::as_str),
            Some("0190abcd-1234-7890-abcd-ef0123456789"),
        );
        assert_eq!(
            assigned.metadata.get("dispatch_mode").map(String::as_str),
            Some("push"),
        );
        assert_eq!(
            assigned.metadata.get("lease_worker_id").map(String::as_str),
            Some("worker-a"),
        );
        assert_eq!(
            assigned
                .metadata
                .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
                .map(String::as_str),
            Some(lease.lease_session_hash.as_str()),
        );
        assert_eq!(
            assigned
                .metadata
                .get("lease_expires_at_ms")
                .map(String::as_str),
            Some("1700000120000"),
        );
        assert_eq!(
            assigned
                .metadata
                .get("lease_timeout_ms")
                .map(String::as_str),
            Some("120000"),
        );
        assert_eq!(
            assigned
                .metadata
                .get(engine_core::lease_fence::RUN_META_LEASE_ATTEMPT)
                .map(String::as_str),
            Some("3"),
        );
        // Preserves original metadata alongside the lease fields.
        assert_eq!(
            assigned.metadata.get("component_name").map(String::as_str),
            Some("my_func"),
        );
        engine_core::journal_contract::validate_append_records(std::slice::from_ref(&assigned))
            .expect("lease-backed run.assigned must satisfy the journal contract");
    }

    #[test]
    fn workerless_assignment_preserves_http_route_for_lease_expiry() {
        let mut metadata = HashMap::new();
        metadata.insert("dispatch_mode".to_string(), "http".to_string());
        metadata.insert(
            "workerless_endpoint".to_string(),
            "https://workerless.example".to_string(),
        );
        let queued = make_record("run.queued", metadata);
        let lease = LeaseFields {
            lease_id: "workerless-lease".to_string(),
            lease_worker_id: "workerless-http".to_string(),
            lease_session_hash: engine_core::lease_fence::worker_session_hash("workerless-http"),
            lease_expires_at_ms: 1_700_000_120_000,
            lease_timeout_ms: DEFAULT_PUSH_LEASE_TIMEOUT_MS,
        };

        let assigned = build_assigned_record(&queued, Some(&lease));

        assert_eq!(
            assigned.metadata.get("dispatch_mode").map(String::as_str),
            Some("http")
        );
    }

    #[test]
    fn build_assigned_record_uses_next_attempt_after_lease_expiry() {
        let mut meta = HashMap::new();
        meta.insert("component_name".to_string(), "my_func".to_string());
        meta.insert(
            engine_core::lease_fence::META_FENCE_ATTEMPT.to_string(),
            "1".to_string(),
        );
        meta.insert(
            engine_core::lease_fence::META_FENCE_KIND.to_string(),
            "1".to_string(),
        );
        meta.insert(
            engine_core::lease_fence::META_FENCE_LEASE_ID.to_string(),
            "expiry-fingerprint".to_string(),
        );
        let record = make_record("run.lease_expired", meta);
        let lease = LeaseFields {
            lease_id: "lease-next".to_string(),
            lease_worker_id: "worker-b".to_string(),
            lease_session_hash: engine_core::lease_fence::worker_session_hash("worker-b"),
            lease_expires_at_ms: 1_700_000_120_000,
            lease_timeout_ms: DEFAULT_PUSH_LEASE_TIMEOUT_MS,
        };

        let assigned = build_assigned_record(&record, Some(&lease));
        assert_eq!(
            assigned.metadata.get("attempt").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            assigned
                .metadata
                .get(engine_core::lease_fence::RUN_META_LEASE_ATTEMPT)
                .map(String::as_str),
            Some("2")
        );
        assert!(!assigned
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_KIND));
        assert!(engine_core::journal_contract::record_idempotency_key(&assigned).is_none());
    }

    #[test]
    fn lease_backed_push_enabled_by_default() {
        // Make sure clusters without the env var mint push leases by default.
        // The value is cached by `OnceLock`, so we can't flip it mid-test —
        // but a fresh process without the env set must read `true`.
        //
        // This is effectively a sanity check on the default branch of the
        // env parse. Because the flag is OnceLock-cached process-wide, we
        // can only rely on it being unset in the test environment.
        if std::env::var("AGNT5_LEASE_BACKED_PUSH").is_err() {
            assert!(lease_backed_push_enabled());
        }
    }

    #[test]
    fn lease_backed_push_env_is_default_on_with_explicit_opt_out() {
        assert!(parse_lease_backed_push_env(""));
        assert!(parse_lease_backed_push_env("1"));
        assert!(parse_lease_backed_push_env("true"));
        assert!(parse_lease_backed_push_env("yes"));
        assert!(!parse_lease_backed_push_env("0"));
        assert!(!parse_lease_backed_push_env(" false "));
        assert!(!parse_lease_backed_push_env("NO"));
    }

    #[test]
    fn build_failed_record_correct() {
        let record = make_record("run.queued", HashMap::new());
        let failed = build_failed_record(&record, "no workers available");
        assert_eq!(failed.event_type, "run.failed");
        assert_eq!(failed.run_id, "r1");

        let payload: serde_json::Value = serde_json::from_slice(&failed.data).unwrap();
        assert_eq!(payload["error_message"], "no workers available");
        assert_eq!(payload["error_code"], "DISPATCH_FAILED");
    }

    #[test]
    fn build_failed_record_starts_a_clean_attempt_after_lease_expiry() {
        let mut meta = HashMap::new();
        meta.insert("attempt".to_string(), "0".to_string());
        meta.insert(
            engine_core::lease_fence::META_FENCE_ATTEMPT.to_string(),
            "0".to_string(),
        );
        meta.insert(
            engine_core::lease_fence::META_FENCE_KIND.to_string(),
            "1".to_string(),
        );
        let record = make_record("run.lease_expired", meta);

        let failed = build_failed_record(&record, "no workers available");

        assert_eq!(
            failed.metadata.get("attempt").map(String::as_str),
            Some("1")
        );
        assert!(!failed
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_ATTEMPT));
        assert!(!failed
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_KIND));
        assert!(engine_core::journal_contract::record_idempotency_key(&failed).is_none());
    }

    #[test]
    fn build_failed_record_marks_workerless_payload_limit_terminal() {
        let mut meta = HashMap::new();
        meta.insert("attempt".to_string(), "1".to_string());
        meta.insert("max_attempts".to_string(), "3".to_string());
        let record = make_record("run.queued", meta);
        let failed = build_failed_record(
            &record,
            "workerless input payload exceeds 1024 bytes (got 2048 bytes); configure payload references or reduce payload size",
        );

        let payload: serde_json::Value = serde_json::from_slice(&failed.data).unwrap();
        assert_eq!(payload["error_code"], WORKERLESS_PAYLOAD_TOO_LARGE_CODE);
        assert_eq!(
            failed.metadata.get("error_code").map(String::as_str),
            Some(WORKERLESS_PAYLOAD_TOO_LARGE_CODE)
        );
        assert_eq!(
            failed
                .metadata
                .get("workerless_retryable")
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            failed.metadata.get("attempt").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            failed.metadata.get("max_attempts").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            evaluate_failed(&failed, Some(&make_run("assigned"))),
            OrchestratorAction::NoOp
        );
    }

    #[test]
    fn dispatch_and_retry_records_drop_lease_loss_metadata() {
        let lease_loss = [
            (
                run_state_machine::LEASE_WORKER_TERMINATIONS_META,
                r#"[[1,"OOMKilled"]]"#,
            ),
            (
                run_state_machine::WORKER_TERMINATION_REASON_META,
                "OOMKilled",
            ),
            (
                run_state_machine::LEASE_RECOVERY_REASON_META,
                "worker_terminated",
            ),
        ];
        let mut metadata: HashMap<String, String> = lease_loss
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        metadata.insert("component_name".to_string(), "export_orders".to_string());
        sanitize_dispatch_generation_metadata(&mut metadata);
        assert_eq!(
            metadata.get("component_name").map(String::as_str),
            Some("export_orders")
        );

        let mut failed_meta = metadata.clone();
        failed_meta.extend(
            lease_loss
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        failed_meta.insert("attempt".to_string(), "1".to_string());
        failed_meta.insert("max_attempts".to_string(), "3".to_string());
        let retry = build_retry_record(&make_record("run.failed", failed_meta), None, 2, 3);
        for (key, _) in lease_loss {
            assert!(!metadata.contains_key(key), "dispatch kept {key}");
            assert!(!retry.metadata.contains_key(key), "retry record kept {key}");
        }
    }

    #[test]
    fn build_retry_record_restores_workerless_target_metadata_from_run() {
        let mut failed_meta = HashMap::new();
        failed_meta.insert("attempt".to_string(), "0".to_string());
        failed_meta.insert("max_attempts".to_string(), "3".to_string());
        failed_meta.insert("dispatch_mode".to_string(), "pull".to_string());
        failed_meta.insert(
            engine_core::lease_fence::META_FENCE_KIND.to_string(),
            "1".to_string(),
        );
        failed_meta.insert(
            engine_core::lease_fence::META_FENCE_WORKER_ID.to_string(),
            "stale-signature".to_string(),
        );
        for key in STALE_RETRY_ASSIGNMENT_METADATA_KEYS {
            failed_meta.insert((*key).to_string(), format!("stale-{key}"));
        }
        failed_meta.insert(
            "workerless_endpoint".to_string(),
            "https://replacement.example".to_string(),
        );
        let failed = make_record("run.failed", failed_meta);

        let mut run_meta = HashMap::new();
        run_meta.insert("dispatch_mode".to_string(), "http".to_string());
        run_meta.insert(
            "workerless_endpoint".to_string(),
            "https://pinned.example".to_string(),
        );
        run_meta.insert(
            "workerless_protocol_version".to_string(),
            "workerless.v1".to_string(),
        );
        run_meta.insert(
            "workerless_manifest_hash".to_string(),
            "manifest-old".to_string(),
        );
        run_meta.insert(
            "workerless_signing_secret".to_string(),
            "secret-old".to_string(),
        );
        run_meta.insert(
            "workerless_request_timeout_ms".to_string(),
            "30000".to_string(),
        );
        run_meta.insert(
            "workerless_yield_before_timeout_ms".to_string(),
            "2500".to_string(),
        );
        run_meta.insert(
            "workerless_max_payload_bytes".to_string(),
            "1048576".to_string(),
        );
        run_meta.insert(
            "workerless_payload_ref_threshold_bytes".to_string(),
            "65536".to_string(),
        );
        run_meta.insert(
            "workerless_invoke_headers".to_string(),
            r#"{"x":"y"}"#.to_string(),
        );
        run_meta.insert("workerless_concurrency_limit".to_string(), "2".to_string());
        run_meta.insert(
            "workerless_concurrency_scope".to_string(),
            "component".to_string(),
        );
        run_meta.insert(
            "workerless_concurrency_key".to_string(),
            "component-key".to_string(),
        );
        run_meta.insert("workerless_throttle_limit".to_string(), "10".to_string());
        run_meta.insert(
            "workerless_throttle_window_ms".to_string(),
            "1000".to_string(),
        );
        run_meta.insert(
            "workerless_throttle_key".to_string(),
            "throttle-key".to_string(),
        );
        run_meta.insert("workerless_rate_limit_limit".to_string(), "100".to_string());
        run_meta.insert(
            "workerless_rate_limit_window_ms".to_string(),
            "60000".to_string(),
        );
        run_meta.insert(
            "workerless_rate_limit_key".to_string(),
            "rate-limit-key".to_string(),
        );
        run_meta.insert(
            "workerless_debounce_window_ms".to_string(),
            "500".to_string(),
        );
        run_meta.insert(
            "workerless_debounce_key".to_string(),
            "debounce-key".to_string(),
        );
        run_meta.insert(
            "workerless_debounce_queued_at_ms".to_string(),
            "123456".to_string(),
        );
        run_meta.insert("workerless_batch_max_size".to_string(), "25".to_string());
        run_meta.insert("workerless_batch_window_ms".to_string(), "250".to_string());
        run_meta.insert("workerless_batch_key".to_string(), "batch-key".to_string());
        run_meta.insert(
            "workerless_idempotency_key".to_string(),
            "idempotency-key".to_string(),
        );
        run_meta.insert(
            "workerless_idempotency_ttl_ms".to_string(),
            "60000".to_string(),
        );
        run_meta.insert(
            "workerless_singleton_key".to_string(),
            "singleton-key".to_string(),
        );
        run_meta.insert("workerless_singleton_mode".to_string(), "queue".to_string());
        let existing_run = Run {
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            status: "started".to_string(),
            component_name: "workerless".to_string(),
            component_type: "workflow".to_string(),
            deployment_id: "deployment-old".to_string(),
            input_data: br#"{"input":"hello"}"#.to_vec(),
            metadata: run_meta,
            ..Default::default()
        }
        .encode_to_vec();

        let retry = build_retry_record(&failed, Some(&existing_run), 1, 3);

        assert_eq!(retry.event_type, "run.queued");
        assert_eq!(retry.data.as_ref(), br#"{"input":"hello"}"#);
        assert_eq!(retry.metadata.get("attempt").map(String::as_str), Some("1"));
        assert_eq!(
            retry.metadata.get("max_attempts").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            retry.metadata.get(RETRY_SOURCE_META).map(String::as_str),
            Some(RETRY_SOURCE_RUN_FAILED)
        );
        assert_eq!(
            retry
                .metadata
                .get(RETRY_READY_AT_MS_META)
                .map(String::as_str),
            Some("1000")
        );
        assert_eq!(
            retry.metadata.get(READY_AT_MS_META).map(String::as_str),
            Some("1000")
        );
        assert!(!retry
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_KIND));
        assert!(!retry
            .metadata
            .contains_key(engine_core::lease_fence::META_FENCE_WORKER_ID));
        for key in STALE_RETRY_ASSIGNMENT_METADATA_KEYS {
            assert!(
                !retry.metadata.contains_key(*key),
                "retry must not retain stale assignment metadata key {key}"
            );
        }
        let unleased_assignment = build_assigned_record(&retry, None);
        for key in STALE_RETRY_ASSIGNMENT_METADATA_KEYS {
            assert!(
                !unleased_assignment.metadata.contains_key(*key),
                "unleased assignment must not recreate stale metadata key {key}"
            );
        }
        assert_eq!(
            retry.metadata.get("dispatch_mode").map(String::as_str),
            Some("http")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_endpoint")
                .map(String::as_str),
            Some("https://pinned.example")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_protocol_version")
                .map(String::as_str),
            Some("workerless.v1")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_manifest_hash")
                .map(String::as_str),
            Some("manifest-old")
        );
        assert!(!retry.metadata.contains_key("workerless_signing_secret"));
        assert_eq!(
            retry
                .metadata
                .get("workerless_max_payload_bytes")
                .map(String::as_str),
            Some("1048576")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_payload_ref_threshold_bytes")
                .map(String::as_str),
            Some("65536")
        );
        assert!(!retry.metadata.contains_key("workerless_invoke_headers"));
        assert_eq!(
            retry
                .metadata
                .get("workerless_concurrency_limit")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_throttle_key")
                .map(String::as_str),
            Some("throttle-key")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_rate_limit_key")
                .map(String::as_str),
            Some("rate-limit-key")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_debounce_key")
                .map(String::as_str),
            Some("debounce-key")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_debounce_queued_at_ms")
                .map(String::as_str),
            Some("123456")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_batch_key")
                .map(String::as_str),
            Some("batch-key")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_idempotency_key")
                .map(String::as_str),
            Some("idempotency-key")
        );
        assert_eq!(
            retry
                .metadata
                .get("workerless_singleton_key")
                .map(String::as_str),
            Some("singleton-key")
        );
        assert_eq!(
            retry.metadata.get("deployment_id").map(String::as_str),
            Some("deployment-old")
        );
    }
}
