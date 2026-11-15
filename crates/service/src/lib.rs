pub mod in_process_engine;
pub mod telemetry;

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use prost::Message;
use serde_json::json;
use tokio_stream::StreamExt;
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::debug;

use engine_cache::RecordCache;
use engine_core::activation::{
    self, ActivationDecision, ActivationState, BeginActivationCommand, CancelActivationCommand,
    CompleteActivationCommand, FailActivationCommand,
};
use engine_core::activation_authority::ActivationLeaseGuard;
use engine_core::error::{ActivationError, EngineError};
use engine_core::lease_fence::{
    strip_reserved_fence_metadata, worker_session_signing_key, LeaseFence, LeaseFenceKind,
    RUN_META_LEASE_ATTEMPT,
};
use engine_core::partition_offset::decode_offset_for_partition_count;
use engine_core::routing::partition_for_run;
use engine_core::storage_contract::{
    EntityStateWriteStore, LeaseWorkStore, PendingWorkStore, ProjectionCheckpointStore,
    QueueQueryStore, RunQueryStore, SessionQueryStore, TimerWorkStore, WorkflowQueryStore,
};
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_core::workload_identity::ExternalWorkerIdentity;
use engine_proto::engine_service_server::EngineService;
use runtime_common::{backend::EngineBackend, ClaimRegistry};

use engine_core::pull_dispatch::{
    PollFailureKind, PollOutcome, PollRejection, PullDispatch, VerifiedPoll,
};

use engine_proto::{
    ActivationConflictReceipt, ActivationErrorCode, ActivationErrorDetail,
    ActivationUnknownOutcomeReceipt, ActivationWaitReceipt, AppendBatchRequest,
    AppendBatchResponse, AppendObservedRequest, AppendObservedResponse, AppendRequest,
    AppendResponse, BeginActivationOutcome, BeginActivationRequest, BeginActivationResponse,
    CheckpointRequest, CheckpointResponse, CompleteActivationRequest, CompleteActivationResponse,
    CompleteJobRequest, CompleteJobResponse, EngineListApprovalsRequest,
    EngineListApprovalsResponse, EventStreamAck, EventStreamMessage, FailActivationRequest,
    FailActivationResponse, FindByStepKeyRequest, FindByStepKeyResponse, GetApprovalRequest,
    GetApprovalResponse, GetBatchRequest, GetBatchResponse, GetEntityStateRequest,
    GetEntityStateResponse, GetJobRequest, GetJobResponse, GetMessageRequest, GetMessageResponse,
    GetRunRequest, GetRunResponse, GetRunStatsRequest, GetRunStatsResponse, GetSessionRequest,
    GetSessionResponse, GetSignalRequest, GetSignalResponse, GetStatusRequest, GetStatusResponse,
    GetTimerRequest, GetTimerResponse, LeaseRenewalOutcome, ListDueTimersRequest,
    ListDueTimersResponse, ListEventsRequest, ListEventsResponse, ListJobsByBatchRequest,
    ListJobsByBatchResponse, ListJobsByRunRequest, ListJobsByRunResponse,
    ListMessagesByCorrelationRequest, ListMessagesByCorrelationResponse, ListRoutingWorkersRequest,
    ListRoutingWorkersResponse, ListRunsRequest, ListRunsResponse, ListSessionsRequest,
    ListSessionsResponse, ListSignalsRequest, ListSignalsResponse, ListTimersRequest,
    ListTimersResponse, LookupSignalRequest, LookupSignalResponse, ObservedRecordOutcome,
    PollJobRequest, PollJobResponse, PollJobsRequest, PollJobsResponse, PullReadyDelegation,
    PutEntityStateRequest, PutEntityStateResponse, ReadByRunIdRequest, ReadByRunIdResponse,
    RegisterWorkerSessionRequest, RegisterWorkerSessionResponse, RenewJobLeaseRequest,
    RenewJobLeaseResponse, ReportWorkerCapacityRequest, ReportWorkerCapacityResponse, Run,
    SuspendActivationRequest, SuspendActivationResponse, TailRequest, TailResponse, WorkerMode,
    WorkerSlotPolicy,
};

struct ProfileQueryLake;

#[derive(Debug, Clone, Default)]
struct RunListFilter {
    workspace_id: String,
    project_id: String,
    deployment_id: Option<String>,
    deployment_ids: Vec<String>,
    statuses: Vec<String>,
    component_name: Option<String>,
    component_names: Vec<String>,
    component_type: Option<String>,
    min_duration_ms: Option<i64>,
    max_duration_ms: Option<i64>,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    limit: i32,
    offset: i32,
    cursor: Option<String>,
    session_id: Option<String>,
    parent_run_id: Option<String>,
    trigger_type: Option<String>,
    mcp_server: Option<String>,
}

#[derive(Debug, Default)]
struct RunListResult {
    items: Vec<Run>,
    limit: i32,
    offset: i32,
    has_more: bool,
    next_cursor: String,
    next_offset: Option<i32>,
}

/// Metadata marking a record as a live update: the engine passes it to stream
/// subscribers and never writes it to the journal.
pub const TRANSIENT_EVENT_METADATA_KEY: &str = "_agnt5_transient";

fn external_worker_identity<T>(request: &Request<T>) -> Option<ExternalWorkerIdentity> {
    request
        .extensions()
        .get::<ExternalWorkerIdentity>()
        .cloned()
}

fn authorize_external_project(
    identity: Option<&ExternalWorkerIdentity>,
    project_id: &str,
) -> Result<(), Status> {
    ensure_external_identity_not_expired(identity)?;
    if identity.is_some_and(|identity| identity.project_id != project_id) {
        if let Some(identity) = identity {
            tracing::warn!(
                token_project_id = %identity.project_id,
                request_project_id = %project_id,
                "external workload project authority mismatch"
            );
        }
        return Err(Status::permission_denied(
            "workload token project_id does not match request",
        ));
    }
    Ok(())
}

fn authorize_external_worker(
    identity: Option<&ExternalWorkerIdentity>,
    worker_id: &str,
) -> Result<(), Status> {
    ensure_external_identity_not_expired(identity)?;
    if identity
        .and_then(|identity| identity.worker_id.as_deref())
        .is_some_and(|expected| expected != worker_id)
    {
        return Err(Status::permission_denied(
            "worker certificate identity does not match request",
        ));
    }
    Ok(())
}

fn authorize_external_deployment(
    identity: Option<&ExternalWorkerIdentity>,
    project_id: &str,
    deployment_id: &str,
) -> Result<(), Status> {
    authorize_external_project(identity, project_id)?;
    if identity.is_some_and(|identity| identity.deployment_id != deployment_id) {
        return Err(Status::permission_denied(
            "workload token deployment_id does not match request",
        ));
    }
    Ok(())
}

fn ensure_external_identity_not_expired(
    identity: Option<&ExternalWorkerIdentity>,
) -> Result<(), Status> {
    if identity.is_some_and(|identity| {
        identity.expires_at_unix <= now_ms() / 1_000
            || identity
                .authority
                .as_ref()
                .is_some_and(|authority| !authority.is_current())
    }) {
        return Err(Status::unauthenticated(
            "worker authorization has expired or is no longer current",
        ));
    }
    Ok(())
}

fn authorize_external_lease_renewal_mode(
    identity: Option<&ExternalWorkerIdentity>,
    mode: LeaseRenewalMode,
) -> Result<(), Status> {
    if identity.is_some() && mode == LeaseRenewalMode::Push {
        return Err(Status::permission_denied(
            "external workers must renew leases through an authenticated pull session",
        ));
    }
    Ok(())
}

fn dispatch_mode_value_is_pull(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "pull" | "worker_mode_pull" | "2"
    )
}

/// Classify an active lease without treating its required session hash as a
/// pull-only marker. New push and pull assignments both carry a complete
/// authority tuple; `dispatch_mode` is the discriminator. The hash-only
/// fallback preserves fail-closed behavior for legacy pull assignments that
/// predate the projected mode field.
fn run_uses_pull_lease(run: &Run) -> bool {
    match run.metadata.get("dispatch_mode") {
        Some(mode) => dispatch_mode_value_is_pull(mode),
        None => run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
            .is_some_and(|session_hash| !session_hash.is_empty()),
    }
}

mod poll_jobs;
mod pull_workers;
mod queue_metrics;
pub mod run_lease_authority;
use self::poll_jobs::LeaseRenewalMode;
use self::poll_jobs::ServicePullDispatch;
use self::pull_workers::{
    PollAttemptRegistry, PollJobWaiterRegistry, PullWakeSource, QueueMetricRefreshRegistry,
    WorkerSession, WorkerSessionRegistry,
};

const PER_RUN_TAIL_SEGMENT_POLL_INTERVAL: Duration = Duration::from_millis(100);
// Each retry means another step settled, and a run has few steps open at once.
const SUSPENSION_APPEND_ATTEMPTS: usize = 16;
const ACTIVATION_VISIBILITY_RETRY_ATTEMPTS: usize = 40;
const ACTIVATION_VISIBILITY_RETRY_INTERVAL: Duration = Duration::from_millis(25);

/// Consumer-owned storage bundle for the runtime service.
///
/// Managed startup supplies one RocksDB adapter implementing these ports. A
/// different runtime profile can supply the same bundle without implementing
/// the legacy `MaterializedStore` mega-trait.
pub trait EngineServiceStore:
    RunQueryStore
    + WorkflowQueryStore
    + SessionQueryStore
    + TimerWorkStore
    + PendingWorkStore
    + LeaseWorkStore
    + QueueQueryStore
    + ProjectionCheckpointStore
    + EntityStateWriteStore
{
}

impl<T> EngineServiceStore for T where
    T: RunQueryStore
        + WorkflowQueryStore
        + SessionQueryStore
        + TimerWorkStore
        + PendingWorkStore
        + LeaseWorkStore
        + QueueQueryStore
        + ProjectionCheckpointStore
        + EntityStateWriteStore
        + ?Sized
{
}

enum AppendBatchResponseSlot {
    Existing(Offset),
    AppendedIndex(usize),
}

/// Validate that a hot-tier record already carries the public offset for the
/// run's partition. Processor projection and cache writes both consume
/// `PartitionRouter` streams, so interpreting a low offset as "local" here
/// would silently turn a corrupt partition-0 offset into another partition.
fn validate_public_run_offset(
    offset: Offset,
    expected_partition: usize,
    partition_count: usize,
) -> engine_core::error::EngineResult<Offset> {
    let (encoded_partition, _) = decode_offset_for_partition_count(offset, partition_count)?;
    if encoded_partition != expected_partition {
        return Err(EngineError::Internal(format!(
            "run history offset {} encodes partition {}, expected {}",
            offset.0, encoded_partition, expected_partition
        )));
    }
    Ok(offset)
}

fn record_append_rpc_metrics(
    metrics: &Option<crate::telemetry::EngineMetrics>,
    method: &'static str,
    result: &'static str,
    start: Instant,
    appended_count: Option<u64>,
) {
    if let Some(m) = metrics {
        let labels = [
            opentelemetry::KeyValue::new("method", method),
            opentelemetry::KeyValue::new("result", result),
        ];
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        m.append_duration.record(elapsed, &labels);
        if let Some(count) = appended_count {
            m.append_records.add(count, &labels);
        }
    }
}

fn record_pull_queue_commit(
    metrics: &Option<crate::telemetry::EngineMetrics>,
    method: &'static str,
    result: &'static str,
    started_at: Instant,
    contains_queued_run: bool,
) {
    if !contains_queued_run {
        return;
    }
    if let Some(metrics) = metrics {
        metrics.pull_queue_commit_duration.record(
            started_at.elapsed().as_secs_f64() * 1_000.0,
            &[
                opentelemetry::KeyValue::new("method", method),
                opentelemetry::KeyValue::new("result", result),
            ],
        );
    }
}

fn checkpoint_type_label(checkpoint_type: engine_proto::CheckpointType) -> &'static str {
    match checkpoint_type {
        engine_proto::CheckpointType::StepStarted => "step_started",
        engine_proto::CheckpointType::StepCompleted => "step_completed",
        engine_proto::CheckpointType::StepFailed => "step_failed",
        engine_proto::CheckpointType::StateSnapshot => "state_snapshot",
        engine_proto::CheckpointType::Unspecified => "unspecified",
    }
}

fn record_checkpoint_phase(
    metrics: &Option<crate::telemetry::EngineMetrics>,
    phase: &'static str,
    checkpoint_type: engine_proto::CheckpointType,
    result: &'static str,
    started_at: Instant,
) {
    if let Some(metrics) = metrics {
        metrics.checkpoint_phase_duration.record(
            started_at.elapsed().as_secs_f64() * 1000.0,
            &[
                opentelemetry::KeyValue::new("phase", phase),
                opentelemetry::KeyValue::new(
                    "checkpoint_type",
                    checkpoint_type_label(checkpoint_type),
                ),
                opentelemetry::KeyValue::new("result", result),
            ],
        );
    }
}

fn record_activation_phase(
    metrics: &Option<crate::telemetry::EngineMetrics>,
    operation: &'static str,
    phase: &'static str,
    result: &'static str,
    started_at: Instant,
) {
    if let Some(metrics) = metrics {
        metrics.activation_phase_duration.record(
            started_at.elapsed().as_secs_f64() * 1000.0,
            &[
                opentelemetry::KeyValue::new("operation", operation),
                opentelemetry::KeyValue::new("phase", phase),
                opentelemetry::KeyValue::new("result", result),
            ],
        );
    }
}

fn now_unix_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn assign_missing_batch_timestamps(records: &mut [Record], now_ns: i64) {
    for (index, record) in records.iter_mut().enumerate() {
        if record.timestamp_ns == 0 {
            record.timestamp_ns = now_ns.saturating_add(index as i64);
        }
    }
}

fn is_terminal_run_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

fn failure_event_is_terminal(record: &Record) -> bool {
    let attempt = record
        .metadata
        .get("attempt")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let max_attempts = record
        .metadata
        .get("max_attempts")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    attempt.saturating_add(1) >= max_attempts
}

fn terminal_status_for_record(record: &Record) -> Option<&'static str> {
    match record.event_type.as_str() {
        "run.completed" => Some("completed"),
        "run.cancelled" => Some("cancelled"),
        "run.failed" if failure_event_is_terminal(record) => Some("failed"),
        _ => None,
    }
}

fn should_end_run_tail(record: &Record) -> bool {
    terminal_status_for_record(record).is_some()
        || (matches!(record.event_type.as_str(), "run.paused" | "workflow.paused")
            && !record.metadata.contains_key("workerless_suspend_reason"))
}

fn drain_pending_transient_records(
    receiver: &mut tokio::sync::broadcast::Receiver<Arc<Record>>,
    run_id: &str,
) -> Vec<Arc<Record>> {
    let mut records = Vec::new();
    loop {
        match receiver.try_recv() {
            Ok(record) => records.push(record),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                tracing::debug!(run_id, skipped, "Transient run tail lagged before terminal");
            }
            Err(
                tokio::sync::broadcast::error::TryRecvError::Empty
                | tokio::sync::broadcast::error::TryRecvError::Closed,
            ) => break,
        }
    }
    records
}

fn should_drop_after_terminal(event_type: &str) -> bool {
    match event_type {
        // `run.cancelled` is authored by the cancel endpoint, which performs
        // its own conflict handling. Do not silently turn a user cancel into
        // a no-op at this lower append boundary.
        "run.cancelled" | "run.queued" | "run.archived" => false,
        "run.assigned" | "run.started" | "run.completed" | "run.failed" => true,
        _ => {
            event_type.ends_with(".started")
                || event_type.ends_with(".completed")
                || event_type.ends_with(".failed")
                || event_type.ends_with(".resumed")
                // `ctx.progress` reports are coalesced and sent off the
                // run's critical path, so the last one can land after the
                // run finished; it would only say something stale. This is
                // a best-effort preflight against the cache/projection, not
                // the serialized append: a report racing the terminal (or
                // after it in one AppendBatch) can still be committed. The
                // readers are authoritative: the MCP edge stops at the first
                // terminal record and the run view ignores progress after it.
                || event_type.starts_with("progress.")
        }
    }
}

/// Refuse an observed-run record on the worker append paths. Observed runs
/// have no lease, so the marker would otherwise let a caller skip the lease
/// checks; their records go through `AppendObserved` only.
fn reject_observed_record(record: &Record) -> Result<(), Status> {
    if engine_core::journal_contract::is_observed_record(record) {
        return Err(Status::invalid_argument(format!(
            "records with {}={} are written through AppendObserved",
            engine_core::journal_contract::META_ORIGIN,
            engine_core::journal_contract::ORIGIN_OTLP,
        )));
    }
    Ok(())
}

/// Check an `AppendObserved` batch before anything else: every record is
/// marked as observed, all belong to one run, and each event type is one an
/// observed run may hold. The gateway builds these records, so a violation is
/// a bug there and the whole batch is refused.
fn validate_observed_batch(records: &[Record]) -> Result<(), Status> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    for record in records {
        if !engine_core::journal_contract::is_observed_record(record) {
            return Err(Status::invalid_argument(format!(
                "AppendObserved record {} lacks {}={}",
                record.event_type,
                engine_core::journal_contract::META_ORIGIN,
                engine_core::journal_contract::ORIGIN_OTLP,
            )));
        }
        if record.project_id != first.project_id || record.run_id != first.run_id {
            return Err(Status::invalid_argument(
                "AppendObserved records must all belong to one run",
            ));
        }
        if !engine_core::journal_contract::is_observed_event_type(&record.event_type) {
            return Err(Status::invalid_argument(format!(
                "event type {} is not allowed on an observed run",
                record.event_type
            )));
        }
    }
    Ok(())
}

fn negotiate_worker_session_protocols(
    request: &RegisterWorkerSessionRequest,
    _profile: runtime_common::durability::DurabilityProfile,
) -> Result<(Vec<String>, Vec<String>), Status> {
    let runtime_supported = runtime_common::durability::supported_protocol_capabilities();
    // Production durability validates runtime-owned infrastructure. Worker
    // protocol upgrades remain additive: legacy sessions are accepted without
    // negotiating durable activation/suspension, while workers may still opt
    // into strict negotiation through their own required capabilities.
    let runtime_required = Vec::new();
    if let Some(required) = request
        .required_protocol_capabilities
        .iter()
        .find(|required| !runtime_supported.contains(required))
    {
        return Err(Status::failed_precondition(format!(
            "required worker protocol capability is unavailable: {required}"
        )));
    }
    if let Some(required) = request
        .required_protocol_capabilities
        .iter()
        .find(|required| !request.supported_protocol_capabilities.contains(required))
    {
        return Err(Status::invalid_argument(format!(
            "required protocol capability was not advertised as supported: {required}"
        )));
    }
    Ok((runtime_supported, runtime_required))
}

/// Caller-supplied metadata keys a pull completion may never carry: the
/// runtime stamps every lease and fence field itself.
const COMPLETION_RESERVED_METADATA_KEYS: &[&str] = &[
    "worker_id",
    "lease_id",
    "worker_session_id",
    "lease_worker_id",
    "lease_expires_at_ms",
    "lease_attempt",
    "lease_session_hash",
    engine_core::journal_contract::META_PULL_COMPLETION_BUNDLE_DIGEST,
];

fn is_worker_owned_mutation(event_type: &str) -> bool {
    if matches!(
        event_type,
        "run.cancelled"
            | "run.queued"
            | "run.assigned"
            | "run.resumed"
            | "run.archived"
            | "run.terminalizing"
            | "approval.resolved"
            | "signal.received"
            | "timer.fired"
            | "timer.expired"
    ) {
        return false;
    }

    matches!(
        event_type,
        "approval.requested"
            | "signal.acked"
            | "timer.scheduled"
            | "timer.canceled"
            | "workflow.state.changed"
    ) || event_type.ends_with(".started")
        || event_type.ends_with(".completed")
        || event_type.ends_with(".failed")
        || event_type.ends_with(".paused")
        || event_type.ends_with(".resumed")
}

fn is_push_terminal_mutation(event_type: &str) -> bool {
    matches!(
        event_type,
        "run.completed" | "run.failed" | "run.paused" | "workflow.paused"
    )
}

fn requests_successful_run_completion(record: &Record) -> bool {
    record.event_type == "run.completed"
}

fn requests_run_cancellation(record: &Record) -> bool {
    record.event_type == "run.cancelled"
}

/// Only the runtime's own lease expiry, fenced to the lease it ends, may
/// settle the attempts that lease left open.
fn requests_run_lease_expiry(record: &Record, runtime_authored: bool) -> bool {
    runtime_authored
        && record.event_type == engine_core::lease_fence::RUN_LEASE_EXPIRED_EVENT
        && engine_core::lease_fence::is_lease_fenced(record)
}

fn run_cancellation_reason(record: &Record) -> &str {
    ["cancel_reason", "cancellation_reason", "reason"]
        .into_iter()
        .filter_map(|key| record.metadata.get(key))
        .map(String::as_str)
        .find(|value| !value.trim().is_empty())
        .unwrap_or("parent run cancelled")
}

/// Fence a push worker's terminal record with the lease the projection shows
/// for it. The segment decides the fence against its own lease entry, so a
/// lagging projection can only make the record lose, never win wrongly.
fn prepare_push_terminal_transition(record: &mut Record, run: &Run, received_at_ms: i64) {
    strip_reserved_fence_metadata(&mut record.metadata);
    record
        .metadata
        .insert("worker_id".to_string(), run.lease_worker_id.clone());
    record
        .metadata
        .insert("worker_session_id".to_string(), run.lease_worker_id.clone());
    record
        .metadata
        .insert("lease_id".to_string(), run.lease_id.clone());
    record
        .metadata
        .insert("lease_worker_id".to_string(), run.lease_worker_id.clone());
    record.metadata.insert(
        "lease_expires_at_ms".to_string(),
        run.lease_expires_at_ms.to_string(),
    );
    record
        .metadata
        .insert(RUN_META_LEASE_ATTEMPT.to_string(), run.attempt.to_string());
    record
        .metadata
        .insert("attempt".to_string(), run.attempt.to_string());
    record
        .metadata
        .insert("dispatch_mode".to_string(), "push".to_string());
    LeaseFence {
        kind: LeaseFenceKind::PushCompletion,
        lease_id: run.lease_id.clone(),
        worker_id: run.lease_worker_id.clone(),
        session_hash: String::new(),
        attempt: run.attempt,
        received_at_ms,
        expiry_reason: None,
    }
    .stamp(record);
    record.offset = Offset::ZERO;
    record.timestamp_ns = received_at_ms.saturating_mul(1_000_000);
}

fn legacy_unleased_push_mutation_allowed(
    run: &Run,
    record: &Record,
    lease_backed_push_enabled: bool,
) -> bool {
    !lease_backed_push_enabled
        && run.lease_id.is_empty()
        && run
            .metadata
            .get("dispatch_mode")
            .is_none_or(|mode| mode.eq_ignore_ascii_case("push"))
        && record
            .metadata
            .get("dispatch_mode")
            .is_some_and(|mode| mode.eq_ignore_ascii_case("push"))
}

/// Marker the in-process engine backend attaches to requests it relays for
/// runtime producers. Records in such requests may carry lease fences.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeAuthored;

/// Whether an Engine append comes from the runtime itself: either the
/// in-process backend (extension) or a peer node forwarding to the partition
/// owner (header carrying the peer token). Everything else is a worker or
/// client and may not author lease fences. A runtime with no peer nodes
/// passes `accept_peer_mark = false`, so the header proves nothing there.
fn runtime_authored_request<T>(request: &Request<T>, accept_peer_mark: bool) -> bool {
    if request.extensions().get::<RuntimeAuthored>().is_some() {
        return true;
    }
    if !accept_peer_mark {
        return false;
    }
    request
        .metadata()
        .get(engine_core::lease_fence::RUNTIME_AUTHORED_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value == engine_core::lease_fence::runtime_peer_token(&worker_session_signing_key())
        })
}

/// Remove lease fences a caller may not author. A runtime-authored request
/// keeps fences on the record types the runtime fences; every other
/// combination is stripped so the segment never sees a forged fence.
fn sanitize_ingress_fence(record: &mut Record, runtime_authored: bool) {
    if !runtime_authored || !engine_core::lease_fence::fenceable_event(&record.event_type) {
        strip_reserved_fence_metadata(&mut record.metadata);
    }
}

fn reject_external_internal_event(record: &Record, runtime_authored: bool) -> Result<(), Status> {
    if runtime_authored {
        return Ok(());
    }
    if engine_core::runtime_work::is_work_record(record)
        || engine_core::experiment_feedback::is_feedback_record(record)
    {
        return Err(Status::permission_denied(
            "runtime controller state is internal",
        ));
    }
    let reserved_terminal_outcome = matches!(
        record.event_type.as_str(),
        "run.completed" | "run.failed" | "run.cancelled"
    ) && record.metadata.contains_key("terminal_reservation_id");
    // Activation records are appended only through the activation RPCs, which
    // fold them against the journal aggregate; a marked record on the public
    // append path is a forgery.
    if engine_core::activation::has_activation_marker(record) {
        return Err(Status::invalid_argument(format!(
            "{} carries the durable activation marker and can only be appended through the activation RPCs",
            record.event_type
        )));
    }
    let unfenced_lease_event = matches!(
        record.event_type.as_str(),
        engine_core::lease_fence::RUN_LEASE_EXPIRED_EVENT
            | engine_core::lease_fence::RUN_LEASE_RENEWED_EVENT
    ) && !engine_core::lease_fence::is_lease_fenced(record);
    if matches!(
        record.event_type.as_str(),
        "run.assigned"
            | "run.archived"
            | "run.terminalizing"
            | engine_core::journal_contract::DISPATCH_RETRY_SCHEDULED_EVENT
    ) || reserved_terminal_outcome
        || unfenced_lease_event
    {
        return Err(Status::invalid_argument(format!(
            "{} is runtime-internal and cannot be appended through the public API",
            record.event_type
        )));
    }
    Ok(())
}

/// Default lease timeout applied to parked long-poll assignments.
const DEFAULT_LEASE_TIMEOUT_MS: i64 = 120_000;
const DEFAULT_WORKER_SESSION_TTL_MS: i64 = 3_600_000;
const MAX_POLL_JOB_WAIT_MS: i64 = 30_000;
// Overscan beyond the worker slot count so a small stale or unroutable prefix
// cannot hide ready work. Keep the read bounded to protect the poll hot path.
const POLL_JOB_CANDIDATE_SCAN_LIMIT: usize = 256;
// Pages of candidates one probe may walk past an unclaimable prefix before
// it gives up for this tick. Bounds probe latency under a large stale backlog.
const POLL_JOB_CANDIDATE_MAX_PAGES: usize = 8;
// Queue appends wake parked polls immediately. This is only the cross-node HA
// fallback when the append and poll land on different runtime replicas. The
// deployment probe gate coalesces all idle slots into one index scan per tick.
const POLL_JOB_RECHECK_INTERVAL_MS: u64 = 1_000;

/// Outcome of a worker routing refresh: the active count the entry carries
/// and whether this call actually republished it.
// Only the hosted runtime publishes worker routes and reads these back.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct RouteRefresh {
    active_invocations: u32,
    published: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct RouteCapacity {
    open_poll_slots: u32,
    desired_slots: u32,
    effective_max_slots: u32,
    cpu_usage: f64,
    memory_usage: f64,
    observed_at_ms: i64,
}
const PROJECTION_PUBLICATION_POLL_INTERVAL_MS: u64 = 25;
const QUEUE_METRIC_REFRESH_DELAY_MS: u64 = 100;
const QUEUE_METRIC_REFRESH_INTERVAL_MS: u64 = 1_000;
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn infer_checkpoint_correlation(
    cache: &RecordCache,
    project_id: &str,
    checkpoint: &engine_proto::DurableStepCheckpoint,
) -> (Option<String>, Option<String>) {
    let step_type = checkpoint.step_type.to_ascii_lowercase();
    let step_key = checkpoint.step_key.to_ascii_lowercase();
    let event_prefix =
        if step_type.contains("llm") || step_type.contains("lm") || step_key.contains(".lm.") {
            Some("lm.")
        } else if step_type.contains("agent") || step_key.starts_with("agent.") {
            Some("agent.")
        } else if step_type.contains("function") || step_key.contains(".function.") {
            Some("function.")
        } else {
            None
        };

    let Some(prefix) = event_prefix else {
        return (None, None);
    };
    let Some(records) = cache.get_by_run_id(project_id, &checkpoint.run_id) else {
        return (None, None);
    };

    records
        .iter()
        .rev()
        .find(|record| record.event_type.starts_with(prefix) && record.correlation_id.is_some())
        .map(|record| {
            (
                record.correlation_id.clone(),
                record.parent_event_id.clone(),
            )
        })
        .unwrap_or((None, None))
}

/// Explain whether this runtime node may dequeue a pull run at the supplied
/// durable submission timestamp. Keeping the denial reason makes the HA policy
/// observable without duplicating ownership logic in `PollJob`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PullAssignmentGateDecision {
    EligibleStandalone,
    EligibleOwner,
    EligibleFollower,
    EligibleDelegatedFollower,
    IneligibleNoOwner,
    IneligibleOwnerGrace,
    IneligibleFollowerTurn,
}

impl PullAssignmentGateDecision {
    pub const fn is_eligible(self) -> bool {
        matches!(
            self,
            Self::EligibleStandalone
                | Self::EligibleOwner
                | Self::EligibleFollower
                | Self::EligibleDelegatedFollower
        )
    }

    pub const fn decision_label(self) -> &'static str {
        match self {
            Self::EligibleStandalone
            | Self::EligibleOwner
            | Self::EligibleFollower
            | Self::EligibleDelegatedFollower => "eligible",
            Self::IneligibleNoOwner => "no_owner",
            Self::IneligibleOwnerGrace => "owner_grace",
            Self::IneligibleFollowerTurn => "follower_turn",
        }
    }

    pub const fn role_label(self) -> &'static str {
        match self {
            Self::EligibleStandalone => "standalone",
            Self::EligibleOwner => "owner",
            Self::EligibleFollower | Self::EligibleDelegatedFollower => "follower",
            Self::IneligibleNoOwner => "unknown",
            Self::IneligibleOwnerGrace | Self::IneligibleFollowerTurn => "follower",
        }
    }
}

pub type PullAssignmentGate = Arc<
    dyn Fn(&str, i64, Option<&PullReadyDelegation>) -> PullAssignmentGateDecision + Send + Sync,
>;

/// The core gRPC service implementation for the AGNT5 Engine.
///
/// Holds a segment for durable storage and a record cache for
/// zero-latency read-after-write on active runs.
pub struct EngineServiceImpl<S: Segment> {
    segment: Arc<S>,
    cache: Arc<RecordCache>,
    store: Option<Arc<dyn EngineServiceStore>>,
    query_lake: Option<Arc<ProfileQueryLake>>,
    metrics: Option<crate::telemetry::EngineMetrics>,
    start_time: Instant,
    node_id: String,
    /// Shared in-memory claim map for pull-based workers.
    ///
    /// Populated by parked `PollJob` (SKIP LOCKED insert) and drained by
    /// `CompleteJob` or the coordinator's periodic reaper.
    claim_registry: Arc<ClaimRegistry>,
    /// HA dequeue authority for pull assignments.
    ///
    /// The pending-run index is replicated to every runtime node. Without an
    /// ownership gate, each replica scans the same rows and attempts a
    /// competing `run.assigned` append; the journal idempotency fence prevents
    /// duplicate delivery only after the cluster has paid for all of those
    /// losing writes. In HA this gate gives the lifecycle-Active partition
    /// owner first refusal, then admits one rotating follower after a bounded
    /// grace period so worker/node affinity cannot strand a run. Standalone/test
    /// composition leaves it unset and retains the single-node behavior.
    pull_assignment_gate: Option<PullAssignmentGate>,
    /// Whether pull workers may claim runs stamped with `stream_mode=full`.
    /// Defaults on so Studio/local `/stream` executions do not stall, but
    /// high-fidelity token/progress streaming still depends on
    /// the EventStream fanout stage.
    streamed_pull_enabled: bool,
    /// Unary registration for parked single-job pull workers.
    /// polling. Durable correctness is still enforced by the journaled lease
    /// on the run; this registry binds a worker to its deployment lane and
    /// provides a policy hook for capability-aware workers.
    worker_sessions: Arc<WorkerSessionRegistry>,
    /// One identity and atomic reservation state per live public PollJob.
    /// Request cancellation is handled by the attempt guard's Drop path.
    poll_attempts: Arc<PollAttemptRegistry>,
    /// Process-lifetime cancellation for promptly draining parked polls.
    poll_shutdown: tokio_util::sync::CancellationToken,
    /// Wakes parked `PollJob` calls by project/deployment. Pollers use
    /// scan-register-rescan so a wakeup between the first empty scan and
    /// waiter registration is recoverable without waiting for the timeout.
    poll_waiters: Arc<PollJobWaiterRegistry>,
    /// Dedupes background queue metric refresh loops. The one-shot refresh after
    /// a queue-changing event turns into a bounded periodic refresh while the
    /// deployment still has pending work, so backlog age can keep increasing.
    queue_metric_refreshes: Arc<QueueMetricRefreshRegistry>,
    /// Shared replication-lag clock. Stamped by the
    /// partition processor on every applied record; read by the
    /// `list_runs` handler to populate `replication_lag_ms` on the
    /// response. Default is an unset clock (lag reports 0); production
    /// wires it through `run_engine` so the same handle is shared
    /// between the partition processors and the service.
    lag_clock: engine_processor::lag_clock::LagClock,
    /// Local partition processor readiness. `GetStatus` fails closed while a
    /// processor is starting, replaying, backing off, terminally failed, or
    /// when every supervisor sender has disappeared.
    processor_health:
        Option<tokio::sync::watch::Receiver<engine_processor::ProcessorHealthSnapshot>>,
    /// Above this threshold the receiving node
    /// returns a `failed_precondition` (staleness error). Clients can
    /// retry after a brief delay or fall back to the leader-routed
    /// path. 0 disables the cap (clients still see the lag in the
    /// response and can decide). Configurable via
    /// `AGNT5_LAG_FAILSAFE_MS` at process start; default 0 (off).
    lag_failsafe_ms: i64,
    /// How far back from now the hot run index is guaranteed complete. CF_RUNS
    /// retention removes a run only after it was archived longer ago than its
    /// horizon, and a run is archived after it was submitted, so every run
    /// submitted inside this window is still hot. A `list_runs` window that
    /// starts inside it skips both archive tiers. `None` never skips.
    hot_runs_complete_window_ms: Option<i64>,
    /// Pull-worker completions must write terminal records through the same
    /// partition-aware path used by gateway/coordinator dispatch. Otherwise a
    /// non-owner `CompleteJob` caller can update only its local cache while a
    /// synchronous `/run` tail is parked on the partition owner.
    completion_append_backend: RwLock<Option<Arc<dyn EngineBackend>>>,
    /// Bound lock memory independently of retained workflow history. Equal
    /// entity identities always share a stripe; collisions only serialize
    /// unrelated saves. Hold through append and write-through so same-entity
    /// compare-and-set writers cannot race on this Engine instance.
    entity_state_write_locks: [tokio::sync::Mutex<()>; 1024],
    /// Optional single-project/deployment boundary used by the single-process profile.
    ///
    /// Managed composition leaves this unset and applies tenant policy at its
    /// authenticated gateway. The standalone binary binds gRPC directly, so
    /// it must reject records and workers outside its configured scope.
    execution_scope: Option<ExecutionScope>,
    /// Whether the runtime-authored header from a peer node counts as proof
    /// that a request comes from the runtime. Only a runtime with peers needs
    /// it; anything that can reach the port and knows the signing key can
    /// compute the header.
    accept_peer_runtime_mark: bool,
}

#[derive(Clone)]
struct ExecutionScope {
    project_id: Arc<str>,
    deployment_id: Arc<str>,
}

impl<S: Segment> EngineServiceImpl<S> {
    pub fn new(
        segment: Arc<S>,
        cache: Arc<RecordCache>,
        store: Option<Arc<dyn EngineServiceStore>>,
        node_id: String,
    ) -> Self {
        // Studio executes components through `/stream`, which marks queued runs
        // with `stream_mode=full`. Parked pull workers must claim those runs or
        // local UI execution stalls forever. Keep an explicit opt-out while the
        // deployment explicitly disables streamed pull claims.
        let streamed_pull_enabled = std::env::var("AGNT5_STREAMED_PULL_ENABLED")
            .map(|v| !matches!(v.as_str(), "0" | "false" | "FALSE" | "no" | "NO"))
            .unwrap_or(true);
        if !streamed_pull_enabled {
            tracing::warn!("AGNT5_STREAMED_PULL_ENABLED=0 — poll_job will skip streamed runs");
        }
        // Optional staleness cap. Default off: the
        // response carries `replication_lag_ms` and clients decide.
        let lag_failsafe_ms = std::env::var("AGNT5_LAG_FAILSAFE_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v >= 0)
            .unwrap_or(0);
        if lag_failsafe_ms > 0 {
            tracing::info!(
                lag_failsafe_ms,
                "AGNT5_LAG_FAILSAFE_MS — list_runs returns failed_precondition above this threshold"
            );
        }

        Self {
            segment,
            cache,
            store,
            query_lake: None,
            metrics: None,
            start_time: Instant::now(),
            node_id,
            claim_registry: ClaimRegistry::new(),
            pull_assignment_gate: None,
            streamed_pull_enabled,
            worker_sessions: Arc::new(WorkerSessionRegistry::default()),
            poll_attempts: Arc::new(PollAttemptRegistry::default()),
            poll_shutdown: tokio_util::sync::CancellationToken::new(),
            poll_waiters: Arc::new(PollJobWaiterRegistry::default()),
            queue_metric_refreshes: Arc::new(QueueMetricRefreshRegistry::default()),
            lag_clock: engine_processor::lag_clock::LagClock::new(),
            processor_health: None,
            lag_failsafe_ms,
            hot_runs_complete_window_ms: None,
            completion_append_backend: RwLock::new(None),
            entity_state_write_locks: std::array::from_fn(|_| tokio::sync::Mutex::new(())),
            execution_scope: None,
            accept_peer_runtime_mark: true,
        }
    }

    /// Stop treating the peer runtime-authored header as proof of a runtime
    /// caller. A single-process runtime has no peers, so every legitimate
    /// runtime-authored request arrives in process with the
    /// [`RuntimeAuthored`] extension instead.
    pub fn without_peer_runtime_mark(mut self) -> Self {
        self.accept_peer_runtime_mark = false;
        self
    }

    /// Restrict direct EngineService mutations to one execution scope.
    pub fn with_execution_scope(
        mut self,
        project_id: impl Into<String>,
        deployment_id: impl Into<String>,
    ) -> Self {
        self.execution_scope = Some(ExecutionScope {
            project_id: Arc::from(project_id.into()),
            deployment_id: Arc::from(deployment_id.into()),
        });
        self
    }

    /// Use the runtime's process-lifetime token to drain parked polls during
    /// graceful shutdown instead of waiting for their public deadlines.
    pub fn with_poll_shutdown(mut self, shutdown: tokio_util::sync::CancellationToken) -> Self {
        self.poll_shutdown = shutdown;
        self
    }

    /// Publish the distinct demand keys represented by unreserved parked
    /// polls. First/last transitions are serialized with poll reservation.
    pub fn with_pull_interest_publisher(
        self,
        publisher: tokio::sync::watch::Sender<BTreeSet<engine_core::demand::DemandKey>>,
    ) -> Self {
        self.poll_attempts.set_interest_publisher(publisher);
        self
    }

    fn validate_project_scope(&self, project_id: &str) -> Result<(), Status> {
        if let Some(scope) = &self.execution_scope {
            if project_id != scope.project_id.as_ref() {
                return Err(Status::permission_denied(format!(
                    "project {project_id:?} is outside this runtime's configured scope"
                )));
            }
        }
        Ok(())
    }

    fn validate_deployment_scope(&self, deployment_id: &str) -> Result<(), Status> {
        if let Some(scope) = &self.execution_scope {
            if deployment_id != scope.deployment_id.as_ref() {
                return Err(Status::permission_denied(format!(
                    "deployment {deployment_id:?} is outside this runtime's configured scope"
                )));
            }
        }
        Ok(())
    }

    fn validate_record_scope(&self, record: &Record) -> Result<(), Status> {
        self.validate_project_scope(&record.project_id)?;
        if let Some(deployment_id) = record.metadata.get("deployment_id") {
            self.validate_deployment_scope(deployment_id)?;
        }
        Ok(())
    }

    async fn authorize_external_run(
        &self,
        identity: Option<&ExternalWorkerIdentity>,
        project_id: &str,
        run_id: &str,
    ) -> Result<(), Status> {
        authorize_external_project(identity, project_id)?;
        let Some(identity) = identity else {
            return Ok(());
        };
        if run_id.is_empty() {
            return Err(Status::permission_denied(
                "external workload request requires run authority",
            ));
        }
        let run = self
            .authoritative_completion_run(project_id, run_id, None)
            .await?;
        // Do not compare the caller to the run's current lease here: an exact
        // durable activation retry remains valid after that lease changes.
        // Mutation ownership is decided by the existing session/fence checks.
        authorize_external_deployment(Some(identity), project_id, &run.deployment_id)
    }

    /// Get a reference to the service's explicit storage port bundle.
    pub fn store(&self) -> Option<&Arc<dyn EngineServiceStore>> {
        self.store.as_ref()
    }

    /// Rehydrate one activation exclusively from canonical journal records.
    async fn load_activation_state(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> Result<Option<ActivationState>, EngineError> {
        self.segment
            .load_activation_authority(project_id, run_id, activation_id)
            .await
    }

    /// Load an activation that a fenced follow-up command requires.
    ///
    /// HA appends can be acknowledged after quorum while the replica serving
    /// the next RPC is still catching up. A missing aggregate is therefore
    /// ambiguous for a short bounded window; do not turn normal replication
    /// lag into an irreversible ILLEGAL_TRANSITION decision.
    async fn load_required_activation_state(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> Result<ActivationState, Status> {
        for attempt in 0..=ACTIVATION_VISIBILITY_RETRY_ATTEMPTS {
            let current = self
                .load_activation_state(project_id, run_id, activation_id)
                .await
                .map_err(|error| {
                    engine_err_to_status(activation_recovery_error(activation_id, error))
                })?;
            if let Some(current) = current {
                if attempt > 0 {
                    debug!(
                        project_id,
                        run_id,
                        activation_id,
                        visibility_attempt = attempt,
                        "Activation aggregate became visible after replica catch-up"
                    );
                }
                return Ok(current);
            }
            if attempt < ACTIVATION_VISIBILITY_RETRY_ATTEMPTS {
                tokio::time::sleep(ACTIVATION_VISIBILITY_RETRY_INTERVAL).await;
            }
        }
        Err(Status::unavailable(format!(
            "activation {activation_id} is not yet visible on this replica"
        )))
    }

    async fn reject_unsettled_required_children(
        &self,
        project_id: &str,
        run_id: &str,
        parent_activation_id: Option<&str>,
    ) -> Result<(), Status> {
        let child = self
            .segment
            .first_unsettled_required_child(project_id, run_id, parent_activation_id)
            .await
            .map_err(|error| {
                engine_err_to_status(activation_recovery_error(
                    parent_activation_id.unwrap_or(run_id),
                    error,
                ))
            })?;
        let Some(child_activation_id) = child else {
            return Ok(());
        };
        Err(activation_error_to_status(
            ActivationError::RequiredChildUnresolved {
                activation_id: parent_activation_id.unwrap_or(run_id).to_string(),
                child_activation_id,
            },
        ))
    }

    /// Build canonical cancellation receipts for every nonterminal activation
    /// owned by a run. Callers append these records in the same batch as the
    /// parent `run.cancelled` record so cancellation propagation cannot be
    /// partially observed.
    async fn activation_cancellation_records(
        &self,
        project_id: &str,
        run_id: &str,
        reason: &str,
    ) -> Result<Vec<Record>, Status> {
        let states = self
            .segment
            .list_nonterminal_activation_authority(project_id, run_id)
            .await
            .map_err(|error| engine_err_to_status(activation_recovery_error(run_id, error)))?;
        let mut states = states;
        states.sort_by(|left, right| {
            left.identity
                .activation_id
                .cmp(&right.identity.activation_id)
        });
        let mut records = Vec::new();
        for state in states {
            let transition = activation::decide_cancel(
                &state,
                &CancelActivationCommand {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                    activation_id: state.identity.activation_id.clone(),
                    reason: reason.to_string(),
                },
            )
            .map_err(activation_error_to_status)?;
            for canonical in transition.records {
                records.push(
                    activation::to_engine_record(&canonical, now_unix_ns())
                        .map_err(activation_error_to_status)?,
                );
            }
        }
        Ok(records)
    }

    /// Failures for every activation a run's expiring lease left active, to
    /// append in the same batch as `run.lease_expired`
    /// (see [`activation::lease_loss_failures`]).
    async fn activation_lease_loss_records(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<Vec<Record>, Status> {
        let states = self
            .segment
            .list_nonterminal_activation_authority(project_id, run_id)
            .await
            .map_err(|error| engine_err_to_status(activation_recovery_error(run_id, error)))?;
        activation::lease_loss_failures(&states, now_unix_ns()).map_err(activation_error_to_status)
    }

    async fn append_activation_record(
        &self,
        current: Option<ActivationState>,
        canonical: &activation::ActivationJournalRecord,
        validated_run: Option<&Run>,
    ) -> Result<(ActivationState, Offset, bool), EngineError> {
        let mut record =
            activation::to_engine_record(canonical, now_unix_ns()).map_err(EngineError::from)?;
        if let Some(run) = validated_run {
            ActivationLeaseGuard {
                lease_id: run.lease_id.clone(),
                attempt: run.attempt,
            }
            .stamp(&mut record);
        }
        let activation_id = record
            .metadata
            .get(activation::META_ACTIVATION_ID)
            .cloned()
            .unwrap_or_default();
        let outcome = self
            .segment
            .append_batch_with_outcome(std::slice::from_ref(&record))
            .await
            .map_err(|error| activation_append_error(&activation_id, error))?;
        outcome
            .validate_cardinality(1, "activation append")
            .map_err(|error| activation_append_error(&activation_id, error))?;
        let offset = outcome.offsets[0];
        if outcome.appended[0] {
            let state = activation::apply_journal_record(current, canonical, offset)
                .map_err(EngineError::from)?;
            let mut cached = record;
            cached.offset = offset;
            self.cache.put(Arc::new(cached));
            return Ok((state, offset, true));
        }

        let end = offset.0.checked_add(1).ok_or_else(|| {
            EngineError::Activation(ActivationError::UnknownWriteOutcome {
                activation_id: activation_id.clone(),
                reason: "accepted journal offset overflow".to_string(),
            })
        })?;
        let existing = self
            .segment
            .read_range(offset, Offset(end))
            .await
            .map_err(|error| {
                EngineError::Activation(ActivationError::UnknownWriteOutcome {
                    activation_id: activation_id.clone(),
                    reason: format!(
                        "canonical idempotent winner at offset {} is not readable: {error}",
                        offset.0
                    ),
                })
            })?
            .into_iter()
            .next()
            .ok_or_else(|| {
                EngineError::Activation(ActivationError::UnknownWriteOutcome {
                    activation_id: activation_id.clone(),
                    reason: format!(
                        "canonical idempotent winner at offset {} is missing",
                        offset.0
                    ),
                })
            })?;
        let existing = activation::from_engine_record(&existing)
            .map_err(EngineError::from)?
            .ok_or_else(|| {
                EngineError::Internal(
                    "idempotency winner is not an activation journal record".to_string(),
                )
            })?;
        let state = activation::apply_journal_record(current, &existing, offset)
            .map_err(EngineError::from)?;
        Ok((state, offset, false))
    }

    /// Commit the activation suspension, run pause, and timer schedule as one
    /// same-partition journal transaction. The activation record owns retry
    /// identity; an exact RPC retry observes the suspended aggregate and never
    /// attempts this batch again.
    async fn append_activation_suspension(
        &self,
        current: ActivationState,
        canonical: &activation::ActivationJournalRecord,
        request: &SuspendActivationRequest,
        run: Option<&Run>,
    ) -> Result<(ActivationState, Offset), EngineError> {
        let timestamp_ns = now_unix_ns();
        let activation_record =
            activation::to_engine_record(canonical, timestamp_ns).map_err(EngineError::from)?;
        let timer_id = activation::suspension_timer_id(request);
        // Lease-backed pauses are projected under their lease generation.
        // The timer must carry that same identity or its firing is stale.
        let resume_source = run
            .map(|run| format!("lease:{}", run.lease_id))
            .unwrap_or_else(|| format!("timer:{timer_id}"));
        let continuation_b64 =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&request.continuation);

        let shared_metadata = std::collections::HashMap::from([
            ("activation_id".to_string(), request.activation_id.clone()),
            (
                "activation_attempt".to_string(),
                request.attempt.to_string(),
            ),
            ("timer_id".to_string(), timer_id.clone()),
            ("timer_key".to_string(), request.timer_key.clone()),
            ("ready_at_ms".to_string(), request.ready_at_ms.to_string()),
            (
                engine_core::lease_fence::META_RESUME_SOURCE_ID.to_string(),
                resume_source,
            ),
            (
                "worker_session_id".to_string(),
                current.current_attempt.worker_session_id.clone(),
            ),
        ]);

        let pause_data = serde_json::to_vec(&json!({
            "status": "suspended",
            "reason": "timer",
            "timer_id": timer_id,
            "timer_key": request.timer_key,
            "ready_at_ms": request.ready_at_ms,
        }))
        .map_err(|error| EngineError::Internal(format!("encode suspension pause: {error}")))?;
        let mut pause_record = Record {
            offset: Offset::ZERO,
            project_id: request.project_id.clone(),
            run_id: request.run_id.clone(),
            event_type: "workflow.paused".to_string(),
            data: Bytes::from(pause_data),
            timestamp_ns,
            step_key: None,
            correlation_id: Some(format!("activation-suspend-{}", request.activation_id)),
            parent_event_id: None,
            metadata: shared_metadata.clone(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        // Release the exact run lease validated for this activation in the
        // same durable batch as the suspension and timer. A newer assignment
        // racing this append must reject the entire batch at the lease fence.
        if let Some(run) = run {
            LeaseFence {
                kind: if run_uses_pull_lease(run) {
                    LeaseFenceKind::PullCompletion
                } else {
                    LeaseFenceKind::PushCompletion
                },
                lease_id: run.lease_id.clone(),
                worker_id: run.lease_worker_id.clone(),
                session_hash: run
                    .metadata
                    .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
                    .cloned()
                    .unwrap_or_default(),
                attempt: run.attempt,
                received_at_ms: timestamp_ns / 1_000_000,
                expiry_reason: None,
            }
            .stamp(&mut pause_record);
        }

        let mut timer_metadata = shared_metadata.clone();
        timer_metadata.insert("waiting_step".to_string(), request.timer_key.clone());
        let timer_data = serde_json::to_vec(&json!({
            "fire_at_ns": request.ready_at_ms.saturating_mul(1_000_000),
            "ready_at_ms": request.ready_at_ms,
            "schedule_payload": {
                "resume_metadata": {
                    engine_core::lease_fence::META_RESUME_SOURCE_ID: shared_metadata
                        .get(engine_core::lease_fence::META_RESUME_SOURCE_ID),
                    "activation_id": request.activation_id,
                    "activation_attempt": request.attempt.to_string(),
                    "timer_id": timer_id,
                    "timer_key": request.timer_key,
                    "continuation_b64": continuation_b64,
                }
            }
        }))
        .map_err(|error| EngineError::Internal(format!("encode suspension timer: {error}")))?;
        let timer_record = Record {
            offset: Offset::ZERO,
            project_id: request.project_id.clone(),
            run_id: request.run_id.clone(),
            event_type: "timer.scheduled".to_string(),
            data: Bytes::from(timer_data),
            timestamp_ns,
            step_key: None,
            correlation_id: Some(format!("activation-timer-{timer_id}")),
            parent_event_id: None,
            metadata: timer_metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        // Settle other open attempts before the pause releases their lease.
        // The suspension stays first: its offset is the accepted offset below.
        let mut attempts_left = SUSPENSION_APPEND_ATTEMPTS;
        let (records, outcome) = loop {
            let open_states = self
                .segment
                .list_nonterminal_activation_authority(&request.project_id, &request.run_id)
                .await
                .map_err(|error| activation_append_error(&request.activation_id, error))?;
            let sibling_records = activation::suspension_sibling_failures(
                &open_states,
                &request.activation_id,
                timestamp_ns,
            )
            .map_err(EngineError::from)?;
            let mut records = Vec::with_capacity(sibling_records.len() + 3);
            records.push(activation_record.clone());
            records.extend(sibling_records);
            records.push(pause_record.clone());
            records.push(timer_record.clone());
            match self.segment.append_batch_with_outcome(&records).await {
                Ok(outcome) => break (records, outcome),
                // A step finished between the read and the append, so its result
                // holds the slot the failure wanted. A step finishing must not
                // cost the suspension: nothing was written, so read again.
                Err(EngineError::Activation(ActivationError::PayloadConflict {
                    activation_id,
                    ..
                })) if activation_id != request.activation_id && attempts_left > 1 => {
                    attempts_left -= 1;
                }
                Err(EngineError::LeaseFenceMismatch { .. }) => {
                    return Err(EngineError::Activation(ActivationError::StaleAuthority {
                        activation_id: request.activation_id.clone(),
                        request_attempt: request.attempt,
                        current_attempt: current.current_attempt.attempt,
                    }));
                }
                Err(error) => return Err(activation_append_error(&request.activation_id, error)),
            }
        };
        outcome
            .validate_cardinality(records.len(), "activation suspension append")
            .map_err(|error| activation_append_error(&request.activation_id, error))?;
        if outcome.appended.iter().any(|appended| !appended) {
            return Err(EngineError::Activation(
                ActivationError::UnknownWriteOutcome {
                    activation_id: request.activation_id.clone(),
                    reason: "activation suspension transaction did not append as one new batch"
                        .to_string(),
                },
            ));
        }

        let accepted_offset = outcome.offsets[0];
        let state = activation::apply_journal_record(Some(current), canonical, accepted_offset)
            .map_err(EngineError::from)?;
        for (mut record, offset) in records.into_iter().zip(outcome.offsets) {
            record.offset = offset;
            self.cache.put(Arc::new(record));
        }
        // The suspension already stands, so failing here only risks the rare step
        // this sweep exists for.
        if let Err(error) = self
            .settle_steps_begun_during_suspension(request, timestamp_ns)
            .await
        {
            tracing::warn!(
                run_id = %request.run_id,
                activation_id = %request.activation_id,
                error = %error,
                "could not settle steps begun while the run suspended"
            );
        }
        Ok((state, accepted_offset))
    }

    /// A step can begin between the last read and the suspension's append. The
    /// pause then fences it off from finishing, and the resumed run would wait on
    /// it forever, so settle it the same way. No result can race this write,
    /// because the released lease rejects them.
    async fn settle_steps_begun_during_suspension(
        &self,
        request: &SuspendActivationRequest,
        timestamp_ns: i64,
    ) -> Result<(), EngineError> {
        let open_states = self
            .segment
            .list_nonterminal_activation_authority(&request.project_id, &request.run_id)
            .await?;
        let records = activation::suspension_sibling_failures(
            &open_states,
            &request.activation_id,
            timestamp_ns,
        )
        .map_err(EngineError::from)?;
        if records.is_empty() {
            return Ok(());
        }
        let outcome = self.segment.append_batch_with_outcome(&records).await?;
        for (mut record, offset) in records.into_iter().zip(outcome.offsets) {
            record.offset = offset;
            self.cache.put(Arc::new(record));
        }
        Ok(())
    }

    /// Protect upgraded stores whose pre-V1 entity versions predate the
    /// additive journal version index. Once V1 writes begin, the journal
    /// adapter remains authoritative and rejects races even if this projection
    /// is behind; this check only rejects a projection version newer than the
    /// caller's expectation.
    async fn reject_stale_activation_state_baseline(
        &self,
        request: &CompleteActivationRequest,
    ) -> Result<(), EngineError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        for mutation in &request.state_mutations {
            let current = WorkflowQueryStore::load_entity_state(
                store.as_ref(),
                &request.project_id,
                &mutation.entity_type,
                &mutation.entity_key,
                &mutation.scope,
                &mutation.scope_id,
            )
            .await?
            .map(|(_, version)| version)
            .unwrap_or(0);
            if current > mutation.expected_version {
                return Err(ActivationError::StateVersionConflict {
                    activation_id: request.activation_id.clone(),
                    attempt: request.attempt,
                    target: format!(
                        "{}/{}/{}/{}",
                        mutation.entity_type,
                        mutation.entity_key,
                        mutation.scope,
                        mutation.scope_id
                    ),
                    expected_version: mutation.expected_version,
                    current_version: current,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Bind Durable Activation authority to the run's current execution lease.
    ///
    /// The activation aggregate's fence prevents two attempts of the same
    /// logical step from committing. This additional check prevents an old
    /// handler from using that still-valid activation fence after its parent
    /// run lease has expired or moved to another worker. Exact activation
    /// replays bypass this check because they append no new record.
    #[allow(clippy::too_many_arguments)]
    async fn validate_activation_run_authority(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
        activation_attempt: u32,
        worker_session_id: &str,
        run_authority: &[u8],
        lease_authority: &[u8],
    ) -> Result<Option<Run>, Status> {
        if worker_session_id.is_empty() || run_authority.is_empty() || lease_authority.is_empty() {
            return Err(activation_error_to_status(
                ActivationError::StaleAuthority {
                    activation_id: activation_id.to_string(),
                    request_attempt: activation_attempt,
                    current_attempt: activation_attempt,
                },
            ));
        }

        if self.completion_append_backend().is_none() && self.store.is_none() {
            // Unit-only/legacy compositions without a materialized run store
            // retain the activation aggregate's own fence. Production
            // profiles always configure one of the authoritative read paths.
            return Ok(None);
        }
        // PollJob returns only after run.assigned is durably committed, while
        // the run projection may still say queued. Validate against the same
        // journal-backed assignment fallback used by worker lifecycle commits
        // so projection lag cannot revoke already-granted execution authority.
        let run = self
            .authoritative_completion_run(project_id, run_id, None)
            .await
            .map_err(|status| {
                if status.code() == tonic::Code::NotFound {
                    return activation_error_to_status(ActivationError::StaleAuthority {
                        activation_id: activation_id.to_string(),
                        request_attempt: activation_attempt,
                        current_attempt: activation_attempt,
                    });
                }
                status
            })?;
        let projected_session_hash = run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
            .map(String::as_str)
            .unwrap_or_default();
        let session_matches = if run_uses_pull_lease(&run) {
            engine_core::lease_fence::worker_session_hash(worker_session_id)
                == projected_session_hash
        } else {
            // PUSH execution uses the durable lease worker as its session
            // identity. Its complete assignment also carries a session hash,
            // so hash presence alone cannot distinguish it from PULL.
            worker_session_id == run.lease_worker_id
        };
        let projected_attempt = run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_ATTEMPT)
            .and_then(|value| value.parse::<u32>().ok());
        let checked_at_ms = now_ms();
        let active = run.project_id == project_id
            && run.run_id == run_id
            && matches!(run.status.as_str(), "assigned" | "started")
            && !run.lease_id.is_empty()
            && lease_authority == run.lease_id.as_bytes()
            && session_matches
            && projected_attempt == Some(run.attempt)
            && run.lease_expires_at_ms > checked_at_ms;
        if active {
            Ok(Some(run))
        } else {
            Err(activation_error_to_status(
                ActivationError::StaleAuthority {
                    activation_id: activation_id.to_string(),
                    request_attempt: activation_attempt,
                    current_attempt: activation_attempt,
                },
            ))
        }
    }

    fn reject_worker_mutation_authority(&self, reason: &'static str) -> Result<(), Status> {
        if let Some(metrics) = &self.metrics {
            metrics
                .stale_worker_mutations
                .add(1, &[opentelemetry::KeyValue::new("reason", reason)]);
        }
        Err(Status::failed_precondition(format!(
            "worker lifecycle mutation rejected: {reason}"
        )))
    }

    /// Verify that a worker-authored lifecycle record still belongs to the
    /// current durable run lease. Cooperative handler cancellation reduces
    /// wasted work, but this check remains the final commit boundary when user
    /// code ignores cancellation or an old process reconnects late.
    async fn validate_worker_mutation_authority(&self, record: &mut Record) -> Result<(), Status> {
        // Public append rejects this reserved namespace before reaching here.
        // Built-in controllers own journal CAS claims, not worker run leases.
        if engine_core::runtime_work::RuntimeWork::from_record(record)
            .map_err(engine_err_to_status)?
            .is_some()
        {
            return Ok(());
        }
        // Activation records are folded against the journal aggregate by the
        // activation RPCs; the suffix rules below do not apply to them.
        if engine_core::activation::has_activation_marker(record)
            || !is_worker_owned_mutation(&record.event_type)
        {
            return Ok(());
        }

        if self.completion_append_backend().is_none() && self.store.is_none() {
            // Unit-only and legacy in-process compositions without a
            // materialized run store cannot validate lease authority. All
            // production profiles configure one of the read paths above.
            return Ok(());
        }
        let run = self
            .authoritative_completion_run(
                &record.project_id,
                &record.run_id,
                crate::poll_jobs::assignment_commit_offset(&record.metadata),
            )
            .await
            .or_else(|status| {
                if status.code() == tonic::Code::NotFound {
                    self.reject_worker_mutation_authority("run_not_found")?;
                }
                Err(status)
            })?;

        // PUSH delivery can beat the dispatch outbox's assignment append.
        // The committed-assignment fallback above bridges projection lag, but
        // cannot authorize an assignment that has not committed yet. Reject
        // without writing and let the SDK retry its exact lifecycle request.
        if run.status == "queued" {
            return Err(Status::unavailable(
                "worker assignment is not yet committed",
            ));
        }
        if !matches!(run.status.as_str(), "assigned" | "started") {
            return self.reject_worker_mutation_authority("run_not_active");
        }
        if legacy_unleased_push_mutation_allowed(
            &run,
            record,
            engine_processor::orchestrator::lease_backed_push_enabled(),
        ) {
            return Ok(());
        }
        let received_at_ms = now_ms();
        if run.lease_expires_at_ms <= received_at_ms {
            return self.reject_worker_mutation_authority("lease_expired");
        }

        let worker_id = record
            .metadata
            .get("worker_id")
            .map(String::as_str)
            .unwrap_or_default();
        let worker_session_id = record
            .metadata
            .get("worker_session_id")
            .map(String::as_str)
            .unwrap_or_default();
        let lease_id = record
            .metadata
            .get("lease_id")
            .map(String::as_str)
            .unwrap_or_default();
        let lease_attempt = record
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_ATTEMPT)
            .and_then(|value| value.parse::<u32>().ok());
        let dispatch_mode = record
            .metadata
            .get("dispatch_mode")
            .map(|value| value.to_ascii_lowercase());
        if worker_id.is_empty() {
            return self.reject_worker_mutation_authority("worker_id_missing");
        }
        if worker_session_id.is_empty() {
            return self.reject_worker_mutation_authority("worker_session_id_missing");
        }
        if lease_id.is_empty() {
            return self.reject_worker_mutation_authority("lease_id_missing");
        }
        if lease_attempt.is_none() {
            return self.reject_worker_mutation_authority("lease_attempt_missing");
        }
        if dispatch_mode.is_none() {
            return self.reject_worker_mutation_authority("dispatch_mode_missing");
        }

        let projected_attempt = run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_ATTEMPT)
            .and_then(|value| value.parse::<u32>().ok());
        let projected_session_hash = run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
            .map(String::as_str)
            .unwrap_or_default();
        let projected_pull = run_uses_pull_lease(&run);
        let mode_matches = if projected_pull {
            dispatch_mode.as_deref() == Some("pull")
        } else {
            dispatch_mode.as_deref() == Some("push")
        };
        let session_matches = if projected_pull {
            engine_core::lease_fence::worker_session_hash(worker_session_id)
                == projected_session_hash
        } else {
            worker_session_id == run.lease_worker_id
        };
        let active = run.project_id == record.project_id
            && run.run_id == record.run_id
            && !run.lease_id.is_empty()
            && lease_id == run.lease_id
            && worker_id == run.lease_worker_id
            && lease_attempt == Some(run.attempt)
            && projected_attempt == Some(run.attempt)
            && mode_matches
            && session_matches;
        if !active {
            return self.reject_worker_mutation_authority("authority_stale");
        }

        if !projected_pull && is_push_terminal_mutation(&record.event_type) {
            prepare_push_terminal_transition(record, &run, received_at_ms);
        }

        Ok(())
    }

    async fn apply_guardrails(&self, record: Record) -> Vec<Record> {
        vec![record]
    }

    fn guardrails_enabled(&self) -> bool {
        {
            false
        }
    }

    async fn existing_terminal_for_late_event(
        &self,
        record: &Record,
    ) -> Option<(Offset, i64, String)> {
        if engine_core::runtime_work::is_work_record(record)
            || !should_drop_after_terminal(&record.event_type)
        {
            return None;
        }

        if let Some(records) = self.cache.get_by_run_id(&record.project_id, &record.run_id) {
            if let Some(terminal) = records
                .iter()
                .filter_map(|record| {
                    terminal_status_for_record(record)
                        .map(|status| (record.offset, record.timestamp_ns, status.to_string()))
                })
                .min_by_key(|(offset, _, _)| offset.0)
            {
                return Some(terminal);
            }

            return None;
        }

        let Some(store) = &self.store else {
            return None;
        };

        if let Ok(events) =
            RunQueryStore::load_run_events(store.as_ref(), &record.project_id, &record.run_id, 0)
                .await
        {
            if let Some(terminal) = events
                .iter()
                .filter_map(|record| {
                    terminal_status_for_record(record)
                        .map(|status| (record.offset, record.timestamp_ns, status.to_string()))
                })
                .min_by_key(|(offset, _, _)| offset.0)
            {
                return Some(terminal);
            }
        }

        if let Ok(Some(run_bytes)) =
            RunQueryStore::load_run(store.as_ref(), &record.project_id, &record.run_id).await
        {
            if let Ok(run) = Run::decode(run_bytes.as_slice()) {
                if is_terminal_run_status(&run.status) {
                    return Some((Offset::ZERO, 0, run.status));
                }
            }
        }

        None
    }

    /// Whether an existing run is observed, read from its `run.queued` record.
    /// `None` when the run isn't known yet.
    async fn existing_run_is_observed(&self, project_id: &str, run_id: &str) -> Option<bool> {
        // The cache may hold only a run's later records, so a miss on
        // run.queued there falls through to the store.
        if let Some(observed) = self
            .cache
            .get_by_run_id(project_id, run_id)
            .and_then(|records| {
                records
                    .iter()
                    .find(|record| record.event_type == "run.queued")
                    .map(|record| engine_core::journal_contract::is_observed_record(record))
            })
        {
            return Some(observed);
        }
        let store = self.store.as_ref()?;
        let events = RunQueryStore::load_run_events(store.as_ref(), project_id, run_id, 0)
            .await
            .ok()?;
        events
            .iter()
            .find(|record| record.event_type == "run.queued")
            .map(engine_core::journal_contract::is_observed_record)
    }

    /// Refuse an `AppendObserved` batch aimed at a native run. Returns whether
    /// the observed run already exists.
    async fn check_observed_run_target(&self, first: &Record) -> Result<bool, Status> {
        match self
            .existing_run_is_observed(&first.project_id, &first.run_id)
            .await
        {
            Some(true) => Ok(true),
            Some(false) => Err(Status::failed_precondition(format!(
                "run {} is not an observed run",
                first.run_id
            ))),
            None => Ok(false),
        }
    }

    /// Pull workers must cross the authenticated `CompleteJob` boundary for
    /// every lease-ending outcome. The generic journal API remains available
    /// to push workers, but it must not let an SDK event queue bypass an active
    /// pull lease's worker/session/attempt fence.
    async fn reject_unfenced_pull_terminal(&self, record: &Record) -> Result<(), Status> {
        let runtime_authored_pull = record
            .metadata
            .get("dispatch_mode")
            .is_some_and(|mode| mode == "pull");
        if record.event_type == "run.cancelled" {
            if runtime_authored_pull {
                return Err(Status::failed_precondition(
                    "pull workers cannot author run.cancelled",
                ));
            }
            // Cancellation is authored by the control-plane cancel boundary,
            // which performs its own conflict handling and may end an active
            // pull lease.
            return Ok(());
        }
        if !matches!(
            record.event_type.as_str(),
            "run.completed" | "run.failed" | "run.paused" | "workflow.paused"
        ) {
            return Ok(());
        }
        if runtime_authored_pull {
            return Err(Status::failed_precondition(
                "pull assignments must terminate through CompleteJob",
            ));
        }

        let run = if let Some(backend) = self.completion_append_backend() {
            backend
                .get_run(&record.project_id, &record.run_id)
                .await
                .map_err(Status::from)?
        } else if let Some(store) = &self.store {
            RunQueryStore::load_run(store.as_ref(), &record.project_id, &record.run_id)
                .await
                .map_err(engine_err_to_status)?
        } else {
            None
        };
        let Some(run) = run else {
            return Ok(());
        };
        let run = Run::decode(run.as_slice())
            .map_err(|error| Status::internal(format!("decode active Run: {error}")))?;
        let active_pull_lease = !run.lease_id.is_empty() && run_uses_pull_lease(&run);
        if active_pull_lease {
            return Err(Status::failed_precondition(
                "active pull leases must terminate through CompleteJob",
            ));
        }

        Ok(())
    }

    /// Get a clone of the shared claim registry handle. Use this when
    /// constructing the coordinator so both sides observe the same claims.
    pub fn claim_registry(&self) -> Arc<ClaimRegistry> {
        self.claim_registry.clone()
    }

    async fn publish_event_stream_messages(
        &self,
        messages: Vec<EventStreamMessage>,
    ) -> Result<i64, Status> {
        let mut published = 0i64;
        for message in messages {
            if message.project_id.is_empty()
                || message.run_id.is_empty()
                || message.event_type.is_empty()
            {
                return Err(Status::invalid_argument(
                    "project_id, run_id and event_type are required",
                ));
            }
            let mut metadata = std::collections::HashMap::new();
            if !message.worker_id.is_empty() {
                metadata.insert("worker_id".to_string(), message.worker_id);
            }
            metadata.insert(TRANSIENT_EVENT_METADATA_KEY.to_string(), "true".to_string());
            let record = Record {
                offset: Offset::ZERO,
                project_id: message.project_id,
                run_id: message.run_id,
                event_type: message.event_type,
                data: Bytes::from(message.data),
                timestamp_ns: if message.source_timestamp_ns > 0 {
                    message.source_timestamp_ns
                } else {
                    now_unix_ns()
                },
                step_key: None,
                correlation_id: (!message.trace_id.is_empty()).then_some(message.trace_id),
                parent_event_id: (!message.span_id.is_empty()).then_some(message.span_id),
                metadata,
                data_type: "json".to_string(),
                data_checksum: None,
                data_compressed: false,
            };
            if let Some(backend) = self.completion_append_backend() {
                backend.append(record).await.map_err(Status::from)?;
            } else {
                self.cache.publish_transient(Arc::new(record));
            }
            published += 1;
        }
        Ok(published)
    }

    fn worker_session_component_route(
        &self,
        session: &WorkerSession,
        demand_key: &engine_core::demand::DemandKey,
    ) -> Option<engine_proto::ComponentRouteSpec> {
        if session.project_id != demand_key.project_id()
            || session.deployment_id != demand_key.deployment_id()
        {
            return None;
        }
        if let Some(component) = session.route_components.iter().find(|component| {
            component.name == demand_key.component_name()
                && component.component_type == demand_key.component_type_value()
        }) {
            return Some(component.clone());
        }

        None
    }

    fn worker_session_demand_keys(
        &self,
        session: &WorkerSession,
    ) -> BTreeSet<engine_core::demand::DemandKey> {
        let routes = if session.route_components.is_empty() {
            {
                Vec::new()
            }
        } else {
            session.route_components.clone()
        };
        routes
            .iter()
            .filter_map(|route| {
                engine_core::demand::DemandKey::from_worker_route(
                    &session.project_id,
                    &session.deployment_id,
                    route,
                )
                .ok()
            })
            .collect()
    }

    /// Refresh the worker's routing entry. `active_invocations` is `Some(v)` to
    /// set the count authoritatively (registration, ReportWorkerCapacity) or
    /// `None` to preserve the existing count (liveness-only heartbeats). Returns
    /// the count the entry ends up with so the caller can mirror it to the
    /// control plane.
    fn refresh_worker_session_route(
        &self,
        session: &WorkerSession,
        active_invocations: Option<u32>,
        capacity: Option<RouteCapacity>,
    ) -> RouteRefresh {
        {
            let _ = (session, capacity);
            RouteRefresh {
                active_invocations: active_invocations.unwrap_or(0),
                published: false,
            }
        }
    }

    fn worker_session_route_is_active(&self, session: &WorkerSession) -> bool {
        {
            let _ = session;
            false
        }
    }

    fn record_worker_session_heartbeat(&self, session: &WorkerSession, active_invocations: u32) {
        self.refresh_worker_session_route(session, Some(active_invocations), None);
    }

    fn record_worker_session_capacity(
        &self,
        session: &WorkerSession,
        request: &ReportWorkerCapacityRequest,
        desired_slots: u32,
        effective_max_slots: u32,
    ) {
        self.refresh_worker_session_route(
            session,
            Some(request.active_slots),
            Some(RouteCapacity {
                open_poll_slots: request.open_poll_slots,
                desired_slots,
                effective_max_slots,
                cpu_usage: request.cpu_usage,
                memory_usage: request.memory_usage,
                observed_at_ms: now_ms(),
            }),
        );
    }

    /// Refresh session liveness (route freshness, TTL, and control-plane
    /// heartbeat) WITHOUT overwriting the worker's active-invocation count. Used
    /// by the shared parked-mode poll/claim/renew/complete paths: with several
    /// ramped slots sharing one session, a single slot's per-poll view would
    /// otherwise zero (or under-count) the aggregate that ReportWorkerCapacity
    /// maintains. The preserved count is echoed to the control plane so its
    /// heartbeat TTL advances in step without corrupting the active count.
    fn refresh_worker_session_liveness(&self, session: &WorkerSession) {
        let refresh = self.refresh_worker_session_route(session, None, None);
        let _ = refresh;
    }

    /// Serve `list_runs` windows inside the CF_RUNS retention horizon from the
    /// hot tier alone. The margin absorbs clock skew between the node that
    /// stamped `submitted_at_ms` and the node running retention.
    pub fn with_hot_runs_retention_horizon(mut self, horizon_days: u32) -> Self {
        const SKEW_MARGIN_MS: i64 = 60 * 60 * 1000;
        let horizon_ms = i64::from(horizon_days) * 86_400_000;
        self.hot_runs_complete_window_ms =
            Some(horizon_ms - SKEW_MARGIN_MS).filter(|window| *window > 0);
        self
    }

    /// Set engine metrics for telemetry.
    pub fn with_metrics(mut self, metrics: crate::telemetry::EngineMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Install the partition-aware append backend used by `CompleteJob`.
    pub fn set_completion_append_backend(&self, backend: Arc<dyn EngineBackend>) {
        if let Ok(mut slot) = self.completion_append_backend.write() {
            *slot = Some(backend);
        }
    }

    fn completion_append_backend(&self) -> Option<Arc<dyn EngineBackend>> {
        self.completion_append_backend
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Install the shared replication-lag clock.
    /// Called from `run_engine` so the same clock the partition
    /// processors stamp on each record is what the service reads on
    /// `list_runs`.
    pub fn with_lag_clock(mut self, clock: engine_processor::lag_clock::LagClock) -> Self {
        self.lag_clock = clock;
        self
    }

    pub fn with_processor_health(
        mut self,
        health: tokio::sync::watch::Receiver<engine_processor::ProcessorHealthSnapshot>,
    ) -> Self {
        self.processor_health = Some(health);
        self
    }

    /// Restrict pull dequeue to the partition owner or its bounded HA fallback.
    pub fn with_pull_assignment_gate(mut self, gate: PullAssignmentGate) -> Self {
        self.pull_assignment_gate = Some(gate);
        self
    }

    fn pull_assignment_gate_decision(
        &self,
        run_id: &str,
        submitted_at_ms: i64,
        delegation: Option<&PullReadyDelegation>,
    ) -> PullAssignmentGateDecision {
        self.pull_assignment_gate
            .as_ref()
            .map_or(PullAssignmentGateDecision::EligibleStandalone, |gate| {
                gate(run_id, submitted_at_ms, delegation)
            })
    }

    async fn validate_entity_state_write_authority(
        &self,
        req: &PutEntityStateRequest,
    ) -> Result<(), Status> {
        if req.run_id.is_empty() {
            if !req.worker_id.is_empty()
                || !req.worker_session_id.is_empty()
                || !req.lease_id.is_empty()
                || req.attempt.is_some()
            {
                return Err(Status::invalid_argument(
                    "run_id is required when entity-state worker authority is supplied",
                ));
            }
            return Ok(());
        }

        let attempt = req.attempt.ok_or_else(|| {
            Status::invalid_argument(
                "worker_id, worker_session_id, lease_id, attempt and operation_id are required for run-scoped entity-state writes",
            )
        })?;
        if req.worker_id.is_empty()
            || req.worker_session_id.is_empty()
            || req.lease_id.is_empty()
            || req.operation_id.is_empty()
        {
            return Err(Status::invalid_argument(
                "worker_id, worker_session_id, lease_id, attempt and operation_id are required for run-scoped entity-state writes",
            ));
        }

        let session = self
            .worker_sessions
            .get_current_and_extend(&req.worker_id, &req.worker_session_id)
            .ok_or_else(|| Status::permission_denied("worker session is not active"))?;
        if session.project_id != req.project_id
            || session.worker_id != req.worker_id
            || (!session.registered_locally && !self.worker_session_route_is_active(&session))
        {
            return Err(Status::permission_denied(
                "worker session does not own this project",
            ));
        }

        let run = self
            .authoritative_completion_run(&req.project_id, &req.run_id, None)
            .await?;
        let session_hash = pull_workers::worker_session_hash(&req.worker_session_id);
        let projected_session_hash = run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
            .map(String::as_str)
            .unwrap_or_default();
        let projected_attempt = run
            .metadata
            .get(engine_core::lease_fence::RUN_META_LEASE_ATTEMPT)
            .and_then(|value| value.parse::<u32>().ok());
        let active = run.project_id == req.project_id
            && run.deployment_id == session.deployment_id
            && run.lease_id == req.lease_id
            && run.lease_worker_id == req.worker_id
            && run.attempt == attempt
            && projected_attempt == Some(attempt)
            && !session_hash.is_empty()
            && projected_session_hash == session_hash
            && matches!(run.status.as_str(), "assigned" | "started")
            && run.lease_expires_at_ms > now_ms();
        if !active {
            return Err(Status::failed_precondition(
                "entity-state write fence does not match the active job lease",
            ));
        }

        self.refresh_worker_session_liveness(&session);
        Ok(())
    }
}

/// Journal recovery is the correctness read. If it cannot produce a complete
/// aggregate, the runtime cannot safely distinguish execute from replay.
fn activation_recovery_error(activation_id: &str, error: EngineError) -> EngineError {
    match error {
        error @ EngineError::Activation(ActivationError::UnknownWriteOutcome { .. }) => error,
        error => EngineError::Activation(ActivationError::UnknownWriteOutcome {
            activation_id: activation_id.to_string(),
            reason: format!("journal-authoritative activation recovery failed: {error}"),
        }),
    }
}

/// Preserve errors that prove an activation write was rejected before its
/// correctness boundary. Every other append failure is conservatively
/// ambiguous: the storage/replication layer may have committed before the
/// acknowledgement was lost.
fn activation_append_error(activation_id: &str, error: EngineError) -> EngineError {
    match error {
        error @ (EngineError::Activation(_)
        | EngineError::InvalidArgument(_)
        | EngineError::Sealed
        | EngineError::UnsupportedStorageVersion { .. }
        | EngineError::StaleEpoch { .. }
        | EngineError::EpochAhead { .. }
        | EngineError::NotPartitionOwner { .. }
        | EngineError::PartitionNotWritable { .. }) => error,
        error => EngineError::Activation(ActivationError::UnknownWriteOutcome {
            activation_id: activation_id.to_string(),
            reason: error.to_string(),
        }),
    }
}

fn validate_invocation_lookup_record(
    request: &ReadByRunIdRequest,
    record: &Record,
    offset: Offset,
) -> Result<(), Status> {
    use engine_core::journal_contract::META_INVOCATION_IDEMPOTENCY_KEY;
    if record.project_id != request.project_id || record.run_id != request.run_id {
        return Err(Status::failed_precondition(
            "invocation identity does not match the requested run",
        ));
    }
    if record.offset != offset
        || !matches!(record.event_type.as_str(), "run.queued" | "batch.created")
        || record
            .metadata
            .get(META_INVOCATION_IDEMPOTENCY_KEY)
            .map(String::as_str)
            != Some(request.invocation_idempotency_key.as_str())
    {
        return Err(Status::data_loss(
            "invocation index disagrees with its journal record",
        ));
    }
    Ok(())
}

/// Convert an EngineError into a tonic Status.
fn engine_err_to_status(e: EngineError) -> Status {
    match e {
        EngineError::Sealed => Status::failed_precondition("segment sealed"),
        EngineError::OffsetOutOfRange(o) => {
            Status::out_of_range(format!("offset {o} out of range"))
        }
        error @ (EngineError::Trimmed { .. } | EngineError::Gap { .. }) => {
            Status::failed_precondition(error.to_string())
        }
        EngineError::InvalidArgument(msg) => Status::invalid_argument(msg),
        error @ EngineError::LeaseFenceMismatch { .. } => {
            Status::failed_precondition(error.to_string())
        }
        EngineError::InvocationConflict { run_id } => Status::already_exists(format!(
            "invocation idempotency key conflicts with the accepted run {run_id}"
        )),
        EngineError::Storage(msg) => Status::internal(format!("storage error: {msg}")),
        EngineError::Serialization(msg) => Status::internal(format!("serialization error: {msg}")),
        error @ EngineError::UnsupportedStorageVersion { .. } => {
            Status::failed_precondition(error.to_string())
        }
        EngineError::Cancelled => Status::cancelled("operation cancelled"),
        EngineError::Activation(error) => activation_error_to_status(error),
        EngineError::StaleEpoch { .. }
        | EngineError::EpochAhead { .. }
        | EngineError::NotPartitionOwner { .. }
        | EngineError::PartitionNotWritable { .. } => Status::unavailable(e.to_string()),
        error @ (EngineError::OverlapConflict(_)
        | EngineError::ColdJournalDivergence(_)
        | EngineError::OverlapMissing { .. }
        | EngineError::IntegrityFenced(_)) => Status::data_loss(error.to_string()),
        EngineError::Internal(msg) if is_transient_partition_write_error(&msg) => {
            Status::unavailable(msg)
        }
        EngineError::Internal(msg) => Status::internal(msg),
    }
}

fn poll_outcome_to_response(
    outcome: PollOutcome,
    slot_scaling: Option<engine_proto::SlotScalingHint>,
) -> Result<Response<PollJobResponse>, Status> {
    match outcome {
        PollOutcome::Assigned(job) => Ok(Response::new(PollJobResponse {
            job: Some(*job),
            slot_scaling,
        })),
        PollOutcome::DeadlineExpired => Ok(Response::new(PollJobResponse {
            job: None,
            slot_scaling,
        })),
        PollOutcome::Rejected(PollRejection::SessionInactive) => {
            Err(Status::permission_denied("worker session is not active"))
        }
        PollOutcome::Rejected(PollRejection::AuthorizationExpired) => {
            Err(Status::unauthenticated("workload token has expired"))
        }
        PollOutcome::Rejected(PollRejection::ShuttingDown) => {
            Err(Status::unavailable("pull dispatch is shutting down"))
        }
        PollOutcome::Rejected(PollRejection::ResourceExhausted { retry_after }) => {
            Err(Status::resource_exhausted(format!(
                "pull dispatch is saturated; retry after {} ms",
                retry_after.as_millis()
            )))
        }
        PollOutcome::Rejected(PollRejection::Failure(failure)) => Err(Status::new(
            poll_failure_code(failure.kind()),
            failure.message(),
        )),
    }
}

const fn poll_failure_code(kind: PollFailureKind) -> Code {
    match kind {
        PollFailureKind::Cancelled => Code::Cancelled,
        PollFailureKind::Unknown => Code::Unknown,
        PollFailureKind::InvalidArgument => Code::InvalidArgument,
        PollFailureKind::DeadlineExceeded => Code::DeadlineExceeded,
        PollFailureKind::NotFound => Code::NotFound,
        PollFailureKind::AlreadyExists => Code::AlreadyExists,
        PollFailureKind::PermissionDenied => Code::PermissionDenied,
        PollFailureKind::ResourceExhausted => Code::ResourceExhausted,
        PollFailureKind::FailedPrecondition => Code::FailedPrecondition,
        PollFailureKind::Aborted => Code::Aborted,
        PollFailureKind::OutOfRange => Code::OutOfRange,
        PollFailureKind::Unimplemented => Code::Unimplemented,
        PollFailureKind::Internal => Code::Internal,
        PollFailureKind::Unavailable => Code::Unavailable,
        PollFailureKind::DataLoss => Code::DataLoss,
        PollFailureKind::Unauthenticated => Code::Unauthenticated,
    }
}

// google.rpc.Status wire shape. Keeping this tiny local message avoids a
// production dependency while still emitting the standard rich-error
// envelope that grpc-go and other non-tonic clients require.
#[derive(Clone, PartialEq, Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<GoogleProtobufAny>,
}

#[derive(Clone, PartialEq, Message)]
struct GoogleProtobufAny {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

const ACTIVATION_ERROR_DETAIL_TYPE_URL: &str = "type.googleapis.com/api.v1.ActivationErrorDetail";

fn activation_error_to_status(error: ActivationError) -> Status {
    let message = error.to_string();
    let (code, grpc_code, activation_id, attempt) = match &error {
        ActivationError::NondeterministicReplay { activation_id, .. } => (
            ActivationErrorCode::NondeterministicReplay,
            Code::FailedPrecondition,
            activation_id.clone(),
            0,
        ),
        ActivationError::StaleAuthority {
            activation_id,
            request_attempt,
            ..
        } => (
            ActivationErrorCode::StaleAuthority,
            Code::FailedPrecondition,
            activation_id.clone(),
            *request_attempt,
        ),
        ActivationError::UnknownWriteOutcome { activation_id, .. } => (
            ActivationErrorCode::UnknownWriteOutcome,
            Code::Unavailable,
            activation_id.clone(),
            0,
        ),
        ActivationError::PayloadConflict {
            activation_id,
            attempt,
        } => (
            ActivationErrorCode::PayloadConflict,
            Code::AlreadyExists,
            activation_id.clone(),
            *attempt,
        ),
        ActivationError::StateVersionConflict {
            activation_id,
            attempt,
            ..
        } => (
            ActivationErrorCode::StateVersionConflict,
            Code::FailedPrecondition,
            activation_id.clone(),
            *attempt,
        ),
        ActivationError::RequiredChildUnresolved { activation_id, .. } => (
            ActivationErrorCode::RequiredChildUnresolved,
            Code::FailedPrecondition,
            activation_id.clone(),
            0,
        ),
        ActivationError::IllegalTransition { activation_id, .. } => (
            ActivationErrorCode::IllegalTransition,
            Code::FailedPrecondition,
            activation_id.clone(),
            0,
        ),
        ActivationError::InvalidArgument(_) => (
            ActivationErrorCode::InvalidArgument,
            Code::InvalidArgument,
            String::new(),
            0,
        ),
        ActivationError::InlinePayloadTooLarge { .. } => (
            ActivationErrorCode::InlinePayloadTooLarge,
            Code::OutOfRange,
            String::new(),
            0,
        ),
        ActivationError::InvalidReference(_) => (
            ActivationErrorCode::ReferenceRequired,
            Code::InvalidArgument,
            String::new(),
            0,
        ),
    };
    let detail = ActivationErrorDetail {
        code: code as i32,
        activation_id,
        attempt,
        message: message.clone(),
    };
    let details = GoogleRpcStatus {
        code: grpc_code as i32,
        message: message.clone(),
        details: vec![GoogleProtobufAny {
            type_url: ACTIVATION_ERROR_DETAIL_TYPE_URL.to_string(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(grpc_code, message, Bytes::from(details.encode_to_vec()))
}

fn begin_activation_response(
    decision: engine_core::activation::BeginDecision,
) -> BeginActivationResponse {
    let conflict = decision.conflict.map(|conflict| ActivationConflictReceipt {
        error_code: ActivationErrorCode::NondeterministicReplay as i32,
        expected_input_digest: conflict.expected_input_digest,
        provided_input_digest: conflict.provided_input_digest,
        expected_definition_digest: conflict.expected_definition_digest,
        provided_definition_digest: conflict.provided_definition_digest,
        message: conflict.reason,
    });
    let wait = (decision.outcome == BeginActivationOutcome::Wait).then(|| ActivationWaitReceipt {
        attempt: decision.attempt,
        active_worker_session_id: decision
            .active_worker_session_id
            .as_deref()
            .map(engine_core::lease_fence::worker_session_hash)
            .unwrap_or_default(),
        accepted_journal_offset: decision.accepted_offset.unwrap_or(Offset::ZERO).0,
    });
    let unknown_outcome = (decision.outcome == BeginActivationOutcome::UnknownOutcome).then(|| {
        let failure = decision.failure.as_ref();
        ActivationUnknownOutcomeReceipt {
            error_code: failure
                .map(|failure| failure.error_code.clone())
                .unwrap_or_else(|| "UNKNOWN_OUTCOME".to_string()),
            error_data: failure.and_then(|failure| failure.error_data.clone()),
            accepted_journal_offset: decision.accepted_offset.unwrap_or(Offset::ZERO).0,
        }
    });
    BeginActivationResponse {
        outcome: decision.outcome as i32,
        activation_id: decision.activation_id,
        attempt: decision.attempt,
        fence_token: decision.fence_token,
        accepted_journal_offset: decision.accepted_offset.unwrap_or(Offset::ZERO).0,
        replay_result: decision.replay_result,
        conflict,
        wait,
        unknown_outcome,
    }
}

fn legacy_activation_payload_bytes(payload: &engine_proto::ActivationPayload) -> Vec<u8> {
    match payload.value.as_ref() {
        Some(engine_proto::activation_payload::Value::InlineData(data)) => data.clone(),
        Some(engine_proto::activation_payload::Value::Reference(_)) => payload.encode_to_vec(),
        None => Vec::new(),
    }
}

fn is_transient_partition_write_error(message: &str) -> bool {
    let msg = message.to_ascii_lowercase();
    msg.contains("no sequencer available")
        || msg.contains("this node is not the sequencer")
        || msg.contains("stale epoch")
        || msg.contains("future epoch")
        || msg.contains("epoch ahead")
        || msg.contains("not partition owner")
        || msg.contains("partition not writable")
        || msg.contains("quorum not reached")
        || msg.contains("no connected peers for quorum replication")
        || msg.contains("not connected for catch-up")
        || msg.contains("catch-up failed")
}

// ---------------------------------------------------------------------------
// Hot ∪ cold list_runs merge
// ---------------------------------------------------------------------------

/// Decode the opaque cursor `submitted_at_ms:run_id` into its parts.
/// Opaque cursor shared by hot scans and query-lake pages; kept
/// here so the merge layer can pass it to both sides.
fn decode_list_cursor(cursor: &str) -> Option<(i64, String)> {
    let (ts_str, run_id) = cursor.split_once(':')?;
    let ts: i64 = ts_str.parse().ok()?;
    Some((ts, run_id.to_string()))
}

/// Encode a cursor for the next page from `(submitted_at_ms, run_id)`.
fn encode_list_cursor(submitted_at_ms: i64, run_id: &str) -> String {
    format!("{submitted_at_ms}:{run_id}")
}

/// Extract `submitted_at_ms` from a Run proto, defaulting to 0 when
/// the field is unset (treated as "very old" under newest-first sort).
fn run_submitted_ms(run: &engine_proto::Run) -> i64 {
    run.submitted_at
        .as_ref()
        .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
        .unwrap_or(0)
}

/// Result of the events hot-index scan.
struct EventListResult {
    items: Vec<engine_proto::EventSummary>,
    has_more: bool,
    next_cursor: String,
}

/// Scan the inbound-events hot index (`CF_EVENTS_HOT`) for a project,
/// newest-first, applying the post-scan filters (`source`, `event_name`,
/// `deployment_id`, time range) the key can't push down.
///
/// Like the runs hot side, a strict filter is paginated through the index
/// in pages of `SCAN_PAGE` until `limit` matches accumulate OR the budget
/// (`MAX_BUDGET`) caps the scan — at the cap, `has_more` is true with a
/// cursor at the last row considered so the next call resumes from there.
async fn list_events_hot<M: RunQueryStore + ?Sized>(
    store: Option<&Arc<M>>,
    req: &engine_proto::ListEventsRequest,
) -> Result<EventListResult, EngineError> {
    const SCAN_PAGE: usize = 200;
    const MAX_BUDGET: usize = 5_000;

    let limit = if req.limit > 0 {
        req.limit as usize
    } else {
        50
    };
    let Some(store) = store else {
        // Cold-only / single-node-without-store mode: events list is
        // unavailable. Empty (not an error) keeps the page rendering.
        return Ok(EventListResult {
            items: Vec::new(),
            has_more: false,
            next_cursor: String::new(),
        });
    };

    let mut scan_cursor = if req.cursor.is_empty() {
        // The events hot index is ordered newest-first. A bounded historical
        // query can seek directly to the upper edge of the requested window
        // instead of spending the scan budget on newer rows that cannot match.
        req.until_ms.map(|until| (until, String::new()))
    } else {
        decode_list_cursor(&req.cursor)
    };
    let mut items: Vec<engine_proto::EventSummary> = Vec::new();
    let mut scanned = 0usize;
    let mut last_considered: Option<(i64, String)> = None;
    let mut exhausted = false;
    let mut has_more = false;

    'outer: while items.len() <= limit && scanned < MAX_BUDGET {
        let page = RunQueryStore::scan_event_summaries(
            store.as_ref(),
            &req.project_id,
            scan_cursor.clone(),
            SCAN_PAGE,
        )
        .await?;
        let page_len = page.len();
        if page_len == 0 {
            exhausted = true;
            break;
        }
        for (event_run_id, timestamp_ms, summary_bytes) in page {
            scanned += 1;
            last_considered = Some((timestamp_ms, event_run_id.clone()));
            scan_cursor = Some((timestamp_ms, event_run_id.clone()));

            // Time-range filter pushes down onto the index timestamp.
            if let Some(since) = req.since_ms {
                if timestamp_ms < since {
                    continue;
                }
            }
            if let Some(until) = req.until_ms {
                if timestamp_ms > until {
                    continue;
                }
            }

            let summary: serde_json::Value = match serde_json::from_slice(&summary_bytes) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        event_run_id = %event_run_id,
                        "skipping undecodable events-hot summary"
                    );
                    continue;
                }
            };
            let s = |k: &str| summary.get(k).and_then(|v| v.as_str()).unwrap_or("");
            let source = s("source");
            let event_name = s("event_name");
            let deployment_id = s("deployment_id");
            if let Some(ref f) = req.source {
                if f != source {
                    continue;
                }
            }
            if let Some(ref f) = req.event_name {
                if f != event_name {
                    continue;
                }
            }
            if let Some(ref f) = req.deployment_id {
                if f != deployment_id {
                    continue;
                }
            }

            items.push(engine_proto::EventSummary {
                event_run_id,
                event_id: s("event_id").to_string(),
                event_name: event_name.to_string(),
                source: source.to_string(),
                deployment_id: deployment_id.to_string(),
                timestamp_ns: summary
                    .get("timestamp_ns")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(timestamp_ms * 1_000_000),
                matched_count: summary
                    .get("matched_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32,
                skipped_unsupported_count: summary
                    .get("skipped_unsupported_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32,
                status: s("status").to_string(),
                error_message: s("error_message").to_string(),
                dispatch_kind: s("dispatch_kind").to_string(),
                bound_component_name: s("bound_component_name").to_string(),
                bound_component_type: s("bound_component_type").to_string(),
            });
            // Over-fetch by one: a (limit+1)th match proves there's a
            // next page. Stop and trim below.
            if items.len() > limit {
                has_more = true;
                break 'outer;
            }
        }
        if page_len < SCAN_PAGE {
            exhausted = true;
            break;
        }
    }

    let next_cursor = if has_more {
        // Drop the proof-of-more extra row; resume after the last kept.
        items.truncate(limit);
        match items.last() {
            Some(last) => encode_list_cursor(last.timestamp_ns / 1_000_000, &last.event_run_id),
            None => String::new(),
        }
    } else if !exhausted && scanned >= MAX_BUDGET {
        // Budget exhausted under a strict filter: resume from the last
        // index row we looked at so progress isn't stuck.
        has_more = true;
        match last_considered {
            Some((ts, id)) => encode_list_cursor(ts, &id),
            None => String::new(),
        }
    } else {
        String::new()
    };

    Ok(EventListResult {
        items,
        has_more,
        next_cursor,
    })
}

/// Filter predicate applied to runs returned by the **hot** scan. The
/// cold scan applies the same filters after winner resolution, so cold runs already
/// match — we only need to filter the hot side post-fetch.
fn run_matches_filter(run: &engine_proto::Run, filter: &RunListFilter) -> bool {
    if !filter.statuses.is_empty() && !filter.statuses.contains(&run.status) {
        return false;
    }
    if let Some(ref dep) = filter.deployment_id {
        if &run.deployment_id != dep {
            return false;
        }
    }
    if !filter.deployment_ids.is_empty() && !filter.deployment_ids.contains(&run.deployment_id) {
        return false;
    }
    if let Some(ref name) = filter.component_name {
        if &run.component_name != name {
            return false;
        }
    }
    if !filter.component_names.is_empty() && !filter.component_names.contains(&run.component_name) {
        return false;
    }
    if let Some(ref ctype) = filter.component_type {
        if &run.component_type != ctype {
            return false;
        }
    }
    if filter.min_duration_ms.is_some() || filter.max_duration_ms.is_some() {
        // `duration_ms` is a plain int64, so a run still executing reads 0
        // and would otherwise match the fastest bucket. `completed_at` is the
        // field that says whether it finished.
        if run.completed_at.is_none()
            || !engine_core::run_filter::duration_in_window(
                run.duration_ms,
                filter.min_duration_ms,
                filter.max_duration_ms,
            )
        {
            return false;
        }
    }
    // Hot-tier counterpart to the cold-tier
    // JSON extracts on `metadata_json`. The Run proto carries
    // metadata as a real `map<string,string>`, so a direct lookup
    // is enough.
    if let Some(ref session) = filter.session_id {
        if run.metadata.get("session_id").map(|s| s.as_str()) != Some(session.as_str()) {
            return false;
        }
    }
    if let Some(ref parent) = filter.parent_run_id {
        if run.metadata.get("parent_run_id").map(|s| s.as_str()) != Some(parent.as_str()) {
            return false;
        }
    }
    if let Some(ref trigger) = filter.trigger_type {
        if run.metadata.get("trigger_type") != Some(trigger) {
            return false;
        }
    }
    if let Some(ref server) = filter.mcp_server {
        if run.metadata.get("mcp.server") != Some(server) {
            return false;
        }
    }
    let submitted_ms = run_submitted_ms(run);
    if let Some(since) = filter.since_ms {
        if submitted_ms < since {
            return false;
        }
    }
    if let Some(until) = filter.until_ms {
        if submitted_ms > until {
            return false;
        }
    }
    true
}

/// Whether either archive tier could hold a row for `filter` that the hot tier
/// does not.
///
/// Both tiers hold only finished runs, so a filter made only of active
/// statuses never needs them. A window that starts inside
/// `hot_complete_window_ms` doesn't either: retention has not removed any run
/// submitted inside it. Neither shortcut applies when the hot tier is not
/// scanned (no store, or a legacy request without a workspace).
fn archive_can_contribute(
    filter: &RunListFilter,
    has_hot_store: bool,
    now_ms: i64,
    hot_complete_window_ms: Option<i64>,
) -> bool {
    if !has_hot_store || filter.workspace_id.is_empty() {
        return true;
    }
    if !engine_core::run_filter::statuses_reach_archive(&filter.statuses) {
        return false;
    }
    match (hot_complete_window_ms, filter.since_ms) {
        (Some(window), Some(since_ms)) => since_ms < now_ms.saturating_sub(window),
        _ => true,
    }
}

/// The archive window a full hot page needs: from its oldest row (inclusive,
/// so a run submitted in the same millisecond still sorts by id), and never
/// wider than the request's own window. `None` when the hot side didn't fill
/// the page, which leaves the archive query as requested.
fn archive_since_for_full_hot_page(
    requested_since_ms: Option<i64>,
    hot_page: &[engine_proto::Run],
    limit: usize,
) -> Option<i64> {
    if hot_page.len() < limit {
        return None;
    }
    let oldest = hot_page.iter().map(run_submitted_ms).min()?;
    Some(requested_since_ms.map_or(oldest, |since| since.max(oldest)))
}

/// `list_runs_merged`, also skipping both archive tiers once the hot tier fills
/// the page with runs submitted at or after `hot_complete_since_ms`.
///
/// Every run submitted since then is still hot (see
/// `hot_runs_complete_window_ms`). An archived run could only enter this page
/// by sorting before its oldest row, which makes it newer than that row and so
/// inside the window: the hot scan, which reads newest-first, already
/// considered it. The page is therefore final, and `has_more` is set so the
/// next page still reaches older, archived runs.
async fn list_runs_merged_with_hot_window<M: RunQueryStore + ?Sized>(
    store: Option<&Arc<M>>,
    query_lake: Option<&Arc<ProfileQueryLake>>,
    filter: &RunListFilter,
    hot_complete_since_ms: Option<i64>,
) -> Result<RunListResult, Status> {
    use std::collections::HashSet;

    let limit = if filter.limit > 0 { filter.limit } else { 50 };
    let limit_usize = limit as usize;
    let cursor = filter.cursor.as_deref().and_then(decode_list_cursor);
    let started = Instant::now();
    let mut hot_scanned = 0usize;
    let query_lake_elapsed = Duration::ZERO;

    let mut all: Vec<engine_proto::Run> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    // Includes decoded hot winners even when they do not match the requested
    // status/component predicates. An older cold version must not reappear
    // merely because the current hot version was filtered out.
    let mut hot_winners: HashSet<String> = HashSet::new();
    let mut hot_has_more = false;
    let cold_resume_cursor: Option<(i64, String)> = None;
    let cold_has_more = false;
    // If the hot scan bails on its budget without matching anything,
    // we still need a cursor for the next call so progress isn't
    // stuck at the same point. This holds the (ts, run_id) of the
    // last index entry the hot scan looked at, regardless of whether
    // it matched the filter. Only consulted on budget exhaustion.
    let mut hot_budget_cursor: Option<(i64, String)> = None;

    // ── Hot side ────────────────────────────────────────────────────
    // Skipped when:
    //   - no run-query port wired (single-node/cold-only mode), or
    //   - workspace_id is empty (legacy auth fallback — the hot index
    //     key requires a workspace prefix to avoid cross-workspace
    //     leakage; the cold side has its own glob-based scoping).
    //
    // Filtering caveat: the hot index key only encodes `(workspace,
    // project, ms_inv, run_id)`. Filters on status / deployment /
    // component / time-range can't push down — we apply them after
    // `get_run`. To keep the page-size contract honest in the face of
    // a strict filter (e.g. status=running on a project where most
    // runs are completed), we paginate through the index in pages of
    // `HOT_SCAN_PAGE` until we have `limit_usize` matches OR the
    // index is exhausted. Cap the total scan budget at
    // `HOT_SCAN_MAX_BUDGET` so a degenerate filter (zero matches in a
    // huge index) doesn't pin a worker forever — at the cap we let
    // `has_more` reflect "more to scan, but the cursor is the last
    // one we considered" so the next call resumes from there.
    const HOT_SCAN_PAGE: usize = 200;
    const HOT_SCAN_MAX_BUDGET: usize = 5_000;

    if let Some(store) = store {
        if !filter.workspace_id.is_empty() {
            let mut scan_cursor = cursor.clone();
            let mut scanned: usize = 0;
            // The (ts, run_id) of the last index entry we examined —
            // not the last match. Used as the budget-bail cursor so a
            // strict filter that returns 0 matches still progresses.
            let mut last_considered: Option<(i64, String)> = None;
            // True iff we know the index has unscanned entries beyond
            // what we read. Set when we bail out of the inner loop
            // mid-page because the user's `limit` filled.
            let mut hot_more_to_scan = false;

            'outer: while all.len() < limit_usize && scanned < HOT_SCAN_MAX_BUDGET {
                let page = RunQueryStore::scan_hot_run_ids(
                    store.as_ref(),
                    &filter.workspace_id,
                    &filter.project_id,
                    scan_cursor.clone(),
                    HOT_SCAN_PAGE,
                )
                .await
                .map_err(engine_err_to_status)?;
                let page_len = page.len();
                if page_len == 0 {
                    break;
                }

                let mut consumed_this_page = 0usize;
                for run_id in page {
                    consumed_this_page += 1;
                    scanned += 1;
                    let bytes =
                        match RunQueryStore::load_run(store.as_ref(), &filter.project_id, &run_id)
                            .await
                            .map_err(engine_err_to_status)?
                        {
                            Some(b) => b,
                            None => continue, // index ahead of CF_RUNS — shouldn't happen
                        };
                    let run = match engine_proto::Run::decode(bytes.as_slice()) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                run_id = %run_id,
                                "skipping undecodable Run from CF_RUNS"
                            );
                            continue;
                        }
                    };
                    hot_winners.insert(run.run_id.clone());
                    last_considered = Some((run_submitted_ms(&run), run.run_id.clone()));
                    if !run_matches_filter(&run, filter) {
                        continue;
                    }
                    seen.insert(run.run_id.clone());
                    all.push(run);
                    if all.len() >= limit_usize {
                        // We filled the page mid-scan. If we didn't
                        // consume every entry in this index page, the
                        // remainder (and any pages after it) are
                        // unscanned — has_more applies. If we did
                        // consume the full page, we don't yet know
                        // whether there's a next page; the outer-
                        // loop check below decides.
                        if consumed_this_page < page_len {
                            hot_more_to_scan = true;
                        } else if page_len == HOT_SCAN_PAGE {
                            // Full page consumed, exactly at the user
                            // limit. Index might have a next page;
                            // the safe assumption is yes (we
                            // intentionally don't burn another scan
                            // call to confirm — user will discover
                            // empty-hot via cold side or the next
                            // call returns 0 rows).
                            hot_more_to_scan = true;
                        }
                        break 'outer;
                    }
                }

                // Index returned fewer rows than the page size → we've
                // exhausted the (workspace, project) slice; nothing
                // more in hot.
                if page_len < HOT_SCAN_PAGE {
                    break;
                }
                // Advance the scan cursor past the last considered
                // entry so the next iteration doesn't redo work.
                scan_cursor = last_considered.clone();
            }

            // hot_has_more reasoning:
            //   - Budget exhausted → publish `last_considered` as the
            //     resume cursor so progress isn't stuck. This matters
            //     when 0 of the budgeted entries matched the filter
            //     and `all.last()` is None / pointing at an early
            //     match that wouldn't unblock further scanning.
            //   - We bailed early because the user's page filled
            //     (`hot_more_to_scan = true`) → there's more to read
            //     on the next call.
            //   - Otherwise the index was exhausted within budget
            //     and there's nothing further on the hot side.
            if scanned >= HOT_SCAN_MAX_BUDGET {
                tracing::warn!(
                    workspace_id = %filter.workspace_id,
                    project_id = %filter.project_id,
                    scanned,
                    matched = all.len(),
                    limit = limit_usize,
                    "list_runs hot scan hit budget; reporting has_more=true so client can resume"
                );
                hot_has_more = true;
                hot_budget_cursor = last_considered;
            } else {
                hot_has_more = hot_more_to_scan;
            }
            hot_scanned = scanned;
        }
    }
    let hot_elapsed = started.elapsed();

    let page_complete_from_hot = hot_complete_since_ms.is_some_and(|since_ms| {
        all.len() >= limit_usize
            && all
                .iter()
                .map(run_submitted_ms)
                .min()
                .is_some_and(|oldest| oldest >= since_ms)
    });
    let query_lake = if page_complete_from_hot {
        hot_has_more = true;
        None
    } else {
        query_lake
    };
    // A full hot page also bounds what the archive can add: an archived run
    // belongs on this page only if it sorts before the page's oldest row, so
    // it was submitted no earlier than that row. Ask the archive for that
    // window alone. Older archived runs stay reachable through `has_more`.
    // On a project with many small archive files this is the difference
    // between reading the files of the whole requested window and a handful.
    let narrowed_filter;
    let archive_queried = false;
    let narrowed_since = archive_since_for_full_hot_page(filter.since_ms, &all, limit_usize)
        .filter(|_| archive_queried);
    let cold_filter = match narrowed_since {
        Some(since_ms) => {
            hot_has_more = true;
            narrowed_filter = RunListFilter {
                since_ms: Some(since_ms),
                ..filter.clone()
            };
            &narrowed_filter
        }
        None => filter,
    };

    let _ = (query_lake, cold_filter);

    // ── Merge ───────────────────────────────────────────────────────
    // Sort newest-first; stable tiebreak by run_id DESC matches both
    // sides' encoding (cold's `ORDER BY submitted_at_ms DESC, run_id
    // DESC`; hot's `(workspace, project, ms_inv, run_id)` key order
    // under forward iteration).
    all.sort_by(|a, b| {
        run_submitted_ms(b)
            .cmp(&run_submitted_ms(a))
            .then_with(|| b.run_id.cmp(&a.run_id))
    });
    let merge_truncated = all.len() > limit_usize;
    all.truncate(limit_usize);

    let has_more = hot_has_more || cold_has_more || merge_truncated;
    let elapsed = started.elapsed();
    if elapsed >= Duration::from_secs(1) {
        tracing::warn!(
            workspace_id = %filter.workspace_id,
            project_id = %filter.project_id,
            statuses = ?filter.statuses,
            since_ms = ?filter.since_ms,
            limit = limit_usize,
            hot_scanned,
            hot_ms = hot_elapsed.as_millis() as u64,
            query_lake_ms = query_lake_elapsed.as_millis() as u64,
            total_ms = elapsed.as_millis() as u64,
            "slow list_runs"
        );
    }
    let next_cursor = if has_more {
        if let Some(last) = all.last() {
            // Advancing only through a returned row cannot skip a row from
            // another tier that sorts between that row and a scan budget.
            encode_list_cursor(run_submitted_ms(last), &last.run_id)
        } else {
            // A strict filter may suppress every row in a source page. Keep
            // the newest consumed source boundary so the next request makes
            // progress without skipping an unconsumed row from another tier.
            let resume = [hot_budget_cursor, cold_resume_cursor]
                .into_iter()
                .flatten()
                .max();
            let Some((ts, run_id)) = resume else {
                return Err(Status::internal(
                    "list_runs source reported another page without an advancing cursor",
                ));
            };
            encode_list_cursor(ts, &run_id)
        }
    } else {
        String::new()
    };

    Ok(RunListResult {
        items: all,
        limit,
        offset: filter.offset,
        has_more,
        next_cursor,
        next_offset: None,
    })
}

#[tonic::async_trait]
impl<S: Segment> EngineService for EngineServiceImpl<S> {
    #[tracing::instrument(name = "engine.append", skip_all, fields(run_id, event_type))]
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let start = Instant::now();
        let external_identity = external_worker_identity(&request);
        let runtime_authored = runtime_authored_request(&request, self.accept_peer_runtime_mark);
        let req = request.into_inner();
        let proto_record = match req.record {
            Some(record) => record,
            None => {
                record_append_rpc_metrics(&self.metrics, "append", "failure", start, None);
                return Err(Status::invalid_argument("missing record"));
            }
        };

        let mut record = Record::from_proto(proto_record);
        sanitize_ingress_fence(&mut record, runtime_authored);
        self.authorize_external_run(
            external_identity.as_ref(),
            &record.project_id,
            &record.run_id,
        )
        .await?;
        self.validate_record_scope(&record)?;
        reject_external_internal_event(&record, runtime_authored)?;
        reject_observed_record(&record)?;
        // A lease-fenced record is runtime-authored: the segment decides it
        // against the lease entry in log order, so the projection-based
        // worker pre-checks below do not apply.
        let lease_fenced = engine_core::lease_fence::is_lease_fenced(&record);
        if !lease_fenced {
            self.reject_unfenced_pull_terminal(&record).await?;
        }
        if record
            .metadata
            .get(TRANSIENT_EVENT_METADATA_KEY)
            .is_some_and(|value| value == "true")
        {
            self.validate_worker_mutation_authority(&mut record).await?;
            let timestamp_ns = if record.timestamp_ns > 0 {
                record.timestamp_ns
            } else {
                now_unix_ns()
            };
            record.timestamp_ns = timestamp_ns;
            self.cache.publish_transient(Arc::new(record));
            return Ok(Response::new(AppendResponse {
                offset: 0,
                timestamp_ns,
            }));
        }
        tracing::Span::current().record("run_id", tracing::field::display(&record.run_id));
        tracing::Span::current().record("event_type", tracing::field::display(&record.event_type));
        debug!(
            project_id = %record.project_id,
            run_id = %record.run_id,
            event_type = %record.event_type,
            "Append: single record"
        );

        let now_ns = now_unix_ns();
        if let Some((offset, timestamp_ns, terminal_status)) = if lease_fenced {
            None
        } else {
            self.existing_terminal_for_late_event(&record).await
        } {
            debug!(
                project_id = %record.project_id,
                run_id = %record.run_id,
                event_type = %record.event_type,
                terminal_status = %terminal_status,
                terminal_offset = offset.0,
                "Append: dropping late lifecycle event after terminal run"
            );
            record_append_rpc_metrics(&self.metrics, "append", "success", start, Some(0));
            return Ok(Response::new(AppendResponse {
                offset: offset.0,
                timestamp_ns: if timestamp_ns > 0 {
                    timestamp_ns
                } else {
                    now_ns
                },
            }));
        }
        let successful_run_completion = requests_successful_run_completion(&record);
        let run_cancellation = requests_run_cancellation(&record);
        let run_lease_expiry = requests_run_lease_expiry(&record, runtime_authored);
        if !lease_fenced {
            self.validate_worker_mutation_authority(&mut record).await?;
        }
        if successful_run_completion {
            self.reject_unsettled_required_children(&record.project_id, &record.run_id, None)
                .await?;
        }

        let cancellation_records = if run_cancellation {
            self.activation_cancellation_records(
                &record.project_id,
                &record.run_id,
                run_cancellation_reason(&record),
            )
            .await?
        } else if run_lease_expiry {
            // Settled in the same batch as the expiry, so a stale expiry that
            // loses its lease fence writes neither.
            self.activation_lease_loss_records(&record.project_id, &record.run_id)
                .await?
        } else {
            Vec::new()
        };
        let mut records = cancellation_records;
        records.extend(self.apply_guardrails(record).await);
        if records.is_empty() {
            record_append_rpc_metrics(&self.metrics, "append", "failure", start, None);
            return Err(Status::internal(
                "guardrail expansion returned no record for Append input",
            ));
        }
        for expanded in &records {
            if engine_core::activation::has_activation_marker(expanded) {
                // Runtime-authored cancellation receipts, screened by the fold.
                continue;
            }
            reject_external_internal_event(expanded, runtime_authored)?;
            if !engine_core::lease_fence::is_lease_fenced(expanded) {
                self.reject_unfenced_pull_terminal(expanded).await?;
            }
        }
        let response_index = records.len().saturating_sub(1);

        // Stamp `timestamp_ns` BEFORE the segment write so the projection
        // (which reads `record.timestamp_ns` to populate `Run.submitted_at`)
        // sees the assigned value. Stamping after the append-batch call
        // would only fix the cache copy, not the durable segment record
        // — leaving every projected `Run.submitted_at` at epoch 0 and
        // breaking any `since_ms`-style filter against the runs list.
        assign_missing_batch_timestamps(&mut records, now_ns);

        let contains_queued_run = records
            .iter()
            .any(|record| record.event_type == "run.queued");
        let journal_append_started_at = Instant::now();
        let outcome = match self.segment.append_batch_with_outcome(&records).await {
            Ok(outcome) => outcome,
            Err(err) => {
                record_pull_queue_commit(
                    &self.metrics,
                    "append",
                    "error",
                    journal_append_started_at,
                    contains_queued_run,
                );
                record_append_rpc_metrics(&self.metrics, "append", "failure", start, None);
                return Err(engine_err_to_status(err));
            }
        };
        outcome
            .validate_cardinality(records.len(), "engine append")
            .map_err(engine_err_to_status)?;
        record_pull_queue_commit(
            &self.metrics,
            "append",
            "success",
            journal_append_started_at,
            contains_queued_run,
        );
        let queue_commit_ms = journal_append_started_at.elapsed().as_secs_f64() * 1_000.0;
        for (record, appended) in records.iter().zip(outcome.appended.iter()) {
            if record.event_type == "run.queued" {
                tracing::debug!(
                    target: "agnt5.pull_queue_stage",
                    stage = "queue_committed",
                    run_id = %record.run_id,
                    queued_at_ms = record.timestamp_ns / 1_000_000,
                    queue_commit_ms,
                    appended,
                    "PULL queue stage"
                );
            }
        }
        let offset = outcome.offsets[response_index];

        // Write-through to cache.
        let mut response_timestamp_ns = now_ns;
        for ((mut record, &offset), appended) in records
            .into_iter()
            .zip(outcome.offsets.iter())
            .zip(outcome.appended.iter())
        {
            record.offset = offset;
            if record.offset == outcome.offsets[response_index] {
                response_timestamp_ns = record.timestamp_ns;
            }
            if *appended {
                let cached = Arc::new(record);
                self.cache.put(cached.clone());
                RecordCache::mark_terminal_if_complete(&self.cache, cached.as_ref());
                self.notify_if_queue_affecting_record(cached.as_ref());
                self.observe_queue_metrics_for_record(cached.as_ref());
            }
        }

        record_append_rpc_metrics(
            &self.metrics,
            "append",
            "success",
            start,
            Some(outcome.appended_count() as u64),
        );

        Ok(Response::new(AppendResponse {
            offset: offset.0,
            timestamp_ns: response_timestamp_ns,
        }))
    }

    #[tracing::instrument(name = "engine.append_batch", skip_all, fields(batch_size))]
    async fn append_batch(
        &self,
        request: Request<AppendBatchRequest>,
    ) -> Result<Response<AppendBatchResponse>, Status> {
        let start = Instant::now();
        let external_identity = external_worker_identity(&request);
        let runtime_authored = runtime_authored_request(&request, self.accept_peer_runtime_mark);
        let req = request.into_inner();
        if req.records.is_empty() {
            record_append_rpc_metrics(&self.metrics, "append_batch", "success", start, Some(0));
            return Ok(Response::new(AppendBatchResponse {
                offsets: vec![],
                written_count: 0,
            }));
        }

        let now_ns = now_unix_ns();
        let input_records: Vec<Record> = req
            .records
            .into_iter()
            .map(|proto| {
                let mut record = Record::from_proto(proto);
                sanitize_ingress_fence(&mut record, runtime_authored);
                record
            })
            .collect();
        for record in &input_records {
            self.authorize_external_run(
                external_identity.as_ref(),
                &record.project_id,
                &record.run_id,
            )
            .await?;
            self.validate_record_scope(record)?;
            reject_external_internal_event(record, runtime_authored)?;
            reject_observed_record(record)?;
        }
        if let Err(error) = self.segment.validate_append_batch(&input_records) {
            record_append_rpc_metrics(&self.metrics, "append_batch", "failure", start, None);
            return Err(engine_err_to_status(error));
        }
        for record in &input_records {
            if !engine_core::lease_fence::is_lease_fenced(record) {
                self.reject_unfenced_pull_terminal(record).await?;
            }
        }
        let mut records: Vec<Record> = Vec::with_capacity(input_records.len());
        let mut response_slots = Vec::with_capacity(input_records.len());
        let mut successful_completion_runs = std::collections::HashSet::new();
        let mut cancelled_runs = std::collections::HashSet::new();
        for record in input_records {
            if let Some((offset, _timestamp_ns, terminal_status)) =
                if engine_core::lease_fence::is_lease_fenced(&record) {
                    None
                } else {
                    self.existing_terminal_for_late_event(&record).await
                }
            {
                debug!(
                    project_id = %record.project_id,
                    run_id = %record.run_id,
                    event_type = %record.event_type,
                    terminal_status = %terminal_status,
                    terminal_offset = offset.0,
                    "AppendBatch: dropping late lifecycle event after terminal run"
                );
                response_slots.push(AppendBatchResponseSlot::Existing(offset));
                continue;
            }

            if requests_successful_run_completion(&record) {
                successful_completion_runs
                    .insert((record.project_id.clone(), record.run_id.clone()));
            }

            if requests_run_cancellation(&record)
                && cancelled_runs.insert((record.project_id.clone(), record.run_id.clone()))
            {
                records.extend(
                    self.activation_cancellation_records(
                        &record.project_id,
                        &record.run_id,
                        run_cancellation_reason(&record),
                    )
                    .await?,
                );
            }

            let expanded = self.apply_guardrails(record).await;
            if expanded.is_empty() {
                record_append_rpc_metrics(&self.metrics, "append_batch", "failure", start, None);
                return Err(Status::internal(
                    "guardrail expansion returned no record for AppendBatch input",
                ));
            }
            records.extend(expanded);
            response_slots.push(AppendBatchResponseSlot::AppendedIndex(
                records.len().saturating_sub(1),
            ));
        }
        tracing::Span::current().record("batch_size", records.len());
        for expanded in &mut records {
            // Runtime-authored expansions (activation cancellation receipts)
            // carry the activation marker; every caller-supplied record was
            // already screened above.
            if engine_core::activation::has_activation_marker(expanded) {
                continue;
            }
            reject_external_internal_event(expanded, runtime_authored)?;
            if engine_core::lease_fence::is_lease_fenced(expanded) {
                continue;
            }
            self.reject_unfenced_pull_terminal(expanded).await?;
            self.validate_worker_mutation_authority(expanded).await?;
        }
        for (project_id, run_id) in successful_completion_runs {
            self.reject_unsettled_required_children(&project_id, &run_id, None)
                .await?;
        }
        for r in &records {
            debug!(
                project_id = %r.project_id,
                run_id = %r.run_id,
                event_type = %r.event_type,
                "AppendBatch: record"
            );
        }

        // Stamp `timestamp_ns` BEFORE the segment write so the projection
        // (which reads `record.timestamp_ns` to populate `Run.submitted_at`)
        // sees the assigned value. See the matching comment in `append`.
        assign_missing_batch_timestamps(&mut records, now_ns);

        if records.is_empty() {
            record_append_rpc_metrics(&self.metrics, "append_batch", "success", start, Some(0));
            return Ok(Response::new(AppendBatchResponse {
                offsets: response_slots
                    .iter()
                    .filter_map(|slot| match slot {
                        AppendBatchResponseSlot::Existing(offset) => Some(offset.0),
                        AppendBatchResponseSlot::AppendedIndex(_) => None,
                    })
                    .collect(),
                written_count: 0,
            }));
        }

        let contains_queued_run = records
            .iter()
            .any(|record| record.event_type == "run.queued");
        let journal_append_started_at = Instant::now();
        let outcome = match self.segment.append_batch_with_outcome(&records).await {
            Ok(outcome) => outcome,
            Err(err) => {
                record_pull_queue_commit(
                    &self.metrics,
                    "append_batch",
                    "error",
                    journal_append_started_at,
                    contains_queued_run,
                );
                record_append_rpc_metrics(&self.metrics, "append_batch", "failure", start, None);
                return Err(engine_err_to_status(err));
            }
        };
        outcome
            .validate_cardinality(records.len(), "engine append batch")
            .map_err(engine_err_to_status)?;
        record_pull_queue_commit(
            &self.metrics,
            "append_batch",
            "success",
            journal_append_started_at,
            contains_queued_run,
        );
        let queue_commit_ms = journal_append_started_at.elapsed().as_secs_f64() * 1_000.0;
        for (record, appended) in records.iter().zip(outcome.appended.iter()) {
            if record.event_type == "run.queued" {
                tracing::debug!(
                    target: "agnt5.pull_queue_stage",
                    stage = "queue_committed",
                    run_id = %record.run_id,
                    queued_at_ms = record.timestamp_ns / 1_000_000,
                    queue_commit_ms,
                    appended,
                    "PULL queue stage"
                );
            }
        }

        // Write-through to cache.
        for ((mut record, &offset), appended) in records
            .into_iter()
            .zip(outcome.offsets.iter())
            .zip(outcome.appended.iter())
        {
            record.offset = offset;
            if *appended {
                let cached = Arc::new(record);
                self.cache.put(cached.clone());
                RecordCache::mark_terminal_if_complete(&self.cache, cached.as_ref());
                self.notify_if_queue_affecting_record(cached.as_ref());
                self.observe_queue_metrics_for_record(cached.as_ref());
            }
        }

        // The response is input-shaped even when guardrails expand one input
        // into multiple journal records. Report how many input records were
        // newly accepted; retain the physical append count for telemetry.
        let written_count = response_slots
            .iter()
            .filter(|slot| match slot {
                AppendBatchResponseSlot::Existing(_) => false,
                AppendBatchResponseSlot::AppendedIndex(idx) => outcome.appended[*idx],
            })
            .count() as i32;
        let appended_record_count = outcome.appended_count() as u64;

        record_append_rpc_metrics(
            &self.metrics,
            "append_batch",
            "success",
            start,
            Some(appended_record_count),
        );

        Ok(Response::new(AppendBatchResponse {
            offsets: response_slots
                .iter()
                .map(|slot| match slot {
                    AppendBatchResponseSlot::Existing(offset) => offset.0,
                    AppendBatchResponseSlot::AppendedIndex(idx) => outcome.offsets[*idx].0,
                })
                .collect(),
            written_count,
        }))
    }

    #[tracing::instrument(name = "engine.append_observed", skip_all, fields(batch_size))]
    async fn append_observed(
        &self,
        request: Request<AppendObservedRequest>,
    ) -> Result<Response<AppendObservedResponse>, Status> {
        let start = Instant::now();
        // Only the runtime's own ingest path writes observed runs; a worker
        // holding a session must not reach a path with no lease check.
        if !runtime_authored_request(&request, self.accept_peer_runtime_mark) {
            record_append_rpc_metrics(&self.metrics, "append_observed", "failure", start, None);
            return Err(Status::permission_denied(
                "AppendObserved is only for the runtime's ingest path",
            ));
        }
        let records: Vec<Record> = request
            .into_inner()
            .records
            .into_iter()
            .map(Record::from_proto)
            .collect();
        tracing::Span::current().record("batch_size", records.len());
        let Some(first) = records.first() else {
            return Ok(Response::new(AppendObservedResponse::default()));
        };
        validate_observed_batch(&records)?;
        for record in &records {
            self.validate_record_scope(record)?;
        }
        let run_exists = self.check_observed_run_target(first).await?;
        if !run_exists && first.event_type != "run.queued" {
            // The gateway sends a trace's spans without opening a run when
            // none of them is AI; they join the run only if one is open.
            record_append_rpc_metrics(&self.metrics, "append_observed", "success", start, Some(0));
            return Ok(Response::new(AppendObservedResponse {
                offsets: vec![0; records.len()],
                outcomes: vec![ObservedRecordOutcome::NoRun as i32; records.len()],
            }));
        }
        if let Err(error) = self.segment.validate_append_batch(&records) {
            record_append_rpc_metrics(&self.metrics, "append_observed", "failure", start, None);
            return Err(engine_err_to_status(error));
        }

        // A span that arrives after its run ended is dropped, the same rule
        // the worker paths apply to late lifecycle events. A record written
        // before (the run's opening records from a later request, or any
        // span an exporter resends) takes its existing slot in the journal,
        // in the same write, and counts as committed.
        let mut outcomes = Vec::with_capacity(records.len());
        let mut appends = Vec::with_capacity(records.len());
        let mut to_write = Vec::with_capacity(records.len());
        for record in records {
            if self
                .existing_terminal_for_late_event(&record)
                .await
                .is_some()
            {
                outcomes.push(ObservedRecordOutcome::RunEnded);
                appends.push(false);
            } else {
                outcomes.push(ObservedRecordOutcome::Written);
                appends.push(true);
                to_write.push(record);
            }
        }
        if to_write.is_empty() {
            record_append_rpc_metrics(&self.metrics, "append_observed", "success", start, Some(0));
            return Ok(Response::new(AppendObservedResponse {
                offsets: vec![0; outcomes.len()],
                outcomes: outcomes.into_iter().map(i32::from).collect(),
            }));
        }

        assign_missing_batch_timestamps(&mut to_write, now_unix_ns());
        let outcome = match self.segment.append_batch_with_outcome(&to_write).await {
            Ok(outcome) => outcome,
            Err(err) => {
                record_append_rpc_metrics(&self.metrics, "append_observed", "failure", start, None);
                return Err(engine_err_to_status(err));
            }
        };
        outcome
            .validate_cardinality(to_write.len(), "engine append observed")
            .map_err(engine_err_to_status)?;

        // Write-through to cache, as AppendBatch does.
        for ((mut record, &offset), appended) in to_write
            .into_iter()
            .zip(outcome.offsets.iter())
            .zip(outcome.appended.iter())
        {
            record.offset = offset;
            if *appended {
                let cached = Arc::new(record);
                self.cache.put(cached.clone());
                RecordCache::mark_terminal_if_complete(&self.cache, cached.as_ref());
                self.notify_if_queue_affecting_record(cached.as_ref());
                self.observe_queue_metrics_for_record(cached.as_ref());
            }
        }

        let mut written = outcome.offsets.iter();
        let offsets = appends
            .iter()
            .map(|appended| {
                if *appended {
                    written.next().map_or(0, |offset| offset.0)
                } else {
                    0
                }
            })
            .collect();
        record_append_rpc_metrics(
            &self.metrics,
            "append_observed",
            "success",
            start,
            Some(outcome.appended_count() as u64),
        );
        Ok(Response::new(AppendObservedResponse {
            outcomes: outcomes.into_iter().map(i32::from).collect(),
            offsets,
        }))
    }

    async fn read_by_run_id(
        &self,
        request: Request<ReadByRunIdRequest>,
    ) -> Result<Response<ReadByRunIdResponse>, Status> {
        use std::collections::BTreeMap;

        let req = request.into_inner();
        let partition_count = self.segment.partition_count();
        let run_partition = partition_for_run(&req.run_id, partition_count);

        let mut expected_invocation_offset = None;
        if !req.invocation_idempotency_key.is_empty() {
            use engine_core::traits::InvocationLookup;
            if req.project_id.is_empty()
                || req.run_id.is_empty()
                || req.from_timestamp_ns != 0
                || req.event_types.len() != 2
                || !req.event_types.iter().any(|kind| kind == "run.queued")
                || !req.event_types.iter().any(|kind| kind == "batch.created")
            {
                return Err(Status::invalid_argument("invocation lookup requires a project/run and unfiltered invocation event types"));
            }
            match self
                .segment
                .lookup_invocation(
                    &req.project_id,
                    &req.run_id,
                    &req.invocation_idempotency_key,
                )
                .await
                .map_err(engine_err_to_status)?
            {
                InvocationLookup::Missing => {
                    return Ok(Response::new(ReadByRunIdResponse { records: vec![] }))
                }
                InvocationLookup::Pending => {
                    return Err(Status::unavailable(
                        "invocation is not yet committed on this replica",
                    ))
                }
                InvocationLookup::Unsupported => {}
                InvocationLookup::Found(offset) => {
                    expected_invocation_offset = Some(offset);
                    let end = offset
                        .0
                        .checked_add(1)
                        .ok_or_else(|| Status::data_loss("invocation offset overflow"))?;
                    match self.segment.read_range(offset, Offset(end)).await {
                        Ok(records) => {
                            let [record] = records.as_slice() else {
                                return Err(Status::data_loss(
                                    "invocation index points to a missing journal record",
                                ));
                            };
                            validate_invocation_lookup_record(&req, record, offset)?;
                            return Ok(Response::new(ReadByRunIdResponse {
                                records: vec![record.to_proto()],
                            }));
                        }
                        // Adapters that do not compose hot and cold journal
                        // ranges retain the existing complete-history reader.
                        Err(EngineError::Trimmed { .. }) => {}
                        Err(error) => return Err(engine_err_to_status(error)),
                    }
                }
            }
        }

        // Always read the durable history from CF_EVENTS. The cache is a
        // tail accelerator, not a complete history — gap B3 was that a
        // partial cache (cache only had a suffix of events because the
        // bounded cache evicted earlier ones) used to short-circuit
        // ReadByRunID, hiding everything in the durable store. We now
        // read both, merge by `offset`, and let the cache supersede on
        // overlap (cache rows have the post-append-stamp `timestamp_ns`
        // that earlier-projected store rows might be missing).
        let store_records = if let Some(ref store) = self.store {
            RunQueryStore::load_run_events(
                store.as_ref(),
                &req.project_id,
                &req.run_id,
                req.from_timestamp_ns,
            )
            .await
            .map_err(engine_err_to_status)?
        } else {
            Vec::new()
        };

        let cached_opt = self.cache.get_by_run_id(&req.project_id, &req.run_id);
        let cache_hit = cached_opt.is_some();
        if let Some(ref m) = self.metrics {
            m.cache_lookups
                .add(1, &[crate::telemetry::attrs::cache_result(cache_hit)]);
        }

        // Merge hot tiers by public offset. Processor projection writes
        // router-encoded offsets to both CF_EVENTS and cache. Cold Parquet is
        // partition-local and is encoded separately at the cold boundary.
        //
        // Cache only fills offsets the store has not seen yet. If both hot
        // tiers contain one journal identity, their canonical records must be
        // identical; silently choosing either observation would hide storage
        // corruption before the L0/L1 merge can enforce the same invariant.
        //
        // `from_timestamp_ns` is applied ONCE after the merge so
        // precedence isn't accidentally tied to filter order. A
        // record with timestamp < watermark is dropped regardless of
        // which tier it came from.
        let mut merged: BTreeMap<u64, engine_core::types::Record> = BTreeMap::new();
        for mut r in store_records {
            r.offset = validate_public_run_offset(r.offset, run_partition, partition_count)
                .map_err(engine_err_to_status)?;
            merged.insert(r.offset.0, r);
        }
        if let Some(cached) = cached_opt {
            for r in cached.iter() {
                let mut r = (**r).clone();
                r.offset = validate_public_run_offset(r.offset, run_partition, partition_count)
                    .map_err(engine_err_to_status)?;
                match merged.entry(r.offset.0) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(r);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if !entry.get().canonical_differing_fields(&r).is_empty() =>
                    {
                        return Err(Status::data_loss(format!(
                            "hot run-event tiers disagree at journal offset {}",
                            r.offset.0
                        )));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
            }
        }

        if let Some(offset) = expected_invocation_offset {
            let record = merged.get(&offset.0).ok_or_else(|| {
                Status::unavailable("accepted invocation is absent from retained history")
            })?;
            validate_invocation_lookup_record(&req, record, offset)?;
            return Ok(Response::new(ReadByRunIdResponse {
                records: vec![record.to_proto()],
            }));
        }

        // Single from_timestamp_ns filter applied to the merged map.
        // Doing this once at the end (instead of three times during
        // the merge) keeps precedence unambiguous: the comparison
        // happens against whatever timestamp the *winning* tier
        // wrote, not the first one to land.
        if req.from_timestamp_ns > 0 {
            merged.retain(|_, r| r.timestamp_ns >= req.from_timestamp_ns);
        }

        let records: Vec<engine_proto::Record> = merged
            .into_values()
            .filter(|record| !engine_core::journal_contract::is_runtime_internal_event(record))
            .filter(|r| req.event_types.is_empty() || req.event_types.contains(&r.event_type))
            .map(|r| r.to_proto())
            .collect();

        Ok(Response::new(ReadByRunIdResponse { records }))
    }

    async fn find_by_step_key(
        &self,
        request: Request<FindByStepKeyRequest>,
    ) -> Result<Response<FindByStepKeyResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        self.authorize_external_run(external_identity.as_ref(), &req.project_id, &req.run_id)
            .await?;

        if let Some(record) = self
            .cache
            .get_step_memo(&req.project_id, &req.run_id, &req.step_key)
        {
            return Ok(Response::new(FindByStepKeyResponse {
                record: Some(record.to_proto()),
                found: true,
            }));
        }

        // Cache miss — fall back to materialized store.
        if let Some(ref store) = self.store {
            if let Some(data) = RunQueryStore::load_step_memo(
                store.as_ref(),
                &req.project_id,
                &req.run_id,
                &req.step_key,
            )
            .await
            .map_err(engine_err_to_status)?
            {
                // Reconstruct a minimal Record from the memoized data
                let record = engine_proto::Record {
                    project_id: req.project_id,
                    run_id: req.run_id,
                    step_key: req.step_key,
                    data,
                    data_type: "json".to_string(),
                    ..Default::default()
                };
                return Ok(Response::new(FindByStepKeyResponse {
                    record: Some(record),
                    found: true,
                }));
            }
        }

        Ok(Response::new(FindByStepKeyResponse {
            record: None,
            found: false,
        }))
    }

    async fn get_run(
        &self,
        request: Request<GetRunRequest>,
    ) -> Result<Response<GetRunResponse>, Status> {
        let req = request.into_inner();

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;

        // Hot tier: CF_RUNS holds the canonical Run proto for any run
        // still within RocksDB retention.
        if let Some(data) = RunQueryStore::load_run(store.as_ref(), &req.project_id, &req.run_id)
            .await
            .map_err(engine_err_to_status)?
        {
            let run = prost::Message::decode(data.as_slice())
                .map_err(|e| Status::internal(format!("decode run: {e}")))?;
            return Ok(Response::new(GetRunResponse {
                run: Some(run),
                found: true,
            }));
        }

        Ok(Response::new(GetRunResponse {
            run: None,
            found: false,
        }))
    }

    async fn list_routing_workers(
        &self,
        _request: Request<ListRoutingWorkersRequest>,
    ) -> Result<Response<ListRoutingWorkersResponse>, Status> {
        // The journal-fed routing projection is retired;
        // gossip is the routing source of record. The gRPC method
        // remains on the proto contract for backward compat, but
        // returns an empty list — callers should migrate to reading
        // from the gossip routing map (see /v1/cluster/gossip).
        Ok(Response::new(ListRoutingWorkersResponse {
            workers: Vec::new(),
        }))
    }

    // --- Durable execution (worker write path) ---

    #[tracing::instrument(
        name = "engine.begin_activation",
        skip_all,
        fields(project_id, run_id, activation_id, outcome)
    )]
    async fn begin_activation(
        &self,
        request: Request<BeginActivationRequest>,
    ) -> Result<Response<BeginActivationResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        let authorize_started_at = Instant::now();
        let authorization = self
            .authorize_external_run(external_identity.as_ref(), &req.project_id, &req.run_id)
            .await;
        record_activation_phase(
            &self.metrics,
            "begin",
            "authorize_run",
            if authorization.is_ok() { "ok" } else { "error" },
            authorize_started_at,
        );
        authorization?;
        self.validate_project_scope(&req.project_id)?;
        let activation_id = activation::activation_id(
            &req.project_id,
            &req.run_id,
            &req.parent_activation_id,
            req.kind,
            &req.stable_key,
        );
        tracing::Span::current().record("project_id", tracing::field::display(&req.project_id));
        tracing::Span::current().record("run_id", tracing::field::display(&req.run_id));
        tracing::Span::current().record("activation_id", tracing::field::display(&activation_id));

        let activation_load_started_at = Instant::now();
        let current_result = self
            .load_activation_state(&req.project_id, &req.run_id, &activation_id)
            .await;
        record_activation_phase(
            &self.metrics,
            "begin",
            "load_activation_state",
            match &current_result {
                Ok(Some(_)) => "found",
                Ok(None) => "missing",
                Err(_) => "error",
            },
            activation_load_started_at,
        );
        let current = current_result.map_err(|error| {
            engine_err_to_status(activation_recovery_error(&activation_id, error))
        })?;
        let issued_attempt = current
            .as_ref()
            .filter(|state| state.status == engine_proto::ActivationStatus::RetryReady)
            .map(|state| state.current_attempt.attempt.saturating_add(1))
            .unwrap_or_else(|| {
                current
                    .as_ref()
                    .map(|state| state.current_attempt.attempt)
                    .unwrap_or(1)
            });
        let signing_key = worker_session_signing_key();
        let fence_token = activation::mint_fence_token(
            signing_key.as_bytes(),
            &activation_id,
            issued_attempt,
            &req.worker_session_id,
            &req.run_authority,
            &req.lease_authority,
        )
        .map_err(activation_error_to_status)?;
        let command = BeginActivationCommand {
            request: req,
            issued_fence_token: fence_token,
        };
        let transition = activation::decide_begin(current.as_ref(), &command)
            .map_err(activation_error_to_status)?;

        let validated_run = if !transition.records.is_empty() {
            let authority_started_at = Instant::now();
            let authority = self
                .validate_activation_run_authority(
                    &command.request.project_id,
                    &command.request.run_id,
                    &activation_id,
                    issued_attempt,
                    &command.request.worker_session_id,
                    &command.request.run_authority,
                    &command.request.lease_authority,
                )
                .await;
            record_activation_phase(
                &self.metrics,
                "begin",
                "validate_run_authority",
                if authority.is_ok() { "ok" } else { "error" },
                authority_started_at,
            );
            authority?
        } else {
            None
        };

        let decision = if let Some(canonical) = transition.records.first() {
            let append_started_at = Instant::now();
            let append_result = self
                .append_activation_record(current, canonical, validated_run.as_ref())
                .await;
            record_activation_phase(
                &self.metrics,
                "begin",
                "append",
                match &append_result {
                    Ok((_, _, true)) => "appended",
                    Ok((_, _, false)) => "replayed",
                    Err(_) => "error",
                },
                append_started_at,
            );
            let (accepted_state, offset, appended) = append_result.map_err(engine_err_to_status)?;
            if appended {
                let ActivationDecision::Begin(mut decision) = transition.decision else {
                    return Err(Status::internal(
                        "begin activation produced a non-begin decision",
                    ));
                };
                decision.accepted_offset = Some(offset);
                decision
            } else {
                let retry = activation::decide_begin(Some(&accepted_state), &command)
                    .map_err(activation_error_to_status)?;
                let ActivationDecision::Begin(decision) = retry.decision else {
                    return Err(Status::internal(
                        "begin activation replay produced a non-begin decision",
                    ));
                };
                decision
            }
        } else {
            let ActivationDecision::Begin(decision) = transition.decision else {
                return Err(Status::internal(
                    "begin activation produced a non-begin decision",
                ));
            };
            decision
        };
        tracing::Span::current().record("outcome", tracing::field::debug(decision.outcome));
        Ok(Response::new(begin_activation_response(decision)))
    }

    #[tracing::instrument(
        name = "engine.complete_activation",
        skip_all,
        fields(project_id, run_id, activation_id, attempt)
    )]
    async fn complete_activation(
        &self,
        request: Request<CompleteActivationRequest>,
    ) -> Result<Response<CompleteActivationResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        let authorize_started_at = Instant::now();
        let authorization = self
            .authorize_external_run(external_identity.as_ref(), &req.project_id, &req.run_id)
            .await;
        record_activation_phase(
            &self.metrics,
            "complete",
            "authorize_run",
            if authorization.is_ok() { "ok" } else { "error" },
            authorize_started_at,
        );
        authorization?;
        self.validate_project_scope(&req.project_id)?;
        tracing::Span::current().record("project_id", tracing::field::display(&req.project_id));
        tracing::Span::current().record("run_id", tracing::field::display(&req.run_id));
        tracing::Span::current()
            .record("activation_id", tracing::field::display(&req.activation_id));
        tracing::Span::current().record("attempt", req.attempt);
        let activation_load_started_at = Instant::now();
        let current_result = self
            .load_required_activation_state(&req.project_id, &req.run_id, &req.activation_id)
            .await;
        record_activation_phase(
            &self.metrics,
            "complete",
            "load_activation_state",
            if current_result.is_ok() {
                "found"
            } else {
                "error"
            },
            activation_load_started_at,
        );
        let current = current_result?;
        let command = CompleteActivationCommand { request: req };
        let transition =
            activation::decide_complete(&current, &command).map_err(activation_error_to_status)?;
        let mut validated_run = None;
        if !transition.records.is_empty() {
            let authority_started_at = Instant::now();
            let authority = self
                .validate_activation_run_authority(
                    &command.request.project_id,
                    &command.request.run_id,
                    &command.request.activation_id,
                    command.request.attempt,
                    &current.current_attempt.worker_session_id,
                    &current.current_attempt.run_authority,
                    &current.current_attempt.lease_authority,
                )
                .await;
            record_activation_phase(
                &self.metrics,
                "complete",
                "validate_run_authority",
                if authority.is_ok() { "ok" } else { "error" },
                authority_started_at,
            );
            validated_run = authority?;

            let baseline_started_at = Instant::now();
            let baseline = self
                .reject_stale_activation_state_baseline(&command.request)
                .await;
            record_activation_phase(
                &self.metrics,
                "complete",
                "validate_state_baseline",
                if baseline.is_ok() { "ok" } else { "error" },
                baseline_started_at,
            );
            baseline.map_err(engine_err_to_status)?;

            let children_started_at = Instant::now();
            let children = self
                .reject_unsettled_required_children(
                    &command.request.project_id,
                    &command.request.run_id,
                    Some(&command.request.activation_id),
                )
                .await;
            record_activation_phase(
                &self.metrics,
                "complete",
                "validate_required_children",
                if children.is_ok() { "ok" } else { "error" },
                children_started_at,
            );
            children?;
        }
        let mut decision = match transition.decision {
            ActivationDecision::Complete(decision) => decision,
            _ => {
                return Err(Status::internal(
                    "complete activation produced a non-completion decision",
                ));
            }
        };
        if let Some(canonical) = transition.records.first() {
            let append_started_at = Instant::now();
            let append_result = self
                .append_activation_record(Some(current), canonical, validated_run.as_ref())
                .await;
            record_activation_phase(
                &self.metrics,
                "complete",
                "append",
                match &append_result {
                    Ok((_, _, true)) => "appended",
                    Ok((_, _, false)) => "replayed",
                    Err(_) => "error",
                },
                append_started_at,
            );
            let (accepted_state, offset, appended) = append_result.map_err(engine_err_to_status)?;
            if appended {
                decision.accepted_offset = Some(offset);
            } else {
                let replay = activation::decide_complete(&accepted_state, &command)
                    .map_err(activation_error_to_status)?;
                let ActivationDecision::Complete(replayed) = replay.decision else {
                    return Err(Status::internal(
                        "completion replay produced a non-completion decision",
                    ));
                };
                decision = replayed;
            }
        }
        Ok(Response::new(CompleteActivationResponse {
            accepted: decision.accepted,
            replayed: decision.replayed,
            activation_id: decision.activation_id,
            attempt: decision.attempt,
            accepted_journal_offset: decision.accepted_offset.unwrap_or(Offset::ZERO).0,
        }))
    }

    #[tracing::instrument(
        name = "engine.fail_activation",
        skip_all,
        fields(project_id, run_id, activation_id, attempt)
    )]
    async fn fail_activation(
        &self,
        request: Request<FailActivationRequest>,
    ) -> Result<Response<FailActivationResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        self.authorize_external_run(external_identity.as_ref(), &req.project_id, &req.run_id)
            .await?;
        self.validate_project_scope(&req.project_id)?;
        tracing::Span::current().record("project_id", tracing::field::display(&req.project_id));
        tracing::Span::current().record("run_id", tracing::field::display(&req.run_id));
        tracing::Span::current()
            .record("activation_id", tracing::field::display(&req.activation_id));
        tracing::Span::current().record("attempt", req.attempt);
        let current = self
            .load_required_activation_state(&req.project_id, &req.run_id, &req.activation_id)
            .await?;
        let command = FailActivationCommand { request: req };
        let transition =
            activation::decide_fail(&current, &command).map_err(activation_error_to_status)?;
        let validated_run = if !transition.records.is_empty() {
            self.validate_activation_run_authority(
                &command.request.project_id,
                &command.request.run_id,
                &command.request.activation_id,
                command.request.attempt,
                &current.current_attempt.worker_session_id,
                &current.current_attempt.run_authority,
                &current.current_attempt.lease_authority,
            )
            .await?
        } else {
            None
        };
        let mut decision = match transition.decision {
            ActivationDecision::Fail(decision) => decision,
            _ => {
                return Err(Status::internal(
                    "fail activation produced a non-failure decision",
                ));
            }
        };
        if let Some(canonical) = transition.records.first() {
            let (accepted_state, offset, appended) = self
                .append_activation_record(Some(current), canonical, validated_run.as_ref())
                .await
                .map_err(engine_err_to_status)?;
            if appended {
                decision.accepted_offset = Some(offset);
            } else {
                let replay = activation::decide_fail(&accepted_state, &command)
                    .map_err(activation_error_to_status)?;
                let ActivationDecision::Fail(replayed) = replay.decision else {
                    return Err(Status::internal(
                        "failure replay produced a non-failure decision",
                    ));
                };
                decision = replayed;
            }
        }
        Ok(Response::new(FailActivationResponse {
            accepted: decision.accepted,
            replayed: decision.replayed,
            activation_id: decision.activation_id,
            attempt: decision.attempt,
            status: decision.status as i32,
            accepted_journal_offset: decision.accepted_offset.unwrap_or(Offset::ZERO).0,
        }))
    }

    #[tracing::instrument(
        name = "engine.suspend_activation",
        skip_all,
        fields(project_id, run_id, activation_id, attempt, timer_key)
    )]
    async fn suspend_activation(
        &self,
        request: Request<SuspendActivationRequest>,
    ) -> Result<Response<SuspendActivationResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let mut req = request.into_inner();
        self.authorize_external_run(external_identity.as_ref(), &req.project_id, &req.run_id)
            .await?;
        self.validate_project_scope(&req.project_id)?;
        tracing::Span::current().record("project_id", tracing::field::display(&req.project_id));
        tracing::Span::current().record("run_id", tracing::field::display(&req.run_id));
        tracing::Span::current()
            .record("activation_id", tracing::field::display(&req.activation_id));
        tracing::Span::current().record("attempt", req.attempt);
        tracing::Span::current().record("timer_key", tracing::field::display(&req.timer_key));

        let current = self
            .load_required_activation_state(&req.project_id, &req.run_id, &req.activation_id)
            .await?;
        if req.delay_ms > 0 {
            let accepted_deadline = current.suspension.as_ref().filter(|suspension| {
                suspension.timer_key == req.timer_key
                    && suspension.header.input_digest == req.input_digest
                    && suspension.header.definition_digest == req.definition_digest
                    && suspension.continuation == req.continuation
            });
            req.ready_at_ms = accepted_deadline
                .map(|suspension| suspension.ready_at_ms)
                .unwrap_or_else(|| now_ms().saturating_add(req.delay_ms));
        }
        let timer_id = activation::suspension_timer_id(&req);
        let transition =
            activation::decide_timer_suspend(&current, &req).map_err(activation_error_to_status)?;
        let run = if !transition.records.is_empty() {
            self.validate_activation_run_authority(
                &req.project_id,
                &req.run_id,
                &req.activation_id,
                req.attempt,
                &current.current_attempt.worker_session_id,
                &current.current_attempt.run_authority,
                &current.current_attempt.lease_authority,
            )
            .await?
        } else {
            None
        };
        let mut decision = match transition.decision {
            ActivationDecision::Suspend(decision) => decision,
            _ => {
                return Err(Status::internal(
                    "suspend activation produced a non-suspension decision",
                ));
            }
        };
        if let Some(canonical) = transition.records.first() {
            let (_accepted_state, offset) = self
                .append_activation_suspension(current, canonical, &req, run.as_ref())
                .await
                .map_err(engine_err_to_status)?;
            decision.accepted_offset = Some(offset);
        }
        Ok(Response::new(SuspendActivationResponse {
            accepted: decision.accepted,
            replayed: decision.replayed,
            activation_id: decision.activation_id,
            attempt: decision.attempt,
            accepted_journal_offset: decision.accepted_offset.unwrap_or(Offset::ZERO).0,
            timer_id,
        }))
    }

    /// Durable step checkpoint from SDK workers.
    ///
    /// Three phases mapped to the `CheckpointType` enum:
    ///
    /// * `STEP_STARTED` — memoization lookup. Check the record cache first,
    ///   then fall back to the materialized store's `step_memo` index. If a
    ///   prior completion is found, return `memoized = true` with the cached
    ///   payload so the SDK can skip re-executing the step. No record is
    ///   appended on this path.
    ///
    /// * `STEP_COMPLETED` — append a `checkpoint.created` record carrying
    ///   the step output. The processor's runs projection picks up this
    ///   event (it's in `STEP_COMPLETION_EVENTS`) and writes the step_memo
    ///   entry, so subsequent `STEP_STARTED` lookups will hit.
    ///
    /// * `STEP_FAILED` — append a `checkpoint.failed` record for
    ///   observability. Not in the memoization list, which is deliberate:
    ///   we want retries to re-run failed steps, not cache their error.
    #[tracing::instrument(
        name = "engine.checkpoint",
        skip_all,
        fields(project_id, run_id, step_key, ckpt_type, memoized)
    )]
    async fn checkpoint(
        &self,
        request: Request<CheckpointRequest>,
    ) -> Result<Response<CheckpointResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        let project_id = req.project_id;
        authorize_external_project(external_identity.as_ref(), &project_id)?;
        self.validate_project_scope(&project_id)?;
        let checkpoint = req
            .checkpoint
            .ok_or_else(|| Status::invalid_argument("missing checkpoint payload"))?;

        if project_id.is_empty() {
            return Err(Status::invalid_argument("project_id is required"));
        }
        if checkpoint.run_id.is_empty() || checkpoint.step_key.is_empty() {
            return Err(Status::invalid_argument("run_id and step_key are required"));
        }
        let ckpt_type = engine_proto::CheckpointType::try_from(checkpoint.r#type)
            .unwrap_or(engine_proto::CheckpointType::Unspecified);

        let authorize_started_at = Instant::now();
        let authorization = self
            .authorize_external_run(external_identity.as_ref(), &project_id, &checkpoint.run_id)
            .await;
        record_checkpoint_phase(
            &self.metrics,
            "authorize_run",
            ckpt_type,
            if authorization.is_ok() { "ok" } else { "error" },
            authorize_started_at,
        );
        authorization?;

        tracing::Span::current().record("project_id", tracing::field::display(&project_id));
        tracing::Span::current().record("run_id", tracing::field::display(&checkpoint.run_id));
        tracing::Span::current().record("step_key", tracing::field::display(&checkpoint.step_key));
        tracing::Span::current().record("ckpt_type", tracing::field::debug(ckpt_type));

        // Legacy checkpoint traffic remains wire-compatible, but a root V1
        // activation with the same stable key owns replay and completion.
        // Consult journal truth so projection lag cannot reopen the step.
        let v1_activation_id = activation::activation_id(
            &project_id,
            &checkpoint.run_id,
            "",
            engine_proto::ActivationKind::Step as i32,
            &checkpoint.step_key,
        );
        let activation_load_started_at = Instant::now();
        let v1_state_result = self
            .load_activation_state(&project_id, &checkpoint.run_id, &v1_activation_id)
            .await;
        record_checkpoint_phase(
            &self.metrics,
            "load_activation_state",
            ckpt_type,
            match &v1_state_result {
                Ok(Some(_)) => "found",
                Ok(None) => "missing",
                Err(_) => "error",
            },
            activation_load_started_at,
        );
        let v1_state = v1_state_result.map_err(|error| {
            engine_err_to_status(activation_recovery_error(&v1_activation_id, error))
        })?;
        if let Some(state) = v1_state {
            if state.status == engine_proto::ActivationStatus::Completed {
                let cached_output = state
                    .completion
                    .as_ref()
                    .and_then(|completion| completion.output.as_ref())
                    .map(legacy_activation_payload_bytes)
                    .unwrap_or_default();
                return Ok(Response::new(CheckpointResponse {
                    success: true,
                    error_message: String::new(),
                    memoized: true,
                    cached_output,
                }));
            }
            return Err(Status::failed_precondition(format!(
                "{} owns step key {:?}; legacy checkpoint cannot execute or write it while status is {:?}",
                activation::DURABLE_ACTIVATION_V1_CAPABILITY,
                checkpoint.step_key,
                state.status
            )));
        }

        // STEP_STARTED → memoization lookup only, no append.
        if ckpt_type == engine_proto::CheckpointType::StepStarted {
            // 1. Cache fast path.
            if let Some(cached) =
                self.cache
                    .get_step_memo(&project_id, &checkpoint.run_id, &checkpoint.step_key)
            {
                tracing::Span::current().record("memoized", true);
                debug!(
                    project_id = %project_id,
                    run_id = %checkpoint.run_id,
                    step_key = %checkpoint.step_key,
                    "Checkpoint STEP_STARTED: memo hit (cache)"
                );
                return Ok(Response::new(CheckpointResponse {
                    success: true,
                    error_message: String::new(),
                    memoized: true,
                    cached_output: cached.data.to_vec(),
                }));
            }

            // 2. Materialized store fallback.
            if let Some(ref store) = self.store {
                let memo_load_started_at = Instant::now();
                let memo_load = RunQueryStore::load_step_memo(
                    store.as_ref(),
                    &project_id,
                    &checkpoint.run_id,
                    &checkpoint.step_key,
                )
                .await;
                record_checkpoint_phase(
                    &self.metrics,
                    "load_step_memo",
                    ckpt_type,
                    match &memo_load {
                        Ok(Some(_)) => "hit",
                        Ok(None) => "miss",
                        Err(_) => "error",
                    },
                    memo_load_started_at,
                );
                if let Some(data) = memo_load.map_err(engine_err_to_status)? {
                    tracing::Span::current().record("memoized", true);
                    debug!(
                        project_id = %project_id,
                        run_id = %checkpoint.run_id,
                        step_key = %checkpoint.step_key,
                        "Checkpoint STEP_STARTED: memo hit (store)"
                    );
                    return Ok(Response::new(CheckpointResponse {
                        success: true,
                        error_message: String::new(),
                        memoized: true,
                        cached_output: data,
                    }));
                }
            }

            tracing::Span::current().record("memoized", false);
            return Ok(Response::new(CheckpointResponse {
                success: true,
                error_message: String::new(),
                memoized: false,
                cached_output: Vec::new(),
            }));
        }

        // STEP_COMPLETED / STEP_FAILED → append a record to the journal.
        let event_type = match ckpt_type {
            engine_proto::CheckpointType::StepCompleted => "checkpoint.created",
            engine_proto::CheckpointType::StepFailed => "checkpoint.failed",
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unsupported checkpoint type: {ckpt_type:?}"
                )));
            }
        };

        // Payload: output bytes for COMPLETED, error JSON for FAILED.
        let data = if ckpt_type == engine_proto::CheckpointType::StepCompleted {
            bytes::Bytes::from(checkpoint.payload.clone())
        } else {
            let err_json = serde_json::json!({
                "error_message": checkpoint.error_message,
                "error_type": checkpoint.error_type,
            });
            bytes::Bytes::from(err_json.to_string().into_bytes())
        };

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("step_name".to_string(), checkpoint.step_name.clone());
        metadata.insert("step_type".to_string(), checkpoint.step_type.clone());
        if checkpoint.latency_ms > 0 {
            metadata.insert("latency_ms".to_string(), checkpoint.latency_ms.to_string());
        }

        let (correlation_id, parent_event_id) =
            infer_checkpoint_correlation(&self.cache, &project_id, &checkpoint);

        let record = Record {
            offset: Offset(0),
            project_id: project_id.clone(),
            run_id: checkpoint.run_id.clone(),
            event_type: event_type.to_string(),
            data,
            timestamp_ns: now_ns,
            step_key: Some(checkpoint.step_key.clone()),
            correlation_id,
            parent_event_id,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        let append_started_at = Instant::now();
        let append_result = self
            .segment
            .append_batch(std::slice::from_ref(&record))
            .await;
        record_checkpoint_phase(
            &self.metrics,
            "append",
            ckpt_type,
            if append_result.is_ok() { "ok" } else { "error" },
            append_started_at,
        );
        let offsets = append_result.map_err(engine_err_to_status)?;
        let offset = offsets[0];

        // Write-through to the record cache so a STEP_STARTED lookup within
        // the same process sees the completion immediately, before the
        // processor has projected it into the materialized store.
        let mut cached = record;
        cached.offset = offset;
        self.cache.put(Arc::new(cached));

        tracing::Span::current().record("memoized", false);
        debug!(
            project_id = %project_id,
            run_id = %checkpoint.run_id,
            step_key = %checkpoint.step_key,
            event_type = event_type,
            "Checkpoint appended"
        );

        Ok(Response::new(CheckpointResponse {
            success: true,
            error_message: String::new(),
            memoized: false,
            cached_output: Vec::new(),
        }))
    }

    /// Drain ephemeral events from a worker stream.
    ///
    /// Client-streaming RPC: the worker opens a stream and writes
    /// `EventStreamMessage`s (SSE tokens, progress, logs, spans). The engine
    /// publishes each frame immediately to the transient run stream used by
    /// gateway SSE subscribers, and returns an `EventStreamAck` on half-close. This
    /// matches the coordinator response path used by push workers, so pull
    /// and push workers expose the same observable streaming contract.
    #[tracing::instrument(name = "engine.event_stream", skip_all, fields(events_received))]
    async fn event_stream(
        &self,
        request: Request<Streaming<EventStreamMessage>>,
    ) -> Result<Response<EventStreamAck>, Status> {
        let external_identity = external_worker_identity(&request);
        let mut stream = request.into_inner();
        let mut events_received: i64 = 0;
        let mut had_error = false;
        loop {
            match stream.message().await {
                Ok(Some(msg)) => {
                    events_received += 1;
                    ensure_external_identity_not_expired(external_identity.as_ref())?;
                    self.authorize_external_run(
                        external_identity.as_ref(),
                        &msg.project_id,
                        &msg.run_id,
                    )
                    .await?;
                    self.validate_project_scope(&msg.project_id)?;
                    self.publish_event_stream_messages(vec![msg]).await?;
                }
                Ok(None) => break, // client half-close
                Err(e) => {
                    tracing::warn!(error = %e, "EventStream receive error");
                    had_error = true;
                    break;
                }
            }
        }

        tracing::Span::current().record("events_received", events_received);
        debug!(events_received, had_error, "EventStream drained");

        Ok(Response::new(EventStreamAck {
            success: !had_error,
            events_received,
        }))
    }

    /// Claim a batch of pending jobs for a worker using SKIP LOCKED semantics.
    ///
    /// Worker flow:
    /// 1. Scan `CF_RUNS_PENDING` for runs matching `(project_id, component_names)`.
    /// 2. For each candidate, try to insert a claim into the shared
    ///    [`ClaimRegistry`]; skip if another poller has already claimed it.
    /// 3. Decode the `Run` proto and return it as a `JobAssignment`.
    ///
    /// Claims automatically expire after `claim_timeout_ms` (default 5m) via
    /// the coordinator's periodic reaper — workers should complete and
    /// re-poll rather than hold a claim forever.
    #[tracing::instrument(
        name = "engine.register_worker_session",
        skip_all,
        fields(worker_id, project_id, deployment_id)
    )]
    async fn register_worker_session(
        &self,
        request: Request<RegisterWorkerSessionRequest>,
    ) -> Result<Response<RegisterWorkerSessionResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        authorize_external_worker(external_identity.as_ref(), &req.worker_id)?;
        let profile = runtime_common::durability::DurabilityProfile::from_env()
            .map_err(Status::failed_precondition)?;
        let (runtime_protocol_capabilities, required_protocol_capabilities) =
            negotiate_worker_session_protocols(&req, profile)?;
        if req.worker_id.is_empty() || req.project_id.is_empty() || req.deployment_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id, project_id and deployment_id are required",
            ));
        }
        authorize_external_deployment(
            external_identity.as_ref(),
            &req.project_id,
            &req.deployment_id,
        )?;
        self.validate_project_scope(&req.project_id)?;
        self.validate_deployment_scope(&req.deployment_id)?;

        tracing::Span::current().record("worker_id", tracing::field::display(&req.worker_id));
        tracing::Span::current().record("project_id", tracing::field::display(&req.project_id));
        tracing::Span::current()
            .record("deployment_id", tracing::field::display(&req.deployment_id));

        let worker_id = req.worker_id.clone();
        let project_id = req.project_id.clone();
        let deployment_id = req.deployment_id.clone();
        let capabilities = req.capabilities.clone();
        let components = req.components.clone();
        let service_name = req.service_name.clone();
        let service_version = req.service_version.clone();
        let service_type = req.service_type.clone();
        let route_components = engine_core::worker_routing::route_components_from_capabilities(
            &capabilities,
            &components,
        );

        let requested = req.slot_policy.unwrap_or_default();
        let requested_max = req.max_slots.max(requested.max_slots);
        let max_slots = if requested_max == 0 {
            100
        } else {
            requested_max.clamp(1, 100)
        };
        let min_slots = if requested.min_slots == 0 {
            1
        } else {
            requested.min_slots.min(max_slots)
        };
        let slot_policy = WorkerSlotPolicy {
            min_slots,
            max_slots,
            target_cpu_usage: if requested.target_cpu_usage > 0.0 {
                requested.target_cpu_usage
            } else {
                0.75
            },
            target_memory_usage: if requested.target_memory_usage > 0.0 {
                requested.target_memory_usage
            } else {
                0.80
            },
            ramp_throttle_ms: if requested.ramp_throttle_ms > 0 {
                requested.ramp_throttle_ms
            } else {
                1_000
            },
        };

        let server_slot_scaling = req
            .supported_protocol_capabilities
            .iter()
            .any(|capability| {
                capability == engine_core::activation::SERVER_SLOT_SCALING_V1_CAPABILITY
            })
            && runtime_protocol_capabilities.iter().any(|capability| {
                capability == engine_core::activation::SERVER_SLOT_SCALING_V1_CAPABILITY
            });
        let (session, replaced) = self.worker_sessions.register_with_slot_scaling(
            worker_id.clone(),
            project_id.clone(),
            deployment_id.clone(),
            slot_policy,
            route_components.clone(),
            server_slot_scaling,
        );
        if let Some(old) = replaced {
            self.poll_waiters.notify_all(
                &old.project_id,
                &old.deployment_id,
                PullWakeSource::SessionChange,
            );
        }
        let _ = (&service_name, &service_version, &service_type);
        self.record_worker_session_heartbeat(&session, 0);

        Ok(Response::new(RegisterWorkerSessionResponse {
            worker_session_id: session.session_id,
            expires_at_ms: session.expires_at_ms,
            effective_slot_policy: Some(session.slot_policy),
            supported_protocol_capabilities: runtime_protocol_capabilities,
            required_protocol_capabilities,
        }))
    }

    #[tracing::instrument(
        name = "engine.poll_job",
        skip_all,
        fields(worker_id, worker_session_hash, claimed)
    )]
    async fn poll_job(
        &self,
        request: Request<PollJobRequest>,
    ) -> Result<Response<PollJobResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        authorize_external_worker(external_identity.as_ref(), &req.worker_id)?;
        if req.worker_id.is_empty() || req.worker_session_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id and worker_session_id are required",
            ));
        }
        tracing::Span::current().record("worker_id", tracing::field::display(&req.worker_id));
        tracing::Span::current().record(
            "worker_session_hash",
            tracing::field::display(pull_workers::worker_session_hash(&req.worker_session_id)),
        );

        let session = self
            .worker_sessions
            .get_current_and_extend(&req.worker_id, &req.worker_session_id)
            .ok_or_else(|| Status::permission_denied("worker session is not active"))?;
        authorize_external_deployment(
            external_identity.as_ref(),
            &session.project_id,
            &session.deployment_id,
        )?;
        // A signed session token can survive a runtime process restart, but
        // the in-memory route (including its component inventory) cannot. Tell
        // the SDK to register again instead of accepting an unrouteable session
        // forever and silently failing to republish its components. Locally
        // registered sessions may still revive a tombstoned route through the
        // heartbeat below; adopted HA tokens require the matching live route.
        if !session.registered_locally && !self.worker_session_route_is_active(&session) {
            return Err(Status::permission_denied("worker session is not active"));
        }
        self.refresh_worker_session_liveness(&session);
        let wait_ms = req.wait_ms.clamp(0, MAX_POLL_JOB_WAIT_MS);
        let claim_timeout_ms = if req.claim_timeout_ms > 0 {
            req.claim_timeout_ms
        } else {
            300_000
        };
        let claim_timeout = Duration::from_millis(claim_timeout_ms as u64);
        let deadline = Instant::now() + Duration::from_millis(wait_ms as u64);

        let verified = VerifiedPoll::new(
            req.worker_id,
            req.worker_session_id,
            session.project_id.clone(),
            session.deployment_id.clone(),
            self.worker_session_demand_keys(&session),
            claim_timeout,
        )
        .map_err(engine_err_to_status)?;
        let dispatcher = ServicePullDispatch::new(self, session.clone(), external_identity);
        let outcome = dispatcher.poll(verified.clone(), deadline).await;
        let slot_scaling = None;
        poll_outcome_to_response(outcome, slot_scaling)
    }

    #[tracing::instrument(
        name = "engine.renew_job_lease",
        skip_all,
        fields(worker_id, worker_session_hash, run_id)
    )]
    async fn renew_job_lease(
        &self,
        request: Request<RenewJobLeaseRequest>,
    ) -> Result<Response<RenewJobLeaseResponse>, Status> {
        let received_at_ms = now_ms();
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        authorize_external_worker(external_identity.as_ref(), &req.worker_id)?;
        if req.worker_id.is_empty() || req.run_id.is_empty() || req.lease_id.is_empty() {
            return Err(Status::invalid_argument(
                "worker_id, run_id and lease_id are required",
            ));
        }
        tracing::Span::current().record("worker_id", tracing::field::display(&req.worker_id));
        tracing::Span::current().record("run_id", tracing::field::display(&req.run_id));
        if !req.worker_session_id.is_empty() {
            tracing::Span::current().record(
                "worker_session_hash",
                tracing::field::display(pull_workers::worker_session_hash(&req.worker_session_id)),
            );
        }
        let requested_mode = WorkerMode::try_from(req.mode)
            .map_err(|_| Status::invalid_argument("unknown worker mode"))?;
        let legacy_pull = requested_mode == WorkerMode::Unspecified;
        let mode = match requested_mode {
            WorkerMode::Pull => LeaseRenewalMode::Pull,
            WorkerMode::Push => LeaseRenewalMode::Push,
            WorkerMode::Unspecified if !req.worker_session_id.is_empty() => LeaseRenewalMode::Pull,
            WorkerMode::Unspecified => {
                return Err(Status::invalid_argument(
                    "mode is required when worker_session_id is empty",
                ));
            }
        };
        authorize_external_lease_renewal_mode(external_identity.as_ref(), mode)?;

        let (project_id, deployment_id, attempt, requested_session_hash) = match mode {
            LeaseRenewalMode::Pull => {
                if req.worker_session_id.is_empty() {
                    return Err(Status::invalid_argument(
                        "worker_session_id is required for pull renewal",
                    ));
                }
                let Some(session) = self
                    .worker_sessions
                    .get_current_and_extend(&req.worker_id, &req.worker_session_id)
                else {
                    self.record_renew_job_lease_result("session_inactive");
                    if legacy_pull {
                        return Err(Status::permission_denied("worker session is not active"));
                    }
                    return Ok(Response::new(RenewJobLeaseResponse {
                        renewed: false,
                        lease_expires_at_ms: 0,
                        outcome: LeaseRenewalOutcome::SessionInactive as i32,
                    }));
                };
                if !session.registered_locally && !self.worker_session_route_is_active(&session) {
                    self.record_renew_job_lease_result("session_inactive");
                    if legacy_pull {
                        return Err(Status::permission_denied("worker session is not active"));
                    }
                    return Ok(Response::new(RenewJobLeaseResponse {
                        renewed: false,
                        lease_expires_at_ms: 0,
                        outcome: LeaseRenewalOutcome::SessionInactive as i32,
                    }));
                }
                if (!req.project_id.is_empty() && req.project_id != session.project_id)
                    || (!req.deployment_id.is_empty() && req.deployment_id != session.deployment_id)
                {
                    self.record_renew_job_lease_result("authority_lost");
                    return Ok(Response::new(RenewJobLeaseResponse {
                        renewed: false,
                        lease_expires_at_ms: 0,
                        outcome: LeaseRenewalOutcome::AuthorityLost as i32,
                    }));
                }
                self.refresh_worker_session_liveness(&session);
                (
                    session.project_id.clone(),
                    session.deployment_id.clone(),
                    req.attempt,
                    pull_workers::worker_session_hash(&session.session_id),
                )
            }
            LeaseRenewalMode::Push => {
                if req.project_id.is_empty()
                    || req.deployment_id.is_empty()
                    || req.attempt.is_none()
                {
                    return Err(Status::invalid_argument(
                        "project_id, deployment_id and attempt are required for push renewal",
                    ));
                }
                (
                    req.project_id.clone(),
                    req.deployment_id.clone(),
                    req.attempt,
                    String::new(),
                )
            }
        };
        authorize_external_deployment(external_identity.as_ref(), &project_id, &deployment_id)?;
        let run = self
            .authoritative_completion_run(&project_id, &req.run_id, None)
            .await?;
        let session_hash = match mode {
            LeaseRenewalMode::Pull => requested_session_hash,
            // Push renewals authenticate with the worker/lease tuple and do
            // not carry a worker session. Preserve the assignment's durable
            // hash (empty only for legacy push leases) across renewal.
            LeaseRenewalMode::Push => run
                .metadata
                .get(engine_core::lease_fence::RUN_META_LEASE_SESSION_HASH)
                .cloned()
                .unwrap_or_default(),
        };
        if is_terminal_run_status(&run.status) {
            self.record_renew_job_lease_result("terminal");
            return Ok(Response::new(RenewJobLeaseResponse {
                renewed: false,
                lease_expires_at_ms: run.lease_expires_at_ms,
                outcome: LeaseRenewalOutcome::Terminal as i32,
            }));
        }
        let attempt = attempt.unwrap_or(run.attempt);
        let lease_timeout_ms = if req.lease_timeout_ms > 0 {
            req.lease_timeout_ms
        } else {
            run.lease_timeout_ms.max(DEFAULT_LEASE_TIMEOUT_MS)
        };
        let lease_expires_at_ms = match self
            .append_lease_renewal(
                &run,
                &req.worker_id,
                &deployment_id,
                &req.lease_id,
                attempt,
                &session_hash,
                mode,
                lease_timeout_ms,
                received_at_ms,
            )
            .await
        {
            Ok(expires_at_ms) => expires_at_ms,
            Err(status) if status.code() == Code::FailedPrecondition => {
                self.record_renew_job_lease_result("authority_lost");
                return Ok(Response::new(RenewJobLeaseResponse {
                    renewed: false,
                    lease_expires_at_ms: run.lease_expires_at_ms,
                    outcome: LeaseRenewalOutcome::AuthorityLost as i32,
                }));
            }
            Err(status) => {
                self.record_renew_job_lease_result("append_error");
                return Err(status);
            }
        };
        self.record_renew_job_lease_result(match mode {
            LeaseRenewalMode::Push => "push_renewed",
            LeaseRenewalMode::Pull => "pull_renewed",
        });

        Ok(Response::new(RenewJobLeaseResponse {
            renewed: true,
            lease_expires_at_ms,
            outcome: LeaseRenewalOutcome::Renewed as i32,
        }))
    }

    #[tracing::instrument(
        name = "engine.report_worker_capacity",
        skip_all,
        fields(worker_id, worker_session_hash)
    )]
    async fn report_worker_capacity(
        &self,
        request: Request<ReportWorkerCapacityRequest>,
    ) -> Result<Response<ReportWorkerCapacityResponse>, Status> {
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        authorize_external_worker(external_identity.as_ref(), &req.worker_id)?;
        if req.worker_id.is_empty() || req.worker_session_id.is_empty() {
            self.record_worker_capacity_result("rejected");
            return Err(Status::invalid_argument(
                "worker_id and worker_session_id are required",
            ));
        }
        tracing::Span::current().record("worker_id", tracing::field::display(&req.worker_id));
        tracing::Span::current().record(
            "worker_session_hash",
            tracing::field::display(pull_workers::worker_session_hash(&req.worker_session_id)),
        );

        let session = match self
            .worker_sessions
            .get_current_and_extend(&req.worker_id, &req.worker_session_id)
        {
            Some(session) => session,
            None => {
                self.record_worker_capacity_result("rejected");
                return Err(Status::permission_denied("worker session is not active"));
            }
        };
        if !session.registered_locally && !self.worker_session_route_is_active(&session) {
            self.record_worker_capacity_result("rejected");
            return Err(Status::permission_denied("worker session is not active"));
        }
        authorize_external_deployment(
            external_identity.as_ref(),
            &session.project_id,
            &session.deployment_id,
        )?;

        let policy_max = session.slot_policy.max_slots.max(1);
        let desired_slots = if req.desired_slots == 0 {
            session.slot_policy.min_slots.max(1)
        } else {
            req.desired_slots.min(policy_max)
        };
        let effective_max_slots = if req.effective_max_slots == 0 {
            policy_max
        } else {
            req.effective_max_slots.min(policy_max)
        };

        self.record_worker_session_capacity(&session, &req, desired_slots, effective_max_slots);
        self.record_worker_capacity_gauges(&session, &req, desired_slots, effective_max_slots);
        self.record_worker_capacity_result("accepted");

        Ok(Response::new(ReportWorkerCapacityResponse {
            accepted: true,
            recorded_at_ms: now_ms(),
        }))
    }

    #[tracing::instrument(
        name = "engine.poll_jobs",
        skip_all,
        fields(worker_id, project_id, claimed)
    )]
    async fn poll_jobs(
        &self,
        _request: Request<PollJobsRequest>,
    ) -> Result<Response<PollJobsResponse>, Status> {
        Err(Status::unimplemented(
            "legacy PollJobs batch polling has been removed; use RegisterWorkerSession and PollJob",
        ))
    }

    /// Complete a claimed job by appending its terminal journal event and
    /// releasing the in-memory claim. Pull workers do not have a bidirectional
    /// response stream, so this RPC is the durable completion boundary for
    /// parked long-poll execution.
    #[tracing::instrument(
        name = "engine.complete_job",
        skip_all,
        fields(job_id, worker_id, success)
    )]
    async fn complete_job(
        &self,
        request: Request<CompleteJobRequest>,
    ) -> Result<Response<CompleteJobResponse>, Status> {
        let received_at_ms = now_ms();
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        authorize_external_worker(external_identity.as_ref(), &req.worker_id)?;
        let project_id = req.project_id;
        authorize_external_project(external_identity.as_ref(), &project_id)?;
        self.validate_project_scope(&project_id)?;
        let job_id = req.job_id; // job_id == run_id
        let worker_id = req.worker_id;
        let success = req.success;
        let output_data = req.output_data;
        let error_message = req.error_message;
        let error_code = req.error_code;
        let mut metadata = req.metadata;
        let completion_event_type = metadata.remove("completion_event_type");
        let lease_id = req.lease_id;
        let worker_session_id = req.worker_session_id;
        let lifecycle_records = req.lifecycle_records;
        let attempt = req.attempt.ok_or_else(|| {
            Status::invalid_argument(
                "project_id, job_id, worker_id, lease_id, worker_session_id and attempt are required",
            )
        })?;

        if project_id.is_empty()
            || job_id.is_empty()
            || worker_id.is_empty()
            || lease_id.is_empty()
            || worker_session_id.is_empty()
        {
            return Err(Status::invalid_argument(
                "project_id, job_id, worker_id, lease_id, worker_session_id and attempt are required",
            ));
        }

        tracing::Span::current().record("job_id", tracing::field::display(&job_id));
        tracing::Span::current().record("worker_id", tracing::field::display(&worker_id));
        tracing::Span::current().record("success", success);

        engine_core::lease_fence::strip_reserved_fence_metadata(&mut metadata);
        for key in COMPLETION_RESERVED_METADATA_KEYS {
            metadata.remove(*key);
        }

        let paused_event_type = completion_event_type
            .as_deref()
            .filter(|value| matches!(*value, "run.paused" | "workflow.paused"));
        let (event_type, data) = match (success, paused_event_type) {
            (true, Some(paused_event_type)) => (paused_event_type, output_data),
            (true, None) => {
                let output = serde_json::from_slice::<serde_json::Value>(&output_data)
                    .unwrap_or_else(|_| {
                        serde_json::Value::String(String::from_utf8_lossy(&output_data).to_string())
                    });
                (
                    "run.completed",
                    serde_json::to_vec(&json!({ "output_data": output })).unwrap_or_default(),
                )
            }
            (false, _) => {
                if !error_code.is_empty() {
                    metadata.insert("error_code".to_string(), error_code.clone());
                }
                (
                    "run.failed",
                    serde_json::to_vec(&json!({
                        "error_message": error_message,
                        "error_code": error_code,
                        "error_type": metadata.get("error_type"),
                        "error_stack": metadata.get("error_stack"),
                    }))
                    .unwrap_or_default(),
                )
            }
        };
        let successful_run_completion = event_type == "run.completed";

        let run = self
            .authoritative_completion_run(
                &project_id,
                &job_id,
                crate::poll_jobs::assignment_commit_offset(&metadata),
            )
            .await
            .inspect_err(|status| self.record_complete_job_rejected(status.code()))?;
        authorize_external_deployment(external_identity.as_ref(), &project_id, &run.deployment_id)?;
        metadata.insert("deployment_id".to_string(), run.deployment_id.clone());
        if !run.component_name.is_empty() {
            metadata.insert("component_name".to_string(), run.component_name.clone());
        }
        if !run.component_type.is_empty() {
            metadata.insert("component_type".to_string(), run.component_type.clone());
        }
        let lifecycle_bundle_digest = engine_core::journal_contract::pull_completion_bundle_digest(
            &lifecycle_records
                .iter()
                .cloned()
                .map(Record::from_proto)
                .collect::<Vec<_>>(),
        );
        let lifecycle_records = self
            .validate_completion_lifecycle_records(
                lifecycle_records,
                &project_id,
                &job_id,
                &run,
                &worker_id,
                &worker_session_id,
                &lease_id,
                attempt,
                received_at_ms,
            )
            .inspect_err(|status| self.record_complete_job_rejected(status.code()))?;

        metadata.insert(
            engine_core::journal_contract::META_PULL_COMPLETION_BUNDLE_DIGEST.to_string(),
            lifecycle_bundle_digest,
        );

        // A runtime-authored pull terminal closes the same observable run
        // lifecycle as run.started. Prefer the start carried in this atomic
        // completion bundle, then an earlier cached start for SDKs that sent
        // it separately. The run id is a deterministic fallback for legacy
        // histories that have no start correlation.
        let completion_correlation_id = lifecycle_records
            .iter()
            .rev()
            .find(|record| record.event_type == "run.started")
            .and_then(|record| record.correlation_id.clone())
            .or_else(|| {
                self.cache
                    .get_by_run_id(&project_id, &job_id)
                    .and_then(|records| {
                        records
                            .into_iter()
                            .rev()
                            .find(|record| record.event_type == "run.started")
                            .and_then(|record| record.correlation_id.clone())
                    })
            });
        let completion_correlation_id = if completion_correlation_id.is_some() {
            completion_correlation_id
        } else {
            // Mixed-version workers may have appended run.started before
            // CompleteJob. Only that compatibility path pays for a durable
            // read when the local cache does not contain the start.
            let records = if let Some(backend) = self.completion_append_backend() {
                backend
                    .read_by_run_id(
                        &project_id,
                        &job_id,
                        Offset::ZERO,
                        Some(vec!["run.started".to_string()]),
                    )
                    .await
                    .ok()
            } else if let Some(store) = self.store.as_ref() {
                RunQueryStore::load_run_events(store.as_ref(), &project_id, &job_id, 0)
                    .await
                    .ok()
            } else {
                None
            };
            records.and_then(|records| {
                records
                    .into_iter()
                    .filter(|record| record.event_type == "run.started")
                    .max_by_key(|record| record.offset.0)
                    .and_then(|record| record.correlation_id)
            })
        }
        .unwrap_or_else(|| job_id.clone());

        let desired_record = Record {
            offset: Offset::ZERO,
            project_id: project_id.clone(),
            run_id: job_id.clone(),
            event_type: event_type.to_string(),
            data: Bytes::from(data),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: Some(completion_correlation_id),
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        // Guardrail expansion needs its decision/approval records published
        // only after the same lease fence wins. That contract is outside the
        // pinned beta slice, so guarded pull completion is explicitly
        // fail-closed instead of bypassing policy or leaking an unfenced
        // terminal record.
        if self.guardrails_enabled() {
            let status = Status::failed_precondition(
                "runtime guardrails must be disabled for beta pull completion",
            );
            self.record_complete_job_rejected(status.code());
            return Err(status);
        }
        if desired_record.project_id != project_id
            || desired_record.run_id != job_id
            || !matches!(
                desired_record.event_type.as_str(),
                "run.completed" | "run.failed" | "run.paused" | "workflow.paused"
            )
        {
            let status = Status::failed_precondition(
                "guardrail produced an unsupported pull completion outcome",
            );
            self.record_complete_job_rejected(status.code());
            return Err(status);
        }

        let terminal = self
            .fenced_completion_record(
                desired_record,
                &run,
                &worker_id,
                &worker_session_id,
                &lease_id,
                attempt,
                received_at_ms,
            )
            .inspect_err(|status| self.record_complete_job_rejected(status.code()))?;

        let direct_completion_append = self.completion_append_backend().is_none();
        if successful_run_completion && direct_completion_append {
            self.reject_unsettled_required_children(&project_id, &job_id, None)
                .await?;
        }
        let session = self
            .worker_sessions
            .get_current_and_extend(&worker_id, &worker_session_id)
            .ok_or_else(|| {
                let status = Status::permission_denied("worker session is not active");
                self.record_complete_job_rejected(status.code());
                status
            })?;
        if session.project_id != project_id
            || session.deployment_id != run.deployment_id
            || session.worker_id != worker_id
            || (!session.registered_locally && !self.worker_session_route_is_active(&session))
        {
            let status = Status::permission_denied(
                "worker session does not own this project/deployment lease",
            );
            self.record_complete_job_rejected(status.code());
            return Err(status);
        }
        self.refresh_worker_session_liveness(&session);

        // One batch: lifecycle records below the fenced terminal. The segment
        // decides the fence inside the write, so a losing completion writes
        // nothing at all, and a lost-ack retry resolves every record to its
        // earlier offset through the idempotency slots.
        let submitted_lifecycle = lifecycle_records.len();
        let mut batch = lifecycle_records;
        batch.push(terminal);
        let written = self
            .append_completion_batch(batch)
            .await
            .inspect_err(|status| self.record_complete_job_rejected(status.code()))?;
        let lifecycle_written_count = written.saturating_sub(1).min(submitted_lifecycle) as u32;
        if let Some(metrics) = &self.metrics {
            metrics
                .complete_job_lifecycle_records
                .record(submitted_lifecycle as u64, &[]);
            if submitted_lifecycle > lifecycle_written_count as usize {
                metrics.complete_job_lifecycle_duplicates.add(
                    (submitted_lifecycle - lifecycle_written_count as usize) as u64,
                    &[],
                );
            }
        }
        let claim = self.claim_registry.release_entry(&job_id);
        if let Some(metrics) = &self.metrics {
            let result = if paused_event_type.is_some() {
                "paused"
            } else if success {
                "success"
            } else {
                "failure"
            };
            let claim_state = if claim.is_some() {
                "present"
            } else {
                "missing"
            };
            let labels = [
                opentelemetry::KeyValue::new("worker_id", worker_id.clone()),
                opentelemetry::KeyValue::new("result", result),
                opentelemetry::KeyValue::new("claim", claim_state),
            ];
            metrics.complete_job_count.add(1, &labels);
            if let Some(claim) = claim.as_ref() {
                metrics
                    .complete_job_duration
                    .record(claim.claimed_at.elapsed().as_secs_f64() * 1000.0, &labels);
            }
        }

        debug!(
            job_id = %job_id,
            worker_id = %worker_id,
            success = success,
            "CompleteJob acknowledged",
        );

        // Liveness-only: ReportWorkerCapacity owns the active count, so a
        // completion must not zero it while other slots are still busy.
        self.refresh_worker_session_liveness(&session);
        Ok(Response::new(CompleteJobResponse {
            acknowledged: true,
            lifecycle_written_count,
        }))
    }

    type TailStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<TailResponse, Status>> + Send>>;

    async fn tail(
        &self,
        request: Request<TailRequest>,
    ) -> Result<Response<Self::TailStream>, Status> {
        let req = request.into_inner();
        let from = Offset(req.from_offset);

        let has_run_filter = !req.run_id.is_empty() && !req.project_id.is_empty();

        if has_run_filter {
            // Per-run cache-based tail: efficient, no segment scan, no thundering herd.
            // Reads from RecordCache (indexed by run_id) and subscribes to per-run Notify.
            let cache = self.cache.clone();
            let segment = self.segment.clone();
            let project_id = req.project_id;
            let run_id = req.run_id;
            let partition_count = segment.partition_count();
            let partition = partition_for_run(&run_id, partition_count);
            if from != Offset::ZERO {
                let (from_partition, _) = decode_offset_for_partition_count(from, partition_count)
                    .map_err(engine_err_to_status)?;
                if from_partition != partition {
                    return Err(Status::invalid_argument(format!(
                        "tail cursor belongs to partition {from_partition}, but run {run_id} maps to partition {partition}"
                    )));
                }
            }
            let notify = cache.subscribe(&project_id, &run_id);
            let mut transient_rx = cache.subscribe_transient(&project_id, &run_id);
            let segment_cursor_start = segment
                .partition_tail_offset(partition)
                .await
                .map_err(engine_err_to_status)?;

            let stream = async_stream::stream! {
                let mut cursor = from;
                let mut segment_cursor = segment_cursor_start;
                let mut has_seen_records = false;

                'tail: loop {
                    // Register interest before reading cache state. `notify_waiters`
                    // only wakes existing `Notified` futures; if a terminal event
                    // lands between the cache read and `notified().await`, a sync
                    // /run waiter can otherwise sleep until the client times out.
                    let notified = notify.notified();

                    // Read any records from cursor onwards.
                    let records = cache.get_by_run_id_from_offset(&project_id, &run_id, cursor);

                    match records {
                        Some(batch) if !batch.is_empty() => {
                            has_seen_records = true;
                            for record in batch {
                                let next = Offset(record.offset.0 + 1);
                                if engine_core::journal_contract::is_runtime_internal_event(
                                    record.as_ref(),
                                ) {
                                    cursor = next;
                                    continue;
                                }
                                if terminal_status_for_record(record.as_ref()).is_some() {
                                    for transient in drain_pending_transient_records(
                                        &mut transient_rx,
                                        &run_id,
                                    ) {
                                        yield Ok(TailResponse {
                                            record: Some(transient.to_proto()),
                                        });
                                    }
                                }
                                let is_terminal = should_end_run_tail(record.as_ref());
                                yield Ok(TailResponse {
                                    record: Some(record.to_proto()),
                                });
                                cursor = next;
                                if is_terminal {
                                    break 'tail;
                                }
                            }
                        }
                        Some(_) => {}
                        None if !has_seen_records => {
                            // Run not yet in cache. The subscriber may have connected
                            // before run.queued was written (PrepareTerminalEventWaiter
                            // pattern). Wait for the first record to appear.
                        }
                        None => {
                            // Run was in cache but has been evicted (completed/LRU).
                            break;
                        }
                    }

                    // Wait for new records. The Notify is woken when:
                    // - A new record is put() for this run
                    // - The run is removed/evicted (cleanup wake)
                    let should_poll_segment = tokio::select! {
                        transient = transient_rx.recv() => {
                            match transient {
                                Ok(record) => {
                                    yield Ok(TailResponse { record: Some(record.to_proto()) });
                                    continue;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    tracing::debug!(run_id = %run_id, skipped, "Transient run tail lagged");
                                    continue;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                            }
                        }
                        _ = notified => false,
                        _ = tokio::time::sleep(PER_RUN_TAIL_SEGMENT_POLL_INTERVAL) => true,
                    };
                    if should_poll_segment {
                        let tail_offset = match segment.partition_tail_offset(partition).await {
                            Ok(offset) => offset,
                            Err(e) => {
                                yield Err(engine_err_to_status(e));
                                return;
                            }
                        };
                        if segment_cursor < tail_offset {
                            let records = match segment
                                .read_partition_range(partition, segment_cursor, tail_offset)
                                .await
                            {
                                Ok(records) => records,
                                Err(e) => {
                                    yield Err(engine_err_to_status(e));
                                    return;
                                }
                            };
                            for record in records {
                                let next = Offset(record.offset.0 + 1);
                                let (_, local_offset) = match
                                    decode_offset_for_partition_count(record.offset, partition_count)
                                {
                                    Ok(decoded) => decoded,
                                    Err(e) => {
                                        yield Err(engine_err_to_status(e));
                                        return;
                                    }
                                };
                                let matches_run =
                                    record.project_id == project_id && record.run_id == run_id;
                                segment_cursor = Offset(local_offset.saturating_add(1));
                                if matches_run {
                                    if next <= cursor {
                                        continue;
                                    }
                                    if engine_core::journal_contract::is_runtime_internal_event(
                                        &record,
                                    ) {
                                        cursor = next;
                                        continue;
                                    }
                                    has_seen_records = true;
                                    let is_terminal = should_end_run_tail(&record);
                                    cursor = next;
                                    if terminal_status_for_record(&record).is_some() {
                                        for transient in drain_pending_transient_records(
                                            &mut transient_rx,
                                            &run_id,
                                        ) {
                                            yield Ok(TailResponse {
                                                record: Some(transient.to_proto()),
                                            });
                                        }
                                    }
                                    yield Ok(TailResponse {
                                        record: Some(record.to_proto()),
                                    });
                                    if is_terminal {
                                        break 'tail;
                                    }
                                }
                            }
                        }
                    }
                }
            };

            Ok(Response::new(Box::pin(stream)))
        } else {
            // Segment-level tail: scans the full log. Used by processors and admin tools.
            let record_stream = self
                .segment
                .tail(from)
                .await
                .map_err(engine_err_to_status)?;

            let tenant_filter = if req.project_id.is_empty() {
                None
            } else {
                Some(req.project_id)
            };

            let stream = record_stream.filter_map(move |result| match result {
                Ok(record) => {
                    if engine_core::journal_contract::is_runtime_internal_event(&record) {
                        return None;
                    }
                    if let Some(ref tid) = tenant_filter {
                        if record.project_id != *tid {
                            return None;
                        }
                    }
                    Some(Ok(TailResponse {
                        record: Some(record.to_proto()),
                    }))
                }
                Err(e) => Some(Err(engine_err_to_status(e))),
            });

            Ok(Response::new(Box::pin(stream)))
        }
    }

    async fn get_entity_state(
        &self,
        request: Request<GetEntityStateRequest>,
    ) -> Result<Response<GetEntityStateResponse>, Status> {
        if (request.get_ref().entity_type == engine_core::runtime_work::ENTITY_TYPE
            || request.get_ref().entity_type == engine_core::experiment_feedback::ENTITY_TYPE)
            && !runtime_authored_request(&request, self.accept_peer_runtime_mark)
        {
            return Err(Status::permission_denied(
                "runtime controller state is internal",
            ));
        }
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        if external_identity.is_some() && (req.scope != "run" || req.scope_id.is_empty()) {
            return Err(Status::permission_denied(
                "external workloads may read only run-scoped entity state",
            ));
        }
        self.authorize_external_run(external_identity.as_ref(), &req.project_id, &req.scope_id)
            .await?;

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;

        match WorkflowQueryStore::load_entity_state(
            store.as_ref(),
            &req.project_id,
            &req.entity_type,
            &req.entity_key,
            &req.scope,
            &req.scope_id,
        )
        .await
        .map_err(engine_err_to_status)?
        {
            Some((data, version)) => Ok(Response::new(GetEntityStateResponse {
                found: true,
                state_json: data,
                version,
            })),
            None => Ok(Response::new(GetEntityStateResponse {
                found: false,
                state_json: vec![],
                version: 0,
            })),
        }
    }

    async fn put_entity_state(
        &self,
        request: Request<PutEntityStateRequest>,
    ) -> Result<Response<PutEntityStateResponse>, Status> {
        if (request.get_ref().entity_type == engine_core::runtime_work::ENTITY_TYPE
            || request.get_ref().entity_type == engine_core::experiment_feedback::ENTITY_TYPE)
            && !runtime_authored_request(&request, self.accept_peer_runtime_mark)
        {
            return Err(Status::permission_denied(
                "runtime controller state is internal",
            ));
        }
        let external_identity = external_worker_identity(&request);
        let req = request.into_inner();
        if external_identity.is_some()
            && (req.scope != "run" || req.scope_id.is_empty() || req.scope_id != req.run_id)
        {
            return Err(Status::permission_denied(
                "external workloads may write only their active run-scoped entity state",
            ));
        }
        self.authorize_external_run(external_identity.as_ref(), &req.project_id, &req.run_id)
            .await?;
        if req.project_id.is_empty() || req.entity_type.is_empty() || req.entity_key.is_empty() {
            return Err(Status::invalid_argument(
                "project_id, entity_type and entity_key are required",
            ));
        }
        self.validate_project_scope(&req.project_id)?;
        if req.expected_version < 0 {
            return Err(Status::invalid_argument(
                "expected_version must be non-negative",
            ));
        }
        self.validate_entity_state_write_authority(&req).await?;

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (
            &req.project_id,
            &req.entity_type,
            &req.entity_key,
            &req.scope,
            &req.scope_id,
        )
            .hash(&mut hasher);
        let stripe = hasher.finish() as usize % self.entity_state_write_locks.len();
        let _write_guard = self.entity_state_write_locks[stripe].lock().await;

        // Optimistic locking: expected_version == 0 means create-only, while
        // updates must pass the current materialized version. An exact
        // expected+1/data match is a lost-response retry of an already applied
        // idempotent write.
        let current = WorkflowQueryStore::load_entity_state(
            store.as_ref(),
            &req.project_id,
            &req.entity_type,
            &req.entity_key,
            &req.scope,
            &req.scope_id,
        )
        .await
        .map_err(engine_err_to_status)?;
        let new_version = match (req.expected_version, current) {
            (0, None) => 1,
            (expected, Some((current_data, current_version)))
                if current_version == expected.saturating_add(1)
                    && current_data == req.state_json =>
            {
                return Ok(Response::new(PutEntityStateResponse {
                    new_version: current_version,
                }));
            }
            (0, Some((_, current_version))) => {
                return Err(Status::failed_precondition(format!(
                    "version conflict: expected new entity, current {}",
                    current_version
                )));
            }
            (expected, Some((_, current_version))) if current_version == expected => expected + 1,
            (expected, Some((_, current_version))) => {
                return Err(Status::failed_precondition(format!(
                    "version conflict: expected {}, current {}",
                    expected, current_version
                )));
            }
            (expected, None) => {
                return Err(Status::failed_precondition(format!(
                    "version conflict: expected {}, current 0",
                    expected
                )));
            }
        };

        // Build metadata for the journal event
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("entity_type".to_string(), req.entity_type.clone());
        metadata.insert("entity_key".to_string(), req.entity_key.clone());
        metadata.insert("scope".to_string(), req.scope.clone());
        metadata.insert("scope_id".to_string(), req.scope_id.clone());
        metadata.insert("new_version".to_string(), new_version.to_string());
        metadata.insert(
            "expected_version".to_string(),
            req.expected_version.to_string(),
        );
        if !req.operation_id.is_empty() {
            metadata.insert("operation_id".to_string(), req.operation_id.clone());
        }
        if !req.run_id.is_empty() {
            metadata.insert("worker_id".to_string(), req.worker_id.clone());
            metadata.insert("lease_id".to_string(), req.lease_id.clone());
            metadata.insert(
                "lease_attempt".to_string(),
                req.attempt.unwrap_or_default().to_string(),
            );
            metadata.insert(
                "lease_session_hash".to_string(),
                pull_workers::worker_session_hash(&req.worker_session_id),
            );
        }

        let run_id = if req.run_id.is_empty() {
            format!(
                "entity::{}::{}::{}::{}::{}",
                req.project_id, req.entity_type, req.entity_key, req.scope, req.scope_id
            )
        } else {
            req.run_id.clone()
        };

        // Append state change event to journal
        let record = Record {
            offset: Offset::ZERO, // assigned by segment
            project_id: req.project_id.clone(),
            run_id,
            event_type: "workflow.state.changed".to_string(),
            data: req.state_json.clone().into(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            step_key: None,
            correlation_id: (!req.operation_id.is_empty()).then(|| req.operation_id.clone()),
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        self.segment
            .append_batch(std::slice::from_ref(&record))
            .await
            .map_err(engine_err_to_status)?;

        // Write-through to store for immediate read-after-write consistency
        EntityStateWriteStore::store_entity_state(
            store.as_ref(),
            &req.project_id,
            &req.entity_type,
            &req.entity_key,
            &req.scope,
            &req.scope_id,
            record.data.as_ref(),
            new_version,
        )
        .await
        .map_err(engine_err_to_status)?;

        Ok(Response::new(PutEntityStateResponse { new_version }))
    }

    // --- Approvals ---

    async fn get_approval(
        &self,
        request: Request<GetApprovalRequest>,
    ) -> Result<Response<GetApprovalResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match WorkflowQueryStore::load_approval(store.as_ref(), &req.project_id, &req.approval_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let approval = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode approval: {e}")))?;
                Ok(Response::new(GetApprovalResponse {
                    approval: Some(approval),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetApprovalResponse {
                approval: None,
                found: false,
            })),
        }
    }

    async fn list_approvals(
        &self,
        request: Request<EngineListApprovalsRequest>,
    ) -> Result<Response<EngineListApprovalsResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items =
            WorkflowQueryStore::scan_run_approvals(store.as_ref(), &req.project_id, &req.run_id)
                .await
                .map_err(engine_err_to_status)?;
        let mut approvals = Vec::with_capacity(items.len());
        for data in items {
            let a: engine_proto::Approval = prost::Message::decode(data.as_slice())
                .map_err(|e| Status::internal(format!("decode approval: {e}")))?;
            if !req.status_filter.is_empty() && a.status != req.status_filter {
                continue;
            }
            approvals.push(a);
        }
        Ok(Response::new(EngineListApprovalsResponse { approvals }))
    }

    // --- Signals ---

    async fn get_signal(
        &self,
        request: Request<GetSignalRequest>,
    ) -> Result<Response<GetSignalResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match WorkflowQueryStore::load_signal(store.as_ref(), &req.project_id, &req.signal_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let signal = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode signal: {e}")))?;
                Ok(Response::new(GetSignalResponse {
                    signal: Some(signal),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetSignalResponse {
                signal: None,
                found: false,
            })),
        }
    }

    async fn lookup_signal(
        &self,
        request: Request<LookupSignalRequest>,
    ) -> Result<Response<LookupSignalResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match WorkflowQueryStore::lookup_run_signal(
            store.as_ref(),
            &req.project_id,
            &req.run_id,
            &req.signal_name,
            &req.waiting_step,
        )
        .await
        .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let signal = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode signal: {e}")))?;
                Ok(Response::new(LookupSignalResponse {
                    signal: Some(signal),
                    found: true,
                }))
            }
            None => Ok(Response::new(LookupSignalResponse {
                signal: None,
                found: false,
            })),
        }
    }

    async fn list_signals(
        &self,
        request: Request<ListSignalsRequest>,
    ) -> Result<Response<ListSignalsResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items =
            WorkflowQueryStore::scan_run_signals(store.as_ref(), &req.project_id, &req.run_id)
                .await
                .map_err(engine_err_to_status)?;
        let mut signals = Vec::with_capacity(items.len());
        for data in items {
            let s = prost::Message::decode(data.as_slice())
                .map_err(|e| Status::internal(format!("decode signal: {e}")))?;
            signals.push(s);
        }
        Ok(Response::new(ListSignalsResponse { signals }))
    }

    // --- Timers ---

    async fn get_timer(
        &self,
        request: Request<GetTimerRequest>,
    ) -> Result<Response<GetTimerResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match WorkflowQueryStore::load_timer(store.as_ref(), &req.project_id, &req.timer_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let timer = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode timer: {e}")))?;
                Ok(Response::new(GetTimerResponse {
                    timer: Some(timer),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetTimerResponse {
                timer: None,
                found: false,
            })),
        }
    }

    async fn list_timers(
        &self,
        request: Request<ListTimersRequest>,
    ) -> Result<Response<ListTimersResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items =
            WorkflowQueryStore::scan_run_timers(store.as_ref(), &req.project_id, &req.run_id)
                .await
                .map_err(engine_err_to_status)?;
        let mut timers = Vec::with_capacity(items.len());
        for data in items {
            let t = prost::Message::decode(data.as_slice())
                .map_err(|e| Status::internal(format!("decode timer: {e}")))?;
            timers.push(t);
        }
        Ok(Response::new(ListTimersResponse { timers }))
    }

    async fn list_due_timers(
        &self,
        request: Request<ListDueTimersRequest>,
    ) -> Result<Response<ListDueTimersResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            100
        };
        let items = TimerWorkStore::scan_due_timers(store.as_ref(), req.now_ns, limit)
            .await
            .map_err(engine_err_to_status)?;
        let mut timers = Vec::with_capacity(items.len());
        for data in items {
            let t = prost::Message::decode(data.as_slice())
                .map_err(|e| Status::internal(format!("decode timer: {e}")))?;
            timers.push(t);
        }
        Ok(Response::new(ListDueTimersResponse { timers }))
    }

    // --- Jobs ---

    async fn get_job(
        &self,
        request: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match WorkflowQueryStore::load_job(store.as_ref(), &req.project_id, &req.job_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let job = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode job: {e}")))?;
                Ok(Response::new(GetJobResponse {
                    job: Some(job),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetJobResponse {
                job: None,
                found: false,
            })),
        }
    }

    async fn list_jobs_by_run(
        &self,
        request: Request<ListJobsByRunRequest>,
    ) -> Result<Response<ListJobsByRunResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items = WorkflowQueryStore::scan_run_jobs(store.as_ref(), &req.project_id, &req.run_id)
            .await
            .map_err(engine_err_to_status)?;
        let jobs = items
            .iter()
            .filter_map(|d| prost::Message::decode(d.as_slice()).ok())
            .collect();
        Ok(Response::new(ListJobsByRunResponse { jobs }))
    }

    async fn list_jobs_by_batch(
        &self,
        request: Request<ListJobsByBatchRequest>,
    ) -> Result<Response<ListJobsByBatchResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items =
            WorkflowQueryStore::scan_batch_jobs(store.as_ref(), &req.project_id, &req.batch_id)
                .await
                .map_err(engine_err_to_status)?;
        let jobs = items
            .iter()
            .filter_map(|d| prost::Message::decode(d.as_slice()).ok())
            .collect();
        Ok(Response::new(ListJobsByBatchResponse { jobs }))
    }

    // --- Batches ---

    async fn get_batch(
        &self,
        request: Request<GetBatchRequest>,
    ) -> Result<Response<GetBatchResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match WorkflowQueryStore::load_batch(store.as_ref(), &req.project_id, &req.batch_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let batch = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode batch: {e}")))?;
                Ok(Response::new(GetBatchResponse {
                    batch: Some(batch),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetBatchResponse {
                batch: None,
                found: false,
            })),
        }
    }

    // --- Sessions ---

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match SessionQueryStore::load_session(store.as_ref(), &req.project_id, &req.session_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let session = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode session: {e}")))?;
                Ok(Response::new(GetSessionResponse {
                    session: Some(session),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetSessionResponse {
                session: None,
                found: false,
            })),
        }
    }

    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items = if !req.component_name.is_empty() {
            SessionQueryStore::scan_component_sessions(
                store.as_ref(),
                &req.project_id,
                &req.component_name,
            )
            .await
            .map_err(engine_err_to_status)?
        } else {
            // TODO: list all sessions (needs prefix scan on CF_SESSIONS by tenant)
            vec![]
        };
        let mut sessions: Vec<engine_proto::Session> = items
            .iter()
            .filter_map(|d| prost::Message::decode(d.as_slice()).ok())
            .collect();
        if !req.session_type.is_empty() {
            sessions.retain(|s| s.session_type == req.session_type);
        }
        Ok(Response::new(ListSessionsResponse { sessions }))
    }

    // --- Messages ---

    async fn get_message(
        &self,
        request: Request<GetMessageRequest>,
    ) -> Result<Response<GetMessageResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        match SessionQueryStore::load_message(store.as_ref(), &req.project_id, &req.message_id)
            .await
            .map_err(engine_err_to_status)?
        {
            Some(data) => {
                let message = prost::Message::decode(data.as_slice())
                    .map_err(|e| Status::internal(format!("decode message: {e}")))?;
                Ok(Response::new(GetMessageResponse {
                    message: Some(message),
                    found: true,
                }))
            }
            None => Ok(Response::new(GetMessageResponse {
                message: None,
                found: false,
            })),
        }
    }

    async fn list_messages_by_correlation(
        &self,
        request: Request<ListMessagesByCorrelationRequest>,
    ) -> Result<Response<ListMessagesByCorrelationResponse>, Status> {
        let req = request.into_inner();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
        let items = SessionQueryStore::scan_correlation_messages(
            store.as_ref(),
            &req.project_id,
            &req.correlation_id,
        )
        .await
        .map_err(engine_err_to_status)?;
        let messages = items
            .iter()
            .filter_map(|d| prost::Message::decode(d.as_slice()).ok())
            .collect();
        Ok(Response::new(ListMessagesByCorrelationResponse {
            messages,
        }))
    }

    async fn query(
        &self,
        request: Request<engine_proto::QueryRequest>,
    ) -> Result<Response<engine_proto::QueryResponse>, Status> {
        {
            let _ = request;
            Err(Status::unimplemented(
                "SQL is available only in the managed runtime",
            ))
        }
    }

    // --- Run listing ---

    async fn list_runs(
        &self,
        request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        // Hot ∪ cold merge served entirely from the
        // local node. The hot side scans `CF_HOT_INDEX` (active or
        // queued-not-yet-archived runs) and fetches each Run from
        // `CF_RUNS`. The cold side reads admitted Vortex winners
        // through the query lake. We dedup by `run_id` (a brief
        // overlap exists for a terminal run between PUT-success and
        // run.archived projection), sort newest-first by
        // (submitted_at_ms, run_id), and emit a cursor of the last
        // returned row.
        //
        // This list is eventually consistent:
        // eventually consistent (replication lag — typically a few ms
        // in healthy HA). Strong-consistency callers use
        // `GET /v1/runs/<id>` which routes to the partition leader.
        let req = request.into_inner();
        let cursor = if req.cursor.is_empty() {
            None
        } else {
            Some(req.cursor.clone())
        };
        // `service_name` is a deprecated synonym
        // for `component_name`. If both are set, an explicit
        // `component_name` wins (caller knows which one they mean);
        // otherwise route `service_name` through the same predicate.
        // The "service_name vs component_name" design question is
        // open (README); aliasing keeps API stable while we sort it.
        //
        // Warn when both are set and disagree — likely a caller bug,
        // not a deliberate combination, and silently picking one of
        // them would cost a debugging session.
        if let (Some(ref c), Some(ref s)) = (&req.component_name, &req.service_name) {
            if c != s {
                tracing::warn!(
                    component_name = %c,
                    service_name = %s,
                    "ListRunsRequest has conflicting component_name and service_name; using component_name"
                );
            }
        }
        let component_name = req
            .component_name
            .clone()
            .or_else(|| req.service_name.clone());
        engine_core::run_filter::validate_duration_window(req.min_duration_ms, req.max_duration_ms)
            .map_err(Status::invalid_argument)?;

        #[allow(deprecated)]
        let filter = RunListFilter {
            workspace_id: req.workspace_id.clone(),
            project_id: req.project_id.clone(),
            deployment_id: req.deployment_id.clone(),
            deployment_ids: engine_core::run_filter::normalize_deployment_ids(
                req.deployment_ids.iter().map(String::as_str),
            )
            .map_err(Status::invalid_argument)?,
            statuses: engine_core::run_filter::normalize_statuses(req.status.as_deref())
                .map_err(Status::invalid_argument)?,
            component_name,
            component_names: engine_core::run_filter::normalize_component_names(
                req.component_names.iter().map(String::as_str),
            )
            .map_err(Status::invalid_argument)?,
            component_type: req.component_type.clone(),
            min_duration_ms: req.min_duration_ms,
            max_duration_ms: req.max_duration_ms,
            since_ms: req.since_ms,
            until_ms: req.until_ms,
            limit: req.limit,
            offset: req.offset,
            cursor,
            session_id: req.session_id.clone(),
            parent_run_id: req.parent_run_id.clone(),
            trigger_type: req.trigger_type.clone(),
            mcp_server: req.mcp_server.clone(),
        };

        // Compute lag against `now` before we run
        // the merge so the reported value reflects the client's view
        // of staleness when they read the response, not after the
        // merge work. This is conservative (lag is what was true at
        // the start of the call) but matches what dashboards want.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let replication_lag_ms = self.lag_clock.lag_ms(now_ms);

        // If the operator opted in to the staleness
        // cap and we're above it, return a failed_precondition rather
        // than serve stale data. Clients can retry or escalate to a
        // leader-routed read. 0 disables the cap (default).
        if self.lag_failsafe_ms > 0 && replication_lag_ms > self.lag_failsafe_ms {
            return Err(Status::failed_precondition(format!(
                "list_runs replication lag {replication_lag_ms}ms exceeds failsafe \
                 {lag_failsafe}ms; retry or use GET /v1/runs/<id> for read-after-write",
                lag_failsafe = self.lag_failsafe_ms,
            )));
        }

        let read_archive = archive_can_contribute(
            &filter,
            self.store.is_some(),
            now_ms,
            self.hot_runs_complete_window_ms,
        );
        let result = list_runs_merged_with_hot_window(
            self.store.as_ref(),
            self.query_lake.as_ref().filter(|_| read_archive),
            &filter,
            self.hot_runs_complete_window_ms
                .map(|window| now_ms.saturating_sub(window)),
        )
        .await?;

        #[allow(deprecated)]
        let resp = ListRunsResponse {
            items: result.items,
            total_count: -1, // deprecated sentinel — clients use has_more.
            limit: result.limit,
            offset: result.offset,
            next_offset: result.next_offset,
            has_more: result.has_more,
            next_cursor: result.next_cursor,
            replication_lag_ms,
        };
        Ok(Response::new(resp))
    }

    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        // Hot-only, served from the local node's events hot index
        // (`CF_EVENTS_HOT`) — same per-node read-model rationale as
        // `list_runs`. No cold tier for events yet.
        let req = request.into_inner();
        let result = list_events_hot(self.store.as_ref(), &req)
            .await
            .map_err(|e| Status::internal(format!("list_events: {e}")))?;
        let limit = if req.limit > 0 { req.limit } else { 50 };
        Ok(Response::new(ListEventsResponse {
            items: result.items,
            limit,
            has_more: result.has_more,
            next_cursor: result.next_cursor,
        }))
    }

    async fn get_run_stats(
        &self,
        request: Request<GetRunStatsRequest>,
    ) -> Result<Response<GetRunStatsResponse>, Status> {
        {
            let _ = request;
            return Ok(Response::new(GetRunStatsResponse::default()));
        }
    }

    // --- Admin ---

    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        // What makes this call fail is a node-wide fault, not a partition-scoped
        // one. One fenced partition out of eight leaves the node able to serve
        // the other seven; reporting that as unavailable takes the whole node
        // out of rotation, which is the failure BOOT-201 exists to remove.
        // Per-partition detail is served by /readyz, which reads the partition
        // channels directly rather than inferring them from this call.
        let readiness = self.segment.partition_readiness().await;
        if !readiness.is_empty() && readiness.iter().all(|entry| !entry.is_ready()) {
            let detail = readiness
                .iter()
                .filter_map(|entry| {
                    entry
                        .failure
                        .as_ref()
                        .map(|failure| format!("{}:{failure}", entry.partition_id))
                })
                .collect::<Vec<_>>()
                .join(",");
            return Err(Status::unavailable(format!(
                "no partition can serve: {detail}"
            )));
        }
        if let Some(health) = self.processor_health.as_ref() {
            if health.has_changed().is_err() {
                return Err(Status::unavailable(
                    "partition processor supervisor channel closed",
                ));
            }
            let snapshot = health.borrow();
            if !snapshot.partitions.is_empty()
                && snapshot.partitions.iter().all(|status| !status.is_ready())
            {
                let unavailable = snapshot
                    .partitions
                    .iter()
                    .map(|status| {
                        format!(
                            "{}:{:?}:failures={}",
                            status.partition_id, status.state, status.consecutive_failures
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                return Err(Status::unavailable(format!(
                    "no partition processor is ready: {unavailable}"
                )));
            }
        }

        let partition_count = self.segment.partition_count();
        let mut tail_offset = 0u64;
        for partition in 0..partition_count {
            tail_offset = tail_offset.max(
                self.segment
                    .partition_tail_offset(partition)
                    .await
                    .map_err(engine_err_to_status)?
                    .0,
            );
        }
        let processor_offset = if let Some(ref store) = self.store {
            let mut minimum = None;
            for partition in 0..partition_count {
                let checkpoint = ProjectionCheckpointStore::load_projection_checkpoint(
                    store.as_ref(),
                    &format!("processor-partition-{partition}"),
                )
                .await
                .map_err(engine_err_to_status)?;
                let local = match checkpoint {
                    Some(offset) => {
                        let (encoded_partition, local) =
                            decode_offset_for_partition_count(offset, partition_count)
                                .map_err(engine_err_to_status)?;
                        if encoded_partition != partition {
                            return Err(Status::failed_precondition(format!(
                                "processor-partition-{partition} checkpoint encodes partition {encoded_partition}"
                            )));
                        }
                        local
                    }
                    None => 0,
                };
                minimum = Some(minimum.map_or(local, |current: u64| current.min(local)));
            }
            minimum.unwrap_or(0)
        } else {
            0
        };

        Ok(Response::new(GetStatusResponse {
            node_id: self.node_id.clone(),
            role: "standalone".to_string(),
            tail_offset,
            processor_offset,
            active_segments: 1,
            cache_entries: self.cache.run_count() as i64,
            cache_bytes: self.cache.size_bytes() as i64,
            uptime_seconds: self.start_time.elapsed().as_secs() as i64,
            partition_readiness: readiness
                .iter()
                .map(|entry| engine_proto::PartitionStorageReadiness {
                    partition_id: entry.partition_id,
                    ready: entry.is_ready(),
                    failure: entry.failure.clone().unwrap_or_default(),
                })
                .collect(),
        }))
    }
}

#[cfg(test)]
mod late_event_tests {
    use super::should_drop_after_terminal;

    #[test]
    fn progress_reports_after_a_runs_end_are_dropped() {
        assert!(should_drop_after_terminal("progress.update"));
        assert!(should_drop_after_terminal("function.completed"));
        assert!(!should_drop_after_terminal("run.cancelled"));
        assert!(!should_drop_after_terminal("log.info"));
    }
}
#[cfg(test)]
mod external_worker_authority_tests {
    use super::*;

    fn identity() -> ExternalWorkerIdentity {
        ExternalWorkerIdentity {
            workspace_id: "workspace-1".to_string(),
            project_id: "project-1".to_string(),
            environment_id: "environment-1".to_string(),
            deployment_id: "deployment-1".to_string(),
            worker_pool_id: "external-deployment-1".to_string(),
            worker_id: None,
            authority: None,
            expires_at_unix: i64::MAX,
        }
    }

    #[test]
    fn legacy_requests_without_external_identity_remain_unchanged() {
        assert!(authorize_external_project(None, "any-project").is_ok());
        assert!(authorize_external_deployment(None, "any-project", "any-deployment").is_ok());
    }

    #[test]
    fn external_authority_rejects_cross_project_and_cross_deployment_requests() {
        let identity = identity();
        assert!(
            authorize_external_deployment(Some(&identity), "project-1", "deployment-1").is_ok()
        );

        let project_error = authorize_external_project(Some(&identity), "project-2")
            .expect_err("cross-project authority must reject");
        assert_eq!(project_error.code(), tonic::Code::PermissionDenied);

        let deployment_error =
            authorize_external_deployment(Some(&identity), "project-1", "deployment-2")
                .expect_err("cross-deployment authority must reject");
        assert_eq!(deployment_error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn external_stream_authority_expires_with_its_workload_token() {
        let mut identity = identity();
        identity.expires_at_unix = 0;
        let error = ensure_external_identity_not_expired(Some(&identity))
            .expect_err("expired stream authority must reject");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert!(ensure_external_identity_not_expired(None).is_ok());
    }

    #[test]
    fn long_lived_worker_authority_is_rechecked_at_work_boundaries() {
        #[derive(Debug)]
        struct Authority(std::sync::atomic::AtomicBool);
        impl engine_core::workload_identity::ExternalWorkerAuthority for Authority {
            fn is_current(&self) -> bool {
                self.0.load(std::sync::atomic::Ordering::Acquire)
            }
        }
        let authority = Arc::new(Authority(std::sync::atomic::AtomicBool::new(true)));
        let mut identity = identity();
        identity.authority = Some(authority.clone());
        assert!(ensure_external_identity_not_expired(Some(&identity)).is_ok());
        authority
            .0
            .store(false, std::sync::atomic::Ordering::Release);
        assert_eq!(
            ensure_external_identity_not_expired(Some(&identity))
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        assert!(authorize_external_project(Some(&identity), "project-1").is_err());
    }

    #[test]
    fn external_workers_cannot_bypass_pull_sessions_with_push_renewal() {
        let identity = identity();
        let error = authorize_external_lease_renewal_mode(Some(&identity), LeaseRenewalMode::Push)
            .expect_err("external push renewal must reject");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            authorize_external_lease_renewal_mode(Some(&identity), LeaseRenewalMode::Pull,).is_ok()
        );
        assert!(authorize_external_lease_renewal_mode(None, LeaseRenewalMode::Push).is_ok());
    }
}
#[cfg(test)]
mod run_filter_tests {
    use super::*;

    #[test]
    fn hot_runs_filter_by_trigger_and_mcp_server() {
        let run = |trigger: Option<&str>, server: Option<&str>| {
            let mut run = engine_proto::Run::default();
            if let Some(trigger) = trigger {
                run.metadata.insert("trigger_type".into(), trigger.into());
            }
            if let Some(server) = server {
                run.metadata.insert("mcp.server".into(), server.into());
            }
            run
        };
        let filter = |trigger: Option<&str>, server: Option<&str>| RunListFilter {
            trigger_type: trigger.map(str::to_string),
            mcp_server: server.map(str::to_string),
            ..Default::default()
        };
        let smoke = run(Some("mcp"), Some("smoke"));
        let nightly = run(Some("cron"), None);
        assert!(run_matches_filter(&smoke, &filter(Some("mcp"), None)));
        assert!(run_matches_filter(
            &smoke,
            &filter(Some("mcp"), Some("smoke"))
        ));
        assert!(!run_matches_filter(&smoke, &filter(None, Some("billing"))));
        assert!(!run_matches_filter(&nightly, &filter(Some("mcp"), None)));
        assert!(run_matches_filter(&nightly, &filter(None, None)));
    }
}

#[cfg(test)]
mod internal_event_admission_tests {
    use super::*;
    use std::collections::HashMap;

    fn record(event_type: &str) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "project-1".to_string(),
            run_id: "run-1".to_string(),
            event_type: event_type.to_string(),
            data: bytes::Bytes::new(),
            timestamp_ns: 1,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    #[test]
    fn terminal_reservations_are_runtime_internal() {
        let error = reject_external_internal_event(&record("run.terminalizing"), false)
            .expect_err("public terminal reservation must reject");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("run.terminalizing"));
        assert!(reject_external_internal_event(&record("run.completed"), false).is_ok());

        let mut reserved_outcome = record("run.failed");
        reserved_outcome.metadata.insert(
            "terminal_reservation_id".to_string(),
            "reservation-1".to_string(),
        );
        assert!(reject_external_internal_event(&reserved_outcome, false).is_err());
        assert!(reject_external_internal_event(
            &record(engine_core::journal_contract::DISPATCH_RETRY_SCHEDULED_EVENT),
            false
        )
        .is_err());
        assert!(reject_external_internal_event(&record("run.assigned"), false).is_err());
        assert!(reject_external_internal_event(&record("run.assigned"), true).is_ok());
    }
}

#[cfg(test)]
mod peer_runtime_mark_tests {
    use super::*;

    fn request_with_peer_mark() -> Request<()> {
        let mut request = Request::new(());
        let token = engine_core::lease_fence::runtime_peer_token(&worker_session_signing_key());
        request.metadata_mut().insert(
            engine_core::lease_fence::RUNTIME_AUTHORED_HEADER,
            token.parse().expect("peer token is valid metadata"),
        );
        request
    }

    #[test]
    fn peer_mark_counts_only_when_peers_are_accepted() {
        let request = request_with_peer_mark();
        assert!(runtime_authored_request(&request, true));
        assert!(!runtime_authored_request(&request, false));
    }

    #[test]
    fn in_process_extension_counts_without_peers() {
        let mut request = Request::new(());
        request.extensions_mut().insert(RuntimeAuthored);
        assert!(runtime_authored_request(&request, false));
    }
}
