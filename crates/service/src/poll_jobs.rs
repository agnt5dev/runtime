//! Pull-worker polling and completion transaction preparation.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use engine_core::demand::{component_type_from_run_label, DemandKey};
use engine_core::dispatch_generation::resolve_projected_dispatch_generation;
use engine_core::error::EngineError;
use engine_core::journal_contract::{
    META_PULL_COMPLETION_LIFECYCLE, META_PULL_COMPLETION_LIFECYCLE_INDEX,
};
use engine_core::lease_fence::{
    strip_reserved_fence_metadata, LeaseFence, LeaseFenceKind, RUN_LEASE_RENEWED_EVENT,
    RUN_META_DISPATCH_GENERATION, RUN_META_GRANT_ID, RUN_META_GRANT_REQUEST_FINGERPRINT,
    RUN_META_LEASE_ATTEMPT, RUN_META_LEASE_SESSION_HASH,
};
use engine_core::pull_dispatch::{
    PollFailure, PollFailureKind, PollOutcome, PollRejection, PullDispatch, VerifiedPoll,
};
use engine_core::storage_contract::{PendingRunCursor, PendingWorkStore, RunQueryStore};
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record, MAX_JOURNAL_RECORD_BYTES};
use engine_core::workload_identity::ExternalWorkerIdentity;
use engine_proto::{
    ComponentRouteSpec, ComponentType, JobAssignment, ReportWorkerCapacityRequest, Run,
};
use runtime_common::error::DataplaneError;
use tonic::Status;

use super::pull_workers::{
    lease_expiry_is_active, AssignedLease, CachedLease, CommittedAssignmentReceipt,
    PullWakeContext, PullWakeSource, WorkerSession,
};
use super::{
    engine_err_to_status, ensure_external_identity_not_expired, now_ms, EngineServiceImpl,
    PullAssignmentGateDecision, DEFAULT_LEASE_TIMEOUT_MS, POLL_JOB_CANDIDATE_MAX_PAGES,
    POLL_JOB_CANDIDATE_SCAN_LIMIT, POLL_JOB_RECHECK_INTERVAL_MS,
    PROJECTION_PUBLICATION_POLL_INTERVAL_MS,
};

use async_trait::async_trait;

/// Upper bounds for lifecycle records carried inside one `CompleteJob`.
pub(super) const MAX_COMPLETION_LIFECYCLE_RECORDS: usize = 32;
pub(super) const MAX_COMPLETION_LIFECYCLE_BYTES: usize = 256 * 1024;
/// A worker clock may run ahead of the runtime's; beyond this the record is
/// treated as malformed rather than silently reordering run history.
const MAX_COMPLETION_LIFECYCLE_SKEW_NS: i64 = 60_000_000_000;

pub(super) enum ParkedPollResult {
    Assigned(Box<JobAssignment>),
    DeadlineExpired,
    SessionInactive,
    AuthorizationExpired,
    ShuttingDown,
    Failed(Status),
}

struct LegacyPollSelection {
    assignment: JobAssignment,
}

pub(super) struct ServicePullDispatch<'a, S: Segment> {
    service: &'a EngineServiceImpl<S>,
    session: WorkerSession,
    external_identity: Option<ExternalWorkerIdentity>,
}

impl<'a, S: Segment> ServicePullDispatch<'a, S> {
    pub(super) fn new(
        service: &'a EngineServiceImpl<S>,
        session: WorkerSession,
        external_identity: Option<ExternalWorkerIdentity>,
    ) -> Self {
        Self {
            service,
            session,
            external_identity,
        }
    }
}

#[async_trait]
impl<S: Segment> PullDispatch for ServicePullDispatch<'_, S> {
    async fn poll(&self, poll: VerifiedPoll, deadline: Instant) -> PollOutcome {
        if poll.worker_id() != self.session.worker_id
            || poll.worker_session_id() != self.session.session_id
            || poll.project_id() != self.session.project_id
            || poll.deployment_id() != self.session.deployment_id
        {
            return PollOutcome::Rejected(PollRejection::SessionInactive);
        }
        match self
            .service
            .park_verified_poll(
                &self.session,
                self.external_identity.as_ref(),
                poll.demand_keys(),
                poll.claim_timeout(),
                deadline,
            )
            .await
        {
            ParkedPollResult::Assigned(job) => PollOutcome::Assigned(job),
            ParkedPollResult::DeadlineExpired => PollOutcome::DeadlineExpired,
            ParkedPollResult::SessionInactive => {
                PollOutcome::Rejected(PollRejection::SessionInactive)
            }
            ParkedPollResult::AuthorizationExpired => {
                PollOutcome::Rejected(PollRejection::AuthorizationExpired)
            }
            ParkedPollResult::ShuttingDown => PollOutcome::Rejected(PollRejection::ShuttingDown),
            ParkedPollResult::Failed(status) => PollOutcome::Rejected(PollRejection::Failure(
                PollFailure::new(poll_failure_kind(status.code()), status.message()),
            )),
        }
    }

    fn has_ready_supply(&self, demand_keys: &BTreeSet<DemandKey>) -> Option<bool> {
        let _ = demand_keys;
        None
    }
}

fn poll_failure_kind(code: tonic::Code) -> PollFailureKind {
    match code {
        tonic::Code::Cancelled => PollFailureKind::Cancelled,
        tonic::Code::Unknown => PollFailureKind::Unknown,
        tonic::Code::InvalidArgument => PollFailureKind::InvalidArgument,
        tonic::Code::DeadlineExceeded => PollFailureKind::DeadlineExceeded,
        tonic::Code::NotFound => PollFailureKind::NotFound,
        tonic::Code::AlreadyExists => PollFailureKind::AlreadyExists,
        tonic::Code::PermissionDenied => PollFailureKind::PermissionDenied,
        tonic::Code::ResourceExhausted => PollFailureKind::ResourceExhausted,
        tonic::Code::FailedPrecondition => PollFailureKind::FailedPrecondition,
        tonic::Code::Aborted => PollFailureKind::Aborted,
        tonic::Code::OutOfRange => PollFailureKind::OutOfRange,
        tonic::Code::Unimplemented => PollFailureKind::Unimplemented,
        tonic::Code::Internal => PollFailureKind::Internal,
        tonic::Code::Unavailable => PollFailureKind::Unavailable,
        tonic::Code::DataLoss => PollFailureKind::DataLoss,
        tonic::Code::Unauthenticated => PollFailureKind::Unauthenticated,
        tonic::Code::Ok => PollFailureKind::Unknown,
    }
}

impl<S: Segment> EngineServiceImpl<S> {
    /// Validate and stamp the lifecycle records a pull worker delivered inside
    /// `CompleteJob`. Pure apart from scope validation: nothing is appended.
    ///
    /// Each record must belong to this project and run, be a worker-owned
    /// non-terminal lifecycle or handler event, and carry a timestamp that
    /// does not go backwards within the batch or run ahead of receipt by more
    /// than the skew bound. The lease fence already proven for the completion
    /// covers the batch, so per-record mutation authority is not rechecked.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn validate_completion_lifecycle_records(
        &self,
        records: Vec<engine_proto::Record>,
        project_id: &str,
        run_id: &str,
        run: &Run,
        worker_id: &str,
        worker_session_id: &str,
        lease_id: &str,
        attempt: u32,
        received_at_ms: i64,
    ) -> Result<Vec<Record>, Status> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        if records.len() > MAX_COMPLETION_LIFECYCLE_RECORDS {
            return Err(Status::invalid_argument(format!(
                "CompleteJob carries {} lifecycle records; the limit is {MAX_COMPLETION_LIFECYCLE_RECORDS}",
                records.len()
            )));
        }
        let received_at_ns = received_at_ms.saturating_mul(1_000_000);
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let mut validated = Vec::with_capacity(records.len());
        let mut total_bytes = 0usize;
        let mut previous_timestamp_ns = 0i64;
        for (index, proto) in records.into_iter().enumerate() {
            let mut record = Record::from_proto(proto);
            if record.event_type.is_empty() {
                return Err(Status::invalid_argument(
                    "CompleteJob lifecycle record requires an event_type",
                ));
            }
            if record.project_id != project_id || record.run_id != run_id {
                return Err(Status::failed_precondition(format!(
                    "CompleteJob lifecycle record {} targets {}/{} instead of the completing run",
                    record.event_type, record.project_id, record.run_id
                )));
            }
            if !completion_lifecycle_event_allowed(&record) {
                return Err(Status::invalid_argument(format!(
                    "{} cannot be carried inside CompleteJob; only worker-owned non-terminal lifecycle and handler events may",
                    record.event_type
                )));
            }
            super::reject_external_internal_event(&record, false)?;
            self.validate_record_scope(&record)?;
            if record.data.len() > MAX_JOURNAL_RECORD_BYTES {
                return Err(Status::invalid_argument(format!(
                    "CompleteJob lifecycle record {} exceeds {MAX_JOURNAL_RECORD_BYTES} bytes",
                    record.event_type
                )));
            }
            total_bytes = total_bytes
                .saturating_add(record.data.len())
                .saturating_add(
                    record
                        .metadata
                        .iter()
                        .map(|(key, value)| key.len() + value.len())
                        .sum::<usize>(),
                );
            if total_bytes > MAX_COMPLETION_LIFECYCLE_BYTES {
                return Err(Status::invalid_argument(format!(
                    "CompleteJob lifecycle records exceed {MAX_COMPLETION_LIFECYCLE_BYTES} bytes"
                )));
            }

            strip_reserved_fence_metadata(&mut record.metadata);
            for key in super::COMPLETION_RESERVED_METADATA_KEYS {
                record.metadata.remove(*key);
            }
            record.metadata.remove(META_PULL_COMPLETION_LIFECYCLE);
            record.metadata.remove(META_PULL_COMPLETION_LIFECYCLE_INDEX);
            record
                .metadata
                .insert("deployment_id".to_string(), run.deployment_id.clone());
            if !run.component_name.is_empty() {
                record
                    .metadata
                    .insert("component_name".to_string(), run.component_name.clone());
            }
            if !run.component_type.is_empty() {
                record
                    .metadata
                    .insert("component_type".to_string(), run.component_type.clone());
            }
            record
                .metadata
                .insert("worker_id".to_string(), worker_id.to_string());
            // The HA path appends through the partition owner's AppendBatch
            // handler, whose worker-mutation authority check requires the
            // session as well as the lease.
            record.metadata.insert(
                "worker_session_id".to_string(),
                worker_session_id.to_string(),
            );
            record
                .metadata
                .insert("lease_id".to_string(), lease_id.to_string());
            record
                .metadata
                .insert(RUN_META_LEASE_ATTEMPT.to_string(), attempt.to_string());
            record
                .metadata
                .insert("dispatch_mode".to_string(), "pull".to_string());
            record
                .metadata
                .insert(META_PULL_COMPLETION_LIFECYCLE.to_string(), "1".to_string());
            record.metadata.insert(
                META_PULL_COMPLETION_LIFECYCLE_INDEX.to_string(),
                index.to_string(),
            );

            if record.timestamp_ns == 0 {
                record.timestamp_ns = now_ns.saturating_add(index as i64);
            }
            if record.timestamp_ns < previous_timestamp_ns {
                return Err(Status::invalid_argument(format!(
                    "CompleteJob lifecycle record {} timestamp precedes the previous record",
                    record.event_type
                )));
            }
            if record.timestamp_ns > received_at_ns.saturating_add(MAX_COMPLETION_LIFECYCLE_SKEW_NS)
            {
                return Err(Status::invalid_argument(format!(
                    "CompleteJob lifecycle record {} timestamp is too far ahead of receipt",
                    record.event_type
                )));
            }
            previous_timestamp_ns = record.timestamp_ns;
            record.offset = Offset::ZERO;
            if record.data_type.is_empty() {
                record.data_type = "json".to_string();
            }
            validated.push(record);
        }
        Ok(validated)
    }

    /// Append a pull completion as one atomic batch: the validated lifecycle
    /// records followed by the lease-fenced terminal. The segment decides the
    /// fence inside the same write, so a losing completion writes nothing,
    /// including its lifecycle records. Returns how many of the records were
    /// newly written; a retried CompleteJob resolves every record to its
    /// earlier offset through the idempotency slots and writes nothing.
    pub(super) async fn append_completion_batch(
        &self,
        records: Vec<Record>,
    ) -> Result<usize, Status> {
        if records.is_empty() {
            return Ok(0);
        }
        let submitted = records.len();
        let written = if let Some(backend) = self.completion_append_backend() {
            backend
                .append_batch_with_outcome(records)
                .await
                .map_err(Status::from)?
                .written_count
        } else {
            let outcome = self
                .segment
                .append_batch_with_outcome(&records)
                .await
                .map_err(engine_err_to_status)?;
            outcome
                .validate_cardinality(submitted, "pull completion append")
                .map_err(engine_err_to_status)?;
            let mut written = 0usize;
            for (mut record, (offset, appended)) in records
                .into_iter()
                .zip(outcome.offsets.iter().zip(outcome.appended.iter()))
            {
                if !*appended {
                    continue;
                }
                written += 1;
                record.offset = *offset;
                self.observe_queue_metrics_for_record(&record);
                self.cache.put(Arc::new(record));
            }
            written
        };
        Ok(written)
    }
}

/// Event types a worker may fold into `CompleteJob`: worker-owned lifecycle
/// and handler events that are not terminal, not another `run.*` transition,
/// not runtime-internal, and not part of a stricter contract (activations,
/// entity state) that has its own RPC.
fn completion_lifecycle_event_allowed(record: &Record) -> bool {
    let event_type = record.event_type.as_str();
    // Hosted Python and TypeScript agents emit session.created to materialize
    // their conversation session. It is ordinary bookkeeping accepted by
    // Append, and may follow agent.completed in the held lifecycle queue.
    // Keep the exact event exception local to bundling: session updates and
    // other runtime-owned transitions retain their existing admission rules.
    (super::is_worker_owned_mutation(event_type) || event_type == "session.created")
        && (!event_type.starts_with("run.") || event_type == "run.started")
        && !matches!(event_type, "workflow.paused" | "workflow.state.changed")
        && !engine_core::activation::has_activation_marker(record)
        && !engine_core::journal_contract::is_runtime_internal_event(record)
        && super::terminal_status_for_record(record).is_none()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeaseRenewalMode {
    Push,
    Pull,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PullProbeSource {
    Initial,
    Notification(PullWakeSource),
    InvalidatedProbe,
    FallbackProbe,
}

impl PullProbeSource {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Notification(source) => source.label(),
            Self::InvalidatedProbe => "invalidated_probe",
            Self::FallbackProbe => "fallback_probe",
        }
    }
}

fn wake_match_label(wake: Option<&PullWakeContext>, run_id: &str) -> &'static str {
    match wake.and_then(|context| context.queued_run_id.as_deref()) {
        Some(queued_run_id) if queued_run_id == run_id => "same_run",
        Some(_) => "different_run",
        None => "none",
    }
}

fn pull_dispatch_generation(run: &Run) -> String {
    resolve_projected_dispatch_generation(&run.metadata, run.attempt)
        .generation
        .into_string()
}

pub(super) struct PullAssignmentIdentity {
    pub(super) lease_id: String,
    pub(super) grant_id: String,
    pub(super) grant_request_fingerprint: Option<String>,
}

pub(super) fn assignment_commit_offset(
    metadata: &std::collections::HashMap<String, String>,
) -> Option<Offset> {
    metadata
        .get(engine_core::journal_contract::META_ASSIGNMENT_COMMIT_OFFSET)
        .and_then(|value| value.parse::<u64>().ok())
        .map(Offset)
}

impl<S: Segment> EngineServiceImpl<S> {
    /// Append a lease-backed `run.assigned` journal record for a run that was
    /// just claimed via parked `PollJob`. The record carries the
    /// lease metadata the projection needs to populate the lease fields on
    /// the Run proto and write the lease index CFs.
    ///
    /// `run` is the decoded Run proto from the pending index entry.
    ///
    /// Write-through to the record cache mirrors the `append` handler so
    /// subsequent reads on the same run see the event immediately.
    pub(super) async fn append_lease_assigned(
        &self,
        project_id: &str,
        run: &Run,
        session: &WorkerSession,
        lease_timeout_ms: i64,
    ) -> Result<CommittedAssignmentReceipt, EngineError> {
        self.append_lease_assigned_with_ids(
            project_id,
            run,
            session,
            PullAssignmentIdentity {
                lease_id: uuid::Uuid::now_v7().to_string(),
                grant_id: uuid::Uuid::now_v7().to_string(),
                grant_request_fingerprint: None,
            },
            lease_timeout_ms,
        )
        .await
    }

    pub(super) async fn append_lease_assigned_with_ids(
        &self,
        project_id: &str,
        run: &Run,
        session: &WorkerSession,
        identity: PullAssignmentIdentity,
        lease_timeout_ms: i64,
    ) -> Result<CommittedAssignmentReceipt, EngineError> {
        let PullAssignmentIdentity {
            lease_id,
            grant_id,
            grant_request_fingerprint,
        } = identity;
        let worker_id = session.worker_id.as_str();
        let session_hash = super::pull_workers::worker_session_hash(&session.session_id);
        let now_ms = now_ms();
        let now_ns = now_ms.saturating_mul(1_000_000);
        let normalized_timeout_ms = if lease_timeout_ms > 0 {
            lease_timeout_ms
        } else {
            DEFAULT_LEASE_TIMEOUT_MS
        };
        let expires_at_ms = now_ms.saturating_add(normalized_timeout_ms);

        let mut metadata = std::collections::HashMap::with_capacity(10);
        metadata.insert("lease_id".to_string(), lease_id.clone());
        metadata.insert("lease_worker_id".to_string(), worker_id.to_string());
        metadata.insert("lease_expires_at_ms".to_string(), expires_at_ms.to_string());
        metadata.insert(
            RUN_META_LEASE_SESSION_HASH.to_string(),
            session_hash.clone(),
        );
        metadata.insert(RUN_META_LEASE_ATTEMPT.to_string(), run.attempt.to_string());
        metadata.insert(
            RUN_META_DISPATCH_GENERATION.to_string(),
            pull_dispatch_generation(run),
        );
        metadata.insert(RUN_META_GRANT_ID.to_string(), grant_id);
        if let Some(fingerprint) = grant_request_fingerprint.filter(|value| !value.is_empty()) {
            metadata.insert(RUN_META_GRANT_REQUEST_FINGERPRINT.to_string(), fingerprint);
        }
        metadata.insert(
            "lease_timeout_ms".to_string(),
            normalized_timeout_ms.to_string(),
        );
        // Preserve component context so downstream projections and the
        // read path don't lose the run's routing when they rebuild state.
        if !run.component_name.is_empty() {
            metadata.insert("component_name".to_string(), run.component_name.clone());
        }
        if !run.deployment_id.is_empty() {
            metadata.insert("deployment_id".to_string(), run.deployment_id.clone());
        }
        if run.priority != 0 {
            metadata.insert("priority".to_string(), run.priority.to_string());
        }
        if let Ok(demand_key) = DemandKey::from_run(run) {
            if let Some(route) = self.worker_session_component_route(session, &demand_key) {
                if let Some(max_attempts) = route.config.get("max_attempts") {
                    metadata.insert("max_attempts".to_string(), max_attempts.clone());
                }
            }
        }

        let record = Record {
            offset: Offset(0),
            project_id: project_id.to_string(),
            run_id: run.run_id.clone(),
            event_type: "run.assigned".to_string(),
            data: bytes::Bytes::new(),
            timestamp_ns: now_ns,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };
        tracing::debug!(
            target: "agnt5.pull_queue_stage",
            stage = "assignment_constructed",
            run_id = %run.run_id,
            assigned_at_ms = now_ms,
            "PULL queue stage"
        );

        let append_started_at = Instant::now();
        let (offset, appended) = if let Some(backend) = self.completion_append_backend() {
            let outcome = match backend
                .append_batch_with_outcome(vec![record.clone()])
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.record_assignment_commit_duration("error", append_started_at);
                    return Err(match error {
                        DataplaneError::Engine(error) => error,
                        error => EngineError::Internal(error.to_string()),
                    });
                }
            };
            if outcome.offsets.len() != 1 || outcome.written_count > 1 {
                self.record_assignment_commit_duration("invalid_receipt", append_started_at);
                return Err(EngineError::Internal(format!(
                    "pull assignment receipt cardinality mismatch: offsets={}, written_count={}",
                    outcome.offsets.len(),
                    outcome.written_count
                )));
            }
            (outcome.offsets[0], outcome.written_count == 1)
        } else {
            let outcome = match self
                .segment
                .append_batch_with_outcome(std::slice::from_ref(&record))
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.record_assignment_commit_duration("error", append_started_at);
                    return Err(error);
                }
            };
            if let Err(error) = outcome.validate_cardinality(1, "pull assignment receipt") {
                self.record_assignment_commit_duration("invalid_receipt", append_started_at);
                return Err(error);
            }
            (outcome.offsets[0], outcome.appended[0])
        };

        // Only publish the exact record this call committed. A replayed receipt
        // already has an authoritative journal winner and may carry a different
        // timestamp than this reconstructed command.
        if appended {
            let mut cached = record;
            cached.offset = offset;
            self.cache.put(Arc::new(cached));
        }
        self.observe_queue_metrics_for_run(project_id, run);
        self.record_assignment_commit_duration(
            if appended { "fresh" } else { "replayed" },
            append_started_at,
        );
        let assignment_commit_ms = append_started_at.elapsed().as_secs_f64() * 1_000.0;

        tracing::debug!(
            target: "agnt5.pull_queue_stage",
            stage = "assignment_committed",
            project_id = %project_id,
            run_id = %run.run_id,
            worker_id = %worker_id,
            lease_id = %lease_id,
            expires_at_ms,
            assignment_commit_ms,
            appended,
            accepted_offset = offset.0,
            "PULL queue stage"
        );

        Ok(CommittedAssignmentReceipt {
            lease: AssignedLease {
                lease_id,
                lease_expires_at_ms: expires_at_ms,
                lease_timeout_ms: normalized_timeout_ms,
            },
            accepted_offset: offset,
            appended,
        })
    }

    pub(super) fn latest_cached_lifecycle_is_terminal(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> bool {
        let Some(records) = self.cache.get_by_run_id(project_id, run_id) else {
            return false;
        };

        records
            .iter()
            .rev()
            .find_map(|record| match record.event_type.as_str() {
                "run.queued" | "run.resumed" | "run.lease_expired" => Some(false),
                "run.completed" | "run.failed" | "run.cancelled" => Some(true),
                _ => None,
            })
            .unwrap_or(false)
    }

    pub(super) fn latest_cached_active_lease(
        &self,
        project_id: &str,
        run_id: &str,
        expected_attempt: u32,
    ) -> Option<CachedLease> {
        let records = self.cache.get_by_run_id(project_id, run_id)?;

        for record in records.iter().rev() {
            match record.event_type.as_str() {
                "run.assigned" => {
                    if record
                        .metadata
                        .get(RUN_META_LEASE_ATTEMPT)
                        .and_then(|value| value.parse::<u32>().ok())
                        .is_some_and(|attempt| attempt != expected_attempt)
                    {
                        // A queued retry can overtake a stale replica's pending
                        // scan. That replica may still append its old assignment,
                        // but it must not hide the current retry generation from
                        // subsequent pollers while projection rejects it.
                        return None;
                    }
                    let lease_id = record.metadata.get("lease_id")?.clone();
                    if lease_id.is_empty() {
                        return None;
                    }
                    let lease_expires_at_ms = record
                        .metadata
                        .get("lease_expires_at_ms")
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or_default();
                    return Some(CachedLease {
                        lease_expires_at_ms,
                    });
                }
                "run.queued" | "run.resumed" | "run.lease_expired" | "run.completed"
                | "run.failed" | "run.cancelled" => return None,
                _ => {}
            }
        }

        None
    }

    pub(super) fn record_poll_job_result(&self, result: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .poll_job_count
                .add(1, &[opentelemetry::KeyValue::new("result", result)]);
        }
    }

    pub(super) fn record_poll_job_waiter_event(&self, event: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .poll_job_waiter_events
                .add(1, &[opentelemetry::KeyValue::new("event", event)]);
        }
    }

    pub(super) fn record_pull_notification_to_probe(&self, wake: &PullWakeContext) {
        if let Some(metrics) = &self.metrics {
            metrics.pull_notification_to_probe_duration.record(
                wake.notified_at.elapsed().as_secs_f64() * 1_000.0,
                &[opentelemetry::KeyValue::new("source", wake.source.label())],
            );
        }
    }

    fn record_pull_gate_decision(
        &self,
        decision: PullAssignmentGateDecision,
        discovery: PullProbeSource,
        wake: Option<&PullWakeContext>,
        run_id: &str,
        submitted_at_ms: i64,
    ) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let labels = [
            opentelemetry::KeyValue::new("decision", decision.decision_label()),
            opentelemetry::KeyValue::new("role", decision.role_label()),
            opentelemetry::KeyValue::new("discovered_by", discovery.label()),
            opentelemetry::KeyValue::new("wake_match", wake_match_label(wake, run_id)),
        ];
        metrics.pull_assignment_gate_decisions.add(1, &labels);
        if decision.is_eligible() && submitted_at_ms > 0 {
            metrics
                .pull_queue_age_at_probe
                .record(now_ms().saturating_sub(submitted_at_ms) as f64, &labels);
        }
    }

    fn record_pull_candidate_selection(
        &self,
        decision: PullAssignmentGateDecision,
        discovery: PullProbeSource,
        wake: Option<&PullWakeContext>,
        run_id: &str,
        probe_started_at: Instant,
    ) {
        if let Some(metrics) = &self.metrics {
            metrics.pull_candidate_selection_duration.record(
                probe_started_at.elapsed().as_secs_f64() * 1_000.0,
                &[
                    opentelemetry::KeyValue::new("role", decision.role_label()),
                    opentelemetry::KeyValue::new("discovered_by", discovery.label()),
                    opentelemetry::KeyValue::new("wake_match", wake_match_label(wake, run_id)),
                ],
            );
        }
    }

    fn record_assignment_commit_duration(&self, result: &'static str, started_at: Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.assignment_commit_duration.record(
                started_at.elapsed().as_secs_f64() * 1000.0,
                &[opentelemetry::KeyValue::new("result", result)],
            );
        }
    }

    pub(super) fn record_renew_job_lease_result(&self, result: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .renew_job_lease_count
                .add(1, &[opentelemetry::KeyValue::new("result", result)]);
        }
    }

    pub(super) fn record_complete_job_rejected(&self, code: tonic::Code) {
        if let Some(metrics) = &self.metrics {
            metrics
                .complete_job_rejected
                .add(1, &[opentelemetry::KeyValue::new("code", code.to_string())]);
        }
    }

    pub(super) fn record_worker_capacity_result(&self, result: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .worker_capacity_reports
                .add(1, &[opentelemetry::KeyValue::new("result", result)]);
        }
    }

    pub(super) fn record_worker_capacity_gauges(
        &self,
        session: &WorkerSession,
        req: &ReportWorkerCapacityRequest,
        desired_slots: u32,
        effective_max_slots: u32,
    ) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let labels = [
            opentelemetry::KeyValue::new("agnt5.project.id", session.project_id.clone()),
            opentelemetry::KeyValue::new("agnt5.deployment.id", session.deployment_id.clone()),
            opentelemetry::KeyValue::new("worker_id", session.worker_id.clone()),
        ];
        metrics
            .worker_capacity_observed_at_ms
            .record(now_ms().max(0) as u64, &labels);
        metrics
            .worker_slots_open_polls
            .record(req.open_poll_slots as u64, &labels);
        metrics
            .worker_slots_active
            .record(req.active_slots as u64, &labels);
        metrics
            .worker_slots_desired
            .record(desired_slots as u64, &labels);
        metrics
            .worker_slots_effective_max
            .record(effective_max_slots as u64, &labels);
        if req.cpu_usage > 0.0 {
            metrics
                .worker_cpu_usage
                .record(req.cpu_usage.clamp(0.0, 1.0), &labels);
        }
        if req.memory_usage > 0.0 {
            metrics
                .worker_memory_usage
                .record(req.memory_usage.clamp(0.0, 1.0), &labels);
        }
    }

    pub(super) fn notify_if_queue_affecting_record(&self, record: &Record) {
        if matches!(
            record.event_type.as_str(),
            "run.queued" | "run.resumed" | "run.lease_expired"
        ) {
            if let Some(deployment_id) = record.metadata.get("deployment_id") {
                let context = PullWakeContext::queue_event(
                    PullWakeSource::RequestAppend,
                    &record.run_id,
                    record.timestamp_ns / 1_000_000,
                );
                let event =
                    if self
                        .poll_waiters
                        .notify_one(&record.project_id, deployment_id, context)
                    {
                        "request_wakeup"
                    } else {
                        "request_no_waiter"
                    };
                self.record_poll_job_waiter_event(event);
            }
        }
    }

    /// Wake one parked pull slot after the active partition owner has made a
    /// queue transition visible in its materialized projection.
    ///
    /// This complements the request-local append wakeup above: an ingress node
    /// is not necessarily the HA owner allowed to assign the run. The periodic
    /// queue probe remains the repair path when no matching waiter is parked.
    pub fn notify_pull_queue_ready(
        &self,
        project_id: &str,
        deployment_id: &str,
        run_id: &str,
        queued_at_ms: i64,
        projected_at: Instant,
    ) -> bool {
        let projection_to_notification_ms = projected_at.elapsed().as_secs_f64() * 1_000.0;
        let context =
            PullWakeContext::queue_event(PullWakeSource::Projection, run_id, queued_at_ms);
        let woke_waiter = self
            .poll_waiters
            .notify_one(project_id, deployment_id, context);
        let event = if woke_waiter {
            "projection_wakeup"
        } else {
            "projection_no_waiter"
        };
        self.record_poll_job_waiter_event(event);
        if let Some(metrics) = &self.metrics {
            metrics.pull_projection_to_notification_duration.record(
                projection_to_notification_ms,
                &[opentelemetry::KeyValue::new("result", event)],
            );
        }
        tracing::debug!(
            target: "agnt5.pull_queue_stage",
            stage = "notification_attempted",
            run_id,
            queued_at_ms,
            projection_to_notification_ms,
            result = event,
            "PULL queue stage"
        );
        woke_waiter
    }

    /// Accept an advisory exact-run hint from the active owner. The gate later
    /// validates target, owner epoch, partition, and expiry before it can
    /// bypass follower grace.
    pub fn notify_delegated_pull_ready(&self, delegation: engine_proto::PullReadyDelegation) {
        let run_id = delegation.run_id.clone();
        let ready_at_ms = delegation.ready_at_ms;
        let target = delegation.delegate_coordinator_id.clone();
        let event = if self.poll_waiters.notify_delegated(delegation) {
            "delegation_wakeup"
        } else {
            "delegation_stored"
        };
        self.record_poll_job_waiter_event(event);
        tracing::debug!(
            target: "agnt5.pull_queue_stage",
            stage = "delegation_received",
            run_id,
            ready_at_ms,
            delegate_coordinator_id = target,
            result = event,
            "PULL queue stage"
        );
    }

    /// Resolve the component route from the just-committed owner projection.
    /// Requeue transition records do not always repeat component metadata, so
    /// delegation falls back to the materialized Run only on the no-waiter
    /// path rather than adding a read to every queue transition.
    pub async fn projected_pull_route(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Option<(String, i32)> {
        let store = self.store.as_ref()?;
        let data = RunQueryStore::load_run(store.as_ref(), project_id, run_id)
            .await
            .ok()??;
        let run = <Run as prost::Message>::decode(data.as_slice()).ok()?;
        let demand_key = DemandKey::from_run(&run).ok()?;
        Some((
            demand_key.component_name().to_string(),
            demand_key.component_type_value(),
        ))
    }

    pub(super) fn build_job_assignment(
        run: &Run,
        committed_lease: Option<(&AssignedLease, Offset)>,
        component_route: Option<&ComponentRouteSpec>,
    ) -> JobAssignment {
        let component_type =
            component_type_from_run_label(&run.component_type).unwrap_or(ComponentType::Function);
        let mut metadata = run.metadata.clone();
        if let Some(component_route) = component_route {
            for key in [
                "max_attempts",
                "initial_interval_ms",
                "max_interval_ms",
                "backoff_type",
                "backoff_multiplier",
            ] {
                if let Some(value) = component_route.config.get(key) {
                    metadata.insert(key.to_string(), value.clone());
                }
            }
        }
        let attempt = i32::try_from(run.attempt).unwrap_or(i32::MAX);
        metadata.insert("attempt".to_string(), run.attempt.to_string());
        let (lease_id, lease_expires_at_ms) =
            if let Some((lease, accepted_offset)) = committed_lease {
                metadata.insert("lease_id".to_string(), lease.lease_id.clone());
                metadata.insert(
                    "lease_expires_at_ms".to_string(),
                    lease.lease_expires_at_ms.to_string(),
                );
                metadata.insert(
                    "lease_timeout_ms".to_string(),
                    lease.lease_timeout_ms.to_string(),
                );
                metadata.insert(
                    engine_core::journal_contract::META_ASSIGNMENT_COMMIT_OFFSET.to_string(),
                    accepted_offset.0.to_string(),
                );
                (lease.lease_id.clone(), lease.lease_expires_at_ms)
            } else {
                (String::new(), 0)
            };

        JobAssignment {
            job_id: run.run_id.clone(),
            run_id: run.run_id.clone(),
            component_id: String::new(),
            component_type: component_type.into(),
            component_name: run.component_name.clone(),
            input_data: run.input_data.clone(),
            metadata,
            attempt,
            timeout_ms: 0,
            trace_id: run.trace_id.clone(),
            lease_id,
            lease_expires_at_ms,
        }
    }

    /// Stamp the authenticated PollJob caller onto the transport-only job
    /// assignment. These values must override queued run metadata and must not
    /// be written into `run.assigned`: the signed worker session is authority
    /// for the current delivery, not durable run data.
    pub(super) fn stamp_pull_execution_authority(job: &mut JobAssignment, session: &WorkerSession) {
        job.metadata
            .insert("dispatch_mode".to_string(), "pull".to_string());
        job.metadata
            .insert("worker_id".to_string(), session.worker_id.clone());
        job.metadata
            .insert("worker_session_id".to_string(), session.session_id.clone());
        job.metadata
            .insert("lease_id".to_string(), job.lease_id.clone());
        job.metadata
            .insert(RUN_META_LEASE_ATTEMPT.to_string(), job.attempt.to_string());
    }

    /// Shared parking implementation for managed demand dispatch and the OSS
    /// compatibility path. One attempt guard spans every waiter registration
    /// and fallback probe made by this public request.
    pub(super) async fn park_verified_poll(
        &self,
        session: &WorkerSession,
        external_identity: Option<&ExternalWorkerIdentity>,
        demand_keys: &BTreeSet<DemandKey>,
        claim_timeout: Duration,
        deadline: Instant,
    ) -> ParkedPollResult {
        if self.poll_shutdown.is_cancelled() {
            self.poll_attempts.begin_shutdown();
            return ParkedPollResult::ShuttingDown;
        }
        let Some(mut attempt) = self.poll_attempts.register(demand_keys) else {
            return ParkedPollResult::ShuttingDown;
        };
        let recheck_interval = Duration::from_millis(POLL_JOB_RECHECK_INTERVAL_MS);
        let claim_timeout_ms = i64::try_from(claim_timeout.as_millis()).unwrap_or(i64::MAX);
        let mut pending_probe: Option<(PullProbeSource, Option<PullWakeContext>)> = None;
        let mut has_probed = false;

        loop {
            // Long polls may outlive the workload token that authenticated the
            // request. Re-check before every queue probe/claim boundary.
            if ensure_external_identity_not_expired(external_identity).is_err() {
                attempt.finish();
                return ParkedPollResult::AuthorizationExpired;
            }
            if self
                .worker_sessions
                .get_current(&session.worker_id, &session.session_id)
                .is_none()
            {
                attempt.finish();
                return ParkedPollResult::SessionInactive;
            }
            if self.poll_shutdown.is_cancelled() {
                self.poll_attempts.begin_shutdown();
                return ParkedPollResult::ShuttingDown;
            }

            let probe = if let Some(probe) = pending_probe.take() {
                Some(probe)
            } else if let Some(wake) = self
                .poll_waiters
                .take_delegated(&session.project_id, &session.deployment_id)
            {
                self.record_poll_job_waiter_event("delegation_consumed");
                Some((
                    PullProbeSource::Notification(PullWakeSource::Delegation),
                    Some(wake),
                ))
            } else if self.poll_waiters.begin_probe(
                &session.project_id,
                &session.deployment_id,
                demand_keys,
                recheck_interval,
            ) {
                // A scheduled deployment-level fallback may be the first
                // local signal for a pre-existing or remotely appended
                // backlog. If it claims work, wake the remaining capacity.
                let source = if has_probed {
                    PullProbeSource::FallbackProbe
                } else {
                    PullProbeSource::Initial
                };
                Some((source, None))
            } else {
                None
            };
            if let Some((source, wake)) = probe {
                has_probed = true;
                let fanout_on_claim = matches!(
                    source,
                    PullProbeSource::Initial | PullProbeSource::FallbackProbe
                );
                if let Some(wake) = wake.as_ref() {
                    self.record_pull_notification_to_probe(wake);
                    tracing::debug!(
                        target: "agnt5.pull_queue_stage",
                        stage = "probe_started",
                        notified_run_id = wake.queued_run_id.as_deref().unwrap_or_default(),
                        queued_at_ms = wake.queued_at_ms.unwrap_or_default(),
                        discovered_by = source.label(),
                        notification_to_probe_ms = wake.notified_at.elapsed().as_secs_f64() * 1_000.0,
                        "PULL queue stage"
                    );
                }
                if !attempt.try_reserve() {
                    self.poll_attempts.begin_shutdown();
                    return ParkedPollResult::ShuttingDown;
                }
                let claim = self
                    .poll_one_deployment_job(
                        session,
                        claim_timeout,
                        claim_timeout_ms,
                        source,
                        wake.as_ref(),
                    )
                    .await;
                match claim {
                    Ok(Some(selection)) => {
                        tracing::Span::current().record("claimed", true);
                        self.record_poll_job_result("claimed");
                        self.refresh_worker_session_liveness(session);
                        if fanout_on_claim {
                            // A fallback probe may discover a pre-existing or
                            // remotely appended backlog without one local wake
                            // per run. Fan out once so it drains at worker
                            // capacity.
                            let woke = self.poll_waiters.notify_all(
                                &session.project_id,
                                &session.deployment_id,
                                PullWakeSource::BacklogFanout,
                            );
                            if woke > 0 {
                                self.record_poll_job_waiter_event("backlog_fanout");
                            }
                        }
                        attempt.finish();
                        return ParkedPollResult::Assigned(Box::new(selection.assignment));
                    }
                    Ok(None) => {
                        if !attempt.release() {
                            return ParkedPollResult::ShuttingDown;
                        }
                    }
                    Err(status) => {
                        attempt.finish();
                        return ParkedPollResult::Failed(status);
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::Span::current().record("claimed", false);
                self.record_poll_job_result("timeout");
                self.refresh_worker_session_liveness(session);
                attempt.finish();
                return ParkedPollResult::DeadlineExpired;
            }

            let waiter = self
                .poll_waiters
                .register(&session.project_id, &session.deployment_id);
            self.record_poll_job_waiter_event("registered");

            if self
                .worker_sessions
                .get_current(&session.worker_id, &session.session_id)
                .is_none()
            {
                attempt.finish();
                return ParkedPollResult::SessionInactive;
            }

            // Close the scan -> register race without issuing a duplicate
            // empty scan. A queue append that found no waiter invalidates the
            // probe gate, so only that case continues immediately.
            if self.poll_waiters.begin_probe(
                &session.project_id,
                &session.deployment_id,
                demand_keys,
                recheck_interval,
            ) {
                let delegated = self
                    .poll_waiters
                    .take_delegated(&session.project_id, &session.deployment_id);
                pending_probe = Some((PullProbeSource::InvalidatedProbe, delegated));
                continue;
            }

            let sleep_for = remaining.min(recheck_interval);
            let timed_out = remaining <= recheck_interval;
            let wake = tokio::select! {
                wake = waiter.notified() => Some(wake),
                _ = tokio::time::sleep(sleep_for) => None,
                _ = self.poll_shutdown.cancelled() => {
                    self.poll_attempts.begin_shutdown();
                    return ParkedPollResult::ShuttingDown;
                }
            };
            let Some(wake) = wake else {
                if !timed_out {
                    self.record_poll_job_waiter_event("recheck");
                    continue;
                }
                tracing::Span::current().record("claimed", false);
                self.record_poll_job_waiter_event("timeout");
                self.record_poll_job_result("timeout");
                self.refresh_worker_session_liveness(session);
                attempt.finish();
                return ParkedPollResult::DeadlineExpired;
            };
            pending_probe = Some((PullProbeSource::Notification(wake.source), Some(wake)));
        }
    }

    async fn poll_one_deployment_job(
        &self,
        session: &WorkerSession,
        claim_timeout: Duration,
        lease_timeout_ms: i64,
        discovery: PullProbeSource,
        wake: Option<&PullWakeContext>,
    ) -> Result<Option<LegacyPollSelection>, Status> {
        let probe_started_at = Instant::now();
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;

        let delegated_run_id = wake
            .and_then(|context| context.delegation.as_ref())
            .map(|delegation| delegation.run_id.as_str());
        // Each page arrives with its projected run data in one store round
        // trip. Paging past a full page of unclaimable rows keeps a stale or
        // unroutable prefix from hiding ready work; the page budget bounds the
        // scan so one probe cannot walk an unbounded backlog.
        let mut after: Option<PendingRunCursor> = None;
        let mut pages_scanned = 0usize;
        loop {
            let candidates = PendingWorkStore::scan_ready_deployment_candidates(
                store.as_ref(),
                &session.project_id,
                &session.deployment_id,
                now_ms(),
                after.as_ref(),
                POLL_JOB_CANDIDATE_SCAN_LIMIT,
            )
            .await
            .map_err(engine_err_to_status)?;
            pages_scanned += 1;
            let page_full = candidates.len() == POLL_JOB_CANDIDATE_SCAN_LIMIT;
            let next_after = candidates.last().map(|candidate| candidate.cursor.clone());
            let mut page: Vec<(String, Option<PendingRunCursor>, Option<Vec<u8>>)> = candidates
                .into_iter()
                .map(|candidate| {
                    (
                        candidate.cursor.run_id.clone(),
                        Some(candidate.cursor),
                        candidate.data,
                    )
                })
                .collect();
            if let Some(run_id) = delegated_run_id {
                if pages_scanned == 1 && !page.iter().any(|(id, _, _)| id == run_id) {
                    page.insert(0, (run_id.to_string(), None, None));
                }
            }

            if let Some(assignment) = self
                .claim_from_candidate_page(
                    session,
                    page,
                    claim_timeout,
                    lease_timeout_ms,
                    discovery,
                    wake,
                    probe_started_at,
                )
                .await?
            {
                return Ok(Some(assignment));
            }

            if !page_full {
                break;
            }
            if pages_scanned >= POLL_JOB_CANDIDATE_MAX_PAGES {
                self.record_poll_job_waiter_event("candidate_scan_exhausted");
                break;
            }
            after = next_after;
        }
        Ok(None)
    }

    /// Evaluate one page of pending candidates in index order and lease the
    /// first claimable run. `None` means every row was stale, unroutable,
    /// already leased, or lost to a competing claim.
    #[allow(clippy::too_many_arguments)]
    async fn claim_from_candidate_page(
        &self,
        session: &WorkerSession,
        page: Vec<(String, Option<PendingRunCursor>, Option<Vec<u8>>)>,
        claim_timeout: Duration,
        lease_timeout_ms: i64,
        discovery: PullProbeSource,
        wake: Option<&PullWakeContext>,
        probe_started_at: Instant,
    ) -> Result<Option<LegacyPollSelection>, Status> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| Status::unavailable("materialized store not configured"))?;

        for (id, selection_rank, projected_data) in page {
            let _ = &selection_rank;
            let exact_delegation = wake
                .and_then(|context| context.delegation.as_ref())
                .filter(|delegation| delegation.run_id == id);
            let authoritative_data = if exact_delegation.is_some() {
                if let Some(backend) = self.completion_append_backend() {
                    let mut result = None;
                    for attempt in 0..3 {
                        match backend.get_run(&session.project_id, &id).await {
                            Ok(Some(data)) => {
                                result = Some(data);
                                break;
                            }
                            Ok(None) => {}
                            Err(error) => {
                                tracing::debug!(
                                    %error,
                                    run_id = %id,
                                    attempt,
                                    "PollJob: authoritative delegated run read failed"
                                );
                            }
                        }
                        if attempt < 2 {
                            tokio::time::sleep(Duration::from_millis(
                                PROJECTION_PUBLICATION_POLL_INTERVAL_MS,
                            ))
                            .await;
                        }
                    }
                    result
                } else {
                    None
                }
            } else {
                None
            };
            let data = match authoritative_data.or(projected_data) {
                Some(data) => data,
                // A delegated hint may name a run the local index has not
                // caught up with yet; only that case earns a point read.
                None if exact_delegation.is_some() => match PendingWorkStore::load_pending_run(
                    store.as_ref(),
                    &session.project_id,
                    &id,
                )
                .await
                {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => {
                        tracing::debug!(run_id = %id, "PollJob: run proto missing");
                        continue;
                    }
                    Err(e) => return Err(engine_err_to_status(e)),
                },
                None => {
                    tracing::debug!(run_id = %id, "PollJob: run proto missing");
                    continue;
                }
            };
            let run = match <Run as prost::Message>::decode(data.as_slice()) {
                Ok(run) => run,
                Err(e) => {
                    tracing::warn!(error = %e, run_id = %id, "PollJob: decode Run failed");
                    continue;
                }
            };
            let submitted_at_ms = run
                .submitted_at
                .as_ref()
                .map(|timestamp| {
                    timestamp
                        .seconds
                        .saturating_mul(1_000)
                        .saturating_add(i64::from(timestamp.nanos) / 1_000_000)
                })
                .unwrap_or_default();
            // Every HA replica projects the same pending queue. The partition
            // owner gets the first dequeue opportunity; after a bounded grace
            // period exactly one rotating follower may take over so delivery
            // does not depend on an SDK's parked polls being load-balanced to
            // every owner. This keeps competing replicated writes bounded
            // while preserving progress for sparse worker/node placement.
            let gate_decision =
                self.pull_assignment_gate_decision(&id, submitted_at_ms, exact_delegation);
            self.record_pull_gate_decision(gate_decision, discovery, wake, &id, submitted_at_ms);
            tracing::debug!(
                target: "agnt5.pull_queue_stage",
                stage = "gate_evaluated",
                run_id = %id,
                submitted_at_ms,
                decision = gate_decision.decision_label(),
                role = gate_decision.role_label(),
                discovered_by = discovery.label(),
                wake_match = wake_match_label(wake, &id),
                "PULL queue stage"
            );
            if !gate_decision.is_eligible() {
                continue;
            }
            let Ok(demand_key) = DemandKey::from_run(&run) else {
                continue;
            };
            let component_route = self.worker_session_component_route(session, &demand_key);
            if component_route.is_none() {
                continue;
            }
            if run.status != "queued" {
                tracing::debug!(
                    run_id = %run.run_id,
                    project_id = %session.project_id,
                    status = %run.status,
                    "PollJob: skipping stale pending entry for non-queued run"
                );
                continue;
            }
            if !self.streamed_pull_enabled
                && run.metadata.get("stream_mode").map(String::as_str) == Some("full")
            {
                tracing::debug!(
                    run_id = %run.run_id,
                    project_id = %session.project_id,
                    "PollJob: skipping streamed run until streamed pull is enabled"
                );
                continue;
            }
            if self.latest_cached_lifecycle_is_terminal(&session.project_id, &run.run_id) {
                tracing::debug!(
                    run_id = %run.run_id,
                    project_id = %session.project_id,
                    "PollJob: skipping stale pending entry for terminal run"
                );
                continue;
            }
            if !run.lease_id.is_empty() && lease_expiry_is_active(run.lease_expires_at_ms) {
                tracing::debug!(
                    run_id = %run.run_id,
                    project_id = %session.project_id,
                    lease_id = %run.lease_id,
                    "PollJob: skipping stale pending entry for actively leased run"
                );
                continue;
            }
            if self
                .latest_cached_active_lease(&session.project_id, &run.run_id, run.attempt)
                .is_some_and(|lease| lease_expiry_is_active(lease.lease_expires_at_ms))
            {
                tracing::debug!(
                    run_id = %run.run_id,
                    project_id = %session.project_id,
                    "PollJob: skipping stale pending entry with cached active lease"
                );
                continue;
            }
            if !self
                .claim_registry
                .try_claim(&run.run_id, &session.worker_id, claim_timeout)
            {
                continue;
            }

            self.record_pull_candidate_selection(
                gate_decision,
                discovery,
                wake,
                &run.run_id,
                probe_started_at,
            );
            tracing::debug!(
                target: "agnt5.pull_queue_stage",
                stage = "candidate_selected",
                run_id = %run.run_id,
                role = gate_decision.role_label(),
                discovered_by = discovery.label(),
                wake_match = wake_match_label(wake, &run.run_id),
                selection_ms = probe_started_at.elapsed().as_secs_f64() * 1_000.0,
                "PULL queue stage"
            );

            let receipt = match self
                .append_lease_assigned(&session.project_id, &run, session, lease_timeout_ms)
                .await
            {
                Ok(receipt) => receipt,
                Err(e) => {
                    let error = e.to_string();
                    if error.contains("run.assigned conflicts with the accepted lease")
                        || error.contains("run lifecycle gate is closed")
                    {
                        // Another HA node won this dispatch generation. Keep
                        // the process-local claim until its short TTL expires;
                        // releasing it here lets every parked slot retry the
                        // same stale pending row until projection catches up,
                        // turning one bounded loser into a write storm.
                        tracing::debug!(
                            error = %e,
                            run_id = %run.run_id,
                            worker_id = %session.worker_id,
                            "PollJob: competing assignment lost; retaining local claim"
                        );
                        self.record_poll_job_waiter_event("assignment_conflict_damped");
                        continue;
                    }
                    tracing::warn!(
                        error = %e,
                        run_id = %run.run_id,
                        worker_id = %session.worker_id,
                        "PollJob: append run.assigned failed; releasing claim"
                    );
                    self.claim_registry.release(&run.run_id);
                    continue;
                }
            };
            // The committed receipt is the cross-node fence. Projection lag is
            // irrelevant to delivery: a competing lease for this dispatch
            // generation would have failed the strict journal append above.
            self.claim_registry.release(&run.run_id);
            tracing::debug!(
                run_id = %run.run_id,
                lease_id = %receipt.lease.lease_id,
                accepted_offset = receipt.accepted_offset.0,
                appended = receipt.appended,
                "PollJob: returning committed assignment receipt"
            );
            let mut assignment = Self::build_job_assignment(
                &run,
                Some((&receipt.lease, receipt.accepted_offset)),
                component_route.as_ref(),
            );
            Self::stamp_pull_execution_authority(&mut assignment, session);
            return Ok(Some(LegacyPollSelection { assignment }));
        }
        Ok(None)
    }

    pub(super) async fn authoritative_completion_run(
        &self,
        project_id: &str,
        run_id: &str,
        assignment_offset: Option<Offset>,
    ) -> Result<Run, Status> {
        let data = if let Some(backend) = self.completion_append_backend() {
            backend
                .get_run(project_id, run_id)
                .await
                .map_err(Status::from)?
        } else {
            let store = self
                .store
                .as_ref()
                .ok_or_else(|| Status::unavailable("materialized store not configured"))?;
            RunQueryStore::load_run(store.as_ref(), project_id, run_id)
                .await
                .map_err(engine_err_to_status)?
        }
        .ok_or_else(|| Status::not_found("run not found"))?;
        let run = <Run as prost::Message>::decode(data.as_slice())
            .map_err(|error| Status::internal(format!("decode Run failed: {error}")))?;
        self.apply_committed_assignment_fallback(run, assignment_offset)
            .await
    }

    pub(super) async fn apply_committed_assignment_fallback(
        &self,
        mut run: Run,
        assignment_offset: Option<Offset>,
    ) -> Result<Run, Status> {
        // A committed assignment may bridge projection lag only while the
        // projection still says queued. Never rewrite paused or terminal state
        // back to assigned during mutation validation.
        if run.status != "queued" {
            return Ok(run);
        }
        let Some(assignment) = self
            .committed_assignment_record(&run, assignment_offset)
            .await?
        else {
            return Ok(run);
        };
        let metadata_i64 = |key: &str| {
            assignment
                .metadata
                .get(key)
                .and_then(|value| value.parse::<i64>().ok())
        };
        let Some(lease_id) = assignment
            .metadata
            .get("lease_id")
            .filter(|v| !v.is_empty())
        else {
            return Ok(run);
        };
        let Some(worker_id) = assignment
            .metadata
            .get("lease_worker_id")
            .filter(|v| !v.is_empty())
        else {
            return Ok(run);
        };
        let Some(attempt) = assignment
            .metadata
            .get(RUN_META_LEASE_ATTEMPT)
            .and_then(|value| value.parse::<u32>().ok())
        else {
            return Ok(run);
        };
        let Some(expires_at_ms) = metadata_i64("lease_expires_at_ms") else {
            return Ok(run);
        };
        let Some(timeout_ms) = metadata_i64("lease_timeout_ms") else {
            return Ok(run);
        };

        run.status = "assigned".to_string();
        run.lease_id = lease_id.clone();
        run.lease_worker_id = worker_id.clone();
        run.lease_expires_at_ms = expires_at_ms;
        run.lease_timeout_ms = timeout_ms;
        run.lease_created_at_ms = assignment.timestamp_ns / 1_000_000;
        run.attempt = attempt;
        run.metadata.extend(assignment.metadata);
        Ok(run)
    }

    async fn committed_assignment_record(
        &self,
        run: &Run,
        offset: Option<Offset>,
    ) -> Result<Option<Record>, Status> {
        let generation = pull_dispatch_generation(run);
        let matches_assignment = |record: &Record| {
            offset.is_none_or(|offset| record.offset == offset)
                && record.project_id == run.project_id
                && record.run_id == run.run_id
                && record.event_type == "run.assigned"
                && record
                    .metadata
                    .get(RUN_META_DISPATCH_GENERATION)
                    .is_some_and(|value| value == &generation)
        };
        if let Some(records) = self.cache.get_by_run_id(&run.project_id, &run.run_id) {
            if let Some(record) = records
                .into_iter()
                .filter(|record| matches_assignment(record))
                .max_by_key(|record| record.offset.0)
            {
                return Ok(Some((*record).clone()));
            }
        }
        let records = if let Some(backend) = self.completion_append_backend() {
            backend
                .read_by_run_id(
                    &run.project_id,
                    &run.run_id,
                    offset.unwrap_or(Offset::ZERO),
                    Some(vec!["run.assigned".to_string()]),
                )
                .await
                .map_err(Status::from)?
        } else {
            let Some(offset) = offset else {
                return Ok(None);
            };
            self.segment
                .read_range(offset, Offset(offset.0.saturating_add(1)))
                .await
                .map_err(engine_err_to_status)?
        };
        Ok(records
            .into_iter()
            .filter(matches_assignment)
            .max_by_key(|record| record.offset.0))
    }

    /// Build the lease-fenced terminal record for a pull completion.
    ///
    /// The segment decides the fence against the lease entry it folded from
    /// the journal, in log order, inside the append. Nothing here consults
    /// the projection for lease state; it only checks that the request
    /// addresses the run it claims to.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn fenced_completion_record(
        &self,
        mut desired: Record,
        run: &Run,
        worker_id: &str,
        worker_session_id: &str,
        lease_id: &str,
        attempt: u32,
        received_at_ms: i64,
    ) -> Result<Record, Status> {
        if run.project_id != desired.project_id || run.run_id != desired.run_id {
            return Err(Status::failed_precondition(
                "completion project/run does not match materialized Run",
            ));
        }
        if run.deployment_id.is_empty() {
            return Err(Status::failed_precondition(
                "completion Run has no deployment fence",
            ));
        }
        strip_reserved_fence_metadata(&mut desired.metadata);
        desired
            .metadata
            .insert("worker_id".to_string(), worker_id.to_string());
        desired
            .metadata
            .insert("lease_id".to_string(), lease_id.to_string());
        LeaseFence {
            kind: LeaseFenceKind::PullCompletion,
            lease_id: lease_id.to_string(),
            worker_id: worker_id.to_string(),
            session_hash: super::pull_workers::worker_session_hash(worker_session_id),
            attempt,
            received_at_ms,
            expiry_reason: None,
        }
        .stamp(&mut desired);
        desired.offset = Offset::ZERO;
        desired.timestamp_ns = received_at_ms.saturating_mul(1_000_000);
        Ok(desired)
    }

    /// Append `run.lease_renewed` fenced by the renewing lease. The segment
    /// accepts it only while that lease is the current entry and the new
    /// expiry extends it; the response reports the durable expiry.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn append_lease_renewal(
        &self,
        run: &Run,
        worker_id: &str,
        deployment_id: &str,
        lease_id: &str,
        attempt: u32,
        session_hash: &str,
        mode: LeaseRenewalMode,
        lease_timeout_ms: i64,
        received_at_ms: i64,
    ) -> Result<i64, Status> {
        let mode_matches = match mode {
            LeaseRenewalMode::Pull => super::run_uses_pull_lease(run),
            LeaseRenewalMode::Push => !super::run_uses_pull_lease(run),
        };
        if run.deployment_id != deployment_id || !mode_matches {
            return Err(Status::failed_precondition(
                "job lease changed or expired before renewal",
            ));
        }
        let normalized_timeout_ms = if lease_timeout_ms > 0 {
            lease_timeout_ms
        } else {
            DEFAULT_LEASE_TIMEOUT_MS
        };
        let renewed_expires_at_ms = received_at_ms.saturating_add(normalized_timeout_ms);
        let mut metadata = std::collections::HashMap::from([
            ("lease_id".to_string(), lease_id.to_string()),
            ("lease_worker_id".to_string(), worker_id.to_string()),
            (
                "lease_expires_at_ms".to_string(),
                renewed_expires_at_ms.to_string(),
            ),
            (
                "lease_timeout_ms".to_string(),
                normalized_timeout_ms.to_string(),
            ),
            (RUN_META_LEASE_ATTEMPT.to_string(), attempt.to_string()),
        ]);
        if !session_hash.is_empty() {
            metadata.insert(
                RUN_META_LEASE_SESSION_HASH.to_string(),
                session_hash.to_string(),
            );
        }
        if !run.component_name.is_empty() {
            metadata.insert("component_name".to_string(), run.component_name.clone());
        }
        let mut record = Record {
            offset: Offset::ZERO,
            project_id: run.project_id.clone(),
            run_id: run.run_id.clone(),
            event_type: RUN_LEASE_RENEWED_EVENT.to_string(),
            data: bytes::Bytes::new(),
            timestamp_ns: received_at_ms.saturating_mul(1_000_000),
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };
        LeaseFence {
            kind: LeaseFenceKind::LeaseRenewal,
            lease_id: lease_id.to_string(),
            worker_id: worker_id.to_string(),
            session_hash: session_hash.to_string(),
            attempt,
            received_at_ms,
            expiry_reason: None,
        }
        .stamp(&mut record);
        self.append_completion_batch(vec![record]).await?;
        Ok(renewed_expires_at_ms)
    }
}
