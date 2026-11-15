//! Backend traits for the dataplane.
//!
//! These traits abstract over in-process vs gRPC access to Engine and Coordinator.
//! In `--target=all` mode, implementations wrap direct `Arc<T>` references.
//! In disaggregated mode, implementations wrap tonic gRPC clients.

use std::pin::Pin;

use async_trait::async_trait;
use tokio_stream::Stream;

use engine_core::types::{LeaseExpiryCursor, Offset, Record};
use engine_proto::*;

use crate::error::DataplaneError;
use crate::error::DataplaneResult;

/// A stream of records returned by tail/read operations.
pub type RecordStream = Pin<Box<dyn Stream<Item = DataplaneResult<Record>> + Send>>;

/// Per-record result from an explicitly non-atomic append.
///
/// `Unknown` is deliberately conservative: the backend returned an error, but
/// a lost response may mean the record committed. Callers must reconcile it
/// before retrying non-idempotent records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NonAtomicAppendRecordOutcome {
    Acknowledged(Offset),
    Unknown(String),
    NotAttempted,
}

/// Exact input-ordered outcomes from an explicitly non-atomic append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonAtomicAppendOutcome {
    records: Vec<NonAtomicAppendRecordOutcome>,
    cardinality_error: Option<String>,
}

impl NonAtomicAppendOutcome {
    pub fn new(expected: usize, mut records: Vec<NonAtomicAppendRecordOutcome>) -> Self {
        let actual = records.len();
        let cardinality_error = (actual != expected).then(|| {
            format!(
                "non-atomic append outcome cardinality mismatch: expected {expected}, outcomes={actual}"
            )
        });
        if actual < expected {
            let error = cardinality_error
                .as_deref()
                .unwrap_or("non-atomic append outcome missing")
                .to_string();
            records.resize(expected, NonAtomicAppendRecordOutcome::Unknown(error));
        } else {
            records.truncate(expected);
        }
        Self {
            records,
            cardinality_error,
        }
    }

    pub fn records(&self) -> &[NonAtomicAppendRecordOutcome] {
        &self.records
    }

    pub fn is_complete(&self) -> bool {
        self.cardinality_error.is_none()
            && self
                .records
                .iter()
                .all(|outcome| matches!(outcome, NonAtomicAppendRecordOutcome::Acknowledged(_)))
    }

    pub fn may_have_committed(&self) -> bool {
        self.records.iter().any(|outcome| {
            matches!(
                outcome,
                NonAtomicAppendRecordOutcome::Acknowledged(_)
                    | NonAtomicAppendRecordOutcome::Unknown(_)
            )
        })
    }

    pub fn acknowledged_indices(&self) -> Vec<usize> {
        self.indices_matching(|outcome| {
            matches!(outcome, NonAtomicAppendRecordOutcome::Acknowledged(_))
        })
    }

    pub fn unknown_indices(&self) -> Vec<usize> {
        self.indices_matching(|outcome| matches!(outcome, NonAtomicAppendRecordOutcome::Unknown(_)))
    }

    pub fn not_attempted_indices(&self) -> Vec<usize> {
        self.indices_matching(|outcome| {
            matches!(outcome, NonAtomicAppendRecordOutcome::NotAttempted)
        })
    }

    pub fn summary(&self) -> String {
        let acknowledged = self.acknowledged_indices();
        let unknown = self.unknown_indices();
        let not_attempted = self.not_attempted_indices();
        let first_error = self.records.iter().find_map(|outcome| match outcome {
            NonAtomicAppendRecordOutcome::Unknown(message) => Some(message.as_str()),
            _ => None,
        });
        format!(
            "acknowledged_indices={acknowledged:?}, unknown_indices={unknown:?}, not_attempted_indices={not_attempted:?}, first_error={}, cardinality_error={}",
            first_error.unwrap_or("none"),
            self.cardinality_error.as_deref().unwrap_or("none")
        )
    }

    pub fn require_complete(self, context: &str) -> DataplaneResult<Vec<Offset>> {
        if self.is_complete() {
            return Ok(self
                .records
                .into_iter()
                .map(|outcome| match outcome {
                    NonAtomicAppendRecordOutcome::Acknowledged(offset) => offset,
                    _ => unreachable!("complete outcome contains only acknowledged records"),
                })
                .collect());
        }

        Err(DataplaneError::Internal(format!(
            "{context}: explicitly non-atomic append incomplete: {}",
            self.summary()
        )))
    }

    fn indices_matching(
        &self,
        predicate: impl Fn(&NonAtomicAppendRecordOutcome) -> bool,
    ) -> Vec<usize> {
        self.records
            .iter()
            .enumerate()
            .filter_map(|(index, outcome)| predicate(outcome).then_some(index))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// EngineBackend — how Gateway + Coordinator access the Engine
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct BackendAppendBatchOutcome {
    pub offsets: Vec<Offset>,
    pub written_count: usize,
}

/// Check that an `AppendObserved` response has one outcome and one offset per
/// input record, so callers can read both by input position.
pub fn check_observed_response(
    response: &engine_proto::AppendObservedResponse,
    expected: usize,
) -> DataplaneResult<()> {
    if response.outcomes.len() != expected || response.offsets.len() != expected {
        return Err(DataplaneError::Internal(format!(
            "engine AppendObserved response cardinality mismatch: expected {expected}, outcomes={}, offsets={}",
            response.outcomes.len(),
            response.offsets.len()
        )));
    }
    Ok(())
}

/// Abstraction over the Engine service.
///
/// In-process: wraps `Arc<EngineServiceImpl>` with direct calls.
/// Disaggregated: wraps `EngineServiceClient<Channel>` with gRPC calls.
#[async_trait]
pub trait EngineBackend: Send + Sync + 'static {
    // --- Write path ---

    /// Append a single record to the journal.
    async fn append(&self, record: Record) -> DataplaneResult<(Offset, u64)>;

    /// Append a batch of records to the journal.
    async fn append_batch(&self, records: Vec<Record>) -> DataplaneResult<Vec<Offset>>;

    /// Append a batch and preserve whether the input records were newly
    /// written or resolved from an existing idempotency winner.
    ///
    /// Backends that expose the Engine `AppendBatch` response should override
    /// this method so correctness-sensitive callers can retain its
    /// `written_count`. Test and compatibility backends may use the default
    /// when they only support fresh appends.
    async fn append_batch_with_outcome(
        &self,
        records: Vec<Record>,
    ) -> DataplaneResult<BackendAppendBatchOutcome> {
        let offsets = self.append_batch(records).await?;
        let written_count = offsets.len();
        Ok(BackendAppendBatchOutcome {
            offsets,
            written_count,
        })
    }

    // The next three have no default body on purpose. A default that
    // refuses lets a backend, or a wrapper such as the HA router, compile
    // without the method and fail every call in production; the HA router
    // once refused every OTLP export that way. A backend that can't serve one
    // says so in its own implementation.

    /// Append the records of one observed run through the engine's
    /// `AppendObserved` path, which checks no worker lease. The response
    /// gives one outcome per record: written, or dropped because the run had
    /// already ended.
    async fn append_observed(
        &self,
        records: Vec<Record>,
    ) -> DataplaneResult<engine_proto::AppendObservedResponse>;

    /// Atomically park one fenced activation and schedule its durable timer.
    async fn suspend_activation(
        &self,
        request: engine_proto::SuspendActivationRequest,
    ) -> DataplaneResult<engine_proto::SuspendActivationResponse>;

    /// Append records whose routing may span partitions.
    ///
    /// This operation is explicitly non-atomic: an error can follow a durable
    /// prefix. Callers must use it only for bulk features whose contract
    /// already permits per-record progress.
    async fn append_batch_non_atomic(&self, records: Vec<Record>) -> NonAtomicAppendOutcome {
        let expected = records.len();
        let mut outcomes = Vec::with_capacity(expected);
        for record in records {
            match self.append(record).await {
                Ok((offset, _)) => {
                    outcomes.push(NonAtomicAppendRecordOutcome::Acknowledged(offset))
                }
                Err(error) => {
                    outcomes.push(NonAtomicAppendRecordOutcome::Unknown(error.to_string()));
                    outcomes.resize(expected, NonAtomicAppendRecordOutcome::NotAttempted);
                    break;
                }
            }
        }
        NonAtomicAppendOutcome::new(expected, outcomes)
    }

    // --- Read path (active runs) ---

    /// Look up the original invocation through the journal idempotency index.
    /// Older/alternative backends retain complete-history compatibility.
    async fn lookup_invocation(
        &self,
        project_id: &str,
        run_id: &str,
        caller_key: &str,
    ) -> DataplaneResult<Option<Record>> {
        let records = self
            .read_by_run_id(
                project_id,
                run_id,
                Offset::ZERO,
                Some(vec!["run.queued".into(), "batch.created".into()]),
            )
            .await?;
        Ok(records.into_iter().find(|record| {
            record
                .metadata
                .get(engine_core::journal_contract::META_INVOCATION_IDEMPOTENCY_KEY)
                .is_some_and(|key| key == caller_key)
        }))
    }

    /// Read all records for a run.
    async fn read_by_run_id(
        &self,
        project_id: &str,
        run_id: &str,
        from_offset: Offset,
        event_types: Option<Vec<String>>,
    ) -> DataplaneResult<Vec<Record>>;

    /// Find a record by step key (memoization lookup).
    async fn find_by_step_key(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> DataplaneResult<Option<Record>>;

    /// Get materialized run state.
    async fn get_run(&self, project_id: &str, run_id: &str) -> DataplaneResult<Option<Vec<u8>>>;

    /// Tail the journal for a specific run (live streaming).
    async fn tail(
        &self,
        project_id: &str,
        run_id: &str,
        from_offset: Offset,
    ) -> DataplaneResult<RecordStream>;

    // --- Entity state ---

    async fn get_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> DataplaneResult<Option<(Vec<u8>, i64)>>;

    #[allow(clippy::too_many_arguments)]
    async fn put_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
        data: Vec<u8>,
        version: i64,
    ) -> DataplaneResult<i64>;

    // --- Queries (materialized store + query lake) ---

    async fn query(
        &self,
        _req: engine_proto::QueryRequest,
    ) -> DataplaneResult<engine_proto::QueryResponse> {
        Err(crate::error::DataplaneError::Grpc(
            tonic::Status::unimplemented("SQL is unavailable on this backend"),
        ))
    }

    async fn list_runs(&self, req: ListRunsRequest) -> DataplaneResult<ListRunsResponse>;

    async fn get_run_stats(&self, req: GetRunStatsRequest) -> DataplaneResult<GetRunStatsResponse>;

    /// List inbound events a project received, newest-first (Events page).
    async fn list_events(&self, req: ListEventsRequest) -> DataplaneResult<ListEventsResponse>;

    // --- Projections: approvals, signals, timers, jobs, batches, sessions, messages ---

    async fn get_approval(&self, req: GetApprovalRequest) -> DataplaneResult<GetApprovalResponse>;
    async fn list_approvals(
        &self,
        req: EngineListApprovalsRequest,
    ) -> DataplaneResult<EngineListApprovalsResponse>;

    async fn get_signal(&self, req: GetSignalRequest) -> DataplaneResult<GetSignalResponse>;
    async fn lookup_signal(
        &self,
        req: LookupSignalRequest,
    ) -> DataplaneResult<LookupSignalResponse>;
    async fn list_signals(&self, req: ListSignalsRequest) -> DataplaneResult<ListSignalsResponse>;

    async fn get_timer(&self, req: GetTimerRequest) -> DataplaneResult<GetTimerResponse>;
    async fn list_timers(&self, req: ListTimersRequest) -> DataplaneResult<ListTimersResponse>;
    async fn list_due_timers(
        &self,
        req: ListDueTimersRequest,
    ) -> DataplaneResult<ListDueTimersResponse>;

    async fn get_job(&self, req: GetJobRequest) -> DataplaneResult<GetJobResponse>;
    async fn list_jobs_by_run(
        &self,
        req: ListJobsByRunRequest,
    ) -> DataplaneResult<ListJobsByRunResponse>;
    async fn list_jobs_by_batch(
        &self,
        req: ListJobsByBatchRequest,
    ) -> DataplaneResult<ListJobsByBatchResponse>;

    async fn get_batch(&self, req: GetBatchRequest) -> DataplaneResult<GetBatchResponse>;

    async fn get_session(&self, req: GetSessionRequest) -> DataplaneResult<GetSessionResponse>;
    async fn list_sessions(
        &self,
        req: ListSessionsRequest,
    ) -> DataplaneResult<ListSessionsResponse>;

    async fn get_message(&self, req: GetMessageRequest) -> DataplaneResult<GetMessageResponse>;
    async fn list_messages_by_correlation(
        &self,
        req: ListMessagesByCorrelationRequest,
    ) -> DataplaneResult<ListMessagesByCorrelationResponse>;

    // --- Pending Runs (pull workers) ---

    /// List pending runs for given components (returns serialized Run protos).
    async fn list_pending_runs(
        &self,
        project_id: &str,
        component_names: &[String],
        limit: usize,
    ) -> DataplaneResult<Vec<Vec<u8>>>;

    // --- Lease expiry ---

    /// Range-scan the lease index for leases whose `expires_at_ms <= now_ms`,
    /// strictly after `start_after` when supplied. Returns stable expiry
    /// cursors bounded by `limit`.
    ///
    /// Used by the lease expiry sweeper background task to find expired
    /// leases that need to be re-queued or dead-lettered. The sweeper
    /// then calls [`Self::get_run`] for each entry to read the current
    /// `attempt` count before deciding the action. `expires_at_ms` is
    /// returned so the sweeper can reconstruct the exact index key for
    /// orphan GC via [`Self::gc_expiry_index_entry`].
    ///
    /// The disaggregated gRPC backend currently returns an empty list —
    /// in disaggregated mode lease expiry handling is the engine's
    /// responsibility, not the coordinator's. (TODO: dedicated RPC.)
    async fn list_expired_leases(
        &self,
        now_ms: i64,
        limit: usize,
        start_after: Option<LeaseExpiryCursor>,
    ) -> DataplaneResult<Vec<LeaseExpiryCursor>>;

    /// Delete a single orphan entry from the lease expiry index.
    ///
    /// Invoked by the sweeper when an expired index entry points at a
    /// Run whose lease has already been cleared (or the Run is missing
    /// entirely). Without this, orphan entries get re-read on every
    /// tick and logged as `skipped`, growing the log and the index over
    /// time. Idempotent — a missing key is a no-op.
    ///
    /// The disaggregated gRPC backend returns `Ok(())` as a no-op —
    /// lease lifecycle is the engine's responsibility there.
    async fn gc_expiry_index_entry(
        &self,
        expires_at_ms: i64,
        project_id: &str,
        run_id: &str,
    ) -> DataplaneResult<()>;

    // --- Active leases ---

    /// List all active leases known to the engine, returning
    /// `(invocation_id, lease_id)` pairs.
    ///
    /// Used by the coordinator on startup to rebuild its in-memory
    /// `active_leases` fence map. Without this rehydration, a worker
    /// reconnecting after a coordinator restart would land in the "fencing
    /// disabled" branch (empty expected lease) and have its responses
    /// silently allowed through, even if the run had been re-leased to a
    /// different worker meanwhile.
    ///
    /// In-process backends scan the lease index and resolve each Run proto
    /// to extract its `lease_id`. The disaggregated gRPC backend currently
    /// returns an empty list — coordinator restarts in disaggregated mode
    /// reconstitute the fence map only as new dispatches mint new leases.
    /// Correctness is preserved either way: a stale ack with a mismatched
    /// `lease_id` is still rejected by the per-invocation fence check.
    async fn list_active_leases(&self, limit: usize) -> DataplaneResult<Vec<(String, String)>>;

    /// List the `(project_id, run_id)` pairs the lease index shows as held by
    /// `worker_id`. The rows are a hint: callers re-read each Run and fence
    /// any append against the lease they observe.
    ///
    /// A backend without access to the lease index fails the call. An empty
    /// list would tell the caller the worker holds nothing.
    async fn list_worker_leases(&self, worker_id: &str) -> DataplaneResult<Vec<(String, String)>>;

    // --- Status ---

    async fn get_status(&self) -> DataplaneResult<GetStatusResponse>;
}

// ---------------------------------------------------------------------------
// CoordinatorBackend — how Gateway accesses the Coordinator
// ---------------------------------------------------------------------------

/// Abstraction over the Worker Coordinator service.
///
/// In-process: wraps `Arc<CoordinatorService>` with direct calls.
/// Disaggregated: wraps `WorkerCoordinatorServiceClient<Channel>` with gRPC calls.
#[async_trait]
pub trait CoordinatorBackend: Send + Sync + 'static {
    /// Dispatch a component to a worker (synchronous, waits for response).
    async fn dispatch_component(
        &self,
        req: DispatchComponentRequest,
    ) -> DataplaneResult<DispatchComponentResponse>;

    /// Dispatch a component with streaming response.
    async fn dispatch_component_stream(
        &self,
        req: DispatchComponentRequest,
    ) -> DataplaneResult<
        Pin<Box<dyn Stream<Item = DataplaneResult<DispatchComponentResponse>> + Send>>,
    >;

    /// List registered workers.
    async fn list_workers(&self, req: ListWorkersRequest) -> DataplaneResult<ListWorkersResponse>;

    /// List active `(project_id, deployment_id)` pairs known to the runtime.
    ///
    /// The in-process HA coordinator can include both local worker streams and
    /// alive gossip entries. Other coordinator backends may return an empty
    /// list and let callers fall back to `list_workers` or control-plane
    /// discovery.
    async fn active_deployments(&self) -> DataplaneResult<Vec<(String, String)>> {
        Ok(Vec::new())
    }

    /// Walk in-memory worker registrations for triggers matching
    /// `event_name` in `(project_id, deployment_id)`. Returns
    /// `(component_type, component_name, spec_bytes)` tuples — same
    /// shape as `MaterializedStore::list_event_triggers` so the gateway's
    /// events handler can treat both sources uniformly.
    ///
    /// Used as the standalone-no-CP fallback for the gateway: when no
    /// control plane is configured, CF_TRIGGERS is never populated, so
    /// the in-memory worker map is the only source of truth. Returns an
    /// empty Vec in disaggregated coordinator mode (the gRPC backend
    /// has no in-memory workers to walk).
    async fn list_in_memory_event_triggers(
        &self,
        project_id: &str,
        deployment_id: &str,
        event_name: &str,
    ) -> DataplaneResult<Vec<(i32, String, Vec<u8>)>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_atomic_outcome_preserves_exact_input_order() {
        let outcome = NonAtomicAppendOutcome::new(
            2,
            vec![
                NonAtomicAppendRecordOutcome::Acknowledged(Offset(7)),
                NonAtomicAppendRecordOutcome::Acknowledged(Offset(3)),
            ],
        );

        assert_eq!(
            outcome.require_complete("test").unwrap(),
            vec![Offset(7), Offset(3)]
        );
    }

    #[test]
    fn non_atomic_outcome_exposes_partial_and_unknown_records() {
        let outcome = NonAtomicAppendOutcome::new(
            3,
            vec![
                NonAtomicAppendRecordOutcome::Acknowledged(Offset(7)),
                NonAtomicAppendRecordOutcome::Unknown("response lost".to_string()),
                NonAtomicAppendRecordOutcome::NotAttempted,
            ],
        );

        assert_eq!(outcome.acknowledged_indices(), vec![0]);
        assert_eq!(outcome.unknown_indices(), vec![1]);
        assert_eq!(outcome.not_attempted_indices(), vec![2]);
        assert!(outcome.may_have_committed());
        let error = outcome.require_complete("test").unwrap_err();
        assert!(error.to_string().contains("acknowledged_indices=[0]"));
        assert!(error.to_string().contains("unknown_indices=[1]"));
        assert!(error.to_string().contains("not_attempted_indices=[2]"));
    }

    #[test]
    fn non_atomic_outcome_normalizes_malformed_cardinality_conservatively() {
        let short = NonAtomicAppendOutcome::new(
            3,
            vec![NonAtomicAppendRecordOutcome::Acknowledged(Offset(7))],
        );
        assert_eq!(short.records().len(), 3);
        assert_eq!(short.acknowledged_indices(), vec![0]);
        assert_eq!(short.unknown_indices(), vec![1, 2]);
        assert!(!short.is_complete());
        assert!(short.summary().contains("cardinality mismatch"));

        let long = NonAtomicAppendOutcome::new(
            1,
            vec![
                NonAtomicAppendRecordOutcome::Acknowledged(Offset(7)),
                NonAtomicAppendRecordOutcome::Acknowledged(Offset(8)),
            ],
        );
        assert_eq!(long.records().len(), 1);
        assert!(!long.is_complete());
        assert!(long.summary().contains("outcomes=2"));
    }
}
