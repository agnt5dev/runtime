//! In-process EngineBackend implementation.
//!
//! Wraps Arc<EngineServiceImpl> for use when running --target=all.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_stream::StreamExt;
use tonic::Request;

use crate::EngineServiceImpl;
use engine_core::storage_contract::{LeaseWorkStore, PendingWorkStore};
use engine_core::traits::Segment;
use engine_core::types::{LeaseExpiryCursor, Offset, Record};
use engine_proto::engine_service_server::EngineService;
use engine_proto::*;
use runtime_common::backend::{
    check_observed_response, BackendAppendBatchOutcome, EngineBackend, RecordStream,
};
use runtime_common::error::{DataplaneError, DataplaneResult};

/// In-process engine backend — calls EngineServiceImpl directly (no gRPC hop).
pub struct InProcessEngine<S: Segment> {
    svc: Arc<EngineServiceImpl<S>>,
}

impl<S: Segment> InProcessEngine<S> {
    pub fn new(svc: Arc<EngineServiceImpl<S>>) -> Self {
        Self { svc }
    }
}

fn grpc_err(s: tonic::Status) -> DataplaneError {
    DataplaneError::Grpc(s)
}

/// Requests relayed by the in-process backend come from runtime producers
/// (CompleteJob, RenewJobLease, sweepers, dispatchers), never straight from a
/// worker, so their lease fences are authentic.
fn runtime_authored_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.extensions_mut().insert(crate::RuntimeAuthored);
    request
}

#[async_trait]
impl<S: Segment> EngineBackend for InProcessEngine<S> {
    async fn lookup_invocation(
        &self,
        project_id: &str,
        run_id: &str,
        caller_key: &str,
    ) -> DataplaneResult<Option<Record>> {
        let response = self
            .svc
            .read_by_run_id(Request::new(ReadByRunIdRequest {
                project_id: project_id.into(),
                run_id: run_id.into(),
                event_types: vec!["run.queued".into(), "batch.created".into()],
                invocation_idempotency_key: caller_key.into(),
                ..Default::default()
            }))
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(response
            .records
            .into_iter()
            .map(Record::from_proto)
            .find(|record| {
                record
                    .metadata
                    .get(engine_core::journal_contract::META_INVOCATION_IDEMPOTENCY_KEY)
                    .is_some_and(|key| key == caller_key)
            }))
    }

    async fn append(&self, record: Record) -> DataplaneResult<(Offset, u64)> {
        let resp = self
            .svc
            .append(runtime_authored_request(AppendRequest {
                record: Some(record.to_proto()),
            }))
            .await
            .map_err(grpc_err)?;
        let r = resp.into_inner();
        Ok((Offset(r.offset), r.timestamp_ns as u64))
    }

    async fn append_batch(&self, records: Vec<Record>) -> DataplaneResult<Vec<Offset>> {
        Ok(self.append_batch_with_outcome(records).await?.offsets)
    }

    async fn append_batch_with_outcome(
        &self,
        records: Vec<Record>,
    ) -> DataplaneResult<BackendAppendBatchOutcome> {
        let expected = records.len();
        let resp = self
            .svc
            .append_batch(runtime_authored_request(AppendBatchRequest {
                records: records.iter().map(|r| r.to_proto()).collect(),
            }))
            .await
            .map_err(grpc_err)?;
        let response = resp.into_inner();
        if response.offsets.len() != expected {
            return Err(DataplaneError::Internal(format!(
                "engine AppendBatch response cardinality mismatch: expected {expected}, offsets={}",
                response.offsets.len()
            )));
        }
        let written_count = usize::try_from(response.written_count).map_err(|_| {
            DataplaneError::Internal(format!(
                "engine AppendBatch returned negative written_count {}",
                response.written_count
            ))
        })?;
        if written_count > expected {
            return Err(DataplaneError::Internal(format!(
                "engine AppendBatch written_count exceeds input cardinality: expected at most {expected}, written_count={written_count}"
            )));
        }
        Ok(BackendAppendBatchOutcome {
            offsets: response.offsets.into_iter().map(Offset).collect(),
            written_count,
        })
    }

    async fn append_observed(
        &self,
        records: Vec<Record>,
    ) -> DataplaneResult<AppendObservedResponse> {
        let expected = records.len();
        let response = self
            .svc
            .append_observed(runtime_authored_request(AppendObservedRequest {
                records: records.iter().map(|r| r.to_proto()).collect(),
            }))
            .await
            .map_err(grpc_err)?
            .into_inner();
        check_observed_response(&response, expected)?;
        Ok(response)
    }

    async fn suspend_activation(
        &self,
        request: SuspendActivationRequest,
    ) -> DataplaneResult<SuspendActivationResponse> {
        Ok(self
            .svc
            .suspend_activation(runtime_authored_request(request))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }

    async fn read_by_run_id(
        &self,
        project_id: &str,
        run_id: &str,
        _from_offset: Offset,
        event_types: Option<Vec<String>>,
    ) -> DataplaneResult<Vec<Record>> {
        let resp = self
            .svc
            .read_by_run_id(Request::new(ReadByRunIdRequest {
                project_id: project_id.to_string(),
                run_id: run_id.to_string(),
                from_timestamp_ns: 0,
                event_types: event_types.unwrap_or_default(),
                ..Default::default()
            }))
            .await
            .map_err(grpc_err)?;
        Ok(resp
            .into_inner()
            .records
            .into_iter()
            .map(Record::from_proto)
            .collect())
    }

    async fn find_by_step_key(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> DataplaneResult<Option<Record>> {
        let resp = self
            .svc
            .find_by_step_key(Request::new(FindByStepKeyRequest {
                project_id: project_id.to_string(),
                run_id: run_id.to_string(),
                step_key: step_key.to_string(),
            }))
            .await
            .map_err(grpc_err)?;
        Ok(resp.into_inner().record.map(Record::from_proto))
    }

    async fn get_run(&self, project_id: &str, run_id: &str) -> DataplaneResult<Option<Vec<u8>>> {
        let resp = self
            .svc
            .get_run(Request::new(GetRunRequest {
                project_id: project_id.to_string(),
                run_id: run_id.to_string(),
            }))
            .await
            .map_err(grpc_err)?;
        let r = resp.into_inner();
        if r.found {
            // Serialize the Run proto to bytes for the backend interface
            Ok(r.run.map(|run| prost::Message::encode_to_vec(&run)))
        } else {
            Ok(None)
        }
    }

    async fn tail(
        &self,
        project_id: &str,
        run_id: &str,
        from_offset: Offset,
    ) -> DataplaneResult<RecordStream> {
        let resp = self
            .svc
            .tail(Request::new(TailRequest {
                project_id: project_id.to_string(),
                run_id: run_id.to_string(),
                from_offset: from_offset.0,
            }))
            .await
            .map_err(grpc_err)?;
        let stream = resp.into_inner().map(|result| match result {
            Ok(tail_resp) => match tail_resp.record {
                Some(r) => Ok(Record::from_proto(r)),
                None => Err(DataplaneError::Internal("empty tail record".to_string())),
            },
            Err(s) => Err(DataplaneError::Grpc(s)),
        });
        Ok(Box::pin(stream))
    }

    async fn get_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> DataplaneResult<Option<(Vec<u8>, i64)>> {
        let resp = self
            .svc
            .get_entity_state(runtime_authored_request(GetEntityStateRequest {
                project_id: project_id.to_string(),
                entity_type: entity_type.to_string(),
                entity_key: entity_key.to_string(),
                scope: scope.to_string(),
                scope_id: scope_id.to_string(),
            }))
            .await
            .map_err(grpc_err)?;
        let r = resp.into_inner();
        if r.found {
            Ok(Some((r.state_json, r.version)))
        } else {
            Ok(None)
        }
    }

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
    ) -> DataplaneResult<i64> {
        let resp = self
            .svc
            .put_entity_state(runtime_authored_request(PutEntityStateRequest {
                project_id: project_id.to_string(),
                entity_type: entity_type.to_string(),
                entity_key: entity_key.to_string(),
                scope: scope.to_string(),
                scope_id: scope_id.to_string(),
                state_json: data,
                expected_version: version,
                run_id: String::new(),
                worker_id: String::new(),
                worker_session_id: String::new(),
                lease_id: String::new(),
                attempt: None,
                operation_id: String::new(),
            }))
            .await
            .map_err(grpc_err)?;
        Ok(resp.into_inner().new_version)
    }

    // --- Delegate query/projection RPCs directly ---

    async fn query(
        &self,
        req: engine_proto::QueryRequest,
    ) -> DataplaneResult<engine_proto::QueryResponse> {
        Ok(self
            .svc
            .query(runtime_authored_request(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }

    async fn list_runs(&self, req: ListRunsRequest) -> DataplaneResult<ListRunsResponse> {
        Ok(self
            .svc
            .list_runs(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_events(&self, req: ListEventsRequest) -> DataplaneResult<ListEventsResponse> {
        Ok(self
            .svc
            .list_events(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_run_stats(&self, req: GetRunStatsRequest) -> DataplaneResult<GetRunStatsResponse> {
        Ok(self
            .svc
            .get_run_stats(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_approval(&self, req: GetApprovalRequest) -> DataplaneResult<GetApprovalResponse> {
        Ok(self
            .svc
            .get_approval(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_approvals(
        &self,
        req: EngineListApprovalsRequest,
    ) -> DataplaneResult<EngineListApprovalsResponse> {
        Ok(self
            .svc
            .list_approvals(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_signal(&self, req: GetSignalRequest) -> DataplaneResult<GetSignalResponse> {
        Ok(self
            .svc
            .get_signal(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn lookup_signal(
        &self,
        req: LookupSignalRequest,
    ) -> DataplaneResult<LookupSignalResponse> {
        Ok(self
            .svc
            .lookup_signal(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_signals(&self, req: ListSignalsRequest) -> DataplaneResult<ListSignalsResponse> {
        Ok(self
            .svc
            .list_signals(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_timer(&self, req: GetTimerRequest) -> DataplaneResult<GetTimerResponse> {
        Ok(self
            .svc
            .get_timer(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_timers(&self, req: ListTimersRequest) -> DataplaneResult<ListTimersResponse> {
        Ok(self
            .svc
            .list_timers(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_due_timers(
        &self,
        req: ListDueTimersRequest,
    ) -> DataplaneResult<ListDueTimersResponse> {
        Ok(self
            .svc
            .list_due_timers(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_job(&self, req: GetJobRequest) -> DataplaneResult<GetJobResponse> {
        Ok(self
            .svc
            .get_job(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_jobs_by_run(
        &self,
        req: ListJobsByRunRequest,
    ) -> DataplaneResult<ListJobsByRunResponse> {
        Ok(self
            .svc
            .list_jobs_by_run(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_jobs_by_batch(
        &self,
        req: ListJobsByBatchRequest,
    ) -> DataplaneResult<ListJobsByBatchResponse> {
        Ok(self
            .svc
            .list_jobs_by_batch(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_batch(&self, req: GetBatchRequest) -> DataplaneResult<GetBatchResponse> {
        Ok(self
            .svc
            .get_batch(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_session(&self, req: GetSessionRequest) -> DataplaneResult<GetSessionResponse> {
        Ok(self
            .svc
            .get_session(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_sessions(
        &self,
        req: ListSessionsRequest,
    ) -> DataplaneResult<ListSessionsResponse> {
        Ok(self
            .svc
            .list_sessions(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_message(&self, req: GetMessageRequest) -> DataplaneResult<GetMessageResponse> {
        Ok(self
            .svc
            .get_message(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_messages_by_correlation(
        &self,
        req: ListMessagesByCorrelationRequest,
    ) -> DataplaneResult<ListMessagesByCorrelationResponse> {
        Ok(self
            .svc
            .list_messages_by_correlation(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_pending_runs(
        &self,
        project_id: &str,
        component_names: &[String],
        limit: usize,
    ) -> DataplaneResult<Vec<Vec<u8>>> {
        match self.svc.store() {
            Some(store) => PendingWorkStore::scan_pending_runs(
                store.as_ref(),
                project_id,
                component_names,
                limit,
            )
            .await
            .map_err(|e| DataplaneError::Internal(format!("list_pending_runs: {e}"))),
            None => Ok(vec![]),
        }
    }

    async fn list_expired_leases(
        &self,
        now_ms: i64,
        limit: usize,
        start_after: Option<LeaseExpiryCursor>,
    ) -> DataplaneResult<Vec<LeaseExpiryCursor>> {
        // Thin pass-through to the lease-work port. The sweeper
        // calls get_run separately for each entry to read the current
        // attempt count before deciding requeue vs dead-letter.
        match self.svc.store() {
            Some(store) => {
                LeaseWorkStore::scan_expired_leases(store.as_ref(), now_ms, limit, start_after)
                    .await
                    .map_err(|e| DataplaneError::Internal(format!("list_expired_leases: {e}")))
            }
            None => Ok(vec![]),
        }
    }

    async fn gc_expiry_index_entry(
        &self,
        expires_at_ms: i64,
        project_id: &str,
        run_id: &str,
    ) -> DataplaneResult<()> {
        match self.svc.store() {
            Some(store) => LeaseWorkStore::quarantine_expiry_entry(
                store.as_ref(),
                expires_at_ms,
                project_id,
                run_id,
            )
            .await
            .map_err(|e| DataplaneError::Internal(format!("gc_expiry_index_entry: {e}"))),
            None => Ok(()),
        }
    }

    async fn list_worker_leases(&self, worker_id: &str) -> DataplaneResult<Vec<(String, String)>> {
        match self.svc.store() {
            Some(store) => LeaseWorkStore::scan_worker_leases(store.as_ref(), worker_id)
                .await
                .map_err(|e| DataplaneError::Internal(format!("list_worker_leases: {e}"))),
            None => Err(DataplaneError::Internal(
                "worker lease projection is unavailable".into(),
            )),
        }
    }

    async fn list_active_leases(&self, limit: usize) -> DataplaneResult<Vec<(String, String)>> {
        // Scan the lease index, then fetch each Run proto and
        // extract its lease_id. Returns (run_id, lease_id) pairs which the
        // coordinator uses to rebuild the in-memory `active_leases` fence
        // map after restart.
        let store = match self.svc.store() {
            Some(s) => s,
            None => return Ok(vec![]),
        };
        let pairs = LeaseWorkStore::scan_active_lease_runs(store.as_ref(), limit)
            .await
            .map_err(|e| DataplaneError::Internal(format!("list_active_leases: {e}")))?;

        let mut out = Vec::with_capacity(pairs.len());
        for (project_id, run_id) in pairs {
            let bytes = match LeaseWorkStore::load_lease_run(store.as_ref(), &project_id, &run_id)
                .await
                .map_err(|e| DataplaneError::Internal(format!("get_run: {e}")))?
            {
                Some(b) => b,
                None => continue,
            };
            let run = match <Run as prost::Message>::decode(bytes.as_slice()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        project_id = %project_id,
                        run_id = %run_id,
                        error = %e,
                        "list_active_leases: failed to decode Run proto, skipping"
                    );
                    continue;
                }
            };
            // Skip runs whose lease has already been released between the
            // index scan and the Run fetch — the index entry will be
            // collected by the next sweeper pass.
            if run.lease_id.is_empty() {
                continue;
            }
            out.push((run.run_id, run.lease_id));
        }
        Ok(out)
    }

    async fn get_status(&self) -> DataplaneResult<GetStatusResponse> {
        Ok(self
            .svc
            .get_status(Request::new(GetStatusRequest {}))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
}
