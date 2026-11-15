//! gRPC `EngineBackend` implementation.
//!
//! Connects to a remote Engine via gRPC for use in disaggregated mode
//! (`--target=coordinator` or `--target=gateway`).

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

use engine_core::types::{LeaseExpiryCursor, Offset, Record};
use engine_proto::engine_service_client::EngineServiceClient;
use engine_proto::*;

use crate::backend::{
    check_observed_response, BackendAppendBatchOutcome, EngineBackend, RecordStream,
};
use crate::error::{DataplaneError, DataplaneResult};
use crate::util::grpc_client_pool_size;

/// gRPC engine backend — connects to a remote `EngineService`.
pub struct GrpcEngineBackend {
    clients: Vec<EngineServiceClient<Channel>>,
    next: AtomicUsize,
}

impl GrpcEngineBackend {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = if addr.starts_with("http") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let pool_size = grpc_client_pool_size();
        let mut clients = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            clients.push(EngineServiceClient::connect(endpoint.clone()).await?);
        }
        info!(addr = %addr, pool_size, "Connected to remote Engine");
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    fn next_client(&self) -> EngineServiceClient<Channel> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[idx].clone()
    }
}

/// Runtime-authored requests carry the peer token so the owner's Engine service
/// accepts internal controller state and lease fences from another runtime.
fn runtime_authored_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    let token = engine_core::lease_fence::runtime_peer_token(
        &engine_core::lease_fence::worker_session_signing_key(),
    );
    if let Ok(value) = token.parse() {
        request
            .metadata_mut()
            .insert(engine_core::lease_fence::RUNTIME_AUTHORED_HEADER, value);
    }
    request
}

fn grpc_err(s: tonic::Status) -> DataplaneError {
    DataplaneError::Grpc(s)
}

#[async_trait]
impl EngineBackend for GrpcEngineBackend {
    async fn lookup_invocation(
        &self,
        project_id: &str,
        run_id: &str,
        caller_key: &str,
    ) -> DataplaneResult<Option<Record>> {
        let response = self
            .next_client()
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
        // Old servers ignore the new request field and return history.
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
        let mut c = self.next_client();
        let resp = c
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
        let mut c = self.next_client();
        let resp = c
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
        let mut c = self.next_client();
        let response = c
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
        request: engine_proto::SuspendActivationRequest,
    ) -> DataplaneResult<engine_proto::SuspendActivationResponse> {
        let mut client = self.next_client();
        client
            .suspend_activation(Request::new(request))
            .await
            .map(|response| response.into_inner())
            .map_err(grpc_err)
    }

    async fn list_worker_leases(&self, _worker_id: &str) -> DataplaneResult<Vec<(String, String)>> {
        // The engine API has no call for the lease index, and an empty list
        // would tell the caller the worker holds nothing.
        Err(DataplaneError::Internal(
            "list_worker_leases is not supported by this engine backend".to_string(),
        ))
    }

    async fn read_by_run_id(
        &self,
        project_id: &str,
        run_id: &str,
        _from_offset: Offset,
        event_types: Option<Vec<String>>,
    ) -> DataplaneResult<Vec<Record>> {
        let mut c = self.next_client();
        let resp = c
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
        let mut c = self.next_client();
        let resp = c
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
        let mut c = self.next_client();
        let resp = c
            .get_run(Request::new(GetRunRequest {
                project_id: project_id.to_string(),
                run_id: run_id.to_string(),
            }))
            .await
            .map_err(grpc_err)?;
        let r = resp.into_inner();
        if r.found {
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
        let mut c = self.next_client();
        let resp = c
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
        let mut c = self.next_client();
        let resp = c
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
        let mut c = self.next_client();
        let resp = c
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

    async fn query(
        &self,
        req: engine_proto::QueryRequest,
    ) -> DataplaneResult<engine_proto::QueryResponse> {
        let mut client = self.next_client();
        let mut request = runtime_authored_request(req);
        request.set_timeout(std::time::Duration::from_secs(30));
        Ok(client.query(request).await.map_err(grpc_err)?.into_inner())
    }

    async fn list_runs(&self, req: ListRunsRequest) -> DataplaneResult<ListRunsResponse> {
        let mut c = self.next_client();
        Ok(c.list_runs(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_events(&self, req: ListEventsRequest) -> DataplaneResult<ListEventsResponse> {
        let mut c = self.next_client();
        Ok(c.list_events(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_run_stats(&self, req: GetRunStatsRequest) -> DataplaneResult<GetRunStatsResponse> {
        let mut c = self.next_client();
        Ok(c.get_run_stats(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_approval(&self, req: GetApprovalRequest) -> DataplaneResult<GetApprovalResponse> {
        let mut c = self.next_client();
        Ok(c.get_approval(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_approvals(
        &self,
        req: EngineListApprovalsRequest,
    ) -> DataplaneResult<EngineListApprovalsResponse> {
        let mut c = self.next_client();
        Ok(c.list_approvals(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_signal(&self, req: GetSignalRequest) -> DataplaneResult<GetSignalResponse> {
        let mut c = self.next_client();
        Ok(c.get_signal(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn lookup_signal(
        &self,
        req: LookupSignalRequest,
    ) -> DataplaneResult<LookupSignalResponse> {
        let mut c = self.next_client();
        Ok(c.lookup_signal(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_signals(&self, req: ListSignalsRequest) -> DataplaneResult<ListSignalsResponse> {
        let mut c = self.next_client();
        Ok(c.list_signals(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_timer(&self, req: GetTimerRequest) -> DataplaneResult<GetTimerResponse> {
        let mut c = self.next_client();
        Ok(c.get_timer(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_timers(&self, req: ListTimersRequest) -> DataplaneResult<ListTimersResponse> {
        let mut c = self.next_client();
        Ok(c.list_timers(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_due_timers(
        &self,
        req: ListDueTimersRequest,
    ) -> DataplaneResult<ListDueTimersResponse> {
        let mut c = self.next_client();
        Ok(c.list_due_timers(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_job(&self, req: GetJobRequest) -> DataplaneResult<GetJobResponse> {
        let mut c = self.next_client();
        Ok(c.get_job(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_jobs_by_run(
        &self,
        req: ListJobsByRunRequest,
    ) -> DataplaneResult<ListJobsByRunResponse> {
        let mut c = self.next_client();
        Ok(c.list_jobs_by_run(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_jobs_by_batch(
        &self,
        req: ListJobsByBatchRequest,
    ) -> DataplaneResult<ListJobsByBatchResponse> {
        let mut c = self.next_client();
        Ok(c.list_jobs_by_batch(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_batch(&self, req: GetBatchRequest) -> DataplaneResult<GetBatchResponse> {
        let mut c = self.next_client();
        Ok(c.get_batch(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_session(&self, req: GetSessionRequest) -> DataplaneResult<GetSessionResponse> {
        let mut c = self.next_client();
        Ok(c.get_session(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_sessions(
        &self,
        req: ListSessionsRequest,
    ) -> DataplaneResult<ListSessionsResponse> {
        let mut c = self.next_client();
        Ok(c.list_sessions(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn get_message(&self, req: GetMessageRequest) -> DataplaneResult<GetMessageResponse> {
        let mut c = self.next_client();
        Ok(c.get_message(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_messages_by_correlation(
        &self,
        req: ListMessagesByCorrelationRequest,
    ) -> DataplaneResult<ListMessagesByCorrelationResponse> {
        let mut c = self.next_client();
        Ok(c.list_messages_by_correlation(Request::new(req))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
    async fn list_pending_runs(
        &self,
        _project_id: &str,
        _component_names: &[String],
        _limit: usize,
    ) -> DataplaneResult<Vec<Vec<u8>>> {
        // Legacy batch polling is no longer supported. Pull workers use the
        // engine's parked PollJob RPC directly.
        Err(DataplaneError::Internal(
            "list_pending_runs not supported in disaggregated mode".into(),
        ))
    }

    async fn list_expired_leases(
        &self,
        _now_ms: i64,
        _limit: usize,
        _start_after: Option<LeaseExpiryCursor>,
    ) -> DataplaneResult<Vec<LeaseExpiryCursor>> {
        // In disaggregated mode the coordinator does not run the
        // expiry sweeper — lease lifecycle is the engine's responsibility
        // when the two run in separate processes. Returning empty makes
        // the sweeper a no-op (it'll log "nothing expired" each tick).
        // TODO: add a `ListExpiredLeases` engine RPC and wire it here
        // when disaggregated mode becomes load-bearing.
        tracing::debug!(
            "list_expired_leases stubbed in disaggregated backend; \
             expiry sweeper will be a no-op"
        );
        Ok(vec![])
    }

    async fn gc_expiry_index_entry(
        &self,
        _expires_at_ms: i64,
        _project_id: &str,
        _run_id: &str,
    ) -> DataplaneResult<()> {
        // Disaggregated mode doesn't run the sweeper, so orphan GC never
        // fires here. No-op to satisfy the trait.
        Ok(())
    }

    async fn list_active_leases(&self, _limit: usize) -> DataplaneResult<Vec<(String, String)>> {
        // In disaggregated mode the coordinator can't reach the
        // engine's lease index without a dedicated RPC. Returning an empty
        // list is correctness-preserving — the per-invocation fence still
        // rejects mismatched acks; the only loss is observability for the
        // brief window between coordinator restart and the next dispatch.
        // TODO: add a `ListActiveLeases` engine RPC and wire it here when
        // disaggregated mode becomes load-bearing.
        tracing::debug!(
            "list_active_leases stubbed in disaggregated backend; \
             coordinator fence map starts empty"
        );
        Ok(vec![])
    }

    async fn get_status(&self) -> DataplaneResult<GetStatusResponse> {
        let mut c = self.next_client();
        Ok(c.get_status(Request::new(GetStatusRequest {}))
            .await
            .map_err(grpc_err)?
            .into_inner())
    }
}
