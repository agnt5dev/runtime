//! Worker registration, polling, leases, and completion for one active runtime.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use agnt5_core::{
    AppendOutcome, MaterializedStore, NewJournalRecord, Offset, PendingJob, RunState, RuntimeEvent,
    Segment,
};
use agnt5_processor::{decode_run, run_key, PENDING, RUNS};
use agnt5_proto::{
    api::v1::{
        engine_service_server::EngineService,
        execution_engine_service_server::ExecutionEngineService, CompleteJobRequest,
        CompleteJobResponse, ComponentType as LegacyComponentType, JobAssignment, PollJobRequest,
        PollJobResponse, RegisterWorkerSessionRequest, RegisterWorkerSessionResponse,
        RenewJobLeaseRequest, RenewJobLeaseResponse, WorkerSlotPolicy, WriteCheckpointRequest,
        WriteCheckpointResponse,
    },
    protocol::v2::{
        payload, poll_run_response, protocol_service_server::ProtocolService, run_outcome,
        worker_service_server::WorkerService, AppendRunEventsRequest, AppendRunEventsResponse,
        CommitDisposition, CommitRunOutcomeRequest, CommitRunOutcomeResponse, ComponentDescriptor,
        ComponentTarget, ComponentType, ExecuteRunRequest, GetCapabilitiesRequest,
        GetCapabilitiesResponse, Payload, PollIdle, PollRunRequest, PollRunResponse,
        ProtocolLimits, ProtocolVersion, PublishRunOutputRequest, PublishRunOutputResponse,
        RegisterWorkerRequest, RegisterWorkerResponse, RenewRunLeaseRequest, RenewRunLeaseResponse,
        RunStatus, UnregisterWorkerRequest, UnregisterWorkerResponse,
    },
};
use bytes::Bytes;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const DEFAULT_SESSION_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);
const DEFAULT_MAXIMUM_POLL_WAIT: Duration = Duration::from_secs(30);
const DEFAULT_RENEW_INTERVAL: Duration = Duration::from_secs(10);
const IDLE_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    pub session_lifetime: Duration,
    pub lease_duration: Duration,
    pub maximum_poll_wait: Duration,
    pub recommended_renew_interval: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            session_lifetime: DEFAULT_SESSION_LIFETIME,
            lease_duration: DEFAULT_LEASE_DURATION,
            maximum_poll_wait: DEFAULT_MAXIMUM_POLL_WAIT,
            recommended_renew_interval: DEFAULT_RENEW_INTERVAL,
        }
    }
}

#[derive(Clone)]
struct ComponentRegistration {
    component_type: String,
    name: String,
    version: String,
}

#[derive(Clone)]
struct WorkerSession {
    worker_id: String,
    components: Vec<ComponentRegistration>,
    max_concurrency: u32,
    expires_at_ms: i64,
}

#[derive(Clone)]
struct ExecutionLease {
    session_id: Uuid,
    run_id: String,
    lease_id: String,
    lease_expires_at_ms: i64,
    active: bool,
}

enum DispatchCandidate {
    Queued(PendingJob),
    Expired(RunState),
}

impl DispatchCandidate {
    fn run_id(&self) -> &str {
        match self {
            Self::Queued(job) => &job.run_id,
            Self::Expired(run) => &run.run_id,
        }
    }
}

struct ClaimedRun {
    run: RunState,
    lease_id: String,
    lease_expires_at_ms: i64,
}

#[derive(Clone, Default)]
pub struct CheckpointService;

#[tonic::async_trait]
impl ExecutionEngineService for CheckpointService {
    async fn write_checkpoint(
        &self,
        request: Request<WriteCheckpointRequest>,
    ) -> Result<Response<WriteCheckpointResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(WriteCheckpointResponse {
            success: true,
            sequence_number: request.sequence_number,
            error_message: String::new(),
        }))
    }
}

pub struct Coordinator<S: Segment, M: MaterializedStore> {
    project_id: String,
    segment: Arc<S>,
    store: Arc<M>,
    config: CoordinatorConfig,
    sessions: RwLock<HashMap<Uuid, WorkerSession>>,
    execution_tokens: RwLock<HashMap<Uuid, ExecutionLease>>,
    poll_results: RwLock<HashMap<(Uuid, String), PollRunResponse>>,
    coordination_lock: Mutex<()>,
}

impl<S: Segment, M: MaterializedStore> Coordinator<S, M> {
    pub fn new(project_id: impl Into<String>, segment: Arc<S>, store: Arc<M>) -> Self {
        Self::with_config(project_id, segment, store, CoordinatorConfig::default())
    }

    pub fn with_config(
        project_id: impl Into<String>,
        segment: Arc<S>,
        store: Arc<M>,
        config: CoordinatorConfig,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            segment,
            store,
            config,
            sessions: RwLock::new(HashMap::new()),
            execution_tokens: RwLock::new(HashMap::new()),
            poll_results: RwLock::new(HashMap::new()),
            coordination_lock: Mutex::new(()),
        }
    }

    async fn legacy_session(
        &self,
        worker_id: &str,
        session_id: &str,
    ) -> Result<WorkerSession, Status> {
        let sessions = self.sessions.read().await;
        let session = if session_id.is_empty() {
            sessions
                .values()
                .find(|session| session.worker_id == worker_id)
        } else {
            Uuid::parse_str(session_id)
                .ok()
                .and_then(|id| sessions.get(&id))
                .filter(|session| session.worker_id == worker_id)
        };
        valid_session(session)
    }

    async fn v2_session(&self, token: &[u8]) -> Result<(Uuid, WorkerSession), Status> {
        let session_id = token_uuid(token, "worker_session_token")?;
        let sessions = self.sessions.read().await;
        let session = valid_session(sessions.get(&session_id))?;
        Ok((session_id, session))
    }

    async fn execution(&self, token: &[u8]) -> Result<(Uuid, ExecutionLease), Status> {
        let token_id = token_uuid(token, "execution_token")?;
        let execution = self
            .execution_tokens
            .read()
            .await
            .get(&token_id)
            .cloned()
            .ok_or_else(|| Status::unauthenticated("execution token is invalid"))?;
        let sessions = self.sessions.read().await;
        valid_session(sessions.get(&execution.session_id))?;
        Ok((token_id, execution))
    }

    async fn append(&self, key: String, event: RuntimeEvent) -> Result<AppendOutcome, Status> {
        let payload = serde_json::to_vec(&event).map_err(internal)?;
        self.segment
            .append_batch(&[NewJournalRecord {
                idempotency_key: Some(key.into_bytes()),
                payload: Bytes::from(payload),
            }])
            .await
            .map_err(internal)
    }

    async fn existing_event(&self, key: &[u8]) -> Result<Option<RuntimeEvent>, Status> {
        let tail = self.segment.tail_offset().await.map_err(internal)?;
        let records = self
            .segment
            .read_range(Offset::ZERO, tail)
            .await
            .map_err(internal)?;
        records
            .into_iter()
            .find(|record| record.idempotency_key.as_deref() == Some(key))
            .map(|record| serde_json::from_slice(&record.payload).map_err(internal))
            .transpose()
    }

    async fn find_candidate(
        &self,
        session: &WorkerSession,
    ) -> Result<Option<DispatchCandidate>, Status> {
        let pending = self
            .store
            .scan_prefix(PENDING, b"", 100)
            .await
            .map_err(internal)?;
        for (_, value) in pending {
            let job: PendingJob = serde_json::from_slice(&value).map_err(internal)?;
            if supports(session, &job.component_type, &job.component_name) {
                return Ok(Some(DispatchCandidate::Queued(job)));
            }
        }

        let runs = self
            .store
            .scan_prefix(RUNS, b"", 100)
            .await
            .map_err(internal)?;
        let now = now_ms();
        for (_, value) in runs {
            let run = decode_run(&value).map_err(internal)?;
            if run.status == "running"
                && run
                    .lease_expires_at_ms
                    .is_some_and(|expires| expires <= now)
                && run.lease_id.is_some()
                && supports(session, &run.component_type, &run.component_name)
            {
                return Ok(Some(DispatchCandidate::Expired(run)));
            }
        }
        Ok(None)
    }

    async fn claim_run(
        &self,
        session: &WorkerSession,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<ClaimedRun>, Status> {
        let Some(candidate) = self.find_candidate(session).await? else {
            return Ok(None);
        };
        let run_id = candidate.run_id().to_string();
        let lease_id = Uuid::now_v7().to_string();
        let lease_expires_at_ms = now_ms() + duration_ms(lease_duration);
        let event = match candidate {
            DispatchCandidate::Queued(_) => RuntimeEvent::JobClaimed {
                project_id: self.project_id.clone(),
                run_id: run_id.clone(),
                worker_id: worker_id.to_string(),
                lease_id: lease_id.clone(),
                lease_expires_at_ms,
            },
            DispatchCandidate::Expired(run) => {
                let previous_lease_id = run.lease_id.expect("expired candidate has a lease");
                self.execution_tokens
                    .write()
                    .await
                    .values_mut()
                    .for_each(|execution| {
                        if execution.run_id == run_id && execution.lease_id == previous_lease_id {
                            execution.active = false;
                        }
                    });
                RuntimeEvent::JobReclaimed {
                    project_id: self.project_id.clone(),
                    run_id: run_id.clone(),
                    worker_id: worker_id.to_string(),
                    previous_lease_id,
                    lease_id: lease_id.clone(),
                    lease_expires_at_ms,
                }
            }
        };
        let _ = self
            .append(format!("claim:{run_id}:{lease_id}"), event)
            .await?;
        self.wait_for_lease(&run_id, &lease_id).await?;
        let run = self.load_run(&run_id).await?;
        Ok(Some(ClaimedRun {
            run,
            lease_id,
            lease_expires_at_ms,
        }))
    }

    async fn load_run(&self, run_id: &str) -> Result<RunState, Status> {
        let value = self
            .store
            .get(RUNS, &run_key(run_id))
            .await
            .map_err(internal)?
            .ok_or_else(|| Status::not_found("run not found"))?;
        decode_run(&value).map_err(internal)
    }

    async fn wait_for_lease(&self, run_id: &str, lease_id: &str) -> Result<(), Status> {
        for _ in 0..200 {
            if let Some(value) = self
                .store
                .get(RUNS, &run_key(run_id))
                .await
                .map_err(internal)?
            {
                let run = decode_run(&value).map_err(internal)?;
                if run.lease_id.as_deref() == Some(lease_id) {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(Status::unavailable("run projection did not catch up"))
    }

    async fn wait_for_lease_expiry(
        &self,
        run_id: &str,
        lease_id: &str,
        lease_expires_at_ms: i64,
    ) -> Result<(), Status> {
        for _ in 0..200 {
            let run = self.load_run(run_id).await?;
            if run.lease_id.as_deref() == Some(lease_id)
                && run.lease_expires_at_ms == Some(lease_expires_at_ms)
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(Status::unavailable("lease projection did not catch up"))
    }

    async fn active_execution_count(&self, session_id: Uuid) -> usize {
        let now = now_ms();
        self.execution_tokens
            .read()
            .await
            .values()
            .filter(|execution| {
                execution.session_id == session_id
                    && execution.active
                    && execution.lease_expires_at_ms > now
            })
            .count()
    }

    async fn remove_session_state(&self, session_ids: &HashSet<Uuid>) {
        self.execution_tokens
            .write()
            .await
            .retain(|_, execution| !session_ids.contains(&execution.session_id));
        self.poll_results
            .write()
            .await
            .retain(|(session_id, _), _| !session_ids.contains(session_id));
    }
}

#[tonic::async_trait]
impl<S: Segment, M: MaterializedStore> EngineService for Coordinator<S, M> {
    async fn register_worker_session(
        &self,
        request: Request<RegisterWorkerSessionRequest>,
    ) -> Result<Response<RegisterWorkerSessionResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        if !request.project_id.is_empty() && request.project_id != self.project_id {
            return Err(Status::permission_denied(
                "project_id does not match this runtime",
            ));
        }
        let session_id = Uuid::now_v7();
        let mut components: Vec<_> = request
            .components
            .into_iter()
            .map(|component| ComponentRegistration {
                component_type: legacy_component_type_name(component.component_type),
                name: component.name,
                version: String::new(),
            })
            .collect();
        components.extend(request.capabilities.into_iter().map(|capability| {
            ComponentRegistration {
                component_type: legacy_component_type_name(capability.component_type),
                name: capability.component_name,
                version: String::new(),
            }
        }));
        let expires_at_ms = now_ms() + duration_ms(self.config.session_lifetime);
        self.sessions.write().await.insert(
            session_id,
            WorkerSession {
                worker_id: request.worker_id,
                components,
                max_concurrency: request.max_slots.max(1),
                expires_at_ms,
            },
        );
        let max_slots = request.max_slots.max(1);
        Ok(Response::new(RegisterWorkerSessionResponse {
            worker_session_id: session_id.to_string(),
            expires_at_ms,
            effective_slot_policy: Some(request.slot_policy.unwrap_or(WorkerSlotPolicy {
                min_slots: 1,
                max_slots,
                target_cpu_usage: 0.0,
                target_memory_usage: 0.0,
                ramp_throttle_ms: 0,
            })),
        }))
    }

    async fn poll_job(
        &self,
        request: Request<PollJobRequest>,
    ) -> Result<Response<PollJobResponse>, Status> {
        let request = request.into_inner();
        let session = self
            .legacy_session(&request.worker_id, &request.worker_session_id)
            .await?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(request.wait_ms.clamp(0, 30_000) as u64);
        loop {
            let _coordination = self.coordination_lock.lock().await;
            let lease_ms = request
                .claim_timeout_ms
                .max(duration_ms(self.config.lease_duration));
            if let Some(claimed) = self
                .claim_run(
                    &session,
                    &request.worker_id,
                    Duration::from_millis(lease_ms as u64),
                )
                .await?
            {
                return Ok(Response::new(PollJobResponse {
                    job: Some(JobAssignment {
                        job_id: claimed.run.run_id.clone(),
                        run_id: claimed.run.run_id,
                        component_id: claimed.run.component_name.clone(),
                        component_type: legacy_component_type_number(&claimed.run.component_type),
                        component_name: claimed.run.component_name,
                        input_data: claimed.run.input_data,
                        metadata: HashMap::new(),
                        attempt: claimed.run.attempt as i32,
                        timeout_ms: lease_ms,
                        trace_id: String::new(),
                        lease_id: claimed.lease_id,
                        lease_expires_at_ms: claimed.lease_expires_at_ms,
                    }),
                }));
            }
            drop(_coordination);
            if tokio::time::Instant::now() >= deadline {
                return Ok(Response::new(PollJobResponse { job: None }));
            }
            tokio::time::sleep(IDLE_RETRY_DELAY).await;
        }
    }

    async fn renew_job_lease(
        &self,
        request: Request<RenewJobLeaseRequest>,
    ) -> Result<Response<RenewJobLeaseResponse>, Status> {
        let request = request.into_inner();
        self.legacy_session(&request.worker_id, &request.worker_session_id)
            .await?;
        let _coordination = self.coordination_lock.lock().await;
        validate_lease(&*self.store, &request.run_id, &request.lease_id).await?;
        let lease_ms = request
            .lease_timeout_ms
            .max(duration_ms(self.config.lease_duration));
        let expires_at = now_ms() + lease_ms;
        let run_id = request.run_id;
        let lease_id = request.lease_id;
        let _ = self
            .append(
                format!("renew:{run_id}:{lease_id}:{expires_at}"),
                RuntimeEvent::JobLeaseRenewed {
                    project_id: self.project_id.clone(),
                    run_id: run_id.clone(),
                    lease_id: lease_id.clone(),
                    lease_expires_at_ms: expires_at,
                },
            )
            .await?;
        self.wait_for_lease_expiry(&run_id, &lease_id, expires_at)
            .await?;
        Ok(Response::new(RenewJobLeaseResponse {
            renewed: true,
            lease_expires_at_ms: expires_at,
        }))
    }

    async fn complete_job(
        &self,
        request: Request<CompleteJobRequest>,
    ) -> Result<Response<CompleteJobResponse>, Status> {
        let request = request.into_inner();
        self.legacy_session(&request.worker_id, &request.worker_session_id)
            .await?;
        if !request.project_id.is_empty() && request.project_id != self.project_id {
            return Err(Status::permission_denied(
                "project_id does not match this runtime",
            ));
        }
        let _coordination = self.coordination_lock.lock().await;
        validate_lease(&*self.store, &request.job_id, &request.lease_id).await?;
        let event = if request.success {
            RuntimeEvent::JobCompleted {
                project_id: self.project_id.clone(),
                run_id: request.job_id.clone(),
                lease_id: request.lease_id.clone(),
                output_data: request.output_data,
                completed_at_ms: now_ms(),
            }
        } else {
            RuntimeEvent::JobFailed {
                project_id: self.project_id.clone(),
                run_id: request.job_id.clone(),
                lease_id: request.lease_id.clone(),
                error_message: request.error_message,
                error_code: request.error_code,
                completed_at_ms: now_ms(),
            }
        };
        let _ = self
            .append(
                format!("complete:{}:{}", request.job_id, request.lease_id),
                event,
            )
            .await?;
        Ok(Response::new(CompleteJobResponse { acknowledged: true }))
    }
}

#[tonic::async_trait]
impl<S: Segment, M: MaterializedStore> ProtocolService for Coordinator<S, M> {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        let request = request.into_inner();
        let selected = negotiate_protocol(
            request.minimum_protocol,
            request.maximum_protocol,
            &request.capabilities,
        )?;
        Ok(Response::new(GetCapabilitiesResponse {
            selected_protocol: Some(selected),
            capabilities: Vec::new(),
            runtime_name: "agnt5-runtime".into(),
            runtime_version: env!("CARGO_PKG_VERSION").into(),
            limits: Some(protocol_limits()),
        }))
    }
}

#[tonic::async_trait]
impl<S: Segment, M: MaterializedStore> WorkerService for Coordinator<S, M> {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        let selected = negotiate_protocol(
            request.minimum_protocol,
            request.maximum_protocol,
            &request.capabilities,
        )?;
        let components = v2_components(request.components)?;
        let session_id = Uuid::now_v7();
        let expires_at_ms = now_ms() + duration_ms(self.config.session_lifetime);
        let _coordination = self.coordination_lock.lock().await;

        let mut sessions = self.sessions.write().await;
        let fenced: HashSet<_> = sessions
            .iter()
            .filter_map(|(id, session)| (session.worker_id == request.worker_id).then_some(*id))
            .collect();
        sessions.retain(|id, _| !fenced.contains(id));
        sessions.insert(
            session_id,
            WorkerSession {
                worker_id: request.worker_id,
                components,
                max_concurrency: request.max_concurrency.max(1),
                expires_at_ms,
            },
        );
        drop(sessions);
        self.remove_session_state(&fenced).await;

        Ok(Response::new(RegisterWorkerResponse {
            worker_session_token: session_id.as_bytes().to_vec(),
            session_expires_at: Some(timestamp(expires_at_ms)),
            selected_protocol: Some(selected),
            capabilities: Vec::new(),
            maximum_poll_wait: Some(proto_duration(self.config.maximum_poll_wait)),
            lease_duration: Some(proto_duration(self.config.lease_duration)),
            recommended_renew_interval: Some(proto_duration(
                self.config.recommended_renew_interval,
            )),
            limits: Some(protocol_limits()),
        }))
    }

    async fn unregister_worker(
        &self,
        request: Request<UnregisterWorkerRequest>,
    ) -> Result<Response<UnregisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let (session_id, _) = self.v2_session(&request.worker_session_token).await?;
        let _coordination = self.coordination_lock.lock().await;
        self.v2_session(&request.worker_session_token).await?;
        self.sessions.write().await.remove(&session_id);
        self.remove_session_state(&HashSet::from([session_id]))
            .await;
        Ok(Response::new(UnregisterWorkerResponse {}))
    }

    async fn poll_run(
        &self,
        request: Request<PollRunRequest>,
    ) -> Result<Response<PollRunResponse>, Status> {
        let request = request.into_inner();
        if request.poll_id.trim().is_empty() {
            return Err(Status::invalid_argument("poll_id is required"));
        }
        let (session_id, session) = self.v2_session(&request.worker_session_token).await?;
        let poll_key = (session_id, request.poll_id.clone());
        if let Some(result) = self.poll_results.read().await.get(&poll_key).cloned() {
            return Ok(Response::new(result));
        }
        let wait = request
            .wait_timeout
            .as_ref()
            .map(duration_from_proto)
            .transpose()?
            .unwrap_or(self.config.maximum_poll_wait)
            .min(self.config.maximum_poll_wait);
        let deadline = tokio::time::Instant::now() + wait;

        loop {
            let _coordination = self.coordination_lock.lock().await;
            let (_, current_session) = self.v2_session(&request.worker_session_token).await?;
            if let Some(result) = self.poll_results.read().await.get(&poll_key).cloned() {
                return Ok(Response::new(result));
            }
            if self.active_execution_count(session_id).await
                >= current_session.max_concurrency as usize
            {
                return Err(Status::resource_exhausted(
                    "worker has no available execution slots",
                ));
            }
            if let Some(claimed) = self
                .claim_run(&session, &session.worker_id, self.config.lease_duration)
                .await?
            {
                let execution_token = Uuid::now_v7();
                let execution_id = Uuid::now_v7().to_string();
                let target_version = component_version(
                    &session,
                    &claimed.run.component_type,
                    &claimed.run.component_name,
                );
                self.execution_tokens.write().await.insert(
                    execution_token,
                    ExecutionLease {
                        session_id,
                        run_id: claimed.run.run_id.clone(),
                        lease_id: claimed.lease_id,
                        lease_expires_at_ms: claimed.lease_expires_at_ms,
                        active: true,
                    },
                );
                let input_size = claimed.run.input_data.len() as u64;
                let result = PollRunResponse {
                    result: Some(poll_run_response::Result::Execute(Box::new(
                        ExecuteRunRequest {
                            run_id: claimed.run.run_id,
                            target: Some(ComponentTarget {
                                r#type: v2_component_type_number(&claimed.run.component_type),
                                name: claimed.run.component_name,
                                version: target_version,
                                method: String::new(),
                                instance_key: String::new(),
                            }),
                            input: Some(Payload {
                                body: Some(payload::Body::InlineData(claimed.run.input_data)),
                                content_type: "application/json".into(),
                                content_encoding: String::new(),
                                size_bytes: input_size,
                                sha256: Vec::new(),
                            }),
                            checkpoint: None,
                            attempt: claimed.run.attempt,
                            execution_timeout: None,
                            trace_context: None,
                            metadata: HashMap::new(),
                            execution_token: execution_token.as_bytes().to_vec(),
                            lease_expires_at: Some(timestamp(claimed.lease_expires_at_ms)),
                            execution_id,
                            effective_run_policy: None,
                            effective_run_policy_digest: Vec::new(),
                            application_context: None,
                        },
                    ))),
                };
                self.poll_results
                    .write()
                    .await
                    .insert(poll_key, result.clone());
                return Ok(Response::new(result));
            }
            if tokio::time::Instant::now() >= deadline {
                let result = PollRunResponse {
                    result: Some(poll_run_response::Result::Idle(PollIdle {
                        retry_at: Some(timestamp(now_ms() + duration_ms(IDLE_RETRY_DELAY))),
                    })),
                };
                self.poll_results
                    .write()
                    .await
                    .insert(poll_key, result.clone());
                return Ok(Response::new(result));
            }
            drop(_coordination);
            tokio::time::sleep(IDLE_RETRY_DELAY).await;
        }
    }

    async fn renew_run_lease(
        &self,
        request: Request<RenewRunLeaseRequest>,
    ) -> Result<Response<RenewRunLeaseResponse>, Status> {
        let request = request.into_inner();
        let (token_id, execution) = self.execution(&request.execution_token).await?;
        if !execution.active {
            return Err(Status::failed_precondition(
                "execution lease is no longer active",
            ));
        }
        let _coordination = self.coordination_lock.lock().await;
        validate_lease(&*self.store, &execution.run_id, &execution.lease_id).await?;
        let expires_at = now_ms() + duration_ms(self.config.lease_duration);
        let run_id = execution.run_id;
        let lease_id = execution.lease_id;
        let _ = self
            .append(
                format!("renew:{run_id}:{lease_id}:{expires_at}"),
                RuntimeEvent::JobLeaseRenewed {
                    project_id: self.project_id.clone(),
                    run_id: run_id.clone(),
                    lease_id: lease_id.clone(),
                    lease_expires_at_ms: expires_at,
                },
            )
            .await?;
        self.wait_for_lease_expiry(&run_id, &lease_id, expires_at)
            .await?;
        if let Some(current) = self.execution_tokens.write().await.get_mut(&token_id) {
            current.lease_expires_at_ms = expires_at;
        }
        Ok(Response::new(RenewRunLeaseResponse {
            lease_expires_at: Some(timestamp(expires_at)),
            cancellation_requested: false,
            cancellation_reason: String::new(),
        }))
    }

    async fn append_run_events(
        &self,
        _request: Request<AppendRunEventsRequest>,
    ) -> Result<Response<AppendRunEventsResponse>, Status> {
        Err(Status::unimplemented(
            "durable event append is not implemented by this runtime bridge",
        ))
    }

    async fn publish_run_output(
        &self,
        _request: Request<PublishRunOutputRequest>,
    ) -> Result<Response<PublishRunOutputResponse>, Status> {
        Err(Status::unimplemented(
            "live output streaming is not implemented by this runtime bridge",
        ))
    }

    async fn commit_run_outcome(
        &self,
        request: Request<CommitRunOutcomeRequest>,
    ) -> Result<Response<CommitRunOutcomeResponse>, Status> {
        let request = request.into_inner();
        if request.commit_id.trim().is_empty() {
            return Err(Status::invalid_argument("commit_id is required"));
        }
        if !request.final_events.is_empty() || request.expected_last_event_sequence.is_some() {
            return Err(Status::unimplemented(
                "atomic final events are not implemented by this runtime bridge",
            ));
        }
        let (token_id, execution) = self.execution(&request.execution_token).await?;
        let event = outcome_event(
            &self.project_id,
            &execution.run_id,
            &execution.lease_id,
            request.outcome,
        )?;
        let key = format!(
            "v2:commit:{}:{}:{}",
            execution.run_id, execution.lease_id, request.commit_id
        );
        let _coordination = self.coordination_lock.lock().await;
        if let Some(existing) = self.existing_event(key.as_bytes()).await? {
            if !same_outcome(&existing, &event) {
                return Err(Status::already_exists(
                    "commit_id was already used with a different outcome",
                ));
            }
            let (committed_at_ms, run_status) = committed_result(&existing)?;
            return Ok(Response::new(CommitRunOutcomeResponse {
                disposition: CommitDisposition::AlreadyCommitted as i32,
                committed_at: Some(timestamp(committed_at_ms)),
                accepted_through_sequence: 0,
                run_status: run_status as i32,
            }));
        }
        if !execution.active {
            return Err(Status::failed_precondition(
                "execution lease is no longer active",
            ));
        }
        validate_lease(&*self.store, &execution.run_id, &execution.lease_id).await?;
        let (committed_at_ms, run_status) = committed_result(&event)?;
        let outcome = self.append(key, event.clone()).await?;
        if outcome.appended.first() != Some(&true) {
            let existing = self
                .existing_event(
                    format!(
                        "v2:commit:{}:{}:{}",
                        execution.run_id, execution.lease_id, request.commit_id
                    )
                    .as_bytes(),
                )
                .await?
                .ok_or_else(|| Status::internal("idempotent commit record is missing"))?;
            if !same_outcome(&existing, &event) {
                return Err(Status::already_exists(
                    "commit_id was already used with a different outcome",
                ));
            }
        }
        if let Some(current) = self.execution_tokens.write().await.get_mut(&token_id) {
            current.active = false;
        }
        Ok(Response::new(CommitRunOutcomeResponse {
            disposition: CommitDisposition::Committed as i32,
            committed_at: Some(timestamp(committed_at_ms)),
            accepted_through_sequence: 0,
            run_status: run_status as i32,
        }))
    }
}

async fn validate_lease<M: MaterializedStore>(
    store: &M,
    run_id: &str,
    lease_id: &str,
) -> Result<(), Status> {
    let value = store
        .get(RUNS, &run_key(run_id))
        .await
        .map_err(internal)?
        .ok_or_else(|| Status::not_found("run not found"))?;
    let run = decode_run(&value).map_err(internal)?;
    if run.status != "running"
        || run.lease_id.as_deref() != Some(lease_id)
        || run
            .lease_expires_at_ms
            .is_none_or(|expires| expires <= now_ms())
    {
        return Err(Status::failed_precondition(
            "execution lease is no longer current",
        ));
    }
    Ok(())
}

fn valid_session(session: Option<&WorkerSession>) -> Result<WorkerSession, Status> {
    session
        .filter(|session| session.expires_at_ms > now_ms())
        .cloned()
        .ok_or_else(|| Status::unauthenticated("worker session is invalid or expired"))
}

fn supports(session: &WorkerSession, component_type: &str, component_name: &str) -> bool {
    let component_type = canonical_component_type(component_type);
    session.components.iter().any(|component| {
        (component.component_type.is_empty() || component.component_type == component_type)
            && (component.name.is_empty() || component.name == component_name)
    })
}

fn component_version(
    session: &WorkerSession,
    component_type: &str,
    component_name: &str,
) -> String {
    let component_type = canonical_component_type(component_type);
    session
        .components
        .iter()
        .find(|component| {
            (component.component_type.is_empty() || component.component_type == component_type)
                && (component.name.is_empty() || component.name == component_name)
        })
        .map(|component| component.version.clone())
        .unwrap_or_default()
}

fn v2_components(
    components: Vec<ComponentDescriptor>,
) -> Result<Vec<ComponentRegistration>, Status> {
    components
        .into_iter()
        .map(|component| {
            if component.name.trim().is_empty() {
                return Err(Status::invalid_argument("component name is required"));
            }
            if component
                .run_policy
                .as_ref()
                .is_some_and(|policy| policy != &Default::default())
            {
                return Err(Status::failed_precondition(
                    "run.policy is not supported by this runtime bridge",
                ));
            }
            let component_type =
                ComponentType::try_from(component.r#type).unwrap_or(ComponentType::Unspecified);
            let component_type = match component_type {
                ComponentType::Unspecified => {
                    return Err(Status::invalid_argument("component type is required"));
                }
                ComponentType::Function => "function",
                ComponentType::Workflow => "workflow",
                ComponentType::Entity => "entity",
                ComponentType::Agent => "agent",
                ComponentType::Tool => "tool",
                ComponentType::Scorer => "scorer",
            };
            Ok(ComponentRegistration {
                component_type: component_type.into(),
                name: component.name,
                version: component.version,
            })
        })
        .collect()
}

fn negotiate_protocol(
    minimum: Option<ProtocolVersion>,
    maximum: Option<ProtocolVersion>,
    requirements: &[agnt5_proto::protocol::v2::CapabilityRequirement],
) -> Result<ProtocolVersion, Status> {
    let selected = ProtocolVersion { major: 2, minor: 0 };
    let minimum = minimum.unwrap_or(selected);
    let maximum = maximum.unwrap_or(selected);
    let min = (minimum.major, minimum.minor);
    let max = (maximum.major, maximum.minor);
    let target = (selected.major, selected.minor);
    if min > max {
        return Err(Status::invalid_argument(
            "minimum_protocol must not exceed maximum_protocol",
        ));
    }
    if target < min || target > max {
        return Err(Status::failed_precondition(
            "runtime does not support the requested protocol range",
        ));
    }
    if let Some(requirement) = requirements.iter().find(|requirement| requirement.required) {
        return Err(Status::failed_precondition(format!(
            "required capability '{}' is not supported",
            requirement.name
        )));
    }
    Ok(selected)
}

fn outcome_event(
    project_id: &str,
    run_id: &str,
    lease_id: &str,
    outcome: Option<agnt5_proto::protocol::v2::RunOutcome>,
) -> Result<RuntimeEvent, Status> {
    match outcome.and_then(|outcome| outcome.kind) {
        Some(run_outcome::Kind::Completed(completed)) => Ok(RuntimeEvent::JobCompleted {
            project_id: project_id.into(),
            run_id: run_id.into(),
            lease_id: lease_id.into(),
            output_data: inline_payload(completed.output, "completed output")?,
            completed_at_ms: now_ms(),
        }),
        Some(run_outcome::Kind::Failed(failed)) => {
            let failure = failed
                .failure
                .ok_or_else(|| Status::invalid_argument("failure is required"))?;
            Ok(RuntimeEvent::JobFailed {
                project_id: project_id.into(),
                run_id: run_id.into(),
                lease_id: lease_id.into(),
                error_message: failure.message,
                error_code: failure.code,
                completed_at_ms: now_ms(),
            })
        }
        Some(run_outcome::Kind::Cancelled(_)) => Err(Status::unimplemented(
            "cancelled outcomes are not implemented by this runtime bridge",
        )),
        Some(run_outcome::Kind::Suspended(_)) => Err(Status::unimplemented(
            "suspended outcomes are not implemented by this runtime bridge",
        )),
        Some(run_outcome::Kind::Yielded(_)) => Err(Status::unimplemented(
            "yielded outcomes are not implemented by this runtime bridge",
        )),
        None => Err(Status::invalid_argument("outcome is required")),
    }
}

fn inline_payload(payload: Option<Payload>, field: &str) -> Result<Vec<u8>, Status> {
    match payload.and_then(|payload| payload.body) {
        Some(payload::Body::InlineData(data)) => Ok(data),
        Some(payload::Body::Reference(_)) => Err(Status::unimplemented(format!(
            "referenced {field} is not implemented by this runtime bridge"
        ))),
        None => Ok(Vec::new()),
    }
}

fn same_outcome(left: &RuntimeEvent, right: &RuntimeEvent) -> bool {
    match (left, right) {
        (
            RuntimeEvent::JobCompleted {
                project_id: lp,
                run_id: lr,
                lease_id: ll,
                output_data: lo,
                ..
            },
            RuntimeEvent::JobCompleted {
                project_id: rp,
                run_id: rr,
                lease_id: rl,
                output_data: ro,
                ..
            },
        ) => lp == rp && lr == rr && ll == rl && lo == ro,
        (
            RuntimeEvent::JobFailed {
                project_id: lp,
                run_id: lr,
                lease_id: ll,
                error_message: lm,
                error_code: lc,
                ..
            },
            RuntimeEvent::JobFailed {
                project_id: rp,
                run_id: rr,
                lease_id: rl,
                error_message: rm,
                error_code: rc,
                ..
            },
        ) => lp == rp && lr == rr && ll == rl && lm == rm && lc == rc,
        _ => false,
    }
}

fn committed_result(event: &RuntimeEvent) -> Result<(i64, RunStatus), Status> {
    match event {
        RuntimeEvent::JobCompleted {
            completed_at_ms, ..
        } => Ok((*completed_at_ms, RunStatus::Completed)),
        RuntimeEvent::JobFailed {
            completed_at_ms, ..
        } => Ok((*completed_at_ms, RunStatus::Failed)),
        _ => Err(Status::internal("commit record has an invalid event type")),
    }
}

fn protocol_limits() -> ProtocolLimits {
    ProtocolLimits {
        maximum_message_bytes: 4 * 1024 * 1024,
        maximum_inline_payload_bytes: 1024 * 1024,
        maximum_event_batch_bytes: 0,
        maximum_events_per_batch: 0,
    }
}

fn duration_from_proto(value: &prost_types::Duration) -> Result<Duration, Status> {
    if value.seconds < 0 || value.nanos < 0 || value.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument("duration must be non-negative"));
    }
    Ok(Duration::new(value.seconds as u64, value.nanos as u32))
}

fn proto_duration(value: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: value.as_secs().min(i64::MAX as u64) as i64,
        nanos: value.subsec_nanos() as i32,
    }
}

fn timestamp(milliseconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: milliseconds.div_euclid(1_000),
        nanos: (milliseconds.rem_euclid(1_000) * 1_000_000) as i32,
    }
}

fn token_uuid(token: &[u8], name: &str) -> Result<Uuid, Status> {
    Uuid::from_slice(token).map_err(|_| Status::unauthenticated(format!("{name} is invalid")))
}

fn canonical_component_type(value: &str) -> String {
    value.trim().trim_end_matches('s').to_ascii_lowercase()
}

fn legacy_component_type_name(value: i32) -> String {
    match LegacyComponentType::try_from(value).unwrap_or(LegacyComponentType::Unspecified) {
        LegacyComponentType::Unspecified => String::new(),
        LegacyComponentType::Function => "function".into(),
        LegacyComponentType::Flow => "flow".into(),
        LegacyComponentType::Object => "object".into(),
        LegacyComponentType::Task => "task".into(),
        LegacyComponentType::Workflow => "workflow".into(),
        LegacyComponentType::Agent => "agent".into(),
        LegacyComponentType::Tool => "tool".into(),
        LegacyComponentType::Mcp => "mcp".into(),
        LegacyComponentType::Entity => "entity".into(),
        LegacyComponentType::Scorer => "scorer".into(),
    }
}

fn legacy_component_type_number(value: &str) -> i32 {
    match canonical_component_type(value).as_str() {
        "function" => LegacyComponentType::Function as i32,
        "flow" => LegacyComponentType::Flow as i32,
        "object" => LegacyComponentType::Object as i32,
        "task" => LegacyComponentType::Task as i32,
        "workflow" => LegacyComponentType::Workflow as i32,
        "agent" => LegacyComponentType::Agent as i32,
        "tool" => LegacyComponentType::Tool as i32,
        "mcp" => LegacyComponentType::Mcp as i32,
        "entity" => LegacyComponentType::Entity as i32,
        "scorer" => LegacyComponentType::Scorer as i32,
        _ => LegacyComponentType::Unspecified as i32,
    }
}

fn v2_component_type_number(value: &str) -> i32 {
    match canonical_component_type(value).as_str() {
        "function" => ComponentType::Function as i32,
        "workflow" => ComponentType::Workflow as i32,
        "entity" => ComponentType::Entity as i32,
        "agent" => ComponentType::Agent as i32,
        "tool" => ComponentType::Tool as i32,
        "scorer" => ComponentType::Scorer as i32,
        _ => ComponentType::Unspecified as i32,
    }
}

fn duration_ms(value: Duration) -> i64 {
    value.as_millis().min(i64::MAX as u128) as i64
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
