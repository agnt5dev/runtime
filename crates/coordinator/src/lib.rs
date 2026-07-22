//! Worker registration, polling, leases, and completion for one active runtime.

use std::{collections::HashMap, sync::Arc, time::Duration};

use agnt5_core::{MaterializedStore, NewJournalRecord, PendingJob, RuntimeEvent, Segment};
use agnt5_processor::{decode_run, run_key, PENDING, RUNS};
use agnt5_proto::api::v1::{
    engine_service_server::EngineService, execution_engine_service_server::ExecutionEngineService,
    CompleteJobRequest, CompleteJobResponse, ComponentType, JobAssignment, PollJobRequest,
    PollJobResponse, RegisterWorkerSessionRequest, RegisterWorkerSessionResponse,
    RenewJobLeaseRequest, RenewJobLeaseResponse, WorkerSlotPolicy, WriteCheckpointRequest,
    WriteCheckpointResponse,
};
use bytes::Bytes;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use uuid::Uuid;

const SESSION_LIFETIME_MS: i64 = 24 * 60 * 60 * 1_000;
const DEFAULT_LEASE_MS: i64 = 30_000;

#[derive(Clone)]
struct WorkerSession {
    worker_id: String,
    components: Vec<(i32, String)>,
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
    sessions: RwLock<HashMap<String, WorkerSession>>,
    claim_lock: Mutex<()>,
}

impl<S: Segment, M: MaterializedStore> Coordinator<S, M> {
    pub fn new(project_id: impl Into<String>, segment: Arc<S>, store: Arc<M>) -> Self {
        Self {
            project_id: project_id.into(),
            segment,
            store,
            sessions: RwLock::new(HashMap::new()),
            claim_lock: Mutex::new(()),
        }
    }

    async fn session(&self, worker_id: &str, session_id: &str) -> Result<WorkerSession, Status> {
        let sessions = self.sessions.read().await;
        let session = if session_id.is_empty() {
            sessions
                .values()
                .find(|session| session.worker_id == worker_id)
        } else {
            sessions
                .get(session_id)
                .filter(|session| session.worker_id == worker_id)
        };
        session
            .cloned()
            .ok_or_else(|| Status::unauthenticated("worker session is invalid"))
    }

    async fn append(&self, key: String, event: RuntimeEvent) -> Result<(), Status> {
        let payload = serde_json::to_vec(&event).map_err(internal)?;
        self.segment
            .append_batch(&[NewJournalRecord {
                idempotency_key: Some(key.into_bytes()),
                payload: Bytes::from(payload),
            }])
            .await
            .map_err(internal)?;
        Ok(())
    }

    async fn find_job(&self, session: &WorkerSession) -> Result<Option<PendingJob>, Status> {
        let rows = self
            .store
            .scan_prefix(PENDING, b"", 100)
            .await
            .map_err(internal)?;
        for (_, value) in rows {
            let job: PendingJob = serde_json::from_slice(&value).map_err(internal)?;
            let component_type = component_type_number(&job.component_type);
            if session.components.iter().any(|(kind, name)| {
                (*kind == component_type || *kind == ComponentType::Unspecified as i32)
                    && (name.is_empty() || name == &job.component_name)
            }) {
                return Ok(Some(job));
            }
        }
        Ok(None)
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
        let session_id = Uuid::now_v7().to_string();
        let mut components: Vec<_> = request
            .components
            .into_iter()
            .map(|component| (component.component_type, component.name))
            .collect();
        components.extend(
            request
                .capabilities
                .into_iter()
                .map(|capability| (capability.component_type, capability.component_name)),
        );
        self.sessions.write().await.insert(
            session_id.clone(),
            WorkerSession {
                worker_id: request.worker_id,
                components,
            },
        );
        let max_slots = request.max_slots.max(1);
        Ok(Response::new(RegisterWorkerSessionResponse {
            worker_session_id: session_id,
            expires_at_ms: now_ms() + SESSION_LIFETIME_MS,
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
            .session(&request.worker_id, &request.worker_session_id)
            .await?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(request.wait_ms.clamp(0, 30_000) as u64);
        loop {
            let _claim = self.claim_lock.lock().await;
            if let Some(job) = self.find_job(&session).await? {
                let lease_id = Uuid::now_v7().to_string();
                let lease_ms = request.claim_timeout_ms.max(DEFAULT_LEASE_MS);
                let lease_expires_at_ms = now_ms() + lease_ms;
                self.append(
                    format!("claim:{}:{lease_id}", job.run_id),
                    RuntimeEvent::JobClaimed {
                        project_id: self.project_id.clone(),
                        run_id: job.run_id.clone(),
                        worker_id: request.worker_id.clone(),
                        lease_id: lease_id.clone(),
                        lease_expires_at_ms,
                    },
                )
                .await?;
                self.wait_for_lease(&job.run_id, &lease_id).await?;
                return Ok(Response::new(PollJobResponse {
                    job: Some(JobAssignment {
                        job_id: job.run_id.clone(),
                        run_id: job.run_id,
                        component_id: job.component_name.clone(),
                        component_type: component_type_number(&job.component_type),
                        component_name: job.component_name,
                        input_data: job.input_data,
                        metadata: HashMap::new(),
                        attempt: 1,
                        timeout_ms: lease_ms,
                        trace_id: String::new(),
                        lease_id,
                        lease_expires_at_ms,
                    }),
                }));
            }
            drop(_claim);
            if tokio::time::Instant::now() >= deadline {
                return Ok(Response::new(PollJobResponse { job: None }));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn renew_job_lease(
        &self,
        request: Request<RenewJobLeaseRequest>,
    ) -> Result<Response<RenewJobLeaseResponse>, Status> {
        let request = request.into_inner();
        self.session(&request.worker_id, &request.worker_session_id)
            .await?;
        validate_lease(&*self.store, &request.run_id, &request.lease_id).await?;
        let expires_at = now_ms() + request.lease_timeout_ms.max(DEFAULT_LEASE_MS);
        self.append(
            format!("renew:{}:{}:{expires_at}", request.run_id, request.lease_id),
            RuntimeEvent::JobLeaseRenewed {
                project_id: self.project_id.clone(),
                run_id: request.run_id,
                lease_id: request.lease_id,
                lease_expires_at_ms: expires_at,
            },
        )
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
        self.session(&request.worker_id, &request.worker_session_id)
            .await?;
        if !request.project_id.is_empty() && request.project_id != self.project_id {
            return Err(Status::permission_denied(
                "project_id does not match this runtime",
            ));
        }
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
        self.append(
            format!("complete:{}:{}", request.job_id, request.lease_id),
            event,
        )
        .await?;
        Ok(Response::new(CompleteJobResponse { acknowledged: true }))
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
    if run.status != "running" || run.lease_id.as_deref() != Some(lease_id) {
        return Err(Status::failed_precondition(
            "job lease is no longer current",
        ));
    }
    Ok(())
}

fn component_type_number(value: &str) -> i32 {
    match value.trim_end_matches('s') {
        "function" => ComponentType::Function as i32,
        "flow" => ComponentType::Flow as i32,
        "object" => ComponentType::Object as i32,
        "task" => ComponentType::Task as i32,
        "workflow" => ComponentType::Workflow as i32,
        "agent" => ComponentType::Agent as i32,
        "tool" => ComponentType::Tool as i32,
        "mcp" => ComponentType::Mcp as i32,
        "entity" => ComponentType::Entity as i32,
        "scorer" => ComponentType::Scorer as i32,
        _ => ComponentType::Unspecified as i32,
    }
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
