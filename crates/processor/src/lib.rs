//! Journal processing and workflow projections.

use std::sync::Arc;

use agnt5_core::{JournalError, KvOp, MaterializedStore, Offset, RunState, RuntimeEvent, Segment};
use tokio_stream::StreamExt;

pub const RUNS: &str = "runs";
pub const PENDING: &str = "pending";
const CHECKPOINT: &str = "runtime-partition-0";

pub fn run_key(run_id: &str) -> Vec<u8> {
    run_id.as_bytes().to_vec()
}

pub fn pending_key(component_type: &str, component_name: &str, run_id: &str) -> Vec<u8> {
    format!("{component_type}\0{component_name}\0{run_id}").into_bytes()
}

pub struct Processor<S: Segment, M: MaterializedStore> {
    segment: Arc<S>,
    store: Arc<M>,
}

impl<S: Segment, M: MaterializedStore> Processor<S, M> {
    pub fn new(segment: Arc<S>, store: Arc<M>) -> Self {
        Self { segment, store }
    }

    pub async fn run(&self) -> Result<(), JournalError> {
        let from = self
            .store
            .get_checkpoint(CHECKPOINT)
            .await?
            .map(|offset| Offset(offset.0.saturating_add(1)))
            .unwrap_or(Offset::ZERO);
        let mut tail = self.segment.tail(from).await?;
        while let Some(record) = tail.next().await {
            let record = record?;
            let event: RuntimeEvent = serde_json::from_slice(&record.payload)
                .map_err(|error| JournalError::Storage(error.to_string()))?;
            let operations = self.project(event).await?;
            self.store
                .write_batch_and_checkpoint(&operations, CHECKPOINT, record.offset)
                .await?;
        }
        Ok(())
    }

    async fn project(&self, event: RuntimeEvent) -> Result<Vec<KvOp>, JournalError> {
        let run_id = event.run_id().to_string();
        let existing = self.store.get(RUNS, &run_key(&run_id)).await?;
        let mut state = existing.as_deref().map(decode_run).transpose()?;
        let mut operations = Vec::new();

        match event {
            RuntimeEvent::RunQueued {
                project_id,
                run_id,
                component_type,
                component_name,
                input_data,
                submitted_at_ms,
            } => {
                if state.is_none() {
                    let run = RunState {
                        project_id: project_id.clone(),
                        run_id: run_id.clone(),
                        component_type: component_type.clone(),
                        component_name: component_name.clone(),
                        status: "queued".into(),
                        input_data: input_data.clone(),
                        output_data: None,
                        error_message: None,
                        error_code: None,
                        submitted_at_ms,
                        completed_at_ms: None,
                        worker_id: None,
                        lease_id: None,
                        lease_expires_at_ms: None,
                        attempt: 0,
                    };
                    let pending = agnt5_core::PendingJob {
                        project_id,
                        run_id: run_id.clone(),
                        component_type: component_type.clone(),
                        component_name: component_name.clone(),
                        input_data,
                    };
                    operations.push(KvOp::Put {
                        namespace: PENDING.into(),
                        key: pending_key(&component_type, &component_name, &run_id),
                        value: encode(&pending)?,
                    });
                    state = Some(run);
                }
            }
            RuntimeEvent::JobClaimed {
                worker_id,
                lease_id,
                lease_expires_at_ms,
                ..
            } => {
                if let Some(run) = state.as_mut() {
                    if run.status == "queued" {
                        operations.push(KvOp::Delete {
                            namespace: PENDING.into(),
                            key: pending_key(&run.component_type, &run.component_name, &run.run_id),
                        });
                        run.status = "running".into();
                        run.worker_id = Some(worker_id);
                        run.lease_id = Some(lease_id);
                        run.lease_expires_at_ms = Some(lease_expires_at_ms);
                        run.attempt += 1;
                    }
                }
            }
            RuntimeEvent::JobReclaimed {
                worker_id,
                previous_lease_id,
                lease_id,
                lease_expires_at_ms,
                ..
            } => {
                if let Some(run) = state.as_mut() {
                    if run.status == "running"
                        && run.lease_id.as_deref() == Some(&previous_lease_id)
                    {
                        run.worker_id = Some(worker_id);
                        run.lease_id = Some(lease_id);
                        run.lease_expires_at_ms = Some(lease_expires_at_ms);
                    }
                }
            }
            RuntimeEvent::JobLeaseRenewed {
                lease_id,
                lease_expires_at_ms,
                ..
            } => {
                if let Some(run) = state.as_mut() {
                    if run.lease_id.as_deref() == Some(&lease_id) && run.status == "running" {
                        run.lease_expires_at_ms = Some(lease_expires_at_ms);
                    }
                }
            }
            RuntimeEvent::JobCompleted {
                lease_id,
                output_data,
                completed_at_ms,
                ..
            } => {
                if let Some(run) = state.as_mut() {
                    if run.lease_id.as_deref() == Some(&lease_id) && run.status == "running" {
                        run.status = "completed".into();
                        run.output_data = Some(output_data);
                        run.completed_at_ms = Some(completed_at_ms);
                        run.lease_expires_at_ms = None;
                    }
                }
            }
            RuntimeEvent::JobFailed {
                lease_id,
                error_message,
                error_code,
                completed_at_ms,
                ..
            } => {
                if let Some(run) = state.as_mut() {
                    if run.lease_id.as_deref() == Some(&lease_id) && run.status == "running" {
                        run.status = "failed".into();
                        run.error_message = Some(error_message);
                        run.error_code = Some(error_code);
                        run.completed_at_ms = Some(completed_at_ms);
                        run.lease_expires_at_ms = None;
                    }
                }
            }
        }
        if let Some(run) = state {
            operations.push(KvOp::Put {
                namespace: RUNS.into(),
                key: run_key(&run.run_id),
                value: encode(&run)?,
            });
        }
        Ok(operations)
    }
}

pub fn decode_run(bytes: &[u8]) -> Result<RunState, JournalError> {
    serde_json::from_slice(bytes).map_err(|error| JournalError::Storage(error.to_string()))
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, JournalError> {
    serde_json::to_vec(value).map_err(|error| JournalError::Storage(error.to_string()))
}
