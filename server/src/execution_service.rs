use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use engine_core::types::{Offset, Record};
use engine_proto::execution_engine_service_server::ExecutionEngineService;
use engine_proto::{
    BatchExecuteRequest, CancelBatchRequest, CancelBatchResponse, EeResolveApprovalRequest,
    EeResolveApprovalResponse, EeSendSignalRequest, EeSendSignalResponse, EventStreamAck,
    EventStreamMessage, ExecuteComponentRequest, ExecuteSandboxRequest, ExecuteSandboxResponse,
    GetBatchStatusRequest, GetBatchStatusResponse, GetRunResultRequest, GetRunResultResponse,
    GetRunStatusRequest, GetRunStatusResponse, SubmitBatchResponse, SubmitComponentResponse,
    WriteCheckpointRequest, WriteCheckpointResponse, WriteJournalEventRequest,
    WriteJournalEventResponse, WriteJournalEventsBatchRequest, WriteJournalEventsBatchResponse,
};
use runtime_common::backend::EngineBackend;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

/// Compatibility surface for the older `ExecutionEngineService`.
///
/// Current SDK workers write through `EngineService`, which checks the worker's
/// lease and session. They still open this service's event stream at startup,
/// so it stays mounted, but its write RPCs are refused. Everything forwarded
/// through the in-process engine counts as runtime-authored, so accepting
/// writes here would let any caller skip the checks that keep workers from
/// writing records only the runtime may write.
pub struct LegacyExecutionService {
    engine: Arc<dyn EngineBackend>,
    project_id: Arc<str>,
    deployment_id: Arc<str>,
}

impl LegacyExecutionService {
    pub fn new(
        engine: Arc<dyn EngineBackend>,
        project_id: impl Into<String>,
        deployment_id: impl Into<String>,
    ) -> Self {
        Self {
            engine,
            project_id: Arc::from(project_id.into()),
            deployment_id: Arc::from(deployment_id.into()),
        }
    }

    fn transient_record(&self, message: EventStreamMessage) -> Result<Record, Status> {
        transient_record(message, &self.project_id, &self.deployment_id)
    }
}

/// Build the record for one live update. It carries the engine's transient
/// marker, so the engine passes it to stream subscribers without writing it
/// to the journal.
fn transient_record(
    message: EventStreamMessage,
    project_id: &str,
    deployment_id: &str,
) -> Result<Record, Status> {
    if message.run_id.is_empty() || message.event_type.is_empty() || message.project_id.is_empty() {
        return Err(Status::invalid_argument(
            "run_id, event_type and project_id are required",
        ));
    }
    if message.project_id != project_id {
        return Err(Status::permission_denied(format!(
            "project {:?} is outside this runtime's configured scope",
            message.project_id
        )));
    }
    let mut metadata = HashMap::new();
    metadata.insert(
        engine_service::TRANSIENT_EVENT_METADATA_KEY.to_string(),
        "true".to_string(),
    );
    metadata.insert("deployment_id".to_string(), deployment_id.to_string());
    if !message.trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), message.trace_id);
    }
    if !message.span_id.is_empty() {
        metadata.insert("span_id".to_string(), message.span_id);
    }
    if !message.worker_id.is_empty() {
        metadata.insert("worker_id".to_string(), message.worker_id);
    }
    Ok(Record {
        offset: Offset::ZERO,
        project_id: message.project_id,
        run_id: message.run_id,
        event_type: message.event_type,
        data: Bytes::from(message.data),
        timestamp_ns: message.source_timestamp_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

#[tonic::async_trait]
impl ExecutionEngineService for LegacyExecutionService {
    async fn enqueue_component(
        &self,
        _request: Request<ExecuteComponentRequest>,
    ) -> Result<Response<SubmitComponentResponse>, Status> {
        Err(unsupported("EnqueueComponent"))
    }

    async fn get_run_status(
        &self,
        _request: Request<GetRunStatusRequest>,
    ) -> Result<Response<GetRunStatusResponse>, Status> {
        Err(unsupported("GetRunStatus"))
    }

    async fn get_run_result(
        &self,
        _request: Request<GetRunResultRequest>,
    ) -> Result<Response<GetRunResultResponse>, Status> {
        Err(unsupported("GetRunResult"))
    }

    async fn execute_sandbox(
        &self,
        _request: Request<ExecuteSandboxRequest>,
    ) -> Result<Response<ExecuteSandboxResponse>, Status> {
        Err(unsupported("ExecuteSandbox"))
    }

    async fn enqueue_batch(
        &self,
        _request: Request<BatchExecuteRequest>,
    ) -> Result<Response<SubmitBatchResponse>, Status> {
        Err(unsupported("EnqueueBatch"))
    }

    async fn get_batch_status(
        &self,
        _request: Request<GetBatchStatusRequest>,
    ) -> Result<Response<GetBatchStatusResponse>, Status> {
        Err(unsupported("GetBatchStatus"))
    }

    async fn cancel_batch(
        &self,
        _request: Request<CancelBatchRequest>,
    ) -> Result<Response<CancelBatchResponse>, Status> {
        Err(unsupported("CancelBatch"))
    }

    async fn send_signal(
        &self,
        _request: Request<EeSendSignalRequest>,
    ) -> Result<Response<EeSendSignalResponse>, Status> {
        Err(unsupported("SendSignal"))
    }

    async fn resolve_approval(
        &self,
        _request: Request<EeResolveApprovalRequest>,
    ) -> Result<Response<EeResolveApprovalResponse>, Status> {
        Err(unsupported("ResolveApproval"))
    }

    async fn write_journal_event(
        &self,
        _request: Request<WriteJournalEventRequest>,
    ) -> Result<Response<WriteJournalEventResponse>, Status> {
        Err(legacy_write_refused("WriteJournalEvent"))
    }

    async fn write_checkpoint(
        &self,
        _request: Request<WriteCheckpointRequest>,
    ) -> Result<Response<WriteCheckpointResponse>, Status> {
        Err(legacy_write_refused("WriteCheckpoint"))
    }

    async fn write_journal_events_batch(
        &self,
        _request: Request<WriteJournalEventsBatchRequest>,
    ) -> Result<Response<WriteJournalEventsBatchResponse>, Status> {
        Err(legacy_write_refused("WriteJournalEventsBatch"))
    }

    async fn event_stream(
        &self,
        request: Request<Streaming<EventStreamMessage>>,
    ) -> Result<Response<EventStreamAck>, Status> {
        let mut stream = request.into_inner();
        let mut events_received = 0_i64;
        while let Some(message) = stream.next().await {
            let record = self.transient_record(message?)?;
            self.engine.append(record).await.map_err(dataplane_status)?;
            events_received = events_received.saturating_add(1);
        }
        Ok(Response::new(EventStreamAck {
            success: true,
            events_received,
        }))
    }
}

fn unsupported(method: &str) -> Status {
    Status::unimplemented(format!(
        "{method} is outside the minimal execution profile; use the HTTP API"
    ))
}

fn dataplane_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

/// Writes must go through `EngineService`, which checks the worker's lease.
fn legacy_write_refused(method: &str) -> Status {
    Status::unimplemented(format!(
        "{method} is not accepted by the single-process runtime; current SDKs write through EngineService"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(project_id: &str) -> EventStreamMessage {
        EventStreamMessage {
            run_id: "run-1".to_string(),
            event_type: "lm.stream.delta".to_string(),
            project_id: project_id.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn live_updates_carry_the_engine_transient_marker() {
        let record = transient_record(message("p1"), "p1", "d1").expect("valid live update");
        assert_eq!(
            record
                .metadata
                .get(engine_service::TRANSIENT_EVENT_METADATA_KEY)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            record.metadata.get("deployment_id").map(String::as_str),
            Some("d1")
        );
    }

    #[test]
    fn live_updates_for_another_project_are_rejected() {
        let error = transient_record(message("p2"), "p1", "d1").expect_err("wrong project");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn legacy_writes_are_refused_as_unimplemented() {
        assert_eq!(
            legacy_write_refused("WriteJournalEvent").code(),
            tonic::Code::Unimplemented
        );
    }
}
