//! gRPC `CoordinatorBackend` implementation.
//!
//! Connects to a remote Worker Coordinator via gRPC for use in
//! disaggregated mode (`--target=gateway`).

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

use engine_proto::worker_coordinator_service_client::WorkerCoordinatorServiceClient;
use engine_proto::*;

use crate::backend::CoordinatorBackend;
use crate::error::{DataplaneError, DataplaneResult};
use crate::util::grpc_client_pool_size;

/// gRPC coordinator backend — connects to a remote `WorkerCoordinatorService`.
pub struct GrpcCoordinatorBackend {
    clients: Vec<WorkerCoordinatorServiceClient<Channel>>,
    next: AtomicUsize,
}

impl GrpcCoordinatorBackend {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = if addr.starts_with("http") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let pool_size = grpc_client_pool_size();
        let mut clients = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            clients.push(WorkerCoordinatorServiceClient::connect(endpoint.clone()).await?);
        }
        info!(addr = %addr, pool_size, "Connected to remote Coordinator");
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    fn next_client(&self) -> WorkerCoordinatorServiceClient<Channel> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[idx].clone()
    }
}

#[async_trait]
impl CoordinatorBackend for GrpcCoordinatorBackend {
    async fn dispatch_component(
        &self,
        req: DispatchComponentRequest,
    ) -> DataplaneResult<DispatchComponentResponse> {
        let mut c = self.next_client();
        let resp = c
            .dispatch_component(Request::new(req))
            .await
            .map_err(DataplaneError::Grpc)?;
        Ok(resp.into_inner())
    }

    async fn dispatch_component_stream(
        &self,
        req: DispatchComponentRequest,
    ) -> DataplaneResult<
        Pin<
            Box<dyn tokio_stream::Stream<Item = DataplaneResult<DispatchComponentResponse>> + Send>,
        >,
    > {
        let mut c = self.next_client();
        let resp = c
            .dispatch_component_stream(Request::new(req))
            .await
            .map_err(DataplaneError::Grpc)?;
        let stream = resp.into_inner().map(|r| r.map_err(DataplaneError::Grpc));
        Ok(Box::pin(stream))
    }

    async fn list_workers(&self, req: ListWorkersRequest) -> DataplaneResult<ListWorkersResponse> {
        let mut c = self.next_client();
        let resp = c
            .list_workers(Request::new(req))
            .await
            .map_err(DataplaneError::Grpc)?;
        Ok(resp.into_inner())
    }

    async fn list_in_memory_event_triggers(
        &self,
        _project_id: &str,
        _deployment_id: &str,
        _event_name: &str,
    ) -> DataplaneResult<Vec<(i32, String, Vec<u8>)>> {
        // Disaggregated mode: the gateway runs in its own process and
        // doesn't have direct access to a coordinator's in-memory worker
        // map. The standalone-no-CP fallback is moot here because
        // disaggregated deployments already require a control plane.
        // Return empty to make this a no-op fallback.
        Ok(Vec::new())
    }
}
