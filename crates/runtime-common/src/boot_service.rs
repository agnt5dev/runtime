//! Answering before the state exists.
//!
//! A tonic server builds a fixed router when it starts: services cannot be
//! added to a running server, and rebinding the port leaves a window where
//! peers get connection-refused. So binding before recovery means every service
//! on that port must exist from the first second with its state filled in
//! later.
//!
//! [`Deferred`] is that wrapper. It holds an empty cell and a view of the boot
//! machine; boot fills the cell when the real service is built. Requests that
//! arrive first are answered — with `UNAVAILABLE` for gRPC, `503` for HTTP —
//! and carry the boot state and step, so a caller learns the node is booting
//! rather than that it is absent.
//!
//! Because tonic services are `tower::Service<http::Request<_>>` and so is an
//! axum `Router`, one wrapper covers both surfaces.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use tokio::sync::watch;
use tonic::server::NamedService;
use tonic::Status;
use tower::Service;

use crate::boot::{BootState, BootStatus, BootStep};
use crate::error_codes::codes;

/// Header carrying the stable error code for a not-ready response.
pub const ERROR_CODE_HEADER: &str = "agnt5-error-code";
/// Header naming the boot state a not-ready response was produced in.
pub const BOOT_STATE_HEADER: &str = "agnt5-boot-state";
/// Header naming the boot step in flight, when there is one.
pub const BOOT_STEP_HEADER: &str = "agnt5-boot-step";
/// Header carrying how many partitions have reached `Serving`.
pub const BOOT_PARTITIONS_SERVING_HEADER: &str = "agnt5-boot-partitions-serving";
/// Header carrying how many partitions this node has.
pub const BOOT_PARTITIONS_TOTAL_HEADER: &str = "agnt5-boot-partitions-total";
/// Header naming the state of the partition furthest behind.
pub const BOOT_SLOWEST_PARTITION_HEADER: &str = "agnt5-boot-slowest-partition";

/// What a caller is told when the node is reachable but not yet serving.
///
/// A rejection carries enough to act on without a second request: the stable
/// code to branch on, the state and step to report, and how much of the node is
/// already up. `/boot-status` carries the same information per partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveringReason {
    pub state: BootState,
    pub step: Option<BootStep>,
    pub partitions_serving: usize,
    pub partitions_total: usize,
    pub slowest_partition: Option<BootState>,
}

impl RecoveringReason {
    pub fn from_status(status: &BootStatus) -> Self {
        Self {
            state: status.node,
            step: status.step,
            partitions_serving: status.partitions_serving(),
            partitions_total: status.partitions.len(),
            slowest_partition: status.slowest_partition(),
        }
    }

    /// The stable handle a caller branches on.
    pub fn code(&self) -> &'static str {
        codes::RT_BOOT_E001.code
    }

    fn message(&self) -> String {
        let step = match self.step {
            Some(step) => format!(" ({})", step.as_str()),
            None => String::new(),
        };
        format!(
            "[{}] node is recovering: {}{}; {}/{} partitions serving",
            self.code(),
            self.state.as_str(),
            step,
            self.partitions_serving,
            self.partitions_total
        )
    }

    /// The headers a not-ready response carries, on either protocol.
    fn headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![
            (ERROR_CODE_HEADER, self.code().to_string()),
            (BOOT_STATE_HEADER, self.state.as_str().to_string()),
            (
                BOOT_PARTITIONS_SERVING_HEADER,
                self.partitions_serving.to_string(),
            ),
            (
                BOOT_PARTITIONS_TOTAL_HEADER,
                self.partitions_total.to_string(),
            ),
        ];
        if let Some(step) = self.step {
            headers.push((BOOT_STEP_HEADER, step.as_str().to_string()));
        }
        if let Some(slowest) = self.slowest_partition {
            headers.push((BOOT_SLOWEST_PARTITION_HEADER, slowest.as_str().to_string()));
        }
        headers
    }
}

/// How to tell a caller the service is not ready yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// `UNAVAILABLE`, so a gRPC client retries rather than failing the call.
    Grpc,
    /// `503 Service Unavailable` with `Retry-After`.
    Http,
}

/// A service whose backing state is built during boot.
pub struct Deferred<S> {
    inner: Arc<OnceLock<S>>,
    boot: watch::Receiver<BootStatus>,
    protocol: Protocol,
}

impl<S> Clone for Deferred<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            boot: self.boot.clone(),
            protocol: self.protocol,
        }
    }
}

impl<S> Deferred<S> {
    pub fn new(boot: watch::Receiver<BootStatus>, protocol: Protocol) -> Self {
        Self {
            inner: Arc::new(OnceLock::new()),
            boot,
            protocol,
        }
    }

    /// A deferred gRPC service.
    pub fn grpc(boot: watch::Receiver<BootStatus>) -> Self {
        Self::new(boot, Protocol::Grpc)
    }

    /// A deferred HTTP service.
    pub fn http(boot: watch::Receiver<BootStatus>) -> Self {
        Self::new(boot, Protocol::Http)
    }

    /// Install the real service. Returns `false` if one is already installed,
    /// which means boot built it twice.
    pub fn fill(&self, service: S) -> bool {
        self.inner.set(service).is_ok()
    }

    pub fn is_ready(&self) -> bool {
        self.inner.get().is_some()
    }

    fn not_ready<B: Default>(&self) -> http::Response<B> {
        let reason = RecoveringReason::from_status(&self.boot.borrow());

        match self.protocol {
            Protocol::Grpc => {
                let mut status = Status::unavailable(reason.message());
                for (name, value) in reason.headers() {
                    let name: Result<tonic::metadata::MetadataKey<_>, _> = name.parse();
                    let value: Result<tonic::metadata::MetadataValue<_>, _> = value.parse();
                    if let (Ok(name), Ok(value)) = (name, value) {
                        status.metadata_mut().insert(name, value);
                    }
                }
                status.into_http()
            }
            Protocol::Http => {
                let mut builder = http::Response::builder()
                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                    .header(http::header::RETRY_AFTER, "1");
                for (name, value) in reason.headers() {
                    builder = builder.header(name, value);
                }
                builder
                    .body(B::default())
                    .unwrap_or_else(|_| http::Response::new(B::default()))
            }
        }
    }
}

/// The wrapper is transparent to tonic's service discovery: it registers under
/// the name of the service it will hold, whether or not it holds it yet.
impl<S> NamedService for Deferred<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for Deferred<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// Always ready. The inner service does not exist yet to be polled, and
    /// once it does it is cloned per call — which is how tonic's generated
    /// servers and axum's `Router` are meant to be driven.
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        match self.inner.get() {
            Some(service) => {
                let mut service = service.clone();
                Box::pin(async move { service.call(request).await })
            }
            None => {
                let response = self.not_ready();
                Box::pin(async move { Ok(response) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boot::{BootProgress, BootState, BootStep};
    use crate::boot_service::{
        BOOT_PARTITIONS_SERVING_HEADER, BOOT_PARTITIONS_TOTAL_HEADER,
        BOOT_SLOWEST_PARTITION_HEADER, ERROR_CODE_HEADER,
    };

    #[derive(Clone)]
    struct Echo;

    impl Service<http::Request<String>> for Echo {
        type Response = http::Response<String>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<String>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(request.into_body())))
        }
    }

    impl NamedService for Echo {
        const NAME: &'static str = "test.Echo";
    }

    fn request() -> http::Request<String> {
        http::Request::new("hello".to_string())
    }

    #[tokio::test]
    async fn http_reports_booting_until_the_service_is_installed() {
        let (progress, rx) = BootProgress::new(1);
        progress.advance_all(BootState::Reachable).unwrap();
        progress.advance_all(BootState::AdmittingStorage).unwrap();
        let _step = progress.begin(BootStep::AdmitColdCatalog);

        let mut deferred: Deferred<Echo> = Deferred::http(rx);
        assert!(!deferred.is_ready());

        let response = deferred.call(request()).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(BOOT_STATE_HEADER).unwrap(),
            "admitting_storage"
        );
        assert_eq!(
            response.headers().get(BOOT_STEP_HEADER).unwrap(),
            "admit_cold_catalog"
        );
        assert!(response.headers().contains_key(http::header::RETRY_AFTER));
    }

    #[tokio::test]
    async fn a_filled_service_handles_the_request_itself() {
        let (_progress, rx) = BootProgress::new(1);
        let mut deferred: Deferred<Echo> = Deferred::http(rx);
        assert!(deferred.fill(Echo));
        assert!(deferred.is_ready());

        let response = deferred.call(request()).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.into_body(), "hello");
    }

    #[tokio::test]
    async fn grpc_reports_unavailable_so_the_caller_retries() {
        let (progress, rx) = BootProgress::new(1);
        progress.advance_all(BootState::Reachable).unwrap();

        let mut deferred: Deferred<Echo> = Deferred::grpc(rx);
        let response = deferred.call(request()).await.unwrap();

        // A gRPC status rides in the response, not the HTTP status line.
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.headers().get("grpc-status").unwrap(), "14");
        assert_eq!(
            response.headers().get(BOOT_STATE_HEADER).unwrap(),
            "reachable"
        );
    }

    #[tokio::test]
    async fn a_rejection_says_how_much_of_the_node_is_already_up() {
        let (progress, rx) = BootProgress::new(4);
        for state in BootState::SEQUENCE.iter().skip(1) {
            progress.advance(*state).unwrap();
        }
        // Two partitions serving, one behind, one failed.
        for partition in 0..4 {
            progress
                .advance_partition(partition, BootState::Reachable)
                .unwrap();
        }
        for partition in [0, 1] {
            for state in BootState::SEQUENCE.iter().skip(2) {
                progress.advance_partition(partition, *state).unwrap();
            }
        }
        progress
            .advance_partition(2, BootState::AdmittingStorage)
            .unwrap();
        progress.advance_partition(3, BootState::Failed).unwrap();

        let mut deferred: Deferred<Echo> = Deferred::http(rx);
        let response = deferred.call(request()).await.unwrap();
        let headers = response.headers();

        assert_eq!(headers.get(ERROR_CODE_HEADER).unwrap(), "RT_BOOT_E001");
        assert_eq!(headers.get(BOOT_PARTITIONS_SERVING_HEADER).unwrap(), "2");
        assert_eq!(headers.get(BOOT_PARTITIONS_TOTAL_HEADER).unwrap(), "4");
        // The failed partition is the headline, not the merely slow one.
        assert_eq!(
            headers.get(BOOT_SLOWEST_PARTITION_HEADER).unwrap(),
            "failed"
        );
    }

    #[test]
    fn the_rejection_message_carries_the_code_and_the_progress() {
        let (progress, _rx) = BootProgress::new(2);
        progress.advance_all(BootState::Reachable).unwrap();
        progress.advance_all(BootState::AdmittingStorage).unwrap();
        let _step = progress.begin(BootStep::RecoverProjection);

        let reason = RecoveringReason::from_status(&progress.status());
        assert_eq!(reason.code(), "RT_BOOT_E001");
        assert_eq!(
            reason.message(),
            concat!(
                "[RT_BOOT_E001] node is recovering: admitting_storage ",
                "(recover_projection); 0/2 partitions serving"
            )
        );
    }

    #[test]
    fn filling_twice_is_refused_so_a_double_build_is_visible() {
        let (_progress, rx) = BootProgress::new(1);
        let deferred: Deferred<Echo> = Deferred::grpc(rx);
        assert!(deferred.fill(Echo));
        assert!(!deferred.fill(Echo));
    }

    #[test]
    fn the_wrapper_registers_under_the_name_of_the_service_it_will_hold() {
        assert_eq!(<Deferred<Echo> as NamedService>::NAME, Echo::NAME);
    }

    #[tokio::test]
    async fn clones_share_one_cell_so_boot_can_fill_a_registered_service() {
        let (_progress, rx) = BootProgress::new(1);
        let deferred: Deferred<Echo> = Deferred::http(rx);
        // The router owns one clone; boot keeps another to fill later.
        let mut registered = deferred.clone();
        assert!(deferred.fill(Echo));

        let response = registered.call(request()).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
    }
}
