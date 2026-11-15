/// A local, refreshable authorization view. Implementations must not perform
/// network I/O: polling and streaming use this at each work boundary.
pub trait ExternalWorkerAuthority: std::fmt::Debug + Send + Sync {
    fn is_current(&self) -> bool;
}

/// Verified external-worker identity attached to authenticated gRPC requests.
///
/// Runtime services treat the extension as authority: request fields may
/// narrow it, but must never override or conflict with it.
#[derive(Clone, Debug)]
pub struct ExternalWorkerIdentity {
    pub workspace_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub deployment_id: String,
    pub worker_pool_id: String,
    /// Server-assigned worker from a verified certificate; absent for legacy bearer auth.
    pub worker_id: Option<String>,
    /// Revalidates long-lived requests after local trust/authority updates.
    pub authority: Option<std::sync::Arc<dyn ExternalWorkerAuthority>>,
    pub expires_at_unix: i64,
}
