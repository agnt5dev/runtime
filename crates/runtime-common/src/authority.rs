//! Verified request authority shared by runtime modules. Authentication remains in the ingress adapter.

/// Which credential produced an `AuthContext`. Scope enforcement
/// (`middleware::scope`) only gates `ApiKey` (Mode A) requests — service/user
/// API keys carry the `run`/`entity`/`workflow`/`admin` vocabulary. Mode B
/// (`Session`) is already authorised by the control plane (`/auth/check-access`)
/// and Mode C (`Workload`) is enforced by `validate_workload_request`, so both
/// pass scope enforcement untouched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CredentialKind {
    /// Only produced by `Default`; never emitted by a real auth path.
    #[default]
    Unknown,
    /// Mode A — service key (`agnt5_sk_`) or user key (`agnt5_uk_`).
    ApiKey,
    /// Mode B — Studio session JWT.
    Session,
    /// Mode C — AGNT5 workload/FWI service token.
    Workload,
    /// An OAuth access token for one hosted MCP server, issued by the AGNT5
    /// MCP authorization server and verified locally.
    McpOAuth,
}

/// Auth context injected into request extensions and read by handlers.
///
/// Resolution targets (`deployment_id` and `environment_id`) are mutually
/// exclusive in practice: callers either name a specific deployment via
/// `X-DEPLOYMENT-ID` (Mode B) or carry an environment UUID baked into an
/// API key (Mode A). Downstream resolvers prefer `deployment_id` when set;
/// if both are empty for a project-scoped key, the deployment resolver
/// defaults to the production environment.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AuthContext {
    /// Workspace UUID — the auth/org scope. Use this for authorisation and
    /// workspace-scoped audit/metrics. Always populated.
    pub workspace_id: String,
    /// Project UUID — the execution isolation scope (k8s namespace). Use this
    /// as the routing key for the runtime (journals, leases, queries).
    pub project_id: String,
    /// Deployment UUID supplied by the caller (Mode B `X-DEPLOYMENT-ID`).
    /// Empty for Mode A (API key) — those carry an environment, not a
    /// deployment. Resolvers should prefer this over `environment_id` when
    /// non-empty (Path A: explicit deployment).
    pub deployment_id: String,
    /// Environment UUID (Mode A — embedded in the API key's CP record).
    /// Empty for Mode B. Used for late-binding env-ref resolution
    /// (Path B: latest active deployment in env).
    pub environment_id: String,
    pub scopes: Vec<String>,
    /// PropelAuth user ID when Mode B; for Mode A (user key), this is the
    /// user who owns the key. Empty for service keys.
    pub user_id: String,
    /// Validated credential record ID, never the secret key.
    #[serde(default)]
    pub api_key_id: String,
    pub authenticated: bool,
    /// The credential mode that produced this context. Drives scope
    /// enforcement (`middleware::scope`).
    pub credential_kind: CredentialKind,
}

impl AuthContext {
    /// The identity tag to attach to spans / logs that need a single
    /// "who is this request addressing" string. Prefers an explicit
    /// deployment, falls back to environment.
    pub fn target_id(&self) -> &str {
        if !self.deployment_id.is_empty() {
            &self.deployment_id
        } else {
            &self.environment_id
        }
    }
}

/// Default customer sub-tenant when a request does not name one.
pub const DEFAULT_SUBTENANT: &str = "default";
