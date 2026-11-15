//! Demand-paced pull-dispatch boundary.
//!
//! The public Engine service verifies transport identity and constructs a
//! `VerifiedPoll`. Everything after that point—parking, interest aggregation,
//! supply selection, proposals, redirects, exact-grant retries, and cleanup—
//! belongs behind `PullDispatch::poll`.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use engine_proto::JobAssignment;

use crate::demand::DemandKey;
use crate::error::{EngineError, EngineResult};

/// Maximum deployment IDs accepted by the reversible demand rollout gate.
pub const MAX_DEMAND_DEPLOYMENT_ALLOWLIST: usize = 1024;
pub const DEMAND_DEPLOYMENT_SCOPE_ENV: &str = "AGNT5_PULL_DISPATCH_DEMAND_DEPLOYMENTS";

/// Deployment scope for authoritative demand dispatch.
///
/// An absent configuration preserves the `demand` mode's all-deployment
/// behavior. An explicitly empty value selects no deployments, which lets a
/// coordinator proxy every poll to the legacy runtime path without changing
/// worker configuration or journal state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum DemandDeploymentScope {
    #[default]
    All,
    Allowlist(BTreeSet<String>),
}

impl DemandDeploymentScope {
    pub fn parse(configured: Option<&str>) -> EngineResult<Self> {
        let Some(raw) = configured else {
            return Ok(Self::All);
        };
        if raw.is_empty() {
            return Ok(Self::Allowlist(BTreeSet::new()));
        }
        if raw == "*" {
            return Ok(Self::All);
        }

        let mut deployments = BTreeSet::new();
        for entry in raw.split(',') {
            if entry.is_empty()
                || entry.trim() != entry
                || entry == "*"
                || entry.len() > 255
                || entry.chars().any(char::is_control)
            {
                return Err(EngineError::InvalidArgument(format!(
                    "demand deployment allowlist contains invalid entry {entry:?}"
                )));
            }
            deployments.insert(entry.to_string());
            if deployments.len() > MAX_DEMAND_DEPLOYMENT_ALLOWLIST {
                return Err(EngineError::InvalidArgument(format!(
                    "demand deployment allowlist exceeds {MAX_DEMAND_DEPLOYMENT_ALLOWLIST} entries"
                )));
            }
        }
        Ok(Self::Allowlist(deployments))
    }

    pub fn includes(&self, deployment_id: &str) -> bool {
        match self {
            Self::All => true,
            Self::Allowlist(deployments) => deployments.contains(deployment_id),
        }
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Allowlist(deployments) if deployments.is_empty())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPoll {
    worker_id: String,
    worker_session_id: String,
    project_id: String,
    deployment_id: String,
    demand_keys: BTreeSet<DemandKey>,
    claim_timeout: Duration,
}

impl VerifiedPoll {
    pub fn new(
        worker_id: impl Into<String>,
        worker_session_id: impl Into<String>,
        project_id: impl Into<String>,
        deployment_id: impl Into<String>,
        demand_keys: BTreeSet<DemandKey>,
        claim_timeout: Duration,
    ) -> EngineResult<Self> {
        let worker_id = worker_id.into();
        let worker_session_id = worker_session_id.into();
        let project_id = project_id.into();
        let deployment_id = deployment_id.into();
        if worker_id.is_empty() || worker_session_id.is_empty() {
            return Err(EngineError::InvalidArgument(
                "verified poll requires worker and session identity".to_string(),
            ));
        }
        if project_id.is_empty() || deployment_id.is_empty() {
            return Err(EngineError::InvalidArgument(
                "verified poll requires project and deployment scope".to_string(),
            ));
        }
        if claim_timeout.is_zero() {
            return Err(EngineError::InvalidArgument(
                "verified poll claim timeout must be positive".to_string(),
            ));
        }
        if demand_keys
            .iter()
            .any(|key| key.project_id() != project_id || key.deployment_id() != deployment_id)
        {
            return Err(EngineError::InvalidArgument(
                "verified poll demand keys must match its authenticated scope".to_string(),
            ));
        }
        Ok(Self {
            worker_id,
            worker_session_id,
            project_id,
            deployment_id,
            demand_keys,
            claim_timeout,
        })
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub fn worker_session_id(&self) -> &str {
        &self.worker_session_id
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    pub fn demand_keys(&self) -> &BTreeSet<DemandKey> {
        &self.demand_keys
    }

    pub fn claim_timeout(&self) -> Duration {
        self.claim_timeout
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollRejection {
    SessionInactive,
    AuthorizationExpired,
    ShuttingDown,
    ResourceExhausted { retry_after: Duration },
    Failure(PollFailure),
}

/// Transport-neutral failure retained across the service/dispatcher seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PollFailure {
    kind: PollFailureKind,
    message: String,
}

impl PollFailure {
    pub fn new(kind: PollFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> PollFailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PollFailureKind {
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PollOutcome {
    Assigned(Box<JobAssignment>),
    DeadlineExpired,
    Rejected(PollRejection),
}

/// The only pull scheduling operation exposed to the Engine service.
#[async_trait]
pub trait PullDispatch: Send + Sync {
    async fn poll(&self, poll: VerifiedPoll, deadline: Instant) -> PollOutcome;

    /// Return the current level-triggered readiness for any compatible demand
    /// key. `None` means this dispatcher does not expose authoritative supply,
    /// so callers must omit server-driven scaling feedback and preserve the
    /// worker's local fallback policy.
    fn has_ready_supply(&self, _demand_keys: &BTreeSet<DemandKey>) -> Option<bool> {
        None
    }

    /// Close admission and start returning already parked polls retryably.
    ///
    /// The default keeps test and legacy adapters source-compatible. Managed
    /// dispatchers override all three lifecycle methods so process shutdown can
    /// wait for proposal/grant cleanup without knowing their internal state.
    fn begin_draining(&self) {}

    fn active_poll_count(&self) -> usize {
        0
    }

    async fn wait_for_drain(&self, _timeout: Duration) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_proto::ComponentType;

    fn key(project_id: &str, deployment_id: &str, name: &str) -> DemandKey {
        DemandKey::new(project_id, deployment_id, ComponentType::Function, name).unwrap()
    }

    #[test]
    fn verified_poll_accepts_only_authenticated_route_scope() {
        let poll = VerifiedPoll::new(
            "worker-a",
            "session-a",
            "project-a",
            "deployment-a",
            BTreeSet::from([
                key("project-a", "deployment-a", "alpha"),
                key("project-a", "deployment-a", "beta"),
            ]),
            Duration::from_secs(300),
        )
        .unwrap();
        assert_eq!(poll.worker_id(), "worker-a");
        assert_eq!(poll.worker_session_id(), "session-a");
        assert_eq!(poll.project_id(), "project-a");
        assert_eq!(poll.deployment_id(), "deployment-a");
        assert_eq!(poll.demand_keys().len(), 2);
        assert_eq!(poll.claim_timeout(), Duration::from_secs(300));

        assert!(VerifiedPoll::new(
            "worker-a",
            "session-a",
            "project-a",
            "deployment-a",
            BTreeSet::from([key("project-a", "deployment-b", "alpha")]),
            Duration::from_secs(300),
        )
        .is_err());
    }

    #[test]
    fn verified_poll_rejects_incomplete_authority() {
        assert!(VerifiedPoll::new(
            "",
            "session-a",
            "project-a",
            "deployment-a",
            BTreeSet::new(),
            Duration::from_secs(1),
        )
        .is_err());
        assert!(VerifiedPoll::new(
            "worker-a",
            "session-a",
            "project-a",
            "deployment-a",
            BTreeSet::new(),
            Duration::ZERO,
        )
        .is_err());
    }

    #[test]
    fn demand_deployment_scope_distinguishes_unset_empty_and_all() {
        assert_eq!(
            DemandDeploymentScope::parse(None).unwrap(),
            DemandDeploymentScope::All
        );
        assert_eq!(
            DemandDeploymentScope::parse(Some("*")).unwrap(),
            DemandDeploymentScope::All
        );
        let empty = DemandDeploymentScope::parse(Some("")).unwrap();
        assert!(empty.is_empty());
        assert!(!empty.includes("deployment-a"));
    }

    #[test]
    fn demand_deployment_scope_is_exact_bounded_and_strict() {
        let scope =
            DemandDeploymentScope::parse(Some("deployment-b,deployment-a,deployment-a")).unwrap();
        assert!(scope.includes("deployment-a"));
        assert!(scope.includes("deployment-b"));
        assert!(!scope.includes("deployment-c"));
        for invalid in [
            " deployment-a",
            "deployment-a ",
            "deployment-a,",
            "*,deployment-a",
        ] {
            assert!(DemandDeploymentScope::parse(Some(invalid)).is_err());
        }
        let too_many = (0..=MAX_DEMAND_DEPLOYMENT_ALLOWLIST)
            .map(|index| format!("deployment-{index}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(DemandDeploymentScope::parse(Some(&too_many)).is_err());
    }
}
