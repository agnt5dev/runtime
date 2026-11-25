//! Lease fences for run lifecycle records.
//!
//! A lease-fenced record carries the lease authority its producer holds:
//! lease id, attempt, worker, and (for pull workers) the session hash. The
//! segment's authority index (`activation_authority`) decides the fence
//! inside the same atomic write as the append, against the lease entry it
//! folded from earlier accepted records. A losing record is never written.
//!
//! Nothing here signs or verifies anything. The journal is self-proving:
//! rejected records do not exist, and the fence metadata on an accepted
//! record documents which lease authored it.

use std::collections::HashMap;

use base64::Engine;
use sha2::{Digest, Sha256};

use crate::error::{EngineError, EngineResult};
use crate::types::Record;

/// Plain lifecycle record appended by `RenewJobLease`. Extends the lease
/// entry's expiry in the authority index; the projection mirrors it.
pub const RUN_LEASE_RENEWED_EVENT: &str = "run.lease_renewed";
pub const RUN_LEASE_EXPIRED_EVENT: &str = "run.lease_expired";

pub const META_FENCE_KIND: &str = "agnt5_fence_kind";
pub const META_FENCE_LEASE_ID: &str = "agnt5_fence_lease_id";
pub const META_FENCE_WORKER_ID: &str = "agnt5_fence_worker_id";
pub const META_FENCE_SESSION_HASH: &str = "agnt5_fence_session_hash";
pub const META_FENCE_ATTEMPT: &str = "agnt5_fence_attempt";
pub const META_FENCE_RECEIVED_AT_MS: &str = "agnt5_fence_received_at_ms";
pub const META_FENCE_REASON: &str = "agnt5_fence_reason";
pub const META_RESUME_SOURCE_ID: &str = "agnt5_resume_source_id";

pub const RUN_META_LEASE_SESSION_HASH: &str = "lease_session_hash";
pub const RUN_META_LEASE_ATTEMPT: &str = "lease_attempt";
pub const RUN_META_DISPATCH_GENERATION: &str = "lease_dispatch_generation";
pub const RUN_META_GRANT_ID: &str = "grant_id";
pub const RUN_META_GRANT_REQUEST_FINGERPRINT: &str = "grant_request_fingerprint";

pub const WORKER_SESSION_SIGNING_KEY_ENV: &str = "AGNT5_WORKER_SESSION_SIGNING_KEY";
pub const DEFAULT_WORKER_SESSION_SIGNING_KEY: &str = "agnt5-local-worker-session-signing-key-v1";
/// Public fallback used by the local HA Compose stack. Neither local key is
/// suitable for authenticating privileged SQL calls across the network.
pub const DEFAULT_LOCAL_HA_WORKER_SESSION_SIGNING_KEY: &str =
    "agnt5-local-ha-worker-session-signing-key-v1";

/// gRPC metadata header a runtime node attaches when it forwards
/// runtime-authored records (fenced terminals, renewals, expiries) to a peer
/// node's Engine service. Its value is [`runtime_peer_token`].
pub const RUNTIME_AUTHORED_HEADER: &str = "x-agnt5-runtime-authored";

/// Token proving an Engine append comes from another runtime node. Derived
/// from the node signing key, which workers never hold, so a worker cannot
/// present a fenced record as runtime-authored.
pub fn runtime_peer_token(signing_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"agnt5/runtime-peer/v1\0");
    hasher.update(signing_key.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// Event types a lease fence may legitimately appear on. Fence keys on any
/// other record are noise and are stripped at ingress.
pub fn fenceable_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "run.completed"
            | "run.failed"
            | "run.paused"
            | "workflow.paused"
            | RUN_LEASE_EXPIRED_EVENT
            | RUN_LEASE_RENEWED_EVENT
    )
}

/// Every metadata key the runtime owns on a lease-fenced record. Stripped
/// from caller-provided metadata on every external append path.
pub const RESERVED_FENCE_METADATA_KEYS: &[&str] = &[
    META_FENCE_KIND,
    META_FENCE_LEASE_ID,
    META_FENCE_WORKER_ID,
    META_FENCE_SESSION_HASH,
    META_FENCE_ATTEMPT,
    META_FENCE_RECEIVED_AT_MS,
    META_FENCE_REASON,
];

/// Return the shared signing key used by worker-session tokens. Production
/// HA validates that the environment value is explicitly configured; the
/// deterministic default remains for local tests.
pub fn worker_session_signing_key() -> String {
    std::env::var(WORKER_SESSION_SIGNING_KEY_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_WORKER_SESSION_SIGNING_KEY.to_string())
}

pub fn worker_session_hash(session_id: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(session_id.as_bytes()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseFenceKind {
    PullCompletion,
    PushCompletion,
    LeaseExpiry,
    LeaseRenewal,
}

impl LeaseFenceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PullCompletion => "pull_completion",
            Self::PushCompletion => "push_completion",
            Self::LeaseExpiry => "lease_expiry",
            Self::LeaseRenewal => "lease_renewal",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pull_completion" => Some(Self::PullCompletion),
            "push_completion" => Some(Self::PushCompletion),
            "lease_expiry" => Some(Self::LeaseExpiry),
            "lease_renewal" => Some(Self::LeaseRenewal),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseExpiryReason {
    /// The wall clock passed the lease expiry. The index requires the
    /// record's own timestamp to be at or past the entry's expiry.
    Timeout,
    /// The owning worker session disconnected. No clock comparison.
    WorkerDisconnect,
    /// An operator stopped the agent instance that owns the run. Its leader
    /// holds the run with a `run.paused` that ends the attempt's lease. No
    /// clock comparison.
    OperatorStop,
}

impl LeaseExpiryReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::WorkerDisconnect => "worker_disconnect",
            Self::OperatorStop => "operator_stop",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "timeout" => Some(Self::Timeout),
            "worker_disconnect" => Some(Self::WorkerDisconnect),
            "operator_stop" => Some(Self::OperatorStop),
            _ => None,
        }
    }
}

/// The lease authority a producer claims when it appends a fenced record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseFence {
    pub kind: LeaseFenceKind,
    pub lease_id: String,
    pub worker_id: String,
    /// Required for pull completions; empty for push completions and
    /// runtime-authored expiries, whose authority is the lease itself.
    pub session_hash: String,
    pub attempt: u32,
    /// Advisory: when the runtime received the request. Not part of the
    /// fence decision and excluded from retry fingerprints.
    pub received_at_ms: i64,
    /// Present only for `LeaseExpiry`.
    pub expiry_reason: Option<LeaseExpiryReason>,
}

impl LeaseFence {
    pub fn stamp(&self, record: &mut Record) {
        strip_reserved_fence_metadata(&mut record.metadata);
        let metadata = &mut record.metadata;
        metadata.insert(META_FENCE_KIND.to_string(), self.kind.as_str().to_string());
        metadata.insert(META_FENCE_LEASE_ID.to_string(), self.lease_id.clone());
        metadata.insert(META_FENCE_WORKER_ID.to_string(), self.worker_id.clone());
        if !self.session_hash.is_empty() {
            metadata.insert(
                META_FENCE_SESSION_HASH.to_string(),
                self.session_hash.clone(),
            );
        }
        metadata.insert(META_FENCE_ATTEMPT.to_string(), self.attempt.to_string());
        metadata.insert(
            META_FENCE_RECEIVED_AT_MS.to_string(),
            self.received_at_ms.to_string(),
        );
        if let Some(reason) = self.expiry_reason {
            metadata.insert(META_FENCE_REASON.to_string(), reason.as_str().to_string());
        }
    }
}

pub fn is_lease_fenced(record: &Record) -> bool {
    record.metadata.contains_key(META_FENCE_KIND)
}

/// Decode the fence a record carries. `Ok(None)` when the record is not
/// fenced; an error when it claims to be fenced but the envelope is
/// incomplete. Producers stamp through [`LeaseFence::stamp`], so a malformed
/// fence is a contract violation, never a routine outcome.
pub fn lease_fence(record: &Record) -> EngineResult<Option<LeaseFence>> {
    let Some(kind) = record.metadata.get(META_FENCE_KIND) else {
        return Ok(None);
    };
    let invalid = |reason: String| {
        EngineError::InvalidArgument(format!(
            "{} lease fence is invalid: {reason}",
            record.event_type
        ))
    };
    let kind = LeaseFenceKind::parse(kind)
        .ok_or_else(|| invalid(format!("unknown metadata[{META_FENCE_KIND}] {kind:?}")))?;
    let field = |key: &str| -> EngineResult<String> {
        record
            .metadata
            .get(key)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| invalid(format!("metadata[{key}] is required")))
    };
    let lease_id = field(META_FENCE_LEASE_ID)?;
    let worker_id = field(META_FENCE_WORKER_ID)?;
    let attempt = field(META_FENCE_ATTEMPT)?
        .parse::<u32>()
        .map_err(|_| invalid(format!("metadata[{META_FENCE_ATTEMPT}] must be a u32")))?;
    let session_hash = record
        .metadata
        .get(META_FENCE_SESSION_HASH)
        .cloned()
        .unwrap_or_default();
    if kind == LeaseFenceKind::PullCompletion && session_hash.is_empty() {
        return Err(invalid(format!(
            "pull completion requires metadata[{META_FENCE_SESSION_HASH}]"
        )));
    }
    let received_at_ms = record
        .metadata
        .get(META_FENCE_RECEIVED_AT_MS)
        .map(|value| {
            value.parse::<i64>().map_err(|_| {
                invalid(format!(
                    "metadata[{META_FENCE_RECEIVED_AT_MS}] must be an i64"
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();
    let expiry_reason = match (kind, record.metadata.get(META_FENCE_REASON)) {
        (LeaseFenceKind::LeaseExpiry, Some(reason)) => Some(
            LeaseExpiryReason::parse(reason)
                .ok_or_else(|| invalid(format!("unknown expiry reason {reason:?}")))?,
        ),
        (LeaseFenceKind::LeaseExpiry, None) => {
            return Err(invalid(format!(
                "lease expiry requires metadata[{META_FENCE_REASON}]"
            )))
        }
        _ => None,
    };
    let allowed_event = match kind {
        LeaseFenceKind::PullCompletion | LeaseFenceKind::PushCompletion => matches!(
            record.event_type.as_str(),
            "run.completed" | "run.failed" | "run.paused" | "workflow.paused"
        ),
        // An operator stop holds the run rather than retrying or failing it,
        // so it fences a pause, and only a pause.
        LeaseFenceKind::LeaseExpiry if expiry_reason == Some(LeaseExpiryReason::OperatorStop) => {
            record.event_type == "run.paused"
        }
        LeaseFenceKind::LeaseExpiry => {
            matches!(
                record.event_type.as_str(),
                RUN_LEASE_EXPIRED_EVENT | "run.failed"
            )
        }
        LeaseFenceKind::LeaseRenewal => record.event_type == RUN_LEASE_RENEWED_EVENT,
    };
    if !allowed_event {
        return Err(invalid(format!(
            "fence kind {} cannot fence event {}",
            kind.as_str(),
            record.event_type
        )));
    }
    if kind == LeaseFenceKind::LeaseRenewal && renewed_lease_expiry(record).is_none() {
        return Err(invalid(
            "lease renewal requires a positive metadata[lease_expires_at_ms]".to_string(),
        ));
    }
    Ok(Some(LeaseFence {
        kind,
        lease_id,
        worker_id,
        session_hash,
        attempt,
        received_at_ms,
        expiry_reason,
    }))
}

/// The new expiry a `run.lease_renewed` record carries.
pub fn renewed_lease_expiry(record: &Record) -> Option<i64> {
    record
        .metadata
        .get("lease_expires_at_ms")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
}

/// Remove every runtime-owned fence key from caller-provided metadata.
pub fn strip_reserved_fence_metadata(metadata: &mut HashMap<String, String>) {
    for key in RESERVED_FENCE_METADATA_KEYS {
        metadata.remove(*key);
    }
}

/// Remove fence and assignment metadata that must not cross a pause/resume
/// boundary. Resume creates a new dispatch generation; retaining assignment
/// fields can make the new dispatch appear to own the old lease.
///
/// The previous resume's `user_response` goes too. An answer belongs to the
/// pause it was given for, but the run projection keeps it in the run's
/// metadata, so a timer or signal resume built from that metadata handed it to
/// the next question, which then completed without anyone being asked.
/// A manual resume adds its own answer after this runs.
pub fn strip_stale_resume_metadata(metadata: &mut HashMap<String, String>) {
    strip_reserved_fence_metadata(metadata);
    for key in [
        "user_response",
        "agnt5_approval_response",
        "approval_idempotency_key",
        "approval_id",
        "decision",
        "decided_by",
        "channel",
        "lease_id",
        "worker_id",
        RUN_META_LEASE_ATTEMPT,
        "lease_expires_at_ms",
        RUN_META_LEASE_SESSION_HASH,
        "lease_worker_id",
        "lease_timeout_ms",
        RUN_META_GRANT_ID,
        RUN_META_GRANT_REQUEST_FINGERPRINT,
        META_RESUME_SOURCE_ID,
        "worker_session_id",
    ] {
        metadata.remove(key);
    }
}

/// Resolve the stable pause generation used to make every resume trigger
/// (manual, signal, or timer) converge on one journal idempotency key.
///
/// Lease-backed pauses use the lease that produced the pause. An explicit
/// source remains available for unleased producers that generate their own
/// stable pause token.
pub fn resume_source_id(metadata: &HashMap<String, String>) -> Option<String> {
    metadata
        .get(META_FENCE_LEASE_ID)
        .or_else(|| metadata.get("lease_id"))
        .filter(|value| !value.is_empty())
        .map(|lease_id| format!("lease:{lease_id}"))
        .or_else(|| {
            metadata
                .get(META_RESUME_SOURCE_ID)
                .filter(|value| !value.is_empty())
                .cloned()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Offset;
    use bytes::Bytes;

    fn record(event_type: &str) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "project-1".to_string(),
            run_id: "run-1".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::from_static(br#"{"output_data":"ok"}"#),
            timestamp_ns: 1_700_000_000_000_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn fence(kind: LeaseFenceKind) -> LeaseFence {
        LeaseFence {
            kind,
            lease_id: "lease-1".to_string(),
            worker_id: "worker-1".to_string(),
            session_hash: worker_session_hash("session-1"),
            attempt: 2,
            received_at_ms: 1_700_000_000_123,
            expiry_reason: (kind == LeaseFenceKind::LeaseExpiry)
                .then_some(LeaseExpiryReason::Timeout),
        }
    }

    #[test]
    fn stamp_and_decode_round_trip() {
        let mut completed = record("run.completed");
        fence(LeaseFenceKind::PullCompletion).stamp(&mut completed);
        assert_eq!(
            lease_fence(&completed).unwrap(),
            Some(fence(LeaseFenceKind::PullCompletion))
        );
        assert!(is_lease_fenced(&completed));
        assert!(lease_fence(&record("run.completed")).unwrap().is_none());
    }

    #[test]
    fn pull_completion_requires_session_hash() {
        let mut completed = record("run.completed");
        let mut pull = fence(LeaseFenceKind::PullCompletion);
        pull.session_hash.clear();
        pull.stamp(&mut completed);
        assert!(lease_fence(&completed).is_err());

        let mut push = fence(LeaseFenceKind::PushCompletion);
        push.session_hash.clear();
        push.stamp(&mut completed);
        assert!(lease_fence(&completed).unwrap().is_some());
    }

    #[test]
    fn fence_kind_must_match_event() {
        let mut expired = record(RUN_LEASE_EXPIRED_EVENT);
        fence(LeaseFenceKind::PullCompletion).stamp(&mut expired);
        assert!(lease_fence(&expired).is_err());

        fence(LeaseFenceKind::LeaseExpiry).stamp(&mut expired);
        assert_eq!(
            lease_fence(&expired).unwrap().unwrap().expiry_reason,
            Some(LeaseExpiryReason::Timeout)
        );

        let mut renewed = record(RUN_LEASE_RENEWED_EVENT);
        fence(LeaseFenceKind::LeaseRenewal).stamp(&mut renewed);
        assert!(lease_fence(&renewed).is_err(), "renewal needs a new expiry");
        renewed
            .metadata
            .insert("lease_expires_at_ms".to_string(), "5000".to_string());
        assert_eq!(renewed_lease_expiry(&renewed), Some(5000));
        assert!(lease_fence(&renewed).unwrap().is_some());

        // An operator stop fences a pause, and nothing else; no other expiry
        // fences a pause.
        let mut stop = fence(LeaseFenceKind::LeaseExpiry);
        stop.expiry_reason = Some(LeaseExpiryReason::OperatorStop);
        let mut hold = record("run.paused");
        stop.stamp(&mut hold);
        assert_eq!(
            lease_fence(&hold).unwrap().unwrap().expiry_reason,
            Some(LeaseExpiryReason::OperatorStop)
        );
        let mut stopped_expiry = record(RUN_LEASE_EXPIRED_EVENT);
        stop.stamp(&mut stopped_expiry);
        assert!(lease_fence(&stopped_expiry).is_err());
        let mut timed_out_pause = record("run.paused");
        fence(LeaseFenceKind::LeaseExpiry).stamp(&mut timed_out_pause);
        assert!(lease_fence(&timed_out_pause).is_err());
    }

    #[test]
    fn stale_resume_metadata_is_removed_without_dropping_checkpoint_state() {
        let mut metadata = HashMap::from([
            (META_FENCE_KIND.to_string(), "pull_completion".to_string()),
            (META_FENCE_LEASE_ID.to_string(), "lease-1".to_string()),
            ("lease_id".to_string(), "lease-1".to_string()),
            ("worker_id".to_string(), "worker-1".to_string()),
            (RUN_META_LEASE_ATTEMPT.to_string(), "2".to_string()),
            ("lease_expires_at_ms".to_string(), "1234".to_string()),
            (
                RUN_META_LEASE_SESSION_HASH.to_string(),
                "session-hash".to_string(),
            ),
            ("lease_worker_id".to_string(), "worker-1".to_string()),
            ("lease_timeout_ms".to_string(), "30000".to_string()),
            (META_RESUME_SOURCE_ID.to_string(), "pause:7".to_string()),
            (
                "workerless_checkpoint".to_string(),
                r#"{"steps":{"wait":true}}"#.to_string(),
            ),
        ]);

        strip_stale_resume_metadata(&mut metadata);

        assert_eq!(
            metadata.get("workerless_checkpoint").map(String::as_str),
            Some(r#"{"steps":{"wait":true}}"#)
        );
        assert_eq!(metadata.len(), 1, "every lease and fence key is stripped");
    }

    #[test]
    fn resume_source_prefers_the_pause_fence_generation() {
        let metadata = HashMap::from([
            (
                META_RESUME_SOURCE_ID.to_string(),
                "pause:legacy".to_string(),
            ),
            ("lease_id".to_string(), "lease-unfenced".to_string()),
            (META_FENCE_LEASE_ID.to_string(), "lease-fenced".to_string()),
        ]);

        assert_eq!(
            resume_source_id(&metadata).as_deref(),
            Some("lease:lease-fenced")
        );
    }
}

#[cfg(test)]
mod answer_metadata_tests {
    use super::*;
    #[test]
    fn timer_and_signal_resumes_do_not_inherit_a_previous_human_answer() {
        let mut metadata = HashMap::from([
            ("user_response".into(), "yes".into()),
            ("agnt5_approval_response".into(), "intent".into()),
            ("approval_idempotency_key".into(), "request-1".into()),
            ("approval_id".into(), "run:0".into()),
            ("decision".into(), "approved".into()),
            ("decided_by".into(), "user-1".into()),
            ("channel".into(), "web".into()),
            ("component_name".into(), "agent".into()),
        ]);
        strip_stale_resume_metadata(&mut metadata);
        assert_eq!(
            metadata,
            HashMap::from([("component_name".into(), "agent".into())])
        );
    }
}
