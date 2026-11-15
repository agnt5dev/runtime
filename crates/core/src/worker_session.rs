//! Signed pull-worker session tokens shared by runtimes and coordinator edges.
//!
//! The token authenticates identity and deployment scope. Component routes
//! remain in the live routing record so re-registration can supersede an old
//! token without changing the public PollJob request.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use engine_proto::WorkerSlotPolicy;

pub const WORKER_SESSION_TOKEN_VERSION: &str = "agnt5ws1";
pub const DEFAULT_WORKER_SESSION_TTL_MS: i64 = 3_600_000;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerSessionClaims {
    #[serde(rename = "nonce")]
    pub routing_id: String,
    pub worker_id: String,
    pub project_id: String,
    pub deployment_id: String,
    #[serde(default)]
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
    /// Present only when server-driven slot scaling was negotiated. Keeping
    /// the effective policy in the signed token lets any stateless coordinator
    /// edge make the same bounded decision without trusting worker input or a
    /// replicated per-session policy cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_scaling: Option<WorkerSessionSlotScalingClaims>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerSessionSlotScalingClaims {
    pub min_slots: u32,
    pub max_slots: u32,
    pub target_cpu_micros: u32,
    pub target_memory_micros: u32,
    pub ramp_throttle_ms: i64,
}

impl WorkerSessionSlotScalingClaims {
    pub fn from_policy(policy: &WorkerSlotPolicy) -> Self {
        Self {
            min_slots: policy.min_slots,
            max_slots: policy.max_slots,
            target_cpu_micros: usage_to_micros(policy.target_cpu_usage),
            target_memory_micros: usage_to_micros(policy.target_memory_usage),
            ramp_throttle_ms: policy.ramp_throttle_ms,
        }
    }

    pub fn to_policy(&self) -> WorkerSlotPolicy {
        WorkerSlotPolicy {
            min_slots: self.min_slots,
            max_slots: self.max_slots,
            target_cpu_usage: f64::from(self.target_cpu_micros) / 1_000_000.0,
            target_memory_usage: f64::from(self.target_memory_micros) / 1_000_000.0,
            ramp_throttle_ms: self.ramp_throttle_ms,
        }
    }
}

fn usage_to_micros(usage: f64) -> u32 {
    (usage.clamp(0.0, 1.0) * 1_000_000.0).round() as u32
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkerSessionTokenError {
    #[error("worker session token is malformed")]
    Malformed,
    #[error("worker session token signature is invalid")]
    InvalidSignature,
    #[error("worker session token identity does not match")]
    WorkerMismatch,
    #[error("worker session token scope is invalid")]
    InvalidScope,
    #[error("worker session token is expired")]
    Expired,
}

pub fn encode_worker_session_token(
    claims: &WorkerSessionClaims,
    signing_key: &str,
) -> Option<String> {
    let payload_json = serde_json::to_vec(claims).ok()?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload_json);
    let signature = sign_payload(&payload_b64, signing_key)?;
    Some(format!(
        "{WORKER_SESSION_TOKEN_VERSION}.{payload_b64}.{signature}"
    ))
}

pub fn verify_worker_session_token(
    worker_id: &str,
    token: &str,
    signing_key: &str,
    now_ms: i64,
) -> Result<WorkerSessionClaims, WorkerSessionTokenError> {
    let claims = verify_worker_session_token_claims(token, signing_key, now_ms)?;
    if claims.worker_id != worker_id {
        return Err(WorkerSessionTokenError::WorkerMismatch);
    }
    Ok(claims)
}

pub fn verify_worker_session_token_claims(
    token: &str,
    signing_key: &str,
    now_ms: i64,
) -> Result<WorkerSessionClaims, WorkerSessionTokenError> {
    let mut parts = token.split('.');
    if parts.next() != Some(WORKER_SESSION_TOKEN_VERSION) {
        return Err(WorkerSessionTokenError::Malformed);
    }
    let payload_b64 = parts.next().ok_or(WorkerSessionTokenError::Malformed)?;
    let signature_b64 = parts.next().ok_or(WorkerSessionTokenError::Malformed)?;
    if parts.next().is_some() {
        return Err(WorkerSessionTokenError::Malformed);
    }
    verify_signature(payload_b64, signature_b64, signing_key)?;
    let payload_json = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| WorkerSessionTokenError::Malformed)?;
    let mut claims: WorkerSessionClaims =
        serde_json::from_slice(&payload_json).map_err(|_| WorkerSessionTokenError::Malformed)?;
    if claims.routing_id.is_empty()
        || claims.project_id.is_empty()
        || claims.deployment_id.is_empty()
    {
        return Err(WorkerSessionTokenError::InvalidScope);
    }
    if claims.expires_at_ms <= now_ms {
        return Err(WorkerSessionTokenError::Expired);
    }
    if claims.issued_at_ms <= 0 {
        claims.issued_at_ms = claims
            .expires_at_ms
            .saturating_sub(DEFAULT_WORKER_SESSION_TTL_MS);
    }
    Ok(claims)
}

fn sign_payload(payload_b64: &str, signing_key: &str) -> Option<String> {
    let mut mac = HmacSha256::new_from_slice(signing_key.as_bytes()).ok()?;
    mac.update(payload_b64.as_bytes());
    Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn verify_signature(
    payload_b64: &str,
    signature_b64: &str,
    signing_key: &str,
) -> Result<(), WorkerSessionTokenError> {
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(signature_b64)
        .map_err(|_| WorkerSessionTokenError::InvalidSignature)?;
    let mut mac = HmacSha256::new_from_slice(signing_key.as_bytes())
        .map_err(|_| WorkerSessionTokenError::InvalidSignature)?;
    mac.update(payload_b64.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| WorkerSessionTokenError::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> WorkerSessionClaims {
        WorkerSessionClaims {
            routing_id: "route-1".to_string(),
            worker_id: "worker-1".to_string(),
            project_id: "project-1".to_string(),
            deployment_id: "deployment-1".to_string(),
            issued_at_ms: 1_000,
            expires_at_ms: 2_000,
            slot_scaling: None,
        }
    }

    #[test]
    fn signed_session_round_trips_and_binds_worker() {
        let token = encode_worker_session_token(&claims(), "key-a").unwrap();
        assert_eq!(
            verify_worker_session_token("worker-1", &token, "key-a", 1_500).unwrap(),
            claims()
        );
        assert_eq!(
            verify_worker_session_token("worker-2", &token, "key-a", 1_500),
            Err(WorkerSessionTokenError::WorkerMismatch)
        );
    }

    #[test]
    fn forged_and_expired_sessions_are_rejected() {
        let token = encode_worker_session_token(&claims(), "key-a").unwrap();
        assert_eq!(
            verify_worker_session_token("worker-1", &token, "key-b", 1_500),
            Err(WorkerSessionTokenError::InvalidSignature)
        );
        assert_eq!(
            verify_worker_session_token("worker-1", &token, "key-a", 2_000),
            Err(WorkerSessionTokenError::Expired)
        );
    }

    #[test]
    fn legacy_claims_decode_without_slot_scaling_and_policy_round_trips() {
        let legacy = serde_json::json!({
            "nonce": "route-1",
            "worker_id": "worker-1",
            "project_id": "project-1",
            "deployment_id": "deployment-1",
            "issued_at_ms": 1_000,
            "expires_at_ms": 2_000
        });
        let decoded: WorkerSessionClaims = serde_json::from_value(legacy).unwrap();
        assert!(decoded.slot_scaling.is_none());

        let policy = WorkerSlotPolicy {
            min_slots: 2,
            max_slots: 12,
            target_cpu_usage: 0.75,
            target_memory_usage: 0.8,
            ramp_throttle_ms: 750,
        };
        let claims = WorkerSessionSlotScalingClaims::from_policy(&policy);
        assert_eq!(claims.to_policy(), policy);
    }
}
