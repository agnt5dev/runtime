//! Exact, journal-authoritative pull grant admission.
//!
//! The caller supplies a live-offer authority; Phase 3 will back that port
//! with the bounded outstanding-offer table. This module owns everything
//! after offer validation: exact retry recovery, session/route rechecks,
//! assignment construction, append, and typed race outcomes.

use engine_core::demand::DemandKey;
use engine_core::dispatch_generation::{resolve_projected_dispatch_generation, DispatchGeneration};
use engine_core::error::EngineError;
use engine_core::storage_contract::RunQueryStore;
use engine_core::traits::Segment;
use engine_core::types::Offset;
use engine_proto::Run;
use prost::Message;
use sha2::{Digest, Sha256};

use super::poll_jobs::PullAssignmentIdentity;
use super::{now_ms, EngineServiceImpl, DEFAULT_LEASE_TIMEOUT_MS};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TryGrantLease {
    pub coordinator_incarnation: String,
    pub offer_id: String,
    pub grant_id: String,
    pub project_id: String,
    pub deployment_id: String,
    pub component_type: i32,
    pub component_name: String,
    pub run_id: String,
    pub dispatch_generation: DispatchGeneration,
    pub partition_id: u32,
    pub leader_epoch: u64,
    pub worker_id: String,
    pub worker_session_id: String,
    pub lease_timeout_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrantedLease {
    pub grant_id: String,
    pub lease_id: String,
    pub lease_expires_at_ms: i64,
    pub lease_timeout_ms: i64,
    pub accepted_offset: Offset,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantOutcome {
    Granted(GrantedLease),
    OfferExpired,
    StaleGeneration,
    AlreadyLeased,
    RunSettled,
    Incompatible,
    NotLeader { owner: Option<String> },
    ResourceExhausted { retry_after_ms: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantError {
    InvalidRequest(&'static str),
    /// The caller cannot know whether a write committed and must retry this
    /// exact request without selecting another offer or grant identity.
    TransportUnknown(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfferRejection {
    OfferExpired,
    StaleGeneration,
    Incompatible,
    NotLeader { owner: Option<String> },
}

/// Authority for the leader-local outstanding-offer table.
///
/// Validation must prove the offer is live, belongs to the request's exact
/// run and generation, and was minted in the current leader epoch.
pub trait LiveGrantOffer: Send + Sync {
    fn validate(&self, request: &TryGrantLease, now_ms: i64) -> Result<(), OfferRejection>;
}

pub struct RunLeaseAuthority<'a, S: Segment> {
    service: &'a EngineServiceImpl<S>,
    offers: &'a dyn LiveGrantOffer,
}

impl<'a, S: Segment> RunLeaseAuthority<'a, S> {
    pub fn new(service: &'a EngineServiceImpl<S>, offers: &'a dyn LiveGrantOffer) -> Self {
        Self { service, offers }
    }

    pub async fn try_grant(&self, request: TryGrantLease) -> Result<GrantOutcome, GrantError> {
        let request_demand_key = validate_request(&request)?;

        if let Some(outcome) = self.resolve_existing(&request).await? {
            return Ok(outcome);
        }

        if let Err(rejection) = self.offers.validate(&request, now_ms()) {
            return Ok(rejection.into());
        }

        let Some(session) = self
            .service
            .worker_sessions
            .get_current(&request.worker_id, &request.worker_session_id)
        else {
            return Ok(GrantOutcome::Incompatible);
        };
        if session.project_id != request.project_id
            || session.deployment_id != request.deployment_id
        {
            return Ok(GrantOutcome::Incompatible);
        }

        let Some(run) = self.load_run(&request).await? else {
            return Ok(GrantOutcome::RunSettled);
        };
        if run.status != "queued" {
            return Ok(if run.lease_id.is_empty() {
                GrantOutcome::RunSettled
            } else {
                GrantOutcome::AlreadyLeased
            });
        }
        if resolve_projected_dispatch_generation(&run.metadata, run.attempt).generation
            != request.dispatch_generation
        {
            return Ok(GrantOutcome::StaleGeneration);
        }
        let Ok(run_demand_key) = DemandKey::from_run(&run) else {
            return Ok(GrantOutcome::Incompatible);
        };
        if run_demand_key != request_demand_key
            || run.run_id != request.run_id
            || self
                .service
                .worker_session_component_route(&session, &request_demand_key)
                .is_none()
        {
            return Ok(GrantOutcome::Incompatible);
        }

        let lease_id = uuid::Uuid::now_v7().to_string();
        let request_fingerprint = grant_request_fingerprint(&request);
        match self
            .service
            .append_lease_assigned_with_ids(
                &request.project_id,
                &run,
                &session,
                PullAssignmentIdentity {
                    lease_id,
                    grant_id: request.grant_id.clone(),
                    grant_request_fingerprint: Some(request_fingerprint),
                },
                request.lease_timeout_ms,
            )
            .await
        {
            Ok(receipt) => Ok(GrantOutcome::Granted(GrantedLease {
                grant_id: request.grant_id,
                lease_id: receipt.lease.lease_id,
                lease_expires_at_ms: receipt.lease.lease_expires_at_ms,
                lease_timeout_ms: receipt.lease.lease_timeout_ms,
                accepted_offset: receipt.accepted_offset,
                replayed: !receipt.appended,
            })),
            Err(EngineError::LeaseFenceMismatch { .. }) => {
                if let Some(outcome) = self.resolve_existing(&request).await? {
                    Ok(outcome)
                } else {
                    Ok(GrantOutcome::StaleGeneration)
                }
            }
            Err(EngineError::InvalidArgument(_)) => Ok(GrantOutcome::RunSettled),
            Err(
                EngineError::StaleEpoch { current_owner, .. }
                | EngineError::NotPartitionOwner { current_owner, .. },
            ) => Ok(GrantOutcome::NotLeader {
                owner: current_owner,
            }),
            Err(
                EngineError::EpochAhead { .. }
                | EngineError::PartitionNotWritable { .. }
                | EngineError::Sealed,
            ) => Ok(GrantOutcome::NotLeader { owner: None }),
            Err(error) => Err(GrantError::TransportUnknown(error.to_string())),
        }
    }

    async fn resolve_existing(
        &self,
        request: &TryGrantLease,
    ) -> Result<Option<GrantOutcome>, GrantError> {
        let lease = self
            .service
            .segment
            .resolve_run_lease_authority(
                &request.project_id,
                &request.run_id,
                &request.grant_id,
                &grant_request_fingerprint(request),
                request.leader_epoch,
            )
            .await
            .map_err(|error| GrantError::TransportUnknown(error.to_string()))?;
        let Some(lease) = lease else {
            return Ok(None);
        };
        if lease.dispatch_generation == request.dispatch_generation.as_str() {
            let exact = !lease.grant_id.is_empty()
                && lease.grant_id == request.grant_id
                && !lease.grant_request_fingerprint.is_empty()
                && lease.grant_request_fingerprint == grant_request_fingerprint(request);
            if exact {
                if lease.released {
                    return Ok(Some(GrantOutcome::RunSettled));
                }
                let Some(accepted_offset) = lease.accepted_offset else {
                    return Err(GrantError::TransportUnknown(
                        "accepted grant has no journal offset".to_string(),
                    ));
                };
                return Ok(Some(GrantOutcome::Granted(GrantedLease {
                    grant_id: lease.grant_id,
                    lease_id: lease.lease_id,
                    lease_expires_at_ms: lease.expires_at_ms,
                    lease_timeout_ms: lease.lease_timeout_ms,
                    accepted_offset: Offset(accepted_offset),
                    replayed: true,
                })));
            }
            return Ok(Some(GrantOutcome::AlreadyLeased));
        }
        if !lease.released {
            return Ok(Some(GrantOutcome::AlreadyLeased));
        }
        Ok(None)
    }

    async fn load_run(&self, request: &TryGrantLease) -> Result<Option<Run>, GrantError> {
        let bytes = if let Some(backend) = self.service.completion_append_backend() {
            backend
                .get_run(&request.project_id, &request.run_id)
                .await
                .map_err(|error| GrantError::TransportUnknown(error.to_string()))?
        } else {
            let Some(store) = self.service.store.as_ref() else {
                return Err(GrantError::TransportUnknown(
                    "materialized run store is unavailable".to_string(),
                ));
            };
            RunQueryStore::load_run(store.as_ref(), &request.project_id, &request.run_id)
                .await
                .map_err(|error| GrantError::TransportUnknown(error.to_string()))?
        };
        bytes
            .map(|bytes| {
                Run::decode(bytes.as_slice())
                    .map_err(|error| GrantError::TransportUnknown(error.to_string()))
            })
            .transpose()
    }
}

impl From<OfferRejection> for GrantOutcome {
    fn from(rejection: OfferRejection) -> Self {
        match rejection {
            OfferRejection::OfferExpired => Self::OfferExpired,
            OfferRejection::StaleGeneration => Self::StaleGeneration,
            OfferRejection::Incompatible => Self::Incompatible,
            OfferRejection::NotLeader { owner } => Self::NotLeader { owner },
        }
    }
}

fn validate_request(request: &TryGrantLease) -> Result<DemandKey, GrantError> {
    for (value, name) in [
        (&request.coordinator_incarnation, "coordinator_incarnation"),
        (&request.offer_id, "offer_id"),
        (&request.grant_id, "grant_id"),
        (&request.project_id, "project_id"),
        (&request.deployment_id, "deployment_id"),
        (&request.component_name, "component_name"),
        (&request.run_id, "run_id"),
        (&request.worker_id, "worker_id"),
        (&request.worker_session_id, "worker_session_id"),
    ] {
        if value.is_empty() {
            return Err(GrantError::InvalidRequest(name));
        }
    }
    if request.dispatch_generation.as_str().is_empty() {
        return Err(GrantError::InvalidRequest("dispatch_generation"));
    }
    let component_type = engine_proto::ComponentType::try_from(request.component_type)
        .map_err(|_| GrantError::InvalidRequest("demand_key"))?;
    DemandKey::new(
        &request.project_id,
        &request.deployment_id,
        component_type,
        &request.component_name,
    )
    .map_err(|_| GrantError::InvalidRequest("demand_key"))
}

fn normalized_timeout(timeout_ms: i64) -> i64 {
    if timeout_ms > 0 {
        timeout_ms
    } else {
        DEFAULT_LEASE_TIMEOUT_MS
    }
}

fn grant_request_fingerprint(request: &TryGrantLease) -> String {
    use base64::Engine as _;

    let mut hasher = Sha256::new();
    for value in [
        request.coordinator_incarnation.as_bytes(),
        request.offer_id.as_bytes(),
        request.grant_id.as_bytes(),
        request.project_id.as_bytes(),
        request.deployment_id.as_bytes(),
        request.component_name.as_bytes(),
        request.run_id.as_bytes(),
        request.dispatch_generation.as_str().as_bytes(),
        request.worker_id.as_bytes(),
        request.worker_session_id.as_bytes(),
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    hasher.update(request.component_type.to_be_bytes());
    hasher.update(request.partition_id.to_be_bytes());
    hasher.update(request.leader_epoch.to_be_bytes());
    hasher.update(normalized_timeout(request.lease_timeout_ms).to_be_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}
