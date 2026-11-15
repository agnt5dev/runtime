//! Journal-authoritative Durable Activation and run-lease indexes.
//!
//! These helpers describe the state that storage adapters maintain in the
//! same atomic write as the canonical journal record. Unlike processor
//! projections, this index is allowed to participate in correctness decisions
//! because it cannot advance independently of the journal append.
//!
//! Four key families live here: activation heads, required-child
//! obligations, run gates, and run leases. The run lease entry is a fold over
//! the lease events already in the journal (`run.assigned`,
//! `run.lease_renewed`, `run.lease_expired`, pause, terminal). A lease-fenced
//! record is decided against that fold in log order, so completion versus
//! expiry versus renewal is settled by partition order and the loser never
//! enters the journal.

use std::collections::{BTreeMap, HashMap};

use engine_proto::{ActivationKind, ActivationStatus, ChildJoinPolicy};
use prost::Message;

use crate::activation::{
    self, apply_journal_record, from_engine_record, has_activation_marker,
    is_unsettled_required_child, state_from_snapshot, ActivationState, Event,
};
use crate::error::{ActivationError, EngineError, EngineResult};
use crate::lease_fence::{
    lease_fence, renewed_lease_expiry, LeaseExpiryReason, LeaseFence, RUN_LEASE_EXPIRED_EVENT,
    RUN_LEASE_RENEWED_EVENT, RUN_META_DISPATCH_GENERATION, RUN_META_LEASE_ATTEMPT,
    RUN_META_LEASE_SESSION_HASH,
};
use crate::routing::partition_for_run;
use crate::traits::Segment;
use crate::types::{Offset, Record};

const HEAD_PREFIX: &[u8] = b"activation-authority/head/v1\0";
const OBLIGATION_PREFIX: &[u8] = b"activation-authority/required-child/v1\0";
const RUN_GATE_PREFIX: &[u8] = b"activation-authority/run-gate/v1\0";
const RUN_LEASE_PREFIX: &[u8] = b"activation-authority/run-lease/v1\0";

/// The lease entry the index holds for one run. Never deleted once written:
/// a released lease stays as a tombstone so a late fenced record from that
/// lease is rejected rather than accepted on the gate alone. An absent entry
/// means "unindexed" (the assignment predates the retained floor), and the
/// gate alone decides.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct RunLeaseEntry {
    #[prost(string, tag = "1")]
    pub lease_id: String,
    #[prost(uint32, tag = "2")]
    pub attempt: u32,
    #[prost(string, tag = "3")]
    pub worker_id: String,
    #[prost(string, tag = "4")]
    pub session_hash: String,
    #[prost(int64, tag = "5")]
    pub expires_at_ms: i64,
    #[prost(string, tag = "6")]
    pub dispatch_generation: String,
    #[prost(bool, tag = "7")]
    pub released: bool,
    #[prost(string, tag = "8")]
    pub release_reason: String,
    /// Stable identity of the coordinator grant that created the assignment.
    /// Empty only for assignments written before grant identity existed.
    #[prost(string, tag = "9")]
    pub grant_id: String,
    /// Journal offset of the accepted assignment. Optional for legacy index
    /// entries rebuilt from formats that did not retain it.
    #[prost(uint64, optional, tag = "10")]
    pub accepted_offset: Option<u64>,
    /// Lease duration requested by the accepted grant. Retained so an exact
    /// retry can validate the full request after cache loss.
    #[prost(int64, tag = "11")]
    pub lease_timeout_ms: i64,
    /// Digest of the exact grant request. Empty for assignments predating the
    /// exact-grant protocol.
    #[prost(string, tag = "12")]
    pub grant_request_fingerprint: String,
    /// Local journal offset of the latest assignment, renewal, or release.
    /// Replicas must fence the entire lease fold against their committed prefix.
    #[prost(uint64, optional, tag = "13")]
    pub updated_offset: Option<u64>,
}

impl RunLeaseEntry {
    fn released(mut self, reason: &str) -> Self {
        self.released = true;
        self.release_reason = reason.to_string();
        self
    }
}

pub fn encode_run_lease_entry(entry: &RunLeaseEntry) -> Vec<u8> {
    entry.encode_to_vec()
}

pub fn decode_run_lease_entry(bytes: &[u8]) -> EngineResult<RunLeaseEntry> {
    RunLeaseEntry::decode(bytes).map_err(|error| EngineError::Serialization(error.to_string()))
}

/// How strictly the lease fence is applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityMode {
    /// A new append. The fence rejects and nothing is written.
    Live,
    /// Records the journal already accepted elsewhere: cold restore, index
    /// rebuild, and a follower persisting a leader's batch. The lease fold is
    /// applied in full and never rejects on lease grounds, because the leader
    /// that accepted the record may have held index state this replica has
    /// trimmed or never built.
    TrustedHistory,
}

#[derive(Clone, Debug)]
pub struct ActivationProbe {
    pub canonical: activation::ActivationJournalRecord,
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub parent_activation_id: String,
    pub attempt: u32,
    pub is_started: bool,
    pub is_snapshot: bool,
    pub requires_parent_active: bool,
    pub requires_children_settled: bool,
    pub lease_guard: Option<ActivationLeaseGuard>,
}

/// The run lease a worker-driven activation write was validated against. The
/// RPC reads the run before appending, so a pause or reassignment can land in
/// between; carrying the lease lets the append itself reject that write.
///
/// These keys are deliberately not the `agnt5_fence_*` lifecycle family: a
/// guard only checks the lease, never changes it, and owns no retry slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationLeaseGuard {
    pub lease_id: String,
    pub attempt: u32,
}

impl ActivationLeaseGuard {
    pub fn stamp(&self, record: &mut Record) {
        record
            .metadata
            .insert(META_ACTIVATION_LEASE_ID.to_string(), self.lease_id.clone());
        record.metadata.insert(
            META_ACTIVATION_LEASE_ATTEMPT.to_string(),
            self.attempt.to_string(),
        );
    }

    fn from_record(record: &Record) -> Option<Self> {
        Some(Self {
            lease_id: record.metadata.get(META_ACTIVATION_LEASE_ID)?.clone(),
            attempt: record
                .metadata
                .get(META_ACTIVATION_LEASE_ATTEMPT)?
                .parse()
                .ok()?,
        })
    }

    fn matches(&self, entry: &RunLeaseEntry) -> bool {
        !entry.released && entry.lease_id == self.lease_id && entry.attempt == self.attempt
    }
}

pub const META_ACTIVATION_LEASE_ID: &str = "agnt5_activation_lease_id";
pub const META_ACTIVATION_LEASE_ATTEMPT: &str = "agnt5_activation_lease_attempt";

#[derive(Clone, Debug)]
pub struct RunTerminalProbe {
    pub project_id: String,
    pub run_id: String,
    pub requires_children_settled: bool,
    /// The record's own timestamp, for the timeout rule on a fenced
    /// dead-letter failure authored by an expiry sweeper.
    pub record_ts_ms: i64,
    /// Present for worker- and sweeper-authored terminals. Control-plane
    /// cancellation and terminal-supervisor outcomes are unfenced and keep
    /// gate-only semantics.
    pub fence: Option<LeaseFence>,
}

#[derive(Clone, Debug)]
pub struct RunAssignmentProbe {
    pub project_id: String,
    pub run_id: String,
    pub lease: RunLeaseEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseTransition {
    /// `run.lease_renewed`: extend the entry's expiry.
    Renewal { expires_at_ms: i64 },
    /// `run.lease_expired`: release the entry. `record_ts_ms` is the record's
    /// own timestamp, compared to the entry's expiry for timeout expiries.
    Expiry { record_ts_ms: i64 },
    /// Pause or retryable failure: release the entry without closing the gate.
    Release,
}

#[derive(Clone, Debug)]
pub struct RunLeaseProbe {
    pub project_id: String,
    pub run_id: String,
    pub fence: LeaseFence,
    pub transition: LeaseTransition,
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum AuthorityProbe {
    Activation(ActivationProbe),
    RunAssignment(RunAssignmentProbe),
    RunLease(RunLeaseProbe),
    RunTerminal(RunTerminalProbe),
}

impl AuthorityProbe {
    pub fn run_scope(&self) -> (&str, &str) {
        match self {
            Self::Activation(probe) => (&probe.project_id, &probe.run_id),
            Self::RunAssignment(probe) => (&probe.project_id, &probe.run_id),
            Self::RunLease(probe) => (&probe.project_id, &probe.run_id),
            Self::RunTerminal(probe) => (&probe.project_id, &probe.run_id),
        }
    }

    pub fn reads_lease(&self) -> bool {
        match self {
            Self::Activation(probe) => probe.lease_guard.is_some(),
            _ => true,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AuthorityContext {
    pub current: Option<ActivationState>,
    pub parent: Option<ActivationState>,
    pub first_unsettled_required_child: Option<String>,
    pub run_terminal_offset: Option<Offset>,
    pub lease: Option<RunLeaseEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObligationMutation {
    None,
    Upsert {
        project_id: String,
        run_id: String,
        parent_activation_id: String,
        child_activation_id: String,
    },
    Delete {
        project_id: String,
        run_id: String,
        parent_activation_id: String,
        child_activation_id: String,
    },
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum AuthorityDecision {
    Activation {
        state: ActivationState,
        obligation: ObligationMutation,
    },
    RunAssignment {
        project_id: String,
        run_id: String,
        lease: RunLeaseEntry,
    },
    RunLease {
        project_id: String,
        run_id: String,
        lease: RunLeaseEntry,
    },
    RunTerminal {
        project_id: String,
        run_id: String,
        accepted_offset: Offset,
        already_terminal: bool,
        /// The lease entry to write, when the terminal releases one.
        lease: Option<RunLeaseEntry>,
    },
}

/// Identify whether one newly-appended record needs an authority decision.
/// Idempotent duplicates must be removed before calling this: the accepted
/// winner already mutated the index.
pub fn authority_probe(record: &Record) -> EngineResult<Option<AuthorityProbe>> {
    if has_activation_marker(record) {
        return activation_probe(record).map(Some);
    }

    // Every lease-backed assignment becomes the lease entry later fences
    // are decided against, and the run gate prevents it from landing after
    // a terminal transition. Pull assignments additionally carry a dispatch
    // generation whose idempotency key elects one assignment per generation.
    if record.event_type == "run.assigned" {
        let strict = record
            .metadata
            .get(RUN_META_DISPATCH_GENERATION)
            .is_some_and(|generation| !generation.is_empty());
        return match assignment_lease(record)? {
            Some(lease) => Ok(Some(AuthorityProbe::RunAssignment(RunAssignmentProbe {
                project_id: record.project_id.clone(),
                run_id: record.run_id.clone(),
                lease,
            }))),
            None if strict => Err(EngineError::InvalidArgument(
                "lease-backed run.assigned requires non-empty metadata[lease_id]".to_string(),
            )),
            None => Ok(None),
        };
    }

    let fence = lease_fence(record)?;
    let lease_probe = |fence: LeaseFence, transition: LeaseTransition| {
        Ok(Some(AuthorityProbe::RunLease(RunLeaseProbe {
            project_id: record.project_id.clone(),
            run_id: record.run_id.clone(),
            fence,
            transition,
        })))
    };
    match (record.event_type.as_str(), fence) {
        (RUN_LEASE_RENEWED_EVENT, Some(fence)) => {
            let expires_at_ms = renewed_lease_expiry(record).ok_or_else(|| {
                EngineError::InvalidArgument(format!(
                    "{RUN_LEASE_RENEWED_EVENT} requires metadata[lease_expires_at_ms]"
                ))
            })?;
            lease_probe(fence, LeaseTransition::Renewal { expires_at_ms })
        }
        (RUN_LEASE_EXPIRED_EVENT, Some(fence)) => lease_probe(
            fence,
            LeaseTransition::Expiry {
                record_ts_ms: record.timestamp_ns / 1_000_000,
            },
        ),
        ("run.paused" | "workflow.paused", Some(fence)) => {
            lease_probe(fence, LeaseTransition::Release)
        }
        ("run.failed", Some(fence)) if !terminal_failure(record) => {
            lease_probe(fence, LeaseTransition::Release)
        }
        (RUN_LEASE_RENEWED_EVENT | RUN_LEASE_EXPIRED_EVENT, None) => Ok(None),
        (_, fence) => {
            let Some((requires_children_settled, closes_gate)) = run_terminal_semantics(record)
            else {
                return Ok(None);
            };
            if !closes_gate {
                return Ok(None);
            }
            Ok(Some(AuthorityProbe::RunTerminal(RunTerminalProbe {
                project_id: record.project_id.clone(),
                run_id: record.run_id.clone(),
                requires_children_settled,
                record_ts_ms: record.timestamp_ns / 1_000_000,
                fence,
            })))
        }
    }
}

/// The lease an assignment grants, or `None` for an unleased assignment.
/// A partially specified lease is a contract violation.
fn assignment_lease(record: &Record) -> EngineResult<Option<RunLeaseEntry>> {
    let field = |key: &str| {
        record
            .metadata
            .get(key)
            .filter(|value| !value.is_empty())
            .cloned()
    };
    let missing = |key: &str| {
        EngineError::InvalidArgument(format!(
            "lease-backed run.assigned requires non-empty metadata[{key}]"
        ))
    };
    let Some(lease_id) = field("lease_id") else {
        return Ok(None);
    };
    let worker_id = field("lease_worker_id").ok_or_else(|| missing("lease_worker_id"))?;
    let attempt = field(RUN_META_LEASE_ATTEMPT)
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| missing(RUN_META_LEASE_ATTEMPT))?;
    let expires_at_ms = field("lease_expires_at_ms")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| missing("lease_expires_at_ms"))?;
    Ok(Some(RunLeaseEntry {
        lease_id,
        attempt,
        worker_id,
        session_hash: field(RUN_META_LEASE_SESSION_HASH).unwrap_or_default(),
        expires_at_ms,
        dispatch_generation: field(RUN_META_DISPATCH_GENERATION).unwrap_or_default(),
        grant_id: field(crate::lease_fence::RUN_META_GRANT_ID).unwrap_or_default(),
        released: false,
        release_reason: String::new(),
        accepted_offset: None,
        updated_offset: None,
        lease_timeout_ms: field("lease_timeout_ms")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or_default(),
        grant_request_fingerprint: field(crate::lease_fence::RUN_META_GRANT_REQUEST_FINGERPRINT)
            .unwrap_or_default(),
    }))
}

fn activation_probe(record: &Record) -> EngineResult<AuthorityProbe> {
    let canonical = from_engine_record(record)
        .map_err(EngineError::from)?
        .ok_or_else(|| EngineError::Internal("activation record failed to decode".into()))?;
    let event = canonical.event.as_ref().ok_or_else(|| {
        EngineError::Activation(ActivationError::InvalidArgument(
            "activation journal event is required".to_string(),
        ))
    })?;
    let (
        project_id,
        run_id,
        activation_id,
        parent_activation_id,
        attempt,
        is_started,
        is_snapshot,
        kind,
    ) = match event {
        Event::Started(value) => (
            value.project_id.clone(),
            value.run_id.clone(),
            value.activation_id.clone(),
            value.parent_activation_id.clone(),
            value.attempt,
            true,
            false,
            value.kind,
        ),
        Event::Completed(value) => (
            value.project_id.clone(),
            value.run_id.clone(),
            value.activation_id.clone(),
            String::new(),
            value.attempt,
            false,
            false,
            ActivationKind::Unspecified as i32,
        ),
        Event::Failed(value) => (
            value.project_id.clone(),
            value.run_id.clone(),
            value.activation_id.clone(),
            String::new(),
            value.attempt,
            false,
            false,
            ActivationKind::Unspecified as i32,
        ),
        Event::Suspended(value) => (
            value.project_id.clone(),
            value.run_id.clone(),
            value.activation_id.clone(),
            String::new(),
            value.attempt,
            false,
            false,
            ActivationKind::Unspecified as i32,
        ),
        Event::Cancelled(value) => (
            value.project_id.clone(),
            value.run_id.clone(),
            value.activation_id.clone(),
            String::new(),
            value.attempt,
            false,
            false,
            ActivationKind::Unspecified as i32,
        ),
        Event::UnknownOutcome(value) => {
            let failure = value.failure.as_ref().ok_or_else(|| {
                EngineError::Activation(ActivationError::InvalidArgument(
                    "unknown-outcome record is missing failure".to_string(),
                ))
            })?;
            (
                failure.project_id.clone(),
                failure.run_id.clone(),
                failure.activation_id.clone(),
                String::new(),
                failure.attempt,
                false,
                false,
                ActivationKind::Unspecified as i32,
            )
        }
        Event::Snapshot(value) => {
            let current = value.current_attempt.as_ref().ok_or_else(|| {
                EngineError::Activation(ActivationError::InvalidArgument(
                    "activation snapshot is missing current_attempt".to_string(),
                ))
            })?;
            (
                value.project_id.clone(),
                value.run_id.clone(),
                value.activation_id.clone(),
                current.parent_activation_id.clone(),
                current.attempt,
                false,
                true,
                current.kind,
            )
        }
    };
    let requires_children_settled = matches!(event, Event::Completed(_));
    Ok(AuthorityProbe::Activation(ActivationProbe {
        canonical,
        project_id,
        run_id,
        activation_id,
        requires_parent_active: is_started
            && kind == ActivationKind::Child as i32
            && !parent_activation_id.is_empty(),
        parent_activation_id,
        attempt,
        is_started,
        is_snapshot,
        requires_children_settled,
        lease_guard: ActivationLeaseGuard::from_record(record),
    }))
}

pub fn decide_authority(
    probe: &AuthorityProbe,
    accepted_offset: Offset,
    context: AuthorityContext,
    mode: AuthorityMode,
) -> EngineResult<AuthorityDecision> {
    match probe {
        AuthorityProbe::Activation(probe) => {
            decide_activation(probe, accepted_offset, context, mode)
        }
        AuthorityProbe::RunAssignment(probe) => {
            // Accepted legacy history may contain an assignment after a terminal.
            // Restore its lease fold without reopening the terminal gate; only
            // a new live assignment is rejected against today's lifecycle fence.
            if context.run_terminal_offset.is_some() && mode == AuthorityMode::Live {
                return Err(EngineError::InvalidArgument(
                    "run.assigned rejected because the run lifecycle gate is closed".into(),
                ));
            }
            if mode == AuthorityMode::Live
                && context.lease.as_ref().is_some_and(|current| {
                    !current.released
                        && current.dispatch_generation != probe.lease.dispatch_generation
                })
            {
                let current = context.lease.as_ref().expect("active lease checked above");
                return Err(EngineError::LeaseFenceMismatch {
                    run_id: probe.run_id.clone(),
                    reason: format!(
                        "dispatch generation {} cannot replace active generation {}",
                        probe.lease.dispatch_generation, current.dispatch_generation
                    ),
                });
            }
            // The per-generation idempotency slot resolves pull retries before
            // this fold. A new generation may replace only a released tombstone;
            // TrustedHistory always folds the leader-accepted sequence.
            let mut lease = probe.lease.clone();
            lease.accepted_offset = Some(accepted_offset.0);
            lease.updated_offset = Some(accepted_offset.0);
            Ok(AuthorityDecision::RunAssignment {
                project_id: probe.project_id.clone(),
                run_id: probe.run_id.clone(),
                lease,
            })
        }
        AuthorityProbe::RunLease(probe) => {
            if context.run_terminal_offset.is_some() && mode == AuthorityMode::Live {
                return Err(EngineError::LeaseFenceMismatch {
                    run_id: probe.run_id.clone(),
                    reason: "run is already terminal".to_string(),
                });
            }
            let mut lease = decide_lease_transition(probe, context.lease, mode)?;
            lease.updated_offset = Some(accepted_offset.0);
            Ok(AuthorityDecision::RunLease {
                project_id: probe.project_id.clone(),
                run_id: probe.run_id.clone(),
                lease,
            })
        }
        AuthorityProbe::RunTerminal(probe) => {
            if probe.requires_children_settled {
                reject_unsettled_child(
                    &probe.run_id,
                    context.first_unsettled_required_child.as_deref(),
                )?;
            }
            let already_terminal = context.run_terminal_offset.is_some();
            let lease = match &probe.fence {
                Some(fence) => {
                    if already_terminal && mode == AuthorityMode::Live {
                        return Err(EngineError::LeaseFenceMismatch {
                            run_id: probe.run_id.clone(),
                            reason: "run is already terminal".to_string(),
                        });
                    }
                    let matched = check_fence(&probe.run_id, context.lease.as_ref(), fence, mode)?;
                    if let Some(entry) = &matched {
                        reject_early_timeout(
                            &probe.run_id,
                            fence,
                            entry,
                            probe.record_ts_ms,
                            mode,
                        )?;
                    }
                    matched.map(|entry| entry.released(fence.kind.as_str()))
                }
                // Unfenced terminals (control-plane cancel, terminal
                // supervisor outcomes) keep gate-only semantics and release
                // whatever lease is active so a late worker record loses.
                None => context
                    .lease
                    .filter(|entry| !entry.released)
                    .map(|entry| entry.released("terminal")),
            };
            let lease = lease.map(|mut lease| {
                lease.updated_offset = Some(accepted_offset.0);
                lease
            });
            Ok(AuthorityDecision::RunTerminal {
                project_id: probe.project_id.clone(),
                run_id: probe.run_id.clone(),
                accepted_offset,
                already_terminal,
                lease,
            })
        }
    }
}

fn decide_activation(
    probe: &ActivationProbe,
    accepted_offset: Offset,
    context: AuthorityContext,
    mode: AuthorityMode,
) -> EngineResult<AuthorityDecision> {
    // Exact idempotent duplicates are removed before authority evaluation.
    // Once the run gate is closed, every other activation transition is
    // stale: completions, failures, suspensions, cancellations, and snapshots
    // can carry state/outbox mutations just as a newly-started activation can.
    //
    // A guarded write is stale the same way once its lease is released or
    // replaced. An unindexed run has no entry to compare and keeps gate-only
    // semantics, and accepted history always folds.
    let lease_lost = mode == AuthorityMode::Live
        && probe.lease_guard.as_ref().is_some_and(|guard| {
            context
                .lease
                .as_ref()
                .is_some_and(|entry| !guard.matches(entry))
        });
    if context.run_terminal_offset.is_some() || lease_lost {
        return Err(ActivationError::StaleAuthority {
            activation_id: probe.activation_id.clone(),
            request_attempt: probe.attempt,
            current_attempt: context
                .current
                .as_ref()
                .map(|state| state.current_attempt.attempt)
                .unwrap_or(probe.attempt),
        }
        .into());
    }
    if probe.requires_parent_active
        && context
            .parent
            .as_ref()
            .is_none_or(|parent| parent.status != ActivationStatus::Active)
    {
        return Err(ActivationError::IllegalTransition {
            activation_id: probe.activation_id.clone(),
            reason: format!(
                "parent activation {} is not active",
                probe.parent_activation_id
            ),
        }
        .into());
    }
    if probe.requires_children_settled {
        reject_unsettled_child(
            &probe.activation_id,
            context.first_unsettled_required_child.as_deref(),
        )?;
    }

    let prior = context.current;
    let was_obligation = prior
        .as_ref()
        .is_some_and(|state| is_unsettled_required_child(state, None));
    let state = apply_journal_record(prior.clone(), &probe.canonical, accepted_offset)
        .map_err(EngineError::from)?;
    let is_obligation = is_unsettled_required_child(&state, None);
    let obligation = if is_obligation {
        ObligationMutation::Upsert {
            project_id: state.identity.project_id.clone(),
            run_id: state.identity.run_id.clone(),
            parent_activation_id: state.identity.parent_activation_id.clone(),
            child_activation_id: state.identity.activation_id.clone(),
        }
    } else if was_obligation {
        let prior = prior.expect("was_obligation requires prior state");
        ObligationMutation::Delete {
            project_id: prior.identity.project_id,
            run_id: prior.identity.run_id,
            parent_activation_id: prior.identity.parent_activation_id,
            child_activation_id: prior.identity.activation_id,
        }
    } else {
        ObligationMutation::None
    };
    Ok(AuthorityDecision::Activation { state, obligation })
}

fn decide_lease_transition(
    probe: &RunLeaseProbe,
    entry: Option<RunLeaseEntry>,
    mode: AuthorityMode,
) -> EngineResult<RunLeaseEntry> {
    let fence = &probe.fence;
    let matched = check_fence(&probe.run_id, entry.as_ref(), fence, mode)?;
    match &probe.transition {
        LeaseTransition::Renewal { expires_at_ms } => {
            let mut lease = matched.unwrap_or_else(|| RunLeaseEntry {
                lease_id: fence.lease_id.clone(),
                attempt: fence.attempt,
                worker_id: fence.worker_id.clone(),
                session_hash: fence.session_hash.clone(),
                ..Default::default()
            });
            if *expires_at_ms <= lease.expires_at_ms && mode == AuthorityMode::Live {
                return Err(EngineError::LeaseFenceMismatch {
                    run_id: probe.run_id.clone(),
                    reason: format!(
                        "lease renewal to {expires_at_ms} does not extend the current expiry {}",
                        lease.expires_at_ms
                    ),
                });
            }
            lease.expires_at_ms = (*expires_at_ms).max(lease.expires_at_ms);
            lease.released = false;
            lease.release_reason.clear();
            Ok(lease)
        }
        LeaseTransition::Expiry { record_ts_ms } => {
            if let Some(lease) = &matched {
                reject_early_timeout(&probe.run_id, fence, lease, *record_ts_ms, mode)?;
            }
            let reason = fence
                .expiry_reason
                .map(LeaseExpiryReason::as_str)
                .unwrap_or("expired");
            Ok(matched
                .unwrap_or_else(|| entry_from_fence(fence))
                .released(reason))
        }
        LeaseTransition::Release => Ok(matched
            .unwrap_or_else(|| entry_from_fence(fence))
            .released(fence.kind.as_str())),
    }
}

/// A timeout expiry (requeue or dead-letter) must not land before the
/// entry's expiry: the entry is the fold of every accepted renewal, and a
/// sweeper reading a stale projection would otherwise kill a live lease.
fn reject_early_timeout(
    run_id: &str,
    fence: &LeaseFence,
    entry: &RunLeaseEntry,
    record_ts_ms: i64,
    mode: AuthorityMode,
) -> EngineResult<()> {
    if fence.expiry_reason == Some(LeaseExpiryReason::Timeout)
        && record_ts_ms < entry.expires_at_ms
        && mode == AuthorityMode::Live
    {
        return Err(EngineError::LeaseFenceMismatch {
            run_id: run_id.to_string(),
            reason: format!(
                "lease expiry at {record_ts_ms} precedes the current expiry {}",
                entry.expires_at_ms
            ),
        });
    }
    Ok(())
}

fn entry_from_fence(fence: &LeaseFence) -> RunLeaseEntry {
    RunLeaseEntry {
        lease_id: fence.lease_id.clone(),
        attempt: fence.attempt,
        worker_id: fence.worker_id.clone(),
        session_hash: fence.session_hash.clone(),
        ..Default::default()
    }
}

/// Compare a fence against the indexed entry.
///
/// Returns the matched entry, or `None` when the run is unindexed and the
/// gate alone decides. In `Live` mode a released or different lease is a
/// `LeaseFenceMismatch`; in `TrustedHistory` the fold proceeds as if the
/// fence matched, because the record was already accepted by the journal.
fn check_fence(
    run_id: &str,
    entry: Option<&RunLeaseEntry>,
    fence: &LeaseFence,
    mode: AuthorityMode,
) -> EngineResult<Option<RunLeaseEntry>> {
    let Some(entry) = entry else {
        return Ok(None);
    };
    let mismatch = if entry.released {
        Some(format!(
            "lease {} attempt {} was released ({})",
            entry.lease_id, entry.attempt, entry.release_reason
        ))
    } else if entry.lease_id != fence.lease_id {
        Some(format!(
            "lease {} is not the current lease {}",
            fence.lease_id, entry.lease_id
        ))
    } else if entry.attempt != fence.attempt {
        Some(format!(
            "attempt {} is not the current attempt {}",
            fence.attempt, entry.attempt
        ))
    } else if entry.worker_id != fence.worker_id {
        Some(format!(
            "worker {} does not own lease {}",
            fence.worker_id, entry.lease_id
        ))
    } else if !fence.session_hash.is_empty() && entry.session_hash != fence.session_hash {
        Some(format!(
            "worker session does not own lease {}",
            entry.lease_id
        ))
    } else {
        None
    };
    match (mismatch, mode) {
        (Some(reason), AuthorityMode::Live) => Err(EngineError::LeaseFenceMismatch {
            run_id: run_id.to_string(),
            reason,
        }),
        (Some(_), AuthorityMode::TrustedHistory) => Ok(Some(entry_from_fence(fence))),
        (None, _) => Ok(Some(entry.clone())),
    }
}

fn reject_unsettled_child(parent: &str, child: Option<&str>) -> EngineResult<()> {
    if let Some(child) = child {
        return Err(ActivationError::RequiredChildUnresolved {
            activation_id: parent.to_string(),
            child_activation_id: child.to_string(),
        }
        .into());
    }
    Ok(())
}

fn run_terminal_semantics(record: &Record) -> Option<(bool, bool)> {
    match record.event_type.as_str() {
        "run.completed" => Some((true, true)),
        "run.cancelled" | "run.terminalizing" => Some((false, true)),
        "run.failed" => Some((false, terminal_failure(record))),
        _ => None,
    }
}

fn terminal_failure(record: &Record) -> bool {
    if record
        .metadata
        .get("workerless_retryable")
        .is_some_and(|value| value.eq_ignore_ascii_case("false"))
    {
        return true;
    }
    let attempt = record
        .metadata
        .get("attempt")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let max_attempts = record
        .metadata
        .get("max_attempts")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(1);
    attempt.saturating_add(1) >= max_attempts
}

/// Authority heads persist the aggregate as its canonical JSON snapshot.
pub fn encode_activation_state(state: &ActivationState) -> Vec<u8> {
    activation::encode_journal_record(&activation::snapshot_record(state))
}

pub fn decode_activation_state(bytes: &[u8]) -> EngineResult<ActivationState> {
    let record = activation::decode_journal_record(bytes).map_err(EngineError::from)?;
    match record.event {
        Some(Event::Snapshot(snapshot)) => {
            state_from_snapshot(&snapshot).map_err(EngineError::from)
        }
        _ => Err(EngineError::Storage(
            "activation authority head is not a snapshot record".to_string(),
        )),
    }
}

pub fn activation_head_key(project_id: &str, run_id: &str, activation_id: &str) -> Vec<u8> {
    composite_key(HEAD_PREFIX, &[project_id, run_id, activation_id])
}

pub fn activation_head_run_prefix(project_id: &str, run_id: &str) -> Vec<u8> {
    composite_key(HEAD_PREFIX, &[project_id, run_id])
}

pub fn required_child_obligation_key(
    project_id: &str,
    run_id: &str,
    parent_activation_id: &str,
    child_activation_id: &str,
) -> Vec<u8> {
    composite_key(
        OBLIGATION_PREFIX,
        &[
            project_id,
            run_id,
            parent_activation_id,
            child_activation_id,
        ],
    )
}

pub fn required_child_obligation_prefix(
    project_id: &str,
    run_id: &str,
    parent_activation_id: Option<&str>,
) -> Vec<u8> {
    let mut fields = vec![project_id, run_id];
    if let Some(parent) = parent_activation_id {
        fields.push(parent);
    }
    composite_key(OBLIGATION_PREFIX, &fields)
}

pub fn run_gate_key(project_id: &str, run_id: &str) -> Vec<u8> {
    composite_key(RUN_GATE_PREFIX, &[project_id, run_id])
}

pub fn run_lease_key(project_id: &str, run_id: &str) -> Vec<u8> {
    composite_key(RUN_LEASE_PREFIX, &[project_id, run_id])
}

pub fn encode_gate_offset(offset: Offset) -> [u8; 8] {
    offset.0.to_be_bytes()
}

pub fn decode_gate_offset(bytes: &[u8]) -> EngineResult<Offset> {
    let value: [u8; 8] = bytes
        .try_into()
        .map_err(|_| EngineError::Storage("invalid activation run gate offset".into()))?;
    Ok(Offset(u64::from_be_bytes(value)))
}

fn composite_key(prefix: &[u8], fields: &[&str]) -> Vec<u8> {
    let capacity = prefix.len() + fields.iter().map(|value| value.len() + 4).sum::<usize>();
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(prefix);
    for value in fields {
        let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(value.as_bytes());
    }
    key
}

/// Compatibility recovery used only by Segment implementations that have not
/// supplied an indexed read path. Production hot segments override the query
/// methods; this keeps small test doubles and read-only segments correct.
pub async fn replay_run_activation_states<S: Segment + ?Sized>(
    segment: &S,
    project_id: &str,
    run_id: &str,
) -> EngineResult<HashMap<String, ActivationState>> {
    let partition = partition_for_run(run_id, segment.partition_count());
    let floor = segment.partition_retained_floor(partition).await?;
    let tail = segment.partition_tail_offset(partition).await?;
    let records = segment.read_partition_range(partition, floor, tail).await?;
    let mut ordered = BTreeMap::new();
    for record in records {
        ordered.insert(record.offset.0, record);
    }
    let mut states = HashMap::new();
    for record in ordered.into_values() {
        if record.project_id != project_id || record.run_id != run_id {
            continue;
        }
        let Some((activation_id, _, _)) = activation::activation_record_identity(&record) else {
            continue;
        };
        let activation_id = activation_id.to_string();
        let canonical = from_engine_record(&record)
            .map_err(EngineError::from)?
            .ok_or_else(|| EngineError::Internal("activation record failed to decode".into()))?;
        let current = states.remove(&activation_id);
        let next =
            apply_journal_record(current, &canonical, record.offset).map_err(EngineError::from)?;
        states.insert(activation_id, next);
    }
    Ok(states)
}

pub fn nonterminal_activation(state: &ActivationState) -> bool {
    matches!(
        state.status,
        ActivationStatus::Active | ActivationStatus::RetryReady | ActivationStatus::Suspended
    )
}

pub fn required_child(state: &ActivationState) -> bool {
    state.identity.kind == ActivationKind::Child as i32
        && state.identity.child.as_ref().is_some_and(|child| {
            ChildJoinPolicy::try_from(child.join_policy).unwrap_or(ChildJoinPolicy::Unspecified)
                == ChildJoinPolicy::Required
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease_fence::{worker_session_hash, LeaseFenceKind};
    use bytes::Bytes;
    use std::collections::HashMap;

    fn record(event_type: &str, metadata: &[(&str, &str)]) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "p".to_string(),
            run_id: "r".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::new(),
            timestamp_ns: 5_000 * 1_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: metadata
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn session() -> String {
        worker_session_hash("session-1")
    }

    fn assigned() -> Record {
        let session = session();
        record(
            "run.assigned",
            &[
                ("lease_id", "lease-1"),
                ("lease_worker_id", "worker-1"),
                (RUN_META_LEASE_ATTEMPT, "0"),
                ("lease_expires_at_ms", "10000"),
                (RUN_META_LEASE_SESSION_HASH, session.as_str()),
                (RUN_META_DISPATCH_GENERATION, "gen-1"),
            ],
        )
    }

    fn fence(kind: LeaseFenceKind) -> LeaseFence {
        LeaseFence {
            kind,
            lease_id: "lease-1".to_string(),
            worker_id: "worker-1".to_string(),
            session_hash: session(),
            attempt: 0,
            received_at_ms: 6_000,
            expiry_reason: (kind == LeaseFenceKind::LeaseExpiry)
                .then_some(LeaseExpiryReason::Timeout),
        }
    }

    fn fenced(event_type: &str, fence: &LeaseFence, extra: &[(&str, &str)]) -> Record {
        let mut record = record(event_type, extra);
        fence.stamp(&mut record);
        record
    }

    fn entry() -> RunLeaseEntry {
        match authority_probe(&assigned()).unwrap().unwrap() {
            AuthorityProbe::RunAssignment(probe) => probe.lease,
            other => panic!("unexpected probe {other:?}"),
        }
    }

    fn decide(
        record: &Record,
        lease: Option<RunLeaseEntry>,
        mode: AuthorityMode,
    ) -> EngineResult<AuthorityDecision> {
        let probe = authority_probe(record).unwrap().expect("probe");
        decide_authority(
            &probe,
            Offset(7),
            AuthorityContext {
                lease,
                ..Default::default()
            },
            mode,
        )
    }

    fn terminal_lease(decision: AuthorityDecision) -> Option<RunLeaseEntry> {
        match decision {
            AuthorityDecision::RunTerminal { lease, .. } => lease,
            AuthorityDecision::RunLease { lease, .. } => Some(lease),
            other => panic!("unexpected decision {other:?}"),
        }
    }

    #[test]
    fn assignment_builds_the_lease_entry() {
        let entry = entry();
        assert_eq!(entry.lease_id, "lease-1");
        assert_eq!(entry.expires_at_ms, 10_000);
        assert!(!entry.released);
    }

    #[test]
    fn assignment_without_lease_fields_is_rejected() {
        let record = record("run.assigned", &[(RUN_META_DISPATCH_GENERATION, "gen-1")]);
        assert!(authority_probe(&record).is_err());
    }

    #[test]
    fn active_lease_rejects_another_generation_only_in_live_mode() {
        let mut reassigned = assigned();
        reassigned.metadata.insert(
            RUN_META_DISPATCH_GENERATION.to_string(),
            "gen-2".to_string(),
        );
        reassigned
            .metadata
            .insert("lease_id".to_string(), "lease-2".to_string());

        assert!(matches!(
            decide(&reassigned, Some(entry()), AuthorityMode::Live),
            Err(EngineError::LeaseFenceMismatch { .. })
        ));

        match decide(&reassigned, Some(entry()), AuthorityMode::TrustedHistory).unwrap() {
            AuthorityDecision::RunAssignment { lease, .. } => {
                assert_eq!(lease.lease_id, "lease-2");
                assert_eq!(lease.dispatch_generation, "gen-2");
            }
            other => panic!("unexpected {other:?}"),
        }

        assert!(decide(
            &reassigned,
            Some(entry().released("pause")),
            AuthorityMode::Live,
        )
        .is_ok());
    }

    #[test]
    fn fenced_completion_requires_the_active_lease() {
        let completed = fenced("run.completed", &fence(LeaseFenceKind::PullCompletion), &[]);
        let released =
            terminal_lease(decide(&completed, Some(entry()), AuthorityMode::Live).unwrap())
                .expect("lease released");
        assert!(released.released);
        assert_eq!(released.release_reason, "pull_completion");

        let mut stale = fence(LeaseFenceKind::PullCompletion);
        stale.lease_id = "lease-0".to_string();
        let stale = fenced("run.completed", &stale, &[]);
        assert!(matches!(
            decide(&stale, Some(entry()), AuthorityMode::Live),
            Err(EngineError::LeaseFenceMismatch { .. })
        ));

        let mut other_session = fence(LeaseFenceKind::PullCompletion);
        other_session.session_hash = worker_session_hash("session-2");
        let other_session = fenced("run.completed", &other_session, &[]);
        assert!(decide(&other_session, Some(entry()), AuthorityMode::Live).is_err());

        let mut push = fence(LeaseFenceKind::PushCompletion);
        push.session_hash.clear();
        let push = fenced("run.completed", &push, &[]);
        assert!(decide(&push, Some(entry()), AuthorityMode::Live).is_ok());
    }

    #[test]
    fn released_entry_rejects_and_absent_entry_accepts_on_the_gate() {
        let completed = fenced("run.completed", &fence(LeaseFenceKind::PullCompletion), &[]);
        assert!(decide(
            &completed,
            Some(entry().released("pause")),
            AuthorityMode::Live
        )
        .is_err());
        let decision = decide(&completed, None, AuthorityMode::Live).unwrap();
        assert!(terminal_lease(decision).is_none());
    }

    #[test]
    fn fenced_terminal_against_closed_gate_is_rejected_live_and_folded_trusted() {
        let completed = fenced("run.completed", &fence(LeaseFenceKind::PullCompletion), &[]);
        let probe = authority_probe(&completed).unwrap().unwrap();
        let context = || AuthorityContext {
            run_terminal_offset: Some(Offset(3)),
            lease: Some(entry()),
            ..Default::default()
        };
        assert!(matches!(
            decide_authority(&probe, Offset(7), context(), AuthorityMode::Live),
            Err(EngineError::LeaseFenceMismatch { .. })
        ));
        match decide_authority(&probe, Offset(7), context(), AuthorityMode::TrustedHistory).unwrap()
        {
            AuthorityDecision::RunTerminal {
                already_terminal,
                lease,
                ..
            } => {
                assert!(already_terminal);
                assert!(lease.unwrap().released);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn unfenced_terminal_keeps_gate_semantics_and_releases_the_lease() {
        let cancelled = record("run.cancelled", &[]);
        let released =
            terminal_lease(decide(&cancelled, Some(entry()), AuthorityMode::Live).unwrap());
        assert_eq!(released.unwrap().release_reason, "terminal");
        let probe = authority_probe(&cancelled).unwrap().unwrap();
        match decide_authority(
            &probe,
            Offset(7),
            AuthorityContext {
                run_terminal_offset: Some(Offset(3)),
                lease: Some(entry().released("pull_completion")),
                ..Default::default()
            },
            AuthorityMode::Live,
        )
        .unwrap()
        {
            AuthorityDecision::RunTerminal {
                already_terminal,
                lease,
                ..
            } => {
                assert!(already_terminal);
                assert!(lease.is_none(), "an already-released entry is left alone");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn pause_releases_without_closing_the_gate_and_reassignment_follows() {
        let paused = fenced(
            "workflow.paused",
            &fence(LeaseFenceKind::PullCompletion),
            &[],
        );
        let released =
            terminal_lease(decide(&paused, Some(entry()), AuthorityMode::Live).unwrap()).unwrap();
        assert!(released.released);

        let late = fenced("run.completed", &fence(LeaseFenceKind::PullCompletion), &[]);
        assert!(decide(&late, Some(released.clone()), AuthorityMode::Live).is_err());

        let mut reassigned = assigned();
        reassigned
            .metadata
            .insert("lease_id".to_string(), "lease-2".to_string());
        match decide(&reassigned, Some(released), AuthorityMode::Live).unwrap() {
            AuthorityDecision::RunAssignment { lease, .. } => {
                assert_eq!(lease.lease_id, "lease-2");
                assert!(!lease.released);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn retryable_failure_releases_and_terminal_failure_closes() {
        let retryable = fenced(
            "run.failed",
            &fence(LeaseFenceKind::PullCompletion),
            &[("attempt", "0"), ("max_attempts", "3")],
        );
        assert!(matches!(
            authority_probe(&retryable).unwrap(),
            Some(AuthorityProbe::RunLease(RunLeaseProbe {
                transition: LeaseTransition::Release,
                ..
            }))
        ));
        let terminal = fenced(
            "run.failed",
            &fence(LeaseFenceKind::PullCompletion),
            &[("attempt", "2"), ("max_attempts", "3")],
        );
        assert!(matches!(
            authority_probe(&terminal).unwrap(),
            Some(AuthorityProbe::RunTerminal(_))
        ));
    }

    #[test]
    fn renewal_extends_forward_only_and_upserts_when_unindexed() {
        let renewed = fenced(
            RUN_LEASE_RENEWED_EVENT,
            &fence(LeaseFenceKind::LeaseRenewal),
            &[("lease_expires_at_ms", "20000")],
        );
        let lease =
            terminal_lease(decide(&renewed, Some(entry()), AuthorityMode::Live).unwrap()).unwrap();
        assert_eq!(lease.expires_at_ms, 20_000);
        assert!(!lease.released);

        let shorter = fenced(
            RUN_LEASE_RENEWED_EVENT,
            &fence(LeaseFenceKind::LeaseRenewal),
            &[("lease_expires_at_ms", "9000")],
        );
        assert!(decide(&shorter, Some(entry()), AuthorityMode::Live).is_err());

        let unindexed =
            terminal_lease(decide(&renewed, None, AuthorityMode::Live).unwrap()).unwrap();
        assert_eq!(unindexed.lease_id, "lease-1");
        assert_eq!(unindexed.expires_at_ms, 20_000);

        assert!(decide(
            &renewed,
            Some(entry().released("pause")),
            AuthorityMode::Live
        )
        .is_err());
    }

    #[test]
    fn expiry_respects_the_entry_expiry_for_timeouts_only() {
        let mut early = fenced(
            RUN_LEASE_EXPIRED_EVENT,
            &fence(LeaseFenceKind::LeaseExpiry),
            &[],
        );
        early.timestamp_ns = 9_999 * 1_000_000;
        assert!(decide(&early, Some(entry()), AuthorityMode::Live).is_err());

        let mut due = early.clone();
        due.timestamp_ns = 10_000 * 1_000_000;
        let released =
            terminal_lease(decide(&due, Some(entry()), AuthorityMode::Live).unwrap()).unwrap();
        assert_eq!(released.release_reason, "timeout");

        let mut disconnect = fence(LeaseFenceKind::LeaseExpiry);
        disconnect.expiry_reason = Some(LeaseExpiryReason::WorkerDisconnect);
        let mut disconnect = fenced(RUN_LEASE_EXPIRED_EVENT, &disconnect, &[]);
        disconnect.timestamp_ns = 1;
        assert!(decide(&disconnect, Some(entry()), AuthorityMode::Live).is_ok());

        // A second sweeper loses to the released tombstone.
        assert!(decide(&due, Some(released), AuthorityMode::Live).is_err());
        // Trusted history folds the same record without rejecting.
        assert!(decide(&early, Some(entry()), AuthorityMode::TrustedHistory).is_ok());
    }

    #[test]
    fn unfenced_lease_events_produce_no_probe() {
        assert!(authority_probe(&record(RUN_LEASE_EXPIRED_EVENT, &[]))
            .unwrap()
            .is_none());
        assert!(authority_probe(&record(RUN_LEASE_RENEWED_EVENT, &[]))
            .unwrap()
            .is_none());
        assert!(authority_probe(&record("run.paused", &[]))
            .unwrap()
            .is_none());
    }

    #[test]
    fn guarded_activation_write_needs_the_lease_it_was_validated_against() {
        let guard = ActivationLeaseGuard {
            lease_id: "lease-1".to_string(),
            attempt: 2,
        };
        let matching = RunLeaseEntry {
            lease_id: "lease-1".to_string(),
            attempt: 2,
            ..Default::default()
        };
        assert!(guard.matches(&matching));
        assert!(!guard.matches(&matching.clone().released("pull_completion")));
        assert!(!guard.matches(&RunLeaseEntry {
            lease_id: "lease-2".to_string(),
            ..matching.clone()
        }));
        assert!(!guard.matches(&RunLeaseEntry {
            attempt: 3,
            ..matching
        }));
    }

    #[test]
    fn lease_entry_round_trips() {
        let entry = entry().released("pull_completion");
        let decoded = decode_run_lease_entry(&encode_run_lease_entry(&entry)).unwrap();
        assert_eq!(decoded, entry);
        let _ = HashMap::<Vec<u8>, RunLeaseEntry>::from([(run_lease_key("p", "r"), decoded)]);
    }

    #[test]
    fn push_assignment_without_generation_still_installs_the_lease_entry() {
        let mut push = assigned();
        push.metadata.remove(RUN_META_DISPATCH_GENERATION);
        match decide(&push, Some(entry().released("pause")), AuthorityMode::Live).unwrap() {
            AuthorityDecision::RunAssignment { lease, .. } => {
                assert_eq!(lease.lease_id, "lease-1");
                assert!(!lease.released);
                assert!(lease.dispatch_generation.is_empty());
            }
            other => panic!("unexpected {other:?}"),
        }
        let mut partial = push.clone();
        partial.metadata.remove("lease_worker_id");
        assert!(authority_probe(&partial).is_err());
        let mut unleased = push;
        unleased.metadata.remove("lease_id");
        assert!(authority_probe(&unleased).unwrap().is_none());
    }

    #[test]
    fn fenced_dead_letter_failure_respects_the_renewed_expiry() {
        let mut dead_letter = fenced(
            "run.failed",
            &fence(LeaseFenceKind::LeaseExpiry),
            &[("attempt", "2"), ("max_attempts", "3")],
        );
        dead_letter.timestamp_ns = 9_999 * 1_000_000;
        assert!(matches!(
            decide(&dead_letter, Some(entry()), AuthorityMode::Live),
            Err(EngineError::LeaseFenceMismatch { .. })
        ));
        dead_letter.timestamp_ns = 10_000 * 1_000_000;
        let lease =
            terminal_lease(decide(&dead_letter, Some(entry()), AuthorityMode::Live).unwrap());
        assert!(lease.unwrap().released);
    }

    #[test]
    fn lease_transitions_lose_to_a_closed_gate() {
        let renewed = fenced(
            RUN_LEASE_RENEWED_EVENT,
            &fence(LeaseFenceKind::LeaseRenewal),
            &[("lease_expires_at_ms", "20000")],
        );
        let probe = authority_probe(&renewed).unwrap().unwrap();
        let context = || AuthorityContext {
            run_terminal_offset: Some(Offset(3)),
            lease: None,
            ..Default::default()
        };
        assert!(matches!(
            decide_authority(&probe, Offset(7), context(), AuthorityMode::Live),
            Err(EngineError::LeaseFenceMismatch { .. })
        ));
        assert!(
            decide_authority(&probe, Offset(7), context(), AuthorityMode::TrustedHistory).is_ok()
        );
    }

    #[test]
    fn workerless_non_retryable_failure_is_terminal() {
        let failed = fenced(
            "run.failed",
            &fence(LeaseFenceKind::PullCompletion),
            &[
                ("attempt", "0"),
                ("max_attempts", "3"),
                ("workerless_retryable", "false"),
            ],
        );
        assert!(matches!(
            authority_probe(&failed).unwrap(),
            Some(AuthorityProbe::RunTerminal(_))
        ));
    }
}
