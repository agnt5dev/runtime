//! Durable Activation journal aggregate.
//!
//! The functions in this module are pure: callers load journal-owned state,
//! decide one command, append the returned canonical record, and then apply
//! that accepted record. Projections may cache the resulting state, but must
//! never participate in a correctness decision.
//!
//! Records are journaled under the boundary the user sees (`workflow.step.*`,
//! `lm.*`, `tool_call.*`, `agent.*`) with a canonical JSON payload; see
//! `codec` for the envelope and `payload` for the shapes.

mod codec;
mod payload;

use std::collections::HashSet;

use base64::Engine as _;
use engine_proto::activation_payload::Value;
use engine_proto::{
    ActivationEvidence, ActivationExternalOutcomeCertainty, ActivationKind, ActivationOutboxIntent,
    ActivationPayload, ActivationRecoveryPolicy, ActivationStateMutation, ActivationStatus,
    BeginActivationOutcome, BeginActivationRequest, ChildActivationLinkage, ChildJoinPolicy,
    CompleteActivationRequest, FailActivationRequest, SuspendActivationRequest,
};
use hmac::{Hmac, Mac};
use prost::Message;
use sha2::{Digest, Sha256};

pub use codec::{
    activation_record_identity, all_event_types, decode_journal_record, encode_journal_record,
    event_prefix, event_type, from_engine_record, has_activation_marker, is_activation_record,
    journal_record, public_view, to_engine_record, transition_label, ALL_TRANSITIONS,
    LIVE_ACTIVATION_KINDS, META_ACTIVATION_ATTEMPT, META_ACTIVATION_CONTRACT, META_ACTIVATION_ID,
    META_ACTIVATION_KIND, META_ACTIVATION_TRANSITION, TRANSITION_CANCELLED, TRANSITION_COMPLETED,
    TRANSITION_FAILED, TRANSITION_RETRY_STARTED, TRANSITION_SNAPSHOT, TRANSITION_STARTED,
    TRANSITION_SUSPENDED, TRANSITION_UNKNOWN_OUTCOME,
};
pub use payload::{
    ActivationCancelledRecord, ActivationCompletedRecord, ActivationFailedRecord, ActivationHeader,
    ActivationJournalRecord, ActivationSnapshotRecord, ActivationStartedRecord,
    ActivationSuspendedRecord, ActivationUnknownOutcomeRecord, Event, ACTIVATION_JOURNAL_VERSION,
};

use crate::error::ActivationError;
use crate::types::Offset;

type HmacSha256 = Hmac<Sha256>;

pub const DURABLE_ACTIVATION_V1_CAPABILITY: &str = "durable_activation_v1";
pub const DURABLE_SUSPENSION_V1_CAPABILITY: &str = "durable_suspension_v1";
/// A worker session that negotiated this capability may carry a non-streaming
/// pull job's lifecycle records inside `CompleteJob` instead of appending them
/// one RPC at a time. See `docs/contracts/pull-completion-lifecycle-v1.md`.
pub const PULL_COMPLETION_LIFECYCLE_V1_CAPABILITY: &str = "pull_completion_lifecycle_v1";
/// A worker session that negotiated this capability may treat the optional
/// `PollJobResponse.slot_scaling` field as authoritative slot feedback.
pub const SERVER_SLOT_SCALING_V1_CAPABILITY: &str = "server_slot_scaling_v1";
/// Metadata marker value on every activation journal record.
pub const DURABLE_ACTIVATION_V2_CAPABILITY: &str = "durable_activation_v2";
pub const MAX_ACTIVATION_INLINE_BYTES: usize = 256 * 1024;
pub const MAX_ACTIVATION_INPUT_DATA_BYTES: usize = 64 * 1024;
pub const MAX_ACTIVATION_NAME_BYTES: usize = 256;
pub const MAX_DISPLAY_PARENT_CORRELATION_ID_BYTES: usize = 256;
pub const MAX_ACTIVATION_EVIDENCE_BYTES: usize = 64 * 1024;
pub const MAX_ACTIVATION_EVIDENCE_ITEMS: usize = 32;
pub const MAX_ACTIVATION_STATE_MUTATIONS: usize = 128;
pub const MAX_ACTIVATION_OUTBOX_INTENTS: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub struct ActivationIdentity {
    pub project_id: String,
    pub run_id: String,
    pub parent_activation_id: String,
    pub activation_id: String,
    pub kind: i32,
    pub stable_key: String,
    pub input_digest: Vec<u8>,
    pub definition_digest: Vec<u8>,
    pub recovery_policy: i32,
    pub child: Option<ChildActivationLinkage>,
}

impl ActivationIdentity {
    fn from_begin(request: &BeginActivationRequest) -> Self {
        Self {
            project_id: request.project_id.clone(),
            run_id: request.run_id.clone(),
            parent_activation_id: request.parent_activation_id.clone(),
            activation_id: activation_id(
                &request.project_id,
                &request.run_id,
                &request.parent_activation_id,
                request.kind,
                &request.stable_key,
            ),
            kind: request.kind,
            stable_key: request.stable_key.clone(),
            input_digest: request.input_digest.clone(),
            definition_digest: request.definition_digest.clone(),
            recovery_policy: request.recovery_policy,
            child: request.child.clone(),
        }
    }

    fn from_started(started: &ActivationStartedRecord) -> Self {
        Self {
            project_id: started.project_id.clone(),
            run_id: started.run_id.clone(),
            parent_activation_id: started.parent_activation_id.clone(),
            activation_id: started.activation_id.clone(),
            kind: started.kind,
            stable_key: started.stable_key.clone(),
            input_digest: started.input_digest.clone(),
            definition_digest: started.definition_digest.clone(),
            recovery_policy: started.recovery_policy,
            child: started.child.clone(),
        }
    }

    fn matches_begin(&self, request: &BeginActivationRequest) -> bool {
        self.project_id == request.project_id
            && self.run_id == request.run_id
            && self.parent_activation_id == request.parent_activation_id
            && self.activation_id
                == activation_id(
                    &request.project_id,
                    &request.run_id,
                    &request.parent_activation_id,
                    request.kind,
                    &request.stable_key,
                )
            && self.kind == request.kind
            && self.stable_key == request.stable_key
            && self.input_digest == request.input_digest
            && self.definition_digest == request.definition_digest
            && self.recovery_policy == request.recovery_policy
            && self.child == request.child
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivationState {
    pub identity: ActivationIdentity,
    pub status: ActivationStatus,
    pub current_attempt: ActivationStartedRecord,
    pub completion: Option<ActivationCompletedRecord>,
    pub failure: Option<ActivationFailedRecord>,
    pub suspension: Option<ActivationSuspendedRecord>,
    pub cancellation: Option<ActivationCancelledRecord>,
    /// Offset of the record that established the current decision.
    pub accepted_offset: Offset,
    /// Offset of the started record of the current attempt; settling records
    /// repeat it so a lost-ack retry rebuilds byte-identical bytes.
    pub attempt_started_offset: Offset,
}

/// Return whether a child has a durable terminal or interrupted receipt.
///
/// Retry-ready, active, and suspended children remain obligations. Failed,
/// cancelled, and unknown-outcome children are settled receipts that parent
/// logic may handle without rerunning or silently discarding the outcome.
pub fn child_is_settled(state: &ActivationState) -> bool {
    matches!(
        state.status,
        ActivationStatus::Completed
            | ActivationStatus::Failed
            | ActivationStatus::Cancelled
            | ActivationStatus::UnknownOutcome
    )
}

/// Return whether this state is one unresolved required child of `parent`.
/// `None` matches required children anywhere in the run terminal obligation.
pub fn is_unsettled_required_child(state: &ActivationState, parent: Option<&str>) -> bool {
    state.identity.kind == ActivationKind::Child as i32
        && state.identity.child.as_ref().is_some_and(|child| {
            ChildJoinPolicy::try_from(child.join_policy).unwrap_or(ChildJoinPolicy::Unspecified)
                == ChildJoinPolicy::Required
        })
        && parent.is_none_or(|parent| state.identity.parent_activation_id == parent)
        && !child_is_settled(state)
}

#[derive(Clone, Debug)]
pub struct BeginActivationCommand {
    pub request: BeginActivationRequest,
    /// Opaque authority minted by the partition owner for a possible new attempt.
    pub issued_fence_token: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct CompleteActivationCommand {
    pub request: CompleteActivationRequest,
}

#[derive(Clone, Debug)]
pub struct FailActivationCommand {
    pub request: FailActivationRequest,
}

#[derive(Clone, Debug)]
pub struct SuspendActivationCommand {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub attempt: u32,
    pub fence_token: Vec<u8>,
    pub resume_source: String,
}

#[derive(Clone, Debug)]
pub struct CancelActivationCommand {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BeginDecision {
    pub outcome: BeginActivationOutcome,
    pub activation_id: String,
    pub attempt: u32,
    pub fence_token: Vec<u8>,
    pub accepted_offset: Option<Offset>,
    pub replay_result: Option<ActivationPayload>,
    pub active_worker_session_id: Option<String>,
    pub conflict: Option<ActivationConflict>,
    pub failure: Option<ActivationFailedRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivationConflict {
    pub expected_input_digest: Vec<u8>,
    pub provided_input_digest: Vec<u8>,
    pub expected_definition_digest: Vec<u8>,
    pub provided_definition_digest: Vec<u8>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteDecision {
    pub accepted: bool,
    pub replayed: bool,
    pub activation_id: String,
    pub attempt: u32,
    pub status: ActivationStatus,
    pub accepted_offset: Option<Offset>,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ActivationDecision {
    Begin(BeginDecision),
    Complete(WriteDecision),
    Fail(WriteDecision),
    Suspend(WriteDecision),
    Cancel(WriteDecision),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActivationTransition {
    pub records: Vec<ActivationJournalRecord>,
    pub decision: ActivationDecision,
}

/// Decide activation admission from journal-owned aggregate state.
pub fn decide_begin(
    state: Option<&ActivationState>,
    command: &BeginActivationCommand,
) -> Result<ActivationTransition, ActivationError> {
    validate_begin(&command.request)?;
    if command.issued_fence_token.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "partition owner issued an empty fence token".to_string(),
        ));
    }

    let proposed = ActivationIdentity::from_begin(&command.request);
    let Some(state) = state else {
        let started = started_record(
            &command.request,
            &proposed.activation_id,
            1,
            0,
            &command.issued_fence_token,
        );
        return Ok(ActivationTransition {
            records: vec![journal_record(Event::Started(started))],
            decision: ActivationDecision::Begin(BeginDecision {
                outcome: BeginActivationOutcome::Execute,
                activation_id: proposed.activation_id,
                attempt: 1,
                fence_token: command.issued_fence_token.clone(),
                accepted_offset: None,
                replay_result: None,
                active_worker_session_id: Some(command.request.worker_session_id.clone()),
                conflict: None,
                failure: None,
            }),
        });
    };

    if !state.identity.matches_begin(&command.request) {
        return Ok(ActivationTransition {
            records: Vec::new(),
            decision: ActivationDecision::Begin(BeginDecision {
                outcome: BeginActivationOutcome::Conflict,
                activation_id: state.identity.activation_id.clone(),
                attempt: state.current_attempt.attempt,
                fence_token: Vec::new(),
                accepted_offset: Some(state.accepted_offset),
                replay_result: None,
                active_worker_session_id: None,
                conflict: Some(ActivationConflict {
                    expected_input_digest: state.identity.input_digest.clone(),
                    provided_input_digest: command.request.input_digest.clone(),
                    expected_definition_digest: state.identity.definition_digest.clone(),
                    provided_definition_digest: command.request.definition_digest.clone(),
                    reason: "stable activation key was reused with different durable semantics"
                        .to_string(),
                }),
                failure: None,
            }),
        });
    }

    let decision = match state.status {
        ActivationStatus::Active => {
            let same_authority = state.current_attempt.worker_session_id
                == command.request.worker_session_id
                && state.current_attempt.run_authority == command.request.run_authority
                && state.current_attempt.lease_authority == command.request.lease_authority;
            BeginDecision {
                outcome: if same_authority {
                    BeginActivationOutcome::Execute
                } else {
                    BeginActivationOutcome::Wait
                },
                activation_id: state.identity.activation_id.clone(),
                attempt: state.current_attempt.attempt,
                fence_token: if same_authority {
                    state.current_attempt.fence_token.clone()
                } else {
                    Vec::new()
                },
                accepted_offset: Some(state.accepted_offset),
                replay_result: None,
                active_worker_session_id: Some(state.current_attempt.worker_session_id.clone()),
                conflict: None,
                failure: None,
            }
        }
        ActivationStatus::RetryReady => {
            let attempt = state
                .current_attempt
                .attempt
                .checked_add(1)
                .ok_or_else(|| ActivationError::IllegalTransition {
                    activation_id: state.identity.activation_id.clone(),
                    reason: "attempt counter overflow".to_string(),
                })?;
            let started = started_record(
                &command.request,
                &state.identity.activation_id,
                attempt,
                state.current_attempt.attempt,
                &command.issued_fence_token,
            );
            return Ok(ActivationTransition {
                records: vec![journal_record(Event::Started(started))],
                decision: ActivationDecision::Begin(BeginDecision {
                    outcome: BeginActivationOutcome::Execute,
                    activation_id: state.identity.activation_id.clone(),
                    attempt,
                    fence_token: command.issued_fence_token.clone(),
                    accepted_offset: None,
                    replay_result: None,
                    active_worker_session_id: Some(command.request.worker_session_id.clone()),
                    conflict: None,
                    failure: None,
                }),
            });
        }
        ActivationStatus::Completed => BeginDecision {
            outcome: BeginActivationOutcome::Replay,
            activation_id: state.identity.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            fence_token: Vec::new(),
            accepted_offset: Some(state.accepted_offset),
            replay_result: state
                .completion
                .as_ref()
                .and_then(|value| value.output.clone()),
            active_worker_session_id: None,
            conflict: None,
            failure: None,
        },
        ActivationStatus::Suspended => BeginDecision {
            outcome: BeginActivationOutcome::Wait,
            activation_id: state.identity.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            fence_token: Vec::new(),
            accepted_offset: Some(state.accepted_offset),
            replay_result: None,
            active_worker_session_id: Some(state.current_attempt.worker_session_id.clone()),
            conflict: None,
            failure: None,
        },
        ActivationStatus::Cancelled => BeginDecision {
            outcome: BeginActivationOutcome::Cancelled,
            activation_id: state.identity.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            fence_token: Vec::new(),
            accepted_offset: Some(state.accepted_offset),
            replay_result: None,
            active_worker_session_id: None,
            conflict: None,
            failure: None,
        },
        ActivationStatus::UnknownOutcome | ActivationStatus::Failed => BeginDecision {
            outcome: BeginActivationOutcome::UnknownOutcome,
            activation_id: state.identity.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            fence_token: Vec::new(),
            accepted_offset: Some(state.accepted_offset),
            replay_result: None,
            active_worker_session_id: None,
            conflict: None,
            failure: state.failure.clone(),
        },
        ActivationStatus::Unspecified => {
            return Err(ActivationError::IllegalTransition {
                activation_id: state.identity.activation_id.clone(),
                reason: "aggregate has unspecified status".to_string(),
            });
        }
    };

    Ok(ActivationTransition {
        records: Vec::new(),
        decision: ActivationDecision::Begin(decision),
    })
}

/// Decide one fenced completion. Exact retries return the original receipt.
pub fn decide_complete(
    state: &ActivationState,
    command: &CompleteActivationCommand,
) -> Result<ActivationTransition, ActivationError> {
    validate_complete(&command.request)?;
    validate_scope_and_activation(
        state,
        &command.request.project_id,
        &command.request.run_id,
        &command.request.activation_id,
    )?;

    let completed = completed_record(
        &command.request,
        ActivationHeader::from_started(&state.current_attempt),
        state.attempt_started_offset,
    );
    if state.status == ActivationStatus::Completed {
        if state.completion.as_ref() == Some(&completed) {
            return Ok(ActivationTransition {
                records: Vec::new(),
                decision: ActivationDecision::Complete(write_replay(state)),
            });
        }
        return Err(ActivationError::PayloadConflict {
            activation_id: command.request.activation_id.clone(),
            attempt: command.request.attempt,
        });
    }

    validate_active_authority(state, command.request.attempt, &command.request.fence_token)?;
    Ok(ActivationTransition {
        records: vec![journal_record(Event::Completed(completed))],
        decision: ActivationDecision::Complete(WriteDecision {
            accepted: true,
            replayed: false,
            activation_id: command.request.activation_id.clone(),
            attempt: command.request.attempt,
            status: ActivationStatus::Completed,
            accepted_offset: None,
        }),
    })
}

/// Decide one fenced failure. Retryable known failures reopen the same logical
/// activation for a new attempt; uncertain effects become terminal unknown outcomes.
pub fn decide_fail(
    state: &ActivationState,
    command: &FailActivationCommand,
) -> Result<ActivationTransition, ActivationError> {
    validate_fail(&command.request)?;
    validate_scope_and_activation(
        state,
        &command.request.project_id,
        &command.request.run_id,
        &command.request.activation_id,
    )?;
    let failed = failed_record(
        &command.request,
        ActivationHeader::from_started(&state.current_attempt),
        state.attempt_started_offset,
    );
    let certainty =
        ActivationExternalOutcomeCertainty::try_from(command.request.external_outcome_certainty)
            .unwrap_or(ActivationExternalOutcomeCertainty::Unspecified);
    let is_unknown = certainty == ActivationExternalOutcomeCertainty::Unknown;
    let target_status = failure_status(state.identity.recovery_policy, &failed, is_unknown);

    if matches!(
        state.status,
        ActivationStatus::RetryReady | ActivationStatus::Failed | ActivationStatus::UnknownOutcome
    ) {
        if state.failure.as_ref() == Some(&failed) {
            return Ok(ActivationTransition {
                records: Vec::new(),
                decision: ActivationDecision::Fail(write_replay(state)),
            });
        }
        return Err(ActivationError::PayloadConflict {
            activation_id: command.request.activation_id.clone(),
            attempt: command.request.attempt,
        });
    }

    validate_active_authority(state, command.request.attempt, &command.request.fence_token)?;
    // A replay-safe policy retries even when the effect is uncertain, so that
    // attempt is an ordinary failure. An unknown-outcome record is kept for
    // failures that end there, where the uncertainty is what the reader needs.
    let event = if is_unknown && target_status != ActivationStatus::RetryReady {
        Event::UnknownOutcome(ActivationUnknownOutcomeRecord {
            failure: Some(failed),
        })
    } else {
        Event::Failed(failed)
    };
    Ok(ActivationTransition {
        records: vec![journal_record(event)],
        decision: ActivationDecision::Fail(WriteDecision {
            accepted: true,
            replayed: false,
            activation_id: command.request.activation_id.clone(),
            attempt: command.request.attempt,
            status: target_status,
            accepted_offset: None,
        }),
    })
}

pub fn decide_suspend(
    state: &ActivationState,
    command: &SuspendActivationCommand,
) -> Result<ActivationTransition, ActivationError> {
    validate_scope_and_activation(
        state,
        &command.project_id,
        &command.run_id,
        &command.activation_id,
    )?;
    if command.resume_source.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "resume_source is required".to_string(),
        ));
    }
    let suspended = ActivationSuspendedRecord {
        project_id: command.project_id.clone(),
        run_id: command.run_id.clone(),
        activation_id: command.activation_id.clone(),
        attempt: command.attempt,
        header: ActivationHeader::from_started(&state.current_attempt),
        started_journal_offset: state.attempt_started_offset.0,
        fence_token: command.fence_token.clone(),
        resume_source: command.resume_source.clone(),
        ..Default::default()
    };
    decide_suspension(state, suspended)
}

/// Decide a durable timer suspension without changing the existing generic
/// approval/signal suspension contract.
pub fn decide_timer_suspend(
    state: &ActivationState,
    request: &SuspendActivationRequest,
) -> Result<ActivationTransition, ActivationError> {
    validate_suspend(request)?;
    validate_scope_and_activation(
        state,
        &request.project_id,
        &request.run_id,
        &request.activation_id,
    )?;
    if request.input_digest != state.identity.input_digest
        || request.definition_digest != state.identity.definition_digest
    {
        return Err(ActivationError::PayloadConflict {
            activation_id: request.activation_id.clone(),
            attempt: request.attempt,
        });
    }
    let suspended = ActivationSuspendedRecord {
        project_id: request.project_id.clone(),
        run_id: request.run_id.clone(),
        activation_id: request.activation_id.clone(),
        attempt: request.attempt,
        header: ActivationHeader::from_started(&state.current_attempt),
        started_journal_offset: state.attempt_started_offset.0,
        fence_token: request.fence_token.clone(),
        resume_source: format!("timer:{}", suspension_timer_id(request)),
        timer_key: request.timer_key.clone(),
        ready_at_ms: request.ready_at_ms,
        continuation: request.continuation.clone(),
    };
    decide_suspension(state, suspended)
}

fn decide_suspension(
    state: &ActivationState,
    suspended: ActivationSuspendedRecord,
) -> Result<ActivationTransition, ActivationError> {
    if state.status == ActivationStatus::Suspended {
        if state.suspension.as_ref() == Some(&suspended) {
            return Ok(ActivationTransition {
                records: Vec::new(),
                decision: ActivationDecision::Suspend(write_replay(state)),
            });
        }
        return Err(ActivationError::PayloadConflict {
            activation_id: suspended.activation_id.clone(),
            attempt: suspended.attempt,
        });
    }
    validate_active_authority(state, suspended.attempt, &suspended.fence_token)?;
    Ok(ActivationTransition {
        records: vec![journal_record(Event::Suspended(suspended.clone()))],
        decision: ActivationDecision::Suspend(WriteDecision {
            accepted: true,
            replayed: false,
            activation_id: suspended.activation_id,
            attempt: suspended.attempt,
            status: ActivationStatus::Suspended,
            accepted_offset: None,
        }),
    })
}

pub fn decide_cancel(
    state: &ActivationState,
    command: &CancelActivationCommand,
) -> Result<ActivationTransition, ActivationError> {
    validate_scope_and_activation(
        state,
        &command.project_id,
        &command.run_id,
        &command.activation_id,
    )?;
    if command.reason.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "cancellation reason is required".to_string(),
        ));
    }
    if matches!(
        state.status,
        ActivationStatus::Completed | ActivationStatus::Failed | ActivationStatus::UnknownOutcome
    ) {
        return Err(ActivationError::IllegalTransition {
            activation_id: command.activation_id.clone(),
            reason: "cancellation is not legal from the current activation status".to_string(),
        });
    }
    let cancelled = ActivationCancelledRecord {
        project_id: command.project_id.clone(),
        run_id: command.run_id.clone(),
        activation_id: command.activation_id.clone(),
        attempt: state.current_attempt.attempt,
        header: ActivationHeader::from_started(&state.current_attempt),
        reason: command.reason.clone(),
    };
    if state.status == ActivationStatus::Cancelled {
        if state.cancellation.as_ref() == Some(&cancelled) {
            return Ok(ActivationTransition {
                records: Vec::new(),
                decision: ActivationDecision::Cancel(write_replay(state)),
            });
        }
        return Err(ActivationError::PayloadConflict {
            activation_id: command.activation_id.clone(),
            attempt: state.current_attempt.attempt,
        });
    }
    Ok(ActivationTransition {
        records: vec![journal_record(Event::Cancelled(cancelled))],
        decision: ActivationDecision::Cancel(WriteDecision {
            accepted: true,
            replayed: false,
            activation_id: command.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            status: ActivationStatus::Cancelled,
            accepted_offset: None,
        }),
    })
}

/// Error code on the failure written for an activation whose worker lost its
/// lease partway through an attempt.
pub const WORKER_LOST_ERROR_CODE: &str = "WORKER_LOST";

/// Failures that settle every active attempt a run's expiring lease left open.
/// Every writer of `run.lease_expired` appends them in the same batch, before
/// the expiry, so an expiry that loses its lease fence writes neither.
///
/// A worker that dies mid-attempt never settles the attempt, and an attempt
/// left active under another worker's authority makes every later begin wait
/// on it, so the run could never resume. Each failure is retryable with an
/// unknown external outcome, under the dead attempt's own fence, so the
/// activation's recovery policy decides: replay-safe policies make it
/// retry-ready, and the others record an unknown outcome rather than run a
/// possibly completed effect again.
pub fn lease_loss_failures(
    states: &[ActivationState],
    timestamp_ns: i64,
) -> Result<Vec<crate::types::Record>, ActivationError> {
    open_attempt_failures(
        states,
        WORKER_LOST_ERROR_CODE,
        "the worker running this attempt stopped before it finished, and its lease expired",
        timestamp_ns,
    )
}

/// Error code on the failure written for a step still running when its run
/// suspended and released the lease the step was running under.
pub const SUSPENDED_WHILE_ACTIVE_ERROR_CODE: &str = "SUSPENDED_WHILE_ACTIVE";

/// Failures that settle every other attempt still active when a run suspends,
/// appended in the suspension's batch.
///
/// Suspending releases the run lease, which fences those attempts off from
/// completing, and the resumed run holds a new lease, so its begin would wait
/// on them. The lease it renews never expires, so without this nothing settles
/// them and the run stays stuck. Current SDKs keep the lease while steps are in
/// flight; this protects workers on older SDKs, whose steps run again.
pub fn suspension_sibling_failures(
    states: &[ActivationState],
    suspending_activation_id: &str,
    timestamp_ns: i64,
) -> Result<Vec<crate::types::Record>, ActivationError> {
    let siblings: Vec<ActivationState> = states
        .iter()
        .filter(|state| state.identity.activation_id != suspending_activation_id)
        .cloned()
        .collect();
    open_attempt_failures(
        &siblings,
        SUSPENDED_WHILE_ACTIVE_ERROR_CODE,
        "the run suspended while this attempt was still running",
        timestamp_ns,
    )
}

fn open_attempt_failures(
    states: &[ActivationState],
    error_code: &str,
    message: &str,
    timestamp_ns: i64,
) -> Result<Vec<crate::types::Record>, ActivationError> {
    let mut active: Vec<&ActivationState> = states
        .iter()
        .filter(|state| state.status == ActivationStatus::Active)
        .collect();
    active.sort_by(|left, right| {
        left.identity
            .activation_id
            .cmp(&right.identity.activation_id)
    });
    let message =
        serde_json::to_vec(&serde_json::json!({ "message": message })).unwrap_or_default();
    let mut records = Vec::new();
    for state in active {
        let attempt = &state.current_attempt;
        let transition = decide_fail(
            state,
            &FailActivationCommand {
                request: FailActivationRequest {
                    project_id: state.identity.project_id.clone(),
                    run_id: state.identity.run_id.clone(),
                    activation_id: state.identity.activation_id.clone(),
                    attempt: attempt.attempt,
                    fence_token: attempt.fence_token.clone(),
                    error_code: error_code.to_string(),
                    error_data: Some(ActivationPayload {
                        value: Some(Value::InlineData(message.clone())),
                    }),
                    retryable: true,
                    external_outcome_certainty: ActivationExternalOutcomeCertainty::Unknown as i32,
                    evidence: Vec::new(),
                    latency_ms: 0,
                },
            },
        )?;
        for canonical in &transition.records {
            records.push(to_engine_record(canonical, timestamp_ns)?);
        }
    }
    Ok(records)
}

/// Apply one accepted canonical journal record to aggregate state.
pub fn apply_journal_record(
    state: Option<ActivationState>,
    record: &ActivationJournalRecord,
    offset: Offset,
) -> Result<ActivationState, ActivationError> {
    if record.version != ACTIVATION_JOURNAL_VERSION {
        return Err(ActivationError::InvalidArgument(format!(
            "unsupported activation journal version {}",
            record.version
        )));
    }
    validate_journal_record_payload(record)?;
    let event = record.event.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument("activation journal event is required".to_string())
    })?;
    match event {
        Event::Started(started) => apply_started(state, started, offset),
        Event::Completed(completed) => {
            let mut state = require_state(state, &completed.activation_id)?;
            validate_scope_and_activation(
                &state,
                &completed.project_id,
                &completed.run_id,
                &completed.activation_id,
            )?;
            validate_active_authority(&state, completed.attempt, &completed.fence_token)?;
            validate_header(&state, &completed.header)?;
            state.status = ActivationStatus::Completed;
            state.completion = Some(completed.clone());
            state.failure = None;
            state.suspension = None;
            state.cancellation = None;
            state.accepted_offset = offset;
            Ok(state)
        }
        Event::Failed(failed) => apply_failed(state, failed, false, offset),
        Event::UnknownOutcome(unknown) => {
            let failure = unknown.failure.as_ref().ok_or_else(|| {
                ActivationError::InvalidArgument(
                    "unknown-outcome record is missing failure".to_string(),
                )
            })?;
            apply_failed(state, failure, true, offset)
        }
        Event::Suspended(suspended) => {
            let mut state = require_state(state, &suspended.activation_id)?;
            validate_scope_and_activation(
                &state,
                &suspended.project_id,
                &suspended.run_id,
                &suspended.activation_id,
            )?;
            validate_active_authority(&state, suspended.attempt, &suspended.fence_token)?;
            validate_header(&state, &suspended.header)?;
            state.status = ActivationStatus::Suspended;
            state.suspension = Some(suspended.clone());
            state.completion = None;
            state.failure = None;
            state.cancellation = None;
            state.accepted_offset = offset;
            Ok(state)
        }
        Event::Cancelled(cancelled) => {
            let mut state = require_state(state, &cancelled.activation_id)?;
            validate_scope_and_activation(
                &state,
                &cancelled.project_id,
                &cancelled.run_id,
                &cancelled.activation_id,
            )?;
            if !matches!(
                state.status,
                ActivationStatus::Active
                    | ActivationStatus::RetryReady
                    | ActivationStatus::Suspended
            ) {
                return Err(ActivationError::IllegalTransition {
                    activation_id: cancelled.activation_id.clone(),
                    reason: "cancellation is not legal from the current activation status"
                        .to_string(),
                });
            }
            validate_header(&state, &cancelled.header)?;
            state.status = ActivationStatus::Cancelled;
            state.cancellation = Some(cancelled.clone());
            state.completion = None;
            state.failure = None;
            state.suspension = None;
            state.accepted_offset = offset;
            Ok(state)
        }
        Event::Snapshot(snapshot) => state_from_snapshot(snapshot),
    }
}

pub fn snapshot_record(state: &ActivationState) -> ActivationJournalRecord {
    journal_record(Event::Snapshot(snapshot(state)))
}

pub fn snapshot(state: &ActivationState) -> ActivationSnapshotRecord {
    ActivationSnapshotRecord {
        project_id: state.identity.project_id.clone(),
        run_id: state.identity.run_id.clone(),
        activation_id: state.identity.activation_id.clone(),
        status: state.status as i32,
        current_attempt: Some(state.current_attempt.clone()),
        completion: state.completion.clone(),
        failure: state.failure.clone(),
        accepted_journal_offset: state.accepted_offset.0,
        attempt_started_offset: state.attempt_started_offset.0,
        suspension: state.suspension.clone(),
        cancellation: state.cancellation.clone(),
    }
}

pub fn activation_id(
    project_id: &str,
    run_id: &str,
    parent_activation_id: &str,
    kind: i32,
    stable_key: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"agnt5.activation.identity.v1\0");
    for value in [project_id, run_id, parent_activation_id] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.update(kind.to_be_bytes());
    hasher.update((stable_key.len() as u64).to_be_bytes());
    hasher.update(stable_key.as_bytes());
    format!(
        "actv1_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
    )
}

pub fn mint_fence_token(
    signing_key: &[u8],
    activation_id: &str,
    attempt: u32,
    worker_session_id: &str,
    run_authority: &[u8],
    lease_authority: &[u8],
) -> Result<Vec<u8>, ActivationError> {
    let mut mac = HmacSha256::new_from_slice(signing_key).map_err(|_| {
        ActivationError::InvalidArgument("activation fence signing key is invalid".to_string())
    })?;
    mac.update(b"agnt5.activation.fence.v1\0");
    for value in [
        activation_id.as_bytes(),
        &attempt.to_be_bytes(),
        worker_session_id.as_bytes(),
        run_authority,
        lease_authority,
    ] {
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value);
    }
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(mac.finalize().into_bytes())
        .into_bytes())
}

pub fn payload_digest(payload: &ActivationPayload) -> Result<Vec<u8>, ActivationError> {
    let value = payload.value.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument("activation payload value is required".to_string())
    })?;
    let digest = match value {
        Value::InlineData(bytes) => Sha256::digest(bytes).to_vec(),
        Value::Reference(reference) => Sha256::digest(reference.encode_to_vec()).to_vec(),
    };
    Ok(digest)
}

/// Derive the stable timer generation owned by one accepted suspension.
pub fn suspension_timer_id(request: &SuspendActivationRequest) -> String {
    let mut digest = Sha256::new();
    digest.update(b"agnt5.activation.timer.v1\0");
    for value in [
        request.project_id.as_bytes(),
        request.run_id.as_bytes(),
        request.activation_id.as_bytes(),
        &request.attempt.to_be_bytes(),
        request.timer_key.as_bytes(),
        &request.ready_at_ms.to_be_bytes(),
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    format!(
        "tmrv1_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
    )
}

fn started_record(
    request: &BeginActivationRequest,
    activation_id: &str,
    attempt: u32,
    previous_attempt: u32,
    fence_token: &[u8],
) -> ActivationStartedRecord {
    ActivationStartedRecord {
        project_id: request.project_id.clone(),
        run_id: request.run_id.clone(),
        parent_activation_id: request.parent_activation_id.clone(),
        activation_id: activation_id.to_string(),
        kind: request.kind,
        stable_key: request.stable_key.clone(),
        name: request.display_name.clone(),
        display_parent_correlation_id: (!request.display_parent_correlation_id.is_empty())
            .then(|| request.display_parent_correlation_id.clone()),
        input_digest: request.input_digest.clone(),
        definition_digest: request.definition_digest.clone(),
        recovery_policy: request.recovery_policy,
        attempt,
        previous_attempt,
        child: request.child.clone(),
        decision: "execute".to_string(),
        input_data: parse_input_data(&request.input_data),
        fence_token: fence_token.to_vec(),
        worker_session_id: request.worker_session_id.clone(),
        run_authority: request.run_authority.clone(),
        lease_authority: request.lease_authority.clone(),
    }
}

/// Plaintext input for readers. Validation already rejected non-JSON input,
/// so a parse failure here is unreachable and folds to `None`.
fn parse_input_data(bytes: &[u8]) -> Option<serde_json::Value> {
    if bytes.is_empty() {
        return None;
    }
    serde_json::from_slice(bytes).ok()
}

fn completed_record(
    request: &CompleteActivationRequest,
    header: ActivationHeader,
    started_offset: Offset,
) -> ActivationCompletedRecord {
    let mut state_mutations = request.state_mutations.clone();
    state_mutations.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    let mut outbox_intents = request.outbox_intents.clone();
    outbox_intents.sort_by(|left, right| left.intent_id.cmp(&right.intent_id));
    let mut evidence = request.evidence.clone();
    evidence.sort_by(|left, right| {
        left.evidence_type
            .cmp(&right.evidence_type)
            .then_with(|| left.sha256.cmp(&right.sha256))
            .then_with(|| left.encode_to_vec().cmp(&right.encode_to_vec()))
    });
    let mut usage = request.usage.clone();
    if let Some(usage) = &mut usage {
        if usage.cost_usd == 0.0 {
            usage.cost_usd = 0.0;
        }
    }
    let duration_ms = usage
        .as_ref()
        .map(|usage| usage.latency_ms)
        .filter(|latency| *latency > 0);
    ActivationCompletedRecord {
        project_id: request.project_id.clone(),
        run_id: request.run_id.clone(),
        activation_id: request.activation_id.clone(),
        attempt: request.attempt,
        header,
        started_journal_offset: started_offset.0,
        fence_token: request.fence_token.clone(),
        output: request.output.clone(),
        output_digest: request.output_digest.clone(),
        state_mutations,
        outbox_intents,
        usage,
        duration_ms,
        evidence,
    }
}

fn failed_record(
    request: &FailActivationRequest,
    header: ActivationHeader,
    started_offset: Offset,
) -> ActivationFailedRecord {
    let mut evidence = request.evidence.clone();
    evidence.sort_by(|left, right| {
        left.evidence_type
            .cmp(&right.evidence_type)
            .then_with(|| left.sha256.cmp(&right.sha256))
            .then_with(|| left.encode_to_vec().cmp(&right.encode_to_vec()))
    });
    ActivationFailedRecord {
        project_id: request.project_id.clone(),
        run_id: request.run_id.clone(),
        activation_id: request.activation_id.clone(),
        attempt: request.attempt,
        header,
        started_journal_offset: started_offset.0,
        fence_token: request.fence_token.clone(),
        error_code: request.error_code.clone(),
        error_message: error_message(request.error_data.as_ref()),
        error_data: request.error_data.clone(),
        retryable: request.retryable,
        external_outcome_certainty: request.external_outcome_certainty,
        latency_ms: request.latency_ms,
        duration_ms: (request.latency_ms > 0).then_some(request.latency_ms),
        evidence,
    }
}

/// A human-readable message when the inline error payload is a JSON object
/// carrying one; derived deterministically so retries stay byte-identical.
fn error_message(error_data: Option<&ActivationPayload>) -> String {
    let Some(Value::InlineData(bytes)) = error_data.and_then(|payload| payload.value.as_ref())
    else {
        return String::new();
    };
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .or_else(|| value.get("error_message"))
                .and_then(|message| message.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default()
}

/// A settling record must describe the attempt it settles.
fn validate_header(
    state: &ActivationState,
    header: &ActivationHeader,
) -> Result<(), ActivationError> {
    if *header != ActivationHeader::from_started(&state.current_attempt) {
        return Err(ActivationError::InvalidArgument(
            "activation record header does not match the current attempt".to_string(),
        ));
    }
    Ok(())
}

fn validate_display_parent(value: &str) -> Result<(), ActivationError> {
    if value.len() > MAX_DISPLAY_PARENT_CORRELATION_ID_BYTES {
        return Err(ActivationError::InvalidArgument(format!(
            "display_parent_correlation_id exceeds {MAX_DISPLAY_PARENT_CORRELATION_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_begin(request: &BeginActivationRequest) -> Result<(), ActivationError> {
    for (name, value) in [
        ("project_id", request.project_id.as_str()),
        ("run_id", request.run_id.as_str()),
        ("stable_key", request.stable_key.as_str()),
        ("worker_session_id", request.worker_session_id.as_str()),
    ] {
        if value.is_empty() {
            return Err(ActivationError::InvalidArgument(format!(
                "{name} is required"
            )));
        }
    }
    validate_digest("input_digest", &request.input_digest)?;
    validate_digest("definition_digest", &request.definition_digest)?;
    if ActivationKind::try_from(request.kind).unwrap_or(ActivationKind::Unspecified)
        == ActivationKind::Unspecified
    {
        return Err(ActivationError::InvalidArgument(
            "activation kind is required".to_string(),
        ));
    }
    if ActivationRecoveryPolicy::try_from(request.recovery_policy)
        .unwrap_or(ActivationRecoveryPolicy::Unspecified)
        == ActivationRecoveryPolicy::Unspecified
    {
        return Err(ActivationError::InvalidArgument(
            "activation recovery policy is required".to_string(),
        ));
    }
    if request.run_authority.is_empty() || request.lease_authority.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "run_authority and lease_authority are required".to_string(),
        ));
    }
    validate_display_parent(&request.display_parent_correlation_id)?;
    if request.display_name.len() > MAX_ACTIVATION_NAME_BYTES {
        return Err(ActivationError::InvalidArgument(format!(
            "display_name exceeds {MAX_ACTIVATION_NAME_BYTES} bytes"
        )));
    }
    if request.input_data.len() > MAX_ACTIVATION_INPUT_DATA_BYTES {
        return Err(ActivationError::InlinePayloadTooLarge {
            limit: MAX_ACTIVATION_INPUT_DATA_BYTES,
            actual: request.input_data.len(),
        });
    }
    if !request.input_data.is_empty()
        && serde_json::from_slice::<serde_json::Value>(&request.input_data).is_err()
    {
        return Err(ActivationError::InvalidArgument(
            "input_data must be JSON".to_string(),
        ));
    }
    validate_child_linkage(request)?;
    Ok(())
}

fn validate_child_linkage(request: &BeginActivationRequest) -> Result<(), ActivationError> {
    let kind = ActivationKind::try_from(request.kind).unwrap_or(ActivationKind::Unspecified);
    match (kind, request.child.as_ref()) {
        (ActivationKind::Child, Some(child)) => {
            if child.child_key.trim().is_empty()
                || child.child_run_id.trim().is_empty()
                || child.child_session_id.trim().is_empty()
            {
                return Err(ActivationError::InvalidArgument(
                    "child_key, child_run_id, and child_session_id are required for child activations"
                        .to_string(),
                ));
            }
            validate_digest("child_definition_digest", &child.child_definition_digest)?;
            if child.child_definition_digest != request.definition_digest {
                return Err(ActivationError::InvalidArgument(
                    "child_definition_digest must match the activation definition_digest"
                        .to_string(),
                ));
            }
            if child.child_key != request.stable_key {
                return Err(ActivationError::InvalidArgument(
                    "child_key must match the activation stable_key".to_string(),
                ));
            }
            if ChildJoinPolicy::try_from(child.join_policy).unwrap_or(ChildJoinPolicy::Unspecified)
                == ChildJoinPolicy::Unspecified
            {
                return Err(ActivationError::InvalidArgument(
                    "child join_policy is required".to_string(),
                ));
            }
        }
        (ActivationKind::Child, None) => {
            return Err(ActivationError::InvalidArgument(
                "child activation linkage is required".to_string(),
            ));
        }
        (_, Some(_)) => {
            return Err(ActivationError::InvalidArgument(
                "child activation linkage is only valid for child activations".to_string(),
            ));
        }
        (_, None) => {}
    }
    Ok(())
}

fn validate_complete(request: &CompleteActivationRequest) -> Result<(), ActivationError> {
    validate_write_identity(
        &request.project_id,
        &request.run_id,
        &request.activation_id,
        request.attempt,
        &request.fence_token,
    )?;
    let output = request.output.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument("completion output is required".to_string())
    })?;
    validate_payload(output, &request.project_id)?;
    validate_digest("output_digest", &request.output_digest)?;
    if payload_digest(output)? != request.output_digest {
        return Err(ActivationError::InvalidArgument(
            "output_digest does not match the canonical output payload".to_string(),
        ));
    }
    validate_mutations(&request.state_mutations, &request.run_id)?;
    validate_outbox(&request.outbox_intents, &request.project_id)?;
    validate_usage(request.usage.as_ref())?;
    validate_evidence(&request.evidence, &request.project_id)?;
    Ok(())
}

fn validate_fail(request: &FailActivationRequest) -> Result<(), ActivationError> {
    validate_write_identity(
        &request.project_id,
        &request.run_id,
        &request.activation_id,
        request.attempt,
        &request.fence_token,
    )?;
    if request.error_code.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "stable error_code is required".to_string(),
        ));
    }
    if let Some(error_data) = &request.error_data {
        validate_payload(error_data, &request.project_id)?;
    }
    if ActivationExternalOutcomeCertainty::try_from(request.external_outcome_certainty)
        .unwrap_or(ActivationExternalOutcomeCertainty::Unspecified)
        == ActivationExternalOutcomeCertainty::Unspecified
    {
        return Err(ActivationError::InvalidArgument(
            "external_outcome_certainty is required".to_string(),
        ));
    }
    validate_evidence(&request.evidence, &request.project_id)
}

fn validate_suspend(request: &SuspendActivationRequest) -> Result<(), ActivationError> {
    validate_write_identity(
        &request.project_id,
        &request.run_id,
        &request.activation_id,
        request.attempt,
        &request.fence_token,
    )?;
    if request.timer_key.trim().is_empty() {
        return Err(ActivationError::InvalidArgument(
            "stable timer_key is required".to_string(),
        ));
    }
    if request.ready_at_ms <= 0 {
        return Err(ActivationError::InvalidArgument(
            "ready_at_ms must be positive".to_string(),
        ));
    }
    validate_digest("input_digest", &request.input_digest)?;
    validate_digest("definition_digest", &request.definition_digest)?;
    if request.continuation.len() > MAX_ACTIVATION_INLINE_BYTES {
        return Err(ActivationError::InlinePayloadTooLarge {
            limit: MAX_ACTIVATION_INLINE_BYTES,
            actual: request.continuation.len(),
        });
    }
    Ok(())
}

fn validate_write_identity(
    project_id: &str,
    run_id: &str,
    activation_id: &str,
    attempt: u32,
    fence_token: &[u8],
) -> Result<(), ActivationError> {
    if project_id.is_empty() || run_id.is_empty() || activation_id.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "project_id, run_id, and activation_id are required".to_string(),
        ));
    }
    if attempt == 0 || fence_token.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "attempt and fence_token are required".to_string(),
        ));
    }
    Ok(())
}

fn validate_digest(name: &str, digest: &[u8]) -> Result<(), ActivationError> {
    if digest.len() != 32 {
        return Err(ActivationError::InvalidArgument(format!(
            "{name} must contain exactly 32 SHA-256 bytes"
        )));
    }
    Ok(())
}

fn validate_payload(payload: &ActivationPayload, project_id: &str) -> Result<(), ActivationError> {
    match payload.value.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument("activation payload value is required".to_string())
    })? {
        Value::InlineData(bytes) if bytes.len() > MAX_ACTIVATION_INLINE_BYTES => {
            Err(ActivationError::InlinePayloadTooLarge {
                limit: MAX_ACTIVATION_INLINE_BYTES,
                actual: bytes.len(),
            })
        }
        Value::InlineData(_) => Ok(()),
        Value::Reference(reference) => {
            if reference.project_id != project_id {
                return Err(ActivationError::InvalidReference(
                    "reference project_id does not match the activation".to_string(),
                ));
            }
            if reference.kind.is_empty()
                || reference.uri.is_empty()
                || reference.size_bytes == 0
                || reference.sha256.len() != 32
                || reference.content_type.is_empty()
            {
                return Err(ActivationError::InvalidReference(
                    "kind, immutable uri, size_bytes, content_type, and a 32-byte SHA-256 checksum are required"
                        .to_string(),
                ));
            }
            Ok(())
        }
    }
}

fn validate_mutations(
    mutations: &[ActivationStateMutation],
    activation_run_id: &str,
) -> Result<(), ActivationError> {
    if mutations.len() > MAX_ACTIVATION_STATE_MUTATIONS {
        return Err(ActivationError::InvalidArgument(format!(
            "at most {MAX_ACTIVATION_STATE_MUTATIONS} state mutations are allowed"
        )));
    }
    let mut ids = HashSet::new();
    let mut targets = HashSet::new();
    for mutation in mutations {
        if mutation.entity_type == crate::runtime_work::ENTITY_TYPE {
            return Err(ActivationError::InvalidArgument(
                "runtime controller state is internal".into(),
            ));
        }
        if mutation.operation_id.is_empty()
            || mutation.entity_type.is_empty()
            || mutation.entity_key.is_empty()
            || mutation.scope.is_empty()
            || mutation.scope_id.is_empty()
            || mutation.data_type.is_empty()
        {
            return Err(ActivationError::InvalidArgument(
                "state mutation identity fields and data_type are required".to_string(),
            ));
        }
        if mutation.expected_version < 0 {
            return Err(ActivationError::InvalidArgument(
                "state mutation expected_version must be non-negative".to_string(),
            ));
        }
        if mutation.scope != "run" {
            return Err(ActivationError::InvalidArgument(
                "Durable Activation V1 state mutations must use run scope".to_string(),
            ));
        }
        if mutation.scope_id != activation_run_id {
            return Err(ActivationError::InvalidArgument(
                "Durable Activation V1 run-scoped state mutation scope_id must match run_id"
                    .to_string(),
            ));
        }
        if matches!(
            mutation.entity_type.as_str(),
            crate::storage_contract::ACTIVATION_PROJECTION_ENTITY_TYPE
                | crate::storage_contract::ACTIVATION_OUTBOX_ENTITY_TYPE
        ) {
            return Err(ActivationError::InvalidArgument(
                "state mutations cannot target runtime-reserved activation entity types"
                    .to_string(),
            ));
        }
        if mutation.data.len() > MAX_ACTIVATION_INLINE_BYTES {
            return Err(ActivationError::InlinePayloadTooLarge {
                limit: MAX_ACTIVATION_INLINE_BYTES,
                actual: mutation.data.len(),
            });
        }
        if !ids.insert(mutation.operation_id.as_str()) {
            return Err(ActivationError::InvalidArgument(format!(
                "duplicate state mutation operation_id {:?}",
                mutation.operation_id
            )));
        }
        if !targets.insert((
            mutation.entity_type.as_str(),
            mutation.entity_key.as_str(),
            mutation.scope.as_str(),
            mutation.scope_id.as_str(),
        )) {
            return Err(ActivationError::InvalidArgument(format!(
                "multiple state mutations target the same entity in one completion: {}/{}/{}/{}",
                mutation.entity_type, mutation.entity_key, mutation.scope, mutation.scope_id
            )));
        }
    }
    Ok(())
}

fn validate_usage(usage: Option<&engine_proto::ActivationUsage>) -> Result<(), ActivationError> {
    let Some(usage) = usage else {
        return Ok(());
    };
    if usage.tokens_in < 0
        || usage.tokens_out < 0
        || usage.cached_tokens < 0
        || usage.latency_ms < 0
    {
        return Err(ActivationError::InvalidArgument(
            "activation usage counters must be non-negative".to_string(),
        ));
    }
    if !usage.cost_usd.is_finite() || usage.cost_usd < 0.0 {
        return Err(ActivationError::InvalidArgument(
            "activation usage cost_usd must be finite and non-negative".to_string(),
        ));
    }
    Ok(())
}

fn validate_outbox(
    intents: &[ActivationOutboxIntent],
    project_id: &str,
) -> Result<(), ActivationError> {
    if intents.len() > MAX_ACTIVATION_OUTBOX_INTENTS {
        return Err(ActivationError::InvalidArgument(format!(
            "at most {MAX_ACTIVATION_OUTBOX_INTENTS} outbox intents are allowed"
        )));
    }
    let mut ids = HashSet::new();
    for intent in intents {
        if intent.intent_id.is_empty()
            || intent.kind.is_empty()
            || intent.idempotency_key.is_empty()
        {
            return Err(ActivationError::InvalidArgument(
                "outbox intent_id, kind, and idempotency_key are required".to_string(),
            ));
        }
        if !ids.insert(intent.intent_id.as_str()) {
            return Err(ActivationError::InvalidArgument(format!(
                "duplicate outbox intent_id {:?}",
                intent.intent_id
            )));
        }
        validate_payload(
            intent.payload.as_ref().ok_or_else(|| {
                ActivationError::InvalidArgument("outbox payload is required".to_string())
            })?,
            project_id,
        )?;
    }
    Ok(())
}

fn validate_evidence(
    evidence: &[ActivationEvidence],
    project_id: &str,
) -> Result<(), ActivationError> {
    if evidence.len() > MAX_ACTIVATION_EVIDENCE_ITEMS {
        return Err(ActivationError::InvalidArgument(format!(
            "at most {MAX_ACTIVATION_EVIDENCE_ITEMS} evidence items are allowed"
        )));
    }
    let mut inline_total = 0usize;
    for item in evidence {
        if item.evidence_type.is_empty() || item.sha256.len() != 32 {
            return Err(ActivationError::InvalidArgument(
                "evidence_type and a 32-byte SHA-256 checksum are required".to_string(),
            ));
        }
        let payload = item.payload.as_ref().ok_or_else(|| {
            ActivationError::InvalidArgument("evidence payload is required".to_string())
        })?;
        validate_payload(payload, project_id)?;
        if payload_digest(payload)? != item.sha256 {
            return Err(ActivationError::InvalidArgument(
                "evidence sha256 does not match its canonical payload".to_string(),
            ));
        }
        if let Some(Value::InlineData(bytes)) = payload.value.as_ref() {
            inline_total = inline_total.saturating_add(bytes.len());
        }
    }
    if inline_total > MAX_ACTIVATION_EVIDENCE_BYTES {
        return Err(ActivationError::InlinePayloadTooLarge {
            limit: MAX_ACTIVATION_EVIDENCE_BYTES,
            actual: inline_total,
        });
    }
    Ok(())
}

fn validate_scope_and_activation(
    state: &ActivationState,
    project_id: &str,
    run_id: &str,
    activation_id: &str,
) -> Result<(), ActivationError> {
    if state.identity.project_id != project_id
        || state.identity.run_id != run_id
        || state.identity.activation_id != activation_id
    {
        return Err(ActivationError::IllegalTransition {
            activation_id: activation_id.to_string(),
            reason: "command scope does not match the journal aggregate".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_journal_record_payload(
    record: &ActivationJournalRecord,
) -> Result<(), ActivationError> {
    if record.version != ACTIVATION_JOURNAL_VERSION {
        return Err(ActivationError::InvalidArgument(format!(
            "unsupported activation journal version {}",
            record.version
        )));
    }
    let event = record.event.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument("activation journal event is required".to_string())
    })?;
    if let Some(parent) = event.display_parent_correlation_id() {
        if parent.is_empty() {
            return Err(ActivationError::InvalidArgument(
                "absent display_parent_correlation_id must be omitted".to_string(),
            ));
        }
        validate_display_parent(parent)?;
    }
    match event {
        Event::Started(started) => validate_started(started),
        Event::Completed(completed) => {
            let request = CompleteActivationRequest {
                project_id: completed.project_id.clone(),
                run_id: completed.run_id.clone(),
                activation_id: completed.activation_id.clone(),
                attempt: completed.attempt,
                fence_token: completed.fence_token.clone(),
                output: completed.output.clone(),
                output_digest: completed.output_digest.clone(),
                state_mutations: completed.state_mutations.clone(),
                outbox_intents: completed.outbox_intents.clone(),
                usage: completed.usage.clone(),
                evidence: completed.evidence.clone(),
            };
            validate_complete(&request)?;
            event_prefix(completed.header.kind)?;
            if completed_record(
                &request,
                completed.header.clone(),
                Offset(completed.started_journal_offset),
            ) != *completed
            {
                return Err(ActivationError::InvalidArgument(
                    "activation completion is not in canonical V1 order".to_string(),
                ));
            }
            Ok(())
        }
        Event::Failed(failed) => validate_failed_record(failed),
        Event::UnknownOutcome(unknown) => {
            validate_failed_record(unknown.failure.as_ref().ok_or_else(|| {
                ActivationError::InvalidArgument(
                    "unknown-outcome record is missing failure".to_string(),
                )
            })?)
        }
        Event::Suspended(suspended) => {
            validate_write_identity(
                &suspended.project_id,
                &suspended.run_id,
                &suspended.activation_id,
                suspended.attempt,
                &suspended.fence_token,
            )?;
            if suspended.resume_source.is_empty() {
                return Err(ActivationError::InvalidArgument(
                    "activation suspension resume_source is required".to_string(),
                ));
            }
            event_prefix(suspended.header.kind)?;
            if !suspended.timer_key.is_empty() {
                let request = SuspendActivationRequest {
                    project_id: suspended.project_id.clone(),
                    run_id: suspended.run_id.clone(),
                    activation_id: suspended.activation_id.clone(),
                    attempt: suspended.attempt,
                    fence_token: suspended.fence_token.clone(),
                    timer_key: suspended.timer_key.clone(),
                    ready_at_ms: suspended.ready_at_ms,
                    input_digest: suspended.header.input_digest.clone(),
                    definition_digest: suspended.header.definition_digest.clone(),
                    continuation: suspended.continuation.clone(),
                    delay_ms: 0,
                };
                validate_suspend(&request)?;
                if suspended.resume_source != format!("timer:{}", suspension_timer_id(&request)) {
                    return Err(ActivationError::InvalidArgument(
                        "activation timer suspension resume_source is not canonical".to_string(),
                    ));
                }
            } else if suspended.ready_at_ms != 0 || !suspended.continuation.is_empty() {
                return Err(ActivationError::InvalidArgument(
                    "generic activation suspension carries partial timer state".to_string(),
                ));
            }
            Ok(())
        }
        Event::Cancelled(cancelled) => {
            if cancelled.project_id.is_empty()
                || cancelled.run_id.is_empty()
                || cancelled.activation_id.is_empty()
                || cancelled.attempt == 0
                || cancelled.reason.is_empty()
            {
                return Err(ActivationError::InvalidArgument(
                    "activation cancellation identity and reason are required".to_string(),
                ));
            }
            event_prefix(cancelled.header.kind)?;
            Ok(())
        }
        Event::Snapshot(value) => state_from_snapshot(value).map(|_| ()),
    }
}

fn validate_failed_record(failed: &ActivationFailedRecord) -> Result<(), ActivationError> {
    let request = FailActivationRequest {
        project_id: failed.project_id.clone(),
        run_id: failed.run_id.clone(),
        activation_id: failed.activation_id.clone(),
        attempt: failed.attempt,
        fence_token: failed.fence_token.clone(),
        error_code: failed.error_code.clone(),
        error_data: failed.error_data.clone(),
        retryable: failed.retryable,
        external_outcome_certainty: failed.external_outcome_certainty,
        evidence: failed.evidence.clone(),
        latency_ms: failed.latency_ms,
    };
    validate_fail(&request)?;
    event_prefix(failed.header.kind)?;
    if failed_record(
        &request,
        failed.header.clone(),
        Offset(failed.started_journal_offset),
    ) != *failed
    {
        return Err(ActivationError::InvalidArgument(
            "activation failure is not in canonical V1 order".to_string(),
        ));
    }
    Ok(())
}

fn validate_active_authority(
    state: &ActivationState,
    attempt: u32,
    fence_token: &[u8],
) -> Result<(), ActivationError> {
    if state.status != ActivationStatus::Active
        || state.current_attempt.attempt != attempt
        || state.current_attempt.fence_token != fence_token
    {
        return Err(ActivationError::StaleAuthority {
            activation_id: state.identity.activation_id.clone(),
            request_attempt: attempt,
            current_attempt: state.current_attempt.attempt,
        });
    }
    Ok(())
}

fn apply_started(
    state: Option<ActivationState>,
    started: &ActivationStartedRecord,
    offset: Offset,
) -> Result<ActivationState, ActivationError> {
    validate_started(started)?;
    let identity = ActivationIdentity::from_started(started);
    if let Some(mut state) = state {
        if state.identity != identity {
            return Err(ActivationError::NondeterministicReplay {
                activation_id: started.activation_id.clone(),
                reason: "accepted retry changed immutable activation identity".to_string(),
            });
        }
        if state.status != ActivationStatus::RetryReady
            || started.previous_attempt != state.current_attempt.attempt
            || started.attempt != state.current_attempt.attempt.saturating_add(1)
        {
            return Err(ActivationError::IllegalTransition {
                activation_id: started.activation_id.clone(),
                reason: "retry start does not follow the retry-ready attempt".to_string(),
            });
        }
        state.status = ActivationStatus::Active;
        state.current_attempt = started.clone();
        state.failure = None;
        state.completion = None;
        state.suspension = None;
        state.cancellation = None;
        state.accepted_offset = offset;
        state.attempt_started_offset = offset;
        return Ok(state);
    }
    if started.attempt != 1 || started.previous_attempt != 0 {
        return Err(ActivationError::IllegalTransition {
            activation_id: started.activation_id.clone(),
            reason: "first accepted attempt must be attempt 1".to_string(),
        });
    }
    Ok(ActivationState {
        identity,
        status: ActivationStatus::Active,
        current_attempt: started.clone(),
        completion: None,
        failure: None,
        suspension: None,
        cancellation: None,
        accepted_offset: offset,
        attempt_started_offset: offset,
    })
}

fn validate_started(started: &ActivationStartedRecord) -> Result<(), ActivationError> {
    let request = BeginActivationRequest {
        project_id: started.project_id.clone(),
        run_id: started.run_id.clone(),
        parent_activation_id: started.parent_activation_id.clone(),
        kind: started.kind,
        stable_key: started.stable_key.clone(),
        input_digest: started.input_digest.clone(),
        definition_digest: started.definition_digest.clone(),
        recovery_policy: started.recovery_policy,
        worker_session_id: started.worker_session_id.clone(),
        run_authority: started.run_authority.clone(),
        lease_authority: started.lease_authority.clone(),
        child: started.child.clone(),
        display_name: started.name.clone(),
        display_parent_correlation_id: started
            .display_parent_correlation_id
            .clone()
            .unwrap_or_default(),
        input_data: started
            .input_data
            .as_ref()
            .map(|value| serde_json::to_vec(value).unwrap_or_default())
            .unwrap_or_default(),
    };
    validate_begin(&request)?;
    event_prefix(started.kind)?;
    if started.decision != "execute" {
        return Err(ActivationError::InvalidArgument(
            "started record decision must be execute".to_string(),
        ));
    }
    if started.activation_id
        != activation_id(
            &started.project_id,
            &started.run_id,
            &started.parent_activation_id,
            started.kind,
            &started.stable_key,
        )
    {
        return Err(ActivationError::NondeterministicReplay {
            activation_id: started.activation_id.clone(),
            reason: "activation ID does not match canonical identity".to_string(),
        });
    }
    if started.attempt == 0 || started.fence_token.is_empty() {
        return Err(ActivationError::InvalidArgument(
            "started record requires attempt and fence token".to_string(),
        ));
    }
    Ok(())
}

fn apply_failed(
    state: Option<ActivationState>,
    failed: &ActivationFailedRecord,
    force_unknown: bool,
    offset: Offset,
) -> Result<ActivationState, ActivationError> {
    let mut state = require_state(state, &failed.activation_id)?;
    validate_scope_and_activation(
        &state,
        &failed.project_id,
        &failed.run_id,
        &failed.activation_id,
    )?;
    validate_active_authority(&state, failed.attempt, &failed.fence_token)?;
    validate_header(&state, &failed.header)?;
    state.status = failure_status(state.identity.recovery_policy, failed, force_unknown);
    state.failure = Some(failed.clone());
    state.completion = None;
    state.suspension = None;
    state.cancellation = None;
    state.accepted_offset = offset;
    Ok(state)
}

fn failure_status(
    recovery_policy: i32,
    failed: &ActivationFailedRecord,
    force_unknown: bool,
) -> ActivationStatus {
    let certainty = ActivationExternalOutcomeCertainty::try_from(failed.external_outcome_certainty)
        .unwrap_or(ActivationExternalOutcomeCertainty::Unspecified);
    let policy = ActivationRecoveryPolicy::try_from(recovery_policy)
        .unwrap_or(ActivationRecoveryPolicy::Unspecified);
    if failed.retryable
        && matches!(
            policy,
            ActivationRecoveryPolicy::IdempotentRetry | ActivationRecoveryPolicy::DurableSteps
        )
    {
        ActivationStatus::RetryReady
    } else if force_unknown || certainty == ActivationExternalOutcomeCertainty::Unknown {
        ActivationStatus::UnknownOutcome
    } else {
        ActivationStatus::Failed
    }
}

fn require_state(
    state: Option<ActivationState>,
    activation_id: &str,
) -> Result<ActivationState, ActivationError> {
    state.ok_or_else(|| ActivationError::IllegalTransition {
        activation_id: activation_id.to_string(),
        reason: "activation aggregate does not exist".to_string(),
    })
}

pub fn state_from_snapshot(
    snapshot: &ActivationSnapshotRecord,
) -> Result<ActivationState, ActivationError> {
    let current = snapshot.current_attempt.as_ref().ok_or_else(|| {
        ActivationError::InvalidArgument(
            "activation snapshot is missing current_attempt".to_string(),
        )
    })?;
    validate_started(current)?;
    if snapshot.project_id != current.project_id
        || snapshot.run_id != current.run_id
        || snapshot.activation_id != current.activation_id
    {
        return Err(ActivationError::InvalidArgument(
            "activation snapshot identity does not match current_attempt".to_string(),
        ));
    }
    let status =
        ActivationStatus::try_from(snapshot.status).unwrap_or(ActivationStatus::Unspecified);
    if status == ActivationStatus::Unspecified {
        return Err(ActivationError::InvalidArgument(
            "activation snapshot status is required".to_string(),
        ));
    }
    if let Some(completion) = &snapshot.completion {
        validate_journal_record_payload(&journal_record(Event::Completed(completion.clone())))?;
        if completion.project_id != snapshot.project_id
            || completion.run_id != snapshot.run_id
            || completion.activation_id != snapshot.activation_id
            || completion.attempt != current.attempt
            || completion.fence_token != current.fence_token
        {
            return Err(ActivationError::InvalidArgument(
                "activation snapshot completion authority does not match current_attempt"
                    .to_string(),
            ));
        }
    }
    if let Some(failure) = &snapshot.failure {
        validate_failed_record(failure)?;
        if failure.project_id != snapshot.project_id
            || failure.run_id != snapshot.run_id
            || failure.activation_id != snapshot.activation_id
            || failure.attempt != current.attempt
            || failure.fence_token != current.fence_token
        {
            return Err(ActivationError::InvalidArgument(
                "activation snapshot failure authority does not match current_attempt".to_string(),
            ));
        }
    }
    if let Some(suspension) = &snapshot.suspension {
        validate_journal_record_payload(&journal_record(Event::Suspended(suspension.clone())))?;
        if suspension.project_id != snapshot.project_id
            || suspension.run_id != snapshot.run_id
            || suspension.activation_id != snapshot.activation_id
            || suspension.attempt != current.attempt
            || suspension.fence_token != current.fence_token
        {
            return Err(ActivationError::InvalidArgument(
                "activation snapshot suspension authority does not match current_attempt"
                    .to_string(),
            ));
        }
    }
    if let Some(cancellation) = &snapshot.cancellation {
        validate_journal_record_payload(&journal_record(Event::Cancelled(cancellation.clone())))?;
        if cancellation.project_id != snapshot.project_id
            || cancellation.run_id != snapshot.run_id
            || cancellation.activation_id != snapshot.activation_id
            || cancellation.attempt != current.attempt
        {
            return Err(ActivationError::InvalidArgument(
                "activation snapshot cancellation attempt does not match current_attempt"
                    .to_string(),
            ));
        }
    }
    match status {
        ActivationStatus::Completed if snapshot.completion.is_none() => {
            return Err(ActivationError::InvalidArgument(
                "completed activation snapshot is missing completion".to_string(),
            ));
        }
        ActivationStatus::RetryReady
        | ActivationStatus::Failed
        | ActivationStatus::UnknownOutcome
            if snapshot.failure.is_none() =>
        {
            return Err(ActivationError::InvalidArgument(
                "failed activation snapshot is missing failure".to_string(),
            ));
        }
        ActivationStatus::Suspended if snapshot.suspension.is_none() => {
            return Err(ActivationError::InvalidArgument(
                "suspended activation snapshot is missing suspension".to_string(),
            ));
        }
        ActivationStatus::Cancelled if snapshot.cancellation.is_none() => {
            return Err(ActivationError::InvalidArgument(
                "cancelled activation snapshot is missing cancellation".to_string(),
            ));
        }
        ActivationStatus::Active
            if snapshot.completion.is_some()
                || snapshot.failure.is_some()
                || snapshot.suspension.is_some()
                || snapshot.cancellation.is_some() =>
        {
            return Err(ActivationError::InvalidArgument(
                "nonterminal activation snapshot contains a terminal receipt".to_string(),
            ));
        }
        ActivationStatus::Completed
            if snapshot.failure.is_some()
                || snapshot.suspension.is_some()
                || snapshot.cancellation.is_some() =>
        {
            return Err(ActivationError::InvalidArgument(
                "completed activation snapshot contains another decision receipt".to_string(),
            ));
        }
        ActivationStatus::RetryReady
        | ActivationStatus::Failed
        | ActivationStatus::UnknownOutcome
            if snapshot.completion.is_some()
                || snapshot.suspension.is_some()
                || snapshot.cancellation.is_some() =>
        {
            return Err(ActivationError::InvalidArgument(
                "failed activation snapshot contains another decision receipt".to_string(),
            ));
        }
        ActivationStatus::Suspended
            if snapshot.completion.is_some()
                || snapshot.failure.is_some()
                || snapshot.cancellation.is_some() =>
        {
            return Err(ActivationError::InvalidArgument(
                "suspended activation snapshot contains another decision receipt".to_string(),
            ));
        }
        ActivationStatus::Cancelled
            if snapshot.completion.is_some()
                || snapshot.failure.is_some()
                || snapshot.suspension.is_some() =>
        {
            return Err(ActivationError::InvalidArgument(
                "cancelled activation snapshot contains another decision receipt".to_string(),
            ));
        }
        _ => {}
    }
    if let Some(failure) = &snapshot.failure {
        let certainty =
            ActivationExternalOutcomeCertainty::try_from(failure.external_outcome_certainty)
                .unwrap_or(ActivationExternalOutcomeCertainty::Unspecified);
        let policy = ActivationRecoveryPolicy::try_from(current.recovery_policy)
            .unwrap_or(ActivationRecoveryPolicy::Unspecified);
        let safe_retry = failure.retryable
            && matches!(
                policy,
                ActivationRecoveryPolicy::IdempotentRetry | ActivationRecoveryPolicy::DurableSteps
            );
        let status_matches_failure = match status {
            ActivationStatus::RetryReady => safe_retry,
            ActivationStatus::Failed => {
                !safe_retry && certainty != ActivationExternalOutcomeCertainty::Unknown
            }
            ActivationStatus::UnknownOutcome => {
                certainty == ActivationExternalOutcomeCertainty::Unknown
            }
            _ => true,
        };
        if !status_matches_failure {
            return Err(ActivationError::InvalidArgument(
                "activation snapshot status does not match its failure receipt".to_string(),
            ));
        }
    }
    Ok(ActivationState {
        identity: ActivationIdentity::from_started(current),
        status,
        current_attempt: current.clone(),
        completion: snapshot.completion.clone(),
        failure: snapshot.failure.clone(),
        suspension: snapshot.suspension.clone(),
        cancellation: snapshot.cancellation.clone(),
        accepted_offset: Offset(snapshot.accepted_journal_offset),
        attempt_started_offset: Offset(snapshot.attempt_started_offset),
    })
}

fn write_replay(state: &ActivationState) -> WriteDecision {
    WriteDecision {
        accepted: true,
        replayed: true,
        activation_id: state.identity.activation_id.clone(),
        attempt: state.current_attempt.attempt,
        status: state.status,
        accepted_offset: Some(state.accepted_offset),
    }
}

#[cfg(test)]
mod tests {
    mod display_parent;
    use engine_proto::activation_payload::Value;
    use engine_proto::{ActivationPayload, BeginActivationRequest, CompleteActivationRequest};

    use super::*;

    fn digest(value: &[u8]) -> Vec<u8> {
        Sha256::digest(value).to_vec()
    }

    fn begin(worker: &str) -> BeginActivationCommand {
        BeginActivationCommand {
            request: BeginActivationRequest {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                parent_activation_id: "parent-1".to_string(),
                kind: ActivationKind::Step as i32,
                stable_key: "step/load".to_string(),
                input_digest: digest(b"input"),
                definition_digest: digest(b"definition"),
                recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                worker_session_id: worker.to_string(),
                run_authority: b"run-authority".to_vec(),
                lease_authority: b"lease-authority".to_vec(),
                child: None,
                display_name: "load".to_string(),
                input_data: br#"{"order_id":42}"#.to_vec(),
                display_parent_correlation_id: String::new(),
            },
            issued_fence_token: format!("fence-{worker}").into_bytes(),
        }
    }

    fn apply_transition(
        mut state: Option<ActivationState>,
        transition: &ActivationTransition,
        first_offset: u64,
    ) -> ActivationState {
        for (index, record) in transition.records.iter().enumerate() {
            state = Some(
                apply_journal_record(state, record, Offset(first_offset + index as u64)).unwrap(),
            );
        }
        state.unwrap()
    }

    fn complete(state: &ActivationState, output: &[u8]) -> CompleteActivationCommand {
        let payload = ActivationPayload {
            value: Some(Value::InlineData(output.to_vec())),
        };
        CompleteActivationCommand {
            request: CompleteActivationRequest {
                project_id: state.identity.project_id.clone(),
                run_id: state.identity.run_id.clone(),
                activation_id: state.identity.activation_id.clone(),
                attempt: state.current_attempt.attempt,
                fence_token: state.current_attempt.fence_token.clone(),
                output_digest: payload_digest(&payload).unwrap(),
                output: Some(payload),
                state_mutations: Vec::new(),
                outbox_intents: Vec::new(),
                usage: None,
                evidence: Vec::new(),
            },
        }
    }

    fn suspend(state: &ActivationState, resume_source: &str) -> SuspendActivationCommand {
        SuspendActivationCommand {
            project_id: state.identity.project_id.clone(),
            run_id: state.identity.run_id.clone(),
            activation_id: state.identity.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            fence_token: state.current_attempt.fence_token.clone(),
            resume_source: resume_source.to_string(),
        }
    }

    fn timer_suspend(state: &ActivationState, timer_key: &str) -> SuspendActivationRequest {
        SuspendActivationRequest {
            project_id: state.identity.project_id.clone(),
            run_id: state.identity.run_id.clone(),
            activation_id: state.identity.activation_id.clone(),
            attempt: state.current_attempt.attempt,
            fence_token: state.current_attempt.fence_token.clone(),
            timer_key: timer_key.to_string(),
            ready_at_ms: 4_200,
            input_digest: state.identity.input_digest.clone(),
            definition_digest: state.identity.definition_digest.clone(),
            continuation: br#"{"step":2}"#.to_vec(),
            delay_ms: 0,
        }
    }

    #[test]
    fn begin_is_execute_then_wait_for_another_worker() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let state = apply_transition(None, &first, 7);
        let second = decide_begin(Some(&state), &begin("worker-b")).unwrap();
        let ActivationDecision::Begin(decision) = second.decision else {
            panic!("expected begin decision");
        };
        assert_eq!(decision.outcome, BeginActivationOutcome::Wait);
        assert!(decision.fence_token.is_empty());
        assert_eq!(decision.accepted_offset, Some(Offset(7)));
    }

    #[test]
    fn child_linkage_is_required_immutable_spawn_identity() {
        let mut child = begin("worker-a");
        child.request.kind = ActivationKind::Child as i32;
        child.request.stable_key = "child:researcher:0".to_string();
        child.request.definition_digest = digest(b"researcher-definition");

        assert!(matches!(
            decide_begin(None, &child),
            Err(ActivationError::InvalidArgument(message))
                if message == "child activation linkage is required"
        ));

        child.request.child = Some(ChildActivationLinkage {
            child_key: child.request.stable_key.clone(),
            child_run_id: "child-run-1".to_string(),
            child_session_id: "child-session-1".to_string(),
            child_definition_digest: child.request.definition_digest.clone(),
            join_policy: ChildJoinPolicy::Required as i32,
        });
        let first = decide_begin(None, &child).unwrap();
        let active = apply_transition(None, &first, 1);
        assert_eq!(active.identity.child, child.request.child);

        let mut changed = child.clone();
        changed.request.child.as_mut().unwrap().child_run_id = "child-run-2".to_string();
        let transition = decide_begin(Some(&active), &changed).unwrap();
        assert!(transition.records.is_empty());
        let ActivationDecision::Begin(decision) = transition.decision else {
            panic!("expected begin decision");
        };
        assert_eq!(decision.outcome, BeginActivationOutcome::Conflict);

        let mut non_child = begin("worker-b");
        non_child.request.child = child.request.child;
        assert!(matches!(
            decide_begin(None, &non_child),
            Err(ActivationError::InvalidArgument(message))
                if message == "child activation linkage is only valid for child activations"
        ));
    }

    #[test]
    fn different_digest_is_a_non_mutating_conflict() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let state = apply_transition(None, &first, 1);
        let mut changed = begin("worker-a");
        changed.request.input_digest = digest(b"different");
        let transition = decide_begin(Some(&state), &changed).unwrap();
        assert!(transition.records.is_empty());
        let ActivationDecision::Begin(decision) = transition.decision else {
            panic!("expected begin decision");
        };
        assert_eq!(decision.outcome, BeginActivationOutcome::Conflict);
        assert!(decision.conflict.is_some());
    }

    #[test]
    fn completion_replays_without_a_projection_and_rejects_stale_fence() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let command = complete(&active, b"result");
        let completion = decide_complete(&active, &command).unwrap();
        let completed = apply_transition(Some(active.clone()), &completion, 2);

        let exact_retry = decide_complete(&completed, &command).unwrap();
        let ActivationDecision::Complete(replay) = exact_retry.decision else {
            panic!("expected completion decision");
        };
        assert!(replay.replayed);
        assert_eq!(replay.accepted_offset, Some(Offset(2)));

        let begin_replay = decide_begin(Some(&completed), &begin("worker-b")).unwrap();
        let ActivationDecision::Begin(replay) = begin_replay.decision else {
            panic!("expected begin decision");
        };
        assert_eq!(replay.outcome, BeginActivationOutcome::Replay);
        assert_eq!(replay.replay_result, command.request.output);

        let mut stale = command.clone();
        stale.request.fence_token = b"stale".to_vec();
        assert!(matches!(
            decide_complete(&active, &stale),
            Err(ActivationError::StaleAuthority { .. })
        ));
    }

    #[test]
    fn retryable_failure_increments_attempt_and_fence() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let old_owner_completion = complete(&active, b"late-result");
        let fail = FailActivationCommand {
            request: FailActivationRequest {
                project_id: active.identity.project_id.clone(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: active.current_attempt.fence_token.clone(),
                error_code: "TRANSIENT".to_string(),
                error_data: None,
                retryable: true,
                external_outcome_certainty: ActivationExternalOutcomeCertainty::NoEffect as i32,
                evidence: Vec::new(),
                latency_ms: 12,
            },
        };
        let failed = decide_fail(&active, &fail).unwrap();
        let retry_ready = apply_transition(Some(active), &failed, 2);
        assert_eq!(retry_ready.status, ActivationStatus::RetryReady);
        let cancellation = decide_cancel(
            &retry_ready,
            &CancelActivationCommand {
                project_id: retry_ready.identity.project_id.clone(),
                run_id: retry_ready.identity.run_id.clone(),
                activation_id: retry_ready.identity.activation_id.clone(),
                reason: "cancel between attempts".to_string(),
            },
        )
        .unwrap();
        let cancelled = apply_transition(Some(retry_ready.clone()), &cancellation, 3);
        assert_eq!(cancelled.status, ActivationStatus::Cancelled);

        let retry = decide_begin(Some(&retry_ready), &begin("worker-b")).unwrap();
        let retried = apply_transition(Some(retry_ready), &retry, 4);
        assert_eq!(retried.current_attempt.attempt, 2);
        assert_eq!(retried.current_attempt.previous_attempt, 1);
        assert_eq!(retried.current_attempt.fence_token, b"fence-worker-b");
        assert!(matches!(
            decide_complete(&retried, &old_owner_completion),
            Err(ActivationError::StaleAuthority { .. })
        ));
    }

    #[test]
    fn retryable_failures_obey_the_declared_recovery_policy() {
        let cases = [
            (
                ActivationRecoveryPolicy::IdempotentRetry,
                ActivationStatus::RetryReady,
            ),
            (
                ActivationRecoveryPolicy::DurableSteps,
                ActivationStatus::RetryReady,
            ),
            (
                ActivationRecoveryPolicy::UnknownOutcome,
                ActivationStatus::Failed,
            ),
            (
                ActivationRecoveryPolicy::Compensate,
                ActivationStatus::Failed,
            ),
            (ActivationRecoveryPolicy::Fail, ActivationStatus::Failed),
        ];

        for (index, (policy, expected)) in cases.into_iter().enumerate() {
            let mut begin = begin(&format!("worker-{index}"));
            begin.request.recovery_policy = policy as i32;
            let started = decide_begin(None, &begin).unwrap();
            let active = apply_transition(None, &started, 10 + index as u64 * 2);
            let failure = FailActivationCommand {
                request: FailActivationRequest {
                    project_id: active.identity.project_id.clone(),
                    run_id: active.identity.run_id.clone(),
                    activation_id: active.identity.activation_id.clone(),
                    attempt: active.current_attempt.attempt,
                    fence_token: active.current_attempt.fence_token.clone(),
                    error_code: "TRANSIENT".to_string(),
                    error_data: None,
                    retryable: true,
                    external_outcome_certainty: ActivationExternalOutcomeCertainty::NoEffect as i32,
                    evidence: Vec::new(),
                    latency_ms: 0,
                },
            };
            let transition = decide_fail(&active, &failure).unwrap();
            let failed = apply_transition(Some(active), &transition, 11 + index as u64 * 2);
            assert_eq!(failed.status, expected, "policy {policy:?}");
            let recovered = state_from_snapshot(&super::snapshot(&failed)).unwrap();
            assert_eq!(recovered.status, expected, "snapshot policy {policy:?}");
        }
    }

    #[test]
    fn uncertain_external_effect_requires_an_explicit_safe_retry_policy() {
        let cases = [
            (
                ActivationRecoveryPolicy::IdempotentRetry,
                ActivationStatus::RetryReady,
            ),
            (
                ActivationRecoveryPolicy::DurableSteps,
                ActivationStatus::RetryReady,
            ),
            (
                ActivationRecoveryPolicy::UnknownOutcome,
                ActivationStatus::UnknownOutcome,
            ),
            (
                ActivationRecoveryPolicy::Compensate,
                ActivationStatus::UnknownOutcome,
            ),
            (
                ActivationRecoveryPolicy::Fail,
                ActivationStatus::UnknownOutcome,
            ),
        ];

        for (index, (policy, expected)) in cases.into_iter().enumerate() {
            let mut begin = begin(&format!("worker-unknown-{index}"));
            begin.request.recovery_policy = policy as i32;
            let started = decide_begin(None, &begin).unwrap();
            let active = apply_transition(None, &started, 30 + index as u64 * 2);
            let transition = decide_fail(
                &active,
                &FailActivationCommand {
                    request: FailActivationRequest {
                        project_id: active.identity.project_id.clone(),
                        run_id: active.identity.run_id.clone(),
                        activation_id: active.identity.activation_id.clone(),
                        attempt: active.current_attempt.attempt,
                        fence_token: active.current_attempt.fence_token.clone(),
                        error_code: "EFFECT_UNCERTAIN".to_string(),
                        error_data: None,
                        retryable: true,
                        external_outcome_certainty: ActivationExternalOutcomeCertainty::Unknown
                            as i32,
                        evidence: Vec::new(),
                        latency_ms: 0,
                    },
                },
            )
            .unwrap();
            let failed = apply_transition(Some(active), &transition, 31 + index as u64 * 2);
            assert_eq!(failed.status, expected, "policy {policy:?}");
            let recovered = state_from_snapshot(&super::snapshot(&failed)).unwrap();
            assert_eq!(recovered.status, expected, "snapshot policy {policy:?}");
        }
    }

    #[test]
    fn suspension_cancellation_and_unknown_outcome_follow_terminal_matrix() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let suspended = decide_suspend(&active, &suspend(&active, "approval:42")).unwrap();
        let suspended = apply_transition(Some(active), &suspended, 2);
        assert_eq!(suspended.status, ActivationStatus::Suspended);
        let exact_suspend = suspend(&suspended, "approval:42");
        assert!(decide_suspend(&suspended, &exact_suspend)
            .unwrap()
            .records
            .is_empty());
        let mut conflicting_suspend = exact_suspend;
        conflicting_suspend.resume_source = "approval:43".to_string();
        assert!(matches!(
            decide_suspend(&suspended, &conflicting_suspend),
            Err(ActivationError::PayloadConflict { .. })
        ));
        let ActivationDecision::Begin(wait) = decide_begin(Some(&suspended), &begin("worker-b"))
            .unwrap()
            .decision
        else {
            panic!("expected wait decision")
        };
        assert_eq!(wait.outcome, BeginActivationOutcome::Wait);

        let cancellation = CancelActivationCommand {
            project_id: suspended.identity.project_id.clone(),
            run_id: suspended.identity.run_id.clone(),
            activation_id: suspended.identity.activation_id.clone(),
            reason: "parent cancelled".to_string(),
        };
        let cancelled = decide_cancel(&suspended, &cancellation).unwrap();
        let cancelled = apply_transition(Some(suspended), &cancelled, 3);
        assert_eq!(cancelled.status, ActivationStatus::Cancelled);
        let replayed_cancel = decide_cancel(&cancelled, &cancellation).unwrap();
        assert!(replayed_cancel.records.is_empty());
        let mut conflicting_cancel = cancellation.clone();
        conflicting_cancel.reason = "different reason".to_string();
        assert!(matches!(
            decide_cancel(&cancelled, &conflicting_cancel),
            Err(ActivationError::PayloadConflict { .. })
        ));
        let ActivationDecision::Begin(cancelled_begin) =
            decide_begin(Some(&cancelled), &begin("worker-c"))
                .unwrap()
                .decision
        else {
            panic!("expected cancelled begin")
        };
        assert_eq!(cancelled_begin.outcome, BeginActivationOutcome::Cancelled);

        let mut unknown_begin = begin("worker-u");
        unknown_begin.request.recovery_policy = ActivationRecoveryPolicy::UnknownOutcome as i32;
        let unknown_start = decide_begin(None, &unknown_begin).unwrap();
        let unknown_active = apply_transition(None, &unknown_start, 10);
        let unknown = decide_fail(
            &unknown_active,
            &FailActivationCommand {
                request: FailActivationRequest {
                    project_id: unknown_active.identity.project_id.clone(),
                    run_id: unknown_active.identity.run_id.clone(),
                    activation_id: unknown_active.identity.activation_id.clone(),
                    attempt: unknown_active.current_attempt.attempt,
                    fence_token: unknown_active.current_attempt.fence_token.clone(),
                    error_code: "EFFECT_UNCERTAIN".to_string(),
                    error_data: None,
                    retryable: true,
                    external_outcome_certainty: ActivationExternalOutcomeCertainty::Unknown as i32,
                    evidence: Vec::new(),
                    latency_ms: 0,
                },
            },
        )
        .unwrap();
        let unknown = apply_transition(Some(unknown_active), &unknown, 11);
        assert_eq!(unknown.status, ActivationStatus::UnknownOutcome);
        let mut unknown_retry = begin("worker-u2");
        unknown_retry.request.recovery_policy = ActivationRecoveryPolicy::UnknownOutcome as i32;
        let ActivationDecision::Begin(unknown_begin) = decide_begin(Some(&unknown), &unknown_retry)
            .unwrap()
            .decision
        else {
            panic!("expected unknown begin")
        };
        assert_eq!(
            unknown_begin.outcome,
            BeginActivationOutcome::UnknownOutcome
        );
        assert!(matches!(
            decide_cancel(
                &unknown,
                &CancelActivationCommand {
                    project_id: unknown.identity.project_id.clone(),
                    run_id: unknown.identity.run_id.clone(),
                    activation_id: unknown.identity.activation_id.clone(),
                    reason: "too late".to_string(),
                }
            ),
            Err(ActivationError::IllegalTransition { .. })
        ));
    }

    #[test]
    fn timer_suspension_is_generation_bound_and_replay_safe() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let request = timer_suspend(&active, "sleep:42");
        let transition = decide_timer_suspend(&active, &request).unwrap();
        let suspended = apply_transition(Some(active), &transition, 2);
        let canonical = suspended.suspension.as_ref().unwrap();
        assert_eq!(canonical.timer_key, request.timer_key);
        assert_eq!(
            canonical.resume_source,
            format!("timer:{}", suspension_timer_id(&request))
        );
        assert!(decide_timer_suspend(&suspended, &request)
            .unwrap()
            .records
            .is_empty());

        let mut changed = request;
        changed.ready_at_ms += 1;
        assert!(matches!(
            decide_timer_suspend(&suspended, &changed),
            Err(ActivationError::PayloadConflict { .. })
        ));
    }

    #[test]
    fn activation_kind_is_part_of_logical_identity() {
        assert_ne!(
            activation_id(
                "project",
                "run",
                "parent",
                ActivationKind::Step as i32,
                "key"
            ),
            activation_id(
                "project",
                "run",
                "parent",
                ActivationKind::Tool as i32,
                "key"
            )
        );
    }

    #[test]
    fn snapshot_plus_suffix_matches_uninterrupted_fold() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 10);
        let snapshot = snapshot_record(&active);
        let completion = decide_complete(&active, &complete(&active, b"ok")).unwrap();
        let uninterrupted = apply_transition(Some(active), &completion, 11);

        let restored = apply_journal_record(None, &snapshot, Offset(999)).unwrap();
        let restored = apply_transition(Some(restored), &completion, 11);
        assert_eq!(restored, uninterrupted);

        let mut malformed = super::snapshot(&uninterrupted);
        malformed
            .completion
            .as_mut()
            .expect("completed snapshot receipt")
            .fence_token = b"different-fence".to_vec();
        assert!(matches!(
            state_from_snapshot(&malformed),
            Err(ActivationError::InvalidArgument(_))
        ));
    }

    #[test]
    fn engine_record_round_trip_uses_canonical_json_and_kind_names() {
        let transition = decide_begin(None, &begin("worker-a")).unwrap();
        let journal = &transition.records[0];
        let record = to_engine_record(journal, 123).unwrap();
        assert_eq!(from_engine_record(&record).unwrap(), Some(journal.clone()));
        assert_eq!(record.event_type, "workflow.step.started");
        assert_eq!(record.data_type, "json");
        assert_eq!(record.step_key.as_deref(), Some("step/load"));
        assert_eq!(
            record.correlation_id.as_deref(),
            Some(journal_activation_id(journal).as_str())
        );
        assert_eq!(record.parent_event_id.as_deref(), Some("parent-1"));
        assert!(has_activation_marker(&record));
        assert_eq!(
            activation_record_identity(&record)
                .map(|(_, attempt, transition)| (attempt, transition)),
            Some((1, TRANSITION_STARTED))
        );
        assert!(!record.data.contains(&b' '), "no whitespace");

        let mut malformed_envelopes = Vec::new();
        let mut missing_kind = record.clone();
        missing_kind.metadata.remove(META_ACTIVATION_KIND);
        malformed_envelopes.push(missing_kind);
        let mut wrong_correlation = record.clone();
        wrong_correlation.correlation_id = Some("another-activation".to_string());
        malformed_envelopes.push(wrong_correlation);
        let mut missing_parent = record.clone();
        missing_parent.parent_event_id = None;
        malformed_envelopes.push(missing_parent);
        let mut wrong_step = record.clone();
        wrong_step.step_key = Some("another-step".to_string());
        malformed_envelopes.push(wrong_step);
        let mut wrong_data_type = record.clone();
        wrong_data_type.data_type = "bytes".to_string();
        malformed_envelopes.push(wrong_data_type);
        let mut missing_checksum = record.clone();
        missing_checksum.data_checksum = None;
        malformed_envelopes.push(missing_checksum);
        let mut compressed = record.clone();
        compressed.data_compressed = true;
        malformed_envelopes.push(compressed);
        for malformed in malformed_envelopes {
            assert!(
                from_engine_record(&malformed).is_err(),
                "every marked record requires the complete canonical envelope"
            );
        }

        // Same payload, non-canonical bytes: rejected.
        let mut pretty = record.clone();
        pretty.data = bytes::Bytes::from(
            serde_json::to_vec_pretty(
                &serde_json::from_slice::<serde_json::Value>(&record.data).unwrap(),
            )
            .unwrap(),
        );
        assert!(from_engine_record(&pretty).is_err());
        // Marker without a payload: rejected, never "not an activation".
        let mut forged = record.clone();
        forged.data = bytes::Bytes::from_static(b"{}");
        assert!(from_engine_record(&forged).is_err());
        assert!(is_activation_record(&forged).is_err());
        // No marker: not an activation record, whatever the name says.
        let mut plain = record.clone();
        plain.metadata.remove(META_ACTIVATION_CONTRACT);
        assert_eq!(from_engine_record(&plain).unwrap(), None);
    }

    #[test]
    fn activation_usage_rejects_negative_cached_tokens() {
        let started = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &started, 10);
        let mut completion = complete(&active, b"ok");
        completion.request.usage = Some(engine_proto::ActivationUsage {
            cached_tokens: -1,
            ..Default::default()
        });
        assert!(matches!(
            decide_complete(&active, &completion),
            Err(ActivationError::InvalidArgument(message))
                if message == "activation usage counters must be non-negative"
        ));
    }

    #[test]
    fn suspension_json_uses_the_contracted_continuation_b64_field() {
        let started = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &started, 10);
        let transition = decide_timer_suspend(&active, &timer_suspend(&active, "timer-1"))
            .expect("timer suspension");
        let record = to_engine_record(&transition.records[0], 123).expect("engine record");
        let event =
            serde_json::from_slice::<serde_json::Value>(&record.data).unwrap()["event"].clone();
        assert!(event.get("continuation_b64").is_some());
        assert!(event.get("continuation").is_none());
    }

    fn journal_activation_id(record: &ActivationJournalRecord) -> String {
        match record.event.as_ref().unwrap() {
            Event::Started(started) => started.activation_id.clone(),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn every_live_kind_is_named_after_its_boundary() {
        let expected = [
            (ActivationKind::Step, "workflow.step"),
            (ActivationKind::Timer, "workflow.step"),
            (ActivationKind::Model, "lm"),
            (ActivationKind::Tool, "tool_call"),
            (ActivationKind::Child, "agent"),
        ];
        for (kind, prefix) in expected {
            assert_eq!(event_prefix(kind as i32).unwrap(), prefix);
        }
        assert!(event_prefix(ActivationKind::Unspecified as i32).is_err());
        let names = all_event_types();
        assert_eq!(names.len(), 28);
        assert!(names.contains(&"workflow.step.suspended".to_string()));
        assert!(names.contains(&"tool_call.unknown_outcome".to_string()));

        let mut tool = begin("worker-a");
        tool.request.kind = ActivationKind::Tool as i32;
        tool.request.display_name = "search".to_string();
        let started = decide_begin(None, &tool).unwrap();
        let active = apply_transition(None, &started, 1);
        let record = to_engine_record(&started.records[0], 1).unwrap();
        assert_eq!(record.event_type, "tool_call.started");
        let completion = decide_complete(&active, &complete(&active, b"{\"hits\":3}")).unwrap();
        let record = to_engine_record(&completion.records[0], 2).unwrap();
        assert_eq!(record.event_type, "tool_call.completed");
        assert_eq!(record.step_key.as_deref(), Some("step/load"));
        let view = public_view(&record);
        assert_eq!(view["name"], "search");
        assert_eq!(view["transition"], "completed");
        assert_eq!(view["output_data"]["hits"], 3);
        assert_eq!(view["started_journal_offset"], 1);
        assert!(view.get("fence_token").is_none());
        assert!(view.get("output").is_none());
    }

    #[test]
    fn an_uncertain_failure_is_an_ordinary_failure_when_the_policy_retries_it() {
        let uncertain_failure = |active: &ActivationState| FailActivationCommand {
            request: FailActivationRequest {
                project_id: active.identity.project_id.clone(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: active.current_attempt.fence_token.clone(),
                error_code: "TRANSIENT".to_string(),
                error_data: None,
                retryable: true,
                external_outcome_certainty: ActivationExternalOutcomeCertainty::Unknown as i32,
                evidence: Vec::new(),
                latency_ms: 0,
            },
        };

        // Idempotent retry: the next attempt runs, so the record reads as a failed attempt.
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let retried = decide_fail(&active, &uncertain_failure(&active)).unwrap();
        let record = to_engine_record(&retried.records[0], 2).unwrap();
        assert_eq!(record.event_type, "workflow.step.failed");
        assert_eq!(
            apply_transition(Some(active), &retried, 2).status,
            ActivationStatus::RetryReady
        );

        // Unknown-outcome policy: the run stops here, so the uncertainty is recorded.
        let mut command = begin("worker-a");
        command.request.recovery_policy = ActivationRecoveryPolicy::UnknownOutcome as i32;
        let first = decide_begin(None, &command).unwrap();
        let active = apply_transition(None, &first, 1);
        let ended = decide_fail(&active, &uncertain_failure(&active)).unwrap();
        let record = to_engine_record(&ended.records[0], 2).unwrap();
        assert_eq!(record.event_type, "workflow.step.unknown_outcome");
        assert_eq!(
            apply_transition(Some(active), &ended, 2).status,
            ActivationStatus::UnknownOutcome
        );
    }

    #[test]
    fn retry_started_keeps_the_started_name_with_a_retry_transition() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let fail = FailActivationCommand {
            request: FailActivationRequest {
                project_id: active.identity.project_id.clone(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: active.current_attempt.fence_token.clone(),
                error_code: "TRANSIENT".to_string(),
                error_data: Some(ActivationPayload {
                    value: Some(Value::InlineData(br#"{"message":"boom"}"#.to_vec())),
                }),
                retryable: true,
                external_outcome_certainty: ActivationExternalOutcomeCertainty::NoEffect as i32,
                evidence: Vec::new(),
                latency_ms: 7,
            },
        };
        let failed = decide_fail(&active, &fail).unwrap();
        let failed_record = to_engine_record(&failed.records[0], 2).unwrap();
        assert_eq!(failed_record.event_type, "workflow.step.failed");
        let view = public_view(&failed_record);
        assert_eq!(view["error_message"], "boom");
        assert_eq!(view["error_data"]["message"], "boom");
        assert_eq!(view["duration_ms"], 7);
        let retry_ready = apply_transition(Some(active), &failed, 2);
        let retry = decide_begin(Some(&retry_ready), &begin("worker-b")).unwrap();
        let record = to_engine_record(&retry.records[0], 3).unwrap();
        assert_eq!(record.event_type, "workflow.step.started");
        assert_eq!(
            activation_record_identity(&record)
                .map(|(_, attempt, transition)| (attempt, transition)),
            Some((2, TRANSITION_RETRY_STARTED))
        );
        assert_eq!(public_view(&record)["previous_attempt"], 1);
        assert!(public_view(&record).get("worker_session_id").is_none());
    }

    #[test]
    fn settling_records_must_describe_the_current_attempt() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let completion = decide_complete(&active, &complete(&active, b"ok")).unwrap();
        let Event::Completed(mut completed) = completion.records[0].event.clone().unwrap() else {
            panic!("expected completion");
        };
        completed.header.name = "renamed".to_string();
        assert!(matches!(
            apply_journal_record(
                Some(active),
                &journal_record(Event::Completed(completed)),
                Offset(2)
            ),
            Err(ActivationError::InvalidArgument(_))
        ));
    }

    #[test]
    fn exact_retry_of_a_settling_record_is_byte_identical() {
        let first = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &first, 1);
        let command = complete(&active, b"ok");
        let one = decide_complete(&active, &command).unwrap();
        let two = decide_complete(&active, &command).unwrap();
        assert_eq!(
            encode_journal_record(&one.records[0]),
            encode_journal_record(&two.records[0])
        );
        let record = to_engine_record(&one.records[0], 5).unwrap();
        assert!(has_activation_marker(&record));
    }

    #[test]
    fn activation_golden_fixture_is_stable() {
        let started = decide_begin(None, &begin("worker-a")).unwrap();
        let active = apply_transition(None, &started, 7);
        let mut completion = complete(&active, b"result");
        completion.request.state_mutations = vec![
            ActivationStateMutation {
                operation_id: "state-z".to_string(),
                entity_type: "workflow".to_string(),
                entity_key: "z".to_string(),
                scope: "run".to_string(),
                scope_id: "run-1".to_string(),
                data: b"z-value".to_vec(),
                data_type: "json".to_string(),
                expected_version: 0,
            },
            ActivationStateMutation {
                operation_id: "state-a".to_string(),
                entity_type: "workflow".to_string(),
                entity_key: "a".to_string(),
                scope: "run".to_string(),
                scope_id: "run-1".to_string(),
                data: b"a-value".to_vec(),
                data_type: "json".to_string(),
                expected_version: 0,
            },
        ];
        completion.request.outbox_intents = vec![
            ActivationOutboxIntent {
                intent_id: "intent-z".to_string(),
                kind: "http".to_string(),
                idempotency_key: "downstream-z".to_string(),
                payload: Some(ActivationPayload {
                    value: Some(Value::InlineData(b"z-effect".to_vec())),
                }),
            },
            ActivationOutboxIntent {
                intent_id: "intent-a".to_string(),
                kind: "http".to_string(),
                idempotency_key: "downstream-a".to_string(),
                payload: Some(ActivationPayload {
                    value: Some(Value::InlineData(b"a-effect".to_vec())),
                }),
            },
        ];
        let completed = decide_complete(&active, &completion).unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/contracts/fixtures/durable-activation-v1.json"
        )))
        .unwrap();
        assert_eq!(
            fixture["identity"]["activation_id"].as_str(),
            Some(active.identity.activation_id.as_str())
        );
        assert_eq!(
            fixture["identity"]["input_digest_base64"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&active.identity.input_digest)
                    .as_str()
            )
        );
        assert_eq!(
            fixture["identity"]["definition_digest_base64"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(&active.identity.definition_digest)
                    .as_str()
            )
        );
        let canonical_input = fixture["digest_vectors"]["canonical_input_utf8"]
            .as_str()
            .unwrap();
        assert_eq!(
            fixture["digest_vectors"]["input_sha256_base64"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(Sha256::digest(canonical_input.as_bytes()))
                    .as_str()
            )
        );
        let artifact_digest = Sha256::digest(b"artifact-v1").to_vec();
        let mut definition = Sha256::new();
        definition.update(b"agnt5.activation.definition.v1\0");
        for value in [
            artifact_digest.as_slice(),
            b"workflow",
            b"v1",
            DURABLE_ACTIVATION_V1_CAPABILITY.as_bytes(),
            b"[\"object\",[]]",
        ] {
            definition.update((value.len() as u64).to_be_bytes());
            definition.update(value);
        }
        assert_eq!(
            fixture["digest_vectors"]["definition_sha256_base64"].as_str(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(definition.finalize())
                    .as_str()
            )
        );
        assert_eq!(
            fixture["canonical_records_json"]["started"].as_str(),
            Some(
                String::from_utf8(encode_journal_record(&started.records[0]))
                    .unwrap()
                    .as_str()
            )
        );
        assert_eq!(
            fixture["canonical_records_json"]["completed"].as_str(),
            Some(
                String::from_utf8(encode_journal_record(&completed.records[0]))
                    .unwrap()
                    .as_str()
            )
        );
        assert_eq!(
            fixture["record_names"]["STEP"].as_str(),
            Some(event_prefix(ActivationKind::Step as i32).unwrap())
        );
        let Event::Completed(completed) = completed.records[0].event.as_ref().unwrap() else {
            panic!("expected completion fixture")
        };
        assert_eq!(
            completed
                .state_mutations
                .iter()
                .map(|mutation| mutation.operation_id.as_str())
                .collect::<Vec<_>>(),
            ["state-a", "state-z"]
        );
        assert_eq!(
            completed
                .outbox_intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            ["intent-a", "intent-z"]
        );
    }
}
