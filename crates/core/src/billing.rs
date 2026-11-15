//! Which journal records bill operations, and how many.
//!
//! Billing counts straight from the journal, so these rules decide every
//! billed operation. The counts endpoint and the daily journal digest both
//! call them, which keeps the two in agreement.
//!
//! * A run start is the run's first dispatch record: its first
//!   `run.assigned`, or its first `run.started` when that lands first.
//! * A checkpoint is a durable activation's accepted completion, of any
//!   kind, one operation per started 64 KiB of its saved result.
//! * A timer bills one operation at its accepted suspension, because a timer
//!   never completes.
//!
//! Nothing else bills: replays, retries and failed attempts append no
//! completion, observed runs are skipped, and signals and approvals aren't
//! metered at launch. [`unmetered`] names the records those exclusions skip,
//! so a metric can size them.
//!
//! The daily journal digest names each billed record with an `op:` key and
//! sums the keys' hashes into a [`KeyDigest`].

use std::fmt::Write as _;

use engine_proto::activation_payload::Value;
use engine_proto::{ActivationPayload, Run};
use sha2::{Digest, Sha256};

use crate::activation::{
    self, has_activation_marker, Event, META_ACTIVATION_ID, META_ACTIVATION_TRANSITION,
    TRANSITION_COMPLETED, TRANSITION_SUSPENDED,
};
use crate::error::ActivationError;
use crate::journal_contract::is_observed_record;
use crate::types::Record;

/// A checkpoint bills one operation per started block of this many bytes of
/// its saved result.
pub const CHECKPOINT_OPERATION_BYTES: u64 = 64 * 1024;

/// The records that show a run was dispatched. The processor appends
/// `run.assigned` only after the worker has accepted the dispatch, so a fast
/// worker's `run.started` can land first.
const RUN_DISPATCH_EVENTS: &[&str] = &["run.assigned", "run.started"];

const SECONDS_PER_HOUR: i64 = 3_600;
const NANOS_PER_HOUR: i64 = SECONDS_PER_HOUR * 1_000_000_000;

/// The operations a checkpoint record bills, or `None` when it bills none.
///
/// Only a completion or a timer's suspension bills. The journal accepts one
/// completion per activation, and an activation suspends once, from `Active`,
/// so a record that bills is never billed twice. A record that carries the
/// durable activation marker but doesn't decode is an error: billing fails
/// rather than guesses.
pub fn checkpoint_operations(record: &Record) -> Result<Option<u64>, ActivationError> {
    if is_observed_record(record) {
        return Ok(None);
    }
    // Most records aren't billable; skip decoding them.
    if !matches!(
        record
            .metadata
            .get(META_ACTIVATION_TRANSITION)
            .map(String::as_str),
        Some(TRANSITION_COMPLETED | TRANSITION_SUSPENDED)
    ) {
        return Ok(None);
    }
    let Some(canonical) = activation::from_engine_record(record)? else {
        return Ok(None);
    };
    Ok(match canonical.event {
        Some(Event::Completed(completed)) => Some(result_operations(saved_result_bytes(
            completed.output.as_ref(),
        ))),
        // The SDKs mark a sleep done themselves once `timer.fired` resumes
        // the run, so the suspension is the only record a timer leaves. A
        // suspension without a timer key waits for a signal or an approval,
        // which aren't metered at launch.
        Some(Event::Suspended(suspended)) if !suspended.timer_key.is_empty() => Some(1),
        _ => None,
    })
}

/// One operation per started 64 KiB of saved result, and at least one.
pub fn result_operations(saved_result_bytes: u64) -> u64 {
    saved_result_bytes
        .div_ceil(CHECKPOINT_OPERATION_BYTES)
        .max(1)
}

fn saved_result_bytes(output: Option<&ActivationPayload>) -> u64 {
    match output.and_then(|payload| payload.value.as_ref()) {
        Some(Value::InlineData(bytes)) => bytes.len() as u64,
        Some(Value::Reference(reference)) => reference.size_bytes,
        None => 0,
    }
}

/// Whether a record could be a run start, so a counter knows to look up the
/// run's row before calling `is_run_start`.
pub fn is_dispatch_record(record: &Record) -> bool {
    RUN_DISPATCH_EVENTS.contains(&record.event_type.as_str()) && !is_observed_record(record)
}

/// Whether a record is the run start billing counts. Lease expiry, retries
/// and resume each dispatch the run again, so only the dispatch record at the
/// offset the run projection pinned in `first_dispatch_offset` bills. `run` is
/// the projected row for the record's run, and `record.offset` is the
/// partition-encoded offset the projection saw.
pub fn is_run_start(record: &Record, run: &Run) -> bool {
    is_dispatch_record(record) && run.first_dispatch_offset == Some(record.offset.0)
}

/// The start of the UTC hour a billable record falls in, in Unix seconds.
/// The runtime stamps `run.assigned` and every activation record with its own
/// clock, and the public append path rejects them, so a caller can't move
/// that usage into another hour. A start pinned at a worker's `run.started`
/// carries the worker's timestamp.
pub fn hour_start_unix(record: &Record) -> i64 {
    record.timestamp_ns.div_euclid(NANOS_PER_HOUR) * SECONDS_PER_HOUR
}

/// Whether a record might bill, judged from its envelope alone: a dispatch
/// record, or a durable activation's completion or suspension. It never
/// misses a record that bills, and it may include one that doesn't (a
/// re-dispatch, or a wait for a signal), so it suits a bound that must be
/// conservative and cheap.
pub fn may_bill(record: &Record) -> bool {
    is_dispatch_record(record)
        || (!is_observed_record(record)
            && has_activation_marker(record)
            && matches!(
                record
                    .metadata
                    .get(META_ACTIVATION_TRANSITION)
                    .map(String::as_str),
                Some(TRANSITION_COMPLETED | TRANSITION_SUSPENDED)
            ))
}

/// Whether the runtime stamped the record's `timestamp_ns` with its own
/// clock. It does for `run.assigned` and every durable activation record, and
/// the public append path refuses both. Any other record may carry a
/// caller's timestamp, which can be anything, so only these can say how far
/// the journal has got in time.
pub fn is_runtime_clocked(record: &Record) -> bool {
    !is_observed_record(record)
        && (record.event_type == "run.assigned" || has_activation_marker(record))
}

/// The digest key of a run start: `op:start:{project_id}:{run_id}`. A run
/// bills one start, so the key is unique.
pub fn start_key(record: &Record) -> String {
    format!("op:start:{}:{}", record.project_id, record.run_id)
}

/// The digest key of a checkpoint: `op:act:{project_id}:{run_id}:{activation_id}`.
/// An activation bills once, so the key is unique. Only call it for a record
/// [`checkpoint_operations`] bills: that check has already matched the
/// envelope's activation id against the canonical payload.
pub fn checkpoint_key(record: &Record) -> Option<String> {
    let activation_id = record.metadata.get(META_ACTIVATION_ID)?;
    Some(format!(
        "op:act:{}:{}:{activation_id}",
        record.project_id, record.run_id
    ))
}

/// An order-independent digest of a set of `op:` keys: the sum, modulo
/// 2^256, of each key's SHA-256 read as a big-endian number.
///
/// A digest over sorted keys would need every key of the day in memory at
/// once. A sum adds up page by page and partition by partition instead, so
/// a day read in any number of pages, on any replica, gives the same value,
/// and partitions' digests add up to the day's. Unlike XOR, a key counted
/// twice changes the sum rather than cancelling out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeyDigest([u8; 32]);

impl KeyDigest {
    /// Adds one key.
    pub fn add_key(&mut self, key: &str) {
        self.add(&KeyDigest(Sha256::digest(key.as_bytes()).into()));
    }

    /// Adds another digest, as when summing pages or partitions.
    pub fn add(&mut self, other: &KeyDigest) {
        let mut carry = 0u16;
        for (sum, addend) in self.0.iter_mut().zip(other.0.iter()).rev() {
            let total = u16::from(*sum) + u16::from(*addend) + carry;
            *sum = total as u8;
            carry = total >> 8;
        }
    }

    /// The digest as 64 lowercase hex digits.
    pub fn to_hex(&self) -> String {
        self.0
            .iter()
            .fold(String::with_capacity(64), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            })
    }
}

/// The event-type prefixes durable activations are journaled under
/// (`activation::event_prefix`). The SDKs also emit unmarked step records
/// under these names.
const STEP_PREFIXES: &[&str] = &[
    "workflow.step.",
    "function.",
    "lm.",
    "tool_call.",
    "agent.",
    "approval.",
    "eval.",
];

/// Why a record that looks like billable work bills nothing: one of the
/// exclusions billing makes at launch. The `agnt5.billing.unmetered_steps`
/// metric counts records by this, so each exclusion can be sized before it
/// is revisited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unmetered {
    /// A completed step in a run observed from OpenTelemetry traces.
    Observed,
    /// A step completion without the durable activation marker: a legacy
    /// `checkpoint.created`, or an SDK's or a workerless run's step record.
    Unmarked,
    /// A durable activation suspended without a timer, waiting for a signal
    /// or an approval.
    Wait,
    /// `signal.received`.
    Signal,
    /// `approval.resolved`.
    Approval,
}

impl Unmetered {
    /// The metric label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Unmarked => "unmarked",
            Self::Wait => "wait",
            Self::Signal => "signal",
            Self::Approval => "approval",
        }
    }
}

/// The exclusion a record falls under, or `None` when it bills or isn't work
/// at all. Only for metrics: a durable record that doesn't decode is `None`
/// here, and billing itself fails on it.
pub fn unmetered(record: &Record) -> Option<Unmetered> {
    match record.event_type.as_str() {
        "signal.received" => return Some(Unmetered::Signal),
        "approval.resolved" => return Some(Unmetered::Approval),
        "checkpoint.created" => return Some(Unmetered::Unmarked),
        _ => {}
    }
    let step_completion = record.event_type.ends_with(".completed")
        && STEP_PREFIXES
            .iter()
            .any(|prefix| record.event_type.starts_with(prefix));
    if is_observed_record(record) {
        return step_completion.then_some(Unmetered::Observed);
    }
    if !has_activation_marker(record) {
        return step_completion.then_some(Unmetered::Unmarked);
    }
    let suspended = record
        .metadata
        .get(META_ACTIVATION_TRANSITION)
        .is_some_and(|transition| transition == TRANSITION_SUSPENDED);
    (suspended && matches!(checkpoint_operations(record), Ok(None))).then_some(Unmetered::Wait)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use engine_proto::{
        ActivationExternalOutcomeCertainty, ActivationKind, ActivationRecoveryPolicy,
        BeginActivationRequest, CompleteActivationRequest, FailActivationRequest,
        ImmutablePayloadReference, SuspendActivationRequest,
    };
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::activation::{
        apply_journal_record, decide_begin, decide_complete, decide_fail, decide_suspend,
        decide_timer_suspend, payload_digest, to_engine_record, ActivationState,
        ActivationTransition, BeginActivationCommand, CompleteActivationCommand,
        FailActivationCommand, SuspendActivationCommand,
    };
    use crate::types::Offset;

    const PROJECT: &str = "project-1";

    /// Begins an activation of `kind`, returning its active state and the
    /// journaled `.started` record.
    fn begin(kind: ActivationKind) -> (ActivationState, Record) {
        let started = decide_begin(
            None,
            &BeginActivationCommand {
                request: BeginActivationRequest {
                    project_id: PROJECT.to_string(),
                    run_id: "run-1".to_string(),
                    parent_activation_id: "parent-1".to_string(),
                    kind: kind as i32,
                    stable_key: "step/load".to_string(),
                    input_digest: Sha256::digest(b"input").to_vec(),
                    definition_digest: Sha256::digest(b"definition").to_vec(),
                    recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                    worker_session_id: "worker-a".to_string(),
                    run_authority: b"run-authority".to_vec(),
                    lease_authority: b"lease-authority".to_vec(),
                    child: None,
                    display_name: "load".to_string(),
                    input_data: Vec::new(),
                    display_parent_correlation_id: String::new(),
                },
                issued_fence_token: b"fence-a".to_vec(),
            },
        )
        .unwrap();
        let state = apply(None, &started, 1);
        (state, journaled(&started, 1))
    }

    fn apply(
        state: Option<ActivationState>,
        transition: &ActivationTransition,
        offset: u64,
    ) -> ActivationState {
        apply_journal_record(state, &transition.records[0], Offset(offset)).unwrap()
    }

    fn journaled(transition: &ActivationTransition, offset: u64) -> Record {
        let mut record = to_engine_record(&transition.records[0], 1_000).unwrap();
        record.offset = Offset(offset);
        record
    }

    fn completion(state: &ActivationState, output: Value) -> Record {
        let payload = ActivationPayload {
            value: Some(output),
        };
        let completed = decide_complete(
            state,
            &CompleteActivationCommand {
                request: CompleteActivationRequest {
                    project_id: PROJECT.to_string(),
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
            },
        )
        .unwrap();
        journaled(&completed, 2)
    }

    fn reference(size_bytes: u64) -> Value {
        Value::Reference(ImmutablePayloadReference {
            project_id: PROJECT.to_string(),
            kind: "s3".to_string(),
            uri: "s3://results/run-1/load".to_string(),
            size_bytes,
            sha256: Sha256::digest(b"result").to_vec(),
            content_type: "application/json".to_string(),
        })
    }

    fn plain(event_type: &str) -> Record {
        Record {
            offset: Offset(42),
            project_id: PROJECT.to_string(),
            run_id: "run-1".to_string(),
            event_type: event_type.to_string(),
            data: bytes::Bytes::new(),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    #[test]
    fn every_completed_kind_bills_by_saved_result_size() {
        for kind in [
            ActivationKind::Step,
            ActivationKind::Function,
            ActivationKind::Model,
            ActivationKind::Tool,
        ] {
            let (active, started) = begin(kind);
            assert_eq!(checkpoint_operations(&started).unwrap(), None, "{kind:?}");
            let record = completion(&active, Value::InlineData(b"{\"ok\":true}".to_vec()));
            assert!(record.event_type.ends_with(".completed"), "{kind:?}");
            assert_eq!(checkpoint_operations(&record).unwrap(), Some(1), "{kind:?}");
        }

        let (active, _) = begin(ActivationKind::Step);
        let inline = vec![b'x'; CHECKPOINT_OPERATION_BYTES as usize + 1];
        let record = completion(&active, Value::InlineData(inline));
        assert_eq!(checkpoint_operations(&record).unwrap(), Some(2));
        let record = completion(&active, reference(300 * 1024));
        assert_eq!(checkpoint_operations(&record).unwrap(), Some(5));
    }

    #[test]
    fn a_timer_bills_once_at_its_suspension_and_a_wait_bills_nothing() {
        let (active, _) = begin(ActivationKind::Timer);
        let suspended = decide_timer_suspend(
            &active,
            &SuspendActivationRequest {
                project_id: PROJECT.to_string(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: active.current_attempt.attempt,
                fence_token: active.current_attempt.fence_token.clone(),
                timer_key: "timer-1".to_string(),
                ready_at_ms: 4_200,
                input_digest: active.identity.input_digest.clone(),
                definition_digest: active.identity.definition_digest.clone(),
                continuation: br#"{"step":2}"#.to_vec(),
                delay_ms: 0,
            },
        )
        .unwrap();
        let record = journaled(&suspended, 2);
        assert_eq!(record.event_type, "workflow.step.suspended");
        assert_eq!(checkpoint_operations(&record).unwrap(), Some(1));

        let (active, _) = begin(ActivationKind::Step);
        let waiting = decide_suspend(
            &active,
            &SuspendActivationCommand {
                project_id: PROJECT.to_string(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: active.current_attempt.attempt,
                fence_token: active.current_attempt.fence_token.clone(),
                resume_source: "signal:approved".to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            checkpoint_operations(&journaled(&waiting, 2)).unwrap(),
            None
        );
    }

    #[test]
    fn a_failed_attempt_bills_nothing() {
        let (active, _) = begin(ActivationKind::Step);
        let failed = decide_fail(
            &active,
            &FailActivationCommand {
                request: FailActivationRequest {
                    project_id: PROJECT.to_string(),
                    run_id: active.identity.run_id.clone(),
                    activation_id: active.identity.activation_id.clone(),
                    attempt: active.current_attempt.attempt,
                    fence_token: active.current_attempt.fence_token.clone(),
                    error_code: "TIMEOUT".to_string(),
                    retryable: true,
                    external_outcome_certainty: ActivationExternalOutcomeCertainty::NoEffect as i32,
                    ..Default::default()
                },
            },
        )
        .unwrap();
        assert_eq!(checkpoint_operations(&journaled(&failed, 2)).unwrap(), None);
    }

    #[test]
    fn unmarked_and_observed_records_bill_nothing() {
        // Legacy checkpoints and steps without the durable marker.
        let mut legacy = plain("workflow.step.completed");
        legacy.metadata.insert(
            META_ACTIVATION_TRANSITION.to_string(),
            "completed".to_string(),
        );
        assert_eq!(checkpoint_operations(&legacy).unwrap(), None);
        assert_eq!(
            checkpoint_operations(&plain("checkpoint.created")).unwrap(),
            None
        );

        let (active, _) = begin(ActivationKind::Step);
        let mut observed = completion(&active, Value::InlineData(b"{}".to_vec()));
        observed.metadata.insert(
            crate::journal_contract::META_ORIGIN.to_string(),
            crate::journal_contract::ORIGIN_OTLP.to_string(),
        );
        assert_eq!(checkpoint_operations(&observed).unwrap(), None);
    }

    #[test]
    fn a_marked_record_that_does_not_decode_fails_billing() {
        let (active, _) = begin(ActivationKind::Step);
        let mut record = completion(&active, Value::InlineData(b"{}".to_vec()));
        record.data = bytes::Bytes::from_static(b"{\"not\":\"canonical\"}");
        assert!(checkpoint_operations(&record).is_err());
    }

    #[test]
    fn result_operations_bill_each_started_block_and_at_least_one() {
        assert_eq!(result_operations(0), 1);
        assert_eq!(result_operations(1), 1);
        assert_eq!(result_operations(CHECKPOINT_OPERATION_BYTES), 1);
        assert_eq!(result_operations(CHECKPOINT_OPERATION_BYTES + 1), 2);
        assert_eq!(result_operations(300 * 1024), 5);
    }

    #[test]
    fn hour_start_floors_to_the_utc_hour() {
        let mut record = plain("run.assigned");
        record.timestamp_ns = (2 * NANOS_PER_HOUR) + 59 * 60 * 1_000_000_000;
        assert_eq!(hour_start_unix(&record), 2 * SECONDS_PER_HOUR);
        record.timestamp_ns = 2 * NANOS_PER_HOUR;
        assert_eq!(hour_start_unix(&record), 2 * SECONDS_PER_HOUR);
    }

    #[test]
    fn run_start_is_only_the_pinned_dispatch() {
        let mut record = plain("run.assigned");
        let mut run = Run {
            first_dispatch_offset: Some(42),
            ..Default::default()
        };
        assert!(is_run_start(&record, &run));

        // A fast worker's start that landed before the assignment.
        record.event_type = "run.started".to_string();
        assert!(is_run_start(&record, &run));

        // A later dispatch of the same run: a lease expiry, retry or resume.
        record.event_type = "run.assigned".to_string();
        run.first_dispatch_offset = Some(7);
        assert!(!is_run_start(&record, &run));

        run.first_dispatch_offset = None;
        assert!(!is_run_start(&record, &run));

        run.first_dispatch_offset = Some(42);
        record.event_type = "run.completed".to_string();
        assert!(!is_run_start(&record, &run));

        record.event_type = "run.assigned".to_string();
        record.metadata = HashMap::from([(
            crate::journal_contract::META_ORIGIN.to_string(),
            crate::journal_contract::ORIGIN_OTLP.to_string(),
        )]);
        assert!(!is_run_start(&record, &run));
    }

    fn observed(mut record: Record) -> Record {
        record.metadata.insert(
            crate::journal_contract::META_ORIGIN.to_string(),
            crate::journal_contract::ORIGIN_OTLP.to_string(),
        );
        record
    }

    #[test]
    fn keys_name_the_run_start_and_the_activation() {
        let record = plain("run.assigned");
        assert_eq!(start_key(&record), "op:start:project-1:run-1");

        let (active, _) = begin(ActivationKind::Step);
        let completed = completion(&active, Value::InlineData(b"{}".to_vec()));
        assert_eq!(
            checkpoint_key(&completed).unwrap(),
            format!("op:act:project-1:run-1:{}", active.identity.activation_id)
        );
    }

    #[test]
    fn the_key_digest_ignores_order_and_adds_up_across_parts() {
        let keys = ["op:start:p:r1", "op:act:p:r1:a1", "op:act:p:r1:a2"];
        let mut forward = KeyDigest::default();
        keys.iter().for_each(|key| forward.add_key(key));
        let mut backward = KeyDigest::default();
        keys.iter().rev().for_each(|key| backward.add_key(key));
        assert_eq!(forward, backward);

        let mut first = KeyDigest::default();
        first.add_key(keys[0]);
        let mut rest = KeyDigest::default();
        rest.add_key(keys[1]);
        rest.add_key(keys[2]);
        first.add(&rest);
        assert_eq!(first, forward);

        // A key counted twice shows.
        let mut doubled = forward;
        doubled.add_key(keys[0]);
        assert_ne!(doubled, forward);

        assert_eq!(KeyDigest::default().to_hex(), "0".repeat(64));
        assert_eq!(forward.to_hex().len(), 64);
    }

    #[test]
    fn the_key_digest_carries_and_wraps_at_256_bits() {
        let mut digest = KeyDigest([0xff; 32]);
        let mut one = KeyDigest::default();
        one.0[31] = 1;
        digest.add(&one);
        assert_eq!(digest, KeyDigest::default());

        let mut digest = KeyDigest::default();
        digest.0[31] = 0xff;
        digest.add(&one);
        assert_eq!(&digest.0[30..], &[1, 0]);
    }

    #[test]
    fn only_runtime_stamped_records_tell_the_journal_time() {
        assert!(is_runtime_clocked(&plain("run.assigned")));
        let (_, started) = begin(ActivationKind::Step);
        assert!(is_runtime_clocked(&started));
        // A worker's start and a caller's event carry their own clocks.
        assert!(!is_runtime_clocked(&plain("run.started")));
        assert!(!is_runtime_clocked(&plain("event.received")));
        assert!(!is_runtime_clocked(&observed(plain("run.assigned"))));
    }

    #[test]
    fn may_bill_includes_everything_that_bills() {
        assert!(may_bill(&plain("run.assigned")));
        assert!(may_bill(&plain("run.started")));
        let (active, started) = begin(ActivationKind::Step);
        assert!(!may_bill(&started));
        assert!(may_bill(&completion(
            &active,
            Value::InlineData(b"{}".to_vec())
        )));
        assert!(!may_bill(&plain("run.completed")));
        assert!(!may_bill(&observed(plain("run.started"))));
    }

    #[test]
    fn unmetered_names_each_exclusion() {
        assert_eq!(
            unmetered(&plain("signal.received")),
            Some(Unmetered::Signal)
        );
        assert_eq!(
            unmetered(&plain("approval.resolved")),
            Some(Unmetered::Approval)
        );
        assert_eq!(
            unmetered(&plain("checkpoint.created")),
            Some(Unmetered::Unmarked)
        );
        assert_eq!(
            unmetered(&plain("workflow.step.completed")),
            Some(Unmetered::Unmarked)
        );
        assert_eq!(
            unmetered(&observed(plain("lm.completed"))),
            Some(Unmetered::Observed)
        );
        assert_eq!(unmetered(&observed(plain("run.completed"))), None);
        assert_eq!(unmetered(&plain("run.completed")), None);
        assert_eq!(unmetered(&plain("workflow.step.started")), None);

        // A durable completion bills, so it isn't an exclusion.
        let (active, started) = begin(ActivationKind::Step);
        assert_eq!(unmetered(&started), None);
        assert_eq!(
            unmetered(&completion(&active, Value::InlineData(b"{}".to_vec()))),
            None
        );

        let waiting = decide_suspend(
            &active,
            &SuspendActivationCommand {
                project_id: PROJECT.to_string(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: active.current_attempt.attempt,
                fence_token: active.current_attempt.fence_token.clone(),
                resume_source: "signal:approved".to_string(),
            },
        )
        .unwrap();
        assert_eq!(unmetered(&journaled(&waiting, 2)), Some(Unmetered::Wait));
        assert_eq!(Unmetered::Wait.label(), "wait");
    }
}
