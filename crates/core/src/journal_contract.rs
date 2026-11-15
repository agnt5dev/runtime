//! Backend-neutral journal admission and idempotency rules.
//!
//! Every durable journal adapter must share these rules so retry identity and
//! validation do not drift with the physical storage implementation.

use crate::activation::{
    activation_record_identity, from_engine_record, has_activation_marker, Event,
    TRANSITION_RETRY_STARTED, TRANSITION_SNAPSHOT, TRANSITION_STARTED, TRANSITION_SUSPENDED,
};
use crate::error::{EngineError, EngineResult};
use crate::lease_fence::{
    is_lease_fenced, lease_fence, META_FENCE_ATTEMPT, META_FENCE_LEASE_ID,
    META_FENCE_RECEIVED_AT_MS, META_RESUME_SOURCE_ID, RUN_LEASE_EXPIRED_EVENT,
    RUN_META_DISPATCH_GENERATION, RUN_META_LEASE_ATTEMPT,
};
use crate::types::{Offset, Record, MAX_JOURNAL_RECORD_BYTES};
use base64::Engine;
use sha2::{Digest, Sha256};

const RUN_QUEUED_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.queued/v1\0";
const RUN_ASSIGNED_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.assigned/v1\0";
const EVENT_RECEIVED_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/event.received/v1\0";
const RUN_CANCELLED_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.cancelled/v1\0";
/// Canonical answer intent for strict retries of one pause generation.
pub const META_APPROVAL_RESPONSE: &str = "agnt5_approval_response";
const APPROVAL_REQUEST_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/approval.request/v1\0";
const APPROVAL_ANSWER_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/approval.answer/v1\0";
const RUN_RESUMED_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.resumed/v1\0";
const TIMER_SCHEDULED_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/timer.scheduled/v1\0";
const TIMER_DECISION_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/timer.decision/v1\0";
const TERMINAL_RESERVATION_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.terminal_reservation/v1\0";
const TERMINAL_OUTCOME_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.terminal_outcome/v1\0";
/// One slot per `(project, run, lease, attempt, event)` for lease-fenced
/// terminals, pauses, and renewals, so a lost-ack retry resolves to the
/// accepted offset before the fence sees a released lease.
const RUN_LEASE_FENCE_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/run.lease_fence/v1\0";
const ENTITY_STATE_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/workflow.state.changed/v1\0";
const ACTIVATION_BEGIN_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/activation.begin/v1\0";
const ACTIVATION_SUSPENSION_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/activation.suspension/v1\0";
const ACTIVATION_DECISION_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/activation.decision/v1\0";
const PULL_LIFECYCLE_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/pull.completion_lifecycle/v1\0";
const OBSERVED_SPAN_IDEMPOTENCY_PREFIX: &[u8] = b"idempotency/observed.span/v1\0";
const ACTIVATION_STATE_VERSION_PREFIX: &[u8] = b"state-version/activation/v1\0";
const IDEMPOTENCY_VALIDATION_PREFIX: &[u8] = b"idempotency-validation/v1\0";
const META_ENTITY_STATE_OPERATION_ID: &str = "operation_id";
const META_TERMINAL_RESERVATION_ID: &str = "terminal_reservation_id";
const META_TERMINAL_OUTCOME: &str = "terminal_outcome";
/// Optional stable caller operation for retryable runtime-authored cancellation.
pub const META_CANCEL_OPERATION_ID: &str = "cancel_operation_id";
pub const META_INVOCATION_IDEMPOTENCY_KEY: &str = "invocation_idempotency_key";
/// Runtime-stamped marker on a lifecycle record the worker delivered inside
/// `CompleteJob`. With `lease_id` and `lease_attempt` it gives the record an
/// idempotency slot so a retried completion resolves to the earlier offsets.
pub const META_PULL_COMPLETION_LIFECYCLE: &str = "pull_completion_lifecycle";
/// Zero-based position of the record within its `CompleteJob` batch.
pub const META_PULL_COMPLETION_LIFECYCLE_INDEX: &str = "pull_completion_lifecycle_index";
/// Digest of the complete ordered lifecycle bundle carried by `CompleteJob`.
///
/// The runtime stamps this on the lease-fenced terminal so the terminal's
/// strict idempotency fingerprint also covers records that precede it. A
/// lost-ack retry must therefore reproduce the whole bundle, not merely the
/// terminal outcome.
pub const META_PULL_COMPLETION_BUNDLE_DIGEST: &str = "pull_completion_bundle_digest";
pub const META_INVOCATION_DIGEST: &str = "invocation_digest";
/// Runtime-internal event that durably advances one dispatch outbox retry.
pub const DISPATCH_RETRY_SCHEDULED_EVENT: &str = "run.dispatch_retry_scheduled";
/// Journal offset of the authoritative `run.assigned` record delivered by PollJob.
pub const META_ASSIGNMENT_COMMIT_OFFSET: &str = "assignment_commit_offset";
/// Where a run's records come from. Absent on native runs, which a worker
/// executes.
pub const META_ORIGIN: &str = "origin";
/// [`META_ORIGIN`] on an observed run: one built from a customer's
/// OpenTelemetry traces, which no worker owns. Only the `AppendObserved` path
/// may write these records, so a worker can't use the marker to skip its
/// lease checks.
pub const ORIGIN_OTLP: &str = "otlp";

/// When the gateway received the span an observed-run record came from, in
/// Unix milliseconds. A run ends once spans stop arriving for a settle
/// window, so this is arrival time, not the span's own clock.
pub const META_OBSERVED_RECEIVED_AT_MS: &str = "observed_received_at_ms";
/// On the end record of an observed run's root span: `true` for the trace's
/// true root (no parent) or `local` for a span whose parent lives in another
/// service. A true root outranks a local one when both arrive.
pub const META_OBSERVED_ROOT: &str = "observed_root";
/// On a root's end record: `completed` or `failed`, decided by the gateway's
/// per-instrumentation outcome rule. Absent means the root's own status.
pub const META_OBSERVED_OUTCOME: &str = "observed_outcome";
/// Payload fields for a span's content, as native events name them: the
/// input on its start event and the output on its end event. The run takes
/// its own input and output from these.
pub const OBSERVED_INPUT_FIELD: &str = "input_data";
pub const OBSERVED_OUTPUT_FIELD: &str = "output_data";
/// Payload field on a failed span's end event: `true` when the span didn't
/// say why it failed and its error message was inferred.
pub const OBSERVED_ERROR_INFERRED_FIELD: &str = "error_inferred";

/// The component type of every observed run. Run listings filter on it to
/// find observed runs, since every listing backend already indexes it.
pub const OBSERVED_COMPONENT_TYPE: &str = "observed";

/// True when the record belongs to an observed run.
pub fn is_observed_record(record: &Record) -> bool {
    is_observed_metadata(&record.metadata)
}

/// True when record or run metadata marks an observed run.
pub fn is_observed_metadata(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata
        .get(META_ORIGIN)
        .is_some_and(|origin| origin == ORIGIN_OTLP)
}

/// The event types an observed run may hold: span events mapped from OTLP
/// spans, plus the run lifecycle records that open and end the run.
pub fn is_observed_event_type(event_type: &str) -> bool {
    matches!(
        event_type,
        "run.queued" | "run.started" | "run.completed" | "run.failed"
    ) || ["lm.", "tool_call.", "agent.", "workflow.step."]
        .iter()
        .any(|prefix| event_type.starts_with(prefix))
}

/// Return a stable digest for an ordered `CompleteJob` lifecycle bundle.
///
/// `prost` map encoding is not deterministic, so hash the submitted engine
/// record fields directly and sort metadata by key. Hashing the submitted
/// bundle also keeps retries stable when the runtime fills a missing timestamp.
pub fn pull_completion_bundle_digest(records: &[Record]) -> String {
    fn field(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    fn optional_field(hasher: &mut Sha256, value: Option<&[u8]>) {
        match value {
            Some(value) => {
                hasher.update([1]);
                field(hasher, value);
            }
            None => hasher.update([0]),
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(b"pull-completion-bundle/v1\0");
    hasher.update((records.len() as u64).to_be_bytes());
    for record in records {
        field(&mut hasher, record.project_id.as_bytes());
        field(&mut hasher, record.run_id.as_bytes());
        field(&mut hasher, record.event_type.as_bytes());
        field(&mut hasher, record.data.as_ref());
        hasher.update(record.timestamp_ns.to_be_bytes());
        optional_field(&mut hasher, record.step_key.as_deref().map(str::as_bytes));
        optional_field(
            &mut hasher,
            record.correlation_id.as_deref().map(str::as_bytes),
        );
        optional_field(
            &mut hasher,
            record.parent_event_id.as_deref().map(str::as_bytes),
        );
        let mut metadata = record.metadata.iter().collect::<Vec<_>>();
        metadata.sort_unstable_by(|left, right| left.0.cmp(right.0));
        hasher.update((metadata.len() as u64).to_be_bytes());
        for (key, value) in metadata {
            field(&mut hasher, key.as_bytes());
            field(&mut hasher, value.as_bytes());
        }
        field(&mut hasher, record.data_type.as_bytes());
        optional_field(&mut hasher, record.data_checksum.as_deref());
        hasher.update([u8::from(record.data_compressed)]);
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// Records that are durable implementation details rather than public run
/// history. Storage and processors must retain them; public history and stream
/// APIs must omit them.
pub fn is_runtime_internal_event(record: &Record) -> bool {
    matches!(
        record.event_type.as_str(),
        "run.archived" | "run.terminalizing" | DISPATCH_RETRY_SCHEDULED_EVENT
    ) || crate::runtime_work::is_work_record(record)
        || crate::experiment_feedback::is_feedback_record(record)
}

/// One optimistic state-version reservation embedded in a journal record.
/// Adapters validate and advance these claims in the same atomic transaction
/// or write batch as the record append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateVersionClaim {
    pub index_key: Vec<u8>,
    pub target: String,
    pub expected_version: i64,
    pub next_version: i64,
    pub activation_id: String,
    pub attempt: u32,
}

/// Extract state-version claims from an accepted activation completion or a
/// legacy entity-state journal write. Non-state records return an empty list.
pub fn record_state_version_claims(record: &Record) -> EngineResult<Vec<StateVersionClaim>> {
    if has_activation_marker(record) {
        let Some(decoded) = from_engine_record(record).map_err(EngineError::from)? else {
            return Ok(Vec::new());
        };
        let Some(Event::Completed(completed)) = decoded.event else {
            return Ok(Vec::new());
        };
        return completed
            .state_mutations
            .iter()
            .map(|mutation| {
                state_version_claim(
                    &completed.project_id,
                    &mutation.entity_type,
                    &mutation.entity_key,
                    &mutation.scope,
                    &mutation.scope_id,
                    mutation.expected_version,
                    &completed.activation_id,
                    completed.attempt,
                )
            })
            .collect();
    }

    if record.event_type != "workflow.state.changed" {
        return Ok(Vec::new());
    }
    let required = |key: &str| record.metadata.get(key).filter(|value| !value.is_empty());
    let (Some(entity_type), Some(entity_key), Some(scope), Some(scope_id), Some(expected)) = (
        required("entity_type"),
        required("entity_key"),
        required("scope"),
        required("scope_id"),
        required("expected_version"),
    ) else {
        // Old generic state records did not freeze every identity field. They
        // remain readable but do not participate in the additive V1 index.
        return Ok(Vec::new());
    };
    let expected_version = expected.parse::<i64>().map_err(|_| {
        EngineError::InvalidArgument(
            "workflow.state.changed expected_version must be an integer".to_string(),
        )
    })?;
    let claim = state_version_claim(
        &record.project_id,
        entity_type,
        entity_key,
        scope,
        scope_id,
        expected_version,
        "",
        0,
    )?;
    if let Some(new_version) = required("new_version") {
        let new_version = new_version.parse::<i64>().map_err(|_| {
            EngineError::InvalidArgument(
                "workflow.state.changed new_version must be an integer".to_string(),
            )
        })?;
        if new_version != claim.next_version {
            return Err(EngineError::InvalidArgument(format!(
                "workflow.state.changed new_version {new_version} does not follow expected_version {expected_version}"
            )));
        }
    }
    Ok(vec![claim])
}

#[allow(clippy::too_many_arguments)]
fn state_version_claim(
    project_id: &str,
    entity_type: &str,
    entity_key: &str,
    scope: &str,
    scope_id: &str,
    expected_version: i64,
    activation_id: &str,
    attempt: u32,
) -> EngineResult<StateVersionClaim> {
    let next_version = expected_version.checked_add(1).ok_or_else(|| {
        EngineError::InvalidArgument("state mutation version overflow".to_string())
    })?;
    if expected_version < 0 {
        return Err(EngineError::InvalidArgument(
            "state mutation expected_version must be non-negative".to_string(),
        ));
    }
    let mut index_key = Vec::with_capacity(
        ACTIVATION_STATE_VERSION_PREFIX.len()
            + project_id.len()
            + entity_type.len()
            + entity_key.len()
            + scope.len()
            + scope_id.len()
            + 40,
    );
    index_key.extend_from_slice(ACTIVATION_STATE_VERSION_PREFIX);
    for value in [project_id, entity_type, entity_key, scope, scope_id] {
        index_key.extend_from_slice(&(value.len() as u64).to_be_bytes());
        index_key.extend_from_slice(value.as_bytes());
    }
    Ok(StateVersionClaim {
        index_key,
        target: format!("{entity_type}/{entity_key}/{scope}/{scope_id}"),
        expected_version,
        next_version,
        activation_id: activation_id.to_string(),
        attempt,
    })
}

/// Reject a stale claim before any record, projection, or outbox mutation is
/// committed. A missing index is version zero (create-only).
pub fn validate_state_version_claim(
    current_version: Option<i64>,
    claim: &StateVersionClaim,
) -> EngineResult<()> {
    let current_version = current_version.unwrap_or(0);
    if current_version == claim.expected_version {
        return Ok(());
    }
    if !claim.activation_id.is_empty() {
        return Err(crate::error::ActivationError::StateVersionConflict {
            activation_id: claim.activation_id.clone(),
            attempt: claim.attempt,
            target: claim.target.clone(),
            expected_version: claim.expected_version,
            current_version,
        }
        .into());
    }
    Err(EngineError::InvalidArgument(format!(
        "state version conflict at {}: expected {}, current {}",
        claim.target, claim.expected_version, current_version
    )))
}

pub fn encode_state_version(version: i64) -> [u8; 8] {
    version.to_be_bytes()
}

pub fn decode_state_version(bytes: &[u8]) -> EngineResult<i64> {
    let encoded: [u8; 8] = bytes
        .try_into()
        .map_err(|_| EngineError::Storage("invalid state version index value".into()))?;
    let version = i64::from_be_bytes(encoded);
    if version < 0 {
        return Err(EngineError::Storage(
            "negative state version index value".into(),
        ));
    }
    Ok(version)
}

/// Validate records before an adapter filters retries or mutates durable state.
/// Check that a leader's batch lands exactly at the offsets the leader gave it.
///
/// Segments assign offsets from their own tail. A follower's tail can move
/// between the store RPC's tail check and the append, for example when
/// compaction restores committed records from cold history, so the check
/// must run again under the segment's write lock. `assigned` is what the
/// segment would write (or found already written) for each record; any
/// difference rejects the batch before anything is persisted.
pub fn validate_replicated_offsets(records: &[Record], assigned: &[Offset]) -> EngineResult<()> {
    for (record, assigned) in records.iter().zip(assigned) {
        if record.offset != *assigned {
            return Err(EngineError::Gap {
                expected: assigned.0,
                found: Some(record.offset.0),
            });
        }
    }
    if records.len() != assigned.len() {
        return Err(EngineError::Internal(format!(
            "replicated append cardinality mismatch: records={}, offsets={}",
            records.len(),
            assigned.len()
        )));
    }
    Ok(())
}

pub fn validate_append_records(records: &[Record]) -> EngineResult<()> {
    for record in records {
        crate::runtime_work::RuntimeWork::from_record(record)?;
        if let Some(operation) = record.metadata.get(META_CANCEL_OPERATION_ID) {
            if record.event_type != "run.cancelled"
                || operation.is_empty()
                || operation.len() > 128
                || !operation
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(EngineError::InvalidArgument(
                    "invalid cancellation operation identity".into(),
                ));
            }
        }
        let decoded_size = record.decoded_size_bytes();
        if decoded_size > MAX_JOURNAL_RECORD_BYTES {
            return Err(EngineError::InvalidArgument(format!(
                "journal record exceeds the {} byte beta limit (got {decoded_size} bytes); store large payloads by reference",
                MAX_JOURNAL_RECORD_BYTES
            )));
        }
        if idempotency_prefix(record).is_some() && record.run_id.is_empty() {
            return Err(EngineError::InvalidArgument(format!(
                "{} requires a non-empty run_id",
                record.event_type
            )));
        }
        if matches!(record.event_type.as_str(), "run.queued" | "batch.created") {
            if invocation_idempotency_key(record).is_some() {
                invocation_validation_fingerprint(record)?;
            } else if record.metadata.contains_key(META_INVOCATION_DIGEST) {
                return Err(EngineError::InvalidArgument(format!(
                    "{} metadata[{META_INVOCATION_DIGEST}] requires non-empty metadata[{META_INVOCATION_IDEMPOTENCY_KEY}]",
                    record.event_type
                )));
            }
        }
        if record
            .metadata
            .get(META_PULL_COMPLETION_LIFECYCLE)
            .is_some_and(|value| value == "1")
            && pull_completion_lifecycle_identity(record).is_none()
        {
            return Err(EngineError::InvalidArgument(format!(
                "{} metadata[{META_PULL_COMPLETION_LIFECYCLE}] requires non-empty lease_id, lease_attempt, and {META_PULL_COMPLETION_LIFECYCLE_INDEX}",
                record.event_type
            )));
        }
        // A marked record must carry a valid canonical payload; the marker
        // alone never makes something an activation record.
        if has_activation_marker(record) {
            from_engine_record(record).map_err(EngineError::from)?;
        }
        // A malformed fence envelope is a contract violation, never a fence
        // decision: producers stamp through `LeaseFence::stamp`.
        lease_fence(record)?;
        if record.event_type == "run.terminalizing" {
            for key in [META_TERMINAL_RESERVATION_ID, META_TERMINAL_OUTCOME] {
                if record.metadata.get(key).is_none_or(String::is_empty) {
                    return Err(EngineError::InvalidArgument(format!(
                        "run.terminalizing requires non-empty metadata[{key}]"
                    )));
                }
            }
            let outcome = record
                .metadata
                .get(META_TERMINAL_OUTCOME)
                .map(String::as_str);
            // An observed run ends as its trace did once the trace settles;
            // only observed runs may reserve those outcomes.
            let supported = matches!(
                outcome,
                Some("cancelled" | "timeout" | "wait_timeout" | "attempts_exhausted" | "stranded")
            ) || (is_observed_record(record)
                && matches!(outcome, Some("observed_completed" | "observed_failed")));
            if !supported {
                return Err(EngineError::InvalidArgument(format!(
                    "run.terminalizing metadata[{META_TERMINAL_OUTCOME}] is not a supported outcome"
                )));
            }
        }
        if matches!(
            record.event_type.as_str(),
            "run.completed" | "run.failed" | "run.cancelled"
        ) && record.metadata.contains_key(META_TERMINAL_RESERVATION_ID)
        {
            for key in [META_TERMINAL_RESERVATION_ID, META_TERMINAL_OUTCOME] {
                if record.metadata.get(key).is_none_or(String::is_empty) {
                    return Err(EngineError::InvalidArgument(format!(
                        "{} requires non-empty metadata[{key}]",
                        record.event_type
                    )));
                }
            }
        }
        if matches!(
            record.event_type.as_str(),
            "timer.scheduled" | "timer.fired" | "timer.canceled" | "timer.expired"
        ) && record.metadata.get("timer_id").is_none_or(String::is_empty)
        {
            return Err(EngineError::InvalidArgument(format!(
                "{} requires non-empty metadata[timer_id]",
                record.event_type
            )));
        }
        if is_strict_lease_assignment(record) {
            for key in [
                "lease_id",
                "lease_attempt",
                "lease_worker_id",
                "lease_session_hash",
                "lease_expires_at_ms",
                "lease_timeout_ms",
            ] {
                if record.metadata.get(key).is_none_or(String::is_empty) {
                    return Err(EngineError::InvalidArgument(format!(
                        "run.assigned requires non-empty metadata[{key}]"
                    )));
                }
            }
            record.metadata["lease_attempt"]
                .parse::<u32>()
                .map_err(|_| {
                    EngineError::InvalidArgument(
                        "run.assigned metadata[lease_attempt] must be a non-negative integer"
                            .to_string(),
                    )
                })?;
            for key in ["lease_expires_at_ms", "lease_timeout_ms"] {
                let value = record.metadata[key].parse::<i64>().map_err(|_| {
                    EngineError::InvalidArgument(format!(
                        "run.assigned metadata[{key}] must be a positive integer"
                    ))
                })?;
                if value <= 0 {
                    return Err(EngineError::InvalidArgument(format!(
                        "run.assigned metadata[{key}] must be a positive integer"
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Run metadata key for the workflow continuation a durable timer suspension
/// hands back to the worker when the run resumes. Workers read it from the
/// dispatched run's metadata.
pub const META_CONTINUATION_B64: &str = "continuation_b64";

/// Leave the workflow continuation out of a runtime-built record that copied
/// it from the run, when keeping it would breach the journal record limit.
///
/// Metadata counts six times its length toward [`MAX_JOURNAL_RECORD_BYTES`]
/// (the worst case of cold Parquet's JSON escaping), so roughly 175 KB of
/// base64 fills a whole record. A suspension accepts a continuation of up to
/// 256 KiB, about 350 KB once encoded, because it journals the continuation
/// as data. A timer resume, a lease expiry, or a supervisor terminal record
/// that copied the run's metadata would then be rejected, and nothing retries
/// it: the run stays paused or stranded.
///
/// The run projection already holds the continuation (a timer's
/// `timer.scheduled` record sets it), so the copy can go. It is dropped only
/// when `kept_by_run`, the value on the run row, is this same continuation,
/// so nothing is lost; a record that fits is left exactly as it was. Returns
/// whether the continuation was dropped.
pub fn shed_oversized_continuation(record: &mut Record, kept_by_run: Option<&str>) -> bool {
    if record.decoded_size_bytes() <= MAX_JOURNAL_RECORD_BYTES {
        return false;
    }
    let carried = record
        .metadata
        .get(META_CONTINUATION_B64)
        .map(String::as_str);
    if carried.is_none() || carried != kept_by_run {
        return false;
    }
    record.metadata.remove(META_CONTINUATION_B64);
    true
}

/// Exact index key shared by run.queued and batch.created invocation admission.
pub fn invocation_index_key(project_id: &str, caller_key: &str) -> Vec<u8> {
    let mut key = RUN_QUEUED_IDEMPOTENCY_PREFIX.to_vec();
    for value in [project_id.as_bytes(), caller_key.as_bytes()] {
        key.extend_from_slice(&(value.len() as u64).to_be_bytes());
        key.extend_from_slice(value);
    }
    key
}

/// Return the stable retry identity for records whose append is idempotent.
pub fn record_idempotency_key(record: &Record) -> Option<Vec<u8>> {
    if record.event_type == "approval.answered"
        && record.metadata.contains_key(META_APPROVAL_RESPONSE)
    {
        if let Some(request_key) = record
            .metadata
            .get("approval_idempotency_key")
            .filter(|value| !value.is_empty())
        {
            let mut key = APPROVAL_REQUEST_IDEMPOTENCY_PREFIX.to_vec();
            for value in [
                record.project_id.as_bytes(),
                record.run_id.as_bytes(),
                request_key.as_bytes(),
            ] {
                key.extend_from_slice(&(value.len() as u64).to_be_bytes());
                key.extend_from_slice(value);
            }
            return Some(key);
        }
    }

    if let Some((activation_id, attempt, transition)) = activation_record_identity(record) {
        let prefix = activation_idempotency_prefix(transition)?;
        let mut key = Vec::with_capacity(
            prefix.len() + record.project_id.len() + record.run_id.len() + activation_id.len() + 24,
        );
        key.extend_from_slice(prefix);
        let attempt_string = attempt.to_string();
        for value in [
            record.project_id.as_bytes(),
            record.run_id.as_bytes(),
            activation_id.as_bytes(),
            attempt_string.as_bytes(),
        ] {
            key.extend_from_slice(&(value.len() as u64).to_be_bytes());
            key.extend_from_slice(value);
        }
        return Some(key);
    }
    let prefix = idempotency_prefix(record)?;
    if let Some(caller_key) = invocation_idempotency_key(record) {
        return Some(invocation_index_key(&record.project_id, caller_key));
    }
    if let Some((lease_id, lease_attempt, index)) = pull_completion_lifecycle_identity(record) {
        let correlation_id = record.correlation_id.as_deref().unwrap_or_default();
        let mut key = Vec::with_capacity(
            prefix.len()
                + record.project_id.len()
                + record.run_id.len()
                + lease_id.len()
                + record.event_type.len()
                + correlation_id.len()
                + 64,
        );
        key.extend_from_slice(prefix);
        for value in [
            record.project_id.as_bytes(),
            record.run_id.as_bytes(),
            lease_id.as_bytes(),
            lease_attempt.as_bytes(),
            record.event_type.as_bytes(),
            correlation_id.as_bytes(),
            index.as_bytes(),
        ] {
            key.extend_from_slice(&(value.len() as u64).to_be_bytes());
            key.extend_from_slice(value);
        }
        return Some(key);
    }
    if is_strict_lease_assignment(record) {
        let generation = &record.metadata[RUN_META_DISPATCH_GENERATION];
        let mut key = Vec::with_capacity(
            prefix.len() + record.project_id.len() + record.run_id.len() + generation.len() + 24,
        );
        key.extend_from_slice(prefix);
        for value in [
            record.project_id.as_bytes(),
            record.run_id.as_bytes(),
            generation.as_bytes(),
        ] {
            key.extend_from_slice(&(value.len() as u64).to_be_bytes());
            key.extend_from_slice(value);
        }
        return Some(key);
    }
    let attempt = record.event_type.eq("run.queued").then(|| {
        record
            .metadata
            .get("attempt")
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0)
    });

    let mut key =
        Vec::with_capacity(prefix.len() + record.project_id.len() + record.run_id.len() + 16);
    key.extend_from_slice(prefix);
    key.extend_from_slice(record.project_id.as_bytes());
    key.push(0);
    key.extend_from_slice(record.run_id.as_bytes());
    if let Some(attempt) = attempt {
        key.push(0);
        key.extend_from_slice(attempt.to_string().as_bytes());
    }
    if record.event_type == "run.cancelled" {
        if let Some(operation) = record.metadata.get(META_CANCEL_OPERATION_ID) {
            key.push(0);
            key.extend_from_slice(operation.as_bytes());
        }
    }
    if record.event_type == "run.resumed" || is_resolver_approval(record) {
        key.push(0);
        key.extend_from_slice(
            record
                .metadata
                .get(META_RESUME_SOURCE_ID)
                .map(String::as_bytes)
                .unwrap_or_default(),
        );
    }
    if is_resolver_approval(record) {
        key.push(0);
        key.extend_from_slice(record.event_type.as_bytes());
    }
    if matches!(
        record.event_type.as_str(),
        "timer.scheduled" | "timer.fired" | "timer.canceled" | "timer.expired"
    ) {
        key.push(0);
        key.extend_from_slice(
            record
                .metadata
                .get("timer_id")
                .map(String::as_bytes)
                .unwrap_or_default(),
        );
    }
    if has_lease_fence_slot(record) {
        for value in [
            record
                .metadata
                .get(META_FENCE_LEASE_ID)
                .map(String::as_str)
                .unwrap_or_default(),
            record
                .metadata
                .get(META_FENCE_ATTEMPT)
                .map(String::as_str)
                .unwrap_or_default(),
            record.event_type.as_str(),
            // A renewal slot is one per requested expiry so a worker that
            // keeps renewing gets one record per extension.
            record
                .metadata
                .get("lease_expires_at_ms")
                .filter(|_| record.event_type == crate::lease_fence::RUN_LEASE_RENEWED_EVENT)
                .map(String::as_str)
                .unwrap_or_default(),
        ] {
            key.push(0);
            key.extend_from_slice(value.as_bytes());
        }
    }
    if let Some((event_type, span_id)) = observed_span_identity(record) {
        for value in [event_type, span_id] {
            key.push(0);
            key.extend_from_slice(value.as_bytes());
        }
    }
    if record.event_type == "workflow.state.changed" {
        key.push(0);
        key.extend_from_slice(
            record
                .metadata
                .get(META_ENTITY_STATE_OPERATION_ID)
                .map(String::as_bytes)
                .unwrap_or_default(),
        );
    }
    if matches!(
        record.event_type.as_str(),
        "run.terminalizing" | "run.completed" | "run.failed" | "run.cancelled"
    ) && record.metadata.contains_key(META_TERMINAL_RESERVATION_ID)
    {
        key.push(0);
        key.extend_from_slice(
            record
                .metadata
                .get(META_TERMINAL_RESERVATION_ID)
                .map(String::as_bytes)
                .unwrap_or_default(),
        );
    }
    Some(key)
}

/// Validate an idempotent duplicate against the previously accepted record.
///
/// Begin records intentionally use first-writer-wins bytes: two workers can
/// race with distinct session/fence authority, and the loser must read the
/// canonical winner. Suspension has its own nonterminal exact-retry slot so a
/// later cancellation can still win the terminal slot. Every terminal attempt
/// decision (completion, failure, cancellation, or unknown outcome) is
/// stricter: the same idempotency slot is reusable only for byte-identical
/// canonical journal data.
pub fn validate_idempotent_duplicate(existing: &Record, incoming: &Record) -> EngineResult<()> {
    let existing_fingerprint = record_idempotency_validation_fingerprint(existing);
    validate_idempotency_validation_fingerprint(existing_fingerprint.as_ref(), incoming)
}

/// Return the sidecar key used to retain strict retry validation after the
/// corresponding hot journal record is compacted and trimmed.
///
/// The deployed offset index remains unchanged for rolling compatibility;
/// binaries that do not know about this additive key continue to read the
/// original eight-byte offset value.
pub fn idempotency_validation_index_key(idempotency_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(IDEMPOTENCY_VALIDATION_PREFIX.len() + idempotency_key.len());
    key.extend_from_slice(IDEMPOTENCY_VALIDATION_PREFIX);
    key.extend_from_slice(idempotency_key);
    key
}

/// Return the compact validation identity needed by strict idempotent slots.
///
/// Non-activation records and activation begin records are intentionally
/// first-writer-wins, so their offset alone is sufficient. Suspension,
/// terminal activation decisions, and timer schedules require the same event
/// type and payload. Timer generation decisions compare the event type only:
/// duplicate HA pollers may observe different fire timestamps, but fire versus
/// cancellation must still be a strict first-writer-wins race.
pub fn record_idempotency_validation_fingerprint(record: &Record) -> Option<[u8; 32]> {
    if invocation_idempotency_key(record).is_some() {
        return invocation_validation_fingerprint(record).ok();
    }
    if let Some(answer) = record.metadata.get(META_APPROVAL_RESPONSE) {
        if record.event_type == "run.resumed" || is_resolver_approval(record) {
            let mut hasher = Sha256::new();
            hasher.update(answer.as_bytes());
            if record.event_type == "approval.answered"
                && record.metadata.contains_key("approval_idempotency_key")
            {
                hasher.update([0]);
                hasher.update(
                    record
                        .metadata
                        .get(META_RESUME_SOURCE_ID)
                        .map(String::as_bytes)
                        .unwrap_or_default(),
                );
            }
            return Some(hasher.finalize().into());
        }
    }
    let strict_activation = activation_record_identity(record).is_some_and(|(_, _, transition)| {
        !matches!(transition, TRANSITION_STARTED | TRANSITION_RETRY_STARTED)
    });
    let strict_timer = matches!(
        record.event_type.as_str(),
        "timer.scheduled" | "timer.fired" | "timer.canceled" | "timer.expired"
    );
    let strict_cancellation = record.event_type == "run.cancelled"
        && record.metadata.contains_key(META_CANCEL_OPERATION_ID);
    let strict_assignment = is_strict_lease_assignment(record);
    let strict_lease_fence = has_lease_fence_slot(record);
    if !strict_activation
        && !strict_timer
        && !strict_assignment
        && !strict_lease_fence
        && !strict_cancellation
    {
        return None;
    }

    let event_type = record.event_type.as_bytes();
    let mut hasher = Sha256::new();
    hasher.update((event_type.len() as u64).to_be_bytes());
    hasher.update(event_type);
    if strict_assignment || strict_lease_fence || strict_cancellation {
        hasher.update((record.data.len() as u64).to_be_bytes());
        hasher.update(record.data.as_ref());
        // A lost-ack retry of a fenced record is rebuilt by the service with
        // a fresh receive time; everything else must be byte-identical.
        let mut metadata = record
            .metadata
            .iter()
            .filter(|(key, _)| !strict_lease_fence || key.as_str() != META_FENCE_RECEIVED_AT_MS)
            .collect::<Vec<_>>();
        metadata.sort_unstable_by(|left, right| left.0.cmp(right.0));
        for (key, value) in metadata {
            hasher.update((key.len() as u64).to_be_bytes());
            hasher.update(key.as_bytes());
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        return Some(hasher.finalize().into());
    }
    if !matches!(
        record.event_type.as_str(),
        "timer.fired" | "timer.canceled" | "timer.expired"
    ) {
        hasher.update((record.data.len() as u64).to_be_bytes());
        hasher.update(record.data.as_ref());
    }
    Some(hasher.finalize().into())
}

pub fn decode_idempotency_validation_fingerprint(bytes: &[u8]) -> EngineResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| EngineError::Storage("invalid idempotency validation fingerprint".to_string()))
}

/// Validate a retry using the compact sidecar retained beyond hot trimming.
///
/// A missing sidecar is accepted for indexes written before this additive
/// format existed. New strict records always persist the sidecar atomically
/// with the offset index and record append.
pub fn validate_idempotency_validation_fingerprint(
    existing: Option<&[u8; 32]>,
    incoming: &Record,
) -> EngineResult<()> {
    let Some(incoming_fingerprint) = record_idempotency_validation_fingerprint(incoming) else {
        return Ok(());
    };
    let Some(existing_fingerprint) = existing else {
        // During rollout a legacy gateway can win the resume slot. Refuse
        // the strict batch here so its losing audit records cannot commit.
        if incoming.metadata.contains_key(META_APPROVAL_RESPONSE) {
            return Err(EngineError::InvalidArgument(
                "approval answer conflicts with accepted intent".into(),
            ));
        }
        return Ok(());
    };
    if existing_fingerprint == &incoming_fingerprint {
        return Ok(());
    }

    if invocation_idempotency_key(incoming).is_some() {
        return Err(EngineError::InvocationConflict {
            run_id: incoming.run_id.clone(),
        });
    }

    if let Some((activation_id, attempt, _)) = activation_record_identity(incoming) {
        return Err(crate::error::ActivationError::PayloadConflict {
            activation_id: activation_id.to_string(),
            attempt,
        }
        .into());
    }
    if incoming.metadata.contains_key(META_APPROVAL_RESPONSE) {
        return Err(EngineError::InvalidArgument(
            "approval answer conflicts with accepted intent".into(),
        ));
    }
    if incoming.event_type == "run.cancelled"
        && incoming.metadata.contains_key(META_CANCEL_OPERATION_ID)
    {
        return Err(EngineError::InvalidArgument(
            "cancellation operation conflicts with accepted intent".into(),
        ));
    }
    if incoming.event_type == "run.assigned" {
        return Err(EngineError::InvalidArgument(
            "run.assigned conflicts with the accepted lease for this dispatch generation"
                .to_string(),
        ));
    }
    if has_lease_fence_slot(incoming) {
        return Err(EngineError::LeaseFenceMismatch {
            run_id: incoming.run_id.clone(),
            reason: format!(
                "{} conflicts with the outcome already accepted for this lease attempt",
                incoming.event_type
            ),
        });
    }
    Err(EngineError::InvalidArgument(format!(
        "{} conflicts with the accepted timer generation decision",
        incoming.event_type
    )))
}

fn invocation_idempotency_key(record: &Record) -> Option<&str> {
    matches!(record.event_type.as_str(), "run.queued" | "batch.created")
        .then(|| record.metadata.get(META_INVOCATION_IDEMPOTENCY_KEY))
        .flatten()
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

fn invocation_validation_fingerprint(record: &Record) -> EngineResult<[u8; 32]> {
    let encoded = record
        .metadata
        .get(META_INVOCATION_DIGEST)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            EngineError::InvalidArgument(format!(
                "{} requires non-empty metadata[{META_INVOCATION_DIGEST}] when metadata[{META_INVOCATION_IDEMPOTENCY_KEY}] is present",
                record.event_type
            ))
        })?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| {
            EngineError::InvalidArgument(format!(
                "{} metadata[{META_INVOCATION_DIGEST}] must be a base64url SHA-256 digest",
                record.event_type
            ))
        })?;
    decoded.try_into().map_err(|_| {
        EngineError::InvalidArgument(format!(
            "{} metadata[{META_INVOCATION_DIGEST}] must encode exactly 32 bytes",
            record.event_type
        ))
    })
}

/// Encode an idempotent append result in the deployed v1 representation.
pub fn encode_idempotency_offset(offset: Offset) -> [u8; 8] {
    offset.0.to_be_bytes()
}

/// Decode an idempotent append result from the deployed v1 representation.
pub fn decode_idempotency_offset(bytes: &[u8]) -> EngineResult<Offset> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| EngineError::Storage("invalid idempotency offset".into()))?;
    Ok(Offset(u64::from_be_bytes(arr)))
}

/// Begin records are first-writer-wins per attempt, suspensions have their
/// own nonterminal slot, every other decision shares the strict terminal
/// slot. Snapshots are not idempotent appends.
fn activation_idempotency_prefix(transition: &str) -> Option<&'static [u8]> {
    match transition {
        TRANSITION_SNAPSHOT => None,
        TRANSITION_STARTED | TRANSITION_RETRY_STARTED => Some(ACTIVATION_BEGIN_IDEMPOTENCY_PREFIX),
        TRANSITION_SUSPENDED => Some(ACTIVATION_SUSPENSION_IDEMPOTENCY_PREFIX),
        _ => Some(ACTIVATION_DECISION_IDEMPOTENCY_PREFIX),
    }
}

fn is_resolver_approval(record: &Record) -> bool {
    matches!(
        record.event_type.as_str(),
        "approval.answered" | "approval.resolved"
    ) && record.metadata.contains_key(META_APPROVAL_RESPONSE)
        && record.metadata.contains_key(META_RESUME_SOURCE_ID)
}

fn idempotency_prefix(record: &Record) -> Option<&'static [u8]> {
    if let Some((_, _, transition)) = activation_record_identity(record) {
        return activation_idempotency_prefix(transition);
    }
    if has_lease_fence_slot(record) {
        return Some(RUN_LEASE_FENCE_IDEMPOTENCY_PREFIX);
    }
    if record.event_type == "workflow.state.changed"
        && record
            .metadata
            .get(META_ENTITY_STATE_OPERATION_ID)
            .is_some_and(|value| !value.is_empty())
    {
        return Some(ENTITY_STATE_IDEMPOTENCY_PREFIX);
    }
    if record.event_type == "run.terminalizing"
        && record
            .metadata
            .get(META_TERMINAL_RESERVATION_ID)
            .is_some_and(|value| !value.is_empty())
    {
        return Some(TERMINAL_RESERVATION_IDEMPOTENCY_PREFIX);
    }
    if matches!(
        record.event_type.as_str(),
        "run.completed" | "run.failed" | "run.cancelled"
    ) && record
        .metadata
        .get(META_TERMINAL_RESERVATION_ID)
        .is_some_and(|value| !value.is_empty())
    {
        return Some(TERMINAL_OUTCOME_IDEMPOTENCY_PREFIX);
    }
    if record.event_type == "timer.scheduled" {
        return Some(TIMER_SCHEDULED_IDEMPOTENCY_PREFIX);
    }
    if matches!(
        record.event_type.as_str(),
        "timer.fired" | "timer.canceled" | "timer.expired"
    ) {
        return Some(TIMER_DECISION_IDEMPOTENCY_PREFIX);
    }
    if is_strict_lease_assignment(record) {
        return Some(RUN_ASSIGNED_IDEMPOTENCY_PREFIX);
    }
    if pull_completion_lifecycle_identity(record).is_some() {
        return Some(PULL_LIFECYCLE_IDEMPOTENCY_PREFIX);
    }
    if observed_span_identity(record).is_some() {
        return Some(OBSERVED_SPAN_IDEMPOTENCY_PREFIX);
    }
    match record.event_type.as_str() {
        "run.queued" | "batch.created" => Some(RUN_QUEUED_IDEMPOTENCY_PREFIX),
        "event.received" => Some(EVENT_RECEIVED_IDEMPOTENCY_PREFIX),
        "approval.answered" | "approval.resolved" if is_resolver_approval(record) => {
            Some(APPROVAL_ANSWER_IDEMPOTENCY_PREFIX)
        }
        "run.cancelled" if record.metadata.contains_key(META_CANCEL_OPERATION_ID) => {
            Some(RUN_CANCELLED_IDEMPOTENCY_PREFIX)
        }
        "run.resumed" if record.metadata.contains_key(META_RESUME_SOURCE_ID) => {
            Some(RUN_RESUMED_IDEMPOTENCY_PREFIX)
        }
        _ => None,
    }
}

/// The slot an observed run's record takes, so an exporter's retry of the
/// same spans adds nothing: `run.started` once per run, and each span event
/// once per run, event type and span ID. `run.queued` already has a slot per
/// run, and the terminal records the supervisor writes have theirs.
fn observed_span_identity(record: &Record) -> Option<(&str, &str)> {
    if !is_observed_record(record) {
        return None;
    }
    if record.event_type == "run.started" {
        return Some(("run.started", ""));
    }
    if record.event_type.starts_with("run.") {
        return None;
    }
    let span_id = record
        .correlation_id
        .as_deref()
        .filter(|id| !id.is_empty())?;
    Some((record.event_type.as_str(), span_id))
}

/// `(lease_id, lease_attempt, index)` for a lifecycle record that arrived
/// inside `CompleteJob`, or `None` when the marker or any field is absent.
fn pull_completion_lifecycle_identity(record: &Record) -> Option<(&str, &str, &str)> {
    if record
        .metadata
        .get(META_PULL_COMPLETION_LIFECYCLE)
        .map(String::as_str)
        != Some("1")
    {
        return None;
    }
    let field = |key: &str| {
        record
            .metadata
            .get(key)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    };
    let lease_id = field("lease_id")?;
    let lease_attempt = field(RUN_META_LEASE_ATTEMPT)?;
    let index = field(META_PULL_COMPLETION_LIFECYCLE_INDEX)?;
    index.parse::<u32>().ok()?;
    Some((lease_id, lease_attempt, index))
}

/// Lease-fenced records that own a retry slot. Expiries are excluded: two
/// sweepers racing on the same lease should not compare payloads, the second
/// simply loses to the released entry at the fence.
fn has_lease_fence_slot(record: &Record) -> bool {
    is_lease_fenced(record) && record.event_type != RUN_LEASE_EXPIRED_EVENT
}

fn is_strict_lease_assignment(record: &Record) -> bool {
    record.event_type == "run.assigned"
        && record
            .metadata
            .get(RUN_META_DISPATCH_GENERATION)
            .is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use base64::Engine;
    use bytes::Bytes;

    use super::{
        record_idempotency_key, record_idempotency_validation_fingerprint,
        shed_oversized_continuation, validate_append_records, validate_idempotent_duplicate,
        META_CONTINUATION_B64, META_INVOCATION_DIGEST, META_INVOCATION_IDEMPOTENCY_KEY,
        META_PULL_COMPLETION_LIFECYCLE, META_PULL_COMPLETION_LIFECYCLE_INDEX,
    };
    use crate::lease_fence::RUN_META_LEASE_ATTEMPT;
    use crate::types::{Offset, Record};

    fn state_record(operation_id: &str) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "project-1".to_string(),
            run_id: "run-1".to_string(),
            event_type: "workflow.state.changed".to_string(),
            data: Bytes::from_static(br#"{"value":1}"#),
            timestamp_ns: 1,
            step_key: None,
            correlation_id: Some(operation_id.to_string()),
            parent_event_id: None,
            metadata: HashMap::from([("operation_id".to_string(), operation_id.to_string())]),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn invocation_record(caller_key: &str, digest_byte: u8) -> Record {
        let mut record = state_record("invocation-test");
        record.event_type = "run.queued".to_string();
        record.metadata.insert(
            META_INVOCATION_IDEMPOTENCY_KEY.to_string(),
            caller_key.to_string(),
        );
        record.metadata.insert(
            META_INVOCATION_DIGEST.to_string(),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([digest_byte; 32]),
        );
        record
    }

    fn resume_with_continuation(continuation_len: usize) -> (Record, String) {
        let continuation = "A".repeat(continuation_len);
        let mut record = state_record("resume");
        record.event_type = "run.resumed".to_string();
        record.metadata.clear();
        record
            .metadata
            .insert(META_CONTINUATION_B64.to_string(), continuation.clone());
        (record, continuation)
    }

    #[test]
    fn a_continuation_that_breaches_the_record_limit_is_shed_when_the_run_keeps_it() {
        // The second timer of a stalled production run carried 202,062 base64
        // characters, which metadata counts six times over the 1 MiB limit.
        let (mut record, continuation) = resume_with_continuation(202_062);
        assert!(validate_append_records(std::slice::from_ref(&record)).is_err());

        assert!(shed_oversized_continuation(
            &mut record,
            Some(&continuation)
        ));
        assert!(!record.metadata.contains_key(META_CONTINUATION_B64));
        validate_append_records(&[record]).expect("the shed record is admissible");
    }

    #[test]
    fn a_continuation_is_kept_when_the_record_fits_or_the_run_lacks_it() {
        // 121,882 characters, the first timer of the same run, still fits.
        let (mut fits, continuation) = resume_with_continuation(121_882);
        assert!(!shed_oversized_continuation(&mut fits, Some(&continuation)));
        assert_eq!(
            fits.metadata.get(META_CONTINUATION_B64),
            Some(&continuation)
        );

        // A run row without this continuation would lose it, so the record
        // keeps it and the append fails as before.
        let (mut stale, _) = resume_with_continuation(202_062);
        assert!(!shed_oversized_continuation(&mut stale, None));
        assert!(!shed_oversized_continuation(&mut stale, Some("older")));
        assert!(stale.metadata.contains_key(META_CONTINUATION_B64));
    }

    #[test]
    fn invocation_idempotency_uses_project_and_caller_key() {
        let first = invocation_record("checkout-42", 1);
        let mut retry = invocation_record("checkout-42", 1);
        retry.data = Bytes::from_static(br#"{"ignored_dynamic_field":true}"#);
        let changed = invocation_record("checkout-42", 2);
        let other_key = invocation_record("checkout-43", 1);
        let mut changed_operation = invocation_record("checkout-42", 2);
        changed_operation.event_type = "batch.created".to_string();

        assert_eq!(
            record_idempotency_key(&first),
            record_idempotency_key(&retry)
        );
        assert_ne!(
            record_idempotency_key(&first),
            record_idempotency_key(&other_key)
        );
        assert!(validate_idempotent_duplicate(&first, &retry).is_ok());
        assert!(matches!(
            validate_idempotent_duplicate(&first, &changed),
            Err(crate::error::EngineError::InvocationConflict { .. })
        ));
        assert_eq!(
            record_idempotency_key(&first),
            record_idempotency_key(&changed_operation),
            "changing the API operation must conflict in the same caller-key slot"
        );
    }

    fn lifecycle_record(event_type: &str, index: &str) -> Record {
        let mut record = state_record("lifecycle-test");
        record.event_type = event_type.to_string();
        record.correlation_id = Some("corr-1".to_string());
        record.metadata = HashMap::from([
            (META_PULL_COMPLETION_LIFECYCLE.to_string(), "1".to_string()),
            ("lease_id".to_string(), "lease-1".to_string()),
            (RUN_META_LEASE_ATTEMPT.to_string(), "0".to_string()),
            (
                META_PULL_COMPLETION_LIFECYCLE_INDEX.to_string(),
                index.to_string(),
            ),
        ]);
        record
    }

    #[test]
    fn pull_completion_lifecycle_records_are_idempotent_per_lease_slot() {
        let first = lifecycle_record("run.started", "0");
        let retry = lifecycle_record("run.started", "0");
        let other_index = lifecycle_record("run.started", "1");
        let other_event = lifecycle_record("function.started", "0");
        let mut other_lease = lifecycle_record("run.started", "0");
        other_lease
            .metadata
            .insert("lease_id".to_string(), "lease-2".to_string());
        let mut unmarked = lifecycle_record("run.started", "0");
        unmarked.metadata.remove(META_PULL_COMPLETION_LIFECYCLE);

        let key = record_idempotency_key(&first).expect("marked lifecycle record is keyed");
        assert_eq!(Some(key.clone()), record_idempotency_key(&retry));
        assert_ne!(Some(key.clone()), record_idempotency_key(&other_index));
        assert_ne!(Some(key.clone()), record_idempotency_key(&other_event));
        assert_ne!(Some(key), record_idempotency_key(&other_lease));
        assert_eq!(record_idempotency_key(&unmarked), None);
        assert!(validate_append_records(std::slice::from_ref(&first)).is_ok());

        let mut missing_attempt = lifecycle_record("run.started", "0");
        missing_attempt.metadata.remove(RUN_META_LEASE_ATTEMPT);
        assert!(validate_append_records(std::slice::from_ref(&missing_attempt)).is_err());
    }

    #[test]
    fn invocation_idempotency_requires_a_sha256_digest() {
        let mut missing = invocation_record("checkout-42", 1);
        missing.metadata.remove(META_INVOCATION_DIGEST);
        assert!(validate_append_records(&[missing])
            .expect_err("digest is required")
            .to_string()
            .contains(META_INVOCATION_DIGEST));

        let mut malformed = invocation_record("checkout-42", 1);
        malformed
            .metadata
            .insert(META_INVOCATION_DIGEST.to_string(), "short".to_string());
        assert!(validate_append_records(&[malformed]).is_err());
    }

    #[test]
    fn entity_state_idempotency_is_scoped_to_operation() {
        let first = record_idempotency_key(&state_record("op-1")).expect("state key");
        let retry = record_idempotency_key(&state_record("op-1")).expect("retry key");
        let next = record_idempotency_key(&state_record("op-2")).expect("next key");

        assert_eq!(first, retry);
        assert_ne!(first, next);
    }

    fn terminal_record(event_type: &str, reservation_id: &str) -> Record {
        Record {
            event_type: event_type.to_string(),
            metadata: HashMap::from([
                (
                    "terminal_reservation_id".to_string(),
                    reservation_id.to_string(),
                ),
                ("terminal_outcome".to_string(), "timeout".to_string()),
            ]),
            ..state_record("terminal-test")
        }
    }

    #[test]
    fn terminal_reservation_and_outcome_retries_are_idempotent() {
        let reservation = terminal_record("run.terminalizing", "reservation-1");
        let retry = terminal_record("run.terminalizing", "reservation-1");
        let next = terminal_record("run.terminalizing", "reservation-2");
        let outcome = terminal_record("run.failed", "reservation-1");

        assert_eq!(
            record_idempotency_key(&reservation),
            record_idempotency_key(&retry)
        );
        assert_ne!(
            record_idempotency_key(&reservation),
            record_idempotency_key(&next)
        );
        assert_ne!(
            record_idempotency_key(&reservation),
            record_idempotency_key(&outcome),
            "the reservation and its final outcome need separate durable slots"
        );
    }

    fn timer_record(event_type: &str, timer_id: &str, timestamp_ns: i64) -> Record {
        Record {
            event_type: event_type.to_string(),
            timestamp_ns,
            metadata: HashMap::from([("timer_id".to_string(), timer_id.to_string())]),
            ..state_record("timer-test")
        }
    }

    fn lease_assignment(lease_id: &str, attempt: u32, generation: &str) -> Record {
        Record {
            event_type: "run.assigned".to_string(),
            metadata: HashMap::from([
                ("lease_id".to_string(), lease_id.to_string()),
                ("lease_attempt".to_string(), attempt.to_string()),
                ("lease_worker_id".to_string(), "worker-1".to_string()),
                (
                    "lease_session_hash".to_string(),
                    "session-hash-1".to_string(),
                ),
                ("lease_expires_at_ms".to_string(), "60000".to_string()),
                ("lease_timeout_ms".to_string(), "15000".to_string()),
                (
                    super::RUN_META_DISPATCH_GENERATION.to_string(),
                    generation.to_string(),
                ),
            ]),
            ..state_record("assignment-test")
        }
    }

    #[test]
    fn lease_assignment_generation_has_one_strict_idempotency_slot() {
        let first = lease_assignment("lease-1", 2, "queue-offset-10");
        let retry = first.clone();
        let competitor = lease_assignment("lease-2", 2, "queue-offset-10");
        let mut competing_grant = first.clone();
        competing_grant.metadata.insert(
            crate::lease_fence::RUN_META_GRANT_ID.to_string(),
            "grant-2".to_string(),
        );
        let resumed_generation = lease_assignment("lease-3", 2, "resume-offset-20");
        let retry_generation = lease_assignment("lease-4", 3, "retry-offset-30");

        validate_append_records(std::slice::from_ref(&first)).unwrap();
        assert_eq!(
            record_idempotency_key(&first),
            record_idempotency_key(&competitor),
            "competing workers must race for one assignment slot"
        );
        assert_ne!(
            record_idempotency_key(&first),
            record_idempotency_key(&resumed_generation),
            "a resumed dispatch in the same attempt must have its own assignment slot"
        );
        assert_ne!(
            record_idempotency_key(&first),
            record_idempotency_key(&retry_generation),
            "a retried dispatch must have its own assignment slot"
        );
        assert!(record_idempotency_validation_fingerprint(&first).is_some());
        assert!(validate_idempotent_duplicate(&first, &retry).is_ok());
        assert!(validate_idempotent_duplicate(&first, &competitor).is_err());
        assert!(
            validate_idempotent_duplicate(&first, &competing_grant).is_err(),
            "grant identity participates in strict assignment retry validation"
        );
    }

    #[test]
    fn invocation_identity_metadata_may_propagate_to_lifecycle_records() {
        let invocation = invocation_record("issue-dispatch-42", 7);
        let mut assignment = lease_assignment("lease-1", 1, "queue-offset-10");
        assignment.metadata.extend(invocation.metadata);

        validate_append_records(&[assignment])
            .expect("derived lifecycle records may retain the accepted invocation identity");
    }

    #[test]
    fn lease_assignment_requires_complete_authority_metadata() {
        let mut assignment = lease_assignment("lease-1", 0, "queue-offset-10");
        assignment.metadata.remove("lease_session_hash");

        let error = validate_append_records(&[assignment]).unwrap_err();
        assert!(error.to_string().contains("lease_session_hash"));
    }

    #[test]
    fn timer_schedule_and_single_decision_have_distinct_durable_slots() {
        let scheduled = timer_record("timer.scheduled", "timer-1", 10);
        let fired = timer_record("timer.fired", "timer-1", 20);
        let mut duplicate_fire = timer_record("timer.fired", "timer-1", 22);
        duplicate_fire.data = Bytes::from_static(br#"{"observed_at_ns":22}"#);
        let cancelled = timer_record("timer.canceled", "timer-1", 21);
        let other_generation = timer_record("timer.fired", "timer-2", 20);

        assert_ne!(
            record_idempotency_key(&scheduled),
            record_idempotency_key(&fired)
        );
        assert_eq!(
            record_idempotency_key(&fired),
            record_idempotency_key(&cancelled),
            "fire and cancellation race for one timer generation"
        );
        assert_ne!(
            record_idempotency_key(&fired),
            record_idempotency_key(&other_generation)
        );
        assert!(record_idempotency_validation_fingerprint(&fired).is_some());
        assert!(validate_idempotent_duplicate(&fired, &fired).is_ok());
        assert!(validate_idempotent_duplicate(&fired, &duplicate_fire).is_ok());
        assert!(validate_idempotent_duplicate(&fired, &cancelled).is_err());
    }

    #[test]
    fn timer_events_require_a_generation_id() {
        let record = timer_record("timer.fired", "", 20);
        let error = validate_append_records(&[record]).expect_err("timer id is required");
        assert!(error.to_string().contains("timer_id"));
    }

    #[test]
    fn terminal_reservation_requires_identity_and_outcome() {
        let mut record = terminal_record("run.terminalizing", "reservation-1");
        assert!(validate_append_records(std::slice::from_ref(&record)).is_ok());

        record.metadata.remove("terminal_outcome");
        let error = validate_append_records(&[record]).unwrap_err();
        assert!(error.to_string().contains("terminal_outcome"));

        let mut unknown = terminal_record("run.terminalizing", "reservation-2");
        unknown
            .metadata
            .insert("terminal_outcome".to_string(), "success".to_string());
        assert!(validate_append_records(&[unknown])
            .unwrap_err()
            .to_string()
            .contains("not a supported outcome"));
    }

    fn observed_span(event_type: &str, span_id: Option<&str>) -> Record {
        let mut record = Record {
            event_type: event_type.to_string(),
            correlation_id: span_id.map(str::to_string),
            ..state_record("observed-test")
        };
        record.metadata.insert(
            super::META_ORIGIN.to_string(),
            super::ORIGIN_OTLP.to_string(),
        );
        record
    }

    /// An exporter that resends a request adds nothing: each span event and
    /// the run's start take one slot per run.
    #[test]
    fn observed_records_take_one_slot_per_span_event() {
        let key = |event_type, span_id| record_idempotency_key(&observed_span(event_type, span_id));
        let completed = key("lm.completed", Some("00f067aa0ba902b7"));
        assert!(completed.is_some());
        assert_eq!(completed, key("lm.completed", Some("00f067aa0ba902b7")));
        assert_ne!(completed, key("lm.started", Some("00f067aa0ba902b7")));
        assert_ne!(completed, key("lm.completed", Some("a3ce929d0e0e4736")));
        assert_eq!(key("run.started", None), key("run.started", None));
        assert!(key("run.started", None).is_some());

        // A span event with no span ID, and the same event on a native run,
        // keep their old behaviour: no slot.
        assert!(key("lm.completed", None).is_none());
        let mut native = observed_span("lm.completed", Some("00f067aa0ba902b7"));
        native.metadata.remove(super::META_ORIGIN);
        assert!(record_idempotency_key(&native).is_none());
    }

    #[test]
    fn only_observed_runs_reserve_a_settled_outcome() {
        for outcome in ["observed_completed", "observed_failed"] {
            let mut record = terminal_record("run.terminalizing", "reservation-1");
            record
                .metadata
                .insert("terminal_outcome".to_string(), outcome.to_string());
            assert!(validate_append_records(std::slice::from_ref(&record))
                .unwrap_err()
                .to_string()
                .contains("not a supported outcome"));

            record.metadata.insert(
                super::META_ORIGIN.to_string(),
                super::ORIGIN_OTLP.to_string(),
            );
            assert!(validate_append_records(&[record]).is_ok(), "{outcome}");
        }
    }

    #[test]
    fn lease_fenced_records_retry_through_one_slot_per_lease_attempt() {
        use crate::lease_fence::{
            worker_session_hash, LeaseFence, LeaseFenceKind, META_FENCE_RECEIVED_AT_MS,
            RUN_LEASE_EXPIRED_EVENT, RUN_LEASE_RENEWED_EVENT,
        };
        let fence = LeaseFence {
            kind: LeaseFenceKind::PullCompletion,
            lease_id: "lease-1".to_string(),
            worker_id: "worker-1".to_string(),
            session_hash: worker_session_hash("session-1"),
            attempt: 2,
            received_at_ms: 1_000,
            expiry_reason: None,
        };
        let mut completed = state_record("op-1");
        completed.event_type = "run.completed".to_string();
        completed.metadata.clear();
        fence.stamp(&mut completed);
        validate_append_records(std::slice::from_ref(&completed)).unwrap();

        let mut retry = completed.clone();
        retry.timestamp_ns += 1;
        retry
            .metadata
            .insert(META_FENCE_RECEIVED_AT_MS.to_string(), "2000".to_string());
        assert_eq!(
            record_idempotency_key(&completed),
            record_idempotency_key(&retry)
        );
        validate_idempotent_duplicate(&completed, &retry).unwrap();

        let mut changed = completed.clone();
        changed.data = Bytes::from_static(br#"{"value":2}"#);
        assert!(validate_idempotent_duplicate(&completed, &changed).is_err());

        let mut failed = completed.clone();
        failed.event_type = "run.failed".to_string();
        assert_ne!(
            record_idempotency_key(&completed),
            record_idempotency_key(&failed)
        );

        let mut renewed = completed.clone();
        renewed.event_type = RUN_LEASE_RENEWED_EVENT.to_string();
        renewed
            .metadata
            .insert("agnt5_fence_kind".to_string(), "lease_renewal".to_string());
        renewed
            .metadata
            .insert("lease_expires_at_ms".to_string(), "5000".to_string());
        let mut renewed_again = renewed.clone();
        renewed_again
            .metadata
            .insert("lease_expires_at_ms".to_string(), "6000".to_string());
        validate_append_records(&[renewed.clone(), renewed_again.clone()]).unwrap();
        assert_ne!(
            record_idempotency_key(&renewed),
            record_idempotency_key(&renewed_again)
        );

        let mut expired = completed.clone();
        expired.event_type = RUN_LEASE_EXPIRED_EVENT.to_string();
        expired
            .metadata
            .insert("agnt5_fence_kind".to_string(), "lease_expiry".to_string());
        expired
            .metadata
            .insert("agnt5_fence_reason".to_string(), "timeout".to_string());
        validate_append_records(std::slice::from_ref(&expired)).unwrap();
        assert!(record_idempotency_key(&expired).is_none());

        let mut malformed = completed.clone();
        malformed.metadata.remove("agnt5_fence_lease_id");
        assert!(validate_append_records(std::slice::from_ref(&malformed)).is_err());
    }
}

#[cfg(test)]
mod cancellation_operation_tests {
    use super::*;
    #[test]
    fn optional_cancellation_operation_binds_scope_and_exact_intent() {
        let mut record = Record::from_proto(engine_proto::Record {
            project_id: "project".into(),
            run_id: "run".into(),
            event_type: "run.cancelled".into(),
            data: b"{}".to_vec(),
            ..Default::default()
        });
        assert!(
            record_idempotency_key(&record).is_none(),
            "legacy cancellation remains unchanged"
        );
        record
            .metadata
            .insert(META_CANCEL_OPERATION_ID.into(), "cancel-1".into());
        record
            .metadata
            .insert("reason".into(), "experiment cancelled".into());
        validate_append_records(std::slice::from_ref(&record)).unwrap();
        let key = record_idempotency_key(&record).unwrap();
        let mut retry = record.clone();
        retry.timestamp_ns = 100;
        assert_eq!(record_idempotency_key(&retry).unwrap(), key);
        validate_idempotent_duplicate(&record, &retry).unwrap();
        retry
            .metadata
            .insert("reason".into(), "changed intent".into());
        assert!(validate_idempotent_duplicate(&record, &retry).is_err());
        retry.project_id = "other-project".into();
        assert_ne!(record_idempotency_key(&retry).unwrap(), key);
        retry
            .metadata
            .insert(META_CANCEL_OPERATION_ID.into(), "invalid.identity".into());
        assert!(validate_append_records(std::slice::from_ref(&retry)).is_err());
    }
}
