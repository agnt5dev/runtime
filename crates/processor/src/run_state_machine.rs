//! Run lifecycle state machine.
//!
//! Codifies the legal run state transitions so the processor can validate
//! events before (or instead of) blindly applying them.
//!
//! Validation is observe-only: illegal transitions are logged, not rejected.
//! The processor writes `run.assigned` after successful dispatch to a worker.

use std::fmt;

pub const RETRY_SOURCE_META: &str = "retry_source";
pub const RETRY_SOURCE_RUN_FAILED: &str = "run.failed";
/// The offset of the `run.failed` record a processor-built retry follows.
pub const RETRY_FROM_OFFSET_META: &str = "retry_from_offset";
pub const RETRY_READY_AT_MS_META: &str = "retry_ready_at_ms";
pub const READY_AT_MS_META: &str = "ready_at_ms";
pub const RETRY_INITIAL_BACKOFF_MS_META: &str = "retry_initial_backoff_ms";
pub const RETRY_MAX_BACKOFF_MS_META: &str = "retry_max_backoff_ms";
pub const WAIT_TIMEOUT_MS_META: &str = "wait_timeout_ms";
pub const WAIT_DEADLINE_MS_META: &str = "wait_deadline_ms";
pub const TERMINAL_OBLIGATION_OFFSET_META: &str = "terminal_obligation_offset";
/// Observed runs (built from OTLP traces) end when their trace settles. The
/// runs projection keeps this state on the run so the terminal supervisor can
/// decide when, and how, to end it.
///
/// When the latest span arrived, in Unix milliseconds.
pub const OBSERVED_LAST_SPAN_MS_META: &str = "observed_last_span_ms";
/// `true` or `local`: the kind of root seen so far. Set once a root arrives.
pub const OBSERVED_ROOT_KIND_META: &str = "observed_root_kind";
/// `completed` or `failed`, from the root that decides the run.
pub const OBSERVED_OUTCOME_META: &str = "observed_outcome";
/// How many spans ended in error, so recovered failures stay visible.
pub const OBSERVED_FAILED_SPANS_META: &str = "observed_failed_spans";
/// Set when the run's input and output came from a root, so later model
/// calls don't replace them.
pub const OBSERVED_ROOT_IO_META: &str = "observed_root_io";
/// The latest span time seen, in Unix nanoseconds. The run ends at this time
/// rather than when the supervisor notices the trace has settled.
pub const OBSERVED_ENDED_AT_NS_META: &str = "observed_ended_at_ns";
/// The error the deciding root reported, copied onto the run when it fails.
pub const OBSERVED_ERROR_CODE_META: &str = "observed_error_code";
pub const OBSERVED_ERROR_MESSAGE_META: &str = "observed_error_message";
/// Set when the error was inferred by the gateway because the root didn't
/// say why it failed; a real error from another root replaces it.
pub const OBSERVED_ERROR_INFERRED_META: &str = "observed_error_inferred";
pub const TERMINAL_OBLIGATION_UPDATED_AT_MS_META: &str = "terminal_obligation_updated_at_ms";
pub const TERMINAL_RESERVATION_ID_META: &str = "terminal_reservation_id";
pub const TERMINAL_OUTCOME_META: &str = "terminal_outcome";
pub const TERMINALIZING_OWNER_META: &str = "terminalizing_owner";
pub const TERMINALIZING_REASON_META: &str = "terminalizing_reason";
pub const RUN_DEADLINE_MS_META: &str = "deadline_ms";
pub const CANCELLATION_REQUESTED_META: &str = "cancellation_requested";
/// Run metadata holding the worker terminations that ended earlier attempts:
/// a JSON array of `[attempt, reason]` pairs in attempt order, with 1-based
/// attempts. The run projection appends to it from each `run.lease_expired`
/// that names a termination, and the final lease-loss failure reads it, so an
/// OOM kill on attempt 2 isn't lost when attempt 3 times out.
pub const LEASE_WORKER_TERMINATIONS_META: &str = "lease_worker_terminations";
/// The history entry for an attempt whose worker's stream dropped while it
/// held the lease. Push dispatches recover on the disconnect itself, before
/// any container termination report can name a cause.
pub const WORKER_DISCONNECTED_REASON: &str = "disconnected";
/// `lease_recovery_reason` of a lease taken back because its worker
/// disconnected.
pub const WORKER_DISCONNECT_RECOVERY: &str = "worker_disconnect";
/// Lease-loss record metadata naming why the worker's container terminated.
pub const WORKER_TERMINATION_REASON_META: &str = "worker_termination_reason";
/// Lease-loss record metadata naming why the runtime took the lease back.
pub const LEASE_RECOVERY_REASON_META: &str = "lease_recovery_reason";
/// Bounds the history; each reason is free text of up to 128 characters.
const MAX_LEASE_WORKER_TERMINATIONS: usize = 16;
const ATTEMPT_META: &str = "attempt";
const MAX_ATTEMPTS_META: &str = "max_attempts";
const WORKERLESS_RETRYABLE_META: &str = "workerless_retryable";
const WORKERLESS_RETRY_DELAY_MS_META: &str = "workerless_retry_delay_ms";
const DEFAULT_RETRY_INITIAL_BACKOFF_MS: u64 = 1_000;
const DEFAULT_RETRY_MAX_BACKOFF_MS: u64 = 60_000;

/// Run lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunStatus {
    Queued,
    Assigned,
    Started,
    RetryDelayed,
    Terminalizing,
    Completed,
    Failed,
    Paused,
    Cancelled,
}

impl RunStatus {
    /// Parse from the proto string representation.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "assigned" => Some(Self::Assigned),
            "started" => Some(Self::Started),
            "retry_delayed" => Some(Self::RetryDelayed),
            "terminalizing" => Some(Self::Terminalizing),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "paused" => Some(Self::Paused),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Proto string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Assigned => "assigned",
            Self::Started => "started",
            Self::RetryDelayed => "retry_delayed",
            Self::Terminalizing => "terminalizing",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Paused => "paused",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether this status is terminal (no further transitions allowed).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Check whether a transition from `from` to `to` is legal.
///
/// Legal transitions:
/// - queued → assigned    (processor dispatches successfully)
/// - queued → started     (backward compat: legacy coordinator dispatch without run.assigned)
/// - queued → cancelled
/// - assigned → started   (worker begins execution)
/// - assigned → completed (worker completed without a separate started event)
/// - assigned → failed    (worker or infra failure)
/// - assigned → retry_delayed (retryable worker or infra failure)
/// - assigned → cancelled
/// - assigned → queued    (lease expired — sweeper re-queues for redelivery)
/// - started → completed
/// - started → failed
/// - started → retry_delayed
/// - started → paused
/// - started → cancelled
/// - started → queued     (lease expired mid-execution — re-queue with attempt bump)
/// - paused → queued      (via run.resumed — re-queues for fresh dispatch)
/// - paused → started     (backward compat: legacy resume that skipped re-dispatch)
/// - paused → cancelled
/// - retry_delayed → queued (durable retry schedule becomes ready)
/// - nonterminal → terminalizing (partition-owner terminal reservation)
/// - terminalizing → completed/failed/cancelled
pub fn can_transition(from: RunStatus, to: RunStatus) -> bool {
    matches!(
        (from, to),
        (RunStatus::Queued, RunStatus::Assigned)
            | (RunStatus::Queued, RunStatus::Started) // backward compat
            | (RunStatus::Queued, RunStatus::Cancelled)
            | (RunStatus::Assigned, RunStatus::Started)
            | (RunStatus::Assigned, RunStatus::Completed)
            | (RunStatus::Assigned, RunStatus::Failed)
            | (RunStatus::Assigned, RunStatus::RetryDelayed)
            | (RunStatus::Assigned, RunStatus::Paused) // pull completion bundle with no run.started
            | (RunStatus::Assigned, RunStatus::Cancelled)
            | (RunStatus::Assigned, RunStatus::Queued) // lease expired
            | (RunStatus::Started, RunStatus::Completed)
            | (RunStatus::Started, RunStatus::Failed)
            | (RunStatus::Started, RunStatus::RetryDelayed)
            | (RunStatus::Started, RunStatus::Paused)
            | (RunStatus::Started, RunStatus::Cancelled)
            | (RunStatus::Started, RunStatus::Queued) // lease expired
            | (RunStatus::Paused, RunStatus::Queued) // run.resumed → re-queue
            | (RunStatus::Paused, RunStatus::Started) // backward compat
            | (RunStatus::Paused, RunStatus::Cancelled)
            | (RunStatus::RetryDelayed, RunStatus::Queued)
            | (RunStatus::RetryDelayed, RunStatus::Cancelled)
            | (RunStatus::Queued, RunStatus::Terminalizing)
            | (RunStatus::Assigned, RunStatus::Terminalizing)
            | (RunStatus::Started, RunStatus::Terminalizing)
            | (RunStatus::RetryDelayed, RunStatus::Terminalizing)
            | (RunStatus::Paused, RunStatus::Terminalizing)
            | (RunStatus::Terminalizing, RunStatus::Completed)
            | (RunStatus::Terminalizing, RunStatus::Failed)
            | (RunStatus::Terminalizing, RunStatus::Cancelled)
    )
}

/// Map a journal event type to the target run status it produces.
///
/// Returns `None` for non-lifecycle events (e.g. `lm.completed`).
pub fn event_to_target_status(event_type: &str) -> Option<RunStatus> {
    match event_type {
        "run.queued" => Some(RunStatus::Queued),
        "run.assigned" => Some(RunStatus::Assigned),
        "run.started" => Some(RunStatus::Started),
        "run.completed" => Some(RunStatus::Completed),
        "run.failed" => Some(RunStatus::Failed),
        "run.paused" | "workflow.paused" => Some(RunStatus::Paused),
        // Resume re-queues the run; the orchestrator's evaluate_resumed
        // mints a fresh dispatch and writes run.assigned next.
        "run.resumed" => Some(RunStatus::Queued),
        "run.cancelled" => Some(RunStatus::Cancelled),
        "run.terminalizing" => Some(RunStatus::Terminalizing),
        // Lease timeout → re-queue for redelivery.
        "run.lease_expired" => Some(RunStatus::Queued),
        _ => None,
    }
}

/// Map a lifecycle record to its projected status.
///
/// A retryable `run.failed` is an attempt outcome, not the canonical run
/// terminal. Project it as `retry_delayed` so `failed` remains a strict sink.
pub fn record_target_status(
    event_type: &str,
    metadata: &std::collections::HashMap<String, String>,
) -> Option<RunStatus> {
    if event_type == "run.failed" && next_retry_attempt(metadata).is_some() {
        Some(RunStatus::RetryDelayed)
    } else {
        event_to_target_status(event_type)
    }
}

/// Return the next attempt for a retryable failure.
///
/// Attempts are zero-based. `max_attempts=3` therefore permits attempts
/// `0`, `1`, and `2`. An explicit `workerless_retryable=false` always wins
/// over the numeric budget.
pub fn next_retry_attempt(
    metadata: &std::collections::HashMap<String, String>,
) -> Option<(u32, u32)> {
    if metadata
        .get(WORKERLESS_RETRYABLE_META)
        .is_some_and(|value| value.eq_ignore_ascii_case("false"))
    {
        return None;
    }

    let current = metadata.get(ATTEMPT_META)?.parse::<u32>().ok()?;
    let max_attempts = metadata.get(MAX_ATTEMPTS_META)?.parse::<u32>().ok()?;
    let next = current.checked_add(1)?;

    (max_attempts > 1 && next < max_attempts).then_some((next, max_attempts))
}

/// Resolve the durable delay before the next retry attempt becomes runnable.
pub fn retry_delay_ms(metadata: &std::collections::HashMap<String, String>) -> Option<u64> {
    let (next_attempt, _) = next_retry_attempt(metadata)?;
    if let Some(delay) = metadata
        .get(WORKERLESS_RETRY_DELAY_MS_META)
        .and_then(|value| value.parse::<u64>().ok())
    {
        return Some(delay);
    }

    let initial = metadata
        .get(RETRY_INITIAL_BACKOFF_MS_META)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RETRY_INITIAL_BACKOFF_MS);
    let maximum = metadata
        .get(RETRY_MAX_BACKOFF_MS_META)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RETRY_MAX_BACKOFF_MS)
        .max(initial);
    let shift = next_attempt.saturating_sub(1).min(63);
    Some(initial.saturating_mul(1_u64 << shift).min(maximum))
}

/// Resolve the wall-clock timestamp at which a retry becomes runnable.
pub fn retry_ready_at_ms(
    failed_at_ns: i64,
    metadata: &std::collections::HashMap<String, String>,
) -> Option<i64> {
    let failed_at_ms = failed_at_ns / 1_000_000;
    let delay = i64::try_from(retry_delay_ms(metadata)?).unwrap_or(i64::MAX);
    Some(failed_at_ms.saturating_add(delay))
}

/// Whether a queued record is authorized to advance a retry-delayed run.
///
/// Final `Failed -> Queued` remains illegal. The processor-built retry
/// record must identify its source and advance exactly one attempt within the
/// declared retry budget.
pub fn is_authorized_failed_retry(
    current_status: RunStatus,
    current_attempt: u32,
    metadata: &std::collections::HashMap<String, String>,
) -> bool {
    if current_status != RunStatus::RetryDelayed
        || metadata.get(RETRY_SOURCE_META).map(String::as_str) != Some(RETRY_SOURCE_RUN_FAILED)
    {
        return false;
    }

    let Some(attempt) = metadata
        .get(ATTEMPT_META)
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return false;
    };
    let Some(max_attempts) = metadata
        .get(MAX_ATTEMPTS_META)
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return false;
    };

    metadata
        .get(WORKERLESS_RETRYABLE_META)
        .is_none_or(|value| !value.eq_ignore_ascii_case("false"))
        && attempt == current_attempt.saturating_add(1)
        && attempt < max_attempts
}

/// Remove the runtime-owned lease-loss keys from metadata that a caller, a
/// worker or a lifecycle record supplied. Only the run projection writes the
/// termination history, and only the runtime's own lease-loss records name a
/// lost lease's cause.
pub fn strip_runtime_lease_loss_metadata(metadata: &mut std::collections::HashMap<String, String>) {
    for key in [
        LEASE_WORKER_TERMINATIONS_META,
        WORKER_TERMINATION_REASON_META,
        LEASE_RECOVERY_REASON_META,
    ] {
        metadata.remove(key);
    }
}

/// The worker terminations recorded for earlier attempts, in attempt order.
pub fn lease_worker_terminations(
    metadata: &std::collections::HashMap<String, String>,
) -> Vec<(u32, String)> {
    metadata
        .get(LEASE_WORKER_TERMINATIONS_META)
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default()
}

/// Record that `attempt` (1-based) ended because its worker terminated with
/// `reason`. An attempt already recorded is left as it is, so applying the
/// same record twice changes nothing.
pub fn record_lease_worker_termination(
    metadata: &mut std::collections::HashMap<String, String>,
    attempt: u32,
    reason: &str,
) {
    let mut terminations = lease_worker_terminations(metadata);
    if terminations.len() >= MAX_LEASE_WORKER_TERMINATIONS
        || terminations
            .iter()
            .any(|(recorded, _)| *recorded == attempt)
    {
        return;
    }
    terminations.push((attempt, reason.to_string()));
    if let Ok(raw) = serde_json::to_string(&terminations) {
        metadata.insert(LEASE_WORKER_TERMINATIONS_META.to_string(), raw);
    }
}

/// The earlier attempts' worker terminations as a suffix for a failure
/// message, such as ` (attempt 2: worker OOM-killed)`, or empty when there
/// were none.
pub fn lease_worker_terminations_suffix(
    metadata: &std::collections::HashMap<String, String>,
) -> String {
    let terminations = lease_worker_terminations(metadata);
    if terminations.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = terminations
        .iter()
        .map(|(attempt, reason)| {
            if reason.eq_ignore_ascii_case("OOMKilled") {
                format!("attempt {attempt}: worker OOM-killed")
            } else if reason == WORKER_DISCONNECTED_REASON {
                format!("attempt {attempt}: worker disconnected")
            } else {
                format!("attempt {attempt}: worker terminated ({reason})")
            }
        })
        .collect();
    format!(" ({})", parts.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_variants() {
        let variants = [
            RunStatus::Queued,
            RunStatus::Assigned,
            RunStatus::Started,
            RunStatus::RetryDelayed,
            RunStatus::Terminalizing,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Paused,
            RunStatus::Cancelled,
        ];
        for v in variants {
            assert_eq!(RunStatus::from_str(v.as_str()), Some(v));
            assert_eq!(v.to_string(), v.as_str());
        }
    }

    #[test]
    fn list_filter_status_contract_matches_the_state_machine() {
        use engine_core::run_filter::{ARCHIVED_RUN_STATUSES, RUN_STATUSES};
        for status in RUN_STATUSES {
            assert!(RunStatus::from_str(status).is_some(), "{status}");
        }
        let variants = [
            RunStatus::Queued,
            RunStatus::Assigned,
            RunStatus::Started,
            RunStatus::RetryDelayed,
            RunStatus::Terminalizing,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Paused,
            RunStatus::Cancelled,
        ];
        assert_eq!(variants.len(), RUN_STATUSES.len());
        for v in variants {
            assert_eq!(
                ARCHIVED_RUN_STATUSES.contains(&v.as_str()),
                v.is_terminal(),
                "{v}"
            );
        }
    }

    #[test]
    fn from_str_unknown() {
        assert_eq!(RunStatus::from_str(""), None);
        assert_eq!(RunStatus::from_str("running"), None);
        assert_eq!(RunStatus::from_str("QUEUED"), None);
    }

    #[test]
    fn is_terminal() {
        assert!(!RunStatus::Queued.is_terminal());
        assert!(!RunStatus::Assigned.is_terminal());
        assert!(!RunStatus::Started.is_terminal());
        assert!(!RunStatus::RetryDelayed.is_terminal());
        assert!(!RunStatus::Terminalizing.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(!RunStatus::Paused.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
    }

    #[test]
    fn legal_transitions() {
        let legal = [
            (RunStatus::Queued, RunStatus::Assigned),
            (RunStatus::Queued, RunStatus::Started), // backward compat
            (RunStatus::Queued, RunStatus::Cancelled),
            (RunStatus::Assigned, RunStatus::Started),
            (RunStatus::Assigned, RunStatus::Completed),
            (RunStatus::Assigned, RunStatus::Failed),
            (RunStatus::Assigned, RunStatus::RetryDelayed),
            (RunStatus::Assigned, RunStatus::Paused), // HITL pause folded into CompleteJob
            (RunStatus::Assigned, RunStatus::Cancelled),
            (RunStatus::Assigned, RunStatus::Queued), // lease expired
            (RunStatus::Started, RunStatus::Completed),
            (RunStatus::Started, RunStatus::Failed),
            (RunStatus::Started, RunStatus::RetryDelayed),
            (RunStatus::Started, RunStatus::Paused),
            (RunStatus::Started, RunStatus::Cancelled),
            (RunStatus::Started, RunStatus::Queued), // lease expired
            (RunStatus::Paused, RunStatus::Queued),  // run.resumed
            (RunStatus::Paused, RunStatus::Started), // backward compat
            (RunStatus::Paused, RunStatus::Cancelled),
            (RunStatus::RetryDelayed, RunStatus::Queued),
            (RunStatus::Queued, RunStatus::Terminalizing),
            (RunStatus::Terminalizing, RunStatus::Failed),
        ];
        for (from, to) in legal {
            assert!(can_transition(from, to), "{from} -> {to} should be legal");
        }
    }

    #[test]
    fn illegal_from_terminal() {
        let terminals = [
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ];
        let all = [
            RunStatus::Queued,
            RunStatus::Assigned,
            RunStatus::Started,
            RunStatus::RetryDelayed,
            RunStatus::Terminalizing,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Paused,
            RunStatus::Cancelled,
        ];
        for from in terminals {
            for to in all {
                assert!(
                    !can_transition(from, to),
                    "{from} -> {to} should be illegal (terminal source)"
                );
            }
        }
    }

    #[test]
    fn illegal_self_transitions() {
        let all = [
            RunStatus::Queued,
            RunStatus::Assigned,
            RunStatus::Started,
            RunStatus::RetryDelayed,
            RunStatus::Terminalizing,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Paused,
            RunStatus::Cancelled,
        ];
        for s in all {
            assert!(
                !can_transition(s, s),
                "{s} -> {s} self-transition should be illegal"
            );
        }
    }

    #[test]
    fn illegal_skip_and_reverse() {
        // Can't skip from queued to completed/failed
        assert!(!can_transition(RunStatus::Queued, RunStatus::Completed));
        assert!(!can_transition(RunStatus::Queued, RunStatus::Failed));
        assert!(!can_transition(RunStatus::Queued, RunStatus::Paused));
        // Backwards motion is generally illegal EXCEPT for the lease-expiry
        // re-queue paths (Assigned→Queued, Started→Queued) and the resume
        // re-queue path (Paused→Queued), which are tested in
        // legal_transitions above.
        assert!(!can_transition(RunStatus::Started, RunStatus::Assigned));
    }

    #[test]
    fn event_to_target_status_all() {
        assert_eq!(
            event_to_target_status("run.queued"),
            Some(RunStatus::Queued)
        );
        assert_eq!(
            event_to_target_status("run.assigned"),
            Some(RunStatus::Assigned)
        );
        assert_eq!(
            event_to_target_status("run.started"),
            Some(RunStatus::Started)
        );
        assert_eq!(
            event_to_target_status("run.completed"),
            Some(RunStatus::Completed)
        );
        assert_eq!(
            event_to_target_status("run.failed"),
            Some(RunStatus::Failed)
        );
        assert_eq!(
            event_to_target_status("run.paused"),
            Some(RunStatus::Paused)
        );
        assert_eq!(
            event_to_target_status("workflow.paused"),
            Some(RunStatus::Paused)
        );
        assert_eq!(
            event_to_target_status("run.resumed"),
            Some(RunStatus::Queued)
        );
        assert_eq!(
            event_to_target_status("run.cancelled"),
            Some(RunStatus::Cancelled)
        );
        assert_eq!(
            event_to_target_status("run.lease_expired"),
            Some(RunStatus::Queued)
        );
        assert_eq!(
            event_to_target_status("run.terminalizing"),
            Some(RunStatus::Terminalizing)
        );
    }

    #[test]
    fn event_to_target_status_non_lifecycle() {
        assert_eq!(event_to_target_status("lm.completed"), None);
        assert_eq!(event_to_target_status("timer.fired"), None);
        assert_eq!(event_to_target_status(""), None);
    }

    #[test]
    fn retry_budget_returns_next_zero_based_attempt() {
        let metadata = std::collections::HashMap::from([
            ("attempt".to_string(), "0".to_string()),
            ("max_attempts".to_string(), "3".to_string()),
        ]);

        assert_eq!(next_retry_attempt(&metadata), Some((1, 3)));
    }

    #[test]
    fn retry_budget_rejects_exhausted_or_non_retryable_failure() {
        let exhausted = std::collections::HashMap::from([
            ("attempt".to_string(), "2".to_string()),
            ("max_attempts".to_string(), "3".to_string()),
        ]);
        let non_retryable = std::collections::HashMap::from([
            ("attempt".to_string(), "0".to_string()),
            ("max_attempts".to_string(), "3".to_string()),
            ("workerless_retryable".to_string(), "false".to_string()),
        ]);

        assert_eq!(next_retry_attempt(&exhausted), None);
        assert_eq!(next_retry_attempt(&non_retryable), None);
    }

    #[test]
    fn retry_schedule_uses_bounded_exponential_backoff() {
        let first = std::collections::HashMap::from([
            ("attempt".to_string(), "0".to_string()),
            ("max_attempts".to_string(), "5".to_string()),
            (RETRY_INITIAL_BACKOFF_MS_META.to_string(), "200".to_string()),
            (RETRY_MAX_BACKOFF_MS_META.to_string(), "500".to_string()),
        ]);
        let mut second = first.clone();
        second.insert("attempt".to_string(), "1".to_string());
        let mut capped = first.clone();
        capped.insert("attempt".to_string(), "2".to_string());

        assert_eq!(retry_delay_ms(&first), Some(200));
        assert_eq!(retry_delay_ms(&second), Some(400));
        assert_eq!(retry_delay_ms(&capped), Some(500));
        assert_eq!(retry_ready_at_ms(2_000_000_000, &first), Some(2_200));
    }

    #[test]
    fn retry_delayed_advance_requires_source_and_monotonic_attempt() {
        let authorized = std::collections::HashMap::from([
            (
                RETRY_SOURCE_META.to_string(),
                RETRY_SOURCE_RUN_FAILED.to_string(),
            ),
            ("attempt".to_string(), "1".to_string()),
            ("max_attempts".to_string(), "3".to_string()),
        ]);
        let mut unmarked = authorized.clone();
        unmarked.remove(RETRY_SOURCE_META);
        let mut stale = authorized.clone();
        stale.insert("attempt".to_string(), "0".to_string());

        assert!(is_authorized_failed_retry(
            RunStatus::RetryDelayed,
            0,
            &authorized
        ));
        assert!(!is_authorized_failed_retry(
            RunStatus::RetryDelayed,
            0,
            &unmarked
        ));
        assert!(!is_authorized_failed_retry(
            RunStatus::RetryDelayed,
            0,
            &stale
        ));
        assert!(!is_authorized_failed_retry(
            RunStatus::Failed,
            0,
            &authorized
        ));
    }

    #[test]
    fn retryable_failure_targets_nonterminal_retry_delay() {
        let metadata = std::collections::HashMap::from([
            ("attempt".to_string(), "0".to_string()),
            ("max_attempts".to_string(), "3".to_string()),
        ]);

        assert_eq!(
            record_target_status("run.failed", &metadata),
            Some(RunStatus::RetryDelayed)
        );
        assert_eq!(
            record_target_status("run.failed", &std::collections::HashMap::new()),
            Some(RunStatus::Failed)
        );
    }

    #[test]
    fn a_disconnected_attempt_reads_as_disconnected() {
        let mut metadata = std::collections::HashMap::new();
        record_lease_worker_termination(&mut metadata, 1, "OOMKilled");
        record_lease_worker_termination(&mut metadata, 2, WORKER_DISCONNECTED_REASON);
        assert_eq!(
            lease_worker_terminations_suffix(&metadata),
            " (attempt 1: worker OOM-killed; attempt 2: worker disconnected)"
        );
    }

    #[test]
    fn lease_worker_terminations_keep_earlier_attempts_in_order() {
        let mut metadata = std::collections::HashMap::new();
        assert_eq!(lease_worker_terminations_suffix(&metadata), "");

        record_lease_worker_termination(&mut metadata, 1, "Error");
        record_lease_worker_termination(&mut metadata, 2, "OOMKilled");
        // Re-applying attempt 2's record changes nothing.
        record_lease_worker_termination(&mut metadata, 2, "Error");
        assert_eq!(
            lease_worker_terminations(&metadata),
            vec![(1, "Error".to_string()), (2, "OOMKilled".to_string())]
        );
        assert_eq!(
            lease_worker_terminations_suffix(&metadata),
            " (attempt 1: worker terminated (Error); attempt 2: worker OOM-killed)"
        );

        // Malformed history reads as none rather than failing the run.
        metadata.insert(
            LEASE_WORKER_TERMINATIONS_META.to_string(),
            "not json".to_string(),
        );
        assert!(lease_worker_terminations(&metadata).is_empty());

        let mut bounded = std::collections::HashMap::new();
        for attempt in 1..=40 {
            record_lease_worker_termination(&mut bounded, attempt, "OOMKilled");
        }
        assert_eq!(
            lease_worker_terminations(&bounded).len(),
            MAX_LEASE_WORKER_TERMINATIONS
        );
    }
}
