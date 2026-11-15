//! Partition-owned recovery for durable run terminal obligations.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use engine_core::error::EngineResult;
use engine_core::lease_fence::{
    strip_reserved_fence_metadata, LeaseExpiryReason, LeaseFence, LeaseFenceKind,
    RUN_META_LEASE_SESSION_HASH,
};
use engine_core::storage_contract::TerminalObligationStore;
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record, TerminalObligationCursor};
use engine_proto::Run;
use prost::Message;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::run_state_machine::{self, RunStatus};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_STRANDED_GRACE: Duration = Duration::from_secs(5);
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// How long an observed run's trace must go without a new span, after its
/// root has arrived, before the run ends. Exporters batch spans, so a root
/// can land before its children.
const DEFAULT_OBSERVED_SETTLE_WINDOW: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct TerminalSupervisorConfig {
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub stranded_grace: Duration,
    pub observed_settle_window: Duration,
}

impl Default for TerminalSupervisorConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
            stranded_grace: DEFAULT_STRANDED_GRACE,
            observed_settle_window: DEFAULT_OBSERVED_SETTLE_WINDOW,
        }
    }
}

#[derive(Debug)]
struct RecoveryAction {
    record: Record,
    outcome: &'static str,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_partition_terminal_supervisor<M: TerminalObligationStore + ?Sized + 'static>(
    partition_id: u32,
    node_id: String,
    store: Arc<M>,
    segment: Arc<dyn Segment>,
    replay_ready: watch::Receiver<bool>,
    is_leader: watch::Receiver<bool>,
    cancel: CancellationToken,
    config: TerminalSupervisorConfig,
) {
    info!(
        partition_id,
        node_id, "terminal obligation supervisor starting"
    );
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut scan_cursor = None;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                if !*replay_ready.borrow() || !*is_leader.borrow() {
                    continue;
                }
                if let Err(error) = poll_once(
                    partition_id,
                    &node_id,
                    store.as_ref(),
                    segment.as_ref(),
                    config,
                    &mut scan_cursor,
                ).await {
                    error!(partition_id, error = %error, "terminal obligation supervisor cycle failed");
                }
            }
        }
    }
    info!(
        partition_id,
        node_id, "terminal obligation supervisor stopped"
    );
}

async fn poll_once<M: TerminalObligationStore + ?Sized>(
    partition_id: u32,
    node_id: &str,
    store: &M,
    segment: &dyn Segment,
    config: TerminalSupervisorConfig,
    scan_cursor: &mut Option<TerminalObligationCursor>,
) -> EngineResult<()> {
    let now_ms = now_unix_ms();
    let limit = config.batch_size.max(1);
    let obligations = store
        .scan_terminal_obligations(partition_id, limit, scan_cursor.clone())
        .await?;
    let next_cursor = (obligations.len() == limit).then(|| {
        let last = obligations.last().expect("non-empty full obligation page");
        TerminalObligationCursor {
            project_id: last.project_id.clone(),
            run_id: last.run_id.clone(),
        }
    });

    for entry in obligations {
        let Ok(snapshot) = Run::decode(entry.run_data.as_slice()) else {
            warn!(
                partition_id,
                project_id = %entry.project_id,
                run_id = %entry.run_id,
                "terminal obligation contains an invalid Run projection"
            );
            continue;
        };
        if snapshot.project_id != entry.project_id || snapshot.run_id != entry.run_id {
            warn!(
                partition_id,
                project_id = %entry.project_id,
                run_id = %entry.run_id,
                "terminal obligation identity does not match its Run projection"
            );
            continue;
        }
        let Some(current_data) = store
            .load_terminal_obligation_run(&snapshot.project_id, &snapshot.run_id)
            .await?
        else {
            continue;
        };
        let Ok(current) = Run::decode(current_data.as_slice()) else {
            warn!(
                partition_id,
                project_id = %snapshot.project_id,
                run_id = %snapshot.run_id,
                "terminal obligation points to an invalid current Run projection"
            );
            continue;
        };
        if obligation_generation(&snapshot) != obligation_generation(&current) {
            continue;
        }

        let age_ms = obligation_age_ms(&current, now_ms);
        crate::metrics::record_terminal_obligation_observed(
            partition_id,
            current.status.as_str(),
            age_ms,
        );
        let Some(action) = recovery_action(partition_id, node_id, &current, now_ms, config) else {
            continue;
        };

        debug!(
            partition_id,
            project_id = %current.project_id,
            run_id = %current.run_id,
            status = %current.status,
            outcome = action.outcome,
            "terminal obligation recovery append"
        );
        // An expiry settles the attempts its lease left open in the same
        // batch, so the next worker can run them instead of waiting on them.
        // Without that read the expiry waits for the next tick: written alone
        // it would leave those attempts open for good.
        let mut batch = Vec::new();
        if action.record.event_type == engine_core::lease_fence::RUN_LEASE_EXPIRED_EVENT {
            let settled = match segment
                .list_nonterminal_activation_authority(&current.project_id, &current.run_id)
                .await
                .map_err(|error| error.to_string())
                .and_then(|states| {
                    engine_core::activation::lease_loss_failures(&states, now_ms * 1_000_000)
                        .map_err(|error| error.to_string())
                }) {
                Ok(records) => records,
                Err(error) => {
                    warn!(
                        partition_id,
                        project_id = %current.project_id,
                        run_id = %current.run_id,
                        error = %error,
                        "lease expiry deferred: could not settle the lease's open activations"
                    );
                    continue;
                }
            };
            batch.extend(settled);
        }
        batch.push(action.record);
        match segment.append_batch_with_outcome(&batch).await {
            Ok(outcome) => crate::metrics::record_terminal_supervisor_action(
                partition_id,
                action.outcome,
                if outcome.appended.last() == Some(&true) {
                    "appended"
                } else {
                    "deduplicated"
                },
            ),
            Err(error) => {
                crate::metrics::record_terminal_supervisor_action(
                    partition_id,
                    action.outcome,
                    "append_error",
                );
                warn!(
                    partition_id,
                    project_id = %current.project_id,
                    run_id = %current.run_id,
                    outcome = action.outcome,
                    error = %error,
                    "terminal obligation recovery append failed"
                );
            }
        }
    }
    *scan_cursor = next_cursor;
    Ok(())
}

fn recovery_action(
    partition_id: u32,
    node_id: &str,
    run: &Run,
    now_ms: i64,
    config: TerminalSupervisorConfig,
) -> Option<RecoveryAction> {
    let status = RunStatus::from_str(&run.status)?;
    if status == RunStatus::Terminalizing {
        return terminal_record(run, now_ms);
    }

    let cancel_requested = run
        .metadata
        .get(run_state_machine::CANCELLATION_REQUESTED_META)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    if cancel_requested {
        return Some(RecoveryAction {
            record: terminalizing_record(partition_id, node_id, run, now_ms, "cancelled"),
            outcome: "reserve_cancelled",
        });
    }

    let deadline_exceeded = run
        .metadata
        .get(run_state_machine::RUN_DEADLINE_MS_META)
        .and_then(|value| value.parse::<i64>().ok())
        .is_some_and(|deadline_ms| deadline_ms > 0 && now_ms >= deadline_ms);
    if deadline_exceeded {
        return Some(RecoveryAction {
            record: terminalizing_record(partition_id, node_id, run, now_ms, "timeout"),
            outcome: "reserve_timeout",
        });
    }

    // An observed run has no worker or lease, so it is never stranded; it
    // ends once its root has arrived and the trace has gone quiet.
    if engine_core::journal_contract::is_observed_metadata(&run.metadata) {
        return observed_settled_record(partition_id, node_id, run, now_ms, config);
    }

    if status == RunStatus::Paused
        && run
            .metadata
            .get(run_state_machine::WAIT_DEADLINE_MS_META)
            .and_then(|value| value.parse::<i64>().ok())
            .is_some_and(|deadline| now_ms >= deadline)
    {
        return Some(RecoveryAction {
            record: terminalizing_record(partition_id, node_id, run, now_ms, "wait_timeout"),
            outcome: "reserve_wait_timeout",
        });
    }

    let attempts_exhausted = run
        .metadata
        .get("max_attempts")
        .and_then(|value| value.parse::<u32>().ok())
        .is_some_and(|max_attempts| max_attempts > 0 && run.attempt >= max_attempts);
    if attempts_exhausted {
        return Some(RecoveryAction {
            record: terminalizing_record(partition_id, node_id, run, now_ms, "attempts_exhausted"),
            outcome: "reserve_attempts_exhausted",
        });
    }

    match status {
        RunStatus::RetryDelayed => retry_record(run),
        RunStatus::Assigned | RunStatus::Started
            if run.lease_id.is_empty() && missing_lease_requires_recovery(run) =>
        {
            let grace_ms = i64::try_from(config.stranded_grace.as_millis()).unwrap_or(i64::MAX);
            (obligation_age_ms(run, now_ms) >= grace_ms).then(|| RecoveryAction {
                record: terminalizing_record(partition_id, node_id, run, now_ms, "stranded"),
                outcome: "reserve_stranded",
            })
        }
        RunStatus::Assigned | RunStatus::Started
            if run.lease_expires_at_ms > 0 && now_ms >= run.lease_expires_at_ms =>
        {
            expired_lease_record(run, now_ms)
        }
        RunStatus::Queued
        | RunStatus::Assigned
        | RunStatus::Started
        | RunStatus::Paused
        | RunStatus::Completed
        | RunStatus::Failed
        | RunStatus::Cancelled => None,
        RunStatus::Terminalizing => unreachable!(),
    }
}

/// Reserve the end of an observed run whose trace has settled: it has started,
/// a root has arrived, and no span has come for the settle window.
fn observed_settled_record(
    partition_id: u32,
    node_id: &str,
    run: &Run,
    now_ms: i64,
    config: TerminalSupervisorConfig,
) -> Option<RecoveryAction> {
    if RunStatus::from_str(&run.status)? != RunStatus::Started
        || !run
            .metadata
            .contains_key(run_state_machine::OBSERVED_ROOT_KIND_META)
    {
        return None;
    }
    let last_span_ms = run
        .metadata
        .get(run_state_machine::OBSERVED_LAST_SPAN_MS_META)
        .and_then(|value| value.parse::<i64>().ok())?;
    let window_ms = i64::try_from(config.observed_settle_window.as_millis()).unwrap_or(i64::MAX);
    if now_ms.saturating_sub(last_span_ms) < window_ms {
        return None;
    }
    let failed = run
        .metadata
        .get(run_state_machine::OBSERVED_OUTCOME_META)
        .is_some_and(|outcome| outcome == "failed");
    let (outcome, metric) = if failed {
        ("observed_failed", "reserve_observed_failed")
    } else {
        ("observed_completed", "reserve_observed_completed")
    };
    Some(RecoveryAction {
        record: terminalizing_record(partition_id, node_id, run, now_ms, outcome),
        outcome: metric,
    })
}

fn missing_lease_requires_recovery(run: &Run) -> bool {
    missing_lease_requires_recovery_with_policy(
        run,
        crate::orchestrator::lease_backed_push_enabled(),
    )
}

fn missing_lease_requires_recovery_with_policy(run: &Run, lease_backed_push: bool) -> bool {
    let is_pull = run
        .metadata
        .get("dispatch_mode")
        .is_some_and(|mode| mode == "pull");
    is_pull || lease_backed_push
}

fn retry_record(run: &Run) -> Option<RecoveryAction> {
    let (next_attempt, max_attempts) = run_state_machine::next_retry_attempt(&run.metadata)?;
    let failed_at_ms = obligation_updated_at_ms(run);
    let failed = Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: "run.failed".to_string(),
        data: Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "error_message": run.error_message,
                "error_code": run.error_code,
            }))
            .unwrap_or_default(),
        ),
        timestamp_ns: failed_at_ms.saturating_mul(1_000_000),
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: run.metadata.clone(),
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    let run_data = run.encode_to_vec();
    Some(RecoveryAction {
        record: crate::orchestrator::build_retry_record(
            &failed,
            Some(run_data.as_slice()),
            next_attempt as i32,
            max_attempts as i32,
        ),
        outcome: "retry_queued",
    })
}

fn expired_lease_record(run: &Run, now_ms: i64) -> Option<RecoveryAction> {
    let (record, retryable) = lease_loss_record(run, now_ms, LeaseLoss::Timeout)?;
    Some(RecoveryAction {
        record,
        outcome: if retryable {
            "lease_requeue"
        } else {
            "lease_exhausted"
        },
    })
}

/// Why the runtime takes a lease back from the worker that holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseLoss<'a> {
    /// The lease passed its expiry without a renewal.
    Timeout,
    /// The worker's container terminated while it held the lease. `reason` is
    /// the container's termination reason, e.g. `OOMKilled` or `Error`.
    WorkerTerminated { reason: &'a str },
}

impl LeaseLoss<'_> {
    fn expiry_reason(self) -> LeaseExpiryReason {
        match self {
            Self::Timeout => LeaseExpiryReason::Timeout,
            Self::WorkerTerminated { .. } => LeaseExpiryReason::WorkerDisconnect,
        }
    }

    fn exhausted_error(self) -> (&'static str, String) {
        match self {
            Self::Timeout => (
                "LEASE_RETRY_EXHAUSTED",
                "execution lease expired and retry budget was exhausted".to_string(),
            ),
            Self::WorkerTerminated { reason } if reason.eq_ignore_ascii_case("OOMKilled") => (
                "WORKER_OOM_KILLED",
                "worker was OOM-killed during this run and retry budget was exhausted".to_string(),
            ),
            Self::WorkerTerminated { reason } => (
                "WORKER_TERMINATED",
                format!(
                    "worker terminated ({reason}) during this run and retry budget was exhausted"
                ),
            ),
        }
    }
}

/// Build the fenced record that takes a lost lease back: `run.lease_expired`
/// while the run has attempts left, `run.failed` once the budget is spent.
/// Returns the record and whether the run will be retried, or `None` when the
/// projected run carries no lease to fence against.
pub fn lease_loss_record(run: &Run, now_ms: i64, loss: LeaseLoss<'_>) -> Option<(Record, bool)> {
    let max_attempts = run
        .metadata
        .get("max_attempts")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(DEFAULT_MAX_ATTEMPTS);
    let retryable = run.attempt.saturating_add(1) < max_attempts;
    let event_type = if retryable {
        "run.lease_expired"
    } else {
        "run.failed"
    };
    let mut metadata = run.metadata.clone();
    strip_reserved_fence_metadata(&mut metadata);
    // A termination named by an earlier record must not ride along on this
    // one: the projection reads it to record why this attempt ended.
    run_state_machine::strip_runtime_lease_loss_metadata(&mut metadata);
    metadata.remove("lease_worker_id");
    metadata.insert("attempt".to_string(), run.attempt.to_string());
    metadata.insert("max_attempts".to_string(), max_attempts.to_string());
    metadata.insert("component_name".to_string(), run.component_name.clone());
    if let LeaseLoss::WorkerTerminated { reason } = loss {
        metadata.insert(
            run_state_machine::LEASE_RECOVERY_REASON_META.to_string(),
            "worker_terminated".to_string(),
        );
        metadata.insert(
            run_state_machine::WORKER_TERMINATION_REASON_META.to_string(),
            reason.to_string(),
        );
        metadata.insert("lease_worker_id".to_string(), run.lease_worker_id.clone());
    }
    let data = if retryable {
        Vec::new()
    } else {
        // The code follows the last attempt's loss; the message also names
        // earlier attempts' worker terminations.
        let (error_code, mut error_message) = loss.exhausted_error();
        error_message.push_str(&run_state_machine::lease_worker_terminations_suffix(
            &run.metadata,
        ));
        serde_json::to_vec(&serde_json::json!({
            "error_message": error_message,
            "error_code": error_code,
        }))
        .unwrap_or_default()
    };
    let desired = Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: event_type.to_string(),
        data: Bytes::from(data),
        timestamp_ns: now_ms.saturating_mul(1_000_000),
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    let mut fenced = fenced_expiry(desired, run, now_ms, loss.expiry_reason())?;
    shed_run_continuation(&mut fenced, run);
    Some((fenced, retryable))
}

/// Fence the expiry with the lease the projection shows. The segment decides
/// it against the lease entry in log order: a renewal or completion that
/// landed first makes this record lose at append time, and nothing is written.
fn fenced_expiry(
    mut desired: Record,
    run: &Run,
    now_ms: i64,
    expiry_reason: LeaseExpiryReason,
) -> Option<Record> {
    if run.lease_id.is_empty() || run.lease_worker_id.is_empty() || run.lease_expires_at_ms <= 0 {
        return None;
    }
    LeaseFence {
        kind: LeaseFenceKind::LeaseExpiry,
        lease_id: run.lease_id.clone(),
        worker_id: run.lease_worker_id.clone(),
        session_hash: run
            .metadata
            .get(RUN_META_LEASE_SESSION_HASH)
            .cloned()
            .unwrap_or_default(),
        attempt: run.attempt,
        received_at_ms: now_ms,
        expiry_reason: Some(expiry_reason),
    }
    .stamp(&mut desired);
    Some(desired)
}

fn terminalizing_record(
    partition_id: u32,
    node_id: &str,
    run: &Run,
    now_ms: i64,
    outcome: &str,
) -> Record {
    let reservation = format!(
        "{partition_id}:{}:{}:{}:{outcome}",
        run.project_id,
        run.run_id,
        obligation_generation(run)
    );
    let mut metadata = run.metadata.clone();
    strip_reserved_fence_metadata(&mut metadata);
    metadata.insert(
        run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
        reservation,
    );
    metadata.insert(
        run_state_machine::TERMINAL_OUTCOME_META.to_string(),
        outcome.to_string(),
    );
    metadata.insert(
        run_state_machine::TERMINALIZING_OWNER_META.to_string(),
        format!("{node_id}/partition-{partition_id}"),
    );
    metadata.insert(
        run_state_machine::TERMINALIZING_REASON_META.to_string(),
        outcome.to_string(),
    );
    let mut record = Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: "run.terminalizing".to_string(),
        data: Bytes::new(),
        timestamp_ns: now_ms.saturating_mul(1_000_000),
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    shed_run_continuation(&mut record, run);
    record
}

/// Leave the run's workflow continuation out of a record built from its
/// metadata when the copy would breach the journal limit. The projection
/// keeps the continuation on the run row for the next dispatch, so a lease
/// expiry or a terminal outcome never needs to carry it.
fn shed_run_continuation(record: &mut Record, run: &Run) {
    engine_core::journal_contract::shed_oversized_continuation(
        record,
        run.metadata
            .get(engine_core::journal_contract::META_CONTINUATION_B64)
            .map(String::as_str),
    );
}

fn terminal_record(run: &Run, now_ms: i64) -> Option<RecoveryAction> {
    let outcome = run
        .metadata
        .get(run_state_machine::TERMINAL_OUTCOME_META)
        .map(String::as_str)
        .unwrap_or("stranded");
    if matches!(outcome, "observed_completed" | "observed_failed") {
        return Some(observed_terminal_record(run, outcome, now_ms));
    }
    let (event_type, error_code, error_message, metric_outcome) = match outcome {
        "cancelled" => (
            "run.cancelled",
            "",
            "run cancellation completed",
            "cancelled",
        ),
        "timeout" => (
            "run.failed",
            "RUN_DEADLINE_EXCEEDED",
            "run wall-clock deadline exceeded",
            "timeout",
        ),
        "wait_timeout" => (
            "run.failed",
            "WAIT_TIMEOUT",
            "workflow wait deadline exceeded",
            "wait_timeout",
        ),
        "attempts_exhausted" => (
            "run.failed",
            "RUN_ATTEMPTS_EXHAUSTED",
            "run attempt budget was exhausted before a terminal outcome",
            "attempts_exhausted",
        ),
        _ => (
            "run.failed",
            "RUN_STRANDED",
            "run lost executable authority before reaching a terminal state",
            "stranded",
        ),
    };
    // A failure also names earlier attempts' worker terminations, as a lease
    // loss's does: the last attempt's cause alone hid an OOM kill.
    let mut error_message = error_message.to_string();
    if event_type == "run.failed" {
        error_message.push_str(&run_state_machine::lease_worker_terminations_suffix(
            &run.metadata,
        ));
    }
    let data = serde_json::to_vec(&serde_json::json!({
        "error_code": error_code,
        "error_message": error_message,
    }))
    .unwrap_or_default();
    let mut metadata = run.metadata.clone();
    strip_reserved_fence_metadata(&mut metadata);
    // A supervisor terminal outcome is final even when the original run
    // carried a retry budget. Without this marker `run.failed` could project
    // back to `retry_delayed` instead of consuming the reservation.
    metadata.insert("workerless_retryable".to_string(), "false".to_string());
    metadata.insert("error_code".to_string(), error_code.to_string());
    let mut record = Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: event_type.to_string(),
        data: Bytes::from(data),
        timestamp_ns: now_ms.saturating_mul(1_000_000),
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    shed_run_continuation(&mut record, run);
    Some(RecoveryAction {
        record,
        outcome: metric_outcome,
    })
}

/// End an observed run as its trace did. The run completes with the output
/// the projection gathered, or fails with the root's error; either way it
/// ends at its latest span's time.
fn observed_terminal_record(run: &Run, outcome: &str, now_ms: i64) -> RecoveryAction {
    let ended_ns = run
        .metadata
        .get(run_state_machine::OBSERVED_ENDED_AT_NS_META)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|ns| *ns > 0)
        .unwrap_or_else(|| now_ms.saturating_mul(1_000_000));
    let (event_type, data, metric) = if outcome == "observed_failed" {
        let field = |key: &str, fallback: &str| {
            run.metadata
                .get(key)
                .filter(|value| !value.is_empty())
                .cloned()
                .unwrap_or_else(|| fallback.to_string())
        };
        let data = serde_json::json!({
            "error_code": field(run_state_machine::OBSERVED_ERROR_CODE_META, "OBSERVED_FAILED"),
            "error_message": field(
                run_state_machine::OBSERVED_ERROR_MESSAGE_META,
                "the trace's root span ended in error",
            ),
        });
        ("run.failed", data, "observed_failed")
    } else {
        let output = serde_json::from_slice::<serde_json::Value>(&run.output_data)
            .unwrap_or(serde_json::Value::Null);
        (
            "run.completed",
            serde_json::json!({ "output": output }),
            "observed_completed",
        )
    };
    let mut metadata = run.metadata.clone();
    strip_reserved_fence_metadata(&mut metadata);
    metadata.insert("workerless_retryable".to_string(), "false".to_string());
    RecoveryAction {
        record: Record {
            offset: Offset::ZERO,
            project_id: run.project_id.clone(),
            run_id: run.run_id.clone(),
            event_type: event_type.to_string(),
            data: Bytes::from(serde_json::to_vec(&data).unwrap_or_default()),
            timestamp_ns: ended_ns,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        },
        outcome: metric,
    }
}

fn obligation_generation(run: &Run) -> u64 {
    run.metadata
        .get(run_state_machine::TERMINAL_OBLIGATION_OFFSET_META)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
}

fn obligation_updated_at_ms(run: &Run) -> i64 {
    run.metadata
        .get(run_state_machine::TERMINAL_OBLIGATION_UPDATED_AT_MS_META)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or_default()
}

fn obligation_age_ms(run: &Run, now_ms: i64) -> i64 {
    now_ms.saturating_sub(obligation_updated_at_ms(run)).max(0)
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::lease_fence::{lease_fence, META_FENCE_KIND};
    use engine_core::types::TerminalObligationEntry;
    use engine_journal_memory::MemorySegment;
    use std::collections::HashMap;

    fn grace(stranded_grace: Duration) -> TerminalSupervisorConfig {
        TerminalSupervisorConfig {
            stranded_grace,
            ..Default::default()
        }
    }

    fn run(status: &str, now_ms: i64) -> Run {
        Run {
            project_id: "project-1".to_string(),
            run_id: "run-1".to_string(),
            deployment_id: "deployment-1".to_string(),
            component_name: "workflow".to_string(),
            status: status.to_string(),
            attempt: 0,
            metadata: HashMap::from([
                ("attempt".to_string(), "0".to_string()),
                ("max_attempts".to_string(), "3".to_string()),
                (
                    run_state_machine::TERMINAL_OBLIGATION_OFFSET_META.to_string(),
                    "7".to_string(),
                ),
                (
                    run_state_machine::TERMINAL_OBLIGATION_UPDATED_AT_MS_META.to_string(),
                    now_ms.to_string(),
                ),
            ]),
            ..Default::default()
        }
    }

    fn observed_run(last_span_ms: i64) -> Run {
        let mut run = run("started", 1_000);
        run.metadata.insert(
            engine_core::journal_contract::META_ORIGIN.to_string(),
            engine_core::journal_contract::ORIGIN_OTLP.to_string(),
        );
        // Observed runs have no worker and no lease; a pull-dispatched native
        // run in this state would be stranded.
        run.metadata
            .insert("dispatch_mode".to_string(), "pull".to_string());
        run.metadata.remove("max_attempts");
        run.metadata.insert(
            run_state_machine::OBSERVED_LAST_SPAN_MS_META.to_string(),
            last_span_ms.to_string(),
        );
        run.metadata.insert(
            run_state_machine::OBSERVED_ENDED_AT_NS_META.to_string(),
            "1900000000".to_string(),
        );
        run
    }

    fn with_root(mut run: Run, outcome: &str) -> Run {
        run.metadata.insert(
            run_state_machine::OBSERVED_ROOT_KIND_META.to_string(),
            "true".to_string(),
        );
        run.metadata.insert(
            run_state_machine::OBSERVED_OUTCOME_META.to_string(),
            outcome.to_string(),
        );
        run
    }

    fn settle(run: &Run, now_ms: i64) -> Option<RecoveryAction> {
        recovery_action(
            0,
            "node-1",
            run,
            now_ms,
            TerminalSupervisorConfig::default(),
        )
    }

    #[test]
    fn observed_run_is_never_stranded() {
        // Long past the stranded grace, with no lease and no root yet.
        let run = observed_run(1_000);
        assert!(settle(&run, 600_000).is_none());
    }

    #[test]
    fn observed_run_ends_only_once_its_trace_settles() {
        let run = with_root(observed_run(1_000), "completed");
        assert!(settle(&run, 10_999).is_none());
        let action = settle(&run, 11_000).expect("settled run reserves its end");
        assert_eq!(action.outcome, "reserve_observed_completed");
        assert_eq!(action.record.event_type, "run.terminalizing");
        assert_eq!(
            action.record.metadata[run_state_machine::TERMINAL_OUTCOME_META],
            "observed_completed"
        );
    }

    #[test]
    fn observed_span_inside_the_window_restarts_it() {
        let run = with_root(observed_run(1_000), "completed");
        assert!(settle(&run, 11_000).is_some());
        // A span that arrived at 9s pushes the end out to 19s.
        let mut later = run.clone();
        later.metadata.insert(
            run_state_machine::OBSERVED_LAST_SPAN_MS_META.to_string(),
            "9000".to_string(),
        );
        assert!(settle(&later, 11_000).is_none());
        assert!(settle(&later, 19_000).is_some());
    }

    #[test]
    fn observed_run_without_a_root_times_out_at_its_deadline() {
        let mut run = observed_run(1_000);
        run.metadata.insert(
            run_state_machine::RUN_DEADLINE_MS_META.to_string(),
            "60000".to_string(),
        );
        assert!(settle(&run, 59_999).is_none());
        let action = settle(&run, 60_000).expect("deadline reserves a timeout");
        assert_eq!(action.outcome, "reserve_timeout");
    }

    #[test]
    fn observed_completed_reservation_completes_with_the_gathered_output() {
        let mut run = with_root(observed_run(1_000), "completed");
        run.output_data = br#"{"answer":42}"#.to_vec();
        let reservation = settle(&run, 11_000).unwrap().record;
        let mut terminalizing = run.clone();
        terminalizing.status = "terminalizing".to_string();
        terminalizing.metadata = reservation.metadata;

        let action = settle(&terminalizing, 12_000).expect("terminal record");
        assert_eq!(action.outcome, "observed_completed");
        assert_eq!(action.record.event_type, "run.completed");
        // The run ends at its latest span, not when the supervisor noticed.
        assert_eq!(action.record.timestamp_ns, 1_900_000_000);
        assert!(engine_core::journal_contract::is_observed_record(
            &action.record
        ));
        let data: serde_json::Value = serde_json::from_slice(&action.record.data).unwrap();
        assert_eq!(data["output"]["answer"], 42);
    }

    #[test]
    fn observed_failed_reservation_fails_with_the_root_error() {
        let mut run = with_root(observed_run(1_000), "failed");
        run.metadata.insert(
            run_state_machine::OBSERVED_ERROR_CODE_META.to_string(),
            "ValueError".to_string(),
        );
        let reservation = settle(&run, 11_000).unwrap();
        assert_eq!(reservation.outcome, "reserve_observed_failed");
        let mut terminalizing = run.clone();
        terminalizing.status = "terminalizing".to_string();
        terminalizing.metadata = reservation.record.metadata;

        let action = settle(&terminalizing, 12_000).expect("terminal record");
        assert_eq!(action.record.event_type, "run.failed");
        assert_eq!(action.record.metadata["workerless_retryable"], "false");
        let data: serde_json::Value = serde_json::from_slice(&action.record.data).unwrap();
        assert_eq!(data["error_code"], "ValueError");
        assert_eq!(
            data["error_message"],
            "the trace's root span ended in error"
        );
    }

    #[test]
    fn retry_delayed_obligation_reconstructs_retry_queue_record() {
        let action = recovery_action(
            0,
            "node-1",
            &run("retry_delayed", 1_000),
            2_000,
            grace(Duration::from_secs(5)),
        )
        .expect("retry action");
        assert_eq!(action.outcome, "retry_queued");
        assert_eq!(action.record.event_type, "run.queued");
        assert_eq!(
            action.record.metadata.get("attempt").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn exhausted_timeout_names_an_earlier_attempts_worker_termination() {
        // Attempt 3 of 3 times out after attempt 2's worker was OOM-killed.
        let mut last = run("started", 1_000);
        last.attempt = 2;
        last.lease_id = "lease-3".to_string();
        last.lease_worker_id = "worker-3".to_string();
        last.lease_expires_at_ms = 1_500;
        last.metadata.insert(
            RUN_META_LEASE_SESSION_HASH.to_string(),
            "session-hash-3".to_string(),
        );
        last.metadata
            .insert("max_attempts".to_string(), "3".to_string());
        run_state_machine::record_lease_worker_termination(&mut last.metadata, 2, "OOMKilled");
        // A reason left over from an earlier record must not ride along.
        last.metadata.insert(
            run_state_machine::WORKER_TERMINATION_REASON_META.to_string(),
            "OOMKilled".to_string(),
        );

        let (record, retryable) =
            lease_loss_record(&last, 2_000, LeaseLoss::Timeout).expect("fenced record");
        assert!(!retryable);
        assert_eq!(record.event_type, "run.failed");
        assert!(!record
            .metadata
            .contains_key(run_state_machine::WORKER_TERMINATION_REASON_META));
        let data: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        assert_eq!(data["error_code"], "LEASE_RETRY_EXHAUSTED");
        assert_eq!(
            data["error_message"],
            "execution lease expired and retry budget was exhausted (attempt 2: worker OOM-killed)"
        );

        // Without earlier terminations the message is unchanged.
        let mut plain = last.clone();
        plain
            .metadata
            .remove(run_state_machine::LEASE_WORKER_TERMINATIONS_META);
        let (record, _) =
            lease_loss_record(&plain, 2_000, LeaseLoss::Timeout).expect("fenced record");
        let data: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        assert_eq!(
            data["error_message"],
            "execution lease expired and retry budget was exhausted"
        );
    }

    #[test]
    fn a_resumed_runs_large_continuation_does_not_block_its_lease_expiry_or_terminal() {
        // After a timer resume the run row holds a continuation that would
        // breach the journal limit if every record copied it.
        let continuation = "A".repeat(250_000);
        let mut active = run("started", 1_000);
        active.lease_id = "lease-1".to_string();
        active.lease_worker_id = "worker-1".to_string();
        active.lease_expires_at_ms = 1_500;
        active.metadata.insert(
            RUN_META_LEASE_SESSION_HASH.to_string(),
            "session-hash-1".to_string(),
        );
        active.metadata.insert(
            engine_core::journal_contract::META_CONTINUATION_B64.to_string(),
            continuation,
        );

        let (expiry, retryable) =
            lease_loss_record(&active, 2_000, LeaseLoss::Timeout).expect("fenced record");
        assert!(retryable);
        assert_eq!(expiry.event_type, "run.lease_expired");
        assert!(lease_fence(&expiry).unwrap().is_some());
        assert!(!expiry
            .metadata
            .contains_key(engine_core::journal_contract::META_CONTINUATION_B64));
        engine_core::journal_contract::validate_append_records(&[expiry])
            .expect("the lease expiry fits the journal");

        let terminalizing = terminalizing_record(0, "node-a", &active, 2_000, "stranded");
        let terminal = terminal_record(&active, 2_000)
            .expect("terminal record")
            .record;
        engine_core::journal_contract::validate_append_records(&[terminalizing, terminal])
            .expect("the supervisor's terminal records fit the journal");
    }

    #[test]
    fn expired_lease_obligation_is_a_plain_fenced_expiry() {
        let mut active = run("started", 1_000);
        active.lease_id = "lease-1".to_string();
        active.lease_worker_id = "worker-1".to_string();
        active.lease_expires_at_ms = 1_500;
        active.metadata.insert(
            RUN_META_LEASE_SESSION_HASH.to_string(),
            "session-hash-1".to_string(),
        );
        // A leaked fence key from an earlier record must not make the
        // supervisor's own record look like it was authored by that lease.
        active
            .metadata
            .insert(META_FENCE_KIND.to_string(), "pull_completion".to_string());
        let action = recovery_action(0, "node-1", &active, 2_000, grace(Duration::from_secs(5)))
            .expect("expiry action");
        assert_eq!(action.outcome, "lease_requeue");
        assert_eq!(action.record.event_type, "run.lease_expired");
        let fence = lease_fence(&action.record)
            .expect("valid fence")
            .expect("fenced");
        assert_eq!(fence.kind, LeaseFenceKind::LeaseExpiry);
        assert_eq!(fence.lease_id, "lease-1");
        assert_eq!(fence.worker_id, "worker-1");
        assert_eq!(fence.session_hash, "session-hash-1");
        assert_eq!(fence.expiry_reason, Some(LeaseExpiryReason::Timeout));
        assert_eq!(fence.received_at_ms, 2_000);

        let terminalizing = recovery_action(
            0,
            "node-1",
            &run("started", 1_000),
            2_000,
            grace(Duration::from_secs(5)),
        );
        if let Some(action) = terminalizing {
            assert!(!action.record.metadata.contains_key(META_FENCE_KIND));
        }
    }

    #[test]
    fn terminal_reservation_survives_crash_and_finishes_canonical_outcome() {
        let mut terminalizing = run("terminalizing", 1_000);
        terminalizing.metadata.insert(
            run_state_machine::TERMINAL_RESERVATION_ID_META.to_string(),
            "reservation-1".to_string(),
        );
        terminalizing.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.to_string(),
            "timeout".to_string(),
        );
        let action = recovery_action(
            0,
            "node-1",
            &terminalizing,
            2_000,
            grace(Duration::from_secs(5)),
        )
        .expect("terminal action");
        assert_eq!(action.outcome, "timeout");
        assert_eq!(action.record.event_type, "run.failed");
        assert_eq!(
            action
                .record
                .metadata
                .get(run_state_machine::TERMINAL_RESERVATION_ID_META)
                .map(String::as_str),
            Some("reservation-1")
        );
    }

    #[test]
    fn paused_wait_timeout_is_reserved_only_when_due() {
        let mut run = Run {
            run_id: "wait-run".into(),
            project_id: "project".into(),
            status: "paused".into(),
            ..Default::default()
        };
        assert!(recovery_action(1, "node", &run, 4000, grace(Duration::ZERO)).is_none());
        run.metadata.insert(
            run_state_machine::WAIT_DEADLINE_MS_META.into(),
            "4000".into(),
        );
        assert!(recovery_action(1, "node", &run, 3999, grace(Duration::ZERO)).is_none());
        let due = recovery_action(1, "node", &run, 4000, grace(Duration::ZERO)).unwrap();
        assert_eq!(due.outcome, "reserve_wait_timeout");
        run.status = "terminalizing".into();
        run.metadata.insert(
            run_state_machine::TERMINAL_OUTCOME_META.into(),
            "wait_timeout".into(),
        );
        let terminal = terminal_record(&run, 4001).unwrap();
        assert_eq!(terminal.record.event_type, "run.failed");
        let payload: serde_json::Value = serde_json::from_slice(&terminal.record.data).unwrap();
        assert_eq!(payload["error_code"], "WAIT_TIMEOUT");
    }

    #[test]
    fn deadline_first_reserves_timeout_terminalization() {
        let mut active = run("queued", 1_000);
        active.metadata.insert(
            run_state_machine::RUN_DEADLINE_MS_META.to_string(),
            "1500".to_string(),
        );
        let action = recovery_action(0, "node-1", &active, 2_000, grace(Duration::from_secs(5)))
            .expect("deadline action");
        assert_eq!(action.outcome, "reserve_timeout");
        assert_eq!(action.record.event_type, "run.terminalizing");
    }

    #[test]
    fn queued_work_does_not_expire_after_days() {
        let mut queued = run("queued", 1_000);
        queued
            .metadata
            .insert("dispatch_deadline_ms".into(), "1500".into());
        let three_days_later = 3 * 24 * 60 * 60 * 1_000;
        assert!(recovery_action(
            0,
            "node-1",
            &queued,
            three_days_later,
            grace(Duration::from_secs(5))
        )
        .is_none());
    }

    #[test]
    fn exhausted_attempt_budget_reserves_typed_terminal_failure() {
        let mut exhausted = run("queued", 1_000);
        exhausted.attempt = 3;
        exhausted
            .metadata
            .insert("attempt".to_string(), "3".to_string());
        let reservation = recovery_action(
            0,
            "node-1",
            &exhausted,
            2_000,
            grace(Duration::from_secs(5)),
        )
        .expect("attempt reservation");
        assert_eq!(reservation.outcome, "reserve_attempts_exhausted");

        let mut terminalizing = exhausted;
        terminalizing.status = "terminalizing".to_string();
        terminalizing.metadata.extend(reservation.record.metadata);
        let terminal = recovery_action(
            0,
            "node-2",
            &terminalizing,
            3_000,
            grace(Duration::from_secs(5)),
        )
        .expect("attempt terminal");
        assert_eq!(terminal.outcome, "attempts_exhausted");
        assert_eq!(terminal.record.event_type, "run.failed");
        assert_eq!(
            terminal
                .record
                .metadata
                .get("workerless_retryable")
                .map(String::as_str),
            Some("false")
        );
        assert!(String::from_utf8_lossy(&terminal.record.data).contains("RUN_ATTEMPTS_EXHAUSTED"));
        // Without earlier terminations the message is the plain one.
        let data: serde_json::Value = serde_json::from_slice(&terminal.record.data).unwrap();
        assert_eq!(
            data["error_message"],
            "run attempt budget was exhausted before a terminal outcome"
        );

        // With them, it names each, as a lease loss's failure does.
        let mut with_history = terminalizing.clone();
        run_state_machine::record_lease_worker_termination(
            &mut with_history.metadata,
            2,
            "OOMKilled",
        );
        run_state_machine::record_lease_worker_termination(
            &mut with_history.metadata,
            3,
            run_state_machine::WORKER_DISCONNECTED_REASON,
        );
        let terminal = recovery_action(
            0,
            "node-2",
            &with_history,
            3_000,
            grace(Duration::from_secs(5)),
        )
        .expect("attempt terminal");
        let data: serde_json::Value = serde_json::from_slice(&terminal.record.data).unwrap();
        assert_eq!(data["error_code"], "RUN_ATTEMPTS_EXHAUSTED");
        assert_eq!(
            data["error_message"],
            "run attempt budget was exhausted before a terminal outcome \
             (attempt 2: worker OOM-killed; attempt 3: worker disconnected)"
        );
        // The same run snapshot always yields the same record.
        let again = recovery_action(
            0,
            "node-3",
            &with_history,
            9_000,
            grace(Duration::from_secs(5)),
        )
        .expect("attempt terminal");
        assert_eq!(again.record.data, terminal.record.data);
    }

    #[test]
    fn push_lease_opt_out_does_not_strand_legacy_unleased_execution() {
        let push = run("started", 1_000);
        assert!(!missing_lease_requires_recovery_with_policy(&push, false));

        let mut pull = push;
        pull.metadata
            .insert("dispatch_mode".to_string(), "pull".to_string());
        assert!(missing_lease_requires_recovery_with_policy(&pull, false));
        assert!(missing_lease_requires_recovery_with_policy(
            &run("started", 1_000),
            true
        ));
    }

    struct TestObligationStore {
        run_data: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl TerminalObligationStore for TestObligationStore {
        async fn scan_terminal_obligations(
            &self,
            partition_id: u32,
            limit: usize,
            start_after: Option<TerminalObligationCursor>,
        ) -> EngineResult<Vec<TerminalObligationEntry>> {
            if partition_id != 0 || limit == 0 || start_after.is_some() {
                return Ok(Vec::new());
            }
            Ok(vec![TerminalObligationEntry {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                run_data: self.run_data.clone(),
            }])
        }

        async fn load_terminal_obligation_run(
            &self,
            project_id: &str,
            run_id: &str,
        ) -> EngineResult<Option<Vec<u8>>> {
            Ok((project_id == "project-1" && run_id == "run-1").then(|| self.run_data.clone()))
        }
    }

    #[tokio::test]
    async fn supervisor_waits_for_replay_and_partition_ownership() {
        let mut overdue = run("queued", 1);
        overdue.metadata.insert(
            run_state_machine::RUN_DEADLINE_MS_META.to_string(),
            "1".to_string(),
        );
        let store = Arc::new(TestObligationStore {
            run_data: overdue.encode_to_vec(),
        });
        let segment = Arc::new(MemorySegment::new(0));
        let (replay_tx, replay_rx) = watch::channel(false);
        let (leader_tx, leader_rx) = watch::channel(false);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run_partition_terminal_supervisor(
            0,
            "node-1".to_string(),
            store,
            segment.clone(),
            replay_rx,
            leader_rx,
            cancel.clone(),
            TerminalSupervisorConfig {
                poll_interval: Duration::from_millis(5),
                batch_size: 10,
                stranded_grace: Duration::from_millis(1),
                ..Default::default()
            },
        ));

        leader_tx.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(segment.tail_offset().await.unwrap(), Offset::ZERO);

        replay_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while segment.tail_offset().await.unwrap() == Offset::ZERO {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active owner should append terminal reservation");

        cancel.cancel();
        handle.await.unwrap();
        let records = segment
            .read_range(Offset::ZERO, segment.tail_offset().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            records.len(),
            1,
            "repeated polls must deduplicate reservation"
        );
        assert_eq!(records[0].event_type, "run.terminalizing");
    }

    /// A timed-out lease settles the attempt its dead worker left open in the
    /// expiry's own batch, so the next worker can run that step again instead
    /// of waiting on it forever.
    #[tokio::test]
    async fn an_expired_lease_settles_its_open_activations() {
        use engine_core::activation::{decide_begin, to_engine_record, BeginActivationCommand};
        let segment = MemorySegment::new(0);
        let begin = decide_begin(
            None,
            &BeginActivationCommand {
                request: engine_proto::BeginActivationRequest {
                    project_id: "project-1".to_string(),
                    run_id: "run-1".to_string(),
                    kind: engine_proto::ActivationKind::Step as i32,
                    stable_key: "step/post_to_ledger".to_string(),
                    input_digest: vec![1; 32],
                    definition_digest: vec![2; 32],
                    recovery_policy: engine_proto::ActivationRecoveryPolicy::DurableSteps as i32,
                    worker_session_id: "session-1".to_string(),
                    run_authority: b"run-authority".to_vec(),
                    lease_authority: b"lease-1".to_vec(),
                    ..Default::default()
                },
                issued_fence_token: b"fence-1".to_vec(),
            },
        )
        .unwrap();
        let started = to_engine_record(&begin.records[0], 1).unwrap();
        segment
            .append_batch_with_outcome(std::slice::from_ref(&started))
            .await
            .unwrap();

        let mut leased = run("started", 1_000);
        leased.lease_id = "lease-1".to_string();
        leased.lease_worker_id = "worker-1".to_string();
        leased.lease_expires_at_ms = 1;
        leased.metadata.insert(
            RUN_META_LEASE_SESSION_HASH.to_string(),
            "session-hash-1".to_string(),
        );
        let mut cursor = None;
        poll_once(
            0,
            "node-1",
            &TestObligationStore {
                run_data: leased.encode_to_vec(),
            },
            &segment,
            TerminalSupervisorConfig::default(),
            &mut cursor,
        )
        .await
        .unwrap();

        let records = segment
            .read_range(Offset(1), segment.tail_offset().await.unwrap())
            .await
            .unwrap();
        let event_types: Vec<&str> = records.iter().map(|r| r.event_type.as_str()).collect();
        assert_eq!(event_types, ["workflow.step.failed", "run.lease_expired"]);
        let open = segment
            .list_nonterminal_activation_authority("project-1", "run-1")
            .await
            .unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].status, engine_proto::ActivationStatus::RetryReady);
    }

    /// A paused workflow whose wait deadline passed ends through the journal:
    /// both its reservation and its terminal record pass the record contract.
    #[tokio::test]
    async fn wait_timeout_ends_through_the_journal() {
        let mut paused = run("paused", 1_000);
        paused.metadata.insert(
            run_state_machine::WAIT_DEADLINE_MS_META.to_string(),
            "1".to_string(),
        );
        let segment = MemorySegment::new(0);
        let mut cursor = None;
        poll_once(
            0,
            "node-1",
            &TestObligationStore {
                run_data: paused.encode_to_vec(),
            },
            &segment,
            TerminalSupervisorConfig::default(),
            &mut cursor,
        )
        .await
        .unwrap();
        let reservation = segment
            .read_range(Offset::ZERO, segment.tail_offset().await.unwrap())
            .await
            .unwrap();
        assert_eq!(reservation.len(), 1, "the reservation was refused");
        assert_eq!(
            reservation[0].metadata[run_state_machine::TERMINAL_OUTCOME_META],
            "wait_timeout"
        );

        let mut terminalizing = paused.clone();
        terminalizing.status = "terminalizing".to_string();
        terminalizing.metadata = reservation[0].metadata.clone();
        let mut cursor = None;
        poll_once(
            0,
            "node-1",
            &TestObligationStore {
                run_data: terminalizing.encode_to_vec(),
            },
            &segment,
            TerminalSupervisorConfig::default(),
            &mut cursor,
        )
        .await
        .unwrap();
        let records = segment
            .read_range(Offset::ZERO, segment.tail_offset().await.unwrap())
            .await
            .unwrap();
        assert_eq!(records.len(), 2, "the terminal record was refused");
        assert_eq!(records[1].event_type, "run.failed");
    }

    /// The reservation and the terminal record for an observed run pass the
    /// journal's record contract, not just the supervisor's own logic.
    #[tokio::test]
    async fn settled_observed_run_ends_through_the_journal() {
        let settled = with_root(observed_run(1), "completed");
        let segment = MemorySegment::new(0);
        let mut cursor = None;
        poll_once(
            0,
            "node-1",
            &TestObligationStore {
                run_data: settled.encode_to_vec(),
            },
            &segment,
            TerminalSupervisorConfig::default(),
            &mut cursor,
        )
        .await
        .unwrap();
        let reservation = segment
            .read_range(Offset::ZERO, segment.tail_offset().await.unwrap())
            .await
            .unwrap();
        assert_eq!(reservation.len(), 1, "the reservation was refused");
        assert_eq!(reservation[0].event_type, "run.terminalizing");

        let mut terminalizing = settled.clone();
        terminalizing.status = "terminalizing".to_string();
        terminalizing.metadata = reservation[0].metadata.clone();
        let mut cursor = None;
        poll_once(
            0,
            "node-1",
            &TestObligationStore {
                run_data: terminalizing.encode_to_vec(),
            },
            &segment,
            TerminalSupervisorConfig::default(),
            &mut cursor,
        )
        .await
        .unwrap();
        let records = segment
            .read_range(Offset::ZERO, segment.tail_offset().await.unwrap())
            .await
            .unwrap();
        assert_eq!(records.len(), 2, "the terminal record was refused");
        assert_eq!(records[1].event_type, "run.completed");
    }
}
