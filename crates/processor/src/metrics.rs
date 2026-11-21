//! Processor-level metrics (lazy-initialized via OnceLock).
//!
//! Tracks S3 flush, processor lag, and record throughput.

use std::sync::OnceLock;

use engine_core::types::Record;
use engine_proto::Run;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::KeyValue;
use prost::Message;

/// Time a synchronous projection function and record its latency under
/// `agnt5.runtime.processor.projection.duration_ms{projection}`. Returns
/// the projection's result unchanged. Cardinality is bounded — the
/// `projection` label takes one of a closed set of `&'static str` values.
pub fn time_projection<T>(projection: &'static str, f: impl FnOnce() -> T) -> T {
    let start = std::time::Instant::now();
    let result = f();
    processor_metrics().projection_duration_ms.record(
        start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("projection", projection)],
    );
    result
}

pub fn record_processor_stage_duration(
    event_type: &str,
    stage: &'static str,
    started_at: std::time::Instant,
) {
    processor_metrics().record_stage_duration_ms.record(
        started_at.elapsed().as_secs_f64() * 1000.0,
        &[
            KeyValue::new("event_type", event_type_label(event_type)),
            KeyValue::new("stage", stage),
        ],
    );
}

pub fn record_processor_op_counts(event_type: &str, store_ops: u64, backend_ops: Option<u64>) {
    let labels = [KeyValue::new("event_type", event_type_label(event_type))];
    let m = processor_metrics();
    m.record_store_ops.record(store_ops, &labels);
    if let Some(backend_ops) = backend_ops {
        m.record_kv_ops.record(backend_ops, &labels);
    }
}

pub fn record_queue_to_dispatch(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
) {
    let Some(duration_ms) = elapsed_since_timestamp_ms(record.timestamp_ns) else {
        return;
    };

    let mut labels = vec![
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("result", result),
        KeyValue::new(
            "component_type",
            record
                .metadata
                .get("component_type")
                .map(|s| engine_proto::labels::component_type_label_from_str(s))
                .unwrap_or("unknown"),
        ),
    ];
    if let Some(deployment_id) = record
        .metadata
        .get("deployment_id")
        .filter(|s| !s.is_empty())
    {
        labels.push(KeyValue::new("agnt5.deployment.id", deployment_id.clone()));
    }

    processor_metrics()
        .orchestrator_queue_to_dispatch_ms
        .record(duration_ms, &labels);
}

#[derive(Debug, Default, PartialEq)]
struct PullHotPathDurations {
    queue_to_projection_ms: Option<f64>,
    queue_to_assignment_ms: Option<f64>,
    assignment_to_start_ms: Option<f64>,
    queue_to_start_ms: Option<f64>,
    terminalization_ms: Option<f64>,
}

fn pull_hotpath_durations(record: &Record, run: &Run, observed_at_ms: f64) -> PullHotPathDurations {
    if run.metadata.get("dispatch_mode").map(String::as_str) != Some("pull") {
        return PullHotPathDurations::default();
    }
    let event_at_ms = (record.timestamp_ns > 0).then_some(record.timestamp_ns as f64 / 1_000_000.0);
    let submitted_at_ms = run.submitted_at.as_ref().map(|submitted| {
        submitted.seconds as f64 * 1_000.0 + f64::from(submitted.nanos) / 1_000_000.0
    });
    let assigned_at_ms = run.assigned_at.as_ref().map(|assigned| {
        assigned.seconds as f64 * 1_000.0 + f64::from(assigned.nanos) / 1_000_000.0
    });
    let queue_to_projection_ms = (record.event_type == "run.queued")
        .then(|| Some((observed_at_ms - event_at_ms?).max(0.0)))
        .flatten();
    let queue_to_assignment_ms = (record.event_type == "run.assigned")
        .then(|| Some((event_at_ms? - submitted_at_ms?).max(0.0)))
        .flatten();
    let assignment_to_start_ms = (record.event_type == "run.started")
        .then(|| Some((event_at_ms? - assigned_at_ms?).max(0.0)))
        .flatten();
    let queue_to_start_ms = (record.event_type == "run.started")
        .then(|| Some((event_at_ms? - submitted_at_ms?).max(0.0)))
        .flatten();
    let terminalization_ms = matches!(
        record.event_type.as_str(),
        "run.completed" | "run.failed" | "run.cancelled"
    )
    .then(|| {
        let received_at_ms = record
            .metadata
            .get(engine_core::lease_fence::META_FENCE_RECEIVED_AT_MS)?
            .parse::<f64>()
            .ok()?;
        Some((observed_at_ms - received_at_ms).max(0.0))
    })
    .flatten();
    PullHotPathDurations {
        queue_to_projection_ms,
        queue_to_assignment_ms,
        assignment_to_start_ms,
        queue_to_start_ms,
        terminalization_ms,
    }
}

pub fn record_pull_hotpath_durations(record: &Record, run: &Run, partition_id: u32) {
    let observed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64() * 1_000.0)
        .unwrap_or_default();
    let durations = pull_hotpath_durations(record, run, observed_at_ms);
    let mut labels = vec![
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new(
            "component_type",
            run_component_type_label(Some(record), Some(run)),
        ),
    ];
    if !run.deployment_id.is_empty() {
        labels.push(KeyValue::new(
            "agnt5.deployment.id",
            run.deployment_id.clone(),
        ));
    }
    let metrics = processor_metrics();
    if let Some(duration_ms) = durations.queue_to_projection_ms {
        metrics
            .pull_queue_to_projection_ms
            .record(duration_ms, &labels);
    }
    if let Some(duration_ms) = durations.queue_to_assignment_ms {
        metrics
            .pull_queue_to_assignment_ms
            .record(duration_ms, &labels);
    }
    if let Some(duration_ms) = durations.assignment_to_start_ms {
        metrics
            .pull_assignment_to_start_ms
            .record(duration_ms, &labels);
    }
    if let Some(duration_ms) = durations.queue_to_start_ms {
        metrics.pull_queue_to_start_ms.record(duration_ms, &labels);
    }
    if let Some(duration_ms) = durations.terminalization_ms {
        metrics.pull_terminalization_ms.record(duration_ms, &labels);
    }
}

pub fn record_dispatch_channel_send_wait(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    started_at: std::time::Instant,
) {
    let labels = dispatch_wait_labels(record, partition_id, node_id, result);
    processor_metrics()
        .dispatch_channel_send_wait_ms
        .record(started_at.elapsed().as_secs_f64() * 1000.0, &labels);
}

pub fn record_dispatch_ack_wait(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    started_at: std::time::Instant,
) {
    let labels = dispatch_wait_labels(record, partition_id, node_id, result);
    processor_metrics()
        .dispatch_ack_wait_ms
        .record(started_at.elapsed().as_secs_f64() * 1000.0, &labels);
}

pub fn record_dispatch_assigned_append_wait(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    started_at: std::time::Instant,
) {
    let labels = dispatch_wait_labels(record, partition_id, node_id, result);
    processor_metrics()
        .dispatch_assigned_append_ms
        .record(started_at.elapsed().as_secs_f64() * 1000.0, &labels);
}

pub fn record_dispatch_outbox_state(
    partition_id: u32,
    node_id: &str,
    depth: u64,
    oldest_age_ms: u64,
    in_flight: u64,
    deferred: u64,
    max_in_flight: u64,
) {
    let attrs = [
        opentelemetry::KeyValue::new("partition_id", partition_id as i64),
        opentelemetry::KeyValue::new("node_id", node_id.to_string()),
    ];
    let metrics = processor_metrics();
    metrics.dispatch_outbox_depth.record(depth, &attrs);
    metrics
        .dispatch_outbox_oldest_entry_age_ms
        .record(oldest_age_ms, &attrs);
    record_dispatch_outbox_runtime_state(partition_id, node_id, in_flight, deferred, max_in_flight);
}

pub fn record_dispatch_outbox_runtime_state(
    partition_id: u32,
    node_id: &str,
    in_flight: u64,
    deferred: u64,
    max_in_flight: u64,
) {
    let attrs = [
        opentelemetry::KeyValue::new("partition_id", partition_id as i64),
        opentelemetry::KeyValue::new("node_id", node_id.to_string()),
    ];
    let metrics = processor_metrics();
    metrics.dispatch_outbox_in_flight.record(in_flight, &attrs);
    metrics.dispatch_outbox_deferred.record(deferred, &attrs);
    metrics
        .dispatch_outbox_max_in_flight
        .record(max_in_flight, &attrs);
}

pub fn record_dispatch_outbox_cycle(partition_id: u32, node_id: &str, result: &'static str) {
    processor_metrics().dispatch_outbox_drain_cycles.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("result", result),
        ],
    );
}

pub fn record_dispatch_outbox_tick_gap(partition_id: u32, node_id: &str, gap: std::time::Duration) {
    processor_metrics().dispatch_outbox_tick_gap_ms.record(
        gap.as_secs_f64() * 1000.0,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
        ],
    );
}

pub fn record_dispatch_outbox_list_duration(
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    started_at: std::time::Instant,
) {
    processor_metrics()
        .dispatch_outbox_list_dispatch_outbox_duration_ms
        .record(
            started_at.elapsed().as_secs_f64() * 1000.0,
            &[
                KeyValue::new("partition_id", partition_id as i64),
                KeyValue::new("node_id", node_id.to_string()),
                KeyValue::new("result", result),
            ],
        );
}

pub fn record_dispatch_outbox_scan_rows(
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    rows: u64,
) {
    processor_metrics().dispatch_outbox_scan_rows.record(
        rows,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("result", result),
        ],
    );
}

pub fn record_dispatch_outbox_row_age_at_spawn(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    age_ms: u64,
) {
    let labels = dispatch_wait_labels(record, partition_id, node_id, result);
    processor_metrics()
        .dispatch_outbox_row_age_at_spawn_ms
        .record(age_ms as f64, &labels);
}

pub fn record_dispatch_outbox_insert_to_spawn(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
    inserted_at_ms: i64,
) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0);
    let duration_ms = now_ms.saturating_sub(inserted_at_ms).max(0) as f64;
    let labels = dispatch_wait_labels(record, partition_id, node_id, result);
    processor_metrics()
        .dispatch_outbox_insert_to_spawn_ms
        .record(duration_ms, &labels);
}

pub fn record_dispatch_outbox_guardrail(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    reason: &'static str,
) {
    let labels = dispatch_wait_labels(record, partition_id, node_id, reason);
    processor_metrics()
        .dispatch_outbox_guardrail_events
        .add(1, &labels);
}

pub fn record_processor_replay_start(
    partition_id: u32,
    node_id: &str,
    start_offset: Option<u64>,
    target_offset: Option<u64>,
) {
    let attrs = [
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("node_id", node_id.to_string()),
    ];
    let metrics = processor_metrics();
    if let Some(start) = start_offset {
        metrics.processor_replay_start_offset.record(start, &attrs);
    }
    if let Some(target) = target_offset {
        metrics
            .processor_replay_target_offset
            .record(target, &attrs);
        let remaining = start_offset
            .map(|start| target.saturating_sub(start))
            .unwrap_or(target);
        metrics
            .processor_replay_remaining_records
            .record(remaining, &attrs);
    }
    metrics.processor_replay_readiness_events.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("state", "tracking_started"),
        ],
    );
}

pub fn record_processor_replay_ready(
    partition_id: u32,
    node_id: &str,
    state: &'static str,
    duration_ms: f64,
) {
    let attrs = [
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("node_id", node_id.to_string()),
    ];
    let metrics = processor_metrics();
    metrics.processor_replay_remaining_records.record(0, &attrs);
    metrics.processor_replay_readiness_events.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("state", state),
        ],
    );
    metrics.replay_duration_ms.record(duration_ms, &attrs);
}

pub fn record_processor_supervisor_state(
    partition_id: u32,
    node_id: &str,
    live: bool,
    consecutive_failures: u32,
) {
    let attrs = [
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("node_id", node_id.to_string()),
    ];
    let metrics = processor_metrics();
    metrics.processor_liveness.record(u64::from(live), &attrs);
    metrics
        .processor_consecutive_failures
        .record(u64::from(consecutive_failures), &attrs);
}

pub fn record_processor_supervisor_event(partition_id: u32, node_id: &str, event: &'static str) {
    processor_metrics().processor_supervisor_events.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("event", event),
        ],
    );
}

/// Record a durable terminal obligation seen by the partition owner.
///
/// Labels intentionally use closed sets: run and project identifiers must never
/// become metric dimensions.
pub fn record_terminal_obligation_observed(partition_id: u32, status: &str, age_ms: i64) {
    let labels = [
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("status", terminal_obligation_status_label(status)),
    ];
    let metrics = processor_metrics();
    metrics.terminal_obligations_observed.add(1, &labels);
    metrics
        .terminal_obligation_age_ms
        .record(age_ms.max(0) as f64, &labels);
}

/// Record an action taken by the partition-owned terminal supervisor.
pub fn record_terminal_supervisor_action(partition_id: u32, outcome: &str, result: &str) {
    processor_metrics().terminal_supervisor_actions.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("outcome", terminal_supervisor_outcome_label(outcome)),
            KeyValue::new("result", terminal_supervisor_result_label(result)),
        ],
    );
}

/// The instance obligations on a partition, seen by one supervisor pass.
pub fn record_instance_obligations(partition_id: u32, count: usize) {
    processor_metrics().instance_obligations.record(
        count as u64,
        &[KeyValue::new("partition_id", partition_id as i64)],
    );
}

/// One decision the instance supervisor took. `kind` is start, resume or
/// hold; `result` is appended, deduplicated, waiting or error.
pub fn record_instance_decision(
    partition_id: u32,
    kind: &'static str,
    result: &'static str,
    delay_ms: Option<i64>,
) {
    let metrics = processor_metrics();
    metrics.instance_decisions.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("kind", kind),
            KeyValue::new("result", result),
        ],
    );
    if let Some(delay_ms) = delay_ms {
        metrics
            .instance_decision_delay_ms
            .record(delay_ms.max(0) as f64, &[KeyValue::new("kind", kind)]);
    }
}

/// A start or resume checked as it was applied.
pub fn record_instance_decision_applied(
    partition_id: u32,
    kind: &'static str,
    outcome: &'static str,
) {
    processor_metrics().instance_decisions_applied.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("kind", kind),
            KeyValue::new("outcome", outcome),
        ],
    );
}

/// A journal record the instance projection refused to fold.
pub fn record_instance_change_refused(partition_id: u32, change: &'static str) {
    processor_metrics().instance_changes_refused.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("change", change),
        ],
    );
}

/// Appends the instance guard refused since the last report.
pub fn record_instance_guard_refusals(count: u64) {
    if count > 0 {
        processor_metrics().instance_guard_refusals.add(count, &[]);
    }
}

/// Record a durable activation transition only after its projection commit
/// succeeds. The transition label is a closed set from the V1 contract.
pub fn record_activation_journal_event(record: &Record, partition_id: u32) {
    let Some(transition) = activation_transition_label(record) else {
        return;
    };
    processor_metrics().activation_journal_events.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("transition", transition),
        ],
    );
}

/// Counts a projected record that billing excludes, by why. Every replica
/// projects every record, so each replica counts each one: sum one replica,
/// or take the largest rate across replicas.
pub fn record_billing_unmetered_step(record: &Record) {
    if let Some(kind) = engine_core::billing::unmetered(record) {
        processor_metrics()
            .billing_unmetered_steps
            .add(1, &[KeyValue::new("kind", kind.label())]);
    }
}

/// Bucket boundaries (seconds) for billing counting lag: the fleet manager
/// counts every minute, the daily check alerts at 15 minutes and pages at an
/// hour, and a run's row lasts about a day after it is archived.
const BILLING_COUNT_LAG_SECONDS: &[f64] = &[
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0, 1800.0, 3600.0, 7200.0, 21600.0,
    86400.0,
];

/// Counts one request the internal billing listener served. `endpoint` and
/// `result` come from closed sets the listener names.
pub fn record_billing_request(endpoint: &'static str, result: &'static str) {
    processor_metrics().billing_requests.add(
        1,
        &[
            KeyValue::new("endpoint", endpoint),
            KeyValue::new("result", result),
        ],
    );
}

/// Records how far one billing count request found counting behind on a
/// partition. A histogram rather than a gauge: requests spread over
/// replicas, and a gauge would keep showing a replica's last answer after
/// requests moved elsewhere.
pub fn record_billing_count_lag(partition_id: u32, lag_seconds: f64) {
    processor_metrics().billing_count_lag_seconds.record(
        lag_seconds.max(0.0),
        &[KeyValue::new("partition_id", partition_id as i64)],
    );
}

pub fn record_timer_backlog(
    partition_id: u32,
    due: u64,
    oldest_overdue_age_ms: u64,
    batch_saturated: bool,
) {
    let metrics = processor_metrics();
    let labels = [KeyValue::new("partition_id", partition_id as i64)];
    metrics.timer_due.record(due, &labels);
    metrics
        .timer_oldest_overdue_age_ms
        .record(oldest_overdue_age_ms, &labels);
    metrics
        .timer_batch_saturated
        .record(u64::from(batch_saturated), &labels);
}

/// The transition label comes from the record envelope, gated on the
/// activation marker, so an SDK-emitted `workflow.step.completed` without a
/// marker never counts as a durable activation transition.
fn activation_transition_label(record: &Record) -> Option<&'static str> {
    let (_, _, transition) = engine_core::activation::activation_record_identity(record)?;
    engine_core::activation::ALL_TRANSITIONS
        .iter()
        .copied()
        .find(|label| *label == transition)
}

fn terminal_obligation_status_label(status: &str) -> &'static str {
    match status {
        "queued" => "queued",
        "assigned" => "assigned",
        "started" => "started",
        "retry_delayed" => "retry_delayed",
        "terminalizing" => "terminalizing",
        _ => "unknown",
    }
}

fn terminal_supervisor_outcome_label(outcome: &str) -> &'static str {
    match outcome {
        "retry_queued" => "retry_queued",
        "lease_requeue" => "lease_requeue",
        "lease_exhausted" => "lease_exhausted",
        "reserve_cancelled" => "reserve_cancelled",
        "reserve_timeout" => "reserve_timeout",
        "reserve_dispatch_timeout" => "reserve_dispatch_timeout",
        "reserve_attempts_exhausted" => "reserve_attempts_exhausted",
        "reserve_stranded" => "reserve_stranded",
        "cancelled" => "cancelled",
        "timeout" => "timeout",
        "attempts_exhausted" => "attempts_exhausted",
        "stranded" => "stranded",
        _ => "unknown",
    }
}

fn terminal_supervisor_result_label(result: &str) -> &'static str {
    match result {
        "appended" => "appended",
        "deduplicated" => "deduplicated",
        "append_error" => "append_error",
        _ => "unknown",
    }
}

pub fn record_terminal_tx_state(
    partition_id: u32,
    node_id: &str,
    available_capacity: usize,
    max_capacity: usize,
) {
    let queue_depth = max_capacity.saturating_sub(available_capacity) as u64;
    let max_capacity = max_capacity as u64;
    let labels = [
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("node_id", node_id.to_string()),
    ];
    let metrics = processor_metrics();
    metrics.terminal_tx_queue_depth.record(queue_depth, &labels);
    metrics.terminal_tx_capacity.record(max_capacity, &labels);
}

pub fn record_terminal_tx_event(
    partition_id: Option<u32>,
    node_id: Option<&str>,
    result: &'static str,
) {
    let mut labels = vec![KeyValue::new("result", result)];
    if let Some(partition_id) = partition_id {
        labels.push(KeyValue::new("partition_id", partition_id as i64));
    }
    if let Some(node_id) = node_id {
        labels.push(KeyValue::new("node_id", node_id.to_string()));
    }
    processor_metrics().terminal_tx_events.add(1, &labels);
}

pub fn record_pull_ready_notification(partition_id: u32, node_id: &str, result: &'static str) {
    processor_metrics().pull_ready_notifications.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("result", result),
        ],
    );
}

pub fn record_demand_dirty_notification(partition_id: u32, node_id: &str, result: &'static str) {
    processor_metrics().demand_dirty_notifications.add(
        1,
        &[
            KeyValue::new("partition_id", partition_id as i64),
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("result", result),
        ],
    );
}

pub fn record_terminal_tx_send_wait(
    partition_id: Option<u32>,
    node_id: Option<&str>,
    result: &'static str,
    started_at: std::time::Instant,
) {
    let mut labels = vec![KeyValue::new("result", result)];
    if let Some(partition_id) = partition_id {
        labels.push(KeyValue::new("partition_id", partition_id as i64));
    }
    if let Some(node_id) = node_id {
        labels.push(KeyValue::new("node_id", node_id.to_string()));
    }
    processor_metrics()
        .terminal_tx_send_wait_ms
        .record(started_at.elapsed().as_secs_f64() * 1000.0, &labels);
}

fn dispatch_wait_labels(
    record: &Record,
    partition_id: u32,
    node_id: &str,
    result: &'static str,
) -> Vec<KeyValue> {
    let mut labels = vec![
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("node_id", node_id.to_string()),
        KeyValue::new("result", result),
        KeyValue::new(
            "component_type",
            record
                .metadata
                .get("component_type")
                .map(|s| engine_proto::labels::component_type_label_from_str(s))
                .unwrap_or("unknown"),
        ),
    ];
    if let Some(deployment_id) = record
        .metadata
        .get("deployment_id")
        .filter(|s| !s.is_empty())
    {
        labels.push(KeyValue::new("agnt5.deployment.id", deployment_id.clone()));
    }
    labels
}

fn elapsed_since_timestamp_ms(timestamp_ns: i64) -> Option<f64> {
    if timestamp_ns <= 0 {
        return None;
    }

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos() as i128;
    let elapsed_ns = now_ns.saturating_sub(timestamp_ns as i128);
    if elapsed_ns < 0 {
        return None;
    }

    Some(elapsed_ns as f64 / 1_000_000.0)
}

fn event_type_label(event_type: &str) -> &'static str {
    match event_type {
        "agent.cancelled" => "agent.cancelled",
        "agent.snapshot" => "agent.snapshot",
        "agent.suspended" => "agent.suspended",
        "agent.unknown_outcome" => "agent.unknown_outcome",
        "lm.cancelled" => "lm.cancelled",
        "lm.failed" => "lm.failed",
        "lm.snapshot" => "lm.snapshot",
        "lm.started" => "lm.started",
        "lm.suspended" => "lm.suspended",
        "lm.unknown_outcome" => "lm.unknown_outcome",
        "tool_call.cancelled" => "tool_call.cancelled",
        "tool_call.completed" => "tool_call.completed",
        "tool_call.failed" => "tool_call.failed",
        "tool_call.snapshot" => "tool_call.snapshot",
        "tool_call.started" => "tool_call.started",
        "tool_call.suspended" => "tool_call.suspended",
        "tool_call.unknown_outcome" => "tool_call.unknown_outcome",
        "workflow.step.cancelled" => "workflow.step.cancelled",
        "workflow.step.failed" => "workflow.step.failed",
        "workflow.step.snapshot" => "workflow.step.snapshot",
        "workflow.step.started" => "workflow.step.started",
        "workflow.step.suspended" => "workflow.step.suspended",
        "workflow.step.unknown_outcome" => "workflow.step.unknown_outcome",
        "agent.completed" => "agent.completed",
        "agent.failed" => "agent.failed",
        "agent.iteration.completed" => "agent.iteration.completed",
        "agent.iteration.started" => "agent.iteration.started",
        "agent.started" => "agent.started",
        "approval.requested" => "approval.requested",
        "approval.resolved" => "approval.resolved",
        "batch.cancelled" => "batch.cancelled",
        "batch.completed" => "batch.completed",
        "batch.created" => "batch.created",
        "batch.item_completed" => "batch.item_completed",
        "batch.item_failed" => "batch.item_failed",
        "batch.started" => "batch.started",
        "checkpoint.created" => "checkpoint.created",
        "eval.completed" => "eval.completed",
        "eval.failed" => "eval.failed",
        "eval.started" => "eval.started",
        "function.completed" => "function.completed",
        "function.failed" => "function.failed",
        "function.started" => "function.started",
        "guardrail.decision" => "guardrail.decision",
        "job.cancelled" => "job.cancelled",
        "job.claimed" => "job.claimed",
        "job.completed" => "job.completed",
        "job.enqueued" => "job.enqueued",
        "job.failed" => "job.failed",
        "job.started" => "job.started",
        "lm.call.completed" => "lm.call.completed",
        "lm.call.failed" => "lm.call.failed",
        "lm.call.started" => "lm.call.started",
        "lm.completed" => "lm.completed",
        "lm.message.delta" => "lm.message.delta",
        "lm.stream.completed" => "lm.stream.completed",
        "lm.stream.delta" => "lm.stream.delta",
        "message.acked" => "message.acked",
        "message.enqueued" => "message.enqueued",
        "run.archived" => "run.archived",
        "run.assigned" => "run.assigned",
        "run.cancelled" => "run.cancelled",
        "run.completed" => "run.completed",
        "run.failed" => "run.failed",
        "run.lease_expired" => "run.lease_expired",
        "run.paused" => "run.paused",
        "run.queued" => "run.queued",
        "run.resumed" => "run.resumed",
        "run.started" => "run.started",
        "score.recorded" => "score.recorded",
        "scorer.completed" => "scorer.completed",
        "scorer.failed" => "scorer.failed",
        "scorer.started" => "scorer.started",
        "session.created" => "session.created",
        "session.updated" => "session.updated",
        "signal.acked" => "signal.acked",
        "signal.received" => "signal.received",
        "timer.canceled" => "timer.canceled",
        "timer.expired" => "timer.expired",
        "timer.fired" => "timer.fired",
        "timer.scheduled" => "timer.scheduled",
        "tool.completed" => "tool.completed",
        "tool.call.completed" => "tool.call.completed",
        "tool.call.failed" => "tool.call.failed",
        "tool.call.started" => "tool.call.started",
        "workflow.paused" => "workflow.paused",
        "workflow.state.changed" => "workflow.state.changed",
        "workflow.step.completed" => "workflow.step.completed",
        _ => "other",
    }
}

/// Label set for `processor.retries` and `processor.dead_letters`.
///
/// Both metrics are emitted at the orchestrator's failure-handling path,
/// so they share the same dimensions: `component_type` (the kind of
/// component that failed) and `failure_kind` (a coarse classification of
/// why). Both default to `"unknown"` when neither the record's metadata
/// nor the originating Run carries them.
///
/// Worker-emitted `run.failed` records typically don't carry
/// `component_type` themselves — that label is stamped on the original
/// `run.queued` and projected into the Run state. We prefer the Run's
/// value so dead-letter rows surface as `function` / `agent` / … instead
/// of `unknown`.
pub fn failure_labels(record: &Record, existing_run_bytes: Option<&[u8]>) -> [KeyValue; 2] {
    // Normalize whatever the gateway/SDK stamped into metadata
    // (`functions`, `agents`, …) into the canonical short-form label
    // (`function`, `agent`, …) so it joins correctly with coordinator
    // metrics that use `engine_proto::labels::component_type_label`.
    let run_component_type = existing_run_bytes
        .and_then(|bytes| engine_proto::Run::decode(bytes).ok())
        .map(|run| run.component_type)
        .filter(|s| !s.is_empty());
    let component_type = record
        .metadata
        .get("component_type")
        .cloned()
        .filter(|s| !s.is_empty())
        .or(run_component_type)
        .as_deref()
        .map(engine_proto::labels::component_type_label_from_str)
        .unwrap_or("unknown");
    let failure_kind = record
        .metadata
        .get("error_code")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    [
        KeyValue::new("component_type", component_type),
        KeyValue::new("failure_kind", failure_kind),
    ]
}

/// Record a run lifecycle event after the processor has successfully applied
/// the journal record to the materialized store. This is the durable source of
/// truth for "did the run reach the end?", unlike gateway HTTP metrics which
/// can be affected by client disconnects and synchronous wait behavior.
pub fn record_run_lifecycle_event(
    record: &Record,
    run: Option<&engine_proto::Run>,
    partition_id: u32,
) {
    let Some(stage) = run_lifecycle_stage(record.event_type.as_str()) else {
        return;
    };

    let mut labels = vec![
        KeyValue::new("stage", stage),
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new(
            "component_type",
            run_component_type_label(Some(record), run),
        ),
    ];
    if let Some(deployment_id) = run_deployment_label(Some(record), run) {
        labels.push(KeyValue::new("agnt5.deployment.id", deployment_id));
    }

    processor_metrics().run_lifecycle_events.add(1, &labels);
}

pub fn record_run_terminal_duration(
    run: &engine_proto::Run,
    terminal_stage: &str,
    partition_id: u32,
) {
    let (Some(submitted_at), Some(completed_at)) =
        (run.submitted_at.as_ref(), run.completed_at.as_ref())
    else {
        return;
    };

    let duration_ms = timestamp_ms(completed_at) - timestamp_ms(submitted_at);
    if duration_ms < 0 {
        return;
    }

    let mut labels = vec![
        KeyValue::new("stage", terminal_stage.to_string()),
        KeyValue::new("partition_id", partition_id as i64),
        KeyValue::new("component_type", run_component_type_label(None, Some(run))),
    ];
    if let Some(deployment_id) = run_deployment_label(None, Some(run)) {
        labels.push(KeyValue::new("agnt5.deployment.id", deployment_id));
    }

    processor_metrics()
        .run_duration_ms
        .record(duration_ms as f64, &labels);
}

fn timestamp_ms(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds.saturating_mul(1000) + i64::from(ts.nanos / 1_000_000)
}

fn run_deployment_label(
    record: Option<&Record>,
    run: Option<&engine_proto::Run>,
) -> Option<String> {
    run.and_then(|r| non_empty(&r.deployment_id)).or_else(|| {
        record.and_then(|r| {
            r.metadata
                .get("deployment_id")
                .and_then(|value| non_empty(value))
        })
    })
}

fn run_component_type_label(
    record: Option<&Record>,
    run: Option<&engine_proto::Run>,
) -> &'static str {
    let value = run.and_then(|r| non_empty(&r.component_type)).or_else(|| {
        record.and_then(|r| {
            r.metadata
                .get("component_type")
                .and_then(|value| non_empty(value))
        })
    });
    value
        .as_deref()
        .map(engine_proto::labels::component_type_label_from_str)
        .unwrap_or("unknown")
}

fn non_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn run_lifecycle_stage(event_type: &str) -> Option<&'static str> {
    match event_type {
        "run.queued" => Some("queued"),
        "run.assigned" => Some("assigned"),
        "run.started" => Some("started"),
        "run.completed" => Some("completed"),
        "run.failed" => Some("failed"),
        "run.cancelled" => Some("cancelled"),
        "run.paused" | "workflow.paused" => Some("paused"),
        "run.resumed" => Some("resumed"),
        "run.lease_expired" => Some("lease_expired"),
        _ => None,
    }
}

/// Access the processor metrics singleton.
pub fn processor_metrics() -> &'static ProcessorMetrics {
    static METRICS: OnceLock<ProcessorMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = opentelemetry::global::meter("agnt5.runtime");
        ProcessorMetrics {
            s3_flush_duration: meter
                .f64_histogram("agnt5.runtime.s3.flush.duration_ms")
                .with_description("S3 Parquet flush duration in milliseconds")
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            s3_put_errors: meter
                .u64_counter("agnt5.runtime.s3.put.errors")
                .with_description("S3 PUT failures during Parquet flush; labeled by `error_code`")
                .build(),
            s3_put_retries: meter
                .u64_counter("agnt5.runtime.s3.put.retries")
                .with_description("S3 PUT retry attempts (excludes the first attempt)")
                .build(),
            s3_circuit_open: meter
                .u64_counter("agnt5.runtime.s3.put.circuit_open")
                .with_description(
                    "Times the S3 writer's circuit breaker tripped open after \
                     consecutive failures",
                )
                .build(),
            s3_put_bytes: meter
                .u64_counter("agnt5.runtime.s3.put.bytes")
                .with_description(
                    "Bytes pushed to S3 across every PUT path. Labeled by \
                     `path ∈ {outbox_flush, segment_compaction, snapshot, compaction_pass}` \
                     so dashboards can break down throughput by source.",
                )
                .with_unit("By")
                .build(),
            s3_put_duration_ms: meter
                .f64_histogram("agnt5.runtime.s3.put.duration_ms")
                .with_description(
                    "Per-PUT object_store latency at the network layer. Distinct \
                     from `s3.flush.duration_ms` which also includes Parquet \
                     construction time. Same `path` label as `s3.put.bytes`.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            s3_flush_batch_size: meter
                .u64_histogram("agnt5.runtime.s3.flush.batch_size")
                .with_description(
                    "Records-per-Parquet distribution from the outbox drainer. \
                     Tells operators whether files are appropriately sized — \
                     small batches mean compaction has more work to do.",
                )
                .build(),
            processor_lag: meter
                .u64_gauge("agnt5.runtime.processor.lag")
                .with_description(
                    "Records in the partition's segment the processor has not applied yet, \
                     sampled every 10 seconds",
                )
                .build(),
            processor_lag_seconds: meter
                .u64_gauge("agnt5.runtime.processor.lag.seconds")
                .with_description(
                    "Wall-clock seconds the processor is behind the segment tail, sampled \
                     every 10 seconds: zero when caught up, otherwise the age of the newest \
                     applied record. Easier to alert on than the records-behind variant.",
                )
                .build(),
            projection_duration_ms: meter
                .f64_histogram("agnt5.runtime.processor.projection.duration_ms")
                .with_description(
                    "Per-projection apply latency in ms. Each pure projection \
                     function (runs, workers, state, approvals, signals, timers, \
                     jobs, batches, sessions, messages) records once per record \
                     under its `projection` label.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            record_stage_duration_ms: meter
                .f64_histogram("agnt5.runtime.processor.record.stage.duration_ms")
                .with_description(
                    "Per-record processor stage latency, labeled by bounded event_type \
                     and stage: read_existing, project, encode, write_batch.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            record_store_ops: meter
                .u64_histogram("agnt5.runtime.processor.record.store_ops")
                .with_description(
                    "High-level projection StoreOp count per processed record, labeled by bounded event_type.",
                )
                .build(),
            record_kv_ops: meter
                .u64_histogram("agnt5.runtime.processor.record.kv_ops")
                .with_description(
                    "Encoded low-level KV op count per processed record, labeled by bounded event_type.",
                )
                .build(),
            replay_duration_ms: meter
                .f64_histogram("agnt5.runtime.processor.replay.duration_ms")
                .with_description(
                    "One-shot histogram per partition at startup completion: time \
                     from processor start to catching up with the segment tail. \
                     Pairs with `partition_id` label so each restart records once.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            processor_replay_start_offset: meter
                .u64_gauge("agnt5.runtime.processor.replay.start_offset")
                .with_description("Local partition offset where startup replay began.")
                .build(),
            processor_replay_target_offset: meter
                .u64_gauge("agnt5.runtime.processor.replay.target_offset")
                .with_description(
                    "Local partition tail offset captured at processor startup.",
                )
                .build(),
            processor_replay_remaining_records: meter
                .u64_gauge("agnt5.runtime.processor.replay.remaining_records")
                .with_description(
                    "Records remaining between startup replay start and captured target tail.",
                )
                .build(),
            processor_replay_readiness_events: meter
                .u64_counter("agnt5.runtime.processor.replay.readiness_events")
                .with_description(
                    "Startup replay readiness transitions by state: tracking_started, \
                     initial_ready, target_reached, or tracking_disabled.",
                )
                .build(),
            processor_liveness: meter
                .u64_gauge("agnt5.runtime.processor.liveness")
                .with_description(
                    "1 while the supervised partition processor attempt is running; 0 otherwise.",
                )
                .build(),
            processor_consecutive_failures: meter
                .u64_gauge("agnt5.runtime.processor.consecutive_failures")
                .with_description(
                    "Consecutive partition processor failures since the last stable completed-replay attempt.",
                )
                .build(),
            processor_supervisor_events: meter
                .u64_counter("agnt5.runtime.processor.supervisor.events")
                .with_description(
                    "Partition supervisor lifecycle events such as restart scheduling, panic, and retry exhaustion.",
                )
                .build(),
            terminal_obligations_observed: meter
                .u64_counter("agnt5.runtime.terminal_obligations.observed")
                .with_description(
                    "Durable non-terminal run obligations observed by the active partition owner.",
                )
                .build(),
            terminal_obligation_age_ms: meter
                .f64_histogram("agnt5.runtime.terminal_obligations.age_ms")
                .with_description(
                    "Age of durable non-terminal run obligations observed by the active partition owner.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            terminal_supervisor_actions: meter
                .u64_counter("agnt5.runtime.terminal_supervisor.actions")
                .with_description(
                    "Recovery actions appended by the partition-owned terminal supervisor.",
                )
                .build(),
            instance_obligations: meter
                .u64_gauge("agnt5.runtime.instance.obligations")
                .with_description(
                    "Agent instances on the partition waiting for their leader to decide a start, \
                     resume or hold, as of the instance supervisor's last pass.",
                )
                .build(),
            instance_decisions: meter
                .u64_counter("agnt5.runtime.instance.decisions")
                .with_description(
                    "Decisions the instance supervisor appended (start, resume, hold), by result.",
                )
                .build(),
            instance_decision_delay_ms: meter
                .f64_histogram("agnt5.runtime.instance.decision_delay_ms")
                .with_description(
                    "From the record that made an agent instance need a decision to the \
                     supervisor's append of it: the start delay of an instance's next run.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            instance_decisions_applied: meter
                .u64_counter("agnt5.runtime.instance.decisions_applied")
                .with_description(
                    "Instance starts and resumes checked as they were applied, by outcome: \
                     accepted, rejected, stale or invalid.",
                )
                .build(),
            instance_changes_refused: meter
                .u64_counter("agnt5.runtime.instance.changes_refused")
                .with_description(
                    "Journal records the instance projection refused to fold, such as an item for \
                     a full inbox. The record stays in the journal and changes nothing.",
                )
                .build(),
            instance_guard_refusals: meter
                .u64_counter("agnt5.runtime.instance.guard_refusals")
                .with_description(
                    "Appends refused because they would start, resume or hold an instance's run \
                     without its leader's decision.",
                )
                .build(),
            activation_journal_events: meter
                .u64_counter("agnt5.runtime.activation.journal_events")
                .with_description(
                    "Durable activation V1 transitions after their projection commit succeeds.",
                )
                .build(),
            // Named for billing rather than the runtime, as the billing spec
            // lists it; Prometheus sees `agnt5_billing_unmetered_steps_total`.
            billing_unmetered_steps: meter
                .u64_counter("agnt5.billing.unmetered_steps")
                .with_description(
                    "Projected records that look like billable work but that billing \
                     excludes, by `kind`: observed, unmarked, wait, signal, approval.",
                )
                .build(),
            billing_requests: meter
                .u64_counter("agnt5.runtime.billing.requests")
                .with_description(
                    "Requests served by the internal billing listener, by `endpoint` and `result`.",
                )
                .build(),
            billing_count_lag_seconds: meter
                .f64_histogram("agnt5.runtime.billing.count_lag.seconds")
                .with_description(
                    "Per billing count request: how long ago the runtime stamped the oldest \
                     record the fleet manager had not yet counted; 0 when the page left it \
                     caught up.",
                )
                .with_unit("s")
                .with_boundaries(BILLING_COUNT_LAG_SECONDS.to_vec())
                .build(),
            timer_due: meter
                .u64_gauge("agnt5.runtime.timer.due")
                .with_description("Due timer rows returned by the active timer poller scan.")
                .build(),
            timer_oldest_overdue_age_ms: meter
                .u64_gauge("agnt5.runtime.timer.oldest_overdue_age_ms")
                .with_description("Age of the oldest decodable scheduled timer returned by the due scan.")
                .with_unit("ms")
                .build(),
            timer_batch_saturated: meter
                .u64_gauge("agnt5.runtime.timer.batch_saturated")
                .with_description("1 when a due timer scan reaches its configured batch size; 0 otherwise.")
                .build(),
            processor_records: meter
                .u64_counter("agnt5.runtime.processor.records")
                .with_description("Records processed by the materialization processor")
                .build(),
            run_lifecycle_events: meter
                .u64_counter("agnt5.runtime.runs.lifecycle.events")
                .with_description(
                    "Durable run lifecycle events after the processor applies the journal record",
                )
                .build(),
            run_duration_ms: meter
                .f64_histogram("agnt5.runtime.runs.duration_ms")
                .with_description(
                    "Durable run duration from submitted_at to completed_at after terminal projection",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            pull_queue_to_start_ms: meter
                .f64_histogram("agnt5.runtime.pull.queue_to_start.duration_ms")
                .with_description(
                    "Durable PULL latency from run submission to accepted run.started.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            pull_queue_to_projection_ms: meter
                .f64_histogram("agnt5.runtime.pull.queue_to_projection.duration_ms")
                .with_description(
                    "PULL latency from the run.queued timestamp to committed pending projection.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            pull_queue_to_assignment_ms: meter
                .f64_histogram("agnt5.runtime.pull.queue_to_assignment.duration_ms")
                .with_description(
                    "PULL latency from the run.queued timestamp to construction of run.assigned.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            pull_assignment_to_start_ms: meter
                .f64_histogram("agnt5.runtime.pull.assignment_to_start.duration_ms")
                .with_description(
                    "PULL latency from the run.assigned timestamp to accepted run.started.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            pull_terminalization_ms: meter
                .f64_histogram("agnt5.runtime.pull.terminalization.duration_ms")
                .with_description(
                    "Durable PULL terminalization latency from service receipt to canonical journal record.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            illegal_transitions: meter
                .u64_counter("agnt5.runtime.processor.illegal_transitions")
                .with_description("Run lifecycle transitions that violate the state machine")
                .build(),
            processor_dispatches: meter
                .u64_counter("agnt5.runtime.processor.dispatches")
                .with_description("Runs dispatched by the processor orchestrator")
                .build(),
            orchestrator_queue_to_dispatch_ms: meter
                .f64_histogram("agnt5.runtime.processor.orchestrator.queue_to_dispatch_ms")
                .with_description(
                    "Time from the original run.queued journal timestamp to \
                     processor-orchestrator dispatch acceptance or failure.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            dispatch_channel_send_wait_ms: meter
                .f64_histogram("agnt5.runtime.processor.dispatch.channel_send_wait_ms")
                .with_description(
                    "Time the partition processor waits to enqueue a dispatch envelope \
                     onto the bounded dispatch channel.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            dispatch_ack_wait_ms: meter
                .f64_histogram("agnt5.runtime.processor.dispatch.ack_wait_ms")
                .with_description(
                    "Time the partition processor waits for the engine dispatch ACK \
                     after the dispatch envelope has been accepted by the channel.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            dispatch_assigned_append_ms: meter
                .f64_histogram("agnt5.runtime.processor.dispatch.assigned_append_ms")
                .with_description(
                    "Time spent appending the follow-up run.assigned record after \
                     dispatch acceptance.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            processor_retries: meter
                .u64_counter("agnt5.runtime.processor.retries")
                .with_description(
                    "Retry events written by the processor's orchestrator. Fires when \
                     a `run.failed` is observed AND retry policy still has budget — \
                     i.e. the next attempt was actually scheduled, not just policy-evaluated.",
                )
                .build(),
            dead_letters: meter
                .u64_counter("agnt5.runtime.processor.dead_letters")
                .with_description(
                    "Failures that exhausted their retry budget (or had no retry policy) \
                     and were not re-queued. Pairs with `processor.retries` to track the \
                     full retry/dead-letter outcome distribution.",
                )
                .build(),
            outbox_depth: meter
                .u64_gauge("agnt5.runtime.outbox.depth")
                .with_description(
                    "Number of pending entries in CF_S3_OUTBOX for a partition. \
                     High values indicate the active owner's drain is behind \
                     (S3 degraded, drainer stuck, or burst of terminal events).",
                )
                .build(),
            outbox_oldest_entry_age_ms: meter
                .u64_gauge("agnt5.runtime.outbox.oldest_entry_age_ms")
                .with_description(
                    "Wall-clock age of the oldest pending outbox entry in a \
                     partition (now − Run.completed_at_ms). Replaces the \
                     originally-proposed `flush_watermark_lag_ms` per Decision \
                     22 — same operational signal, no contiguous-prefix \
                     bookkeeping. Zero when the outbox slice is empty.",
                )
                .build(),
            query_archive_shadow_depth: meter
                .u64_gauge("agnt5.runtime.query_archive_shadow.outbox_depth")
                .with_description(
                    "Bounded count of pending Storage V2 shadow intents observed for a partition.",
                )
                .build(),
            query_archive_shadow_rows: meter
                .u64_counter("agnt5.runtime.query_archive_shadow.rows")
                .with_description(
                    "Storage V2 shadow rows by bounded result: committed, acknowledged, \
                     publish_failed, acknowledgement_failed, or quarantined.",
                )
                .build(),
            query_archive_shadow_drain_cycles: meter
                .u64_counter("agnt5.runtime.query_archive_shadow.drain_cycles")
                .with_description(
                    "Leader-owned Storage V2 shadow drain passes by bounded result: empty, deferred, \
                     acknowledged, partial_failure, or scan_error.",
                )
                .build(),
            query_archive_shadow_publish_duration_ms: meter
                .f64_histogram("agnt5.runtime.query_archive_shadow.publish_duration_ms")
                .with_description(
                    "Blob preparation and committed Vortex segment publication latency by result.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            query_archive_compaction_passes: meter
                .u64_counter("agnt5.runtime.query_archive.compaction.passes")
                .with_description(
                    "Leader-owned Storage V2 compaction passes by bounded result.",
                )
                .build(),
            query_archive_compaction_projects: meter
                .u64_counter("agnt5.runtime.query_archive.compaction.projects")
                .with_description(
                    "Storage V2 project compaction attempts by compacted or failed result.",
                )
                .build(),
            query_archive_compaction_duration_ms: meter
                .f64_histogram("agnt5.runtime.query_archive.compaction.duration_ms")
                .with_description("Storage V2 catalog rebuild and compaction pass latency.")
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            query_archive_gc_runs: meter
                .u64_counter("agnt5.runtime.query_archive.gc.runs")
                .with_description(
                    "Obsolete-parent GC attempts at the end of partition 0's compaction passes, by result.",
                )
                .build(),
            query_archive_gc_retired_segments: meter
                .u64_counter("agnt5.runtime.query_archive.gc.retired_segments")
                .with_description("Obsolete query-archive segments retired by GC, across all partitions.")
                .build(),
            query_archive_gc_due_segments: meter
                .u64_gauge("agnt5.runtime.query_archive.gc.due_segments")
                .with_description(
                    "Obsolete parents past their grace period and waiting for GC, across all partitions, as of partition 0's last pass.",
                )
                .build(),
            query_archive_gc_lag_ms: meter
                .u64_gauge("agnt5.runtime.query_archive.gc.lag_ms")
                .with_description(
                    "How long the oldest due obsolete parent has waited past its grace period, as of partition 0's last pass.",
                )
                .with_unit("ms")
                .build(),
            dispatch_outbox_depth: meter
                .u64_gauge("agnt5.runtime.dispatch_outbox.depth")
                .with_description(
                    "Number of durable dispatch outbox entries pending drain for a partition.",
                )
                .build(),
            dispatch_outbox_oldest_entry_age_ms: meter
                .u64_gauge("agnt5.runtime.dispatch_outbox.oldest_entry_age_ms")
                .with_description(
                    "Wall-clock age of the oldest durable dispatch outbox entry pending drain.",
                )
                .build(),
            dispatch_outbox_in_flight: meter
                .u64_gauge("agnt5.runtime.dispatch_outbox.in_flight")
                .with_description(
                    "Durable dispatch outbox entries currently being dispatched for a partition.",
                )
                .build(),
            dispatch_outbox_deferred: meter
                .u64_gauge("agnt5.runtime.dispatch_outbox.deferred")
                .with_description(
                    "Durable dispatch outbox entries currently delayed by retry backoff.",
                )
                .build(),
            dispatch_outbox_max_in_flight: meter
                .u64_gauge("agnt5.runtime.dispatch_outbox.max_in_flight")
                .with_description("Configured in-flight dispatch cap for a partition.")
                .build(),
            dispatch_outbox_drain_cycles: meter
                .u64_counter("agnt5.runtime.dispatch_outbox.drain_cycles")
                .with_description(
                    "Dispatch outbox drain loop cycles by result: scanned, skipped_not_leader, \
                     skipped_replay_not_ready, skipped_in_flight_full, or scan_error.",
                )
                .build(),
            dispatch_outbox_tick_gap_ms: meter
                .f64_histogram("agnt5.runtime.dispatch_outbox.tick_gap_ms")
                .with_description(
                    "Wall-clock gap between dispatch outbox drainer tick executions.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            dispatch_outbox_list_dispatch_outbox_duration_ms: meter
                .f64_histogram("agnt5.runtime.dispatch_outbox.list_dispatch_outbox.duration_ms")
                .with_description(
                    "Time spent listing dispatch outbox rows from the materialized store.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            dispatch_outbox_scan_rows: meter
                .u64_histogram("agnt5.runtime.dispatch_outbox.scan.rows")
                .with_description(
                    "Rows observed per dispatch outbox scan by result: scanned, decoded, \
                    dispatched, skipped_in_flight, skipped_deferred, skipped_capacity, \
                    skipped_route_limit, \
                     malformed_key, or malformed_value.",
                )
                .build(),
            dispatch_outbox_row_age_at_spawn_ms: meter
                .f64_histogram("agnt5.runtime.dispatch_outbox.row_age_at_spawn_ms")
                .with_description(
                    "Age of a durable dispatch outbox row when the drainer starts dispatching it.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            dispatch_outbox_insert_to_spawn_ms: meter
                .f64_histogram("agnt5.runtime.dispatch_outbox.insert_to_spawn_ms")
                .with_description(
                    "Time from durable dispatch outbox row insertion to the drainer \
                     starting dispatch. Excludes gateway-to-projection lag.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            dispatch_outbox_guardrail_events: meter
                .u64_counter("agnt5.runtime.dispatch_outbox.guardrail.events")
                .with_description(
                    "Dispatch outbox guardrails that converted stuck retryable rows into terminal failures.",
                )
                .build(),
            terminal_tx_queue_depth: meter
                .u64_gauge("agnt5.runtime.processor.terminal_tx.queue_depth")
                .with_description(
                    "Observed queued terminal notifications waiting on the \
                     processor-to-coordinator terminal channel.",
                )
                .build(),
            terminal_tx_capacity: meter
                .u64_gauge("agnt5.runtime.processor.terminal_tx.capacity")
                .with_description(
                    "Maximum capacity of the processor-to-coordinator terminal \
                     notification channel.",
                )
                .build(),
            terminal_tx_events: meter
                .u64_counter("agnt5.runtime.processor.terminal_tx.events")
                .with_description(
                    "Processor-to-coordinator terminal notifications by send result. \
                     `sent` is the fast non-blocking path; `full_deferred` means \
                     the processor handed delivery to a background task; `closed` \
                     means the receiver was gone.",
                )
                .build(),
            pull_ready_notifications: meter
                .u64_counter("agnt5.runtime.processor.pull_ready_notification.count")
                .with_description(
                    "Post-projection PULL queue notifications by non-blocking send result.",
                )
                .build(),
            demand_dirty_notifications: meter
                .u64_counter("agnt5.runtime.processor.demand_dirty_notification.count")
                .with_description(
                    "Post-commit demand-key dirty notifications by non-blocking send result.",
                )
                .build(),
            terminal_tx_send_wait_ms: meter
                .f64_histogram("agnt5.runtime.processor.terminal_tx.send_wait_ms")
                .with_description(
                    "Wait time for deferred terminal notification sends. Fast-path \
                     try_send success records no sample.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            parquet_files_written: meter
                .u64_counter("agnt5.runtime.s3.parquet_files_written")
                .with_description(
                    "Parquet files successfully PUT by the outbox drainer, \
                     labeled by `workspace`, `project`, `deployment`, \
                     `component`. Per-partition rate of file production — \
                     when divided by a time window it answers \
                     'how fragmented is this partition?' for compaction.",
                )
                .build(),
            cold_get_run_hits: meter
                .u64_counter("agnt5.runtime.runs.cold_get.hits")
                .with_description(
                    "GET /v1/runs/<id> requests served from the S3 cold \
                     tier via CF_RUNS_ARCHIVE. Non-zero means CF_RUNS \
                     retention is dropping rows that users are still \
                     fetching — operationally fine, but a load-bearing \
                     observability signal for tuning hot-tier retention.",
                )
                .build(),
            cold_get_run_misses: meter
                .u64_counter("agnt5.runtime.runs.cold_get.misses")
                .with_description(
                    "GET /v1/runs/<id> requests where the archive pointer \
                     was present but the Parquet did not yield the row \
                     (file retention-deleted, or CF_RUNS_ARCHIVE leaked a \
                     stale entry). Should be near zero in a healthy stack.",
                )
                .build(),
            compaction_runs: meter
                .u64_counter("agnt5.runtime.s3.compaction.runs")
                .with_description(
                    "Compaction passes that successfully merged a partition \
                     into a single Parquet file. Combined with \
                     `parquet_files_written` and partition file-count gauges, \
                     answers 'is compaction keeping up with drainer output?'",
                )
                .build(),
            cf_runs_retention_writes: meter
                .u64_counter("agnt5.runtime.runs_retention.writes")
                .with_description(
                    "CF_RUNS_RETENTION entries written, one per `run.archived` \
                     event. The retention worker drains entries in \
                     archived-time order. Operators read the lifetime sum \
                     to size the worker before it ships.",
                )
                .build(),
            cf_runs_retention_deletes: meter
                .u64_counter("agnt5.runtime.runs_retention.deletes")
                .with_description(
                    "CF_RUNS rows deleted by the retention worker, \
                     one per expired CF_RUNS_RETENTION entry. Combine with \
                     `cf_runs_retention_writes` to track the lag between \
                     archival and hot-tier pruning.",
                )
                .build(),
            cf_runs_retention_outcomes: meter
                .u64_counter("agnt5.runtime.runs_retention.outcomes")
                .with_description(
                    "Archived-run retention reconciliation outcomes, labeled by result.",
                )
                .build(),
            compaction_scheduler_passes: meter
                .u64_counter("agnt5.runtime.s3.compaction.scheduler.passes")
                .with_description(
                    "Compaction scheduler ticks that ran (leader was active \
                     and at least one eligible partition existed). Rate of \
                     1/h is the healthy steady state.",
                )
                .build(),
            compaction_scheduler_errors: meter
                .u64_counter("agnt5.runtime.s3.compaction.scheduler.errors")
                .with_description(
                    "Partitions that returned an error during a compaction \
                     scheduler pass. Non-zero warrants investigation.",
                )
                .build(),
            compaction_scheduler_duration_ms: meter
                .f64_histogram("agnt5.runtime.s3.compaction.scheduler.duration_ms")
                .with_description(
                    "Wall-clock duration of one compaction scheduler pass \
                     (all partitions compacted in that tick) in milliseconds.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
        }
    })
}

pub struct ProcessorMetrics {
    pub s3_flush_duration: Histogram<f64>,
    pub s3_put_errors: Counter<u64>,
    pub s3_put_retries: Counter<u64>,
    pub s3_circuit_open: Counter<u64>,
    pub s3_put_bytes: Counter<u64>,
    pub s3_put_duration_ms: Histogram<f64>,
    pub s3_flush_batch_size: Histogram<u64>,
    pub processor_lag: Gauge<u64>,
    pub processor_lag_seconds: Gauge<u64>,
    pub projection_duration_ms: Histogram<f64>,
    pub record_stage_duration_ms: Histogram<f64>,
    pub record_store_ops: Histogram<u64>,
    pub record_kv_ops: Histogram<u64>,
    pub replay_duration_ms: Histogram<f64>,
    pub processor_replay_start_offset: Gauge<u64>,
    pub processor_replay_target_offset: Gauge<u64>,
    pub processor_replay_remaining_records: Gauge<u64>,
    pub processor_replay_readiness_events: Counter<u64>,
    pub processor_liveness: Gauge<u64>,
    pub processor_consecutive_failures: Gauge<u64>,
    pub processor_supervisor_events: Counter<u64>,
    pub terminal_obligations_observed: Counter<u64>,
    pub terminal_obligation_age_ms: Histogram<f64>,
    pub terminal_supervisor_actions: Counter<u64>,
    pub instance_obligations: Gauge<u64>,
    pub instance_decisions: Counter<u64>,
    pub instance_decision_delay_ms: Histogram<f64>,
    pub instance_decisions_applied: Counter<u64>,
    pub instance_changes_refused: Counter<u64>,
    pub instance_guard_refusals: Counter<u64>,
    pub activation_journal_events: Counter<u64>,
    pub billing_unmetered_steps: Counter<u64>,
    pub billing_requests: Counter<u64>,
    pub billing_count_lag_seconds: Histogram<f64>,
    pub timer_due: Gauge<u64>,
    pub timer_oldest_overdue_age_ms: Gauge<u64>,
    pub timer_batch_saturated: Gauge<u64>,
    pub processor_records: Counter<u64>,
    pub run_lifecycle_events: Counter<u64>,
    pub run_duration_ms: Histogram<f64>,
    pub pull_queue_to_start_ms: Histogram<f64>,
    pub pull_queue_to_projection_ms: Histogram<f64>,
    pub pull_queue_to_assignment_ms: Histogram<f64>,
    pub pull_assignment_to_start_ms: Histogram<f64>,
    pub pull_terminalization_ms: Histogram<f64>,
    pub illegal_transitions: Counter<u64>,
    pub processor_dispatches: Counter<u64>,
    pub orchestrator_queue_to_dispatch_ms: Histogram<f64>,
    pub dispatch_channel_send_wait_ms: Histogram<f64>,
    pub dispatch_ack_wait_ms: Histogram<f64>,
    pub dispatch_assigned_append_ms: Histogram<f64>,
    pub processor_retries: Counter<u64>,
    pub dead_letters: Counter<u64>,
    pub outbox_depth: Gauge<u64>,
    pub outbox_oldest_entry_age_ms: Gauge<u64>,
    pub query_archive_shadow_depth: Gauge<u64>,
    pub query_archive_shadow_rows: Counter<u64>,
    pub query_archive_shadow_drain_cycles: Counter<u64>,
    pub query_archive_shadow_publish_duration_ms: Histogram<f64>,
    pub query_archive_compaction_passes: Counter<u64>,
    pub query_archive_compaction_projects: Counter<u64>,
    pub query_archive_compaction_duration_ms: Histogram<f64>,
    pub query_archive_gc_runs: Counter<u64>,
    pub query_archive_gc_retired_segments: Counter<u64>,
    pub query_archive_gc_due_segments: Gauge<u64>,
    pub query_archive_gc_lag_ms: Gauge<u64>,
    pub dispatch_outbox_depth: Gauge<u64>,
    pub dispatch_outbox_oldest_entry_age_ms: Gauge<u64>,
    pub dispatch_outbox_in_flight: Gauge<u64>,
    pub dispatch_outbox_deferred: Gauge<u64>,
    pub dispatch_outbox_max_in_flight: Gauge<u64>,
    pub dispatch_outbox_drain_cycles: Counter<u64>,
    pub dispatch_outbox_tick_gap_ms: Histogram<f64>,
    pub dispatch_outbox_list_dispatch_outbox_duration_ms: Histogram<f64>,
    pub dispatch_outbox_scan_rows: Histogram<u64>,
    pub dispatch_outbox_row_age_at_spawn_ms: Histogram<f64>,
    pub dispatch_outbox_insert_to_spawn_ms: Histogram<f64>,
    pub dispatch_outbox_guardrail_events: Counter<u64>,
    pub terminal_tx_queue_depth: Gauge<u64>,
    pub terminal_tx_capacity: Gauge<u64>,
    pub terminal_tx_events: Counter<u64>,
    pub pull_ready_notifications: Counter<u64>,
    pub demand_dirty_notifications: Counter<u64>,
    pub terminal_tx_send_wait_ms: Histogram<f64>,
    pub parquet_files_written: Counter<u64>,
    pub cold_get_run_hits: Counter<u64>,
    pub cold_get_run_misses: Counter<u64>,
    pub compaction_runs: Counter<u64>,
    pub cf_runs_retention_writes: Counter<u64>,
    pub cf_runs_retention_deletes: Counter<u64>,
    pub cf_runs_retention_outcomes: Counter<u64>,
    pub compaction_scheduler_passes: Counter<u64>,
    pub compaction_scheduler_errors: Counter<u64>,
    pub compaction_scheduler_duration_ms: Histogram<f64>,
}

#[cfg(test)]
mod tests {
    use super::{
        activation_transition_label, event_type_label, pull_hotpath_durations,
        run_component_type_label, run_deployment_label, run_lifecycle_stage,
    };
    use bytes::Bytes;
    use engine_core::types::{Offset, Record};
    use std::collections::HashMap;

    #[test]
    fn pull_hotpath_durations_use_durable_event_timestamps() {
        let run = engine_proto::Run {
            submitted_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            assigned_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 100_000_000,
            }),
            metadata: HashMap::from([("dispatch_mode".to_string(), "pull".to_string())]),
            ..Default::default()
        };
        let mut started = Record {
            offset: Offset::ZERO,
            project_id: "p".to_string(),
            run_id: "r".to_string(),
            event_type: "run.started".to_string(),
            data: Bytes::new(),
            timestamp_ns: 1_250_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };
        let mut assigned = started.clone();
        assigned.event_type = "run.assigned".to_string();
        assigned.timestamp_ns = 1_100_000_000;
        let mut queued = started.clone();
        queued.event_type = "run.queued".to_string();
        queued.timestamp_ns = 1_000_000_000;
        let queued_durations = pull_hotpath_durations(&queued, &run, 1_050.0);
        assert_eq!(queued_durations.queue_to_projection_ms, Some(50.0));
        assert_eq!(queued_durations.queue_to_assignment_ms, None);

        let assigned_durations = pull_hotpath_durations(&assigned, &run, 1_100.0);
        assert_eq!(assigned_durations.queue_to_projection_ms, None);
        assert_eq!(assigned_durations.queue_to_assignment_ms, Some(100.0));
        assert_eq!(assigned_durations.assignment_to_start_ms, None);
        assert_eq!(assigned_durations.queue_to_start_ms, None);

        let started_durations = pull_hotpath_durations(&started, &run, 1_250.0);
        assert_eq!(started_durations.assignment_to_start_ms, Some(150.0));
        assert_eq!(started_durations.queue_to_start_ms, Some(250.0));

        started.event_type = "run.completed".to_string();
        // Canonical terminal records retain the command timestamp, so use the
        // processor observation time for receipt-to-terminalization latency.
        started.timestamp_ns = 1_325_000_000;
        started.metadata.insert(
            engine_core::lease_fence::META_FENCE_RECEIVED_AT_MS.to_string(),
            "1325".to_string(),
        );
        assert_eq!(
            pull_hotpath_durations(&started, &run, 1_400.0).terminalization_ms,
            Some(75.0)
        );
    }

    #[test]
    fn run_lifecycle_stage_maps_terminal_events() {
        assert_eq!(run_lifecycle_stage("run.completed"), Some("completed"));
        assert_eq!(run_lifecycle_stage("run.failed"), Some("failed"));
        assert_eq!(run_lifecycle_stage("run.cancelled"), Some("cancelled"));
    }

    #[test]
    fn run_lifecycle_stage_ignores_non_run_events() {
        assert_eq!(run_lifecycle_stage("lm.completed"), None);
        assert_eq!(run_lifecycle_stage("job.completed"), None);
    }

    #[test]
    fn activation_metrics_use_the_closed_transition_set() {
        use engine_core::activation::{
            DURABLE_ACTIVATION_V2_CAPABILITY, META_ACTIVATION_ATTEMPT, META_ACTIVATION_CONTRACT,
            META_ACTIVATION_ID, META_ACTIVATION_TRANSITION,
        };
        let marked = |event_type: &str, transition: &str| Record {
            offset: engine_core::types::Offset::ZERO,
            project_id: "p".to_string(),
            run_id: "r".to_string(),
            event_type: event_type.to_string(),
            data: bytes::Bytes::new(),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: std::collections::HashMap::from([
                (
                    META_ACTIVATION_CONTRACT.to_string(),
                    DURABLE_ACTIVATION_V2_CAPABILITY.to_string(),
                ),
                (META_ACTIVATION_ID.to_string(), "actv1_x".to_string()),
                (META_ACTIVATION_ATTEMPT.to_string(), "1".to_string()),
                (
                    META_ACTIVATION_TRANSITION.to_string(),
                    transition.to_string(),
                ),
            ]),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };
        for transition in engine_core::activation::ALL_TRANSITIONS {
            assert_eq!(
                activation_transition_label(&marked("workflow.step.completed", transition)),
                Some(*transition)
            );
        }
        assert_eq!(
            activation_transition_label(&marked("lm.completed", "future")),
            None
        );
        let mut unmarked = marked("workflow.step.completed", "completed");
        unmarked.metadata.remove(META_ACTIVATION_CONTRACT);
        assert_eq!(activation_transition_label(&unmarked), None);
        for name in engine_core::activation::all_event_types() {
            assert_eq!(event_type_label(&name), name.as_str(), "{name}");
        }
        assert_eq!(event_type_label("activation.started.v1"), "other");
    }

    #[test]
    fn timestamp_ms_converts_seconds_and_millis() {
        let ts = prost_types::Timestamp {
            seconds: 12,
            nanos: 345_600_000,
        };
        assert_eq!(super::timestamp_ms(&ts), 12_345);
    }

    #[test]
    fn run_metric_labels_prefer_run_state_over_record_metadata() {
        let mut record = make_record("run.completed");
        record
            .metadata
            .insert("deployment_id".to_string(), "dep-from-record".to_string());
        record
            .metadata
            .insert("component_type".to_string(), "functions".to_string());
        let run = engine_proto::Run {
            deployment_id: "dep-from-run".to_string(),
            component_type: "agent".to_string(),
            ..Default::default()
        };

        assert_eq!(
            run_deployment_label(Some(&record), Some(&run)).as_deref(),
            Some("dep-from-run"),
        );
        assert_eq!(run_component_type_label(Some(&record), Some(&run)), "agent",);
    }

    #[test]
    fn run_metric_labels_fall_back_to_record_metadata() {
        let mut record = make_record("run.queued");
        record
            .metadata
            .insert("deployment_id".to_string(), "dep-from-record".to_string());
        record
            .metadata
            .insert("component_type".to_string(), "functions".to_string());

        assert_eq!(
            run_deployment_label(Some(&record), None).as_deref(),
            Some("dep-from-record"),
        );
        assert_eq!(run_component_type_label(Some(&record), None), "function");
    }

    fn make_record(event_type: &str) -> Record {
        Record {
            offset: Offset(0),
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::new(),
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
}
