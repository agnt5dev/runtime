//! Engine-specific metric instruments + thin wrapper over the generic
//! telemetry init provided by `runtime_common::telemetry`.
//!
//! The OpenTelemetry plumbing (traces, metrics, OTLP exporter) lives in
//! `runtime_common::telemetry`. This module adds the engine-specific
//! `EngineMetrics` struct (write path + cache counters) and the `attrs`
//! helpers used by `service.rs`.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

/// Initialize telemetry and construct engine-specific metrics.
///
/// Delegates to `runtime_common::telemetry::init` for OTEL setup; when
/// OTEL is configured, constructs `EngineMetrics` so the engine service
/// can record write path + cache counters. Returns `None` in console-only
/// mode so hot paths can skip metric recording entirely.
pub fn init(service_name: &str, node_id: &str) -> Option<EngineMetrics> {
    if runtime_common::telemetry::init(service_name, env!("CARGO_PKG_VERSION"), node_id) {
        Some(EngineMetrics::new())
    } else {
        None
    }
}

/// Shutdown telemetry providers.
pub fn shutdown() {
    runtime_common::telemetry::shutdown();
}

/// Engine-specific metrics — only what the engine service uniquely knows.
///
/// Write path and cache metrics live here (recorded in service.rs).
/// Other subsystems have their own crate-level metrics modules:
/// - Storage: engine_storage::metrics (compaction, snapshot)
/// - Processor: engine_processor::metrics (s3 flush, lag, records)
/// - Replication: engine_replication::metrics (quorum, election, seal)
/// - Coordinator: engine_coordinator::metrics (workers, invocations, retries)
#[derive(Clone)]
pub struct EngineMetrics {
    // --- Write path ---
    pub append_duration: Histogram<f64>,
    pub append_records: Counter<u64>,
    pub stale_worker_mutations: Counter<u64>,
    pub checkpoint_phase_duration: Histogram<f64>,
    pub activation_phase_duration: Histogram<f64>,

    // --- Cache ---
    pub cache_lookups: Counter<u64>,

    // --- Pull workers ---
    pub poll_job_count: Counter<u64>,
    pub poll_job_waiter_events: Counter<u64>,
    pub pull_queue_commit_duration: Histogram<f64>,
    pub pull_projection_to_notification_duration: Histogram<f64>,
    pub pull_notification_to_probe_duration: Histogram<f64>,
    pub pull_queue_age_at_probe: Histogram<f64>,
    pub pull_candidate_selection_duration: Histogram<f64>,
    pub pull_assignment_gate_decisions: Counter<u64>,
    pub assignment_commit_duration: Histogram<f64>,
    pub renew_job_lease_count: Counter<u64>,
    pub complete_job_rejected: Counter<u64>,
    pub complete_job_count: Counter<u64>,
    pub complete_job_duration: Histogram<f64>,
    pub complete_job_lifecycle_records: Histogram<u64>,
    pub complete_job_lifecycle_duplicates: Counter<u64>,

    // --- Durable queue backlog ---
    pub queue_pending: Gauge<u64>,
    pub queue_oldest_ready_age_ms: Gauge<u64>,

    // --- Worker capacity reports ---
    pub worker_capacity_reports: Counter<u64>,
    pub worker_capacity_observed_at_ms: Gauge<u64>,
    pub worker_slots_open_polls: Gauge<u64>,
    pub worker_slots_active: Gauge<u64>,
    pub worker_slots_desired: Gauge<u64>,
    pub worker_slots_effective_max: Gauge<u64>,
    pub worker_cpu_usage: Gauge<f64>,
    pub worker_memory_usage: Gauge<f64>,
}

impl EngineMetrics {
    fn new() -> Self {
        let meter = global::meter("agnt5.runtime");

        Self {
            append_duration: meter
                .f64_histogram("agnt5.runtime.engine.append.duration_ms")
                .with_description(
                    "Append RPC latency in milliseconds, labeled by method and result",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            append_records: meter
                .u64_counter("agnt5.runtime.engine.append.records")
                .with_description(
                    "Total records appended to the journal, labeled by method and result",
                )
                .build(),
            stale_worker_mutations: meter
                .u64_counter("agnt5.runtime.engine.worker_mutation.rejected")
                .with_description(
                    "Worker-authored lifecycle mutations rejected by durable lease authority",
                )
                .build(),
            checkpoint_phase_duration: meter
                .f64_histogram("agnt5.runtime.engine.checkpoint.phase.duration_ms")
                .with_description(
                    "Checkpoint RPC phase latency in milliseconds, labeled by phase, checkpoint type, and result",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            cache_lookups: meter
                .u64_counter("agnt5.runtime.engine.cache.lookups")
                .with_description("Cache lookup count")
                .build(),
            poll_job_count: meter
                .u64_counter("agnt5.runtime.engine.poll_job.count")
                .with_description("Single-job PollJob requests by result")
                .build(),
            poll_job_waiter_events: meter
                .u64_counter("agnt5.runtime.engine.poll_job.waiter_events")
                .with_description("Parked PollJob waiter registrations, wakeups, and timeouts")
                .build(),
            pull_queue_commit_duration: meter
                .f64_histogram("agnt5.runtime.engine.pull.queue_commit.duration_ms")
                .with_description(
                    "Durability latency for the journal append containing run.queued",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            pull_projection_to_notification_duration: meter
                .f64_histogram(
                    "agnt5.runtime.engine.pull.projection_to_notification.duration_ms",
                )
                .with_description(
                    "Process-local delay from committed pending projection to waiter notification attempt",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            pull_notification_to_probe_duration: meter
                .f64_histogram("agnt5.runtime.engine.pull.notification_to_probe.duration_ms")
                .with_description(
                    "Process-local delay from waiter notification to the awakened PollJob queue probe",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            pull_queue_age_at_probe: meter
                .f64_histogram("agnt5.runtime.engine.pull.queue_age_at_probe.duration_ms")
                .with_description(
                    "Age of a pending run when PollJob evaluates its HA assignment gate",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::slow_latency_ms())
                .build(),
            pull_candidate_selection_duration: meter
                .f64_histogram("agnt5.runtime.engine.pull.candidate_selection.duration_ms")
                .with_description(
                    "PollJob probe time from pending-index scan start to selection of a claimable run",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            pull_assignment_gate_decisions: meter
                .u64_counter("agnt5.runtime.engine.pull.assignment_gate.count")
                .with_description(
                    "Pending-run HA assignment gate decisions by discovery source, role, and reason",
                )
                .build(),
            assignment_commit_duration: meter
                .f64_histogram("agnt5.runtime.engine.pull.assignment_commit.duration_ms")
                .with_description(
                    "Authoritative run.assigned append latency before PollJob returns a committed lease receipt",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            activation_phase_duration: meter
                .f64_histogram("agnt5.runtime.engine.activation.phase.duration_ms")
                .with_description(
                    "Durable activation RPC phase latency in milliseconds, labeled by operation, phase, and result",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            renew_job_lease_count: meter
                .u64_counter("agnt5.runtime.engine.renew_job_lease.count")
                .with_description("RenewJobLease requests by result")
                .build(),
            complete_job_rejected: meter
                .u64_counter("agnt5.runtime.engine.complete_job.rejected")
                .with_description("CompleteJob requests rejected by lease/session fencing")
                .build(),
            complete_job_count: meter
                .u64_counter("agnt5.runtime.engine.complete_job.count")
                .with_description("CompleteJob acknowledgements received from pull workers")
                .build(),
            complete_job_duration: meter
                .f64_histogram("agnt5.runtime.engine.complete_job.duration_ms")
                .with_description(
                    "Time from parked PollJob claim to CompleteJob receipt for pull workers. \
                     Labeled by result and whether the original claim was still present.",
                )
                .with_unit("ms")
                .with_boundaries(engine_proto::buckets::fast_latency_ms())
                .build(),
            complete_job_lifecycle_records: meter
                .u64_histogram("agnt5.runtime.engine.complete_job.lifecycle_records")
                .with_description(
                    "Lifecycle records carried inside each accepted CompleteJob under \
                     pull_completion_lifecycle_v1, before deduplication.",
                )
                .build(),
            complete_job_lifecycle_duplicates: meter
                .u64_counter("agnt5.runtime.engine.complete_job.lifecycle_duplicates")
                .with_description(
                    "Lifecycle records inside a retried CompleteJob that resolved to an \
                     earlier journal offset instead of being written again.",
                )
                .build(),
            queue_pending: meter
                .u64_gauge("agnt5.runtime.queue.pending")
                .with_description(
                    "Exact pending run count from materialized queue counters, labeled by deployment and priority",
                )
                .build(),
            queue_oldest_ready_age_ms: meter
                .u64_gauge("agnt5.runtime.queue.oldest_ready_age_ms")
                .with_description(
                    "Age in milliseconds of the oldest ready pending run from materialized queue counters",
                )
                .with_unit("ms")
                .build(),
            worker_capacity_reports: meter
                .u64_counter("agnt5.runtime.worker.capacity_reports")
                .with_description("Worker capacity reports accepted or rejected by result")
                .build(),
            worker_capacity_observed_at_ms: meter
                .u64_gauge("agnt5.runtime.worker.capacity_observed_at_ms")
                .with_description(
                    "Runtime receipt time for the worker capacity sample; use it to select the freshest HA series per worker",
                )
                .with_unit("ms")
                .build(),
            worker_slots_open_polls: meter
                .u64_gauge("agnt5.runtime.worker.slots.open_polls")
                .with_description("Open parked PollJob requests currently held by the worker")
                .build(),
            worker_slots_active: meter
                .u64_gauge("agnt5.runtime.worker.slots.active")
                .with_description("Active handler invocations currently running on the worker")
                .build(),
            worker_slots_desired: meter
                .u64_gauge("agnt5.runtime.worker.slots.desired")
                .with_description("Worker-reported desired pull slots")
                .build(),
            worker_slots_effective_max: meter
                .u64_gauge("agnt5.runtime.worker.slots.effective_max")
                .with_description("Worker-reported effective maximum pull slots")
                .build(),
            worker_cpu_usage: meter
                .f64_gauge("agnt5.runtime.worker.resource.cpu_usage")
                .with_description("Worker-reported CPU usage ratio between 0 and 1")
                .build(),
            worker_memory_usage: meter
                .f64_gauge("agnt5.runtime.worker.resource.memory_usage")
                .with_description("Worker-reported memory usage ratio between 0 and 1")
                .build(),
        }
    }
}

/// Attribute keys for consistent labeling.
pub mod attrs {
    use opentelemetry::KeyValue;

    pub fn cache_result(hit: bool) -> KeyValue {
        KeyValue::new("cache", if hit { "hit" } else { "miss" })
    }
}
