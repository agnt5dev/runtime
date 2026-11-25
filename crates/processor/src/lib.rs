//! engine-processor: Event processing and materialized projections.
//!
//! The processor tails the journal log and materializes state into the
//! MaterializedStore. It handles checkpoint management for crash recovery
//! and processes events idempotently (replay-safe).

pub mod dispatch_outbox_drain;
pub mod instance_supervisor;
pub mod lag_clock;
#[cfg(test)]
mod memory_work_store;
pub mod metrics;
pub mod orchestrator;
pub mod periodic;
mod processor_lag;
pub mod projections;
pub mod replay_admission;
pub mod retired_deployments;
pub mod run_state_machine;
pub mod supervision;
pub mod terminal_supervisor;
pub mod timer_poller;
pub mod usage;

#[cfg(test)]
pub use supervision::PartitionProcessorSupervisors;
pub use supervision::{
    PartitionProcessorExit, PartitionProcessorFailureKind, PartitionProcessorState,
    PartitionProcessorStatus, ProcessorHealthSnapshot,
};

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use engine_core::demand::DemandKey;
use engine_core::error::{EngineError, EngineResult};
use engine_core::lease_fence::{
    resume_source_id, strip_stale_resume_metadata, META_RESUME_SOURCE_ID,
};
use engine_core::storage_assessment::{projection_applied_key, ProjectionPartitionCertification};
use engine_core::storage_contract::{
    select_projection_resume_checkpoint, DispatchOutboxStore, JournalPosition,
    ProjectionAdmissionEvidence, ProjectionAdmissionGate, ProjectionCheckpointStore,
    ProjectionCommit, ProjectionCommitter, ProjectionSource, ProjectionStateReader,
    TerminalObligationStore,
};
#[cfg(test)]
use engine_core::traits::MaterializedStore;
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_proto::{ComponentType, Run, Timer};

use crate::projections::{
    activations, approvals, batches, events, jobs, messages, runs, sessions, signals, state,
    timers, StoreOp,
};
use crate::supervision::PartitionProcessorReporter;

use crate::usage::{LLMEventSummary, ModelPricing, UsageReporter};

/// LLM event types that should be forwarded to the control plane for analytics.
const LLM_EVENT_TYPES: &[&str] = &["lm.completed", "lm.call.completed", "lm.stream.completed"];

pub const DISPATCH_OUTBOX_PARTITION_ID_META: &str = "dispatch_outbox_partition_id";
pub const DISPATCH_OUTBOX_QUEUED_OFFSET_META: &str = "dispatch_outbox_queued_offset";
pub const DISPATCH_MODE_META: &str = "dispatch_mode";
pub const DISPATCH_MODE_PULL: &str = "pull";

/// Configuration for the processor.
pub struct ProcessorConfig {
    /// Checkpoint every N records.
    pub checkpoint_interval: u64,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: 100,
        }
    }
}

/// The Processor tails the journal segment and materializes state into the store.
///
/// It is generic over both the segment (for testing with MemorySegment) and
/// the store implementation.
/// Notification sent when a run reaches a terminal state.
/// The coordinator uses this to decrement worker load.
#[derive(Debug, Clone)]
pub struct RunTerminalEvent {
    pub run_id: String,
    pub event_type: String,
    /// Lease generation that produced the terminal projection. Empty only for
    /// legacy/unleased records; consumers must not use an empty generation to
    /// clear a non-empty active dispatch fence.
    pub lease_id: String,
}

/// Advisory wakeup emitted after a pull-claimable queue transition has been
/// projected on the active partition owner. The runtime service uses this to
/// wake one local parked `PollJob`; the periodic queue probe remains the repair
/// path if this bounded notification channel is full or closed.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PullReadyEvent {
    pub project_id: String,
    pub deployment_id: String,
    pub run_id: String,
    pub ready_at_ms: i64,
    pub partition_id: u32,
    pub ready_offset: u64,
    pub component_name: String,
    pub component_type: i32,
    pub projected_at: std::time::Instant,
}

/// Advisory notification that one partition-local demand head may have
/// changed. The derived pending index remains authoritative: consumers must
/// refresh this key from storage and periodically repair missed notifications.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DemandKeyDirtyEvent {
    pub partition_id: u32,
    pub demand_key: DemandKey,
    pub removed_pending_run: Option<RemovedPendingRun>,
}

/// Exact old generation no longer present in the committed pending projection.
/// This is process-local advisory evidence, never grant authority.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RemovedPendingRun {
    pub run_id: String,
    pub dispatch_generation: engine_core::dispatch_generation::DispatchGeneration,
}

fn removed_pending_run(
    existing: Option<&[u8]>,
    projected: Option<&engine_proto::Run>,
    mutations: &[StoreOp],
) -> Option<(DemandKey, RemovedPendingRun)> {
    use engine_core::dispatch_generation::resolve_projected_dispatch_generation;
    if !mutations.iter().any(|op| {
        matches!(
            op,
            StoreOp::RemovePendingPriority { .. } | StoreOp::PutPendingPriority { .. }
        )
    }) {
        return None;
    }
    // Use the old projection, not the lifecycle record's offset: assignment
    // retains the queue generation, whereas resume/expiry creates a new one.
    let old = engine_proto::Run::decode(existing?).ok()?;
    let new = projected?;
    if new.project_id != old.project_id || new.run_id != old.run_id {
        return None;
    }
    let generation = resolve_projected_dispatch_generation(&old.metadata, old.attempt).generation;
    if new.status == "queued"
        && resolve_projected_dispatch_generation(&new.metadata, new.attempt).generation
            == generation
    {
        return None;
    }
    let key = DemandKey::from_run(&old).ok()?;
    // Require a real pending-index mutation for this route. A suppressed or
    // duplicate projection must not manufacture removal evidence.
    let changed = mutations.iter().any(|op| {
        let (deployment_id, component_type, component_name) = match op {
            StoreOp::RemovePendingPriority {
                deployment_id,
                component_type,
                component_name,
                ..
            }
            | StoreOp::PutPendingPriority {
                deployment_id,
                component_type,
                component_name,
                ..
            } => (deployment_id, component_type, component_name),
            _ => return false,
        };
        *deployment_id == old.deployment_id
            && *component_type == key.component_type() as i32
            && *component_name == old.component_name
    });
    changed.then_some((
        key,
        RemovedPendingRun {
            run_id: old.run_id,
            dispatch_generation: generation,
        },
    ))
}

fn dirty_demand_keys(record: &Record, mutations: &[StoreOp]) -> EngineResult<Vec<DemandKey>> {
    let mut keys = BTreeSet::new();
    for mutation in mutations {
        let (deployment_id, component_type, component_name) = match mutation {
            StoreOp::PutPendingPriority {
                deployment_id,
                component_type,
                component_name,
                ..
            }
            | StoreOp::RemovePendingPriority {
                deployment_id,
                component_type,
                component_name,
                ..
            } => (deployment_id, *component_type, component_name),
            _ => continue,
        };
        let component_type = ComponentType::try_from(component_type).map_err(|_| {
            EngineError::Storage(format!(
                "pending demand mutation for run {} has unknown component type {component_type}",
                record.run_id
            ))
        })?;
        let key = DemandKey::new(
            record.project_id.clone(),
            deployment_id.clone(),
            component_type,
            component_name.clone(),
        )
        .map_err(|error| {
            EngineError::Storage(format!(
                "pending demand mutation for run {} has invalid route: {error}",
                record.run_id
            ))
        })?;
        keys.insert(key);
    }
    Ok(keys.into_iter().collect())
}

fn pull_ready_event(
    record: &Record,
    partition_id: u32,
    projected_at: std::time::Instant,
) -> Option<PullReadyEvent> {
    if !matches!(
        record.event_type.as_str(),
        "run.queued" | "run.resumed" | "run.lease_expired"
    ) {
        return None;
    }
    let deployment_id = record.metadata.get("deployment_id")?.clone();
    let component_name = record
        .metadata
        .get("component_name")
        .cloned()
        .unwrap_or_default();
    let component_type = match record
        .metadata
        .get("component_type")
        .map(|value| value.to_lowercase())
        .as_deref()
    {
        Some("function" | "functions") => ComponentType::Function as i32,
        Some("workflow" | "workflows") => ComponentType::Workflow as i32,
        Some("agent" | "agents") => ComponentType::Agent as i32,
        Some("tool" | "tools") => ComponentType::Tool as i32,
        Some("scorer" | "scorers") => ComponentType::Scorer as i32,
        _ => ComponentType::Unspecified as i32,
    };
    if record.project_id.is_empty() || deployment_id.is_empty() {
        return None;
    }
    Some(PullReadyEvent {
        project_id: record.project_id.clone(),
        deployment_id,
        run_id: record.run_id.clone(),
        ready_at_ms: record.timestamp_ns / 1_000_000,
        partition_id,
        ready_offset: record.offset.0,
        component_name,
        component_type,
        projected_at,
    })
}

fn notify_pull_ready_event(
    tx: &tokio::sync::mpsc::Sender<PullReadyEvent>,
    event: PullReadyEvent,
    partition_id: u32,
    node_id: &str,
) {
    let result = match tx.try_send(event) {
        Ok(()) => "sent",
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => "full_dropped",
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => "closed",
    };
    crate::metrics::record_pull_ready_notification(partition_id, node_id, result);
}

fn notify_demand_key_dirty(
    tx: &tokio::sync::mpsc::Sender<DemandKeyDirtyEvent>,
    event: DemandKeyDirtyEvent,
    node_id: &str,
) {
    let partition_id = event.partition_id;
    let result = match tx.try_send(event) {
        Ok(()) => "sent",
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => "full_dropped",
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => "closed",
    };
    crate::metrics::record_demand_dirty_notification(partition_id, node_id, result);
}

fn notify_terminal_event(
    tx: &tokio::sync::mpsc::Sender<RunTerminalEvent>,
    evt: RunTerminalEvent,
    partition_id: Option<u32>,
    node_id: Option<&str>,
) {
    if let (Some(partition_id), Some(node_id)) = (partition_id, node_id) {
        crate::metrics::record_terminal_tx_state(
            partition_id,
            node_id,
            tx.capacity(),
            tx.max_capacity(),
        );
    }

    match tx.try_send(evt) {
        Ok(()) => {
            crate::metrics::record_terminal_tx_event(partition_id, node_id, "sent");
            if let (Some(partition_id), Some(node_id)) = (partition_id, node_id) {
                crate::metrics::record_terminal_tx_state(
                    partition_id,
                    node_id,
                    tx.capacity(),
                    tx.max_capacity(),
                );
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(evt)) => {
            crate::metrics::record_terminal_tx_event(partition_id, node_id, "closed");
            warn!(
                run_id = %evt.run_id,
                event_type = %evt.event_type,
                "Terminal event notification channel closed"
            );
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(evt)) => {
            crate::metrics::record_terminal_tx_event(partition_id, node_id, "full_dropped");
            warn!(
                run_id = %evt.run_id,
                event_type = %evt.event_type,
                lease_id = %evt.lease_id,
                "Terminal event notification channel full; dropping advisory notification"
            );
        }
    }
}

/// What projecting one journal record changed, as the processing loop needs
/// to know it.
#[derive(Debug, Default)]
pub struct ProcessedRecord {
    terminal_lease_id: Option<String>,
    terminal_event_accepted: bool,
    cache_journal_record: bool,
    dispatch_outbox_inserted: bool,
    /// Whether this record left an always-on agent instance needing a
    /// decision, which wakes the partition's instance supervisor.
    instance_obligation_put: bool,
    dirty_demand_keys: Vec<DemandKey>,
    removed_pending_run: Option<(DemandKey, RemovedPendingRun)>,
    /// Model usage for control-plane analytics, attributed through the
    /// projected run when the record itself carries no deployment context.
    llm_event: Option<LLMEventSummary>,
}

impl ProcessedRecord {
    /// Whether this record wrote a push-dispatch outbox entry.
    pub fn dispatch_outbox_inserted(&self) -> bool {
        self.dispatch_outbox_inserted
    }

    /// Whether this record left an instance needing its leader's decision.
    pub fn instance_obligation_put(&self) -> bool {
        self.instance_obligation_put
    }
}

/// Fixed, partition-scoped range applied by the offline recovery tool.
///
/// Both offsets are local, exclusive journal frontiers. The interface omits a
/// journal writer by design: offline replay may mutate only the disposable
/// projection supplied through `projection_committer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfflineProjectionReplayRequest {
    pub partition_id: u32,
    pub partition_count: usize,
    pub from: u64,
    pub through: u64,
    pub page_records: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfflineProjectionReplayResult {
    pub partition_id: u32,
    pub from: u64,
    pub through: u64,
    pub records_applied: u64,
}

/// The lease a lease-ending record released, for the coordinator's
/// active-dispatch bookkeeping. Runtime- and worker-authored records carry
/// the lease in metadata; control-plane records (cancellation) do not, so the
/// pre-projection Run supplies the lease that was active when they landed.
fn released_lease_id(record: &Record, existing_run: Option<&[u8]>) -> Option<String> {
    if !matches!(
        record.event_type.as_str(),
        "run.paused" | "workflow.paused" | "run.completed" | "run.failed" | "run.cancelled"
    ) {
        return None;
    }
    record
        .metadata
        .get(engine_core::lease_fence::META_FENCE_LEASE_ID)
        .or_else(|| record.metadata.get("lease_id"))
        .filter(|lease_id| !lease_id.is_empty())
        .cloned()
        .or_else(|| {
            let run = <engine_proto::Run as prost::Message>::decode(existing_run?).ok()?;
            (!run.lease_id.is_empty()).then_some(run.lease_id)
        })
}

/// Writes one projection operation into a key-value batch, in the layout of
/// the store the batch is for. The hosted runtime passes
/// `engine_store::projection_adapter::encode_op`; the processor itself knows
/// no storage layout.
#[cfg(test)]
pub type LegacyOpEncoder = fn(&Record, &StoreOp, &mut engine_core::types::BatchWriteOps);

#[cfg(test)]
pub struct Processor<S: Segment, M: MaterializedStore> {
    segment: Arc<S>,
    store: Arc<M>,
    config: ProcessorConfig,
    encode_op: LegacyOpEncoder,
    analytics_reporter: Option<Arc<dyn UsageReporter>>,
    pricing_cache: Option<Arc<dyn ModelPricing>>,
    terminal_tx: Option<tokio::sync::mpsc::Sender<RunTerminalEvent>>,
}

#[cfg(test)]
impl<S: Segment, M: MaterializedStore> Processor<S, M> {
    pub fn new(
        segment: Arc<S>,
        store: Arc<M>,
        config: ProcessorConfig,
        encode_op: LegacyOpEncoder,
    ) -> Self {
        Self {
            segment,
            store,
            config,
            encode_op,
            analytics_reporter: None,
            pricing_cache: None,
            terminal_tx: None,
        }
    }

    /// Set the analytics reporter for sending run completions to the control plane.
    pub fn with_analytics_reporter(mut self, reporter: Arc<dyn UsageReporter>) -> Self {
        self.analytics_reporter = Some(reporter);
        self
    }

    /// Set the cached LLM pricing snapshot used to materialize run costs.
    pub fn with_pricing_cache(mut self, pricing_cache: Arc<dyn ModelPricing>) -> Self {
        self.pricing_cache = Some(pricing_cache);
        self
    }

    /// Set the terminal event notifier for coordinator load tracking.
    pub fn with_terminal_notifier(
        mut self,
        tx: tokio::sync::mpsc::Sender<RunTerminalEvent>,
    ) -> Self {
        self.terminal_tx = Some(tx);
        self
    }

    /// Run the processor loop. Tails the segment from the last checkpoint
    /// and materializes events into the store.
    ///
    /// This method runs until the cancellation token is triggered or an
    /// unrecoverable error occurs.
    pub async fn run(&self, cancel: CancellationToken) -> EngineResult<()> {
        // Load checkpoint — resume from last processed offset
        // Use empty project_id for global checkpoint (single-tenant MVP)
        let checkpoint = self.store.get_checkpoint("").await?;
        let start_offset = match checkpoint {
            Some(offset) => offset.next(),
            None => Offset::ZERO,
        };

        info!(
            start_offset = %start_offset,
            checkpoint = ?checkpoint,
            "Processor starting"
        );

        let mut stream = self.segment.tail(start_offset).await?;
        let mut records_since_checkpoint: u64 = 0;
        let mut last_offset = checkpoint.unwrap_or(Offset::ZERO);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Final checkpoint before shutdown
                    if records_since_checkpoint > 0 {
                        if let Err(e) = self.store.set_checkpoint("", last_offset).await {
                            error!(error = %e, "Failed to write final checkpoint");
                        } else {
                            info!(offset = %last_offset, "Final checkpoint saved");
                        }
                    }
                    info!("Processor shutting down");
                    return Ok(());
                }
                item = stream.next() => {
                    match item {
                        Some(Ok(record)) => {
                            let rec_start = std::time::Instant::now();
                            self.process_record(&record).await?;

                            // Record per-record processing time via OTel
                            {
                                use opentelemetry::global;
                                static RECORD_DURATION: std::sync::OnceLock<opentelemetry::metrics::Histogram<f64>> = std::sync::OnceLock::new();
                                let h = RECORD_DURATION.get_or_init(|| {
                                    global::meter("agnt5.runtime")
                                        .f64_histogram("agnt5.runtime.processor.record.duration_ms")
                                        .with_description("Time to process a single record in ms")
                                        .with_unit("ms")
                                        .with_boundaries(engine_proto::buckets::fast_latency_ms())
                                        .build()
                                });
                                h.record(rec_start.elapsed().as_secs_f64() * 1000.0, &[]);
                            }

                            last_offset = record.offset;
                            records_since_checkpoint += 1;
                            crate::metrics::processor_metrics().processor_records.add(1, &[]);

                            // Periodic checkpoint
                            if records_since_checkpoint >= self.config.checkpoint_interval {
                                self.store.set_checkpoint("", last_offset).await?;
                                debug!(offset = %last_offset, "Checkpoint saved");
                                records_since_checkpoint = 0;

                                // Sample processor lag at checkpoint boundaries
                                if let Ok(tail) = self.segment.tail_offset().await {
                                    let lag = tail.0.saturating_sub(last_offset.0);
                                    crate::metrics::processor_metrics().processor_lag.record(lag, &[]);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "Error reading from segment stream");
                            return Err(e);
                        }
                        None => {
                            // Stream ended (segment sealed)
                            info!("Segment stream ended");
                            if records_since_checkpoint > 0 {
                                self.store.set_checkpoint("", last_offset).await?;
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// Process a single record: apply all projections and write to store.
    async fn process_record(&self, record: &Record) -> EngineResult<()> {
        let journal_record = record;
        let enriched_record = self
            .pricing_cache
            .as_ref()
            .and_then(|cache| cache.enrich_record(record));
        let record = enriched_record.as_ref().unwrap_or(record);

        // Load existing state needed by projections
        let existing_run = ProjectionStateReader::load_projection_run(
            self.store.as_ref(),
            &record.project_id,
            &record.run_id,
        )
        .await?;
        let projection_activation_id = activations::activation_id(record).map(str::to_string);
        let existing_activation = if let Some(activation_id) = projection_activation_id.as_deref() {
            ProjectionStateReader::load_projection_activation(
                self.store.as_ref(),
                &record.project_id,
                &record.run_id,
                activation_id,
            )
            .await?
        } else {
            None
        };

        let existing_approval = if record.event_type == "approval.resolved" {
            if let Some(aid) = record.metadata.get("approval_id") {
                ProjectionStateReader::load_projection_approval(
                    self.store.as_ref(),
                    &record.project_id,
                    aid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        let existing_signal = if record.event_type == "signal.acked" {
            if let Some(sid) = record.metadata.get("signal_id") {
                ProjectionStateReader::load_projection_signal(
                    self.store.as_ref(),
                    &record.project_id,
                    sid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        let existing_timer = if matches!(
            record.event_type.as_str(),
            "timer.fired" | "timer.canceled" | "timer.expired"
        ) {
            if let Some(tid) = record.metadata.get("timer_id") {
                ProjectionStateReader::load_projection_timer(
                    self.store.as_ref(),
                    &record.project_id,
                    tid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        let existing_job = if matches!(
            record.event_type.as_str(),
            "job.claimed" | "job.started" | "job.completed" | "job.failed" | "job.cancelled"
        ) {
            if let Some(jid) = record.metadata.get("job_id") {
                ProjectionStateReader::load_projection_job(
                    self.store.as_ref(),
                    &record.project_id,
                    jid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        let existing_batch = if matches!(
            record.event_type.as_str(),
            "batch.started"
                | "batch.item_completed"
                | "batch.item_failed"
                | "batch.completed"
                | "batch.cancelled"
        ) {
            if let Some(bid) = record.metadata.get("batch_id") {
                ProjectionStateReader::load_projection_batch(
                    self.store.as_ref(),
                    &record.project_id,
                    bid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        let existing_session = if record.event_type == "session.updated" {
            if let Some(sid) = record.metadata.get("session_id") {
                ProjectionStateReader::load_projection_session(
                    self.store.as_ref(),
                    &record.project_id,
                    sid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        let existing_message = if record.event_type == "message.acked" {
            if let Some(mid) = record.metadata.get("message_id") {
                ProjectionStateReader::load_projection_message(
                    self.store.as_ref(),
                    &record.project_id,
                    mid,
                )
                .await?
            } else {
                None
            }
        } else {
            None
        };

        // worker.registered/heartbeat/deregistered no longer
        // produce projection ops — gossip is the routing source of record.

        // A refused instance start is a no-op for the run, as in the
        // production path.
        let instance_start = crate::projections::instances::check_decision_on_apply(
            self.store.as_ref(),
            record,
            engine_core::partition_offset::decode_offset(record.offset).0 as u32,
            existing_run.as_deref(),
        )
        .await?;
        let instance_start_refused = instance_start
            .as_ref()
            .is_some_and(|applied| !applied.accepted());

        // Collect ops from all projections
        let run_result = if instance_start_refused {
            crate::projections::ProjectionResult { ops: Vec::new() }
        } else {
            runs::project(record, existing_run.as_deref())
        };
        let activation_result = activations::project(record, existing_activation.as_deref())?;
        let mut state_ops = state::project(record);
        state_ops.extend(
            runtime_event_capture(
                self.store.as_ref(),
                record,
                engine_core::partition_offset::decode_offset(record.offset).0 as u32,
                &run_result,
            )
            .await?,
        );
        if let Some(work) = engine_core::runtime_work::RuntimeWork::from_record(record)? {
            state_ops.push(StoreOp::PutRuntimeWork(work));
        }
        let approval_ops = approvals::project(record, existing_approval.as_deref());
        let signal_ops = signals::project(record, existing_signal.as_deref());
        let timer_ops = timers::project(record, existing_timer.as_deref());
        let job_ops = jobs::project(record, existing_job.as_deref());
        let batch_ops = batches::project(record, existing_batch.as_deref());
        let session_ops = sessions::project(record, existing_session.as_deref());
        let message_ops = messages::project(record, existing_message.as_deref());
        let event_ops = events::project(record);
        let mut instance_ops = crate::projections::instances::project_record(
            self.store.as_ref(),
            record,
            engine_core::partition_offset::decode_offset(record.offset).0 as u32,
            existing_run.as_deref(),
            &run_result,
        )
        .await?;
        if let Some(applied) = instance_start {
            instance_ops.extend(applied.ops);
        }

        // Dispatch all store operations
        for op in run_result
            .ops
            .iter()
            .chain(activation_result.ops.iter())
            .chain(state_ops.iter())
            .chain(approval_ops.iter())
            .chain(signal_ops.iter())
            .chain(timer_ops.iter())
            .chain(job_ops.iter())
            .chain(batch_ops.iter())
            .chain(session_ops.iter())
            .chain(message_ops.iter())
            .chain(event_ops.iter())
            .chain(instance_ops.iter())
        {
            self.dispatch_op(op, journal_record).await?;
        }
        if let Some(run) = run_result
            .run_data()
            .and_then(|data| engine_proto::Run::decode(data).ok())
        {
            let (partition_id, _) = engine_core::partition_offset::decode_offset(record.offset);
            crate::metrics::record_pull_hotpath_durations(record, &run, partition_id as u32);
        }

        // Notify the coordinator on terminal run events so it can
        // decrement worker load tied to the active dispatch.
        if matches!(
            record.event_type.as_str(),
            "run.completed" | "run.failed" | "run.cancelled" | "run.paused" | "workflow.paused"
        ) {
            if let Some(tx) = &self.terminal_tx {
                notify_terminal_event(
                    tx,
                    RunTerminalEvent {
                        run_id: record.run_id.clone(),
                        event_type: record.event_type.clone(),
                        lease_id: released_lease_id(record, existing_run.as_deref())
                            .unwrap_or_default(),
                    },
                    None,
                    None,
                );
            }
        }

        // Orchestrator: evaluate and log (observe-only in the Processor<S,M> path).
        // Active dispatch only happens through run_partition_processor.
        let action = crate::orchestrator::evaluate(record, existing_run.as_deref());
        crate::orchestrator::log_action(&action);

        // Buffer terminal runs for control plane analytics.
        if let Some(reporter) = self.analytics_reporter.as_ref() {
            if is_terminal_event(record) {
                if let Some(run_data) = run_result.run_data() {
                    reporter.run_finished(run_data, &record.project_id).await;
                }
            }

            // Buffer LLM events for control plane analytics
            if LLM_EVENT_TYPES.contains(&record.event_type.as_str()) {
                let projected_run = run_result
                    .run_data()
                    .and_then(|data| engine_proto::Run::decode(data).ok());
                if let Some(event) = parse_llm_event(record, projected_run.as_ref()) {
                    reporter.model_call(event).await;
                }
            }
        }

        Ok(())
    }

    /// Execute a single store operation.
    async fn dispatch_op(&self, op: &StoreOp, record: &Record) -> EngineResult<()> {
        match op {
            StoreOp::PutRun(data) => {
                self.store
                    .put_run(&record.project_id, &record.run_id, data)
                    .await?;
            }
            StoreOp::PutStepMemo { step_key, data } => {
                self.store
                    .put_step_memo(&record.project_id, &record.run_id, step_key, data)
                    .await?;
            }
            StoreOp::PutActivation {
                activation_id,
                data,
                version,
            } => {
                self.store
                    .put_entity_state(
                        &record.project_id,
                        engine_core::storage_contract::ACTIVATION_PROJECTION_ENTITY_TYPE,
                        activation_id,
                        engine_core::storage_contract::ACTIVATION_PROJECTION_SCOPE,
                        &record.run_id,
                        data,
                        *version,
                    )
                    .await?;
            }
            StoreOp::PutActivationOutbox {
                activation_id,
                attempt,
                intent_id,
                data,
            } => {
                self.store
                    .put_entity_state(
                        &record.project_id,
                        engine_core::storage_contract::ACTIVATION_OUTBOX_ENTITY_TYPE,
                        &format!("{activation_id}/{attempt}/{intent_id}"),
                        engine_core::storage_contract::ACTIVATION_PROJECTION_SCOPE,
                        &record.run_id,
                        data,
                        i64::from(*attempt),
                    )
                    .await?;
            }
            StoreOp::PutEvent => {
                self.store.put_event(record).await?;
            }
            StoreOp::PutEntityState {
                entity_type,
                entity_key,
                scope,
                scope_id,
                data,
                version,
            } => {
                self.store
                    .put_entity_state(
                        &record.project_id,
                        entity_type,
                        entity_key,
                        scope,
                        scope_id,
                        data,
                        *version,
                    )
                    .await?;
            }
            StoreOp::PutApproval {
                approval_id,
                run_id,
                data,
            } => {
                self.store
                    .put_approval(&record.project_id, approval_id, run_id, data)
                    .await?;
            }
            StoreOp::PutSignal {
                signal_id,
                run_id,
                signal_name,
                waiting_step,
                data,
            } => {
                self.store
                    .put_signal(
                        &record.project_id,
                        signal_id,
                        run_id,
                        signal_name,
                        waiting_step,
                        data,
                    )
                    .await?;
            }
            StoreOp::PutTimer {
                timer_id,
                run_id,
                status,
                fire_at_ns,
                data,
            } => {
                self.store
                    .put_timer(
                        &record.project_id,
                        timer_id,
                        run_id,
                        status,
                        *fire_at_ns,
                        data,
                    )
                    .await?;
            }
            StoreOp::PutJob {
                job_id,
                run_id,
                batch_id,
                data,
            } => {
                self.store
                    .put_job(&record.project_id, job_id, run_id, batch_id, data)
                    .await?;
            }
            StoreOp::PutBatch { batch_id, data } => {
                self.store
                    .put_batch(&record.project_id, batch_id, data)
                    .await?;
            }
            StoreOp::PutSession {
                session_id,
                component_name,
                data,
            } => {
                self.store
                    .put_session(&record.project_id, session_id, component_name, data)
                    .await?;
            }
            StoreOp::PutMessage {
                message_id,
                correlation_id,
                data,
            } => {
                self.store
                    .put_message(&record.project_id, message_id, correlation_id, data)
                    .await?;
            }
            StoreOp::PutPendingRun {
                component_name,
                data,
            } => {
                self.store
                    .put_pending_run(&record.project_id, component_name, &record.run_id, data)
                    .await?;
            }
            StoreOp::RemovePendingRun { component_name } => {
                self.store
                    .remove_pending_run(&record.project_id, component_name, &record.run_id)
                    .await?;
            }
            StoreOp::PutPendingPriority {
                deployment_id,
                component_type: _,
                component_name,
                priority,
                ready_at_ms,
                created_at_ms,
            } => {
                self.store
                    .put_pending_priority(
                        &record.project_id,
                        deployment_id,
                        component_name,
                        *priority,
                        *ready_at_ms,
                        *created_at_ms,
                        &record.run_id,
                    )
                    .await?;
            }
            StoreOp::RemovePendingPriority {
                deployment_id,
                component_type: _,
                component_name,
                priority,
                ready_at_ms,
                created_at_ms,
            } => {
                self.store
                    .remove_pending_priority(
                        &record.project_id,
                        deployment_id,
                        component_name,
                        *priority,
                        *ready_at_ms,
                        *created_at_ms,
                        &record.run_id,
                    )
                    .await?;
            }
            StoreOp::PutPendingDeploymentPriority {
                deployment_id,
                priority,
                ready_at_ms,
                created_at_ms,
            } => {
                self.store
                    .put_pending_deployment_priority(
                        &record.project_id,
                        deployment_id,
                        *priority,
                        *ready_at_ms,
                        *created_at_ms,
                        &record.run_id,
                    )
                    .await?;
            }
            StoreOp::RemovePendingDeploymentPriority {
                deployment_id,
                priority,
                ready_at_ms,
                created_at_ms,
            } => {
                self.store
                    .remove_pending_deployment_priority(
                        &record.project_id,
                        deployment_id,
                        *priority,
                        *ready_at_ms,
                        *created_at_ms,
                        &record.run_id,
                    )
                    .await?;
            }
            StoreOp::PutLeaseIndex {
                expires_at_ms,
                worker_id,
            } => {
                self.store
                    .put_lease_index(
                        &record.project_id,
                        &record.run_id,
                        *expires_at_ms,
                        worker_id,
                    )
                    .await?;
            }
            StoreOp::RemoveLeaseIndex {
                expires_at_ms,
                worker_id,
            } => {
                self.store
                    .remove_lease_index(
                        &record.project_id,
                        &record.run_id,
                        *expires_at_ms,
                        worker_id,
                    )
                    .await?;
            }
            // The legacy `Processor<S,M>::dispatch_op` path commits each
            // op separately (one `MaterializedStore` call per op),
            // breaking the atomic-batch invariant the production
            // `process_record_standalone` path enforces. We route the
            // outbox ops through `write_kv_batch` so they at least land,
            // but the `PutRun` for the same record is a separate write —
            // a crash between the two leaves the outbox in a recoverable
            // (Run absent → next replay re-emits PutRunArchiveIntent) but not
            // strictly atomic state. This path is only used by
            // integration-test fixtures, never the production hot path.
            StoreOp::PutRuntimeWork(_)
            | StoreOp::PutTerminalObligation { .. }
            | StoreOp::DeleteTerminalObligation { .. }
            | StoreOp::PutInstance { .. }
            | StoreOp::PutInstanceObligation { .. }
            | StoreOp::DeleteInstanceObligation { .. }
            | StoreOp::PutInboxItemContent { .. }
            | StoreOp::DeleteInboxItemContent { .. }
            | StoreOp::PutInstanceResumeRequest { .. }
            | StoreOp::DeleteInstanceResumeRequest { .. }
            | StoreOp::PutDispatchOutbox { .. }
            | StoreOp::DeleteDispatchOutbox { .. }
            | StoreOp::PutRunArchiveIntent { .. }
            | StoreOp::DeleteRunArchiveIntent { .. }
            | StoreOp::PutRunQueryArchiveIntent { .. }
            | StoreOp::DeleteRunQueryArchiveIntent { .. }
            | StoreOp::PutRunQueryArchiveReceipt { .. }
            | StoreOp::PutRunHotIndex { .. }
            | StoreOp::PutRunArchiveLocation { .. }
            | StoreOp::PutRunRetention { .. }
            | StoreOp::PutEventHotIndex { .. }
            | StoreOp::UpdateQueueCounters { .. } => {
                let mut batch = engine_core::types::BatchWriteOps::new();
                (self.encode_op)(record, op, &mut batch);
                self.store.write_kv_batch(batch).await?;
            }
        }
        Ok(())
    }
}

// =============================================================================
// Standalone per-partition processor
// =============================================================================

use engine_core::traits::RecordStream;

struct AttemptBackgroundTask {
    cancel: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl AttemptBackgroundTask {
    async fn shutdown(mut self) {
        self.cancel.cancel();
        let Some(mut handle) = self.handle.take() else {
            return;
        };
        if tokio::time::timeout(std::time::Duration::from_secs(1), &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for AttemptBackgroundTask {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.as_ref() {
            handle.abort();
        }
    }
}

async fn wait_for_attempt_background_task(
    task: &mut Option<AttemptBackgroundTask>,
) -> Result<(), tokio::task::JoinError> {
    match task.as_mut().and_then(|task| task.handle.as_mut()) {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

async fn finish_partition_processor_attempt(
    dispatch_drainer: &mut Option<AttemptBackgroundTask>,
    terminal_supervisor: &mut Option<AttemptBackgroundTask>,
    exit: PartitionProcessorExit,
) -> PartitionProcessorExit {
    if let Some(task) = dispatch_drainer.take() {
        task.shutdown().await;
    }
    if let Some(task) = terminal_supervisor.take() {
        task.shutdown().await;
    }
    exit
}

/// Run the shared processor against a single partition without managed
/// routing, RocksDB, S3, replication, or control-plane composition.
///
/// The PostgreSQL profile uses partition zero initially. Its journal
/// offsets are already local, which is byte-for-byte equivalent to the public
/// encoded form for partition zero.
#[allow(clippy::too_many_arguments)]
pub async fn run_single_partition_processor<T>(
    node_id: String,
    journal: Arc<dyn Segment>,
    store: Arc<T>,
    projection_committer: Arc<dyn ProjectionCommitter>,
    checkpoint_interval: u64,
    cancel: CancellationToken,
    dispatch_tx: Option<tokio::sync::mpsc::Sender<crate::orchestrator::DispatchEnvelope>>,
    terminal_tx: Option<tokio::sync::mpsc::Sender<RunTerminalEvent>>,
) -> EngineResult<PartitionProcessorExit>
where
    T: ProjectionStateReader
        + ProjectionCheckpointStore
        + DispatchOutboxStore
        + TerminalObligationStore
        + engine_core::storage_contract::InstanceSupervisorStore
        + engine_core::storage_contract::TimerWorkStore
        + 'static,
{
    let checkpoint_key = "processor-partition-0".to_string();
    let periodic_checkpoint = store.load_projection_checkpoint(&checkpoint_key).await?;
    let exact_applied = store
        .load_projection_checkpoint(&projection_applied_key(0))
        .await?;
    let checkpoint = select_projection_resume_checkpoint(periodic_checkpoint, exact_applied)?;
    let tail = journal.tail_offset().await?;
    let start = match checkpoint {
        Some(offset) => {
            if offset.0 >= engine_core::partition_offset::MAX_LOCAL_OFFSET {
                return Err(EngineError::InvalidArgument(format!(
                    "{checkpoint_key} is at the maximum encodable local offset"
                )));
            }
            let start = Offset(offset.0 + 1);
            if start.0 > tail.0 {
                return Err(EngineError::InvalidArgument(format!(
                    "{checkpoint_key} checkpoint {} is past journal tail {}",
                    offset.0, tail.0
                )));
            }
            start
        }
        None => Offset::ZERO,
    };
    let stream = journal.tail(start).await?;
    Ok(run_partition_processor_attempt_with_ports(
        0,
        node_id,
        stream,
        store.clone(),
        store.clone(),
        store,
        checkpoint_key,
        checkpoint_interval.max(1),
        cancel,
        None,
        None,
        dispatch_tx,
        Some(journal.clone()),
        None,
        terminal_tx,
        None,
        None,
        Some(start),
        Some(tail),
        Some(journal),
        None,
        None,
        projection_committer,
        None,
        None,
        None,
    )
    .await)
}

async fn admit_projection_replay(
    partition_id: u32,
    replay_target: Option<u64>,
    journal: Option<&Arc<dyn Segment>>,
    admission: Option<&Arc<dyn ProjectionAdmissionGate>>,
) -> EngineResult<()> {
    let Some(admission) = admission else {
        return Ok(());
    };
    if admission.is_admitted().await? {
        return Ok(());
    }
    let target = replay_target.ok_or_else(|| {
        EngineError::Storage(format!(
            "partition {partition_id} cannot certify projection replay without a fixed target"
        ))
    })?;
    let journal = journal.ok_or_else(|| {
        EngineError::Storage(format!(
            "partition {partition_id} cannot certify projection replay without a journal"
        ))
    })?;
    let committed = journal.committed_prefix_evidence().await?;
    if committed.through.0 < target {
        return Err(EngineError::Storage(format!(
            "partition {partition_id} replay target {target} exceeds verified committed frontier {}",
            committed.through.0
        )));
    }
    let journal_digest_v1 = if target == committed.through.0 {
        committed.digest_v1
    } else if target == 0 {
        String::new()
    } else {
        journal
            .extend_commit_chain_digest_v1(Offset::ZERO, Offset(target), "")
            .await?
    };
    admission
        .admit(ProjectionAdmissionEvidence {
            partition_id,
            certification: ProjectionPartitionCertification {
                applied_through: target,
                journal_digest_v1,
            },
        })
        .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_partition_processor_attempt_with_ports<R, C, D>(
    partition_id: u32,
    node_id: String,
    mut stream: RecordStream,
    projection_state: Arc<R>,
    checkpoint_store: Arc<C>,
    dispatch_outbox_store: Arc<D>,
    checkpoint_key: String,
    checkpoint_interval: u64,
    cancel: CancellationToken,
    analytics_reporter: Option<Arc<dyn UsageReporter>>,
    pricing_cache: Option<Arc<dyn ModelPricing>>,
    dispatch_tx: Option<tokio::sync::mpsc::Sender<crate::orchestrator::DispatchEnvelope>>,
    segment: Option<Arc<dyn Segment>>,
    cache: Option<Arc<engine_cache::RecordCache>>,
    terminal_tx: Option<tokio::sync::mpsc::Sender<RunTerminalEvent>>,
    is_leader_rx: Option<tokio::sync::watch::Receiver<bool>>,
    lag_clock: Option<crate::lag_clock::LagClock>,
    startup_start_offset: Option<Offset>,
    startup_replay_target: Option<Offset>,
    lag_segment: Option<Arc<dyn Segment>>,
    health_reporter: Option<PartitionProcessorReporter>,
    mut startup_replay_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    projection_committer: Arc<dyn ProjectionCommitter>,
    projection_admission: Option<Arc<dyn ProjectionAdmissionGate>>,
    pull_ready_tx: Option<tokio::sync::mpsc::Sender<PullReadyEvent>>,
    demand_dirty_tx: Option<tokio::sync::mpsc::Sender<DemandKeyDirtyEvent>>,
) -> PartitionProcessorExit
where
    R: ProjectionStateReader + ?Sized + 'static,
    C: ProjectionCheckpointStore + ?Sized + 'static,
    D: DispatchOutboxStore
        + TerminalObligationStore
        + engine_core::storage_contract::InstanceSupervisorStore
        + engine_core::storage_contract::TimerWorkStore
        + ?Sized
        + 'static,
{
    info!(partition_id, node_id = %node_id, checkpoint_key = %checkpoint_key, "Partition processor starting");

    // Per-partition labels reused for every metric this loop emits.
    let partition_attrs = [
        opentelemetry::KeyValue::new("partition_id", partition_id as i64),
        opentelemetry::KeyValue::new("node_id", node_id.clone()),
    ];

    // Replay tracking: capture the segment tail at start so we can emit the
    // one-shot replay-duration histogram when we cross it. If no segment is
    // wired or the tail read fails, replay tracking is skipped (no metric).
    let replay_started_at = std::time::Instant::now();
    let replay_start_offset = startup_start_offset.map(|offset| offset.0);
    let replay_target_offset = match startup_replay_target {
        Some(offset) => Some(offset.0),
        None => match segment.as_ref() {
            Some(seg) => match seg.tail_offset().await {
                Ok(t) => Some(t.0),
                Err(_) => None,
            },
            None => None,
        },
    };
    crate::metrics::record_processor_replay_start(
        partition_id,
        &node_id,
        replay_start_offset,
        replay_target_offset,
    );
    let mut replay_complete = match replay_target_offset {
        Some(target) => replay_start_offset
            .map(|start| start >= target)
            .unwrap_or(target == 0),
        None => true, // no target → don't track
    };
    if replay_complete {
        startup_replay_permit.take();
        if let Err(error) = admit_projection_replay(
            partition_id,
            replay_target_offset,
            lag_segment.as_ref(),
            projection_admission.as_ref(),
        )
        .await
        {
            return PartitionProcessorExit::Failed(error.to_string());
        }
    }
    info!(
        partition_id,
        node_id = %node_id,
        checkpoint_key = %checkpoint_key,
        replay_start_offset = replay_start_offset,
        replay_target_offset = replay_target_offset,
        replay_complete,
        "Partition processor replay tracking initialized"
    );
    if replay_complete {
        let state = if replay_target_offset.is_some() {
            "initial_ready"
        } else {
            "tracking_disabled"
        };
        crate::metrics::record_processor_replay_ready(partition_id, &node_id, state, 0.0);
        info!(
            partition_id,
            node_id = %node_id,
            replay_ready_state = state,
            "Partition processor replay ready"
        );
    }
    if let Some(reporter) = health_reporter.as_ref() {
        reporter.mark_running(replay_complete);
    }
    let (replay_ready_tx, replay_ready_rx) = tokio::sync::watch::channel(replay_complete);
    let committer_owns_checkpoint = checkpoint_key == format!("processor-partition-{partition_id}");
    let dispatch_outbox_notify = Arc::new(tokio::sync::Notify::new());

    let mut dispatch_drainer = if let (Some(tx), Some(seg)) = (dispatch_tx.clone(), segment.clone())
    {
        let store_for_drainer = dispatch_outbox_store.clone();
        let cache_for_drainer = cache.clone();
        let cancel_for_drainer = cancel.child_token();
        let cancel_for_task = cancel_for_drainer.clone();
        let node_id_for_drainer = node_id.clone();
        let is_leader_rx_for_drainer = is_leader_rx.clone().unwrap_or_else(|| {
            let (_tx, rx) = tokio::sync::watch::channel(true);
            rx
        });
        let replay_ready_rx_for_drainer = replay_ready_rx.clone();
        let dispatch_outbox_notify_for_drainer = dispatch_outbox_notify.clone();
        let handle = tokio::spawn(async move {
            crate::dispatch_outbox_drain::run_partition_dispatch_outbox_drainer(
                partition_id,
                node_id_for_drainer,
                store_for_drainer,
                tx,
                seg,
                cache_for_drainer,
                is_leader_rx_for_drainer,
                replay_ready_rx_for_drainer,
                dispatch_outbox_notify_for_drainer,
                cancel_for_task,
            )
            .await;
        });
        Some(AttemptBackgroundTask {
            cancel: cancel_for_drainer,
            handle: Some(handle),
        })
    } else {
        None
    };

    // Woken after a record leaves an always-on agent instance needing its
    // leader's decision; its sweep repairs a missed wake.
    let instance_supervisor_wake = Arc::new(tokio::sync::Notify::new());

    // Keep terminal recovery in the same supervised processor attempt as the
    // projection and dispatch drainer. It cannot act until replay is complete,
    // and it is cancelled before a failed attempt relinquishes ownership. The
    // instance supervisor and the timer poller share its task, so they are
    // gated, cancelled and failed the same way.
    let mut terminal_supervisor = if let Some(seg) = segment.clone() {
        let store_for_supervisor = dispatch_outbox_store.clone();
        let store_for_instances = dispatch_outbox_store.clone();
        let seg_for_instances = seg.clone();
        let node_id_for_instances = node_id.clone();
        let replay_ready_for_instances = replay_ready_rx.clone();
        let wake_for_instances = instance_supervisor_wake.clone();
        let cancel_for_supervisor = cancel.child_token();
        let cancel_for_task = cancel_for_supervisor.clone();
        let node_id_for_supervisor = node_id.clone();
        let is_leader_rx_for_supervisor = is_leader_rx.clone().unwrap_or_else(|| {
            let (_tx, rx) = tokio::sync::watch::channel(true);
            rx
        });
        let is_leader_for_instances = is_leader_rx_for_supervisor.clone();
        let cancel_for_instances = cancel_for_task.clone();
        let store_for_timers = dispatch_outbox_store.clone();
        let seg_for_timers = seg.clone();
        let node_id_for_timers = node_id.clone();
        let replay_ready_for_timers = replay_ready_rx.clone();
        let is_leader_for_timers = is_leader_rx_for_supervisor.clone();
        let cancel_for_timers = cancel_for_task.clone();
        let handle = tokio::spawn(async move {
            tokio::join!(
                crate::terminal_supervisor::run_partition_terminal_supervisor(
                    partition_id,
                    node_id_for_supervisor,
                    store_for_supervisor,
                    seg,
                    replay_ready_rx,
                    is_leader_rx_for_supervisor,
                    cancel_for_task,
                    crate::terminal_supervisor::TerminalSupervisorConfig::default(),
                ),
                crate::instance_supervisor::run_partition_instance_supervisor(
                    partition_id,
                    node_id_for_instances,
                    store_for_instances,
                    seg_for_instances,
                    replay_ready_for_instances,
                    is_leader_for_instances,
                    wake_for_instances,
                    cancel_for_instances,
                    crate::instance_supervisor::InstanceSupervisorConfig::default(),
                ),
                // Only this partition's leader fires its timers (ADR-006).
                crate::timer_poller::run_partition_timer_poller(
                    partition_id,
                    node_id_for_timers,
                    store_for_timers,
                    seg_for_timers,
                    replay_ready_for_timers,
                    is_leader_for_timers,
                    cancel_for_timers,
                    crate::timer_poller::TimerPollerConfig::default(),
                ),
            );
        });
        Some(AttemptBackgroundTask {
            cancel: cancel_for_supervisor,
            handle: Some(handle),
        })
    } else {
        None
    };

    let mut records_since_checkpoint: u64 = 0;
    let mut last_offset = Offset::ZERO;
    // Lag is sampled on a timer from the partition's own segment, whose
    // offsets are local, so track the next local offset to apply and the
    // timestamp of the newest applied record.
    let mut next_local_offset = startup_start_offset.map_or(0, |offset| offset.0);
    let mut last_applied_timestamp_ns: Option<i64> = None;
    let mut lag_sample_ticker = tokio::time::interval(processor_lag::SAMPLE_INTERVAL);
    lag_sample_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                if records_since_checkpoint > 0 {
                    if let Err(e) = checkpoint_store
                        .store_projection_checkpoint(&checkpoint_key, last_offset)
                        .await
                    {
                        error!(error = %e, key = %checkpoint_key, "Failed to write final checkpoint");
                    } else {
                        info!(offset = %last_offset, key = %checkpoint_key, "Final checkpoint saved");
                        if let Some(reporter) = health_reporter.as_ref() {
                            let (_, local) =
                                engine_core::partition_offset::decode_offset(last_offset);
                            reporter.mark_checkpoint(local);
                        }
                    }
                }
                info!(key = %checkpoint_key, "Partition processor shutting down");
                return finish_partition_processor_attempt(
                    &mut dispatch_drainer,
                    &mut terminal_supervisor,
                    PartitionProcessorExit::Cancelled,
                )
                .await;
            }
            result = wait_for_attempt_background_task(&mut dispatch_drainer) => {
                let _ = dispatch_drainer.take();
                let error = match result {
                    Ok(()) => "dispatch outbox drainer exited unexpectedly".to_string(),
                    Err(error) if error.is_panic() => {
                        format!("dispatch outbox drainer panicked: {error}")
                    }
                    Err(error) => format!("dispatch outbox drainer task failed: {error}"),
                };
                error!(
                    partition_id,
                    node_id = %node_id,
                    error = %error,
                    "Partition processor attempt lost its dispatch outbox drainer"
                );
                if let Some(task) = terminal_supervisor.take() {
                    task.shutdown().await;
                }
                return PartitionProcessorExit::Failed(error);
            }
            result = wait_for_attempt_background_task(&mut terminal_supervisor) => {
                let _ = terminal_supervisor.take();
                let error = match result {
                    Ok(()) => "terminal obligation supervisor exited unexpectedly".to_string(),
                    Err(error) if error.is_panic() => {
                        format!("terminal obligation supervisor panicked: {error}")
                    }
                    Err(error) => format!("terminal obligation supervisor task failed: {error}"),
                };
                error!(
                    partition_id,
                    node_id = %node_id,
                    error = %error,
                    "Partition processor attempt lost its terminal obligation supervisor"
                );
                if let Some(task) = dispatch_drainer.take() {
                    task.shutdown().await;
                }
                return PartitionProcessorExit::Failed(error);
            }
            // Polled before the record stream so a busy replay still samples
            // once per interval.
            _ = lag_sample_ticker.tick(), if lag_segment.is_some() => {
                if let Some(seg) = lag_segment.as_ref() {
                    processor_lag::record_lag_sample(
                        seg,
                        next_local_offset,
                        last_applied_timestamp_ns,
                        &partition_attrs,
                    )
                    .await;
                }
            }
            item = stream.next() => {
                match item {
                    Some(Ok(record)) => {
                        let (_, record_local_offset) =
                            engine_core::partition_offset::decode_offset(record.offset);
                        // Startup replay must stay projection-only. External
                        // side effects (cache warmup, dispatch-drain appends,
                        // coordinator notifications, analytics) resume only
                        // for records that arrive after the tail captured at
                        // processor start. Durable dispatch outbox rows are
                        // projection state and may be written during replay;
                        // the drainer is separately gated on replay readiness.
                        // Partition streams carry encoded offsets, while
                        // replay targets are local per-partition tails, so
                        // compare against the decoded local offset.
                        let side_effects_enabled = replay_complete
                            || matches!(
                                replay_target_offset,
                                Some(target) if record_local_offset >= target
                            );

                        // Stamp the per-node lag clock
                        // with the just-applied record's wall-clock
                        // timestamp. The runs list reads this on every
                        // RPC and reports `now − last_applied_ts` as
                        // `replication_lag_ms`. Convert ns → ms.
                        if let Some(ref clock) = lag_clock {
                            clock.record_applied(record.timestamp_ns / 1_000_000);
                        }

                        // In cluster mode, only the active leader drains the
                        // dispatch outbox and writes orchestrator records
                        // (run.assigned, retries). Followers still materialize
                        // durable outbox/projection state so a new owner can
                        // pick up pending dispatch work after handoff.
                        let is_leader = is_leader_rx
                            .as_ref()
                            .map(|rx| *rx.borrow())
                            .unwrap_or(true);
                        let active_leader = side_effects_enabled && is_leader;
                        let dispatch_outbox_enabled = dispatch_tx.is_some() && segment.is_some();
                        let eff_segment = if active_leader {
                            segment.as_deref()
                        } else {
                            None
                        };

                        let enriched_record = pricing_cache
                            .as_ref()
                            .and_then(|cache| cache.enrich_record(&record));
                        let record_ref = enriched_record.as_ref().unwrap_or(&record);

                        let advance_checkpoint = committer_owns_checkpoint
                            && records_since_checkpoint.saturating_add(1) >= checkpoint_interval;
                        let processed = match process_record_standalone_with_checkpoint(
                            &record,
                            enriched_record.as_ref(),
                            partition_id,
                            &node_id,
                            &*projection_state,
                            dispatch_outbox_enabled,
                            eff_segment,
                            advance_checkpoint,
                            projection_committer.as_ref(),
                        ).await {
                            Ok(processed) => processed,
                            Err(e) => {
                                if matches!(&e, EngineError::Activation(_)) {
                                    // The projection boundary already captured
                                    // this divergence explicitly with a stable
                                    // Sentry fingerprint and aggregate state.
                                    warn!(
                                        error = %e,
                                        key = %checkpoint_key,
                                        partition_id,
                                        record_offset = record_ref.offset.0,
                                        record_local_offset,
                                        applied_checkpoint = last_offset.0,
                                        replay_target_offset,
                                        event_type = %record_ref.event_type,
                                        "Partition processor stopped after captured activation divergence"
                                    );
                                } else {
                                    error!(
                                        error = %e,
                                        key = %checkpoint_key,
                                        partition_id,
                                        record_offset = record_ref.offset.0,
                                        record_local_offset,
                                        applied_checkpoint = last_offset.0,
                                        replay_target_offset,
                                        event_type = %record_ref.event_type,
                                        "Partition processor: error processing record — stopping"
                                    );
                                }
                                // Stop rather than silently skipping a record. No
                                // record is skipped; the supervisor reopens the
                                // partition tail from its durable checkpoint.
                                return finish_partition_processor_attempt(
                                    &mut dispatch_drainer,
                                    &mut terminal_supervisor,
                                    PartitionProcessorExit::Failed(format!(
                                        "process {} for run {}: {e}",
                                        record_ref.event_type, record_ref.run_id
                                    )),
                                )
                                .await;
                            }
                        };

                        // Wake only after the atomic projection commit has
                        // made the durable row visible. Notify retains a
                        // permit if the drainer is between scans; its timer
                        // remains the repair path for missed process-local
                        // signals and rows recovered after restart.
                        if processed.dispatch_outbox_inserted {
                            dispatch_outbox_notify.notify_one();
                        }
                        if processed.instance_obligation_put {
                            instance_supervisor_wake.notify_one();
                        }

                        // The request-local append path can only wake parked
                        // polls on the ingress node. Emit again after the
                        // durable projection is visible on the partition
                        // owner, where the HA assignment gate permits the
                        // immediate claim. Followers retain the 1s periodic
                        // probe as the worker-placement repair path.
                        if active_leader && processed.cache_journal_record {
                            if let Some(tx) = demand_dirty_tx.as_ref() {
                                for demand_key in &processed.dirty_demand_keys {
                                    notify_demand_key_dirty(
                                        tx,
                                        DemandKeyDirtyEvent {
                                            partition_id,
                                            demand_key: demand_key.clone(),
                                            removed_pending_run: processed.removed_pending_run
                                                .as_ref()
                                                .filter(|(key, _)| key == demand_key)
                                                .map(|(_, removed)| removed.clone()),
                                        },
                                        &node_id,
                                    );
                                }
                            }
                            if let (Some(tx), Some(event)) =
                                (
                                    pull_ready_tx.as_ref(),
                                    pull_ready_event(
                                        record_ref,
                                        partition_id,
                                        std::time::Instant::now(),
                                    ),
                                )
                            {
                                tracing::debug!(
                                    target: "agnt5.pull_queue_stage",
                                    stage = "projection_committed",
                                    run_id = %event.run_id,
                                    ready_at_ms = event.ready_at_ms,
                                    partition_id,
                                    node_id,
                                    "PULL queue stage"
                                );
                                notify_pull_ready_event(tx, event, partition_id, &node_id);
                            }
                        }

                        // Cache only after projection decides whether this
                        // journal record is externally visible. In particular,
                        // a forged or losing fenced canonical record must not
                        // terminate a per-run cache tail.
                        if side_effects_enabled && processed.cache_journal_record {
                            if let Some(ref c) = cache {
                                c.put(std::sync::Arc::new(record.clone()));
                                engine_cache::RecordCache::mark_terminal_if_complete(c, &record);
                            }
                        }

                        // Notify the local coordinator on terminal run events
                        // for load tracking. In cluster mode, the partition
                        // leader and worker-owning coordinator can be different
                        // nodes for peer-forwarded dispatches, so every node
                        // that replays the terminal event gives its local
                        // coordinator a chance to clear a matching
                        // active_dispatch entry. Non-owners ignore it.
                        if side_effects_enabled && processed.terminal_event_accepted && matches!(
                            record_ref.event_type.as_str(),
                            "run.completed" | "run.failed" | "run.cancelled" | "run.paused" | "workflow.paused"
                        ) {
                            if let Some(ref tx) = terminal_tx {
                                notify_terminal_event(
                                    tx,
                                    RunTerminalEvent {
                                        run_id: record_ref.run_id.clone(),
                                        event_type: record_ref.event_type.clone(),
                                        lease_id: processed
                                            .terminal_lease_id
                                            .clone()
                                            .unwrap_or_default(),
                                    },
                                    Some(partition_id),
                                    Some(&node_id),
                                );
                            } else {
                                tracing::warn!(
                                    run_id = %record_ref.run_id,
                                    event_type = %record_ref.event_type,
                                    "Terminal event but no terminal_tx channel"
                                );
                            }
                        }

                        let mut run_for_lifecycle_metrics = None;
                        if side_effects_enabled
                            && processed.terminal_event_accepted
                            && is_terminal_event(record_ref)
                        {
                            match ProjectionStateReader::load_projection_run(
                                projection_state.as_ref(),
                                &record_ref.project_id,
                                &record_ref.run_id,
                            )
                            .await
                            {
                                Ok(Some(run_data)) => {
                                    if let Ok(run) =
                                        <engine_proto::Run as prost::Message>::decode(
                                            run_data.as_slice(),
                                        )
                                    {
                                        crate::metrics::record_run_terminal_duration(
                                            &run,
                                            terminal_stage(&record_ref.event_type),
                                            partition_id,
                                        );
                                        run_for_lifecycle_metrics = Some(run);
                                    }
                                    if let Some(reporter) = analytics_reporter.as_ref() {
                                        reporter
                                            .run_finished(&run_data, &record_ref.project_id)
                                            .await;
                                    }
                                }
                                Ok(None) => {
                                    tracing::warn!(
                                        run_id = %record_ref.run_id,
                                        event_type = %record_ref.event_type,
                                        "Terminal run missing from materialized store after projection"
                                    );
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        run_id = %record_ref.run_id,
                                        event_type = %record_ref.event_type,
                                        error = %e,
                                        "Failed to read terminal run for durable duration metric"
                                    );
                                }
                            }
                        }

                        // Buffer LLM events for control plane analytics
                        if side_effects_enabled {
                            if let Some(ref reporter) = analytics_reporter {
                                if let Some(event) = processed.llm_event.clone() {
                                    reporter.model_call(event).await;
                                }
                            }
                        }

                        last_offset = record.offset;
                        records_since_checkpoint += 1;
                        next_local_offset = record_local_offset.saturating_add(1);
                        last_applied_timestamp_ns = Some(record.timestamp_ns);

                        // `records` is the throughput counter. The lag
                        // gauges are sampled on `lag_sample_ticker`.
                        let m = crate::metrics::processor_metrics();
                        m.processor_records.add(1, &partition_attrs);
                        if should_record_run_lifecycle_event(record_ref, &processed) {
                            crate::metrics::record_run_lifecycle_event(
                                record_ref,
                                run_for_lifecycle_metrics.as_ref(),
                                partition_id,
                            );
                        }

                        // Replay-duration one-shot: fire the moment we
                        // first cross the segment tail captured at startup.
                        if !replay_complete {
                            if let Some(target) = replay_target_offset {
                                if record_local_offset.saturating_add(1) >= target {
                                    startup_replay_permit.take();
                                    if let Err(error) = admit_projection_replay(
                                        partition_id,
                                        Some(target),
                                        lag_segment.as_ref(),
                                        projection_admission.as_ref(),
                                    )
                                    .await
                                    {
                                        return finish_partition_processor_attempt(
                                            &mut dispatch_drainer,
                                            &mut terminal_supervisor,
                                            PartitionProcessorExit::Failed(error.to_string()),
                                        )
                                        .await;
                                    }
                                    let elapsed_ms =
                                        replay_started_at.elapsed().as_secs_f64() * 1000.0;
                                    crate::metrics::record_processor_replay_ready(
                                        partition_id,
                                        &node_id,
                                        "target_reached",
                                        elapsed_ms,
                                    );
                                    info!(
                                        partition_id,
                                        node_id = %node_id,
                                        replay_target_offset = target,
                                        last_replay_offset = record_local_offset,
                                        replay_duration_ms = elapsed_ms,
                                        "Partition processor replay ready"
                                    );
                                    replay_complete = true;
                                    let _ = replay_ready_tx.send(true);
                                    if let Some(reporter) = health_reporter.as_ref() {
                                        reporter.mark_replay_complete();
                                    }
                                }
                            }
                        }

                        if records_since_checkpoint >= checkpoint_interval {
                            if !committer_owns_checkpoint {
                                if let Err(e) = checkpoint_store
                                    .store_projection_checkpoint(&checkpoint_key, last_offset)
                                    .await
                                {
                                    error!(error = %e, key = %checkpoint_key, "Checkpoint write failed");
                                    return finish_partition_processor_attempt(
                                        &mut dispatch_drainer,
                                        &mut terminal_supervisor,
                                        PartitionProcessorExit::Failed(format!(
                                            "write {checkpoint_key} checkpoint at {}: {e}",
                                            last_offset.0
                                        )),
                                    )
                                    .await;
                                }
                            }
                            debug!(offset = %last_offset, key = %checkpoint_key, "Checkpoint saved");
                            if let Some(reporter) = health_reporter.as_ref() {
                                reporter.mark_checkpoint(record_local_offset);
                            }
                            records_since_checkpoint = 0;
                        }
                    }
                    Some(Err(e)) => {
                        error!(error = %e, key = %checkpoint_key, "Partition stream error");
                        return finish_partition_processor_attempt(
                            &mut dispatch_drainer,
                            &mut terminal_supervisor,
                            PartitionProcessorExit::Failed(format!(
                                "read {checkpoint_key} tail: {e}"
                            )),
                        )
                        .await;
                    }
                    None => {
                        info!(key = %checkpoint_key, "Partition stream ended");
                        if records_since_checkpoint > 0 {
                            if let Err(e) = checkpoint_store
                                .store_projection_checkpoint(&checkpoint_key, last_offset)
                                .await
                            {
                                return finish_partition_processor_attempt(
                                    &mut dispatch_drainer,
                                    &mut terminal_supervisor,
                                    PartitionProcessorExit::Failed(format!(
                                        "write final {checkpoint_key} checkpoint at {}: {e}",
                                        last_offset.0
                                    )),
                                )
                                .await;
                            }
                            if let Some(reporter) = health_reporter.as_ref() {
                                let (_, local) =
                                    engine_core::partition_offset::decode_offset(last_offset);
                                reporter.mark_checkpoint(local);
                            }
                        }
                        return finish_partition_processor_attempt(
                            &mut dispatch_drainer,
                            &mut terminal_supervisor,
                            PartitionProcessorExit::StreamEnded,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

///// Append a record to the segment and optionally write through to the cache.
async fn append_and_cache(
    seg: &dyn Segment,
    record: Record,
    cache: Option<&Arc<engine_cache::RecordCache>>,
) -> EngineResult<Offset> {
    let outcome = seg
        .append_batch_with_outcome(std::slice::from_ref(&record))
        .await?;
    outcome.validate_cardinality(1, "append and cache")?;
    let offset = outcome.offsets[0];
    if let Some(cache) = cache {
        if outcome.appended[0] {
            let mut cached = record;
            cached.offset = offset;
            let cached = Arc::new(cached);
            cache.put(Arc::clone(&cached));
            engine_cache::RecordCache::mark_terminal_if_complete(cache, cached.as_ref());
        }
    }
    Ok(offset)
}

///// Append a single processor-authored side effect to the segment.
///
/// Do not write through to the cache here. This helper runs while offset N is
/// still being projected and commonly appends N+1; caching N+1 now would put it
/// ahead of N for live Tail readers. The partition loop observes the appended
/// record next and caches it only after projection accepts it.
/// Returns the assigned offset on success.
async fn append_processor_side_effect(seg: &dyn Segment, record: Record) -> EngineResult<Offset> {
    append_and_cache(seg, record, None).await
}

fn build_timer_resume_record(
    fired_record: &Record,
    existing_run: Option<&[u8]>,
    existing_timer: Option<&[u8]>,
) -> Option<Record> {
    if fired_record.event_type != "timer.fired" {
        return None;
    }

    let run = Run::decode(existing_run?).ok()?;
    if run.status != "paused" {
        return None;
    }
    let kept_continuation = run
        .metadata
        .get(engine_core::journal_contract::META_CONTINUATION_B64)
        .cloned();

    let timer = Timer::decode(existing_timer?).ok()?;
    // The first attempt sees "scheduled"; replay after the timer.fired
    // projection committed sees "fired". Both refer to this same durable
    // timer generation, and the resume-source idempotency key makes rebuilding
    // the side effect safe.
    if !matches!(timer.status.as_str(), "scheduled" | "fired") {
        return None;
    }

    let schedule_payload =
        serde_json::from_slice::<serde_json::Value>(&timer.schedule_payload).ok()?;
    let resume_metadata = schedule_payload
        .get("resume_metadata")
        .and_then(serde_json::Value::as_object)?;

    let mut metadata = run.metadata.clone();
    metadata.extend(json_string_map(resume_metadata));
    let timer_resume_source = resume_source_id(&metadata)?;
    let current_pause_source = resume_source_id(&run.metadata)?;
    if timer_resume_source != current_pause_source {
        warn!(
            run_id = %fired_record.run_id,
            timer_id = %timer.timer_id,
            timer_resume_source,
            current_pause_source,
            "Ignoring timer resume for a stale pause generation"
        );
        return None;
    }
    strip_stale_resume_metadata(&mut metadata);
    metadata.insert(META_RESUME_SOURCE_ID.to_string(), timer_resume_source);
    if !run.component_name.is_empty() {
        metadata
            .entry("component_name".to_string())
            .or_insert(run.component_name);
    }
    if !run.component_type.is_empty() {
        metadata
            .entry("component_type".to_string())
            .or_insert(run.component_type);
    }
    if !run.deployment_id.is_empty() {
        metadata
            .entry("deployment_id".to_string())
            .or_insert(run.deployment_id);
    }
    metadata.insert("timer_id".to_string(), timer.timer_id.clone());
    if !timer.timer_key.is_empty() {
        metadata.insert("timer_key".to_string(), timer.timer_key.clone());
        metadata.insert("workerless_timer_key".to_string(), timer.timer_key.clone());
    }
    if let Some(value) = fired_record.metadata.get("workerless_ready_at_ms") {
        metadata.insert("workerless_ready_at_ms".to_string(), value.clone());
    }
    metadata.insert("workerless_timer_id".to_string(), timer.timer_id);
    metadata.insert("workerless_resume_reason".to_string(), "timer".to_string());

    let mut resume = Record {
        offset: Offset::ZERO,
        project_id: fired_record.project_id.clone(),
        run_id: fired_record.run_id.clone(),
        event_type: "run.resumed".to_string(),
        data: Bytes::from(run.input_data),
        timestamp_ns: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        step_key: None,
        correlation_id: Some(format!(
            "timer-resume-{}",
            fired_record.run_id.chars().take(8).collect::<String>()
        )),
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    // A large continuation in metadata would get this record rejected, and a
    // failed timer resume is never retried. The run row already holds the
    // continuation from this timer's `timer.scheduled`, and the resumed
    // dispatch reads it from there.
    if engine_core::journal_contract::shed_oversized_continuation(
        &mut resume,
        kept_continuation.as_deref(),
    ) {
        debug!(
            run_id = %fired_record.run_id,
            "Timer resume leaves its continuation on the run row to fit the journal limit"
        );
    }
    Some(resume)
}

fn json_string_map(object: &serde_json::Map<String, serde_json::Value>) -> HashMap<String, String> {
    object
        .iter()
        .filter_map(|(key, value)| {
            value
                .as_str()
                .map(|string| (key.clone(), string.to_string()))
        })
        .collect()
}

/// Apply one finite committed journal range to a disposable projection.
///
/// The range is captured by the recovery assessment and never follows a live
/// tail. This function has no journal-writer or external-effect interface, so
/// it cannot append canonical records, dispatch work, fire timers, publish
/// analytics, or transition into live processing. Each record and its exact
/// applied frontier commit atomically through `projection_committer`, making a
/// partially completed work directory safe to resume from that frontier.
pub async fn replay_projection_offline<R: ProjectionStateReader + ?Sized>(
    journal: &dyn Segment,
    store: &R,
    projection_committer: &dyn ProjectionCommitter,
    request: OfflineProjectionReplayRequest,
    cancel: &CancellationToken,
) -> EngineResult<OfflineProjectionReplayResult> {
    if request.partition_count == 0
        || request.partition_count > engine_core::partition_offset::MAX_PARTITION_COUNT
    {
        return Err(EngineError::InvalidArgument(format!(
            "offline replay partition_count {} is outside 1..={}",
            request.partition_count,
            engine_core::partition_offset::MAX_PARTITION_COUNT
        )));
    }
    if request.partition_id as usize >= request.partition_count {
        return Err(EngineError::InvalidArgument(format!(
            "offline replay partition {} is outside partition_count {}",
            request.partition_id, request.partition_count
        )));
    }
    if request.from > request.through {
        return Err(EngineError::InvalidArgument(format!(
            "offline replay range [{}..{}) is reversed",
            request.from, request.through
        )));
    }
    if request.page_records == 0 {
        return Err(EngineError::InvalidArgument(
            "offline replay page_records must be greater than zero".to_string(),
        ));
    }
    let max_exclusive = engine_core::partition_offset::MAX_LOCAL_OFFSET
        .checked_add(1)
        .expect("encoded local offset domain has an exclusive frontier");
    if request.through > max_exclusive {
        return Err(EngineError::InvalidArgument(format!(
            "offline replay frontier {} exceeds the encoded local offset domain",
            request.through
        )));
    }

    let retained_floor = journal.retained_floor().await?.0;
    let available_through = journal.tail_offset().await?.0;
    if request.from < retained_floor {
        return Err(EngineError::Gap {
            expected: request.from,
            found: Some(retained_floor),
        });
    }
    if request.through > available_through {
        return Err(EngineError::Gap {
            expected: request.through,
            found: Some(available_through),
        });
    }

    let page_records = u64::try_from(request.page_records).map_err(|_| {
        EngineError::InvalidArgument("offline replay page_records does not fit u64".to_string())
    })?;
    let mut cursor = request.from;
    let mut records_applied = 0u64;
    while cursor < request.through {
        if cancel.is_cancelled() {
            return Err(EngineError::Storage(format!(
                "offline projection replay cancelled at partition {} frontier {cursor}",
                request.partition_id
            )));
        }
        let page_through = cursor.saturating_add(page_records).min(request.through);
        let records = journal
            .read_range(Offset(cursor), Offset(page_through))
            .await?;
        let expected_count = usize::try_from(page_through - cursor).map_err(|_| {
            EngineError::Storage("offline replay page length does not fit usize".to_string())
        })?;
        if records.len() != expected_count {
            return Err(EngineError::Gap {
                expected: cursor.saturating_add(records.len() as u64),
                found: None,
            });
        }
        for mut record in records {
            if record.offset.0 != cursor {
                return Err(EngineError::Gap {
                    expected: cursor,
                    found: Some(record.offset.0),
                });
            }
            record.offset = engine_core::partition_offset::encode_offset(
                request.partition_id as usize,
                cursor,
            )?;
            process_record_standalone_with_checkpoint(
                &record,
                None,
                request.partition_id,
                "offline-recovery",
                store,
                false,
                None,
                true,
                projection_committer,
            )
            .await?;
            cursor += 1;
            records_applied += 1;
        }
    }

    Ok(OfflineProjectionReplayResult {
        partition_id: request.partition_id,
        from: request.from,
        through: request.through,
        records_applied,
    })
}

/// Process a single record: apply all projections and write to store.
/// Standalone version that takes only the projection-state read port.
///
/// When processor orchestration is enabled, `dispatch_tx` and `segment` are
/// used to dispatch runs and write lifecycle events.
#[cfg_attr(not(test), allow(dead_code))]
#[allow(clippy::too_many_arguments)]
pub async fn process_record_standalone<R: ProjectionStateReader + ?Sized>(
    journal_record: &Record,
    partition_id: u32,
    _node_id: &str,
    store: &R,
    dispatch_outbox_enabled: bool,
    side_effect_segment: Option<&dyn Segment>,
    projection_committer: &dyn ProjectionCommitter,
) -> EngineResult<ProcessedRecord> {
    process_record_standalone_with_checkpoint(
        journal_record,
        None,
        partition_id,
        _node_id,
        store,
        dispatch_outbox_enabled,
        side_effect_segment,
        false,
        projection_committer,
    )
    .await
}

/// [`process_record_standalone`], also advancing the projection checkpoint
/// with the commit.
#[allow(clippy::too_many_arguments)]
pub async fn process_record_standalone_with_checkpoint<R: ProjectionStateReader + ?Sized>(
    journal_record: &Record,
    projection_record: Option<&Record>,
    partition_id: u32,
    _node_id: &str,
    store: &R,
    dispatch_outbox_enabled: bool,
    side_effect_segment: Option<&dyn Segment>,
    advance_checkpoint: bool,
    projection_committer: &dyn ProjectionCommitter,
) -> EngineResult<ProcessedRecord> {
    let read_existing_start = std::time::Instant::now();
    let existing_run = ProjectionStateReader::load_projection_run(
        store,
        &journal_record.project_id,
        &journal_record.run_id,
    )
    .await?;
    let projection_activation_id = activations::activation_id(journal_record).map(str::to_string);
    let existing_activation = if let Some(activation_id) = projection_activation_id.as_deref() {
        ProjectionStateReader::load_projection_activation(
            store,
            &journal_record.project_id,
            &journal_record.run_id,
            activation_id,
        )
        .await?
    } else {
        None
    };

    let record = projection_record.unwrap_or(journal_record);

    let existing_approval = if record.event_type == "approval.resolved" {
        if let Some(aid) = record.metadata.get("approval_id") {
            ProjectionStateReader::load_projection_approval(store, &record.project_id, aid).await?
        } else {
            None
        }
    } else {
        None
    };

    let existing_signal = if record.event_type == "signal.acked" {
        if let Some(sid) = record.metadata.get("signal_id") {
            ProjectionStateReader::load_projection_signal(store, &record.project_id, sid).await?
        } else {
            None
        }
    } else {
        None
    };

    let existing_timer = if matches!(
        record.event_type.as_str(),
        "timer.fired" | "timer.canceled" | "timer.expired"
    ) {
        if let Some(tid) = record.metadata.get("timer_id") {
            ProjectionStateReader::load_projection_timer(store, &record.project_id, tid).await?
        } else {
            None
        }
    } else {
        None
    };

    let existing_job = if matches!(
        record.event_type.as_str(),
        "job.claimed" | "job.started" | "job.completed" | "job.failed" | "job.cancelled"
    ) {
        if let Some(jid) = record.metadata.get("job_id") {
            ProjectionStateReader::load_projection_job(store, &record.project_id, jid).await?
        } else {
            None
        }
    } else {
        None
    };

    let existing_batch = if matches!(
        record.event_type.as_str(),
        "batch.started"
            | "batch.item_completed"
            | "batch.item_failed"
            | "batch.completed"
            | "batch.cancelled"
    ) {
        if let Some(bid) = record.metadata.get("batch_id") {
            ProjectionStateReader::load_projection_batch(store, &record.project_id, bid).await?
        } else {
            None
        }
    } else {
        None
    };

    let existing_session = if record.event_type == "session.updated" {
        if let Some(sid) = record.metadata.get("session_id") {
            ProjectionStateReader::load_projection_session(store, &record.project_id, sid).await?
        } else {
            None
        }
    } else {
        None
    };

    let existing_message = if record.event_type == "message.acked" {
        if let Some(mid) = record.metadata.get("message_id") {
            ProjectionStateReader::load_projection_message(store, &record.project_id, mid).await?
        } else {
            None
        }
    } else {
        None
    };
    crate::metrics::record_processor_stage_duration(
        &record.event_type,
        "read_existing",
        read_existing_start,
    );

    // A supervisor's start for an always-on agent instance takes effect only if
    // the instance can still take it as the record is applied. A refused start
    // is a no-op for the run: no projection, no dispatch.
    let instance_start = crate::projections::instances::check_decision_on_apply(
        store,
        record,
        partition_id,
        existing_run.as_deref(),
    )
    .await?;
    let instance_start_refused = instance_start
        .as_ref()
        .is_some_and(|applied| !applied.accepted());

    // Orchestrator decisions use pre-projection run state for replay-safe
    // deduplication. Dispatch decisions are materialized as durable outbox
    // rows; retry decisions still append a new run.queued from the leader.
    let orchestration_action = if instance_start_refused {
        None
    } else if dispatch_outbox_enabled || side_effect_segment.is_some() {
        let action = crate::orchestrator::evaluate(record, existing_run.as_deref());
        crate::orchestrator::log_action(&action);
        Some(action)
    } else {
        None
    };

    // worker.registered/heartbeat/deregistered no longer
    // produce projection ops — gossip is the routing source of record.

    use crate::metrics::{
        record_processor_op_counts, record_processor_stage_duration, time_projection,
    };
    let project_start = std::time::Instant::now();
    let run_result = if instance_start_refused {
        crate::projections::ProjectionResult { ops: Vec::new() }
    } else {
        time_projection("runs", || runs::project(record, existing_run.as_deref()))
    };
    let activation_result = time_projection("activations", || {
        activations::project(record, existing_activation.as_deref())
    })?;
    let mut state_ops = time_projection("state", || state::project(record));
    state_ops.extend(runtime_event_capture(store, record, partition_id, &run_result).await?);
    if let Some(work) = engine_core::runtime_work::RuntimeWork::from_record(record)? {
        state_ops.push(StoreOp::PutRuntimeWork(work));
    }
    let approval_ops = time_projection("approvals", || {
        approvals::project(record, existing_approval.as_deref())
    });
    let signal_ops = time_projection("signals", || {
        signals::project(record, existing_signal.as_deref())
    });
    let timer_ops = time_projection("timers", || {
        timers::project(record, existing_timer.as_deref())
    });
    let job_ops = time_projection("jobs", || jobs::project(record, existing_job.as_deref()));
    let batch_ops = time_projection("batches", || {
        batches::project(record, existing_batch.as_deref())
    });
    let session_ops = time_projection("sessions", || {
        sessions::project(record, existing_session.as_deref())
    });
    let message_ops = time_projection("messages", || {
        messages::project(record, existing_message.as_deref())
    });
    let event_ops = time_projection("events", || events::project(record));
    // The instance's record and obligation commit with the record that
    // changes them: an item, a stop or lift, or an instance-owned run ending.
    let mut instance_ops = crate::projections::instances::project_record(
        store,
        record,
        partition_id,
        existing_run.as_deref(),
        &run_result,
    )
    .await?;
    if let Some(applied) = instance_start {
        instance_ops.extend(applied.ops);
    }
    let instance_obligation_put = instance_ops
        .iter()
        .any(|op| matches!(op, StoreOp::PutInstanceObligation { .. }));
    record_processor_stage_duration(&record.event_type, "project", project_start);

    // Every projection op for this record commits in one atomic RocksDB
    // WriteBatch. The durable outbox is part of the same batch;
    // entry on top — without atomicity, the outbox can be lost between
    // the Run write and the outbox write on a crash.
    let encode_start = std::time::Instant::now();
    let mut orchestration_ops = Vec::new();
    if let Some(crate::orchestrator::OrchestratorAction::Dispatch { .. }) =
        orchestration_action.as_ref()
    {
        // Pull-dispatched runs are leased by PollJob from the pending index.
        // Unknown/mixed routes still get the push outbox compatibility path.
        if dispatch_outbox_enabled && !pull_dispatch_mode_requested(record, existing_run.as_deref())
        {
            let (_, queued_offset) = engine_core::partition_offset::decode_offset(record.offset);
            let mut outbox_record = crate::orchestrator::hydrate_dispatch_source_record(
                record,
                existing_run.as_deref(),
            );
            outbox_record.metadata.insert(
                DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
                partition_id.to_string(),
            );
            outbox_record.metadata.insert(
                DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
                queued_offset.to_string(),
            );
            let request = crate::orchestrator::build_dispatch_request(&outbox_record);
            orchestration_ops.push(StoreOp::PutDispatchOutbox {
                partition_id,
                queued_offset,
                request_data: crate::dispatch_outbox_drain::encode_dispatch_outbox_payload(
                    &request,
                    &outbox_record,
                )?,
            });
        } else if dispatch_outbox_enabled {
            debug!(
                run_id = %record.run_id,
                event_type = %record.event_type,
                "skipping push dispatch outbox for pull-dispatched run"
            );
        }
    }
    if record.event_type == crate::dispatch_outbox_drain::DISPATCH_RETRY_SCHEDULED_EVENT {
        if let Some((outbox_partition_id, queued_offset)) = dispatch_outbox_metadata(record) {
            // Retry records written before the compact metadata format carried
            // the complete outbox payload. Keep those records replayable.
            let request_data = if !record.data.is_empty() {
                record.data.to_vec()
            } else {
                let existing_run = existing_run.as_deref().ok_or_else(|| {
                    EngineError::Storage(format!(
                        "dispatch retry for {} has no durable Run projection to rehydrate",
                        record.run_id
                    ))
                })?;
                let source_record =
                    crate::orchestrator::hydrate_dispatch_source_record(record, Some(existing_run));
                let request = crate::orchestrator::build_dispatch_request(&source_record);
                let metadata_i64 = |key: &str| -> EngineResult<i64> {
                    record
                        .metadata
                        .get(key)
                        .ok_or_else(|| {
                            EngineError::Storage(format!(
                                "dispatch retry for {} is missing metadata[{key}]",
                                record.run_id
                            ))
                        })?
                        .parse::<i64>()
                        .map_err(|error| {
                            EngineError::Storage(format!(
                                "dispatch retry for {} has invalid metadata[{key}]: {error}",
                                record.run_id
                            ))
                        })
                };
                let retry_attempt = record
                    .metadata
                    .get(crate::dispatch_outbox_drain::DISPATCH_RETRY_ATTEMPT_META)
                    .ok_or_else(|| {
                        EngineError::Storage(format!(
                            "dispatch retry for {} is missing retry attempt",
                            record.run_id
                        ))
                    })?
                    .parse::<u32>()
                    .map_err(|error| {
                        EngineError::Storage(format!(
                            "dispatch retry for {} has invalid retry attempt: {error}",
                            record.run_id
                        ))
                    })?;
                crate::dispatch_outbox_drain::encode_dispatch_outbox_payload_with_schedule(
                    &request,
                    &source_record,
                    metadata_i64(
                        crate::dispatch_outbox_drain::DISPATCH_RETRY_SOURCE_TIMESTAMP_NS_META,
                    )?,
                    metadata_i64(
                        crate::dispatch_outbox_drain::DISPATCH_RETRY_OUTBOX_INSERTED_AT_MS_META,
                    )?,
                    retry_attempt,
                    metadata_i64(
                        crate::dispatch_outbox_drain::DISPATCH_RETRY_NEXT_ATTEMPT_AT_MS_META,
                    )?,
                )?
            };
            orchestration_ops.push(StoreOp::PutDispatchOutbox {
                partition_id: outbox_partition_id,
                queued_offset,
                request_data,
            });
        }
    }
    if record.event_type != crate::dispatch_outbox_drain::DISPATCH_RETRY_SCHEDULED_EVENT {
        if let Some((outbox_partition_id, queued_offset)) = dispatch_outbox_metadata(record) {
            orchestration_ops.push(StoreOp::DeleteDispatchOutbox {
                partition_id: outbox_partition_id,
                project_id: record.project_id.clone(),
                queued_offset,
                run_id: record.run_id.clone(),
            });
        }
    }
    let dispatch_outbox_inserted = orchestration_ops
        .iter()
        .any(|op| matches!(op, StoreOp::PutDispatchOutbox { .. }));

    let terminal_lease_id = released_lease_id(record, existing_run.as_deref());
    let projected_run_for_metrics = run_result
        .run_data()
        .and_then(|data| engine_proto::Run::decode(data).ok());
    let mut all_ops: Vec<StoreOp> = Vec::with_capacity(
        run_result.ops.len()
            + activation_result.ops.len()
            + state_ops.len()
            + approval_ops.len()
            + signal_ops.len()
            + timer_ops.len()
            + job_ops.len()
            + batch_ops.len()
            + session_ops.len()
            + message_ops.len()
            + event_ops.len()
            + instance_ops.len()
            + orchestration_ops.len(),
    );
    all_ops.extend(run_result.ops);
    all_ops.extend(activation_result.ops);
    all_ops.extend(state_ops);
    all_ops.extend(approval_ops);
    all_ops.extend(signal_ops);
    all_ops.extend(timer_ops);
    all_ops.extend(job_ops);
    all_ops.extend(batch_ops);
    all_ops.extend(session_ops);
    all_ops.extend(message_ops);
    all_ops.extend(event_ops);
    all_ops.extend(instance_ops);
    all_ops.extend(orchestration_ops);

    let dirty_demand_keys = dirty_demand_keys(record, &all_ops)?;
    let removed_pending_run = removed_pending_run(
        existing_run.as_deref(),
        projected_run_for_metrics.as_ref(),
        &all_ops,
    );

    let store_op_count = all_ops.len() as u64;
    let mut backend_op_count = None;
    record_processor_stage_duration(&record.event_type, "encode", encode_start);
    if !all_ops.is_empty() || advance_checkpoint {
        let write_start = std::time::Instant::now();
        let capabilities = projection_committer.capabilities_for(journal_record);
        let commit = projection_commit(journal_record, partition_id, all_ops, advance_checkpoint)?
            .for_capabilities(&capabilities);
        let result = projection_committer.commit(commit).await.map(|outcome| {
            backend_op_count = outcome.backend_operations();
        });
        record_processor_stage_duration(&record.event_type, "write_batch", write_start);
        result?;
    }
    record_processor_op_counts(&record.event_type, store_op_count, backend_op_count);
    if let Some(run) = projected_run_for_metrics.as_ref() {
        crate::metrics::record_pull_hotpath_durations(record, run, partition_id);
    }
    crate::metrics::record_activation_journal_event(record, partition_id);
    crate::metrics::record_billing_unmetered_step(record);
    let llm_event = LLM_EVENT_TYPES
        .contains(&record.event_type.as_str())
        .then(|| parse_llm_event(record, projected_run_for_metrics.as_ref()))
        .flatten();
    let processed = ProcessedRecord {
        terminal_lease_id,
        terminal_event_accepted: matches!(
            record.event_type.as_str(),
            "run.completed" | "run.failed" | "run.cancelled" | "run.paused" | "workflow.paused"
        ),
        cache_journal_record: true,
        dispatch_outbox_inserted,
        instance_obligation_put,
        dirty_demand_keys,
        removed_pending_run,
        llm_event,
    };

    if side_effect_segment.is_none() {
        return Ok(processed);
    }

    match orchestration_action.as_ref() {
        Some(crate::orchestrator::OrchestratorAction::Dispatch { .. }) => {}
        Some(crate::orchestrator::OrchestratorAction::Retry {
            attempt,
            max_attempts,
            ..
        }) => {
            if let Some(seg) = side_effect_segment {
                let retry_record = crate::orchestrator::build_retry_record(
                    record,
                    existing_run.as_deref(),
                    *attempt,
                    *max_attempts,
                );
                append_processor_side_effect(seg, retry_record)
                    .await
                    .map_err(|error| {
                        error!(
                            run_id = %record.run_id,
                            attempt = attempt,
                            max_attempts = max_attempts,
                            error = %error,
                            "Failed to write retry run.queued"
                        );
                        error
                    })?;
                crate::metrics::processor_metrics().processor_retries.add(
                    1,
                    &crate::metrics::failure_labels(record, existing_run.as_deref()),
                );
            }
        }
        Some(crate::orchestrator::OrchestratorAction::NoOp)
            if record.event_type == "run.failed" =>
        {
            // A `run.failed` reaching NoOp means the orchestrator saw no
            // retry budget remaining (or there was no retry policy) — the
            // run is effectively dead-lettered.
            crate::metrics::processor_metrics().dead_letters.add(
                1,
                &crate::metrics::failure_labels(record, existing_run.as_deref()),
            );
        }
        // Other event types reaching NoOp are normal flow and aren't counted.
        Some(crate::orchestrator::OrchestratorAction::NoOp) => {}
        None => {}
    }

    if let (Some(seg), Some(mut resume_record)) = (
        side_effect_segment,
        build_timer_resume_record(record, existing_run.as_deref(), existing_timer.as_deref()),
    ) {
        // An always-on agent instance's run is resumed by its supervisor; the
        // timer firing only records the request.
        engine_core::instance::defer_instance_resume(&mut resume_record);
        append_processor_side_effect(seg, resume_record)
            .await
            .map_err(|error| {
                error!(
                    run_id = %record.run_id,
                    error = %error,
                    "Failed to write timer-driven run.resumed"
                );
                error
            })?;
    }

    Ok(processed)
}

fn projection_commit(
    record: &Record,
    partition_id: u32,
    mutations: Vec<StoreOp>,
    advance_checkpoint: bool,
) -> EngineResult<ProjectionCommit> {
    let (encoded_partition, local_offset) =
        engine_core::partition_offset::decode_offset(record.offset);
    if encoded_partition != partition_id as usize {
        return Err(EngineError::InvalidArgument(format!(
            "record offset {} encodes partition {encoded_partition}, expected {partition_id}",
            record.offset.0
        )));
    }
    let commit = ProjectionCommit::new(
        ProjectionSource::new(
            JournalPosition::new(partition_id, Offset(local_offset)),
            record.clone(),
        ),
        mutations,
    );
    Ok(if advance_checkpoint {
        commit.advancing_checkpoint()
    } else {
        commit
    })
}

pub(crate) fn dispatch_outbox_metadata(record: &Record) -> Option<(u32, u64)> {
    let partition_id = record
        .metadata
        .get(DISPATCH_OUTBOX_PARTITION_ID_META)?
        .parse::<u32>()
        .ok()?;
    let queued_offset = record
        .metadata
        .get(DISPATCH_OUTBOX_QUEUED_OFFSET_META)?
        .parse::<u64>()
        .ok()?;
    Some((partition_id, queued_offset))
}

fn pull_dispatch_mode_requested(record: &Record, existing_run: Option<&[u8]>) -> bool {
    if dispatch_mode_metadata_is_pull(&record.metadata) {
        return true;
    }

    let Some(existing_run) = existing_run else {
        return false;
    };
    let Ok(run) = engine_proto::Run::decode(existing_run) else {
        return false;
    };
    dispatch_mode_metadata_is_pull(&run.metadata)
}

pub fn dispatch_mode_metadata_is_pull(
    metadata: &std::collections::HashMap<String, String>,
) -> bool {
    metadata
        .get(DISPATCH_MODE_META)
        .map(|value| dispatch_mode_value_is_pull(value))
        .unwrap_or(false)
}

fn dispatch_mode_value_is_pull(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        DISPATCH_MODE_PULL | "worker_mode_pull" | "2"
    )
}

async fn runtime_event_capture<R: ProjectionStateReader + ?Sized>(
    store: &R,
    record: &Record,
    partition_id: u32,
    result: &crate::projections::ProjectionResult,
) -> EngineResult<Vec<StoreOp>> {
    use engine_core::{
        runtime_events::{Configuration, Event, MatchIntent},
        runtime_work::{RuntimeWork, WorkKind},
    };
    // An agent instance's message is an event as a run's ending is: matched
    // against the project's subscriptions, which deliver it.
    let message = record.event_type == engine_core::instance::INSTANCE_MESSAGE_EVENT;
    if !message && !is_terminal_event(record) {
        return Ok(vec![]);
    }
    // A duplicate or rejected lifecycle record has no new Run projection.
    let run = if message {
        None
    } else {
        let Some(bytes) = result.run_data() else {
            return Ok(vec![]);
        };
        Some(
            engine_proto::Run::decode(bytes)
                .map_err(|error| EngineError::Storage(error.to_string()))?,
        )
    };
    let Some(head) = store
        .load_projection_event_configuration(partition_id, &record.project_id)
        .await?
    else {
        // Before the first durable configuration barrier, the project is not
        // activated in this source scope. This is not an empty applied revision.
        return Ok(vec![]);
    };
    head.validate()?;
    let configuration: Configuration = serde_json::from_value(head.payload)
        .map_err(|error| EngineError::Storage(error.to_string()))?;
    let (_, terminal_offset) = engine_core::partition_offset::decode_offset(record.offset);
    let event = match &run {
        Some(run) => Event::from_terminal(
            &configuration,
            run,
            &record.event_type,
            partition_id,
            terminal_offset,
            record.timestamp_ns,
        ),
        None => Event::from_message(&configuration, record, partition_id, terminal_offset),
    };
    let Some(event) = event else {
        return Ok(vec![]);
    };
    let work = RuntimeWork {
        partition_id,
        project_id: record.project_id.clone(),
        run_id: record.run_id.clone(),
        work_id: event.event_id.clone(),
        kind: WorkKind::EventMatch,
        version: 0,
        due_at_ms: 0,
        lease: None,
        terminal: false,
        payload: serde_json::to_value(MatchIntent {
            event,
            configuration,
        })
        .map_err(|error| EngineError::Storage(error.to_string()))?,
    };
    // Version zero is a derived obligation from the authoritative terminal and
    // preceding configuration records. The first claim journals version one.
    // Commit it with the source Run and checkpoint, independently of archival.
    Ok(vec![StoreOp::PutRuntimeWork(work)])
}

/// Whether a run event is final for archival, analytics, and lifecycle metrics.
///
/// A retryable `run.failed` releases its worker lease, but is not final until
/// the retry budget is exhausted or retry is explicitly disabled.
fn is_terminal_event(record: &Record) -> bool {
    matches!(
        record.event_type.as_str(),
        "run.completed" | "run.cancelled"
    ) || (record.event_type == "run.failed"
        && run_state_machine::next_retry_attempt(&record.metadata).is_none())
}

fn should_record_run_lifecycle_event(record: &Record, processed: &ProcessedRecord) -> bool {
    !is_terminal_event(record) || processed.terminal_event_accepted
}

fn terminal_stage(event_type: &str) -> &'static str {
    match event_type {
        "run.completed" => "completed",
        "run.failed" => "failed",
        "run.cancelled" => "cancelled",
        _ => "unknown",
    }
}

/// Parse an LLM event record into an LLMEventSummary for analytics forwarding.
///
/// The SDK puts LLM metrics in `record.metadata` as string values:
///   model="openai/gpt-4o-mini", provider="openai", input_tokens="36", etc.
fn parse_llm_event(record: &Record, run: Option<&engine_proto::Run>) -> Option<LLMEventSummary> {
    let meta = &record.metadata;

    let model = meta.get("model").cloned().unwrap_or_default();
    let provider = meta.get("provider").cloned().unwrap_or_default();
    let input_tokens = meta
        .get("input_tokens")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let output_tokens = meta
        .get("output_tokens")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let cached_tokens = meta
        .get("cached_tokens")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let cache_write_tokens = meta
        .get("cache_write_tokens")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let cost_micros = meta
        .get("cost_micros")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let latency_ms = meta
        .get("duration_ms")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);

    // Skip events with no meaningful data, and an observed model call that
    // another instrumentation's span already reported: it is the same call,
    // and counting it would add a call with no tokens.
    if model.is_empty() && input_tokens == 0 && output_tokens == 0 {
        return None;
    }
    if meta.contains_key("usage_duplicate_of") {
        return None;
    }

    let component_name = meta
        .get("component_name")
        .cloned()
        .filter(|value| !value.is_empty())
        .or_else(|| run.map(|run| run.component_name.clone()))
        .unwrap_or_default();
    let deployment_id = meta
        .get("deployment_id")
        .cloned()
        .filter(|value| !value.is_empty())
        .or_else(|| run.map(|run| run.deployment_id.clone()))
        .unwrap_or_default();
    let step_key = record.step_key.clone();

    Some(LLMEventSummary {
        run_id: record.run_id.clone(),
        project_id: record.project_id.clone(),
        deployment_id,
        component_name,
        provider,
        model,
        input_tokens,
        output_tokens,
        cached_tokens,
        cache_write_tokens,
        cost_micros,
        latency_ms,
        step_key,
        timestamp_ns: record.timestamp_ns,
        origin: usage::run_origin(meta).to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use engine_core::types::Offset;
    use prost::Message;
    use std::collections::HashMap;

    struct AlreadyAdmittedProjection;

    #[async_trait::async_trait]
    impl ProjectionAdmissionGate for AlreadyAdmittedProjection {
        fn exact_applied_through(&self, _partition_id: u32) -> EngineResult<u64> {
            Ok(0)
        }

        async fn is_admitted(&self) -> EngineResult<bool> {
            Ok(true)
        }

        async fn admit(&self, _evidence: ProjectionAdmissionEvidence) -> EngineResult<()> {
            panic!("an already-admitted generation must not be admitted again")
        }
    }

    #[tokio::test]
    async fn admitted_projection_skips_cold_journal_evidence_scan() {
        let admission: Arc<dyn ProjectionAdmissionGate> = Arc::new(AlreadyAdmittedProjection);
        admit_projection_replay(0, Some(10), None, Some(&admission))
            .await
            .unwrap();
    }

    fn observed_model_call(extra: &[(&str, &str)]) -> Record {
        let mut metadata = HashMap::from([
            ("origin".to_string(), "otlp".to_string()),
            ("provider".to_string(), "anthropic".to_string()),
            ("model".to_string(), "claude-haiku-5-5".to_string()),
        ]);
        for (key, value) in extra {
            metadata.insert((*key).to_string(), (*value).to_string());
        }
        make_record("lm.completed", 1, "{}", metadata)
    }

    #[test]
    fn observed_model_calls_reach_analytics_with_their_origin() {
        let event = parse_llm_event(
            &observed_model_call(&[
                ("input_tokens", "1200"),
                ("output_tokens", "300"),
                ("cache_write_tokens", "1000"),
            ]),
            None,
        )
        .expect("an observed call is reported");
        assert_eq!(event.origin, "otlp");
        assert_eq!(event.cache_write_tokens, 1000);

        // The same call reported by a second instrumentation adds nothing.
        let duplicate = observed_model_call(&[("usage_duplicate_of", "00f067aa0ba902b7")]);
        assert!(parse_llm_event(&duplicate, None).is_none());

        let native = make_record(
            "lm.completed",
            1,
            "{}",
            HashMap::from([
                ("model".to_string(), "gpt-5".to_string()),
                ("input_tokens".to_string(), "10".to_string()),
            ]),
        );
        assert_eq!(parse_llm_event(&native, None).unwrap().origin, "native");
    }

    fn make_record(
        event_type: &str,
        ts_ns: i64,
        data: &str,
        metadata: HashMap<String, String>,
    ) -> Record {
        Record {
            offset: Offset(0), // Will be assigned by segment
            project_id: "t1".to_string(),
            run_id: "r1".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::from(data.to_string()),
            timestamp_ns: ts_ns,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    #[test]
    fn demand_dirty_keys_cover_put_remove_and_coalesce() {
        let record = make_record("run.queued", 1_000_000_000, "{}", HashMap::new());
        let pending = StoreOp::PutPendingPriority {
            deployment_id: "deployment".to_string(),
            component_type: ComponentType::Function as i32,
            component_name: "component".to_string(),
            priority: 1,
            ready_at_ms: 1_000,
            created_at_ms: 1_000,
        };
        let removed = StoreOp::RemovePendingPriority {
            deployment_id: "deployment".to_string(),
            component_type: ComponentType::Function as i32,
            component_name: "component".to_string(),
            priority: 1,
            ready_at_ms: 1_000,
            created_at_ms: 1_000,
        };

        assert_eq!(
            dirty_demand_keys(&record, &[pending.clone(), removed, pending]).unwrap(),
            vec![DemandKey::new("t1", "deployment", ComponentType::Function, "component").unwrap()]
        );
    }

    #[tokio::test]
    async fn demand_dirty_notification_is_lossy_and_non_blocking() {
        let key = DemandKey::new(
            "project",
            "deployment",
            ComponentType::Function,
            "component",
        )
        .unwrap();
        let event = DemandKeyDirtyEvent {
            partition_id: 3,
            demand_key: key,
            removed_pending_run: None,
        };
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(event.clone()).unwrap();

        notify_demand_key_dirty(&tx, event.clone(), "node-test");
        assert_eq!(rx.len(), 1, "a full channel stays bounded");

        drop(rx);
        notify_demand_key_dirty(&tx, event, "node-test");
    }

    #[test]
    fn pending_removal_uses_exact_old_projected_generation() {
        use engine_core::lease_fence::RUN_META_DISPATCH_GENERATION;
        let queued = make_record(
            "run.queued",
            1_000_000_000,
            "{}",
            HashMap::from([
                ("workspace_id".into(), "workspace".into()),
                ("deployment_id".into(), "deployment".into()),
                ("component_type".into(), "function".into()),
                ("component_name".into(), "component".into()),
                ("dispatch_mode".into(), "pull".into()),
            ]),
        );
        let queued_ops = runs::project(&queued, None);
        let before = queued_ops.run_data().unwrap();
        let old = engine_proto::Run::decode(before).unwrap();
        let generation = &old.metadata[RUN_META_DISPATCH_GENERATION];
        let mut assigned = make_record(
            "run.assigned",
            2_000_000_000,
            "{}",
            HashMap::from([(RUN_META_DISPATCH_GENERATION.into(), generation.clone())]),
        );
        assigned.offset = Offset(9);
        let assigned_ops = runs::project(&assigned, Some(before));
        let after = engine_proto::Run::decode(assigned_ops.run_data().unwrap()).unwrap();
        assert_eq!(
            after.metadata[RUN_META_DISPATCH_GENERATION], *generation,
            "assignment must retain the offered queue generation, not its assignment offset"
        );
        let (key, removed) =
            removed_pending_run(Some(before), Some(&after), &assigned_ops.ops).unwrap();
        assert_eq!(key, DemandKey::from_run(&old).unwrap());
        assert_eq!(removed.run_id, old.run_id);
        assert_eq!(removed.dispatch_generation.as_str(), generation);
        assert!(
            removed_pending_run(Some(before), Some(&after), &[]).is_none(),
            "no mutation is not removal evidence"
        );
        assert!(
            removed_pending_run(Some(before), Some(&old), &queued_ops.ops).is_none(),
            "same pending generation remains eligible"
        );
        let mut advanced = old.clone();
        advanced
            .metadata
            .insert(RUN_META_DISPATCH_GENERATION.into(), "10".into());
        assert_eq!(
            removed_pending_run(Some(before), Some(&advanced), &queued_ops.ops)
                .unwrap()
                .1,
            removed
        );
        assert!(removed_pending_run(None, Some(&after), &assigned_ops.ops).is_none());
        assert!(removed_pending_run(Some(before), None, &assigned_ops.ops).is_none());
    }

    #[test]
    fn retryable_failure_is_not_final_for_side_effects() {
        let retryable = make_record(
            "run.failed",
            1_000_000_000,
            "{}",
            HashMap::from([
                ("attempt".to_string(), "0".to_string()),
                ("max_attempts".to_string(), "3".to_string()),
            ]),
        );
        let exhausted = make_record(
            "run.failed",
            1_000_000_000,
            "{}",
            HashMap::from([
                ("attempt".to_string(), "2".to_string()),
                ("max_attempts".to_string(), "3".to_string()),
            ]),
        );

        assert!(!is_terminal_event(&retryable));
        assert!(is_terminal_event(&exhausted));
    }

    #[test]
    fn timer_fired_builds_workerless_resume_record_with_checkpoint_metadata() {
        let checkpoint = serde_json::json!({"steps": {"wait": 1000}}).to_string();
        let run = Run {
            status: "paused".to_string(),
            input_data: br#"{"topic":"durability"}"#.to_vec(),
            component_name: "research".to_string(),
            component_type: "workflow".to_string(),
            deployment_id: "dep-1".to_string(),
            metadata: HashMap::from([
                ("agnt5_fence_kind".to_string(), "1".to_string()),
                ("agnt5_fence_lease_id".to_string(), "lease-1".to_string()),
                ("lease_id".to_string(), "lease-1".to_string()),
                ("worker_id".to_string(), "worker-1".to_string()),
                ("workerless_checkpoint".to_string(), checkpoint.clone()),
            ]),
            ..Default::default()
        };
        let timer = Timer {
            timer_id: "workerless-r1-2500-wait".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            timer_key: "wait".to_string(),
            status: "scheduled".to_string(),
            schedule_payload: serde_json::to_vec(&serde_json::json!({
                "resume_metadata": {
                    "component_name": "research",
                    "component_type": "workflow",
                    "dispatch_mode": "http",
                    "workerless_checkpoint": checkpoint,
                    "lease_id": "lease-1",
                    "lease_worker_id": "worker-1",
                }
            }))
            .unwrap(),
            ..Default::default()
        };
        let mut fired_metadata = HashMap::new();
        fired_metadata.insert("timer_id".to_string(), timer.timer_id.clone());
        fired_metadata.insert("workerless_ready_at_ms".to_string(), "2500".to_string());
        let fired = make_record("timer.fired", 2_500_000_000, "{}", fired_metadata);
        let run_bytes = run.encode_to_vec();
        let timer_bytes = timer.encode_to_vec();

        let resume =
            build_timer_resume_record(&fired, Some(&run_bytes), Some(&timer_bytes)).unwrap();

        assert_eq!(resume.event_type, "run.resumed");
        assert_eq!(
            resume.data,
            Bytes::from_static(br#"{"topic":"durability"}"#)
        );
        assert_eq!(
            resume.metadata.get("component_name").map(String::as_str),
            Some("research")
        );
        assert_eq!(
            resume
                .metadata
                .get("workerless_checkpoint")
                .map(String::as_str),
            Some(r#"{"steps":{"wait":1000}}"#)
        );
        assert_eq!(
            resume
                .metadata
                .get("workerless_timer_id")
                .map(String::as_str),
            Some("workerless-r1-2500-wait")
        );
        assert_eq!(
            resume
                .metadata
                .get("workerless_resume_reason")
                .map(String::as_str),
            Some("timer")
        );
        assert_eq!(
            resume
                .metadata
                .get(META_RESUME_SOURCE_ID)
                .map(String::as_str),
            Some("lease:lease-1")
        );
        for key in [
            "agnt5_fence_kind",
            "agnt5_fence_lease_id",
            "lease_id",
            "worker_id",
            "lease_worker_id",
        ] {
            assert!(
                !resume.metadata.contains_key(key),
                "timer resume metadata must not inherit {key}"
            );
        }
    }

    // The run projection keeps the last resume's answer in run metadata. A
    // timer resume built from that metadata used to carry it, and the next
    // question completed with it without anyone being asked.
    #[test]
    fn timer_resume_never_carries_a_user_response() {
        let run = Run {
            status: "paused".to_string(),
            input_data: b"{}".to_vec(),
            component_name: "fanout_approval".to_string(),
            component_type: "workflow".to_string(),
            metadata: HashMap::from([
                ("lease_id".to_string(), "lease-1".to_string()),
                ("user_response".to_string(), "approve".to_string()),
                (
                    "pause_reason".to_string(),
                    "user_input_required".to_string(),
                ),
            ]),
            ..Default::default()
        };
        let timer = Timer {
            timer_id: "timer-cooldown".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            timer_key: "sleep:cooldown".to_string(),
            status: "scheduled".to_string(),
            schedule_payload: serde_json::to_vec(&serde_json::json!({
                "resume_metadata": {"lease_id": "lease-1"}
            }))
            .unwrap(),
            ..Default::default()
        };
        let mut fired_metadata = HashMap::new();
        fired_metadata.insert("timer_id".to_string(), timer.timer_id.clone());
        let fired = make_record("timer.fired", 2_000_000_000, "{}", fired_metadata);

        let resume = build_timer_resume_record(
            &fired,
            Some(&run.encode_to_vec()),
            Some(&timer.encode_to_vec()),
        )
        .expect("the timer resumes its own pause");

        assert_eq!(resume.event_type, "run.resumed");
        assert_eq!(resume.metadata.get("user_response"), None);
    }

    #[test]
    fn stale_timer_cannot_resume_a_newer_pause_generation() {
        let run = Run {
            status: "paused".to_string(),
            metadata: HashMap::from([(
                META_RESUME_SOURCE_ID.to_string(),
                "lease:pause-b".to_string(),
            )]),
            ..Default::default()
        };
        let timer = Timer {
            timer_id: "timer-from-pause-a".to_string(),
            status: "scheduled".to_string(),
            schedule_payload: serde_json::to_vec(&serde_json::json!({
                "resume_metadata": {
                    "lease_id": "pause-a",
                    "component_name": "research",
                }
            }))
            .unwrap(),
            ..Default::default()
        };
        let fired = make_record(
            "timer.fired",
            2_500_000_000,
            "{}",
            HashMap::from([("timer_id".to_string(), "timer-from-pause-a".to_string())]),
        );

        assert!(
            build_timer_resume_record(
                &fired,
                Some(&run.encode_to_vec()),
                Some(&timer.encode_to_vec()),
            )
            .is_none(),
            "a timer from pause A must not resume pause B"
        );
    }

    fn suspension_timer_record(
        offset: u64,
        timer_id: &str,
        resume_source: &str,
        continuation: &str,
    ) -> Record {
        let mut record = make_record(
            "timer.scheduled",
            2_000_000_000,
            &serde_json::json!({
                "fire_at_ns": 2_500_000_000_i64,
                "schedule_payload": {
                    "resume_metadata": {
                        META_RESUME_SOURCE_ID: resume_source,
                        "activation_id": "activation-sleep-2",
                        "timer_id": timer_id,
                        "timer_key": "sleep:gap-2",
                        "continuation_b64": continuation,
                    }
                }
            })
            .to_string(),
            HashMap::from([
                ("timer_id".to_string(), timer_id.to_string()),
                ("timer_key".to_string(), "sleep:gap-2".to_string()),
                (META_RESUME_SOURCE_ID.to_string(), resume_source.to_string()),
            ]),
        );
        record.offset = Offset(offset);
        record
    }

    #[tokio::test]
    async fn a_resume_keeps_a_continuation_the_run_row_does_not_hold() {
        // A pause projected before the run row kept continuations: dropping
        // the copy would dispatch a stale continuation, so the record keeps
        // it and the oversized append fails as it did before.
        let continuation = "A".repeat(202_062);
        let run = Run {
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            status: "paused".to_string(),
            component_name: "scan_reddit".to_string(),
            metadata: HashMap::from([
                (
                    META_RESUME_SOURCE_ID.to_string(),
                    "lease:lease-2".to_string(),
                ),
                ("continuation_b64".to_string(), "A".repeat(121_882)),
            ]),
            ..Default::default()
        };
        let scheduled = suspension_timer_record(3, "timer-2", "lease:lease-2", &continuation);
        let schedule_payload = serde_json::from_slice::<serde_json::Value>(&scheduled.data)
            .unwrap()["schedule_payload"]
            .clone();
        let timer = Timer {
            timer_id: "timer-2".to_string(),
            status: "scheduled".to_string(),
            schedule_payload: serde_json::to_vec(&schedule_payload).unwrap(),
            ..Default::default()
        };
        let fired = make_record(
            "timer.fired",
            2_500_000_000,
            "{}",
            HashMap::from([("timer_id".to_string(), "timer-2".to_string())]),
        );

        let resume = build_timer_resume_record(
            &fired,
            Some(&run.encode_to_vec()),
            Some(&timer.encode_to_vec()),
        )
        .unwrap();
        assert_eq!(resume.metadata.get("continuation_b64"), Some(&continuation));
        assert!(engine_core::journal_contract::validate_append_records(&[resume]).is_err());
    }

    #[test]
    fn pull_dispatch_mode_requested_reads_record_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert(
            DISPATCH_MODE_META.to_string(),
            DISPATCH_MODE_PULL.to_string(),
        );
        let record = make_record("run.queued", 1_000_000_000, "{}", metadata);

        assert!(pull_dispatch_mode_requested(&record, None));
    }

    #[test]
    fn pull_dispatch_mode_requested_reads_existing_run_metadata() {
        let record = make_record("run.resumed", 1_000_000_000, "{}", HashMap::new());
        let mut run = engine_proto::Run::default();
        run.metadata.insert(
            DISPATCH_MODE_META.to_string(),
            DISPATCH_MODE_PULL.to_string(),
        );
        let mut encoded = Vec::new();
        run.encode(&mut encoded).unwrap();

        assert!(pull_dispatch_mode_requested(&record, Some(&encoded)));
    }

    #[test]
    fn pull_dispatch_mode_requested_defaults_to_push() {
        let record = make_record("run.queued", 1_000_000_000, "{}", HashMap::new());

        assert!(!pull_dispatch_mode_requested(&record, None));
    }
}
