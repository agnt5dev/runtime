//! Backend-neutral storage contract types.
//!
//! These types define the consumer-owned boundaries used by explicit runtime
//! storage profiles. `MaterializedStore` remains only as a managed
//! compatibility implementation behind those ports while backend-specific
//! adapters are extracted.

use std::collections::BTreeSet;

use async_trait::async_trait;
use engine_proto::Run;

use crate::{
    demand::DemandKey,
    error::{EngineError, EngineResult},
    storage_assessment::ProjectionPartitionCertification,
    traits::MaterializedStore,
    types::{LeaseExpiryCursor, Offset, Record, TerminalObligationCursor, TerminalObligationEntry},
};

/// One explicit project rollout scope shared by publishers, projections,
/// query readers, compaction, and retention.
///
/// Keeping `Disabled` distinct from `All` prevents the former
/// `Option<BTreeSet<_>>` ambiguity where both states appeared as `None`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ProjectScope {
    #[default]
    Disabled,
    Projects(BTreeSet<String>),
    All,
}

impl ProjectScope {
    pub fn projects(projects: impl IntoIterator<Item = String>) -> Self {
        let projects = projects.into_iter().collect::<BTreeSet<_>>();
        if projects.is_empty() {
            Self::Disabled
        } else {
            Self::Projects(projects)
        }
    }

    pub const fn enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    pub fn includes(&self, project_id: &str) -> bool {
        match self {
            Self::Disabled => false,
            Self::Projects(projects) => projects.contains(project_id),
            Self::All => true,
        }
    }

    pub fn project_count(&self) -> Option<usize> {
        match self {
            Self::Disabled => Some(0),
            Self::Projects(projects) => Some(projects.len()),
            Self::All => None,
        }
    }
}

/// Partition evidence presented at the projection-admission barrier after
/// startup replay reaches its fixed committed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionAdmissionEvidence {
    pub partition_id: u32,
    pub certification: ProjectionPartitionCertification,
}

/// One shared seam controlling when a projection generation may become ready.
/// Implementations block each caller until every configured partition is
/// admitted or the generation is rejected.
#[async_trait]
pub trait ProjectionAdmissionGate: Send + Sync {
    /// Return the exact exclusive applied frontier (`M`) for one partition of
    /// the admitted or rebuilding generation. Unlike a raw checkpoint read,
    /// this preserves the contract that a missing key means exact zero only
    /// for a provenance-marked generation.
    fn exact_applied_through(&self, partition_id: u32) -> EngineResult<u64>;

    /// Whether this generation has already crossed the shared admission
    /// barrier. Callers use this as a cheap preflight before constructing
    /// journal certification evidence, which may require scanning cold data.
    async fn is_admitted(&self) -> EngineResult<bool> {
        Ok(false)
    }

    async fn admit(&self, evidence: ProjectionAdmissionEvidence) -> EngineResult<()>;
}

/// Reserved projection identities for Durable Activation V1. They are kept in
/// the existing entity-state physical domain so older binaries can ignore the
/// additive rows during mixed-version rollback.
pub const ACTIVATION_PROJECTION_ENTITY_TYPE: &str = "__agnt5_activation_v1";
pub const ACTIVATION_OUTBOX_ENTITY_TYPE: &str = "__agnt5_activation_outbox_v1";
pub const ACTIVATION_PROJECTION_SCOPE: &str = "run";

/// Backend-neutral position in one partition's journal.
///
/// `offset` is always partition-local. Router-specific encoded offsets must be
/// decoded before crossing the storage-adapter boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JournalPosition {
    partition_id: u32,
    offset: Offset,
}

impl JournalPosition {
    pub const fn new(partition_id: u32, offset: Offset) -> Self {
        Self {
            partition_id,
            offset,
        }
    }

    pub const fn partition_id(self) -> u32 {
        self.partition_id
    }

    pub const fn offset(self) -> Offset {
        self.offset
    }
}

/// Source journal record and its backend-neutral partition-local position.
///
/// The original record is retained because projection mutations use its
/// project, run, timestamp, metadata, and serialized event representation.
#[derive(Debug, Clone)]
pub struct ProjectionSource {
    position: JournalPosition,
    record: Record,
}

impl ProjectionSource {
    pub fn new(position: JournalPosition, record: Record) -> Self {
        Self { position, record }
    }

    pub fn position(&self) -> JournalPosition {
        self.position
    }

    pub fn record(&self) -> &Record {
        &self.record
    }

    pub fn into_record(self) -> Record {
        self.record
    }
}

/// One semantic mutation produced by the partition processor.
///
/// These variants describe runtime intent rather than physical column
/// families, encoded keys, SQL tables, or indexes. Storage adapters own the
/// lowering into their backend representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionMutation {
    /// Upsert the serialized run projection.
    PutRun(Vec<u8>),
    /// Upsert the active terminal obligation for a partition-owned run.
    PutTerminalObligation {
        partition_id: u32,
        run_data: Vec<u8>,
    },
    /// Remove the terminal obligation after a run becomes terminal or suspended.
    DeleteTerminalObligation { partition_id: u32 },
    /// Upsert one step memoization value.
    PutStepMemo { step_key: String, data: Vec<u8> },
    /// Upsert one derived activation snapshot. Journal records remain authoritative.
    PutActivation {
        activation_id: String,
        data: Vec<u8>,
        version: i64,
    },
    /// Persist one effect intent from an accepted activation completion.
    PutActivationOutbox {
        activation_id: String,
        attempt: u32,
        intent_id: String,
        data: Vec<u8>,
    },
    /// Index the source record as a run event.
    PutEvent,
    /// Upsert one versioned entity-state projection.
    PutEntityState {
        entity_type: String,
        entity_key: String,
        scope: String,
        scope_id: String,
        data: Vec<u8>,
        version: i64,
    },
    /// Maintain the partition recovery index for a journal-backed controller.
    PutRuntimeWork(crate::runtime_work::RuntimeWork),
    /// Upsert serialized approval state and its run relationship.
    PutApproval {
        approval_id: String,
        run_id: String,
        data: Vec<u8>,
    },
    /// Upsert serialized signal state and its lookup indexes.
    PutSignal {
        signal_id: String,
        run_id: String,
        signal_name: String,
        waiting_step: String,
        data: Vec<u8>,
    },
    /// Upsert serialized timer state and maintain its due index.
    PutTimer {
        timer_id: String,
        run_id: String,
        status: String,
        fire_at_ns: i64,
        data: Vec<u8>,
    },
    /// Upsert serialized job state and its run and batch relationships.
    PutJob {
        job_id: String,
        run_id: String,
        batch_id: String,
        data: Vec<u8>,
    },
    /// Upsert serialized batch state.
    PutBatch { batch_id: String, data: Vec<u8> },
    /// Upsert serialized worker-session state and component relationship.
    PutSession {
        session_id: String,
        component_name: String,
        data: Vec<u8>,
    },
    /// Upsert a serialized message and correlation relationship.
    PutMessage {
        message_id: String,
        correlation_id: String,
        data: Vec<u8>,
    },
    /// Add a run to the compatibility pending-work index.
    PutPendingRun {
        component_name: String,
        data: Vec<u8>,
    },
    /// Remove a run from the compatibility pending-work index.
    RemovePendingRun { component_name: String },
    /// Add a run to the priority-aware pending-work index.
    PutPendingPriority {
        deployment_id: String,
        component_type: i32,
        component_name: String,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
    },
    /// Remove the exact priority-aware pending-work entry.
    RemovePendingPriority {
        deployment_id: String,
        component_type: i32,
        component_name: String,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
    },
    /// Add a run to the deployment-scoped pending-work index.
    PutPendingDeploymentPriority {
        deployment_id: String,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
    },
    /// Remove the exact deployment-scoped pending-work entry.
    RemovePendingDeploymentPriority {
        deployment_id: String,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
    },
    /// Adjust durable project, deployment, and priority queue rollups.
    UpdateQueueCounters {
        deployment_id: String,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        delta: i64,
    },
    /// Add the expiry and worker relationships for one delivery lease.
    PutLeaseIndex {
        expires_at_ms: i64,
        worker_id: String,
    },
    /// Remove the expiry and worker relationships for one delivery lease.
    RemoveLeaseIndex {
        expires_at_ms: i64,
        worker_id: String,
    },
    /// Persist dispatch intent atomically with the state that created it.
    PutDispatchOutbox {
        partition_id: u32,
        queued_offset: u64,
        request_data: Vec<u8>,
    },
    /// Acknowledge and delete one durable dispatch intent.
    DeleteDispatchOutbox {
        partition_id: u32,
        project_id: String,
        queued_offset: u64,
        run_id: String,
    },
    /// Durable intent to publish one terminal run to an archive.
    ///
    /// Whether the adapter lowers this into an S3 outbox, another object
    /// store, or no optional archive port is a storage-profile decision.
    PutRunArchiveIntent {
        partition_id: u32,
        terminal_offset: u64,
        run_data: Vec<u8>,
    },
    /// Acknowledge and delete one durable run-archive intent.
    DeleteRunArchiveIntent {
        partition_id: u32,
        project_id: String,
        terminal_offset: u64,
        run_id: String,
    },
    /// Independent V2 shadow intent for one terminal Run projection.
    PutRunQueryArchiveIntent {
        partition_id: u32,
        terminal_offset: u64,
        run_data: Vec<u8>,
    },
    /// Complete one V2 shadow intent without touching the Parquet outbox.
    DeleteRunQueryArchiveIntent {
        partition_id: u32,
        project_id: String,
        terminal_offset: u64,
        run_id: String,
    },
    /// Record exact committed-segment coverage for one Run row version.
    PutRunQueryArchiveReceipt {
        project_id: String,
        run_id: String,
        row_version: u64,
        receipt_data: Vec<u8>,
    },
    /// Index a queued run for newest-first hot queries.
    PutRunHotIndex {
        workspace_id: String,
        submitted_at_ms: i64,
    },
    /// Publish the durable location of an archived run generation.
    PutRunArchiveLocation {
        project_id: String,
        run_id: String,
        archive_location: String,
    },
    /// Schedule hot-run reclamation after a verified archive publication.
    PutRunRetention {
        project_id: String,
        run_id: String,
        archived_at_ms: i64,
        archive_generation: String,
    },
    /// Index an inbound event for newest-first hot queries.
    PutEventHotIndex { timestamp_ms: i64, summary: Vec<u8> },
}

impl ProjectionMutation {
    /// Optional storage capability required to apply this mutation.
    ///
    /// Required projection mutations return `None`. Runtime composition must
    /// not emit capability-gated mutations to a profile that does not
    /// advertise the corresponding port.
    pub const fn required_capability(&self) -> Option<StorageCapability> {
        match self {
            Self::PutRuntimeWork(_) => Some(StorageCapability::RuntimeWork),
            Self::PutRunArchiveIntent { .. }
            | Self::DeleteRunArchiveIntent { .. }
            | Self::PutRunArchiveLocation { .. }
            | Self::PutRunRetention { .. } => Some(StorageCapability::RunArchive),
            Self::PutRunQueryArchiveIntent { .. }
            | Self::DeleteRunQueryArchiveIntent { .. }
            | Self::PutRunQueryArchiveReceipt { .. } => Some(StorageCapability::QueryArchiveShadow),
            _ => None,
        }
    }
}

/// Durability requested for one projection commit.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CommitDurability {
    /// Use the backend's normal durable-write behavior.
    #[default]
    BackendDefault,
    /// Do not return until the backend can prove the commit is durable.
    ///
    /// Managed archive catalog publication requires this before trimming the
    /// corresponding hot data.
    Synchronous,
}

/// All derived mutations produced while projecting one journal record.
#[derive(Debug, Clone)]
pub struct ProjectionCommit {
    source: ProjectionSource,
    advance_checkpoint: bool,
    durability: CommitDurability,
    mutations: Vec<ProjectionMutation>,
}

impl ProjectionCommit {
    /// Create a projection commit for one journal record.
    pub fn new(source: ProjectionSource, mutations: Vec<ProjectionMutation>) -> Self {
        Self {
            source,
            advance_checkpoint: false,
            durability: CommitDurability::BackendDefault,
            mutations,
        }
    }

    /// Atomically advance the processor checkpoint with this commit.
    ///
    /// The checkpoint is deliberately derived from `source`: callers cannot
    /// advance it beyond the state represented by this commit.
    pub fn advancing_checkpoint(mut self) -> Self {
        self.advance_checkpoint = true;
        self
    }

    pub fn with_durability(mut self, durability: CommitDurability) -> Self {
        self.durability = durability;
        self
    }

    pub fn source(&self) -> &ProjectionSource {
        &self.source
    }

    pub fn checkpoint(&self) -> Option<JournalPosition> {
        self.advance_checkpoint.then_some(self.source.position())
    }

    pub fn durability(&self) -> CommitDurability {
        self.durability
    }

    pub fn mutations(&self) -> &[ProjectionMutation] {
        &self.mutations
    }

    /// Select the mutations that the explicitly composed storage profile can
    /// durably apply.
    ///
    /// Only capability-gated optional behavior is omitted. Required
    /// mutations are always retained and adapters still validate the final
    /// commit before writing it.
    pub fn for_capabilities(mut self, capabilities: &StorageCapabilities) -> Self {
        self.mutations.retain(|mutation| {
            mutation
                .required_capability()
                .is_none_or(|required| capabilities.supports(required))
        });
        self
    }

    /// Reject optional mutations that the selected storage profile cannot
    /// durably apply.
    pub fn validate_capabilities(&self, capabilities: &StorageCapabilities) -> EngineResult<()> {
        for mutation in &self.mutations {
            if let Some(required) = mutation.required_capability() {
                if !capabilities.supports(required) {
                    return Err(EngineError::InvalidArgument(format!(
                        "projection mutation requires unsupported storage capability: {required:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Adapter-specific execution details returned after a successful commit.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionCommitOutcome {
    backend_operations: Option<u64>,
}

impl ProjectionCommitOutcome {
    pub const fn with_backend_operations(backend_operations: u64) -> Self {
        Self {
            backend_operations: Some(backend_operations),
        }
    }

    pub const fn backend_operations(self) -> Option<u64> {
        self.backend_operations
    }
}

/// Adapter-owned identity for one durable outbox row.
///
/// Consumers may compare, clone, and return this token to the adapter for
/// acknowledgement or quarantine. They must not interpret its bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutboxEntryToken(Vec<u8>);

impl OutboxEntryToken {
    #[doc(hidden)]
    pub fn from_adapter(raw: Vec<u8>) -> Self {
        Self(raw)
    }

    #[doc(hidden)]
    pub fn adapter_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One run waiting in a partition's route-aware pending index, which holds
/// every queued deployment run whatever its dispatch mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingQueueRow {
    /// Adapter key; pass it back to resume a scan after this row.
    pub key: Vec<u8>,
    pub project_id: String,
    pub deployment_id: String,
    pub run_id: String,
}

/// One semantically identified dispatch intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchOutboxEntry {
    pub token: OutboxEntryToken,
    pub partition_id: u32,
    pub project_id: String,
    pub queued_offset: u64,
    pub run_id: String,
    pub payload: Vec<u8>,
}

/// Result of scanning one managed dispatch-outbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutboxScanEntry {
    Ready(DispatchOutboxEntry),
    CorruptKey { token: OutboxEntryToken },
}

impl DispatchOutboxScanEntry {
    pub fn token(&self) -> &OutboxEntryToken {
        match self {
            Self::Ready(entry) => &entry.token,
            Self::CorruptKey { token } => token,
        }
    }
}

/// Opaque continuation position for a partition-local dispatch-outbox scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchOutboxCursor {
    token: OutboxEntryToken,
    position: Option<(u64, String, String)>,
}

impl DispatchOutboxCursor {
    pub fn after(entry: &DispatchOutboxScanEntry) -> Self {
        let position = match entry {
            DispatchOutboxScanEntry::Ready(entry) => Some((
                entry.queued_offset,
                entry.project_id.clone(),
                entry.run_id.clone(),
            )),
            DispatchOutboxScanEntry::CorruptKey { .. } => None,
        };
        Self {
            token: entry.token().clone(),
            position,
        }
    }

    #[doc(hidden)]
    pub fn adapter_bytes(&self) -> &[u8] {
        self.token.adapter_bytes()
    }

    pub fn position(&self) -> Option<(u64, &str, &str)> {
        self.position
            .as_ref()
            .map(|(offset, project_id, run_id)| (*offset, project_id.as_str(), run_id.as_str()))
    }
}

/// One semantically identified run-archive intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveOutboxEntry {
    pub token: OutboxEntryToken,
    pub partition_id: u32,
    pub project_id: String,
    pub terminal_offset: u64,
    pub run_id: String,
    pub run_data: Vec<u8>,
}

/// Result of scanning one managed archive-outbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveOutboxScanEntry {
    Ready(ArchiveOutboxEntry),
    CorruptKey { token: OutboxEntryToken },
}

/// One semantically identified V2 shadow archive intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryArchiveOutboxEntry {
    pub token: OutboxEntryToken,
    pub partition_id: u32,
    pub project_id: String,
    pub terminal_offset: u64,
    pub run_id: String,
    pub run_data: Vec<u8>,
    /// Optional local event-index observations. Retry reservations leave these
    /// empty; archive publication must prove history through its journal reader.
    pub run_events: Vec<Record>,
}

/// Result of scanning one V2 shadow archive outbox row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryArchiveOutboxScanEntry {
    Ready(QueryArchiveOutboxEntry),
    CorruptKey { token: OutboxEntryToken },
}

/// One retry reservation. The pending obligation remains until its receipt is projected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryArchiveOutboxAttempt {
    pub entry: QueryArchiveOutboxScanEntry,
    pub attempt: u32,
    pub next_attempt_ms: u64,
}

/// Selection evidence keeps deferred obligations visible independently of eligibility.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryArchiveOutboxBatch {
    pub attempts: Vec<QueryArchiveOutboxAttempt>,
    pub pending_observed: usize,
}

/// Atomic projection commit port implemented by each storage adapter.
#[async_trait]
pub trait ProjectionCommitter: Send + Sync + 'static {
    /// Optional behavior genuinely supported by this adapter profile.
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::none()
    }

    /// Capabilities admitted for this exact source record. Most adapters are
    /// global and inherit `capabilities`; rollout-aware adapters override this
    /// so the processor filters optional mutations exactly once.
    fn capabilities_for(&self, _record: &Record) -> StorageCapabilities {
        self.capabilities()
    }

    async fn commit(&self, commit: ProjectionCommit) -> EngineResult<ProjectionCommitOutcome>;
}

/// Consumer-owned port for durable dispatch intent draining.
#[async_trait]
pub trait DispatchOutboxStore: Send + Sync {
    async fn scan_dispatch_intents(
        &self,
        partition_id: u32,
        cursor: Option<&DispatchOutboxCursor>,
        limit: usize,
    ) -> EngineResult<Vec<DispatchOutboxScanEntry>>;

    async fn delete_dispatch_intent(&self, token: &OutboxEntryToken) -> EngineResult<()>;

    async fn load_dispatch_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    #[allow(clippy::too_many_arguments)]
    async fn ensure_pending_deployment_run(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        run_id: &str,
    ) -> EngineResult<()>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> DispatchOutboxStore for T {
    async fn scan_dispatch_intents(
        &self,
        partition_id: u32,
        cursor: Option<&DispatchOutboxCursor>,
        limit: usize,
    ) -> EngineResult<Vec<DispatchOutboxScanEntry>> {
        MaterializedStore::scan_dispatch_outbox_entries(self, partition_id, cursor, limit).await
    }

    async fn delete_dispatch_intent(&self, token: &OutboxEntryToken) -> EngineResult<()> {
        MaterializedStore::delete_dispatch_outbox_entry(self, token).await
    }

    async fn load_dispatch_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }

    async fn ensure_pending_deployment_run(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        run_id: &str,
    ) -> EngineResult<()> {
        MaterializedStore::put_pending_deployment_priority(
            self,
            project_id,
            deployment_id,
            priority,
            ready_at_ms,
            created_at_ms,
            run_id,
        )
        .await
    }
}

/// Consumer-owned port for partition-scoped terminal-obligation recovery.
#[async_trait]
pub trait TerminalObligationStore: Send + Sync {
    async fn scan_terminal_obligations(
        &self,
        partition_id: u32,
        limit: usize,
        start_after: Option<TerminalObligationCursor>,
    ) -> EngineResult<Vec<TerminalObligationEntry>>;

    async fn load_terminal_obligation_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> TerminalObligationStore for T {
    async fn scan_terminal_obligations(
        &self,
        partition_id: u32,
        limit: usize,
        start_after: Option<TerminalObligationCursor>,
    ) -> EngineResult<Vec<TerminalObligationEntry>> {
        MaterializedStore::list_terminal_obligations(self, partition_id, limit, start_after).await
    }

    async fn load_terminal_obligation_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }
}

/// Consumer-owned port for durable run-archive intent draining.
#[async_trait]
pub trait ArchiveOutboxStore: Send + Sync {
    async fn scan_archive_intents(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<ArchiveOutboxScanEntry>>;

    async fn quarantine_archive_intent(&self, token: &OutboxEntryToken) -> EngineResult<()>;
}

/// Consumer-owned port for the independent V2 shadow archive lane.
#[async_trait]
pub trait QueryArchiveOutboxStore: Send + Sync {
    async fn scan_query_archive_intents(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<QueryArchiveOutboxScanEntry>>;

    /// Visit at most `limit` pending rows, advancing a persistent partition cursor.
    /// Reserve due attempts with persisted capped backoff before returning them.
    /// An empty result can mean the visited page is deferred, rather than an empty queue.
    async fn claim_query_archive_intents(
        &self,
        partition_id: u32,
        now_ms: u64,
        limit: usize,
    ) -> EngineResult<QueryArchiveOutboxBatch>;

    async fn quarantine_query_archive_intent(&self, token: &OutboxEntryToken) -> EngineResult<()>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> QueryArchiveOutboxStore for T {
    async fn scan_query_archive_intents(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<QueryArchiveOutboxScanEntry>> {
        MaterializedStore::scan_query_archive_outbox_entries(self, partition_id, limit).await
    }

    async fn claim_query_archive_intents(
        &self,
        partition_id: u32,
        now_ms: u64,
        limit: usize,
    ) -> EngineResult<QueryArchiveOutboxBatch> {
        MaterializedStore::claim_query_archive_outbox_entries(self, partition_id, now_ms, limit)
            .await
    }

    async fn quarantine_query_archive_intent(&self, token: &OutboxEntryToken) -> EngineResult<()> {
        MaterializedStore::delete_query_archive_outbox_entry(self, token).await
    }
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> ArchiveOutboxStore for T {
    async fn scan_archive_intents(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<ArchiveOutboxScanEntry>> {
        MaterializedStore::scan_archive_outbox_entries(self, partition_id, limit).await
    }

    async fn quarantine_archive_intent(&self, token: &OutboxEntryToken) -> EngineResult<()> {
        MaterializedStore::delete_archive_outbox_entry(self, token).await
    }
}

/// Consumer-owned port for timer scheduling scans.
#[async_trait]
pub trait TimerWorkStore: Send + Sync {
    async fn scan_due_timers(&self, now_ns: i64, limit: usize) -> EngineResult<Vec<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> TimerWorkStore for T {
    async fn scan_due_timers(&self, now_ns: i64, limit: usize) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_due_timers(self, now_ns, limit).await
    }
}

/// Position in the deployment pending index. Orders exactly as the index
/// scans: priority, then ready time, then creation time, then run id.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingRunCursor {
    pub priority: u8,
    pub ready_at_ms: i64,
    pub created_at_ms: i64,
    pub run_id: String,
}

/// One pending index row together with the projected run it points at.
/// `data` is `None` when the run projection is missing, which a poller
/// treats the same as a stale index row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRunCandidate {
    pub cursor: PendingRunCursor,
    pub data: Option<Vec<u8>>,
}

/// Unique logical row in the route- and partition-aware pending index.
///
/// Physical adapters order this identity by partition, demand key, and rank.
/// Because `run_id` terminates the rank, a pending run has exactly one key and
/// repeated projection is an idempotent upsert.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingDemandRow {
    pub partition_id: u32,
    pub demand_key: DemandKey,
    pub rank: PendingRunCursor,
}

impl PendingDemandRow {
    /// Rebuild one demand-index row from the canonical pending Run projection.
    ///
    /// The live projector and migration path must preserve the same priority
    /// and readiness rules. Persisted pending Runs always carry
    /// `submitted_at`; rejecting a missing timestamp prevents a migration from
    /// silently inventing a different rank without the source journal record.
    pub fn from_projected_run(run: &Run, partition_count: usize) -> EngineResult<Self> {
        if !(1..=crate::partition_offset::MAX_PARTITION_COUNT).contains(&partition_count) {
            return Err(EngineError::InvalidArgument(format!(
                "partition count must be in 1..={}, got {partition_count}",
                crate::partition_offset::MAX_PARTITION_COUNT
            )));
        }
        let demand_key = DemandKey::from_run(run).map_err(|error| {
            EngineError::Storage(format!(
                "pending run {} has an invalid demand key: {error}",
                run.run_id
            ))
        })?;
        if run.run_id.is_empty() {
            return Err(EngineError::Storage(
                "pending run has an empty run_id".to_string(),
            ));
        }
        let priority = match run.priority {
            0 => engine_proto::Priority::Normal as u8,
            1..=3 => run.priority as u8,
            value => {
                return Err(EngineError::Storage(format!(
                    "pending run {} has invalid priority {value}",
                    run.run_id
                )))
            }
        };
        let submitted_at = run.submitted_at.as_ref().ok_or_else(|| {
            EngineError::Storage(format!(
                "pending run {} has no submitted_at timestamp",
                run.run_id
            ))
        })?;
        let created_at_ms = submitted_at
            .seconds
            .saturating_mul(1_000)
            .saturating_add(i64::from(submitted_at.nanos) / 1_000_000);
        let ready_at_ms = run
            .metadata
            .get("ready_at_ms")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(created_at_ms)
            .max(created_at_ms);
        Ok(Self {
            partition_id: crate::routing::partition_for_run(&run.run_id, partition_count) as u32,
            demand_key,
            rank: PendingRunCursor {
                priority,
                ready_at_ms,
                created_at_ms,
                run_id: run.run_id.clone(),
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PendingDemandIndexOutcome {
    pub rebuilt: bool,
    pub rows: usize,
}

/// Startup-only administrative port for the derived demand index.
///
/// Implementations rebuild from the canonical pending-Run projection and
/// persist a completion marker only after every row is durable. Repeating an
/// interrupted or completed call is safe. Runtimes must complete this gate
/// before enabling demand-index reads or starting projection writers.
#[async_trait]
pub trait PendingDemandIndexAdmin: Send + Sync {
    async fn ensure_pending_demand_index(
        &self,
        partition_count: usize,
    ) -> EngineResult<PendingDemandIndexOutcome>;
}

/// One route-aware pending row joined to its projected Run bytes in the same
/// adapter round trip. Missing bytes expose a stale derived row for repair;
/// they never authorize an offer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingDemandCandidate {
    pub row: PendingDemandRow,
    pub data: Option<Vec<u8>>,
}

/// Bounded supply-head read for one owned partition and demand key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingDemandHead {
    /// Best due candidate under `PendingRunCursor` ordering, if any.
    pub ready: Option<PendingDemandCandidate>,
    /// Earliest future readiness time across the key, strictly after the
    /// caller's `now_ms`, regardless of priority.
    pub next_ready_at_ms: Option<i64>,
}

/// Consumer-owned read port for the Coordinator V2 pending index.
///
/// Implementations use a composite index ordered by:
/// `(partition_id, project_id, deployment_id, component_type,
/// component_name, priority, ready_at_ms, created_at_ms, run_id)`.
/// Priority uses the protobuf numeric order, where Interactive precedes
/// Normal and Batch. Unspecified priority is canonicalized to Normal before
/// insertion. Every operation is bounded by the fixed priority domain or the
/// explicit page limit; queue depth must not determine scan work.
#[async_trait]
pub trait DemandPendingStore: Send + Sync {
    /// Return the best due row and earliest future wake-up for one exact key.
    /// The ready row includes projected Run bytes from the same adapter round
    /// trip. This call must not walk an unbounded incompatible prefix.
    async fn read_pending_demand_head(
        &self,
        partition_id: u32,
        demand_key: &DemandKey,
        now_ms: i64,
    ) -> EngineResult<PendingDemandHead>;

    /// Return due candidates in rank order, strictly after `after` when it is
    /// present. Each row includes projected Run bytes from the same adapter
    /// round trip; missing bytes identify stale index rows. A new proposal
    /// starts with `after = None` so rows that become ready after an earlier
    /// point-in-time scan cannot be hidden by its cursor.
    async fn scan_pending_demand_candidates(
        &self,
        partition_id: u32,
        demand_key: &DemandKey,
        now_ms: i64,
        after: Option<&PendingRunCursor>,
        limit: usize,
    ) -> EngineResult<Vec<PendingDemandCandidate>>;
}

/// Consumer-owned port for durable pending work.
///
/// Projection commits own inserts and removals. Pull workers only receive the
/// ordered scans and run state required to claim work through the journal.
#[async_trait]
pub trait PendingWorkStore: Send + Sync {
    async fn scan_pending_runs(
        &self,
        project_id: &str,
        component_names: &[String],
        limit: usize,
    ) -> EngineResult<Vec<Vec<u8>>>;

    async fn scan_ready_deployment_runs(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<String>>;

    /// One page of ready deployment candidates with their projected run
    /// data, in `scan_ready_deployment_runs` order. Backends fetch the page
    /// and its run data in a single round trip. `after` resumes strictly past
    /// a row from an earlier page so a long unclaimable prefix cannot hide
    /// ready work behind the page limit.
    async fn scan_ready_deployment_candidates(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        after: Option<&PendingRunCursor>,
        limit: usize,
    ) -> EngineResult<Vec<PendingRunCandidate>>;

    async fn load_pending_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> PendingWorkStore for T {
    async fn scan_pending_runs(
        &self,
        project_id: &str,
        component_names: &[String],
        limit: usize,
    ) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_pending_runs(self, project_id, component_names, limit).await
    }

    async fn scan_ready_deployment_runs(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        MaterializedStore::list_pending_deployment_priority(
            self,
            project_id,
            deployment_id,
            now_ms,
            limit,
        )
        .await
    }

    async fn scan_ready_deployment_candidates(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        after: Option<&PendingRunCursor>,
        limit: usize,
    ) -> EngineResult<Vec<PendingRunCandidate>> {
        let page = MaterializedStore::list_pending_deployment_priority_page(
            self,
            project_id,
            deployment_id,
            now_ms,
            after,
            limit,
        )
        .await?;
        let mut candidates = Vec::with_capacity(page.len());
        for cursor in page {
            let data = MaterializedStore::get_run(self, project_id, &cursor.run_id).await?;
            candidates.push(PendingRunCandidate { cursor, data });
        }
        Ok(candidates)
    }

    async fn load_pending_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }
}

/// Consumer-owned port for durable lease recovery and expiry work.
///
/// Lease creation and release are semantic projection mutations. Coordinator
/// recovery and sweepers receive only ordered scans, exact-generation
/// quarantine, and the run state needed to validate a scanned lease.
#[async_trait]
pub trait LeaseWorkStore: Send + Sync {
    async fn scan_expired_leases(
        &self,
        now_ms: i64,
        limit: usize,
        start_after: Option<LeaseExpiryCursor>,
    ) -> EngineResult<Vec<LeaseExpiryCursor>>;

    async fn quarantine_expiry_entry(
        &self,
        expires_at_ms: i64,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<()>;

    async fn scan_worker_leases(&self, worker_id: &str) -> EngineResult<Vec<(String, String)>>;

    async fn scan_active_lease_runs(&self, limit: usize) -> EngineResult<Vec<(String, String)>>;

    async fn load_lease_run(&self, project_id: &str, run_id: &str)
        -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> LeaseWorkStore for T {
    async fn scan_expired_leases(
        &self,
        now_ms: i64,
        limit: usize,
        start_after: Option<LeaseExpiryCursor>,
    ) -> EngineResult<Vec<LeaseExpiryCursor>> {
        MaterializedStore::list_expired_leases(self, now_ms, limit, start_after).await
    }

    async fn quarantine_expiry_entry(
        &self,
        expires_at_ms: i64,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<()> {
        MaterializedStore::gc_expiry_index_entry(self, expires_at_ms, project_id, run_id).await
    }

    async fn scan_worker_leases(&self, worker_id: &str) -> EngineResult<Vec<(String, String)>> {
        MaterializedStore::list_worker_leases(self, worker_id).await
    }

    async fn scan_active_lease_runs(&self, limit: usize) -> EngineResult<Vec<(String, String)>> {
        MaterializedStore::list_active_leases(self, limit).await
    }

    async fn load_lease_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }
}

/// Managed run-retention maintenance port.
///
/// This is an optional managed-profile behavior: the PostgreSQL profile
/// does not need to imitate local RocksDB reclamation or S3 re-archive repair.
#[async_trait]
pub trait RunRetentionStore: Send + Sync {
    async fn scan_retention_candidates(
        &self,
        before_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<crate::types::RunsRetentionEntry>>;

    async fn load_retained_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_run_archive_path(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<String>>;

    /// Load the exact Storage V2 committed-segment receipt for one journal
    /// row version. A missing receipt cannot authorize query-lake-backed L0
    /// eviction.
    async fn load_query_archive_receipt(
        &self,
        project_id: &str,
        run_id: &str,
        row_version: u64,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn stage_legacy_rearchive(
        &self,
        partition_id: u32,
        project_id: &str,
        terminal_offset: u64,
        run_id: &str,
        hot_run: &[u8],
    ) -> EngineResult<()>;

    async fn prune_retention_candidates(
        &self,
        entries: Vec<crate::types::VerifiedRunsRetentionEntry>,
    ) -> EngineResult<crate::types::RunsRetentionPruneResult>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> RunRetentionStore for T {
    async fn scan_retention_candidates(
        &self,
        before_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<crate::types::RunsRetentionEntry>> {
        MaterializedStore::scan_runs_retention_rotating(self, before_ms, limit).await
    }

    async fn load_retained_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }

    async fn load_run_archive_path(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<String>> {
        MaterializedStore::get_run_archive_path(self, project_id, run_id).await
    }

    async fn load_query_archive_receipt(
        &self,
        project_id: &str,
        run_id: &str,
        row_version: u64,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_query_archive_receipt(self, project_id, run_id, row_version).await
    }

    async fn stage_legacy_rearchive(
        &self,
        partition_id: u32,
        project_id: &str,
        terminal_offset: u64,
        run_id: &str,
        hot_run: &[u8],
    ) -> EngineResult<()> {
        MaterializedStore::stage_legacy_run_rearchive(
            self,
            partition_id,
            project_id,
            terminal_offset,
            run_id,
            hot_run,
        )
        .await
    }

    async fn prune_retention_candidates(
        &self,
        entries: Vec<crate::types::VerifiedRunsRetentionEntry>,
    ) -> EngineResult<crate::types::RunsRetentionPruneResult> {
        MaterializedStore::prune_query_lake_runs_retention(self, entries).await
    }
}

/// State reads required to deterministically project the next journal record.
///
/// This port is intentionally separate from user-facing queries. A processor
/// receives only the current serialized aggregate states it may fold with the
/// authoritative journal record.
#[async_trait]
pub trait ProjectionStateReader: Send + Sync {
    async fn load_projection_event_configuration(
        &self,
        _partition_id: u32,
        _project_id: &str,
    ) -> EngineResult<Option<crate::runtime_work::RuntimeWork>> {
        Ok(None)
    }

    async fn load_projection_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_activation(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_signal(
        &self,
        project_id: &str,
        signal_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_timer(
        &self,
        project_id: &str,
        timer_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_job(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_batch(
        &self,
        project_id: &str,
        batch_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn load_projection_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> ProjectionStateReader for T {
    async fn load_projection_event_configuration(
        &self,
        partition_id: u32,
        project_id: &str,
    ) -> EngineResult<Option<crate::runtime_work::RuntimeWork>> {
        MaterializedStore::runtime_event_configuration(self, partition_id, project_id).await
    }

    async fn load_projection_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }

    async fn load_projection_activation(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(MaterializedStore::get_entity_state(
            self,
            project_id,
            ACTIVATION_PROJECTION_ENTITY_TYPE,
            activation_id,
            ACTIVATION_PROJECTION_SCOPE,
            run_id,
        )
        .await?
        .map(|(data, _version)| data))
    }

    async fn load_projection_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_approval(self, project_id, approval_id).await
    }

    async fn load_projection_signal(
        &self,
        project_id: &str,
        signal_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_signal(self, project_id, signal_id).await
    }

    async fn load_projection_timer(
        &self,
        project_id: &str,
        timer_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_timer(self, project_id, timer_id).await
    }

    async fn load_projection_job(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_job(self, project_id, job_id).await
    }

    async fn load_projection_batch(
        &self,
        project_id: &str,
        batch_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_batch(self, project_id, batch_id).await
    }

    async fn load_projection_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_session(self, project_id, session_id).await
    }

    async fn load_projection_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_message(self, project_id, message_id).await
    }
}

/// Processor checkpoint persistence used by startup, readiness, and rollback
/// compatibility paths.
///
/// Normal projection checkpoints are committed through [`ProjectionCommitter`]
/// in the same atomic write as their projection mutations. This narrow port
/// remains for startup inspection, legacy checkpoint adoption, explicit
/// repair, and the final-shutdown compatibility write.
#[async_trait]
pub trait ProjectionCheckpointStore: Send + Sync {
    async fn load_projection_checkpoint(&self, key: &str) -> EngineResult<Option<Offset>>;

    async fn store_projection_checkpoint(&self, key: &str, offset: Offset) -> EngineResult<()>;
}

/// Select the exact projection resume position without allowing the periodic
/// compatibility checkpoint (`P`) to outrun the exact applied position (`M`).
pub fn select_projection_resume_checkpoint(
    periodic_checkpoint: Option<Offset>,
    exact_applied: Option<Offset>,
) -> EngineResult<Option<Offset>> {
    if let (Some(periodic), Some(exact)) = (periodic_checkpoint, exact_applied) {
        if periodic > exact {
            return Err(EngineError::Storage(format!(
                "periodic projection checkpoint {} is ahead of exact applied position {}",
                periodic.0, exact.0
            )));
        }
    }
    Ok(exact_applied.or(periodic_checkpoint))
}

/// Select a managed commit-gated resume checkpoint from exact exclusive `M`.
/// Periodic `P` is only a consistency check and is never a fallback.
pub fn select_commit_gated_projection_resume_checkpoint(
    partition_id: u32,
    periodic_checkpoint: Option<Offset>,
    exact_applied_through: u64,
) -> EngineResult<Option<Offset>> {
    let exact_checkpoint = if exact_applied_through == 0 {
        None
    } else {
        Some(crate::partition_offset::encode_offset(
            partition_id as usize,
            exact_applied_through - 1,
        )?)
    };
    if let Some(periodic) = periodic_checkpoint {
        let Some(exact) = exact_checkpoint else {
            return Err(EngineError::Storage(format!(
                "periodic projection checkpoint {} exists while exact applied frontier M=0 for partition {partition_id}",
                periodic.0
            )));
        };
        if periodic > exact {
            return Err(EngineError::Storage(format!(
                "periodic projection checkpoint {} is ahead of exact applied position {} for partition {partition_id}",
                periodic.0, exact.0
            )));
        }
    }
    Ok(exact_checkpoint)
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> ProjectionCheckpointStore for T {
    async fn load_projection_checkpoint(&self, key: &str) -> EngineResult<Option<Offset>> {
        MaterializedStore::get_checkpoint(self, key).await
    }

    async fn store_projection_checkpoint(&self, key: &str, offset: Offset) -> EngineResult<()> {
        MaterializedStore::set_checkpoint(self, key, offset).await
    }
}

/// User-facing run and event query port.
#[async_trait]
pub trait RunQueryStore: Send + Sync {
    /// One coherent, bounded hot projection cut for analytical queries.
    async fn capture_query_snapshot(
        &self,
        project_id: &str,
        limits: &QuerySnapshotLimits,
    ) -> Result<QueryHotSnapshot, QuerySnapshotError> {
        let _ = (project_id, limits);
        Err(QuerySnapshotError::Unavailable(
            "adapter does not support bounded query snapshots".into(),
        ))
    }
    async fn load_run(&self, project_id: &str, run_id: &str) -> EngineResult<Option<Vec<u8>>>;

    /// Ordered by `(submitted_at_ms DESC, run_id DESC)` with an exclusive cursor.
    async fn scan_hot_run_ids(
        &self,
        workspace_id: &str,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<String>>;

    async fn scan_event_summaries(
        &self,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<(String, i64, Vec<u8>)>>;

    async fn load_run_events(
        &self,
        project_id: &str,
        run_id: &str,
        from_timestamp_ns: i64,
    ) -> EngineResult<Vec<Record>>;

    async fn scan_run_segments(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<crate::types::RunSegmentChunk>>;

    async fn load_query_archive_path(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<String>>;

    async fn load_step_memo(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> RunQueryStore for T {
    async fn capture_query_snapshot(
        &self,
        project_id: &str,
        limits: &QuerySnapshotLimits,
    ) -> Result<QueryHotSnapshot, QuerySnapshotError> {
        MaterializedStore::capture_query_snapshot(self, project_id, limits).await
    }
    async fn load_run(&self, project_id: &str, run_id: &str) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_run(self, project_id, run_id).await
    }

    async fn scan_hot_run_ids(
        &self,
        workspace_id: &str,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        MaterializedStore::list_hot_run_ids(self, workspace_id, project_id, cursor, limit).await
    }

    async fn scan_event_summaries(
        &self,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<(String, i64, Vec<u8>)>> {
        MaterializedStore::list_event_summaries(self, project_id, cursor, limit).await
    }

    async fn load_run_events(
        &self,
        project_id: &str,
        run_id: &str,
        from_timestamp_ns: i64,
    ) -> EngineResult<Vec<Record>> {
        MaterializedStore::get_run_events(self, project_id, run_id, from_timestamp_ns).await
    }

    async fn scan_run_segments(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<crate::types::RunSegmentChunk>> {
        MaterializedStore::list_run_segments(self, project_id, run_id).await
    }

    async fn load_query_archive_path(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<String>> {
        MaterializedStore::get_run_archive_path(self, project_id, run_id).await
    }

    async fn load_step_memo(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_step_memo(self, project_id, run_id, step_key).await
    }
}

/// Raw protobuf values copied from the same storage snapshot. Decoding and
/// payload verification happen in the isolated query worker.
#[derive(Default)]
pub struct QueryHotSnapshot {
    pub runs: Vec<Vec<u8>>,
    pub events: Vec<Vec<u8>>,
}

pub struct QuerySnapshotLimits {
    pub max_bytes: usize,
    pub max_rows: usize,
    pub tables: u32,
    pub deadline: std::time::Instant,
    pub cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl QuerySnapshotLimits {
    pub fn checkpoint(&self) -> Result<(), QuerySnapshotError> {
        if std::time::Instant::now() >= self.deadline
            || self.cancelled.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(QuerySnapshotError::Cancelled);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QuerySnapshotError {
    #[error("hot query snapshot source limit exceeded")]
    Limit,
    #[error("hot query snapshot cancelled or timed out")]
    Cancelled,
    #[error("hot query snapshot unavailable: {0}")]
    Unavailable(String),
}

/// Derived Durable Activation V1 query port. Correctness decisions must use
/// journal replay rather than this potentially lagging read model.
#[async_trait]
pub trait ActivationQueryStore: Send + Sync {
    async fn load_activation(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> ActivationQueryStore for T {
    async fn load_activation(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(MaterializedStore::get_entity_state(
            self,
            project_id,
            ACTIVATION_PROJECTION_ENTITY_TYPE,
            activation_id,
            ACTIVATION_PROJECTION_SCOPE,
            run_id,
        )
        .await?
        .map(|(data, _version)| data))
    }
}

/// User-facing workflow state and lifecycle-object query port.
#[async_trait]
pub trait WorkflowQueryStore: Send + Sync {
    async fn load_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> EngineResult<Option<(Vec<u8>, i64)>>;

    async fn load_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn scan_run_approvals(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    async fn load_signal(&self, project_id: &str, signal_id: &str)
        -> EngineResult<Option<Vec<u8>>>;

    async fn lookup_run_signal(
        &self,
        project_id: &str,
        run_id: &str,
        signal_name: &str,
        waiting_step: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn scan_run_signals(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>>;

    async fn load_timer(&self, project_id: &str, timer_id: &str) -> EngineResult<Option<Vec<u8>>>;

    async fn scan_run_timers(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>>;

    async fn load_job(&self, project_id: &str, job_id: &str) -> EngineResult<Option<Vec<u8>>>;

    async fn scan_run_jobs(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>>;

    async fn scan_batch_jobs(&self, project_id: &str, batch_id: &str)
        -> EngineResult<Vec<Vec<u8>>>;

    async fn load_batch(&self, project_id: &str, batch_id: &str) -> EngineResult<Option<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> WorkflowQueryStore for T {
    async fn load_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> EngineResult<Option<(Vec<u8>, i64)>> {
        MaterializedStore::get_entity_state(
            self,
            project_id,
            entity_type,
            entity_key,
            scope,
            scope_id,
        )
        .await
    }

    async fn load_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_approval(self, project_id, approval_id).await
    }

    async fn scan_run_approvals(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_approvals_by_run(self, project_id, run_id).await
    }

    async fn load_signal(
        &self,
        project_id: &str,
        signal_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_signal(self, project_id, signal_id).await
    }

    async fn lookup_run_signal(
        &self,
        project_id: &str,
        run_id: &str,
        signal_name: &str,
        waiting_step: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::lookup_signal(self, project_id, run_id, signal_name, waiting_step).await
    }

    async fn scan_run_signals(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_signals_by_run(self, project_id, run_id).await
    }

    async fn load_timer(&self, project_id: &str, timer_id: &str) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_timer(self, project_id, timer_id).await
    }

    async fn scan_run_timers(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_timers_by_run(self, project_id, run_id).await
    }

    async fn load_job(&self, project_id: &str, job_id: &str) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_job(self, project_id, job_id).await
    }

    async fn scan_run_jobs(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_jobs_by_run(self, project_id, run_id).await
    }

    async fn scan_batch_jobs(
        &self,
        project_id: &str,
        batch_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_jobs_by_batch(self, project_id, batch_id).await
    }

    async fn load_batch(&self, project_id: &str, batch_id: &str) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_batch(self, project_id, batch_id).await
    }
}

/// Immediate entity-state write-through used by the compare-and-set API.
///
/// The authoritative state change is appended to the journal first. This port
/// supplies only the local read-after-write projection update that follows
/// that successful append.
#[async_trait]
pub trait EntityStateWriteStore: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn store_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
        data: &[u8],
        version: i64,
    ) -> EngineResult<()>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> EntityStateWriteStore for T {
    async fn store_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
        data: &[u8],
        version: i64,
    ) -> EngineResult<()> {
        MaterializedStore::put_entity_state(
            self,
            project_id,
            entity_type,
            entity_key,
            scope,
            scope_id,
            data,
            version,
        )
        .await
    }
}

/// User-facing worker-session and message query port.
#[async_trait]
pub trait SessionQueryStore: Send + Sync {
    async fn load_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn scan_component_sessions(
        &self,
        project_id: &str,
        component_name: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    async fn load_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    async fn scan_correlation_messages(
        &self,
        project_id: &str,
        correlation_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> SessionQueryStore for T {
    async fn load_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_session(self, project_id, session_id).await
    }

    async fn scan_component_sessions(
        &self,
        project_id: &str,
        component_name: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_sessions_by_component(self, project_id, component_name).await
    }

    async fn load_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        MaterializedStore::get_message(self, project_id, message_id).await
    }

    async fn scan_correlation_messages(
        &self,
        project_id: &str,
        correlation_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        MaterializedStore::list_messages_by_correlation(self, project_id, correlation_id).await
    }
}

/// Read-only queue rollups used by runtime scheduling metrics.
#[async_trait]
pub trait QueueQueryStore: Send + Sync {
    async fn load_project_queue_counter(
        &self,
        project_id: &str,
    ) -> EngineResult<crate::types::QueueCounter>;

    async fn load_deployment_queue_counter(
        &self,
        project_id: &str,
        deployment_id: &str,
    ) -> EngineResult<crate::types::QueueCounter>;

    async fn load_priority_queue_counter(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
    ) -> EngineResult<crate::types::QueueCounter>;
}

#[async_trait]
impl<T: MaterializedStore + ?Sized> QueueQueryStore for T {
    async fn load_project_queue_counter(
        &self,
        project_id: &str,
    ) -> EngineResult<crate::types::QueueCounter> {
        MaterializedStore::get_queue_counter_project(self, project_id).await
    }

    async fn load_deployment_queue_counter(
        &self,
        project_id: &str,
        deployment_id: &str,
    ) -> EngineResult<crate::types::QueueCounter> {
        MaterializedStore::get_queue_counter_deployment(self, project_id, deployment_id).await
    }

    async fn load_priority_queue_counter(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
    ) -> EngineResult<crate::types::QueueCounter> {
        MaterializedStore::get_queue_counter_priority(self, project_id, deployment_id, priority)
            .await
    }
}

/// Optional behavior that a storage profile may advertise.
///
/// Journal append/tail and projection/query/work persistence are required
/// contracts and therefore are not capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageCapability {
    JournalRestore,
    JournalRetention,
    JournalEpochTransition,
    JournalColdTier,
    JournalReplication,
    RunArchive,
    QueryArchiveShadow,
    RuntimeWork,
    RoutingSnapshot,
}

/// Explicit optional capabilities supplied by one storage profile.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StorageCapabilities {
    supported: BTreeSet<StorageCapability>,
}

impl StorageCapabilities {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn with(mut self, capability: StorageCapability) -> Self {
        self.supported.insert(capability);
        self
    }

    pub fn supports(&self, capability: StorageCapability) -> bool {
        self.supported.contains(&capability)
    }

    pub fn iter(&self) -> impl Iterator<Item = StorageCapability> + '_ {
        self.supported.iter().copied()
    }
}

impl FromIterator<StorageCapability> for StorageCapabilities {
    fn from_iter<T: IntoIterator<Item = StorageCapability>>(iter: T) -> Self {
        Self {
            supported: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use engine_proto::ComponentType;
    use std::collections::HashMap;

    struct NoopCommitter;

    #[async_trait]
    impl ProjectionCommitter for NoopCommitter {
        async fn commit(&self, _commit: ProjectionCommit) -> EngineResult<ProjectionCommitOutcome> {
            Ok(ProjectionCommitOutcome::default())
        }
    }

    fn test_record() -> Record {
        Record {
            offset: Offset(99),
            project_id: "project-a".to_string(),
            run_id: "run-a".to_string(),
            event_type: "run.queued".to_string(),
            data: Bytes::from_static(b"{}"),
            timestamp_ns: 1,
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
    fn checkpoint_can_only_advance_to_the_commit_source() {
        let position = JournalPosition::new(3, Offset(42));
        let source = ProjectionSource::new(position, test_record());
        let commit = ProjectionCommit::new(source, vec![ProjectionMutation::PutEvent])
            .advancing_checkpoint();

        assert_eq!(commit.source().position(), position);
        assert_eq!(commit.source().position().partition_id(), 3);
        assert_eq!(commit.source().position().offset(), Offset(42));
        assert_eq!(commit.source().record().offset, Offset(99));
        assert_eq!(commit.checkpoint(), Some(position));
        assert_eq!(commit.mutations(), &[ProjectionMutation::PutEvent]);
    }

    #[test]
    fn synchronous_durability_is_explicit() {
        let source = ProjectionSource::new(JournalPosition::new(0, Offset::ZERO), test_record());
        let commit = ProjectionCommit::new(source, Vec::new())
            .with_durability(CommitDurability::Synchronous);

        assert_eq!(commit.durability(), CommitDurability::Synchronous);
        assert!(commit.checkpoint().is_none());
    }

    #[test]
    fn pending_demand_rows_have_stable_route_and_rank_identity() {
        let demand_key = DemandKey::new(
            "project-a",
            "deployment-a",
            ComponentType::Function,
            "summarize",
        )
        .unwrap();
        let interactive = PendingDemandRow {
            partition_id: 3,
            demand_key: demand_key.clone(),
            rank: PendingRunCursor {
                priority: engine_proto::Priority::Interactive as u8,
                ready_at_ms: 100,
                created_at_ms: 90,
                run_id: "run-a".into(),
            },
        };
        let batch = PendingDemandRow {
            partition_id: 3,
            demand_key,
            rank: PendingRunCursor {
                priority: engine_proto::Priority::Batch as u8,
                ready_at_ms: 1,
                created_at_ms: 1,
                run_id: "run-b".into(),
            },
        };

        assert!(interactive.rank < batch.rank);
        assert_ne!(interactive.rank.run_id, batch.rank.run_id);
        assert_eq!(interactive.demand_key, batch.demand_key);
    }

    #[test]
    fn pending_demand_rebuild_row_preserves_route_rank_and_partition() {
        let run = Run {
            project_id: "project-a".into(),
            deployment_id: "deployment-a".into(),
            component_type: "functions".into(),
            component_name: "summarize".into(),
            run_id: "run-a".into(),
            priority: engine_proto::Priority::Unspecified as i32,
            submitted_at: Some(Default::default()),
            metadata: HashMap::from([("ready_at_ms".into(), "25".into())]),
            ..Default::default()
        };

        let row = PendingDemandRow::from_projected_run(&run, 8).unwrap();
        assert_eq!(
            row.partition_id,
            crate::routing::partition_for_run("run-a", 8) as u32
        );
        assert_eq!(row.demand_key.component_type(), ComponentType::Function);
        assert_eq!(row.rank.priority, engine_proto::Priority::Normal as u8);
        assert_eq!(row.rank.created_at_ms, 0);
        assert_eq!(row.rank.ready_at_ms, 25);
    }

    #[test]
    fn pending_demand_rebuild_rejects_invented_rank_or_topology() {
        let run = Run {
            project_id: "project-a".into(),
            deployment_id: "deployment-a".into(),
            component_type: "function".into(),
            component_name: "summarize".into(),
            run_id: "run-a".into(),
            ..Default::default()
        };
        assert!(PendingDemandRow::from_projected_run(&run, 8).is_err());

        let run = Run {
            submitted_at: Some(Default::default()),
            ..run
        };
        assert!(PendingDemandRow::from_projected_run(&run, 0).is_err());
    }

    #[test]
    fn exact_progress_controls_resume_without_allowing_periodic_checkpoint_ahead() {
        assert_eq!(
            select_projection_resume_checkpoint(Some(Offset(8)), Some(Offset(10))).unwrap(),
            Some(Offset(10))
        );
        assert_eq!(
            select_projection_resume_checkpoint(Some(Offset(8)), None).unwrap(),
            Some(Offset(8))
        );
        assert_eq!(
            select_projection_resume_checkpoint(None, Some(Offset::ZERO)).unwrap(),
            Some(Offset::ZERO)
        );
        assert!(select_projection_resume_checkpoint(Some(Offset(11)), Some(Offset(10))).is_err());
    }

    #[test]
    fn commit_gated_resume_never_falls_back_to_periodic_progress() {
        assert_eq!(
            select_commit_gated_projection_resume_checkpoint(0, None, 0).unwrap(),
            None
        );
        assert!(select_commit_gated_projection_resume_checkpoint(0, Some(Offset(8)), 0).is_err());

        let exact = crate::partition_offset::encode_offset(2, 9).unwrap();
        assert_eq!(
            select_commit_gated_projection_resume_checkpoint(2, Some(exact), 10).unwrap(),
            Some(exact)
        );
        assert!(select_commit_gated_projection_resume_checkpoint(
            2,
            Some(crate::partition_offset::encode_offset(2, 10).unwrap()),
            10,
        )
        .is_err());
    }

    #[tokio::test]
    async fn projection_committer_is_object_safe() {
        let committer: &dyn ProjectionCommitter = &NoopCommitter;
        let source = ProjectionSource::new(JournalPosition::new(0, Offset::ZERO), test_record());

        committer
            .commit(ProjectionCommit::new(source, Vec::new()))
            .await
            .expect("commit through trait object");
    }

    #[test]
    fn capabilities_are_opt_in_and_iterable() {
        let capabilities = StorageCapabilities::none()
            .with(StorageCapability::RunArchive)
            .with(StorageCapability::JournalRetention)
            .with(StorageCapability::QueryArchiveShadow);

        assert!(capabilities.supports(StorageCapability::RunArchive));
        assert!(capabilities.supports(StorageCapability::JournalRetention));
        assert!(capabilities.supports(StorageCapability::QueryArchiveShadow));
        assert!(!capabilities.supports(StorageCapability::JournalReplication));
        assert_eq!(capabilities.iter().count(), 3);
    }

    #[test]
    fn archive_mutations_are_capability_gated() {
        let archive = ProjectionMutation::PutRunArchiveLocation {
            project_id: "project-a".to_string(),
            run_id: "run-a".to_string(),
            archive_location: "archive/runs".to_string(),
        };

        assert_eq!(
            archive.required_capability(),
            Some(StorageCapability::RunArchive)
        );
        assert_eq!(
            ProjectionMutation::PutRun(Vec::new()).required_capability(),
            None
        );

        let source = ProjectionSource::new(JournalPosition::new(0, Offset::ZERO), test_record());
        let commit = ProjectionCommit::new(source, vec![archive]);
        assert!(commit
            .validate_capabilities(&StorageCapabilities::none())
            .is_err());
        assert!(commit
            .validate_capabilities(&StorageCapabilities::none().with(StorageCapability::RunArchive))
            .is_ok());
    }

    #[test]
    fn query_archive_mutations_are_independently_gated_and_default_off() {
        let source = ProjectionSource::new(JournalPosition::new(0, Offset::ZERO), test_record());
        let commit = ProjectionCommit::new(
            source,
            vec![
                ProjectionMutation::PutRun(Vec::new()),
                ProjectionMutation::PutRunArchiveIntent {
                    partition_id: 0,
                    terminal_offset: 99,
                    run_data: Vec::new(),
                },
                ProjectionMutation::PutRunQueryArchiveIntent {
                    partition_id: 0,
                    terminal_offset: 99,
                    run_data: Vec::new(),
                },
            ],
        );

        let current_only = commit
            .clone()
            .for_capabilities(&StorageCapabilities::none().with(StorageCapability::RunArchive));
        assert!(current_only
            .mutations()
            .iter()
            .any(|mutation| matches!(mutation, ProjectionMutation::PutRun(_))));
        assert!(current_only
            .mutations()
            .iter()
            .any(|mutation| matches!(mutation, ProjectionMutation::PutRunArchiveIntent { .. })));
        assert!(!current_only.mutations().iter().any(|mutation| matches!(
            mutation,
            ProjectionMutation::PutRunQueryArchiveIntent { .. }
        )));

        assert!(commit
            .validate_capabilities(&StorageCapabilities::none().with(StorageCapability::RunArchive))
            .is_err());
        assert!(commit
            .validate_capabilities(
                &StorageCapabilities::none()
                    .with(StorageCapability::RunArchive)
                    .with(StorageCapability::QueryArchiveShadow),
            )
            .is_ok());
    }
}
