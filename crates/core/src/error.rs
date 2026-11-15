use thiserror::Error;

/// Stable Durable Activation V1 errors shared by the service and adapters.
#[derive(Clone, Debug, Error)]
pub enum ActivationError {
    /// One logical key was reused with different input, definition, or kind.
    #[error("nondeterministic replay for activation {activation_id}: {reason}")]
    NondeterministicReplay {
        activation_id: String,
        reason: String,
    },

    /// The caller no longer owns the current attempt/fence authority.
    #[error(
        "stale activation authority for {activation_id}: request attempt={request_attempt}, current attempt={current_attempt}"
    )]
    StaleAuthority {
        activation_id: String,
        request_attempt: u32,
        current_attempt: u32,
    },

    /// A correctness write may have committed, so success cannot be inferred.
    #[error("unknown activation write outcome for {activation_id}: {reason}")]
    UnknownWriteOutcome {
        activation_id: String,
        reason: String,
    },

    /// The same logical transition was retried with different canonical bytes.
    #[error("activation payload conflict for {activation_id} attempt {attempt}")]
    PayloadConflict { activation_id: String, attempt: u32 },

    /// An accepted completion cannot apply a state mutation at the requested version.
    #[error(
        "activation state version conflict for {activation_id} attempt {attempt} at {target}: expected {expected_version}, current {current_version}"
    )]
    StateVersionConflict {
        activation_id: String,
        attempt: u32,
        target: String,
        expected_version: i64,
        current_version: i64,
    },

    /// A successful parent transition was attempted before one required child settled.
    #[error(
        "required child activation {child_activation_id} is unresolved for parent {activation_id}"
    )]
    RequiredChildUnresolved {
        activation_id: String,
        child_activation_id: String,
    },

    /// A command is not legal from the aggregate's current durable state.
    #[error("illegal activation transition for {activation_id}: {reason}")]
    IllegalTransition {
        activation_id: String,
        reason: String,
    },

    /// The activation request violates the frozen V1 contract.
    #[error("invalid activation argument: {0}")]
    InvalidArgument(String),

    /// Inline activation data exceeded the frozen V1 limit.
    #[error("activation inline payload exceeds {limit} bytes (got {actual})")]
    InlinePayloadTooLarge { limit: usize, actual: usize },

    /// The immutable reference is incomplete or not scoped to the request project.
    #[error("activation immutable reference is invalid: {0}")]
    InvalidReference(String),
}

/// Details for a replicated Store overlap whose local and leader records differ.
/// Boxed inside [`EngineError`] because integrity conflicts are rare and the
/// diagnostic strings should not inflate every engine error result.
#[derive(Clone, Debug, Error)]
#[error(
    "replication overlap conflict: partition={partition_id} offset={offset} expected_digest_v1={expected_digest} actual_digest_v1={actual_digest} differing_fields={differing_fields} leader={leader_id} replica={local_replica_id} epoch={epoch}"
)]
pub struct OverlapConflictError {
    pub partition_id: u32,
    pub offset: u64,
    pub expected_digest: String,
    pub actual_digest: String,
    pub differing_fields: String,
    pub leader_id: String,
    pub local_replica_id: String,
    pub epoch: u64,
}

/// Details for a retained local journal range that differs from the committed
/// cold-tier copy. No record payloads or metadata values are included.
#[derive(Clone, Debug, Error)]
#[error(
    "local journal diverges from committed cold history: partition={partition_id} retained_range=[{range_start}..{range_end_exclusive}) local_chain_digest_v1={local_digest_v1} cold_chain_digest_v1={cold_digest_v1}; fail-closed, no data changed"
)]
pub struct ColdJournalDivergenceError {
    pub partition_id: u32,
    pub range_start: u64,
    pub range_end_exclusive: u64,
    pub local_digest_v1: String,
    pub cold_digest_v1: String,
}

/// A partition has been durably fenced after detecting an integrity failure.
#[derive(Clone, Debug, Error)]
#[error(
    "partition integrity fenced: partition={partition_id} failure={failure_type} offset={offset} epoch={epoch}"
)]
pub struct IntegrityFencedError {
    pub partition_id: u32,
    pub failure_type: String,
    pub offset: u64,
    pub epoch: u64,
}

/// Errors produced by the AGNT5 Engine.
#[derive(Clone, Debug, Error)]
pub enum EngineError {
    /// The loglet segment has been sealed; no further appends allowed.
    #[error("segment sealed")]
    Sealed,

    /// The offset is out of range (trimmed or not yet written).
    #[error("offset {0} out of range")]
    OffsetOutOfRange(u64),

    /// The requested journal offset was durably trimmed from the hot tier.
    #[error(
        "offset {requested} is below retained floor {retained_floor}; restart through committed cold history"
    )]
    Trimmed { requested: u64, retained_floor: u64 },

    /// A supposedly contiguous journal range contains a missing offset.
    #[error("journal gap at offset {expected}; next stored offset is {found:?}")]
    Gap { expected: u64, found: Option<u64> },

    /// Caller supplied an invalid request.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A caller idempotency key was already accepted for different invocation data.
    #[error("invocation idempotency key conflicts with the accepted run {run_id}")]
    InvocationConflict { run_id: String },

    /// A lease-fenced record does not match the lease the journal has accepted.
    /// Decided at append time by the authority index; nothing was written.
    #[error("lease fence mismatch for run {run_id}: {reason}")]
    LeaseFenceMismatch { run_id: String, reason: String },

    /// Storage backend error (RocksDB, S3, etc.).
    #[error("storage error: {0}")]
    Storage(String),

    /// Record serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Persisted bytes use a format this process cannot safely read.
    #[error(
        "unsupported {domain} storage version {persisted}; readable range is {min_readable}..={max_readable}, writer version is {writer}"
    )]
    UnsupportedStorageVersion {
        domain: &'static str,
        persisted: u32,
        min_readable: u32,
        max_readable: u32,
        writer: u32,
    },

    /// The operation was cancelled (e.g., shutdown in progress).
    #[error("operation cancelled")]
    Cancelled,

    /// A stable Durable Activation V1 protocol error.
    #[error(transparent)]
    Activation(#[from] ActivationError),

    /// Request used an epoch older than the partition's current epoch.
    #[error(
        "stale epoch: partition={partition_id} request={request_epoch} current={current_epoch} owner={current_owner:?}"
    )]
    StaleEpoch {
        partition_id: u32,
        request_epoch: u64,
        current_epoch: u64,
        current_owner: Option<String>,
    },

    /// Request used an epoch this node has not accepted.
    #[error(
        "epoch ahead: partition={partition_id} request={request_epoch} current={current_epoch}"
    )]
    EpochAhead {
        partition_id: u32,
        request_epoch: u64,
        current_epoch: u64,
    },

    /// Write reached a node that is not the active partition owner.
    #[error(
        "not partition owner: partition={partition_id} owner={current_owner:?} epoch={current_epoch} state={lifecycle_state}"
    )]
    NotPartitionOwner {
        partition_id: u32,
        current_owner: Option<String>,
        current_epoch: u64,
        lifecycle_state: String,
    },

    /// Partition lifecycle currently rejects writes.
    #[error(
        "partition not writable: partition={partition_id} epoch={current_epoch} state={lifecycle_state} reason={reason:?}"
    )]
    PartitionNotWritable {
        partition_id: u32,
        current_epoch: u64,
        lifecycle_state: String,
        reason: Option<String>,
    },

    /// A leader Store batch reused an offset for a different record.
    #[error(transparent)]
    OverlapConflict(Box<OverlapConflictError>),

    /// Retained local journal bytes differ from committed cold history.
    #[error(transparent)]
    ColdJournalDivergence(Box<ColdJournalDivergenceError>),

    /// A local offset that should overlap a leader Store batch is unavailable.
    #[error(
        "replication overlap missing: partition={partition_id} offset={offset} leader={leader_id} replica={local_replica_id} epoch={epoch}"
    )]
    OverlapMissing {
        partition_id: u32,
        offset: u64,
        leader_id: String,
        local_replica_id: String,
        epoch: u64,
    },

    /// A prior replication integrity failure has disabled this partition.
    #[error(transparent)]
    IntegrityFenced(Box<IntegrityFencedError>),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type alias for engine operations.
pub type EngineResult<T> = Result<T, EngineError>;
