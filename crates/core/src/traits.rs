use async_trait::async_trait;
use tokio_stream::Stream;

use crate::activation::ActivationState;
use crate::activation_authority::{
    nonterminal_activation, replay_run_activation_states, required_child, RunLeaseEntry,
};
use crate::error::{EngineError, EngineResult};
use crate::types::{
    ArchivedRunReadRequest, ArchivedRunSeed, BatchWriteOps, Offset, QueueCounter, Record,
    RunSegmentChunk, TerminalObligationCursor, TerminalObligationEntry,
};

/// A stream of records from a tailing read.
pub type RecordStream = std::pin::Pin<Box<dyn Stream<Item = EngineResult<Record>> + Send>>;
pub type ArchivedRunSeedStream =
    std::pin::Pin<Box<dyn Stream<Item = EngineResult<ArchivedRunSeed>> + Send>>;

/// Canonical journal prefix currently safe for derived-state consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedPrefixEvidence {
    /// Exclusive partition-local boundary.
    pub through: Offset,
    /// Canonical digest of `[0, through)`; empty only when `through == 0`.
    pub digest_v1: String,
}

/// What a journal-index export produced.
///
/// A journal keeps lookups beside its records (idempotency keys, state
/// versions, run leases) that it checks before accepting a write. A cold
/// rebuild must refill them before it serves; an export lets it load them
/// up to an exact record instead of reading every record back from zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalIndexExportOutcome {
    /// The file holds exactly the indexes for records `[0, through)`, and
    /// `digest_v1` is the journal's canonical chain digest at `through`. An
    /// export with no entries writes no file.
    Exported {
        through: Offset,
        digest_v1: String,
        entries: u64,
        size_bytes: u64,
        sha256: String,
    },
    /// The journal trimmed between the caller reading its floor and the
    /// export's point-in-time view. Retry with the new floor.
    FloorMoved { observed: Offset },
    /// These indexes may be missing entries, so exporting them would spread
    /// the gap to every node rebuilt from the file.
    Incomplete { reason: &'static str },
}

/// What a journal-index import did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalIndexImportOutcome {
    /// The indexes for `[0, through)` are loaded; the fold resumes at `through`.
    Imported { entries: u64 },
    /// The file was unusable. Nothing it held remains, and the journal is
    /// exactly as it was, so the caller can try an older export or fold
    /// from zero.
    Rejected { reason: String },
}

/// Result of a batch append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    /// Offset for each input record. Idempotent duplicates return the existing offset.
    pub offsets: Vec<Offset>,
    /// Whether the corresponding input record was newly written to durable storage.
    pub appended: Vec<bool>,
}

impl AppendOutcome {
    pub fn all_appended(offsets: Vec<Offset>) -> Self {
        let appended = vec![true; offsets.len()];
        Self { offsets, appended }
    }

    pub fn appended_count(&self) -> usize {
        self.appended.iter().filter(|&&v| v).count()
    }

    /// Require one offset and one appended flag for every input record.
    ///
    /// Batch consumers must validate before indexing or zipping these vectors;
    /// otherwise a malformed implementation or RPC response can silently
    /// truncate processing or manufacture default response slots.
    pub fn validate_cardinality(&self, expected: usize, context: &str) -> EngineResult<()> {
        if self.offsets.len() == expected && self.appended.len() == expected {
            return Ok(());
        }
        Err(EngineError::Internal(format!(
            "{context}: append outcome cardinality mismatch: expected {expected}, offsets={}, appended={}",
            self.offsets.len(),
            self.appended.len()
        )))
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod append_outcome_tests {
    use super::*;

    #[test]
    fn exact_cardinality_is_valid() {
        let outcome = AppendOutcome {
            offsets: vec![Offset(1), Offset(2)],
            appended: vec![true, false],
        };
        assert!(outcome.validate_cardinality(2, "test").is_ok());
    }

    #[test]
    fn offset_or_appended_cardinality_mismatch_is_internal_error() {
        for outcome in [
            AppendOutcome {
                offsets: vec![Offset(1)],
                appended: vec![true, false],
            },
            AppendOutcome {
                offsets: vec![Offset(1), Offset(2)],
                appended: vec![true],
            },
        ] {
            let error = outcome.validate_cardinality(2, "test").unwrap_err();
            assert!(matches!(error, EngineError::Internal(_)));
            assert!(error.to_string().contains("cardinality mismatch"));
        }
    }
}

/// Whether one partition's storage can serve.
///
/// Exists so a readiness verdict can name which partition is at fault instead
/// of collapsing to a single bool. The node-wide answer is derived from these,
/// never the other way round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReadiness {
    pub partition_id: u32,
    /// `None` when the partition can serve.
    pub failure: Option<String>,
}

impl PartitionReadiness {
    pub fn ready(partition_id: u32) -> Self {
        Self {
            partition_id,
            failure: None,
        }
    }

    pub fn failed(partition_id: u32, failure: impl Into<String>) -> Self {
        Self {
            partition_id,
            failure: Some(failure.into()),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.failure.is_none()
    }
}

/// Result from the journal's append-atomic invocation idempotency index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationLookup {
    Unsupported,
    Missing,
    /// The index contains a record outside this replica's committed prefix.
    Pending,
    /// Public offset in a router, partition-local offset in a plain segment.
    Found(Offset),
}

/// Segment is the pluggable storage segment interface for the journal log.
///
/// Each segment covers a contiguous range of offsets. Segments can be sealed
/// (no further appends) and trimmed (reclaim storage after compaction to S3).
///
/// Implementations:
/// - `MemorySegment`: In-memory, for testing (engine-segment crate)
/// - `RocksDBSegment`: RocksDB-backed, for production (engine-segment crate)
/// - `S3Segment`: S3/Parquet-backed, for cold reads (engine-segment crate, later)
#[async_trait]
pub trait Segment: Send + Sync + 'static {
    /// Exact append-time invocation index lookup. Absence is advisory: a
    /// concurrent append or another replica may already have accepted it.
    /// Only append can authorize a fresh invocation. Unknown adapters retain
    /// the complete-history lookup instead of inventing an empty result.
    async fn lookup_invocation(
        &self,
        _project_id: &str,
        _run_id: &str,
        _caller_key: &str,
    ) -> EngineResult<InvocationLookup> {
        Ok(InvocationLookup::Unsupported)
    }
    /// Number of independent local offset domains exposed by this segment.
    ///
    /// Plain segments have one domain. Partition routers override this and
    /// must use the partition-scoped APIs below for resumable reads.
    fn partition_count(&self) -> usize {
        1
    }

    /// Validate a public append batch before any filtering or durable work.
    ///
    /// Single-partition segments accept every batch. Routers override this to
    /// reject unsupported cross-partition atomic requests before mutation.
    fn validate_append_batch(&self, _records: &[Record]) -> EngineResult<()> {
        Ok(())
    }

    /// Identity of the on-disk journal instance backing this segment.
    ///
    /// `None` means the segment cannot name its own bytes: an in-memory
    /// segment, a read-only open of a journal that was never issued an id, or
    /// a wrapper that does not forward. Treat `None` as "no durable proof may
    /// be recorded against this journal", never as "any proof matches".
    async fn journal_instance_id(&self) -> EngineResult<Option<String>> {
        Ok(None)
    }

    /// Append a batch of records. Returns assigned offsets after durable write.
    async fn append_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>>;

    /// Append a batch and report which records were newly committed.
    async fn append_batch_with_outcome(&self, records: &[Record]) -> EngineResult<AppendOutcome> {
        self.append_batch(records)
            .await
            .map(AppendOutcome::all_appended)
    }

    /// Persist a batch the partition leader already accepted.
    ///
    /// Identical to `append_batch` except that lease fences fold in
    /// `AuthorityMode::TrustedHistory`: a follower must never reject a record
    /// the leader wrote, even when its own lease index was trimmed or never
    /// built for that run. Adapters without a lease index delegate.
    async fn append_replicated_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>> {
        self.append_batch(records).await
    }

    /// Load one journal-authoritative activation head. Production hot
    /// segments override this with their append-atomic authority index.
    async fn load_activation_authority(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<ActivationState>> {
        Ok(replay_run_activation_states(self, project_id, run_id)
            .await?
            .remove(activation_id))
    }

    /// Load the journal-authoritative lease fold for one run.
    ///
    /// Exact grant retries use this instead of a materialized projection so
    /// cache loss and leader changes still resolve to the accepted assignment.
    /// Replicated adapters must reject folds whose latest transition is not
    /// committed, and reject reads while the partition is integrity-fenced.
    async fn load_run_lease_authority(
        &self,
        _project_id: &str,
        _run_id: &str,
    ) -> EngineResult<Option<RunLeaseEntry>> {
        Ok(None)
    }

    /// Resolve an exact grant, optionally completing replication of its
    /// canonical pending assignment in the original leader epoch. Unlike
    /// load_run_lease_authority, this operation may drive a correctness write.
    async fn resolve_run_lease_authority(
        &self,
        project_id: &str,
        run_id: &str,
        _grant_id: &str,
        _request_fingerprint: &str,
        _leader_epoch: u64,
    ) -> EngineResult<Option<RunLeaseEntry>> {
        self.load_run_lease_authority(project_id, run_id).await
    }

    /// Return the lexicographically first unresolved required child for a run
    /// or parent activation.
    async fn first_unsettled_required_child(
        &self,
        project_id: &str,
        run_id: &str,
        parent_activation_id: Option<&str>,
    ) -> EngineResult<Option<String>> {
        Ok(replay_run_activation_states(self, project_id, run_id)
            .await?
            .into_values()
            .filter(|state| {
                required_child(state)
                    && nonterminal_activation(state)
                    && parent_activation_id
                        .is_none_or(|parent| state.identity.parent_activation_id == parent)
            })
            .map(|state| state.identity.activation_id)
            .min())
    }

    /// List only nonterminal activations for one run. Cancellation needs this
    /// bounded run-local set to create canonical receipts.
    async fn list_nonterminal_activation_authority(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<ActivationState>> {
        Ok(replay_run_activation_states(self, project_id, run_id)
            .await?
            .into_values()
            .filter(nonterminal_activation)
            .collect())
    }

    /// Restore an exact contiguous batch from an already-validated durable tier.
    ///
    /// This is a startup-recovery operation, not a public append path. The
    /// records retain their assigned offsets, and implementations must preserve
    /// lifecycle state such as a persisted seal.
    async fn restore_committed_batch(&self, _records: &[Record]) -> EngineResult<()> {
        Err(EngineError::Storage(
            "segment does not support committed-batch restoration".to_string(),
        ))
    }

    /// Exclusive boundary durably restored from verified committed cold records.
    /// This is a local lower bound for tail repair, not a quorum certificate.
    async fn restored_committed_tail_offset(&self) -> EngineResult<Offset> {
        Ok(Offset::ZERO)
    }

    /// Unreconstructed cold prefix [next, through) of a freshly bootstrapped
    /// journal. A pending prefix must fence appends until its validation indexes
    /// have been rebuilt from verified committed history.
    async fn cold_metadata_recovery_range(&self) -> EngineResult<Option<(Offset, Offset)>> {
        Ok(None)
    }

    /// Fold verified committed records into validation indexes only, preserving
    /// the hot journal's offset and retained floor. Progress and index updates
    /// must be one durable write. Never call this with speculative peer records.
    async fn restore_cold_metadata_batch(&self, _records: &[Record]) -> EngineResult<()> {
        Err(EngineError::Storage(
            "cold metadata recovery is unsupported".into(),
        ))
    }

    /// Write this journal's validation indexes, as of one point in time, to
    /// `out`. The caller passes the retained floor it read and the canonical
    /// digest at that floor, so the export can extend the digest over its own
    /// retained records up to the exact record its indexes end at.
    async fn export_journal_index(
        &self,
        _out: &std::path::Path,
        _retained_floor: Offset,
        _digest_at_floor: &str,
    ) -> EngineResult<JournalIndexExportOutcome> {
        Ok(JournalIndexExportOutcome::Incomplete {
            reason: "segment does not keep journal indexes",
        })
    }

    /// Load exported indexes for records `[0, through)` into a journal whose
    /// cold rebuild has not folded any record yet, then leave `[through, end)`
    /// for the fold. `file` is `None` for an export with no entries.
    async fn import_journal_index(
        &self,
        _file: Option<&std::path::Path>,
        _through: Offset,
    ) -> EngineResult<JournalIndexImportOutcome> {
        Ok(JournalIndexImportOutcome::Rejected {
            reason: "segment does not keep journal indexes".into(),
        })
    }

    /// Create a tailing read stream from the given offset.
    /// The stream yields records as they are appended (live tail).
    async fn tail(&self, from: Offset) -> EngineResult<RecordStream>;

    /// Create the tail consumed by derived-state processors.
    ///
    /// Standalone segments acknowledge their local append directly, so their
    /// normal tail is already safe to project. HA wrappers override this to
    /// withhold locally appended records until the quorum commit boundary has
    /// advanced past them. Projections must never observe an uncommitted
    /// suffix because election repair may later truncate and reuse its offsets.
    async fn committed_tail(&self, from: Offset) -> EngineResult<RecordStream> {
        self.tail(from).await
    }

    /// Tail one partition from a local (not partition-encoded) offset.
    async fn tail_partition(
        &self,
        partition: usize,
        from_local: Offset,
    ) -> EngineResult<RecordStream> {
        if partition != 0 {
            return Err(EngineError::InvalidArgument(format!(
                "partition {partition} out of range for single-partition segment"
            )));
        }
        self.tail(from_local).await
    }

    /// Read a specific range of records [from, to).
    async fn read_range(&self, from: Offset, to: Offset) -> EngineResult<Vec<Record>>;

    /// Read one partition's local offset range [from, to).
    async fn read_partition_range(
        &self,
        partition: usize,
        from_local: Offset,
        to_local: Offset,
    ) -> EngineResult<Vec<Record>> {
        if partition != 0 {
            return Err(EngineError::InvalidArgument(format!(
                "partition {partition} out of range for single-partition segment"
            )));
        }
        self.read_range(from_local, to_local).await
    }

    /// Get the current tail offset (one past the latest committed record).
    ///
    /// This is a resumable cursor only for single-partition segments. Call
    /// `partition_tail_offset` when `partition_count() > 1`.
    async fn tail_offset(&self) -> EngineResult<Offset>;

    /// One-past-the-last record safe for derived-state processors to consume.
    /// Plain segments commit on local append; HA wrappers override this with
    /// their durable quorum frontier.
    async fn committed_tail_offset(&self) -> EngineResult<Offset> {
        self.tail_offset().await
    }

    /// Return the canonical committed prefix and prove its bytes before a
    /// derived generation is admitted. Standalone segments commit on local
    /// append, so recomputing their current prefix is sufficient. HA wrappers
    /// override this method to compare against durable quorum evidence.
    async fn committed_prefix_evidence(&self) -> EngineResult<CommittedPrefixEvidence> {
        let through = self.committed_tail_offset().await?;
        let digest_v1 = if through == Offset::ZERO {
            String::new()
        } else {
            self.extend_commit_chain_digest_v1(Offset::ZERO, through, "")
                .await?
        };
        Ok(CommittedPrefixEvidence { through, digest_v1 })
    }

    /// Extend a canonical commit-chain digest across `[from, to)` without
    /// materializing the complete range. Storage implementations with native
    /// streaming readers override this method; the one-record fallback keeps
    /// memory bounded for adapters that only implement `read_range`.
    async fn extend_commit_chain_digest_v1(
        &self,
        from: Offset,
        to: Offset,
        previous_digest: &str,
    ) -> EngineResult<String> {
        if to < from {
            return Err(EngineError::InvalidArgument(format!(
                "invalid commit-digest range [{from}, {to})"
            )));
        }

        let mut cursor = from.0;
        let mut digest = previous_digest.to_string();
        while cursor < to.0 {
            let through = cursor
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("journal offset overflow".to_string()))?;
            let records = self.read_range(Offset(cursor), Offset(through)).await?;
            if records.len() != 1 || records[0].offset.0 != cursor {
                return Err(EngineError::Gap {
                    expected: cursor,
                    found: records.first().map(|record| record.offset.0),
                });
            }
            digest = crate::types::extend_commit_chain_digest_v1(&digest, &records);
            cursor = through;
        }
        Ok(digest)
    }

    /// Verify that local storage exactly matches a durable full-prefix commit
    /// digest. Election candidates call this before advertising authority.
    async fn verify_durable_prefix(
        &self,
        committed_through: Offset,
        committed_digest_v1: &str,
    ) -> EngineResult<()> {
        if committed_through == Offset::ZERO {
            return if committed_digest_v1.is_empty() {
                Ok(())
            } else {
                Err(EngineError::Storage(
                    "zero durable commit boundary has a non-empty digest".to_string(),
                ))
            };
        }
        if committed_digest_v1.is_empty() {
            return Err(EngineError::Storage(
                "non-zero durable commit boundary has no digest".to_string(),
            ));
        }

        let digest = self
            .extend_commit_chain_digest_v1(Offset::ZERO, committed_through, "")
            .await?;
        if digest == committed_digest_v1 {
            Ok(())
        } else {
            Err(EngineError::Storage(
                "durable commit digest does not match the local journal prefix".to_string(),
            ))
        }
    }

    /// First offset still readable from this segment's local retained tier.
    ///
    /// Segments that never trim may keep the default zero floor. Durable
    /// segments override this so a cross-tier reader can prove the cold/hot
    /// handoff before yielding any records.
    async fn retained_floor(&self) -> EngineResult<Offset> {
        Ok(Offset::ZERO)
    }

    /// First partition-local offset still readable for one partition.
    async fn partition_retained_floor(&self, partition: usize) -> EngineResult<Offset> {
        if partition != 0 {
            return Err(EngineError::InvalidArgument(format!(
                "partition {partition} out of range for single-partition segment"
            )));
        }
        self.retained_floor().await
    }

    /// Fail closed when this segment cannot prove its persisted read contract.
    ///
    /// Plain segments have no external readiness dependency. Composite
    /// segments override this and propagate the health of their backing tiers.
    ///
    /// Returns the *first* failure it meets, which is what a caller needing
    /// every partition wants. A caller deciding whether the node can serve
    /// anything at all wants [`Segment::partition_readiness`] instead.
    async fn check_readiness(&self) -> EngineResult<()> {
        Ok(())
    }

    /// Readiness of every partition, without one partition's failure standing
    /// for the node's.
    ///
    /// One fenced partition out of eight is a degraded node, not a dead one.
    /// `check_readiness` cannot express that: it stops at the first error, so
    /// a caller cannot tell "everything is broken" from "one thing is".
    ///
    /// The default reports this segment's single verdict across its declared
    /// partition count. Routers override it and ask each pipeline separately.
    async fn partition_readiness(&self) -> Vec<PartitionReadiness> {
        let failure = self
            .check_readiness()
            .await
            .err()
            .map(|error| error.to_string());
        (0..self.partition_count() as u32)
            .map(|partition_id| PartitionReadiness {
                partition_id,
                failure: failure.clone(),
            })
            .collect()
    }

    /// Get one partition's local tail offset.
    async fn partition_tail_offset(&self, partition: usize) -> EngineResult<Offset> {
        if partition != 0 {
            return Err(EngineError::InvalidArgument(format!(
                "partition {partition} out of range for single-partition segment"
            )));
        }
        self.tail_offset().await
    }

    /// Seal this segment at one lifecycle transition point shared with append
    /// and `prepare_for_new_epoch`.
    ///
    /// An append that owns the transition point first may commit; `seal`
    /// waits for it and returns only after the resulting tail is stable. Any
    /// append reaching the transition point after a successful seal returns
    /// `EngineError::Sealed`. Returns `Ok(())` if already sealed.
    async fn seal(&self) -> EngineResult<()>;

    /// The exclusive offset at which this segment stopped accepting writes.
    ///
    /// `None` means either "not sealed" or "sealed by a writer that predates
    /// the durable seal record". Callers that need to fence at exactly the
    /// seal boundary must treat the two the same way: an unknown boundary is
    /// not a boundary. Storage adapters that record it override this.
    async fn sealed_through(&self) -> EngineResult<Option<Offset>> {
        Ok(None)
    }

    /// Raise the recorded seal boundary to the quorum-established value.
    ///
    /// A segment is sealed locally *before* the quorum is asked, so fencing
    /// takes effect immediately — which means the boundary first recorded is
    /// this node's own tail. Once the quorum reports a higher tail, the true
    /// boundary is that one, and it must replace the local guess.
    ///
    /// Monotonic and idempotent: the boundary only ever rises. A node whose
    /// journal ends below the recorded boundary is not in error — it is
    /// behind, and the gap is exactly what it must catch up.
    async fn record_seal_boundary(&self, _boundary: Offset) -> EngineResult<()> {
        Ok(())
    }

    /// Durably record this partition's quorum commit certificate.
    ///
    /// `through` is the exclusive boundary every offset below which has a
    /// quorum certificate; `digest_v1` is the canonical chain digest at it.
    /// Monotonic — a lower boundary is refused, because moving a commit
    /// boundary backwards would un-commit acknowledged records.
    ///
    /// This lives on the journal rather than in the election store because it
    /// is storage truth, not election state: it survives the election being
    /// deleted (BOOT-604), and a partition's journal should describe itself
    /// completely — retained floor, next offset, seal boundary, commit
    /// boundary — from one RocksDB instance.
    ///
    /// **It deliberately does not reach the object store.** `Q` advances on
    /// every acknowledgement, it is a lower bound rather than a consensus
    /// fact, and it is reconstructible by catch-up, so a round trip per commit
    /// would cost the append path everything and buy nothing.
    async fn record_commit_certificate(
        &self,
        _through: Offset,
        _digest_v1: &str,
    ) -> EngineResult<()> {
        Err(EngineError::Storage(
            "this segment cannot store a commit certificate".to_string(),
        ))
    }

    /// Record that the committed prefix through `through` has been hashed and
    /// matched against `digest_v1`, so a later start resumes from there rather
    /// than from offset zero.
    ///
    /// Storing this on the journal is what makes it sound. BOOT-501 kept it in
    /// the election store and had to bind it to a journal instance id, because
    /// a proof about one journal's bytes could otherwise be read beside a
    /// different journal. A proof stored *in* the journal it describes cannot
    /// be, so the binding is the location and no id is needed.
    async fn record_verified_prefix(&self, _through: Offset, _digest_v1: &str) -> EngineResult<()> {
        Ok(())
    }

    /// The proof recorded by [`Segment::record_verified_prefix`].
    async fn load_verified_prefix(&self) -> EngineResult<Option<(Offset, String)>> {
        Ok(None)
    }

    /// Load the certificate recorded by [`Segment::record_commit_certificate`].
    ///
    /// `None` means this journal has never recorded one, which is how a store
    /// written before the move is recognised so it can be migrated once.
    async fn load_commit_certificate(&self) -> EngineResult<Option<(Offset, String)>> {
        Ok(None)
    }

    /// Trim records up to and including the given offset.
    /// Used after S3 compaction to reclaim local storage.
    async fn trim(&self, through: Offset) -> EngineResult<()>;

    /// Trim one partition through a local offset.
    async fn trim_partition(&self, partition: usize, through_local: Offset) -> EngineResult<()> {
        if partition != 0 {
            return Err(EngineError::InvalidArgument(format!(
                "partition {partition} out of range for single-partition segment"
            )));
        }
        self.trim(through_local).await
    }

    /// Remove an uncommitted suffix beginning at `from` and restore every
    /// append-derived index to its state at that exclusive commit boundary.
    ///
    /// Implementations must make record deletion, index rollback, and the new
    /// tail durable atomically. The default fails closed because deleting only
    /// record bodies would leave idempotency and activation authority corrupt.
    async fn truncate_uncommitted_tail(&self, from: Offset) -> EngineResult<()> {
        Err(EngineError::Storage(format!(
            "segment does not support atomic uncommitted-tail repair from {from}"
        )))
    }

    /// Prepare the segment for a new leadership epoch.
    ///
    /// Clears the sealed flag so the segment can accept writes under a new
    /// leader. Called on both the new leader and followers when accepting a
    /// new epoch after a leader transition. The old leader's quorum Seal RPC
    /// sets the sealed flag on all replicas; this clears it.
    ///
    /// This transition is serialized with append and seal. Implementations
    /// must not expose the unsealed state unless its persisted lifecycle
    /// update succeeds.
    ///
    /// NOTE: This is only correct because the current architecture uses one
    /// physical RocksDB per partition with logical segment boundaries via
    /// SegmentConfig. Epoch fencing (not the sealed flag) is the actual
    /// stale-writer safety boundary. If physical segment-directory rotation
    /// is added later, this method must NOT be used to unseal a truly
    /// finalized segment.
    async fn prepare_for_new_epoch(&self) -> EngineResult<()> {
        Ok(()) // default no-op for segments that don't need it
    }
}

/// MaterializedStore provides queryable projections built from the journal log.
///
/// The processor tails the log and writes to this store.
/// Read RPCs query this store (with record cache checked first).
#[async_trait]
pub trait MaterializedStore: Send + Sync + 'static {
    // --- Run state ---

    /// Get the current state of a run.
    async fn get_run(&self, project_id: &str, run_id: &str) -> EngineResult<Option<Vec<u8>>>;

    /// Upsert run state.
    async fn put_run(&self, project_id: &str, run_id: &str, data: &[u8]) -> EngineResult<()>;

    /// Scan the partition-owned terminal-obligation index. Values are
    /// serialized Run projections; keys remain adapter-owned.
    async fn list_terminal_obligations(
        &self,
        partition_id: u32,
        limit: usize,
        start_after: Option<TerminalObligationCursor>,
    ) -> EngineResult<Vec<TerminalObligationEntry>>;

    // --- Atomic batch write ---

    /// Apply a batch of low-level writes atomically.
    ///
    /// The projection layer encodes one record's worth of `StoreOp`s into a
    /// single `BatchWriteOps` and hands it here. The store must commit every
    /// op or none — archive correctness depends on it: the
    /// `CF_S3_OUTBOX` entry and the `CF_RUNS` row must both land or both
    /// roll back.
    async fn write_kv_batch(&self, batch: BatchWriteOps) -> EngineResult<()>;

    /// Apply a batch atomically and require it to be durable before returning.
    ///
    /// Cold-tier publication uses this to project an immutable S3 catalog
    /// commit into `CF_RUN_SEGMENTS` before trimming the local hot copy.
    /// Durable implementations should override this method with the store's
    /// synchronous-write option.
    async fn write_kv_batch_durable(&self, batch: BatchWriteOps) -> EngineResult<()> {
        self.write_kv_batch(batch).await
    }

    // --- S3 archive outbox ---

    /// Scan the durable archive outbox for one partition.
    ///
    /// Returns up to `limit` `(key_bytes, value_bytes)` pairs, ordered by
    /// `(project_id, terminal_offset)` ascending — drain processes oldest-
    /// terminal-first within a project, and projects iterate naturally
    /// because the schema key groups by project. Caller decodes the value
    /// (a serialized `Run` proto from `PutRunArchiveIntent`) and decodes the key
    /// for project_id / terminal_offset / run_id via the schema helpers.
    async fn list_s3_outbox(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Compatibility facade for the adapter-neutral archive-outbox port.
    async fn scan_archive_outbox_entries(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<crate::storage_contract::ArchiveOutboxScanEntry>>;

    /// Quarantine a corrupt archive intent by adapter-owned identity.
    async fn delete_archive_outbox_entry(
        &self,
        token: &crate::storage_contract::OutboxEntryToken,
    ) -> EngineResult<()>;

    // --- V2 shadow query archive ---

    /// Scan independent V2 shadow intents for one partition.
    async fn scan_query_archive_outbox_entries(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<crate::storage_contract::QueryArchiveOutboxScanEntry>> {
        let _ = (partition_id, limit);
        Err(crate::error::EngineError::Storage(
            "query archive outbox is not supported by this materialized-store adapter".to_string(),
        ))
    }

    /// Reserve due query-archive attempts with durable backoff and bounded scan progress.
    async fn claim_query_archive_outbox_entries(
        &self,
        partition_id: u32,
        now_ms: u64,
        limit: usize,
    ) -> EngineResult<crate::storage_contract::QueryArchiveOutboxBatch> {
        let _ = (partition_id, now_ms, limit);
        Err(crate::error::EngineError::Storage(
            "query archive retry selection is not supported by this materialized-store adapter"
                .to_string(),
        ))
    }

    /// Quarantine one malformed V2 shadow intent by adapter-owned identity.
    async fn delete_query_archive_outbox_entry(
        &self,
        token: &crate::storage_contract::OutboxEntryToken,
    ) -> EngineResult<()> {
        let _ = token;
        Err(crate::error::EngineError::Storage(
            "query archive quarantine is not supported by this materialized-store adapter"
                .to_string(),
        ))
    }

    /// Load exact committed-segment coverage for one Run row version.
    async fn get_query_archive_receipt(
        &self,
        project_id: &str,
        run_id: &str,
        row_version: u64,
    ) -> EngineResult<Option<Vec<u8>>> {
        let _ = (project_id, run_id, row_version);
        Err(crate::error::EngineError::Storage(
            "query archive receipts are not supported by this materialized-store adapter"
                .to_string(),
        ))
    }

    // --- Dispatch outbox ---

    /// Scan durable dispatch intents for one partition.
    ///
    /// Returns up to `limit` `(key_bytes, value_bytes)` pairs ordered by
    /// `(queued_offset, project_id, run_id)`. The value is a serialized
    /// `DispatchComponentRequest`; callers decode the key using store schema
    /// helpers to recover partition/project/queued_offset/run_id.
    async fn list_dispatch_outbox(
        &self,
        partition_id: u32,
        limit: usize,
    ) -> EngineResult<Vec<(Vec<u8>, Vec<u8>)>>;

    /// One page of a partition's pending queue, in key order, strictly after
    /// `after` when given. Adapters without a route-aware pending index have
    /// nothing to report.
    async fn scan_pending_queue_partition(
        &self,
        _partition_id: u32,
        _after: Option<&[u8]>,
        _limit: usize,
    ) -> EngineResult<Vec<crate::storage_contract::PendingQueueRow>> {
        Ok(Vec::new())
    }

    /// Compatibility facade for the adapter-neutral dispatch-outbox port.
    async fn scan_dispatch_outbox_entries(
        &self,
        partition_id: u32,
        cursor: Option<&crate::storage_contract::DispatchOutboxCursor>,
        limit: usize,
    ) -> EngineResult<Vec<crate::storage_contract::DispatchOutboxScanEntry>>;

    /// Delete or quarantine one dispatch row by adapter-owned identity.
    async fn delete_dispatch_outbox_entry(
        &self,
        token: &crate::storage_contract::OutboxEntryToken,
    ) -> EngineResult<()>;

    // --- Hot index ---

    /// Scan the local hot index for `run_id`s under
    /// `(workspace_id, project_id)`, newest-first. Up to `limit` rows.
    ///
    /// `cursor` is `(submitted_at_ms, run_id)` — the last row of the
    /// previous page. Order is `(submitted_at_ms DESC, run_id DESC)`;
    /// the scan resumes strictly after the cursor. `None` means "first page."
    ///
    /// The query layer calls this for the hot side
    /// of the hot ∪ cold merge. Caller fetches each Run via `get_run`
    /// — the index value is empty (key-only).
    async fn capture_query_snapshot(
        &self,
        _project_id: &str,
        _limits: &crate::storage_contract::QuerySnapshotLimits,
    ) -> Result<
        crate::storage_contract::QueryHotSnapshot,
        crate::storage_contract::QuerySnapshotError,
    > {
        Err(crate::storage_contract::QuerySnapshotError::Unavailable(
            "adapter does not support bounded query snapshots".into(),
        ))
    }

    async fn list_hot_run_ids(
        &self,
        workspace_id: &str,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<String>>;

    // --- Events hot index ---

    /// Scan the inbound-events hot index for a project, newest-first.
    /// Backs the Studio "Events" list. Returns up to `limit` rows of
    /// `(event_run_id, timestamp_ms, summary_json)` where `summary_json`
    /// is the compact value stored on the index row (see `CF_EVENTS_HOT`).
    ///
    /// `cursor` is `(timestamp_ms, event_run_id)` — the last row of the
    /// previous page; the scan resumes strictly past it. `None` = first
    /// page. The value carries enough to render the list without reading
    /// the full event payload from `CF_EVENTS`.
    async fn list_event_summaries(
        &self,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<(String, i64, Vec<u8>)>>;

    // --- Durable gossip routing state ---

    /// Upsert a gossip-fed routing entry into `CF_ROUTING_GOSSIP`. The
    /// in-memory `RoutingMap` is the read-side of record; this CF is
    /// only read at startup to rehydrate the map after a restart.
    async fn put_routing_gossip(
        &self,
        project_id: &str,
        worker_id: &str,
        data: &[u8],
    ) -> EngineResult<()>;

    /// Delete a gossip-fed routing entry from `CF_ROUTING_GOSSIP`.
    /// Called by the tombstone GC sweeper after `tombstone_at_ms`
    /// expiry — by then the entry is already gone from the in-memory
    /// `RoutingMap`.
    async fn delete_routing_gossip(&self, project_id: &str, worker_id: &str) -> EngineResult<()>;

    /// Stream every gossip routing entry currently persisted. Returns
    /// `(project_id, worker_id, encoded_entry_bytes)` tuples. Used at
    /// startup to rehydrate `RoutingMap` so dispatch starts warm
    /// without waiting on anti-entropy.
    async fn scan_routing_gossip(&self) -> EngineResult<Vec<(String, String, Vec<u8>)>>;

    // --- Event triggers (CP-synced cache) ---

    /// List trigger entries for an event in a deployment. Returns
    /// `(component_type, component_name, spec_bytes)` tuples; `spec_bytes`
    /// is the opaque payload the sync task wrote (proto-encoded TriggerSpec).
    /// Empty result means no component in this deployment subscribes to
    /// `event_name`.
    async fn list_event_triggers(
        &self,
        project_id: &str,
        deployment_id: &str,
        event_name: &str,
    ) -> EngineResult<Vec<(i32, String, Vec<u8>)>>;

    /// Replace the trigger set for `(project_id, deployment_id)` atomically.
    /// Any existing entries under this deployment are cleared, then `entries`
    /// are written. The sync sentinel is also stamped to `synced_at_ms` in
    /// the same batch so a reader can distinguish "synced, no triggers"
    /// from "never synced on this node." Used by the trigger-sync task to
    /// apply CP's view in one shot. `entries` is `(event_name,
    /// component_type, component_name, trigger_id, spec_bytes)`.
    async fn replace_deployment_triggers(
        &self,
        project_id: &str,
        deployment_id: &str,
        entries: Vec<(String, i32, String, String, Vec<u8>)>,
        synced_at_ms: i64,
    ) -> EngineResult<()>;

    /// Read the trigger-sync sentinel for `(project_id, deployment_id)`.
    /// Returns the unix-millisecond timestamp of the most recent successful
    /// sync, or `None` if this deployment has never been synced on this
    /// node. Callers use this to decide whether to trust an empty
    /// `list_event_triggers` result or to fall back to a different source.
    async fn get_trigger_sync_sentinel(
        &self,
        project_id: &str,
        deployment_id: &str,
    ) -> EngineResult<Option<i64>>;

    // --- Events by run ---

    /// Get all events for a run, ordered by timestamp_ns.
    async fn get_run_events(
        &self,
        project_id: &str,
        run_id: &str,
        from_timestamp_ns: i64,
    ) -> EngineResult<Vec<Record>>;

    /// Store a record in the events index.
    async fn put_event(&self, record: &Record) -> EngineResult<()>;

    // --- Run sealed-segment index ---

    /// List the S3 sealed-segment chunks that hold this run's records.
    ///
    /// Populated by the segment compactor on each Parquet PUT. Returns
    /// chunks in offset order. Empty Vec when the run has no records
    /// in any sealed segment yet (still entirely in the live CF_EVENTS
    /// or active segment).
    async fn list_run_segments(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<RunSegmentChunk>>;

    // --- Run archive pointer ---

    /// Return the S3 path of the archived-run Parquet that holds this
    /// run's full Run proto, if one exists. Populated by the
    /// `run.archived` projection. Used by the cold-`get_run`
    /// fallback when `CF_RUNS` no longer has the row after retention.
    ///
    /// Returns `Ok(None)` if the run is still in flight or its archive has no
    /// pointer; the caller falls through to "not found".
    async fn get_run_archive_path(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<String>>;

    /// Scan `CF_RUNS_RETENTION` forward from
    /// the beginning, returning up to `limit` entries whose
    /// `archived_at_ms < before_ms`. Each entry encodes the archived
    /// timestamp, project_id, and run_id in the key. The value is the exact
    /// archived-object generation that must still be current before deletion.
    ///
    async fn scan_runs_retention(
        &self,
        before_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<crate::types::RunsRetentionEntry>>;

    /// Scan eligible retention entries from a persisted rotating cursor.
    ///
    /// Unlike the deterministic oldest-first scan above, this method advances
    /// past unresolved candidates and wraps at the retention boundary. It is
    /// used by the background worker so a full page of fail-closed legacy or
    /// temporarily unavailable archives cannot permanently starve later rows.
    async fn scan_runs_retention_rotating(
        &self,
        before_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<crate::types::RunsRetentionEntry>>;

    /// Stage an idempotent archive retry and restore the hot query index for a
    /// legacy retention row. The adapter owns all physical key lowering.
    async fn stage_legacy_run_rearchive(
        &self,
        partition_id: u32,
        project_id: &str,
        terminal_offset: u64,
        run_id: &str,
        hot_run: &[u8],
    ) -> EngineResult<()>;

    /// Atomically reconcile scanned retention entries against their current
    /// per-run generation and cold catalog pointer. A hot row is deleted only
    /// when both still match the scanned generation. Legacy, malformed, and
    /// stale entries are cleaned without deleting the hot row.
    async fn prune_runs_retention(
        &self,
        entries: Vec<crate::types::VerifiedRunsRetentionEntry>,
    ) -> EngineResult<crate::types::RunsRetentionPruneResult>;

    /// Apply an exact Vortex proof, retaining the same atomic hot-row and
    /// generation checks while accepting legacy archive pointers as opaque.
    async fn prune_query_lake_runs_retention(
        &self,
        _entries: Vec<crate::types::VerifiedRunsRetentionEntry>,
    ) -> EngineResult<crate::types::RunsRetentionPruneResult> {
        Err(crate::error::EngineError::Storage(
            "query-lake retention is unsupported by this store".into(),
        ))
    }

    // --- Step memoization ---

    /// Look up a memoized step result.
    async fn get_step_memo(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    /// Store a memoized step result.
    async fn put_step_memo(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
        data: &[u8],
    ) -> EngineResult<()>;

    /// Applied event configuration at the current projection frontier. None
    /// means this partition has no configuration barrier for the project.
    async fn runtime_event_configuration(
        &self,
        _partition_id: u32,
        _project_id: &str,
    ) -> EngineResult<Option<crate::runtime_work::RuntimeWork>> {
        Ok(None)
    }

    /// Bounded partition scan of derived controller work. Unsupported profiles
    /// must reject commissioning rather than acknowledge unrecoverable work.
    async fn scan_runtime_work(
        &self,
        _partition_id: u32,
        _start_after: Option<&[u8]>,
        _limit: usize,
    ) -> EngineResult<crate::runtime_work::WorkPage> {
        Err(EngineError::InvalidArgument(
            "storage profile does not support runtime work".into(),
        ))
    }

    /// Retained native delivery snapshots for one source run, including terminal
    /// admission dispositions. This is a projection, not business completion.
    async fn scan_runtime_deliveries(
        &self,
        _project: &str,
        _run: &str,
        _start_after: Option<&[u8]>,
        _limit: usize,
    ) -> EngineResult<crate::runtime_work::WorkPage> {
        Err(EngineError::InvalidArgument(
            "storage profile does not support runtime delivery inspection".into(),
        ))
    }

    /// Bounded scan of terminal native delivery history eligible for retention.
    async fn scan_runtime_delivery_retention(
        &self,
        _partition_id: u32,
        _start_after: Option<&[u8]>,
        _limit: usize,
    ) -> EngineResult<crate::runtime_work::WorkPage> {
        Err(EngineError::InvalidArgument(
            "storage profile does not support delivery retention".into(),
        ))
    }

    // --- Entity state ---

    /// Get entity state by scoped key. Returns (state_bytes, version) or None.
    async fn get_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> EngineResult<Option<(Vec<u8>, i64)>>;

    /// Put entity state with version.
    #[allow(clippy::too_many_arguments)]
    async fn put_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
        data: &[u8],
        version: i64,
    ) -> EngineResult<()>;

    // --- Approvals ---

    async fn get_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
    async fn put_approval(
        &self,
        project_id: &str,
        approval_id: &str,
        run_id: &str,
        data: &[u8],
    ) -> EngineResult<()>;
    async fn list_approvals_by_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    // --- Signals ---

    async fn get_signal(&self, project_id: &str, signal_id: &str) -> EngineResult<Option<Vec<u8>>>;
    #[allow(clippy::too_many_arguments)]
    async fn put_signal(
        &self,
        project_id: &str,
        signal_id: &str,
        run_id: &str,
        signal_name: &str,
        waiting_step: &str,
        data: &[u8],
    ) -> EngineResult<()>;
    async fn lookup_signal(
        &self,
        project_id: &str,
        run_id: &str,
        signal_name: &str,
        waiting_step: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
    async fn list_signals_by_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    // --- Timers ---

    async fn get_timer(&self, project_id: &str, timer_id: &str) -> EngineResult<Option<Vec<u8>>>;
    #[allow(clippy::too_many_arguments)]
    async fn put_timer(
        &self,
        project_id: &str,
        timer_id: &str,
        run_id: &str,
        status: &str,
        fire_at_ns: i64,
        data: &[u8],
    ) -> EngineResult<()>;
    async fn list_timers_by_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;
    async fn list_due_timers(&self, now_ns: i64, limit: usize) -> EngineResult<Vec<Vec<u8>>>;

    // --- Jobs ---

    async fn get_job(&self, project_id: &str, job_id: &str) -> EngineResult<Option<Vec<u8>>>;
    async fn put_job(
        &self,
        project_id: &str,
        job_id: &str,
        run_id: &str,
        batch_id: &str,
        data: &[u8],
    ) -> EngineResult<()>;
    async fn list_jobs_by_run(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>>;
    async fn list_jobs_by_batch(
        &self,
        project_id: &str,
        batch_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    // --- Pending Runs (for pull workers) ---

    /// Add a run to the pending index (keyed by project + component for efficient polling).
    async fn put_pending_run(
        &self,
        project_id: &str,
        component_name: &str,
        run_id: &str,
        data: &[u8],
    ) -> EngineResult<()>;
    /// Remove a run from the pending index (on start/complete/fail/cancel).
    async fn remove_pending_run(
        &self,
        project_id: &str,
        component_name: &str,
        run_id: &str,
    ) -> EngineResult<()>;
    /// List pending runs for a set of components (returns serialized Run protos).
    async fn list_pending_runs(
        &self,
        project_id: &str,
        component_names: &[String],
        limit: usize,
    ) -> EngineResult<Vec<Vec<u8>>>;

    // --- Lease secondary indexes ---
    //
    // Lease state is stored on the Run proto. These indexes allow O(log N)
    // expiry sweeps and worker-scoped lookups without scanning all runs.

    /// Atomically insert a lease into CF_LEASES_BY_EXPIRY and
    /// CF_LEASES_BY_WORKER. Idempotent: overwriting with the same key
    /// produces no change.
    async fn put_lease_index(
        &self,
        project_id: &str,
        run_id: &str,
        expires_at_ms: i64,
        worker_id: &str,
    ) -> EngineResult<()>;

    /// Atomically delete a lease from both index CFs. The caller supplies
    /// the OLD `expires_at_ms` and `worker_id` from the existing Run proto
    /// so the exact keys can be located. Idempotent: deleting a missing
    /// key is a no-op.
    async fn remove_lease_index(
        &self,
        project_id: &str,
        run_id: &str,
        expires_at_ms: i64,
        worker_id: &str,
    ) -> EngineResult<()>;

    /// Range-scan CF_LEASES_BY_EXPIRY for leases expiring at or before
    /// `now_ms`, strictly after `start_after` when supplied. Returns stable
    /// expiry cursors bounded by `limit`. The caller loads the Run proto
    /// separately if it needs lease details. `expires_at_ms` is returned so
    /// the sweeper can reconstruct the exact index key for orphan GC when the
    /// Run's lease has already been released but the index entry survives.
    async fn list_expired_leases(
        &self,
        now_ms: i64,
        limit: usize,
        start_after: Option<crate::types::LeaseExpiryCursor>,
    ) -> EngineResult<Vec<crate::types::LeaseExpiryCursor>>;

    /// Delete a single `CF_LEASES_BY_EXPIRY` entry by exact key.
    ///
    /// Used by the lease expiry sweeper to GC orphan index entries —
    /// rows where the index survives but the Run proto no longer has
    /// an active lease (e.g. projection removal failed on replay, or a
    /// crash between append and batch-write). Does NOT touch
    /// `CF_LEASES_BY_WORKER` — the sweeper doesn't know worker_id from
    /// the expiry key, and the worker-index leak is invisible (not
    /// iterated every tick). Idempotent: missing key is a no-op.
    async fn gc_expiry_index_entry(
        &self,
        expires_at_ms: i64,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<()>;

    /// Prefix-scan CF_LEASES_BY_WORKER for all leases held by a worker.
    /// Returns `(project_id, run_id)` pairs. Used on worker disconnect for
    /// eager lease release.
    async fn list_worker_leases(&self, worker_id: &str) -> EngineResult<Vec<(String, String)>>;

    /// Range-scan CF_LEASES_BY_EXPIRY for ALL active leases regardless of
    /// expiry. Returns `(project_id, run_id)` pairs bounded by `limit`.
    ///
    /// This is the inverse of [`list_expired_leases`]: it walks the same
    /// CF without an upper bound, so callers can rebuild in-memory views
    /// of the full active-lease set on startup. The caller loads the Run
    /// proto separately to extract `lease_id` if it needs the fence token.
    async fn list_active_leases(&self, limit: usize) -> EngineResult<Vec<(String, String)>>;

    // --- Priority-aware pending index ---
    //
    // Parallel to the pending-runs CF but keyed by `(project, deployment,
    // component, priority, ready_at, created_at, run_id)` so the scheduler
    // can iterate highest-priority-first within a triple.
    //
    // Value is empty — the Run proto lookup goes through `get_run`.
    // This keeps the index lean: Run updates don't rewrite index entries.

    /// Insert a pending entry into the priority-aware pending index.
    /// Idempotent: overwriting with the same key produces no change.
    #[allow(clippy::too_many_arguments)]
    async fn put_pending_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        component_name: &str,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        run_id: &str,
    ) -> EngineResult<()>;

    /// Remove a pending entry from the priority-aware pending index.
    /// The caller must supply the same (priority, ready_at, created_at)
    /// values used on insert so the exact key can be located. Idempotent:
    /// deleting a missing key is a no-op.
    #[allow(clippy::too_many_arguments)]
    async fn remove_pending_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        component_name: &str,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        run_id: &str,
    ) -> EngineResult<()>;

    /// List ready pending runs for a `(project, deployment, component)`
    /// triple in priority-then-ready_at order. Entries with
    /// `ready_at_ms > now_ms` are skipped (delayed/backoff runs).
    ///
    /// Returns up to `limit` `run_id`s. The caller fetches the Run proto
    /// via `get_run` for each returned id.
    async fn list_pending_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        component_name: &str,
        now_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<String>>;

    // --- Deployment priority-aware pending index (pull-based workers) ---
    //
    // Parallel to the component-scoped priority index, but keyed by
    // `(project, deployment, priority, ready_at, created_at, run_id)` because
    // a registered worker session owns all components in its deployment.

    /// Insert a pending entry into the deployment-scoped priority index.
    #[allow(clippy::too_many_arguments)]
    async fn put_pending_deployment_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        run_id: &str,
    ) -> EngineResult<()>;

    /// Remove a pending entry from the deployment-scoped priority index.
    /// The caller must supply the same (priority, ready_at, created_at)
    /// values used on insert so the exact key can be located.
    #[allow(clippy::too_many_arguments)]
    async fn remove_pending_deployment_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
        ready_at_ms: i64,
        created_at_ms: i64,
        run_id: &str,
    ) -> EngineResult<()>;

    /// List ready pending runs for a `(project, deployment)` pair in
    /// priority-then-ready_at order. Entries with `ready_at_ms > now_ms` are
    /// skipped.
    async fn list_pending_deployment_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<String>>;

    /// Same order and readiness filter as `list_pending_deployment_priority`,
    /// returning full index positions and resuming strictly after `after`
    /// when given.
    async fn list_pending_deployment_priority_page(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        after: Option<&crate::storage_contract::PendingRunCursor>,
        limit: usize,
    ) -> EngineResult<Vec<crate::storage_contract::PendingRunCursor>>;

    // --- Queue counters ---

    /// Read the exact pending-count rollup for a project.
    async fn get_queue_counter_project(&self, project_id: &str) -> EngineResult<QueueCounter>;

    /// Read the exact pending-count rollup for a deployment.
    async fn get_queue_counter_deployment(
        &self,
        project_id: &str,
        deployment_id: &str,
    ) -> EngineResult<QueueCounter>;

    /// Read the exact pending-count rollup for a priority bucket.
    async fn get_queue_counter_priority(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
    ) -> EngineResult<QueueCounter>;

    // --- Batches ---

    async fn get_batch(&self, project_id: &str, batch_id: &str) -> EngineResult<Option<Vec<u8>>>;
    async fn put_batch(&self, project_id: &str, batch_id: &str, data: &[u8]) -> EngineResult<()>;

    // --- Sessions ---

    async fn get_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
    async fn put_session(
        &self,
        project_id: &str,
        session_id: &str,
        component_name: &str,
        data: &[u8],
    ) -> EngineResult<()>;
    async fn list_sessions_by_component(
        &self,
        project_id: &str,
        component_name: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    // --- Messages ---

    async fn get_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;
    async fn put_message(
        &self,
        project_id: &str,
        message_id: &str,
        correlation_id: &str,
        data: &[u8],
    ) -> EngineResult<()>;
    async fn list_messages_by_correlation(
        &self,
        project_id: &str,
        correlation_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>>;

    // --- Processor checkpoint ---

    /// Get the last processed offset for a project.
    async fn get_checkpoint(&self, project_id: &str) -> EngineResult<Option<Offset>>;

    /// Backend-neutral generation identity for operator status. Stores without
    /// a persisted generation contract return `None`.
    fn projection_generation_status(
        &self,
    ) -> Option<crate::storage_assessment::ProjectionGenerationStatus> {
        None
    }

    /// Exact exclusive projection-applied frontier (`M`) for one partition.
    /// `None` means the backend cannot prove an exact frontier.
    async fn projection_applied_frontier(&self, _partition_id: u32) -> EngineResult<Option<u64>> {
        Ok(None)
    }

    /// Set the processor checkpoint.
    async fn set_checkpoint(&self, project_id: &str, offset: Offset) -> EngineResult<()>;
}

/// Lossless archived-run reader used by offline recovery.
///
/// A missing row must be returned as `None`; infrastructure, query, decoding,
/// and schema failures must be returned as errors so recovery fails closed.
#[async_trait]
pub trait ArchivedRunReader: Send + Sync + 'static {
    /// Accepted project history boundary, when the archive records one.
    /// Absence grants no exemption from proving an archived row.
    async fn run_coverage_start_ms(&self, _project_id: &str) -> EngineResult<Option<i64>> {
        Ok(None)
    }

    async fn read_archived_run(
        &self,
        archive_directory: &str,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>>;

    /// Read a bounded group of archived Runs. Query-backed adapters should
    /// override this method to avoid one Parquet scan per run.
    async fn read_archived_runs(
        &self,
        requests: &[ArchivedRunReadRequest],
    ) -> EngineResult<Vec<Option<Vec<u8>>>> {
        let mut archived = Vec::with_capacity(requests.len());
        for request in requests {
            archived.push(
                self.read_archived_run(
                    &request.archive_directory,
                    &request.project_id,
                    &request.run_id,
                )
                .await?,
            );
        }
        Ok(archived)
    }

    /// Stream every distinct lossless archived Run in one UTC archive day
    /// without buffering the whole day in memory.
    async fn stream_archived_runs_for_day(
        &self,
        _day: &str,
    ) -> EngineResult<ArchivedRunSeedStream> {
        Err(EngineError::Storage(
            "archive reader does not support day-scoped run seeding".to_string(),
        ))
    }
}
