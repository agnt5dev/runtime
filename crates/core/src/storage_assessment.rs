//! Backend-neutral storage evidence and reconciliation decisions.
//!
//! This module is intentionally pure. Storage adapters collect the evidence;
//! callers pass one partition state to [`assess_partition_storage`]; recovery,
//! readiness, status, metrics, and telemetry consume the returned assessment.
//! No inspection or mutation happens behind this interface.

use serde::{Deserialize, Serialize};

/// First projection-source contract that permits only quorum-committed input.
pub const COMMIT_GATED_PROJECTION_SOURCE_CONTRACT_V1: u32 = 1;

/// Local-only recovery contract for a bounded run projection seeded from the
/// lossless archive at an already verified journal frontier.
///
/// This contract is safe to serve and continue. It may be published only as an
/// explicitly typed archive-seed baseline whose immutable lineage states that
/// it is a bounded hot-storage projection, not full journal history.
pub const ARCHIVE_RUN_SEED_PROJECTION_SOURCE_CONTRACT_V1: u32 = 2;

/// Stable logical key for the exact last-applied record of one partition.
///
/// The persisted value is the source record's inclusive encoded offset. A
/// reader normalizes it to the exclusive `M` frontier by adding one locally.
pub fn projection_applied_key(partition_id: u32) -> String {
    format!("projection-applied-partition-{partition_id}")
}

/// Which persisted domains are required by the active storage profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRequirements {
    pub cold_recovery: bool,
    pub projection: bool,
    pub cold_index: bool,
}

impl StorageRequirements {
    pub const fn managed_ha() -> Self {
        Self {
            cold_recovery: true,
            projection: true,
            cold_index: true,
        }
    }
}

impl Default for StorageRequirements {
    fn default() -> Self {
        Self::managed_ha()
    }
}

/// Physically retained local journal evidence for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotJournalState {
    /// First retained local record; the hot range is `[retained_floor, tail)`.
    pub retained_floor: u64,
    /// Next local offset after the last physically retained record.
    pub tail: u64,
}

/// Durable quorum certificate and this process's local verification state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuorumCommitState {
    /// Exclusive quorum-committed boundary (`Q`).
    pub through: u64,
    /// Canonical digest of `[0, through)`. Required when `through > 0`.
    pub digest_v1: Option<String>,
    /// Exclusive locally verified boundary (`V`).
    pub locally_verified_through: u64,
    /// Digest recomputed from this node's admitted composite journal at `Q`.
    pub composite_digest_v1: Option<String>,
}

/// One independently admitted cold-journal failure domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdJournalState {
    pub failure_domain: String,
    /// Exclusive admitted catalog boundary (`C_f`).
    pub catalog_through: u64,
    /// Canonical digest of the admitted catalog at `catalog_through`.
    pub catalog_digest_v1: Option<String>,
    /// Catalog prefix digest at local `Q`, when the catalog covers `Q`.
    pub catalog_digest_at_commit_v1: Option<String>,
    /// Exclusive cold proof-checkpoint boundary (`J_f`).
    pub proof_through: u64,
    /// Digest persisted by the proof checkpoint at `proof_through`.
    pub proof_digest_v1: Option<String>,
    /// Digest recomputed from the admitted catalog at `proof_through`.
    pub catalog_digest_at_proof_v1: Option<String>,
    /// Whether publication authority and the immutable catalog chain validate.
    pub authority_validated: bool,
}

/// Digest evidence for the duplicate range retained in hot and cold storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotColdOverlapState {
    pub range_start: u64,
    pub range_end_exclusive: u64,
    pub hot_digest_v1: String,
    pub cold_digest_v1: String,
}

/// Provenance lifecycle for one materialized projection generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionProvenance {
    LegacyUnmarked,
    Rebuilding,
    CommitGatedTrusted,
    ArchiveSeededTrusted,
    Invalidated,
}

/// How one projection generation obtained its initial contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionBuildMode {
    EmptyJournalReplay,
    CertifiedSnapshotRestore,
    ArchiveRunSeed,
}

/// Immutable root from which a trusted projection generation descends.
///
/// Build mode answers how the current local generation was created. Lineage
/// answers what historical completeness contract that generation may claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectionLineage {
    FullJournal,
    ArchiveSeed(ArchiveSeedLineage),
}

/// Source identity for the traffic-dark archive cutover baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSeedLineage {
    pub lineage_id: String,
    pub source_bucket: String,
    /// Newest day in the selected archive window. Retained for v3 lineage
    /// compatibility and for concise operational display.
    pub archive_day: String,
    /// Complete ordered window when recovery selected more than one day.
    /// Legacy and one-day lineages may omit it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub archive_days: Vec<String>,
    pub source_assessment_digest_v1: String,
    pub archive_inventory_digest_v1: String,
    pub archive_object_count: u64,
    pub runs_seeded: u64,
}

impl ArchiveSeedLineage {
    pub fn is_valid(&self) -> bool {
        let valid_window = self.archive_days.is_empty()
            || (self.archive_days.iter().all(|day| valid_archive_day(day))
                && self.archive_days.windows(2).all(|days| days[0] < days[1])
                && self.archive_days.last() == Some(&self.archive_day));
        uuid::Uuid::parse_str(&self.lineage_id).is_ok()
            && !self.source_bucket.trim().is_empty()
            && valid_archive_day(&self.archive_day)
            && valid_window
            && valid_sha256(&self.source_assessment_digest_v1)
            && valid_sha256(&self.archive_inventory_digest_v1)
            && self.archive_object_count > 0
            && self.runs_seeded > 0
    }
}

fn valid_archive_day(day: &str) -> bool {
    let bytes = day.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Journal evidence bound to one partition when a generation is certified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionPartitionCertification {
    /// Exact exclusive applied frontier (`M`) at certification.
    pub applied_through: u64,
    /// Canonical journal digest of `[0, applied_through)`.
    pub journal_digest_v1: String,
}

/// Durable source contract bound to one projection-store generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionGenerationProvenance {
    pub source_contract_version: Option<u32>,
    pub state: ProjectionProvenance,
    pub build_mode: Option<ProjectionBuildMode>,
    pub parent_snapshot_generation: Option<String>,
    /// Added after the v2 source contract. `None` on an older commit-gated
    /// generation means the legacy-equivalent `FullJournal` lineage.
    #[serde(default)]
    pub lineage: Option<ProjectionLineage>,
}

/// Read-only identity exported by storage status surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionGenerationStatus {
    pub format_version: u32,
    pub generation_id: String,
    pub provenance: ProjectionGenerationProvenance,
}

impl ProjectionGenerationProvenance {
    pub fn legacy_unmarked() -> Self {
        Self {
            source_contract_version: None,
            state: ProjectionProvenance::LegacyUnmarked,
            build_mode: None,
            parent_snapshot_generation: None,
            lineage: None,
        }
    }

    /// Whether this generation is admitted for local serving.
    ///
    /// The contract marker is the trust boundary, and since BOOT-404 it is
    /// **sufficient**: the marker is written at certification and cleared at
    /// invalidation, so every untrusted situation — legacy, mid-rebuild,
    /// crashed mid-rebuild, invalidated — is simply the absence of a marker.
    ///
    /// That is what makes an interrupted rebuild safe without keeping a second
    /// copy of the store: nothing ever marked the partial work trusted.
    ///
    /// The four-state enum said the same thing four ways. It is unread after
    /// this and BOOT-405 deletes it.
    pub fn is_trusted(&self) -> bool {
        matches!(
            self.source_contract_version,
            Some(
                COMMIT_GATED_PROJECTION_SOURCE_CONTRACT_V1
                    | ARCHIVE_RUN_SEED_PROJECTION_SOURCE_CONTRACT_V1
            )
        )
    }

    /// Whether this generation is complete enough to publish as a portable
    /// projection snapshot while preserving its historical completeness
    /// contract.
    pub fn is_snapshot_eligible(&self) -> bool {
        match (self.state, self.source_contract_version, self.build_mode) {
            (
                ProjectionProvenance::CommitGatedTrusted,
                Some(COMMIT_GATED_PROJECTION_SOURCE_CONTRACT_V1),
                Some(
                    ProjectionBuildMode::EmptyJournalReplay
                    | ProjectionBuildMode::CertifiedSnapshotRestore,
                ),
            ) => matches!(self.lineage, None | Some(ProjectionLineage::FullJournal)),
            (
                ProjectionProvenance::ArchiveSeededTrusted,
                Some(ARCHIVE_RUN_SEED_PROJECTION_SOURCE_CONTRACT_V1),
                Some(
                    ProjectionBuildMode::ArchiveRunSeed
                    | ProjectionBuildMode::CertifiedSnapshotRestore,
                ),
            ) => matches!(
                self.lineage.as_ref(),
                Some(ProjectionLineage::ArchiveSeed(lineage)) if lineage.is_valid()
            ),
            _ => false,
        }
    }

    /// Resolve old commit-gated generations to their implicit full-journal
    /// lineage. Archive generations must always carry an explicit lineage.
    pub fn resolved_lineage(&self) -> Option<ProjectionLineage> {
        match (&self.lineage, self.source_contract_version) {
            (Some(lineage), _) => Some(lineage.clone()),
            (None, Some(COMMIT_GATED_PROJECTION_SOURCE_CONTRACT_V1)) => {
                Some(ProjectionLineage::FullJournal)
            }
            _ => None,
        }
    }

    /// Strip a contract marker that a pre-BOOT-404 binary wrote onto a
    /// generation that had not earned it.
    ///
    /// Before BOOT-404 the marker was written when a rebuild *started*. A store
    /// persisted mid-rebuild or invalidated therefore carries a marker while
    /// being untrusted, and once the state field is deleted in BOOT-405 nothing
    /// would remain to say so. Normalising on open closes that window while the
    /// state is still readable.
    ///
    /// Returns true when it changed something, so the caller can persist it.
    pub fn normalize_legacy_marker(&mut self) -> bool {
        let untrusted_state = matches!(
            self.state,
            ProjectionProvenance::Rebuilding | ProjectionProvenance::Invalidated
        );
        if untrusted_state && self.source_contract_version.is_some() {
            self.source_contract_version = None;
            return true;
        }
        false
    }

    /// Whether this generation must be rebuilt before it can be served.
    pub fn needs_rebuild(&self) -> bool {
        !self.is_trusted()
    }

    /// A generation that is being built and has not earned the contract marker.
    ///
    /// The marker is written at certification, never before. That is what makes
    /// an interrupted rebuild safe by construction: whatever the crash leaves
    /// behind carries no marker, so it is untrusted and gets rebuilt again. No
    /// second copy of the store is needed to achieve it.
    pub fn rebuilding_from_empty_journal() -> Self {
        Self {
            source_contract_version: None,
            state: ProjectionProvenance::Rebuilding,
            build_mode: Some(ProjectionBuildMode::EmptyJournalReplay),
            parent_snapshot_generation: None,
            lineage: None,
        }
    }

    /// A fresh local generation being populated from the bounded lossless run
    /// archive. It remains untrusted until all rows and exact frontiers are
    /// durably written.
    pub fn rebuilding_from_archive_runs() -> Self {
        Self {
            source_contract_version: None,
            state: ProjectionProvenance::Rebuilding,
            build_mode: Some(ProjectionBuildMode::ArchiveRunSeed),
            parent_snapshot_generation: None,
            lineage: None,
        }
    }
}

/// Materialized projection evidence for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionState {
    pub generation_id: Option<String>,
    /// Periodic normalized exclusive checkpoint (`P`).
    pub checkpoint_through: u64,
    /// Exact normalized exclusive applied frontier (`M`), once persisted.
    pub applied_through: Option<u64>,
    /// Commit frontier captured for the current processor attempt (`R`).
    pub replay_target: u64,
    pub provenance: ProjectionProvenance,
}

/// Correctness-relevant work that is not representable as one frontier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObligationState {
    pub kind: String,
    pub pending: u64,
    pub failed: u64,
    pub deferred: u64,
    pub oldest_age_ms: Option<u64>,
    pub generation_id: Option<String>,
}

/// Non-offset identity and authority evidence required before serving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageGuards {
    pub partition_identity_valid: bool,
    pub topology_valid: bool,
    pub segment_lineage_valid: bool,
    pub authority_unique: bool,
    /// True only after the old epoch is fenced and suffix repair is authorized.
    pub epoch_transition_accepted: bool,
    pub integrity_fenced: bool,
}

impl Default for StorageGuards {
    fn default() -> Self {
        Self {
            partition_identity_valid: true,
            topology_valid: true,
            segment_lineage_valid: true,
            authority_unique: true,
            epoch_transition_accepted: false,
            integrity_fenced: false,
        }
    }
}

/// Complete currently-observable state for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStorageState {
    pub partition_id: u32,
    pub requirements: StorageRequirements,
    pub hot: HotJournalState,
    pub commit: QuorumCommitState,
    pub cold: Vec<ColdJournalState>,
    pub selected_cold_failure_domain: Option<String>,
    pub hot_cold_overlap: Option<HotColdOverlapState>,
    /// Exclusive cold run-index boundary (`I`).
    pub cold_index_through: Option<u64>,
    pub projection: Option<ProjectionState>,
    pub obligations: Vec<ObligationState>,
    pub guards: StorageGuards,
}

impl PartitionStorageState {
    fn selected_cold(&self) -> Option<&ColdJournalState> {
        let selected = self.selected_cold_failure_domain.as_deref()?;
        self.cold
            .iter()
            .find(|cold| cold.failure_domain == selected)
    }
}

/// Operator-facing classification; lag is deliberately distinct from conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageCondition {
    Healthy,
    AvailabilityLag,
    IntegrityDivergence,
    InvalidShape,
    UnknownProvenance,
}

/// Exact action selected after read-only reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RecoveryDisposition {
    Healthy,
    WaitForCommit { from: u64, to: u64 },
    TrimUncommittedHotSuffix { from: u64, to: u64 },
    HydrateFromCold { source: String, from: u64, to: u64 },
    CatchUpFromLeader { from: u64, to: u64 },
    WaitForColdPublication { from: u64, to: u64 },
    AdvanceLocalCertificateFromCold { source: String, from: u64, to: u64 },
    RestoreJournalRange { source: String, from: u64, to: u64 },
    VerifyHotColdOverlap { from: u64, to: u64 },
    VerifyCommittedPrefix { from: u64, to: u64 },
    ReplayCommittedSuffix { from: u64, to: u64 },
    RebuildProjectionFromJournal { through: u64 },
    RebuildColdIndex { from: u64, to: u64 },
    FenceIntegrity,
    OperatorRequired,
}

/// One raw invariant result retained for status and incident evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageFinding {
    IntegrityAlreadyFenced,
    InvalidGuard {
        guard: String,
    },
    InvalidHotRange {
        retained_floor: u64,
        tail: u64,
    },
    MissingSelectedColdSource,
    ColdAuthorityInvalid {
        failure_domain: String,
    },
    ColdProofAhead {
        failure_domain: String,
        proof_through: u64,
        catalog_through: u64,
    },
    ColdProofDigestMismatch {
        failure_domain: String,
        through: u64,
    },
    ColdProofDigestMissing {
        failure_domain: String,
        through: u64,
    },
    ColdCatalogDigestMissing {
        failure_domain: String,
        through: u64,
    },
    HotColdGap {
        failure_domain: String,
        cold_through: u64,
        retained_floor: u64,
    },
    HotColdOverlapDigestMismatch {
        from: u64,
        to: u64,
    },
    HotColdOverlapUnverified {
        from: u64,
        to: u64,
    },
    HotColdOverlapRangeMismatch {
        expected_from: u64,
        expected_to: u64,
        actual_from: u64,
        actual_to: u64,
    },
    MissingCommitDigest {
        through: u64,
    },
    MissingCompositeCommitDigest {
        through: u64,
    },
    CommitDigestMismatch {
        through: u64,
    },
    MissingColdCommitDigest {
        failure_domain: String,
        through: u64,
    },
    CommitColdDigestMismatch {
        failure_domain: String,
        through: u64,
    },
    LocalVerificationAhead {
        verified: u64,
        committed: u64,
    },
    LocalVerificationBehind {
        verified: u64,
        committed: u64,
    },
    MissingCommittedHistory {
        available: u64,
        committed: u64,
    },
    LocalHotBehind {
        failure_domain: String,
        hot_tail: u64,
        committed: u64,
    },
    LocalUncommittedSuffix {
        committed: u64,
        hot_tail: u64,
    },
    ColdBehindCommit {
        failure_domain: String,
        cold_through: u64,
        committed: u64,
    },
    ColdAheadOfCommit {
        failure_domain: String,
        committed: u64,
        cold_through: u64,
    },
    ColdIndexMissing,
    ColdIndexBehind {
        indexed: u64,
        cold_through: u64,
    },
    ColdIndexAhead {
        indexed: u64,
        cold_through: u64,
    },
    ProjectionMissing,
    ProjectionUnknownProvenance,
    ProjectionInvalidated,
    ProjectionAppliedUnknown,
    ProjectionCheckpointAhead {
        checkpoint: u64,
        applied: u64,
    },
    ProjectionAheadOfCommit {
        applied: u64,
        committed: u64,
    },
    ProjectionBehindReplayTarget {
        applied: u64,
        target: u64,
    },
}

impl StorageFinding {
    fn priority(&self) -> u8 {
        match self {
            Self::IntegrityAlreadyFenced
            | Self::ColdAuthorityInvalid { .. }
            | Self::ColdProofDigestMismatch { .. }
            | Self::CommitColdDigestMismatch { .. }
            | Self::HotColdOverlapDigestMismatch { .. }
            | Self::CommitDigestMismatch { .. }
            | Self::ProjectionInvalidated => 0,
            Self::InvalidGuard { .. }
            | Self::InvalidHotRange { .. }
            | Self::MissingSelectedColdSource
            | Self::ColdProofAhead { .. }
            | Self::ColdProofDigestMissing { .. }
            | Self::ColdCatalogDigestMissing { .. }
            | Self::HotColdGap { .. }
            | Self::HotColdOverlapRangeMismatch { .. }
            | Self::MissingCommitDigest { .. }
            | Self::MissingCompositeCommitDigest { .. }
            | Self::MissingColdCommitDigest { .. }
            | Self::LocalVerificationAhead { .. }
            | Self::ColdIndexAhead { .. }
            | Self::ProjectionMissing
            | Self::ProjectionCheckpointAhead { .. }
            | Self::ProjectionAheadOfCommit { .. } => 10,
            Self::ProjectionUnknownProvenance | Self::ProjectionAppliedUnknown => 20,
            Self::MissingCommittedHistory { .. } => 30,
            Self::LocalHotBehind { .. } => 31,
            Self::LocalUncommittedSuffix { .. } => 32,
            Self::ColdAheadOfCommit { .. } => 33,
            Self::HotColdOverlapUnverified { .. } => 34,
            Self::LocalVerificationBehind { .. } => 35,
            Self::ProjectionBehindReplayTarget { .. } => 36,
            Self::ColdIndexMissing | Self::ColdIndexBehind { .. } => 37,
            Self::ColdBehindCommit { .. } => 40,
        }
    }

    fn condition(&self) -> StorageCondition {
        match self {
            Self::IntegrityAlreadyFenced
            | Self::ColdAuthorityInvalid { .. }
            | Self::ColdProofDigestMismatch { .. }
            | Self::CommitColdDigestMismatch { .. }
            | Self::HotColdOverlapDigestMismatch { .. }
            | Self::CommitDigestMismatch { .. }
            | Self::ProjectionInvalidated => StorageCondition::IntegrityDivergence,
            Self::InvalidGuard { .. }
            | Self::InvalidHotRange { .. }
            | Self::MissingSelectedColdSource
            | Self::ColdProofAhead { .. }
            | Self::ColdProofDigestMissing { .. }
            | Self::ColdCatalogDigestMissing { .. }
            | Self::HotColdGap { .. }
            | Self::HotColdOverlapRangeMismatch { .. }
            | Self::MissingCommitDigest { .. }
            | Self::MissingCompositeCommitDigest { .. }
            | Self::MissingColdCommitDigest { .. }
            | Self::LocalVerificationAhead { .. }
            | Self::ColdIndexAhead { .. }
            | Self::ProjectionMissing
            | Self::ProjectionCheckpointAhead { .. }
            | Self::ProjectionAheadOfCommit { .. } => StorageCondition::InvalidShape,
            Self::ProjectionUnknownProvenance | Self::ProjectionAppliedUnknown => {
                StorageCondition::UnknownProvenance
            }
            Self::LocalVerificationBehind { .. }
            | Self::MissingCommittedHistory { .. }
            | Self::LocalHotBehind { .. }
            | Self::LocalUncommittedSuffix { .. }
            | Self::HotColdOverlapUnverified { .. }
            | Self::ColdBehindCommit { .. }
            | Self::ColdAheadOfCommit { .. }
            | Self::ColdIndexMissing
            | Self::ColdIndexBehind { .. }
            | Self::ProjectionBehindReplayTarget { .. } => StorageCondition::AvailabilityLag,
        }
    }

    fn blocks_authority(&self) -> bool {
        !matches!(
            self,
            Self::ColdBehindCommit { .. } | Self::ColdIndexMissing | Self::ColdIndexBehind { .. }
        )
    }

    fn blocks_independent_recovery(&self) -> bool {
        !matches!(
            self,
            Self::LocalUncommittedSuffix { .. }
                | Self::LocalVerificationBehind { .. }
                | Self::ProjectionBehindReplayTarget { .. }
        )
    }
}

/// Pure assessment shared by recovery, readiness, status, and telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAssessment {
    pub partition_id: u32,
    pub condition: StorageCondition,
    pub disposition: RecoveryDisposition,
    pub authority_eligible: bool,
    pub independent_recovery_ready: bool,
    /// Findings are ordered by deterministic reconciliation priority.
    pub findings: Vec<StorageFinding>,
}

/// Evaluate one partition without reading or mutating any storage backend.
pub fn assess_partition_storage(state: &PartitionStorageState) -> StorageAssessment {
    let mut findings = collect_findings(state);
    findings.sort_by_key(StorageFinding::priority);

    let Some(primary) = findings.first() else {
        return StorageAssessment {
            partition_id: state.partition_id,
            condition: StorageCondition::Healthy,
            disposition: RecoveryDisposition::Healthy,
            authority_eligible: true,
            independent_recovery_ready: true,
            findings,
        };
    };

    StorageAssessment {
        partition_id: state.partition_id,
        condition: primary.condition(),
        disposition: disposition_for(state, primary),
        authority_eligible: findings.iter().all(|finding| !finding.blocks_authority()),
        independent_recovery_ready: findings
            .iter()
            .all(|finding| !finding.blocks_independent_recovery()),
        findings,
    }
}

fn collect_findings(state: &PartitionStorageState) -> Vec<StorageFinding> {
    let mut findings = Vec::new();
    let hot = &state.hot;
    let commit = &state.commit;

    if state.guards.integrity_fenced {
        findings.push(StorageFinding::IntegrityAlreadyFenced);
    }
    for (valid, guard) in [
        (state.guards.partition_identity_valid, "partition_identity"),
        (state.guards.topology_valid, "topology"),
        (state.guards.segment_lineage_valid, "segment_lineage"),
        (state.guards.authority_unique, "authority_unique"),
    ] {
        if !valid {
            findings.push(StorageFinding::InvalidGuard {
                guard: guard.to_string(),
            });
        }
    }
    if hot.retained_floor > hot.tail {
        findings.push(StorageFinding::InvalidHotRange {
            retained_floor: hot.retained_floor,
            tail: hot.tail,
        });
    }

    let selected_cold = state.selected_cold();
    if state.requirements.cold_recovery && selected_cold.is_none() {
        findings.push(StorageFinding::MissingSelectedColdSource);
    }
    for cold in &state.cold {
        if !cold.authority_validated {
            findings.push(StorageFinding::ColdAuthorityInvalid {
                failure_domain: cold.failure_domain.clone(),
            });
        }
        if cold.catalog_through > 0 && cold.catalog_digest_v1.is_none() {
            findings.push(StorageFinding::ColdCatalogDigestMissing {
                failure_domain: cold.failure_domain.clone(),
                through: cold.catalog_through,
            });
        }
        if cold.proof_through > cold.catalog_through {
            findings.push(StorageFinding::ColdProofAhead {
                failure_domain: cold.failure_domain.clone(),
                proof_through: cold.proof_through,
                catalog_through: cold.catalog_through,
            });
        }
        if cold.proof_through > 0 {
            if cold.proof_digest_v1.is_none() || cold.catalog_digest_at_proof_v1.is_none() {
                findings.push(StorageFinding::ColdProofDigestMissing {
                    failure_domain: cold.failure_domain.clone(),
                    through: cold.proof_through,
                });
            } else if cold.proof_digest_v1 != cold.catalog_digest_at_proof_v1 {
                findings.push(StorageFinding::ColdProofDigestMismatch {
                    failure_domain: cold.failure_domain.clone(),
                    through: cold.proof_through,
                });
            }
        }
    }

    if let Some(cold) = selected_cold {
        if hot.retained_floor > cold.catalog_through {
            findings.push(StorageFinding::HotColdGap {
                failure_domain: cold.failure_domain.clone(),
                cold_through: cold.catalog_through,
                retained_floor: hot.retained_floor,
            });
        }
        if cold.catalog_through < commit.through {
            findings.push(StorageFinding::ColdBehindCommit {
                failure_domain: cold.failure_domain.clone(),
                cold_through: cold.catalog_through,
                committed: commit.through,
            });
        } else if cold.catalog_through > commit.through {
            findings.push(StorageFinding::ColdAheadOfCommit {
                failure_domain: cold.failure_domain.clone(),
                committed: commit.through,
                cold_through: cold.catalog_through,
            });
        }
        if commit.through > 0 && cold.catalog_through >= commit.through {
            match (
                commit.digest_v1.as_ref(),
                cold.catalog_digest_at_commit_v1.as_ref(),
            ) {
                (_, None) => findings.push(StorageFinding::MissingColdCommitDigest {
                    failure_domain: cold.failure_domain.clone(),
                    through: commit.through,
                }),
                (Some(commit_digest), Some(cold_digest)) if commit_digest != cold_digest => {
                    findings.push(StorageFinding::CommitColdDigestMismatch {
                        failure_domain: cold.failure_domain.clone(),
                        through: commit.through,
                    });
                }
                _ => {}
            }
        }

        if state.requirements.cold_index {
            match state.cold_index_through {
                None => findings.push(StorageFinding::ColdIndexMissing),
                Some(indexed) if indexed < cold.catalog_through => {
                    findings.push(StorageFinding::ColdIndexBehind {
                        indexed,
                        cold_through: cold.catalog_through,
                    });
                }
                Some(indexed) if indexed > cold.catalog_through => {
                    findings.push(StorageFinding::ColdIndexAhead {
                        indexed,
                        cold_through: cold.catalog_through,
                    });
                }
                Some(_) => {}
            }
        }
    }

    if let Some(cold) = selected_cold {
        let overlap_from = hot.retained_floor;
        let overlap_to = hot.tail.min(cold.catalog_through);
        if overlap_from < overlap_to {
            match state.hot_cold_overlap.as_ref() {
                None => findings.push(StorageFinding::HotColdOverlapUnverified {
                    from: overlap_from,
                    to: overlap_to,
                }),
                Some(overlap)
                    if overlap.range_start != overlap_from
                        || overlap.range_end_exclusive != overlap_to =>
                {
                    findings.push(StorageFinding::HotColdOverlapRangeMismatch {
                        expected_from: overlap_from,
                        expected_to: overlap_to,
                        actual_from: overlap.range_start,
                        actual_to: overlap.range_end_exclusive,
                    });
                }
                Some(overlap) if overlap.hot_digest_v1 != overlap.cold_digest_v1 => {
                    findings.push(StorageFinding::HotColdOverlapDigestMismatch {
                        from: overlap.range_start,
                        to: overlap.range_end_exclusive,
                    });
                }
                Some(_) => {}
            }
        }
    }

    if commit.through > 0 && commit.digest_v1.is_none() {
        findings.push(StorageFinding::MissingCommitDigest {
            through: commit.through,
        });
    }
    if commit.through > 0 && commit.composite_digest_v1.is_none() {
        findings.push(StorageFinding::MissingCompositeCommitDigest {
            through: commit.through,
        });
    }
    if commit.through > 0
        && commit.digest_v1.is_some()
        && commit.composite_digest_v1.is_some()
        && commit.digest_v1 != commit.composite_digest_v1
    {
        findings.push(StorageFinding::CommitDigestMismatch {
            through: commit.through,
        });
    }
    if commit.locally_verified_through > commit.through {
        findings.push(StorageFinding::LocalVerificationAhead {
            verified: commit.locally_verified_through,
            committed: commit.through,
        });
    } else if commit.locally_verified_through < commit.through {
        findings.push(StorageFinding::LocalVerificationBehind {
            verified: commit.locally_verified_through,
            committed: commit.through,
        });
    }

    let cold_available = selected_cold.map_or(0, |cold| cold.catalog_through);
    let available = hot.tail.max(cold_available);
    if available < commit.through {
        findings.push(StorageFinding::MissingCommittedHistory {
            available,
            committed: commit.through,
        });
    } else if hot.tail < commit.through && cold_available >= commit.through {
        findings.push(StorageFinding::LocalHotBehind {
            failure_domain: selected_cold
                .map(|cold| cold.failure_domain.clone())
                .unwrap_or_default(),
            hot_tail: hot.tail,
            committed: commit.through,
        });
    }
    if hot.tail > commit.through {
        findings.push(StorageFinding::LocalUncommittedSuffix {
            committed: commit.through,
            hot_tail: hot.tail,
        });
    }

    if state.requirements.projection {
        match state.projection.as_ref() {
            None => findings.push(StorageFinding::ProjectionMissing),
            Some(projection) => collect_projection_findings(projection, commit, &mut findings),
        }
    }

    findings
}

fn collect_projection_findings(
    projection: &ProjectionState,
    commit: &QuorumCommitState,
    findings: &mut Vec<StorageFinding>,
) {
    match projection.provenance {
        ProjectionProvenance::LegacyUnmarked => {
            findings.push(StorageFinding::ProjectionUnknownProvenance)
        }
        ProjectionProvenance::Invalidated => findings.push(StorageFinding::ProjectionInvalidated),
        ProjectionProvenance::Rebuilding
        | ProjectionProvenance::CommitGatedTrusted
        | ProjectionProvenance::ArchiveSeededTrusted => {}
    }

    let Some(applied) = projection.applied_through else {
        findings.push(StorageFinding::ProjectionAppliedUnknown);
        return;
    };
    if projection.checkpoint_through > applied {
        findings.push(StorageFinding::ProjectionCheckpointAhead {
            checkpoint: projection.checkpoint_through,
            applied,
        });
    }
    if applied > commit.through {
        findings.push(StorageFinding::ProjectionAheadOfCommit {
            applied,
            committed: commit.through,
        });
    }
    if applied < projection.replay_target {
        findings.push(StorageFinding::ProjectionBehindReplayTarget {
            applied,
            target: projection.replay_target,
        });
    }
}

fn disposition_for(state: &PartitionStorageState, finding: &StorageFinding) -> RecoveryDisposition {
    match finding {
        StorageFinding::IntegrityAlreadyFenced
        | StorageFinding::ColdAuthorityInvalid { .. }
        | StorageFinding::ColdProofDigestMismatch { .. }
        | StorageFinding::CommitColdDigestMismatch { .. }
        | StorageFinding::HotColdOverlapDigestMismatch { .. }
        | StorageFinding::CommitDigestMismatch { .. } => RecoveryDisposition::FenceIntegrity,
        StorageFinding::ProjectionInvalidated
        | StorageFinding::ProjectionUnknownProvenance
        | StorageFinding::ProjectionAppliedUnknown
        | StorageFinding::ProjectionCheckpointAhead { .. }
        | StorageFinding::ProjectionAheadOfCommit { .. }
        | StorageFinding::ProjectionMissing => RecoveryDisposition::RebuildProjectionFromJournal {
            through: state.commit.through,
        },
        StorageFinding::ColdIndexAhead {
            indexed,
            cold_through,
        }
        | StorageFinding::ColdIndexBehind {
            indexed,
            cold_through,
        } => RecoveryDisposition::RebuildColdIndex {
            from: *indexed,
            to: *cold_through,
        },
        StorageFinding::ColdIndexMissing => RecoveryDisposition::RebuildColdIndex {
            from: 0,
            to: state.selected_cold().map_or(0, |cold| cold.catalog_through),
        },
        StorageFinding::MissingCommittedHistory {
            available,
            committed,
        } => RecoveryDisposition::CatchUpFromLeader {
            from: *available,
            to: *committed,
        },
        StorageFinding::LocalHotBehind {
            failure_domain,
            hot_tail,
            committed,
        } => RecoveryDisposition::HydrateFromCold {
            source: failure_domain.clone(),
            from: *hot_tail,
            to: *committed,
        },
        StorageFinding::LocalUncommittedSuffix {
            committed,
            hot_tail,
        } if state.guards.epoch_transition_accepted => {
            RecoveryDisposition::TrimUncommittedHotSuffix {
                from: *committed,
                to: *hot_tail,
            }
        }
        StorageFinding::LocalUncommittedSuffix {
            committed,
            hot_tail,
        } => RecoveryDisposition::WaitForCommit {
            from: *committed,
            to: *hot_tail,
        },
        StorageFinding::ColdBehindCommit {
            cold_through,
            committed,
            ..
        } => RecoveryDisposition::WaitForColdPublication {
            from: *cold_through,
            to: *committed,
        },
        StorageFinding::ColdAheadOfCommit {
            failure_domain,
            committed,
            cold_through,
        } => RecoveryDisposition::AdvanceLocalCertificateFromCold {
            source: failure_domain.clone(),
            from: *committed,
            to: *cold_through,
        },
        StorageFinding::HotColdGap {
            failure_domain,
            cold_through,
            retained_floor,
        } => RecoveryDisposition::RestoreJournalRange {
            source: failure_domain.clone(),
            from: *cold_through,
            to: *retained_floor,
        },
        StorageFinding::LocalVerificationBehind {
            verified,
            committed,
        } => RecoveryDisposition::VerifyCommittedPrefix {
            from: *verified,
            to: *committed,
        },
        StorageFinding::ProjectionBehindReplayTarget { applied, target } => {
            RecoveryDisposition::ReplayCommittedSuffix {
                from: *applied,
                to: *target,
            }
        }
        StorageFinding::InvalidGuard { .. }
        | StorageFinding::InvalidHotRange { .. }
        | StorageFinding::MissingSelectedColdSource
        | StorageFinding::ColdProofAhead { .. }
        | StorageFinding::ColdProofDigestMissing { .. }
        | StorageFinding::ColdCatalogDigestMissing { .. }
        | StorageFinding::HotColdOverlapRangeMismatch { .. }
        | StorageFinding::MissingCommitDigest { .. }
        | StorageFinding::MissingCompositeCommitDigest { .. }
        | StorageFinding::MissingColdCommitDigest { .. }
        | StorageFinding::LocalVerificationAhead { .. } => RecoveryDisposition::OperatorRequired,
        StorageFinding::HotColdOverlapUnverified { from, to } => {
            RecoveryDisposition::VerifyHotColdOverlap {
                from: *from,
                to: *to,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trusted() -> ProjectionGenerationProvenance {
        ProjectionGenerationProvenance {
            source_contract_version: Some(COMMIT_GATED_PROJECTION_SOURCE_CONTRACT_V1),
            state: ProjectionProvenance::CommitGatedTrusted,
            build_mode: Some(ProjectionBuildMode::EmptyJournalReplay),
            parent_snapshot_generation: None,
            lineage: Some(ProjectionLineage::FullJournal),
        }
    }

    #[test]
    fn the_contract_marker_decides_trust() {
        assert!(trusted().is_trusted());

        // No marker means the generation predates the contract. This is what
        // `LegacyUnmarked` said, and it is the only thing that mattered.
        let legacy = ProjectionGenerationProvenance::legacy_unmarked();
        assert!(!legacy.is_trusted());
        assert!(legacy.needs_rebuild());

        let archive_seed = ProjectionGenerationProvenance {
            source_contract_version: Some(ARCHIVE_RUN_SEED_PROJECTION_SOURCE_CONTRACT_V1),
            state: ProjectionProvenance::ArchiveSeededTrusted,
            build_mode: Some(ProjectionBuildMode::ArchiveRunSeed),
            parent_snapshot_generation: None,
            lineage: Some(ProjectionLineage::ArchiveSeed(ArchiveSeedLineage {
                lineage_id: uuid::Uuid::now_v7().to_string(),
                source_bucket: "fixture".to_string(),
                archive_day: "2026-08-14".to_string(),
                archive_days: vec!["2026-08-13".to_string(), "2026-08-14".to_string()],
                source_assessment_digest_v1: "a".repeat(64),
                archive_inventory_digest_v1: "b".repeat(64),
                archive_object_count: 33,
                runs_seeded: 293_664,
            })),
        };
        assert!(archive_seed.is_trusted());
        assert!(archive_seed.is_snapshot_eligible());

        // An unknown marker is not a supported contract.
        let other = ProjectionGenerationProvenance {
            source_contract_version: Some(ARCHIVE_RUN_SEED_PROJECTION_SOURCE_CONTRACT_V1 + 1),
            ..trusted()
        };
        assert!(!other.is_trusted());
    }

    /// The marker is earned, never assumed.
    ///
    /// Before BOOT-404 it was written when a rebuild *started*, so a store
    /// persisted mid-rebuild carried a marker it had not earned. Writing it only
    /// at certification is what makes an interrupted rebuild safe without
    /// keeping a second copy of the store: whatever the crash leaves behind
    /// carries no marker, so it is untrusted and gets rebuilt again.
    #[test]
    fn a_rebuilding_generation_has_not_earned_the_marker() {
        let rebuilding = ProjectionGenerationProvenance::rebuilding_from_empty_journal();
        assert_eq!(rebuilding.source_contract_version, None);
        assert!(!rebuilding.is_trusted());
        assert!(rebuilding.needs_rebuild());
    }

    /// A store written by an older binary carries a marker it never earned.
    ///
    /// The state field still says so today, but BOOT-405 deletes it — so the
    /// marker has to be corrected while something can still tell. Without this,
    /// an upgraded node would read a half-built projection as trusted.
    #[test]
    fn a_legacy_marker_on_an_unearned_generation_is_stripped_on_read() {
        for state in [
            ProjectionProvenance::Rebuilding,
            ProjectionProvenance::Invalidated,
        ] {
            let mut legacy = ProjectionGenerationProvenance { state, ..trusted() };
            assert!(
                legacy.is_trusted(),
                "{state:?}: the unearned marker reads as trusted"
            );
            assert!(legacy.normalize_legacy_marker(), "{state:?}");
            assert_eq!(legacy.source_contract_version, None, "{state:?}");
            assert!(!legacy.is_trusted(), "{state:?}");
        }
    }

    #[test]
    fn normalizing_a_generation_that_earned_its_marker_changes_nothing() {
        let mut certified = trusted();
        assert!(!certified.normalize_legacy_marker());
        assert!(certified.is_trusted());

        let mut legacy = ProjectionGenerationProvenance::legacy_unmarked();
        assert!(!legacy.normalize_legacy_marker());
        assert!(!legacy.is_trusted());
    }

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn healthy_state() -> PartitionStorageState {
        let commit_digest = digest('a');
        PartitionStorageState {
            partition_id: 3,
            requirements: StorageRequirements::managed_ha(),
            hot: HotJournalState {
                retained_floor: 5,
                tail: 10,
            },
            commit: QuorumCommitState {
                through: 10,
                digest_v1: Some(commit_digest.clone()),
                locally_verified_through: 10,
                composite_digest_v1: Some(commit_digest.clone()),
            },
            cold: vec![ColdJournalState {
                failure_domain: "primary".to_string(),
                catalog_through: 10,
                catalog_digest_v1: Some(commit_digest.clone()),
                catalog_digest_at_commit_v1: Some(commit_digest),
                proof_through: 10,
                proof_digest_v1: Some(digest('a')),
                catalog_digest_at_proof_v1: Some(digest('a')),
                authority_validated: true,
            }],
            selected_cold_failure_domain: Some("primary".to_string()),
            hot_cold_overlap: Some(HotColdOverlapState {
                range_start: 5,
                range_end_exclusive: 10,
                hot_digest_v1: digest('c'),
                cold_digest_v1: digest('c'),
            }),
            cold_index_through: Some(10),
            projection: Some(ProjectionState {
                generation_id: Some("projection-generation".to_string()),
                checkpoint_through: 10,
                applied_through: Some(10),
                replay_target: 10,
                provenance: ProjectionProvenance::CommitGatedTrusted,
            }),
            obligations: Vec::new(),
            guards: StorageGuards::default(),
        }
    }

    #[test]
    fn fully_verified_partition_is_healthy() {
        let assessment = assess_partition_storage(&healthy_state());
        assert_eq!(assessment.condition, StorageCondition::Healthy);
        assert_eq!(assessment.disposition, RecoveryDisposition::Healthy);
        assert!(assessment.authority_eligible);
        assert!(assessment.independent_recovery_ready);
        assert!(assessment.findings.is_empty());
    }

    #[test]
    fn uncommitted_suffix_waits_until_epoch_transition_authorizes_trim() {
        let mut state = healthy_state();
        state.hot.tail = 12;
        state.hot_cold_overlap.as_mut().unwrap().range_end_exclusive = 10;

        let waiting = assess_partition_storage(&state);
        assert_eq!(waiting.condition, StorageCondition::AvailabilityLag);
        assert_eq!(
            waiting.disposition,
            RecoveryDisposition::WaitForCommit { from: 10, to: 12 }
        );
        assert!(!waiting.authority_eligible);
        assert!(waiting.independent_recovery_ready);

        state.guards.epoch_transition_accepted = true;
        assert_eq!(
            assess_partition_storage(&state).disposition,
            RecoveryDisposition::TrimUncommittedHotSuffix { from: 10, to: 12 }
        );
    }

    #[test]
    fn overlap_digest_conflict_fences_instead_of_choosing_longest_tail() {
        let mut state = healthy_state();
        state.hot_cold_overlap.as_mut().unwrap().cold_digest_v1 = digest('d');

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::IntegrityDivergence);
        assert_eq!(assessment.disposition, RecoveryDisposition::FenceIntegrity);
        assert!(!assessment.authority_eligible);
        assert!(!assessment.independent_recovery_ready);
    }

    #[test]
    fn malformed_hot_range_requires_operator() {
        let mut state = healthy_state();
        state.hot.retained_floor = 11;

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::InvalidShape);
        assert_eq!(
            assessment.disposition,
            RecoveryDisposition::OperatorRequired
        );
    }

    #[test]
    fn legacy_projection_is_rebuilt_even_when_numeric_frontiers_look_safe() {
        let mut state = healthy_state();
        state.projection.as_mut().unwrap().provenance = ProjectionProvenance::LegacyUnmarked;
        state.projection.as_mut().unwrap().applied_through = None;

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::UnknownProvenance);
        assert_eq!(
            assessment.disposition,
            RecoveryDisposition::RebuildProjectionFromJournal { through: 10 }
        );
    }

    #[test]
    fn projection_ahead_of_commit_is_invalid_and_rebuilt() {
        let mut state = healthy_state();
        state.projection.as_mut().unwrap().applied_through = Some(11);

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::InvalidShape);
        assert_eq!(
            assessment.disposition,
            RecoveryDisposition::RebuildProjectionFromJournal { through: 10 }
        );
    }

    #[test]
    fn cold_lag_is_visible_without_blocking_journal_authority() {
        let mut state = healthy_state();
        let cold = state.cold.first_mut().unwrap();
        cold.catalog_through = 8;
        cold.proof_through = 8;
        state.cold_index_through = Some(8);
        state.hot_cold_overlap.as_mut().unwrap().range_end_exclusive = 8;

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::AvailabilityLag);
        assert_eq!(
            assessment.disposition,
            RecoveryDisposition::WaitForColdPublication { from: 8, to: 10 }
        );
        assert!(assessment.authority_eligible);
        assert!(!assessment.independent_recovery_ready);
    }

    #[test]
    fn cold_ahead_of_stale_local_certificate_advances_only_from_validated_source() {
        let mut state = healthy_state();
        state.commit.through = 8;
        state.commit.locally_verified_through = 8;
        state.hot.tail = 8;
        state.projection.as_mut().unwrap().checkpoint_through = 8;
        state.projection.as_mut().unwrap().applied_through = Some(8);
        state.projection.as_mut().unwrap().replay_target = 8;
        state.hot_cold_overlap.as_mut().unwrap().range_end_exclusive = 8;

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::AvailabilityLag);
        assert_eq!(
            assessment.disposition,
            RecoveryDisposition::AdvanceLocalCertificateFromCold {
                source: "primary".to_string(),
                from: 8,
                to: 10,
            }
        );

        state.cold.first_mut().unwrap().authority_validated = false;
        assert_eq!(
            assess_partition_storage(&state).disposition,
            RecoveryDisposition::FenceIntegrity
        );
    }

    #[test]
    fn index_ahead_of_catalog_rebuilds_instead_of_trusting_watermark() {
        let mut state = healthy_state();
        state.cold_index_through = Some(12);

        let assessment = assess_partition_storage(&state);
        assert_eq!(assessment.condition, StorageCondition::InvalidShape);
        assert_eq!(
            assessment.disposition,
            RecoveryDisposition::RebuildColdIndex { from: 12, to: 10 }
        );
        assert!(!assessment.authority_eligible);
    }

    #[test]
    fn incomplete_or_conflicting_digest_evidence_never_looks_healthy() {
        type EvidenceCase = (
            &'static str,
            fn(&mut PartitionStorageState),
            StorageCondition,
            RecoveryDisposition,
        );
        let cases: Vec<EvidenceCase> = vec![
            (
                "missing composite commit digest",
                |state| state.commit.composite_digest_v1 = None,
                StorageCondition::InvalidShape,
                RecoveryDisposition::OperatorRequired,
            ),
            (
                "unverified hot cold overlap",
                |state| state.hot_cold_overlap = None,
                StorageCondition::AvailabilityLag,
                RecoveryDisposition::VerifyHotColdOverlap { from: 5, to: 10 },
            ),
            (
                "cold disagrees with commit at the same boundary",
                |state| {
                    state.cold[0].catalog_digest_at_commit_v1 = Some(digest('d'));
                },
                StorageCondition::IntegrityDivergence,
                RecoveryDisposition::FenceIntegrity,
            ),
            (
                "cold proof digest missing",
                |state| state.cold[0].proof_digest_v1 = None,
                StorageCondition::InvalidShape,
                RecoveryDisposition::OperatorRequired,
            ),
        ];

        for (name, mutate, expected_condition, expected_disposition) in cases {
            let mut state = healthy_state();
            mutate(&mut state);
            let assessment = assess_partition_storage(&state);
            assert_eq!(assessment.condition, expected_condition, "{name}");
            assert_eq!(assessment.disposition, expected_disposition, "{name}");
            assert!(!assessment.authority_eligible, "{name}");
        }
    }
}
