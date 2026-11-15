use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// A single low-level write operation against a column family.
///
/// Built by the projection layer and applied atomically by the
/// store layer through `MaterializedStore::write_kv_batch`. The
/// `cf` field names a logical column family — the implementation
/// resolves it to a physical handle.
#[derive(Debug, Clone)]
pub enum KvOp {
    Put {
        cf: &'static str,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: &'static str,
        key: Vec<u8>,
    },
    QueueCounterDelta {
        project_id: String,
        deployment_id: String,
        priority: u8,
        delta: i64,
        ready_at_ms: i64,
        pending_key: Vec<u8>,
        updated_at_ms: i64,
    },
}

/// Builder for an atomic batch of writes against the materialized store.
///
/// The projection layer constructs one of these per record, fills it with
/// every CF write the projection demands (run + indexes + archive outbox
/// entry), and hands it to `MaterializedStore::write_kv_batch`. The store
/// commits all ops in a single underlying batch (`rocksdb::WriteBatch` for
/// the RocksDB impl) so a crash either lands the whole record's worth of
/// state or none of it — archive correctness depends on this.
#[derive(Debug, Default, Clone)]
pub struct BatchWriteOps {
    pub ops: Vec<KvOp>,
}

impl BatchWriteOps {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            ops: Vec::with_capacity(cap),
        }
    }

    pub fn put(&mut self, cf: &'static str, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(KvOp::Put { cf, key, value });
    }

    pub fn delete(&mut self, cf: &'static str, key: Vec<u8>) {
        self.ops.push(KvOp::Delete { cf, key });
    }

    // The arguments intentionally mirror the QueueCounterDelta variant so a
    // caller cannot accidentally omit a field from the atomic batch entry.
    #[allow(clippy::too_many_arguments)]
    pub fn queue_counter_delta(
        &mut self,
        project_id: String,
        deployment_id: String,
        priority: u8,
        delta: i64,
        ready_at_ms: i64,
        pending_key: Vec<u8>,
        updated_at_ms: i64,
    ) {
        self.ops.push(KvOp::QueueCounterDelta {
            project_id,
            deployment_id,
            priority,
            delta,
            ready_at_ms,
            pending_key,
            updated_at_ms,
        });
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// One time-ordered archived-run retention candidate.
///
/// `generation` identifies the acknowledged publication. Old candidates may
/// contain Parquet paths or an empty value; Vortex proof remains mandatory
/// before the managed sweeper deletes a hot row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunsRetentionEntry {
    pub raw_key: Vec<u8>,
    pub project_id: String,
    pub run_id: String,
    pub archived_at_ms: i64,
    pub generation: Vec<u8>,
}

/// One lossless archived-run read requested by recovery.
///
/// Recovery validates the returned protobuf as an authoritative terminal Run;
/// it does not require equality with a stale projection rebuilt under a newer
/// worker-session signing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedRunReadRequest {
    pub archive_directory: String,
    pub project_id: String,
    pub run_id: String,
}

/// One lossless archived Run selected for a bounded local projection seed.
///
/// The archive directory is retained alongside the exact protobuf so a hot
/// `GetRun` miss can still resolve the cold row after local eviction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedRunSeed {
    pub archive_directory: String,
    pub project_id: String,
    pub run_id: String,
    pub run_proto: Vec<u8>,
    pub archived_at_ms: i64,
}

/// A retention entry whose cold fallback was checked by the runtime's strict
/// archive reader immediately before the store compare-and-delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRunsRetentionEntry {
    pub entry: RunsRetentionEntry,
    /// Exact hot bytes observed before cold verification. `None` means the
    /// hot row was already absent and only index cleanup is allowed.
    pub expected_hot_run: Option<Vec<u8>>,
    /// Archive pointer observed before verification. The store compares this
    /// exact optional value again under its write lock; Vortex proof can also
    /// authorize rows whose legacy pointer is absent.
    pub verified_archive_directory: Option<Vec<u8>>,
}

/// Result of an atomic archived-run retention prune.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RunsRetentionPruneResult {
    /// Hot `CF_RUNS` rows removed after provenance and generation checks.
    pub deleted: u64,
    /// Obsolete entries removed because a newer generation is current.
    pub stale_cleaned: u64,
    /// Legacy or malformed entries removed without touching the hot row.
    pub unproven_cleaned: u64,
    /// Entries kept because no proven cold fallback was available.
    pub unresolved: u64,
    /// Legacy hot rows queued for lossless re-archive and index restoration.
    pub rearchive_staged: u64,
}

/// Exact pending-queue rollup maintained by the run projection.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueCounter {
    pub pending_count: u64,
    pub oldest_ready_at_ms: Option<i64>,
    pub oldest_pending_key: Option<Vec<u8>>,
    pub updated_at_ms: i64,
}

/// Stable position in the lease-expiry index.
///
/// The tuple fields match the index's total ordering:
/// `(expires_at_ms, project_id, run_id)`. Expiry scans treat this as a
/// strict start-after cursor so a retained or corrupt row at the head of the
/// index cannot starve later due leases.
pub type LeaseExpiryCursor = (i64, String, String);

/// Stable pagination cursor for a partition's terminal-obligation index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalObligationCursor {
    pub project_id: String,
    pub run_id: String,
}

/// One indexed non-terminal run returned to the partition owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalObligationEntry {
    pub project_id: String,
    pub run_id: String,
    pub run_data: Vec<u8>,
}

/// One sealed-segment chunk that holds a slice of a run's records.
///
/// The segment compactor produces one chunk per
/// `(run_id, sealed_segment_path)` pair after a Parquet PUT succeeds.
/// Stored under `CF_RUN_SEGMENTS`; read on the cold-fallback path
/// when `CF_EVENTS` doesn't cover a requested window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSegmentChunk {
    /// S3 path to the Parquet file (e.g., `segments/p0/seg-…parquet`).
    pub path: String,
    /// First offset of THIS run's records in the segment. Doubles as
    /// the in-CF row key tail so chunks sort in the right order.
    pub first_offset: u64,
    /// Last offset of THIS run's records in the segment.
    pub last_offset: u64,
    /// Number of this run's records in the segment. Hint for the
    /// reader to size buffers; not load-bearing for correctness.
    pub record_count: u32,
}

/// Offset is a monotonically increasing position in the journal log.
/// Encoded as big-endian u64 bytes for natural RocksDB sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Offset(pub u64);

impl Offset {
    pub const ZERO: Offset = Offset(0);
    pub const INVALID: Offset = Offset(u64::MAX);

    /// Encode as big-endian bytes for RocksDB key ordering.
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Decode from big-endian bytes.
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Offset(u64::from_be_bytes(bytes))
    }

    /// Next offset.
    pub fn next(self) -> Self {
        Offset(self.0 + 1)
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Offset {
    fn from(v: u64) -> Self {
        Offset(v)
    }
}

impl From<Offset> for u64 {
    fn from(o: Offset) -> Self {
        o.0
    }
}

/// Record is the fundamental unit stored in the journal log.
/// All orchestration state is derived from records.
///
/// The limit makes journal replay memory predictable: cold readers can choose
/// their Parquet batch size before Arrow allocates a batch, knowing that every
/// record accepted by a fresh beta cluster is bounded.
pub const MAX_JOURNAL_RECORD_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Offset assigned by the sequencer.
    pub offset: Offset,

    /// Project identity — the execution / routing scope.
    pub project_id: String,

    /// Run this record belongs to.
    pub run_id: String,

    /// Event type determines how the processor materializes this record.
    pub event_type: String,

    /// JSON-encoded event payload.
    pub data: bytes::Bytes,

    /// Nanosecond-precision timestamp.
    pub timestamp_ns: i64,

    /// Step memoization key (present for step completion events).
    pub step_key: Option<String>,

    /// Correlation ID for pairing start/complete events.
    pub correlation_id: Option<String>,

    /// Parent event ID for event hierarchy.
    pub parent_event_id: Option<String>,

    /// Display metadata for UI rendering.
    pub metadata: std::collections::HashMap<String, String>,

    /// Data format: "json" or "binary".
    pub data_type: String,

    /// xxHash64 checksum of data (before compression).
    pub data_checksum: Option<bytes::Bytes>,

    /// Whether data is zstd-compressed.
    pub data_compressed: bool,
}

impl Record {
    /// Names of fields whose canonical values differ, without exposing field
    /// contents. This is safe to attach to integrity diagnostics that must not
    /// log journal payloads or metadata values.
    pub fn canonical_differing_fields(&self, other: &Self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.offset != other.offset {
            fields.push("offset");
        }
        if self.project_id != other.project_id {
            fields.push("project_id");
        }
        if self.run_id != other.run_id {
            fields.push("run_id");
        }
        if self.event_type != other.event_type {
            fields.push("event_type");
        }
        if self.data != other.data {
            fields.push("data");
        }
        if self.timestamp_ns != other.timestamp_ns {
            fields.push("timestamp_ns");
        }
        if canonical_optional_str(self.step_key.as_deref())
            != canonical_optional_str(other.step_key.as_deref())
        {
            fields.push("step_key");
        }
        if canonical_optional_str(self.correlation_id.as_deref())
            != canonical_optional_str(other.correlation_id.as_deref())
        {
            fields.push("correlation_id");
        }
        if canonical_optional_str(self.parent_event_id.as_deref())
            != canonical_optional_str(other.parent_event_id.as_deref())
        {
            fields.push("parent_event_id");
        }
        if self.metadata != other.metadata {
            fields.push("metadata");
        }
        if canonical_data_type(&self.data_type) != canonical_data_type(&other.data_type) {
            fields.push("data_type");
        }
        if canonical_optional_bytes(self.data_checksum.as_deref())
            != canonical_optional_bytes(other.data_checksum.as_deref())
        {
            fields.push("data_checksum");
        }
        if self.data_compressed != other.data_compressed {
            fields.push("data_compressed");
        }
        fields
    }

    /// Return the version-1 canonical SHA-256 identity for this journal record.
    ///
    /// Unlike protobuf encoding, this representation does not depend on
    /// `HashMap` iteration order. Every field is length-delimited, metadata is
    /// sorted by key, and the domain prefix makes future incompatible digest
    /// formats explicit rather than silently changing record identity.
    pub fn canonical_digest_v1(&self) -> [u8; 32] {
        let Record {
            offset,
            project_id,
            run_id,
            event_type,
            data,
            timestamp_ns,
            step_key,
            correlation_id,
            parent_event_id,
            metadata,
            data_type,
            data_checksum,
            data_compressed,
        } = self;

        let mut digest = Sha256::new();
        digest.update(b"agnt5.journal-record.canonical-digest.v1\0");
        update_digest_field(&mut digest, 1, &offset.0.to_be_bytes());
        update_digest_field(&mut digest, 2, project_id.as_bytes());
        update_digest_field(&mut digest, 3, run_id.as_bytes());
        update_digest_field(&mut digest, 4, event_type.as_bytes());
        update_digest_field(&mut digest, 5, data.as_ref());
        update_digest_field(&mut digest, 6, &timestamp_ns.to_be_bytes());
        update_optional_digest_field(
            &mut digest,
            7,
            step_key
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(str::as_bytes),
        );
        update_optional_digest_field(
            &mut digest,
            8,
            correlation_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(str::as_bytes),
        );
        update_optional_digest_field(
            &mut digest,
            9,
            parent_event_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(str::as_bytes),
        );

        digest.update([10]);
        digest.update((metadata.len() as u64).to_be_bytes());
        let mut sorted_metadata: Vec<_> = metadata.iter().collect();
        sorted_metadata.sort_unstable_by_key(|(key, _)| *key);
        for (key, value) in sorted_metadata {
            update_digest_bytes(&mut digest, key.as_bytes());
            update_digest_bytes(&mut digest, value.as_bytes());
        }

        update_digest_field(&mut digest, 11, canonical_data_type(data_type));
        update_optional_digest_field(
            &mut digest,
            12,
            canonical_optional_bytes(data_checksum.as_deref()),
        );
        update_digest_field(&mut digest, 13, &[*data_compressed as u8]);

        digest.finalize().into()
    }

    /// Lowercase hexadecimal form of [`Record::canonical_digest_v1`].
    pub fn canonical_digest_v1_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let digest = self.canonical_digest_v1();
        let mut encoded = String::with_capacity(digest.len() * 2);
        for byte in digest {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    /// Conservative in-memory size used by append admission and cold replay.
    ///
    /// Saturating arithmetic deliberately turns an impossible accounting
    /// overflow into an over-limit record instead of wrapping it small.
    pub fn decoded_size_bytes(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        for field_size in [
            self.project_id.len(),
            self.run_id.len(),
            self.event_type.len(),
            self.data.len(),
            self.step_key.as_ref().map_or(0, String::len),
            self.correlation_id.as_ref().map_or(0, String::len),
            self.parent_event_id.as_ref().map_or(0, String::len),
            self.data_type.len(),
            self.data_checksum.as_ref().map_or(0, bytes::Bytes::len),
        ] {
            size = size.saturating_add(field_size);
        }
        // Cold Parquet stores metadata as JSON. A control character can expand
        // to a six-byte `\u00xx` escape, so account for that worst case plus
        // quotes/separators before using this value as a row-group bound.
        size = size.saturating_add(2);
        for (key, value) in &self.metadata {
            size = size
                .saturating_add(key.len().saturating_mul(6))
                .saturating_add(value.len().saturating_mul(6))
                .saturating_add(6);
        }
        size
    }

    /// Convert from proto Record message.
    pub fn from_proto(proto: engine_proto::Record) -> Self {
        Record {
            offset: Offset(proto.offset),
            project_id: proto.project_id,
            run_id: proto.run_id,
            event_type: proto.event_type,
            data: bytes::Bytes::from(proto.data),
            timestamp_ns: proto.timestamp_ns,
            step_key: if proto.step_key.is_empty() {
                None
            } else {
                Some(proto.step_key)
            },
            correlation_id: if proto.correlation_id.is_empty() {
                None
            } else {
                Some(proto.correlation_id)
            },
            parent_event_id: if proto.parent_event_id.is_empty() {
                None
            } else {
                Some(proto.parent_event_id)
            },
            metadata: proto.metadata,
            data_type: if proto.data_type.is_empty() {
                "json".to_string()
            } else {
                proto.data_type
            },
            data_checksum: if proto.data_checksum.is_empty() {
                None
            } else {
                Some(bytes::Bytes::from(proto.data_checksum))
            },
            data_compressed: proto.data_compressed,
        }
    }

    /// Convert to proto Record message.
    pub fn to_proto(&self) -> engine_proto::Record {
        engine_proto::Record {
            offset: self.offset.0,
            project_id: self.project_id.clone(),
            run_id: self.run_id.clone(),
            event_type: self.event_type.clone(),
            data: self.data.to_vec(),
            timestamp_ns: self.timestamp_ns,
            step_key: self.step_key.clone().unwrap_or_default(),
            correlation_id: self.correlation_id.clone().unwrap_or_default(),
            parent_event_id: self.parent_event_id.clone().unwrap_or_default(),
            metadata: self.metadata.clone(),
            data_type: self.data_type.clone(),
            data_checksum: self
                .data_checksum
                .as_ref()
                .map(|c| c.to_vec())
                .unwrap_or_default(),
            data_compressed: self.data_compressed,
        }
    }
}

/// Extend the replication commit-chain digest with an offset-contiguous batch.
///
/// The prior digest is included verbatim, so equal final digests commit to the
/// entire journal prefix rather than only to the record at the boundary. An
/// empty prior digest denotes the empty log. Callers must supply records in
/// offset order and separately enforce contiguity with their commit boundary.
pub fn extend_commit_chain_digest_v1(previous_digest: &str, records: &[Record]) -> String {
    use sha2::{Digest, Sha256};

    let mut previous = previous_digest.to_string();
    for record in records {
        let mut digest = Sha256::new();
        digest.update(b"agnt5.journal-commit-chain.v1\0");
        digest.update((previous.len() as u64).to_be_bytes());
        digest.update(previous.as_bytes());
        digest.update(record.offset.0.to_be_bytes());
        digest.update(record.canonical_digest_v1());
        let bytes = digest.finalize();
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            write!(&mut encoded, "{byte:02x}").expect("write to String cannot fail");
        }
        previous = encoded;
    }
    previous
}

fn update_digest_field(digest: &mut Sha256, tag: u8, value: &[u8]) {
    digest.update([tag]);
    update_digest_bytes(digest, value);
}

fn canonical_optional_str(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn canonical_optional_bytes(value: Option<&[u8]>) -> Option<&[u8]> {
    value.filter(|value| !value.is_empty())
}

fn canonical_data_type(value: &str) -> &[u8] {
    if value.is_empty() {
        b"json"
    } else {
        value.as_bytes()
    }
}

fn update_optional_digest_field(digest: &mut Sha256, tag: u8, value: Option<&[u8]>) {
    digest.update([tag]);
    match value {
        Some(value) => {
            digest.update([1]);
            update_digest_bytes(digest, value);
        }
        None => digest.update([0]),
    }
}

fn update_digest_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod record_digest_tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn record() -> Record {
        Record {
            offset: Offset(42),
            project_id: "project-1".to_string(),
            run_id: "run-1".to_string(),
            event_type: "run.completed".to_string(),
            data: Bytes::from_static(br#"{"ok":true}"#),
            timestamp_ns: 1_725_000_000_123_456_789,
            step_key: Some("step-1".to_string()),
            correlation_id: Some("correlation-1".to_string()),
            parent_event_id: Some("event-0".to_string()),
            metadata: HashMap::from([
                ("zeta".to_string(), "last".to_string()),
                ("alpha".to_string(), "first".to_string()),
            ]),
            data_type: "json".to_string(),
            data_checksum: Some(Bytes::from_static(b"checksum")),
            data_compressed: true,
        }
    }

    fn assert_digest_changes(base: &Record, mutate: impl FnOnce(&mut Record)) {
        let mut changed = base.clone();
        mutate(&mut changed);
        assert_ne!(base.canonical_digest_v1(), changed.canonical_digest_v1());
    }

    #[test]
    fn canonical_digest_v1_has_a_stable_vector() {
        assert_eq!(
            record().canonical_digest_v1_hex(),
            "6823948b62f02f70ed4a9b030c64422bf443639d76a303e91e6ccce77bc497d0"
        );
    }

    #[test]
    fn canonical_digest_v1_ignores_metadata_insertion_order() {
        let first = record();
        let mut second = first.clone();
        second.metadata.clear();
        second
            .metadata
            .insert("alpha".to_string(), "first".to_string());
        second
            .metadata
            .insert("zeta".to_string(), "last".to_string());

        assert_eq!(first.canonical_digest_v1(), second.canonical_digest_v1());
    }

    #[test]
    fn canonical_digest_v1_survives_proto_roundtrip() {
        let original = record();
        let decoded = Record::from_proto(original.to_proto());
        assert_eq!(
            original.canonical_digest_v1(),
            decoded.canonical_digest_v1()
        );
    }

    #[test]
    fn canonical_digest_v1_normalizes_proto_default_values() {
        let mut original = record();
        original.step_key = Some(String::new());
        original.correlation_id = Some(String::new());
        original.parent_event_id = Some(String::new());
        original.data_type.clear();
        original.data_checksum = Some(Bytes::new());

        let decoded = Record::from_proto(original.to_proto());
        assert_eq!(
            original.canonical_digest_v1(),
            decoded.canonical_digest_v1()
        );
    }

    #[test]
    fn canonical_digest_v1_covers_every_record_field() {
        let base = record();
        assert_digest_changes(&base, |record| record.offset = Offset(43));
        assert_digest_changes(&base, |record| record.project_id.push('x'));
        assert_digest_changes(&base, |record| record.run_id.push('x'));
        assert_digest_changes(&base, |record| record.event_type.push('x'));
        assert_digest_changes(&base, |record| {
            record.data = Bytes::from_static(b"different")
        });
        assert_digest_changes(&base, |record| record.timestamp_ns += 1);
        assert_digest_changes(&base, |record| record.step_key = None);
        assert_digest_changes(&base, |record| record.correlation_id = None);
        assert_digest_changes(&base, |record| record.parent_event_id = None);
        assert_digest_changes(&base, |record| {
            record
                .metadata
                .insert("alpha".to_string(), "changed".to_string());
        });
        assert_digest_changes(&base, |record| record.data_type.push('x'));
        assert_digest_changes(&base, |record| record.data_checksum = None);
        assert_digest_changes(&base, |record| record.data_compressed = false);
    }

    #[test]
    fn canonical_difference_summary_names_fields_without_values() {
        let base = record();
        let mut changed = base.clone();
        changed.event_type = "sensitive-event-value".to_string();
        changed.data = Bytes::from_static(b"sensitive-payload-value");
        changed
            .metadata
            .insert("sensitive-key".to_string(), "sensitive-value".to_string());

        let summary = base.canonical_differing_fields(&changed);
        assert_eq!(summary, vec!["event_type", "data", "metadata"]);
        let rendered = summary.join(",");
        assert!(!rendered.contains("sensitive"));
    }

    #[test]
    fn commit_chain_digest_is_independent_of_commit_batch_boundaries() {
        let first = record();
        let mut second = first.clone();
        second.offset = first.offset.next();
        second.event_type = "run.archived".to_string();

        let together = extend_commit_chain_digest_v1("", &[first.clone(), second.clone()]);
        let first_commit = extend_commit_chain_digest_v1("", &[first]);
        let separately = extend_commit_chain_digest_v1(&first_commit, &[second]);

        assert_eq!(together, separately);
    }

    #[test]
    fn commit_chain_digest_detects_a_changed_record() {
        let original = record();
        let mut changed = original.clone();
        changed.data = Bytes::from_static(br#"{"ok":false}"#);

        assert_ne!(
            extend_commit_chain_digest_v1("", &[original]),
            extend_commit_chain_digest_v1("", &[changed])
        );
    }
}
