use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Notify, RwLock};

use engine_core::activation::ActivationState;
use engine_core::activation_authority::{
    activation_head_key, activation_head_run_prefix, authority_probe, decide_authority,
    decode_activation_state, decode_run_lease_entry, encode_activation_state,
    encode_run_lease_entry, nonterminal_activation, required_child_obligation_key,
    required_child_obligation_prefix, run_gate_key, run_lease_key, AuthorityContext,
    AuthorityDecision, AuthorityMode, AuthorityProbe, ObligationMutation, RunLeaseEntry,
};
use engine_core::error::{EngineError, EngineResult};
use engine_core::journal_contract::{
    record_idempotency_key, record_idempotency_validation_fingerprint, record_state_version_claims,
    validate_append_records, validate_idempotency_validation_fingerprint,
    validate_idempotent_duplicate, validate_replicated_offsets, validate_state_version_claim,
};
use engine_core::traits::{AppendOutcome, RecordStream, Segment};

use crate::Placement;
use engine_core::types::{Offset, Record};

#[cfg(feature = "test-hooks")]
use crate::AppendLifecycleTestHook;

/// In-memory state behind the lock.
struct Inner {
    records: Vec<Record>,
    /// The partition's quorum commit certificate (BOOT-607), mirrored here so
    /// the replication path is assertable without standing up RocksDB.
    commit_certificate: Option<(Offset, String)>,
    /// The prefix proof (BOOT-501/607), mirrored for the same reason.
    verified_prefix: Option<(Offset, String)>,
    /// Offset of the first record still stored (advances on trim).
    base_offset: u64,
    /// Durable-segment equivalent idempotency index for tests.
    idempotency_offsets: HashMap<Vec<u8>, (Offset, Option<[u8; 32]>)>,
    /// Journal-authoritative optimistic versions for activation state bundles.
    state_versions: HashMap<Vec<u8>, i64>,
    /// Append-atomic Durable Activation aggregate heads, ordered for run-prefix reads.
    activation_heads: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Unsettled required children, ordered for run/parent-prefix checks.
    required_child_obligations: BTreeMap<Vec<u8>, String>,
    /// Runs that have accepted a terminal transition intent.
    activation_run_gates: HashMap<Vec<u8>, Offset>,
    /// Lease entries folded from accepted lease events, keyed per run.
    run_leases: HashMap<Vec<u8>, Vec<u8>>,
    /// Full derived-state snapshots at append-batch boundaries. These make
    /// test/dev tail repair atomic with the same semantics as RocksDB's
    /// per-batch undo records.
    tail_undo: BTreeMap<u64, DerivedStateSnapshot>,
}

#[derive(Clone)]
struct DerivedStateSnapshot {
    idempotency_offsets: HashMap<Vec<u8>, (Offset, Option<[u8; 32]>)>,
    state_versions: HashMap<Vec<u8>, i64>,
    activation_heads: BTreeMap<Vec<u8>, Vec<u8>>,
    required_child_obligations: BTreeMap<Vec<u8>, String>,
    activation_run_gates: HashMap<Vec<u8>, Offset>,
    run_leases: HashMap<Vec<u8>, Vec<u8>>,
}

/// In-memory segment for testing. Not durable — all data lives in a Vec.
pub struct MemorySegment {
    inner: Arc<RwLock<Inner>>,
    /// Next offset to assign (one past the last committed record).
    next_offset: AtomicU64,
    /// Whether this segment has been sealed.
    ///
    /// Shared with `tail` streams, which outlive the borrow they would
    /// otherwise need. Arc rather than a raw-pointer cast — a stream must not
    /// be able to dereference a dropped segment.
    sealed: Arc<AtomicBool>,
    /// Exclusive offset this segment was sealed at; `u64::MAX` when unknown.
    /// Mirrors the durable record `RocksDBSegment` keeps, so seal-path tests
    /// can assert the boundary end to end.
    sealed_through: Arc<AtomicU64>,
    #[cfg(feature = "test-hooks")]
    append_lifecycle_test_hook: Option<Arc<AppendLifecycleTestHook>>,
    /// Wakes tail streams when new records are appended.
    notify: Arc<Notify>,
}

impl MemorySegment {
    /// Create a new empty MemorySegment starting at the given offset.
    pub fn new(start_offset: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                commit_certificate: None,
                verified_prefix: None,
                records: Vec::new(),
                base_offset: start_offset,
                idempotency_offsets: HashMap::new(),
                state_versions: HashMap::new(),
                activation_heads: BTreeMap::new(),
                required_child_obligations: BTreeMap::new(),
                activation_run_gates: HashMap::new(),
                run_leases: HashMap::new(),
                tail_undo: BTreeMap::new(),
            })),
            next_offset: AtomicU64::new(start_offset),
            sealed: Arc::new(AtomicBool::new(false)),
            sealed_through: Arc::new(AtomicU64::new(u64::MAX)),
            #[cfg(feature = "test-hooks")]
            append_lifecycle_test_hook: None,
            notify: Arc::new(Notify::new()),
        }
    }

    #[cfg(feature = "test-hooks")]
    pub fn install_append_lifecycle_test_hook(&mut self, hook: Arc<AppendLifecycleTestHook>) {
        self.append_lifecycle_test_hook = Some(hook);
    }
}

impl MemorySegment {
    async fn append_batch_with_mode(
        &self,
        records: &[Record],
        mode: AuthorityMode,
        placement: Placement,
    ) -> EngineResult<AppendOutcome> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(EngineError::Sealed);
        }

        if records.is_empty() {
            return Ok(AppendOutcome::all_appended(Vec::new()));
        }

        validate_append_records(records)?;

        let mut inner = self.inner.write().await;

        // The record write lock is also the lifecycle transition lock. An
        // append that reaches this check linearizes before a later seal;
        // otherwise it observes the seal and rejects without mutation.
        if self.sealed.load(Ordering::Acquire) {
            return Err(EngineError::Sealed);
        }
        #[cfg(feature = "test-hooks")]
        if let Some(hook) = &self.append_lifecycle_test_hook {
            hook.pause_once();
        }

        let mut next = self.next_offset.load(Ordering::Acquire);
        let batch_start = next;
        let undo_snapshot = DerivedStateSnapshot {
            idempotency_offsets: inner.idempotency_offsets.clone(),
            state_versions: inner.state_versions.clone(),
            activation_heads: inner.activation_heads.clone(),
            required_child_obligations: inner.required_child_obligations.clone(),
            activation_run_gates: inner.activation_run_gates.clone(),
            run_leases: inner.run_leases.clone(),
        };
        let mut offsets = vec![Offset::ZERO; records.len()];
        let mut appended = vec![false; records.len()];
        let mut pending_records = Vec::<Record>::new();
        let mut pending_idempotency = HashMap::<Vec<u8>, (Offset, usize)>::new();
        let mut pending_state_versions = inner.state_versions.clone();
        let mut pending_activation_heads = inner.activation_heads.clone();
        let mut pending_required_child_obligations = inner.required_child_obligations.clone();
        let mut pending_activation_run_gates = inner.activation_run_gates.clone();
        let mut pending_run_leases = inner.run_leases.clone();

        for (i, record) in records.iter().enumerate() {
            if let Some(key) = record_idempotency_key(record) {
                if let Some((offset, prior_index)) = pending_idempotency.get(&key).copied() {
                    validate_idempotent_duplicate(&records[prior_index], record)?;
                    offsets[i] = offset;
                    continue;
                }
                if let Some((offset, validation_fingerprint)) =
                    inner.idempotency_offsets.get(&key).copied()
                {
                    let existing = inner
                        .records
                        .iter()
                        .find(|existing| existing.offset == offset);
                    if let Some(existing) = existing {
                        validate_idempotent_duplicate(existing, record)?;
                    } else if offset.0 < inner.base_offset {
                        validate_idempotency_validation_fingerprint(
                            validation_fingerprint.as_ref(),
                            record,
                        )?;
                    } else {
                        return Err(EngineError::Storage(format!(
                            "idempotency index points to missing offset {}",
                            offset.0
                        )));
                    }
                    offsets[i] = offset;
                    continue;
                }
                pending_idempotency.insert(key, (Offset(next), i));
            }

            let offset = Offset(next);
            for claim in record_state_version_claims(record)? {
                let current = pending_state_versions.get(&claim.index_key).copied();
                validate_state_version_claim(current, &claim)?;
                pending_state_versions.insert(claim.index_key, claim.next_version);
            }

            if let Some(probe) = authority_probe(record)? {
                let (project_id, run_id) = probe.run_scope();
                let (activation_id, parent_id, child_scope) = match &probe {
                    AuthorityProbe::Activation(probe) => (
                        Some(probe.activation_id.as_str()),
                        probe
                            .requires_parent_active
                            .then_some(probe.parent_activation_id.as_str()),
                        probe
                            .requires_children_settled
                            .then_some(Some(probe.activation_id.as_str())),
                    ),
                    AuthorityProbe::RunAssignment(_) | AuthorityProbe::RunLease(_) => {
                        (None, None, None)
                    }
                    AuthorityProbe::RunTerminal(probe) => {
                        (None, None, probe.requires_children_settled.then_some(None))
                    }
                };
                let current = activation_id
                    .map(|activation_id| activation_head_key(project_id, run_id, activation_id))
                    .and_then(|key| pending_activation_heads.get(&key))
                    .map(|bytes| decode_activation_state(bytes))
                    .transpose()?;
                let parent = parent_id
                    .map(|parent_id| activation_head_key(project_id, run_id, parent_id))
                    .and_then(|key| pending_activation_heads.get(&key))
                    .map(|bytes| decode_activation_state(bytes))
                    .transpose()?;
                let first_unsettled_required_child = child_scope.and_then(|parent| {
                    let prefix = required_child_obligation_prefix(project_id, run_id, parent);
                    pending_required_child_obligations
                        .range(prefix.clone()..)
                        .take_while(|(key, _)| key.starts_with(&prefix))
                        .map(|(_, child_id)| child_id.clone())
                        .next()
                });
                let run_terminal_offset = pending_activation_run_gates
                    .get(&run_gate_key(project_id, run_id))
                    .copied();
                let lease_key = run_lease_key(project_id, run_id);
                let lease = pending_run_leases
                    .get(&lease_key)
                    .map(|bytes| decode_run_lease_entry(bytes))
                    .transpose()?;
                match decide_authority(
                    &probe,
                    offset,
                    AuthorityContext {
                        current,
                        parent,
                        first_unsettled_required_child,
                        run_terminal_offset,
                        lease,
                    },
                    mode,
                )? {
                    AuthorityDecision::Activation { state, obligation } => {
                        pending_activation_heads.insert(
                            activation_head_key(
                                &state.identity.project_id,
                                &state.identity.run_id,
                                &state.identity.activation_id,
                            ),
                            encode_activation_state(&state),
                        );
                        match obligation {
                            ObligationMutation::None => {}
                            ObligationMutation::Upsert {
                                project_id,
                                run_id,
                                parent_activation_id,
                                child_activation_id,
                            } => {
                                pending_required_child_obligations.insert(
                                    required_child_obligation_key(
                                        &project_id,
                                        &run_id,
                                        &parent_activation_id,
                                        &child_activation_id,
                                    ),
                                    child_activation_id,
                                );
                            }
                            ObligationMutation::Delete {
                                project_id,
                                run_id,
                                parent_activation_id,
                                child_activation_id,
                            } => {
                                pending_required_child_obligations.remove(
                                    &required_child_obligation_key(
                                        &project_id,
                                        &run_id,
                                        &parent_activation_id,
                                        &child_activation_id,
                                    ),
                                );
                            }
                        }
                    }
                    AuthorityDecision::RunAssignment { lease, .. }
                    | AuthorityDecision::RunLease { lease, .. } => {
                        pending_run_leases.insert(lease_key, encode_run_lease_entry(&lease));
                    }
                    AuthorityDecision::RunTerminal {
                        project_id,
                        run_id,
                        accepted_offset,
                        already_terminal,
                        lease,
                    } => {
                        if let Some(lease) = &lease {
                            pending_run_leases.insert(lease_key, encode_run_lease_entry(lease));
                        }
                        if !already_terminal {
                            pending_activation_run_gates
                                .insert(run_gate_key(&project_id, &run_id), accepted_offset);
                        }
                    }
                }
            }

            next += 1;
            let mut stored = record.clone();
            stored.offset = offset;
            pending_records.push(stored);
            offsets[i] = offset;
            appended[i] = true;
        }

        // Checked under the write lock and before any state changes, so a
        // tail that moved after the caller's own check rejects the batch.
        if placement == Placement::AtRecordOffsets {
            validate_replicated_offsets(records, &offsets)?;
        }

        inner.records.extend(pending_records);
        inner
            .idempotency_offsets
            .extend(
                pending_idempotency
                    .into_iter()
                    .map(|(key, (offset, index))| {
                        (
                            key,
                            (
                                offset,
                                record_idempotency_validation_fingerprint(&records[index]),
                            ),
                        )
                    }),
            );
        inner.state_versions = pending_state_versions;
        inner.activation_heads = pending_activation_heads;
        inner.required_child_obligations = pending_required_child_obligations;
        inner.activation_run_gates = pending_activation_run_gates;
        inner.run_leases = pending_run_leases;

        self.next_offset.store(next, Ordering::Release);

        if appended.iter().any(|&v| v) {
            inner.tail_undo.insert(batch_start, undo_snapshot);
            // Wake any tail streams waiting for new records.
            self.notify.notify_waiters();
        }

        Ok(AppendOutcome { offsets, appended })
    }
}

#[async_trait]
impl Segment for MemorySegment {
    async fn lookup_invocation(
        &self,
        project_id: &str,
        _run_id: &str,
        caller_key: &str,
    ) -> EngineResult<engine_core::traits::InvocationLookup> {
        use engine_core::traits::InvocationLookup;
        let key = engine_core::journal_contract::invocation_index_key(project_id, caller_key);
        Ok(self
            .inner
            .read()
            .await
            .idempotency_offsets
            .get(&key)
            .map_or(InvocationLookup::Missing, |(offset, _)| {
                InvocationLookup::Found(*offset)
            }))
    }

    async fn append_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>> {
        Ok(self.append_batch_with_outcome(records).await?.offsets)
    }

    async fn append_replicated_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>> {
        Ok(self
            .append_batch_with_mode(
                records,
                AuthorityMode::TrustedHistory,
                Placement::AtRecordOffsets,
            )
            .await?
            .offsets)
    }

    async fn append_batch_with_outcome(&self, records: &[Record]) -> EngineResult<AppendOutcome> {
        self.append_batch_with_mode(records, AuthorityMode::Live, Placement::AtTail)
            .await
    }

    async fn load_activation_authority(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<ActivationState>> {
        self.inner
            .read()
            .await
            .activation_heads
            .get(&activation_head_key(project_id, run_id, activation_id))
            .map(|bytes| decode_activation_state(bytes))
            .transpose()
    }

    async fn load_run_lease_authority(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<RunLeaseEntry>> {
        self.inner
            .read()
            .await
            .run_leases
            .get(&run_lease_key(project_id, run_id))
            .map(|bytes| decode_run_lease_entry(bytes))
            .transpose()
    }

    async fn first_unsettled_required_child(
        &self,
        project_id: &str,
        run_id: &str,
        parent_activation_id: Option<&str>,
    ) -> EngineResult<Option<String>> {
        let prefix = required_child_obligation_prefix(project_id, run_id, parent_activation_id);
        Ok(self
            .inner
            .read()
            .await
            .required_child_obligations
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(_, child_id)| child_id.clone())
            .next())
    }

    async fn list_nonterminal_activation_authority(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<ActivationState>> {
        let prefix = activation_head_run_prefix(project_id, run_id);
        self.inner
            .read()
            .await
            .activation_heads
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.starts_with(&prefix))
            .map(|(_, bytes)| decode_activation_state(bytes))
            .filter_map(|state| match state {
                Ok(state) if nonterminal_activation(&state) => Some(Ok(state)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    async fn tail(&self, from: Offset) -> EngineResult<RecordStream> {
        let inner = self.inner.clone();
        let notify = self.notify.clone();
        let sealed = self.sealed.clone();

        let stream = async_stream::stream! {
            let mut cursor = from.0;

            loop {
                // Register interest BEFORE reading state so that a
                // concurrent append / seal / unseal cannot slip between
                // the state check and the await (lost-wakeup prevention).
                let notified = notify.notified();

                let batch = {
                    let guard = inner.read().await;
                    let _is_sealed = sealed.load(Ordering::Acquire);

                    if cursor < guard.base_offset {
                        let retained_floor = guard.base_offset;
                        drop(guard);
                        yield Err(EngineError::Trimmed {
                            requested: cursor,
                            retained_floor,
                        });
                        return;
                    }

                    let start_idx = (cursor - guard.base_offset) as usize;
                    if start_idx < guard.records.len() {
                        Some(guard.records[start_idx..].to_vec())
                    } else {
                        None
                    }
                };

                if let Some(batch) = batch {
                    cursor += batch.len() as u64;
                    for record in batch {
                        yield Ok(record);
                    }
                    continue;
                }

                // No unread records — wait for any state change
                // (append, seal, or prepare_for_new_epoch unseal).
                notified.await;
            }
        };

        Ok(Box::pin(stream))
    }

    async fn read_range(&self, from: Offset, to: Offset) -> EngineResult<Vec<Record>> {
        if to < from {
            return Err(EngineError::InvalidArgument(format!(
                "invalid read range [{from}, {to})"
            )));
        }
        let inner = self.inner.read().await;

        if from.0 < inner.base_offset {
            return Err(EngineError::Trimmed {
                requested: from.0,
                retained_floor: inner.base_offset,
            });
        }

        let start_idx = (from.0 - inner.base_offset) as usize;
        let end_idx = (to.0 - inner.base_offset) as usize;

        let end_idx = end_idx.min(inner.records.len());
        if start_idx >= inner.records.len() {
            return Ok(Vec::new());
        }

        Ok(inner.records[start_idx..end_idx].to_vec())
    }

    async fn record_commit_certificate(
        &self,
        through: Offset,
        digest_v1: &str,
    ) -> EngineResult<()> {
        if through.0 > 0 && digest_v1.is_empty() {
            return Err(EngineError::Storage(
                "a non-zero commit boundary requires a digest".to_string(),
            ));
        }
        let mut inner = self.inner.write().await;
        if let Some((current, _)) = inner.commit_certificate.as_ref() {
            if through.0 < current.0 {
                return Err(EngineError::Storage(format!(
                    "commit boundary may not regress from {} to {}",
                    current.0, through.0
                )));
            }
        }
        inner.commit_certificate = Some((through, digest_v1.to_string()));
        Ok(())
    }

    async fn load_commit_certificate(&self) -> EngineResult<Option<(Offset, String)>> {
        Ok(self.inner.read().await.commit_certificate.clone())
    }

    async fn record_verified_prefix(&self, through: Offset, digest_v1: &str) -> EngineResult<()> {
        let mut inner = self.inner.write().await;
        if inner
            .verified_prefix
            .as_ref()
            .is_some_and(|(current, _)| through.0 <= current.0)
        {
            return Ok(());
        }
        inner.verified_prefix = Some((through, digest_v1.to_string()));
        Ok(())
    }

    async fn load_verified_prefix(&self) -> EngineResult<Option<(Offset, String)>> {
        Ok(self.inner.read().await.verified_prefix.clone())
    }

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
        let inner = self.inner.read().await;
        if from.0 < inner.base_offset {
            return Err(EngineError::Trimmed {
                requested: from.0,
                retained_floor: inner.base_offset,
            });
        }
        let available_to = inner
            .base_offset
            .checked_add(inner.records.len() as u64)
            .ok_or_else(|| EngineError::Storage("journal offset overflow".to_string()))?;
        if to.0 > available_to {
            return Err(EngineError::Gap {
                expected: available_to,
                found: None,
            });
        }

        let start_idx = usize::try_from(from.0 - inner.base_offset)
            .map_err(|_| EngineError::Storage("journal index does not fit usize".to_string()))?;
        let end_idx = usize::try_from(to.0 - inner.base_offset)
            .map_err(|_| EngineError::Storage("journal index does not fit usize".to_string()))?;
        let mut expected = from.0;
        let mut digest = previous_digest.to_string();
        for record in &inner.records[start_idx..end_idx] {
            if record.offset.0 != expected {
                return Err(EngineError::Gap {
                    expected,
                    found: Some(record.offset.0),
                });
            }
            digest = engine_core::types::extend_commit_chain_digest_v1(
                &digest,
                std::slice::from_ref(record),
            );
            expected = expected
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("journal offset overflow".to_string()))?;
        }
        Ok(digest)
    }

    async fn tail_offset(&self) -> EngineResult<Offset> {
        Ok(Offset(self.next_offset.load(Ordering::Acquire)))
    }

    async fn retained_floor(&self) -> EngineResult<Offset> {
        Ok(Offset(self.inner.read().await.base_offset))
    }

    async fn sealed_through(&self) -> EngineResult<Option<Offset>> {
        if !self.sealed.load(Ordering::Acquire) {
            return Ok(None);
        }
        Ok(match self.sealed_through.load(Ordering::Acquire) {
            u64::MAX => None,
            boundary => Some(Offset(boundary)),
        })
    }

    async fn record_seal_boundary(&self, boundary: Offset) -> EngineResult<()> {
        if !self.sealed.load(Ordering::Acquire) {
            return Err(EngineError::Storage(
                "cannot record a seal boundary on a segment that is not sealed".to_string(),
            ));
        }
        let current = self.sealed_through.load(Ordering::Acquire);
        if current != u64::MAX && boundary.0 < current {
            return Err(EngineError::Storage(format!(
                "seal boundary may not regress from {current} to {}",
                boundary.0
            )));
        }
        self.sealed_through.store(boundary.0, Ordering::Release);
        Ok(())
    }

    async fn seal(&self) -> EngineResult<()> {
        let _lifecycle_guard = self.inner.write().await;
        if self.sealed.load(Ordering::Acquire) {
            return Ok(());
        }
        let boundary = self.next_offset.load(Ordering::Acquire);
        self.sealed_through.store(boundary, Ordering::Release);
        self.sealed.store(true, Ordering::Release);
        // Wake tail streams so they see the seal.
        self.notify.notify_waiters();
        Ok(())
    }

    async fn prepare_for_new_epoch(&self) -> EngineResult<()> {
        let _lifecycle_guard = self.inner.write().await;
        if !self.sealed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.sealed_through.store(u64::MAX, Ordering::Release);
        self.sealed.store(false, Ordering::Release);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn truncate_uncommitted_tail(&self, from: Offset) -> EngineResult<()> {
        let mut inner = self.inner.write().await;
        if !self.sealed.load(Ordering::Acquire) {
            return Err(EngineError::Storage(
                "uncommitted-tail repair requires a sealed segment".to_string(),
            ));
        }
        let next = self.next_offset.load(Ordering::Acquire);
        if from.0 < inner.base_offset || from.0 > next {
            return Err(EngineError::InvalidArgument(format!(
                "tail repair boundary {} outside retained range [{}, {next}]",
                from.0, inner.base_offset
            )));
        }
        if from.0 == next {
            return Ok(());
        }
        let snapshot = inner.tail_undo.get(&from.0).cloned().ok_or_else(|| {
            EngineError::Storage(format!(
                "missing atomic tail-undo snapshot at uncommitted boundary {}",
                from.0
            ))
        })?;
        let keep = (from.0 - inner.base_offset) as usize;
        inner.records.truncate(keep);
        inner.idempotency_offsets = snapshot.idempotency_offsets;
        inner.state_versions = snapshot.state_versions;
        inner.activation_heads = snapshot.activation_heads;
        inner.required_child_obligations = snapshot.required_child_obligations;
        inner.activation_run_gates = snapshot.activation_run_gates;
        inner.run_leases = snapshot.run_leases;
        inner.tail_undo.retain(|offset, _| *offset < from.0);
        self.next_offset.store(from.0, Ordering::Release);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn trim(&self, through: Offset) -> EngineResult<()> {
        let mut inner = self.inner.write().await;
        let trim_up_to = through
            .0
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidArgument("trim offset overflow".to_string()))?;
        let next = self.next_offset.load(Ordering::Acquire);
        if trim_up_to > next {
            return Err(EngineError::InvalidArgument(format!(
                "trim floor {trim_up_to} exceeds next offset {next}"
            )));
        }

        if trim_up_to <= inner.base_offset {
            // Already trimmed past this point.
            return Ok(());
        }

        let remove_count = (trim_up_to - inner.base_offset) as usize;
        let remove_count = remove_count.min(inner.records.len());
        inner.records.drain(..remove_count);
        inner.base_offset = trim_up_to;
        inner.tail_undo.retain(|offset, _| *offset >= trim_up_to);

        Ok(())
    }
}
