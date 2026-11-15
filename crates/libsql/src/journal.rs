use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use engine_core::activation::ActivationState;
use engine_core::activation_authority::{
    authority_probe, decide_authority, decode_activation_state, decode_run_lease_entry,
    encode_activation_state, encode_run_lease_entry, nonterminal_activation, AuthorityContext,
    AuthorityDecision, AuthorityMode, AuthorityProbe, ObligationMutation, RunLeaseEntry,
};
use engine_core::error::{EngineError, EngineResult};
use engine_core::journal_contract::{
    record_idempotency_key, record_state_version_claims, validate_append_records,
    validate_idempotent_duplicate, validate_state_version_claim,
};
use engine_core::traits::{AppendOutcome, RecordStream, Segment};
use engine_core::types::{Offset, Record};
use prost::Message;
use tokio::sync::{Notify, OnceCell};

use crate::{decode_offset, encode_offset, libsql_error, LibsqlStorage};

const TAIL_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// One libSQL-backed, partition-local journal.
pub struct LibsqlJournal {
    storage: LibsqlStorage,
    partition_id: u32,
    notify: Arc<Notify>,
    authority_ready: Arc<OnceCell<()>>,
}

impl LibsqlJournal {
    pub(crate) fn new(storage: LibsqlStorage, partition_id: u32) -> Self {
        Self {
            storage,
            partition_id,
            notify: Arc::new(Notify::new()),
            authority_ready: Arc::new(OnceCell::new()),
        }
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    async fn ensure_partition(&self) -> EngineResult<()> {
        crate::db::query(
            r#"
            INSERT INTO agnt5_runtime_journal_partitions
                (namespace, partition_id)
            VALUES (?1, ?2)
            ON CONFLICT (namespace, partition_id) DO NOTHING
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .execute(self.storage.connection())
        .await
        .map_err(|error| libsql_error("ensure journal partition", error))?;
        Ok(())
    }

    async fn partition_state(&self) -> EngineResult<(u64, u64, bool)> {
        self.ensure_partition().await?;
        let row = crate::db::query(
            r#"
            SELECT next_offset, retained_floor, sealed
            FROM agnt5_runtime_journal_partitions
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .fetch_one(self.storage.connection())
        .await
        .map_err(|error| libsql_error("read journal partition", error))?;
        Ok((
            decode_offset(
                row.try_get("next_offset")
                    .map_err(|error| libsql_error("decode journal next_offset", error))?,
            )?,
            decode_offset(
                row.try_get("retained_floor")
                    .map_err(|error| libsql_error("decode journal retained_floor", error))?,
            )?,
            row.try_get("sealed")
                .map_err(|error| libsql_error("decode journal sealed flag", error))?,
        ))
    }

    async fn read_available(&self, from: u64, to: u64) -> EngineResult<Vec<Record>> {
        if to <= from {
            return Ok(Vec::new());
        }
        let rows = crate::db::query(
            r#"
            SELECT journal_offset, record_bytes
            FROM agnt5_runtime_journal_records
            WHERE namespace = ?1
              AND partition_id = ?2
              AND journal_offset >= ?3
              AND journal_offset < ?4
            ORDER BY journal_offset ASC
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .bind(encode_offset(from)?)
        .bind(encode_offset(to)?)
        .fetch_all(self.storage.connection())
        .await
        .map_err(|error| libsql_error("read journal range", error))?;

        let mut records = Vec::with_capacity(rows.len());
        let mut expected = from;
        for row in rows {
            let stored_offset = decode_offset(
                row.try_get("journal_offset")
                    .map_err(|error| libsql_error("decode journal row offset", error))?,
            )?;
            if stored_offset != expected {
                return Err(EngineError::Gap {
                    expected,
                    found: Some(stored_offset),
                });
            }
            let bytes: Vec<u8> = row
                .try_get("record_bytes")
                .map_err(|error| libsql_error("decode journal row bytes", error))?;
            let proto = engine_proto::Record::decode(bytes.as_slice())
                .map_err(|error| EngineError::Serialization(error.to_string()))?;
            let record = Record::from_proto(proto);
            if record.offset.0 != stored_offset {
                return Err(EngineError::Storage(format!(
                    "libsql journal record offset {} does not match row {stored_offset}",
                    record.offset.0
                )));
            }
            records.push(record);
            expected = expected
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("journal offset overflow".to_string()))?;
        }
        Ok(records)
    }

    async fn ensure_authority_ready(&self) -> EngineResult<()> {
        self.authority_ready
            .get_or_try_init(|| async {
                self.ensure_partition().await?;
                let partition = i32::try_from(self.partition_id)
                    .expect("u32 partition must fit libSQL INTEGER");
                let transaction = self
                    .storage
                    .new_connection()
                    .await?
                    .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
                    .await
                    .map_err(|error| libsql_error("begin activation authority backfill", error))?;
                let tail_row = crate::db::query(
                    r#"
                    SELECT next_offset
                    FROM agnt5_runtime_journal_partitions
                    WHERE namespace = ?1 AND partition_id = ?2
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .fetch_one(&transaction)
                .await
                .map_err(|error| libsql_error("lock activation authority partition", error))?;
                let tail = decode_offset(
                    tail_row
                        .try_get("next_offset")
                        .map_err(|error| libsql_error("decode authority journal tail", error))?,
                )?;
                crate::db::query(
                    r#"
                    INSERT INTO agnt5_runtime_activation_authority_watermarks
                        (namespace, partition_id, indexed_through)
                    VALUES (?1, ?2, 0)
                    ON CONFLICT (namespace, partition_id) DO NOTHING
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .execute(&transaction)
                .await
                .map_err(|error| libsql_error("ensure activation authority watermark", error))?;
                let watermark_row = crate::db::query(
                    r#"
                    SELECT indexed_through
                    FROM agnt5_runtime_activation_authority_watermarks
                    WHERE namespace = ?1 AND partition_id = ?2
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .fetch_one(&transaction)
                .await
                .map_err(|error| libsql_error("read activation authority watermark", error))?;
                let watermark =
                    decode_offset(watermark_row.try_get("indexed_through").map_err(|error| {
                        libsql_error("decode activation authority watermark", error)
                    })?)?;
                if watermark > tail {
                    return Err(EngineError::Storage(format!(
                        "activation authority watermark {watermark} exceeds journal tail {tail}"
                    )));
                }
                if watermark < tail {
                    let rows = crate::db::query(
                        r#"
                        SELECT record_bytes
                        FROM agnt5_runtime_journal_records
                        WHERE namespace = ?1
                          AND partition_id = ?2
                          AND journal_offset >= ?3
                          AND journal_offset < ?4
                        ORDER BY journal_offset ASC
                        "#,
                    )
                    .bind(self.storage.namespace())
                    .bind(partition)
                    .bind(encode_offset(watermark)?)
                    .bind(encode_offset(tail)?)
                    .fetch_all(&transaction)
                    .await
                    .map_err(|error| libsql_error("read activation authority backfill", error))?;
                    for row in rows {
                        let bytes: Vec<u8> = row.try_get("record_bytes").map_err(|error| {
                            libsql_error("decode activation authority backfill row", error)
                        })?;
                        let proto = engine_proto::Record::decode(bytes.as_slice())
                            .map_err(|error| EngineError::Serialization(error.to_string()))?;
                        let record = Record::from_proto(proto);
                        self.stage_authority_record(
                            &transaction,
                            partition,
                            &record,
                            record.offset,
                            AuthorityMode::TrustedHistory,
                        )
                        .await?;
                    }
                    self.update_authority_watermark(&transaction, partition, tail)
                        .await?;
                }
                transaction
                    .commit()
                    .await
                    .map_err(|error| libsql_error("commit activation authority backfill", error))?;
                Ok(())
            })
            .await
            .map(|_| ())
    }

    async fn update_authority_watermark(
        &self,
        transaction: &libsql::Transaction,
        partition: i32,
        indexed_through: u64,
    ) -> EngineResult<()> {
        crate::db::query(
            r#"
            UPDATE agnt5_runtime_activation_authority_watermarks
            SET indexed_through = ?3
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(encode_offset(indexed_through)?)
        .execute(transaction)
        .await
        .map_err(|error| libsql_error("advance activation authority watermark", error))?;
        Ok(())
    }

    async fn load_authority_head(
        &self,
        transaction: &libsql::Transaction,
        partition: i32,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<ActivationState>> {
        crate::db::query(
            r#"
            SELECT snapshot_bytes
            FROM agnt5_runtime_activation_authority_heads
            WHERE namespace = ?1 AND partition_id = ?2
              AND project_id = ?3 AND run_id = ?4 AND activation_id = ?5
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(project_id)
        .bind(run_id)
        .bind(activation_id)
        .fetch_optional(transaction)
        .await
        .map_err(|error| libsql_error("read activation authority head", error))?
        .map(|row| {
            row.try_get::<Vec<u8>>("snapshot_bytes")
                .map_err(|error| libsql_error("decode activation authority head", error))
                .and_then(|bytes| decode_activation_state(&bytes))
        })
        .transpose()
    }

    async fn load_run_lease(
        &self,
        transaction: &libsql::Transaction,
        partition: i32,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<RunLeaseEntry>> {
        crate::db::query(
            r#"
            SELECT entry_bytes
            FROM agnt5_runtime_activation_run_leases
            WHERE namespace = ?1 AND partition_id = ?2
              AND project_id = ?3 AND run_id = ?4
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(transaction)
        .await
        .map_err(|error| libsql_error("read activation run lease", error))?
        .map(|row| {
            row.try_get::<Vec<u8>>("entry_bytes")
                .map_err(|error| libsql_error("decode activation run lease", error))
                .and_then(|bytes| decode_run_lease_entry(&bytes))
        })
        .transpose()
    }

    async fn store_run_lease(
        &self,
        transaction: &libsql::Transaction,
        partition: i32,
        project_id: &str,
        run_id: &str,
        entry: &RunLeaseEntry,
    ) -> EngineResult<()> {
        crate::db::query(
            r#"
            INSERT INTO agnt5_runtime_activation_run_leases
                (namespace, partition_id, project_id, run_id, entry_bytes)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT (namespace, partition_id, project_id, run_id)
            DO UPDATE SET entry_bytes = excluded.entry_bytes
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(project_id)
        .bind(run_id)
        .bind(encode_run_lease_entry(entry))
        .execute(transaction)
        .await
        .map_err(|error| libsql_error("store activation run lease", error))?;
        Ok(())
    }

    async fn stage_authority_record(
        &self,
        transaction: &libsql::Transaction,
        partition: i32,
        record: &Record,
        offset: Offset,
        mode: AuthorityMode,
    ) -> EngineResult<()> {
        let Some(probe) = authority_probe(record)? else {
            return Ok(());
        };
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
            AuthorityProbe::RunAssignment(_) | AuthorityProbe::RunLease(_) => (None, None, None),
            AuthorityProbe::RunTerminal(probe) => {
                (None, None, probe.requires_children_settled.then_some(None))
            }
        };
        let current = if let Some(activation_id) = activation_id {
            self.load_authority_head(transaction, partition, project_id, run_id, activation_id)
                .await?
        } else {
            None
        };
        let parent = if let Some(parent_id) = parent_id {
            self.load_authority_head(transaction, partition, project_id, run_id, parent_id)
                .await?
        } else {
            None
        };
        let first_unsettled_required_child = if let Some(parent) = child_scope {
            let row = if let Some(parent) = parent {
                crate::db::query(
                    r#"
                    SELECT child_activation_id
                    FROM agnt5_runtime_activation_required_child_obligations
                    WHERE namespace = ?1 AND partition_id = ?2
                      AND project_id = ?3 AND run_id = ?4
                      AND parent_activation_id = ?5
                    ORDER BY child_activation_id ASC
                    LIMIT 1
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .bind(project_id)
                .bind(run_id)
                .bind(parent)
                .fetch_optional(transaction)
                .await
            } else {
                crate::db::query(
                    r#"
                    SELECT child_activation_id
                    FROM agnt5_runtime_activation_required_child_obligations
                    WHERE namespace = ?1 AND partition_id = ?2
                      AND project_id = ?3 AND run_id = ?4
                    ORDER BY parent_activation_id ASC, child_activation_id ASC
                    LIMIT 1
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .bind(project_id)
                .bind(run_id)
                .fetch_optional(transaction)
                .await
            };
            row.map_err(|error| libsql_error("read required-child authority", error))?
                .map(|row| {
                    row.try_get("child_activation_id")
                        .map_err(|error| libsql_error("decode required-child authority", error))
                })
                .transpose()?
        } else {
            None
        };
        let run_terminal_offset = crate::db::query(
            r#"
            SELECT accepted_offset
            FROM agnt5_runtime_activation_run_gates
            WHERE namespace = ?1 AND partition_id = ?2
              AND project_id = ?3 AND run_id = ?4
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(transaction)
        .await
        .map_err(|error| libsql_error("read activation run gate", error))?
        .map(|row| {
            row.try_get("accepted_offset")
                .map_err(|error| libsql_error("decode activation run gate", error))
                .and_then(decode_offset)
                .map(Offset)
        })
        .transpose()?;
        let lease = if probe.reads_lease() {
            self.load_run_lease(transaction, partition, project_id, run_id)
                .await?
        } else {
            None
        };

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
                crate::db::query(
                    r#"
                    INSERT INTO agnt5_runtime_activation_authority_heads
                        (namespace, partition_id, project_id, run_id, activation_id, snapshot_bytes)
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    ON CONFLICT (namespace, partition_id, project_id, run_id, activation_id)
                    DO UPDATE SET snapshot_bytes = excluded.snapshot_bytes
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .bind(&state.identity.project_id)
                .bind(&state.identity.run_id)
                .bind(&state.identity.activation_id)
                .bind(encode_activation_state(&state))
                .execute(transaction)
                .await
                .map_err(|error| libsql_error("store activation authority head", error))?;
                match obligation {
                    ObligationMutation::None => {}
                    ObligationMutation::Upsert {
                        project_id,
                        run_id,
                        parent_activation_id,
                        child_activation_id,
                    } => {
                        crate::db::query(
                            r#"
                            INSERT INTO agnt5_runtime_activation_required_child_obligations
                                (namespace, partition_id, project_id, run_id,
                                 parent_activation_id, child_activation_id)
                            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                            ON CONFLICT DO NOTHING
                            "#,
                        )
                        .bind(self.storage.namespace())
                        .bind(partition)
                        .bind(project_id)
                        .bind(run_id)
                        .bind(parent_activation_id)
                        .bind(child_activation_id)
                        .execute(transaction)
                        .await
                        .map_err(|error| libsql_error("store required-child authority", error))?;
                    }
                    ObligationMutation::Delete {
                        project_id,
                        run_id,
                        parent_activation_id,
                        child_activation_id,
                    } => {
                        crate::db::query(
                            r#"
                            DELETE FROM agnt5_runtime_activation_required_child_obligations
                            WHERE namespace = ?1 AND partition_id = ?2
                              AND project_id = ?3 AND run_id = ?4
                              AND parent_activation_id = ?5 AND child_activation_id = ?6
                            "#,
                        )
                        .bind(self.storage.namespace())
                        .bind(partition)
                        .bind(project_id)
                        .bind(run_id)
                        .bind(parent_activation_id)
                        .bind(child_activation_id)
                        .execute(transaction)
                        .await
                        .map_err(|error| libsql_error("delete required-child authority", error))?;
                    }
                }
            }
            AuthorityDecision::RunAssignment { lease, .. }
            | AuthorityDecision::RunLease { lease, .. } => {
                self.store_run_lease(transaction, partition, project_id, run_id, &lease)
                    .await?;
            }
            AuthorityDecision::RunTerminal {
                project_id,
                run_id,
                accepted_offset,
                already_terminal,
                lease,
            } => {
                if let Some(lease) = &lease {
                    self.store_run_lease(transaction, partition, &project_id, &run_id, lease)
                        .await?;
                }
                if !already_terminal {
                    crate::db::query(
                        r#"
                        INSERT INTO agnt5_runtime_activation_run_gates
                            (namespace, partition_id, project_id, run_id, accepted_offset)
                        VALUES (?1, ?2, ?3, ?4, ?5)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(self.storage.namespace())
                    .bind(partition)
                    .bind(project_id)
                    .bind(run_id)
                    .bind(encode_offset(accepted_offset.0)?)
                    .execute(transaction)
                    .await
                    .map_err(|error| libsql_error("store activation run gate", error))?;
                }
            }
        }
        Ok(())
    }
}

impl LibsqlJournal {
    async fn append_batch_with_mode(
        &self,
        records: &[Record],
        mode: AuthorityMode,
    ) -> EngineResult<AppendOutcome> {
        if records.is_empty() {
            return Ok(AppendOutcome::all_appended(Vec::new()));
        }
        validate_append_records(records)?;
        self.ensure_partition().await?;
        self.ensure_authority_ready().await?;

        let transaction = self
            .storage
            .new_connection()
            .await?
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|error| libsql_error("begin journal append", error))?;
        let partition =
            i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER");
        let state = crate::db::query(
            r#"
            SELECT next_offset, sealed
            FROM agnt5_runtime_journal_partitions
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .fetch_one(&transaction)
        .await
        .map_err(|error| libsql_error("lock journal partition", error))?;
        let mut next = decode_offset(
            state
                .try_get("next_offset")
                .map_err(|error| libsql_error("decode locked next_offset", error))?,
        )?;
        let sealed: bool = state
            .try_get("sealed")
            .map_err(|error| libsql_error("decode locked sealed flag", error))?;
        if sealed {
            return Err(EngineError::Sealed);
        }

        let mut offsets = vec![Offset::ZERO; records.len()];
        let mut appended = vec![false; records.len()];
        let mut pending_idempotency = HashMap::<Vec<u8>, (Offset, usize)>::new();
        let mut pending_state_versions = HashMap::<Vec<u8>, i64>::new();

        for (index, record) in records.iter().enumerate() {
            let idempotency_key = record_idempotency_key(record);
            if let Some(key) = idempotency_key.as_ref() {
                if let Some((offset, prior_index)) = pending_idempotency.get(key).copied() {
                    validate_idempotent_duplicate(&records[prior_index], record)?;
                    offsets[index] = offset;
                    continue;
                }
                let existing = crate::db::query(
                    r#"
                    SELECT i.journal_offset, r.record_bytes
                    FROM agnt5_runtime_journal_idempotency i
                    JOIN agnt5_runtime_journal_records r
                      ON r.namespace = i.namespace
                     AND r.partition_id = i.partition_id
                     AND r.journal_offset = i.journal_offset
                    WHERE i.namespace = ?1
                      AND i.partition_id = ?2
                      AND i.idempotency_key = ?3
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .bind(key)
                .fetch_optional(&transaction)
                .await
                .map_err(|error| libsql_error("read journal idempotency", error))?;
                if let Some(existing) = existing {
                    let offset =
                        Offset(decode_offset(existing.try_get("journal_offset").map_err(
                            |error| libsql_error("decode idempotent journal offset", error),
                        )?)?);
                    let bytes: Vec<u8> = existing
                        .try_get("record_bytes")
                        .map_err(|error| libsql_error("decode idempotent journal record", error))?;
                    let proto = engine_proto::Record::decode(bytes.as_slice())
                        .map_err(|error| EngineError::Serialization(error.to_string()))?;
                    let existing_record = Record::from_proto(proto);
                    validate_idempotent_duplicate(&existing_record, record)?;
                    offsets[index] = offset;
                    pending_idempotency.insert(key.clone(), (offset, index));
                    continue;
                }
            }

            for claim in record_state_version_claims(record)? {
                let current = if let Some(version) =
                    pending_state_versions.get(&claim.index_key).copied()
                {
                    Some(version)
                } else {
                    crate::db::query(
                        r#"
                        SELECT version
                        FROM agnt5_runtime_activation_state_versions
                        WHERE namespace = ?1 AND partition_id = ?2 AND state_key = ?3
                        "#,
                    )
                    .bind(self.storage.namespace())
                    .bind(partition)
                    .bind(&claim.index_key)
                    .fetch_optional(&transaction)
                    .await
                    .map_err(|error| libsql_error("read activation state version", error))?
                    .map(|row| {
                        row.try_get("version")
                            .map_err(|error| libsql_error("decode activation state version", error))
                    })
                    .transpose()?
                };
                validate_state_version_claim(current, &claim)?;
                crate::db::query(
                    r#"
                    INSERT INTO agnt5_runtime_activation_state_versions
                        (namespace, partition_id, state_key, version)
                    VALUES (?1, ?2, ?3, ?4)
                    ON CONFLICT (namespace, partition_id, state_key)
                    DO UPDATE SET version = excluded.version
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .bind(&claim.index_key)
                .bind(claim.next_version)
                .execute(&transaction)
                .await
                .map_err(|error| libsql_error("store activation state version", error))?;
                pending_state_versions.insert(claim.index_key, claim.next_version);
            }

            let offset = Offset(next);
            self.stage_authority_record(&transaction, partition, record, offset, mode)
                .await?;

            next = next
                .checked_add(1)
                .ok_or_else(|| EngineError::Storage("journal offset overflow".to_string()))?;
            let mut stored = record.clone();
            stored.offset = offset;
            let bytes = stored.to_proto().encode_to_vec();
            crate::db::query(
                r#"
                INSERT INTO agnt5_runtime_journal_records
                    (namespace, partition_id, journal_offset, record_bytes)
                VALUES (?1, ?2, ?3, ?4)
                "#,
            )
            .bind(self.storage.namespace())
            .bind(partition)
            .bind(encode_offset(offset.0)?)
            .bind(bytes)
            .execute(&transaction)
            .await
            .map_err(|error| libsql_error("insert journal record", error))?;

            if let Some(key) = idempotency_key {
                crate::db::query(
                    r#"
                    INSERT INTO agnt5_runtime_journal_idempotency
                        (namespace, partition_id, idempotency_key, journal_offset)
                    VALUES (?1, ?2, ?3, ?4)
                    "#,
                )
                .bind(self.storage.namespace())
                .bind(partition)
                .bind(&key)
                .bind(encode_offset(offset.0)?)
                .execute(&transaction)
                .await
                .map_err(|error| libsql_error("insert journal idempotency", error))?;
                pending_idempotency.insert(key, (offset, index));
            }
            offsets[index] = offset;
            appended[index] = true;
        }

        if appended.iter().any(|appended| *appended) {
            crate::db::query(
                r#"
                UPDATE agnt5_runtime_journal_partitions
                SET next_offset = ?3
                WHERE namespace = ?1 AND partition_id = ?2
                "#,
            )
            .bind(self.storage.namespace())
            .bind(partition)
            .bind(encode_offset(next)?)
            .execute(&transaction)
            .await
            .map_err(|error| libsql_error("advance journal next_offset", error))?;
            self.update_authority_watermark(&transaction, partition, next)
                .await?;
        }

        transaction
            .commit()
            .await
            .map_err(|error| libsql_error("commit journal append", error))?;
        if appended.iter().any(|appended| *appended) {
            self.notify.notify_waiters();
        }
        Ok(AppendOutcome { offsets, appended })
    }
}

#[async_trait]
impl Segment for LibsqlJournal {
    async fn append_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>> {
        Ok(self.append_batch_with_outcome(records).await?.offsets)
    }

    async fn append_batch_with_outcome(&self, records: &[Record]) -> EngineResult<AppendOutcome> {
        self.append_batch_with_mode(records, AuthorityMode::Live)
            .await
    }

    async fn append_replicated_batch(&self, records: &[Record]) -> EngineResult<Vec<Offset>> {
        Ok(self
            .append_batch_with_mode(records, AuthorityMode::TrustedHistory)
            .await?
            .offsets)
    }

    async fn load_activation_authority(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<ActivationState>> {
        self.ensure_authority_ready().await?;
        crate::db::query(
            r#"
            SELECT snapshot_bytes
            FROM agnt5_runtime_activation_authority_heads
            WHERE namespace = ?1 AND partition_id = ?2
              AND project_id = ?3 AND run_id = ?4 AND activation_id = ?5
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .bind(project_id)
        .bind(run_id)
        .bind(activation_id)
        .fetch_optional(self.storage.connection())
        .await
        .map_err(|error| libsql_error("read activation authority head", error))?
        .map(|row| {
            row.try_get::<Vec<u8>>("snapshot_bytes")
                .map_err(|error| libsql_error("decode activation authority head", error))
                .and_then(|bytes| decode_activation_state(&bytes))
        })
        .transpose()
    }

    async fn load_run_lease_authority(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<RunLeaseEntry>> {
        self.ensure_authority_ready().await?;
        crate::db::query(
            r#"
            SELECT entry_bytes
            FROM agnt5_runtime_activation_run_leases
            WHERE namespace = ?1 AND partition_id = ?2
              AND project_id = ?3 AND run_id = ?4
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(self.storage.connection())
        .await
        .map_err(|error| libsql_error("read run lease authority", error))?
        .map(|row| {
            row.try_get::<Vec<u8>>("entry_bytes")
                .map_err(|error| libsql_error("decode run lease authority", error))
                .and_then(|bytes| decode_run_lease_entry(&bytes))
        })
        .transpose()
    }

    async fn first_unsettled_required_child(
        &self,
        project_id: &str,
        run_id: &str,
        parent_activation_id: Option<&str>,
    ) -> EngineResult<Option<String>> {
        self.ensure_authority_ready().await?;
        let partition =
            i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER");
        let row = if let Some(parent_activation_id) = parent_activation_id {
            crate::db::query(
                r#"
                SELECT child_activation_id
                FROM agnt5_runtime_activation_required_child_obligations
                WHERE namespace = ?1 AND partition_id = ?2
                  AND project_id = ?3 AND run_id = ?4 AND parent_activation_id = ?5
                ORDER BY child_activation_id ASC
                LIMIT 1
                "#,
            )
            .bind(self.storage.namespace())
            .bind(partition)
            .bind(project_id)
            .bind(run_id)
            .bind(parent_activation_id)
            .fetch_optional(self.storage.connection())
            .await
        } else {
            crate::db::query(
                r#"
                SELECT child_activation_id
                FROM agnt5_runtime_activation_required_child_obligations
                WHERE namespace = ?1 AND partition_id = ?2
                  AND project_id = ?3 AND run_id = ?4
                ORDER BY parent_activation_id ASC, child_activation_id ASC
                LIMIT 1
                "#,
            )
            .bind(self.storage.namespace())
            .bind(partition)
            .bind(project_id)
            .bind(run_id)
            .fetch_optional(self.storage.connection())
            .await
        };
        row.map_err(|error| libsql_error("read required-child authority", error))?
            .map(|row| {
                row.try_get("child_activation_id")
                    .map_err(|error| libsql_error("decode required-child authority", error))
            })
            .transpose()
    }

    async fn list_nonterminal_activation_authority(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<ActivationState>> {
        self.ensure_authority_ready().await?;
        let rows = crate::db::query(
            r#"
            SELECT snapshot_bytes
            FROM agnt5_runtime_activation_authority_heads
            WHERE namespace = ?1 AND partition_id = ?2
              AND project_id = ?3 AND run_id = ?4
            ORDER BY activation_id ASC
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .bind(project_id)
        .bind(run_id)
        .fetch_all(self.storage.connection())
        .await
        .map_err(|error| libsql_error("list activation authority heads", error))?;
        rows.into_iter()
            .map(|row| {
                row.try_get::<Vec<u8>>("snapshot_bytes")
                    .map_err(|error| libsql_error("decode activation authority head", error))
                    .and_then(|bytes| decode_activation_state(&bytes))
            })
            .filter_map(|state| match state {
                Ok(state) if nonterminal_activation(&state) => Some(Ok(state)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    async fn tail(&self, from: Offset) -> EngineResult<RecordStream> {
        self.ensure_partition().await?;
        let storage = self.storage.clone();
        let partition_id = self.partition_id;
        let notify = self.notify.clone();
        let authority_ready = self.authority_ready.clone();
        let stream = async_stream::stream! {
            let journal = LibsqlJournal {
                storage,
                partition_id,
                notify: notify.clone(),
                authority_ready,
            };
            let mut cursor = from.0;
            loop {
                let notified = notify.notified();
                let (next, floor, _) = match journal.partition_state().await {
                    Ok(state) => state,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                if cursor < floor {
                    yield Err(EngineError::Trimmed {
                        requested: cursor,
                        retained_floor: floor,
                    });
                    return;
                }
                if cursor < next {
                    let records = match journal.read_available(cursor, next).await {
                        Ok(records) => records,
                        Err(error) => {
                            yield Err(error);
                            return;
                        }
                    };
                    let Some(last) = records.last() else {
                        yield Err(EngineError::Gap {
                            expected: cursor,
                            found: None,
                        });
                        return;
                    };
                    cursor = last.offset.0.saturating_add(1);
                    for record in records {
                        yield Ok(record);
                    }
                    continue;
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(TAIL_POLL_INTERVAL) => {}
                }
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
        let (next, floor, _) = self.partition_state().await?;
        if from.0 < floor {
            return Err(EngineError::Trimmed {
                requested: from.0,
                retained_floor: floor,
            });
        }
        let available_to = to.0.min(next);
        let records = self.read_available(from.0, available_to).await?;
        let expected_count = available_to.saturating_sub(from.0);
        if records.len() as u64 != expected_count {
            return Err(EngineError::Gap {
                expected: from.0 + records.len() as u64,
                found: None,
            });
        }
        Ok(records)
    }

    async fn tail_offset(&self) -> EngineResult<Offset> {
        let (next, _, _) = self.partition_state().await?;
        Ok(Offset(next))
    }

    async fn retained_floor(&self) -> EngineResult<Offset> {
        let (_, floor, _) = self.partition_state().await?;
        Ok(Offset(floor))
    }

    async fn check_readiness(&self) -> EngineResult<()> {
        let (next, floor, _) = self.partition_state().await?;
        if floor > next {
            return Err(EngineError::Storage(format!(
                "libSQL retained floor {floor} exceeds next offset {next}"
            )));
        }
        Ok(())
    }

    async fn seal(&self) -> EngineResult<()> {
        self.ensure_partition().await?;
        crate::db::query(
            r#"
            UPDATE agnt5_runtime_journal_partitions
            SET sealed = TRUE
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .execute(self.storage.connection())
        .await
        .map_err(|error| libsql_error("seal journal partition", error))?;
        self.notify.notify_waiters();
        Ok(())
    }

    async fn prepare_for_new_epoch(&self) -> EngineResult<()> {
        self.ensure_partition().await?;
        crate::db::query(
            r#"
            UPDATE agnt5_runtime_journal_partitions
            SET sealed = FALSE
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER"))
        .execute(self.storage.connection())
        .await
        .map_err(|error| libsql_error("prepare journal epoch", error))?;
        self.notify.notify_waiters();
        Ok(())
    }

    async fn trim(&self, through: Offset) -> EngineResult<()> {
        self.ensure_partition().await?;
        let requested_floor = through
            .0
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidArgument("trim offset overflow".to_string()))?;
        let transaction = self
            .storage
            .new_connection()
            .await?
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|error| libsql_error("begin journal trim", error))?;
        let partition =
            i32::try_from(self.partition_id).expect("u32 partition must fit libSQL INTEGER");
        let state = crate::db::query(
            r#"
            SELECT next_offset, retained_floor
            FROM agnt5_runtime_journal_partitions
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .fetch_one(&transaction)
        .await
        .map_err(|error| libsql_error("lock journal trim", error))?;
        let next = decode_offset(
            state
                .try_get("next_offset")
                .map_err(|error| libsql_error("decode trim next_offset", error))?,
        )?;
        let current_floor = decode_offset(
            state
                .try_get("retained_floor")
                .map_err(|error| libsql_error("decode trim retained_floor", error))?,
        )?;
        let new_floor = requested_floor.max(current_floor);
        if new_floor > next {
            return Err(EngineError::InvalidArgument(format!(
                "trim floor {new_floor} exceeds next offset {next}"
            )));
        }
        if new_floor == current_floor {
            return Ok(());
        }
        crate::db::query(
            r#"
            DELETE FROM agnt5_runtime_journal_records
            WHERE namespace = ?1 AND partition_id = ?2 AND journal_offset < ?3
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(encode_offset(new_floor)?)
        .execute(&transaction)
        .await
        .map_err(|error| libsql_error("delete trimmed journal records", error))?;
        crate::db::query(
            r#"
            UPDATE agnt5_runtime_journal_partitions
            SET retained_floor = ?3
            WHERE namespace = ?1 AND partition_id = ?2
            "#,
        )
        .bind(self.storage.namespace())
        .bind(partition)
        .bind(encode_offset(new_floor)?)
        .execute(&transaction)
        .await
        .map_err(|error| libsql_error("advance journal retained_floor", error))?;
        transaction
            .commit()
            .await
            .map_err(|error| libsql_error("commit journal trim", error))?;
        self.notify.notify_waiters();
        Ok(())
    }
}
