use async_trait::async_trait;
use engine_core::demand::DemandKey;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    DemandPendingStore, DispatchOutboxCursor, DispatchOutboxEntry, DispatchOutboxScanEntry,
    DispatchOutboxStore, LeaseWorkStore, OutboxEntryToken, PendingDemandCandidate,
    PendingDemandHead, PendingDemandRow, PendingRunCandidate, PendingRunCursor, PendingWorkStore,
    TerminalObligationStore, TimerWorkStore,
};
use engine_core::types::{LeaseExpiryCursor, TerminalObligationCursor, TerminalObligationEntry};
use sqlx::{postgres::PgRow, Row};

use crate::{decode_offset, postgres_error, PostgresStorage};

#[async_trait]
impl DispatchOutboxStore for PostgresStorage {
    async fn scan_dispatch_intents(
        &self,
        partition_id: u32,
        cursor: Option<&DispatchOutboxCursor>,
        limit: usize,
    ) -> EngineResult<Vec<DispatchOutboxScanEntry>> {
        let (after_offset, after_project, after_run) = cursor
            .and_then(DispatchOutboxCursor::position)
            .map(|(offset, project, run)| {
                (
                    i64::try_from(offset).unwrap_or(i64::MAX),
                    project.to_string(),
                    run.to_string(),
                )
            })
            .unwrap_or((-1, String::new(), String::new()));
        let rows = sqlx::query(
            r#"
            SELECT entry_id, project_id, queued_offset, run_id, payload
            FROM agnt5_runtime.dispatch_outbox
            WHERE namespace = $1 AND partition_id = $2
              AND (queued_offset, project_id, run_id) > ($3, $4, $5)
            ORDER BY queued_offset ASC, project_id ASC, run_id ASC
            LIMIT $6
            "#,
        )
        .bind(self.namespace())
        .bind(i32::try_from(partition_id).map_err(|_| {
            EngineError::InvalidArgument("partition id exceeds PostgreSQL INTEGER".into())
        })?)
        .bind(after_offset)
        .bind(after_project)
        .bind(after_run)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan dispatch outbox", error))?;

        rows.into_iter()
            .map(|row| {
                let entry_id: i64 = row
                    .try_get("entry_id")
                    .map_err(|error| postgres_error("decode dispatch entry id", error))?;
                if entry_id < 0 {
                    return Err(EngineError::Storage(
                        "negative PostgreSQL dispatch entry id".to_string(),
                    ));
                }
                let queued_offset: i64 = row
                    .try_get("queued_offset")
                    .map_err(|error| postgres_error("decode dispatch offset", error))?;
                Ok(DispatchOutboxScanEntry::Ready(DispatchOutboxEntry {
                    token: OutboxEntryToken::from_adapter((entry_id as u64).to_be_bytes().to_vec()),
                    partition_id,
                    project_id: row
                        .try_get("project_id")
                        .map_err(|error| postgres_error("decode dispatch project", error))?,
                    queued_offset: decode_offset(queued_offset)?,
                    run_id: row
                        .try_get("run_id")
                        .map_err(|error| postgres_error("decode dispatch run", error))?,
                    payload: row
                        .try_get("payload")
                        .map_err(|error| postgres_error("decode dispatch payload", error))?,
                }))
            })
            .collect()
    }

    async fn delete_dispatch_intent(&self, token: &OutboxEntryToken) -> EngineResult<()> {
        let bytes: [u8; 8] = token.adapter_bytes().try_into().map_err(|_| {
            EngineError::InvalidArgument("invalid PostgreSQL dispatch token".to_string())
        })?;
        let entry_id = u64::from_be_bytes(bytes);
        let entry_id = i64::try_from(entry_id)
            .map_err(|_| EngineError::InvalidArgument("dispatch token exceeds BIGINT".into()))?;
        sqlx::query(
            r#"
            DELETE FROM agnt5_runtime.dispatch_outbox
            WHERE namespace = $1 AND entry_id = $2
            "#,
        )
        .bind(self.namespace())
        .bind(entry_id)
        .execute(self.pool())
        .await
        .map_err(|error| postgres_error("delete dispatch outbox", error))?;
        Ok(())
    }

    async fn load_dispatch_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("runs", "run_id", project_id, run_id)
            .await
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
        sqlx::query(
            r#"
            INSERT INTO agnt5_runtime.pending_deployment_priority
                (namespace, project_id, deployment_id, priority,
                 ready_at_ms, created_at_ms, run_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(deployment_id)
        .bind(i16::from(priority))
        .bind(ready_at_ms)
        .bind(created_at_ms)
        .bind(run_id)
        .execute(self.pool())
        .await
        .map_err(|error| postgres_error("ensure deployment pending run", error))?;
        Ok(())
    }
}

#[async_trait]
impl TerminalObligationStore for PostgresStorage {
    async fn scan_terminal_obligations(
        &self,
        partition_id: u32,
        limit: usize,
        start_after: Option<TerminalObligationCursor>,
    ) -> EngineResult<Vec<TerminalObligationEntry>> {
        let cursor = start_after.unwrap_or(TerminalObligationCursor {
            project_id: String::new(),
            run_id: String::new(),
        });
        let rows = sqlx::query(
            r#"
            SELECT project_id, run_id, data
            FROM agnt5_runtime.terminal_obligations
            WHERE namespace = $1 AND partition_id = $2
              AND (project_id, run_id) > ($3, $4)
            ORDER BY project_id ASC, run_id ASC
            LIMIT $5
            "#,
        )
        .bind(self.namespace())
        .bind(i32::try_from(partition_id).map_err(|_| {
            EngineError::InvalidArgument("partition id exceeds PostgreSQL INTEGER".into())
        })?)
        .bind(cursor.project_id)
        .bind(cursor.run_id)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan terminal obligations", error))?;

        rows.into_iter()
            .map(|row| {
                Ok(TerminalObligationEntry {
                    project_id: row.try_get("project_id").map_err(|error| {
                        postgres_error("decode terminal obligation project", error)
                    })?,
                    run_id: row
                        .try_get("run_id")
                        .map_err(|error| postgres_error("decode terminal obligation run", error))?,
                    run_data: row.try_get("data").map_err(|error| {
                        postgres_error("decode terminal obligation data", error)
                    })?,
                })
            })
            .collect()
    }

    async fn load_terminal_obligation_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("runs", "run_id", project_id, run_id)
            .await
    }
}

#[async_trait]
impl TimerWorkStore for PostgresStorage {
    async fn scan_due_timers(&self, now_ns: i64, limit: usize) -> EngineResult<Vec<Vec<u8>>> {
        let rows = sqlx::query(
            r#"
            SELECT data
            FROM agnt5_runtime.timers
            WHERE namespace = $1 AND status = 'scheduled'
              AND fire_at_ns > 0 AND fire_at_ns <= $2
            ORDER BY fire_at_ns ASC, project_id ASC, timer_id ASC
            LIMIT $3
            "#,
        )
        .bind(self.namespace())
        .bind(now_ns)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan due timers", error))?;
        rows.into_iter()
            .map(|row| {
                row.try_get("data")
                    .map_err(|error| postgres_error("decode due timer", error))
            })
            .collect()
    }
}

#[async_trait]
impl PendingWorkStore for PostgresStorage {
    async fn scan_pending_runs(
        &self,
        project_id: &str,
        component_names: &[String],
        limit: usize,
    ) -> EngineResult<Vec<Vec<u8>>> {
        if component_names.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT data
            FROM agnt5_runtime.pending_runs
            WHERE namespace = $1 AND project_id = $2
              AND component_name = ANY($3::text[])
            ORDER BY component_name ASC, run_id ASC
            LIMIT $4
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(component_names)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan pending runs", error))?;
        rows.into_iter()
            .map(|row| {
                row.try_get("data")
                    .map_err(|error| postgres_error("decode pending run", error))
            })
            .collect()
    }

    async fn scan_ready_deployment_runs(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT run_id
            FROM agnt5_runtime.pending_deployment_priority
            WHERE namespace = $1 AND project_id = $2
              AND deployment_id = $3 AND ready_at_ms <= $4
            ORDER BY priority ASC, ready_at_ms ASC, created_at_ms ASC, run_id ASC
            LIMIT $5
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(deployment_id)
        .bind(now_ms)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan deployment pending runs", error))?;
        rows.into_iter()
            .map(|row| {
                row.try_get("run_id")
                    .map_err(|error| postgres_error("decode deployment pending run", error))
            })
            .collect()
    }

    async fn scan_ready_deployment_candidates(
        &self,
        project_id: &str,
        deployment_id: &str,
        now_ms: i64,
        after: Option<&PendingRunCursor>,
        limit: usize,
    ) -> EngineResult<Vec<PendingRunCandidate>> {
        // Row-value comparison resumes strictly after the cursor. Without a
        // cursor the sentinel sorts before every real row.
        let (after_priority, after_ready_at_ms, after_created_at_ms, after_run_id) = after
            .map(|cursor| {
                (
                    i16::from(cursor.priority),
                    cursor.ready_at_ms,
                    cursor.created_at_ms,
                    cursor.run_id.clone(),
                )
            })
            .unwrap_or((-1, i64::MIN, i64::MIN, String::new()));
        let rows = sqlx::query(
            r#"
            SELECT p.run_id, p.priority, p.ready_at_ms, p.created_at_ms, r.data
            FROM agnt5_runtime.pending_deployment_priority p
            LEFT JOIN agnt5_runtime.runs r
              ON r.namespace = p.namespace
             AND r.project_id = p.project_id
             AND r.run_id = p.run_id
            WHERE p.namespace = $1 AND p.project_id = $2
              AND p.deployment_id = $3 AND p.ready_at_ms <= $4
              AND (p.priority, p.ready_at_ms, p.created_at_ms, p.run_id)
                  > ($5, $6, $7, $8)
            ORDER BY p.priority ASC, p.ready_at_ms ASC, p.created_at_ms ASC, p.run_id ASC
            LIMIT $9
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(deployment_id)
        .bind(now_ms)
        .bind(after_priority)
        .bind(after_ready_at_ms)
        .bind(after_created_at_ms)
        .bind(after_run_id)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan deployment pending candidates", error))?;
        rows.into_iter()
            .map(|row| {
                let decode = |error| postgres_error("decode deployment pending candidate", error);
                let priority: i16 = row.try_get("priority").map_err(decode)?;
                let priority = u8::try_from(priority).map_err(|_| {
                    EngineError::Storage(format!("pending priority {priority} is out of range"))
                })?;
                Ok(PendingRunCandidate {
                    cursor: PendingRunCursor {
                        priority,
                        ready_at_ms: row.try_get("ready_at_ms").map_err(decode)?,
                        created_at_ms: row.try_get("created_at_ms").map_err(decode)?,
                        run_id: row.try_get("run_id").map_err(decode)?,
                    },
                    data: row.try_get("data").map_err(decode)?,
                })
            })
            .collect()
    }

    async fn load_pending_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("runs", "run_id", project_id, run_id)
            .await
    }
}

fn decode_pending_demand_candidate(
    row: &PgRow,
    partition_id: u32,
    demand_key: &DemandKey,
) -> EngineResult<PendingDemandCandidate> {
    let decode = |error| postgres_error("decode pending demand candidate", error);
    let priority: i16 = row.try_get("priority").map_err(decode)?;
    let priority = u8::try_from(priority)
        .ok()
        .filter(|priority| (1..=3).contains(priority))
        .ok_or_else(|| {
            EngineError::Storage(format!("invalid pending demand priority {priority}"))
        })?;
    Ok(PendingDemandCandidate {
        row: PendingDemandRow {
            partition_id,
            demand_key: demand_key.clone(),
            rank: PendingRunCursor {
                priority,
                ready_at_ms: row.try_get("ready_at_ms").map_err(decode)?,
                created_at_ms: row.try_get("created_at_ms").map_err(decode)?,
                run_id: row.try_get("run_id").map_err(decode)?,
            },
        },
        data: row.try_get("data").map_err(decode)?,
    })
}

#[async_trait]
impl DemandPendingStore for PostgresStorage {
    async fn read_pending_demand_head(
        &self,
        partition_id: u32,
        demand_key: &DemandKey,
        now_ms: i64,
    ) -> EngineResult<PendingDemandHead> {
        let partition_id_i32 = i32::try_from(partition_id).map_err(|_| {
            EngineError::InvalidArgument("partition id exceeds PostgreSQL INTEGER".into())
        })?;
        let row = sqlx::query(
            r#"
            WITH ready_candidates AS (
                SELECT candidate.*
                FROM (VALUES (1::smallint), (2::smallint), (3::smallint)) AS band(priority)
                CROSS JOIN LATERAL (
                    SELECT p.priority, p.ready_at_ms, p.created_at_ms, p.run_id
                    FROM agnt5_runtime.pending_demand p
                    WHERE p.namespace = $1 AND p.partition_id = $2
                      AND p.project_id = $3 AND p.deployment_id = $4
                      AND p.component_type = $5 AND p.component_name = $6
                      AND p.priority = band.priority AND p.ready_at_ms <= $7
                    ORDER BY p.ready_at_ms, p.created_at_ms, p.run_id
                    LIMIT 1
                ) candidate
            ),
            ready_head AS (
                SELECT * FROM ready_candidates
                ORDER BY priority, ready_at_ms, created_at_ms, run_id
                LIMIT 1
            ),
            future_candidates AS (
                SELECT candidate.ready_at_ms
                FROM (VALUES (1::smallint), (2::smallint), (3::smallint)) AS band(priority)
                CROSS JOIN LATERAL (
                    SELECT p.ready_at_ms
                    FROM agnt5_runtime.pending_demand p
                    WHERE p.namespace = $1 AND p.partition_id = $2
                      AND p.project_id = $3 AND p.deployment_id = $4
                      AND p.component_type = $5 AND p.component_name = $6
                      AND p.priority = band.priority AND p.ready_at_ms > $7
                    ORDER BY p.ready_at_ms
                    LIMIT 1
                ) candidate
            )
            SELECT h.priority, h.ready_at_ms, h.created_at_ms, h.run_id,
                   r.data, (SELECT MIN(ready_at_ms) FROM future_candidates) AS next_ready_at_ms
            FROM (SELECT 1) seed
            LEFT JOIN ready_head h ON TRUE
            LEFT JOIN agnt5_runtime.runs r
              ON r.namespace = $1 AND r.project_id = $3 AND r.run_id = h.run_id
            "#,
        )
        .bind(self.namespace())
        .bind(partition_id_i32)
        .bind(demand_key.project_id())
        .bind(demand_key.deployment_id())
        .bind(demand_key.component_type_value())
        .bind(demand_key.component_name())
        .bind(now_ms)
        .fetch_one(self.pool())
        .await
        .map_err(|error| postgres_error("read pending demand head", error))?;

        let priority: Option<i16> = row
            .try_get("priority")
            .map_err(|error| postgres_error("decode pending demand head", error))?;
        let ready = if priority.is_some() {
            Some(decode_pending_demand_candidate(
                &row,
                partition_id,
                demand_key,
            )?)
        } else {
            None
        };
        Ok(PendingDemandHead {
            ready,
            next_ready_at_ms: row
                .try_get("next_ready_at_ms")
                .map_err(|error| postgres_error("decode pending demand future head", error))?,
        })
    }

    async fn scan_pending_demand_candidates(
        &self,
        partition_id: u32,
        demand_key: &DemandKey,
        now_ms: i64,
        after: Option<&PendingRunCursor>,
        limit: usize,
    ) -> EngineResult<Vec<PendingDemandCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if after.is_some_and(|cursor| !(1..=3).contains(&cursor.priority)) {
            return Err(EngineError::InvalidArgument(
                "pending demand cursor has an invalid priority".into(),
            ));
        }
        let partition_id_i32 = i32::try_from(partition_id).map_err(|_| {
            EngineError::InvalidArgument("partition id exceeds PostgreSQL INTEGER".into())
        })?;
        let (after_priority, after_ready_at_ms, after_created_at_ms, after_run_id) = after
            .map(|cursor| {
                (
                    i16::from(cursor.priority),
                    cursor.ready_at_ms,
                    cursor.created_at_ms,
                    cursor.run_id.clone(),
                )
            })
            .unwrap_or((0, i64::MIN, i64::MIN, String::new()));
        let rows = sqlx::query(
            r#"
            WITH candidates AS (
                SELECT candidate.*
                FROM (VALUES (1::smallint), (2::smallint), (3::smallint)) AS band(priority)
                CROSS JOIN LATERAL (
                    SELECT p.priority, p.ready_at_ms, p.created_at_ms, p.run_id
                    FROM agnt5_runtime.pending_demand p
                    WHERE p.namespace = $1 AND p.partition_id = $2
                      AND p.project_id = $3 AND p.deployment_id = $4
                      AND p.component_type = $5 AND p.component_name = $6
                      AND p.priority = band.priority AND p.ready_at_ms <= $7
                      AND (p.priority, p.ready_at_ms, p.created_at_ms, p.run_id)
                          > ($8, $9, $10, $11)
                    ORDER BY p.ready_at_ms, p.created_at_ms, p.run_id
                    LIMIT $12
                ) candidate
            )
            SELECT c.priority, c.ready_at_ms, c.created_at_ms, c.run_id, r.data
            FROM candidates c
            LEFT JOIN agnt5_runtime.runs r
              ON r.namespace = $1 AND r.project_id = $3 AND r.run_id = c.run_id
            ORDER BY c.priority, c.ready_at_ms, c.created_at_ms, c.run_id
            LIMIT $12
            "#,
        )
        .bind(self.namespace())
        .bind(partition_id_i32)
        .bind(demand_key.project_id())
        .bind(demand_key.deployment_id())
        .bind(demand_key.component_type_value())
        .bind(demand_key.component_name())
        .bind(now_ms)
        .bind(after_priority)
        .bind(after_ready_at_ms)
        .bind(after_created_at_ms)
        .bind(after_run_id)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan pending demand candidates", error))?;
        rows.iter()
            .map(|row| decode_pending_demand_candidate(row, partition_id, demand_key))
            .collect()
    }
}

#[async_trait]
impl LeaseWorkStore for PostgresStorage {
    async fn scan_expired_leases(
        &self,
        now_ms: i64,
        limit: usize,
        start_after: Option<LeaseExpiryCursor>,
    ) -> EngineResult<Vec<LeaseExpiryCursor>> {
        let (cursor_expires, cursor_project, cursor_run) =
            start_after.unwrap_or((i64::MIN, String::new(), String::new()));
        let rows = sqlx::query(
            r#"
            SELECT expires_at_ms, project_id, run_id
            FROM agnt5_runtime.leases
            WHERE namespace = $1 AND expires_at_ms <= $2
              AND (expires_at_ms, project_id, run_id) > ($3, $4, $5)
            ORDER BY expires_at_ms ASC, project_id ASC, run_id ASC
            LIMIT $6
            "#,
        )
        .bind(self.namespace())
        .bind(now_ms)
        .bind(cursor_expires)
        .bind(cursor_project)
        .bind(cursor_run)
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan expired leases", error))?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("expires_at_ms")
                        .map_err(|error| postgres_error("decode lease expiry", error))?,
                    row.try_get("project_id")
                        .map_err(|error| postgres_error("decode lease project", error))?,
                    row.try_get("run_id")
                        .map_err(|error| postgres_error("decode lease run", error))?,
                ))
            })
            .collect()
    }

    async fn quarantine_expiry_entry(
        &self,
        expires_at_ms: i64,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<()> {
        sqlx::query(
            r#"
            DELETE FROM agnt5_runtime.leases
            WHERE namespace = $1 AND expires_at_ms = $2
              AND project_id = $3 AND run_id = $4
            "#,
        )
        .bind(self.namespace())
        .bind(expires_at_ms)
        .bind(project_id)
        .bind(run_id)
        .execute(self.pool())
        .await
        .map_err(|error| postgres_error("quarantine lease expiry", error))?;
        Ok(())
    }

    async fn scan_worker_leases(&self, worker_id: &str) -> EngineResult<Vec<(String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT project_id, run_id
            FROM agnt5_runtime.leases
            WHERE namespace = $1 AND worker_id = $2
            ORDER BY project_id ASC, run_id ASC
            "#,
        )
        .bind(self.namespace())
        .bind(worker_id)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan worker leases", error))?;
        decode_run_id_pairs(rows, "worker lease")
    }

    async fn scan_active_lease_runs(&self, limit: usize) -> EngineResult<Vec<(String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT project_id, run_id
            FROM agnt5_runtime.leases
            WHERE namespace = $1
            ORDER BY project_id ASC, run_id ASC
            LIMIT $2
            "#,
        )
        .bind(self.namespace())
        .bind(limit_i64(limit)?)
        .fetch_all(self.pool())
        .await
        .map_err(|error| postgres_error("scan active leases", error))?;
        decode_run_id_pairs(rows, "active lease")
    }

    async fn load_lease_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("runs", "run_id", project_id, run_id)
            .await
    }
}

pub(crate) fn limit_i64(limit: usize) -> EngineResult<i64> {
    i64::try_from(limit)
        .map_err(|_| EngineError::InvalidArgument("query limit exceeds BIGINT".to_string()))
}

fn decode_run_id_pairs(
    rows: Vec<sqlx::postgres::PgRow>,
    context: &str,
) -> EngineResult<Vec<(String, String)>> {
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("project_id")
                    .map_err(|error| postgres_error(&format!("decode {context} project"), error))?,
                row.try_get("run_id")
                    .map_err(|error| postgres_error(&format!("decode {context} run"), error))?,
            ))
        })
        .collect()
}

/// The open-source runtime keeps no always-on agent instances; the
/// supervisor's reads refuse.
impl engine_core::storage_contract::InstanceSupervisorStore for PostgresStorage {}
