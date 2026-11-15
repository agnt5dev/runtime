use async_trait::async_trait;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    ActivationQueryStore, EntityStateWriteStore, QueueQueryStore, RunQueryStore, SessionQueryStore,
    WorkflowQueryStore, ACTIVATION_PROJECTION_ENTITY_TYPE, ACTIVATION_PROJECTION_SCOPE,
};
use engine_core::types::{QueueCounter, Record, RunSegmentChunk};
use prost::Message;

use crate::work::limit_i64;
use crate::{libsql_error, LibsqlStorage};

#[async_trait]
impl ActivationQueryStore for LibsqlStorage {
    async fn load_activation(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(WorkflowQueryStore::load_entity_state(
            self,
            project_id,
            ACTIVATION_PROJECTION_ENTITY_TYPE,
            activation_id,
            ACTIVATION_PROJECTION_SCOPE,
            run_id,
        )
        .await?
        .map(|(data, _)| data))
    }
}

#[async_trait]
impl RunQueryStore for LibsqlStorage {
    async fn load_run(&self, project_id: &str, run_id: &str) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("runs", "run_id", project_id, run_id)
            .await
    }

    async fn scan_hot_run_ids(
        &self,
        workspace_id: &str,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        let rows = match cursor {
            Some((submitted_at_ms, run_id)) => {
                crate::db::query(
                    r#"
                    SELECT run_id
                    FROM agnt5_runtime_run_hot_index
                    WHERE namespace = ?1 AND workspace_id = ?2 AND project_id = ?3
                      AND (
                        submitted_at_ms < ?4
                        OR (submitted_at_ms = ?4 AND run_id < ?5)
                      )
                    ORDER BY submitted_at_ms DESC, run_id DESC
                    LIMIT ?6
                    "#,
                )
                .bind(self.namespace())
                .bind(workspace_id)
                .bind(project_id)
                .bind(submitted_at_ms)
                .bind(run_id)
                .bind(limit_i64(limit)?)
                .fetch_all(self.connection())
                .await
            }
            None => {
                crate::db::query(
                    r#"
                    SELECT run_id
                    FROM agnt5_runtime_run_hot_index
                    WHERE namespace = ?1 AND workspace_id = ?2 AND project_id = ?3
                    ORDER BY submitted_at_ms DESC, run_id DESC
                    LIMIT ?4
                    "#,
                )
                .bind(self.namespace())
                .bind(workspace_id)
                .bind(project_id)
                .bind(limit_i64(limit)?)
                .fetch_all(self.connection())
                .await
            }
        }
        .map_err(|error| libsql_error("scan hot runs", error))?;

        rows.into_iter()
            .map(|row| {
                row.try_get("run_id")
                    .map_err(|error| libsql_error("decode hot run", error))
            })
            .collect()
    }

    async fn scan_event_summaries(
        &self,
        project_id: &str,
        cursor: Option<(i64, String)>,
        limit: usize,
    ) -> EngineResult<Vec<(String, i64, Vec<u8>)>> {
        let rows = match cursor {
            Some((timestamp_ms, run_id)) => {
                crate::db::query(
                    r#"
                    SELECT run_id, timestamp_ms, summary
                    FROM agnt5_runtime_event_hot_index
                    WHERE namespace = ?1 AND project_id = ?2
                      AND (
                        timestamp_ms < ?3
                        OR (timestamp_ms = ?3 AND run_id > ?4)
                      )
                    ORDER BY timestamp_ms DESC, run_id ASC
                    LIMIT ?5
                    "#,
                )
                .bind(self.namespace())
                .bind(project_id)
                .bind(timestamp_ms)
                .bind(run_id)
                .bind(limit_i64(limit)?)
                .fetch_all(self.connection())
                .await
            }
            None => {
                crate::db::query(
                    r#"
                    SELECT run_id, timestamp_ms, summary
                    FROM agnt5_runtime_event_hot_index
                    WHERE namespace = ?1 AND project_id = ?2
                    ORDER BY timestamp_ms DESC, run_id ASC
                    LIMIT ?3
                    "#,
                )
                .bind(self.namespace())
                .bind(project_id)
                .bind(limit_i64(limit)?)
                .fetch_all(self.connection())
                .await
            }
        }
        .map_err(|error| libsql_error("scan event summaries", error))?;

        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("run_id")
                        .map_err(|error| libsql_error("decode event summary run", error))?,
                    row.try_get("timestamp_ms")
                        .map_err(|error| libsql_error("decode event summary time", error))?,
                    row.try_get("summary")
                        .map_err(|error| libsql_error("decode event summary", error))?,
                ))
            })
            .collect()
    }

    async fn load_run_events(
        &self,
        project_id: &str,
        run_id: &str,
        from_timestamp_ns: i64,
    ) -> EngineResult<Vec<Record>> {
        let rows = crate::db::query(
            r#"
            SELECT record_bytes
            FROM agnt5_runtime_events
            WHERE namespace = ?1 AND project_id = ?2 AND run_id = ?3
              AND timestamp_ns >= ?4
            ORDER BY timestamp_ns ASC
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(run_id)
        .bind(from_timestamp_ns)
        .fetch_all(self.connection())
        .await
        .map_err(|error| libsql_error("load run events", error))?;
        rows.into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row
                    .try_get("record_bytes")
                    .map_err(|error| libsql_error("decode run event bytes", error))?;
                let proto = engine_proto::Record::decode(bytes.as_slice())
                    .map_err(|error| EngineError::Serialization(error.to_string()))?;
                Ok(Record::from_proto(proto))
            })
            .collect()
    }

    async fn scan_run_segments(
        &self,
        _project_id: &str,
        _run_id: &str,
    ) -> EngineResult<Vec<RunSegmentChunk>> {
        Ok(Vec::new())
    }

    async fn load_query_archive_path(
        &self,
        _project_id: &str,
        _run_id: &str,
    ) -> EngineResult<Option<String>> {
        Ok(None)
    }

    async fn load_step_memo(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        let row = crate::db::query(
            r#"
            SELECT data
            FROM agnt5_runtime_step_memos
            WHERE namespace = ?1 AND project_id = ?2
              AND run_id = ?3 AND step_key = ?4
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(run_id)
        .bind(step_key)
        .fetch_optional(self.connection())
        .await
        .map_err(|error| libsql_error("load step memo", error))?;
        decode_optional_blob(row, "step memo")
    }
}

#[async_trait]
impl WorkflowQueryStore for LibsqlStorage {
    async fn load_entity_state(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> EngineResult<Option<(Vec<u8>, i64)>> {
        let row = crate::db::query(
            r#"
            SELECT data, version
            FROM agnt5_runtime_entity_states
            WHERE namespace = ?1 AND project_id = ?2
              AND entity_type = ?3 AND entity_key = ?4
              AND scope = ?5 AND scope_id = ?6
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(entity_type)
        .bind(entity_key)
        .bind(scope)
        .bind(scope_id)
        .fetch_optional(self.connection())
        .await
        .map_err(|error| libsql_error("load entity state", error))?;
        row.map(|row| {
            Ok((
                row.try_get("data")
                    .map_err(|error| libsql_error("decode entity state", error))?,
                row.try_get("version")
                    .map_err(|error| libsql_error("decode entity state version", error))?,
            ))
        })
        .transpose()
    }

    async fn load_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("approvals", "approval_id", project_id, approval_id)
            .await
    }

    async fn scan_run_approvals(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(
            self,
            "approvals",
            "run_id",
            "approval_id",
            project_id,
            run_id,
        )
        .await
    }

    async fn load_signal(
        &self,
        project_id: &str,
        signal_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("signals", "signal_id", project_id, signal_id)
            .await
    }

    async fn lookup_run_signal(
        &self,
        project_id: &str,
        run_id: &str,
        signal_name: &str,
        waiting_step: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        let row = crate::db::query(
            r#"
            SELECT data
            FROM agnt5_runtime_signals
            WHERE namespace = ?1 AND project_id = ?2 AND run_id = ?3
              AND signal_name = ?4 AND waiting_step = ?5
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(run_id)
        .bind(signal_name)
        .bind(waiting_step)
        .fetch_optional(self.connection())
        .await
        .map_err(|error| libsql_error("lookup run signal", error))?;
        decode_optional_blob(row, "run signal")
    }

    async fn scan_run_signals(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(self, "signals", "run_id", "signal_id", project_id, run_id).await
    }

    async fn load_timer(&self, project_id: &str, timer_id: &str) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("timers", "timer_id", project_id, timer_id)
            .await
    }

    async fn scan_run_timers(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(self, "timers", "run_id", "timer_id", project_id, run_id).await
    }

    async fn load_job(&self, project_id: &str, job_id: &str) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("jobs", "job_id", project_id, job_id)
            .await
    }

    async fn scan_run_jobs(&self, project_id: &str, run_id: &str) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(self, "jobs", "run_id", "job_id", project_id, run_id).await
    }

    async fn scan_batch_jobs(
        &self,
        project_id: &str,
        batch_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(self, "jobs", "batch_id", "job_id", project_id, batch_id).await
    }

    async fn load_batch(&self, project_id: &str, batch_id: &str) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("batches", "batch_id", project_id, batch_id)
            .await
    }
}

#[async_trait]
impl EntityStateWriteStore for LibsqlStorage {
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
        crate::db::query(
            r#"
            INSERT INTO agnt5_runtime_entity_states
                (namespace, project_id, entity_type, entity_key, scope, scope_id, version, data)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT (namespace, project_id, entity_type, entity_key, scope, scope_id)
            DO UPDATE SET version = EXCLUDED.version, data = EXCLUDED.data
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(entity_type)
        .bind(entity_key)
        .bind(scope)
        .bind(scope_id)
        .bind(version)
        .bind(data)
        .execute(self.connection())
        .await
        .map_err(|error| libsql_error("write through entity state", error))?;
        Ok(())
    }
}

#[async_trait]
impl SessionQueryStore for LibsqlStorage {
    async fn load_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("sessions", "session_id", project_id, session_id)
            .await
    }

    async fn scan_component_sessions(
        &self,
        project_id: &str,
        component_name: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(
            self,
            "sessions",
            "component_name",
            "session_id",
            project_id,
            component_name,
        )
        .await
    }

    async fn load_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("messages", "message_id", project_id, message_id)
            .await
    }

    async fn scan_correlation_messages(
        &self,
        project_id: &str,
        correlation_id: &str,
    ) -> EngineResult<Vec<Vec<u8>>> {
        scan_related_blobs(
            self,
            "messages",
            "correlation_id",
            "message_id",
            project_id,
            correlation_id,
        )
        .await
    }
}

#[async_trait]
impl QueueQueryStore for LibsqlStorage {
    async fn load_project_queue_counter(&self, project_id: &str) -> EngineResult<QueueCounter> {
        load_queue_counter(self, 0, project_id, "", -1).await
    }

    async fn load_deployment_queue_counter(
        &self,
        project_id: &str,
        deployment_id: &str,
    ) -> EngineResult<QueueCounter> {
        load_queue_counter(self, 1, project_id, deployment_id, -1).await
    }

    async fn load_priority_queue_counter(
        &self,
        project_id: &str,
        deployment_id: &str,
        priority: u8,
    ) -> EngineResult<QueueCounter> {
        load_queue_counter(self, 2, project_id, deployment_id, i16::from(priority)).await
    }
}

async fn scan_related_blobs(
    storage: &LibsqlStorage,
    table: &'static str,
    relation_column: &'static str,
    order_column: &'static str,
    project_id: &str,
    relation_id: &str,
) -> EngineResult<Vec<Vec<u8>>> {
    let statement = format!(
        "SELECT data FROM agnt5_runtime_{table} \
         WHERE namespace = ?1 AND project_id = ?2 AND {relation_column} = ?3 \
         ORDER BY {order_column} ASC"
    );
    // Table and column identifiers are internal constants selected by the
    // trait implementations; external identities remain bind parameters.
    let rows = crate::db::query(statement)
        .bind(storage.namespace())
        .bind(project_id)
        .bind(relation_id)
        .fetch_all(storage.connection())
        .await
        .map_err(|error| libsql_error("scan related projections", error))?;
    rows.into_iter()
        .map(|row| {
            row.try_get("data")
                .map_err(|error| libsql_error("decode related projection", error))
        })
        .collect()
}

fn decode_optional_blob(
    row: Option<crate::db::DbRow>,
    context: &str,
) -> EngineResult<Option<Vec<u8>>> {
    row.map(|row| {
        row.try_get("data")
            .map_err(|error| libsql_error(&format!("decode {context}"), error))
    })
    .transpose()
}

async fn load_queue_counter(
    storage: &LibsqlStorage,
    scope_kind: i16,
    project_id: &str,
    deployment_id: &str,
    priority: i16,
) -> EngineResult<QueueCounter> {
    let row = crate::db::query(
        r#"
        SELECT pending_count, oldest_ready_at_ms, oldest_pending_key, updated_at_ms
        FROM agnt5_runtime_queue_counters
        WHERE namespace = ?1 AND scope_kind = ?2 AND project_id = ?3
          AND deployment_id = ?4 AND priority = ?5
        "#,
    )
    .bind(storage.namespace())
    .bind(scope_kind)
    .bind(project_id)
    .bind(deployment_id)
    .bind(priority)
    .fetch_optional(storage.connection())
    .await
    .map_err(|error| libsql_error("load queue counter", error))?;
    let Some(row) = row else {
        return Ok(QueueCounter::default());
    };
    let pending_count: i64 = row
        .try_get("pending_count")
        .map_err(|error| libsql_error("decode queue pending count", error))?;
    Ok(QueueCounter {
        pending_count: u64::try_from(pending_count)
            .map_err(|_| EngineError::Storage("negative queue pending count".to_string()))?,
        oldest_ready_at_ms: row
            .try_get("oldest_ready_at_ms")
            .map_err(|error| libsql_error("decode oldest queue time", error))?,
        oldest_pending_key: row
            .try_get("oldest_pending_key")
            .map_err(|error| libsql_error("decode oldest queue key", error))?,
        updated_at_ms: row
            .try_get("updated_at_ms")
            .map_err(|error| libsql_error("decode queue update time", error))?,
    })
}
