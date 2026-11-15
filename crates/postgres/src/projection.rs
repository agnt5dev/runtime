use async_trait::async_trait;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    ProjectionCheckpointStore, ProjectionCommit, ProjectionCommitOutcome, ProjectionCommitter,
    ProjectionMutation, ProjectionStateReader, StorageCapabilities,
};
use engine_core::types::Offset;
use prost::Message;
use sqlx::{Postgres, Row, Transaction};

use crate::{encode_offset, postgres_error, PostgresStorage};

impl PostgresStorage {
    pub(crate) async fn load_projection_blob(
        &self,
        table: &'static str,
        id_column: &'static str,
        project_id: &str,
        id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        let statement =
            format!("SELECT data FROM agnt5_runtime.{table} WHERE namespace = $1 AND project_id = $2 AND {id_column} = $3");
        // Both identifiers are internal constants selected by trait methods;
        // project and object identities remain bind parameters.
        let row = sqlx::query(sqlx::AssertSqlSafe(statement))
            .bind(self.namespace())
            .bind(project_id)
            .bind(id)
            .fetch_optional(self.pool())
            .await
            .map_err(|error| postgres_error("load projection", error))?;
        row.map(|row| {
            row.try_get("data")
                .map_err(|error| postgres_error("decode projection data", error))
        })
        .transpose()
    }

    async fn load_projection_entity_blob(
        &self,
        project_id: &str,
        entity_type: &str,
        entity_key: &str,
        scope: &str,
        scope_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        let row = sqlx::query(
            r#"
            SELECT data
            FROM agnt5_runtime.entity_states
            WHERE namespace = $1 AND project_id = $2 AND entity_type = $3
              AND entity_key = $4 AND scope = $5 AND scope_id = $6
            "#,
        )
        .bind(self.namespace())
        .bind(project_id)
        .bind(entity_type)
        .bind(entity_key)
        .bind(scope)
        .bind(scope_id)
        .fetch_optional(self.pool())
        .await
        .map_err(|error| postgres_error("load projection entity", error))?;
        row.map(|row| {
            row.try_get("data")
                .map_err(|error| postgres_error("decode projection entity", error))
        })
        .transpose()
    }
}

#[async_trait]
impl ProjectionCommitter for PostgresStorage {
    fn capabilities(&self) -> StorageCapabilities {
        PostgresStorage::capabilities(self)
    }

    async fn commit(&self, commit: ProjectionCommit) -> EngineResult<ProjectionCommitOutcome> {
        commit.validate_capabilities(&StorageCapabilities::none())?;

        let source = commit.source();
        let record = source.record();
        let project_id = record.project_id.as_str();
        let run_id = record.run_id.as_str();
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|error| postgres_error("begin projection commit", error))?;
        let mut backend_operations = 0_u64;

        for mutation in commit.mutations() {
            match mutation {
                ProjectionMutation::PutRun(data) => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.runs
                            (namespace, project_id, run_id, data)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (namespace, project_id, run_id)
                        DO UPDATE SET data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(run_id)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit run projection", error))?;
                }
                ProjectionMutation::PutTerminalObligation {
                    partition_id,
                    run_data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.terminal_obligations
                            (namespace, partition_id, project_id, run_id, data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, partition_id, project_id, run_id)
                        DO UPDATE SET data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(i32::try_from(*partition_id).map_err(|_| {
                        EngineError::InvalidArgument(
                            "partition id exceeds PostgreSQL INTEGER".into(),
                        )
                    })?)
                    .bind(project_id)
                    .bind(run_id)
                    .bind(run_data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit terminal obligation", error))?;
                }
                ProjectionMutation::DeleteTerminalObligation { partition_id } => {
                    sqlx::query(
                        r#"
                        DELETE FROM agnt5_runtime.terminal_obligations
                        WHERE namespace = $1 AND partition_id = $2
                          AND project_id = $3 AND run_id = $4
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(i32::try_from(*partition_id).map_err(|_| {
                        EngineError::InvalidArgument(
                            "partition id exceeds PostgreSQL INTEGER".into(),
                        )
                    })?)
                    .bind(project_id)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("delete terminal obligation", error))?;
                }
                ProjectionMutation::PutStepMemo { step_key, data } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.step_memos
                            (namespace, project_id, run_id, step_key, data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, run_id, step_key)
                        DO UPDATE SET data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(run_id)
                    .bind(step_key)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit step memo", error))?;
                }
                ProjectionMutation::PutActivation {
                    activation_id,
                    data,
                    version,
                } => {
                    upsert_entity_state(
                        &mut transaction,
                        self.namespace(),
                        project_id,
                        engine_core::storage_contract::ACTIVATION_PROJECTION_ENTITY_TYPE,
                        activation_id,
                        engine_core::storage_contract::ACTIVATION_PROJECTION_SCOPE,
                        run_id,
                        data,
                        *version,
                    )
                    .await?;
                }
                ProjectionMutation::PutActivationOutbox {
                    activation_id,
                    attempt,
                    intent_id,
                    data,
                } => {
                    upsert_entity_state(
                        &mut transaction,
                        self.namespace(),
                        project_id,
                        engine_core::storage_contract::ACTIVATION_OUTBOX_ENTITY_TYPE,
                        &format!("{activation_id}/{attempt}/{intent_id}"),
                        engine_core::storage_contract::ACTIVATION_PROJECTION_SCOPE,
                        run_id,
                        data,
                        i64::from(*attempt),
                    )
                    .await?;
                }
                ProjectionMutation::PutEvent => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.events
                            (namespace, project_id, run_id, timestamp_ns, record_bytes)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, run_id, timestamp_ns)
                        DO UPDATE SET record_bytes = EXCLUDED.record_bytes
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(run_id)
                    .bind(record.timestamp_ns)
                    .bind(record.to_proto().encode_to_vec())
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit run event", error))?;
                }
                ProjectionMutation::PutEntityState {
                    entity_type,
                    entity_key,
                    scope,
                    scope_id,
                    data,
                    version,
                } => {
                    upsert_entity_state(
                        &mut transaction,
                        self.namespace(),
                        project_id,
                        entity_type,
                        entity_key,
                        scope,
                        scope_id,
                        data,
                        *version,
                    )
                    .await?;
                }
                ProjectionMutation::PutApproval {
                    approval_id,
                    run_id,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.approvals
                            (namespace, project_id, approval_id, run_id, data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, approval_id)
                        DO UPDATE SET run_id = EXCLUDED.run_id, data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(approval_id)
                    .bind(run_id)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit approval", error))?;
                }
                ProjectionMutation::PutSignal {
                    signal_id,
                    run_id,
                    signal_name,
                    waiting_step,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.signals
                            (namespace, project_id, signal_id, run_id, signal_name, waiting_step, data)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (namespace, project_id, signal_id)
                        DO UPDATE SET
                            run_id = EXCLUDED.run_id,
                            signal_name = EXCLUDED.signal_name,
                            waiting_step = EXCLUDED.waiting_step,
                            data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(signal_id)
                    .bind(run_id)
                    .bind(signal_name)
                    .bind(waiting_step)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit signal", error))?;
                }
                ProjectionMutation::PutTimer {
                    timer_id,
                    run_id,
                    status,
                    fire_at_ns,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.timers
                            (namespace, project_id, timer_id, run_id, status, fire_at_ns, data)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (namespace, project_id, timer_id)
                        DO UPDATE SET
                            run_id = EXCLUDED.run_id,
                            status = EXCLUDED.status,
                            fire_at_ns = EXCLUDED.fire_at_ns,
                            data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(timer_id)
                    .bind(run_id)
                    .bind(status)
                    .bind(fire_at_ns)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit timer", error))?;
                }
                ProjectionMutation::PutJob {
                    job_id,
                    run_id,
                    batch_id,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.jobs
                            (namespace, project_id, job_id, run_id, batch_id, data)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (namespace, project_id, job_id)
                        DO UPDATE SET
                            run_id = EXCLUDED.run_id,
                            batch_id = EXCLUDED.batch_id,
                            data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(job_id)
                    .bind(run_id)
                    .bind(batch_id)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit job", error))?;
                }
                ProjectionMutation::PutBatch { batch_id, data } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.batches
                            (namespace, project_id, batch_id, data)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (namespace, project_id, batch_id)
                        DO UPDATE SET data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(batch_id)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit batch", error))?;
                }
                ProjectionMutation::PutSession {
                    session_id,
                    component_name,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.sessions
                            (namespace, project_id, session_id, component_name, data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, session_id)
                        DO UPDATE SET
                            component_name = EXCLUDED.component_name,
                            data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(session_id)
                    .bind(component_name)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit session", error))?;
                }
                ProjectionMutation::PutMessage {
                    message_id,
                    correlation_id,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.messages
                            (namespace, project_id, message_id, correlation_id, data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, message_id)
                        DO UPDATE SET
                            correlation_id = EXCLUDED.correlation_id,
                            data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(message_id)
                    .bind(correlation_id)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit message", error))?;
                }
                ProjectionMutation::PutPendingRun {
                    component_name,
                    data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.pending_runs
                            (namespace, project_id, component_name, run_id, data)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, component_name, run_id)
                        DO UPDATE SET data = EXCLUDED.data
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(component_name)
                    .bind(run_id)
                    .bind(data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit pending run", error))?;
                }
                ProjectionMutation::RemovePendingRun { component_name } => {
                    sqlx::query(
                        r#"
                        DELETE FROM agnt5_runtime.pending_runs
                        WHERE namespace = $1 AND project_id = $2
                          AND component_name = $3 AND run_id = $4
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(component_name)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("remove pending run", error))?;
                }
                ProjectionMutation::PutPendingPriority {
                    deployment_id,
                    component_type,
                    component_name,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.pending_priority
                            (namespace, project_id, deployment_id, component_name,
                             priority, ready_at_ms, created_at_ms, run_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(deployment_id)
                    .bind(component_name)
                    .bind(i16::from(*priority))
                    .bind(ready_at_ms)
                    .bind(created_at_ms)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit pending priority", error))?;
                    if engine_proto::ComponentType::try_from(*component_type)
                        .is_ok_and(|value| value != engine_proto::ComponentType::Unspecified)
                    {
                        sqlx::query(
                            r#"
                            INSERT INTO agnt5_runtime.pending_demand
                                (namespace, partition_id, project_id, deployment_id,
                                 component_type, component_name, priority, ready_at_ms,
                                 created_at_ms, run_id)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            ON CONFLICT DO NOTHING
                            "#,
                        )
                        .bind(self.namespace())
                        .bind(
                            i32::try_from(source.position().partition_id()).map_err(|_| {
                                EngineError::InvalidArgument(
                                    "partition id exceeds PostgreSQL INTEGER".into(),
                                )
                            })?,
                        )
                        .bind(project_id)
                        .bind(deployment_id)
                        .bind(*component_type)
                        .bind(component_name)
                        .bind(i16::from(*priority))
                        .bind(ready_at_ms)
                        .bind(created_at_ms)
                        .bind(run_id)
                        .execute(&mut *transaction)
                        .await
                        .map_err(|error| postgres_error("commit pending demand", error))?;
                        backend_operations += 1;
                    }
                }
                ProjectionMutation::RemovePendingPriority {
                    deployment_id,
                    component_type,
                    component_name,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                } => {
                    sqlx::query(
                        r#"
                        DELETE FROM agnt5_runtime.pending_priority
                        WHERE namespace = $1 AND project_id = $2
                          AND deployment_id = $3 AND component_name = $4
                          AND priority = $5 AND ready_at_ms = $6
                          AND created_at_ms = $7 AND run_id = $8
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(deployment_id)
                    .bind(component_name)
                    .bind(i16::from(*priority))
                    .bind(ready_at_ms)
                    .bind(created_at_ms)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("remove pending priority", error))?;
                    if engine_proto::ComponentType::try_from(*component_type)
                        .is_ok_and(|value| value != engine_proto::ComponentType::Unspecified)
                    {
                        sqlx::query(
                            r#"
                            DELETE FROM agnt5_runtime.pending_demand
                            WHERE namespace = $1 AND partition_id = $2
                              AND project_id = $3 AND deployment_id = $4
                              AND component_type = $5 AND component_name = $6
                              AND priority = $7 AND ready_at_ms = $8
                              AND created_at_ms = $9 AND run_id = $10
                            "#,
                        )
                        .bind(self.namespace())
                        .bind(
                            i32::try_from(source.position().partition_id()).map_err(|_| {
                                EngineError::InvalidArgument(
                                    "partition id exceeds PostgreSQL INTEGER".into(),
                                )
                            })?,
                        )
                        .bind(project_id)
                        .bind(deployment_id)
                        .bind(*component_type)
                        .bind(component_name)
                        .bind(i16::from(*priority))
                        .bind(ready_at_ms)
                        .bind(created_at_ms)
                        .bind(run_id)
                        .execute(&mut *transaction)
                        .await
                        .map_err(|error| postgres_error("remove pending demand", error))?;
                        backend_operations += 1;
                    }
                }
                ProjectionMutation::PutPendingDeploymentPriority {
                    deployment_id,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                } => {
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
                    .bind(i16::from(*priority))
                    .bind(ready_at_ms)
                    .bind(created_at_ms)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit deployment pending run", error))?;
                }
                ProjectionMutation::RemovePendingDeploymentPriority {
                    deployment_id,
                    priority,
                    ready_at_ms,
                    created_at_ms,
                } => {
                    sqlx::query(
                        r#"
                        DELETE FROM agnt5_runtime.pending_deployment_priority
                        WHERE namespace = $1 AND project_id = $2
                          AND deployment_id = $3 AND priority = $4
                          AND ready_at_ms = $5 AND created_at_ms = $6
                          AND run_id = $7
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(deployment_id)
                    .bind(i16::from(*priority))
                    .bind(ready_at_ms)
                    .bind(created_at_ms)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("remove deployment pending run", error))?;
                }
                ProjectionMutation::UpdateQueueCounters {
                    deployment_id,
                    priority,
                    ready_at_ms: _,
                    created_at_ms: _,
                    delta: _,
                } => {
                    let updated_at_ms = record.timestamp_ns / 1_000_000;
                    refresh_queue_counter(
                        &mut transaction,
                        self.namespace(),
                        project_id,
                        None,
                        None,
                        updated_at_ms,
                    )
                    .await?;
                    refresh_queue_counter(
                        &mut transaction,
                        self.namespace(),
                        project_id,
                        Some(deployment_id),
                        None,
                        updated_at_ms,
                    )
                    .await?;
                    refresh_queue_counter(
                        &mut transaction,
                        self.namespace(),
                        project_id,
                        Some(deployment_id),
                        Some(*priority),
                        updated_at_ms,
                    )
                    .await?;
                    backend_operations += 2;
                }
                ProjectionMutation::PutLeaseIndex {
                    expires_at_ms,
                    worker_id,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.leases
                            (namespace, project_id, run_id, expires_at_ms, worker_id)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, run_id)
                        DO UPDATE SET
                            expires_at_ms = EXCLUDED.expires_at_ms,
                            worker_id = EXCLUDED.worker_id
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(run_id)
                    .bind(expires_at_ms)
                    .bind(worker_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit lease index", error))?;
                }
                ProjectionMutation::RemoveLeaseIndex {
                    expires_at_ms,
                    worker_id,
                } => {
                    sqlx::query(
                        r#"
                        DELETE FROM agnt5_runtime.leases
                        WHERE namespace = $1 AND project_id = $2 AND run_id = $3
                          AND expires_at_ms = $4 AND worker_id = $5
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(run_id)
                    .bind(expires_at_ms)
                    .bind(worker_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("remove lease index", error))?;
                }
                ProjectionMutation::PutDispatchOutbox {
                    partition_id,
                    queued_offset,
                    request_data,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.dispatch_outbox
                            (namespace, partition_id, project_id, queued_offset, run_id, payload)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (namespace, partition_id, project_id, queued_offset, run_id)
                        DO UPDATE SET payload = EXCLUDED.payload
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(i32::try_from(*partition_id).map_err(|_| {
                        EngineError::InvalidArgument(
                            "partition id exceeds PostgreSQL INTEGER".into(),
                        )
                    })?)
                    .bind(project_id)
                    .bind(encode_offset(*queued_offset)?)
                    .bind(run_id)
                    .bind(request_data)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit dispatch outbox", error))?;
                }
                ProjectionMutation::DeleteDispatchOutbox {
                    partition_id,
                    project_id,
                    queued_offset,
                    run_id,
                } => {
                    sqlx::query(
                        r#"
                        DELETE FROM agnt5_runtime.dispatch_outbox
                        WHERE namespace = $1 AND partition_id = $2
                          AND project_id = $3 AND queued_offset = $4 AND run_id = $5
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(i32::try_from(*partition_id).map_err(|_| {
                        EngineError::InvalidArgument(
                            "partition id exceeds PostgreSQL INTEGER".into(),
                        )
                    })?)
                    .bind(project_id)
                    .bind(encode_offset(*queued_offset)?)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("delete dispatch outbox", error))?;
                }
                ProjectionMutation::PutRunHotIndex {
                    workspace_id,
                    submitted_at_ms,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.run_hot_index
                            (namespace, workspace_id, project_id, submitted_at_ms, run_id)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT DO NOTHING
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(workspace_id)
                    .bind(project_id)
                    .bind(submitted_at_ms)
                    .bind(run_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit hot run index", error))?;
                }
                ProjectionMutation::PutEventHotIndex {
                    timestamp_ms,
                    summary,
                } => {
                    sqlx::query(
                        r#"
                        INSERT INTO agnt5_runtime.event_hot_index
                            (namespace, project_id, timestamp_ms, run_id, summary)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (namespace, project_id, timestamp_ms, run_id)
                        DO UPDATE SET summary = EXCLUDED.summary
                        "#,
                    )
                    .bind(self.namespace())
                    .bind(project_id)
                    .bind(timestamp_ms)
                    .bind(run_id)
                    .bind(summary)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| postgres_error("commit hot event index", error))?;
                }
                ProjectionMutation::PutRuntimeWork(_) => {
                    return Err(EngineError::InvalidArgument(
                        "postgres profile does not support runtime work".into(),
                    ));
                }
                ProjectionMutation::PutRunArchiveIntent { .. }
                | ProjectionMutation::DeleteRunArchiveIntent { .. }
                | ProjectionMutation::PutRunArchiveLocation { .. }
                | ProjectionMutation::PutRunRetention { .. }
                | ProjectionMutation::PutRunQueryArchiveIntent { .. }
                | ProjectionMutation::DeleteRunQueryArchiveIntent { .. }
                | ProjectionMutation::PutRunQueryArchiveReceipt { .. } => {
                    return Err(EngineError::InvalidArgument(
                        "PostgreSQL profile does not support managed archive mutations".to_string(),
                    ));
                }
            }
            backend_operations += 1;
        }

        if let Some(position) = commit.checkpoint() {
            if record.offset != position.offset() {
                return Err(EngineError::InvalidArgument(format!(
                    "PostgreSQL projection source offset {} does not match local position {}",
                    record.offset.0,
                    position.offset().0
                )));
            }
            let key = format!("processor-partition-{}", position.partition_id());
            let result = sqlx::query(
                r#"
                INSERT INTO agnt5_runtime.projection_checkpoints
                    (namespace, checkpoint_key, checkpoint_offset)
                VALUES ($1, $2, $3)
                ON CONFLICT (namespace, checkpoint_key)
                DO UPDATE SET checkpoint_offset = EXCLUDED.checkpoint_offset
                WHERE agnt5_runtime.projection_checkpoints.checkpoint_offset
                      <= EXCLUDED.checkpoint_offset
                "#,
            )
            .bind(self.namespace())
            .bind(&key)
            .bind(encode_offset(position.offset().0)?)
            .execute(&mut *transaction)
            .await
            .map_err(|error| postgres_error("commit projection checkpoint", error))?;
            if result.rows_affected() == 0 {
                return Err(EngineError::InvalidArgument(format!(
                    "projection checkpoint regression rejected for {key}"
                )));
            }
            backend_operations += 1;
        }

        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit projection transaction", error))?;
        Ok(ProjectionCommitOutcome::with_backend_operations(
            backend_operations,
        ))
    }
}

#[async_trait]
impl ProjectionStateReader for PostgresStorage {
    async fn load_projection_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("runs", "run_id", project_id, run_id)
            .await
    }

    async fn load_projection_activation(
        &self,
        project_id: &str,
        run_id: &str,
        activation_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_entity_blob(
            project_id,
            engine_core::storage_contract::ACTIVATION_PROJECTION_ENTITY_TYPE,
            activation_id,
            engine_core::storage_contract::ACTIVATION_PROJECTION_SCOPE,
            run_id,
        )
        .await
    }

    async fn load_projection_approval(
        &self,
        project_id: &str,
        approval_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("approvals", "approval_id", project_id, approval_id)
            .await
    }

    async fn load_projection_signal(
        &self,
        project_id: &str,
        signal_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("signals", "signal_id", project_id, signal_id)
            .await
    }

    async fn load_projection_timer(
        &self,
        project_id: &str,
        timer_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("timers", "timer_id", project_id, timer_id)
            .await
    }

    async fn load_projection_job(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("jobs", "job_id", project_id, job_id)
            .await
    }

    async fn load_projection_batch(
        &self,
        project_id: &str,
        batch_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("batches", "batch_id", project_id, batch_id)
            .await
    }

    async fn load_projection_session(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("sessions", "session_id", project_id, session_id)
            .await
    }

    async fn load_projection_message(
        &self,
        project_id: &str,
        message_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        self.load_projection_blob("messages", "message_id", project_id, message_id)
            .await
    }
}

#[async_trait]
impl ProjectionCheckpointStore for PostgresStorage {
    async fn load_projection_checkpoint(&self, key: &str) -> EngineResult<Option<Offset>> {
        let row = sqlx::query(
            r#"
            SELECT checkpoint_offset
            FROM agnt5_runtime.projection_checkpoints
            WHERE namespace = $1 AND checkpoint_key = $2
            "#,
        )
        .bind(self.namespace())
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_err(|error| postgres_error("load projection checkpoint", error))?;
        row.map(|row| {
            let value: i64 = row
                .try_get("checkpoint_offset")
                .map_err(|error| postgres_error("decode projection checkpoint", error))?;
            Ok(Offset(crate::decode_offset(value)?))
        })
        .transpose()
    }

    async fn store_projection_checkpoint(&self, key: &str, offset: Offset) -> EngineResult<()> {
        sqlx::query(
            r#"
            INSERT INTO agnt5_runtime.projection_checkpoints
                (namespace, checkpoint_key, checkpoint_offset)
            VALUES ($1, $2, $3)
            ON CONFLICT (namespace, checkpoint_key)
            DO UPDATE SET checkpoint_offset = EXCLUDED.checkpoint_offset
            "#,
        )
        .bind(self.namespace())
        .bind(key)
        .bind(encode_offset(offset.0)?)
        .execute(self.pool())
        .await
        .map_err(|error| postgres_error("store projection checkpoint", error))?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn upsert_entity_state(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: &str,
    project_id: &str,
    entity_type: &str,
    entity_key: &str,
    scope: &str,
    scope_id: &str,
    data: &[u8],
    version: i64,
) -> EngineResult<()> {
    sqlx::query(
        r#"
        INSERT INTO agnt5_runtime.entity_states
            (namespace, project_id, entity_type, entity_key, scope, scope_id, version, data)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (namespace, project_id, entity_type, entity_key, scope, scope_id)
        DO UPDATE SET version = EXCLUDED.version, data = EXCLUDED.data
        "#,
    )
    .bind(namespace)
    .bind(project_id)
    .bind(entity_type)
    .bind(entity_key)
    .bind(scope)
    .bind(scope_id)
    .bind(version)
    .bind(data)
    .execute(&mut **transaction)
    .await
    .map_err(|error| postgres_error("store entity state", error))?;
    Ok(())
}

async fn refresh_queue_counter(
    transaction: &mut Transaction<'_, Postgres>,
    namespace: &str,
    project_id: &str,
    deployment_id: Option<&str>,
    priority: Option<u8>,
    updated_at_ms: i64,
) -> EngineResult<()> {
    let (scope_kind, deployment_key, priority_key, row) = match (deployment_id, priority) {
        (None, None) => (
            0_i16,
            "",
            -1_i16,
            sqlx::query(
                r#"
                SELECT priority, ready_at_ms, created_at_ms, deployment_id, run_id
                FROM agnt5_runtime.pending_deployment_priority
                WHERE namespace = $1 AND project_id = $2
                ORDER BY ready_at_ms ASC, deployment_id ASC, priority ASC,
                         created_at_ms ASC, run_id ASC
                LIMIT 1
                "#,
            )
            .bind(namespace)
            .bind(project_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| postgres_error("find project queue head", error))?,
        ),
        (Some(deployment_id), None) => (
            1_i16,
            deployment_id,
            -1_i16,
            sqlx::query(
                r#"
                SELECT priority, ready_at_ms, created_at_ms, deployment_id, run_id
                FROM agnt5_runtime.pending_deployment_priority
                WHERE namespace = $1 AND project_id = $2 AND deployment_id = $3
                ORDER BY ready_at_ms ASC, priority ASC, created_at_ms ASC, run_id ASC
                LIMIT 1
                "#,
            )
            .bind(namespace)
            .bind(project_id)
            .bind(deployment_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| postgres_error("find deployment queue head", error))?,
        ),
        (Some(deployment_id), Some(priority)) => (
            2_i16,
            deployment_id,
            i16::from(priority),
            sqlx::query(
                r#"
                SELECT priority, ready_at_ms, created_at_ms, deployment_id, run_id
                FROM agnt5_runtime.pending_deployment_priority
                WHERE namespace = $1 AND project_id = $2
                  AND deployment_id = $3 AND priority = $4
                ORDER BY ready_at_ms ASC, created_at_ms ASC, run_id ASC
                LIMIT 1
                "#,
            )
            .bind(namespace)
            .bind(project_id)
            .bind(deployment_id)
            .bind(i16::from(priority))
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| postgres_error("find priority queue head", error))?,
        ),
        (None, Some(_)) => {
            return Err(EngineError::InvalidArgument(
                "priority queue counter requires deployment id".to_string(),
            ));
        }
    };

    let count: i64 = match (deployment_id, priority) {
        (None, None) => sqlx::query_scalar(
            r#"
                SELECT COUNT(*)
                FROM agnt5_runtime.pending_deployment_priority
                WHERE namespace = $1 AND project_id = $2
                "#,
        )
        .bind(namespace)
        .bind(project_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| postgres_error("count project queue", error))?,
        (Some(deployment_id), None) => sqlx::query_scalar(
            r#"
                SELECT COUNT(*)
                FROM agnt5_runtime.pending_deployment_priority
                WHERE namespace = $1 AND project_id = $2 AND deployment_id = $3
                "#,
        )
        .bind(namespace)
        .bind(project_id)
        .bind(deployment_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| postgres_error("count deployment queue", error))?,
        (Some(deployment_id), Some(priority)) => sqlx::query_scalar(
            r#"
                SELECT COUNT(*)
                FROM agnt5_runtime.pending_deployment_priority
                WHERE namespace = $1 AND project_id = $2
                  AND deployment_id = $3 AND priority = $4
                "#,
        )
        .bind(namespace)
        .bind(project_id)
        .bind(deployment_id)
        .bind(i16::from(priority))
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| postgres_error("count priority queue", error))?,
        (None, Some(_)) => unreachable!(),
    };

    let (oldest_ready_at_ms, oldest_pending_key) = row
        .map(|row| -> EngineResult<(Option<i64>, Option<Vec<u8>>)> {
            let ready_at_ms: i64 = row
                .try_get("ready_at_ms")
                .map_err(|error| postgres_error("decode queue head ready_at", error))?;
            let priority: i16 = row
                .try_get("priority")
                .map_err(|error| postgres_error("decode queue head priority", error))?;
            let created_at_ms: i64 = row
                .try_get("created_at_ms")
                .map_err(|error| postgres_error("decode queue head created_at", error))?;
            let deployment_id: String = row
                .try_get("deployment_id")
                .map_err(|error| postgres_error("decode queue head deployment", error))?;
            let run_id: String = row
                .try_get("run_id")
                .map_err(|error| postgres_error("decode queue head run", error))?;
            let key = format!(
                "{project_id}\0{deployment_id}\0{priority}\0{ready_at_ms}\0{created_at_ms}\0{run_id}"
            )
            .into_bytes();
            Ok((Some(ready_at_ms), Some(key)))
        })
        .transpose()?
        .unwrap_or((None, None));

    sqlx::query(
        r#"
        INSERT INTO agnt5_runtime.queue_counters
            (namespace, scope_kind, project_id, deployment_id, priority,
             pending_count, oldest_ready_at_ms, oldest_pending_key, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (namespace, scope_kind, project_id, deployment_id, priority)
        DO UPDATE SET
            pending_count = EXCLUDED.pending_count,
            oldest_ready_at_ms = EXCLUDED.oldest_ready_at_ms,
            oldest_pending_key = EXCLUDED.oldest_pending_key,
            updated_at_ms = EXCLUDED.updated_at_ms
        "#,
    )
    .bind(namespace)
    .bind(scope_kind)
    .bind(project_id)
    .bind(deployment_key)
    .bind(priority_key)
    .bind(count)
    .bind(oldest_ready_at_ms)
    .bind(oldest_pending_key)
    .bind(updated_at_ms)
    .execute(&mut **transaction)
    .await
    .map_err(|error| postgres_error("refresh queue counter", error))?;
    Ok(())
}
