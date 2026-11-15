use async_trait::async_trait;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    PendingDemandIndexAdmin, PendingDemandIndexOutcome, PendingDemandRow,
};
use prost::Message;
use sqlx::{Postgres, QueryBuilder, Row};

use crate::{postgres_error, PostgresStorage};

const INDEX_NAME: &str = "pending_demand";
const FORMAT_VERSION: i32 = 1;
const PAGE_SIZE: i64 = 512;

#[async_trait]
impl PendingDemandIndexAdmin for PostgresStorage {
    async fn ensure_pending_demand_index(
        &self,
        partition_count: usize,
    ) -> EngineResult<PendingDemandIndexOutcome> {
        if !(1..=engine_core::partition_offset::MAX_PARTITION_COUNT).contains(&partition_count) {
            return Err(EngineError::InvalidArgument(format!(
                "partition count must be in 1..={}, got {partition_count}",
                engine_core::partition_offset::MAX_PARTITION_COUNT
            )));
        }
        let partition_count = i32::try_from(partition_count).map_err(|_| {
            EngineError::InvalidArgument("partition count exceeds PostgreSQL INTEGER".into())
        })?;
        let mut transaction = self
            .pool()
            .begin()
            .await
            .map_err(|error| postgres_error("begin pending demand rebuild", error))?;

        // This is a startup gate. The table locks make the method safe even if
        // a caller accidentally overlaps it with a projection commit: writers
        // either land entirely before the snapshot or dual-write after it.
        sqlx::query(
            r#"
            LOCK TABLE agnt5_runtime.pending_runs,
                       agnt5_runtime.pending_demand,
                       agnt5_runtime.derived_index_state
            IN SHARE ROW EXCLUSIVE MODE
            "#,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| postgres_error("lock pending demand rebuild sources", error))?;

        let complete = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM agnt5_runtime.derived_index_state
                WHERE namespace = $1 AND index_name = $2
                  AND format_version = $3 AND partition_count = $4
            )
            "#,
        )
        .bind(self.namespace())
        .bind(INDEX_NAME)
        .bind(FORMAT_VERSION)
        .bind(partition_count)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| postgres_error("read pending demand rebuild marker", error))?;
        if complete {
            transaction
                .commit()
                .await
                .map_err(|error| postgres_error("finish pending demand rebuild check", error))?;
            return Ok(PendingDemandIndexOutcome::default());
        }

        sqlx::query("DELETE FROM agnt5_runtime.pending_demand WHERE namespace = $1")
            .bind(self.namespace())
            .execute(&mut *transaction)
            .await
            .map_err(|error| postgres_error("clear pending demand index", error))?;
        sqlx::query(
            "DELETE FROM agnt5_runtime.derived_index_state WHERE namespace = $1 AND index_name = $2",
        )
        .bind(self.namespace())
        .bind(INDEX_NAME)
        .execute(&mut *transaction)
        .await
        .map_err(|error| postgres_error("invalidate pending demand rebuild marker", error))?;

        let mut after_project = String::new();
        let mut after_component = String::new();
        let mut after_run = String::new();
        let mut rebuilt_rows = 0_usize;
        loop {
            let source_rows = sqlx::query(
                r#"
                SELECT project_id, component_name, run_id, data
                FROM agnt5_runtime.pending_runs
                WHERE namespace = $1
                  AND (project_id, component_name, run_id) > ($2, $3, $4)
                ORDER BY project_id, component_name, run_id
                LIMIT $5
                "#,
            )
            .bind(self.namespace())
            .bind(&after_project)
            .bind(&after_component)
            .bind(&after_run)
            .bind(PAGE_SIZE)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|error| postgres_error("scan pending runs for demand rebuild", error))?;
            if source_rows.is_empty() {
                break;
            }

            let mut rows = Vec::with_capacity(source_rows.len());
            for source in &source_rows {
                let project_id: String = source
                    .try_get("project_id")
                    .map_err(|error| postgres_error("decode pending run project", error))?;
                let component_name: String = source
                    .try_get("component_name")
                    .map_err(|error| postgres_error("decode pending run component", error))?;
                let run_id: String = source
                    .try_get("run_id")
                    .map_err(|error| postgres_error("decode pending run id", error))?;
                let data: Vec<u8> = source
                    .try_get("data")
                    .map_err(|error| postgres_error("decode pending run data", error))?;
                let run = engine_proto::Run::decode(data.as_slice()).map_err(|error| {
                    EngineError::Storage(format!(
                        "decode pending run {project_id}/{run_id} for demand rebuild: {error}"
                    ))
                })?;
                if run.project_id != project_id
                    || run.component_name != component_name
                    || run.run_id != run_id
                {
                    return Err(EngineError::Storage(format!(
                        "pending run key {project_id}/{component_name}/{run_id} disagrees with its projection"
                    )));
                }
                rows.push(PendingDemandRow::from_projected_run(
                    &run,
                    partition_count as usize,
                )?);
            }

            let mut insert = QueryBuilder::<Postgres>::new(
                "INSERT INTO agnt5_runtime.pending_demand (namespace, partition_id, project_id, deployment_id, component_type, component_name, priority, ready_at_ms, created_at_ms, run_id) ",
            );
            insert.push_values(&rows, |mut values, row| {
                values
                    .push_bind(self.namespace())
                    .push_bind(row.partition_id as i32)
                    .push_bind(row.demand_key.project_id())
                    .push_bind(row.demand_key.deployment_id())
                    .push_bind(row.demand_key.component_type_value())
                    .push_bind(row.demand_key.component_name())
                    .push_bind(i16::from(row.rank.priority))
                    .push_bind(row.rank.ready_at_ms)
                    .push_bind(row.rank.created_at_ms)
                    .push_bind(&row.rank.run_id);
            });
            insert.push(" ON CONFLICT DO NOTHING");
            insert
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(|error| postgres_error("write pending demand rebuild page", error))?;
            rebuilt_rows += rows.len();

            let last = source_rows.last().expect("non-empty page");
            after_project = last
                .try_get("project_id")
                .map_err(|error| postgres_error("decode pending run page project", error))?;
            after_component = last
                .try_get("component_name")
                .map_err(|error| postgres_error("decode pending run page component", error))?;
            after_run = last
                .try_get("run_id")
                .map_err(|error| postgres_error("decode pending run page id", error))?;
            if source_rows.len() < PAGE_SIZE as usize {
                break;
            }
        }

        sqlx::query(
            r#"
            INSERT INTO agnt5_runtime.derived_index_state
                (namespace, index_name, format_version, partition_count)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (namespace, index_name) DO UPDATE SET
                format_version = EXCLUDED.format_version,
                partition_count = EXCLUDED.partition_count,
                completed_at = NOW()
            "#,
        )
        .bind(self.namespace())
        .bind(INDEX_NAME)
        .bind(FORMAT_VERSION)
        .bind(partition_count)
        .execute(&mut *transaction)
        .await
        .map_err(|error| postgres_error("certify pending demand rebuild", error))?;
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit pending demand rebuild", error))?;
        Ok(PendingDemandIndexOutcome {
            rebuilt: true,
            rows: rebuilt_rows,
        })
    }
}
