use async_trait::async_trait;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    PendingDemandIndexAdmin, PendingDemandIndexOutcome, PendingDemandRow,
};
use prost::Message;

use crate::{libsql_error, LibsqlStorage};

const INDEX_NAME: &str = "pending_demand";
const FORMAT_VERSION: i32 = 1;
// Ten bind parameters per rebuilt row. Staying below SQLite's historical
// 999-variable limit keeps the same statement portable to remote libSQL
// deployments with conservative limits.
const PAGE_SIZE: i64 = 64;

#[async_trait]
impl PendingDemandIndexAdmin for LibsqlStorage {
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
            EngineError::InvalidArgument("partition count exceeds libSQL INTEGER".into())
        })?;
        // Immediate transactions acquire writer authority before reading the
        // source. Projection writers therefore land wholly before this
        // snapshot or dual-write after the certified rebuild commits.
        let transaction = self
            .new_connection()
            .await?
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|error| libsql_error("begin pending demand rebuild", error))?;

        let complete: i64 = crate::db::query(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM agnt5_runtime_derived_index_state
                WHERE namespace = ?1 AND index_name = ?2
                  AND format_version = ?3 AND partition_count = ?4
            ) AS complete
            "#,
        )
        .bind(self.namespace())
        .bind(INDEX_NAME)
        .bind(FORMAT_VERSION)
        .bind(partition_count)
        .fetch_one(&transaction)
        .await
        .map_err(|error| libsql_error("read pending demand rebuild marker", error))?
        .try_get("complete")
        .map_err(|error| libsql_error("decode pending demand rebuild marker", error))?;
        if complete != 0 {
            transaction
                .commit()
                .await
                .map_err(|error| libsql_error("finish pending demand rebuild check", error))?;
            return Ok(PendingDemandIndexOutcome::default());
        }

        crate::db::query("DELETE FROM agnt5_runtime_pending_demand WHERE namespace = ?1")
            .bind(self.namespace())
            .execute(&transaction)
            .await
            .map_err(|error| libsql_error("clear pending demand index", error))?;
        crate::db::query(
            "DELETE FROM agnt5_runtime_derived_index_state WHERE namespace = ?1 AND index_name = ?2",
        )
        .bind(self.namespace())
        .bind(INDEX_NAME)
        .execute(&transaction)
        .await
        .map_err(|error| libsql_error("invalidate pending demand rebuild marker", error))?;

        let mut after_project = String::new();
        let mut after_component = String::new();
        let mut after_run = String::new();
        let mut rebuilt_rows = 0_usize;
        loop {
            let source_rows = crate::db::query(
                r#"
                SELECT project_id, component_name, run_id, data
                FROM agnt5_runtime_pending_runs
                WHERE namespace = ?1
                  AND (project_id, component_name, run_id) > (?2, ?3, ?4)
                ORDER BY project_id, component_name, run_id
                LIMIT ?5
                "#,
            )
            .bind(self.namespace())
            .bind(&after_project)
            .bind(&after_component)
            .bind(&after_run)
            .bind(PAGE_SIZE)
            .fetch_all(&transaction)
            .await
            .map_err(|error| libsql_error("scan pending runs for demand rebuild", error))?;
            if source_rows.is_empty() {
                break;
            }

            let mut rows = Vec::with_capacity(source_rows.len());
            for source in &source_rows {
                let project_id: String = source
                    .try_get("project_id")
                    .map_err(|error| libsql_error("decode pending run project", error))?;
                let component_name: String = source
                    .try_get("component_name")
                    .map_err(|error| libsql_error("decode pending run component", error))?;
                let run_id: String = source
                    .try_get("run_id")
                    .map_err(|error| libsql_error("decode pending run id", error))?;
                let data: Vec<u8> = source
                    .try_get("data")
                    .map_err(|error| libsql_error("decode pending run data", error))?;
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

            let values = std::iter::repeat_n("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows.len())
                .collect::<Vec<_>>()
                .join(", ");
            let statement = format!(
                "INSERT INTO agnt5_runtime_pending_demand \
                 (namespace, partition_id, project_id, deployment_id, component_type, \
                  component_name, priority, ready_at_ms, created_at_ms, run_id) \
                 VALUES {values} ON CONFLICT DO NOTHING"
            );
            let mut insert = crate::db::query(statement);
            for row in &rows {
                insert = insert
                    .bind(self.namespace())
                    .bind(row.partition_id)
                    .bind(row.demand_key.project_id())
                    .bind(row.demand_key.deployment_id())
                    .bind(row.demand_key.component_type_value())
                    .bind(row.demand_key.component_name())
                    .bind(row.rank.priority)
                    .bind(row.rank.ready_at_ms)
                    .bind(row.rank.created_at_ms)
                    .bind(&row.rank.run_id);
            }
            insert
                .execute(&transaction)
                .await
                .map_err(|error| libsql_error("write pending demand rebuild page", error))?;
            rebuilt_rows += rows.len();

            let last = source_rows.last().expect("non-empty page");
            after_project = last
                .try_get("project_id")
                .map_err(|error| libsql_error("decode pending run page project", error))?;
            after_component = last
                .try_get("component_name")
                .map_err(|error| libsql_error("decode pending run page component", error))?;
            after_run = last
                .try_get("run_id")
                .map_err(|error| libsql_error("decode pending run page id", error))?;
            if source_rows.len() < PAGE_SIZE as usize {
                break;
            }
        }

        crate::db::query(
            r#"
            INSERT INTO agnt5_runtime_derived_index_state
                (namespace, index_name, format_version, partition_count)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT (namespace, index_name) DO UPDATE SET
                format_version = EXCLUDED.format_version,
                partition_count = EXCLUDED.partition_count,
                completed_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(self.namespace())
        .bind(INDEX_NAME)
        .bind(FORMAT_VERSION)
        .bind(partition_count)
        .execute(&transaction)
        .await
        .map_err(|error| libsql_error("certify pending demand rebuild", error))?;
        transaction
            .commit()
            .await
            .map_err(|error| libsql_error("commit pending demand rebuild", error))?;
        Ok(PendingDemandIndexOutcome {
            rebuilt: true,
            rows: rebuilt_rows,
        })
    }
}
