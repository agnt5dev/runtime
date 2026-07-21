use agnt5_core::{JournalError, KvOp, MaterializedStore, Offset};
use async_trait::async_trait;
use sqlx::PgPool;

use crate::storage_error;

#[derive(Clone)]
pub struct PostgresMaterializedStore {
    pool: PgPool,
}

impl PostgresMaterializedStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MaterializedStore for PostgresMaterializedStore {
    async fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, JournalError> {
        sqlx::query_scalar("SELECT value FROM agnt5_materialized WHERE namespace = $1 AND key = $2")
            .bind(namespace)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error)
    }

    async fn write_batch(&self, operations: &[KvOp]) -> Result<(), JournalError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        for operation in operations {
            match operation {
                KvOp::Put {
                    namespace,
                    key,
                    value,
                } => {
                    sqlx::query(
                        "INSERT INTO agnt5_materialized (namespace, key, value) \
                         VALUES ($1, $2, $3) \
                         ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value",
                    )
                    .bind(namespace)
                    .bind(key)
                    .bind(value)
                    .execute(&mut *transaction)
                    .await
                    .map_err(storage_error)?;
                }
                KvOp::Delete { namespace, key } => {
                    sqlx::query("DELETE FROM agnt5_materialized WHERE namespace = $1 AND key = $2")
                        .bind(namespace)
                        .bind(key)
                        .execute(&mut *transaction)
                        .await
                        .map_err(storage_error)?;
                }
            }
        }
        transaction.commit().await.map_err(storage_error)
    }

    async fn get_checkpoint(&self, processor: &str) -> Result<Option<Offset>, JournalError> {
        let offset = sqlx::query_scalar::<_, i64>(
            "SELECT offset_value FROM agnt5_checkpoints WHERE processor = $1",
        )
        .bind(processor)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        offset
            .map(|value| {
                u64::try_from(value)
                    .map(Offset)
                    .map_err(|_| JournalError::Storage("negative checkpoint".into()))
            })
            .transpose()
    }

    async fn set_checkpoint(&self, processor: &str, offset: Offset) -> Result<(), JournalError> {
        let offset = i64::try_from(offset.0)
            .map_err(|_| JournalError::Storage("checkpoint exceeds i64::MAX".into()))?;
        sqlx::query(
            "INSERT INTO agnt5_checkpoints (processor, offset_value) VALUES ($1, $2) \
             ON CONFLICT (processor) DO UPDATE SET \
             offset_value = EXCLUDED.offset_value, updated_at = transaction_timestamp()",
        )
        .bind(processor)
        .bind(offset)
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(())
    }
}
