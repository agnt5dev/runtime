use std::time::Duration;

use agnt5_core::{
    AppendOutcome, JournalError, JournalRecord, NewJournalRecord, Offset, RecordStream, Segment,
};
use async_trait::async_trait;
use bytes::Bytes;
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::storage_error;

#[derive(Clone)]
pub struct PostgresSegment {
    pool: PgPool,
    partition_id: u32,
    poll_interval: Duration,
}

impl PostgresSegment {
    pub async fn open(pool: PgPool, partition_id: u32) -> Result<Self, JournalError> {
        sqlx::query("INSERT INTO agnt5_segments (partition_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(i64::from(partition_id))
            .execute(&pool)
            .await
            .map_err(storage_error)?;

        Ok(Self {
            pool,
            partition_id,
            poll_interval: Duration::from_millis(100),
        })
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    async fn existing_offset(
        transaction: &mut Transaction<'_, Postgres>,
        partition_id: u32,
        idempotency_key: &[u8],
    ) -> Result<Option<Offset>, JournalError> {
        let offset = sqlx::query_scalar::<_, i64>(
            "SELECT offset_value FROM agnt5_journal \
             WHERE partition_id = $1 AND idempotency_key = $2",
        )
        .bind(i64::from(partition_id))
        .bind(idempotency_key)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        offset.map(offset_from_i64).transpose()
    }
}

#[async_trait]
impl Segment for PostgresSegment {
    async fn append_batch(
        &self,
        records: &[NewJournalRecord],
    ) -> Result<AppendOutcome, JournalError> {
        if records.is_empty() {
            return Ok(AppendOutcome {
                offsets: Vec::new(),
                appended: Vec::new(),
            });
        }

        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let row = sqlx::query(
            "SELECT next_offset, sealed FROM agnt5_segments \
             WHERE partition_id = $1 FOR UPDATE",
        )
        .bind(i64::from(self.partition_id))
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let mut next_offset = row
            .try_get::<i64, _>("next_offset")
            .map_err(storage_error)?;
        if row.try_get::<bool, _>("sealed").map_err(storage_error)? {
            return Err(JournalError::Sealed);
        }

        let mut offsets = Vec::with_capacity(records.len());
        let mut appended = Vec::with_capacity(records.len());
        for record in records {
            if let Some(key) = record.idempotency_key.as_deref() {
                if let Some(existing) =
                    Self::existing_offset(&mut transaction, self.partition_id, key).await?
                {
                    offsets.push(existing);
                    appended.push(false);
                    continue;
                }
            }

            let assigned = offset_from_i64(next_offset)?;
            sqlx::query(
                "INSERT INTO agnt5_journal \
                 (partition_id, offset_value, idempotency_key, payload) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(i64::from(self.partition_id))
            .bind(next_offset)
            .bind(record.idempotency_key.as_deref())
            .bind(record.payload.as_ref())
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
            next_offset = next_offset
                .checked_add(1)
                .ok_or_else(|| JournalError::Storage("journal offset overflow".into()))?;
            offsets.push(assigned);
            appended.push(true);
        }

        sqlx::query("UPDATE agnt5_segments SET next_offset = $2 WHERE partition_id = $1")
            .bind(i64::from(self.partition_id))
            .bind(next_offset)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(AppendOutcome { offsets, appended })
    }

    async fn tail(&self, from: Offset) -> Result<RecordStream, JournalError> {
        let segment = self.clone();
        let stream = async_stream::stream! {
            let mut cursor = from;
            loop {
                match segment.read_range(cursor, Offset(u64::MAX)).await {
                    Ok(records) if records.is_empty() => {
                        tokio::time::sleep(segment.poll_interval).await;
                    }
                    Ok(records) => {
                        for record in records {
                            cursor = Offset(record.offset.0 + 1);
                            yield Ok(record);
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn read_range(
        &self,
        from: Offset,
        to: Offset,
    ) -> Result<Vec<JournalRecord>, JournalError> {
        if from >= to {
            return Ok(Vec::new());
        }
        let retained_from = sqlx::query_scalar::<_, i64>(
            "SELECT retained_from FROM agnt5_segments WHERE partition_id = $1",
        )
        .bind(i64::from(self.partition_id))
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        if from.0 < offset_from_i64(retained_from)?.0 {
            return Err(JournalError::OffsetOutOfRange(from.0));
        }

        let rows = sqlx::query(
            "SELECT offset_value, idempotency_key, payload FROM agnt5_journal \
             WHERE partition_id = $1 AND offset_value >= $2 AND offset_value < $3 \
             ORDER BY offset_value ASC",
        )
        .bind(i64::from(self.partition_id))
        .bind(offset_to_i64(from)?)
        .bind(i64::try_from(to.0).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;

        rows.into_iter()
            .map(|row| {
                Ok(JournalRecord {
                    partition_id: self.partition_id,
                    offset: offset_from_i64(
                        row.try_get::<i64, _>("offset_value")
                            .map_err(storage_error)?,
                    )?,
                    idempotency_key: row
                        .try_get::<Option<Vec<u8>>, _>("idempotency_key")
                        .map_err(storage_error)?,
                    payload: Bytes::from(
                        row.try_get::<Vec<u8>, _>("payload")
                            .map_err(storage_error)?,
                    ),
                })
            })
            .collect()
    }

    async fn tail_offset(&self) -> Result<Offset, JournalError> {
        let offset = sqlx::query_scalar::<_, i64>(
            "SELECT next_offset FROM agnt5_segments WHERE partition_id = $1",
        )
        .bind(i64::from(self.partition_id))
        .fetch_one(&self.pool)
        .await
        .map_err(storage_error)?;
        offset_from_i64(offset)
    }

    async fn seal(&self) -> Result<(), JournalError> {
        sqlx::query("UPDATE agnt5_segments SET sealed = TRUE WHERE partition_id = $1")
            .bind(i64::from(self.partition_id))
            .execute(&self.pool)
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn trim(&self, through: Offset) -> Result<(), JournalError> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let next_offset = sqlx::query_scalar::<_, i64>(
            "SELECT next_offset FROM agnt5_segments WHERE partition_id = $1 FOR UPDATE",
        )
        .bind(i64::from(self.partition_id))
        .fetch_one(&mut *transaction)
        .await
        .map_err(storage_error)?;
        let retained_from = through
            .0
            .saturating_add(1)
            .min(offset_from_i64(next_offset)?.0);
        sqlx::query("DELETE FROM agnt5_journal WHERE partition_id = $1 AND offset_value <= $2")
            .bind(i64::from(self.partition_id))
            .bind(offset_to_i64(through)?)
            .execute(&mut *transaction)
            .await
            .map_err(storage_error)?;
        sqlx::query(
            "UPDATE agnt5_segments SET retained_from = GREATEST(retained_from, $2) \
             WHERE partition_id = $1",
        )
        .bind(i64::from(self.partition_id))
        .bind(i64::try_from(retained_from).map_err(storage_error)?)
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(())
    }
}

fn offset_to_i64(offset: Offset) -> Result<i64, JournalError> {
    i64::try_from(offset.0).map_err(|_| JournalError::Storage("offset exceeds i64::MAX".into()))
}

fn offset_from_i64(offset: i64) -> Result<Offset, JournalError> {
    u64::try_from(offset)
        .map(Offset)
        .map_err(|_| JournalError::Storage("database returned a negative offset".into()))
}
