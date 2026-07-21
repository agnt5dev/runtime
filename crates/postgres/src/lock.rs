use agnt5_core::JournalError;
use sqlx::{Connection, PgConnection};

use crate::storage_error;

const RUNTIME_LOCK_ID: i64 = 0x4147_4e54_3500_0001;

/// Session-scoped ownership guard for a community runtime database.
///
/// The lock owns a dedicated connection. A pooled connection must not hold a
/// session advisory lock because returning it to the pool would preserve the
/// lock for an unrelated future borrower.
pub struct RuntimeLock {
    connection: PgConnection,
}

impl RuntimeLock {
    pub async fn acquire(database_url: &str) -> Result<Self, JournalError> {
        let mut connection = PgConnection::connect(database_url)
            .await
            .map_err(storage_error)?;
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(RUNTIME_LOCK_ID)
            .fetch_one(&mut connection)
            .await
            .map_err(storage_error)?;
        if !acquired {
            return Err(JournalError::Storage(
                "another AGNT5 community runtime owns this database".into(),
            ));
        }
        Ok(Self { connection })
    }

    pub async fn release(mut self) -> Result<(), JournalError> {
        sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
            .bind(RUNTIME_LOCK_ID)
            .fetch_one(&mut self.connection)
            .await
            .map_err(storage_error)?;
        self.connection.close().await.map_err(storage_error)
    }
}
