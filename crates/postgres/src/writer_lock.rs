use std::sync::Arc;

use engine_core::error::{EngineError, EngineResult};
use sqlx::postgres::PgConnection;
use sqlx::Connection;
use tokio::sync::Mutex;

use crate::postgres_error;

/// Session-scoped single-writer authority for one runtime namespace.
///
/// This connection is deliberately separate from the query pool. PostgreSQL
/// releases the advisory lock when the dedicated session closes, including
/// process crashes and ungraceful disconnects. Because that release is silent
/// from the holder's side, the runtime calls [`PostgresWriterLock::verify`]
/// on a timer and stops when the lock is gone.
pub struct PostgresWriterLock {
    connection: Mutex<Option<PgConnection>>,
    namespace: Arc<str>,
}

impl PostgresWriterLock {
    pub async fn acquire(database_url: &str, namespace: impl Into<String>) -> EngineResult<Self> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "PostgreSQL writer-lock namespace must not be empty".to_string(),
            ));
        }
        let mut connection = PgConnection::connect(database_url)
            .await
            .map_err(|error| postgres_error("connect writer lock", error))?;
        let acquired: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
                .bind(&namespace)
                .fetch_one(&mut connection)
                .await
                .map_err(|error| postgres_error("acquire writer lock", error))?;
        if !acquired {
            connection
                .close()
                .await
                .map_err(|error| postgres_error("close rejected writer lock connection", error))?;
            return Err(EngineError::Storage(format!(
                "PostgreSQL runtime namespace {namespace:?} already has an active writer"
            )));
        }
        Ok(Self {
            connection: Mutex::new(Some(connection)),
            namespace: Arc::from(namespace),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Confirm that this lock's session is still open and still holds the
    /// advisory lock. An error means another runtime may now hold it, so the
    /// caller must stop writing.
    pub async fn verify(&self) -> EngineResult<()> {
        let mut guard = self.connection.lock().await;
        let Some(connection) = guard.as_mut() else {
            return Err(EngineError::Storage(format!(
                "PostgreSQL writer lock for {:?} was already released",
                self.namespace()
            )));
        };
        let held: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) \
             AND pg_advisory_unlock(hashtextextended($1, 0))",
        )
        .bind(self.namespace())
        .fetch_one(connection)
        .await
        .map_err(|error| postgres_error("verify writer lock", error))?;
        if !held {
            return Err(EngineError::Storage(format!(
                "PostgreSQL writer lock for {:?} is no longer held by this runtime",
                self.namespace()
            )));
        }
        Ok(())
    }

    /// Backend process ID of the session holding the lock, for diagnostics
    /// and for tests that end that session.
    pub async fn session_pid(&self) -> EngineResult<i32> {
        let mut guard = self.connection.lock().await;
        let connection = guard.as_mut().ok_or_else(|| {
            EngineError::Storage("PostgreSQL writer lock was already released".to_string())
        })?;
        sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(connection)
            .await
            .map_err(|error| postgres_error("read writer lock session", error))
    }

    /// Release authority explicitly during graceful shutdown.
    pub async fn release(&self) -> EngineResult<()> {
        let Some(mut connection) = self.connection.lock().await.take() else {
            return Ok(());
        };
        let released: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
                .bind(self.namespace())
                .fetch_one(&mut connection)
                .await
                .map_err(|error| postgres_error("release writer lock", error))?;
        if !released {
            return Err(EngineError::Storage(format!(
                "PostgreSQL writer lock for {:?} was not held by its dedicated session",
                self.namespace()
            )));
        }
        connection
            .close()
            .await
            .map_err(|error| postgres_error("close writer lock connection", error))
    }
}
