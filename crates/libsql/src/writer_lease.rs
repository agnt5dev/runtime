use std::sync::Arc;
use std::time::Duration;

use engine_core::error::{EngineError, EngineResult};
use libsql::{Connection, TransactionBehavior};

use crate::db::query;
use crate::{libsql_error, LibsqlStorage};

/// Namespace-scoped single-active authority for SQLite and Turso profiles.
///
/// Unlike a PostgreSQL advisory lock this is a renewable database lease. The
/// runtime must terminate if renewal fails, and a replacement must wait for
/// the previous lease to expire before taking authority.
pub struct LibsqlWriterLease {
    connection: Connection,
    namespace: Arc<str>,
    owner_id: Arc<str>,
    generation: i64,
    lease_duration: Duration,
}

impl LibsqlWriterLease {
    pub async fn acquire(
        storage: &LibsqlStorage,
        owner_id: impl Into<String>,
        lease_duration: Duration,
    ) -> EngineResult<Self> {
        if lease_duration.is_zero() {
            return Err(EngineError::InvalidArgument(
                "libSQL writer lease duration must be greater than zero".to_string(),
            ));
        }
        let owner_id = owner_id.into();
        if owner_id.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "libSQL writer lease owner must not be empty".to_string(),
            ));
        }
        let connection = storage.new_connection().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(|error| libsql_error("begin writer lease acquisition", error))?;
        let now_ms = database_time_ms(&transaction).await?;
        let existing = query(
            "SELECT owner_id, lease_expires_at_ms, generation \
             FROM agnt5_runtime_writer_authority WHERE namespace = ?1",
        )
        .bind(storage.namespace())
        .fetch_optional(&transaction)
        .await
        .map_err(|error| libsql_error("read writer authority", error))?;
        let generation = if let Some(row) = existing {
            let existing_owner: String = row
                .try_get("owner_id")
                .map_err(|error| libsql_error("decode writer owner", error))?;
            let expires_at_ms: i64 = row
                .try_get("lease_expires_at_ms")
                .map_err(|error| libsql_error("decode writer lease expiry", error))?;
            let generation: i64 = row
                .try_get("generation")
                .map_err(|error| libsql_error("decode writer generation", error))?;
            if expires_at_ms > now_ms {
                transaction
                    .rollback()
                    .await
                    .map_err(|error| libsql_error("rollback rejected writer lease", error))?;
                return Err(EngineError::Storage(format!(
                    "libsql writer authority for namespace {:?} is held by {:?} until {}",
                    storage.namespace(),
                    existing_owner,
                    expires_at_ms
                )));
            }
            generation.checked_add(1).ok_or_else(|| {
                EngineError::Storage("libSQL writer generation overflow".to_string())
            })?
        } else {
            1
        };
        let expires_at_ms = lease_expiry(now_ms, lease_duration)?;
        query(
            "INSERT INTO agnt5_runtime_writer_authority \
                 (namespace, owner_id, lease_expires_at_ms, generation) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(namespace) DO UPDATE SET \
                 owner_id = excluded.owner_id, \
                 lease_expires_at_ms = excluded.lease_expires_at_ms, \
                 generation = excluded.generation",
        )
        .bind(storage.namespace())
        .bind(owner_id.as_str())
        .bind(expires_at_ms)
        .bind(generation)
        .execute(&transaction)
        .await
        .map_err(|error| libsql_error("claim writer authority", error))?;
        transaction
            .commit()
            .await
            .map_err(|error| libsql_error("commit writer lease acquisition", error))?;

        Ok(Self {
            connection,
            namespace: Arc::from(storage.namespace()),
            owner_id: Arc::from(owner_id),
            generation,
            lease_duration,
        })
    }

    pub fn generation(&self) -> i64 {
        self.generation
    }

    pub async fn renew(&self) -> EngineResult<()> {
        let duration_ms = duration_ms(self.lease_duration)?;
        let result = query(
            "UPDATE agnt5_runtime_writer_authority \
             SET lease_expires_at_ms = \
                 CAST(unixepoch('subsec') * 1000 AS INTEGER) + ?1 \
             WHERE namespace = ?2 AND owner_id = ?3 AND generation = ?4 \
               AND lease_expires_at_ms > \
                   CAST(unixepoch('subsec') * 1000 AS INTEGER)",
        )
        .bind(duration_ms)
        .bind(self.namespace.as_ref())
        .bind(self.owner_id.as_ref())
        .bind(self.generation)
        .execute(&self.connection)
        .await
        .map_err(|error| libsql_error("renew writer lease", error))?;
        if result.rows_affected() != 1 {
            return Err(EngineError::Storage(format!(
                "libsql writer authority for namespace {:?} was lost",
                self.namespace
            )));
        }
        Ok(())
    }

    pub async fn release(&self) -> EngineResult<()> {
        query(
            "DELETE FROM agnt5_runtime_writer_authority \
             WHERE namespace = ?1 AND owner_id = ?2 AND generation = ?3",
        )
        .bind(self.namespace.as_ref())
        .bind(self.owner_id.as_ref())
        .bind(self.generation)
        .execute(&self.connection)
        .await
        .map_err(|error| libsql_error("release writer lease", error))?;
        Ok(())
    }
}

async fn database_time_ms(connection: &Connection) -> EngineResult<i64> {
    let row = query("SELECT CAST(unixepoch('subsec') * 1000 AS INTEGER) AS database_time_ms")
        .fetch_one(connection)
        .await
        .map_err(|error| libsql_error("read database time", error))?;
    row.try_get("database_time_ms")
        .map_err(|error| libsql_error("decode database time", error))
}

fn lease_expiry(now_ms: i64, duration: Duration) -> EngineResult<i64> {
    let duration_ms = duration_ms(duration)?;
    now_ms
        .checked_add(duration_ms)
        .ok_or_else(|| EngineError::Storage("libSQL writer lease expiry overflow".to_string()))
}

fn duration_ms(duration: Duration) -> EngineResult<i64> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        EngineError::InvalidArgument("libSQL writer lease duration is too large".to_string())
    })
}
