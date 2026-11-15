#![forbid(unsafe_code)]

//! PostgreSQL storage profile for the single-active OSS runtime.
//!
//! The adapter owns SQL schema and transaction details. Runtime processors
//! continue to operate on backend-neutral journal and projection contracts.

mod demand_index;
mod journal;
mod projection;
mod query;
mod work;
mod writer_lock;

use std::sync::Arc;
use std::time::Duration;

use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{StorageCapabilities, StorageCapability};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub use journal::PostgresJournal;
pub use writer_lock::PostgresWriterLock;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Shared PostgreSQL profile state.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: PgPool,
    namespace: Arc<str>,
}

impl PostgresStorage {
    /// Connect, run embedded migrations, and create one logical runtime
    /// namespace.
    pub async fn connect(
        database_url: &str,
        namespace: impl Into<String>,
        max_connections: u32,
    ) -> EngineResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await
            .map_err(|error| postgres_error("connect", error))?;
        Self::migrate(&pool).await?;
        Self::from_pool(pool, namespace)
    }

    /// Construct over an already-migrated pool.
    pub fn from_pool(pool: PgPool, namespace: impl Into<String>) -> EngineResult<Self> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "PostgreSQL runtime namespace must not be empty".to_string(),
            ));
        }
        Ok(Self {
            pool,
            namespace: Arc::from(namespace),
        })
    }

    /// Apply the embedded, forward-only runtime schema.
    pub async fn migrate(pool: &PgPool) -> EngineResult<()> {
        MIGRATOR
            .run(pool)
            .await
            .map_err(|error| EngineError::Storage(format!("postgres migrate: {error}")))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn journal(&self, partition_id: u32) -> PostgresJournal {
        PostgresJournal::new(self.clone(), partition_id)
    }

    /// Optional behavior genuinely implemented by this profile.
    pub fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::none()
            .with(StorageCapability::JournalRetention)
            .with(StorageCapability::JournalEpochTransition)
    }

    /// Remove one logical runtime namespace. Intended for integration-test
    /// isolation and explicit administrative teardown.
    pub async fn delete_namespace(&self) -> EngineResult<()> {
        let tables = [
            "inbox",
            "event_hot_index",
            "run_hot_index",
            "dispatch_outbox",
            "leases",
            "queue_counters",
            "pending_deployment_priority",
            "pending_demand",
            "derived_index_state",
            "pending_priority",
            "pending_runs",
            "messages",
            "sessions",
            "batches",
            "jobs",
            "timers",
            "signals",
            "approvals",
            "entity_states",
            "events",
            "step_memos",
            "terminal_obligations",
            "runs",
            "projection_checkpoints",
            "journal_partitions",
        ];
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| postgres_error("begin namespace delete", error))?;
        for table in tables {
            let statement = format!("DELETE FROM agnt5_runtime.{table} WHERE namespace = $1");
            // `table` comes only from the static allowlist above; namespace
            // remains a bind parameter.
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .bind(self.namespace())
                .execute(&mut *transaction)
                .await
                .map_err(|error| postgres_error("delete namespace table", error))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| postgres_error("commit namespace delete", error))?;
        Ok(())
    }
}

pub(crate) fn postgres_error(context: &str, error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(format!("postgres {context}: {error}"))
}

pub(crate) fn encode_offset(offset: u64) -> EngineResult<i64> {
    i64::try_from(offset).map_err(|_| {
        EngineError::Storage(format!("runtime offset {offset} exceeds PostgreSQL BIGINT"))
    })
}

pub(crate) fn decode_offset(offset: i64) -> EngineResult<u64> {
    u64::try_from(offset).map_err(|_| {
        EngineError::Storage(format!(
            "PostgreSQL runtime offset must be non-negative, got {offset}"
        ))
    })
}
