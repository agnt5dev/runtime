#![forbid(unsafe_code)]

//! SQLite and Turso storage profile for the single-active OSS runtime.
//!
//! The adapter uses the same backend-neutral journal and projection contracts
//! as the managed and PostgreSQL profiles. Local files and remote Turso
//! databases therefore persist the same logical V1 records, not PostgreSQL or
//! RocksDB physical layouts.

mod db;
mod demand_index;
mod journal;
mod projection;
mod query;
mod work;
mod writer_lease;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{StorageCapabilities, StorageCapability};
use libsql::{Builder, Connection, Database};

pub use journal::LibsqlJournal;
pub use writer_lease::LibsqlWriterLease;

const MIGRATIONS: &[(&str, &str)] = &[
    (
        "0001_runtime_foundation",
        include_str!("../migrations/0001_runtime_foundation.sql"),
    ),
    (
        "0002_runtime_state",
        include_str!("../migrations/0002_runtime_state.sql"),
    ),
    (
        "0003_writer_authority",
        include_str!("../migrations/0003_writer_authority.sql"),
    ),
    (
        "0004_activation_state_versions",
        include_str!("../migrations/0004_activation_state_versions.sql"),
    ),
    (
        "0005_terminal_obligations",
        include_str!("../migrations/0005_terminal_obligations.sql"),
    ),
    (
        "0006_activation_authority",
        include_str!("../migrations/0006_activation_authority.sql"),
    ),
    (
        "0007_run_leases",
        include_str!("../migrations/0007_run_leases.sql"),
    ),
    (
        "0008_pending_demand",
        include_str!("../migrations/0008_pending_demand.sql"),
    ),
    (
        "0009_pending_demand_state",
        include_str!("../migrations/0009_pending_demand_state.sql"),
    ),
];

#[derive(Clone, Debug)]
pub enum LibsqlDatabase {
    Local { path: PathBuf },
    Remote { url: String, auth_token: String },
}

impl LibsqlDatabase {
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local { path: path.into() }
    }

    pub fn remote(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self::Remote {
            url: url.into(),
            auth_token: auth_token.into(),
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Local { .. } => "sqlite",
            Self::Remote { .. } => "turso",
        }
    }
}

#[derive(Clone)]
pub struct LibsqlStorage {
    database: Arc<Database>,
    connection: Connection,
    namespace: Arc<str>,
    backend: &'static str,
}

impl LibsqlStorage {
    pub async fn connect(
        database: LibsqlDatabase,
        namespace: impl Into<String>,
    ) -> EngineResult<Self> {
        let is_local = matches!(database, LibsqlDatabase::Local { .. });
        let backend = database.kind();
        let database = match database {
            LibsqlDatabase::Local { path } => Builder::new_local(path)
                .build()
                .await
                .map_err(|error| libsql_error("open local database", error))?,
            LibsqlDatabase::Remote { url, auth_token } => Builder::new_remote(url, auth_token)
                .build()
                .await
                .map_err(|error| libsql_error("connect remote database", error))?,
        };
        let database = Arc::new(database);
        let connection = database
            .connect()
            .map_err(|error| libsql_error("open connection", error))?;
        if is_local {
            connection
                .busy_timeout(Duration::from_secs(10))
                .map_err(|error| libsql_error("configure busy timeout", error))?;
            connection
                .execute_batch("PRAGMA journal_mode = WAL;")
                .await
                .map_err(|error| libsql_error("configure local SQLite", error))?;
        }
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .await
            .map_err(|error| libsql_error("enable foreign keys", error))?;
        Self::migrate(&connection).await?;
        Self::from_parts(database, connection, namespace, backend)
    }

    fn from_parts(
        database: Arc<Database>,
        connection: Connection,
        namespace: impl Into<String>,
        backend: &'static str,
    ) -> EngineResult<Self> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() {
            return Err(EngineError::InvalidArgument(
                "libSQL runtime namespace must not be empty".to_string(),
            ));
        }
        Ok(Self {
            database,
            connection,
            namespace: Arc::from(namespace),
            backend,
        })
    }

    pub async fn migrate(connection: &Connection) -> EngineResult<()> {
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS agnt5_runtime_migrations (\
                 version TEXT PRIMARY KEY NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP\
                 );",
            )
            .await
            .map_err(|error| libsql_error("create migration ledger", error))?;
        for (version, sql) in MIGRATIONS {
            let mut rows = connection
                .query(
                    "SELECT 1 FROM agnt5_runtime_migrations WHERE version = ?1",
                    [*version],
                )
                .await
                .map_err(|error| libsql_error("read migration ledger", error))?;
            if rows
                .next()
                .await
                .map_err(|error| libsql_error("read migration row", error))?
                .is_some()
            {
                continue;
            }
            let batch = format!(
                "{sql}\nINSERT OR IGNORE INTO agnt5_runtime_migrations (version) \
                 VALUES ('{version}');"
            );
            connection
                .execute_transactional_batch(&batch)
                .await
                .map_err(|error| libsql_error(&format!("apply migration {version}"), error))?;
        }
        Ok(())
    }

    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn backend(&self) -> &'static str {
        self.backend
    }

    pub fn journal(&self, partition_id: u32) -> LibsqlJournal {
        LibsqlJournal::new(self.clone(), partition_id)
    }

    /// Reuse the same database with an isolated logical namespace. This is
    /// primarily useful for conformance tests and multi-tenant embeddings.
    pub fn with_namespace(&self, namespace: impl Into<String>) -> EngineResult<Self> {
        Self::from_parts(
            self.database.clone(),
            self.connection.clone(),
            namespace,
            self.backend,
        )
    }

    pub fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::none()
            .with(StorageCapability::JournalRetention)
            .with(StorageCapability::JournalEpochTransition)
    }

    pub(crate) async fn new_connection(&self) -> EngineResult<Connection> {
        let connection = self
            .database
            .connect()
            .map_err(|error| libsql_error("open dedicated connection", error))?;
        if self.backend == "sqlite" {
            connection
                .busy_timeout(Duration::from_secs(10))
                .map_err(|error| libsql_error("configure dedicated busy timeout", error))?;
        }
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .await
            .map_err(|error| libsql_error("enable dedicated foreign keys", error))?;
        Ok(connection)
    }

    pub async fn check_readiness(&self) -> EngineResult<()> {
        let mut rows = self
            .connection
            .query("SELECT 1", ())
            .await
            .map_err(|error| libsql_error("readiness query", error))?;
        rows.next()
            .await
            .map_err(|error| libsql_error("read readiness row", error))?
            .ok_or_else(|| EngineError::Storage("libSQL readiness query returned no row".into()))?;
        Ok(())
    }

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
            "activation_state_versions",
            "journal_idempotency",
            "journal_records",
            "journal_partitions",
        ];
        let transaction = self
            .new_connection()
            .await?
            .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
            .await
            .map_err(|error| libsql_error("begin namespace delete", error))?;
        for table in tables {
            let statement = format!("DELETE FROM agnt5_runtime_{table} WHERE namespace = ?1");
            db::query(statement)
                .bind(self.namespace())
                .execute(&transaction)
                .await
                .map_err(|error| libsql_error("delete namespace table", error))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| libsql_error("commit namespace delete", error))?;
        Ok(())
    }
}

pub(crate) fn libsql_error(context: &str, error: impl std::fmt::Display) -> EngineError {
    EngineError::Storage(format!("libsql {context}: {error}"))
}

pub(crate) fn encode_offset(offset: u64) -> EngineResult<i64> {
    i64::try_from(offset).map_err(|_| {
        EngineError::Storage(format!("runtime offset {offset} exceeds SQLite INTEGER"))
    })
}

pub(crate) fn decode_offset(offset: i64) -> EngineResult<u64> {
    u64::try_from(offset).map_err(|_| {
        EngineError::Storage(format!(
            "SQLite runtime offset must be non-negative, got {offset}"
        ))
    })
}
