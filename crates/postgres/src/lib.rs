//! PostgreSQL-backed journal and materialized state.

mod journal;
mod lock;
mod store;

use agnt5_core::JournalError;
use sqlx::{postgres::PgPoolOptions, PgPool};

pub use journal::PostgresSegment;
pub use lock::RuntimeLock;
pub use store::PostgresMaterializedStore;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl PostgresConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections: 10,
        }
    }
}

pub async fn connect(config: &PostgresConfig) -> Result<PgPool, JournalError> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .map_err(storage_error)
}

pub async fn migrate(pool: &PgPool) -> Result<(), JournalError> {
    sqlx::migrate!("../../migrations")
        .run(pool)
        .await
        .map_err(storage_error)
}

fn storage_error(error: impl std::fmt::Display) -> JournalError {
    JournalError::Storage(error.to_string())
}
