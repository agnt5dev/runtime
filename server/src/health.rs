use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use engine_core::traits::Segment;
use engine_libsql::LibsqlStorage;
use engine_postgres::PostgresStorage;

#[async_trait]
pub trait StorageHealth: Send + Sync {
    async fn check_readiness(&self) -> bool;
    fn backend(&self) -> &'static str;
    fn connection_count(&self) -> u32;
    fn idle_connection_count(&self) -> usize;
}

#[async_trait]
impl StorageHealth for PostgresStorage {
    async fn check_readiness(&self) -> bool {
        self.pool().acquire().await.is_ok()
    }

    fn backend(&self) -> &'static str {
        "postgres"
    }

    fn connection_count(&self) -> u32 {
        self.pool().size()
    }

    fn idle_connection_count(&self) -> usize {
        self.pool().num_idle()
    }
}

#[async_trait]
impl StorageHealth for LibsqlStorage {
    async fn check_readiness(&self) -> bool {
        LibsqlStorage::check_readiness(self).await.is_ok()
    }

    fn backend(&self) -> &'static str {
        LibsqlStorage::backend(self)
    }

    fn connection_count(&self) -> u32 {
        1
    }

    fn idle_connection_count(&self) -> usize {
        0
    }
}

pub struct RuntimeHealth {
    ready: AtomicBool,
    processor_alive: AtomicBool,
    started_at: Instant,
    storage: Arc<dyn StorageHealth>,
    journal: Arc<dyn Segment>,
}

impl RuntimeHealth {
    pub fn new(storage: Arc<dyn StorageHealth>, journal: Arc<dyn Segment>) -> Self {
        Self {
            ready: AtomicBool::new(false),
            processor_alive: AtomicBool::new(false),
            started_at: Instant::now(),
            storage,
            journal,
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn set_processor_alive(&self, alive: bool) {
        self.processor_alive.store(alive, Ordering::Release);
        if !alive {
            self.set_ready(false);
        }
    }

    pub fn processor_alive(&self) -> bool {
        self.processor_alive.load(Ordering::Acquire)
    }

    pub async fn is_ready(&self) -> bool {
        if !self.ready.load(Ordering::Acquire) || !self.processor_alive() {
            return false;
        }
        self.journal.check_readiness().await.is_ok() && self.storage.check_readiness().await
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn backend(&self) -> &'static str {
        self.storage.backend()
    }

    pub fn connection_count(&self) -> u32 {
        self.storage.connection_count()
    }

    pub fn idle_connection_count(&self) -> usize {
        self.storage.idle_connection_count()
    }
}
