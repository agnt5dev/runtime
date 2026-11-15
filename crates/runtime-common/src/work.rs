//! Bounded, partition-owned execution of journal-backed controller work.
//! Handlers reconcile idempotent effects; this runner owns claim durability,
//! fencing, cancellation on ownership loss, and atomic progress/fanout writes.
use crate::backend::EngineBackend;
use engine_core::{runtime_work::RuntimeWork, traits::MaterializedStore};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const PAGE_SIZE: usize = 64;
const MAX_IN_FLIGHT: usize = 16;
const CLAIM_MS: i64 = 30_000;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);

/// Returns the current serving ownership epoch. Election alone is insufficient:
/// recovery/admission must also have completed before returning Some(epoch).
pub trait WorkAuthority: Send + Sync {
    fn partition_count(&self) -> u32;
    fn serving_epoch(&self, partition_id: u32) -> Option<u64>;
}

pub struct WorkOutcome {
    pub payload: serde_json::Value,
    pub due_at_ms: i64,
    pub terminal: bool,
    /// Children must share the parent's journal partition and run, so fanout
    /// and parent progress can commit in one atomic journal batch.
    pub children: Vec<RuntimeWork>,
}

#[async_trait::async_trait]
pub trait WorkHandler: Send + Sync {
    fn accepts(&self, work: &RuntimeWork) -> bool;
    async fn reconcile(&self, claimed: &RuntimeWork) -> Result<WorkOutcome, String>;
}

#[derive(Clone)]
pub struct WorkRunner {
    pub engine: Arc<dyn EngineBackend>,
    pub store: Arc<dyn MaterializedStore>,
    pub authority: Arc<dyn WorkAuthority>,
    pub handler: Arc<dyn WorkHandler>,
    pub owner: String,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

impl WorkRunner {
    pub async fn run(self, cancel: CancellationToken) {
        let partitions = self.authority.partition_count();
        let mut cursors = vec![None; partitions as usize];
        let mut active = JoinSet::new();
        let mut task_keys = HashMap::new();
        let mut in_flight = HashSet::new();
        let mut next_partition = 0;
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = active.join_next_with_id(), if !active.is_empty() => {
                    let id = match result {
                        Some(Ok((id, ()))) => Some(id),
                        Some(Err(error)) => { tracing::error!(%error, "runtime work task failed"); Some(error.id()) }
                        None => None,
                    };
                    if let Some(key) = id.and_then(|id| task_keys.remove(&id)) { in_flight.remove(&key); }
                }
                _ = ticker.tick() => {
                    for offset in 0..partitions {
                        let partition = (next_partition + offset) % partitions;
                        let Some(epoch) = self.authority.serving_epoch(partition) else {
                            cursors[partition as usize] = None;
                            continue;
                        };
                        if active.len() >= MAX_IN_FLIGHT { break; }
                        let page = match self.store.scan_runtime_work(partition, cursors[partition as usize].as_deref(), PAGE_SIZE).await {
                            Ok(page) => page,
                            Err(error) => { tracing::error!(partition, %error, "runtime work scan failed"); continue; }
                        };
                        // Visit a bounded page even when its first entries are
                        // delayed or owned. Later ready work must not starve.
                        for work in page.items {
                            if active.len() >= MAX_IN_FLIGHT { break; }
                            let key = (work.partition_id, work.project_id.clone(), work.work_id.clone());
                            if self.handler.accepts(&work) && work.ready(now_ms()) && in_flight.insert(key.clone()) {
                                let runner = self.clone();
                                let cancel = cancel.clone();
                                let task = active.spawn(async move { runner.attempt(work, epoch, cancel).await; });
                                task_keys.insert(task.id(), key);
                            }
                        }
                        cursors[partition as usize] = page.next_cursor;
                    }
                    if partitions > 0 { next_partition = (next_partition + 1) % partitions; }
                }
            }
        }
        active.abort_all();
        while active.join_next().await.is_some() {}
    }

    async fn attempt(&self, work: RuntimeWork, epoch: u64, cancel: CancellationToken) {
        if self.authority.serving_epoch(work.partition_id) != Some(epoch) {
            return;
        }
        let claimed = match work.claim(&self.owner, now_ms(), CLAIM_MS) {
            Ok(claimed) => claimed,
            Err(_) => return,
        };
        let record = match claimed.to_record() {
            Ok(record) => record,
            Err(_) => return,
        };
        // An error can be an ambiguous acknowledgement. Never begin effects
        // on that basis; the committed claim is recovered from the projection.
        if self.engine.append(record).await.is_err() {
            return;
        }
        let lost_authority = async {
            loop {
                if self.authority.serving_epoch(work.partition_id) != Some(epoch)
                    || !claimed.held_by(&self.owner, now_ms())
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = lost_authority => return,
            result = tokio::time::timeout(ATTEMPT_TIMEOUT, self.handler.reconcile(&claimed)) => match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => {
                    tracing::warn!(work_id = %work.work_id, %error, "runtime work reconciliation deferred");
                    WorkOutcome { payload: work.payload.clone(), due_at_ms: now_ms() + 5_000, terminal:false, children:vec![] }
                }
                Err(_) => WorkOutcome { payload:work.payload.clone(), due_at_ms:now_ms() + 1_000, terminal:false, children:vec![] },
            }
        };
        if self.authority.serving_epoch(work.partition_id) != Some(epoch) {
            return;
        }
        let next = match claimed.advance(
            &self.owner,
            now_ms(),
            outcome.payload,
            outcome.due_at_ms,
            outcome.terminal,
        ) {
            Ok(next) => next,
            Err(_) => return,
        };
        let mut records = match next.to_record() {
            Ok(record) => vec![record],
            Err(_) => return,
        };
        for child in outcome.children {
            if child.partition_id != work.partition_id
                || child.project_id != work.project_id
                || child.run_id != work.run_id
                || child.version != 1
                || child.lease.is_some()
            {
                tracing::error!(work_id = %work.work_id, "runtime work fanout crossed identity or partition");
                return;
            }
            match child.to_record() {
                Ok(record) => records.push(record),
                Err(error) => {
                    tracing::error!(%error, "invalid runtime work child");
                    return;
                }
            }
        }
        // Same-partition append_batch is atomic in the commissioned profile.
        // Partial/unknown outcomes never authorize further effects.
        let expected = records.len();
        match self.engine.append_batch_with_outcome(records).await {
            Ok(outcome) if outcome.offsets.len() == expected => {}
            Ok(_) => {
                tracing::error!(work_id = %work.work_id, "runtime work progress returned an incomplete acknowledgement")
            }
            Err(error) => {
                tracing::warn!(work_id = %work.work_id, %error, "runtime work progress not acknowledged")
            }
        }
    }
}
