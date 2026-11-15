//! An in-memory dispatch outbox, run table and timer index for the
//! processor's own tests.
//!
//! The dispatch outbox drainer and the timer poller reach storage only
//! through the `DispatchOutboxStore` and `TimerWorkStore` ports. This store
//! implements both over plain maps, so their tests run without a storage
//! engine. It keeps the ordering the ports promise: outbox rows scan by
//! `(queued_offset, project_id, run_id)` within a partition, and due timers by
//! fire time.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use async_trait::async_trait;
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::{
    DispatchOutboxCursor, DispatchOutboxEntry, DispatchOutboxScanEntry, DispatchOutboxStore,
    OutboxEntryToken, TimerWorkStore,
};
use engine_core::types::Record;

use crate::projections::StoreOp;

/// `(partition_id, queued_offset, project_id, run_id)`: the scan order.
type OutboxKey = (u32, u64, String, String);

#[derive(Default)]
struct State {
    outbox: BTreeMap<OutboxKey, Vec<u8>>,
    runs: HashMap<(String, String), Vec<u8>>,
    /// Scheduled timers by `(fire_at_ns, project_id, timer_id)`.
    due_timers: BTreeMap<(i64, String, String), Vec<u8>>,
}

#[derive(Default)]
pub(crate) struct MemoryWorkStore {
    state: Mutex<State>,
}

impl MemoryWorkStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The token this store gives the outbox row with this identity.
    pub(crate) fn token(
        partition_id: u32,
        project_id: &str,
        queued_offset: u64,
        run_id: &str,
    ) -> OutboxEntryToken {
        OutboxEntryToken::from_adapter(
            format!("{partition_id}\0{queued_offset}\0{project_id}\0{run_id}").into_bytes(),
        )
    }

    fn key_of(token: &OutboxEntryToken) -> EngineResult<OutboxKey> {
        let raw = std::str::from_utf8(token.adapter_bytes())
            .map_err(|_| EngineError::InvalidArgument("outbox token is not UTF-8".to_string()))?;
        let mut parts = raw.split('\0');
        let mut next = || {
            parts.next().ok_or_else(|| {
                EngineError::InvalidArgument(format!("malformed outbox token {raw:?}"))
            })
        };
        let partition_id = next()?
            .parse()
            .map_err(|_| EngineError::InvalidArgument(format!("bad partition in {raw:?}")))?;
        let queued_offset = next()?
            .parse()
            .map_err(|_| EngineError::InvalidArgument(format!("bad offset in {raw:?}")))?;
        let project_id = next()?.to_string();
        let run_id = next()?.to_string();
        Ok((partition_id, queued_offset, project_id, run_id))
    }

    pub(crate) fn put_dispatch_intent(
        &self,
        partition_id: u32,
        project_id: &str,
        queued_offset: u64,
        run_id: &str,
        payload: Vec<u8>,
    ) {
        self.state.lock().unwrap().outbox.insert(
            (
                partition_id,
                queued_offset,
                project_id.to_string(),
                run_id.to_string(),
            ),
            payload,
        );
    }

    pub(crate) fn put_run(&self, project_id: &str, run_id: &str, run: Vec<u8>) {
        self.state
            .lock()
            .unwrap()
            .runs
            .insert((project_id.to_string(), run_id.to_string()), run);
    }

    pub(crate) fn run(&self, project_id: &str, run_id: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .unwrap()
            .runs
            .get(&(project_id.to_string(), run_id.to_string()))
            .cloned()
    }

    /// Every outbox row in a partition, in scan order.
    pub(crate) fn dispatch_intents(&self, partition_id: u32) -> Vec<DispatchOutboxEntry> {
        self.state
            .lock()
            .unwrap()
            .outbox
            .iter()
            .filter(|((partition, ..), _)| *partition == partition_id)
            .map(
                |((partition, offset, project, run), payload)| DispatchOutboxEntry {
                    token: Self::token(*partition, project, *offset, run),
                    partition_id: *partition,
                    project_id: project.clone(),
                    queued_offset: *offset,
                    run_id: run.clone(),
                    payload: payload.clone(),
                },
            )
            .collect()
    }

    /// Index a timer as the projection would. Only a `scheduled` timer is in
    /// the due index; `timer` is the stored `Timer`, which may say otherwise.
    pub(crate) fn put_timer(
        &self,
        project_id: &str,
        timer_id: &str,
        indexed_status: &str,
        fire_at_ns: i64,
        timer: Vec<u8>,
    ) {
        if indexed_status == "scheduled" {
            self.state.lock().unwrap().due_timers.insert(
                (fire_at_ns, project_id.to_string(), timer_id.to_string()),
                timer,
            );
        }
    }

    /// Apply the run and outbox writes of one projected record. Other
    /// operations have no place in this store and are ignored.
    pub(crate) fn apply(&self, record: &Record, ops: &[StoreOp]) {
        for op in ops {
            match op {
                StoreOp::PutRun(data) => {
                    self.put_run(&record.project_id, &record.run_id, data.clone())
                }
                StoreOp::PutDispatchOutbox {
                    partition_id,
                    queued_offset,
                    request_data,
                } => self.put_dispatch_intent(
                    *partition_id,
                    &record.project_id,
                    *queued_offset,
                    &record.run_id,
                    request_data.clone(),
                ),
                StoreOp::DeleteDispatchOutbox {
                    partition_id,
                    project_id,
                    queued_offset,
                    run_id,
                } => {
                    self.state.lock().unwrap().outbox.remove(&(
                        *partition_id,
                        *queued_offset,
                        project_id.clone(),
                        run_id.clone(),
                    ));
                }
                _ => {}
            }
        }
    }
}

#[async_trait]
impl DispatchOutboxStore for MemoryWorkStore {
    async fn scan_dispatch_intents(
        &self,
        partition_id: u32,
        cursor: Option<&DispatchOutboxCursor>,
        limit: usize,
    ) -> EngineResult<Vec<DispatchOutboxScanEntry>> {
        let after = cursor.and_then(|cursor| {
            cursor
                .position()
                .map(|(offset, project, run)| (offset, project.to_string(), run.to_string()))
        });
        Ok(self
            .dispatch_intents(partition_id)
            .into_iter()
            .filter(|entry| match &after {
                Some(after) => {
                    (
                        entry.queued_offset,
                        entry.project_id.clone(),
                        entry.run_id.clone(),
                    ) > *after
                }
                None => true,
            })
            .take(limit)
            .map(DispatchOutboxScanEntry::Ready)
            .collect())
    }

    async fn delete_dispatch_intent(&self, token: &OutboxEntryToken) -> EngineResult<()> {
        let key = Self::key_of(token)?;
        self.state.lock().unwrap().outbox.remove(&key);
        Ok(())
    }

    async fn load_dispatch_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(self.run(project_id, run_id))
    }

    /// The route-aware pending index has no reader in these tests, so the
    /// drainer's request to keep a run in it is accepted and dropped.
    async fn ensure_pending_deployment_run(
        &self,
        _project_id: &str,
        _deployment_id: &str,
        _priority: u8,
        _ready_at_ms: i64,
        _created_at_ms: i64,
        _run_id: &str,
    ) -> EngineResult<()> {
        Ok(())
    }
}

#[async_trait]
impl TimerWorkStore for MemoryWorkStore {
    async fn scan_due_timers(&self, now_ns: i64, limit: usize) -> EngineResult<Vec<Vec<u8>>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .due_timers
            .iter()
            .filter(|((fire_at_ns, ..), _)| *fire_at_ns <= now_ns)
            .take(limit)
            .map(|(_, timer)| timer.clone())
            .collect())
    }
}
