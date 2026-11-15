//! Timer background poller.
//!
//! Scans the materialized store for due timers and fires them by
//! appending `timer.fired` events to the journal segment. The processor
//! picks these up and updates the materialized store.
//!
//! This preserves the single-writer model: the poller only reads from
//! the store and writes to the segment (journal).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use engine_core::error::EngineResult;
use engine_core::storage_contract::TimerWorkStore;
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_proto::Timer;
use prost::Message;

/// Configuration for the timer poller.
pub struct TimerPollerConfig {
    /// How often to poll for due timers.
    pub poll_interval: Duration,
    /// Maximum timers to fire per poll cycle.
    pub batch_size: usize,
}

impl Default for TimerPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
        }
    }
}

/// Background task that fires due timers by appending events to the journal.
pub struct TimerPoller<S: Segment, M: TimerWorkStore + ?Sized> {
    segment: Arc<S>,
    store: Arc<M>,
    config: TimerPollerConfig,
}

impl<S: Segment, M: TimerWorkStore + ?Sized> TimerPoller<S, M> {
    pub fn new(segment: Arc<S>, store: Arc<M>, config: TimerPollerConfig) -> Self {
        Self {
            segment,
            store,
            config,
        }
    }

    /// Run the poller loop until cancelled.
    pub async fn run(&self, cancel: CancellationToken) -> EngineResult<()> {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis() as u64,
            batch_size = self.config.batch_size,
            "Timer poller starting"
        );

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Timer poller shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    if let Err(e) = self.poll_once().await {
                        error!(error = %e, "Timer poller cycle failed");
                    }
                }
            }
        }
    }

    /// Single poll cycle: find due timers and fire them.
    async fn poll_once(&self) -> EngineResult<()> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let due_timers = self
            .store
            .scan_due_timers(now_ns, self.config.batch_size)
            .await?;

        let mut oldest_fire_at_ns = None;
        for timer_data in &due_timers {
            if let Ok(timer) = Timer::decode(timer_data.as_slice()) {
                if timer.status == "scheduled" {
                    if let Some(fire_at) = timer.fire_at {
                        let fire_at_ns = fire_at
                            .seconds
                            .saturating_mul(1_000_000_000)
                            .saturating_add(i64::from(fire_at.nanos));
                        oldest_fire_at_ns = Some(
                            oldest_fire_at_ns
                                .map_or(fire_at_ns, |oldest: i64| oldest.min(fire_at_ns)),
                        );
                    }
                }
            }
        }
        let oldest_overdue_age_ms = oldest_fire_at_ns
            .map(|fire_at_ns| now_ns.saturating_sub(fire_at_ns).max(0) as u64 / 1_000_000)
            .unwrap_or(0);
        crate::metrics::record_timer_backlog(
            due_timers.len() as u64,
            oldest_overdue_age_ms,
            due_timers.len() >= self.config.batch_size,
        );

        if due_timers.is_empty() {
            return Ok(());
        }

        debug!(count = due_timers.len(), "Found due timers");

        for timer_data in due_timers {
            let timer = match Timer::decode(timer_data.as_slice()) {
                Ok(t) => t,
                Err(e) => {
                    warn!(error = %e, "Failed to decode timer, skipping");
                    continue;
                }
            };

            // Skip if already fired (idempotency — processor may not have caught up yet)
            if timer.status == "fired" {
                continue;
            }

            if let Err(e) = self.fire_timer(&timer, now_ns).await {
                error!(
                    timer_id = %timer.timer_id,
                    error = %e,
                    "Failed to fire timer"
                );
            }
        }

        Ok(())
    }

    /// Fire a single timer by appending a timer.fired event to the journal.
    async fn fire_timer(&self, timer: &Timer, now_ns: i64) -> EngineResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert("timer_id".to_string(), timer.timer_id.clone());
        metadata.insert("timer_key".to_string(), timer.timer_key.clone());
        if !timer.waiting_step.is_empty() {
            metadata.insert("waiting_step".to_string(), timer.waiting_step.clone());
        }
        if !timer.component_id.is_empty() {
            metadata.insert("component_id".to_string(), timer.component_id.clone());
        }

        let payload = serde_json::json!({
            "timer_id": timer.timer_id,
            "timer_key": timer.timer_key,
            "fired_at_ns": now_ns,
        });

        let record = Record {
            offset: Offset::ZERO,
            project_id: timer.project_id.clone(),
            run_id: timer.run_id.clone(),
            event_type: "timer.fired".to_string(),
            data: Bytes::from(serde_json::to_vec(&payload).unwrap_or_default()),
            timestamp_ns: now_ns,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        self.segment
            .append_batch(std::slice::from_ref(&record))
            .await?;

        debug!(timer_id = %timer.timer_id, "Timer fired");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_work_store::MemoryWorkStore;
    use engine_journal_memory::MemorySegment;

    async fn setup() -> (
        Arc<MemorySegment>,
        Arc<MemoryWorkStore>,
        TimerPoller<MemorySegment, MemoryWorkStore>,
    ) {
        let segment = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        let config = TimerPollerConfig {
            poll_interval: Duration::from_millis(50),
            batch_size: 10,
        };
        let poller = TimerPoller::new(segment.clone(), store.clone(), config);
        (segment, store, poller)
    }

    #[tokio::test]
    async fn fires_due_timer() {
        let (segment, store, poller) = setup().await;

        // Manually insert a scheduled timer into the store
        let timer = Timer {
            timer_id: "tmr-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            timer_key: "sleep_step".to_string(),
            waiting_step: "step_2".to_string(),
            status: "scheduled".to_string(),
            fire_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        store.put_timer(
            "t1",
            "tmr-1",
            "scheduled",
            1_000_000_000,
            timer.encode_to_vec(),
        );

        // Poll — timer is due (fire_at = 1s, now >> 1s)
        poller.poll_once().await.unwrap();

        // Verify a timer.fired event was appended to the segment
        let tail = segment.tail_offset().await.unwrap();
        assert_eq!(tail, Offset(1)); // One record appended

        let records = segment.read_range(Offset(0), Offset(1)).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event_type, "timer.fired");
        assert_eq!(records[0].run_id, "r1");
        assert_eq!(records[0].project_id, "t1");
        assert_eq!(records[0].metadata.get("timer_id").unwrap(), "tmr-1");
    }

    #[tokio::test]
    async fn skips_already_fired_timer() {
        let (segment, store, poller) = setup().await;

        // Insert a timer that's already fired but still in due index
        // (race condition where processor hasn't caught up)
        let timer = Timer {
            timer_id: "tmr-1".to_string(),
            run_id: "r1".to_string(),
            project_id: "t1".to_string(),
            status: "fired".to_string(),
            fire_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        // Manually put in scheduled state in due index but fired in primary
        store.put_timer(
            "t1",
            "tmr-1",
            "scheduled",
            1_000_000_000,
            timer.encode_to_vec(),
        );

        poller.poll_once().await.unwrap();

        // Should NOT have appended any records (timer already fired)
        let tail = segment.tail_offset().await.unwrap();
        assert_eq!(tail, Offset(0));
    }

    #[tokio::test]
    async fn no_due_timers_is_noop() {
        let (segment, _store, poller) = setup().await;
        poller.poll_once().await.unwrap();
        let tail = segment.tail_offset().await.unwrap();
        assert_eq!(tail, Offset(0));
    }
}
