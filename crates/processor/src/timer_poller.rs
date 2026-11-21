//! The timer poller: fires each partition's due timers from its leader.
//!
//! Each partition has its own poller, which runs only while its node leads
//! the partition and replay has caught up (ADR-006, ADR-019), gated like the
//! terminal and instance supervisors it shares a task with. It reads its
//! partition's due timers from the materialized store and fires each by
//! appending `timer.fired` to the partition's journal; the processor applies
//! it. Only the poller reads the due index and only the journal writes to it,
//! so the single-writer model holds.
//!
//! A timer belongs to its run's partition. The store keeps a due index per
//! partition, so a backlog on one partition can't delay another's timers.
//! Timers scheduled before that index existed are only in the global one;
//! each leader also drains its share of those, on a slower beat. A store
//! without a per-partition index (the single-partition open-source stores)
//! is read through the global index, filtered to the partition.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use engine_core::error::EngineResult;
use engine_core::routing::partition_for_run;
use engine_core::storage_contract::{InstanceSupervisorStore, TimerWorkStore};
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_proto::Timer;
use prost::Message;

/// Configuration for a partition's timer poller.
#[derive(Debug, Clone, Copy)]
pub struct TimerPollerConfig {
    /// How often to poll for due timers.
    pub poll_interval: Duration,
    /// Maximum timers to fire per poll cycle.
    pub batch_size: usize,
    /// Every how many polls the global index is also read, for timers
    /// scheduled before the per-partition index existed.
    pub legacy_every: u32,
}

impl Default for TimerPollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
            legacy_every: 30,
        }
    }
}

/// What one poll did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TimerPass {
    pub due: usize,
    pub fired: usize,
    /// Follow-ups held because their instance's inbox is full.
    pub held: usize,
}

/// Run one partition's timer poller until cancelled. It fires timers only
/// while this node leads the partition and replay has caught up.
#[allow(clippy::too_many_arguments)]
pub async fn run_partition_timer_poller<M: TimerWorkStore + InstanceSupervisorStore + ?Sized>(
    partition_id: u32,
    node_id: String,
    store: Arc<M>,
    segment: Arc<dyn Segment>,
    replay_ready: watch::Receiver<bool>,
    is_leader: watch::Receiver<bool>,
    cancel: CancellationToken,
    config: TimerPollerConfig,
) {
    let partition_count = crate::instance_supervisor::partition_count(segment.as_ref());
    info!(
        partition_id,
        node_id,
        partition_count,
        poll_interval_ms = config.poll_interval.as_millis() as u64,
        "timer poller starting"
    );
    let mut tick = tokio::time::interval(config.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut polls: u32 = 0;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {}
        }
        if !*replay_ready.borrow() || !*is_leader.borrow() {
            // A node that no longer leads must not leave its last backlog
            // reading standing for the partition.
            crate::metrics::record_timer_backlog(partition_id, 0, 0, false);
            continue;
        }
        let legacy = config.legacy_every > 0 && polls.is_multiple_of(config.legacy_every);
        polls = polls.wrapping_add(1);
        match poll_partition_once(
            partition_id,
            partition_count,
            store.as_ref(),
            segment.as_ref(),
            config,
            legacy,
            now_unix_ns(),
        )
        .await
        {
            Ok(pass) if pass.fired > 0 => debug!(partition_id, ?pass, "timers fired"),
            Ok(_) => {}
            Err(error) => error!(partition_id, error = %error, "timer poll failed"),
        }
    }
    info!(partition_id, node_id, "timer poller stopped");
}

fn now_unix_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// One poll of one partition: read its due timers and fire each that is
/// still scheduled. `legacy` also reads the global index for this
/// partition's timers scheduled before the per-partition index existed.
pub async fn poll_partition_once<M: TimerWorkStore + InstanceSupervisorStore + ?Sized>(
    partition_id: u32,
    partition_count: usize,
    store: &M,
    segment: &dyn Segment,
    config: TimerPollerConfig,
    legacy: bool,
    now_ns: i64,
) -> EngineResult<TimerPass> {
    let limit = config.batch_size.max(1);
    let mut due: Vec<Timer> = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut keep = |timer: Timer, due: &mut Vec<Timer>| {
        if seen.insert((timer.project_id.clone(), timer.timer_id.clone())) {
            due.push(timer);
        }
    };
    let partitioned = store
        .scan_due_timers_for_partition(partition_id, now_ns, limit)
        .await?;
    let global = match partitioned {
        Some(timers) => {
            for timer in decode_all(&timers) {
                keep(timer, &mut due);
            }
            legacy
        }
        None => true,
    };
    if global {
        // The global index holds every partition's timers; only this
        // partition's are this leader's to fire. Reading a batch per
        // partition keeps the others' timers from crowding this one's out.
        let global_limit = limit.saturating_mul(partition_count.max(1));
        for timer in decode_all(&store.scan_due_timers(now_ns, global_limit).await?) {
            if partition_for_run(&timer.run_id, partition_count) == partition_id as usize {
                keep(timer, &mut due);
            }
        }
    }

    let scheduled: Vec<&Timer> = due
        .iter()
        .filter(|timer| timer.status == "scheduled")
        .collect();
    let oldest_fire_at_ns = scheduled.iter().filter_map(|timer| fire_at_ns(timer)).min();
    crate::metrics::record_timer_backlog(
        partition_id,
        scheduled.len() as u64,
        oldest_fire_at_ns
            .map(|fire_at| now_ns.saturating_sub(fire_at).max(0) as u64 / 1_000_000)
            .unwrap_or(0),
        due.len() >= limit,
    );

    let mut pass = TimerPass {
        due: scheduled.len(),
        fired: 0,
        held: 0,
    };
    for timer in scheduled {
        if followup_inbox_full(store, timer).await {
            // Held, not dropped: it fires once the instance has room.
            pass.held += 1;
            continue;
        }
        // The index can lag the projection by a record: a timer already
        // fired or canceled is skipped, and a repeat fire is a no-op in the
        // journal, which keeps one decision per timer generation.
        match fire_timer(segment, timer, now_ns).await {
            Ok(()) => pass.fired += 1,
            Err(error) => error!(
                partition_id,
                timer_id = %timer.timer_id,
                error = %error,
                "failed to fire timer"
            ),
        }
    }
    Ok(pass)
}

/// Whether a follow-up's instance already has as many items waiting as an
/// inbox may hold. Its item would be refused by the fold, so the timer waits
/// in the due index until the instance has room.
async fn followup_inbox_full<M: InstanceSupervisorStore + ?Sized>(
    store: &M,
    timer: &Timer,
) -> bool {
    if !store.supports_instances()
        || engine_core::instance::FollowupPayload::from_schedule_payload(&timer.schedule_payload)
            .is_none()
    {
        return false;
    }
    let Some(identity) = engine_core::instance_placement::parse_inbox_run_id(&timer.run_id) else {
        return false;
    };
    match store.load_instance(&identity.instance_key()).await {
        Ok(Some(data)) => {
            engine_core::instance::InstanceRecord::decode(&data).is_ok_and(|record| {
                record.waiting_item_ids.len() >= engine_core::instance::MAX_WAITING_ITEMS
            })
        }
        _ => false,
    }
}

fn decode_all(timers: &[Vec<u8>]) -> Vec<Timer> {
    timers
        .iter()
        .filter_map(|data| match Timer::decode(data.as_slice()) {
            Ok(timer) => Some(timer),
            Err(error) => {
                warn!(error = %error, "failed to decode timer, skipping");
                None
            }
        })
        .collect()
}

fn fire_at_ns(timer: &Timer) -> Option<i64> {
    timer.fire_at.as_ref().map(|fire_at| {
        fire_at
            .seconds
            .saturating_mul(1_000_000_000)
            .saturating_add(i64::from(fire_at.nanos))
    })
}

/// Fire a single timer by appending a timer.fired event to the journal.
async fn fire_timer(segment: &dyn Segment, timer: &Timer, now_ns: i64) -> EngineResult<()> {
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

    // A follow-up delivers an item to its instance's inbox in the same
    // append, so the timer is never fired without its item or the item
    // delivered twice: both are on the inbox, in one partition, and a retry
    // repeats both and the journal keeps the first.
    let mut batch = vec![record];
    if let Some(followup) =
        engine_core::instance::FollowupPayload::from_schedule_payload(&timer.schedule_payload)
    {
        match engine_core::instance::followup_item_record(
            &timer.project_id,
            &timer.run_id,
            &timer.timer_id,
            &followup,
            now_ns,
        ) {
            Ok(item) => batch.push(item),
            Err(error) => warn!(
                timer_id = %timer.timer_id,
                error = %error,
                "follow-up timer fired without its item"
            ),
        }
    }
    segment.append_batch(&batch).await?;
    debug!(timer_id = %timer.timer_id, items = batch.len() - 1, "timer fired");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_work_store::MemoryWorkStore;
    use engine_journal_memory::MemorySegment;

    const NOW_NS: i64 = 10_000_000_000;

    fn config() -> TimerPollerConfig {
        TimerPollerConfig {
            poll_interval: Duration::from_millis(50),
            batch_size: 10,
            legacy_every: 30,
        }
    }

    fn put(store: &MemoryWorkStore, timer_id: &str, run_id: &str, status: &str) {
        let timer = Timer {
            timer_id: timer_id.to_string(),
            run_id: run_id.to_string(),
            project_id: "t1".to_string(),
            timer_key: "sleep_step".to_string(),
            waiting_step: "step_2".to_string(),
            status: status.to_string(),
            fire_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        };
        // The due index holds it as scheduled; its record says `status`.
        store.put_timer(
            "t1",
            timer_id,
            "scheduled",
            1_000_000_000,
            timer.encode_to_vec(),
        );
    }

    async fn poll(
        segment: &MemorySegment,
        store: &MemoryWorkStore,
        partition: u32,
        count: usize,
    ) -> TimerPass {
        poll_partition_once(partition, count, store, segment, config(), false, NOW_NS)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn fires_due_timer() {
        let segment = MemorySegment::new(0);
        let store = MemoryWorkStore::new();
        put(&store, "tmr-1", "r1", "scheduled");

        assert_eq!(
            poll(&segment, &store, 0, 1).await,
            TimerPass {
                due: 1,
                fired: 1,
                held: 0
            }
        );
        let records = segment.read_range(Offset(0), Offset(1)).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event_type, "timer.fired");
        assert_eq!(records[0].run_id, "r1");
        assert_eq!(records[0].project_id, "t1");
        assert_eq!(records[0].metadata.get("timer_id").unwrap(), "tmr-1");
    }

    #[tokio::test]
    async fn skips_already_fired_timer() {
        let segment = MemorySegment::new(0);
        let store = MemoryWorkStore::new();
        // Still in the due index, already fired in its record: the
        // processor hasn't removed the index entry yet.
        put(&store, "tmr-1", "r1", "fired");
        assert_eq!(poll(&segment, &store, 0, 1).await.fired, 0);
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(0));
    }

    #[tokio::test]
    async fn no_due_timers_is_noop() {
        let segment = MemorySegment::new(0);
        let store = MemoryWorkStore::new();
        assert_eq!(poll(&segment, &store, 0, 1).await, TimerPass::default());
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(0));
    }

    #[tokio::test]
    async fn only_a_caught_up_leader_fires() {
        let segment: Arc<MemorySegment> = Arc::new(MemorySegment::new(0));
        let store = Arc::new(MemoryWorkStore::new());
        put(&store, "tmr-1", "r1", "scheduled");
        let (ready_tx, ready) = watch::channel(true);
        let (leader_tx, is_leader) = watch::channel(false);
        let cancel = CancellationToken::new();
        let poller = tokio::spawn(run_partition_timer_poller(
            0,
            "node-1".into(),
            store.clone(),
            segment.clone() as Arc<dyn Segment>,
            ready,
            is_leader,
            cancel.clone(),
            TimerPollerConfig {
                poll_interval: Duration::from_millis(10),
                ..config()
            },
        ));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            segment.tail_offset().await.unwrap(),
            Offset(0),
            "a follower fires nothing"
        );

        // Leading but still replaying: nothing yet.
        ready_tx.send(false).unwrap();
        leader_tx.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(0));

        ready_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while segment.tail_offset().await.unwrap() == Offset(0) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the caught-up leader fires");
        cancel.cancel();
        poller.await.unwrap();
    }

    /// A store whose one instance's inbox is full.
    struct FullInbox(Arc<MemoryWorkStore>);

    #[async_trait::async_trait]
    impl TimerWorkStore for FullInbox {
        async fn scan_due_timers(&self, now_ns: i64, limit: usize) -> EngineResult<Vec<Vec<u8>>> {
            self.0.scan_due_timers(now_ns, limit).await
        }
    }

    #[async_trait::async_trait]
    impl InstanceSupervisorStore for FullInbox {
        fn supports_instances(&self) -> bool {
            true
        }
        async fn load_instance(
            &self,
            key: &engine_core::instance::InstanceKey,
        ) -> EngineResult<Option<Vec<u8>>> {
            let mut record = engine_core::instance::InstanceRecord::new(key.clone(), 1);
            record.waiting_item_ids = (0..engine_core::instance::MAX_WAITING_ITEMS)
                .map(|n| format!("{n:08}-0000-0000-0000-000000000000"))
                .collect();
            Ok(Some(record.encode().unwrap()))
        }
    }

    #[tokio::test]
    async fn a_followup_into_a_full_inbox_is_held_not_fired() {
        use engine_core::instance_placement::{inbox_run_id, InstanceIdentity};
        let identity = InstanceIdentity::new("t1", "env-1", "support", "u_1", 1).unwrap();
        let inbox = inbox_run_id(&identity, 0, 1).unwrap();
        let memory = Arc::new(MemoryWorkStore::new());
        let timer = Timer {
            timer_id: "followup-tmr".into(),
            run_id: inbox,
            project_id: "t1".into(),
            status: "scheduled".into(),
            fire_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            schedule_payload: serde_json::to_vec(&serde_json::json!({
                "followup": {"conversation_id": "api", "content": "check again"},
            }))
            .unwrap(),
            ..Default::default()
        };
        memory.put_timer(
            "t1",
            "followup-tmr",
            "scheduled",
            1_000_000_000,
            timer.encode_to_vec(),
        );
        let segment = MemorySegment::new(0);
        let pass = poll_partition_once(
            0,
            1,
            &FullInbox(memory.clone()),
            &segment,
            config(),
            false,
            NOW_NS,
        )
        .await
        .unwrap();
        assert_eq!(
            pass,
            TimerPass {
                due: 1,
                fired: 0,
                held: 1
            }
        );
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(0));
        // With room (a store that keeps no instances reads as room), it fires.
        let pass = poll_partition_once(0, 1, memory.as_ref(), &segment, config(), false, NOW_NS)
            .await
            .unwrap();
        assert_eq!(pass.fired, 1);
    }

    // Without a per-partition index, each leader fires only the timers of
    // runs on its own partition, so two leaders never fire the same timer.
    #[tokio::test]
    async fn a_leader_fires_only_its_own_partitions_timers() {
        let store = MemoryWorkStore::new();
        let runs: Vec<String> = (0..16).map(|n| format!("run-{n}")).collect();
        for (n, run) in runs.iter().enumerate() {
            put(&store, &format!("tmr-{n}"), run, "scheduled");
        }
        let mut fired = Vec::new();
        for partition in 0..4u32 {
            let segment = MemorySegment::new(0);
            let pass = poll(&segment, &store, partition, 4).await;
            let tail = segment.tail_offset().await.unwrap();
            let records = segment.read_range(Offset(0), tail).await.unwrap();
            assert_eq!(records.len(), pass.fired);
            for record in records {
                assert_eq!(partition_for_run(&record.run_id, 4), partition as usize);
                fired.push(record.run_id);
            }
        }
        fired.sort();
        let mut expected = runs.clone();
        expected.sort();
        assert_eq!(fired, expected, "every timer fired once, by its own leader");
    }
}
