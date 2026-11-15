use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use engine_core::types::{Offset, Record};

use crate::{CacheConfig, RecordCache};

fn test_record(tenant: &str, run: &str, event_type: &str) -> Record {
    static NEXT_OFFSET: AtomicU64 = AtomicU64::new(1_000_000);
    Record {
        offset: Offset(NEXT_OFFSET.fetch_add(1, Ordering::Relaxed)),
        project_id: tenant.to_string(),
        run_id: run.to_string(),
        event_type: event_type.to_string(),
        data: bytes::Bytes::from(b"{}".as_ref()),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: HashMap::new(),
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

fn test_record_with_step(tenant: &str, run: &str, step_key: &str) -> Record {
    let mut r = test_record(tenant, run, "step.completed");
    r.step_key = Some(step_key.to_string());
    r
}

fn failed_record_with_attempt(tenant: &str, run: &str, attempt: i32, max_attempts: i32) -> Record {
    let mut r = test_record(tenant, run, "run.failed");
    r.metadata
        .insert("attempt".to_string(), attempt.to_string());
    r.metadata
        .insert("max_attempts".to_string(), max_attempts.to_string());
    r
}

#[test]
fn put_and_get_by_run_id() {
    let cache = RecordCache::new(CacheConfig::default());

    cache.put(Arc::new(test_record("t1", "r1", "run.started")));
    cache.put(Arc::new(test_record("t1", "r1", "step.completed")));
    cache.put(Arc::new(test_record("t1", "r1", "run.completed")));

    let records = cache.get_by_run_id("t1", "r1").unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].event_type, "run.started");
    assert_eq!(records[2].event_type, "run.completed");
}

#[test]
fn get_missing_run() {
    let cache = RecordCache::new(CacheConfig::default());
    assert!(cache.get_by_run_id("t1", "nonexistent").is_none());
}

#[test]
fn multiple_runs_isolated() {
    let cache = RecordCache::new(CacheConfig::default());

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    cache.put(Arc::new(test_record("t1", "r2", "b")));
    cache.put(Arc::new(test_record("t2", "r1", "c")));

    let r1 = cache.get_by_run_id("t1", "r1").unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].event_type, "a");

    let r2 = cache.get_by_run_id("t1", "r2").unwrap();
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].event_type, "b");

    // Same run_id, different tenant.
    let r3 = cache.get_by_run_id("t2", "r1").unwrap();
    assert_eq!(r3[0].event_type, "c");

    assert_eq!(cache.run_count(), 3);
}

#[test]
fn get_step_memo() {
    let cache = RecordCache::new(CacheConfig::default());

    cache.put(Arc::new(test_record("t1", "r1", "run.started")));
    cache.put(Arc::new(test_record_with_step("t1", "r1", "step-a")));
    cache.put(Arc::new(test_record_with_step("t1", "r1", "step-b")));

    let memo = cache.get_step_memo("t1", "r1", "step-b").unwrap();
    assert_eq!(memo.step_key.as_deref(), Some("step-b"));

    // Missing step key.
    assert!(cache.get_step_memo("t1", "r1", "step-c").is_none());

    // Missing run.
    assert!(cache.get_step_memo("t1", "r999", "step-a").is_none());
}

#[test]
fn remove_run() {
    let cache = RecordCache::new(CacheConfig::default());

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    cache.put(Arc::new(test_record("t1", "r1", "b")));
    let size_before = cache.size_bytes();
    assert!(size_before > 0);

    cache.remove("t1", "r1");

    assert!(cache.get_by_run_id("t1", "r1").is_none());
    assert_eq!(cache.size_bytes(), 0);
    assert_eq!(cache.run_count(), 0);
}

#[test]
fn remove_nonexistent_is_noop() {
    let cache = RecordCache::new(CacheConfig::default());
    cache.remove("t1", "r1"); // should not panic
}

#[test]
fn memory_budget_eviction() {
    // Very small budget — should trigger eviction.
    // Each test record is ~262 bytes (256 overhead + data + keys).
    let cache = RecordCache::new(CacheConfig {
        max_bytes: 300, // fits ~1 record
        completion_ttl_secs: 60,
    });

    // Each record is ~256+ bytes overhead. Insert two runs.
    cache.put(Arc::new(test_record("t1", "r1", "a")));
    assert_eq!(cache.run_count(), 1);

    // Second run should trigger eviction of first (LRU).
    cache.put(Arc::new(test_record("t1", "r2", "b")));

    // The oldest run (r1) should have been evicted.
    assert!(cache.get_by_run_id("t1", "r1").is_none());
    assert!(cache.get_by_run_id("t1", "r2").is_some());
    assert!(cache.size_bytes() <= 300);
}

#[test]
fn lru_ordering() {
    // Each test record is ~262 bytes. Budget fits 3 records but not 4.
    let cache = RecordCache::new(CacheConfig {
        max_bytes: 850,
        completion_ttl_secs: 60,
    });

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    cache.put(Arc::new(test_record("t1", "r2", "b")));
    cache.put(Arc::new(test_record("t1", "r3", "c")));

    // Access r1 again — moves it to most recent in LRU.
    cache.put(Arc::new(test_record("t1", "r1", "d")));

    // Over budget now (~1048). Should evict r2 (oldest in LRU), not r1 or r3.

    assert!(
        cache.get_by_run_id("t1", "r2").is_none(),
        "r2 should be evicted (LRU)"
    );
    assert!(
        cache.get_by_run_id("t1", "r1").is_some(),
        "r1 should still be cached"
    );
    assert!(
        cache.get_by_run_id("t1", "r3").is_some(),
        "r3 should be cached"
    );
}

#[test]
fn write_recency_work_is_constant_at_high_cardinality() {
    let cache = RecordCache::new(CacheConfig::default());
    for index in 0..1_024 {
        cache.put(Arc::new(test_record("t1", &format!("r{index}"), "queued")));
    }

    cache.take_recency_work();
    cache.put(Arc::new(test_record("t1", "r0", "assigned")));

    assert!(
        cache.take_recency_work() <= 4,
        "one recency update must not walk the cache cardinality"
    );
}

#[test]
fn write_recency_handles_touch_remove_and_oldest_eviction() {
    let mut recency = crate::RecencyList::default();
    let key = |run: &str| ("t1".to_string(), run.to_string());

    recency.touch(key("r1"));
    recency.touch(key("r2"));
    recency.touch(key("r3"));
    recency.touch(key("r1"));
    assert!(recency.remove(&key("r3")));

    assert_eq!(recency.pop_oldest(), Some(key("r2")));
    assert_eq!(recency.pop_oldest(), Some(key("r1")));
    assert_eq!(recency.pop_oldest(), None);
}

#[test]
fn memory_pressure_eviction_cleans_transient_sender() {
    let cache = RecordCache::new(CacheConfig {
        max_bytes: 300,
        completion_ttl_secs: 60,
    });
    cache.put(Arc::new(test_record("t1", "r1", "a")));
    let _receiver = cache.subscribe_transient("t1", "r1");

    cache.put(Arc::new(test_record("t1", "r2", "b")));

    assert!(!cache
        .transient_senders
        .contains_key(&("t1".to_string(), "r1".to_string())));
}

#[test]
fn size_tracking() {
    let cache = RecordCache::new(CacheConfig::default());
    assert_eq!(cache.size_bytes(), 0);

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    let size_one = cache.size_bytes();
    assert!(size_one > 0);

    cache.put(Arc::new(test_record("t1", "r1", "b")));
    let size_two = cache.size_bytes();
    assert!(size_two > size_one);

    cache.remove("t1", "r1");
    assert_eq!(cache.size_bytes(), 0);
}

#[tokio::test]
async fn mark_complete_evicts_after_ttl() {
    let cache = Arc::new(RecordCache::new(CacheConfig {
        max_bytes: DEFAULT_MAX_BYTES,
        completion_ttl_secs: 0, // immediate eviction for testing
    }));

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    assert!(cache.get_by_run_id("t1", "r1").is_some());

    RecordCache::mark_complete(cache.clone(), "t1".to_string(), "r1".to_string());

    // Give the spawned task time to run.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(cache.get_by_run_id("t1", "r1").is_none());
}

#[tokio::test]
async fn terminal_evictions_share_one_bounded_sweeper() {
    let cache = Arc::new(RecordCache::new(CacheConfig {
        max_bytes: DEFAULT_MAX_BYTES,
        completion_ttl_secs: 60,
    }));

    for index in 0..1_000 {
        let run_id = format!("r{index}");
        cache.put(Arc::new(test_record("t1", &run_id, "run.completed")));
        RecordCache::mark_complete(cache.clone(), "t1".to_string(), run_id);
    }

    assert_eq!(cache.completion_sweeper_starts(), 1);
    let schedule = cache
        .completion_evictions
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    assert_eq!(schedule.by_key.len(), 1_000);
    assert_eq!(schedule.ordered.len(), 1_000);
}

#[test]
fn rescheduled_completion_cannot_be_claimed_by_an_old_expiry() {
    let mut schedule = crate::CompletionEvictions::default();
    let key = ("t1".to_string(), "r1".to_string());
    let first = std::time::Instant::now();
    let second = first + std::time::Duration::from_secs(60);

    schedule.schedule(key.clone(), first);
    let due = schedule.take_due(first);
    schedule.schedule(key.clone(), second);

    assert_eq!(due, vec![(first, key.clone())]);
    assert!(!schedule.claim(first, &key));
    assert_eq!(schedule.by_key.get(&key), Some(&second));
}

#[tokio::test]
async fn terminal_completed_record_evicts_after_ttl() {
    let cache = Arc::new(RecordCache::new(CacheConfig {
        max_bytes: DEFAULT_MAX_BYTES,
        completion_ttl_secs: 0,
    }));
    let record = test_record("t1", "r1", "run.completed");

    cache.put(Arc::new(record.clone()));
    RecordCache::mark_terminal_if_complete(&cache, &record);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(cache.get_by_run_id("t1", "r1").is_none());
}

#[tokio::test]
async fn intermediate_failed_retry_does_not_evict_after_ttl() {
    let cache = Arc::new(RecordCache::new(CacheConfig {
        max_bytes: DEFAULT_MAX_BYTES,
        completion_ttl_secs: 0,
    }));
    let record = failed_record_with_attempt("t1", "r1", 0, 3);

    cache.put(Arc::new(record.clone()));
    RecordCache::mark_terminal_if_complete(&cache, &record);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(cache.get_by_run_id("t1", "r1").is_some());
}

#[tokio::test]
async fn final_failed_retry_evicts_after_ttl() {
    let cache = Arc::new(RecordCache::new(CacheConfig {
        max_bytes: DEFAULT_MAX_BYTES,
        completion_ttl_secs: 0,
    }));
    let record = failed_record_with_attempt("t1", "r1", 2, 3);

    cache.put(Arc::new(record.clone()));
    RecordCache::mark_terminal_if_complete(&cache, &record);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(cache.get_by_run_id("t1", "r1").is_none());
}

const DEFAULT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

fn test_record_with_offset(tenant: &str, run: &str, event_type: &str, offset: u64) -> Record {
    let mut r = test_record(tenant, run, event_type);
    r.offset = Offset(offset);
    r
}

#[test]
fn get_by_run_id_from_offset() {
    let cache = RecordCache::new(CacheConfig::default());

    cache.put(Arc::new(test_record_with_offset("t1", "r1", "a", 0)));
    cache.put(Arc::new(test_record_with_offset("t1", "r1", "b", 1)));
    cache.put(Arc::new(test_record_with_offset("t1", "r1", "c", 2)));
    cache.put(Arc::new(test_record_with_offset("t1", "r1", "d", 3)));

    // From offset 0 — all records.
    let all = cache
        .get_by_run_id_from_offset("t1", "r1", Offset(0))
        .unwrap();
    assert_eq!(all.len(), 4);

    // From offset 2 — last two.
    let from_2 = cache
        .get_by_run_id_from_offset("t1", "r1", Offset(2))
        .unwrap();
    assert_eq!(from_2.len(), 2);
    assert_eq!(from_2[0].event_type, "c");
    assert_eq!(from_2[1].event_type, "d");

    // From offset 10 — none match but run exists.
    let from_10 = cache
        .get_by_run_id_from_offset("t1", "r1", Offset(10))
        .unwrap();
    assert!(from_10.is_empty());

    // Missing run.
    assert!(cache
        .get_by_run_id_from_offset("t1", "r999", Offset(0))
        .is_none());
}

#[test]
fn put_dedupes_same_record_replayed_from_tail() {
    let cache = RecordCache::new(CacheConfig::default());
    let record = test_record_with_offset("t1", "r1", "run.started", 42);

    cache.put(Arc::new(record.clone()));
    cache.put(Arc::new(record));

    let records = cache.get_by_run_id("t1", "r1").unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].event_type, "run.started");
    assert_eq!(records[0].offset, Offset(42));
}

#[test]
fn put_orders_processor_side_effects_by_journal_offset() {
    let cache = RecordCache::new(CacheConfig::default());
    let retry = test_record_with_offset("t1", "r1", "run.queued", 43);
    let failure = test_record_with_offset("t1", "r1", "run.failed", 42);

    // Simulate concurrent write-through observing N+1 before N.
    cache.put(Arc::new(retry));
    cache.put(Arc::new(failure));

    let records = cache.get_by_run_id("t1", "r1").unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| (record.offset.0, record.event_type.as_str()))
            .collect::<Vec<_>>(),
        vec![(42, "run.failed"), (43, "run.queued")]
    );
}

#[tokio::test]
async fn put_rejects_conflicting_records_without_accounting_or_notification() {
    let cache = RecordCache::new(CacheConfig::default());
    cache.put(Arc::new(test_record_with_offset(
        "t1",
        "r1",
        "run.queued",
        0,
    )));
    let r1_size = cache.size_bytes();
    let r1_notify = cache.subscribe("t1", "r1");
    let r1_notified = r1_notify.notified();
    cache.put(Arc::new(test_record_with_offset(
        "t1",
        "r1",
        "run.failed",
        0,
    )));
    assert_eq!(cache.size_bytes(), r1_size);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), r1_notified)
            .await
            .is_err(),
        "rejected conflict must not notify subscribers"
    );

    let first_data = test_record_with_offset("t1", "r2", "run.queued", 1);
    let mut conflicting_data = first_data.clone();
    conflicting_data.data = bytes::Bytes::from_static(b"{\"different\":true}");
    cache.put(Arc::new(first_data));
    let r2_size = cache.size_bytes();
    let r2_notify = cache.subscribe("t1", "r2");
    let r2_notified = r2_notify.notified();
    cache.put(Arc::new(conflicting_data));
    assert_eq!(cache.size_bytes(), r2_size);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), r2_notified)
            .await
            .is_err(),
        "rejected conflict must not notify subscribers"
    );

    for run_id in ["r1", "r2"] {
        let records = cache.get_by_run_id("t1", run_id).unwrap();
        assert_eq!(records.len(), 1, "conflict retained for {run_id}");
        assert_eq!(records[0].event_type, "run.queued");
    }
}

#[test]
fn has_run() {
    let cache = RecordCache::new(CacheConfig::default());
    assert!(!cache.has_run("t1", "r1"));

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    assert!(cache.has_run("t1", "r1"));
    assert!(!cache.has_run("t1", "r2"));

    cache.remove("t1", "r1");
    assert!(!cache.has_run("t1", "r1"));
}

#[tokio::test]
async fn subscribe_notifies_on_put() {
    let cache = Arc::new(RecordCache::new(CacheConfig::default()));

    let notify = cache.subscribe("t1", "r1");

    // Spawn a task that waits for notification.
    let cache2 = cache.clone();
    let handle = tokio::spawn(async move {
        notify.notified().await;
        // After notification, should be able to read the new record.
        cache2.get_by_run_id("t1", "r1").unwrap().len()
    });

    // Small delay then put a record.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    cache.put(Arc::new(test_record("t1", "r1", "a")));

    let count = handle.await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn subscribe_only_notifies_matching_run() {
    let cache = Arc::new(RecordCache::new(CacheConfig::default()));

    let notify_r1 = cache.subscribe("t1", "r1");
    let notify_r2 = cache.subscribe("t1", "r2");

    // Register notified() futures BEFORE the put — Notify only wakes
    // tasks already waiting, it does not store permits for future waiters.
    let r1_notified = notify_r1.notified();
    let r2_notified = notify_r2.notified();

    // Put a record for r2 — should wake r2 subscriber but NOT r1.
    cache.put(Arc::new(test_record("t1", "r2", "b")));

    // r2 notification should resolve immediately.
    let r2_result = tokio::time::timeout(std::time::Duration::from_millis(50), r2_notified).await;
    assert!(r2_result.is_ok(), "r2 subscriber should be notified");

    // r1 notification should timeout (not notified).
    let r1_result = tokio::time::timeout(std::time::Duration::from_millis(50), r1_notified).await;
    assert!(r1_result.is_err(), "r1 subscriber should NOT be notified");
}

#[test]
fn remove_cleans_up_notifier() {
    let cache = RecordCache::new(CacheConfig::default());

    cache.put(Arc::new(test_record("t1", "r1", "a")));
    let _notify = cache.subscribe("t1", "r1");

    cache.remove("t1", "r1");

    // Subscribing again should create a fresh notifier (old one was cleaned up).
    let notify2 = cache.subscribe("t1", "r1");
    // Should not panic and should be a valid Notify.
    drop(notify2);
}
