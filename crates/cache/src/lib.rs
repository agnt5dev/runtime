use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use engine_core::types::{Offset, Record};
use tokio::sync::{broadcast, Notify};

pub mod metrics;

/// Approximate overhead per record entry (struct fields, Arc, pointers).
const RECORD_OVERHEAD: u64 = 256;

/// Default memory budget: 2 GB.
const DEFAULT_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default TTL for completed run eviction: 5 minutes.
///
/// Set to 300s so the run drawer's live
/// tail still serves an event subscription if a user opens it a few
/// minutes after a run finished. The cache is bounded by `max_bytes`
/// (default 2 GB) so a longer TTL doesn't make memory unbounded —
/// LRU eviction kicks in when active runs need the space.
const DEFAULT_COMPLETION_TTL_SECS: u64 = 300;

/// Per-run live-event buffer. Transient output is deliberately bounded and
/// never enters the durable record collection.
const TRANSIENT_BROADCAST_CAPACITY: usize = 1_024;

/// Cache key: (project_id, run_id).
type CacheKey = (String, String);

#[derive(Clone)]
struct RecencyLinks {
    older: Option<CacheKey>,
    newer: Option<CacheKey>,
}

/// Intrusive write-recency list with constant-time touch and removal.
///
/// Keeping the links in one hash table avoids the full `VecDeque::retain`
/// walk that made every append proportional to the number of cached runs.
#[derive(Default)]
struct RecencyList {
    links: HashMap<CacheKey, RecencyLinks>,
    oldest: Option<CacheKey>,
    newest: Option<CacheKey>,
}

#[derive(Default)]
struct CompletionEvictions {
    ordered: BTreeSet<(Instant, CacheKey)>,
    by_key: HashMap<CacheKey, Instant>,
    sweeper_running: bool,
}

impl CompletionEvictions {
    fn schedule(&mut self, key: CacheKey, deadline: Instant) -> bool {
        if let Some(previous) = self.by_key.insert(key.clone(), deadline) {
            self.ordered.remove(&(previous, key.clone()));
        }
        self.ordered.insert((deadline, key));
        if self.sweeper_running {
            false
        } else {
            self.sweeper_running = true;
            true
        }
    }

    fn remove(&mut self, key: &CacheKey) {
        if let Some(deadline) = self.by_key.remove(key) {
            self.ordered.remove(&(deadline, key.clone()));
        }
    }

    fn take_due(&mut self, now: Instant) -> Vec<(Instant, CacheKey)> {
        let mut due = Vec::new();
        while self
            .ordered
            .first()
            .is_some_and(|(deadline, _)| *deadline <= now)
        {
            let Some((deadline, key)) = self.ordered.pop_first() else {
                break;
            };
            due.push((deadline, key));
        }
        due
    }

    fn claim(&mut self, deadline: Instant, key: &CacheKey) -> bool {
        if self.by_key.get(key) != Some(&deadline) {
            return false;
        }
        self.by_key.remove(key);
        true
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.ordered.first().map(|(deadline, _)| *deadline)
    }
}

impl RecencyList {
    fn touch(&mut self, key: CacheKey) {
        if self.newest.as_ref() == Some(&key) {
            return;
        }

        if let Some(existing) = self.links.get(&key).cloned() {
            if let Some(older) = existing.older.as_ref() {
                if let Some(links) = self.links.get_mut(older) {
                    links.newer = existing.newer.clone();
                }
            } else {
                self.oldest = existing.newer.clone();
            }
            if let Some(newer) = existing.newer.as_ref() {
                if let Some(links) = self.links.get_mut(newer) {
                    links.older = existing.older.clone();
                }
            }
        }

        let previous_newest = self.newest.replace(key.clone());
        if let Some(previous_newest) = previous_newest.as_ref() {
            if let Some(links) = self.links.get_mut(previous_newest) {
                links.newer = Some(key.clone());
            }
        } else {
            self.oldest = Some(key.clone());
        }
        self.links.insert(
            key,
            RecencyLinks {
                older: previous_newest,
                newer: None,
            },
        );
    }

    fn remove(&mut self, key: &CacheKey) -> bool {
        let Some(removed) = self.links.remove(key) else {
            return false;
        };
        if let Some(older) = removed.older.as_ref() {
            if let Some(links) = self.links.get_mut(older) {
                links.newer = removed.newer.clone();
            }
        } else {
            self.oldest = removed.newer.clone();
        }
        if let Some(newer) = removed.newer.as_ref() {
            if let Some(links) = self.links.get_mut(newer) {
                links.older = removed.older.clone();
            }
        } else {
            self.newest = removed.older;
        }
        true
    }

    fn pop_oldest(&mut self) -> Option<CacheKey> {
        let key = self.oldest.clone()?;
        self.remove(&key);
        Some(key)
    }
}

/// Configuration for RecordCache.
pub struct CacheConfig {
    /// Maximum memory budget in bytes.
    pub max_bytes: u64,
    /// Seconds to keep a completed run in cache before eviction.
    pub completion_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_BYTES,
            completion_ttl_secs: DEFAULT_COMPLETION_TTL_SECS,
        }
    }
}

/// In-memory concurrent cache for zero-latency read-after-write on active runs.
///
/// Records are keyed by `(project_id, run_id)`. The cache tracks approximate memory
/// usage and evicts the least-recently-used runs when the budget is exceeded.
///
/// Supports per-run notifications via `subscribe()` for efficient Tail streaming.
/// Subscribers are only woken when their specific run receives new records —
/// no thundering herd.
pub struct RecordCache {
    /// Records grouped by (project_id, run_id).
    entries: DashMap<CacheKey, Vec<Arc<Record>>>,
    /// Approximate current memory usage in bytes.
    size_bytes: AtomicU64,
    /// Maximum memory budget.
    max_bytes: u64,
    /// Write-recency eviction order with constant-time updates.
    recency: Mutex<RecencyList>,
    #[cfg(test)]
    recency_work: AtomicU64,
    /// TTL for completed run eviction.
    completion_ttl_secs: u64,
    /// One bounded expiry schedule replaces one Tokio sleeper per terminal run.
    completion_evictions: Mutex<CompletionEvictions>,
    completion_wakeup: Arc<Notify>,
    #[cfg(test)]
    completion_sweeper_starts: AtomicU64,
    /// Per-run notifiers for Tail subscribers. Cleaned up on run removal.
    run_notifiers: DashMap<CacheKey, Arc<Notify>>,
    /// Per-run fanout for SSE-only events. Broadcast receivers observe only
    /// events published after subscription; lagged receivers may miss deltas.
    transient_senders: DashMap<CacheKey, broadcast::Sender<Arc<Record>>>,
}

impl RecordCache {
    /// Create a new cache with the given configuration.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            entries: DashMap::new(),
            size_bytes: AtomicU64::new(0),
            max_bytes: config.max_bytes,
            recency: Mutex::new(RecencyList::default()),
            #[cfg(test)]
            recency_work: AtomicU64::new(0),
            completion_ttl_secs: config.completion_ttl_secs,
            completion_evictions: Mutex::new(CompletionEvictions::default()),
            completion_wakeup: Arc::new(Notify::new()),
            #[cfg(test)]
            completion_sweeper_starts: AtomicU64::new(0),
            run_notifiers: DashMap::new(),
            transient_senders: DashMap::new(),
        }
    }

    /// Approximate byte size of a record.
    fn record_size(record: &Record) -> u64 {
        let data_len = record.data.len() as u64;
        let key_len = (record.project_id.len() + record.run_id.len()) as u64;
        data_len + key_len + RECORD_OVERHEAD
    }

    /// Insert a record into the cache. Call this after a successful segment append.
    ///
    /// Notifies any Tail subscribers watching this run.
    pub fn put(&self, record: Arc<Record>) {
        let key = (record.project_id.clone(), record.run_id.clone());
        let size = Self::record_size(&record);

        // Append/write-through and processor tail replay can both observe the
        // same journal record. Keep the cache idempotent so run event reads do
        // not surface duplicate lifecycle events.
        {
            let mut records = self.entries.entry(key.clone()).or_default();
            let insert_at = match records
                .binary_search_by_key(&record.offset.0, |existing| existing.offset.0)
            {
                Ok(existing_index) => {
                    let existing = &records[existing_index];
                    // Journal offsets are unique, including partition 0/local
                    // offset 0. A differing record at one offset is corruption,
                    // never another cache entry.
                    if existing.event_type != record.event_type
                        || existing.data != record.data
                        || existing.timestamp_ns != record.timestamp_ns
                        || existing.step_key != record.step_key
                        || existing.correlation_id != record.correlation_id
                        || existing.parent_event_id != record.parent_event_id
                        || existing.metadata != record.metadata
                        || existing.data_type != record.data_type
                        || existing.data_checksum != record.data_checksum
                        || existing.data_compressed != record.data_compressed
                    {
                        tracing::error!(
                            project_id = %record.project_id,
                            run_id = %record.run_id,
                            offset = record.offset.0,
                            existing_event_type = %existing.event_type,
                            incoming_event_type = %record.event_type,
                            "rejecting conflicting cache record at an existing journal offset"
                        );
                    }
                    return;
                }
                Err(insert_at) => insert_at,
            };
            // Concurrent/general write-through calls may arrive out of order.
            // Readers treat the final record as the latest lifecycle state, so
            // keep the cache ordered by journal offset instead of arrival time.
            records.insert(insert_at, record);
        }

        self.size_bytes.fetch_add(size, Ordering::Relaxed);

        // Update write recency in constant time.
        {
            self.recency.lock().unwrap().touch(key.clone());
            #[cfg(test)]
            self.recency_work.fetch_add(1, Ordering::Relaxed);
        }

        // Evict if over budget.
        self.evict_if_needed();

        metrics::record_size(
            self.size_bytes.load(Ordering::Relaxed),
            self.entries.len() as u64,
        );

        // Notify per-run subscribers (Tail watchers).
        if let Some(notify) = self.run_notifiers.get(&key) {
            notify.notify_waiters();
            metrics::record_notify();
        }
    }

    /// Get all cached records for a run, ordered by journal offset.
    pub fn get_by_run_id(&self, project_id: &str, run_id: &str) -> Option<Vec<Arc<Record>>> {
        let key = (project_id.to_string(), run_id.to_string());
        self.entries.get(&key).map(|v| v.clone())
    }

    /// Look up a memoized step result by scanning cached records for a matching step_key.
    /// Returns the first record with the given step_key (step completion events).
    pub fn get_step_memo(
        &self,
        project_id: &str,
        run_id: &str,
        step_key: &str,
    ) -> Option<Arc<Record>> {
        let key = (project_id.to_string(), run_id.to_string());
        self.entries.get(&key).and_then(|records| {
            records
                .iter()
                .find(|r| {
                    r.step_key.as_deref() == Some(step_key)
                        // Every activation record carries the stable key;
                        // only the completion holds a result.
                        && engine_core::activation::activation_record_identity(r)
                            .is_none_or(|(_, _, transition)| {
                                transition == engine_core::activation::TRANSITION_COMPLETED
                            })
                })
                .cloned()
        })
    }

    /// Mark a run as complete. Schedules eviction after the configured TTL.
    ///
    /// Call this when a terminal run event is processed.
    pub fn mark_complete(cache: Arc<Self>, project_id: String, run_id: String) {
        let deadline = Instant::now() + Duration::from_secs(cache.completion_ttl_secs);
        let should_start = cache
            .completion_evictions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .schedule((project_id, run_id), deadline);
        cache.completion_wakeup.notify_one();
        if should_start {
            #[cfg(test)]
            cache
                .completion_sweeper_starts
                .fetch_add(1, Ordering::Relaxed);
            let weak = Arc::downgrade(&cache);
            let wakeup = cache.completion_wakeup.clone();
            tokio::spawn(Self::run_completion_sweeper(weak, wakeup));
        }
    }

    async fn run_completion_sweeper(cache: Weak<Self>, wakeup: Arc<Notify>) {
        loop {
            let Some(cache) = cache.upgrade() else {
                return;
            };
            let (due, next_deadline) = {
                let mut schedule = cache
                    .completion_evictions
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let due = schedule.take_due(Instant::now());
                let next_deadline = schedule.next_deadline();
                if due.is_empty() && next_deadline.is_none() {
                    schedule.sweeper_running = false;
                    return;
                }
                (due, next_deadline)
            };

            for (deadline, key) in due {
                let claimed = cache
                    .completion_evictions
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .claim(deadline, &key);
                if !claimed {
                    continue;
                }
                if cache.entries.contains_key(&key) {
                    metrics::record_eviction("completion_ttl");
                }
                cache.remove_key(&key, false);
            }
            drop(cache);

            if let Some(deadline) = next_deadline {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline.into()) => {}
                    _ = wakeup.notified() => {}
                }
            }
        }
    }

    /// Schedule completion-TTL eviction when `record` is terminal.
    ///
    /// `run.failed` can be an intermediate retry signal. Match the gateway's
    /// terminal predicate so the cache does not evict a run that still has a
    /// retry attempt coming.
    pub fn mark_terminal_if_complete(cache: &Arc<Self>, record: &Record) {
        if !Self::should_evict_after_terminal(record) {
            return;
        }
        Self::mark_complete(
            Arc::clone(cache),
            record.project_id.clone(),
            record.run_id.clone(),
        );
    }

    fn should_evict_after_terminal(record: &Record) -> bool {
        match record.event_type.as_str() {
            "run.completed" | "run.cancelled" => true,
            "run.failed" => failure_exhausted_retries(record),
            _ => false,
        }
    }

    /// Remove a run from the cache and update accounting.
    /// Also cleans up the per-run notifier.
    pub fn remove(&self, project_id: &str, run_id: &str) {
        let key = (project_id.to_string(), run_id.to_string());
        self.remove_key(&key, true);
    }

    fn remove_key(&self, key: &CacheKey, unschedule_completion: bool) {
        if unschedule_completion {
            self.completion_evictions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(key);
        }

        if let Some((_, records)) = self.entries.remove(key) {
            let freed: u64 = records.iter().map(|r| Self::record_size(r)).sum();
            self.size_bytes.fetch_sub(freed, Ordering::Relaxed);

            self.recency.lock().unwrap().remove(key);
        }

        // Clean up notifier. Any waiting subscribers will be woken
        // and see an empty result, causing them to end the stream.
        if let Some((_, notify)) = self.run_notifiers.remove(key) {
            notify.notify_waiters();
            metrics::record_notify();
        }
        self.transient_senders.remove(key);

        metrics::record_size(
            self.size_bytes.load(Ordering::Relaxed),
            self.entries.len() as u64,
        );
        metrics::record_subscribers(self.run_notifiers.len() as u64);
    }

    /// Subscribe to notifications for a specific run.
    ///
    /// Returns a `Notify` that is woken each time a record is appended for this run.
    /// Used by the Tail RPC for efficient per-run streaming without segment scanning.
    pub fn subscribe(&self, project_id: &str, run_id: &str) -> Arc<Notify> {
        let key = (project_id.to_string(), run_id.to_string());
        let notifier = self
            .run_notifiers
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();
        metrics::record_subscribers(self.run_notifiers.len() as u64);
        notifier
    }

    /// Subscribe to future transient events for a run. Unlike durable cache
    /// records, this channel has no history or replay contract.
    pub fn subscribe_transient(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> broadcast::Receiver<Arc<Record>> {
        let key = (project_id.to_string(), run_id.to_string());
        self.transient_senders
            .entry(key)
            .or_insert_with(|| broadcast::channel(TRANSIENT_BROADCAST_CAPACITY).0)
            .subscribe()
    }

    /// Publish one SSE-only event to currently connected run subscribers.
    /// Returns the number of receivers that accepted the event.
    pub fn publish_transient(&self, record: Arc<Record>) -> usize {
        let key = (record.project_id.clone(), record.run_id.clone());
        self.transient_senders
            .entry(key)
            .or_insert_with(|| broadcast::channel(TRANSIENT_BROADCAST_CAPACITY).0)
            .send(record)
            .unwrap_or(0)
    }

    /// Get cached records for a run starting from the given offset.
    ///
    /// Returns records with `offset >= from_offset`, ordered by offset.
    /// Returns `None` if the run is not in the cache at all.
    pub fn get_by_run_id_from_offset(
        &self,
        project_id: &str,
        run_id: &str,
        from_offset: Offset,
    ) -> Option<Vec<Arc<Record>>> {
        let key = (project_id.to_string(), run_id.to_string());
        self.entries.get(&key).map(|records| {
            records
                .iter()
                .filter(|r| r.offset.0 >= from_offset.0)
                .cloned()
                .collect()
        })
    }

    /// Check whether a run exists in the cache.
    pub fn has_run(&self, project_id: &str, run_id: &str) -> bool {
        let key = (project_id.to_string(), run_id.to_string());
        self.entries.contains_key(&key)
    }

    /// Current approximate memory usage in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Number of runs currently cached.
    pub fn run_count(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn take_recency_work(&self) -> u64 {
        self.recency_work.swap(0, Ordering::Relaxed)
    }

    #[cfg(test)]
    fn completion_sweeper_starts(&self) -> u64 {
        self.completion_sweeper_starts.load(Ordering::Relaxed)
    }

    /// Evict least-recently-used runs until we're under budget.
    fn evict_if_needed(&self) {
        while self.size_bytes.load(Ordering::Relaxed) > self.max_bytes {
            let key_to_evict = { self.recency.lock().unwrap().pop_oldest() };

            match key_to_evict {
                Some(key) => {
                    if let Some((_, records)) = self.entries.remove(&key) {
                        let freed: u64 = records.iter().map(|r| Self::record_size(r)).sum();
                        self.size_bytes.fetch_sub(freed, Ordering::Relaxed);
                        metrics::record_eviction("memory_pressure");
                        // Wake and clean up any Tail subscribers for this evicted run.
                        if let Some((_, notify)) = self.run_notifiers.remove(&key) {
                            notify.notify_waiters();
                            metrics::record_notify();
                        }
                        self.transient_senders.remove(&key);
                        self.completion_evictions
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .remove(&key);
                        tracing::debug!(
                            project_id = %key.0,
                            run_id = %key.1,
                            freed_bytes = freed,
                            "evicted run from cache (memory pressure)"
                        );
                    }
                }
                None => break, // LRU is empty, nothing to evict.
            }
        }
    }
}

fn failure_exhausted_retries(record: &Record) -> bool {
    let attempt = record
        .metadata
        .get("attempt")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let max_attempts = record
        .metadata
        .get("max_attempts")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(1);

    attempt.saturating_add(1) >= max_attempts
}

#[cfg(test)]
mod tests;
