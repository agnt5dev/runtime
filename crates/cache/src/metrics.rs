//! Cache-level metrics (lazy-initialized via OnceLock).
//!
//! `crates/cache/` had zero metrics before this; the engine-level
//! `agnt5.runtime.engine.cache.lookups` counter measures hit/miss but says
//! nothing about cache *contents* or *eviction pressure*. These metrics
//! expose the gauges and counters needed to answer "is the cache healthy?"

use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry::KeyValue;

pub fn cache_metrics() -> &'static CacheMetrics {
    static METRICS: OnceLock<CacheMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = opentelemetry::global::meter("agnt5.runtime");
        CacheMetrics {
            bytes: meter
                .u64_gauge("agnt5.runtime.cache.bytes")
                .with_description("Approximate current memory usage of the record cache in bytes.")
                .with_unit("By")
                .build(),
            run_count: meter
                .u64_gauge("agnt5.runtime.cache.run_count")
                .with_description("Number of distinct (project_id, run_id) entries currently cached.")
                .build(),
            evictions: meter
                .u64_counter("agnt5.runtime.cache.evictions")
                .with_description(
                    "Cache entries evicted, labeled by `reason ∈ {memory_pressure, completion_ttl, manual}`. \
                     Memory_pressure means LRU eviction kicked in; completion_ttl means a finished run \
                     aged out; manual is operator-/projection-driven removal.",
                )
                .build(),
            tail_subscribers: meter
                .u64_gauge("agnt5.runtime.cache.tail_subscribers")
                .with_description(
                    "Active per-run Tail subscribers. High values + low cache hit ratio suggests \
                     subscribers are not draining as fast as records arrive.",
                )
                .build(),
            notifier_wakeups: meter
                .u64_counter("agnt5.runtime.cache.notifier_wakeups")
                .with_description(
                    "Total Tail-subscriber notifications fired. Pairs with `tail_subscribers` to \
                     compute average notifications per subscriber per window.",
                )
                .build(),
        }
    })
}

pub struct CacheMetrics {
    pub bytes: Gauge<u64>,
    pub run_count: Gauge<u64>,
    pub evictions: Counter<u64>,
    pub tail_subscribers: Gauge<u64>,
    pub notifier_wakeups: Counter<u64>,
}

/// Record an eviction with a closed-enum reason.
pub fn record_eviction(reason: &'static str) {
    cache_metrics()
        .evictions
        .add(1, &[KeyValue::new("reason", reason)]);
}

/// Sample the cache size + run-count gauges. Called on every mutation
/// (put/remove/evict). Cheap — `DashMap::len()` is O(1).
pub fn record_size(bytes: u64, run_count: u64) {
    let m = cache_metrics();
    m.bytes.record(bytes, &[]);
    m.run_count.record(run_count, &[]);
}

pub fn record_subscribers(count: u64) {
    cache_metrics().tail_subscribers.record(count, &[]);
}

pub fn record_notify() {
    cache_metrics().notifier_wakeups.add(1, &[]);
}
