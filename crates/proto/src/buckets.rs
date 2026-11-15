//! Shared explicit-bucket-boundary sets for OTel histograms.
//!
//! OTel Rust's default histogram boundaries (`0, 5, 10, 25, 50, 75, 100,
//! 250, 500, 750, 1000, 2500, 5000, 7500, 10000` ms) badly miss two
//! operational regimes:
//!
//! - **Sub-ms tails** — RocksDB gets, cache lookups, in-process dispatches
//!   often complete in 100-500µs. The default puts everything <5ms in one
//!   bucket, hiding latency improvements.
//! - **>1s outliers** — peer-replication tail, processor catch-up, S3
//!   reads under stress can extend to seconds. The default tops out at
//!   10s with no resolution beyond.
//!
//! Two boundary sets:
//! - [`FAST_LATENCY_MS`] — anything that should normally complete in <10ms
//!   (operation latencies, in-process dispatches, cache lookups).
//! - [`SLOW_LATENCY_MS`] — anything that takes seconds-to-minutes
//!   (compactions, snapshots, recovery, full S3 round-trips).
//!
//! Both are in milliseconds so they pair with `with_unit("ms")`.

/// Bucket boundaries (ms) for fast latency histograms — sub-ms resolution
/// at the bottom, exponential growth up to ~30s.
pub const FAST_LATENCY_MS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10000.0, 30000.0,
];

/// Bucket boundaries (ms) for slow latency histograms — coarser resolution
/// at the bottom, extending to ~10 minutes.
pub const SLOW_LATENCY_MS: &[f64] = &[
    100.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0, 120000.0, 300000.0, 600000.0,
];

/// Convenience: the boundary slice as an owned `Vec<f64>`. The OTel builder
/// takes `Vec<f64>`, not `&[f64]`, so call sites use this to avoid
/// re-typing the array.
pub fn fast_latency_ms() -> Vec<f64> {
    FAST_LATENCY_MS.to_vec()
}

/// Fast-latency boundaries converted to seconds for legacy metrics whose
/// stable names and units predate the shared millisecond convention.
pub fn fast_latency_seconds() -> Vec<f64> {
    FAST_LATENCY_MS
        .iter()
        .map(|milliseconds| milliseconds / 1_000.0)
        .collect()
}

/// Owned variant of [`SLOW_LATENCY_MS`].
pub fn slow_latency_ms() -> Vec<f64> {
    SLOW_LATENCY_MS.to_vec()
}

/// Boundaries for background jobs that can run for hours, such as snapshot
/// publication. The slow scale stops at ten minutes, which hid how long
/// production snapshots really took.
pub const BACKGROUND_JOB_MS: &[f64] = &[
    1000.0, 10000.0, 30000.0, 60000.0, 120000.0, 300000.0, 600000.0, 1200000.0, 1800000.0,
    3600000.0, 7200000.0, 10800000.0, 14400000.0, 21600000.0,
];

/// Owned variant of [`BACKGROUND_JOB_MS`].
pub fn background_job_ms() -> Vec<f64> {
    BACKGROUND_JOB_MS.to_vec()
}

#[cfg(test)]
mod tests {
    #[test]
    fn second_boundaries_preserve_fast_millisecond_resolution() {
        assert_eq!(
            super::fast_latency_seconds()[..4],
            [0.0001, 0.00025, 0.0005, 0.001]
        );
        assert_eq!(super::fast_latency_seconds().last(), Some(&30.0));
    }
}
