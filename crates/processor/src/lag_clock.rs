//! Shared per-node replication-lag clock.
//!
//! The partition processor stamps `last_applied_record_ts_ms` on every
//! record it tails. The service reads it on each `list_runs` call and
//! reports `now − last_applied_ts` as `replication_lag_ms` on the
//! response.
//!
//! The runs list is eventually consistent (replication lag — typically a few ms in
//! healthy HA), and clients render a staleness banner above some
//! threshold instead of the runtime trying to hide the lag.
//!
//! # Why a shared atomic vs. an OTel gauge read
//!
//! OTel gauges write-only; reading back the last value isn't part of
//! the API. Wrapping a shared `AtomicI64` keeps the read path
//! lock-free (single load) and side-steps a metrics-registry round
//! trip per RPC. The OTel `processor_lag` gauges are a different
//! signal: per-partition lag behind the segment tail, sampled on a
//! timer (see `processor_lag`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Wall-clock timestamp (ms since epoch) of the most recent record
/// the local node has applied. `0` means "no record applied yet" and
/// is the safe default — callers report `replication_lag_ms = 0`
/// rather than `now_ms` (which would alarm) until the first record
/// lands.
#[derive(Clone, Default, Debug)]
pub struct LagClock(Arc<AtomicI64>);

impl LagClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the last-applied timestamp. Called by the partition
    /// processor after a record is projected. `record_ts_ms` is the
    /// record's own timestamp (not `now`), so the lag the service
    /// computes reflects "how stale the data actually is" rather than
    /// "when did the last tick happen."
    ///
    /// Uses `Relaxed` ordering: the value is monotonically increasing
    /// in the common case, and a single read returning a slightly
    /// stale value is harmless (the next call updates it). No CAS,
    /// no memory fencing.
    pub fn record_applied(&self, record_ts_ms: i64) {
        self.0.store(record_ts_ms, Ordering::Relaxed);
    }

    /// Read the last-applied timestamp. Caller computes
    /// `now_ms - last_applied_ms` to get the lag. Returns `0` when no
    /// record has been applied yet.
    pub fn last_applied_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Compute lag against `now_ms`. Returns `0` when the clock is
    /// unset (no records applied yet) or when `now_ms` is somehow
    /// behind the recorded timestamp (clock skew).
    pub fn lag_ms(&self, now_ms: i64) -> i64 {
        let last = self.last_applied_ms();
        if last == 0 {
            return 0;
        }
        (now_ms - last).max(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_lag_is_zero() {
        let clock = LagClock::new();
        assert_eq!(clock.last_applied_ms(), 0);
        assert_eq!(clock.lag_ms(1_000_000), 0);
    }

    #[test]
    fn lag_tracks_now_minus_last_applied() {
        let clock = LagClock::new();
        clock.record_applied(100_000);
        assert_eq!(clock.lag_ms(150_000), 50_000);
    }

    #[test]
    fn lag_clamps_clock_skew_to_zero() {
        // Defensive: if the recorded ts is somehow ahead of `now`
        // (clocks disagreed across processes), the lag clamps to 0
        // rather than wrapping into a huge value.
        let clock = LagClock::new();
        clock.record_applied(200_000);
        assert_eq!(clock.lag_ms(100_000), 0);
    }

    #[test]
    fn last_applied_overwrite_takes_latest_value() {
        // Producers may stamp ts out of order in tests; the clock
        // simply takes the latest store. Callers don't depend on
        // monotonicity (lag computation is `now - last`, where any
        // stale `last` just over-estimates lag — a safe direction).
        let clock = LagClock::new();
        clock.record_applied(100);
        clock.record_applied(50);
        assert_eq!(clock.last_applied_ms(), 50);
    }
}
