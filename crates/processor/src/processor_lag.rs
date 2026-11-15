//! Partition processor lag gauges.
//!
//! Each partition processor reports how far it trails its segment tail, in
//! records (`agnt5.runtime.processor.lag`) and in wall-clock seconds
//! (`agnt5.runtime.processor.lag.seconds`). It samples both on a timer rather
//! than when it applies records. A gauge keeps its last value until the next
//! sample, so a sample taken only while records flow holds a restart's
//! catch-up lag for as long as a quiet partition takes to see more records,
//! and never moves on a processor that has stopped applying records.

use std::sync::Arc;
use std::time::Duration;

use engine_core::traits::Segment;
use opentelemetry::KeyValue;

/// How often each partition processor samples its lag. Shorter than the
/// 15-second metric export interval, so every export carries a fresh value.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LagSample {
    /// Records in the segment that the processor has not applied yet.
    pub records: u64,
    /// Zero once the processor has applied every record in the segment;
    /// otherwise the age of the newest record it has applied. `None` when the
    /// processor is behind but has applied nothing in this attempt, so there
    /// is no record to measure from.
    pub seconds: Option<u64>,
}

/// Compute lag from the segment tail (one past the newest record) and the
/// local offset of the next record the processor will apply.
///
/// While the processor is behind, the age of the newest applied record is an
/// upper bound on how stale its projections are, and it keeps growing if the
/// processor stops applying records.
pub(crate) fn lag_sample(
    tail: u64,
    next_offset: u64,
    last_applied_timestamp_ns: Option<i64>,
    now_ns: i64,
) -> LagSample {
    let records = tail.saturating_sub(next_offset);
    let seconds = if records == 0 {
        Some(0)
    } else {
        last_applied_timestamp_ns
            .map(|timestamp_ns| (now_ns.saturating_sub(timestamp_ns).max(0) / 1_000_000_000) as u64)
    };
    LagSample { records, seconds }
}

/// Read the partition's tail and record both lag gauges. A failed tail read
/// skips this sample; the next tick tries again.
pub(crate) async fn record_lag_sample(
    segment: &Arc<dyn Segment>,
    next_offset: u64,
    last_applied_timestamp_ns: Option<i64>,
    attrs: &[KeyValue],
) {
    let Ok(tail) = segment.tail_offset().await else {
        return;
    };
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let sample = lag_sample(tail.0, next_offset, last_applied_timestamp_ns, now_ns);
    let metrics = crate::metrics::processor_metrics();
    metrics.processor_lag.record(sample.records, attrs);
    if let Some(seconds) = sample.seconds {
        metrics.processor_lag_seconds.record(seconds, attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_NS: i64 = 1_000_000_000;

    #[test]
    fn caught_up_processor_reports_zero_lag_however_old_its_last_record() {
        // A processor that restarted, caught up on a backlog, and then saw
        // a quiet partition: its newest applied record is 395 seconds old,
        // but it has applied everything there is.
        let now = 10_000 * SECOND_NS;
        let sample = lag_sample(4_590_702, 4_590_702, Some(now - 395 * SECOND_NS), now);
        assert_eq!(
            sample,
            LagSample {
                records: 0,
                seconds: Some(0),
            }
        );
    }

    #[test]
    fn processor_behind_the_tail_reports_the_age_of_its_newest_record() {
        let now = 10_000 * SECOND_NS;
        let sample = lag_sample(150, 100, Some(now - 42 * SECOND_NS), now);
        assert_eq!(
            sample,
            LagSample {
                records: 50,
                seconds: Some(42),
            }
        );
    }

    #[test]
    fn stalled_processor_reports_growing_lag() {
        let applied_at = 10_000 * SECOND_NS;
        let first = lag_sample(150, 100, Some(applied_at), applied_at + 10 * SECOND_NS);
        let later = lag_sample(150, 100, Some(applied_at), applied_at + 400 * SECOND_NS);
        assert_eq!(first.seconds, Some(10));
        assert_eq!(later.seconds, Some(400));
    }

    #[test]
    fn processor_behind_with_nothing_applied_leaves_seconds_unknown() {
        let sample = lag_sample(150, 100, None, 10_000 * SECOND_NS);
        assert_eq!(
            sample,
            LagSample {
                records: 50,
                seconds: None,
            }
        );
    }

    #[test]
    fn record_timestamp_ahead_of_local_clock_reports_zero_seconds() {
        let now = 10_000 * SECOND_NS;
        let sample = lag_sample(150, 100, Some(now + 5 * SECOND_NS), now);
        assert_eq!(sample.seconds, Some(0));
    }

    #[test]
    fn next_offset_past_the_tail_reports_zero_records() {
        let sample = lag_sample(100, 120, None, 10_000 * SECOND_NS);
        assert_eq!(sample.records, 0);
        assert_eq!(sample.seconds, Some(0));
    }
}
