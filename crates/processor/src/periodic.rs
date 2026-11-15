//! Shared scaffolding for the processor's background periodic loops.
//!
//! `query_archive_drain`, `query_archive_compaction`, `runs_retention`, and
//! `retention` all follow the same shape:
//!
//! 1. `tokio::time::interval` with `MissedTickBehavior::Skip`
//!    (avoid catch-up bursts on resource pressure).
//! 2. Skip the first immediate tick (don't sweep on startup).
//! 3. `tokio::select!` between `cancel.cancelled()` and `interval.tick()`.
//! 4. (optional) Leader check before doing work.
//!
//! Without a helper, that scaffolding is copy-pasted into every
//! background module. `await_next_active_tick` extracts only the
//! hard part — combining cancel, tick, and leader gating into a
//! single suspension point — so each loop's body stays visible
//! and can have its own metrics / error handling.

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Build a tokio interval with the conventions the processor's
/// periodic loops use: skip missed ticks (no catch-up bursts) and
/// pre-tick once so the first iteration of the caller's loop only
/// fires after a full `period` has elapsed.
pub fn build_periodic_interval(period: Duration) -> Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

/// Wait for the next active tick of `interval`, observing
/// `cancel` and (optionally) `leader_rx`.
///
/// Returns `true` when the caller should perform a work pass.
/// Returns `false` when the loop should exit cleanly (cancellation).
///
/// When `leader_rx` is `Some` and the current value is `false`,
/// the helper logs a `not leader` debug line and waits for the
/// next tick — the caller doesn't see "non-leader ticks" at all.
///
/// Usage:
/// ```ignore
/// let mut interval = build_periodic_interval(Duration::from_secs(3600));
/// interval.tick().await; // skip first
/// while await_next_active_tick(
///     "compaction_scheduler",
///     &mut interval,
///     &cancel,
///     Some(&leader_rx),
/// )
/// .await
/// {
///     // do work
/// }
/// ```
pub async fn await_next_active_tick(
    loop_name: &str,
    interval: &mut Interval,
    cancel: &CancellationToken,
    leader_rx: Option<&watch::Receiver<bool>>,
) -> bool {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return false,
            _ = interval.tick() => {
                if let Some(rx) = leader_rx {
                    if !*rx.borrow() {
                        debug!(loop_name, "not leader, skipping tick");
                        continue;
                    }
                }
                return true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn await_next_active_tick_returns_false_on_cancel() {
        let mut interval = build_periodic_interval(Duration::from_secs(3600));
        interval.tick().await; // skip first
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let task = tokio::spawn(async move {
            await_next_active_tick("test", &mut interval, &cancel_clone, None).await
        });
        // Cancel before the next tick fires (long interval).
        cancel.cancel();
        let did_tick = task.await.unwrap();
        assert!(!did_tick, "cancel must yield false");
    }

    #[tokio::test]
    async fn await_next_active_tick_returns_true_when_leader() {
        let mut interval = build_periodic_interval(Duration::from_millis(10));
        interval.tick().await; // skip first
        let cancel = CancellationToken::new();
        let (_tx, leader_rx) = watch::channel(true);
        let did_tick =
            await_next_active_tick("test", &mut interval, &cancel, Some(&leader_rx)).await;
        assert!(did_tick, "active leader tick must yield true");
    }

    #[tokio::test]
    async fn await_next_active_tick_skips_when_not_leader() {
        // With leader=false, ticks are silently skipped. The helper
        // returns true only when the caller should do work — so
        // toggling to leader=true after some skipped ticks must
        // produce a single `true` return.
        let mut interval = build_periodic_interval(Duration::from_millis(20));
        interval.tick().await; // skip first
        let cancel = CancellationToken::new();
        let (leader_tx, leader_rx) = watch::channel(false);
        let cancel_clone = cancel.clone();
        let task = tokio::spawn(async move {
            await_next_active_tick("test", &mut interval, &cancel_clone, Some(&leader_rx)).await
        });
        // Wait through several non-leader ticks to confirm we
        // haven't returned early.
        tokio::time::sleep(Duration::from_millis(80)).await;
        // Flip to leader; the helper should return true on the next tick.
        leader_tx.send(true).unwrap();
        let did_tick = task.await.unwrap();
        assert!(did_tick, "promotion to leader must yield a tick");
    }
}
