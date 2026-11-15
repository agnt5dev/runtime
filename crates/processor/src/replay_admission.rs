//! How many partitions may replay at once.
//!
//! Production runs startup replay at one partition because eight concurrent
//! replays OOM-killed a 19.5 GiB container. That number is a scar rather than a
//! measurement: it is correct for the worst backlog anyone has hit and wrong for
//! every other boot, where seven partitions wait on one for no reason.
//!
//! The runtime already samples memory every tick and already gates replay on a
//! semaphore. The two were never connected — the monitor warned at 70% and 90%
//! and fed back into nothing. This connects them: admit another partition while
//! there is measured headroom, shed one under pressure, and never move outside
//! the floor and ceiling.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tracing::info;

/// Grow while the used-memory ratio is below this. Well under the monitor's
/// 70% warning, so growth stops long before anything looks unhealthy.
const GROW_BELOW_RATIO: f64 = 0.60;

/// Shed at or above this. Below the monitor's 90% critical line, because the
/// point is to avoid reaching it rather than to react once it is hit.
const SHED_AT_RATIO: f64 = 0.80;

/// Governs the startup replay semaphore against measured headroom.
///
/// Cheap to clone; every clone drives the same semaphore.
#[derive(Clone)]
pub struct ReplayAdmission {
    inner: Arc<Inner>,
}

struct Inner {
    permits: Arc<Semaphore>,
    /// Never shed below this. A replay that admits nobody never finishes, so
    /// the floor is what keeps shedding from becoming a deadlock.
    floor: usize,
    /// Never admit above this however much headroom there appears to be.
    ceiling: usize,
    granted: AtomicUsize,
    peak: AtomicUsize,
}

impl ReplayAdmission {
    /// `initial` is the statically configured concurrency, so behaviour is
    /// unchanged until a measurement says otherwise.
    pub fn new(permits: Arc<Semaphore>, initial: usize, ceiling: usize) -> Self {
        let floor = 1.max(initial.min(ceiling));
        let granted = initial.clamp(floor, 1.max(ceiling));
        Self {
            inner: Arc::new(Inner {
                permits,
                floor: floor.min(granted),
                ceiling: ceiling.max(granted),
                granted: AtomicUsize::new(granted),
                peak: AtomicUsize::new(granted),
            }),
        }
    }

    /// Feed one memory sample in. `ratio` is used bytes over the container
    /// limit; `None` when the limit is unknown, which is not a reason to
    /// assume headroom.
    pub fn observe(&self, ratio: Option<f64>) {
        let Some(ratio) = ratio else { return };
        let granted = self.inner.granted.load(Ordering::Acquire);
        if ratio < GROW_BELOW_RATIO && granted < self.inner.ceiling {
            self.inner.permits.add_permits(1);
            let now = granted + 1;
            self.inner.granted.store(now, Ordering::Release);
            self.inner.peak.fetch_max(now, Ordering::AcqRel);
            info!(
                replay_concurrency = now,
                memory_ratio = ratio,
                "Admitted another partition to startup replay"
            );
        } else if ratio >= SHED_AT_RATIO && granted > self.inner.floor {
            // Only permits that are currently free can be taken back, so under
            // full utilisation this sheds as replays finish rather than
            // interrupting one part-way. Interrupting is not on the table: a
            // half-replayed partition is not a state worth having.
            let removed = self.inner.permits.forget_permits(1);
            if removed > 0 {
                let now = granted.saturating_sub(removed);
                self.inner.granted.store(now, Ordering::Release);
                info!(
                    replay_concurrency = now,
                    memory_ratio = ratio,
                    "Shed a startup replay slot under memory pressure"
                );
            }
        }
    }

    /// Slots currently admitted.
    pub fn granted(&self) -> usize {
        self.inner.granted.load(Ordering::Acquire)
    }

    /// The most slots ever admitted, for the boot evidence record.
    pub fn peak(&self) -> usize {
        self.inner.peak.load(Ordering::Acquire)
    }

    pub fn floor(&self) -> usize {
        self.inner.floor
    }

    pub fn ceiling(&self) -> usize {
        self.inner.ceiling
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(initial: usize, ceiling: usize) -> ReplayAdmission {
        ReplayAdmission::new(Arc::new(Semaphore::new(initial)), initial, ceiling)
    }

    #[test]
    fn headroom_admits_more_partitions_up_to_the_ceiling() {
        let admission = admission(1, 8);
        for _ in 0..20 {
            admission.observe(Some(0.10));
        }
        assert_eq!(admission.granted(), 8, "growth must stop at the ceiling");
        assert_eq!(admission.peak(), 8);
    }

    #[test]
    fn pressure_sheds_but_never_below_the_floor() {
        let admission = admission(1, 8);
        for _ in 0..10 {
            admission.observe(Some(0.10));
        }
        assert!(admission.granted() > 1);
        for _ in 0..50 {
            admission.observe(Some(0.95));
        }
        assert_eq!(
            admission.granted(),
            admission.floor(),
            "shedding past the floor would leave replay admitting nobody"
        );
        assert!(admission.floor() >= 1);
    }

    #[test]
    fn the_band_between_the_thresholds_holds_steady() {
        let admission = admission(2, 8);
        let before = admission.granted();
        for _ in 0..20 {
            admission.observe(Some(0.70));
        }
        assert_eq!(
            admission.granted(),
            before,
            "a sample between the thresholds is not a reason to move"
        );
    }

    #[test]
    fn an_unknown_limit_is_not_treated_as_headroom() {
        let admission = admission(1, 8);
        for _ in 0..20 {
            admission.observe(None);
        }
        assert_eq!(
            admission.granted(),
            1,
            "no measurement means no basis for admitting more"
        );
    }

    #[test]
    fn admitted_slots_are_really_available_on_the_semaphore() {
        let permits = Arc::new(Semaphore::new(1));
        let admission = ReplayAdmission::new(permits.clone(), 1, 4);
        admission.observe(Some(0.10));
        admission.observe(Some(0.10));
        assert_eq!(admission.granted(), 3);
        assert_eq!(
            permits.available_permits(),
            3,
            "the counter and the semaphore must not drift"
        );
    }
}
