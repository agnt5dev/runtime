//! Claim registry — shared in-memory state for the pull-based job queue.
//!
//! When workers poll for jobs via parked `PollJob`, we need SKIP LOCKED semantics
//! so concurrent pollers get distinct runs. The engine's job queue is backed
//! by RocksDB (the CF_RUNS_PENDING column family) and does not natively
//! provide "claim and skip" — instead we layer an in-memory claim map on top.
//!
//! Both the engine gRPC handler (which implements `PollJob`/`CompleteJob`)
//! and the coordinator's background claim reaper share one `Arc<ClaimRegistry>`
//! so they see the same set of active claims. The registry is process-local;
//! in multi-node deployments each node claims its own subset of runs, which
//! is fine because the pending-runs index is per-tenant+component and work
//! can be picked up by any healthy worker.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// One entry in the claim map: who owns the run and when it will expire.
#[derive(Debug)]
pub struct ClaimEntry {
    pub worker_id: String,
    pub claimed_at: Instant,
    pub timeout: Duration,
}

/// Thread-safe map of `run_id → ClaimEntry`.
///
/// Cheap to clone via `Arc` — share one instance across the engine handler
/// and the coordinator's reaper task.
#[derive(Debug, Default)]
pub struct ClaimRegistry {
    claims: DashMap<String, ClaimEntry>,
}

impl ClaimRegistry {
    /// Create an empty registry.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            claims: DashMap::new(),
        })
    }

    /// Try to claim a run for a worker. Returns `true` if the claim was
    /// inserted (caller owns the run), `false` if the run is already claimed
    /// (caller must skip it — SKIP LOCKED semantics).
    pub fn try_claim(&self, run_id: &str, worker_id: &str, timeout: Duration) -> bool {
        use dashmap::mapref::entry::Entry;
        match self.claims.entry(run_id.to_string()) {
            Entry::Vacant(e) => {
                e.insert(ClaimEntry {
                    worker_id: worker_id.to_string(),
                    claimed_at: Instant::now(),
                    timeout,
                });
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Release a claim (called on `CompleteJob` or expiry).
    pub fn release(&self, run_id: &str) {
        self.claims.remove(run_id);
    }

    /// Release a claim and return the removed entry.
    pub fn release_entry(&self, run_id: &str) -> Option<ClaimEntry> {
        self.claims.remove(run_id).map(|(_, entry)| entry)
    }

    /// Check whether a run is currently claimed.
    pub fn is_claimed(&self, run_id: &str) -> bool {
        self.claims.contains_key(run_id)
    }

    /// Number of active claims (for metrics / debugging).
    pub fn len(&self) -> usize {
        self.claims.len()
    }

    /// Returns true when the registry has no active claims.
    pub fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }

    /// Expire claims whose `claimed_at + timeout` is in the past.
    /// Returns the number of claims that were removed.
    pub fn expire_stale(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<String> = self
            .claims
            .iter()
            .filter(|e| now.duration_since(e.value().claimed_at) > e.value().timeout)
            .map(|e| e.key().clone())
            .collect();
        let count = expired.len();
        for run_id in expired {
            self.claims.remove(&run_id);
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_then_release() {
        let reg = ClaimRegistry::new();
        assert!(reg.try_claim("r1", "w1", Duration::from_secs(60)));
        assert!(reg.is_claimed("r1"));
        assert_eq!(reg.len(), 1);

        reg.release("r1");
        assert!(!reg.is_claimed("r1"));
        assert!(reg.is_empty());
    }

    #[test]
    fn skip_locked_semantics() {
        let reg = ClaimRegistry::new();
        assert!(reg.try_claim("r1", "w1", Duration::from_secs(60)));
        // Second claim attempt from a different worker returns false.
        assert!(!reg.try_claim("r1", "w2", Duration::from_secs(60)));
    }

    #[test]
    fn expire_stale_removes_timed_out_claims() {
        let reg = ClaimRegistry::new();
        // Claim with a zero-duration timeout so it expires immediately.
        assert!(reg.try_claim("r1", "w1", Duration::from_nanos(1)));
        // Sleep a nanosecond so the claim is stale.
        std::thread::sleep(Duration::from_millis(1));

        let expired = reg.expire_stale();
        assert_eq!(expired, 1);
        assert!(!reg.is_claimed("r1"));
    }
}
