//! Pull-worker sessions, assignment fencing, and waiter registries.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine_core::demand::DemandKey;
use engine_core::lease_fence::{
    worker_session_hash as hash_worker_session, worker_session_signing_key,
};
use engine_core::types::Offset;
#[cfg(test)]
use engine_core::worker_session::verify_worker_session_token_claims;
use engine_core::worker_session::{
    encode_worker_session_token, verify_worker_session_token, WorkerSessionClaims,
    WorkerSessionSlotScalingClaims,
};
use engine_proto::WorkerSlotPolicy;

use super::{now_ms, DEFAULT_WORKER_SESSION_TTL_MS};

fn decode_worker_session_token(worker_id: &str, token: &str) -> Option<WorkerSession> {
    let payload =
        verify_worker_session_token(worker_id, token, &worker_session_signing_key(), now_ms())
            .ok()?;

    let slot_scaling = payload.slot_scaling;
    Some(WorkerSession {
        session_id: token.to_string(),
        routing_id: payload.routing_id,
        registered_locally: false,
        worker_id: payload.worker_id,
        project_id: payload.project_id,
        deployment_id: payload.deployment_id,
        issued_at_ms: payload.issued_at_ms,
        expires_at_ms: payload.expires_at_ms,
        route_components: Vec::new(),
        slot_policy: slot_scaling
            .as_ref()
            .map_or_else(default_slot_policy, |claims| claims.to_policy()),
        server_slot_scaling: slot_scaling.is_some(),
    })
}

fn default_slot_policy() -> WorkerSlotPolicy {
    WorkerSlotPolicy {
        min_slots: 1,
        max_slots: 100,
        target_cpu_usage: 0.75,
        target_memory_usage: 0.80,
        ramp_throttle_ms: 1_000,
    }
}

pub(super) fn worker_session_hash(session_id: &str) -> String {
    hash_worker_session(session_id)
}

#[cfg(test)]
pub(super) fn worker_session_routing_id(session_id: &str) -> Option<String> {
    verify_worker_session_token_claims(session_id, &worker_session_signing_key(), now_ms())
        .ok()
        .map(|payload| payload.routing_id)
}

pub(super) fn lease_expiry_is_active(lease_expires_at_ms: i64) -> bool {
    lease_expires_at_ms <= 0 || now_ms() < lease_expires_at_ms
}

#[derive(Clone, Debug)]
pub(super) struct AssignedLease {
    pub(super) lease_id: String,
    pub(super) lease_expires_at_ms: i64,
    pub(super) lease_timeout_ms: i64,
}

/// Authoritative result of committing one lease assignment to the journal.
///
/// The receipt is returned only after the active storage profile has accepted
/// the strict `run.assigned` idempotency slot for this dispatch generation.
/// Callers therefore do not need to wait for a materialized Run projection to
/// repeat the same decision.
#[derive(Clone, Debug)]
pub(super) struct CommittedAssignmentReceipt {
    pub(super) lease: AssignedLease,
    pub(super) accepted_offset: Offset,
    pub(super) appended: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CachedLease {
    pub(super) lease_expires_at_ms: i64,
}

#[derive(Clone, Debug)]
pub(super) struct WorkerSession {
    pub(super) session_id: String,
    /// Sortable UUIDv7 registration fence published through gossip. The signed
    /// API token remains opaque to the routing layer.
    pub(super) routing_id: String,
    /// Only the runtime that handled registration may revive a missing or
    /// tombstoned route. Sessions adopted from another HA node must continue to
    /// match the shared gossip route on every request.
    pub(super) registered_locally: bool,
    pub(super) worker_id: String,
    pub(super) project_id: String,
    pub(super) deployment_id: String,
    pub(super) issued_at_ms: i64,
    pub(super) expires_at_ms: i64,
    /// Component inventory captured at registration. This is authoritative in
    /// the single-process profile; managed cross-node adoption can recover
    /// it from the gossip route when a signed session token lands elsewhere.
    pub(super) route_components: Vec<engine_proto::ComponentRouteSpec>,
    pub(super) slot_policy: WorkerSlotPolicy,
    // Read only by the hosted runtime's slot scaling.
    #[allow(dead_code)]
    pub(super) server_slot_scaling: bool,
}

#[derive(Default)]
pub(super) struct WorkerSessionRegistry {
    inner: Mutex<WorkerSessionRegistryInner>,
}

#[derive(Default)]
struct WorkerSessionRegistryInner {
    by_session: HashMap<String, WorkerSession>,
    active_by_worker: HashMap<String, String>,
    revoked_sessions: HashSet<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PollJobWaiterKey {
    project_id: String,
    deployment_id: String,
}

type ComponentProbeDeadlines = HashMap<BTreeSet<DemandKey>, Instant>;

#[derive(Default)]
pub(super) struct PollJobWaiterRegistry {
    inner: Mutex<HashMap<PollJobWaiterKey, Vec<Arc<PollJobWaiterSignal>>>>,
    next_probe_at: Mutex<HashMap<PollJobWaiterKey, ComponentProbeDeadlines>>,
    delegated_ready: Mutex<HashMap<PollJobWaiterKey, Vec<engine_proto::PullReadyDelegation>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PullWakeSource {
    Unknown,
    RequestAppend,
    Projection,
    Delegation,
    BacklogFanout,
    SessionChange,
}

impl PullWakeSource {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::RequestAppend => "request_append",
            Self::Projection => "projection",
            Self::Delegation => "delegation",
            Self::BacklogFanout => "backlog_fanout",
            Self::SessionChange => "session_change",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PullWakeContext {
    pub(super) source: PullWakeSource,
    pub(super) notified_at: Instant,
    pub(super) queued_run_id: Option<String>,
    pub(super) queued_at_ms: Option<i64>,
    pub(super) delegation: Option<engine_proto::PullReadyDelegation>,
}

impl PullWakeContext {
    pub(super) fn queue_event(source: PullWakeSource, run_id: &str, queued_at_ms: i64) -> Self {
        Self {
            source,
            notified_at: Instant::now(),
            queued_run_id: (!run_id.is_empty()).then(|| run_id.to_string()),
            queued_at_ms: (queued_at_ms > 0).then_some(queued_at_ms),
            delegation: None,
        }
    }

    pub(super) fn delegated(delegation: engine_proto::PullReadyDelegation) -> Self {
        Self {
            source: PullWakeSource::Delegation,
            notified_at: Instant::now(),
            queued_run_id: Some(delegation.run_id.clone()),
            queued_at_ms: (delegation.ready_at_ms > 0).then_some(delegation.ready_at_ms),
            delegation: Some(delegation),
        }
    }

    pub(super) fn control(source: PullWakeSource) -> Self {
        Self {
            source,
            notified_at: Instant::now(),
            queued_run_id: None,
            queued_at_ms: None,
            delegation: None,
        }
    }
}

#[derive(Default)]
struct PollJobWaiterSignal {
    notify: tokio::sync::Notify,
    context: Mutex<Option<PullWakeContext>>,
}

pub(super) struct PollJobWaiter {
    registry: Arc<PollJobWaiterRegistry>,
    key: PollJobWaiterKey,
    signal: Arc<PollJobWaiterSignal>,
}

const POLL_ATTEMPT_UNRESERVED: u8 = 0;
const POLL_ATTEMPT_RESERVED: u8 = 1;
const POLL_ATTEMPT_FINISHED: u8 = 2;

/// Registry for live public PollJob requests.
///
/// The guard is the ownership unit: dropping a request future removes its
/// attempt, while explicit terminal paths may finish it early. The atomic
/// state makes reservation and completion mutually exclusive with shutdown.
#[derive(Default)]
pub(super) struct PollAttemptRegistry {
    inner: Mutex<PollAttemptRegistryInner>,
    interest_publisher: Mutex<Option<tokio::sync::watch::Sender<BTreeSet<DemandKey>>>>,
    shutting_down: AtomicBool,
    completed: AtomicU64,
}

#[derive(Default)]
struct PollAttemptRegistryInner {
    attempts: HashMap<String, Arc<PollAttemptEntry>>,
    unreserved_by_key: BTreeMap<DemandKey, usize>,
}

struct PollAttemptEntry {
    state: AtomicU8,
    demand_keys: BTreeSet<DemandKey>,
}

pub(super) struct PollAttempt {
    registry: Arc<PollAttemptRegistry>,
    id: String,
    entry: Arc<PollAttemptEntry>,
}

impl PollAttemptRegistry {
    pub(super) fn register(
        self: &Arc<Self>,
        demand_keys: &BTreeSet<DemandKey>,
    ) -> Option<PollAttempt> {
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        let id = uuid::Uuid::now_v7().to_string();
        let entry = Arc::new(PollAttemptEntry {
            state: AtomicU8::new(POLL_ATTEMPT_UNRESERVED),
            demand_keys: demand_keys.clone(),
        });
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        Self::add_interests(&mut inner.unreserved_by_key, &entry.demand_keys);
        inner.attempts.insert(id.clone(), entry.clone());
        self.publish_interests(&inner);
        Some(PollAttempt {
            registry: self.clone(),
            id,
            entry,
        })
    }

    /// Fence new polls and finish every live attempt. Each guard may still be
    /// dropped by its request future; its atomic Finished state makes that a
    /// no-op rather than a second removal.
    pub(super) fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let attempts = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            inner.unreserved_by_key.clear();
            inner
                .attempts
                .drain()
                .map(|(_, entry)| entry)
                .collect::<Vec<_>>()
        };
        for entry in attempts {
            if entry.state.swap(POLL_ATTEMPT_FINISHED, Ordering::AcqRel) != POLL_ATTEMPT_FINISHED {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(publisher) = self
            .interest_publisher
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            publisher.send_replace(BTreeSet::new());
        }
    }

    fn try_reserve(&self, id: &str, entry: &Arc<PollAttemptEntry>) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !inner
            .attempts
            .get(id)
            .is_some_and(|candidate| Arc::ptr_eq(candidate, entry))
            || entry
                .state
                .compare_exchange(
                    POLL_ATTEMPT_UNRESERVED,
                    POLL_ATTEMPT_RESERVED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return false;
        }
        Self::remove_interests(&mut inner.unreserved_by_key, &entry.demand_keys);
        self.publish_interests(&inner);
        true
    }

    fn release(&self, id: &str, entry: &Arc<PollAttemptEntry>) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !inner
            .attempts
            .get(id)
            .is_some_and(|candidate| Arc::ptr_eq(candidate, entry))
            || entry
                .state
                .compare_exchange(
                    POLL_ATTEMPT_RESERVED,
                    POLL_ATTEMPT_UNRESERVED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return false;
        }
        Self::add_interests(&mut inner.unreserved_by_key, &entry.demand_keys);
        self.publish_interests(&inner);
        true
    }

    fn finish(&self, id: &str, entry: &Arc<PollAttemptEntry>) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let previous = entry.state.swap(POLL_ATTEMPT_FINISHED, Ordering::AcqRel);
        if previous == POLL_ATTEMPT_FINISHED {
            return false;
        }
        if previous == POLL_ATTEMPT_UNRESERVED {
            Self::remove_interests(&mut inner.unreserved_by_key, &entry.demand_keys);
        }
        if inner
            .attempts
            .get(id)
            .is_some_and(|candidate| Arc::ptr_eq(candidate, entry))
        {
            inner.attempts.remove(id);
        }
        self.publish_interests(&inner);
        self.completed.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(super) fn set_interest_publisher(
        &self,
        publisher: tokio::sync::watch::Sender<BTreeSet<DemandKey>>,
    ) {
        let interests = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .unreserved_by_key
            .keys()
            .cloned()
            .collect();
        publisher.send_replace(interests);
        *self
            .interest_publisher
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(publisher);
    }

    fn publish_interests(&self, inner: &PollAttemptRegistryInner) {
        let interests = inner.unreserved_by_key.keys().cloned().collect();
        if let Some(publisher) = self
            .interest_publisher
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            publisher.send_replace(interests);
        }
    }

    fn add_interests(counts: &mut BTreeMap<DemandKey, usize>, keys: &BTreeSet<DemandKey>) {
        for key in keys {
            *counts.entry(key.clone()).or_default() += 1;
        }
    }

    fn remove_interests(counts: &mut BTreeMap<DemandKey, usize>, keys: &BTreeSet<DemandKey>) {
        for key in keys {
            let remove = {
                let Some(count) = counts.get_mut(key) else {
                    debug_assert!(false, "unreserved poll interest count is missing");
                    continue;
                };
                *count = count.saturating_sub(1);
                *count == 0
            };
            if remove {
                counts.remove(key);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn active_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .attempts
            .len()
    }

    #[cfg(test)]
    pub(super) fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn interests(&self) -> BTreeMap<DemandKey, usize> {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .unreserved_by_key
            .clone()
    }
}

impl PollAttempt {
    pub(super) fn try_reserve(&self) -> bool {
        self.registry.try_reserve(&self.id, &self.entry)
    }

    pub(super) fn release(&self) -> bool {
        self.registry.release(&self.id, &self.entry)
    }

    pub(super) fn finish(&mut self) -> bool {
        self.registry.finish(&self.id, &self.entry)
    }
}

impl Drop for PollAttempt {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct QueueMetricRefreshKey {
    pub(super) project_id: String,
    pub(super) deployment_id: String,
    pub(super) priority: Option<u8>,
}

#[derive(Default)]
pub(super) struct QueueMetricRefreshRegistry {
    pub(super) active: Mutex<HashSet<QueueMetricRefreshKey>>,
}

impl Drop for PollJobWaiter {
    fn drop(&mut self) {
        self.registry.unregister(&self.key, &self.signal);
    }
}

impl PollJobWaiter {
    pub(super) async fn notified(&self) -> PullWakeContext {
        self.signal.notify.notified().await;
        self.signal
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .unwrap_or_else(|| PullWakeContext::control(PullWakeSource::Unknown))
    }
}

impl WorkerSessionRegistry {
    #[cfg(test)]
    pub(super) fn register(
        &self,
        worker_id: String,
        project_id: String,
        deployment_id: String,
        slot_policy: WorkerSlotPolicy,
        route_components: Vec<engine_proto::ComponentRouteSpec>,
    ) -> (WorkerSession, Option<WorkerSession>) {
        self.register_with_slot_scaling(
            worker_id,
            project_id,
            deployment_id,
            slot_policy,
            route_components,
            false,
        )
    }

    pub(super) fn register_with_slot_scaling(
        &self,
        worker_id: String,
        project_id: String,
        deployment_id: String,
        slot_policy: WorkerSlotPolicy,
        route_components: Vec<engine_proto::ComponentRouteSpec>,
        server_slot_scaling: bool,
    ) -> (WorkerSession, Option<WorkerSession>) {
        let issued_at_ms = now_ms();
        let expires_at_ms = issued_at_ms.saturating_add(DEFAULT_WORKER_SESSION_TTL_MS);
        let routing_id = uuid::Uuid::now_v7().to_string();
        let session_id = encode_worker_session_token(
            &WorkerSessionClaims {
                routing_id: routing_id.clone(),
                worker_id: worker_id.clone(),
                project_id: project_id.clone(),
                deployment_id: deployment_id.clone(),
                issued_at_ms,
                expires_at_ms,
                slot_scaling: server_slot_scaling
                    .then(|| WorkerSessionSlotScalingClaims::from_policy(&slot_policy)),
            },
            &worker_session_signing_key(),
        )
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let session = WorkerSession {
            session_id,
            routing_id,
            registered_locally: true,
            worker_id,
            project_id,
            deployment_id,
            issued_at_ms,
            expires_at_ms,
            route_components,
            slot_policy,
            server_slot_scaling,
        };
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let replaced = if let Some(old_id) = inner
            .active_by_worker
            .insert(session.worker_id.clone(), session.session_id.clone())
        {
            let old = inner.by_session.remove(&old_id);
            inner.revoked_sessions.insert(old_id);
            old
        } else {
            None
        };
        inner
            .by_session
            .insert(session.session_id.clone(), session.clone());
        (session, replaced)
    }

    pub(super) fn get_current(&self, worker_id: &str, session_id: &str) -> Option<WorkerSession> {
        self.get_current_inner(worker_id, session_id, false)
    }

    pub(super) fn get_current_and_extend(
        &self,
        worker_id: &str,
        session_id: &str,
    ) -> Option<WorkerSession> {
        self.get_current_inner(worker_id, session_id, true)
    }

    fn get_current_inner(
        &self,
        worker_id: &str,
        session_id: &str,
        extend_session: bool,
    ) -> Option<WorkerSession> {
        {
            let now = now_ms();
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.revoked_sessions.contains(session_id) {
                return None;
            }
            if inner
                .active_by_worker
                .get(worker_id)
                .is_some_and(|active| active == session_id)
            {
                let expired = inner
                    .by_session
                    .get(session_id)
                    .map(|session| session.expires_at_ms <= now)
                    .unwrap_or(true);
                if expired {
                    inner.by_session.remove(session_id);
                    inner.active_by_worker.remove(worker_id);
                    return None;
                }
                let session = inner.by_session.get_mut(session_id)?;
                if extend_session {
                    session.expires_at_ms = now.saturating_add(DEFAULT_WORKER_SESSION_TTL_MS);
                }
                return Some(session.clone());
            }
        }

        // In HA, RegisterWorkerSession may land on node A and PollJob may land
        // on node B. Node B can have stale local state for the same worker from
        // an earlier session, so a valid signed token must be accepted unless
        // this node explicitly revoked that exact session ID.
        let mut session = decode_worker_session_token(worker_id, session_id)?;
        if extend_session {
            session.expires_at_ms = now_ms().saturating_add(DEFAULT_WORKER_SESSION_TTL_MS);
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.revoked_sessions.contains(session_id) {
                return None;
            }
            if let Some(old_id) = inner.active_by_worker.get(worker_id).cloned() {
                if old_id != session_id {
                    match inner.by_session.get(&old_id) {
                        Some(active) if active.expires_at_ms <= now_ms() => {
                            inner.by_session.remove(&old_id);
                            inner.active_by_worker.remove(worker_id);
                        }
                        // Match gossip's total ordering: issuance time first,
                        // then the sortable UUIDv7 routing fence. This prevents
                        // an older cross-node token minted in the same
                        // millisecond from displacing the active session.
                        Some(active)
                            if (active.issued_at_ms, active.routing_id.as_str())
                                >= (session.issued_at_ms, session.routing_id.as_str()) =>
                        {
                            return None;
                        }
                        Some(_) => {
                            inner.by_session.remove(&old_id);
                            inner.revoked_sessions.insert(old_id);
                        }
                        None => {
                            inner.active_by_worker.remove(worker_id);
                        }
                    }
                }
            }
            inner
                .active_by_worker
                .insert(worker_id.to_string(), session_id.to_string());
            inner
                .by_session
                .insert(session.session_id.clone(), session.clone());
        }
        Some(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_session_with_empty_routing_fence_is_rejected() {
        let token = encode_worker_session_token(
            &WorkerSessionClaims {
                routing_id: String::new(),
                worker_id: "worker-1".into(),
                project_id: "project-1".into(),
                deployment_id: "deployment-1".into(),
                issued_at_ms: now_ms(),
                expires_at_ms: now_ms().saturating_add(DEFAULT_WORKER_SESSION_TTL_MS),
                slot_scaling: None,
            },
            &worker_session_signing_key(),
        )
        .expect("test token should encode");

        assert!(decode_worker_session_token("worker-1", &token).is_none());
        assert!(worker_session_routing_id(&token).is_none());
    }

    #[test]
    fn get_current_and_extend_refreshes_local_session_expiry() {
        let registry = WorkerSessionRegistry::default();
        let (session, _) = registry.register(
            "worker-1".into(),
            "project-1".into(),
            "deployment-1".into(),
            WorkerSlotPolicy {
                min_slots: 1,
                max_slots: 10,
                target_cpu_usage: 0.75,
                target_memory_usage: 0.80,
                ramp_throttle_ms: 1_000,
            },
            Vec::new(),
        );

        {
            let mut inner = registry.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner
                .by_session
                .get_mut(&session.session_id)
                .expect("session should be registered")
                .expires_at_ms = now_ms().saturating_add(1_000);
        }

        let refreshed = registry
            .get_current_and_extend(&session.worker_id, &session.session_id)
            .expect("active session should refresh");

        assert!(
            refreshed.expires_at_ms
                >= now_ms().saturating_add(DEFAULT_WORKER_SESSION_TTL_MS - 1_000)
        );
    }

    #[test]
    fn get_current_does_not_extend_local_session_expiry() {
        let registry = WorkerSessionRegistry::default();
        let (session, _) = registry.register(
            "worker-1".into(),
            "project-1".into(),
            "deployment-1".into(),
            WorkerSlotPolicy {
                min_slots: 1,
                max_slots: 10,
                target_cpu_usage: 0.75,
                target_memory_usage: 0.80,
                ramp_throttle_ms: 1_000,
            },
            Vec::new(),
        );
        let short_expiry = now_ms().saturating_add(1_000);
        {
            let mut inner = registry.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner
                .by_session
                .get_mut(&session.session_id)
                .expect("session should be registered")
                .expires_at_ms = short_expiry;
        }

        let current = registry
            .get_current(&session.worker_id, &session.session_id)
            .expect("active session should be returned");

        assert_eq!(current.expires_at_ms, short_expiry);
    }

    #[test]
    fn get_current_and_extend_adopts_fallback_token_and_revokes_old_session() {
        let registry = WorkerSessionRegistry::default();
        let (old_session, _) = registry.register(
            "worker-1".into(),
            "project-1".into(),
            "deployment-1".into(),
            WorkerSlotPolicy {
                min_slots: 1,
                max_slots: 10,
                target_cpu_usage: 0.75,
                target_memory_usage: 0.80,
                ramp_throttle_ms: 1_000,
            },
            Vec::new(),
        );
        let fallback_token = encode_worker_session_token(
            &WorkerSessionClaims {
                routing_id: "fallback".into(),
                worker_id: old_session.worker_id.clone(),
                project_id: old_session.project_id.clone(),
                deployment_id: old_session.deployment_id.clone(),
                issued_at_ms: old_session.issued_at_ms.saturating_add(1_000),
                expires_at_ms: now_ms().saturating_add(DEFAULT_WORKER_SESSION_TTL_MS),
                slot_scaling: None,
            },
            &worker_session_signing_key(),
        )
        .expect("fallback token should encode");

        let adopted = registry
            .get_current_and_extend(&old_session.worker_id, &fallback_token)
            .expect("valid fallback token should be adopted");

        assert_eq!(adopted.session_id, fallback_token);
        assert!(
            adopted.expires_at_ms >= now_ms().saturating_add(DEFAULT_WORKER_SESSION_TTL_MS - 1_000)
        );
        assert!(registry
            .get_current(&old_session.worker_id, &old_session.session_id)
            .is_none());
        assert!(registry
            .get_current(&old_session.worker_id, &fallback_token)
            .is_some());
    }

    #[test]
    fn get_current_and_extend_rejects_older_fallback_token_when_newer_session_is_active() {
        let registry = WorkerSessionRegistry::default();
        let (old_session, _) = registry.register(
            "worker-1".into(),
            "project-1".into(),
            "deployment-1".into(),
            WorkerSlotPolicy {
                min_slots: 1,
                max_slots: 10,
                target_cpu_usage: 0.75,
                target_memory_usage: 0.80,
                ramp_throttle_ms: 1_000,
            },
            Vec::new(),
        );
        let (new_session, _) = registry.register(
            "worker-1".into(),
            "project-1".into(),
            "deployment-1".into(),
            WorkerSlotPolicy {
                min_slots: 1,
                max_slots: 10,
                target_cpu_usage: 0.75,
                target_memory_usage: 0.80,
                ramp_throttle_ms: 1_000,
            },
            Vec::new(),
        );
        let old_fallback_token = encode_worker_session_token(
            &WorkerSessionClaims {
                routing_id: "00000000-0000-7000-8000-000000000000".into(),
                worker_id: old_session.worker_id.clone(),
                project_id: old_session.project_id.clone(),
                deployment_id: old_session.deployment_id.clone(),
                issued_at_ms: new_session.issued_at_ms,
                expires_at_ms: now_ms().saturating_add(DEFAULT_WORKER_SESSION_TTL_MS),
                slot_scaling: None,
            },
            &worker_session_signing_key(),
        )
        .expect("fallback token should encode");

        assert!(registry
            .get_current_and_extend(&old_session.worker_id, &old_fallback_token)
            .is_none());
        assert!(registry
            .get_current(&new_session.worker_id, &new_session.session_id)
            .is_some());
    }
}

impl PollJobWaiterRegistry {
    /// Coalesce the HA fallback scan performed by otherwise idle parked polls.
    /// Queue append notifications bypass this gate; it only prevents every
    /// slot with the same component inventory from repeating an empty index
    /// scan. A different inventory can see work the first worker cannot claim.
    pub(super) fn begin_probe(
        &self,
        project_id: &str,
        deployment_id: &str,
        demand_keys: &BTreeSet<DemandKey>,
        interval: Duration,
    ) -> bool {
        let key = PollJobWaiterKey {
            project_id: project_id.to_string(),
            deployment_id: deployment_id.to_string(),
        };
        let now = Instant::now();
        let mut probes = self.next_probe_at.lock().unwrap_or_else(|e| e.into_inner());
        let inventories = probes.entry(key).or_default();
        if inventories.get(demand_keys).is_some_and(|next| *next > now) {
            return false;
        }
        // Component inventories can change during a rolling upgrade. Keep only
        // live coalescing windows so retired inventories do not accumulate.
        inventories.retain(|_, next| *next > now);
        inventories.insert(demand_keys.clone(), now + interval);
        true
    }

    pub(super) fn invalidate_probe(&self, project_id: &str, deployment_id: &str) {
        let key = PollJobWaiterKey {
            project_id: project_id.to_string(),
            deployment_id: deployment_id.to_string(),
        };
        self.next_probe_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
    }

    pub(super) fn notify_delegated(&self, delegation: engine_proto::PullReadyDelegation) -> bool {
        let project_id = delegation.project_id.clone();
        let deployment_id = delegation.deployment_id.clone();
        let run_id = delegation.run_id.clone();
        let context = PullWakeContext::delegated(delegation.clone());
        if self.notify_one(&project_id, &deployment_id, context.clone()) {
            return true;
        }

        let key = PollJobWaiterKey {
            project_id: project_id.clone(),
            deployment_id: deployment_id.clone(),
        };
        {
            let mut ready = self
                .delegated_ready
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let hints = ready.entry(key.clone()).or_default();
            hints.retain(|hint| hint.expires_at_ms >= now_ms() && hint.run_id != delegation.run_id);
            hints.push(delegation);
        }

        // Close the no-waiter -> store race: a poll may have registered while
        // the hint was being retained.
        if self.notify_one(&project_id, &deployment_id, context) {
            let mut ready = self
                .delegated_ready
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(hints) = ready.get_mut(&key) {
                hints.retain(|hint| hint.run_id != run_id);
                if hints.is_empty() {
                    ready.remove(&key);
                }
            }
            true
        } else {
            false
        }
    }

    pub(super) fn take_delegated(
        &self,
        project_id: &str,
        deployment_id: &str,
    ) -> Option<PullWakeContext> {
        let key = PollJobWaiterKey {
            project_id: project_id.to_string(),
            deployment_id: deployment_id.to_string(),
        };
        let mut ready = self
            .delegated_ready
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let hints = ready.get_mut(&key)?;
        hints.retain(|hint| hint.expires_at_ms >= now_ms());
        let delegation = (!hints.is_empty()).then(|| hints.remove(0));
        if hints.is_empty() {
            ready.remove(&key);
        }
        delegation.map(PullWakeContext::delegated)
    }

    pub(super) fn register(
        self: &Arc<Self>,
        project_id: &str,
        deployment_id: &str,
    ) -> PollJobWaiter {
        let key = PollJobWaiterKey {
            project_id: project_id.to_string(),
            deployment_id: deployment_id.to_string(),
        };
        let signal = Arc::new(PollJobWaiterSignal::default());
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.entry(key.clone()).or_default().push(signal.clone());
        PollJobWaiter {
            registry: self.clone(),
            key,
            signal,
        }
    }

    fn unregister(&self, key: &PollJobWaiterKey, signal: &Arc<PollJobWaiterSignal>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(waiters) = inner.get_mut(key) {
            waiters.retain(|candidate| !Arc::ptr_eq(candidate, signal));
            if waiters.is_empty() {
                inner.remove(key);
            }
        }
    }

    pub(super) fn notify_one(
        &self,
        project_id: &str,
        deployment_id: &str,
        context: PullWakeContext,
    ) -> bool {
        self.invalidate_probe(project_id, deployment_id);
        self.notify_n(project_id, deployment_id, 1, context) > 0
    }

    pub(super) fn notify_n(
        &self,
        project_id: &str,
        deployment_id: &str,
        count: usize,
        context: PullWakeContext,
    ) -> usize {
        let key = PollJobWaiterKey {
            project_id: project_id.to_string(),
            deployment_id: deployment_id.to_string(),
        };
        let waiters = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let Some(waiters) = inner.get_mut(&key) else {
                return 0;
            };
            let split_at = waiters.len().saturating_sub(count);
            let selected = waiters.split_off(split_at);
            if waiters.is_empty() {
                inner.remove(&key);
            }
            selected
        };

        let count = waiters.len();
        for waiter in waiters {
            *waiter
                .context
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(context.clone());
            waiter.notify.notify_one();
        }
        count
    }

    pub(super) fn notify_all(
        &self,
        project_id: &str,
        deployment_id: &str,
        source: PullWakeSource,
    ) -> usize {
        self.invalidate_probe(project_id, deployment_id);
        let key = PollJobWaiterKey {
            project_id: project_id.to_string(),
            deployment_id: deployment_id.to_string(),
        };
        let waiters = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.remove(&key).unwrap_or_default()
        };
        let count = waiters.len();
        for waiter in waiters {
            *waiter
                .context
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                Some(PullWakeContext::control(source));
            waiter.notify.notify_one();
        }
        count
    }
}

#[cfg(test)]
mod poll_attempt_tests {
    use super::*;
    use engine_proto::ComponentType;

    fn demand_key(component_name: &str) -> DemandKey {
        DemandKey::new(
            "project-1",
            "deployment-1",
            ComponentType::Function,
            component_name,
        )
        .expect("test demand key should be valid")
    }

    fn demand_keys(names: &[&str]) -> BTreeSet<DemandKey> {
        names.iter().map(|name| demand_key(name)).collect()
    }

    #[test]
    fn poll_attempt_reservation_and_finish_are_atomic() {
        let registry = Arc::new(PollAttemptRegistry::default());
        let mut attempt = registry
            .register(&BTreeSet::new())
            .expect("registry is accepting polls");

        assert_eq!(registry.active_count(), 1);
        assert!(attempt.try_reserve());
        assert!(!attempt.try_reserve());
        assert!(attempt.release());
        assert!(!attempt.release());
        assert!(attempt.try_reserve());
        assert!(attempt.finish());
        assert!(!attempt.finish());
        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.completed_count(), 1);

        drop(attempt);
        assert_eq!(registry.completed_count(), 1, "Drop must not finish twice");
    }

    #[test]
    fn dropping_cancelled_poll_removes_it_once() {
        let registry = Arc::new(PollAttemptRegistry::default());
        let attempt = registry
            .register(&BTreeSet::new())
            .expect("registry is accepting polls");
        assert_eq!(registry.active_count(), 1);

        drop(attempt);

        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.completed_count(), 1);
    }

    #[test]
    fn shutdown_drains_each_poll_once_and_fences_registration() {
        let registry = Arc::new(PollAttemptRegistry::default());
        let first = registry
            .register(&BTreeSet::new())
            .expect("registry is accepting polls");
        let second = registry
            .register(&BTreeSet::new())
            .expect("registry is accepting polls");

        registry.begin_shutdown();
        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.completed_count(), 2);
        assert!(registry.register(&BTreeSet::new()).is_none());

        drop((first, second));
        assert_eq!(registry.completed_count(), 2, "Drop must not finish twice");
    }

    #[test]
    fn overlapping_route_sets_count_each_poll_once_and_reserve_atomically() {
        let registry = Arc::new(PollAttemptRegistry::default());
        let alpha = demand_key("alpha");
        let beta = demand_key("beta");
        let gamma = demand_key("gamma");
        let mut first = registry
            .register(&demand_keys(&["alpha", "beta", "beta"]))
            .expect("registry is accepting polls");
        let second = registry
            .register(&demand_keys(&["beta", "gamma"]))
            .expect("registry is accepting polls");

        assert_eq!(
            registry.interests(),
            BTreeMap::from([(alpha.clone(), 1), (beta.clone(), 2), (gamma.clone(), 1),])
        );

        assert!(first.try_reserve());
        assert_eq!(
            registry.interests(),
            BTreeMap::from([(beta.clone(), 1), (gamma.clone(), 1)])
        );
        assert!(!first.try_reserve());
        assert_eq!(
            registry.interests(),
            BTreeMap::from([(beta.clone(), 1), (gamma.clone(), 1)])
        );

        assert!(second.try_reserve());
        assert!(registry.interests().is_empty());
        assert!(!second.try_reserve());
        assert!(registry.interests().is_empty());

        assert!(first.release());
        assert_eq!(
            registry.interests(),
            BTreeMap::from([(alpha, 1), (beta, 1)])
        );
        assert!(first.finish());
        assert!(registry.interests().is_empty());

        drop(second);
        assert_eq!(registry.completed_count(), 2);
    }

    #[test]
    fn first_and_last_unreserved_poll_drive_interest_lifecycle() {
        let registry = Arc::new(PollAttemptRegistry::default());
        let key = demand_key("shared");
        let keys = BTreeSet::from([key.clone()]);
        let first = registry
            .register(&keys)
            .expect("registry is accepting polls");
        let mut second = registry
            .register(&keys)
            .expect("registry is accepting polls");

        assert_eq!(registry.interests(), BTreeMap::from([(key.clone(), 2)]));
        assert!(first.try_reserve());
        assert_eq!(registry.interests(), BTreeMap::from([(key.clone(), 1)]));
        drop(first);
        assert_eq!(registry.interests(), BTreeMap::from([(key.clone(), 1)]));
        assert!(second.finish());
        assert!(registry.interests().is_empty());
    }

    #[test]
    fn interest_publisher_tracks_only_distinct_unreserved_keys() {
        let registry = Arc::new(PollAttemptRegistry::default());
        let (publisher, receiver) = tokio::sync::watch::channel(BTreeSet::new());
        registry.set_interest_publisher(publisher);
        let alpha = demand_key("alpha");
        let beta = demand_key("beta");
        let first = registry
            .register(&BTreeSet::from([alpha.clone(), beta.clone()]))
            .unwrap();
        let mut second = registry.register(&BTreeSet::from([beta.clone()])).unwrap();

        assert_eq!(
            *receiver.borrow(),
            BTreeSet::from([alpha.clone(), beta.clone()])
        );
        assert!(first.try_reserve());
        assert_eq!(*receiver.borrow(), BTreeSet::from([beta.clone()]));
        drop(first);
        assert_eq!(*receiver.borrow(), BTreeSet::from([beta]));
        assert!(second.finish());
        assert!(receiver.borrow().is_empty());
    }
}
