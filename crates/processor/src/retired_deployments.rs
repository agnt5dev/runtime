//! Cancel queued runs whose deployment is retired for good.
//!
//! Queued runs have no automatic expiry, so a run queued for a deployment
//! whose worker pool is gone would wait, and be retried, forever. On each
//! partition's leader a reconciler periodically collects the deployments that
//! queued runs wait on, from the push dispatch outbox and the route-aware
//! pending index, and asks the control plane which are retired. The answer is
//! fresh on every pass: a deployment that is restarted, or a failed one the
//! operator heals, is never reported retired and keeps its queue.
//!
//! Retired deployments go into a short-lived registry. A push run is
//! cancelled by the dispatch outbox drainer, which owns that run's dispatch,
//! when it next picks the row up, so the cancel can never race a dispatch. A
//! pull run has no drainer and is cancelled by the reconciler. Both cancel
//! through the runtime's normal cancellation path, which also ends the run's
//! durable activations in the same append.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use engine_core::error::EngineResult;
use engine_core::types::{Offset, Record};

use crate::{DISPATCH_OUTBOX_PARTITION_ID_META, DISPATCH_OUTBOX_QUEUED_OFFSET_META};

/// Cancellation reason recorded on runs of retired deployments.
pub const DEPLOYMENT_RETIRED_REASON: &str = "deployment_retired";

/// `(project_id, deployment_id)`. A deployment is always asked about, and
/// answered for, within the project its queued run belongs to.
pub type DeploymentKey = (String, String);

/// Cancels one queued run through the runtime's normal cancellation path,
/// which appends the run's activation cancellations in the same batch.
#[async_trait]
pub trait QueuedRunCanceller: Send + Sync {
    async fn cancel_queued_run(&self, record: Record) -> EngineResult<()>;
}

static CANCELLER: OnceLock<Arc<dyn QueuedRunCanceller>> = OnceLock::new();
static REGISTRY: OnceLock<RetiredDeploymentRegistry> = OnceLock::new();

/// Install the cancellation path. Until it is installed nothing is
/// cancelled; a second install is ignored.
pub fn install_canceller(canceller: Arc<dyn QueuedRunCanceller>) {
    let _ = CANCELLER.set(canceller);
}

/// The installed cancellation path, if any.
pub fn installed_canceller() -> Option<&'static Arc<dyn QueuedRunCanceller>> {
    CANCELLER.get()
}

fn registry() -> &'static RetiredDeploymentRegistry {
    REGISTRY.get_or_init(RetiredDeploymentRegistry::default)
}

/// Record one reconciler pass: every deployment asked about, and the ones the
/// control plane said are retired. Retired entries expire after `ttl`.
pub fn record_retired_pass(
    asked: &[DeploymentKey],
    retired: &HashMap<DeploymentKey, String>,
    ttl: Duration,
) {
    registry().record_pass(asked, retired, ttl);
}

/// The status that retired this deployment, while that answer is fresh.
pub fn retired_status(project_id: &str, deployment_id: &str) -> Option<String> {
    registry().status(project_id, deployment_id)
}

/// Deployments the reconciler last found retired. An entry lives only until
/// the next pass should have refreshed it, so a restarted deployment stops
/// being treated as retired within one pass.
#[derive(Default)]
struct RetiredDeploymentRegistry {
    retired: RwLock<HashMap<DeploymentKey, (String, Instant)>>,
}

impl RetiredDeploymentRegistry {
    fn record_pass(
        &self,
        asked: &[DeploymentKey],
        retired: &HashMap<DeploymentKey, String>,
        ttl: Duration,
    ) {
        let Ok(mut entries) = self.retired.write() else {
            return;
        };
        let now = Instant::now();
        entries.retain(|_, (_, expires)| *expires > now);
        for key in asked {
            match retired.get(key) {
                Some(status) => {
                    entries.insert(key.clone(), (status.clone(), now + ttl));
                }
                None => {
                    entries.remove(key);
                }
            }
        }
    }

    fn status(&self, project_id: &str, deployment_id: &str) -> Option<String> {
        let entries = self.retired.read().ok()?;
        let (status, expires) =
            entries.get(&(project_id.to_string(), deployment_id.to_string()))?;
        (*expires > Instant::now()).then(|| status.clone())
    }
}

#[derive(Debug, Clone)]
pub struct RetiredDeploymentReconcilerConfig {
    pub tick_interval: Duration,
    /// A run is cancelled only after its current queue generation has waited
    /// this long, so a cancel never races a deployment's own startup or a
    /// requeue that is still settling.
    pub min_queued_age: Duration,
    /// Rows read from each queue per pass. Scans resume where the previous
    /// pass stopped, so a long queue is covered over several passes.
    pub max_scan_rows: usize,
    /// Pull runs cancelled per pass; the rest wait for the next pass.
    pub max_cancels_per_pass: usize,
    /// Deployments per control-plane lookup.
    pub lookup_batch: usize,
    /// Whether push runs have dispatch outbox rows. When they do not, every
    /// queued run is pulled and the reconciler cancels it.
    pub dispatch_outbox_enabled: bool,
}

impl Default for RetiredDeploymentReconcilerConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(300),
            min_queued_age: Duration::from_secs(600),
            max_scan_rows: 20_000,
            max_cancels_per_pass: 1_000,
            lookup_batch: 500,
            dispatch_outbox_enabled: true,
        }
    }
}

impl RetiredDeploymentReconcilerConfig {
    /// Long enough for the next pass to refresh an entry, short enough that a
    /// restarted deployment is not treated as retired much longer than that.
    pub fn registry_ttl(&self) -> Duration {
        self.tick_interval + self.tick_interval / 2
    }
}

/// Whether the drainer should cancel this push run instead of dispatching
/// it, and with which retired status. `None` while no cancellation path is
/// installed, the deployment is not known retired, or the run's queue
/// generation is too young.
pub(crate) fn retired_status_for_dispatch(
    project_id: &str,
    deployment_id: &str,
    queued_at_ms: i64,
    now_ms: i64,
) -> Option<(String, &'static Arc<dyn QueuedRunCanceller>)> {
    let canceller = installed_canceller()?;
    if deployment_id.is_empty() {
        return None;
    }
    let status = registry().status(project_id, deployment_id)?;
    let min_age_ms = i64::try_from(
        RetiredDeploymentReconcilerConfig::default()
            .min_queued_age
            .as_millis(),
    )
    .unwrap_or(i64::MAX);
    (now_ms.saturating_sub(queued_at_ms) >= min_age_ms).then_some((status, canceller))
}

/// A `run.cancelled` for a queued run of a retired deployment. Its metadata
/// is built fresh rather than copied from the queued record, which can carry
/// lease-fence fields the journal accepts only on other events. An outbox
/// position is included for a push run, so projecting the cancellation also
/// removes its outbox row on every replica.
#[allow(clippy::too_many_arguments)]
pub fn cancellation_record(
    project_id: &str,
    run_id: &str,
    deployment_id: &str,
    component_name: &str,
    component_type: &str,
    run_metadata: &HashMap<String, String>,
    outbox_position: Option<(u32, u64)>,
    status: &str,
) -> Record {
    let message = format!(
        "deployment {deployment_id} is retired ({status}); this queued run can never start"
    );
    let mut metadata = HashMap::new();
    for key in ["tenant_id", "workspace_id", "trigger_type"] {
        if let Some(value) = run_metadata.get(key).filter(|value| !value.is_empty()) {
            metadata.insert(key.to_string(), value.clone());
        }
    }
    metadata.insert("deployment_id".to_string(), deployment_id.to_string());
    if !component_name.is_empty() {
        metadata.insert("component_name".to_string(), component_name.to_string());
    }
    if !component_type.is_empty() {
        metadata.insert("component_type".to_string(), component_type.to_string());
    }
    if let Some((partition_id, queued_offset)) = outbox_position {
        metadata.insert(
            DISPATCH_OUTBOX_PARTITION_ID_META.to_string(),
            partition_id.to_string(),
        );
        metadata.insert(
            DISPATCH_OUTBOX_QUEUED_OFFSET_META.to_string(),
            queued_offset.to_string(),
        );
    }
    metadata.insert(
        "cancel_reason".to_string(),
        DEPLOYMENT_RETIRED_REASON.to_string(),
    );
    metadata.insert("error_message".to_string(), message.clone());
    let data = serde_json::json!({
        "reason": DEPLOYMENT_RETIRED_REASON,
        "message": message,
    });
    Record {
        offset: Offset::ZERO,
        project_id: project_id.to_string(),
        run_id: run_id.to_string(),
        event_type: "run.cancelled".to_string(),
        data: Bytes::from(serde_json::to_vec(&data).unwrap_or_default()),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Test hooks for the process-wide registry and cancellation path, shared
/// with the dispatch outbox drainer's tests and the hosted reconciler's.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use super::*;
    use std::sync::Mutex;

    /// Records every cancellation instead of appending it.
    #[derive(Default)]
    pub struct RecordingCanceller {
        pub records: Mutex<Vec<Record>>,
        pub fail_runs: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl QueuedRunCanceller for RecordingCanceller {
        async fn cancel_queued_run(&self, record: Record) -> EngineResult<()> {
            if self.fail_runs.lock().unwrap().contains(&record.run_id) {
                return Err(engine_core::error::EngineError::Storage(
                    "journal unavailable".to_string(),
                ));
            }
            self.records.lock().unwrap().push(record);
            Ok(())
        }
    }

    /// The process-wide canceller tests install; every test shares it.
    pub fn installed_recording_canceller() -> Arc<RecordingCanceller> {
        static RECORDER: OnceLock<Arc<RecordingCanceller>> = OnceLock::new();
        let recorder = RECORDER
            .get_or_init(|| Arc::new(RecordingCanceller::default()))
            .clone();
        install_canceller(recorder.clone());
        recorder
    }

    pub fn mark_retired(project_id: &str, deployment_id: &str, status: &str) {
        let key = (project_id.to_string(), deployment_id.to_string());
        registry().record_pass(
            std::slice::from_ref(&key),
            &HashMap::from([(key.clone(), status.to_string())]),
            Duration::from_secs(60),
        );
    }
}
