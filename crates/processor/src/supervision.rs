//! Partition-processor supervision and readiness state.

#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::watch;
#[cfg(test)]
use tokio::task::JoinHandle;
#[cfg(test)]
use tokio_util::sync::CancellationToken;

/// Runtime state of one local partition processor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionProcessorState {
    Starting,
    Replaying,
    Ready,
    BackingOff,
    Failed,
    Stopped,
}

/// Stable machine-readable classification for correctness failures that must
/// reach runtime observability without parsing human-readable error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionProcessorFailureKind {
    ProjectionAppliedAheadOfCommit,
}

impl PartitionProcessorFailureKind {
    pub fn divergence_type(self) -> &'static str {
        match self {
            Self::ProjectionAppliedAheadOfCommit => "projection_applied_ahead_of_commit",
        }
    }
}

/// Readiness-relevant state for one local partition processor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionProcessorStatus {
    pub partition_id: u32,
    pub state: PartitionProcessorState,
    pub live: bool,
    pub consecutive_failures: u32,
    pub checkpoint_known: bool,
    /// Last durable checkpoint in this partition's local offset space.
    pub applied_checkpoint: Option<u64>,
    pub replay_target_known: bool,
    /// Tail captured for this attempt in the same local offset space.
    pub replay_target: Option<u64>,
    pub replay_complete: bool,
    pub failure_kind: Option<PartitionProcessorFailureKind>,
    pub last_error: Option<String>,
}

impl PartitionProcessorStatus {
    fn starting(partition_id: u32) -> Self {
        Self {
            partition_id,
            state: PartitionProcessorState::Starting,
            live: false,
            consecutive_failures: 0,
            checkpoint_known: false,
            applied_checkpoint: None,
            replay_target_known: false,
            replay_target: None,
            replay_complete: false,
            failure_kind: None,
            last_error: None,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.state == PartitionProcessorState::Ready
            && self.live
            && self.checkpoint_known
            && self.replay_target_known
            && self.replay_complete
    }
}

/// Aggregate state consumed by the engine status RPC and HTTP readiness probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessorHealthSnapshot {
    pub partitions: Vec<PartitionProcessorStatus>,
}

impl ProcessorHealthSnapshot {
    pub fn starting(partition_count: usize) -> Self {
        Self {
            partitions: (0..partition_count)
                .map(|partition_id| PartitionProcessorStatus::starting(partition_id as u32))
                .collect(),
        }
    }

    pub fn is_ready(&self) -> bool {
        !self.partitions.is_empty()
            && self
                .partitions
                .iter()
                .all(PartitionProcessorStatus::is_ready)
    }
}

/// Why one processor attempt returned to its supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionProcessorExit {
    Cancelled,
    Failed(String),
    Diverged {
        kind: PartitionProcessorFailureKind,
        message: String,
    },
    StreamEnded,
}

/// Bounded retry policy for one partition supervisor.
#[derive(Debug, Clone, Copy)]
#[cfg(test)]
pub struct ProcessorSupervisorConfig {
    pub max_consecutive_failures: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub failure_reset_after: Duration,
}

#[cfg(test)]
impl Default for ProcessorSupervisorConfig {
    fn default() -> Self {
        Self {
            max_consecutive_failures: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            failure_reset_after: Duration::from_secs(30),
        }
    }
}

#[derive(Clone)]
pub struct PartitionProcessorReporter {
    partition_id: u32,
    node_id: String,
    health_tx: watch::Sender<ProcessorHealthSnapshot>,
}

impl PartitionProcessorReporter {
    #[cfg(test)]
    pub fn new(
        partition_id: u32,
        node_id: String,
        health_tx: watch::Sender<ProcessorHealthSnapshot>,
    ) -> Self {
        Self {
            partition_id,
            node_id,
            health_tx,
        }
    }

    fn update(
        &self,
        event: Option<&'static str>,
        update: impl FnOnce(&mut PartitionProcessorStatus),
    ) {
        let mut updated = None;
        self.health_tx.send_modify(|snapshot| {
            if let Some(status) = snapshot.partitions.get_mut(self.partition_id as usize) {
                update(status);
                updated = Some(status.clone());
            }
        });

        if let Some(status) = updated {
            crate::metrics::record_processor_supervisor_state(
                self.partition_id,
                &self.node_id,
                status.live,
                status.consecutive_failures,
            );
            if let Some(event) = event {
                crate::metrics::record_processor_supervisor_event(
                    self.partition_id,
                    &self.node_id,
                    event,
                );
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_preparing(&self) {
        self.update(None, |status| {
            status.state = PartitionProcessorState::Starting;
            status.live = false;
            status.checkpoint_known = false;
            status.applied_checkpoint = None;
            status.replay_target_known = false;
            status.replay_target = None;
            status.replay_complete = false;
            status.failure_kind = None;
            status.last_error = None;
        });
    }

    #[cfg(test)]
    pub fn mark_prepared(&self, checkpoint: Option<u64>, replay_target: u64) {
        self.update(None, |status| {
            status.state = PartitionProcessorState::Replaying;
            status.checkpoint_known = true;
            status.applied_checkpoint = checkpoint;
            status.replay_target_known = true;
            status.replay_target = Some(replay_target);
            status.replay_complete = false;
            status.failure_kind = None;
            status.last_error = None;
        });
    }

    pub(crate) fn mark_running(&self, replay_complete: bool) {
        self.update(Some("attempt_started"), |status| {
            status.live = true;
            status.replay_complete = replay_complete;
            status.state = if replay_complete {
                PartitionProcessorState::Ready
            } else {
                PartitionProcessorState::Replaying
            };
        });
    }

    pub(crate) fn mark_replay_complete(&self) {
        self.update(Some("replay_complete"), |status| {
            status.state = PartitionProcessorState::Ready;
            status.live = true;
            status.replay_complete = true;
            status.failure_kind = None;
            status.last_error = None;
        });
    }

    #[cfg(test)]
    fn mark_failure_series_reset(&self) {
        self.update(Some("failure_series_reset"), |status| {
            status.consecutive_failures = 0;
        });
    }

    pub(crate) fn mark_checkpoint(&self, checkpoint: u64) {
        self.update(None, |status| {
            status.checkpoint_known = true;
            status.applied_checkpoint = Some(checkpoint);
        });
    }

    #[cfg(test)]
    fn consecutive_failures(&self) -> u32 {
        self.health_tx
            .borrow()
            .partitions
            .get(self.partition_id as usize)
            .map(|status| status.consecutive_failures)
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn mark_backing_off_with_kind(
        &self,
        failures: u32,
        error: String,
        failure_kind: Option<PartitionProcessorFailureKind>,
    ) {
        self.update(Some("restart_scheduled"), |status| {
            status.state = PartitionProcessorState::BackingOff;
            status.live = false;
            status.consecutive_failures = failures;
            status.replay_complete = false;
            status.failure_kind = failure_kind;
            status.last_error = Some(error);
        });
    }

    #[cfg(test)]
    pub(crate) fn mark_failed(&self, failures: u32, error: String) {
        self.mark_failed_with_kind(failures, error, None);
    }

    #[cfg(test)]
    fn mark_failed_with_kind(
        &self,
        failures: u32,
        error: String,
        failure_kind: Option<PartitionProcessorFailureKind>,
    ) {
        self.update(Some("retry_exhausted"), |status| {
            status.state = PartitionProcessorState::Failed;
            status.live = false;
            status.consecutive_failures = failures;
            status.replay_complete = false;
            status.failure_kind = failure_kind;
            status.last_error = Some(error);
        });
    }

    #[cfg(test)]
    pub(crate) fn mark_stopped(&self) {
        self.update(Some("stopped"), |status| {
            status.state = PartitionProcessorState::Stopped;
            status.live = false;
            status.replay_complete = false;
        });
    }
}

#[async_trait]
#[cfg(test)]
pub trait PartitionProcessorAttempt: Send + Sync + 'static {
    async fn run_attempt(
        &self,
        reporter: PartitionProcessorReporter,
        cancel: CancellationToken,
    ) -> PartitionProcessorExit;
}

#[cfg(test)]
struct AbortOnDropTask<T>(JoinHandle<T>);

#[cfg(test)]
impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
struct SupervisorExitGuard {
    reporter: PartitionProcessorReporter,
    cancel: CancellationToken,
    armed: bool,
}

#[cfg(test)]
impl SupervisorExitGuard {
    fn new(reporter: PartitionProcessorReporter, cancel: CancellationToken) -> Self {
        Self {
            reporter,
            cancel,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(test)]
impl Drop for SupervisorExitGuard {
    fn drop(&mut self) {
        if self.armed {
            let failures = self.reporter.consecutive_failures().saturating_add(1);
            self.reporter.mark_failed(
                failures,
                "partition processor supervisor exited unexpectedly".to_string(),
            );
            self.cancel.cancel();
        }
    }
}

#[cfg(test)]
pub async fn supervise_partition_processor(
    attempt: std::sync::Arc<dyn PartitionProcessorAttempt>,
    reporter: PartitionProcessorReporter,
    cancel: CancellationToken,
    config: ProcessorSupervisorConfig,
) {
    let failure_limit = config.max_consecutive_failures.max(1);
    let mut consecutive_failures = 0_u32;
    let mut exit_guard = SupervisorExitGuard::new(reporter.clone(), cancel.clone());

    loop {
        if cancel.is_cancelled() {
            reporter.mark_stopped();
            exit_guard.disarm();
            return;
        }

        reporter.mark_preparing();
        let attempt_runner = attempt.clone();
        let attempt_reporter = reporter.clone();
        let attempt_cancel = cancel.clone();
        let mut health_rx = reporter.health_tx.subscribe();
        let mut task = AbortOnDropTask(tokio::spawn(async move {
            attempt_runner
                .run_attempt(attempt_reporter, attempt_cancel)
                .await
        }));

        let mut failure_reset_deadline = None;
        let result = loop {
            tokio::select! {
                result = &mut task.0 => break result,
                changed = health_rx.changed() => {
                    if changed.is_err() {
                        continue;
                    }
                    let status = health_rx
                        .borrow()
                        .partitions
                        .get(reporter.partition_id as usize)
                        .cloned();
                    let stable = status
                        .as_ref()
                        .is_some_and(|status| status.live && status.replay_complete);
                    if consecutive_failures > 0 && stable && failure_reset_deadline.is_none() {
                        failure_reset_deadline =
                            Some(tokio::time::Instant::now() + config.failure_reset_after);
                    } else if !stable {
                        failure_reset_deadline = None;
                    }
                }
                _ = wait_for_optional_deadline(failure_reset_deadline) => {
                    consecutive_failures = 0;
                    reporter.mark_failure_series_reset();
                    failure_reset_deadline = None;
                }
            }
        };
        if cancel.is_cancelled() {
            reporter.mark_stopped();
            exit_guard.disarm();
            return;
        }

        if failure_reset_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            consecutive_failures = 0;
            reporter.mark_failure_series_reset();
        }
        let (error, failure_kind) = match result {
            Ok(PartitionProcessorExit::Cancelled) => (
                "processor cancelled without node cancellation".to_string(),
                None,
            ),
            Ok(PartitionProcessorExit::Failed(error)) => (error, None),
            Ok(PartitionProcessorExit::Diverged { kind, message }) => (message, Some(kind)),
            Ok(PartitionProcessorExit::StreamEnded) => {
                ("partition tail stream ended unexpectedly".to_string(), None)
            }
            Err(error) if error.is_panic() => {
                crate::metrics::record_processor_supervisor_event(
                    reporter.partition_id,
                    &reporter.node_id,
                    "panic",
                );
                (format!("partition processor panicked: {error}"), None)
            }
            Err(error) => (format!("partition processor task failed: {error}"), None),
        };

        // Election formation, ownership handoff, and temporary quorum loss are
        // availability transitions, not corrupt processor state. Reopen from
        // the durable checkpoint after backoff without consuming the bounded
        // fatal-failure budget. The attempt never skips the failing record.
        if is_transient_topology_failure(&error) {
            crate::metrics::record_processor_supervisor_event(
                reporter.partition_id,
                &reporter.node_id,
                "transient_topology",
            );
            reporter.mark_backing_off_with_kind(consecutive_failures, error, failure_kind);
            let backoff = restart_backoff(config, consecutive_failures.max(1));
            tokio::select! {
                _ = cancel.cancelled() => {
                    reporter.mark_stopped();
                    exit_guard.disarm();
                    return;
                }
                _ = tokio::time::sleep(backoff) => {}
            }
            continue;
        }

        consecutive_failures = consecutive_failures.saturating_add(1);

        if consecutive_failures >= failure_limit {
            reporter.mark_failed_with_kind(consecutive_failures, error, failure_kind);
            // Readiness drains traffic during bounded retries. Once the budget
            // is exhausted, stop the runtime so Kubernetes can replace the pod;
            // readiness alone does not trigger a restart.
            cancel.cancel();
            exit_guard.disarm();
            return;
        }

        reporter.mark_backing_off_with_kind(consecutive_failures, error, failure_kind);
        let backoff = restart_backoff(config, consecutive_failures);
        tokio::select! {
            _ = cancel.cancelled() => {
                reporter.mark_stopped();
                exit_guard.disarm();
                return;
            }
            _ = tokio::time::sleep(backoff) => {}
        }
    }
}

#[cfg(test)]
fn is_transient_topology_failure(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    let transitional_ownership = error.contains("partition not writable")
        && (error.contains("state=opening")
            || error.contains("state=draining")
            || error.contains("state=passive"));
    let forwarding_transport = error.contains("forward append")
        && (error.contains("transport")
            || error.contains("timeout")
            || error.contains("http2")
            || error.contains("connection"));

    transitional_ownership
        || forwarding_transport
        || error.contains("write quorum unavailable before local append")
        || error.contains("quorum not reached")
        || error.contains("no connected peers for quorum")
        || error.contains("no sequencer available after append retry budget")
        || error.contains("not partition owner")
        || error.contains("stale epoch")
        || error.contains("epoch superseded")
        || (error.contains("sequencer ") && error.contains(" not connected"))
}

#[cfg(test)]
async fn wait_for_optional_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
pub(crate) fn restart_backoff(
    config: ProcessorSupervisorConfig,
    consecutive_failures: u32,
) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(31);
    std::cmp::min(
        config.initial_backoff.saturating_mul(1_u32 << exponent),
        config.max_backoff,
    )
}

/// Owned supervisor tasks. Dropping this value aborts any task not explicitly
/// drained, preventing detached partition processors.
#[cfg(test)]
pub struct PartitionProcessorSupervisors {
    handles: Vec<JoinHandle<()>>,
    background_handles: Vec<JoinHandle<()>>,
}

#[cfg(test)]
impl PartitionProcessorSupervisors {
    pub fn new(handles: Vec<JoinHandle<()>>) -> Self {
        Self {
            handles,
            background_handles: Vec::new(),
        }
    }

    pub fn with_background_handles(mut self, background_handles: Vec<JoinHandle<()>>) -> Self {
        self.background_handles = background_handles;
        self
    }

    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Wait for cancellation-aware supervisors, then explicitly abort any
    /// stragglers once the shutdown budget expires.
    pub async fn shutdown(mut self, timeout: Duration) -> bool {
        let mut handles = std::mem::take(&mut self.handles);
        let mut background_handles = std::mem::take(&mut self.background_handles);
        let mut handles_drained = vec![false; handles.len()];
        let mut background_handles_drained = vec![false; background_handles.len()];
        let drained = tokio::time::timeout(timeout, async {
            for (index, handle) in handles.iter_mut().enumerate() {
                let _ = handle.await;
                handles_drained[index] = true;
            }
            for (index, handle) in background_handles.iter_mut().enumerate() {
                let _ = handle.await;
                background_handles_drained[index] = true;
            }
        })
        .await
        .is_ok();

        if drained {
            tracing::info!(
                processor_supervisors = handles.len(),
                processor_background_tasks = background_handles.len(),
                "Partition processor supervisors drained"
            );
            return true;
        }

        let mut aborted = 0_usize;
        for (index, handle) in handles.into_iter().enumerate() {
            if handles_drained[index] {
                continue;
            }
            if !handle.is_finished() {
                handle.abort();
                aborted += 1;
            }
            let _ = handle.await;
        }
        for (index, handle) in background_handles.into_iter().enumerate() {
            if background_handles_drained[index] {
                continue;
            }
            if !handle.is_finished() {
                handle.abort();
                aborted += 1;
            }
            let _ = handle.await;
        }
        tracing::warn!(
            processor_supervisors_aborted = aborted,
            timeout_ms = timeout.as_millis() as u64,
            "Partition processor supervisor shutdown timed out"
        );
        false
    }
}

#[cfg(test)]
impl Drop for PartitionProcessorSupervisors {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        for handle in &self.background_handles {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use engine_core::traits::Segment;
    use engine_core::types::{Offset, Record};
    use engine_journal_memory::MemorySegment;

    use super::*;

    async fn wait_for_status(
        health: &mut watch::Receiver<ProcessorHealthSnapshot>,
        predicate: impl Fn(&PartitionProcessorStatus) -> bool,
    ) -> PartitionProcessorStatus {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = health.borrow().partitions[0].clone();
                if predicate(&status) {
                    return status;
                }
                health
                    .changed()
                    .await
                    .expect("supervisor health channel closed early");
            }
        })
        .await
        .expect("timed out waiting for processor status")
    }

    fn test_reporter() -> (
        PartitionProcessorReporter,
        watch::Receiver<ProcessorHealthSnapshot>,
    ) {
        let (health_tx, health) = watch::channel(ProcessorHealthSnapshot::starting(1));
        (
            PartitionProcessorReporter::new(0, "node-test".to_string(), health_tx),
            health,
        )
    }

    #[tokio::test]
    async fn shutdown_timeout_does_not_repoll_completed_handles() {
        let completed = tokio::spawn(async {});
        let blocked = tokio::spawn(std::future::pending::<()>());
        let completed_background = tokio::spawn(async {});
        tokio::task::yield_now().await;

        let supervisors = PartitionProcessorSupervisors::new(vec![completed, blocked])
            .with_background_handles(vec![completed_background]);
        let drained = tokio::time::timeout(
            Duration::from_secs(1),
            supervisors.shutdown(Duration::from_millis(10)),
        )
        .await
        .expect("shutdown fallback must remain bounded");

        assert!(!drained, "the blocked supervisor must exhaust the budget");
    }

    #[tokio::test]
    async fn shutdown_reports_success_when_every_handle_drains() {
        let supervisors = PartitionProcessorSupervisors::new(vec![tokio::spawn(async {})])
            .with_background_handles(vec![tokio::spawn(async {})]);

        assert!(supervisors.shutdown(Duration::from_secs(1)).await);
    }

    struct TransientStoreFailure {
        attempts: AtomicUsize,
        starts: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl PartitionProcessorAttempt for TransientStoreFailure {
        async fn run_attempt(
            &self,
            reporter: PartitionProcessorReporter,
            cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            let durable_checkpoint = 7_u64;
            self.starts
                .lock()
                .expect("starts mutex")
                .push(durable_checkpoint + 1);
            reporter.mark_prepared(Some(durable_checkpoint), 9);
            reporter.mark_running(false);
            if attempt == 0 {
                return PartitionProcessorExit::Failed(
                    "transient materialized-store read".to_string(),
                );
            }

            reporter.mark_checkpoint(8);
            reporter.mark_replay_complete();
            cancel.cancelled().await;
            PartitionProcessorExit::Cancelled
        }
    }

    #[tokio::test]
    async fn transient_store_failure_restarts_from_checkpoint_before_ready() {
        let attempt = Arc::new(TransientStoreFailure {
            attempts: AtomicUsize::new(0),
            starts: Mutex::new(Vec::new()),
        });
        let (reporter, mut health) = test_reporter();
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(supervise_partition_processor(
            attempt.clone(),
            reporter,
            cancel.clone(),
            ProcessorSupervisorConfig {
                max_consecutive_failures: 3,
                initial_backoff: Duration::from_millis(25),
                max_backoff: Duration::from_millis(25),
                failure_reset_after: Duration::from_millis(75),
            },
        ));

        let backing_off = wait_for_status(&mut health, |status| {
            status.state == PartitionProcessorState::BackingOff
        })
        .await;
        assert!(!backing_off.is_ready());
        assert_eq!(backing_off.consecutive_failures, 1);

        let ready = wait_for_status(&mut health, PartitionProcessorStatus::is_ready).await;
        assert_eq!(ready.applied_checkpoint, Some(8));
        assert_eq!(ready.replay_target, Some(9));
        assert_eq!(
            ready.consecutive_failures, 1,
            "replay completion alone must not reset the failure series"
        );
        assert_eq!(
            attempt.starts.lock().expect("starts mutex").as_slice(),
            &[8, 8],
            "both attempts must resume after the same durable checkpoint"
        );

        let stable = wait_for_status(&mut health, |status| {
            status.is_ready() && status.consecutive_failures == 0
        })
        .await;
        assert_eq!(
            stable.consecutive_failures, 0,
            "the failure series resets only after the recovered attempt stays live"
        );

        cancel.cancel();
        handle.await.expect("supervisor joins");
        assert_eq!(
            health.borrow().partitions[0].state,
            PartitionProcessorState::Stopped
        );
    }

    struct TransientTopologyFailure {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl PartitionProcessorAttempt for TransientTopologyFailure {
        async fn run_attempt(
            &self,
            reporter: PartitionProcessorReporter,
            cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            reporter.mark_prepared(Some(7), 9);
            reporter.mark_running(false);
            if attempt < 3 {
                return PartitionProcessorExit::Failed(
                    "process run.completed: partition not writable: partition=2 epoch=0 state=Opening reason=Some(\"startup\")"
                        .to_string(),
                );
            }

            cancel.cancelled().await;
            PartitionProcessorExit::Cancelled
        }
    }

    #[tokio::test]
    async fn transient_topology_failures_do_not_exhaust_fatal_budget() {
        let attempt = Arc::new(TransientTopologyFailure {
            attempts: AtomicUsize::new(0),
        });
        let (reporter, health) = test_reporter();
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(supervise_partition_processor(
            attempt.clone(),
            reporter,
            cancel.clone(),
            ProcessorSupervisorConfig {
                max_consecutive_failures: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
                failure_reset_after: Duration::from_secs(30),
            },
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            while attempt.attempts.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("topology retries continue past the fatal budget");

        assert!(!cancel.is_cancelled());
        assert_ne!(
            health.borrow().partitions[0].state,
            PartitionProcessorState::Failed
        );
        cancel.cancel();
        handle.await.expect("supervisor joins");
    }

    #[test]
    fn topology_failure_classifier_excludes_integrity_failures() {
        assert!(is_transient_topology_failure(
            "commit quorum not reached: 0/1 peer acks"
        ));
        assert!(is_transient_topology_failure(
            "sequencer agnt5-runtime-2 not connected"
        ));
        assert!(is_transient_topology_failure(
            "internal error: forward append: code: 'The operation was cancelled', message: \"Timeout expired\", source: tonic::transport::Error(Transport, TimeoutExpired(()))"
        ));
        assert!(is_transient_topology_failure(
            "internal error: forward append: code: 'The service is currently unavailable', message: \"http2 error\", source: tonic::transport::Error(Transport, hyper::Error(Http2, KeepAliveTimedOut))"
        ));
        assert!(!is_transient_topology_failure(
            "partition not writable: partition=2 state=Failed reason=integrity fence"
        ));
        assert!(!is_transient_topology_failure("corrupt projection record"));
    }

    struct PermanentCorruptRecord {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl PartitionProcessorAttempt for PermanentCorruptRecord {
        async fn run_attempt(
            &self,
            reporter: PartitionProcessorReporter,
            _cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            reporter.mark_prepared(None, 1);
            reporter.mark_running(false);
            PartitionProcessorExit::Failed("corrupt projection record".to_string())
        }
    }

    #[tokio::test]
    async fn permanent_failure_exhausts_budget_and_append_stays_durable() {
        let attempt = Arc::new(PermanentCorruptRecord {
            attempts: AtomicUsize::new(0),
        });
        let (reporter, mut health) = test_reporter();
        let cancel = CancellationToken::new();
        supervise_partition_processor(
            attempt.clone(),
            reporter,
            cancel.clone(),
            ProcessorSupervisorConfig {
                max_consecutive_failures: 3,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                failure_reset_after: Duration::from_secs(30),
            },
        )
        .await;

        let failed = wait_for_status(&mut health, |status| {
            status.state == PartitionProcessorState::Failed
        })
        .await;
        assert!(!failed.is_ready());
        assert_eq!(failed.consecutive_failures, 3);
        assert_eq!(attempt.attempts.load(Ordering::SeqCst), 3);
        assert!(
            cancel.is_cancelled(),
            "retry exhaustion must stop the runtime so the pod is replaced"
        );

        // Degraded mode remains journal-first: processor failure makes the node
        // unready but does not seal or disable the append path.
        let segment = MemorySegment::new(0);
        let offset = segment
            .append_batch(&[Record {
                offset: Offset::ZERO,
                project_id: "project".to_string(),
                run_id: "run".to_string(),
                event_type: "run.queued".to_string(),
                data: Bytes::from_static(b"{}"),
                timestamp_ns: 1,
                step_key: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: HashMap::new(),
                data_type: "json".to_string(),
                data_checksum: None,
                data_compressed: false,
            }])
            .await
            .expect("append remains available");
        assert_eq!(offset, vec![Offset::ZERO]);
    }

    struct AppliedAheadOfCommit;

    #[async_trait]
    impl PartitionProcessorAttempt for AppliedAheadOfCommit {
        async fn run_attempt(
            &self,
            reporter: PartitionProcessorReporter,
            _cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            reporter.mark_prepared(Some(8), 7);
            PartitionProcessorExit::Diverged {
                kind: PartitionProcessorFailureKind::ProjectionAppliedAheadOfCommit,
                message: "M=9 exceeds Q=8".to_string(),
            }
        }
    }

    #[tokio::test]
    async fn divergence_kind_survives_supervision_without_error_text_parsing() {
        let (reporter, mut health) = test_reporter();
        supervise_partition_processor(
            Arc::new(AppliedAheadOfCommit),
            reporter,
            CancellationToken::new(),
            ProcessorSupervisorConfig {
                max_consecutive_failures: 1,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                failure_reset_after: Duration::from_secs(30),
            },
        )
        .await;

        let failed = wait_for_status(&mut health, |status| {
            status.state == PartitionProcessorState::Failed
        })
        .await;
        assert_eq!(
            failed.failure_kind,
            Some(PartitionProcessorFailureKind::ProjectionAppliedAheadOfCommit)
        );
        assert_eq!(
            failed.failure_kind.unwrap().divergence_type(),
            "projection_applied_ahead_of_commit"
        );
    }

    struct PanickingAttempt;

    #[async_trait]
    impl PartitionProcessorAttempt for PanickingAttempt {
        async fn run_attempt(
            &self,
            _reporter: PartitionProcessorReporter,
            _cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            panic!("injected processor panic");
        }
    }

    #[tokio::test]
    async fn panics_are_bounded_and_surface_as_terminal_failure() {
        let (reporter, health) = test_reporter();
        supervise_partition_processor(
            Arc::new(PanickingAttempt),
            reporter,
            CancellationToken::new(),
            ProcessorSupervisorConfig {
                max_consecutive_failures: 2,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                failure_reset_after: Duration::from_secs(30),
            },
        )
        .await;

        let failed = &health.borrow().partitions[0];
        assert_eq!(failed.state, PartitionProcessorState::Failed);
        assert_eq!(failed.consecutive_failures, 2);
        assert!(failed
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("panicked")));
    }

    struct ImmediatelyFailingReadyAttempt;

    #[async_trait]
    impl PartitionProcessorAttempt for ImmediatelyFailingReadyAttempt {
        async fn run_attempt(
            &self,
            reporter: PartitionProcessorReporter,
            _cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            reporter.mark_prepared(None, 0);
            reporter.mark_running(true);
            PartitionProcessorExit::Failed("tail read failed".to_string())
        }
    }

    #[tokio::test]
    async fn brief_ready_transition_does_not_reset_failure_budget() {
        let (reporter, health) = test_reporter();
        supervise_partition_processor(
            Arc::new(ImmediatelyFailingReadyAttempt),
            reporter,
            CancellationToken::new(),
            ProcessorSupervisorConfig {
                max_consecutive_failures: 3,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                failure_reset_after: Duration::from_secs(30),
            },
        )
        .await;

        let failed = &health.borrow().partitions[0];
        assert_eq!(failed.state, PartitionProcessorState::Failed);
        assert_eq!(failed.consecutive_failures, 3);
    }

    #[test]
    fn restart_backoff_is_exponential_and_capped() {
        let config = ProcessorSupervisorConfig {
            max_consecutive_failures: 5,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
            failure_reset_after: Duration::from_secs(30),
        };
        assert_eq!(restart_backoff(config, 1), Duration::from_millis(10));
        assert_eq!(restart_backoff(config, 2), Duration::from_millis(20));
        assert_eq!(restart_backoff(config, 3), Duration::from_millis(40));
        assert_eq!(restart_backoff(config, 4), Duration::from_millis(40));
    }

    struct StuckAttempt {
        dropped: Arc<AtomicBool>,
    }

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl PartitionProcessorAttempt for StuckAttempt {
        async fn run_attempt(
            &self,
            reporter: PartitionProcessorReporter,
            _cancel: CancellationToken,
        ) -> PartitionProcessorExit {
            let _marker = DropMarker(self.dropped.clone());
            reporter.mark_prepared(None, 1);
            reporter.mark_running(false);
            std::future::pending::<PartitionProcessorExit>().await
        }
    }

    #[tokio::test]
    async fn shutdown_explicitly_aborts_a_stuck_processor_attempt() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (reporter, mut health) = test_reporter();
        let supervisor = tokio::spawn(supervise_partition_processor(
            Arc::new(StuckAttempt {
                dropped: dropped.clone(),
            }),
            reporter,
            CancellationToken::new(),
            ProcessorSupervisorConfig::default(),
        ));
        wait_for_status(&mut health, |status| status.live).await;

        let drained = PartitionProcessorSupervisors::new(vec![supervisor])
            .shutdown(Duration::from_millis(5))
            .await;
        assert!(!drained);
        assert!(
            dropped.load(Ordering::SeqCst),
            "aborting the supervisor must also abort its active attempt"
        );
        assert_eq!(
            health.borrow().partitions[0].state,
            PartitionProcessorState::Failed,
            "an aborted supervisor cannot leave a stale Ready/live state"
        );
    }

    #[tokio::test]
    async fn shutdown_explicitly_aborts_a_stuck_background_task() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let background = tokio::spawn(async move {
            let _drop_flag = DropFlag(task_dropped);
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;

        let drained = PartitionProcessorSupervisors::new(Vec::new())
            .with_background_handles(vec![background])
            .shutdown(Duration::from_millis(5))
            .await;

        assert!(!drained);
        assert!(
            dropped.load(Ordering::SeqCst),
            "shutdown timeout must abort and join owned background tasks"
        );
    }
}
