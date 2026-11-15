//! The boot state machine.
//!
//! Boot is the least observable part of the runtime. The only signal today is
//! [`NodeLifecycleState::Starting`](crate::NodeLifecycleState), which says
//! nothing about how far along a node is or which partition is holding it up.
//!
//! The machine is linear and alternates between work and the milestone that
//! work earns:
//!
//! ```text
//!   Starting ──> Reachable ──> AdmittingStorage ──> LocalTruth
//!                                                       │
//!        ┌──────────────────────────────────────────────┘
//!        v
//!   ClaimingOwnership ──> ClusterAuthority ──> CatchingUp ──> Serving
//!
//!   any state ──> Failed
//! ```
//!
//! The four milestones — `Reachable`, `LocalTruth`, `ClusterAuthority`,
//! `Serving` — are the four stages of
//! the runtime's target boot sequence. The states between
//! them name the work that earns the next one.
//!
//! Nothing here changes boot behaviour. Steps report into a [`BootProgress`]
//! handle; the published [`BootStatus`] has no readers yet.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// Where a node, or one partition of it, has got to in boot.
///
/// Settled states ([`BootState::is_milestone`]) are the four stages of the boot
/// sequence. The states between them name the work in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootState {
    /// The process is alive. Nothing is bound and nothing has been read.
    Starting,
    /// Health, replication, and boot-status surfaces answer. Stage 0.
    Reachable,
    /// Reading local storage: cold catalog, hot suffix, projection, journals.
    AdmittingStorage,
    /// Local storage is admitted, and this node knows what it holds. Stage A.
    LocalTruth,
    /// Settling which node owns each partition.
    ClaimingOwnership,
    /// Ownership of every partition this node serves is settled. Stage B.
    ClusterAuthority,
    /// Replaying owned partitions up to their commit frontier.
    CatchingUp,
    /// Every owned partition answers correctly. Stage C, and the healthy end.
    Serving,
    /// Boot stopped. The unhealthy end.
    Failed,
}

impl BootState {
    /// The states of a healthy boot, in order.
    pub const SEQUENCE: [BootState; 8] = [
        Self::Starting,
        Self::Reachable,
        Self::AdmittingStorage,
        Self::LocalTruth,
        Self::ClaimingOwnership,
        Self::ClusterAuthority,
        Self::CatchingUp,
        Self::Serving,
    ];

    /// Position in [`BootState::SEQUENCE`]; `None` for [`BootState::Failed`],
    /// which is off the line rather than at the end of it.
    pub const fn rank(self) -> Option<u8> {
        Some(match self {
            Self::Starting => 0,
            Self::Reachable => 1,
            Self::AdmittingStorage => 2,
            Self::LocalTruth => 3,
            Self::ClaimingOwnership => 4,
            Self::ClusterAuthority => 5,
            Self::CatchingUp => 6,
            Self::Serving => 7,
            Self::Failed => return None,
        })
    }

    /// The next state of a healthy boot, or `None` at either end.
    pub const fn next(self) -> Option<BootState> {
        Some(match self {
            Self::Starting => Self::Reachable,
            Self::Reachable => Self::AdmittingStorage,
            Self::AdmittingStorage => Self::LocalTruth,
            Self::LocalTruth => Self::ClaimingOwnership,
            Self::ClaimingOwnership => Self::ClusterAuthority,
            Self::ClusterAuthority => Self::CatchingUp,
            Self::CatchingUp => Self::Serving,
            Self::Serving | Self::Failed => return None,
        })
    }

    /// A settled state: one of the four stages the boot sequence is defined by.
    pub const fn is_milestone(self) -> bool {
        matches!(
            self,
            Self::Reachable | Self::LocalTruth | Self::ClusterAuthority | Self::Serving
        )
    }

    /// No transition leaves this state.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Serving | Self::Failed)
    }

    /// The only legal moves are one step forward, or off the line to
    /// [`BootState::Failed`]. Boot never goes backwards and never skips.
    pub const fn can_advance_to(self, to: BootState) -> bool {
        if self.is_terminal() {
            return false;
        }
        match to {
            Self::Failed => true,
            _ => match self.next() {
                Some(expected) => expected as u8 == to as u8,
                None => false,
            },
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Reachable => "reachable",
            Self::AdmittingStorage => "admitting_storage",
            Self::LocalTruth => "local_truth",
            Self::ClaimingOwnership => "claiming_ownership",
            Self::ClusterAuthority => "cluster_authority",
            Self::CatchingUp => "catching_up",
            Self::Serving => "serving",
            Self::Failed => "failed",
        }
    }
}

/// What it means for a step to run past its bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepPolicy {
    /// The step does a fixed amount of work, so exceeding the bound means it
    /// is stuck. Report it and fail the boot, so the supervisor retries
    /// against a stated reason instead of a silent wait.
    FailClosed,
    /// The step's duration is a property of how much history it has to move.
    /// Report it and keep waiting: a slow step is still doing useful work, and
    /// abandoning one part-way is how partial state gets written.
    FailOpen,
}

impl StepPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailClosed => "fail_closed",
            Self::FailOpen => "fail_open",
        }
    }
}

/// A unit of boot work, named for what it does.
///
/// Steps are detail inside a [`BootState`], not states themselves: they answer
/// "what is it doing right now", which is what an operator watching a slow boot
/// wants. They carry no transitions of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootStep {
    /// Phase 4 — build the object store and admit the cold catalog.
    AdmitColdCatalog,
    /// Phase 5 — trim HA hot suffixes that overlap the cold tier.
    RepairHotSuffix,
    /// Phase 6 — recover the projection from an S3 snapshot.
    RecoverProjection,
    /// Phase 7 — open the projection and run optional backfills.
    OpenProjection,
    /// Phase 8 — assemble the per-partition journal pipelines.
    AssembleJournals,
    /// Phase 9 — start the replication subsystem.
    ///
    /// This is the only step of [`BootState::ClaimingOwnership`] because
    /// nothing in today's boot waits for ownership to settle: the election
    /// coordinator runs asynchronously and `run_engine` moves on. BOOT-302
    /// adds the wait that makes the milestone mean what it says.
    StartReplication,
    /// Phase 11a — reset a projection the journal layers found untrusted.
    ResetProjection,
    /// Phase 12 — ownership watchers, dispatch gates, S3 subsystem, schedulers.
    StartBackground,
    /// Phase 11b — spawn partition processors and replay.
    ReplayPartitions,
    /// Phase 13 — wait for replay to reach a servable state.
    ///
    /// Spawning the processors starts replay; it does not finish it. Until
    /// this step existed the wait ran with no step in flight, so the one part
    /// of boot that can genuinely hang for hours was the one part nothing was
    /// watching.
    AwaitReplay,
}

impl BootStep {
    /// Every step, in the order `run_engine` performs it.
    pub const ALL: [BootStep; 10] = [
        Self::AdmitColdCatalog,
        Self::RepairHotSuffix,
        Self::RecoverProjection,
        Self::OpenProjection,
        Self::AssembleJournals,
        Self::StartReplication,
        Self::ResetProjection,
        Self::StartBackground,
        Self::ReplayPartitions,
        Self::AwaitReplay,
    ];

    /// How long this step may run before it is reported as overdue.
    ///
    /// These are reporting thresholds, not SLAs. A step whose duration is a
    /// property of how much history it has to move gets [`StepPolicy::FailOpen`];
    /// a step that does a fixed amount of local work gets
    /// [`StepPolicy::FailClosed`], because fixed work that has not finished is
    /// stuck rather than slow.
    ///
    /// The fail-closed bounds are deliberately loose — far above anything the
    /// step should take. The two errors are not symmetric: a bound set too high
    /// only delays the report, while one set too low crash-loops a node that
    /// was merely slow, which is worse than the hang it replaced.
    pub const fn bound(self) -> Duration {
        match self {
            // Lists the committed catalog, which is unbounded until BOOT-503.
            Self::AdmitColdCatalog => Duration::from_secs(600),
            // Trims local hot suffixes; bounded by the overlap, not history.
            Self::RepairHotSuffix => Duration::from_secs(600),
            // Downloads and validates a snapshot: proportional to its size.
            Self::RecoverProjection => Duration::from_secs(1800),
            // Opens RocksDB and runs whatever backfills still lack watermarks.
            Self::OpenProjection => Duration::from_secs(600),
            // Opens journals, backfills indexes, verifies retained hot/cold
            // overlap and restores missing cold suffixes. Work grows with history.
            Self::AssembleJournals => Duration::from_secs(600),
            // Binds listeners and connects peers. Peer connection retries in
            // the background, so this should not be slow.
            Self::StartReplication => Duration::from_secs(600),
            // Rebuilds a projection from the journal.
            Self::ResetProjection => Duration::from_secs(1800),
            // Spawns tasks.
            Self::StartBackground => Duration::from_secs(300),
            // Spawns processors; the replay itself is the next step.
            Self::ReplayPartitions => Duration::from_secs(600),
            // Replays every partition to a servable offset.
            Self::AwaitReplay => Duration::from_secs(3600),
        }
    }

    /// What being overdue means for this step.
    pub const fn policy(self) -> StepPolicy {
        match self {
            Self::AdmitColdCatalog
            | Self::RecoverProjection
            | Self::OpenProjection
            | Self::AssembleJournals
            | Self::ResetProjection
            | Self::AwaitReplay => StepPolicy::FailOpen,
            Self::RepairHotSuffix
            | Self::StartReplication
            | Self::ReplayPartitions
            | Self::StartBackground => StepPolicy::FailClosed,
        }
    }

    /// The state this step runs inside.
    pub const fn state(self) -> BootState {
        match self {
            Self::AdmitColdCatalog
            | Self::RepairHotSuffix
            | Self::RecoverProjection
            | Self::OpenProjection
            | Self::AssembleJournals => BootState::AdmittingStorage,
            Self::StartReplication => BootState::ClaimingOwnership,
            Self::ResetProjection
            | Self::StartBackground
            | Self::ReplayPartitions
            | Self::AwaitReplay => BootState::CatchingUp,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AdmitColdCatalog => "admit_cold_catalog",
            Self::RepairHotSuffix => "repair_hot_suffix",
            Self::RecoverProjection => "recover_projection",
            Self::OpenProjection => "open_projection",
            Self::AssembleJournals => "assemble_journals",
            Self::StartReplication => "start_replication",
            Self::ResetProjection => "reset_projection",
            Self::StartBackground => "start_background",
            Self::ReplayPartitions => "replay_partitions",
            Self::AwaitReplay => "await_replay",
        }
    }
}

/// Whether a transition describes the node or one partition of it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BootScope {
    Node,
    Partition(u32),
}

/// One move of the machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BootTransition {
    pub scope: BootScope,
    pub from: BootState,
    pub to: BootState,
    /// The step in flight when the move happened, if any.
    pub step: Option<BootStep>,
}

/// An illegal move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{scope:?} cannot advance from {from:?} to {to:?}")]
pub struct BootTransitionError {
    pub scope: BootScope,
    pub from: BootState,
    pub to: BootState,
}

/// The published snapshot: where the node is, and where each partition is.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BootStatus {
    pub node: BootState,
    /// The work in flight. On a failed boot this names the step it died in.
    pub step: Option<BootStep>,
    pub partitions: BTreeMap<u32, BootState>,
    /// Steps that ran past their bound, in the order they did. Present on a
    /// boot that is still running, which is the case an operator cares about.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overdue: Vec<OverdueStep>,
}

/// A step that ran past its declared bound.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OverdueStep {
    pub step: BootStep,
    pub policy: StepPolicy,
    pub bound_ms: u64,
    pub elapsed_ms: u64,
}

impl BootStatus {
    fn new(partition_count: u32) -> Self {
        Self {
            node: BootState::Starting,
            step: None,
            overdue: Vec::new(),
            partitions: (0..partition_count)
                .map(|partition| (partition, BootState::Starting))
                .collect(),
        }
    }

    /// The node and every partition reached [`BootState::Serving`].
    pub fn is_serving(&self) -> bool {
        self.node == BootState::Serving
            && self
                .partitions
                .values()
                .all(|state| *state == BootState::Serving)
    }

    /// How many partitions have reached [`BootState::Serving`].
    pub fn partitions_serving(&self) -> usize {
        self.partitions
            .values()
            .filter(|state| **state == BootState::Serving)
            .count()
    }

    /// The furthest state every partition has reached — the node is only as
    /// booted as its slowest partition.
    pub fn slowest_partition(&self) -> Option<BootState> {
        self.partitions.values().copied().min_by_key(|state| {
            // Failed sorts first: one failed partition is the headline.
            state.rank().map_or(0, |rank| u16::from(rank) + 1)
        })
    }
}

struct Inner {
    tx: watch::Sender<BootStatus>,
    history: Mutex<Vec<BootTransition>>,
    in_flight: Mutex<Option<StepInFlight>>,
    /// Overrides every step's declared bound. Tests set a tiny value so they
    /// assert the policy rather than the wall clock.
    bound_override: Option<Duration>,
}

struct StepInFlight {
    step: BootStep,
    started: Instant,
    reported: bool,
}

/// Drives the boot state machine and publishes the result.
///
/// Cheap to clone; every clone writes the same channel.
#[derive(Clone)]
pub struct BootProgress(Arc<Inner>);

impl BootProgress {
    /// Create the channel. Call this before any boot work begins, so a reader
    /// exists from the first second of the process.
    pub fn new(partition_count: u32) -> (Self, watch::Receiver<BootStatus>) {
        Self::with_bound_override(partition_count, None)
    }

    /// Same, with every step's bound replaced. For tests that need to assert
    /// what being overdue does without waiting for a real bound to elapse.
    pub fn with_bound_override(
        partition_count: u32,
        bound_override: Option<Duration>,
    ) -> (Self, watch::Receiver<BootStatus>) {
        let (tx, rx) = watch::channel(BootStatus::new(partition_count));
        (
            Self(Arc::new(Inner {
                tx,
                history: Mutex::new(Vec::new()),
                in_flight: Mutex::new(None),
                bound_override,
            })),
            rx,
        )
    }

    /// Move the node to its next state.
    pub fn advance(&self, to: BootState) -> Result<(), BootTransitionError> {
        self.transition(BootScope::Node, to)
    }

    /// Move one partition to its next state.
    pub fn advance_partition(
        &self,
        partition: u32,
        to: BootState,
    ) -> Result<(), BootTransitionError> {
        self.transition(BootScope::Partition(partition), to)
    }

    /// Move every partition to its next state, stopping at the first illegal
    /// move. Partitions advance in lockstep for node-wide work.
    pub fn advance_all_partitions(&self, to: BootState) -> Result<(), BootTransitionError> {
        let partitions: Vec<u32> = self.0.tx.borrow().partitions.keys().copied().collect();
        for partition in partitions {
            self.advance_partition(partition, to)?;
        }
        Ok(())
    }

    /// Move the node and every partition together.
    ///
    /// Today's `run_engine` does node-wide work: one slow partition stalls all
    /// of them, so they share a state. BOOT-201 gives partitions independent
    /// progress, and callers move to [`BootProgress::advance_partition`] then.
    pub fn advance_all(&self, to: BootState) -> Result<(), BootTransitionError> {
        self.advance(to)?;
        self.advance_all_partitions(to)
    }

    fn transition(&self, scope: BootScope, to: BootState) -> Result<(), BootTransitionError> {
        let mut result = Ok(());
        self.0.tx.send_modify(|status| {
            let slot = match scope {
                BootScope::Node => &mut status.node,
                BootScope::Partition(partition) => match status.partitions.get_mut(&partition) {
                    Some(slot) => slot,
                    None => {
                        result = Err(BootTransitionError {
                            scope,
                            from: BootState::Failed,
                            to,
                        });
                        return;
                    }
                },
            };
            let from = *slot;
            if !from.can_advance_to(to) {
                result = Err(BootTransitionError { scope, from, to });
                return;
            }
            *slot = to;
            let step = status.step;
            // A state change ends whatever step earned it, so a stale step name
            // can never outlive the work it described. A failed boot keeps it:
            // the step it died in is the whole point of the report.
            if scope == BootScope::Node && to != BootState::Failed {
                status.step = None;
            }
            self.0
                .history
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(BootTransition {
                    scope,
                    from,
                    to,
                    step,
                });
        });
        result
    }

    /// Begin a step. The returned guard clears it on completion; dropping the
    /// guard without completing leaves the step named, which is how a failed
    /// boot reports where it stopped.
    fn in_flight(&self) -> std::sync::MutexGuard<'_, Option<StepInFlight>> {
        self.0
            .in_flight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Report the step in flight if it has run past its bound, and act on its
    /// policy. Returns `true` once boot has reached a terminal state and there
    /// is nothing left to watch.
    ///
    /// Reports each step at most once: the point is to name the step that went
    /// long, not to repeat it every tick.
    pub fn check_step_bound(&self) -> bool {
        if self.status().node.is_terminal() {
            return true;
        }
        let (step, elapsed, bound) = {
            let mut in_flight = self.in_flight();
            let Some(current) = in_flight.as_mut() else {
                return false;
            };
            let bound = self
                .0
                .bound_override
                .unwrap_or_else(|| current.step.bound());
            let elapsed = current.started.elapsed();
            if current.reported || elapsed < bound {
                return false;
            }
            current.reported = true;
            (current.step, elapsed, bound)
        };

        let policy = step.policy();
        let overdue = OverdueStep {
            step,
            policy,
            bound_ms: bound.as_millis() as u64,
            elapsed_ms: elapsed.as_millis() as u64,
        };
        tracing::warn!(
            step = step.as_str(),
            policy = policy.as_str(),
            bound_ms = overdue.bound_ms,
            elapsed_ms = overdue.elapsed_ms,
            "Boot step ran past its bound"
        );
        self.0.tx.send_modify(|status| status.overdue.push(overdue));

        if policy == StepPolicy::FailClosed {
            // A fixed amount of work that has not finished is stuck. Say so;
            // the supervisor's retry then has a reason attached to it.
            let _ = self.advance(BootState::Failed);
            return true;
        }
        false
    }

    /// Watch the step in flight until boot reaches a terminal state.
    ///
    /// The watchdog reports and, for a fail-closed step, fails the boot. It
    /// never cancels the work: a step that is slow because it has a lot of
    /// history to move is still making progress, and abandoning one part-way
    /// is how partial state gets written.
    /// `cancel` resolves when the caller is shutting down. Taking a future
    /// rather than a token keeps this crate independent of which cancellation
    /// type the binary happens to use.
    pub fn spawn_watchdog(
        &self,
        tick: Duration,
        cancel: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        let progress = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(tick);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tokio::pin!(cancel);
            loop {
                tokio::select! {
                    _ = &mut cancel => return,
                    _ = ticker.tick() => {
                        if progress.check_step_bound() {
                            return;
                        }
                    }
                }
            }
        })
    }

    pub fn begin(&self, step: BootStep) -> StepGuard {
        *self.in_flight() = Some(StepInFlight {
            step,
            started: Instant::now(),
            reported: false,
        });
        self.0.tx.send_modify(|status| status.step = Some(step));
        StepGuard {
            progress: self.clone(),
            step,
        }
    }

    /// Every move the machine has made, in order.
    ///
    /// A [`watch`] channel holds only the latest value, so a caller asserting on
    /// the whole path reads it here.
    pub fn history(&self) -> Vec<BootTransition> {
        self.0
            .history
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn status(&self) -> BootStatus {
        self.0.tx.borrow().clone()
    }
}

/// Clears the in-flight step when the work finishes.
#[must_use = "dropping the guard without completing leaves the step reported as in flight"]
pub struct StepGuard {
    progress: BootProgress,
    step: BootStep,
}

impl StepGuard {
    /// The step finished, with work done or with nothing to do in this
    /// configuration — either way it is no longer in flight.
    pub fn finish(self) {
        {
            let mut in_flight = self.progress.in_flight();
            if in_flight
                .as_ref()
                .is_some_and(|current| current.step == self.step)
            {
                *in_flight = None;
            }
        }
        self.progress.0.tx.send_modify(|status| {
            if status.step == Some(self.step) {
                status.step = None;
            }
        });
    }
}

impl Drop for StepGuard {
    /// A step abandoned without finishing — an early return on a shutdown
    /// path — stops being watched.
    ///
    /// `status.step` is deliberately left alone: on a boot that died it names
    /// the step it died in, which is the whole reason it is published.
    fn drop(&mut self) {
        let mut in_flight = self.progress.in_flight();
        if in_flight
            .as_ref()
            .is_some_and(|current| current.step == self.step)
        {
            *in_flight = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk the node from `Starting` to `Serving`, asserting every move is legal.
    fn boot_node(progress: &BootProgress) {
        for state in BootState::SEQUENCE.iter().skip(1) {
            progress.advance(*state).expect("healthy boot move");
        }
    }

    #[test]
    fn the_sequence_is_a_line_with_no_gaps() {
        for pair in BootState::SEQUENCE.windows(2) {
            assert_eq!(
                pair[0].next(),
                Some(pair[1]),
                "{:?} must be followed by {:?}",
                pair[0],
                pair[1]
            );
            assert!(pair[0].can_advance_to(pair[1]));
        }
        assert_eq!(BootState::Serving.next(), None);
        assert_eq!(BootState::Failed.next(), None);
        assert_eq!(BootState::Failed.rank(), None);
    }

    #[test]
    fn work_and_milestones_alternate() {
        // Every milestone is earned by exactly one working state before it, and
        // every working state leads to a milestone. This is the shape the boot
        // sequence document describes; a state added on one side only is a bug.
        for pair in BootState::SEQUENCE.windows(2) {
            assert_ne!(
                pair[0].is_milestone(),
                pair[1].is_milestone(),
                "{:?} and {:?} are both {}",
                pair[0],
                pair[1],
                if pair[0].is_milestone() {
                    "milestones"
                } else {
                    "work"
                }
            );
        }
        let milestones: Vec<_> = BootState::SEQUENCE
            .iter()
            .filter(|state| state.is_milestone())
            .collect();
        assert_eq!(
            milestones,
            vec![
                &BootState::Reachable,
                &BootState::LocalTruth,
                &BootState::ClusterAuthority,
                &BootState::Serving,
            ]
        );
    }

    #[test]
    fn boot_never_goes_backwards_and_never_skips() {
        for from in BootState::SEQUENCE {
            for to in BootState::SEQUENCE {
                let legal = from.next() == Some(to);
                assert_eq!(
                    from.can_advance_to(to),
                    legal,
                    "{from:?} -> {to:?} should be {}",
                    if legal { "legal" } else { "refused" }
                );
            }
        }
    }

    #[test]
    fn any_live_state_can_fail_and_nothing_leaves_a_terminal_state() {
        for state in BootState::SEQUENCE {
            assert_eq!(
                state.can_advance_to(BootState::Failed),
                !state.is_terminal(),
                "{state:?}"
            );
        }
        for terminal in [BootState::Serving, BootState::Failed] {
            for to in BootState::SEQUENCE {
                assert!(!terminal.can_advance_to(to), "{terminal:?} -> {to:?}");
            }
        }
    }

    #[test]
    fn every_step_belongs_to_a_working_state() {
        for step in BootStep::ALL {
            assert!(
                !step.state().is_milestone(),
                "{step:?} claims to run inside milestone {:?}",
                step.state()
            );
        }
    }

    #[test]
    fn steps_are_declared_in_the_order_their_states_run() {
        let mut previous = 0u8;
        for step in BootStep::ALL {
            let rank = step.state().rank().expect("working states are ranked");
            assert!(
                rank >= previous,
                "{step:?} runs in {:?}, which is behind the previous step",
                step.state()
            );
            previous = rank;
        }
    }

    #[test]
    fn names_are_unique() {
        let states: std::collections::BTreeSet<_> =
            BootState::SEQUENCE.iter().map(|s| s.as_str()).collect();
        assert_eq!(states.len(), BootState::SEQUENCE.len());
        let steps: std::collections::BTreeSet<_> =
            BootStep::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(steps.len(), BootStep::ALL.len());
        // A step must never be mistakable for a state in a log line.
        for step in BootStep::ALL {
            assert!(!states.contains(step.as_str()), "{step:?} collides");
        }
    }

    #[test]
    fn a_healthy_boot_records_one_move_per_state() {
        let (progress, rx) = BootProgress::new(2);
        boot_node(&progress);
        progress
            .advance_all_partitions(BootState::Reachable)
            .unwrap();

        let node_moves: Vec<_> = progress
            .history()
            .into_iter()
            .filter(|t| t.scope == BootScope::Node)
            .collect();
        assert_eq!(node_moves.len(), BootState::SEQUENCE.len() - 1);
        for (move_, pair) in node_moves.iter().zip(BootState::SEQUENCE.windows(2)) {
            assert_eq!((move_.from, move_.to), (pair[0], pair[1]));
        }
        assert_eq!(rx.borrow().node, BootState::Serving);
    }

    #[test]
    fn an_illegal_move_is_refused_and_leaves_the_state_alone() {
        let (progress, _rx) = BootProgress::new(1);
        let error = progress.advance(BootState::Serving).unwrap_err();
        assert_eq!(error.from, BootState::Starting);
        assert_eq!(error.to, BootState::Serving);
        assert_eq!(progress.status().node, BootState::Starting);
        assert!(progress.history().is_empty());
    }

    #[test]
    fn a_partition_that_does_not_exist_is_refused() {
        let (progress, _rx) = BootProgress::new(1);
        assert!(progress.advance_partition(7, BootState::Reachable).is_err());
        assert!(progress.history().is_empty());
    }

    #[test]
    fn serving_requires_every_partition_not_just_the_node() {
        let (progress, _rx) = BootProgress::new(3);
        boot_node(&progress);
        assert!(!progress.status().is_serving());

        for state in BootState::SEQUENCE.iter().skip(1) {
            progress.advance_all_partitions(*state).unwrap();
        }
        assert!(progress.status().is_serving());
    }

    #[test]
    fn the_slowest_partition_is_the_headline() {
        let (progress, _rx) = BootProgress::new(3);
        for state in BootState::SEQUENCE.iter().skip(1) {
            progress.advance_all_partitions(*state).unwrap();
        }
        assert_eq!(
            progress.status().slowest_partition(),
            Some(BootState::Serving)
        );

        let (progress, _rx) = BootProgress::new(3);
        progress
            .advance_all_partitions(BootState::Reachable)
            .unwrap();
        progress
            .advance_partition(0, BootState::AdmittingStorage)
            .unwrap();
        assert_eq!(
            progress.status().slowest_partition(),
            Some(BootState::Reachable)
        );

        progress.advance_partition(1, BootState::Failed).unwrap();
        assert_eq!(
            progress.status().slowest_partition(),
            Some(BootState::Failed),
            "a failed partition outranks a merely slow one"
        );
    }

    #[test]
    fn a_finished_step_stops_being_reported() {
        let (progress, _rx) = BootProgress::new(1);
        progress.advance(BootState::Reachable).unwrap();
        progress.advance(BootState::AdmittingStorage).unwrap();

        let guard = progress.begin(BootStep::AdmitColdCatalog);
        assert_eq!(progress.status().step, Some(BootStep::AdmitColdCatalog));
        guard.finish();
        assert_eq!(progress.status().step, None);
    }

    #[test]
    fn an_abandoned_step_names_where_boot_died() {
        let (progress, _rx) = BootProgress::new(1);
        progress.advance(BootState::Reachable).unwrap();
        progress.advance(BootState::AdmittingStorage).unwrap();

        // A `?` return out of the middle of a step drops the guard without
        // finishing it. The step must survive into the failure report.
        drop(progress.begin(BootStep::RecoverProjection));
        progress.advance(BootState::Failed).unwrap();

        let status = progress.status();
        assert_eq!(status.node, BootState::Failed);
        assert_eq!(status.step, Some(BootStep::RecoverProjection));
        let last = *progress.history().last().unwrap();
        assert_eq!(last.step, Some(BootStep::RecoverProjection));
    }

    #[test]
    fn reaching_a_milestone_clears_the_step_that_earned_it() {
        let (progress, _rx) = BootProgress::new(1);
        progress.advance(BootState::Reachable).unwrap();
        progress.advance(BootState::AdmittingStorage).unwrap();
        drop(progress.begin(BootStep::AssembleJournals));

        progress.advance(BootState::LocalTruth).unwrap();
        assert_eq!(
            progress.status().step,
            None,
            "a step must not outlive the state it ran in"
        );
    }

    /// A bound of zero makes "overdue" true immediately, so these assert the
    /// policy rather than the wall clock.
    fn overdue_progress() -> (BootProgress, watch::Receiver<BootStatus>) {
        BootProgress::with_bound_override(1, Some(Duration::ZERO))
    }

    #[test]
    fn an_overdue_fail_open_step_is_reported_and_left_running() {
        let (progress, _rx) = overdue_progress();
        let _guard = progress.begin(BootStep::AwaitReplay);

        assert!(
            !progress.check_step_bound(),
            "a fail-open step must not end the watch"
        );
        let status = progress.status();
        assert_eq!(status.overdue.len(), 1);
        assert_eq!(status.overdue[0].step, BootStep::AwaitReplay);
        assert_eq!(status.overdue[0].policy, StepPolicy::FailOpen);
        assert_ne!(
            status.node,
            BootState::Failed,
            "replay that is merely slow is still making progress"
        );
        assert_eq!(status.step, Some(BootStep::AwaitReplay));
    }

    #[test]
    fn history_proportional_journal_assembly_can_finish_after_reporting_overdue() {
        let (progress, _rx) = overdue_progress();
        progress.advance(BootState::Reachable).unwrap();
        progress.advance(BootState::AdmittingStorage).unwrap();
        let guard = progress.begin(BootStep::AssembleJournals);
        assert!(!progress.check_step_bound());
        let status = progress.status();
        assert_eq!(status.node, BootState::AdmittingStorage);
        assert!(!status.is_serving());
        assert_eq!(status.overdue[0].policy, StepPolicy::FailOpen);
        guard.finish();
        progress.advance(BootState::LocalTruth).unwrap();
        assert_eq!(progress.status().node, BootState::LocalTruth);
        assert!(!progress.status().is_serving());
        // Actual validation failure still fences the boot independently of time.
        progress.advance(BootState::Failed).unwrap();
        assert!(progress.advance(BootState::Serving).is_err());
    }

    #[test]
    fn an_overdue_fail_closed_step_fails_the_boot_and_names_itself() {
        let (progress, _rx) = overdue_progress();
        let _guard = progress.begin(BootStep::StartReplication);

        assert!(
            progress.check_step_bound(),
            "a failed boot has nothing left to watch"
        );
        let status = progress.status();
        assert_eq!(status.node, BootState::Failed);
        assert_eq!(status.overdue[0].policy, StepPolicy::FailClosed);
        assert_eq!(
            status.step,
            Some(BootStep::StartReplication),
            "a failed boot must name the step it died in"
        );
    }

    #[test]
    fn an_overdue_step_is_reported_once_not_every_tick() {
        let (progress, _rx) = overdue_progress();
        let _guard = progress.begin(BootStep::AwaitReplay);
        for _ in 0..5 {
            progress.check_step_bound();
        }
        assert_eq!(progress.status().overdue.len(), 1);
    }

    #[test]
    fn a_step_that_finished_or_was_abandoned_is_not_reported() {
        let (progress, _rx) = overdue_progress();
        progress.begin(BootStep::AssembleJournals).finish();
        assert!(!progress.check_step_bound());
        assert!(progress.status().overdue.is_empty());

        // Dropped without finishing — the shutdown paths in `complete_startup`
        // return rather than finishing their guard.
        drop(progress.begin(BootStep::AwaitReplay));
        assert!(!progress.check_step_bound());
        assert!(
            progress.status().overdue.is_empty(),
            "an abandoned step must not fail a boot nobody is running"
        );
    }

    #[test]
    fn every_step_declares_a_bound_and_the_policy_it_was_given() {
        // Spelled out so changing what a stage does when it goes long is a
        // deliberate edit rather than a side effect of adding a step.
        let expected = [
            (BootStep::AdmitColdCatalog, StepPolicy::FailOpen),
            (BootStep::RepairHotSuffix, StepPolicy::FailClosed),
            (BootStep::RecoverProjection, StepPolicy::FailOpen),
            (BootStep::OpenProjection, StepPolicy::FailOpen),
            (BootStep::AssembleJournals, StepPolicy::FailOpen),
            (BootStep::StartReplication, StepPolicy::FailClosed),
            (BootStep::ResetProjection, StepPolicy::FailOpen),
            (BootStep::StartBackground, StepPolicy::FailClosed),
            (BootStep::ReplayPartitions, StepPolicy::FailClosed),
            (BootStep::AwaitReplay, StepPolicy::FailOpen),
        ];
        assert_eq!(expected.len(), BootStep::ALL.len());
        for (step, policy) in expected {
            assert_eq!(step.policy(), policy, "policy for {}", step.as_str());
            assert!(
                !step.bound().is_zero(),
                "{} declares no bound",
                step.as_str()
            );
        }
    }

    #[test]
    fn clones_write_the_same_machine() {
        let (progress, _rx) = BootProgress::new(1);
        let clone = progress.clone();
        progress.advance(BootState::Reachable).unwrap();
        clone.advance(BootState::AdmittingStorage).unwrap();
        assert_eq!(progress.status().node, BootState::AdmittingStorage);
        assert_eq!(clone.history().len(), 2);
    }
}
