use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use engine_core::error::EngineError;
use engine_core::lease_fence::{
    strip_reserved_fence_metadata, LeaseExpiryReason, LeaseFence, LeaseFenceKind,
    RUN_META_LEASE_SESSION_HASH,
};
use engine_core::storage_contract::LeaseWorkStore;
use engine_core::traits::Segment;
use engine_core::types::{LeaseExpiryCursor, Offset, Record};
use engine_proto::Run;
use prost::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const SWEEP_LIMIT: usize = 500;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpiryAction {
    Requeue,
    DeadLetter,
}

pub async fn run<J, S>(
    journal: Arc<J>,
    storage: Arc<S>,
    max_attempts: u32,
    cancel: CancellationToken,
) where
    J: Segment + 'static,
    S: LeaseWorkStore + 'static,
{
    info!(
        max_attempts,
        interval_seconds = SWEEP_INTERVAL.as_secs(),
        batch_limit = SWEEP_LIMIT,
        "OSS lease expiry sweeper started"
    );
    let mut cursor = None;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("OSS lease expiry sweeper stopped");
                return;
            }
            _ = tokio::time::sleep(SWEEP_INTERVAL) => {
                match sweep_once(journal.as_ref(), storage.as_ref(), max_attempts, &mut cursor).await {
                    Ok(stats) if stats.total() > 0 => {
                        info!(
                            requeued = stats.requeued,
                            dead_lettered = stats.dead_lettered,
                            quarantined = stats.quarantined,
                            skipped = stats.skipped,
                            fence_lost = stats.fence_lost,
                            "OSS lease expiry sweep complete"
                        );
                    }
                    Ok(_) => debug!("OSS lease expiry sweep found no due leases"),
                    Err(error) => warn!(%error, "OSS lease expiry sweep failed; retrying"),
                }
            }
        }
    }
}

#[derive(Default)]
struct SweepStats {
    requeued: usize,
    dead_lettered: usize,
    quarantined: usize,
    skipped: usize,
    /// Expiries the segment rejected at append time because the lease entry
    /// no longer matched. Nothing was written and nothing needs retrying.
    fence_lost: usize,
}

impl SweepStats {
    fn total(&self) -> usize {
        self.requeued + self.dead_lettered + self.quarantined + self.skipped + self.fence_lost
    }
}

async fn sweep_once<J, S>(
    journal: &J,
    storage: &S,
    max_attempts: u32,
    cursor: &mut Option<LeaseExpiryCursor>,
) -> Result<SweepStats, String>
where
    J: Segment,
    S: LeaseWorkStore,
{
    let now_ms = now_ms();
    let rows = storage
        .scan_expired_leases(now_ms, SWEEP_LIMIT, cursor.clone())
        .await
        .map_err(|error| error.to_string())?;
    *cursor = (rows.len() == SWEEP_LIMIT)
        .then(|| rows.last().cloned())
        .flatten();
    let mut stats = SweepStats::default();

    for (expires_at_ms, project_id, run_id) in rows {
        let bytes = match storage.load_lease_run(&project_id, &run_id).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                quarantine(storage, expires_at_ms, &project_id, &run_id).await?;
                stats.quarantined += 1;
                continue;
            }
            Err(error) => {
                warn!(%project_id, %run_id, %error, "unable to load expired lease run");
                stats.skipped += 1;
                continue;
            }
        };
        let run = match Run::decode(bytes.as_slice()) {
            Ok(run) => run,
            Err(error) => {
                warn!(%project_id, %run_id, %error, "invalid run row for expired lease");
                stats.skipped += 1;
                continue;
            }
        };

        if run.lease_id.is_empty() || run.lease_expires_at_ms != expires_at_ms {
            quarantine(storage, expires_at_ms, &project_id, &run_id).await?;
            stats.quarantined += 1;
            continue;
        }
        if run.lease_expires_at_ms > now_ms {
            stats.skipped += 1;
            continue;
        }

        let action = decide_action(run.attempt, max_attempts);
        let desired = match action {
            ExpiryAction::Requeue => lease_expired_record(&run, now_ms.saturating_mul(1_000_000)),
            ExpiryAction::DeadLetter => {
                dead_letter_record(&run, max_attempts, now_ms.saturating_mul(1_000_000))
            }
        };
        let Some(record) = fenced_expiry(desired, &run, now_ms) else {
            warn!(%project_id, %run_id, "expired lease had incomplete fencing metadata");
            stats.skipped += 1;
            continue;
        };
        match journal.append_batch(std::slice::from_ref(&record)).await {
            Ok(_) => match action {
                ExpiryAction::Requeue => stats.requeued += 1,
                ExpiryAction::DeadLetter => stats.dead_lettered += 1,
            },
            Err(error @ EngineError::LeaseFenceMismatch { .. }) => {
                debug!(%project_id, %run_id, %error, "lease expiry lost its append-time fence");
                stats.fence_lost += 1;
            }
            Err(error) => {
                warn!(%project_id, %run_id, %error, "unable to journal lease expiry");
                stats.skipped += 1;
            }
        }
    }
    Ok(stats)
}

fn decide_action(attempt: u32, max_attempts: u32) -> ExpiryAction {
    if max_attempts > 0 && attempt >= max_attempts {
        ExpiryAction::DeadLetter
    } else {
        ExpiryAction::Requeue
    }
}

fn lease_expired_record(run: &Run, timestamp_ns: i64) -> Record {
    let mut metadata = HashMap::new();
    if !run.component_name.is_empty() {
        metadata.insert("component_name".to_string(), run.component_name.clone());
    }
    Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: "run.lease_expired".to_string(),
        data: Bytes::new(),
        timestamp_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

fn dead_letter_record(run: &Run, max_attempts: u32, timestamp_ns: i64) -> Record {
    let mut metadata = HashMap::new();
    metadata.insert("component_name".to_string(), run.component_name.clone());
    metadata.insert("component_type".to_string(), run.component_type.clone());
    metadata.insert("dead_letter_reason".to_string(), "lease_expiry".to_string());
    metadata.insert(
        "error_code".to_string(),
        "lease_max_attempts_exceeded".to_string(),
    );
    // Name earlier attempts' worker terminations, as the managed runtime's
    // failures do.
    let earlier =
        engine_processor::run_state_machine::lease_worker_terminations_suffix(&run.metadata);
    let data = serde_json::to_vec(&serde_json::json!({
        "error_code": "lease_max_attempts_exceeded",
        "error_message": format!(
            "Run dead-lettered after {} delivery attempts (max_attempts={max_attempts}){earlier}",
            run.attempt
        ),
    }))
    .unwrap_or_default();
    Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: "run.failed".to_string(),
        data: Bytes::from(data),
        timestamp_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

/// Fence the expiry with the lease the projection shows. The segment decides
/// it against the lease entry in log order: a renewal or completion that
/// landed first makes this record lose at append time, and nothing is written.
fn fenced_expiry(mut desired: Record, run: &Run, now_ms: i64) -> Option<Record> {
    if run.lease_id.is_empty() || run.lease_worker_id.is_empty() || run.lease_expires_at_ms <= 0 {
        return None;
    }
    strip_reserved_fence_metadata(&mut desired.metadata);
    desired.offset = Offset::ZERO;
    LeaseFence {
        kind: LeaseFenceKind::LeaseExpiry,
        lease_id: run.lease_id.clone(),
        worker_id: run.lease_worker_id.clone(),
        session_hash: run
            .metadata
            .get(RUN_META_LEASE_SESSION_HASH)
            .cloned()
            .unwrap_or_default(),
        attempt: run.attempt,
        received_at_ms: now_ms,
        expiry_reason: Some(LeaseExpiryReason::Timeout),
    }
    .stamp(&mut desired);
    Some(desired)
}

async fn quarantine<S>(
    storage: &S,
    expires_at_ms: i64,
    project_id: &str,
    run_id: &str,
) -> Result<(), String>
where
    S: LeaseWorkStore,
{
    storage
        .quarantine_expiry_entry(expires_at_ms, project_id, run_id)
        .await
        .map_err(|error| error.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_names_earlier_worker_terminations() {
        let mut run = engine_proto::Run {
            project_id: "p".into(),
            run_id: "r".into(),
            attempt: 3,
            ..Default::default()
        };
        engine_processor::run_state_machine::record_lease_worker_termination(
            &mut run.metadata,
            2,
            engine_processor::run_state_machine::WORKER_DISCONNECTED_REASON,
        );
        let record = super::dead_letter_record(&run, 3, 0);
        let payload: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        assert_eq!(
            payload["error_message"],
            "Run dead-lettered after 3 delivery attempts (max_attempts=3) (attempt 2: worker disconnected)"
        );
    }

    #[test]
    fn retry_budget_is_explicit() {
        assert_eq!(decide_action(2, 3), ExpiryAction::Requeue);
        assert_eq!(decide_action(3, 3), ExpiryAction::DeadLetter);
        assert_eq!(decide_action(99, 0), ExpiryAction::Requeue);
    }

    #[test]
    fn expiry_transition_preserves_exact_lease_generation() {
        let mut run = Run {
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            deployment_id: "deployment".to_string(),
            component_name: "handler".to_string(),
            lease_id: "lease".to_string(),
            lease_worker_id: "worker".to_string(),
            lease_expires_at_ms: 123,
            attempt: 2,
            ..Default::default()
        };
        run.metadata.insert(
            RUN_META_LEASE_SESSION_HASH.to_string(),
            "session".to_string(),
        );
        let record = fenced_expiry(lease_expired_record(&run, 1), &run, 456).unwrap();
        assert_eq!(record.event_type, "run.lease_expired");
        let fence = engine_core::lease_fence::lease_fence(&record)
            .expect("fence metadata is well-formed")
            .expect("expiry is fenced");
        assert_eq!(fence.kind, LeaseFenceKind::LeaseExpiry);
        assert_eq!(fence.lease_id, "lease");
        assert_eq!(fence.worker_id, "worker");
        assert_eq!(fence.session_hash, "session");
        assert_eq!(fence.attempt, 2);
        assert_eq!(fence.received_at_ms, 456);
        assert_eq!(fence.expiry_reason, Some(LeaseExpiryReason::Timeout));
    }

    #[test]
    fn expiry_without_lease_identity_is_not_fenced() {
        let run = Run {
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            ..Default::default()
        };
        assert!(fenced_expiry(lease_expired_record(&run, 1), &run, 1).is_none());
    }
}
