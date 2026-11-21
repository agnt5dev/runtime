//! The instance supervisor: the only thing that starts an always-on agent
//! instance's runs.
//!
//! It runs on the partition leader once replay has caught up, like the
//! terminal supervisor and the dispatch drainer. It reads the partition's
//! instance obligations, each one an instance that is idle, not stopped, and
//! has items waiting, and appends the instance's next `run.queued`.
//!
//! The start's run id is derived from the instance and its next sequence
//! number, so two attempts at the same decision (a retry after failover, a
//! wake and a sweep racing) append the same record and the journal keeps the
//! first. Whether the start takes effect is decided again when the record is
//! applied, against the instance as it is then: a start the instance can no
//! longer take is a no-op that uses up its sequence, and the next decision
//! gets a fresh id.
//!
//! It also resumes an instance's paused run once its answer has arrived, and
//! holds the active run of a stopped instance: a `run.paused` fenced by the
//! running attempt's lease, so that attempt can write nothing more. The hold
//! doubles as the run's resume request, so the lift continues the run as a new
//! attempt on the same deployment.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use engine_core::error::{EngineError, EngineResult};
use engine_core::instance::{
    InstanceKey, InstanceRecord, StoredInboxItem, StoredResumeRequest, INSTANCE_DECISION_HOLD,
    INSTANCE_DECISION_RESUME, INSTANCE_DECISION_START, META_INSTANCE_DECISION,
    META_INSTANCE_INBOX_RUN_ID, META_INSTANCE_ITEM_IDS, META_INSTANCE_REJECTED_DECISIONS,
    META_INSTANCE_RESUME_REQUEST_OFFSET, META_INSTANCE_SEQUENCE,
};
use engine_core::instance_placement::{inbox_run_id, instance_run_id, InstanceIdentity};
use engine_core::journal_contract::META_INBOX_SOURCE_ID;
use engine_core::lease_fence::{
    strip_reserved_fence_metadata, LeaseExpiryReason, LeaseFence, LeaseFenceKind,
    RUN_META_LEASE_SESSION_HASH,
};
use engine_core::storage_contract::InstanceSupervisorStore;
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_proto::Run;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::projections::instances::needs_decision;

const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_BATCH_SIZE: usize = 100;
const ENGINE_PARTITIONS_ENV: &str = "AGNT5_ENGINE_PARTITIONS";

/// Item metadata that describes the item, not the run that takes it.
const ITEM_ONLY_METADATA: &[&str] = &[
    META_INBOX_SOURCE_ID,
    "inbox_channel",
    "gateway_received_at_ns",
];

#[derive(Debug, Clone, Copy)]
pub struct InstanceSupervisorConfig {
    /// The backstop sweep. The wake signal from the processor is what starts
    /// runs promptly; the sweep repairs a missed wake and a new leader.
    pub sweep_interval: Duration,
    pub batch_size: usize,
}

impl Default for InstanceSupervisorConfig {
    fn default() -> Self {
        Self {
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

/// The supervisor's choice for one instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartDecision {
    pub sequence: u64,
    pub conversation_id: String,
    /// The items the run takes, oldest first.
    pub item_ids: Vec<String>,
}

/// Decide what an instance does next, from its record and its waiting items
/// (in the record's order). Phase 0 has one decision: start the next run with
/// the oldest waiting item and every later one in the same conversation.
pub fn decide(record: &InstanceRecord, waiting: &[StoredInboxItem]) -> Option<StartDecision> {
    if !needs_decision(record) {
        return None;
    }
    let first = waiting.first()?;
    let conversation_id = first.item.conversation_id.clone();
    let item_ids = waiting
        .iter()
        .filter(|stored| stored.item.conversation_id == conversation_id)
        .map(|stored| stored.item.item_id.clone())
        .collect();
    Some(StartDecision {
        sequence: record.next_run_sequence,
        conversation_id,
        item_ids,
    })
}

/// The `run.queued` that carries a start decision. Everything in its payload
/// comes from the store, so a repeated decision builds the same bytes and the
/// journal absorbs it.
pub fn start_record(
    key: &InstanceKey,
    record: &InstanceRecord,
    decision: &StartDecision,
    waiting: &[StoredInboxItem],
    partition_id: u32,
    partition_count: usize,
) -> EngineResult<Record> {
    let identity = InstanceIdentity::new(
        key.project_id.as_str(),
        key.environment_id.as_str(),
        key.agent_name.as_str(),
        key.inbox_key.as_str(),
        record.generation,
    )?;
    let run_id = instance_run_id(&identity, decision.sequence, partition_id, partition_count)?;
    let inbox = inbox_run_id(&identity, partition_id, partition_count)?;
    let taken: Vec<&StoredInboxItem> = decision
        .item_ids
        .iter()
        .map(|id| {
            waiting
                .iter()
                .find(|stored| &stored.item.item_id == id)
                .ok_or_else(|| EngineError::Internal(format!("decision names unknown item {id}")))
        })
        .collect::<EngineResult<_>>()?;
    let first = taken
        .first()
        .ok_or_else(|| EngineError::Internal("a start needs at least one item".into()))?;

    let input = serde_json::json!({
        "conversation_id": decision.conversation_id,
        "inbox_items": taken
            .iter()
            .map(|stored| serde_json::json!({
                "item_id": stored.item.item_id,
                "conversation_id": stored.item.conversation_id,
                "channel": stored.item.channel,
                "content": stored.content,
            }))
            .collect::<Vec<_>>(),
    });
    let data = serde_json::to_vec(&input)
        .map_err(|error| EngineError::Internal(format!("encode instance run input: {error}")))?;

    // The first item's metadata is the template: the dispatch route, the
    // deployment pin and the caller context a run queued by the gateway would
    // have carried.
    let mut metadata: HashMap<String, String> = first
        .run_metadata
        .iter()
        .filter(|(key, _)| !ITEM_ONLY_METADATA.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    // A queued run without its deployment pin can't be routed, and the
    // processor refuses to project one; refuse it here, where it is only
    // skipped, instead of in the projection, where it would stall the
    // partition.
    if metadata
        .get("deployment_id")
        .is_none_or(|deployment| deployment.is_empty())
    {
        return Err(EngineError::InvalidArgument(format!(
            "item {} carries no deployment to start the run on",
            first.item.item_id
        )));
    }
    metadata.insert("project_id".to_string(), key.project_id.clone());
    // The instance's key names its agent, whatever the item carried.
    metadata.insert("component_name".to_string(), key.agent_name.clone());
    metadata
        .entry("component_type".to_string())
        .or_insert_with(|| "agent".to_string());
    metadata.insert("root_run_id".to_string(), run_id.clone());
    metadata.insert("user_id".to_string(), key.inbox_key.clone());
    metadata.insert(META_INSTANCE_INBOX_RUN_ID.to_string(), inbox);
    metadata.insert(
        META_INSTANCE_DECISION.to_string(),
        INSTANCE_DECISION_START.to_string(),
    );
    metadata.insert(
        META_INSTANCE_SEQUENCE.to_string(),
        decision.sequence.to_string(),
    );
    metadata.insert(
        META_INSTANCE_ITEM_IDS.to_string(),
        decision.item_ids.join(","),
    );

    Ok(Record {
        offset: Offset::ZERO,
        project_id: key.project_id.clone(),
        run_id: run_id.clone(),
        event_type: "run.queued".to_string(),
        data: data.into(),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: Some(run_id.chars().take(8).collect()),
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

/// The leader's `run.resumed` for a pending resume request: the record the
/// path that got the answer would have written, marked as the leader's
/// decision. Its idempotency slot is the request plus the rejected count, so a
/// repeated decision collides and one retried after a rejection doesn't.
pub fn resume_record(
    key: &InstanceKey,
    record: &InstanceRecord,
    request: &StoredResumeRequest,
    partition_id: u32,
    partition_count: usize,
) -> EngineResult<Record> {
    let identity = InstanceIdentity::new(
        key.project_id.as_str(),
        key.environment_id.as_str(),
        key.agent_name.as_str(),
        key.inbox_key.as_str(),
        record.generation,
    )?;
    let mut metadata: HashMap<String, String> = request
        .metadata
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    metadata.insert(
        META_INSTANCE_INBOX_RUN_ID.to_string(),
        inbox_run_id(&identity, partition_id, partition_count)?,
    );
    metadata.insert(
        META_INSTANCE_DECISION.to_string(),
        INSTANCE_DECISION_RESUME.to_string(),
    );
    metadata.insert(
        META_INSTANCE_RESUME_REQUEST_OFFSET.to_string(),
        request.request_offset.to_string(),
    );
    metadata.insert(
        META_INSTANCE_REJECTED_DECISIONS.to_string(),
        record.rejected_decisions.to_string(),
    );
    Ok(Record {
        offset: Offset::ZERO,
        project_id: key.project_id.clone(),
        run_id: request.run_id.clone(),
        event_type: "run.resumed".to_string(),
        data: request
            .data()
            .map_err(|error| EngineError::Internal(error.to_string()))?
            .into(),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

/// What one pass over the partition's obligations did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorPass {
    pub obligations: usize,
    pub appended: usize,
    pub deduplicated: usize,
    pub skipped: usize,
}

/// Read every instance obligation on the partition and append each due
/// instance's next start.
pub async fn poll_once<M: InstanceSupervisorStore + ?Sized>(
    partition_id: u32,
    partition_count: usize,
    store: &M,
    segment: &dyn Segment,
    config: InstanceSupervisorConfig,
) -> EngineResult<SupervisorPass> {
    let mut pass = SupervisorPass::default();
    let limit = config.batch_size.max(1);
    let mut cursor: Option<InstanceKey> = None;
    loop {
        let page = store
            .scan_instance_obligations(partition_id, limit, cursor.as_ref())
            .await?;
        let full = page.len() == limit;
        for (key, obligation) in &page {
            pass.obligations += 1;
            match decide_and_append(partition_id, partition_count, store, segment, key).await {
                Ok(Some((kind, appended))) => {
                    if appended {
                        pass.appended += 1;
                    } else {
                        pass.deduplicated += 1;
                    }
                    let delay_ms = appended
                        .then(|| obligation_delay_ms(obligation, now_unix_ms()))
                        .flatten();
                    crate::metrics::record_instance_decision(
                        partition_id,
                        kind,
                        if appended { "appended" } else { "deduplicated" },
                        delay_ms,
                    );
                }
                Ok(None) => pass.skipped += 1,
                Err(error) => {
                    pass.skipped += 1;
                    crate::metrics::record_instance_decision(
                        partition_id,
                        "unknown",
                        "error",
                        None,
                    );
                    warn!(
                        partition_id,
                        project_id = %key.project_id,
                        agent = %key.agent_name,
                        error = %error,
                        "instance decision not appended"
                    );
                }
            }
        }
        cursor = page.last().map(|(key, _)| key.clone());
        if !full {
            crate::metrics::record_instance_obligations(partition_id, pass.obligations);
            crate::metrics::record_instance_guard_refusals(
                engine_core::journal_contract::take_instance_guard_refusals(),
            );
            return Ok(pass);
        }
    }
}

/// How long an obligation has stood: from the record that made the instance
/// need a decision, which the hook keeps in the obligation's data.
fn obligation_delay_ms(obligation: &[u8], now_ms: i64) -> Option<i64> {
    let since_ns = serde_json::from_slice::<serde_json::Value>(obligation)
        .ok()?
        .get("timestamp_ns")?
        .as_i64()
        .filter(|since| *since > 0)?;
    Some(now_ms.saturating_sub(since_ns / 1_000_000))
}

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// The decision taken (start, resume or hold) and whether the journal
/// appended it (`false` when it already held it), or `None` when there was
/// nothing to do yet.
async fn decide_and_append<M: InstanceSupervisorStore + ?Sized>(
    partition_id: u32,
    partition_count: usize,
    store: &M,
    segment: &dyn Segment,
    key: &InstanceKey,
) -> EngineResult<Option<(&'static str, bool)>> {
    let Some(data) = store.load_instance(key).await? else {
        return Ok(None);
    };
    let record = InstanceRecord::decode(&data)
        .map_err(|error| EngineError::Internal(format!("decode instance record: {error}")))?;
    if !needs_decision(&record) {
        return Ok(None);
    }
    if record.stopped {
        return Ok(hold_active_run(partition_id, store, segment, key, &record)
            .await?
            .map(|appended| ("hold", appended)));
    }
    // A paused active run with an answer waiting is resumed; nothing else can
    // happen while it holds the turn.
    if let Some(pending) = record.resume_requested.as_ref() {
        let Some(stored) = store.load_resume_request(key).await? else {
            return Err(EngineError::Internal(format!(
                "resume request for {} has no stored record",
                pending.run_id
            )));
        };
        let request: StoredResumeRequest = serde_json::from_slice(&stored)
            .map_err(|error| EngineError::Internal(format!("decode resume request: {error}")))?;
        let mut resume = resume_record(key, &record, &request, partition_id, partition_count)?;
        resume.timestamp_ns = now_unix_ms().saturating_mul(1_000_000);
        debug!(partition_id, run_id = %resume.run_id, "instance resume");
        let outcome = segment.append_batch_with_outcome(&[resume]).await?;
        return Ok(Some(("resume", outcome.appended.first() == Some(&true))));
    }
    let mut waiting = Vec::with_capacity(record.waiting_item_ids.len());
    for item_id in &record.waiting_item_ids {
        let Some(stored) = store.load_inbox_item(key, item_id).await? else {
            return Err(EngineError::Internal(format!(
                "waiting item {item_id} has no stored content"
            )));
        };
        waiting.push(
            serde_json::from_slice::<StoredInboxItem>(&stored).map_err(|error| {
                EngineError::Internal(format!("decode waiting item {item_id}: {error}"))
            })?,
        );
    }
    let Some(decision) = decide(&record, &waiting) else {
        return Ok(None);
    };
    let (decision, mut start) = fitting_start(
        key,
        &record,
        decision,
        &waiting,
        partition_id,
        partition_count,
    )?;
    start.timestamp_ns = now_unix_ms().saturating_mul(1_000_000);
    debug!(
        partition_id,
        run_id = %start.run_id,
        sequence = decision.sequence,
        items = decision.item_ids.len(),
        "instance start"
    );
    let outcome = segment.append_batch_with_outcome(&[start]).await?;
    Ok(Some(("start", outcome.appended.first() == Some(&true))))
}

/// Hold a stopped instance's active run, ending the attempt it holds. Waits,
/// returning `None`, while the run holds no lease to fence against, such as
/// between a retry's queueing and its dispatch; the sweep comes back to it.
async fn hold_active_run<M: InstanceSupervisorStore + ?Sized>(
    partition_id: u32,
    store: &M,
    segment: &dyn Segment,
    key: &InstanceKey,
    record: &InstanceRecord,
) -> EngineResult<Option<bool>> {
    let Some(run_id) = record.active_run_id.as_deref() else {
        return Ok(None);
    };
    let Some(data) = store.load_instance_run(&key.project_id, run_id).await? else {
        return Ok(None);
    };
    let run = <Run as prost::Message>::decode(data.as_slice())
        .map_err(|error| EngineError::Internal(format!("decode run {run_id}: {error}")))?;
    let now_ms = now_unix_ms();
    let Some(hold) = hold_record(&run, now_ms) else {
        debug!(partition_id, run_id, status = %run.status, "instance hold waits for a lease");
        return Ok(None);
    };
    // The attempts the hold fences off are settled in the same batch, so the
    // attempt that continues the run after the lift doesn't wait on them.
    let states = segment
        .list_nonterminal_activation_authority(&run.project_id, &run.run_id)
        .await?;
    let mut batch = engine_core::activation::stop_failures(&states, now_ms * 1_000_000)
        .map_err(|error| EngineError::Internal(format!("settle stopped attempts: {error}")))?;
    batch.push(hold);
    info!(partition_id, run_id, "instance run held for a stop");
    let outcome = segment.append_batch_with_outcome(&batch).await?;
    Ok(Some(outcome.appended.last() == Some(&true)))
}

/// The leader's hold of a running instance run: a `run.paused` fenced by the
/// lease the projection shows, as an operator stop. `None` when the run holds
/// no lease. The journal decides the fence: if the worker's own completion or
/// pause landed first, the hold loses and nothing is written.
pub fn hold_record(run: &Run, now_ms: i64) -> Option<Record> {
    if run.lease_id.is_empty() || run.lease_worker_id.is_empty() {
        return None;
    }
    let mut metadata = run.metadata.clone();
    strip_reserved_fence_metadata(&mut metadata);
    // A hold waits for the lift, not for an answer with a deadline.
    metadata.remove(crate::run_state_machine::WAIT_TIMEOUT_MS_META);
    metadata.insert("component_name".to_string(), run.component_name.clone());
    metadata.insert(
        META_INSTANCE_DECISION.to_string(),
        INSTANCE_DECISION_HOLD.to_string(),
    );
    let mut hold = Record {
        offset: Offset::ZERO,
        project_id: run.project_id.clone(),
        run_id: run.run_id.clone(),
        event_type: "run.paused".to_string(),
        data: serde_json::to_vec(&serde_json::json!({ "reason": "instance_stopped" }))
            .unwrap_or_default()
            .into(),
        timestamp_ns: now_ms.saturating_mul(1_000_000),
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
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
        expiry_reason: Some(LeaseExpiryReason::OperatorStop),
    }
    .stamp(&mut hold);
    Some(hold)
}

/// The decision's start, with only as many of its items as fit in one journal
/// record. Each item was admitted on its own, so several large ones in one
/// conversation could together exceed the limit, and a start that can never
/// be appended would block the instance for good. The newest items are left
/// waiting for the next run. Deterministic in the waiting items, so every
/// leader builds the same start for the same sequence.
pub fn fitting_start(
    key: &InstanceKey,
    record: &InstanceRecord,
    mut decision: StartDecision,
    waiting: &[StoredInboxItem],
    partition_id: u32,
    partition_count: usize,
) -> EngineResult<(StartDecision, Record)> {
    loop {
        let start = start_record(
            key,
            record,
            &decision,
            waiting,
            partition_id,
            partition_count,
        )?;
        if decision.item_ids.len() == 1
            || start.decoded_size_bytes() <= engine_core::types::MAX_JOURNAL_RECORD_BYTES
        {
            return Ok((decision, start));
        }
        decision.item_ids.pop();
    }
}

/// The partition count instance run ids are placed for: the segment's own
/// topology when it routes, otherwise the configured count every node shares.
pub fn partition_count(segment: &dyn Segment) -> usize {
    let routed = segment.partition_count();
    if routed > 1 {
        return routed;
    }
    std::env::var(ENGINE_PARTITIONS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|count| *count > 0)
        .unwrap_or(1)
}

#[allow(clippy::too_many_arguments)]
pub async fn run_partition_instance_supervisor<M: InstanceSupervisorStore + ?Sized + 'static>(
    partition_id: u32,
    node_id: String,
    store: Arc<M>,
    segment: Arc<dyn Segment>,
    replay_ready: watch::Receiver<bool>,
    is_leader: watch::Receiver<bool>,
    wake: Arc<Notify>,
    cancel: CancellationToken,
    config: InstanceSupervisorConfig,
) {
    if !store.supports_instances() {
        return;
    }
    let partition_count = partition_count(segment.as_ref());
    info!(
        partition_id,
        node_id, partition_count, "instance supervisor starting"
    );
    let mut sweep = tokio::time::interval(config.sweep_interval);
    sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = wake.notified() => {}
            _ = sweep.tick() => {}
        }
        if !*replay_ready.borrow() || !*is_leader.borrow() {
            continue;
        }
        match poll_once(
            partition_id,
            partition_count,
            store.as_ref(),
            segment.as_ref(),
            config,
        )
        .await
        {
            Ok(pass) if pass.obligations > 0 => {
                debug!(partition_id, ?pass, "instance supervisor pass")
            }
            Ok(_) => {}
            Err(error) => {
                error!(partition_id, error = %error, "instance supervisor pass failed")
            }
        }
    }
    info!(partition_id, node_id, "instance supervisor stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_obligations_delay_runs_from_the_record_that_raised_it() {
        let obligation = serde_json::to_vec(&serde_json::json!({
            "trigger": "inbox.item",
            "offset": 7,
            "timestamp_ns": 1_000_000_000i64,
        }))
        .unwrap();
        assert_eq!(obligation_delay_ms(&obligation, 1_250), Some(250));
        let untimed = serde_json::to_vec(&serde_json::json!({"timestamp_ns": 0})).unwrap();
        assert_eq!(obligation_delay_ms(&untimed, 1_250), None);
        assert_eq!(obligation_delay_ms(b"not json", 1_250), None);
    }
    use engine_core::instance::InboxItem;
    use engine_core::instance_placement::{inbox_item_id, is_instance_run_id, parse_inbox_run_id};
    use engine_core::routing::partition_for_run;
    use std::collections::BTreeMap;

    fn key() -> InstanceKey {
        InstanceKey {
            project_id: "project-1".into(),
            environment_id: "env-1".into(),
            agent_name: "support".into(),
            inbox_key: "u_123".into(),
        }
    }

    fn stored(n: u64, conversation: &str) -> StoredInboxItem {
        StoredInboxItem {
            item: InboxItem {
                item_id: inbox_item_id("inbox", &n.to_string()),
                source_id: n.to_string(),
                inbox_key: "u_123".into(),
                generation: 1,
                conversation_id: conversation.into(),
                channel: "api".into(),
                create_if_absent: true,
            },
            content: serde_json::json!({"message": n}),
            run_metadata: BTreeMap::from([
                ("deployment_id".to_string(), "dep-1".to_string()),
                ("dispatch_mode".to_string(), "pull".to_string()),
                (META_INBOX_SOURCE_ID.to_string(), n.to_string()),
                ("inbox_channel".to_string(), "api".to_string()),
            ]),
        }
    }

    fn idle(waiting: &[StoredInboxItem]) -> InstanceRecord {
        let mut record = InstanceRecord::new(key(), 1);
        record.waiting_item_ids = waiting.iter().map(|s| s.item.item_id.clone()).collect();
        record.next_run_sequence = 4;
        record
    }

    #[test]
    fn a_start_takes_the_oldest_items_conversation() {
        let waiting = vec![stored(1, "c1"), stored(2, "c2"), stored(3, "c1")];
        let decision = decide(&idle(&waiting), &waiting).unwrap();
        assert_eq!(decision.sequence, 4);
        assert_eq!(decision.conversation_id, "c1");
        assert_eq!(
            decision.item_ids,
            vec![
                waiting[0].item.item_id.clone(),
                waiting[2].item.item_id.clone()
            ]
        );
    }

    #[test]
    fn a_start_takes_only_as_many_items_as_fit_in_one_record() {
        let big = |n: u64| {
            let mut item = stored(n, "c1");
            item.content = serde_json::json!({"message": "x".repeat(600 * 1024)});
            item
        };
        let waiting = vec![big(1), big(2), stored(3, "c1")];
        let record = idle(&waiting);
        let decision = decide(&record, &waiting).unwrap();
        assert_eq!(decision.item_ids.len(), 3);
        let (fitted, start) = fitting_start(&key(), &record, decision, &waiting, 0, 1).unwrap();
        assert_eq!(fitted.item_ids, vec![waiting[0].item.item_id.clone()]);
        assert!(start.decoded_size_bytes() <= engine_core::types::MAX_JOURNAL_RECORD_BYTES);
        assert_eq!(
            start.metadata[META_INSTANCE_ITEM_IDS],
            fitted.item_ids.join(",")
        );

        // Small items all fit.
        let small = vec![stored(1, "c1"), stored(2, "c1")];
        let decision = decide(&idle(&small), &small).unwrap();
        let (fitted, _) = fitting_start(&key(), &idle(&small), decision, &small, 0, 1).unwrap();
        assert_eq!(fitted.item_ids.len(), 2);
    }

    #[test]
    fn nothing_is_decided_for_a_busy_stopped_or_empty_instance() {
        let waiting = vec![stored(1, "c1")];
        let mut busy = idle(&waiting);
        busy.active_run_id = Some("run".into());
        assert_eq!(decide(&busy, &waiting), None);
        let mut stopped = idle(&waiting);
        stopped.stopped = true;
        assert_eq!(decide(&stopped, &waiting), None);
        assert_eq!(decide(&idle(&[]), &[]), None);
    }

    #[test]
    fn the_start_is_deterministic_marked_and_placed() {
        let waiting = vec![stored(1, "c1"), stored(2, "c1")];
        let record = idle(&waiting);
        let decision = decide(&record, &waiting).unwrap();
        let start = start_record(&key(), &record, &decision, &waiting, 3, 8).unwrap();
        let again = start_record(&key(), &record, &decision, &waiting, 3, 8).unwrap();
        assert_eq!(start.run_id, again.run_id);
        assert_eq!(start.data, again.data);
        assert!(is_instance_run_id(&start.run_id));
        assert_eq!(partition_for_run(&start.run_id, 8), 3);

        let meta = &start.metadata;
        assert_eq!(meta[META_INSTANCE_DECISION], INSTANCE_DECISION_START);
        assert_eq!(meta[META_INSTANCE_SEQUENCE], "4");
        assert_eq!(meta[META_INSTANCE_ITEM_IDS], decision.item_ids.join(","));
        assert_eq!(meta["root_run_id"], start.run_id);
        assert_eq!(meta["component_name"], "support");
        assert_eq!(
            meta["dispatch_mode"], "pull",
            "the item's route carries over"
        );
        assert!(
            !meta.contains_key(META_INBOX_SOURCE_ID),
            "item-only metadata stays behind"
        );
        let inbox = parse_inbox_run_id(&meta[META_INSTANCE_INBOX_RUN_ID]).unwrap();
        assert_eq!(inbox.instance_key(), key());
        assert_eq!(partition_for_run(&meta[META_INSTANCE_INBOX_RUN_ID], 8), 3);

        // The next sequence is a different run.
        let mut later = record.clone();
        later.next_run_sequence = 5;
        let next = start_record(
            &key(),
            &later,
            &decide(&later, &waiting).unwrap(),
            &waiting,
            3,
            8,
        )
        .unwrap();
        assert_ne!(next.run_id, start.run_id);
    }
}
