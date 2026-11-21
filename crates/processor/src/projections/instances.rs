//! Instance projection.
//!
//! Folds an instance's journal records into its one live record. The fold is
//! pure: it reads only the current record and the journal record, so every
//! replica that applies the same journal in the same order reaches the same
//! record, byte for byte.
//!
//! The obligation hook ([`project_record`]) runs for every journal record the
//! processor applies. It folds instance records and the endings of
//! instance-owned runs into the record, and in the same store write keeps the
//! instance's obligation: present exactly while the instance is idle, not
//! stopped, and has items waiting, which is when its leader must decide.

use engine_core::error::{EngineError, EngineResult};
use engine_core::instance::{
    validate_generation, validate_id, validate_item_id, InboxItem, InstanceCreated, InstanceError,
    InstanceEvent, InstanceKey, InstanceRecord, PendingResume, RunEnd, RunStart, StoredInboxItem,
    StoredResumeRequest, INBOX_ITEM_EVENT, INSTANCE_CREATED_EVENT, INSTANCE_DECISION_HOLD,
    INSTANCE_DECISION_RESUME, INSTANCE_DECISION_START, INSTANCE_RESUME_REQUESTED_EVENT,
    INSTANCE_STOPPED_EVENT, INSTANCE_STOP_LIFTED_EVENT, META_INSTANCE_DECISION,
    META_INSTANCE_INBOX_RUN_ID, META_INSTANCE_ITEM_IDS, META_INSTANCE_RESUME_REQUEST_OFFSET,
    META_INSTANCE_SEQUENCE,
};
use engine_core::instance_placement::{is_instance_run_id, parse_inbox_run_id};
use engine_core::storage_contract::ProjectionStateReader;
use engine_core::types::Record;
use engine_proto::Run;
use prost::Message;
use tracing::warn;

use crate::projections::{ProjectionResult, StoreOp};
use crate::run_state_machine::RunStatus;

/// What applying a run start does to the instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartOutcome {
    /// The run takes its items and the instance's turn. Only an accepted start
    /// may be dispatched to a worker.
    Accepted,
    /// The start was for the next sequence, but the instance changed between
    /// the supervisor's decision and its append. The sequence is used up, the
    /// items keep waiting, and the rejection triggers a fresh decision.
    Rejected(StartRejection),
    /// The start isn't for the next sequence. Its sequence was already used up
    /// by an earlier start, or was never one the supervisor could choose. The
    /// record doesn't change at all, so a duplicate applies as a no-op.
    Stale { expected: u64, got: u64 },
}

/// Why a start for the next sequence was rejected when applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartRejection {
    /// An operator stopped the instance.
    Stopped,
    /// Another run holds the instance's turn, including one still retrying.
    ActiveRun { run_id: String },
    /// A run already took this item, or it never arrived.
    ItemNotWaiting { item_id: String },
    /// The start couldn't be read or checked; it is never dispatched.
    Invalid,
    /// A resume for a run that isn't the active one, a pause that isn't
    /// waiting, or a request already applied.
    ResumeNotPending,
}

/// Apply one instance journal record to the instance's current record.
///
/// Returns the new record, or an error when the journal record is malformed,
/// names a different instance or generation, arrives for an instance in the
/// wrong state (a creation for one that exists; anything but a creation or a
/// trusted item for one that doesn't), or would make the record larger than
/// its 64 KiB limit. On an error the current record is left as it was.
pub fn fold(
    existing: Option<InstanceRecord>,
    event: &InstanceEvent,
) -> Result<InstanceRecord, InstanceError> {
    let record = match (existing, event) {
        (None, InstanceEvent::Created(created)) => create(created)?,
        (Some(record), InstanceEvent::Created(_)) => {
            return Err(InstanceError::AlreadyExists {
                generation: record.generation,
            })
        }
        // The customer's authenticated backend creates an instance on first
        // use. Its gateway can't tell whether the instance exists, so the
        // item says it may create one, and the fold decides here.
        (None, InstanceEvent::ItemAccepted { key, item }) if item.create_if_absent => {
            let mut record = create(&InstanceCreated {
                key: key.clone(),
                generation: item.generation,
            })?;
            accept_item(&mut record, key, item)?;
            record
        }
        (None, other) => {
            return Err(InstanceError::Missing {
                event: other.name(),
            })
        }
        (Some(mut record), event) => {
            apply(&mut record, event)?;
            record
        }
    };
    // Encoding is the size check, so the limit is on the exact bytes stored.
    record.encode()?;
    Ok(record)
}

/// Check a run start against the record at the point it is applied. The
/// sequence is checked first: a start for a sequence already used up must
/// change nothing, even if the instance is stopped or busy by then. Then: not
/// stopped, no run active, and every item still waiting. The projection that
/// dispatches a start uses this too, so only an accepted start reaches a
/// worker.
pub fn check_start(record: &InstanceRecord, start: &RunStart) -> StartOutcome {
    if start.sequence != record.next_run_sequence {
        return StartOutcome::Stale {
            expected: record.next_run_sequence,
            got: start.sequence,
        };
    }
    if record.stopped {
        return StartOutcome::Rejected(StartRejection::Stopped);
    }
    if let Some(run_id) = &record.active_run_id {
        return StartOutcome::Rejected(StartRejection::ActiveRun {
            run_id: run_id.clone(),
        });
    }
    if let Some(item_id) = start
        .item_ids
        .iter()
        .find(|item_id| !record.waiting_item_ids.contains(item_id))
    {
        return StartOutcome::Rejected(StartRejection::ItemNotWaiting {
            item_id: item_id.clone(),
        });
    }
    StartOutcome::Accepted
}

fn create(created: &InstanceCreated) -> Result<InstanceRecord, InstanceError> {
    created.key.validate()?;
    validate_generation(created.generation)?;
    Ok(InstanceRecord::new(created.key.clone(), created.generation))
}

fn apply(record: &mut InstanceRecord, event: &InstanceEvent) -> Result<(), InstanceError> {
    match event {
        InstanceEvent::Created(_) => unreachable!("creation is handled by fold"),
        InstanceEvent::ItemAccepted { key, item } => accept_item(record, key, item)?,
        InstanceEvent::RunStarted(start) => {
            validate_start(start)?;
            match check_start(record, start) {
                StartOutcome::Accepted => {
                    record
                        .waiting_item_ids
                        .retain(|item_id| !start.item_ids.contains(item_id));
                    record.active_run_id = Some(start.run_id.clone());
                    record.next_run_sequence += 1;
                }
                // The run id is derived from the sequence, so the sequence is
                // used up even though the run never starts: the next decision
                // gets a new id instead of colliding with this one.
                StartOutcome::Rejected(_) => {
                    record.next_run_sequence += 1;
                    record.rejected_decisions += 1;
                }
                // Not counted as a rejection: this sequence was already
                // decided, and that decision already triggered whatever
                // followed. Counting it would only change later resume and
                // attempt ids, and a duplicate must apply as a no-op.
                StartOutcome::Stale { .. } => {}
            }
        }
        InstanceEvent::RunEnded(end) => {
            // An ending for any other run, such as a cancelled start that was
            // rejected and never ran, leaves the instance as it is.
            if record.active_run_id.as_deref() == Some(end.run_id.as_str()) {
                record.active_run_id = None;
                record.resume_requested = None;
                record.active_paused = false;
            }
        }
        InstanceEvent::RunPaused { run_id } => {
            if record.active_run_id.as_deref() == Some(run_id.as_str()) {
                record.active_paused = true;
            }
        }
        InstanceEvent::RunHeld(hold) => {
            // The hold is the run's resume request too, unless a request
            // already stands (it can't while the run held an attempt).
            if record.active_run_id.as_deref() == Some(hold.run_id.as_str()) {
                record.active_paused = true;
                if record.resume_requested.is_none() {
                    record.resume_requested = Some(hold.clone());
                }
            }
        }
        InstanceEvent::ResumeRequested(request) => {
            // Only the active run can be resumed, and one request per pause:
            // the first stands until it is applied or the run ends.
            if record.active_run_id.as_deref() == Some(request.run_id.as_str())
                && record.resume_requested.is_none()
            {
                record.resume_requested = Some(request.clone());
            }
        }
        InstanceEvent::ResumeApplied(request) => {
            if record.resume_requested.as_ref() == Some(request) {
                record.resume_requested = None;
                // The resumed run is queued for a new attempt.
                record.active_paused = false;
            }
        }
        InstanceEvent::Stopped => record.stopped = true,
        InstanceEvent::StopLifted => record.stopped = false,
    }
    Ok(())
}

fn accept_item(
    record: &mut InstanceRecord,
    key: &InstanceKey,
    item: &InboxItem,
) -> Result<(), InstanceError> {
    item.validate()?;
    if *key != record.key || item.inbox_key != record.key.inbox_key {
        return Err(InstanceError::Invalid(
            "inbox.item names a different instance".into(),
        ));
    }
    // An item for another generation was meant for a deleted instance, or for
    // one not created yet; neither may land in this one.
    if item.generation != record.generation {
        return Err(InstanceError::Invalid(format!(
            "inbox.item is for generation {}, the instance is at {}",
            item.generation, record.generation
        )));
    }
    // Items wait while the instance is stopped too; only the start is held.
    // An id already waiting is the same item applied again.
    if !record.waiting_item_ids.contains(&item.item_id) {
        record.waiting_item_ids.push(item.item_id.clone());
    }
    Ok(())
}

/// A start the supervisor could never have built: these are refused as
/// malformed, not counted as rejected decisions.
fn validate_start(start: &RunStart) -> Result<(), InstanceError> {
    validate_id("run_id", &start.run_id)?;
    if start.item_ids.is_empty() {
        return Err(InstanceError::Invalid(
            "a run start takes at least one item".into(),
        ));
    }
    for (index, item_id) in start.item_ids.iter().enumerate() {
        validate_item_id(item_id)?;
        if start.item_ids[..index].contains(item_id) {
            return Err(InstanceError::Invalid(format!(
                "a run start names item {item_id} twice"
            )));
        }
    }
    Ok(())
}

/// The instance a journal record changes, and how.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstanceChange {
    pub key: InstanceKey,
    pub event: InstanceEvent,
}

/// Find the instance change a journal record makes, if any.
///
/// The instance's own records (`instance.created`, `inbox.item`, stop and
/// lift) live under its inbox id, which names the instance. An instance-owned
/// run's ending is found from the run as projected: it ends for good when its
/// status becomes completed, failed or cancelled (a failure with attempts left
/// is still retrying), and its run carries the inbox it belongs to.
pub fn instance_change(
    record: &Record,
    existing_run: Option<&[u8]>,
    run_result: &ProjectionResult,
) -> Result<Option<InstanceChange>, InstanceError> {
    if matches!(
        record.event_type.as_str(),
        INSTANCE_CREATED_EVENT
            | INBOX_ITEM_EVENT
            | INSTANCE_STOPPED_EVENT
            | INSTANCE_STOP_LIFTED_EVENT
    ) {
        let key = parse_inbox_run_id(&record.run_id)
            .ok_or_else(|| InstanceError::Invalid("record is not under an inbox id".into()))?
            .instance_key();
        return Ok(
            InstanceEvent::from_journal(&key, &record.event_type, &record.data)?
                .map(|event| InstanceChange { key, event }),
        );
    }
    if !is_instance_run_id(&record.run_id) {
        return Ok(None);
    }
    if record.event_type == INSTANCE_RESUME_REQUESTED_EVENT {
        let key = existing_run
            .and_then(|data| Run::decode(data).ok())
            .and_then(|run| run.metadata.get(META_INSTANCE_INBOX_RUN_ID).cloned())
            .and_then(|inbox| parse_inbox_run_id(&inbox))
            .ok_or_else(|| InstanceError::Invalid("resume names no instance run".into()))?
            .instance_key();
        return Ok(Some(InstanceChange {
            key,
            event: InstanceEvent::ResumeRequested(PendingResume {
                run_id: record.run_id.clone(),
                request_offset: record.offset.0,
            }),
        }));
    }
    let Some(run) = run_result
        .run_data()
        .and_then(|data| Run::decode(data).ok())
    else {
        return Ok(None);
    };
    let ended = |status: &str| {
        matches!(
            RunStatus::from_str(status),
            Some(RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled)
        )
    };
    let paused = |status: &str| RunStatus::from_str(status) == Some(RunStatus::Paused);
    let before = existing_run.and_then(|data| Run::decode(data).ok());
    let was = |test: &dyn Fn(&str) -> bool| before.as_ref().is_some_and(|run| test(&run.status));
    let event = if ended(&run.status) && !was(&ended) {
        InstanceEvent::RunEnded(RunEnd {
            run_id: record.run_id.clone(),
        })
    } else if paused(&run.status) && !was(&paused) {
        // A resume is the leader's and is folded where it is applied, so only
        // the pause is found here.
        let held = record.event_type == "run.paused"
            && record
                .metadata
                .get(META_INSTANCE_DECISION)
                .is_some_and(|decision| decision == INSTANCE_DECISION_HOLD);
        if held {
            InstanceEvent::RunHeld(PendingResume {
                run_id: record.run_id.clone(),
                request_offset: record.offset.0,
            })
        } else {
            InstanceEvent::RunPaused {
                run_id: record.run_id.clone(),
            }
        }
    } else {
        return Ok(None);
    };
    let key = run
        .metadata
        .get(META_INSTANCE_INBOX_RUN_ID)
        .and_then(|inbox| parse_inbox_run_id(inbox))
        .ok_or_else(|| InstanceError::Invalid("instance run names no inbox".into()))?
        .instance_key();
    Ok(Some(InstanceChange { key, event }))
}

/// Whether the instance's leader must decide what it does next.
///
/// A stopped instance needs one only to hold its active run while that run
/// holds an attempt; a paused run holds none.
pub fn needs_decision(record: &InstanceRecord) -> bool {
    if record.stopped {
        return record.active_run_id.is_some() && !record.active_paused;
    }
    match (&record.active_run_id, &record.resume_requested) {
        (None, _) => !record.waiting_item_ids.is_empty(),
        (Some(active), Some(resume)) => *active == resume.run_id,
        (Some(_), None) => false,
    }
}

fn had_request(before: &Option<PendingResume>, request: &PendingResume) -> bool {
    before.as_ref() == Some(request)
}

/// The store writes for one instance change, given the record as it was.
///
/// The obligation is written when the instance starts needing a decision and
/// deleted when it stops needing one, so it records when the need began and
/// the start delay can be measured from it. A change the fold refuses writes
/// nothing; the caller reports it.
pub fn instance_ops(
    record: &Record,
    partition_id: u32,
    existing: Option<&[u8]>,
    change: &InstanceChange,
) -> Result<Vec<StoreOp>, InstanceError> {
    let before = existing.map(InstanceRecord::decode).transpose()?;
    let needed_before = before.as_ref().is_some_and(needs_decision);
    let before_resume = before
        .as_ref()
        .and_then(|record| record.resume_requested.clone());
    let after = fold(before, &change.event)?;
    let mut ops = vec![StoreOp::PutInstance {
        key: change.key.clone(),
        data: after.encode()?,
    }];
    if let InstanceEvent::ItemAccepted { item, .. } = &change.event {
        // The item waits beside the record until a run takes it, with its
        // content and the metadata the run will carry.
        let content = serde_json::from_slice::<serde_json::Value>(&record.data)
            .ok()
            .and_then(|mut data| data.get_mut("content").map(serde_json::Value::take))
            .unwrap_or(serde_json::Value::Null);
        let stored = StoredInboxItem {
            item: item.clone(),
            content,
            run_metadata: record
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        };
        ops.push(StoreOp::PutInboxItemContent {
            key: change.key.clone(),
            item_id: item.item_id.clone(),
            data: serde_json::to_vec(&stored)
                .map_err(|error| InstanceError::Encoding(error.to_string()))?,
        });
    }
    if let InstanceEvent::ResumeRequested(request) = &change.event {
        if after.resume_requested.as_ref() == Some(request) && !had_request(&before_resume, request)
        {
            ops.push(StoreOp::PutInstanceResumeRequest {
                key: change.key.clone(),
                data: serde_json::to_vec(&StoredResumeRequest::from_record(record))
                    .map_err(|error| InstanceError::Encoding(error.to_string()))?,
            });
        }
    }
    if let InstanceEvent::RunHeld(hold) = &change.event {
        if after.resume_requested.as_ref() == Some(hold) && !had_request(&before_resume, hold) {
            ops.push(StoreOp::PutInstanceResumeRequest {
                key: change.key.clone(),
                data: serde_json::to_vec(&hold_resume_request(record))
                    .map_err(|error| InstanceError::Encoding(error.to_string()))?,
            });
        }
    }
    if before_resume.is_some() && after.resume_requested.is_none() {
        ops.push(StoreOp::DeleteInstanceResumeRequest {
            key: change.key.clone(),
        });
    }
    match (needed_before, needs_decision(&after)) {
        (false, true) => ops.push(StoreOp::PutInstanceObligation {
            partition_id,
            key: change.key.clone(),
            data: serde_json::to_vec(&serde_json::json!({
                "trigger": change.event.name(),
                "offset": record.offset.0,
                "timestamp_ns": record.timestamp_ns,
            }))
            .map_err(|error| InstanceError::Encoding(error.to_string()))?,
        }),
        (true, false) => ops.push(StoreOp::DeleteInstanceObligation {
            partition_id,
            key: change.key.clone(),
        }),
        _ => {}
    }
    Ok(ops)
}

/// The `run.resumed` a lift continues a held run with: no answer, and the
/// run's own metadata from the hold, without the hold's fence and mark. The
/// resumed run is queued for a new attempt on the same deployment, which
/// replays what the stopped attempt recorded.
fn hold_resume_request(hold: &Record) -> StoredResumeRequest {
    let mut metadata = hold.metadata.clone();
    engine_core::lease_fence::strip_stale_resume_metadata(&mut metadata);
    engine_core::instance::strip_instance_decision(&mut metadata);
    metadata.insert(
        engine_core::instance::META_INSTANCE_RESUME_LIFT.to_string(),
        "true".to_string(),
    );
    let mut request = StoredResumeRequest::from_record(&Record {
        data: Default::default(),
        metadata,
        ..hold.clone()
    });
    request.request_offset = hold.offset.0;
    request
}

/// The obligation hook for one journal record: load the instance it changes,
/// fold the change, and return the writes that commit with the record's other
/// projections. A change the fold refuses is logged and writes nothing, the
/// same on every replica, so it can never stall the partition.
pub async fn project_record<R: ProjectionStateReader + ?Sized>(
    store: &R,
    record: &Record,
    partition_id: u32,
    existing_run: Option<&[u8]>,
    run_result: &ProjectionResult,
) -> EngineResult<Vec<StoreOp>> {
    let change = match instance_change(record, existing_run, run_result) {
        Ok(Some(change)) => change,
        Ok(None) => return Ok(Vec::new()),
        Err(error) => {
            crate::metrics::record_instance_change_refused(partition_id, "unreadable");
            warn!(
                run_id = %record.run_id,
                event_type = %record.event_type,
                error = %error,
                "instance record refused"
            );
            return Ok(Vec::new());
        }
    };
    let existing = match store.load_projection_instance(&change.key).await {
        Ok(existing) => existing,
        // A store without instances (the open-source runtime's) refuses the
        // read. The record changes nothing there, and must not stall the
        // partition.
        Err(EngineError::InvalidArgument(error)) => {
            crate::metrics::record_instance_change_refused(partition_id, "unsupported");
            warn!(
                run_id = %record.run_id,
                event_type = %record.event_type,
                error = %error,
                "instance record refused: this store keeps no agent instances"
            );
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    match instance_ops(record, partition_id, existing.as_deref(), &change) {
        Ok(ops) => Ok(ops),
        Err(error) => {
            crate::metrics::record_instance_change_refused(partition_id, change.event.name());
            warn!(
                run_id = %record.run_id,
                event_type = %record.event_type,
                instance_agent = %change.key.agent_name,
                error = %error,
                "instance change refused"
            );
            Ok(Vec::new())
        }
    }
}

/// A supervisor's start, read back from its `run.queued`.
pub fn run_start(record: &Record) -> Option<Result<(InstanceKey, RunStart), InstanceError>> {
    if record.event_type != "run.queued"
        || record
            .metadata
            .get(META_INSTANCE_DECISION)
            .map(String::as_str)
            != Some(INSTANCE_DECISION_START)
    {
        return None;
    }
    Some((|| {
        let invalid = |what: &str| InstanceError::Invalid(format!("instance start {what}"));
        if !is_instance_run_id(&record.run_id) {
            return Err(invalid("is not for an instance run id"));
        }
        let key = record
            .metadata
            .get(META_INSTANCE_INBOX_RUN_ID)
            .and_then(|inbox| parse_inbox_run_id(inbox))
            .ok_or_else(|| invalid("names no inbox"))?
            .instance_key();
        let sequence = record
            .metadata
            .get(META_INSTANCE_SEQUENCE)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| invalid("has no sequence"))?;
        let item_ids = record
            .metadata
            .get(META_INSTANCE_ITEM_IDS)
            .map(|ids| ids.split(',').map(str::to_string).collect::<Vec<_>>())
            .ok_or_else(|| invalid("takes no items"))?;
        Ok((
            key,
            RunStart {
                run_id: record.run_id.clone(),
                sequence,
                item_ids,
            },
        ))
    })())
}

/// The re-check on apply of a supervisor's start.
#[derive(Debug)]
pub struct StartApplied {
    pub outcome: StartOutcome,
    /// The instance's writes, which commit with the record.
    pub ops: Vec<StoreOp>,
}

impl StartApplied {
    /// Only an accepted start becomes a run that is dispatched.
    pub fn accepted(&self) -> bool {
        self.outcome == StartOutcome::Accepted
    }
}

/// Decide, as the record is applied, whether a supervisor's start takes
/// effect. Every replica decides the same way from the same record and
/// journal. An accepted start takes the instance's turn and its items. A
/// rejected one uses up its sequence, takes nothing, and leaves a fresh
/// obligation when the instance still needs a decision, so the supervisor
/// decides again under a new run id. A stale one changes nothing.
pub fn apply_start(
    record: &Record,
    partition_id: u32,
    existing: Option<&[u8]>,
    key: &InstanceKey,
    start: &RunStart,
) -> Result<StartApplied, InstanceError> {
    validate_start(start)?;
    let before =
        existing
            .map(InstanceRecord::decode)
            .transpose()?
            .ok_or(InstanceError::Missing {
                event: "run started",
            })?;
    let outcome = check_start(&before, start);
    if matches!(outcome, StartOutcome::Stale { .. }) {
        return Ok(StartApplied {
            outcome,
            ops: Vec::new(),
        });
    }
    let needed_before = needs_decision(&before);
    let after = fold(Some(before), &InstanceEvent::RunStarted(start.clone()))?;
    let mut ops = vec![StoreOp::PutInstance {
        key: key.clone(),
        data: after.encode()?,
    }];
    if outcome == StartOutcome::Accepted {
        for item_id in &start.item_ids {
            ops.push(StoreOp::DeleteInboxItemContent {
                key: key.clone(),
                item_id: item_id.clone(),
            });
        }
    }
    if needs_decision(&after) {
        // Written again even if one exists, so the supervisor wakes for the
        // fresh decision instead of waiting for its sweep.
        ops.push(StoreOp::PutInstanceObligation {
            partition_id,
            key: key.clone(),
            data: serde_json::to_vec(&serde_json::json!({
                "trigger": "start rejected",
                "offset": record.offset.0,
                "timestamp_ns": record.timestamp_ns,
            }))
            .map_err(|error| InstanceError::Encoding(error.to_string()))?,
        });
    } else if needed_before {
        ops.push(StoreOp::DeleteInstanceObligation {
            partition_id,
            key: key.clone(),
        });
    }
    Ok(StartApplied { outcome, ops })
}

/// The leader's resume, read back from its `run.resumed`.
pub fn run_resume(record: &Record) -> Option<Result<(InstanceKey, PendingResume), InstanceError>> {
    if record.event_type != "run.resumed"
        || record
            .metadata
            .get(META_INSTANCE_DECISION)
            .map(String::as_str)
            != Some(INSTANCE_DECISION_RESUME)
    {
        return None;
    }
    Some((|| {
        let invalid = |what: &str| InstanceError::Invalid(format!("instance resume {what}"));
        let key = record
            .metadata
            .get(META_INSTANCE_INBOX_RUN_ID)
            .and_then(|inbox| parse_inbox_run_id(inbox))
            .ok_or_else(|| invalid("names no inbox"))?
            .instance_key();
        let request_offset = record
            .metadata
            .get(META_INSTANCE_RESUME_REQUEST_OFFSET)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| invalid("names no request"))?;
        Ok((
            key,
            PendingResume {
                run_id: record.run_id.clone(),
                request_offset,
            },
        ))
    })())
}

/// Decide, as the record is applied, whether the leader's resume takes
/// effect: the instance isn't stopped, the run is its active run and still
/// paused, and this is the request pending for it. A rejected resume raises
/// the rejected count, so the leader's next attempt gets a fresh idempotency
/// slot, and leaves a fresh obligation while the request still stands.
pub fn apply_resume(
    record: &Record,
    partition_id: u32,
    existing: Option<&[u8]>,
    existing_run: Option<&[u8]>,
    key: &InstanceKey,
    request: &PendingResume,
) -> Result<StartApplied, InstanceError> {
    let before =
        existing
            .map(InstanceRecord::decode)
            .transpose()?
            .ok_or(InstanceError::Missing {
                event: "resume applied",
            })?;
    let paused = existing_run
        .and_then(|data| Run::decode(data).ok())
        .is_some_and(|run| RunStatus::from_str(&run.status) == Some(RunStatus::Paused));
    let outcome = if before.stopped {
        StartOutcome::Rejected(StartRejection::Stopped)
    } else if before.active_run_id.as_deref() != Some(request.run_id.as_str())
        || before.resume_requested.as_ref() != Some(request)
        || !paused
    {
        StartOutcome::Rejected(StartRejection::ResumeNotPending)
    } else {
        StartOutcome::Accepted
    };
    let needed_before = needs_decision(&before);
    let mut ops = Vec::new();
    let after = if outcome == StartOutcome::Accepted {
        ops.push(StoreOp::DeleteInstanceResumeRequest { key: key.clone() });
        fold(Some(before), &InstanceEvent::ResumeApplied(request.clone()))?
    } else {
        let mut after = before;
        after.rejected_decisions += 1;
        after
    };
    ops.insert(
        0,
        StoreOp::PutInstance {
            key: key.clone(),
            data: after.encode()?,
        },
    );
    if needs_decision(&after) {
        ops.push(StoreOp::PutInstanceObligation {
            partition_id,
            key: key.clone(),
            data: serde_json::to_vec(&serde_json::json!({
                "trigger": "resume rejected",
                "offset": record.offset.0,
                "timestamp_ns": record.timestamp_ns,
            }))
            .map_err(|error| InstanceError::Encoding(error.to_string()))?,
        });
    } else if needed_before {
        ops.push(StoreOp::DeleteInstanceObligation {
            partition_id,
            key: key.clone(),
        });
    }
    Ok(StartApplied { outcome, ops })
}

/// The re-check on apply for one journal record: a supervisor's start or
/// resume. `None` for any other record. A decision that can't be read or
/// checked is refused, the same on every replica: it never runs.
pub async fn check_decision_on_apply<R: ProjectionStateReader + ?Sized>(
    store: &R,
    record: &Record,
    partition_id: u32,
    existing_run: Option<&[u8]>,
) -> EngineResult<Option<StartApplied>> {
    let refused = |error: InstanceError| {
        warn!(run_id = %record.run_id, error = %error, "instance decision refused on apply");
        StartApplied {
            outcome: StartOutcome::Rejected(StartRejection::Invalid),
            ops: Vec::new(),
        }
    };
    if let Some(parsed) = run_resume(record) {
        let (key, request) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => return Ok(Some(refused(error))),
        };
        let existing = match store.load_projection_instance(&key).await {
            Ok(existing) => existing,
            Err(EngineError::InvalidArgument(error)) => {
                return Ok(Some(refused(InstanceError::Invalid(error))))
            }
            Err(error) => return Err(error),
        };
        let applied = match apply_resume(
            record,
            partition_id,
            existing.as_deref(),
            existing_run,
            &key,
            &request,
        ) {
            Ok(applied) => applied,
            Err(error) => refused(error),
        };
        record_applied(partition_id, "resume", &applied.outcome);
        return Ok(Some(applied));
    }
    let applied = check_start_on_apply(store, record, partition_id).await?;
    if let Some(applied) = &applied {
        record_applied(partition_id, "start", &applied.outcome);
    }
    Ok(applied)
}

fn record_applied(partition_id: u32, kind: &'static str, outcome: &StartOutcome) {
    let outcome = match outcome {
        StartOutcome::Accepted => "accepted",
        StartOutcome::Rejected(StartRejection::Invalid) => "invalid",
        StartOutcome::Rejected(_) => "rejected",
        StartOutcome::Stale { .. } => "stale",
    };
    crate::metrics::record_instance_decision_applied(partition_id, kind, outcome);
}

/// The re-check for one journal record, or `None` when the record isn't a
/// supervisor's start. A start that can't be read or checked is refused, the
/// same on every replica: it is never dispatched.
pub async fn check_start_on_apply<R: ProjectionStateReader + ?Sized>(
    store: &R,
    record: &Record,
    partition_id: u32,
) -> EngineResult<Option<StartApplied>> {
    let Some(parsed) = run_start(record) else {
        return Ok(None);
    };
    let refused = |error: InstanceError| {
        warn!(run_id = %record.run_id, error = %error, "instance start refused on apply");
        StartApplied {
            outcome: StartOutcome::Rejected(StartRejection::Invalid),
            ops: Vec::new(),
        }
    };
    let (key, start) = match parsed {
        Ok(parsed) => parsed,
        Err(error) => return Ok(Some(refused(error))),
    };
    let existing = match store.load_projection_instance(&key).await {
        Ok(existing) => existing,
        // A store without instances refuses the read: the start is refused,
        // never dispatched, and the partition goes on.
        Err(EngineError::InvalidArgument(error)) => {
            return Ok(Some(refused(InstanceError::Invalid(error))))
        }
        Err(error) => return Err(error),
    };
    Ok(Some(
        match apply_start(record, partition_id, existing.as_deref(), &key, &start) {
            Ok(applied) => applied,
            Err(error) => refused(error),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::instance::{InstanceStatus, RunEnd, MAX_INSTANCE_RECORD_BYTES};

    fn key() -> InstanceKey {
        key_with_inbox("user-priya")
    }

    fn key_with_inbox(inbox_key: &str) -> InstanceKey {
        InstanceKey {
            project_id: "project-1".into(),
            environment_id: "production".into(),
            agent_name: "support".into(),
            inbox_key: inbox_key.into(),
        }
    }

    /// The item id for item number `n`: a UUID, as the gateway writes.
    fn id(n: u64) -> String {
        format!("{n:08x}-0000-4000-8000-{n:012x}")
    }

    fn created() -> InstanceEvent {
        InstanceEvent::Created(InstanceCreated {
            key: key(),
            generation: 1,
        })
    }

    fn inbox_item(key: &InstanceKey, n: u64, create_if_absent: bool) -> InstanceEvent {
        InstanceEvent::ItemAccepted {
            key: key.clone(),
            item: InboxItem {
                item_id: id(n),
                source_id: format!("source-{n}"),
                inbox_key: key.inbox_key.clone(),
                generation: 1,
                conversation_id: "default".into(),
                channel: "api".into(),
                create_if_absent,
            },
        }
    }

    fn item(n: u64) -> InstanceEvent {
        inbox_item(&key(), n, false)
    }

    fn trusted_item(n: u64) -> InstanceEvent {
        inbox_item(&key(), n, true)
    }

    fn start(run_id: &str, sequence: u64, items: &[u64]) -> InstanceEvent {
        InstanceEvent::RunStarted(RunStart {
            run_id: run_id.into(),
            sequence,
            item_ids: items.iter().map(|n| id(*n)).collect(),
        })
    }

    fn end(run_id: &str) -> InstanceEvent {
        InstanceEvent::RunEnded(RunEnd {
            run_id: run_id.into(),
        })
    }

    fn fold_all(events: &[InstanceEvent]) -> InstanceRecord {
        events
            .iter()
            .fold(None, |record, event| Some(fold(record, event).unwrap()))
            .unwrap()
    }

    fn modify(event: InstanceEvent, change: impl FnOnce(&mut InboxItem)) -> InstanceEvent {
        let InstanceEvent::ItemAccepted { key, mut item } = event else {
            unreachable!()
        };
        change(&mut item);
        InstanceEvent::ItemAccepted { key, item }
    }

    #[test]
    fn creation_makes_an_idle_record() {
        let record = fold(None, &created()).unwrap();
        assert_eq!(record, InstanceRecord::new(key(), 1));
        assert_eq!(record.status(), InstanceStatus::Idle);
    }

    #[test]
    fn creation_is_refused_for_an_existing_instance() {
        let record = fold_all(&[created()]);
        assert_eq!(
            fold(Some(record), &created()),
            Err(InstanceError::AlreadyExists { generation: 1 })
        );
    }

    #[test]
    fn creation_is_refused_with_a_bad_identity() {
        let mut bad_key = key();
        bad_key.agent_name = String::new();
        for event in [
            InstanceEvent::Created(InstanceCreated {
                key: bad_key,
                generation: 1,
            }),
            InstanceEvent::Created(InstanceCreated {
                key: key(),
                generation: 0,
            }),
        ] {
            assert!(matches!(fold(None, &event), Err(InstanceError::Invalid(_))));
        }
    }

    #[test]
    fn a_trusted_item_creates_the_instance_and_waits_in_it() {
        let record = fold(None, &trusted_item(1)).unwrap();
        let mut expected = InstanceRecord::new(key(), 1);
        expected.waiting_item_ids = vec![id(1)];
        assert_eq!(record, expected);

        // Once the instance exists, the flag changes nothing.
        let record = fold(Some(record), &trusted_item(2)).unwrap();
        assert_eq!(record.waiting_item_ids, vec![id(1), id(2)]);
        assert_eq!(record.generation, 1);
    }

    #[test]
    fn a_trusted_item_takes_its_generation_from_the_item() {
        let event = modify(trusted_item(1), |item| item.generation = 3);
        assert_eq!(fold(None, &event).unwrap().generation, 3);
    }

    #[test]
    fn records_that_cant_create_are_refused_before_creation() {
        for event in [
            item(1),
            start("run-0", 0, &[1]),
            end("run-0"),
            InstanceEvent::Stopped,
            InstanceEvent::StopLifted,
        ] {
            assert!(matches!(
                fold(None, &event),
                Err(InstanceError::Missing { .. })
            ));
        }
    }

    #[test]
    fn items_wait_in_arrival_order_and_a_repeat_is_absorbed() {
        let record = fold_all(&[created(), item(1), item(2), item(1), item(3)]);
        assert_eq!(record.waiting_item_ids, vec![id(1), id(2), id(3)]);
    }

    #[test]
    fn a_bad_item_is_refused() {
        let record = fold_all(&[created()]);
        let cases = [
            modify(item(1), |item| item.item_id = "item-1".into()),
            modify(item(1), |item| item.item_id = id(0xabc).to_uppercase()),
            modify(item(1), |item| item.channel = String::new()),
            // The payload's inbox key disagrees with the instance's.
            modify(item(1), |item| item.inbox_key = "user-sam".into()),
            // The record arrived on another instance's key.
            inbox_item(&key_with_inbox("user-sam"), 1, false),
            // Meant for another generation of this instance.
            modify(item(1), |item| item.generation = 2),
        ];
        for event in cases {
            assert!(
                matches!(
                    fold(Some(record.clone()), &event),
                    Err(InstanceError::Invalid(_))
                ),
                "{event:?}"
            );
        }
        // Creating from an item validates the same way.
        let bad = modify(trusted_item(1), |item| item.item_id = "item-1".into());
        assert!(matches!(fold(None, &bad), Err(InstanceError::Invalid(_))));
    }

    #[test]
    fn an_accepted_start_takes_its_items_and_the_next_sequence() {
        let record = fold_all(&[
            created(),
            item(1),
            item(2),
            item(3),
            start("run-0", 0, &[2]),
        ]);
        assert_eq!(record.active_run_id.as_deref(), Some("run-0"));
        assert_eq!(record.waiting_item_ids, vec![id(1), id(3)]);
        assert_eq!(record.next_run_sequence, 1);
        assert_eq!(record.rejected_decisions, 0);
        assert_eq!(record.status(), InstanceStatus::Running);
    }

    #[test]
    fn a_rejected_start_uses_up_its_sequence_and_counts_a_rejection() {
        let waiting = fold_all(&[created(), item(1), item(2)]);
        let running = fold(Some(waiting.clone()), &start("run-0", 0, &[1])).unwrap();
        let stopped = fold(Some(waiting.clone()), &InstanceEvent::Stopped).unwrap();
        let cases = [
            (
                running,
                start("run-1", 1, &[2]),
                StartRejection::ActiveRun {
                    run_id: "run-0".into(),
                },
            ),
            (stopped, start("run-0", 0, &[1]), StartRejection::Stopped),
            (
                waiting,
                start("run-0", 0, &[1, 9]),
                StartRejection::ItemNotWaiting { item_id: id(9) },
            ),
        ];
        for (before, event, rejection) in cases {
            let InstanceEvent::RunStarted(run_start) = &event else {
                unreachable!()
            };
            assert_eq!(
                check_start(&before, run_start),
                StartOutcome::Rejected(rejection)
            );
            let after = fold(Some(before.clone()), &event).unwrap();
            let mut expected = before;
            expected.next_run_sequence += 1;
            expected.rejected_decisions += 1;
            assert_eq!(after, expected);
        }
    }

    #[test]
    fn a_start_for_another_sequence_changes_nothing() {
        // Sequence 0 was accepted, sequence 1 rejected; both are used up.
        let record = fold_all(&[
            created(),
            item(1),
            item(2),
            start("run-0", 0, &[1]),
            start("run-1", 1, &[2]),
        ]);
        assert_eq!(record.next_run_sequence, 2);
        assert_eq!(record.rejected_decisions, 1);
        for event in [
            // A duplicate of the accepted start.
            start("run-0", 0, &[1]),
            // A second decision for the rejected sequence.
            start("run-1b", 1, &[2]),
            // A sequence the supervisor couldn't have chosen yet.
            start("run-5", 5, &[2]),
        ] {
            let InstanceEvent::RunStarted(run_start) = &event else {
                unreachable!()
            };
            assert!(matches!(
                check_start(&record, run_start),
                StartOutcome::Stale { expected: 2, .. }
            ));
            assert_eq!(fold(Some(record.clone()), &event).unwrap(), record);
        }
    }

    #[test]
    fn a_stale_start_is_a_no_op_even_while_stopped() {
        let record = fold_all(&[
            created(),
            item(1),
            start("run-0", 0, &[1]),
            InstanceEvent::Stopped,
        ]);
        assert_eq!(
            fold(Some(record.clone()), &start("run-0", 0, &[1])).unwrap(),
            record
        );
    }

    #[test]
    fn a_malformed_start_is_refused_not_rejected() {
        let record = fold_all(&[created(), item(1)]);
        let not_a_uuid = InstanceEvent::RunStarted(RunStart {
            run_id: "run-0".into(),
            sequence: 0,
            item_ids: vec!["item-1".into()],
        });
        for event in [
            start("run-0", 0, &[]),
            start("run-0", 0, &[1, 1]),
            start("", 0, &[1]),
            not_a_uuid,
        ] {
            assert!(matches!(
                fold(Some(record.clone()), &event),
                Err(InstanceError::Invalid(_))
            ));
        }
    }

    #[test]
    fn only_the_active_runs_end_frees_the_turn() {
        let running = fold_all(&[created(), item(1), item(2), start("run-0", 0, &[1])]);
        let other = fold(Some(running.clone()), &end("run-9")).unwrap();
        assert_eq!(other, running);
        let idle = fold(Some(running), &end("run-0")).unwrap();
        assert_eq!(idle.active_run_id, None);
        let next = fold(Some(idle), &start("run-1", 1, &[2])).unwrap();
        assert_eq!(next.active_run_id.as_deref(), Some("run-1"));
        assert!(next.waiting_item_ids.is_empty());
    }

    #[test]
    fn a_stop_holds_the_active_run_and_a_lift_releases_it() {
        let running = fold_all(&[created(), item(1), start("run-0", 0, &[1])]);
        let stopped = fold(Some(running), &InstanceEvent::Stopped).unwrap();
        assert_eq!(stopped.status(), InstanceStatus::Stopped);
        assert_eq!(stopped.active_run_id.as_deref(), Some("run-0"));
        let stopped_again = fold(Some(stopped.clone()), &InstanceEvent::Stopped).unwrap();
        assert_eq!(stopped_again, stopped);
        let lifted = fold(Some(stopped), &InstanceEvent::StopLifted).unwrap();
        assert_eq!(lifted.status(), InstanceStatus::Running);
        let lifted_again = fold(Some(lifted.clone()), &InstanceEvent::StopLifted).unwrap();
        assert_eq!(lifted_again, lifted);
    }

    fn held(run_id: &str, offset: u64) -> PendingResume {
        PendingResume {
            run_id: run_id.into(),
            request_offset: offset,
        }
    }

    #[test]
    fn a_stop_needs_a_hold_only_while_the_run_holds_an_attempt() {
        let running = fold_all(&[created(), item(1), start("run-0", 0, &[1])]);
        assert!(!needs_decision(&running));
        let stopped = fold(Some(running), &InstanceEvent::Stopped).unwrap();
        assert!(needs_decision(&stopped), "the running attempt must be held");

        // The hold pauses the run and is its resume request.
        let hold = held("run-0", 7);
        let holding = fold(Some(stopped), &InstanceEvent::RunHeld(hold.clone())).unwrap();
        assert!(holding.active_paused);
        assert_eq!(holding.resume_requested, Some(hold.clone()));
        assert!(!needs_decision(&holding), "nothing to do until the lift");

        // The lift asks the leader to continue the run.
        let lifted = fold(Some(holding), &InstanceEvent::StopLifted).unwrap();
        assert!(needs_decision(&lifted));
        let resumed = fold(Some(lifted), &InstanceEvent::ResumeApplied(hold)).unwrap();
        assert!(!resumed.active_paused);
        assert_eq!(resumed.resume_requested, None);
        assert!(!needs_decision(&resumed));
        assert_eq!(resumed.active_run_id.as_deref(), Some("run-0"));
    }

    #[test]
    fn a_stop_of_a_paused_run_holds_nothing_and_its_answer_waits_for_the_lift() {
        let paused = fold_all(&[
            created(),
            item(1),
            start("run-0", 0, &[1]),
            InstanceEvent::RunPaused {
                run_id: "run-0".into(),
            },
        ]);
        assert!(paused.active_paused);
        let stopped = fold(Some(paused), &InstanceEvent::Stopped).unwrap();
        assert!(!needs_decision(&stopped));
        let answered = fold(
            Some(stopped),
            &InstanceEvent::ResumeRequested(held("run-0", 9)),
        )
        .unwrap();
        assert!(!needs_decision(&answered), "no resume while stopped");
        let lifted = fold(Some(answered), &InstanceEvent::StopLifted).unwrap();
        assert!(needs_decision(&lifted));
    }

    #[test]
    fn a_pause_or_hold_of_another_run_changes_nothing() {
        let running = fold_all(&[created(), item(1), start("run-0", 0, &[1])]);
        for event in [
            InstanceEvent::RunPaused {
                run_id: "run-9".into(),
            },
            InstanceEvent::RunHeld(held("run-9", 3)),
        ] {
            assert_eq!(fold(Some(running.clone()), &event).unwrap(), running);
        }
    }

    #[test]
    fn an_ending_clears_a_held_run() {
        let held_run = fold_all(&[
            created(),
            item(1),
            start("run-0", 0, &[1]),
            InstanceEvent::Stopped,
            InstanceEvent::RunHeld(held("run-0", 7)),
            end("run-0"),
        ]);
        assert!(!held_run.active_paused);
        assert_eq!(held_run.resume_requested, None);
        assert_eq!(held_run.active_run_id, None);
    }

    // A start rejected during a stop must not strand its item: after the
    // lift, the supervisor's next decision, at the next sequence, takes it.
    #[test]
    fn a_rejected_start_leaves_its_items_for_the_next_decision() {
        let record = fold_all(&[
            created(),
            item(1),
            InstanceEvent::Stopped,
            start("run-0", 0, &[1]),
            InstanceEvent::StopLifted,
        ]);
        assert_eq!(record.rejected_decisions, 1);
        assert_eq!(record.next_run_sequence, 1);
        assert_eq!(record.waiting_item_ids, vec![id(1)]);
        let record = fold(Some(record), &start("run-1", 1, &[1])).unwrap();
        assert_eq!(record.active_run_id.as_deref(), Some("run-1"));
        assert_eq!(record.next_run_sequence, 2);
        assert!(record.waiting_item_ids.is_empty());
    }

    #[test]
    fn replicas_folding_the_same_journal_store_the_same_bytes() {
        let journal = [
            trusted_item(1),
            item(2),
            start("run-0", 0, &[1]),
            start("run-x", 1, &[2]),
            start("run-0", 0, &[1]),
            end("run-0"),
            InstanceEvent::Stopped,
            item(3),
            InstanceEvent::StopLifted,
            start("run-2", 2, &[2, 3]),
        ];
        let first = fold_all(&journal).encode().unwrap();
        let second = fold_all(&journal).encode().unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn a_full_inbox_of_1000_items_fits_the_limit() {
        let mut record = fold_all(&[created()]);
        for n in 0..1_000 {
            record = fold(Some(record), &item(n)).unwrap();
        }
        assert!(record.encode().unwrap().len() < MAX_INSTANCE_RECORD_BYTES);
    }

    #[test]
    fn the_size_limit_allows_exactly_64_kib_and_refuses_one_byte_more() {
        // Item ids are all 36 bytes, so each waiting item after the first adds
        // the same 39 bytes (`,"<id>"`). Fill the inbox as far as it goes with
        // a one-byte inbox key, then lengthen the key by the bytes left over,
        // so that the last item lands the record exactly on the limit.
        let filled = |inbox_key: &str, count: u64| {
            let mut record = InstanceRecord::new(key_with_inbox(inbox_key), 1);
            record.waiting_item_ids = (0..count).map(id).collect();
            record
        };
        let size = |record: &InstanceRecord| serde_json::to_vec(record).unwrap().len();
        let first = size(&filled("k", 1));
        let per_item = size(&filled("k", 2)) - first;
        let count = 1 + (MAX_INSTANCE_RECORD_BYTES - first) / per_item;
        let slack = MAX_INSTANCE_RECORD_BYTES - size(&filled("k", count as u64));
        assert!(slack < per_item);

        let last_item =
            |inbox_key: &str| inbox_item(&key_with_inbox(inbox_key), count as u64 - 1, false);
        let at_limit_key = "k".repeat(1 + slack);
        let at_limit = fold(
            Some(filled(&at_limit_key, count as u64 - 1)),
            &last_item(&at_limit_key),
        )
        .unwrap();
        assert_eq!(at_limit.encode().unwrap().len(), MAX_INSTANCE_RECORD_BYTES);

        let over_key = "k".repeat(2 + slack);
        assert_eq!(
            fold(
                Some(filled(&over_key, count as u64 - 1)),
                &last_item(&over_key)
            ),
            Err(InstanceError::RecordTooLarge {
                limit: MAX_INSTANCE_RECORD_BYTES,
                actual: MAX_INSTANCE_RECORD_BYTES + 1,
            })
        );
        let message = InstanceError::RecordTooLarge {
            limit: MAX_INSTANCE_RECORD_BYTES,
            actual: MAX_INSTANCE_RECORD_BYTES + 1,
        }
        .to_string();
        assert_eq!(
            message,
            "instance record is 65537 bytes, over the 65536-byte limit"
        );
    }
}

#[cfg(test)]
mod obligation_hook_tests {
    use super::*;
    use crate::projections::runs;
    use engine_core::instance_placement::{inbox_run_id, instance_run_id, InstanceIdentity};
    use engine_core::types::Offset;
    use std::collections::HashMap;

    fn identity() -> InstanceIdentity {
        InstanceIdentity::new("project-1", "env-1", "support", "u_123", 1).unwrap()
    }

    fn inbox() -> String {
        inbox_run_id(&identity(), 0, 1).unwrap()
    }

    fn key() -> InstanceKey {
        identity().instance_key()
    }

    fn journal(run_id: &str, event_type: &str, data: serde_json::Value, offset: u64) -> Record {
        Record {
            offset: Offset(offset),
            project_id: "project-1".into(),
            run_id: run_id.into(),
            event_type: event_type.into(),
            data: serde_json::to_vec(&data).unwrap().into(),
            timestamp_ns: offset as i64 * 1_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".into(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn item_id(n: u64) -> String {
        engine_core::instance_placement::inbox_item_id(&inbox(), &format!("source-{n}"))
    }

    fn item_record(n: u64) -> Record {
        journal(
            &inbox(),
            INBOX_ITEM_EVENT,
            serde_json::json!({
                "item_id": item_id(n),
                "source_id": format!("source-{n}"),
                "inbox_key": "u_123",
                "generation": 1,
                "conversation_id": "api",
                "channel": "api",
                "create_if_absent": true,
                "content": {"message": format!("message {n}")},
            }),
            n,
        )
    }

    /// Apply journal records through the hook in order, as the processor
    /// would, returning the ops of the last one and the record after it.
    fn apply(records: &[(Record, Option<InstanceEvent>)]) -> (Vec<StoreOp>, Option<Vec<u8>>) {
        let mut stored: Option<Vec<u8>> = None;
        let mut last = Vec::new();
        for (record, event) in records {
            let event = match event {
                Some(event) => event.clone(),
                None => InstanceEvent::from_journal(&key(), &record.event_type, &record.data)
                    .unwrap()
                    .unwrap(),
            };
            let change = InstanceChange { key: key(), event };
            last = instance_ops(record, 0, stored.as_deref(), &change).unwrap();
            for op in &last {
                if let StoreOp::PutInstance { data, .. } = op {
                    stored = Some(data.clone());
                }
            }
        }
        (last, stored)
    }

    fn obligation(ops: &[StoreOp]) -> Option<bool> {
        ops.iter().find_map(|op| match op {
            StoreOp::PutInstanceObligation { .. } => Some(true),
            StoreOp::DeleteInstanceObligation { .. } => Some(false),
            _ => None,
        })
    }

    fn start(run_id: &str, sequence: u64, items: &[u64]) -> Option<InstanceEvent> {
        Some(InstanceEvent::RunStarted(RunStart {
            run_id: run_id.into(),
            sequence,
            item_ids: items.iter().map(|n| item_id(*n)).collect(),
        }))
    }

    fn ended(run_id: &str) -> Option<InstanceEvent> {
        Some(InstanceEvent::RunEnded(RunEnd {
            run_id: run_id.into(),
        }))
    }

    fn control(event_type: &str, offset: u64) -> (Record, Option<InstanceEvent>) {
        (
            journal(&inbox(), event_type, serde_json::json!({}), offset),
            None,
        )
    }

    #[test]
    fn a_first_item_creates_the_instance_keeps_its_content_and_needs_a_decision() {
        let (ops, stored) = apply(&[(item_record(1), None)]);
        let record = InstanceRecord::decode(&stored.unwrap()).unwrap();
        assert_eq!(record.waiting_item_ids, vec![item_id(1)]);
        let stored = ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutInboxItemContent {
                    item_id: id, data, ..
                } => Some((
                    id.clone(),
                    serde_json::from_slice::<StoredInboxItem>(data).unwrap(),
                )),
                _ => None,
            })
            .unwrap();
        assert_eq!(stored.0, item_id(1));
        assert_eq!(
            stored.1.content,
            serde_json::json!({"message": "message 1"})
        );
        assert_eq!(stored.1.item.item_id, item_id(1));
        let Some(StoreOp::PutInstanceObligation { data, .. }) = ops
            .iter()
            .find(|op| matches!(op, StoreOp::PutInstanceObligation { .. }))
        else {
            panic!("no obligation: {ops:?}");
        };
        let data: serde_json::Value = serde_json::from_slice(data).unwrap();
        assert_eq!(data["trigger"], INBOX_ITEM_EVENT);
        assert_eq!(data["offset"], 1);
    }

    #[test]
    fn the_obligation_is_written_once_when_the_need_begins() {
        let (ops, _) = apply(&[(item_record(1), None), (item_record(2), None)]);
        assert_eq!(
            obligation(&ops),
            None,
            "a second item keeps the first's obligation"
        );
    }

    #[test]
    fn stop_clears_the_obligation_and_lift_restores_it() {
        let (ops, _) = apply(&[(item_record(1), None), control(INSTANCE_STOPPED_EVENT, 2)]);
        assert_eq!(obligation(&ops), Some(false));
        let (ops, _) = apply(&[
            (item_record(1), None),
            control(INSTANCE_STOPPED_EVENT, 2),
            control(INSTANCE_STOP_LIFTED_EVENT, 3),
        ]);
        assert_eq!(obligation(&ops), Some(true));
    }

    #[test]
    fn a_run_ending_with_items_waiting_needs_the_next_decision() {
        let run = "61676e35-0100-8000-8000-000000000001";
        let started = (
            journal(run, "run.queued", serde_json::json!({}), 2),
            start(run, 0, &[1]),
        );
        let (ops, _) = apply(&[(item_record(1), None), started.clone()]);
        assert_eq!(obligation(&ops), Some(false), "the start takes the turn");

        // An item arriving mid-run waits; the run ending makes the next decision due.
        let (ops, _) = apply(&[
            (item_record(1), None),
            started.clone(),
            (item_record(2), None),
        ]);
        assert_eq!(obligation(&ops), None);
        let ending = (
            journal(run, "run.completed", serde_json::json!({}), 4),
            ended(run),
        );
        let (ops, stored) = apply(&[
            (item_record(1), None),
            started.clone(),
            (item_record(2), None),
            ending.clone(),
        ]);
        assert_eq!(obligation(&ops), Some(true));
        let record = InstanceRecord::decode(&stored.unwrap()).unwrap();
        assert_eq!(record.active_run_id, None);

        // With nothing waiting, an ending needs no decision.
        let (ops, _) = apply(&[(item_record(1), None), started, ending]);
        assert_eq!(obligation(&ops), None);
    }

    #[test]
    fn a_change_the_fold_refuses_writes_nothing() {
        let mut untrusted = item_record(1);
        let mut data: serde_json::Value = serde_json::from_slice(&untrusted.data).unwrap();
        data["create_if_absent"] = serde_json::json!(false);
        untrusted.data = serde_json::to_vec(&data).unwrap().into();
        let event = InstanceEvent::from_journal(&key(), INBOX_ITEM_EVENT, &untrusted.data)
            .unwrap()
            .unwrap();
        let change = InstanceChange { key: key(), event };
        assert!(instance_ops(&untrusted, 0, None, &change).is_err());
    }

    #[test]
    fn instance_records_resolve_to_their_instance_and_others_to_none() {
        let empty = ProjectionResult { ops: vec![] };
        let change = instance_change(&item_record(1), None, &empty)
            .unwrap()
            .unwrap();
        assert_eq!(change.key, key());
        assert!(matches!(change.event, InstanceEvent::ItemAccepted { .. }));

        let plain = journal("run-1", "run.completed", serde_json::json!({}), 1);
        assert_eq!(instance_change(&plain, None, &empty).unwrap(), None);

        let mut stray = item_record(1);
        stray.run_id = "run-1".into();
        assert!(instance_change(&stray, None, &empty).is_err());
    }

    // --- Every way a run ends ---------------------------------------------
    //
    // About 14 sites write a record that ends a run, and all of them reach the
    // instance through the one run projection. The hook reads the run's
    // projected status, so what matters is the record each site writes and the
    // status the run was in. Each row below is one site's record. A new
    // run-ending record must be added here.

    fn instance_run() -> String {
        instance_run_id(&identity(), 0, 0, 1).unwrap()
    }

    fn run_record(event_type: &str, metadata: &[(&str, &str)], offset: u64) -> Record {
        let mut record = journal(&instance_run(), event_type, serde_json::json!({}), offset);
        record.metadata = metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        record
    }

    /// The run as projected after these records, in order.
    fn project_run(records: &[Record]) -> Option<Vec<u8>> {
        records.iter().fold(None, |run, record| {
            runs::project(record, run.as_deref())
                .run_data()
                .map(<[u8]>::to_vec)
                .or(run)
        })
    }

    fn queued() -> Record {
        let inbox = inbox();
        run_record(
            "run.queued",
            &[
                (META_INSTANCE_INBOX_RUN_ID, inbox.as_str()),
                ("component_type", "agent"),
                ("component_name", "support"),
                ("workspace_id", "workspace-1"),
                ("deployment_id", "dep-1"),
            ],
            10,
        )
    }

    fn prior(status: &str) -> Vec<Record> {
        let mut records = vec![queued()];
        match status {
            "queued" => {}
            "started" => records.push(run_record("run.started", &[], 11)),
            "paused" => {
                records.push(run_record("run.started", &[], 11));
                records.push(run_record("run.paused", &[], 12));
            }
            "retry_delayed" => {
                records.push(run_record("run.started", &[], 11));
                records.push(run_record(
                    "run.failed",
                    &[("attempt", "0"), ("max_attempts", "3")],
                    12,
                ));
            }
            other => panic!("no prior status {other}"),
        }
        records
    }

    /// The run as it stands before the ending record.
    fn prior_run(status: &str) -> Option<Vec<u8>> {
        if status != "terminalizing" {
            return project_run(&prior(status));
        }
        // A supervisor-authored ending consumes the reservation its
        // `run.terminalizing` made, with the same outcome.
        let mut run = Run::decode(project_run(&prior("started")).unwrap().as_slice()).unwrap();
        run.status = "terminalizing".into();
        for (key, value) in RESERVED {
            run.metadata.insert(key.to_string(), value.to_string());
        }
        Some(run.encode_to_vec())
    }

    const RESERVED: &[(&str, &str)] = &[
        ("terminal_reservation_id", "res-1"),
        ("terminal_outcome", "timeout"),
    ];

    #[test]
    fn every_run_ending_record_ends_the_instances_run() {
        let exhausted: &[(&str, &str)] = &[("attempt", "2"), ("max_attempts", "3")];
        let not_retryable: &[(&str, &str)] = &[
            ("attempt", "0"),
            ("max_attempts", "3"),
            ("workerless_retryable", "false"),
        ];
        let retrying: &[(&str, &str)] = &[("attempt", "0"), ("max_attempts", "3")];
        let cancel_op: &[(&str, &str)] = &[("cancel_operation_id", "op-1")];
        let reserved = RESERVED;

        // (site, prior status, ending event, its metadata, ends the run)
        // (site, prior status, event type, metadata, whether it ends the run)
        type Row<'a> = (&'a str, &'a str, &'a str, &'a [(&'a str, &'a str)], bool);
        let table: &[Row] = &[
            (
                "gateway cancel",
                "started",
                "run.cancelled",
                cancel_op,
                true,
            ),
            (
                "gateway cancel before start",
                "queued",
                "run.cancelled",
                cancel_op,
                true,
            ),
            (
                "gateway cancel while paused",
                "paused",
                "run.cancelled",
                cancel_op,
                true,
            ),
            (
                "gateway batch cancel",
                "started",
                "run.cancelled",
                &[],
                true,
            ),
            (
                "gateway eval child cancel",
                "queued",
                "run.cancelled",
                &[],
                true,
            ),
            (
                "service worker completion",
                "started",
                "run.completed",
                &[],
                true,
            ),
            (
                "service worker failure, attempts used up",
                "started",
                "run.failed",
                exhausted,
                true,
            ),
            (
                "service guardrail failure",
                "started",
                "run.failed",
                not_retryable,
                true,
            ),
            (
                "coordinator push-worker completion",
                "terminalizing",
                "run.completed",
                reserved,
                true,
            ),
            (
                "coordinator failed dispatch",
                "queued",
                "run.failed",
                &[],
                true,
            ),
            (
                "coordinator dead-letter on expiry",
                "started",
                "run.failed",
                exhausted,
                true,
            ),
            (
                "processor orchestrator failure",
                "started",
                "run.failed",
                &[],
                true,
            ),
            (
                "processor retired deployment",
                "queued",
                "run.cancelled",
                &[],
                true,
            ),
            (
                "processor debounce cancel",
                "queued",
                "run.cancelled",
                &[],
                true,
            ),
            (
                "processor terminal supervisor",
                "terminalizing",
                "run.failed",
                reserved,
                true,
            ),
            (
                "terminal supervisor during a retry delay",
                "retry_delayed",
                "run.cancelled",
                &[],
                true,
            ),
            (
                "runtime server workerless completion",
                "started",
                "run.completed",
                &[],
                true,
            ),
            (
                "open-source server cancel",
                "started",
                "run.cancelled",
                cancel_op,
                true,
            ),
            (
                "open-source lease sweeper",
                "started",
                "run.failed",
                exhausted,
                true,
            ),
            // Not endings: the run keeps the instance's turn.
            (
                "failure with attempts left",
                "started",
                "run.failed",
                retrying,
                false,
            ),
            ("pause", "started", "run.paused", &[], false),
            ("terminalizing", "started", "run.terminalizing", &[], false),
            (
                "lease expiry, redelivered",
                "started",
                "run.lease_expired",
                &[],
                false,
            ),
        ];
        for (site, status, event_type, metadata, ends) in table {
            let before = prior_run(status);
            let ending = run_record(event_type, metadata, 20);
            let result = runs::project(&ending, before.as_deref());
            let change = instance_change(&ending, before.as_deref(), &result)
                .unwrap_or_else(|error| panic!("{site}: {error}"))
                // A pause changes the instance too, but doesn't end its run.
                .filter(|change| matches!(change.event, InstanceEvent::RunEnded(_)));
            assert_eq!(
                change.is_some(),
                *ends,
                "{site}: {event_type} from {status} -> {:?}",
                result
                    .run_data()
                    .and_then(|data| Run::decode(data).ok())
                    .map(|run| run.status)
            );
            if let Some(change) = change {
                assert_eq!(change.key, key(), "{site}");
                assert_eq!(
                    change.event,
                    InstanceEvent::RunEnded(RunEnd {
                        run_id: instance_run()
                    }),
                    "{site}"
                );
            }
        }
    }

    /// The open-source stores' state reader: no instances.
    struct NoInstances;

    #[async_trait::async_trait]
    impl ProjectionStateReader for NoInstances {
        async fn load_projection_run(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_activation(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_approval(
            &self,
            _: &str,
            _: &str,
        ) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_signal(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_timer(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_job(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_batch(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_session(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn load_projection_message(&self, _: &str, _: &str) -> EngineResult<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    // On a store without instances, an instance record or a start changes
    // nothing and never stalls the partition.
    #[tokio::test]
    async fn a_store_without_instances_refuses_without_stalling() {
        let item = item_record(1);
        let result = runs::project(&item, None);
        assert_eq!(
            project_record(&NoInstances, &item, 0, None, &result)
                .await
                .unwrap(),
            Vec::new()
        );
        let mut start = run_record("run.queued", &[], 3);
        start.metadata.insert(
            META_INSTANCE_DECISION.to_string(),
            INSTANCE_DECISION_START.to_string(),
        );
        start
            .metadata
            .insert(META_INSTANCE_SEQUENCE.to_string(), "0".to_string());
        start
            .metadata
            .insert(META_INSTANCE_ITEM_IDS.to_string(), item_id(1));
        start
            .metadata
            .insert(META_INSTANCE_INBOX_RUN_ID.to_string(), inbox());
        let applied = check_decision_on_apply(&NoInstances, &start, 0, None)
            .await
            .unwrap()
            .expect("a start is checked");
        assert!(!applied.accepted());
        assert!(applied.ops.is_empty());
    }

    #[test]
    fn a_repeated_ending_ends_the_run_once() {
        let mut records = prior("started");
        records.push(run_record("run.completed", &[], 20));
        let ended_run = project_run(&records);
        let again = run_record("run.completed", &[], 21);
        let result = runs::project(&again, ended_run.as_deref());
        assert_eq!(
            instance_change(&again, ended_run.as_deref(), &result).unwrap(),
            None
        );
    }

    #[test]
    fn a_plain_run_never_touches_an_instance() {
        let mut ending = run_record("run.completed", &[], 20);
        ending.run_id = "0190d4a8-0000-7000-8000-000000000001".into();
        let result = runs::project(&ending, None);
        assert_eq!(instance_change(&ending, None, &result).unwrap(), None);
    }
}
