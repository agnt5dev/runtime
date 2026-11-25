//! The always-on agent instance: its record, and the journal records that
//! change it.
//!
//! An instance is one agent working for one user over many runs. Its record
//! is a projection of the instance's journal records, kept on the instance's
//! partition and changed only by that partition's leader. The fold that
//! builds it lives in the processor's instance projection.
//!
//! The record holds only what the supervisor needs to decide the next run and
//! what the projection needs to check that decision when it is applied: who
//! the instance is, whether it is stopped, its active run, its waiting item
//! ids, its run sequence and its count of rejected decisions. Item content is
//! never in the instance record, which keeps the record under a fixed size.
//! It arrives in the item's journal record, and the projection keeps waiting
//! items' content beside the record in the materialized store (ADR-026), so a
//! long-idle instance can wake after its journal is trimmed without reading
//! object storage.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The largest an encoded instance record may be. The record holds ids and
/// counters only, so the per-instance limits keep it under this; a record
/// that would grow past it is refused rather than written.
pub const MAX_INSTANCE_RECORD_BYTES: usize = 64 * 1024;

/// How many items may wait in one inbox before the gateway refuses more. The
/// record's size limit holds about 1,700 waiting ids; the gap absorbs items
/// admitted by gateways reading a slightly stale record, so an item the
/// gateway acknowledged is not refused by the fold.
pub const MAX_WAITING_ITEMS: usize = 1_000;

/// The longest identity part, run id or other id the record accepts. Long
/// enough for any external user id, short enough to bound the record.
pub const MAX_INSTANCE_ID_BYTES: usize = 256;

/// An instance was created through the instance API.
pub const INSTANCE_CREATED_EVENT: &str = "instance.created";
/// An item was accepted into an instance's inbox.
pub const INBOX_ITEM_EVENT: &str = "inbox.item";
/// Run metadata naming the inbox of the instance that owns the run. The
/// supervisor stamps it on every run it starts, so the run projection can find
/// the instance when the run ends.
pub const META_INSTANCE_INBOX_RUN_ID: &str = "instance_inbox_run_id";
/// An answer, signal or timer asked to resume an instance's paused run. The
/// leader decides the resume; the path that got the answer only records it.
pub const INSTANCE_RESUME_REQUESTED_EVENT: &str = "instance.resume_requested";
/// An operator stopped the instance.
pub const INSTANCE_STOPPED_EVENT: &str = "instance.stopped";
/// An operator lifted the instance's stop.
pub const INSTANCE_STOP_LIFTED_EVENT: &str = "instance.stop_lifted";

/// Why an instance record or an instance journal record was refused.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum InstanceError {
    /// The record would be larger than [`MAX_INSTANCE_RECORD_BYTES`].
    #[error("instance record is {actual} bytes, over the {limit}-byte limit")]
    RecordTooLarge { limit: usize, actual: usize },

    /// A record that can't create an instance arrived for one that has none.
    #[error("no instance record exists to apply {event} to")]
    Missing { event: &'static str },

    /// A creation arrived for an instance that already has a record.
    #[error("instance already exists at generation {generation}")]
    AlreadyExists { generation: u64 },

    /// The journal record is malformed, or names a different instance.
    #[error("invalid instance journal record: {0}")]
    Invalid(String),

    /// The bytes could not be encoded or decoded.
    #[error("instance record encoding: {0}")]
    Encoding(String),
}

/// Which instance a record belongs to: one agent, in one environment of one
/// project, for one inbox key (such as a user id). The generation is kept on
/// the record rather than here, because a deleted and recreated instance
/// lives under the same key.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceKey {
    pub project_id: String,
    pub environment_id: String,
    pub agent_name: String,
    pub inbox_key: String,
}

impl InstanceKey {
    /// Every part is non-empty, bounded, and free of the NUL byte that
    /// separates key parts in the materialized store.
    pub fn validate(&self) -> Result<(), InstanceError> {
        for (name, value) in [
            ("project_id", &self.project_id),
            ("environment_id", &self.environment_id),
            ("agent_name", &self.agent_name),
            ("inbox_key", &self.inbox_key),
        ] {
            validate_id(name, value)?;
        }
        Ok(())
    }
}

/// What the instance is doing, as the leader and its controls see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstanceStatus {
    /// No active run; the next waiting item can start one.
    Idle,
    /// A run is active, including one that failed and is still retrying.
    Running,
    /// An operator stopped it. Any active run is held, and nothing starts.
    Stopped,
}

/// The one live record of an instance, on its partition.
///
/// Field order is the serialized order, so two replicas that fold the same
/// journal write the same bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceRecord {
    pub key: InstanceKey,
    /// Counts up each time an instance for this key is deleted and created
    /// again, so nothing from an earlier instance matches this one. Starts at 1.
    pub generation: u64,
    /// Set by an operator's stop, cleared by its lift.
    pub stopped: bool,
    /// The run that holds the instance's one turn. It stays set while the run
    /// retries, and clears only when the run has ended for good.
    pub active_run_id: Option<String>,
    /// Ids of accepted items no run has taken yet, oldest first. Each is a
    /// UUID, 36 bytes, so 1,000 waiting items take about 39 KiB.
    pub waiting_item_ids: Vec<String>,
    /// The sequence number the next run start must carry. Every start applied
    /// at this number uses it up, accepted or rejected, so a run's id, which
    /// is derived from its sequence, is never reused for a different decision.
    pub next_run_sequence: u64,
    /// How many starts were rejected when applied. Resume and attempt ids
    /// carry it, so a decision retried after a rejection gets a fresh id.
    pub rejected_decisions: u64,
    /// A resume asked for the active run and not yet applied: the leader must
    /// decide it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_requested: Option<PendingResume>,
    /// The active run is paused: waiting for an answer, signal or timer, or
    /// held by its leader for an operator stop. A paused run holds no attempt,
    /// so a stop has nothing to cancel.
    #[serde(default, skip_serializing_if = "is_false")]
    pub active_paused: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// A request to resume the instance's active run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingResume {
    pub run_id: String,
    /// The offset of the request's journal record. Names this pause's resume
    /// apart from any earlier one of the same run.
    pub request_offset: u64,
}

impl InstanceRecord {
    /// A new, idle instance with nothing waiting.
    pub fn new(key: InstanceKey, generation: u64) -> Self {
        Self {
            key,
            generation,
            stopped: false,
            active_run_id: None,
            waiting_item_ids: Vec::new(),
            next_run_sequence: 0,
            rejected_decisions: 0,
            resume_requested: None,
            active_paused: false,
        }
    }

    pub fn status(&self) -> InstanceStatus {
        if self.stopped {
            InstanceStatus::Stopped
        } else if self.active_run_id.is_some() {
            InstanceStatus::Running
        } else {
            InstanceStatus::Idle
        }
    }

    /// Serialize the record, refusing one over [`MAX_INSTANCE_RECORD_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>, InstanceError> {
        let bytes =
            serde_json::to_vec(self).map_err(|error| InstanceError::Encoding(error.to_string()))?;
        if bytes.len() > MAX_INSTANCE_RECORD_BYTES {
            return Err(InstanceError::RecordTooLarge {
                limit: MAX_INSTANCE_RECORD_BYTES,
                actual: bytes.len(),
            });
        }
        Ok(bytes)
    }

    /// Read a stored record. Unknown fields are refused, so a binary never
    /// rewrites a record written by a newer one and silently drops its fields.
    pub fn decode(bytes: &[u8]) -> Result<Self, InstanceError> {
        serde_json::from_slice(bytes).map_err(|error| InstanceError::Encoding(error.to_string()))
    }
}

/// Payload of an `instance.created` record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceCreated {
    pub key: InstanceKey,
    pub generation: u64,
}

/// Payload of an `inbox.item` record. The item's content travels in the same
/// journal record and is kept beside the instance record while it waits; only
/// `item_id` goes into the instance record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxItem {
    /// A UUID in lowercase hyphenated form.
    pub item_id: String,
    /// Where the item came from, such as an event id. A duplicate delivery is
    /// absorbed at append by the journal's idempotency, not by the record,
    /// which would otherwise have to remember every source it has seen.
    pub source_id: String,
    /// The instance's inbox key. Project, environment and agent come from the
    /// record the item arrives on.
    pub inbox_key: String,
    /// The generation of the instance the item is for.
    pub generation: u64,
    /// The caller's conversation id, or a default the gateway picks.
    pub conversation_id: String,
    /// How the item arrived, such as "api", "chat", "batch", "mcp" or "event".
    pub channel: String,
    /// Set when the caller is the customer's authenticated backend, which
    /// creates instances on first use. The gateway can't know whether the
    /// instance exists, so the fold creates it when it doesn't. An item
    /// without it, for an instance that doesn't exist, is refused.
    pub create_if_absent: bool,
}

impl InboxItem {
    pub fn validate(&self) -> Result<(), InstanceError> {
        validate_item_id(&self.item_id)?;
        for (name, value) in [
            ("source_id", &self.source_id),
            ("inbox_key", &self.inbox_key),
            ("conversation_id", &self.conversation_id),
            ("channel", &self.channel),
        ] {
            validate_id(name, value)?;
        }
        validate_generation(self.generation)
    }
}

/// A waiting item as the store keeps it beside its instance: the item, its
/// content, and the metadata its run will carry. The map is ordered, so every
/// replica stores the same bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredInboxItem {
    pub item: InboxItem,
    pub content: serde_json::Value,
    pub run_metadata: std::collections::BTreeMap<String, String>,
}

/// A resume request as the store keeps it beside its instance: the
/// `run.resumed` the path would have written, for the leader to append.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredResumeRequest {
    pub run_id: String,
    pub request_offset: u64,
    /// The would-be `run.resumed` payload, base64.
    pub data_b64: String,
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl StoredResumeRequest {
    /// Keep a deferred request, as its journal record carried it.
    pub fn from_record(record: &crate::types::Record) -> Self {
        use base64::Engine;
        Self {
            run_id: record.run_id.clone(),
            request_offset: record.offset.0,
            data_b64: base64::engine::general_purpose::STANDARD.encode(&record.data),
            metadata: record
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }

    /// The request's original `run.resumed` payload.
    pub fn data(&self) -> Result<Vec<u8>, InstanceError> {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .decode(&self.data_b64)
            .map_err(|error| InstanceError::Encoding(error.to_string()))
    }
}

/// Run metadata on the leader's `run.resumed`: the request it applies and the
/// instance's rejected-decision count, which together name its idempotency
/// slot, so a resume refused during a stop isn't blocked after the lift.
pub const META_INSTANCE_RESUME_REQUEST_OFFSET: &str = "instance_resume_request_offset";
pub const META_INSTANCE_REJECTED_DECISIONS: &str = "instance_rejected_decisions";
/// Run metadata on the leader's `run.resumed` that continues a run held for a
/// stop. It carries no answer, so a pushed attempt is sent the run's original
/// input, as a retry is.
pub const META_INSTANCE_RESUME_LIFT: &str = "instance_resume_lift";

/// For an instance-owned run, turn a path's `run.resumed` into a request the
/// leader decides. Every path that resumes a paused run (a signal, an approval
/// answer, a timer) calls this just before it appends; the leader never does,
/// so any decision marker on the record was copied from the run's metadata
/// (a timer's resume clones it) and is removed. Other records are left as they
/// are. Returns whether the record was deferred.
pub fn defer_instance_resume(record: &mut crate::types::Record) -> bool {
    if record.event_type != "run.resumed"
        || !crate::instance_placement::is_instance_run_id(&record.run_id)
    {
        return false;
    }
    strip_instance_decision(&mut record.metadata);
    record.event_type = INSTANCE_RESUME_REQUESTED_EVENT.to_string();
    true
}

/// Remove the leader's decision markers, which a record copied from a run's
/// metadata inherits from the run's last decision.
pub fn strip_instance_decision(metadata: &mut std::collections::HashMap<String, String>) {
    for key in [
        META_INSTANCE_DECISION,
        META_INSTANCE_SEQUENCE,
        META_INSTANCE_ITEM_IDS,
        META_INSTANCE_RESUME_REQUEST_OFFSET,
        META_INSTANCE_REJECTED_DECISIONS,
        META_INSTANCE_RESUME_LIFT,
    ] {
        metadata.remove(key);
    }
}

/// Run metadata marking a `run.queued` as the instance supervisor's decision.
/// The append guard admits a start of an instance-owned run only with it.
pub const META_INSTANCE_DECISION: &str = "instance_decision";
/// The value of [`META_INSTANCE_DECISION`] for a run start.
pub const INSTANCE_DECISION_START: &str = "start";
/// The value of [`META_INSTANCE_DECISION`] for resuming a paused run.
pub const INSTANCE_DECISION_RESUME: &str = "resume";
/// The value of [`META_INSTANCE_DECISION`] on the leader's `run.paused` that
/// holds the active run of a stopped instance. It is fenced by the attempt's
/// lease, with the `operator_stop` expiry reason, so it ends that attempt.
pub const INSTANCE_DECISION_HOLD: &str = "hold";
/// The run's sequence number in its instance.
pub const META_INSTANCE_SEQUENCE: &str = "instance_sequence";
/// The ids of the items the run takes, comma-separated, oldest first.
pub const META_INSTANCE_ITEM_IDS: &str = "instance_item_ids";

/// The supervisor's decision to start run `sequence` with these items. It
/// reaches the fold from the supervisor's `run.queued` for an instance-owned
/// run, so it has no event type of its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStart {
    pub run_id: String,
    pub sequence: u64,
    /// The waiting items the run takes, in the order they arrived.
    pub item_ids: Vec<String>,
}

/// A run of the instance ended for good. It reaches the fold from the run
/// projection when a run reaches a final status; a `run.failed` with attempts
/// left is not an ending.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunEnd {
    pub run_id: String,
}

/// A journal record that changes an instance, as the fold sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstanceEvent {
    Created(InstanceCreated),
    /// `key` is the instance the item's record arrived on; the fold checks it
    /// against the item and, when it creates the instance, uses it.
    ItemAccepted {
        key: InstanceKey,
        item: InboxItem,
    },
    RunStarted(RunStart),
    RunEnded(RunEnd),
    /// The active run paused on its own, to wait for an answer, signal or
    /// timer.
    RunPaused {
        run_id: String,
    },
    /// The leader held the active run for an operator stop. The hold is also
    /// the run's resume request: the lift continues the run as a new attempt.
    RunHeld(PendingResume),
    /// A path asked to resume the active run.
    ResumeRequested(PendingResume),
    /// The leader's resume of the active run was applied.
    ResumeApplied(PendingResume),
    Stopped,
    StopLifted,
}

impl InstanceEvent {
    /// A short name for errors and logs.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Created(_) => INSTANCE_CREATED_EVENT,
            Self::ItemAccepted { .. } => INBOX_ITEM_EVENT,
            Self::RunStarted(_) => "run started",
            Self::RunEnded(_) => "run ended",
            Self::RunPaused { .. } => "run paused",
            Self::RunHeld(_) => "run held",
            Self::ResumeRequested(_) => INSTANCE_RESUME_REQUESTED_EVENT,
            Self::ResumeApplied(_) => "resume applied",
            Self::Stopped => INSTANCE_STOPPED_EVENT,
            Self::StopLifted => INSTANCE_STOP_LIFTED_EVENT,
        }
    }

    /// Decode one of the instance's own journal records from its event type
    /// and JSON data. `key` is the instance the caller resolved the record to;
    /// a payload naming another instance is refused. Returns `None` for any
    /// other event type; run starts and endings are built by the run
    /// projection instead.
    pub fn from_journal(
        key: &InstanceKey,
        event_type: &str,
        data: &[u8],
    ) -> Result<Option<Self>, InstanceError> {
        fn parse<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, InstanceError> {
            serde_json::from_slice(data).map_err(|error| InstanceError::Invalid(error.to_string()))
        }
        let event = match event_type {
            INSTANCE_CREATED_EVENT => {
                let created: InstanceCreated = parse(data)?;
                if created.key != *key {
                    return Err(InstanceError::Invalid(
                        "instance.created names a different instance".into(),
                    ));
                }
                Self::Created(created)
            }
            INBOX_ITEM_EVENT => Self::ItemAccepted {
                key: key.clone(),
                item: parse(data)?,
            },
            INSTANCE_STOPPED_EVENT => Self::Stopped,
            INSTANCE_STOP_LIFTED_EVENT => Self::StopLifted,
            _ => return Ok(None),
        };
        Ok(Some(event))
    }
}

/// The key of a follow-up in a timer's schedule payload.
pub const FOLLOWUP_PAYLOAD_KEY: &str = "followup";
/// The channel of an item a follow-up delivers.
pub const FOLLOWUP_CHANNEL: &str = "followup";
/// Run template keys that name one run, not the item: the leader sets them on
/// the run it starts.
const FOLLOWUP_RUN_ONLY_METADATA: &[&str] = &["root_run_id", "session_id"];

/// What a follow-up timer delivers when it fires: an item in its instance's
/// inbox, in a conversation, carrying the run template the next start needs.
///
/// A follow-up is a timer on the instance's inbox, not on the run that asked
/// for it: a fired timer only resumes a paused run, and the instance's next
/// run is the leader's to start.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowupPayload {
    pub conversation_id: String,
    pub content: serde_json::Value,
    /// The dispatch metadata the run that takes the item carries: route,
    /// deployment pin, workspace. Copied from the run that scheduled it.
    #[serde(default)]
    pub run_template: std::collections::BTreeMap<String, String>,
}

impl FollowupPayload {
    /// The follow-up a stored timer's schedule payload carries, if any.
    pub fn from_schedule_payload(schedule_payload: &[u8]) -> Option<Self> {
        let mut payload = serde_json::from_slice::<serde_json::Value>(schedule_payload).ok()?;
        serde_json::from_value(payload.get_mut(FOLLOWUP_PAYLOAD_KEY)?.take()).ok()
    }
}

/// A run of an instance asked for a follow-up. The worker appends it on its
/// own run, under its lease; the runtime turns it, at append, into the
/// follow-up's timer on the run's inbox (see [`followup_timer_record`]).
pub const INSTANCE_FOLLOWUP_REQUESTED_EVENT: &str = "instance.followup_requested";

/// The payload of an `instance.followup_requested` record.
/// Unknown fields are allowed: the SDK sends it inside its event envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowupRequest {
    /// Names this follow-up within its run: the tool call's stable key. A
    /// replay of the call asks again with the same key and gets the same
    /// timer.
    pub followup_key: String,
    /// When it is due, in nanoseconds since the Unix epoch.
    pub fire_at_ns: i64,
    pub conversation_id: String,
    pub content: serde_json::Value,
}

/// The longest follow-up key.
pub const MAX_FOLLOWUP_KEY_BYTES: usize = 128;

/// The timer id of a run's follow-up: one per key, so the SDK knows it before
/// the request is appended and a replay finds the same timer.
pub fn followup_timer_id(run_id: &str, followup_key: &str) -> String {
    format!("followup-{run_id}-{followup_key}")
}

/// Dispatch metadata a follow-up's run inherits from the run that asked for
/// it. Only these: never the asking run's lease, markers, attempt or trace.
const FOLLOWUP_TEMPLATE_KEYS: &[&str] = &[
    "deployment_id",
    "workspace_id",
    "tenant_id",
    "environment_id",
    "project_id",
    "component_type",
    "component_name",
    "dispatch_mode",
    "priority",
    "user_id",
];

/// Turn an instance run's `instance.followup_requested` into the follow-up's
/// `timer.scheduled` on the run's inbox. `run_metadata` is the asking run's,
/// as projected: the inbox and the run template come from it, not from the
/// worker, so a run can schedule follow-ups only for its own instance.
pub fn followup_timer_record(
    request: &crate::types::Record,
    run_metadata: &std::collections::HashMap<String, String>,
) -> Result<crate::types::Record, InstanceError> {
    if !crate::instance_placement::is_instance_run_id(&request.run_id) {
        return Err(InstanceError::Invalid(
            "only an agent instance's run can schedule a follow-up".into(),
        ));
    }
    let inbox_run_id = run_metadata
        .get(META_INSTANCE_INBOX_RUN_ID)
        .filter(|inbox| crate::instance_placement::parse_inbox_run_id(inbox).is_some())
        .ok_or_else(|| InstanceError::Invalid("the run names no instance inbox".into()))?;
    let followup: FollowupRequest = serde_json::from_slice(&request.data)
        .map_err(|error| InstanceError::Invalid(format!("follow-up request: {error}")))?;
    let key = &followup.followup_key;
    validate_key("followup_key", key)?;
    if followup.fire_at_ns <= 0 {
        return Err(InstanceError::Invalid("fire_at_ns must be positive".into()));
    }
    validate_id("conversation_id", &followup.conversation_id)?;
    let timer_id = followup_timer_id(&request.run_id, key);
    let payload = FollowupPayload {
        conversation_id: followup.conversation_id,
        content: followup.content,
        run_template: FOLLOWUP_TEMPLATE_KEYS
            .iter()
            .filter_map(|key| {
                run_metadata
                    .get(*key)
                    .map(|value| (key.to_string(), value.clone()))
            })
            .collect(),
    };
    let data = serde_json::to_vec(&serde_json::json!({
        "fire_at_ns": followup.fire_at_ns,
        "schedule_payload": { FOLLOWUP_PAYLOAD_KEY: payload },
    }))
    .map_err(|error| InstanceError::Encoding(error.to_string()))?;
    Ok(crate::types::Record {
        offset: crate::types::Offset::ZERO,
        project_id: request.project_id.clone(),
        run_id: inbox_run_id.clone(),
        event_type: "timer.scheduled".to_string(),
        data: data.into(),
        timestamp_ns: request.timestamp_ns,
        step_key: None,
        correlation_id: request.correlation_id.clone(),
        parent_event_id: None,
        metadata: std::collections::HashMap::from([
            ("timer_id".to_string(), timer_id),
            ("timer_key".to_string(), format!("followup:{key}")),
            ("schedule_type".to_string(), "absolute".to_string()),
            (
                crate::journal_contract::META_FOLLOWUP_REQUESTED_BY.to_string(),
                request.run_id.clone(),
            ),
        ]),
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

/// A run of an instance asked to send its user a message. The worker appends
/// it on its own run, under its lease; the runtime turns it, at append, into
/// an [`INSTANCE_MESSAGE_EVENT`] on the run's inbox.
pub const INSTANCE_MESSAGE_REQUESTED_EVENT: &str = "instance.message_requested";
/// A message an instance sent its user, on the instance's inbox. Each becomes
/// a `message` event for the project's subscriptions to deliver.
pub const INSTANCE_MESSAGE_EVENT: &str = "instance.message";
/// Metadata naming an `instance.message`'s message id, its idempotency slot.
pub const META_INSTANCE_MESSAGE_ID: &str = "instance_message_id";

/// How many of one instance's messages may wait to be delivered before a new
/// `send` is refused.
pub const MAX_HELD_MESSAGES: usize = 100;

/// The payload of an `instance.message_requested` record. Unknown fields are
/// allowed: the SDK sends it inside its event envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRequest {
    /// Names this message within its run: the sending call's key plus a hash
    /// of what it sends, so a replay sends nothing new and a different
    /// message at the same call is a different message.
    pub message_key: String,
    pub conversation_id: String,
    pub content: serde_json::Value,
}

/// The payload of an `instance.message` record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredMessage {
    pub message_id: String,
    pub conversation_id: String,
    pub content: serde_json::Value,
    pub sender_run_id: String,
}

/// The id of a message a run sends: one per key.
pub fn message_id(run_id: &str, message_key: &str) -> String {
    format!("msg-{run_id}-{message_key}")
}

/// Turn an instance run's `instance.message_requested` into the message on
/// its inbox. As for a follow-up, the inbox and the dispatch metadata the
/// event is matched on come from the run as projected, never from the worker.
pub fn message_record(
    request: &crate::types::Record,
    run_metadata: &std::collections::HashMap<String, String>,
) -> Result<crate::types::Record, InstanceError> {
    if !crate::instance_placement::is_instance_run_id(&request.run_id) {
        return Err(InstanceError::Invalid(
            "only an agent instance's run can send its user a message".into(),
        ));
    }
    let inbox_run_id = run_metadata
        .get(META_INSTANCE_INBOX_RUN_ID)
        .filter(|inbox| crate::instance_placement::parse_inbox_run_id(inbox).is_some())
        .ok_or_else(|| InstanceError::Invalid("the run names no instance inbox".into()))?;
    let message: MessageRequest = serde_json::from_slice(&request.data)
        .map_err(|error| InstanceError::Invalid(format!("message request: {error}")))?;
    validate_key("message_key", &message.message_key)?;
    validate_id("conversation_id", &message.conversation_id)?;
    let message_id = message_id(&request.run_id, &message.message_key);
    let stored = StoredMessage {
        message_id: message_id.clone(),
        conversation_id: message.conversation_id,
        content: message.content,
        sender_run_id: request.run_id.clone(),
    };
    let mut metadata: std::collections::HashMap<String, String> = FOLLOWUP_TEMPLATE_KEYS
        .iter()
        .filter_map(|key| {
            run_metadata
                .get(*key)
                .map(|value| (key.to_string(), value.clone()))
        })
        .collect();
    metadata.insert(META_INSTANCE_MESSAGE_ID.to_string(), message_id);
    Ok(crate::types::Record {
        offset: crate::types::Offset::ZERO,
        project_id: request.project_id.clone(),
        run_id: inbox_run_id.clone(),
        event_type: INSTANCE_MESSAGE_EVENT.to_string(),
        data: serde_json::to_vec(&stored)
            .map_err(|error| InstanceError::Encoding(error.to_string()))?
            .into(),
        timestamp_ns: request.timestamp_ns,
        step_key: None,
        correlation_id: request.correlation_id.clone(),
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

/// A key a call names its follow-up or message by: 1 to 128 bytes of letters,
/// digits and `-_.:#`, so ids built from it stay unambiguous.
fn validate_key(name: &str, key: &str) -> Result<(), InstanceError> {
    if key.is_empty()
        || key.len() > MAX_FOLLOWUP_KEY_BYTES
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:#".contains(&byte))
    {
        return Err(InstanceError::Invalid(format!(
            "{name} must be 1 to {MAX_FOLLOWUP_KEY_BYTES} bytes of letters, digits and -_.:#"
        )));
    }
    Ok(())
}

/// The source of the item a follow-up delivers. One timer id per occurrence,
/// so one item per firing, however often the leader retries it.
pub fn followup_source_id(timer_id: &str) -> String {
    format!("followup:{timer_id}")
}

/// The `inbox.item` a follow-up appends when it fires, in the same append as
/// its `timer.fired`, on the instance's inbox.
pub fn followup_item_record(
    project_id: &str,
    inbox_run_id: &str,
    timer_id: &str,
    payload: &FollowupPayload,
    timestamp_ns: i64,
) -> Result<crate::types::Record, InstanceError> {
    let identity = crate::instance_placement::parse_inbox_run_id(inbox_run_id)
        .ok_or_else(|| InstanceError::Invalid("a follow-up's timer is not on an inbox".into()))?;
    let source_id = followup_source_id(timer_id);
    let item = InboxItem {
        item_id: crate::instance_placement::inbox_item_id(inbox_run_id, &source_id),
        source_id: source_id.clone(),
        inbox_key: identity.inbox_key().to_string(),
        generation: identity.generation(),
        conversation_id: payload.conversation_id.clone(),
        channel: FOLLOWUP_CHANNEL.to_string(),
        // The instance asked for it, so it exists; one deleted since then
        // must not be brought back by its own timer.
        create_if_absent: false,
    };
    item.validate()?;
    let mut data =
        serde_json::to_value(&item).map_err(|error| InstanceError::Encoding(error.to_string()))?;
    data["content"] = payload.content.clone();
    let mut metadata: std::collections::HashMap<String, String> = payload
        .run_template
        .iter()
        .filter(|(key, _)| !FOLLOWUP_RUN_ONLY_METADATA.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    metadata.insert(
        crate::journal_contract::META_INBOX_SOURCE_ID.to_string(),
        source_id,
    );
    metadata.insert("project_id".to_string(), project_id.to_string());
    metadata.insert("inbox_channel".to_string(), FOLLOWUP_CHANNEL.to_string());
    Ok(crate::types::Record {
        offset: crate::types::Offset::ZERO,
        project_id: project_id.to_string(),
        run_id: inbox_run_id.to_string(),
        event_type: INBOX_ITEM_EVENT.to_string(),
        data: serde_json::to_vec(&data)
            .map_err(|error| InstanceError::Encoding(error.to_string()))?
            .into(),
        timestamp_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    })
}

/// Non-empty, at most [`MAX_INSTANCE_ID_BYTES`], and without a NUL byte.
pub fn validate_id(name: &str, value: &str) -> Result<(), InstanceError> {
    if value.is_empty() || value.len() > MAX_INSTANCE_ID_BYTES || value.contains('\0') {
        return Err(InstanceError::Invalid(format!(
            "{name} must be 1 to {MAX_INSTANCE_ID_BYTES} bytes with no NUL byte"
        )));
    }
    Ok(())
}

/// A UUID in lowercase hyphenated form, such as
/// `0190a8f2-7c1e-7b3a-9f00-2c4d5e6f7a8b`. Fixing the id's size is what keeps
/// a full inbox under the record's size limit, and fixing its case means the
/// same item can't wait twice under two spellings.
pub fn validate_item_id(value: &str) -> Result<(), InstanceError> {
    let bytes = value.as_bytes();
    let well_formed = bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(byte),
        });
    if !well_formed {
        return Err(InstanceError::Invalid(format!(
            "item id {value:?} is not a lowercase hyphenated UUID"
        )));
    }
    Ok(())
}

/// Generations count from 1.
pub fn validate_generation(generation: u64) -> Result<(), InstanceError> {
    if generation == 0 {
        return Err(InstanceError::Invalid("generation starts at 1".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> InstanceKey {
        InstanceKey {
            project_id: "project-1".into(),
            environment_id: "production".into(),
            agent_name: "support".into(),
            inbox_key: "user-alex".into(),
        }
    }

    fn item() -> InboxItem {
        InboxItem {
            item_id: "0190a8f2-7c1e-7b3a-9f00-2c4d5e6f7a8b".into(),
            source_id: "evt-1".into(),
            inbox_key: "user-alex".into(),
            generation: 1,
            conversation_id: "default".into(),
            channel: "api".into(),
            create_if_absent: true,
        }
    }

    #[test]
    fn record_round_trips() {
        let mut record = InstanceRecord::new(key(), 1);
        record.active_run_id = Some("run-0".into());
        record.waiting_item_ids = vec![item().item_id];
        record.next_run_sequence = 1;
        record.rejected_decisions = 2;
        let bytes = record.encode().unwrap();
        assert_eq!(InstanceRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn decode_refuses_unknown_fields() {
        let mut value = serde_json::to_value(InstanceRecord::new(key(), 1)).unwrap();
        value["later_field"] = serde_json::json!(1);
        let bytes = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            InstanceRecord::decode(&bytes),
            Err(InstanceError::Encoding(_))
        ));
    }

    #[test]
    fn encode_refuses_a_record_over_the_limit() {
        let mut record = InstanceRecord::new(key(), 1);
        record.waiting_item_ids = vec!["x".repeat(MAX_INSTANCE_RECORD_BYTES)];
        assert!(matches!(
            record.encode(),
            Err(InstanceError::RecordTooLarge { limit, actual })
                if limit == MAX_INSTANCE_RECORD_BYTES && actual > limit
        ));
    }

    #[test]
    fn status_follows_stop_then_active_run() {
        let mut record = InstanceRecord::new(key(), 1);
        assert_eq!(record.status(), InstanceStatus::Idle);
        record.active_run_id = Some("run-0".into());
        assert_eq!(record.status(), InstanceStatus::Running);
        record.stopped = true;
        assert_eq!(record.status(), InstanceStatus::Stopped);
    }

    #[test]
    fn key_validation_refuses_empty_long_and_nul_parts() {
        assert!(key().validate().is_ok());
        for bad in [
            String::new(),
            "a".repeat(MAX_INSTANCE_ID_BYTES + 1),
            "a\0b".into(),
        ] {
            let mut k = key();
            k.inbox_key = bad;
            assert!(matches!(k.validate(), Err(InstanceError::Invalid(_))));
        }
    }

    #[test]
    fn item_ids_must_be_lowercase_hyphenated_uuids() {
        assert!(validate_item_id("0190a8f2-7c1e-7b3a-9f00-2c4d5e6f7a8b").is_ok());
        for bad in [
            "",
            "item-1",
            "0190A8F2-7C1E-7B3A-9F00-2C4D5E6F7A8B",
            "0190a8f27c1e7b3a9f002c4d5e6f7a8b",
            "{0190a8f2-7c1e-7b3a-9f00-2c4d5e6f7a8b}",
            "0190a8f2-7c1e-7b3a-9f00-2c4d5e6f7a8g",
            "0190a8f2-7c1e-7b3a-9f00-2c4d5e6f7a8b0",
        ] {
            assert!(
                matches!(validate_item_id(bad), Err(InstanceError::Invalid(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn item_validation_covers_every_field() {
        assert!(item().validate().is_ok());
        let cases: [fn(&mut InboxItem); 6] = [
            |item| item.item_id = "item-1".into(),
            |item| item.source_id = String::new(),
            |item| item.inbox_key = "a\0b".into(),
            |item| item.conversation_id = String::new(),
            |item| item.channel = "c".repeat(MAX_INSTANCE_ID_BYTES + 1),
            |item| item.generation = 0,
        ];
        for break_it in cases {
            let mut bad = item();
            break_it(&mut bad);
            assert!(matches!(bad.validate(), Err(InstanceError::Invalid(_))));
        }
    }

    #[test]
    fn journal_records_decode_by_event_type() {
        let created = serde_json::to_vec(&InstanceCreated {
            key: key(),
            generation: 1,
        })
        .unwrap();
        assert_eq!(
            InstanceEvent::from_journal(&key(), INSTANCE_CREATED_EVENT, &created).unwrap(),
            Some(InstanceEvent::Created(InstanceCreated {
                key: key(),
                generation: 1
            }))
        );
        let item_data = serde_json::to_vec(&item()).unwrap();
        assert_eq!(
            InstanceEvent::from_journal(&key(), INBOX_ITEM_EVENT, &item_data).unwrap(),
            Some(InstanceEvent::ItemAccepted {
                key: key(),
                item: item()
            })
        );
        assert_eq!(
            InstanceEvent::from_journal(&key(), INSTANCE_STOPPED_EVENT, b"{}").unwrap(),
            Some(InstanceEvent::Stopped)
        );
        assert_eq!(
            InstanceEvent::from_journal(&key(), INSTANCE_STOP_LIFTED_EVENT, b"").unwrap(),
            Some(InstanceEvent::StopLifted)
        );
        assert_eq!(
            InstanceEvent::from_journal(&key(), "run.queued", b"{}").unwrap(),
            None
        );
        assert!(matches!(
            InstanceEvent::from_journal(&key(), INBOX_ITEM_EVENT, br#"{"item_id":"x"}"#),
            Err(InstanceError::Invalid(_))
        ));
        let mut other = key();
        other.agent_name = "billing".into();
        assert!(matches!(
            InstanceEvent::from_journal(&other, INSTANCE_CREATED_EVENT, &created),
            Err(InstanceError::Invalid(_))
        ));
    }
}

#[cfg(test)]
mod defer_tests {
    use super::*;
    use crate::instance_placement::{instance_run_id, InstanceIdentity};

    fn resumed(run_id: &str) -> crate::types::Record {
        crate::types::Record {
            offset: crate::types::Offset(42),
            project_id: "p".into(),
            run_id: run_id.into(),
            event_type: "run.resumed".into(),
            data: bytes::Bytes::from_static(br#"{"x":1}"#),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: std::collections::HashMap::from([("k".to_string(), "v".to_string())]),
            data_type: "json".into(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    #[test]
    fn a_followup_delivers_one_item_the_journal_accepts_to_its_inbox() {
        use crate::instance_placement::{inbox_run_id, parse_inbox_run_id};
        let identity = InstanceIdentity::new("project-1", "env-1", "support", "u_1", 1).unwrap();
        let inbox = inbox_run_id(&identity, 0, 1).unwrap();
        let payload = FollowupPayload {
            conversation_id: "pr-42".into(),
            content: serde_json::json!({"message": "check the PR again"}),
            run_template: [
                ("deployment_id".to_string(), "dep-1".to_string()),
                ("root_run_id".to_string(), "a-previous-run".to_string()),
            ]
            .into_iter()
            .collect(),
        };
        let schedule =
            serde_json::to_vec(&serde_json::json!({ FOLLOWUP_PAYLOAD_KEY: payload })).unwrap();
        assert_eq!(
            FollowupPayload::from_schedule_payload(&schedule),
            Some(payload.clone())
        );
        assert_eq!(FollowupPayload::from_schedule_payload(b"{}"), None);

        let record = followup_item_record("project-1", &inbox, "tmr-1", &payload, 7).unwrap();
        crate::journal_contract::validate_append_records(std::slice::from_ref(&record)).unwrap();
        let item: InboxItem = serde_json::from_slice(&record.data).unwrap();
        assert_eq!(item.source_id, "followup:tmr-1");
        assert_eq!(item.channel, FOLLOWUP_CHANNEL);
        assert_eq!(item.conversation_id, "pr-42");
        assert!(!item.create_if_absent);
        assert_eq!(
            parse_inbox_run_id(&record.run_id).unwrap().inbox_key(),
            "u_1"
        );
        assert_eq!(record.metadata["deployment_id"], "dep-1");
        assert!(
            !record.metadata.contains_key("root_run_id"),
            "the leader names the run"
        );
        // The same timer always delivers the same item.
        let again = followup_item_record("project-1", &inbox, "tmr-1", &payload, 9).unwrap();
        assert_eq!(again.data, record.data);
        assert!(followup_item_record("project-1", "not-an-inbox", "tmr-1", &payload, 7).is_err());
    }

    #[test]
    fn a_runs_followup_request_becomes_a_timer_on_its_own_inbox() {
        use crate::instance_placement::{inbox_run_id, instance_run_id};
        let identity = InstanceIdentity::new("project-1", "env-1", "support", "u_1", 1).unwrap();
        let inbox = inbox_run_id(&identity, 0, 1).unwrap();
        let run = instance_run_id(&identity, 0, 0, 1).unwrap();
        let mut request = resumed(&run);
        request.event_type = INSTANCE_FOLLOWUP_REQUESTED_EVENT.into();
        request.data = serde_json::to_vec(&serde_json::json!({
            "followup_key": "call-7",
            "fire_at_ns": 5_000_000_000_i64,
            "conversation_id": "pr-42",
            "content": {"message": "check again"},
        }))
        .unwrap()
        .into();
        // The worker's own fields never reach the timer; the run's do.
        request
            .metadata
            .insert("deployment_id".into(), "forged".into());
        let run_metadata = std::collections::HashMap::from([
            (META_INSTANCE_INBOX_RUN_ID.to_string(), inbox.clone()),
            ("deployment_id".to_string(), "dep-1".to_string()),
            ("lease_id".to_string(), "lease-9".to_string()),
            (META_INSTANCE_DECISION.to_string(), "start".to_string()),
        ]);
        let timer = followup_timer_record(&request, &run_metadata).unwrap();
        assert_eq!(timer.run_id, inbox);
        assert_eq!(timer.event_type, "timer.scheduled");
        assert_eq!(
            timer.metadata["timer_id"],
            followup_timer_id(&run, "call-7")
        );
        crate::journal_contract::validate_append_records(std::slice::from_ref(&timer)).unwrap();
        let data: serde_json::Value = serde_json::from_slice(&timer.data).unwrap();
        let payload = FollowupPayload::from_schedule_payload(
            &serde_json::to_vec(&data["schedule_payload"]).unwrap(),
        )
        .unwrap();
        assert_eq!(payload.conversation_id, "pr-42");
        assert_eq!(
            payload
                .run_template
                .get("deployment_id")
                .map(String::as_str),
            Some("dep-1")
        );
        assert!(!payload.run_template.contains_key("lease_id"));
        assert!(!payload.run_template.contains_key(META_INSTANCE_DECISION));

        // A plain run, a run without an inbox, or a bad key are refused.
        let mut plain = request.clone();
        plain.run_id = "0190d4a8-0000-7000-8000-000000000001".into();
        assert!(followup_timer_record(&plain, &run_metadata).is_err());
        assert!(followup_timer_record(&request, &std::collections::HashMap::new()).is_err());
        let mut bad_key = request.clone();
        bad_key.data = serde_json::to_vec(&serde_json::json!({
            "followup_key": "a/b", "fire_at_ns": 1, "conversation_id": "c", "content": null,
        }))
        .unwrap()
        .into();
        assert!(followup_timer_record(&bad_key, &run_metadata).is_err());
    }

    #[test]
    fn only_a_paths_resume_of_an_instance_run_is_deferred() {
        let identity = InstanceIdentity::new("p", "e", "a", "u", 1).unwrap();
        let instance_run = instance_run_id(&identity, 0, 0, 1).unwrap();

        let mut record = resumed(&instance_run);
        assert!(defer_instance_resume(&mut record));
        assert_eq!(record.event_type, INSTANCE_RESUME_REQUESTED_EVENT);

        let mut plain = resumed("0190d4a8-0000-7000-8000-000000000001");
        assert!(!defer_instance_resume(&mut plain));
        assert_eq!(plain.event_type, "run.resumed");

        // A timer's resume clones the run's metadata, and with it the
        // marker of the run's last decision: it is still a path's resume.
        for inherited in [INSTANCE_DECISION_START, INSTANCE_DECISION_RESUME] {
            let mut timer = resumed(&instance_run);
            timer
                .metadata
                .insert(META_INSTANCE_DECISION.into(), inherited.into());
            timer
                .metadata
                .insert(META_INSTANCE_SEQUENCE.into(), "0".into());
            assert!(defer_instance_resume(&mut timer));
            assert_eq!(timer.event_type, INSTANCE_RESUME_REQUESTED_EVENT);
            assert!(!timer.metadata.contains_key(META_INSTANCE_DECISION));
            assert!(!timer.metadata.contains_key(META_INSTANCE_SEQUENCE));
        }
    }

    #[test]
    fn a_stored_request_keeps_the_resume_exactly() {
        let identity = InstanceIdentity::new("p", "e", "a", "u", 1).unwrap();
        let record = resumed(&instance_run_id(&identity, 0, 0, 1).unwrap());
        let stored = StoredResumeRequest::from_record(&record);
        assert_eq!(stored.request_offset, 42);
        assert_eq!(stored.data().unwrap(), record.data.to_vec());
        assert_eq!(stored.metadata.get("k").map(String::as_str), Some("v"));
    }
}
