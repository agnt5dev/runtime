//! Durable Activation journal payloads.
//!
//! These structs are the JSON `data` of every activation record. The wire
//! request types stay protobuf; the journal keeps canonical JSON so readers
//! (SSE, run events, the UI) see the same shape the decorative step events
//! used to carry, and the runtime still byte-compares retries.

use engine_proto::activation_payload::Value;
use engine_proto::{
    ActivationEvidence, ActivationOutboxIntent, ActivationPayload, ActivationStateMutation,
    ActivationUsage, ChildActivationLinkage, ImmutablePayloadReference,
};
use serde::{Deserialize, Serialize};

/// Journal record version for JSON-encoded activation payloads.
pub const ACTIVATION_JOURNAL_VERSION: u32 = 2;

/// Fields every non-started record repeats from the attempt it settles, so a
/// completion, failure, suspension, or cancellation is renderable on its own
/// and the fold can check it against the current attempt.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationHeader {
    pub name: String,
    pub kind: i32,
    pub stable_key: String,
    pub parent_activation_id: String,
    /// Reader ancestry, independent of durable ownership and activation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_parent_correlation_id: Option<String>,
    pub recovery_policy: i32,
    #[serde(with = "b64")]
    pub input_digest: Vec<u8>,
    #[serde(with = "b64")]
    pub definition_digest: Vec<u8>,
    #[serde(with = "child")]
    pub child: Option<ChildActivationLinkage>,
}

impl ActivationHeader {
    pub fn from_started(started: &ActivationStartedRecord) -> Self {
        Self {
            name: started.name.clone(),
            kind: started.kind,
            stable_key: started.stable_key.clone(),
            parent_activation_id: started.parent_activation_id.clone(),
            display_parent_correlation_id: started.display_parent_correlation_id.clone(),
            recovery_policy: started.recovery_policy,
            input_digest: started.input_digest.clone(),
            definition_digest: started.definition_digest.clone(),
            child: started.child.clone(),
        }
    }
}

/// A new attempt. `attempt == 1` is the first execution; a retry carries
/// `previous_attempt` and is still a `.started` record.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationStartedRecord {
    pub project_id: String,
    pub run_id: String,
    pub parent_activation_id: String,
    /// Reader ancestry, independent of durable ownership and activation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_parent_correlation_id: Option<String>,
    pub activation_id: String,
    pub kind: i32,
    pub stable_key: String,
    pub name: String,
    #[serde(with = "b64")]
    pub input_digest: Vec<u8>,
    #[serde(with = "b64")]
    pub definition_digest: Vec<u8>,
    pub recovery_policy: i32,
    pub attempt: u32,
    pub previous_attempt: u32,
    #[serde(with = "child")]
    pub child: Option<ChildActivationLinkage>,
    /// Always `"execute"`: a replay appends nothing.
    pub decision: String,
    /// Plaintext JSON input supplied by the worker for readers. Never part of
    /// the activation identity.
    pub input_data: Option<serde_json::Value>,
    /// Authority the attempt was admitted with. Stripped from public views.
    #[serde(with = "b64")]
    pub fence_token: Vec<u8>,
    pub worker_session_id: String,
    #[serde(with = "b64")]
    pub run_authority: Vec<u8>,
    #[serde(with = "b64")]
    pub lease_authority: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationCompletedRecord {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub attempt: u32,
    #[serde(flatten)]
    pub header: ActivationHeader,
    pub started_journal_offset: u64,
    #[serde(with = "b64")]
    pub fence_token: Vec<u8>,
    #[serde(with = "payload_value")]
    pub output: Option<ActivationPayload>,
    #[serde(with = "b64")]
    pub output_digest: Vec<u8>,
    #[serde(with = "mutations")]
    pub state_mutations: Vec<ActivationStateMutation>,
    #[serde(with = "intents")]
    pub outbox_intents: Vec<ActivationOutboxIntent>,
    #[serde(with = "usage")]
    pub usage: Option<ActivationUsage>,
    /// `usage.latency_ms` when positive; derived, never clocked here.
    pub duration_ms: Option<i64>,
    #[serde(with = "evidence")]
    pub evidence: Vec<ActivationEvidence>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationFailedRecord {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub attempt: u32,
    #[serde(flatten)]
    pub header: ActivationHeader,
    pub started_journal_offset: u64,
    #[serde(with = "b64")]
    pub fence_token: Vec<u8>,
    pub error_code: String,
    /// `error_data.message` when the inline error is a JSON object with a
    /// string message; derived deterministically for readers.
    pub error_message: String,
    #[serde(with = "payload_value")]
    pub error_data: Option<ActivationPayload>,
    pub retryable: bool,
    pub external_outcome_certainty: i32,
    pub latency_ms: i64,
    pub duration_ms: Option<i64>,
    #[serde(with = "evidence")]
    pub evidence: Vec<ActivationEvidence>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationSuspendedRecord {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub attempt: u32,
    #[serde(flatten)]
    pub header: ActivationHeader,
    pub started_journal_offset: u64,
    #[serde(with = "b64")]
    pub fence_token: Vec<u8>,
    pub resume_source: String,
    pub timer_key: String,
    pub ready_at_ms: i64,
    #[serde(rename = "continuation_b64", with = "b64")]
    pub continuation: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationCancelledRecord {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub attempt: u32,
    #[serde(flatten)]
    pub header: ActivationHeader,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationUnknownOutcomeRecord {
    pub failure: Option<ActivationFailedRecord>,
}

/// Journal-owned compaction snapshot of one aggregate.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ActivationSnapshotRecord {
    pub project_id: String,
    pub run_id: String,
    pub activation_id: String,
    pub status: i32,
    pub current_attempt: Option<ActivationStartedRecord>,
    pub completion: Option<ActivationCompletedRecord>,
    pub failure: Option<ActivationFailedRecord>,
    pub accepted_journal_offset: u64,
    pub attempt_started_offset: u64,
    pub suspension: Option<ActivationSuspendedRecord>,
    pub cancellation: Option<ActivationCancelledRecord>,
}

/// One activation transition. The tag is the transition name every reader
/// and metric already uses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "transition", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Started(ActivationStartedRecord),
    Completed(ActivationCompletedRecord),
    Failed(ActivationFailedRecord),
    Suspended(ActivationSuspendedRecord),
    Cancelled(ActivationCancelledRecord),
    Snapshot(ActivationSnapshotRecord),
    UnknownOutcome(ActivationUnknownOutcomeRecord),
}

/// The canonical activation journal payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActivationJournalRecord {
    pub version: u32,
    pub event: Option<Event>,
}

impl Event {
    /// The optional display parent retained by this attempt and its receipts.
    pub fn display_parent_correlation_id(&self) -> Option<&str> {
        match self {
            Event::Started(value) => value.display_parent_correlation_id.as_deref(),
            Event::Completed(value) => value.header.display_parent_correlation_id.as_deref(),
            Event::Failed(value) => value.header.display_parent_correlation_id.as_deref(),
            Event::Suspended(value) => value.header.display_parent_correlation_id.as_deref(),
            Event::Cancelled(value) => value.header.display_parent_correlation_id.as_deref(),
            Event::UnknownOutcome(value) => value
                .failure
                .as_ref()
                .and_then(|failure| failure.header.display_parent_correlation_id.as_deref()),
            Event::Snapshot(value) => value
                .current_attempt
                .as_ref()
                .and_then(|started| started.display_parent_correlation_id.as_deref()),
        }
    }

    /// The activation kind this record belongs to, when the payload names it.
    pub fn activation_kind(&self) -> Option<i32> {
        match self {
            Event::Started(started) => Some(started.kind),
            Event::Completed(value) => Some(value.header.kind),
            Event::Failed(value) => Some(value.header.kind),
            Event::Suspended(value) => Some(value.header.kind),
            Event::Cancelled(value) => Some(value.header.kind),
            Event::UnknownOutcome(value) => value.failure.as_ref().map(|f| f.header.kind),
            Event::Snapshot(value) => value.current_attempt.as_ref().map(|s| s.kind),
        }
    }
}

pub(crate) mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn encode(bytes: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }

    pub fn decode(value: &str) -> Result<Vec<u8>, base64::DecodeError> {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value)
    }

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        encode(bytes).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let value = String::deserialize(deserializer)?;
        decode(&value).map_err(serde::de::Error::custom)
    }
}

/// JSON mirror of `ActivationPayload`: `{"inline_b64": ..}`, `{"reference": {..}}`,
/// or `null`.
pub(crate) mod payload_value {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    pub(crate) struct Reference {
        project_id: String,
        kind: String,
        uri: String,
        size_bytes: u64,
        #[serde(with = "b64")]
        sha256: Vec<u8>,
        content_type: String,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    pub(crate) enum Mirror {
        Inline {
            #[serde(with = "b64")]
            inline_b64: Vec<u8>,
        },
        Reference {
            reference: Reference,
        },
    }

    pub(crate) fn to_mirror(payload: &ActivationPayload) -> Option<Mirror> {
        match payload.value.as_ref()? {
            Value::InlineData(bytes) => Some(Mirror::Inline {
                inline_b64: bytes.clone(),
            }),
            Value::Reference(reference) => Some(Mirror::Reference {
                reference: Reference {
                    project_id: reference.project_id.clone(),
                    kind: reference.kind.clone(),
                    uri: reference.uri.clone(),
                    size_bytes: reference.size_bytes,
                    sha256: reference.sha256.clone(),
                    content_type: reference.content_type.clone(),
                },
            }),
        }
    }

    pub(crate) fn from_mirror(mirror: Mirror) -> ActivationPayload {
        ActivationPayload {
            value: Some(match mirror {
                Mirror::Inline { inline_b64 } => Value::InlineData(inline_b64),
                Mirror::Reference { reference } => Value::Reference(ImmutablePayloadReference {
                    project_id: reference.project_id,
                    kind: reference.kind,
                    uri: reference.uri,
                    size_bytes: reference.size_bytes,
                    sha256: reference.sha256,
                    content_type: reference.content_type,
                }),
            }),
        }
    }

    pub fn serialize<S: Serializer>(
        value: &Option<ActivationPayload>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.as_ref().and_then(to_mirror).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ActivationPayload>, D::Error> {
        Ok(Option::<Mirror>::deserialize(deserializer)?.map(from_mirror))
    }
}

pub(crate) mod usage {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Mirror {
        tokens_in: i64,
        tokens_out: i64,
        cached_tokens: i64,
        cost_usd: f64,
        latency_ms: i64,
        provider: String,
        model: String,
    }

    pub fn serialize<S: Serializer>(
        value: &Option<ActivationUsage>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(|usage| Mirror {
                tokens_in: usage.tokens_in,
                tokens_out: usage.tokens_out,
                cached_tokens: usage.cached_tokens,
                cost_usd: usage.cost_usd,
                latency_ms: usage.latency_ms,
                provider: usage.provider.clone(),
                model: usage.model.clone(),
            })
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ActivationUsage>, D::Error> {
        Ok(
            Option::<Mirror>::deserialize(deserializer)?.map(|usage| ActivationUsage {
                tokens_in: usage.tokens_in,
                tokens_out: usage.tokens_out,
                cached_tokens: usage.cached_tokens,
                cost_usd: usage.cost_usd,
                latency_ms: usage.latency_ms,
                provider: usage.provider,
                model: usage.model,
            }),
        )
    }
}

pub(crate) mod evidence {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Mirror {
        evidence_type: String,
        payload: Option<payload_value::Mirror>,
        #[serde(with = "b64")]
        sha256: Vec<u8>,
    }

    pub fn serialize<S: Serializer>(
        value: &[ActivationEvidence],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|item| Mirror {
                evidence_type: item.evidence_type.clone(),
                payload: item.payload.as_ref().and_then(payload_value::to_mirror),
                sha256: item.sha256.clone(),
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ActivationEvidence>, D::Error> {
        Ok(Vec::<Mirror>::deserialize(deserializer)?
            .into_iter()
            .map(|item| ActivationEvidence {
                evidence_type: item.evidence_type,
                payload: item.payload.map(payload_value::from_mirror),
                sha256: item.sha256,
            })
            .collect())
    }
}

pub(crate) mod mutations {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Mirror {
        operation_id: String,
        entity_type: String,
        entity_key: String,
        scope: String,
        scope_id: String,
        #[serde(with = "b64")]
        data: Vec<u8>,
        data_type: String,
        expected_version: i64,
    }

    pub fn serialize<S: Serializer>(
        value: &[ActivationStateMutation],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|item| Mirror {
                operation_id: item.operation_id.clone(),
                entity_type: item.entity_type.clone(),
                entity_key: item.entity_key.clone(),
                scope: item.scope.clone(),
                scope_id: item.scope_id.clone(),
                data: item.data.clone(),
                data_type: item.data_type.clone(),
                expected_version: item.expected_version,
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ActivationStateMutation>, D::Error> {
        Ok(Vec::<Mirror>::deserialize(deserializer)?
            .into_iter()
            .map(|item| ActivationStateMutation {
                operation_id: item.operation_id,
                entity_type: item.entity_type,
                entity_key: item.entity_key,
                scope: item.scope,
                scope_id: item.scope_id,
                data: item.data,
                data_type: item.data_type,
                expected_version: item.expected_version,
            })
            .collect())
    }
}

pub(crate) mod intents {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Mirror {
        intent_id: String,
        kind: String,
        idempotency_key: String,
        payload: Option<payload_value::Mirror>,
    }

    pub fn serialize<S: Serializer>(
        value: &[ActivationOutboxIntent],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|item| Mirror {
                intent_id: item.intent_id.clone(),
                kind: item.kind.clone(),
                idempotency_key: item.idempotency_key.clone(),
                payload: item.payload.as_ref().and_then(payload_value::to_mirror),
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ActivationOutboxIntent>, D::Error> {
        Ok(Vec::<Mirror>::deserialize(deserializer)?
            .into_iter()
            .map(|item| ActivationOutboxIntent {
                intent_id: item.intent_id,
                kind: item.kind,
                idempotency_key: item.idempotency_key,
                payload: item.payload.map(payload_value::from_mirror),
            })
            .collect())
    }
}

pub(crate) mod child {
    use super::*;
    use serde::{Deserializer, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Mirror {
        child_key: String,
        child_run_id: String,
        child_session_id: String,
        #[serde(with = "b64")]
        child_definition_digest: Vec<u8>,
        join_policy: i32,
    }

    pub fn serialize<S: Serializer>(
        value: &Option<ChildActivationLinkage>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(|child| Mirror {
                child_key: child.child_key.clone(),
                child_run_id: child.child_run_id.clone(),
                child_session_id: child.child_session_id.clone(),
                child_definition_digest: child.child_definition_digest.clone(),
                join_policy: child.join_policy,
            })
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<ChildActivationLinkage>, D::Error> {
        Ok(
            Option::<Mirror>::deserialize(deserializer)?.map(|child| ChildActivationLinkage {
                child_key: child.child_key,
                child_run_id: child.child_run_id,
                child_session_id: child.child_session_id,
                child_definition_digest: child.child_definition_digest,
                join_policy: child.join_policy,
            }),
        )
    }
}
