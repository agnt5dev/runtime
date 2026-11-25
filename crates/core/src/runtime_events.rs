//! Frozen, partition-applied configuration and source identities for runtime
//! event delivery. These values are journal data, never a process-local cache.
use crate::{
    error::{EngineError, EngineResult},
    routing::partition_for_run,
    runtime_work::RuntimeWork,
};
use engine_proto::Run;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const MAX_SUBSCRIPTIONS: usize = 100;
pub const MAX_CONFIGURATION_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDestination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookDestination>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub online_eval: Option<serde_json::Value>,
    #[serde(default)]
    pub deployment_id: String,
    pub component_type: String,
    #[serde(default)]
    pub component_name: String,
}

/// Only a reference is journalled; signing material stays in the runtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookDestination {
    pub url: String,
    pub signing_secret_ref: String,
    pub max_attempts: u32,
}

pub fn valid_secret_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSelector {
    pub event_types: Vec<String>,
    pub environment_id: Option<String>,
    pub deployment_id: Option<String>,
    pub component_name: Option<String>,
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub subscription_id: String,
    pub revision: u64,
    pub enabled: bool,
    pub source: SourceSelector,
    pub destination: NativeDestination,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Configuration {
    /// Explicit installation/lineage identity, preserved through restart and
    /// replica handoff. It is not a node ID or a wall-clock timestamp.
    pub namespace: String,
    pub project_id: String,
    pub workspace_id: String,
    pub desired_revision: u64,
    pub subscriptions: Vec<Subscription>,
    /// A receipt from the serving owner. Management checks the current owner
    /// epoch before calling this revision active; an old receipt is insufficient.
    pub applied_epoch: Option<u64>,
    pub applied_owner: Option<String>,
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0')
}
fn invalid(message: &str) -> EngineError {
    EngineError::InvalidArgument(message.into())
}

impl Configuration {
    pub fn validate(&self) -> EngineResult<()> {
        if ![&self.namespace, &self.project_id, &self.workspace_id]
            .into_iter()
            .all(|value| valid_id(value))
            || self.desired_revision == 0
            || self.subscriptions.len() > MAX_SUBSCRIPTIONS
        {
            return Err(invalid(
                "invalid runtime event configuration identity or limits",
            ));
        }
        if self.applied_epoch.is_some() != self.applied_owner.is_some()
            || self
                .applied_owner
                .as_deref()
                .is_some_and(|owner| !valid_id(owner))
        {
            return Err(invalid("incomplete runtime event application receipt"));
        }
        let mut ids = HashSet::new();
        for subscription in &self.subscriptions {
            if !valid_id(&subscription.subscription_id)
                || subscription.revision == 0
                || !ids.insert(&subscription.subscription_id)
                || subscription.source.event_types.is_empty()
                || subscription.source.event_types.len() > 4
                || subscription.source.event_types.iter().any(|kind| {
                    !matches!(
                        kind.as_str(),
                        "run.completed" | "run.failed" | "run.cancelled" | MESSAGE_EVENT_TYPE
                    )
                })
            {
                return Err(invalid("invalid runtime event subscription"));
            }
            for selector in [
                &subscription.source.environment_id,
                &subscription.source.deployment_id,
                &subscription.source.component_name,
                &subscription.source.tenant_id,
            ] {
                if selector.as_deref().is_some_and(|value| !valid_id(value)) {
                    return Err(invalid("invalid runtime event selector"));
                }
            }
            let target = &subscription.destination;
            if (target.component_type == "webhook") != target.webhook.is_some() {
                return Err(invalid(
                    "webhook destinations require a webhook configuration",
                ));
            }
            if let Some(webhook) = &target.webhook {
                if target.online_eval.is_some()
                    || !target.deployment_id.is_empty()
                    || !target.component_name.is_empty()
                    || webhook.url.len() > 2048
                    || !webhook.url.starts_with("https://")
                    || !valid_secret_reference(&webhook.signing_secret_ref)
                    || !(1..=100).contains(&webhook.max_attempts)
                {
                    return Err(invalid("invalid webhook destination"));
                }
                continue;
            }
            if (subscription.destination.component_type == "online_eval")
                != subscription.destination.online_eval.is_some()
                || (subscription.destination.online_eval.is_some()
                    && subscription.source.event_types != ["run.completed"])
            {
                return Err(invalid(
                    "online eval destinations require a policy and run.completed only",
                ));
            }
            if !valid_id(&subscription.destination.deployment_id)
                || !valid_id(&subscription.destination.component_name)
                || !matches!(
                    subscription.destination.component_type.as_str(),
                    "function" | "workflow" | "agent" | "online_eval"
                )
            {
                return Err(invalid("invalid native event destination"));
            }
        }
        if serde_json::to_vec(self)
            .map_err(|error| invalid(&error.to_string()))?
            .len()
            > MAX_CONFIGURATION_BYTES
        {
            return Err(invalid("runtime event configuration exceeds 128 KiB"));
        }
        Ok(())
    }

    /// The receipt is not part of desired intent. Reapplying an identical
    /// management revision must not conflict merely because it became active.
    pub fn same_intent(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && self.project_id == other.project_id
            && self.workspace_id == other.workspace_id
            && self.desired_revision == other.desired_revision
            && self.subscriptions == other.subscriptions
    }
}

/// Deterministic synthetic run in the required partition. Configuration is
/// partition-local so its append order is also its source activation boundary.
pub fn configuration_run_id(
    project_id: &str,
    partition_id: u32,
    count: usize,
) -> EngineResult<String> {
    if count == 0 || count > 256 || partition_id as usize >= count {
        return Err(invalid("unsupported runtime event partition topology"));
    }
    for salt in 0..(count * 256) {
        let candidate = RuntimeWork::stable_id(&[
            "event-configuration",
            project_id,
            &partition_id.to_string(),
            &salt.to_string(),
        ]);
        if partition_for_run(&candidate, count) == partition_id as usize {
            return Ok(candidate);
        }
    }
    Err(invalid("could not route runtime event configuration"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub schema_version: String,
    pub event_id: String,
    pub namespace: String,
    pub project_id: String,
    pub partition_id: u32,
    pub terminal_offset: u64,
    pub run_id: String,
    pub event_type: String,
    pub occurred_at_ms: i64,
    pub deployment_id: String,
    pub environment_id: Option<String>,
    pub component_type: String,
    pub component_name: String,
    pub tenant_id: String,
    /// What an agent instance sent its user, on a `message` event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<EventMessage>,
}

/// The event type of a message an agent instance sent its user.
pub const MESSAGE_EVENT_TYPE: &str = "message";

/// The body of a `message` event: what an agent instance sent, and to whom.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventMessage {
    /// Stable for the sending call: a retried or replayed `send` carries the
    /// same id, so the app can drop a duplicate.
    pub message_id: String,
    /// The instance's inbox key, the customer's user id.
    pub user_id: String,
    pub conversation_id: String,
    pub content: serde_json::Value,
    /// The instance run that sent it.
    pub sender_run_id: String,
}

/// Whether a Run belongs to another Run: it names a parent, or a root other
/// than itself. This is the one definition of "not a root application Run"
/// shared by event subscriptions and deployment verification; an explicitly
/// empty root_run_id counts as naming another root.
pub fn is_descendant_run(run: &Run) -> bool {
    run.metadata
        .get("parent_run_id")
        .is_some_and(|value| !value.is_empty())
        || run
            .metadata
            .get("root_run_id")
            .is_some_and(|value| value != &run.run_id)
}

impl Event {
    /// Called only for an accepted final transition. Retryable failures are
    /// excluded using the canonical projected status, not just the event name.
    pub fn from_terminal(
        config: &Configuration,
        run: &Run,
        event_type: &str,
        partition_id: u32,
        terminal_offset: u64,
        timestamp_ns: i64,
    ) -> Option<Self> {
        let expected_status = match event_type {
            "run.completed" => "completed",
            "run.failed" => "failed",
            "run.cancelled" => "cancelled",
            _ => return None,
        };
        if run.status != expected_status
            || run.project_id != config.project_id
            || run.metadata.get("workspace_id").map(String::as_str)
                != Some(config.workspace_id.as_str())
            || is_descendant_run(run)
            || run.metadata.contains_key("agnt5_eval_role")
            || run.metadata.contains_key("agnt5_event_role")
            || run.component_type == "scorer"
        {
            return None;
        }
        Some(Self {
            schema_version: "agnt5.runtime.event.v1".into(),
            event_id: RuntimeWork::stable_id(&[
                "runtime-event",
                &config.namespace,
                &config.project_id,
                &partition_id.to_string(),
                &terminal_offset.to_string(),
            ]),
            namespace: config.namespace.clone(),
            project_id: config.project_id.clone(),
            partition_id,
            terminal_offset,
            run_id: run.run_id.clone(),
            event_type: event_type.into(),
            occurred_at_ms: (timestamp_ns / 1_000_000).max(0),
            deployment_id: run.deployment_id.clone(),
            environment_id: run.metadata.get("environment_id").cloned(),
            component_type: run.component_type.clone(),
            component_name: run.component_name.clone(),
            tenant_id: run
                .metadata
                .get("tenant_id")
                .cloned()
                .unwrap_or_else(|| run.project_id.clone()),
            message: None,
        })
    }

    /// The `message` event for an `instance.message` record on an instance's
    /// inbox. Its id comes from the message id, so the same message is one
    /// event however often it is appended or replayed.
    pub fn from_message(
        config: &Configuration,
        record: &crate::types::Record,
        partition_id: u32,
        offset: u64,
    ) -> Option<Self> {
        if record.event_type != crate::instance::INSTANCE_MESSAGE_EVENT
            || record.project_id != config.project_id
            || record.metadata.get("workspace_id").map(String::as_str)
                != Some(config.workspace_id.as_str())
        {
            return None;
        }
        let identity = crate::instance_placement::parse_inbox_run_id(&record.run_id)?;
        let body: crate::instance::StoredMessage = serde_json::from_slice(&record.data).ok()?;
        let meta = |key: &str| record.metadata.get(key).cloned().unwrap_or_default();
        Some(Self {
            schema_version: "agnt5.runtime.event.v1".into(),
            event_id: RuntimeWork::stable_id(&[
                "runtime-message",
                &config.namespace,
                &config.project_id,
                &body.message_id,
            ]),
            namespace: config.namespace.clone(),
            project_id: config.project_id.clone(),
            partition_id,
            terminal_offset: offset,
            run_id: record.run_id.clone(),
            event_type: MESSAGE_EVENT_TYPE.into(),
            occurred_at_ms: (record.timestamp_ns / 1_000_000).max(0),
            deployment_id: meta("deployment_id"),
            environment_id: Some(identity.environment_id().to_string()),
            component_type: meta("component_type"),
            component_name: identity.agent().to_string(),
            tenant_id: record
                .metadata
                .get("tenant_id")
                .cloned()
                .unwrap_or_else(|| record.project_id.clone()),
            message: Some(EventMessage {
                message_id: body.message_id,
                user_id: identity.inbox_key().to_string(),
                conversation_id: body.conversation_id,
                content: body.content,
                sender_run_id: body.sender_run_id,
            }),
        })
    }

    pub fn matches(&self, subscription: &Subscription) -> bool {
        let source = &subscription.source;
        subscription.enabled
            && source.event_types.contains(&self.event_type)
            && source
                .environment_id
                .as_ref()
                .is_none_or(|value| self.environment_id.as_ref() == Some(value))
            && source
                .deployment_id
                .as_ref()
                .is_none_or(|value| value == &self.deployment_id)
            && source
                .component_name
                .as_ref()
                .is_none_or(|value| value == &self.component_name)
            && source
                .tenant_id
                .as_ref()
                .is_none_or(|value| value == &self.tenant_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchIntent {
    pub event: Event,
    pub configuration: Configuration,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Configuration {
        Configuration {
            namespace: "installation".into(),
            project_id: "project".into(),
            workspace_id: "workspace".into(),
            desired_revision: 1,
            subscriptions: vec![],
            applied_epoch: None,
            applied_owner: None,
        }
    }
    fn run() -> Run {
        Run {
            project_id: "project".into(),
            run_id: "run".into(),
            status: "completed".into(),
            component_type: "function".into(),
            metadata: [("workspace_id".into(), "workspace".into())].into(),
            ..Default::default()
        }
    }
    // An instance run's `send` becomes one message on its inbox, and that one
    // message event, whose id follows the message, for its subscriptions.
    #[test]
    fn a_runs_send_becomes_one_message_event_on_its_instance() {
        use crate::instance::{message_id, message_record, INSTANCE_MESSAGE_REQUESTED_EVENT};
        use crate::instance_placement::{inbox_run_id, instance_run_id, InstanceIdentity};
        let identity = InstanceIdentity::new("project", "env-1", "support", "u_1", 1).unwrap();
        let inbox = inbox_run_id(&identity, 0, 1).unwrap();
        let run_id = instance_run_id(&identity, 0, 0, 1).unwrap();
        let request = crate::types::Record {
            offset: crate::types::Offset::ZERO,
            project_id: "project".into(),
            run_id: run_id.clone(),
            event_type: INSTANCE_MESSAGE_REQUESTED_EVENT.into(),
            data: serde_json::to_vec(&serde_json::json!({
                "message_key": "send-1.ab12",
                "conversation_id": "pr-42",
                "content": {"text": "Your PR has a review"},
                "event_id": "envelope-fields-are-ignored",
            }))
            .unwrap()
            .into(),
            timestamp_ns: 5_000_000,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: [("deployment_id".to_string(), "forged".to_string())].into(),
            data_type: "json".into(),
            data_checksum: None,
            data_compressed: false,
        };
        let run_metadata = std::collections::HashMap::from([
            (
                crate::instance::META_INSTANCE_INBOX_RUN_ID.to_string(),
                inbox.clone(),
            ),
            ("deployment_id".to_string(), "dep-1".to_string()),
            ("workspace_id".to_string(), "workspace".to_string()),
        ]);
        let message = message_record(&request, &run_metadata).unwrap();
        assert_eq!(message.run_id, inbox);
        crate::journal_contract::validate_append_records(std::slice::from_ref(&message)).unwrap();
        // A replayed send has the same idempotency slot and fingerprint.
        let again = message_record(&request, &run_metadata).unwrap();
        assert_eq!(
            crate::journal_contract::record_idempotency_key(&message),
            crate::journal_contract::record_idempotency_key(&again)
        );
        assert!(crate::journal_contract::record_idempotency_key(&message).is_some());
        assert_eq!(
            crate::journal_contract::record_idempotency_validation_fingerprint(&message),
            crate::journal_contract::record_idempotency_validation_fingerprint(&again)
        );

        let event = Event::from_message(&config(), &message, 0, 7).unwrap();
        assert_eq!(event.event_type, MESSAGE_EVENT_TYPE);
        assert_eq!(event.run_id, inbox);
        assert_eq!(event.deployment_id, "dep-1");
        assert_eq!(event.component_name, "support");
        let body = event.message.as_ref().unwrap();
        assert_eq!(body.message_id, message_id(&run_id, "send-1.ab12"));
        assert_eq!(body.user_id, "u_1");
        assert_eq!(body.conversation_id, "pr-42");
        assert_eq!(body.sender_run_id, run_id);
        // The same message at another offset is the same event.
        assert_eq!(
            Event::from_message(&config(), &message, 0, 99)
                .unwrap()
                .event_id,
            event.event_id
        );
        // A subscription may select messages.
        let mut configured = config();
        configured.subscriptions.push(Subscription {
            subscription_id: "app-webhook".into(),
            revision: 1,
            enabled: true,
            source: SourceSelector {
                event_types: vec![MESSAGE_EVENT_TYPE.into()],
                environment_id: None,
                deployment_id: None,
                component_name: None,
                tenant_id: None,
            },
            destination: NativeDestination {
                component_type: "webhook".into(),
                deployment_id: String::new(),
                component_name: String::new(),
                online_eval: None,
                webhook: Some(WebhookDestination {
                    url: "https://app.example.com/agnt5".into(),
                    signing_secret_ref: "app-webhook-key".into(),
                    max_attempts: 5,
                }),
            },
        });
        configured.validate().unwrap();
        assert!(event.matches(&configured.subscriptions[0]));
        // Another workspace's message is not this configuration's.
        let mut other = message.clone();
        other
            .metadata
            .insert("workspace_id".into(), "elsewhere".into());
        assert!(Event::from_message(&config(), &other, 0, 7).is_none());
    }

    #[test]
    fn identity_survives_owner_changes_and_configuration_edits() {
        let first = Event::from_terminal(&config(), &run(), "run.completed", 3, 10, 1).unwrap();
        let mut next = config();
        next.desired_revision = 2;
        next.applied_epoch = Some(42);
        next.applied_owner = Some("successor".into());
        let second = Event::from_terminal(&next, &run(), "run.completed", 3, 10, 1).unwrap();
        assert_eq!(first.event_id, second.event_id);
        assert_ne!(
            first.event_id,
            Event::from_terminal(&next, &run(), "run.completed", 4, 10, 1)
                .unwrap()
                .event_id
        );
        next.namespace = "other-installation".into();
        assert_ne!(
            first.event_id,
            Event::from_terminal(&next, &run(), "run.completed", 3, 10, 1)
                .unwrap()
                .event_id
        );
    }
    #[test]
    fn only_final_root_application_outcomes_are_sources() {
        let mut source = run();
        source.status = "retry_delayed".into();
        assert!(Event::from_terminal(&config(), &source, "run.failed", 0, 1, 0).is_none());
        for key in [
            "parent_run_id",
            "root_run_id",
            "agnt5_eval_role",
            "agnt5_event_role",
            "workspace_id",
        ] {
            let mut source = run();
            source.metadata.insert(key.into(), "other".into());
            assert!(
                Event::from_terminal(&config(), &source, "run.completed", 0, 1, 0).is_none(),
                "{key}"
            );
        }
    }
    #[test]
    fn each_configuration_routes_to_its_source_partition() {
        for partition in 0..8 {
            let id = configuration_run_id("project", partition, 8).unwrap();
            assert_eq!(partition_for_run(&id, 8), partition as usize);
            assert_eq!(id, configuration_run_id("project", partition, 8).unwrap());
        }
        assert!(configuration_run_id("project", 8, 8).is_err());
    }
}
