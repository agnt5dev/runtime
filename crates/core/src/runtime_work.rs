//! Journal-backed work for built-in controllers. The journal owns every
//! transition; the partition work index is disposable and rebuilt by replay.
use crate::{
    error::{EngineError, EngineResult},
    types::Record,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub const ENTITY_TYPE: &str = "__agnt5_runtime_work_v1";
pub const SCOPE: &str = "run";
const WORK_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x78b5_5270_89ef_5b8b_a261_f070_152f_67c0);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    EvalBatch,
    EvalItem,
    EvalCancel,
    /// Observed-run sampling decision and recoverable score-only handoff.
    EvalOnline,
    /// At-least-once publication of a failing online eval result to the
    /// control plane. Never gates the result it reports on.
    EvalFailurePublish,
    EventConfiguration,
    EventMatch,
    EventDelivery,
    Callback,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkLease {
    pub owner: String,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeWork {
    pub partition_id: u32,
    pub project_id: String,
    /// All transitions route to this run's partition. Event work uses the
    /// source run; an offline controller uses its immutable controller run ID.
    pub run_id: String,
    pub work_id: String,
    pub kind: WorkKind,
    pub version: i64,
    /// Next attempt for nonterminal work; terminal event deliveries use this
    /// for their retained-history deadline (zero means no expiry policy).
    pub due_at_ms: i64,
    pub lease: Option<WorkLease>,
    pub terminal: bool,
    pub payload: Value,
}

impl RuntimeWork {
    /// Domain-separated child IDs survive retries and do not depend on time,
    /// worker registrations, deployment routing, or controller process state.
    pub fn stable_id(parts: &[&str]) -> String {
        let mut bytes = Vec::new();
        for part in parts {
            bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
            bytes.extend_from_slice(part.as_bytes());
        }
        uuid::Uuid::new_v5(&WORK_NAMESPACE, &bytes).to_string()
    }

    pub fn validate(&self) -> EngineResult<()> {
        if self.kind == WorkKind::EventConfiguration {
            let config: crate::runtime_events::Configuration =
                serde_json::from_value(self.payload.clone()).map_err(|error| {
                    invalid(&format!("invalid runtime event configuration: {error}"))
                })?;
            config.validate()?;
            if config.project_id != self.project_id || self.work_id != self.run_id {
                return Err(invalid("runtime event configuration scope mismatch"));
            }
        }
        for value in [&self.project_id, &self.run_id, &self.work_id] {
            if value.is_empty() || value.len() > 256 || value.contains('\0') {
                return Err(invalid("invalid runtime work identity"));
            }
        }
        if self.partition_id > u16::MAX as u32 || self.version < 0 || self.due_at_ms < 0 {
            return Err(invalid("invalid runtime work position"));
        }
        if let Some(lease) = &self.lease {
            if self.terminal
                || lease.owner.is_empty()
                || lease.owner.len() > 256
                || lease.expires_at_ms <= 0
            {
                return Err(invalid("invalid runtime work lease"));
            }
        }
        Ok(())
    }

    pub fn ready(&self, now_ms: i64) -> bool {
        !self.terminal
            && self.due_at_ms <= now_ms
            && self
                .lease
                .as_ref()
                .is_none_or(|lease| lease.expires_at_ms <= now_ms)
    }

    /// This is only a proposed claim. The caller owns nothing until the
    /// returned snapshot is acknowledged by the journal's version CAS.
    pub fn claim(&self, owner: &str, now_ms: i64, lease_ms: i64) -> EngineResult<Self> {
        if !self.ready(now_ms) || !(1..=300_000).contains(&lease_ms) {
            return Err(invalid("runtime work is not claimable"));
        }
        let mut next = self.clone();
        next.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| invalid("runtime work version exhausted"))?;
        next.lease = Some(WorkLease {
            owner: owner.into(),
            expires_at_ms: now_ms
                .checked_add(lease_ms)
                .ok_or_else(|| invalid("runtime work deadline overflow"))?,
        });
        next.validate()?;
        Ok(next)
    }

    pub fn held_by(&self, owner: &str, now_ms: i64) -> bool {
        !self.terminal
            && self
                .lease
                .as_ref()
                .is_some_and(|lease| lease.owner == owner && now_ms < lease.expires_at_ms)
    }

    /// Progress and completion consume the exact claim version. A successor
    /// owner's claim makes an old owner's write conflict in the journal.
    pub fn advance(
        &self,
        owner: &str,
        now_ms: i64,
        payload: Value,
        due_at_ms: i64,
        terminal: bool,
    ) -> EngineResult<Self> {
        if !self.held_by(owner, now_ms) {
            return Err(invalid("runtime work lease is not current"));
        }
        let mut next = self.clone();
        next.version = self
            .version
            .checked_add(1)
            .ok_or_else(|| invalid("runtime work version exhausted"))?;
        next.payload = payload;
        next.due_at_ms = due_at_ms;
        next.terminal = terminal;
        next.lease = None;
        next.validate()?;
        Ok(next)
    }

    pub fn to_record(&self) -> EngineResult<Record> {
        self.validate()?;
        if self.version == 0 {
            return Err(invalid("runtime work journal version must be positive"));
        }
        let data = serde_json::to_vec(self).map_err(|e| invalid(&e.to_string()))?;
        // Generic state idempotency keys are operation IDs. Bind this ID to
        // the complete proposed snapshot, so competing writes at one version
        // cannot be mistaken for exact lost-response retries.
        let operation_id = uuid::Uuid::new_v5(&WORK_NAMESPACE, &data).to_string();
        Ok(Record::from_proto(engine_proto::Record {
            project_id: self.project_id.clone(),
            run_id: self.run_id.clone(),
            event_type: "workflow.state.changed".into(),
            data,
            data_type: "json".into(),
            metadata: HashMap::from([
                ("entity_type".into(), ENTITY_TYPE.into()),
                ("entity_key".into(), self.work_id.clone()),
                ("scope".into(), SCOPE.into()),
                ("scope_id".into(), self.run_id.clone()),
                ("expected_version".into(), (self.version - 1).to_string()),
                ("new_version".into(), self.version.to_string()),
                ("operation_id".into(), operation_id),
            ]),
            ..Default::default()
        }))
    }

    pub fn from_record(record: &Record) -> EngineResult<Option<Self>> {
        if !is_work_record(record) {
            return Ok(None);
        }
        let work: Self = serde_json::from_slice(&record.data)
            .map_err(|e| invalid(&format!("invalid runtime work: {e}")))?;
        work.validate()?;
        let canonical = work.to_record()?;
        if record.project_id != work.project_id
            || record.run_id != work.run_id
            || canonical
                .metadata
                .iter()
                .any(|(key, value)| record.metadata.get(key) != Some(value))
        {
            return Err(invalid(
                "runtime work envelope disagrees with journal identity",
            ));
        }
        Ok(Some(work))
    }
}

pub fn is_work_record(record: &Record) -> bool {
    record.event_type == "workflow.state.changed"
        && record
            .metadata
            .get("entity_type")
            .is_some_and(|kind| kind == ENTITY_TYPE)
}

fn invalid(message: &str) -> EngineError {
    EngineError::InvalidArgument(message.into())
}

#[derive(Clone, Debug)]
pub struct WorkPage {
    pub items: Vec<RuntimeWork>,
    /// Opaque, exclusive adapter cursor. A full walk resets this to None.
    pub next_cursor: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn work() -> RuntimeWork {
        RuntimeWork {
            partition_id: 0,
            project_id: "project".into(),
            run_id: "run".into(),
            work_id: RuntimeWork::stable_id(&["eval", "batch", "item"]),
            kind: WorkKind::EvalItem,
            version: 1,
            due_at_ms: 0,
            lease: None,
            terminal: false,
            payload: serde_json::json!({"input":42}),
        }
    }
    #[test]
    fn claim_and_progress_require_current_lease_and_monotonic_version() {
        let first = work().claim("a", 100, 50).unwrap();
        assert!(!first.ready(149));
        assert!(first.advance("b", 120, Value::Null, 0, true).is_err());
        assert!(first.advance("a", 150, Value::Null, 0, true).is_err());
        let successor = first.claim("b", 150, 50).unwrap();
        assert_eq!(successor.version, 3);
        let done = successor.advance("b", 151, Value::Null, 0, true).unwrap();
        assert_eq!(done.version, 4);
        assert!(!done.ready(1000));
        assert!(done.lease.is_none());
    }
    #[test]
    fn envelope_cannot_change_scope_or_bypass_version_claim() {
        let value = work();
        let record = value.to_record().unwrap();
        assert_eq!(RuntimeWork::from_record(&record).unwrap(), Some(value));
        for key in [
            "entity_key",
            "scope_id",
            "expected_version",
            "new_version",
            "operation_id",
        ] {
            let mut changed = record.clone();
            changed.metadata.insert(key.into(), "different".into());
            assert!(RuntimeWork::from_record(&changed).is_err());
        }
        let mut changed = record;
        changed.project_id = "other".into();
        assert!(RuntimeWork::from_record(&changed).is_err());
    }
    #[test]
    fn stable_ids_preserve_component_boundaries() {
        assert_ne!(
            RuntimeWork::stable_id(&["ab", "c"]),
            RuntimeWork::stable_id(&["a", "bc"])
        );
        assert_eq!(
            RuntimeWork::stable_id(&["item", "1"]),
            RuntimeWork::stable_id(&["item", "1"])
        );
    }
}
