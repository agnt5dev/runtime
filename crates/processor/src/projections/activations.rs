//! Derived Durable Activation V1 projection.
//!
//! This read model is rebuildable acceleration only. The Engine service folds
//! the canonical journal records directly for every correctness decision.

use engine_core::activation::{
    self, apply_journal_record, decode_journal_record, encode_journal_record, from_engine_record,
    snapshot_record, state_from_snapshot, Event,
};
use engine_core::error::{EngineError, EngineResult};
use engine_core::storage_contract::ProjectionMutation as StoreOp;
use engine_core::types::Record;
use engine_proto::activation_payload::Value;
use prost::Message;

use super::ProjectionResult;

pub fn activation_id(record: &Record) -> Option<&str> {
    activation::activation_record_identity(record).map(|(activation_id, _, _)| activation_id)
}

/// Decode a persisted activation projection row (a canonical snapshot record).
pub fn state_from_projection(bytes: &[u8]) -> EngineResult<activation::ActivationState> {
    let record = decode_journal_record(bytes).map_err(|error| {
        EngineError::Serialization(format!("decode activation projection snapshot: {error}"))
    })?;
    match record.event {
        Some(Event::Snapshot(snapshot)) => {
            state_from_snapshot(&snapshot).map_err(EngineError::from)
        }
        _ => Err(EngineError::Serialization(
            "activation projection row is not a snapshot record".to_string(),
        )),
    }
}

pub fn project(record: &Record, existing: Option<&[u8]>) -> EngineResult<ProjectionResult> {
    let Some(canonical) = from_engine_record(record).map_err(EngineError::from)? else {
        return Ok(ProjectionResult { ops: Vec::new() });
    };
    let current = existing.map(state_from_projection).transpose()?;
    // A processor may deliberately rewind its journal checkpoint while the
    // atomically committed activation read model is still ahead (for example,
    // when a replica rejoins after an ownership handoff). Older activation
    // records are already represented by that snapshot and must be replay
    // no-ops; feeding them back through the strict aggregate fold would turn a
    // completed activation into an illegal transition during startup.
    if current
        .as_ref()
        .is_some_and(|state| record.offset.0 <= state.accepted_offset.0)
    {
        return Ok(ProjectionResult { ops: Vec::new() });
    }
    let state = match apply_journal_record(current.clone(), &canonical, record.offset) {
        Ok(state) => state,
        Err(error) => {
            let (partition_id, record_local_offset) =
                engine_core::partition_offset::decode_offset(record.offset);
            let partition_id = partition_id as u32;
            let existing_accepted_offset = current
                .as_ref()
                .map(|state| engine_core::partition_offset::decode_offset(state.accepted_offset).1);
            tracing::error!(
                event_name = "runtime.state_divergence",
                divergence_type = "activation_state_transition_conflict",
                component = "activation_projection",
                partition_id,
                record_offset = record.offset.0,
                record_local_offset,
                incoming_attempt = activation::activation_record_identity(record)
                    .map(|(_, attempt, _)| attempt),
                existing_status = current.as_ref().map(|state| state.status.as_str_name()),
                existing_attempt = current
                    .as_ref()
                    .map(|state| state.current_attempt.attempt),
                existing_accepted_offset = current
                    .as_ref()
                    .map(|state| state.accepted_offset.0),
                projection_checkpoint = existing_accepted_offset,
                differing_fields = "activation_status,attempt,accepted_offset",
                error = %error,
                "Activation projection rejected canonical journal record"
            );
            return Err(EngineError::from(error));
        }
    };
    let snapshot = encode_journal_record(&snapshot_record(&state));
    let version = i64::try_from(record.offset.0).map_err(|_| {
        EngineError::Storage(format!(
            "activation projection offset {} exceeds signed storage version",
            record.offset.0
        ))
    })?;
    let mut ops = vec![StoreOp::PutActivation {
        activation_id: state.identity.activation_id.clone(),
        data: snapshot,
        version,
    }];

    // A compaction snapshot refreshes only the activation aggregate. Reusing
    // its embedded completion as a new state/outbox transition could replay an
    // older entity version after another activation has advanced that entity.
    // Derived effects are emitted only by the accepted completion record.
    if let Some(Event::Completed(completion)) = canonical.event.as_ref() {
        // Every kind memoizes under its stable key: the record is the step
        // boundary the SDK replays through FindByStepKey and the cache.
        if let Some(payload) = &completion.output {
            let data = match payload.value.as_ref() {
                Some(Value::InlineData(data)) => data.clone(),
                Some(Value::Reference(_)) => payload.encode_to_vec(),
                None => Vec::new(),
            };
            ops.push(StoreOp::PutStepMemo {
                step_key: state.identity.stable_key.clone(),
                data,
            });
        }
        for mutation in &completion.state_mutations {
            let version = mutation.expected_version.checked_add(1).ok_or_else(|| {
                EngineError::InvalidArgument(format!(
                    "activation state mutation {:?} version overflow",
                    mutation.operation_id
                ))
            })?;
            ops.push(StoreOp::PutEntityState {
                entity_type: mutation.entity_type.clone(),
                entity_key: mutation.entity_key.clone(),
                scope: mutation.scope.clone(),
                scope_id: mutation.scope_id.clone(),
                data: mutation.data.clone(),
                version,
            });
        }
        for intent in &completion.outbox_intents {
            ops.push(StoreOp::PutActivationOutbox {
                activation_id: state.identity.activation_id.clone(),
                attempt: completion.attempt,
                intent_id: intent.intent_id.clone(),
                data: intent.encode_to_vec(),
            });
        }
    }

    Ok(ProjectionResult { ops })
}

#[cfg(test)]
mod tests {
    use engine_core::activation::{
        decide_begin, decide_complete, payload_digest, to_engine_record, BeginActivationCommand,
        CompleteActivationCommand,
    };
    use engine_core::types::Offset;
    use engine_proto::activation_payload::Value;
    use engine_proto::{
        ActivationKind, ActivationOutboxIntent, ActivationPayload, ActivationRecoveryPolicy,
        ActivationStateMutation, BeginActivationRequest, ChildActivationLinkage, ChildJoinPolicy,
        CompleteActivationRequest,
    };

    use super::*;

    #[test]
    fn accepted_completion_projects_result_state_and_outbox_together() {
        let begin = BeginActivationCommand {
            request: BeginActivationRequest {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                parent_activation_id: String::new(),
                kind: ActivationKind::Step as i32,
                stable_key: "step/load".to_string(),
                input_digest: vec![1; 32],
                definition_digest: vec![2; 32],
                recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                worker_session_id: "worker-1".to_string(),
                run_authority: b"run-auth".to_vec(),
                lease_authority: b"lease-auth".to_vec(),
                child: None,

                display_name: String::new(),
                input_data: Vec::new(),
                display_parent_correlation_id: String::new(),
            },
            issued_fence_token: b"fence-1".to_vec(),
        };
        let started = decide_begin(None, &begin).unwrap();
        let mut started_record = to_engine_record(&started.records[0], 1).unwrap();
        started_record.offset = Offset(1);
        let started_projection = project(&started_record, None).unwrap();
        let snapshot = started_projection
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutActivation { data, .. } => Some(data.clone()),
                _ => None,
            })
            .unwrap();
        let active = state_from_projection(snapshot.as_slice()).unwrap();
        let output = ActivationPayload {
            value: Some(Value::InlineData(b"result".to_vec())),
        };
        let completion = CompleteActivationCommand {
            request: CompleteActivationRequest {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: b"fence-1".to_vec(),
                output_digest: payload_digest(&output).unwrap(),
                output: Some(output),
                state_mutations: vec![ActivationStateMutation {
                    operation_id: "state-1".to_string(),
                    entity_type: "agent".to_string(),
                    entity_key: "agent-1".to_string(),
                    scope: "run".to_string(),
                    scope_id: "run-1".to_string(),
                    data: b"state".to_vec(),
                    data_type: "json".to_string(),
                    expected_version: 4,
                }],
                outbox_intents: vec![ActivationOutboxIntent {
                    intent_id: "effect-1".to_string(),
                    kind: "http".to_string(),
                    idempotency_key: "downstream-1".to_string(),
                    payload: Some(ActivationPayload {
                        value: Some(Value::InlineData(b"request".to_vec())),
                    }),
                }],
                usage: None,
                evidence: Vec::new(),
            },
        };
        let completed = decide_complete(&active, &completion).unwrap();
        let mut completed_record = to_engine_record(&completed.records[0], 2).unwrap();
        completed_record.offset = Offset(2);
        let projected = project(&completed_record, Some(&snapshot)).unwrap();
        assert!(projected
            .ops
            .iter()
            .any(|op| matches!(op, StoreOp::PutStepMemo { data, .. } if data == b"result")));
        assert!(projected.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutEntityState { version: 5, data, .. } if data == b"state"
        )));
        assert!(projected.ops.iter().any(|op| matches!(
            op,
            StoreOp::PutActivationOutbox { intent_id, .. } if intent_id == "effect-1"
        )));

        let completed_projection = projected
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutActivation { data, .. } => Some(data.clone()),
                _ => None,
            })
            .unwrap();
        let completed_state = state_from_projection(completed_projection.as_slice()).unwrap();
        let mut compaction_record = to_engine_record(
            &engine_core::activation::snapshot_record(&completed_state),
            3,
        )
        .unwrap();
        compaction_record.offset = Offset(3);
        let compacted = project(&compaction_record, Some(&completed_projection)).unwrap();
        assert_eq!(compacted.ops.len(), 1);
        assert!(matches!(
            compacted.ops.first(),
            Some(StoreOp::PutActivation { .. })
        ));
    }

    #[test]
    fn replay_before_materialized_activation_offset_is_a_noop() {
        let begin = BeginActivationCommand {
            request: BeginActivationRequest {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                parent_activation_id: String::new(),
                kind: ActivationKind::Step as i32,
                stable_key: "step/load".to_string(),
                input_digest: vec![1; 32],
                definition_digest: vec![2; 32],
                recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                worker_session_id: "worker-1".to_string(),
                run_authority: b"run-auth".to_vec(),
                lease_authority: b"lease-auth".to_vec(),
                child: None,

                display_name: String::new(),
                input_data: Vec::new(),
                display_parent_correlation_id: String::new(),
            },
            issued_fence_token: b"fence-1".to_vec(),
        };
        let started = decide_begin(None, &begin).unwrap();
        let mut started_record = to_engine_record(&started.records[0], 1).unwrap();
        started_record.offset = Offset(10);
        let started_projection = project(&started_record, None).unwrap();
        let started_snapshot = started_projection
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutActivation { data, .. } => Some(data.clone()),
                _ => None,
            })
            .unwrap();
        let active = state_from_projection(started_snapshot.as_slice()).unwrap();
        let output = ActivationPayload {
            value: Some(Value::InlineData(b"result".to_vec())),
        };
        let completion = CompleteActivationCommand {
            request: CompleteActivationRequest {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: b"fence-1".to_vec(),
                output_digest: payload_digest(&output).unwrap(),
                output: Some(output),
                state_mutations: Vec::new(),
                outbox_intents: Vec::new(),
                usage: None,
                evidence: Vec::new(),
            },
        };
        let completed = decide_complete(&active, &completion).unwrap();
        let mut completed_record = to_engine_record(&completed.records[0], 2).unwrap();
        completed_record.offset = Offset(20);
        let completed_projection = project(&completed_record, Some(&started_snapshot)).unwrap();
        let completed_snapshot = completed_projection
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutActivation { data, .. } => Some(data.clone()),
                _ => None,
            })
            .unwrap();

        let replayed_start = project(&started_record, Some(&completed_snapshot)).unwrap();
        let replayed_completion = project(&completed_record, Some(&completed_snapshot)).unwrap();

        assert!(replayed_start.ops.is_empty());
        assert!(replayed_completion.ops.is_empty());
    }

    #[test]
    fn child_linkage_survives_projection_and_snapshot_rebuild() {
        let child = ChildActivationLinkage {
            child_key: "child/researcher".to_string(),
            child_run_id: "child-run-1".to_string(),
            child_session_id: "child-session-1".to_string(),
            child_definition_digest: vec![3; 32],
            join_policy: ChildJoinPolicy::Detached as i32,
        };
        let begin = BeginActivationCommand {
            request: BeginActivationRequest {
                project_id: "project-1".to_string(),
                run_id: "run-1".to_string(),
                parent_activation_id: "actv1_parent".to_string(),
                kind: ActivationKind::Child as i32,
                stable_key: child.child_key.clone(),
                input_digest: vec![1; 32],
                definition_digest: child.child_definition_digest.clone(),
                recovery_policy: ActivationRecoveryPolicy::DurableSteps as i32,
                worker_session_id: "worker-1".to_string(),
                run_authority: b"run-auth".to_vec(),
                lease_authority: b"lease-auth".to_vec(),
                child: Some(child.clone()),

                display_name: String::new(),
                input_data: Vec::new(),
                display_parent_correlation_id: String::new(),
            },
            issued_fence_token: b"fence-1".to_vec(),
        };
        let started = decide_begin(None, &begin).unwrap();
        let mut started_record = to_engine_record(&started.records[0], 1).unwrap();
        started_record.offset = Offset(7);

        let projected = project(&started_record, None).unwrap();
        let snapshot = projected
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutActivation { data, .. } => Some(data.clone()),
                _ => None,
            })
            .expect("child activation projection");
        let projected_state = state_from_projection(snapshot.as_slice()).unwrap();
        assert_eq!(projected_state.identity.child.as_ref(), Some(&child));

        let mut rebuild_record = to_engine_record(
            &engine_core::activation::snapshot_record(&projected_state),
            8,
        )
        .unwrap();
        rebuild_record.offset = Offset(8);
        let rebuilt = project(&rebuild_record, Some(&snapshot)).unwrap();
        let rebuilt_snapshot = rebuilt
            .ops
            .iter()
            .find_map(|op| match op {
                StoreOp::PutActivation { data, .. } => Some(data.clone()),
                _ => None,
            })
            .expect("rebuilt child activation projection");
        let rebuilt_state = state_from_projection(rebuilt_snapshot.as_slice()).unwrap();
        assert_eq!(rebuilt_state.identity.child.as_ref(), Some(&child));
    }
}
