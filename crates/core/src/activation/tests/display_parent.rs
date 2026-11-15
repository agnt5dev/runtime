use super::*;

const DISPLAY_PARENT: &str = "iteration-1";
const META_DISPLAY_PARENT: &str = "display_parent_correlation_id";

fn display_begin(worker: &str) -> BeginActivationCommand {
    let mut command = begin(worker);
    command.request.kind = ActivationKind::Model as i32;
    command.request.display_parent_correlation_id = DISPLAY_PARENT.to_string();
    command
}

fn assert_display_parent(record: &ActivationJournalRecord) {
    let envelope = to_engine_record(record, 123).unwrap();
    assert_eq!(
        envelope
            .metadata
            .get(META_DISPLAY_PARENT)
            .map(String::as_str),
        Some(DISPLAY_PARENT)
    );
    assert_eq!(envelope.parent_event_id.as_deref(), Some("parent-1"));
    assert_eq!(
        from_engine_record(&envelope).unwrap().as_ref(),
        Some(record)
    );
}

#[test]
fn display_parent_survives_completion_and_snapshot_rebuild() {
    let started = decide_begin(None, &display_begin("worker-a")).unwrap();
    assert_display_parent(&started.records[0]);
    let active = apply_transition(None, &started, 1);
    let completion = decide_complete(&active, &complete(&active, b"result")).unwrap();
    assert_display_parent(&completion.records[0]);
    let completed = apply_transition(Some(active), &completion, 2);
    let snapshot = snapshot_record(&completed);
    assert_display_parent(&snapshot);
    let encoded = encode_journal_record(&snapshot);
    let decoded = decode_journal_record(&encoded).unwrap();
    let restored = apply_journal_record(None, &decoded, Offset(999)).unwrap();
    assert_eq!(snapshot_record(&restored), snapshot);
}

#[test]
fn display_parent_does_not_change_identity_or_replay() {
    let command = display_begin("worker-a");
    let mut plain = command.clone();
    plain.request.display_parent_correlation_id.clear();
    let started = decide_begin(None, &command).unwrap();
    let active = apply_transition(None, &started, 1);
    let plain_started = decide_begin(None, &plain).unwrap();
    let plain_active = apply_transition(None, &plain_started, 1);
    assert_eq!(active.identity, plain_active.identity);
    let mut changed = command;
    changed.request.display_parent_correlation_id = "another-iteration".to_string();
    let repeated = decide_begin(Some(&active), &changed).unwrap();
    assert!(repeated.records.is_empty());
    assert_eq!(
        repeated.decision,
        decide_begin(Some(&active), &plain).unwrap().decision
    );
    let command = complete(&active, b"result");
    let completion = decide_complete(&active, &command).unwrap();
    let completed = apply_transition(Some(active), &completion, 2);
    let replay = decide_begin(Some(&completed), &changed).unwrap();
    assert!(replay.records.is_empty());
    let ActivationDecision::Begin(receipt) = replay.decision else {
        panic!("expected begin");
    };
    assert_eq!(receipt.outcome, BeginActivationOutcome::Replay);
    assert_eq!(receipt.replay_result, command.request.output);
    let repeated_completion = decide_complete(&completed, &command).unwrap();
    assert!(repeated_completion.records.is_empty());
}

#[test]
fn display_parent_survives_retry_started_and_failure_records() {
    let started = decide_begin(None, &display_begin("worker-a")).unwrap();
    let active = apply_transition(None, &started, 1);
    let failure = decide_fail(
        &active,
        &FailActivationCommand {
            request: FailActivationRequest {
                project_id: active.identity.project_id.clone(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: active.current_attempt.fence_token.clone(),
                error_code: "TRANSIENT".to_string(),
                retryable: true,
                external_outcome_certainty: ActivationExternalOutcomeCertainty::NoEffect as i32,
                ..Default::default()
            },
        },
    )
    .unwrap();
    assert_display_parent(&failure.records[0]);
    let retry_ready = apply_transition(Some(active), &failure, 2);
    let retried = decide_begin(Some(&retry_ready), &display_begin("worker-b")).unwrap();
    assert_display_parent(&retried.records[0]);
    let state = apply_transition(Some(retry_ready), &retried, 3);
    assert_eq!(state.current_attempt.attempt, 2);
}

#[test]
fn display_parent_survives_suspension_cancellation_and_unknown_outcome() {
    let started = decide_begin(None, &display_begin("worker-a")).unwrap();
    let active = apply_transition(None, &started, 1);
    let suspension = decide_suspend(&active, &suspend(&active, "external-resume")).unwrap();
    assert_display_parent(&suspension.records[0]);
    let cancellation = decide_cancel(
        &active,
        &CancelActivationCommand {
            project_id: active.identity.project_id.clone(),
            run_id: active.identity.run_id.clone(),
            activation_id: active.identity.activation_id.clone(),
            reason: "cancelled".to_string(),
        },
    )
    .unwrap();
    assert_display_parent(&cancellation.records[0]);
    let uncertain = decide_fail(
        &active,
        &FailActivationCommand {
            request: FailActivationRequest {
                project_id: active.identity.project_id.clone(),
                run_id: active.identity.run_id.clone(),
                activation_id: active.identity.activation_id.clone(),
                attempt: 1,
                fence_token: active.current_attempt.fence_token.clone(),
                error_code: "UNKNOWN".to_string(),
                external_outcome_certainty: ActivationExternalOutcomeCertainty::Unknown as i32,
                ..Default::default()
            },
        },
    )
    .unwrap();
    assert_display_parent(&uncertain.records[0]);
}

#[test]
fn display_parent_is_bounded_in_utf8_bytes() {
    let mut command = display_begin("worker-a");
    command.request.display_parent_correlation_id = "é".repeat(128);
    assert!(decide_begin(None, &command).is_ok());
    command.request.display_parent_correlation_id.push('x');
    assert!(
        matches!(decide_begin(None, &command), Err(ActivationError::InvalidArgument(message)) if message.contains(META_DISPLAY_PARENT))
    );
}

#[test]
fn historical_records_without_display_parent_keep_their_canonical_bytes() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../docs/contracts/fixtures/durable-activation-v1.json"
    ))
    .unwrap();
    let fixtures = fixture["canonical_records_json"].as_object().unwrap();
    assert!(!fixtures.is_empty());
    for bytes in fixtures.values() {
        let bytes = bytes.as_str().unwrap().as_bytes();
        let decoded = decode_journal_record(bytes).unwrap();
        assert_eq!(encode_journal_record(&decoded), bytes);
        assert!(!to_engine_record(&decoded, 0)
            .unwrap()
            .metadata
            .contains_key(META_DISPLAY_PARENT));
    }
}
