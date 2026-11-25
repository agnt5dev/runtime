//! Reusable conformance checks for runtime storage adapters.
//!
//! Backend crates depend on this crate only for tests. The checks deliberately
//! exercise the public storage contracts and do not inspect physical keys,
//! column families, tables, or files.

mod required_ports;

pub use required_ports::{
    assert_agent_instances_unsupported, assert_required_storage_port_contract, RequiredStoragePorts,
};

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use engine_core::{
    activation::{
        activation_id, payload_digest, to_engine_record, ActivationCancelledRecord,
        ActivationCompletedRecord, ActivationHeader, ActivationJournalRecord,
        ActivationStartedRecord, ActivationSuspendedRecord, Event as ActivationEvent,
        ACTIVATION_JOURNAL_VERSION,
    },
    error::EngineError,
    lease_fence::{
        LeaseExpiryReason, LeaseFence, LeaseFenceKind, META_FENCE_RECEIVED_AT_MS,
        RUN_LEASE_EXPIRED_EVENT, RUN_LEASE_RENEWED_EVENT, RUN_META_DISPATCH_GENERATION,
        RUN_META_GRANT_ID,
    },
    storage_contract::{
        ArchiveOutboxScanEntry, ArchiveOutboxStore, DispatchOutboxScanEntry, DispatchOutboxStore,
        JournalPosition, LeaseWorkStore, PendingWorkStore, ProjectionCommit, ProjectionCommitter,
        ProjectionMutation, ProjectionSource, ProjectionStateReader, RunQueryStore,
        SessionQueryStore, StorageCapabilities, StorageCapability, TimerWorkStore,
        WorkflowQueryStore,
    },
    traits::{AppendOutcome, MaterializedStore, Segment},
    types::{Offset, Record},
};
use engine_proto::activation_payload::Value as ActivationValue;
use engine_proto::{
    ActivationKind, ActivationPayload, ActivationRecoveryPolicy, ActivationStateMutation,
    ChildActivationLinkage, ChildJoinPolicy,
};
use tokio_stream::StreamExt;

fn record(run_id: &str, event_type: &str) -> Record {
    Record {
        offset: Offset::ZERO,
        project_id: "storage-conformance".to_string(),
        run_id: run_id.to_string(),
        event_type: event_type.to_string(),
        data: Bytes::from_static(b"{}"),
        timestamp_ns: 1,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: HashMap::new(),
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

/// Assert the required journal behavior shared by every runtime profile.
///
/// The factory returns a segment plus an arbitrary guard. RocksDB uses the
/// guard to keep its temporary directory alive; in-memory adapters can use
/// `()`. Each contract case receives a fresh segment.
pub async fn assert_journal_contract<F, G>(create: F, capabilities: &StorageCapabilities)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    append_range_and_tail_contract(&create).await;
    append_outcome_idempotency_contract(&create).await;
    lease_assignment_idempotency_contract(&create).await;
    run_lease_fence_contract(&create, capabilities).await;
    activation_idempotency_contract(&create).await;
    activation_authority_contract(&create).await;
    activation_terminal_race_contract(&create).await;
    timer_generation_idempotency_contract(&create).await;
    resumable_tail_contract(&create).await;
    empty_journal_readiness_contract(&create).await;
    if capabilities.supports(StorageCapability::JournalRestore) {
        committed_restore_contract(&create).await;
    }
    if capabilities.supports(StorageCapability::JournalRetention) {
        retention_contract(&create).await;
    }
    if capabilities.supports(StorageCapability::JournalEpochTransition) {
        epoch_transition_contract(&create).await;
    }
}

async fn activation_terminal_race_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let project_id = "storage-conformance";
    let run_id = "activation-terminal-race";
    let stable_key = "child/root-required";
    let child_id = activation_id(
        project_id,
        run_id,
        "",
        ActivationKind::Child as i32,
        stable_key,
    );
    let child = to_engine_record(
        &ActivationJournalRecord {
            version: ACTIVATION_JOURNAL_VERSION,
            event: Some(ActivationEvent::Started(ActivationStartedRecord {
                project_id: project_id.to_string(),
                run_id: run_id.to_string(),
                parent_activation_id: String::new(),
                activation_id: child_id,
                kind: ActivationKind::Child as i32,
                stable_key: stable_key.to_string(),
                input_digest: vec![9; 32],
                definition_digest: vec![10; 32],
                recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                attempt: 1,
                fence_token: b"race-child-fence".to_vec(),
                worker_session_id: "race-worker".to_string(),
                run_authority: b"run-authority".to_vec(),
                lease_authority: b"lease-authority".to_vec(),
                previous_attempt: 0,
                name: String::new(),
                decision: "execute".to_string(),
                input_data: None,
                display_parent_correlation_id: None,
                child: Some(ChildActivationLinkage {
                    child_key: stable_key.to_string(),
                    child_run_id: "race-child-run".to_string(),
                    child_session_id: "race-child-session".to_string(),
                    child_definition_digest: vec![10; 32],
                    join_policy: ChildJoinPolicy::Required as i32,
                }),
            })),
        },
        1,
    )
    .expect("required contract: encode racing required child");
    let completion = record(run_id, "run.completed");
    let child_segment = segment.clone();
    let completion_segment = segment.clone();
    let (child_result, completion_result) = tokio::join!(
        async move { child_segment.append_batch(&[child]).await },
        async move { completion_segment.append_batch(&[completion]).await },
    );

    match (child_result, completion_result) {
        (Ok(_), Err(EngineError::Activation(
            engine_core::error::ActivationError::RequiredChildUnresolved { .. },
        ))) => {}
        (Err(EngineError::Activation(
            engine_core::error::ActivationError::StaleAuthority { .. },
        )), Ok(_)) => {}
        outcomes => panic!(
            "required contract: child-begin/run-completion race must have exactly one winner: {outcomes:?}"
        ),
    }
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(1));
}

async fn activation_authority_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let project_id = "storage-conformance";
    let run_id = "activation-authority-run";
    let parent_id = activation_id(
        project_id,
        run_id,
        "",
        ActivationKind::Step as i32,
        "parent",
    );
    let child_id = activation_id(
        project_id,
        run_id,
        &parent_id,
        ActivationKind::Child as i32,
        "child/required",
    );
    let start = |activation_id: &str,
                 parent_activation_id: &str,
                 kind: ActivationKind,
                 stable_key: &str,
                 fence: &[u8],
                 child: Option<ChildActivationLinkage>| {
        to_engine_record(
            &ActivationJournalRecord {
                version: ACTIVATION_JOURNAL_VERSION,
                event: Some(ActivationEvent::Started(ActivationStartedRecord {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                    parent_activation_id: parent_activation_id.to_string(),
                    activation_id: activation_id.to_string(),
                    kind: kind as i32,
                    stable_key: stable_key.to_string(),
                    input_digest: vec![7; 32],
                    definition_digest: vec![8; 32],
                    recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                    attempt: 1,
                    fence_token: fence.to_vec(),
                    worker_session_id: "worker-authority".to_string(),
                    run_authority: b"run-authority".to_vec(),
                    lease_authority: b"lease-authority".to_vec(),
                    previous_attempt: 0,
                    name: String::new(),
                    decision: "execute".to_string(),
                    input_data: None,
                    display_parent_correlation_id: None,
                    child,
                })),
            },
            1,
        )
        .expect("required contract: encode authority activation start")
    };
    let step_header = ActivationHeader {
        kind: ActivationKind::Step as i32,
        stable_key: "parent".to_string(),
        recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
        input_digest: vec![7; 32],
        definition_digest: vec![8; 32],
        ..Default::default()
    };
    let child_header = ActivationHeader {
        display_parent_correlation_id: None,
        name: String::new(),
        kind: ActivationKind::Child as i32,
        stable_key: "child/required".to_string(),
        parent_activation_id: parent_id.clone(),
        recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
        input_digest: vec![7; 32],
        definition_digest: vec![8; 32],
        child: Some(ChildActivationLinkage {
            child_key: "child/required".to_string(),
            child_run_id: "child-run".to_string(),
            child_session_id: "child-session".to_string(),
            child_definition_digest: vec![8; 32],
            join_policy: ChildJoinPolicy::Required as i32,
        }),
    };
    let complete = |activation_id: &str, header: ActivationHeader, fence: &[u8], output: &[u8]| {
        let output = ActivationPayload {
            value: Some(ActivationValue::InlineData(output.to_vec())),
        };
        to_engine_record(
            &ActivationJournalRecord {
                version: ACTIVATION_JOURNAL_VERSION,
                event: Some(ActivationEvent::Completed(ActivationCompletedRecord {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                    activation_id: activation_id.to_string(),
                    attempt: 1,
                    header,
                    started_journal_offset: 0,
                    fence_token: fence.to_vec(),
                    output_digest: payload_digest(&output)
                        .expect("required contract: authority completion digest"),
                    output: Some(output),
                    state_mutations: Vec::new(),
                    outbox_intents: Vec::new(),
                    usage: None,
                    duration_ms: None,
                    evidence: Vec::new(),
                })),
            },
            2,
        )
        .expect("required contract: encode authority activation completion")
    };

    let parent_start = start(
        &parent_id,
        "",
        ActivationKind::Step,
        "parent",
        b"parent-fence",
        None,
    );
    let child_start = start(
        &child_id,
        &parent_id,
        ActivationKind::Child,
        "child/required",
        b"child-fence",
        Some(ChildActivationLinkage {
            child_key: "child/required".to_string(),
            child_run_id: "child-run".to_string(),
            child_session_id: "child-session".to_string(),
            child_definition_digest: vec![8; 32],
            join_policy: ChildJoinPolicy::Required as i32,
        }),
    );
    segment
        .append_batch(&[parent_start, child_start])
        .await
        .expect("required contract: append parent and required child");
    assert!(segment
        .load_activation_authority(project_id, run_id, &parent_id)
        .await
        .expect("required contract: load parent authority")
        .is_some());
    assert_eq!(
        segment
            .first_unsettled_required_child(project_id, run_id, Some(&parent_id))
            .await
            .expect("required contract: read parent obligation"),
        Some(child_id.clone())
    );

    assert!(matches!(
        segment
            .append_batch(&[complete(
                &parent_id,
                step_header.clone(),
                b"parent-fence",
                b"parent"
            )])
            .await,
        Err(EngineError::Activation(
            engine_core::error::ActivationError::RequiredChildUnresolved { .. }
        ))
    ));
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(2));

    segment
        .append_batch(&[
            complete(&child_id, child_header.clone(), b"child-fence", b"child"),
            complete(&parent_id, step_header.clone(), b"parent-fence", b"parent"),
        ])
        .await
        .expect("required contract: settle child then parent atomically");
    assert_eq!(
        segment
            .first_unsettled_required_child(project_id, run_id, None)
            .await
            .expect("required contract: all obligations settled"),
        None
    );
    assert!(segment
        .list_nonterminal_activation_authority(project_id, run_id)
        .await
        .expect("required contract: list nonterminal authority")
        .is_empty());

    let active_at_terminal_id = activation_id(
        project_id,
        run_id,
        "",
        ActivationKind::Step as i32,
        "active-at-terminal",
    );
    segment
        .append_batch(&[start(
            &active_at_terminal_id,
            "",
            ActivationKind::Step,
            "active-at-terminal",
            b"active-at-terminal-fence",
            None,
        )])
        .await
        .expect("required contract: append activation active at run terminalization");
    segment
        .append_batch(&[record(run_id, "run.completed")])
        .await
        .expect("required contract: close run activation gate");
    let late_id = activation_id(project_id, run_id, "", ActivationKind::Step as i32, "late");
    assert!(matches!(
        segment
            .append_batch(&[start(
                &late_id,
                "",
                ActivationKind::Step,
                "late",
                b"late-fence",
                None,
            )])
            .await,
        Err(EngineError::Activation(
            engine_core::error::ActivationError::StaleAuthority { .. }
        ))
    ));
    assert!(matches!(
        segment
            .append_batch(&[complete(
                &active_at_terminal_id,
                ActivationHeader {
                    stable_key: "active-at-terminal".to_string(),
                    ..step_header.clone()
                },
                b"active-at-terminal-fence",
                b"late-completion",
            )])
            .await,
        Err(EngineError::Activation(
            engine_core::error::ActivationError::StaleAuthority { .. }
        ))
    ));
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(6));
}

async fn lease_assignment_idempotency_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let assignment = |lease_id: &str, attempt: u32, generation: &str| Record {
        event_type: "run.assigned".to_string(),
        metadata: HashMap::from([
            ("lease_id".to_string(), lease_id.to_string()),
            ("lease_attempt".to_string(), attempt.to_string()),
            ("lease_worker_id".to_string(), "worker-1".to_string()),
            (
                "lease_session_hash".to_string(),
                "session-hash-1".to_string(),
            ),
            ("lease_expires_at_ms".to_string(), "60000".to_string()),
            ("lease_timeout_ms".to_string(), "15000".to_string()),
            (
                RUN_META_DISPATCH_GENERATION.to_string(),
                generation.to_string(),
            ),
            (RUN_META_GRANT_ID.to_string(), format!("grant-{generation}")),
        ]),
        ..record("lease-run", "run.assigned")
    };

    let first = assignment("lease-1", 0, "queue-offset-10");
    let admitted = segment
        .append_batch_with_outcome(std::slice::from_ref(&first))
        .await
        .expect("required contract: first lease assignment");
    assert_eq!(admitted.offsets, [Offset(0)]);
    assert_eq!(admitted.appended, [true]);
    let lease = segment
        .load_run_lease_authority("storage-conformance", "lease-run")
        .await
        .expect("required contract: load run lease authority")
        .expect("required contract: assignment installs run lease authority");
    assert_eq!(lease.dispatch_generation, "queue-offset-10");
    assert_eq!(lease.grant_id, "grant-queue-offset-10");
    assert_eq!(lease.accepted_offset, Some(0));
    assert_eq!(lease.lease_timeout_ms, 15_000);

    let exact = segment
        .append_batch_with_outcome(std::slice::from_ref(&first))
        .await
        .expect("required contract: exact lease assignment retry");
    assert_eq!(exact.offsets, [Offset(0)]);
    assert_eq!(exact.appended, [false]);

    let competitor = assignment("lease-2", 0, "queue-offset-10");
    assert!(segment
        .append_batch_with_outcome(&[competitor])
        .await
        .is_err());
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(1));

    let resumed_generation = assignment("lease-3", 0, "resume-offset-20");
    let neighbor = record("neighbor-run", "run.queued");
    assert_lease_fence_mismatch(
        segment
            .append_batch_with_outcome(&[neighbor, resumed_generation.clone()])
            .await,
        "active lease replacement by another dispatch generation",
    );
    assert_eq!(
        segment.tail_offset().await.unwrap(),
        Offset(1),
        "required contract: the whole conflicting batch must remain unwritten"
    );

    let paused = fenced_record(
        "lease-run",
        "workflow.paused",
        &lease_fence(LeaseFenceKind::PullCompletion, "lease-1", 0),
    );
    let paused = segment
        .append_batch_with_outcome(&[paused])
        .await
        .expect("required contract: release the active generation");
    assert_eq!(paused.offsets, [Offset(1)]);

    let resumed = segment
        .append_batch_with_outcome(&[resumed_generation])
        .await
        .expect("required contract: resumed dispatch generation assignment");
    assert_eq!(resumed.offsets, [Offset(2)]);
    assert_eq!(resumed.appended, [true]);

    let paused = fenced_record(
        "lease-run",
        "workflow.paused",
        &lease_fence(LeaseFenceKind::PullCompletion, "lease-3", 0),
    );
    let paused = segment
        .append_batch_with_outcome(&[paused])
        .await
        .expect("required contract: release the resumed generation");
    assert_eq!(paused.offsets, [Offset(3)]);

    let retry_generation = assignment("lease-4", 1, "retry-offset-30");
    let retry = segment
        .append_batch_with_outcome(&[retry_generation])
        .await
        .expect("required contract: retried dispatch generation assignment");
    assert_eq!(retry.offsets, [Offset(4)]);
    assert_eq!(retry.appended, [true]);
}

/// The lease-backed `run.assigned` record every fence case starts from.
fn lease_assignment(
    run_id: &str,
    lease_id: &str,
    attempt: u32,
    expires_at_ms: i64,
    generation: &str,
) -> Record {
    Record {
        metadata: HashMap::from([
            ("lease_id".to_string(), lease_id.to_string()),
            ("lease_attempt".to_string(), attempt.to_string()),
            ("lease_worker_id".to_string(), "worker-1".to_string()),
            (
                "lease_session_hash".to_string(),
                "session-hash-1".to_string(),
            ),
            ("lease_expires_at_ms".to_string(), expires_at_ms.to_string()),
            ("lease_timeout_ms".to_string(), "15000".to_string()),
            (
                RUN_META_DISPATCH_GENERATION.to_string(),
                generation.to_string(),
            ),
            (RUN_META_GRANT_ID.to_string(), format!("grant-{generation}")),
        ]),
        ..record(run_id, "run.assigned")
    }
}

fn lease_fence(kind: LeaseFenceKind, lease_id: &str, attempt: u32) -> LeaseFence {
    LeaseFence {
        kind,
        lease_id: lease_id.to_string(),
        worker_id: "worker-1".to_string(),
        session_hash: match kind {
            LeaseFenceKind::PullCompletion => "session-hash-1".to_string(),
            _ => String::new(),
        },
        attempt,
        received_at_ms: 1_000,
        expiry_reason: (kind == LeaseFenceKind::LeaseExpiry).then_some(LeaseExpiryReason::Timeout),
    }
}

fn fenced_record(run_id: &str, event_type: &str, fence: &LeaseFence) -> Record {
    let mut record = record(run_id, event_type);
    fence.stamp(&mut record);
    record
}

fn assert_lease_fence_mismatch(result: Result<AppendOutcome, EngineError>, context: &str) {
    match result {
        Err(EngineError::LeaseFenceMismatch { .. }) => {}
        other => {
            panic!("required contract: {context} must fail with LeaseFenceMismatch, got {other:?}")
        }
    }
}

/// Lease fences are decided at append time against the lease entry the
/// journal folded from earlier accepted records. Losers never enter the
/// journal; trusted history (restore, replication) never rejects on lease
/// grounds.
async fn run_lease_fence_contract<F, G>(create: &F, capabilities: &StorageCapabilities)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    // Completion fencing: only the active lease may complete the run, a
    // lost-ack retry converges on the accepted offset, and a later terminal
    // from the same lease loses to the closed gate.
    {
        let (segment, _guard) = create();
        let run_id = "fence-run";
        let assigned = segment
            .append_batch_with_outcome(&[lease_assignment(run_id, "lease-1", 0, 60_000, "gen-1")])
            .await
            .expect("required contract: lease assignment");
        assert_eq!(assigned.offsets, [Offset(0)]);

        let stale = fenced_record(
            run_id,
            "run.completed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-0", 0),
        );
        assert_lease_fence_mismatch(
            segment.append_batch_with_outcome(&[stale]).await,
            "completion from a different lease",
        );
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(1));

        let completed = fenced_record(
            run_id,
            "run.completed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-1", 0),
        );
        let accepted = segment
            .append_batch_with_outcome(std::slice::from_ref(&completed))
            .await
            .expect("required contract: completion from the active lease");
        assert_eq!(accepted.offsets, [Offset(1)]);
        assert_eq!(accepted.appended, [true]);

        let mut retry = completed.clone();
        retry
            .metadata
            .insert(META_FENCE_RECEIVED_AT_MS.to_string(), "2000".to_string());
        let retried = segment
            .append_batch_with_outcome(&[retry])
            .await
            .expect("required contract: lost-ack completion retry");
        assert_eq!(retried.offsets, [Offset(1)]);
        assert_eq!(retried.appended, [false]);

        let late_failure = fenced_record(
            run_id,
            "run.failed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-1", 0),
        );
        assert_lease_fence_mismatch(
            segment.append_batch_with_outcome(&[late_failure]).await,
            "terminal failure after the run completed",
        );
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(2));
    }

    // Renewal and expiry: renewals only extend forward, timeout expiries
    // honour the renewed expiry, and the released tombstone rejects the
    // lease's own late completion.
    {
        let (segment, _guard) = create();
        let run_id = "renew-run";
        segment
            .append_batch_with_outcome(&[lease_assignment(run_id, "lease-1", 0, 60_000, "gen-1")])
            .await
            .expect("required contract: lease assignment");

        let renewal = |expires_at_ms: i64| {
            let mut record = fenced_record(
                run_id,
                RUN_LEASE_RENEWED_EVENT,
                &lease_fence(LeaseFenceKind::LeaseRenewal, "lease-1", 0),
            );
            record
                .metadata
                .insert("lease_expires_at_ms".to_string(), expires_at_ms.to_string());
            record
        };
        let renewed = segment
            .append_batch_with_outcome(&[renewal(90_000)])
            .await
            .expect("required contract: forward lease renewal");
        assert_eq!(renewed.offsets, [Offset(1)]);
        assert_lease_fence_mismatch(
            segment.append_batch_with_outcome(&[renewal(70_000)]).await,
            "lease renewal that does not extend the expiry",
        );
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(2));

        let expiry = |timestamp_ms: i64| {
            let mut record = fenced_record(
                run_id,
                RUN_LEASE_EXPIRED_EVENT,
                &lease_fence(LeaseFenceKind::LeaseExpiry, "lease-1", 0),
            );
            record.timestamp_ns = timestamp_ms * 1_000_000;
            record
        };
        assert_lease_fence_mismatch(
            segment.append_batch_with_outcome(&[expiry(89_999)]).await,
            "timeout expiry before the renewed expiry",
        );
        let expired = segment
            .append_batch_with_outcome(&[expiry(90_000)])
            .await
            .expect("required contract: timeout expiry at the renewed expiry");
        assert_eq!(expired.offsets, [Offset(2)]);

        let late_completion = fenced_record(
            run_id,
            "run.completed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-1", 0),
        );
        assert_lease_fence_mismatch(
            segment.append_batch_with_outcome(&[late_completion]).await,
            "completion from an expired lease",
        );
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(3));
    }

    // Trusted restore: a legacy history may contain an Active assignment
    // overwritten by another generation. Restoring that accepted sequence
    // must fold the second assignment so subsequent Live fences agree.
    if capabilities.supports(StorageCapability::JournalRestore) {
        let (segment, _guard) = create();
        let run_id = "restored-mixed-generation-run";
        let mut first = lease_assignment(run_id, "lease-1", 0, 60_000, "gen-1");
        first.offset = Offset(0);
        let mut second = lease_assignment(run_id, "lease-2", 0, 60_000, "gen-2");
        second.offset = Offset(1);
        segment
            .restore_committed_batch(&[first, second])
            .await
            .expect("restore capability: mixed assignment generations");

        let completed = fenced_record(
            run_id,
            "run.completed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-2", 0),
        );
        let accepted = segment
            .append_batch_with_outcome(&[completed])
            .await
            .expect("restored authority must select the last accepted assignment");
        assert_eq!(accepted.offsets, [Offset(2)]);
    }

    // Trusted follower append obeys the same rule: it persists a leader's
    // already-accepted mixed-generation sequence and installs the last lease.
    {
        let (segment, _guard) = create();
        let run_id = "replica-run";
        let assigned = segment
            .append_batch_with_outcome(&[lease_assignment(run_id, "lease-1", 0, 60_000, "gen-1")])
            .await
            .expect("required contract: lease assignment");
        assert_eq!(assigned.offsets, [Offset(0)]);
        let mut replicated = lease_assignment(run_id, "lease-2", 0, 60_000, "gen-2");
        replicated.offset = Offset(1);
        let offsets = segment
            .append_replicated_batch(&[replicated])
            .await
            .expect("required contract: replicated assignment from another generation");
        assert_eq!(offsets, [Offset(1)]);

        let completed = fenced_record(
            run_id,
            "run.completed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-2", 0),
        );
        let accepted = segment
            .append_batch_with_outcome(&[completed])
            .await
            .expect("replicated authority must select the last accepted assignment");
        assert_eq!(accepted.offsets, [Offset(2)]);
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(3));
    }

    // Pause releases the lease without closing the gate: the run can be
    // re-dispatched under a new lease, and the paused lease cannot complete.
    {
        let (segment, _guard) = create();
        let run_id = "pause-run";
        segment
            .append_batch_with_outcome(&[lease_assignment(run_id, "lease-1", 0, 60_000, "gen-1")])
            .await
            .expect("required contract: lease assignment");
        let paused = fenced_record(
            run_id,
            "workflow.paused",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-1", 0),
        );
        let paused = segment
            .append_batch_with_outcome(&[paused])
            .await
            .expect("required contract: pause from the active lease");
        assert_eq!(paused.offsets, [Offset(1)]);

        let resumed = segment
            .append_batch_with_outcome(&[lease_assignment(run_id, "lease-2", 0, 60_000, "gen-2")])
            .await
            .expect("required contract: re-dispatch after pause");
        assert_eq!(resumed.offsets, [Offset(2)]);

        let stale = fenced_record(
            run_id,
            "run.completed",
            &lease_fence(LeaseFenceKind::PullCompletion, "lease-1", 0),
        );
        assert_lease_fence_mismatch(
            segment.append_batch_with_outcome(&[stale]).await,
            "completion from the paused lease",
        );
        assert_eq!(segment.tail_offset().await.unwrap(), Offset(3));
    }
}

async fn timer_generation_idempotency_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let timer = |event_type: &str, data: &'static [u8], timestamp_ns: i64| Record {
        event_type: event_type.to_string(),
        data: Bytes::from_static(data),
        timestamp_ns,
        metadata: HashMap::from([("timer_id".to_string(), "timer-generation-1".to_string())]),
        ..record("timer-run", event_type)
    };

    let scheduled = timer("timer.scheduled", br#"{"fire_at_ns":42000000000}"#, 1);
    let admitted = segment
        .append_batch_with_outcome(std::slice::from_ref(&scheduled))
        .await
        .expect("required contract: timer schedule");
    assert_eq!(admitted.offsets, [Offset(0)]);
    assert_eq!(admitted.appended, [true]);
    let exact_schedule = segment
        .append_batch_with_outcome(std::slice::from_ref(&scheduled))
        .await
        .expect("required contract: exact timer schedule retry");
    assert_eq!(exact_schedule.offsets, [Offset(0)]);
    assert_eq!(exact_schedule.appended, [false]);

    let changed_schedule = timer("timer.scheduled", br#"{"fire_at_ns":43000000000}"#, 2);
    assert!(segment
        .append_batch_with_outcome(&[changed_schedule])
        .await
        .is_err());

    let fired = timer("timer.fired", br#"{"fired_at_ns":42000000001}"#, 42);
    let fire = segment
        .append_batch_with_outcome(std::slice::from_ref(&fired))
        .await
        .expect("required contract: timer fire");
    assert_eq!(fire.offsets, [Offset(1)]);
    assert_eq!(fire.appended, [true]);
    let exact_fire = segment
        .append_batch_with_outcome(std::slice::from_ref(&fired))
        .await
        .expect("required contract: exact timer fire retry");
    assert_eq!(exact_fire.offsets, [Offset(1)]);
    assert_eq!(exact_fire.appended, [false]);

    let canceled = timer("timer.canceled", br#"{"reason":"cancelled"}"#, 43);
    assert!(segment
        .append_batch_with_outcome(&[canceled])
        .await
        .is_err());
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(2));
}

async fn activation_idempotency_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let activation_id = activation_id(
        "storage-conformance",
        "activation-run",
        "",
        ActivationKind::Step as i32,
        "step/load",
    );
    let header = ActivationHeader {
        kind: ActivationKind::Step as i32,
        stable_key: "step/load".to_string(),
        recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
        input_digest: vec![1; 32],
        definition_digest: vec![2; 32],
        ..Default::default()
    };
    let started = |worker: &str, fence: &[u8]| {
        to_engine_record(
            &ActivationJournalRecord {
                version: ACTIVATION_JOURNAL_VERSION,
                event: Some(ActivationEvent::Started(ActivationStartedRecord {
                    project_id: "storage-conformance".to_string(),
                    run_id: "activation-run".to_string(),
                    parent_activation_id: String::new(),
                    activation_id: activation_id.clone(),
                    kind: ActivationKind::Step as i32,
                    stable_key: "step/load".to_string(),
                    input_digest: vec![1; 32],
                    definition_digest: vec![2; 32],
                    recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                    attempt: 1,
                    fence_token: fence.to_vec(),
                    worker_session_id: worker.to_string(),
                    run_authority: b"run-authority".to_vec(),
                    lease_authority: b"lease-authority".to_vec(),
                    previous_attempt: 0,
                    name: String::new(),
                    decision: "execute".to_string(),
                    input_data: None,
                    display_parent_correlation_id: None,
                    child: None,
                })),
            },
            1,
        )
        .expect("required contract: encode activation start")
    };

    let first = started("worker-a", b"fence-a");
    let racing = started("worker-b", b"fence-b");
    let admitted = segment
        .append_batch_with_outcome(std::slice::from_ref(&first))
        .await
        .expect("required contract: first activation begin");
    assert_eq!(admitted.offsets, [Offset(0)]);
    assert_eq!(admitted.appended, [true]);

    let loser = segment
        .append_batch_with_outcome(std::slice::from_ref(&racing))
        .await
        .expect("required contract: racing activation begin");
    assert_eq!(loser.offsets, [Offset(0)]);
    assert_eq!(loser.appended, [false]);

    let completion = |output: &[u8]| {
        let output = ActivationPayload {
            value: Some(ActivationValue::InlineData(output.to_vec())),
        };
        to_engine_record(
            &ActivationJournalRecord {
                version: ACTIVATION_JOURNAL_VERSION,
                event: Some(ActivationEvent::Completed(ActivationCompletedRecord {
                    project_id: "storage-conformance".to_string(),
                    run_id: "activation-run".to_string(),
                    activation_id: activation_id.clone(),
                    attempt: 1,
                    header: header.clone(),
                    started_journal_offset: 0,
                    fence_token: b"fence-a".to_vec(),
                    output_digest: payload_digest(&output)
                        .expect("required contract: completion output digest"),
                    output: Some(output),
                    state_mutations: vec![ActivationStateMutation {
                        operation_id: "state-op-1".to_string(),
                        entity_type: "workflow".to_string(),
                        entity_key: "state-a".to_string(),
                        scope: "run".to_string(),
                        scope_id: "activation-run".to_string(),
                        data: b"state-v1".to_vec(),
                        data_type: "json".to_string(),
                        expected_version: 0,
                    }],
                    outbox_intents: Vec::new(),
                    usage: None,
                    duration_ms: None,
                    evidence: Vec::new(),
                })),
            },
            2,
        )
        .expect("required contract: encode activation completion")
    };
    let accepted = completion(b"result-a");
    let exact = segment
        .append_batch_with_outcome(std::slice::from_ref(&accepted))
        .await
        .expect("required contract: activation completion");
    assert_eq!(exact.offsets, [Offset(1)]);
    assert_eq!(exact.appended, [true]);
    let replay = segment
        .append_batch_with_outcome(std::slice::from_ref(&accepted))
        .await
        .expect("required contract: exact activation completion retry");
    assert_eq!(replay.offsets, [Offset(1)]);
    assert_eq!(replay.appended, [false]);

    let conflict = segment
        .append_batch_with_outcome(&[completion(b"result-b")])
        .await;
    assert!(matches!(
        conflict,
        Err(EngineError::Activation(
            engine_core::error::ActivationError::PayloadConflict { .. }
        ))
    ));

    let stale_output = ActivationPayload {
        value: Some(ActivationValue::InlineData(b"other-result".to_vec())),
    };
    let stale_state = to_engine_record(
        &ActivationJournalRecord {
            version: ACTIVATION_JOURNAL_VERSION,
            event: Some(ActivationEvent::Completed(ActivationCompletedRecord {
                project_id: "storage-conformance".to_string(),
                run_id: "activation-run".to_string(),
                activation_id: "actv1_stale_state_writer".to_string(),
                attempt: 1,
                header: ActivationHeader {
                    kind: ActivationKind::Step as i32,
                    stable_key: "step/stale".to_string(),
                    recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                    input_digest: vec![1; 32],
                    definition_digest: vec![2; 32],
                    ..Default::default()
                },
                started_journal_offset: 0,
                fence_token: b"fence-stale".to_vec(),
                output_digest: payload_digest(&stale_output)
                    .expect("required contract: stale completion output digest"),
                output: Some(stale_output),
                state_mutations: vec![ActivationStateMutation {
                    operation_id: "state-op-stale".to_string(),
                    entity_type: "workflow".to_string(),
                    entity_key: "state-a".to_string(),
                    scope: "run".to_string(),
                    scope_id: "activation-run".to_string(),
                    data: b"stale-state".to_vec(),
                    data_type: "json".to_string(),
                    expected_version: 0,
                }],
                outbox_intents: Vec::new(),
                usage: None,
                duration_ms: None,
                evidence: Vec::new(),
            })),
        },
        3,
    )
    .expect("required contract: encode stale state completion");
    assert!(matches!(
        segment.append_batch_with_outcome(&[stale_state]).await,
        Err(EngineError::Activation(
            engine_core::error::ActivationError::StateVersionConflict { .. }
        ))
    ));
    assert_eq!(
        segment
            .tail_offset()
            .await
            .expect("required contract: activation tail after conflict"),
        Offset(2)
    );

    let suspend_header = ActivationHeader {
        kind: ActivationKind::Step as i32,
        stable_key: "step/suspend".to_string(),
        recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
        input_digest: vec![3; 32],
        definition_digest: vec![4; 32],
        ..Default::default()
    };
    let suspended_activation_id = engine_core::activation::activation_id(
        "storage-conformance",
        "activation-run",
        "",
        ActivationKind::Step as i32,
        "step/suspend",
    );
    let suspended_start = to_engine_record(
        &ActivationJournalRecord {
            version: ACTIVATION_JOURNAL_VERSION,
            event: Some(ActivationEvent::Started(ActivationStartedRecord {
                project_id: "storage-conformance".to_string(),
                run_id: "activation-run".to_string(),
                parent_activation_id: String::new(),
                activation_id: suspended_activation_id.clone(),
                kind: ActivationKind::Step as i32,
                stable_key: "step/suspend".to_string(),
                input_digest: vec![3; 32],
                definition_digest: vec![4; 32],
                recovery_policy: ActivationRecoveryPolicy::IdempotentRetry as i32,
                attempt: 1,
                fence_token: b"fence-suspend".to_vec(),
                worker_session_id: "worker-suspend".to_string(),
                run_authority: b"run-authority".to_vec(),
                lease_authority: b"lease-authority".to_vec(),
                previous_attempt: 0,
                name: String::new(),
                decision: "execute".to_string(),
                input_data: None,
                display_parent_correlation_id: None,
                child: None,
            })),
        },
        4,
    )
    .expect("required contract: encode suspended activation start");
    let suspended = to_engine_record(
        &ActivationJournalRecord {
            version: ACTIVATION_JOURNAL_VERSION,
            event: Some(ActivationEvent::Suspended(ActivationSuspendedRecord {
                project_id: "storage-conformance".to_string(),
                run_id: "activation-run".to_string(),
                activation_id: suspended_activation_id.clone(),
                attempt: 1,
                header: suspend_header.clone(),
                started_journal_offset: 0,
                fence_token: b"fence-suspend".to_vec(),
                resume_source: "approval:42".to_string(),
                ..Default::default()
            })),
        },
        5,
    )
    .expect("required contract: encode activation suspension");
    let cancelled = to_engine_record(
        &ActivationJournalRecord {
            version: ACTIVATION_JOURNAL_VERSION,
            event: Some(ActivationEvent::Cancelled(ActivationCancelledRecord {
                project_id: "storage-conformance".to_string(),
                run_id: "activation-run".to_string(),
                activation_id: suspended_activation_id,
                attempt: 1,
                header: suspend_header,
                reason: "parent cancelled".to_string(),
            })),
        },
        6,
    )
    .expect("required contract: encode activation cancellation");
    assert_eq!(
        segment
            .append_batch_with_outcome(&[suspended_start, suspended.clone()])
            .await
            .expect("required contract: activation suspension")
            .appended,
        [true, true]
    );
    let suspension_retry = segment
        .append_batch_with_outcome(&[suspended])
        .await
        .expect("required contract: exact suspension retry");
    assert_eq!(suspension_retry.offsets, [Offset(3)]);
    assert_eq!(suspension_retry.appended, [false]);
    let cancellation = segment
        .append_batch_with_outcome(&[cancelled])
        .await
        .expect("required contract: cancellation after suspension");
    assert_eq!(cancellation.offsets, [Offset(4)]);
    assert_eq!(cancellation.appended, [true]);
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(5));
}

async fn append_range_and_tail_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let records = [
        record("run-1", "run.queued"),
        record("run-1", "run.started"),
        record("run-1", "run.completed"),
    ];

    let offsets = segment
        .append_batch(&records)
        .await
        .expect("required contract: append batch");
    assert_eq!(offsets, [Offset(0), Offset(1), Offset(2)]);
    assert_eq!(
        segment
            .tail_offset()
            .await
            .expect("required contract: tail offset"),
        Offset(3)
    );

    let range = segment
        .read_range(Offset(1), Offset(3))
        .await
        .expect("required contract: read range");
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].offset, Offset(1));
    assert_eq!(range[0].event_type, "run.started");
    assert_eq!(range[1].offset, Offset(2));
    assert_eq!(range[1].event_type, "run.completed");
}

async fn append_outcome_idempotency_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let queued = record("idempotent-run", "run.queued");

    let first = segment
        .append_batch_with_outcome(std::slice::from_ref(&queued))
        .await
        .expect("required contract: first idempotent append");
    assert_eq!(first.offsets, [Offset(0)]);
    assert_eq!(first.appended, [true]);

    let duplicate = segment
        .append_batch_with_outcome(std::slice::from_ref(&queued))
        .await
        .expect("required contract: duplicate idempotent append");
    assert_eq!(duplicate.offsets, [Offset(0)]);
    assert_eq!(duplicate.appended, [false]);
    assert_eq!(
        segment
            .tail_offset()
            .await
            .expect("required contract: tail after duplicate"),
        Offset(1)
    );

    let committed = segment
        .read_range(Offset::ZERO, Offset(1))
        .await
        .expect("required contract: read idempotent result");
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].run_id, "idempotent-run");
}

async fn resumable_tail_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    segment
        .append_batch(&[
            record("tail-run", "first"),
            record("tail-run", "second"),
            record("tail-run", "third"),
        ])
        .await
        .expect("required contract: seed tail");

    let mut tail = segment
        .tail(Offset(1))
        .await
        .expect("required contract: open tail");
    for (offset, event_type) in [(Offset(1), "second"), (Offset(2), "third")] {
        let item = tokio::time::timeout(Duration::from_secs(1), tail.next())
            .await
            .expect("required contract: tail timed out")
            .expect("required contract: tail ended")
            .expect("required contract: tail returned an error");
        assert_eq!(item.offset, offset);
        assert_eq!(item.event_type, event_type);
    }
}

async fn empty_journal_readiness_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    assert_eq!(segment.partition_count(), 1);
    assert_eq!(
        segment
            .tail_offset()
            .await
            .expect("required contract: empty tail"),
        Offset::ZERO
    );
    assert_eq!(
        segment
            .retained_floor()
            .await
            .expect("required contract: retained floor"),
        Offset::ZERO
    );
    segment
        .check_readiness()
        .await
        .expect("required contract: readiness");
    assert!(segment
        .append_batch(&[])
        .await
        .expect("required contract: empty append")
        .is_empty());
}

async fn committed_restore_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    let mut first = record("restored-run", "run.queued");
    first.offset = Offset(0);
    let mut second = record("restored-run", "run.started");
    second.offset = Offset(1);

    segment
        .restore_committed_batch(&[first, second])
        .await
        .expect("restore capability: committed restore");
    assert_eq!(
        segment
            .tail_offset()
            .await
            .expect("restore capability: tail"),
        Offset(2)
    );
    let restored = segment
        .read_range(Offset::ZERO, Offset(2))
        .await
        .expect("restore capability: read restored range");
    assert_eq!(
        restored
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        [Offset(0), Offset(1)]
    );
}

async fn retention_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    segment
        .append_batch(&[
            record("retained-run", "first"),
            record("retained-run", "second"),
        ])
        .await
        .expect("retention capability: seed journal");
    segment
        .trim(Offset::ZERO)
        .await
        .expect("retention capability: trim");

    assert_eq!(
        segment
            .retained_floor()
            .await
            .expect("retention capability: retained floor"),
        Offset(1)
    );
    assert!(matches!(
        segment.read_range(Offset::ZERO, Offset(2)).await,
        Err(EngineError::Trimmed {
            requested: 0,
            retained_floor: 1
        })
    ));
    let retained = segment
        .read_range(Offset(1), Offset(2))
        .await
        .expect("retention capability: retained read");
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].event_type, "second");
}

async fn epoch_transition_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn Segment>, G),
{
    let (segment, _guard) = create();
    segment
        .append_batch(&[record("epoch-run", "before-seal")])
        .await
        .expect("epoch capability: seed journal");
    segment.seal().await.expect("epoch capability: seal");
    assert!(matches!(
        segment
            .append_batch(&[record("epoch-run", "while-sealed")])
            .await,
        Err(EngineError::Sealed)
    ));

    segment
        .prepare_for_new_epoch()
        .await
        .expect("epoch capability: prepare");
    assert_eq!(
        segment
            .append_batch(&[record("epoch-run", "next-epoch")])
            .await
            .expect("epoch capability: append after prepare"),
        [Offset(1)]
    );
}

/// Assert backend-neutral state, checkpoint, work-index, lease, timer, and
/// object-query behavior exposed by the compatibility bridge.
///
/// Atomic commits, outboxes, and cursor/provenance cases have separate entry
/// points because their factories must supply both read and commit ports.
pub async fn assert_materialized_store_contract<F, G>(create: F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    run_state_contract(&create).await;
    checkpoint_characterization(&create).await;
    pending_priority_contract(&create).await;
    lease_index_contract(&create).await;
    timer_index_contract(&create).await;
    projection_and_object_query_contract(&create).await;
}

/// Assert the semantic durable-work contracts consumed by processors,
/// drainers, pull workers, and lease recovery.
///
/// The factory returns both read/work and commit ports backed by the same
/// adapter instance. Cases only use backend-neutral mutations and opaque
/// acknowledgement tokens.
pub async fn assert_work_store_contract<F, G>(create: F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, Arc<dyn ProjectionCommitter>, G),
{
    dispatch_outbox_contract(&create).await;
    archive_outbox_contract(&create).await;
}

/// Assert query ordering, cursor resumption, and archive provenance through
/// semantic read ports.
pub async fn assert_query_store_contract<F, G>(create: F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, Arc<dyn ProjectionCommitter>, G),
{
    run_query_pagination_contract(&create).await;
    run_archive_provenance_contract(&create).await;
}

fn projection_commit(
    run_id: &str,
    offset: u64,
    mutations: Vec<ProjectionMutation>,
) -> ProjectionCommit {
    projection_commit_for("storage-conformance", run_id, offset, 1, mutations)
}

fn projection_commit_for(
    project_id: &str,
    run_id: &str,
    offset: u64,
    timestamp_ns: i64,
    mutations: Vec<ProjectionMutation>,
) -> ProjectionCommit {
    let mut source_record = record(run_id, "storage.conformance");
    source_record.project_id = project_id.to_string();
    source_record.offset = Offset(offset);
    source_record.timestamp_ns = timestamp_ns;
    ProjectionCommit::new(
        ProjectionSource::new(JournalPosition::new(0, Offset(offset)), source_record),
        mutations,
    )
}

async fn run_query_pagination_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, Arc<dyn ProjectionCommitter>, G),
{
    let (store, committer, _guard) = create();
    for (run_id, submitted_at_ms, offset) in [
        ("run-oldest", 100, 1),
        ("run-newest", 300, 2),
        ("run-middle", 200, 3),
    ] {
        committer
            .commit(projection_commit_for(
                "project-a",
                run_id,
                offset,
                submitted_at_ms * 1_000_000,
                vec![
                    ProjectionMutation::PutRun(run_id.as_bytes().to_vec()),
                    ProjectionMutation::PutRunHotIndex {
                        workspace_id: "workspace-a".to_string(),
                        submitted_at_ms,
                    },
                ],
            ))
            .await
            .expect("query contract: commit hot run");
    }

    let first =
        RunQueryStore::scan_hot_run_ids(store.as_ref(), "workspace-a", "project-a", None, 2)
            .await
            .expect("query contract: first hot-run page");
    assert_eq!(first, ["run-newest", "run-middle"]);
    let second = RunQueryStore::scan_hot_run_ids(
        store.as_ref(),
        "workspace-a",
        "project-a",
        Some((200, "run-middle".to_string())),
        2,
    )
    .await
    .expect("query contract: resumed hot-run page");
    assert_eq!(second, ["run-oldest"]);
    assert_eq!(
        RunQueryStore::load_run(store.as_ref(), "project-a", "run-newest")
            .await
            .expect("query contract: load indexed run"),
        Some(b"run-newest".to_vec())
    );

    for (run_id, timestamp_ms, offset) in [
        ("event-oldest", 100, 4),
        ("event-newest", 300, 5),
        ("event-middle", 200, 6),
    ] {
        committer
            .commit(projection_commit_for(
                "project-a",
                run_id,
                offset,
                timestamp_ms * 1_000_000,
                vec![ProjectionMutation::PutEventHotIndex {
                    timestamp_ms,
                    summary: run_id.as_bytes().to_vec(),
                }],
            ))
            .await
            .expect("query contract: commit event summary");
    }

    let first = RunQueryStore::scan_event_summaries(store.as_ref(), "project-a", None, 2)
        .await
        .expect("query contract: first event page");
    assert_eq!(
        first
            .iter()
            .map(|(run_id, timestamp_ms, _)| (run_id.as_str(), *timestamp_ms))
            .collect::<Vec<_>>(),
        [("event-newest", 300), ("event-middle", 200)]
    );
    let second = RunQueryStore::scan_event_summaries(
        store.as_ref(),
        "project-a",
        Some((200, "event-middle".to_string())),
        2,
    )
    .await
    .expect("query contract: resumed event page");
    assert_eq!(
        second
            .iter()
            .map(|(run_id, timestamp_ms, _)| (run_id.as_str(), *timestamp_ms))
            .collect::<Vec<_>>(),
        [("event-oldest", 100)]
    );
}

async fn run_archive_provenance_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, Arc<dyn ProjectionCommitter>, G),
{
    let (store, committer, _guard) = create();
    committer
        .commit(projection_commit_for(
            "project-a",
            "run-archived",
            1,
            1,
            vec![ProjectionMutation::PutRun(b"hot-v1".to_vec())],
        ))
        .await
        .expect("query contract: commit hot run");
    committer
        .commit(projection_commit_for(
            "project-a",
            "run-archived",
            2,
            2,
            vec![ProjectionMutation::PutRunArchiveLocation {
                project_id: "project-a".to_string(),
                run_id: "run-archived".to_string(),
                archive_location: "archive/workspace-a/day=2026-07-29".to_string(),
            }],
        ))
        .await
        .expect("query contract: publish archive provenance");
    assert_eq!(
        RunQueryStore::load_query_archive_path(store.as_ref(), "project-a", "run-archived")
            .await
            .expect("query contract: load archive provenance"),
        Some("archive/workspace-a/day=2026-07-29".to_string())
    );

    committer
        .commit(projection_commit_for(
            "project-a",
            "run-archived",
            3,
            3,
            vec![ProjectionMutation::PutRun(b"hot-v2".to_vec())],
        ))
        .await
        .expect("query contract: replace hot generation");
    assert_eq!(
        RunQueryStore::load_query_archive_path(store.as_ref(), "project-a", "run-archived")
            .await
            .expect("query contract: stale archive provenance cleared"),
        None
    );
}

async fn dispatch_outbox_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, Arc<dyn ProjectionCommitter>, G),
{
    let (store, committer, _guard) = create();
    for (source_offset, queued_offset, project_id) in [
        (1, 3, "project-a"),
        (2, 1, "project-z"),
        (3, 2, "project-m"),
    ] {
        let commit = projection_commit_for(
            project_id,
            &format!("dispatch-{queued_offset}"),
            source_offset,
            1,
            vec![ProjectionMutation::PutDispatchOutbox {
                partition_id: 0,
                queued_offset,
                request_data: format!("payload-{queued_offset}").into_bytes(),
            }],
        );
        committer
            .commit(commit.clone())
            .await
            .expect("dispatch contract: first commit");
        committer
            .commit(commit)
            .await
            .expect("dispatch contract: idempotent replay");
    }

    let first_page = DispatchOutboxStore::scan_dispatch_intents(store.as_ref(), 0, None, 2)
        .await
        .expect("dispatch contract: first page");
    assert_eq!(first_page.len(), 2);
    let cursor = engine_core::storage_contract::DispatchOutboxCursor::after(
        first_page
            .last()
            .expect("dispatch contract: full first page"),
    );
    let second_page =
        DispatchOutboxStore::scan_dispatch_intents(store.as_ref(), 0, Some(&cursor), 2)
            .await
            .expect("dispatch contract: continuation page");
    assert_eq!(second_page.len(), 1);
    assert!(matches!(
        &second_page[0],
        DispatchOutboxScanEntry::Ready(entry) if entry.queued_offset == 3
    ));
    let ready: Vec<_> = first_page
        .into_iter()
        .map(|entry| match entry {
            DispatchOutboxScanEntry::Ready(entry) => entry,
            DispatchOutboxScanEntry::CorruptKey { .. } => {
                panic!("dispatch contract: semantic commit produced corrupt key")
            }
        })
        .collect();
    assert_eq!(
        ready
            .iter()
            .map(|entry| (entry.queued_offset, entry.run_id.as_str()))
            .collect::<Vec<_>>(),
        [(1, "dispatch-1"), (2, "dispatch-2")]
    );

    DispatchOutboxStore::delete_dispatch_intent(store.as_ref(), &ready[0].token)
        .await
        .expect("dispatch contract: acknowledge");
    let after_ack = DispatchOutboxStore::scan_dispatch_intents(store.as_ref(), 0, None, 10)
        .await
        .expect("dispatch contract: scan after acknowledge");
    let offsets: Vec<_> = after_ack
        .into_iter()
        .map(|entry| match entry {
            DispatchOutboxScanEntry::Ready(entry) => entry.queued_offset,
            DispatchOutboxScanEntry::CorruptKey { .. } => {
                panic!("dispatch contract: semantic commit produced corrupt key")
            }
        })
        .collect();
    assert_eq!(offsets, [2, 3]);
}

async fn archive_outbox_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, Arc<dyn ProjectionCommitter>, G),
{
    let (store, committer, _guard) = create();
    for (source_offset, terminal_offset) in [(1, 9), (2, 7), (3, 8)] {
        let commit = projection_commit(
            &format!("archive-{terminal_offset}"),
            source_offset,
            vec![ProjectionMutation::PutRunArchiveIntent {
                partition_id: 0,
                terminal_offset,
                run_data: format!("run-{terminal_offset}").into_bytes(),
            }],
        );
        committer
            .commit(commit.clone())
            .await
            .expect("archive contract: first commit");
        committer
            .commit(commit)
            .await
            .expect("archive contract: idempotent replay");
    }

    let first_page = ArchiveOutboxStore::scan_archive_intents(store.as_ref(), 0, 2)
        .await
        .expect("archive contract: first page");
    assert_eq!(first_page.len(), 2);
    let ready: Vec<_> = first_page
        .into_iter()
        .map(|entry| match entry {
            ArchiveOutboxScanEntry::Ready(entry) => entry,
            ArchiveOutboxScanEntry::CorruptKey { .. } => {
                panic!("archive contract: semantic commit produced corrupt key")
            }
        })
        .collect();
    assert_eq!(
        ready
            .iter()
            .map(|entry| (entry.terminal_offset, entry.run_id.as_str()))
            .collect::<Vec<_>>(),
        [(7, "archive-7"), (8, "archive-8")]
    );

    let acknowledged = &ready[0];
    committer
        .commit(projection_commit(
            "archive-ack",
            4,
            vec![ProjectionMutation::DeleteRunArchiveIntent {
                partition_id: acknowledged.partition_id,
                project_id: acknowledged.project_id.clone(),
                terminal_offset: acknowledged.terminal_offset,
                run_id: acknowledged.run_id.clone(),
            }],
        ))
        .await
        .expect("archive contract: acknowledge");
    let after_ack = ArchiveOutboxStore::scan_archive_intents(store.as_ref(), 0, 10)
        .await
        .expect("archive contract: scan after acknowledge");
    let offsets: Vec<_> = after_ack
        .into_iter()
        .map(|entry| match entry {
            ArchiveOutboxScanEntry::Ready(entry) => entry.terminal_offset,
            ArchiveOutboxScanEntry::CorruptKey { .. } => {
                panic!("archive contract: semantic commit produced corrupt key")
            }
        })
        .collect();
    assert_eq!(offsets, [8, 9]);
}

async fn run_state_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    let (store, _guard) = create();
    assert_eq!(
        store
            .get_run("project-a", "run-a")
            .await
            .expect("run contract: initial read"),
        None
    );

    store
        .put_run("project-a", "run-a", b"first")
        .await
        .expect("run contract: first write");
    assert_eq!(
        store
            .get_run("project-a", "run-a")
            .await
            .expect("run contract: first read"),
        Some(b"first".to_vec())
    );

    store
        .put_run("project-a", "run-a", b"second")
        .await
        .expect("run contract: overwrite");
    assert_eq!(
        store
            .get_run("project-a", "run-a")
            .await
            .expect("run contract: overwritten read"),
        Some(b"second".to_vec())
    );
    assert_eq!(
        store
            .get_run("project-b", "run-a")
            .await
            .expect("run contract: project isolation"),
        None
    );
}

async fn projection_and_object_query_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    let (store, _guard) = create();
    store
        .put_run("project-a", "run-a", b"run")
        .await
        .expect("projection read contract: put run");
    store
        .put_entity_state(
            "project-a",
            "entity-type",
            "entity-key",
            "run",
            "run-a",
            b"state",
            7,
        )
        .await
        .expect("query contract: put entity state");
    store
        .put_approval("project-a", "approval-a", "run-a", b"approval")
        .await
        .expect("projection read contract: put approval");
    store
        .put_signal(
            "project-a",
            "signal-a",
            "run-a",
            "signal-name",
            "step-a",
            b"signal",
        )
        .await
        .expect("projection read contract: put signal");
    store
        .put_timer("project-a", "timer-a", "run-a", "scheduled", 100, b"timer")
        .await
        .expect("projection read contract: put timer");
    store
        .put_job("project-a", "job-a", "run-a", "batch-a", b"job")
        .await
        .expect("projection read contract: put job");
    store
        .put_batch("project-a", "batch-a", b"batch")
        .await
        .expect("projection read contract: put batch");
    store
        .put_session("project-a", "session-a", "component-a", b"session")
        .await
        .expect("projection read contract: put session");
    store
        .put_message("project-a", "message-a", "correlation-a", b"message")
        .await
        .expect("projection read contract: put message");

    assert_eq!(
        ProjectionStateReader::load_projection_run(store.as_ref(), "project-a", "run-a")
            .await
            .unwrap(),
        Some(b"run".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_approval(store.as_ref(), "project-a", "approval-a",)
            .await
            .unwrap(),
        Some(b"approval".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_signal(store.as_ref(), "project-a", "signal-a")
            .await
            .unwrap(),
        Some(b"signal".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_timer(store.as_ref(), "project-a", "timer-a")
            .await
            .unwrap(),
        Some(b"timer".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_job(store.as_ref(), "project-a", "job-a")
            .await
            .unwrap(),
        Some(b"job".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_batch(store.as_ref(), "project-a", "batch-a")
            .await
            .unwrap(),
        Some(b"batch".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_session(store.as_ref(), "project-a", "session-a")
            .await
            .unwrap(),
        Some(b"session".to_vec())
    );
    assert_eq!(
        ProjectionStateReader::load_projection_message(store.as_ref(), "project-a", "message-a")
            .await
            .unwrap(),
        Some(b"message".to_vec())
    );

    assert_eq!(
        WorkflowQueryStore::load_entity_state(
            store.as_ref(),
            "project-a",
            "entity-type",
            "entity-key",
            "run",
            "run-a",
        )
        .await
        .unwrap(),
        Some((b"state".to_vec(), 7))
    );
    assert_eq!(
        WorkflowQueryStore::scan_run_approvals(store.as_ref(), "project-a", "run-a")
            .await
            .unwrap(),
        [b"approval".to_vec()]
    );
    assert_eq!(
        WorkflowQueryStore::lookup_run_signal(
            store.as_ref(),
            "project-a",
            "run-a",
            "signal-name",
            "step-a",
        )
        .await
        .unwrap(),
        Some(b"signal".to_vec())
    );
    assert_eq!(
        WorkflowQueryStore::scan_run_timers(store.as_ref(), "project-a", "run-a")
            .await
            .unwrap(),
        [b"timer".to_vec()]
    );
    assert_eq!(
        WorkflowQueryStore::scan_batch_jobs(store.as_ref(), "project-a", "batch-a")
            .await
            .unwrap(),
        [b"job".to_vec()]
    );
    assert_eq!(
        SessionQueryStore::scan_component_sessions(store.as_ref(), "project-a", "component-a",)
            .await
            .unwrap(),
        [b"session".to_vec()]
    );
    assert_eq!(
        SessionQueryStore::scan_correlation_messages(store.as_ref(), "project-a", "correlation-a",)
            .await
            .unwrap(),
        [b"message".to_vec()]
    );
}

async fn checkpoint_characterization<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    let (store, _guard) = create();
    assert_eq!(
        store
            .get_checkpoint("processor-partition-0")
            .await
            .expect("checkpoint contract: initial read"),
        None
    );
    store
        .set_checkpoint("processor-partition-0", Offset(42))
        .await
        .expect("checkpoint contract: first write");
    store
        .set_checkpoint("processor-partition-1", Offset(7))
        .await
        .expect("checkpoint contract: isolated write");

    assert_eq!(
        store
            .get_checkpoint("processor-partition-0")
            .await
            .expect("checkpoint contract: first read"),
        Some(Offset(42))
    );
    assert_eq!(
        store
            .get_checkpoint("processor-partition-1")
            .await
            .expect("checkpoint contract: isolated read"),
        Some(Offset(7))
    );

    // Characterize the compatibility facade exactly: it currently accepts a
    // lower replacement for the same namespace. ProjectionCommitter must
    // close this gap before it becomes the production write path.
    store
        .set_checkpoint("processor-partition-0", Offset(11))
        .await
        .expect("checkpoint characterization: lower replacement");
    assert_eq!(
        store
            .get_checkpoint("processor-partition-0")
            .await
            .expect("checkpoint characterization: replaced read"),
        Some(Offset(11))
    );
}

async fn pending_priority_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    let (store, _guard) = create();
    store
        .put_pending_run("project-a", "component-a", "run-b", b"pending-b")
        .await
        .expect("pending contract: compatibility insert b");
    store
        .put_pending_run("project-a", "component-a", "run-a", b"pending-a")
        .await
        .expect("pending contract: compatibility insert a");
    assert_eq!(
        PendingWorkStore::scan_pending_runs(
            store.as_ref(),
            "project-a",
            &["component-a".to_string()],
            10,
        )
        .await
        .expect("pending contract: semantic compatibility scan"),
        [b"pending-a".to_vec(), b"pending-b".to_vec()]
    );

    for (priority, ready_at_ms, run_id) in [
        (3, 100, "run-batch"),
        (2, 100, "run-normal"),
        (1, 100, "run-interactive"),
        (1, 5_000, "run-delayed"),
    ] {
        store
            .put_pending_priority(
                "project-a",
                "deployment-a",
                "component-a",
                priority,
                ready_at_ms,
                10,
                run_id,
            )
            .await
            .expect("pending contract: insert");
    }
    store
        .put_pending_priority(
            "project-a",
            "deployment-a",
            "component-a",
            1,
            100,
            10,
            "run-interactive",
        )
        .await
        .expect("pending contract: idempotent insert");

    assert_eq!(
        store
            .list_pending_priority("project-a", "deployment-a", "component-a", 1_000, 10,)
            .await
            .expect("pending contract: ready ordering"),
        ["run-interactive", "run-normal", "run-batch"]
    );
    assert_eq!(
        store
            .list_pending_priority("project-a", "deployment-a", "component-a", 10_000, 2,)
            .await
            .expect("pending contract: bounded ordering"),
        ["run-interactive", "run-delayed"]
    );

    store
        .remove_pending_priority(
            "project-a",
            "deployment-a",
            "component-a",
            1,
            100,
            10,
            "run-interactive",
        )
        .await
        .expect("pending contract: remove");
    let remaining = store
        .list_pending_priority("project-a", "deployment-a", "component-a", 1_000, 10)
        .await
        .expect("pending contract: read after remove");
    assert_eq!(remaining, ["run-normal", "run-batch"]);

    for (priority, ready_at_ms, run_id) in [
        (3, 100, "deployment-batch"),
        (1, 5_000, "deployment-delayed"),
        (1, 100, "deployment-interactive"),
    ] {
        store
            .put_pending_deployment_priority(
                "project-a",
                "deployment-a",
                priority,
                ready_at_ms,
                10,
                run_id,
            )
            .await
            .expect("pending contract: deployment insert");
    }
    assert_eq!(
        PendingWorkStore::scan_ready_deployment_runs(
            store.as_ref(),
            "project-a",
            "deployment-a",
            1_000,
            10,
        )
        .await
        .expect("pending contract: semantic deployment scan"),
        ["deployment-interactive", "deployment-batch"]
    );
    let first_page = PendingWorkStore::scan_ready_deployment_candidates(
        store.as_ref(),
        "project-a",
        "deployment-a",
        1_000,
        None,
        1,
    )
    .await
    .expect("pending contract: candidate page");
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].cursor.run_id, "deployment-interactive");
    let second_page = PendingWorkStore::scan_ready_deployment_candidates(
        store.as_ref(),
        "project-a",
        "deployment-a",
        1_000,
        Some(&first_page[0].cursor),
        10,
    )
    .await
    .expect("pending contract: candidate page after cursor");
    assert_eq!(
        second_page
            .iter()
            .map(|candidate| candidate.cursor.run_id.as_str())
            .collect::<Vec<_>>(),
        ["deployment-batch"]
    );
}

async fn lease_index_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    let (store, _guard) = create();
    for (project_id, run_id, expires_at_ms, worker_id) in [
        ("project-a", "run-a", 100, "worker-a"),
        ("project-a", "run-b", 100, "worker-a"),
        ("project-b", "run-a", 200, "worker-b"),
    ] {
        store
            .put_lease_index(project_id, run_id, expires_at_ms, worker_id)
            .await
            .expect("lease contract: insert");
    }
    store
        .put_lease_index("project-a", "run-a", 100, "worker-a")
        .await
        .expect("lease contract: idempotent insert");

    let first_page = LeaseWorkStore::scan_expired_leases(store.as_ref(), 500, 2, None)
        .await
        .expect("lease contract: first page");
    assert_eq!(
        first_page,
        [
            (100, "project-a".to_string(), "run-a".to_string()),
            (100, "project-a".to_string(), "run-b".to_string()),
        ]
    );
    let second_page =
        LeaseWorkStore::scan_expired_leases(store.as_ref(), 500, 2, first_page.last().cloned())
            .await
            .expect("lease contract: second page");
    assert_eq!(
        second_page,
        [(200, "project-b".to_string(), "run-a".to_string())]
    );

    assert_eq!(
        LeaseWorkStore::scan_worker_leases(store.as_ref(), "worker-a")
            .await
            .expect("lease contract: worker index"),
        [
            ("project-a".to_string(), "run-a".to_string()),
            ("project-a".to_string(), "run-b".to_string()),
        ]
    );
    store
        .remove_lease_index("project-a", "run-a", 100, "worker-a")
        .await
        .expect("lease contract: remove");
    assert_eq!(
        LeaseWorkStore::scan_worker_leases(store.as_ref(), "worker-a")
            .await
            .expect("lease contract: worker index after remove"),
        [("project-a".to_string(), "run-b".to_string())]
    );
}

async fn timer_index_contract<F, G>(create: &F)
where
    F: Fn() -> (Arc<dyn MaterializedStore>, G),
{
    let (store, _guard) = create();
    for (timer_id, fire_at_ns, data) in [
        ("timer-3", 300, b"three".as_slice()),
        ("timer-1", 100, b"one".as_slice()),
        ("timer-2", 200, b"two".as_slice()),
    ] {
        store
            .put_timer(
                "project-a",
                timer_id,
                "run-a",
                "scheduled",
                fire_at_ns,
                data,
            )
            .await
            .expect("timer contract: insert");
    }

    assert_eq!(
        TimerWorkStore::scan_due_timers(store.as_ref(), 150, 10)
            .await
            .expect("timer contract: bounded due scan"),
        [b"one".to_vec()]
    );
    assert_eq!(
        TimerWorkStore::scan_due_timers(store.as_ref(), 500, 2)
            .await
            .expect("timer contract: ordered due scan"),
        [b"one".to_vec(), b"two".to_vec()]
    );

    store
        .put_timer("project-a", "timer-1", "run-a", "fired", 100, b"one-fired")
        .await
        .expect("timer contract: remove due index on terminal state");
    assert_eq!(
        TimerWorkStore::scan_due_timers(store.as_ref(), 150, 10)
            .await
            .expect("timer contract: due scan after firing"),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        store
            .get_timer("project-a", "timer-1")
            .await
            .expect("timer contract: primary state after firing"),
        Some(b"one-fired".to_vec())
    );
}
