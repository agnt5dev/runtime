use std::{sync::Arc, time::Duration};

use agnt5_coordinator::{Coordinator, CoordinatorConfig};
use agnt5_core::{KvOp, MaterializedStore, NewJournalRecord, Offset, RuntimeEvent, Segment};
use agnt5_postgres::{PostgresConfig, PostgresMaterializedStore, PostgresSegment, RuntimeLock};
use agnt5_processor::{decode_run, run_key, Processor, PENDING, RUNS};
use agnt5_proto::protocol::v2::{
    payload, poll_run_response, protocol_service_server::ProtocolService, run_outcome,
    worker_service_server::WorkerService, CommitDisposition, CommitRunOutcomeRequest,
    ComponentDescriptor, ComponentType, GetCapabilitiesRequest, Payload, PollRunRequest,
    ProtocolVersion, RegisterWorkerRequest, RenewRunLeaseRequest, RunCompleted, RunOutcome,
};
use bytes::Bytes;
use tonic::{Code, Request};

static DATABASE_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn database() -> (sqlx::PgPool, String) {
    let url = std::env::var("AGNT5_TEST_DATABASE_URL")
        .expect("AGNT5_TEST_DATABASE_URL is required for PostgreSQL integration tests");
    let config = PostgresConfig::new(url.clone());
    let pool = agnt5_postgres::connect(&config)
        .await
        .expect("connect to PostgreSQL test database");
    agnt5_postgres::migrate(&pool)
        .await
        .expect("run PostgreSQL test migrations");
    sqlx::query(
        "TRUNCATE agnt5_journal, agnt5_materialized, agnt5_checkpoints, agnt5_segments CASCADE",
    )
    .execute(&pool)
    .await
    .expect("reset PostgreSQL test tables");
    (pool, url)
}

#[tokio::test]
async fn append_is_contiguous_and_idempotent() {
    let _guard = DATABASE_TEST.lock().await;
    let (pool, _) = database().await;
    let segment = PostgresSegment::open(pool, 0).await.unwrap();
    let records = vec![
        NewJournalRecord {
            idempotency_key: Some(b"a".to_vec()),
            payload: Bytes::from_static(b"one"),
        },
        NewJournalRecord {
            idempotency_key: Some(b"b".to_vec()),
            payload: Bytes::from_static(b"two"),
        },
    ];
    let first = segment.append_batch(&records).await.unwrap();
    assert_eq!(first.offsets, vec![Offset(0), Offset(1)]);
    assert_eq!(first.appended, vec![true, true]);

    let retry = segment.append_batch(&records).await.unwrap();
    assert_eq!(retry.offsets, first.offsets);
    assert_eq!(retry.appended, vec![false, false]);
    assert_eq!(segment.tail_offset().await.unwrap(), Offset(2));
}

#[tokio::test]
async fn materialized_batch_and_checkpoint_round_trip() {
    let _guard = DATABASE_TEST.lock().await;
    let (pool, _) = database().await;
    let store = PostgresMaterializedStore::new(pool);
    store
        .write_batch(&[KvOp::Put {
            namespace: "runs".into(),
            key: b"run-1".to_vec(),
            value: b"complete".to_vec(),
        }])
        .await
        .unwrap();
    assert_eq!(
        store.get("runs", b"run-1").await.unwrap(),
        Some(b"complete".to_vec())
    );
    store
        .write_batch_and_checkpoint(
            &[KvOp::Put {
                namespace: "runs".into(),
                key: b"run-2".to_vec(),
                value: b"queued".to_vec(),
            }],
            "partition-0",
            Offset(7),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_checkpoint("partition-0").await.unwrap(),
        Some(Offset(7))
    );
    assert_eq!(
        store.scan_prefix("runs", b"run-", 10).await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn processor_recovers_from_checkpoint_and_projects_completion() {
    let _guard = DATABASE_TEST.lock().await;
    let (pool, _) = database().await;
    let segment = Arc::new(
        PostgresSegment::open(pool.clone(), 0)
            .await
            .unwrap()
            .with_poll_interval(Duration::from_millis(10)),
    );
    let store = Arc::new(PostgresMaterializedStore::new(pool));

    append_event(
        &segment,
        "submit:run-1",
        RuntimeEvent::RunQueued {
            project_id: "default".into(),
            run_id: "run-1".into(),
            component_type: "function".into(),
            component_name: "hello".into(),
            input_data: br#"{"name":"world"}"#.to_vec(),
            submitted_at_ms: 1,
        },
    )
    .await;
    let first_processor = Processor::new(Arc::clone(&segment), Arc::clone(&store));
    let first_task = tokio::spawn(async move { first_processor.run().await });
    wait_for_status(&store, "run-1", "queued").await;
    first_task.abort();

    append_event(
        &segment,
        "claim:run-1:lease-1",
        RuntimeEvent::JobClaimed {
            project_id: "default".into(),
            run_id: "run-1".into(),
            worker_id: "worker-1".into(),
            lease_id: "lease-1".into(),
            lease_expires_at_ms: 10_000,
        },
    )
    .await;
    append_event(
        &segment,
        "complete:run-1:lease-1",
        RuntimeEvent::JobCompleted {
            project_id: "default".into(),
            run_id: "run-1".into(),
            lease_id: "lease-1".into(),
            output_data: br#"{"message":"hello world"}"#.to_vec(),
            completed_at_ms: 2,
        },
    )
    .await;

    let recovered_processor = Processor::new(Arc::clone(&segment), Arc::clone(&store));
    let recovered_task = tokio::spawn(async move { recovered_processor.run().await });
    let run = wait_for_status(&store, "run-1", "completed").await;
    recovered_task.abort();

    assert_eq!(run.attempt, 1);
    assert_eq!(
        run.output_data,
        Some(br#"{"message":"hello world"}"#.to_vec())
    );
    assert!(store
        .scan_prefix(PENDING, b"", 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store.get_checkpoint("runtime-partition-0").await.unwrap(),
        Some(Offset(2))
    );
}

async fn append_event(segment: &PostgresSegment, key: &str, event: RuntimeEvent) {
    segment
        .append_batch(&[NewJournalRecord {
            idempotency_key: Some(key.as_bytes().to_vec()),
            payload: Bytes::from(serde_json::to_vec(&event).unwrap()),
        }])
        .await
        .unwrap();
}

async fn wait_for_status(
    store: &PostgresMaterializedStore,
    run_id: &str,
    status: &str,
) -> agnt5_core::RunState {
    for _ in 0..100 {
        if let Some(value) = store.get(RUNS, &run_key(run_id)).await.unwrap() {
            let run = decode_run(&value).unwrap();
            if run.status == status {
                return run;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {run_id} did not reach {status}");
}

#[tokio::test]
async fn only_one_runtime_lock_is_acquired() {
    let _guard = DATABASE_TEST.lock().await;
    let (_pool, url) = database().await;
    let first = RuntimeLock::acquire(&url).await.unwrap();
    assert!(RuntimeLock::acquire(&url).await.is_err());
    first.release().await.unwrap();
    let second = RuntimeLock::acquire(&url).await.unwrap();
    second.release().await.unwrap();
}

#[tokio::test]
async fn tail_observes_later_append() {
    use tokio_stream::StreamExt;

    let _guard = DATABASE_TEST.lock().await;
    let (pool, _) = database().await;
    let segment = PostgresSegment::open(pool, 0)
        .await
        .unwrap()
        .with_poll_interval(Duration::from_millis(10));
    let mut tail = segment.tail(Offset::ZERO).await.unwrap();
    segment
        .append_batch(&[NewJournalRecord {
            idempotency_key: None,
            payload: Bytes::from_static(b"later"),
        }])
        .await
        .unwrap();
    let record = tokio::time::timeout(Duration::from_secs(1), tail.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(record.payload, Bytes::from_static(b"later"));
}

#[tokio::test]
async fn v2_worker_bridge_replays_polls_fences_leases_and_commits_idempotently() {
    let _guard = DATABASE_TEST.lock().await;
    let (pool, _) = database().await;
    let segment = Arc::new(
        PostgresSegment::open(pool.clone(), 0)
            .await
            .unwrap()
            .with_poll_interval(Duration::from_millis(5)),
    );
    let store = Arc::new(PostgresMaterializedStore::new(pool));
    let processor = Processor::new(Arc::clone(&segment), Arc::clone(&store));
    let processor_task = tokio::spawn(async move { processor.run().await });
    let coordinator = Coordinator::with_config(
        "default",
        Arc::clone(&segment),
        Arc::clone(&store),
        CoordinatorConfig {
            session_lifetime: Duration::from_secs(60),
            lease_duration: Duration::from_millis(80),
            maximum_poll_wait: Duration::from_millis(20),
            recommended_renew_interval: Duration::from_millis(25),
        },
    );

    let capabilities = ProtocolService::get_capabilities(
        &coordinator,
        Request::new(GetCapabilitiesRequest {
            minimum_protocol: Some(ProtocolVersion { major: 2, minor: 0 }),
            maximum_protocol: Some(ProtocolVersion { major: 2, minor: 0 }),
            capabilities: Vec::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(capabilities.selected_protocol.unwrap().major, 2);
    assert!(capabilities.capabilities.is_empty());

    append_queued(&segment, "run-v2-1").await;
    wait_for_status(&store, "run-v2-1", "queued").await;
    let worker_one = register_v2(&coordinator, "worker-v2-1").await;
    let poll = PollRunRequest {
        worker_session_token: worker_one.clone(),
        poll_id: "poll-1".into(),
        wait_timeout: None,
    };
    let first = WorkerService::poll_run(&coordinator, Request::new(poll.clone()))
        .await
        .unwrap()
        .into_inner();
    let replay = WorkerService::poll_run(&coordinator, Request::new(poll))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, replay);
    let first_execute = execute(first);
    assert_eq!(first_execute.attempt, 1);

    let renewed = WorkerService::renew_run_lease(
        &coordinator,
        Request::new(RenewRunLeaseRequest {
            execution_token: first_execute.execution_token.clone(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(renewed.lease_expires_at.is_some());

    let commit = completed_commit(
        first_execute.execution_token.clone(),
        "commit-1",
        br#"{"ok":true}"#,
    );
    let committed = WorkerService::commit_run_outcome(&coordinator, Request::new(commit.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.disposition, CommitDisposition::Committed as i32);
    wait_for_status(&store, "run-v2-1", "completed").await;

    let duplicate = WorkerService::commit_run_outcome(&coordinator, Request::new(commit.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        duplicate.disposition,
        CommitDisposition::AlreadyCommitted as i32
    );
    let conflict = WorkerService::commit_run_outcome(
        &coordinator,
        Request::new(completed_commit(
            first_execute.execution_token,
            "commit-1",
            br#"{"ok":false}"#,
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.code(), Code::AlreadyExists);

    append_queued(&segment, "run-v2-2").await;
    wait_for_status(&store, "run-v2-2", "queued").await;
    let old_execution = execute(
        WorkerService::poll_run(
            &coordinator,
            Request::new(PollRunRequest {
                worker_session_token: worker_one.clone(),
                poll_id: "poll-2".into(),
                wait_timeout: None,
            }),
        )
        .await
        .unwrap()
        .into_inner(),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let worker_two = register_v2(&coordinator, "worker-v2-2").await;
    let redelivered = execute(
        WorkerService::poll_run(
            &coordinator,
            Request::new(PollRunRequest {
                worker_session_token: worker_two,
                poll_id: "poll-redelivery".into(),
                wait_timeout: None,
            }),
        )
        .await
        .unwrap()
        .into_inner(),
    );
    assert_eq!(redelivered.run_id, "run-v2-2");
    assert_eq!(redelivered.attempt, 1);
    assert_ne!(redelivered.execution_token, old_execution.execution_token);

    let stale_renewal = WorkerService::renew_run_lease(
        &coordinator,
        Request::new(RenewRunLeaseRequest {
            execution_token: old_execution.execution_token.clone(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stale_renewal.code(), Code::FailedPrecondition);
    let stale_commit = WorkerService::commit_run_outcome(
        &coordinator,
        Request::new(completed_commit(
            old_execution.execution_token,
            "stale-commit",
            br#"{"stale":true}"#,
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(stale_commit.code(), Code::FailedPrecondition);

    WorkerService::commit_run_outcome(
        &coordinator,
        Request::new(completed_commit(
            redelivered.execution_token,
            "commit-redelivery",
            br#"{"redelivered":true}"#,
        )),
    )
    .await
    .unwrap();
    let completed = wait_for_status(&store, "run-v2-2", "completed").await;
    assert_eq!(completed.attempt, 1);

    let _replacement = register_v2(&coordinator, "worker-v2-1").await;
    let fenced = WorkerService::poll_run(
        &coordinator,
        Request::new(PollRunRequest {
            worker_session_token: worker_one,
            poll_id: "poll-fenced".into(),
            wait_timeout: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(fenced.code(), Code::Unauthenticated);

    processor_task.abort();
}

async fn append_queued(segment: &PostgresSegment, run_id: &str) {
    append_event(
        segment,
        &format!("submit:{run_id}"),
        RuntimeEvent::RunQueued {
            project_id: "default".into(),
            run_id: run_id.into(),
            component_type: "function".into(),
            component_name: "hello".into(),
            input_data: br#"{"name":"world"}"#.to_vec(),
            submitted_at_ms: 1,
        },
    )
    .await;
}

async fn register_v2(
    coordinator: &Coordinator<PostgresSegment, PostgresMaterializedStore>,
    worker_id: &str,
) -> Vec<u8> {
    WorkerService::register_worker(
        coordinator,
        Request::new(RegisterWorkerRequest {
            worker_id: worker_id.into(),
            service_name: "test-worker".into(),
            service_version: "1".into(),
            sdk_language: "rust".into(),
            sdk_version: "test".into(),
            minimum_protocol: Some(ProtocolVersion { major: 2, minor: 0 }),
            maximum_protocol: Some(ProtocolVersion { major: 2, minor: 0 }),
            capabilities: Vec::new(),
            components: vec![ComponentDescriptor {
                r#type: ComponentType::Function as i32,
                name: "hello".into(),
                version: "v1".into(),
                ..Default::default()
            }],
            max_concurrency: 1,
            metadata: Default::default(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .worker_session_token
}

fn execute(
    response: agnt5_proto::protocol::v2::PollRunResponse,
) -> Box<agnt5_proto::protocol::v2::ExecuteRunRequest> {
    match response.result.unwrap() {
        poll_run_response::Result::Execute(execute) => execute,
        poll_run_response::Result::Idle(_) => panic!("expected an execute response"),
    }
}

fn completed_commit(token: Vec<u8>, commit_id: &str, output: &[u8]) -> CommitRunOutcomeRequest {
    CommitRunOutcomeRequest {
        execution_token: token,
        commit_id: commit_id.into(),
        outcome: Some(RunOutcome {
            kind: Some(run_outcome::Kind::Completed(RunCompleted {
                output: Some(Payload {
                    body: Some(payload::Body::InlineData(output.to_vec())),
                    content_type: "application/json".into(),
                    content_encoding: String::new(),
                    size_bytes: output.len() as u64,
                    sha256: Vec::new(),
                }),
                metadata: Default::default(),
            })),
        }),
        final_events: Vec::new(),
        expected_last_event_sequence: None,
    }
}
