use std::{sync::Arc, time::Duration};

use agnt5_core::{KvOp, MaterializedStore, NewJournalRecord, Offset, RuntimeEvent, Segment};
use agnt5_postgres::{PostgresConfig, PostgresMaterializedStore, PostgresSegment, RuntimeLock};
use agnt5_processor::{decode_run, run_key, Processor, PENDING, RUNS};
use bytes::Bytes;

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
