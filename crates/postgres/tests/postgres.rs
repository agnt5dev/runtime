use std::time::Duration;

use agnt5_core::{KvOp, MaterializedStore, NewJournalRecord, Offset, Segment};
use agnt5_postgres::{PostgresConfig, PostgresMaterializedStore, PostgresSegment, RuntimeLock};
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
        .set_checkpoint("partition-0", Offset(7))
        .await
        .unwrap();
    assert_eq!(
        store.get_checkpoint("partition-0").await.unwrap(),
        Some(Offset(7))
    );
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
