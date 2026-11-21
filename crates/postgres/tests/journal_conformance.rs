use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use engine_core::lease_fence::{LeaseFence, LeaseFenceKind, RUN_META_DISPATCH_GENERATION};
use engine_core::storage_contract::{ProjectionCommitter, StorageCapabilities, StorageCapability};
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_postgres::{PostgresStorage, PostgresWriterLock};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn test_database_url() -> String {
    std::env::var("AGNT5_POSTGRES_TEST_URL").unwrap_or_else(|_| {
        "postgresql://agnt5:agnt5_dev_password@localhost:5432/agnt5?sslmode=disable".to_string()
    })
}

async fn test_pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(16)
        .connect(&test_database_url())
        .await
        .expect("connect to AGNT5 PostgreSQL test database")
}

fn record(run_id: &str) -> Record {
    Record {
        offset: Offset::ZERO,
        project_id: "postgres-conformance".to_string(),
        run_id: run_id.to_string(),
        event_type: "run.queued".to_string(),
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

fn assignment(run_id: &str, lease_id: &str, generation: &str) -> Record {
    let mut assignment = record(run_id);
    assignment.event_type = "run.assigned".to_string();
    assignment.metadata.extend([
        ("lease_id".to_string(), lease_id.to_string()),
        ("lease_attempt".to_string(), "0".to_string()),
        ("lease_worker_id".to_string(), "worker-1".to_string()),
        ("lease_session_hash".to_string(), "session-1".to_string()),
        ("lease_expires_at_ms".to_string(), "60000".to_string()),
        ("lease_timeout_ms".to_string(), "15000".to_string()),
        (
            RUN_META_DISPATCH_GENERATION.to_string(),
            generation.to_string(),
        ),
    ]);
    assignment
}

fn completion(run_id: &str, lease_id: &str) -> Record {
    let mut completion = record(run_id);
    completion.event_type = "run.completed".to_string();
    LeaseFence {
        kind: LeaseFenceKind::PullCompletion,
        lease_id: lease_id.to_string(),
        worker_id: "worker-1".to_string(),
        session_hash: "session-1".to_string(),
        attempt: 0,
        received_at_ms: 1,
        expiry_reason: None,
    }
    .stamp(&mut completion);
    completion
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn postgres_required_journal_contract() {
    let pool = test_pool().await;
    PostgresStorage::migrate(&pool)
        .await
        .expect("migrate PostgreSQL runtime schema");
    let next_partition = Arc::new(AtomicU32::new(10_000));
    engine_storage_conformance::assert_journal_contract(
        {
            let pool = pool.clone();
            let next_partition = next_partition.clone();
            move || {
                let namespace = format!("test-{}", Uuid::new_v4());
                let storage =
                    PostgresStorage::from_pool(pool.clone(), namespace).expect("test storage");
                let partition = next_partition.fetch_add(1, Ordering::Relaxed);
                (
                    Arc::new(storage.journal(partition)) as Arc<dyn Segment>,
                    storage,
                )
            }
        },
        &StorageCapabilities::none()
            .with(StorageCapability::JournalRetention)
            .with(StorageCapability::JournalEpochTransition),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn postgres_authority_backfill_folds_legacy_mixed_assignment_generations() {
    let pool = test_pool().await;
    PostgresStorage::migrate(&pool)
        .await
        .expect("migrate PostgreSQL runtime schema");
    let namespace = format!("test-{}", Uuid::new_v4());
    let storage = PostgresStorage::from_pool(pool.clone(), namespace.clone()).unwrap();
    let journal = storage.journal(0);
    journal
        .append_batch(&[assignment("mixed-generation-backfill", "lease-1", "gen-1")])
        .await
        .unwrap();
    journal
        .append_replicated_batch(&[assignment("mixed-generation-backfill", "lease-2", "gen-2")])
        .await
        .expect("trusted follower append accepts legacy mixed generations");

    sqlx::query(
        "DELETE FROM agnt5_runtime.activation_run_leases WHERE namespace = $1 AND partition_id = 0",
    )
    .bind(&namespace)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "DELETE FROM agnt5_runtime.activation_authority_watermarks WHERE namespace = $1 AND partition_id = 0",
    )
    .bind(&namespace)
    .execute(&pool)
    .await
    .unwrap();

    let reopened = PostgresStorage::from_pool(pool, namespace).unwrap();
    let offsets = reopened
        .journal(0)
        .append_batch(&[completion("mixed-generation-backfill", "lease-2")])
        .await
        .expect("backfill must install the last leader-accepted assignment");
    assert_eq!(offsets, [Offset(2)]);
    reopened.delete_namespace().await.unwrap();
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn concurrent_append_allocates_one_contiguous_sequence() {
    let storage =
        PostgresStorage::connect(&test_database_url(), format!("test-{}", Uuid::new_v4()), 16)
            .await
            .expect("connect PostgreSQL storage");
    let journal = Arc::new(storage.journal(0));
    let mut tasks = Vec::new();
    for index in 0..32 {
        let journal = journal.clone();
        tasks.push(tokio::spawn(async move {
            let mut record = record(&format!("run-{index}"));
            record.event_type = "custom.event".to_string();
            journal.append_batch(&[record]).await.unwrap()[0]
        }));
    }
    let mut offsets = Vec::new();
    for task in tasks {
        offsets.push(task.await.expect("append task"));
    }
    offsets.sort();
    assert_eq!(
        offsets,
        (0..32).map(Offset).collect::<Vec<_>>(),
        "partition-row locking must allocate a contiguous sequence"
    );
    storage.delete_namespace().await.unwrap();
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn concurrent_idempotent_retry_returns_original_offset_once() {
    let storage =
        PostgresStorage::connect(&test_database_url(), format!("test-{}", Uuid::new_v4()), 16)
            .await
            .expect("connect PostgreSQL storage");
    let journal = Arc::new(storage.journal(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let journal = journal.clone();
        tasks.push(tokio::spawn(async move {
            journal
                .append_batch_with_outcome(&[record("same-run")])
                .await
                .unwrap()
        }));
    }
    let mut appended = 0;
    for task in tasks {
        let outcome = task.await.expect("idempotent append task");
        assert_eq!(outcome.offsets, [Offset::ZERO]);
        appended += outcome.appended[0] as usize;
    }
    assert_eq!(appended, 1);
    assert_eq!(journal.tail_offset().await.unwrap(), Offset(1));
    storage.delete_namespace().await.unwrap();
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn postgres_required_storage_port_contract() {
    let pool = test_pool().await;
    PostgresStorage::migrate(&pool)
        .await
        .expect("migrate PostgreSQL runtime schema");
    engine_storage_conformance::assert_required_storage_port_contract(|| {
        let storage = Arc::new(
            PostgresStorage::from_pool(pool.clone(), format!("test-{}", Uuid::new_v4()))
                .expect("test storage"),
        );
        let committer: Arc<dyn ProjectionCommitter> = storage.clone();
        (
            engine_storage_conformance::RequiredStoragePorts::from_shared(
                storage.clone(),
                committer,
            ),
            storage,
        )
    })
    .await;
}

// The open-source runtime keeps no always-on agent instances.
#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn postgres_refuses_agent_instances() {
    let pool = test_pool().await;
    PostgresStorage::migrate(&pool)
        .await
        .expect("migrate PostgreSQL runtime schema");
    let storage = Arc::new(
        PostgresStorage::from_pool(pool.clone(), format!("test-{}", Uuid::new_v4()))
            .expect("test storage"),
    );
    let committer: Arc<dyn ProjectionCommitter> = storage.clone();
    let ports =
        engine_storage_conformance::RequiredStoragePorts::from_shared(storage.clone(), committer);
    engine_storage_conformance::assert_agent_instances_unsupported(&ports, storage.as_ref()).await;
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn dedicated_writer_lock_enforces_one_active_runtime_per_namespace() {
    let database_url = test_database_url();
    let namespace = format!("writer-lock-{}", Uuid::new_v4());
    let first = PostgresWriterLock::acquire(&database_url, namespace.clone())
        .await
        .expect("acquire first writer lock");
    assert_eq!(first.namespace(), namespace);
    let duplicate = PostgresWriterLock::acquire(&database_url, namespace.clone()).await;
    assert!(
        duplicate.is_err(),
        "a second runtime must not acquire the same namespace"
    );

    let independent =
        PostgresWriterLock::acquire(&database_url, format!("{namespace}-independent"))
            .await
            .expect("acquire independent namespace");
    independent
        .release()
        .await
        .expect("release independent lock");
    first.release().await.expect("release first writer lock");

    PostgresWriterLock::acquire(&database_url, namespace)
        .await
        .expect("reacquire namespace after release")
        .release()
        .await
        .expect("release reacquired lock");
}

#[tokio::test]
#[ignore = "requires AGNT5_POSTGRES_TEST_URL or the local AGNT5 PostgreSQL container"]
async fn writer_lock_check_fails_once_its_session_is_gone() {
    let database_url = test_database_url();
    let namespace = format!("writer-lock-loss-{}", Uuid::new_v4());
    let lock = PostgresWriterLock::acquire(&database_url, namespace.clone())
        .await
        .expect("acquire writer lock");
    lock.verify()
        .await
        .expect("a live session still holds the lock");

    let pid = lock.session_pid().await.expect("read lock session pid");
    let pool = test_pool().await;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&pool)
        .await
        .expect("terminate the lock session");
    assert!(terminated, "the lock session should have been terminated");

    assert!(
        lock.verify().await.is_err(),
        "a dropped session must be reported as a lost lock"
    );
    // PostgreSQL released the lock with the session, so another runtime can
    // now take it; that is exactly why the holder must stop.
    PostgresWriterLock::acquire(&database_url, namespace)
        .await
        .expect("the namespace is free once the session is gone")
        .release()
        .await
        .expect("release the new holder");
}
