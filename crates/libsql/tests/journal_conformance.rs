use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use engine_core::lease_fence::{LeaseFence, LeaseFenceKind, RUN_META_DISPATCH_GENERATION};
use engine_core::storage_contract::{ProjectionCommitter, StorageCapabilities, StorageCapability};
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_libsql::{LibsqlDatabase, LibsqlStorage, LibsqlWriterLease};
use tempfile::TempDir;
use uuid::Uuid;

async fn test_storage(prefix: &str) -> (TempDir, LibsqlStorage) {
    let directory = tempfile::tempdir().expect("create SQLite test directory");
    let storage = LibsqlStorage::connect(
        LibsqlDatabase::local(directory.path().join("runtime.db")),
        format!("{prefix}-{}", Uuid::new_v4()),
    )
    .await
    .expect("connect local SQLite storage");
    (directory, storage)
}

fn record(run_id: &str) -> Record {
    Record {
        offset: Offset::ZERO,
        project_id: "libsql-conformance".to_string(),
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
async fn sqlite_required_journal_contract() {
    let (_directory, root) = test_storage("journal-contract").await;
    let next_partition = Arc::new(AtomicU32::new(10_000));
    engine_storage_conformance::assert_journal_contract(
        {
            let root = root.clone();
            let next_partition = next_partition.clone();
            move || {
                let storage = root
                    .with_namespace(format!("test-{}", Uuid::new_v4()))
                    .expect("isolated test storage");
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
async fn sqlite_authority_backfill_folds_legacy_mixed_assignment_generations() {
    let (_directory, storage) = test_storage("mixed-generation-backfill").await;
    let namespace = storage.namespace().to_string();
    let journal = storage.journal(0);
    journal
        .append_batch(&[assignment("mixed-generation-backfill", "lease-1", "gen-1")])
        .await
        .unwrap();
    journal
        .append_replicated_batch(&[assignment("mixed-generation-backfill", "lease-2", "gen-2")])
        .await
        .expect("trusted follower append accepts legacy mixed generations");

    storage
        .connection()
        .execute(
            "DELETE FROM agnt5_runtime_activation_run_leases WHERE namespace = ?1 AND partition_id = 0",
            libsql::params![namespace.clone()],
        )
        .await
        .unwrap();
    storage
        .connection()
        .execute(
            "DELETE FROM agnt5_runtime_activation_authority_watermarks WHERE namespace = ?1 AND partition_id = 0",
            libsql::params![namespace],
        )
        .await
        .unwrap();

    let offsets = storage
        .journal(0)
        .append_batch(&[completion("mixed-generation-backfill", "lease-2")])
        .await
        .expect("backfill must install the last leader-accepted assignment");
    assert_eq!(offsets, [Offset(2)]);
}

#[tokio::test]
async fn concurrent_append_allocates_one_contiguous_sequence() {
    let (_directory, storage) = test_storage("concurrent-append").await;
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
        "immediate write transactions must allocate a contiguous sequence"
    );
}

#[tokio::test]
async fn concurrent_idempotent_retry_returns_original_offset_once() {
    let (_directory, storage) = test_storage("concurrent-retry").await;
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
}

#[tokio::test]
async fn sqlite_required_storage_port_contract() {
    let (_directory, root) = test_storage("storage-contract").await;
    engine_storage_conformance::assert_required_storage_port_contract(|| {
        let storage = Arc::new(
            root.with_namespace(format!("test-{}", Uuid::new_v4()))
                .expect("isolated test storage"),
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
async fn sqlite_refuses_agent_instances() {
    let (_directory, root) = test_storage("instances").await;
    let storage = Arc::new(
        root.with_namespace(format!("test-{}", Uuid::new_v4()))
            .expect("isolated test storage"),
    );
    let committer: Arc<dyn ProjectionCommitter> = storage.clone();
    let ports =
        engine_storage_conformance::RequiredStoragePorts::from_shared(storage.clone(), committer);
    engine_storage_conformance::assert_agent_instances_unsupported(&ports, storage.as_ref()).await;
}

#[tokio::test]
async fn writer_lease_enforces_one_active_runtime_per_namespace() {
    let (_directory, storage) = test_storage("writer-lease").await;
    let first = LibsqlWriterLease::acquire(&storage, "runtime-1", Duration::from_secs(30))
        .await
        .expect("acquire first writer lease");
    assert!(first.generation() > 0);
    let duplicate =
        LibsqlWriterLease::acquire(&storage, "runtime-2", Duration::from_secs(30)).await;
    assert!(
        duplicate.is_err(),
        "a second runtime must not acquire the same namespace"
    );

    let independent_storage = storage
        .with_namespace(format!("{}-independent", storage.namespace()))
        .expect("independent namespace storage");
    let independent =
        LibsqlWriterLease::acquire(&independent_storage, "runtime-2", Duration::from_secs(30))
            .await
            .expect("acquire independent namespace");
    independent
        .release()
        .await
        .expect("release independent lease");
    first.release().await.expect("release first writer lease");

    LibsqlWriterLease::acquire(&storage, "runtime-3", Duration::from_secs(30))
        .await
        .expect("reacquire namespace after release")
        .release()
        .await
        .expect("release reacquired lease");
}
