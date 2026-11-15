use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use engine_core::storage_contract::{
    DispatchOutboxStore, JournalPosition, ProjectionCheckpointStore, ProjectionCommit,
    ProjectionCommitter, ProjectionMutation, ProjectionSource, ProjectionStateReader,
    RunQueryStore, TerminalObligationStore, WorkflowQueryStore, ACTIVATION_OUTBOX_ENTITY_TYPE,
    ACTIVATION_PROJECTION_SCOPE,
};
use engine_core::traits::Segment;
use engine_core::types::{Offset, Record};
use engine_libsql::{LibsqlDatabase, LibsqlStorage};
use engine_processor::{run_single_partition_processor, PartitionProcessorExit};
use engine_proto::Run;
use prost::Message;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CHECKPOINT: &str = "processor-partition-0";

fn activation_atomic_ops(dispatch_offset: u64) -> Vec<ProjectionMutation> {
    vec![
        ProjectionMutation::PutRun(b"projected-run".to_vec()),
        ProjectionMutation::PutTerminalObligation {
            partition_id: 0,
            run_data: b"projected-run".to_vec(),
        },
        ProjectionMutation::PutEvent,
        ProjectionMutation::PutActivation {
            activation_id: "activation-a".to_string(),
            data: b"activation-snapshot".to_vec(),
            version: 1,
        },
        ProjectionMutation::PutStepMemo {
            step_key: "step/a".to_string(),
            data: b"activation-result".to_vec(),
        },
        ProjectionMutation::PutEntityState {
            entity_type: "workflow".to_string(),
            entity_key: "state-a".to_string(),
            scope: "run".to_string(),
            scope_id: "atomic-run".to_string(),
            data: b"state-v1".to_vec(),
            version: 1,
        },
        ProjectionMutation::PutActivationOutbox {
            activation_id: "activation-a".to_string(),
            attempt: 1,
            intent_id: "intent-a".to_string(),
            data: b"outbox-v1".to_vec(),
        },
        ProjectionMutation::PutDispatchOutbox {
            partition_id: 0,
            queued_offset: dispatch_offset,
            request_data: b"dispatch".to_vec(),
        },
    ]
}

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

fn record(run_id: &str, event_type: &str) -> Record {
    let mut metadata = HashMap::new();
    metadata.insert("component_name".to_string(), "crash-test".to_string());
    metadata.insert("component_type".to_string(), "function".to_string());
    metadata.insert("deployment_id".to_string(), "crash-deployment".to_string());
    metadata.insert("workspace_id".to_string(), "crash-workspace".to_string());
    metadata.insert("dispatch_mode".to_string(), "pull".to_string());
    metadata.insert("priority".to_string(), "3".to_string());
    Record {
        offset: Offset::ZERO,
        project_id: "crash-project".to_string(),
        run_id: run_id.to_string(),
        event_type: event_type.to_string(),
        data: Bytes::from_static(br#"{"input":"durable"}"#),
        timestamp_ns: 1_785_484_000_000_000_000,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

#[tokio::test]
async fn journal_transaction_rolls_back_an_interrupted_batch_without_a_gap() {
    let (_directory, storage) = test_storage("crash-journal").await;
    let journal = storage.journal(0);
    journal
        .tail_offset()
        .await
        .expect("create journal partition");

    // Place the allocator at libSQL's last representable offset. The
    // first row can be inserted, but allocating the second row fails while
    // the transaction is still open. This deterministically models process
    // loss before append commit without adding a production failpoint.
    storage
        .connection()
        .execute(
            r#"
        UPDATE agnt5_runtime_journal_partitions
        SET next_offset = ?3
        WHERE namespace = ?1 AND partition_id = ?2
        "#,
            libsql::params![storage.namespace(), 0_i32, i64::MAX],
        )
        .await
        .expect("position test allocator");

    let result = journal
        .append_batch(&[
            record("interrupted-a", "custom.event"),
            record("interrupted-b", "custom.event"),
        ])
        .await;
    assert!(result.is_err(), "second allocation must exceed BIGINT");
    assert_eq!(
        journal
            .tail_offset()
            .await
            .expect("read allocator after abort"),
        Offset(i64::MAX as u64),
        "failed transaction must not advance the allocator"
    );
    let mut rows = storage
        .connection()
        .query(
            r#"
        SELECT COUNT(*)
        FROM agnt5_runtime_journal_records
        WHERE namespace = ?1 AND partition_id = ?2 AND journal_offset = ?3
        "#,
            libsql::params![storage.namespace(), 0_i32, i64::MAX],
        )
        .await
        .expect("count interrupted rows");
    let row = rows
        .next()
        .await
        .expect("read count row")
        .expect("count row");
    assert_eq!(
        row.get::<i64>(0).expect("decode count"),
        0,
        "the first row must roll back with the batch"
    );
    storage
        .delete_namespace()
        .await
        .expect("delete isolated test namespace");
}

#[tokio::test]
async fn projection_transaction_is_all_or_nothing_and_retry_idempotent() {
    let (_directory, storage) = test_storage("crash-projection").await;
    let storage = Arc::new(storage);
    let mut source_record = record("atomic-run", "run.queued");
    source_record.offset = Offset(0);
    let source = ProjectionSource::new(JournalPosition::new(0, Offset(0)), source_record.clone());
    let interrupted = ProjectionCommit::new(source.clone(), activation_atomic_ops(u64::MAX))
        .advancing_checkpoint();
    assert!(
        storage.commit(interrupted).await.is_err(),
        "unrepresentable outbox offset must abort after earlier SQL mutations"
    );
    assert_eq!(
        storage
            .load_projection_run("crash-project", "atomic-run")
            .await
            .expect("load run after abort"),
        None
    );
    assert!(storage
        .load_run_events("crash-project", "atomic-run", 0)
        .await
        .expect("load events after abort")
        .is_empty());
    assert_eq!(
        storage
            .load_projection_activation("crash-project", "atomic-run", "activation-a")
            .await
            .expect("load activation after abort"),
        None
    );
    assert_eq!(
        storage
            .load_entity_state("crash-project", "workflow", "state-a", "run", "atomic-run")
            .await
            .expect("load state after abort"),
        None
    );
    assert_eq!(
        storage
            .load_entity_state(
                "crash-project",
                ACTIVATION_OUTBOX_ENTITY_TYPE,
                "activation-a/1/intent-a",
                ACTIVATION_PROJECTION_SCOPE,
                "atomic-run",
            )
            .await
            .expect("load activation outbox after abort"),
        None
    );
    assert_eq!(
        storage
            .load_projection_checkpoint(CHECKPOINT)
            .await
            .expect("load checkpoint after abort"),
        None
    );
    assert!(storage
        .scan_terminal_obligations(0, 10, None)
        .await
        .expect("scan obligations after abort")
        .is_empty());

    let retry = ProjectionCommit::new(source, activation_atomic_ops(0)).advancing_checkpoint();
    storage
        .commit(retry.clone())
        .await
        .expect("retry projection commit");
    storage
        .commit(retry)
        .await
        .expect("lost-ack projection retry");
    assert_eq!(
        storage
            .load_projection_run("crash-project", "atomic-run")
            .await
            .expect("load committed run"),
        Some(b"projected-run".to_vec())
    );
    assert_eq!(
        storage
            .load_projection_activation("crash-project", "atomic-run", "activation-a")
            .await
            .expect("load committed activation"),
        Some(b"activation-snapshot".to_vec())
    );
    let obligations = storage
        .scan_terminal_obligations(0, 10, None)
        .await
        .expect("scan committed obligations");
    assert_eq!(obligations.len(), 1);
    assert_eq!(obligations[0].project_id, "crash-project");
    assert_eq!(obligations[0].run_id, "atomic-run");
    assert_eq!(obligations[0].run_data, b"projected-run".to_vec());
    assert_eq!(
        storage
            .load_entity_state("crash-project", "workflow", "state-a", "run", "atomic-run")
            .await
            .expect("load committed state"),
        Some((b"state-v1".to_vec(), 1))
    );
    assert_eq!(
        storage
            .load_entity_state(
                "crash-project",
                ACTIVATION_OUTBOX_ENTITY_TYPE,
                "activation-a/1/intent-a",
                ACTIVATION_PROJECTION_SCOPE,
                "atomic-run",
            )
            .await
            .expect("load committed activation outbox"),
        Some((b"outbox-v1".to_vec(), 1))
    );
    assert_eq!(
        storage
            .load_run_events("crash-project", "atomic-run", 0)
            .await
            .expect("load committed events")
            .len(),
        1,
        "lost-ack retry must not duplicate the event"
    );
    assert_eq!(
        storage
            .scan_dispatch_intents(0, None, 10)
            .await
            .expect("scan committed outbox")
            .len(),
        1,
        "lost-ack retry must not duplicate durable dispatch intent"
    );
    assert_eq!(
        storage
            .load_projection_checkpoint(CHECKPOINT)
            .await
            .expect("load committed checkpoint"),
        Some(Offset(0))
    );
    storage
        .delete_namespace()
        .await
        .expect("delete isolated test namespace");
}

#[tokio::test]
async fn processor_restart_converges_from_journal_and_checkpoint() {
    let (_directory, storage) = test_storage("crash-runtime").await;
    let storage = Arc::new(storage);
    let journal = Arc::new(storage.journal(0));
    journal
        .append_batch(&[record("replay-run", "run.queued")])
        .await
        .expect("journal commit before simulated processor crash");
    assert_eq!(
        storage
            .load_projection_run("crash-project", "replay-run")
            .await
            .expect("load absent projection"),
        None,
        "journal commit is authoritative before projection"
    );

    run_processor_until_checkpoint(storage.clone(), journal.clone(), Offset(0)).await;
    let projected = storage
        .load_projection_run("crash-project", "replay-run")
        .await
        .expect("load replayed run")
        .expect("run projected after restart");
    let run = Run::decode(projected.as_slice()).expect("decode projected run");
    assert_eq!(run.status, "queued");
    assert_eq!(run.component_name, "crash-test");

    // A second process starts from checkpoint + 1 and applies only the new
    // journal suffix. Replaying offset 0 would duplicate its event.
    journal
        .append_batch(&[record("replay-run", "custom.event")])
        .await
        .expect("append suffix while processor is stopped");
    run_processor_until_checkpoint(storage.clone(), journal, Offset(1)).await;
    assert_eq!(
        storage
            .load_run_events("crash-project", "replay-run", 0)
            .await
            .expect("load replayed events")
            .len(),
        1,
        "checkpoint resume must not replay and duplicate offset 0"
    );
    storage
        .delete_namespace()
        .await
        .expect("delete isolated test namespace");
}

async fn run_processor_until_checkpoint(
    storage: Arc<LibsqlStorage>,
    journal: Arc<engine_libsql::LibsqlJournal>,
    expected: Offset,
) {
    let cancel = CancellationToken::new();
    let journal_port: Arc<dyn Segment> = journal;
    let committer: Arc<dyn ProjectionCommitter> = storage.clone();
    let processor_cancel = cancel.clone();
    let processor_storage = storage.clone();
    let processor = tokio::spawn(async move {
        run_single_partition_processor(
            "libsql-crash-test".to_string(),
            journal_port,
            processor_storage,
            committer,
            1,
            processor_cancel,
            None,
            None,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if storage
                .load_projection_checkpoint(CHECKPOINT)
                .await
                .expect("load replay checkpoint")
                == Some(expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("processor reached expected checkpoint");
    cancel.cancel();
    assert_eq!(
        processor
            .await
            .expect("join processor")
            .expect("processor result"),
        PartitionProcessorExit::Cancelled
    );
}
