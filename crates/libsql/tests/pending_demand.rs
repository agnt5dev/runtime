use std::collections::HashMap;

use bytes::Bytes;
use engine_core::demand::DemandKey;
use engine_core::storage_contract::{
    DemandPendingStore, JournalPosition, PendingDemandIndexAdmin, PendingDemandIndexOutcome,
    ProjectionCommit, ProjectionCommitter, ProjectionMutation, ProjectionSource,
};
use engine_core::types::{Offset, Record};
use engine_libsql::{LibsqlDatabase, LibsqlStorage};
use engine_proto::{ComponentType, Priority, Run};
use prost::Message;

async fn test_storage() -> (tempfile::TempDir, LibsqlStorage) {
    let directory = tempfile::tempdir().expect("create SQLite test directory");
    let storage = LibsqlStorage::connect(
        LibsqlDatabase::local(directory.path().join("runtime.db")),
        "pending-demand",
    )
    .await
    .expect("connect local SQLite storage");
    (directory, storage)
}

fn pending_commit(
    partition_id: u32,
    run_id: &str,
    priority: Priority,
    ready_at_ms: i64,
    created_at_ms: i64,
) -> ProjectionCommit {
    let run = Run {
        project_id: "project-a".into(),
        deployment_id: "deployment-a".into(),
        component_type: "function".into(),
        component_name: "summarize".into(),
        run_id: run_id.into(),
        status: "queued".into(),
        priority: priority as i32,
        submitted_at: Some(prost_types::Timestamp {
            seconds: created_at_ms / 1_000,
            nanos: ((created_at_ms % 1_000) * 1_000_000) as i32,
        }),
        metadata: HashMap::from([("ready_at_ms".into(), ready_at_ms.to_string())]),
        ..Default::default()
    };
    let record = Record {
        offset: Offset::ZERO,
        project_id: "project-a".into(),
        run_id: run_id.into(),
        event_type: "run.queued".into(),
        data: Bytes::new(),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: HashMap::new(),
        data_type: "json".into(),
        data_checksum: None,
        data_compressed: false,
    };
    let data = run.encode_to_vec();
    ProjectionCommit::new(
        ProjectionSource::new(JournalPosition::new(partition_id, Offset::ZERO), record),
        vec![
            ProjectionMutation::PutRun(data.clone()),
            ProjectionMutation::PutPendingRun {
                component_name: "summarize".into(),
                data,
            },
            ProjectionMutation::PutPendingPriority {
                deployment_id: "deployment-a".into(),
                component_type: ComponentType::Function as i32,
                component_name: "summarize".into(),
                priority: priority as u8,
                ready_at_ms,
                created_at_ms,
            },
        ],
    )
}

fn removal_commit(
    partition_id: u32,
    run_id: &str,
    priority: Priority,
    ready_at_ms: i64,
    created_at_ms: i64,
) -> ProjectionCommit {
    let record = Record {
        offset: Offset(1),
        project_id: "project-a".into(),
        run_id: run_id.into(),
        event_type: "run.assigned".into(),
        data: Bytes::new(),
        timestamp_ns: 1,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: HashMap::new(),
        data_type: "json".into(),
        data_checksum: None,
        data_compressed: false,
    };
    ProjectionCommit::new(
        ProjectionSource::new(JournalPosition::new(partition_id, Offset(1)), record),
        vec![
            ProjectionMutation::RemovePendingRun {
                component_name: "summarize".into(),
            },
            ProjectionMutation::RemovePendingPriority {
                deployment_id: "deployment-a".into(),
                component_type: ComponentType::Function as i32,
                component_name: "summarize".into(),
                priority: priority as u8,
                ready_at_ms,
                created_at_ms,
            },
        ],
    )
}

#[tokio::test]
async fn sqlite_pending_demand_is_bounded_ranked_and_idempotent() {
    let (_directory, storage) = test_storage().await;
    let demand_key = DemandKey::new(
        "project-a",
        "deployment-a",
        ComponentType::Function,
        "summarize",
    )
    .unwrap();

    let future = pending_commit(0, "future-interactive", Priority::Interactive, 200, 10);
    let normal = pending_commit(0, "ready-normal", Priority::Normal, 100, 20);
    let batch = pending_commit(0, "ready-batch", Priority::Batch, 50, 30);
    storage.commit(future).await.unwrap();
    storage.commit(normal.clone()).await.unwrap();
    storage.commit(normal).await.unwrap();
    storage.commit(batch).await.unwrap();
    storage
        .connection()
        .execute(
            r#"
            INSERT INTO agnt5_runtime_pending_demand
                (namespace, partition_id, project_id, deployment_id, component_type,
                 component_name, priority, ready_at_ms, created_at_ms, run_id)
            VALUES (?1, 0, 'project-a', 'deployment-a', 1,
                    'summarize', 1, 1, 1, 'stale-partial')
            "#,
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();
    assert_eq!(
        storage.ensure_pending_demand_index(1).await.unwrap(),
        PendingDemandIndexOutcome {
            rebuilt: true,
            rows: 3
        }
    );
    assert_eq!(
        storage.ensure_pending_demand_index(1).await.unwrap(),
        PendingDemandIndexOutcome::default()
    );

    storage
        .connection()
        .execute(
            r#"
            WITH RECURSIVE sequence(value) AS (
                SELECT 1
                UNION ALL
                SELECT value + 1 FROM sequence WHERE value < 5000
            )
            INSERT INTO agnt5_runtime_pending_demand
                (namespace, partition_id, project_id, deployment_id, component_type,
                 component_name, priority, ready_at_ms, created_at_ms, run_id)
            SELECT ?1, 0, 'project-a', 'deployment-a', 1,
                   'incompatible', 1, value, value, 'other-' || value
            FROM sequence
            "#,
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();

    let head = storage
        .read_pending_demand_head(0, &demand_key, 150)
        .await
        .unwrap();
    assert_eq!(
        head.ready
            .as_ref()
            .map(|candidate| candidate.row.rank.run_id.as_str()),
        Some("ready-normal")
    );
    assert_eq!(head.next_ready_at_ms, Some(200));
    let first = storage
        .scan_pending_demand_candidates(0, &demand_key, 150, None, 1)
        .await
        .unwrap();
    assert_eq!(first[0].row.rank.run_id, "ready-normal");
    let second = storage
        .scan_pending_demand_candidates(0, &demand_key, 150, Some(&first[0].row.rank), 1)
        .await
        .unwrap();
    assert_eq!(second[0].row.rank.run_id, "ready-batch");

    let mut rows = storage
        .connection()
        .query(
            r#"
            EXPLAIN QUERY PLAN
            SELECT priority, ready_at_ms, created_at_ms, run_id
            FROM agnt5_runtime_pending_demand
            WHERE namespace = ?1 AND partition_id = 0
              AND project_id = 'project-a' AND deployment_id = 'deployment-a'
              AND component_type = 1 AND component_name = 'summarize'
              AND priority = 1 AND ready_at_ms <= 150
            ORDER BY ready_at_ms, created_at_ms, run_id
            LIMIT 32
            "#,
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();
    let mut plan = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        plan.push(row.get::<String>(3).unwrap());
    }
    let plan = plan.join("\n");
    assert!(
        plan.contains("sqlite_autoindex_agnt5_runtime_pending_demand_1"),
        "unexpected plan:\n{plan}"
    );

    let remove = removal_commit(0, "ready-normal", Priority::Normal, 100, 20);
    storage.commit(remove.clone()).await.unwrap();
    storage.commit(remove).await.unwrap();
    let head = storage
        .read_pending_demand_head(0, &demand_key, 150)
        .await
        .unwrap();
    assert_eq!(
        head.ready
            .as_ref()
            .map(|candidate| candidate.row.rank.run_id.as_str()),
        Some("ready-batch")
    );

    storage.delete_namespace().await.unwrap();
}

#[tokio::test]
async fn sqlite_pending_demand_rebuild_rolls_back_before_certification() {
    let (_directory, storage) = test_storage().await;
    storage
        .connection()
        .execute(
            r#"
            INSERT INTO agnt5_runtime_pending_runs
                (namespace, project_id, component_name, run_id, data)
            VALUES (?1, 'project-a', 'summarize', 'corrupt', X'00')
            "#,
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();
    storage
        .connection()
        .execute(
            r#"
            INSERT INTO agnt5_runtime_pending_demand
                (namespace, partition_id, project_id, deployment_id, component_type,
                 component_name, priority, ready_at_ms, created_at_ms, run_id)
            VALUES (?1, 0, 'project-a', 'deployment-a', 1,
                    'summarize', 1, 1, 1, 'partial')
            "#,
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();

    assert!(storage.ensure_pending_demand_index(1).await.is_err());
    let mut count = storage
        .connection()
        .query(
            "SELECT COUNT(*) FROM agnt5_runtime_pending_demand WHERE namespace = ?1",
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();
    assert_eq!(
        count.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    drop(count);

    storage
        .connection()
        .execute(
            "DELETE FROM agnt5_runtime_pending_runs WHERE namespace = ?1",
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();
    assert_eq!(
        storage.ensure_pending_demand_index(1).await.unwrap(),
        PendingDemandIndexOutcome {
            rebuilt: true,
            rows: 0
        }
    );
    let mut count = storage
        .connection()
        .query(
            "SELECT COUNT(*) FROM agnt5_runtime_pending_demand WHERE namespace = ?1",
            libsql::params![storage.namespace()],
        )
        .await
        .unwrap();
    assert_eq!(
        count.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}
