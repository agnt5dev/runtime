use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use engine_core::demand::DemandKey;
use engine_core::storage_contract::{
    DemandPendingStore, DispatchOutboxScanEntry, DispatchOutboxStore, EntityStateWriteStore,
    JournalPosition, LeaseWorkStore, PendingWorkStore, ProjectionCheckpointStore, ProjectionCommit,
    ProjectionCommitter, ProjectionMutation, ProjectionSource, ProjectionStateReader,
    QueueQueryStore, RunQueryStore, SessionQueryStore, TimerWorkStore, WorkflowQueryStore,
};
use engine_core::types::{Offset, Record};
use engine_proto::{ComponentType, Priority};

/// The required PostgreSQL/managed storage roles, kept as explicit ports so
/// conformance never grows a backend-shaped catch-all store interface.
pub struct RequiredStoragePorts {
    pub committer: Arc<dyn ProjectionCommitter>,
    pub projection_state: Arc<dyn ProjectionStateReader>,
    pub checkpoints: Arc<dyn ProjectionCheckpointStore>,
    pub dispatch_outbox: Arc<dyn DispatchOutboxStore>,
    pub timers: Arc<dyn TimerWorkStore>,
    pub pending_work: Arc<dyn PendingWorkStore>,
    pub demand_pending: Arc<dyn DemandPendingStore>,
    pub leases: Arc<dyn LeaseWorkStore>,
    pub run_queries: Arc<dyn RunQueryStore>,
    pub workflow_queries: Arc<dyn WorkflowQueryStore>,
    pub entity_writes: Arc<dyn EntityStateWriteStore>,
    pub session_queries: Arc<dyn SessionQueryStore>,
    pub queue_queries: Arc<dyn QueueQueryStore>,
}

impl RequiredStoragePorts {
    pub fn from_shared<T>(store: Arc<T>, committer: Arc<dyn ProjectionCommitter>) -> Self
    where
        T: ProjectionStateReader
            + ProjectionCheckpointStore
            + DispatchOutboxStore
            + TimerWorkStore
            + PendingWorkStore
            + DemandPendingStore
            + LeaseWorkStore
            + RunQueryStore
            + WorkflowQueryStore
            + EntityStateWriteStore
            + SessionQueryStore
            + QueueQueryStore
            + 'static,
    {
        Self {
            committer,
            projection_state: store.clone(),
            checkpoints: store.clone(),
            dispatch_outbox: store.clone(),
            timers: store.clone(),
            pending_work: store.clone(),
            demand_pending: store.clone(),
            leases: store.clone(),
            run_queries: store.clone(),
            workflow_queries: store.clone(),
            entity_writes: store.clone(),
            session_queries: store.clone(),
            queue_queries: store,
        }
    }
}

/// Exercise every required consumer-owned storage role using semantic
/// projection commits. This suite intentionally excludes optional archive,
/// cold-tier, replication, restore, and routing capabilities.
pub async fn assert_required_storage_port_contract<F, G>(create: F)
where
    F: Fn() -> (RequiredStoragePorts, G),
{
    projection_atomicity_contract(&create).await;
    projection_object_contract(&create).await;
    durable_work_contract(&create).await;
    demand_pending_contract(&create).await;
    query_cursor_contract(&create).await;
}

fn record(project_id: &str, run_id: &str, offset: u64, timestamp_ns: i64) -> Record {
    Record {
        offset: Offset(offset),
        project_id: project_id.to_string(),
        run_id: run_id.to_string(),
        event_type: "storage.conformance".to_string(),
        data: Bytes::from_static(b"{}"),
        timestamp_ns,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: HashMap::new(),
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    }
}

fn commit(
    project_id: &str,
    run_id: &str,
    offset: u64,
    timestamp_ns: i64,
    mutations: Vec<ProjectionMutation>,
) -> ProjectionCommit {
    let record = record(project_id, run_id, offset, timestamp_ns);
    ProjectionCommit::new(
        ProjectionSource::new(JournalPosition::new(0, Offset(offset)), record),
        mutations,
    )
}

#[allow(clippy::too_many_arguments)]
fn demand_row_commit(
    partition_id: u32,
    project_id: &str,
    run_id: &str,
    offset: u64,
    deployment_id: &str,
    component_name: &str,
    priority: Priority,
    ready_at_ms: i64,
    created_at_ms: i64,
    include_run: bool,
) -> ProjectionCommit {
    let mut source_record = record(project_id, run_id, offset, created_at_ms * 1_000_000);
    source_record.offset =
        engine_core::partition_offset::encode_offset(partition_id as usize, offset)
            .expect("required ports: encode demand source offset");
    let source = ProjectionSource::new(
        JournalPosition::new(partition_id, Offset(offset)),
        source_record,
    );
    let mut mutations = Vec::with_capacity(2);
    if include_run {
        mutations.push(ProjectionMutation::PutRun(run_id.as_bytes().to_vec()));
    }
    mutations.push(ProjectionMutation::PutPendingPriority {
        deployment_id: deployment_id.to_string(),
        component_type: ComponentType::Function as i32,
        component_name: component_name.to_string(),
        priority: priority as u8,
        ready_at_ms,
        created_at_ms,
    });
    ProjectionCommit::new(source, mutations)
}

#[allow(clippy::too_many_arguments)]
fn demand_row_remove_commit(
    partition_id: u32,
    project_id: &str,
    run_id: &str,
    offset: u64,
    deployment_id: &str,
    component_name: &str,
    priority: Priority,
    ready_at_ms: i64,
    created_at_ms: i64,
) -> ProjectionCommit {
    let mut source_record = record(project_id, run_id, offset, created_at_ms * 1_000_000);
    source_record.offset =
        engine_core::partition_offset::encode_offset(partition_id as usize, offset)
            .expect("required ports: encode demand removal source offset");
    ProjectionCommit::new(
        ProjectionSource::new(
            JournalPosition::new(partition_id, Offset(offset)),
            source_record,
        ),
        vec![ProjectionMutation::RemovePendingPriority {
            deployment_id: deployment_id.to_string(),
            component_type: ComponentType::Function as i32,
            component_name: component_name.to_string(),
            priority: priority as u8,
            ready_at_ms,
            created_at_ms,
        }],
    )
}

async fn demand_pending_contract<F, G>(create: &F)
where
    F: Fn() -> (RequiredStoragePorts, G),
{
    let (ports, _guard) = create();
    let partition_id = 3;
    let project_id = "required-demand";
    let deployment_id = "deployment-a";
    let component_name = "target";
    let demand_key = DemandKey::new(
        project_id,
        deployment_id,
        ComponentType::Function,
        component_name,
    )
    .expect("required ports: canonical demand key");

    // Exact demand-key seeks must not be affected by a substantial adjacent
    // incompatible prefix. Adapter-specific depth tests extend this to 5,000
    // rows and assert the physical query plan.
    for index in 0..64_u64 {
        ports
            .committer
            .commit(demand_row_commit(
                partition_id,
                project_id,
                &format!("incompatible-{index:03}"),
                index + 1,
                deployment_id,
                &format!("other-{index:03}"),
                Priority::Interactive,
                1,
                index as i64,
                true,
            ))
            .await
            .expect("required ports: seed incompatible demand row");
    }

    let rows = [
        ("normal-a", Priority::Normal, 100, 10),
        ("normal-b", Priority::Normal, 100, 20),
        ("batch", Priority::Batch, 50, 5),
        ("future-interactive", Priority::Interactive, 200, 1),
    ];
    for (index, (run_id, priority, ready_at_ms, created_at_ms)) in rows.iter().copied().enumerate()
    {
        let projection = demand_row_commit(
            partition_id,
            project_id,
            run_id,
            100 + index as u64,
            deployment_id,
            component_name,
            priority,
            ready_at_ms,
            created_at_ms,
            true,
        );
        ports
            .committer
            .commit(projection.clone())
            .await
            .expect("required ports: seed compatible demand row");
        ports
            .committer
            .commit(projection)
            .await
            .expect("required ports: replay compatible demand row");
    }
    ports
        .committer
        .commit(demand_row_commit(
            partition_id + 1,
            project_id,
            "other-partition",
            200,
            deployment_id,
            component_name,
            Priority::Interactive,
            1,
            1,
            true,
        ))
        .await
        .expect("required ports: seed other partition");

    let head = ports
        .demand_pending
        .read_pending_demand_head(partition_id, &demand_key, 150)
        .await
        .expect("required ports: read demand head");
    assert_eq!(
        head.ready
            .as_ref()
            .map(|candidate| candidate.row.rank.run_id.as_str()),
        Some("normal-a")
    );
    assert_eq!(
        head.ready
            .as_ref()
            .and_then(|candidate| candidate.data.as_deref()),
        Some(b"normal-a".as_slice())
    );
    assert_eq!(head.next_ready_at_ms, Some(200));

    let first = ports
        .demand_pending
        .scan_pending_demand_candidates(partition_id, &demand_key, 150, None, 2)
        .await
        .expect("required ports: first demand page");
    assert_eq!(
        first
            .iter()
            .map(|candidate| candidate.row.rank.run_id.as_str())
            .collect::<Vec<_>>(),
        ["normal-a", "normal-b"]
    );
    let second = ports
        .demand_pending
        .scan_pending_demand_candidates(
            partition_id,
            &demand_key,
            150,
            first.last().map(|candidate| &candidate.row.rank),
            2,
        )
        .await
        .expect("required ports: resumed demand page");
    assert_eq!(
        second
            .iter()
            .map(|candidate| candidate.row.rank.run_id.as_str())
            .collect::<Vec<_>>(),
        ["batch"]
    );

    let all_due = ports
        .demand_pending
        .scan_pending_demand_candidates(partition_id, &demand_key, 250, None, 8)
        .await
        .expect("required ports: scan after delayed row becomes ready");
    assert_eq!(
        all_due
            .iter()
            .map(|candidate| candidate.row.rank.run_id.as_str())
            .collect::<Vec<_>>(),
        ["future-interactive", "normal-a", "normal-b", "batch"]
    );

    // A stale derived row remains visible for repair but never fabricates Run
    // bytes. Removing it is exact and replay-safe.
    let stale_put = demand_row_commit(
        partition_id,
        project_id,
        "stale",
        300,
        deployment_id,
        component_name,
        Priority::Interactive,
        80,
        1,
        false,
    );
    ports
        .committer
        .commit(stale_put)
        .await
        .expect("required ports: seed stale demand row");
    let stale_head = ports
        .demand_pending
        .read_pending_demand_head(partition_id, &demand_key, 150)
        .await
        .expect("required ports: read stale demand head");
    let stale = stale_head
        .ready
        .expect("required ports: stale row is visible");
    assert_eq!(stale.row.rank.run_id, "stale");
    assert_eq!(stale.data, None);

    let stale_remove = demand_row_remove_commit(
        partition_id,
        project_id,
        "stale",
        301,
        deployment_id,
        component_name,
        Priority::Interactive,
        80,
        1,
    );
    ports
        .committer
        .commit(stale_remove.clone())
        .await
        .expect("required ports: remove stale demand row");
    ports
        .committer
        .commit(stale_remove)
        .await
        .expect("required ports: replay stale demand removal");
    let repaired = ports
        .demand_pending
        .read_pending_demand_head(partition_id, &demand_key, 150)
        .await
        .expect("required ports: read repaired demand head");
    assert_eq!(
        repaired.ready.map(|candidate| candidate.row.rank.run_id),
        Some("normal-a".to_string())
    );
}

async fn projection_atomicity_contract<F, G>(create: &F)
where
    F: Fn() -> (RequiredStoragePorts, G),
{
    let (ports, _guard) = create();
    let projection = commit(
        "required-ports",
        "atomic-run",
        5,
        500,
        vec![
            ProjectionMutation::PutRun(b"run-state".to_vec()),
            ProjectionMutation::PutStepMemo {
                step_key: "step-a".to_string(),
                data: b"memo".to_vec(),
            },
            ProjectionMutation::PutEvent,
        ],
    )
    .advancing_checkpoint();
    ports
        .committer
        .commit(projection.clone())
        .await
        .expect("required ports: atomic projection commit");
    ports
        .committer
        .commit(projection)
        .await
        .expect("required ports: idempotent projection replay");

    assert_eq!(
        ports
            .projection_state
            .load_projection_run("required-ports", "atomic-run")
            .await
            .expect("required ports: load projected run"),
        Some(b"run-state".to_vec())
    );
    assert_eq!(
        ports
            .run_queries
            .load_step_memo("required-ports", "atomic-run", "step-a")
            .await
            .expect("required ports: load step memo"),
        Some(b"memo".to_vec())
    );
    let events = ports
        .run_queries
        .load_run_events("required-ports", "atomic-run", 0)
        .await
        .expect("required ports: load projected event");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].offset, Offset(5));
    assert_eq!(
        ports
            .checkpoints
            .load_projection_checkpoint("processor-partition-0")
            .await
            .expect("required ports: load checkpoint"),
        Some(Offset(5))
    );

    let activation_projection = commit(
        "required-ports",
        "activation-run",
        6,
        600,
        vec![
            ProjectionMutation::PutActivation {
                activation_id: "activation-a".to_string(),
                data: b"activation-snapshot".to_vec(),
                version: 6,
            },
            ProjectionMutation::PutStepMemo {
                step_key: "step/activation".to_string(),
                data: b"activation-result".to_vec(),
            },
            ProjectionMutation::PutEntityState {
                entity_type: "workflow".to_string(),
                entity_key: "activation-state".to_string(),
                scope: "run".to_string(),
                scope_id: "activation-run".to_string(),
                data: b"state-after-completion".to_vec(),
                version: 1,
            },
            ProjectionMutation::PutActivationOutbox {
                activation_id: "activation-a".to_string(),
                attempt: 1,
                intent_id: "effect-a".to_string(),
                data: b"effect-intent".to_vec(),
            },
        ],
    )
    .advancing_checkpoint();
    ports
        .committer
        .commit(activation_projection.clone())
        .await
        .expect("required ports: activation bundle commit");
    ports
        .committer
        .commit(activation_projection)
        .await
        .expect("required ports: activation bundle replay");
    assert_eq!(
        ports
            .projection_state
            .load_projection_activation("required-ports", "activation-run", "activation-a")
            .await
            .expect("required ports: activation snapshot"),
        Some(b"activation-snapshot".to_vec())
    );
    assert_eq!(
        ports
            .run_queries
            .load_step_memo("required-ports", "activation-run", "step/activation")
            .await
            .expect("required ports: activation memo"),
        Some(b"activation-result".to_vec())
    );
    assert_eq!(
        ports
            .workflow_queries
            .load_entity_state(
                "required-ports",
                "workflow",
                "activation-state",
                "run",
                "activation-run",
            )
            .await
            .expect("required ports: activation state mutation"),
        Some((b"state-after-completion".to_vec(), 1))
    );
    assert_eq!(
        ports
            .workflow_queries
            .load_entity_state(
                "required-ports",
                engine_core::storage_contract::ACTIVATION_OUTBOX_ENTITY_TYPE,
                "activation-a/1/effect-a",
                engine_core::storage_contract::ACTIVATION_PROJECTION_SCOPE,
                "activation-run",
            )
            .await
            .expect("required ports: activation outbox intent"),
        Some((b"effect-intent".to_vec(), 1))
    );
    assert!(ports
        .dispatch_outbox
        .scan_dispatch_intents(0, None, 10)
        .await
        .expect("required ports: activation intent must not enter dispatch outbox")
        .is_empty());

    ports
        .checkpoints
        .store_projection_checkpoint("repair-checkpoint", Offset(9))
        .await
        .expect("required ports: explicit checkpoint write");
    assert_eq!(
        ports
            .checkpoints
            .load_projection_checkpoint("repair-checkpoint")
            .await
            .expect("required ports: explicit checkpoint read"),
        Some(Offset(9))
    );
}

async fn projection_object_contract<F, G>(create: &F)
where
    F: Fn() -> (RequiredStoragePorts, G),
{
    let (ports, _guard) = create();
    ports
        .committer
        .commit(commit(
            "required-ports",
            "object-run",
            1,
            100,
            vec![
                ProjectionMutation::PutRun(b"object-run-state".to_vec()),
                ProjectionMutation::PutEntityState {
                    entity_type: "workflow".to_string(),
                    entity_key: "entity-a".to_string(),
                    scope: "run".to_string(),
                    scope_id: "object-run".to_string(),
                    data: b"entity".to_vec(),
                    version: 7,
                },
                ProjectionMutation::PutApproval {
                    approval_id: "approval-a".to_string(),
                    run_id: "object-run".to_string(),
                    data: b"approval".to_vec(),
                },
                ProjectionMutation::PutSignal {
                    signal_id: "signal-a".to_string(),
                    run_id: "object-run".to_string(),
                    signal_name: "ready".to_string(),
                    waiting_step: "step-a".to_string(),
                    data: b"signal".to_vec(),
                },
                ProjectionMutation::PutTimer {
                    timer_id: "timer-a".to_string(),
                    run_id: "object-run".to_string(),
                    status: "scheduled".to_string(),
                    fire_at_ns: 500,
                    data: b"timer".to_vec(),
                },
                ProjectionMutation::PutJob {
                    job_id: "job-a".to_string(),
                    run_id: "object-run".to_string(),
                    batch_id: "batch-a".to_string(),
                    data: b"job".to_vec(),
                },
                ProjectionMutation::PutBatch {
                    batch_id: "batch-a".to_string(),
                    data: b"batch".to_vec(),
                },
                ProjectionMutation::PutSession {
                    session_id: "session-a".to_string(),
                    component_name: "component-a".to_string(),
                    data: b"session".to_vec(),
                },
                ProjectionMutation::PutMessage {
                    message_id: "message-a".to_string(),
                    correlation_id: "correlation-a".to_string(),
                    data: b"message".to_vec(),
                },
            ],
        ))
        .await
        .expect("required ports: commit object projections");

    assert_eq!(
        ports
            .workflow_queries
            .load_entity_state(
                "required-ports",
                "workflow",
                "entity-a",
                "run",
                "object-run",
            )
            .await
            .unwrap(),
        Some((b"entity".to_vec(), 7))
    );
    assert_eq!(
        ports
            .workflow_queries
            .scan_run_approvals("required-ports", "object-run")
            .await
            .unwrap(),
        [b"approval".to_vec()]
    );
    assert_eq!(
        ports
            .workflow_queries
            .lookup_run_signal("required-ports", "object-run", "ready", "step-a",)
            .await
            .unwrap(),
        Some(b"signal".to_vec())
    );
    assert_eq!(
        ports
            .workflow_queries
            .scan_run_timers("required-ports", "object-run")
            .await
            .unwrap(),
        [b"timer".to_vec()]
    );
    assert_eq!(
        ports
            .workflow_queries
            .scan_run_jobs("required-ports", "object-run")
            .await
            .unwrap(),
        [b"job".to_vec()]
    );
    assert_eq!(
        ports
            .workflow_queries
            .scan_batch_jobs("required-ports", "batch-a")
            .await
            .unwrap(),
        [b"job".to_vec()]
    );
    assert_eq!(
        ports
            .session_queries
            .scan_component_sessions("required-ports", "component-a")
            .await
            .unwrap(),
        [b"session".to_vec()]
    );
    assert_eq!(
        ports
            .session_queries
            .scan_correlation_messages("required-ports", "correlation-a")
            .await
            .unwrap(),
        [b"message".to_vec()]
    );
    assert_eq!(
        ports.timers.scan_due_timers(500, 10).await.unwrap(),
        [b"timer".to_vec()]
    );

    ports
        .entity_writes
        .store_entity_state(
            "required-ports",
            "workflow",
            "entity-a",
            "run",
            "object-run",
            b"entity-v8",
            8,
        )
        .await
        .expect("required ports: entity write-through");
    assert_eq!(
        ports
            .workflow_queries
            .load_entity_state(
                "required-ports",
                "workflow",
                "entity-a",
                "run",
                "object-run",
            )
            .await
            .unwrap(),
        Some((b"entity-v8".to_vec(), 8))
    );
}

async fn durable_work_contract<F, G>(create: &F)
where
    F: Fn() -> (RequiredStoragePorts, G),
{
    let (ports, _guard) = create();
    for (run_id, offset, priority, ready_at_ms, worker_id, expires_at_ms) in [
        ("work-a", 1, 1, 200, "worker-a", 100),
        ("work-b", 2, 3, 100, "worker-a", 200),
    ] {
        let projection = commit(
            "required-ports",
            run_id,
            offset,
            offset as i64 * 1_000_000,
            vec![
                ProjectionMutation::PutRun(run_id.as_bytes().to_vec()),
                ProjectionMutation::PutPendingRun {
                    component_name: "component-a".to_string(),
                    data: run_id.as_bytes().to_vec(),
                },
                ProjectionMutation::PutPendingDeploymentPriority {
                    deployment_id: "deployment-a".to_string(),
                    priority,
                    ready_at_ms,
                    created_at_ms: 10,
                },
                ProjectionMutation::UpdateQueueCounters {
                    deployment_id: "deployment-a".to_string(),
                    priority,
                    ready_at_ms,
                    created_at_ms: 10,
                    delta: 1,
                },
                ProjectionMutation::PutLeaseIndex {
                    expires_at_ms,
                    worker_id: worker_id.to_string(),
                },
                ProjectionMutation::PutDispatchOutbox {
                    partition_id: 0,
                    queued_offset: offset,
                    request_data: format!("dispatch-{run_id}").into_bytes(),
                },
            ],
        );
        ports
            .committer
            .commit(projection)
            .await
            .expect("required ports: commit durable work");
    }

    assert_eq!(
        ports
            .pending_work
            .scan_pending_runs("required-ports", &["component-a".to_string()], 10,)
            .await
            .unwrap(),
        [b"work-a".to_vec(), b"work-b".to_vec()]
    );
    assert_eq!(
        ports
            .pending_work
            .scan_ready_deployment_runs("required-ports", "deployment-a", 1_000, 10,)
            .await
            .unwrap(),
        ["work-a", "work-b"]
    );
    assert_eq!(
        ports
            .pending_work
            .load_pending_run("required-ports", "work-a")
            .await
            .unwrap(),
        Some(b"work-a".to_vec())
    );
    let first_page = ports
        .pending_work
        .scan_ready_deployment_candidates("required-ports", "deployment-a", 1_000, None, 1)
        .await
        .unwrap();
    assert_eq!(first_page.len(), 1, "candidate page honors the limit");
    assert_eq!(first_page[0].cursor.run_id, "work-a");
    assert_eq!(
        first_page[0].data,
        Some(b"work-a".to_vec()),
        "candidate page carries the projected run data"
    );
    let second_page = ports
        .pending_work
        .scan_ready_deployment_candidates(
            "required-ports",
            "deployment-a",
            1_000,
            Some(&first_page[0].cursor),
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        second_page
            .iter()
            .map(|candidate| candidate.cursor.run_id.as_str())
            .collect::<Vec<_>>(),
        ["work-b"],
        "candidate page resumes strictly after the cursor"
    );

    assert_eq!(
        ports
            .leases
            .scan_expired_leases(500, 10, None)
            .await
            .unwrap(),
        [
            (100, "required-ports".to_string(), "work-a".to_string()),
            (200, "required-ports".to_string(), "work-b".to_string()),
        ]
    );
    assert_eq!(
        ports.leases.scan_worker_leases("worker-a").await.unwrap(),
        [
            ("required-ports".to_string(), "work-a".to_string()),
            ("required-ports".to_string(), "work-b".to_string()),
        ]
    );
    assert_eq!(
        ports
            .leases
            .load_lease_run("required-ports", "work-a")
            .await
            .unwrap(),
        Some(b"work-a".to_vec())
    );

    let dispatch = ports
        .dispatch_outbox
        .scan_dispatch_intents(0, None, 10)
        .await
        .expect("required ports: scan dispatch outbox");
    let ready = dispatch
        .into_iter()
        .map(|entry| match entry {
            DispatchOutboxScanEntry::Ready(entry) => entry,
            DispatchOutboxScanEntry::CorruptKey { .. } => {
                panic!("semantic adapter produced corrupt dispatch identity")
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ready
            .iter()
            .map(|entry| (entry.queued_offset, entry.run_id.as_str()))
            .collect::<Vec<_>>(),
        [(1, "work-a"), (2, "work-b")]
    );
    ports
        .dispatch_outbox
        .delete_dispatch_intent(&ready[0].token)
        .await
        .expect("required ports: acknowledge dispatch");
    assert_eq!(
        ports
            .dispatch_outbox
            .scan_dispatch_intents(0, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );

    let project_counter = ports
        .queue_queries
        .load_project_queue_counter("required-ports")
        .await
        .unwrap();
    assert_eq!(project_counter.pending_count, 2);
    assert_eq!(project_counter.oldest_ready_at_ms, Some(100));
    assert_eq!(
        ports
            .queue_queries
            .load_deployment_queue_counter("required-ports", "deployment-a")
            .await
            .unwrap()
            .pending_count,
        2
    );
    assert_eq!(
        ports
            .queue_queries
            .load_priority_queue_counter("required-ports", "deployment-a", 1)
            .await
            .unwrap()
            .pending_count,
        1
    );
}

async fn query_cursor_contract<F, G>(create: &F)
where
    F: Fn() -> (RequiredStoragePorts, G),
{
    let (ports, _guard) = create();
    for (run_id, submitted_at_ms, offset) in
        [("oldest", 100, 1), ("newest", 300, 2), ("middle", 200, 3)]
    {
        ports
            .committer
            .commit(commit(
                "query-project",
                run_id,
                offset,
                submitted_at_ms * 1_000_000,
                vec![
                    ProjectionMutation::PutRun(run_id.as_bytes().to_vec()),
                    ProjectionMutation::PutRunHotIndex {
                        workspace_id: "workspace-a".to_string(),
                        submitted_at_ms,
                    },
                    ProjectionMutation::PutEventHotIndex {
                        timestamp_ms: submitted_at_ms,
                        summary: run_id.as_bytes().to_vec(),
                    },
                ],
            ))
            .await
            .expect("required ports: commit query indexes");
    }

    assert_eq!(
        ports
            .run_queries
            .scan_hot_run_ids("workspace-a", "query-project", None, 2)
            .await
            .unwrap(),
        ["newest", "middle"]
    );
    assert_eq!(
        ports
            .run_queries
            .scan_hot_run_ids(
                "workspace-a",
                "query-project",
                Some((200, "middle".to_string())),
                2,
            )
            .await
            .unwrap(),
        ["oldest"]
    );
    // Public Runs cursors use descending IDs within a timestamp. Exercise
    // a page boundary in that group, not only distinct timestamps.
    for (run_id, offset) in [("middle-z", 4), ("middle-a", 5)] {
        ports
            .committer
            .commit(commit(
                "query-project",
                run_id,
                offset,
                200_000_000,
                vec![ProjectionMutation::PutRunHotIndex {
                    workspace_id: "workspace-a".into(),
                    submitted_at_ms: 200,
                }],
            ))
            .await
            .unwrap();
    }
    assert_eq!(
        ports
            .run_queries
            .scan_hot_run_ids(
                "workspace-a",
                "query-project",
                Some((300, "newest".into())),
                2
            )
            .await
            .unwrap(),
        ["middle-z", "middle-a"]
    );
    assert_eq!(
        ports
            .run_queries
            .scan_hot_run_ids(
                "workspace-a",
                "query-project",
                Some((200, "middle-a".into())),
                2
            )
            .await
            .unwrap(),
        ["middle", "oldest"]
    );

    assert_eq!(
        ports
            .run_queries
            .scan_event_summaries("query-project", Some((300, "newest".to_string())), 10,)
            .await
            .unwrap()
            .into_iter()
            .map(|(run_id, _, _)| run_id)
            .collect::<Vec<_>>(),
        ["middle", "oldest"]
    );
    assert!(ports
        .run_queries
        .scan_run_segments("query-project", "newest")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        ports
            .run_queries
            .load_query_archive_path("query-project", "newest")
            .await
            .unwrap(),
        None
    );
}
