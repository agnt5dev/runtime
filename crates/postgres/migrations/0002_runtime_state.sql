CREATE TABLE IF NOT EXISTS agnt5_runtime.projection_checkpoints (
    namespace TEXT NOT NULL,
    checkpoint_key TEXT NOT NULL,
    checkpoint_offset BIGINT NOT NULL CHECK (checkpoint_offset >= 0),
    PRIMARY KEY (namespace, checkpoint_key)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.runs (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, run_id)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.step_memos (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_key TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, run_id, step_key)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.events (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    timestamp_ns BIGINT NOT NULL,
    record_bytes BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, run_id, timestamp_ns)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.entity_states (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    scope TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (
        namespace,
        project_id,
        entity_type,
        entity_key,
        scope,
        scope_id
    )
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.approvals (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    approval_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, approval_id)
);

CREATE INDEX IF NOT EXISTS approvals_by_run_idx
    ON agnt5_runtime.approvals (namespace, project_id, run_id, approval_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime.signals (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    waiting_step TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, signal_id)
);

CREATE INDEX IF NOT EXISTS signals_by_run_idx
    ON agnt5_runtime.signals (namespace, project_id, run_id, signal_id);

CREATE UNIQUE INDEX IF NOT EXISTS signals_lookup_idx
    ON agnt5_runtime.signals (
        namespace,
        project_id,
        run_id,
        signal_name,
        waiting_step
    );

CREATE TABLE IF NOT EXISTS agnt5_runtime.timers (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    fire_at_ns BIGINT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, timer_id)
);

CREATE INDEX IF NOT EXISTS timers_by_run_idx
    ON agnt5_runtime.timers (namespace, project_id, run_id, timer_id);

CREATE INDEX IF NOT EXISTS timers_due_idx
    ON agnt5_runtime.timers (namespace, fire_at_ns, project_id, timer_id)
    WHERE status = 'scheduled' AND fire_at_ns > 0;

CREATE TABLE IF NOT EXISTS agnt5_runtime.jobs (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, job_id)
);

CREATE INDEX IF NOT EXISTS jobs_by_run_idx
    ON agnt5_runtime.jobs (namespace, project_id, run_id, job_id);

CREATE INDEX IF NOT EXISTS jobs_by_batch_idx
    ON agnt5_runtime.jobs (namespace, project_id, batch_id, job_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime.batches (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, batch_id)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.sessions (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    component_name TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, session_id)
);

CREATE INDEX IF NOT EXISTS sessions_by_component_idx
    ON agnt5_runtime.sessions (namespace, project_id, component_name, session_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime.messages (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, message_id)
);

CREATE INDEX IF NOT EXISTS messages_by_correlation_idx
    ON agnt5_runtime.messages (namespace, project_id, correlation_id, message_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime.pending_runs (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    component_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, component_name, run_id)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.pending_priority (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    component_name TEXT NOT NULL,
    priority SMALLINT NOT NULL CHECK (priority BETWEEN 0 AND 255),
    ready_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (
        namespace,
        project_id,
        deployment_id,
        component_name,
        priority,
        ready_at_ms,
        created_at_ms,
        run_id
    )
);

CREATE INDEX IF NOT EXISTS pending_priority_ready_idx
    ON agnt5_runtime.pending_priority (
        namespace,
        project_id,
        deployment_id,
        component_name,
        priority,
        ready_at_ms,
        created_at_ms,
        run_id
    );

CREATE TABLE IF NOT EXISTS agnt5_runtime.pending_deployment_priority (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    priority SMALLINT NOT NULL CHECK (priority BETWEEN 0 AND 255),
    ready_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (
        namespace,
        project_id,
        deployment_id,
        priority,
        ready_at_ms,
        created_at_ms,
        run_id
    )
);

CREATE INDEX IF NOT EXISTS pending_deployment_ready_idx
    ON agnt5_runtime.pending_deployment_priority (
        namespace,
        project_id,
        deployment_id,
        priority,
        ready_at_ms,
        created_at_ms,
        run_id
    );

CREATE TABLE IF NOT EXISTS agnt5_runtime.queue_counters (
    namespace TEXT NOT NULL,
    scope_kind SMALLINT NOT NULL CHECK (scope_kind BETWEEN 0 AND 2),
    project_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL DEFAULT '',
    priority SMALLINT NOT NULL DEFAULT -1 CHECK (priority BETWEEN -1 AND 255),
    pending_count BIGINT NOT NULL CHECK (pending_count >= 0),
    oldest_ready_at_ms BIGINT,
    oldest_pending_key BYTEA,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (namespace, scope_kind, project_id, deployment_id, priority)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime.leases (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL,
    worker_id TEXT NOT NULL,
    PRIMARY KEY (namespace, project_id, run_id)
);

CREATE INDEX IF NOT EXISTS leases_by_expiry_idx
    ON agnt5_runtime.leases (namespace, expires_at_ms, project_id, run_id);

CREATE INDEX IF NOT EXISTS leases_by_worker_idx
    ON agnt5_runtime.leases (namespace, worker_id, project_id, run_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime.dispatch_outbox (
    entry_id BIGSERIAL PRIMARY KEY,
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    queued_offset BIGINT NOT NULL CHECK (queued_offset >= 0),
    run_id TEXT NOT NULL,
    payload BYTEA NOT NULL,
    UNIQUE (namespace, partition_id, project_id, queued_offset, run_id)
);

CREATE INDEX IF NOT EXISTS dispatch_outbox_scan_idx
    ON agnt5_runtime.dispatch_outbox (namespace, partition_id, queued_offset, project_id, run_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime.run_hot_index (
    namespace TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    submitted_at_ms BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (namespace, workspace_id, project_id, submitted_at_ms, run_id)
);

CREATE INDEX IF NOT EXISTS run_hot_scan_idx
    ON agnt5_runtime.run_hot_index (
        namespace,
        workspace_id,
        project_id,
        submitted_at_ms DESC,
        run_id ASC
    );

CREATE TABLE IF NOT EXISTS agnt5_runtime.event_hot_index (
    namespace TEXT NOT NULL,
    project_id TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    summary BYTEA NOT NULL,
    PRIMARY KEY (namespace, project_id, timestamp_ms, run_id)
);

CREATE INDEX IF NOT EXISTS event_hot_scan_idx
    ON agnt5_runtime.event_hot_index (
        namespace,
        project_id,
        timestamp_ms DESC,
        run_id ASC
    );

-- Reserved for processor-owned command/result deduplication. Journal append
-- idempotency remains separate because it identifies authoritative records,
-- while this inbox records the durable decision for an accepted command.
CREATE TABLE IF NOT EXISTS agnt5_runtime.inbox (
    namespace TEXT NOT NULL,
    inbox_key BYTEA NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    accepted_offset BIGINT NOT NULL CHECK (accepted_offset >= 0),
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, inbox_key)
);
