CREATE TABLE IF NOT EXISTS agnt5_runtime_activation_authority_heads (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    snapshot_bytes BLOB NOT NULL,
    PRIMARY KEY (namespace, partition_id, project_id, run_id, activation_id)
);

CREATE INDEX IF NOT EXISTS activation_authority_heads_run_idx
    ON agnt5_runtime_activation_authority_heads
        (namespace, partition_id, project_id, run_id, activation_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime_activation_required_child_obligations (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    parent_activation_id TEXT NOT NULL,
    child_activation_id TEXT NOT NULL,
    PRIMARY KEY (
        namespace,
        partition_id,
        project_id,
        run_id,
        parent_activation_id,
        child_activation_id
    )
);

CREATE INDEX IF NOT EXISTS activation_required_children_run_idx
    ON agnt5_runtime_activation_required_child_obligations
        (namespace, partition_id, project_id, run_id, parent_activation_id, child_activation_id);

CREATE TABLE IF NOT EXISTS agnt5_runtime_activation_run_gates (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    accepted_offset INTEGER NOT NULL CHECK (accepted_offset >= 0),
    PRIMARY KEY (namespace, partition_id, project_id, run_id)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime_activation_authority_watermarks (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    indexed_through INTEGER NOT NULL CHECK (indexed_through >= 0),
    PRIMARY KEY (namespace, partition_id)
);
