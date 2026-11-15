CREATE TABLE IF NOT EXISTS agnt5_runtime.activation_run_leases (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    entry_bytes BYTEA NOT NULL,
    PRIMARY KEY (namespace, partition_id, project_id, run_id)
);
