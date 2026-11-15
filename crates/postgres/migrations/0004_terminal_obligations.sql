CREATE TABLE IF NOT EXISTS agnt5_runtime.terminal_obligations (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace, partition_id, project_id, run_id)
);

CREATE INDEX IF NOT EXISTS terminal_obligations_scan_idx
    ON agnt5_runtime.terminal_obligations
        (namespace, partition_id, project_id, run_id);
