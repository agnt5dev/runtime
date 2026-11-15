CREATE TABLE IF NOT EXISTS agnt5_runtime_activation_state_versions (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    state_key BLOB NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    PRIMARY KEY (namespace, partition_id, state_key),
    FOREIGN KEY (namespace, partition_id)
        REFERENCES agnt5_runtime_journal_partitions(namespace, partition_id)
        ON DELETE CASCADE
);
