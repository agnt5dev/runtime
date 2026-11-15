CREATE TABLE IF NOT EXISTS agnt5_runtime_journal_partitions (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    next_offset BIGINT NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    retained_floor BIGINT NOT NULL DEFAULT 0 CHECK (retained_floor >= 0),
    sealed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (namespace, partition_id),
    CHECK (retained_floor <= next_offset)
);

CREATE TABLE IF NOT EXISTS agnt5_runtime_journal_records (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    journal_offset BIGINT NOT NULL CHECK (journal_offset >= 0),
    record_bytes BLOB NOT NULL,
    PRIMARY KEY (namespace, partition_id, journal_offset),
    FOREIGN KEY (namespace, partition_id)
        REFERENCES agnt5_runtime_journal_partitions(namespace, partition_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS agnt5_runtime_journal_idempotency (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    idempotency_key BLOB NOT NULL,
    journal_offset BIGINT NOT NULL CHECK (journal_offset >= 0),
    PRIMARY KEY (namespace, partition_id, idempotency_key),
    FOREIGN KEY (namespace, partition_id)
        REFERENCES agnt5_runtime_journal_partitions(namespace, partition_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS journal_records_run_idx
    ON agnt5_runtime_journal_records (namespace, partition_id, journal_offset);
