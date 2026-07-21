CREATE TABLE agnt5_segments (
    partition_id BIGINT PRIMARY KEY CHECK (partition_id >= 0),
    next_offset BIGINT NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    retained_from BIGINT NOT NULL DEFAULT 0 CHECK (retained_from >= 0),
    sealed BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK (retained_from <= next_offset)
);

CREATE TABLE agnt5_journal (
    partition_id BIGINT NOT NULL REFERENCES agnt5_segments(partition_id),
    offset_value BIGINT NOT NULL CHECK (offset_value >= 0),
    idempotency_key BYTEA,
    payload BYTEA NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (partition_id, offset_value),
    UNIQUE (partition_id, idempotency_key)
);

CREATE TABLE agnt5_materialized (
    namespace TEXT NOT NULL,
    key BYTEA NOT NULL,
    value BYTEA NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE TABLE agnt5_checkpoints (
    processor TEXT PRIMARY KEY,
    offset_value BIGINT NOT NULL CHECK (offset_value >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);
