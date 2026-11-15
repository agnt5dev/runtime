CREATE TABLE IF NOT EXISTS agnt5_runtime.derived_index_state (
    namespace TEXT NOT NULL,
    index_name TEXT NOT NULL,
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    partition_count INTEGER NOT NULL CHECK (partition_count > 0),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, index_name)
);
