CREATE TABLE IF NOT EXISTS agnt5_runtime_writer_authority (
    namespace TEXT PRIMARY KEY NOT NULL,
    owner_id TEXT NOT NULL,
    lease_expires_at_ms INTEGER NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0)
);

CREATE INDEX IF NOT EXISTS writer_authority_expiry_idx
    ON agnt5_runtime_writer_authority (lease_expires_at_ms, namespace);
