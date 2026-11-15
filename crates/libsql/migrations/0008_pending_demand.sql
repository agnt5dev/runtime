CREATE TABLE IF NOT EXISTS agnt5_runtime_pending_demand (
    namespace TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    project_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    component_type INTEGER NOT NULL CHECK (component_type BETWEEN 1 AND 10),
    component_name TEXT NOT NULL,
    priority SMALLINT NOT NULL CHECK (priority BETWEEN 1 AND 3),
    ready_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (
        namespace,
        partition_id,
        project_id,
        deployment_id,
        component_type,
        component_name,
        priority,
        ready_at_ms,
        created_at_ms,
        run_id
    )
);
