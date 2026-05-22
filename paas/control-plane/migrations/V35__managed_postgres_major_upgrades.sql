CREATE TABLE managed_postgres_major_upgrades (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    source_postgres_version TEXT NOT NULL,
    target_postgres_version TEXT NOT NULL,
    strategy TEXT NOT NULL CHECK (strategy IN ('logical_replication_copy', 'pg_upgrade_copy')),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
    command_id TEXT,
    operation_id TEXT REFERENCES operations(id),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX managed_postgres_major_upgrades_cluster_status_idx
    ON managed_postgres_major_upgrades(cluster_id, status);

CREATE INDEX managed_postgres_major_upgrades_created_idx
    ON managed_postgres_major_upgrades(created_at DESC);
