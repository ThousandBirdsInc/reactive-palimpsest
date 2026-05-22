CREATE TABLE managed_postgres_failovers (
    id TEXT PRIMARY KEY,
    source_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    target_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX managed_postgres_failovers_source_status_idx
    ON managed_postgres_failovers(source_cluster_id, status);

CREATE INDEX managed_postgres_failovers_target_idx
    ON managed_postgres_failovers(target_cluster_id);
