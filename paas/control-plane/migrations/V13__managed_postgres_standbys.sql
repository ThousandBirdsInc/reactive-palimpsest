CREATE TABLE managed_postgres_standbys (
    id TEXT PRIMARY KEY,
    source_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    target_cluster_id TEXT NOT NULL UNIQUE REFERENCES managed_postgres_clusters(id),
    backup_id TEXT NOT NULL REFERENCES managed_postgres_backups(id),
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX managed_postgres_standbys_source_status_idx
    ON managed_postgres_standbys(source_cluster_id, status);
