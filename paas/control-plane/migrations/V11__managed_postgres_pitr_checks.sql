CREATE TABLE managed_postgres_pitr_checks (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    backup_id TEXT REFERENCES managed_postgres_backups(id),
    status TEXT NOT NULL,
    segment_count INTEGER NOT NULL DEFAULT 0 CHECK (segment_count >= 0),
    first_segment TEXT,
    latest_segment TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX managed_postgres_pitr_checks_cluster_status_idx
    ON managed_postgres_pitr_checks(cluster_id, status);

CREATE INDEX managed_postgres_pitr_checks_cluster_created_idx
    ON managed_postgres_pitr_checks(cluster_id, created_at DESC);
