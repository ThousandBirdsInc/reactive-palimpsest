CREATE TABLE managed_postgres_restore_drills (
    id TEXT PRIMARY KEY,
    source_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    restore_id TEXT NOT NULL UNIQUE REFERENCES managed_postgres_restores(id),
    backup_id TEXT NOT NULL REFERENCES managed_postgres_backups(id),
    target_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX managed_postgres_restore_drills_source_status_idx
    ON managed_postgres_restore_drills(source_cluster_id, status);

CREATE INDEX managed_postgres_restore_drills_source_completed_idx
    ON managed_postgres_restore_drills(source_cluster_id, completed_at DESC NULLS LAST);
