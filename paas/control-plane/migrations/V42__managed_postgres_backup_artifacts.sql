CREATE TABLE managed_postgres_backup_artifacts (
    id TEXT PRIMARY KEY,
    backup_id TEXT NOT NULL REFERENCES managed_postgres_backups(id),
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    provider TEXT NOT NULL,
    object_uri TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    manifest_sha256 TEXT,
    size_bytes BIGINT CHECK (size_bytes IS NULL OR size_bytes >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'available', 'expired', 'deleted', 'failed')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (backup_id, provider, object_uri)
);

CREATE INDEX managed_postgres_backup_artifacts_backup_status_idx
    ON managed_postgres_backup_artifacts(backup_id, status);

CREATE INDEX managed_postgres_backup_artifacts_cluster_status_idx
    ON managed_postgres_backup_artifacts(cluster_id, status);
