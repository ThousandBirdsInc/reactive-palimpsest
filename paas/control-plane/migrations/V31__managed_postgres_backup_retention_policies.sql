CREATE TABLE managed_postgres_backup_retention_policies (
    cluster_id TEXT PRIMARY KEY REFERENCES managed_postgres_clusters(id),
    retention_days INTEGER NOT NULL CHECK (retention_days >= 0 AND retention_days <= 3650),
    keep_min_successful_backups INTEGER NOT NULL DEFAULT 1 CHECK (keep_min_successful_backups >= 0 AND keep_min_successful_backups <= 1000),
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX managed_postgres_backup_retention_policies_enabled_idx
    ON managed_postgres_backup_retention_policies(enabled)
    WHERE enabled = true;
