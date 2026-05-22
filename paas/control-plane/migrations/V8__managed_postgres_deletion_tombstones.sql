CREATE TABLE managed_postgres_deletion_tombstones (
    cluster_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    region TEXT NOT NULL,
    postgres_version TEXT NOT NULL,
    tier TEXT NOT NULL,
    retained_backup_id TEXT,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retention_expires_at TIMESTAMPTZ,
    unrecoverable_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX managed_postgres_deletion_tombstones_environment_idx
    ON managed_postgres_deletion_tombstones(environment_id, deleted_at DESC);

CREATE INDEX managed_postgres_deletion_tombstones_retention_idx
    ON managed_postgres_deletion_tombstones(retention_expires_at)
    WHERE retention_expires_at IS NOT NULL;
