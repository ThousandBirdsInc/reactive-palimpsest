CREATE TABLE organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE environments (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE node_hosts (
    id TEXT PRIMARY KEY,
    region TEXT NOT NULL,
    failure_domain TEXT NOT NULL,
    data_root TEXT NOT NULL DEFAULT '/var/lib/palimpsest/postgres',
    first_port INTEGER NOT NULL DEFAULT 55000 CHECK (first_port > 0 AND first_port <= 65535),
    state TEXT NOT NULL,
    max_clusters INTEGER NOT NULL CHECK (max_clusters >= 0),
    assigned_clusters INTEGER NOT NULL DEFAULT 0 CHECK (assigned_clusters >= 0),
    storage_gib INTEGER NOT NULL CHECK (storage_gib >= 0),
    used_storage_gib INTEGER NOT NULL DEFAULT 0 CHECK (used_storage_gib >= 0),
    last_heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE secret_refs (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    secret_material TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, external_ref)
);

CREATE TABLE managed_postgres_clusters (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    host_id TEXT REFERENCES node_hosts(id),
    region TEXT NOT NULL,
    postgres_version TEXT NOT NULL CHECK (postgres_version ~ '^(1[8-9]|[2-9][0-9])([.-].*)?$'),
    tier TEXT NOT NULL,
    storage_gib INTEGER NOT NULL CHECK (storage_gib > 0),
    lifecycle_state TEXT NOT NULL,
    host_data_dir TEXT,
    host_port INTEGER CHECK (host_port > 0 AND host_port <= 65535),
    app_secret_ref TEXT REFERENCES secret_refs(id),
    replication_secret_ref TEXT REFERENCES secret_refs(id),
    backup_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    maintenance_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sync_deployments (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    managed_postgres_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    config_version TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE managed_postgres_backups (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    status TEXT NOT NULL,
    backup_dir TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE managed_postgres_restores (
    id TEXT PRIMARY KEY,
    source_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    target_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    backup_id TEXT NOT NULL REFERENCES managed_postgres_backups(id),
    status TEXT NOT NULL,
    recovery_target_lsn TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE managed_postgres_wal_archives (
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    segment_name TEXT NOT NULL,
    status TEXT NOT NULL,
    archive_dir TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (cluster_id, segment_name)
);

CREATE TABLE config_versions (
    id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL REFERENCES environments(id),
    rendered_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    target_resource_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    current_step TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE agent_commands (
    id TEXT PRIMARY KEY,
    operation_id TEXT REFERENCES operations(id),
    host_id TEXT NOT NULL REFERENCES node_hosts(id),
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    action JSONB NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE usage_events (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    metric TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX managed_postgres_clusters_environment_idx
    ON managed_postgres_clusters(environment_id);

CREATE INDEX managed_postgres_backups_cluster_status_idx
    ON managed_postgres_backups(cluster_id, status);

CREATE INDEX managed_postgres_restores_source_status_idx
    ON managed_postgres_restores(source_cluster_id, status);

CREATE INDEX managed_postgres_wal_archives_cluster_status_idx
    ON managed_postgres_wal_archives(cluster_id, status);

CREATE INDEX operations_target_status_idx
    ON operations(target_resource_id, status);

CREATE INDEX config_versions_environment_idx
    ON config_versions(environment_id, status);

CREATE INDEX agent_commands_host_status_idx
    ON agent_commands(host_id, status);

CREATE INDEX audit_events_resource_idx
    ON audit_events(resource_id, occurred_at);

CREATE INDEX usage_events_environment_metric_idx
    ON usage_events(environment_id, metric, occurred_at);
