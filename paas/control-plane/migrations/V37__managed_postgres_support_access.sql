CREATE TABLE managed_postgres_support_access_sessions (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    requested_by TEXT NOT NULL,
    approved_by TEXT,
    revoked_by TEXT,
    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    ticket_ref TEXT,
    status TEXT NOT NULL CHECK (status IN ('requested', 'active', 'revoked', 'expired')),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    CHECK (expires_at > requested_at)
);

CREATE INDEX managed_postgres_support_access_sessions_cluster_status_idx
    ON managed_postgres_support_access_sessions(cluster_id, status);

CREATE INDEX managed_postgres_support_access_sessions_scope_status_idx
    ON managed_postgres_support_access_sessions(organization_id, project_id, environment_id, status);
