CREATE TABLE managed_postgres_role_credential_rotations (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    command_id TEXT UNIQUE REFERENCES agent_commands(id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'applying', 'applied', 'failed')),
    pending_secret_prefix TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at TIMESTAMPTZ
);

CREATE INDEX managed_postgres_role_credential_rotations_cluster_idx
    ON managed_postgres_role_credential_rotations(cluster_id, created_at DESC);

CREATE INDEX managed_postgres_role_credential_rotations_status_idx
    ON managed_postgres_role_credential_rotations(status, created_at);
