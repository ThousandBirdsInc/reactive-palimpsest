CREATE TABLE managed_postgres_endpoints (
    environment_id TEXT PRIMARY KEY REFERENCES environments(id),
    active_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    updated_by_failover_id TEXT REFERENCES managed_postgres_failovers(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX managed_postgres_endpoints_active_cluster_idx
    ON managed_postgres_endpoints(active_cluster_id);
