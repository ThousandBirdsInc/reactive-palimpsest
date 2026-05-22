CREATE TABLE database_proxy_routes (
    listen_addr TEXT PRIMARY KEY,
    upstream_addr TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX database_proxy_routes_environment_idx
    ON database_proxy_routes(environment_id);

CREATE INDEX database_proxy_routes_cluster_idx
    ON database_proxy_routes(cluster_id);
