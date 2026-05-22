CREATE TABLE gateway_routes (
    host TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    sync_endpoint TEXT NOT NULL,
    tls_policy TEXT NOT NULL CHECK (tls_policy IN ('terminate_at_gateway', 'mutual_tls_to_sync')),
    mtls_ca_secret_ref TEXT,
    max_connections INTEGER NOT NULL CHECK (max_connections > 0),
    max_requests_per_minute INTEGER NOT NULL CHECK (max_requests_per_minute > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (tls_policy = 'mutual_tls_to_sync' AND mtls_ca_secret_ref IS NOT NULL)
        OR (tls_policy = 'terminate_at_gateway' AND mtls_ca_secret_ref IS NULL)
    )
);

CREATE INDEX gateway_routes_environment_idx
    ON gateway_routes(environment_id);

CREATE INDEX gateway_routes_project_idx
    ON gateway_routes(project_id);
