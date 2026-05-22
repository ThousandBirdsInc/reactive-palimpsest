CREATE TABLE domains (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL UNIQUE,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    route_host TEXT NOT NULL REFERENCES gateway_routes(host) ON DELETE CASCADE,
    verification_status TEXT NOT NULL CHECK (verification_status IN ('pending', 'verified', 'failed')),
    verification_token TEXT NOT NULL,
    tls_status TEXT NOT NULL CHECK (tls_status IN ('pending', 'active', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX domains_scope_status_idx
    ON domains(organization_id, project_id, environment_id, verification_status, tls_status);
