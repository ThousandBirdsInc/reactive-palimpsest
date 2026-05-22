CREATE TABLE webhook_endpoints (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT REFERENCES projects(id),
    environment_id TEXT REFERENCES environments(id),
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    event_types JSONB NOT NULL DEFAULT '[]'::jsonb,
    signing_secret_ref JSONB,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, url),
    UNIQUE (project_id, url),
    UNIQUE (organization_id, url)
);

CREATE INDEX webhook_endpoints_scope_idx
    ON webhook_endpoints(organization_id, project_id, environment_id, status);
