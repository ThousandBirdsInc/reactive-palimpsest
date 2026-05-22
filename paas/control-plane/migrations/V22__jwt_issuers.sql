CREATE TABLE jwt_issuers (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT REFERENCES projects(id),
    environment_id TEXT REFERENCES environments(id),
    name TEXT NOT NULL,
    issuer TEXT NOT NULL,
    audience TEXT NOT NULL,
    jwks_url TEXT NOT NULL,
    claim_to_field JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, issuer, audience),
    UNIQUE (project_id, issuer, audience),
    UNIQUE (organization_id, issuer, audience)
);

CREATE INDEX jwt_issuers_scope_idx
    ON jwt_issuers(organization_id, project_id, environment_id, status);
