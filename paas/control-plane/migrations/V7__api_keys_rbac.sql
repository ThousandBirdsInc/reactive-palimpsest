CREATE TABLE team_memberships (
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    actor_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'developer', 'viewer', 'ci')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, actor_id)
);

CREATE TABLE api_keys (
    id TEXT PRIMARY KEY,
    token_prefix TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    organization_id TEXT REFERENCES organizations(id),
    project_id TEXT REFERENCES projects(id),
    environment_id TEXT REFERENCES environments(id),
    role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'developer', 'viewer', 'ci')),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);

CREATE INDEX api_keys_scope_idx
    ON api_keys(organization_id, project_id, environment_id, revoked_at);

CREATE INDEX api_keys_token_prefix_idx
    ON api_keys(token_prefix)
    WHERE revoked_at IS NULL;
