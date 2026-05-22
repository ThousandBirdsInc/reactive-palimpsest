CREATE TABLE query_permission_policies (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    name TEXT NOT NULL,
    table_schema TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('read', 'subscribe')),
    principal_claim TEXT NOT NULL,
    predicate_sql TEXT NOT NULL,
    sample_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('draft', 'active')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, table_schema, table_name, operation, name)
);

CREATE INDEX query_permission_policies_scope_idx
    ON query_permission_policies(organization_id, project_id, environment_id);

CREATE INDEX query_permission_policies_table_idx
    ON query_permission_policies(environment_id, table_schema, table_name, operation, status);
