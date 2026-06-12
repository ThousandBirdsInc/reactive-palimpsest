-- One permission-rule DSL document per environment. The document is the
-- palimpsest-permissions TOML config that operators author in the PaaS UI
-- and that the verifier compiles against a catalog before activation.
CREATE TABLE permission_rule_documents (
    environment_id TEXT PRIMARY KEY REFERENCES environments(id),
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    dsl TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX permission_rule_documents_scope_idx
    ON permission_rule_documents(organization_id, project_id, environment_id);
