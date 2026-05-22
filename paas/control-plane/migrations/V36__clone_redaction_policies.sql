CREATE TABLE managed_postgres_clone_redaction_policies (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    rules JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE managed_postgres_restores
    ADD COLUMN target_environment_id TEXT REFERENCES environments(id),
    ADD COLUMN redaction_policy_id TEXT REFERENCES managed_postgres_clone_redaction_policies(id);

UPDATE managed_postgres_restores
SET target_environment_id = managed_postgres_clusters.environment_id
FROM managed_postgres_clusters
WHERE managed_postgres_restores.target_cluster_id = managed_postgres_clusters.id
  AND managed_postgres_restores.target_environment_id IS NULL;

ALTER TABLE managed_postgres_restores
    ALTER COLUMN target_environment_id SET NOT NULL;

CREATE INDEX managed_postgres_clone_redaction_policies_scope_status_idx
    ON managed_postgres_clone_redaction_policies(organization_id, project_id, environment_id, status);
