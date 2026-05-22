CREATE TABLE incidents (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT REFERENCES projects(id),
    environment_id TEXT REFERENCES environments(id),
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    status TEXT NOT NULL CHECK (status IN ('investigating', 'identified', 'monitoring', 'resolved')),
    impacted_services JSONB NOT NULL DEFAULT '[]'::jsonb,
    started_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (status = 'resolved' OR resolved_at IS NULL)
);

CREATE INDEX incidents_scope_status_idx
    ON incidents(organization_id, project_id, environment_id, status, severity);

CREATE INDEX incidents_started_at_idx
    ON incidents(started_at DESC);
