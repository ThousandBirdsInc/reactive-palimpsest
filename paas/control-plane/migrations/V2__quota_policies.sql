CREATE TABLE quota_policies (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    metric TEXT NOT NULL,
    limit_quantity BIGINT NOT NULL CHECK (limit_quantity >= 0),
    window_seconds BIGINT NOT NULL CHECK (window_seconds > 0),
    enforcement TEXT NOT NULL CHECK (enforcement IN ('reject', 'observe')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, metric)
);

CREATE INDEX quota_policies_environment_metric_idx
    ON quota_policies(environment_id, metric);
