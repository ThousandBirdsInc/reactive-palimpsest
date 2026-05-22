CREATE TABLE billing_exports (
    id TEXT PRIMARY KEY,
    destination TEXT NOT NULL,
    organization_id TEXT REFERENCES organizations(id),
    project_id TEXT REFERENCES projects(id),
    environment_id TEXT REFERENCES environments(id),
    metric TEXT,
    occurred_at_from TIMESTAMPTZ,
    occurred_at_to TIMESTAMPTZ,
    event_count BIGINT NOT NULL CHECK (event_count >= 0),
    quantity_total BIGINT NOT NULL CHECK (quantity_total >= 0),
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX billing_exports_environment_metric_idx
    ON billing_exports(environment_id, metric, created_at);
