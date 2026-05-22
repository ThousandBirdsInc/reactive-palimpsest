CREATE TABLE quota_alerts (
    id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL REFERENCES quota_policies(id) ON UPDATE CASCADE ON DELETE CASCADE,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    metric TEXT NOT NULL,
    threshold_basis_points INTEGER NOT NULL CHECK (
        threshold_basis_points > 0
        AND threshold_basis_points <= 10000
    ),
    current_quantity BIGINT NOT NULL DEFAULT 0 CHECK (current_quantity >= 0),
    limit_quantity BIGINT NOT NULL CHECK (limit_quantity >= 0),
    window_seconds BIGINT NOT NULL CHECK (window_seconds > 0),
    state TEXT NOT NULL DEFAULT 'ok' CHECK (state IN ('ok', 'firing')),
    last_evaluated_at TIMESTAMPTZ,
    fired_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (policy_id, threshold_basis_points)
);

CREATE INDEX quota_alerts_scope_idx
    ON quota_alerts(organization_id, project_id, environment_id, metric);

CREATE INDEX quota_alerts_state_idx
    ON quota_alerts(state, updated_at DESC);
