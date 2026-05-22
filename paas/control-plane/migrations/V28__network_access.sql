CREATE TABLE ip_allowlist_rules (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    name TEXT NOT NULL,
    cidr TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('app', 'migration', 'support')),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, cidr, purpose)
);

CREATE INDEX ip_allowlist_rules_scope_idx
    ON ip_allowlist_rules(organization_id, project_id, environment_id, purpose, status);

CREATE TABLE static_egress_ips (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    environment_id TEXT NOT NULL REFERENCES environments(id),
    region TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    provider_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('provisioning', 'active', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (environment_id, ip_address)
);

CREATE INDEX static_egress_ips_scope_idx
    ON static_egress_ips(organization_id, project_id, environment_id, region, status);
