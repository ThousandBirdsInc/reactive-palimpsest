CREATE TABLE sso_identity_providers (
    id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(id),
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('saml', 'oidc')),
    issuer TEXT NOT NULL,
    sso_url TEXT NOT NULL,
    certificate_secret_ref JSONB,
    claim_mappings JSONB NOT NULL DEFAULT '[]'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, issuer)
);

CREATE INDEX sso_identity_providers_scope_idx
    ON sso_identity_providers(organization_id, kind, status);
