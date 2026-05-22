CREATE TABLE managed_postgres_certificate_authority_providers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('local_dev', 'acme', 'external_pki')),
    issuer_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    default_for_managed_postgres BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX managed_postgres_ca_providers_default_idx
    ON managed_postgres_certificate_authority_providers(default_for_managed_postgres)
    WHERE default_for_managed_postgres = true;

CREATE INDEX managed_postgres_ca_providers_status_idx
    ON managed_postgres_certificate_authority_providers(status);

INSERT INTO managed_postgres_certificate_authority_providers
    (id, name, kind, issuer_ref, status, default_for_managed_postgres)
VALUES
    ('ca_local_dev', 'Local development CA', 'local_dev', 'palimpsest-owned-local-dev-ca', 'active', true);
