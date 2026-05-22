ALTER TABLE managed_postgres_endpoint_certificates
    ALTER COLUMN certificate_secret_ref DROP NOT NULL;

CREATE TABLE managed_postgres_acme_orders (
    id TEXT PRIMARY KEY,
    certificate_id TEXT NOT NULL REFERENCES managed_postgres_endpoint_certificates(id),
    environment_id TEXT NOT NULL REFERENCES managed_postgres_endpoints(environment_id),
    ca_provider_id TEXT NOT NULL REFERENCES managed_postgres_certificate_authority_providers(id),
    common_name TEXT NOT NULL,
    challenge_type TEXT NOT NULL CHECK (challenge_type IN ('http_01')),
    challenge_token TEXT NOT NULL,
    key_authorization_secret_ref TEXT NOT NULL REFERENCES secret_refs(id),
    csr_secret_ref TEXT NOT NULL REFERENCES secret_refs(id),
    directory_url TEXT NOT NULL,
    account_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending_challenge', 'ready_to_finalize', 'succeeded', 'failed')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX managed_postgres_acme_orders_certificate_idx
    ON managed_postgres_acme_orders(certificate_id);

CREATE INDEX managed_postgres_acme_orders_environment_idx
    ON managed_postgres_acme_orders(environment_id);

CREATE INDEX managed_postgres_acme_orders_status_idx
    ON managed_postgres_acme_orders(status);
