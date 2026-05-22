CREATE TABLE managed_postgres_endpoint_certificates (
    id TEXT PRIMARY KEY,
    environment_id TEXT NOT NULL REFERENCES managed_postgres_endpoints(environment_id),
    listen_addr TEXT NOT NULL,
    common_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('provisioning', 'active', 'rotating', 'revoked', 'failed')),
    certificate_secret_ref TEXT NOT NULL REFERENCES secret_refs(id),
    private_key_secret_ref TEXT NOT NULL REFERENCES secret_refs(id),
    not_before TEXT NOT NULL,
    not_after TEXT NOT NULL,
    fingerprint_sha256 TEXT NOT NULL,
    issued_by TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE managed_postgres_endpoints
    ADD COLUMN active_certificate_id TEXT REFERENCES managed_postgres_endpoint_certificates(id);

CREATE INDEX managed_postgres_endpoint_certificates_environment_idx
    ON managed_postgres_endpoint_certificates(environment_id);

CREATE INDEX managed_postgres_endpoint_certificates_status_idx
    ON managed_postgres_endpoint_certificates(status);

CREATE UNIQUE INDEX managed_postgres_endpoint_certificates_active_environment_idx
    ON managed_postgres_endpoint_certificates(environment_id)
    WHERE status = 'active';
