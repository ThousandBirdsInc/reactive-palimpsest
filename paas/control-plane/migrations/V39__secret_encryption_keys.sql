CREATE TABLE secret_encryption_keys (
    key_ref TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'retiring', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ
);

CREATE INDEX secret_encryption_keys_status_idx
    ON secret_encryption_keys(status);
