CREATE TABLE secret_rewrap_plans (
    id TEXT PRIMARY KEY,
    source_key_ref TEXT NOT NULL REFERENCES secret_encryption_keys(key_ref),
    target_key_ref TEXT NOT NULL REFERENCES secret_encryption_keys(key_ref),
    status TEXT NOT NULL CHECK (status IN ('planned', 'running', 'succeeded', 'failed')),
    matched_secret_count INTEGER NOT NULL DEFAULT 0 CHECK (matched_secret_count >= 0),
    rewrapped_secret_count INTEGER NOT NULL DEFAULT 0 CHECK (rewrapped_secret_count >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CHECK (source_key_ref <> target_key_ref),
    CHECK (rewrapped_secret_count <= matched_secret_count)
);

CREATE INDEX secret_rewrap_plans_status_idx
    ON secret_rewrap_plans(status);

CREATE INDEX secret_rewrap_plans_source_target_idx
    ON secret_rewrap_plans(source_key_ref, target_key_ref);
