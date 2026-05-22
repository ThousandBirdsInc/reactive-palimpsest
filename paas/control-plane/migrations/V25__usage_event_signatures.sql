ALTER TABLE usage_events
    ADD COLUMN signature_key_id TEXT,
    ADD COLUMN signature_algorithm TEXT,
    ADD COLUMN signature TEXT;

ALTER TABLE usage_events
    ADD CONSTRAINT usage_events_signature_check
    CHECK (
        (signature_key_id IS NULL AND signature_algorithm IS NULL AND signature IS NULL)
        OR (
            signature_key_id IS NOT NULL
            AND signature_algorithm = 'hmac_sha256_v1'
            AND signature IS NOT NULL
        )
    );
