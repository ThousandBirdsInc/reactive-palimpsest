ALTER TABLE node_host_agent_credentials
    ADD COLUMN revoked_at TIMESTAMPTZ,
    ADD COLUMN last_used_at TIMESTAMPTZ,
    ADD COLUMN last_used_operation TEXT;

UPDATE node_host_agent_credentials
SET revoked_at = rotated_at
WHERE state = 'revoked'
  AND revoked_at IS NULL;

CREATE INDEX node_host_agent_credentials_last_used_idx
    ON node_host_agent_credentials(host_id, last_used_at DESC)
    WHERE last_used_at IS NOT NULL;
