ALTER TABLE agent_commands
    ADD COLUMN operation_token_hash TEXT,
    ADD COLUMN operation_token_expires_at TIMESTAMPTZ;

CREATE INDEX agent_commands_operation_token_expiry_idx
    ON agent_commands(operation_token_expires_at)
    WHERE operation_token_expires_at IS NOT NULL;
