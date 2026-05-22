CREATE TABLE node_host_agent_credentials (
    id TEXT PRIMARY KEY,
    host_id TEXT NOT NULL REFERENCES node_hosts(id),
    secret_ref TEXT NOT NULL REFERENCES secret_refs(id),
    state TEXT NOT NULL CHECK (state IN ('active', 'rotated', 'revoked')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rotated_at TIMESTAMPTZ,
    UNIQUE (host_id, id)
);

CREATE INDEX node_host_agent_credentials_host_state_idx
    ON node_host_agent_credentials(host_id, state);
