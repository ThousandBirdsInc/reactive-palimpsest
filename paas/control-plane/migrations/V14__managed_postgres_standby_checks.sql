CREATE TABLE managed_postgres_standby_checks (
    id TEXT PRIMARY KEY,
    standby_id TEXT NOT NULL REFERENCES managed_postgres_standbys(id),
    source_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    target_cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    slot_name TEXT NOT NULL,
    max_lag_bytes BIGINT NOT NULL CHECK (max_lag_bytes >= 0),
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX managed_postgres_standby_checks_standby_status_idx
    ON managed_postgres_standby_checks(standby_id, status);
