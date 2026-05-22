CREATE TABLE managed_postgres_runtime_checks (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    status TEXT NOT NULL CHECK (status IN ('healthy', 'degraded', 'failed')),
    connection_count INTEGER NOT NULL CHECK (connection_count >= 0),
    max_connections INTEGER NOT NULL CHECK (max_connections >= 0),
    replication_slot_lag_bytes BIGINT CHECK (replication_slot_lag_bytes IS NULL OR replication_slot_lag_bytes >= 0),
    long_running_query_count INTEGER NOT NULL CHECK (long_running_query_count >= 0),
    blocked_lock_count INTEGER NOT NULL CHECK (blocked_lock_count >= 0),
    oldest_transaction_age_seconds BIGINT CHECK (oldest_transaction_age_seconds IS NULL OR oldest_transaction_age_seconds >= 0),
    autovacuum_running BOOLEAN NOT NULL,
    error_message TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX managed_postgres_runtime_checks_cluster_status_idx
    ON managed_postgres_runtime_checks(cluster_id, status, checked_at DESC);
