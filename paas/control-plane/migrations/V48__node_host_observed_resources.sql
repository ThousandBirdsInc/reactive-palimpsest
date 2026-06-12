CREATE TABLE node_host_observed_clusters (
    host_id TEXT NOT NULL REFERENCES node_hosts(id) ON DELETE CASCADE,
    cluster_id TEXT NOT NULL,
    data_dir TEXT NOT NULL,
    postgres_running BOOLEAN NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (host_id, cluster_id)
);

CREATE INDEX node_host_observed_clusters_running_idx
    ON node_host_observed_clusters(host_id, postgres_running);

CREATE TABLE node_host_observed_sync_deployments (
    host_id TEXT NOT NULL REFERENCES node_hosts(id) ON DELETE CASCADE,
    deployment_id TEXT NOT NULL,
    running BOOLEAN NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (host_id, deployment_id)
);

CREATE INDEX node_host_observed_sync_deployments_running_idx
    ON node_host_observed_sync_deployments(host_id, running);
