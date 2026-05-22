CREATE UNIQUE INDEX managed_postgres_clusters_host_port_active_idx
    ON managed_postgres_clusters(host_id, host_port)
    WHERE host_id IS NOT NULL
      AND host_port IS NOT NULL
      AND lifecycle_state <> 'deleted';
