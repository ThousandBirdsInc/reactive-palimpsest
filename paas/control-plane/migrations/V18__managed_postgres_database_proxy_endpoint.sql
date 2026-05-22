ALTER TABLE managed_postgres_endpoints
    ADD COLUMN database_proxy_listen_addr TEXT;

CREATE UNIQUE INDEX managed_postgres_endpoints_database_proxy_listen_addr_idx
    ON managed_postgres_endpoints(database_proxy_listen_addr)
    WHERE database_proxy_listen_addr IS NOT NULL;
