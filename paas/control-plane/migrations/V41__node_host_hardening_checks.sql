CREATE TABLE node_host_hardening_checks (
    id TEXT PRIMARY KEY,
    host_id TEXT NOT NULL REFERENCES node_hosts(id),
    status TEXT NOT NULL CHECK (status IN ('passing', 'warning', 'failing')),
    image_ref TEXT NOT NULL,
    os_release TEXT NOT NULL,
    kernel_version TEXT NOT NULL,
    postgres_major_min INTEGER NOT NULL CHECK (postgres_major_min >= 18),
    container_runtime TEXT NOT NULL,
    disk_encryption BOOLEAN NOT NULL,
    firewall_enabled BOOLEAN NOT NULL,
    unattended_upgrades BOOLEAN NOT NULL,
    last_patched_at TIMESTAMPTZ,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message TEXT
);

CREATE INDEX node_host_hardening_checks_host_status_idx
    ON node_host_hardening_checks(host_id, status, checked_at DESC);
