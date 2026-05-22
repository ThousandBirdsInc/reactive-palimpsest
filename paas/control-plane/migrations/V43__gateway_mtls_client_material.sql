ALTER TABLE gateway_routes
    ADD COLUMN mtls_client_certificate_secret_ref TEXT,
    ADD COLUMN mtls_client_private_key_secret_ref TEXT,
    ADD COLUMN mtls_server_name TEXT;

ALTER TABLE gateway_routes
    ADD CONSTRAINT gateway_routes_mtls_client_material_check CHECK (
        (
            tls_policy = 'terminate_at_gateway'
            AND mtls_ca_secret_ref IS NULL
            AND mtls_client_certificate_secret_ref IS NULL
            AND mtls_client_private_key_secret_ref IS NULL
            AND mtls_server_name IS NULL
        )
        OR (
            tls_policy = 'mutual_tls_to_sync'
            AND mtls_ca_secret_ref IS NOT NULL
            AND (
                (
                    mtls_client_certificate_secret_ref IS NULL
                    AND mtls_client_private_key_secret_ref IS NULL
                )
                OR (
                    mtls_client_certificate_secret_ref IS NOT NULL
                    AND mtls_client_private_key_secret_ref IS NOT NULL
                )
            )
        )
    );
