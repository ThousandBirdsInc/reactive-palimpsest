ALTER TABLE secret_refs
    ADD COLUMN encrypted_material TEXT,
    ADD COLUMN key_ref TEXT;

ALTER TABLE secret_refs
    ADD CONSTRAINT secret_refs_material_check
    CHECK (
        (provider = 'local-dev' AND secret_material IS NOT NULL)
        OR (provider <> 'local-dev' AND encrypted_material IS NOT NULL AND key_ref IS NOT NULL)
    );
