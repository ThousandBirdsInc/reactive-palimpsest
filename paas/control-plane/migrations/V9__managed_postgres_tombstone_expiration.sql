ALTER TABLE managed_postgres_deletion_tombstones
    ADD COLUMN expired_at TIMESTAMPTZ;

CREATE INDEX managed_postgres_deletion_tombstones_expired_idx
    ON managed_postgres_deletion_tombstones(expired_at)
    WHERE expired_at IS NOT NULL;
