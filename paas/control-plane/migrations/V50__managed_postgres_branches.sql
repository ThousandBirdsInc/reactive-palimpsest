CREATE TABLE managed_postgres_branches (
    id TEXT PRIMARY KEY,
    cluster_id TEXT NOT NULL REFERENCES managed_postgres_clusters(id),
    name TEXT NOT NULL,
    parent_branch_id TEXT REFERENCES managed_postgres_branches(id),
    mode TEXT NOT NULL CHECK (mode IN ('head_cow', 'point_in_time')),
    source_database TEXT NOT NULL,
    branch_database TEXT,
    branch_cluster_id TEXT REFERENCES managed_postgres_clusters(id),
    created_from_lsn TEXT,
    redaction_policy_id TEXT REFERENCES managed_postgres_clone_redaction_policies(id),
    lifecycle_state TEXT NOT NULL
        CHECK (lifecycle_state IN ('creating', 'ready', 'failed', 'deleting', 'deleted')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    -- A head_cow branch points at a database; a point_in_time branch points at a
    -- restored cluster. Exactly one backing reference is set.
    CONSTRAINT managed_postgres_branches_backing_ref CHECK (
        (mode = 'head_cow' AND branch_database IS NOT NULL AND branch_cluster_id IS NULL)
        OR (mode = 'point_in_time' AND branch_cluster_id IS NOT NULL AND branch_database IS NULL)
    )
);

-- Branch names are unique per cluster among live (non-deleted) branches; names
-- may be reused after a branch is tombstoned.
CREATE UNIQUE INDEX managed_postgres_branches_cluster_name_active_idx
    ON managed_postgres_branches(cluster_id, name)
    WHERE deleted_at IS NULL;

CREATE INDEX managed_postgres_branches_parent_idx
    ON managed_postgres_branches(parent_branch_id)
    WHERE deleted_at IS NULL;

CREATE INDEX managed_postgres_branches_cluster_idx
    ON managed_postgres_branches(cluster_id)
    WHERE deleted_at IS NULL;
