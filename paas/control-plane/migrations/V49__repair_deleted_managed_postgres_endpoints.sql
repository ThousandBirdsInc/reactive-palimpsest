UPDATE managed_postgres_endpoints endpoint
SET active_cluster_id = (
        SELECT replacement.id
        FROM managed_postgres_clusters replacement
        WHERE replacement.environment_id = endpoint.environment_id
          AND replacement.lifecycle_state = 'ready'
        ORDER BY replacement.updated_at DESC, replacement.created_at DESC, replacement.id DESC
        LIMIT 1
    ),
    updated_at = now()
WHERE EXISTS (
    SELECT 1
    FROM managed_postgres_clusters active
    WHERE active.id = endpoint.active_cluster_id
      AND active.lifecycle_state = 'deleted'
)
AND EXISTS (
    SELECT 1
    FROM managed_postgres_clusters replacement
    WHERE replacement.environment_id = endpoint.environment_id
      AND replacement.lifecycle_state = 'ready'
);
