// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Control-plane adapter over the Kubernetes/CloudNativePG runtime.
//!
//! Wraps [`palimpsest_paas_runtime`] so the reconciler can apply managed
//! Postgres desired state to a cluster (or render it in dry-run mode for local
//! development without Kubernetes).

#![allow(clippy::doc_markdown)]

use palimpsest_paas_core::{DatabaseRoleCredential, ManagedPostgresCluster, ManagedPostgresSpec};
use palimpsest_paas_runtime::{KubectlApplier, RenderedManifests, RuntimeConfig};

/// Applies managed-Postgres manifests, or no-ops in dry-run mode.
#[derive(Debug, Clone)]
pub struct ClusterRuntime {
    config: RuntimeConfig,
    applier: KubectlApplier,
    dry_run: bool,
}

impl Default for ClusterRuntime {
    fn default() -> Self {
        Self::from_env()
    }
}

impl ClusterRuntime {
    /// Build from `PALIMPSEST_PAAS_RUNTIME_*` / `PALIMPSEST_PAAS_KUBECTL` env.
    ///
    /// Set `PALIMPSEST_PAAS_RUNTIME_DRY_RUN=true` to skip `kubectl` entirely:
    /// manifests are rendered (and validated) but not applied, and clusters are
    /// reported ready immediately. Useful for local development and tests that
    /// have no Kubernetes cluster.
    #[must_use]
    pub fn from_env() -> Self {
        let dry_run = std::env::var("PALIMPSEST_PAAS_RUNTIME_DRY_RUN")
            .ok()
            .is_some_and(|value| matches!(value.trim(), "1" | "true" | "yes"));
        Self {
            config: RuntimeConfig::from_env(),
            applier: KubectlApplier::default(),
            dry_run,
        }
    }

    #[must_use]
    pub const fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    /// Render the CloudNativePG manifests for a cluster's desired state.
    pub fn render(
        &self,
        cluster: &ManagedPostgresCluster,
        spec: Option<&ManagedPostgresSpec>,
        roles: &[DatabaseRoleCredential],
    ) -> Result<RenderedManifests, RuntimeAdapterError> {
        RenderedManifests::render(cluster, spec, roles, &self.config)
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Render and apply the manifests (server-side apply), unless in dry-run.
    pub fn apply(&self, manifests: &RenderedManifests) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let yaml = manifests
            .to_yaml()
            .map_err(|err| RuntimeAdapterError(err.to_string()))?;
        self.applier
            .apply(&yaml)
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Delete the manifests for a cluster.
    pub fn delete(&self, manifests: &RenderedManifests) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let yaml = manifests
            .to_yaml()
            .map_err(|err| RuntimeAdapterError(err.to_string()))?;
        self.applier
            .delete(&yaml)
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Provision a cluster that recovers another cluster's object-store backups
    /// (restore / clone / point-in-time branch); no-op in dry-run.
    pub fn restore(
        &self,
        target: &ManagedPostgresCluster,
        source: &palimpsest_paas_runtime::RestoreSource,
    ) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let manifests = RenderedManifests::render_restore(target, source, None, &[], &self.config)
            .map_err(|err| RuntimeAdapterError(err.to_string()))?;
        let yaml = manifests
            .to_yaml()
            .map_err(|err| RuntimeAdapterError(err.to_string()))?;
        self.applier
            .apply(&yaml)
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Promote a replica (standby) cluster to a standalone primary by
    /// re-applying it without the replica section, which CloudNativePG detects
    /// as `replica.enabled` going false; no-op in dry-run.
    pub fn promote(&self, cluster: &ManagedPostgresCluster) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let manifests = self.render(cluster, None, &[])?;
        self.apply(&manifests)
    }

    /// Fence (or, with `false`, unfence) all instances of a cluster via the
    /// CloudNativePG fencing annotation, stopping writes during a failover;
    /// no-op in dry-run.
    pub fn fence(
        &self,
        cluster: &ManagedPostgresCluster,
        fenced: bool,
    ) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let namespace = self.config.namespace_for(cluster);
        let name = palimpsest_paas_runtime::resource_name(cluster);
        self.applier
            .annotate(
                "clusters.postgresql.cnpg.io",
                &name,
                &namespace,
                "cnpg.io/fencing",
                fenced.then_some("*"),
            )
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Create an on-demand CloudNativePG `Backup` for a cluster (no-op in
    /// dry-run). The cluster must have object-storage backups configured.
    pub fn create_backup(
        &self,
        cluster: &ManagedPostgresCluster,
        backup_id: &str,
    ) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let backup =
            palimpsest_paas_runtime::render_backup_resource(cluster, backup_id, &self.config);
        let json =
            serde_json::to_string(&backup).map_err(|err| RuntimeAdapterError(err.to_string()))?;
        self.applier
            .apply(&json)
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Read the phase CloudNativePG reports for an on-demand backup
    /// (e.g. `running`, `completed`, `failed`); `None` if not found / dry-run.
    pub fn backup_phase(
        &self,
        cluster: &ManagedPostgresCluster,
        backup_id: &str,
    ) -> Result<Option<String>, RuntimeAdapterError> {
        if self.dry_run {
            return Ok(None);
        }
        let namespace = self.config.namespace_for(cluster);
        let name = palimpsest_paas_runtime::backup_resource_name(cluster, backup_id);
        self.applier
            .resource_field(
                "backups.postgresql.cnpg.io",
                &name,
                &namespace,
                "{.status.phase}",
            )
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Create `target_db` as a copy of `source_db` inside the cluster's primary
    /// (`CREATE DATABASE ... TEMPLATE`); no-op in dry-run. Identifiers must be
    /// validated by the caller. Optionally terminates connections to the source
    /// first (required for a template copy).
    pub fn clone_database(
        &self,
        cluster: &ManagedPostgresCluster,
        source_db: &str,
        target_db: &str,
        terminate_source_connections: bool,
    ) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let (namespace, pod) = self.primary_pod(cluster)?;
        if terminate_source_connections {
            let terminate = format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = '{source_db}' AND pid <> pg_backend_pid();"
            );
            self.psql(&namespace, &pod, &terminate)?;
        }
        let create = format!("CREATE DATABASE \"{target_db}\" TEMPLATE \"{source_db}\";");
        self.psql(&namespace, &pod, &create)
    }

    /// Drop a database from the cluster's primary (terminating connections);
    /// no-op in dry-run. The identifier must be validated by the caller.
    pub fn drop_database(
        &self,
        cluster: &ManagedPostgresCluster,
        database: &str,
    ) -> Result<(), RuntimeAdapterError> {
        if self.dry_run {
            return Ok(());
        }
        let (namespace, pod) = self.primary_pod(cluster)?;
        self.psql(
            &namespace,
            &pod,
            &format!("DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE);"),
        )
    }

    /// Resolve the namespace and primary pod name for a cluster.
    fn primary_pod(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<(String, String), RuntimeAdapterError> {
        let namespace = self.config.namespace_for(cluster);
        let name = palimpsest_paas_runtime::resource_name(cluster);
        let pod = self
            .applier
            .primary_pod(&namespace, &name)
            .map_err(|err| RuntimeAdapterError(err.to_string()))?
            .ok_or_else(|| {
                RuntimeAdapterError(format!("cluster {} has no primary pod", cluster.cluster_id))
            })?;
        Ok((namespace, pod))
    }

    /// Run a single SQL statement on the primary as the postgres superuser.
    fn psql(&self, namespace: &str, pod: &str, sql: &str) -> Result<(), RuntimeAdapterError> {
        self.applier
            .exec(
                namespace,
                pod,
                "postgres",
                &["psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql],
            )
            .map(|_| ())
            .map_err(|err| RuntimeAdapterError(err.to_string()))
    }

    /// Whether CloudNativePG reports the cluster's instances ready.
    ///
    /// In dry-run mode this is always `true` so local provisioning converges.
    pub fn cluster_ready(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<bool, RuntimeAdapterError> {
        if self.dry_run {
            return Ok(true);
        }
        let namespace = self.config.namespace_for(cluster);
        let name = palimpsest_paas_runtime::resource_name(cluster);
        let ready = self
            .applier
            .ready_instances(&namespace, &name)
            .map_err(|err| RuntimeAdapterError(err.to_string()))?;
        Ok(ready >= 1)
    }
}

/// Opaque runtime failure surfaced to the control-plane error type.
#[derive(Debug, thiserror::Error)]
#[error("kubernetes runtime: {0}")]
pub struct RuntimeAdapterError(pub String);
