// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Kubernetes runtime for Palimpsest managed Postgres.
//!
//! This crate replaces the former host-local node agent. Instead of running
//! PostgreSQL binaries directly on owned hosts and supervising them through
//! leased commands, the managed-database runtime is expressed declaratively as
//! [CloudNativePG] custom resources and reconciled onto a Kubernetes cluster.
//!
//! The control plane persists desired state (`ManagedPostgresCluster` plus its
//! `ManagedPostgresSpec`); this crate turns that desired state into a
//! CloudNativePG [`Cluster`](CnpgCluster) (and companion `ScheduledBackup` /
//! `Pooler`) manifest. The CloudNativePG operator then performs all host-local
//! work — instance placement, failover, backups, PITR, minor/major upgrades,
//! and storage resize — which the owned node agent used to do by hand.
//!
//! Rendering is pure and fully unit-testable. Applying manifests to a live
//! cluster is delegated to `kubectl` (see [`KubectlApplier`]), mirroring how
//! the old node agent shelled out to `pg_ctl`/`initdb`.
//!
//! [CloudNativePG]: https://cloudnative-pg.io

// Prose in these docs names products (CloudNativePG, PostgreSQL, Kubernetes,
// kubectl) that `doc_markdown` wants backticked; keep the prose readable.
#![allow(clippy::doc_markdown)]

use std::{
    collections::BTreeMap,
    io::Write,
    process::{Command, Stdio},
};

use palimpsest_paas_core::{
    BackupPolicy, ClusterLifecycleState, DatabaseRoleCredential, DatabaseRoleKind,
    ManagedPostgresCluster, ManagedPostgresSpec, MIN_SUPPORTED_POSTGRES_MAJOR,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// CloudNativePG API group/version that the runtime targets.
pub const CNPG_API_VERSION: &str = "postgresql.cnpg.io/v1";

/// Label key prefix for Palimpsest ownership metadata on rendered resources.
pub const LABEL_PREFIX: &str = "palimpsest.thousandbirds.ai";

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("cluster '{cluster_id}' has unsupported postgres major {major}; managed Postgres requires {min}+")]
    UnsupportedPostgresMajor {
        cluster_id: String,
        major: u16,
        min: u16,
    },
    #[error("failed to render manifest: {0}")]
    Render(#[from] serde_yaml::Error),
    #[error("failed to render manifest: {0}")]
    RenderJson(#[from] serde_json::Error),
    #[error("kubectl invocation failed: {0}")]
    Kubectl(String),
    #[error("io error running kubectl: {0}")]
    Io(#[from] std::io::Error),
}

/// Deployment-wide knobs that are independent of any single cluster.
///
/// These come from the platform installation (Helm values), not from a
/// customer's database intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Container image repository for the managed PostgreSQL image. The major
    /// version tag is appended per cluster.
    pub postgres_image_repository: String,
    /// Kubernetes `StorageClass` for managed-Postgres volumes. `None` uses the
    /// cluster default.
    pub storage_class: Option<String>,
    /// Base object-store URI for WAL archiving and base backups. The cluster id
    /// is appended as a path segment. `None` disables the backup stanza.
    pub backup_object_store_base: Option<String>,
    /// Name of the Kubernetes `Secret` holding object-store credentials for
    /// backups (referenced by the CloudNativePG `barmanObjectStore`).
    pub backup_credentials_secret: Option<String>,
    /// Object-store endpoint URL for S3-compatible stores (e.g. MinIO). `None`
    /// uses the provider default (AWS S3).
    pub backup_object_store_endpoint: Option<String>,
    /// When true, each environment gets its own namespace
    /// (`palimpsest-<environment_id>`); otherwise everything lands in
    /// [`RuntimeConfig::default_namespace`].
    pub namespace_per_environment: bool,
    /// Namespace used when `namespace_per_environment` is false.
    pub default_namespace: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            postgres_image_repository: "ghcr.io/cloudnative-pg/postgresql".to_owned(),
            storage_class: None,
            backup_object_store_base: None,
            backup_credentials_secret: None,
            backup_object_store_endpoint: None,
            namespace_per_environment: true,
            default_namespace: "palimpsest-paas".to_owned(),
        }
    }
}

impl RuntimeConfig {
    /// Read configuration from `PALIMPSEST_PAAS_RUNTIME_*` environment
    /// variables, falling back to defaults.
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(value) = non_empty_env("PALIMPSEST_PAAS_RUNTIME_POSTGRES_IMAGE") {
            config.postgres_image_repository = value;
        }
        config.storage_class = non_empty_env("PALIMPSEST_PAAS_RUNTIME_STORAGE_CLASS");
        config.backup_object_store_base =
            non_empty_env("PALIMPSEST_PAAS_RUNTIME_BACKUP_OBJECT_STORE");
        config.backup_credentials_secret =
            non_empty_env("PALIMPSEST_PAAS_RUNTIME_BACKUP_CREDENTIALS_SECRET");
        config.backup_object_store_endpoint =
            non_empty_env("PALIMPSEST_PAAS_RUNTIME_BACKUP_ENDPOINT");
        if let Some(value) = non_empty_env("PALIMPSEST_PAAS_RUNTIME_DEFAULT_NAMESPACE") {
            config.default_namespace = value;
        }
        if let Some(value) = non_empty_env("PALIMPSEST_PAAS_RUNTIME_NAMESPACE_PER_ENVIRONMENT") {
            config.namespace_per_environment = matches!(value.as_str(), "1" | "true" | "yes");
        }
        config
    }

    #[must_use]
    pub fn namespace_for(&self, cluster: &ManagedPostgresCluster) -> String {
        if self.namespace_per_environment {
            format!("palimpsest-{}", sanitize_dns(&cluster.environment_id))
        } else {
            self.default_namespace.clone()
        }
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Sizing derived from a tier name: instance count and resource requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierProfile {
    pub instances: u8,
    pub cpu_request: String,
    pub memory_request: String,
}

impl TierProfile {
    /// Map a free-form tier string to a sizing profile. High-availability tiers
    /// get three CloudNativePG instances (a primary plus two synchronous-capable
    /// replicas); everything else gets a single instance.
    #[must_use]
    pub fn for_tier(tier: &str) -> Self {
        let normalized = tier.to_ascii_lowercase();
        match normalized.as_str() {
            "ha" | "production" | "prod" | "enterprise" => Self {
                instances: 3,
                cpu_request: "2".to_owned(),
                memory_request: "4Gi".to_owned(),
            },
            "standard" | "business" => Self {
                instances: 2,
                cpu_request: "1".to_owned(),
                memory_request: "2Gi".to_owned(),
            },
            // "dev", "free", and anything unrecognized: a single instance.
            _ => Self {
                instances: 1,
                cpu_request: "500m".to_owned(),
                memory_request: "1Gi".to_owned(),
            },
        }
    }
}

/// Render a CloudNativePG [`Cluster`](CnpgCluster) from desired state.
///
/// `spec` carries the customer's backup and maintenance intent and any
/// explicitly requested roles; when `None`, defaults are used. `roles` are the
/// managed database roles the control plane wants present (the operator wires
/// each to a Kubernetes `Secret` named `<cluster>-role-<role>`).
pub fn render_cluster(
    cluster: &ManagedPostgresCluster,
    spec: Option<&ManagedPostgresSpec>,
    roles: &[DatabaseRoleCredential],
    config: &RuntimeConfig,
) -> Result<CnpgCluster, RuntimeError> {
    render_cluster_inner(cluster, spec, roles, config, None)
}

/// Where a restored / cloned cluster recovers its data from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreSource {
    /// Cluster id whose object-store backups should be recovered.
    pub source_cluster_id: String,
    /// RFC3339 timestamp for point-in-time recovery (branch); `None` recovers
    /// to the end of the available WAL.
    pub recovery_target_time: Option<String>,
}

/// Render a CloudNativePG `Cluster` that bootstraps by recovering another
/// cluster's object-store backups (restore, clone, or point-in-time branch).
///
/// Requires object-storage backups to be configured ([`RuntimeConfig`]).
pub fn render_restore_cluster(
    cluster: &ManagedPostgresCluster,
    source: &RestoreSource,
    spec: Option<&ManagedPostgresSpec>,
    roles: &[DatabaseRoleCredential],
    config: &RuntimeConfig,
) -> Result<CnpgCluster, RuntimeError> {
    render_cluster_inner(cluster, spec, roles, config, Some(source))
}

fn render_cluster_inner(
    cluster: &ManagedPostgresCluster,
    spec: Option<&ManagedPostgresSpec>,
    roles: &[DatabaseRoleCredential],
    config: &RuntimeConfig,
    restore: Option<&RestoreSource>,
) -> Result<CnpgCluster, RuntimeError> {
    let major = cluster.postgres_version.major();
    if major < MIN_SUPPORTED_POSTGRES_MAJOR {
        return Err(RuntimeError::UnsupportedPostgresMajor {
            cluster_id: cluster.cluster_id.clone(),
            major,
            min: MIN_SUPPORTED_POSTGRES_MAJOR,
        });
    }

    let tier = TierProfile::for_tier(&cluster.tier);
    let auto_minor = spec.is_none_or(|spec| spec.maintenance_policy.auto_minor_upgrades);
    // A floating major tag (`:18`) lets the operator pick up minor releases on
    // its own rolling-update schedule; a pinned tag (`:18.1`) freezes it.
    let image_tag = if auto_minor {
        major.to_string()
    } else {
        cluster.postgres_version.as_str().to_owned()
    };

    let backup = config
        .backup_object_store_base
        .as_ref()
        .map(|base| render_backup(base, config, cluster, spec.map(|spec| &spec.backup_policy)));

    // Recovery bootstrap (restore/clone/branch) reads the source cluster's
    // backups from object storage via an `externalClusters` entry; otherwise
    // the cluster bootstraps a fresh database with initdb.
    let (bootstrap, external_clusters) = match restore {
        None => (
            CnpgBootstrap {
                initdb: Some(CnpgInitDb {
                    database: "app".to_owned(),
                    owner: "app".to_owned(),
                }),
                recovery: None,
            },
            None,
        ),
        Some(source) => {
            let base = config.backup_object_store_base.as_ref().ok_or_else(|| {
                RuntimeError::Kubectl(format!(
                    "cannot restore cluster '{}': no backup object store is configured",
                    cluster.cluster_id
                ))
            })?;
            let source_name = "recovery-source".to_owned();
            let external = CnpgExternalCluster {
                name: source_name.clone(),
                barman_object_store: CnpgBarmanObjectStore {
                    destination_path: format!(
                        "{}/{}",
                        base.trim_end_matches('/'),
                        source.source_cluster_id
                    ),
                    endpoint_url: config.backup_object_store_endpoint.clone(),
                    server_name: Some(sanitize_dns(&source.source_cluster_id)),
                    s3_credentials: s3_credentials(config),
                },
            };
            (
                CnpgBootstrap {
                    initdb: None,
                    recovery: Some(CnpgRecovery {
                        source: source_name,
                        recovery_target: source
                            .recovery_target_time
                            .clone()
                            .map(|target_time| CnpgRecoveryTarget { target_time }),
                    }),
                },
                Some(vec![external]),
            )
        }
    };

    let managed_roles: Vec<CnpgManagedRole> = roles
        .iter()
        .map(|role| CnpgManagedRole {
            name: role.name.clone(),
            ensure: "present".to_owned(),
            login: true,
            superuser: matches!(role.kind, DatabaseRoleKind::Support),
            replication: matches!(role.kind, DatabaseRoleKind::Replication),
            password_secret: Some(CnpgSecretRef {
                name: format!(
                    "{}-role-{}",
                    resource_name(cluster),
                    sanitize_dns(&role.name)
                ),
            }),
        })
        .collect();

    let spec_body = CnpgClusterSpec {
        instances: tier.instances,
        image_name: format!("{}:{}", config.postgres_image_repository, image_tag),
        image_pull_policy: "IfNotPresent".to_owned(),
        primary_update_strategy: "unsupervised".to_owned(),
        storage: CnpgStorage {
            size: format!("{}Gi", cluster.storage_gib.max(1)),
            storage_class: config.storage_class.clone(),
        },
        resources: CnpgResources {
            requests: BTreeMap::from([
                ("cpu".to_owned(), tier.cpu_request),
                ("memory".to_owned(), tier.memory_request),
            ]),
        },
        bootstrap,
        managed: (!managed_roles.is_empty()).then_some(CnpgManaged {
            roles: managed_roles,
        }),
        backup,
        external_clusters,
    };

    Ok(CnpgCluster {
        api_version: CNPG_API_VERSION.to_owned(),
        kind: "Cluster".to_owned(),
        metadata: object_meta(cluster, config),
        spec: spec_body,
    })
}

fn render_backup(
    base: &str,
    config: &RuntimeConfig,
    cluster: &ManagedPostgresCluster,
    policy: Option<&BackupPolicy>,
) -> CnpgBackup {
    let retention_days = policy.map_or(7, |policy| {
        u32::from(policy.pitr_window_hours).div_ceil(24).max(1)
    });
    CnpgBackup {
        retention_policy: format!("{retention_days}d"),
        barman_object_store: CnpgBarmanObjectStore {
            destination_path: format!("{}/{}", base.trim_end_matches('/'), cluster.cluster_id),
            endpoint_url: config.backup_object_store_endpoint.clone(),
            server_name: None,
            s3_credentials: s3_credentials(config),
        },
    }
}

/// Reference the configured object-store credentials secret, if any.
fn s3_credentials(config: &RuntimeConfig) -> Option<CnpgS3Credentials> {
    config
        .backup_credentials_secret
        .as_ref()
        .map(|secret| CnpgS3Credentials {
            access_key_id: CnpgSecretKeyRef {
                name: secret.clone(),
                key: "ACCESS_KEY_ID".to_owned(),
            },
            secret_access_key: CnpgSecretKeyRef {
                name: secret.clone(),
                key: "ACCESS_SECRET_KEY".to_owned(),
            },
        })
}

/// Kubernetes object name for an on-demand backup of a cluster.
#[must_use]
pub fn backup_resource_name(cluster: &ManagedPostgresCluster, backup_id: &str) -> String {
    format!("{}-{}", resource_name(cluster), sanitize_dns(backup_id))
}

/// Render a CloudNativePG `Backup` resource for an on-demand base backup.
///
/// The backup uses the `barmanObjectStore` method, so the cluster must be
/// configured with object-storage backups (see [`render_backup`]); otherwise
/// the operator rejects the backup for lack of a target.
#[must_use]
pub fn render_backup_resource(
    cluster: &ManagedPostgresCluster,
    backup_id: &str,
    config: &RuntimeConfig,
) -> CnpgBackupResource {
    CnpgBackupResource {
        api_version: CNPG_API_VERSION.to_owned(),
        kind: "Backup".to_owned(),
        metadata: ObjectMeta {
            name: backup_resource_name(cluster, backup_id),
            namespace: config.namespace_for(cluster),
            labels: ownership_labels(cluster),
            annotations: BTreeMap::new(),
        },
        spec: CnpgBackupResourceSpec {
            cluster: CnpgLocalRef {
                name: resource_name(cluster),
            },
            method: "barmanObjectStore".to_owned(),
        },
    }
}

/// Render a CloudNativePG `ScheduledBackup` for the cluster's base-backup
/// cadence. Returns `None` when backups are not configured.
#[must_use]
pub fn render_scheduled_backup(
    cluster: &ManagedPostgresCluster,
    spec: Option<&ManagedPostgresSpec>,
    config: &RuntimeConfig,
) -> Option<CnpgScheduledBackup> {
    config.backup_object_store_base.as_ref()?;
    let interval_hours = spec
        .map_or(24, |spec| spec.backup_policy.base_backup_interval_hours)
        .max(1);
    // CloudNativePG schedules use a 6-field (seconds-leading) cron expression.
    let schedule = if interval_hours >= 24 {
        "0 0 0 * * *".to_owned()
    } else {
        format!("0 0 */{interval_hours} * * *")
    };
    Some(CnpgScheduledBackup {
        api_version: CNPG_API_VERSION.to_owned(),
        kind: "ScheduledBackup".to_owned(),
        metadata: ObjectMeta {
            name: format!("{}-base", resource_name(cluster)),
            namespace: config.namespace_for(cluster),
            labels: ownership_labels(cluster),
            annotations: BTreeMap::new(),
        },
        spec: CnpgScheduledBackupSpec {
            schedule,
            backup_owner_reference: "self".to_owned(),
            cluster: CnpgLocalRef {
                name: resource_name(cluster),
            },
        },
    })
}

/// A rendered set of manifests for one managed-Postgres cluster, ready to apply.
#[derive(Debug, Clone)]
pub struct RenderedManifests {
    pub cluster: CnpgCluster,
    pub scheduled_backup: Option<CnpgScheduledBackup>,
}

impl RenderedManifests {
    /// Render every manifest for a cluster from desired state.
    pub fn render(
        cluster: &ManagedPostgresCluster,
        spec: Option<&ManagedPostgresSpec>,
        roles: &[DatabaseRoleCredential],
        config: &RuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            cluster: render_cluster(cluster, spec, roles, config)?,
            scheduled_backup: render_scheduled_backup(cluster, spec, config),
        })
    }

    /// Render manifests for a cluster that recovers another cluster's backups
    /// (restore / clone / point-in-time branch).
    pub fn render_restore(
        cluster: &ManagedPostgresCluster,
        source: &RestoreSource,
        spec: Option<&ManagedPostgresSpec>,
        roles: &[DatabaseRoleCredential],
        config: &RuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            cluster: render_restore_cluster(cluster, source, spec, roles, config)?,
            scheduled_backup: render_scheduled_backup(cluster, spec, config),
        })
    }

    /// Serialize all manifests as a multi-document stream for `kubectl apply`.
    ///
    /// Each manifest is emitted as pretty-printed JSON separated by `---`. JSON
    /// is a strict subset of YAML 1.2, so the result is a valid YAML stream that
    /// `kubectl` accepts, but unlike YAML it always quotes string scalars. That
    /// matters because kubectl parses manifests with YAML 1.1 semantics, under
    /// which bare tokens like `on`/`off`/`yes`/`no` become booleans and values
    /// like `1.0` become floats. CloudNativePG's hibernation annotation value
    /// must stay the string `"on"`, so emitting JSON keeps it a string instead
    /// of being coerced to a boolean (which kubectl rejects for annotations).
    pub fn to_yaml(&self) -> Result<String, RuntimeError> {
        let mut out = serde_json::to_string_pretty(&self.cluster)?;
        out.push('\n');
        if let Some(scheduled_backup) = &self.scheduled_backup {
            out.push_str("---\n");
            out.push_str(&serde_json::to_string_pretty(scheduled_backup)?);
            out.push('\n');
        }
        Ok(out)
    }
}

fn object_meta(cluster: &ManagedPostgresCluster, config: &RuntimeConfig) -> ObjectMeta {
    // A Stopped/Stopping cluster is expressed declaratively to CloudNativePG via
    // the hibernation annotation, which scales the instances down while keeping
    // the data volumes (the operator equivalent of pausing).
    let mut annotations = BTreeMap::new();
    if matches!(
        cluster.lifecycle_state,
        ClusterLifecycleState::Stopping | ClusterLifecycleState::Stopped
    ) {
        annotations.insert("cnpg.io/hibernation".to_owned(), "on".to_owned());
    }
    ObjectMeta {
        name: resource_name(cluster),
        namespace: config.namespace_for(cluster),
        labels: ownership_labels(cluster),
        annotations,
    }
}

fn ownership_labels(cluster: &ManagedPostgresCluster) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            format!("{LABEL_PREFIX}/organization"),
            sanitize_dns(&cluster.organization_id),
        ),
        (
            format!("{LABEL_PREFIX}/project"),
            sanitize_dns(&cluster.project_id),
        ),
        (
            format!("{LABEL_PREFIX}/environment"),
            sanitize_dns(&cluster.environment_id),
        ),
        (
            format!("{LABEL_PREFIX}/cluster"),
            sanitize_dns(&cluster.cluster_id),
        ),
        (format!("{LABEL_PREFIX}/tier"), sanitize_dns(&cluster.tier)),
    ])
}

/// Kubernetes object name for a cluster: a DNS-safe form of its id.
#[must_use]
pub fn resource_name(cluster: &ManagedPostgresCluster) -> String {
    sanitize_dns(&cluster.cluster_id)
}

/// Coerce an arbitrary identifier into an RFC 1123 DNS label fragment.
fn sanitize_dns(value: &str) -> String {
    let mut out: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    let trimmed = out.trim_matches('-').to_owned();
    let trimmed = if trimmed.is_empty() {
        "x".to_owned()
    } else {
        trimmed
    };
    // DNS labels are capped at 63 characters.
    trimmed.chars().take(63).collect::<String>()
}

/// Applies and deletes rendered manifests via the `kubectl` CLI.
///
/// Shelling out to `kubectl` keeps the runtime dependency-light and lets the
/// operator use the ambient kubeconfig / in-cluster service account, exactly
/// as a Kubernetes controller would.
#[derive(Debug, Clone)]
pub struct KubectlApplier {
    binary: String,
    context: Option<String>,
}

impl Default for KubectlApplier {
    fn default() -> Self {
        Self {
            binary: non_empty_env("PALIMPSEST_PAAS_KUBECTL")
                .unwrap_or_else(|| "kubectl".to_owned()),
            context: non_empty_env("PALIMPSEST_PAAS_KUBE_CONTEXT"),
        }
    }
}

impl KubectlApplier {
    #[must_use]
    pub fn new(binary: impl Into<String>, context: Option<String>) -> Self {
        Self {
            binary: binary.into(),
            context,
        }
    }

    /// Server-side apply a multi-document manifest stream.
    pub fn apply(&self, manifests: &str) -> Result<(), RuntimeError> {
        self.run(&["apply", "--server-side", "-f", "-"], Some(manifests))
            .map(|_| ())
    }

    /// Delete the resources described by a manifest stream.
    pub fn delete(&self, manifests: &str) -> Result<(), RuntimeError> {
        self.run(
            &["delete", "--ignore-not-found", "-f", "-"],
            Some(manifests),
        )
        .map(|_| ())
    }

    /// Read the number of ready instances CloudNativePG reports for a cluster.
    ///
    /// Returns 0 when the cluster exists but has no ready instances yet, or is
    /// not found.
    pub fn ready_instances(&self, namespace: &str, name: &str) -> Result<u32, RuntimeError> {
        let stdout = self.run(
            &[
                "get",
                "clusters.postgresql.cnpg.io",
                name,
                "-n",
                namespace,
                "--ignore-not-found",
                "-o",
                "jsonpath={.status.readyInstances}",
            ],
            None,
        )?;
        Ok(stdout.trim().parse().unwrap_or(0))
    }

    /// Read a single jsonpath field from a named resource. Returns `None` when
    /// the resource does not exist (or the field is empty).
    pub fn resource_field(
        &self,
        resource: &str,
        name: &str,
        namespace: &str,
        jsonpath: &str,
    ) -> Result<Option<String>, RuntimeError> {
        let stdout = self.run(
            &[
                "get",
                resource,
                name,
                "-n",
                namespace,
                "--ignore-not-found",
                "-o",
                &format!("jsonpath={jsonpath}"),
            ],
            None,
        )?;
        let trimmed = stdout.trim();
        Ok((!trimmed.is_empty()).then(|| trimmed.to_owned()))
    }

    fn run(&self, args: &[&str], stdin: Option<&str>) -> Result<String, RuntimeError> {
        let mut command = Command::new(&self.binary);
        if let Some(context) = &self.context {
            command.arg("--context").arg(context);
        }
        command.args(args);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let mut child = command.spawn()?;
        if let Some(stdin) = stdin {
            child
                .stdin
                .take()
                .ok_or_else(|| RuntimeError::Kubectl("failed to open kubectl stdin".to_owned()))?
                .write_all(stdin.as_bytes())?;
        }
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(RuntimeError::Kubectl(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

// ---------------------------------------------------------------------------
// CloudNativePG resource models
//
// These mirror the subset of the `postgresql.cnpg.io/v1` schema the platform
// uses. They are intentionally serialization-only structs; CloudNativePG owns
// the full CRD.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgCluster {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: CnpgClusterSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgClusterSpec {
    pub instances: u8,
    #[serde(rename = "imageName")]
    pub image_name: String,
    #[serde(rename = "imagePullPolicy")]
    pub image_pull_policy: String,
    #[serde(rename = "primaryUpdateStrategy")]
    pub primary_update_strategy: String,
    pub storage: CnpgStorage,
    pub resources: CnpgResources,
    pub bootstrap: CnpgBootstrap,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed: Option<CnpgManaged>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup: Option<CnpgBackup>,
    #[serde(rename = "externalClusters", skip_serializing_if = "Option::is_none")]
    pub external_clusters: Option<Vec<CnpgExternalCluster>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgStorage {
    pub size: String,
    #[serde(rename = "storageClass", skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgResources {
    pub requests: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgBootstrap {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initdb: Option<CnpgInitDb>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<CnpgRecovery>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgInitDb {
    pub database: String,
    pub owner: String,
}

/// Bootstrap from a backup in an external object store (restore / clone / PITR).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgRecovery {
    /// Name of the entry in `externalClusters` to recover from.
    pub source: String,
    #[serde(rename = "recoveryTarget", skip_serializing_if = "Option::is_none")]
    pub recovery_target: Option<CnpgRecoveryTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgRecoveryTarget {
    /// RFC3339 timestamp for point-in-time recovery.
    #[serde(rename = "targetTime")]
    pub target_time: String,
}

/// An external cluster CloudNativePG can recover from via object storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgExternalCluster {
    pub name: String,
    #[serde(rename = "barmanObjectStore")]
    pub barman_object_store: CnpgBarmanObjectStore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgManaged {
    pub roles: Vec<CnpgManagedRole>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgManagedRole {
    pub name: String,
    pub ensure: String,
    pub login: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub superuser: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub replication: bool,
    #[serde(rename = "passwordSecret", skip_serializing_if = "Option::is_none")]
    pub password_secret: Option<CnpgSecretRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgSecretRef {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgBackup {
    #[serde(rename = "retentionPolicy")]
    pub retention_policy: String,
    #[serde(rename = "barmanObjectStore")]
    pub barman_object_store: CnpgBarmanObjectStore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgBarmanObjectStore {
    #[serde(rename = "destinationPath")]
    pub destination_path: String,
    #[serde(rename = "endpointURL", skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// Source server name within the store; set when recovering another
    /// cluster's backups (defaults to the owning cluster otherwise).
    #[serde(rename = "serverName", skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
    #[serde(rename = "s3Credentials", skip_serializing_if = "Option::is_none")]
    pub s3_credentials: Option<CnpgS3Credentials>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgS3Credentials {
    #[serde(rename = "accessKeyId")]
    pub access_key_id: CnpgSecretKeyRef,
    #[serde(rename = "secretAccessKey")]
    pub secret_access_key: CnpgSecretKeyRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgSecretKeyRef {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgScheduledBackup {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: CnpgScheduledBackupSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgScheduledBackupSpec {
    pub schedule: String,
    #[serde(rename = "backupOwnerReference")]
    pub backup_owner_reference: String,
    pub cluster: CnpgLocalRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgLocalRef {
    pub name: String,
}

/// A CloudNativePG `Backup` resource: an on-demand base backup of a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgBackupResource {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: CnpgBackupResourceSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgBackupResourceSpec {
    pub cluster: CnpgLocalRef,
    /// Backup target: `barmanObjectStore` (object storage) or `volumeSnapshot`.
    pub method: String,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;
    use palimpsest_paas_core::{ClusterLifecycleState, MaintenancePolicy, PostgresVersion};

    fn sample_cluster(tier: &str) -> ManagedPostgresCluster {
        ManagedPostgresCluster {
            cluster_id: "cluster_123".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            region: "us-east-1".to_owned(),
            postgres_version: PostgresVersion::new("18").unwrap(),
            tier: tier.to_owned(),
            storage_gib: 20,
            lifecycle_state: ClusterLifecycleState::Requested,
            host_assignment: None,
        }
    }

    fn sample_spec(auto_minor: bool) -> ManagedPostgresSpec {
        ManagedPostgresSpec {
            cluster_id: "cluster_123".to_owned(),
            tier: "dev".to_owned(),
            storage_gib: 20,
            backup_policy: BackupPolicy {
                pitr_window_hours: 72,
                base_backup_interval_hours: 12,
            },
            maintenance_policy: MaintenancePolicy {
                window: "sun:04:00-05:00Z".to_owned(),
                auto_minor_upgrades: auto_minor,
            },
            roles: Vec::new(),
        }
    }

    #[test]
    fn renders_cnpg_cluster_for_dev_tier() {
        let cluster = sample_cluster("dev");
        let config = RuntimeConfig::default();
        let rendered = render_cluster(&cluster, None, &[], &config).unwrap();

        assert_eq!(rendered.api_version, "postgresql.cnpg.io/v1");
        assert_eq!(rendered.kind, "Cluster");
        assert_eq!(rendered.metadata.name, "cluster-123");
        assert_eq!(rendered.metadata.namespace, "palimpsest-env-123");
        assert_eq!(rendered.spec.instances, 1);
        assert_eq!(rendered.spec.storage.size, "20Gi");
        // Auto-minor defaults true -> floating major tag.
        assert_eq!(
            rendered.spec.image_name,
            "ghcr.io/cloudnative-pg/postgresql:18"
        );
    }

    #[test]
    fn ha_tier_gets_three_instances() {
        let rendered =
            render_cluster(&sample_cluster("ha"), None, &[], &RuntimeConfig::default()).unwrap();
        assert_eq!(rendered.spec.instances, 3);
    }

    #[test]
    fn pinned_image_when_auto_minor_disabled() {
        let cluster = ManagedPostgresCluster {
            postgres_version: PostgresVersion::new("18.2").unwrap(),
            ..sample_cluster("dev")
        };
        let spec = sample_spec(false);
        let rendered =
            render_cluster(&cluster, Some(&spec), &[], &RuntimeConfig::default()).unwrap();
        assert_eq!(
            rendered.spec.image_name,
            "ghcr.io/cloudnative-pg/postgresql:18.2"
        );
    }

    #[test]
    fn managed_roles_reference_secrets() {
        let roles = vec![
            DatabaseRoleCredential {
                name: "cluster_123_app".to_owned(),
                kind: DatabaseRoleKind::App,
                password: "secret".to_owned(),
                privileges: Vec::new(),
            },
            DatabaseRoleCredential {
                name: "cluster_123_replication".to_owned(),
                kind: DatabaseRoleKind::Replication,
                password: "secret".to_owned(),
                privileges: Vec::new(),
            },
        ];
        let rendered = render_cluster(
            &sample_cluster("dev"),
            None,
            &roles,
            &RuntimeConfig::default(),
        )
        .unwrap();
        let managed = rendered.spec.managed.expect("managed roles present");
        assert_eq!(managed.roles.len(), 2);
        assert!(managed.roles[1].replication);
        assert_eq!(
            managed.roles[0].password_secret.as_ref().unwrap().name,
            "cluster-123-role-cluster-123-app"
        );
    }

    #[test]
    fn backup_stanza_and_scheduled_backup_when_object_store_configured() {
        let config = RuntimeConfig {
            backup_object_store_base: Some("s3://backups".to_owned()),
            backup_credentials_secret: Some("backup-creds".to_owned()),
            ..RuntimeConfig::default()
        };
        let cluster = sample_cluster("dev");
        let spec = sample_spec(true);
        let rendered = render_cluster(&cluster, Some(&spec), &[], &config).unwrap();
        let backup = rendered.spec.backup.expect("backup configured");
        assert_eq!(
            backup.barman_object_store.destination_path,
            "s3://backups/cluster_123"
        );
        // 72h PITR window -> 3 days retention.
        assert_eq!(backup.retention_policy, "3d");

        let scheduled = render_scheduled_backup(&cluster, Some(&spec), &config)
            .expect("scheduled backup rendered");
        assert_eq!(scheduled.spec.schedule, "0 0 */12 * * *");
        assert_eq!(scheduled.spec.cluster.name, "cluster-123");
    }

    #[test]
    fn no_backup_without_object_store() {
        let rendered =
            render_cluster(&sample_cluster("dev"), None, &[], &RuntimeConfig::default()).unwrap();
        assert!(rendered.spec.backup.is_none());
        assert!(
            render_scheduled_backup(&sample_cluster("dev"), None, &RuntimeConfig::default())
                .is_none()
        );
    }

    #[test]
    fn renders_multi_document_yaml() {
        let config = RuntimeConfig {
            backup_object_store_base: Some("s3://backups".to_owned()),
            ..RuntimeConfig::default()
        };
        let manifests =
            RenderedManifests::render(&sample_cluster("dev"), None, &[], &config).unwrap();
        let stream = manifests.to_yaml().unwrap();
        assert!(stream.contains("\"kind\": \"Cluster\""));
        assert!(stream.contains("\"kind\": \"ScheduledBackup\""));
        assert!(stream.contains("---"));
        // JSON is a valid YAML 1.2 stream and round-trips through a YAML parser.
        for doc in stream.split("\n---\n") {
            serde_yaml::from_str::<serde_yaml::Value>(doc).expect("each document parses as YAML");
        }
    }

    #[test]
    fn renders_recovery_bootstrap_for_restore() {
        let config = RuntimeConfig {
            backup_object_store_base: Some("s3://backups".to_owned()),
            backup_credentials_secret: Some("backup-credentials".to_owned()),
            backup_object_store_endpoint: Some("http://minio:9000".to_owned()),
            ..RuntimeConfig::default()
        };
        let target = ManagedPostgresCluster {
            cluster_id: "restored".to_owned(),
            ..sample_cluster("dev")
        };
        let source = RestoreSource {
            source_cluster_id: "source_db".to_owned(),
            recovery_target_time: Some("2026-06-15T00:00:00Z".to_owned()),
        };
        let cluster = render_restore_cluster(&target, &source, None, &[], &config).unwrap();

        // Bootstraps via recovery, not initdb.
        assert!(cluster.spec.bootstrap.initdb.is_none());
        let recovery = cluster.spec.bootstrap.recovery.expect("recovery bootstrap");
        assert_eq!(recovery.source, "recovery-source");
        assert_eq!(
            recovery.recovery_target.unwrap().target_time,
            "2026-06-15T00:00:00Z"
        );
        let external = cluster.spec.external_clusters.expect("external clusters");
        assert_eq!(external.len(), 1);
        assert_eq!(external[0].name, "recovery-source");
        assert_eq!(
            external[0].barman_object_store.destination_path,
            "s3://backups/source_db"
        );
        // serverName is the sanitized source id so barman finds its backups.
        assert_eq!(
            external[0].barman_object_store.server_name.as_deref(),
            Some("source-db")
        );
    }

    #[test]
    fn renders_on_demand_backup_resource() {
        let cluster = sample_cluster("dev");
        let backup = render_backup_resource(&cluster, "backup_42", &RuntimeConfig::default());
        assert_eq!(backup.kind, "Backup");
        assert_eq!(backup.spec.cluster.name, resource_name(&cluster));
        assert_eq!(backup.spec.method, "barmanObjectStore");
        assert_eq!(
            backup.metadata.name,
            backup_resource_name(&cluster, "backup_42")
        );
        // The name must be DNS-safe (underscores sanitized).
        assert!(!backup.metadata.name.contains('_'));
    }

    #[test]
    fn stopped_cluster_renders_hibernation_annotation() {
        let cluster = ManagedPostgresCluster {
            lifecycle_state: ClusterLifecycleState::Stopped,
            ..sample_cluster("dev")
        };
        let rendered = render_cluster(&cluster, None, &[], &RuntimeConfig::default()).unwrap();
        assert_eq!(
            rendered.metadata.annotations.get("cnpg.io/hibernation"),
            Some(&"on".to_owned())
        );

        // A running cluster carries no hibernation annotation.
        let running =
            render_cluster(&sample_cluster("dev"), None, &[], &RuntimeConfig::default()).unwrap();
        assert!(running.metadata.annotations.is_empty());
    }

    #[test]
    fn sanitize_dns_handles_underscores_and_case() {
        assert_eq!(sanitize_dns("Cluster_ABC_123"), "cluster-abc-123");
        assert_eq!(sanitize_dns("__weird__"), "weird");
    }
}
