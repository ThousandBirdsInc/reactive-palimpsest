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
    BackupPolicy, DatabaseRoleCredential, DatabaseRoleKind, ManagedPostgresCluster,
    ManagedPostgresSpec, MIN_SUPPORTED_POSTGRES_MAJOR,
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
        bootstrap: CnpgBootstrap {
            initdb: CnpgInitDb {
                database: "app".to_owned(),
                owner: "app".to_owned(),
            },
        },
        managed: (!managed_roles.is_empty()).then_some(CnpgManaged {
            roles: managed_roles,
        }),
        backup,
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
            s3_credentials: config.backup_credentials_secret.as_ref().map(|secret| {
                CnpgS3Credentials {
                    access_key_id: CnpgSecretKeyRef {
                        name: secret.clone(),
                        key: "ACCESS_KEY_ID".to_owned(),
                    },
                    secret_access_key: CnpgSecretKeyRef {
                        name: secret.clone(),
                        key: "ACCESS_SECRET_KEY".to_owned(),
                    },
                }
            }),
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

    /// Serialize all manifests as a single multi-document YAML stream.
    pub fn to_yaml(&self) -> Result<String, RuntimeError> {
        let mut out = serde_yaml::to_string(&self.cluster)?;
        if let Some(scheduled_backup) = &self.scheduled_backup {
            out.push_str("---\n");
            out.push_str(&serde_yaml::to_string(scheduled_backup)?);
        }
        Ok(out)
    }
}

fn object_meta(cluster: &ManagedPostgresCluster, config: &RuntimeConfig) -> ObjectMeta {
    ObjectMeta {
        name: resource_name(cluster),
        namespace: config.namespace_for(cluster),
        labels: ownership_labels(cluster),
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
    }

    /// Delete the resources described by a manifest stream.
    pub fn delete(&self, manifests: &str) -> Result<(), RuntimeError> {
        self.run(
            &["delete", "--ignore-not-found", "-f", "-"],
            Some(manifests),
        )
    }

    fn run(&self, args: &[&str], stdin: Option<&str>) -> Result<(), RuntimeError> {
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
        Ok(())
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
    pub initdb: CnpgInitDb,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CnpgInitDb {
    pub database: String,
    pub owner: String,
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
        let yaml = manifests.to_yaml().unwrap();
        assert!(yaml.contains("kind: Cluster"));
        assert!(yaml.contains("kind: ScheduledBackup"));
        assert!(yaml.contains("---"));
    }

    #[test]
    fn sanitize_dns_handles_underscores_and_case() {
        assert_eq!(sanitize_dns("Cluster_ABC_123"), "cluster-abc-123");
        assert_eq!(sanitize_dns("__weird__"), "weird");
    }
}
