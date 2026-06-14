// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Host-local node-agent primitives for the Palimpsest `PaaS`.

use std::{
    fs,
    io::{BufReader, Read, Write},
    net::TcpStream,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palimpsest_paas_core::{
    AgentCommandStatus, CloneRedactionMethod, DatabaseRoleCredential, DatabaseRoleKind,
    ManagedPostgresCloneRedactionPolicy, NodeAgentAction, NodeAgentBackupArtifact,
    NodeAgentCommand, NodeAgentCommandResult, NodeHost, NodeHostCapacity,
    NodeHostClusterObservation, NodeHostHardeningCheck, NodeHostHardeningStatus, NodeHostHeartbeat,
    NodeHostState, NodeHostSyncDeploymentObservation, QueuedNodeAgentCommand,
    MIN_SUPPORTED_POSTGRES_MAJOR,
};
use ring::{digest, hmac};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {
    pub host_id: String,
    pub postgres_bin_dir: PathBuf,
    pub runtime_root: PathBuf,
}

impl AgentConfig {
    #[must_use]
    pub fn local_dev() -> Self {
        Self {
            host_id: "local-dev-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/usr/local/pgsql-18/bin"),
            runtime_root: PathBuf::from("/var/lib/palimpsest"),
        }
    }

    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::local_dev();
        if let Ok(host_id) = std::env::var("PALIMPSEST_PAAS_AGENT_HOST_ID") {
            if !host_id.trim().is_empty() {
                config.host_id = host_id;
            }
        }
        if let Ok(postgres_bin_dir) = std::env::var("PALIMPSEST_PAAS_POSTGRES_BIN_DIR") {
            if !postgres_bin_dir.trim().is_empty() {
                config.postgres_bin_dir = PathBuf::from(postgres_bin_dir);
            }
        }
        if let Ok(runtime_root) = std::env::var("PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT") {
            if !runtime_root.trim().is_empty() {
                config.runtime_root = PathBuf::from(runtime_root);
            }
        }
        config
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPlan {
    pub command_id: String,
    pub cluster_id: String,
    pub steps: Vec<AgentStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentStep {
    CreateDirectory {
        path: PathBuf,
    },
    RenderPostgresConfig {
        path: PathBuf,
        port: u16,
    },
    RunInitdb {
        program: PathBuf,
        data_dir: PathBuf,
    },
    StartPostgres {
        program: PathBuf,
        data_dir: PathBuf,
    },
    StopPostgres {
        program: PathBuf,
        data_dir: PathBuf,
    },
    DeleteDirectory {
        path: PathBuf,
    },
    ConfigurePostgresAccess {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        database: String,
        roles: Vec<DatabaseRoleCredential>,
        publication: String,
        replication_slot: String,
    },
    CreateCopyOnWriteDatabaseClone {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        admin_database: String,
        source_database: String,
        target_database: String,
        terminate_source_connections: bool,
    },
    DropDatabase {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        admin_database: String,
        database: String,
        terminate_connections: bool,
    },
    CreatePhysicalReplicationSlot {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        database: String,
        slot_name: String,
    },
    CheckPostgresStandbyLag {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        database: String,
        slot_name: String,
        max_lag_bytes: u64,
    },
    RunBaseBackup {
        program: PathBuf,
        cluster_id: String,
        data_dir: PathBuf,
        postgres_url: String,
        backup_dir: PathBuf,
        backup_id: String,
    },
    WriteBackupManifest {
        cluster_id: String,
        data_dir: PathBuf,
        backup_dir: PathBuf,
        backup_id: String,
    },
    UploadBackupArtifact {
        cluster_id: String,
        backup_id: String,
        source_dir: PathBuf,
        object_store_dir: Option<PathBuf>,
        endpoint: Option<String>,
        auth: Option<BackupObjectStoreAuth>,
        tls: Option<BackupObjectStoreTls>,
        provider: String,
        bucket: String,
        object_key: String,
    },
    RestoreBaseBackup {
        program: PathBuf,
        backup_dir: PathBuf,
        data_dir: PathBuf,
        backup_id: String,
    },
    RenderRecoveryConfig {
        recovery_signal_path: PathBuf,
        auto_conf_path: PathBuf,
        restore_command: String,
        recovery_target_lsn: Option<String>,
    },
    RenderCloneRedactionManifest {
        manifest_path: PathBuf,
        policy: ManagedPostgresCloneRedactionPolicy,
    },
    RenderCloneRedactionSql {
        sql_path: PathBuf,
        policy: ManagedPostgresCloneRedactionPolicy,
    },
    RunCloneRedactionSql {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        database: String,
        policy: ManagedPostgresCloneRedactionPolicy,
    },
    RenderStandbyConfig {
        standby_signal_path: PathBuf,
        auto_conf_path: PathBuf,
        primary_conninfo: String,
        primary_slot_name: String,
        restore_command: String,
    },
    ArchiveWalSegment {
        source_path: PathBuf,
        destination_path: PathBuf,
        segment_name: String,
    },
    PromotePostgresStandby {
        data_dir: PathBuf,
    },
    FencePostgresPrimary {
        data_dir: PathBuf,
    },
    RecordStorageQuota {
        data_dir: PathBuf,
        storage_gib: u32,
    },
    RecordPostgresVersion {
        data_dir: PathBuf,
        target_postgres_version: palimpsest_paas_core::PostgresVersion,
    },
    RenderPostgresMajorUpgradePlan {
        data_dir: PathBuf,
        plan_path: PathBuf,
        source_postgres_version: palimpsest_paas_core::PostgresVersion,
        target_postgres_version: palimpsest_paas_core::PostgresVersion,
        strategy: palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy,
        preflight_required: bool,
    },
    RunPostgresMajorUpgradePreflightSql {
        program: PathBuf,
        data_dir: PathBuf,
        port: u16,
        database: String,
        source_postgres_version: palimpsest_paas_core::PostgresVersion,
        target_postgres_version: palimpsest_paas_core::PostgresVersion,
        strategy: palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy,
    },
    RecordPostgresMajorUpgrade {
        data_dir: PathBuf,
        source_postgres_version: palimpsest_paas_core::PostgresVersion,
        target_postgres_version: palimpsest_paas_core::PostgresVersion,
        strategy: palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy,
    },
    ProbeStatus {
        data_dir: PathBuf,
    },
    StartSyncDeployment {
        deployment_id: String,
        config_version: String,
        runtime_dir: PathBuf,
    },
    StopSyncDeployment {
        deployment_id: String,
        runtime_dir: PathBuf,
    },
    ProbeSyncDeployment {
        deployment_id: String,
        runtime_dir: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub command_id: String,
    pub cluster_id: String,
    pub outcomes: Vec<AgentStepOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStepOutcome {
    pub step: AgentStep,
    pub status: AgentStepStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStepStatus {
    Succeeded,
    Reported,
}

#[derive(Debug, Clone)]
pub struct NodeAgent {
    config: AgentConfig,
}

impl NodeAgent {
    #[must_use]
    pub const fn new(config: AgentConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub const fn config(&self) -> &AgentConfig {
        &self.config
    }

    #[must_use]
    pub fn local_host_description(&self) -> NodeHost {
        NodeHost {
            host_id: self.config.host_id.clone(),
            region: "local".to_owned(),
            failure_domain: "local-dev".to_owned(),
            data_root: self
                .config
                .runtime_root
                .join("postgres")
                .display()
                .to_string(),
            first_port: agent_first_port(),
            state: NodeHostState::Active,
            capacity: NodeHostCapacity {
                max_clusters: 16,
                assigned_clusters: 0,
                storage_gib: 1024,
                used_storage_gib: 0,
            },
        }
    }

    #[must_use]
    pub fn heartbeat(&self) -> NodeHostHeartbeat {
        let host = self.local_host_description();
        NodeHostHeartbeat {
            state: host.state,
            capacity: host.capacity,
            observed_clusters: self.observed_clusters(),
            observed_sync_deployments: self.observed_sync_deployments(),
        }
    }

    fn observed_clusters(&self) -> Vec<NodeHostClusterObservation> {
        let root = self.config.runtime_root.join("postgres");
        let Ok(entries) = fs::read_dir(root) else {
            return Vec::new();
        };
        entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }
                let cluster_id = entry.file_name().to_string_lossy().into_owned();
                let data_dir = entry.path();
                Some(NodeHostClusterObservation {
                    cluster_id,
                    data_dir: data_dir.display().to_string(),
                    postgres_running: data_dir.join("postmaster.pid").exists(),
                })
            })
            .collect()
    }

    fn observed_sync_deployments(&self) -> Vec<NodeHostSyncDeploymentObservation> {
        let root = self.config.runtime_root.join("sync");
        let Ok(entries) = fs::read_dir(root) else {
            return Vec::new();
        };
        entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let file_type = entry.file_type().ok()?;
                if !file_type.is_dir() {
                    return None;
                }
                let deployment_id = entry.file_name().to_string_lossy().into_owned();
                Some(NodeHostSyncDeploymentObservation {
                    deployment_id,
                    running: entry.path().join("running.json").exists(),
                })
            })
            .collect()
    }

    pub fn hardening_check(&self) -> NodeHostHardeningCheck {
        let disk_encryption =
            env_bool("PALIMPSEST_PAAS_DISK_ENCRYPTION").unwrap_or_else(detect_disk_encryption);
        let firewall_enabled =
            env_bool("PALIMPSEST_PAAS_FIREWALL_ENABLED").unwrap_or_else(detect_firewall_enabled);
        let unattended_upgrades = env_bool("PALIMPSEST_PAAS_UNATTENDED_UPGRADES")
            .unwrap_or_else(detect_unattended_upgrades);
        let warnings = [
            (!disk_encryption).then_some("disk encryption not detected"),
            (!firewall_enabled).then_some("firewall not detected"),
            (!unattended_upgrades).then_some("unattended upgrades not detected"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let checked_at_seconds = unix_timestamp_seconds();
        let checked_at = rfc3339_from_unix_seconds(checked_at_seconds);

        NodeHostHardeningCheck {
            check_id: format!(
                "hardening-{}-{}",
                sanitize_component(&self.config.host_id),
                checked_at_seconds
            ),
            host_id: self.config.host_id.clone(),
            status: if warnings.is_empty() {
                NodeHostHardeningStatus::Passing
            } else {
                NodeHostHardeningStatus::Warning
            },
            image_ref: env_string("PALIMPSEST_PAAS_NODE_IMAGE_REF")
                .unwrap_or_else(|| "local-dev-unpinned".to_owned()),
            os_release: env_string("PALIMPSEST_PAAS_OS_RELEASE").unwrap_or_else(detect_os_release),
            kernel_version: env_string("PALIMPSEST_PAAS_KERNEL_VERSION")
                .unwrap_or_else(detect_kernel_version),
            postgres_major_min: MIN_SUPPORTED_POSTGRES_MAJOR,
            container_runtime: env_string("PALIMPSEST_PAAS_CONTAINER_RUNTIME")
                .unwrap_or_else(detect_container_runtime),
            disk_encryption,
            firewall_enabled,
            unattended_upgrades,
            last_patched_at: env_string("PALIMPSEST_PAAS_LAST_PATCHED_AT"),
            checked_at,
            error_message: if warnings.is_empty() {
                None
            } else {
                Some(warnings.join("; "))
            },
        }
    }

    pub fn plan(&self, command: &NodeAgentCommand) -> Result<CommandPlan, NodeAgentError> {
        let steps = match &command.action {
            NodeAgentAction::PreparePostgres {
                postgres_version,
                data_dir,
                port,
            } => {
                if postgres_version.major() < palimpsest_paas_core::MIN_SUPPORTED_POSTGRES_MAJOR {
                    return Err(NodeAgentError::UnsupportedPostgresVersion(
                        postgres_version.to_string(),
                    ));
                }
                let data_dir = PathBuf::from(data_dir);
                vec![
                    AgentStep::CreateDirectory {
                        path: data_dir.clone(),
                    },
                    AgentStep::RunInitdb {
                        program: self.program("initdb"),
                        data_dir: data_dir.clone(),
                    },
                    AgentStep::RenderPostgresConfig {
                        path: data_dir.join("postgresql.conf"),
                        port: *port,
                    },
                ]
            }
            NodeAgentAction::StartPostgres => {
                let data_dir = self.cluster_data_dir(&command.cluster_id);
                vec![AgentStep::StartPostgres {
                    program: self.program("pg_ctl"),
                    data_dir,
                }]
            }
            NodeAgentAction::StopPostgres => {
                let data_dir = self.cluster_data_dir(&command.cluster_id);
                vec![AgentStep::StopPostgres {
                    program: self.program("pg_ctl"),
                    data_dir,
                }]
            }
            NodeAgentAction::DeletePostgresData { data_dir, .. } => {
                vec![AgentStep::DeleteDirectory {
                    path: PathBuf::from(data_dir),
                }]
            }
            NodeAgentAction::ConfigurePostgresAccess {
                data_dir,
                port,
                database,
                roles,
                publication,
                replication_slot,
            } => vec![AgentStep::ConfigurePostgresAccess {
                program: self.program("psql"),
                data_dir: PathBuf::from(data_dir),
                port: *port,
                database: database.clone(),
                roles: roles.clone(),
                publication: publication.clone(),
                replication_slot: replication_slot.clone(),
            }],
            NodeAgentAction::CreateCopyOnWriteDatabaseClone {
                data_dir,
                port,
                source_database,
                target_database,
                terminate_source_connections,
            } => {
                let admin_database = if source_database == "postgres" {
                    "template1"
                } else {
                    "postgres"
                };
                vec![AgentStep::CreateCopyOnWriteDatabaseClone {
                    program: self.program("psql"),
                    data_dir: PathBuf::from(data_dir),
                    port: *port,
                    admin_database: admin_database.to_owned(),
                    source_database: source_database.clone(),
                    target_database: target_database.clone(),
                    terminate_source_connections: *terminate_source_connections,
                }]
            }
            NodeAgentAction::DropDatabase {
                data_dir,
                port,
                database,
                terminate_connections,
            } => {
                ensure_droppable_database(database)?;
                vec![AgentStep::DropDatabase {
                    program: self.program("psql"),
                    data_dir: PathBuf::from(data_dir),
                    port: *port,
                    admin_database: "postgres".to_owned(),
                    database: database.clone(),
                    terminate_connections: *terminate_connections,
                }]
            }
            NodeAgentAction::ReportStatus => vec![AgentStep::ProbeStatus {
                data_dir: self.cluster_data_dir(&command.cluster_id),
            }],
            NodeAgentAction::StartSyncDeployment {
                deployment_id,
                config_version,
            } => vec![AgentStep::StartSyncDeployment {
                deployment_id: deployment_id.clone(),
                config_version: config_version.clone(),
                runtime_dir: self.sync_deployment_runtime_dir(deployment_id),
            }],
            NodeAgentAction::StopSyncDeployment { deployment_id } => {
                vec![AgentStep::StopSyncDeployment {
                    deployment_id: deployment_id.clone(),
                    runtime_dir: self.sync_deployment_runtime_dir(deployment_id),
                }]
            }
            NodeAgentAction::ReportSyncDeployment { deployment_id } => {
                vec![AgentStep::ProbeSyncDeployment {
                    deployment_id: deployment_id.clone(),
                    runtime_dir: self.sync_deployment_runtime_dir(deployment_id),
                }]
            }
            NodeAgentAction::CheckPostgresStandbyLag {
                source_data_dir,
                source_port,
                database,
                slot_name,
                max_lag_bytes,
            } => vec![AgentStep::CheckPostgresStandbyLag {
                program: self.program("psql"),
                data_dir: PathBuf::from(source_data_dir),
                port: *source_port,
                database: database.clone(),
                slot_name: slot_name.clone(),
                max_lag_bytes: *max_lag_bytes,
            }],
            NodeAgentAction::RunBaseBackup {
                backup_id,
                data_dir,
                postgres_url,
                backup_dir,
            } => {
                let data_dir = PathBuf::from(data_dir);
                let backup_dir = PathBuf::from(backup_dir).join(sanitize_component(backup_id));
                let mut steps = vec![
                    AgentStep::CreateDirectory {
                        path: backup_dir.clone(),
                    },
                    AgentStep::RunBaseBackup {
                        program: self.program("pg_basebackup"),
                        cluster_id: command.cluster_id.clone(),
                        data_dir: data_dir.clone(),
                        postgres_url: postgres_url.clone(),
                        backup_dir: backup_dir.clone(),
                        backup_id: backup_id.clone(),
                    },
                    AgentStep::WriteBackupManifest {
                        cluster_id: command.cluster_id.clone(),
                        data_dir,
                        backup_dir: backup_dir.clone(),
                        backup_id: backup_id.clone(),
                    },
                ];
                if let Some(target) = backup_object_store_target_from_env()? {
                    steps.push(AgentStep::UploadBackupArtifact {
                        cluster_id: command.cluster_id.clone(),
                        backup_id: backup_id.clone(),
                        source_dir: backup_dir,
                        object_store_dir: target.root_dir,
                        endpoint: target.endpoint,
                        auth: target.auth,
                        tls: target.tls,
                        provider: target.provider,
                        bucket: target.bucket,
                        object_key: backup_object_key(&command.cluster_id, backup_id),
                    });
                }
                steps
            }
            NodeAgentAction::DeleteBackupData { backup_dir, .. } => {
                vec![AgentStep::DeleteDirectory {
                    path: PathBuf::from(backup_dir),
                }]
            }
            NodeAgentAction::PrepareRestore {
                backup_id,
                backup_dir,
                data_dir,
                target_port,
                database,
                restore_command,
                recovery_target_lsn,
                redaction_policy,
            } => {
                let backup_dir = PathBuf::from(backup_dir).join(sanitize_component(backup_id));
                let data_dir = PathBuf::from(data_dir);
                let target_port = match (redaction_policy, target_port) {
                    (Some(_), Some(target_port)) => Some(*target_port),
                    (Some(_), None) => {
                        return Err(NodeAgentError::InvalidRedactionPolicy(
                            "redacted restore requires target_port".to_owned(),
                        ));
                    }
                    (None, target_port) => *target_port,
                };
                let database = database.clone().unwrap_or_else(|| "postgres".to_owned());
                let mut steps = vec![
                    AgentStep::CreateDirectory {
                        path: data_dir.clone(),
                    },
                    AgentStep::RestoreBaseBackup {
                        program: PathBuf::from("rsync"),
                        backup_dir,
                        data_dir: data_dir.clone(),
                        backup_id: backup_id.clone(),
                    },
                    AgentStep::RenderRecoveryConfig {
                        recovery_signal_path: data_dir.join("recovery.signal"),
                        auto_conf_path: data_dir.join("postgresql.auto.conf"),
                        restore_command: restore_command.clone(),
                        recovery_target_lsn: recovery_target_lsn.clone(),
                    },
                ];
                if let Some(policy) = redaction_policy {
                    let target_port = target_port.expect("checked above");
                    steps.push(AgentStep::RenderCloneRedactionManifest {
                        manifest_path: data_dir.join("palimpsest-clone-redaction-policy.json"),
                        policy: policy.clone(),
                    });
                    steps.push(AgentStep::RenderCloneRedactionSql {
                        sql_path: data_dir.join("palimpsest-clone-redaction.sql"),
                        policy: policy.clone(),
                    });
                    steps.push(AgentStep::RenderPostgresConfig {
                        path: data_dir.join("postgresql.conf"),
                        port: target_port,
                    });
                    steps.push(AgentStep::StartPostgres {
                        program: self.program("pg_ctl"),
                        data_dir: data_dir.clone(),
                    });
                    steps.push(AgentStep::RunCloneRedactionSql {
                        program: self.program("psql"),
                        data_dir: data_dir.clone(),
                        port: target_port,
                        database,
                        policy: policy.clone(),
                    });
                    steps.push(AgentStep::StopPostgres {
                        program: self.program("pg_ctl"),
                        data_dir,
                    });
                }
                steps
            }
            NodeAgentAction::PreparePostgresStandby {
                backup_id,
                backup_dir,
                data_dir,
                target_port,
                primary_conninfo,
                primary_slot_name,
                source_data_dir,
                source_port,
                database,
                restore_command,
            } => {
                let backup_dir = PathBuf::from(backup_dir).join(sanitize_component(backup_id));
                let data_dir = PathBuf::from(data_dir);
                let mut steps = vec![
                    AgentStep::CreatePhysicalReplicationSlot {
                        program: PathBuf::from("docker"),
                        data_dir: PathBuf::from(source_data_dir),
                        port: *source_port,
                        database: database.clone(),
                        slot_name: primary_slot_name.clone(),
                    },
                    AgentStep::CreateDirectory {
                        path: data_dir.clone(),
                    },
                    AgentStep::RestoreBaseBackup {
                        program: PathBuf::from("rsync"),
                        backup_dir,
                        data_dir: data_dir.clone(),
                        backup_id: backup_id.clone(),
                    },
                    AgentStep::RenderStandbyConfig {
                        standby_signal_path: data_dir.join("standby.signal"),
                        auto_conf_path: data_dir.join("postgresql.auto.conf"),
                        primary_conninfo: primary_conninfo.clone(),
                        primary_slot_name: primary_slot_name.clone(),
                        restore_command: restore_command.clone(),
                    },
                ];
                if let Some(target_port) = target_port {
                    steps.push(AgentStep::RenderPostgresConfig {
                        path: data_dir.join("postgresql.conf"),
                        port: *target_port,
                    });
                    steps.push(AgentStep::StartPostgres {
                        program: self.program("pg_ctl"),
                        data_dir: data_dir.clone(),
                    });
                    steps.push(AgentStep::ProbeStatus { data_dir });
                }
                steps
            }
            NodeAgentAction::ArchiveWalSegment {
                source_path,
                archive_dir,
                segment_name,
            } => vec![
                AgentStep::CreateDirectory {
                    path: PathBuf::from(archive_dir),
                },
                AgentStep::ArchiveWalSegment {
                    source_path: PathBuf::from(source_path),
                    destination_path: PathBuf::from(archive_dir)
                        .join(sanitize_component(segment_name)),
                    segment_name: segment_name.clone(),
                },
            ],
            NodeAgentAction::PromotePostgresStandby { data_dir } => {
                vec![AgentStep::PromotePostgresStandby {
                    data_dir: PathBuf::from(data_dir),
                }]
            }
            NodeAgentAction::FencePostgresPrimary { data_dir } => {
                vec![AgentStep::FencePostgresPrimary {
                    data_dir: PathBuf::from(data_dir),
                }]
            }
            NodeAgentAction::ResizePostgresStorage {
                data_dir,
                storage_gib,
            } => vec![AgentStep::RecordStorageQuota {
                data_dir: PathBuf::from(data_dir),
                storage_gib: *storage_gib,
            }],
            NodeAgentAction::UpdatePostgresMinor {
                data_dir,
                target_postgres_version,
            } => vec![AgentStep::RecordPostgresVersion {
                data_dir: PathBuf::from(data_dir),
                target_postgres_version: target_postgres_version.clone(),
            }],
            NodeAgentAction::UpgradePostgresMajor {
                data_dir,
                source_port,
                database,
                source_postgres_version,
                target_postgres_version,
                strategy,
            } => {
                if target_postgres_version.major() <= source_postgres_version.major() {
                    return Err(NodeAgentError::InvalidMajorUpgrade(format!(
                        "target PostgreSQL major {} must be greater than source major {}",
                        target_postgres_version.major(),
                        source_postgres_version.major()
                    )));
                }

                let data_dir = PathBuf::from(data_dir);
                let mut steps = vec![AgentStep::RenderPostgresMajorUpgradePlan {
                    plan_path: data_dir.join("palimpsest-postgres-major-upgrade-plan.json"),
                    data_dir: data_dir.clone(),
                    source_postgres_version: source_postgres_version.clone(),
                    target_postgres_version: target_postgres_version.clone(),
                    strategy: *strategy,
                    preflight_required: source_port.is_some(),
                }];
                if let Some(port) = source_port {
                    steps.push(AgentStep::RunPostgresMajorUpgradePreflightSql {
                        program: self.config.postgres_bin_dir.join("psql"),
                        data_dir: data_dir.clone(),
                        port: *port,
                        database: database.clone().unwrap_or_else(|| "postgres".to_owned()),
                        source_postgres_version: source_postgres_version.clone(),
                        target_postgres_version: target_postgres_version.clone(),
                        strategy: *strategy,
                    });
                }
                steps.push(AgentStep::RecordPostgresMajorUpgrade {
                    data_dir,
                    source_postgres_version: source_postgres_version.clone(),
                    target_postgres_version: target_postgres_version.clone(),
                    strategy: *strategy,
                });
                steps
            }
        };

        Ok(CommandPlan {
            command_id: command.command_id.clone(),
            cluster_id: command.cluster_id.clone(),
            steps,
        })
    }

    pub fn execute_with_runner<R>(
        &self,
        command: &NodeAgentCommand,
        runner: &R,
    ) -> Result<ExecutionReport, NodeAgentError>
    where
        R: ProcessRunner,
    {
        let plan = self.plan(command)?;
        self.execute_plan_with_runner(&plan, runner)
    }

    pub fn execute_plan_with_runner<R>(
        &self,
        plan: &CommandPlan,
        runner: &R,
    ) -> Result<ExecutionReport, NodeAgentError>
    where
        R: ProcessRunner,
    {
        let mut outcomes = Vec::with_capacity(plan.steps.len());
        for step in &plan.steps {
            let outcome = self.execute_step(step, runner)?;
            outcomes.push(outcome);
        }

        Ok(ExecutionReport {
            command_id: plan.command_id.clone(),
            cluster_id: plan.cluster_id.clone(),
            outcomes,
        })
    }

    fn execute_step<R>(
        &self,
        step: &AgentStep,
        runner: &R,
    ) -> Result<AgentStepOutcome, NodeAgentError>
    where
        R: ProcessRunner,
    {
        match step {
            AgentStep::CreateDirectory { path } => {
                ensure_under_root(path, &self.config.runtime_root)?;
                fs::create_dir_all(path).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", path.display()),
                    source: err,
                })?;
                Ok(succeeded(step, format!("created {}", path.display())))
            }
            AgentStep::RenderPostgresConfig { path, port } => {
                ensure_under_root(path, &self.config.runtime_root)?;
                let parent = path.parent().ok_or_else(|| {
                    NodeAgentError::InvalidPath(format!("missing parent for {}", path.display()))
                })?;
                fs::create_dir_all(parent).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", parent.display()),
                    source: err,
                })?;
                let config = render_postgres_config(*port);
                fs::write(path, config).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", path.display()),
                    source: err,
                })?;
                let hba_path = parent.join("pg_hba.conf");
                fs::write(&hba_path, render_pg_hba_config()).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", hba_path.display()),
                    source: err,
                })?;
                Ok(succeeded(step, format!("rendered {}", path.display())))
            }
            AgentStep::RunInitdb { program, data_dir } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                runner.run(program, &["-D".to_owned(), data_dir.display().to_string()])?;
                Ok(succeeded(
                    step,
                    format!("initialized {}", data_dir.display()),
                ))
            }
            AgentStep::StartPostgres { program, data_dir } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let log_path = data_dir.join("postgresql.log");
                runner.run(
                    program,
                    &[
                        "-D".to_owned(),
                        data_dir.display().to_string(),
                        "-l".to_owned(),
                        log_path.display().to_string(),
                        "start".to_owned(),
                    ],
                )?;
                Ok(succeeded(step, format!("started {}", data_dir.display())))
            }
            AgentStep::StopPostgres { program, data_dir } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                runner.run(
                    program,
                    &[
                        "-D".to_owned(),
                        data_dir.display().to_string(),
                        "stop".to_owned(),
                        "-m".to_owned(),
                        "fast".to_owned(),
                    ],
                )?;
                Ok(succeeded(step, format!("stopped {}", data_dir.display())))
            }
            AgentStep::DeleteDirectory { path } => {
                ensure_deletable_under_root(path, &self.config.runtime_root)?;
                if path.exists() {
                    fs::remove_dir_all(path).map_err(|err| NodeAgentError::Io {
                        action: format!("delete directory {}", path.display()),
                        source: err,
                    })?;
                }
                Ok(succeeded(step, format!("deleted {}", path.display())))
            }
            AgentStep::ConfigurePostgresAccess {
                program,
                data_dir,
                port,
                database,
                roles,
                publication,
                replication_slot,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let sql =
                    render_postgres_access_sql(database, roles, publication, replication_slot)?;
                runner.run_sql(
                    program,
                    data_dir,
                    *port,
                    database,
                    &sql.roles_and_publication,
                )?;
                runner.run_sql(program, data_dir, *port, database, &sql.replication_slot)?;
                Ok(succeeded(
                    step,
                    format!("configured roles and replication for {database}"),
                ))
            }
            AgentStep::CreateCopyOnWriteDatabaseClone {
                program,
                data_dir,
                port,
                admin_database,
                source_database,
                target_database,
                terminate_source_connections,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                if *terminate_source_connections {
                    let terminate_sql = render_terminate_database_connections_sql(source_database)?;
                    runner.run_sql(program, data_dir, *port, admin_database, &terminate_sql)?;
                }
                let clone_sql =
                    render_copy_on_write_database_clone_sql(source_database, target_database)?;
                runner.run_sql(program, data_dir, *port, admin_database, &clone_sql)?;
                Ok(succeeded(
                    step,
                    format!(
                        "created copy-on-write database clone {target_database} from {source_database}"
                    ),
                ))
            }
            AgentStep::DropDatabase {
                program,
                data_dir,
                port,
                admin_database,
                database,
                terminate_connections,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                ensure_droppable_database(database)?;
                if *terminate_connections {
                    let terminate_sql = render_terminate_database_connections_sql(database)?;
                    runner.run_sql(program, data_dir, *port, admin_database, &terminate_sql)?;
                }
                let drop_sql = render_drop_database_sql(database, *terminate_connections)?;
                runner.run_sql(program, data_dir, *port, admin_database, &drop_sql)?;
                Ok(succeeded(step, format!("dropped database {database}")))
            }
            AgentStep::CreatePhysicalReplicationSlot {
                program,
                data_dir,
                port,
                database,
                slot_name,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let sql = render_physical_replication_slot_sql(slot_name)?;
                runner.run_sql(program, data_dir, *port, database, &sql)?;
                Ok(succeeded(
                    step,
                    format!("created physical replication slot {slot_name}"),
                ))
            }
            AgentStep::CheckPostgresStandbyLag {
                program,
                data_dir,
                port,
                database,
                slot_name,
                max_lag_bytes,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let sql = render_standby_lag_check_sql(slot_name, *max_lag_bytes)?;
                runner.run_sql(program, data_dir, *port, database, &sql)?;
                Ok(succeeded(
                    step,
                    format!("checked standby slot {slot_name} lag <= {max_lag_bytes} bytes"),
                ))
            }
            AgentStep::RunBaseBackup {
                program,
                cluster_id: _,
                data_dir,
                postgres_url,
                backup_dir,
                backup_id,
            } => {
                ensure_under_root(backup_dir, &self.config.runtime_root)?;
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                runner.run_base_backup(program, data_dir, postgres_url, backup_dir, backup_id)?;
                Ok(succeeded(
                    step,
                    format!(
                        "base backup {backup_id} written to {}",
                        backup_dir.display()
                    ),
                ))
            }
            AgentStep::WriteBackupManifest {
                cluster_id,
                data_dir,
                backup_dir,
                backup_id,
            } => {
                ensure_under_root(backup_dir, &self.config.runtime_root)?;
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                write_backup_manifest(
                    backup_dir,
                    BackupManifestInput {
                        backup_id,
                        cluster_id,
                        host_id: &self.config.host_id,
                        data_dir,
                    },
                )?;
                Ok(succeeded(
                    step,
                    format!("wrote backup manifest for {backup_id}"),
                ))
            }
            AgentStep::UploadBackupArtifact {
                backup_id,
                source_dir,
                object_store_dir,
                endpoint,
                auth,
                tls,
                provider,
                bucket,
                object_key,
                ..
            } => {
                ensure_under_root(source_dir, &self.config.runtime_root)?;
                if let Some(endpoint) = endpoint {
                    upload_dir_to_http_object_store(
                        source_dir,
                        endpoint,
                        auth.as_ref(),
                        tls.as_ref(),
                        bucket,
                        object_key,
                    )?;
                } else {
                    let object_store_dir = object_store_dir.as_ref().ok_or_else(|| {
                        NodeAgentError::InvalidPath(
                            "backup object-store directory is required for filesystem uploads"
                                .to_owned(),
                        )
                    })?;
                    let destination =
                        object_store_destination(object_store_dir, bucket, object_key)?;
                    copy_dir_all(source_dir, &destination)?;
                }
                Ok(succeeded(
                    step,
                    format!(
                        "uploaded backup {backup_id} to {provider}:{}",
                        object_store_uri(bucket, object_key)
                    ),
                ))
            }
            AgentStep::RestoreBaseBackup {
                backup_dir,
                data_dir,
                backup_id,
                ..
            } => {
                ensure_under_root(backup_dir, &self.config.runtime_root)?;
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                copy_dir_contents(backup_dir, data_dir)?;
                fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700)).map_err(
                    |err| NodeAgentError::Io {
                        action: format!("chmod 0700 {}", data_dir.display()),
                        source: err,
                    },
                )?;
                Ok(succeeded(
                    step,
                    format!(
                        "restored base backup {backup_id} into {}",
                        data_dir.display()
                    ),
                ))
            }
            AgentStep::RenderRecoveryConfig {
                recovery_signal_path,
                auto_conf_path,
                restore_command,
                recovery_target_lsn,
            } => {
                ensure_under_root(recovery_signal_path, &self.config.runtime_root)?;
                ensure_under_root(auto_conf_path, &self.config.runtime_root)?;
                fs::write(recovery_signal_path, "").map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", recovery_signal_path.display()),
                    source: err,
                })?;
                fs::write(
                    auto_conf_path,
                    render_recovery_config(restore_command, recovery_target_lsn.as_deref()),
                )
                .map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", auto_conf_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!("rendered recovery config {}", auto_conf_path.display()),
                ))
            }
            AgentStep::RenderCloneRedactionManifest {
                manifest_path,
                policy,
            } => {
                ensure_under_root(manifest_path, &self.config.runtime_root)?;
                let payload = serde_json::to_vec_pretty(policy)?;
                fs::write(manifest_path, payload).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", manifest_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "rendered clone redaction manifest {}",
                        manifest_path.display()
                    ),
                ))
            }
            AgentStep::RenderCloneRedactionSql { sql_path, policy } => {
                ensure_under_root(sql_path, &self.config.runtime_root)?;
                let sql = render_clone_redaction_sql(policy)?;
                fs::write(sql_path, sql).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", sql_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!("rendered clone redaction SQL {}", sql_path.display()),
                ))
            }
            AgentStep::RunCloneRedactionSql {
                program,
                data_dir,
                port,
                database,
                policy,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let sql = render_clone_redaction_sql(policy)?;
                runner.run_sql(program, data_dir, *port, database, &sql)?;
                let applied_path = data_dir.join("palimpsest-clone-redaction-applied.json");
                let applied = CloneRedactionApplied {
                    policy_id: policy.policy_id.clone(),
                    rule_count: policy.rules.len(),
                    applied_at: unix_timestamp_seconds(),
                };
                fs::write(&applied_path, serde_json::to_vec_pretty(&applied)?).map_err(|err| {
                    NodeAgentError::Io {
                        action: format!("write {}", applied_path.display()),
                        source: err,
                    }
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "applied clone redaction policy {} to database {database}",
                        policy.policy_id
                    ),
                ))
            }
            AgentStep::RenderStandbyConfig {
                standby_signal_path,
                auto_conf_path,
                primary_conninfo,
                primary_slot_name,
                restore_command,
            } => {
                ensure_under_root(standby_signal_path, &self.config.runtime_root)?;
                ensure_under_root(auto_conf_path, &self.config.runtime_root)?;
                fs::write(standby_signal_path, "").map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", standby_signal_path.display()),
                    source: err,
                })?;
                fs::write(
                    auto_conf_path,
                    render_standby_config(primary_conninfo, primary_slot_name, restore_command),
                )
                .map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", auto_conf_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!("rendered standby config {}", auto_conf_path.display()),
                ))
            }
            AgentStep::ArchiveWalSegment {
                source_path,
                destination_path,
                segment_name,
            } => {
                ensure_under_root(source_path, &self.config.runtime_root)?;
                ensure_under_root(destination_path, &self.config.runtime_root)?;
                let parent = destination_path.parent().ok_or_else(|| {
                    NodeAgentError::InvalidPath(format!(
                        "missing parent for {}",
                        destination_path.display()
                    ))
                })?;
                fs::create_dir_all(parent).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", parent.display()),
                    source: err,
                })?;
                fs::copy(source_path, destination_path).map_err(|err| NodeAgentError::Io {
                    action: format!(
                        "archive WAL segment {} to {}",
                        source_path.display(),
                        destination_path.display()
                    ),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "archived WAL segment {segment_name} to {}",
                        destination_path.display()
                    ),
                ))
            }
            AgentStep::PromotePostgresStandby { data_dir } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let recovery_signal = data_dir.join("recovery.signal");
                if recovery_signal.exists() {
                    fs::remove_file(&recovery_signal).map_err(|err| NodeAgentError::Io {
                        action: format!("delete {}", recovery_signal.display()),
                        source: err,
                    })?;
                }
                let marker_path = data_dir.join("promotion.intent");
                fs::write(&marker_path, b"promoted\n").map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", marker_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!("promoted standby data directory {}", data_dir.display()),
                ))
            }
            AgentStep::FencePostgresPrimary { data_dir } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                fs::create_dir_all(data_dir).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", data_dir.display()),
                    source: err,
                })?;
                runner.fence_postgres_primary(&self.program("pg_ctl"), data_dir)?;
                let marker_path = data_dir.join("fence.intent");
                let payload = serde_json::json!({
                    "fenced": true,
                    "method": "local_stop_then_marker",
                    "fenced_at_unix_seconds": unix_timestamp_seconds(),
                });
                fs::write(&marker_path, payload.to_string()).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", marker_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!("fenced primary data directory {}", data_dir.display()),
                ))
            }
            AgentStep::RecordStorageQuota {
                data_dir,
                storage_gib,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                fs::create_dir_all(data_dir).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", data_dir.display()),
                    source: err,
                })?;
                let quota_path = data_dir.join("palimpsest-storage-quota.json");
                let payload = serde_json::json!({ "storage_gib": storage_gib });
                fs::write(&quota_path, payload.to_string()).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", quota_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "recorded storage quota {storage_gib} GiB in {}",
                        quota_path.display()
                    ),
                ))
            }
            AgentStep::RecordPostgresVersion {
                data_dir,
                target_postgres_version,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                fs::create_dir_all(data_dir).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", data_dir.display()),
                    source: err,
                })?;
                let version_path = data_dir.join("palimpsest-postgres-version.json");
                let payload = serde_json::json!({
                    "target_postgres_version": target_postgres_version,
                });
                fs::write(&version_path, payload.to_string()).map_err(|err| {
                    NodeAgentError::Io {
                        action: format!("write {}", version_path.display()),
                        source: err,
                    }
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "recorded PostgreSQL target version {} in {}",
                        target_postgres_version,
                        version_path.display()
                    ),
                ))
            }
            AgentStep::RenderPostgresMajorUpgradePlan {
                data_dir,
                plan_path,
                source_postgres_version,
                target_postgres_version,
                strategy,
                preflight_required,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                ensure_under_root(plan_path, &self.config.runtime_root)?;
                fs::create_dir_all(data_dir).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", data_dir.display()),
                    source: err,
                })?;
                let payload = serde_json::json!({
                    "source_postgres_version": source_postgres_version,
                    "target_postgres_version": target_postgres_version,
                    "strategy": strategy,
                    "preflight_required": preflight_required,
                    "phase": "preflight",
                });
                fs::write(plan_path, payload.to_string()).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", plan_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "rendered PostgreSQL major upgrade plan {}",
                        plan_path.display()
                    ),
                ))
            }
            AgentStep::RunPostgresMajorUpgradePreflightSql {
                program,
                data_dir,
                port,
                database,
                source_postgres_version,
                target_postgres_version,
                strategy,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let sql = major_upgrade_preflight_sql(
                    source_postgres_version,
                    target_postgres_version,
                    *strategy,
                );
                runner.run_sql(program, data_dir, *port, database, &sql)?;
                let preflight_path =
                    data_dir.join("palimpsest-postgres-major-upgrade-preflight.json");
                let payload = serde_json::json!({
                    "source_postgres_version": source_postgres_version,
                    "target_postgres_version": target_postgres_version,
                    "strategy": strategy,
                    "database": database,
                    "port": port,
                    "status": "succeeded",
                    "checked_at_unix_seconds": unix_timestamp_seconds(),
                });
                fs::write(&preflight_path, payload.to_string()).map_err(|err| {
                    NodeAgentError::Io {
                        action: format!("write {}", preflight_path.display()),
                        source: err,
                    }
                })?;
                Ok(succeeded(
                    step,
                    format!("ran PostgreSQL major upgrade preflight on {database}:{port}"),
                ))
            }
            AgentStep::RecordPostgresMajorUpgrade {
                data_dir,
                source_postgres_version,
                target_postgres_version,
                strategy,
            } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                fs::create_dir_all(data_dir).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", data_dir.display()),
                    source: err,
                })?;
                let upgrade_path = data_dir.join("palimpsest-postgres-major-upgrade.json");
                let payload = serde_json::json!({
                    "source_postgres_version": source_postgres_version,
                    "target_postgres_version": target_postgres_version,
                    "strategy": strategy,
                });
                fs::write(&upgrade_path, payload.to_string()).map_err(|err| {
                    NodeAgentError::Io {
                        action: format!("write {}", upgrade_path.display()),
                        source: err,
                    }
                })?;
                Ok(succeeded(
                    step,
                    format!(
                        "recorded PostgreSQL major upgrade {} to {} in {}",
                        source_postgres_version,
                        target_postgres_version,
                        upgrade_path.display()
                    ),
                ))
            }
            AgentStep::ProbeStatus { data_dir } => {
                ensure_under_root(data_dir, &self.config.runtime_root)?;
                let running = data_dir.join("postmaster.pid").exists();
                Ok(AgentStepOutcome {
                    step: step.clone(),
                    status: AgentStepStatus::Reported,
                    detail: if running {
                        "postmaster.pid present".to_owned()
                    } else {
                        "postmaster.pid absent".to_owned()
                    },
                })
            }
            AgentStep::StartSyncDeployment {
                deployment_id,
                config_version,
                runtime_dir,
            } => {
                ensure_under_root(runtime_dir, &self.config.runtime_root)?;
                fs::create_dir_all(runtime_dir).map_err(|err| NodeAgentError::Io {
                    action: format!("create directory {}", runtime_dir.display()),
                    source: err,
                })?;
                let marker_path = runtime_dir.join("running.json");
                let payload = serde_json::json!({
                    "deployment_id": deployment_id,
                    "config_version": config_version,
                    "started_at_unix_seconds": unix_timestamp_seconds(),
                });
                fs::write(&marker_path, payload.to_string()).map_err(|err| NodeAgentError::Io {
                    action: format!("write {}", marker_path.display()),
                    source: err,
                })?;
                Ok(succeeded(
                    step,
                    format!("started sync deployment {deployment_id}"),
                ))
            }
            AgentStep::StopSyncDeployment {
                deployment_id,
                runtime_dir,
            } => {
                ensure_under_root(runtime_dir, &self.config.runtime_root)?;
                let marker_path = runtime_dir.join("running.json");
                match fs::remove_file(&marker_path) {
                    Ok(()) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => {
                        return Err(NodeAgentError::Io {
                            action: format!("remove {}", marker_path.display()),
                            source: err,
                        });
                    }
                }
                Ok(succeeded(
                    step,
                    format!("stopped sync deployment {deployment_id}"),
                ))
            }
            AgentStep::ProbeSyncDeployment {
                deployment_id,
                runtime_dir,
            } => {
                ensure_under_root(runtime_dir, &self.config.runtime_root)?;
                let running = runtime_dir.join("running.json").exists();
                Ok(AgentStepOutcome {
                    step: step.clone(),
                    status: AgentStepStatus::Reported,
                    detail: if running {
                        format!("sync deployment {deployment_id} marker present")
                    } else {
                        format!("sync deployment {deployment_id} marker absent")
                    },
                })
            }
        }
    }

    fn program(&self, name: &str) -> PathBuf {
        self.config.postgres_bin_dir.join(name)
    }

    fn cluster_data_dir(&self, cluster_id: &str) -> PathBuf {
        self.config
            .runtime_root
            .join("postgres")
            .join(sanitize_component(cluster_id))
    }

    fn sync_deployment_runtime_dir(&self, deployment_id: &str) -> PathBuf {
        self.config
            .runtime_root
            .join("sync")
            .join(sanitize_component(deployment_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneClientConfig {
    pub base_url: String,
    pub host_id: String,
    pub bearer_token: Option<String>,
    pub signing_key_id: Option<String>,
    pub signing_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct HttpControlPlaneClient {
    endpoint: HttpEndpoint,
    host_id: String,
    bearer_token: Option<String>,
    signing_key_id: Option<String>,
    signing_key: Option<Vec<u8>>,
}

impl HttpControlPlaneClient {
    pub fn new(config: ControlPlaneClientConfig) -> Result<Self, NodeAgentError> {
        let endpoint = HttpEndpoint::parse(&config.base_url)?;
        if endpoint.scheme != HttpScheme::Http {
            return Err(NodeAgentError::Http(
                "only http:// control-plane URLs are supported".to_owned(),
            ));
        }
        Ok(Self {
            endpoint,
            host_id: config.host_id,
            bearer_token: config.bearer_token,
            signing_key_id: config.signing_key_id,
            signing_key: config.signing_key,
        })
    }

    pub fn lease_next_command(&self) -> Result<Option<QueuedNodeAgentCommand>, NodeAgentError> {
        let path = format!(
            "/v1/node-hosts/{}/commands/lease",
            escape_path_segment(&self.host_id)
        );
        let response = self.post_json(
            &path,
            "null",
            AgentAuthScope {
                host_id: &self.host_id,
                operation: "lease",
            },
        )?;
        if response.body.trim().is_empty() || response.body.trim() == "null" {
            return Ok(None);
        }
        serde_json::from_str(&response.body).map_err(NodeAgentError::Json)
    }

    pub fn register_host(&self, host: &NodeHost) -> Result<(), NodeAgentError> {
        let body = serde_json::to_string(host).map_err(NodeAgentError::Json)?;
        self.post_json(
            "/v1/node-hosts",
            &body,
            AgentAuthScope {
                host_id: &host.host_id,
                operation: "register",
            },
        )?;
        Ok(())
    }

    pub fn complete_command(&self, result: &NodeAgentCommandResult) -> Result<(), NodeAgentError> {
        let path = format!(
            "/v1/node-hosts/{}/commands/{}/complete",
            escape_path_segment(&result.host_id),
            escape_path_segment(&result.command_id)
        );
        let body = serde_json::to_string(result).map_err(NodeAgentError::Json)?;
        let operation = format!("complete:{}", result.command_id);
        self.post_json(
            &path,
            &body,
            AgentAuthScope {
                host_id: &result.host_id,
                operation: &operation,
            },
        )?;
        Ok(())
    }

    pub fn record_heartbeat(&self, heartbeat: &NodeHostHeartbeat) -> Result<(), NodeAgentError> {
        let path = format!(
            "/v1/node-hosts/{}/heartbeat",
            escape_path_segment(&self.host_id)
        );
        let body = serde_json::to_string(heartbeat).map_err(NodeAgentError::Json)?;
        self.post_json(
            &path,
            &body,
            AgentAuthScope {
                host_id: &self.host_id,
                operation: "heartbeat",
            },
        )?;
        Ok(())
    }

    pub fn record_hardening_check(
        &self,
        check: &NodeHostHardeningCheck,
    ) -> Result<NodeHostHardeningCheck, NodeAgentError> {
        let path = format!(
            "/v1/node-hosts/{}/hardening-checks",
            escape_path_segment(&check.host_id)
        );
        let body = serde_json::to_string(check).map_err(NodeAgentError::Json)?;
        let response = self.post_json(
            &path,
            &body,
            AgentAuthScope {
                host_id: &check.host_id,
                operation: "hardening-check",
            },
        )?;
        serde_json::from_str(&response.body).map_err(NodeAgentError::Json)
    }

    fn post_json(
        &self,
        path: &str,
        body: &str,
        auth_scope: AgentAuthScope<'_>,
    ) -> Result<HttpResponse, NodeAgentError> {
        let mut stream = TcpStream::connect(&self.endpoint.socket_addr).map_err(|err| {
            NodeAgentError::Http(format!("connect {}: {err}", self.endpoint.socket_addr))
        })?;
        let request = self.render_post_json_request(path, body, auth_scope);
        stream
            .write_all(request.as_bytes())
            .map_err(|err| NodeAgentError::Http(format!("write request: {err}")))?;

        let mut raw = String::new();
        stream
            .read_to_string(&mut raw)
            .map_err(|err| NodeAgentError::Http(format!("read response: {err}")))?;
        parse_http_response(&raw)
    }

    fn render_post_json_request(
        &self,
        path: &str,
        body: &str,
        auth_scope: AgentAuthScope<'_>,
    ) -> String {
        let timestamp = unix_timestamp_seconds();
        self.render_post_json_request_at(path, body, auth_scope, timestamp)
    }

    fn render_post_json_request_at(
        &self,
        path: &str,
        body: &str,
        auth_scope: AgentAuthScope<'_>,
        timestamp: u64,
    ) -> String {
        let path = self.endpoint.path(path);
        let authorization = self
            .bearer_token
            .as_ref()
            .map_or_else(String::new, |token| {
                format!("Authorization: Bearer {token}\r\n")
            });
        let agent_signature = self.signing_key.as_ref().map_or_else(String::new, |key| {
            let signature =
                sign_agent_request(key, auth_scope.host_id, auth_scope.operation, timestamp);
            let key_id = self
                .signing_key_id
                .as_ref()
                .map_or_else(String::new, |key_id| {
                    format!("X-Palimpsest-Agent-Key-Id: {key_id}\r\n")
                });
            format!(
                "{key_id}\
                 X-Palimpsest-Agent-Host-Id: {}\r\n\
                 X-Palimpsest-Agent-Operation: {}\r\n\
                 X-Palimpsest-Agent-Timestamp: {timestamp}\r\n\
                 X-Palimpsest-Agent-Signature: {signature}\r\n",
                auth_scope.host_id, auth_scope.operation
            )
        });
        format!(
            "POST {path} HTTP/1.1\r\n\
             Host: {}\r\n\
             {authorization}\
             {agent_signature}\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            self.endpoint.host_header,
            body.len()
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct AgentAuthScope<'a> {
    host_id: &'a str,
    operation: &'a str,
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn sign_agent_request(key: &[u8], host_id: &str, operation: &str, timestamp: u64) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let canonical = agent_auth_canonical(host_id, operation, timestamp);
    BASE64_STANDARD.encode(hmac::sign(&key, canonical.as_bytes()).as_ref())
}

fn agent_auth_canonical(host_id: &str, operation: &str, timestamp: u64) -> String {
    format!("{host_id}\n{operation}\n{timestamp}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpScheme {
    Http,
    Https,
}

impl HttpScheme {
    const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpEndpoint {
    scheme: HttpScheme,
    host_header: String,
    socket_addr: String,
    base_path: String,
    tls_server_name: String,
}

impl HttpEndpoint {
    fn parse(value: &str) -> Result<Self, NodeAgentError> {
        let (scheme, without_scheme) = if let Some(value) = value.strip_prefix("http://") {
            (HttpScheme::Http, value)
        } else if let Some(value) = value.strip_prefix("https://") {
            (HttpScheme::Https, value)
        } else {
            return Err(NodeAgentError::Http(
                "only http:// or https:// URLs are supported".to_owned(),
            ));
        };
        let (authority, path) = without_scheme
            .split_once('/')
            .map_or((without_scheme, ""), |(authority, path)| (authority, path));
        if authority.is_empty() || authority.contains('@') {
            return Err(NodeAgentError::Http(
                "HTTP URL is missing host or contains unsupported userinfo".to_owned(),
            ));
        }

        let tls_server_name = host_from_authority(authority)?;
        let socket_addr = if authority_has_port(authority) {
            authority.to_owned()
        } else {
            format!("{authority}:{}", scheme.default_port())
        };
        let base_path = path.trim_end_matches('/').to_owned();

        Ok(Self {
            scheme,
            host_header: authority.to_owned(),
            socket_addr,
            base_path,
            tls_server_name,
        })
    }

    fn path(&self, path: &str) -> String {
        if self.base_path.is_empty() {
            path.to_owned()
        } else {
            format!(
                "/{}/{}",
                self.base_path.trim_matches('/'),
                path.trim_start_matches('/')
            )
        }
    }
}

fn authority_has_port(authority: &str) -> bool {
    if let Some(rest) = authority.strip_prefix('[') {
        rest.contains("]:")
    } else {
        authority.rsplit_once(':').is_some()
    }
}

fn host_from_authority(authority: &str) -> Result<String, NodeAgentError> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after_host) = rest.split_once(']').ok_or_else(|| {
            NodeAgentError::Http("bracketed IPv6 URL host is missing closing bracket".to_owned())
        })?;
        if !after_host.is_empty() && !after_host.starts_with(':') {
            return Err(NodeAgentError::Http(
                "URL host contains invalid bracketed IPv6 syntax".to_owned(),
            ));
        }
        if host.is_empty() {
            return Err(NodeAgentError::Http("URL host is empty".to_owned()));
        }
        Ok(host.to_owned())
    } else if authority.matches(':').count() > 1 {
        Err(NodeAgentError::Http(
            "IPv6 URL hosts must be bracketed".to_owned(),
        ))
    } else {
        let host = authority
            .split_once(':')
            .map_or(authority, |(host, _)| host);
        if host.is_empty() {
            return Err(NodeAgentError::Http("URL host is empty".to_owned()));
        }
        Ok(host.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpResponse {
    status: u16,
    body: String,
}

fn parse_http_response(raw: &str) -> Result<HttpResponse, NodeAgentError> {
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| NodeAgentError::Http("malformed HTTP response".to_owned()))?;
    let status_line = head
        .lines()
        .next()
        .ok_or_else(|| NodeAgentError::Http("missing HTTP status line".to_owned()))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| NodeAgentError::Http("missing HTTP status code".to_owned()))?
        .parse::<u16>()
        .map_err(|err| NodeAgentError::Http(format!("invalid HTTP status code: {err}")))?;

    if !(200..300).contains(&status) {
        return Err(NodeAgentError::Http(format!(
            "control-plane returned HTTP {status}: {body}"
        )));
    }

    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

fn escape_path_segment(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                vec![char::from(byte)]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BackupObjectStoreTarget {
    root_dir: Option<PathBuf>,
    endpoint: Option<String>,
    auth: Option<BackupObjectStoreAuth>,
    tls: Option<BackupObjectStoreTls>,
    provider: String,
    bucket: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupObjectStoreAuth {
    Header {
        name: String,
        value: String,
    },
    AwsSigV4 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        service: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupObjectStoreTls {
    pub ca_cert_file: PathBuf,
    pub server_name: Option<String>,
}

fn backup_object_store_target_from_env() -> Result<Option<BackupObjectStoreTarget>, NodeAgentError>
{
    let endpoint = std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_ENDPOINT")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let root_dir = std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_DIR")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    if endpoint.is_none() && root_dir.is_none() {
        return Ok(None);
    }
    let provider = env_non_empty(
        "PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_PROVIDER",
        if endpoint.is_some() {
            "s3_compatible_http"
        } else {
            "s3_compatible_fs"
        },
    );
    let bucket = env_non_empty(
        "PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_BUCKET",
        "palimpsest-managed-postgres-backups",
    );
    validate_object_store_component("backup object-store provider", &provider)?;
    validate_object_store_component("backup object-store bucket", &bucket)?;
    let tls = backup_object_store_tls_from_env(endpoint.as_deref())?;
    if let Some(endpoint) = endpoint.as_deref() {
        validate_backup_object_store_endpoint(endpoint, tls.as_ref())?;
    }
    let auth = backup_object_store_auth_from_env()?;
    Ok(Some(BackupObjectStoreTarget {
        root_dir: root_dir.map(PathBuf::from),
        endpoint,
        auth,
        tls,
        provider,
        bucket,
    }))
}

fn backup_object_store_auth_from_env() -> Result<Option<BackupObjectStoreAuth>, NodeAgentError> {
    let token = std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let access_key_id = std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_ACCESS_KEY_ID")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let secret_access_key =
        std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_SECRET_ACCESS_KEY")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
    if token.is_some() && (access_key_id.is_some() || secret_access_key.is_some()) {
        return Err(NodeAgentError::Http(
            "backup object-store auth token and AWS SigV4 credentials cannot both be configured"
                .to_owned(),
        ));
    }
    let Some(token) = token else {
        return backup_object_store_sigv4_auth_from_env(access_key_id, secret_access_key);
    };
    let scheme = env_non_empty("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_SCHEME", "Bearer");
    let name = env_non_empty(
        "PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AUTH_HEADER",
        "authorization",
    );
    validate_http_header_name(&name)?;
    validate_http_header_value("backup object-store auth scheme", &scheme)?;
    validate_http_header_value("backup object-store auth token", &token)?;
    Ok(Some(BackupObjectStoreAuth::Header {
        name,
        value: format!("{scheme} {token}"),
    }))
}

fn backup_object_store_sigv4_auth_from_env(
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
) -> Result<Option<BackupObjectStoreAuth>, NodeAgentError> {
    match (access_key_id, secret_access_key) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => Err(NodeAgentError::Http(
            "backup object-store AWS SigV4 access key and secret key must be configured together"
                .to_owned(),
        )),
        (Some(access_key_id), Some(secret_access_key)) => {
            let region = env_non_empty(
                "PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_REGION",
                "us-east-1",
            );
            let service = env_non_empty("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_AWS_SERVICE", "s3");
            validate_http_header_value("backup object-store AWS access key id", &access_key_id)?;
            validate_http_header_value(
                "backup object-store AWS secret access key",
                &secret_access_key,
            )?;
            validate_sigv4_scope_component("backup object-store AWS region", &region)?;
            validate_sigv4_scope_component("backup object-store AWS service", &service)?;
            Ok(Some(BackupObjectStoreAuth::AwsSigV4 {
                access_key_id,
                secret_access_key,
                region,
                service,
            }))
        }
    }
}

fn backup_object_store_tls_from_env(
    endpoint: Option<&str>,
) -> Result<Option<BackupObjectStoreTls>, NodeAgentError> {
    let ca_cert_file = std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_CA_CERT_FILE")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let server_name = std::env::var("PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_TLS_SERVER_NAME")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());

    if let Some(server_name) = server_name.as_deref() {
        validate_http_header_value("backup object-store TLS server name", server_name)?;
    }

    let endpoint_scheme = endpoint
        .map(HttpEndpoint::parse)
        .transpose()?
        .map(|endpoint| endpoint.scheme);
    match (endpoint_scheme, ca_cert_file) {
        (Some(HttpScheme::Https), Some(ca_cert_file)) => Ok(Some(BackupObjectStoreTls {
            ca_cert_file: PathBuf::from(ca_cert_file),
            server_name,
        })),
        (Some(HttpScheme::Https), None) => Err(NodeAgentError::Http(
            "https backup object-store endpoint requires PALIMPSEST_PAAS_BACKUP_OBJECT_STORE_CA_CERT_FILE".to_owned(),
        )),
        (Some(HttpScheme::Http) | None, Some(_)) => Err(NodeAgentError::Http(
            "backup object-store TLS CA file requires an https object-store endpoint".to_owned(),
        )),
        (Some(HttpScheme::Http) | None, None) if server_name.is_some() => Err(NodeAgentError::Http(
            "backup object-store TLS server name requires an https object-store endpoint".to_owned(),
        )),
        (Some(HttpScheme::Http) | None, None) => Ok(None),
    }
}

fn env_non_empty(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn agent_first_port() -> u16 {
    std::env::var("PALIMPSEST_PAAS_AGENT_FIRST_PORT")
        .ok()
        .and_then(|value| value.trim().parse::<u16>().ok())
        .unwrap_or(55_000)
}

fn backup_object_key(cluster_id: &str, backup_id: &str) -> String {
    format!(
        "managed-postgres/{}/base-backups/{}",
        sanitize_component(cluster_id),
        sanitize_component(backup_id)
    )
}

fn object_store_uri(bucket: &str, object_key: &str) -> String {
    format!("s3://{bucket}/{}", object_key.trim_matches('/'))
}

fn object_store_manifest_uri(bucket: &str, object_key: &str) -> String {
    format!("{}/manifest.json", object_store_uri(bucket, object_key))
}

fn object_store_destination(
    object_store_dir: &Path,
    bucket: &str,
    object_key: &str,
) -> Result<PathBuf, NodeAgentError> {
    validate_object_store_component("backup object-store bucket", bucket)?;
    let mut destination = object_store_dir.join(bucket);
    for segment in object_key.split('/') {
        validate_object_store_component("backup object-store key segment", segment)?;
        destination = destination.join(segment);
    }
    Ok(destination)
}

fn validate_object_store_component(field: &str, value: &str) -> Result<(), NodeAgentError> {
    if value.is_empty() || value == "." || value == ".." || value.contains('/') {
        return Err(NodeAgentError::InvalidPath(format!(
            "{field} contains an invalid path component"
        )));
    }
    Ok(())
}

fn validate_backup_object_store_endpoint(
    endpoint: &str,
    tls: Option<&BackupObjectStoreTls>,
) -> Result<(), NodeAgentError> {
    let endpoint = HttpEndpoint::parse(endpoint)?;
    if endpoint.scheme == HttpScheme::Https && tls.is_none() {
        return Err(NodeAgentError::Http(
            "https backup object-store endpoint requires TLS CA configuration".to_owned(),
        ));
    }
    Ok(())
}

fn validate_http_header_name(value: &str) -> Result<(), NodeAgentError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-'))
    {
        return Err(NodeAgentError::Http(
            "backup object-store auth header name is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_http_header_value(field: &str, value: &str) -> Result<(), NodeAgentError> {
    if value.is_empty() || value.bytes().any(|byte| matches!(byte, b'\r' | b'\n')) {
        return Err(NodeAgentError::Http(format!("{field} is invalid")));
    }
    Ok(())
}

fn validate_sigv4_scope_component(field: &str, value: &str) -> Result<(), NodeAgentError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_'))
    {
        return Err(NodeAgentError::Http(format!("{field} is invalid")));
    }
    Ok(())
}

fn backup_artifacts_from_report(report: &ExecutionReport) -> Vec<NodeAgentBackupArtifact> {
    report
        .outcomes
        .iter()
        .filter_map(|outcome| {
            if outcome.status != AgentStepStatus::Succeeded {
                return None;
            }
            match &outcome.step {
                AgentStep::WriteBackupManifest {
                    backup_dir,
                    backup_id,
                    ..
                } => {
                    let manifest_path = backup_dir.join("manifest.json");
                    Some(NodeAgentBackupArtifact {
                        backup_id: backup_id.clone(),
                        provider: "local_fs".to_owned(),
                        object_uri: format!("file://{}", backup_dir.display()),
                        manifest_path: manifest_path.display().to_string(),
                        manifest_sha256: sha256_file(&manifest_path),
                        size_bytes: directory_size_bytes(backup_dir),
                    })
                }
                AgentStep::UploadBackupArtifact {
                    backup_id,
                    object_store_dir,
                    endpoint,
                    tls: _,
                    provider,
                    bucket,
                    object_key,
                    source_dir,
                    ..
                } => {
                    let size_source = if endpoint.is_some() {
                        source_dir.clone()
                    } else {
                        let destination = object_store_destination(
                            object_store_dir.as_ref()?,
                            bucket,
                            object_key,
                        )
                        .ok()?;
                        destination
                    };
                    let object_uri = object_store_uri(bucket, object_key);
                    Some(NodeAgentBackupArtifact {
                        backup_id: backup_id.clone(),
                        provider: provider.clone(),
                        object_uri,
                        manifest_path: object_store_manifest_uri(bucket, object_key),
                        manifest_sha256: sha256_file(&source_dir.join("manifest.json")),
                        size_bytes: directory_size_bytes(&size_source),
                    })
                }
                _ => None,
            }
        })
        .collect()
}

fn sha256_file(path: &Path) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    let digest = digest::digest(&digest::SHA256, &bytes);
    Some(format!("sha256:{}", hex_lower(digest.as_ref())))
}

fn directory_size_bytes(path: &Path) -> Option<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path).ok()? {
        let entry = entry.ok()?;
        let metadata = entry.metadata().ok()?;
        if metadata.is_dir() {
            total = total.checked_add(directory_size_bytes(&entry.path())?)?;
        } else if metadata.is_file() {
            total = total.checked_add(metadata.len())?;
        }
    }
    Some(total)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[must_use]
pub fn result_from_execution_report(
    host_id: &str,
    report: &ExecutionReport,
) -> NodeAgentCommandResult {
    let detail = report
        .outcomes
        .iter()
        .map(|outcome| outcome.detail.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    NodeAgentCommandResult {
        command_id: report.command_id.clone(),
        host_id: host_id.to_owned(),
        status: AgentCommandStatus::Succeeded,
        detail: Some(if detail.is_empty() {
            format!("executed {} step(s)", report.outcomes.len())
        } else {
            detail
        }),
        operation_token: None,
        backup_artifacts: backup_artifacts_from_report(report),
    }
}

#[must_use]
pub fn result_from_command_plan(host_id: &str, plan: &CommandPlan) -> NodeAgentCommandResult {
    NodeAgentCommandResult {
        command_id: plan.command_id.clone(),
        host_id: host_id.to_owned(),
        status: AgentCommandStatus::Succeeded,
        detail: Some(format!("dry-run planned {} step(s)", plan.steps.len())),
        operation_token: None,
        backup_artifacts: Vec::new(),
    }
}

#[must_use]
pub fn failed_command_result(
    host_id: &str,
    command_id: &str,
    err: &NodeAgentError,
) -> NodeAgentCommandResult {
    NodeAgentCommandResult {
        command_id: command_id.to_owned(),
        host_id: host_id.to_owned(),
        status: AgentCommandStatus::Failed,
        detail: Some(err.to_string()),
        operation_token: None,
        backup_artifacts: Vec::new(),
    }
}

pub trait ProcessRunner {
    fn run(&self, program: &Path, args: &[String]) -> Result<(), NodeAgentError>;

    fn fence_postgres_primary(
        &self,
        program: &Path,
        data_dir: &Path,
    ) -> Result<(), NodeAgentError> {
        self.run(
            program,
            &[
                "-D".to_owned(),
                data_dir.display().to_string(),
                "stop".to_owned(),
                "-m".to_owned(),
                "immediate".to_owned(),
            ],
        )
    }

    fn run_sql(
        &self,
        program: &Path,
        _data_dir: &Path,
        port: u16,
        database: &str,
        sql: &str,
    ) -> Result<(), NodeAgentError> {
        self.run(
            program,
            &[
                "--host".to_owned(),
                "127.0.0.1".to_owned(),
                "--port".to_owned(),
                port.to_string(),
                "--username".to_owned(),
                "postgres".to_owned(),
                "--dbname".to_owned(),
                database.to_owned(),
                "--set".to_owned(),
                "ON_ERROR_STOP=1".to_owned(),
                "--command".to_owned(),
                sql.to_owned(),
            ],
        )
    }

    fn run_base_backup(
        &self,
        program: &Path,
        _data_dir: &Path,
        postgres_url: &str,
        backup_dir: &Path,
        backup_id: &str,
    ) -> Result<(), NodeAgentError> {
        self.run(
            program,
            &base_backup_args(postgres_url, backup_dir, backup_id),
        )
    }
}

#[derive(Debug, Default)]
pub struct SystemProcessRunner;

impl ProcessRunner for SystemProcessRunner {
    fn run(&self, program: &Path, args: &[String]) -> Result<(), NodeAgentError> {
        let status =
            Command::new(program)
                .args(args)
                .status()
                .map_err(|err| NodeAgentError::Io {
                    action: format!("run {}", program.display()),
                    source: err,
                })?;
        if status.success() {
            Ok(())
        } else {
            Err(NodeAgentError::ProcessFailed {
                program: program.to_path_buf(),
                args: args.to_vec(),
                status: status.to_string(),
            })
        }
    }

    fn fence_postgres_primary(
        &self,
        program: &Path,
        data_dir: &Path,
    ) -> Result<(), NodeAgentError> {
        let args = vec![
            "-D".to_owned(),
            data_dir.display().to_string(),
            "stop".to_owned(),
            "-m".to_owned(),
            "immediate".to_owned(),
        ];
        let output =
            Command::new(program)
                .args(&args)
                .output()
                .map_err(|err| NodeAgentError::Io {
                    action: format!("run {}", program.display()),
                    source: err,
                })?;
        if output.status.success() || pg_ctl_stop_reports_already_stopped(&output.stderr) {
            Ok(())
        } else {
            Err(NodeAgentError::ProcessFailed {
                program: program.to_path_buf(),
                args,
                status: output.status.to_string(),
            })
        }
    }
}

fn pg_ctl_stop_reports_already_stopped(stderr: &[u8]) -> bool {
    let stderr = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    stderr.contains("no server running") || stderr.contains("pid file does not exist")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerPostgresRunner {
    image: String,
    runtime_root: PathBuf,
}

impl DockerPostgresRunner {
    pub fn new(image: impl Into<String>, runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            image: image.into(),
            runtime_root: runtime_root.into(),
        }
    }

    pub fn postgres18(runtime_root: impl Into<PathBuf>) -> Self {
        Self::new("postgres:18", runtime_root)
    }

    fn pg_ctl_mode(args: &[String]) -> Option<PgCtlMode> {
        if args.iter().any(|arg| arg == "start") {
            Some(PgCtlMode::Start)
        } else if args.iter().any(|arg| arg == "stop") {
            Some(PgCtlMode::Stop)
        } else {
            None
        }
    }

    fn data_dir_arg(args: &[String]) -> Result<PathBuf, NodeAgentError> {
        args.windows(2)
            .find_map(|window| {
                if window[0] == "-D" {
                    Some(PathBuf::from(&window[1]))
                } else {
                    None
                }
            })
            .ok_or_else(|| NodeAgentError::InvalidPath("pg_ctl command missing -D".to_owned()))
    }

    fn container_name_for_data_dir(data_dir: &Path) -> Result<String, NodeAgentError> {
        let cluster_dir = data_dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                NodeAgentError::InvalidPath(format!(
                    "missing cluster directory for {}",
                    data_dir.display()
                ))
            })?;
        Ok(format!("palimpsest-pg-{}", sanitize_component(cluster_dir)))
    }

    fn docker_args(&self, program: &Path, args: &[String]) -> Result<Vec<String>, NodeAgentError> {
        let program_name = program
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                NodeAgentError::InvalidPath(format!(
                    "missing program name for {}",
                    program.display()
                ))
            })?;
        let volume = format!(
            "{}:{}",
            self.runtime_root.display(),
            self.runtime_root.display()
        );
        let mut docker_args = vec![
            "run".to_owned(),
            "--rm".to_owned(),
            "-v".to_owned(),
            volume,
            self.image.clone(),
            "bash".to_owned(),
            "-lc".to_owned(),
            "runtime_root=\"$1\"; program=\"$2\"; shift 2; chown -R postgres \"$runtime_root\" && exec gosu postgres \"/usr/lib/postgresql/18/bin/${program}\" \"$@\"".to_owned(),
            "--".to_owned(),
            self.runtime_root.display().to_string(),
            program_name.to_owned(),
        ];
        docker_args.extend(args.iter().cloned());
        Ok(docker_args)
    }

    fn docker_start_args(
        &self,
        data_dir: &Path,
        port: Option<u16>,
    ) -> Result<Vec<String>, NodeAgentError> {
        let volume = format!(
            "{}:{}",
            self.runtime_root.display(),
            self.runtime_root.display()
        );
        let mut args = vec![
            "run".to_owned(),
            "-d".to_owned(),
            "--name".to_owned(),
            Self::container_name_for_data_dir(data_dir)?,
            "--restart".to_owned(),
            "unless-stopped".to_owned(),
            "-v".to_owned(),
            volume,
        ];
        if let Some(port) = port {
            args.extend(["-p".to_owned(), format!("127.0.0.1:{port}:{port}")]);
        }
        args.extend([
            self.image.clone(),
            "bash".to_owned(),
            "-lc".to_owned(),
            "runtime_root=\"$1\"; data_dir=\"$2\"; chown -R postgres \"$runtime_root\" && exec gosu postgres /usr/lib/postgresql/18/bin/postgres -D \"$data_dir\"".to_owned(),
            "--".to_owned(),
            self.runtime_root.display().to_string(),
            data_dir.display().to_string(),
        ]);
        Ok(args)
    }

    fn postgres_config_port(data_dir: &Path) -> Result<Option<u16>, NodeAgentError> {
        let config_path = data_dir.join("postgresql.conf");
        let config = match fs::read_to_string(&config_path) {
            Ok(config) => config,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(NodeAgentError::Io {
                    action: format!("read {}", config_path.display()),
                    source: err,
                });
            }
        };
        for line in config.lines() {
            let line = line.trim();
            let Some(value) = line.strip_prefix("port") else {
                continue;
            };
            let Some(value) = value.trim_start().strip_prefix('=') else {
                continue;
            };
            let value = value.trim().trim_matches('\'').trim_matches('"');
            return value.parse::<u16>().map(Some).map_err(|err| {
                NodeAgentError::InvalidPath(format!(
                    "invalid PostgreSQL port in {}: {err}",
                    config_path.display()
                ))
            });
        }
        Ok(None)
    }

    fn docker_stop_args(data_dir: &Path) -> Result<Vec<String>, NodeAgentError> {
        Ok(vec![
            "rm".to_owned(),
            "-f".to_owned(),
            Self::container_name_for_data_dir(data_dir)?,
        ])
    }

    fn docker_rm_force(&self, container_name: &str) -> Result<(), NodeAgentError> {
        let output = Command::new("docker")
            .args(["rm", "-f", container_name])
            .output()
            .map_err(|err| NodeAgentError::Io {
                action: "run docker rm -f".to_owned(),
                source: err,
            })?;
        if output.status.success() || output.stderr.windows(14).any(|w| w == b"No such object") {
            Ok(())
        } else {
            Err(NodeAgentError::ProcessFailed {
                program: PathBuf::from("docker"),
                args: vec!["rm".to_owned(), "-f".to_owned(), container_name.to_owned()],
                status: output.status.to_string(),
            })
        }
    }

    fn wait_until_postgres_accepts_connections(
        container_name: &str,
        port: u16,
    ) -> Result<(), NodeAgentError> {
        let port = port.to_string();
        let args = [
            "exec",
            container_name,
            "pg_isready",
            "--host",
            "127.0.0.1",
            "--port",
            port.as_str(),
            "--username",
            "postgres",
        ];
        for _ in 0..30 {
            let status =
                Command::new("docker")
                    .args(args)
                    .status()
                    .map_err(|err| NodeAgentError::Io {
                        action: "run docker exec pg_isready".to_owned(),
                        source: err,
                    })?;
            if status.success() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        Err(NodeAgentError::ProcessFailed {
            program: PathBuf::from("docker"),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            status: "postgres did not become ready within 30 seconds".to_owned(),
        })
    }
}

impl ProcessRunner for DockerPostgresRunner {
    fn run(&self, program: &Path, args: &[String]) -> Result<(), NodeAgentError> {
        let program_name = program
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                NodeAgentError::InvalidPath(format!(
                    "missing program name for {}",
                    program.display()
                ))
            })?;
        let mut wait_for_start = None;
        let docker_args = if program_name == "pg_ctl" {
            match Self::pg_ctl_mode(args) {
                Some(PgCtlMode::Start) => {
                    let data_dir = Self::data_dir_arg(args)?;
                    let container_name = Self::container_name_for_data_dir(&data_dir)?;
                    let port = Self::postgres_config_port(&data_dir)?;
                    self.docker_rm_force(&container_name)?;
                    wait_for_start = port.map(|port| (container_name, port));
                    self.docker_start_args(&data_dir, port)?
                }
                Some(PgCtlMode::Stop) => {
                    let data_dir = Self::data_dir_arg(args)?;
                    Self::docker_stop_args(&data_dir)?
                }
                None => self.docker_args(program, args)?,
            }
        } else {
            self.docker_args(program, args)?
        };

        let status = Command::new("docker")
            .args(&docker_args)
            .status()
            .map_err(|err| NodeAgentError::Io {
                action: "run docker".to_owned(),
                source: err,
            })?;
        if status.success() {
            if let Some((container_name, port)) = wait_for_start {
                Self::wait_until_postgres_accepts_connections(&container_name, port)?;
            }
            Ok(())
        } else {
            Err(NodeAgentError::ProcessFailed {
                program: PathBuf::from("docker"),
                args: docker_args,
                status: status.to_string(),
            })
        }
    }

    fn run_sql(
        &self,
        _program: &Path,
        data_dir: &Path,
        port: u16,
        database: &str,
        sql: &str,
    ) -> Result<(), NodeAgentError> {
        let container_name = Self::container_name_for_data_dir(data_dir)?;
        Self::wait_until_postgres_accepts_connections(&container_name, port)?;
        let docker_args = vec![
            "exec".to_owned(),
            container_name,
            "psql".to_owned(),
            "--host".to_owned(),
            "127.0.0.1".to_owned(),
            "--port".to_owned(),
            port.to_string(),
            "--username".to_owned(),
            "postgres".to_owned(),
            "--dbname".to_owned(),
            database.to_owned(),
            "--set".to_owned(),
            "ON_ERROR_STOP=1".to_owned(),
            "--command".to_owned(),
            sql.to_owned(),
        ];

        let status = Command::new("docker")
            .args(&docker_args)
            .status()
            .map_err(|err| NodeAgentError::Io {
                action: "run docker exec psql".to_owned(),
                source: err,
            })?;
        if status.success() {
            Ok(())
        } else {
            Err(NodeAgentError::ProcessFailed {
                program: PathBuf::from("docker"),
                args: docker_args,
                status: status.to_string(),
            })
        }
    }

    fn run_base_backup(
        &self,
        _program: &Path,
        data_dir: &Path,
        postgres_url: &str,
        backup_dir: &Path,
        backup_id: &str,
    ) -> Result<(), NodeAgentError> {
        let container_name = Self::container_name_for_data_dir(data_dir)?;
        let mut docker_args = vec![
            "exec".to_owned(),
            container_name,
            "pg_basebackup".to_owned(),
        ];
        docker_args.extend(base_backup_args(postgres_url, backup_dir, backup_id));

        let status = Command::new("docker")
            .args(&docker_args)
            .status()
            .map_err(|err| NodeAgentError::Io {
                action: "run docker exec pg_basebackup".to_owned(),
                source: err,
            })?;
        if status.success() {
            Ok(())
        } else {
            Err(NodeAgentError::ProcessFailed {
                program: PathBuf::from("docker"),
                args: docker_args,
                status: status.to_string(),
            })
        }
    }

    fn fence_postgres_primary(
        &self,
        _program: &Path,
        data_dir: &Path,
    ) -> Result<(), NodeAgentError> {
        let container_name = Self::container_name_for_data_dir(data_dir)?;
        self.docker_rm_force(&container_name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgCtlMode {
    Start,
    Stop,
}

fn succeeded(step: &AgentStep, detail: String) -> AgentStepOutcome {
    AgentStepOutcome {
        step: step.clone(),
        status: AgentStepStatus::Succeeded,
        detail,
    }
}

fn base_backup_args(postgres_url: &str, backup_dir: &Path, backup_id: &str) -> Vec<String> {
    vec![
        "--dbname".to_owned(),
        postgres_url.to_owned(),
        "--pgdata".to_owned(),
        backup_dir.display().to_string(),
        "--checkpoint".to_owned(),
        "fast".to_owned(),
        "--wal-method".to_owned(),
        "stream".to_owned(),
        "--label".to_owned(),
        backup_id.to_owned(),
    ]
}

#[derive(Debug, Serialize)]
struct BackupManifest<'a> {
    manifest_version: u8,
    backup_id: &'a str,
    cluster_id: &'a str,
    host_id: &'a str,
    postgres_version: Option<String>,
    backup_dir: String,
    source_data_dir: String,
    created_at_unix_seconds: u64,
    artifacts: Vec<BackupManifestArtifact>,
}

#[derive(Debug, Serialize)]
struct BackupManifestArtifact {
    kind: &'static str,
    path: String,
}

#[derive(Debug, Serialize)]
struct CloneRedactionApplied {
    policy_id: String,
    rule_count: usize,
    applied_at: u64,
}

struct BackupManifestInput<'a> {
    backup_id: &'a str,
    cluster_id: &'a str,
    host_id: &'a str,
    data_dir: &'a Path,
}

fn write_backup_manifest(
    backup_dir: &Path,
    input: BackupManifestInput<'_>,
) -> Result<(), NodeAgentError> {
    let postgres_version = read_optional_trimmed(&backup_dir.join("PG_VERSION"))?;
    let created_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let manifest = BackupManifest {
        manifest_version: 1,
        backup_id: input.backup_id,
        cluster_id: input.cluster_id,
        host_id: input.host_id,
        postgres_version,
        backup_dir: backup_dir.display().to_string(),
        source_data_dir: input.data_dir.display().to_string(),
        created_at_unix_seconds,
        artifacts: vec![BackupManifestArtifact {
            kind: "pg_basebackup_directory",
            path: ".".to_owned(),
        }],
    };
    let raw = serde_json::to_vec_pretty(&manifest)?;
    fs::write(backup_dir.join("manifest.json"), raw).map_err(|err| NodeAgentError::Io {
        action: format!("write backup manifest {}", backup_dir.display()),
        source: err,
    })
}

fn read_optional_trimmed(path: &Path) -> Result<Option<String>, NodeAgentError> {
    match fs::read_to_string(path) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_owned()))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(NodeAgentError::Io {
            action: format!("read {}", path.display()),
            source: err,
        }),
    }
}

fn copy_dir_contents(source: &Path, destination: &Path) -> Result<(), NodeAgentError> {
    fs::create_dir_all(destination).map_err(|err| NodeAgentError::Io {
        action: format!("create directory {}", destination.display()),
        source: err,
    })?;
    for entry in fs::read_dir(source).map_err(|err| NodeAgentError::Io {
        action: format!("read directory {}", source.display()),
        source: err,
    })? {
        let entry = entry.map_err(|err| NodeAgentError::Io {
            action: format!("read directory entry {}", source.display()),
            source: err,
        })?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().map_err(|err| NodeAgentError::Io {
            action: format!("read file type {}", source_path.display()),
            source: err,
        })?;
        if entry.file_name().to_str() == Some("manifest.json") {
            continue;
        }
        if file_type.is_dir() {
            copy_dir_contents(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|err| NodeAgentError::Io {
                action: format!(
                    "copy {} to {}",
                    source_path.display(),
                    destination_path.display()
                ),
                source: err,
            })?;
        }
    }
    Ok(())
}

fn copy_dir_all(source: &Path, destination: &Path) -> Result<(), NodeAgentError> {
    fs::create_dir_all(destination).map_err(|err| NodeAgentError::Io {
        action: format!("create directory {}", destination.display()),
        source: err,
    })?;
    for entry in fs::read_dir(source).map_err(|err| NodeAgentError::Io {
        action: format!("read directory {}", source.display()),
        source: err,
    })? {
        let entry = entry.map_err(|err| NodeAgentError::Io {
            action: format!("read directory entry {}", source.display()),
            source: err,
        })?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().map_err(|err| NodeAgentError::Io {
            action: format!("read file type {}", source_path.display()),
            source: err,
        })?;
        if file_type.is_dir() {
            copy_dir_all(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path).map_err(|err| NodeAgentError::Io {
                action: format!(
                    "copy {} to {}",
                    source_path.display(),
                    destination_path.display()
                ),
                source: err,
            })?;
        }
    }
    Ok(())
}

fn upload_dir_to_http_object_store(
    source: &Path,
    endpoint: &str,
    auth: Option<&BackupObjectStoreAuth>,
    tls: Option<&BackupObjectStoreTls>,
    bucket: &str,
    object_key: &str,
) -> Result<(), NodeAgentError> {
    validate_object_store_component("backup object-store bucket", bucket)?;
    let endpoint = HttpEndpoint::parse(endpoint)?;
    if endpoint.scheme == HttpScheme::Https && tls.is_none() {
        return Err(NodeAgentError::Http(
            "https backup object-store endpoint requires TLS CA configuration".to_owned(),
        ));
    }
    for file_path in files_under_dir(source)? {
        let relative_path = file_path.strip_prefix(source).map_err(|err| {
            NodeAgentError::InvalidPath(format!(
                "backup object path {} is not under {}: {err}",
                file_path.display(),
                source.display()
            ))
        })?;
        let object_path = http_object_store_path(bucket, object_key, relative_path)?;
        let body = fs::read(&file_path).map_err(|err| NodeAgentError::Io {
            action: format!("read backup object {}", file_path.display()),
            source: err,
        })?;
        http_put_bytes(&endpoint, &object_path, auth, tls, &body)?;
    }
    Ok(())
}

fn files_under_dir(path: &Path) -> Result<Vec<PathBuf>, NodeAgentError> {
    let mut files = Vec::new();
    collect_files_under_dir(path, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files_under_dir(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), NodeAgentError> {
    for entry in fs::read_dir(path).map_err(|err| NodeAgentError::Io {
        action: format!("read directory {}", path.display()),
        source: err,
    })? {
        let entry = entry.map_err(|err| NodeAgentError::Io {
            action: format!("read directory entry {}", path.display()),
            source: err,
        })?;
        let entry_path = entry.path();
        let file_type = entry.file_type().map_err(|err| NodeAgentError::Io {
            action: format!("read file type {}", entry_path.display()),
            source: err,
        })?;
        if file_type.is_dir() {
            collect_files_under_dir(&entry_path, files)?;
        } else if file_type.is_file() {
            files.push(entry_path);
        }
    }
    Ok(())
}

fn http_object_store_path(
    bucket: &str,
    object_key: &str,
    relative_path: &Path,
) -> Result<String, NodeAgentError> {
    let mut segments = vec![escape_path_segment(bucket)];
    for segment in object_key.split('/') {
        validate_object_store_component("backup object-store key segment", segment)?;
        segments.push(escape_path_segment(segment));
    }
    for component in relative_path.components() {
        let std::path::Component::Normal(segment) = component else {
            return Err(NodeAgentError::InvalidPath(
                "backup object path contains an invalid component".to_owned(),
            ));
        };
        let segment = segment.to_str().ok_or_else(|| {
            NodeAgentError::InvalidPath("backup object path is not UTF-8".to_owned())
        })?;
        validate_object_store_component("backup object-store relative path segment", segment)?;
        segments.push(escape_path_segment(segment));
    }
    Ok(format!("/{}", segments.join("/")))
}

fn http_put_bytes(
    endpoint: &HttpEndpoint,
    path: &str,
    auth: Option<&BackupObjectStoreAuth>,
    tls: Option<&BackupObjectStoreTls>,
    body: &[u8],
) -> Result<(), NodeAgentError> {
    let request_path = endpoint.path(path);
    let request = http_put_request_bytes(endpoint, &request_path, auth, body);
    match endpoint.scheme {
        HttpScheme::Http => http_put_plaintext(endpoint, &request),
        HttpScheme::Https => {
            let tls = tls.ok_or_else(|| {
                NodeAgentError::Http(
                    "https backup object-store endpoint requires TLS CA configuration".to_owned(),
                )
            })?;
            http_put_tls(endpoint, tls, &request)
        }
    }
}

fn http_put_plaintext(endpoint: &HttpEndpoint, request: &[u8]) -> Result<(), NodeAgentError> {
    let mut stream = TcpStream::connect(&endpoint.socket_addr).map_err(|err| {
        NodeAgentError::Http(format!(
            "connect backup object-store {}: {err}",
            endpoint.socket_addr
        ))
    })?;
    write_put_request_and_parse_response(&mut stream, request)
}

fn http_put_tls(
    endpoint: &HttpEndpoint,
    tls: &BackupObjectStoreTls,
    request: &[u8],
) -> Result<(), NodeAgentError> {
    let config = backup_object_store_tls_client_config(&tls.ca_cert_file)?;
    let server_name = tls
        .server_name
        .as_deref()
        .unwrap_or(&endpoint.tls_server_name)
        .to_owned();
    let server_name = ServerName::try_from(server_name).map_err(|err| {
        NodeAgentError::Http(format!(
            "invalid backup object-store TLS server name: {err}"
        ))
    })?;
    let tcp = TcpStream::connect(&endpoint.socket_addr).map_err(|err| {
        NodeAgentError::Http(format!(
            "connect backup object-store {}: {err}",
            endpoint.socket_addr
        ))
    })?;
    let connection = ClientConnection::new(Arc::new(config), server_name).map_err(|err| {
        NodeAgentError::Http(format!("initialize backup object-store TLS client: {err}"))
    })?;
    let mut stream = StreamOwned::new(connection, tcp);
    write_put_request_and_parse_response(&mut stream, request)
}

fn backup_object_store_tls_client_config(
    ca_cert_file: &Path,
) -> Result<ClientConfig, NodeAgentError> {
    let file = fs::File::open(ca_cert_file).map_err(|err| NodeAgentError::Io {
        action: format!(
            "open backup object-store CA certificate {}",
            ca_cert_file.display()
        ),
        source: err,
    })?;
    let mut reader = BufReader::new(file);
    let ca_certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| NodeAgentError::Http(format!("parse backup object-store CA PEM: {err}")))?;
    if ca_certs.is_empty() {
        return Err(NodeAgentError::Http(
            "backup object-store CA PEM contained no certificates".to_owned(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for cert in ca_certs {
        roots.add(cert).map_err(|err| {
            NodeAgentError::Http(format!("load backup object-store CA certificate: {err}"))
        })?;
    }
    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn write_put_request_and_parse_response<S: Read + Write>(
    stream: &mut S,
    request: &[u8],
) -> Result<(), NodeAgentError> {
    stream
        .write_all(request)
        .map_err(|err| NodeAgentError::Http(format!("write backup object PUT: {err}")))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|err| NodeAgentError::Http(format!("read backup object PUT response: {err}")))?;
    let response = String::from_utf8_lossy(&response);
    parse_http_response(&response)?;
    Ok(())
}

fn http_put_request_bytes(
    endpoint: &HttpEndpoint,
    request_path: &str,
    auth: Option<&BackupObjectStoreAuth>,
    body: &[u8],
) -> Vec<u8> {
    http_put_request_bytes_at(endpoint, request_path, auth, body, unix_timestamp_seconds())
}

fn http_put_request_bytes_at(
    endpoint: &HttpEndpoint,
    request_path: &str,
    auth: Option<&BackupObjectStoreAuth>,
    body: &[u8],
    unix_seconds: u64,
) -> Vec<u8> {
    let auth_headers = auth
        .map(|auth| {
            render_backup_object_store_auth_headers(
                auth,
                endpoint,
                request_path,
                body,
                unix_seconds,
            )
        })
        .unwrap_or_default();
    let request_head = format!(
        "PUT {request_path} HTTP/1.1\r\nhost: {}\r\n{}content-length: {}\r\ncontent-type: application/octet-stream\r\nconnection: close\r\n\r\n",
        endpoint.host_header,
        auth_headers,
        body.len()
    );
    let mut request = request_head.into_bytes();
    request.extend_from_slice(body);
    request
}

fn render_backup_object_store_auth_headers(
    auth: &BackupObjectStoreAuth,
    endpoint: &HttpEndpoint,
    request_path: &str,
    body: &[u8],
    unix_seconds: u64,
) -> String {
    match auth {
        BackupObjectStoreAuth::Header { name, value } => format!("{name}: {value}\r\n"),
        BackupObjectStoreAuth::AwsSigV4 {
            access_key_id,
            secret_access_key,
            region,
            service,
        } => {
            let payload_sha256 = hex_lower(digest::digest(&digest::SHA256, body).as_ref());
            let timestamp = aws_sigv4_timestamp_from_unix_seconds(unix_seconds);
            let signed = aws_sigv4_authorization_header(AwsSigV4SigningInput {
                access_key_id,
                secret_access_key,
                region,
                service,
                host: &endpoint.host_header,
                request_path,
                payload_sha256: &payload_sha256,
                timestamp: &timestamp,
            });
            format!(
                "x-amz-content-sha256: {payload_sha256}\r\nx-amz-date: {}\r\nAuthorization: {signed}\r\n",
                timestamp.amz_date
            )
        }
    }
}

struct AwsSigV4Timestamp {
    date: String,
    amz_date: String,
}

struct AwsSigV4SigningInput<'a> {
    access_key_id: &'a str,
    secret_access_key: &'a str,
    region: &'a str,
    service: &'a str,
    host: &'a str,
    request_path: &'a str,
    payload_sha256: &'a str,
    timestamp: &'a AwsSigV4Timestamp,
}

fn aws_sigv4_authorization_header(input: AwsSigV4SigningInput<'_>) -> String {
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        input.host, input.payload_sha256, input.timestamp.amz_date
    );
    let canonical_request = format!(
        "PUT\n{}\n\n{canonical_headers}\n{signed_headers}\n{}",
        input.request_path, input.payload_sha256
    );
    let canonical_request_hash =
        hex_lower(digest::digest(&digest::SHA256, canonical_request.as_bytes()).as_ref());
    let credential_scope = format!(
        "{}/{}/{}/aws4_request",
        input.timestamp.date, input.region, input.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{credential_scope}\n{canonical_request_hash}",
        input.timestamp.amz_date
    );
    let signing_key = aws_sigv4_signing_key(
        input.secret_access_key,
        &input.timestamp.date,
        input.region,
        input.service,
    );
    let signature = hmac::sign(&signing_key, string_to_sign.as_bytes());
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={}",
        input.access_key_id,
        hex_lower(signature.as_ref())
    )
}

fn aws_sigv4_signing_key(
    secret_access_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> hmac::Key {
    let secret = format!("AWS4{secret_access_key}");
    let date_key = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
        date.as_bytes(),
    );
    let region_key = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, date_key.as_ref()),
        region.as_bytes(),
    );
    let service_key = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, region_key.as_ref()),
        service.as_bytes(),
    );
    let signing_key = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, service_key.as_ref()),
        b"aws4_request",
    );
    hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref())
}

fn aws_sigv4_timestamp_from_unix_seconds(seconds: u64) -> AwsSigV4Timestamp {
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    AwsSigV4Timestamp {
        date: format!("{year:04}{month:02}{day:02}"),
        amz_date: format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
    }
}

fn render_postgres_config(port: u16) -> String {
    format!(
        "\
listen_addresses = '*'
port = {port}
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
file_copy_method = clone
log_connections = on
log_disconnections = on
"
    )
}

const fn render_pg_hba_config() -> &'static str {
    "\
local all all trust
host all all 127.0.0.1/32 trust
host all all ::1/128 trust
host replication all 127.0.0.1/32 trust
host replication all ::1/128 trust
host all all 172.16.0.0/12 scram-sha-256
host replication all 172.16.0.0/12 scram-sha-256
"
}

fn render_recovery_config(restore_command: &str, recovery_target_lsn: Option<&str>) -> String {
    let mut config = format!(
        "restore_command = '{}'\n",
        escape_postgres_string(restore_command)
    );
    if let Some(lsn) = recovery_target_lsn {
        config.push_str(&format!(
            "recovery_target_lsn = '{}'\n",
            escape_postgres_string(lsn)
        ));
    }
    config
}

fn render_standby_config(
    primary_conninfo: &str,
    primary_slot_name: &str,
    restore_command: &str,
) -> String {
    format!(
        "primary_conninfo = '{}'\nprimary_slot_name = '{}'\nrestore_command = '{}'\nhot_standby = on\n",
        escape_postgres_string(primary_conninfo),
        escape_postgres_string(primary_slot_name),
        escape_postgres_string(restore_command)
    )
}

fn render_terminate_database_connections_sql(database: &str) -> Result<String, NodeAgentError> {
    validate_identifier(database)?;
    Ok(format!(
        "\
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = {database_literal}
  AND pid <> pg_backend_pid();
",
        database_literal = quote_literal(database),
    ))
}

fn render_copy_on_write_database_clone_sql(
    source_database: &str,
    target_database: &str,
) -> Result<String, NodeAgentError> {
    validate_identifier(source_database)?;
    validate_identifier(target_database)?;
    Ok(format!(
        "CREATE DATABASE {target_database} TEMPLATE {source_database} STRATEGY FILE_COPY;\n",
        target_database = quote_ident(target_database),
        source_database = quote_ident(source_database),
    ))
}

/// Databases the node agent must never drop, regardless of caller input.
///
/// These are `PostgreSQL`'s built-in admin/template databases; a branch is always
/// a distinct cloned database.
const PROTECTED_DATABASES: [&str; 3] = ["postgres", "template0", "template1"];

fn ensure_droppable_database(database: &str) -> Result<(), NodeAgentError> {
    validate_identifier(database)?;
    if PROTECTED_DATABASES.contains(&database) {
        return Err(NodeAgentError::ProtectedDatabase(database.to_owned()));
    }
    Ok(())
}

fn render_drop_database_sql(database: &str, force: bool) -> Result<String, NodeAgentError> {
    ensure_droppable_database(database)?;
    let force_clause = if force { " WITH (FORCE)" } else { "" };
    Ok(format!(
        "DROP DATABASE IF EXISTS {database}{force_clause};\n",
        database = quote_ident(database),
    ))
}

fn render_physical_replication_slot_sql(slot_name: &str) -> Result<String, NodeAgentError> {
    validate_identifier(slot_name)?;
    let slot_literal = quote_literal(slot_name);
    Ok(format!(
        "\
SELECT pg_create_physical_replication_slot({slot_literal}, true)
WHERE NOT EXISTS (
  SELECT 1 FROM pg_replication_slots WHERE slot_name = {slot_literal}
);
"
    ))
}

fn render_standby_lag_check_sql(
    slot_name: &str,
    max_lag_bytes: u64,
) -> Result<String, NodeAgentError> {
    validate_identifier(slot_name)?;
    let slot_literal = quote_literal(slot_name);
    Ok(format!(
        "\
DO $$
DECLARE
  observed_lag numeric;
BEGIN
  SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
    INTO observed_lag
    FROM pg_replication_slots
   WHERE slot_name = {slot_literal}
     AND slot_type = 'physical';

  IF observed_lag IS NULL THEN
    RAISE EXCEPTION 'physical replication slot % is missing or has no restart_lsn', {slot_literal};
  END IF;

  IF observed_lag > {max_lag_bytes} THEN
    RAISE EXCEPTION 'physical replication slot % lag % exceeds {max_lag_bytes} bytes', {slot_literal}, observed_lag;
  END IF;
END
$$;
"
    ))
}

fn render_clone_redaction_sql(
    policy: &ManagedPostgresCloneRedactionPolicy,
) -> Result<String, NodeAgentError> {
    if policy.rules.is_empty() {
        return Err(NodeAgentError::InvalidRedactionPolicy(
            "clone redaction policy requires at least one rule".to_owned(),
        ));
    }
    let mut sql = String::from("BEGIN;\n");
    for rule in &policy.rules {
        validate_identifier(&rule.table_schema)?;
        validate_identifier(&rule.table_name)?;
        validate_identifier(&rule.column_name)?;
        let table = format!(
            "{}.{}",
            quote_ident(&rule.table_schema),
            quote_ident(&rule.table_name)
        );
        let column = quote_ident(&rule.column_name);
        let expression = match rule.method {
            CloneRedactionMethod::Null => "NULL".to_owned(),
            CloneRedactionMethod::StaticValue => {
                let value = rule.static_value.as_deref().ok_or_else(|| {
                    NodeAgentError::InvalidRedactionPolicy(format!(
                        "static redaction rule {}.{}.{} requires static_value",
                        rule.table_schema, rule.table_name, rule.column_name
                    ))
                })?;
                quote_literal(value)
            }
            CloneRedactionMethod::HashSha256 => {
                format!("encode(sha256(convert_to(COALESCE({column}::text, ''), 'UTF8')), 'hex')")
            }
        };
        sql.push_str(&format!(
            "UPDATE {table} SET {column} = {expression} WHERE {column} IS NOT NULL;\n"
        ));
    }
    sql.push_str("COMMIT;\n");
    Ok(sql)
}

fn major_upgrade_preflight_sql(
    source_postgres_version: &palimpsest_paas_core::PostgresVersion,
    target_postgres_version: &palimpsest_paas_core::PostgresVersion,
    strategy: palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy,
) -> String {
    let source_major = source_postgres_version.major();
    let target_major = target_postgres_version.major();
    let strategy_label = major_upgrade_strategy_label(strategy);
    format!(
        "DO $palimpsest_major_upgrade_preflight$\n\
         DECLARE\n\
           source_major integer := {source_major};\n\
           target_major integer := {target_major};\n\
           actual_major integer := current_setting('server_version_num')::integer / 10000;\n\
           invalid_indexes bigint := 0;\n\
         BEGIN\n\
           IF target_major <= source_major THEN\n\
             RAISE EXCEPTION 'target PostgreSQL major % must be greater than source major %', target_major, source_major;\n\
           END IF;\n\
           IF actual_major <> source_major THEN\n\
             RAISE EXCEPTION 'connected PostgreSQL major % does not match expected source major %', actual_major, source_major;\n\
           END IF;\n\
           IF {strategy_literal} = 'logical_replication_copy' AND current_setting('wal_level') <> 'logical' THEN\n\
             RAISE EXCEPTION 'logical replication major upgrades require wal_level=logical, found %', current_setting('wal_level');\n\
           END IF;\n\
           SELECT count(*) INTO invalid_indexes FROM pg_catalog.pg_index WHERE NOT indisvalid OR NOT indisready;\n\
           IF invalid_indexes > 0 THEN\n\
             RAISE EXCEPTION 'major upgrade preflight found % invalid or not-ready indexes', invalid_indexes;\n\
           END IF;\n\
         END\n\
         $palimpsest_major_upgrade_preflight$;\n\
         SELECT jsonb_build_object(\n\
           'source_major', {source_major},\n\
           'target_major', {target_major},\n\
           'strategy', {strategy_literal},\n\
           'server_version_num', current_setting('server_version_num'),\n\
           'wal_level', current_setting('wal_level'),\n\
           'connectable_databases', (SELECT count(*) FROM pg_catalog.pg_database WHERE datallowconn),\n\
           'extensions', COALESCE((SELECT jsonb_agg(jsonb_build_object('name', extname, 'version', extversion) ORDER BY extname) FROM pg_catalog.pg_extension), '[]'::jsonb)\n\
         ) AS palimpsest_major_upgrade_preflight;",
        strategy_literal = quote_literal(strategy_label),
    )
}

const fn major_upgrade_strategy_label(
    strategy: palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy,
) -> &'static str {
    match strategy {
        palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy => {
            "logical_replication_copy"
        }
        palimpsest_paas_core::ManagedPostgresMajorUpgradeStrategy::PgUpgradeCopy => {
            "pg_upgrade_copy"
        }
    }
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    env_string(name).and_then(|value| match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}

fn detect_os_release() -> String {
    fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|raw| {
            raw.lines()
                .find_map(|line| line.strip_prefix("PRETTY_NAME="))
                .map(|value| value.trim_matches('"').to_owned())
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| std::env::consts::OS.to_owned())
}

fn detect_kernel_version() -> String {
    command_stdout("uname", &["-r"]).unwrap_or_else(|| "unknown".to_owned())
}

fn detect_container_runtime() -> String {
    command_stdout("docker", &["--version"])
        .or_else(|| command_stdout("podman", &["--version"]))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn detect_disk_encryption() -> bool {
    if cfg!(target_os = "macos") {
        return command_stdout("fdesetup", &["status"])
            .is_some_and(|output| output.to_ascii_lowercase().contains("filevault is on"));
    }
    false
}

fn detect_firewall_enabled() -> bool {
    if cfg!(target_os = "linux") {
        return command_stdout("ufw", &["status"])
            .is_some_and(|output| output.to_ascii_lowercase().contains("status: active"));
    }
    if cfg!(target_os = "macos") {
        return command_stdout(
            "/usr/libexec/ApplicationFirewall/socketfilterfw",
            &["--getglobalstate"],
        )
        .is_some_and(|output| output.to_ascii_lowercase().contains("enabled"));
    }
    false
}

fn detect_unattended_upgrades() -> bool {
    fs::read_to_string("/etc/apt/apt.conf.d/20auto-upgrades").is_ok_and(|raw| {
        raw.contains("APT::Periodic::Update-Package-Lists \"1\"")
            && raw.contains("APT::Periodic::Unattended-Upgrade \"1\"")
    })
}

fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn rfc3339_from_unix_seconds(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

const fn civil_from_days(days_since_unix_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32, day as u32)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PostgresAccessSql {
    roles_and_publication: String,
    replication_slot: String,
}

fn render_postgres_access_sql(
    database: &str,
    roles: &[DatabaseRoleCredential],
    publication: &str,
    replication_slot: &str,
) -> Result<PostgresAccessSql, NodeAgentError> {
    validate_identifier(database)?;
    let mut roles_and_publication = String::new();
    for role in roles {
        validate_identifier(&role.name)?;
        roles_and_publication.push_str(&render_role_sql(database, role));
    }
    validate_identifier(publication)?;
    validate_identifier(replication_slot)?;
    roles_and_publication.push_str(&format!(
        "\
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = {publication_literal}) THEN
    EXECUTE 'CREATE PUBLICATION {publication_ident} FOR ALL TABLES';
  END IF;
END
$$;
",
        publication_ident = quote_ident(publication),
        publication_literal = quote_literal(publication),
    ));
    let replication_slot = format!(
        "\
SELECT pg_create_logical_replication_slot({slot_literal}, 'pgoutput')
WHERE NOT EXISTS (
  SELECT 1 FROM pg_replication_slots WHERE slot_name = {slot_literal}
);
",
        slot_literal = quote_literal(replication_slot),
    );
    Ok(PostgresAccessSql {
        roles_and_publication,
        replication_slot,
    })
}

fn render_role_sql(database: &str, role: &DatabaseRoleCredential) -> String {
    let database_ident = quote_ident(database);
    let role_ident = quote_ident(&role.name);
    let role_literal = quote_literal(&role.name);
    let password_literal = quote_literal_for_execute(&role.password);
    let role_flags = match role.kind {
        DatabaseRoleKind::Replication => "LOGIN REPLICATION",
        DatabaseRoleKind::App | DatabaseRoleKind::Migration | DatabaseRoleKind::Support => "LOGIN",
    };

    let mut sql = format!(
        "\
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {role_literal}) THEN
    EXECUTE 'CREATE ROLE {role_ident} {role_flags} PASSWORD {password_literal}';
  ELSE
    EXECUTE 'ALTER ROLE {role_ident} WITH {role_flags} PASSWORD {password_literal}';
  END IF;
END
$$;
",
    );

    match role.kind {
        DatabaseRoleKind::App => {
            sql.push_str(&format!(
                "GRANT CONNECT ON DATABASE {database_ident} TO {role_ident};\n\
                 GRANT USAGE ON SCHEMA public TO {role_ident};\n\
                 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {role_ident};\n"
            ));
        }
        DatabaseRoleKind::Migration => {
            sql.push_str(&format!(
                "GRANT CONNECT, CREATE ON DATABASE {database_ident} TO {role_ident};\n\
                 GRANT CREATE, USAGE ON SCHEMA public TO {role_ident};\n"
            ));
        }
        DatabaseRoleKind::Replication => {
            sql.push_str(&format!(
                "GRANT CONNECT ON DATABASE {database_ident} TO {role_ident};\n"
            ));
        }
        DatabaseRoleKind::Support => {
            sql.push_str(&format!(
                "GRANT CONNECT ON DATABASE {database_ident} TO {role_ident};\n\
                 GRANT pg_monitor TO {role_ident};\n"
            ));
        }
    }
    sql
}

fn validate_identifier(value: &str) -> Result<(), NodeAgentError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(NodeAgentError::InvalidIdentifier(value.to_owned()));
    }
    Ok(())
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", escape_postgres_string(value))
}

fn quote_literal_for_execute(value: &str) -> String {
    quote_literal(value).replace('\'', "''")
}

fn escape_postgres_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn sanitize_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum NodeAgentError {
    #[error("unsupported postgres version '{0}'")]
    UnsupportedPostgresVersion(String),
    #[error("path is outside the node-agent runtime root: {0}")]
    PathOutsideRuntimeRoot(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid postgres identifier: {0}")]
    InvalidIdentifier(String),
    #[error("refusing to drop protected database: {0}")]
    ProtectedDatabase(String),
    #[error("invalid clone redaction policy: {0}")]
    InvalidRedactionPolicy(String),
    #[error("invalid postgres major upgrade: {0}")]
    InvalidMajorUpgrade(String),
    #[error("{action}: {source}")]
    Io {
        action: String,
        #[source]
        source: std::io::Error,
    },
    #[error("process failed: {program:?} {args:?} exited with {status}")]
    ProcessFailed {
        program: PathBuf,
        args: Vec<String>,
        status: String,
    },
    #[error("control-plane HTTP request failed: {0}")]
    Http(String),
    #[error("failed to encode or decode node-agent json: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn ensure_under_root(path: &Path, root: &Path) -> Result<(), NodeAgentError> {
    if path.starts_with(root) {
        Ok(())
    } else {
        Err(NodeAgentError::PathOutsideRuntimeRoot(
            path.display().to_string(),
        ))
    }
}

pub fn ensure_deletable_under_root(path: &Path, root: &Path) -> Result<(), NodeAgentError> {
    ensure_under_root(path, root)?;
    if path == root {
        return Err(NodeAgentError::InvalidPath(format!(
            "refusing to delete runtime root {}",
            root.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use palimpsest_paas_core::{
        ManagedPostgresMajorUpgradeStrategy, NodeAgentAction, NodeAgentCommand, PostgresVersion,
    };

    use super::*;

    #[test]
    fn prepare_postgres_plan_uses_pg18_and_data_dir() {
        let agent = NodeAgent::new(AgentConfig::local_dev());
        let command = NodeAgentCommand {
            command_id: "cmd_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::PreparePostgres {
                postgres_version: PostgresVersion::new("18").expect("valid version"),
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                port: 55_000,
            },
        };

        let plan = agent.plan(&command).expect("plan succeeds");

        assert_eq!(plan.steps.len(), 3);
        assert!(matches!(
            plan.steps[2],
            AgentStep::RenderPostgresConfig { port: 55_000, .. }
        ));
    }

    #[test]
    fn cluster_ids_are_sanitized_for_default_data_dir() {
        let agent = NodeAgent::new(AgentConfig::local_dev());
        let command = NodeAgentCommand {
            command_id: "cmd_123".to_owned(),
            cluster_id: "../cluster".to_owned(),
            action: NodeAgentAction::StartPostgres,
        };

        let plan = agent.plan(&command).expect("plan succeeds");
        assert!(matches!(
            &plan.steps[0],
            AgentStep::StartPostgres { data_dir, .. }
                if data_dir.ends_with("postgres/___cluster")
        ));
    }

    #[test]
    fn execute_prepare_creates_directory_renders_config_and_runs_initdb() {
        let root = unique_temp_root("prepare");
        let data_dir = root.join("postgres/cluster_123");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::PreparePostgres {
                postgres_version: PostgresVersion::new("18").expect("valid version"),
                data_dir: data_dir.display().to_string(),
                port: 55_000,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("execution succeeds");

        assert_eq!(report.outcomes.len(), 3);
        assert!(data_dir.exists());
        let rendered = fs::read_to_string(data_dir.join("postgresql.conf"))
            .expect("rendered postgres config should exist");
        assert!(rendered.contains("wal_level = logical"));
        assert!(rendered.contains("file_copy_method = clone"));
        assert!(rendered.contains("port = 55000"));
        let hba =
            fs::read_to_string(data_dir.join("pg_hba.conf")).expect("rendered hba should exist");
        assert!(hba.contains("host all all 172.16.0.0/12 scram-sha-256"));
        assert_eq!(
            runner.calls.borrow().as_slice(),
            &[(
                PathBuf::from("/pg18/bin/initdb"),
                vec!["-D".to_owned(), data_dir.display().to_string()]
            )]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_copy_on_write_clone_runs_file_copy_create_database() {
        let root = unique_temp_root("cow-clone");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_clone".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::CreateCopyOnWriteDatabaseClone {
                data_dir: data_dir.display().to_string(),
                port: 55_000,
                source_database: "source_db".to_owned(),
                target_database: "target_db".to_owned(),
                terminate_source_connections: true,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 2);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0].4.contains("SELECT pg_terminate_backend(pid)"));
        assert!(sql_calls[1]
            .4
            .contains("CREATE DATABASE \"target_db\" TEMPLATE \"source_db\" STRATEGY FILE_COPY"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_drop_database_terminates_then_drops_with_force() {
        let root = unique_temp_root("drop-db");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_drop".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::DropDatabase {
                data_dir: data_dir.display().to_string(),
                port: 55_000,
                database: "branch_db".to_owned(),
                terminate_connections: true,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 2);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0].4.contains("SELECT pg_terminate_backend(pid)"));
        assert!(sql_calls[1]
            .4
            .contains("DROP DATABASE IF EXISTS \"branch_db\" WITH (FORCE)"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_drop_database_refuses_protected_database() {
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: unique_temp_root("drop-db-guard"),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_drop_guard".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::DropDatabase {
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                port: 55_000,
                database: "postgres".to_owned(),
                terminate_connections: false,
            },
        };

        let err = agent.plan(&command).expect_err("plan must reject");
        assert!(matches!(err, NodeAgentError::ProtectedDatabase(db) if db == "postgres"));
    }

    #[test]
    fn execute_base_backup_creates_backup_dir_and_runs_pg_basebackup() {
        let root = unique_temp_root("backup");
        let data_dir = root.join("postgres/cluster_123");
        let backup_root = root.join("backups/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_backup".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::RunBaseBackup {
                backup_id: "backup_001".to_owned(),
                data_dir: data_dir.display().to_string(),
                postgres_url: "postgres://repl:secret@localhost:5432/app".to_owned(),
                backup_dir: backup_root.display().to_string(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("backup execution succeeds");

        assert_eq!(report.outcomes.len(), 3);
        let backup_dir = backup_root.join("backup_001");
        assert!(backup_dir.exists());
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(backup_dir.join("manifest.json")).expect("manifest exists"),
        )
        .expect("manifest is valid json");
        assert_eq!(manifest["manifest_version"], 1);
        assert_eq!(manifest["backup_id"], "backup_001");
        assert_eq!(manifest["cluster_id"], "cluster_123");
        assert_eq!(manifest["host_id"], "test-host");
        let backup_calls = runner.backup_calls.borrow();
        assert_eq!(backup_calls.len(), 1);
        assert_eq!(backup_calls[0].0, PathBuf::from("/pg18/bin/pg_basebackup"));
        assert_eq!(backup_calls[0].1, data_dir);
        assert_eq!(backup_calls[0].3, backup_dir);
        assert_eq!(backup_calls[0].4, "backup_001");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn configure_postgres_access_renders_roles_publication_and_slot() {
        let root = unique_temp_root("access");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_access".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::ConfigurePostgresAccess {
                data_dir: data_dir.display().to_string(),
                port: 55_000,
                database: "postgres".to_owned(),
                roles: vec![
                    DatabaseRoleCredential {
                        name: "cluster_123_app".to_owned(),
                        kind: DatabaseRoleKind::App,
                        password: "app-password".to_owned(),
                        privileges: Vec::new(),
                    },
                    DatabaseRoleCredential {
                        name: "cluster_123_replication".to_owned(),
                        kind: DatabaseRoleKind::Replication,
                        password: "repl-password".to_owned(),
                        privileges: Vec::new(),
                    },
                ],
                publication: "palimpsest_publication".to_owned(),
                replication_slot: "cluster_123_palimpsest_slot".to_owned(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("access configuration succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 2);
        assert_eq!(sql_calls[0].0, PathBuf::from("/pg18/bin/psql"));
        assert_eq!(sql_calls[0].1, data_dir);
        assert_eq!(sql_calls[0].2, 55_000);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0]
            .4
            .contains("CREATE ROLE \"cluster_123_app\" LOGIN"));
        assert!(sql_calls[0].4.contains("LOGIN REPLICATION"));
        assert!(sql_calls[0]
            .4
            .contains("CREATE PUBLICATION \"palimpsest_publication\""));
        assert!(sql_calls[1]
            .4
            .contains("pg_create_logical_replication_slot('cluster_123_palimpsest_slot'"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_prepare_restore_copies_backup_and_renders_recovery_files() {
        let root = unique_temp_root("restore");
        let backup_root = root.join("backups/cluster_123");
        let backup_dir = backup_root.join("backup_001");
        fs::create_dir_all(&backup_dir).expect("backup dir exists");
        fs::write(backup_dir.join("PG_VERSION"), b"18\n").expect("backup file exists");
        fs::write(
            backup_dir.join("manifest.json"),
            b"{\"backup_id\":\"backup_001\"}\n",
        )
        .expect("manifest exists");
        let data_dir = root.join("postgres/cluster_123-restored");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_restore".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::PrepareRestore {
                backup_id: "backup_001".to_owned(),
                backup_dir: backup_root.display().to_string(),
                data_dir: data_dir.display().to_string(),
                target_port: None,
                database: None,
                restore_command: "cp /archive/%f %p".to_owned(),
                recovery_target_lsn: Some("0/16B6C50".to_owned()),
                redaction_policy: None,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("restore execution succeeds");

        assert_eq!(report.outcomes.len(), 3);
        assert_eq!(
            fs::read(data_dir.join("PG_VERSION")).expect("restored backup file"),
            b"18\n"
        );
        assert!(!data_dir.join("manifest.json").exists());
        assert!(data_dir.join("recovery.signal").exists());
        let auto_conf =
            fs::read_to_string(data_dir.join("postgresql.auto.conf")).expect("auto conf exists");
        assert!(auto_conf.contains("restore_command = 'cp /archive/%f %p'"));
        assert!(auto_conf.contains("recovery_target_lsn = '0/16B6C50'"));
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_redacted_prepare_restore_applies_clone_redaction_sql() {
        let root = unique_temp_root("redacted-restore");
        let backup_root = root.join("backups/cluster_123");
        let backup_dir = backup_root.join("backup_001");
        fs::create_dir_all(&backup_dir).expect("backup dir exists");
        fs::write(backup_dir.join("PG_VERSION"), b"18\n").expect("backup file exists");
        let data_dir = root.join("postgres/cluster_123-redacted");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let policy = ManagedPostgresCloneRedactionPolicy {
            policy_id: "redact_prod_to_dev".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            name: "Production to development".to_owned(),
            status: palimpsest_paas_core::CloneRedactionPolicyStatus::Active,
            rules: vec![
                palimpsest_paas_core::CloneRedactionRule {
                    table_schema: "public".to_owned(),
                    table_name: "customers".to_owned(),
                    column_name: "email".to_owned(),
                    method: CloneRedactionMethod::HashSha256,
                    static_value: None,
                },
                palimpsest_paas_core::CloneRedactionRule {
                    table_schema: "public".to_owned(),
                    table_name: "customers".to_owned(),
                    column_name: "phone".to_owned(),
                    method: CloneRedactionMethod::Null,
                    static_value: None,
                },
            ],
            created_at: "2026-05-18T00:00:00Z".to_owned(),
            updated_at: "2026-05-18T00:00:00Z".to_owned(),
        };
        let command = NodeAgentCommand {
            command_id: "cmd_restore".to_owned(),
            cluster_id: "cluster_123_redacted".to_owned(),
            action: NodeAgentAction::PrepareRestore {
                backup_id: "backup_001".to_owned(),
                backup_dir: backup_root.display().to_string(),
                data_dir: data_dir.display().to_string(),
                target_port: Some(56_000),
                database: Some("postgres".to_owned()),
                restore_command: "cp /archive/%f %p".to_owned(),
                recovery_target_lsn: None,
                redaction_policy: Some(policy),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("redacted restore execution succeeds");

        assert_eq!(report.outcomes.len(), 9);
        let redaction_sql = fs::read_to_string(data_dir.join("palimpsest-clone-redaction.sql"))
            .expect("redaction SQL exists");
        assert!(redaction_sql.contains("encode(sha256(convert_to"));
        assert!(redaction_sql.contains("UPDATE \"public\".\"customers\" SET \"phone\" = NULL"));
        assert!(
            fs::read_to_string(data_dir.join("palimpsest-clone-redaction-applied.json"))
                .expect("applied marker exists")
                .contains("\"policy_id\": \"redact_prod_to_dev\"")
        );
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 1);
        assert_eq!(sql_calls[0].2, 56_000);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0].4.contains("COMMIT;"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_prepare_postgres_standby_restores_backup_and_renders_standby_files() {
        let root = unique_temp_root("standby");
        let backup_root = root.join("backups/cluster_123");
        let backup_dir = backup_root.join("backup_001");
        fs::create_dir_all(&backup_dir).expect("backup dir exists");
        fs::write(backup_dir.join("PG_VERSION"), b"18\n").expect("backup file exists");
        let data_dir = root.join("postgres/cluster_123_standby");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_standby".to_owned(),
            cluster_id: "cluster_123_standby".to_owned(),
            action: NodeAgentAction::PreparePostgresStandby {
                backup_id: "backup_001".to_owned(),
                backup_dir: backup_root.display().to_string(),
                data_dir: data_dir.display().to_string(),
                target_port: Some(58_000),
                primary_conninfo: "host=127.0.0.1 port=55000 user=cluster_123_replication"
                    .to_owned(),
                primary_slot_name: "cluster_123_standby_slot".to_owned(),
                source_data_dir: root.join("postgres/cluster_123").display().to_string(),
                source_port: 55000,
                database: "postgres".to_owned(),
                restore_command: "cp /archive/%f %p".to_owned(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("standby execution succeeds");

        assert_eq!(report.outcomes.len(), 7);
        assert_eq!(
            fs::read(data_dir.join("PG_VERSION")).expect("restored backup file"),
            b"18\n"
        );
        assert!(data_dir.join("standby.signal").exists());
        let auto_conf =
            fs::read_to_string(data_dir.join("postgresql.auto.conf")).expect("auto conf exists");
        assert!(auto_conf.contains(
            "primary_conninfo = 'host=127.0.0.1 port=55000 user=cluster_123_replication'"
        ));
        assert!(auto_conf.contains("primary_slot_name = 'cluster_123_standby_slot'"));
        assert!(auto_conf.contains("restore_command = 'cp /archive/%f %p'"));
        assert!(auto_conf.contains("hot_standby = on"));
        let postgres_conf =
            fs::read_to_string(data_dir.join("postgresql.conf")).expect("postgres config exists");
        assert!(postgres_conf.contains("port = 58000"));
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 1);
        assert_eq!(sql_calls[0].2, 55000);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0]
            .4
            .contains("pg_create_physical_replication_slot('cluster_123_standby_slot', true)"));
        let calls = runner.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, PathBuf::from("/pg18/bin/pg_ctl"));
        assert!(calls[0].1.contains(&"start".to_owned()));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_check_postgres_standby_lag_runs_threshold_sql() {
        let root = unique_temp_root("standby-lag");
        let source_data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&source_data_dir).expect("source dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_check_standby_lag".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::CheckPostgresStandbyLag {
                source_data_dir: source_data_dir.display().to_string(),
                source_port: 55000,
                database: "postgres".to_owned(),
                slot_name: "cluster_123_standby_slot".to_owned(),
                max_lag_bytes: 16_777_216,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("standby lag check succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 1);
        assert_eq!(sql_calls[0].2, 55000);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0].4.contains("pg_replication_slots"));
        assert!(sql_calls[0].4.contains("cluster_123_standby_slot"));
        assert!(sql_calls[0].4.contains("16777216"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_archive_wal_segment_copies_file_inside_runtime_root() {
        let root = unique_temp_root("wal");
        let source_dir = root.join("postgres/cluster_123/pg_wal");
        fs::create_dir_all(&source_dir).expect("wal source dir exists");
        let source_path = source_dir.join("000000010000000000000001");
        fs::write(&source_path, b"wal").expect("wal segment exists");
        let archive_dir = root.join("wal/cluster_123");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_archive_wal".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::ArchiveWalSegment {
                source_path: source_path.display().to_string(),
                archive_dir: archive_dir.display().to_string(),
                segment_name: "000000010000000000000001".to_owned(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("archive execution succeeds");

        assert_eq!(report.outcomes.len(), 2);
        assert_eq!(
            fs::read(archive_dir.join("000000010000000000000001")).expect("archived segment"),
            b"wal"
        );
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_promote_postgres_standby_records_promotion_intent() {
        let root = unique_temp_root("promote");
        let data_dir = root.join("postgres/cluster_123_standby");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        fs::write(data_dir.join("recovery.signal"), b"").expect("recovery signal exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_promote".to_owned(),
            cluster_id: "cluster_123_standby".to_owned(),
            action: NodeAgentAction::PromotePostgresStandby {
                data_dir: data_dir.display().to_string(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("promotion execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        assert_eq!(report.outcomes[0].status, AgentStepStatus::Succeeded);
        assert!(!data_dir.join("recovery.signal").exists());
        assert_eq!(
            fs::read_to_string(data_dir.join("promotion.intent")).expect("promotion marker exists"),
            "promoted\n"
        );
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_fence_postgres_primary_records_fence_intent() {
        let root = unique_temp_root("fence");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_fence".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::FencePostgresPrimary {
                data_dir: data_dir.display().to_string(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("fence execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        assert_eq!(report.outcomes[0].status, AgentStepStatus::Succeeded);
        let marker =
            fs::read_to_string(data_dir.join("fence.intent")).expect("fence marker exists");
        assert!(marker.contains("\"fenced\":true"));
        assert!(marker.contains("\"method\":\"local_stop_then_marker\""));
        let calls = runner.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, PathBuf::from("/pg18/bin/pg_ctl"));
        assert!(calls[0].1.contains(&"immediate".to_owned()));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_delete_postgres_data_removes_cluster_directory() {
        let root = unique_temp_root("delete");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        fs::write(data_dir.join("PG_VERSION"), b"18\n").expect("postgres file exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_delete".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::DeletePostgresData {
                data_dir: data_dir.display().to_string(),
                tombstone_retention_days: None,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("delete execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        assert!(!data_dir.exists());
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_delete_backup_data_removes_backup_directory() {
        let root = unique_temp_root("delete-backup");
        let backup_dir = root.join("backups/cluster_123/backup_123");
        fs::create_dir_all(&backup_dir).expect("backup dir exists");
        fs::write(backup_dir.join("backup_manifest.json"), "{}").expect("backup file exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_delete_backup".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::DeleteBackupData {
                backup_id: "backup_123".to_owned(),
                backup_dir: backup_dir.display().to_string(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("delete backup execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        assert!(!backup_dir.exists());
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_resize_postgres_storage_records_quota_intent() {
        let root = unique_temp_root("resize-storage");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_resize".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::ResizePostgresStorage {
                data_dir: data_dir.display().to_string(),
                storage_gib: 32,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("resize execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let quota: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(data_dir.join("palimpsest-storage-quota.json"))
                .expect("quota file exists"),
        )
        .expect("quota json parses");
        assert_eq!(quota["storage_gib"], 32);
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_update_postgres_minor_records_version_intent() {
        let root = unique_temp_root("update-postgres-minor");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_update_minor".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::UpdatePostgresMinor {
                data_dir: data_dir.display().to_string(),
                target_postgres_version: PostgresVersion::new("18.4").expect("valid version"),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("minor update execution succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let version: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(data_dir.join("palimpsest-postgres-version.json"))
                .expect("version file exists"),
        )
        .expect("version json parses");
        assert_eq!(version["target_postgres_version"], "18.4");
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execute_upgrade_postgres_major_runs_preflight_and_records_plan() {
        let root = unique_temp_root("upgrade-postgres-major");
        let data_dir = root.join("postgres/cluster_123");
        fs::create_dir_all(&data_dir).expect("data dir exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_upgrade_major".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::UpgradePostgresMajor {
                data_dir: data_dir.display().to_string(),
                source_port: Some(55_000),
                database: Some("postgres".to_owned()),
                source_postgres_version: PostgresVersion::new("18.5").expect("valid version"),
                target_postgres_version: PostgresVersion::new("19").expect("valid version"),
                strategy: ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy,
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("major upgrade execution succeeds");

        assert_eq!(report.outcomes.len(), 3);
        let plan: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(data_dir.join("palimpsest-postgres-major-upgrade-plan.json"))
                .expect("upgrade plan file exists"),
        )
        .expect("upgrade plan json parses");
        assert_eq!(plan["source_postgres_version"], "18.5");
        assert_eq!(plan["target_postgres_version"], "19");
        assert_eq!(plan["strategy"], "logical_replication_copy");
        assert_eq!(plan["preflight_required"], true);
        let preflight: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(data_dir.join("palimpsest-postgres-major-upgrade-preflight.json"))
                .expect("upgrade preflight file exists"),
        )
        .expect("upgrade preflight json parses");
        assert_eq!(preflight["status"], "succeeded");
        let sql_calls = runner.sql_calls.borrow();
        assert_eq!(sql_calls.len(), 1);
        assert_eq!(sql_calls[0].0, PathBuf::from("/pg18/bin/psql"));
        assert_eq!(sql_calls[0].1, data_dir);
        assert_eq!(sql_calls[0].2, 55_000);
        assert_eq!(sql_calls[0].3, "postgres");
        assert!(sql_calls[0]
            .4
            .contains("palimpsest_major_upgrade_preflight"));
        assert!(sql_calls[0].4.contains("current_setting('wal_level')"));
        let upgrade: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(data_dir.join("palimpsest-postgres-major-upgrade.json"))
                .expect("upgrade file exists"),
        )
        .expect("upgrade json parses");
        assert_eq!(upgrade["source_postgres_version"], "18.5");
        assert_eq!(upgrade["target_postgres_version"], "19");
        assert_eq!(upgrade["strategy"], "logical_replication_copy");
        assert!(runner.calls.borrow().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_postgres_data_refuses_runtime_root() {
        let root = unique_temp_root("delete-root");
        fs::create_dir_all(&root).expect("root exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_delete".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::DeletePostgresData {
                data_dir: root.display().to_string(),
                tombstone_retention_days: None,
            },
        };
        let runner = RecordingRunner::default();

        let err = agent
            .execute_with_runner(&command, &runner)
            .expect_err("runtime root delete should fail");

        assert!(matches!(err, NodeAgentError::InvalidPath(_)));
        assert!(root.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execution_rejects_paths_outside_runtime_root() {
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: PathBuf::from("/tmp/palimpsest-agent-test-root"),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::PreparePostgres {
                postgres_version: PostgresVersion::new("18").expect("valid version"),
                data_dir: "/tmp/not-owned-by-agent".to_owned(),
                port: 55_000,
            },
        };
        let runner = RecordingRunner::default();

        let err = agent
            .execute_with_runner(&command, &runner)
            .expect_err("outside root should fail");

        assert!(matches!(err, NodeAgentError::PathOutsideRuntimeRoot(_)));
    }

    #[test]
    fn control_plane_endpoint_defaults_port_and_joins_paths() {
        let endpoint = HttpEndpoint::parse("http://127.0.0.1/api").expect("endpoint parses");

        assert_eq!(endpoint.scheme, HttpScheme::Http);
        assert_eq!(endpoint.socket_addr, "127.0.0.1:80");
        assert_eq!(
            endpoint.path("/v1/node-hosts/host_1/commands/lease"),
            "/api/v1/node-hosts/host_1/commands/lease"
        );
    }

    #[test]
    fn https_object_store_endpoint_requires_ca_configuration() {
        let endpoint =
            HttpEndpoint::parse("https://object-store.internal/minio").expect("endpoint parses");

        assert_eq!(endpoint.scheme, HttpScheme::Https);
        assert_eq!(endpoint.socket_addr, "object-store.internal:443");
        assert!(
            validate_backup_object_store_endpoint("https://object-store.internal/minio", None)
                .is_err()
        );
        validate_backup_object_store_endpoint(
            "https://object-store.internal/minio",
            Some(&BackupObjectStoreTls {
                ca_cert_file: PathBuf::from("/etc/palimpsest/object-store-ca.pem"),
                server_name: Some("object-store.internal".to_owned()),
            }),
        )
        .expect("CA-backed https object store endpoint is accepted");
    }

    #[test]
    fn control_plane_client_adds_bearer_token_when_configured() {
        let client = HttpControlPlaneClient::new(ControlPlaneClientConfig {
            base_url: "http://127.0.0.1:8088".to_owned(),
            host_id: "host-a".to_owned(),
            bearer_token: Some("agent-token".to_owned()),
            signing_key_id: None,
            signing_key: None,
        })
        .expect("client builds");

        let request = client.render_post_json_request(
            "/v1/node-hosts/host-a/commands/lease",
            "null",
            AgentAuthScope {
                host_id: "host-a",
                operation: "lease",
            },
        );

        assert!(request.contains("Authorization: Bearer agent-token\r\n"));
        assert!(request.contains("Content-Length: 4\r\n"));
    }

    #[test]
    fn control_plane_client_adds_signed_agent_headers_when_configured() {
        let client = HttpControlPlaneClient::new(ControlPlaneClientConfig {
            base_url: "http://127.0.0.1:8088".to_owned(),
            host_id: "host-a".to_owned(),
            bearer_token: None,
            signing_key_id: Some("host-a:agent-signing:1".to_owned()),
            signing_key: Some(vec![7; 32]),
        })
        .expect("client builds");

        let request = client.render_post_json_request_at(
            "/v1/node-hosts/host-a/commands/lease",
            "null",
            AgentAuthScope {
                host_id: "host-a",
                operation: "lease",
            },
            1_800_000_000,
        );

        assert!(request.contains("X-Palimpsest-Agent-Host-Id: host-a\r\n"));
        assert!(request.contains("X-Palimpsest-Agent-Key-Id: host-a:agent-signing:1\r\n"));
        assert!(request.contains("X-Palimpsest-Agent-Operation: lease\r\n"));
        assert!(request.contains("X-Palimpsest-Agent-Timestamp: 1800000000\r\n"));
        assert!(request.contains("X-Palimpsest-Agent-Signature: "));
    }

    #[test]
    fn parse_http_response_rejects_non_success_status() {
        let err = parse_http_response(
            "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 4\r\n\r\nfail",
        )
        .expect_err("non-success should fail");

        assert!(matches!(err, NodeAgentError::Http(_)));
    }

    #[test]
    fn execution_report_becomes_successful_command_result() {
        let report = ExecutionReport {
            command_id: "cmd_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            outcomes: vec![AgentStepOutcome {
                step: AgentStep::ProbeStatus {
                    data_dir: PathBuf::from("/var/lib/palimpsest/postgres/cluster_123"),
                },
                status: AgentStepStatus::Reported,
                detail: "postmaster.pid absent".to_owned(),
            }],
        };

        let result = result_from_execution_report("host_123", &report);

        assert_eq!(result.command_id, "cmd_123");
        assert_eq!(result.host_id, "host_123");
        assert_eq!(result.status, AgentCommandStatus::Succeeded);
    }

    #[test]
    fn execution_report_includes_backup_artifact_metadata() {
        let root = PathBuf::from(format!(
            "/tmp/palimpsest-paas-node-agent-artifact-test-{}",
            std::process::id()
        ));
        let backup_dir = root.join("backups/cluster_123/backup_123");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&backup_dir).expect("backup dir exists");
        fs::write(backup_dir.join("PG_VERSION"), b"18\n").expect("backup file exists");
        fs::write(
            backup_dir.join("manifest.json"),
            b"{\"backup_id\":\"backup_123\"}\n",
        )
        .expect("manifest exists");
        fs::create_dir_all(backup_dir.join("base")).expect("nested dir exists");
        fs::write(backup_dir.join("base/data"), b"payload").expect("nested backup file exists");

        let report = ExecutionReport {
            command_id: "cmd_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            outcomes: vec![AgentStepOutcome {
                step: AgentStep::WriteBackupManifest {
                    cluster_id: "cluster_123".to_owned(),
                    data_dir: root.join("postgres/cluster_123"),
                    backup_dir: backup_dir.clone(),
                    backup_id: "backup_123".to_owned(),
                },
                status: AgentStepStatus::Succeeded,
                detail: "wrote backup manifest".to_owned(),
            }],
        };

        let result = result_from_execution_report("host_123", &report);

        assert_eq!(result.backup_artifacts.len(), 1);
        let artifact = &result.backup_artifacts[0];
        assert_eq!(artifact.backup_id, "backup_123");
        assert_eq!(artifact.provider, "local_fs");
        assert_eq!(
            artifact.object_uri,
            format!("file://{}", backup_dir.display())
        );
        assert_eq!(
            artifact.manifest_path,
            backup_dir.join("manifest.json").display().to_string()
        );
        assert!(artifact
            .manifest_sha256
            .as_deref()
            .is_some_and(|hash| hash.starts_with("sha256:") && hash.len() == 71));
        assert!(artifact.size_bytes.is_some_and(|bytes| bytes >= 7));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn upload_backup_artifact_copies_manifest_and_reports_object_metadata() {
        let root = unique_temp_root("object-backup");
        let backup_dir = root.join("backups/cluster_123/backup_123");
        let object_store_dir = root.join("object-store");
        fs::create_dir_all(backup_dir.join("base")).expect("backup dir exists");
        fs::write(
            backup_dir.join("manifest.json"),
            b"{\"backup_id\":\"backup_123\"}\n",
        )
        .expect("manifest exists");
        fs::write(backup_dir.join("base/data"), b"payload").expect("payload exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let plan = CommandPlan {
            command_id: "cmd_upload".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            steps: vec![AgentStep::UploadBackupArtifact {
                cluster_id: "cluster_123".to_owned(),
                backup_id: "backup_123".to_owned(),
                source_dir: backup_dir,
                object_store_dir: Some(object_store_dir.clone()),
                endpoint: None,
                auth: None,
                tls: None,
                provider: "s3_compatible_fs".to_owned(),
                bucket: "palimpsest-managed-postgres-backups".to_owned(),
                object_key: backup_object_key("cluster_123", "backup_123"),
            }],
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_plan_with_runner(&plan, &runner)
            .expect("upload succeeds");
        let destination = object_store_dir
            .join("palimpsest-managed-postgres-backups")
            .join("managed-postgres/cluster_123/base-backups/backup_123");
        assert_eq!(
            fs::read(destination.join("manifest.json")).expect("object manifest exists"),
            b"{\"backup_id\":\"backup_123\"}\n"
        );
        assert_eq!(
            fs::read(destination.join("base/data")).expect("object data exists"),
            b"payload"
        );

        let result = result_from_execution_report("host_123", &report);
        assert_eq!(result.backup_artifacts.len(), 1);
        let artifact = &result.backup_artifacts[0];
        assert_eq!(artifact.provider, "s3_compatible_fs");
        assert_eq!(
            artifact.object_uri,
            "s3://palimpsest-managed-postgres-backups/managed-postgres/cluster_123/base-backups/backup_123"
        );
        assert_eq!(
            artifact.manifest_path,
            "s3://palimpsest-managed-postgres-backups/managed-postgres/cluster_123/base-backups/backup_123/manifest.json"
        );
        assert!(artifact
            .manifest_sha256
            .as_deref()
            .is_some_and(|hash| hash.starts_with("sha256:") && hash.len() == 71));
        assert!(artifact.size_bytes.is_some_and(|bytes| bytes >= 7));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn http_object_store_upload_builds_path_style_put_requests() {
        let endpoint = HttpEndpoint::parse("http://127.0.0.1:9000/minio").expect("endpoint");
        let object_path = http_object_store_path(
            "palimpsest-managed-postgres-backups",
            &backup_object_key("cluster_123", "backup_123"),
            Path::new("base/data"),
        )
        .expect("object path");
        let request_path = endpoint.path(&object_path);
        let auth = BackupObjectStoreAuth::Header {
            name: "authorization".to_owned(),
            value: "Bearer object-token".to_owned(),
        };
        let request = http_put_request_bytes(&endpoint, &request_path, Some(&auth), b"payload");
        let request = String::from_utf8(request).expect("request utf8");

        assert!(request.starts_with(
            "PUT /minio/palimpsest-managed-postgres-backups/managed-postgres/cluster_123/base-backups/backup_123/base/data HTTP/1.1\r\n"
        ));
        assert!(request.contains("host: 127.0.0.1:9000\r\n"));
        assert!(request.contains("authorization: Bearer object-token\r\n"));
        assert!(request.contains("content-length: 7\r\n"));
        assert!(request.ends_with("\r\n\r\npayload"));
    }

    #[test]
    fn http_object_store_upload_can_sign_s3_compatible_put_requests() {
        let endpoint = HttpEndpoint::parse("https://s3.internal:9443/minio").expect("endpoint");
        let request_path = endpoint.path(
            "/palimpsest-managed-postgres-backups/managed-postgres/cluster_123/base-backups/backup_123/base/data",
        );
        let auth = BackupObjectStoreAuth::AwsSigV4 {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            region: "us-east-1".to_owned(),
            service: "s3".to_owned(),
        };
        let request = http_put_request_bytes_at(
            &endpoint,
            &request_path,
            Some(&auth),
            b"payload",
            1_369_353_600,
        );
        let request = String::from_utf8(request).expect("request utf8");

        assert!(request.contains("x-amz-date: 20130524T000000Z\r\n"));
        assert!(request.contains(
            "x-amz-content-sha256: 239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5\r\n"
        ));
        assert!(request.contains(
            "Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="
        ));
    }

    #[test]
    fn local_host_description_uses_agent_host_id() {
        let agent = NodeAgent::new(AgentConfig {
            host_id: "host_123".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: PathBuf::from("/var/lib/palimpsest"),
        });

        let host = agent.local_host_description();
        let heartbeat = agent.heartbeat();

        assert_eq!(host.host_id, "host_123");
        assert_eq!(host.region, "local");
        assert_eq!(host.data_root, "/var/lib/palimpsest/postgres");
        assert_eq!(host.first_port, 55_000);
        assert_eq!(host.state, NodeHostState::Active);
        assert_eq!(heartbeat.capacity.max_clusters, host.capacity.max_clusters);
    }

    #[test]
    fn hardening_check_uses_host_identity_and_supported_postgres_floor() {
        let agent = NodeAgent::new(AgentConfig {
            host_id: "host_123".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: PathBuf::from("/var/lib/palimpsest"),
        });

        let check = agent.hardening_check();

        assert!(check.check_id.starts_with("hardening-host_123-"));
        assert_eq!(check.host_id, "host_123");
        assert_eq!(check.postgres_major_min, MIN_SUPPORTED_POSTGRES_MAJOR);
        assert!(!check.image_ref.trim().is_empty());
        assert!(!check.os_release.trim().is_empty());
        assert!(!check.kernel_version.trim().is_empty());
        assert!(!check.container_runtime.trim().is_empty());
        assert!(check.checked_at.ends_with('Z'));
    }

    #[test]
    fn unix_timestamp_formatter_renders_rfc3339_utc() {
        assert_eq!(rfc3339_from_unix_seconds(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339_from_unix_seconds(86_400 + 3_723),
            "1970-01-02T01:02:03Z"
        );
    }

    #[test]
    fn agent_config_can_be_overridden_from_environment() {
        std::env::set_var("PALIMPSEST_PAAS_AGENT_HOST_ID", "host_env");
        std::env::set_var("PALIMPSEST_PAAS_POSTGRES_BIN_DIR", "/pg/bin");
        std::env::set_var("PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT", "/tmp/paas-agent");

        let config = AgentConfig::from_env();

        assert_eq!(config.host_id, "host_env");
        assert_eq!(config.postgres_bin_dir, PathBuf::from("/pg/bin"));
        assert_eq!(config.runtime_root, PathBuf::from("/tmp/paas-agent"));

        std::env::remove_var("PALIMPSEST_PAAS_AGENT_HOST_ID");
        std::env::remove_var("PALIMPSEST_PAAS_POSTGRES_BIN_DIR");
        std::env::remove_var("PALIMPSEST_PAAS_AGENT_RUNTIME_ROOT");
    }

    #[test]
    fn command_plan_becomes_successful_dry_run_result() {
        let plan = CommandPlan {
            command_id: "cmd_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            steps: vec![AgentStep::ProbeStatus {
                data_dir: PathBuf::from("/var/lib/palimpsest/postgres/cluster_123"),
            }],
        };

        let result = result_from_command_plan("host_123", &plan);

        assert_eq!(result.command_id, "cmd_123");
        assert_eq!(result.host_id, "host_123");
        assert_eq!(result.status, AgentCommandStatus::Succeeded);
        assert_eq!(result.detail.as_deref(), Some("dry-run planned 1 step(s)"));
    }

    #[test]
    fn docker_postgres_runner_builds_docker_invocation() {
        let runner = DockerPostgresRunner::postgres18("/tmp/palimpsest-agent-test-root");
        let args = runner
            .docker_args(
                Path::new("/usr/local/pgsql-18/bin/initdb"),
                &[
                    "-D".to_owned(),
                    "/tmp/palimpsest-agent-test-root/postgres/c1".to_owned(),
                ],
            )
            .expect("docker args build");

        assert_eq!(args[0], "run");
        assert!(args.contains(&"postgres:18".to_owned()));
        assert!(args.contains(&"initdb".to_owned()));
        assert!(args.contains(
            &"/tmp/palimpsest-agent-test-root:/tmp/palimpsest-agent-test-root".to_owned()
        ));
    }

    #[test]
    fn docker_postgres_runner_names_cluster_container_from_data_dir() {
        let name = DockerPostgresRunner::container_name_for_data_dir(Path::new(
            "/tmp/palimpsest-agent-test-root/postgres/cluster_123",
        ))
        .expect("container name builds");

        assert_eq!(name, "palimpsest-pg-cluster_123");
    }

    #[test]
    fn docker_postgres_runner_builds_detached_start_invocation() {
        let runner = DockerPostgresRunner::postgres18("/tmp/palimpsest-agent-test-root");
        let data_dir = Path::new("/tmp/palimpsest-agent-test-root/postgres/cluster_123");
        let args = runner
            .docker_start_args(data_dir, Some(55_000))
            .expect("docker start args build");

        assert_eq!(args[0], "run");
        assert!(args.contains(&"-d".to_owned()));
        assert!(args.contains(&"--name".to_owned()));
        assert!(args.contains(&"palimpsest-pg-cluster_123".to_owned()));
        assert!(args.contains(&"--restart".to_owned()));
        assert!(args.contains(&"unless-stopped".to_owned()));
        assert!(args.contains(&"127.0.0.1:55000:55000".to_owned()));
        assert!(args.contains(&"postgres:18".to_owned()));
        assert!(args.contains(&data_dir.display().to_string()));
        assert!(args
            .iter()
            .any(|arg| arg.contains("/usr/lib/postgresql/18/bin/postgres")));
    }

    #[test]
    fn docker_postgres_runner_builds_force_remove_stop_invocation() {
        let data_dir = Path::new("/tmp/palimpsest-agent-test-root/postgres/cluster_123");
        let args =
            DockerPostgresRunner::docker_stop_args(data_dir).expect("docker stop args build");

        assert_eq!(
            args,
            vec![
                "rm".to_owned(),
                "-f".to_owned(),
                "palimpsest-pg-cluster_123".to_owned()
            ]
        );
    }

    #[test]
    fn docker_postgres_runner_detects_pg_ctl_modes_and_data_dir() {
        let start_args = vec![
            "-D".to_owned(),
            "/tmp/runtime/postgres/c1".to_owned(),
            "-l".to_owned(),
            "/tmp/runtime/postgres/c1/postgresql.log".to_owned(),
            "start".to_owned(),
        ];
        let stop_args = vec![
            "-D".to_owned(),
            "/tmp/runtime/postgres/c1".to_owned(),
            "stop".to_owned(),
            "-m".to_owned(),
            "fast".to_owned(),
        ];

        assert_eq!(
            DockerPostgresRunner::pg_ctl_mode(&start_args),
            Some(PgCtlMode::Start)
        );
        assert_eq!(
            DockerPostgresRunner::pg_ctl_mode(&stop_args),
            Some(PgCtlMode::Stop)
        );
        assert_eq!(
            DockerPostgresRunner::data_dir_arg(&start_args).expect("data dir"),
            PathBuf::from("/tmp/runtime/postgres/c1")
        );
    }

    #[derive(Default)]
    struct RecordingRunner {
        calls: RefCell<Vec<(PathBuf, Vec<String>)>>,
        sql_calls: RefCell<Vec<(PathBuf, PathBuf, u16, String, String)>>,
        backup_calls: RefCell<Vec<(PathBuf, PathBuf, String, PathBuf, String)>>,
    }

    impl ProcessRunner for RecordingRunner {
        fn run(&self, program: &Path, args: &[String]) -> Result<(), NodeAgentError> {
            self.calls
                .borrow_mut()
                .push((program.to_path_buf(), args.to_vec()));
            Ok(())
        }

        fn run_sql(
            &self,
            program: &Path,
            data_dir: &Path,
            port: u16,
            database: &str,
            sql: &str,
        ) -> Result<(), NodeAgentError> {
            self.sql_calls.borrow_mut().push((
                program.to_path_buf(),
                data_dir.to_path_buf(),
                port,
                database.to_owned(),
                sql.to_owned(),
            ));
            Ok(())
        }

        fn run_base_backup(
            &self,
            program: &Path,
            data_dir: &Path,
            postgres_url: &str,
            backup_dir: &Path,
            backup_id: &str,
        ) -> Result<(), NodeAgentError> {
            self.backup_calls.borrow_mut().push((
                program.to_path_buf(),
                data_dir.to_path_buf(),
                postgres_url.to_owned(),
                backup_dir.to_path_buf(),
                backup_id.to_owned(),
            ));
            Ok(())
        }
    }

    #[test]
    fn heartbeat_reports_observed_postgres_and_sync_resources() {
        let root = unique_temp_root("heartbeat-observed");
        let _ = fs::remove_dir_all(&root);
        let postgres_dir = root.join("postgres").join("cluster_123");
        let sync_dir = root.join("sync").join("sync_123");
        fs::create_dir_all(&postgres_dir).expect("postgres dir exists");
        fs::create_dir_all(&sync_dir).expect("sync dir exists");
        fs::write(postgres_dir.join("postmaster.pid"), "123\n").expect("pid marker writes");
        fs::write(sync_dir.join("running.json"), "{}").expect("sync marker writes");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });

        let heartbeat = agent.heartbeat();

        assert_eq!(heartbeat.observed_clusters.len(), 1);
        assert_eq!(heartbeat.observed_clusters[0].cluster_id, "cluster_123");
        assert!(heartbeat.observed_clusters[0].postgres_running);
        assert_eq!(heartbeat.observed_sync_deployments.len(), 1);
        assert_eq!(
            heartbeat.observed_sync_deployments[0].deployment_id,
            "sync_123"
        );
        assert!(heartbeat.observed_sync_deployments[0].running);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn start_sync_deployment_records_running_marker() {
        let root = unique_temp_root("start-sync");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root exists");
        let agent = NodeAgent::new(AgentConfig {
            host_id: "test-host".to_owned(),
            postgres_bin_dir: PathBuf::from("/pg18/bin"),
            runtime_root: root.clone(),
        });
        let command = NodeAgentCommand {
            command_id: "cmd_sync_start".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            action: NodeAgentAction::StartSyncDeployment {
                deployment_id: "sync_123".to_owned(),
                config_version: "config_001".to_owned(),
            },
        };
        let runner = RecordingRunner::default();

        let report = agent
            .execute_with_runner(&command, &runner)
            .expect("sync start succeeds");

        assert_eq!(report.outcomes.len(), 1);
        let marker = root.join("sync").join("sync_123").join("running.json");
        let marker_json: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(marker).expect("marker exists"))
                .expect("marker parses");
        assert_eq!(marker_json["deployment_id"], "sync_123");
        assert_eq!(marker_json["config_version"], "config_001");

        let _ = fs::remove_dir_all(root);
    }

    fn unique_temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "palimpsest-node-agent-{label}-{}",
            std::process::id()
        ))
    }
}
