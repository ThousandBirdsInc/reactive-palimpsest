// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Shared models for the Palimpsest managed PaaS.
//!
//! This crate intentionally has no runtime dependencies on the existing
//! Palimpsest server. It captures platform intent and agent command contracts
//! so the PaaS can be built additively under `paas/`.

use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MIN_SUPPORTED_POSTGRES_MAJOR: u16 = 18;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PostgresVersion {
    major: u16,
    original: String,
}

impl PostgresVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let original = value.into();
        let major = parse_major(&original)?;
        if major < MIN_SUPPORTED_POSTGRES_MAJOR {
            return Err(ModelError::UnsupportedPostgresVersion {
                version: original,
                minimum_major: MIN_SUPPORTED_POSTGRES_MAJOR,
            });
        }
        Ok(Self { major, original })
    }

    pub const fn major(&self) -> u16 {
        self.major
    }

    pub fn as_str(&self) -> &str {
        &self.original
    }
}

impl fmt::Display for PostgresVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.original)
    }
}

impl From<PostgresVersion> for String {
    fn from(value: PostgresVersion) -> Self {
        value.original
    }
}

impl FromStr for PostgresVersion {
    type Err = ModelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for PostgresVersion {
    type Error = ModelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

fn parse_major(value: &str) -> Result<u16, ModelError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ModelError::InvalidPostgresVersion(value.to_owned()));
    }
    let major = trimmed
        .split(['.', '-'])
        .next()
        .ok_or_else(|| ModelError::InvalidPostgresVersion(value.to_owned()))?;
    major
        .parse()
        .map_err(|_| ModelError::InvalidPostgresVersion(value.to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseMode {
    Managed,
    Local,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentSpec {
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub region: String,
    pub database: DatabaseSpec,
    pub sync: SyncDeploymentSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Organization {
    pub organization_id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub project_id: String,
    pub organization_id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Environment {
    pub environment_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub name: String,
    pub region: String,
}

impl EnvironmentSpec {
    pub fn validate(&self) -> Result<(), ModelError> {
        self.database.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseSpec {
    pub mode: DatabaseMode,
    pub postgres_version: Option<PostgresVersion>,
    pub managed: Option<ManagedPostgresSpec>,
}

impl DatabaseSpec {
    pub fn validate(&self) -> Result<(), ModelError> {
        match self.mode {
            DatabaseMode::Managed | DatabaseMode::Local => {
                if self.postgres_version.is_none() {
                    return Err(ModelError::MissingPostgresVersion {
                        mode: self.mode.clone(),
                    });
                }
            }
            DatabaseMode::External => {}
        }

        if matches!(self.mode, DatabaseMode::Managed) && self.managed.is_none() {
            return Err(ModelError::MissingManagedPostgresSpec);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresSpec {
    pub cluster_id: String,
    pub tier: String,
    pub storage_gib: u32,
    pub backup_policy: BackupPolicy,
    pub maintenance_policy: MaintenancePolicy,
    #[serde(default)]
    pub roles: Vec<DatabaseRoleSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupPolicy {
    pub pitr_window_hours: u16,
    pub base_backup_interval_hours: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenancePolicy {
    pub window: String,
    pub auto_minor_upgrades: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncDeploymentSpec {
    pub deployment_id: String,
    pub config_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigVersion {
    pub config_version: String,
    pub environment_id: String,
    pub rendered_hash: String,
    pub status: ConfigVersionStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncDeployment {
    pub deployment_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub managed_postgres_cluster_id: String,
    pub config_version: String,
    pub lifecycle_state: SyncDeploymentLifecycleState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncDeploymentLifecycleState {
    Requested,
    Starting,
    Running,
    Draining,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayRoute {
    pub host: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub sync_endpoint: String,
    pub tls_policy: TlsPolicy,
    pub rate_limit: RateLimitPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Domain {
    pub domain_id: String,
    pub hostname: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub route_host: String,
    pub verification_status: DomainVerificationStatus,
    pub verification_token: String,
    pub tls_status: DomainTlsStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpAllowlistRule {
    pub rule_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub name: String,
    pub cidr: String,
    pub purpose: IpAllowlistPurpose,
    pub status: IpAllowlistStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticEgressIp {
    pub egress_ip_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub region: String,
    pub ip_address: String,
    pub provider_ref: String,
    pub status: StaticEgressIpStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    pub window_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub name: String,
    pub day_of_week: MaintenanceDayOfWeek,
    pub start_time: String,
    pub duration_minutes: u32,
    pub auto_minor_upgrades: bool,
    pub status: MaintenanceWindowStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsPolicy {
    TerminateAtGateway,
    MutualTlsToSync {
        ca_secret_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_certificate_secret_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_private_key_secret_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        server_name: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitPolicy {
    pub max_connections: u32,
    pub max_requests_per_minute: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainVerificationStatus {
    Pending,
    Verified,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainTlsStatus {
    Pending,
    Active,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpAllowlistPurpose {
    App,
    Migration,
    Support,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IpAllowlistStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticEgressIpStatus {
    Provisioning,
    Active,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceDayOfWeek {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceWindowStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseProxyRoute {
    pub listen_addr: String,
    pub upstream_addr: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub cluster_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<DatabaseProxyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<DatabaseProxyTlsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseProxyPolicy {
    #[serde(default)]
    pub allowed_users: Vec<String>,
    #[serde(default)]
    pub allowed_databases: Vec<String>,
    #[serde(default)]
    pub forbidden_startup_parameters: Vec<String>,
    #[serde(default)]
    pub required_startup_parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub forbidden_simple_query_verbs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_simple_query_bytes: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseProxyTlsConfig {
    pub mode: DatabaseProxyTlsMode,
    pub certificate_id: String,
    pub common_name: String,
    pub certificate_secret_ref: SecretRef,
    pub private_key_secret_ref: SecretRef,
    pub fingerprint_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseProxyTlsMode {
    TerminateAtProxy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigVersionStatus {
    Uploaded,
    Validated,
    Deployed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseRoleSpec {
    pub name: String,
    pub kind: DatabaseRoleKind,
    pub secret_ref: Option<SecretRef>,
    #[serde(default)]
    pub privileges: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseRoleCredential {
    pub name: String,
    pub kind: DatabaseRoleKind,
    pub password: String,
    #[serde(default)]
    pub privileges: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseRoleKind {
    App,
    Migration,
    Replication,
    Support,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresCluster {
    pub cluster_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub region: String,
    pub postgres_version: PostgresVersion,
    pub tier: String,
    pub storage_gib: u32,
    pub lifecycle_state: ClusterLifecycleState,
    pub host_assignment: Option<HostAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresEndpoint {
    pub environment_id: String,
    pub active_cluster_id: String,
    pub updated_by_failover_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_proxy_listen_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_certificate_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresEndpointCertificate {
    pub certificate_id: String,
    pub environment_id: String,
    pub listen_addr: String,
    pub common_name: String,
    pub status: CertificateLifecycleState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_secret_ref: Option<SecretRef>,
    pub private_key_secret_ref: SecretRef,
    pub not_before: String,
    pub not_after: String,
    pub fingerprint_sha256: String,
    pub issued_by: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresEndpointCertificateBundle {
    pub certificate: ManagedPostgresEndpointCertificate,
    pub certificate_pem: String,
    pub private_key_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayRouteMtlsBundle {
    pub host: String,
    pub environment_id: String,
    pub ca_secret_ref: String,
    pub ca_pem: String,
    pub client_certificate_secret_ref: Option<String>,
    pub client_certificate_pem: Option<String>,
    pub client_private_key_secret_ref: Option<String>,
    pub client_private_key_pem: Option<String>,
    pub server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresCertificateAuthorityProvider {
    pub ca_provider_id: String,
    pub name: String,
    pub kind: CertificateAuthorityProviderKind,
    pub issuer_ref: String,
    pub status: CertificateAuthorityProviderStatus,
    pub default_for_managed_postgres: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateAuthorityProviderKind {
    LocalDev,
    Acme,
    ExternalPki,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateAuthorityProviderStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateLifecycleState {
    Provisioning,
    Active,
    Rotating,
    Revoked,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresAcmeOrder {
    pub order_id: String,
    pub certificate_id: String,
    pub environment_id: String,
    pub ca_provider_id: String,
    pub common_name: String,
    pub challenge_type: AcmeChallengeType,
    pub challenge_token: String,
    pub key_authorization_secret_ref: SecretRef,
    pub csr_secret_ref: SecretRef,
    pub directory_url: String,
    pub account_ref: String,
    pub status: AcmeOrderStatus,
    pub error_message: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AcmeChallengeType {
    #[serde(rename = "http_01")]
    Http01,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcmeOrderStatus {
    PendingChallenge,
    ReadyToFinalize,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresBackup {
    pub backup_id: String,
    pub cluster_id: String,
    pub status: BackupLifecycleState,
    pub backup_dir: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresBackupArtifact {
    pub artifact_id: String,
    pub backup_id: String,
    pub cluster_id: String,
    pub provider: String,
    pub object_uri: String,
    pub manifest_path: String,
    pub manifest_sha256: Option<String>,
    pub size_bytes: Option<u64>,
    pub status: BackupArtifactStatus,
    pub created_at: String,
    pub updated_at: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupArtifactStatus {
    Pending,
    Available,
    Expired,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresBackupRetentionPolicy {
    pub cluster_id: String,
    pub retention_days: u32,
    pub keep_min_successful_backups: u32,
    pub enabled: bool,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresCloneRedactionPolicy {
    pub policy_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub name: String,
    pub status: CloneRedactionPolicyStatus,
    pub rules: Vec<CloneRedactionRule>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloneRedactionPolicyStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloneRedactionRule {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
    pub method: CloneRedactionMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub static_value: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloneRedactionMethod {
    Null,
    StaticValue,
    HashSha256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresSupportAccessSession {
    pub session_id: String,
    pub cluster_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub requested_by: String,
    pub approved_by: Option<String>,
    pub revoked_by: Option<String>,
    pub reason: String,
    pub ticket_ref: Option<String>,
    pub status: SupportAccessStatus,
    pub requested_at: String,
    pub approved_at: Option<String>,
    pub expires_at: String,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportAccessStatus {
    Requested,
    Active,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresDeletionTombstone {
    pub cluster_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub region: String,
    pub postgres_version: PostgresVersion,
    pub tier: String,
    pub retained_backup_id: Option<String>,
    pub deleted_at: String,
    pub retention_expires_at: Option<String>,
    pub unrecoverable_at: Option<String>,
    pub expired_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresRestore {
    pub restore_id: String,
    pub source_cluster_id: String,
    pub target_cluster_id: String,
    pub target_environment_id: String,
    pub backup_id: String,
    pub status: RestoreLifecycleState,
    pub redaction_policy_id: Option<String>,
    pub recovery_target_lsn: Option<String>,
    pub error_message: Option<String>,
}

/// A named, addressable database state with a parent pointer.
///
/// Modeled on Neon database branches and backed by either a copy-on-write
/// clone (`HeadCow`) or a point-in-time restore (`PointInTime`); see
/// `paas/BRANCHING-API-DESIGN.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresBranch {
    pub branch_id: String,
    pub cluster_id: String,
    pub name: String,
    pub parent_branch_id: Option<String>,
    pub mode: BranchMode,
    pub source_database: String,
    /// Set for `HeadCow` branches: the cloned database in the same instance.
    pub branch_database: Option<String>,
    /// Set for `PointInTime` branches: the restored cluster backing the branch.
    pub branch_cluster_id: Option<String>,
    /// `None` for `HeadCow` (means "HEAD at creation"); the recovery target LSN
    /// for `PointInTime`.
    pub created_from_lsn: Option<String>,
    pub redaction_policy_id: Option<String>,
    pub lifecycle_state: BranchLifecycleState,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchMode {
    /// Instant copy-on-write clone of the parent database within the same
    /// cluster instance (`CREATE DATABASE ... STRATEGY FILE_COPY`).
    HeadCow,
    /// Point-in-time restore of the parent into its own cluster instance.
    PointInTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchLifecycleState {
    Creating,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresRestoreDrill {
    pub drill_id: String,
    pub source_cluster_id: String,
    pub restore_id: String,
    pub backup_id: String,
    pub target_cluster_id: String,
    pub status: RestoreLifecycleState,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresWalArchiveSegment {
    pub cluster_id: String,
    pub segment_name: String,
    pub status: WalArchiveSegmentStatus,
    pub archive_dir: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresPitrCheck {
    pub check_id: String,
    pub cluster_id: String,
    pub backup_id: Option<String>,
    pub status: PitrCheckStatus,
    pub segment_count: u32,
    pub first_segment: Option<String>,
    pub latest_segment: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresFailover {
    pub failover_id: String,
    pub source_cluster_id: String,
    pub target_cluster_id: String,
    pub status: FailoverLifecycleState,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresStandby {
    pub standby_id: String,
    pub source_cluster_id: String,
    pub target_cluster_id: String,
    pub backup_id: String,
    pub status: StandbyLifecycleState,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresStandbyCheck {
    pub check_id: String,
    pub standby_id: String,
    pub source_cluster_id: String,
    pub target_cluster_id: String,
    pub slot_name: String,
    pub max_lag_bytes: u64,
    pub status: StandbyCheckStatus,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresRuntimeCheck {
    pub check_id: String,
    pub cluster_id: String,
    pub status: RuntimeCheckStatus,
    pub connection_count: u32,
    pub max_connections: u32,
    pub replication_slot_lag_bytes: Option<u64>,
    pub long_running_query_count: u32,
    pub blocked_lock_count: u32,
    pub oldest_transaction_age_seconds: Option<u64>,
    pub autovacuum_running: bool,
    pub checked_at: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPostgresMajorUpgrade {
    pub upgrade_id: String,
    pub cluster_id: String,
    pub source_postgres_version: PostgresVersion,
    pub target_postgres_version: PostgresVersion,
    pub strategy: ManagedPostgresMajorUpgradeStrategy,
    pub status: ManagedPostgresMajorUpgradeStatus,
    pub command_id: Option<String>,
    pub operation_id: Option<String>,
    pub error_message: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedPostgresMajorUpgradeStrategy {
    LogicalReplicationCopy,
    PgUpgradeCopy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedPostgresMajorUpgradeStatus {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupLifecycleState {
    Requested,
    Running,
    Succeeded,
    Failed,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreLifecycleState {
    Requested,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalArchiveSegmentStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PitrCheckStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailoverLifecycleState {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandbyLifecycleState {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StandbyCheckStatus {
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCheckStatus {
    Healthy,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostAssignment {
    pub host_id: String,
    pub data_dir: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterLifecycleState {
    Requested,
    Placing,
    AllocatingStorage,
    InitializingPostgres,
    ConfiguringRoles,
    ConfiguringReplication,
    Restoring,
    Resizing,
    UpdatingPostgres,
    Starting,
    Verifying,
    Ready,
    Stopping,
    Stopped,
    Deleting,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAgentCommand {
    pub command_id: String,
    pub cluster_id: String,
    pub action: NodeAgentAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedNodeAgentCommand {
    pub host_id: String,
    pub operation_id: Option<String>,
    pub command: NodeAgentCommand,
    pub status: AgentCommandStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAgentCommandResult {
    pub command_id: String,
    pub host_id: String,
    pub status: AgentCommandStatus,
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_token: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backup_artifacts: Vec<NodeAgentBackupArtifact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAgentBackupArtifact {
    pub backup_id: String,
    pub provider: String,
    pub object_uri: String,
    pub manifest_path: String,
    pub manifest_sha256: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentCommandStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeAgentAction {
    PreparePostgres {
        postgres_version: PostgresVersion,
        data_dir: String,
        port: u16,
    },
    StartPostgres,
    StopPostgres,
    DeletePostgresData {
        data_dir: String,
        tombstone_retention_days: Option<u32>,
    },
    ConfigurePostgresAccess {
        data_dir: String,
        port: u16,
        database: String,
        roles: Vec<DatabaseRoleCredential>,
        publication: String,
        replication_slot: String,
    },
    CreateCopyOnWriteDatabaseClone {
        data_dir: String,
        port: u16,
        source_database: String,
        target_database: String,
        #[serde(default)]
        terminate_source_connections: bool,
    },
    DropDatabase {
        data_dir: String,
        port: u16,
        database: String,
        #[serde(default)]
        terminate_connections: bool,
    },
    ReportStatus,
    CheckPostgresStandbyLag {
        source_data_dir: String,
        source_port: u16,
        database: String,
        slot_name: String,
        max_lag_bytes: u64,
    },
    RunBaseBackup {
        backup_id: String,
        data_dir: String,
        postgres_url: String,
        backup_dir: String,
    },
    DeleteBackupData {
        backup_id: String,
        backup_dir: String,
    },
    PrepareRestore {
        backup_id: String,
        backup_dir: String,
        data_dir: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_port: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        restore_command: String,
        recovery_target_lsn: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        redaction_policy: Option<ManagedPostgresCloneRedactionPolicy>,
    },
    PreparePostgresStandby {
        backup_id: String,
        backup_dir: String,
        data_dir: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_port: Option<u16>,
        primary_conninfo: String,
        primary_slot_name: String,
        source_data_dir: String,
        source_port: u16,
        database: String,
        restore_command: String,
    },
    ArchiveWalSegment {
        source_path: String,
        archive_dir: String,
        segment_name: String,
    },
    PromotePostgresStandby {
        data_dir: String,
    },
    FencePostgresPrimary {
        data_dir: String,
    },
    ResizePostgresStorage {
        data_dir: String,
        storage_gib: u32,
    },
    UpdatePostgresMinor {
        data_dir: String,
        target_postgres_version: PostgresVersion,
    },
    UpgradePostgresMajor {
        data_dir: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_port: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        source_postgres_version: PostgresVersion,
        target_postgres_version: PostgresVersion,
        strategy: ManagedPostgresMajorUpgradeStrategy,
    },
    StartSyncDeployment {
        deployment_id: String,
        config_version: String,
    },
    StopSyncDeployment {
        deployment_id: String,
    },
    ReportSyncDeployment {
        deployment_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAgentStatus {
    pub host_id: String,
    pub cluster_id: String,
    pub observed_state: ClusterLifecycleState,
    pub postgres_running: bool,
    pub postgres_version: Option<PostgresVersion>,
    pub backup_status: Option<BackupStatus>,
    pub wal_archive_status: Option<WalArchiveStatus>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHost {
    pub host_id: String,
    pub region: String,
    pub failure_domain: String,
    pub data_root: String,
    pub first_port: u16,
    pub state: NodeHostState,
    pub capacity: NodeHostCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHostHardeningCheck {
    pub check_id: String,
    pub host_id: String,
    pub status: NodeHostHardeningStatus,
    pub image_ref: String,
    pub os_release: String,
    pub kernel_version: String,
    pub postgres_major_min: u16,
    pub container_runtime: String,
    pub disk_encryption: bool,
    pub firewall_enabled: bool,
    pub unattended_upgrades: bool,
    pub last_patched_at: Option<String>,
    pub checked_at: String,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHostHardeningStatus {
    Passing,
    Warning,
    Failing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHostAgentCredential {
    pub host_id: String,
    pub key_id: String,
    pub state: NodeHostAgentCredentialState,
    pub created_at: String,
    pub rotated_at: Option<String>,
    pub revoked_at: Option<String>,
    pub last_used_at: Option<String>,
    pub last_used_operation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHostAgentCredentialState {
    Active,
    Rotated,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHostHeartbeat {
    pub state: NodeHostState,
    pub capacity: NodeHostCapacity,
    #[serde(default)]
    pub observed_clusters: Vec<NodeHostClusterObservation>,
    #[serde(default)]
    pub observed_sync_deployments: Vec<NodeHostSyncDeploymentObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHostClusterObservation {
    pub cluster_id: String,
    pub data_dir: String,
    pub postgres_running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHostSyncDeploymentObservation {
    pub deployment_id: String,
    pub running: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHostState {
    Registering,
    Active,
    Draining,
    Maintenance,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHostCapacity {
    pub max_clusters: u32,
    pub assigned_clusters: u32,
    pub storage_gib: u32,
    pub used_storage_gib: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation_id: String,
    pub idempotency_key: String,
    pub target_resource_id: String,
    pub kind: OperationKind,
    pub status: OperationStatus,
    pub current_step: String,
    pub lease_owner: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    CreateCluster,
    StartCluster,
    StopCluster,
    ResizeCluster,
    UpdateCluster,
    RotateCredentials,
    BackupCluster,
    DeleteBackup,
    ArchiveWalSegment,
    RestoreCluster,
    CreateDatabaseClone,
    CreateBranch,
    DeleteBranch,
    PrepareStandby,
    CheckStandby,
    FencePrimary,
    FailoverCluster,
    DeleteCluster,
    StartSyncDeployment,
    StopSyncDeployment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: String,
    pub actor_id: String,
    pub action: String,
    pub resource_id: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKey {
    pub key_id: String,
    pub token_prefix: String,
    pub name: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub role: TeamRole,
    pub created_by: String,
    pub revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamMembership {
    pub organization_id: String,
    pub actor_id: String,
    pub role: TeamRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretEncryptionKey {
    pub key_ref: String,
    pub provider: String,
    pub purpose: String,
    pub status: SecretEncryptionKeyStatus,
    pub created_at: String,
    pub activated_at: Option<String>,
    pub retired_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretEncryptionKeyStatus {
    Active,
    Retiring,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRewrapPlan {
    pub plan_id: String,
    pub source_key_ref: String,
    pub target_key_ref: String,
    pub status: SecretRewrapPlanStatus,
    pub matched_secret_count: u32,
    pub rewrapped_secret_count: u32,
    pub error_message: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretRewrapPlanStatus {
    Planned,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtIssuer {
    pub issuer_id: String,
    pub organization_id: String,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub name: String,
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub claim_to_field: Vec<JwtClaimMapping>,
    pub status: JwtIssuerStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    pub endpoint_id: String,
    pub organization_id: String,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub name: String,
    pub url: String,
    pub event_types: Vec<String>,
    pub signing_secret_ref: Option<SecretRef>,
    pub status: WebhookEndpointStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsoIdentityProvider {
    pub provider_id: String,
    pub organization_id: String,
    pub name: String,
    pub kind: SsoProviderKind,
    pub issuer: String,
    pub sso_url: String,
    pub certificate_secret_ref: Option<SecretRef>,
    pub claim_mappings: Vec<SsoClaimMapping>,
    pub status: SsoProviderStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incident {
    pub incident_id: String,
    pub organization_id: String,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub title: String,
    pub summary: String,
    pub severity: IncidentSeverity,
    pub status: IncidentStatus,
    #[serde(default)]
    pub impacted_services: Vec<String>,
    pub started_at: String,
    pub resolved_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SsoClaimMapping {
    pub claim: String,
    pub field: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtClaimMapping {
    pub claim: String,
    pub field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamRole {
    Owner,
    Admin,
    Developer,
    Viewer,
    Ci,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JwtIssuerStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEndpointStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SsoProviderKind {
    Saml,
    Oidc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SsoProviderStatus {
    Active,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Investigating,
    Identified,
    Monitoring,
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub event_id: String,
    pub idempotency_key: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub metric: String,
    pub quantity: u64,
    pub occurred_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<UsageEventSignature>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEventSignature {
    pub key_id: String,
    pub algorithm: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaPolicy {
    pub policy_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub metric: String,
    pub limit_quantity: u64,
    pub window_seconds: u64,
    pub enforcement: QuotaEnforcement,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaAlert {
    pub alert_id: String,
    pub policy_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub metric: String,
    pub threshold_basis_points: u32,
    pub current_quantity: u64,
    pub limit_quantity: u64,
    pub window_seconds: u64,
    pub state: QuotaAlertState,
    pub last_evaluated_at: Option<String>,
    pub fired_at: Option<String>,
    pub resolved_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPermissionPolicy {
    pub policy_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub environment_id: String,
    pub name: String,
    pub table_schema: String,
    pub table_name: String,
    pub operation: QueryPermissionOperation,
    pub principal_claim: String,
    pub predicate_sql: String,
    pub sample_context: serde_json::Value,
    pub status: QueryPermissionPolicyStatus,
}

/// Per-environment permission-rule DSL document authored in the PaaS UI.
///
/// The `dsl` field holds the raw `palimpsest-permissions` TOML config; it is
/// stored verbatim so operators can keep drafts, and is compiled by the
/// verifier on demand (see the `/v1/permissions/verify` endpoint).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRuleDocument {
    pub environment_id: String,
    pub organization_id: String,
    pub project_id: String,
    pub dsl: String,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaEnforcement {
    Reject,
    Observe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaAlertState {
    Ok,
    Firing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryPermissionOperation {
    Read,
    Subscribe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryPermissionPolicyStatus {
    Draft,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BillingExport {
    pub export_id: String,
    pub destination: String,
    pub delivery_ref: Option<String>,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub metric: Option<String>,
    pub occurred_at_from: Option<String>,
    pub occurred_at_to: Option<String>,
    pub event_count: u64,
    pub quantity_total: u64,
    pub status: BillingExportStatus,
    pub error_message: Option<String>,
    pub events: Vec<UsageEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingExportStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    pub secret_id: String,
    pub provider: String,
    pub external_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupStatus {
    pub last_base_backup_id: Option<String>,
    pub last_base_backup_at: Option<String>,
    pub last_successful_restore_check_at: Option<String>,
    pub pitr_window_hours: u16,
    pub state: BackupState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupState {
    Unknown,
    Healthy,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalArchiveStatus {
    pub latest_archived_lsn: Option<String>,
    pub latest_archived_at: Option<String>,
    pub archive_lag_bytes: Option<u64>,
    pub state: WalArchiveState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalArchiveState {
    Unknown,
    Streaming,
    Lagging,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomerEnvironmentHealth {
    pub environment_id: String,
    pub state: CustomerEnvironmentHealthState,
    pub summary: String,
    pub components: Vec<CustomerEnvironmentHealthComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomerEnvironmentHealthComponent {
    pub name: String,
    pub state: CustomerEnvironmentHealthState,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustomerEnvironmentHealthState {
    Healthy,
    Degraded,
    AtRisk,
    Maintenance,
    Unavailable,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ModelError {
    #[error("invalid postgres version '{0}'")]
    InvalidPostgresVersion(String),
    #[error("postgres version '{version}' is unsupported; managed and local PaaS require PostgreSQL {minimum_major}+")]
    UnsupportedPostgresVersion { version: String, minimum_major: u16 },
    #[error("database mode {mode:?} requires postgres_version")]
    MissingPostgresVersion { mode: DatabaseMode },
    #[error("managed database mode requires a managed postgres spec")]
    MissingManagedPostgresSpec,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_version_accepts_eighteen_and_newer() {
        let version = PostgresVersion::new("18.1").expect("version should parse");
        assert_eq!(version.major(), 18);

        let version = PostgresVersion::new("19-beta1").expect("version should parse");
        assert_eq!(version.major(), 19);
    }

    #[test]
    fn postgres_version_rejects_pre_eighteen() {
        let err = PostgresVersion::new("17.5").expect_err("version should be rejected");
        assert_eq!(
            err,
            ModelError::UnsupportedPostgresVersion {
                version: "17.5".to_owned(),
                minimum_major: 18
            }
        );
    }

    #[test]
    fn managed_database_requires_version_and_spec() {
        let spec = DatabaseSpec {
            mode: DatabaseMode::Managed,
            postgres_version: Some(PostgresVersion::new("18").expect("valid version")),
            managed: None,
        };

        assert_eq!(spec.validate(), Err(ModelError::MissingManagedPostgresSpec));
    }

    #[test]
    fn managed_postgres_spec_roles_default_when_omitted() {
        let raw = r#"{
            "cluster_id": "cluster_123",
            "tier": "dev",
            "storage_gib": 20,
            "backup_policy": {
                "pitr_window_hours": 72,
                "base_backup_interval_hours": 24
            },
            "maintenance_policy": {
                "window": "sun:04:00-05:00Z",
                "auto_minor_upgrades": true
            }
        }"#;

        let spec: ManagedPostgresSpec = serde_json::from_str(raw).expect("spec parses");

        assert!(spec.roles.is_empty());
    }
}
