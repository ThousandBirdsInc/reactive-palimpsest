// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! SQL-backed control-plane persistence.
//!
//! Server-side Postgres access uses `sqlx`. Schema changes are applied by
//! Flyway from `paas/control-plane/migrations`; this crate intentionally does
//! not run migrations itself.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use palimpsest_paas_core::{
    AcmeChallengeType, AcmeOrderStatus, AgentCommandStatus, ApiKey, AuditEvent,
    BackupArtifactStatus, BackupLifecycleState, BillingExport, BillingExportStatus,
    CertificateAuthorityProviderKind, CertificateAuthorityProviderStatus,
    CertificateLifecycleState, CloneRedactionPolicyStatus, CloneRedactionRule,
    ClusterLifecycleState, ConfigVersion, ConfigVersionStatus, CustomerEnvironmentHealth,
    CustomerEnvironmentHealthComponent, CustomerEnvironmentHealthState, DatabaseProxyPolicy,
    DatabaseProxyRoute, DatabaseProxyTlsConfig, DatabaseProxyTlsMode, DatabaseRoleCredential,
    DatabaseRoleKind, Domain, DomainTlsStatus, DomainVerificationStatus, Environment,
    FailoverLifecycleState, GatewayRoute, GatewayRouteMtlsBundle, HostAssignment, Incident,
    IncidentSeverity, IncidentStatus, IpAllowlistPurpose, IpAllowlistRule, IpAllowlistStatus,
    JwtIssuer, JwtIssuerStatus, MaintenanceDayOfWeek, MaintenanceWindow, MaintenanceWindowStatus,
    ManagedPostgresAcmeOrder, ManagedPostgresBackup, ManagedPostgresBackupArtifact,
    ManagedPostgresBackupRetentionPolicy, ManagedPostgresCertificateAuthorityProvider,
    ManagedPostgresCloneRedactionPolicy, ManagedPostgresCluster, ManagedPostgresDeletionTombstone,
    ManagedPostgresEndpoint, ManagedPostgresEndpointCertificate,
    ManagedPostgresEndpointCertificateBundle, ManagedPostgresFailover, ManagedPostgresMajorUpgrade,
    ManagedPostgresMajorUpgradeStatus, ManagedPostgresMajorUpgradeStrategy,
    ManagedPostgresPitrCheck, ManagedPostgresRestore, ManagedPostgresRestoreDrill,
    ManagedPostgresRuntimeCheck, ManagedPostgresStandby, ManagedPostgresStandbyCheck,
    ManagedPostgresSupportAccessSession, ManagedPostgresWalArchiveSegment, NodeAgentAction,
    NodeAgentBackupArtifact, NodeAgentCommand, NodeHost, NodeHostAgentCredential,
    NodeHostAgentCredentialState, NodeHostClusterObservation, NodeHostHardeningCheck,
    NodeHostHardeningStatus, NodeHostHeartbeat, NodeHostState, NodeHostSyncDeploymentObservation,
    OperationKind, OperationRecord, OperationStatus, Organization, PermissionRuleDocument,
    PitrCheckStatus, PostgresVersion, Project, QueryPermissionOperation, QueryPermissionPolicy,
    QueryPermissionPolicyStatus, QueuedNodeAgentCommand, QuotaAlert, QuotaAlertState,
    QuotaEnforcement, QuotaPolicy, RateLimitPolicy, RestoreLifecycleState, RuntimeCheckStatus,
    SecretEncryptionKey, SecretEncryptionKeyStatus, SecretRef, SecretRewrapPlan,
    SecretRewrapPlanStatus, SsoIdentityProvider, SsoProviderKind, SsoProviderStatus,
    StandbyCheckStatus, StandbyLifecycleState, StaticEgressIp, StaticEgressIpStatus,
    SupportAccessStatus, SyncDeployment, SyncDeploymentLifecycleState, TeamMembership, TeamRole,
    TlsPolicy, UsageEvent, WalArchiveSegmentStatus, WebhookEndpoint, WebhookEndpointStatus,
};
use rcgen::{CertificateParams, CertifiedKey, KeyPair};
use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM},
    digest,
    rand::{SecureRandom, SystemRandom},
};
use sqlx::{postgres::PgPoolOptions, types::Json, PgPool, Row};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufReader, Cursor, Write},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

use crate::HostCapacity;

pub async fn connect(database_url: &str) -> Result<PgPool, SqlStoreError> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .map_err(SqlStoreError::from)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretMaterialBackend {
    LocalDev,
    EnvEnvelope(EnvEnvelopeSecretBackend),
    FileEnvelope(EnvEnvelopeSecretBackend),
}

impl SecretMaterialBackend {
    pub fn from_env() -> Result<Self, SqlStoreError> {
        match std::env::var("PALIMPSEST_PAAS_SECRET_PROVIDER")
            .unwrap_or_else(|_| "local-dev".to_owned())
            .as_str()
        {
            "local-dev" => Ok(Self::LocalDev),
            "env-envelope" => {
                let key_ref = std::env::var("PALIMPSEST_PAAS_SECRET_KEY_REF")
                    .unwrap_or_else(|_| "env:PALIMPSEST_PAAS_SECRET_KEY_BASE64".to_owned());
                let key = std::env::var("PALIMPSEST_PAAS_SECRET_KEY_BASE64").map_err(|_| {
                    SqlStoreError::InvalidValue(
                        "PALIMPSEST_PAAS_SECRET_KEY_BASE64 is required for env-envelope secrets"
                            .to_owned(),
                    )
                })?;
                let mut keys = BTreeMap::from([(key_ref.clone(), key)]);
                if let Ok(encoded_keyring) = std::env::var("PALIMPSEST_PAAS_SECRET_KEYRING_BASE64")
                {
                    let configured_keys: BTreeMap<String, String> =
                        serde_json::from_str(&encoded_keyring).map_err(|err| {
                            SqlStoreError::InvalidValue(format!(
                                "parse PALIMPSEST_PAAS_SECRET_KEYRING_BASE64 JSON: {err}"
                            ))
                        })?;
                    keys.extend(configured_keys);
                }
                EnvEnvelopeSecretBackend::from_base64_keyring(key_ref, keys).map(Self::EnvEnvelope)
            }
            "file-envelope" => {
                let keyring_path =
                    std::env::var("PALIMPSEST_PAAS_SECRET_KEYRING_FILE").map_err(|_| {
                        SqlStoreError::InvalidValue(
                            "PALIMPSEST_PAAS_SECRET_KEYRING_FILE is required for file-envelope secrets"
                                .to_owned(),
                        )
                    })?;
                EnvEnvelopeSecretBackend::from_keyring_file(&keyring_path).map(Self::FileEnvelope)
            }
            other => Err(SqlStoreError::InvalidValue(format!(
                "unsupported secret provider '{other}'"
            ))),
        }
    }

    fn provider(&self) -> &'static str {
        match self {
            Self::LocalDev => "local-dev",
            Self::EnvEnvelope(_) => "env-envelope",
            Self::FileEnvelope(_) => "file-envelope",
        }
    }

    fn secret_id(&self, cluster_id: &str, role: &str) -> String {
        match self {
            Self::LocalDev => format!("{cluster_id}:{role}:password"),
            Self::EnvEnvelope(_) | Self::FileEnvelope(_) => {
                format!("{}:{cluster_id}:{role}:password", self.provider())
            }
        }
    }

    fn seal(
        &self,
        external_ref: &str,
        material: &str,
    ) -> Result<StoredSecretMaterial, SqlStoreError> {
        match self {
            Self::LocalDev => Ok(StoredSecretMaterial {
                secret_material: Some(material.to_owned()),
                encrypted_material: None,
                key_ref: None,
            }),
            Self::EnvEnvelope(backend) | Self::FileEnvelope(backend) => Ok(StoredSecretMaterial {
                secret_material: None,
                encrypted_material: Some(backend.seal(external_ref, material)?),
                key_ref: Some(backend.primary_key_ref.clone()),
            }),
        }
    }

    fn seal_for_key(
        &self,
        external_ref: &str,
        material: &str,
        key_ref: &str,
    ) -> Result<StoredSecretMaterial, SqlStoreError> {
        match self {
            Self::LocalDev => Ok(StoredSecretMaterial {
                secret_material: Some(material.to_owned()),
                encrypted_material: None,
                key_ref: Some(key_ref.to_owned()),
            }),
            Self::EnvEnvelope(backend) | Self::FileEnvelope(backend) => Ok(StoredSecretMaterial {
                secret_material: None,
                encrypted_material: Some(backend.seal_for_key(external_ref, material, key_ref)?),
                key_ref: Some(key_ref.to_owned()),
            }),
        }
    }

    fn open(
        &self,
        external_ref: &str,
        secret_material: Option<String>,
        encrypted_material: Option<String>,
        key_ref: Option<String>,
    ) -> Result<String, SqlStoreError> {
        match self {
            Self::LocalDev => secret_material.ok_or_else(|| {
                SqlStoreError::InvalidValue(format!(
                    "local-dev secret {external_ref} has no material"
                ))
            }),
            Self::EnvEnvelope(backend) | Self::FileEnvelope(backend) => {
                let encrypted_material = encrypted_material.ok_or_else(|| {
                    SqlStoreError::InvalidValue(format!(
                        "{} secret {external_ref} has no encrypted material",
                        self.provider()
                    ))
                })?;
                let key_ref = key_ref.ok_or_else(|| {
                    SqlStoreError::InvalidValue(format!(
                        "{} secret {external_ref} has no key ref",
                        self.provider()
                    ))
                })?;
                backend.open_for_key(external_ref, &encrypted_material, &key_ref)
            }
        }
    }
}

impl Default for SecretMaterialBackend {
    fn default() -> Self {
        Self::LocalDev
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvEnvelopeSecretBackend {
    primary_key_ref: String,
    keyring: BTreeMap<String, [u8; 32]>,
}

impl EnvEnvelopeSecretBackend {
    pub fn from_base64_key(key_ref: String, encoded_key: &str) -> Result<Self, SqlStoreError> {
        Self::from_base64_keyring(
            key_ref.clone(),
            BTreeMap::from([(key_ref, encoded_key.to_owned())]),
        )
    }

    pub fn from_base64_keyring(
        primary_key_ref: String,
        encoded_keys: BTreeMap<String, String>,
    ) -> Result<Self, SqlStoreError> {
        let keyring = encoded_keys
            .into_iter()
            .map(|(key_ref, encoded_key)| decode_env_envelope_key(&key_ref, &encoded_key))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if !keyring.contains_key(&primary_key_ref) {
            return Err(SqlStoreError::InvalidValue(format!(
                "primary env-envelope key {primary_key_ref} is missing from keyring"
            )));
        }
        Ok(Self {
            primary_key_ref,
            keyring,
        })
    }

    pub fn from_keyring_file(path: &str) -> Result<Self, SqlStoreError> {
        let contents = fs::read_to_string(path).map_err(|err| {
            SqlStoreError::InvalidValue(format!("read secret keyring file {path}: {err}"))
        })?;
        Self::from_keyring_json(&contents)
    }

    pub fn from_keyring_json(contents: &str) -> Result<Self, SqlStoreError> {
        let config: FileEnvelopeKeyring = serde_json::from_str(contents).map_err(|err| {
            SqlStoreError::InvalidValue(format!("parse file-envelope keyring JSON: {err}"))
        })?;
        Self::from_base64_keyring(config.primary_key_ref, config.keys)
    }

    fn seal(&self, external_ref: &str, material: &str) -> Result<String, SqlStoreError> {
        self.seal_for_key(external_ref, material, &self.primary_key_ref)
    }

    fn seal_for_key(
        &self,
        external_ref: &str,
        material: &str,
        key_ref: &str,
    ) -> Result<String, SqlStoreError> {
        let key_bytes = self.keyring.get(key_ref).ok_or_else(|| {
            SqlStoreError::InvalidValue(format!("env-envelope key {key_ref} is not configured"))
        })?;
        let key = envelope_key(key_bytes)?;
        let rng = SystemRandom::new();
        let mut nonce_bytes = [0_u8; 12];
        rng.fill(&mut nonce_bytes).map_err(|err| {
            SqlStoreError::InvalidValue(format!("generate secret nonce: {err:?}"))
        })?;

        let mut sealed = material.as_bytes().to_vec();
        key.seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(external_ref.as_bytes()),
            &mut sealed,
        )
        .map_err(|err| SqlStoreError::InvalidValue(format!("seal secret: {err:?}")))?;

        let mut envelope = Vec::with_capacity(nonce_bytes.len() + sealed.len());
        envelope.extend_from_slice(&nonce_bytes);
        envelope.extend_from_slice(&sealed);
        Ok(BASE64.encode(envelope))
    }

    fn open_for_key(
        &self,
        external_ref: &str,
        envelope: &str,
        key_ref: &str,
    ) -> Result<String, SqlStoreError> {
        let key_bytes = self.keyring.get(key_ref).ok_or_else(|| {
            SqlStoreError::InvalidValue(format!(
                "env-envelope secret {external_ref} is wrapped by unconfigured key {key_ref}"
            ))
        })?;
        let key = envelope_key(key_bytes)?;
        let mut decoded = BASE64
            .decode(envelope)
            .map_err(|err| SqlStoreError::InvalidValue(format!("decode secret envelope: {err}")))?;
        if decoded.len() <= 12 {
            return Err(SqlStoreError::InvalidValue(
                "secret envelope is missing nonce or ciphertext".to_owned(),
            ));
        }
        let mut nonce_bytes = [0_u8; 12];
        nonce_bytes.copy_from_slice(&decoded[..12]);
        let opened = key
            .open_in_place(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(external_ref.as_bytes()),
                &mut decoded[12..],
            )
            .map_err(|err| SqlStoreError::InvalidValue(format!("open secret: {err:?}")))?;
        String::from_utf8(opened.to_vec())
            .map_err(|err| SqlStoreError::InvalidValue(format!("decode secret utf8: {err}")))
    }
}

#[derive(Debug, serde::Deserialize)]
struct FileEnvelopeKeyring {
    primary_key_ref: String,
    keys: BTreeMap<String, String>,
}

fn decode_env_envelope_key(
    key_ref: &str,
    encoded_key: &str,
) -> Result<(String, [u8; 32]), SqlStoreError> {
    let decoded = BASE64.decode(encoded_key).map_err(|err| {
        SqlStoreError::InvalidValue(format!("decode secret key {key_ref}: {err}"))
    })?;
    let key_bytes: [u8; 32] = decoded.try_into().map_err(|decoded: Vec<u8>| {
        SqlStoreError::InvalidValue(format!(
            "env-envelope secret key {key_ref} must be 32 bytes, got {}",
            decoded.len()
        ))
    })?;
    Ok((key_ref.to_owned(), key_bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredSecretMaterial {
    secret_material: Option<String>,
    encrypted_material: Option<String>,
    key_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IssuedNodeAgentCredential {
    pub host_id: String,
    pub key_id: String,
    pub signing_key_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedPostgresSqlEndpoint {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDatabaseRoleCredentialRotation {
    pub rotation_id: String,
    pub credentials: Vec<DatabaseRoleCredential>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatabaseRoleCredentialWriteMode<'a> {
    CanonicalInsert,
    PendingRotation(&'a str),
}

fn envelope_key(key_bytes: &[u8; 32]) -> Result<LessSafeKey, SqlStoreError> {
    let unbound = UnboundKey::new(&AES_256_GCM, key_bytes)
        .map_err(|err| SqlStoreError::InvalidValue(format!("load secret key: {err:?}")))?;
    Ok(aead::LessSafeKey::new(unbound))
}

fn generate_agent_signing_key() -> Result<[u8; 32], SqlStoreError> {
    let rng = SystemRandom::new();
    let mut key = [0_u8; 32];
    rng.fill(&mut key).map_err(|err| {
        SqlStoreError::InvalidValue(format!("generate agent signing key: {err:?}"))
    })?;
    Ok(key)
}

fn generate_operation_token() -> Result<String, SqlStoreError> {
    let rng = SystemRandom::new();
    let mut token = [0_u8; 32];
    rng.fill(&mut token)
        .map_err(|err| SqlStoreError::InvalidValue(format!("generate operation token: {err:?}")))?;
    Ok(format!("plmp_op_{}", BASE64.encode(token)))
}

fn operation_token_hash(token: &str) -> String {
    BASE64.encode(digest::digest(&digest::SHA256, token.as_bytes()).as_ref())
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn push_health_component(
    components: &mut Vec<CustomerEnvironmentHealthComponent>,
    name: &str,
    state: CustomerEnvironmentHealthState,
    detail: impl Into<String>,
) {
    components.push(CustomerEnvironmentHealthComponent {
        name: name.to_owned(),
        state,
        detail: detail.into(),
    });
}

fn database_health_for_cluster(
    cluster_id: &str,
    state: ClusterLifecycleState,
) -> (CustomerEnvironmentHealthState, String) {
    match state {
        ClusterLifecycleState::Ready => (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} is ready"),
        ),
        ClusterLifecycleState::Stopping
        | ClusterLifecycleState::Deleting
        | ClusterLifecycleState::Resizing
        | ClusterLifecycleState::UpdatingPostgres
        | ClusterLifecycleState::Restoring => (
            CustomerEnvironmentHealthState::Maintenance,
            format!("managed Postgres cluster {cluster_id} is in {state:?}"),
        ),
        ClusterLifecycleState::Stopped
        | ClusterLifecycleState::Deleted
        | ClusterLifecycleState::Failed => (
            CustomerEnvironmentHealthState::Unavailable,
            format!("managed Postgres cluster {cluster_id} is in {state:?}"),
        ),
        ClusterLifecycleState::Requested
        | ClusterLifecycleState::Placing
        | ClusterLifecycleState::AllocatingStorage
        | ClusterLifecycleState::InitializingPostgres
        | ClusterLifecycleState::ConfiguringRoles
        | ClusterLifecycleState::ConfiguringReplication
        | ClusterLifecycleState::Starting
        | ClusterLifecycleState::Verifying => (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} is still provisioning"),
        ),
    }
}

fn observed_database_health_for_cluster(
    cluster_id: &str,
    postgres_running: Option<bool>,
) -> (CustomerEnvironmentHealthState, String) {
    match postgres_running {
        Some(true) => (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} is observed running"),
        ),
        Some(false) => (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} is ready but not observed running"),
        ),
        None => (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} has no host runtime observation"),
        ),
    }
}

fn storage_health_for_cluster(
    cluster_id: &str,
    storage_gib: Option<i32>,
    used_storage_gib: Option<i32>,
) -> (CustomerEnvironmentHealthState, String) {
    let Some(storage_gib) = storage_gib else {
        return (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} is not assigned to a reporting host"),
        );
    };
    if storage_gib <= 0 {
        return (
            CustomerEnvironmentHealthState::AtRisk,
            format!("host storage capacity for cluster {cluster_id} is not reported"),
        );
    }
    let used_storage_gib = used_storage_gib.unwrap_or_default();
    let ratio = f64::from(used_storage_gib) / f64::from(storage_gib);
    if ratio >= 0.95 {
        (
            CustomerEnvironmentHealthState::AtRisk,
            format!("host storage for cluster {cluster_id} is above 95%"),
        )
    } else if ratio >= 0.85 {
        (
            CustomerEnvironmentHealthState::Degraded,
            format!("host storage for cluster {cluster_id} is above 85%"),
        )
    } else {
        (
            CustomerEnvironmentHealthState::Healthy,
            format!("host storage for cluster {cluster_id} is within threshold"),
        )
    }
}

fn backup_health_for_cluster(
    cluster_id: &str,
    status: Option<&str>,
) -> (CustomerEnvironmentHealthState, String) {
    match status {
        Some("succeeded") => (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} has a successful backup"),
        ),
        Some("requested" | "running") => (
            CustomerEnvironmentHealthState::Maintenance,
            format!("managed Postgres cluster {cluster_id} has a backup in progress"),
        ),
        Some("failed") => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest backup for cluster {cluster_id} failed"),
        ),
        Some(other) => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest backup for cluster {cluster_id} has unknown status {other}"),
        ),
        None => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("managed Postgres cluster {cluster_id} has no backup record"),
        ),
    }
}

fn restore_drill_health_for_cluster(
    cluster_id: &str,
    status: Option<&str>,
) -> (CustomerEnvironmentHealthState, String) {
    match status {
        Some("succeeded") => (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} has a successful restore drill"),
        ),
        Some("requested" | "running") => (
            CustomerEnvironmentHealthState::Maintenance,
            format!("managed Postgres cluster {cluster_id} has a restore drill in progress"),
        ),
        Some("failed") => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest restore drill for cluster {cluster_id} failed"),
        ),
        Some(other) => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest restore drill for cluster {cluster_id} has unknown status {other}"),
        ),
        None => (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} has no restore drill record"),
        ),
    }
}

fn wal_health_for_cluster(
    cluster_id: &str,
    failed_count: i64,
    succeeded_count: i64,
) -> (CustomerEnvironmentHealthState, String) {
    if failed_count > 0 {
        (
            CustomerEnvironmentHealthState::AtRisk,
            format!("managed Postgres cluster {cluster_id} has failed WAL archive records"),
        )
    } else if succeeded_count > 0 {
        (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} has archived WAL"),
        )
    } else {
        (
            CustomerEnvironmentHealthState::AtRisk,
            format!("managed Postgres cluster {cluster_id} has no WAL archive record"),
        )
    }
}

fn pitr_health_for_cluster(
    cluster_id: &str,
    status: Option<&str>,
) -> (CustomerEnvironmentHealthState, String) {
    match status {
        Some("succeeded") => (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} has a valid PITR continuity check"),
        ),
        Some("failed") => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest PITR continuity check for cluster {cluster_id} failed"),
        ),
        Some(other) => (
            CustomerEnvironmentHealthState::AtRisk,
            format!(
                "latest PITR continuity check for cluster {cluster_id} has unknown status {other}"
            ),
        ),
        None => (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} has no PITR continuity check"),
        ),
    }
}

fn standby_check_health_for_cluster(
    cluster_id: &str,
    status: Option<&str>,
) -> (CustomerEnvironmentHealthState, String) {
    match status {
        Some("succeeded") => (
            CustomerEnvironmentHealthState::Healthy,
            format!("managed Postgres cluster {cluster_id} has a valid standby lag check"),
        ),
        Some("running") => (
            CustomerEnvironmentHealthState::Maintenance,
            format!("managed Postgres cluster {cluster_id} has a standby lag check in progress"),
        ),
        Some("failed") => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest standby lag check for cluster {cluster_id} failed"),
        ),
        Some(other) => (
            CustomerEnvironmentHealthState::AtRisk,
            format!("latest standby lag check for cluster {cluster_id} has unknown status {other}"),
        ),
        None => (
            CustomerEnvironmentHealthState::Degraded,
            format!("managed Postgres cluster {cluster_id} has no standby lag check"),
        ),
    }
}

fn sync_health_for_deployment(
    deployment_id: &str,
    state: SyncDeploymentLifecycleState,
) -> (CustomerEnvironmentHealthState, String) {
    match state {
        SyncDeploymentLifecycleState::Running => (
            CustomerEnvironmentHealthState::Healthy,
            format!("SyncDeployment {deployment_id} is running"),
        ),
        SyncDeploymentLifecycleState::Starting | SyncDeploymentLifecycleState::Requested => (
            CustomerEnvironmentHealthState::Degraded,
            format!("SyncDeployment {deployment_id} is still starting"),
        ),
        SyncDeploymentLifecycleState::Draining => (
            CustomerEnvironmentHealthState::Maintenance,
            format!("SyncDeployment {deployment_id} is draining"),
        ),
        SyncDeploymentLifecycleState::Stopped | SyncDeploymentLifecycleState::Failed => (
            CustomerEnvironmentHealthState::Unavailable,
            format!("SyncDeployment {deployment_id} is in {state:?}"),
        ),
    }
}

fn strongest_health_state(
    components: &[CustomerEnvironmentHealthComponent],
) -> CustomerEnvironmentHealthState {
    components
        .iter()
        .map(|component| component.state)
        .max_by_key(|state| health_score(*state))
        .unwrap_or(CustomerEnvironmentHealthState::Unavailable)
}

fn health_score(state: CustomerEnvironmentHealthState) -> u8 {
    match state {
        CustomerEnvironmentHealthState::Healthy => 0,
        CustomerEnvironmentHealthState::Degraded => 1,
        CustomerEnvironmentHealthState::Maintenance => 2,
        CustomerEnvironmentHealthState::AtRisk => 3,
        CustomerEnvironmentHealthState::Unavailable => 4,
    }
}

fn health_summary(state: CustomerEnvironmentHealthState) -> String {
    match state {
        CustomerEnvironmentHealthState::Healthy => "environment is healthy",
        CustomerEnvironmentHealthState::Degraded => {
            "environment is serving with degraded platform signals"
        }
        CustomerEnvironmentHealthState::Maintenance => "environment has planned maintenance active",
        CustomerEnvironmentHealthState::AtRisk => "environment has recoverability or capacity risk",
        CustomerEnvironmentHealthState::Unavailable => "environment is unavailable",
    }
    .to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BillingDelivery {
    delivery_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BillingDeliveryError {
    message: String,
}

impl std::fmt::Display for BillingDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

fn deliver_billing_export(
    export_id: &str,
    destination: &str,
    events: &[UsageEvent],
) -> Result<BillingDelivery, BillingDeliveryError> {
    let Some(dir) = local_jsonl_export_dir(destination) else {
        return Err(BillingDeliveryError {
            message: format!("unsupported billing export destination '{destination}'"),
        });
    };
    fs::create_dir_all(&dir).map_err(|err| BillingDeliveryError {
        message: format!("create billing export directory {}: {err}", dir.display()),
    })?;
    let path = dir.join(format!(
        "{}.jsonl",
        sanitize_identifier_component(export_id)
    ));
    let mut file = fs::File::create(&path).map_err(|err| BillingDeliveryError {
        message: format!("create billing export {}: {err}", path.display()),
    })?;
    for event in events {
        serde_json::to_writer(&mut file, event).map_err(|err| BillingDeliveryError {
            message: format!("encode billing export event: {err}"),
        })?;
        file.write_all(b"\n").map_err(|err| BillingDeliveryError {
            message: format!("write billing export {}: {err}", path.display()),
        })?;
    }
    file.sync_all().map_err(|err| BillingDeliveryError {
        message: format!("sync billing export {}: {err}", path.display()),
    })?;
    Ok(BillingDelivery {
        delivery_ref: path.display().to_string(),
    })
}

fn local_jsonl_export_dir(destination: &str) -> Option<PathBuf> {
    if destination == "local-jsonl" {
        return Some(
            std::env::var("PALIMPSEST_PAAS_BILLING_EXPORT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/tmp/palimpsest-paas-billing-exports")),
        );
    }
    destination
        .strip_prefix("local-jsonl:")
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
}

#[derive(Debug, Clone)]
pub struct SqlControlPlaneStore {
    pool: PgPool,
    secret_backend: SecretMaterialBackend,
}

impl SqlControlPlaneStore {
    pub fn new(pool: PgPool) -> Self {
        Self::with_secret_backend(pool, SecretMaterialBackend::default())
    }

    pub fn with_secret_backend(pool: PgPool, secret_backend: SecretMaterialBackend) -> Self {
        Self {
            pool,
            secret_backend,
        }
    }

    pub async fn insert_organization(
        &self,
        organization: &Organization,
    ) -> Result<(), SqlStoreError> {
        sqlx::query("INSERT INTO organizations (id, name) VALUES ($1, $2)")
            .bind(&organization.organization_id)
            .bind(&organization.name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert_project(&self, project: &Project) -> Result<(), SqlStoreError> {
        sqlx::query("INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, $3)")
            .bind(&project.project_id)
            .bind(&project.organization_id)
            .bind(&project.name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn project_scope(
        &self,
        project_id: &str,
    ) -> Result<Option<(String, String)>, SqlStoreError> {
        let row = sqlx::query("SELECT organization_id FROM projects WHERE id = $1")
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| {
            Ok::<_, SqlStoreError>((row.try_get("organization_id")?, project_id.to_owned()))
        })
        .transpose()
    }

    pub async fn insert_environment(&self, environment: &Environment) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO environments (id, organization_id, project_id, name, region) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&environment.environment_id)
        .bind(&environment.organization_id)
        .bind(&environment.project_id)
        .bind(&environment.name)
        .bind(&environment.region)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_onboarding_workspace(
        &self,
        actor_id: &str,
        organization: &Organization,
        project: &Project,
        environment: &Environment,
        owner_actor_id: &str,
    ) -> Result<TeamMembership, SqlStoreError> {
        if project.organization_id != organization.organization_id {
            return Err(SqlStoreError::InvalidValue(
                "project organization_id must match organization".to_owned(),
            ));
        }
        if environment.organization_id != organization.organization_id {
            return Err(SqlStoreError::InvalidValue(
                "environment organization_id must match organization".to_owned(),
            ));
        }
        if environment.project_id != project.project_id {
            return Err(SqlStoreError::InvalidValue(
                "environment project_id must match project".to_owned(),
            ));
        }
        if owner_actor_id.trim().is_empty() {
            return Err(SqlStoreError::InvalidValue(
                "owner_actor_id cannot be empty".to_owned(),
            ));
        }

        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO organizations (id, name) VALUES ($1, $2)")
            .bind(&organization.organization_id)
            .bind(&organization.name)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO projects (id, organization_id, name) VALUES ($1, $2, $3)")
            .bind(&project.project_id)
            .bind(&project.organization_id)
            .bind(&project.name)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO environments (id, organization_id, project_id, name, region) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&environment.environment_id)
        .bind(&environment.organization_id)
        .bind(&environment.project_id)
        .bind(&environment.name)
        .bind(&environment.region)
        .execute(&mut *tx)
        .await?;

        let membership = TeamMembership {
            organization_id: organization.organization_id.clone(),
            actor_id: owner_actor_id.to_owned(),
            role: TeamRole::Owner,
        };
        sqlx::query(
            "INSERT INTO team_memberships (organization_id, actor_id, role) \
             VALUES ($1, $2, 'owner')",
        )
        .bind(&membership.organization_id)
        .bind(&membership.actor_id)
        .execute(&mut *tx)
        .await?;

        let audit_prefix = format!("audit_onboarding_{}", unix_nanos());
        let audit_events = [
            ("organization.create", organization.organization_id.clone()),
            ("project.create", project.project_id.clone()),
            ("environment.create", environment.environment_id.clone()),
            (
                "team_membership.upsert",
                format!("{}:{owner_actor_id}", organization.organization_id),
            ),
        ]
        .into_iter();
        for (index, (action, resource_id)) in audit_events.enumerate() {
            sqlx::query(
                "INSERT INTO audit_events (id, actor_id, action, resource_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(format!("{audit_prefix}_{index}"))
            .bind(actor_id)
            .bind(action)
            .bind(&resource_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(membership)
    }

    pub async fn environment_scope(
        &self,
        environment_id: &str,
    ) -> Result<Option<(String, String, String)>, SqlStoreError> {
        let row = sqlx::query("SELECT organization_id, project_id FROM environments WHERE id = $1")
            .bind(environment_id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| {
            Ok::<_, SqlStoreError>((
                row.try_get("organization_id")?,
                row.try_get("project_id")?,
                environment_id.to_owned(),
            ))
        })
        .transpose()
    }

    pub async fn upsert_node_host(&self, host: &NodeHost) -> Result<(), SqlStoreError> {
        let state = node_host_state_label(host.state);
        let first_port = i32::from(host.first_port);
        let max_clusters = checked_i32(host.capacity.max_clusters, "max_clusters")?;
        let assigned_clusters = checked_i32(host.capacity.assigned_clusters, "assigned_clusters")?;
        let storage_gib = checked_i32(host.capacity.storage_gib, "storage_gib")?;
        let used_storage_gib = checked_i32(host.capacity.used_storage_gib, "used_storage_gib")?;

        sqlx::query(
            "INSERT INTO node_hosts \
                (id, region, failure_domain, data_root, first_port, state, max_clusters, assigned_clusters, storage_gib, used_storage_gib, last_heartbeat_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now()) \
             ON CONFLICT (id) DO UPDATE SET \
                region = EXCLUDED.region, \
                failure_domain = EXCLUDED.failure_domain, \
                data_root = EXCLUDED.data_root, \
                first_port = EXCLUDED.first_port, \
                state = EXCLUDED.state, \
                max_clusters = EXCLUDED.max_clusters, \
                assigned_clusters = EXCLUDED.assigned_clusters, \
                storage_gib = EXCLUDED.storage_gib, \
                used_storage_gib = EXCLUDED.used_storage_gib, \
                last_heartbeat_at = now()",
        )
        .bind(&host.host_id)
        .bind(&host.region)
        .bind(&host.failure_domain)
        .bind(&host.data_root)
        .bind(first_port)
        .bind(state)
        .bind(max_clusters)
        .bind(assigned_clusters)
        .bind(storage_gib)
        .bind(used_storage_gib)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn issue_node_host_agent_credential(
        &self,
        host_id: &str,
    ) -> Result<IssuedNodeAgentCredential, SqlStoreError> {
        let signing_key = generate_agent_signing_key()?;
        let signing_key_base64 = BASE64.encode(signing_key);
        let key_id = format!("{}:agent-signing:{}", host_id, unix_nanos());
        let secret_id = format!("agent-signing:{key_id}");
        let external_ref = format!("node-hosts/{host_id}/agent-signing/{key_id}");
        let sealed = self
            .secret_backend
            .seal(&external_ref, &signing_key_base64)?;

        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM node_hosts WHERE id = $1")
            .bind(host_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(host_id.to_owned()))?;

        sqlx::query(
            "UPDATE node_host_agent_credentials \
             SET state = 'rotated', rotated_at = now() \
             WHERE host_id = $1 AND state = 'active'",
        )
        .bind(host_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO secret_refs \
                (id, provider, external_ref, secret_material, encrypted_material, key_ref) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&secret_id)
        .bind(self.secret_backend.provider())
        .bind(&external_ref)
        .bind(sealed.secret_material.as_deref())
        .bind(sealed.encrypted_material.as_deref())
        .bind(sealed.key_ref.as_deref())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO node_host_agent_credentials \
                (id, host_id, secret_ref, state) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(&key_id)
        .bind(host_id)
        .bind(&secret_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(IssuedNodeAgentCredential {
            host_id: host_id.to_owned(),
            key_id,
            signing_key_base64,
        })
    }

    pub async fn node_host_agent_credentials(
        &self,
        host_id: &str,
        filter: &NodeHostAgentCredentialFilter,
    ) -> Result<Vec<NodeHostAgentCredential>, SqlStoreError> {
        let state = filter.state.map(node_host_agent_credential_state_label);
        let rows = sqlx::query(
            "SELECT id, host_id, state, created_at::text AS created_at, \
                    rotated_at::text AS rotated_at, revoked_at::text AS revoked_at, \
                    last_used_at::text AS last_used_at, last_used_operation \
             FROM node_host_agent_credentials \
             WHERE host_id = $1 \
                AND ($2::text IS NULL OR state = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(host_id)
        .bind(state)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(node_host_agent_credential_from_row)
            .collect()
    }

    pub async fn revoke_node_host_agent_credential(
        &self,
        host_id: &str,
        key_id: &str,
    ) -> Result<Option<NodeHostAgentCredential>, SqlStoreError> {
        let row = sqlx::query(
            "UPDATE node_host_agent_credentials \
             SET state = 'revoked', revoked_at = COALESCE(revoked_at, now()) \
             WHERE host_id = $1 AND id = $2 \
             RETURNING id, host_id, state, created_at::text AS created_at, \
                       rotated_at::text AS rotated_at, revoked_at::text AS revoked_at, \
                       last_used_at::text AS last_used_at, last_used_operation",
        )
        .bind(host_id)
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(node_host_agent_credential_from_row).transpose()
    }

    pub async fn insert_node_host_hardening_check(
        &self,
        check: &NodeHostHardeningCheck,
    ) -> Result<NodeHostHardeningCheck, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO node_host_hardening_checks \
                (id, host_id, status, image_ref, os_release, kernel_version, postgres_major_min, \
                 container_runtime, disk_encryption, firewall_enabled, unattended_upgrades, \
                 last_patched_at, checked_at, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::timestamptz, $13::timestamptz, $14) \
             RETURNING id, host_id, status, image_ref, os_release, kernel_version, postgres_major_min, \
                container_runtime, disk_encryption, firewall_enabled, unattended_upgrades, \
                last_patched_at::text AS last_patched_at, checked_at::text AS checked_at, error_message",
        )
        .bind(&check.check_id)
        .bind(&check.host_id)
        .bind(node_host_hardening_status_label(check.status))
        .bind(&check.image_ref)
        .bind(&check.os_release)
        .bind(&check.kernel_version)
        .bind(i32::from(check.postgres_major_min))
        .bind(&check.container_runtime)
        .bind(check.disk_encryption)
        .bind(check.firewall_enabled)
        .bind(check.unattended_upgrades)
        .bind(check.last_patched_at.as_deref())
        .bind(&check.checked_at)
        .bind(check.error_message.as_deref())
        .fetch_one(&self.pool)
        .await?;

        node_host_hardening_check_from_row(row)
    }

    pub async fn node_host_hardening_check(
        &self,
        host_id: &str,
        check_id: &str,
    ) -> Result<Option<NodeHostHardeningCheck>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, host_id, status, image_ref, os_release, kernel_version, postgres_major_min, \
                container_runtime, disk_encryption, firewall_enabled, unattended_upgrades, \
                last_patched_at::text AS last_patched_at, checked_at::text AS checked_at, error_message \
             FROM node_host_hardening_checks \
             WHERE host_id = $1 AND id = $2",
        )
        .bind(host_id)
        .bind(check_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(node_host_hardening_check_from_row).transpose()
    }

    pub async fn node_host_hardening_checks(
        &self,
        filter: &NodeHostHardeningCheckFilter<'_>,
    ) -> Result<Vec<NodeHostHardeningCheck>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, host_id, status, image_ref, os_release, kernel_version, postgres_major_min, \
                container_runtime, disk_encryption, firewall_enabled, unattended_upgrades, \
                last_patched_at::text AS last_patched_at, checked_at::text AS checked_at, error_message \
             FROM node_host_hardening_checks \
             WHERE host_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY checked_at DESC, id DESC",
        )
        .bind(filter.host_id)
        .bind(filter.status.map(node_host_hardening_status_label))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(node_host_hardening_check_from_row)
            .collect()
    }

    pub async fn record_node_host_agent_credential_use(
        &self,
        host_id: &str,
        key_id: &str,
        operation: &str,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "UPDATE node_host_agent_credentials \
             SET last_used_at = now(), last_used_operation = $3 \
             WHERE host_id = $1 AND id = $2 AND state = 'active'",
        )
        .bind(host_id)
        .bind(key_id)
        .bind(operation)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn node_host_agent_signing_key(
        &self,
        host_id: &str,
        key_id: &str,
    ) -> Result<Vec<u8>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT secret_refs.external_ref, \
                    secret_refs.secret_material, \
                    secret_refs.encrypted_material, \
                    secret_refs.key_ref \
             FROM node_host_agent_credentials \
             JOIN secret_refs ON secret_refs.id = node_host_agent_credentials.secret_ref \
             WHERE node_host_agent_credentials.host_id = $1 \
                AND node_host_agent_credentials.id = $2 \
                AND node_host_agent_credentials.state = 'active'",
        )
        .bind(host_id)
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SqlStoreError::MissingResource(key_id.to_owned()))?;

        let signing_key_base64 = self.secret_backend.open(
            row.try_get("external_ref")?,
            row.try_get("secret_material")?,
            row.try_get("encrypted_material")?,
            row.try_get("key_ref")?,
        )?;
        let signing_key = BASE64
            .decode(signing_key_base64.as_bytes())
            .map_err(|err| {
                SqlStoreError::InvalidValue(format!("decode agent signing key: {err}"))
            })?;
        if signing_key.len() < 32 {
            return Err(SqlStoreError::InvalidValue(format!(
                "agent signing key must be at least 32 bytes, got {}",
                signing_key.len()
            )));
        }
        Ok(signing_key)
    }

    pub async fn set_node_host_state(
        &self,
        host_id: &str,
        state: NodeHostState,
    ) -> Result<(), SqlStoreError> {
        let result = sqlx::query("UPDATE node_hosts SET state = $2 WHERE id = $1")
            .bind(host_id)
            .bind(node_host_state_label(state))
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(host_id.to_owned()));
        }
        Ok(())
    }

    pub async fn replace_node_host_observations(
        &self,
        host_id: &str,
        clusters: &[NodeHostClusterObservation],
        sync_deployments: &[NodeHostSyncDeploymentObservation],
    ) -> Result<(), SqlStoreError> {
        sqlx::query("DELETE FROM node_host_observed_clusters WHERE host_id = $1")
            .bind(host_id)
            .execute(&self.pool)
            .await?;
        for cluster in clusters {
            sqlx::query(
                "INSERT INTO node_host_observed_clusters \
                    (host_id, cluster_id, data_dir, postgres_running, observed_at) \
                 VALUES ($1, $2, $3, $4, now())",
            )
            .bind(host_id)
            .bind(&cluster.cluster_id)
            .bind(&cluster.data_dir)
            .bind(cluster.postgres_running)
            .execute(&self.pool)
            .await?;
        }

        sqlx::query("DELETE FROM node_host_observed_sync_deployments WHERE host_id = $1")
            .bind(host_id)
            .execute(&self.pool)
            .await?;
        for deployment in sync_deployments {
            sqlx::query(
                "INSERT INTO node_host_observed_sync_deployments \
                    (host_id, deployment_id, running, observed_at) \
                 VALUES ($1, $2, $3, now())",
            )
            .bind(host_id)
            .bind(&deployment.deployment_id)
            .bind(deployment.running)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn node_host(&self, host_id: &str) -> Result<Option<NodeHost>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, region, failure_domain, data_root, first_port, state, max_clusters, assigned_clusters, storage_gib, used_storage_gib \
             FROM node_hosts WHERE id = $1",
        )
        .bind(host_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(node_host_from_row).transpose()
    }

    pub async fn node_hosts(
        &self,
        filter: &NodeHostFilter<'_>,
    ) -> Result<Vec<NodeHost>, SqlStoreError> {
        let state = filter.state.map(node_host_state_label);
        let rows = sqlx::query(
            "SELECT id, region, failure_domain, data_root, first_port, state, max_clusters, assigned_clusters, storage_gib, used_storage_gib \
             FROM node_hosts \
             WHERE ($1::text IS NULL OR region = $1) \
               AND ($2::text IS NULL OR state = $2) \
               AND ($3::text IS NULL OR failure_domain = $3) \
             ORDER BY region, id",
        )
        .bind(filter.region)
        .bind(state)
        .bind(filter.failure_domain)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(node_host_from_row).collect()
    }

    pub async fn active_host_capacities(&self) -> Result<Vec<HostCapacity>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT node_hosts.id, \
                    node_hosts.data_root, \
                    node_hosts.first_port, \
                    node_hosts.max_clusters, \
                    GREATEST(node_hosts.assigned_clusters, COALESCE(assignments.assigned_clusters, 0)) AS assigned_clusters, \
                    COALESCE(assignments.used_ports, ARRAY[]::integer[]) AS used_ports, \
                    node_hosts.storage_gib, \
                    GREATEST(node_hosts.used_storage_gib, COALESCE(assignments.used_storage_gib, 0)) AS used_storage_gib \
             FROM node_hosts \
             LEFT JOIN ( \
                SELECT host_id, \
                       COUNT(*)::integer AS assigned_clusters, \
                       COALESCE(SUM(storage_gib), 0)::integer AS used_storage_gib, \
                       ARRAY_AGG(host_port ORDER BY host_port) FILTER (WHERE host_port IS NOT NULL) AS used_ports \
                FROM managed_postgres_clusters \
                WHERE host_id IS NOT NULL AND lifecycle_state <> 'deleted' \
                GROUP BY host_id \
             ) assignments ON assignments.host_id = node_hosts.id \
             WHERE node_hosts.state = 'active' \
             ORDER BY assigned_clusters, node_hosts.id",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let first_port: i32 = row.try_get("first_port")?;
                let max_clusters: i32 = row.try_get("max_clusters")?;
                let assigned_clusters: i32 = row.try_get("assigned_clusters")?;
                let used_ports: Vec<i32> = row.try_get("used_ports")?;
                let storage_gib: i32 = row.try_get("storage_gib")?;
                let used_storage_gib: i32 = row.try_get("used_storage_gib")?;
                Ok(HostCapacity {
                    host_id: row.try_get("id")?,
                    data_root: row.try_get("data_root")?,
                    first_port: u16::try_from(first_port).map_err(|_| {
                        SqlStoreError::InvalidValue(format!("invalid first_port {first_port}"))
                    })?,
                    max_clusters: usize::try_from(max_clusters).map_err(|_| {
                        SqlStoreError::InvalidValue(format!("invalid max_clusters {max_clusters}"))
                    })?,
                    assigned_clusters: usize::try_from(assigned_clusters).map_err(|_| {
                        SqlStoreError::InvalidValue(format!(
                            "invalid assigned_clusters {assigned_clusters}"
                        ))
                    })?,
                    used_ports: used_ports
                        .into_iter()
                        .map(|port| {
                            u16::try_from(port).map_err(|_| {
                                SqlStoreError::InvalidValue(format!("invalid host_port {port}"))
                            })
                        })
                        .collect::<Result<_, _>>()?,
                    storage_gib: u32::try_from(storage_gib).map_err(|_| {
                        SqlStoreError::InvalidValue(format!("invalid storage_gib {storage_gib}"))
                    })?,
                    used_storage_gib: u32::try_from(used_storage_gib).map_err(|_| {
                        SqlStoreError::InvalidValue(format!(
                            "invalid used_storage_gib {used_storage_gib}"
                        ))
                    })?,
                })
            })
            .collect()
    }

    pub async fn next_available_host_port(&self, host_id: &str) -> Result<u16, SqlStoreError> {
        let row = sqlx::query(
            "SELECT first_port, max_clusters \
             FROM node_hosts \
             WHERE id = $1",
        )
        .bind(host_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SqlStoreError::MissingResource(host_id.to_owned()))?;
        let first_port: i32 = row.try_get("first_port")?;
        let max_clusters: i32 = row.try_get("max_clusters")?;
        let first_port = u16::try_from(first_port)
            .map_err(|_| SqlStoreError::InvalidValue(format!("invalid first_port {first_port}")))?;
        let max_clusters = usize::try_from(max_clusters).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid max_clusters {max_clusters}"))
        })?;
        let rows = sqlx::query(
            "SELECT host_port \
             FROM managed_postgres_clusters \
             WHERE host_id = $1 \
               AND host_port IS NOT NULL \
               AND lifecycle_state <> 'deleted'",
        )
        .bind(host_id)
        .fetch_all(&self.pool)
        .await?;
        let mut used_ports = BTreeSet::new();
        for row in rows {
            let port: i32 = row.try_get("host_port")?;
            used_ports.insert(
                u16::try_from(port).map_err(|_| {
                    SqlStoreError::InvalidValue(format!("invalid host_port {port}"))
                })?,
            );
        }
        for offset in 0..max_clusters {
            let offset = u16::try_from(offset)
                .map_err(|_| SqlStoreError::InvalidValue("host port range exhausted".to_owned()))?;
            let port = first_port.checked_add(offset).ok_or_else(|| {
                SqlStoreError::InvalidValue("host port range exhausted".to_owned())
            })?;
            if !used_ports.contains(&port) {
                return Ok(port);
            }
        }
        Err(SqlStoreError::InvalidValue(format!(
            "node host {host_id} has no available Postgres ports"
        )))
    }

    pub async fn record_node_heartbeat(
        &self,
        host_id: &str,
        heartbeat: &NodeHostHeartbeat,
    ) -> Result<(), SqlStoreError> {
        let state = node_host_state_label(heartbeat.state);
        let max_clusters = checked_i32(heartbeat.capacity.max_clusters, "max_clusters")?;
        let assigned_clusters =
            checked_i32(heartbeat.capacity.assigned_clusters, "assigned_clusters")?;
        let storage_gib = checked_i32(heartbeat.capacity.storage_gib, "storage_gib")?;
        let used_storage_gib =
            checked_i32(heartbeat.capacity.used_storage_gib, "used_storage_gib")?;

        let result = sqlx::query(
            "UPDATE node_hosts SET \
                state = CASE \
                    WHEN state IN ('draining', 'maintenance', 'offline') THEN state \
                    ELSE $2 \
                END, \
                max_clusters = $3, \
                assigned_clusters = $4, \
                storage_gib = $5, \
                used_storage_gib = $6, \
                last_heartbeat_at = now() \
             WHERE id = $1",
        )
        .bind(host_id)
        .bind(state)
        .bind(max_clusters)
        .bind(assigned_clusters)
        .bind(storage_gib)
        .bind(used_storage_gib)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(host_id.to_owned()));
        }
        self.replace_node_host_observations(
            host_id,
            &heartbeat.observed_clusters,
            &heartbeat.observed_sync_deployments,
        )
        .await?;
        Ok(())
    }

    pub async fn upsert_config_version(&self, config: &ConfigVersion) -> Result<(), SqlStoreError> {
        let status = config_status_label(config.status);
        let result = sqlx::query(
            "INSERT INTO config_versions (id, environment_id, rendered_hash, status) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (id) DO UPDATE SET \
                rendered_hash = EXCLUDED.rendered_hash, \
                status = EXCLUDED.status, \
                updated_at = now() \
             WHERE config_versions.status <> 'deployed'",
        )
        .bind(&config.config_version)
        .bind(&config.environment_id)
        .bind(&config.rendered_hash)
        .bind(status)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::ImmutableResource(
                config.config_version.clone(),
            ));
        }
        Ok(())
    }

    pub async fn config_version(
        &self,
        config_version: &str,
    ) -> Result<Option<ConfigVersion>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, environment_id, rendered_hash, status \
             FROM config_versions WHERE id = $1",
        )
        .bind(config_version)
        .fetch_optional(&self.pool)
        .await?;

        row.map(config_version_from_row).transpose()
    }

    pub async fn config_versions_for_environment(
        &self,
        environment_id: &str,
    ) -> Result<Vec<ConfigVersion>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, environment_id, rendered_hash, status \
             FROM config_versions \
             WHERE environment_id = $1 \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(config_version_from_row).collect()
    }

    pub async fn rollback_environment_config(
        &self,
        environment_id: &str,
        rollback_config_version: &str,
    ) -> Result<ConfigVersion, SqlStoreError> {
        let row = sqlx::query(
            "WITH previous_deployed AS ( \
                SELECT environment_id, rendered_hash \
                FROM config_versions \
                WHERE environment_id = $1 AND status = 'deployed' \
                ORDER BY created_at DESC, id DESC \
                OFFSET 1 LIMIT 1 \
             ) \
             INSERT INTO config_versions (id, environment_id, rendered_hash, status) \
             SELECT $2, environment_id, rendered_hash, 'deployed' \
             FROM previous_deployed \
             RETURNING id, environment_id, rendered_hash, status",
        )
        .bind(environment_id)
        .bind(rollback_config_version)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(format!(
                "previous deployed config for environment {environment_id}"
            )));
        };
        config_version_from_row(row)
    }

    pub async fn insert_sync_deployment(
        &self,
        deployment: &SyncDeployment,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO sync_deployments \
                (id, organization_id, project_id, environment_id, managed_postgres_cluster_id, config_version, lifecycle_state) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&deployment.deployment_id)
        .bind(&deployment.organization_id)
        .bind(&deployment.project_id)
        .bind(&deployment.environment_id)
        .bind(&deployment.managed_postgres_cluster_id)
        .bind(&deployment.config_version)
        .bind(sync_deployment_lifecycle_state_label(
            deployment.lifecycle_state,
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn sync_deployment(
        &self,
        deployment_id: &str,
    ) -> Result<Option<SyncDeployment>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, managed_postgres_cluster_id, config_version, lifecycle_state \
             FROM sync_deployments WHERE id = $1",
        )
        .bind(deployment_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(sync_deployment_from_row).transpose()
    }

    pub async fn sync_deployments(
        &self,
        filter: &SyncDeploymentFilter<'_>,
    ) -> Result<Vec<SyncDeployment>, SqlStoreError> {
        let lifecycle_state = filter
            .lifecycle_state
            .map(sync_deployment_lifecycle_state_label);
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, managed_postgres_cluster_id, config_version, lifecycle_state \
             FROM sync_deployments \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR lifecycle_state = $4) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(lifecycle_state)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(sync_deployment_from_row).collect()
    }

    pub async fn update_sync_deployment_lifecycle_state(
        &self,
        deployment_id: &str,
        state: SyncDeploymentLifecycleState,
    ) -> Result<(), SqlStoreError> {
        let result = sqlx::query(
            "UPDATE sync_deployments SET lifecycle_state = $2, updated_at = now() WHERE id = $1",
        )
        .bind(deployment_id)
        .bind(sync_deployment_lifecycle_state_label(state))
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(deployment_id.to_owned()));
        }
        Ok(())
    }

    pub async fn ready_clusters_assigned_to_host(
        &self,
        host_id: &str,
    ) -> Result<Vec<ManagedPostgresCluster>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, host_id, region, \
                    postgres_version, tier, storage_gib, lifecycle_state, host_data_dir, host_port \
             FROM managed_postgres_clusters \
             WHERE host_id = $1 AND lifecycle_state = 'ready' \
             ORDER BY id",
        )
        .bind(host_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_cluster_from_row)
            .collect()
    }

    pub async fn desired_sync_deployments_assigned_to_host(
        &self,
        host_id: &str,
    ) -> Result<Vec<SyncDeployment>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT sync_deployments.id, sync_deployments.organization_id, \
                    sync_deployments.project_id, sync_deployments.environment_id, \
                    sync_deployments.managed_postgres_cluster_id, \
                    sync_deployments.config_version, sync_deployments.lifecycle_state \
             FROM sync_deployments \
             JOIN managed_postgres_clusters \
               ON managed_postgres_clusters.id = sync_deployments.managed_postgres_cluster_id \
             WHERE managed_postgres_clusters.host_id = $1 \
               AND managed_postgres_clusters.lifecycle_state = 'ready' \
               AND sync_deployments.lifecycle_state IN ('requested', 'starting', 'running') \
             ORDER BY sync_deployments.id",
        )
        .bind(host_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(sync_deployment_from_row).collect()
    }

    pub async fn pending_or_running_agent_command_exists(
        &self,
        host_id: &str,
        cluster_id: &str,
        action_kind: &str,
    ) -> Result<bool, SqlStoreError> {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS ( \
                SELECT 1 FROM agent_commands \
                WHERE host_id = $1 \
                  AND cluster_id = $2 \
                  AND status IN ('pending', 'running') \
                  AND action->>'kind' = $3 \
             )",
        )
        .bind(host_id)
        .bind(cluster_id)
        .bind(action_kind)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub async fn observed_cluster_running(
        &self,
        host_id: &str,
        cluster_id: &str,
        data_dir: &str,
    ) -> Result<Option<bool>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT postgres_running \
             FROM node_host_observed_clusters \
             WHERE host_id = $1 AND (cluster_id = $2 OR data_dir = $3) \
             ORDER BY observed_at DESC \
             LIMIT 1",
        )
        .bind(host_id)
        .bind(cluster_id)
        .bind(data_dir)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| row.try_get("postgres_running"))
            .transpose()
            .map_err(SqlStoreError::from)
    }

    pub async fn upsert_gateway_route(&self, route: &GatewayRoute) -> Result<(), SqlStoreError> {
        let (
            tls_policy,
            mtls_ca_secret_ref,
            mtls_client_certificate_secret_ref,
            mtls_client_private_key_secret_ref,
            mtls_server_name,
        ) = gateway_tls_policy_parts(&route.tls_policy);
        let max_connections = checked_i32(route.rate_limit.max_connections, "max_connections")?;
        let max_requests_per_minute = checked_i32(
            route.rate_limit.max_requests_per_minute,
            "max_requests_per_minute",
        )?;
        sqlx::query(
            "INSERT INTO gateway_routes \
                (host, organization_id, project_id, environment_id, sync_endpoint, tls_policy, \
                 mtls_ca_secret_ref, mtls_client_certificate_secret_ref, \
                 mtls_client_private_key_secret_ref, mtls_server_name, max_connections, \
                 max_requests_per_minute) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT (host) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                sync_endpoint = EXCLUDED.sync_endpoint, \
                tls_policy = EXCLUDED.tls_policy, \
                mtls_ca_secret_ref = EXCLUDED.mtls_ca_secret_ref, \
                mtls_client_certificate_secret_ref = EXCLUDED.mtls_client_certificate_secret_ref, \
                mtls_client_private_key_secret_ref = EXCLUDED.mtls_client_private_key_secret_ref, \
                mtls_server_name = EXCLUDED.mtls_server_name, \
                max_connections = EXCLUDED.max_connections, \
                max_requests_per_minute = EXCLUDED.max_requests_per_minute, \
                updated_at = now()",
        )
        .bind(normalize_gateway_host(&route.host)?)
        .bind(&route.organization_id)
        .bind(&route.project_id)
        .bind(&route.environment_id)
        .bind(&route.sync_endpoint)
        .bind(tls_policy)
        .bind(mtls_ca_secret_ref)
        .bind(mtls_client_certificate_secret_ref)
        .bind(mtls_client_private_key_secret_ref)
        .bind(mtls_server_name)
        .bind(max_connections)
        .bind(max_requests_per_minute)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn gateway_route(&self, host: &str) -> Result<Option<GatewayRoute>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT host, organization_id, project_id, environment_id, sync_endpoint, tls_policy, \
                    mtls_ca_secret_ref, mtls_client_certificate_secret_ref, \
                    mtls_client_private_key_secret_ref, mtls_server_name, max_connections, \
                    max_requests_per_minute \
             FROM gateway_routes WHERE host = $1",
        )
        .bind(normalize_gateway_host(host)?)
        .fetch_optional(&self.pool)
        .await?;

        row.map(gateway_route_from_row).transpose()
    }

    pub async fn delete_gateway_route(&self, host: &str) -> Result<bool, SqlStoreError> {
        let result = sqlx::query("DELETE FROM gateway_routes WHERE host = $1")
            .bind(normalize_gateway_host(host)?)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn gateway_routes(
        &self,
        filter: &GatewayRouteFilter<'_>,
    ) -> Result<Vec<GatewayRoute>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT host, organization_id, project_id, environment_id, sync_endpoint, tls_policy, \
                    mtls_ca_secret_ref, mtls_client_certificate_secret_ref, \
                    mtls_client_private_key_secret_ref, mtls_server_name, max_connections, \
                    max_requests_per_minute \
             FROM gateway_routes \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
             ORDER BY environment_id, host",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(gateway_route_from_row).collect()
    }

    pub async fn gateway_route_mtls_bundle(
        &self,
        host: &str,
    ) -> Result<Option<GatewayRouteMtlsBundle>, SqlStoreError> {
        let Some(route) = self.gateway_route(host).await? else {
            return Ok(None);
        };
        let TlsPolicy::MutualTlsToSync {
            ca_secret_ref,
            client_certificate_secret_ref,
            client_private_key_secret_ref,
            server_name,
        } = route.tls_policy
        else {
            return Err(SqlStoreError::InvalidValue(format!(
                "gateway route {} does not use mutual_tls_to_sync",
                route.host
            )));
        };
        let ca_pem = self.open_secret_material_by_id(&ca_secret_ref).await?;
        let client_certificate_pem = match client_certificate_secret_ref.as_deref() {
            Some(secret_ref) => Some(self.open_secret_material_by_id(secret_ref).await?),
            None => None,
        };
        let client_private_key_pem = match client_private_key_secret_ref.as_deref() {
            Some(secret_ref) => Some(self.open_secret_material_by_id(secret_ref).await?),
            None => None,
        };
        Ok(Some(GatewayRouteMtlsBundle {
            host: route.host,
            environment_id: route.environment_id,
            ca_secret_ref,
            ca_pem,
            client_certificate_secret_ref,
            client_certificate_pem,
            client_private_key_secret_ref,
            client_private_key_pem,
            server_name,
        }))
    }

    pub async fn upsert_domain(&self, domain: &Domain) -> Result<Domain, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO domains \
                (id, hostname, organization_id, project_id, environment_id, route_host, verification_status, verification_token, tls_status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (hostname) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                route_host = EXCLUDED.route_host, \
                verification_status = EXCLUDED.verification_status, \
                verification_token = EXCLUDED.verification_token, \
                tls_status = EXCLUDED.tls_status, \
                updated_at = now() \
             RETURNING id, hostname, organization_id, project_id, environment_id, route_host, verification_status, verification_token, tls_status",
        )
        .bind(&domain.domain_id)
        .bind(normalize_gateway_host(&domain.hostname)?)
        .bind(&domain.organization_id)
        .bind(&domain.project_id)
        .bind(&domain.environment_id)
        .bind(normalize_gateway_host(&domain.route_host)?)
        .bind(domain_verification_status_label(domain.verification_status))
        .bind(&domain.verification_token)
        .bind(domain_tls_status_label(domain.tls_status))
        .fetch_one(&self.pool)
        .await?;
        domain_from_row(row)
    }

    pub async fn domain(&self, hostname: &str) -> Result<Option<Domain>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, hostname, organization_id, project_id, environment_id, route_host, verification_status, verification_token, tls_status \
             FROM domains WHERE hostname = $1",
        )
        .bind(normalize_gateway_host(hostname)?)
        .fetch_optional(&self.pool)
        .await?;
        row.map(domain_from_row).transpose()
    }

    pub async fn delete_domain(&self, hostname: &str) -> Result<bool, SqlStoreError> {
        let result = sqlx::query("DELETE FROM domains WHERE hostname = $1")
            .bind(normalize_gateway_host(hostname)?)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn domains(&self, filter: &DomainFilter<'_>) -> Result<Vec<Domain>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, hostname, organization_id, project_id, environment_id, route_host, verification_status, verification_token, tls_status \
             FROM domains \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR verification_status = $4) \
               AND ($5::text IS NULL OR tls_status = $5) \
             ORDER BY environment_id, hostname",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.verification_status.map(domain_verification_status_label))
        .bind(filter.tls_status.map(domain_tls_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(domain_from_row).collect()
    }

    pub async fn upsert_ip_allowlist_rule(
        &self,
        rule: &IpAllowlistRule,
    ) -> Result<IpAllowlistRule, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO ip_allowlist_rules \
                (id, organization_id, project_id, environment_id, name, cidr, purpose, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (environment_id, cidr, purpose) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                name = EXCLUDED.name, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, name, cidr, purpose, status",
        )
        .bind(&rule.rule_id)
        .bind(&rule.organization_id)
        .bind(&rule.project_id)
        .bind(&rule.environment_id)
        .bind(&rule.name)
        .bind(&rule.cidr)
        .bind(ip_allowlist_purpose_label(rule.purpose))
        .bind(ip_allowlist_status_label(rule.status))
        .fetch_one(&self.pool)
        .await?;
        ip_allowlist_rule_from_row(row)
    }

    pub async fn ip_allowlist_rule(
        &self,
        rule_id: &str,
    ) -> Result<Option<IpAllowlistRule>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, cidr, purpose, status \
             FROM ip_allowlist_rules WHERE id = $1",
        )
        .bind(rule_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(ip_allowlist_rule_from_row).transpose()
    }

    pub async fn delete_ip_allowlist_rule(&self, rule_id: &str) -> Result<bool, SqlStoreError> {
        let result = sqlx::query("DELETE FROM ip_allowlist_rules WHERE id = $1")
            .bind(rule_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn ip_allowlist_rules(
        &self,
        filter: &IpAllowlistRuleFilter<'_>,
    ) -> Result<Vec<IpAllowlistRule>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, cidr, purpose, status \
             FROM ip_allowlist_rules \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR purpose = $4) \
               AND ($5::text IS NULL OR status = $5) \
             ORDER BY environment_id, purpose, cidr",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.purpose.map(ip_allowlist_purpose_label))
        .bind(filter.status.map(ip_allowlist_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(ip_allowlist_rule_from_row).collect()
    }

    pub async fn upsert_static_egress_ip(
        &self,
        egress_ip: &StaticEgressIp,
    ) -> Result<StaticEgressIp, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO static_egress_ips \
                (id, organization_id, project_id, environment_id, region, ip_address, provider_ref, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (environment_id, ip_address) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                region = EXCLUDED.region, \
                provider_ref = EXCLUDED.provider_ref, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, region, ip_address, provider_ref, status",
        )
        .bind(&egress_ip.egress_ip_id)
        .bind(&egress_ip.organization_id)
        .bind(&egress_ip.project_id)
        .bind(&egress_ip.environment_id)
        .bind(&egress_ip.region)
        .bind(&egress_ip.ip_address)
        .bind(&egress_ip.provider_ref)
        .bind(static_egress_ip_status_label(egress_ip.status))
        .fetch_one(&self.pool)
        .await?;
        static_egress_ip_from_row(row)
    }

    pub async fn static_egress_ip(
        &self,
        egress_ip_id: &str,
    ) -> Result<Option<StaticEgressIp>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, region, ip_address, provider_ref, status \
             FROM static_egress_ips WHERE id = $1",
        )
        .bind(egress_ip_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(static_egress_ip_from_row).transpose()
    }

    pub async fn delete_static_egress_ip(&self, egress_ip_id: &str) -> Result<bool, SqlStoreError> {
        let result = sqlx::query("DELETE FROM static_egress_ips WHERE id = $1")
            .bind(egress_ip_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn static_egress_ips(
        &self,
        filter: &StaticEgressIpFilter<'_>,
    ) -> Result<Vec<StaticEgressIp>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, region, ip_address, provider_ref, status \
             FROM static_egress_ips \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR region = $4) \
               AND ($5::text IS NULL OR status = $5) \
             ORDER BY environment_id, region, ip_address",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.region)
        .bind(filter.status.map(static_egress_ip_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(static_egress_ip_from_row).collect()
    }

    pub async fn upsert_maintenance_window(
        &self,
        window: &MaintenanceWindow,
    ) -> Result<MaintenanceWindow, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO maintenance_windows \
                (id, organization_id, project_id, environment_id, name, day_of_week, start_time, duration_minutes, auto_minor_upgrades, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (environment_id, day_of_week, start_time) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                name = EXCLUDED.name, \
                duration_minutes = EXCLUDED.duration_minutes, \
                auto_minor_upgrades = EXCLUDED.auto_minor_upgrades, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, name, day_of_week, start_time, duration_minutes, auto_minor_upgrades, status",
        )
        .bind(&window.window_id)
        .bind(&window.organization_id)
        .bind(&window.project_id)
        .bind(&window.environment_id)
        .bind(&window.name)
        .bind(maintenance_day_of_week_label(window.day_of_week))
        .bind(&window.start_time)
        .bind(checked_i32(window.duration_minutes, "duration_minutes")?)
        .bind(window.auto_minor_upgrades)
        .bind(maintenance_window_status_label(window.status))
        .fetch_one(&self.pool)
        .await?;
        maintenance_window_from_row(row)
    }

    pub async fn maintenance_window(
        &self,
        window_id: &str,
    ) -> Result<Option<MaintenanceWindow>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, day_of_week, start_time, duration_minutes, auto_minor_upgrades, status \
             FROM maintenance_windows WHERE id = $1",
        )
        .bind(window_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(maintenance_window_from_row).transpose()
    }

    pub async fn delete_maintenance_window(&self, window_id: &str) -> Result<bool, SqlStoreError> {
        let result = sqlx::query("DELETE FROM maintenance_windows WHERE id = $1")
            .bind(window_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn maintenance_windows(
        &self,
        filter: &MaintenanceWindowFilter<'_>,
    ) -> Result<Vec<MaintenanceWindow>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, day_of_week, start_time, duration_minutes, auto_minor_upgrades, status \
             FROM maintenance_windows \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR status = $4) \
             ORDER BY environment_id, day_of_week, start_time",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.status.map(maintenance_window_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(maintenance_window_from_row).collect()
    }

    pub async fn upsert_database_proxy_route(
        &self,
        route: &DatabaseProxyRoute,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO database_proxy_routes \
                (listen_addr, upstream_addr, organization_id, project_id, environment_id, cluster_id) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (listen_addr) DO UPDATE SET \
                upstream_addr = EXCLUDED.upstream_addr, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                cluster_id = EXCLUDED.cluster_id, \
                updated_at = now()",
        )
        .bind(normalize_endpoint_addr(&route.listen_addr, "listen_addr")?)
        .bind(normalize_endpoint_addr(&route.upstream_addr, "upstream_addr")?)
        .bind(&route.organization_id)
        .bind(&route.project_id)
        .bind(&route.environment_id)
        .bind(&route.cluster_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn database_proxy_route(
        &self,
        listen_addr: &str,
    ) -> Result<Option<DatabaseProxyRoute>, SqlStoreError> {
        let sql = database_proxy_route_select_sql(
            "database_proxy_routes.listen_addr = $1",
            "database_proxy_routes.listen_addr",
        );
        let row = sqlx::query(&sql)
            .bind(normalize_endpoint_addr(listen_addr, "listen_addr")?)
            .fetch_optional(&self.pool)
            .await?;

        row.map(database_proxy_route_from_row).transpose()
    }

    pub async fn delete_database_proxy_route(
        &self,
        listen_addr: &str,
    ) -> Result<bool, SqlStoreError> {
        let result = sqlx::query("DELETE FROM database_proxy_routes WHERE listen_addr = $1")
            .bind(normalize_endpoint_addr(listen_addr, "listen_addr")?)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn database_proxy_routes(
        &self,
        filter: &DatabaseProxyRouteFilter<'_>,
    ) -> Result<Vec<DatabaseProxyRoute>, SqlStoreError> {
        let sql = database_proxy_route_select_sql(
            "($1::text IS NULL OR database_proxy_routes.organization_id = $1) \
               AND ($2::text IS NULL OR database_proxy_routes.project_id = $2) \
               AND ($3::text IS NULL OR database_proxy_routes.environment_id = $3)",
            "database_proxy_routes.environment_id, database_proxy_routes.listen_addr",
        );
        let rows = sqlx::query(&sql)
            .bind(filter.organization_id)
            .bind(filter.project_id)
            .bind(filter.environment_id)
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(database_proxy_route_from_row)
            .collect()
    }

    pub async fn insert_managed_postgres_cluster(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<(), SqlStoreError> {
        if self
            .managed_postgres_deletion_tombstone(&cluster.cluster_id)
            .await?
            .is_some()
        {
            return Err(SqlStoreError::InvalidValue(format!(
                "managed Postgres cluster id '{}' is tombstoned",
                cluster.cluster_id
            )));
        }
        let storage_gib = checked_i32(cluster.storage_gib, "storage_gib")?;
        let lifecycle_state = lifecycle_state_label(cluster.lifecycle_state);
        let postgres_version = cluster.postgres_version.as_str();
        let (host_id, host_data_dir, host_port) =
            if let Some(assignment) = cluster.host_assignment.as_ref() {
                (
                    Some(assignment.host_id.as_str()),
                    Some(assignment.data_dir.as_str()),
                    Some(i32::from(assignment.port)),
                )
            } else {
                (None, None, None)
            };

        sqlx::query(
            "INSERT INTO managed_postgres_clusters \
                (id, organization_id, project_id, environment_id, host_id, region, \
                 postgres_version, tier, storage_gib, lifecycle_state, host_data_dir, host_port) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(&cluster.cluster_id)
        .bind(&cluster.organization_id)
        .bind(&cluster.project_id)
        .bind(&cluster.environment_id)
        .bind(host_id)
        .bind(&cluster.region)
        .bind(postgres_version)
        .bind(&cluster.tier)
        .bind(storage_gib)
        .bind(lifecycle_state)
        .bind(host_data_dir)
        .bind(host_port)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn initialize_managed_postgres_endpoint(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<ManagedPostgresEndpoint, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO managed_postgres_endpoints \
                (environment_id, active_cluster_id) \
             VALUES ($1, $2) \
             ON CONFLICT (environment_id) DO UPDATE SET \
                active_cluster_id = CASE \
                    WHEN EXISTS ( \
                        SELECT 1 FROM managed_postgres_clusters \
                        WHERE id = managed_postgres_endpoints.active_cluster_id \
                          AND lifecycle_state <> 'deleted' \
                    ) THEN managed_postgres_endpoints.active_cluster_id \
                    ELSE EXCLUDED.active_cluster_id \
                END, \
                updated_at = CASE \
                    WHEN EXISTS ( \
                        SELECT 1 FROM managed_postgres_clusters \
                        WHERE id = managed_postgres_endpoints.active_cluster_id \
                          AND lifecycle_state <> 'deleted' \
                    ) THEN managed_postgres_endpoints.updated_at \
                    ELSE now() \
                END \
             RETURNING environment_id, active_cluster_id, updated_by_failover_id, \
                database_proxy_listen_addr, active_certificate_id",
        )
        .bind(&cluster.environment_id)
        .bind(&cluster.cluster_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = row {
            return managed_postgres_endpoint_from_row(row);
        }
        self.managed_postgres_endpoint(&cluster.environment_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(cluster.environment_id.clone()))
    }

    pub async fn cutover_managed_postgres_endpoint(
        &self,
        environment_id: &str,
        active_cluster_id: &str,
        failover_id: &str,
    ) -> Result<ManagedPostgresEndpoint, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO managed_postgres_endpoints \
                (environment_id, active_cluster_id, updated_by_failover_id) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (environment_id) DO UPDATE SET \
                active_cluster_id = EXCLUDED.active_cluster_id, \
                updated_by_failover_id = EXCLUDED.updated_by_failover_id, \
                updated_at = now() \
             RETURNING environment_id, active_cluster_id, updated_by_failover_id, \
                database_proxy_listen_addr, active_certificate_id",
        )
        .bind(environment_id)
        .bind(active_cluster_id)
        .bind(failover_id)
        .fetch_one(&self.pool)
        .await?;
        let endpoint = managed_postgres_endpoint_from_row(row)?;
        if endpoint.database_proxy_listen_addr.is_some() {
            self.sync_database_proxy_route_for_endpoint(&endpoint)
                .await?;
        }
        Ok(endpoint)
    }

    pub async fn managed_postgres_endpoint(
        &self,
        environment_id: &str,
    ) -> Result<Option<ManagedPostgresEndpoint>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT environment_id, active_cluster_id, updated_by_failover_id, \
                database_proxy_listen_addr, active_certificate_id \
             FROM managed_postgres_endpoints WHERE environment_id = $1",
        )
        .bind(environment_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_endpoint_from_row).transpose()
    }

    pub async fn configure_managed_postgres_database_proxy_route(
        &self,
        environment_id: &str,
        listen_addr: &str,
    ) -> Result<(ManagedPostgresEndpoint, DatabaseProxyRoute), SqlStoreError> {
        let listen_addr = normalize_endpoint_addr(listen_addr, "listen_addr")?;
        let row = sqlx::query(
            "UPDATE managed_postgres_endpoints SET \
                database_proxy_listen_addr = $2, \
                updated_at = now() \
             WHERE environment_id = $1 \
             RETURNING environment_id, active_cluster_id, updated_by_failover_id, \
                database_proxy_listen_addr, active_certificate_id",
        )
        .bind(environment_id)
        .bind(&listen_addr)
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or_else(|| SqlStoreError::MissingResource(environment_id.to_owned()))?;
        let endpoint = managed_postgres_endpoint_from_row(row)?;
        let route = self
            .sync_database_proxy_route_for_endpoint(&endpoint)
            .await?;
        Ok((endpoint, route))
    }

    pub async fn deconfigure_managed_postgres_database_proxy_route(
        &self,
        environment_id: &str,
    ) -> Result<(ManagedPostgresEndpoint, Option<String>, Vec<String>), SqlStoreError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT environment_id, active_cluster_id, updated_by_failover_id, \
                database_proxy_listen_addr, active_certificate_id \
             FROM managed_postgres_endpoints \
             WHERE environment_id = $1 \
             FOR UPDATE",
        )
        .bind(environment_id)
        .fetch_optional(&mut *tx)
        .await?;
        let row = row.ok_or_else(|| SqlStoreError::MissingResource(environment_id.to_owned()))?;
        let removed_listen_addr: Option<String> = row.try_get("database_proxy_listen_addr")?;
        let mut revoked_certificate_ids = Vec::new();

        if let Some(listen_addr) = removed_listen_addr.as_ref() {
            sqlx::query("DELETE FROM database_proxy_routes WHERE listen_addr = $1")
                .bind(listen_addr)
                .execute(&mut *tx)
                .await?;
        }

        let revoked_rows = sqlx::query(
            "UPDATE managed_postgres_endpoint_certificates SET \
                status = 'revoked', \
                updated_at = now() \
             WHERE environment_id = $1 AND status = 'active' \
             RETURNING id",
        )
        .bind(environment_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in revoked_rows {
            revoked_certificate_ids.push(row.try_get("id")?);
        }

        let row = sqlx::query(
            "UPDATE managed_postgres_endpoints SET \
                database_proxy_listen_addr = NULL, \
                active_certificate_id = NULL, \
                updated_at = now() \
             WHERE environment_id = $1 \
             RETURNING environment_id, active_cluster_id, updated_by_failover_id, \
                database_proxy_listen_addr, active_certificate_id",
        )
        .bind(environment_id)
        .fetch_one(&mut *tx)
        .await?;
        let endpoint = managed_postgres_endpoint_from_row(row)?;
        tx.commit().await?;
        Ok((endpoint, removed_listen_addr, revoked_certificate_ids))
    }

    async fn sync_database_proxy_route_for_endpoint(
        &self,
        endpoint: &ManagedPostgresEndpoint,
    ) -> Result<DatabaseProxyRoute, SqlStoreError> {
        let listen_addr = endpoint.database_proxy_listen_addr.clone().ok_or_else(|| {
            SqlStoreError::InvalidValue(format!(
                "managed Postgres endpoint {} has no database proxy listen address",
                endpoint.environment_id
            ))
        })?;
        let cluster = self
            .managed_postgres_cluster(&endpoint.active_cluster_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(endpoint.active_cluster_id.clone()))?;
        let assignment = cluster.host_assignment.as_ref().ok_or_else(|| {
            SqlStoreError::InvalidValue(format!(
                "managed Postgres cluster {} has no host assignment",
                cluster.cluster_id
            ))
        })?;
        let upstream_host = std::env::var("PALIMPSEST_PAAS_DATABASE_PROXY_UPSTREAM_HOST")
            .or_else(|_| std::env::var("PALIMPSEST_PAAS_SQL_CONSOLE_HOST"))
            .unwrap_or_else(|_| "127.0.0.1".to_owned());
        let route = DatabaseProxyRoute {
            listen_addr,
            upstream_addr: format!("{}:{}", upstream_host.trim(), assignment.port),
            organization_id: cluster.organization_id,
            project_id: cluster.project_id,
            environment_id: cluster.environment_id,
            policy: managed_postgres_database_proxy_policy(&cluster.cluster_id),
            cluster_id: cluster.cluster_id,
            tls: None,
        };
        self.upsert_database_proxy_route(&route).await?;
        Ok(route)
    }

    pub async fn issue_managed_postgres_endpoint_certificate(
        &self,
        environment_id: &str,
        common_name: &str,
        validity_days: u32,
        ca_provider_id: Option<&str>,
        imported_material: Option<ImportedEndpointCertificateMaterial<'_>>,
    ) -> Result<ManagedPostgresEndpointCertificate, SqlStoreError> {
        let endpoint = self
            .managed_postgres_endpoint(environment_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(environment_id.to_owned()))?;
        let listen_addr = endpoint.database_proxy_listen_addr.ok_or_else(|| {
            SqlStoreError::InvalidValue(format!(
                "managed Postgres endpoint {environment_id} has no database proxy listen address"
            ))
        })?;
        let common_name = normalize_certificate_common_name(common_name)?;
        let certificate_id = format!("cert_{}", unix_nanos());
        let ca_provider = self
            .managed_postgres_certificate_authority_provider_for_issue(ca_provider_id)
            .await?;
        if ca_provider.status != CertificateAuthorityProviderStatus::Active {
            return Err(SqlStoreError::InvalidValue(format!(
                "certificate authority provider {} is not active",
                ca_provider.ca_provider_id
            )));
        }
        let not_before = unix_nanos().to_string();
        let seconds = u128::from(validity_days.max(1)) * 24 * 60 * 60;
        let not_after = (unix_nanos() + seconds * 1_000_000_000).to_string();
        let issued_by = ca_provider.issuer_ref.clone();
        if ca_provider.kind == CertificateAuthorityProviderKind::Acme {
            if imported_material.is_some() {
                return Err(SqlStoreError::InvalidValue(
                    "acme certificate authority providers create ACME orders; finalize with certificate_pem after challenge validation".to_owned(),
                ));
            }
            let pending = create_acme_pending_certificate(
                &common_name,
                &ca_provider.ca_provider_id,
                &ca_provider.issuer_ref,
            )?;
            let private_key_secret_ref = self
                .store_secret_material(
                    &format!("{certificate_id}:private-key"),
                    &format!("managed-postgres-endpoints/{environment_id}/certificates/{certificate_id}/private-key.pem"),
                    &pending.private_key_pem,
                )
                .await?;
            let csr_secret_ref = self
                .store_secret_material(
                    &format!("{certificate_id}:acme-csr"),
                    &format!("managed-postgres-endpoints/{environment_id}/certificates/{certificate_id}/acme.csr"),
                    &pending.csr_pem,
                )
                .await?;
            let key_authorization_secret_ref = self
                .store_secret_material(
                    &format!("{certificate_id}:acme-key-authorization"),
                    &format!("managed-postgres-endpoints/{environment_id}/certificates/{certificate_id}/http-01-key-authorization"),
                    &pending.key_authorization,
                )
                .await?;
            let certificate = ManagedPostgresEndpointCertificate {
                certificate_id: certificate_id.clone(),
                environment_id: environment_id.to_owned(),
                listen_addr,
                common_name: common_name.clone(),
                status: CertificateLifecycleState::Provisioning,
                certificate_secret_ref: None,
                private_key_secret_ref,
                not_before,
                not_after,
                fingerprint_sha256: sha256_base64(&pending.csr_pem),
                issued_by: issued_by.clone(),
                error_message: Some("ACME http-01 challenge is pending".to_owned()),
            };
            self.insert_managed_postgres_endpoint_certificate(&certificate)
                .await?;
            let order = ManagedPostgresAcmeOrder {
                order_id: format!("acme_order_{}", unix_nanos()),
                certificate_id: certificate_id.clone(),
                environment_id: environment_id.to_owned(),
                ca_provider_id: ca_provider.ca_provider_id.clone(),
                common_name,
                challenge_type: AcmeChallengeType::Http01,
                challenge_token: pending.challenge_token,
                key_authorization_secret_ref,
                csr_secret_ref,
                directory_url: ca_provider.issuer_ref,
                account_ref: ca_provider.ca_provider_id,
                status: AcmeOrderStatus::PendingChallenge,
                error_message: None,
                created_at: unix_nanos().to_string(),
                updated_at: unix_nanos().to_string(),
            };
            self.insert_managed_postgres_acme_order(&order).await?;
            return Ok(certificate);
        }

        let (certificate_pem, private_key_pem) = match ca_provider.kind {
            CertificateAuthorityProviderKind::LocalDev => {
                if imported_material.is_some() {
                    return Err(SqlStoreError::InvalidValue(
                        "local-dev certificate issuance does not accept imported PEM material"
                            .to_owned(),
                    ));
                }
                issue_local_dev_x509_certificate(&common_name)?
            }
            CertificateAuthorityProviderKind::ExternalPki => {
                let material = imported_material.ok_or_else(|| {
                    SqlStoreError::InvalidValue(
                        "external_pki certificate authority providers require certificate_pem and private_key_pem".to_owned(),
                    )
                })?;
                validate_pem_block("certificate_pem", material.certificate_pem, "CERTIFICATE")?;
                validate_pem_block("private_key_pem", material.private_key_pem, "PRIVATE KEY")?;
                (
                    material.certificate_pem.to_owned(),
                    material.private_key_pem.to_owned(),
                )
            }
            CertificateAuthorityProviderKind::Acme => unreachable!("acme handled above"),
        };
        let fingerprint_sha256 = sha256_base64(&certificate_pem);
        let certificate_secret_ref = self
            .store_secret_material(
                &format!("{certificate_id}:certificate"),
                &format!("managed-postgres-endpoints/{environment_id}/certificates/{certificate_id}/certificate.pem"),
                &certificate_pem,
            )
            .await?;
        let private_key_secret_ref = self
            .store_secret_material(
                &format!("{certificate_id}:private-key"),
                &format!("managed-postgres-endpoints/{environment_id}/certificates/{certificate_id}/private-key.pem"),
                &private_key_pem,
            )
            .await?;
        let certificate = ManagedPostgresEndpointCertificate {
            certificate_id,
            environment_id: environment_id.to_owned(),
            listen_addr,
            common_name,
            status: CertificateLifecycleState::Active,
            certificate_secret_ref: Some(certificate_secret_ref),
            private_key_secret_ref,
            not_before,
            not_after,
            fingerprint_sha256,
            issued_by,
            error_message: None,
        };
        self.insert_managed_postgres_endpoint_certificate(&certificate)
            .await?;
        Ok(certificate)
    }

    pub async fn renew_managed_postgres_endpoint_certificate(
        &self,
        environment_id: &str,
        renewal_window_days: u32,
        force: bool,
        validity_days: u32,
        ca_provider_id: Option<&str>,
        imported_material: Option<ImportedEndpointCertificateMaterial<'_>>,
    ) -> Result<ManagedPostgresEndpointCertificate, SqlStoreError> {
        let endpoint = self
            .managed_postgres_endpoint(environment_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(environment_id.to_owned()))?;
        let active_certificate_id = endpoint.active_certificate_id.ok_or_else(|| {
            SqlStoreError::InvalidValue(format!(
                "managed Postgres endpoint {environment_id} has no active certificate to renew"
            ))
        })?;
        let active = self
            .managed_postgres_endpoint_certificate(&active_certificate_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(active_certificate_id.clone()))?;
        if active.environment_id != environment_id
            || active.status != CertificateLifecycleState::Active
        {
            return Err(SqlStoreError::InvalidValue(format!(
                "managed Postgres endpoint {environment_id} active certificate {active_certificate_id} is not active"
            )));
        }
        if !force {
            let not_after = parse_unix_nanos("certificate not_after", &active.not_after)?;
            let renewal_window_nanos =
                u128::from(renewal_window_days.max(1)) * 24 * 60 * 60 * 1_000_000_000;
            let renewal_threshold = unix_nanos().saturating_add(renewal_window_nanos);
            if not_after > renewal_threshold {
                return Err(SqlStoreError::InvalidValue(format!(
                    "active certificate {active_certificate_id} is not within the {renewal_window_days}-day renewal window"
                )));
            }
        }

        self.issue_managed_postgres_endpoint_certificate(
            environment_id,
            &active.common_name,
            validity_days,
            ca_provider_id,
            imported_material,
        )
        .await
    }

    pub async fn upsert_managed_postgres_certificate_authority_provider(
        &self,
        provider: &ManagedPostgresCertificateAuthorityProvider,
    ) -> Result<ManagedPostgresCertificateAuthorityProvider, SqlStoreError> {
        normalize_non_empty("ca_provider_id", &provider.ca_provider_id)?;
        normalize_non_empty("ca provider name", &provider.name)?;
        normalize_non_empty("ca provider issuer_ref", &provider.issuer_ref)?;
        let mut tx = self.pool.begin().await?;
        if provider.default_for_managed_postgres {
            sqlx::query(
                "UPDATE managed_postgres_certificate_authority_providers \
                 SET default_for_managed_postgres = false, updated_at = now() \
                 WHERE default_for_managed_postgres = true AND id <> $1",
            )
            .bind(&provider.ca_provider_id)
            .execute(&mut *tx)
            .await?;
        }
        let row = sqlx::query(
            "INSERT INTO managed_postgres_certificate_authority_providers \
                (id, name, kind, issuer_ref, status, default_for_managed_postgres) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (id) DO UPDATE SET \
                name = EXCLUDED.name, \
                kind = EXCLUDED.kind, \
                issuer_ref = EXCLUDED.issuer_ref, \
                status = EXCLUDED.status, \
                default_for_managed_postgres = EXCLUDED.default_for_managed_postgres, \
                updated_at = now() \
             RETURNING id, name, kind, issuer_ref, status, default_for_managed_postgres, updated_at::text AS updated_at",
        )
        .bind(&provider.ca_provider_id)
        .bind(&provider.name)
        .bind(ca_provider_kind_label(provider.kind))
        .bind(&provider.issuer_ref)
        .bind(ca_provider_status_label(provider.status))
        .bind(provider.default_for_managed_postgres)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        managed_postgres_certificate_authority_provider_from_row(row)
    }

    pub async fn managed_postgres_certificate_authority_provider(
        &self,
        ca_provider_id: &str,
    ) -> Result<Option<ManagedPostgresCertificateAuthorityProvider>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, name, kind, issuer_ref, status, default_for_managed_postgres, updated_at::text AS updated_at \
             FROM managed_postgres_certificate_authority_providers WHERE id = $1",
        )
        .bind(ca_provider_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_certificate_authority_provider_from_row)
            .transpose()
    }

    pub async fn managed_postgres_certificate_authority_providers(
        &self,
        filter: &ManagedPostgresCertificateAuthorityProviderFilter,
    ) -> Result<Vec<ManagedPostgresCertificateAuthorityProvider>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, name, kind, issuer_ref, status, default_for_managed_postgres, updated_at::text AS updated_at \
             FROM managed_postgres_certificate_authority_providers \
             WHERE ($1::text IS NULL OR status = $1) \
               AND ($2::boolean IS NULL OR default_for_managed_postgres = $2) \
             ORDER BY default_for_managed_postgres DESC, name, id",
        )
        .bind(filter.status.map(ca_provider_status_label))
        .bind(filter.default_for_managed_postgres)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_certificate_authority_provider_from_row)
            .collect()
    }

    async fn managed_postgres_certificate_authority_provider_for_issue(
        &self,
        ca_provider_id: Option<&str>,
    ) -> Result<ManagedPostgresCertificateAuthorityProvider, SqlStoreError> {
        let row = if let Some(ca_provider_id) = ca_provider_id {
            sqlx::query(
                "SELECT id, name, kind, issuer_ref, status, default_for_managed_postgres, updated_at::text AS updated_at \
                 FROM managed_postgres_certificate_authority_providers WHERE id = $1",
            )
            .bind(ca_provider_id)
            .fetch_optional(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT id, name, kind, issuer_ref, status, default_for_managed_postgres, updated_at::text AS updated_at \
                 FROM managed_postgres_certificate_authority_providers \
                 WHERE default_for_managed_postgres = true \
                 ORDER BY updated_at DESC, id \
                 LIMIT 1",
            )
            .fetch_optional(&self.pool)
            .await?
        };
        row.map(managed_postgres_certificate_authority_provider_from_row)
            .transpose()?
            .ok_or_else(|| {
                SqlStoreError::MissingResource(
                    ca_provider_id
                        .unwrap_or("default certificate authority provider")
                        .to_owned(),
                )
            })
    }

    pub async fn managed_postgres_endpoint_certificate(
        &self,
        certificate_id: &str,
    ) -> Result<Option<ManagedPostgresEndpointCertificate>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT certs.id, certs.environment_id, certs.listen_addr, certs.common_name, \
                    certs.status, certs.certificate_secret_ref, certs.private_key_secret_ref, \
                    certs.not_before, certs.not_after, certs.fingerprint_sha256, certs.issued_by, \
                    certs.error_message, cert_secret.provider AS certificate_provider, \
                    cert_secret.external_ref AS certificate_external_ref, \
                    key_secret.provider AS private_key_provider, \
                    key_secret.external_ref AS private_key_external_ref \
             FROM managed_postgres_endpoint_certificates certs \
             LEFT JOIN secret_refs cert_secret ON cert_secret.id = certs.certificate_secret_ref \
             JOIN secret_refs key_secret ON key_secret.id = certs.private_key_secret_ref \
             WHERE certs.id = $1",
        )
        .bind(certificate_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_endpoint_certificate_from_row)
            .transpose()
    }

    pub async fn managed_postgres_endpoint_certificate_bundle(
        &self,
        certificate_id: &str,
    ) -> Result<Option<ManagedPostgresEndpointCertificateBundle>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT certs.id, certs.environment_id, certs.listen_addr, certs.common_name, \
                    certs.status, certs.certificate_secret_ref, certs.private_key_secret_ref, \
                    certs.not_before, certs.not_after, certs.fingerprint_sha256, certs.issued_by, \
                    certs.error_message, cert_secret.provider AS certificate_provider, \
                    cert_secret.external_ref AS certificate_external_ref, \
                    cert_secret.secret_material AS certificate_secret_material, \
                    cert_secret.encrypted_material AS certificate_encrypted_material, \
                    cert_secret.key_ref AS certificate_key_ref, \
                    key_secret.provider AS private_key_provider, \
                    key_secret.external_ref AS private_key_external_ref, \
                    key_secret.secret_material AS private_key_secret_material, \
                    key_secret.encrypted_material AS private_key_encrypted_material, \
                    key_secret.key_ref AS private_key_key_ref \
             FROM managed_postgres_endpoint_certificates certs \
             LEFT JOIN secret_refs cert_secret ON cert_secret.id = certs.certificate_secret_ref \
             JOIN secret_refs key_secret ON key_secret.id = certs.private_key_secret_ref \
             WHERE certs.id = $1 AND certs.status = 'active' AND certs.certificate_secret_ref IS NOT NULL",
        )
        .bind(certificate_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let certificate_external_ref: String = row.try_get("certificate_external_ref")?;
        let private_key_external_ref: String = row.try_get("private_key_external_ref")?;
        let certificate_pem = self.secret_backend.open(
            &certificate_external_ref,
            row.try_get("certificate_secret_material")?,
            row.try_get("certificate_encrypted_material")?,
            row.try_get("certificate_key_ref")?,
        )?;
        let private_key_pem = self.secret_backend.open(
            &private_key_external_ref,
            row.try_get("private_key_secret_material")?,
            row.try_get("private_key_encrypted_material")?,
            row.try_get("private_key_key_ref")?,
        )?;
        Ok(Some(ManagedPostgresEndpointCertificateBundle {
            certificate: managed_postgres_endpoint_certificate_from_row(row)?,
            certificate_pem,
            private_key_pem,
        }))
    }

    pub async fn managed_postgres_endpoint_certificates(
        &self,
        filter: &ManagedPostgresEndpointCertificateFilter<'_>,
    ) -> Result<Vec<ManagedPostgresEndpointCertificate>, SqlStoreError> {
        let status = filter.status.map(certificate_status_label);
        let rows = sqlx::query(
            "SELECT certs.id, certs.environment_id, certs.listen_addr, certs.common_name, \
                    certs.status, certs.certificate_secret_ref, certs.private_key_secret_ref, \
                    certs.not_before, certs.not_after, certs.fingerprint_sha256, certs.issued_by, \
                    certs.error_message, cert_secret.provider AS certificate_provider, \
                    cert_secret.external_ref AS certificate_external_ref, \
                    key_secret.provider AS private_key_provider, \
                    key_secret.external_ref AS private_key_external_ref \
             FROM managed_postgres_endpoint_certificates certs \
             LEFT JOIN secret_refs cert_secret ON cert_secret.id = certs.certificate_secret_ref \
             JOIN secret_refs key_secret ON key_secret.id = certs.private_key_secret_ref \
             WHERE ($1::text IS NULL OR certs.environment_id = $1) \
               AND ($2::text IS NULL OR certs.status = $2) \
             ORDER BY certs.created_at DESC, certs.id DESC",
        )
        .bind(filter.environment_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(managed_postgres_endpoint_certificate_from_row)
            .collect()
    }

    async fn insert_managed_postgres_endpoint_certificate(
        &self,
        certificate: &ManagedPostgresEndpointCertificate,
    ) -> Result<(), SqlStoreError> {
        let mut tx = self.pool.begin().await?;
        if certificate.status == CertificateLifecycleState::Active {
            sqlx::query(
                "UPDATE managed_postgres_endpoint_certificates SET \
                    status = 'revoked', \
                    updated_at = now() \
                 WHERE environment_id = $1 AND status = 'active'",
            )
            .bind(&certificate.environment_id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO managed_postgres_endpoint_certificates \
                (id, environment_id, listen_addr, common_name, status, certificate_secret_ref, \
                 private_key_secret_ref, not_before, not_after, fingerprint_sha256, issued_by, \
                 error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(&certificate.certificate_id)
        .bind(&certificate.environment_id)
        .bind(&certificate.listen_addr)
        .bind(&certificate.common_name)
        .bind(certificate_status_label(certificate.status))
        .bind(
            certificate
                .certificate_secret_ref
                .as_ref()
                .map(|secret| secret.secret_id.as_str()),
        )
        .bind(&certificate.private_key_secret_ref.secret_id)
        .bind(&certificate.not_before)
        .bind(&certificate.not_after)
        .bind(&certificate.fingerprint_sha256)
        .bind(&certificate.issued_by)
        .bind(certificate.error_message.as_deref())
        .execute(&mut *tx)
        .await?;
        if certificate.status == CertificateLifecycleState::Active {
            sqlx::query(
                "UPDATE managed_postgres_endpoints SET \
                    active_certificate_id = $2, \
                    updated_at = now() \
                 WHERE environment_id = $1",
            )
            .bind(&certificate.environment_id)
            .bind(&certificate.certificate_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn insert_managed_postgres_acme_order(
        &self,
        order: &ManagedPostgresAcmeOrder,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_acme_orders \
                (id, certificate_id, environment_id, ca_provider_id, common_name, \
                 challenge_type, challenge_token, key_authorization_secret_ref, \
                 csr_secret_ref, directory_url, account_ref, status, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(&order.order_id)
        .bind(&order.certificate_id)
        .bind(&order.environment_id)
        .bind(&order.ca_provider_id)
        .bind(&order.common_name)
        .bind(acme_challenge_type_label(order.challenge_type))
        .bind(&order.challenge_token)
        .bind(&order.key_authorization_secret_ref.secret_id)
        .bind(&order.csr_secret_ref.secret_id)
        .bind(&order.directory_url)
        .bind(&order.account_ref)
        .bind(acme_order_status_label(order.status))
        .bind(order.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_acme_order_for_certificate(
        &self,
        certificate_id: &str,
    ) -> Result<Option<ManagedPostgresAcmeOrder>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT orders.id, orders.certificate_id, orders.environment_id, \
                    orders.ca_provider_id, orders.common_name, orders.challenge_type, \
                    orders.challenge_token, orders.key_authorization_secret_ref, \
                    key_auth_secret.provider AS key_authorization_provider, \
                    key_auth_secret.external_ref AS key_authorization_external_ref, \
                    orders.csr_secret_ref, csr_secret.provider AS csr_provider, \
                    csr_secret.external_ref AS csr_external_ref, orders.directory_url, \
                    orders.account_ref, orders.status, orders.error_message, \
                    orders.created_at::text AS created_at, orders.updated_at::text AS updated_at \
             FROM managed_postgres_acme_orders orders \
             JOIN secret_refs key_auth_secret ON key_auth_secret.id = orders.key_authorization_secret_ref \
             JOIN secret_refs csr_secret ON csr_secret.id = orders.csr_secret_ref \
             WHERE orders.certificate_id = $1",
        )
        .bind(certificate_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_acme_order_from_row).transpose()
    }

    pub async fn pending_managed_postgres_acme_orders(
        &self,
        limit: i64,
    ) -> Result<Vec<ManagedPostgresAcmeOrder>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT certificate_id \
             FROM managed_postgres_acme_orders \
             WHERE status = 'pending_challenge' \
             ORDER BY created_at, id \
             LIMIT $1",
        )
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await?;
        let mut orders = Vec::with_capacity(rows.len());
        for row in rows {
            let certificate_id: String = row.try_get("certificate_id")?;
            if let Some(order) = self
                .managed_postgres_acme_order_for_certificate(&certificate_id)
                .await?
            {
                orders.push(order);
            }
        }
        Ok(orders)
    }

    pub async fn managed_postgres_acme_http_01_key_authorization(
        &self,
        challenge_token: &str,
    ) -> Result<Option<String>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT key_authorization_secret_ref \
             FROM managed_postgres_acme_orders \
             WHERE challenge_type = 'http_01' \
                AND challenge_token = $1 \
                AND status IN ('pending_challenge', 'ready_to_finalize') \
             ORDER BY created_at DESC \
             LIMIT 1",
        )
        .bind(challenge_token)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let secret_id: String = row.try_get("key_authorization_secret_ref")?;
        self.open_secret_material_by_id(&secret_id).await.map(Some)
    }

    pub async fn validate_managed_postgres_acme_http_01_challenge(
        &self,
        environment_id: &str,
        certificate_id: &str,
    ) -> Result<ManagedPostgresAcmeOrder, SqlStoreError> {
        let certificate = self
            .managed_postgres_endpoint_certificate(certificate_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(certificate_id.to_owned()))?;
        if certificate.environment_id != environment_id {
            return Err(SqlStoreError::MissingResource(certificate_id.to_owned()));
        }
        if certificate.status != CertificateLifecycleState::Provisioning {
            return Err(SqlStoreError::InvalidValue(format!(
                "certificate {certificate_id} is not provisioning"
            )));
        }
        let order = self
            .managed_postgres_acme_order_for_certificate(certificate_id)
            .await?
            .ok_or_else(|| {
                SqlStoreError::MissingResource(format!(
                    "ACME order for certificate {certificate_id}"
                ))
            })?;
        match order.status {
            AcmeOrderStatus::ReadyToFinalize => return Ok(order),
            AcmeOrderStatus::PendingChallenge => {}
            AcmeOrderStatus::Succeeded | AcmeOrderStatus::Failed => {
                return Err(SqlStoreError::InvalidValue(format!(
                    "ACME order {} is {:?}",
                    order.order_id, order.status
                )));
            }
        }
        let key_authorization = self
            .managed_postgres_acme_http_01_key_authorization(&order.challenge_token)
            .await?
            .ok_or_else(|| {
                SqlStoreError::InvalidValue(format!(
                    "ACME HTTP-01 challenge {} is not being served",
                    order.challenge_token
                ))
            })?;
        if !key_authorization.starts_with(&format!("{}.", order.challenge_token)) {
            return Err(SqlStoreError::InvalidValue(format!(
                "ACME HTTP-01 key authorization for {} does not match challenge token",
                order.challenge_token
            )));
        }
        sqlx::query(
            "UPDATE managed_postgres_acme_orders SET \
                status = 'ready_to_finalize', error_message = NULL, updated_at = now() \
             WHERE id = $1 AND status = 'pending_challenge'",
        )
        .bind(&order.order_id)
        .execute(&self.pool)
        .await?;
        self.managed_postgres_acme_order_for_certificate(certificate_id)
            .await?
            .ok_or_else(|| {
                SqlStoreError::MissingResource(format!(
                    "ACME order for certificate {certificate_id}"
                ))
            })
    }

    pub async fn finalize_managed_postgres_acme_order(
        &self,
        environment_id: &str,
        certificate_id: &str,
        certificate_pem: &str,
    ) -> Result<ManagedPostgresEndpointCertificate, SqlStoreError> {
        let certificate = self
            .managed_postgres_endpoint_certificate(certificate_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(certificate_id.to_owned()))?;
        if certificate.environment_id != environment_id {
            return Err(SqlStoreError::MissingResource(certificate_id.to_owned()));
        }
        if certificate.status != CertificateLifecycleState::Provisioning {
            return Err(SqlStoreError::InvalidValue(format!(
                "certificate {certificate_id} is not provisioning"
            )));
        }
        let order = self
            .managed_postgres_acme_order_for_certificate(certificate_id)
            .await?
            .ok_or_else(|| {
                SqlStoreError::MissingResource(format!(
                    "ACME order for certificate {certificate_id}"
                ))
            })?;
        if order.status != AcmeOrderStatus::ReadyToFinalize {
            return Err(SqlStoreError::InvalidValue(format!(
                "ACME order {} is not ready to finalize",
                order.order_id
            )));
        }
        if let Err(err) = validate_pem_block("certificate_pem", certificate_pem, "CERTIFICATE") {
            self.fail_managed_postgres_acme_order_finalization(certificate_id, &err.to_string())
                .await?;
            return Err(err);
        }
        let private_key_pem = self
            .open_secret_material_by_id(&certificate.private_key_secret_ref.secret_id)
            .await?;
        if let Err(err) = validate_certificate_key_pair(certificate_pem, &private_key_pem) {
            self.fail_managed_postgres_acme_order_finalization(certificate_id, &err.to_string())
                .await?;
            return Err(err);
        }
        let certificate_secret_ref = self
            .store_secret_material(
                &format!("{certificate_id}:certificate"),
                &format!("managed-postgres-endpoints/{environment_id}/certificates/{certificate_id}/certificate.pem"),
                certificate_pem,
            )
            .await?;
        let fingerprint_sha256 = sha256_base64(certificate_pem);
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE managed_postgres_endpoint_certificates SET \
                status = 'revoked', updated_at = now() \
             WHERE environment_id = $1 AND status = 'active'",
        )
        .bind(environment_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE managed_postgres_endpoint_certificates SET \
                status = 'active', certificate_secret_ref = $2, \
                fingerprint_sha256 = $3, error_message = NULL, updated_at = now() \
             WHERE id = $1",
        )
        .bind(certificate_id)
        .bind(&certificate_secret_ref.secret_id)
        .bind(&fingerprint_sha256)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE managed_postgres_acme_orders SET \
                status = 'succeeded', error_message = NULL, updated_at = now() \
             WHERE certificate_id = $1",
        )
        .bind(certificate_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE managed_postgres_endpoints SET active_certificate_id = $2, updated_at = now() \
             WHERE environment_id = $1",
        )
        .bind(environment_id)
        .bind(certificate_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.managed_postgres_endpoint_certificate(certificate_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(certificate_id.to_owned()))
    }

    async fn fail_managed_postgres_acme_order_finalization(
        &self,
        certificate_id: &str,
        error_message: &str,
    ) -> Result<(), SqlStoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE managed_postgres_acme_orders SET \
                status = 'failed', error_message = $2, updated_at = now() \
             WHERE certificate_id = $1 AND status IN ('pending_challenge', 'ready_to_finalize')",
        )
        .bind(certificate_id)
        .bind(error_message)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE managed_postgres_endpoint_certificates SET \
                status = 'failed', error_message = $2, updated_at = now() \
             WHERE id = $1 AND status = 'provisioning'",
        )
        .bind(certificate_id)
        .bind(error_message)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn store_secret_material(
        &self,
        secret_id: &str,
        external_ref: &str,
        material: &str,
    ) -> Result<SecretRef, SqlStoreError> {
        let sealed = self.secret_backend.seal(external_ref, material)?;
        sqlx::query(
            "INSERT INTO secret_refs \
                (id, provider, external_ref, secret_material, encrypted_material, key_ref) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (provider, external_ref) DO UPDATE SET \
                secret_material = EXCLUDED.secret_material, \
                encrypted_material = EXCLUDED.encrypted_material, \
                key_ref = EXCLUDED.key_ref",
        )
        .bind(secret_id)
        .bind(self.secret_backend.provider())
        .bind(external_ref)
        .bind(sealed.secret_material.as_deref())
        .bind(sealed.encrypted_material.as_deref())
        .bind(sealed.key_ref.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(SecretRef {
            secret_id: secret_id.to_owned(),
            provider: self.secret_backend.provider().to_owned(),
            external_ref: external_ref.to_owned(),
        })
    }

    async fn open_secret_material_by_id(&self, secret_id: &str) -> Result<String, SqlStoreError> {
        let row = sqlx::query(
            "SELECT external_ref, secret_material, encrypted_material, key_ref \
             FROM secret_refs \
             WHERE id = $1",
        )
        .bind(secret_id)
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or_else(|| SqlStoreError::MissingResource(secret_id.to_owned()))?;
        let external_ref: String = row.try_get("external_ref")?;
        self.secret_backend.open(
            &external_ref,
            row.try_get("secret_material")?,
            row.try_get("encrypted_material")?,
            row.try_get("key_ref")?,
        )
    }

    pub async fn insert_managed_postgres_deletion_tombstone(
        &self,
        cluster_id: &str,
        retention_days: Option<u32>,
    ) -> Result<(), SqlStoreError> {
        let Some(cluster) = self.managed_postgres_cluster(cluster_id).await? else {
            return Err(SqlStoreError::MissingResource(cluster_id.to_owned()));
        };
        let retained_backup = self.latest_succeeded_backup_for_cluster(cluster_id).await?;
        let retention_days = retention_days
            .map(|days| checked_i32(days, "retention_days"))
            .transpose()?;
        sqlx::query(
            "INSERT INTO managed_postgres_deletion_tombstones \
                (cluster_id, organization_id, project_id, environment_id, region, postgres_version, tier, retained_backup_id, retention_expires_at, unrecoverable_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $9::integer IS NULL THEN NULL ELSE now() + ($9::integer * interval '1 day') END, now()) \
             ON CONFLICT (cluster_id) DO NOTHING",
        )
        .bind(&cluster.cluster_id)
        .bind(&cluster.organization_id)
        .bind(&cluster.project_id)
        .bind(&cluster.environment_id)
        .bind(&cluster.region)
        .bind(cluster.postgres_version.as_str())
        .bind(&cluster.tier)
        .bind(retained_backup.as_ref().map(|backup| backup.backup_id.as_str()))
        .bind(retention_days)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_deletion_tombstone(
        &self,
        cluster_id: &str,
    ) -> Result<Option<ManagedPostgresDeletionTombstone>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT cluster_id, organization_id, project_id, environment_id, region, postgres_version, tier, retained_backup_id, \
                    deleted_at::text AS deleted_at, retention_expires_at::text AS retention_expires_at, unrecoverable_at::text AS unrecoverable_at, expired_at::text AS expired_at \
             FROM managed_postgres_deletion_tombstones WHERE cluster_id = $1",
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_deletion_tombstone_from_row)
            .transpose()
    }

    pub async fn managed_postgres_deletion_tombstones(
        &self,
        filter: &ManagedPostgresDeletionTombstoneFilter<'_>,
    ) -> Result<Vec<ManagedPostgresDeletionTombstone>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT cluster_id, organization_id, project_id, environment_id, region, postgres_version, tier, retained_backup_id, \
                    deleted_at::text AS deleted_at, retention_expires_at::text AS retention_expires_at, unrecoverable_at::text AS unrecoverable_at, expired_at::text AS expired_at \
             FROM managed_postgres_deletion_tombstones \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::boolean IS NULL OR ($4 = true AND expired_at IS NOT NULL) OR ($4 = false AND expired_at IS NULL)) \
             ORDER BY deleted_at DESC, cluster_id",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.expired)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_deletion_tombstone_from_row)
            .collect()
    }

    pub async fn expire_managed_postgres_deletion_tombstones(
        &self,
    ) -> Result<Vec<ManagedPostgresDeletionTombstone>, SqlStoreError> {
        let rows = sqlx::query(
            "UPDATE managed_postgres_deletion_tombstones \
             SET expired_at = now() \
             WHERE expired_at IS NULL \
               AND retention_expires_at IS NOT NULL \
               AND retention_expires_at <= now() \
             RETURNING cluster_id, organization_id, project_id, environment_id, region, postgres_version, tier, retained_backup_id, \
                       deleted_at::text AS deleted_at, retention_expires_at::text AS retention_expires_at, unrecoverable_at::text AS unrecoverable_at, expired_at::text AS expired_at",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_deletion_tombstone_from_row)
            .collect()
    }

    pub async fn managed_postgres_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<Option<ManagedPostgresCluster>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, host_id, region, \
                    postgres_version, tier, storage_gib, lifecycle_state, host_data_dir, host_port \
             FROM managed_postgres_clusters WHERE id = $1",
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_cluster_from_row).transpose()
    }

    pub async fn managed_postgres_clusters(
        &self,
        filter: &ManagedPostgresClusterFilter<'_>,
    ) -> Result<Vec<ManagedPostgresCluster>, SqlStoreError> {
        let lifecycle_state = filter.lifecycle_state.map(lifecycle_state_label);
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, host_id, region, \
                    postgres_version, tier, storage_gib, lifecycle_state, host_data_dir, host_port \
             FROM managed_postgres_clusters \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR lifecycle_state = $4) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(lifecycle_state)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(managed_postgres_cluster_from_row)
            .collect()
    }

    pub async fn ready_clusters_requiring_base_backup(
        &self,
    ) -> Result<Vec<ManagedPostgresCluster>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, host_id, region, \
                    postgres_version, tier, storage_gib, lifecycle_state, host_data_dir, host_port \
             FROM managed_postgres_clusters \
             WHERE lifecycle_state = 'ready' \
               AND NOT EXISTS ( \
                    SELECT 1 FROM managed_postgres_backups \
                    WHERE managed_postgres_backups.cluster_id = managed_postgres_clusters.id \
                      AND managed_postgres_backups.status IN ('requested', 'running', 'succeeded') \
               ) \
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(managed_postgres_cluster_from_row)
            .collect()
    }

    pub async fn update_managed_postgres_cluster(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<(), SqlStoreError> {
        let lifecycle_state = lifecycle_state_label(cluster.lifecycle_state);
        let storage_gib = checked_i32(cluster.storage_gib, "storage_gib")?;
        let (host_id, host_data_dir, host_port) =
            if let Some(assignment) = cluster.host_assignment.as_ref() {
                (
                    Some(assignment.host_id.as_str()),
                    Some(assignment.data_dir.as_str()),
                    Some(i32::from(assignment.port)),
                )
            } else {
                (None, None, None)
            };

        let result = sqlx::query(
            "UPDATE managed_postgres_clusters SET \
                host_id = $2, \
                lifecycle_state = $3, \
                host_data_dir = $4, \
                host_port = $5, \
                storage_gib = $6, \
                postgres_version = $7, \
                updated_at = now() \
             WHERE id = $1",
        )
        .bind(&cluster.cluster_id)
        .bind(host_id)
        .bind(lifecycle_state)
        .bind(host_data_dir)
        .bind(host_port)
        .bind(storage_gib)
        .bind(cluster.postgres_version.to_string())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(cluster.cluster_id.clone()));
        }
        Ok(())
    }

    pub async fn issue_database_role_credentials(
        &self,
        cluster_id: &str,
    ) -> Result<Vec<DatabaseRoleCredential>, SqlStoreError> {
        self.write_database_role_credentials(
            cluster_id,
            DatabaseRoleCredentialWriteMode::CanonicalInsert,
        )
        .await
    }

    pub async fn rotate_database_role_credentials(
        &self,
        cluster_id: &str,
    ) -> Result<PendingDatabaseRoleCredentialRotation, SqlStoreError> {
        let rotation_id = unix_nanos().to_string();
        let credentials = self
            .write_database_role_credentials(
                cluster_id,
                DatabaseRoleCredentialWriteMode::PendingRotation(&rotation_id),
            )
            .await?;
        sqlx::query(
            "INSERT INTO managed_postgres_role_credential_rotations \
                (id, cluster_id, status, pending_secret_prefix) \
             VALUES ($1, $2, 'pending', $3)",
        )
        .bind(&rotation_id)
        .bind(cluster_id)
        .bind(database_role_pending_secret_prefix(
            cluster_id,
            &rotation_id,
        ))
        .execute(&self.pool)
        .await?;
        Ok(PendingDatabaseRoleCredentialRotation {
            rotation_id,
            credentials,
        })
    }

    pub async fn attach_database_role_credential_rotation_command(
        &self,
        rotation_id: &str,
        command_id: &str,
    ) -> Result<(), SqlStoreError> {
        let result = sqlx::query(
            "UPDATE managed_postgres_role_credential_rotations \
             SET command_id = $2, status = 'applying', updated_at = now() \
             WHERE id = $1 AND status = 'pending'",
        )
        .bind(rotation_id)
        .bind(command_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(rotation_id.to_owned()));
        }
        Ok(())
    }

    async fn write_database_role_credentials(
        &self,
        cluster_id: &str,
        mode: DatabaseRoleCredentialWriteMode<'_>,
    ) -> Result<Vec<DatabaseRoleCredential>, SqlStoreError> {
        let prefix = sanitize_identifier_component(cluster_id);
        let specs = [
            (DatabaseRoleKind::App, "app"),
            (DatabaseRoleKind::Migration, "migration"),
            (DatabaseRoleKind::Replication, "replication"),
            (DatabaseRoleKind::Support, "support"),
        ];

        let mut credentials = Vec::with_capacity(specs.len());
        for (kind, suffix) in specs {
            let (secret_id, external_ref) = match mode {
                DatabaseRoleCredentialWriteMode::CanonicalInsert => (
                    self.secret_backend.secret_id(cluster_id, suffix),
                    database_role_canonical_external_ref(cluster_id, suffix),
                ),
                DatabaseRoleCredentialWriteMode::PendingRotation(rotation_id) => (
                    database_role_pending_secret_id(
                        self.secret_backend.provider(),
                        cluster_id,
                        rotation_id,
                        suffix,
                    ),
                    database_role_pending_external_ref(cluster_id, rotation_id, suffix),
                ),
            };
            let generated = generated_local_secret(cluster_id, suffix);
            let sealed = self.secret_backend.seal(&external_ref, &generated)?;
            sqlx::query(
                "INSERT INTO secret_refs \
                    (id, provider, external_ref, secret_material, encrypted_material, key_ref) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (provider, external_ref) DO NOTHING",
            )
            .bind(&secret_id)
            .bind(self.secret_backend.provider())
            .bind(&external_ref)
            .bind(sealed.secret_material.as_deref())
            .bind(sealed.encrypted_material.as_deref())
            .bind(sealed.key_ref.as_deref())
            .execute(&self.pool)
            .await?;

            let row = sqlx::query(
                "SELECT id, secret_material, encrypted_material, key_ref FROM secret_refs \
                 WHERE provider = $1 AND external_ref = $2",
            )
            .bind(self.secret_backend.provider())
            .bind(&external_ref)
            .fetch_one(&self.pool)
            .await?;
            let password = self.secret_backend.open(
                &external_ref,
                row.try_get("secret_material")?,
                row.try_get("encrypted_material")?,
                row.try_get("key_ref")?,
            )?;
            credentials.push(DatabaseRoleCredential {
                name: format!("{prefix}_{suffix}"),
                kind,
                password,
                privileges: Vec::new(),
            });
        }

        self.update_cluster_secret_refs(
            cluster_id,
            &self.secret_backend.secret_id(cluster_id, "app"),
            &self.secret_backend.secret_id(cluster_id, "replication"),
        )
        .await?;

        Ok(credentials)
    }

    pub async fn promote_pending_database_role_credential_rotation_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
    ) -> Result<bool, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action, operations.kind \
             FROM agent_commands \
             LEFT JOIN operations ON operations.id = agent_commands.operation_id \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };
        let operation_kind: Option<String> = row.try_get("kind")?;
        if operation_kind.as_deref() != Some("rotate_credentials") {
            return Ok(false);
        }
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        if !matches!(action.0, NodeAgentAction::ConfigurePostgresAccess { .. }) {
            return Ok(false);
        }
        let cluster_id: String = row.try_get("cluster_id")?;
        let rotation_row = sqlx::query(
            "SELECT id, cluster_id, status \
             FROM managed_postgres_role_credential_rotations \
             WHERE command_id = $1",
        )
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let (rotation_id, cluster_id, rotation_status) = if let Some(row) = rotation_row {
            (
                row.try_get::<String, _>("id")?,
                row.try_get::<String, _>("cluster_id")?,
                row.try_get::<String, _>("status")?,
            )
        } else {
            let Some(rotation_id) =
                database_role_rotation_id_from_command_id(&cluster_id, command_id)
            else {
                return Err(SqlStoreError::InvalidValue(format!(
                    "database role rotation command id has no rotation id: {command_id}"
                )));
            };
            (
                rotation_id.to_owned(),
                cluster_id,
                "legacy_command".to_owned(),
            )
        };

        if status != AgentCommandStatus::Succeeded {
            if rotation_status != "legacy_command" && rotation_status != "applied" {
                sqlx::query(
                    "UPDATE managed_postgres_role_credential_rotations \
                     SET status = 'failed', updated_at = now(), error_message = $2 \
                     WHERE id = $1",
                )
                .bind(&rotation_id)
                .bind(format!("node agent reported {status:?}"))
                .execute(&self.pool)
                .await?;
            }
            return Ok(false);
        }
        if rotation_status == "applied" {
            return Ok(true);
        }

        let specs = ["app", "migration", "replication", "support"];
        let mut promoted = Vec::with_capacity(specs.len());
        for suffix in specs {
            let pending_external_ref =
                database_role_pending_external_ref(&cluster_id, &rotation_id, suffix);
            let row = sqlx::query(
                "SELECT secret_material, encrypted_material, key_ref FROM secret_refs \
                 WHERE provider = $1 AND external_ref = $2",
            )
            .bind(self.secret_backend.provider())
            .bind(&pending_external_ref)
            .fetch_optional(&self.pool)
            .await?;
            let Some(row) = row else {
                return Ok(false);
            };
            let password = self.secret_backend.open(
                &pending_external_ref,
                row.try_get("secret_material")?,
                row.try_get("encrypted_material")?,
                row.try_get("key_ref")?,
            )?;
            let canonical_external_ref = database_role_canonical_external_ref(&cluster_id, suffix);
            let sealed = self
                .secret_backend
                .seal(&canonical_external_ref, &password)?;
            promoted.push((
                self.secret_backend.secret_id(&cluster_id, suffix),
                canonical_external_ref,
                sealed,
            ));
        }

        let mut tx = self.pool.begin().await?;
        for (secret_id, canonical_external_ref, sealed) in promoted {
            sqlx::query(
                "INSERT INTO secret_refs \
                    (id, provider, external_ref, secret_material, encrypted_material, key_ref) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (provider, external_ref) DO UPDATE SET \
                    secret_material = EXCLUDED.secret_material, \
                    encrypted_material = EXCLUDED.encrypted_material, \
                    key_ref = EXCLUDED.key_ref",
            )
            .bind(&secret_id)
            .bind(self.secret_backend.provider())
            .bind(&canonical_external_ref)
            .bind(sealed.secret_material.as_deref())
            .bind(sealed.encrypted_material.as_deref())
            .bind(sealed.key_ref.as_deref())
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "DELETE FROM secret_refs \
             WHERE provider = $1 AND external_ref LIKE $2",
        )
        .bind(self.secret_backend.provider())
        .bind(format!(
            "managed-postgres/{cluster_id}/credential-rotations/{rotation_id}/%/password"
        ))
        .execute(&mut *tx)
        .await?;
        if rotation_status != "legacy_command" {
            sqlx::query(
                "UPDATE managed_postgres_role_credential_rotations \
                 SET status = 'applied', updated_at = now(), applied_at = now(), error_message = NULL \
                 WHERE id = $1",
            )
            .bind(&rotation_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    pub async fn managed_postgres_app_endpoint(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<ManagedPostgresSqlEndpoint, SqlStoreError> {
        self.managed_postgres_role_endpoint(cluster, "app").await
    }

    pub async fn managed_postgres_support_endpoint(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<ManagedPostgresSqlEndpoint, SqlStoreError> {
        self.managed_postgres_role_endpoint(cluster, "support")
            .await
    }

    pub async fn managed_postgres_replication_endpoint(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<ManagedPostgresSqlEndpoint, SqlStoreError> {
        self.managed_postgres_role_endpoint(cluster, "replication")
            .await
    }

    pub async fn managed_postgres_migration_endpoint(
        &self,
        cluster: &ManagedPostgresCluster,
    ) -> Result<ManagedPostgresSqlEndpoint, SqlStoreError> {
        self.managed_postgres_role_endpoint(cluster, "migration")
            .await
    }

    /// Per-cluster Postgres role name for one of the standard suffixes
    /// (`app`, `migration`, `support`, `replication`). Matches the names
    /// the node agent provisions in [`render_role_sql`].
    pub fn managed_postgres_role_name(cluster_id: &str, role_suffix: &str) -> String {
        format!(
            "{}_{}",
            sanitize_identifier_component(cluster_id),
            role_suffix
        )
    }

    async fn managed_postgres_role_endpoint(
        &self,
        cluster: &ManagedPostgresCluster,
        role_suffix: &str,
    ) -> Result<ManagedPostgresSqlEndpoint, SqlStoreError> {
        if cluster.lifecycle_state != ClusterLifecycleState::Ready {
            return Err(SqlStoreError::InvalidValue(format!(
                "managed Postgres cluster {} is not ready",
                cluster.cluster_id
            )));
        }
        let assignment = cluster.host_assignment.as_ref().ok_or_else(|| {
            SqlStoreError::InvalidValue(format!(
                "managed Postgres cluster {} has no host assignment",
                cluster.cluster_id
            ))
        })?;
        let secret_id = self
            .secret_backend
            .secret_id(&cluster.cluster_id, role_suffix);
        let row = sqlx::query(
            "SELECT secret_refs.external_ref, \
                    secret_refs.secret_material, \
                    secret_refs.encrypted_material, \
                    secret_refs.key_ref \
             FROM secret_refs \
             WHERE secret_refs.id = $1",
        )
        .bind(&secret_id)
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or_else(|| {
            SqlStoreError::MissingResource(format!(
                "{role_suffix} secret for managed Postgres cluster {}",
                cluster.cluster_id,
            ))
        })?;
        let external_ref: String = row.try_get("external_ref")?;
        let password = self.secret_backend.open(
            &external_ref,
            row.try_get("secret_material")?,
            row.try_get("encrypted_material")?,
            row.try_get("key_ref")?,
        )?;
        let host = std::env::var("PALIMPSEST_PAAS_SQL_CONSOLE_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_owned());
        Ok(ManagedPostgresSqlEndpoint {
            host,
            port: assignment.port,
            database: "postgres".to_owned(),
            username: format!(
                "{}_{}",
                sanitize_identifier_component(&cluster.cluster_id),
                role_suffix
            ),
            password,
        })
    }

    async fn update_cluster_secret_refs(
        &self,
        cluster_id: &str,
        app_secret_ref: &str,
        replication_secret_ref: &str,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "UPDATE managed_postgres_clusters SET \
                app_secret_ref = $2, \
                replication_secret_ref = $3, \
                updated_at = now() \
             WHERE id = $1",
        )
        .bind(cluster_id)
        .bind(app_secret_ref)
        .bind(replication_secret_ref)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_managed_postgres_backup(
        &self,
        backup: &ManagedPostgresBackup,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_backups \
                (id, cluster_id, status, backup_dir, error_message) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&backup.backup_id)
        .bind(&backup.cluster_id)
        .bind(backup_status_label(backup.status))
        .bind(&backup.backup_dir)
        .bind(backup.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_managed_postgres_runtime_check(
        &self,
        check: &ManagedPostgresRuntimeCheck,
    ) -> Result<ManagedPostgresRuntimeCheck, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO managed_postgres_runtime_checks \
                (id, cluster_id, status, connection_count, max_connections, replication_slot_lag_bytes, \
                 long_running_query_count, blocked_lock_count, oldest_transaction_age_seconds, \
                 autovacuum_running, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             RETURNING id, cluster_id, status, connection_count, max_connections, replication_slot_lag_bytes, \
                long_running_query_count, blocked_lock_count, oldest_transaction_age_seconds, \
                autovacuum_running, checked_at::text AS checked_at, error_message",
        )
        .bind(&check.check_id)
        .bind(&check.cluster_id)
        .bind(runtime_check_status_label(check.status))
        .bind(checked_i32(check.connection_count, "connection_count")?)
        .bind(checked_i32(check.max_connections, "max_connections")?)
        .bind(check.replication_slot_lag_bytes.map(|value| checked_i64(value, "replication_slot_lag_bytes")).transpose()?)
        .bind(checked_i32(
            check.long_running_query_count,
            "long_running_query_count",
        )?)
        .bind(checked_i32(check.blocked_lock_count, "blocked_lock_count")?)
        .bind(
            check
                .oldest_transaction_age_seconds
                .map(|value| checked_i64(value, "oldest_transaction_age_seconds"))
                .transpose()?,
        )
        .bind(check.autovacuum_running)
        .bind(check.error_message.as_deref())
        .fetch_one(&self.pool)
        .await?;
        managed_postgres_runtime_check_from_row(row)
    }

    pub async fn managed_postgres_runtime_check(
        &self,
        check_id: &str,
    ) -> Result<Option<ManagedPostgresRuntimeCheck>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, cluster_id, status, connection_count, max_connections, replication_slot_lag_bytes, \
                long_running_query_count, blocked_lock_count, oldest_transaction_age_seconds, \
                autovacuum_running, checked_at::text AS checked_at, error_message \
             FROM managed_postgres_runtime_checks WHERE id = $1",
        )
        .bind(check_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_runtime_check_from_row).transpose()
    }

    pub async fn managed_postgres_runtime_checks(
        &self,
        filter: &ManagedPostgresRuntimeCheckFilter<'_>,
    ) -> Result<Vec<ManagedPostgresRuntimeCheck>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, cluster_id, status, connection_count, max_connections, replication_slot_lag_bytes, \
                long_running_query_count, blocked_lock_count, oldest_transaction_age_seconds, \
                autovacuum_running, checked_at::text AS checked_at, error_message \
             FROM managed_postgres_runtime_checks \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY checked_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(filter.status.map(runtime_check_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_runtime_check_from_row)
            .collect()
    }

    pub async fn insert_managed_postgres_major_upgrade(
        &self,
        upgrade: &ManagedPostgresMajorUpgrade,
    ) -> Result<ManagedPostgresMajorUpgrade, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO managed_postgres_major_upgrades \
                (id, cluster_id, source_postgres_version, target_postgres_version, strategy, status, command_id, operation_id, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING id, cluster_id, source_postgres_version, target_postgres_version, strategy, status, command_id, operation_id, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at",
        )
        .bind(&upgrade.upgrade_id)
        .bind(&upgrade.cluster_id)
        .bind(upgrade.source_postgres_version.as_str())
        .bind(upgrade.target_postgres_version.as_str())
        .bind(major_upgrade_strategy_label(upgrade.strategy))
        .bind(major_upgrade_status_label(upgrade.status))
        .bind(upgrade.command_id.as_deref())
        .bind(upgrade.operation_id.as_deref())
        .bind(upgrade.error_message.as_deref())
        .fetch_one(&self.pool)
        .await?;
        managed_postgres_major_upgrade_from_row(row)
    }

    pub async fn managed_postgres_major_upgrade(
        &self,
        upgrade_id: &str,
    ) -> Result<Option<ManagedPostgresMajorUpgrade>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, cluster_id, source_postgres_version, target_postgres_version, strategy, status, command_id, operation_id, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at \
             FROM managed_postgres_major_upgrades WHERE id = $1",
        )
        .bind(upgrade_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_major_upgrade_from_row).transpose()
    }

    pub async fn managed_postgres_major_upgrades(
        &self,
        filter: &ManagedPostgresMajorUpgradeFilter<'_>,
    ) -> Result<Vec<ManagedPostgresMajorUpgrade>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, cluster_id, source_postgres_version, target_postgres_version, strategy, status, command_id, operation_id, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at \
             FROM managed_postgres_major_upgrades \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(filter.status.map(major_upgrade_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_major_upgrade_from_row)
            .collect()
    }

    pub async fn managed_postgres_backup(
        &self,
        backup_id: &str,
    ) -> Result<Option<ManagedPostgresBackup>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, cluster_id, status, backup_dir, error_message \
             FROM managed_postgres_backups WHERE id = $1",
        )
        .bind(backup_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_backup_from_row).transpose()
    }

    pub async fn managed_postgres_backups(
        &self,
        filter: &ManagedPostgresBackupFilter<'_>,
    ) -> Result<Vec<ManagedPostgresBackup>, SqlStoreError> {
        let status = filter.status.map(backup_status_label);
        let rows = sqlx::query(
            "SELECT id, cluster_id, status, backup_dir, error_message \
             FROM managed_postgres_backups \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_backup_from_row)
            .collect()
    }

    pub async fn upsert_managed_postgres_backup_artifact(
        &self,
        artifact: &ManagedPostgresBackupArtifact,
    ) -> Result<ManagedPostgresBackupArtifact, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO managed_postgres_backup_artifacts \
                (id, backup_id, cluster_id, provider, object_uri, manifest_path, manifest_sha256, size_bytes, status, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (backup_id, provider, object_uri) DO UPDATE SET \
                id = EXCLUDED.id, \
                cluster_id = EXCLUDED.cluster_id, \
                manifest_path = EXCLUDED.manifest_path, \
                manifest_sha256 = EXCLUDED.manifest_sha256, \
                size_bytes = EXCLUDED.size_bytes, \
                status = EXCLUDED.status, \
                error_message = EXCLUDED.error_message, \
                updated_at = now() \
             RETURNING id, backup_id, cluster_id, provider, object_uri, manifest_path, manifest_sha256, size_bytes, status, \
                created_at::text AS created_at, updated_at::text AS updated_at, error_message",
        )
        .bind(&artifact.artifact_id)
        .bind(&artifact.backup_id)
        .bind(&artifact.cluster_id)
        .bind(normalize_non_empty("provider", &artifact.provider)?)
        .bind(normalize_non_empty("object_uri", &artifact.object_uri)?)
        .bind(normalize_non_empty("manifest_path", &artifact.manifest_path)?)
        .bind(artifact.manifest_sha256.as_deref())
        .bind(artifact.size_bytes.map(|value| checked_i64(value, "size_bytes")).transpose()?)
        .bind(backup_artifact_status_label(artifact.status))
        .bind(artifact.error_message.as_deref())
        .fetch_one(&self.pool)
        .await?;
        managed_postgres_backup_artifact_from_row(row)
    }

    pub async fn managed_postgres_backup_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<ManagedPostgresBackupArtifact>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, backup_id, cluster_id, provider, object_uri, manifest_path, manifest_sha256, size_bytes, status, \
                created_at::text AS created_at, updated_at::text AS updated_at, error_message \
             FROM managed_postgres_backup_artifacts WHERE id = $1",
        )
        .bind(artifact_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_backup_artifact_from_row)
            .transpose()
    }

    pub async fn managed_postgres_backup_artifacts(
        &self,
        filter: &ManagedPostgresBackupArtifactFilter<'_>,
    ) -> Result<Vec<ManagedPostgresBackupArtifact>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, backup_id, cluster_id, provider, object_uri, manifest_path, manifest_sha256, size_bytes, status, \
                created_at::text AS created_at, updated_at::text AS updated_at, error_message \
             FROM managed_postgres_backup_artifacts \
             WHERE backup_id = $1 \
               AND cluster_id = $2 \
               AND ($3::text IS NULL OR status = $3) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.backup_id)
        .bind(filter.cluster_id)
        .bind(filter.status.map(backup_artifact_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_backup_artifact_from_row)
            .collect()
    }

    pub async fn expire_backup_artifacts_for_backup(
        &self,
        backup_id: &str,
    ) -> Result<Vec<ManagedPostgresBackupArtifact>, SqlStoreError> {
        let rows = sqlx::query(
            "UPDATE managed_postgres_backup_artifacts \
             SET status = 'expired', error_message = NULL, updated_at = now() \
             WHERE backup_id = $1 AND status IN ('pending', 'available') \
             RETURNING id, backup_id, cluster_id, provider, object_uri, manifest_path, manifest_sha256, size_bytes, status, \
                created_at::text AS created_at, updated_at::text AS updated_at, error_message",
        )
        .bind(backup_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(managed_postgres_backup_artifact_from_row)
            .collect()
    }

    pub async fn mark_managed_postgres_backup_deleted(
        &self,
        backup_id: &str,
    ) -> Result<(), SqlStoreError> {
        let result = sqlx::query(
            "UPDATE managed_postgres_backups SET \
                status = 'deleted', \
                error_message = NULL, \
                completed_at = now() \
             WHERE id = $1",
        )
        .bind(backup_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(backup_id.to_owned()));
        }
        Ok(())
    }

    pub async fn upsert_managed_postgres_backup_retention_policy(
        &self,
        policy: &ManagedPostgresBackupRetentionPolicy,
    ) -> Result<ManagedPostgresBackupRetentionPolicy, SqlStoreError> {
        let retention_days = checked_i32(policy.retention_days, "retention_days")?;
        let keep_min_successful_backups = checked_i32(
            policy.keep_min_successful_backups,
            "keep_min_successful_backups",
        )?;
        let row = sqlx::query(
            "INSERT INTO managed_postgres_backup_retention_policies \
                (cluster_id, retention_days, keep_min_successful_backups, enabled) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (cluster_id) DO UPDATE SET \
                retention_days = EXCLUDED.retention_days, \
                keep_min_successful_backups = EXCLUDED.keep_min_successful_backups, \
                enabled = EXCLUDED.enabled, \
                updated_at = now() \
             RETURNING cluster_id, retention_days, keep_min_successful_backups, enabled, updated_at::text AS updated_at",
        )
        .bind(&policy.cluster_id)
        .bind(retention_days)
        .bind(keep_min_successful_backups)
        .bind(policy.enabled)
        .fetch_one(&self.pool)
        .await?;

        managed_postgres_backup_retention_policy_from_row(row)
    }

    pub async fn managed_postgres_backup_retention_policy(
        &self,
        cluster_id: &str,
    ) -> Result<Option<ManagedPostgresBackupRetentionPolicy>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT cluster_id, retention_days, keep_min_successful_backups, enabled, updated_at::text AS updated_at \
             FROM managed_postgres_backup_retention_policies \
             WHERE cluster_id = $1",
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_backup_retention_policy_from_row)
            .transpose()
    }

    pub async fn upsert_managed_postgres_clone_redaction_policy(
        &self,
        policy: &ManagedPostgresCloneRedactionPolicy,
    ) -> Result<ManagedPostgresCloneRedactionPolicy, SqlStoreError> {
        if policy.rules.is_empty() {
            return Err(SqlStoreError::InvalidValue(
                "clone redaction policy requires at least one rule".to_owned(),
            ));
        }
        let row = sqlx::query(
            "INSERT INTO managed_postgres_clone_redaction_policies \
                (id, organization_id, project_id, environment_id, name, status, rules) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (id) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                name = EXCLUDED.name, \
                status = EXCLUDED.status, \
                rules = EXCLUDED.rules, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, name, status, rules, \
                created_at::text AS created_at, updated_at::text AS updated_at",
        )
        .bind(&policy.policy_id)
        .bind(&policy.organization_id)
        .bind(&policy.project_id)
        .bind(&policy.environment_id)
        .bind(&policy.name)
        .bind(clone_redaction_policy_status_label(policy.status))
        .bind(Json(&policy.rules))
        .fetch_one(&self.pool)
        .await?;
        managed_postgres_clone_redaction_policy_from_row(row)
    }

    pub async fn managed_postgres_clone_redaction_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<ManagedPostgresCloneRedactionPolicy>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, status, rules, \
                created_at::text AS created_at, updated_at::text AS updated_at \
             FROM managed_postgres_clone_redaction_policies WHERE id = $1",
        )
        .bind(policy_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_clone_redaction_policy_from_row)
            .transpose()
    }

    pub async fn managed_postgres_clone_redaction_policies(
        &self,
        filter: &CloneRedactionPolicyFilter<'_>,
    ) -> Result<Vec<ManagedPostgresCloneRedactionPolicy>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, status, rules, \
                created_at::text AS created_at, updated_at::text AS updated_at \
             FROM managed_postgres_clone_redaction_policies \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR status = $4) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.status.map(clone_redaction_policy_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_clone_redaction_policy_from_row)
            .collect()
    }

    pub async fn create_managed_postgres_support_access_session(
        &self,
        session_id: &str,
        cluster: &ManagedPostgresCluster,
        requested_by: &str,
        reason: &str,
        ticket_ref: Option<&str>,
        duration_minutes: u32,
    ) -> Result<ManagedPostgresSupportAccessSession, SqlStoreError> {
        let duration_minutes = checked_i32(duration_minutes, "duration_minutes")?;
        let row = sqlx::query(
            "INSERT INTO managed_postgres_support_access_sessions \
                (id, cluster_id, organization_id, project_id, environment_id, requested_by, reason, ticket_ref, status, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'requested', now() + make_interval(mins => $9)) \
             RETURNING id, cluster_id, organization_id, project_id, environment_id, requested_by, approved_by, revoked_by, \
                reason, ticket_ref, status, requested_at::text AS requested_at, approved_at::text AS approved_at, \
                expires_at::text AS expires_at, revoked_at::text AS revoked_at",
        )
        .bind(session_id)
        .bind(&cluster.cluster_id)
        .bind(&cluster.organization_id)
        .bind(&cluster.project_id)
        .bind(&cluster.environment_id)
        .bind(requested_by)
        .bind(reason)
        .bind(ticket_ref)
        .bind(duration_minutes)
        .fetch_one(&self.pool)
        .await?;

        managed_postgres_support_access_session_from_row(row)
    }

    pub async fn managed_postgres_support_access_session(
        &self,
        session_id: &str,
    ) -> Result<Option<ManagedPostgresSupportAccessSession>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, cluster_id, organization_id, project_id, environment_id, requested_by, approved_by, revoked_by, \
                reason, ticket_ref, status, requested_at::text AS requested_at, approved_at::text AS approved_at, \
                expires_at::text AS expires_at, revoked_at::text AS revoked_at \
             FROM managed_postgres_support_access_sessions \
             WHERE id = $1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_support_access_session_from_row)
            .transpose()
    }

    pub async fn managed_postgres_support_access_sessions(
        &self,
        filter: &ManagedPostgresSupportAccessSessionFilter<'_>,
    ) -> Result<Vec<ManagedPostgresSupportAccessSession>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, cluster_id, organization_id, project_id, environment_id, requested_by, approved_by, revoked_by, \
                reason, ticket_ref, status, requested_at::text AS requested_at, approved_at::text AS approved_at, \
                expires_at::text AS expires_at, revoked_at::text AS revoked_at \
             FROM managed_postgres_support_access_sessions \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY requested_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(filter.status.map(support_access_status_label))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(managed_postgres_support_access_session_from_row)
            .collect()
    }

    pub async fn approve_managed_postgres_support_access_session(
        &self,
        session_id: &str,
        approved_by: &str,
    ) -> Result<Option<ManagedPostgresSupportAccessSession>, SqlStoreError> {
        let row = sqlx::query(
            "UPDATE managed_postgres_support_access_sessions \
             SET status = 'active', approved_by = $2, approved_at = now() \
             WHERE id = $1 AND status = 'requested' AND expires_at > now() \
             RETURNING id, cluster_id, organization_id, project_id, environment_id, requested_by, approved_by, revoked_by, \
                reason, ticket_ref, status, requested_at::text AS requested_at, approved_at::text AS approved_at, \
                expires_at::text AS expires_at, revoked_at::text AS revoked_at",
        )
        .bind(session_id)
        .bind(approved_by)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_support_access_session_from_row)
            .transpose()
    }

    pub async fn revoke_managed_postgres_support_access_session(
        &self,
        session_id: &str,
        revoked_by: &str,
    ) -> Result<Option<ManagedPostgresSupportAccessSession>, SqlStoreError> {
        let row = sqlx::query(
            "UPDATE managed_postgres_support_access_sessions \
             SET status = 'revoked', revoked_by = $2, revoked_at = now() \
             WHERE id = $1 AND status IN ('requested', 'active') \
             RETURNING id, cluster_id, organization_id, project_id, environment_id, requested_by, approved_by, revoked_by, \
                reason, ticket_ref, status, requested_at::text AS requested_at, approved_at::text AS approved_at, \
                expires_at::text AS expires_at, revoked_at::text AS revoked_at",
        )
        .bind(session_id)
        .bind(revoked_by)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_support_access_session_from_row)
            .transpose()
    }

    pub async fn upsert_secret_encryption_key(
        &self,
        key: &SecretEncryptionKey,
    ) -> Result<SecretEncryptionKey, SqlStoreError> {
        if key.key_ref.trim().is_empty()
            || key.provider.trim().is_empty()
            || key.purpose.trim().is_empty()
        {
            return Err(SqlStoreError::InvalidValue(
                "secret encryption key fields cannot be empty".to_owned(),
            ));
        }
        let row = sqlx::query(
            "INSERT INTO secret_encryption_keys \
                (key_ref, provider, purpose, status, activated_at, retired_at) \
             VALUES ($1, $2, $3, $4, $5::timestamptz, $6::timestamptz) \
             ON CONFLICT (key_ref) DO UPDATE SET \
                provider = EXCLUDED.provider, \
                purpose = EXCLUDED.purpose, \
                status = EXCLUDED.status, \
                activated_at = EXCLUDED.activated_at, \
                retired_at = EXCLUDED.retired_at \
             RETURNING key_ref, provider, purpose, status, created_at::text AS created_at, \
                activated_at::text AS activated_at, retired_at::text AS retired_at",
        )
        .bind(&key.key_ref)
        .bind(&key.provider)
        .bind(&key.purpose)
        .bind(secret_encryption_key_status_label(key.status))
        .bind(key.activated_at.as_deref())
        .bind(key.retired_at.as_deref())
        .fetch_one(&self.pool)
        .await?;

        secret_encryption_key_from_row(row)
    }

    pub async fn secret_encryption_key(
        &self,
        key_ref: &str,
    ) -> Result<Option<SecretEncryptionKey>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT key_ref, provider, purpose, status, created_at::text AS created_at, \
                activated_at::text AS activated_at, retired_at::text AS retired_at \
             FROM secret_encryption_keys \
             WHERE key_ref = $1",
        )
        .bind(key_ref)
        .fetch_optional(&self.pool)
        .await?;

        row.map(secret_encryption_key_from_row).transpose()
    }

    pub async fn secret_encryption_keys(
        &self,
        filter: &SecretEncryptionKeyFilter<'_>,
    ) -> Result<Vec<SecretEncryptionKey>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT key_ref, provider, purpose, status, created_at::text AS created_at, \
                activated_at::text AS activated_at, retired_at::text AS retired_at \
             FROM secret_encryption_keys \
             WHERE ($1::text IS NULL OR provider = $1) \
               AND ($2::text IS NULL OR purpose = $2) \
               AND ($3::text IS NULL OR status = $3) \
             ORDER BY created_at DESC, key_ref DESC",
        )
        .bind(filter.provider)
        .bind(filter.purpose)
        .bind(filter.status.map(secret_encryption_key_status_label))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(secret_encryption_key_from_row)
            .collect()
    }

    pub async fn create_secret_rewrap_plan(
        &self,
        plan_id: &str,
        source_key_ref: &str,
        target_key_ref: &str,
    ) -> Result<SecretRewrapPlan, SqlStoreError> {
        let matched_secret_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM secret_refs WHERE key_ref = $1")
                .bind(source_key_ref)
                .fetch_one(&self.pool)
                .await?;
        let matched_secret_count = u32::try_from(matched_secret_count)
            .map_err(|_| SqlStoreError::InvalidValue("matched_secret_count exceeds u32".to_owned()))
            .and_then(|count| checked_i32(count, "matched_secret_count"))?;
        let row = sqlx::query(
            "INSERT INTO secret_rewrap_plans \
                (id, source_key_ref, target_key_ref, status, matched_secret_count, rewrapped_secret_count) \
             VALUES ($1, $2, $3, 'planned', $4, 0) \
             RETURNING id, source_key_ref, target_key_ref, status, matched_secret_count, rewrapped_secret_count, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at",
        )
        .bind(plan_id)
        .bind(source_key_ref)
        .bind(target_key_ref)
        .bind(matched_secret_count)
        .fetch_one(&self.pool)
        .await?;

        secret_rewrap_plan_from_row(row)
    }

    pub async fn secret_rewrap_plan(
        &self,
        plan_id: &str,
    ) -> Result<Option<SecretRewrapPlan>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, source_key_ref, target_key_ref, status, matched_secret_count, rewrapped_secret_count, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at \
             FROM secret_rewrap_plans \
             WHERE id = $1",
        )
        .bind(plan_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(secret_rewrap_plan_from_row).transpose()
    }

    pub async fn secret_rewrap_plans(
        &self,
        filter: &SecretRewrapPlanFilter<'_>,
    ) -> Result<Vec<SecretRewrapPlan>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, source_key_ref, target_key_ref, status, matched_secret_count, rewrapped_secret_count, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at \
             FROM secret_rewrap_plans \
             WHERE ($1::text IS NULL OR source_key_ref = $1) \
               AND ($2::text IS NULL OR target_key_ref = $2) \
               AND ($3::text IS NULL OR status = $3) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.source_key_ref)
        .bind(filter.target_key_ref)
        .bind(filter.status.map(secret_rewrap_plan_status_label))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(secret_rewrap_plan_from_row).collect()
    }

    pub async fn run_secret_rewrap_plan(
        &self,
        plan_id: &str,
    ) -> Result<SecretRewrapPlan, SqlStoreError> {
        let plan = self
            .secret_rewrap_plan(plan_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(plan_id.to_owned()))?;
        match plan.status {
            SecretRewrapPlanStatus::Planned | SecretRewrapPlanStatus::Running => {}
            SecretRewrapPlanStatus::Succeeded => return Ok(plan),
            SecretRewrapPlanStatus::Failed => {
                return Err(SqlStoreError::InvalidValue(format!(
                    "secret rewrap plan {plan_id} has already failed"
                )));
            }
        }

        match self.try_run_secret_rewrap_plan(&plan).await {
            Ok(plan) => Ok(plan),
            Err(err) => {
                let message = err.to_string();
                self.fail_secret_rewrap_plan(plan_id, &message).await
            }
        }
    }

    async fn try_run_secret_rewrap_plan(
        &self,
        plan: &SecretRewrapPlan,
    ) -> Result<SecretRewrapPlan, SqlStoreError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE secret_rewrap_plans \
             SET status = 'running', error_message = NULL, completed_at = NULL \
             WHERE id = $1",
        )
        .bind(&plan.plan_id)
        .execute(&mut *tx)
        .await?;

        let rows = sqlx::query(
            "SELECT id, external_ref, secret_material, encrypted_material, key_ref \
             FROM secret_refs \
             WHERE key_ref = $1 \
             ORDER BY id \
             FOR UPDATE",
        )
        .bind(&plan.source_key_ref)
        .fetch_all(&mut *tx)
        .await?;

        let matched_secret_count = checked_i32(
            u32::try_from(rows.len()).map_err(|_| {
                SqlStoreError::InvalidValue("matched secret count exceeds u32".to_owned())
            })?,
            "matched_secret_count",
        )?;

        let mut rewrapped_secret_count = 0_i32;
        for row in rows {
            let secret_id: String = row.try_get("id")?;
            let external_ref: String = row.try_get("external_ref")?;
            let material = self.secret_backend.open(
                &external_ref,
                row.try_get("secret_material")?,
                row.try_get("encrypted_material")?,
                row.try_get("key_ref")?,
            )?;
            let sealed =
                self.secret_backend
                    .seal_for_key(&external_ref, &material, &plan.target_key_ref)?;
            sqlx::query(
                "UPDATE secret_refs \
                 SET secret_material = $2, encrypted_material = $3, key_ref = $4 \
                 WHERE id = $1",
            )
            .bind(&secret_id)
            .bind(sealed.secret_material.as_deref())
            .bind(sealed.encrypted_material.as_deref())
            .bind(sealed.key_ref.as_deref())
            .execute(&mut *tx)
            .await?;
            rewrapped_secret_count += 1;
        }

        let row = sqlx::query(
            "UPDATE secret_rewrap_plans \
             SET status = 'succeeded', matched_secret_count = $2, rewrapped_secret_count = $3, \
                error_message = NULL, completed_at = now() \
             WHERE id = $1 \
             RETURNING id, source_key_ref, target_key_ref, status, matched_secret_count, rewrapped_secret_count, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at",
        )
        .bind(&plan.plan_id)
        .bind(matched_secret_count)
        .bind(rewrapped_secret_count)
        .fetch_one(&mut *tx)
        .await?;
        let plan = secret_rewrap_plan_from_row(row)?;
        tx.commit().await?;
        Ok(plan)
    }

    async fn fail_secret_rewrap_plan(
        &self,
        plan_id: &str,
        error_message: &str,
    ) -> Result<SecretRewrapPlan, SqlStoreError> {
        let row = sqlx::query(
            "UPDATE secret_rewrap_plans \
             SET status = 'failed', error_message = $2, completed_at = now() \
             WHERE id = $1 \
             RETURNING id, source_key_ref, target_key_ref, status, matched_secret_count, rewrapped_secret_count, \
                error_message, created_at::text AS created_at, completed_at::text AS completed_at",
        )
        .bind(plan_id)
        .bind(error_message)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SqlStoreError::MissingResource(plan_id.to_owned()))?;

        secret_rewrap_plan_from_row(row)
    }

    pub async fn managed_postgres_backups_due_for_retention_expiration(
        &self,
    ) -> Result<Vec<ManagedPostgresBackup>, SqlStoreError> {
        let rows = sqlx::query(
            "WITH ranked AS ( \
                SELECT b.id, b.cluster_id, b.status, b.backup_dir, b.error_message, \
                    b.created_at, b.completed_at, p.retention_days, \
                    p.keep_min_successful_backups, \
                    row_number() OVER ( \
                        PARTITION BY b.cluster_id \
                        ORDER BY b.completed_at DESC NULLS LAST, b.created_at DESC, b.id DESC \
                    ) AS successful_rank \
                FROM managed_postgres_backups b \
                JOIN managed_postgres_backup_retention_policies p ON p.cluster_id = b.cluster_id \
                JOIN managed_postgres_clusters c ON c.id = b.cluster_id \
                WHERE b.status = 'succeeded' \
                  AND p.enabled = true \
                  AND c.lifecycle_state NOT IN ('deleting', 'deleted') \
             ) \
             SELECT id, cluster_id, status, backup_dir, error_message \
             FROM ranked \
             WHERE successful_rank > keep_min_successful_backups \
               AND COALESCE(completed_at, created_at) <= now() - make_interval(days => retention_days) \
             ORDER BY cluster_id, created_at, id",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(managed_postgres_backup_from_row)
            .collect()
    }

    pub async fn latest_succeeded_backup_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<Option<ManagedPostgresBackup>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, cluster_id, status, backup_dir, error_message \
             FROM managed_postgres_backups \
             WHERE cluster_id = $1 AND status = 'succeeded' \
             ORDER BY completed_at DESC NULLS LAST, created_at DESC \
             LIMIT 1",
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_backup_from_row).transpose()
    }

    pub async fn insert_managed_postgres_restore(
        &self,
        restore: &ManagedPostgresRestore,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_restores \
                (id, source_cluster_id, target_cluster_id, target_environment_id, backup_id, status, redaction_policy_id, recovery_target_lsn, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(&restore.restore_id)
        .bind(&restore.source_cluster_id)
        .bind(&restore.target_cluster_id)
        .bind(&restore.target_environment_id)
        .bind(&restore.backup_id)
        .bind(restore_status_label(restore.status))
        .bind(restore.redaction_policy_id.as_deref())
        .bind(restore.recovery_target_lsn.as_deref())
        .bind(restore.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_wal_archive_segment(
        &self,
        segment: &ManagedPostgresWalArchiveSegment,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_wal_archives \
                (cluster_id, segment_name, status, archive_dir, error_message) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (cluster_id, segment_name) DO UPDATE SET \
                status = EXCLUDED.status, \
                archive_dir = EXCLUDED.archive_dir, \
                error_message = EXCLUDED.error_message, \
                completed_at = NULL",
        )
        .bind(&segment.cluster_id)
        .bind(&segment.segment_name)
        .bind(wal_archive_segment_status_label(segment.status))
        .bind(&segment.archive_dir)
        .bind(segment.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn wal_archive_segment(
        &self,
        cluster_id: &str,
        segment_name: &str,
    ) -> Result<Option<ManagedPostgresWalArchiveSegment>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT cluster_id, segment_name, status, archive_dir, error_message \
             FROM managed_postgres_wal_archives \
             WHERE cluster_id = $1 AND segment_name = $2",
        )
        .bind(cluster_id)
        .bind(segment_name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(wal_archive_segment_from_row).transpose()
    }

    pub async fn wal_archive_segments(
        &self,
        filter: &WalArchiveSegmentFilter<'_>,
    ) -> Result<Vec<ManagedPostgresWalArchiveSegment>, SqlStoreError> {
        let status = filter.status.map(wal_archive_segment_status_label);
        let rows = sqlx::query(
            "SELECT cluster_id, segment_name, status, archive_dir, error_message \
             FROM managed_postgres_wal_archives \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, segment_name DESC",
        )
        .bind(filter.cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(wal_archive_segment_from_row).collect()
    }

    pub async fn insert_managed_postgres_pitr_check(
        &self,
        check: &ManagedPostgresPitrCheck,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_pitr_checks \
                (id, cluster_id, backup_id, status, segment_count, first_segment, latest_segment, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&check.check_id)
        .bind(&check.cluster_id)
        .bind(check.backup_id.as_deref())
        .bind(pitr_check_status_label(check.status))
        .bind(checked_i32(check.segment_count, "segment_count")?)
        .bind(check.first_segment.as_deref())
        .bind(check.latest_segment.as_deref())
        .bind(check.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_pitr_check(
        &self,
        check_id: &str,
    ) -> Result<Option<ManagedPostgresPitrCheck>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, cluster_id, backup_id, status, segment_count, first_segment, latest_segment, error_message \
             FROM managed_postgres_pitr_checks WHERE id = $1",
        )
        .bind(check_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_pitr_check_from_row).transpose()
    }

    pub async fn managed_postgres_pitr_checks(
        &self,
        filter: &ManagedPostgresPitrCheckFilter<'_>,
    ) -> Result<Vec<ManagedPostgresPitrCheck>, SqlStoreError> {
        let status = filter.status.map(pitr_check_status_label);
        let rows = sqlx::query(
            "SELECT id, cluster_id, backup_id, status, segment_count, first_segment, latest_segment, error_message \
             FROM managed_postgres_pitr_checks \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_pitr_check_from_row)
            .collect()
    }

    pub async fn clusters_due_for_pitr_check(
        &self,
        max_age_hours: i64,
    ) -> Result<Vec<ManagedPostgresCluster>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT managed_postgres_clusters.id, \
                    managed_postgres_clusters.organization_id, \
                    managed_postgres_clusters.project_id, \
                    managed_postgres_clusters.environment_id, \
                    managed_postgres_clusters.host_id, \
                    managed_postgres_clusters.region, \
                    managed_postgres_clusters.postgres_version, \
                    managed_postgres_clusters.tier, \
                    managed_postgres_clusters.storage_gib, \
                    managed_postgres_clusters.lifecycle_state, \
                    managed_postgres_clusters.host_data_dir, \
                    managed_postgres_clusters.host_port \
             FROM managed_postgres_clusters \
             WHERE managed_postgres_clusters.lifecycle_state = 'ready' \
               AND NOT EXISTS ( \
                    SELECT 1 FROM managed_postgres_pitr_checks \
                    WHERE managed_postgres_pitr_checks.cluster_id = managed_postgres_clusters.id \
                      AND managed_postgres_pitr_checks.status = 'succeeded' \
                      AND managed_postgres_pitr_checks.created_at >= now() - ($1::bigint * interval '1 hour') \
               ) \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(max_age_hours)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_cluster_from_row)
            .collect()
    }

    pub async fn insert_managed_postgres_failover(
        &self,
        failover: &ManagedPostgresFailover,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_failovers \
                (id, source_cluster_id, target_cluster_id, status, error_message) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&failover.failover_id)
        .bind(&failover.source_cluster_id)
        .bind(&failover.target_cluster_id)
        .bind(failover_status_label(failover.status))
        .bind(failover.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_failover(
        &self,
        failover_id: &str,
    ) -> Result<Option<ManagedPostgresFailover>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, source_cluster_id, target_cluster_id, status, error_message \
             FROM managed_postgres_failovers WHERE id = $1",
        )
        .bind(failover_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_failover_from_row).transpose()
    }

    pub async fn managed_postgres_failovers(
        &self,
        filter: &ManagedPostgresFailoverFilter<'_>,
    ) -> Result<Vec<ManagedPostgresFailover>, SqlStoreError> {
        let status = filter.status.map(failover_status_label);
        let rows = sqlx::query(
            "SELECT id, source_cluster_id, target_cluster_id, status, error_message \
             FROM managed_postgres_failovers \
             WHERE source_cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.source_cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_failover_from_row)
            .collect()
    }

    pub async fn insert_managed_postgres_standby(
        &self,
        standby: &ManagedPostgresStandby,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_standbys \
                (id, source_cluster_id, target_cluster_id, backup_id, status, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&standby.standby_id)
        .bind(&standby.source_cluster_id)
        .bind(&standby.target_cluster_id)
        .bind(&standby.backup_id)
        .bind(standby_status_label(standby.status))
        .bind(standby.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_standby(
        &self,
        standby_id: &str,
    ) -> Result<Option<ManagedPostgresStandby>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, source_cluster_id, target_cluster_id, backup_id, status, error_message \
             FROM managed_postgres_standbys WHERE id = $1",
        )
        .bind(standby_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_standby_from_row).transpose()
    }

    pub async fn managed_postgres_standbys(
        &self,
        filter: &ManagedPostgresStandbyFilter<'_>,
    ) -> Result<Vec<ManagedPostgresStandby>, SqlStoreError> {
        let status = filter.status.map(standby_status_label);
        let rows = sqlx::query(
            "SELECT id, source_cluster_id, target_cluster_id, backup_id, status, error_message \
             FROM managed_postgres_standbys \
             WHERE source_cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.source_cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_standby_from_row)
            .collect()
    }

    pub async fn insert_managed_postgres_standby_check(
        &self,
        check: &ManagedPostgresStandbyCheck,
    ) -> Result<(), SqlStoreError> {
        let max_lag_bytes = i64::try_from(check.max_lag_bytes).map_err(|_| {
            SqlStoreError::InvalidValue("standby check max_lag_bytes exceeds bigint".to_owned())
        })?;
        sqlx::query(
            "INSERT INTO managed_postgres_standby_checks \
                (id, standby_id, source_cluster_id, target_cluster_id, slot_name, max_lag_bytes, status, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&check.check_id)
        .bind(&check.standby_id)
        .bind(&check.source_cluster_id)
        .bind(&check.target_cluster_id)
        .bind(&check.slot_name)
        .bind(max_lag_bytes)
        .bind(standby_check_status_label(check.status))
        .bind(check.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_standby_check(
        &self,
        check_id: &str,
    ) -> Result<Option<ManagedPostgresStandbyCheck>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, standby_id, source_cluster_id, target_cluster_id, slot_name, max_lag_bytes, status, error_message \
             FROM managed_postgres_standby_checks WHERE id = $1",
        )
        .bind(check_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_standby_check_from_row).transpose()
    }

    pub async fn managed_postgres_standby_checks(
        &self,
        filter: &ManagedPostgresStandbyCheckFilter<'_>,
    ) -> Result<Vec<ManagedPostgresStandbyCheck>, SqlStoreError> {
        let status = filter.status.map(standby_check_status_label);
        let rows = sqlx::query(
            "SELECT id, standby_id, source_cluster_id, target_cluster_id, slot_name, max_lag_bytes, status, error_message \
             FROM managed_postgres_standby_checks \
             WHERE standby_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.standby_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_standby_check_from_row)
            .collect()
    }

    pub async fn managed_postgres_restore(
        &self,
        restore_id: &str,
    ) -> Result<Option<ManagedPostgresRestore>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, source_cluster_id, target_cluster_id, target_environment_id, backup_id, status, redaction_policy_id, recovery_target_lsn, error_message \
             FROM managed_postgres_restores WHERE id = $1",
        )
        .bind(restore_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(managed_postgres_restore_from_row).transpose()
    }

    pub async fn managed_postgres_restores(
        &self,
        filter: &ManagedPostgresRestoreFilter<'_>,
    ) -> Result<Vec<ManagedPostgresRestore>, SqlStoreError> {
        let status = filter.status.map(restore_status_label);
        let rows = sqlx::query(
            "SELECT id, source_cluster_id, target_cluster_id, target_environment_id, backup_id, status, redaction_policy_id, recovery_target_lsn, error_message \
             FROM managed_postgres_restores \
             WHERE source_cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.source_cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_restore_from_row)
            .collect()
    }

    pub async fn insert_managed_postgres_restore_drill(
        &self,
        drill: &ManagedPostgresRestoreDrill,
    ) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO managed_postgres_restore_drills \
                (id, source_cluster_id, restore_id, backup_id, target_cluster_id, status, error_message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&drill.drill_id)
        .bind(&drill.source_cluster_id)
        .bind(&drill.restore_id)
        .bind(&drill.backup_id)
        .bind(&drill.target_cluster_id)
        .bind(restore_status_label(drill.status))
        .bind(drill.error_message.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn managed_postgres_restore_drill(
        &self,
        drill_id: &str,
    ) -> Result<Option<ManagedPostgresRestoreDrill>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, source_cluster_id, restore_id, backup_id, target_cluster_id, status, error_message \
             FROM managed_postgres_restore_drills \
             WHERE id = $1",
        )
        .bind(drill_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_restore_drill_from_row).transpose()
    }

    pub async fn managed_postgres_restore_drills(
        &self,
        filter: &ManagedPostgresRestoreDrillFilter<'_>,
    ) -> Result<Vec<ManagedPostgresRestoreDrill>, SqlStoreError> {
        let status = filter.status.map(restore_status_label);
        let rows = sqlx::query(
            "SELECT id, source_cluster_id, restore_id, backup_id, target_cluster_id, status, error_message \
             FROM managed_postgres_restore_drills \
             WHERE source_cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.source_cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_restore_drill_from_row)
            .collect()
    }

    pub async fn latest_succeeded_restore_drill_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<Option<ManagedPostgresRestoreDrill>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, source_cluster_id, restore_id, backup_id, target_cluster_id, status, error_message \
             FROM managed_postgres_restore_drills \
             WHERE source_cluster_id = $1 AND status = 'succeeded' \
             ORDER BY completed_at DESC NULLS LAST, created_at DESC \
             LIMIT 1",
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(managed_postgres_restore_drill_from_row).transpose()
    }

    pub async fn clusters_due_for_restore_drill(
        &self,
        max_age_hours: i64,
    ) -> Result<Vec<ManagedPostgresCluster>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT managed_postgres_clusters.id, \
                    managed_postgres_clusters.organization_id, \
                    managed_postgres_clusters.project_id, \
                    managed_postgres_clusters.environment_id, \
                    managed_postgres_clusters.host_id, \
                    managed_postgres_clusters.region, \
                    managed_postgres_clusters.postgres_version, \
                    managed_postgres_clusters.tier, \
                    managed_postgres_clusters.storage_gib, \
                    managed_postgres_clusters.lifecycle_state, \
                    managed_postgres_clusters.host_data_dir, \
                    managed_postgres_clusters.host_port \
             FROM managed_postgres_clusters \
             WHERE managed_postgres_clusters.lifecycle_state = 'ready' \
               AND EXISTS ( \
                    SELECT 1 FROM managed_postgres_backups \
                    WHERE managed_postgres_backups.cluster_id = managed_postgres_clusters.id \
                      AND managed_postgres_backups.status = 'succeeded' \
               ) \
               AND NOT EXISTS ( \
                    SELECT 1 FROM managed_postgres_restore_drills \
                    WHERE managed_postgres_restore_drills.source_cluster_id = managed_postgres_clusters.id \
                      AND managed_postgres_restore_drills.status = 'running' \
               ) \
               AND NOT EXISTS ( \
                    SELECT 1 FROM managed_postgres_restore_drills \
                    WHERE managed_postgres_restore_drills.source_cluster_id = managed_postgres_clusters.id \
                      AND managed_postgres_restore_drills.status = 'succeeded' \
                      AND managed_postgres_restore_drills.completed_at >= now() - ($1::bigint * interval '1 hour') \
               ) \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(max_age_hours)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(managed_postgres_cluster_from_row)
            .collect()
    }

    pub async fn insert_operation(&self, operation: &OperationRecord) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO operations \
                (id, idempotency_key, target_resource_id, kind, status, current_step, lease_owner) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&operation.operation_id)
        .bind(&operation.idempotency_key)
        .bind(&operation.target_resource_id)
        .bind(operation_kind_label(operation.kind))
        .bind(operation_status_label(operation.status))
        .bind(&operation.current_step)
        .bind(&operation.lease_owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn operation(
        &self,
        cluster_id: &str,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, idempotency_key, target_resource_id, kind, status, current_step, lease_owner \
             FROM operations \
             WHERE id = $1 \
               AND (target_resource_id = $2 \
                    OR target_resource_id LIKE $2 || ':%' \
                    OR EXISTS ( \
                        SELECT 1 FROM agent_commands \
                        WHERE agent_commands.operation_id = operations.id \
                          AND agent_commands.cluster_id = $2 \
                    ))",
        )
        .bind(operation_id)
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(operation_from_row).transpose()
    }

    pub async fn operations(
        &self,
        filter: &OperationFilter<'_>,
    ) -> Result<Vec<OperationRecord>, SqlStoreError> {
        let kind = filter.kind.map(operation_kind_label);
        let status = filter.status.map(operation_status_label);
        let rows = sqlx::query(
            "SELECT id, idempotency_key, target_resource_id, kind, status, current_step, lease_owner \
             FROM operations \
             WHERE (target_resource_id = $1 \
                    OR target_resource_id LIKE $1 || ':%' \
                    OR EXISTS ( \
                        SELECT 1 FROM agent_commands \
                        WHERE agent_commands.operation_id = operations.id \
                          AND agent_commands.cluster_id = $1 \
                    )) \
               AND ($2::text IS NULL OR kind = $2) \
               AND ($3::text IS NULL OR status = $3) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(kind)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(operation_from_row).collect()
    }

    pub async fn enqueue_agent_command(
        &self,
        host_id: &str,
        operation_id: Option<&str>,
        command: &NodeAgentCommand,
    ) -> Result<(), SqlStoreError> {
        let status = agent_command_status_label(AgentCommandStatus::Pending);
        sqlx::query(
            "INSERT INTO agent_commands \
                (id, operation_id, host_id, cluster_id, action, status) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&command.command_id)
        .bind(operation_id)
        .bind(host_id)
        .bind(&command.cluster_id)
        .bind(Json(&command.action))
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn lease_next_agent_command(
        &self,
        host_id: &str,
    ) -> Result<Option<QueuedNodeAgentCommand>, SqlStoreError> {
        let operation_token = generate_operation_token()?;
        let operation_token_hash = operation_token_hash(&operation_token);
        let row = sqlx::query(
            "WITH next_command AS ( \
                SELECT id FROM agent_commands \
                WHERE host_id = $1 AND status = 'pending' \
                ORDER BY created_at \
                LIMIT 1 \
                FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE agent_commands SET \
                status = 'running', \
                attempts = attempts + 1, \
                operation_token_hash = $2, \
                operation_token_expires_at = now() + interval '15 minutes', \
                updated_at = now() \
             WHERE id = (SELECT id FROM next_command) \
             RETURNING id, operation_id, host_id, cluster_id, action, status, attempts, last_error, \
                $3::text AS operation_token",
        )
        .bind(host_id)
        .bind(operation_token_hash)
        .bind(&operation_token)
        .fetch_optional(&self.pool)
        .await?;

        row.map(queued_command_from_row).transpose()
    }

    pub async fn agent_command(
        &self,
        cluster_id: &str,
        command_id: &str,
    ) -> Result<Option<QueuedNodeAgentCommand>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, operation_id, host_id, cluster_id, action, status, attempts, last_error, \
                NULL::text AS operation_token \
             FROM agent_commands \
             WHERE cluster_id = $1 AND id = $2",
        )
        .bind(cluster_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(queued_command_from_row).transpose()
    }

    pub async fn agent_commands(
        &self,
        filter: &AgentCommandFilter<'_>,
    ) -> Result<Vec<QueuedNodeAgentCommand>, SqlStoreError> {
        let status = filter.status.map(agent_command_status_label);
        let rows = sqlx::query(
            "SELECT id, operation_id, host_id, cluster_id, action, status, attempts, last_error, \
                NULL::text AS operation_token \
             FROM agent_commands \
             WHERE cluster_id = $1 \
               AND ($2::text IS NULL OR status = $2) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.cluster_id)
        .bind(status)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(queued_command_from_row).collect()
    }

    pub async fn complete_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        last_error: Option<&str>,
    ) -> Result<(), SqlStoreError> {
        if !matches!(
            status,
            AgentCommandStatus::Succeeded
                | AgentCommandStatus::Failed
                | AgentCommandStatus::Cancelled
        ) {
            return Err(SqlStoreError::InvalidValue(format!(
                "agent command completion cannot use status '{}'",
                agent_command_status_label(status)
            )));
        }

        let status = agent_command_status_label(status);
        let result = sqlx::query(
            "UPDATE agent_commands SET \
                status = $3, \
                last_error = $4, \
                operation_token_hash = NULL, \
                operation_token_expires_at = NULL, \
                updated_at = now() \
             WHERE host_id = $1 AND id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .bind(status)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        }
        Ok(())
    }

    pub async fn agent_command_operation_token_is_valid(
        &self,
        host_id: &str,
        command_id: &str,
        operation_token: Option<&str>,
    ) -> Result<bool, SqlStoreError> {
        let row = sqlx::query(
            "SELECT operation_token_hash, operation_token_expires_at <= now() AS token_expired \
             FROM agent_commands \
             WHERE host_id = $1 AND id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };
        let expected_hash: Option<String> = row.try_get("operation_token_hash")?;
        let Some(expected_hash) = expected_hash else {
            return Ok(true);
        };
        if row
            .try_get::<Option<bool>, _>("token_expired")?
            .unwrap_or(true)
        {
            return Ok(false);
        }
        let Some(operation_token) = operation_token else {
            return Ok(false);
        };
        Ok(operation_token_hash(operation_token) == expected_hash)
    }

    pub async fn advance_cluster_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
    ) -> Result<Option<(String, ClusterLifecycleState)>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action, \
                    managed_postgres_clusters.lifecycle_state, \
                    operations.kind AS operation_kind \
             FROM agent_commands \
             JOIN managed_postgres_clusters ON managed_postgres_clusters.id = agent_commands.cluster_id \
             LEFT JOIN operations ON operations.id = agent_commands.operation_id \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let lifecycle_state: String = row.try_get("lifecycle_state")?;
        let operation_kind: Option<String> = row.try_get("operation_kind")?;
        let current = parse_lifecycle_state(&lifecycle_state)?;
        let Some(next) = next_state_after_agent_command_for_operation(
            current,
            &action.0,
            status,
            operation_kind.as_deref(),
        ) else {
            return Ok(None);
        };

        let next_label = lifecycle_state_label(next);
        sqlx::query(
            "UPDATE managed_postgres_clusters SET lifecycle_state = $2, updated_at = now() WHERE id = $1",
        )
        .bind(&cluster_id)
        .bind(next_label)
        .execute(&self.pool)
        .await?;
        if next == ClusterLifecycleState::Deleted {
            if let NodeAgentAction::DeletePostgresData {
                tombstone_retention_days,
                ..
            } = action.0
            {
                self.insert_managed_postgres_deletion_tombstone(
                    &cluster_id,
                    tombstone_retention_days,
                )
                .await?;
            }
        }
        Ok(Some((cluster_id, next)))
    }

    pub async fn advance_sync_deployment_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
    ) -> Result<Option<(String, SyncDeploymentLifecycleState)>, SqlStoreError> {
        let row = sqlx::query("SELECT action FROM agent_commands WHERE host_id = $1 AND id = $2")
            .bind(host_id)
            .bind(command_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let (deployment_id, success_state) = match &action.0 {
            NodeAgentAction::StartSyncDeployment { deployment_id, .. } => (
                deployment_id.as_str(),
                SyncDeploymentLifecycleState::Running,
            ),
            NodeAgentAction::StopSyncDeployment { deployment_id } => (
                deployment_id.as_str(),
                SyncDeploymentLifecycleState::Stopped,
            ),
            _ => return Ok(None),
        };

        let next = if status == AgentCommandStatus::Succeeded {
            success_state
        } else if status == AgentCommandStatus::Failed {
            SyncDeploymentLifecycleState::Failed
        } else {
            return Ok(None);
        };
        self.update_sync_deployment_lifecycle_state(deployment_id, next)
            .await?;
        Ok(Some((deployment_id.to_owned(), next)))
    }

    pub async fn advance_operation_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<(), SqlStoreError> {
        let next = match status {
            AgentCommandStatus::Succeeded => OperationStatus::Succeeded,
            AgentCommandStatus::Failed => OperationStatus::Failed,
            AgentCommandStatus::Cancelled => OperationStatus::Cancelled,
            _ => return Ok(()),
        };
        let current_step = match next {
            OperationStatus::Succeeded => "completed",
            OperationStatus::Failed => "failed",
            OperationStatus::Cancelled => "cancelled",
            OperationStatus::Pending | OperationStatus::Running => return Ok(()),
        };
        sqlx::query(
            "UPDATE operations SET \
                status = $3, \
                current_step = $4, \
                error_message = $5, \
                updated_at = now() \
             WHERE id = ( \
                SELECT operation_id FROM agent_commands WHERE host_id = $1 AND id = $2 \
             )",
        )
        .bind(host_id)
        .bind(command_id)
        .bind(operation_status_label(next))
        .bind(current_step)
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn advance_major_upgrade_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<ManagedPostgresMajorUpgradeStatus>, SqlStoreError> {
        let row = sqlx::query("SELECT action FROM agent_commands WHERE host_id = $1 AND id = $2")
            .bind(host_id)
            .bind(command_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::UpgradePostgresMajor { .. } = action.0 else {
            return Ok(None);
        };

        let next = match status {
            AgentCommandStatus::Succeeded => ManagedPostgresMajorUpgradeStatus::Succeeded,
            AgentCommandStatus::Failed => ManagedPostgresMajorUpgradeStatus::Failed,
            AgentCommandStatus::Cancelled => ManagedPostgresMajorUpgradeStatus::Cancelled,
            _ => return Ok(None),
        };

        sqlx::query(
            "UPDATE managed_postgres_major_upgrades SET \
                status = $3, \
                error_message = $4, \
                completed_at = now() \
             WHERE command_id = $1 AND operation_id = ( \
                SELECT operation_id FROM agent_commands WHERE host_id = $2 AND id = $1 \
             )",
        )
        .bind(command_id)
        .bind(host_id)
        .bind(major_upgrade_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;

        Ok(Some(next))
    }

    pub async fn advance_backup_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<BackupLifecycleState>, SqlStoreError> {
        let row = sqlx::query("SELECT action FROM agent_commands WHERE host_id = $1 AND id = $2")
            .bind(host_id)
            .bind(command_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let (backup_id, next) = match action.0 {
            NodeAgentAction::RunBaseBackup { backup_id, .. } => {
                let next = match status {
                    AgentCommandStatus::Succeeded => BackupLifecycleState::Succeeded,
                    AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                        BackupLifecycleState::Failed
                    }
                    _ => return Ok(None),
                };
                (backup_id, next)
            }
            NodeAgentAction::DeleteBackupData { backup_id, .. } => {
                let next = match status {
                    AgentCommandStatus::Succeeded => BackupLifecycleState::Deleted,
                    AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                        BackupLifecycleState::Failed
                    }
                    _ => return Ok(None),
                };
                (backup_id, next)
            }
            _ => return Ok(None),
        };
        sqlx::query(
            "UPDATE managed_postgres_backups SET \
                status = $2, \
                error_message = $3, \
                completed_at = now() \
             WHERE id = $1",
        )
        .bind(&backup_id)
        .bind(backup_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(Some(next))
    }

    pub async fn record_backup_artifacts_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        reported_artifacts: &[NodeAgentBackupArtifact],
    ) -> Result<Vec<ManagedPostgresBackupArtifact>, SqlStoreError> {
        if status != AgentCommandStatus::Succeeded {
            return Ok(Vec::new());
        }

        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::RunBaseBackup {
            backup_id,
            backup_dir,
            ..
        } = action.0
        else {
            return Ok(Vec::new());
        };
        let backup = self
            .managed_postgres_backup(&backup_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(backup_id.clone()))?;
        if backup.status != BackupLifecycleState::Succeeded {
            return Ok(Vec::new());
        }

        let matching_reported_artifacts: Vec<&NodeAgentBackupArtifact> = reported_artifacts
            .iter()
            .filter(|artifact| artifact.backup_id == backup_id)
            .collect();
        let artifact_dir = PathBuf::from(&backup_dir).join(&backup_id);
        let mut recorded = Vec::new();
        if matching_reported_artifacts.is_empty() {
            let artifact = ManagedPostgresBackupArtifact {
                artifact_id: format!("backup_artifact_{backup_id}_local_fs"),
                backup_id,
                cluster_id,
                provider: "local_fs".to_owned(),
                object_uri: format!("file://{}", artifact_dir.display()),
                manifest_path: artifact_dir.join("manifest.json").display().to_string(),
                manifest_sha256: None,
                size_bytes: None,
                status: BackupArtifactStatus::Available,
                created_at: String::new(),
                updated_at: String::new(),
                error_message: None,
            };
            recorded.push(
                self.upsert_managed_postgres_backup_artifact(&artifact)
                    .await?,
            );
            return Ok(recorded);
        }

        for reported_artifact in matching_reported_artifacts {
            let provider = normalize_non_empty("provider", &reported_artifact.provider)?;
            let artifact = ManagedPostgresBackupArtifact {
                artifact_id: format!(
                    "backup_artifact_{}_{}",
                    backup_id,
                    backup_artifact_id_component(&provider)
                ),
                backup_id: backup_id.clone(),
                cluster_id: cluster_id.clone(),
                provider,
                object_uri: reported_artifact.object_uri.clone(),
                manifest_path: reported_artifact.manifest_path.clone(),
                manifest_sha256: reported_artifact.manifest_sha256.clone(),
                size_bytes: reported_artifact.size_bytes,
                status: BackupArtifactStatus::Available,
                created_at: String::new(),
                updated_at: String::new(),
                error_message: None,
            };
            recorded.push(
                self.upsert_managed_postgres_backup_artifact(&artifact)
                    .await?,
            );
        }
        Ok(recorded)
    }

    pub async fn advance_backup_artifacts_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<BackupArtifactStatus>, SqlStoreError> {
        let row = sqlx::query("SELECT action FROM agent_commands WHERE host_id = $1 AND id = $2")
            .bind(host_id)
            .bind(command_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::DeleteBackupData { backup_id, .. } = action.0 else {
            return Ok(None);
        };
        let next = match status {
            AgentCommandStatus::Succeeded => BackupArtifactStatus::Deleted,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                BackupArtifactStatus::Failed
            }
            _ => return Ok(None),
        };

        sqlx::query(
            "UPDATE managed_postgres_backup_artifacts SET \
                status = $2, \
                error_message = $3, \
                updated_at = now() \
             WHERE backup_id = $1",
        )
        .bind(&backup_id)
        .bind(backup_artifact_status_label(next))
        .bind(match next {
            BackupArtifactStatus::Failed => detail,
            _ => None,
        })
        .execute(&self.pool)
        .await?;
        Ok(Some(next))
    }

    pub async fn advance_restore_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<RestoreLifecycleState>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let target_cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::PrepareRestore { .. } = action.0 else {
            return Ok(None);
        };

        let next = match status {
            AgentCommandStatus::Succeeded => RestoreLifecycleState::Succeeded,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                RestoreLifecycleState::Failed
            }
            _ => return Ok(None),
        };
        sqlx::query(
            "UPDATE managed_postgres_restores SET \
                status = $2, \
                error_message = $3, \
                completed_at = now() \
             WHERE target_cluster_id = $1 AND status = 'running'",
        )
        .bind(&target_cluster_id)
        .bind(restore_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(Some(next))
    }

    pub async fn advance_restore_drill_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<RestoreLifecycleState>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let target_cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::PrepareRestore { .. } = action.0 else {
            return Ok(None);
        };

        let next = match status {
            AgentCommandStatus::Succeeded => RestoreLifecycleState::Succeeded,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                RestoreLifecycleState::Failed
            }
            _ => return Ok(None),
        };
        sqlx::query(
            "UPDATE managed_postgres_restore_drills SET \
                status = $2, \
                error_message = $3, \
                completed_at = now() \
             WHERE target_cluster_id = $1 AND status = 'running'",
        )
        .bind(&target_cluster_id)
        .bind(restore_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(Some(next))
    }

    pub async fn advance_standby_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<StandbyLifecycleState>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let target_cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::PreparePostgresStandby { target_port, .. } = action.0 else {
            return Ok(None);
        };
        let starts_target = target_port.is_some();

        let next = match status {
            AgentCommandStatus::Succeeded => StandbyLifecycleState::Succeeded,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                StandbyLifecycleState::Failed
            }
            _ => return Ok(None),
        };
        sqlx::query(
            "UPDATE managed_postgres_standbys SET \
                status = $2, \
                error_message = $3, \
                completed_at = now() \
             WHERE target_cluster_id = $1 AND status = 'running'",
        )
        .bind(&target_cluster_id)
        .bind(standby_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;
        if next == StandbyLifecycleState::Succeeded {
            let lifecycle_state = if starts_target { "ready" } else { "stopped" };
            sqlx::query(
                "UPDATE managed_postgres_clusters SET lifecycle_state = $2, updated_at = now() \
                 WHERE id = $1",
            )
            .bind(&target_cluster_id)
            .bind(lifecycle_state)
            .execute(&self.pool)
            .await?;
        }
        Ok(Some(next))
    }

    pub async fn advance_standby_check_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<StandbyCheckStatus>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::CheckPostgresStandbyLag { slot_name, .. } = action.0 else {
            return Ok(None);
        };

        let next = match status {
            AgentCommandStatus::Succeeded => StandbyCheckStatus::Succeeded,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                StandbyCheckStatus::Failed
            }
            _ => return Ok(None),
        };
        sqlx::query(
            "UPDATE managed_postgres_standby_checks SET \
                status = $2, \
                error_message = $3, \
                completed_at = now() \
             WHERE slot_name = $1 AND status = 'running'",
        )
        .bind(&slot_name)
        .bind(standby_check_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(Some(next))
    }

    pub async fn advance_wal_archive_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<WalArchiveSegmentStatus>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::ArchiveWalSegment { segment_name, .. } = action.0 else {
            return Ok(None);
        };

        let next = match status {
            AgentCommandStatus::Succeeded => WalArchiveSegmentStatus::Succeeded,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                WalArchiveSegmentStatus::Failed
            }
            _ => return Ok(None),
        };
        sqlx::query(
            "UPDATE managed_postgres_wal_archives SET \
                status = $3, \
                error_message = $4, \
                completed_at = now() \
             WHERE cluster_id = $1 AND segment_name = $2",
        )
        .bind(&cluster_id)
        .bind(&segment_name)
        .bind(wal_archive_segment_status_label(next))
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(Some(next))
    }

    pub async fn advance_failover_after_agent_command(
        &self,
        host_id: &str,
        command_id: &str,
        status: AgentCommandStatus,
        detail: Option<&str>,
    ) -> Result<Option<FailoverLifecycleState>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT agent_commands.cluster_id, agent_commands.action \
             FROM agent_commands \
             WHERE agent_commands.host_id = $1 AND agent_commands.id = $2",
        )
        .bind(host_id)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(command_id.to_owned()));
        };

        let target_cluster_id: String = row.try_get("cluster_id")?;
        let action: Json<NodeAgentAction> = row.try_get("action")?;
        let NodeAgentAction::PromotePostgresStandby { .. } = action.0 else {
            return Ok(None);
        };

        let next = match status {
            AgentCommandStatus::Succeeded => FailoverLifecycleState::Succeeded,
            AgentCommandStatus::Failed | AgentCommandStatus::Cancelled => {
                FailoverLifecycleState::Failed
            }
            _ => return Ok(None),
        };
        let row = sqlx::query(
            "UPDATE managed_postgres_failovers SET \
                status = $2, \
                error_message = $3, \
                completed_at = now() \
             WHERE target_cluster_id = $1 AND status = 'running' \
             RETURNING id, source_cluster_id",
        )
        .bind(&target_cluster_id)
        .bind(failover_status_label(next))
        .bind(detail)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        if next == FailoverLifecycleState::Succeeded {
            let failover_id: String = row.try_get("id")?;
            let source_cluster_id: String = row.try_get("source_cluster_id")?;
            sqlx::query(
                "UPDATE managed_postgres_clusters SET lifecycle_state = 'stopped', updated_at = now() \
                 WHERE id = $1",
            )
            .bind(&source_cluster_id)
            .execute(&self.pool)
            .await?;
            sqlx::query(
                "UPDATE managed_postgres_clusters SET lifecycle_state = 'ready', updated_at = now() \
                 WHERE id = $1",
            )
            .bind(&target_cluster_id)
            .execute(&self.pool)
            .await?;
            let environment_id: String = sqlx::query_scalar(
                "SELECT environment_id FROM managed_postgres_clusters WHERE id = $1",
            )
            .bind(&target_cluster_id)
            .fetch_one(&self.pool)
            .await?;
            self.cutover_managed_postgres_endpoint(
                &environment_id,
                &target_cluster_id,
                &failover_id,
            )
            .await?;
        }
        Ok(Some(next))
    }

    pub async fn insert_api_key(
        &self,
        api_key: &ApiKey,
        token_hash: &str,
    ) -> Result<ApiKey, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO api_keys \
                (id, token_prefix, token_hash, name, organization_id, project_id, environment_id, role, created_by) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING id, token_prefix, name, organization_id, project_id, environment_id, role, created_by, revoked_at IS NOT NULL AS revoked",
        )
        .bind(&api_key.key_id)
        .bind(&api_key.token_prefix)
        .bind(token_hash)
        .bind(&api_key.name)
        .bind(&api_key.organization_id)
        .bind(&api_key.project_id)
        .bind(&api_key.environment_id)
        .bind(team_role_label(api_key.role))
        .bind(&api_key.created_by)
        .fetch_one(&self.pool)
        .await?;
        api_key_from_row(&row)
    }

    pub async fn revoke_api_key(&self, key_id: &str) -> Result<ApiKey, SqlStoreError> {
        let row = sqlx::query(
            "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) \
             WHERE id = $1 \
             RETURNING id, token_prefix, name, organization_id, project_id, environment_id, role, created_by, revoked_at IS NOT NULL AS revoked",
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(SqlStoreError::MissingResource(key_id.to_owned()));
        };
        api_key_from_row(&row)
    }

    pub async fn api_key(&self, key_id: &str) -> Result<Option<ApiKey>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, token_prefix, name, organization_id, project_id, environment_id, role, created_by, revoked_at IS NOT NULL AS revoked \
             FROM api_keys \
             WHERE id = $1",
        )
        .bind(key_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(api_key_from_row).transpose()
    }

    pub async fn api_key_by_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<ApiKey>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, token_prefix, name, organization_id, project_id, environment_id, role, created_by, revoked_at IS NOT NULL AS revoked \
             FROM api_keys \
             WHERE token_hash = $1 AND revoked_at IS NULL",
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(api_key_from_row).transpose()
    }

    pub async fn api_keys(&self, filter: &ApiKeyFilter<'_>) -> Result<Vec<ApiKey>, SqlStoreError> {
        let include_revoked = filter.include_revoked.unwrap_or(false);
        let rows = sqlx::query(
            "SELECT id, token_prefix, name, organization_id, project_id, environment_id, role, created_by, revoked_at IS NOT NULL AS revoked \
             FROM api_keys \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::bool OR revoked_at IS NULL) \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(include_revoked)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(api_key_from_row).collect()
    }

    pub async fn upsert_team_membership(
        &self,
        membership: &TeamMembership,
    ) -> Result<TeamMembership, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO team_memberships (organization_id, actor_id, role) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (organization_id, actor_id) DO UPDATE SET role = EXCLUDED.role \
             RETURNING organization_id, actor_id, role",
        )
        .bind(&membership.organization_id)
        .bind(&membership.actor_id)
        .bind(team_role_label(membership.role))
        .fetch_one(&self.pool)
        .await?;
        team_membership_from_row(&row)
    }

    pub async fn team_memberships(
        &self,
        organization_id: &str,
    ) -> Result<Vec<TeamMembership>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT organization_id, actor_id, role \
             FROM team_memberships \
             WHERE organization_id = $1 \
             ORDER BY actor_id ASC",
        )
        .bind(organization_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(team_membership_from_row).collect()
    }

    pub async fn upsert_jwt_issuer(&self, issuer: &JwtIssuer) -> Result<JwtIssuer, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO jwt_issuers \
                (id, organization_id, project_id, environment_id, name, issuer, audience, jwks_url, claim_to_field, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (id) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                name = EXCLUDED.name, \
                issuer = EXCLUDED.issuer, \
                audience = EXCLUDED.audience, \
                jwks_url = EXCLUDED.jwks_url, \
                claim_to_field = EXCLUDED.claim_to_field, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, name, issuer, audience, jwks_url, claim_to_field, status",
        )
        .bind(&issuer.issuer_id)
        .bind(&issuer.organization_id)
        .bind(&issuer.project_id)
        .bind(&issuer.environment_id)
        .bind(&issuer.name)
        .bind(&issuer.issuer)
        .bind(&issuer.audience)
        .bind(&issuer.jwks_url)
        .bind(Json(issuer.claim_to_field.clone()))
        .bind(jwt_issuer_status_label(issuer.status))
        .fetch_one(&self.pool)
        .await?;
        jwt_issuer_from_row(&row)
    }

    pub async fn jwt_issuer(&self, issuer_id: &str) -> Result<Option<JwtIssuer>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, issuer, audience, jwks_url, claim_to_field, status \
             FROM jwt_issuers \
             WHERE id = $1",
        )
        .bind(issuer_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(jwt_issuer_from_row).transpose()
    }

    pub async fn jwt_issuers(
        &self,
        filter: &JwtIssuerFilter<'_>,
    ) -> Result<Vec<JwtIssuer>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, issuer, audience, jwks_url, claim_to_field, status \
             FROM jwt_issuers \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR status = $4) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.status.map(jwt_issuer_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(jwt_issuer_from_row).collect()
    }

    pub async fn upsert_webhook_endpoint(
        &self,
        endpoint: &WebhookEndpoint,
    ) -> Result<WebhookEndpoint, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO webhook_endpoints \
                (id, organization_id, project_id, environment_id, name, url, event_types, signing_secret_ref, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (id) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                name = EXCLUDED.name, \
                url = EXCLUDED.url, \
                event_types = EXCLUDED.event_types, \
                signing_secret_ref = EXCLUDED.signing_secret_ref, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, name, url, event_types, signing_secret_ref, status",
        )
        .bind(&endpoint.endpoint_id)
        .bind(&endpoint.organization_id)
        .bind(&endpoint.project_id)
        .bind(&endpoint.environment_id)
        .bind(&endpoint.name)
        .bind(&endpoint.url)
        .bind(Json(endpoint.event_types.clone()))
        .bind(endpoint.signing_secret_ref.clone().map(Json))
        .bind(webhook_endpoint_status_label(endpoint.status))
        .fetch_one(&self.pool)
        .await?;
        webhook_endpoint_from_row(&row)
    }

    pub async fn webhook_endpoint(
        &self,
        endpoint_id: &str,
    ) -> Result<Option<WebhookEndpoint>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, url, event_types, signing_secret_ref, status \
             FROM webhook_endpoints \
             WHERE id = $1",
        )
        .bind(endpoint_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(webhook_endpoint_from_row).transpose()
    }

    pub async fn webhook_endpoints(
        &self,
        filter: &WebhookEndpointFilter<'_>,
    ) -> Result<Vec<WebhookEndpoint>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, url, event_types, signing_secret_ref, status \
             FROM webhook_endpoints \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR status = $4) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.status.map(webhook_endpoint_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(webhook_endpoint_from_row).collect()
    }

    pub async fn upsert_sso_identity_provider(
        &self,
        provider: &SsoIdentityProvider,
    ) -> Result<SsoIdentityProvider, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO sso_identity_providers \
                (id, organization_id, name, kind, issuer, sso_url, certificate_secret_ref, claim_mappings, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (id) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                name = EXCLUDED.name, \
                kind = EXCLUDED.kind, \
                issuer = EXCLUDED.issuer, \
                sso_url = EXCLUDED.sso_url, \
                certificate_secret_ref = EXCLUDED.certificate_secret_ref, \
                claim_mappings = EXCLUDED.claim_mappings, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, name, kind, issuer, sso_url, certificate_secret_ref, claim_mappings, status",
        )
        .bind(&provider.provider_id)
        .bind(&provider.organization_id)
        .bind(&provider.name)
        .bind(sso_provider_kind_label(provider.kind))
        .bind(&provider.issuer)
        .bind(&provider.sso_url)
        .bind(provider.certificate_secret_ref.clone().map(Json))
        .bind(Json(provider.claim_mappings.clone()))
        .bind(sso_provider_status_label(provider.status))
        .fetch_one(&self.pool)
        .await?;
        sso_identity_provider_from_row(&row)
    }

    pub async fn sso_identity_provider(
        &self,
        provider_id: &str,
    ) -> Result<Option<SsoIdentityProvider>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, name, kind, issuer, sso_url, certificate_secret_ref, claim_mappings, status \
             FROM sso_identity_providers \
             WHERE id = $1",
        )
        .bind(provider_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(sso_identity_provider_from_row).transpose()
    }

    pub async fn sso_identity_providers(
        &self,
        filter: &SsoIdentityProviderFilter<'_>,
    ) -> Result<Vec<SsoIdentityProvider>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, name, kind, issuer, sso_url, certificate_secret_ref, claim_mappings, status \
             FROM sso_identity_providers \
             WHERE organization_id = $1 \
               AND ($2::text IS NULL OR kind = $2) \
               AND ($3::text IS NULL OR status = $3) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.kind.map(sso_provider_kind_label))
        .bind(filter.status.map(sso_provider_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(sso_identity_provider_from_row).collect()
    }

    pub async fn upsert_incident(&self, incident: &Incident) -> Result<Incident, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO incidents \
                (id, organization_id, project_id, environment_id, title, summary, severity, status, impacted_services, started_at, resolved_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz, $11::timestamptz) \
             ON CONFLICT (id) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                title = EXCLUDED.title, \
                summary = EXCLUDED.summary, \
                severity = EXCLUDED.severity, \
                status = EXCLUDED.status, \
                impacted_services = EXCLUDED.impacted_services, \
                started_at = EXCLUDED.started_at, \
                resolved_at = EXCLUDED.resolved_at, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, title, summary, severity, status, impacted_services, \
                started_at::text AS started_at, resolved_at::text AS resolved_at",
        )
        .bind(&incident.incident_id)
        .bind(&incident.organization_id)
        .bind(&incident.project_id)
        .bind(&incident.environment_id)
        .bind(&incident.title)
        .bind(&incident.summary)
        .bind(incident_severity_label(incident.severity))
        .bind(incident_status_label(incident.status))
        .bind(Json(incident.impacted_services.clone()))
        .bind(&incident.started_at)
        .bind(&incident.resolved_at)
        .fetch_one(&self.pool)
        .await?;
        incident_from_row(row)
    }

    pub async fn incident(&self, incident_id: &str) -> Result<Option<Incident>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, title, summary, severity, status, impacted_services, \
                started_at::text AS started_at, resolved_at::text AS resolved_at \
             FROM incidents WHERE id = $1",
        )
        .bind(incident_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(incident_from_row).transpose()
    }

    pub async fn incidents(
        &self,
        filter: &IncidentFilter<'_>,
    ) -> Result<Vec<Incident>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, title, summary, severity, status, impacted_services, \
                started_at::text AS started_at, resolved_at::text AS resolved_at \
             FROM incidents \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR severity = $4) \
               AND ($5::text IS NULL OR status = $5) \
               AND ($6::boolean IS NOT FALSE OR status <> 'resolved') \
             ORDER BY started_at DESC, updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.severity.map(incident_severity_label))
        .bind(filter.status.map(incident_status_label))
        .bind(filter.include_resolved)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(incident_from_row).collect()
    }

    pub async fn append_audit_event(&self, event: &AuditEvent) -> Result<(), SqlStoreError> {
        sqlx::query(
            "INSERT INTO audit_events (id, actor_id, action, resource_id, occurred_at) \
             VALUES ($1, $2, $3, $4, $5::timestamptz)",
        )
        .bind(&event.event_id)
        .bind(&event.actor_id)
        .bind(&event.action)
        .bind(&event.resource_id)
        .bind(&event.occurred_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn audit_events(
        &self,
        filter: &AuditEventFilter<'_>,
    ) -> Result<Vec<AuditEvent>, SqlStoreError> {
        let limit = i64::from(filter.limit.unwrap_or(500).min(2_000));
        let rows = sqlx::query(
            "SELECT id, actor_id, action, resource_id, occurred_at::text AS occurred_at \
             FROM audit_events \
             WHERE ($1::text IS NULL OR actor_id = $1) \
               AND ($2::text IS NULL OR action = $2) \
               AND ($3::text IS NULL OR resource_id = $3) \
               AND ( \
                    $4::text IS NULL \
                    OR resource_id = $4 \
                    OR resource_id LIKE $4 || ':%' \
                    OR resource_id IN ( \
                        SELECT id FROM projects \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR id = $5) \
                    ) \
                    OR resource_id IN ( \
                        SELECT id FROM environments \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT id FROM managed_postgres_clusters \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR EXISTS ( \
                        SELECT 1 FROM managed_postgres_clusters \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                          AND audit_events.resource_id LIKE managed_postgres_clusters.id || ':%' \
                    ) \
                    OR resource_id IN ( \
                        SELECT config_versions.id FROM config_versions \
                        JOIN environments ON environments.id = config_versions.environment_id \
                        WHERE environments.organization_id = $4 \
                          AND ($5::text IS NULL OR environments.project_id = $5) \
                          AND ($6::text IS NULL OR environments.id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT id FROM sync_deployments \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT managed_postgres_backups.id FROM managed_postgres_backups \
                        JOIN managed_postgres_clusters ON managed_postgres_clusters.id = managed_postgres_backups.cluster_id \
                        WHERE managed_postgres_clusters.organization_id = $4 \
                          AND ($5::text IS NULL OR managed_postgres_clusters.project_id = $5) \
                          AND ($6::text IS NULL OR managed_postgres_clusters.environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT managed_postgres_restores.id FROM managed_postgres_restores \
                        JOIN managed_postgres_clusters ON managed_postgres_clusters.id = managed_postgres_restores.source_cluster_id \
                        WHERE managed_postgres_clusters.organization_id = $4 \
                          AND ($5::text IS NULL OR managed_postgres_clusters.project_id = $5) \
                          AND ($6::text IS NULL OR managed_postgres_clusters.environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT managed_postgres_runtime_checks.id FROM managed_postgres_runtime_checks \
                        JOIN managed_postgres_clusters ON managed_postgres_clusters.id = managed_postgres_runtime_checks.cluster_id \
                        WHERE managed_postgres_clusters.organization_id = $4 \
                          AND ($5::text IS NULL OR managed_postgres_clusters.project_id = $5) \
                          AND ($6::text IS NULL OR managed_postgres_clusters.environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT id FROM managed_postgres_support_access_sessions \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT agent_commands.id FROM agent_commands \
                        JOIN managed_postgres_clusters ON managed_postgres_clusters.id = agent_commands.cluster_id \
                        WHERE managed_postgres_clusters.organization_id = $4 \
                          AND ($5::text IS NULL OR managed_postgres_clusters.project_id = $5) \
                          AND ($6::text IS NULL OR managed_postgres_clusters.environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT billing_exports.id FROM billing_exports \
                        WHERE ($4::text IS NULL OR organization_id = $4) \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT quota_policies.id FROM quota_policies \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT api_keys.id FROM api_keys \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT secret_encryption_keys.key_ref FROM secret_encryption_keys \
                    ) \
                    OR resource_id IN ( \
                        SELECT secret_rewrap_plans.id FROM secret_rewrap_plans \
                    ) \
                    OR resource_id IN ( \
                        SELECT node_host_hardening_checks.id FROM node_host_hardening_checks \
                    ) \
                    OR resource_id IN ( \
                        SELECT jwt_issuers.id FROM jwt_issuers \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT webhook_endpoints.id FROM webhook_endpoints \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT domains.hostname FROM domains \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT ip_allowlist_rules.id FROM ip_allowlist_rules \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT static_egress_ips.id FROM static_egress_ips \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT maintenance_windows.id FROM maintenance_windows \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
                    OR resource_id IN ( \
                        SELECT sso_identity_providers.id FROM sso_identity_providers \
                        WHERE organization_id = $4 \
                    ) \
                    OR resource_id IN ( \
                        SELECT incidents.id FROM incidents \
                        WHERE organization_id = $4 \
                          AND ($5::text IS NULL OR project_id = $5) \
                          AND ($6::text IS NULL OR environment_id = $6) \
                    ) \
               ) \
             ORDER BY occurred_at DESC, id DESC \
             LIMIT $7",
        )
        .bind(filter.actor_id)
        .bind(filter.action)
        .bind(filter.resource_id)
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(audit_event_from_row).collect()
    }

    pub async fn append_usage_event(&self, event: &UsageEvent) -> Result<bool, SqlStoreError> {
        let already_recorded: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM usage_events WHERE idempotency_key = $1)",
        )
        .bind(&event.idempotency_key)
        .fetch_one(&self.pool)
        .await?;
        if already_recorded {
            return Ok(false);
        }

        self.enforce_usage_quota(event).await?;
        let quantity = checked_i64(event.quantity, "quantity")?;
        let (signature_key_id, signature_algorithm, signature) = event
            .signature
            .as_ref()
            .map(|signature| {
                (
                    Some(signature.key_id.as_str()),
                    Some(signature.algorithm.as_str()),
                    Some(signature.signature.as_str()),
                )
            })
            .unwrap_or((None, None, None));
        let result = sqlx::query(
            "INSERT INTO usage_events \
                (id, idempotency_key, organization_id, project_id, environment_id, metric, quantity, occurred_at, signature_key_id, signature_algorithm, signature) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9, $10, $11) \
             ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(&event.event_id)
        .bind(&event.idempotency_key)
        .bind(&event.organization_id)
        .bind(&event.project_id)
        .bind(&event.environment_id)
        .bind(&event.metric)
        .bind(quantity)
        .bind(&event.occurred_at)
        .bind(signature_key_id)
        .bind(signature_algorithm)
        .bind(signature)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn enforce_usage_quota(&self, event: &UsageEvent) -> Result<(), SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, limit_quantity, window_seconds, enforcement \
             FROM quota_policies \
             WHERE environment_id = $1 AND metric = $2",
        )
        .bind(&event.environment_id)
        .bind(&event.metric)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(());
        };

        let enforcement: String = row.try_get("enforcement")?;
        if parse_quota_enforcement(&enforcement)? == QuotaEnforcement::Observe {
            return Ok(());
        }

        let limit_quantity: i64 = row.try_get("limit_quantity")?;
        let window_seconds: i64 = row.try_get("window_seconds")?;
        let current: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(quantity), 0)::bigint \
             FROM usage_events \
             WHERE environment_id = $1 \
               AND metric = $2 \
               AND occurred_at >= $3::timestamptz - ($4::bigint * interval '1 second') \
               AND occurred_at <= $3::timestamptz",
        )
        .bind(&event.environment_id)
        .bind(&event.metric)
        .bind(&event.occurred_at)
        .bind(window_seconds)
        .fetch_one(&self.pool)
        .await?;
        let attempted = current
            .checked_add(checked_i64(event.quantity, "quantity")?)
            .ok_or_else(|| {
                SqlStoreError::InvalidValue("quota usage overflows bigint".to_owned())
            })?;
        if attempted > limit_quantity {
            return Err(SqlStoreError::QuotaExceeded {
                environment_id: event.environment_id.clone(),
                metric: event.metric.clone(),
                limit: u64::try_from(limit_quantity).map_err(|_| {
                    SqlStoreError::InvalidValue(format!("invalid quota limit {limit_quantity}"))
                })?,
                attempted: u64::try_from(attempted).map_err(|_| {
                    SqlStoreError::InvalidValue(format!("invalid quota attempted {attempted}"))
                })?,
            });
        }
        Ok(())
    }

    pub async fn upsert_quota_policy(&self, policy: &QuotaPolicy) -> Result<(), SqlStoreError> {
        let limit_quantity = checked_i64(policy.limit_quantity, "limit_quantity")?;
        let window_seconds = checked_i64(policy.window_seconds, "window_seconds")?;
        sqlx::query(
            "INSERT INTO quota_policies \
                (id, organization_id, project_id, environment_id, metric, limit_quantity, window_seconds, enforcement) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (environment_id, metric) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                limit_quantity = EXCLUDED.limit_quantity, \
                window_seconds = EXCLUDED.window_seconds, \
                enforcement = EXCLUDED.enforcement, \
                updated_at = now()",
        )
        .bind(&policy.policy_id)
        .bind(&policy.organization_id)
        .bind(&policy.project_id)
        .bind(&policy.environment_id)
        .bind(&policy.metric)
        .bind(limit_quantity)
        .bind(window_seconds)
        .bind(quota_enforcement_label(policy.enforcement))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn quota_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<QuotaPolicy>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, metric, limit_quantity, window_seconds, enforcement \
             FROM quota_policies WHERE id = $1",
        )
        .bind(policy_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(quota_policy_from_row).transpose()
    }

    pub async fn quota_policies(
        &self,
        filter: &QuotaPolicyFilter<'_>,
    ) -> Result<Vec<QuotaPolicy>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, metric, limit_quantity, window_seconds, enforcement \
             FROM quota_policies \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR metric = $4) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.metric)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(quota_policy_from_row).collect()
    }

    pub async fn upsert_quota_alert(
        &self,
        alert_id: &str,
        policy_id: &str,
        threshold_basis_points: u32,
    ) -> Result<QuotaAlert, SqlStoreError> {
        if threshold_basis_points == 0 || threshold_basis_points > 10_000 {
            return Err(SqlStoreError::InvalidValue(
                "quota alert threshold_basis_points must be between 1 and 10000".to_owned(),
            ));
        }

        let policy = self
            .quota_policy(policy_id)
            .await?
            .ok_or_else(|| SqlStoreError::MissingResource(policy_id.to_owned()))?;
        let limit_quantity = checked_i64(policy.limit_quantity, "limit_quantity")?;
        let window_seconds = checked_i64(policy.window_seconds, "window_seconds")?;
        let row = sqlx::query(
            "INSERT INTO quota_alerts \
                (id, policy_id, organization_id, project_id, environment_id, metric, \
                 threshold_basis_points, limit_quantity, window_seconds) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (policy_id, threshold_basis_points) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                environment_id = EXCLUDED.environment_id, \
                metric = EXCLUDED.metric, \
                limit_quantity = EXCLUDED.limit_quantity, \
                window_seconds = EXCLUDED.window_seconds, \
                updated_at = now() \
             RETURNING id, policy_id, organization_id, project_id, environment_id, metric, \
                threshold_basis_points, current_quantity, limit_quantity, window_seconds, state, \
                last_evaluated_at::text AS last_evaluated_at, fired_at::text AS fired_at, \
                resolved_at::text AS resolved_at",
        )
        .bind(alert_id)
        .bind(policy_id)
        .bind(&policy.organization_id)
        .bind(&policy.project_id)
        .bind(&policy.environment_id)
        .bind(&policy.metric)
        .bind(i32::try_from(threshold_basis_points).map_err(|_| {
            SqlStoreError::InvalidValue("quota alert threshold exceeds integer".to_owned())
        })?)
        .bind(limit_quantity)
        .bind(window_seconds)
        .fetch_one(&self.pool)
        .await?;
        quota_alert_from_row(row)
    }

    pub async fn quota_alert(&self, alert_id: &str) -> Result<Option<QuotaAlert>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, policy_id, organization_id, project_id, environment_id, metric, \
                threshold_basis_points, current_quantity, limit_quantity, window_seconds, state, \
                last_evaluated_at::text AS last_evaluated_at, fired_at::text AS fired_at, \
                resolved_at::text AS resolved_at \
             FROM quota_alerts WHERE id = $1",
        )
        .bind(alert_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(quota_alert_from_row).transpose()
    }

    pub async fn quota_alerts(
        &self,
        filter: &QuotaAlertFilter<'_>,
    ) -> Result<Vec<QuotaAlert>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, policy_id, organization_id, project_id, environment_id, metric, \
                threshold_basis_points, current_quantity, limit_quantity, window_seconds, state, \
                last_evaluated_at::text AS last_evaluated_at, fired_at::text AS fired_at, \
                resolved_at::text AS resolved_at \
             FROM quota_alerts \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR metric = $4) \
               AND ($5::text IS NULL OR state = $5) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.metric)
        .bind(filter.state.map(quota_alert_state_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(quota_alert_from_row).collect()
    }

    pub async fn evaluate_quota_alerts(
        &self,
        filter: &QuotaAlertFilter<'_>,
    ) -> Result<Vec<QuotaAlert>, SqlStoreError> {
        let candidates = sqlx::query(
            "SELECT quota_alerts.id, quota_alerts.threshold_basis_points, \
                quota_policies.limit_quantity, quota_policies.window_seconds, \
                COALESCE(usage_window.current_quantity, 0)::bigint AS current_quantity \
             FROM quota_alerts \
             JOIN quota_policies ON quota_policies.id = quota_alerts.policy_id \
             LEFT JOIN LATERAL ( \
                SELECT SUM(usage_events.quantity)::bigint AS current_quantity \
                FROM usage_events \
                WHERE usage_events.environment_id = quota_policies.environment_id \
                  AND usage_events.metric = quota_policies.metric \
                  AND usage_events.occurred_at >= now() - (quota_policies.window_seconds * interval '1 second') \
                  AND usage_events.occurred_at <= now() \
             ) usage_window ON true \
             WHERE ($1::text IS NULL OR quota_alerts.organization_id = $1) \
               AND ($2::text IS NULL OR quota_alerts.project_id = $2) \
               AND ($3::text IS NULL OR quota_alerts.environment_id = $3) \
               AND ($4::text IS NULL OR quota_alerts.metric = $4) \
               AND ($5::text IS NULL OR quota_alerts.state = $5) \
             ORDER BY quota_alerts.updated_at DESC, quota_alerts.id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.metric)
        .bind(filter.state.map(quota_alert_state_label))
        .fetch_all(&self.pool)
        .await?;

        let mut evaluated = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let alert_id: String = candidate.try_get("id")?;
            let threshold_basis_points: i32 = candidate.try_get("threshold_basis_points")?;
            let current_quantity: i64 = candidate.try_get("current_quantity")?;
            let limit_quantity: i64 = candidate.try_get("limit_quantity")?;
            let window_seconds: i64 = candidate.try_get("window_seconds")?;
            let next_state = if quota_alert_should_fire(
                current_quantity,
                limit_quantity,
                threshold_basis_points,
            ) {
                QuotaAlertState::Firing
            } else {
                QuotaAlertState::Ok
            };
            let row = sqlx::query(
                "UPDATE quota_alerts SET \
                    current_quantity = $2, \
                    limit_quantity = $3, \
                    window_seconds = $4, \
                    state = $5, \
                    last_evaluated_at = now(), \
                    fired_at = CASE WHEN $5 = 'firing' AND fired_at IS NULL THEN now() ELSE fired_at END, \
                    resolved_at = CASE WHEN $5 = 'ok' AND state = 'firing' THEN now() ELSE resolved_at END, \
                    updated_at = now() \
                 WHERE id = $1 \
                 RETURNING id, policy_id, organization_id, project_id, environment_id, metric, \
                    threshold_basis_points, current_quantity, limit_quantity, window_seconds, state, \
                    last_evaluated_at::text AS last_evaluated_at, fired_at::text AS fired_at, \
                    resolved_at::text AS resolved_at",
            )
            .bind(&alert_id)
            .bind(current_quantity)
            .bind(limit_quantity)
            .bind(window_seconds)
            .bind(quota_alert_state_label(next_state))
            .fetch_one(&self.pool)
            .await?;
            evaluated.push(quota_alert_from_row(row)?);
        }
        Ok(evaluated)
    }

    pub async fn upsert_query_permission_policy(
        &self,
        policy: &QueryPermissionPolicy,
    ) -> Result<QueryPermissionPolicy, SqlStoreError> {
        if policy.name.trim().is_empty() {
            return Err(SqlStoreError::InvalidValue(
                "query permission policy name is empty".to_owned(),
            ));
        }
        if policy.predicate_sql.trim().is_empty() {
            return Err(SqlStoreError::InvalidValue(
                "query permission predicate is empty".to_owned(),
            ));
        }
        let row = sqlx::query(
            "INSERT INTO query_permission_policies \
                (id, organization_id, project_id, environment_id, name, table_schema, table_name, \
                 operation, principal_claim, predicate_sql, sample_context, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT (environment_id, table_schema, table_name, operation, name) DO UPDATE SET \
                id = EXCLUDED.id, \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                principal_claim = EXCLUDED.principal_claim, \
                predicate_sql = EXCLUDED.predicate_sql, \
                sample_context = EXCLUDED.sample_context, \
                status = EXCLUDED.status, \
                updated_at = now() \
             RETURNING id, organization_id, project_id, environment_id, name, table_schema, \
                table_name, operation, principal_claim, predicate_sql, sample_context, status",
        )
        .bind(&policy.policy_id)
        .bind(&policy.organization_id)
        .bind(&policy.project_id)
        .bind(&policy.environment_id)
        .bind(&policy.name)
        .bind(&policy.table_schema)
        .bind(&policy.table_name)
        .bind(query_permission_operation_label(policy.operation))
        .bind(&policy.principal_claim)
        .bind(&policy.predicate_sql)
        .bind(Json(policy.sample_context.clone()))
        .bind(query_permission_policy_status_label(policy.status))
        .fetch_one(&self.pool)
        .await?;
        query_permission_policy_from_row(row)
    }

    pub async fn query_permission_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<QueryPermissionPolicy>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, table_schema, \
                table_name, operation, principal_claim, predicate_sql, sample_context, status \
             FROM query_permission_policies WHERE id = $1",
        )
        .bind(policy_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(query_permission_policy_from_row).transpose()
    }

    pub async fn query_permission_policies(
        &self,
        filter: &QueryPermissionPolicyFilter<'_>,
    ) -> Result<Vec<QueryPermissionPolicy>, SqlStoreError> {
        let rows = sqlx::query(
            "SELECT id, organization_id, project_id, environment_id, name, table_schema, \
                table_name, operation, principal_claim, predicate_sql, sample_context, status \
             FROM query_permission_policies \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR table_schema = $4) \
               AND ($5::text IS NULL OR table_name = $5) \
               AND ($6::text IS NULL OR operation = $6) \
               AND ($7::text IS NULL OR status = $7) \
             ORDER BY updated_at DESC, id DESC",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.table_schema)
        .bind(filter.table_name)
        .bind(filter.operation.map(query_permission_operation_label))
        .bind(filter.status.map(query_permission_policy_status_label))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(query_permission_policy_from_row)
            .collect()
    }

    pub async fn permission_rule_document(
        &self,
        environment_id: &str,
    ) -> Result<Option<PermissionRuleDocument>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT environment_id, organization_id, project_id, dsl, updated_at::text AS updated_at \
             FROM permission_rule_documents WHERE environment_id = $1",
        )
        .bind(environment_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(permission_rule_document_from_row).transpose()
    }

    pub async fn upsert_permission_rule_document(
        &self,
        document: &PermissionRuleDocument,
    ) -> Result<PermissionRuleDocument, SqlStoreError> {
        let row = sqlx::query(
            "INSERT INTO permission_rule_documents \
                (environment_id, organization_id, project_id, dsl) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (environment_id) DO UPDATE SET \
                organization_id = EXCLUDED.organization_id, \
                project_id = EXCLUDED.project_id, \
                dsl = EXCLUDED.dsl, \
                updated_at = now() \
             RETURNING environment_id, organization_id, project_id, dsl, updated_at::text AS updated_at",
        )
        .bind(&document.environment_id)
        .bind(&document.organization_id)
        .bind(&document.project_id)
        .bind(&document.dsl)
        .fetch_one(&self.pool)
        .await?;
        permission_rule_document_from_row(row)
    }

    pub async fn environment_health(
        &self,
        environment_id: &str,
    ) -> Result<CustomerEnvironmentHealth, SqlStoreError> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM environments WHERE id = $1)")
                .bind(environment_id)
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            return Err(SqlStoreError::MissingResource(environment_id.to_owned()));
        }

        let mut components = Vec::new();
        let cluster_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    managed_postgres_clusters.lifecycle_state, \
                    managed_postgres_clusters.host_id, \
                    observed.postgres_running, \
                    node_hosts.storage_gib, \
                    node_hosts.used_storage_gib \
             FROM managed_postgres_clusters \
             LEFT JOIN node_hosts ON node_hosts.id = managed_postgres_clusters.host_id \
             LEFT JOIN node_host_observed_clusters observed \
               ON observed.host_id = managed_postgres_clusters.host_id \
              AND observed.cluster_id = managed_postgres_clusters.id \
             WHERE managed_postgres_clusters.environment_id = $1 \
               AND managed_postgres_clusters.lifecycle_state <> 'deleted' \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;

        if cluster_rows.is_empty() {
            push_health_component(
                &mut components,
                "database",
                CustomerEnvironmentHealthState::Unavailable,
                "environment has no managed Postgres cluster",
            );
        }

        for row in &cluster_rows {
            let cluster_id: String = row.try_get("cluster_id")?;
            let lifecycle_state: String = row.try_get("lifecycle_state")?;
            let lifecycle_state = parse_lifecycle_state(&lifecycle_state)?;
            let (state, detail) = database_health_for_cluster(&cluster_id, lifecycle_state);
            push_health_component(&mut components, "database", state, detail);
            if lifecycle_state == ClusterLifecycleState::Ready {
                let postgres_running: Option<bool> = row.try_get("postgres_running")?;
                let (state, detail) =
                    observed_database_health_for_cluster(&cluster_id, postgres_running);
                push_health_component(&mut components, "database_runtime", state, detail);
            }

            let storage_gib: Option<i32> = row.try_get("storage_gib")?;
            let used_storage_gib: Option<i32> = row.try_get("used_storage_gib")?;
            let (state, detail) =
                storage_health_for_cluster(&cluster_id, storage_gib, used_storage_gib);
            push_health_component(&mut components, "storage", state, detail);
        }

        let backup_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, backup.status AS backup_status \
             FROM managed_postgres_clusters \
             LEFT JOIN LATERAL ( \
                SELECT status FROM managed_postgres_backups \
                WHERE managed_postgres_backups.cluster_id = managed_postgres_clusters.id \
                ORDER BY created_at DESC \
                LIMIT 1 \
             ) backup ON true \
             WHERE managed_postgres_clusters.environment_id = $1 \
               AND managed_postgres_clusters.lifecycle_state <> 'deleted' \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        for row in backup_rows {
            let cluster_id: String = row.try_get("cluster_id")?;
            let backup_status: Option<String> = row.try_get("backup_status")?;
            let (state, detail) = backup_health_for_cluster(&cluster_id, backup_status.as_deref());
            push_health_component(&mut components, "backup", state, detail);
        }

        let restore_drill_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, drill.status AS drill_status \
             FROM managed_postgres_clusters \
             LEFT JOIN LATERAL ( \
                SELECT status FROM managed_postgres_restore_drills \
                WHERE managed_postgres_restore_drills.source_cluster_id = managed_postgres_clusters.id \
                ORDER BY created_at DESC \
                LIMIT 1 \
             ) drill ON true \
             WHERE managed_postgres_clusters.environment_id = $1 \
               AND managed_postgres_clusters.lifecycle_state <> 'deleted' \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        for row in restore_drill_rows {
            let cluster_id: String = row.try_get("cluster_id")?;
            let drill_status: Option<String> = row.try_get("drill_status")?;
            let (state, detail) =
                restore_drill_health_for_cluster(&cluster_id, drill_status.as_deref());
            push_health_component(&mut components, "restore_drill", state, detail);
        }

        let wal_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    COUNT(*) FILTER (WHERE managed_postgres_wal_archives.status = 'failed')::bigint AS failed_count, \
                    COUNT(*) FILTER (WHERE managed_postgres_wal_archives.status = 'succeeded')::bigint AS succeeded_count \
             FROM managed_postgres_clusters \
             LEFT JOIN managed_postgres_wal_archives \
                ON managed_postgres_wal_archives.cluster_id = managed_postgres_clusters.id \
             WHERE managed_postgres_clusters.environment_id = $1 \
               AND managed_postgres_clusters.lifecycle_state <> 'deleted' \
             GROUP BY managed_postgres_clusters.id \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        for row in wal_rows {
            let cluster_id: String = row.try_get("cluster_id")?;
            let failed_count: i64 = row.try_get("failed_count")?;
            let succeeded_count: i64 = row.try_get("succeeded_count")?;
            let (state, detail) =
                wal_health_for_cluster(&cluster_id, failed_count, succeeded_count);
            push_health_component(&mut components, "wal_archive", state, detail);
        }

        let pitr_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, pitr.status AS pitr_status \
             FROM managed_postgres_clusters \
             LEFT JOIN LATERAL ( \
                SELECT status FROM managed_postgres_pitr_checks \
                WHERE managed_postgres_pitr_checks.cluster_id = managed_postgres_clusters.id \
                ORDER BY created_at DESC \
                LIMIT 1 \
             ) pitr ON true \
             WHERE managed_postgres_clusters.environment_id = $1 \
               AND managed_postgres_clusters.lifecycle_state <> 'deleted' \
             ORDER BY managed_postgres_clusters.id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        for row in pitr_rows {
            let cluster_id: String = row.try_get("cluster_id")?;
            let pitr_status: Option<String> = row.try_get("pitr_status")?;
            let (state, detail) = pitr_health_for_cluster(&cluster_id, pitr_status.as_deref());
            push_health_component(&mut components, "pitr", state, detail);
        }

        let standby_check_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, standby_check.status AS check_status \
             FROM managed_postgres_clusters \
             JOIN managed_postgres_standbys \
                ON managed_postgres_standbys.source_cluster_id = managed_postgres_clusters.id \
             LEFT JOIN LATERAL ( \
                SELECT status FROM managed_postgres_standby_checks \
                WHERE managed_postgres_standby_checks.standby_id = managed_postgres_standbys.id \
                ORDER BY created_at DESC \
                LIMIT 1 \
             ) standby_check ON true \
             WHERE managed_postgres_clusters.environment_id = $1 \
               AND managed_postgres_clusters.lifecycle_state <> 'deleted' \
             ORDER BY managed_postgres_clusters.id, managed_postgres_standbys.id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        for row in standby_check_rows {
            let cluster_id: String = row.try_get("cluster_id")?;
            let check_status: Option<String> = row.try_get("check_status")?;
            let (state, detail) =
                standby_check_health_for_cluster(&cluster_id, check_status.as_deref());
            push_health_component(&mut components, "standby", state, detail);
        }

        let sync_rows = sqlx::query(
            "SELECT id, lifecycle_state FROM sync_deployments \
             WHERE environment_id = $1 \
             ORDER BY id",
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        if sync_rows.is_empty() {
            push_health_component(
                &mut components,
                "sync",
                CustomerEnvironmentHealthState::Degraded,
                "environment has no SyncDeployment",
            );
        }
        for row in sync_rows {
            let deployment_id: String = row.try_get("id")?;
            let lifecycle_state: String = row.try_get("lifecycle_state")?;
            let lifecycle_state = parse_sync_deployment_lifecycle_state(&lifecycle_state)?;
            let (state, detail) = sync_health_for_deployment(&deployment_id, lifecycle_state);
            push_health_component(&mut components, "sync", state, detail);
        }

        let state = strongest_health_state(&components);
        Ok(CustomerEnvironmentHealth {
            environment_id: environment_id.to_owned(),
            state,
            summary: health_summary(state),
            components,
        })
    }

    pub async fn metrics_snapshot(&self) -> Result<ControlPlaneMetricsSnapshot, SqlStoreError> {
        let agent_commands_failed_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM agent_commands WHERE status = 'failed'",
        )
        .fetch_one(&self.pool)
        .await?;

        let billing_exports_failed_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM billing_exports WHERE status = 'failed'",
        )
        .fetch_one(&self.pool)
        .await?;

        let wal_archive_failures_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM managed_postgres_wal_archives WHERE status = 'failed'",
        )
        .fetch_one(&self.pool)
        .await?;

        let quota_alerts_firing_total: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM quota_alerts WHERE state = 'firing'")
                .fetch_one(&self.pool)
                .await?;

        let backup_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    EXTRACT(EPOCH FROM MAX(managed_postgres_backups.completed_at))::double precision AS last_successful_backup_at \
             FROM managed_postgres_clusters \
             LEFT JOIN managed_postgres_backups \
                ON managed_postgres_backups.cluster_id = managed_postgres_clusters.id \
               AND managed_postgres_backups.status = 'succeeded' \
             GROUP BY managed_postgres_clusters.id \
             ORDER BY managed_postgres_clusters.id",
        )
        .fetch_all(&self.pool)
        .await?;
        let last_successful_backups = backup_rows
            .into_iter()
            .map(|row| {
                Ok(ClusterTimestampMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    timestamp_seconds: row.try_get("last_successful_backup_at")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let lifecycle_rows = sqlx::query(
            "SELECT id AS cluster_id, environment_id, lifecycle_state \
             FROM managed_postgres_clusters \
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        let cluster_lifecycle_states = lifecycle_rows
            .into_iter()
            .map(|row| {
                Ok(ClusterLifecycleMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    environment_id: row.try_get("environment_id")?,
                    lifecycle_state: row.try_get("lifecycle_state")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let cluster_storage_rows = sqlx::query(
            "SELECT id AS cluster_id, environment_id, host_id, storage_gib \
             FROM managed_postgres_clusters \
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        let cluster_storage_allocations = cluster_storage_rows
            .into_iter()
            .map(|row| {
                let storage_gib: i32 = row.try_get("storage_gib")?;
                Ok(ClusterStorageMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    environment_id: row.try_get("environment_id")?,
                    host_id: row.try_get("host_id")?,
                    storage_gib: u32::try_from(storage_gib).map_err(|_| {
                        SqlStoreError::InvalidValue(format!("invalid storage_gib {storage_gib}"))
                    })?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let wal_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    EXTRACT(EPOCH FROM MAX(managed_postgres_wal_archives.completed_at))::double precision AS last_successful_wal_archive_at \
             FROM managed_postgres_clusters \
             LEFT JOIN managed_postgres_wal_archives \
                ON managed_postgres_wal_archives.cluster_id = managed_postgres_clusters.id \
               AND managed_postgres_wal_archives.status = 'succeeded' \
             GROUP BY managed_postgres_clusters.id \
             ORDER BY managed_postgres_clusters.id",
        )
        .fetch_all(&self.pool)
        .await?;
        let last_successful_wal_archives = wal_rows
            .into_iter()
            .map(|row| {
                Ok(ClusterTimestampMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    timestamp_seconds: row.try_get("last_successful_wal_archive_at")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let pitr_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    EXTRACT(EPOCH FROM MAX(managed_postgres_pitr_checks.created_at))::double precision AS last_successful_pitr_check_at \
             FROM managed_postgres_clusters \
             LEFT JOIN managed_postgres_pitr_checks \
                ON managed_postgres_pitr_checks.cluster_id = managed_postgres_clusters.id \
               AND managed_postgres_pitr_checks.status = 'succeeded' \
             GROUP BY managed_postgres_clusters.id \
             ORDER BY managed_postgres_clusters.id",
        )
        .fetch_all(&self.pool)
        .await?;
        let last_successful_pitr_checks = pitr_rows
            .into_iter()
            .map(|row| {
                Ok(ClusterTimestampMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    timestamp_seconds: row.try_get("last_successful_pitr_check_at")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let restore_drill_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    EXTRACT(EPOCH FROM MAX(managed_postgres_restore_drills.completed_at))::double precision AS last_successful_restore_drill_at \
             FROM managed_postgres_clusters \
             LEFT JOIN managed_postgres_restore_drills \
                ON managed_postgres_restore_drills.source_cluster_id = managed_postgres_clusters.id \
               AND managed_postgres_restore_drills.status = 'succeeded' \
             GROUP BY managed_postgres_clusters.id \
             ORDER BY managed_postgres_clusters.id",
        )
        .fetch_all(&self.pool)
        .await?;
        let last_successful_restore_drills = restore_drill_rows
            .into_iter()
            .map(|row| {
                Ok(ClusterTimestampMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    timestamp_seconds: row.try_get("last_successful_restore_drill_at")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let standby_check_rows = sqlx::query(
            "SELECT managed_postgres_clusters.id AS cluster_id, \
                    EXTRACT(EPOCH FROM MAX(managed_postgres_standby_checks.completed_at))::double precision AS last_successful_standby_check_at \
             FROM managed_postgres_clusters \
             LEFT JOIN managed_postgres_standby_checks \
                ON managed_postgres_standby_checks.source_cluster_id = managed_postgres_clusters.id \
               AND managed_postgres_standby_checks.status = 'succeeded' \
             GROUP BY managed_postgres_clusters.id \
             ORDER BY managed_postgres_clusters.id",
        )
        .fetch_all(&self.pool)
        .await?;
        let last_successful_standby_checks = standby_check_rows
            .into_iter()
            .map(|row| {
                Ok(ClusterTimestampMetric {
                    cluster_id: row.try_get("cluster_id")?,
                    timestamp_seconds: row.try_get("last_successful_standby_check_at")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let quota_rows = sqlx::query(
            "SELECT quota_policies.environment_id, \
                    quota_policies.metric, \
                    CASE \
                        WHEN quota_policies.limit_quantity = 0 \
                            THEN CASE WHEN COALESCE(SUM(usage_events.quantity), 0) > 0 THEN 1.0 ELSE 0.0 END \
                        ELSE COALESCE(SUM(usage_events.quantity), 0)::double precision \
                             / quota_policies.limit_quantity::double precision \
                    END AS usage_ratio \
             FROM quota_policies \
             LEFT JOIN usage_events \
                ON usage_events.environment_id = quota_policies.environment_id \
               AND usage_events.metric = quota_policies.metric \
               AND usage_events.occurred_at >= now() - (quota_policies.window_seconds * interval '1 second') \
             GROUP BY quota_policies.environment_id, quota_policies.metric, quota_policies.limit_quantity \
             ORDER BY quota_policies.environment_id, quota_policies.metric",
        )
        .fetch_all(&self.pool)
        .await?;
        let quota_usage_ratios = quota_rows
            .into_iter()
            .map(|row| {
                Ok(QuotaUsageRatioMetric {
                    environment_id: row.try_get("environment_id")?,
                    metric: row.try_get("metric")?,
                    ratio: row.try_get("usage_ratio")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        let storage_rows = sqlx::query(
            "SELECT id AS host_id, \
                    CASE \
                        WHEN storage_gib = 0 THEN 0.0 \
                        ELSE used_storage_gib::double precision / storage_gib::double precision \
                    END AS used_ratio \
             FROM node_hosts \
             ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        let storage_used_ratios = storage_rows
            .into_iter()
            .map(|row| {
                Ok(HostStorageRatioMetric {
                    host_id: row.try_get("host_id")?,
                    ratio: row.try_get("used_ratio")?,
                })
            })
            .collect::<Result<Vec<_>, SqlStoreError>>()?;

        Ok(ControlPlaneMetricsSnapshot {
            agent_commands_failed_total: u64::try_from(agent_commands_failed_total).map_err(
                |_| SqlStoreError::InvalidValue("negative failed command count".to_owned()),
            )?,
            billing_exports_failed_total: u64::try_from(billing_exports_failed_total).map_err(
                |_| SqlStoreError::InvalidValue("negative failed billing export count".to_owned()),
            )?,
            wal_archive_failures_total: u64::try_from(wal_archive_failures_total).map_err(
                |_| SqlStoreError::InvalidValue("negative WAL archive failure count".to_owned()),
            )?,
            quota_alerts_firing_total: u64::try_from(quota_alerts_firing_total).map_err(|_| {
                SqlStoreError::InvalidValue("negative firing quota alert count".to_owned())
            })?,
            last_successful_backups,
            cluster_lifecycle_states,
            cluster_storage_allocations,
            last_successful_wal_archives,
            last_successful_pitr_checks,
            last_successful_restore_drills,
            last_successful_standby_checks,
            quota_usage_ratios,
            storage_used_ratios,
        })
    }

    pub async fn usage_events(
        &self,
        filter: &UsageEventFilter<'_>,
    ) -> Result<Vec<UsageEvent>, SqlStoreError> {
        let limit = i64::from(filter.limit.unwrap_or(1_000).min(10_000));
        let rows = sqlx::query(
            "SELECT id, idempotency_key, organization_id, project_id, environment_id, metric, quantity, occurred_at::text AS occurred_at, \
                    signature_key_id, signature_algorithm, signature \
             FROM usage_events \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR metric = $4) \
               AND ($5::timestamptz IS NULL OR occurred_at >= $5::timestamptz) \
               AND ($6::timestamptz IS NULL OR occurred_at < $6::timestamptz) \
             ORDER BY occurred_at, id \
             LIMIT $7",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.metric)
        .bind(filter.occurred_at_from)
        .bind(filter.occurred_at_to)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(usage_event_from_row).collect()
    }

    pub async fn create_billing_export(
        &self,
        export_id: &str,
        destination: &str,
        filter: &UsageEventFilter<'_>,
    ) -> Result<BillingExport, SqlStoreError> {
        let events = self.usage_events(filter).await?;
        let quantity_total = events.iter().try_fold(0_u64, |total, event| {
            total.checked_add(event.quantity).ok_or_else(|| {
                SqlStoreError::InvalidValue("billing export total overflows u64".to_owned())
            })
        })?;
        let event_count = u64::try_from(events.len()).map_err(|_| {
            SqlStoreError::InvalidValue("billing export event count overflows u64".to_owned())
        })?;
        let delivery = deliver_billing_export(export_id, destination, &events);
        let export = BillingExport {
            export_id: export_id.to_owned(),
            destination: destination.to_owned(),
            delivery_ref: delivery
                .as_ref()
                .ok()
                .map(|delivery| delivery.delivery_ref.clone()),
            organization_id: filter.organization_id.map(str::to_owned),
            project_id: filter.project_id.map(str::to_owned),
            environment_id: filter.environment_id.map(str::to_owned),
            metric: filter.metric.map(str::to_owned),
            occurred_at_from: filter.occurred_at_from.map(str::to_owned),
            occurred_at_to: filter.occurred_at_to.map(str::to_owned),
            event_count,
            quantity_total,
            status: if delivery.is_ok() {
                BillingExportStatus::Succeeded
            } else {
                BillingExportStatus::Failed
            },
            error_message: delivery.err().map(|err| err.to_string()),
            events,
        };

        sqlx::query(
            "INSERT INTO billing_exports \
                (id, destination, delivery_ref, organization_id, project_id, environment_id, metric, occurred_at_from, occurred_at_to, event_count, quantity_total, status, error_message, delivered_at, payload) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9::timestamptz, $10, $11, $12, $13, CASE WHEN $12 = 'succeeded' THEN now() ELSE NULL END, $14)",
        )
        .bind(&export.export_id)
        .bind(&export.destination)
        .bind(export.delivery_ref.as_deref())
        .bind(export.organization_id.as_deref())
        .bind(export.project_id.as_deref())
        .bind(export.environment_id.as_deref())
        .bind(export.metric.as_deref())
        .bind(export.occurred_at_from.as_deref())
        .bind(export.occurred_at_to.as_deref())
        .bind(checked_i64(export.event_count, "event_count")?)
        .bind(checked_i64(export.quantity_total, "quantity_total")?)
        .bind(billing_export_status_label(export.status))
        .bind(export.error_message.as_deref())
        .bind(Json(&export.events))
        .execute(&self.pool)
        .await?;

        Ok(export)
    }

    pub async fn billing_export(
        &self,
        export_id: &str,
    ) -> Result<Option<BillingExport>, SqlStoreError> {
        let row = sqlx::query(
            "SELECT id, destination, delivery_ref, organization_id, project_id, environment_id, metric, \
                    occurred_at_from::text AS occurred_at_from, occurred_at_to::text AS occurred_at_to, \
                    event_count, quantity_total, status, error_message, payload \
             FROM billing_exports WHERE id = $1",
        )
        .bind(export_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(billing_export_from_row).transpose()
    }

    pub async fn billing_exports(
        &self,
        filter: &BillingExportFilter<'_>,
    ) -> Result<Vec<BillingExport>, SqlStoreError> {
        let status = filter.status.map(billing_export_status_label);
        let limit = i64::from(filter.limit.unwrap_or(500).min(2_000));
        let rows = sqlx::query(
            "SELECT id, destination, delivery_ref, organization_id, project_id, environment_id, metric, \
                    occurred_at_from::text AS occurred_at_from, occurred_at_to::text AS occurred_at_to, \
                    event_count, quantity_total, status, error_message, payload \
             FROM billing_exports \
             WHERE ($1::text IS NULL OR organization_id = $1) \
               AND ($2::text IS NULL OR project_id = $2) \
               AND ($3::text IS NULL OR environment_id = $3) \
               AND ($4::text IS NULL OR metric = $4) \
               AND ($5::text IS NULL OR status = $5) \
             ORDER BY created_at DESC, id DESC \
             LIMIT $6",
        )
        .bind(filter.organization_id)
        .bind(filter.project_id)
        .bind(filter.environment_id)
        .bind(filter.metric)
        .bind(status)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(billing_export_from_row).collect()
    }
}

#[derive(Debug, Default)]
pub struct ApiKeyFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub include_revoked: Option<bool>,
}

#[derive(Debug, Default)]
pub struct JwtIssuerFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub status: Option<JwtIssuerStatus>,
}

#[derive(Debug, Default)]
pub struct WebhookEndpointFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub status: Option<WebhookEndpointStatus>,
}

#[derive(Debug, Default)]
pub struct SsoIdentityProviderFilter<'a> {
    pub organization_id: &'a str,
    pub kind: Option<SsoProviderKind>,
    pub status: Option<SsoProviderStatus>,
}

#[derive(Debug, Default)]
pub struct IncidentFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub severity: Option<IncidentSeverity>,
    pub status: Option<IncidentStatus>,
    pub include_resolved: Option<bool>,
}

#[derive(Debug, Default)]
pub struct BillingExportFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub metric: Option<&'a str>,
    pub status: Option<BillingExportStatus>,
    pub limit: Option<u32>,
}

#[derive(Debug, Default)]
pub struct NodeHostFilter<'a> {
    pub region: Option<&'a str>,
    pub failure_domain: Option<&'a str>,
    pub state: Option<NodeHostState>,
}

#[derive(Debug, Default)]
pub struct NodeHostAgentCredentialFilter {
    pub state: Option<NodeHostAgentCredentialState>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NodeHostHardeningCheckFilter<'a> {
    pub host_id: &'a str,
    pub status: Option<NodeHostHardeningStatus>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresClusterFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub lifecycle_state: Option<ClusterLifecycleState>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManagedPostgresRuntimeCheckFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<RuntimeCheckStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManagedPostgresMajorUpgradeFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<ManagedPostgresMajorUpgradeStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CloneRedactionPolicyFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub status: Option<CloneRedactionPolicyStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManagedPostgresSupportAccessSessionFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<SupportAccessStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SecretEncryptionKeyFilter<'a> {
    pub provider: Option<&'a str>,
    pub purpose: Option<&'a str>,
    pub status: Option<SecretEncryptionKeyStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SecretRewrapPlanFilter<'a> {
    pub source_key_ref: Option<&'a str>,
    pub target_key_ref: Option<&'a str>,
    pub status: Option<SecretRewrapPlanStatus>,
}

#[derive(Debug, Default)]
pub struct SyncDeploymentFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub lifecycle_state: Option<SyncDeploymentLifecycleState>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GatewayRouteFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DomainFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub verification_status: Option<DomainVerificationStatus>,
    pub tls_status: Option<DomainTlsStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IpAllowlistRuleFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub purpose: Option<IpAllowlistPurpose>,
    pub status: Option<IpAllowlistStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StaticEgressIpFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub region: Option<&'a str>,
    pub status: Option<StaticEgressIpStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceWindowFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub status: Option<MaintenanceWindowStatus>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseProxyRouteFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManagedPostgresEndpointCertificateFilter<'a> {
    pub environment_id: Option<&'a str>,
    pub status: Option<CertificateLifecycleState>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManagedPostgresCertificateAuthorityProviderFilter {
    pub status: Option<CertificateAuthorityProviderStatus>,
    pub default_for_managed_postgres: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
pub struct ImportedEndpointCertificateMaterial<'a> {
    pub certificate_pem: &'a str,
    pub private_key_pem: &'a str,
}

#[derive(Debug, Default)]
pub struct QuotaPolicyFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub metric: Option<&'a str>,
}

#[derive(Debug, Default)]
pub struct QuotaAlertFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub metric: Option<&'a str>,
    pub state: Option<QuotaAlertState>,
}

#[derive(Debug, Default)]
pub struct QueryPermissionPolicyFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub table_schema: Option<&'a str>,
    pub table_name: Option<&'a str>,
    pub operation: Option<QueryPermissionOperation>,
    pub status: Option<QueryPermissionPolicyStatus>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresBackupFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<BackupLifecycleState>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresBackupArtifactFilter<'a> {
    pub cluster_id: &'a str,
    pub backup_id: &'a str,
    pub status: Option<BackupArtifactStatus>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresDeletionTombstoneFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub expired: Option<bool>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresRestoreFilter<'a> {
    pub source_cluster_id: &'a str,
    pub status: Option<RestoreLifecycleState>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresRestoreDrillFilter<'a> {
    pub source_cluster_id: &'a str,
    pub status: Option<RestoreLifecycleState>,
}

#[derive(Debug, Default)]
pub struct WalArchiveSegmentFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<WalArchiveSegmentStatus>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresPitrCheckFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<PitrCheckStatus>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresFailoverFilter<'a> {
    pub source_cluster_id: &'a str,
    pub status: Option<FailoverLifecycleState>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresStandbyFilter<'a> {
    pub source_cluster_id: &'a str,
    pub status: Option<StandbyLifecycleState>,
}

#[derive(Debug, Default)]
pub struct ManagedPostgresStandbyCheckFilter<'a> {
    pub standby_id: &'a str,
    pub status: Option<StandbyCheckStatus>,
}

#[derive(Debug, Default)]
pub struct OperationFilter<'a> {
    pub cluster_id: &'a str,
    pub kind: Option<OperationKind>,
    pub status: Option<OperationStatus>,
}

#[derive(Debug, Default)]
pub struct AgentCommandFilter<'a> {
    pub cluster_id: &'a str,
    pub status: Option<AgentCommandStatus>,
}

#[derive(Debug, Default)]
pub struct AuditEventFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub actor_id: Option<&'a str>,
    pub action: Option<&'a str>,
    pub resource_id: Option<&'a str>,
    pub limit: Option<u32>,
}

#[derive(Debug, Default)]
pub struct UsageEventFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub environment_id: Option<&'a str>,
    pub metric: Option<&'a str>,
    pub occurred_at_from: Option<&'a str>,
    pub occurred_at_to: Option<&'a str>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ControlPlaneMetricsSnapshot {
    pub agent_commands_failed_total: u64,
    pub billing_exports_failed_total: u64,
    pub wal_archive_failures_total: u64,
    pub quota_alerts_firing_total: u64,
    pub last_successful_backups: Vec<ClusterTimestampMetric>,
    pub cluster_lifecycle_states: Vec<ClusterLifecycleMetric>,
    pub cluster_storage_allocations: Vec<ClusterStorageMetric>,
    pub last_successful_wal_archives: Vec<ClusterTimestampMetric>,
    pub last_successful_pitr_checks: Vec<ClusterTimestampMetric>,
    pub last_successful_restore_drills: Vec<ClusterTimestampMetric>,
    pub last_successful_standby_checks: Vec<ClusterTimestampMetric>,
    pub quota_usage_ratios: Vec<QuotaUsageRatioMetric>,
    pub storage_used_ratios: Vec<HostStorageRatioMetric>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterLifecycleMetric {
    pub cluster_id: String,
    pub environment_id: String,
    pub lifecycle_state: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterStorageMetric {
    pub cluster_id: String,
    pub environment_id: String,
    pub host_id: Option<String>,
    pub storage_gib: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterTimestampMetric {
    pub cluster_id: String,
    pub timestamp_seconds: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaUsageRatioMetric {
    pub environment_id: String,
    pub metric: String,
    pub ratio: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HostStorageRatioMetric {
    pub host_id: String,
    pub ratio: f64,
}

fn config_version_from_row(row: sqlx::postgres::PgRow) -> Result<ConfigVersion, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ConfigVersion {
        config_version: row.try_get("id")?,
        environment_id: row.try_get("environment_id")?,
        rendered_hash: row.try_get("rendered_hash")?,
        status: parse_config_status(&status)?,
    })
}

fn managed_postgres_cluster_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresCluster, SqlStoreError> {
    let postgres_version: String = row.try_get("postgres_version")?;
    let storage_gib: i32 = row.try_get("storage_gib")?;
    let lifecycle_state: String = row.try_get("lifecycle_state")?;
    let host_id: Option<String> = row.try_get("host_id")?;
    let host_data_dir: Option<String> = row.try_get("host_data_dir")?;
    let host_port: Option<i32> = row.try_get("host_port")?;

    let host_assignment = match (host_id, host_data_dir, host_port) {
        (Some(host_id), Some(data_dir), Some(port)) => Some(HostAssignment {
            host_id,
            data_dir,
            port: u16::try_from(port)
                .map_err(|_| SqlStoreError::InvalidValue(format!("invalid host port {port}")))?,
        }),
        _ => None,
    };

    Ok(ManagedPostgresCluster {
        cluster_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        region: row.try_get("region")?,
        postgres_version: PostgresVersion::new(postgres_version)?,
        tier: row.try_get("tier")?,
        storage_gib: u32::try_from(storage_gib).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid storage_gib {storage_gib}"))
        })?,
        lifecycle_state: parse_lifecycle_state(&lifecycle_state)?,
        host_assignment,
    })
}

fn node_host_from_row(row: sqlx::postgres::PgRow) -> Result<NodeHost, SqlStoreError> {
    let first_port: i32 = row.try_get("first_port")?;
    let max_clusters: i32 = row.try_get("max_clusters")?;
    let assigned_clusters: i32 = row.try_get("assigned_clusters")?;
    let storage_gib: i32 = row.try_get("storage_gib")?;
    let used_storage_gib: i32 = row.try_get("used_storage_gib")?;
    let state: String = row.try_get("state")?;
    Ok(NodeHost {
        host_id: row.try_get("id")?,
        region: row.try_get("region")?,
        failure_domain: row.try_get("failure_domain")?,
        data_root: row.try_get("data_root")?,
        first_port: u16::try_from(first_port)
            .map_err(|_| SqlStoreError::InvalidValue(format!("invalid first_port {first_port}")))?,
        state: parse_node_host_state(&state)?,
        capacity: palimpsest_paas_core::NodeHostCapacity {
            max_clusters: u32::try_from(max_clusters).map_err(|_| {
                SqlStoreError::InvalidValue(format!("invalid max_clusters {max_clusters}"))
            })?,
            assigned_clusters: u32::try_from(assigned_clusters).map_err(|_| {
                SqlStoreError::InvalidValue(format!(
                    "invalid assigned_clusters {assigned_clusters}"
                ))
            })?,
            storage_gib: u32::try_from(storage_gib).map_err(|_| {
                SqlStoreError::InvalidValue(format!("invalid storage_gib {storage_gib}"))
            })?,
            used_storage_gib: u32::try_from(used_storage_gib).map_err(|_| {
                SqlStoreError::InvalidValue(format!("invalid used_storage_gib {used_storage_gib}"))
            })?,
        },
    })
}

fn node_host_agent_credential_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<NodeHostAgentCredential, SqlStoreError> {
    let state: String = row.try_get("state")?;
    Ok(NodeHostAgentCredential {
        host_id: row.try_get("host_id")?,
        key_id: row.try_get("id")?,
        state: parse_node_host_agent_credential_state(&state)?,
        created_at: row.try_get("created_at")?,
        rotated_at: row.try_get("rotated_at")?,
        revoked_at: row.try_get("revoked_at")?,
        last_used_at: row.try_get("last_used_at")?,
        last_used_operation: row.try_get("last_used_operation")?,
    })
}

fn node_host_hardening_check_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<NodeHostHardeningCheck, SqlStoreError> {
    let status: String = row.try_get("status")?;
    let postgres_major_min: i32 = row.try_get("postgres_major_min")?;
    Ok(NodeHostHardeningCheck {
        check_id: row.try_get("id")?,
        host_id: row.try_get("host_id")?,
        status: parse_node_host_hardening_status(&status)?,
        image_ref: row.try_get("image_ref")?,
        os_release: row.try_get("os_release")?,
        kernel_version: row.try_get("kernel_version")?,
        postgres_major_min: u16::try_from(postgres_major_min).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid postgres_major_min {postgres_major_min}"))
        })?,
        container_runtime: row.try_get("container_runtime")?,
        disk_encryption: row.try_get("disk_encryption")?,
        firewall_enabled: row.try_get("firewall_enabled")?,
        unattended_upgrades: row.try_get("unattended_upgrades")?,
        last_patched_at: row.try_get("last_patched_at")?,
        checked_at: row.try_get("checked_at")?,
        error_message: row.try_get("error_message")?,
    })
}

fn sync_deployment_from_row(row: sqlx::postgres::PgRow) -> Result<SyncDeployment, SqlStoreError> {
    let lifecycle_state: String = row.try_get("lifecycle_state")?;
    Ok(SyncDeployment {
        deployment_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        managed_postgres_cluster_id: row.try_get("managed_postgres_cluster_id")?,
        config_version: row.try_get("config_version")?,
        lifecycle_state: parse_sync_deployment_lifecycle_state(&lifecycle_state)?,
    })
}

fn gateway_route_from_row(row: sqlx::postgres::PgRow) -> Result<GatewayRoute, SqlStoreError> {
    let tls_policy: String = row.try_get("tls_policy")?;
    let mtls_ca_secret_ref: Option<String> = row.try_get("mtls_ca_secret_ref")?;
    let mtls_client_certificate_secret_ref: Option<String> =
        row.try_get("mtls_client_certificate_secret_ref")?;
    let mtls_client_private_key_secret_ref: Option<String> =
        row.try_get("mtls_client_private_key_secret_ref")?;
    let mtls_server_name: Option<String> = row.try_get("mtls_server_name")?;
    let max_connections: i32 = row.try_get("max_connections")?;
    let max_requests_per_minute: i32 = row.try_get("max_requests_per_minute")?;
    Ok(GatewayRoute {
        host: row.try_get("host")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        sync_endpoint: row.try_get("sync_endpoint")?,
        tls_policy: parse_gateway_tls_policy(
            &tls_policy,
            mtls_ca_secret_ref,
            mtls_client_certificate_secret_ref,
            mtls_client_private_key_secret_ref,
            mtls_server_name,
        )?,
        rate_limit: RateLimitPolicy {
            max_connections: u32::try_from(max_connections).map_err(|_| {
                SqlStoreError::InvalidValue(format!(
                    "invalid gateway max_connections '{max_connections}'"
                ))
            })?,
            max_requests_per_minute: u32::try_from(max_requests_per_minute).map_err(|_| {
                SqlStoreError::InvalidValue(format!(
                    "invalid gateway max_requests_per_minute '{max_requests_per_minute}'"
                ))
            })?,
        },
    })
}

fn domain_from_row(row: sqlx::postgres::PgRow) -> Result<Domain, SqlStoreError> {
    let verification_status: String = row.try_get("verification_status")?;
    let tls_status: String = row.try_get("tls_status")?;
    Ok(Domain {
        domain_id: row.try_get("id")?,
        hostname: row.try_get("hostname")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        route_host: row.try_get("route_host")?,
        verification_status: parse_domain_verification_status(&verification_status)?,
        verification_token: row.try_get("verification_token")?,
        tls_status: parse_domain_tls_status(&tls_status)?,
    })
}

fn ip_allowlist_rule_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<IpAllowlistRule, SqlStoreError> {
    let purpose: String = row.try_get("purpose")?;
    let status: String = row.try_get("status")?;
    Ok(IpAllowlistRule {
        rule_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        name: row.try_get("name")?,
        cidr: row.try_get("cidr")?,
        purpose: parse_ip_allowlist_purpose(&purpose)?,
        status: parse_ip_allowlist_status(&status)?,
    })
}

fn static_egress_ip_from_row(row: sqlx::postgres::PgRow) -> Result<StaticEgressIp, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(StaticEgressIp {
        egress_ip_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        region: row.try_get("region")?,
        ip_address: row.try_get("ip_address")?,
        provider_ref: row.try_get("provider_ref")?,
        status: parse_static_egress_ip_status(&status)?,
    })
}

fn maintenance_window_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MaintenanceWindow, SqlStoreError> {
    let day_of_week: String = row.try_get("day_of_week")?;
    let duration_minutes: i32 = row.try_get("duration_minutes")?;
    let status: String = row.try_get("status")?;
    Ok(MaintenanceWindow {
        window_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        name: row.try_get("name")?,
        day_of_week: parse_maintenance_day_of_week(&day_of_week)?,
        start_time: row.try_get("start_time")?,
        duration_minutes: u32::try_from(duration_minutes).map_err(|_| {
            SqlStoreError::InvalidValue(format!(
                "invalid maintenance duration '{duration_minutes}'"
            ))
        })?,
        auto_minor_upgrades: row.try_get("auto_minor_upgrades")?,
        status: parse_maintenance_window_status(&status)?,
    })
}

fn database_proxy_route_select_sql(where_clause: &str, order_by: &str) -> String {
    format!(
        "SELECT database_proxy_routes.listen_addr, database_proxy_routes.upstream_addr, \
                database_proxy_routes.organization_id, database_proxy_routes.project_id, \
                database_proxy_routes.environment_id, database_proxy_routes.cluster_id, \
                endpoints.environment_id AS managed_endpoint_environment_id, \
                certs.id AS certificate_id, certs.common_name AS certificate_common_name, \
                certs.fingerprint_sha256 AS certificate_fingerprint_sha256, \
                certs.certificate_secret_ref, certs.private_key_secret_ref, \
                cert_secret.provider AS certificate_provider, \
                cert_secret.external_ref AS certificate_external_ref, \
                key_secret.provider AS private_key_provider, \
                key_secret.external_ref AS private_key_external_ref \
         FROM database_proxy_routes \
         LEFT JOIN managed_postgres_endpoints endpoints \
            ON endpoints.environment_id = database_proxy_routes.environment_id \
           AND endpoints.database_proxy_listen_addr = database_proxy_routes.listen_addr \
         LEFT JOIN managed_postgres_endpoint_certificates certs \
            ON certs.id = endpoints.active_certificate_id \
           AND certs.status = 'active' \
         LEFT JOIN secret_refs cert_secret ON cert_secret.id = certs.certificate_secret_ref \
         LEFT JOIN secret_refs key_secret ON key_secret.id = certs.private_key_secret_ref \
         WHERE {where_clause} \
         ORDER BY {order_by}"
    )
}

fn database_proxy_route_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<DatabaseProxyRoute, SqlStoreError> {
    let certificate_id: Option<String> = row.try_get("certificate_id")?;
    let tls = if let Some(certificate_id) = certificate_id {
        Some(DatabaseProxyTlsConfig {
            mode: DatabaseProxyTlsMode::TerminateAtProxy,
            certificate_id,
            common_name: row.try_get("certificate_common_name")?,
            certificate_secret_ref: SecretRef {
                secret_id: row.try_get("certificate_secret_ref")?,
                provider: row.try_get("certificate_provider")?,
                external_ref: row.try_get("certificate_external_ref")?,
            },
            private_key_secret_ref: SecretRef {
                secret_id: row.try_get("private_key_secret_ref")?,
                provider: row.try_get("private_key_provider")?,
                external_ref: row.try_get("private_key_external_ref")?,
            },
            fingerprint_sha256: row.try_get("certificate_fingerprint_sha256")?,
        })
    } else {
        None
    };
    let cluster_id: String = row.try_get("cluster_id")?;
    let managed_endpoint_environment_id: Option<String> =
        row.try_get("managed_endpoint_environment_id")?;
    let policy = managed_endpoint_environment_id
        .as_ref()
        .and_then(|_| managed_postgres_database_proxy_policy(&cluster_id));
    Ok(DatabaseProxyRoute {
        listen_addr: row.try_get("listen_addr")?,
        upstream_addr: row.try_get("upstream_addr")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        policy,
        cluster_id,
        tls,
    })
}

fn managed_postgres_backup_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresBackup, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresBackup {
        backup_id: row.try_get("id")?,
        cluster_id: row.try_get("cluster_id")?,
        status: parse_backup_status(&status)?,
        backup_dir: row.try_get("backup_dir")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_backup_artifact_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresBackupArtifact, SqlStoreError> {
    let status: String = row.try_get("status")?;
    let size_bytes = row
        .try_get::<Option<i64>, _>("size_bytes")?
        .map(|value| checked_u64(value, "size_bytes"))
        .transpose()?;
    Ok(ManagedPostgresBackupArtifact {
        artifact_id: row.try_get("id")?,
        backup_id: row.try_get("backup_id")?,
        cluster_id: row.try_get("cluster_id")?,
        provider: row.try_get("provider")?,
        object_uri: row.try_get("object_uri")?,
        manifest_path: row.try_get("manifest_path")?,
        manifest_sha256: row.try_get("manifest_sha256")?,
        size_bytes,
        status: parse_backup_artifact_status(&status)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_backup_retention_policy_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresBackupRetentionPolicy, SqlStoreError> {
    Ok(ManagedPostgresBackupRetentionPolicy {
        cluster_id: row.try_get("cluster_id")?,
        retention_days: checked_u32(row.try_get::<i32, _>("retention_days")?, "retention_days")?,
        keep_min_successful_backups: checked_u32(
            row.try_get::<i32, _>("keep_min_successful_backups")?,
            "keep_min_successful_backups",
        )?,
        enabled: row.try_get("enabled")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn managed_postgres_clone_redaction_policy_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresCloneRedactionPolicy, SqlStoreError> {
    let status: String = row.try_get("status")?;
    let rules: Json<Vec<CloneRedactionRule>> = row.try_get("rules")?;
    Ok(ManagedPostgresCloneRedactionPolicy {
        policy_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        name: row.try_get("name")?,
        status: parse_clone_redaction_policy_status(&status)?,
        rules: rules.0,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn managed_postgres_support_access_session_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresSupportAccessSession, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresSupportAccessSession {
        session_id: row.try_get("id")?,
        cluster_id: row.try_get("cluster_id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        requested_by: row.try_get("requested_by")?,
        approved_by: row.try_get("approved_by")?,
        revoked_by: row.try_get("revoked_by")?,
        reason: row.try_get("reason")?,
        ticket_ref: row.try_get("ticket_ref")?,
        status: parse_support_access_status(&status)?,
        requested_at: row.try_get("requested_at")?,
        approved_at: row.try_get("approved_at")?,
        expires_at: row.try_get("expires_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn secret_encryption_key_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<SecretEncryptionKey, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(SecretEncryptionKey {
        key_ref: row.try_get("key_ref")?,
        provider: row.try_get("provider")?,
        purpose: row.try_get("purpose")?,
        status: parse_secret_encryption_key_status(&status)?,
        created_at: row.try_get("created_at")?,
        activated_at: row.try_get("activated_at")?,
        retired_at: row.try_get("retired_at")?,
    })
}

fn secret_rewrap_plan_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<SecretRewrapPlan, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(SecretRewrapPlan {
        plan_id: row.try_get("id")?,
        source_key_ref: row.try_get("source_key_ref")?,
        target_key_ref: row.try_get("target_key_ref")?,
        status: parse_secret_rewrap_plan_status(&status)?,
        matched_secret_count: checked_u32(
            row.try_get::<i32, _>("matched_secret_count")?,
            "matched_secret_count",
        )?,
        rewrapped_secret_count: checked_u32(
            row.try_get::<i32, _>("rewrapped_secret_count")?,
            "rewrapped_secret_count",
        )?,
        error_message: row.try_get("error_message")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn managed_postgres_runtime_check_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresRuntimeCheck, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresRuntimeCheck {
        check_id: row.try_get("id")?,
        cluster_id: row.try_get("cluster_id")?,
        status: parse_runtime_check_status(&status)?,
        connection_count: checked_u32(
            row.try_get::<i32, _>("connection_count")?,
            "connection_count",
        )?,
        max_connections: checked_u32(row.try_get::<i32, _>("max_connections")?, "max_connections")?,
        replication_slot_lag_bytes: row
            .try_get::<Option<i64>, _>("replication_slot_lag_bytes")?
            .map(|value| checked_u64(value, "replication_slot_lag_bytes"))
            .transpose()?,
        long_running_query_count: checked_u32(
            row.try_get::<i32, _>("long_running_query_count")?,
            "long_running_query_count",
        )?,
        blocked_lock_count: checked_u32(
            row.try_get::<i32, _>("blocked_lock_count")?,
            "blocked_lock_count",
        )?,
        oldest_transaction_age_seconds: row
            .try_get::<Option<i64>, _>("oldest_transaction_age_seconds")?
            .map(|value| checked_u64(value, "oldest_transaction_age_seconds"))
            .transpose()?,
        autovacuum_running: row.try_get("autovacuum_running")?,
        checked_at: row.try_get("checked_at")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_major_upgrade_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresMajorUpgrade, SqlStoreError> {
    let source_postgres_version: String = row.try_get("source_postgres_version")?;
    let target_postgres_version: String = row.try_get("target_postgres_version")?;
    let strategy: String = row.try_get("strategy")?;
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresMajorUpgrade {
        upgrade_id: row.try_get("id")?,
        cluster_id: row.try_get("cluster_id")?,
        source_postgres_version: PostgresVersion::new(source_postgres_version)?,
        target_postgres_version: PostgresVersion::new(target_postgres_version)?,
        strategy: parse_major_upgrade_strategy(&strategy)?,
        status: parse_major_upgrade_status(&status)?,
        command_id: row.try_get("command_id")?,
        operation_id: row.try_get("operation_id")?,
        error_message: row.try_get("error_message")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn managed_postgres_deletion_tombstone_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresDeletionTombstone, SqlStoreError> {
    let postgres_version: String = row.try_get("postgres_version")?;
    Ok(ManagedPostgresDeletionTombstone {
        cluster_id: row.try_get("cluster_id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        region: row.try_get("region")?,
        postgres_version: PostgresVersion::new(postgres_version)?,
        tier: row.try_get("tier")?,
        retained_backup_id: row.try_get("retained_backup_id")?,
        deleted_at: row.try_get("deleted_at")?,
        retention_expires_at: row.try_get("retention_expires_at")?,
        unrecoverable_at: row.try_get("unrecoverable_at")?,
        expired_at: row.try_get("expired_at")?,
    })
}

fn managed_postgres_restore_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresRestore, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresRestore {
        restore_id: row.try_get("id")?,
        source_cluster_id: row.try_get("source_cluster_id")?,
        target_cluster_id: row.try_get("target_cluster_id")?,
        target_environment_id: row.try_get("target_environment_id")?,
        backup_id: row.try_get("backup_id")?,
        status: parse_restore_status(&status)?,
        redaction_policy_id: row.try_get("redaction_policy_id")?,
        recovery_target_lsn: row.try_get("recovery_target_lsn")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_restore_drill_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresRestoreDrill, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresRestoreDrill {
        drill_id: row.try_get("id")?,
        source_cluster_id: row.try_get("source_cluster_id")?,
        restore_id: row.try_get("restore_id")?,
        backup_id: row.try_get("backup_id")?,
        target_cluster_id: row.try_get("target_cluster_id")?,
        status: parse_restore_status(&status)?,
        error_message: row.try_get("error_message")?,
    })
}

fn wal_archive_segment_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresWalArchiveSegment, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresWalArchiveSegment {
        cluster_id: row.try_get("cluster_id")?,
        segment_name: row.try_get("segment_name")?,
        status: parse_wal_archive_segment_status(&status)?,
        archive_dir: row.try_get("archive_dir")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_pitr_check_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresPitrCheck, SqlStoreError> {
    let status: String = row.try_get("status")?;
    let segment_count: i32 = row.try_get("segment_count")?;
    Ok(ManagedPostgresPitrCheck {
        check_id: row.try_get("id")?,
        cluster_id: row.try_get("cluster_id")?,
        backup_id: row.try_get("backup_id")?,
        status: parse_pitr_check_status(&status)?,
        segment_count: u32::try_from(segment_count).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid PITR segment count '{segment_count}'"))
        })?,
        first_segment: row.try_get("first_segment")?,
        latest_segment: row.try_get("latest_segment")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_failover_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresFailover, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresFailover {
        failover_id: row.try_get("id")?,
        source_cluster_id: row.try_get("source_cluster_id")?,
        target_cluster_id: row.try_get("target_cluster_id")?,
        status: parse_failover_status(&status)?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_standby_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresStandby, SqlStoreError> {
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresStandby {
        standby_id: row.try_get("id")?,
        source_cluster_id: row.try_get("source_cluster_id")?,
        target_cluster_id: row.try_get("target_cluster_id")?,
        backup_id: row.try_get("backup_id")?,
        status: parse_standby_status(&status)?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_standby_check_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresStandbyCheck, SqlStoreError> {
    let status: String = row.try_get("status")?;
    let max_lag_bytes: i64 = row.try_get("max_lag_bytes")?;
    Ok(ManagedPostgresStandbyCheck {
        check_id: row.try_get("id")?,
        standby_id: row.try_get("standby_id")?,
        source_cluster_id: row.try_get("source_cluster_id")?,
        target_cluster_id: row.try_get("target_cluster_id")?,
        slot_name: row.try_get("slot_name")?,
        max_lag_bytes: u64::try_from(max_lag_bytes).map_err(|_| {
            SqlStoreError::InvalidValue(format!(
                "invalid standby check max_lag_bytes '{max_lag_bytes}'"
            ))
        })?,
        status: parse_standby_check_status(&status)?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_endpoint_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresEndpoint, SqlStoreError> {
    Ok(ManagedPostgresEndpoint {
        environment_id: row.try_get("environment_id")?,
        active_cluster_id: row.try_get("active_cluster_id")?,
        updated_by_failover_id: row.try_get("updated_by_failover_id")?,
        database_proxy_listen_addr: row.try_get("database_proxy_listen_addr")?,
        active_certificate_id: row.try_get("active_certificate_id")?,
    })
}

fn managed_postgres_endpoint_certificate_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresEndpointCertificate, SqlStoreError> {
    let status: String = row.try_get("status")?;
    let certificate_secret_id: Option<String> = row.try_get("certificate_secret_ref")?;
    let certificate_secret_ref = if let Some(secret_id) = certificate_secret_id {
        Some(SecretRef {
            secret_id,
            provider: row.try_get("certificate_provider")?,
            external_ref: row.try_get("certificate_external_ref")?,
        })
    } else {
        None
    };
    Ok(ManagedPostgresEndpointCertificate {
        certificate_id: row.try_get("id")?,
        environment_id: row.try_get("environment_id")?,
        listen_addr: row.try_get("listen_addr")?,
        common_name: row.try_get("common_name")?,
        status: parse_certificate_status(&status)?,
        certificate_secret_ref,
        private_key_secret_ref: SecretRef {
            secret_id: row.try_get("private_key_secret_ref")?,
            provider: row.try_get("private_key_provider")?,
            external_ref: row.try_get("private_key_external_ref")?,
        },
        not_before: row.try_get("not_before")?,
        not_after: row.try_get("not_after")?,
        fingerprint_sha256: row.try_get("fingerprint_sha256")?,
        issued_by: row.try_get("issued_by")?,
        error_message: row.try_get("error_message")?,
    })
}

fn managed_postgres_acme_order_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresAcmeOrder, SqlStoreError> {
    let challenge_type: String = row.try_get("challenge_type")?;
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresAcmeOrder {
        order_id: row.try_get("id")?,
        certificate_id: row.try_get("certificate_id")?,
        environment_id: row.try_get("environment_id")?,
        ca_provider_id: row.try_get("ca_provider_id")?,
        common_name: row.try_get("common_name")?,
        challenge_type: parse_acme_challenge_type(&challenge_type)?,
        challenge_token: row.try_get("challenge_token")?,
        key_authorization_secret_ref: SecretRef {
            secret_id: row.try_get("key_authorization_secret_ref")?,
            provider: row.try_get("key_authorization_provider")?,
            external_ref: row.try_get("key_authorization_external_ref")?,
        },
        csr_secret_ref: SecretRef {
            secret_id: row.try_get("csr_secret_ref")?,
            provider: row.try_get("csr_provider")?,
            external_ref: row.try_get("csr_external_ref")?,
        },
        directory_url: row.try_get("directory_url")?,
        account_ref: row.try_get("account_ref")?,
        status: parse_acme_order_status(&status)?,
        error_message: row.try_get("error_message")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn managed_postgres_certificate_authority_provider_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagedPostgresCertificateAuthorityProvider, SqlStoreError> {
    let kind: String = row.try_get("kind")?;
    let status: String = row.try_get("status")?;
    Ok(ManagedPostgresCertificateAuthorityProvider {
        ca_provider_id: row.try_get("id")?,
        name: row.try_get("name")?,
        kind: parse_ca_provider_kind(&kind)?,
        issuer_ref: row.try_get("issuer_ref")?,
        status: parse_ca_provider_status(&status)?,
        default_for_managed_postgres: row.try_get("default_for_managed_postgres")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn operation_from_row(row: sqlx::postgres::PgRow) -> Result<OperationRecord, SqlStoreError> {
    let kind: String = row.try_get("kind")?;
    let status: String = row.try_get("status")?;
    Ok(OperationRecord {
        operation_id: row.try_get("id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        target_resource_id: row.try_get("target_resource_id")?,
        kind: parse_operation_kind(&kind)?,
        status: parse_operation_status(&status)?,
        current_step: row.try_get("current_step")?,
        lease_owner: row.try_get("lease_owner")?,
    })
}

fn queued_command_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<QueuedNodeAgentCommand, SqlStoreError> {
    let action: Json<NodeAgentAction> = row.try_get("action")?;
    let status: String = row.try_get("status")?;
    let attempts: i32 = row.try_get("attempts")?;
    Ok(QueuedNodeAgentCommand {
        host_id: row.try_get("host_id")?,
        operation_id: row.try_get("operation_id")?,
        command: NodeAgentCommand {
            command_id: row.try_get("id")?,
            cluster_id: row.try_get("cluster_id")?,
            action: action.0,
        },
        status: parse_agent_command_status(&status)?,
        attempts: u32::try_from(attempts)
            .map_err(|_| SqlStoreError::InvalidValue(format!("invalid attempts {attempts}")))?,
        last_error: row.try_get("last_error")?,
        operation_token: row.try_get("operation_token")?,
    })
}

fn api_key_from_row(row: &sqlx::postgres::PgRow) -> Result<ApiKey, SqlStoreError> {
    let role: String = row.try_get("role")?;
    Ok(ApiKey {
        key_id: row.try_get("id")?,
        token_prefix: row.try_get("token_prefix")?,
        name: row.try_get("name")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        role: parse_team_role(&role)?,
        created_by: row.try_get("created_by")?,
        revoked: row.try_get("revoked")?,
    })
}

fn team_membership_from_row(row: &sqlx::postgres::PgRow) -> Result<TeamMembership, SqlStoreError> {
    let role: String = row.try_get("role")?;
    Ok(TeamMembership {
        organization_id: row.try_get("organization_id")?,
        actor_id: row.try_get("actor_id")?,
        role: parse_team_role(&role)?,
    })
}

fn jwt_issuer_from_row(row: &sqlx::postgres::PgRow) -> Result<JwtIssuer, SqlStoreError> {
    let claim_to_field: Json<Vec<palimpsest_paas_core::JwtClaimMapping>> =
        row.try_get("claim_to_field")?;
    let status: String = row.try_get("status")?;
    Ok(JwtIssuer {
        issuer_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        name: row.try_get("name")?,
        issuer: row.try_get("issuer")?,
        audience: row.try_get("audience")?,
        jwks_url: row.try_get("jwks_url")?,
        claim_to_field: claim_to_field.0,
        status: parse_jwt_issuer_status(&status)?,
    })
}

fn webhook_endpoint_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<WebhookEndpoint, SqlStoreError> {
    let event_types: Json<Vec<String>> = row.try_get("event_types")?;
    let signing_secret_ref: Option<Json<SecretRef>> = row.try_get("signing_secret_ref")?;
    let status: String = row.try_get("status")?;
    Ok(WebhookEndpoint {
        endpoint_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        name: row.try_get("name")?,
        url: row.try_get("url")?,
        event_types: event_types.0,
        signing_secret_ref: signing_secret_ref.map(|value| value.0),
        status: parse_webhook_endpoint_status(&status)?,
    })
}

fn sso_identity_provider_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<SsoIdentityProvider, SqlStoreError> {
    let kind: String = row.try_get("kind")?;
    let certificate_secret_ref: Option<Json<SecretRef>> = row.try_get("certificate_secret_ref")?;
    let claim_mappings: Json<Vec<palimpsest_paas_core::SsoClaimMapping>> =
        row.try_get("claim_mappings")?;
    let status: String = row.try_get("status")?;
    Ok(SsoIdentityProvider {
        provider_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        name: row.try_get("name")?,
        kind: parse_sso_provider_kind(&kind)?,
        issuer: row.try_get("issuer")?,
        sso_url: row.try_get("sso_url")?,
        certificate_secret_ref: certificate_secret_ref.map(|value| value.0),
        claim_mappings: claim_mappings.0,
        status: parse_sso_provider_status(&status)?,
    })
}

fn incident_from_row(row: sqlx::postgres::PgRow) -> Result<Incident, SqlStoreError> {
    let severity: String = row.try_get("severity")?;
    let status: String = row.try_get("status")?;
    let impacted_services: Json<Vec<String>> = row.try_get("impacted_services")?;
    Ok(Incident {
        incident_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        title: row.try_get("title")?,
        summary: row.try_get("summary")?,
        severity: parse_incident_severity(&severity)?,
        status: parse_incident_status(&status)?,
        impacted_services: impacted_services.0,
        started_at: row.try_get("started_at")?,
        resolved_at: row.try_get("resolved_at")?,
    })
}

fn audit_event_from_row(row: sqlx::postgres::PgRow) -> Result<AuditEvent, SqlStoreError> {
    Ok(AuditEvent {
        event_id: row.try_get("id")?,
        actor_id: row.try_get("actor_id")?,
        action: row.try_get("action")?,
        resource_id: row.try_get("resource_id")?,
        occurred_at: row.try_get("occurred_at")?,
    })
}

fn usage_event_from_row(row: sqlx::postgres::PgRow) -> Result<UsageEvent, SqlStoreError> {
    let quantity: i64 = row.try_get("quantity")?;
    let signature_key_id: Option<String> = row.try_get("signature_key_id")?;
    let signature_algorithm: Option<String> = row.try_get("signature_algorithm")?;
    let signature: Option<String> = row.try_get("signature")?;
    Ok(UsageEvent {
        event_id: row.try_get("id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        metric: row.try_get("metric")?,
        quantity: u64::try_from(quantity)
            .map_err(|_| SqlStoreError::InvalidValue(format!("invalid quantity {quantity}")))?,
        occurred_at: row.try_get("occurred_at")?,
        signature: match (signature_key_id, signature_algorithm, signature) {
            (Some(key_id), Some(algorithm), Some(signature)) => {
                Some(palimpsest_paas_core::UsageEventSignature {
                    key_id,
                    algorithm,
                    signature,
                })
            }
            (None, None, None) => None,
            _ => {
                return Err(SqlStoreError::InvalidValue(
                    "usage event signature columns are partially populated".to_owned(),
                ));
            }
        },
    })
}

fn quota_policy_from_row(row: sqlx::postgres::PgRow) -> Result<QuotaPolicy, SqlStoreError> {
    let limit_quantity: i64 = row.try_get("limit_quantity")?;
    let window_seconds: i64 = row.try_get("window_seconds")?;
    let enforcement: String = row.try_get("enforcement")?;
    Ok(QuotaPolicy {
        policy_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        metric: row.try_get("metric")?,
        limit_quantity: u64::try_from(limit_quantity).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid quota limit {limit_quantity}"))
        })?,
        window_seconds: u64::try_from(window_seconds).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid quota window {window_seconds}"))
        })?,
        enforcement: parse_quota_enforcement(&enforcement)?,
    })
}

fn quota_alert_from_row(row: sqlx::postgres::PgRow) -> Result<QuotaAlert, SqlStoreError> {
    let threshold_basis_points: i32 = row.try_get("threshold_basis_points")?;
    let current_quantity: i64 = row.try_get("current_quantity")?;
    let limit_quantity: i64 = row.try_get("limit_quantity")?;
    let window_seconds: i64 = row.try_get("window_seconds")?;
    let state: String = row.try_get("state")?;
    Ok(QuotaAlert {
        alert_id: row.try_get("id")?,
        policy_id: row.try_get("policy_id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        metric: row.try_get("metric")?,
        threshold_basis_points: u32::try_from(threshold_basis_points).map_err(|_| {
            SqlStoreError::InvalidValue(format!(
                "invalid quota alert threshold {threshold_basis_points}"
            ))
        })?,
        current_quantity: u64::try_from(current_quantity).map_err(|_| {
            SqlStoreError::InvalidValue(format!(
                "invalid quota alert current quantity {current_quantity}"
            ))
        })?,
        limit_quantity: u64::try_from(limit_quantity).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid quota alert limit {limit_quantity}"))
        })?,
        window_seconds: u64::try_from(window_seconds).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid quota alert window {window_seconds}"))
        })?,
        state: parse_quota_alert_state(&state)?,
        last_evaluated_at: row.try_get("last_evaluated_at")?,
        fired_at: row.try_get("fired_at")?,
        resolved_at: row.try_get("resolved_at")?,
    })
}

fn query_permission_policy_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<QueryPermissionPolicy, SqlStoreError> {
    let operation: String = row.try_get("operation")?;
    let status: String = row.try_get("status")?;
    let sample_context: Json<serde_json::Value> = row.try_get("sample_context")?;
    Ok(QueryPermissionPolicy {
        policy_id: row.try_get("id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        name: row.try_get("name")?,
        table_schema: row.try_get("table_schema")?,
        table_name: row.try_get("table_name")?,
        operation: parse_query_permission_operation(&operation)?,
        principal_claim: row.try_get("principal_claim")?,
        predicate_sql: row.try_get("predicate_sql")?,
        sample_context: sample_context.0,
        status: parse_query_permission_policy_status(&status)?,
    })
}

fn permission_rule_document_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<PermissionRuleDocument, SqlStoreError> {
    Ok(PermissionRuleDocument {
        environment_id: row.try_get("environment_id")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        dsl: row.try_get("dsl")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn billing_export_from_row(row: sqlx::postgres::PgRow) -> Result<BillingExport, SqlStoreError> {
    let event_count: i64 = row.try_get("event_count")?;
    let quantity_total: i64 = row.try_get("quantity_total")?;
    let status: String = row.try_get("status")?;
    let events: Json<Vec<UsageEvent>> = row.try_get("payload")?;
    Ok(BillingExport {
        export_id: row.try_get("id")?,
        destination: row.try_get("destination")?,
        delivery_ref: row.try_get("delivery_ref")?,
        organization_id: row.try_get("organization_id")?,
        project_id: row.try_get("project_id")?,
        environment_id: row.try_get("environment_id")?,
        metric: row.try_get("metric")?,
        occurred_at_from: row.try_get("occurred_at_from")?,
        occurred_at_to: row.try_get("occurred_at_to")?,
        event_count: u64::try_from(event_count).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid billing event count {event_count}"))
        })?,
        quantity_total: u64::try_from(quantity_total).map_err(|_| {
            SqlStoreError::InvalidValue(format!("invalid billing quantity total {quantity_total}"))
        })?,
        status: parse_billing_export_status(&status)?,
        error_message: row.try_get("error_message")?,
        events: events.0,
    })
}

fn quota_enforcement_label(enforcement: QuotaEnforcement) -> &'static str {
    match enforcement {
        QuotaEnforcement::Reject => "reject",
        QuotaEnforcement::Observe => "observe",
    }
}

fn parse_quota_enforcement(value: &str) -> Result<QuotaEnforcement, SqlStoreError> {
    match value {
        "reject" => Ok(QuotaEnforcement::Reject),
        "observe" => Ok(QuotaEnforcement::Observe),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid quota enforcement '{other}'"
        ))),
    }
}

fn quota_alert_state_label(state: QuotaAlertState) -> &'static str {
    match state {
        QuotaAlertState::Ok => "ok",
        QuotaAlertState::Firing => "firing",
    }
}

fn parse_quota_alert_state(value: &str) -> Result<QuotaAlertState, SqlStoreError> {
    match value {
        "ok" => Ok(QuotaAlertState::Ok),
        "firing" => Ok(QuotaAlertState::Firing),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid quota alert state '{other}'"
        ))),
    }
}

fn quota_alert_should_fire(
    current_quantity: i64,
    limit_quantity: i64,
    threshold_basis_points: i32,
) -> bool {
    if limit_quantity <= 0 {
        return current_quantity > 0;
    }
    let current = i128::from(current_quantity.max(0));
    let threshold = i128::from(threshold_basis_points.max(0));
    let limit = i128::from(limit_quantity);
    current * 10_000 >= limit * threshold
}

fn query_permission_operation_label(operation: QueryPermissionOperation) -> &'static str {
    match operation {
        QueryPermissionOperation::Read => "read",
        QueryPermissionOperation::Subscribe => "subscribe",
    }
}

fn parse_query_permission_operation(
    value: &str,
) -> Result<QueryPermissionOperation, SqlStoreError> {
    match value {
        "read" => Ok(QueryPermissionOperation::Read),
        "subscribe" => Ok(QueryPermissionOperation::Subscribe),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid query permission operation '{other}'"
        ))),
    }
}

fn query_permission_policy_status_label(status: QueryPermissionPolicyStatus) -> &'static str {
    match status {
        QueryPermissionPolicyStatus::Draft => "draft",
        QueryPermissionPolicyStatus::Active => "active",
    }
}

fn parse_query_permission_policy_status(
    value: &str,
) -> Result<QueryPermissionPolicyStatus, SqlStoreError> {
    match value {
        "draft" => Ok(QueryPermissionPolicyStatus::Draft),
        "active" => Ok(QueryPermissionPolicyStatus::Active),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid query permission policy status '{other}'"
        ))),
    }
}

fn billing_export_status_label(status: BillingExportStatus) -> &'static str {
    match status {
        BillingExportStatus::Succeeded => "succeeded",
        BillingExportStatus::Failed => "failed",
    }
}

fn parse_billing_export_status(value: &str) -> Result<BillingExportStatus, SqlStoreError> {
    match value {
        "succeeded" => Ok(BillingExportStatus::Succeeded),
        "failed" => Ok(BillingExportStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid billing export status '{other}'"
        ))),
    }
}

pub fn is_unique_violation(err: &SqlStoreError) -> bool {
    matches!(
        err,
        SqlStoreError::Sqlx(sqlx::Error::Database(db_err))
            if db_err.code().as_deref() == Some("23505")
    )
}

pub fn is_foreign_key_violation(err: &SqlStoreError) -> bool {
    matches!(
        err,
        SqlStoreError::Sqlx(sqlx::Error::Database(db_err))
            if db_err.code().as_deref() == Some("23503")
    )
}

fn checked_i32(value: u32, field: &str) -> Result<i32, SqlStoreError> {
    i32::try_from(value)
        .map_err(|_| SqlStoreError::InvalidValue(format!("{field} exceeds integer")))
}

fn normalize_gateway_host(host: &str) -> Result<String, SqlStoreError> {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(SqlStoreError::InvalidValue(
            "gateway route host is empty".to_owned(),
        ));
    }
    Ok(normalized)
}

fn normalize_endpoint_addr(addr: &str, field: &str) -> Result<String, SqlStoreError> {
    let normalized = addr.trim().to_owned();
    if normalized.is_empty() {
        return Err(SqlStoreError::InvalidValue(format!("{field} is empty")));
    }
    Ok(normalized)
}

fn normalize_non_empty(field: &str, value: &str) -> Result<String, SqlStoreError> {
    let normalized = value.trim().to_owned();
    if normalized.is_empty() {
        return Err(SqlStoreError::InvalidValue(format!("{field} is empty")));
    }
    Ok(normalized)
}

fn backup_artifact_id_component(value: &str) -> String {
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

fn normalize_certificate_common_name(value: &str) -> Result<String, SqlStoreError> {
    let normalized = value.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(SqlStoreError::InvalidValue(
            "certificate common_name is empty".to_owned(),
        ));
    }
    Ok(normalized)
}

fn parse_unix_nanos(field: &str, value: &str) -> Result<u128, SqlStoreError> {
    value
        .parse::<u128>()
        .map_err(|_| SqlStoreError::InvalidValue(format!("{field} is not a unix-nanos value")))
}

fn issue_local_dev_x509_certificate(common_name: &str) -> Result<(String, String), SqlStoreError> {
    let CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec![common_name.to_owned()]).map_err(|err| {
            SqlStoreError::InvalidValue(format!("issue local-dev endpoint certificate: {err}"))
        })?;
    Ok((cert.pem(), key_pair.serialize_pem()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcmePendingCertificate {
    private_key_pem: String,
    csr_pem: String,
    challenge_token: String,
    key_authorization: String,
}

fn create_acme_pending_certificate(
    common_name: &str,
    ca_provider_id: &str,
    issuer_ref: &str,
) -> Result<AcmePendingCertificate, SqlStoreError> {
    let key_pair = KeyPair::generate()
        .map_err(|err| SqlStoreError::InvalidValue(format!("generate ACME key pair: {err}")))?;
    let params = CertificateParams::new(vec![common_name.to_owned()])
        .map_err(|err| SqlStoreError::InvalidValue(format!("build ACME CSR params: {err}")))?;
    let csr_pem = params
        .serialize_request(&key_pair)
        .and_then(|csr| csr.pem())
        .map_err(|err| SqlStoreError::InvalidValue(format!("render ACME CSR: {err}")))?;
    let challenge_token = format!("plmp-acme-{}", unix_nanos());
    let account_thumbprint = acme_account_thumbprint(ca_provider_id, issuer_ref);
    let key_authorization = format!("{challenge_token}.{account_thumbprint}");
    Ok(AcmePendingCertificate {
        private_key_pem: key_pair.serialize_pem(),
        csr_pem,
        challenge_token,
        key_authorization,
    })
}

fn acme_account_thumbprint(ca_provider_id: &str, issuer_ref: &str) -> String {
    sha256_base64(&format!("{ca_provider_id}:{issuer_ref}"))
        .replace('+', "-")
        .replace('/', "_")
        .trim_end_matches('=')
        .to_owned()
}

fn validate_pem_block(field: &str, value: &str, marker: &str) -> Result<(), SqlStoreError> {
    if value.trim().is_empty() {
        return Err(SqlStoreError::InvalidValue(format!(
            "{field} cannot be empty"
        )));
    }
    if marker == "PRIVATE KEY" {
        if value.contains("-----BEGIN ")
            && value.contains("PRIVATE KEY-----")
            && value.contains("-----END ")
        {
            return Ok(());
        }
        return Err(SqlStoreError::InvalidValue(format!(
            "{field} must contain a PEM private key block"
        )));
    }
    let begin = format!("-----BEGIN {marker}-----");
    let end = format!("-----END {marker}-----");
    if !value.contains(&begin) || !value.contains(&end) {
        return Err(SqlStoreError::InvalidValue(format!(
            "{field} must contain a PEM {marker} block"
        )));
    }
    Ok(())
}

fn validate_certificate_key_pair(
    certificate_pem: &str,
    private_key_pem: &str,
) -> Result<(), SqlStoreError> {
    let mut cert_reader = BufReader::new(Cursor::new(certificate_pem.as_bytes()));
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| SqlStoreError::InvalidValue(format!("parse certificate PEM: {err}")))?;
    if certs.is_empty() {
        return Err(SqlStoreError::InvalidValue(
            "certificate_pem contained no certificates".to_owned(),
        ));
    }

    let mut key_reader = BufReader::new(Cursor::new(private_key_pem.as_bytes()));
    let private_key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|err| SqlStoreError::InvalidValue(format!("parse private key PEM: {err}")))?
        .ok_or_else(|| {
            SqlStoreError::InvalidValue(
                "private_key_secret_ref material contained no private key".to_owned(),
            )
        })?;

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .map_err(|err| {
            SqlStoreError::InvalidValue(format!(
                "certificate_pem does not match private_key_secret_ref material: {err}"
            ))
        })?;
    Ok(())
}

fn sha256_base64(value: &str) -> String {
    BASE64.encode(digest::digest(&digest::SHA256, value.as_bytes()).as_ref())
}

fn checked_i64(value: u64, field: &str) -> Result<i64, SqlStoreError> {
    i64::try_from(value).map_err(|_| SqlStoreError::InvalidValue(format!("{field} exceeds bigint")))
}

fn checked_u32(value: i32, field: &str) -> Result<u32, SqlStoreError> {
    u32::try_from(value).map_err(|_| SqlStoreError::InvalidValue(format!("{field} is negative")))
}

fn checked_u64(value: i64, field: &str) -> Result<u64, SqlStoreError> {
    u64::try_from(value).map_err(|_| SqlStoreError::InvalidValue(format!("{field} is negative")))
}

fn generated_local_secret(cluster_id: &str, role: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!(
        "plmp_{}_{}_{}",
        sanitize_identifier_component(cluster_id),
        sanitize_identifier_component(role),
        nanos
    )
}

fn database_role_canonical_external_ref(cluster_id: &str, role: &str) -> String {
    format!("managed-postgres/{cluster_id}/{role}/password")
}

fn database_role_pending_external_ref(cluster_id: &str, rotation_id: &str, role: &str) -> String {
    format!(
        "{}{role}/password",
        database_role_pending_secret_prefix(cluster_id, rotation_id)
    )
}

fn database_role_pending_secret_prefix(cluster_id: &str, rotation_id: &str) -> String {
    format!("managed-postgres/{cluster_id}/credential-rotations/{rotation_id}/")
}

fn database_role_pending_secret_id(
    provider: &str,
    cluster_id: &str,
    rotation_id: &str,
    role: &str,
) -> String {
    format!("{provider}:{cluster_id}:role-rotation:{rotation_id}:{role}:password")
}

fn database_role_rotation_id_from_command_id<'a>(
    cluster_id: &str,
    command_id: &'a str,
) -> Option<&'a str> {
    command_id
        .strip_prefix(&format!("{cluster_id}:rotate-database-role-credentials:"))
        .filter(|rotation_id| !rotation_id.trim().is_empty())
}

fn managed_postgres_database_proxy_policy(cluster_id: &str) -> Option<DatabaseProxyPolicy> {
    let prefix = sanitize_identifier_component(cluster_id);
    Some(DatabaseProxyPolicy {
        allowed_users: ["app", "migration", "support"]
            .iter()
            .map(|suffix| format!("{prefix}_{suffix}"))
            .collect(),
        allowed_databases: vec!["postgres".to_owned()],
        forbidden_startup_parameters: vec!["replication".to_owned(), "options".to_owned()],
        required_startup_parameters: BTreeMap::new(),
        forbidden_simple_query_verbs: Vec::new(),
        max_simple_query_bytes: Some(1_048_576),
    })
}

fn sanitize_identifier_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
    {
        sanitized
    } else {
        format!("c_{sanitized}")
    }
}

fn operation_kind_label(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::CreateCluster => "create_cluster",
        OperationKind::StartCluster => "start_cluster",
        OperationKind::StopCluster => "stop_cluster",
        OperationKind::ResizeCluster => "resize_cluster",
        OperationKind::UpdateCluster => "update_cluster",
        OperationKind::RotateCredentials => "rotate_credentials",
        OperationKind::BackupCluster => "backup_cluster",
        OperationKind::DeleteBackup => "delete_backup",
        OperationKind::ArchiveWalSegment => "archive_wal_segment",
        OperationKind::RestoreCluster => "restore_cluster",
        OperationKind::CreateDatabaseClone => "create_database_clone",
        OperationKind::PrepareStandby => "prepare_standby",
        OperationKind::CheckStandby => "check_standby",
        OperationKind::FencePrimary => "fence_primary",
        OperationKind::FailoverCluster => "failover_cluster",
        OperationKind::DeleteCluster => "delete_cluster",
        OperationKind::StartSyncDeployment => "start_sync_deployment",
        OperationKind::StopSyncDeployment => "stop_sync_deployment",
    }
}

fn operation_status_label(status: OperationStatus) -> &'static str {
    match status {
        OperationStatus::Pending => "pending",
        OperationStatus::Running => "running",
        OperationStatus::Succeeded => "succeeded",
        OperationStatus::Failed => "failed",
        OperationStatus::Cancelled => "cancelled",
    }
}

fn parse_operation_kind(value: &str) -> Result<OperationKind, SqlStoreError> {
    match value {
        "create_cluster" => Ok(OperationKind::CreateCluster),
        "start_cluster" => Ok(OperationKind::StartCluster),
        "stop_cluster" => Ok(OperationKind::StopCluster),
        "resize_cluster" => Ok(OperationKind::ResizeCluster),
        "update_cluster" => Ok(OperationKind::UpdateCluster),
        "rotate_credentials" => Ok(OperationKind::RotateCredentials),
        "backup_cluster" => Ok(OperationKind::BackupCluster),
        "delete_backup" => Ok(OperationKind::DeleteBackup),
        "archive_wal_segment" => Ok(OperationKind::ArchiveWalSegment),
        "restore_cluster" => Ok(OperationKind::RestoreCluster),
        "create_database_clone" => Ok(OperationKind::CreateDatabaseClone),
        "prepare_standby" => Ok(OperationKind::PrepareStandby),
        "check_standby" => Ok(OperationKind::CheckStandby),
        "fence_primary" => Ok(OperationKind::FencePrimary),
        "failover_cluster" => Ok(OperationKind::FailoverCluster),
        "delete_cluster" => Ok(OperationKind::DeleteCluster),
        "start_sync_deployment" => Ok(OperationKind::StartSyncDeployment),
        "stop_sync_deployment" => Ok(OperationKind::StopSyncDeployment),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid operation kind '{other}'"
        ))),
    }
}

fn parse_operation_status(value: &str) -> Result<OperationStatus, SqlStoreError> {
    match value {
        "pending" => Ok(OperationStatus::Pending),
        "running" => Ok(OperationStatus::Running),
        "succeeded" => Ok(OperationStatus::Succeeded),
        "failed" => Ok(OperationStatus::Failed),
        "cancelled" => Ok(OperationStatus::Cancelled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid operation status '{other}'"
        ))),
    }
}

fn team_role_label(role: TeamRole) -> &'static str {
    match role {
        TeamRole::Owner => "owner",
        TeamRole::Admin => "admin",
        TeamRole::Developer => "developer",
        TeamRole::Viewer => "viewer",
        TeamRole::Ci => "ci",
    }
}

fn parse_team_role(value: &str) -> Result<TeamRole, SqlStoreError> {
    match value {
        "owner" => Ok(TeamRole::Owner),
        "admin" => Ok(TeamRole::Admin),
        "developer" => Ok(TeamRole::Developer),
        "viewer" => Ok(TeamRole::Viewer),
        "ci" => Ok(TeamRole::Ci),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid team role '{other}'"
        ))),
    }
}

fn jwt_issuer_status_label(status: JwtIssuerStatus) -> &'static str {
    match status {
        JwtIssuerStatus::Active => "active",
        JwtIssuerStatus::Disabled => "disabled",
    }
}

fn parse_jwt_issuer_status(value: &str) -> Result<JwtIssuerStatus, SqlStoreError> {
    match value {
        "active" => Ok(JwtIssuerStatus::Active),
        "disabled" => Ok(JwtIssuerStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid JWT issuer status '{other}'"
        ))),
    }
}

fn webhook_endpoint_status_label(status: WebhookEndpointStatus) -> &'static str {
    match status {
        WebhookEndpointStatus::Active => "active",
        WebhookEndpointStatus::Disabled => "disabled",
    }
}

fn parse_webhook_endpoint_status(value: &str) -> Result<WebhookEndpointStatus, SqlStoreError> {
    match value {
        "active" => Ok(WebhookEndpointStatus::Active),
        "disabled" => Ok(WebhookEndpointStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid webhook endpoint status '{other}'"
        ))),
    }
}

fn sso_provider_kind_label(kind: SsoProviderKind) -> &'static str {
    match kind {
        SsoProviderKind::Saml => "saml",
        SsoProviderKind::Oidc => "oidc",
    }
}

fn parse_sso_provider_kind(value: &str) -> Result<SsoProviderKind, SqlStoreError> {
    match value {
        "saml" => Ok(SsoProviderKind::Saml),
        "oidc" => Ok(SsoProviderKind::Oidc),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid SSO provider kind '{other}'"
        ))),
    }
}

fn sso_provider_status_label(status: SsoProviderStatus) -> &'static str {
    match status {
        SsoProviderStatus::Active => "active",
        SsoProviderStatus::Disabled => "disabled",
    }
}

fn parse_sso_provider_status(value: &str) -> Result<SsoProviderStatus, SqlStoreError> {
    match value {
        "active" => Ok(SsoProviderStatus::Active),
        "disabled" => Ok(SsoProviderStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid SSO provider status '{other}'"
        ))),
    }
}

fn incident_severity_label(severity: IncidentSeverity) -> &'static str {
    match severity {
        IncidentSeverity::Info => "info",
        IncidentSeverity::Warning => "warning",
        IncidentSeverity::Critical => "critical",
    }
}

fn parse_incident_severity(value: &str) -> Result<IncidentSeverity, SqlStoreError> {
    match value {
        "info" => Ok(IncidentSeverity::Info),
        "warning" => Ok(IncidentSeverity::Warning),
        "critical" => Ok(IncidentSeverity::Critical),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid incident severity '{other}'"
        ))),
    }
}

fn incident_status_label(status: IncidentStatus) -> &'static str {
    match status {
        IncidentStatus::Investigating => "investigating",
        IncidentStatus::Identified => "identified",
        IncidentStatus::Monitoring => "monitoring",
        IncidentStatus::Resolved => "resolved",
    }
}

fn parse_incident_status(value: &str) -> Result<IncidentStatus, SqlStoreError> {
    match value {
        "investigating" => Ok(IncidentStatus::Investigating),
        "identified" => Ok(IncidentStatus::Identified),
        "monitoring" => Ok(IncidentStatus::Monitoring),
        "resolved" => Ok(IncidentStatus::Resolved),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid incident status '{other}'"
        ))),
    }
}

fn config_status_label(status: ConfigVersionStatus) -> &'static str {
    match status {
        ConfigVersionStatus::Uploaded => "uploaded",
        ConfigVersionStatus::Validated => "validated",
        ConfigVersionStatus::Deployed => "deployed",
        ConfigVersionStatus::Failed => "failed",
    }
}

fn parse_config_status(value: &str) -> Result<ConfigVersionStatus, SqlStoreError> {
    match value {
        "uploaded" => Ok(ConfigVersionStatus::Uploaded),
        "validated" => Ok(ConfigVersionStatus::Validated),
        "deployed" => Ok(ConfigVersionStatus::Deployed),
        "failed" => Ok(ConfigVersionStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid config status '{other}'"
        ))),
    }
}

fn sync_deployment_lifecycle_state_label(state: SyncDeploymentLifecycleState) -> &'static str {
    match state {
        SyncDeploymentLifecycleState::Requested => "requested",
        SyncDeploymentLifecycleState::Starting => "starting",
        SyncDeploymentLifecycleState::Running => "running",
        SyncDeploymentLifecycleState::Draining => "draining",
        SyncDeploymentLifecycleState::Stopped => "stopped",
        SyncDeploymentLifecycleState::Failed => "failed",
    }
}

fn parse_sync_deployment_lifecycle_state(
    value: &str,
) -> Result<SyncDeploymentLifecycleState, SqlStoreError> {
    match value {
        "requested" => Ok(SyncDeploymentLifecycleState::Requested),
        "starting" => Ok(SyncDeploymentLifecycleState::Starting),
        "running" => Ok(SyncDeploymentLifecycleState::Running),
        "draining" => Ok(SyncDeploymentLifecycleState::Draining),
        "stopped" => Ok(SyncDeploymentLifecycleState::Stopped),
        "failed" => Ok(SyncDeploymentLifecycleState::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid sync deployment lifecycle state '{other}'"
        ))),
    }
}

fn gateway_tls_policy_parts(
    policy: &TlsPolicy,
) -> (
    &'static str,
    Option<&str>,
    Option<&str>,
    Option<&str>,
    Option<&str>,
) {
    match policy {
        TlsPolicy::TerminateAtGateway => ("terminate_at_gateway", None, None, None, None),
        TlsPolicy::MutualTlsToSync {
            ca_secret_ref,
            client_certificate_secret_ref,
            client_private_key_secret_ref,
            server_name,
        } => (
            "mutual_tls_to_sync",
            Some(ca_secret_ref.as_str()),
            client_certificate_secret_ref.as_deref(),
            client_private_key_secret_ref.as_deref(),
            server_name.as_deref(),
        ),
    }
}

fn parse_gateway_tls_policy(
    value: &str,
    ca_secret_ref: Option<String>,
    client_certificate_secret_ref: Option<String>,
    client_private_key_secret_ref: Option<String>,
    server_name: Option<String>,
) -> Result<TlsPolicy, SqlStoreError> {
    match value {
        "terminate_at_gateway" => Ok(TlsPolicy::TerminateAtGateway),
        "mutual_tls_to_sync" => {
            let ca_secret_ref = ca_secret_ref.ok_or_else(|| {
                SqlStoreError::InvalidValue(
                    "mutual_tls_to_sync is missing ca_secret_ref".to_owned(),
                )
            })?;
            Ok(TlsPolicy::MutualTlsToSync {
                ca_secret_ref,
                client_certificate_secret_ref,
                client_private_key_secret_ref,
                server_name,
            })
        }
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid gateway TLS policy '{other}'"
        ))),
    }
}

fn domain_verification_status_label(status: DomainVerificationStatus) -> &'static str {
    match status {
        DomainVerificationStatus::Pending => "pending",
        DomainVerificationStatus::Verified => "verified",
        DomainVerificationStatus::Failed => "failed",
    }
}

fn parse_domain_verification_status(
    value: &str,
) -> Result<DomainVerificationStatus, SqlStoreError> {
    match value {
        "pending" => Ok(DomainVerificationStatus::Pending),
        "verified" => Ok(DomainVerificationStatus::Verified),
        "failed" => Ok(DomainVerificationStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid domain verification status '{other}'"
        ))),
    }
}

fn domain_tls_status_label(status: DomainTlsStatus) -> &'static str {
    match status {
        DomainTlsStatus::Pending => "pending",
        DomainTlsStatus::Active => "active",
        DomainTlsStatus::Failed => "failed",
    }
}

fn parse_domain_tls_status(value: &str) -> Result<DomainTlsStatus, SqlStoreError> {
    match value {
        "pending" => Ok(DomainTlsStatus::Pending),
        "active" => Ok(DomainTlsStatus::Active),
        "failed" => Ok(DomainTlsStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid domain TLS status '{other}'"
        ))),
    }
}

fn ip_allowlist_purpose_label(purpose: IpAllowlistPurpose) -> &'static str {
    match purpose {
        IpAllowlistPurpose::App => "app",
        IpAllowlistPurpose::Migration => "migration",
        IpAllowlistPurpose::Support => "support",
    }
}

fn parse_ip_allowlist_purpose(value: &str) -> Result<IpAllowlistPurpose, SqlStoreError> {
    match value {
        "app" => Ok(IpAllowlistPurpose::App),
        "migration" => Ok(IpAllowlistPurpose::Migration),
        "support" => Ok(IpAllowlistPurpose::Support),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid IP allowlist purpose '{other}'"
        ))),
    }
}

fn ip_allowlist_status_label(status: IpAllowlistStatus) -> &'static str {
    match status {
        IpAllowlistStatus::Active => "active",
        IpAllowlistStatus::Disabled => "disabled",
    }
}

fn parse_ip_allowlist_status(value: &str) -> Result<IpAllowlistStatus, SqlStoreError> {
    match value {
        "active" => Ok(IpAllowlistStatus::Active),
        "disabled" => Ok(IpAllowlistStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid IP allowlist status '{other}'"
        ))),
    }
}

fn static_egress_ip_status_label(status: StaticEgressIpStatus) -> &'static str {
    match status {
        StaticEgressIpStatus::Provisioning => "provisioning",
        StaticEgressIpStatus::Active => "active",
        StaticEgressIpStatus::Retired => "retired",
    }
}

fn parse_static_egress_ip_status(value: &str) -> Result<StaticEgressIpStatus, SqlStoreError> {
    match value {
        "provisioning" => Ok(StaticEgressIpStatus::Provisioning),
        "active" => Ok(StaticEgressIpStatus::Active),
        "retired" => Ok(StaticEgressIpStatus::Retired),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid static egress IP status '{other}'"
        ))),
    }
}

fn maintenance_day_of_week_label(day: MaintenanceDayOfWeek) -> &'static str {
    match day {
        MaintenanceDayOfWeek::Monday => "monday",
        MaintenanceDayOfWeek::Tuesday => "tuesday",
        MaintenanceDayOfWeek::Wednesday => "wednesday",
        MaintenanceDayOfWeek::Thursday => "thursday",
        MaintenanceDayOfWeek::Friday => "friday",
        MaintenanceDayOfWeek::Saturday => "saturday",
        MaintenanceDayOfWeek::Sunday => "sunday",
    }
}

fn parse_maintenance_day_of_week(value: &str) -> Result<MaintenanceDayOfWeek, SqlStoreError> {
    match value {
        "monday" => Ok(MaintenanceDayOfWeek::Monday),
        "tuesday" => Ok(MaintenanceDayOfWeek::Tuesday),
        "wednesday" => Ok(MaintenanceDayOfWeek::Wednesday),
        "thursday" => Ok(MaintenanceDayOfWeek::Thursday),
        "friday" => Ok(MaintenanceDayOfWeek::Friday),
        "saturday" => Ok(MaintenanceDayOfWeek::Saturday),
        "sunday" => Ok(MaintenanceDayOfWeek::Sunday),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid maintenance day of week '{other}'"
        ))),
    }
}

fn maintenance_window_status_label(status: MaintenanceWindowStatus) -> &'static str {
    match status {
        MaintenanceWindowStatus::Active => "active",
        MaintenanceWindowStatus::Disabled => "disabled",
    }
}

fn parse_maintenance_window_status(value: &str) -> Result<MaintenanceWindowStatus, SqlStoreError> {
    match value {
        "active" => Ok(MaintenanceWindowStatus::Active),
        "disabled" => Ok(MaintenanceWindowStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid maintenance window status '{other}'"
        ))),
    }
}

fn lifecycle_state_label(state: ClusterLifecycleState) -> &'static str {
    match state {
        ClusterLifecycleState::Requested => "requested",
        ClusterLifecycleState::Placing => "placing",
        ClusterLifecycleState::AllocatingStorage => "allocating_storage",
        ClusterLifecycleState::InitializingPostgres => "initializing_postgres",
        ClusterLifecycleState::ConfiguringRoles => "configuring_roles",
        ClusterLifecycleState::ConfiguringReplication => "configuring_replication",
        ClusterLifecycleState::Restoring => "restoring",
        ClusterLifecycleState::Resizing => "resizing",
        ClusterLifecycleState::UpdatingPostgres => "updating_postgres",
        ClusterLifecycleState::Starting => "starting",
        ClusterLifecycleState::Verifying => "verifying",
        ClusterLifecycleState::Ready => "ready",
        ClusterLifecycleState::Stopping => "stopping",
        ClusterLifecycleState::Stopped => "stopped",
        ClusterLifecycleState::Deleting => "deleting",
        ClusterLifecycleState::Deleted => "deleted",
        ClusterLifecycleState::Failed => "failed",
    }
}

fn backup_status_label(status: BackupLifecycleState) -> &'static str {
    match status {
        BackupLifecycleState::Requested => "requested",
        BackupLifecycleState::Running => "running",
        BackupLifecycleState::Succeeded => "succeeded",
        BackupLifecycleState::Failed => "failed",
        BackupLifecycleState::Deleted => "deleted",
    }
}

fn backup_artifact_status_label(status: BackupArtifactStatus) -> &'static str {
    match status {
        BackupArtifactStatus::Pending => "pending",
        BackupArtifactStatus::Available => "available",
        BackupArtifactStatus::Expired => "expired",
        BackupArtifactStatus::Deleted => "deleted",
        BackupArtifactStatus::Failed => "failed",
    }
}

fn restore_status_label(status: RestoreLifecycleState) -> &'static str {
    match status {
        RestoreLifecycleState::Requested => "requested",
        RestoreLifecycleState::Running => "running",
        RestoreLifecycleState::Succeeded => "succeeded",
        RestoreLifecycleState::Failed => "failed",
    }
}

fn wal_archive_segment_status_label(status: WalArchiveSegmentStatus) -> &'static str {
    match status {
        WalArchiveSegmentStatus::Running => "running",
        WalArchiveSegmentStatus::Succeeded => "succeeded",
        WalArchiveSegmentStatus::Failed => "failed",
    }
}

fn pitr_check_status_label(status: PitrCheckStatus) -> &'static str {
    match status {
        PitrCheckStatus::Succeeded => "succeeded",
        PitrCheckStatus::Failed => "failed",
    }
}

fn failover_status_label(status: FailoverLifecycleState) -> &'static str {
    match status {
        FailoverLifecycleState::Running => "running",
        FailoverLifecycleState::Succeeded => "succeeded",
        FailoverLifecycleState::Failed => "failed",
    }
}

fn standby_status_label(status: StandbyLifecycleState) -> &'static str {
    match status {
        StandbyLifecycleState::Running => "running",
        StandbyLifecycleState::Succeeded => "succeeded",
        StandbyLifecycleState::Failed => "failed",
    }
}

fn standby_check_status_label(status: StandbyCheckStatus) -> &'static str {
    match status {
        StandbyCheckStatus::Running => "running",
        StandbyCheckStatus::Succeeded => "succeeded",
        StandbyCheckStatus::Failed => "failed",
    }
}

fn runtime_check_status_label(status: RuntimeCheckStatus) -> &'static str {
    match status {
        RuntimeCheckStatus::Healthy => "healthy",
        RuntimeCheckStatus::Degraded => "degraded",
        RuntimeCheckStatus::Failed => "failed",
    }
}

fn major_upgrade_strategy_label(strategy: ManagedPostgresMajorUpgradeStrategy) -> &'static str {
    match strategy {
        ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy => "logical_replication_copy",
        ManagedPostgresMajorUpgradeStrategy::PgUpgradeCopy => "pg_upgrade_copy",
    }
}

fn major_upgrade_status_label(status: ManagedPostgresMajorUpgradeStatus) -> &'static str {
    match status {
        ManagedPostgresMajorUpgradeStatus::Running => "running",
        ManagedPostgresMajorUpgradeStatus::Succeeded => "succeeded",
        ManagedPostgresMajorUpgradeStatus::Failed => "failed",
        ManagedPostgresMajorUpgradeStatus::Cancelled => "cancelled",
    }
}

fn clone_redaction_policy_status_label(status: CloneRedactionPolicyStatus) -> &'static str {
    match status {
        CloneRedactionPolicyStatus::Active => "active",
        CloneRedactionPolicyStatus::Disabled => "disabled",
    }
}

fn support_access_status_label(status: SupportAccessStatus) -> &'static str {
    match status {
        SupportAccessStatus::Requested => "requested",
        SupportAccessStatus::Active => "active",
        SupportAccessStatus::Revoked => "revoked",
        SupportAccessStatus::Expired => "expired",
    }
}

fn secret_encryption_key_status_label(status: SecretEncryptionKeyStatus) -> &'static str {
    match status {
        SecretEncryptionKeyStatus::Active => "active",
        SecretEncryptionKeyStatus::Retiring => "retiring",
        SecretEncryptionKeyStatus::Retired => "retired",
    }
}

fn secret_rewrap_plan_status_label(status: SecretRewrapPlanStatus) -> &'static str {
    match status {
        SecretRewrapPlanStatus::Planned => "planned",
        SecretRewrapPlanStatus::Running => "running",
        SecretRewrapPlanStatus::Succeeded => "succeeded",
        SecretRewrapPlanStatus::Failed => "failed",
    }
}

fn certificate_status_label(status: CertificateLifecycleState) -> &'static str {
    match status {
        CertificateLifecycleState::Provisioning => "provisioning",
        CertificateLifecycleState::Active => "active",
        CertificateLifecycleState::Rotating => "rotating",
        CertificateLifecycleState::Revoked => "revoked",
        CertificateLifecycleState::Failed => "failed",
    }
}

fn acme_challenge_type_label(challenge_type: AcmeChallengeType) -> &'static str {
    match challenge_type {
        AcmeChallengeType::Http01 => "http_01",
    }
}

fn acme_order_status_label(status: AcmeOrderStatus) -> &'static str {
    match status {
        AcmeOrderStatus::PendingChallenge => "pending_challenge",
        AcmeOrderStatus::ReadyToFinalize => "ready_to_finalize",
        AcmeOrderStatus::Succeeded => "succeeded",
        AcmeOrderStatus::Failed => "failed",
    }
}

fn ca_provider_kind_label(kind: CertificateAuthorityProviderKind) -> &'static str {
    match kind {
        CertificateAuthorityProviderKind::LocalDev => "local_dev",
        CertificateAuthorityProviderKind::Acme => "acme",
        CertificateAuthorityProviderKind::ExternalPki => "external_pki",
    }
}

fn ca_provider_status_label(status: CertificateAuthorityProviderStatus) -> &'static str {
    match status {
        CertificateAuthorityProviderStatus::Active => "active",
        CertificateAuthorityProviderStatus::Disabled => "disabled",
    }
}

fn parse_backup_status(value: &str) -> Result<BackupLifecycleState, SqlStoreError> {
    match value {
        "requested" => Ok(BackupLifecycleState::Requested),
        "running" => Ok(BackupLifecycleState::Running),
        "succeeded" => Ok(BackupLifecycleState::Succeeded),
        "failed" => Ok(BackupLifecycleState::Failed),
        "deleted" => Ok(BackupLifecycleState::Deleted),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid backup status '{other}'"
        ))),
    }
}

fn parse_backup_artifact_status(value: &str) -> Result<BackupArtifactStatus, SqlStoreError> {
    match value {
        "pending" => Ok(BackupArtifactStatus::Pending),
        "available" => Ok(BackupArtifactStatus::Available),
        "expired" => Ok(BackupArtifactStatus::Expired),
        "deleted" => Ok(BackupArtifactStatus::Deleted),
        "failed" => Ok(BackupArtifactStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid backup artifact status '{other}'"
        ))),
    }
}

fn parse_restore_status(value: &str) -> Result<RestoreLifecycleState, SqlStoreError> {
    match value {
        "requested" => Ok(RestoreLifecycleState::Requested),
        "running" => Ok(RestoreLifecycleState::Running),
        "succeeded" => Ok(RestoreLifecycleState::Succeeded),
        "failed" => Ok(RestoreLifecycleState::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid restore status '{other}'"
        ))),
    }
}

fn parse_wal_archive_segment_status(value: &str) -> Result<WalArchiveSegmentStatus, SqlStoreError> {
    match value {
        "running" => Ok(WalArchiveSegmentStatus::Running),
        "succeeded" => Ok(WalArchiveSegmentStatus::Succeeded),
        "failed" => Ok(WalArchiveSegmentStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid WAL archive segment status '{other}'"
        ))),
    }
}

fn parse_pitr_check_status(value: &str) -> Result<PitrCheckStatus, SqlStoreError> {
    match value {
        "succeeded" => Ok(PitrCheckStatus::Succeeded),
        "failed" => Ok(PitrCheckStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid PITR check status '{other}'"
        ))),
    }
}

fn parse_failover_status(value: &str) -> Result<FailoverLifecycleState, SqlStoreError> {
    match value {
        "running" => Ok(FailoverLifecycleState::Running),
        "succeeded" => Ok(FailoverLifecycleState::Succeeded),
        "failed" => Ok(FailoverLifecycleState::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid failover status '{other}'"
        ))),
    }
}

fn parse_standby_status(value: &str) -> Result<StandbyLifecycleState, SqlStoreError> {
    match value {
        "running" => Ok(StandbyLifecycleState::Running),
        "succeeded" => Ok(StandbyLifecycleState::Succeeded),
        "failed" => Ok(StandbyLifecycleState::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid standby status '{other}'"
        ))),
    }
}

fn parse_standby_check_status(value: &str) -> Result<StandbyCheckStatus, SqlStoreError> {
    match value {
        "running" => Ok(StandbyCheckStatus::Running),
        "succeeded" => Ok(StandbyCheckStatus::Succeeded),
        "failed" => Ok(StandbyCheckStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid standby check status '{other}'"
        ))),
    }
}

fn parse_runtime_check_status(value: &str) -> Result<RuntimeCheckStatus, SqlStoreError> {
    match value {
        "healthy" => Ok(RuntimeCheckStatus::Healthy),
        "degraded" => Ok(RuntimeCheckStatus::Degraded),
        "failed" => Ok(RuntimeCheckStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid runtime check status '{other}'"
        ))),
    }
}

fn parse_major_upgrade_strategy(
    value: &str,
) -> Result<ManagedPostgresMajorUpgradeStrategy, SqlStoreError> {
    match value {
        "logical_replication_copy" => {
            Ok(ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy)
        }
        "pg_upgrade_copy" => Ok(ManagedPostgresMajorUpgradeStrategy::PgUpgradeCopy),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid major upgrade strategy '{other}'"
        ))),
    }
}

fn parse_major_upgrade_status(
    value: &str,
) -> Result<ManagedPostgresMajorUpgradeStatus, SqlStoreError> {
    match value {
        "running" => Ok(ManagedPostgresMajorUpgradeStatus::Running),
        "succeeded" => Ok(ManagedPostgresMajorUpgradeStatus::Succeeded),
        "failed" => Ok(ManagedPostgresMajorUpgradeStatus::Failed),
        "cancelled" => Ok(ManagedPostgresMajorUpgradeStatus::Cancelled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid major upgrade status '{other}'"
        ))),
    }
}

fn parse_clone_redaction_policy_status(
    value: &str,
) -> Result<CloneRedactionPolicyStatus, SqlStoreError> {
    match value {
        "active" => Ok(CloneRedactionPolicyStatus::Active),
        "disabled" => Ok(CloneRedactionPolicyStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid clone redaction policy status '{other}'"
        ))),
    }
}

fn parse_support_access_status(value: &str) -> Result<SupportAccessStatus, SqlStoreError> {
    match value {
        "requested" => Ok(SupportAccessStatus::Requested),
        "active" => Ok(SupportAccessStatus::Active),
        "revoked" => Ok(SupportAccessStatus::Revoked),
        "expired" => Ok(SupportAccessStatus::Expired),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid support access status '{other}'"
        ))),
    }
}

fn parse_secret_encryption_key_status(
    value: &str,
) -> Result<SecretEncryptionKeyStatus, SqlStoreError> {
    match value {
        "active" => Ok(SecretEncryptionKeyStatus::Active),
        "retiring" => Ok(SecretEncryptionKeyStatus::Retiring),
        "retired" => Ok(SecretEncryptionKeyStatus::Retired),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid secret encryption key status '{other}'"
        ))),
    }
}

fn parse_secret_rewrap_plan_status(value: &str) -> Result<SecretRewrapPlanStatus, SqlStoreError> {
    match value {
        "planned" => Ok(SecretRewrapPlanStatus::Planned),
        "running" => Ok(SecretRewrapPlanStatus::Running),
        "succeeded" => Ok(SecretRewrapPlanStatus::Succeeded),
        "failed" => Ok(SecretRewrapPlanStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid secret rewrap plan status '{other}'"
        ))),
    }
}

fn parse_certificate_status(value: &str) -> Result<CertificateLifecycleState, SqlStoreError> {
    match value {
        "provisioning" => Ok(CertificateLifecycleState::Provisioning),
        "active" => Ok(CertificateLifecycleState::Active),
        "rotating" => Ok(CertificateLifecycleState::Rotating),
        "revoked" => Ok(CertificateLifecycleState::Revoked),
        "failed" => Ok(CertificateLifecycleState::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid certificate status '{other}'"
        ))),
    }
}

fn parse_acme_challenge_type(value: &str) -> Result<AcmeChallengeType, SqlStoreError> {
    match value {
        "http_01" => Ok(AcmeChallengeType::Http01),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid ACME challenge type '{other}'"
        ))),
    }
}

fn parse_acme_order_status(value: &str) -> Result<AcmeOrderStatus, SqlStoreError> {
    match value {
        "pending_challenge" => Ok(AcmeOrderStatus::PendingChallenge),
        "ready_to_finalize" => Ok(AcmeOrderStatus::ReadyToFinalize),
        "succeeded" => Ok(AcmeOrderStatus::Succeeded),
        "failed" => Ok(AcmeOrderStatus::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid ACME order status '{other}'"
        ))),
    }
}

fn parse_ca_provider_kind(value: &str) -> Result<CertificateAuthorityProviderKind, SqlStoreError> {
    match value {
        "local_dev" => Ok(CertificateAuthorityProviderKind::LocalDev),
        "acme" => Ok(CertificateAuthorityProviderKind::Acme),
        "external_pki" => Ok(CertificateAuthorityProviderKind::ExternalPki),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid certificate authority provider kind '{other}'"
        ))),
    }
}

fn parse_ca_provider_status(
    value: &str,
) -> Result<CertificateAuthorityProviderStatus, SqlStoreError> {
    match value {
        "active" => Ok(CertificateAuthorityProviderStatus::Active),
        "disabled" => Ok(CertificateAuthorityProviderStatus::Disabled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid certificate authority provider status '{other}'"
        ))),
    }
}

fn parse_lifecycle_state(value: &str) -> Result<ClusterLifecycleState, SqlStoreError> {
    match value {
        "requested" => Ok(ClusterLifecycleState::Requested),
        "placing" => Ok(ClusterLifecycleState::Placing),
        "allocating_storage" => Ok(ClusterLifecycleState::AllocatingStorage),
        "initializing_postgres" => Ok(ClusterLifecycleState::InitializingPostgres),
        "configuring_roles" => Ok(ClusterLifecycleState::ConfiguringRoles),
        "configuring_replication" => Ok(ClusterLifecycleState::ConfiguringReplication),
        "restoring" => Ok(ClusterLifecycleState::Restoring),
        "resizing" => Ok(ClusterLifecycleState::Resizing),
        "updating_postgres" => Ok(ClusterLifecycleState::UpdatingPostgres),
        "starting" => Ok(ClusterLifecycleState::Starting),
        "verifying" => Ok(ClusterLifecycleState::Verifying),
        "ready" => Ok(ClusterLifecycleState::Ready),
        "stopping" => Ok(ClusterLifecycleState::Stopping),
        "stopped" => Ok(ClusterLifecycleState::Stopped),
        "deleting" => Ok(ClusterLifecycleState::Deleting),
        "deleted" => Ok(ClusterLifecycleState::Deleted),
        "failed" => Ok(ClusterLifecycleState::Failed),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid cluster lifecycle state '{other}'"
        ))),
    }
}

#[cfg(test)]
fn next_state_after_agent_command(
    current: ClusterLifecycleState,
    action: &NodeAgentAction,
    status: AgentCommandStatus,
) -> Option<ClusterLifecycleState> {
    next_state_after_agent_command_for_operation(current, action, status, None)
}

fn next_state_after_agent_command_for_operation(
    current: ClusterLifecycleState,
    action: &NodeAgentAction,
    status: AgentCommandStatus,
    operation_kind: Option<&str>,
) -> Option<ClusterLifecycleState> {
    if matches!(
        action,
        NodeAgentAction::StartSyncDeployment { .. }
            | NodeAgentAction::StopSyncDeployment { .. }
            | NodeAgentAction::ReportSyncDeployment { .. }
    ) {
        return None;
    }
    if status == AgentCommandStatus::Failed
        && operation_kind == Some("rotate_credentials")
        && current == ClusterLifecycleState::ConfiguringReplication
        && matches!(action, NodeAgentAction::ConfigurePostgresAccess { .. })
    {
        return Some(ClusterLifecycleState::Ready);
    }
    if status == AgentCommandStatus::Failed {
        return Some(ClusterLifecycleState::Failed);
    }
    if status != AgentCommandStatus::Succeeded {
        return None;
    }

    match (current, action) {
        (ClusterLifecycleState::InitializingPostgres, NodeAgentAction::PreparePostgres { .. }) => {
            Some(ClusterLifecycleState::Starting)
        }
        (ClusterLifecycleState::Verifying, NodeAgentAction::StartPostgres) => {
            Some(ClusterLifecycleState::ConfiguringRoles)
        }
        (ClusterLifecycleState::Starting, NodeAgentAction::StartPostgres) => {
            Some(ClusterLifecycleState::Ready)
        }
        (
            ClusterLifecycleState::ConfiguringReplication,
            NodeAgentAction::ConfigurePostgresAccess { .. },
        ) => Some(ClusterLifecycleState::Ready),
        (ClusterLifecycleState::Restoring, NodeAgentAction::PrepareRestore { .. }) => {
            Some(ClusterLifecycleState::Stopped)
        }
        (ClusterLifecycleState::Stopping, NodeAgentAction::StopPostgres) => {
            Some(ClusterLifecycleState::Stopped)
        }
        (ClusterLifecycleState::Resizing, NodeAgentAction::ResizePostgresStorage { .. }) => {
            Some(ClusterLifecycleState::Ready)
        }
        (ClusterLifecycleState::UpdatingPostgres, NodeAgentAction::UpdatePostgresMinor { .. }) => {
            Some(ClusterLifecycleState::Ready)
        }
        (ClusterLifecycleState::UpdatingPostgres, NodeAgentAction::UpgradePostgresMajor { .. }) => {
            Some(ClusterLifecycleState::Ready)
        }
        (ClusterLifecycleState::Deleting, NodeAgentAction::DeletePostgresData { .. }) => {
            Some(ClusterLifecycleState::Deleted)
        }
        _ => None,
    }
}

fn node_host_state_label(state: NodeHostState) -> &'static str {
    match state {
        NodeHostState::Registering => "registering",
        NodeHostState::Active => "active",
        NodeHostState::Draining => "draining",
        NodeHostState::Maintenance => "maintenance",
        NodeHostState::Offline => "offline",
    }
}

fn parse_node_host_state(value: &str) -> Result<NodeHostState, SqlStoreError> {
    match value {
        "registering" => Ok(NodeHostState::Registering),
        "active" => Ok(NodeHostState::Active),
        "draining" => Ok(NodeHostState::Draining),
        "maintenance" => Ok(NodeHostState::Maintenance),
        "offline" => Ok(NodeHostState::Offline),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid node host state '{other}'"
        ))),
    }
}

fn node_host_agent_credential_state_label(state: NodeHostAgentCredentialState) -> &'static str {
    match state {
        NodeHostAgentCredentialState::Active => "active",
        NodeHostAgentCredentialState::Rotated => "rotated",
        NodeHostAgentCredentialState::Revoked => "revoked",
    }
}

fn parse_node_host_agent_credential_state(
    value: &str,
) -> Result<NodeHostAgentCredentialState, SqlStoreError> {
    match value {
        "active" => Ok(NodeHostAgentCredentialState::Active),
        "rotated" => Ok(NodeHostAgentCredentialState::Rotated),
        "revoked" => Ok(NodeHostAgentCredentialState::Revoked),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid node host agent credential state '{other}'"
        ))),
    }
}

fn node_host_hardening_status_label(status: NodeHostHardeningStatus) -> &'static str {
    match status {
        NodeHostHardeningStatus::Passing => "passing",
        NodeHostHardeningStatus::Warning => "warning",
        NodeHostHardeningStatus::Failing => "failing",
    }
}

fn parse_node_host_hardening_status(value: &str) -> Result<NodeHostHardeningStatus, SqlStoreError> {
    match value {
        "passing" => Ok(NodeHostHardeningStatus::Passing),
        "warning" => Ok(NodeHostHardeningStatus::Warning),
        "failing" => Ok(NodeHostHardeningStatus::Failing),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid node host hardening status '{other}'"
        ))),
    }
}

fn agent_command_status_label(status: AgentCommandStatus) -> &'static str {
    match status {
        AgentCommandStatus::Pending => "pending",
        AgentCommandStatus::Running => "running",
        AgentCommandStatus::Succeeded => "succeeded",
        AgentCommandStatus::Failed => "failed",
        AgentCommandStatus::Cancelled => "cancelled",
    }
}

fn parse_agent_command_status(value: &str) -> Result<AgentCommandStatus, SqlStoreError> {
    match value {
        "pending" => Ok(AgentCommandStatus::Pending),
        "running" => Ok(AgentCommandStatus::Running),
        "succeeded" => Ok(AgentCommandStatus::Succeeded),
        "failed" => Ok(AgentCommandStatus::Failed),
        "cancelled" => Ok(AgentCommandStatus::Cancelled),
        other => Err(SqlStoreError::InvalidValue(format!(
            "invalid agent command status '{other}'"
        ))),
    }
}

#[derive(Debug, Error)]
pub enum SqlStoreError {
    #[error("postgres store operation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("resource not found: {0}")]
    MissingResource(String),
    #[error("resource is immutable: {0}")]
    ImmutableResource(String),
    #[error("invalid postgres store value: {0}")]
    InvalidValue(String),
    #[error("invalid paas model stored in postgres: {0}")]
    Model(#[from] palimpsest_paas_core::ModelError),
    #[error("quota exceeded for {environment_id}/{metric}: attempted {attempted}, limit {limit}")]
    QuotaExceeded {
        environment_id: String,
        metric: String,
        limit: u64,
        attempted: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_host_hardening_status_labels_round_trip() {
        for (status, label) in [
            (NodeHostHardeningStatus::Passing, "passing"),
            (NodeHostHardeningStatus::Warning, "warning"),
            (NodeHostHardeningStatus::Failing, "failing"),
        ] {
            assert_eq!(node_host_hardening_status_label(status), label);
            assert_eq!(parse_node_host_hardening_status(label).unwrap(), status);
        }
    }

    #[test]
    fn env_envelope_secret_backend_round_trips_without_plaintext() {
        let key = BASE64.encode([7_u8; 32]);
        let backend =
            EnvEnvelopeSecretBackend::from_base64_key("test-key".to_owned(), &key).unwrap();
        let secret_backend = SecretMaterialBackend::EnvEnvelope(backend);

        let sealed = secret_backend
            .seal("managed-postgres/cluster_123/app/password", "s3cr3t")
            .unwrap();
        assert!(sealed.secret_material.is_none());
        assert_ne!(sealed.encrypted_material.as_deref(), Some("s3cr3t"));
        assert_eq!(sealed.key_ref.as_deref(), Some("test-key"));

        let opened = secret_backend
            .open(
                "managed-postgres/cluster_123/app/password",
                sealed.secret_material,
                sealed.encrypted_material,
                sealed.key_ref,
            )
            .unwrap();
        assert_eq!(opened, "s3cr3t");
    }

    #[test]
    fn env_envelope_secret_backend_binds_ciphertext_to_external_ref() {
        let key = BASE64.encode([9_u8; 32]);
        let backend =
            EnvEnvelopeSecretBackend::from_base64_key("test-key".to_owned(), &key).unwrap();
        let secret_backend = SecretMaterialBackend::EnvEnvelope(backend);

        let sealed = secret_backend
            .seal("managed-postgres/cluster_123/app/password", "s3cr3t")
            .unwrap();
        let err = secret_backend
            .open(
                "managed-postgres/cluster_123/replication/password",
                sealed.secret_material,
                sealed.encrypted_material,
                sealed.key_ref,
            )
            .unwrap_err();

        assert!(err.to_string().contains("open secret"));
    }

    #[test]
    fn file_envelope_secret_backend_loads_keyring_json() {
        let key = BASE64.encode([11_u8; 32]);
        let keyring = serde_json::json!({
            "primary_key_ref": "file-key",
            "keys": {
                "file-key": key
            }
        })
        .to_string();
        let backend = EnvEnvelopeSecretBackend::from_keyring_json(&keyring).unwrap();
        let secret_backend = SecretMaterialBackend::FileEnvelope(backend);

        let sealed = secret_backend
            .seal("managed-postgres/cluster_123/app/password", "s3cr3t")
            .unwrap();
        assert!(sealed.secret_material.is_none());
        assert_ne!(sealed.encrypted_material.as_deref(), Some("s3cr3t"));
        assert_eq!(sealed.key_ref.as_deref(), Some("file-key"));

        let opened = secret_backend
            .open(
                "managed-postgres/cluster_123/app/password",
                sealed.secret_material,
                sealed.encrypted_material,
                sealed.key_ref,
            )
            .unwrap();
        assert_eq!(opened, "s3cr3t");
    }

    #[test]
    fn file_envelope_secret_backend_loads_keyring_file() {
        let key = BASE64.encode([12_u8; 32]);
        let keyring = serde_json::json!({
            "primary_key_ref": "file-key",
            "keys": {
                "file-key": key
            }
        })
        .to_string();
        let path = std::env::temp_dir().join(format!(
            "palimpsest-paas-secret-keyring-{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::write(&path, keyring).expect("keyring file is written");
        let backend =
            EnvEnvelopeSecretBackend::from_keyring_file(path.to_str().expect("utf8 path"))
                .expect("keyring file loads");
        fs::remove_file(path).ok();
        let secret_backend = SecretMaterialBackend::FileEnvelope(backend);

        let sealed = secret_backend
            .seal("managed-postgres/cluster_123/app/password", "s3cr3t")
            .unwrap();
        assert!(sealed.secret_material.is_none());
        assert_eq!(sealed.key_ref.as_deref(), Some("file-key"));
    }

    #[test]
    fn env_envelope_secret_backend_rewraps_between_configured_keys() {
        let source_key = BASE64.encode([10_u8; 32]);
        let target_key = BASE64.encode([11_u8; 32]);
        let backend = EnvEnvelopeSecretBackend::from_base64_keyring(
            "target-key".to_owned(),
            BTreeMap::from([
                ("source-key".to_owned(), source_key),
                ("target-key".to_owned(), target_key),
            ]),
        )
        .unwrap();
        let secret_backend = SecretMaterialBackend::EnvEnvelope(backend);

        let source = secret_backend
            .seal_for_key(
                "managed-postgres/cluster_123/app/password",
                "s3cr3t",
                "source-key",
            )
            .unwrap();
        let opened = secret_backend
            .open(
                "managed-postgres/cluster_123/app/password",
                source.secret_material,
                source.encrypted_material,
                source.key_ref,
            )
            .unwrap();
        let target = secret_backend
            .seal_for_key(
                "managed-postgres/cluster_123/app/password",
                &opened,
                "target-key",
            )
            .unwrap();

        assert_eq!(target.key_ref.as_deref(), Some("target-key"));
        assert_eq!(
            secret_backend
                .open(
                    "managed-postgres/cluster_123/app/password",
                    target.secret_material,
                    target.encrypted_material,
                    target.key_ref,
                )
                .unwrap(),
            "s3cr3t"
        );
    }

    #[test]
    fn health_state_uses_strongest_customer_impact() {
        let components = vec![
            CustomerEnvironmentHealthComponent {
                name: "database".to_owned(),
                state: CustomerEnvironmentHealthState::Healthy,
                detail: "ready".to_owned(),
            },
            CustomerEnvironmentHealthComponent {
                name: "backup".to_owned(),
                state: CustomerEnvironmentHealthState::AtRisk,
                detail: "latest backup failed".to_owned(),
            },
            CustomerEnvironmentHealthComponent {
                name: "sync".to_owned(),
                state: CustomerEnvironmentHealthState::Degraded,
                detail: "starting".to_owned(),
            },
        ];

        assert_eq!(
            strongest_health_state(&components),
            CustomerEnvironmentHealthState::AtRisk
        );
    }

    #[test]
    fn local_jsonl_billing_delivery_writes_usage_events() {
        let dir = std::env::temp_dir().join(format!(
            "palimpsest-billing-delivery-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let event = UsageEvent {
            event_id: "usage_123".to_owned(),
            idempotency_key: "usage_123".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            metric: "sync_egress_bytes".to_owned(),
            quantity: 4096,
            occurred_at: "2026-05-17T00:00:00Z".to_owned(),
            signature: None,
        };

        let delivery = deliver_billing_export(
            "billing/export 123",
            &format!("local-jsonl:{}", dir.display()),
            &[event],
        )
        .expect("delivery succeeds");

        assert!(delivery.delivery_ref.ends_with("billing_export_123.jsonl"));
        let contents = fs::read_to_string(&delivery.delivery_ref).expect("export file is readable");
        assert!(contents.contains("\"event_id\":\"usage_123\""));
        assert!(contents.ends_with('\n'));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cluster_lifecycle_maps_to_customer_health() {
        assert_eq!(
            database_health_for_cluster("cluster_123", ClusterLifecycleState::Ready).0,
            CustomerEnvironmentHealthState::Healthy
        );
        assert_eq!(
            database_health_for_cluster("cluster_123", ClusterLifecycleState::Restoring).0,
            CustomerEnvironmentHealthState::Maintenance
        );
        assert_eq!(
            database_health_for_cluster("cluster_123", ClusterLifecycleState::Resizing).0,
            CustomerEnvironmentHealthState::Maintenance
        );
        assert_eq!(
            database_health_for_cluster("cluster_123", ClusterLifecycleState::Failed).0,
            CustomerEnvironmentHealthState::Unavailable
        );
    }

    #[test]
    fn config_status_labels_match_schema_values() {
        assert_eq!(
            config_status_label(ConfigVersionStatus::Uploaded),
            "uploaded"
        );
        assert_eq!(
            config_status_label(ConfigVersionStatus::Validated),
            "validated"
        );
        assert_eq!(
            config_status_label(ConfigVersionStatus::Deployed),
            "deployed"
        );
        assert_eq!(config_status_label(ConfigVersionStatus::Failed), "failed");
    }

    #[test]
    fn sync_deployment_lifecycle_labels_match_schema_values() {
        assert_eq!(
            sync_deployment_lifecycle_state_label(SyncDeploymentLifecycleState::Requested),
            "requested"
        );
        assert_eq!(
            sync_deployment_lifecycle_state_label(SyncDeploymentLifecycleState::Running),
            "running"
        );
        assert_eq!(
            parse_sync_deployment_lifecycle_state("draining").expect("state parses"),
            SyncDeploymentLifecycleState::Draining
        );
    }

    #[test]
    fn node_host_state_labels_match_schema_values() {
        assert_eq!(
            node_host_state_label(NodeHostState::Registering),
            "registering"
        );
        assert_eq!(node_host_state_label(NodeHostState::Active), "active");
        assert_eq!(node_host_state_label(NodeHostState::Draining), "draining");
        assert_eq!(
            node_host_state_label(NodeHostState::Maintenance),
            "maintenance"
        );
        assert_eq!(node_host_state_label(NodeHostState::Offline), "offline");
        assert_eq!(
            parse_node_host_state("active").expect("state parses"),
            NodeHostState::Active
        );
    }

    #[test]
    fn node_host_agent_credential_state_labels_match_schema_values() {
        assert_eq!(
            node_host_agent_credential_state_label(NodeHostAgentCredentialState::Active),
            "active"
        );
        assert_eq!(
            node_host_agent_credential_state_label(NodeHostAgentCredentialState::Rotated),
            "rotated"
        );
        assert_eq!(
            node_host_agent_credential_state_label(NodeHostAgentCredentialState::Revoked),
            "revoked"
        );
        assert_eq!(
            parse_node_host_agent_credential_state("revoked").expect("state parses"),
            NodeHostAgentCredentialState::Revoked
        );
    }

    #[test]
    fn backup_status_labels_match_schema_values() {
        assert_eq!(
            backup_status_label(BackupLifecycleState::Requested),
            "requested"
        );
        assert_eq!(
            backup_status_label(BackupLifecycleState::Running),
            "running"
        );
        assert_eq!(
            backup_status_label(BackupLifecycleState::Succeeded),
            "succeeded"
        );
        assert_eq!(backup_status_label(BackupLifecycleState::Failed), "failed");
        assert_eq!(
            backup_status_label(BackupLifecycleState::Deleted),
            "deleted"
        );
        assert_eq!(
            parse_backup_status("deleted").expect("status parses"),
            BackupLifecycleState::Deleted
        );
    }

    #[test]
    fn backup_artifact_status_labels_match_schema_values() {
        assert_eq!(
            backup_artifact_status_label(BackupArtifactStatus::Pending),
            "pending"
        );
        assert_eq!(
            backup_artifact_status_label(BackupArtifactStatus::Available),
            "available"
        );
        assert_eq!(
            backup_artifact_status_label(BackupArtifactStatus::Expired),
            "expired"
        );
        assert_eq!(
            backup_artifact_status_label(BackupArtifactStatus::Deleted),
            "deleted"
        );
        assert_eq!(
            backup_artifact_status_label(BackupArtifactStatus::Failed),
            "failed"
        );
        assert_eq!(
            parse_backup_artifact_status("available").expect("status parses"),
            BackupArtifactStatus::Available
        );
    }

    #[test]
    fn standby_check_status_labels_match_schema_values() {
        assert_eq!(
            standby_check_status_label(StandbyCheckStatus::Running),
            "running"
        );
        assert_eq!(
            standby_check_status_label(StandbyCheckStatus::Succeeded),
            "succeeded"
        );
        assert_eq!(
            standby_check_status_label(StandbyCheckStatus::Failed),
            "failed"
        );
        assert_eq!(
            parse_standby_check_status("succeeded").expect("status parses"),
            StandbyCheckStatus::Succeeded
        );
    }

    #[test]
    fn runtime_check_status_labels_match_schema_values() {
        assert_eq!(
            runtime_check_status_label(RuntimeCheckStatus::Healthy),
            "healthy"
        );
        assert_eq!(
            runtime_check_status_label(RuntimeCheckStatus::Degraded),
            "degraded"
        );
        assert_eq!(
            runtime_check_status_label(RuntimeCheckStatus::Failed),
            "failed"
        );
        assert_eq!(
            parse_runtime_check_status("degraded").expect("status parses"),
            RuntimeCheckStatus::Degraded
        );
        assert!(parse_runtime_check_status("unknown").is_err());
    }

    #[test]
    fn major_upgrade_labels_match_schema_values() {
        assert_eq!(
            major_upgrade_strategy_label(
                ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy
            ),
            "logical_replication_copy"
        );
        assert_eq!(
            major_upgrade_strategy_label(ManagedPostgresMajorUpgradeStrategy::PgUpgradeCopy),
            "pg_upgrade_copy"
        );
        assert_eq!(
            parse_major_upgrade_strategy("pg_upgrade_copy").expect("strategy parses"),
            ManagedPostgresMajorUpgradeStrategy::PgUpgradeCopy
        );
        assert_eq!(
            major_upgrade_status_label(ManagedPostgresMajorUpgradeStatus::Running),
            "running"
        );
        assert_eq!(
            major_upgrade_status_label(ManagedPostgresMajorUpgradeStatus::Succeeded),
            "succeeded"
        );
        assert_eq!(
            major_upgrade_status_label(ManagedPostgresMajorUpgradeStatus::Failed),
            "failed"
        );
        assert_eq!(
            major_upgrade_status_label(ManagedPostgresMajorUpgradeStatus::Cancelled),
            "cancelled"
        );
        assert_eq!(
            parse_major_upgrade_status("cancelled").expect("status parses"),
            ManagedPostgresMajorUpgradeStatus::Cancelled
        );
    }

    #[test]
    fn clone_redaction_policy_labels_match_schema_values() {
        assert_eq!(
            clone_redaction_policy_status_label(CloneRedactionPolicyStatus::Active),
            "active"
        );
        assert_eq!(
            clone_redaction_policy_status_label(CloneRedactionPolicyStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            parse_clone_redaction_policy_status("disabled").expect("status parses"),
            CloneRedactionPolicyStatus::Disabled
        );
    }

    #[test]
    fn support_access_status_labels_match_schema_values() {
        assert_eq!(
            support_access_status_label(SupportAccessStatus::Requested),
            "requested"
        );
        assert_eq!(
            support_access_status_label(SupportAccessStatus::Active),
            "active"
        );
        assert_eq!(
            support_access_status_label(SupportAccessStatus::Revoked),
            "revoked"
        );
        assert_eq!(
            support_access_status_label(SupportAccessStatus::Expired),
            "expired"
        );
        assert_eq!(
            parse_support_access_status("active").expect("status parses"),
            SupportAccessStatus::Active
        );
    }

    #[test]
    fn secret_encryption_key_status_labels_match_schema_values() {
        assert_eq!(
            secret_encryption_key_status_label(SecretEncryptionKeyStatus::Active),
            "active"
        );
        assert_eq!(
            secret_encryption_key_status_label(SecretEncryptionKeyStatus::Retiring),
            "retiring"
        );
        assert_eq!(
            secret_encryption_key_status_label(SecretEncryptionKeyStatus::Retired),
            "retired"
        );
        assert_eq!(
            parse_secret_encryption_key_status("retiring").expect("status parses"),
            SecretEncryptionKeyStatus::Retiring
        );
    }

    #[test]
    fn secret_rewrap_plan_status_labels_match_schema_values() {
        assert_eq!(
            secret_rewrap_plan_status_label(SecretRewrapPlanStatus::Planned),
            "planned"
        );
        assert_eq!(
            secret_rewrap_plan_status_label(SecretRewrapPlanStatus::Running),
            "running"
        );
        assert_eq!(
            secret_rewrap_plan_status_label(SecretRewrapPlanStatus::Succeeded),
            "succeeded"
        );
        assert_eq!(
            secret_rewrap_plan_status_label(SecretRewrapPlanStatus::Failed),
            "failed"
        );
        assert_eq!(
            parse_secret_rewrap_plan_status("succeeded").expect("status parses"),
            SecretRewrapPlanStatus::Succeeded
        );
    }

    #[test]
    fn ca_provider_labels_match_schema_values() {
        assert_eq!(
            ca_provider_kind_label(CertificateAuthorityProviderKind::LocalDev),
            "local_dev"
        );
        assert_eq!(
            ca_provider_kind_label(CertificateAuthorityProviderKind::Acme),
            "acme"
        );
        assert_eq!(
            ca_provider_kind_label(CertificateAuthorityProviderKind::ExternalPki),
            "external_pki"
        );
        assert_eq!(
            parse_ca_provider_kind("external_pki").expect("kind parses"),
            CertificateAuthorityProviderKind::ExternalPki
        );
        assert_eq!(
            ca_provider_status_label(CertificateAuthorityProviderStatus::Active),
            "active"
        );
        assert_eq!(
            ca_provider_status_label(CertificateAuthorityProviderStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            parse_ca_provider_status("disabled").expect("status parses"),
            CertificateAuthorityProviderStatus::Disabled
        );
        assert!(parse_ca_provider_kind("unknown").is_err());
        assert!(parse_ca_provider_status("unknown").is_err());
    }

    #[test]
    fn acme_order_status_labels_match_schema_values() {
        assert_eq!(
            acme_challenge_type_label(AcmeChallengeType::Http01),
            "http_01"
        );
        assert_eq!(
            acme_order_status_label(AcmeOrderStatus::PendingChallenge),
            "pending_challenge"
        );
        assert_eq!(
            acme_order_status_label(AcmeOrderStatus::ReadyToFinalize),
            "ready_to_finalize"
        );
        assert_eq!(
            acme_order_status_label(AcmeOrderStatus::Succeeded),
            "succeeded"
        );
        assert_eq!(acme_order_status_label(AcmeOrderStatus::Failed), "failed");
        assert_eq!(
            parse_acme_order_status("ready_to_finalize").expect("status parses"),
            AcmeOrderStatus::ReadyToFinalize
        );
        assert!(parse_acme_challenge_type("dns_01").is_err());
        assert!(parse_acme_order_status("valid").is_err());
    }

    #[test]
    fn agent_command_status_labels_match_queue_values() {
        assert_eq!(
            agent_command_status_label(AgentCommandStatus::Pending),
            "pending"
        );
        assert_eq!(
            agent_command_status_label(AgentCommandStatus::Running),
            "running"
        );
        assert_eq!(
            agent_command_status_label(AgentCommandStatus::Succeeded),
            "succeeded"
        );
        assert_eq!(
            agent_command_status_label(AgentCommandStatus::Failed),
            "failed"
        );
        assert_eq!(
            agent_command_status_label(AgentCommandStatus::Cancelled),
            "cancelled"
        );
        assert_eq!(
            parse_agent_command_status("running").expect("status parses"),
            AgentCommandStatus::Running
        );
    }

    #[test]
    fn usage_event_quantity_must_fit_postgres_bigint() {
        assert_eq!(checked_i64(10, "quantity").expect("quantity fits"), 10);
        assert!(checked_i64(u64::MAX, "quantity").is_err());
    }

    #[test]
    fn usage_event_filter_limit_is_capped_for_export_queries() {
        assert_eq!(
            UsageEventFilter {
                limit: Some(20_000),
                ..UsageEventFilter::default()
            }
            .limit
            .unwrap()
            .min(10_000),
            10_000
        );
    }

    #[test]
    fn quota_alert_state_labels_match_schema_values() {
        assert_eq!(quota_alert_state_label(QuotaAlertState::Ok), "ok");
        assert_eq!(quota_alert_state_label(QuotaAlertState::Firing), "firing");
        assert_eq!(
            parse_quota_alert_state("firing").expect("state parses"),
            QuotaAlertState::Firing
        );
        assert!(parse_quota_alert_state("unknown").is_err());
    }

    #[test]
    fn quota_alert_threshold_uses_basis_points_without_float_rounding() {
        assert!(quota_alert_should_fire(8_000, 10_000, 8_000));
        assert!(!quota_alert_should_fire(7_999, 10_000, 8_000));
        assert!(quota_alert_should_fire(1, 0, 8_000));
        assert!(!quota_alert_should_fire(0, 0, 8_000));
    }

    #[test]
    fn query_permission_policy_labels_match_schema_values() {
        assert_eq!(
            query_permission_operation_label(QueryPermissionOperation::Read),
            "read"
        );
        assert_eq!(
            query_permission_operation_label(QueryPermissionOperation::Subscribe),
            "subscribe"
        );
        assert_eq!(
            query_permission_policy_status_label(QueryPermissionPolicyStatus::Draft),
            "draft"
        );
        assert_eq!(
            query_permission_policy_status_label(QueryPermissionPolicyStatus::Active),
            "active"
        );
        assert_eq!(
            parse_query_permission_operation("subscribe").expect("operation parses"),
            QueryPermissionOperation::Subscribe
        );
        assert_eq!(
            parse_query_permission_policy_status("active").expect("status parses"),
            QueryPermissionPolicyStatus::Active
        );
    }

    #[test]
    fn jwt_issuer_status_labels_match_schema_values() {
        assert_eq!(jwt_issuer_status_label(JwtIssuerStatus::Active), "active");
        assert_eq!(
            jwt_issuer_status_label(JwtIssuerStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            parse_jwt_issuer_status("active").expect("status parses"),
            JwtIssuerStatus::Active
        );
        assert!(parse_jwt_issuer_status("unknown").is_err());
    }

    #[test]
    fn webhook_endpoint_status_labels_match_schema_values() {
        assert_eq!(
            webhook_endpoint_status_label(WebhookEndpointStatus::Active),
            "active"
        );
        assert_eq!(
            webhook_endpoint_status_label(WebhookEndpointStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            parse_webhook_endpoint_status("active").expect("status parses"),
            WebhookEndpointStatus::Active
        );
        assert!(parse_webhook_endpoint_status("unknown").is_err());
    }

    #[test]
    fn sso_provider_labels_match_schema_values() {
        assert_eq!(sso_provider_kind_label(SsoProviderKind::Saml), "saml");
        assert_eq!(sso_provider_kind_label(SsoProviderKind::Oidc), "oidc");
        assert_eq!(
            sso_provider_status_label(SsoProviderStatus::Active),
            "active"
        );
        assert_eq!(
            sso_provider_status_label(SsoProviderStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            parse_sso_provider_kind("oidc").expect("kind parses"),
            SsoProviderKind::Oidc
        );
        assert_eq!(
            parse_sso_provider_status("active").expect("status parses"),
            SsoProviderStatus::Active
        );
        assert!(parse_sso_provider_kind("ldap").is_err());
        assert!(parse_sso_provider_status("unknown").is_err());
    }

    #[test]
    fn incident_labels_match_schema_values() {
        assert_eq!(incident_severity_label(IncidentSeverity::Info), "info");
        assert_eq!(
            incident_severity_label(IncidentSeverity::Warning),
            "warning"
        );
        assert_eq!(
            incident_severity_label(IncidentSeverity::Critical),
            "critical"
        );
        assert_eq!(
            incident_status_label(IncidentStatus::Investigating),
            "investigating"
        );
        assert_eq!(
            incident_status_label(IncidentStatus::Identified),
            "identified"
        );
        assert_eq!(
            incident_status_label(IncidentStatus::Monitoring),
            "monitoring"
        );
        assert_eq!(incident_status_label(IncidentStatus::Resolved), "resolved");
        assert_eq!(
            parse_incident_severity("critical").expect("severity parses"),
            IncidentSeverity::Critical
        );
        assert_eq!(
            parse_incident_status("monitoring").expect("status parses"),
            IncidentStatus::Monitoring
        );
        assert!(parse_incident_severity("minor").is_err());
        assert!(parse_incident_status("closed").is_err());
    }

    #[test]
    fn domain_labels_match_schema_values() {
        assert_eq!(
            domain_verification_status_label(DomainVerificationStatus::Pending),
            "pending"
        );
        assert_eq!(
            domain_verification_status_label(DomainVerificationStatus::Verified),
            "verified"
        );
        assert_eq!(
            domain_verification_status_label(DomainVerificationStatus::Failed),
            "failed"
        );
        assert_eq!(domain_tls_status_label(DomainTlsStatus::Pending), "pending");
        assert_eq!(domain_tls_status_label(DomainTlsStatus::Active), "active");
        assert_eq!(domain_tls_status_label(DomainTlsStatus::Failed), "failed");
        assert_eq!(
            parse_domain_verification_status("verified").expect("status parses"),
            DomainVerificationStatus::Verified
        );
        assert_eq!(
            parse_domain_tls_status("active").expect("status parses"),
            DomainTlsStatus::Active
        );
        assert!(parse_domain_verification_status("validating").is_err());
        assert!(parse_domain_tls_status("issued").is_err());
    }

    #[test]
    fn network_access_labels_match_schema_values() {
        assert_eq!(ip_allowlist_purpose_label(IpAllowlistPurpose::App), "app");
        assert_eq!(
            ip_allowlist_purpose_label(IpAllowlistPurpose::Migration),
            "migration"
        );
        assert_eq!(
            ip_allowlist_purpose_label(IpAllowlistPurpose::Support),
            "support"
        );
        assert_eq!(
            ip_allowlist_status_label(IpAllowlistStatus::Active),
            "active"
        );
        assert_eq!(
            ip_allowlist_status_label(IpAllowlistStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            static_egress_ip_status_label(StaticEgressIpStatus::Provisioning),
            "provisioning"
        );
        assert_eq!(
            static_egress_ip_status_label(StaticEgressIpStatus::Active),
            "active"
        );
        assert_eq!(
            static_egress_ip_status_label(StaticEgressIpStatus::Retired),
            "retired"
        );
        assert_eq!(
            parse_ip_allowlist_purpose("migration").expect("purpose parses"),
            IpAllowlistPurpose::Migration
        );
        assert_eq!(
            parse_ip_allowlist_status("disabled").expect("status parses"),
            IpAllowlistStatus::Disabled
        );
        assert_eq!(
            parse_static_egress_ip_status("active").expect("status parses"),
            StaticEgressIpStatus::Active
        );
        assert!(parse_ip_allowlist_purpose("admin").is_err());
        assert!(parse_ip_allowlist_status("deleted").is_err());
        assert!(parse_static_egress_ip_status("allocated").is_err());
    }

    #[test]
    fn maintenance_window_labels_match_schema_values() {
        assert_eq!(
            maintenance_day_of_week_label(MaintenanceDayOfWeek::Monday),
            "monday"
        );
        assert_eq!(
            maintenance_day_of_week_label(MaintenanceDayOfWeek::Sunday),
            "sunday"
        );
        assert_eq!(
            maintenance_window_status_label(MaintenanceWindowStatus::Active),
            "active"
        );
        assert_eq!(
            maintenance_window_status_label(MaintenanceWindowStatus::Disabled),
            "disabled"
        );
        assert_eq!(
            parse_maintenance_day_of_week("wednesday").expect("day parses"),
            MaintenanceDayOfWeek::Wednesday
        );
        assert_eq!(
            parse_maintenance_window_status("disabled").expect("status parses"),
            MaintenanceWindowStatus::Disabled
        );
        assert!(parse_maintenance_day_of_week("weekday").is_err());
        assert!(parse_maintenance_window_status("paused").is_err());
    }

    #[test]
    fn completed_agent_commands_advance_cluster_lifecycle() {
        let next = next_state_after_agent_command(
            ClusterLifecycleState::InitializingPostgres,
            &NodeAgentAction::PreparePostgres {
                postgres_version: PostgresVersion::new("18").expect("valid version"),
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                port: 55_000,
            },
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Starting));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::Verifying,
            &NodeAgentAction::StartPostgres,
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::ConfiguringRoles));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::Starting,
            &NodeAgentAction::StartPostgres,
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Ready));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::ConfiguringReplication,
            &NodeAgentAction::ConfigurePostgresAccess {
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                port: 55_000,
                database: "postgres".to_owned(),
                roles: Vec::new(),
                publication: "palimpsest_publication".to_owned(),
                replication_slot: "cluster_123_palimpsest_slot".to_owned(),
            },
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Ready));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::InitializingPostgres,
            &NodeAgentAction::StartPostgres,
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, None);

        let next = next_state_after_agent_command(
            ClusterLifecycleState::Deleting,
            &NodeAgentAction::DeletePostgresData {
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                tombstone_retention_days: None,
            },
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Deleted));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::Resizing,
            &NodeAgentAction::ResizePostgresStorage {
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                storage_gib: 32,
            },
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Ready));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::UpdatingPostgres,
            &NodeAgentAction::UpdatePostgresMinor {
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                target_postgres_version: PostgresVersion::new("18.4").expect("valid version"),
            },
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Ready));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::UpdatingPostgres,
            &NodeAgentAction::UpgradePostgresMajor {
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                source_port: Some(55_000),
                database: Some("postgres".to_owned()),
                source_postgres_version: PostgresVersion::new("18.5").expect("valid version"),
                target_postgres_version: PostgresVersion::new("19").expect("valid version"),
                strategy: ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy,
            },
            AgentCommandStatus::Succeeded,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Ready));

        let next = next_state_after_agent_command(
            ClusterLifecycleState::InitializingPostgres,
            &NodeAgentAction::PreparePostgres {
                postgres_version: PostgresVersion::new("18").expect("valid version"),
                data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
                port: 55_000,
            },
            AgentCommandStatus::Failed,
        );
        assert_eq!(next, Some(ClusterLifecycleState::Failed));

        let configure_access = NodeAgentAction::ConfigurePostgresAccess {
            data_dir: "/var/lib/palimpsest/postgres/cluster_123".to_owned(),
            port: 55_000,
            database: "postgres".to_owned(),
            roles: Vec::new(),
            publication: "palimpsest_publication".to_owned(),
            replication_slot: "cluster_123_palimpsest_slot".to_owned(),
        };
        let next = next_state_after_agent_command_for_operation(
            ClusterLifecycleState::ConfiguringReplication,
            &configure_access,
            AgentCommandStatus::Failed,
            Some("rotate_credentials"),
        );
        assert_eq!(next, Some(ClusterLifecycleState::Ready));

        let next = next_state_after_agent_command_for_operation(
            ClusterLifecycleState::ConfiguringReplication,
            &configure_access,
            AgentCommandStatus::Failed,
            Some("create_cluster"),
        );
        assert_eq!(next, Some(ClusterLifecycleState::Failed));

        let next = next_state_after_agent_command_for_operation(
            ClusterLifecycleState::Ready,
            &NodeAgentAction::StartSyncDeployment {
                deployment_id: "sync_123".to_owned(),
                config_version: "config_001".to_owned(),
            },
            AgentCommandStatus::Failed,
            Some("start_sync_deployment"),
        );
        assert_eq!(next, None);
    }
}
