// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Additive control-plane primitives for managed Postgres.

// Docs name products (CloudNativePG, PostgreSQL, Kubernetes, kubectl) that
// `doc_markdown` would otherwise want backticked throughout prose.
#![allow(clippy::doc_markdown)]

pub mod runtime;
pub mod sql_store;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palimpsest_paas_core::{
    AgentCommandStatus, ApiKey, AuditEvent, BackupArtifactStatus, BackupLifecycleState,
    BillingExport, BillingExportStatus, BranchLifecycleState, BranchMode,
    CertificateAuthorityProviderKind, CertificateAuthorityProviderStatus,
    CertificateLifecycleState, CloneRedactionPolicyStatus, ClusterLifecycleState, ConfigVersion,
    ConfigVersionStatus, CustomerEnvironmentHealth, DatabaseProxyRoute, DatabaseRoleCredential,
    Domain, DomainTlsStatus, DomainVerificationStatus, Environment, FailoverLifecycleState,
    GatewayRoute, GatewayRouteMtlsBundle, HostAssignment, Incident, IncidentSeverity,
    IncidentStatus, IpAllowlistPurpose, IpAllowlistRule, IpAllowlistStatus, JwtIssuer,
    JwtIssuerStatus, MaintenanceDayOfWeek, MaintenanceWindow, MaintenanceWindowStatus,
    ManagedPostgresAcmeOrder, ManagedPostgresBackup, ManagedPostgresBackupArtifact,
    ManagedPostgresBackupRetentionPolicy, ManagedPostgresBranch,
    ManagedPostgresCertificateAuthorityProvider, ManagedPostgresCloneRedactionPolicy,
    ManagedPostgresCluster, ManagedPostgresDeletionTombstone, ManagedPostgresEndpoint,
    ManagedPostgresEndpointCertificate, ManagedPostgresEndpointCertificateBundle,
    ManagedPostgresFailover, ManagedPostgresMajorUpgrade, ManagedPostgresMajorUpgradeStatus,
    ManagedPostgresMajorUpgradeStrategy, ManagedPostgresPitrCheck, ManagedPostgresRestore,
    ManagedPostgresRestoreDrill, ManagedPostgresRuntimeCheck, ManagedPostgresSpec,
    ManagedPostgresStandby, ManagedPostgresStandbyCheck, ManagedPostgresSupportAccessSession,
    ManagedPostgresWalArchiveSegment, NodeAgentAction, NodeAgentCommand, NodeAgentCommandResult,
    NodeHost, NodeHostAgentCredential, NodeHostAgentCredentialState, NodeHostHardeningCheck,
    NodeHostHardeningStatus, NodeHostHeartbeat, NodeHostState, OperationKind, OperationRecord,
    OperationStatus, Organization, PermissionRuleDocument, PitrCheckStatus, PostgresVersion,
    Project, QueryPermissionOperation, QueryPermissionPolicy, QueryPermissionPolicyStatus,
    QueuedNodeAgentCommand, QuotaAlert, QuotaAlertState, QuotaPolicy, RestoreLifecycleState,
    RuntimeCheckStatus, SecretEncryptionKey, SecretEncryptionKeyStatus, SecretRewrapPlan,
    SecretRewrapPlanStatus, SsoIdentityProvider, SsoProviderKind, SsoProviderStatus,
    StandbyCheckStatus, StandbyLifecycleState, StaticEgressIp, StaticEgressIpStatus,
    SupportAccessStatus, SyncDeployment, SyncDeploymentLifecycleState, TeamMembership, TeamRole,
    UsageEvent, WalArchiveSegmentStatus, WebhookEndpoint, WebhookEndpointStatus,
    MIN_SUPPORTED_POSTGRES_MAJOR,
};
use palimpsest_paas_runtime::{RenderedManifests, RuntimeConfig};
use ring::{
    digest, hmac,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    types::Json as SqlJson,
    Row,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostCapacity {
    pub host_id: String,
    pub data_root: String,
    pub first_port: u16,
    pub max_clusters: usize,
    pub assigned_clusters: usize,
    pub used_ports: BTreeSet<u16>,
    pub storage_gib: u32,
    pub used_storage_gib: u32,
}

impl HostCapacity {
    #[must_use]
    pub fn can_accept_cluster(&self, requested_storage_gib: u32) -> bool {
        self.assigned_clusters < self.max_clusters
            && self
                .used_storage_gib
                .checked_add(requested_storage_gib)
                .is_some_and(|used| used <= self.storage_gib)
    }

    pub fn next_available_port(&self) -> Result<u16, ControlPlaneError> {
        for offset in 0..self.max_clusters {
            let offset =
                u16::try_from(offset).map_err(|_| ControlPlaneError::PortRangeExhausted)?;
            let port = self
                .first_port
                .checked_add(offset)
                .ok_or(ControlPlaneError::PortRangeExhausted)?;
            if !self.used_ports.contains(&port) {
                return Ok(port);
            }
        }
        Err(ControlPlaneError::PortRangeExhausted)
    }
}

#[derive(Debug, Default)]
pub struct PlacementEngine;

impl PlacementEngine {
    pub fn place(
        &self,
        cluster: &ManagedPostgresCluster,
        hosts: &[HostCapacity],
    ) -> Result<HostAssignment, ControlPlaneError> {
        if let Some(existing) = &cluster.host_assignment {
            return Ok(existing.clone());
        }

        let host = hosts
            .iter()
            .find(|host| host.can_accept_cluster(cluster.storage_gib))
            .ok_or(ControlPlaneError::NoHostCapacity)?;

        Ok(HostAssignment {
            host_id: host.host_id.clone(),
            data_dir: format!(
                "{}/{}",
                host.data_root.trim_end_matches('/'),
                cluster.cluster_id
            ),
            port: host.next_available_port()?,
        })
    }
}

/// What the runtime should do for a cluster after a reconcile pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAction {
    /// Server-side apply the rendered CloudNativePG manifests.
    Apply,
    /// Delete the cluster's CloudNativePG resources.
    Delete,
    /// Nothing to do (terminal or externally-driven state).
    None,
}

/// Turns managed-Postgres desired state into a CloudNativePG reconcile plan.
#[derive(Debug, Default)]
pub struct Reconciler;

impl Reconciler {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Compute the next lifecycle state and the runtime action for `cluster`.
    ///
    /// CloudNativePG owns the granular provisioning steps the host runtime used
    /// to drive command-by-command, so the provisioning states collapse to a
    /// single declarative apply. Readiness is then gated on the operator
    /// reporting ready instances (see `reconcile_managed_postgres_cluster_once`).
    pub fn reconcile(
        &self,
        cluster: &ManagedPostgresCluster,
        spec: Option<&ManagedPostgresSpec>,
        roles: &[DatabaseRoleCredential],
        config: &RuntimeConfig,
    ) -> Result<ReconcilePlan, ControlPlaneError> {
        let mut next_cluster = cluster.clone();
        // CloudNativePG schedules onto Kubernetes nodes; there is no owned host
        // assignment.
        next_cluster.host_assignment = None;

        let action = match cluster.lifecycle_state {
            ClusterLifecycleState::Requested
            | ClusterLifecycleState::Placing
            | ClusterLifecycleState::AllocatingStorage
            | ClusterLifecycleState::InitializingPostgres
            | ClusterLifecycleState::ConfiguringRoles
            | ClusterLifecycleState::ConfiguringReplication
            | ClusterLifecycleState::Restoring
            | ClusterLifecycleState::Resizing
            | ClusterLifecycleState::UpdatingPostgres
            | ClusterLifecycleState::Starting => {
                next_cluster.lifecycle_state = ClusterLifecycleState::Verifying;
                RuntimeAction::Apply
            }
            // Re-apply when Verifying/Ready to converge any drift; the caller
            // promotes Verifying -> Ready once the operator reports readiness.
            ClusterLifecycleState::Verifying | ClusterLifecycleState::Ready => RuntimeAction::Apply,
            // Pause: apply the hibernation annotation (rendered from the Stopped
            // state) so CloudNativePG scales the cluster down but keeps volumes.
            ClusterLifecycleState::Stopping | ClusterLifecycleState::Stopped => {
                next_cluster.lifecycle_state = ClusterLifecycleState::Stopped;
                RuntimeAction::Apply
            }
            ClusterLifecycleState::Deleting => {
                next_cluster.lifecycle_state = ClusterLifecycleState::Deleted;
                RuntimeAction::Delete
            }
            ClusterLifecycleState::Deleted | ClusterLifecycleState::Failed => RuntimeAction::None,
        };

        let manifests = match action {
            RuntimeAction::Apply | RuntimeAction::Delete => Some(
                RenderedManifests::render(cluster, spec, roles, config)
                    .map_err(|err| ControlPlaneError::Runtime(err.to_string()))?,
            ),
            RuntimeAction::None => None,
        };

        Ok(ReconcilePlan {
            next_cluster,
            action,
            manifests,
        })
    }
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

fn validate_database_identifier(value: &str) -> Result<(), SqlApiError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(SqlApiError::BadRequest(format!(
            "invalid postgres database identifier '{value}'"
        )));
    }
    Ok(())
}

fn endpoint_for_database(
    mut endpoint: sql_store::ManagedPostgresSqlEndpoint,
    database: Option<&str>,
) -> Result<sql_store::ManagedPostgresSqlEndpoint, SqlApiError> {
    if let Some(database) = database {
        validate_database_identifier(database)?;
        endpoint.database = database.to_owned();
    }
    Ok(endpoint)
}

fn bounded_identifier_with_suffix(value: &str, suffix: &str, max_len: usize) -> String {
    let prefix_len = max_len.saturating_sub(suffix.len());
    let prefix: String = sanitize_identifier_component(value)
        .chars()
        .take(prefix_len)
        .collect();
    format!("{prefix}{suffix}")
}

fn standby_physical_slot_name(target_cluster_id: &str) -> String {
    let sanitized = sanitize_identifier_component(target_cluster_id);
    let suffix = if sanitized.ends_with("_standby") {
        "_slot"
    } else {
        "_standby_slot"
    };
    bounded_identifier_with_suffix(&sanitized, suffix, 63)
}

#[derive(Debug, Clone)]
pub struct ReconcilePlan {
    pub next_cluster: ManagedPostgresCluster,
    pub action: RuntimeAction,
    pub manifests: Option<RenderedManifests>,
}

#[derive(Debug, Default)]
pub struct InMemoryControlPlaneStore {
    organizations: BTreeMap<String, Organization>,
    projects: BTreeMap<String, Project>,
    environments: BTreeMap<String, Environment>,
    config_versions: BTreeMap<String, ConfigVersion>,
    clusters: BTreeMap<String, ManagedPostgresCluster>,
    sync_deployments: BTreeMap<String, SyncDeployment>,
    operations: BTreeMap<String, OperationRecord>,
    audit_events: Vec<AuditEvent>,
}

impl InMemoryControlPlaneStore {
    pub fn insert_organization(
        &mut self,
        organization: Organization,
    ) -> Result<(), ControlPlaneError> {
        if self
            .organizations
            .contains_key(&organization.organization_id)
        {
            return Err(ControlPlaneError::DuplicateResource(
                organization.organization_id,
            ));
        }
        self.organizations
            .insert(organization.organization_id.clone(), organization);
        Ok(())
    }

    #[must_use]
    pub fn organization(&self, organization_id: &str) -> Option<&Organization> {
        self.organizations.get(organization_id)
    }

    pub fn insert_project(&mut self, project: Project) -> Result<(), ControlPlaneError> {
        if !self.organizations.contains_key(&project.organization_id) {
            return Err(ControlPlaneError::MissingResource(project.organization_id));
        }
        if self.projects.contains_key(&project.project_id) {
            return Err(ControlPlaneError::DuplicateResource(project.project_id));
        }
        self.projects.insert(project.project_id.clone(), project);
        Ok(())
    }

    #[must_use]
    pub fn project(&self, project_id: &str) -> Option<&Project> {
        self.projects.get(project_id)
    }

    pub fn insert_environment(
        &mut self,
        environment: Environment,
    ) -> Result<(), ControlPlaneError> {
        if !self.projects.contains_key(&environment.project_id) {
            return Err(ControlPlaneError::MissingResource(environment.project_id));
        }
        if self.environments.contains_key(&environment.environment_id) {
            return Err(ControlPlaneError::DuplicateResource(
                environment.environment_id,
            ));
        }
        self.environments
            .insert(environment.environment_id.clone(), environment);
        Ok(())
    }

    #[must_use]
    pub fn environment(&self, environment_id: &str) -> Option<&Environment> {
        self.environments.get(environment_id)
    }

    pub fn upsert_config_version(
        &mut self,
        config: ConfigVersion,
    ) -> Result<(), ControlPlaneError> {
        if self
            .config_versions
            .get(&config.config_version)
            .is_some_and(|existing| existing.status == ConfigVersionStatus::Deployed)
        {
            return Err(ControlPlaneError::ImmutableResource(config.config_version));
        }
        self.config_versions
            .insert(config.config_version.clone(), config);
        Ok(())
    }

    #[must_use]
    pub fn config_version(&self, config_version: &str) -> Option<&ConfigVersion> {
        self.config_versions.get(config_version)
    }

    pub fn upsert_cluster(&mut self, cluster: ManagedPostgresCluster) {
        self.clusters.insert(cluster.cluster_id.clone(), cluster);
    }

    #[must_use]
    pub fn cluster(&self, cluster_id: &str) -> Option<&ManagedPostgresCluster> {
        self.clusters.get(cluster_id)
    }

    pub fn insert_sync_deployment(
        &mut self,
        deployment: SyncDeployment,
    ) -> Result<(), ControlPlaneError> {
        if !self.environments.contains_key(&deployment.environment_id) {
            return Err(ControlPlaneError::MissingResource(
                deployment.environment_id,
            ));
        }
        if !self
            .clusters
            .contains_key(&deployment.managed_postgres_cluster_id)
        {
            return Err(ControlPlaneError::MissingResource(
                deployment.managed_postgres_cluster_id,
            ));
        }
        if self
            .sync_deployments
            .contains_key(&deployment.deployment_id)
        {
            return Err(ControlPlaneError::DuplicateResource(
                deployment.deployment_id,
            ));
        }
        self.sync_deployments
            .insert(deployment.deployment_id.clone(), deployment);
        Ok(())
    }

    #[must_use]
    pub fn sync_deployment(&self, deployment_id: &str) -> Option<&SyncDeployment> {
        self.sync_deployments.get(deployment_id)
    }

    pub fn insert_operation(
        &mut self,
        operation: OperationRecord,
    ) -> Result<(), ControlPlaneError> {
        if self.operations.contains_key(&operation.operation_id) {
            return Err(ControlPlaneError::DuplicateOperation(
                operation.operation_id,
            ));
        }
        self.operations
            .insert(operation.operation_id.clone(), operation);
        Ok(())
    }

    #[must_use]
    pub fn operation(&self, operation_id: &str) -> Option<&OperationRecord> {
        self.operations.get(operation_id)
    }

    pub fn append_audit_event(&mut self, event: AuditEvent) {
        self.audit_events.push(event);
    }

    #[must_use]
    pub fn audit_events(&self) -> &[AuditEvent] {
        &self.audit_events
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneError {
    #[error("no host has capacity for the requested managed postgres cluster")]
    NoHostCapacity,
    #[error("kubernetes runtime error: {0}")]
    Runtime(String),
    #[error("cluster is missing a host assignment")]
    MissingHostAssignment,
    #[error("port range exhausted for host assignment")]
    PortRangeExhausted,
    #[error("operation already exists: {0}")]
    DuplicateOperation(String),
    #[error("resource already exists: {0}")]
    DuplicateResource(String),
    #[error("resource not found: {0}")]
    MissingResource(String),
    #[error("resource is immutable: {0}")]
    ImmutableResource(String),
}

#[derive(Debug, Default)]
pub struct ControlPlaneService {
    store: InMemoryControlPlaneStore,
    next_audit_id: u64,
}

impl ControlPlaneService {
    pub fn create_organization(
        &mut self,
        actor_id: &str,
        organization: Organization,
    ) -> Result<(), ControlPlaneError> {
        let resource_id = organization.organization_id.clone();
        self.store.insert_organization(organization)?;
        self.audit(actor_id, "organization.create", &resource_id);
        Ok(())
    }

    pub fn create_project(
        &mut self,
        actor_id: &str,
        project: Project,
    ) -> Result<(), ControlPlaneError> {
        let resource_id = project.project_id.clone();
        self.store.insert_project(project)?;
        self.audit(actor_id, "project.create", &resource_id);
        Ok(())
    }

    pub fn create_environment(
        &mut self,
        actor_id: &str,
        environment: Environment,
    ) -> Result<(), ControlPlaneError> {
        let resource_id = environment.environment_id.clone();
        self.store.insert_environment(environment)?;
        self.audit(actor_id, "environment.create", &resource_id);
        Ok(())
    }

    pub fn upload_config(
        &mut self,
        actor_id: &str,
        config: ConfigVersion,
    ) -> Result<(), ControlPlaneError> {
        if !self.store.environments.contains_key(&config.environment_id) {
            return Err(ControlPlaneError::MissingResource(config.environment_id));
        }
        let resource_id = config.config_version.clone();
        self.store.upsert_config_version(config)?;
        self.audit(actor_id, "config.upload", &resource_id);
        Ok(())
    }

    pub fn create_managed_postgres_cluster(
        &mut self,
        actor_id: &str,
        cluster: ManagedPostgresCluster,
    ) -> Result<(), ControlPlaneError> {
        if !self
            .store
            .environments
            .contains_key(&cluster.environment_id)
        {
            return Err(ControlPlaneError::MissingResource(cluster.environment_id));
        }
        if self.store.clusters.contains_key(&cluster.cluster_id) {
            return Err(ControlPlaneError::DuplicateResource(cluster.cluster_id));
        }

        let resource_id = cluster.cluster_id.clone();
        self.store.upsert_cluster(cluster);
        self.audit(actor_id, "managed_postgres_cluster.create", &resource_id);
        Ok(())
    }

    pub fn create_sync_deployment(
        &mut self,
        actor_id: &str,
        deployment: SyncDeployment,
    ) -> Result<(), ControlPlaneError> {
        let resource_id = deployment.deployment_id.clone();
        self.store.insert_sync_deployment(deployment)?;
        self.audit(actor_id, "sync_deployment.create", &resource_id);
        Ok(())
    }

    pub fn config_diff(
        &self,
        old_version: &str,
        new_version: &str,
    ) -> Result<ConfigDiff, ControlPlaneError> {
        let old = self
            .store
            .config_version(old_version)
            .ok_or_else(|| ControlPlaneError::MissingResource(old_version.to_owned()))?;
        let new = self
            .store
            .config_version(new_version)
            .ok_or_else(|| ControlPlaneError::MissingResource(new_version.to_owned()))?;

        Ok(ConfigDiff {
            old_version: old.config_version.clone(),
            new_version: new.config_version.clone(),
            rendered_hash_changed: old.rendered_hash != new.rendered_hash,
            status_changed: old.status != new.status,
        })
    }

    #[must_use]
    pub const fn store(&self) -> &InMemoryControlPlaneStore {
        &self.store
    }

    fn audit(&mut self, actor_id: &str, action: &str, resource_id: &str) {
        self.next_audit_id += 1;
        self.store.append_audit_event(AuditEvent {
            event_id: format!("audit_{}", self.next_audit_id),
            actor_id: actor_id.to_owned(),
            action: action.to_owned(),
            resource_id: resource_id.to_owned(),
            occurred_at: "1970-01-01T00:00:00Z".to_owned(),
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDiff {
    pub old_version: String,
    pub new_version: String,
    pub rendered_hash_changed: bool,
    pub status_changed: bool,
}

pub type SharedControlPlaneService = Arc<Mutex<ControlPlaneService>>;

pub fn control_plane_router(service: SharedControlPlaneService) -> Router {
    Router::new()
        .route("/v1/organizations", post(api_create_organization))
        .route("/v1/projects", post(api_create_project))
        .route("/v1/environments", post(api_create_environment))
        .route("/v1/configs", post(api_upload_config))
        .route("/v1/configs/diff", get(api_config_diff))
        .route(
            "/v1/managed-postgres/clusters",
            post(api_create_managed_postgres_cluster),
        )
        .route("/v1/sync-deployments", post(api_create_sync_deployment))
        .with_state(service)
}

pub type SharedSqlControlPlaneStore = Arc<sql_store::SqlControlPlaneStore>;

pub fn sql_control_plane_router(store: SharedSqlControlPlaneStore) -> Router {
    Router::new()
        .route(
            "/.well-known/acme-challenge/:token",
            get(sql_api_acme_http_01_challenge),
        )
        .route("/v1/organizations", post(sql_api_create_organization))
        .route(
            "/v1/onboarding/workspaces",
            post(sql_api_create_onboarding_workspace),
        )
        .route("/v1/projects", post(sql_api_create_project))
        .route("/v1/environments", post(sql_api_create_environment))
        .route(
            "/v1/environments/:environment_id/health",
            get(sql_api_environment_health),
        )
        .route(
            "/v1/environments/:environment_id/overview",
            get(sql_api_environment_overview),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint",
            get(sql_api_get_managed_postgres_endpoint),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/database-proxy-route",
            post(sql_api_configure_managed_postgres_database_proxy_route)
                .delete(sql_api_deconfigure_managed_postgres_database_proxy_route),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates",
            post(sql_api_issue_managed_postgres_endpoint_certificate)
                .get(sql_api_list_managed_postgres_endpoint_certificates),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates/renew",
            post(sql_api_renew_managed_postgres_endpoint_certificate),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates/:certificate_id",
            get(sql_api_get_managed_postgres_endpoint_certificate),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates/:certificate_id/bundle",
            get(sql_api_get_managed_postgres_endpoint_certificate_bundle),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates/:certificate_id/acme-order",
            get(sql_api_get_managed_postgres_acme_order),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates/:certificate_id/acme-challenge/validate",
            post(sql_api_validate_managed_postgres_acme_challenge),
        )
        .route(
            "/v1/environments/:environment_id/managed-postgres-endpoint/certificates/:certificate_id/acme-finalize",
            post(sql_api_finalize_managed_postgres_acme_order),
        )
        .route(
            "/v1/managed-postgres/certificate-authority-providers",
            post(sql_api_upsert_managed_postgres_certificate_authority_provider)
                .get(sql_api_list_managed_postgres_certificate_authority_providers),
        )
        .route(
            "/v1/managed-postgres/certificate-authority-providers/:ca_provider_id",
            get(sql_api_get_managed_postgres_certificate_authority_provider),
        )
        .route(
            "/v1/node-hosts",
            post(sql_api_upsert_node_host).get(sql_api_list_node_hosts),
        )
        .route("/v1/node-hosts/:host_id", get(sql_api_get_node_host))
        .route(
            "/v1/node-hosts/:host_id/agent-credentials",
            post(sql_api_issue_node_host_agent_credential)
                .get(sql_api_list_node_host_agent_credentials),
        )
        .route(
            "/v1/node-hosts/:host_id/agent-credentials/:key_id/revoke",
            post(sql_api_revoke_node_host_agent_credential),
        )
        .route(
            "/v1/node-hosts/:host_id/hardening-checks",
            post(sql_api_record_node_host_hardening_check)
                .get(sql_api_list_node_host_hardening_checks),
        )
        .route(
            "/v1/node-hosts/:host_id/hardening-checks/:check_id",
            get(sql_api_get_node_host_hardening_check),
        )
        .route(
            "/v1/node-hosts/:host_id/state",
            post(sql_api_set_node_host_state),
        )
        .route(
            "/v1/node-hosts/:host_id/heartbeat",
            post(sql_api_record_node_heartbeat),
        )
        .route(
            "/v1/node-hosts/:host_id/commands",
            post(sql_api_enqueue_agent_command),
        )
        .route(
            "/v1/node-hosts/:host_id/commands/lease",
            post(sql_api_lease_next_agent_command),
        )
        .route(
            "/v1/node-hosts/:host_id/commands/:command_id/complete",
            post(sql_api_complete_agent_command),
        )
        .route(
            "/v1/configs",
            post(sql_api_upload_config).get(sql_api_list_configs),
        )
        .route("/v1/configs/diff", get(sql_api_config_diff))
        .route(
            "/v1/environments/:environment_id/configs/rollback",
            post(sql_api_rollback_environment_config),
        )
        .route(
            "/v1/usage-events",
            post(sql_api_record_usage_event).get(sql_api_export_usage_events),
        )
        .route(
            "/v1/billing-exports",
            post(sql_api_create_billing_export).get(sql_api_list_billing_exports),
        )
        .route(
            "/v1/billing-exports/:export_id",
            get(sql_api_get_billing_export),
        )
        .route(
            "/v1/scheduler/backups/run-once",
            post(sql_api_run_backup_scheduler_once),
        )
        .route(
            "/v1/scheduler/backup-retention/run-once",
            post(sql_api_run_backup_retention_scheduler_once),
        )
        .route(
            "/v1/scheduler/restore-drills/run-once",
            post(sql_api_run_restore_drill_scheduler_once),
        )
        .route(
            "/v1/scheduler/pitr-checks/run-once",
            post(sql_api_run_pitr_check_scheduler_once),
        )
        .route(
            "/v1/scheduler/acme-orders/run-once",
            post(sql_api_run_acme_order_scheduler_once),
        )
        .route(
            "/v1/scheduler/maintenance/run-once",
            post(sql_api_run_maintenance_scheduler_once),
        )
        .route(
            "/v1/api-keys",
            post(sql_api_create_api_key).get(sql_api_list_api_keys),
        )
        .route("/v1/api-keys/:key_id/revoke", post(sql_api_revoke_api_key))
        .route(
            "/v1/secret-encryption-keys",
            post(sql_api_upsert_secret_encryption_key).get(sql_api_list_secret_encryption_keys),
        )
        .route(
            "/v1/secret-encryption-keys/:key_ref",
            get(sql_api_get_secret_encryption_key),
        )
        .route(
            "/v1/secret-rewrap-plans",
            post(sql_api_create_secret_rewrap_plan).get(sql_api_list_secret_rewrap_plans),
        )
        .route(
            "/v1/secret-rewrap-plans/:plan_id",
            get(sql_api_get_secret_rewrap_plan),
        )
        .route(
            "/v1/secret-rewrap-plans/:plan_id/run",
            post(sql_api_run_secret_rewrap_plan),
        )
        .route(
            "/v1/jwt-issuers",
            post(sql_api_upsert_jwt_issuer).get(sql_api_list_jwt_issuers),
        )
        .route("/v1/jwt-issuers/:issuer_id", get(sql_api_get_jwt_issuer))
        .route(
            "/v1/webhook-endpoints",
            post(sql_api_upsert_webhook_endpoint).get(sql_api_list_webhook_endpoints),
        )
        .route(
            "/v1/webhook-endpoints/:endpoint_id",
            get(sql_api_get_webhook_endpoint),
        )
        .route(
            "/v1/sso-providers",
            post(sql_api_upsert_sso_identity_provider).get(sql_api_list_sso_identity_providers),
        )
        .route(
            "/v1/sso-providers/:provider_id",
            get(sql_api_get_sso_identity_provider),
        )
        .route(
            "/v1/incidents",
            post(sql_api_upsert_incident).get(sql_api_list_incidents),
        )
        .route("/v1/incidents/:incident_id", get(sql_api_get_incident))
        .route(
            "/v1/team-memberships",
            post(sql_api_upsert_team_membership).get(sql_api_list_team_memberships),
        )
        .route("/v1/audit-events", get(sql_api_list_audit_events))
        .route("/metrics", get(sql_api_metrics))
        .route(
            "/v1/quota-policies",
            post(sql_api_upsert_quota_policy).get(sql_api_list_quota_policies),
        )
        .route(
            "/v1/quota-policies/:policy_id",
            get(sql_api_get_quota_policy),
        )
        .route(
            "/v1/quota-alerts",
            post(sql_api_upsert_quota_alert).get(sql_api_list_quota_alerts),
        )
        .route(
            "/v1/quota-alerts/evaluate",
            post(sql_api_evaluate_quota_alerts),
        )
        .route("/v1/quota-alerts/:alert_id", get(sql_api_get_quota_alert))
        .route(
            "/v1/managed-postgres/clusters",
            post(sql_api_create_managed_postgres_cluster)
                .get(sql_api_list_managed_postgres_clusters),
        )
        .route(
            "/v1/managed-postgres/deletion-tombstones",
            get(sql_api_list_managed_postgres_deletion_tombstones),
        )
        .route(
            "/v1/managed-postgres/deletion-tombstones/expire",
            post(sql_api_expire_managed_postgres_deletion_tombstones),
        )
        .route(
            "/v1/managed-postgres/deletion-tombstones/:cluster_id",
            get(sql_api_get_managed_postgres_deletion_tombstone),
        )
        .route(
            "/v1/sync-deployments",
            post(sql_api_create_sync_deployment).get(sql_api_list_sync_deployments),
        )
        .route(
            "/v1/sync-deployments/:deployment_id",
            get(sql_api_get_sync_deployment),
        )
        .route(
            "/v1/gateway-routes",
            post(sql_api_upsert_gateway_route).get(sql_api_list_gateway_routes),
        )
        .route(
            "/v1/gateway-routes/:host",
            get(sql_api_get_gateway_route).delete(sql_api_delete_gateway_route),
        )
        .route(
            "/v1/gateway-routes/:host/mtls-bundle",
            get(sql_api_get_gateway_route_mtls_bundle),
        )
        .route(
            "/v1/domains",
            post(sql_api_upsert_domain).get(sql_api_list_domains),
        )
        .route(
            "/v1/domains/:hostname",
            get(sql_api_get_domain).delete(sql_api_delete_domain),
        )
        .route(
            "/v1/ip-allowlist-rules",
            post(sql_api_upsert_ip_allowlist_rule).get(sql_api_list_ip_allowlist_rules),
        )
        .route(
            "/v1/ip-allowlist-rules/:rule_id",
            get(sql_api_get_ip_allowlist_rule).delete(sql_api_delete_ip_allowlist_rule),
        )
        .route(
            "/v1/static-egress-ips",
            post(sql_api_upsert_static_egress_ip).get(sql_api_list_static_egress_ips),
        )
        .route(
            "/v1/static-egress-ips/:egress_ip_id",
            get(sql_api_get_static_egress_ip).delete(sql_api_delete_static_egress_ip),
        )
        .route(
            "/v1/maintenance-windows",
            post(sql_api_upsert_maintenance_window).get(sql_api_list_maintenance_windows),
        )
        .route(
            "/v1/maintenance-windows/:window_id",
            get(sql_api_get_maintenance_window).delete(sql_api_delete_maintenance_window),
        )
        .route(
            "/v1/database-proxy-routes",
            post(sql_api_upsert_database_proxy_route).get(sql_api_list_database_proxy_routes),
        )
        .route(
            "/v1/database-proxy-routes/:listen_addr",
            get(sql_api_get_database_proxy_route).delete(sql_api_delete_database_proxy_route),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id",
            get(sql_api_get_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/schema",
            get(sql_api_get_managed_postgres_schema),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/sql-console/query",
            post(sql_api_query_managed_postgres_console),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/sample-data/seed",
            post(sql_api_seed_managed_postgres_sample_data),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/query-explorer/inspect",
            post(sql_api_inspect_managed_postgres_query),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/runtime-checks",
            get(sql_api_list_managed_postgres_runtime_checks),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/runtime-checks/probe",
            post(sql_api_probe_managed_postgres_runtime),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/runtime-checks/:check_id",
            get(sql_api_get_managed_postgres_runtime_check),
        )
        .route(
            "/v1/query-permission-policies",
            post(sql_api_upsert_query_permission_policy).get(sql_api_list_query_permission_policies),
        )
        .route(
            "/v1/query-permission-policies/:policy_id",
            get(sql_api_get_query_permission_policy),
        )
        .route(
            "/v1/query-permission-policies/:policy_id/dry-run",
            post(sql_api_dry_run_query_permission_policy),
        )
        .route(
            "/v1/environments/:environment_id/permission-rule-document",
            get(sql_api_get_permission_rule_document).put(sql_api_put_permission_rule_document),
        )
        .route("/v1/permissions/verify", post(sql_api_verify_permissions))
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/operations",
            get(sql_api_list_managed_postgres_operations),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/operations/:operation_id",
            get(sql_api_get_managed_postgres_operation),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/agent-commands",
            get(sql_api_list_managed_postgres_agent_commands),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/agent-commands/:command_id",
            get(sql_api_get_managed_postgres_agent_command),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/reconcile",
            post(sql_api_reconcile_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/stop",
            post(sql_api_stop_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/pause",
            post(sql_api_stop_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/resume",
            post(sql_api_resume_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/resize",
            post(sql_api_resize_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/roles/rotate",
            post(sql_api_rotate_managed_postgres_role_credentials),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/update-minor",
            post(sql_api_update_managed_postgres_minor),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/major-upgrades",
            post(sql_api_request_managed_postgres_major_upgrade)
                .get(sql_api_list_managed_postgres_major_upgrades),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/major-upgrades/:upgrade_id",
            get(sql_api_get_managed_postgres_major_upgrade),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/delete",
            post(sql_api_delete_managed_postgres_cluster),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/backups",
            post(sql_api_request_managed_postgres_backup)
                .get(sql_api_list_managed_postgres_backups),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/backups/:backup_id",
            get(sql_api_get_managed_postgres_backup),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/backups/:backup_id/artifacts",
            post(sql_api_upsert_managed_postgres_backup_artifact)
                .get(sql_api_list_managed_postgres_backup_artifacts),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/backups/:backup_id/artifacts/:artifact_id",
            get(sql_api_get_managed_postgres_backup_artifact),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/backup-retention-policy",
            get(sql_api_get_managed_postgres_backup_retention_policy)
                .post(sql_api_upsert_managed_postgres_backup_retention_policy),
        )
        .route(
            "/v1/managed-postgres/clone-redaction-policies",
            post(sql_api_upsert_managed_postgres_clone_redaction_policy)
                .get(sql_api_list_managed_postgres_clone_redaction_policies),
        )
        .route(
            "/v1/managed-postgres/clone-redaction-policies/:policy_id",
            get(sql_api_get_managed_postgres_clone_redaction_policy),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/support-access-sessions",
            post(sql_api_create_managed_postgres_support_access_session)
                .get(sql_api_list_managed_postgres_support_access_sessions),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/support-access-sessions/:session_id",
            get(sql_api_get_managed_postgres_support_access_session),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/support-access-sessions/:session_id/approve",
            post(sql_api_approve_managed_postgres_support_access_session),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/support-access-sessions/:session_id/revoke",
            post(sql_api_revoke_managed_postgres_support_access_session),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/wal-archives",
            post(sql_api_request_wal_archive).get(sql_api_list_wal_archives),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/wal-archives/:segment_name",
            get(sql_api_get_wal_archive),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/pitr-checks",
            post(sql_api_request_managed_postgres_pitr_check)
                .get(sql_api_list_managed_postgres_pitr_checks),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/pitr-checks/:check_id",
            get(sql_api_get_managed_postgres_pitr_check),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/failovers",
            post(sql_api_request_managed_postgres_failover)
                .get(sql_api_list_managed_postgres_failovers),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/failovers/:failover_id",
            get(sql_api_get_managed_postgres_failover),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/standbys",
            post(sql_api_request_managed_postgres_standby)
                .get(sql_api_list_managed_postgres_standbys),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/standbys/:standby_id",
            get(sql_api_get_managed_postgres_standby),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/standbys/:standby_id/checks",
            post(sql_api_request_managed_postgres_standby_check)
                .get(sql_api_list_managed_postgres_standby_checks),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/standbys/:standby_id/checks/:check_id",
            get(sql_api_get_managed_postgres_standby_check),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/restores",
            post(sql_api_request_managed_postgres_restore)
                .get(sql_api_list_managed_postgres_restores),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/database-clones",
            post(sql_api_request_managed_postgres_database_clone),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/branches",
            post(sql_api_create_managed_postgres_branch)
                .get(sql_api_list_managed_postgres_branches),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/branches/:branch_id",
            get(sql_api_get_managed_postgres_branch)
                .delete(sql_api_delete_managed_postgres_branch),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/restores/:restore_id",
            get(sql_api_get_managed_postgres_restore),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/restore-drills",
            post(sql_api_request_managed_postgres_restore_drill)
                .get(sql_api_list_managed_postgres_restore_drills),
        )
        .route(
            "/v1/managed-postgres/clusters/:cluster_id/restore-drills/:drill_id",
            get(sql_api_get_managed_postgres_restore_drill),
        )
        .with_state(store)
}

async fn api_create_organization(
    State(service): State<SharedControlPlaneService>,
    headers: HeaderMap,
    Json(payload): Json<Organization>,
) -> Result<Json<ApiOk>, ApiError> {
    let actor_id = actor_id(&headers);
    let mut service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    service.create_organization(&actor_id, payload)?;
    Ok(Json(ApiOk::new("organization.created")))
}

async fn api_create_project(
    State(service): State<SharedControlPlaneService>,
    headers: HeaderMap,
    Json(payload): Json<Project>,
) -> Result<Json<ApiOk>, ApiError> {
    let actor_id = actor_id(&headers);
    let mut service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    service.create_project(&actor_id, payload)?;
    Ok(Json(ApiOk::new("project.created")))
}

async fn api_create_environment(
    State(service): State<SharedControlPlaneService>,
    headers: HeaderMap,
    Json(payload): Json<Environment>,
) -> Result<Json<ApiOk>, ApiError> {
    let actor_id = actor_id(&headers);
    let mut service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    service.create_environment(&actor_id, payload)?;
    Ok(Json(ApiOk::new("environment.created")))
}

async fn api_upload_config(
    State(service): State<SharedControlPlaneService>,
    headers: HeaderMap,
    Json(payload): Json<ConfigVersion>,
) -> Result<Json<ApiOk>, ApiError> {
    let actor_id = actor_id(&headers);
    let mut service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    service.upload_config(&actor_id, payload)?;
    Ok(Json(ApiOk::new("config.uploaded")))
}

async fn api_create_managed_postgres_cluster(
    State(service): State<SharedControlPlaneService>,
    headers: HeaderMap,
    Json(payload): Json<ManagedPostgresCluster>,
) -> Result<Json<ApiOk>, ApiError> {
    let actor_id = actor_id(&headers);
    let mut service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    service.create_managed_postgres_cluster(&actor_id, payload)?;
    Ok(Json(ApiOk::new("managed_postgres_cluster.created")))
}

async fn api_create_sync_deployment(
    State(service): State<SharedControlPlaneService>,
    headers: HeaderMap,
    Json(payload): Json<SyncDeployment>,
) -> Result<Json<ApiOk>, ApiError> {
    let actor_id = actor_id(&headers);
    let mut service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    service.create_sync_deployment(&actor_id, payload)?;
    Ok(Json(ApiOk::new("sync_deployment.created")))
}

async fn api_config_diff(
    State(service): State<SharedControlPlaneService>,
    Query(query): Query<ConfigDiffQuery>,
) -> Result<Json<ConfigDiff>, ApiError> {
    let service = service.lock().map_err(|_| ApiError::ServicePoisoned)?;
    Ok(Json(service.config_diff(&query.old, &query.new)?))
}

async fn sql_api_create_organization(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<Organization>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::organization(&payload.organization_id))?;
    let resource_id = payload.organization_id.clone();
    store.insert_organization(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "organization.create",
            &resource_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("organization.created")))
}

async fn sql_api_create_onboarding_workspace(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<OnboardingWorkspaceRequest>,
) -> Result<Json<OnboardingWorkspaceResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::organization(
        &payload.organization.organization_id,
    ))?;
    auth.require_can_manage_role(TeamRole::Owner)?;
    let owner_membership = store
        .create_onboarding_workspace(
            auth.actor_id(),
            &payload.organization,
            &payload.project,
            &payload.environment,
            &payload.owner_actor_id,
        )
        .await?;
    Ok(Json(OnboardingWorkspaceResponse {
        organization: payload.organization,
        project: payload.project,
        environment: payload.environment,
        owner_membership,
    }))
}

async fn sql_api_create_project(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<Project>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::project(
        &payload.organization_id,
        &payload.project_id,
    ))?;
    let resource_id = payload.project_id.clone();
    store.insert_project(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "project.create",
            &resource_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("project.created")))
}

async fn sql_api_create_environment(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<Environment>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::environment(
        &payload.organization_id,
        &payload.project_id,
        &payload.environment_id,
    ))?;
    let resource_id = payload.environment_id.clone();
    store.insert_environment(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "environment.create",
            &resource_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("environment.created")))
}

async fn sql_api_environment_health(
    State(store): State<SharedSqlControlPlaneStore>,
    Path(environment_id): Path<String>,
) -> Result<Json<CustomerEnvironmentHealth>, SqlApiError> {
    Ok(Json(store.environment_health(&environment_id).await?))
}

async fn sql_api_environment_overview(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
) -> Result<Json<EnvironmentOverviewResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let (organization_id, project_id, environment_id) = store
        .environment_scope(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &organization_id,
        &project_id,
        &environment_id,
    ))?;

    let health = store.environment_health(&environment_id).await?;
    let managed_postgres_endpoint = store.managed_postgres_endpoint(&environment_id).await?;
    let managed_postgres_clusters = store
        .managed_postgres_clusters(&sql_store::ManagedPostgresClusterFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            lifecycle_state: None,
        })
        .await?;
    let sync_deployments = store
        .sync_deployments(&sql_store::SyncDeploymentFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            lifecycle_state: None,
        })
        .await?;
    let configs = store
        .config_versions_for_environment(&environment_id)
        .await?;
    let quota_alerts = store
        .quota_alerts(&sql_store::QuotaAlertFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            metric: None,
            state: None,
        })
        .await?;
    let domains = store
        .domains(&sql_store::DomainFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            verification_status: None,
            tls_status: None,
        })
        .await?;
    let ip_allowlist_rules = store
        .ip_allowlist_rules(&sql_store::IpAllowlistRuleFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            purpose: None,
            status: None,
        })
        .await?;
    let static_egress_ips = store
        .static_egress_ips(&sql_store::StaticEgressIpFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            region: None,
            status: None,
        })
        .await?;
    let maintenance_windows = store
        .maintenance_windows(&sql_store::MaintenanceWindowFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            status: None,
        })
        .await?;
    let incidents = store
        .incidents(&sql_store::IncidentFilter {
            organization_id: Some(&organization_id),
            project_id: Some(&project_id),
            environment_id: Some(&environment_id),
            severity: None,
            status: None,
            include_resolved: Some(false),
        })
        .await?;

    Ok(Json(EnvironmentOverviewResponse {
        organization_id,
        project_id,
        environment_id,
        health,
        managed_postgres_endpoint,
        managed_postgres_clusters,
        sync_deployments,
        configs,
        quota_alerts,
        domains,
        ip_allowlist_rules,
        static_egress_ips,
        maintenance_windows,
        incidents,
    }))
}

async fn sql_api_upsert_node_host(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<NodeHost>,
) -> Result<Json<ApiOk>, SqlApiError> {
    require_agent_auth(
        &store,
        &headers,
        AgentAuthScope::new(&payload.host_id, "register"),
    )
    .await?;
    let actor_id = actor_id(&headers);
    let resource_id = payload.host_id.clone();
    store.upsert_node_host(&payload).await?;
    store
        .append_audit_event(&audit_event(&actor_id, "node_host.upsert", &resource_id))
        .await?;
    Ok(Json(ApiOk::new("node_host.upserted")))
}

async fn sql_api_list_node_hosts(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<NodeHostsQuery>,
) -> Result<Json<NodeHostsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let hosts = store
        .node_hosts(&sql_store::NodeHostFilter {
            region: query.region.as_deref(),
            failure_domain: query.failure_domain.as_deref(),
            state: query.state,
        })
        .await?;
    Ok(Json(NodeHostsResponse { hosts }))
}

async fn sql_api_get_node_host(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
) -> Result<Json<NodeHost>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let host = store
        .node_host(&host_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(host_id.clone()))?;
    Ok(Json(host))
}

async fn ensure_node_host_exists(
    store: &SharedSqlControlPlaneStore,
    host_id: &str,
) -> Result<(), SqlApiError> {
    store
        .node_host(host_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(host_id.to_owned()))?;
    Ok(())
}

async fn sql_api_issue_node_host_agent_credential(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
) -> Result<Json<sql_store::IssuedNodeAgentCredential>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let credential = store.issue_node_host_agent_credential(&host_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "node_host.agent_credential.issue",
            &host_id,
        ))
        .await?;
    Ok(Json(credential))
}

async fn sql_api_list_node_host_agent_credentials(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
    Query(query): Query<NodeHostAgentCredentialsQuery>,
) -> Result<Json<NodeHostAgentCredentialsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let credentials = store
        .node_host_agent_credentials(
            &host_id,
            &sql_store::NodeHostAgentCredentialFilter { state: query.state },
        )
        .await?;
    Ok(Json(NodeHostAgentCredentialsResponse { credentials }))
}

async fn sql_api_revoke_node_host_agent_credential(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((host_id, key_id)): Path<(String, String)>,
) -> Result<Json<NodeHostAgentCredential>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let credential = store
        .revoke_node_host_agent_credential(&host_id, &key_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(key_id.clone()))?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "node_host.agent_credential.revoke",
            &key_id,
        ))
        .await?;
    Ok(Json(credential))
}

async fn sql_api_record_node_host_hardening_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
    Json(payload): Json<NodeHostHardeningCheck>,
) -> Result<Json<NodeHostHardeningCheck>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    ensure_node_host_exists(&store, &host_id).await?;
    validate_node_host_hardening_check(&host_id, &payload)?;
    let check = store.insert_node_host_hardening_check(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "node_host.hardening_check.record",
            &check.check_id,
        ))
        .await?;
    Ok(Json(check))
}

async fn sql_api_list_node_host_hardening_checks(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
    Query(query): Query<NodeHostHardeningChecksQuery>,
) -> Result<Json<NodeHostHardeningChecksResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    ensure_node_host_exists(&store, &host_id).await?;
    let checks = store
        .node_host_hardening_checks(&sql_store::NodeHostHardeningCheckFilter {
            host_id: &host_id,
            status: query.status,
        })
        .await?;
    Ok(Json(NodeHostHardeningChecksResponse { checks }))
}

async fn sql_api_get_node_host_hardening_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((host_id, check_id)): Path<(String, String)>,
) -> Result<Json<NodeHostHardeningCheck>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let check = store
        .node_host_hardening_check(&host_id, &check_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(check_id.clone()))?;
    Ok(Json(check))
}

async fn sql_api_set_node_host_state(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
    Json(payload): Json<SetNodeHostStateRequest>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    store.set_node_host_state(&host_id, payload.state).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "node_host.state.set",
            &host_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("node_host.state_set")))
}

async fn sql_api_record_node_heartbeat(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
    Json(payload): Json<NodeHostHeartbeat>,
) -> Result<Json<ApiOk>, SqlApiError> {
    require_agent_auth(&store, &headers, AgentAuthScope::new(&host_id, "heartbeat")).await?;
    store.record_node_heartbeat(&host_id, &payload).await?;
    reconcile_node_host_observed_resources(&store, &host_id, &payload).await?;
    Ok(Json(ApiOk::new("node_host.heartbeat_recorded")))
}

async fn reconcile_node_host_observed_resources(
    store: &SharedSqlControlPlaneStore,
    host_id: &str,
    heartbeat: &NodeHostHeartbeat,
) -> Result<(), SqlApiError> {
    let observed_clusters: BTreeMap<&str, bool> = heartbeat
        .observed_clusters
        .iter()
        .map(|observed| (observed.cluster_id.as_str(), observed.postgres_running))
        .collect();
    let observed_cluster_dirs: BTreeMap<&str, bool> = heartbeat
        .observed_clusters
        .iter()
        .map(|observed| (observed.data_dir.as_str(), observed.postgres_running))
        .collect();
    for cluster in store.ready_clusters_assigned_to_host(host_id).await? {
        let Some(assignment) = cluster.host_assignment.as_ref() else {
            continue;
        };
        let running = observed_clusters
            .get(cluster.cluster_id.as_str())
            .copied()
            .or_else(|| {
                observed_cluster_dirs
                    .get(assignment.data_dir.as_str())
                    .copied()
            })
            .unwrap_or(false);
        if running {
            continue;
        }
        if store
            .pending_or_running_agent_command_exists(host_id, &cluster.cluster_id, "start_postgres")
            .await?
        {
            continue;
        }
        enqueue_cluster_start_repair(store, host_id, &cluster, "node-agent").await?;
    }

    let observed_sync: BTreeMap<&str, bool> = heartbeat
        .observed_sync_deployments
        .iter()
        .map(|observed| (observed.deployment_id.as_str(), observed.running))
        .collect();
    for deployment in store
        .desired_sync_deployments_assigned_to_host(host_id)
        .await?
    {
        let running = observed_sync
            .get(deployment.deployment_id.as_str())
            .copied()
            .unwrap_or(false);
        if running {
            if deployment.lifecycle_state != SyncDeploymentLifecycleState::Running {
                store
                    .update_sync_deployment_lifecycle_state(
                        &deployment.deployment_id,
                        SyncDeploymentLifecycleState::Running,
                    )
                    .await?;
            }
            continue;
        }
        if store
            .pending_or_running_agent_command_exists(
                host_id,
                &deployment.managed_postgres_cluster_id,
                "start_sync_deployment",
            )
            .await?
        {
            continue;
        }
        store
            .update_sync_deployment_lifecycle_state(
                &deployment.deployment_id,
                SyncDeploymentLifecycleState::Starting,
            )
            .await?;
        let command = NodeAgentCommand {
            command_id: format!(
                "{}:repair-start-sync:{}",
                deployment.deployment_id,
                monotonic_nanos()
            ),
            cluster_id: deployment.managed_postgres_cluster_id.clone(),
            action: NodeAgentAction::StartSyncDeployment {
                deployment_id: deployment.deployment_id.clone(),
                config_version: deployment.config_version.clone(),
            },
        };
        let operation = operation_for_agent_command(
            OperationKind::StartSyncDeployment,
            &deployment.deployment_id,
            &command,
        );
        store.insert_operation(&operation).await?;
        store
            .enqueue_agent_command(host_id, Some(&operation.operation_id), &command)
            .await?;
        store
            .append_audit_event(&audit_event(
                "node-agent",
                "sync_deployment.repair_start_enqueued",
                &deployment.deployment_id,
            ))
            .await?;
    }
    Ok(())
}

async fn enqueue_cluster_start_repair(
    store: &SharedSqlControlPlaneStore,
    host_id: &str,
    cluster: &ManagedPostgresCluster,
    actor_id: &str,
) -> Result<(), SqlApiError> {
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:repair-start-postgres:{}",
            cluster.cluster_id,
            monotonic_nanos()
        ),
        cluster_id: cluster.cluster_id.clone(),
        action: NodeAgentAction::StartPostgres,
    };
    let operation =
        operation_for_agent_command(OperationKind::StartCluster, &cluster.cluster_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "managed_postgres_cluster.repair_start_enqueued",
            &cluster.cluster_id,
        ))
        .await?;
    Ok(())
}

async fn sql_api_enqueue_agent_command(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
    Json(payload): Json<EnqueueAgentCommandRequest>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let resource_id = payload.command.command_id.clone();
    store
        .enqueue_agent_command(&host_id, payload.operation_id.as_deref(), &payload.command)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "agent_command.enqueue",
            &resource_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("agent_command.enqueued")))
}

async fn sql_api_lease_next_agent_command(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host_id): Path<String>,
) -> Result<Json<Option<palimpsest_paas_core::QueuedNodeAgentCommand>>, SqlApiError> {
    require_agent_auth(&store, &headers, AgentAuthScope::new(&host_id, "lease")).await?;
    Ok(Json(store.lease_next_agent_command(&host_id).await?))
}

async fn sql_api_complete_agent_command(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((host_id, command_id)): Path<(String, String)>,
    Json(payload): Json<NodeAgentCommandResult>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let operation = format!("complete:{command_id}");
    require_agent_auth(&store, &headers, AgentAuthScope::new(&host_id, &operation)).await?;
    if payload.host_id != host_id || payload.command_id != command_id {
        return Err(SqlApiError::BadRequest(
            "command result path does not match payload".to_owned(),
        ));
    }
    if !store
        .agent_command_operation_token_is_valid(
            &host_id,
            &command_id,
            payload.operation_token.as_deref(),
        )
        .await?
    {
        return Err(SqlApiError::Unauthorized);
    }

    store
        .complete_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .promote_pending_database_role_credential_rotation_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
        )
        .await?;
    let advanced_cluster = store
        .advance_cluster_after_agent_command(&host_id, &command_id, payload.status)
        .await?;
    store
        .advance_sync_deployment_after_agent_command(&host_id, &command_id, payload.status)
        .await?;
    store
        .advance_operation_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .advance_major_upgrade_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .advance_backup_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    let artifacts = store
        .record_backup_artifacts_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            &payload.backup_artifacts,
        )
        .await?;
    for artifact in artifacts {
        store
            .append_audit_event(&audit_event(
                "node-agent",
                "managed_postgres_backup_artifact.auto_record",
                &artifact.artifact_id,
            ))
            .await?;
    }
    if let Some(status) = store
        .advance_backup_artifacts_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?
    {
        let action = match status {
            BackupArtifactStatus::Deleted => "managed_postgres_backup_artifact.deleted",
            BackupArtifactStatus::Failed => "managed_postgres_backup_artifact.failed",
            BackupArtifactStatus::Pending
            | BackupArtifactStatus::Available
            | BackupArtifactStatus::Expired => "managed_postgres_backup_artifact.updated",
        };
        store
            .append_audit_event(&audit_event("node-agent", action, &command_id))
            .await?;
    }
    store
        .advance_restore_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    if let Some(state) = store
        .advance_branch_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?
    {
        let action = match state {
            BranchLifecycleState::Ready => "managed_postgres_branch.ready",
            BranchLifecycleState::Deleted => "managed_postgres_branch.deleted",
            BranchLifecycleState::Failed => "managed_postgres_branch.failed",
            BranchLifecycleState::Creating | BranchLifecycleState::Deleting => {
                "managed_postgres_branch.updated"
            }
        };
        store
            .append_audit_event(&audit_event("node-agent", action, &command_id))
            .await?;
    }
    store
        .advance_restore_drill_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .advance_standby_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .advance_standby_check_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .advance_wal_archive_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    store
        .advance_failover_after_agent_command(
            &host_id,
            &command_id,
            payload.status,
            payload.detail.as_deref(),
        )
        .await?;
    if payload.status == AgentCommandStatus::Succeeded {
        if let Some((cluster_id, _state)) = advanced_cluster {
            reconcile_managed_postgres_cluster_passes(&store, "reconciler", &cluster_id, 2).await?;
        }
    }
    Ok(Json(ApiOk::new("agent_command.completed")))
}

async fn sql_api_upload_config(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<ConfigVersion>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&payload.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    let resource_id = payload.config_version.clone();
    store.upsert_config_version(&payload).await?;
    store
        .append_audit_event(&audit_event(auth.actor_id(), "config.upload", &resource_id))
        .await?;
    Ok(Json(ApiOk::new("config.uploaded")))
}

async fn sql_api_list_configs(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<ConfigVersionsQuery>,
) -> Result<Json<ConfigVersionsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&query.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    let configs = store
        .config_versions_for_environment(&query.environment_id)
        .await?;
    Ok(Json(ConfigVersionsResponse { configs }))
}

async fn sql_api_rollback_environment_config(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
    Json(payload): Json<RollbackConfigRequest>,
) -> Result<Json<RollbackConfigResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    let rollback_config_version = payload
        .config_version
        .unwrap_or_else(|| format!("rollback_{}", monotonic_nanos()));
    let config = store
        .rollback_environment_config(&environment_id, &rollback_config_version)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "config.rollback",
            &config.config_version,
        ))
        .await?;
    Ok(Json(RollbackConfigResponse { config }))
}

async fn sql_api_record_usage_event(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<UsageEvent>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::environment(
        &payload.organization_id,
        &payload.project_id,
        &payload.environment_id,
    ))?;
    verify_usage_event_signature_from_env(&payload)?;
    let resource_id = payload.event_id.clone();
    let inserted = store.append_usage_event(&payload).await?;
    if inserted {
        store
            .append_audit_event(&audit_event(
                auth.actor_id(),
                "usage_event.record",
                &resource_id,
            ))
            .await?;
    }
    Ok(Json(ApiOk::new("usage_event.recorded")))
}

async fn sql_api_export_usage_events(
    State(store): State<SharedSqlControlPlaneStore>,
    Query(query): Query<UsageEventsQuery>,
) -> Result<Json<UsageEventsResponse>, SqlApiError> {
    let events = store
        .usage_events(&sql_store::UsageEventFilter {
            organization_id: query.organization_id.as_deref(),
            project_id: query.project_id.as_deref(),
            environment_id: query.environment_id.as_deref(),
            metric: query.metric.as_deref(),
            occurred_at_from: query.occurred_at_from.as_deref(),
            occurred_at_to: query.occurred_at_to.as_deref(),
            limit: query.limit,
        })
        .await?;
    Ok(Json(UsageEventsResponse { events }))
}

async fn sql_api_upsert_quota_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<QuotaPolicy>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(
        &store,
        Some(&payload.organization_id),
        Some(&payload.project_id),
        Some(&payload.environment_id),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    let resource_id = payload.policy_id.clone();
    store.upsert_quota_policy(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "quota_policy.upsert",
            &resource_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("quota_policy.upserted")))
}

async fn sql_api_list_quota_policies(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<QuotaPoliciesQuery>,
) -> Result<Json<QuotaPoliciesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };
    let policies = store
        .quota_policies(&sql_store::QuotaPolicyFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            metric: query.metric.as_deref(),
        })
        .await?;
    Ok(Json(QuotaPoliciesResponse { policies }))
}

async fn sql_api_get_quota_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(policy_id): Path<String>,
) -> Result<Json<QuotaPolicy>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let policy = store
        .quota_policy(&policy_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(policy_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &policy.organization_id,
        &policy.project_id,
        &policy.environment_id,
    ))?;
    Ok(Json(policy))
}

async fn sql_api_upsert_quota_alert(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<QuotaAlertRequest>,
) -> Result<Json<QuotaAlert>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let policy = store
        .quota_policy(&payload.policy_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(payload.policy_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &policy.organization_id,
        &policy.project_id,
        &policy.environment_id,
    ))?;
    let alert = store
        .upsert_quota_alert(
            &payload.alert_id,
            &payload.policy_id,
            payload.threshold_basis_points,
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "quota_alert.upsert",
            &alert.alert_id,
        ))
        .await?;
    Ok(Json(alert))
}

async fn sql_api_list_quota_alerts(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<QuotaAlertsQuery>,
) -> Result<Json<QuotaAlertsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let alerts = store
        .quota_alerts(&sql_store::QuotaAlertFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            metric: query.metric.as_deref(),
            state: query.state,
        })
        .await?;
    Ok(Json(QuotaAlertsResponse { alerts }))
}

async fn sql_api_get_quota_alert(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(alert_id): Path<String>,
) -> Result<Json<QuotaAlert>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let alert = store
        .quota_alert(&alert_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(alert_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &alert.organization_id,
        &alert.project_id,
        &alert.environment_id,
    ))?;
    Ok(Json(alert))
}

async fn sql_api_evaluate_quota_alerts(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<QuotaAlertsQuery>,
) -> Result<Json<QuotaAlertsResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let alerts = store
        .evaluate_quota_alerts(&sql_store::QuotaAlertFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            metric: query.metric.as_deref(),
            state: query.state,
        })
        .await?;
    for alert in &alerts {
        if alert.state == QuotaAlertState::Firing {
            store
                .append_audit_event(&audit_event(
                    auth.actor_id(),
                    "quota_alert.fire",
                    &alert.alert_id,
                ))
                .await?;
        }
    }
    Ok(Json(QuotaAlertsResponse { alerts }))
}

async fn sql_api_create_billing_export(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<BillingExportRequest>,
) -> Result<Json<BillingExport>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(
        &store,
        payload.organization_id.as_deref(),
        payload.project_id.as_deref(),
        payload.environment_id.as_deref(),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    let export = store
        .create_billing_export(
            &payload.export_id,
            &payload.destination,
            &sql_store::UsageEventFilter {
                organization_id: payload.organization_id.as_deref(),
                project_id: payload.project_id.as_deref(),
                environment_id: payload.environment_id.as_deref(),
                metric: payload.metric.as_deref(),
                occurred_at_from: payload.occurred_at_from.as_deref(),
                occurred_at_to: payload.occurred_at_to.as_deref(),
                limit: payload.limit,
            },
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "billing_export.create",
            &export.export_id,
        ))
        .await?;
    Ok(Json(export))
}

async fn sql_api_list_billing_exports(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<BillingExportsQuery>,
) -> Result<Json<BillingExportsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };
    let exports = store
        .billing_exports(&sql_store::BillingExportFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            metric: query.metric.as_deref(),
            status: query.status,
            limit: query.limit,
        })
        .await?;
    Ok(Json(BillingExportsResponse { exports }))
}

async fn sql_api_get_billing_export(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(export_id): Path<String>,
) -> Result<Json<BillingExport>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let export = store
        .billing_export(&export_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(export_id.clone()))?;
    let scope = resolve_resource_scope(
        &store,
        export.organization_id.as_deref(),
        export.project_id.as_deref(),
        export.environment_id.as_deref(),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    Ok(Json(export))
}

async fn sql_api_metrics(
    State(store): State<SharedSqlControlPlaneStore>,
) -> Result<Response, SqlApiError> {
    let snapshot = store.metrics_snapshot().await?;
    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        render_control_plane_metrics(&snapshot),
    )
        .into_response())
}

async fn sql_api_create_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<ManagedPostgresCluster>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::environment(
        &payload.organization_id,
        &payload.project_id,
        &payload.environment_id,
    ))?;
    let resource_id = payload.cluster_id.clone();
    store.insert_managed_postgres_cluster(&payload).await?;
    store.initialize_managed_postgres_endpoint(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_cluster.create",
            &resource_id,
        ))
        .await?;
    reconcile_managed_postgres_cluster_passes(&store, auth.actor_id(), &resource_id, 2).await?;
    Ok(Json(ApiOk::new("managed_postgres_cluster.created")))
}

async fn sql_api_list_managed_postgres_clusters(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<ManagedPostgresClustersQuery>,
) -> Result<Json<ManagedPostgresClustersResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };

    let clusters = store
        .managed_postgres_clusters(&sql_store::ManagedPostgresClusterFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            lifecycle_state: query.lifecycle_state,
        })
        .await?;
    Ok(Json(ManagedPostgresClustersResponse { clusters }))
}

async fn sql_api_get_managed_postgres_endpoint(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
) -> Result<Json<ManagedPostgresEndpoint>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    Ok(Json(endpoint))
}

async fn sql_api_configure_managed_postgres_database_proxy_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
    Json(payload): Json<ConfigureManagedPostgresDatabaseProxyRouteRequest>,
) -> Result<Json<ConfigureManagedPostgresDatabaseProxyRouteResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let (endpoint, route) = store
        .configure_managed_postgres_database_proxy_route(&environment_id, &payload.listen_addr)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_endpoint.database_proxy_route.configure",
            &environment_id,
        ))
        .await?;
    Ok(Json(ConfigureManagedPostgresDatabaseProxyRouteResponse {
        endpoint,
        route,
    }))
}

async fn sql_api_deconfigure_managed_postgres_database_proxy_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
) -> Result<Json<DeconfigureManagedPostgresDatabaseProxyRouteResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let (endpoint, removed_listen_addr, revoked_certificate_ids) = store
        .deconfigure_managed_postgres_database_proxy_route(&environment_id)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_endpoint.database_proxy_route.deconfigure",
            &environment_id,
        ))
        .await?;
    Ok(Json(DeconfigureManagedPostgresDatabaseProxyRouteResponse {
        endpoint,
        removed_listen_addr,
        revoked_certificate_ids,
    }))
}

async fn sql_api_upsert_managed_postgres_certificate_authority_provider(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<UpsertManagedPostgresCertificateAuthorityProviderRequest>,
) -> Result<Json<ManagedPostgresCertificateAuthorityProvider>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let provider = ManagedPostgresCertificateAuthorityProvider {
        ca_provider_id: payload.ca_provider_id,
        name: payload.name,
        kind: payload.kind,
        issuer_ref: payload.issuer_ref,
        status: payload
            .status
            .unwrap_or(CertificateAuthorityProviderStatus::Active),
        default_for_managed_postgres: payload.default_for_managed_postgres.unwrap_or(false),
        updated_at: String::new(),
    };
    let provider = store
        .upsert_managed_postgres_certificate_authority_provider(&provider)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_certificate_authority_provider.upsert",
            &provider.ca_provider_id,
        ))
        .await?;
    Ok(Json(provider))
}

async fn sql_api_list_managed_postgres_certificate_authority_providers(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<ManagedPostgresCertificateAuthorityProvidersQuery>,
) -> Result<Json<ManagedPostgresCertificateAuthorityProvidersResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let providers = store
        .managed_postgres_certificate_authority_providers(
            &sql_store::ManagedPostgresCertificateAuthorityProviderFilter {
                status: query.status,
                default_for_managed_postgres: query.default_for_managed_postgres,
            },
        )
        .await?;
    Ok(Json(ManagedPostgresCertificateAuthorityProvidersResponse {
        providers,
    }))
}

async fn sql_api_get_managed_postgres_certificate_authority_provider(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(ca_provider_id): Path<String>,
) -> Result<Json<ManagedPostgresCertificateAuthorityProvider>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let provider = store
        .managed_postgres_certificate_authority_provider(&ca_provider_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(ca_provider_id.clone()))?;
    Ok(Json(provider))
}

async fn sql_api_issue_managed_postgres_endpoint_certificate(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
    Json(payload): Json<IssueManagedPostgresEndpointCertificateRequest>,
) -> Result<Json<ManagedPostgresEndpointCertificate>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let common_name = payload.common_name.unwrap_or_else(|| {
        endpoint
            .database_proxy_listen_addr
            .as_deref()
            .map_or(environment_id.as_str(), host_part)
            .to_owned()
    });
    if payload.certificate_pem.is_some() != payload.private_key_pem.is_some() {
        return Err(SqlApiError::BadRequest(
            "certificate_pem and private_key_pem must be provided together".to_owned(),
        ));
    }
    let certificate = store
        .issue_managed_postgres_endpoint_certificate(
            &environment_id,
            &common_name,
            payload.validity_days.unwrap_or(90),
            payload.ca_provider_id.as_deref(),
            payload
                .certificate_pem
                .as_deref()
                .zip(payload.private_key_pem.as_deref())
                .map(|(certificate_pem, private_key_pem)| {
                    sql_store::ImportedEndpointCertificateMaterial {
                        certificate_pem,
                        private_key_pem,
                    }
                }),
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_endpoint.certificate.issue",
            &certificate.certificate_id,
        ))
        .await?;
    Ok(Json(certificate))
}

async fn sql_api_renew_managed_postgres_endpoint_certificate(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
    Json(payload): Json<RenewManagedPostgresEndpointCertificateRequest>,
) -> Result<Json<ManagedPostgresEndpointCertificate>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if payload.certificate_pem.is_some() != payload.private_key_pem.is_some() {
        return Err(SqlApiError::BadRequest(
            "certificate_pem and private_key_pem must be provided together".to_owned(),
        ));
    }
    let certificate = store
        .renew_managed_postgres_endpoint_certificate(
            &environment_id,
            payload.renewal_window_days.unwrap_or(30),
            payload.force.unwrap_or(false),
            payload.validity_days.unwrap_or(90),
            payload.ca_provider_id.as_deref(),
            payload
                .certificate_pem
                .as_deref()
                .zip(payload.private_key_pem.as_deref())
                .map(|(certificate_pem, private_key_pem)| {
                    sql_store::ImportedEndpointCertificateMaterial {
                        certificate_pem,
                        private_key_pem,
                    }
                }),
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_endpoint.certificate.renew",
            &certificate.certificate_id,
        ))
        .await?;
    Ok(Json(certificate))
}

async fn sql_api_list_managed_postgres_endpoint_certificates(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
    Query(query): Query<ManagedPostgresEndpointCertificatesQuery>,
) -> Result<Json<ManagedPostgresEndpointCertificatesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let certificates = store
        .managed_postgres_endpoint_certificates(
            &sql_store::ManagedPostgresEndpointCertificateFilter {
                environment_id: Some(&environment_id),
                status: query.status,
            },
        )
        .await?;
    Ok(Json(ManagedPostgresEndpointCertificatesResponse {
        certificates,
    }))
}

async fn sql_api_get_managed_postgres_endpoint_certificate(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((environment_id, certificate_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresEndpointCertificate>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let certificate = store
        .managed_postgres_endpoint_certificate(&certificate_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(certificate_id.clone()))?;
    if certificate.environment_id != environment_id {
        return Err(SqlApiError::MissingResource(certificate_id));
    }
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    Ok(Json(certificate))
}

async fn sql_api_get_managed_postgres_endpoint_certificate_bundle(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((environment_id, certificate_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresEndpointCertificateBundle>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let bundle = store
        .managed_postgres_endpoint_certificate_bundle(&certificate_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(certificate_id.clone()))?;
    if bundle.certificate.environment_id != environment_id {
        return Err(SqlApiError::MissingResource(certificate_id));
    }
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    Ok(Json(bundle))
}

async fn sql_api_get_managed_postgres_acme_order(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((environment_id, certificate_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresAcmeOrder>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let certificate = store
        .managed_postgres_endpoint_certificate(&certificate_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(certificate_id.clone()))?;
    if certificate.environment_id != environment_id {
        return Err(SqlApiError::MissingResource(certificate_id));
    }
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let order = store
        .managed_postgres_acme_order_for_certificate(&certificate_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(format!("acme order for {certificate_id}")))?;
    Ok(Json(order))
}

async fn sql_api_acme_http_01_challenge(
    State(store): State<SharedSqlControlPlaneStore>,
    Path(token): Path<String>,
) -> Result<Response, SqlApiError> {
    let key_authorization = store
        .managed_postgres_acme_http_01_key_authorization(&token)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(format!("ACME HTTP-01 challenge {token}")))?;
    Ok((
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        key_authorization,
    )
        .into_response())
}

async fn sql_api_validate_managed_postgres_acme_challenge(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((environment_id, certificate_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresAcmeOrder>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let order = store
        .validate_managed_postgres_acme_http_01_challenge(&environment_id, &certificate_id)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_endpoint.certificate.acme_challenge.validate",
            &certificate_id,
        ))
        .await?;
    Ok(Json(order))
}

async fn sql_api_finalize_managed_postgres_acme_order(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((environment_id, certificate_id)): Path<(String, String)>,
    Json(payload): Json<FinalizeManagedPostgresAcmeOrderRequest>,
) -> Result<Json<ManagedPostgresEndpointCertificate>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let endpoint = store
        .managed_postgres_endpoint(&environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(environment_id.clone()))?;
    let cluster = store
        .managed_postgres_cluster(&endpoint.active_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint.active_cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let certificate = store
        .finalize_managed_postgres_acme_order(
            &environment_id,
            &certificate_id,
            &payload.certificate_pem,
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_endpoint.certificate.acme_finalize",
            &certificate.certificate_id,
        ))
        .await?;
    Ok(Json(certificate))
}

async fn sql_api_list_managed_postgres_deletion_tombstones(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<ManagedPostgresDeletionTombstonesQuery>,
) -> Result<Json<ManagedPostgresDeletionTombstonesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };

    let tombstones = store
        .managed_postgres_deletion_tombstones(&sql_store::ManagedPostgresDeletionTombstoneFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            expired: query.expired,
        })
        .await?;
    Ok(Json(ManagedPostgresDeletionTombstonesResponse {
        tombstones,
    }))
}

async fn sql_api_get_managed_postgres_deletion_tombstone(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<ManagedPostgresDeletionTombstone>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let tombstone = store
        .managed_postgres_deletion_tombstone(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_deletion_tombstone(&tombstone))?;
    Ok(Json(tombstone))
}

async fn sql_api_expire_managed_postgres_deletion_tombstones(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
) -> Result<Json<ExpireManagedPostgresDeletionTombstonesResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let tombstones = store.expire_managed_postgres_deletion_tombstones().await?;
    let mut cleanup_commands = Vec::new();
    let mut cleanup_operations = Vec::new();
    for tombstone in &tombstones {
        if let Some(backup_id) = tombstone.retained_backup_id.as_deref() {
            if let Some((command, operation)) =
                queue_managed_postgres_backup_cleanup(&store, tombstone, backup_id).await?
            {
                cleanup_commands.push(command);
                cleanup_operations.push(operation);
            }
        }
        store
            .append_audit_event(&audit_event(
                auth.actor_id(),
                "managed_postgres_deletion_tombstone.expire",
                &tombstone.cluster_id,
            ))
            .await?;
    }
    Ok(Json(ExpireManagedPostgresDeletionTombstonesResponse {
        tombstones,
        cleanup_commands,
        cleanup_operations,
    }))
}

async fn sql_api_get_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<ManagedPostgresCluster>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    Ok(Json(cluster))
}

async fn sql_api_get_managed_postgres_schema(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresDatabaseQuery>,
) -> Result<Json<ManagedPostgresSchemaResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if let Some(assignment) = cluster.host_assignment.as_ref() {
        if store
            .observed_cluster_running(
                &assignment.host_id,
                &cluster.cluster_id,
                &assignment.data_dir,
            )
            .await?
            == Some(false)
        {
            if !store
                .pending_or_running_agent_command_exists(
                    &assignment.host_id,
                    &cluster.cluster_id,
                    "start_postgres",
                )
                .await?
            {
                enqueue_cluster_start_repair(
                    &store,
                    &assignment.host_id,
                    &cluster,
                    auth.actor_id(),
                )
                .await?;
            }
            return Err(SqlApiError::BadRequest(format!(
                "managed Postgres cluster {} is ready in control-plane state but not observed running; start repair was enqueued",
                cluster.cluster_id
            )));
        }
    }
    let endpoint = endpoint_for_database(
        store.managed_postgres_app_endpoint(&cluster).await?,
        query.database.as_deref(),
    )?;
    let schema = match read_managed_postgres_schema(&endpoint).await {
        Ok(schema) => schema,
        Err(err) => {
            if let Some(assignment) = cluster.host_assignment.as_ref() {
                if !store
                    .pending_or_running_agent_command_exists(
                        &assignment.host_id,
                        &cluster.cluster_id,
                        "start_postgres",
                    )
                    .await?
                {
                    enqueue_cluster_start_repair(
                        &store,
                        &assignment.host_id,
                        &cluster,
                        auth.actor_id(),
                    )
                    .await?;
                }
            }
            return Err(err);
        }
    };
    Ok(Json(schema))
}

async fn sql_api_query_managed_postgres_console(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<ManagedPostgresSqlQueryRequest>,
) -> Result<Json<ManagedPostgresSqlQueryResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    validate_managed_postgres_read_only_sql(&payload.sql)?;
    let endpoint = endpoint_for_database(
        store.managed_postgres_app_endpoint(&cluster).await?,
        payload.database.as_deref(),
    )?;
    let result = run_managed_postgres_read_only_query(&endpoint, &payload).await?;
    Ok(Json(result))
}

async fn sql_api_seed_managed_postgres_sample_data(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<ManagedPostgresSampleDataSeedResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let endpoint = store.managed_postgres_migration_endpoint(&cluster).await?;
    let app_role =
        sql_store::SqlControlPlaneStore::managed_postgres_role_name(&cluster.cluster_id, "app");
    let response = seed_managed_postgres_sample_data(&endpoint, &app_role).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_sample_data.seed",
            &cluster.cluster_id,
        ))
        .await?;
    Ok(Json(response))
}

async fn sql_api_inspect_managed_postgres_query(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<ManagedPostgresSqlQueryRequest>,
) -> Result<Json<ManagedPostgresQueryExplorerResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    validate_managed_postgres_read_only_sql(&payload.sql)?;
    let endpoint = endpoint_for_database(
        store.managed_postgres_app_endpoint(&cluster).await?,
        payload.database.as_deref(),
    )?;
    Ok(Json(
        inspect_managed_postgres_query(&endpoint, &payload).await?,
    ))
}

async fn sql_api_probe_managed_postgres_runtime(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<ManagedPostgresRuntimeCheck>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let endpoint = store.managed_postgres_support_endpoint(&cluster).await?;
    let check = probe_managed_postgres_runtime(&endpoint, &cluster.cluster_id).await;
    let check = store.insert_managed_postgres_runtime_check(&check).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_runtime_check.probe",
            &check.check_id,
        ))
        .await?;
    Ok(Json(check))
}

async fn sql_api_list_managed_postgres_runtime_checks(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresRuntimeChecksQuery>,
) -> Result<Json<ManagedPostgresRuntimeChecksResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let checks = store
        .managed_postgres_runtime_checks(&sql_store::ManagedPostgresRuntimeCheckFilter {
            cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresRuntimeChecksResponse { checks }))
}

async fn sql_api_get_managed_postgres_runtime_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, check_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresRuntimeCheck>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let check = store
        .managed_postgres_runtime_check(&check_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(check_id.clone()))?;
    if check.cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(check_id));
    }
    Ok(Json(check))
}

async fn sql_api_upsert_query_permission_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<QueryPermissionPolicy>,
) -> Result<Json<QueryPermissionPolicy>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(
        &store,
        Some(&payload.organization_id),
        Some(&payload.project_id),
        Some(&payload.environment_id),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    validate_query_permission_policy(&payload)?;
    let policy = store.upsert_query_permission_policy(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "query_permission_policy.upsert",
            &policy.policy_id,
        ))
        .await?;
    Ok(Json(policy))
}

async fn sql_api_list_query_permission_policies(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<QueryPermissionPoliciesQuery>,
) -> Result<Json<QueryPermissionPoliciesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let policies = store
        .query_permission_policies(&sql_store::QueryPermissionPolicyFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            table_schema: query.table_schema.as_deref(),
            table_name: query.table_name.as_deref(),
            operation: query.operation,
            status: query.status,
        })
        .await?;
    Ok(Json(QueryPermissionPoliciesResponse { policies }))
}

async fn sql_api_get_query_permission_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(policy_id): Path<String>,
) -> Result<Json<QueryPermissionPolicy>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let policy = store
        .query_permission_policy(&policy_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(policy_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &policy.organization_id,
        &policy.project_id,
        &policy.environment_id,
    ))?;
    Ok(Json(policy))
}

async fn sql_api_dry_run_query_permission_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(policy_id): Path<String>,
    Json(payload): Json<QueryPermissionPolicyDryRunRequest>,
) -> Result<Json<QueryPermissionPolicyDryRunResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let policy = store
        .query_permission_policy(&policy_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(policy_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &policy.organization_id,
        &policy.project_id,
        &policy.environment_id,
    ))?;
    validate_query_permission_policy(&policy)?;
    let sample_context = payload
        .sample_context
        .unwrap_or_else(|| policy.sample_context.clone());
    let accepted = sample_context.is_object();
    Ok(Json(QueryPermissionPolicyDryRunResponse {
        policy_id: policy.policy_id,
        accepted,
        table_schema: policy.table_schema,
        table_name: policy.table_name,
        operation: policy.operation,
        checked_predicate_sql: canonicalize_sql_fragment(&policy.predicate_sql),
        sample_context,
        decision_detail: if accepted {
            "sample context is an object and predicate passed static validation".to_owned()
        } else {
            "sample context must be a JSON object".to_owned()
        },
    }))
}

async fn sql_api_get_permission_rule_document(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
) -> Result<Json<PermissionRuleDocument>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    // Absent document reads back as an empty draft so the editor always has
    // something to load for a valid environment.
    let document = store
        .permission_rule_document(&environment_id)
        .await?
        .unwrap_or_else(|| PermissionRuleDocument {
            environment_id: environment_id.clone(),
            organization_id: scope.organization_id.clone().unwrap_or_default(),
            project_id: scope.project_id.clone().unwrap_or_default(),
            dsl: String::new(),
            updated_at: None,
        });
    Ok(Json(document))
}

async fn sql_api_put_permission_rule_document(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(environment_id): Path<String>,
    Json(payload): Json<PermissionRuleDocumentSaveRequest>,
) -> Result<Json<PermissionRuleDocument>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    let document = PermissionRuleDocument {
        environment_id: environment_id.clone(),
        organization_id: scope.organization_id.clone().unwrap_or_default(),
        project_id: scope.project_id.clone().unwrap_or_default(),
        dsl: payload.dsl,
        updated_at: None,
    };
    let saved = store.upsert_permission_rule_document(&document).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "permission_rule_document.save",
            &environment_id,
        ))
        .await?;
    Ok(Json(saved))
}

async fn sql_api_verify_permissions(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<PermissionVerifyRequest>,
) -> Result<Json<PermissionVerifyResponse>, SqlApiError> {
    // Verification is a pure compile against a supplied catalog; it touches
    // no environment-scoped state, so it only requires a valid read actor.
    let _auth = sql_read_auth_context(&store, &headers).await?;
    Ok(Json(verify_permissions_dsl(&payload)))
}

async fn sql_api_list_managed_postgres_operations(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresOperationsQuery>,
) -> Result<Json<ManagedPostgresOperationsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let operations = store
        .operations(&sql_store::OperationFilter {
            cluster_id: &cluster_id,
            kind: query.kind,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresOperationsResponse { operations }))
}

async fn sql_api_get_managed_postgres_operation(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, operation_id)): Path<(String, String)>,
) -> Result<Json<OperationRecord>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let operation = store
        .operation(&cluster_id, &operation_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(operation_id.clone()))?;
    Ok(Json(operation))
}

async fn sql_api_list_managed_postgres_agent_commands(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresAgentCommandsQuery>,
) -> Result<Json<ManagedPostgresAgentCommandsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let commands = store
        .agent_commands(&sql_store::AgentCommandFilter {
            cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresAgentCommandsResponse { commands }))
}

async fn sql_api_get_managed_postgres_agent_command(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, command_id)): Path<(String, String)>,
) -> Result<Json<QueuedNodeAgentCommand>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let command = store
        .agent_command(&cluster_id, &command_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(command_id.clone()))?;
    Ok(Json(command))
}

async fn sql_api_create_sync_deployment(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<SyncDeployment>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&payload.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    let resource_id = payload.deployment_id.clone();
    store.insert_sync_deployment(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "sync_deployment.create",
            &resource_id,
        ))
        .await?;
    enqueue_sync_deployment_start_if_ready(&store, auth.actor_id(), &payload).await?;
    Ok(Json(ApiOk::new("sync_deployment.created")))
}

async fn enqueue_sync_deployment_start_if_ready(
    store: &SharedSqlControlPlaneStore,
    actor_id: &str,
    deployment: &SyncDeployment,
) -> Result<(), SqlApiError> {
    if !matches!(
        deployment.lifecycle_state,
        SyncDeploymentLifecycleState::Requested | SyncDeploymentLifecycleState::Starting
    ) {
        return Ok(());
    }
    let cluster = store
        .managed_postgres_cluster(&deployment.managed_postgres_cluster_id)
        .await?
        .ok_or_else(|| {
            SqlApiError::MissingResource(deployment.managed_postgres_cluster_id.clone())
        })?;
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Ok(());
    }
    let Some(assignment) = cluster.host_assignment.as_ref() else {
        return Ok(());
    };
    if store
        .pending_or_running_agent_command_exists(
            &assignment.host_id,
            &cluster.cluster_id,
            "start_sync_deployment",
        )
        .await?
    {
        return Ok(());
    }
    store
        .update_sync_deployment_lifecycle_state(
            &deployment.deployment_id,
            SyncDeploymentLifecycleState::Starting,
        )
        .await?;
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:start-sync:{}",
            deployment.deployment_id,
            monotonic_nanos()
        ),
        cluster_id: cluster.cluster_id.clone(),
        action: NodeAgentAction::StartSyncDeployment {
            deployment_id: deployment.deployment_id.clone(),
            config_version: deployment.config_version.clone(),
        },
    };
    let operation = operation_for_agent_command(
        OperationKind::StartSyncDeployment,
        &deployment.deployment_id,
        &command,
    );
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "sync_deployment.start_enqueued",
            &deployment.deployment_id,
        ))
        .await?;
    Ok(())
}

async fn sql_api_list_sync_deployments(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<SyncDeploymentsQuery>,
) -> Result<Json<SyncDeploymentsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };

    let deployments = store
        .sync_deployments(&sql_store::SyncDeploymentFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            lifecycle_state: query.lifecycle_state,
        })
        .await?;
    Ok(Json(SyncDeploymentsResponse { deployments }))
}

async fn sql_api_get_sync_deployment(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(deployment_id): Path<String>,
) -> Result<Json<SyncDeployment>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let deployment = store
        .sync_deployment(&deployment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(deployment_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &deployment.organization_id,
        &deployment.project_id,
        &deployment.environment_id,
    ))?;
    Ok(Json(deployment))
}

async fn sql_api_upsert_gateway_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<GatewayRoute>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&payload.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    ensure_scope_component_matches(
        "organization_id",
        Some(&payload.organization_id),
        scope
            .organization_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    ensure_scope_component_matches(
        "project_id",
        Some(&payload.project_id),
        scope
            .project_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    store.upsert_gateway_route(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "gateway_route.upsert",
            &payload.host,
        ))
        .await?;
    Ok(Json(ApiOk::new("gateway_route.upserted")))
}

async fn sql_api_list_gateway_routes(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<GatewayRoutesQuery>,
) -> Result<Json<GatewayRoutesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };

    let routes = store
        .gateway_routes(&sql_store::GatewayRouteFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
        })
        .await?;
    Ok(Json(GatewayRoutesResponse { routes }))
}

async fn sql_api_get_gateway_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host): Path<String>,
) -> Result<Json<GatewayRoute>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let route = store
        .gateway_route(&host)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(host.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &route.organization_id,
        &route.project_id,
        &route.environment_id,
    ))?;
    Ok(Json(route))
}

async fn sql_api_get_gateway_route_mtls_bundle(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host): Path<String>,
) -> Result<Json<GatewayRouteMtlsBundle>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let bundle = store
        .gateway_route_mtls_bundle(&host)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(host.clone()))?;
    let scope = resolve_resource_scope(&store, None, None, Some(&bundle.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    Ok(Json(bundle))
}

async fn sql_api_delete_gateway_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(host): Path<String>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let route = store
        .gateway_route(&host)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(host.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &route.organization_id,
        &route.project_id,
        &route.environment_id,
    ))?;
    store.delete_gateway_route(&host).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "gateway_route.delete",
            &route.host,
        ))
        .await?;
    Ok(Json(ApiOk::new("gateway_route.deleted")))
}

async fn sql_api_upsert_domain(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<Domain>,
) -> Result<Json<Domain>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let route = store
        .gateway_route(&payload.route_host)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(payload.route_host.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &route.organization_id,
        &route.project_id,
        &route.environment_id,
    ))?;
    ensure_scope_component_matches(
        "organization_id",
        Some(&payload.organization_id),
        &route.organization_id,
    )?;
    ensure_scope_component_matches("project_id", Some(&payload.project_id), &route.project_id)?;
    ensure_scope_component_matches(
        "environment_id",
        Some(&payload.environment_id),
        &route.environment_id,
    )?;
    validate_domain(&payload)?;
    let domain = store.upsert_domain(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "domain.upsert",
            &domain.hostname,
        ))
        .await?;
    Ok(Json(domain))
}

async fn sql_api_list_domains(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<DomainsQuery>,
) -> Result<Json<DomainsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let domains = store
        .domains(&sql_store::DomainFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            verification_status: query.verification_status,
            tls_status: query.tls_status,
        })
        .await?;
    Ok(Json(DomainsResponse { domains }))
}

async fn sql_api_get_domain(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(hostname): Path<String>,
) -> Result<Json<Domain>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let domain = store
        .domain(&hostname)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(hostname.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &domain.organization_id,
        &domain.project_id,
        &domain.environment_id,
    ))?;
    Ok(Json(domain))
}

async fn sql_api_delete_domain(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(hostname): Path<String>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let domain = store
        .domain(&hostname)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(hostname.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &domain.organization_id,
        &domain.project_id,
        &domain.environment_id,
    ))?;
    store.delete_domain(&hostname).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "domain.delete",
            &domain.hostname,
        ))
        .await?;
    Ok(Json(ApiOk::new("domain.deleted")))
}

async fn sql_api_upsert_ip_allowlist_rule(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<IpAllowlistRule>,
) -> Result<Json<IpAllowlistRule>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&payload.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    ensure_scope_component_matches(
        "organization_id",
        Some(&payload.organization_id),
        scope
            .organization_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    ensure_scope_component_matches(
        "project_id",
        Some(&payload.project_id),
        scope
            .project_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    validate_ip_allowlist_rule(&payload)?;
    let rule = store.upsert_ip_allowlist_rule(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "ip_allowlist_rule.upsert",
            &rule.rule_id,
        ))
        .await?;
    Ok(Json(rule))
}

async fn sql_api_list_ip_allowlist_rules(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<IpAllowlistRulesQuery>,
) -> Result<Json<IpAllowlistRulesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let rules = store
        .ip_allowlist_rules(&sql_store::IpAllowlistRuleFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            purpose: query.purpose,
            status: query.status,
        })
        .await?;
    Ok(Json(IpAllowlistRulesResponse { rules }))
}

async fn sql_api_get_ip_allowlist_rule(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(rule_id): Path<String>,
) -> Result<Json<IpAllowlistRule>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let rule = store
        .ip_allowlist_rule(&rule_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(rule_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &rule.organization_id,
        &rule.project_id,
        &rule.environment_id,
    ))?;
    Ok(Json(rule))
}

async fn sql_api_delete_ip_allowlist_rule(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(rule_id): Path<String>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let rule = store
        .ip_allowlist_rule(&rule_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(rule_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &rule.organization_id,
        &rule.project_id,
        &rule.environment_id,
    ))?;
    store.delete_ip_allowlist_rule(&rule_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "ip_allowlist_rule.delete",
            &rule.rule_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("ip_allowlist_rule.deleted")))
}

async fn sql_api_upsert_static_egress_ip(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<StaticEgressIp>,
) -> Result<Json<StaticEgressIp>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&payload.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    ensure_scope_component_matches(
        "organization_id",
        Some(&payload.organization_id),
        scope
            .organization_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    ensure_scope_component_matches(
        "project_id",
        Some(&payload.project_id),
        scope
            .project_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    validate_static_egress_ip(&payload)?;
    let egress_ip = store.upsert_static_egress_ip(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "static_egress_ip.upsert",
            &egress_ip.egress_ip_id,
        ))
        .await?;
    Ok(Json(egress_ip))
}

async fn sql_api_list_static_egress_ips(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<StaticEgressIpsQuery>,
) -> Result<Json<StaticEgressIpsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let egress_ips = store
        .static_egress_ips(&sql_store::StaticEgressIpFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            region: query.region.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(StaticEgressIpsResponse { egress_ips }))
}

async fn sql_api_get_static_egress_ip(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(egress_ip_id): Path<String>,
) -> Result<Json<StaticEgressIp>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let egress_ip = store
        .static_egress_ip(&egress_ip_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(egress_ip_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &egress_ip.organization_id,
        &egress_ip.project_id,
        &egress_ip.environment_id,
    ))?;
    Ok(Json(egress_ip))
}

async fn sql_api_delete_static_egress_ip(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(egress_ip_id): Path<String>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let egress_ip = store
        .static_egress_ip(&egress_ip_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(egress_ip_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &egress_ip.organization_id,
        &egress_ip.project_id,
        &egress_ip.environment_id,
    ))?;
    store.delete_static_egress_ip(&egress_ip_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "static_egress_ip.delete",
            &egress_ip.egress_ip_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("static_egress_ip.deleted")))
}

async fn sql_api_upsert_maintenance_window(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<MaintenanceWindow>,
) -> Result<Json<MaintenanceWindow>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let scope = resolve_resource_scope(&store, None, None, Some(&payload.environment_id)).await?;
    auth.require_scope(scope.as_resource_scope())?;
    ensure_scope_component_matches(
        "organization_id",
        Some(&payload.organization_id),
        scope
            .organization_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    ensure_scope_component_matches(
        "project_id",
        Some(&payload.project_id),
        scope
            .project_id
            .as_deref()
            .ok_or_else(|| SqlApiError::MissingResource(payload.environment_id.clone()))?,
    )?;
    validate_maintenance_window(&payload)?;
    let window = store.upsert_maintenance_window(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "maintenance_window.upsert",
            &window.window_id,
        ))
        .await?;
    Ok(Json(window))
}

async fn sql_api_list_maintenance_windows(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<MaintenanceWindowsQuery>,
) -> Result<Json<MaintenanceWindowsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let windows = store
        .maintenance_windows(&sql_store::MaintenanceWindowFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(MaintenanceWindowsResponse { windows }))
}

async fn sql_api_get_maintenance_window(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(window_id): Path<String>,
) -> Result<Json<MaintenanceWindow>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let window = store
        .maintenance_window(&window_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(window_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &window.organization_id,
        &window.project_id,
        &window.environment_id,
    ))?;
    Ok(Json(window))
}

async fn sql_api_delete_maintenance_window(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(window_id): Path<String>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let window = store
        .maintenance_window(&window_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(window_id.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &window.organization_id,
        &window.project_id,
        &window.environment_id,
    ))?;
    store.delete_maintenance_window(&window_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "maintenance_window.delete",
            &window.window_id,
        ))
        .await?;
    Ok(Json(ApiOk::new("maintenance_window.deleted")))
}

async fn sql_api_upsert_database_proxy_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<DatabaseProxyRoute>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&payload.cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(payload.cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    ensure_scope_component_matches(
        "organization_id",
        Some(&payload.organization_id),
        &cluster.organization_id,
    )?;
    ensure_scope_component_matches("project_id", Some(&payload.project_id), &cluster.project_id)?;
    ensure_scope_component_matches(
        "environment_id",
        Some(&payload.environment_id),
        &cluster.environment_id,
    )?;
    store.upsert_database_proxy_route(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "database_proxy_route.upsert",
            &payload.listen_addr,
        ))
        .await?;
    Ok(Json(ApiOk::new("database_proxy_route.upserted")))
}

async fn sql_api_list_database_proxy_routes(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<DatabaseProxyRoutesQuery>,
) -> Result<Json<DatabaseProxyRoutesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };

    let routes = store
        .database_proxy_routes(&sql_store::DatabaseProxyRouteFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
        })
        .await?;
    Ok(Json(DatabaseProxyRoutesResponse { routes }))
}

async fn sql_api_get_database_proxy_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(listen_addr): Path<String>,
) -> Result<Json<DatabaseProxyRoute>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let route = store
        .database_proxy_route(&listen_addr)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(listen_addr.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &route.organization_id,
        &route.project_id,
        &route.environment_id,
    ))?;
    Ok(Json(route))
}

async fn sql_api_delete_database_proxy_route(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(listen_addr): Path<String>,
) -> Result<Json<ApiOk>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let route = store
        .database_proxy_route(&listen_addr)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(listen_addr.clone()))?;
    auth.require_scope(ResourceScope::environment(
        &route.organization_id,
        &route.project_id,
        &route.environment_id,
    ))?;
    store.delete_database_proxy_route(&listen_addr).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "database_proxy_route.delete",
            &route.listen_addr,
        ))
        .await?;
    Ok(Json(ApiOk::new("database_proxy_route.deleted")))
}

async fn sql_api_reconcile_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<ReconcileApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let response =
        reconcile_managed_postgres_cluster_once(&store, auth.actor_id(), &cluster_id).await?;
    Ok(Json(response))
}

async fn reconcile_managed_postgres_cluster_passes(
    store: &SharedSqlControlPlaneStore,
    actor_id: &str,
    cluster_id: &str,
    max_passes: usize,
) -> Result<Option<ReconcileApiResponse>, SqlApiError> {
    let mut last_response = None;
    for _ in 0..max_passes {
        let response = reconcile_managed_postgres_cluster_once(store, actor_id, cluster_id).await?;
        // Keep iterating while the cluster is still converging toward Ready; a
        // pass that applied manifests advances Verifying, and the next pass can
        // promote it to Ready once the operator reports ready instances.
        let should_continue = matches!(
            response.cluster.lifecycle_state,
            ClusterLifecycleState::Verifying
        );
        last_response = Some(response);
        if !should_continue {
            break;
        }
    }
    Ok(last_response)
}

/// Render the cluster's CloudNativePG manifests, apply (or delete) them, and
/// advance its lifecycle state. This replaces the former host placement +
/// node-agent command enqueue path.
async fn reconcile_managed_postgres_cluster_once(
    store: &SharedSqlControlPlaneStore,
    actor_id: &str,
    cluster_id: &str,
) -> Result<ReconcileApiResponse, SqlApiError> {
    let cluster = store
        .managed_postgres_cluster(cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.to_owned()))?;

    let runtime = runtime::ClusterRuntime::from_env();
    let roles = store.issue_database_role_credentials(cluster_id).await?;
    let plan = Reconciler::new()
        .reconcile(&cluster, None, &roles, &RuntimeConfig::from_env())
        .map_err(SqlApiError::from)?;

    let manifests =
        match (plan.action, &plan.manifests) {
            (RuntimeAction::Apply, Some(manifests)) => {
                runtime.apply(manifests).map_err(runtime_error)?;
                Some(manifests.to_yaml().map_err(|err| {
                    SqlApiError::from(ControlPlaneError::Runtime(err.to_string()))
                })?)
            }
            (RuntimeAction::Delete, Some(manifests)) => {
                runtime.delete(manifests).map_err(runtime_error)?;
                None
            }
            _ => None,
        };

    let mut next_cluster = plan.next_cluster;
    // Promote Verifying -> Ready once CloudNativePG reports a ready instance.
    if next_cluster.lifecycle_state == ClusterLifecycleState::Verifying
        && runtime.cluster_ready(&cluster).map_err(runtime_error)?
    {
        next_cluster.lifecycle_state = ClusterLifecycleState::Ready;
    }

    store.update_managed_postgres_cluster(&next_cluster).await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "managed_postgres_cluster.reconcile",
            cluster_id,
        ))
        .await?;

    Ok(ReconcileApiResponse {
        cluster: next_cluster,
        manifests,
    })
}

fn runtime_error(err: runtime::RuntimeAdapterError) -> SqlApiError {
    SqlApiError::from(ControlPlaneError::Runtime(err.0))
}

async fn sql_api_stop_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<StopClusterApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let mut cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;

    if !matches!(
        cluster.lifecycle_state,
        ClusterLifecycleState::Ready | ClusterLifecycleState::Failed
    ) {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be stopped from {:?}",
            cluster.lifecycle_state
        )));
    }

    // Pause = CloudNativePG hibernation, applied via the reconcile path (which
    // re-renders the cluster with the hibernation annotation from the Stopped
    // state).
    cluster.lifecycle_state = ClusterLifecycleState::Stopping;
    store.update_managed_postgres_cluster(&cluster).await?;
    let response =
        reconcile_managed_postgres_cluster_once(&store, auth.actor_id(), &cluster_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_cluster.stop",
            &cluster_id,
        ))
        .await?;

    Ok(Json(StopClusterApiResponse {
        cluster: response.cluster,
    }))
}

async fn sql_api_resume_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<StartClusterApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let mut cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;

    if cluster.lifecycle_state != ClusterLifecycleState::Stopped {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be resumed from {:?}",
            cluster.lifecycle_state
        )));
    }

    // Resume by re-rendering without the hibernation annotation and applying;
    // readiness then promotes the cluster back to Ready.
    cluster.lifecycle_state = ClusterLifecycleState::Starting;
    store.update_managed_postgres_cluster(&cluster).await?;
    let response =
        reconcile_managed_postgres_cluster_once(&store, auth.actor_id(), &cluster_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_cluster.resume",
            &cluster_id,
        ))
        .await?;

    Ok(Json(StartClusterApiResponse {
        cluster: response.cluster,
    }))
}

async fn sql_api_resize_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<ResizeClusterRequest>,
) -> Result<Json<ResizeClusterApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let mut cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;

    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be resized from {:?}",
            cluster.lifecycle_state
        )));
    }
    if payload.storage_gib <= cluster.storage_gib {
        return Err(SqlApiError::BadRequest(format!(
            "resize must increase storage from {} GiB",
            cluster.storage_gib
        )));
    }

    // CloudNativePG expands the PersistentVolumeClaims when the cluster's
    // storage size grows; re-render with the new size and apply via reconcile.
    cluster.storage_gib = payload.storage_gib;
    cluster.lifecycle_state = ClusterLifecycleState::Resizing;
    store.update_managed_postgres_cluster(&cluster).await?;
    let response =
        reconcile_managed_postgres_cluster_once(&store, auth.actor_id(), &cluster_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_cluster.resize",
            &cluster_id,
        ))
        .await?;

    Ok(Json(ResizeClusterApiResponse {
        cluster: response.cluster,
    }))
}

async fn sql_api_rotate_managed_postgres_role_credentials(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<RotateDatabaseRoleCredentialsResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let mut cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "database role credentials cannot be rotated from {:?}",
            cluster.lifecycle_state
        )));
    }
    let assignment = cluster
        .host_assignment
        .clone()
        .ok_or_else(|| SqlApiError::BadRequest("cluster has no host assignment".to_owned()))?;
    let rotation = store
        .rotate_database_role_credentials(&cluster.cluster_id)
        .await?;
    cluster.lifecycle_state = ClusterLifecycleState::ConfiguringReplication;
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:rotate-database-role-credentials:{}",
            cluster.cluster_id, rotation.rotation_id
        ),
        cluster_id: cluster.cluster_id.clone(),
        action: NodeAgentAction::ConfigurePostgresAccess {
            data_dir: assignment.data_dir.clone(),
            port: assignment.port,
            database: "postgres".to_owned(),
            roles: rotation.credentials,
            publication: "palimpsest_publication".to_owned(),
            replication_slot: format!(
                "{}_palimpsest_slot",
                sanitize_identifier_component(&cluster.cluster_id)
            ),
        },
    };
    store.update_managed_postgres_cluster(&cluster).await?;
    let operation =
        operation_for_agent_command(OperationKind::RotateCredentials, &cluster_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .attach_database_role_credential_rotation_command(
            &rotation.rotation_id,
            &command.command_id,
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_cluster.roles.rotate",
            &cluster_id,
        ))
        .await?;
    Ok(Json(RotateDatabaseRoleCredentialsResponse {
        cluster,
        command_id: command.command_id,
        operation,
    }))
}

async fn sql_api_update_managed_postgres_minor(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<UpdatePostgresMinorRequest>,
) -> Result<Json<UpdatePostgresMinorApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    Ok(Json(
        queue_managed_postgres_minor_update(
            &store,
            &cluster,
            payload.target_postgres_version,
            auth.actor_id(),
            "managed_postgres_cluster.update_minor",
        )
        .await?,
    ))
}

async fn sql_api_request_managed_postgres_major_upgrade(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<RequestPostgresMajorUpgradeRequest>,
) -> Result<Json<RequestPostgresMajorUpgradeApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    Ok(Json(
        queue_managed_postgres_major_upgrade(
            &store,
            &cluster,
            payload.target_postgres_version,
            payload
                .strategy
                .unwrap_or(ManagedPostgresMajorUpgradeStrategy::LogicalReplicationCopy),
            auth.actor_id(),
        )
        .await?,
    ))
}

async fn sql_api_list_managed_postgres_major_upgrades(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresMajorUpgradesQuery>,
) -> Result<Json<ManagedPostgresMajorUpgradesResponse>, SqlApiError> {
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    sql_read_auth_context(&store, &headers)
        .await?
        .require_scope(ResourceScope::from_cluster(&cluster))?;
    let upgrades = store
        .managed_postgres_major_upgrades(&sql_store::ManagedPostgresMajorUpgradeFilter {
            cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresMajorUpgradesResponse { upgrades }))
}

async fn sql_api_get_managed_postgres_major_upgrade(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, upgrade_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresMajorUpgrade>, SqlApiError> {
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    sql_read_auth_context(&store, &headers)
        .await?
        .require_scope(ResourceScope::from_cluster(&cluster))?;
    let upgrade = store
        .managed_postgres_major_upgrade(&upgrade_id)
        .await?
        .filter(|upgrade| upgrade.cluster_id == cluster_id)
        .ok_or_else(|| SqlApiError::MissingResource(upgrade_id.clone()))?;
    Ok(Json(upgrade))
}

async fn sql_api_delete_managed_postgres_cluster(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<DeleteClusterRequest>,
) -> Result<Json<DeleteClusterApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let mut cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;

    let final_backup_requested = payload.final_backup.unwrap_or(false);
    if payload
        .tombstone_retention_days
        .is_some_and(|days| days > 3_650)
    {
        return Err(SqlApiError::BadRequest(
            "tombstone_retention_days must be 3650 or less".to_owned(),
        ));
    }

    // CloudNativePG clusters carry no host assignment: delete the operator's
    // resources for this cluster, then tombstone the record. (Final backups are
    // not yet wired to CloudNativePG `Backup` resources.)
    if cluster.host_assignment.is_none() {
        if final_backup_requested {
            return Err(SqlApiError::BadRequest(
                "final backup is not yet supported on the Kubernetes runtime".to_owned(),
            ));
        }
        let runtime = runtime::ClusterRuntime::from_env();
        let manifests = runtime.render(&cluster, None, &[]).map_err(runtime_error)?;
        runtime.delete(&manifests).map_err(runtime_error)?;
        cluster.lifecycle_state = ClusterLifecycleState::Deleted;
        store.update_managed_postgres_cluster(&cluster).await?;
        store
            .append_audit_event(&audit_event(
                auth.actor_id(),
                "managed_postgres_cluster.delete",
                &cluster_id,
            ))
            .await?;
        return Ok(Json(DeleteClusterApiResponse {
            cluster,
            command: None,
            operation: None,
            final_backup: None,
            final_backup_command: None,
            final_backup_operation: None,
            stop_command: None,
            stop_operation: None,
        }));
    }

    let assignment = cluster
        .host_assignment
        .clone()
        .expect("host_assignment is Some after the short-circuit above");

    if final_backup_requested && cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "final backup delete requires a ready cluster, got {:?}",
            cluster.lifecycle_state
        )));
    }
    if !final_backup_requested
        && !matches!(
            cluster.lifecycle_state,
            ClusterLifecycleState::Stopped | ClusterLifecycleState::Failed
        )
    {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be deleted from {:?}",
            cluster.lifecycle_state
        )));
    }

    let final_backup = if final_backup_requested {
        Some(queue_managed_postgres_backup(&store, &cluster).await?)
    } else {
        None
    };
    let stop_command = if final_backup_requested {
        Some(NodeAgentCommand {
            command_id: format!("{}:stop-postgres:{}", cluster.cluster_id, monotonic_nanos()),
            cluster_id: cluster.cluster_id.clone(),
            action: NodeAgentAction::StopPostgres,
        })
    } else {
        None
    };
    let stop_operation = if let Some(command) = stop_command.as_ref() {
        let operation =
            operation_for_agent_command(OperationKind::StopCluster, &cluster_id, command);
        store.insert_operation(&operation).await?;
        store
            .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), command)
            .await?;
        Some(operation)
    } else {
        None
    };

    cluster.lifecycle_state = ClusterLifecycleState::Deleting;
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:delete-postgres-data:{}",
            cluster.cluster_id,
            monotonic_nanos()
        ),
        cluster_id: cluster.cluster_id.clone(),
        action: NodeAgentAction::DeletePostgresData {
            data_dir: assignment.data_dir.clone(),
            tombstone_retention_days: payload.tombstone_retention_days,
        },
    };

    store.update_managed_postgres_cluster(&cluster).await?;
    let operation =
        operation_for_agent_command(OperationKind::DeleteCluster, &cluster_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_cluster.delete",
            &cluster_id,
        ))
        .await?;

    Ok(Json(DeleteClusterApiResponse {
        cluster,
        command: Some(command),
        operation: Some(operation),
        final_backup: final_backup.map(|response| response.backup),
        // The final backup now runs as a CloudNativePG `Backup` resource rather
        // than a host command, so no agent command/operation is produced.
        final_backup_command: None,
        final_backup_operation: None,
        stop_command,
        stop_operation,
    }))
}

async fn sql_api_request_managed_postgres_backup(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<BackupApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be backed up from {:?}",
            cluster.lifecycle_state
        )));
    }
    let response = queue_managed_postgres_backup(&store, &cluster).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_backup.request",
            &response.backup.backup_id,
        ))
        .await?;

    Ok(Json(response))
}

async fn sql_api_list_managed_postgres_backups(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresBackupsQuery>,
) -> Result<Json<ManagedPostgresBackupsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let backups = store
        .managed_postgres_backups(&sql_store::ManagedPostgresBackupFilter {
            cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresBackupsResponse { backups }))
}

async fn sql_api_get_managed_postgres_backup(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, backup_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresBackup>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let backup = store
        .managed_postgres_backup(&backup_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(backup_id.clone()))?;
    if backup.cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(backup_id));
    }
    Ok(Json(backup))
}

async fn sql_api_upsert_managed_postgres_backup_artifact(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, backup_id)): Path<(String, String)>,
    Json(payload): Json<ManagedPostgresBackupArtifact>,
) -> Result<Json<ManagedPostgresBackupArtifact>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let backup = managed_postgres_backup_for_cluster(&store, &cluster_id, &backup_id).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    validate_managed_postgres_backup_artifact(&backup, &payload)?;
    let artifact = store
        .upsert_managed_postgres_backup_artifact(&payload)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_backup_artifact.upsert",
            &artifact.artifact_id,
        ))
        .await?;
    Ok(Json(artifact))
}

async fn sql_api_list_managed_postgres_backup_artifacts(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, backup_id)): Path<(String, String)>,
    Query(query): Query<ManagedPostgresBackupArtifactsQuery>,
) -> Result<Json<ManagedPostgresBackupArtifactsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    managed_postgres_backup_for_cluster(&store, &cluster_id, &backup_id).await?;
    let artifacts = store
        .managed_postgres_backup_artifacts(&sql_store::ManagedPostgresBackupArtifactFilter {
            cluster_id: &cluster_id,
            backup_id: &backup_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresBackupArtifactsResponse { artifacts }))
}

async fn sql_api_get_managed_postgres_backup_artifact(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, backup_id, artifact_id)): Path<(String, String, String)>,
) -> Result<Json<ManagedPostgresBackupArtifact>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    managed_postgres_backup_for_cluster(&store, &cluster_id, &backup_id).await?;
    let artifact = store
        .managed_postgres_backup_artifact(&artifact_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(artifact_id.clone()))?;
    if artifact.cluster_id != cluster_id || artifact.backup_id != backup_id {
        return Err(SqlApiError::MissingResource(artifact_id));
    }
    Ok(Json(artifact))
}

async fn sql_api_get_managed_postgres_backup_retention_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<ManagedPostgresBackupRetentionPolicy>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let policy = store
        .managed_postgres_backup_retention_policy(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    Ok(Json(policy))
}

async fn sql_api_upsert_managed_postgres_backup_retention_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<UpsertBackupRetentionPolicyRequest>,
) -> Result<Json<ManagedPostgresBackupRetentionPolicy>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if payload.retention_days > 3650 {
        return Err(SqlApiError::BadRequest(
            "retention_days must be 3650 or less".to_owned(),
        ));
    }
    if payload.keep_min_successful_backups > 1000 {
        return Err(SqlApiError::BadRequest(
            "keep_min_successful_backups must be 1000 or less".to_owned(),
        ));
    }
    let policy = ManagedPostgresBackupRetentionPolicy {
        cluster_id: cluster_id.clone(),
        retention_days: payload.retention_days,
        keep_min_successful_backups: payload.keep_min_successful_backups,
        enabled: payload.enabled.unwrap_or(true),
        updated_at: String::new(),
    };
    let policy = store
        .upsert_managed_postgres_backup_retention_policy(&policy)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_backup_retention_policy.upsert",
            &cluster_id,
        ))
        .await?;
    Ok(Json(policy))
}

async fn sql_api_upsert_managed_postgres_clone_redaction_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(policy): Json<ManagedPostgresCloneRedactionPolicy>,
) -> Result<Json<ManagedPostgresCloneRedactionPolicy>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::environment(
        &policy.organization_id,
        &policy.project_id,
        &policy.environment_id,
    ))?;
    let policy = store
        .upsert_managed_postgres_clone_redaction_policy(&policy)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_clone_redaction_policy.upsert",
            &policy.policy_id,
        ))
        .await?;
    Ok(Json(policy))
}

async fn sql_api_list_managed_postgres_clone_redaction_policies(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<CloneRedactionPoliciesQuery>,
) -> Result<Json<CloneRedactionPoliciesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::new(
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    ))?;
    let policies = store
        .managed_postgres_clone_redaction_policies(&sql_store::CloneRedactionPolicyFilter {
            organization_id: query.organization_id.as_deref(),
            project_id: query.project_id.as_deref(),
            environment_id: query.environment_id.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(CloneRedactionPoliciesResponse { policies }))
}

async fn sql_api_get_managed_postgres_clone_redaction_policy(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(policy_id): Path<String>,
) -> Result<Json<ManagedPostgresCloneRedactionPolicy>, SqlApiError> {
    let policy = store
        .managed_postgres_clone_redaction_policy(&policy_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(policy_id.clone()))?;
    sql_read_auth_context(&store, &headers)
        .await?
        .require_scope(ResourceScope::environment(
            &policy.organization_id,
            &policy.project_id,
            &policy.environment_id,
        ))?;
    Ok(Json(policy))
}

async fn sql_api_create_managed_postgres_support_access_session(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<CreateSupportAccessSessionRequest>,
) -> Result<Json<ManagedPostgresSupportAccessSession>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    validate_support_access_request(&payload)?;
    let session = store
        .create_managed_postgres_support_access_session(
            &format!("support_access_{}", monotonic_nanos()),
            &cluster,
            auth.actor_id(),
            payload.reason.trim(),
            payload.ticket_ref.as_deref(),
            payload.duration_minutes,
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_support_access.request",
            &session.session_id,
        ))
        .await?;
    Ok(Json(session))
}

async fn sql_api_list_managed_postgres_support_access_sessions(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<SupportAccessSessionsQuery>,
) -> Result<Json<SupportAccessSessionsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let sessions = store
        .managed_postgres_support_access_sessions(
            &sql_store::ManagedPostgresSupportAccessSessionFilter {
                cluster_id: &cluster_id,
                status: query.status,
            },
        )
        .await?;
    Ok(Json(SupportAccessSessionsResponse { sessions }))
}

async fn sql_api_get_managed_postgres_support_access_session(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, session_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresSupportAccessSession>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let session = support_access_session_for_cluster(&store, &cluster_id, &session_id).await?;
    auth.require_scope(ResourceScope::environment(
        &session.organization_id,
        &session.project_id,
        &session.environment_id,
    ))?;
    Ok(Json(session))
}

async fn sql_api_approve_managed_postgres_support_access_session(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, session_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresSupportAccessSession>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let current = support_access_session_for_cluster(&store, &cluster_id, &session_id).await?;
    auth.require_scope(ResourceScope::environment(
        &current.organization_id,
        &current.project_id,
        &current.environment_id,
    ))?;
    let session = store
        .approve_managed_postgres_support_access_session(&session_id, auth.actor_id())
        .await?
        .ok_or_else(|| {
            SqlApiError::BadRequest(
                "support access session must be requested and unexpired to approve".to_owned(),
            )
        })?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_support_access.approve",
            &session.session_id,
        ))
        .await?;
    Ok(Json(session))
}

async fn sql_api_revoke_managed_postgres_support_access_session(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, session_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresSupportAccessSession>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let current = support_access_session_for_cluster(&store, &cluster_id, &session_id).await?;
    auth.require_scope(ResourceScope::environment(
        &current.organization_id,
        &current.project_id,
        &current.environment_id,
    ))?;
    let session = store
        .revoke_managed_postgres_support_access_session(&session_id, auth.actor_id())
        .await?
        .ok_or_else(|| {
            SqlApiError::BadRequest(
                "support access session must be requested or active to revoke".to_owned(),
            )
        })?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_support_access.revoke",
            &session.session_id,
        ))
        .await?;
    Ok(Json(session))
}

async fn support_access_session_for_cluster(
    store: &sql_store::SqlControlPlaneStore,
    cluster_id: &str,
    session_id: &str,
) -> Result<ManagedPostgresSupportAccessSession, SqlApiError> {
    let session = store
        .managed_postgres_support_access_session(session_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(session_id.to_owned()))?;
    if session.cluster_id != cluster_id {
        return Err(SqlApiError::BadRequest(
            "support access session does not belong to the requested cluster".to_owned(),
        ));
    }
    Ok(session)
}

async fn sql_api_run_backup_scheduler_once(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
) -> Result<Json<BackupSchedulerRunResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    Ok(Json(
        run_backup_scheduler_once(&store, auth.actor_id()).await?,
    ))
}

async fn sql_api_run_backup_retention_scheduler_once(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
) -> Result<Json<BackupRetentionSchedulerRunResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    Ok(Json(
        run_backup_retention_scheduler_once(&store, auth.actor_id()).await?,
    ))
}

async fn sql_api_run_restore_drill_scheduler_once(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<RestoreDrillSchedulerRunRequest>,
) -> Result<Json<RestoreDrillSchedulerRunResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    Ok(Json(
        run_restore_drill_scheduler_once(&store, auth.actor_id(), payload.max_age_hours).await?,
    ))
}

async fn sql_api_run_pitr_check_scheduler_once(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<PitrCheckSchedulerRunRequest>,
) -> Result<Json<PitrCheckSchedulerRunResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    Ok(Json(
        run_pitr_check_scheduler_once(&store, auth.actor_id(), payload.max_age_hours).await?,
    ))
}

async fn sql_api_run_acme_order_scheduler_once(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<AcmeOrderSchedulerRunRequest>,
) -> Result<Json<AcmeOrderSchedulerRunResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    Ok(Json(
        run_acme_order_scheduler_once(&store, auth.actor_id(), payload.limit).await?,
    ))
}

async fn sql_api_run_maintenance_scheduler_once(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<MaintenanceSchedulerRunRequest>,
) -> Result<Json<MaintenanceSchedulerRunResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    Ok(Json(
        run_maintenance_scheduler_once(
            &store,
            auth.actor_id(),
            payload.target_postgres_version,
            payload.day_of_week,
            payload.current_time.as_deref(),
        )
        .await?,
    ))
}

async fn sql_api_create_api_key(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<CreateApiKeyRequest>,
) -> Result<Json<CreateApiKeyResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    auth.require_can_manage_role(payload.role)?;
    let scope = resolve_resource_scope(
        &store,
        payload.organization_id.as_deref(),
        payload.project_id.as_deref(),
        payload.environment_id.as_deref(),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    let token = generate_api_key_token()?;
    let token_prefix = token_prefix(&token);
    let api_key = ApiKey {
        key_id: format!("key_{}", monotonic_nanos()),
        token_prefix,
        name: payload.name,
        organization_id: scope.organization_id,
        project_id: scope.project_id,
        environment_id: scope.environment_id,
        role: payload.role,
        created_by: auth.actor_id().to_owned(),
        revoked: false,
    };
    let api_key = store
        .insert_api_key(&api_key, &api_key_token_hash(&token))
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "api_key.create",
            &api_key.key_id,
        ))
        .await?;
    Ok(Json(CreateApiKeyResponse { api_key, token }))
}

async fn sql_api_list_api_keys(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<ApiKeysQuery>,
) -> Result<Json<ApiKeysResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };

    let api_keys = store
        .api_keys(&sql_store::ApiKeyFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            include_revoked: query.include_revoked,
        })
        .await?;
    Ok(Json(ApiKeysResponse { api_keys }))
}

async fn sql_api_revoke_api_key(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Result<Json<ApiKey>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    let target = store
        .api_key(&key_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(key_id.clone()))?;
    auth.require_scope(ResourceScope::from_api_key(&target))?;
    auth.require_can_manage_role(target.role)?;
    let api_key = store.revoke_api_key(&key_id).await?;
    store
        .append_audit_event(&audit_event(auth.actor_id(), "api_key.revoke", &key_id))
        .await?;
    Ok(Json(api_key))
}

async fn sql_api_upsert_secret_encryption_key(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<SecretEncryptionKey>,
) -> Result<Json<SecretEncryptionKey>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    validate_secret_encryption_key(&payload)?;
    let key = store.upsert_secret_encryption_key(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "secret_encryption_key.upsert",
            &key.key_ref,
        ))
        .await?;
    Ok(Json(key))
}

async fn sql_api_list_secret_encryption_keys(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<SecretEncryptionKeysQuery>,
) -> Result<Json<SecretEncryptionKeysResponse>, SqlApiError> {
    sql_read_auth_context(&store, &headers)
        .await?
        .require_platform_admin()?;
    let keys = store
        .secret_encryption_keys(&sql_store::SecretEncryptionKeyFilter {
            provider: query.provider.as_deref(),
            purpose: query.purpose.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(SecretEncryptionKeysResponse { keys }))
}

async fn sql_api_get_secret_encryption_key(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(key_ref): Path<String>,
) -> Result<Json<SecretEncryptionKey>, SqlApiError> {
    sql_read_auth_context(&store, &headers)
        .await?
        .require_platform_admin()?;
    let key = store
        .secret_encryption_key(&key_ref)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(key_ref.clone()))?;
    Ok(Json(key))
}

async fn sql_api_create_secret_rewrap_plan(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<CreateSecretRewrapPlanRequest>,
) -> Result<Json<SecretRewrapPlan>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    validate_secret_rewrap_plan_request(&payload)?;
    let source = store
        .secret_encryption_key(&payload.source_key_ref)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(payload.source_key_ref.clone()))?;
    let target = store
        .secret_encryption_key(&payload.target_key_ref)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(payload.target_key_ref.clone()))?;
    if source.status == SecretEncryptionKeyStatus::Retired {
        return Err(SqlApiError::BadRequest(
            "source secret encryption key is already retired".to_owned(),
        ));
    }
    if target.status != SecretEncryptionKeyStatus::Active {
        return Err(SqlApiError::BadRequest(
            "target secret encryption key must be active".to_owned(),
        ));
    }
    let plan = store
        .create_secret_rewrap_plan(
            &format!("secret_rewrap_{}", monotonic_nanos()),
            &source.key_ref,
            &target.key_ref,
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "secret_rewrap_plan.create",
            &plan.plan_id,
        ))
        .await?;
    Ok(Json(plan))
}

async fn sql_api_list_secret_rewrap_plans(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<SecretRewrapPlansQuery>,
) -> Result<Json<SecretRewrapPlansResponse>, SqlApiError> {
    sql_read_auth_context(&store, &headers)
        .await?
        .require_platform_admin()?;
    let plans = store
        .secret_rewrap_plans(&sql_store::SecretRewrapPlanFilter {
            source_key_ref: query.source_key_ref.as_deref(),
            target_key_ref: query.target_key_ref.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(SecretRewrapPlansResponse { plans }))
}

async fn sql_api_get_secret_rewrap_plan(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(plan_id): Path<String>,
) -> Result<Json<SecretRewrapPlan>, SqlApiError> {
    sql_read_auth_context(&store, &headers)
        .await?
        .require_platform_admin()?;
    let plan = store
        .secret_rewrap_plan(&plan_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(plan_id.clone()))?;
    Ok(Json(plan))
}

async fn sql_api_run_secret_rewrap_plan(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(plan_id): Path<String>,
) -> Result<Json<SecretRewrapPlan>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_platform_admin()?;
    let plan = store.run_secret_rewrap_plan(&plan_id).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "secret_rewrap_plan.run",
            &plan.plan_id,
        ))
        .await?;
    Ok(Json(plan))
}

async fn sql_api_upsert_jwt_issuer(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<JwtIssuer>,
) -> Result<Json<JwtIssuer>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    let scope = resolve_resource_scope(
        &store,
        Some(&payload.organization_id),
        payload.project_id.as_deref(),
        payload.environment_id.as_deref(),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    if scope.organization_id.as_deref() != Some(payload.organization_id.as_str())
        || scope.project_id.as_deref() != payload.project_id.as_deref()
        || scope.environment_id.as_deref() != payload.environment_id.as_deref()
    {
        return Err(SqlApiError::BadRequest(
            "JWT issuer scope does not match resolved resource scope".to_owned(),
        ));
    }
    validate_jwt_issuer(&payload)?;
    let issuer = store.upsert_jwt_issuer(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "jwt_issuer.upsert",
            &issuer.issuer_id,
        ))
        .await?;
    Ok(Json(issuer))
}

async fn sql_api_list_jwt_issuers(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<JwtIssuersQuery>,
) -> Result<Json<JwtIssuersResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let issuers = store
        .jwt_issuers(&sql_store::JwtIssuerFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(JwtIssuersResponse { issuers }))
}

async fn sql_api_get_jwt_issuer(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(issuer_id): Path<String>,
) -> Result<Json<JwtIssuer>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let issuer = store
        .jwt_issuer(&issuer_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(issuer_id.clone()))?;
    auth.require_scope(ResourceScope::new(
        Some(&issuer.organization_id),
        issuer.project_id.as_deref(),
        issuer.environment_id.as_deref(),
    ))?;
    Ok(Json(issuer))
}

async fn sql_api_upsert_webhook_endpoint(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<WebhookEndpoint>,
) -> Result<Json<WebhookEndpoint>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    let scope = resolve_resource_scope(
        &store,
        Some(&payload.organization_id),
        payload.project_id.as_deref(),
        payload.environment_id.as_deref(),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    if scope.organization_id.as_deref() != Some(payload.organization_id.as_str())
        || scope.project_id.as_deref() != payload.project_id.as_deref()
        || scope.environment_id.as_deref() != payload.environment_id.as_deref()
    {
        return Err(SqlApiError::BadRequest(
            "webhook endpoint scope does not match resolved resource scope".to_owned(),
        ));
    }
    validate_webhook_endpoint(&payload)?;
    let endpoint = store.upsert_webhook_endpoint(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "webhook_endpoint.upsert",
            &endpoint.endpoint_id,
        ))
        .await?;
    Ok(Json(endpoint))
}

async fn sql_api_list_webhook_endpoints(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<WebhookEndpointsQuery>,
) -> Result<Json<WebhookEndpointsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let endpoints = store
        .webhook_endpoints(&sql_store::WebhookEndpointFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            status: query.status,
        })
        .await?;
    Ok(Json(WebhookEndpointsResponse { endpoints }))
}

async fn sql_api_get_webhook_endpoint(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(endpoint_id): Path<String>,
) -> Result<Json<WebhookEndpoint>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let endpoint = store
        .webhook_endpoint(&endpoint_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(endpoint_id.clone()))?;
    auth.require_scope(ResourceScope::new(
        Some(&endpoint.organization_id),
        endpoint.project_id.as_deref(),
        endpoint.environment_id.as_deref(),
    ))?;
    Ok(Json(endpoint))
}

async fn sql_api_upsert_sso_identity_provider(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<SsoIdentityProvider>,
) -> Result<Json<SsoIdentityProvider>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    auth.require_scope(ResourceScope::organization(&payload.organization_id))?;
    validate_sso_identity_provider(&payload)?;
    let provider = store.upsert_sso_identity_provider(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "sso_identity_provider.upsert",
            &provider.provider_id,
        ))
        .await?;
    Ok(Json(provider))
}

async fn sql_api_list_sso_identity_providers(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<SsoIdentityProvidersQuery>,
) -> Result<Json<SsoIdentityProvidersResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::organization(&query.organization_id))?;
    let providers = store
        .sso_identity_providers(&sql_store::SsoIdentityProviderFilter {
            organization_id: &query.organization_id,
            kind: query.kind,
            status: query.status,
        })
        .await?;
    Ok(Json(SsoIdentityProvidersResponse { providers }))
}

async fn sql_api_get_sso_identity_provider(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(provider_id): Path<String>,
) -> Result<Json<SsoIdentityProvider>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let provider = store
        .sso_identity_provider(&provider_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(provider_id.clone()))?;
    auth.require_scope(ResourceScope::organization(&provider.organization_id))?;
    Ok(Json(provider))
}

async fn sql_api_upsert_incident(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<Incident>,
) -> Result<Json<Incident>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    let scope = resolve_resource_scope(
        &store,
        Some(&payload.organization_id),
        payload.project_id.as_deref(),
        payload.environment_id.as_deref(),
    )
    .await?;
    auth.require_scope(scope.as_resource_scope())?;
    if scope.organization_id.as_deref() != Some(payload.organization_id.as_str())
        || scope.project_id.as_deref() != payload.project_id.as_deref()
        || scope.environment_id.as_deref() != payload.environment_id.as_deref()
    {
        return Err(SqlApiError::BadRequest(
            "incident scope does not match resolved resource scope".to_owned(),
        ));
    }
    validate_incident(&payload)?;
    let incident = store.upsert_incident(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "incident.upsert",
            &incident.incident_id,
        ))
        .await?;
    Ok(Json(incident))
}

async fn sql_api_list_incidents(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<IncidentsQuery>,
) -> Result<Json<IncidentsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let scope = scoped_query_from_auth(
        &store,
        &auth,
        query.organization_id.as_deref(),
        query.project_id.as_deref(),
        query.environment_id.as_deref(),
    )
    .await?;
    let incidents = store
        .incidents(&sql_store::IncidentFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            severity: query.severity,
            status: query.status,
            include_resolved: query.include_resolved,
        })
        .await?;
    Ok(Json(IncidentsResponse { incidents }))
}

async fn sql_api_get_incident(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(incident_id): Path<String>,
) -> Result<Json<Incident>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let incident = store
        .incident(&incident_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(incident_id.clone()))?;
    auth.require_scope(ResourceScope::new(
        Some(&incident.organization_id),
        incident.project_id.as_deref(),
        incident.environment_id.as_deref(),
    ))?;
    Ok(Json(incident))
}

async fn sql_api_upsert_team_membership(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Json(payload): Json<TeamMembership>,
) -> Result<Json<TeamMembership>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    auth.require_api_key_admin()?;
    auth.require_can_manage_role(payload.role)?;
    auth.require_scope(ResourceScope::organization(&payload.organization_id))?;
    let membership = store.upsert_team_membership(&payload).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "team_membership.upsert",
            &format!("{}:{}", membership.organization_id, membership.actor_id),
        ))
        .await?;
    Ok(Json(membership))
}

async fn sql_api_list_team_memberships(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<TeamMembershipsQuery>,
) -> Result<Json<TeamMembershipsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    auth.require_scope(ResourceScope::organization(&query.organization_id))?;
    let memberships = store.team_memberships(&query.organization_id).await?;
    Ok(Json(TeamMembershipsResponse { memberships }))
}

async fn sql_api_list_audit_events(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Query(query): Query<AuditEventsQuery>,
) -> Result<Json<AuditEventsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let has_explicit_scope = query.organization_id.is_some()
        || query.project_id.is_some()
        || query.environment_id.is_some();
    let scope = if has_explicit_scope {
        let scope = resolve_resource_scope(
            &store,
            query.organization_id.as_deref(),
            query.project_id.as_deref(),
            query.environment_id.as_deref(),
        )
        .await?;
        auth.require_scope(scope.as_resource_scope())?;
        scope
    } else if let Some(api_key) = auth.api_key.as_ref() {
        OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        }
    } else {
        OwnedResourceScope {
            organization_id: None,
            project_id: None,
            environment_id: None,
        }
    };
    let events = store
        .audit_events(&sql_store::AuditEventFilter {
            organization_id: scope.organization_id.as_deref(),
            project_id: scope.project_id.as_deref(),
            environment_id: scope.environment_id.as_deref(),
            actor_id: query.actor_id.as_deref(),
            action: query.action.as_deref(),
            resource_id: query.resource_id.as_deref(),
            limit: query.limit,
        })
        .await?;
    Ok(Json(AuditEventsResponse { events }))
}

async fn sql_api_request_wal_archive(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<WalArchiveRequest>,
) -> Result<Json<WalArchiveApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    validate_wal_segment_name(&payload.segment_name)?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot archive WAL from {:?}",
            cluster.lifecycle_state
        )));
    }
    let assignment = cluster
        .host_assignment
        .as_ref()
        .ok_or_else(|| SqlApiError::BadRequest("cluster has no host assignment".to_owned()))?;

    let archive_dir = wal_archive_root_for_data_dir(&assignment.data_dir, &cluster.cluster_id)?;
    let segment = ManagedPostgresWalArchiveSegment {
        cluster_id: cluster.cluster_id.clone(),
        segment_name: payload.segment_name.clone(),
        status: WalArchiveSegmentStatus::Running,
        archive_dir: archive_dir.display().to_string(),
        error_message: None,
    };
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:archive-wal:{}",
            cluster.cluster_id, payload.segment_name
        ),
        cluster_id: cluster.cluster_id.clone(),
        action: NodeAgentAction::ArchiveWalSegment {
            source_path: FsPath::new(&assignment.data_dir)
                .join("pg_wal")
                .join(&payload.segment_name)
                .display()
                .to_string(),
            archive_dir: segment.archive_dir.clone(),
            segment_name: payload.segment_name,
        },
    };

    store.upsert_wal_archive_segment(&segment).await?;
    let operation = operation_for_agent_command(
        OperationKind::ArchiveWalSegment,
        &format!("{}:{}", segment.cluster_id, segment.segment_name),
        &command,
    );
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_wal_archive.request",
            &format!("{}:{}", segment.cluster_id, segment.segment_name),
        ))
        .await?;

    Ok(Json(WalArchiveApiResponse {
        segment,
        command,
        operation,
    }))
}

async fn sql_api_list_wal_archives(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<WalArchivesQuery>,
) -> Result<Json<WalArchivesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let segments = store
        .wal_archive_segments(&sql_store::WalArchiveSegmentFilter {
            cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(WalArchivesResponse { segments }))
}

async fn sql_api_get_wal_archive(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, segment_name)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresWalArchiveSegment>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    validate_wal_segment_name(&segment_name)?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let segment = store
        .wal_archive_segment(&cluster_id, &segment_name)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(segment_name.clone()))?;
    Ok(Json(segment))
}

async fn sql_api_request_managed_postgres_pitr_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<PitrCheckApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let check = run_pitr_check_for_cluster(&store, &cluster).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_pitr_check.request",
            &check.check_id,
        ))
        .await?;
    Ok(Json(PitrCheckApiResponse { check }))
}

async fn sql_api_list_managed_postgres_pitr_checks(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresPitrChecksQuery>,
) -> Result<Json<ManagedPostgresPitrChecksResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let checks = store
        .managed_postgres_pitr_checks(&sql_store::ManagedPostgresPitrCheckFilter {
            cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresPitrChecksResponse { checks }))
}

async fn sql_api_get_managed_postgres_pitr_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, check_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresPitrCheck>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let check = store
        .managed_postgres_pitr_check(&check_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(check_id.clone()))?;
    if check.cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(check_id));
    }
    Ok(Json(check))
}

async fn sql_api_request_managed_postgres_failover(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<FailoverClusterRequest>,
) -> Result<Json<FailoverApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let target_cluster_id = if let Some(standby_id) = payload.standby_id.as_deref() {
        let standby = store
            .managed_postgres_standby(standby_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(standby_id.to_owned()))?;
        if standby.source_cluster_id != source.cluster_id {
            return Err(SqlApiError::BadRequest(
                "failover standby must belong to the source cluster".to_owned(),
            ));
        }
        if standby.status != StandbyLifecycleState::Succeeded {
            return Err(SqlApiError::BadRequest(
                "failover standby must be prepared successfully".to_owned(),
            ));
        }
        if payload
            .target_cluster_id
            .as_ref()
            .is_some_and(|target_cluster_id| target_cluster_id != &standby.target_cluster_id)
        {
            return Err(SqlApiError::BadRequest(
                "failover target_cluster_id must match standby target".to_owned(),
            ));
        }
        standby.target_cluster_id
    } else {
        payload.target_cluster_id.clone().ok_or_else(|| {
            SqlApiError::BadRequest("failover requires target_cluster_id or standby_id".to_owned())
        })?
    };
    let target = store
        .managed_postgres_cluster(&target_cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(target_cluster_id.clone()))?;
    if target.organization_id != source.organization_id
        || target.project_id != source.project_id
        || target.environment_id != source.environment_id
    {
        return Err(SqlApiError::BadRequest(
            "failover target must belong to the same environment".to_owned(),
        ));
    }
    let failover = ManagedPostgresFailover {
        failover_id: format!("failover_{}", monotonic_nanos()),
        source_cluster_id: source.cluster_id.clone(),
        target_cluster_id: target.cluster_id.clone(),
        status: FailoverLifecycleState::Running,
        error_message: None,
    };

    // Fence the old primary (CloudNativePG fencing annotation) to stop writes,
    // then promote the standby by re-applying it without the replica section.
    let runtime = runtime::ClusterRuntime::from_env();
    runtime.fence(&source, true).map_err(runtime_error)?;
    runtime.promote(&target).map_err(runtime_error)?;

    store.insert_managed_postgres_failover(&failover).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_failover.request",
            &failover.failover_id,
        ))
        .await?;
    Ok(Json(FailoverApiResponse { failover }))
}

async fn sql_api_list_managed_postgres_failovers(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresFailoversQuery>,
) -> Result<Json<ManagedPostgresFailoversResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let failovers = store
        .managed_postgres_failovers(&sql_store::ManagedPostgresFailoverFilter {
            source_cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresFailoversResponse { failovers }))
}

async fn sql_api_get_managed_postgres_failover(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, failover_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresFailover>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let failover = store
        .managed_postgres_failover(&failover_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(failover_id.clone()))?;
    if failover.source_cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(failover_id));
    }
    Ok(Json(failover))
}

async fn sql_api_request_managed_postgres_standby(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<CreateStandbyRequest>,
) -> Result<Json<StandbyApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let backup = if let Some(backup_id) = payload.backup_id.as_deref() {
        store
            .managed_postgres_backup(backup_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(backup_id.to_owned()))?
    } else {
        store
            .latest_succeeded_backup_for_cluster(&source.cluster_id)
            .await?
            .ok_or_else(|| SqlApiError::BadRequest("cluster has no succeeded backup".to_owned()))?
    };
    if backup.cluster_id != source.cluster_id || backup.status != BackupLifecycleState::Succeeded {
        return Err(SqlApiError::BadRequest(
            "standby requires a succeeded backup from the source cluster".to_owned(),
        ));
    }
    let standby_id = format!("standby_{}", monotonic_nanos());
    let target_cluster_id = payload
        .target_cluster_id
        .clone()
        .unwrap_or_else(|| format!("{}_standby_{}", source.cluster_id, monotonic_nanos()));
    // CloudNativePG places the standby; it continuously replays the source's
    // WAL from object storage until promoted (a replica cluster).
    let target = ManagedPostgresCluster {
        cluster_id: target_cluster_id.clone(),
        organization_id: source.organization_id.clone(),
        project_id: source.project_id.clone(),
        environment_id: source.environment_id.clone(),
        region: source.region.clone(),
        postgres_version: source.postgres_version.clone(),
        tier: source.tier.clone(),
        storage_gib: source.storage_gib,
        lifecycle_state: ClusterLifecycleState::Restoring,
        host_assignment: None,
    };
    let standby = ManagedPostgresStandby {
        standby_id: standby_id.clone(),
        source_cluster_id: source.cluster_id.clone(),
        target_cluster_id: target_cluster_id.clone(),
        backup_id: backup.backup_id.clone(),
        status: StandbyLifecycleState::Running,
        error_message: None,
    };
    let replica_source = palimpsest_paas_runtime::RestoreSource {
        source_cluster_id: source.cluster_id.clone(),
        recovery_target_time: None,
        recovery_target_lsn: None,
        continuous_replica: true,
    };
    runtime::ClusterRuntime::from_env()
        .restore(&target, &replica_source)
        .map_err(runtime_error)?;

    store.insert_managed_postgres_cluster(&target).await?;
    store.insert_managed_postgres_standby(&standby).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_standby.request",
            &standby.standby_id,
        ))
        .await?;

    Ok(Json(StandbyApiResponse {
        standby,
        cluster: target,
    }))
}

async fn sql_api_list_managed_postgres_standbys(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresStandbysQuery>,
) -> Result<Json<ManagedPostgresStandbysResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let standbys = store
        .managed_postgres_standbys(&sql_store::ManagedPostgresStandbyFilter {
            source_cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresStandbysResponse { standbys }))
}

async fn sql_api_get_managed_postgres_standby(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, standby_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresStandby>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let standby = store
        .managed_postgres_standby(&standby_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(standby_id.clone()))?;
    if standby.source_cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(standby_id));
    }
    Ok(Json(standby))
}

async fn sql_api_request_managed_postgres_standby_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, standby_id)): Path<(String, String)>,
    Json(payload): Json<CreateStandbyCheckRequest>,
) -> Result<Json<StandbyCheckApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let source_assignment = source.host_assignment.as_ref().ok_or_else(|| {
        SqlApiError::BadRequest("source cluster has no host assignment".to_owned())
    })?;
    let standby = store
        .managed_postgres_standby(&standby_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(standby_id.clone()))?;
    if standby.source_cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(standby_id));
    }
    if standby.status != StandbyLifecycleState::Succeeded {
        return Err(SqlApiError::BadRequest(
            "standby lag check requires a successfully prepared standby".to_owned(),
        ));
    }
    let max_lag_bytes = payload.max_lag_bytes.unwrap_or(16 * 1024 * 1024);
    if max_lag_bytes > i64::MAX as u64 {
        return Err(SqlApiError::BadRequest(
            "max_lag_bytes must fit in a Postgres bigint".to_owned(),
        ));
    }
    let slot_name = standby_physical_slot_name(&standby.target_cluster_id);
    let check = ManagedPostgresStandbyCheck {
        check_id: format!("standby_check_{}", monotonic_nanos()),
        standby_id: standby.standby_id.clone(),
        source_cluster_id: standby.source_cluster_id.clone(),
        target_cluster_id: standby.target_cluster_id.clone(),
        slot_name: slot_name.clone(),
        max_lag_bytes,
        status: StandbyCheckStatus::Running,
        error_message: None,
    };
    let command = NodeAgentCommand {
        command_id: format!("{}:check-standby-lag:{}", source.cluster_id, check.check_id),
        cluster_id: source.cluster_id.clone(),
        action: NodeAgentAction::CheckPostgresStandbyLag {
            source_data_dir: source_assignment.data_dir.clone(),
            source_port: source_assignment.port,
            database: "postgres".to_owned(),
            slot_name,
            max_lag_bytes,
        },
    };
    store.insert_managed_postgres_standby_check(&check).await?;
    let operation =
        operation_for_agent_command(OperationKind::CheckStandby, &check.check_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(
            &source_assignment.host_id,
            Some(&operation.operation_id),
            &command,
        )
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_standby_check.request",
            &check.check_id,
        ))
        .await?;
    Ok(Json(StandbyCheckApiResponse {
        check,
        command,
        operation,
    }))
}

async fn sql_api_list_managed_postgres_standby_checks(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, standby_id)): Path<(String, String)>,
    Query(query): Query<ManagedPostgresStandbyChecksQuery>,
) -> Result<Json<ManagedPostgresStandbyChecksResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let standby = store
        .managed_postgres_standby(&standby_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(standby_id.clone()))?;
    if standby.source_cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(standby_id));
    }
    let checks = store
        .managed_postgres_standby_checks(&sql_store::ManagedPostgresStandbyCheckFilter {
            standby_id: &standby.standby_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresStandbyChecksResponse { checks }))
}

async fn sql_api_get_managed_postgres_standby_check(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, standby_id, check_id)): Path<(String, String, String)>,
) -> Result<Json<ManagedPostgresStandbyCheck>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let check = store
        .managed_postgres_standby_check(&check_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(check_id.clone()))?;
    if check.source_cluster_id != cluster_id || check.standby_id != standby_id {
        return Err(SqlApiError::MissingResource(check_id));
    }
    Ok(Json(check))
}

async fn sql_api_request_managed_postgres_restore(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<RestoreClusterRequest>,
) -> Result<Json<RestoreApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;

    let backup = if let Some(backup_id) = payload.backup_id.as_deref() {
        store
            .managed_postgres_backup(backup_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(backup_id.to_owned()))?
    } else {
        store
            .latest_succeeded_backup_for_cluster(&cluster_id)
            .await?
            .ok_or_else(|| SqlApiError::BadRequest("cluster has no succeeded backup".to_owned()))?
    };
    if backup.cluster_id != source.cluster_id || backup.status != BackupLifecycleState::Succeeded {
        return Err(SqlApiError::BadRequest(
            "restore requires a succeeded backup from the source cluster".to_owned(),
        ));
    }
    let target_environment_id = payload
        .target_environment_id
        .clone()
        .unwrap_or_else(|| source.environment_id.clone());
    let target_scope = store
        .environment_scope(&target_environment_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(target_environment_id.clone()))?;
    if target_scope.0 != source.organization_id || target_scope.1 != source.project_id {
        return Err(SqlApiError::BadRequest(
            "restore target environment must belong to the source project".to_owned(),
        ));
    }
    let redaction_policy = if let Some(policy_id) = payload.redaction_policy_id.as_deref() {
        let policy = store
            .managed_postgres_clone_redaction_policy(policy_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(policy_id.to_owned()))?;
        if policy.organization_id != source.organization_id
            || policy.project_id != source.project_id
            || policy.environment_id != source.environment_id
            || policy.status != CloneRedactionPolicyStatus::Active
        {
            return Err(SqlApiError::BadRequest(
                "restore redaction policy must be active and scoped to the source environment"
                    .to_owned(),
            ));
        }
        Some(policy)
    } else {
        None
    };
    if target_environment_id != source.environment_id && redaction_policy.is_none() {
        return Err(SqlApiError::BadRequest(
            "cross-environment restore requires an active source clone redaction policy".to_owned(),
        ));
    }
    // CloudNativePG recovery restores the source data verbatim; it cannot apply
    // a clone redaction policy. Reject rather than silently restore unmasked
    // data (which a cross-environment clone relies on being redacted).
    if redaction_policy.is_some() {
        return Err(SqlApiError::BadRequest(
            "clone redaction policies are not yet supported on the Kubernetes runtime".to_owned(),
        ));
    }

    let restore_id = format!("restore_{}", monotonic_nanos());
    let target_cluster_id = payload
        .target_cluster_id
        .clone()
        .unwrap_or_else(|| format!("{}_{}", source.cluster_id, restore_id));
    // CloudNativePG owns instance placement; the target carries no host
    // assignment and recovers the source's backups from object storage.
    let target = ManagedPostgresCluster {
        cluster_id: target_cluster_id.clone(),
        organization_id: source.organization_id.clone(),
        project_id: source.project_id.clone(),
        environment_id: target_environment_id.clone(),
        region: source.region.clone(),
        postgres_version: source.postgres_version.clone(),
        tier: source.tier.clone(),
        storage_gib: source.storage_gib,
        lifecycle_state: ClusterLifecycleState::Restoring,
        host_assignment: None,
    };
    let restore = ManagedPostgresRestore {
        restore_id: restore_id.clone(),
        source_cluster_id: source.cluster_id.clone(),
        target_cluster_id: target_cluster_id.clone(),
        target_environment_id: target_environment_id.clone(),
        backup_id: backup.backup_id.clone(),
        status: RestoreLifecycleState::Running,
        redaction_policy_id: None,
        recovery_target_lsn: payload.recovery_target_lsn.clone(),
        error_message: None,
    };

    let restore_source = palimpsest_paas_runtime::RestoreSource {
        source_cluster_id: source.cluster_id.clone(),
        recovery_target_time: None,
        recovery_target_lsn: payload.recovery_target_lsn.clone(),
        continuous_replica: false,
    };
    runtime::ClusterRuntime::from_env()
        .restore(&target, &restore_source)
        .map_err(runtime_error)?;

    store.insert_managed_postgres_cluster(&target).await?;
    store.insert_managed_postgres_restore(&restore).await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_restore.request",
            &restore.restore_id,
        ))
        .await?;

    Ok(Json(RestoreApiResponse {
        restore,
        cluster: target,
    }))
}

async fn sql_api_request_managed_postgres_database_clone(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<DatabaseCloneRequest>,
) -> Result<Json<DatabaseCloneApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(
            "copy-on-write database clone requires a ready source cluster".to_owned(),
        ));
    }
    if cluster.postgres_version.major() < MIN_SUPPORTED_POSTGRES_MAJOR {
        return Err(SqlApiError::BadRequest(format!(
            "copy-on-write database clone requires PostgreSQL {MIN_SUPPORTED_POSTGRES_MAJOR}+"
        )));
    }
    validate_database_identifier(&payload.source_database)?;
    validate_database_identifier(&payload.target_database)?;
    if payload.source_database == payload.target_database {
        return Err(SqlApiError::BadRequest(
            "copy-on-write database clone target must differ from source".to_owned(),
        ));
    }

    let clone_id = format!(
        "{}:{}",
        cluster.cluster_id,
        sanitize_identifier_component(&payload.target_database)
    );
    // CREATE DATABASE <target> TEMPLATE <source> on the cluster's primary.
    runtime::ClusterRuntime::from_env()
        .clone_database(
            &cluster,
            &payload.source_database,
            &payload.target_database,
            payload.terminate_source_connections,
        )
        .map_err(runtime_error)?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_database_clone.request",
            &clone_id,
        ))
        .await?;

    Ok(Json(DatabaseCloneApiResponse {
        cluster,
        source_database: payload.source_database,
        target_database: payload.target_database,
    }))
}

/// Validates a user-facing branch name: 1-63 chars, ASCII alphanumerics plus
/// `_`/`-`.
///
/// The backing database identifier is derived separately and further sanitized
/// to a strict Postgres identifier.
fn validate_branch_name(value: &str) -> Result<(), SqlApiError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(SqlApiError::BadRequest(format!(
            "invalid branch name '{value}'"
        )));
    }
    Ok(())
}

async fn sql_api_create_managed_postgres_branch(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<CreateBranchRequest>,
) -> Result<Json<BranchApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    validate_branch_name(&payload.name)?;

    match payload.mode {
        BranchMode::HeadCow => {
            create_head_cow_branch(&store, auth.actor_id(), &cluster, &payload).await
        }
        BranchMode::PointInTime => {
            create_point_in_time_branch(&store, auth.actor_id(), &cluster, &payload).await
        }
    }
}

fn branch_id_for(cluster_id: &str, name: &str) -> String {
    format!(
        "{cluster_id}:branch:{}",
        sanitize_identifier_component(name)
    )
}

async fn create_head_cow_branch(
    store: &SharedSqlControlPlaneStore,
    actor_id: &str,
    cluster: &ManagedPostgresCluster,
    payload: &CreateBranchRequest,
) -> Result<Json<BranchApiResponse>, SqlApiError> {
    if payload.provision_sync_deployment {
        return Err(SqlApiError::BadRequest(
            "per-branch sync deployments are supported only for point-in-time branches".to_owned(),
        ));
    }
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(
            "copy-on-write branch requires a ready cluster".to_owned(),
        ));
    }
    if cluster.postgres_version.major() < MIN_SUPPORTED_POSTGRES_MAJOR {
        return Err(SqlApiError::BadRequest(format!(
            "copy-on-write branch requires PostgreSQL {MIN_SUPPORTED_POSTGRES_MAJOR}+"
        )));
    }

    // Resolve the source database: a parent branch's database, or — for a root
    // branch — the requested source database (defaulting to `postgres`).
    let source_database = match payload.parent_branch_id.as_deref() {
        Some(parent_id) => {
            let parent = store
                .managed_postgres_branch(parent_id)
                .await?
                .ok_or_else(|| SqlApiError::MissingResource(parent_id.to_owned()))?;
            if parent.cluster_id != cluster.cluster_id {
                return Err(SqlApiError::BadRequest(
                    "parent branch belongs to a different cluster".to_owned(),
                ));
            }
            if parent.lifecycle_state != BranchLifecycleState::Ready {
                return Err(SqlApiError::BadRequest(
                    "parent branch is not ready".to_owned(),
                ));
            }
            parent.branch_database.ok_or_else(|| {
                SqlApiError::BadRequest(
                    "parent branch has no copy-on-write database to branch from".to_owned(),
                )
            })?
        }
        None => payload
            .source_database
            .clone()
            .unwrap_or_else(|| "postgres".to_owned()),
    };
    validate_database_identifier(&source_database)?;

    let branch_database = sanitize_identifier_component(&format!("branch_{}", payload.name));
    validate_database_identifier(&branch_database)?;
    if branch_database == source_database {
        return Err(SqlApiError::BadRequest(
            "derived branch database must differ from the source database".to_owned(),
        ));
    }

    let branch_id = branch_id_for(&cluster.cluster_id, &payload.name);
    // A copy-on-write branch is a templated database copy inside the cluster.
    runtime::ClusterRuntime::from_env()
        .clone_database(
            cluster,
            &source_database,
            &branch_database,
            payload.terminate_source_connections,
        )
        .map_err(runtime_error)?;
    let branch = ManagedPostgresBranch {
        branch_id: branch_id.clone(),
        cluster_id: cluster.cluster_id.clone(),
        name: payload.name.clone(),
        parent_branch_id: payload.parent_branch_id.clone(),
        mode: BranchMode::HeadCow,
        source_database,
        branch_database: Some(branch_database),
        branch_cluster_id: None,
        created_from_lsn: None,
        redaction_policy_id: payload.redaction_policy_id.clone(),
        lifecycle_state: BranchLifecycleState::Ready,
        error_message: None,
    };
    store.insert_managed_postgres_branch(&branch).await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "managed_postgres_branch.create",
            &branch_id,
        ))
        .await?;
    Ok(Json(BranchApiResponse { branch }))
}

async fn create_point_in_time_branch(
    store: &SharedSqlControlPlaneStore,
    actor_id: &str,
    source: &ManagedPostgresCluster,
    payload: &CreateBranchRequest,
) -> Result<Json<BranchApiResponse>, SqlApiError> {
    if payload.parent_branch_id.is_some() {
        return Err(SqlApiError::BadRequest(
            "point-in-time branches are created from the cluster's backups, not a parent branch"
                .to_owned(),
        ));
    }
    let recovery_target_lsn = payload.recovery_target_lsn.clone().ok_or_else(|| {
        SqlApiError::BadRequest("point-in-time branch requires recovery_target_lsn".to_owned())
    })?;
    // A point-in-time branch recovers from the cluster's object-store backups,
    // so at least one succeeded backup must exist.
    store
        .latest_succeeded_backup_for_cluster(&source.cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::BadRequest("cluster has no succeeded backup".to_owned()))?;

    if let Some(policy_id) = payload.redaction_policy_id.as_deref() {
        let policy = store
            .managed_postgres_clone_redaction_policy(policy_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(policy_id.to_owned()))?;
        if policy.organization_id != source.organization_id
            || policy.project_id != source.project_id
            || policy.environment_id != source.environment_id
            || policy.status != CloneRedactionPolicyStatus::Active
        {
            return Err(SqlApiError::BadRequest(
                "branch redaction policy must be active and scoped to the source environment"
                    .to_owned(),
            ));
        }
        // CloudNativePG recovery restores data verbatim and cannot apply a
        // redaction policy; reject rather than expose unmasked data.
        return Err(SqlApiError::BadRequest(
            "clone redaction policies are not yet supported on the Kubernetes runtime".to_owned(),
        ));
    }

    let source_database = payload
        .source_database
        .clone()
        .unwrap_or_else(|| "postgres".to_owned());
    validate_database_identifier(&source_database)?;

    let branch_id = branch_id_for(&source.cluster_id, &payload.name);
    let target_cluster_id = format!(
        "{}_branch_{}_{}",
        source.cluster_id,
        sanitize_identifier_component(&payload.name),
        monotonic_nanos()
    );
    // CloudNativePG places the branch cluster; it recovers the source's backups
    // to the requested LSN.
    let target = ManagedPostgresCluster {
        cluster_id: target_cluster_id.clone(),
        organization_id: source.organization_id.clone(),
        project_id: source.project_id.clone(),
        environment_id: source.environment_id.clone(),
        region: source.region.clone(),
        postgres_version: source.postgres_version.clone(),
        tier: source.tier.clone(),
        storage_gib: source.storage_gib,
        lifecycle_state: ClusterLifecycleState::Restoring,
        host_assignment: None,
    };
    let restore_source = palimpsest_paas_runtime::RestoreSource {
        source_cluster_id: source.cluster_id.clone(),
        recovery_target_time: None,
        recovery_target_lsn: Some(recovery_target_lsn.clone()),
        continuous_replica: false,
    };
    runtime::ClusterRuntime::from_env()
        .restore(&target, &restore_source)
        .map_err(runtime_error)?;
    let branch = ManagedPostgresBranch {
        branch_id: branch_id.clone(),
        cluster_id: source.cluster_id.clone(),
        name: payload.name.clone(),
        parent_branch_id: None,
        mode: BranchMode::PointInTime,
        source_database,
        branch_database: None,
        branch_cluster_id: Some(target_cluster_id),
        created_from_lsn: Some(recovery_target_lsn),
        redaction_policy_id: None,
        lifecycle_state: BranchLifecycleState::Creating,
        error_message: None,
    };

    store.insert_managed_postgres_cluster(&target).await?;
    store.insert_managed_postgres_branch(&branch).await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "managed_postgres_branch.create",
            &branch_id,
        ))
        .await?;
    if payload.provision_sync_deployment {
        provision_branch_sync_deployment(store, actor_id, &branch).await?;
    }
    Ok(Json(BranchApiResponse { branch }))
}

/// Creates and starts a `SyncDeployment` for a branch's dedicated cluster. Only
/// point-in-time branches (which run as their own cluster) carry a
/// `branch_cluster_id`; for any other branch this is a no-op.
async fn provision_branch_sync_deployment(
    store: &SharedSqlControlPlaneStore,
    actor_id: &str,
    branch: &ManagedPostgresBranch,
) -> Result<(), SqlApiError> {
    let Some(branch_cluster_id) = branch.branch_cluster_id.clone() else {
        return Ok(());
    };
    let source = store
        .managed_postgres_cluster(&branch.cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(branch.cluster_id.clone()))?;
    let deployment = SyncDeployment {
        deployment_id: format!("{}:sync", branch.branch_id),
        organization_id: source.organization_id.clone(),
        project_id: source.project_id.clone(),
        environment_id: source.environment_id.clone(),
        managed_postgres_cluster_id: branch_cluster_id,
        config_version: "branch-default".to_owned(),
        lifecycle_state: SyncDeploymentLifecycleState::Requested,
    };
    store.insert_sync_deployment(&deployment).await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "sync_deployment.create",
            &deployment.deployment_id,
        ))
        .await?;
    // No-op until the restored branch cluster reaches Ready.
    enqueue_sync_deployment_start_if_ready(store, actor_id, &deployment).await?;
    Ok(())
}

async fn sql_api_list_managed_postgres_branches(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Json<BranchesResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let branches = store.managed_postgres_branches(&cluster_id).await?;
    Ok(Json(BranchesResponse { branches }))
}

async fn sql_api_get_managed_postgres_branch(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, branch_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresBranch>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let branch = store
        .managed_postgres_branch(&branch_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(branch_id.clone()))?;
    if branch.cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(branch_id));
    }
    Ok(Json(branch))
}

async fn sql_api_delete_managed_postgres_branch(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, branch_id)): Path<(String, String)>,
) -> Result<Json<BranchApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let cluster = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&cluster))?;
    let branch = store
        .managed_postgres_branch(&branch_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(branch_id.clone()))?;
    if branch.cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(branch_id));
    }

    if store
        .managed_postgres_branch_child_count(&branch_id)
        .await?
        > 0
    {
        return Err(SqlApiError::Conflict(
            "branch has child branches; delete them first".to_owned(),
        ));
    }

    // Tear down by backing resource: HEAD branches drop a database on the
    // shared cluster; point-in-time branches delete their dedicated cluster.
    let runtime = runtime::ClusterRuntime::from_env();
    match (
        branch.branch_database.clone(),
        branch.branch_cluster_id.clone(),
    ) {
        (Some(branch_database), _) => {
            runtime
                .drop_database(&cluster, &branch_database)
                .map_err(runtime_error)?;
        }
        (None, Some(branch_cluster_id)) => {
            let mut branch_cluster = store
                .managed_postgres_cluster(&branch_cluster_id)
                .await?
                .ok_or_else(|| SqlApiError::MissingResource(branch_cluster_id.clone()))?;
            let manifests = runtime
                .render(&branch_cluster, None, &[])
                .map_err(runtime_error)?;
            runtime.delete(&manifests).map_err(runtime_error)?;
            branch_cluster.lifecycle_state = ClusterLifecycleState::Deleted;
            store
                .update_managed_postgres_cluster(&branch_cluster)
                .await?;
        }
        (None, None) => {
            return Err(SqlApiError::BadRequest(
                "branch has no backing resource to delete".to_owned(),
            ));
        }
    }

    store
        .update_managed_postgres_branch_state(&branch_id, BranchLifecycleState::Deleting, None)
        .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_branch.delete",
            &branch_id,
        ))
        .await?;

    let branch = store
        .managed_postgres_branch(&branch_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(branch_id.clone()))?;
    Ok(Json(BranchApiResponse { branch }))
}

async fn sql_api_list_managed_postgres_restores(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresRestoresQuery>,
) -> Result<Json<ManagedPostgresRestoresResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let restores = store
        .managed_postgres_restores(&sql_store::ManagedPostgresRestoreFilter {
            source_cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresRestoresResponse { restores }))
}

async fn sql_api_get_managed_postgres_restore(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, restore_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresRestore>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let restore = store
        .managed_postgres_restore(&restore_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(restore_id.clone()))?;
    if restore.source_cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(restore_id));
    }
    Ok(Json(restore))
}

async fn sql_api_request_managed_postgres_restore_drill(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Json(payload): Json<RestoreDrillRequest>,
) -> Result<Json<RestoreDrillApiResponse>, SqlApiError> {
    let auth = sql_mutation_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let response = queue_managed_postgres_restore_drill(
        &store,
        &source,
        payload.backup_id.as_deref(),
        payload.target_cluster_id.as_deref(),
        payload.recovery_target_lsn,
    )
    .await?;
    store
        .append_audit_event(&audit_event(
            auth.actor_id(),
            "managed_postgres_restore_drill.request",
            &response.drill.drill_id,
        ))
        .await?;
    Ok(Json(response))
}

async fn sql_api_list_managed_postgres_restore_drills(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ManagedPostgresRestoreDrillsQuery>,
) -> Result<Json<ManagedPostgresRestoreDrillsResponse>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let drills = store
        .managed_postgres_restore_drills(&sql_store::ManagedPostgresRestoreDrillFilter {
            source_cluster_id: &cluster_id,
            status: query.status,
        })
        .await?;
    Ok(Json(ManagedPostgresRestoreDrillsResponse { drills }))
}

async fn sql_api_get_managed_postgres_restore_drill(
    State(store): State<SharedSqlControlPlaneStore>,
    headers: HeaderMap,
    Path((cluster_id, drill_id)): Path<(String, String)>,
) -> Result<Json<ManagedPostgresRestoreDrill>, SqlApiError> {
    let auth = sql_read_auth_context(&store, &headers).await?;
    let source = store
        .managed_postgres_cluster(&cluster_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(cluster_id.clone()))?;
    auth.require_scope(ResourceScope::from_cluster(&source))?;
    let drill = store
        .managed_postgres_restore_drill(&drill_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(drill_id.clone()))?;
    if drill.source_cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(drill_id));
    }
    Ok(Json(drill))
}

async fn sql_api_config_diff(
    State(store): State<SharedSqlControlPlaneStore>,
    Query(query): Query<ConfigDiffQuery>,
) -> Result<Json<ConfigDiff>, SqlApiError> {
    let old = store
        .config_version(&query.old)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(query.old.clone()))?;
    let new = store
        .config_version(&query.new)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(query.new.clone()))?;

    Ok(Json(ConfigDiff {
        old_version: old.config_version,
        new_version: new.config_version,
        rendered_hash_changed: old.rendered_hash != new.rendered_hash,
        status_changed: old.status != new.status,
    }))
}

fn wal_archive_root_for_data_dir(data_dir: &str, cluster_id: &str) -> Result<PathBuf, SqlApiError> {
    let data_dir = FsPath::new(data_dir);
    let Some(postgres_root) = data_dir.parent() else {
        return Err(SqlApiError::BadRequest(format!(
            "data dir has no parent: {}",
            data_dir.display()
        )));
    };
    let Some(runtime_root) = postgres_root.parent() else {
        return Err(SqlApiError::BadRequest(format!(
            "postgres root has no parent: {}",
            postgres_root.display()
        )));
    };
    Ok(runtime_root.join("wal").join(cluster_id))
}

fn validate_wal_segment_name(value: &str) -> Result<(), SqlApiError> {
    if value.len() == 24 && value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(SqlApiError::BadRequest(format!(
            "invalid WAL segment name: {value}"
        )))
    }
}

fn sibling_cluster_data_dir(
    data_dir: &str,
    target_cluster_id: &str,
) -> Result<PathBuf, SqlApiError> {
    let data_dir = FsPath::new(data_dir);
    let Some(postgres_root) = data_dir.parent() else {
        return Err(SqlApiError::BadRequest(format!(
            "data dir has no parent: {}",
            data_dir.display()
        )));
    };
    Ok(postgres_root.join(target_cluster_id))
}

fn restore_command_for_data_dir(data_dir: &str, cluster_id: &str) -> Result<String, SqlApiError> {
    let data_dir = FsPath::new(data_dir);
    let Some(postgres_root) = data_dir.parent() else {
        return Err(SqlApiError::BadRequest(format!(
            "data dir has no parent: {}",
            data_dir.display()
        )));
    };
    let Some(runtime_root) = postgres_root.parent() else {
        return Err(SqlApiError::BadRequest(format!(
            "postgres root has no parent: {}",
            postgres_root.display()
        )));
    };
    Ok(format!(
        "cp {}/wal/{}/%f %p",
        runtime_root.display(),
        sanitize_identifier_component(cluster_id)
    ))
}

async fn queue_managed_postgres_backup(
    store: &sql_store::SqlControlPlaneStore,
    cluster: &ManagedPostgresCluster,
) -> Result<BackupApiResponse, SqlApiError> {
    let backup_id = format!("backup_{}", monotonic_nanos());

    // Apply a CloudNativePG `Backup` resource; the operator streams a base
    // backup to the cluster's configured object store. We record the Backup
    // resource name so its status can be reconciled back later.
    let runtime = runtime::ClusterRuntime::from_env();
    let backup_resource = palimpsest_paas_runtime::backup_resource_name(cluster, &backup_id);
    runtime
        .create_backup(cluster, &backup_id)
        .map_err(runtime_error)?;

    let backup = ManagedPostgresBackup {
        backup_id,
        cluster_id: cluster.cluster_id.clone(),
        status: BackupLifecycleState::Running,
        backup_dir: backup_resource,
        error_message: None,
    };
    store.insert_managed_postgres_backup(&backup).await?;

    Ok(BackupApiResponse { backup })
}

async fn queue_managed_postgres_backup_cleanup(
    store: &sql_store::SqlControlPlaneStore,
    tombstone: &ManagedPostgresDeletionTombstone,
    backup_id: &str,
) -> Result<Option<(NodeAgentCommand, OperationRecord)>, SqlApiError> {
    let Some(backup) = store.managed_postgres_backup(backup_id).await? else {
        return Ok(None);
    };
    if backup.status == BackupLifecycleState::Deleted {
        return Ok(None);
    }
    let Some(cluster) = store
        .managed_postgres_cluster(&tombstone.cluster_id)
        .await?
    else {
        return Ok(None);
    };
    let Some(assignment) = cluster.host_assignment.as_ref() else {
        return Ok(None);
    };
    let backup_dir = FsPath::new(&backup.backup_dir)
        .join(sanitize_identifier_component(&backup.backup_id))
        .display()
        .to_string();
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:delete-backup-data:{}:{}",
            tombstone.cluster_id,
            backup.backup_id,
            monotonic_nanos()
        ),
        cluster_id: tombstone.cluster_id.clone(),
        action: NodeAgentAction::DeleteBackupData {
            backup_id: backup.backup_id.clone(),
            backup_dir,
        },
    };
    let operation = operation_for_agent_command(OperationKind::DeleteBackup, backup_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .expire_backup_artifacts_for_backup(&backup.backup_id)
        .await?;
    Ok(Some((command, operation)))
}

async fn queue_managed_postgres_backup_retention_cleanup(
    store: &sql_store::SqlControlPlaneStore,
    backup: &ManagedPostgresBackup,
) -> Result<Option<(NodeAgentCommand, OperationRecord)>, SqlApiError> {
    if backup.status == BackupLifecycleState::Deleted {
        return Ok(None);
    }
    let Some(cluster) = store.managed_postgres_cluster(&backup.cluster_id).await? else {
        return Ok(None);
    };
    let Some(assignment) = cluster.host_assignment.as_ref() else {
        return Ok(None);
    };
    let backup_dir = FsPath::new(&backup.backup_dir)
        .join(sanitize_identifier_component(&backup.backup_id))
        .display()
        .to_string();
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:expire-backup-data:{}:{}",
            backup.cluster_id,
            backup.backup_id,
            monotonic_nanos()
        ),
        cluster_id: backup.cluster_id.clone(),
        action: NodeAgentAction::DeleteBackupData {
            backup_id: backup.backup_id.clone(),
            backup_dir,
        },
    };
    let operation =
        operation_for_agent_command(OperationKind::DeleteBackup, &backup.backup_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .expire_backup_artifacts_for_backup(&backup.backup_id)
        .await?;
    Ok(Some((command, operation)))
}

async fn queue_managed_postgres_minor_update(
    store: &sql_store::SqlControlPlaneStore,
    cluster: &ManagedPostgresCluster,
    target_postgres_version: PostgresVersion,
    actor_id: &str,
    audit_action: &str,
) -> Result<UpdatePostgresMinorApiResponse, SqlApiError> {
    let mut cluster = cluster.clone();

    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be updated from {:?}",
            cluster.lifecycle_state
        )));
    }
    if target_postgres_version.major() != cluster.postgres_version.major() {
        return Err(SqlApiError::BadRequest(format!(
            "minor update must stay on PostgreSQL major {}",
            cluster.postgres_version.major()
        )));
    }
    if target_postgres_version == cluster.postgres_version {
        return Err(SqlApiError::BadRequest(format!(
            "cluster is already on PostgreSQL {}",
            cluster.postgres_version
        )));
    }

    // Record the desired version and re-apply. CloudNativePG performs a rolling
    // update of the instances to the new image.
    cluster.postgres_version = target_postgres_version;
    cluster.lifecycle_state = ClusterLifecycleState::UpdatingPostgres;
    store.update_managed_postgres_cluster(&cluster).await?;

    let runtime = runtime::ClusterRuntime::from_env();
    let roles = store
        .issue_database_role_credentials(&cluster.cluster_id)
        .await?;
    let manifests = runtime
        .render(&cluster, None, &roles)
        .map_err(runtime_error)?;
    runtime.apply(&manifests).map_err(runtime_error)?;

    cluster.lifecycle_state = ClusterLifecycleState::Verifying;
    store.update_managed_postgres_cluster(&cluster).await?;
    store
        .append_audit_event(&audit_event(actor_id, audit_action, &cluster.cluster_id))
        .await?;

    Ok(UpdatePostgresMinorApiResponse { cluster })
}

async fn queue_managed_postgres_major_upgrade(
    store: &sql_store::SqlControlPlaneStore,
    cluster: &ManagedPostgresCluster,
    target_postgres_version: PostgresVersion,
    strategy: ManagedPostgresMajorUpgradeStrategy,
    actor_id: &str,
) -> Result<RequestPostgresMajorUpgradeApiResponse, SqlApiError> {
    let mut cluster = cluster.clone();
    let assignment = cluster
        .host_assignment
        .clone()
        .ok_or_else(|| SqlApiError::BadRequest("cluster has no host assignment".to_owned()))?;

    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(format!(
            "cluster cannot be upgraded from {:?}",
            cluster.lifecycle_state
        )));
    }
    if target_postgres_version.major() <= cluster.postgres_version.major() {
        return Err(SqlApiError::BadRequest(format!(
            "major upgrade target must be greater than PostgreSQL major {}",
            cluster.postgres_version.major()
        )));
    }

    let host_is_active = store
        .active_host_capacities()
        .await?
        .into_iter()
        .any(|host| host.host_id == assignment.host_id);
    if !host_is_active {
        return Err(SqlApiError::BadRequest(format!(
            "assigned host {} is not active for major upgrade",
            assignment.host_id
        )));
    }

    let source_postgres_version = cluster.postgres_version.clone();
    let upgrade_id = format!("major_upgrade_{}", monotonic_nanos());
    cluster.postgres_version = target_postgres_version.clone();
    cluster.lifecycle_state = ClusterLifecycleState::UpdatingPostgres;
    let command = NodeAgentCommand {
        command_id: format!(
            "{}:upgrade-postgres-major:{}",
            cluster.cluster_id,
            monotonic_nanos()
        ),
        cluster_id: cluster.cluster_id.clone(),
        action: NodeAgentAction::UpgradePostgresMajor {
            data_dir: assignment.data_dir.clone(),
            source_port: Some(assignment.port),
            database: Some("postgres".to_owned()),
            source_postgres_version: source_postgres_version.clone(),
            target_postgres_version: target_postgres_version.clone(),
            strategy,
        },
    };
    let operation =
        operation_for_agent_command(OperationKind::UpdateCluster, &cluster.cluster_id, &command);
    let upgrade = ManagedPostgresMajorUpgrade {
        upgrade_id,
        cluster_id: cluster.cluster_id.clone(),
        source_postgres_version,
        target_postgres_version,
        strategy,
        status: ManagedPostgresMajorUpgradeStatus::Running,
        command_id: Some(command.command_id.clone()),
        operation_id: Some(operation.operation_id.clone()),
        error_message: None,
        created_at: String::new(),
        completed_at: None,
    };

    store.update_managed_postgres_cluster(&cluster).await?;
    store.insert_operation(&operation).await?;
    let upgrade = store
        .insert_managed_postgres_major_upgrade(&upgrade)
        .await?;
    store
        .enqueue_agent_command(&assignment.host_id, Some(&operation.operation_id), &command)
        .await?;
    store
        .append_audit_event(&audit_event(
            actor_id,
            "managed_postgres_cluster.upgrade_major",
            &cluster.cluster_id,
        ))
        .await?;

    Ok(RequestPostgresMajorUpgradeApiResponse {
        upgrade,
        cluster,
        command,
        operation,
    })
}

pub async fn run_backup_scheduler_once(
    store: &sql_store::SqlControlPlaneStore,
    actor_id: &str,
) -> Result<BackupSchedulerRunResponse, SqlApiError> {
    let clusters = store.ready_clusters_requiring_base_backup().await?;
    let mut scheduled = Vec::with_capacity(clusters.len());
    for cluster in clusters {
        let response = queue_managed_postgres_backup(store, &cluster).await?;
        store
            .append_audit_event(&audit_event(
                actor_id,
                "managed_postgres_backup.schedule",
                &response.backup.backup_id,
            ))
            .await?;
        scheduled.push(response);
    }

    Ok(BackupSchedulerRunResponse { scheduled })
}

pub async fn run_backup_retention_scheduler_once(
    store: &sql_store::SqlControlPlaneStore,
    actor_id: &str,
) -> Result<BackupRetentionSchedulerRunResponse, SqlApiError> {
    let backups = store
        .managed_postgres_backups_due_for_retention_expiration()
        .await?;
    let mut expired_backups = Vec::new();
    let mut cleanup_commands = Vec::new();
    let mut cleanup_operations = Vec::new();
    for backup in backups {
        if let Some((command, operation)) =
            queue_managed_postgres_backup_retention_cleanup(store, &backup).await?
        {
            store
                .append_audit_event(&audit_event(
                    actor_id,
                    "managed_postgres_backup_retention.expire",
                    &backup.backup_id,
                ))
                .await?;
            expired_backups.push(backup);
            cleanup_commands.push(command);
            cleanup_operations.push(operation);
        }
    }

    Ok(BackupRetentionSchedulerRunResponse {
        expired_backups,
        cleanup_commands,
        cleanup_operations,
    })
}

pub async fn run_restore_drill_scheduler_once(
    store: &sql_store::SqlControlPlaneStore,
    actor_id: &str,
    max_age_hours: Option<i64>,
) -> Result<RestoreDrillSchedulerRunResponse, SqlApiError> {
    let max_age_hours = max_age_hours.unwrap_or(24 * 7).max(1);
    let clusters = store.clusters_due_for_restore_drill(max_age_hours).await?;
    let mut scheduled = Vec::with_capacity(clusters.len());
    for cluster in clusters {
        let response =
            queue_managed_postgres_restore_drill(store, &cluster, None, None, None).await?;
        store
            .append_audit_event(&audit_event(
                actor_id,
                "managed_postgres_restore_drill.schedule",
                &response.drill.drill_id,
            ))
            .await?;
        scheduled.push(response);
    }

    Ok(RestoreDrillSchedulerRunResponse { scheduled })
}

pub async fn run_pitr_check_scheduler_once(
    store: &sql_store::SqlControlPlaneStore,
    actor_id: &str,
    max_age_hours: Option<i64>,
) -> Result<PitrCheckSchedulerRunResponse, SqlApiError> {
    let max_age_hours = max_age_hours.unwrap_or(24).max(1);
    let clusters = store.clusters_due_for_pitr_check(max_age_hours).await?;
    let mut checked = Vec::with_capacity(clusters.len());
    for cluster in clusters {
        let check = run_pitr_check_for_cluster(store, &cluster).await?;
        store
            .append_audit_event(&audit_event(
                actor_id,
                "managed_postgres_pitr_check.schedule",
                &check.check_id,
            ))
            .await?;
        checked.push(PitrCheckApiResponse { check });
    }

    Ok(PitrCheckSchedulerRunResponse { checked })
}

pub async fn run_acme_order_scheduler_once(
    store: &sql_store::SqlControlPlaneStore,
    actor_id: &str,
    limit: Option<i64>,
) -> Result<AcmeOrderSchedulerRunResponse, SqlApiError> {
    let orders = store
        .pending_managed_postgres_acme_orders(limit.unwrap_or(50))
        .await?;
    let mut checked = Vec::with_capacity(orders.len());
    for order in orders {
        let ready = store
            .validate_managed_postgres_acme_http_01_challenge(
                &order.environment_id,
                &order.certificate_id,
            )
            .await?;
        store
            .append_audit_event(&audit_event(
                actor_id,
                "managed_postgres_acme_order.schedule_validate",
                &ready.order_id,
            ))
            .await?;
        checked.push(ready);
    }

    Ok(AcmeOrderSchedulerRunResponse { checked })
}

pub async fn run_maintenance_scheduler_once(
    store: &sql_store::SqlControlPlaneStore,
    actor_id: &str,
    target_postgres_version: PostgresVersion,
    day_of_week: Option<MaintenanceDayOfWeek>,
    current_time: Option<&str>,
) -> Result<MaintenanceSchedulerRunResponse, SqlApiError> {
    let (day_of_week, current_time) = match (day_of_week, current_time) {
        (Some(day), Some(time)) => {
            validate_maintenance_clock_time(time)?;
            (day, time.to_owned())
        }
        (None, None) => current_utc_maintenance_clock(),
        _ => {
            return Err(SqlApiError::BadRequest(
                "maintenance scheduler requires both day_of_week and current_time when overriding the clock"
                    .to_owned(),
            ));
        }
    };
    let windows = store
        .maintenance_windows(&sql_store::MaintenanceWindowFilter {
            organization_id: None,
            project_id: None,
            environment_id: None,
            status: Some(MaintenanceWindowStatus::Active),
        })
        .await?;
    let clusters = store
        .managed_postgres_clusters(&sql_store::ManagedPostgresClusterFilter {
            organization_id: None,
            project_id: None,
            environment_id: None,
            lifecycle_state: Some(ClusterLifecycleState::Ready),
        })
        .await?;
    let mut scheduled_cluster_ids = BTreeSet::new();
    let mut scheduled = Vec::new();

    for window in windows
        .into_iter()
        .filter(|window| window.auto_minor_upgrades)
        .filter(|window| maintenance_window_is_open(window, day_of_week, &current_time))
    {
        for cluster in clusters
            .iter()
            .filter(|cluster| cluster.environment_id == window.environment_id)
            .filter(|cluster| cluster.postgres_version.major() == target_postgres_version.major())
            .filter(|cluster| cluster.postgres_version != target_postgres_version)
        {
            if !scheduled_cluster_ids.insert(cluster.cluster_id.clone()) {
                continue;
            }
            let update = queue_managed_postgres_minor_update(
                store,
                cluster,
                target_postgres_version.clone(),
                actor_id,
                "managed_postgres_cluster.maintenance_update_minor",
            )
            .await?;
            scheduled.push(MaintenanceSchedulerUpdate {
                window: window.clone(),
                update,
            });
        }
    }

    Ok(MaintenanceSchedulerRunResponse { scheduled })
}

async fn run_pitr_check_for_cluster(
    store: &sql_store::SqlControlPlaneStore,
    cluster: &ManagedPostgresCluster,
) -> Result<ManagedPostgresPitrCheck, SqlApiError> {
    if cluster.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(
            "PITR continuity check requires a ready cluster".to_owned(),
        ));
    }

    let backup = store
        .latest_succeeded_backup_for_cluster(&cluster.cluster_id)
        .await?;
    let segments = store
        .wal_archive_segments(&sql_store::WalArchiveSegmentFilter {
            cluster_id: &cluster.cluster_id,
            status: Some(WalArchiveSegmentStatus::Succeeded),
        })
        .await?;
    let segment_count = u32::try_from(segments.len())
        .map_err(|_| SqlApiError::BadRequest("too many WAL segments to summarize".to_owned()))?;

    let mut segment_names = segments
        .iter()
        .map(|segment| segment.segment_name.as_str())
        .collect::<Vec<_>>();
    segment_names.sort_unstable();
    let first_segment = segment_names.first().map(|segment| (*segment).to_owned());
    let latest_segment = segment_names.last().map(|segment| (*segment).to_owned());

    let mut errors = Vec::new();
    if backup.is_none() {
        errors.push("no succeeded base backup".to_owned());
    }
    if segment_names.is_empty() {
        errors.push("no succeeded WAL archive segments".to_owned());
    } else if let Err(err) = validate_wal_archive_continuity(&segment_names) {
        errors.push(err);
    }

    let check = ManagedPostgresPitrCheck {
        check_id: format!("pitr_check_{}", monotonic_nanos()),
        cluster_id: cluster.cluster_id.clone(),
        backup_id: backup.map(|backup| backup.backup_id),
        status: if errors.is_empty() {
            PitrCheckStatus::Succeeded
        } else {
            PitrCheckStatus::Failed
        },
        segment_count,
        first_segment,
        latest_segment,
        error_message: if errors.is_empty() {
            None
        } else {
            Some(errors.join("; "))
        },
    };
    store.insert_managed_postgres_pitr_check(&check).await?;
    Ok(check)
}

fn validate_wal_archive_continuity(segment_names: &[&str]) -> Result<(), String> {
    let mut previous: Option<WalSegmentSortKey<'_>> = None;
    for segment_name in segment_names {
        let current = wal_segment_sort_key(segment_name)
            .ok_or_else(|| format!("invalid WAL segment name {segment_name}"))?;
        if let Some(previous) = previous {
            if current.timeline != previous.timeline {
                return Err("WAL archive spans multiple timelines".to_owned());
            }
            if current.position != previous.position + 1 {
                return Err(format!(
                    "WAL archive gap between {} and {}",
                    previous.name, segment_name
                ));
            }
        }
        previous = Some(current);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct WalSegmentSortKey<'a> {
    name: &'a str,
    timeline: u32,
    position: u64,
}

fn wal_segment_sort_key(segment_name: &str) -> Option<WalSegmentSortKey<'_>> {
    if segment_name.len() != 24 {
        return None;
    }
    let timeline = u32::from_str_radix(&segment_name[0..8], 16).ok()?;
    let log = u32::from_str_radix(&segment_name[8..16], 16).ok()?;
    let segment = u32::from_str_radix(&segment_name[16..24], 16).ok()?;
    Some(WalSegmentSortKey {
        name: segment_name,
        timeline,
        position: (u64::from(log) << 32) | u64::from(segment),
    })
}

async fn queue_managed_postgres_restore_drill(
    store: &sql_store::SqlControlPlaneStore,
    source: &ManagedPostgresCluster,
    backup_id: Option<&str>,
    target_cluster_id: Option<&str>,
    recovery_target_lsn: Option<String>,
) -> Result<RestoreDrillApiResponse, SqlApiError> {
    if source.lifecycle_state != ClusterLifecycleState::Ready {
        return Err(SqlApiError::BadRequest(
            "restore drill requires a ready source cluster".to_owned(),
        ));
    }
    let source_assignment = source.host_assignment.as_ref().ok_or_else(|| {
        SqlApiError::BadRequest("source cluster has no host assignment".to_owned())
    })?;
    let backup = if let Some(backup_id) = backup_id {
        store
            .managed_postgres_backup(backup_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(backup_id.to_owned()))?
    } else {
        store
            .latest_succeeded_backup_for_cluster(&source.cluster_id)
            .await?
            .ok_or_else(|| SqlApiError::BadRequest("cluster has no succeeded backup".to_owned()))?
    };
    if backup.cluster_id != source.cluster_id || backup.status != BackupLifecycleState::Succeeded {
        return Err(SqlApiError::BadRequest(
            "restore drill requires a succeeded backup from the source cluster".to_owned(),
        ));
    }

    let drill_id = format!("restore_drill_{}", monotonic_nanos());
    let restore_id = format!("restore_{}", monotonic_nanos());
    let target_cluster_id = target_cluster_id.map_or_else(
        || format!("{}_drill_{}", source.cluster_id, monotonic_nanos()),
        str::to_owned,
    );
    let target_data_dir =
        sibling_cluster_data_dir(&source_assignment.data_dir, &target_cluster_id)?;
    let target_port = store
        .next_available_host_port(&source_assignment.host_id)
        .await?;
    let target = ManagedPostgresCluster {
        cluster_id: target_cluster_id.clone(),
        organization_id: source.organization_id.clone(),
        project_id: source.project_id.clone(),
        environment_id: source.environment_id.clone(),
        region: source.region.clone(),
        postgres_version: source.postgres_version.clone(),
        tier: source.tier.clone(),
        storage_gib: source.storage_gib,
        lifecycle_state: ClusterLifecycleState::Restoring,
        host_assignment: Some(HostAssignment {
            host_id: source_assignment.host_id.clone(),
            data_dir: target_data_dir.display().to_string(),
            port: target_port,
        }),
    };
    let restore = ManagedPostgresRestore {
        restore_id: restore_id.clone(),
        source_cluster_id: source.cluster_id.clone(),
        target_cluster_id: target_cluster_id.clone(),
        target_environment_id: source.environment_id.clone(),
        backup_id: backup.backup_id.clone(),
        status: RestoreLifecycleState::Running,
        redaction_policy_id: None,
        recovery_target_lsn: recovery_target_lsn.clone(),
        error_message: None,
    };
    let drill = ManagedPostgresRestoreDrill {
        drill_id,
        source_cluster_id: source.cluster_id.clone(),
        restore_id: restore_id.clone(),
        backup_id: backup.backup_id.clone(),
        target_cluster_id: target_cluster_id.clone(),
        status: RestoreLifecycleState::Running,
        error_message: None,
    };
    let command = NodeAgentCommand {
        command_id: format!("{target_cluster_id}:prepare-restore-drill:{restore_id}"),
        cluster_id: target_cluster_id.clone(),
        action: NodeAgentAction::PrepareRestore {
            backup_id: backup.backup_id.clone(),
            backup_dir: backup.backup_dir.clone(),
            data_dir: target_data_dir.display().to_string(),
            target_port: Some(target_port),
            database: Some("postgres".to_owned()),
            restore_command: restore_command_for_data_dir(
                &source_assignment.data_dir,
                &source.cluster_id,
            )?,
            recovery_target_lsn,
            redaction_policy: None,
        },
    };

    store.insert_managed_postgres_cluster(&target).await?;
    store.insert_managed_postgres_restore(&restore).await?;
    store.insert_managed_postgres_restore_drill(&drill).await?;
    let operation =
        operation_for_agent_command(OperationKind::RestoreCluster, &drill.drill_id, &command);
    store.insert_operation(&operation).await?;
    store
        .enqueue_agent_command(
            &source_assignment.host_id,
            Some(&operation.operation_id),
            &command,
        )
        .await?;

    Ok(RestoreDrillApiResponse {
        drill,
        restore,
        cluster: target,
        command,
        operation,
    })
}

fn render_control_plane_metrics(snapshot: &sql_store::ControlPlaneMetricsSnapshot) -> String {
    let mut metrics = format!(
        "palimpsest_paas_agent_commands_failed_total {}\n\
         palimpsest_paas_billing_exports_failed_total {}\n\
         palimpsest_managed_postgres_wal_archives_failed_total {}\n\
         palimpsest_paas_quota_alerts_firing_total {}\n",
        snapshot.agent_commands_failed_total,
        snapshot.billing_exports_failed_total,
        snapshot.wal_archive_failures_total,
        snapshot.quota_alerts_firing_total
    );

    for backup in &snapshot.last_successful_backups {
        if let Some(timestamp_seconds) = backup.timestamp_seconds {
            metrics.push_str(&format!(
                "palimpsest_managed_postgres_last_successful_backup_timestamp_seconds{{cluster_id=\"{}\"}} {}\n",
                escape_metric_label(&backup.cluster_id),
                timestamp_seconds
            ));
        }
    }

    for lifecycle in &snapshot.cluster_lifecycle_states {
        metrics.push_str(&format!(
            "palimpsest_managed_postgres_cluster_lifecycle_state{{cluster_id=\"{}\",environment_id=\"{}\",state=\"{}\"}} 1\n",
            escape_metric_label(&lifecycle.cluster_id),
            escape_metric_label(&lifecycle.environment_id),
            escape_metric_label(&lifecycle.lifecycle_state)
        ));
    }

    for storage in &snapshot.cluster_storage_allocations {
        metrics.push_str(&format!(
            "palimpsest_managed_postgres_storage_allocated_gib{{cluster_id=\"{}\",environment_id=\"{}\",host_id=\"{}\"}} {}\n",
            escape_metric_label(&storage.cluster_id),
            escape_metric_label(&storage.environment_id),
            escape_metric_label(storage.host_id.as_deref().unwrap_or("")),
            storage.storage_gib
        ));
    }

    for wal_archive in &snapshot.last_successful_wal_archives {
        if let Some(timestamp_seconds) = wal_archive.timestamp_seconds {
            metrics.push_str(&format!(
                "palimpsest_managed_postgres_last_successful_wal_archive_timestamp_seconds{{cluster_id=\"{}\"}} {}\n",
                escape_metric_label(&wal_archive.cluster_id),
                timestamp_seconds
            ));
        }
    }

    for pitr_check in &snapshot.last_successful_pitr_checks {
        if let Some(timestamp_seconds) = pitr_check.timestamp_seconds {
            metrics.push_str(&format!(
                "palimpsest_managed_postgres_last_successful_pitr_check_timestamp_seconds{{cluster_id=\"{}\"}} {}\n",
                escape_metric_label(&pitr_check.cluster_id),
                timestamp_seconds
            ));
        }
    }

    for restore_drill in &snapshot.last_successful_restore_drills {
        if let Some(timestamp_seconds) = restore_drill.timestamp_seconds {
            metrics.push_str(&format!(
                "palimpsest_managed_postgres_last_successful_restore_drill_timestamp_seconds{{cluster_id=\"{}\"}} {}\n",
                escape_metric_label(&restore_drill.cluster_id),
                timestamp_seconds
            ));
        }
    }

    for standby_check in &snapshot.last_successful_standby_checks {
        if let Some(timestamp_seconds) = standby_check.timestamp_seconds {
            metrics.push_str(&format!(
                "palimpsest_managed_postgres_last_successful_standby_check_timestamp_seconds{{cluster_id=\"{}\"}} {}\n",
                escape_metric_label(&standby_check.cluster_id),
                timestamp_seconds
            ));
        }
    }

    for quota in &snapshot.quota_usage_ratios {
        metrics.push_str(&format!(
            "palimpsest_paas_quota_usage_ratio{{environment_id=\"{}\",metric=\"{}\"}} {}\n",
            escape_metric_label(&quota.environment_id),
            escape_metric_label(&quota.metric),
            quota.ratio
        ));
    }

    for storage in &snapshot.storage_used_ratios {
        metrics.push_str(&format!(
            "palimpsest_managed_postgres_storage_used_ratio{{host_id=\"{}\"}} {}\n",
            escape_metric_label(&storage.host_id),
            storage.ratio
        ));
    }

    metrics
}

fn escape_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn audit_event(actor_id: &str, action: &str, resource_id: &str) -> AuditEvent {
    AuditEvent {
        event_id: format!("audit_{}", monotonic_nanos()),
        actor_id: actor_id.to_owned(),
        action: action.to_owned(),
        resource_id: resource_id.to_owned(),
        occurred_at: "now".to_owned(),
    }
}

fn operation_for_agent_command(
    kind: OperationKind,
    target_resource_id: &str,
    command: &NodeAgentCommand,
) -> OperationRecord {
    OperationRecord {
        operation_id: format!("op_{}", monotonic_nanos()),
        idempotency_key: format!("{}:{}", operation_kind_slug(kind), command.command_id),
        target_resource_id: target_resource_id.to_owned(),
        kind,
        status: OperationStatus::Running,
        current_step: node_agent_action_step(&command.action).to_owned(),
        lease_owner: None,
    }
}

const fn operation_kind_slug(kind: OperationKind) -> &'static str {
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
        OperationKind::CreateBranch => "create_branch",
        OperationKind::DeleteBranch => "delete_branch",
        OperationKind::PrepareStandby => "prepare_standby",
        OperationKind::CheckStandby => "check_standby",
        OperationKind::FencePrimary => "fence_primary",
        OperationKind::FailoverCluster => "failover_cluster",
        OperationKind::DeleteCluster => "delete_cluster",
        OperationKind::StartSyncDeployment => "start_sync_deployment",
        OperationKind::StopSyncDeployment => "stop_sync_deployment",
    }
}

const fn node_agent_action_step(action: &NodeAgentAction) -> &'static str {
    match action {
        NodeAgentAction::PreparePostgres { .. } => "prepare_postgres",
        NodeAgentAction::StartPostgres => "start_postgres",
        NodeAgentAction::StopPostgres => "stop_postgres",
        NodeAgentAction::DeletePostgresData { .. } => "delete_postgres_data",
        NodeAgentAction::ConfigurePostgresAccess { .. } => "configure_postgres_access",
        NodeAgentAction::CreateCopyOnWriteDatabaseClone { .. } => {
            "create_copy_on_write_database_clone"
        }
        NodeAgentAction::DropDatabase { .. } => "drop_database",
        NodeAgentAction::ReportStatus => "report_status",
        NodeAgentAction::CheckPostgresStandbyLag { .. } => "check_postgres_standby_lag",
        NodeAgentAction::RunBaseBackup { .. } => "run_base_backup",
        NodeAgentAction::DeleteBackupData { .. } => "delete_backup_data",
        NodeAgentAction::PrepareRestore { .. } => "prepare_restore",
        NodeAgentAction::PreparePostgresStandby { .. } => "prepare_postgres_standby",
        NodeAgentAction::ArchiveWalSegment { .. } => "archive_wal_segment",
        NodeAgentAction::PromotePostgresStandby { .. } => "promote_postgres_standby",
        NodeAgentAction::FencePostgresPrimary { .. } => "fence_postgres_primary",
        NodeAgentAction::ResizePostgresStorage { .. } => "resize_postgres_storage",
        NodeAgentAction::UpdatePostgresMinor { .. } => "update_postgres_minor",
        NodeAgentAction::UpgradePostgresMajor { .. } => "upgrade_postgres_major",
        NodeAgentAction::StartSyncDeployment { .. } => "start_sync_deployment",
        NodeAgentAction::StopSyncDeployment { .. } => "stop_sync_deployment",
        NodeAgentAction::ReportSyncDeployment { .. } => "report_sync_deployment",
    }
}

fn generate_api_key_token() -> Result<String, SqlApiError> {
    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| SqlApiError::BadRequest("failed to generate api key token".to_owned()))?;
    Ok(format!("plmp_{}", BASE64_STANDARD.encode(bytes)))
}

fn token_prefix(token: &str) -> String {
    token.chars().take(17).collect()
}

fn api_key_token_hash(token: &str) -> String {
    BASE64_STANDARD.encode(digest::digest(&digest::SHA256, token.as_bytes()).as_ref())
}

fn verify_usage_event_signature_from_env(event: &UsageEvent) -> Result<(), SqlApiError> {
    let Some(raw_key) = std::env::var("PALIMPSEST_PAAS_USAGE_EVENT_SIGNING_KEY_BASE64")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(());
    };
    let key = BASE64_STANDARD.decode(raw_key.as_bytes()).map_err(|err| {
        SqlApiError::BadRequest(format!("invalid usage-event signing key: {err}"))
    })?;
    if key.len() < 32 {
        return Err(SqlApiError::BadRequest(
            "usage-event signing key must decode to at least 32 bytes".to_owned(),
        ));
    }
    let expected_key_id = std::env::var("PALIMPSEST_PAAS_USAGE_EVENT_SIGNING_KEY_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    verify_usage_event_signature(event, &key, expected_key_id.as_deref())
}

fn verify_usage_event_signature(
    event: &UsageEvent,
    signing_key: &[u8],
    expected_key_id: Option<&str>,
) -> Result<(), SqlApiError> {
    let signature = event.signature.as_ref().ok_or(SqlApiError::Unauthorized)?;
    if signature.algorithm != "hmac_sha256_v1" {
        return Err(SqlApiError::Unauthorized);
    }
    if let Some(expected_key_id) = expected_key_id {
        if signature.key_id != expected_key_id {
            return Err(SqlApiError::Unauthorized);
        }
    }
    let signature_bytes = BASE64_STANDARD
        .decode(signature.signature.as_bytes())
        .map_err(|_| SqlApiError::Unauthorized)?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    let canonical = usage_event_signature_canonical(event);
    hmac::verify(&key, canonical.as_bytes(), &signature_bytes)
        .map_err(|_| SqlApiError::Unauthorized)
}

#[cfg(test)]
fn sign_usage_event(
    event: &UsageEvent,
    signing_key: &[u8],
    key_id: impl Into<String>,
) -> palimpsest_paas_core::UsageEventSignature {
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    palimpsest_paas_core::UsageEventSignature {
        key_id: key_id.into(),
        algorithm: "hmac_sha256_v1".to_owned(),
        signature: BASE64_STANDARD
            .encode(hmac::sign(&key, usage_event_signature_canonical(event).as_bytes()).as_ref()),
    }
}

fn usage_event_signature_canonical(event: &UsageEvent) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        event.event_id,
        event.idempotency_key,
        event.organization_id,
        event.project_id,
        event.environment_id,
        event.metric,
        event.quantity,
        event.occurred_at
    )
}

fn host_part(addr: &str) -> &str {
    addr.rsplit_once(':').map_or(addr, |(host, _port)| host)
}

fn monotonic_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

fn actor_id(headers: &HeaderMap) -> String {
    headers
        .get("x-actor-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("system")
        .to_owned()
}

#[derive(Debug, Clone)]
struct SqlAuthContext {
    actor_id: String,
    api_key: Option<ApiKey>,
}

impl SqlAuthContext {
    fn actor_id(&self) -> &str {
        &self.actor_id
    }

    fn require_scope(&self, scope: ResourceScope<'_>) -> Result<(), SqlApiError> {
        let Some(api_key) = self.api_key.as_ref() else {
            return Ok(());
        };

        if api_key
            .organization_id
            .as_deref()
            .is_some_and(|organization_id| scope.organization_id != Some(organization_id))
        {
            return Err(SqlApiError::Unauthorized);
        }
        if api_key
            .project_id
            .as_deref()
            .is_some_and(|project_id| scope.project_id != Some(project_id))
        {
            return Err(SqlApiError::Unauthorized);
        }
        if api_key
            .environment_id
            .as_deref()
            .is_some_and(|environment_id| scope.environment_id != Some(environment_id))
        {
            return Err(SqlApiError::Unauthorized);
        }

        Ok(())
    }

    fn require_api_key_admin(&self) -> Result<(), SqlApiError> {
        if self
            .api_key
            .as_ref()
            .is_some_and(|api_key| !api_key_role_can_manage_api_keys(api_key.role))
        {
            return Err(SqlApiError::Unauthorized);
        }
        Ok(())
    }

    const fn require_can_manage_role(&self, target_role: TeamRole) -> Result<(), SqlApiError> {
        let Some(api_key) = self.api_key.as_ref() else {
            return Ok(());
        };
        if !api_key_role_can_manage_role(api_key.role, target_role) {
            return Err(SqlApiError::Unauthorized);
        }
        Ok(())
    }

    fn require_platform_admin(&self) -> Result<(), SqlApiError> {
        if self
            .api_key
            .as_ref()
            .is_some_and(|api_key| !api_key_role_can_manage_platform(api_key.role))
        {
            return Err(SqlApiError::Unauthorized);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ResourceScope<'a> {
    organization_id: Option<&'a str>,
    project_id: Option<&'a str>,
    environment_id: Option<&'a str>,
}

impl<'a> ResourceScope<'a> {
    const fn new(
        organization_id: Option<&'a str>,
        project_id: Option<&'a str>,
        environment_id: Option<&'a str>,
    ) -> Self {
        Self {
            organization_id,
            project_id,
            environment_id,
        }
    }

    const fn organization(organization_id: &'a str) -> Self {
        Self::new(Some(organization_id), None, None)
    }

    const fn project(organization_id: &'a str, project_id: &'a str) -> Self {
        Self::new(Some(organization_id), Some(project_id), None)
    }

    const fn environment(
        organization_id: &'a str,
        project_id: &'a str,
        environment_id: &'a str,
    ) -> Self {
        Self::new(
            Some(organization_id),
            Some(project_id),
            Some(environment_id),
        )
    }

    fn from_cluster(cluster: &'a ManagedPostgresCluster) -> Self {
        Self::environment(
            &cluster.organization_id,
            &cluster.project_id,
            &cluster.environment_id,
        )
    }

    fn from_deletion_tombstone(tombstone: &'a ManagedPostgresDeletionTombstone) -> Self {
        Self::environment(
            &tombstone.organization_id,
            &tombstone.project_id,
            &tombstone.environment_id,
        )
    }

    fn from_api_key(api_key: &'a ApiKey) -> Self {
        Self::new(
            api_key.organization_id.as_deref(),
            api_key.project_id.as_deref(),
            api_key.environment_id.as_deref(),
        )
    }
}

#[derive(Debug, Clone)]
struct OwnedResourceScope {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
}

impl OwnedResourceScope {
    fn as_resource_scope(&self) -> ResourceScope<'_> {
        ResourceScope::new(
            self.organization_id.as_deref(),
            self.project_id.as_deref(),
            self.environment_id.as_deref(),
        )
    }
}

async fn resolve_resource_scope(
    store: &sql_store::SqlControlPlaneStore,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    environment_id: Option<&str>,
) -> Result<OwnedResourceScope, SqlApiError> {
    if let Some(environment_id) = environment_id {
        let (actual_organization_id, actual_project_id, actual_environment_id) = store
            .environment_scope(environment_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(environment_id.to_owned()))?;
        ensure_scope_component_matches(
            "organization_id",
            organization_id,
            &actual_organization_id,
        )?;
        ensure_scope_component_matches("project_id", project_id, &actual_project_id)?;
        return Ok(OwnedResourceScope {
            organization_id: Some(actual_organization_id),
            project_id: Some(actual_project_id),
            environment_id: Some(actual_environment_id),
        });
    }

    if let Some(project_id) = project_id {
        let (actual_organization_id, actual_project_id) = store
            .project_scope(project_id)
            .await?
            .ok_or_else(|| SqlApiError::MissingResource(project_id.to_owned()))?;
        ensure_scope_component_matches(
            "organization_id",
            organization_id,
            &actual_organization_id,
        )?;
        return Ok(OwnedResourceScope {
            organization_id: Some(actual_organization_id),
            project_id: Some(actual_project_id),
            environment_id: None,
        });
    }

    Ok(OwnedResourceScope {
        organization_id: organization_id.map(str::to_owned),
        project_id: None,
        environment_id: None,
    })
}

async fn scoped_query_from_auth(
    store: &sql_store::SqlControlPlaneStore,
    auth: &SqlAuthContext,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    environment_id: Option<&str>,
) -> Result<OwnedResourceScope, SqlApiError> {
    let has_explicit_scope =
        organization_id.is_some() || project_id.is_some() || environment_id.is_some();
    if has_explicit_scope {
        let scope =
            resolve_resource_scope(store, organization_id, project_id, environment_id).await?;
        auth.require_scope(scope.as_resource_scope())?;
        return Ok(scope);
    }

    if let Some(api_key) = auth.api_key.as_ref() {
        return Ok(OwnedResourceScope {
            organization_id: api_key.organization_id.clone(),
            project_id: api_key.project_id.clone(),
            environment_id: api_key.environment_id.clone(),
        });
    }

    Ok(OwnedResourceScope {
        organization_id: None,
        project_id: None,
        environment_id: None,
    })
}

fn ensure_scope_component_matches(
    field: &str,
    requested: Option<&str>,
    actual: &str,
) -> Result<(), SqlApiError> {
    if requested.is_some_and(|requested| requested != actual) {
        return Err(SqlApiError::BadRequest(format!(
            "{field} does not match resolved resource scope"
        )));
    }
    Ok(())
}

async fn sql_mutation_auth_context(
    store: &sql_store::SqlControlPlaneStore,
    headers: &HeaderMap,
) -> Result<SqlAuthContext, SqlApiError> {
    let auth = sql_read_auth_context(store, headers).await?;
    if auth
        .api_key
        .as_ref()
        .is_some_and(|api_key| !api_key_role_can_mutate(api_key.role))
    {
        return Err(SqlApiError::Unauthorized);
    }
    Ok(auth)
}

async fn sql_read_auth_context(
    store: &sql_store::SqlControlPlaneStore,
    headers: &HeaderMap,
) -> Result<SqlAuthContext, SqlApiError> {
    let Some(token) = bearer_api_key_token(headers) else {
        return Ok(SqlAuthContext {
            actor_id: actor_id(headers),
            api_key: None,
        });
    };
    let hash = api_key_token_hash(token);
    let api_key = store
        .api_key_by_token_hash(&hash)
        .await?
        .ok_or(SqlApiError::Unauthorized)?;
    Ok(SqlAuthContext {
        actor_id: format!("api_key:{}", api_key.key_id),
        api_key: Some(api_key),
    })
}

fn bearer_api_key_token(headers: &HeaderMap) -> Option<&str> {
    optional_header(headers, header::AUTHORIZATION.as_str())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| token.starts_with("plmp_"))
}

const fn api_key_role_can_mutate(role: TeamRole) -> bool {
    !matches!(role, TeamRole::Viewer)
}

const fn api_key_role_can_manage_api_keys(role: TeamRole) -> bool {
    matches!(role, TeamRole::Owner | TeamRole::Admin)
}

const fn api_key_role_can_manage_platform(role: TeamRole) -> bool {
    matches!(role, TeamRole::Owner | TeamRole::Admin)
}

const fn api_key_role_can_manage_role(actor_role: TeamRole, target_role: TeamRole) -> bool {
    match actor_role {
        TeamRole::Owner => true,
        TeamRole::Admin => matches!(
            target_role,
            TeamRole::Developer | TeamRole::Viewer | TeamRole::Ci
        ),
        TeamRole::Developer | TeamRole::Viewer | TeamRole::Ci => false,
    }
}

async fn require_agent_auth(
    store: &sql_store::SqlControlPlaneStore,
    headers: &HeaderMap,
    scope: AgentAuthScope<'_>,
) -> Result<(), SqlApiError> {
    let expected_token = std::env::var("PALIMPSEST_PAAS_AGENT_TOKEN").ok();
    let signing_key = agent_signing_key_from_env()?;
    if let Some(key_id) = optional_header(headers, "x-palimpsest-agent-key-id") {
        let signing_key = store
            .node_host_agent_signing_key(scope.host_id, key_id)
            .await
            .map_err(|err| match err {
                sql_store::SqlStoreError::MissingResource(_)
                | sql_store::SqlStoreError::InvalidValue(_) => SqlApiError::Unauthorized,
                other => SqlApiError::Store(other),
            })?;
        validate_signed_agent_auth(
            headers,
            signing_key.as_slice(),
            scope,
            unix_timestamp_seconds(),
        )?;
        store
            .record_node_host_agent_credential_use(scope.host_id, key_id, scope.operation)
            .await?;
        return Ok(());
    }
    validate_agent_auth(
        headers,
        expected_token.as_deref(),
        signing_key.as_deref(),
        scope,
    )
}

#[derive(Debug, Clone, Copy)]
struct AgentAuthScope<'a> {
    host_id: &'a str,
    operation: &'a str,
}

impl<'a> AgentAuthScope<'a> {
    const fn new(host_id: &'a str, operation: &'a str) -> Self {
        Self { host_id, operation }
    }
}

fn validate_agent_auth(
    headers: &HeaderMap,
    expected_token: Option<&str>,
    signing_key: Option<&[u8]>,
    scope: AgentAuthScope<'_>,
) -> Result<(), SqlApiError> {
    if let Some(signing_key) = signing_key {
        return validate_signed_agent_auth(headers, signing_key, scope, unix_timestamp_seconds());
    }

    let Some(expected_token) = expected_token.filter(|token| !token.trim().is_empty()) else {
        return Ok(());
    };

    let expected = format!("Bearer {expected_token}");
    let actual = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if actual == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(SqlApiError::Unauthorized)
    }
}

fn agent_signing_key_from_env() -> Result<Option<Vec<u8>>, SqlApiError> {
    let Some(raw) = std::env::var("PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let key = BASE64_STANDARD
        .decode(raw.as_bytes())
        .map_err(|err| SqlApiError::BadRequest(format!("invalid node-agent signing key: {err}")))?;
    if key.len() < 32 {
        return Err(SqlApiError::BadRequest(
            "node-agent signing key must decode to at least 32 bytes".to_owned(),
        ));
    }
    Ok(Some(key))
}

fn validate_signed_agent_auth(
    headers: &HeaderMap,
    signing_key: &[u8],
    scope: AgentAuthScope<'_>,
    now: u64,
) -> Result<(), SqlApiError> {
    let header_host_id = required_header(headers, "x-palimpsest-agent-host-id")?;
    let header_operation = required_header(headers, "x-palimpsest-agent-operation")?;
    let timestamp = required_header(headers, "x-palimpsest-agent-timestamp")?
        .parse::<u64>()
        .map_err(|_| SqlApiError::Unauthorized)?;
    let signature = required_header(headers, "x-palimpsest-agent-signature")?;

    if header_host_id != scope.host_id || header_operation != scope.operation {
        return Err(SqlApiError::Unauthorized);
    }
    if !timestamp_within_skew(timestamp, now, 300) {
        return Err(SqlApiError::Unauthorized);
    }

    let signature = BASE64_STANDARD
        .decode(signature.as_bytes())
        .map_err(|_| SqlApiError::Unauthorized)?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    let canonical = agent_auth_canonical(scope.host_id, scope.operation, timestamp);
    hmac::verify(&key, canonical.as_bytes(), &signature).map_err(|_| SqlApiError::Unauthorized)
}

fn required_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, SqlApiError> {
    optional_header(headers, name).ok_or(SqlApiError::Unauthorized)
}

fn optional_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
}

const fn timestamp_within_skew(timestamp: u64, now: u64, allowed_skew_seconds: u64) -> bool {
    now.abs_diff(timestamp) <= allowed_skew_seconds
}

#[cfg(test)]
fn sign_agent_request(key: &[u8], host_id: &str, operation: &str, timestamp: u64) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let canonical = agent_auth_canonical(host_id, operation, timestamp);
    BASE64_STANDARD.encode(hmac::sign(&key, canonical.as_bytes()).as_ref())
}

fn agent_auth_canonical(host_id: &str, operation: &str, timestamp: u64) -> String {
    format!("{host_id}\n{operation}\n{timestamp}")
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct ConfigDiffQuery {
    old: String,
    new: String,
}

#[derive(Debug, Deserialize)]
struct ConfigVersionsQuery {
    environment_id: String,
}

#[derive(Debug, Serialize)]
struct ConfigVersionsResponse {
    configs: Vec<ConfigVersion>,
}

#[derive(Debug, Default, Deserialize)]
struct RollbackConfigRequest {
    config_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct RollbackConfigResponse {
    config: ConfigVersion,
}

#[derive(Debug, Serialize)]
struct EnvironmentOverviewResponse {
    organization_id: String,
    project_id: String,
    environment_id: String,
    health: CustomerEnvironmentHealth,
    #[serde(skip_serializing_if = "Option::is_none")]
    managed_postgres_endpoint: Option<ManagedPostgresEndpoint>,
    managed_postgres_clusters: Vec<ManagedPostgresCluster>,
    sync_deployments: Vec<SyncDeployment>,
    configs: Vec<ConfigVersion>,
    quota_alerts: Vec<QuotaAlert>,
    domains: Vec<Domain>,
    ip_allowlist_rules: Vec<IpAllowlistRule>,
    static_egress_ips: Vec<StaticEgressIp>,
    maintenance_windows: Vec<MaintenanceWindow>,
    incidents: Vec<Incident>,
}

#[derive(Debug, Deserialize)]
struct OnboardingWorkspaceRequest {
    organization: Organization,
    project: Project,
    environment: Environment,
    owner_actor_id: String,
}

#[derive(Debug, Serialize)]
struct OnboardingWorkspaceResponse {
    organization: Organization,
    project: Project,
    environment: Environment,
    owner_membership: TeamMembership,
}

#[derive(Debug, Deserialize)]
struct EnqueueAgentCommandRequest {
    operation_id: Option<String>,
    command: NodeAgentCommand,
}

#[derive(Debug, Deserialize)]
struct SetNodeHostStateRequest {
    state: NodeHostState,
}

#[derive(Debug, Default, Deserialize)]
struct NodeHostsQuery {
    region: Option<String>,
    failure_domain: Option<String>,
    state: Option<NodeHostState>,
}

#[derive(Debug, Serialize)]
struct NodeHostsResponse {
    hosts: Vec<NodeHost>,
}

#[derive(Debug, Default, Deserialize)]
struct NodeHostAgentCredentialsQuery {
    state: Option<NodeHostAgentCredentialState>,
}

#[derive(Debug, Serialize)]
struct NodeHostAgentCredentialsResponse {
    credentials: Vec<NodeHostAgentCredential>,
}

#[derive(Debug, Default, Deserialize)]
struct NodeHostHardeningChecksQuery {
    status: Option<NodeHostHardeningStatus>,
}

#[derive(Debug, Serialize)]
struct NodeHostHardeningChecksResponse {
    checks: Vec<NodeHostHardeningCheck>,
}

#[derive(Debug, Default, Deserialize)]
struct RestoreClusterRequest {
    target_cluster_id: Option<String>,
    target_environment_id: Option<String>,
    backup_id: Option<String>,
    redaction_policy_id: Option<String>,
    recovery_target_lsn: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DatabaseCloneRequest {
    source_database: String,
    target_database: String,
    #[serde(default)]
    terminate_source_connections: bool,
}

#[derive(Debug, Deserialize)]
struct WalArchiveRequest {
    segment_name: String,
}

#[derive(Debug, Default, Deserialize)]
struct WalArchivesQuery {
    status: Option<WalArchiveSegmentStatus>,
}

#[derive(Debug, Serialize)]
struct WalArchivesResponse {
    segments: Vec<ManagedPostgresWalArchiveSegment>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresPitrChecksQuery {
    status: Option<PitrCheckStatus>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresPitrChecksResponse {
    checks: Vec<ManagedPostgresPitrCheck>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresFailoversQuery {
    status: Option<FailoverLifecycleState>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresFailoversResponse {
    failovers: Vec<ManagedPostgresFailover>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresStandbysQuery {
    status: Option<StandbyLifecycleState>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresStandbysResponse {
    standbys: Vec<ManagedPostgresStandby>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresStandbyChecksQuery {
    status: Option<StandbyCheckStatus>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresStandbyChecksResponse {
    checks: Vec<ManagedPostgresStandbyCheck>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresRuntimeChecksQuery {
    status: Option<RuntimeCheckStatus>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresRuntimeChecksResponse {
    checks: Vec<ManagedPostgresRuntimeCheck>,
}

#[derive(Debug, Deserialize)]
struct UsageEventsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    metric: Option<String>,
    occurred_at_from: Option<String>,
    occurred_at_to: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct QuotaPoliciesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    metric: Option<String>,
}

#[derive(Debug, Serialize)]
struct QuotaPoliciesResponse {
    policies: Vec<QuotaPolicy>,
}

#[derive(Debug, Deserialize)]
struct QuotaAlertRequest {
    alert_id: String,
    policy_id: String,
    threshold_basis_points: u32,
}

#[derive(Debug, Default, Deserialize)]
struct QuotaAlertsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    metric: Option<String>,
    state: Option<QuotaAlertState>,
}

#[derive(Debug, Serialize)]
struct QuotaAlertsResponse {
    alerts: Vec<QuotaAlert>,
}

#[derive(Debug, Deserialize)]
struct BillingExportRequest {
    export_id: String,
    destination: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    metric: Option<String>,
    occurred_at_from: Option<String>,
    occurred_at_to: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct BillingExportsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    metric: Option<String>,
    status: Option<BillingExportStatus>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct BillingExportsResponse {
    exports: Vec<BillingExport>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresClustersQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    lifecycle_state: Option<ClusterLifecycleState>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresClustersResponse {
    clusters: Vec<ManagedPostgresCluster>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresDeletionTombstonesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    expired: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresDeletionTombstonesResponse {
    tombstones: Vec<ManagedPostgresDeletionTombstone>,
}

#[derive(Debug, Serialize)]
struct ExpireManagedPostgresDeletionTombstonesResponse {
    tombstones: Vec<ManagedPostgresDeletionTombstone>,
    cleanup_commands: Vec<NodeAgentCommand>,
    cleanup_operations: Vec<OperationRecord>,
}

#[derive(Debug, Default, Deserialize)]
struct SyncDeploymentsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    lifecycle_state: Option<SyncDeploymentLifecycleState>,
}

#[derive(Debug, Serialize)]
struct SyncDeploymentsResponse {
    deployments: Vec<SyncDeployment>,
}

#[derive(Debug, Default, Deserialize)]
struct GatewayRoutesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct GatewayRoutesResponse {
    routes: Vec<GatewayRoute>,
}

#[derive(Debug, Default, Deserialize)]
struct DomainsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    verification_status: Option<DomainVerificationStatus>,
    tls_status: Option<DomainTlsStatus>,
}

#[derive(Debug, Serialize)]
struct DomainsResponse {
    domains: Vec<Domain>,
}

#[derive(Debug, Default, Deserialize)]
struct IpAllowlistRulesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    purpose: Option<IpAllowlistPurpose>,
    status: Option<IpAllowlistStatus>,
}

#[derive(Debug, Serialize)]
struct IpAllowlistRulesResponse {
    rules: Vec<IpAllowlistRule>,
}

#[derive(Debug, Default, Deserialize)]
struct StaticEgressIpsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    region: Option<String>,
    status: Option<StaticEgressIpStatus>,
}

#[derive(Debug, Serialize)]
struct StaticEgressIpsResponse {
    egress_ips: Vec<StaticEgressIp>,
}

#[derive(Debug, Default, Deserialize)]
struct MaintenanceWindowsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    status: Option<MaintenanceWindowStatus>,
}

#[derive(Debug, Serialize)]
struct MaintenanceWindowsResponse {
    windows: Vec<MaintenanceWindow>,
}

#[derive(Debug, Default, Deserialize)]
struct DatabaseProxyRoutesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct DatabaseProxyRoutesResponse {
    routes: Vec<DatabaseProxyRoute>,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresSchemaResponse {
    schemas: Vec<ManagedPostgresSchema>,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresSchema {
    name: String,
    tables: Vec<ManagedPostgresTable>,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresTable {
    name: String,
    columns: Vec<ManagedPostgresColumn>,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresColumn {
    name: String,
    data_type: String,
    nullable: bool,
    ordinal_position: i32,
}

#[derive(Debug, Default, Deserialize)]
pub struct ManagedPostgresDatabaseQuery {
    database: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ManagedPostgresSqlQueryRequest {
    sql: String,
    limit: Option<u32>,
    database: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresSqlQueryResponse {
    rows: Vec<JsonValue>,
    row_count: u32,
    truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresSampleDataSeedResponse {
    /// Per-table row counts that were inserted.
    pub tables: Vec<ManagedPostgresSampleDataTable>,
    /// Total rows across all tables.
    pub total_rows: u32,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresSampleDataTable {
    pub name: String,
    pub rows: u32,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresQueryExplorerResponse {
    canonical_sql: String,
    statement_kind: String,
    referenced_tables: Vec<String>,
    sharing_behavior: String,
    estimated_startup_cost: Option<f64>,
    estimated_total_cost: Option<f64>,
    estimated_rows: Option<f64>,
    explain_plan: JsonValue,
}

#[derive(Debug, Default, Deserialize)]
struct QueryPermissionPoliciesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    table_schema: Option<String>,
    table_name: Option<String>,
    operation: Option<QueryPermissionOperation>,
    status: Option<QueryPermissionPolicyStatus>,
}

#[derive(Debug, Serialize)]
struct QueryPermissionPoliciesResponse {
    policies: Vec<QueryPermissionPolicy>,
}

#[derive(Debug, Deserialize)]
struct QueryPermissionPolicyDryRunRequest {
    sample_context: Option<JsonValue>,
}

#[derive(Debug, Serialize)]
struct QueryPermissionPolicyDryRunResponse {
    policy_id: String,
    accepted: bool,
    table_schema: String,
    table_name: String,
    operation: QueryPermissionOperation,
    checked_predicate_sql: String,
    sample_context: JsonValue,
    decision_detail: String,
}

#[derive(Debug, Deserialize)]
struct PermissionRuleDocumentSaveRequest {
    dsl: String,
}

#[derive(Debug, Deserialize)]
struct PermissionVerifyRequest {
    dsl: String,
    /// Optional inline catalog (table/column schema) to compile against. When
    /// absent or empty, the built-in demo catalog is used. The UI populates
    /// this from a selected cluster's live schema.
    #[serde(default)]
    catalog: Option<Vec<PermissionVerifyCatalogTable>>,
}

#[derive(Debug, Deserialize)]
struct PermissionVerifyCatalogTable {
    name: String,
    #[serde(default)]
    columns: Vec<PermissionVerifyCatalogColumn>,
}

#[derive(Debug, Deserialize)]
struct PermissionVerifyCatalogColumn {
    name: String,
    #[serde(rename = "type", default)]
    ty: String,
}

#[derive(Debug, Serialize)]
struct PermissionVerifyResponse {
    /// True when the DSL parsed and every rule compiled against the catalog.
    ok: bool,
    /// First parse or compile error, verbatim, when `ok` is false.
    error: Option<String>,
    /// Which catalog the rules were compiled against (`demo` or `inline`).
    catalog_source: String,
    /// Table names available in the catalog, for operator reference.
    catalog_tables: Vec<String>,
    /// User-context fields declared by the document.
    user_context: Vec<palimpsest_permissions::UserContextField>,
    /// Per-rule results. Populated with compiled detail when `ok`, otherwise
    /// the parsed rules (without canonical predicates).
    rules: Vec<PermissionVerifyRule>,
}

#[derive(Debug, Serialize)]
struct PermissionVerifyRule {
    name: String,
    table: String,
    mode: palimpsest_permissions::Mode,
    predicate: String,
    /// Canonical predicate text with `$user.*` references encoded as
    /// sentinels; present only when the rule compiled.
    canonical: Option<String>,
    /// User-context fields the predicate reads.
    user_fields: Vec<String>,
    /// True when the predicate is a tautology the rewriter would elide.
    tautology: bool,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresOperationsQuery {
    kind: Option<OperationKind>,
    status: Option<OperationStatus>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresOperationsResponse {
    operations: Vec<OperationRecord>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresAgentCommandsQuery {
    status: Option<AgentCommandStatus>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresAgentCommandsResponse {
    commands: Vec<QueuedNodeAgentCommand>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresBackupsQuery {
    status: Option<BackupLifecycleState>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresBackupArtifactsQuery {
    status: Option<BackupArtifactStatus>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresBackupsResponse {
    backups: Vec<ManagedPostgresBackup>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresBackupArtifactsResponse {
    artifacts: Vec<ManagedPostgresBackupArtifact>,
}

#[derive(Debug, Deserialize)]
struct UpsertBackupRetentionPolicyRequest {
    retention_days: u32,
    keep_min_successful_backups: u32,
    enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct CloneRedactionPoliciesQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    status: Option<CloneRedactionPolicyStatus>,
}

#[derive(Debug, Serialize)]
struct CloneRedactionPoliciesResponse {
    policies: Vec<ManagedPostgresCloneRedactionPolicy>,
}

#[derive(Debug, Deserialize)]
struct CreateSupportAccessSessionRequest {
    reason: String,
    ticket_ref: Option<String>,
    duration_minutes: u32,
}

#[derive(Debug, Default, Deserialize)]
struct SupportAccessSessionsQuery {
    status: Option<SupportAccessStatus>,
}

#[derive(Debug, Serialize)]
struct SupportAccessSessionsResponse {
    sessions: Vec<ManagedPostgresSupportAccessSession>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresRestoresQuery {
    status: Option<RestoreLifecycleState>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresRestoresResponse {
    restores: Vec<ManagedPostgresRestore>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresRestoreDrillsQuery {
    status: Option<RestoreLifecycleState>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresRestoreDrillsResponse {
    drills: Vec<ManagedPostgresRestoreDrill>,
}

#[derive(Debug, Deserialize)]
struct TeamMembershipsQuery {
    organization_id: String,
}

#[derive(Debug, Serialize)]
struct TeamMembershipsResponse {
    memberships: Vec<TeamMembership>,
}

#[derive(Debug, Default, Deserialize)]
struct AuditEventsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    actor_id: Option<String>,
    action: Option<String>,
    resource_id: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct AuditEventsResponse {
    events: Vec<AuditEvent>,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    name: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    role: TeamRole,
}

#[derive(Debug, Serialize)]
pub struct CreateApiKeyResponse {
    api_key: ApiKey,
    token: String,
}

#[derive(Debug, Default, Deserialize)]
struct ApiKeysQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    include_revoked: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApiKeysResponse {
    api_keys: Vec<ApiKey>,
}

#[derive(Debug, Default, Deserialize)]
struct SecretEncryptionKeysQuery {
    provider: Option<String>,
    purpose: Option<String>,
    status: Option<SecretEncryptionKeyStatus>,
}

#[derive(Debug, Serialize)]
struct SecretEncryptionKeysResponse {
    keys: Vec<SecretEncryptionKey>,
}

#[derive(Debug, Deserialize)]
struct CreateSecretRewrapPlanRequest {
    source_key_ref: String,
    target_key_ref: String,
}

#[derive(Debug, Default, Deserialize)]
struct SecretRewrapPlansQuery {
    source_key_ref: Option<String>,
    target_key_ref: Option<String>,
    status: Option<SecretRewrapPlanStatus>,
}

#[derive(Debug, Serialize)]
struct SecretRewrapPlansResponse {
    plans: Vec<SecretRewrapPlan>,
}

#[derive(Debug, Default, Deserialize)]
struct JwtIssuersQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    status: Option<JwtIssuerStatus>,
}

#[derive(Debug, Serialize)]
struct JwtIssuersResponse {
    issuers: Vec<JwtIssuer>,
}

#[derive(Debug, Default, Deserialize)]
struct WebhookEndpointsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    status: Option<WebhookEndpointStatus>,
}

#[derive(Debug, Serialize)]
struct WebhookEndpointsResponse {
    endpoints: Vec<WebhookEndpoint>,
}

#[derive(Debug, Default, Deserialize)]
struct SsoIdentityProvidersQuery {
    organization_id: String,
    kind: Option<SsoProviderKind>,
    status: Option<SsoProviderStatus>,
}

#[derive(Debug, Serialize)]
struct SsoIdentityProvidersResponse {
    providers: Vec<SsoIdentityProvider>,
}

#[derive(Debug, Default, Deserialize)]
struct IncidentsQuery {
    organization_id: Option<String>,
    project_id: Option<String>,
    environment_id: Option<String>,
    severity: Option<IncidentSeverity>,
    status: Option<IncidentStatus>,
    include_resolved: Option<bool>,
}

#[derive(Debug, Serialize)]
struct IncidentsResponse {
    incidents: Vec<Incident>,
}

#[derive(Debug, Serialize)]
pub struct ReconcileApiResponse {
    cluster: ManagedPostgresCluster,
    /// The CloudNativePG manifests applied this pass (YAML), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    manifests: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ConfigureManagedPostgresDatabaseProxyRouteRequest {
    listen_addr: String,
}

#[derive(Debug, Serialize)]
pub struct ConfigureManagedPostgresDatabaseProxyRouteResponse {
    endpoint: ManagedPostgresEndpoint,
    route: DatabaseProxyRoute,
}

#[derive(Debug, Serialize)]
pub struct DeconfigureManagedPostgresDatabaseProxyRouteResponse {
    endpoint: ManagedPostgresEndpoint,
    removed_listen_addr: Option<String>,
    revoked_certificate_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct IssueManagedPostgresEndpointCertificateRequest {
    common_name: Option<String>,
    validity_days: Option<u32>,
    ca_provider_id: Option<String>,
    certificate_pem: Option<String>,
    private_key_pem: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RenewManagedPostgresEndpointCertificateRequest {
    renewal_window_days: Option<u32>,
    force: Option<bool>,
    validity_days: Option<u32>,
    ca_provider_id: Option<String>,
    certificate_pem: Option<String>,
    private_key_pem: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FinalizeManagedPostgresAcmeOrderRequest {
    certificate_pem: String,
}

#[derive(Debug, Deserialize)]
pub struct UpsertManagedPostgresCertificateAuthorityProviderRequest {
    ca_provider_id: String,
    name: String,
    kind: CertificateAuthorityProviderKind,
    issuer_ref: String,
    status: Option<CertificateAuthorityProviderStatus>,
    default_for_managed_postgres: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresCertificateAuthorityProvidersQuery {
    status: Option<CertificateAuthorityProviderStatus>,
    default_for_managed_postgres: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresCertificateAuthorityProvidersResponse {
    providers: Vec<ManagedPostgresCertificateAuthorityProvider>,
}

#[derive(Debug, Default, Deserialize)]
struct ManagedPostgresEndpointCertificatesQuery {
    status: Option<CertificateLifecycleState>,
}

#[derive(Debug, Serialize)]
struct ManagedPostgresEndpointCertificatesResponse {
    certificates: Vec<ManagedPostgresEndpointCertificate>,
}

#[derive(Debug, Serialize)]
pub struct StopClusterApiResponse {
    cluster: ManagedPostgresCluster,
}

#[derive(Debug, Serialize)]
pub struct StartClusterApiResponse {
    cluster: ManagedPostgresCluster,
}

#[derive(Debug, Deserialize)]
pub struct ResizeClusterRequest {
    storage_gib: u32,
}

#[derive(Debug, Serialize)]
pub struct ResizeClusterApiResponse {
    cluster: ManagedPostgresCluster,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePostgresMinorRequest {
    target_postgres_version: PostgresVersion,
}

#[derive(Debug, Serialize)]
pub struct UpdatePostgresMinorApiResponse {
    cluster: ManagedPostgresCluster,
}

#[derive(Debug, Serialize)]
pub struct RotateDatabaseRoleCredentialsResponse {
    cluster: ManagedPostgresCluster,
    command_id: String,
    operation: OperationRecord,
}

#[derive(Debug, Deserialize)]
pub struct RequestPostgresMajorUpgradeRequest {
    target_postgres_version: PostgresVersion,
    strategy: Option<ManagedPostgresMajorUpgradeStrategy>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ManagedPostgresMajorUpgradesQuery {
    status: Option<ManagedPostgresMajorUpgradeStatus>,
}

#[derive(Debug, Serialize)]
pub struct RequestPostgresMajorUpgradeApiResponse {
    upgrade: ManagedPostgresMajorUpgrade,
    cluster: ManagedPostgresCluster,
    command: NodeAgentCommand,
    operation: OperationRecord,
}

#[derive(Debug, Serialize)]
pub struct ManagedPostgresMajorUpgradesResponse {
    upgrades: Vec<ManagedPostgresMajorUpgrade>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteClusterRequest {
    final_backup: Option<bool>,
    tombstone_retention_days: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct DeleteClusterApiResponse {
    cluster: ManagedPostgresCluster,
    // command + operation are absent when the cluster had no host
    // assignment to dispatch the delete agent command to (i.e. the
    // request was cancelled before any data was written to a host).
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<NodeAgentCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation: Option<OperationRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_backup: Option<ManagedPostgresBackup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_backup_command: Option<NodeAgentCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_backup_operation: Option<OperationRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_command: Option<NodeAgentCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_operation: Option<OperationRecord>,
}

#[derive(Debug, Serialize)]
pub struct BackupApiResponse {
    backup: ManagedPostgresBackup,
}

#[derive(Debug, Serialize)]
pub struct BackupSchedulerRunResponse {
    pub scheduled: Vec<BackupApiResponse>,
}

#[derive(Debug, Serialize)]
pub struct BackupRetentionSchedulerRunResponse {
    expired_backups: Vec<ManagedPostgresBackup>,
    cleanup_commands: Vec<NodeAgentCommand>,
    cleanup_operations: Vec<OperationRecord>,
}

#[derive(Debug, Deserialize)]
pub struct MaintenanceSchedulerRunRequest {
    target_postgres_version: PostgresVersion,
    day_of_week: Option<MaintenanceDayOfWeek>,
    current_time: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MaintenanceSchedulerUpdate {
    window: MaintenanceWindow,
    update: UpdatePostgresMinorApiResponse,
}

#[derive(Debug, Serialize)]
pub struct MaintenanceSchedulerRunResponse {
    pub scheduled: Vec<MaintenanceSchedulerUpdate>,
}

#[derive(Debug, Serialize)]
pub struct WalArchiveApiResponse {
    segment: ManagedPostgresWalArchiveSegment,
    command: NodeAgentCommand,
    operation: OperationRecord,
}

#[derive(Debug, Serialize)]
pub struct RestoreApiResponse {
    restore: ManagedPostgresRestore,
    cluster: ManagedPostgresCluster,
}

#[derive(Debug, Serialize)]
pub struct DatabaseCloneApiResponse {
    cluster: ManagedPostgresCluster,
    source_database: String,
    target_database: String,
}

const fn default_branch_mode() -> BranchMode {
    BranchMode::HeadCow
}

#[derive(Debug, Deserialize)]
struct CreateBranchRequest {
    name: String,
    #[serde(default)]
    parent_branch_id: Option<String>,
    #[serde(default = "default_branch_mode")]
    mode: BranchMode,
    /// Source database for a root branch (no parent). Defaults to `postgres`.
    #[serde(default)]
    source_database: Option<String>,
    /// Recovery target LSN for point-in-time branches.
    #[serde(default)]
    recovery_target_lsn: Option<String>,
    #[serde(default)]
    redaction_policy_id: Option<String>,
    #[serde(default)]
    terminate_source_connections: bool,
    /// When set, also provision and start a `SyncDeployment` for the branch.
    /// Supported for point-in-time branches (which run as a dedicated cluster).
    #[serde(default)]
    provision_sync_deployment: bool,
}

#[derive(Debug, Serialize)]
pub struct BranchApiResponse {
    branch: ManagedPostgresBranch,
}

#[derive(Debug, Serialize)]
struct BranchesResponse {
    branches: Vec<ManagedPostgresBranch>,
}

#[derive(Debug, Deserialize)]
pub struct RestoreDrillRequest {
    backup_id: Option<String>,
    target_cluster_id: Option<String>,
    recovery_target_lsn: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RestoreDrillSchedulerRunRequest {
    max_age_hours: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct RestoreDrillApiResponse {
    drill: ManagedPostgresRestoreDrill,
    restore: ManagedPostgresRestore,
    cluster: ManagedPostgresCluster,
    command: NodeAgentCommand,
    operation: OperationRecord,
}

#[derive(Debug, Serialize)]
pub struct RestoreDrillSchedulerRunResponse {
    pub scheduled: Vec<RestoreDrillApiResponse>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PitrCheckSchedulerRunRequest {
    max_age_hours: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct PitrCheckApiResponse {
    check: ManagedPostgresPitrCheck,
}

#[derive(Debug, Serialize)]
pub struct PitrCheckSchedulerRunResponse {
    pub checked: Vec<PitrCheckApiResponse>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AcmeOrderSchedulerRunRequest {
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct AcmeOrderSchedulerRunResponse {
    pub checked: Vec<ManagedPostgresAcmeOrder>,
}

#[derive(Debug, Deserialize)]
pub struct FailoverClusterRequest {
    target_cluster_id: Option<String>,
    standby_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FailoverApiResponse {
    failover: ManagedPostgresFailover,
}

#[derive(Debug, Default, Deserialize)]
pub struct CreateStandbyRequest {
    backup_id: Option<String>,
    target_cluster_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StandbyApiResponse {
    standby: ManagedPostgresStandby,
    cluster: ManagedPostgresCluster,
}

#[derive(Debug, Default, Deserialize)]
pub struct CreateStandbyCheckRequest {
    max_lag_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct StandbyCheckApiResponse {
    check: ManagedPostgresStandbyCheck,
    command: NodeAgentCommand,
    operation: OperationRecord,
}

#[derive(Debug, Serialize)]
pub struct UsageEventsResponse {
    events: Vec<UsageEvent>,
}

async fn read_managed_postgres_schema(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
) -> Result<ManagedPostgresSchemaResponse, SqlApiError> {
    let pool = connect_managed_postgres(endpoint).await?;
    let rows = sqlx::query(
        "SELECT table_schema, table_name, column_name, data_type, is_nullable, ordinal_position \
         FROM information_schema.columns \
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
         ORDER BY table_schema, table_name, ordinal_position",
    )
    .fetch_all(&pool)
    .await
    .map_err(managed_postgres_sql_error)?;

    let mut schemas = BTreeMap::<String, BTreeMap<String, Vec<ManagedPostgresColumn>>>::new();
    for row in rows {
        let schema_name: String = row
            .try_get("table_schema")
            .map_err(managed_postgres_sql_error)?;
        let table_name: String = row
            .try_get("table_name")
            .map_err(managed_postgres_sql_error)?;
        let column_name: String = row
            .try_get("column_name")
            .map_err(managed_postgres_sql_error)?;
        let data_type: String = row
            .try_get("data_type")
            .map_err(managed_postgres_sql_error)?;
        let is_nullable: String = row
            .try_get("is_nullable")
            .map_err(managed_postgres_sql_error)?;
        let ordinal_position: i32 = row
            .try_get("ordinal_position")
            .map_err(managed_postgres_sql_error)?;
        schemas
            .entry(schema_name)
            .or_default()
            .entry(table_name)
            .or_default()
            .push(ManagedPostgresColumn {
                name: column_name,
                data_type,
                nullable: is_nullable == "YES",
                ordinal_position,
            });
    }

    Ok(ManagedPostgresSchemaResponse {
        schemas: schemas
            .into_iter()
            .map(|(name, tables)| ManagedPostgresSchema {
                name,
                tables: tables
                    .into_iter()
                    .map(|(name, columns)| ManagedPostgresTable { name, columns })
                    .collect(),
            })
            .collect(),
    })
}

async fn run_managed_postgres_read_only_query(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
    request: &ManagedPostgresSqlQueryRequest,
) -> Result<ManagedPostgresSqlQueryResponse, SqlApiError> {
    let limit = request.limit.unwrap_or(50).clamp(1, 100);
    let statement_sql = managed_postgres_console_statement(&request.sql);
    let pool = connect_managed_postgres(endpoint).await?;
    let mut tx = pool.begin().await.map_err(managed_postgres_sql_error)?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    sqlx::query("SET LOCAL statement_timeout = '5000ms'")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    sqlx::query("SET LOCAL idle_in_transaction_session_timeout = '5000ms'")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;

    let wrapped_sql = format!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(palimpsest_console_rows)), '[]'::jsonb) AS rows \
         FROM (SELECT * FROM ({statement_sql}) AS palimpsest_console_query LIMIT $1) palimpsest_console_rows"
    );
    let SqlJson(mut rows): SqlJson<Vec<JsonValue>> = sqlx::query_scalar(&wrapped_sql)
        .bind(i64::from(limit) + 1)
        .fetch_one(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    tx.rollback().await.map_err(managed_postgres_sql_error)?;

    let truncated = rows.len() > limit as usize;
    if truncated {
        rows.truncate(limit as usize);
    }
    let row_count = u32::try_from(rows.len()).unwrap_or(limit);
    Ok(ManagedPostgresSqlQueryResponse {
        rows,
        row_count,
        truncated,
    })
}

/// Seed a managed Postgres cluster with a tiny three-table sample
/// dataset (`sample_users`, `sample_posts`, `sample_comments`). Uses the
/// `migration` role so DDL is allowed; grants SELECT on the new tables
/// to the app role so the SQL console (which connects as `app`) can
/// read them back. Existing sample tables are dropped first — callers
/// must confirm in the UI.
async fn seed_managed_postgres_sample_data(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
    app_role: &str,
) -> Result<ManagedPostgresSampleDataSeedResponse, SqlApiError> {
    // The role names are constructed from the cluster id via
    // sanitize_identifier_component, so they're guaranteed safe for
    // identifier interpolation. Belt-and-suspenders: validate again.
    if app_role.is_empty()
        || !app_role
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(SqlApiError::BadRequest(format!(
            "invalid app role name {app_role}"
        )));
    }
    let pool = connect_managed_postgres(endpoint).await?;
    let mut tx = pool.begin().await.map_err(managed_postgres_sql_error)?;
    sqlx::query("SET LOCAL statement_timeout = '15000ms'")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;

    sqlx::query("DROP TABLE IF EXISTS sample_comments, sample_posts, sample_users CASCADE")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;

    sqlx::query(
        "CREATE TABLE sample_users (\
             id          bigserial PRIMARY KEY,\
             handle      text NOT NULL UNIQUE,\
             display_name text NOT NULL,\
             joined_at   timestamptz NOT NULL DEFAULT now()\
         )",
    )
    .execute(&mut *tx)
    .await
    .map_err(managed_postgres_sql_error)?;

    sqlx::query(
        "CREATE TABLE sample_posts (\
             id         bigserial PRIMARY KEY,\
             author_id  bigint NOT NULL REFERENCES sample_users(id) ON DELETE CASCADE,\
             title      text NOT NULL,\
             body       text NOT NULL,\
             published  boolean NOT NULL DEFAULT true,\
             created_at timestamptz NOT NULL DEFAULT now()\
         )",
    )
    .execute(&mut *tx)
    .await
    .map_err(managed_postgres_sql_error)?;

    sqlx::query(
        "CREATE TABLE sample_comments (\
             id         bigserial PRIMARY KEY,\
             post_id    bigint NOT NULL REFERENCES sample_posts(id) ON DELETE CASCADE,\
             author_id  bigint NOT NULL REFERENCES sample_users(id) ON DELETE CASCADE,\
             body       text NOT NULL,\
             created_at timestamptz NOT NULL DEFAULT now()\
         )",
    )
    .execute(&mut *tx)
    .await
    .map_err(managed_postgres_sql_error)?;

    let users_inserted: i64 = sqlx::query_scalar(
        "WITH inserted AS (\
             INSERT INTO sample_users (handle, display_name) VALUES \
                 ('alice', 'Alice Reyes'),\
                 ('bob',   'Bob Lin'),\
                 ('carol', 'Carol Singh'),\
                 ('dave',  'Dave Park'),\
                 ('eve',   'Eve Romero')\
             RETURNING 1\
         ) SELECT count(*)::bigint FROM inserted",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(managed_postgres_sql_error)?;

    let posts_inserted: i64 = sqlx::query_scalar(
        "WITH inserted AS (\
             INSERT INTO sample_posts (author_id, title, body, published) VALUES \
                 (1, 'Hello world',        'first post',                  true),\
                 (1, 'On migrations',      'thoughts on schema change',   true),\
                 (2, 'Replication notes',  'logical vs physical',         true),\
                 (2, 'Backups draft',      'not ready yet',               false),\
                 (3, 'PITR walkthrough',   'recovery target_time = ...',  true),\
                 (3, 'Quota policies',     'firing alerts and limits',    true),\
                 (4, 'Operator console',   'designing the IA',            true),\
                 (4, 'TLS rotation',       'cert lifecycle',              true),\
                 (5, 'Audit log review',   'patterns we look for',        true),\
                 (5, 'Sample data',        'seeded from the UI',          true)\
             RETURNING 1\
         ) SELECT count(*)::bigint FROM inserted",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(managed_postgres_sql_error)?;

    let comments_inserted: i64 = sqlx::query_scalar(
        "WITH inserted AS (\
             INSERT INTO sample_comments (post_id, author_id, body) VALUES \
                 (1,  2, 'welcome aboard'),\
                 (1,  3, 'nice'),\
                 (2,  4, 'agree on online DDL'),\
                 (3,  1, 'thanks for the writeup'),\
                 (3,  5, 'how does failover interact?'),\
                 (5,  2, 'pitr saved us last week'),\
                 (5,  4, 'restore drills FTW'),\
                 (6,  1, 'quota basis-points tripped me up'),\
                 (7,  3, 'looking forward to the rebuild'),\
                 (8,  5, 'rotate quarterly'),\
                 (9,  2, 'good catch on the actor field'),\
                 (10, 4, 'one click is sweet'),\
                 (10, 3, 'consider adding orders next'),\
                 (2,  5, 'multi-statement seed plz'),\
                 (4,  1, 'shipping monday')\
             RETURNING 1\
         ) SELECT count(*)::bigint FROM inserted",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(managed_postgres_sql_error)?;

    // Make the new tables visible/queryable through the SQL console,
    // which connects as the `app` role.
    let grant_sql =
        format!("GRANT SELECT ON sample_users, sample_posts, sample_comments TO \"{app_role}\"");
    sqlx::query(&grant_sql)
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    let grant_seq_sql = format!(
        "GRANT USAGE, SELECT ON SEQUENCE \
         sample_users_id_seq, sample_posts_id_seq, sample_comments_id_seq \
         TO \"{app_role}\""
    );
    sqlx::query(&grant_seq_sql)
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;

    tx.commit().await.map_err(managed_postgres_sql_error)?;

    let tables = vec![
        ManagedPostgresSampleDataTable {
            name: "sample_users".to_owned(),
            rows: u32::try_from(users_inserted).unwrap_or(0),
        },
        ManagedPostgresSampleDataTable {
            name: "sample_posts".to_owned(),
            rows: u32::try_from(posts_inserted).unwrap_or(0),
        },
        ManagedPostgresSampleDataTable {
            name: "sample_comments".to_owned(),
            rows: u32::try_from(comments_inserted).unwrap_or(0),
        },
    ];
    let total_rows = tables.iter().map(|t| t.rows).sum();
    Ok(ManagedPostgresSampleDataSeedResponse { tables, total_rows })
}

async fn inspect_managed_postgres_query(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
    request: &ManagedPostgresSqlQueryRequest,
) -> Result<ManagedPostgresQueryExplorerResponse, SqlApiError> {
    let statement_sql = managed_postgres_console_statement(&request.sql);
    let canonical_sql = canonicalize_sql_fragment(statement_sql);
    let pool = connect_managed_postgres(endpoint).await?;
    let mut tx = pool.begin().await.map_err(managed_postgres_sql_error)?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    sqlx::query("SET LOCAL statement_timeout = '5000ms'")
        .execute(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    let explain_sql = format!("EXPLAIN (FORMAT JSON) {statement_sql}");
    let SqlJson(explain_plan): SqlJson<JsonValue> = sqlx::query_scalar(&explain_sql)
        .fetch_one(&mut *tx)
        .await
        .map_err(managed_postgres_sql_error)?;
    tx.rollback().await.map_err(managed_postgres_sql_error)?;

    let root_plan = explain_plan
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("Plan"));
    Ok(ManagedPostgresQueryExplorerResponse {
        statement_kind: sql_tokens(&canonical_sql)
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned()),
        referenced_tables: referenced_tables_for_query(&canonical_sql, &explain_plan),
        sharing_behavior: "read_only_snapshot".to_owned(),
        estimated_startup_cost: root_plan
            .and_then(|plan| plan.get("Startup Cost"))
            .and_then(JsonValue::as_f64),
        estimated_total_cost: root_plan
            .and_then(|plan| plan.get("Total Cost"))
            .and_then(JsonValue::as_f64),
        estimated_rows: root_plan
            .and_then(|plan| plan.get("Plan Rows"))
            .and_then(JsonValue::as_f64),
        canonical_sql,
        explain_plan,
    })
}

async fn probe_managed_postgres_runtime(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
    cluster_id: &str,
) -> ManagedPostgresRuntimeCheck {
    let check_id = format!("runtime_check_{}", monotonic_nanos());
    match collect_managed_postgres_runtime_metrics(endpoint).await {
        Ok(metrics) => ManagedPostgresRuntimeCheck {
            check_id,
            cluster_id: cluster_id.to_owned(),
            status: runtime_check_status(&metrics),
            connection_count: metrics.connection_count,
            max_connections: metrics.max_connections,
            replication_slot_lag_bytes: metrics.replication_slot_lag_bytes,
            long_running_query_count: metrics.long_running_query_count,
            blocked_lock_count: metrics.blocked_lock_count,
            oldest_transaction_age_seconds: metrics.oldest_transaction_age_seconds,
            autovacuum_running: metrics.autovacuum_running,
            checked_at: "now".to_owned(),
            error_message: None,
        },
        Err(err) => ManagedPostgresRuntimeCheck {
            check_id,
            cluster_id: cluster_id.to_owned(),
            status: RuntimeCheckStatus::Failed,
            connection_count: 0,
            max_connections: 0,
            replication_slot_lag_bytes: None,
            long_running_query_count: 0,
            blocked_lock_count: 0,
            oldest_transaction_age_seconds: None,
            autovacuum_running: false,
            checked_at: "now".to_owned(),
            error_message: Some(format!("{err:?}")),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManagedPostgresRuntimeMetrics {
    connection_count: u32,
    max_connections: u32,
    replication_slot_lag_bytes: Option<u64>,
    long_running_query_count: u32,
    blocked_lock_count: u32,
    oldest_transaction_age_seconds: Option<u64>,
    autovacuum_running: bool,
}

async fn collect_managed_postgres_runtime_metrics(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
) -> Result<ManagedPostgresRuntimeMetrics, SqlApiError> {
    let pool = connect_managed_postgres(endpoint).await?;
    let connection_count = query_u32(
        &pool,
        "SELECT count(*)::bigint FROM pg_stat_activity",
        "connection_count",
    )
    .await?;
    let max_connections = sqlx::query_scalar::<_, String>("SHOW max_connections")
        .fetch_one(&pool)
        .await
        .map_err(managed_postgres_sql_error)?
        .parse::<u32>()
        .map_err(|err| SqlApiError::BadRequest(format!("parse max_connections: {err}")))?;
    let replication_slot_lag_bytes = query_optional_u64(
        &pool,
        "SELECT COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 0)::bigint \
         FROM pg_replication_slots WHERE database = current_database()",
        "replication_slot_lag_bytes",
    )
    .await?;
    let long_running_query_count = query_u32(
        &pool,
        "SELECT count(*)::bigint FROM pg_stat_activity \
         WHERE state = 'active' \
           AND pid <> pg_backend_pid() \
           AND query_start IS NOT NULL \
           AND now() - query_start > interval '5 minutes'",
        "long_running_query_count",
    )
    .await?;
    let blocked_lock_count = query_u32(
        &pool,
        "SELECT count(*)::bigint FROM pg_locks WHERE NOT granted",
        "blocked_lock_count",
    )
    .await?;
    let oldest_transaction_age_seconds = query_optional_u64(
        &pool,
        "SELECT EXTRACT(EPOCH FROM max(now() - xact_start))::bigint \
         FROM pg_stat_activity WHERE xact_start IS NOT NULL",
        "oldest_transaction_age_seconds",
    )
    .await?;
    let autovacuum_running = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query ILIKE 'autovacuum:%')",
    )
    .fetch_one(&pool)
    .await
    .map_err(managed_postgres_sql_error)?;

    Ok(ManagedPostgresRuntimeMetrics {
        connection_count,
        max_connections,
        replication_slot_lag_bytes,
        long_running_query_count,
        blocked_lock_count,
        oldest_transaction_age_seconds,
        autovacuum_running,
    })
}

const fn runtime_check_status(metrics: &ManagedPostgresRuntimeMetrics) -> RuntimeCheckStatus {
    let saturated = metrics.max_connections > 0
        && metrics.connection_count.saturating_mul(100) >= metrics.max_connections * 90;
    if saturated || metrics.long_running_query_count > 0 || metrics.blocked_lock_count > 0 {
        RuntimeCheckStatus::Degraded
    } else {
        RuntimeCheckStatus::Healthy
    }
}

async fn query_u32(pool: &sqlx::PgPool, sql: &str, field: &str) -> Result<u32, SqlApiError> {
    let value = sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .map_err(managed_postgres_sql_error)?;
    u32::try_from(value).map_err(|_| {
        SqlApiError::BadRequest(format!(
            "managed Postgres runtime field {field} is out of range"
        ))
    })
}

async fn query_optional_u64(
    pool: &sqlx::PgPool,
    sql: &str,
    field: &str,
) -> Result<Option<u64>, SqlApiError> {
    let value = sqlx::query_scalar::<_, Option<i64>>(sql)
        .fetch_one(pool)
        .await
        .map_err(managed_postgres_sql_error)?;
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                SqlApiError::BadRequest(format!(
                    "managed Postgres runtime field {field} is out of range"
                ))
            })
        })
        .transpose()
}

fn referenced_tables_for_query(canonical_sql: &str, explain_plan: &JsonValue) -> Vec<String> {
    let mut tables = BTreeSet::new();
    collect_explain_tables(explain_plan, &mut tables);
    for table in referenced_tables_from_sql(canonical_sql) {
        tables.insert(table);
    }
    tables.into_iter().collect()
}

fn referenced_tables_from_sql(sql: &str) -> Vec<String> {
    let mut tables = BTreeSet::new();
    let mut expect_table = false;
    for raw in sql.split_whitespace() {
        let token = raw
            .trim_matches(|ch: char| matches!(ch, ',' | ')' | '('))
            .trim_matches('"');
        let lower = token.to_ascii_lowercase();
        if matches!(lower.as_str(), "from" | "join") {
            expect_table = true;
            continue;
        }
        if expect_table {
            if token.contains('.') {
                tables.insert(token.to_owned());
            }
            expect_table = false;
        }
    }
    tables.into_iter().collect()
}

fn collect_explain_tables(value: &JsonValue, tables: &mut BTreeSet<String>) {
    match value {
        JsonValue::Object(map) => {
            if let Some(relation_name) = map.get("Relation Name").and_then(JsonValue::as_str) {
                let table = if let Some(schema_name) = map.get("Schema").and_then(JsonValue::as_str)
                {
                    format!("{schema_name}.{relation_name}")
                } else {
                    relation_name.to_owned()
                };
                tables.insert(table);
            }
            for child in map.values() {
                collect_explain_tables(child, tables);
            }
        }
        JsonValue::Array(values) => {
            for child in values {
                collect_explain_tables(child, tables);
            }
        }
        _ => {}
    }
}

async fn connect_managed_postgres(
    endpoint: &sql_store::ManagedPostgresSqlEndpoint,
) -> Result<sqlx::PgPool, SqlApiError> {
    let options = PgConnectOptions::new()
        .host(&endpoint.host)
        .port(endpoint.port)
        .database(&endpoint.database)
        .username(&endpoint.username)
        .password(&endpoint.password);
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(3))
        .connect_with(options)
        .await
        .map_err(managed_postgres_sql_error)
}

fn validate_managed_postgres_read_only_sql(sql: &str) -> Result<(), SqlApiError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(SqlApiError::BadRequest(
            "SQL console query cannot be empty".to_owned(),
        ));
    }
    if trimmed.len() > 10_000 {
        return Err(SqlApiError::BadRequest(
            "SQL console query is too large".to_owned(),
        ));
    }
    let statement_sql = managed_postgres_console_statement(trimmed);
    if statement_sql.is_empty()
        || statement_sql.contains(';')
        || statement_sql.contains("--")
        || statement_sql.contains("/*")
    {
        return Err(SqlApiError::BadRequest(
            "SQL console accepts one read-only statement without comments".to_owned(),
        ));
    }

    let tokens = sql_tokens(statement_sql);
    let first = tokens.first().map(String::as_str).unwrap_or_default();
    if !matches!(first, "select" | "with") {
        return Err(SqlApiError::BadRequest(
            "SQL console only accepts SELECT or WITH queries".to_owned(),
        ));
    }
    let forbidden: BTreeSet<&'static str> = [
        "alter",
        "analyze",
        "call",
        "comment",
        "copy",
        "create",
        "delete",
        "discard",
        "do",
        "drop",
        "execute",
        "grant",
        "insert",
        "listen",
        "lock",
        "lo_import",
        "merge",
        "notify",
        "pg_read_binary_file",
        "pg_read_file",
        "pg_sleep",
        "refresh",
        "reindex",
        "reset",
        "revoke",
        "security",
        "set",
        "truncate",
        "update",
        "vacuum",
    ]
    .into_iter()
    .collect();
    if let Some(token) = tokens
        .iter()
        .find(|token| forbidden.contains(token.as_str()))
    {
        return Err(SqlApiError::BadRequest(format!(
            "SQL console query contains forbidden token: {token}"
        )));
    }
    Ok(())
}

fn managed_postgres_console_statement(sql: &str) -> &str {
    let trimmed = sql.trim();
    trimmed.strip_suffix(';').map_or(trimmed, str::trim_end)
}

fn validate_query_permission_policy(policy: &QueryPermissionPolicy) -> Result<(), SqlApiError> {
    validate_sql_identifier("table_schema", &policy.table_schema)?;
    validate_sql_identifier("table_name", &policy.table_name)?;
    if policy.name.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "permission policy name cannot be empty".to_owned(),
        ));
    }
    if policy.principal_claim.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "permission policy principal_claim cannot be empty".to_owned(),
        ));
    }
    validate_permission_predicate_sql(&policy.predicate_sql)?;
    if !policy.sample_context.is_object() {
        return Err(SqlApiError::BadRequest(
            "permission policy sample_context must be a JSON object".to_owned(),
        ));
    }
    Ok(())
}

fn validate_jwt_issuer(issuer: &JwtIssuer) -> Result<(), SqlApiError> {
    if issuer.name.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "JWT issuer name cannot be empty".to_owned(),
        ));
    }
    if issuer.issuer.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "JWT issuer URL cannot be empty".to_owned(),
        ));
    }
    if issuer.audience.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "JWT issuer audience cannot be empty".to_owned(),
        ));
    }
    if !issuer.issuer.starts_with("https://") && !issuer.issuer.starts_with("http://localhost") {
        return Err(SqlApiError::BadRequest(
            "JWT issuer must be https or localhost for local development".to_owned(),
        ));
    }
    if !issuer.jwks_url.starts_with("https://") && !issuer.jwks_url.starts_with("http://localhost")
    {
        return Err(SqlApiError::BadRequest(
            "JWT JWKS URL must be https or localhost for local development".to_owned(),
        ));
    }
    if issuer.claim_to_field.is_empty() {
        return Err(SqlApiError::BadRequest(
            "JWT issuer requires at least one claim mapping".to_owned(),
        ));
    }
    for mapping in &issuer.claim_to_field {
        if mapping.claim.trim().is_empty() || mapping.field.trim().is_empty() {
            return Err(SqlApiError::BadRequest(
                "JWT claim mappings require non-empty claim and field".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_webhook_endpoint(endpoint: &WebhookEndpoint) -> Result<(), SqlApiError> {
    if endpoint.name.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "webhook endpoint name cannot be empty".to_owned(),
        ));
    }
    if endpoint.url.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "webhook endpoint URL cannot be empty".to_owned(),
        ));
    }
    if !endpoint.url.starts_with("https://") && !endpoint.url.starts_with("http://localhost") {
        return Err(SqlApiError::BadRequest(
            "webhook endpoint URL must be https or localhost for local development".to_owned(),
        ));
    }
    if endpoint.event_types.is_empty() {
        return Err(SqlApiError::BadRequest(
            "webhook endpoint requires at least one event type".to_owned(),
        ));
    }
    for event_type in &endpoint.event_types {
        let trimmed = event_type.trim();
        if trimmed.is_empty()
            || trimmed.len() > 128
            || !trimmed
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        {
            return Err(SqlApiError::BadRequest(
                "webhook event types must be non-empty stable event names".to_owned(),
            ));
        }
    }
    if let Some(secret_ref) = &endpoint.signing_secret_ref {
        if secret_ref.secret_id.trim().is_empty()
            || secret_ref.provider.trim().is_empty()
            || secret_ref.external_ref.trim().is_empty()
        {
            return Err(SqlApiError::BadRequest(
                "webhook signing_secret_ref fields cannot be empty".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_sso_identity_provider(provider: &SsoIdentityProvider) -> Result<(), SqlApiError> {
    if provider.organization_id.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "SSO provider organization_id cannot be empty".to_owned(),
        ));
    }
    if provider.name.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "SSO provider name cannot be empty".to_owned(),
        ));
    }
    if provider.issuer.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "SSO provider issuer cannot be empty".to_owned(),
        ));
    }
    if provider.sso_url.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "SSO provider sso_url cannot be empty".to_owned(),
        ));
    }
    if !provider.issuer.starts_with("https://") && !provider.issuer.starts_with("http://localhost")
    {
        return Err(SqlApiError::BadRequest(
            "SSO provider issuer must be https or localhost for local development".to_owned(),
        ));
    }
    if !provider.sso_url.starts_with("https://")
        && !provider.sso_url.starts_with("http://localhost")
    {
        return Err(SqlApiError::BadRequest(
            "SSO provider sso_url must be https or localhost for local development".to_owned(),
        ));
    }
    if provider.claim_mappings.is_empty() {
        return Err(SqlApiError::BadRequest(
            "SSO provider requires at least one claim mapping".to_owned(),
        ));
    }
    for mapping in &provider.claim_mappings {
        if mapping.claim.trim().is_empty() || mapping.field.trim().is_empty() {
            return Err(SqlApiError::BadRequest(
                "SSO claim mappings require non-empty claim and field".to_owned(),
            ));
        }
    }
    if let Some(secret_ref) = &provider.certificate_secret_ref {
        if secret_ref.secret_id.trim().is_empty()
            || secret_ref.provider.trim().is_empty()
            || secret_ref.external_ref.trim().is_empty()
        {
            return Err(SqlApiError::BadRequest(
                "SSO certificate_secret_ref fields cannot be empty".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_incident(incident: &Incident) -> Result<(), SqlApiError> {
    if incident.organization_id.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "incident organization_id cannot be empty".to_owned(),
        ));
    }
    if incident.title.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "incident title cannot be empty".to_owned(),
        ));
    }
    if incident.summary.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "incident summary cannot be empty".to_owned(),
        ));
    }
    if incident.started_at.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "incident started_at cannot be empty".to_owned(),
        ));
    }
    if incident.status != IncidentStatus::Resolved && incident.resolved_at.is_some() {
        return Err(SqlApiError::BadRequest(
            "incident resolved_at requires resolved status".to_owned(),
        ));
    }
    for service in &incident.impacted_services {
        let trimmed = service.trim();
        if trimmed.is_empty()
            || trimmed.len() > 128
            || !trimmed
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        {
            return Err(SqlApiError::BadRequest(
                "incident impacted_services must be stable service names".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_support_access_request(
    request: &CreateSupportAccessSessionRequest,
) -> Result<(), SqlApiError> {
    if request.reason.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "support access reason must not be empty".to_owned(),
        ));
    }
    if request.reason.len() > 2_000 {
        return Err(SqlApiError::BadRequest(
            "support access reason must be 2000 characters or less".to_owned(),
        ));
    }
    if request.duration_minutes == 0 || request.duration_minutes > 240 {
        return Err(SqlApiError::BadRequest(
            "support access duration_minutes must be between 1 and 240".to_owned(),
        ));
    }
    if request
        .ticket_ref
        .as_ref()
        .is_some_and(|ticket_ref| ticket_ref.len() > 256)
    {
        return Err(SqlApiError::BadRequest(
            "support access ticket_ref must be 256 characters or less".to_owned(),
        ));
    }
    Ok(())
}

fn validate_secret_encryption_key(key: &SecretEncryptionKey) -> Result<(), SqlApiError> {
    for (field, value) in [
        ("key_ref", key.key_ref.as_str()),
        ("provider", key.provider.as_str()),
        ("purpose", key.purpose.as_str()),
    ] {
        let trimmed = value.trim();
        if trimmed.is_empty()
            || trimmed.len() > 256
            || !trimmed
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, ':' | '/' | '_' | '-' | '.'))
        {
            return Err(SqlApiError::BadRequest(format!(
                "secret encryption key {field} must be a stable non-empty identifier"
            )));
        }
    }
    if key.status == SecretEncryptionKeyStatus::Active && key.retired_at.is_some() {
        return Err(SqlApiError::BadRequest(
            "active secret encryption keys cannot have retired_at".to_owned(),
        ));
    }
    Ok(())
}

fn validate_secret_rewrap_plan_request(
    request: &CreateSecretRewrapPlanRequest,
) -> Result<(), SqlApiError> {
    if request.source_key_ref.trim().is_empty() || request.target_key_ref.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "secret rewrap source and target key refs cannot be empty".to_owned(),
        ));
    }
    if request.source_key_ref == request.target_key_ref {
        return Err(SqlApiError::BadRequest(
            "secret rewrap source and target key refs must differ".to_owned(),
        ));
    }
    Ok(())
}

fn validate_node_host_hardening_check(
    host_id: &str,
    check: &NodeHostHardeningCheck,
) -> Result<(), SqlApiError> {
    if check.host_id != host_id {
        return Err(SqlApiError::BadRequest(
            "hardening check host_id must match the route host".to_owned(),
        ));
    }
    for (field, value) in [
        ("check_id", check.check_id.as_str()),
        ("image_ref", check.image_ref.as_str()),
        ("os_release", check.os_release.as_str()),
        ("kernel_version", check.kernel_version.as_str()),
        ("container_runtime", check.container_runtime.as_str()),
        ("checked_at", check.checked_at.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(SqlApiError::BadRequest(format!(
                "hardening check {field} cannot be empty"
            )));
        }
    }
    if check.postgres_major_min < MIN_SUPPORTED_POSTGRES_MAJOR {
        return Err(SqlApiError::BadRequest(format!(
            "hardening check postgres_major_min must be {MIN_SUPPORTED_POSTGRES_MAJOR} or newer"
        )));
    }
    if check.status == NodeHostHardeningStatus::Failing
        && check
            .error_message
            .as_ref()
            .is_none_or(|message| message.trim().is_empty())
    {
        return Err(SqlApiError::BadRequest(
            "failing hardening checks require error_message".to_owned(),
        ));
    }
    Ok(())
}

async fn managed_postgres_backup_for_cluster(
    store: &sql_store::SqlControlPlaneStore,
    cluster_id: &str,
    backup_id: &str,
) -> Result<ManagedPostgresBackup, SqlApiError> {
    let backup = store
        .managed_postgres_backup(backup_id)
        .await?
        .ok_or_else(|| SqlApiError::MissingResource(backup_id.to_owned()))?;
    if backup.cluster_id != cluster_id {
        return Err(SqlApiError::MissingResource(backup_id.to_owned()));
    }
    Ok(backup)
}

fn validate_managed_postgres_backup_artifact(
    backup: &ManagedPostgresBackup,
    artifact: &ManagedPostgresBackupArtifact,
) -> Result<(), SqlApiError> {
    if artifact.backup_id != backup.backup_id {
        return Err(SqlApiError::BadRequest(
            "backup artifact backup_id must match the route backup".to_owned(),
        ));
    }
    if artifact.cluster_id != backup.cluster_id {
        return Err(SqlApiError::BadRequest(
            "backup artifact cluster_id must match the route cluster".to_owned(),
        ));
    }
    for (field, value) in [
        ("artifact_id", artifact.artifact_id.as_str()),
        ("provider", artifact.provider.as_str()),
        ("object_uri", artifact.object_uri.as_str()),
        ("manifest_path", artifact.manifest_path.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(SqlApiError::BadRequest(format!(
                "backup artifact {field} cannot be empty"
            )));
        }
    }
    if artifact.status == BackupArtifactStatus::Available
        && backup.status != BackupLifecycleState::Succeeded
    {
        return Err(SqlApiError::BadRequest(
            "available backup artifacts require a succeeded backup".to_owned(),
        ));
    }
    if artifact.status == BackupArtifactStatus::Failed
        && artifact
            .error_message
            .as_ref()
            .is_none_or(|message| message.trim().is_empty())
    {
        return Err(SqlApiError::BadRequest(
            "failed backup artifacts require error_message".to_owned(),
        ));
    }
    Ok(())
}

fn validate_domain(domain: &Domain) -> Result<(), SqlApiError> {
    let hostname = domain.hostname.trim();
    if hostname.is_empty() || hostname.len() > 253 {
        return Err(SqlApiError::BadRequest(
            "domain hostname must be a non-empty DNS name".to_owned(),
        ));
    }
    if hostname != hostname.to_ascii_lowercase()
        || hostname.starts_with('.')
        || hostname.ends_with('.')
        || !hostname.contains('.')
        || hostname.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
        })
    {
        return Err(SqlApiError::BadRequest(
            "domain hostname must be a lowercase DNS name".to_owned(),
        ));
    }
    if domain.domain_id.trim().is_empty()
        || domain.route_host.trim().is_empty()
        || domain.verification_token.trim().is_empty()
    {
        return Err(SqlApiError::BadRequest(
            "domain id, route_host, and verification_token cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_ip_allowlist_rule(rule: &IpAllowlistRule) -> Result<(), SqlApiError> {
    if rule.rule_id.trim().is_empty() || rule.name.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "IP allowlist rule id and name cannot be empty".to_owned(),
        ));
    }
    validate_cidr(&rule.cidr)?;
    Ok(())
}

fn validate_static_egress_ip(egress_ip: &StaticEgressIp) -> Result<(), SqlApiError> {
    if egress_ip.egress_ip_id.trim().is_empty()
        || egress_ip.region.trim().is_empty()
        || egress_ip.provider_ref.trim().is_empty()
    {
        return Err(SqlApiError::BadRequest(
            "static egress IP id, region, and provider_ref cannot be empty".to_owned(),
        ));
    }
    egress_ip
        .ip_address
        .parse::<std::net::IpAddr>()
        .map_err(|_| SqlApiError::BadRequest("static egress IP address is invalid".to_owned()))?;
    Ok(())
}

fn validate_maintenance_window(window: &MaintenanceWindow) -> Result<(), SqlApiError> {
    if window.window_id.trim().is_empty() || window.name.trim().is_empty() {
        return Err(SqlApiError::BadRequest(
            "maintenance window id and name cannot be empty".to_owned(),
        ));
    }
    if window.duration_minutes == 0 || window.duration_minutes > 1_440 {
        return Err(SqlApiError::BadRequest(
            "maintenance window duration_minutes must be between 1 and 1440".to_owned(),
        ));
    }
    validate_maintenance_clock_time(&window.start_time)?;
    Ok(())
}

fn validate_maintenance_clock_time(value: &str) -> Result<(), SqlApiError> {
    maintenance_clock_minutes(value).map(|_| ())
}

fn maintenance_clock_minutes(value: &str) -> Result<u32, SqlApiError> {
    let (hour, minute) = value.split_once(':').ok_or_else(|| {
        SqlApiError::BadRequest("maintenance window start_time must be HH:MM".to_owned())
    })?;
    let hour = hour.parse::<u32>().map_err(|_| {
        SqlApiError::BadRequest("maintenance window start_time hour is invalid".to_owned())
    })?;
    let minute = minute.parse::<u32>().map_err(|_| {
        SqlApiError::BadRequest("maintenance window start_time minute is invalid".to_owned())
    })?;
    if hour > 23 || minute > 59 || value.len() != 5 {
        return Err(SqlApiError::BadRequest(
            "maintenance window start_time must be a valid HH:MM value".to_owned(),
        ));
    }
    Ok(hour * 60 + minute)
}

fn maintenance_window_is_open(
    window: &MaintenanceWindow,
    day_of_week: MaintenanceDayOfWeek,
    current_time: &str,
) -> bool {
    let Ok(start) = maintenance_clock_minutes(&window.start_time) else {
        return false;
    };
    let Ok(current) = maintenance_clock_minutes(current_time) else {
        return false;
    };
    let week_minutes = 7 * 1_440;
    let start = maintenance_day_index(window.day_of_week) * 1_440 + start;
    let current = maintenance_day_index(day_of_week) * 1_440 + current;
    let end = start + window.duration_minutes.min(1_440);
    (current >= start && current < end)
        || (current + week_minutes >= start && current + week_minutes < end)
}

fn current_utc_maintenance_clock() -> (MaintenanceDayOfWeek, String) {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    maintenance_clock_from_unix_seconds(seconds)
}

fn maintenance_clock_from_unix_seconds(seconds: u64) -> (MaintenanceDayOfWeek, String) {
    let days = seconds / 86_400;
    let seconds_in_day = seconds % 86_400;
    let day = match (days + 3) % 7 {
        0 => MaintenanceDayOfWeek::Monday,
        1 => MaintenanceDayOfWeek::Tuesday,
        2 => MaintenanceDayOfWeek::Wednesday,
        3 => MaintenanceDayOfWeek::Thursday,
        4 => MaintenanceDayOfWeek::Friday,
        5 => MaintenanceDayOfWeek::Saturday,
        _ => MaintenanceDayOfWeek::Sunday,
    };
    let hour = seconds_in_day / 3_600;
    let minute = (seconds_in_day % 3_600) / 60;
    (day, format!("{hour:02}:{minute:02}"))
}

const fn maintenance_day_index(day: MaintenanceDayOfWeek) -> u32 {
    match day {
        MaintenanceDayOfWeek::Monday => 0,
        MaintenanceDayOfWeek::Tuesday => 1,
        MaintenanceDayOfWeek::Wednesday => 2,
        MaintenanceDayOfWeek::Thursday => 3,
        MaintenanceDayOfWeek::Friday => 4,
        MaintenanceDayOfWeek::Saturday => 5,
        MaintenanceDayOfWeek::Sunday => 6,
    }
}

fn validate_cidr(cidr: &str) -> Result<(), SqlApiError> {
    let (address, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| SqlApiError::BadRequest("CIDR must include a prefix length".to_owned()))?;
    let address = address
        .parse::<std::net::IpAddr>()
        .map_err(|_| SqlApiError::BadRequest("CIDR address is invalid".to_owned()))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|_| SqlApiError::BadRequest("CIDR prefix is invalid".to_owned()))?;
    let max_prefix = if address.is_ipv4() { 32 } else { 128 };
    if prefix > max_prefix {
        return Err(SqlApiError::BadRequest(
            "CIDR prefix exceeds address family size".to_owned(),
        ));
    }
    Ok(())
}

fn validate_sql_identifier(field: &str, value: &str) -> Result<(), SqlApiError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(SqlApiError::BadRequest(format!(
            "{field} must be an unquoted PostgreSQL identifier"
        )));
    }
    Ok(())
}

fn validate_permission_predicate_sql(predicate: &str) -> Result<(), SqlApiError> {
    let trimmed = predicate.trim();
    if trimmed.is_empty() {
        return Err(SqlApiError::BadRequest(
            "permission predicate cannot be empty".to_owned(),
        ));
    }
    if trimmed.len() > 4_000 {
        return Err(SqlApiError::BadRequest(
            "permission predicate is too large".to_owned(),
        ));
    }
    if trimmed.contains(';') || trimmed.contains("--") || trimmed.contains("/*") {
        return Err(SqlApiError::BadRequest(
            "permission predicate accepts one SQL expression without comments".to_owned(),
        ));
    }
    let forbidden: BTreeSet<&'static str> = [
        "alter", "analyze", "call", "comment", "copy", "create", "delete", "discard", "do", "drop",
        "execute", "grant", "insert", "listen", "lock", "merge", "notify", "refresh", "reindex",
        "reset", "revoke", "set", "truncate", "update", "vacuum",
    ]
    .into_iter()
    .collect();
    if let Some(token) = sql_tokens(trimmed)
        .iter()
        .find(|token| forbidden.contains(token.as_str()))
    {
        return Err(SqlApiError::BadRequest(format!(
            "permission predicate contains forbidden token: {token}"
        )));
    }
    Ok(())
}

fn canonicalize_sql_fragment(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Runs the permission-rule DSL through the real `palimpsest-permissions`
/// verifier: parse the TOML, then compile every rule against a catalog.
/// Compilation is what validates column references, `$user.*` fields,
/// predicate depth, and ambiguity, so the result mirrors what the runtime
/// rewriter would accept.
fn verify_permissions_dsl(request: &PermissionVerifyRequest) -> PermissionVerifyResponse {
    use palimpsest_permissions::{compile_rules, parse_config};
    use palimpsest_sql::{Catalog, ColumnSchema, TableSchema};

    let (catalog, catalog_source) = match &request.catalog {
        Some(tables) if !tables.is_empty() => {
            let schemas = tables.iter().map(|table| {
                TableSchema::new(
                    table.name.clone(),
                    table
                        .columns
                        .iter()
                        .map(|column| {
                            ColumnSchema::new(
                                column.name.clone(),
                                parse_verify_column_type(&column.ty),
                            )
                        })
                        .collect(),
                )
            });
            (Catalog::new(schemas), "inline".to_owned())
        }
        _ => (Catalog::demo(), "demo".to_owned()),
    };
    let catalog_tables: Vec<String> = catalog.tables().map(|table| table.name.clone()).collect();

    let config = match parse_config(&request.dsl) {
        Ok(config) => config,
        Err(err) => {
            return PermissionVerifyResponse {
                ok: false,
                error: Some(err.to_string()),
                catalog_source,
                catalog_tables,
                user_context: Vec::new(),
                rules: Vec::new(),
            };
        }
    };

    let user_context = config.user_context.clone();
    let schema = config.user_context_schema();

    match compile_rules(&config.rules(), &catalog, &schema) {
        Ok(compiled) => {
            let rules = compiled
                .iter()
                .map(|rule| PermissionVerifyRule {
                    name: rule.name.clone(),
                    table: rule.table.clone(),
                    mode: rule.mode,
                    predicate: config
                        .rules
                        .iter()
                        .find(|raw| raw.name == rule.name)
                        .map(|raw| raw.predicate.clone())
                        .unwrap_or_default(),
                    canonical: Some(rule.predicate.canonical.clone()),
                    user_fields: rule.predicate.user_fields.iter().cloned().collect(),
                    tautology: rule.predicate.is_tautology(),
                })
                .collect();
            PermissionVerifyResponse {
                ok: true,
                error: None,
                catalog_source,
                catalog_tables,
                user_context,
                rules,
            }
        }
        Err(err) => {
            // Parse succeeded but a rule failed to compile; surface the parsed
            // rules so the editor can still show structure alongside the error.
            let rules = config
                .rules
                .iter()
                .map(|raw| PermissionVerifyRule {
                    name: raw.name.clone(),
                    table: raw.table.clone(),
                    mode: raw.mode,
                    predicate: raw.predicate.clone(),
                    canonical: None,
                    user_fields: Vec::new(),
                    tautology: false,
                })
                .collect();
            PermissionVerifyResponse {
                ok: false,
                error: Some(err.to_string()),
                catalog_source,
                catalog_tables,
                user_context,
                rules,
            }
        }
    }
}

/// Maps a column-type string (our coarse names or raw `PostgreSQL` `data_type`
/// labels forwarded by the UI) onto the verifier's [`ColumnType`]. Unknown
/// types are treated as compatible with everything.
fn parse_verify_column_type(ty: &str) -> palimpsest_sql::ColumnType {
    use palimpsest_sql::ColumnType;
    match ty.trim().to_ascii_lowercase().as_str() {
        "bool" | "boolean" => ColumnType::Bool,
        "int" | "integer" | "bigint" | "smallint" | "int2" | "int4" | "int8" | "serial"
        | "bigserial" => ColumnType::Int,
        "float" | "double" | "double precision" | "real" | "numeric" | "decimal" | "float4"
        | "float8" => ColumnType::Float,
        "text" | "varchar" | "char" | "character varying" | "character" | "name" | "citext"
        | "uuid" => ColumnType::Text,
        "timestamp"
        | "timestamptz"
        | "timestamp with time zone"
        | "timestamp without time zone"
        | "date" => ColumnType::Timestamp,
        _ => ColumnType::Unknown,
    }
}

fn sql_tokens(sql: &str) -> Vec<String> {
    sql.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn managed_postgres_sql_error(error: sqlx::Error) -> SqlApiError {
    SqlApiError::BadRequest(format!("managed Postgres read-only SQL failed: {error}"))
}

#[derive(Debug, Serialize)]
pub struct ApiOk {
    status: &'static str,
}

impl ApiOk {
    const fn new(status: &'static str) -> Self {
        Self { status }
    }
}

#[derive(Debug)]
pub enum ApiError {
    ControlPlane(ControlPlaneError),
    ServicePoisoned,
}

impl From<ControlPlaneError> for ApiError {
    fn from(value: ControlPlaneError) -> Self {
        Self::ControlPlane(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::ControlPlane(
                ControlPlaneError::DuplicateResource(resource)
                | ControlPlaneError::DuplicateOperation(resource),
            ) => (
                StatusCode::CONFLICT,
                format!("resource already exists: {resource}"),
            ),
            Self::ControlPlane(ControlPlaneError::ImmutableResource(resource)) => (
                StatusCode::CONFLICT,
                format!("resource is immutable: {resource}"),
            ),
            Self::ControlPlane(ControlPlaneError::MissingResource(resource)) => (
                StatusCode::NOT_FOUND,
                format!("resource not found: {resource}"),
            ),
            Self::ControlPlane(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            Self::ServicePoisoned => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "control-plane service lock poisoned".to_owned(),
            ),
        };

        (status, Json(ApiErrorBody { error: message })).into_response()
    }
}

#[derive(Debug)]
pub enum SqlApiError {
    Store(sql_store::SqlStoreError),
    MissingResource(String),
    BadRequest(String),
    Conflict(String),
    Unauthorized,
}

impl From<sql_store::SqlStoreError> for SqlApiError {
    fn from(value: sql_store::SqlStoreError) -> Self {
        Self::Store(value)
    }
}

impl From<ControlPlaneError> for SqlApiError {
    fn from(value: ControlPlaneError) -> Self {
        Self::BadRequest(value.to_string())
    }
}

impl IntoResponse for SqlApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Store(err @ sql_store::SqlStoreError::QuotaExceeded { .. }) => {
                (StatusCode::TOO_MANY_REQUESTS, err.to_string())
            }
            Self::Store(err) if sql_store::is_unique_violation(&err) => {
                (StatusCode::CONFLICT, err.to_string())
            }
            Self::Store(err) if sql_store::is_foreign_key_violation(&err) => {
                (StatusCode::NOT_FOUND, err.to_string())
            }
            Self::Store(sql_store::SqlStoreError::MissingResource(resource)) => (
                StatusCode::NOT_FOUND,
                format!("resource not found: {resource}"),
            ),
            Self::Store(sql_store::SqlStoreError::ImmutableResource(resource)) => (
                StatusCode::CONFLICT,
                format!("resource is immutable: {resource}"),
            ),
            Self::Store(sql_store::SqlStoreError::InvalidValue(message)) => {
                (StatusCode::BAD_REQUEST, message)
            }
            Self::Store(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            Self::MissingResource(resource) => (
                StatusCode::NOT_FOUND,
                format!("resource not found: {resource}"),
            ),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "missing, invalid, or out-of-scope authorization".to_owned(),
            ),
        };

        (status, Json(ApiErrorBody { error: message })).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    error: String,
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use palimpsest_paas_core::{ConfigVersionStatus, OperationKind, OperationStatus};
    use palimpsest_paas_core::{PostgresVersion, SyncDeploymentLifecycleState};
    use tower::ServiceExt;

    use super::*;

    const DEMO_DSL: &str = "\
[[user_context]]
name = \"id\"
type = \"int\"

[[rule]]
name = \"posts_owner\"
table = \"posts\"
mode = \"both\"
predicate = \"author_id = $user.id\"
";

    #[test]
    fn verify_compiles_against_demo_catalog() {
        let response = verify_permissions_dsl(&PermissionVerifyRequest {
            dsl: DEMO_DSL.to_owned(),
            catalog: None,
        });
        assert!(response.ok, "expected ok, got {:?}", response.error);
        assert_eq!(response.catalog_source, "demo");
        assert_eq!(response.rules.len(), 1);
        let rule = &response.rules[0];
        assert_eq!(rule.name, "posts_owner");
        assert!(rule.canonical.is_some());
        assert_eq!(rule.user_fields, vec!["id".to_owned()]);
    }

    #[test]
    fn verify_reports_unknown_table() {
        let dsl = "\
[[rule]]
name = \"x\"
table = \"not_a_table\"
predicate = \"true\"
";
        let response = verify_permissions_dsl(&PermissionVerifyRequest {
            dsl: dsl.to_owned(),
            catalog: None,
        });
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("not_a_table"));
        // Parsed structure is still surfaced for the editor.
        assert_eq!(response.rules.len(), 1);
    }

    #[test]
    fn verify_uses_inline_catalog() {
        let dsl = "\
[[user_context]]
name = \"id\"
type = \"int\"

[[rule]]
name = \"tenant_rows\"
table = \"widgets\"
predicate = \"owner_id = $user.id\"
";
        let response = verify_permissions_dsl(&PermissionVerifyRequest {
            dsl: dsl.to_owned(),
            catalog: Some(vec![PermissionVerifyCatalogTable {
                name: "widgets".to_owned(),
                columns: vec![PermissionVerifyCatalogColumn {
                    name: "owner_id".to_owned(),
                    ty: "integer".to_owned(),
                }],
            }]),
        });
        assert!(response.ok, "expected ok, got {:?}", response.error);
        assert_eq!(response.catalog_source, "inline");
        assert_eq!(response.catalog_tables, vec!["widgets".to_owned()]);
    }

    #[test]
    fn verify_reports_toml_parse_error() {
        let response = verify_permissions_dsl(&PermissionVerifyRequest {
            dsl: "this is = not valid = toml".to_owned(),
            catalog: None,
        });
        assert!(!response.ok);
        assert!(response.error.is_some());
        assert!(response.rules.is_empty());
    }

    fn cluster(state: ClusterLifecycleState) -> ManagedPostgresCluster {
        ManagedPostgresCluster {
            cluster_id: "cluster_123".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            region: "us-east-1".to_owned(),
            postgres_version: PostgresVersion::new("18").expect("valid version"),
            tier: "dev".to_owned(),
            storage_gib: 20,
            lifecycle_state: state,
            host_assignment: None,
        }
    }

    fn usage_event() -> UsageEvent {
        UsageEvent {
            event_id: "usage_123".to_owned(),
            idempotency_key: "env_123:sync_egress_bytes:2026-05-17T00:00:00Z".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            metric: "sync_egress_bytes".to_owned(),
            quantity: 4096,
            occurred_at: "2026-05-17T00:00:00Z".to_owned(),
            signature: None,
        }
    }

    #[test]
    fn standby_physical_slot_name_fits_postgres_identifier_limit() {
        let name = standby_physical_slot_name(
            "cluster-with-a-very-long-name-that-would-exceed-postgres-identifier-length",
        );

        assert!(name.len() <= 63);
        assert!(name.ends_with("_standby_slot"));
        assert!(name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_'));
    }

    #[test]
    fn standby_physical_slot_name_avoids_duplicate_standby_suffix() {
        assert_eq!(
            standby_physical_slot_name("cluster_123_standby"),
            "cluster_123_standby_slot"
        );
    }

    #[test]
    fn maintenance_clock_uses_utc_weekday_and_minute() {
        assert_eq!(
            maintenance_clock_from_unix_seconds(0),
            (MaintenanceDayOfWeek::Thursday, "00:00".to_owned())
        );
        assert_eq!(
            maintenance_clock_from_unix_seconds(86_400 + 3_660),
            (MaintenanceDayOfWeek::Friday, "01:01".to_owned())
        );
    }

    #[test]
    fn maintenance_window_open_check_handles_same_day_and_midnight_wrap() {
        let mut window = MaintenanceWindow {
            window_id: "window_123".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            name: "Weekly".to_owned(),
            day_of_week: MaintenanceDayOfWeek::Sunday,
            start_time: "03:00".to_owned(),
            duration_minutes: 120,
            auto_minor_upgrades: true,
            status: MaintenanceWindowStatus::Active,
        };

        assert!(maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Sunday,
            "03:00"
        ));
        assert!(maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Sunday,
            "04:59"
        ));
        assert!(!maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Sunday,
            "05:00"
        ));

        window.start_time = "23:30".to_owned();
        window.duration_minutes = 90;
        assert!(maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Sunday,
            "23:45"
        ));
        assert!(maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Monday,
            "00:30"
        ));
        assert!(!maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Monday,
            "01:00"
        ));
        assert!(!maintenance_window_is_open(
            &window,
            MaintenanceDayOfWeek::Sunday,
            "00:30"
        ));
    }

    #[test]
    fn runtime_check_status_flags_connection_and_lock_pressure() {
        let healthy = ManagedPostgresRuntimeMetrics {
            connection_count: 5,
            max_connections: 100,
            replication_slot_lag_bytes: Some(0),
            long_running_query_count: 0,
            blocked_lock_count: 0,
            oldest_transaction_age_seconds: None,
            autovacuum_running: false,
        };
        assert_eq!(runtime_check_status(&healthy), RuntimeCheckStatus::Healthy);

        let saturated = ManagedPostgresRuntimeMetrics {
            connection_count: 90,
            ..healthy
        };
        assert_eq!(
            runtime_check_status(&saturated),
            RuntimeCheckStatus::Degraded
        );

        let blocked = ManagedPostgresRuntimeMetrics {
            blocked_lock_count: 1,
            ..healthy
        };
        assert_eq!(runtime_check_status(&blocked), RuntimeCheckStatus::Degraded);
    }

    #[test]
    fn control_plane_metrics_render_prometheus_text() {
        let snapshot = sql_store::ControlPlaneMetricsSnapshot {
            agent_commands_failed_total: 2,
            billing_exports_failed_total: 1,
            wal_archive_failures_total: 3,
            quota_alerts_firing_total: 4,
            last_successful_backups: vec![sql_store::ClusterTimestampMetric {
                cluster_id: "cluster_\"quoted\"".to_owned(),
                timestamp_seconds: Some(1_800_000_000.0),
            }],
            cluster_lifecycle_states: vec![sql_store::ClusterLifecycleMetric {
                cluster_id: "cluster_123".to_owned(),
                environment_id: "env_123".to_owned(),
                lifecycle_state: "ready".to_owned(),
            }],
            cluster_storage_allocations: vec![sql_store::ClusterStorageMetric {
                cluster_id: "cluster_123".to_owned(),
                environment_id: "env_123".to_owned(),
                host_id: Some("host-a".to_owned()),
                storage_gib: 32,
            }],
            last_successful_wal_archives: vec![sql_store::ClusterTimestampMetric {
                cluster_id: "cluster_123".to_owned(),
                timestamp_seconds: Some(1_800_000_010.0),
            }],
            last_successful_pitr_checks: vec![sql_store::ClusterTimestampMetric {
                cluster_id: "cluster_123".to_owned(),
                timestamp_seconds: Some(1_800_000_020.0),
            }],
            last_successful_restore_drills: vec![sql_store::ClusterTimestampMetric {
                cluster_id: "cluster_123".to_owned(),
                timestamp_seconds: Some(1_800_000_030.0),
            }],
            last_successful_standby_checks: vec![sql_store::ClusterTimestampMetric {
                cluster_id: "cluster_123".to_owned(),
                timestamp_seconds: Some(1_800_000_040.0),
            }],
            quota_usage_ratios: vec![sql_store::QuotaUsageRatioMetric {
                environment_id: "env_123".to_owned(),
                metric: "sync_egress_bytes".to_owned(),
                ratio: 0.75,
            }],
            storage_used_ratios: vec![sql_store::HostStorageRatioMetric {
                host_id: "host-a".to_owned(),
                ratio: 0.5,
            }],
        };

        let metrics = render_control_plane_metrics(&snapshot);

        assert!(metrics.contains("palimpsest_paas_agent_commands_failed_total 2"));
        assert!(metrics.contains("palimpsest_paas_billing_exports_failed_total 1"));
        assert!(metrics.contains("palimpsest_managed_postgres_wal_archives_failed_total 3"));
        assert!(metrics.contains("palimpsest_paas_quota_alerts_firing_total 4"));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_last_successful_backup_timestamp_seconds{cluster_id=\"cluster_\\\"quoted\\\"\"} 1800000000"
        ));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_cluster_lifecycle_state{cluster_id=\"cluster_123\",environment_id=\"env_123\",state=\"ready\"} 1"
        ));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_storage_allocated_gib{cluster_id=\"cluster_123\",environment_id=\"env_123\",host_id=\"host-a\"} 32"
        ));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_last_successful_wal_archive_timestamp_seconds{cluster_id=\"cluster_123\"} 1800000010"
        ));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_last_successful_pitr_check_timestamp_seconds{cluster_id=\"cluster_123\"} 1800000020"
        ));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_last_successful_restore_drill_timestamp_seconds{cluster_id=\"cluster_123\"} 1800000030"
        ));
        assert!(metrics.contains(
            "palimpsest_managed_postgres_last_successful_standby_check_timestamp_seconds{cluster_id=\"cluster_123\"} 1800000040"
        ));
        assert!(metrics.contains(
            "palimpsest_paas_quota_usage_ratio{environment_id=\"env_123\",metric=\"sync_egress_bytes\"} 0.75"
        ));
        assert!(metrics
            .contains("palimpsest_managed_postgres_storage_used_ratio{host_id=\"host-a\"} 0.5"));
    }

    #[test]
    fn query_explorer_extracts_schema_qualified_tables_from_sql() {
        assert_eq!(
            referenced_tables_from_sql(
                "select id from public.dashboard_probe join app.accounts on true"
            ),
            vec![
                "app.accounts".to_owned(),
                "public.dashboard_probe".to_owned()
            ]
        );
    }

    #[test]
    fn wal_archive_continuity_accepts_contiguous_segments() {
        validate_wal_archive_continuity(&[
            "000000010000000000000001",
            "000000010000000000000002",
            "000000010000000000000003",
        ])
        .expect("segments are contiguous");
    }

    #[test]
    fn wal_archive_continuity_rejects_gaps_and_timeline_changes() {
        assert!(validate_wal_archive_continuity(&[
            "000000010000000000000001",
            "000000010000000000000003",
        ])
        .expect_err("gap is rejected")
        .contains("gap"));
        assert!(validate_wal_archive_continuity(&[
            "000000010000000000000001",
            "000000020000000000000002",
        ])
        .expect_err("timeline change is rejected")
        .contains("multiple timelines"));
    }

    #[test]
    fn managed_postgres_sql_console_allows_read_only_queries() {
        validate_managed_postgres_read_only_sql("select id, name from widgets where id = 1")
            .unwrap();
        validate_managed_postgres_read_only_sql("SELECT * FROM sample_users;").unwrap();
        validate_managed_postgres_read_only_sql(
            "with recent as (select * from widgets) select count(*) from recent;",
        )
        .unwrap();
        assert_eq!(
            managed_postgres_console_statement(" SELECT * FROM sample_users;  "),
            "SELECT * FROM sample_users"
        );
    }

    #[test]
    fn managed_postgres_sql_console_rejects_mutations_and_multi_statement_input() {
        assert!(validate_managed_postgres_read_only_sql("delete from widgets").is_err());
        assert!(validate_managed_postgres_read_only_sql(
            "select * from widgets; drop table widgets"
        )
        .is_err());
        assert!(validate_managed_postgres_read_only_sql(
            "with deleted as (delete from widgets returning *) select * from deleted"
        )
        .is_err());
        assert!(validate_managed_postgres_read_only_sql("select pg_sleep(100)").is_err());
        assert!(
            validate_managed_postgres_read_only_sql("/* hidden */ select * from widgets").is_err()
        );
    }

    #[test]
    fn agent_auth_is_optional_until_token_is_configured() {
        let headers = HeaderMap::new();
        let scope = AgentAuthScope::new("host-a", "lease");
        assert!(validate_agent_auth(&headers, None, None, scope).is_ok());
        assert!(validate_agent_auth(&headers, Some(""), None, scope).is_ok());
    }

    #[test]
    fn agent_auth_requires_matching_bearer_token() {
        let mut headers = HeaderMap::new();
        let scope = AgentAuthScope::new("host-a", "lease");
        assert!(matches!(
            validate_agent_auth(&headers, Some("secret-token"), None, scope),
            Err(SqlApiError::Unauthorized)
        ));

        headers.insert(
            header::AUTHORIZATION,
            "Bearer secret-token".parse().expect("valid header"),
        );
        assert!(validate_agent_auth(&headers, Some("secret-token"), None, scope).is_ok());
    }

    #[test]
    fn signed_agent_auth_accepts_fresh_matching_signature() {
        let key = vec![9; 32];
        let scope = AgentAuthScope::new("host-a", "complete:cmd-123");
        let timestamp = 1_800_000_000;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-palimpsest-agent-host-id",
            "host-a".parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-operation",
            "complete:cmd-123".parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-timestamp",
            timestamp.to_string().parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-signature",
            sign_agent_request(&key, scope.host_id, scope.operation, timestamp)
                .parse()
                .expect("valid header"),
        );

        assert!(validate_signed_agent_auth(&headers, &key, scope, timestamp + 1).is_ok());
    }

    #[test]
    fn signed_agent_auth_rejects_stale_or_mismatched_signature() {
        let key = vec![9; 32];
        let scope = AgentAuthScope::new("host-a", "lease");
        let timestamp = 1_800_000_000;
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-palimpsest-agent-host-id",
            "host-b".parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-operation",
            "lease".parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-timestamp",
            timestamp.to_string().parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-signature",
            sign_agent_request(&key, "host-b", "lease", timestamp)
                .parse()
                .expect("valid header"),
        );

        assert!(matches!(
            validate_signed_agent_auth(&headers, &key, scope, timestamp + 1),
            Err(SqlApiError::Unauthorized)
        ));

        headers.insert(
            "x-palimpsest-agent-host-id",
            "host-a".parse().expect("valid header"),
        );
        headers.insert(
            "x-palimpsest-agent-signature",
            sign_agent_request(&key, "host-a", "lease", timestamp)
                .parse()
                .expect("valid header"),
        );
        assert!(matches!(
            validate_signed_agent_auth(&headers, &key, scope, timestamp + 301),
            Err(SqlApiError::Unauthorized)
        ));
    }

    #[test]
    fn signed_usage_event_accepts_matching_hmac() {
        let key = vec![7; 32];
        let mut event = usage_event();
        event.signature = Some(sign_usage_event(&event, &key, "usage-key-1"));

        assert!(verify_usage_event_signature(&event, &key, Some("usage-key-1")).is_ok());
    }

    #[test]
    fn signed_usage_event_rejects_missing_wrong_or_tampered_signature() {
        let key = vec![7; 32];
        let mut event = usage_event();
        assert!(matches!(
            verify_usage_event_signature(&event, &key, Some("usage-key-1")),
            Err(SqlApiError::Unauthorized)
        ));

        event.signature = Some(sign_usage_event(&event, &key, "usage-key-1"));
        assert!(matches!(
            verify_usage_event_signature(&event, &key, Some("other-key")),
            Err(SqlApiError::Unauthorized)
        ));

        event.quantity += 1;
        assert!(matches!(
            verify_usage_event_signature(&event, &key, Some("usage-key-1")),
            Err(SqlApiError::Unauthorized)
        ));
    }

    #[test]
    fn api_key_tokens_store_only_prefix_and_hash() {
        let token = "plmp_01234567890123456789012345678901";
        let prefix = token_prefix(token);
        let hash = api_key_token_hash(token);

        assert!(token.starts_with(&prefix));
        assert_ne!(hash, token);
        assert!(!hash.starts_with("plmp_"));
    }

    #[test]
    fn api_key_viewer_role_is_read_only_for_mutating_routes() {
        assert!(!api_key_role_can_mutate(TeamRole::Viewer));
        assert!(api_key_role_can_mutate(TeamRole::Ci));
        assert!(api_key_role_can_mutate(TeamRole::Developer));
        assert!(api_key_role_can_mutate(TeamRole::Admin));
        assert!(api_key_role_can_mutate(TeamRole::Owner));
    }

    #[test]
    fn api_key_role_management_prevents_admin_escalation() {
        assert!(api_key_role_can_manage_role(
            TeamRole::Owner,
            TeamRole::Owner
        ));
        assert!(api_key_role_can_manage_role(TeamRole::Admin, TeamRole::Ci));
        assert!(api_key_role_can_manage_role(
            TeamRole::Admin,
            TeamRole::Developer
        ));
        assert!(!api_key_role_can_manage_role(
            TeamRole::Admin,
            TeamRole::Owner
        ));
        assert!(!api_key_role_can_manage_role(
            TeamRole::Admin,
            TeamRole::Admin
        ));
        assert!(!api_key_role_can_manage_role(
            TeamRole::Developer,
            TeamRole::Viewer
        ));
    }

    #[test]
    fn api_key_scope_allows_descendant_resources_only() {
        let context = SqlAuthContext {
            actor_id: "api_key:key_project".to_owned(),
            api_key: Some(ApiKey {
                key_id: "key_project".to_owned(),
                token_prefix: "plmp_project".to_owned(),
                name: "Project key".to_owned(),
                organization_id: Some("org_123".to_owned()),
                project_id: Some("project_123".to_owned()),
                environment_id: None,
                role: TeamRole::Admin,
                created_by: "system".to_owned(),
                revoked: false,
            }),
        };

        assert!(context
            .require_scope(ResourceScope::environment(
                "org_123",
                "project_123",
                "env_123"
            ))
            .is_ok());
        assert!(matches!(
            context.require_scope(ResourceScope::environment(
                "org_123",
                "project_other",
                "env_other"
            )),
            Err(SqlApiError::Unauthorized)
        ));
        assert!(matches!(
            context.require_scope(ResourceScope::organization("org_123")),
            Err(SqlApiError::Unauthorized)
        ));
    }

    #[test]
    fn api_key_scope_rejects_sibling_environments() {
        let context = SqlAuthContext {
            actor_id: "api_key:key_env".to_owned(),
            api_key: Some(ApiKey {
                key_id: "key_env".to_owned(),
                token_prefix: "plmp_env".to_owned(),
                name: "Environment key".to_owned(),
                organization_id: Some("org_123".to_owned()),
                project_id: Some("project_123".to_owned()),
                environment_id: Some("env_123".to_owned()),
                role: TeamRole::Ci,
                created_by: "system".to_owned(),
                revoked: false,
            }),
        };

        assert!(context
            .require_scope(ResourceScope::environment(
                "org_123",
                "project_123",
                "env_123"
            ))
            .is_ok());
        assert!(matches!(
            context.require_scope(ResourceScope::environment(
                "org_123",
                "project_123",
                "env_capacity"
            )),
            Err(SqlApiError::Unauthorized)
        ));
    }

    #[test]
    fn requested_cluster_applies_cnpg_and_advances_to_verifying() {
        let plan = Reconciler::new()
            .reconcile(
                &cluster(ClusterLifecycleState::Requested),
                None,
                &[],
                &RuntimeConfig::default(),
            )
            .expect("reconcile succeeds");

        assert_eq!(plan.action, RuntimeAction::Apply);
        assert_eq!(
            plan.next_cluster.lifecycle_state,
            ClusterLifecycleState::Verifying
        );
        // CloudNativePG owns placement; no host assignment is recorded.
        assert!(plan.next_cluster.host_assignment.is_none());
        let manifests = plan.manifests.expect("manifests rendered");
        let yaml = manifests.to_yaml().expect("yaml renders");
        assert!(yaml.contains("\"kind\": \"Cluster\""));
        assert!(yaml.contains("postgresql.cnpg.io/v1"));
    }

    #[test]
    fn ready_cluster_reapplies_for_drift_convergence() {
        let plan = Reconciler::new()
            .reconcile(
                &cluster(ClusterLifecycleState::Ready),
                None,
                &[],
                &RuntimeConfig::default(),
            )
            .expect("reconcile succeeds");

        assert_eq!(plan.action, RuntimeAction::Apply);
        assert_eq!(
            plan.next_cluster.lifecycle_state,
            ClusterLifecycleState::Ready
        );
    }

    #[test]
    fn deleting_cluster_plans_a_delete() {
        let plan = Reconciler::new()
            .reconcile(
                &cluster(ClusterLifecycleState::Deleting),
                None,
                &[],
                &RuntimeConfig::default(),
            )
            .expect("reconcile succeeds");

        assert_eq!(plan.action, RuntimeAction::Delete);
        assert_eq!(
            plan.next_cluster.lifecycle_state,
            ClusterLifecycleState::Deleted
        );
        assert!(plan.manifests.is_some());
    }

    #[test]
    fn stopped_cluster_applies_hibernation() {
        let plan = Reconciler::new()
            .reconcile(
                &cluster(ClusterLifecycleState::Stopped),
                None,
                &[],
                &RuntimeConfig::default(),
            )
            .expect("reconcile succeeds");

        assert_eq!(plan.action, RuntimeAction::Apply);
        assert_eq!(
            plan.next_cluster.lifecycle_state,
            ClusterLifecycleState::Stopped
        );
        let yaml = plan
            .manifests
            .expect("manifests rendered")
            .to_yaml()
            .expect("yaml renders");
        assert!(yaml.contains("cnpg.io/hibernation"));
    }

    #[test]
    fn failed_cluster_is_a_runtime_noop() {
        let plan = Reconciler::new()
            .reconcile(
                &cluster(ClusterLifecycleState::Failed),
                None,
                &[],
                &RuntimeConfig::default(),
            )
            .expect("reconcile succeeds");

        assert_eq!(plan.action, RuntimeAction::None);
        assert!(plan.manifests.is_none());
    }

    #[test]
    fn store_rejects_duplicate_operation_ids() {
        let operation = palimpsest_paas_core::OperationRecord {
            operation_id: "op_123".to_owned(),
            idempotency_key: "request_123".to_owned(),
            target_resource_id: "cluster_123".to_owned(),
            kind: OperationKind::CreateCluster,
            status: OperationStatus::Pending,
            current_step: "requested".to_owned(),
            lease_owner: None,
        };
        let mut store = InMemoryControlPlaneStore::default();

        store
            .insert_operation(operation.clone())
            .expect("first insert succeeds");
        let err = store
            .insert_operation(operation)
            .expect_err("duplicate insert fails");

        assert_eq!(
            err,
            ControlPlaneError::DuplicateOperation("op_123".to_owned())
        );
    }

    #[test]
    fn service_creates_project_environment_and_audits_mutations() {
        let mut service = ControlPlaneService::default();

        service
            .create_organization(
                "user_123",
                Organization {
                    organization_id: "org_123".to_owned(),
                    name: "Acme".to_owned(),
                },
            )
            .expect("organization is created");
        service
            .create_project(
                "user_123",
                Project {
                    project_id: "project_123".to_owned(),
                    organization_id: "org_123".to_owned(),
                    name: "App".to_owned(),
                },
            )
            .expect("project is created");
        service
            .create_environment(
                "user_123",
                Environment {
                    environment_id: "env_123".to_owned(),
                    organization_id: "org_123".to_owned(),
                    project_id: "project_123".to_owned(),
                    name: "Production".to_owned(),
                    region: "us-east-1".to_owned(),
                },
            )
            .expect("environment is created");

        assert!(service.store().environment("env_123").is_some());
        assert_eq!(service.store().audit_events().len(), 3);
        assert_eq!(
            service.store().audit_events()[2].action,
            "environment.create"
        );
    }

    #[test]
    fn service_uploads_config_and_reports_diff() {
        let mut service = service_with_environment();

        service
            .upload_config(
                "user_123",
                ConfigVersion {
                    config_version: "config_001".to_owned(),
                    environment_id: "env_123".to_owned(),
                    rendered_hash: "hash_a".to_owned(),
                    status: ConfigVersionStatus::Validated,
                },
            )
            .expect("config is uploaded");
        service
            .upload_config(
                "user_123",
                ConfigVersion {
                    config_version: "config_002".to_owned(),
                    environment_id: "env_123".to_owned(),
                    rendered_hash: "hash_b".to_owned(),
                    status: ConfigVersionStatus::Uploaded,
                },
            )
            .expect("config is uploaded");

        let diff = service
            .config_diff("config_001", "config_002")
            .expect("diff succeeds");

        assert!(diff.rendered_hash_changed);
        assert!(diff.status_changed);
        assert_eq!(
            service
                .store()
                .audit_events()
                .last()
                .map(|event| event.action.as_str()),
            Some("config.upload")
        );
    }

    #[test]
    fn service_rejects_updates_to_deployed_config_versions() {
        let mut service = service_with_environment();

        service
            .upload_config(
                "user_123",
                ConfigVersion {
                    config_version: "config_001".to_owned(),
                    environment_id: "env_123".to_owned(),
                    rendered_hash: "hash_a".to_owned(),
                    status: ConfigVersionStatus::Deployed,
                },
            )
            .expect("deployed config is inserted");

        let err = service
            .upload_config(
                "user_123",
                ConfigVersion {
                    config_version: "config_001".to_owned(),
                    environment_id: "env_123".to_owned(),
                    rendered_hash: "hash_b".to_owned(),
                    status: ConfigVersionStatus::Validated,
                },
            )
            .expect_err("deployed config cannot be updated");

        assert!(matches!(err, ControlPlaneError::ImmutableResource(_)));
        assert_eq!(
            service
                .store()
                .config_version("config_001")
                .expect("config exists")
                .rendered_hash,
            "hash_a"
        );
    }

    #[tokio::test]
    async fn http_api_create_organization_writes_audit_event() {
        let service = Arc::new(Mutex::new(ControlPlaneService::default()));
        let app = control_plane_router(Arc::clone(&service));
        let body = serde_json::json!({
            "organization_id": "org_123",
            "name": "Acme"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/organizations")
                    .header("content-type", "application/json")
                    .header("x-actor-id", "user_123")
                    .body(Body::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        let service = service.lock().expect("service lock");
        assert!(service.store().organization("org_123").is_some());
        assert_eq!(service.store().audit_events().len(), 1);
        assert_eq!(service.store().audit_events()[0].actor_id, "user_123");
    }

    #[tokio::test]
    async fn sql_router_builds_with_node_agent_routes() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/palimpsest_control")
            .expect("lazy pool should not connect");

        let _app = sql_control_plane_router(Arc::new(sql_store::SqlControlPlaneStore::new(pool)));
    }

    #[tokio::test]
    async fn http_api_creates_managed_postgres_cluster() {
        let service = Arc::new(Mutex::new(service_with_environment()));
        let app = control_plane_router(Arc::clone(&service));
        let body = serde_json::json!({
            "cluster_id": "cluster_123",
            "organization_id": "org_123",
            "project_id": "project_123",
            "environment_id": "env_123",
            "region": "us-east-1",
            "postgres_version": "18",
            "tier": "dev",
            "storage_gib": 20,
            "lifecycle_state": "requested",
            "host_assignment": null
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/managed-postgres/clusters")
                    .header("content-type", "application/json")
                    .header("x-actor-id", "user_123")
                    .body(Body::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        let service = service.lock().expect("service lock");
        assert!(service.store().cluster("cluster_123").is_some());
        assert_eq!(
            service
                .store()
                .audit_events()
                .last()
                .map(|event| event.action.as_str()),
            Some("managed_postgres_cluster.create")
        );
    }

    #[tokio::test]
    async fn http_api_creates_sync_deployment() {
        let service = Arc::new(Mutex::new(service_with_cluster()));
        let app = control_plane_router(Arc::clone(&service));
        let body = serde_json::json!({
            "deployment_id": "deployment_123",
            "organization_id": "org_123",
            "project_id": "project_123",
            "environment_id": "env_123",
            "managed_postgres_cluster_id": "cluster_123",
            "config_version": "config_001",
            "lifecycle_state": "requested"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/sync-deployments")
                    .header("content-type", "application/json")
                    .header("x-actor-id", "user_123")
                    .body(Body::from(body.to_string()))
                    .expect("request builds"),
            )
            .await
            .expect("request succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        let service = service.lock().expect("service lock");
        let deployment = service
            .store()
            .sync_deployment("deployment_123")
            .expect("deployment exists");
        assert_eq!(
            deployment.lifecycle_state,
            SyncDeploymentLifecycleState::Requested
        );
        assert_eq!(
            service
                .store()
                .audit_events()
                .last()
                .map(|event| event.action.as_str()),
            Some("sync_deployment.create")
        );
    }

    fn service_with_environment() -> ControlPlaneService {
        let mut service = ControlPlaneService::default();
        service
            .create_organization(
                "user_123",
                Organization {
                    organization_id: "org_123".to_owned(),
                    name: "Acme".to_owned(),
                },
            )
            .expect("organization is created");
        service
            .create_project(
                "user_123",
                Project {
                    project_id: "project_123".to_owned(),
                    organization_id: "org_123".to_owned(),
                    name: "App".to_owned(),
                },
            )
            .expect("project is created");
        service
            .create_environment(
                "user_123",
                Environment {
                    environment_id: "env_123".to_owned(),
                    organization_id: "org_123".to_owned(),
                    project_id: "project_123".to_owned(),
                    name: "Production".to_owned(),
                    region: "us-east-1".to_owned(),
                },
            )
            .expect("environment is created");
        service
    }

    fn service_with_cluster() -> ControlPlaneService {
        let mut service = service_with_environment();
        service
            .create_managed_postgres_cluster("user_123", cluster(ClusterLifecycleState::Ready))
            .expect("cluster is created");
        service
    }
}
