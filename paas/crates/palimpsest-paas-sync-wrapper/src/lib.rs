// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Managed SyncDeployment wrapper primitives.
//!
//! The wrapper renders ordinary `palimpsest serve` configuration from
//! platform deployment intent. It does not change the standalone server
//! contract.

use std::collections::BTreeMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedDeploymentSpec {
    pub spec: DeploymentSpec,
    pub signature: DeploymentSignature,
}

impl SignedDeploymentSpec {
    pub fn validate_signature_metadata(&self) -> Result<(), SyncWrapperError> {
        if self.signature.key_id.trim().is_empty() {
            return Err(SyncWrapperError::MissingSignatureField("key_id"));
        }
        if self.signature.algorithm.trim().is_empty() {
            return Err(SyncWrapperError::MissingSignatureField("algorithm"));
        }
        if self.signature.signature.trim().is_empty() {
            return Err(SyncWrapperError::MissingSignatureField("signature"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentSignature {
    pub key_id: String,
    pub algorithm: String,
    pub public_key_base64: Option<String>,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentSpec {
    pub deployment_id: String,
    pub environment_id: String,
    pub region: String,
    pub shard_id: String,
    pub config_version: String,
    pub database: DeploymentDatabase,
    pub sync: SyncRuntime,
    #[serde(default)]
    pub telemetry: TelemetryLabels,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentDatabase {
    pub url: String,
    pub slot_name: String,
    pub publication: String,
    #[serde(default)]
    pub password_secret_ref: Option<DeploymentSecretRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentSecretRef {
    pub provider: String,
    pub external_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRuntime {
    pub grpc_addr: String,
    pub metrics_addr: Option<String>,
    #[serde(default)]
    pub auth: SyncAuth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncAuth {
    Anonymous,
    Jwt {
        secret: String,
        issuer: String,
        audience: String,
        #[serde(default)]
        claim_to_field: BTreeMap<String, String>,
    },
}

impl Default for SyncAuth {
    fn default() -> Self {
        Self::Anonymous
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryLabels {
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedPalimpsestConfig {
    pub toml: String,
}

pub fn render_palimpsest_config(
    signed: &SignedDeploymentSpec,
) -> Result<RenderedPalimpsestConfig, SyncWrapperError> {
    signed.validate_signature_metadata()?;
    let config = PalimpsestConfig::from(&signed.spec);
    let toml = toml::to_string_pretty(&config).map_err(SyncWrapperError::RenderToml)?;
    Ok(RenderedPalimpsestConfig { toml })
}

pub fn render_palimpsest_config_with_secrets<R>(
    signed: &SignedDeploymentSpec,
    resolver: &R,
) -> Result<RenderedPalimpsestConfig, SyncWrapperError>
where
    R: SecretResolver,
{
    signed.validate_signature_metadata()?;
    let spec = materialize_database_secrets(&signed.spec, resolver)?;
    let config = PalimpsestConfig::from(&spec);
    let toml = toml::to_string_pretty(&config).map_err(SyncWrapperError::RenderToml)?;
    Ok(RenderedPalimpsestConfig { toml })
}

pub trait SecretResolver {
    fn resolve_secret(&self, secret_ref: &DeploymentSecretRef) -> Result<String, SyncWrapperError>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InMemorySecretResolver {
    secrets: BTreeMap<(String, String), String>,
}

impl InMemorySecretResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        mut self,
        provider: impl Into<String>,
        external_ref: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.secrets
            .insert((provider.into(), external_ref.into()), value.into());
        self
    }
}

impl SecretResolver for InMemorySecretResolver {
    fn resolve_secret(&self, secret_ref: &DeploymentSecretRef) -> Result<String, SyncWrapperError> {
        self.secrets
            .get(&(secret_ref.provider.clone(), secret_ref.external_ref.clone()))
            .cloned()
            .ok_or_else(|| SyncWrapperError::MissingSecretRef {
                provider: secret_ref.provider.clone(),
                external_ref: secret_ref.external_ref.clone(),
            })
    }
}

pub fn materialize_database_secrets<R>(
    spec: &DeploymentSpec,
    resolver: &R,
) -> Result<DeploymentSpec, SyncWrapperError>
where
    R: SecretResolver,
{
    let mut resolved = spec.clone();
    if let Some(secret_ref) = spec.database.password_secret_ref.as_ref() {
        let password = resolver.resolve_secret(secret_ref)?;
        resolved.database.url = inject_database_password(&spec.database.url, &password)?;
    }
    Ok(resolved)
}

fn inject_database_password(url: &str, password: &str) -> Result<String, SyncWrapperError> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(SyncWrapperError::InvalidDatabaseUrl(url.to_owned()));
    };
    let Some((userinfo, host_and_path)) = rest.split_once('@') else {
        return Err(SyncWrapperError::InvalidDatabaseUrl(url.to_owned()));
    };
    let username = userinfo.split(':').next().unwrap_or_default();
    if username.trim().is_empty() {
        return Err(SyncWrapperError::InvalidDatabaseUrl(url.to_owned()));
    }
    Ok(format!(
        "{scheme}://{username}:{}@{host_and_path}",
        percent_encode_secret(password)
    ))
}

fn percent_encode_secret(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

pub fn verify_ed25519_signature(signed: &SignedDeploymentSpec) -> Result<(), SyncWrapperError> {
    signed.validate_signature_metadata()?;
    if signed.signature.algorithm != "ed25519" {
        return Err(SyncWrapperError::UnsupportedSignatureAlgorithm(
            signed.signature.algorithm.clone(),
        ));
    }
    let public_key = signed
        .signature
        .public_key_base64
        .as_ref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(SyncWrapperError::MissingSignatureField("public_key_base64"))?;
    let public_key = BASE64
        .decode(public_key)
        .map_err(SyncWrapperError::DecodeBase64)?;
    let signature = BASE64
        .decode(&signed.signature.signature)
        .map_err(SyncWrapperError::DecodeBase64)?;
    let payload = serde_json::to_vec(&signed.spec).map_err(SyncWrapperError::SerializeSpec)?;

    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(&payload, &signature)
        .map_err(|_| SyncWrapperError::VerifySignature)
}

pub fn render_verified_palimpsest_config(
    signed: &SignedDeploymentSpec,
) -> Result<RenderedPalimpsestConfig, SyncWrapperError> {
    verify_ed25519_signature(signed)?;
    render_palimpsest_config(signed)
}

#[derive(Debug, Serialize)]
struct PalimpsestConfig<'a> {
    grpc: GrpcConfig<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<MetricsConfig<'a>>,
    auth: &'a SyncAuth,
    upstream: UpstreamConfig<'a>,
    managed: ManagedRuntimeConfig<'a>,
}

impl<'a> From<&'a DeploymentSpec> for PalimpsestConfig<'a> {
    fn from(spec: &'a DeploymentSpec) -> Self {
        Self {
            grpc: GrpcConfig {
                addr: &spec.sync.grpc_addr,
            },
            metrics: spec
                .sync
                .metrics_addr
                .as_ref()
                .map(|addr| MetricsConfig { addr }),
            auth: &spec.sync.auth,
            upstream: UpstreamConfig {
                url: &spec.database.url,
                slot_name: &spec.database.slot_name,
                publication: &spec.database.publication,
            },
            managed: ManagedRuntimeConfig {
                deployment_id: &spec.deployment_id,
                environment_id: &spec.environment_id,
                region: &spec.region,
                shard_id: &spec.shard_id,
                config_version: &spec.config_version,
                organization_id: spec.telemetry.organization_id.as_deref(),
                project_id: spec.telemetry.project_id.as_deref(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct GrpcConfig<'a> {
    addr: &'a str,
}

#[derive(Debug, Serialize)]
struct MetricsConfig<'a> {
    addr: &'a str,
}

#[derive(Debug, Serialize)]
struct UpstreamConfig<'a> {
    url: &'a str,
    slot_name: &'a str,
    publication: &'a str,
}

#[derive(Debug, Serialize)]
struct ManagedRuntimeConfig<'a> {
    deployment_id: &'a str,
    environment_id: &'a str,
    region: &'a str,
    shard_id: &'a str,
    config_version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentHealth {
    Healthy,
    Degraded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncDeploymentHealth {
    pub postgres: ComponentHealth,
    pub wal_slot: ComponentHealth,
    pub dataflow: ComponentHealth,
    pub gateway: ComponentHealth,
    pub config: ComponentHealth,
}

impl SyncDeploymentHealth {
    pub fn overall(&self) -> ComponentHealth {
        let components = [
            self.postgres,
            self.wal_slot,
            self.dataflow,
            self.gateway,
            self.config,
        ];
        if components.contains(&ComponentHealth::Failed) {
            ComponentHealth::Failed
        } else if components.contains(&ComponentHealth::Degraded) {
            ComponentHealth::Degraded
        } else if components.contains(&ComponentHealth::Unknown) {
            ComponentHealth::Unknown
        } else {
            ComponentHealth::Healthy
        }
    }

    pub fn actionable_status(&self) -> &'static str {
        match self.overall() {
            ComponentHealth::Healthy => "ready",
            ComponentHealth::Degraded => "degraded",
            ComponentHealth::Failed if self.config == ComponentHealth::Failed => "config_failed",
            ComponentHealth::Failed if self.postgres == ComponentHealth::Failed => {
                "postgres_failed"
            }
            ComponentHealth::Failed if self.wal_slot == ComponentHealth::Failed => "wal_failed",
            ComponentHealth::Failed if self.dataflow == ComponentHealth::Failed => {
                "dataflow_failed"
            }
            ComponentHealth::Failed if self.gateway == ComponentHealth::Failed => "gateway_failed",
            ComponentHealth::Failed => "failed",
            ComponentHealth::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadClassification {
    SafeReload,
    DrainRestart,
    Blocked,
}

pub fn classify_reload(old: &DeploymentSpec, new: &DeploymentSpec) -> ReloadClassification {
    if old.deployment_id != new.deployment_id
        || old.environment_id != new.environment_id
        || old.region != new.region
        || old.shard_id != new.shard_id
    {
        return ReloadClassification::Blocked;
    }

    if old.database != new.database || old.sync.grpc_addr != new.sync.grpc_addr {
        return ReloadClassification::DrainRestart;
    }

    ReloadClassification::SafeReload
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupervisorPlan {
    pub deployment_id: String,
    pub config_version: String,
    pub actions: Vec<SupervisorAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SupervisorAction {
    RenderConfig,
    StartProcess { binary: String, config_path: String },
    ReloadInPlace,
    DrainConnections { timeout_seconds: u64 },
    StopProcess,
    Blocked { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainPolicy {
    pub timeout_seconds: u64,
}

impl Default for DrainPolicy {
    fn default() -> Self {
        Self {
            timeout_seconds: 30,
        }
    }
}

pub fn plan_initial_start(new: &DeploymentSpec, config_path: &str) -> SupervisorPlan {
    SupervisorPlan {
        deployment_id: new.deployment_id.clone(),
        config_version: new.config_version.clone(),
        actions: vec![
            SupervisorAction::RenderConfig,
            SupervisorAction::StartProcess {
                binary: "palimpsest".to_owned(),
                config_path: config_path.to_owned(),
            },
        ],
    }
}

pub fn plan_supervisor_transition(
    old: &DeploymentSpec,
    new: &DeploymentSpec,
    config_path: &str,
    drain_policy: DrainPolicy,
) -> SupervisorPlan {
    let actions = match classify_reload(old, new) {
        ReloadClassification::SafeReload => {
            vec![
                SupervisorAction::RenderConfig,
                SupervisorAction::ReloadInPlace,
            ]
        }
        ReloadClassification::DrainRestart => {
            vec![
                SupervisorAction::RenderConfig,
                SupervisorAction::DrainConnections {
                    timeout_seconds: drain_policy.timeout_seconds,
                },
                SupervisorAction::StopProcess,
                SupervisorAction::StartProcess {
                    binary: "palimpsest".to_owned(),
                    config_path: config_path.to_owned(),
                },
            ]
        }
        ReloadClassification::Blocked => {
            vec![SupervisorAction::Blocked {
                reason: "deployment identity changed".to_owned(),
            }]
        }
    };

    SupervisorPlan {
        deployment_id: new.deployment_id.clone(),
        config_version: new.config_version.clone(),
        actions,
    }
}

#[derive(Debug, Error)]
pub enum SyncWrapperError {
    #[error("deployment signature is missing {0}")]
    MissingSignatureField(&'static str),
    #[error("unsupported deployment signature algorithm '{0}'")]
    UnsupportedSignatureAlgorithm(String),
    #[error("decode signature material: {0}")]
    DecodeBase64(#[source] base64::DecodeError),
    #[error("serialize deployment spec for signature verification: {0}")]
    SerializeSpec(#[source] serde_json::Error),
    #[error("deployment signature verification failed")]
    VerifySignature,
    #[error("render palimpsest config: {0}")]
    RenderToml(#[source] toml::ser::Error),
    #[error("missing secret ref {provider}:{external_ref}")]
    MissingSecretRef {
        provider: String,
        external_ref: String,
    },
    #[error("invalid database url for secret injection: {0}")]
    InvalidDatabaseUrl(String),
}

#[cfg(test)]
mod tests {
    use ring::signature::{Ed25519KeyPair, KeyPair};

    use super::*;

    #[test]
    fn renders_palimpest_config_from_signed_spec() {
        let signed = signed_spec();

        let rendered = render_palimpsest_config(&signed).expect("render succeeds");

        assert!(rendered.toml.contains("[grpc]"));
        assert!(rendered.toml.contains("addr = \"0.0.0.0:50051\""));
        assert!(rendered.toml.contains("[upstream]"));
        assert!(rendered.toml.contains("slot_name = \"palimpsest\""));
        assert!(rendered.toml.contains("[managed]"));
        assert!(rendered.toml.contains("deployment_id = \"sync_123\""));
    }

    #[test]
    fn renders_database_url_with_resolved_password_secret() {
        let mut signed = signed_spec();
        signed.spec.database.url =
            "postgres://cluster_123_replication@postgres:5432/postgres".to_owned();
        signed.spec.database.password_secret_ref = Some(DeploymentSecretRef {
            provider: "local-dev".to_owned(),
            external_ref: "managed-postgres/cluster_123/replication/password".to_owned(),
        });
        let resolver = InMemorySecretResolver::new().insert(
            "local-dev",
            "managed-postgres/cluster_123/replication/password",
            "secret with @ chars",
        );

        let rendered = render_palimpsest_config_with_secrets(&signed, &resolver)
            .expect("render with secrets succeeds");

        assert!(rendered.toml.contains(
            "url = \"postgres://cluster_123_replication:secret%20with%20%40%20chars@postgres:5432/postgres\""
        ));
    }

    #[test]
    fn missing_database_password_secret_fails_render() {
        let mut signed = signed_spec();
        signed.spec.database.password_secret_ref = Some(DeploymentSecretRef {
            provider: "local-dev".to_owned(),
            external_ref: "missing".to_owned(),
        });
        let resolver = InMemorySecretResolver::new();

        let err = render_palimpsest_config_with_secrets(&signed, &resolver)
            .expect_err("missing secret fails");

        assert!(matches!(err, SyncWrapperError::MissingSecretRef { .. }));
    }

    #[test]
    fn signature_metadata_is_required_before_rendering() {
        let mut signed = signed_spec();
        signed.signature.signature.clear();

        let err = render_palimpsest_config(&signed).expect_err("missing signature fails");

        assert!(matches!(
            err,
            SyncWrapperError::MissingSignatureField("signature")
        ));
    }

    #[test]
    fn reload_classification_distinguishes_safe_restart_and_blocked() {
        let old = signed_spec().spec;
        let mut safe = old.clone();
        safe.config_version = "config_002".to_owned();
        assert_eq!(
            classify_reload(&old, &safe),
            ReloadClassification::SafeReload
        );

        let mut restart = old.clone();
        restart.database.slot_name = "palimpsest_new".to_owned();
        assert_eq!(
            classify_reload(&old, &restart),
            ReloadClassification::DrainRestart
        );

        let mut blocked = old.clone();
        blocked.environment_id = "env_other".to_owned();
        assert_eq!(
            classify_reload(&old, &blocked),
            ReloadClassification::Blocked
        );
    }

    #[test]
    fn health_status_prioritizes_actionable_component() {
        let health = SyncDeploymentHealth {
            postgres: ComponentHealth::Healthy,
            wal_slot: ComponentHealth::Failed,
            dataflow: ComponentHealth::Healthy,
            gateway: ComponentHealth::Healthy,
            config: ComponentHealth::Healthy,
        };

        assert_eq!(health.overall(), ComponentHealth::Failed);
        assert_eq!(health.actionable_status(), "wal_failed");
    }

    #[test]
    fn supervisor_initial_start_renders_config_and_starts_process() {
        let spec = signed_spec().spec;

        let plan = plan_initial_start(&spec, "/etc/palimpsest/palimpsest.toml");

        assert_eq!(
            plan.actions,
            vec![
                SupervisorAction::RenderConfig,
                SupervisorAction::StartProcess {
                    binary: "palimpsest".to_owned(),
                    config_path: "/etc/palimpsest/palimpsest.toml".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn supervisor_transition_plans_drain_restart_for_database_change() {
        let old = signed_spec().spec;
        let mut new = old.clone();
        new.database.slot_name = "palimpsest_new".to_owned();

        let plan = plan_supervisor_transition(
            &old,
            &new,
            "/etc/palimpsest/palimpsest.toml",
            DrainPolicy {
                timeout_seconds: 45,
            },
        );

        assert_eq!(
            plan.actions,
            vec![
                SupervisorAction::RenderConfig,
                SupervisorAction::DrainConnections {
                    timeout_seconds: 45
                },
                SupervisorAction::StopProcess,
                SupervisorAction::StartProcess {
                    binary: "palimpsest".to_owned(),
                    config_path: "/etc/palimpsest/palimpsest.toml".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn supervisor_transition_blocks_identity_change() {
        let old = signed_spec().spec;
        let mut new = old.clone();
        new.environment_id = "env_other".to_owned();

        let plan = plan_supervisor_transition(
            &old,
            &new,
            "/etc/palimpsest/palimpsest.toml",
            DrainPolicy::default(),
        );

        assert!(matches!(
            plan.actions.as_slice(),
            [SupervisorAction::Blocked { .. }]
        ));
    }

    #[test]
    fn verifies_ed25519_signed_deployment_spec() {
        let signed = signed_spec_with_valid_signature();

        verify_ed25519_signature(&signed).expect("signature verifies");
    }

    #[test]
    fn rejects_tampered_signed_deployment_spec() {
        let mut signed = signed_spec_with_valid_signature();
        signed.spec.config_version = "tampered".to_owned();

        let err = verify_ed25519_signature(&signed).expect_err("tampered spec fails");

        assert!(matches!(err, SyncWrapperError::VerifySignature));
    }

    fn signed_spec() -> SignedDeploymentSpec {
        SignedDeploymentSpec {
            spec: DeploymentSpec {
                deployment_id: "sync_123".to_owned(),
                environment_id: "env_123".to_owned(),
                region: "us-east-1".to_owned(),
                shard_id: "shard_0".to_owned(),
                config_version: "config_001".to_owned(),
                database: DeploymentDatabase {
                    url: "postgres://palimpsest_repl:secret@postgres:5432/app".to_owned(),
                    slot_name: "palimpsest".to_owned(),
                    publication: "palimpsest_pub".to_owned(),
                    password_secret_ref: None,
                },
                sync: SyncRuntime {
                    grpc_addr: "0.0.0.0:50051".to_owned(),
                    metrics_addr: Some("0.0.0.0:9090".to_owned()),
                    auth: SyncAuth::Anonymous,
                },
                telemetry: TelemetryLabels {
                    organization_id: Some("org_123".to_owned()),
                    project_id: Some("project_123".to_owned()),
                    extra: BTreeMap::new(),
                },
            },
            signature: DeploymentSignature {
                key_id: "control-plane-key-1".to_owned(),
                algorithm: "ed25519".to_owned(),
                public_key_base64: None,
                signature: "placeholder".to_owned(),
            },
        }
    }

    fn signed_spec_with_valid_signature() -> SignedDeploymentSpec {
        let mut signed = signed_spec();
        let key_pair = Ed25519KeyPair::from_seed_unchecked(&[7_u8; 32]).expect("seed is valid");
        let payload = serde_json::to_vec(&signed.spec).expect("spec serializes");
        let signature = key_pair.sign(&payload);
        signed.signature.public_key_base64 = Some(BASE64.encode(key_pair.public_key().as_ref()));
        signed.signature.signature = BASE64.encode(signature.as_ref());
        signed
    }
}
