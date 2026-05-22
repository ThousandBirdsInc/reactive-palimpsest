// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;

use palimpsest_paas_sync_wrapper::{
    plan_initial_start, plan_supervisor_transition, DeploymentDatabase, DeploymentSpec,
    DrainPolicy, SupervisorAction, SyncAuth, SyncRuntime, TelemetryLabels,
};

const CONFIG_PATH: &str = "/etc/palimpsest/palimpsest.toml";

#[test]
fn initial_start_plan_renders_config_before_starting_process() {
    let spec = deployment_spec("config_001");

    let plan = plan_initial_start(&spec, CONFIG_PATH);

    assert_eq!(plan.deployment_id, "sync_123");
    assert_eq!(plan.config_version, "config_001");
    assert_eq!(
        plan.actions,
        vec![
            SupervisorAction::RenderConfig,
            SupervisorAction::StartProcess {
                binary: "palimpsest".to_owned(),
                config_path: CONFIG_PATH.to_owned(),
            },
        ]
    );
}

#[test]
fn safe_reload_plan_does_not_drain_or_restart() {
    let old = deployment_spec("config_001");
    let new = deployment_spec("config_002");

    let plan = plan_supervisor_transition(
        &old,
        &new,
        CONFIG_PATH,
        DrainPolicy {
            timeout_seconds: 60,
        },
    );

    assert_eq!(plan.config_version, "config_002");
    assert_eq!(
        plan.actions,
        vec![
            SupervisorAction::RenderConfig,
            SupervisorAction::ReloadInPlace,
        ]
    );
}

#[test]
fn database_changes_plan_drain_stop_and_restart_sequence() {
    let old = deployment_spec("config_001");
    let mut new = deployment_spec("config_002");
    new.database.slot_name = "palimpsest_slot_next".to_owned();

    let plan = plan_supervisor_transition(
        &old,
        &new,
        CONFIG_PATH,
        DrainPolicy {
            timeout_seconds: 75,
        },
    );

    assert_eq!(
        plan.actions,
        vec![
            SupervisorAction::RenderConfig,
            SupervisorAction::DrainConnections {
                timeout_seconds: 75,
            },
            SupervisorAction::StopProcess,
            SupervisorAction::StartProcess {
                binary: "palimpsest".to_owned(),
                config_path: CONFIG_PATH.to_owned(),
            },
        ]
    );
}

#[test]
fn identity_changes_are_blocked_before_rendering_or_restarting() {
    let old = deployment_spec("config_001");
    let mut new = deployment_spec("config_002");
    new.environment_id = "env_other".to_owned();

    let plan = plan_supervisor_transition(&old, &new, CONFIG_PATH, DrainPolicy::default());

    assert_eq!(plan.config_version, "config_002");
    assert!(matches!(
        plan.actions.as_slice(),
        [SupervisorAction::Blocked { reason }] if reason == "deployment identity changed"
    ));
}

fn deployment_spec(config_version: &str) -> DeploymentSpec {
    DeploymentSpec {
        deployment_id: "sync_123".to_owned(),
        environment_id: "env_123".to_owned(),
        region: "us-east-1".to_owned(),
        shard_id: "shard_0".to_owned(),
        config_version: config_version.to_owned(),
        database: DeploymentDatabase {
            url: "postgres://cluster_123_replication:secret@postgres:5432/postgres".to_owned(),
            slot_name: "palimpsest_slot".to_owned(),
            publication: "palimpsest_publication".to_owned(),
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
    }
}
