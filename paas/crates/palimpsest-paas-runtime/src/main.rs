// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! CLI for the Palimpsest managed-Postgres Kubernetes runtime.
//!
//! Replaces the former `palimpsest-paas-node-agent`. Instead of leasing
//! host-local commands and running PostgreSQL binaries, this renders
//! CloudNativePG manifests from desired state and reconciles them onto a
//! Kubernetes cluster with `kubectl`.

#![allow(clippy::doc_markdown)]

use std::{fs, path::PathBuf, process::ExitCode};

use palimpsest_paas_core::ManagedPostgresCluster;
use palimpsest_paas_runtime::{KubectlApplier, RenderedManifests, RuntimeConfig};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "help" || command == "--help" || command == "-h" => {
            print_help();
            Ok(())
        }
        [command, cluster_path] if command == "render" => {
            let cluster = read_cluster(cluster_path)?;
            let manifests =
                RenderedManifests::render(&cluster, None, &[], &RuntimeConfig::from_env())
                    .map_err(|err| err.to_string())?;
            print!("{}", manifests.to_yaml().map_err(|err| err.to_string())?);
            Ok(())
        }
        [command, cluster_path] if command == "reconcile" => {
            let cluster = read_cluster(cluster_path)?;
            let manifests =
                RenderedManifests::render(&cluster, None, &[], &RuntimeConfig::from_env())
                    .map_err(|err| err.to_string())?;
            let yaml = manifests.to_yaml().map_err(|err| err.to_string())?;
            KubectlApplier::default()
                .apply(&yaml)
                .map_err(|err| err.to_string())?;
            println!(
                "applied CloudNativePG manifests for cluster {}",
                cluster.cluster_id
            );
            Ok(())
        }
        [command, cluster_path, backup_id] if command == "backup" => {
            let cluster = read_cluster(cluster_path)?;
            let config = RuntimeConfig::from_env();
            let backup = palimpsest_paas_runtime::render_backup_resource(&cluster, backup_id, &config);
            let json = serde_json::to_string_pretty(&backup).map_err(|err| err.to_string())?;
            KubectlApplier::default()
                .apply(&json)
                .map_err(|err| err.to_string())?;
            println!(
                "requested CloudNativePG backup {} for cluster {}",
                palimpsest_paas_runtime::backup_resource_name(&cluster, backup_id),
                cluster.cluster_id
            );
            Ok(())
        }
        [command, cluster_path] if command == "delete" => {
            let cluster = read_cluster(cluster_path)?;
            let manifests =
                RenderedManifests::render(&cluster, None, &[], &RuntimeConfig::from_env())
                    .map_err(|err| err.to_string())?;
            let yaml = manifests.to_yaml().map_err(|err| err.to_string())?;
            KubectlApplier::default()
                .delete(&yaml)
                .map_err(|err| err.to_string())?;
            println!(
                "deleted CloudNativePG manifests for cluster {}",
                cluster.cluster_id
            );
            Ok(())
        }
        [] => {
            print_help();
            Ok(())
        }
        _ => Err(
            "usage: palimpsest-paas-runtime render|reconcile|delete <cluster.json> | backup <cluster.json> <backup_id>"
                .to_owned(),
        ),
    }
}

fn read_cluster(path: &str) -> Result<ManagedPostgresCluster, String> {
    let path = PathBuf::from(path);
    let raw = fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {}: {err}", path.display()))
}

fn print_help() {
    println!(
        "palimpsest-paas-runtime\n\n\
         Renders and reconciles CloudNativePG manifests for managed Postgres.\n\n\
         Usage:\n  palimpsest-paas-runtime <command> <cluster.json>\n\n\
         Commands:\n  render <cluster.json>          Render CloudNativePG manifests to stdout\n  reconcile <cluster.json>       Render and `kubectl apply --server-side`\n  delete <cluster.json>          Render and `kubectl delete` the manifests\n  backup <cluster.json> <id>     Request an on-demand CloudNativePG backup\n  help                           Show this message\n\n\
         Configuration (env):\n  PALIMPSEST_PAAS_RUNTIME_POSTGRES_IMAGE            managed Postgres image repo\n  PALIMPSEST_PAAS_RUNTIME_STORAGE_CLASS            volume StorageClass\n  PALIMPSEST_PAAS_RUNTIME_BACKUP_OBJECT_STORE      base object-store URI for backups\n  PALIMPSEST_PAAS_RUNTIME_BACKUP_CREDENTIALS_SECRET  backup credentials Secret name\n  PALIMPSEST_PAAS_RUNTIME_DEFAULT_NAMESPACE        namespace when not per-environment\n  PALIMPSEST_PAAS_RUNTIME_NAMESPACE_PER_ENVIRONMENT  1/true for per-environment namespaces\n  PALIMPSEST_PAAS_KUBECTL                          kubectl binary (default: kubectl)\n  PALIMPSEST_PAAS_KUBE_CONTEXT                     kube context to target"
    );
}
