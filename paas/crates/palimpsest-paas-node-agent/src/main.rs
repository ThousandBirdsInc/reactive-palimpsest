// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{fs, path::PathBuf, process::ExitCode};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palimpsest_paas_core::NodeAgentCommand;
use palimpsest_paas_node_agent::{
    failed_command_result, result_from_command_plan, result_from_execution_report, AgentConfig,
    ControlPlaneClientConfig, DockerPostgresRunner, HttpControlPlaneClient, NodeAgent,
    SystemProcessRunner,
};

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
        [command, command_path] if command == "plan" => {
            let command = read_command(command_path)?;
            let plan = NodeAgent::new(AgentConfig::from_env())
                .plan(&command)
                .map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, command_path] if command == "apply" => {
            let command = read_command(command_path)?;
            let report = NodeAgent::new(AgentConfig::from_env())
                .execute_with_runner(&command, &SystemProcessRunner)
                .map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, command_path] if command == "apply-container" => {
            let command = read_command(command_path)?;
            let agent = NodeAgent::new(AgentConfig::from_env());
            let runner = DockerPostgresRunner::postgres18(agent.config().runtime_root.clone());
            let report = agent
                .execute_with_runner(&command, &runner)
                .map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, control_plane_url] if command == "register" => {
            let agent = NodeAgent::new(AgentConfig::from_env());
            let client = control_plane_client(control_plane_url, &agent)?;
            client
                .register_host(&agent.local_host_description())
                .map_err(|err| err.to_string())?;
            println!("registered host {}", agent.config().host_id);
            Ok(())
        }
        [command, control_plane_url] if command == "heartbeat" => {
            let agent = NodeAgent::new(AgentConfig::from_env());
            let client = control_plane_client(control_plane_url, &agent)?;
            client
                .record_heartbeat(&agent.heartbeat())
                .map_err(|err| err.to_string())?;
            println!("recorded heartbeat for host {}", agent.config().host_id);
            Ok(())
        }
        [command, control_plane_url] if command == "hardening-check" => {
            let agent = NodeAgent::new(AgentConfig::from_env());
            let client = control_plane_client(control_plane_url, &agent)?;
            let check = client
                .record_hardening_check(&agent.hardening_check())
                .map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&check).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, control_plane_url] if command == "poll-once" => {
            let agent = NodeAgent::new(AgentConfig::from_env());
            let client = control_plane_client(control_plane_url, &agent)?;
            let Some(queued) = client.lease_next_command().map_err(|err| err.to_string())? else {
                println!("no command available");
                return Ok(());
            };

            let mut result = match agent.execute_with_runner(&queued.command, &SystemProcessRunner) {
                Ok(report) => result_from_execution_report(&agent.config().host_id, &report),
                Err(err) => failed_command_result(
                    &agent.config().host_id,
                    &queued.command.command_id,
                    &err,
                ),
            };
            result.operation_token = queued.operation_token;
            client
                .complete_command(&result)
                .map_err(|err| err.to_string())?;
            result.operation_token = None;
            println!(
                "{}",
                serde_json::to_string_pretty(&result).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, control_plane_url] if command == "poll-once-container" => {
            let agent = NodeAgent::new(AgentConfig::from_env());
            let client = control_plane_client(control_plane_url, &agent)?;
            let Some(queued) = client.lease_next_command().map_err(|err| err.to_string())? else {
                println!("no command available");
                return Ok(());
            };

            let runner = DockerPostgresRunner::postgres18(agent.config().runtime_root.clone());
            let mut result = match agent.execute_with_runner(&queued.command, &runner) {
                Ok(report) => result_from_execution_report(&agent.config().host_id, &report),
                Err(err) => failed_command_result(
                    &agent.config().host_id,
                    &queued.command.command_id,
                    &err,
                ),
            };
            result.operation_token = queued.operation_token;
            client
                .complete_command(&result)
                .map_err(|err| err.to_string())?;
            result.operation_token = None;
            println!(
                "{}",
                serde_json::to_string_pretty(&result).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, control_plane_url] if command == "poll-once-dry-run" => {
            let agent = NodeAgent::new(AgentConfig::from_env());
            let client = control_plane_client(control_plane_url, &agent)?;
            let Some(queued) = client.lease_next_command().map_err(|err| err.to_string())? else {
                println!("no command available");
                return Ok(());
            };

            let mut result = match agent.plan(&queued.command) {
                Ok(plan) => result_from_command_plan(&agent.config().host_id, &plan),
                Err(err) => failed_command_result(
                    &agent.config().host_id,
                    &queued.command.command_id,
                    &err,
                ),
            };
            result.operation_token = queued.operation_token;
            client
                .complete_command(&result)
                .map_err(|err| err.to_string())?;
            result.operation_token = None;
            println!(
                "{}",
                serde_json::to_string_pretty(&result).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [] => {
            print_help();
            Ok(())
        }
        _ => Err(
            "usage: palimpsest-paas-node-agent plan|apply|apply-container <command.json> | register|heartbeat|hardening-check|poll-once|poll-once-container|poll-once-dry-run <control-plane-url>"
                .to_owned(),
        ),
    }
}

fn control_plane_client(
    control_plane_url: &str,
    agent: &NodeAgent,
) -> Result<HttpControlPlaneClient, String> {
    HttpControlPlaneClient::new(ControlPlaneClientConfig {
        base_url: control_plane_url.to_owned(),
        host_id: agent.config().host_id.clone(),
        bearer_token: std::env::var("PALIMPSEST_PAAS_AGENT_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty()),
        signing_key_id: std::env::var("PALIMPSEST_PAAS_AGENT_SIGNING_KEY_ID")
            .ok()
            .filter(|key_id| !key_id.trim().is_empty()),
        signing_key: agent_signing_key_from_env()?,
    })
    .map_err(|err| err.to_string())
}

fn agent_signing_key_from_env() -> Result<Option<Vec<u8>>, String> {
    let Some(raw) = std::env::var("PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let key = BASE64_STANDARD
        .decode(raw.as_bytes())
        .map_err(|err| format!("decode PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64: {err}"))?;
    if key.len() < 32 {
        return Err(
            "PALIMPSEST_PAAS_AGENT_SIGNING_KEY_BASE64 must decode to at least 32 bytes".to_owned(),
        );
    }
    Ok(Some(key))
}

fn read_command(path: &str) -> Result<NodeAgentCommand, String> {
    let path = PathBuf::from(path);
    let raw = fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {}: {err}", path.display()))
}

fn print_help() {
    println!(
        "palimpsest-paas-node-agent\n\n\
         Usage:\n  palimpsest-paas-node-agent <command> <command.json>\n  palimpsest-paas-node-agent register <control-plane-url>\n  palimpsest-paas-node-agent heartbeat <control-plane-url>\n  palimpsest-paas-node-agent poll-once <control-plane-url>\n  palimpsest-paas-node-agent poll-once-container <control-plane-url>\n  palimpsest-paas-node-agent poll-once-dry-run <control-plane-url>\n\n\
         Commands:\n  plan <command.json>       Render host-local steps for one node-agent command\n  apply <command.json>      Execute one node-agent command on this host\n  apply-container <json>    Execute one command using the PostgreSQL 18 container image\n  register <url>            Register this host with the control plane\n  heartbeat <url>           Record one control-plane heartbeat for this host\n  hardening-check <url>     Scan this host and record a hardening evidence check\n  poll-once <url>           Lease, execute, and complete one queued control-plane command\n  poll-once-container <url> Lease and execute one queued command with the PostgreSQL 18 image\n  poll-once-dry-run <url>   Lease, plan, and complete one queued command without host mutation\n  help                      Show this message"
    );
}
