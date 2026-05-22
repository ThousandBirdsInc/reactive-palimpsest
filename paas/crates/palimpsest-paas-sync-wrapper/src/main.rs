// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{fs, path::PathBuf, process::ExitCode};

use palimpsest_paas_sync_wrapper::{
    classify_reload, plan_initial_start, plan_supervisor_transition, render_palimpsest_config,
    verify_ed25519_signature, DeploymentSpec, DrainPolicy, SignedDeploymentSpec,
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
        [command, spec_path] if command == "render-config" => {
            let signed = read_signed_spec(spec_path)?;
            let rendered = render_palimpsest_config(&signed).map_err(|err| err.to_string())?;
            print!("{}", rendered.toml);
            Ok(())
        }
        [command, spec_path] if command == "verify-signature" => {
            let signed = read_signed_spec(spec_path)?;
            verify_ed25519_signature(&signed).map_err(|err| err.to_string())?;
            println!("signature ok");
            Ok(())
        }
        [command, old_path, new_path] if command == "classify-reload" => {
            let old = read_deployment_spec(old_path)?;
            let new = read_deployment_spec(new_path)?;
            println!("{:?}", classify_reload(&old, &new));
            Ok(())
        }
        [command, spec_path] if command == "plan-start" => {
            let signed = read_signed_spec(spec_path)?;
            let plan = plan_initial_start(&signed.spec, "/etc/palimpsest/palimpsest.toml");
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [command, old_path, new_path] if command == "plan-transition" => {
            let old = read_deployment_spec(old_path)?;
            let new = read_deployment_spec(new_path)?;
            let plan = plan_supervisor_transition(
                &old,
                &new,
                "/etc/palimpsest/palimpsest.toml",
                DrainPolicy::default(),
            );
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [] => {
            print_help();
            Ok(())
        }
        _ => Err(
            "usage: palimpsest-paas-sync-wrapper render-config|verify-signature|plan-start <signed-spec.json>"
                .to_owned(),
        ),
    }
}

fn read_signed_spec(path: &str) -> Result<SignedDeploymentSpec, String> {
    let path = PathBuf::from(path);
    let raw = fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {}: {err}", path.display()))
}

fn read_deployment_spec(path: &str) -> Result<DeploymentSpec, String> {
    let path = PathBuf::from(path);
    let raw = fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {}: {err}", path.display()))
}

fn print_help() {
    println!(
        "palimpsest-paas-sync-wrapper\n\n\
         Usage:\n  palimpsest-paas-sync-wrapper render-config <signed-spec.json>\n  palimpsest-paas-sync-wrapper verify-signature <signed-spec.json>\n  palimpsest-paas-sync-wrapper classify-reload <old-spec.json> <new-spec.json>\n  palimpsest-paas-sync-wrapper plan-start <signed-spec.json>\n  palimpsest-paas-sync-wrapper plan-transition <old-spec.json> <new-spec.json>\n\n\
         Commands:\n  render-config      Render palimpsest serve config from a signed deployment spec\n  verify-signature   Verify a signed deployment spec using Ed25519\n  classify-reload    Classify config change as SafeReload, DrainRestart, or Blocked\n  plan-start         Plan initial SyncDeployment process start\n  plan-transition    Plan safe reload, drain/restart, or blocked transition\n  help               Show this message"
    );
}
