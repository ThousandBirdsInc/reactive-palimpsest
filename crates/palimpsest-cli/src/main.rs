// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Standalone `palimpsest` CLI (§18.14).
//!
//! Subcommands:
//!
//! - `serve [config]` — boot the embedded server (§18.8). Default if
//!   no subcommand is provided.
//! - `validate-config <config>` — parse the TOML config and compile the
//!   permission rules; exit 0 on success, 1 with a diagnostic on
//!   failure. Suitable for CI.
//! - `dump-catalog [config]` — emit the configured catalog (the demo
//!   catalog for v1) as pretty JSON on stdout. Useful for shipping
//!   schemas to clients during development.
//! - `slot-info <config>` — connect to the configured upstream Postgres
//!   and print one line per replication slot. Requires the optional
//!   `slot-info` Cargo feature.
//! - `dev ...` and `db ...` — additive PaaS helpers for local PostgreSQL 18+
//!   development and managed cluster intent creation.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use palimpsest_paas_core::{ClusterLifecycleState, ManagedPostgresCluster, PostgresVersion};
use palimpsest_permissions::{compile_rules, PermissionRule, UserContextSchema};
use palimpsest_server::{
    AnonymousAuthenticator, EmptyWalRuntime, JwtAuthConfig, JwtAuthenticator, Palimpsest,
};
use palimpsest_sql::{Catalog, ColumnType};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info};

#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    grpc: GrpcConfig,
    #[serde(default)]
    metrics: MetricsConfig,
    #[serde(default)]
    auth: AuthConfig,
    #[serde(default)]
    permissions: PermissionsConfig,
    #[serde(default)]
    upstream: Option<UpstreamConfig>,
}

#[derive(Debug, Deserialize)]
struct GrpcConfig {
    addr: SocketAddr,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:50051".parse().expect("static addr"),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct MetricsConfig {
    addr: Option<SocketAddr>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AuthConfig {
    #[default]
    Anonymous,
    Jwt(JwtAuthConfig),
}

#[derive(Debug, Default, Deserialize)]
struct PermissionsConfig {
    #[serde(default)]
    rules: Vec<PermissionRuleConfig>,
    #[serde(default)]
    user_schema: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct PermissionRuleConfig {
    name: String,
    table: String,
    predicate: String,
}

/// Upstream Postgres connection used by `slot-info` and (in a future
/// patch) by the real WAL runtime once it lands. Optional in v1.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // fields are consumed only behind the `slot-info` feature for now
struct UpstreamConfig {
    /// libpq-style connection URL (e.g.
    /// `postgres://user:pw@host:5432/db`).
    url: String,
    /// Logical replication slot name (e.g. `palimpsest`).
    #[serde(default = "default_slot_name")]
    slot_name: String,
    /// Publication name backing the slot.
    #[serde(default = "default_publication")]
    publication: String,
}

fn default_slot_name() -> String {
    "palimpsest".to_owned()
}

fn default_publication() -> String {
    "palimpsest_pub".to_owned()
}

#[derive(Debug, Error)]
enum CliError {
    #[error("read config '{0}': {1}")]
    ReadConfig(PathBuf, #[source] std::io::Error),
    #[error("parse config: {0}")]
    ParseConfig(#[from] toml::de::Error),
    #[error("compile permissions: {0}")]
    CompilePermissions(String),
    #[error("build server: {0}")]
    BuildServer(String),
    #[error("serve: {0}")]
    Serve(#[from] palimpsest_server::embed::ServeError),
    #[error("dump-catalog: {0}")]
    DumpCatalog(serde_json::Error),
    #[error("dev stack: {0}")]
    DevStack(String),
    #[error("db: {0}")]
    Db(String),
    #[cfg(feature = "slot-info")]
    #[error("slot-info: upstream config is missing — add an [upstream] section to {0}")]
    MissingUpstream(PathBuf),
    #[cfg(not(feature = "slot-info"))]
    #[error("slot-info: feature not enabled (rebuild with `--features slot-info`)")]
    SlotInfoDisabled,
    #[cfg(feature = "slot-info")]
    #[error("slot-info: {0}")]
    SlotInfo(String),
    #[error("usage: {0}")]
    Usage(String),
}

#[tokio::main]
async fn main() -> ExitCode {
    palimpsest_server::tracing_setup::install();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(?err, "palimpsest-cli failed");
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), CliError> {
    // Collect args eagerly so the `std::env::Args` iterator (not Send)
    // is dropped before any `.await`.
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let first = argv.first().map(String::as_str);
    let rest: &[String] = if argv.is_empty() { &[] } else { &argv[1..] };

    match first {
        Some("serve") => cmd_serve(rest.first().map(PathBuf::from)).await,
        Some("validate-config") => cmd_validate_config(&require_one("validate-config", rest)?),
        Some("dump-catalog") => cmd_dump_catalog(rest.first().map(PathBuf::from).as_deref()),
        Some("dev") => cmd_dev(rest),
        Some("db") => cmd_db(rest),
        Some("slot-info") => cmd_slot_info(require_one("slot-info", rest)?).await,
        Some("--help" | "-h" | "help") => {
            print_help();
            Ok(())
        }
        // Backward-compatible: a bare path argument still means "serve
        // with this config".
        Some(path) if !path.starts_with('-') => cmd_serve(Some(PathBuf::from(path))).await,
        Some(other) => Err(CliError::Usage(format!("unknown subcommand '{other}'"))),
        None => cmd_serve(None).await,
    }
}

fn require_one(name: &str, rest: &[String]) -> Result<PathBuf, CliError> {
    rest.first()
        .map(PathBuf::from)
        .ok_or_else(|| CliError::Usage(format!("{name}: expected a config path")))
}

fn print_help() {
    println!(
        "palimpsest — Postgres WAL-backed live query sync engine

Usage: palimpsest <command> [config]

Commands:
  serve [config]              Run the embedded server (default).
  validate-config <config>    Parse the TOML config and exit 0/1.
  dump-catalog [config]       Print the configured catalog as JSON.
  dev up|down|reset|status|env
                              Manage the local PaaS dev stack.
  db create|psql              Manage managed Postgres and local database access.
  slot-info <config>          Print upstream replication slot status
                              (requires --features slot-info).
  help                        Show this message.

If no command is given, behaves as `serve` (with `palimpsest.toml` if no
path is supplied)."
    );
}

fn read_config(path: &Path) -> Result<Config, CliError> {
    let raw =
        fs::read_to_string(path).map_err(|err| CliError::ReadConfig(path.to_path_buf(), err))?;
    Ok(toml::from_str(&raw)?)
}

fn build_user_schema(schema: &BTreeMap<String, String>) -> Result<UserContextSchema, String> {
    let mut fields = Vec::with_capacity(schema.len());
    for (name, ty) in schema {
        fields.push((name.clone(), parse_column_type(ty)?));
    }
    Ok(UserContextSchema::new(fields))
}

fn parse_column_type(value: &str) -> Result<ColumnType, String> {
    match value.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(ColumnType::Bool),
        "int" | "integer" => Ok(ColumnType::Int),
        "float" | "double" => Ok(ColumnType::Float),
        "text" | "string" => Ok(ColumnType::Text),
        "timestamp" => Ok(ColumnType::Timestamp),
        other => Err(format!("unknown column type '{other}'")),
    }
}

const fn column_type_label(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Bool => "bool",
        ColumnType::Int => "int",
        ColumnType::Float => "float",
        ColumnType::Text => "text",
        ColumnType::Timestamp => "timestamp",
        ColumnType::Unknown => "unknown",
    }
}

async fn cmd_serve(path: Option<PathBuf>) -> Result<(), CliError> {
    let path = path.unwrap_or_else(|| PathBuf::from("palimpsest.toml"));
    info!(config = %path.display(), "loading configuration");
    let config = read_config(&path)?;

    let catalog = Catalog::demo();
    let user_schema =
        build_user_schema(&config.permissions.user_schema).map_err(CliError::CompilePermissions)?;
    let rules: Vec<_> = config
        .permissions
        .rules
        .iter()
        .map(|rule| PermissionRule::new(&rule.name, &rule.table, &rule.predicate))
        .collect();
    let compiled = compile_rules(&rules, &catalog, &user_schema)
        .map_err(|err| CliError::CompilePermissions(err.to_string()))?;

    let mut builder = Palimpsest::builder()
        .with_wal(EmptyWalRuntime::default())
        .with_permissions(compiled)
        .with_grpc_addr(config.grpc.addr)
        .with_metrics_addr(config.metrics.addr);

    builder = match config.auth {
        AuthConfig::Anonymous => builder.with_auth(AnonymousAuthenticator),
        AuthConfig::Jwt(jwt) => builder.with_auth(JwtAuthenticator::new(jwt)),
    };

    let server = builder
        .build()
        .map_err(|err| CliError::BuildServer(err.to_string()))?;

    info!("palimpsest-cli ready; press Ctrl-C to shutdown");
    server
        .serve(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

fn cmd_validate_config(path: &Path) -> Result<(), CliError> {
    let config = read_config(path)?;
    let catalog = Catalog::demo();
    let user_schema =
        build_user_schema(&config.permissions.user_schema).map_err(CliError::CompilePermissions)?;
    let rules: Vec<_> = config
        .permissions
        .rules
        .iter()
        .map(|rule| PermissionRule::new(&rule.name, &rule.table, &rule.predicate))
        .collect();
    let _compiled = compile_rules(&rules, &catalog, &user_schema)
        .map_err(|err| CliError::CompilePermissions(err.to_string()))?;

    println!(
        "{}: ok ({} permission rule(s), {} user-context field(s))",
        path.display(),
        rules.len(),
        config.permissions.user_schema.len()
    );
    if config.upstream.is_none() {
        println!("note: no [upstream] section — `slot-info` will be unavailable.");
    }
    Ok(())
}

#[derive(Serialize)]
struct CatalogDump {
    tables: Vec<TableDump>,
}

#[derive(Serialize)]
struct TableDump {
    name: String,
    columns: Vec<ColumnDump>,
}

#[derive(Serialize)]
struct ColumnDump {
    name: String,
    #[serde(rename = "type")]
    ty: &'static str,
}

fn cmd_dump_catalog(path: Option<&Path>) -> Result<(), CliError> {
    if let Some(path) = path {
        let _ = read_config(path)?;
    }
    let catalog = Catalog::demo();
    let dump = CatalogDump {
        tables: catalog
            .tables()
            .map(|table| TableDump {
                name: table.name.clone(),
                columns: table
                    .columns
                    .iter()
                    .map(|col| ColumnDump {
                        name: col.name.clone(),
                        ty: column_type_label(col.ty),
                    })
                    .collect(),
            })
            .collect(),
    };
    let json = serde_json::to_string_pretty(&dump).map_err(CliError::DumpCatalog)?;
    println!("{json}");
    Ok(())
}

fn cmd_dev(rest: &[String]) -> Result<(), CliError> {
    let Some(action) = rest.first().map(String::as_str) else {
        print_dev_help();
        return Ok(());
    };

    match action {
        "up" | "down" | "reset" | "status" => run_dev_stack(action),
        "env" => {
            print_dev_env();
            Ok(())
        }
        "help" | "--help" | "-h" => {
            print_dev_help();
            Ok(())
        }
        other => Err(CliError::Usage(format!(
            "dev: unknown action '{other}' (expected up, down, reset, status, or env)"
        ))),
    }
}

fn print_dev_help() {
    println!(
        "palimpsest dev — local PaaS development stack

Usage: palimpsest dev <action>

Actions:
  up       Start PostgreSQL 18 and Palimpsest with Docker Compose.
  down     Stop the local stack.
  reset    Stop the stack, remove volumes, and start fresh.
  status   Show Docker Compose status.
  env      Print local app environment variables.

Environment:
  PALIMPSEST_PAAS_COMPOSE_FILE can override the compose file path."
    );
}

fn run_dev_stack(action: &str) -> Result<(), CliError> {
    let compose_file = local_dev_compose_file();
    if !compose_file.exists() {
        return Err(CliError::DevStack(format!(
            "compose file not found at {}",
            compose_file.display()
        )));
    }

    for invocation in dev_invocations(action, &compose_file)? {
        eprintln!("$ {} {}", invocation.program, invocation.args.join(" "));
        let status = Command::new(&invocation.program)
            .args(&invocation.args)
            .status()
            .map_err(|err| CliError::DevStack(format!("run {}: {err}", invocation.program)))?;
        if !status.success() {
            return Err(CliError::DevStack(format!(
                "{} {} exited with {status}",
                invocation.program,
                invocation.args.join(" ")
            )));
        }
    }

    if matches!(action, "up" | "reset") {
        print_dev_connection_info();
    }

    Ok(())
}

fn local_dev_compose_file() -> PathBuf {
    if let Some(path) = std::env::var_os("PALIMPSEST_PAAS_COMPOSE_FILE") {
        return PathBuf::from(path);
    }

    let cli_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    cli_dir
        .join("..")
        .join("..")
        .join("paas")
        .join("local")
        .join("docker-compose.yaml")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DevInvocation {
    program: String,
    args: Vec<String>,
}

fn dev_invocations(action: &str, compose_file: &Path) -> Result<Vec<DevInvocation>, CliError> {
    match action {
        "up" => Ok(vec![docker_compose_invocation(compose_file, &["up", "-d"])]),
        "down" => Ok(vec![docker_compose_invocation(compose_file, &["down"])]),
        "reset" => Ok(vec![
            docker_compose_invocation(compose_file, &["down", "-v"]),
            docker_compose_invocation(compose_file, &["up", "-d"]),
        ]),
        "status" => Ok(vec![docker_compose_invocation(compose_file, &["ps"])]),
        other => Err(CliError::Usage(format!("dev: unknown action '{other}'"))),
    }
}

fn docker_compose_invocation(compose_file: &Path, command: &[&str]) -> DevInvocation {
    let mut args = vec![
        "compose".to_owned(),
        "-f".to_owned(),
        compose_file.display().to_string(),
    ];
    args.extend(command.iter().map(|arg| (*arg).to_owned()));
    DevInvocation {
        program: "docker".to_owned(),
        args,
    }
}

fn print_dev_connection_info() {
    println!("local PaaS stack is starting");
    println!(
        "postgres app url: postgres://palimpsest_app:palimpsest_app@localhost:54329/palimpsest_dev"
    );
    println!("postgres admin url: postgres://palimpsest_admin:palimpsest_admin@localhost:54329/palimpsest_dev");
    println!("palimpsest grpc: 127.0.0.1:50051");
    println!("palimpsest metrics: http://127.0.0.1:9090/metrics");
    println!("run `palimpsest dev env` to print .env-compatible settings");
}

fn print_dev_env() {
    print!(
        "\
DATABASE_URL=postgres://palimpsest_app:palimpsest_app@localhost:54329/palimpsest_dev
PALIMPSEST_DATABASE_URL=postgres://palimpsest_app:palimpsest_app@localhost:54329/palimpsest_dev
PALIMPSEST_ADMIN_DATABASE_URL=postgres://palimpsest_admin:palimpsest_admin@localhost:54329/palimpsest_dev
PALIMPSEST_REPLICATION_DATABASE_URL=postgres://palimpsest_repl:palimpsest_repl@localhost:54329/palimpsest_dev
PALIMPSEST_GRPC_ADDR=127.0.0.1:50051
PALIMPSEST_METRICS_URL=http://127.0.0.1:9090/metrics
"
    );
}

fn cmd_db(rest: &[String]) -> Result<(), CliError> {
    let Some(action) = rest.first().map(String::as_str) else {
        print_db_help();
        return Ok(());
    };
    let args = &rest[1..];

    match action {
        "create" => cmd_db_create(args),
        "psql" => cmd_db_psql(args),
        "help" | "--help" | "-h" => {
            print_db_help();
            Ok(())
        }
        other => Err(CliError::Usage(format!(
            "db: unknown action '{other}' (expected create or psql)"
        ))),
    }
}

fn print_db_help() {
    println!(
        "palimpsest db — managed Postgres helper commands

Usage:
  palimpsest db create --cluster-id <id> --organization-id <id> --project-id <id> --environment-id <id> --region <region> [options]
  palimpsest db psql [--local] [--role app|admin|replication] [--url <postgres-url>] [-- <psql args>]

Create options:
  --postgres-version <version>     PostgreSQL version, must be 18 or newer (default: 18).
  --tier <tier>                    Managed Postgres tier label (default: dev).
  --storage-gib <gib>              Requested storage GiB (default: 20).
  --control-plane-url <url>        SQL control-plane base URL (default: PALIMPSEST_PAAS_CONTROL_PLANE_URL or http://127.0.0.1:8088).
  --actor-id <id>                  Audit actor id (default: PALIMPSEST_ACTOR_ID or local-cli).

psql URL resolution:
  explicit --url, then PALIMPSEST_DATABASE_URL, then DATABASE_URL, then the local dev app URL.
  --local ignores environment URLs and uses the local PostgreSQL 18 dev stack URL."
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbCreateOptions {
    cluster_id: String,
    organization_id: String,
    project_id: String,
    environment_id: String,
    region: String,
    postgres_version: PostgresVersion,
    tier: String,
    storage_gib: u32,
    control_plane_url: String,
    actor_id: String,
}

fn cmd_db_create(rest: &[String]) -> Result<(), CliError> {
    if rest
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_db_help();
        return Ok(());
    }
    let options = parse_db_create_options(rest)?;
    let cluster = ManagedPostgresCluster {
        cluster_id: options.cluster_id,
        organization_id: options.organization_id,
        project_id: options.project_id,
        environment_id: options.environment_id,
        region: options.region,
        postgres_version: options.postgres_version,
        tier: options.tier,
        storage_gib: options.storage_gib,
        lifecycle_state: ClusterLifecycleState::Requested,
        host_assignment: None,
    };
    let body = serde_json::to_string(&cluster)
        .map_err(|err| CliError::Db(format!("serialize cluster request: {err}")))?;
    let endpoint = format!(
        "{}/v1/managed-postgres/clusters",
        options.control_plane_url.trim_end_matches('/')
    );
    let response = http_post_json(&endpoint, &body, &options.actor_id)?;
    println!("{response}");
    Ok(())
}

fn parse_db_create_options(rest: &[String]) -> Result<DbCreateOptions, CliError> {
    let mut cluster_id = None;
    let mut organization_id = None;
    let mut project_id = None;
    let mut environment_id = None;
    let mut region = None;
    let mut postgres_version = "18".to_owned();
    let mut tier = "dev".to_owned();
    let mut storage_gib = 20_u32;
    let mut control_plane_url = std::env::var("PALIMPSEST_PAAS_CONTROL_PLANE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8088".to_owned());
    let mut actor_id =
        std::env::var("PALIMPSEST_ACTOR_ID").unwrap_or_else(|_| "local-cli".to_owned());

    let mut iter = rest.iter();
    while let Some(flag) = iter.next() {
        let value = match flag.as_str() {
            "--cluster-id" => &mut cluster_id,
            "--organization-id" => &mut organization_id,
            "--project-id" => &mut project_id,
            "--environment-id" => &mut environment_id,
            "--region" => &mut region,
            "--postgres-version" => {
                postgres_version = next_db_arg(flag, iter.next())?;
                continue;
            }
            "--tier" => {
                tier = next_db_arg(flag, iter.next())?;
                continue;
            }
            "--storage-gib" => {
                let raw = next_db_arg(flag, iter.next())?;
                storage_gib = raw.parse().map_err(|_| {
                    CliError::Db(format!("--storage-gib must be an integer, got '{raw}'"))
                })?;
                if storage_gib == 0 {
                    return Err(CliError::Db(
                        "--storage-gib must be greater than zero".to_owned(),
                    ));
                }
                continue;
            }
            "--control-plane-url" => {
                control_plane_url = next_db_arg(flag, iter.next())?;
                continue;
            }
            "--actor-id" => {
                actor_id = next_db_arg(flag, iter.next())?;
                continue;
            }
            "--help" | "-h" => {
                print_db_help();
                return Err(CliError::Usage("db create help requested".to_owned()));
            }
            other => {
                return Err(CliError::Usage(format!(
                    "db create: unknown option '{other}'"
                )));
            }
        };
        *value = Some(next_db_arg(flag, iter.next())?);
    }

    let postgres_version =
        PostgresVersion::new(postgres_version).map_err(|err| CliError::Db(err.to_string()))?;

    Ok(DbCreateOptions {
        cluster_id: required_db_arg("--cluster-id", cluster_id)?,
        organization_id: required_db_arg("--organization-id", organization_id)?,
        project_id: required_db_arg("--project-id", project_id)?,
        environment_id: required_db_arg("--environment-id", environment_id)?,
        region: required_db_arg("--region", region)?,
        postgres_version,
        tier,
        storage_gib,
        control_plane_url,
        actor_id,
    })
}

fn required_db_arg(name: &str, value: Option<String>) -> Result<String, CliError> {
    value.ok_or_else(|| CliError::Usage(format!("db create: missing {name}")))
}

fn next_db_arg(flag: &str, value: Option<&String>) -> Result<String, CliError> {
    value
        .cloned()
        .ok_or_else(|| CliError::Usage(format!("{flag}: expected a value")))
}

fn cmd_db_psql(rest: &[String]) -> Result<(), CliError> {
    if rest
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_db_help();
        return Ok(());
    }
    let invocation = db_psql_invocation(rest)?;
    let status = Command::new(&invocation.program)
        .args(&invocation.args)
        .status()
        .map_err(|err| CliError::Db(format!("run {}: {err}", invocation.program)))?;
    if !status.success() {
        return Err(CliError::Db(format!(
            "{} exited with {status}",
            invocation.program
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DbPsqlInvocation {
    program: String,
    args: Vec<String>,
}

fn db_psql_invocation(rest: &[String]) -> Result<DbPsqlInvocation, CliError> {
    let mut explicit_url = None;
    let mut force_local = false;
    let mut role = "app".to_owned();
    let mut database_url_env = None;
    let mut psql_args = Vec::new();
    let mut iter = rest.iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--url" => explicit_url = Some(next_db_arg(arg, iter.next())?),
            "--local" => force_local = true,
            "--role" => role = next_db_arg(arg, iter.next())?,
            "--database-url-env" => database_url_env = Some(next_db_arg(arg, iter.next())?),
            "--help" | "-h" => {
                print_db_help();
                return Err(CliError::Usage("db psql help requested".to_owned()));
            }
            "--" => {
                psql_args.extend(iter.cloned());
                break;
            }
            other if other.starts_with("--") => {
                return Err(CliError::Usage(format!(
                    "db psql: unknown option '{other}'"
                )));
            }
            other => psql_args.push(other.to_owned()),
        }
    }

    let url = if let Some(url) = explicit_url {
        url
    } else if force_local {
        local_db_url(&role)?
    } else if let Some(env_name) = database_url_env {
        std::env::var(&env_name)
            .map_err(|_| CliError::Db(format!("environment variable {env_name} is not set")))?
    } else if let Ok(url) = std::env::var("PALIMPSEST_DATABASE_URL") {
        url
    } else if let Ok(url) = std::env::var("DATABASE_URL") {
        url
    } else {
        local_db_url(&role)?
    };

    let mut args = vec![url];
    args.extend(psql_args);
    Ok(DbPsqlInvocation {
        program: "psql".to_owned(),
        args,
    })
}

fn local_db_url(role: &str) -> Result<String, CliError> {
    match role {
        "app" => Ok(
            "postgres://palimpsest_app:palimpsest_app@localhost:54329/palimpsest_dev".to_owned(),
        ),
        "admin" => Ok(
            "postgres://palimpsest_admin:palimpsest_admin@localhost:54329/palimpsest_dev"
                .to_owned(),
        ),
        "replication" | "repl" => Ok(
            "postgres://palimpsest_repl:palimpsest_repl@localhost:54329/palimpsest_dev".to_owned(),
        ),
        other => Err(CliError::Usage(format!(
            "db psql: unknown --role '{other}' (expected app, admin, or replication)"
        ))),
    }
}

fn http_post_json(url: &str, body: &str, actor_id: &str) -> Result<String, CliError> {
    let parsed = ParsedHttpUrl::parse(url)?;
    let mut stream = std::net::TcpStream::connect((parsed.host.as_str(), parsed.port))
        .map_err(|err| CliError::Db(format!("connect to {}: {err}", parsed.authority)))?;
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Actor-Id: {}\r\nConnection: close\r\n\r\n{}",
        parsed.path,
        parsed.authority,
        body.len(),
        actor_id,
        body
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| CliError::Db(format!("write HTTP request: {err}")))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| CliError::Db(format!("read HTTP response: {err}")))?;
    parse_http_response(&response)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedHttpUrl {
    authority: String,
    host: String,
    port: u16,
    path: String,
}

impl ParsedHttpUrl {
    fn parse(url: &str) -> Result<Self, CliError> {
        let without_scheme = url.strip_prefix("http://").ok_or_else(|| {
            CliError::Db(
                "only http:// control-plane URLs are supported by this CLI command".to_owned(),
            )
        })?;
        let (authority, path_suffix) = without_scheme
            .split_once('/')
            .map_or((without_scheme, ""), |(authority, path)| (authority, path));
        if authority.is_empty() || authority.contains('@') {
            return Err(CliError::Db(format!("invalid control-plane URL '{url}'")));
        }
        let (host, port) = parse_http_authority(authority)?;
        Ok(Self {
            authority: authority.to_owned(),
            host,
            port,
            path: format!("/{path_suffix}"),
        })
    }
}

fn parse_http_authority(authority: &str) -> Result<(String, u16), CliError> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| CliError::Db(format!("invalid IPv6 authority '{authority}'")))?;
        let port = if let Some(raw_port) = suffix.strip_prefix(':') {
            parse_port(raw_port)?
        } else {
            80
        };
        return Ok((host.to_owned(), port));
    }

    if let Some((host, raw_port)) = authority.rsplit_once(':') {
        if host.is_empty() {
            return Err(CliError::Db(format!("invalid authority '{authority}'")));
        }
        return Ok((host.to_owned(), parse_port(raw_port)?));
    }
    Ok((authority.to_owned(), 80))
}

fn parse_port(raw_port: &str) -> Result<u16, CliError> {
    raw_port
        .parse()
        .map_err(|_| CliError::Db(format!("invalid control-plane port '{raw_port}'")))
}

fn parse_http_response(response: &str) -> Result<String, CliError> {
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| CliError::Db("malformed HTTP response".to_owned()))?;
    let status_line = headers
        .lines()
        .next()
        .ok_or_else(|| CliError::Db("empty HTTP response".to_owned()))?;
    let mut parts = status_line.split_whitespace();
    let _http_version = parts.next();
    let status = parts
        .next()
        .ok_or_else(|| CliError::Db(format!("malformed HTTP status line '{status_line}'")))?
        .parse::<u16>()
        .map_err(|_| CliError::Db(format!("malformed HTTP status line '{status_line}'")))?;
    if (200..300).contains(&status) {
        return Ok(body.trim().to_owned());
    }
    Err(CliError::Db(format!(
        "control plane returned HTTP {status}: {}",
        body.trim()
    )))
}

#[cfg(feature = "slot-info")]
async fn cmd_slot_info(path: PathBuf) -> Result<(), CliError> {
    use tokio_postgres::{Config, NoTls};

    let config = read_config(&path)?;
    let upstream = config.upstream.ok_or(CliError::MissingUpstream(path))?;
    let pg_config: Config = upstream
        .url
        .parse()
        .map_err(|err: tokio_postgres::Error| CliError::SlotInfo(err.to_string()))?;
    let (client, conn) = pg_config
        .connect(NoTls)
        .await
        .map_err(|err| CliError::SlotInfo(err.to_string()))?;
    let _join = tokio::spawn(async move {
        let _ = conn.await;
    });

    let rows = client
        .query(
            "SELECT slot_name, plugin, slot_type, active, \
                    confirmed_flush_lsn::text, restart_lsn::text \
               FROM pg_replication_slots \
              WHERE slot_name = $1 OR $1 = ''",
            &[&upstream.slot_name],
        )
        .await
        .map_err(|err| CliError::SlotInfo(err.to_string()))?;

    if rows.is_empty() {
        println!("no replication slot named '{}' found", upstream.slot_name);
        println!("  (publication: '{}')", upstream.publication);
        return Ok(());
    }
    println!("slot_name\tplugin\tslot_type\tactive\tconfirmed_flush_lsn\trestart_lsn");
    for row in rows {
        let slot_name: &str = row.get(0);
        let plugin: Option<&str> = row.get(1);
        let slot_type: &str = row.get(2);
        let active: bool = row.get(3);
        let confirmed: Option<String> = row.get(4);
        let restart: Option<String> = row.get(5);
        println!(
            "{slot_name}\t{}\t{slot_type}\t{active}\t{}\t{}",
            plugin.unwrap_or("-"),
            confirmed.as_deref().unwrap_or("-"),
            restart.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

#[cfg(not(feature = "slot-info"))]
#[allow(clippy::unused_async)]
async fn cmd_slot_info(_path: PathBuf) -> Result<(), CliError> {
    Err(CliError::SlotInfoDisabled)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn dev_up_invokes_docker_compose_with_local_file() {
        let invocations = dev_invocations("up", Path::new("paas/local/docker-compose.yaml"))
            .expect("dev up should build invocation");

        assert_eq!(
            invocations,
            vec![DevInvocation {
                program: "docker".to_owned(),
                args: vec![
                    "compose".to_owned(),
                    "-f".to_owned(),
                    "paas/local/docker-compose.yaml".to_owned(),
                    "up".to_owned(),
                    "-d".to_owned(),
                ],
            }]
        );
    }

    #[test]
    fn dev_reset_removes_volumes_before_starting() {
        let invocations = dev_invocations("reset", Path::new("paas/local/docker-compose.yaml"))
            .expect("dev reset should build invocations");

        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[0].args[3..], ["down", "-v"]);
        assert_eq!(invocations[1].args[3..], ["up", "-d"]);
    }

    #[test]
    fn dev_env_is_supported_without_compose() {
        let result = cmd_dev(&["env".to_owned()]);

        assert!(result.is_ok());
    }

    #[test]
    fn db_create_builds_postgres_eighteen_request_defaults() {
        let options = parse_db_create_options(&[
            "--cluster-id".to_owned(),
            "cluster_123".to_owned(),
            "--organization-id".to_owned(),
            "org_123".to_owned(),
            "--project-id".to_owned(),
            "project_123".to_owned(),
            "--environment-id".to_owned(),
            "env_123".to_owned(),
            "--region".to_owned(),
            "us-east-1".to_owned(),
        ])
        .expect("db create options should parse");

        assert_eq!(options.cluster_id, "cluster_123");
        assert_eq!(options.postgres_version.as_str(), "18");
        assert_eq!(options.tier, "dev");
        assert_eq!(options.storage_gib, 20);
        assert_eq!(options.control_plane_url, "http://127.0.0.1:8088");
    }

    #[test]
    fn db_create_rejects_postgres_below_eighteen() {
        let result = parse_db_create_options(&[
            "--cluster-id".to_owned(),
            "cluster_123".to_owned(),
            "--organization-id".to_owned(),
            "org_123".to_owned(),
            "--project-id".to_owned(),
            "project_123".to_owned(),
            "--environment-id".to_owned(),
            "env_123".to_owned(),
            "--region".to_owned(),
            "us-east-1".to_owned(),
            "--postgres-version".to_owned(),
            "17".to_owned(),
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn db_psql_defaults_to_local_app_url() {
        let invocation =
            db_psql_invocation(&["--local".to_owned()]).expect("db psql should build command");

        assert_eq!(invocation.program, "psql");
        assert_eq!(
            invocation.args,
            vec!["postgres://palimpsest_app:palimpsest_app@localhost:54329/palimpsest_dev"]
        );
    }

    #[test]
    fn db_psql_supports_local_admin_role_and_passthrough_args() {
        let invocation = db_psql_invocation(&[
            "--local".to_owned(),
            "--role".to_owned(),
            "admin".to_owned(),
            "--".to_owned(),
            "-c".to_owned(),
            "SELECT 1".to_owned(),
        ])
        .expect("db psql should build command");

        assert_eq!(
            invocation.args,
            vec![
                "postgres://palimpsest_admin:palimpsest_admin@localhost:54329/palimpsest_dev",
                "-c",
                "SELECT 1",
            ]
        );
    }

    #[test]
    fn http_response_parser_rejects_control_plane_errors() {
        let result = parse_http_response(
            "HTTP/1.1 409 Conflict\r\ncontent-length: 27\r\n\r\n{\"error\":\"already exists\"}",
        );

        assert!(result.is_err());
    }
}
