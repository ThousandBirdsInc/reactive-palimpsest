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

use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

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
