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
//! - `permissions eval <config> --query <sql>` — compile permission
//!   rules, rewrite one or more queries for a supplied user context,
//!   and print the before/after canonical MIR.
//! - `skills install` — install Codex and Claude skills that teach
//!   agents how to operate the Palimpsest CLI.
//! - `dump-catalog [config]` — emit the configured catalog (the demo
//!   catalog for v1) as pretty JSON on stdout. Useful for shipping
//!   schemas to clients during development.
//! - `slot-info <config>` — connect to the configured upstream Postgres
//!   and print one line per replication slot. Requires the optional
//!   `slot-info` Cargo feature.
//! - `dev ...` and `db ...` — additive `PaaS` helpers for local `PostgreSQL` 18+
//!   development, managed cluster intent creation, and database branch
//!   operations (`db branch create|list|get|delete`) against the control plane.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use palimpsest_paas_core::{ClusterLifecycleState, ManagedPostgresCluster, PostgresVersion};
use palimpsest_permissions::{
    compile_rules, rewrite, Mode, PermissionRule, UserContext, UserContextSchema, UserValue,
};
use palimpsest_server::{
    AnonymousAuthenticator, EmptyWalRuntime, JwtAuthConfig, JwtAuthenticator, Palimpsest,
};
use palimpsest_sql::{canonical::canonical_form, parse_and_lower, Catalog, ColumnType};
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
    #[serde(default)]
    mode: Mode,
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
    #[error("permissions eval: {0}")]
    EvalPermissions(String),
    #[error("skills: {0}")]
    Skills(String),
    #[error("dev stack: {0}")]
    DevStack(String),
    #[error("db: {0}")]
    Db(String),
    #[error("control plane returned HTTP {status}: {body}")]
    ControlPlane { status: u16, body: String },
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
            exit_code_for(&err)
        }
    }
}

/// Maps an error to a stable process exit code so agents can branch on the
/// failure class without parsing stderr. `0` success, `2` usage, `3` generic
/// control-plane/client error, `4` not found, `5` conflict, `6` unauthorized,
/// `7` control-plane server error, `1` everything else.
fn exit_code_for(err: &CliError) -> ExitCode {
    let code: u8 = match err {
        CliError::Usage(_) => 2,
        CliError::ControlPlane { status, .. } => match *status {
            401 | 403 => 6,
            404 => 4,
            409 => 5,
            s if (500..600).contains(&s) => 7,
            _ => 3,
        },
        _ => 1,
    };
    ExitCode::from(code)
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
        Some("permissions") => cmd_permissions(rest),
        Some("skills") => cmd_skills(rest),
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
  permissions eval <config> --query <sql> [--user field=value]
                              Rewrite query MIR with configured permissions.
  skills install [options]    Install Codex and Claude skills for this CLI.
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
        .map(|rule| {
            PermissionRule::new(&rule.name, &rule.table, &rule.predicate).with_mode(rule.mode)
        })
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
        .map(|rule| {
            PermissionRule::new(&rule.name, &rule.table, &rule.predicate).with_mode(rule.mode)
        })
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

fn cmd_permissions(rest: &[String]) -> Result<(), CliError> {
    let Some(action) = rest.first().map(String::as_str) else {
        print_permissions_help();
        return Ok(());
    };

    match action {
        "eval"
            if rest
                .get(1)
                .is_some_and(|arg| arg == "--help" || arg == "-h") =>
        {
            print_permissions_help();
            Ok(())
        }
        "eval" => cmd_permissions_eval(&rest[1..]),
        "help" | "--help" | "-h" => {
            print_permissions_help();
            Ok(())
        }
        other => Err(CliError::Usage(format!(
            "permissions: unknown action '{other}' (expected eval)"
        ))),
    }
}

fn print_permissions_help() {
    println!(
        "palimpsest permissions — inspect permission-rule behavior

Usage:
  palimpsest permissions eval <config> --query <sql> [options]

Options:
  --query <sql>               Query to evaluate; may be repeated.
  --query-file <path>         Read a query from a file; may be repeated.
  --user <field=value>        Bind a user-context value; may be repeated.
  --user-json <json>          Bind user-context values from a JSON object.
  --format text|json          Output format (default: text).
  --json                      Alias for --format json.

Values are parsed using [permissions.user_schema] from the config."
    );
}

#[derive(Debug, Clone, PartialEq)]
struct PermissionEvalOptions {
    config_path: PathBuf,
    queries: Vec<String>,
    user_values: BTreeMap<String, UserValue>,
    output: PermissionEvalOutput,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PermissionEvalOutput {
    #[default]
    Text,
    Json,
}

fn cmd_permissions_eval(rest: &[String]) -> Result<(), CliError> {
    let options = parse_permission_eval_options(rest)?;
    let config = read_config(&options.config_path)?;
    let catalog = Catalog::demo();
    let user_schema =
        build_user_schema(&config.permissions.user_schema).map_err(CliError::EvalPermissions)?;
    let rules: Vec<_> = config
        .permissions
        .rules
        .iter()
        .map(|rule| {
            PermissionRule::new(&rule.name, &rule.table, &rule.predicate).with_mode(rule.mode)
        })
        .collect();
    let compiled = compile_rules(&rules, &catalog, &user_schema)
        .map_err(|err| CliError::EvalPermissions(format!("compile permissions: {err}")))?;
    let user_context = UserContext::new(options.user_values.clone());
    user_context
        .validate(&user_schema)
        .map_err(|err| CliError::EvalPermissions(format!("validate user context: {err}")))?;

    let mut query_reports = Vec::with_capacity(options.queries.len());
    for query in &options.queries {
        let before = parse_and_lower(query)
            .map_err(|err| CliError::EvalPermissions(format!("parse query: {err}")))?;
        let outcome = rewrite(&before, &compiled, &user_context)
            .map_err(|err| CliError::EvalPermissions(format!("rewrite query: {err}")))?;
        query_reports.push(PermissionQueryEvalReport {
            query: query.clone(),
            canonical_before: canonical_form(&before),
            canonical_after: canonical_form(&outcome.graph),
            stats: PermissionEvalStats {
                base_tables_visited: outcome.stats.base_tables_visited,
                filters_inserted: outcome.stats.filters_inserted,
                rules_elided: outcome.stats.rules_elided,
            },
        });
    }

    let report = PermissionEvalReport {
        config: options.config_path.display().to_string(),
        rules_compiled: compiled.len(),
        user_context: options.user_values,
        queries: query_reports,
    };

    match options.output {
        PermissionEvalOutput::Text => print_permission_eval_text(&report),
        PermissionEvalOutput::Json => {
            let json = serde_json::to_string_pretty(&report)
                .map_err(|err| CliError::EvalPermissions(format!("serialize report: {err}")))?;
            println!("{json}");
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct PermissionEvalReport {
    config: String,
    rules_compiled: usize,
    user_context: BTreeMap<String, UserValue>,
    queries: Vec<PermissionQueryEvalReport>,
}

#[derive(Debug, Serialize)]
struct PermissionQueryEvalReport {
    query: String,
    canonical_before: String,
    canonical_after: String,
    stats: PermissionEvalStats,
}

#[derive(Debug, Serialize)]
struct PermissionEvalStats {
    base_tables_visited: usize,
    filters_inserted: usize,
    rules_elided: usize,
}

fn print_permission_eval_text(report: &PermissionEvalReport) {
    println!("permission evaluation: ok");
    println!("config: {}", report.config);
    println!("rules_compiled: {}", report.rules_compiled);
    println!("user_context:");
    if report.user_context.is_empty() {
        println!("  <empty>");
    } else {
        for (field, value) in &report.user_context {
            println!("  {field} = {}", user_value_label(value));
        }
    }

    for (index, query) in report.queries.iter().enumerate() {
        if report.queries.len() > 1 {
            println!("\nquery {}:", index + 1);
        } else {
            println!("\nquery:");
        }
        println!("  {}", query.query);
        println!("canonical_before:");
        println!("  {}", query.canonical_before);
        println!("canonical_after:");
        println!("  {}", query.canonical_after);
        println!(
            "stats: base_tables_visited={} filters_inserted={} rules_elided={}",
            query.stats.base_tables_visited, query.stats.filters_inserted, query.stats.rules_elided
        );
    }
}

fn user_value_label(value: &UserValue) -> String {
    match value {
        UserValue::Bool(value) => format!("bool:{value}"),
        UserValue::Int(value) => format!("int:{value}"),
        UserValue::Float(value) => format!("float:{value}"),
        UserValue::Text(value) => format!("text:{value:?}"),
        UserValue::Timestamp(value) => format!("timestamp:{value:?}"),
        UserValue::Null => "null".to_owned(),
    }
}

fn parse_permission_eval_options(rest: &[String]) -> Result<PermissionEvalOptions, CliError> {
    if rest
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_permissions_help();
        return Err(CliError::Usage(
            "permissions eval help requested".to_owned(),
        ));
    }
    let config_path = rest
        .first()
        .map(PathBuf::from)
        .ok_or_else(|| CliError::Usage("permissions eval: expected a config path".to_owned()))?;
    let config = read_config(&config_path)?;
    let mut user_values = BTreeMap::new();
    let mut queries = Vec::new();
    let mut output = PermissionEvalOutput::Text;
    let mut iter = rest[1..].iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--query" => queries.push(next_arg(arg, iter.next())?),
            "--query-file" => {
                let path = PathBuf::from(next_arg(arg, iter.next())?);
                let query = fs::read_to_string(&path).map_err(|err| {
                    CliError::EvalPermissions(format!(
                        "read query file '{}': {err}",
                        path.display()
                    ))
                })?;
                queries.push(query);
            }
            "--user" => {
                let assignment = next_arg(arg, iter.next())?;
                parse_user_assignment(
                    &assignment,
                    &config.permissions.user_schema,
                    &mut user_values,
                )?;
            }
            "--user-json" => {
                let raw = next_arg(arg, iter.next())?;
                parse_user_json(&raw, &config.permissions.user_schema, &mut user_values)?;
            }
            "--format" => {
                output = parse_permission_eval_output(&next_arg(arg, iter.next())?)?;
            }
            "--json" => output = PermissionEvalOutput::Json,
            "--help" | "-h" => {
                print_permissions_help();
                return Err(CliError::Usage(
                    "permissions eval help requested".to_owned(),
                ));
            }
            other => {
                return Err(CliError::Usage(format!(
                    "permissions eval: unknown option '{other}'"
                )));
            }
        }
    }

    if queries.is_empty() {
        return Err(CliError::Usage(
            "permissions eval: expected at least one --query or --query-file".to_owned(),
        ));
    }

    Ok(PermissionEvalOptions {
        config_path,
        queries,
        user_values,
        output,
    })
}

fn next_arg(flag: &str, value: Option<&String>) -> Result<String, CliError> {
    value
        .cloned()
        .ok_or_else(|| CliError::Usage(format!("{flag}: expected a value")))
}

fn parse_permission_eval_output(value: &str) -> Result<PermissionEvalOutput, CliError> {
    match value {
        "text" => Ok(PermissionEvalOutput::Text),
        "json" => Ok(PermissionEvalOutput::Json),
        other => Err(CliError::Usage(format!(
            "permissions eval: unknown --format '{other}' (expected text or json)"
        ))),
    }
}

fn parse_user_assignment(
    assignment: &str,
    schema: &BTreeMap<String, String>,
    output: &mut BTreeMap<String, UserValue>,
) -> Result<(), CliError> {
    let (field, raw_value) = assignment.split_once('=').ok_or_else(|| {
        CliError::Usage(format!(
            "permissions eval: --user value must be field=value, got '{assignment}'"
        ))
    })?;
    if field.is_empty() {
        return Err(CliError::Usage(
            "permissions eval: --user field name cannot be empty".to_owned(),
        ));
    }
    let value = parse_user_value(field, raw_value, schema)?;
    output.insert(field.to_owned(), value);
    Ok(())
}

fn parse_user_json(
    raw: &str,
    schema: &BTreeMap<String, String>,
    output: &mut BTreeMap<String, UserValue>,
) -> Result<(), CliError> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|err| CliError::EvalPermissions(format!("parse --user-json: {err}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| CliError::EvalPermissions("--user-json must be a JSON object".to_owned()))?;

    for (field, value) in object {
        let user_value = parse_json_user_value(field, value, schema)?;
        output.insert(field.clone(), user_value);
    }
    Ok(())
}

fn parse_json_user_value(
    field: &str,
    value: &serde_json::Value,
    schema: &BTreeMap<String, String>,
) -> Result<UserValue, CliError> {
    if value.is_null() {
        return Ok(UserValue::Null);
    }
    match schema_type(field, schema)? {
        ColumnType::Bool => value
            .as_bool()
            .map(UserValue::Bool)
            .ok_or_else(|| user_value_type_error(field, "boolean", value)),
        ColumnType::Int => value
            .as_i64()
            .map(UserValue::Int)
            .ok_or_else(|| user_value_type_error(field, "integer", value)),
        ColumnType::Float => value
            .as_f64()
            .map(UserValue::Float)
            .ok_or_else(|| user_value_type_error(field, "number", value)),
        ColumnType::Text => value
            .as_str()
            .map(|value| UserValue::Text(value.to_owned()))
            .ok_or_else(|| user_value_type_error(field, "string", value)),
        ColumnType::Timestamp => value
            .as_str()
            .map(|value| UserValue::Timestamp(value.to_owned()))
            .ok_or_else(|| user_value_type_error(field, "string timestamp", value)),
        ColumnType::Unknown => Err(CliError::EvalPermissions(format!(
            "unknown user-context type for field '{field}'"
        ))),
    }
}

fn user_value_type_error(field: &str, expected: &str, value: &serde_json::Value) -> CliError {
    CliError::EvalPermissions(format!(
        "user field '{field}' expects {expected}, got {value}"
    ))
}

fn parse_user_value(
    field: &str,
    raw_value: &str,
    schema: &BTreeMap<String, String>,
) -> Result<UserValue, CliError> {
    if raw_value.eq_ignore_ascii_case("null") {
        return Ok(UserValue::Null);
    }

    match schema_type(field, schema)? {
        ColumnType::Bool => match raw_value {
            "true" => Ok(UserValue::Bool(true)),
            "false" => Ok(UserValue::Bool(false)),
            other => Err(CliError::EvalPermissions(format!(
                "user field '{field}' expects bool, got '{other}'"
            ))),
        },
        ColumnType::Int => raw_value.parse::<i64>().map(UserValue::Int).map_err(|_| {
            CliError::EvalPermissions(format!(
                "user field '{field}' expects int, got '{raw_value}'"
            ))
        }),
        ColumnType::Float => raw_value.parse::<f64>().map(UserValue::Float).map_err(|_| {
            CliError::EvalPermissions(format!(
                "user field '{field}' expects float, got '{raw_value}'"
            ))
        }),
        ColumnType::Text => Ok(UserValue::Text(raw_value.to_owned())),
        ColumnType::Timestamp => Ok(UserValue::Timestamp(raw_value.to_owned())),
        ColumnType::Unknown => Err(CliError::EvalPermissions(format!(
            "unknown user-context type for field '{field}'"
        ))),
    }
}

fn schema_type(field: &str, schema: &BTreeMap<String, String>) -> Result<ColumnType, CliError> {
    let raw_type = schema.get(field).ok_or_else(|| {
        CliError::EvalPermissions(format!(
            "user field '{field}' is not declared in [permissions.user_schema]"
        ))
    })?;
    parse_column_type(raw_type).map_err(CliError::EvalPermissions)
}

fn cmd_skills(rest: &[String]) -> Result<(), CliError> {
    let Some(action) = rest.first().map(String::as_str) else {
        print_skills_help();
        return Ok(());
    };

    match action {
        "install"
            if rest
                .get(1)
                .is_some_and(|arg| arg == "--help" || arg == "-h") =>
        {
            print_skills_help();
            Ok(())
        }
        "install" => cmd_skills_install(&rest[1..]),
        "help" | "--help" | "-h" => {
            print_skills_help();
            Ok(())
        }
        other => Err(CliError::Usage(format!(
            "skills: unknown action '{other}' (expected install)"
        ))),
    }
}

fn print_skills_help() {
    println!(
        "palimpsest skills - install agent skills for operating the CLI

Usage:
  palimpsest skills install [options]

Options:
  --all                       Install both Codex and Claude skills (default).
  --codex                     Install only/also the Codex skill.
  --claude                    Install only/also the Claude skill.
  --codex-dir <path>          Codex skills root (default: CODEX_HOME/skills or ~/.codex/skills).
  --claude-dir <path>         Claude skills root (default: CLAUDE_HOME/skills or ~/.claude/skills).
  --force                     Replace an existing modified skill.
  --dry-run                   Print intended writes without changing files.

The installed skill is named palimpsest-cli."
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillsInstallOptions {
    targets: Vec<SkillTarget>,
    codex_dir: PathBuf,
    claude_dir: PathBuf,
    force: bool,
    dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkillTarget {
    Codex,
    Claude,
}

impl SkillTarget {
    const fn label(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SkillInstallStatus {
    Created,
    Updated,
    Unchanged,
    DryRunCreate,
    DryRunUpdate,
    DryRunUnchanged,
}

impl SkillInstallStatus {
    const fn label(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Unchanged => "unchanged",
            Self::DryRunCreate => "would create",
            Self::DryRunUpdate => "would update",
            Self::DryRunUnchanged => "would leave unchanged",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillInstallReport {
    target: SkillTarget,
    path: PathBuf,
    status: SkillInstallStatus,
}

const PALIMPSEST_CLI_SKILL_NAME: &str = "palimpsest-cli";

const PALIMPSEST_CLI_SKILL: &str = r"---
name: palimpsest-cli
description: Use when operating the Palimpsest CLI, including config validation, permission query evaluation, local PaaS stack management, managed Postgres helper commands, and CLI installation checks.
---

# Palimpsest CLI

Use the `palimpsest` binary for local operation and diagnostics. Start with `palimpsest help` when the current command surface matters.

## Core Commands

- `palimpsest validate-config <config>`: parse TOML and compile permission rules.
- `palimpsest permissions eval <config> --query <sql> --user field=value`: compile configured permissions and show canonical query MIR before and after rewriting.
- `palimpsest dump-catalog [config]`: print the configured demo catalog as JSON.
- `palimpsest dev status`: inspect the local PaaS development stack.
- `palimpsest dev up`: start the local PostgreSQL 18 and Palimpsest stack.
- `palimpsest dev env`: print `.env`-compatible local connection settings.
- `palimpsest dev down`: stop the local stack when the user asks to stop services.
- `palimpsest db psql --local [--role app|admin|replication]`: open `psql` against the local dev stack.
- `palimpsest db create ...`: create a managed PostgreSQL 18+ cluster intent through the control plane.
- `palimpsest db branch create --cluster-id <id> --name <name> [--parent-branch-id <id>] [--source-database <db>] [--terminate-source-connections]`: create a HEAD copy-on-write database branch.
- `palimpsest db branch create --cluster-id <id> --name <name> --mode point-in-time --recovery-target-lsn <lsn> [--provision-sync-deployment]`: create a point-in-time branch (restores into a dedicated cluster; optionally provisions a SyncDeployment).
- `palimpsest db branch list --cluster-id <id>`: list a cluster's branches (JSON, includes parent lineage).
- `palimpsest db branch get --cluster-id <id> --branch-id <id>`: fetch one branch.
- `palimpsest db branch delete --cluster-id <id> --branch-id <id>`: delete a branch (rejected if it has child branches).
- `palimpsest slot-info <config>`: inspect upstream replication slot status when the binary was built with `--features slot-info`.

## Output And Exit Codes (for agents)

`db` commands print the control-plane JSON response to stdout; parse stdout for results. Exit codes encode the failure class: `0` success, `2` usage error, `3` client error, `4` not found, `5` conflict, `6` unauthorized, `7` control-plane server error. Set `PALIMPSEST_PAAS_CONTROL_PLANE_URL` and `PALIMPSEST_ACTOR_ID` to avoid repeating `--control-plane-url`/`--actor-id`.

## Preferred Workflow

1. Run `palimpsest validate-config <config>` before serving or debugging permission behavior.
2. Use `palimpsest permissions eval` for permission model questions instead of inferring rewrites by inspection.
3. Check `palimpsest dev status` before starting or stopping the local stack.
4. Prefer `palimpsest dev env` and `palimpsest db psql --local` over manually reconstructing local database URLs.
5. Use `cargo install palimpsest-cli` for the published CLI, or `cargo install --path crates/palimpsest-cli` from a checkout.
";

fn cmd_skills_install(rest: &[String]) -> Result<(), CliError> {
    let options = parse_skills_install_options(rest)?;
    let mut reports = Vec::with_capacity(options.targets.len());

    for target in &options.targets {
        let base_dir = match target {
            SkillTarget::Codex => &options.codex_dir,
            SkillTarget::Claude => &options.claude_dir,
        };
        reports.push(install_skill_target(
            *target,
            base_dir,
            options.force,
            options.dry_run,
        )?);
    }

    for report in reports {
        println!(
            "{}: {} {}",
            report.target.label(),
            report.status.label(),
            report.path.display()
        );
    }
    Ok(())
}

fn parse_skills_install_options(rest: &[String]) -> Result<SkillsInstallOptions, CliError> {
    let mut codex = false;
    let mut claude = false;
    let mut codex_dir = None;
    let mut claude_dir = None;
    let mut force = false;
    let mut dry_run = false;
    let mut iter = rest.iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--all" => {
                codex = true;
                claude = true;
            }
            "--codex" => codex = true,
            "--claude" => claude = true,
            "--codex-dir" => codex_dir = Some(PathBuf::from(next_arg(arg, iter.next())?)),
            "--claude-dir" => claude_dir = Some(PathBuf::from(next_arg(arg, iter.next())?)),
            "--force" => force = true,
            "--dry-run" => dry_run = true,
            "--help" | "-h" => {
                print_skills_help();
                return Err(CliError::Usage("skills install help requested".to_owned()));
            }
            other => {
                return Err(CliError::Usage(format!(
                    "skills install: unknown option '{other}'"
                )));
            }
        }
    }

    if !codex && !claude {
        codex = true;
        claude = true;
    }

    let mut targets = Vec::new();
    if codex {
        targets.push(SkillTarget::Codex);
    }
    if claude {
        targets.push(SkillTarget::Claude);
    }

    Ok(SkillsInstallOptions {
        targets,
        codex_dir: codex_dir.unwrap_or_else(default_codex_skills_dir),
        claude_dir: claude_dir.unwrap_or_else(default_claude_skills_dir),
        force,
        dry_run,
    })
}

fn default_codex_skills_dir() -> PathBuf {
    std::env::var_os("CODEX_HOME")
        .map_or_else(|| home_relative(".codex"), PathBuf::from)
        .join("skills")
}

fn default_claude_skills_dir() -> PathBuf {
    std::env::var_os("CLAUDE_HOME")
        .map_or_else(|| home_relative(".claude"), PathBuf::from)
        .join("skills")
}

fn home_relative(path: &str) -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
        .join(path)
}

fn install_skill_target(
    target: SkillTarget,
    base_dir: &Path,
    force: bool,
    dry_run: bool,
) -> Result<SkillInstallReport, CliError> {
    let skill_dir = base_dir.join(PALIMPSEST_CLI_SKILL_NAME);
    let skill_path = skill_dir.join("SKILL.md");
    let existing = match fs::read_to_string(&skill_path) {
        Ok(content) => Some(content),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            return Err(CliError::Skills(format!(
                "read existing skill '{}': {err}",
                skill_path.display()
            )));
        }
    };

    let status = match existing {
        Some(content) if content == PALIMPSEST_CLI_SKILL => {
            if dry_run {
                SkillInstallStatus::DryRunUnchanged
            } else {
                SkillInstallStatus::Unchanged
            }
        }
        Some(_) if !force => {
            return Err(CliError::Skills(format!(
                "{} skill already exists at {}; pass --force to replace it",
                target.label(),
                skill_path.display()
            )));
        }
        Some(_) if dry_run => SkillInstallStatus::DryRunUpdate,
        Some(_) => {
            fs::write(&skill_path, PALIMPSEST_CLI_SKILL).map_err(|err| {
                CliError::Skills(format!("write skill '{}': {err}", skill_path.display()))
            })?;
            SkillInstallStatus::Updated
        }
        None if dry_run => SkillInstallStatus::DryRunCreate,
        None => {
            fs::create_dir_all(&skill_dir).map_err(|err| {
                CliError::Skills(format!(
                    "create skill directory '{}': {err}",
                    skill_dir.display()
                ))
            })?;
            fs::write(&skill_path, PALIMPSEST_CLI_SKILL).map_err(|err| {
                CliError::Skills(format!("write skill '{}': {err}", skill_path.display()))
            })?;
            SkillInstallStatus::Created
        }
    };

    Ok(SkillInstallReport {
        target,
        path: skill_path,
        status,
    })
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
        "branch" => cmd_db_branch(args),
        "help" | "--help" | "-h" => {
            print_db_help();
            Ok(())
        }
        other => Err(CliError::Usage(format!(
            "db: unknown action '{other}' (expected create, psql, or branch)"
        ))),
    }
}

fn print_db_help() {
    println!(
        "palimpsest db — managed Postgres helper commands

Usage:
  palimpsest db create --cluster-id <id> --organization-id <id> --project-id <id> --environment-id <id> --region <region> [options]
  palimpsest db psql [--local] [--role app|admin|replication] [--url <postgres-url>] [-- <psql args>]
  palimpsest db branch <create|list|get|delete> ...   (see `palimpsest db branch help`)

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

fn default_control_plane_url() -> String {
    std::env::var("PALIMPSEST_PAAS_CONTROL_PLANE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8088".to_owned())
}

fn default_actor_id() -> String {
    std::env::var("PALIMPSEST_ACTOR_ID").unwrap_or_else(|_| "local-cli".to_owned())
}

fn wants_help(rest: &[String]) -> bool {
    rest.first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
}

/// Request body for `POST .../branches`, mirroring the control plane's
/// `CreateBranchRequest`. Optional fields are omitted so the server defaults
/// apply.
#[derive(Debug, Serialize)]
struct BranchCreateBody {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_branch_id: Option<String>,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery_target_lsn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redaction_policy_id: Option<String>,
    terminate_source_connections: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    provision_sync_deployment: bool,
}

fn parse_branch_mode_flag(value: &str) -> Result<String, CliError> {
    match value {
        "head-cow" | "head_cow" => Ok("head_cow".to_owned()),
        "point-in-time" | "point_in_time" => Ok("point_in_time".to_owned()),
        other => Err(CliError::Usage(format!(
            "db branch: invalid --mode '{other}' (expected head-cow or point-in-time)"
        ))),
    }
}

#[derive(Debug, Clone)]
struct BranchTarget {
    cluster_id: String,
    branch_id: Option<String>,
    control_plane_url: String,
    actor_id: String,
}

/// Parses the flags shared by the read/delete branch commands
/// (`--cluster-id`, `--branch-id`, `--control-plane-url`, `--actor-id`).
fn parse_branch_target(rest: &[String], context: &str) -> Result<BranchTarget, CliError> {
    let mut cluster_id = None;
    let mut branch_id = None;
    let mut control_plane_url = default_control_plane_url();
    let mut actor_id = default_actor_id();
    let mut iter = rest.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--cluster-id" => cluster_id = Some(next_db_arg(flag, iter.next())?),
            "--branch-id" => branch_id = Some(next_db_arg(flag, iter.next())?),
            "--control-plane-url" => control_plane_url = next_db_arg(flag, iter.next())?,
            "--actor-id" => actor_id = next_db_arg(flag, iter.next())?,
            "--help" | "-h" => {
                print_db_branch_help();
                return Err(CliError::Usage(format!(
                    "db branch {context} help requested"
                )));
            }
            other => {
                return Err(CliError::Usage(format!(
                    "db branch {context}: unknown option '{other}'"
                )));
            }
        }
    }
    Ok(BranchTarget {
        cluster_id: required_db_arg("--cluster-id", cluster_id)?,
        branch_id,
        control_plane_url,
        actor_id,
    })
}

fn branches_endpoint(control_plane_url: &str, cluster_id: &str) -> String {
    format!(
        "{}/v1/managed-postgres/clusters/{cluster_id}/branches",
        control_plane_url.trim_end_matches('/')
    )
}

fn cmd_db_branch(rest: &[String]) -> Result<(), CliError> {
    let Some(action) = rest.first().map(String::as_str) else {
        print_db_branch_help();
        return Ok(());
    };
    let args = &rest[1..];
    match action {
        "create" => cmd_db_branch_create(args),
        "list" => cmd_db_branch_list(args),
        "get" => cmd_db_branch_get(args),
        "delete" => cmd_db_branch_delete(args),
        "help" | "--help" | "-h" => {
            print_db_branch_help();
            Ok(())
        }
        other => Err(CliError::Usage(format!(
            "db branch: unknown action '{other}' (expected create, list, get, or delete)"
        ))),
    }
}

fn cmd_db_branch_create(rest: &[String]) -> Result<(), CliError> {
    if wants_help(rest) {
        print_db_branch_help();
        return Ok(());
    }
    let mut cluster_id = None;
    let mut name = None;
    let mut parent_branch_id = None;
    let mut source_database = None;
    let mut mode = "head-cow".to_owned();
    let mut recovery_target_lsn = None;
    let mut redaction_policy_id = None;
    let mut terminate_source_connections = false;
    let mut provision_sync_deployment = false;
    let mut control_plane_url = default_control_plane_url();
    let mut actor_id = default_actor_id();

    let mut iter = rest.iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--cluster-id" => cluster_id = Some(next_db_arg(flag, iter.next())?),
            "--name" => name = Some(next_db_arg(flag, iter.next())?),
            "--parent-branch-id" => parent_branch_id = Some(next_db_arg(flag, iter.next())?),
            "--source-database" => source_database = Some(next_db_arg(flag, iter.next())?),
            "--mode" => mode = next_db_arg(flag, iter.next())?,
            "--recovery-target-lsn" => recovery_target_lsn = Some(next_db_arg(flag, iter.next())?),
            "--redaction-policy-id" => redaction_policy_id = Some(next_db_arg(flag, iter.next())?),
            "--terminate-source-connections" => terminate_source_connections = true,
            "--provision-sync-deployment" => provision_sync_deployment = true,
            "--control-plane-url" => control_plane_url = next_db_arg(flag, iter.next())?,
            "--actor-id" => actor_id = next_db_arg(flag, iter.next())?,
            "--help" | "-h" => {
                print_db_branch_help();
                return Err(CliError::Usage(
                    "db branch create help requested".to_owned(),
                ));
            }
            other => {
                return Err(CliError::Usage(format!(
                    "db branch create: unknown option '{other}'"
                )));
            }
        }
    }

    let cluster_id = required_db_arg("--cluster-id", cluster_id)?;
    let body = BranchCreateBody {
        name: required_db_arg("--name", name)?,
        parent_branch_id,
        mode: parse_branch_mode_flag(&mode)?,
        source_database,
        recovery_target_lsn,
        redaction_policy_id,
        terminate_source_connections,
        provision_sync_deployment,
    };
    let body = serde_json::to_string(&body)
        .map_err(|err| CliError::Db(format!("serialize branch request: {err}")))?;
    let endpoint = branches_endpoint(&control_plane_url, &cluster_id);
    let response = http_post_json(&endpoint, &body, &actor_id)?;
    println!("{response}");
    Ok(())
}

fn cmd_db_branch_list(rest: &[String]) -> Result<(), CliError> {
    if wants_help(rest) {
        print_db_branch_help();
        return Ok(());
    }
    let target = parse_branch_target(rest, "list")?;
    let endpoint = branches_endpoint(&target.control_plane_url, &target.cluster_id);
    let response = http_get(&endpoint, &target.actor_id)?;
    println!("{response}");
    Ok(())
}

fn cmd_db_branch_get(rest: &[String]) -> Result<(), CliError> {
    if wants_help(rest) {
        print_db_branch_help();
        return Ok(());
    }
    let target = parse_branch_target(rest, "get")?;
    let branch_id = required_db_arg("--branch-id", target.branch_id)?;
    let endpoint = format!(
        "{}/{branch_id}",
        branches_endpoint(&target.control_plane_url, &target.cluster_id)
    );
    let response = http_get(&endpoint, &target.actor_id)?;
    println!("{response}");
    Ok(())
}

fn cmd_db_branch_delete(rest: &[String]) -> Result<(), CliError> {
    if wants_help(rest) {
        print_db_branch_help();
        return Ok(());
    }
    let target = parse_branch_target(rest, "delete")?;
    let branch_id = required_db_arg("--branch-id", target.branch_id)?;
    let endpoint = format!(
        "{}/{branch_id}",
        branches_endpoint(&target.control_plane_url, &target.cluster_id)
    );
    let response = http_delete(&endpoint, &target.actor_id)?;
    println!("{response}");
    Ok(())
}

fn print_db_branch_help() {
    println!(
        "palimpsest db branch — operate managed Postgres database branches

Usage:
  palimpsest db branch create --cluster-id <id> --name <name> [options]
  palimpsest db branch list   --cluster-id <id> [--control-plane-url <url>] [--actor-id <id>]
  palimpsest db branch get    --cluster-id <id> --branch-id <id> [--control-plane-url <url>] [--actor-id <id>]
  palimpsest db branch delete --cluster-id <id> --branch-id <id> [--control-plane-url <url>] [--actor-id <id>]

Create options:
  --name <name>                    Branch name (required). 1-63 chars: letters, digits, '_' or '-'.
  --parent-branch-id <id>          Branch from another branch instead of the cluster's primary database.
  --source-database <db>           Source database for a root branch (default: postgres).
  --mode <head-cow|point-in-time>  Branch mode (default: head-cow). point-in-time restores into a dedicated cluster.
  --recovery-target-lsn <lsn>      Recovery target LSN (required for point-in-time branches).
  --redaction-policy-id <id>       Apply a clone redaction policy.
  --terminate-source-connections   Terminate source-database sessions so the copy-on-write clone can proceed.
  --provision-sync-deployment      Also provision and start a SyncDeployment (point-in-time branches only).

Shared options:
  --control-plane-url <url>        Control-plane base URL (default: PALIMPSEST_PAAS_CONTROL_PLANE_URL or http://127.0.0.1:8088).
  --actor-id <id>                  Audit actor id (default: PALIMPSEST_ACTOR_ID or local-cli).

Output: the control-plane JSON response is printed to stdout. Exit codes:
  0 success, 2 usage, 3 client error, 4 not found, 5 conflict, 6 unauthorized, 7 server error."
    );
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
    http_request("POST", url, Some(body), actor_id)
}

fn http_get(url: &str, actor_id: &str) -> Result<String, CliError> {
    http_request("GET", url, None, actor_id)
}

fn http_delete(url: &str, actor_id: &str) -> Result<String, CliError> {
    http_request("DELETE", url, None, actor_id)
}

fn http_request(
    method: &str,
    url: &str,
    body: Option<&str>,
    actor_id: &str,
) -> Result<String, CliError> {
    let parsed = ParsedHttpUrl::parse(url)?;
    let mut stream = std::net::TcpStream::connect((parsed.host.as_str(), parsed.port))
        .map_err(|err| CliError::Db(format!("connect to {}: {err}", parsed.authority)))?;
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Actor-Id: {}\r\nConnection: close\r\n\r\n{}",
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
    Err(CliError::ControlPlane {
        status,
        body: body.trim().to_owned(),
    })
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

        assert!(matches!(
            result,
            Err(CliError::ControlPlane { status: 409, .. })
        ));
    }

    #[test]
    fn exit_codes_map_control_plane_status_to_failure_class() {
        let code = |status| {
            exit_code_for(&CliError::ControlPlane {
                status,
                body: String::new(),
            })
        };
        assert_eq!(code(404), ExitCode::from(4));
        assert_eq!(code(409), ExitCode::from(5));
        assert_eq!(code(401), ExitCode::from(6));
        assert_eq!(code(403), ExitCode::from(6));
        assert_eq!(code(503), ExitCode::from(7));
        assert_eq!(code(400), ExitCode::from(3));
        assert_eq!(
            exit_code_for(&CliError::Usage("x".to_owned())),
            ExitCode::from(2)
        );
    }

    #[test]
    fn branch_mode_flag_accepts_dashed_and_underscored() {
        assert_eq!(parse_branch_mode_flag("head-cow").unwrap(), "head_cow");
        assert_eq!(parse_branch_mode_flag("head_cow").unwrap(), "head_cow");
        assert_eq!(
            parse_branch_mode_flag("point-in-time").unwrap(),
            "point_in_time"
        );
        assert!(parse_branch_mode_flag("nope").is_err());
    }

    #[test]
    fn branch_create_body_omits_unset_optionals() {
        let body = BranchCreateBody {
            name: "feature-x".to_owned(),
            parent_branch_id: None,
            mode: "head_cow".to_owned(),
            source_database: None,
            recovery_target_lsn: None,
            redaction_policy_id: None,
            terminate_source_connections: false,
            provision_sync_deployment: false,
        };
        let json = serde_json::to_value(&body).expect("serializes");
        assert_eq!(json["name"], "feature-x");
        assert_eq!(json["mode"], "head_cow");
        assert_eq!(json["terminate_source_connections"], false);
        assert!(json.get("parent_branch_id").is_none());
        assert!(json.get("source_database").is_none());
    }

    #[test]
    fn branch_target_requires_cluster_id() {
        let err = parse_branch_target(&[], "list").expect_err("cluster id required");
        assert!(matches!(err, CliError::Usage(_)));

        let target = parse_branch_target(
            &[
                "--cluster-id".to_owned(),
                "cluster_1".to_owned(),
                "--branch-id".to_owned(),
                "cluster_1:branch:feature_x".to_owned(),
            ],
            "get",
        )
        .expect("parses");
        assert_eq!(target.cluster_id, "cluster_1");
        assert_eq!(
            target.branch_id.as_deref(),
            Some("cluster_1:branch:feature_x")
        );
    }

    #[test]
    fn skills_install_defaults_to_codex_and_claude() {
        let options = parse_skills_install_options(&[]).expect("skills options should parse");

        assert_eq!(
            options.targets,
            vec![SkillTarget::Codex, SkillTarget::Claude]
        );
        assert!(options.codex_dir.ends_with(".codex/skills"));
        assert!(options.claude_dir.ends_with(".claude/skills"));
        assert!(!options.force);
        assert!(!options.dry_run);
    }

    #[test]
    fn skills_install_supports_target_and_directory_overrides() {
        let options = parse_skills_install_options(&[
            "--codex".to_owned(),
            "--codex-dir".to_owned(),
            "/tmp/codex-skills".to_owned(),
            "--claude-dir".to_owned(),
            "/tmp/claude-skills".to_owned(),
            "--force".to_owned(),
            "--dry-run".to_owned(),
        ])
        .expect("skills options should parse");

        assert_eq!(options.targets, vec![SkillTarget::Codex]);
        assert_eq!(options.codex_dir, PathBuf::from("/tmp/codex-skills"));
        assert_eq!(options.claude_dir, PathBuf::from("/tmp/claude-skills"));
        assert!(options.force);
        assert!(options.dry_run);
    }

    #[test]
    fn skills_install_creates_skill_and_is_idempotent() {
        let base_dir = temp_test_dir("skills-create");
        let first = install_skill_target(SkillTarget::Codex, &base_dir, false, false)
            .expect("skill should install");
        let second = install_skill_target(SkillTarget::Codex, &base_dir, false, false)
            .expect("same skill should be unchanged");

        assert_eq!(first.status, SkillInstallStatus::Created);
        assert_eq!(second.status, SkillInstallStatus::Unchanged);
        assert_eq!(
            std::fs::read_to_string(base_dir.join("palimpsest-cli").join("SKILL.md"))
                .expect("skill should be readable"),
            PALIMPSEST_CLI_SKILL
        );

        let _ = std::fs::remove_dir_all(base_dir);
    }

    #[test]
    fn skills_install_refuses_modified_existing_skill_without_force() {
        let base_dir = temp_test_dir("skills-force");
        let skill_dir = base_dir.join("palimpsest-cli");
        std::fs::create_dir_all(&skill_dir).expect("skill dir should be writable");
        std::fs::write(skill_dir.join("SKILL.md"), "custom").expect("skill should be writable");

        let result = install_skill_target(SkillTarget::Claude, &base_dir, false, false);
        assert!(result.is_err());

        let forced = install_skill_target(SkillTarget::Claude, &base_dir, true, false)
            .expect("force should replace skill");
        assert_eq!(forced.status, SkillInstallStatus::Updated);

        let _ = std::fs::remove_dir_all(base_dir);
    }

    #[test]
    fn permissions_eval_parses_typed_user_assignments() {
        let schema = BTreeMap::from([
            ("id".to_owned(), "int".to_owned()),
            ("is_admin".to_owned(), "bool".to_owned()),
            ("name".to_owned(), "text".to_owned()),
        ]);
        let mut values = BTreeMap::new();

        parse_user_assignment("id=42", &schema, &mut values).expect("id should parse");
        parse_user_assignment("is_admin=false", &schema, &mut values).expect("bool should parse");
        parse_user_json(r#"{"name":"Ada"}"#, &schema, &mut values)
            .expect("JSON user context should parse");

        assert_eq!(values.get("id"), Some(&UserValue::Int(42)));
        assert_eq!(values.get("is_admin"), Some(&UserValue::Bool(false)));
        assert_eq!(values.get("name"), Some(&UserValue::Text("Ada".to_owned())));
    }

    #[test]
    fn permissions_eval_rejects_unknown_user_field() {
        let schema = BTreeMap::from([("id".to_owned(), "int".to_owned())]);
        let mut values = BTreeMap::new();

        let result = parse_user_assignment("org_id=7", &schema, &mut values);

        assert!(result.is_err());
    }

    #[test]
    fn permissions_eval_options_support_json_output_and_repeated_queries() {
        let config_path = write_temp_permissions_config("options", "");
        let options = parse_permission_eval_options(&[
            config_path.display().to_string(),
            "--query".to_owned(),
            "SELECT id FROM posts".to_owned(),
            "--query".to_owned(),
            "SELECT id FROM authors".to_owned(),
            "--user".to_owned(),
            "id=42".to_owned(),
            "--json".to_owned(),
        ])
        .expect("permissions eval options should parse");

        assert_eq!(options.config_path, config_path);
        assert_eq!(options.queries.len(), 2);
        assert_eq!(options.user_values.get("id"), Some(&UserValue::Int(42)));
        assert_eq!(options.output, PermissionEvalOutput::Json);

        let _ = std::fs::remove_file(options.config_path);
    }

    #[test]
    fn permissions_eval_runs_rewriter_against_query() {
        let config_path = write_temp_permissions_config(
            "rewrite",
            r#"
[[permissions.rules]]
name = "posts_owner"
table = "posts"
mode = "row_visibility"
predicate = "author_id = $user.id"
"#,
        );

        let result = cmd_permissions_eval(&[
            config_path.display().to_string(),
            "--query".to_owned(),
            "SELECT id FROM posts".to_owned(),
            "--user".to_owned(),
            "id=42".to_owned(),
        ]);

        assert!(result.is_ok());
        let _ = std::fs::remove_file(config_path);
    }

    fn write_temp_permissions_config(name: &str, rules: &str) -> PathBuf {
        let path = temp_test_path(name, "toml");
        let content = format!(
            r#"
[permissions.user_schema]
id = "int"
is_admin = "bool"
name = "text"
{rules}
"#
        );
        std::fs::write(&path, content).expect("temp config should be writable");
        path
    }

    fn temp_test_dir(name: &str) -> PathBuf {
        let path = temp_test_path(name, "dir");
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn temp_test_path(name: &str, suffix: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "palimpsest-cli-{name}-{}-{}.toml",
                std::process::id(),
                std::thread::current().name().unwrap_or("test"),
            ))
            .with_extension(suffix)
    }
}
