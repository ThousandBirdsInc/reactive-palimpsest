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

use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

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
    queries: QueriesConfig,
    #[serde(default)]
    upstream: Option<UpstreamConfig>,
}

/// Named prepared-query registration (§ named-queries doc).
///
/// `files` lists sqlc-format query files registered at startup;
/// registration failures abort startup with the query name and the
/// exact rejected construct. When any file is configured, raw-SQL
/// subscribes are refused unless `inline_sql = true`.
#[derive(Debug, Default, Deserialize)]
struct QueriesConfig {
    #[serde(default)]
    files: Vec<PathBuf>,
    #[serde(default)]
    inline_sql: Option<bool>,
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
    #[error("read query file '{0}': {1}")]
    ReadQueryFile(PathBuf, #[source] std::io::Error),
    #[error("register queries: {0}")]
    RegisterQueries(String),
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
/// failure class without parsing stderr. `0` success, `2` usage, `1`
/// everything else.
fn exit_code_for(err: &CliError) -> ExitCode {
    let code: u8 = match err {
        CliError::Usage(_) => 2,
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

/// Builds the named-query registry from the `[queries]` config
/// section. Any registration failure aborts loudly — a template the
/// engine cannot execute must never survive to subscribe time.
///
/// Query files are resolved relative to the config file's directory.
fn build_query_registry(
    config: &QueriesConfig,
    config_path: &Path,
    catalog: &Catalog,
) -> Result<Option<palimpsest_sql::QueryRegistry>, CliError> {
    if config.files.is_empty() {
        return Ok(None);
    }
    let base = config_path.parent().unwrap_or_else(|| Path::new("."));
    let mut registry = palimpsest_sql::QueryRegistry::new();
    for file in &config.files {
        let path = if file.is_absolute() {
            file.clone()
        } else {
            base.join(file)
        };
        let source =
            fs::read_to_string(&path).map_err(|err| CliError::ReadQueryFile(path.clone(), err))?;
        let names = registry
            .register_sqlc_source(&source, &path.display().to_string(), catalog)
            .map_err(|err| CliError::RegisterQueries(err.to_string()))?;
        info!(file = %path.display(), queries = names.len(), "registered named queries");
    }
    Ok(Some(registry))
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
        "timestamptz" => Ok(ColumnType::TimestampTz),
        "date" => Ok(ColumnType::Date),
        "time" => Ok(ColumnType::Time),
        "interval" => Ok(ColumnType::Interval),
        "numeric" | "decimal" => Ok(ColumnType::Numeric),
        "bytea" => Ok(ColumnType::Bytea),
        "uuid" => Ok(ColumnType::Uuid),
        "jsonb" | "json" => Ok(ColumnType::Jsonb),
        "enum" => Ok(ColumnType::Enum),
        "array" => Ok(ColumnType::Array),
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
        ColumnType::TimestampTz => "timestamptz",
        ColumnType::Date => "date",
        ColumnType::Time => "time",
        ColumnType::Interval => "interval",
        ColumnType::Numeric => "numeric",
        ColumnType::Bytea => "bytea",
        ColumnType::Uuid => "uuid",
        ColumnType::Jsonb => "jsonb",
        ColumnType::Enum => "enum",
        ColumnType::Array => "array",
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

    let query_registry = build_query_registry(&config.queries, &path, &catalog)?;

    let mut builder = Palimpsest::builder()
        .with_wal(EmptyWalRuntime::default())
        .with_permissions(compiled)
        .with_grpc_addr(config.grpc.addr)
        .with_metrics_addr(config.metrics.addr);

    if let Some(registry) = query_registry {
        builder = builder.with_query_registry(registry);
    }
    if let Some(inline_sql) = config.queries.inline_sql {
        builder = builder.with_inline_sql(inline_sql);
    }

    builder = match config.auth {
        AuthConfig::Anonymous => builder.with_auth(AnonymousAuthenticator),
        AuthConfig::Jwt(jwt) => builder.with_auth(
            JwtAuthenticator::from_config(jwt)
                .await
                .map_err(|err| CliError::BuildServer(err.to_string()))?,
        ),
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

    let registry = build_query_registry(&config.queries, path, &catalog)?;
    let query_count = registry
        .as_ref()
        .map_or(0, palimpsest_sql::QueryRegistry::len);

    println!(
        "{}: ok ({} permission rule(s), {} user-context field(s), {} named quer{})",
        path.display(),
        rules.len(),
        config.permissions.user_schema.len(),
        query_count,
        if query_count == 1 { "y" } else { "ies" }
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
        UserValue::Uuid(value) => format!("uuid:{value}"),
        UserValue::Jsonb(value) => format!("jsonb:{value}"),
        UserValue::Enum(value) => format!("enum:{value:?}"),
        UserValue::List(items) => {
            let rendered: Vec<String> = items.iter().map(user_value_label).collect();
            format!("list:[{}]", rendered.join(","))
        }
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
    // JSON arrays bind as list values: each element is parsed against
    // the field's declared (element) type, for `ANY($user.field)`
    // predicates. `jsonb` fields keep the raw document instead.
    if let serde_json::Value::Array(items) = value {
        if schema_type(field, schema)? != ColumnType::Jsonb {
            let elements = items
                .iter()
                .map(|item| parse_json_user_value(field, item, schema))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(UserValue::List(elements));
        }
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
        ColumnType::Timestamp
        | ColumnType::TimestampTz
        | ColumnType::Date
        | ColumnType::Time
        | ColumnType::Interval => value
            .as_str()
            .map(|value| UserValue::Timestamp(value.to_owned()))
            .ok_or_else(|| user_value_type_error(field, "string timestamp", value)),
        ColumnType::Numeric => value
            .as_f64()
            .map(UserValue::Float)
            .ok_or_else(|| user_value_type_error(field, "number", value)),
        ColumnType::Uuid => value
            .as_str()
            .ok_or_else(|| user_value_type_error(field, "string uuid", value))
            .and_then(|raw| {
                UserValue::uuid(raw).map_err(|_| user_value_type_error(field, "uuid", value))
            }),
        ColumnType::Jsonb => Ok(UserValue::Jsonb(value.clone())),
        ColumnType::Enum => value
            .as_str()
            .map(|value| UserValue::Enum(value.to_owned()))
            .ok_or_else(|| user_value_type_error(field, "string enum label", value)),
        ColumnType::Bytea | ColumnType::Array | ColumnType::Unknown => {
            Err(CliError::EvalPermissions(format!(
                "unsupported user-context type for field '{field}'"
            )))
        }
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
        ColumnType::Timestamp
        | ColumnType::TimestampTz
        | ColumnType::Date
        | ColumnType::Time
        | ColumnType::Interval => Ok(UserValue::Timestamp(raw_value.to_owned())),
        ColumnType::Numeric => raw_value.parse::<f64>().map(UserValue::Float).map_err(|_| {
            CliError::EvalPermissions(format!(
                "user field '{field}' expects a number, got '{raw_value}'"
            ))
        }),
        ColumnType::Uuid => UserValue::uuid(raw_value).map_err(|_| {
            CliError::EvalPermissions(format!(
                "user field '{field}' expects a uuid, got '{raw_value}'"
            ))
        }),
        ColumnType::Jsonb => serde_json::from_str(raw_value)
            .map(UserValue::Jsonb)
            .map_err(|_| {
                CliError::EvalPermissions(format!(
                    "user field '{field}' expects a JSON document, got '{raw_value}'"
                ))
            }),
        ColumnType::Enum => Ok(UserValue::Enum(raw_value.to_owned())),
        ColumnType::Bytea | ColumnType::Array | ColumnType::Unknown => {
            Err(CliError::EvalPermissions(format!(
                "unsupported user-context type for field '{field}'"
            )))
        }
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
description: Use when operating the Palimpsest CLI, including config validation, permission query evaluation, catalog inspection, replication slot diagnostics, and CLI installation checks.
---

# Palimpsest CLI

Use the `palimpsest` binary for local operation and diagnostics. Start with `palimpsest help` when the current command surface matters.

## Core Commands

- `palimpsest validate-config <config>`: parse TOML, compile permission rules, and register any `[queries]` sqlc files (named prepared queries) — registration failures name the query and the exact rejected construct.
- `palimpsest permissions eval <config> --query <sql> --user field=value`: compile configured permissions and show canonical query MIR before and after rewriting.
- `palimpsest dump-catalog [config]`: print the configured demo catalog as JSON.
- `palimpsest slot-info <config>`: inspect upstream replication slot status when the binary was built with `--features slot-info`.

## Output And Exit Codes (for agents)

Exit codes encode the failure class: `0` success, `2` usage error, `1` everything else.

## Preferred Workflow

1. Run `palimpsest validate-config <config>` before serving or debugging permission behavior.
2. Use `palimpsest permissions eval` for permission model questions instead of inferring rewrites by inspection.
3. Use `cargo install palimpsest-cli` for the published CLI, or `cargo install --path crates/palimpsest-cli` from a checkout.
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
    use super::*;

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
    fn permissions_eval_parses_uuid_jsonb_and_enum_user_values() {
        let schema = BTreeMap::from([
            ("tenant_id".to_owned(), "uuid".to_owned()),
            ("prefs".to_owned(), "jsonb".to_owned()),
            ("role".to_owned(), "enum".to_owned()),
        ]);
        let mut values = BTreeMap::new();

        parse_user_assignment(
            "tenant_id=67E55044-10B1-426F-9247-BB680E5FE0C8",
            &schema,
            &mut values,
        )
        .expect("uuid should parse");
        parse_user_assignment("role=admin", &schema, &mut values).expect("enum should parse");
        parse_user_json(r#"{"prefs":{"theme":"dark"}}"#, &schema, &mut values)
            .expect("jsonb should parse");

        assert_eq!(
            values.get("tenant_id"),
            Some(&UserValue::Uuid(
                "67e55044-10b1-426f-9247-bb680e5fe0c8".to_owned()
            ))
        );
        assert_eq!(
            values.get("role"),
            Some(&UserValue::Enum("admin".to_owned()))
        );
        assert_eq!(
            values.get("prefs"),
            Some(&UserValue::Jsonb(serde_json::json!({"theme": "dark"})))
        );
    }

    #[test]
    fn permissions_eval_rejects_malformed_uuid_assignment() {
        let schema = BTreeMap::from([("tenant_id".to_owned(), "uuid".to_owned())]);
        let mut values = BTreeMap::new();
        assert!(parse_user_assignment("tenant_id=not-a-uuid", &schema, &mut values).is_err());
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
