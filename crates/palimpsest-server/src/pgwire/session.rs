// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! One Postgres wire-protocol connection: startup and authentication,
//! then the simple and extended query cycles, each statement answered
//! from the materialized view of its live query.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use palimpsest_dataflow::palimpsest::{compile_mir, Row};
use palimpsest_permissions::{rewrite, UserContext};
use palimpsest_sql::lower::parse_and_lower_with_limits;
use palimpsest_sql::mir::MirGraph;
use palimpsest_sql::prepared::{BindError, ParamSpec, ParamValue, QueryRegistry};
use palimpsest_sql::{Catalog, ColumnSchema, QueryLimits, SqlError, TableSchema};
use palimpsest_wal::Datum;
use sqlparser::ast::{
    visit_expressions, visit_relations, Expr, ObjectName, Query, Statement, UnaryOperator, Value,
    Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use super::protocol::{
    read_message, read_startup, FieldDescription, FrontendMessage, Outbox, Startup,
};
use super::types;
use crate::diff::DiffEvent;
use crate::grpc::{
    blocking_subscribe, ensure_canonical_pump, schema_definition_from_plan, QueryCore,
    SubscribeBlockingError, SubscribeOutcome, WalTableLookup,
};
use crate::security::LimitDecision;
use crate::subscription::{
    ClientSubscriptionId, ConnectionId, QueryId, SchemaDefinition, SubscriptionId,
};

/// Name the one-off registry gives every extended-protocol statement
/// that carries `$N` placeholders.
const STATEMENT_QUERY_NAME: &str = "pgwire";

/// `server_version` reported to clients. Drivers parse this to pick
/// protocol features; 16 keeps every current driver on paths this
/// front end implements.
const SERVER_VERSION: &str = "16.0";

/// A wire error: SQLSTATE plus message, with an optional detail line.
#[derive(Debug, Clone)]
pub(super) struct PgError {
    code: &'static str,
    message: String,
    detail: Option<String>,
}

impl PgError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            detail: None,
        }
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    fn syntax(message: impl Into<String>) -> Self {
        Self::new("42601", message)
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self::new("0A000", message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new("XX000", message)
    }

    fn from_sql_error(err: &SqlError) -> Self {
        match err {
            SqlError::Parse(_) | SqlError::StatementCount(_) => Self::syntax(err.to_string()),
            SqlError::QueryTooLarge { .. }
            | SqlError::QueryTooComplex { .. }
            | SqlError::QueryTooDeep { .. } => Self::new("54000", err.to_string()),
            SqlError::UnknownTable(_) => Self::new("42P01", err.to_string()),
            SqlError::UnknownColumn(_) => Self::new("42703", err.to_string()),
            SqlError::AmbiguousColumn(_) => Self::new("42702", err.to_string()),
            SqlError::TypeMismatch(_) => Self::new("42804", err.to_string()),
            _ => Self::unsupported(err.to_string()),
        }
    }

    /// Maps the engine's stable subscribe error codes onto SQLSTATEs.
    fn from_engine(code: &str, message: String) -> Self {
        match code {
            "permission_denied" | "permission_unenforceable" | "inline_sql_disabled" => {
                Self::new("42501", message)
            }
            "schema_lookup_failed" => Self::new("42P01", message),
            "unknown_query" => Self::new("42704", message),
            "invalid_params" => Self::new("22023", message),
            _ => Self::internal(message),
        }
    }

    fn from_bind_error(err: &BindError) -> Self {
        match err {
            BindError::UnknownQuery(_) => Self::new("42704", err.to_string()),
            BindError::Rejected { source, .. } => Self::from_sql_error(source),
            other => Self::new("22023", other.to_string()),
        }
    }
}

/// Outcome of running one query against its live view.
struct ResultSet {
    schema: SchemaDefinition,
    rows: Vec<Row>,
}

/// A subscription kept open for the connection's lifetime so the
/// query's shared host plan (and the cursor pump feeding it) stays
/// alive between executions. Keyed by the bound SQL text.
struct Anchor {
    subscription: SubscriptionId,
    canonical: String,
    schema: SchemaDefinition,
    drain: tokio::task::JoinHandle<()>,
}

type Anchors = Arc<Mutex<HashMap<String, Anchor>>>;

/// Utility statements answered locally.
#[derive(Debug, Clone)]
enum Utility {
    Set,
    Begin,
    Commit,
    Rollback,
    Discard,
    Show { column: String, value: String },
}

/// What a prepared statement (or a simple-query statement) resolved to.
#[derive(Clone)]
enum StatementKind {
    Empty,
    Query {
        /// Template SQL (placeholders intact) for parameterised
        /// statements; the final SQL otherwise.
        sql: String,
        /// One-off registry holding the template when `params` is
        /// non-empty; binding renders typed literals into it.
        registry: Option<Arc<QueryRegistry>>,
        params: Vec<ParamSpec>,
        /// Lowered MIR for statements without parameters.
        graph: Option<MirGraph>,
        /// Result columns, known at parse time.
        schema: SchemaDefinition,
    },
    Utility(Utility),
}

struct Prepared {
    kind: StatementKind,
    /// Parameter type OIDs the client declared in `Parse` (`0` =
    /// infer), padded to the parameter count.
    param_oids: Vec<u32>,
}

enum PortalKind {
    Empty,
    Query {
        sql: String,
        graph: MirGraph,
        schema: SchemaDefinition,
    },
    Utility(Utility),
}

struct Portal {
    kind: PortalKind,
    result_formats: Vec<i16>,
    /// Rows not yet sent; `None` until the first `Execute`.
    rows: Option<VecDeque<Row>>,
    sent: usize,
}

struct Session {
    core: QueryCore,
    connection: ConnectionId,
    user_ctx: UserContext,
    user_name: String,
    out: Outbox,
    writer: OwnedWriteHalf,
    statements: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    anchors: Anchors,
    /// Insertion order of `anchors`, for eviction at the cap.
    anchor_order: VecDeque<String>,
    next_client_id: u64,
    in_transaction: bool,
    failed_transaction: bool,
    /// Set after an extended-protocol error: ignore everything until
    /// the client's next `Sync`.
    skip_until_sync: bool,
}

/// Drives one accepted socket to completion.
pub(super) async fn serve_connection(
    core: QueryCore,
    mut stream: TcpStream,
    peer: SocketAddr,
    shutdown: watch::Receiver<bool>,
) {
    let startup = match read_startup(&mut stream).await {
        Ok(startup) => startup,
        Err(err) => {
            debug!(%peer, %err, "pgwire: startup failed");
            return;
        }
    };
    let params = match startup {
        Startup::Start(params) => params,
        Startup::Cancel => return,
    };
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut out = Outbox::new();

    if !matches!(
        core.reconnect_tracker.admit(peer.ip()),
        LimitDecision::Allowed
    ) {
        out.error_response("53300", "too many connections from this address", None);
        let mut writer = writer;
        let _ = out.flush(&mut writer).await;
        return;
    }

    let user_name = params
        .iter()
        .find(|(key, _)| key == "user")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    let application_name = params
        .iter()
        .find(|(key, _)| key == "application_name")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();

    let mut writer = writer;
    let user_ctx = match authenticate(&core, &mut reader, &mut writer, &mut out, &user_name).await {
        Ok(ctx) => ctx,
        Err(err) => {
            if let Some(err) = err {
                out.error_response(err.code, &err.message, err.detail.as_deref());
                let _ = out.flush(&mut writer).await;
            }
            return;
        }
    };

    let connection = core.next_connection();
    out.authentication_ok();
    for (name, value) in [
        ("server_version", SERVER_VERSION),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
        ("is_superuser", "off"),
        ("default_transaction_read_only", "on"),
        ("application_name", application_name.as_str()),
        ("session_authorization", user_name.as_str()),
    ] {
        out.parameter_status(name, value);
    }
    // No cancel support: the key only needs to be well-formed.
    out.backend_key_data(i32::try_from(connection.get()).unwrap_or(1), 0);
    out.ready_for_query(b'I');
    if out.flush(&mut writer).await.is_err() {
        return;
    }
    info!(%peer, connection = connection.get(), user = %user_name, "pgwire: session started");

    let mut session = Session {
        core,
        connection,
        user_ctx,
        user_name,
        out,
        writer,
        statements: HashMap::new(),
        portals: HashMap::new(),
        anchors: Arc::new(Mutex::new(HashMap::new())),
        anchor_order: VecDeque::new(),
        next_client_id: 1,
        in_transaction: false,
        failed_transaction: false,
        skip_until_sync: false,
    };
    session.run(&mut reader, shutdown).await;
    session.close();
    info!(%peer, connection = connection.get(), "pgwire: session ended");
}

/// Runs the configured [`crate::auth::Authenticator`]. An authenticator
/// that accepts an empty header set (anonymous) admits the connection
/// without a prompt; otherwise the client is asked for a cleartext
/// password, which is presented as the `authorization: Bearer` token
/// gRPC clients would send.
///
/// `Err(None)` means the peer went away mid-handshake.
async fn authenticate(
    core: &QueryCore,
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    out: &mut Outbox,
    user_name: &str,
) -> Result<UserContext, Option<PgError>> {
    let empty = tonic::metadata::MetadataMap::new();
    if let Ok(ctx) = core.auth.authenticate(&empty).await {
        return Ok(ctx);
    }
    out.authentication_cleartext_password();
    out.flush(writer).await.map_err(|_| None)?;
    let password = match read_message(reader).await {
        Ok(Some(FrontendMessage::Password(password))) => password,
        Ok(Some(_)) => {
            return Err(Some(PgError::new(
                "08P01",
                "expected a password message during authentication",
            )))
        }
        Ok(None) | Err(_) => return Err(None),
    };
    let mut headers = tonic::metadata::MetadataMap::new();
    let value = format!("Bearer {password}").parse().map_err(|_| {
        Some(PgError::new(
            "28P01",
            format!("password authentication failed for user \"{user_name}\""),
        ))
    })?;
    headers.insert("authorization", value);
    core.auth.authenticate(&headers).await.map_err(|err| {
        Some(
            PgError::new(
                "28P01",
                format!("password authentication failed for user \"{user_name}\""),
            )
            .with_detail(err.to_string()),
        )
    })
}

impl Session {
    async fn run(
        &mut self,
        reader: &mut BufReader<OwnedReadHalf>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            let message = tokio::select! {
                message = read_message(reader) => match message {
                    Ok(Some(message)) => message,
                    Ok(None) => return,
                    Err(err) => {
                        debug!(%err, "pgwire: read failed");
                        return;
                    }
                },
                _ = shutdown.changed() => {
                    self.out.error_response(
                        "57P01",
                        "terminating connection due to administrator command",
                        None,
                    );
                    let _ = self.out.flush(&mut self.writer).await;
                    return;
                }
            };
            let flush = match message {
                FrontendMessage::Query(sql) => {
                    self.simple_query(&sql).await;
                    true
                }
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_oids,
                } => {
                    if !self.skip_until_sync {
                        if let Err(err) = self.parse(name, &sql, param_oids) {
                            self.fail(err);
                        } else {
                            self.out.parse_complete();
                        }
                    }
                    false
                }
                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    params,
                    result_formats,
                } => {
                    if !self.skip_until_sync {
                        match self.bind(&statement, &param_formats, &params, result_formats) {
                            Ok(bound) => {
                                self.portals.insert(portal, bound);
                                self.out.bind_complete();
                            }
                            Err(err) => self.fail(err),
                        }
                    }
                    false
                }
                FrontendMessage::Describe { kind, name } => {
                    if !self.skip_until_sync {
                        if let Err(err) = self.describe(kind, &name) {
                            self.fail(err);
                        }
                    }
                    false
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    if !self.skip_until_sync {
                        if let Err(err) = self.execute(&portal, max_rows).await {
                            self.fail(err);
                        }
                    }
                    false
                }
                FrontendMessage::Close { kind, name } => {
                    if !self.skip_until_sync {
                        match kind {
                            b'S' => {
                                self.statements.remove(&name);
                            }
                            b'P' => {
                                self.portals.remove(&name);
                            }
                            _ => {}
                        }
                        self.out.close_complete();
                    }
                    false
                }
                FrontendMessage::Sync => {
                    self.skip_until_sync = false;
                    self.out.ready_for_query(self.transaction_status());
                    true
                }
                FrontendMessage::Flush => true,
                FrontendMessage::Terminate => return,
                FrontendMessage::Password(_) => {
                    self.fail(PgError::new("08P01", "unexpected password message"));
                    false
                }
                FrontendMessage::Unknown(tag) => {
                    self.fail(PgError::new(
                        "08P01",
                        format!("unsupported frontend message type '{}'", char::from(tag)),
                    ));
                    false
                }
            };
            if flush && self.out.flush(&mut self.writer).await.is_err() {
                return;
            }
        }
    }

    /// Reports an extended-protocol error and skips to the next `Sync`.
    fn fail(&mut self, err: PgError) {
        self.out
            .error_response(err.code, &err.message, err.detail.as_deref());
        self.skip_until_sync = true;
        if self.in_transaction {
            self.failed_transaction = true;
        }
    }

    const fn transaction_status(&self) -> u8 {
        if self.failed_transaction {
            b'E'
        } else if self.in_transaction {
            b'T'
        } else {
            b'I'
        }
    }

    /// Releases every anchor and the connection's router state.
    fn close(&self) {
        let anchors: Vec<Anchor> = self
            .anchors
            .lock()
            .expect("anchor lock")
            .drain()
            .map(|(_, anchor)| anchor)
            .collect();
        for anchor in anchors {
            anchor.drain.abort();
            let _ = self.core.router.unsubscribe(anchor.subscription);
            self.core
                .dataflow_host
                .release(&anchor.canonical, anchor.subscription.get());
        }
        let _ = self.core.router.disconnect(self.connection);
    }

    // ------------------------------------------------------------------
    // Simple query cycle
    // ------------------------------------------------------------------

    async fn simple_query(&mut self, sql: &str) {
        let text = sql.trim();
        let text = text.trim_end_matches(';').trim();
        if text.is_empty() {
            self.out.empty_query_response();
            self.out.ready_for_query(self.transaction_status());
            return;
        }
        let statements = match parse_statements(text) {
            Ok(statements) => statements,
            Err(err) => {
                self.out
                    .error_response(err.code, &err.message, err.detail.as_deref());
                self.out.ready_for_query(self.transaction_status());
                return;
            }
        };
        let single = statements.len() == 1;
        for statement in statements {
            let statement_text = if single {
                text.to_owned()
            } else {
                statement.to_string()
            };
            if let Err(err) = self.run_simple_statement(&statement, &statement_text).await {
                self.out
                    .error_response(err.code, &err.message, err.detail.as_deref());
                if self.in_transaction {
                    self.failed_transaction = true;
                }
                break;
            }
        }
        self.out.ready_for_query(self.transaction_status());
    }

    async fn run_simple_statement(
        &mut self,
        statement: &Statement,
        text: &str,
    ) -> Result<(), PgError> {
        let kind = self.classify(statement, text)?;
        if self.failed_transaction
            && !matches!(
                kind,
                StatementKind::Utility(Utility::Commit | Utility::Rollback)
            )
        {
            return Err(PgError::new(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        match kind {
            StatementKind::Empty => self.out.empty_query_response(),
            StatementKind::Utility(utility) => self.run_utility(&utility),
            StatementKind::Query {
                sql, graph, params, ..
            } => {
                if !params.is_empty() {
                    return Err(PgError::new(
                        "42P02",
                        "there is no parameter $1: the simple query protocol cannot bind parameters",
                    ));
                }
                let graph = graph.ok_or_else(|| PgError::internal("statement carried no plan"))?;
                let result = self.execute_query(&sql, &graph).await?;
                let fields = fields_for(&result.schema, &[]);
                self.out.row_description(&fields);
                for row in &result.rows {
                    write_data_row(&mut self.out, row, &result.schema, &[]);
                }
                self.out
                    .command_complete(&format!("SELECT {}", result.rows.len()));
            }
        }
        Ok(())
    }

    fn run_utility(&mut self, utility: &Utility) {
        match utility {
            Utility::Set => self.out.command_complete("SET"),
            Utility::Begin => {
                self.in_transaction = true;
                self.out.command_complete("BEGIN");
            }
            Utility::Commit => {
                let tag = if self.failed_transaction {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                };
                self.in_transaction = false;
                self.failed_transaction = false;
                self.out.command_complete(tag);
            }
            Utility::Rollback => {
                self.in_transaction = false;
                self.failed_transaction = false;
                self.out.command_complete("ROLLBACK");
            }
            Utility::Discard => self.out.command_complete("DISCARD ALL"),
            Utility::Show { column, value } => {
                self.out.row_description(&[FieldDescription {
                    name: column.clone(),
                    type_oid: types::TEXT,
                    type_len: -1,
                    format: 0,
                }]);
                self.out.data_row(std::iter::once(Some(value.as_bytes())));
                self.out.command_complete("SHOW");
            }
        }
    }

    // ------------------------------------------------------------------
    // Statement resolution
    // ------------------------------------------------------------------

    /// Resolves a parsed statement. `text` is the statement's own SQL,
    /// used (unchanged) as the engine-facing query text so two clients
    /// sending the same bytes share one plan.
    fn classify(&self, statement: &Statement, text: &str) -> Result<StatementKind, PgError> {
        match statement {
            Statement::Query(_) => {
                if !self.core.named.inline_sql_enabled() {
                    return Err(PgError::new(
                        "42501",
                        "this server only accepts registered named queries; run EXECUTE <name>(<args>)",
                    ));
                }
                self.check_relations(statement)?;
                if has_placeholders(statement) {
                    // Parameterised: the caller (Parse) builds the
                    // registry-backed statement.
                    return self.prepare_parameterised(text, statement);
                }
                let graph = parse_and_lower_with_limits(text, QueryLimits::DEFAULT)
                    .map_err(|err| PgError::from_sql_error(&err))?;
                let schema = self.result_schema(text, &graph)?;
                Ok(StatementKind::Query {
                    sql: text.to_owned(),
                    registry: None,
                    params: Vec::new(),
                    graph: Some(graph),
                    schema,
                })
            }
            Statement::Execute {
                name, parameters, ..
            } => {
                let (sql, graph) = self.bind_named(name, parameters)?;
                let schema = self.result_schema(&sql, &graph)?;
                Ok(StatementKind::Query {
                    sql,
                    registry: None,
                    params: Vec::new(),
                    graph: Some(graph),
                    schema,
                })
            }
            Statement::SetVariable { .. }
            | Statement::SetNames { .. }
            | Statement::SetTimeZone { .. }
            | Statement::SetNamesDefault { .. } => Ok(StatementKind::Utility(Utility::Set)),
            Statement::StartTransaction { .. } => Ok(StatementKind::Utility(Utility::Begin)),
            Statement::Commit { .. } => Ok(StatementKind::Utility(Utility::Commit)),
            Statement::Rollback { .. } => Ok(StatementKind::Utility(Utility::Rollback)),
            Statement::Discard { .. } => Ok(StatementKind::Utility(Utility::Discard)),
            Statement::ShowVariable { variable } => {
                let name = variable
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let value = show_value(&name, &self.user_name).ok_or_else(|| {
                    PgError::new(
                        "42704",
                        format!("unrecognized configuration parameter \"{name}\""),
                    )
                })?;
                Ok(StatementKind::Utility(Utility::Show {
                    column: name,
                    value,
                }))
            }
            Statement::Prepare { .. } | Statement::Deallocate { .. } => Err(PgError::unsupported(
                "server-side PREPARE is not supported; use your driver's prepared statements (the extended protocol)",
            )),
            Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_) => {
                Err(PgError::new(
                    "25006",
                    "cannot execute a write statement: this server is a read-only view of its upstream database",
                ))
            }
            _ => Err(PgError::unsupported(
                "only SELECT statements are supported over the Postgres wire protocol",
            )),
        }
    }

    /// Builds a registry-backed statement for SQL with `$N`
    /// placeholders. Parameter types are inferred from the columns
    /// each placeholder is compared against (or explicit casts), the
    /// same way named queries are registered.
    fn prepare_parameterised(
        &self,
        text: &str,
        statement: &Statement,
    ) -> Result<StatementKind, PgError> {
        let catalog = self.catalog_for(statement);
        let mut registry = QueryRegistry::new();
        let source = format!("-- name: {STATEMENT_QUERY_NAME} :many\n{text};\n");
        registry
            .register_sqlc_source(&source, "pgwire", &catalog)
            .map_err(|err| {
                let message = err.to_string();
                let lower = message.to_ascii_lowercase();
                if lower.contains("unknown table") {
                    PgError::new("42P01", message)
                } else if lower.contains("unknown column") {
                    PgError::new("42703", message)
                } else if lower.contains("parameter") || lower.contains("placeholder") {
                    PgError::new("42P18", message)
                } else {
                    PgError::unsupported(message)
                }
            })?;
        let params = registry
            .get(STATEMENT_QUERY_NAME)
            .map(|prepared| prepared.params.clone())
            .unwrap_or_default();
        // The result shape does not depend on parameter values, so a
        // dummy binding is enough to learn it.
        let dummy: HashMap<String, ParamValue> = params
            .iter()
            .map(|spec| (format!("${}", spec.position), dummy_value(spec)))
            .collect();
        let bound = registry
            .bind(STATEMENT_QUERY_NAME, &dummy)
            .map_err(|err| PgError::from_bind_error(&err))?;
        let schema = self.result_schema(&bound.sql, &bound.graph)?;
        Ok(StatementKind::Query {
            sql: text.to_owned(),
            registry: Some(Arc::new(registry)),
            params,
            graph: None,
            schema,
        })
    }

    /// Catalog of every table the statement references, from the WAL
    /// runtime's schema. Tables the runtime does not know are left out
    /// so registration reports them as unknown.
    /// Rejects a statement that names a table the runtime does not
    /// replicate with `42P01`, the way Postgres answers an unknown
    /// relation. Without this the engine would fall back to the
    /// pass-through snapshot path, which cannot serve the table either
    /// and fails later with a less useful message. CTE names are not
    /// tables and are skipped.
    fn check_relations(&self, statement: &Statement) -> Result<(), PgError> {
        let mut relations = RelationNames::default();
        let _ = statement.visit(&mut relations);
        for name in relations.tables {
            if relations.ctes.contains(&name) || self.core.wal.table_schema(&name).is_some() {
                continue;
            }
            return Err(PgError::new(
                "42P01",
                format!("relation \"{name}\" does not exist"),
            ));
        }
        Ok(())
    }

    fn catalog_for(&self, statement: &Statement) -> Catalog {
        let mut names: Vec<String> = Vec::new();
        let _ = visit_relations(statement, |name| {
            if let Some(last) = name.0.last() {
                if !names.contains(&last.value) {
                    names.push(last.value.clone());
                }
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
        let tables = names.into_iter().filter_map(|name| {
            let (_, scalar) = self.core.wal.table_schema(&name)?;
            let columns = scalar
                .columns()
                .iter()
                .map(|(column, ty)| ColumnSchema::new(column.clone(), *ty))
                .collect();
            Some(TableSchema::new(name, columns))
        });
        Catalog::new(tables)
    }

    /// Binds `EXECUTE name(args)` against the named-query registry.
    fn bind_named(
        &self,
        name: &sqlparser::ast::ObjectName,
        args: &[Expr],
    ) -> Result<(String, MirGraph), PgError> {
        let query_name = name
            .0
            .last()
            .map(|ident| ident.value.clone())
            .unwrap_or_default();
        let Some(registry) = self.core.named.registry() else {
            return Err(PgError::new(
                "42704",
                format!("unknown query \"{query_name}\": no named queries are registered"),
            ));
        };
        let mut params = HashMap::with_capacity(args.len());
        for (index, expr) in args.iter().enumerate() {
            params.insert(format!("${}", index + 1), literal_param(expr)?);
        }
        let bound = registry
            .bind(&query_name, &params)
            .map_err(|err| PgError::from_bind_error(&err))?;
        Ok((bound.sql, bound.graph))
    }

    /// Result columns for a lowered query: the compiled plan's output
    /// schema when the dataflow can run it, else the runtime's
    /// pass-through schema.
    fn result_schema(&self, sql: &str, graph: &MirGraph) -> Result<SchemaDefinition, PgError> {
        let rules = self.core.router.permission_rules();
        let rewritten = rewrite(graph, &rules, &self.user_ctx)
            .ok()
            .map(|outcome| outcome.graph);
        let input = rewritten.as_ref().unwrap_or(graph);
        let lookup = WalTableLookup {
            wal: self.core.wal.as_ref(),
        };
        if let Ok(plan) = compile_mir(input, &lookup) {
            return Ok(schema_definition_from_plan(&plan));
        }
        self.core
            .wal
            .query_schema(&QueryId::new(sql.to_owned()))
            .map_err(|err| PgError::new("42P01", err))
    }

    // ------------------------------------------------------------------
    // Extended query cycle
    // ------------------------------------------------------------------

    fn parse(&mut self, name: String, sql: &str, param_oids: Vec<u32>) -> Result<(), PgError> {
        let text = sql.trim().trim_end_matches(';').trim();
        let kind = if text.is_empty() {
            StatementKind::Empty
        } else {
            let statements = parse_statements(text)?;
            if statements.len() != 1 {
                return Err(PgError::syntax(
                    "cannot insert multiple commands into a prepared statement",
                ));
            }
            self.classify(&statements[0], text)?
        };
        let param_count = match &kind {
            StatementKind::Query { params, .. } => params.len(),
            _ => 0,
        };
        if param_oids.len() > param_count {
            return Err(PgError::new(
                "08P01",
                format!(
                    "statement declares {} parameter types but uses {param_count} parameters",
                    param_oids.len()
                ),
            ));
        }
        let mut param_oids = param_oids;
        param_oids.resize(param_count, 0);
        self.statements.insert(name, Prepared { kind, param_oids });
        Ok(())
    }

    fn bind(
        &self,
        statement: &str,
        param_formats: &[i16],
        params: &[Option<Vec<u8>>],
        result_formats: Vec<i16>,
    ) -> Result<Portal, PgError> {
        let prepared = self.statements.get(statement).ok_or_else(|| {
            PgError::new(
                "26000",
                format!("prepared statement \"{statement}\" does not exist"),
            )
        })?;
        let kind = match &prepared.kind {
            StatementKind::Empty => PortalKind::Empty,
            StatementKind::Utility(utility) => PortalKind::Utility(utility.clone()),
            StatementKind::Query {
                sql,
                registry,
                params: specs,
                graph,
                schema,
            } => {
                if specs.len() != params.len() {
                    return Err(PgError::new(
                        "08P01",
                        format!(
                            "bind message supplies {} parameters, but prepared statement \"{statement}\" requires {}",
                            params.len(),
                            specs.len()
                        ),
                    ));
                }
                let (sql, graph) = match registry {
                    Some(registry) => {
                        let mut values = HashMap::with_capacity(specs.len());
                        for (index, spec) in specs.iter().enumerate() {
                            let binary = format_at(param_formats, index) == 1;
                            let declared = prepared.param_oids.get(index).copied().unwrap_or(0);
                            let value = types::decode_param(
                                params[index].as_deref(),
                                binary,
                                declared,
                                spec,
                            )
                            .map_err(|message| {
                                PgError::new(
                                    "22023",
                                    format!("parameter ${}: {message}", index + 1),
                                )
                            })?;
                            values.insert(format!("${}", spec.position), value);
                        }
                        let bound = registry
                            .bind(STATEMENT_QUERY_NAME, &values)
                            .map_err(|err| PgError::from_bind_error(&err))?;
                        (bound.sql, bound.graph)
                    }
                    None => (
                        sql.clone(),
                        graph
                            .clone()
                            .ok_or_else(|| PgError::internal("statement carried no plan"))?,
                    ),
                };
                PortalKind::Query {
                    sql,
                    graph,
                    schema: schema.clone(),
                }
            }
        };
        Ok(Portal {
            kind,
            result_formats,
            rows: None,
            sent: 0,
        })
    }

    fn describe(&mut self, kind: u8, name: &str) -> Result<(), PgError> {
        match kind {
            b'S' => {
                let prepared = self.statements.get(name).ok_or_else(|| {
                    PgError::new(
                        "26000",
                        format!("prepared statement \"{name}\" does not exist"),
                    )
                })?;
                match &prepared.kind {
                    StatementKind::Query { params, schema, .. } => {
                        let oids: Vec<u32> = params
                            .iter()
                            .enumerate()
                            .map(|(index, spec)| match prepared.param_oids.get(index) {
                                Some(oid) if *oid != 0 => *oid,
                                _ => types::param_oid(spec),
                            })
                            .collect();
                        self.out.parameter_description(&oids);
                        self.out.row_description(&fields_for(schema, &[]));
                    }
                    StatementKind::Utility(Utility::Show { column, .. }) => {
                        self.out.parameter_description(&[]);
                        self.out.row_description(&[FieldDescription {
                            name: column.clone(),
                            type_oid: types::TEXT,
                            type_len: -1,
                            format: 0,
                        }]);
                    }
                    _ => {
                        self.out.parameter_description(&[]);
                        self.out.no_data();
                    }
                }
            }
            b'P' => {
                let portal = self.portals.get(name).ok_or_else(|| {
                    PgError::new("34000", format!("portal \"{name}\" does not exist"))
                })?;
                match &portal.kind {
                    PortalKind::Query { schema, .. } => {
                        let fields = fields_for(schema, &portal.result_formats);
                        self.out.row_description(&fields);
                    }
                    PortalKind::Utility(Utility::Show { column, .. }) => {
                        self.out.row_description(&[FieldDescription {
                            name: column.clone(),
                            type_oid: types::TEXT,
                            type_len: -1,
                            format: format_at(&portal.result_formats, 0),
                        }]);
                    }
                    _ => self.out.no_data(),
                }
            }
            other => {
                return Err(PgError::new(
                    "08P01",
                    format!("invalid DESCRIBE message subtype '{}'", char::from(other)),
                ));
            }
        }
        Ok(())
    }

    async fn execute(&mut self, name: &str, max_rows: i32) -> Result<(), PgError> {
        let Some(mut portal) = self.portals.remove(name) else {
            return Err(PgError::new(
                "34000",
                format!("portal \"{name}\" does not exist"),
            ));
        };
        let outcome = self.execute_portal(&mut portal, max_rows).await;
        self.portals.insert(name.to_owned(), portal);
        outcome
    }

    async fn execute_portal(&mut self, portal: &mut Portal, max_rows: i32) -> Result<(), PgError> {
        if self.failed_transaction
            && !matches!(
                portal.kind,
                PortalKind::Utility(Utility::Commit | Utility::Rollback)
            )
        {
            return Err(PgError::new(
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        match &portal.kind {
            PortalKind::Empty => self.out.empty_query_response(),
            PortalKind::Utility(utility) => {
                let utility = utility.clone();
                self.run_utility(&utility);
            }
            PortalKind::Query { sql, graph, schema } => {
                if portal.rows.is_none() {
                    let result = self.execute_query(sql, graph).await?;
                    portal.rows = Some(result.rows.into());
                }
                let limit = usize::try_from(max_rows)
                    .ok()
                    .filter(|limit| *limit > 0)
                    .unwrap_or(usize::MAX);
                let rows = portal.rows.as_mut().expect("rows populated above");
                let mut sent = 0;
                while sent < limit {
                    let Some(row) = rows.pop_front() else {
                        break;
                    };
                    write_data_row(&mut self.out, &row, schema, &portal.result_formats);
                    sent += 1;
                }
                portal.sent += sent;
                if rows.is_empty() {
                    self.out
                        .command_complete(&format!("SELECT {}", portal.sent));
                } else {
                    self.out.portal_suspended();
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Query execution against the live view
    // ------------------------------------------------------------------

    /// Answers `sql` from its materialized view. Hot path: the
    /// connection already anchors this query, so the host's cached
    /// view is returned directly. Cold path: subscribe like a gRPC
    /// client would, take the `Initial` snapshot as the result, and
    /// keep the subscription as the anchor.
    async fn execute_query(&mut self, sql: &str, graph: &MirGraph) -> Result<ResultSet, PgError> {
        if let Some(hit) = self.cached(sql) {
            return Ok(hit);
        }

        let permit = Arc::clone(&self.core.subscribe_blocking_slots)
            .acquire_owned()
            .await
            .map_err(|_| PgError::new("57P01", "server shutting down"))?;
        let client_id = ClientSubscriptionId::new(format!("pgwire-{}", self.next_client_id));
        self.next_client_id += 1;
        let outcome = {
            let core = self.core.clone();
            let connection = self.connection;
            let query = QueryId::new(sql.to_owned());
            let graph = graph.clone();
            let user_ctx = self.user_ctx.clone();
            tokio::task::spawn_blocking(move || {
                blocking_subscribe(
                    connection,
                    client_id,
                    query,
                    graph,
                    user_ctx,
                    None,
                    core.router.as_ref(),
                    core.wal.as_ref(),
                    core.dataflow_host.as_ref(),
                    core.schema_allocator.as_ref(),
                )
            })
            .await
            .map_err(|err| PgError::internal(format!("subscribe task failed: {err}")))?
        };
        drop(permit);
        let SubscribeOutcome {
            mut response,
            schema,
            compiled_plan_for_host,
            host_canonical,
        } = outcome.map_err(|err: SubscribeBlockingError| {
            let (code, message) = err.code_and_message();
            PgError::from_engine(code, message)
        })?;

        let subscription = response.subscription_id;
        let rows = match response.stream.next().await {
            Some(DiffEvent::Initial { rows, .. }) => rows,
            other => {
                let _ = self.core.router.unsubscribe(subscription);
                if let Some(canonical) = host_canonical.as_ref() {
                    self.core
                        .dataflow_host
                        .release(canonical, subscription.get());
                }
                return Err(PgError::internal(format!(
                    "subscription did not produce an initial snapshot (got {})",
                    other.map_or_else(
                        || "end of stream".to_owned(),
                        |event| { format!("{event:?}").chars().take(40).collect() }
                    )
                )));
            }
        };

        match (host_canonical, compiled_plan_for_host) {
            (Some(canonical), Some(plan)) => {
                ensure_canonical_pump(
                    &self.core.pump_registry,
                    canonical.clone(),
                    QueryId::new(sql.to_owned()),
                    response.snapshot_lsn,
                    schema.primary_key_columns.clone(),
                    plan,
                    Arc::clone(&self.core.router),
                    Arc::clone(&self.core.wal),
                    Arc::clone(&self.core.dataflow_host),
                );
                self.remember_anchor(
                    sql,
                    subscription,
                    canonical,
                    schema.clone(),
                    response.stream,
                );
            }
            _ => {
                // Pass-through query the dataflow could not host: no
                // shared view to keep warm, so release it right away.
                let _ = self.core.router.unsubscribe(subscription);
            }
        }
        Ok(ResultSet { schema, rows })
    }

    /// The anchored view for `sql`, if this connection holds one and
    /// the host still has the plan.
    fn cached(&mut self, sql: &str) -> Option<ResultSet> {
        let stale = {
            let anchors = self.anchors.lock().expect("anchor lock");
            let anchor = anchors.get(sql)?;
            let probe = self.core.router.allocate_subscription_id();
            match self
                .core
                .dataflow_host
                .cached_view(&anchor.canonical, probe.get())
            {
                Some((rows, _lsn)) => {
                    self.core
                        .dataflow_host
                        .release(&anchor.canonical, probe.get());
                    return Some(ResultSet {
                        schema: anchor.schema.clone(),
                        rows,
                    });
                }
                None => anchor.subscription,
            }
        };
        // The plan is gone (its pump exited); drop the stale anchor so
        // the next execution re-subscribes.
        self.forget_anchor(sql, stale);
        None
    }

    fn remember_anchor(
        &mut self,
        sql: &str,
        subscription: SubscriptionId,
        canonical: String,
        schema: SchemaDefinition,
        stream: tokio_stream::wrappers::ReceiverStream<DiffEvent>,
    ) {
        let cap = self.core.security.max_subscriptions_per_connection.max(1);
        while self.anchor_order.len() >= cap {
            let Some(oldest) = self.anchor_order.pop_front() else {
                break;
            };
            let removed = self.anchors.lock().expect("anchor lock").remove(&oldest);
            if let Some(anchor) = removed {
                anchor.drain.abort();
                let _ = self.core.router.unsubscribe(anchor.subscription);
                self.core
                    .dataflow_host
                    .release(&anchor.canonical, anchor.subscription.get());
            }
        }
        let drain = tokio::spawn(drain_anchor(
            stream,
            Arc::clone(&self.anchors),
            sql.to_owned(),
            subscription,
            canonical.clone(),
            self.core.clone(),
        ));
        self.anchors.lock().expect("anchor lock").insert(
            sql.to_owned(),
            Anchor {
                subscription,
                canonical,
                schema,
                drain,
            },
        );
        self.anchor_order.push_back(sql.to_owned());
    }

    fn forget_anchor(&mut self, sql: &str, subscription: SubscriptionId) {
        let removed = {
            let mut anchors = self.anchors.lock().expect("anchor lock");
            match anchors.get(sql) {
                Some(anchor) if anchor.subscription == subscription => anchors.remove(sql),
                _ => None,
            }
        };
        if let Some(anchor) = removed {
            anchor.drain.abort();
            let _ = self.core.router.unsubscribe(anchor.subscription);
            self.core
                .dataflow_host
                .release(&anchor.canonical, anchor.subscription.get());
        }
        self.anchor_order.retain(|key| key != sql);
    }
}

/// Consumes an anchor's diff stream so its channel never saturates.
/// A `Resync` (permission rules swapped, slot recreated, ...) means
/// the cached view can no longer be trusted: the anchor is dropped and
/// the next execution subscribes afresh.
/// Collects the relation names a statement references, and the CTE
/// names it defines so they can be told apart from base tables.
#[derive(Default)]
struct RelationNames {
    tables: Vec<String>,
    ctes: Vec<String>,
}

impl Visitor for RelationNames {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<()> {
        if let Some(with) = &query.with {
            self.ctes.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.clone()),
            );
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> std::ops::ControlFlow<()> {
        if let Some(last) = relation.0.last() {
            if !self.tables.contains(&last.value) {
                self.tables.push(last.value.clone());
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

async fn drain_anchor(
    mut stream: tokio_stream::wrappers::ReceiverStream<DiffEvent>,
    anchors: Anchors,
    sql: String,
    subscription: SubscriptionId,
    canonical: String,
    core: QueryCore,
) {
    while let Some(event) = stream.next().await {
        if let DiffEvent::Resync { reason } = event {
            debug!(?reason, "pgwire: anchored view resynced; dropping anchor");
            break;
        }
    }
    let removed = {
        let mut anchors = anchors.lock().expect("anchor lock");
        match anchors.get(&sql) {
            Some(anchor) if anchor.subscription == subscription => anchors.remove(&sql),
            _ => None,
        }
    };
    if removed.is_some() {
        let _ = core.router.unsubscribe(subscription);
        core.dataflow_host.release(&canonical, subscription.get());
    }
}

fn parse_statements(sql: &str) -> Result<Vec<Statement>, PgError> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|err| PgError::syntax(err.to_string()))
}

fn has_placeholders(statement: &Statement) -> bool {
    let mut found = false;
    let _ = visit_expressions(statement, |expr| {
        if matches!(expr, Expr::Value(Value::Placeholder(_))) {
            found = true;
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

/// Converts an `EXECUTE` argument literal into a parameter value.
fn literal_param(expr: &Expr) -> Result<ParamValue, PgError> {
    match expr {
        Expr::Value(Value::Number(text, _)) => Ok(text
            .parse::<i64>()
            .map(ParamValue::Int)
            .or_else(|_| text.parse::<f64>().map(ParamValue::Float))
            .map_err(|_| PgError::new("22023", format!("invalid numeric literal '{text}'")))?),
        Expr::Value(
            Value::SingleQuotedString(text)
            | Value::EscapedStringLiteral(text)
            | Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value: text, .. }),
        ) => Ok(ParamValue::Text(text.clone())),
        Expr::Value(Value::Boolean(flag)) => Ok(ParamValue::Bool(*flag)),
        Expr::Value(Value::Null) => Ok(ParamValue::Null),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match literal_param(expr)? {
            ParamValue::Int(value) => Ok(ParamValue::Int(-value)),
            ParamValue::Float(value) => Ok(ParamValue::Float(-value)),
            _ => Err(PgError::new("22023", "unary minus applies to numbers only")),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr) => literal_param(expr),
        Expr::Array(array) => array
            .elem
            .iter()
            .map(literal_param)
            .collect::<Result<Vec<_>, _>>()
            .map(ParamValue::List),
        other => Err(PgError::new(
            "22023",
            format!("unsupported EXECUTE argument '{other}': pass literals"),
        )),
    }
}

/// Placeholder value of the right shape for learning a statement's
/// result columns before real parameters arrive.
fn dummy_value(spec: &ParamSpec) -> ParamValue {
    use palimpsest_sql::ColumnType;
    let scalar = match spec.ty {
        ColumnType::Bool => ParamValue::Bool(true),
        ColumnType::Int => ParamValue::Int(0),
        ColumnType::Float => ParamValue::Float(0.0),
        ColumnType::Uuid => ParamValue::Text("00000000-0000-0000-0000-000000000000".to_owned()),
        ColumnType::Timestamp => ParamValue::Text("2000-01-01 00:00:00".to_owned()),
        ColumnType::TimestampTz => ParamValue::Text("2000-01-01 00:00:00+00".to_owned()),
        ColumnType::Date => ParamValue::Text("2000-01-01".to_owned()),
        ColumnType::Time => ParamValue::Text("00:00:00".to_owned()),
        ColumnType::Interval => ParamValue::Text("0 seconds".to_owned()),
        ColumnType::Numeric => ParamValue::Text("0".to_owned()),
        ColumnType::Jsonb => ParamValue::Text("{}".to_owned()),
        _ => ParamValue::Text(String::new()),
    };
    if spec.list {
        ParamValue::List(vec![scalar])
    } else {
        scalar
    }
}

/// Postgres' format-code expansion: none = all text, one = all that.
fn format_at(formats: &[i16], index: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(index).copied().unwrap_or(0),
    }
}

fn fields_for(schema: &SchemaDefinition, formats: &[i16]) -> Vec<FieldDescription> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let (type_oid, type_len) = types::wire_type(&column.datum_type);
            FieldDescription {
                name: column.name.clone(),
                type_oid,
                type_len,
                format: format_at(formats, index),
            }
        })
        .collect()
}

fn write_data_row(out: &mut Outbox, row: &Row, schema: &SchemaDefinition, formats: &[i16]) {
    let encoded: Vec<Option<Vec<u8>>> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let datum = row.get(index).unwrap_or(&Datum::Null);
            types::encode_column(datum, &column.datum_type, format_at(formats, index) == 1)
        })
        .collect();
    out.data_row(encoded.iter().map(|column| column.as_deref()));
}

/// Values for the `SHOW` parameters drivers and pools probe.
fn show_value(name: &str, user_name: &str) -> Option<String> {
    Some(
        match name.to_ascii_lowercase().as_str() {
            "server_version" => SERVER_VERSION,
            "server_version_num" => "160000",
            "server_encoding" | "client_encoding" => "UTF8",
            "datestyle" => "ISO, MDY",
            "intervalstyle" => "postgres",
            "timezone" | "time zone" => "UTC",
            "integer_datetimes" | "standard_conforming_strings" => "on",
            "transaction_isolation" | "default_transaction_isolation" => "read committed",
            "transaction_read_only" | "default_transaction_read_only" => "on",
            "is_superuser" => "off",
            "search_path" => "public",
            "max_identifier_length" => "63",
            "application_name" => "",
            "session_authorization" | "role" => user_name,
            _ => return None,
        }
        .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_arguments_map_to_parameter_values() {
        let statements = parse_statements(
            "EXECUTE board_tickets(42, -1.5, 'open', true, NULL, ARRAY[1, 2], '7'::int)",
        )
        .unwrap();
        let Statement::Execute { parameters, .. } = &statements[0] else {
            panic!("expected EXECUTE");
        };
        let values: Vec<ParamValue> = parameters
            .iter()
            .map(|p| literal_param(p).unwrap())
            .collect();
        assert_eq!(
            values,
            vec![
                ParamValue::Int(42),
                ParamValue::Float(-1.5),
                ParamValue::Text("open".to_owned()),
                ParamValue::Bool(true),
                ParamValue::Null,
                ParamValue::List(vec![ParamValue::Int(1), ParamValue::Int(2)]),
                ParamValue::Text("7".to_owned()),
            ]
        );
        let bad = parse_statements("EXECUTE q(now())").unwrap();
        let Statement::Execute { parameters, .. } = &bad[0] else {
            panic!("expected EXECUTE");
        };
        assert_eq!(literal_param(&parameters[0]).unwrap_err().code, "22023");
    }

    #[test]
    fn placeholder_detection_and_format_expansion() {
        let with = parse_statements("SELECT id FROM posts WHERE id = $1").unwrap();
        assert!(has_placeholders(&with[0]));
        let without = parse_statements("SELECT id FROM posts WHERE id = 1").unwrap();
        assert!(!has_placeholders(&without[0]));
        assert_eq!(format_at(&[], 3), 0);
        assert_eq!(format_at(&[1], 3), 1);
        assert_eq!(format_at(&[0, 1], 1), 1);
        assert_eq!(format_at(&[0, 1], 5), 0);
    }

    #[test]
    fn sql_errors_map_to_sqlstates() {
        let err =
            parse_and_lower_with_limits("SELEC id FROM posts", QueryLimits::DEFAULT).unwrap_err();
        assert_eq!(PgError::from_sql_error(&err).code, "42601");
        assert_eq!(
            show_value("TRANSACTION_ISOLATION", "u").as_deref(),
            Some("read committed")
        );
        assert!(show_value("nonsense", "u").is_none());
    }
}
