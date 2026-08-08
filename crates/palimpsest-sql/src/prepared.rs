// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Named prepared queries (server-registered; clients refer by name).
//!
//! A [`QueryRegistry`] holds SQL templates registered at server start —
//! either programmatically ([`QueryRegistry::register`]) or from an
//! sqlc-format query file ([`QueryRegistry::register_sqlc_source`]).
//! Clients subscribe with `Subscribe{name, params}` and never hold or
//! send SQL; the registry [`bind`](QueryRegistry::bind)s the wire
//! params into the stored template and hands the caller the rendered
//! SQL text plus the lowered [`MirGraph`].
//!
//! Design constraints, in order:
//!
//! * **Registration failure is loud and early.** Every template is
//!   fully validated at registration time by binding representative
//!   values of each parameter's type and running the same
//!   parse → validate → lower pipeline a live subscribe would. A
//!   template the engine cannot execute is rejected *here*, with the
//!   exact unsupported construct named — never at subscribe time.
//! * **Fail closed.** Binding an unknown name, a missing parameter, an
//!   ill-typed value, or an unexpected extra parameter is an error.
//! * **sqlc compatibility.** The sqlc query-file annotation format
//!   (`-- name: GetThing :one` / `:many`, positional `$N` parameters,
//!   `sqlc.arg(name)` / `sqlc.narg(name)` named parameters) is the
//!   registration format. Parameter types are inferred from the
//!   [`Catalog`] the same way sqlc infers them from the database
//!   schema, so an sqlc query file registers as written or is rejected
//!   with a named reason.
//!
//! Parameters are bound by substituting **typed literal AST nodes**
//! into a clone of the parsed template — never by splicing client text
//! into SQL. String values become `Value::SingleQuotedString` nodes
//! whose rendering escapes embedded quotes, so a parameter value can
//! never alter the query shape.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use core::ops::ControlFlow;

use sqlparser::ast::{
    Array, BinaryOperator, CastKind, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, ObjectName, Query, Statement, TableFactor, Value, Visit, Visitor,
};
use thiserror::Error;

use crate::{
    catalog::{Catalog, ColumnType},
    limits::enforce_graph_size,
    lower::lower_select_statement,
    mir::MirGraph,
    parser::validate_query,
    QueryLimits, SqlError,
};

/// Longest accepted text/uuid/timestamp parameter value, in bytes.
/// Bounds the rendered SQL (which doubles as the canonical query key)
/// against runaway client input.
pub const MAX_TEXT_PARAM_BYTES: usize = 16 * 1024;

/// Longest accepted list parameter.
pub const MAX_LIST_PARAM_LEN: usize = 1024;

/// sqlc result-cardinality annotation. Both register identically; the
/// engine streams diffs either way, `One` is a client-side hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedCardinality {
    /// `-- name: X :one`
    One,
    /// `-- name: X :many`
    Many,
}

/// Declared (or inferred) shape of one parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamSpec {
    /// 1-based position (`$1` → 1). Named-only parameters
    /// (`sqlc.arg(x)`) are assigned positions in first-appearance
    /// order.
    pub position: usize,
    /// Wire name clients bind by. Inferred sqlc-style from the column
    /// a positional parameter is compared against (falling back to
    /// `argN`), or taken verbatim from `sqlc.arg(name)`.
    pub name: String,
    /// Element type (the list element type when `list` is set).
    pub ty: ColumnType,
    /// True for parameters used as `= ANY($n)` — bound from a list
    /// value and rendered as an `ARRAY[...]` literal.
    pub list: bool,
    /// True for `sqlc.narg(...)` parameters; only these accept null.
    pub nullable: bool,
}

/// Explicit parameter declaration for [`QueryRegistry::register`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamDecl {
    /// Wire name clients bind by.
    pub name: String,
    /// Element type (the list element type when `list` is set).
    pub ty: ColumnType,
    /// Bound from a list value; rendered as `ARRAY[...]`.
    pub list: bool,
    /// Whether null is an accepted value.
    pub nullable: bool,
}

impl ParamDecl {
    /// Scalar, non-nullable parameter.
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
            list: false,
            nullable: false,
        }
    }

    /// Marks the parameter as a list (`= ANY($n)`).
    #[must_use]
    pub const fn list(mut self) -> Self {
        self.list = true;
        self
    }

    /// Marks the parameter as nullable.
    #[must_use]
    pub const fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }
}

/// A typed parameter value supplied at bind time.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue {
    /// Boolean.
    Bool(bool),
    /// Signed integer.
    Int(i64),
    /// Floating-point.
    Float(f64),
    /// Text (also carries uuid / timestamp / enum / jsonb values).
    Text(String),
    /// Explicit null; accepted only by nullable parameters.
    Null,
    /// List of scalars, for `= ANY($n)` parameters.
    List(Vec<Self>),
}

impl ParamValue {
    const fn kind_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Text(_) => "text",
            Self::Null => "null",
            Self::List(_) => "list",
        }
    }
}

/// One registered query.
#[derive(Debug, Clone)]
pub struct PreparedQuery {
    /// Registered name (the wire `query_name`).
    pub name: String,
    /// sqlc cardinality annotation.
    pub cardinality: PreparedCardinality,
    /// Template SQL as registered (placeholders intact).
    pub sql: String,
    /// Parameters in positional order.
    pub params: Vec<ParamSpec>,
    /// Parsed template.
    statement: Statement,
}

/// Result of binding params into a registered query.
#[derive(Debug, Clone)]
pub struct BoundQuery {
    /// Rendered SQL with every placeholder replaced by a typed
    /// literal. This is the engine-facing query text: it feeds the
    /// `QueryId` / canonical subgraph key, so two subscribers binding
    /// the same name + params share one dataflow plan.
    pub sql: String,
    /// Lowered MIR for the bound query.
    pub graph: MirGraph,
}

/// Registration-time failure. Every variant names the query and the
/// exact reason so a bad template is diagnosable from the error alone.
#[derive(Debug, Error)]
pub enum RegisterError {
    /// Query name registered twice.
    #[error("query '{0}' is already registered")]
    Duplicate(String),

    /// The sqlc source had SQL before any `-- name:` header.
    #[error("{origin}: SQL before the first '-- name:' header (line {line})")]
    SqlBeforeHeader {
        /// Source label (file path).
        origin: String,
        /// 1-based line number.
        line: usize,
    },

    /// A `-- name:` header that doesn't parse as `name :verb`.
    #[error("{origin}: malformed query header '{header}' (line {line}); expected '-- name: Name :one|:many'")]
    MalformedHeader {
        /// Source label (file path).
        origin: String,
        /// The offending header text.
        header: String,
        /// 1-based line number.
        line: usize,
    },

    /// sqlc verb other than `:one` / `:many`. Mutation verbs have no
    /// meaning for a live subscription engine.
    #[error("query '{query}': sqlc verb '{verb}' is not supported — only :one and :many queries can be registered for live subscription")]
    UnsupportedVerb {
        /// Query name from the header.
        query: String,
        /// The rejected verb (with leading `:`).
        verb: String,
    },

    /// A header with no SQL under it.
    #[error("query '{0}': header has no SQL statement")]
    EmptyQuery(String),

    /// The template failed the engine's parse / validation / lowering
    /// pipeline. The source error names the exact unsupported
    /// construct (e.g. `unsupported SQL feature: RIGHT JOIN`).
    #[error("query '{query}': {source}")]
    Unsupported {
        /// Query name.
        query: String,
        /// The named construct the engine rejected.
        source: SqlError,
    },

    /// Placeholder form the registrar doesn't understand (e.g. `?`).
    #[error("query '{query}': unsupported placeholder '{placeholder}' — use $1..$N, sqlc.arg(name), or sqlc.narg(name)")]
    UnsupportedPlaceholder {
        /// Query name.
        query: String,
        /// Placeholder text as written.
        placeholder: String,
    },

    /// `$1..$N` must be contiguous from 1.
    #[error("query '{query}': positional parameters must be contiguous from $1 — ${missing} is never used but ${max} is")]
    NonContiguousParams {
        /// Query name.
        query: String,
        /// First missing position.
        missing: usize,
        /// Highest used position.
        max: usize,
    },

    /// The registrar couldn't infer a parameter's type from any of its
    /// use sites.
    #[error("query '{query}': cannot infer the type of {param}: {reason}; annotate the parameter with a cast (e.g. {param}::uuid) or register the query programmatically with an explicit parameter schema")]
    ParamTypeUnknown {
        /// Query name.
        query: String,
        /// Parameter as written (`$1` or `sqlc.arg(x)`).
        param: String,
        /// Why inference failed.
        reason: String,
    },

    /// Two use sites inferred conflicting types for one parameter.
    #[error("query '{query}': conflicting types inferred for {param}: {first} vs {second}")]
    ParamTypeConflict {
        /// Query name.
        query: String,
        /// Parameter as written.
        param: String,
        /// First inferred type.
        first: &'static str,
        /// Conflicting inferred type.
        second: &'static str,
    },

    /// One use site treats the parameter as a scalar and another as a
    /// list.
    #[error("query '{query}': {param} is used both as a scalar and as an ANY(...) list")]
    ParamListConflict {
        /// Query name.
        query: String,
        /// Parameter as written.
        param: String,
    },

    /// Explicit registration declared a different parameter count than
    /// the template uses.
    #[error("query '{query}': template uses {used} parameter(s) but {declared} were declared")]
    ParamCountMismatch {
        /// Query name.
        query: String,
        /// Placeholders found in the template.
        used: usize,
        /// Declarations supplied by the caller.
        declared: usize,
    },

    /// Explicit declaration disagrees with an inferred use site.
    #[error("query '{query}': parameter '{param}' is declared as {declared} but used as {used}")]
    ParamDeclMismatch {
        /// Query name.
        query: String,
        /// Declared parameter name.
        param: String,
        /// Declared shape.
        declared: String,
        /// Shape implied by the template.
        used: String,
    },

    /// Two parameters resolved to the same wire name.
    #[error(
        "query '{query}': parameters {first} and {second} both resolve to the wire name '{name}'"
    )]
    DuplicateParamName {
        /// Query name.
        query: String,
        /// First parameter (`$N` form).
        first: String,
        /// Second parameter (`$N` form).
        second: String,
        /// The colliding wire name.
        name: String,
    },
}

/// Bind-time failure. All variants are client errors; the registry
/// fails closed on any of them.
#[derive(Debug, Error)]
pub enum BindError {
    /// No query registered under this name. The subscribe must be
    /// refused — this is the fail-closed core of the registry.
    #[error("unknown query '{0}'")]
    UnknownQuery(String),

    /// A declared parameter was not supplied.
    #[error("query '{query}': missing parameter '{param}'")]
    MissingParam {
        /// Query name.
        query: String,
        /// Wire name of the missing parameter.
        param: String,
    },

    /// A supplied key matches no declared parameter.
    #[error("query '{query}': unexpected parameter '{param}'")]
    UnexpectedParam {
        /// Query name.
        query: String,
        /// The unrecognized key.
        param: String,
    },

    /// Value kind does not match the declared type.
    #[error("query '{query}': parameter '{param}' expects {expected}, got {got}")]
    TypeMismatch {
        /// Query name.
        query: String,
        /// Wire name.
        param: String,
        /// Human-readable expected shape.
        expected: String,
        /// Supplied value kind.
        got: &'static str,
    },

    /// Value has the right kind but an invalid form (bad uuid, digits
    /// that don't fit i64, non-finite float, oversized text/list).
    #[error("query '{query}': parameter '{param}': {reason}")]
    InvalidValue {
        /// Query name.
        query: String,
        /// Wire name.
        param: String,
        /// What was wrong.
        reason: String,
    },

    /// Null supplied for a non-nullable parameter.
    #[error("query '{query}': parameter '{param}' is not nullable")]
    NullNotAllowed {
        /// Query name.
        query: String,
        /// Wire name.
        param: String,
    },

    /// The bound query failed validation or lowering. Unreachable for
    /// values that respect the declared schema (registration already
    /// validated the template), but kept as defense in depth.
    #[error("query '{query}': {source}")]
    Rejected {
        /// Query name.
        query: String,
        /// Underlying engine error.
        source: SqlError,
    },
}

const fn column_type_name(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Bool => "bool",
        ColumnType::Int => "int",
        ColumnType::Float => "float",
        ColumnType::Text => "text",
        ColumnType::Timestamp => "timestamp",
        ColumnType::Uuid => "uuid",
        ColumnType::Jsonb => "jsonb",
        ColumnType::Enum => "enum",
        ColumnType::Unknown => "unknown",
    }
}

fn param_shape(ty: ColumnType, list: bool) -> String {
    if list {
        format!("list of {}", column_type_name(ty))
    } else {
        column_type_name(ty).to_owned()
    }
}

/// Registry of named prepared queries.
///
/// Build one at startup, register every query (loudly failing the
/// process on error), then share it read-only with the subscribe path.
#[derive(Debug, Default, Clone)]
pub struct QueryRegistry {
    queries: BTreeMap<String, PreparedQuery>,
    limits: QueryLimits,
}

impl QueryRegistry {
    /// Empty registry under [`QueryLimits::DEFAULT`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Empty registry with explicit limits (applied to templates at
    /// registration and to bound queries at subscribe time).
    #[must_use]
    pub const fn with_limits(limits: QueryLimits) -> Self {
        Self {
            queries: BTreeMap::new(),
            limits,
        }
    }

    /// Number of registered queries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// True when nothing is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    /// Looks a registered query up by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&PreparedQuery> {
        self.queries.get(name)
    }

    /// Iterates registered queries in name order.
    pub fn queries(&self) -> impl Iterator<Item = &PreparedQuery> {
        self.queries.values()
    }

    /// Registers one query with an explicit parameter schema
    /// (`RegisterQuery(name, sql, param schema)`).
    ///
    /// `params` is positional: `params[0]` describes `$1`, and so on.
    /// For `sqlc.arg(x)` templates the declaration order must match
    /// first-appearance order and names must match.
    ///
    /// # Errors
    /// Any [`RegisterError`]; in particular
    /// [`RegisterError::Unsupported`] names the exact construct the
    /// engine rejected.
    pub fn register(
        &mut self,
        name: &str,
        sql: &str,
        params: &[ParamDecl],
    ) -> Result<&PreparedQuery, RegisterError> {
        self.register_full(name, sql, PreparedCardinality::Many, Some(params), None)
    }

    /// Registers every query in an sqlc-format source string.
    ///
    /// `origin` labels the source in error messages (typically the
    /// file path). Parameter types are inferred from `catalog`
    /// sqlc-style: from the column a parameter is compared against, or
    /// from an explicit cast (`$1::uuid`).
    ///
    /// Registration is all-or-nothing per source: the first failing
    /// query aborts with no partial registration from this call.
    ///
    /// # Errors
    /// Any [`RegisterError`], naming the query and exact reason.
    pub fn register_sqlc_source(
        &mut self,
        source: &str,
        origin: &str,
        catalog: &Catalog,
    ) -> Result<Vec<String>, RegisterError> {
        let blocks = split_sqlc_source(source, origin)?;
        // Stage into a scratch registry so a failure mid-file leaves
        // `self` untouched.
        let mut staged = Self::with_limits(self.limits);
        staged.queries.clone_from(&self.queries);
        let mut registered = Vec::with_capacity(blocks.len());
        for block in blocks {
            staged.register_full(
                &block.name,
                &block.sql,
                block.cardinality,
                None,
                Some(catalog),
            )?;
            registered.push(block.name);
        }
        self.queries = staged.queries;
        Ok(registered)
    }

    /// Shared implementation behind both registration paths.
    fn register_full(
        &mut self,
        name: &str,
        sql: &str,
        cardinality: PreparedCardinality,
        decls: Option<&[ParamDecl]>,
        catalog: Option<&Catalog>,
    ) -> Result<&PreparedQuery, RegisterError> {
        if self.queries.contains_key(name) {
            return Err(RegisterError::Duplicate(name.to_owned()));
        }

        let mut statement =
            crate::parser::parse_single_query(sql, self.limits).map_err(|source| {
                RegisterError::Unsupported {
                    query: name.to_owned(),
                    source,
                }
            })?;

        // Canonicalize `sqlc.arg(x)` / `sqlc.narg(x)` into placeholder
        // values so the rest of the pipeline sees one placeholder form.
        let named_args = canonicalize_sqlc_args(name, &mut statement)?;

        let occurrences = collect_placeholders(name, &statement)?;
        let params = resolve_params(name, &statement, &occurrences, &named_args, decls, catalog)?;

        let prepared = PreparedQuery {
            name: name.to_owned(),
            cardinality,
            sql: sql.to_owned(),
            params,
            statement,
        };

        // Registration-time validation: bind representative values of
        // each declared type and run the full validate + lower + size
        // pipeline. This is the "loud at registration, never at
        // subscribe" guarantee.
        let dummies: HashMap<String, ParamValue> = prepared
            .params
            .iter()
            .map(|spec| (spec.name.clone(), dummy_value(spec)))
            .collect();
        bind_prepared(&prepared, &dummies, self.limits).map_err(|err| match err {
            BindError::Rejected { query, source } => RegisterError::Unsupported { query, source },
            other => RegisterError::Unsupported {
                query: name.to_owned(),
                source: SqlError::InvalidQuery(other.to_string()),
            },
        })?;

        Ok(self.queries.entry(name.to_owned()).or_insert(prepared))
    }

    /// Binds `params` into the query registered as `name`.
    ///
    /// Parameters are keyed by wire name (e.g. `board_id`) or by
    /// position (`$1`). Every declared parameter must be supplied;
    /// unknown keys are refused.
    ///
    /// # Errors
    /// [`BindError::UnknownQuery`] for unregistered names (fail
    /// closed), otherwise the specific parameter error.
    pub fn bind(
        &self,
        name: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<BoundQuery, BindError> {
        let prepared = self
            .queries
            .get(name)
            .ok_or_else(|| BindError::UnknownQuery(name.to_owned()))?;
        bind_prepared(prepared, params, self.limits)
    }
}

// ---------------------------------------------------------------------
// sqlc source splitting
// ---------------------------------------------------------------------

struct SqlcBlock {
    name: String,
    cardinality: PreparedCardinality,
    sql: String,
}

/// Splits an sqlc query file into `(name, verb, sql)` blocks.
fn split_sqlc_source(source: &str, origin: &str) -> Result<Vec<SqlcBlock>, RegisterError> {
    let mut blocks: Vec<SqlcBlock> = Vec::new();
    let mut current: Option<SqlcBlock> = None;

    for (index, line) in source.lines().enumerate() {
        let line_no = index + 1;
        let trimmed = line.trim();
        let comment_body = trimmed.strip_prefix("--").map(str::trim);

        if let Some(body) = comment_body {
            if let Some(header) = body.strip_prefix("name:") {
                if let Some(block) = current.take() {
                    push_block(&mut blocks, block)?;
                }
                current = Some(parse_sqlc_header(header, origin, line_no)?);
                continue;
            }
            // Non-header comment: keep inside the current block (the
            // SQL parser skips it), ignore before the first header.
            if let Some(block) = current.as_mut() {
                block.sql.push_str(line);
                block.sql.push('\n');
            }
            continue;
        }

        if trimmed.is_empty() {
            if let Some(block) = current.as_mut() {
                block.sql.push('\n');
            }
            continue;
        }

        match current.as_mut() {
            Some(block) => {
                block.sql.push_str(line);
                block.sql.push('\n');
            }
            None => {
                return Err(RegisterError::SqlBeforeHeader {
                    origin: origin.to_owned(),
                    line: line_no,
                })
            }
        }
    }
    if let Some(block) = current.take() {
        push_block(&mut blocks, block)?;
    }
    Ok(blocks)
}

fn push_block(blocks: &mut Vec<SqlcBlock>, block: SqlcBlock) -> Result<(), RegisterError> {
    if block.sql.trim().is_empty() {
        return Err(RegisterError::EmptyQuery(block.name));
    }
    blocks.push(block);
    Ok(())
}

fn parse_sqlc_header(header: &str, origin: &str, line: usize) -> Result<SqlcBlock, RegisterError> {
    let malformed = || RegisterError::MalformedHeader {
        origin: origin.to_owned(),
        header: format!("-- name:{header}"),
        line,
    };
    let mut parts = header.split_whitespace();
    let name = parts.next().ok_or_else(malformed)?;
    let verb = parts.next().ok_or_else(malformed)?;
    if parts.next().is_some() || !verb.starts_with(':') || name.is_empty() {
        return Err(malformed());
    }
    let cardinality = match verb {
        ":one" => PreparedCardinality::One,
        ":many" => PreparedCardinality::Many,
        other => {
            return Err(RegisterError::UnsupportedVerb {
                query: name.to_owned(),
                verb: other.to_owned(),
            })
        }
    };
    Ok(SqlcBlock {
        name: name.to_owned(),
        cardinality,
        sql: String::new(),
    })
}

// ---------------------------------------------------------------------
// Placeholder canonicalization + collection
// ---------------------------------------------------------------------

/// Recognized name for the `sqlc.arg` family. Returns the declared
/// param name and nullability when `func` is one of them.
fn sqlc_arg_call(func: &Function) -> Option<(String, bool)> {
    let ObjectName(parts) = &func.name;
    if parts.len() != 2 || !parts[0].value.eq_ignore_ascii_case("sqlc") {
        return None;
    }
    let nullable = match parts[1].value.to_ascii_lowercase().as_str() {
        "arg" => false,
        "narg" => true,
        _ => return None,
    };
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    if list.args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) = &list.args[0] else {
        return None;
    };
    let name = match arg {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
        _ => return None,
    };
    Some((name, nullable))
}

#[derive(Debug, Clone)]
struct NamedArg {
    name: String,
    nullable: bool,
}

/// Rewrites `sqlc.arg(x)` / `sqlc.narg(x)` calls into synthetic
/// placeholders `$N` (continuing after the highest positional
/// placeholder already present), returning the name/nullability for
/// each synthesized position.
fn canonicalize_sqlc_args(
    query: &str,
    statement: &mut Statement,
) -> Result<HashMap<usize, NamedArg>, RegisterError> {
    // First pass: find the highest existing $N so synthesized
    // positions don't collide.
    let mut max_position = 0_usize;
    let _ = visit_placeholders(statement, |text| {
        if let Some(position) = parse_dollar_position(text) {
            max_position = max_position.max(position);
        }
        ControlFlow::<()>::Continue(())
    });

    let mut named: HashMap<usize, NamedArg> = HashMap::new();
    let mut by_name: HashMap<String, usize> = HashMap::new();
    let mut next_position = max_position;
    let mut failure: Option<RegisterError> = None;

    let _ = sqlparser::ast::visit_expressions_mut(statement, |expr| {
        if let Expr::Function(func) = expr {
            if let Some((name, nullable)) = sqlc_arg_call(func) {
                let position = if let Some(existing) = by_name.get(&name) {
                    let arg = &named[existing];
                    if arg.nullable != nullable {
                        failure = Some(RegisterError::ParamDeclMismatch {
                            query: query.to_owned(),
                            param: name.clone(),
                            declared: if arg.nullable {
                                "sqlc.narg"
                            } else {
                                "sqlc.arg"
                            }
                            .to_owned(),
                            used: if nullable { "sqlc.narg" } else { "sqlc.arg" }.to_owned(),
                        });
                        return ControlFlow::Break(());
                    }
                    *existing
                } else {
                    next_position += 1;
                    by_name.insert(name.clone(), next_position);
                    named.insert(
                        next_position,
                        NamedArg {
                            name: name.clone(),
                            nullable,
                        },
                    );
                    next_position
                };
                *expr = Expr::Value(Value::Placeholder(format!("${position}")));
            }
        }
        ControlFlow::<()>::Continue(())
    });

    if let Some(err) = failure {
        return Err(err);
    }
    Ok(named)
}

fn parse_dollar_position(text: &str) -> Option<usize> {
    let digits = text.strip_prefix('$')?;
    let position: usize = digits.parse().ok()?;
    (position >= 1).then_some(position)
}

/// Walks every placeholder value in the statement.
fn visit_placeholders<B>(
    statement: &Statement,
    visit: impl FnMut(&str) -> ControlFlow<B>,
) -> ControlFlow<B> {
    struct PlaceholderVisitor<F>(F);
    impl<B, F: FnMut(&str) -> ControlFlow<B>> Visitor for PlaceholderVisitor<F> {
        type Break = B;
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<B> {
            if let Expr::Value(Value::Placeholder(text)) = expr {
                self.0(text)?;
            }
            ControlFlow::Continue(())
        }
    }
    statement.visit(&mut PlaceholderVisitor(visit))
}

/// Validates placeholder forms and returns the used positions
/// (contiguity is checked in `resolve_params`).
fn collect_placeholders(query: &str, statement: &Statement) -> Result<Vec<usize>, RegisterError> {
    let mut positions: Vec<usize> = Vec::new();
    let flow = visit_placeholders(statement, |text| match parse_dollar_position(text) {
        Some(position) => {
            positions.push(position);
            ControlFlow::Continue(())
        }
        None => ControlFlow::Break(RegisterError::UnsupportedPlaceholder {
            query: query.to_owned(),
            placeholder: text.to_owned(),
        }),
    });
    if let ControlFlow::Break(err) = flow {
        return Err(err);
    }
    positions.sort_unstable();
    positions.dedup();
    Ok(positions)
}

// ---------------------------------------------------------------------
// Type inference
// ---------------------------------------------------------------------

/// What one use site tells us about a parameter.
#[derive(Debug, Clone, Default)]
struct Inference {
    ty: Option<ColumnType>,
    list: Option<bool>,
    /// Column name the parameter was compared against — the sqlc-style
    /// wire-name candidate.
    name_hint: Option<String>,
}

/// Collects `alias-or-table-name → table-name` for every base table in
/// the statement (all scopes flattened; inference is best-effort and
/// fails loudly on ambiguity).
fn collect_table_aliases(statement: &Statement) -> Vec<(String, String)> {
    struct TableVisitor(Vec<(String, String)>);
    impl Visitor for TableVisitor {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table { name, alias, .. } = factor {
                if let Some(table) = name.0.last() {
                    let key = alias
                        .as_ref()
                        .map_or_else(|| table.value.clone(), |a| a.name.value.clone());
                    self.0.push((key, table.value.clone()));
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut visitor = TableVisitor(Vec::new());
    let _ = statement.visit(&mut visitor);
    visitor.0
}

/// Resolves a column expression to `(column_name, type)` against the
/// catalog. `Err(reason)` explains why resolution failed.
fn resolve_column_type(
    expr: &Expr,
    aliases: &[(String, String)],
    catalog: &Catalog,
) -> Result<(String, ColumnType), String> {
    let (relation, column) = match unwrap_nested(expr) {
        Expr::Identifier(ident) => (None, ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => (
            Some(parts[parts.len() - 2].value.clone()),
            parts[parts.len() - 1].value.clone(),
        ),
        other => return Err(format!("'{other}' is not a column reference")),
    };

    if let Some(qualifier) = relation {
        let Some((_, table)) = aliases.iter().find(|(alias, _)| *alias == qualifier) else {
            return Err(format!("unknown relation '{qualifier}'"));
        };
        let Some(schema) = catalog.table(table) else {
            return Err(format!("table '{table}' is not in the catalog"));
        };
        let Some(col) = schema.column(&column) else {
            return Err(format!("column '{column}' not found on table '{table}'"));
        };
        return Ok((column, col.ty));
    }

    let mut matches: Vec<(&str, ColumnType)> = Vec::new();
    for (_, table) in aliases {
        if let Some(schema) = catalog.table(table) {
            if let Some(col) = schema.column(&column) {
                matches.push((table.as_str(), col.ty));
            }
        }
    }
    matches.dedup_by(|a, b| a == b);
    match matches.as_slice() {
        [] => Err(format!(
            "column '{column}' not found in any catalog table referenced by the query"
        )),
        [(_, ty)] => Ok((column, *ty)),
        many => Err(format!(
            "column '{column}' is ambiguous across tables {}",
            many.iter()
                .map(|(t, _)| format!("'{t}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn unwrap_nested(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unwrap_nested(inner),
        other => other,
    }
}

fn placeholder_position(expr: &Expr) -> Option<usize> {
    match unwrap_nested(expr) {
        Expr::Value(Value::Placeholder(text)) => parse_dollar_position(text),
        _ => None,
    }
}

const fn is_comparison(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    )
}

/// Runs sqlc-style inference over every placeholder use site.
/// Returns `position → Inference`; positions used in uninferrable
/// contexts appear with an empty `Inference`.
#[allow(clippy::too_many_lines)]
fn infer_param_types(
    statement: &Statement,
    catalog: &Catalog,
) -> HashMap<usize, Result<Inference, String>> {
    struct InferVisitor<'a, F> {
        aliases: &'a [(String, String)],
        catalog: &'a Catalog,
        record: F,
    }

    impl<F: FnMut(usize, Result<Inference, String>)> InferVisitor<'_, F> {
        fn column_site(&mut self, position: usize, column_expr: &Expr, list: bool) {
            let site =
                resolve_column_type(column_expr, self.aliases, self.catalog).map(|(name, ty)| {
                    Inference {
                        ty: Some(ty),
                        list: Some(list),
                        name_hint: Some(name),
                    }
                });
            (self.record)(position, site);
        }
    }

    impl<F: FnMut(usize, Result<Inference, String>)> Visitor for InferVisitor<'_, F> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
            // LIMIT $n / OFFSET $n are integers by construction.
            if let Some(limit) = &query.limit {
                if let Some(position) = placeholder_position(limit) {
                    (self.record)(
                        position,
                        Ok(Inference {
                            ty: Some(ColumnType::Int),
                            list: Some(false),
                            name_hint: Some("limit".to_owned()),
                        }),
                    );
                }
            }
            if let Some(offset) = &query.offset {
                if let Some(position) = placeholder_position(&offset.value) {
                    (self.record)(
                        position,
                        Ok(Inference {
                            ty: Some(ColumnType::Int),
                            list: Some(false),
                            name_hint: Some("offset".to_owned()),
                        }),
                    );
                }
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            match expr {
                // `$1::uuid` / `CAST($1 AS uuid)` — the cast target is
                // the declared type, no catalog needed.
                Expr::Cast {
                    kind: CastKind::Cast | CastKind::DoubleColon,
                    expr: inner,
                    data_type,
                    ..
                } => {
                    if let Some(position) = placeholder_position(inner) {
                        let site = ColumnType::from_cast_target(data_type).map_or_else(
                            || Err(format!("cast target '{data_type}' is unsupported")),
                            |ty| {
                                Ok(Inference {
                                    ty: Some(ty),
                                    list: Some(false),
                                    name_hint: None,
                                })
                            },
                        );
                        (self.record)(position, site);
                    }
                }
                Expr::BinaryOp { left, op, right }
                    if is_comparison(op)
                        || matches!(
                            op,
                            BinaryOperator::Plus
                                | BinaryOperator::Minus
                                | BinaryOperator::Multiply
                                | BinaryOperator::Divide
                                | BinaryOperator::Modulo
                        ) =>
                {
                    if let Some(position) = placeholder_position(right) {
                        self.column_site(position, left, false);
                    }
                    if let Some(position) = placeholder_position(left) {
                        self.column_site(position, right, false);
                    }
                }
                // `col = ANY($1)` — list of the column's type.
                Expr::AnyOp { left, right, .. } => {
                    if let Some(position) = placeholder_position(right) {
                        self.column_site(position, left, true);
                    }
                }
                Expr::InList {
                    expr: subject,
                    list,
                    ..
                } => {
                    for item in list {
                        if let Some(position) = placeholder_position(item) {
                            self.column_site(position, subject, false);
                        }
                    }
                }
                Expr::Between {
                    expr: subject,
                    low,
                    high,
                    ..
                } => {
                    for bound in [low, high] {
                        if let Some(position) = placeholder_position(bound) {
                            self.column_site(position, subject, false);
                        }
                    }
                }
                Expr::Like { pattern, .. } | Expr::ILike { pattern, .. } => {
                    if let Some(position) = placeholder_position(pattern) {
                        (self.record)(
                            position,
                            Ok(Inference {
                                ty: Some(ColumnType::Text),
                                list: Some(false),
                                name_hint: None,
                            }),
                        );
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let aliases = collect_table_aliases(statement);
    let mut inferred: HashMap<usize, Result<Inference, String>> = HashMap::new();
    let mut record = |position: usize, site: Result<Inference, String>| {
        merge_inference(&mut inferred, position, site);
    };
    let mut visitor = InferVisitor {
        aliases: &aliases,
        catalog,
        record: &mut record,
    };
    let _ = statement.visit(&mut visitor);
    inferred
}

/// Merges one use site into the accumulated inference for a position.
/// A previous hard failure is overwritten by a later success (any one
/// inferrable site is enough); conflicting successes surface as
/// synthesized `Err` values that `resolve_params` converts into the
/// typed conflict errors.
fn merge_inference(
    inferred: &mut HashMap<usize, Result<Inference, String>>,
    position: usize,
    site: Result<Inference, String>,
) {
    match inferred.entry(position) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(site);
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let slot = entry.get_mut();
            let Ok(new) = site else {
                // A failed site never demotes an already-recorded one.
                return;
            };
            match slot {
                Err(_) => *slot = Ok(new),
                Ok(existing) => {
                    if let Some(conflict) = merge_sites(existing, new) {
                        *slot = Err(conflict);
                    }
                }
            }
        }
    }
}

/// Folds `new` into `existing`; returns the conflict marker when the
/// two use sites disagree.
fn merge_sites(existing: &mut Inference, new: Inference) -> Option<String> {
    if let (Some(a), Some(b)) = (existing.list, new.list) {
        if a != b {
            return Some(CONFLICT_LIST.to_owned());
        }
    }
    match (existing.ty, new.ty) {
        (Some(a), Some(b)) if a != b => {
            return Some(format!(
                "{CONFLICT_TYPE}:{}:{}",
                column_type_name(a),
                column_type_name(b)
            ));
        }
        (None, Some(b)) => existing.ty = Some(b),
        _ => {}
    }
    if existing.name_hint.is_none() {
        existing.name_hint = new.name_hint;
    }
    if existing.list.is_none() {
        existing.list = new.list;
    }
    None
}

const CONFLICT_LIST: &str = "\u{1}conflict-list";
const CONFLICT_TYPE: &str = "\u{1}conflict-type";

// ---------------------------------------------------------------------
// Param resolution (decls or inference → ParamSpec list)
// ---------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn resolve_params(
    query: &str,
    statement: &Statement,
    positions: &[usize],
    named_args: &HashMap<usize, NamedArg>,
    decls: Option<&[ParamDecl]>,
    catalog: Option<&Catalog>,
) -> Result<Vec<ParamSpec>, RegisterError> {
    let max = positions.last().copied().unwrap_or(0);
    for expected in 1..=max {
        if !positions.contains(&expected) {
            return Err(RegisterError::NonContiguousParams {
                query: query.to_owned(),
                missing: expected,
                max,
            });
        }
    }

    if let Some(decls) = decls {
        if decls.len() != max {
            return Err(RegisterError::ParamCountMismatch {
                query: query.to_owned(),
                used: max,
                declared: decls.len(),
            });
        }
        let mut specs = Vec::with_capacity(decls.len());
        for (index, decl) in decls.iter().enumerate() {
            let position = index + 1;
            if let Some(arg) = named_args.get(&position) {
                if arg.name != decl.name {
                    return Err(RegisterError::ParamDeclMismatch {
                        query: query.to_owned(),
                        param: format!("${position}"),
                        declared: decl.name.clone(),
                        used: arg.name.clone(),
                    });
                }
            }
            specs.push(ParamSpec {
                position,
                name: decl.name.clone(),
                ty: decl.ty,
                list: decl.list,
                nullable: decl.nullable,
            });
        }
        check_duplicate_names(query, &specs)?;
        return Ok(specs);
    }

    // Inference-only path (sqlc source): every position must infer.
    let inferred = catalog.map_or_else(HashMap::default, |catalog| {
        infer_param_types(statement, catalog)
    });
    let mut specs = Vec::with_capacity(max);
    for position in 1..=max {
        let param_label = named_args.get(&position).map_or_else(
            || format!("${position}"),
            |arg| format!("sqlc.arg({})", arg.name),
        );
        let site = inferred.get(&position);
        let inference = match site {
            None => {
                return Err(RegisterError::ParamTypeUnknown {
                    query: query.to_owned(),
                    param: param_label,
                    reason: "the parameter is not used in a comparison, ANY(...), IN, BETWEEN, LIKE, LIMIT/OFFSET, or cast position".to_owned(),
                })
            }
            Some(Err(reason)) => {
                if reason == CONFLICT_LIST {
                    return Err(RegisterError::ParamListConflict {
                        query: query.to_owned(),
                        param: param_label,
                    });
                }
                if let Some(rest) = reason.strip_prefix(CONFLICT_TYPE) {
                    let mut parts = rest.trim_start_matches(':').splitn(2, ':');
                    let first = parts.next().unwrap_or("unknown");
                    let second = parts.next().unwrap_or("unknown");
                    return Err(RegisterError::ParamTypeConflict {
                        query: query.to_owned(),
                        param: param_label,
                        first: leak_type_name(first),
                        second: leak_type_name(second),
                    });
                }
                return Err(RegisterError::ParamTypeUnknown {
                    query: query.to_owned(),
                    param: param_label,
                    reason: reason.clone(),
                });
            }
            Some(Ok(inference)) => inference,
        };
        let Some(ty) = inference.ty else {
            return Err(RegisterError::ParamTypeUnknown {
                query: query.to_owned(),
                param: param_label,
                reason: "no use site determines a concrete type".to_owned(),
            });
        };
        let (name, nullable) = named_args.get(&position).map_or_else(
            || {
                (
                    inference
                        .name_hint
                        .clone()
                        .unwrap_or_else(|| format!("arg{position}")),
                    false,
                )
            },
            |arg| (arg.name.clone(), arg.nullable),
        );
        specs.push(ParamSpec {
            position,
            name,
            ty,
            list: inference.list.unwrap_or(false),
            nullable,
        });
    }

    // sqlc-style dedupe: a second parameter deriving an already-used
    // wire name gets a positional suffix.
    let mut seen: HashMap<String, usize> = HashMap::new();
    for spec in &mut specs {
        match seen.get(&spec.name) {
            None => {
                seen.insert(spec.name.clone(), spec.position);
            }
            Some(_) => {
                spec.name = format!("{}_{}", spec.name, spec.position);
            }
        }
    }
    check_duplicate_names(query, &specs)?;
    Ok(specs)
}

/// Maps an inferred type-name string back to the static name table so
/// `RegisterError::ParamTypeConflict` can carry `&'static str`.
fn leak_type_name(name: &str) -> &'static str {
    for ty in [
        ColumnType::Bool,
        ColumnType::Int,
        ColumnType::Float,
        ColumnType::Text,
        ColumnType::Timestamp,
        ColumnType::Uuid,
        ColumnType::Jsonb,
        ColumnType::Enum,
        ColumnType::Unknown,
    ] {
        if column_type_name(ty) == name {
            return column_type_name(ty);
        }
    }
    "unknown"
}

fn check_duplicate_names(query: &str, specs: &[ParamSpec]) -> Result<(), RegisterError> {
    let mut seen: HashMap<&str, usize> = HashMap::new();
    for spec in specs {
        if let Some(first) = seen.insert(&spec.name, spec.position) {
            return Err(RegisterError::DuplicateParamName {
                query: query.to_owned(),
                first: format!("${first}"),
                second: format!("${}", spec.position),
                name: spec.name.clone(),
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Binding
// ---------------------------------------------------------------------

fn dummy_value(spec: &ParamSpec) -> ParamValue {
    let scalar = match spec.ty {
        ColumnType::Bool => ParamValue::Bool(true),
        ColumnType::Int => ParamValue::Int(0),
        ColumnType::Float => ParamValue::Float(0.0),
        ColumnType::Uuid => ParamValue::Text("00000000-0000-0000-0000-000000000000".to_owned()),
        ColumnType::Timestamp => ParamValue::Text("2000-01-01 00:00:00".to_owned()),
        ColumnType::Jsonb => ParamValue::Text("{}".to_owned()),
        ColumnType::Text | ColumnType::Enum | ColumnType::Unknown => {
            ParamValue::Text(String::new())
        }
    };
    if spec.list {
        ParamValue::List(vec![scalar])
    } else {
        scalar
    }
}

fn bind_prepared(
    prepared: &PreparedQuery,
    params: &HashMap<String, ParamValue>,
    limits: QueryLimits,
) -> Result<BoundQuery, BindError> {
    // Resolve every supplied key to a position; refuse unknowns.
    let mut by_position: HashMap<usize, &ParamValue> = HashMap::new();
    for (key, value) in params {
        let spec = prepared
            .params
            .iter()
            .find(|spec| spec.name == *key || format!("${}", spec.position) == *key)
            .ok_or_else(|| BindError::UnexpectedParam {
                query: prepared.name.clone(),
                param: key.clone(),
            })?;
        by_position.insert(spec.position, value);
    }

    // Render a literal for every declared parameter; all must be
    // supplied.
    let mut rendered: HashMap<usize, Expr> = HashMap::with_capacity(prepared.params.len());
    for spec in &prepared.params {
        let value = by_position
            .get(&spec.position)
            .ok_or_else(|| BindError::MissingParam {
                query: prepared.name.clone(),
                param: spec.name.clone(),
            })?;
        rendered.insert(spec.position, render_literal(&prepared.name, spec, value)?);
    }

    // Substitute into a clone of the template.
    let mut statement = prepared.statement.clone();
    let _ = sqlparser::ast::visit_expressions_mut(&mut statement, |expr| {
        if let Expr::Value(Value::Placeholder(text)) = expr {
            if let Some(replacement) = parse_dollar_position(text).and_then(|p| rendered.get(&p)) {
                *expr = replacement.clone();
            }
        }
        ControlFlow::<()>::Continue(())
    });

    // The bound statement must pass the exact validation + lowering a
    // raw-SQL subscribe would.
    let rejected = |source: SqlError| BindError::Rejected {
        query: prepared.name.clone(),
        source,
    };
    let Statement::Query(query) = &statement else {
        return Err(rejected(SqlError::UnsupportedStatement));
    };
    validate_query(query).map_err(rejected)?;
    let graph = lower_select_statement(&statement).map_err(rejected)?;
    enforce_graph_size(graph.node_count(), limits).map_err(rejected)?;

    Ok(BoundQuery {
        sql: statement.to_string(),
        graph,
    })
}

/// Renders one typed literal AST node for a parameter value,
/// type-checking the value against the spec. String content never
/// reaches the SQL text unescaped: it travels as a
/// `Value::SingleQuotedString`, whose `Display` doubles embedded
/// quotes.
fn render_literal(query: &str, spec: &ParamSpec, value: &ParamValue) -> Result<Expr, BindError> {
    if spec.list {
        let ParamValue::List(items) = value else {
            return Err(type_mismatch(query, spec, value));
        };
        if items.len() > MAX_LIST_PARAM_LEN {
            return Err(BindError::InvalidValue {
                query: query.to_owned(),
                param: spec.name.clone(),
                reason: format!(
                    "list has {} elements, limit is {MAX_LIST_PARAM_LEN}",
                    items.len()
                ),
            });
        }
        let scalar_spec = ParamSpec {
            list: false,
            ..spec.clone()
        };
        let elem = items
            .iter()
            .map(|item| render_scalar(query, &scalar_spec, item))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Expr::Array(Array { elem, named: true }));
    }
    render_scalar(query, spec, value)
}

fn render_scalar(query: &str, spec: &ParamSpec, value: &ParamValue) -> Result<Expr, BindError> {
    let literal = match (spec.ty, value) {
        (_, ParamValue::Null) => {
            if !spec.nullable {
                return Err(BindError::NullNotAllowed {
                    query: query.to_owned(),
                    param: spec.name.clone(),
                });
            }
            Value::Null
        }
        (_, ParamValue::List(_)) => return Err(type_mismatch(query, spec, value)),
        (ColumnType::Bool, ParamValue::Bool(b)) => Value::Boolean(*b),
        (ColumnType::Bool, ParamValue::Text(s)) => match s.to_ascii_lowercase().as_str() {
            "true" => Value::Boolean(true),
            "false" => Value::Boolean(false),
            _ => return Err(type_mismatch(query, spec, value)),
        },
        (ColumnType::Int, ParamValue::Int(v)) => Value::Number(v.to_string(), false),
        (ColumnType::Int, ParamValue::Text(s)) => {
            let parsed: i64 = s
                .trim()
                .parse()
                .map_err(|_| type_mismatch(query, spec, value))?;
            Value::Number(parsed.to_string(), false)
        }
        (ColumnType::Float, ParamValue::Float(v)) => {
            if !v.is_finite() {
                return Err(BindError::InvalidValue {
                    query: query.to_owned(),
                    param: spec.name.clone(),
                    reason: "float parameter must be finite".to_owned(),
                });
            }
            Value::Number(v.to_string(), false)
        }
        (ColumnType::Float, ParamValue::Int(v)) => Value::Number(v.to_string(), false),
        (ColumnType::Float, ParamValue::Text(s)) => {
            let parsed: f64 = s
                .trim()
                .parse()
                .map_err(|_| type_mismatch(query, spec, value))?;
            if !parsed.is_finite() {
                return Err(BindError::InvalidValue {
                    query: query.to_owned(),
                    param: spec.name.clone(),
                    reason: "float parameter must be finite".to_owned(),
                });
            }
            Value::Number(parsed.to_string(), false)
        }
        (ColumnType::Uuid, ParamValue::Text(s)) => {
            let normalized = validate_uuid(s).ok_or_else(|| BindError::InvalidValue {
                query: query.to_owned(),
                param: spec.name.clone(),
                reason: format!("'{s}' is not a valid uuid"),
            })?;
            Value::SingleQuotedString(normalized)
        }
        (
            ColumnType::Text
            | ColumnType::Timestamp
            | ColumnType::Jsonb
            | ColumnType::Enum
            | ColumnType::Unknown,
            ParamValue::Text(s),
        ) => {
            if s.len() > MAX_TEXT_PARAM_BYTES {
                return Err(BindError::InvalidValue {
                    query: query.to_owned(),
                    param: spec.name.clone(),
                    reason: format!(
                        "value is {} bytes, limit is {MAX_TEXT_PARAM_BYTES}",
                        s.len()
                    ),
                });
            }
            Value::SingleQuotedString(s.clone())
        }
        _ => return Err(type_mismatch(query, spec, value)),
    };
    Ok(Expr::Value(literal))
}

fn type_mismatch(query: &str, spec: &ParamSpec, value: &ParamValue) -> BindError {
    BindError::TypeMismatch {
        query: query.to_owned(),
        param: spec.name.clone(),
        expected: param_shape(spec.ty, spec.list),
        got: value.kind_name(),
    }
}

/// Validates the canonical `8-4-4-4-12` hex uuid form (case
/// insensitive) and returns the lowercased text.
fn validate_uuid(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return None;
                }
            }
            _ => {
                if !byte.is_ascii_hexdigit() {
                    return None;
                }
            }
        }
    }
    Some(text.to_ascii_lowercase())
}

impl fmt::Display for PreparedCardinality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::One => write!(f, ":one"),
            Self::Many => write!(f, ":many"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BindError, ParamDecl, ParamValue, PreparedCardinality, QueryRegistry, RegisterError,
    };
    use crate::catalog::{Catalog, ColumnSchema, ColumnType, TableSchema};
    use std::collections::HashMap;

    fn boards_catalog() -> Catalog {
        Catalog::new([
            TableSchema::new(
                "boards",
                vec![
                    ColumnSchema::new("id", ColumnType::Uuid),
                    ColumnSchema::new("owner_id", ColumnType::Uuid),
                    ColumnSchema::new("title", ColumnType::Text),
                    ColumnSchema::new("deleted_at", ColumnType::Timestamp),
                ],
            ),
            TableSchema::new(
                "cards",
                vec![
                    ColumnSchema::new("id", ColumnType::Uuid),
                    ColumnSchema::new("board_id", ColumnType::Uuid),
                    ColumnSchema::new("position", ColumnType::Int),
                    ColumnSchema::new("archived", ColumnType::Bool),
                ],
            ),
        ])
    }

    fn params(entries: &[(&str, ParamValue)]) -> HashMap<String, ParamValue> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    const BOARD_SQLC: &str = "\
-- Live board view.
-- name: BoardCards :many
SELECT cards.id, cards.position
FROM cards
WHERE cards.board_id = $1 AND cards.archived = false
ORDER BY cards.position;

-- name: BoardById :one
SELECT id, title FROM boards WHERE id = $1;
";

    #[test]
    fn registers_sqlc_source_and_infers_uuid_param() {
        let mut registry = QueryRegistry::new();
        let names = registry
            .register_sqlc_source(BOARD_SQLC, "board.sql", &boards_catalog())
            .expect("register");
        assert_eq!(names, vec!["BoardCards".to_owned(), "BoardById".to_owned()]);

        let board_cards = registry.get("BoardCards").expect("registered");
        assert_eq!(board_cards.cardinality, PreparedCardinality::Many);
        assert_eq!(board_cards.params.len(), 1);
        let param = &board_cards.params[0];
        assert_eq!(param.name, "board_id");
        assert_eq!(param.ty, ColumnType::Uuid);
        assert!(!param.list);
    }

    #[test]
    fn bind_renders_typed_literals_and_lowers() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(BOARD_SQLC, "board.sql", &boards_catalog())
            .expect("register");

        let bound = registry
            .bind(
                "BoardCards",
                &params(&[(
                    "board_id",
                    ParamValue::Text("A5E9E2C0-0000-4000-8000-000000000042".to_owned()),
                )]),
            )
            .expect("bind");
        assert!(
            bound.sql.contains("'a5e9e2c0-0000-4000-8000-000000000042'"),
            "uuid literal should be normalized into the SQL: {}",
            bound.sql
        );
        assert!(bound.graph.node_count() > 0);

        // Positional key works too.
        registry
            .bind(
                "BoardCards",
                &params(&[(
                    "$1",
                    ParamValue::Text("a5e9e2c0-0000-4000-8000-000000000042".to_owned()),
                )]),
            )
            .expect("bind by position");
    }

    #[test]
    fn unknown_name_fails_closed() {
        let registry = QueryRegistry::new();
        let err = registry.bind("Nope", &HashMap::new()).expect_err("refused");
        assert!(matches!(err, BindError::UnknownQuery(name) if name == "Nope"));
    }

    #[test]
    fn missing_extra_and_illtyped_params_are_refused() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(BOARD_SQLC, "board.sql", &boards_catalog())
            .expect("register");

        let err = registry
            .bind("BoardCards", &HashMap::new())
            .expect_err("missing param");
        assert!(matches!(err, BindError::MissingParam { .. }), "{err}");

        let err = registry
            .bind(
                "BoardCards",
                &params(&[
                    (
                        "board_id",
                        ParamValue::Text("a5e9e2c0-0000-4000-8000-000000000042".to_owned()),
                    ),
                    ("bogus", ParamValue::Int(1)),
                ]),
            )
            .expect_err("extra param");
        assert!(matches!(err, BindError::UnexpectedParam { .. }), "{err}");

        let err = registry
            .bind(
                "BoardCards",
                &params(&[("board_id", ParamValue::Text("not-a-uuid".to_owned()))]),
            )
            .expect_err("bad uuid");
        assert!(matches!(err, BindError::InvalidValue { .. }), "{err}");
    }

    #[test]
    fn quote_bearing_text_param_cannot_alter_query_shape() {
        let mut registry = QueryRegistry::new();
        registry
            .register(
                "TitledBoards",
                "SELECT id FROM boards WHERE title = $1",
                &[ParamDecl::new("title", ColumnType::Text)],
            )
            .expect("register");

        let hostile = "x' OR '1'='1";
        let bound = registry
            .bind(
                "TitledBoards",
                &params(&[("title", ParamValue::Text(hostile.to_owned()))]),
            )
            .expect("bind");
        // The whole payload must stay inside one string literal:
        // sqlparser doubles the embedded quotes on render.
        assert!(
            bound.sql.contains("'x'' OR ''1''=''1'"),
            "expected escaped literal in {}",
            bound.sql
        );
        // And the bound text must round-trip as a single valid SELECT.
        crate::parse_and_lower(&bound.sql).expect("bound SQL parses");
    }

    #[test]
    fn any_list_param_registers_and_binds_as_array_literal() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(
                "-- name: CardsInBoards :many\n\
                 SELECT id FROM cards WHERE board_id = ANY($1);\n",
                "cards.sql",
                &boards_catalog(),
            )
            .expect("register ANY-list query");

        let query = registry.get("CardsInBoards").expect("registered");
        assert!(query.params[0].list, "ANY($1) infers a list param");
        assert_eq!(query.params[0].ty, ColumnType::Uuid);

        let bound = registry
            .bind(
                "CardsInBoards",
                &params(&[(
                    "board_id",
                    ParamValue::List(vec![
                        ParamValue::Text("00000000-0000-0000-0000-000000000001".to_owned()),
                        ParamValue::Text("00000000-0000-0000-0000-000000000002".to_owned()),
                    ]),
                )]),
            )
            .expect("bind list");
        assert!(bound.sql.contains("ARRAY["), "{}", bound.sql);
    }

    #[test]
    fn sqlc_named_args_register_and_bind() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(
                "-- name: CardsAt :many\n\
                 SELECT id FROM cards WHERE position = sqlc.arg(pos) AND archived = sqlc.narg(archived);\n",
                "cards.sql",
                &boards_catalog(),
            )
            .expect("register sqlc.arg query");
        let query = registry.get("CardsAt").expect("registered");
        assert_eq!(query.params[0].name, "pos");
        assert_eq!(query.params[0].ty, ColumnType::Int);
        assert!(!query.params[0].nullable);
        assert_eq!(query.params[1].name, "archived");
        assert!(query.params[1].nullable);

        registry
            .bind(
                "CardsAt",
                &params(&[("pos", ParamValue::Int(3)), ("archived", ParamValue::Null)]),
            )
            .expect("nullable param accepts null");

        let err = registry
            .bind(
                "CardsAt",
                &params(&[("pos", ParamValue::Null), ("archived", ParamValue::Null)]),
            )
            .expect_err("non-nullable param refuses null");
        assert!(matches!(err, BindError::NullNotAllowed { .. }), "{err}");
    }

    #[test]
    fn cast_annotation_types_a_param_without_catalog_help() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(
                "-- name: Recent :many\n\
                 SELECT id FROM boards WHERE deleted_at IS NULL LIMIT $1;\n",
                "boards.sql",
                &boards_catalog(),
            )
            .expect("LIMIT $1 infers int");
        assert_eq!(
            registry.get("Recent").expect("registered").params[0].ty,
            ColumnType::Int
        );
    }

    #[test]
    fn unsupported_construct_is_named_at_registration_time() {
        let mut registry = QueryRegistry::new();
        let err = registry
            .register_sqlc_source(
                "-- name: Bad :many\n\
                 SELECT b.id FROM boards b RIGHT JOIN cards c ON b.id = c.board_id;\n",
                "bad.sql",
                &boards_catalog(),
            )
            .expect_err("RIGHT JOIN must be rejected");
        let message = err.to_string();
        assert!(message.contains("Bad"), "{message}");
        assert!(message.contains("RIGHT JOIN"), "{message}");
        // All-or-nothing: nothing from the failing source registered.
        assert!(registry.is_empty());
    }

    #[test]
    fn uninferrable_param_is_rejected_with_reason() {
        let mut registry = QueryRegistry::new();
        let err = registry
            .register_sqlc_source(
                "-- name: Mystery :many\n\
                 SELECT id FROM boards WHERE mystery_column = $1;\n",
                "mystery.sql",
                &boards_catalog(),
            )
            .expect_err("unknown column cannot type the param");
        assert!(
            matches!(err, RegisterError::ParamTypeUnknown { ref param, .. } if param == "$1"),
            "{err}"
        );
    }

    #[test]
    fn mutation_verbs_are_rejected() {
        let mut registry = QueryRegistry::new();
        let err = registry
            .register_sqlc_source(
                "-- name: Touch :exec\n\
                 UPDATE boards SET title = 'x';\n",
                "touch.sql",
                &boards_catalog(),
            )
            .expect_err(":exec has no live-subscription meaning");
        assert!(
            matches!(err, RegisterError::UnsupportedVerb { ref verb, .. } if verb == ":exec"),
            "{err}"
        );
    }

    #[test]
    fn duplicate_names_and_noncontiguous_params_are_rejected() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(BOARD_SQLC, "board.sql", &boards_catalog())
            .expect("register");
        let err = registry
            .register_sqlc_source(
                "-- name: BoardById :one\nSELECT id FROM boards WHERE id = $1;\n",
                "again.sql",
                &boards_catalog(),
            )
            .expect_err("duplicate name");
        assert!(matches!(err, RegisterError::Duplicate(_)), "{err}");

        let err = registry
            .register_sqlc_source(
                "-- name: Sparse :many\nSELECT id FROM cards WHERE position = $2;\n",
                "sparse.sql",
                &boards_catalog(),
            )
            .expect_err("$2 without $1");
        assert!(
            matches!(err, RegisterError::NonContiguousParams { missing: 1, .. }),
            "{err}"
        );
    }

    #[test]
    fn same_column_twice_dedupes_wire_names_sqlc_style() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(
                "-- name: PositionRange :many\n\
                 SELECT id FROM cards WHERE position >= $1 AND position <= $2;\n",
                "range.sql",
                &boards_catalog(),
            )
            .expect("register");
        let query = registry.get("PositionRange").expect("registered");
        assert_eq!(query.params[0].name, "position");
        assert_eq!(query.params[1].name, "position_2");
    }

    #[test]
    fn explicit_registration_validates_param_count() {
        let mut registry = QueryRegistry::new();
        let err = registry
            .register(
                "Short",
                "SELECT id FROM boards WHERE id = $1 AND title = $2",
                &[ParamDecl::new("id", ColumnType::Uuid)],
            )
            .expect_err("declared 1, used 2");
        assert!(
            matches!(
                err,
                RegisterError::ParamCountMismatch {
                    used: 2,
                    declared: 1,
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn bound_sql_is_deterministic_for_plan_sharing() {
        let mut registry = QueryRegistry::new();
        registry
            .register_sqlc_source(BOARD_SQLC, "board.sql", &boards_catalog())
            .expect("register");
        let uuid = "00000000-0000-0000-0000-00000000abcd";
        let a = registry
            .bind(
                "BoardCards",
                &params(&[("board_id", ParamValue::Text(uuid.to_owned()))]),
            )
            .expect("bind a");
        let b = registry
            .bind(
                "BoardCards",
                &params(&[("$1", ParamValue::Text(uuid.to_owned()))]),
            )
            .expect("bind b");
        assert_eq!(a.sql, b.sql, "same name+params must share a canonical key");
    }

    #[test]
    fn int_param_accepts_stringly_typed_input_strictly() {
        let mut registry = QueryRegistry::new();
        registry
            .register(
                "AtPosition",
                "SELECT id FROM cards WHERE position = $1",
                &[ParamDecl::new("position", ColumnType::Int)],
            )
            .expect("register");
        registry
            .bind(
                "AtPosition",
                &params(&[("position", ParamValue::Text("42".to_owned()))]),
            )
            .expect("digit string coerces");
        let err = registry
            .bind(
                "AtPosition",
                &params(&[("position", ParamValue::Text("42; DROP TABLE".to_owned()))]),
            )
            .expect_err("garbage is refused");
        assert!(matches!(err, BindError::TypeMismatch { .. }), "{err}");
    }
}
