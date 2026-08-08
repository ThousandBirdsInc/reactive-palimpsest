// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Runtime expression evaluator.
//!
//! Compiles SQL expression strings (boolean predicates, projection
//! expressions, aggregate input columns, order-by keys) into closures
//! over [`Row`] values. Mirrors GlueSQL's `Evaluator` shape: the
//! parser produces an `Expr` AST; this module walks the AST and
//! returns a `Box<dyn Fn(&Row) -> Datum>` (or a typed wrapper) that
//! reads concrete column values out of the row at evaluation time.
//!
//! Notably **no `$user.*` resolution lives here**. The permission
//! rewriter materializes user-context references into literal values
//! before the predicate reaches the dataflow
//! (`palimpsest_permissions::compile::CompiledPredicate::materialize`),
//! so the evaluator only deals with column refs + literals + boolean
//! logic.

use std::collections::BTreeMap;
use std::fmt;

use palimpsest_sql::catalog::ColumnType;
use palimpsest_wal::Datum;
use sqlparser::ast::{BinaryOperator, CastKind, DataType, Expr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

use crate::palimpsest::wal::Row;

/// Closure that reads a single `Datum` out of a row.
pub type ScalarFn = Box<dyn Fn(&Row) -> Datum + Send + Sync>;

/// Closure that evaluates a boolean predicate over a row.
pub type PredicateFn = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// Closure that extracts an `i64`-coerced column value (for aggregate
/// inputs). Returns 0 for `NULL` / non-numeric — same handling as
/// SQL's implicit-coalesce-to-zero in `SUM` / `AVG`.
pub type IntExtractor = Box<dyn Fn(&Row) -> i64 + Send + Sync>;

/// Per-column metadata used during compilation to resolve identifiers
/// to row indices. Built by the MIR walker from each node's output
/// schema.
#[derive(Debug, Clone, Default)]
pub struct ScalarSchema {
    columns: Vec<(String, ColumnType)>,
    index: BTreeMap<String, usize>,
    /// Per-column source-relation attribution, aligned with `columns`.
    /// Empty when the caller supplied none. Lets qualified references
    /// (`tickets.id`) bind to the right occurrence when a join carries
    /// the same bare name on both sides.
    provenance: Vec<Option<String>>,
}

impl ScalarSchema {
    /// Build a schema from a sequence of `(name, type)` pairs in
    /// column order. The last column with a given name wins on
    /// collision (mirroring SQL's "last alias wins" projection rule).
    #[must_use]
    pub fn from_pairs(columns: impl IntoIterator<Item = (String, ColumnType)>) -> Self {
        let columns: Vec<_> = columns.into_iter().collect();
        let mut index = BTreeMap::new();
        for (i, (name, _)) in columns.iter().enumerate() {
            index.insert(name.clone(), i);
        }
        Self {
            columns,
            index,
            provenance: Vec::new(),
        }
    }

    /// Attaches per-column source-relation attribution (aligned with
    /// the column order) so qualified references resolve by relation
    /// rather than falling back to bare-name lookup.
    #[must_use]
    pub fn with_provenance(mut self, provenance: Vec<Option<String>>) -> Self {
        self.provenance = provenance;
        self
    }

    /// Row index of the column named `name` whose provenance
    /// attributes it to `relation`, if any.
    #[must_use]
    pub fn index_of_qualified(&self, relation: &str, name: &str) -> Option<usize> {
        self.columns.iter().enumerate().position(|(i, (col, _))| {
            col == name && self.provenance.get(i).and_then(Option::as_deref) == Some(relation)
        })
    }

    /// Row index of the column named `name`, if any.
    #[must_use]
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Declared type of the column named `name`, if any.
    #[must_use]
    pub fn column_type(&self, name: &str) -> Option<ColumnType> {
        self.index.get(name).map(|&i| self.columns[i].1)
    }

    /// Ordered `(name, type)` pairs.
    #[must_use]
    pub fn columns(&self) -> &[(String, ColumnType)] {
        &self.columns
    }

    /// Number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// True when no columns are declared.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Errors raised during compile-time analysis. Runtime evaluation
/// itself is total: every closure returns *some* `Datum` — invalid
/// arithmetic / type mismatches surface as `Datum::Null`, matching
/// SQL's three-valued semantics on most paths.
#[derive(Debug, Error)]
pub enum EvalError {
    /// The SQL parser refused the expression.
    #[error("parse error: {0}")]
    Parse(String),
    /// The expression uses a feature this evaluator doesn't implement.
    #[error("unsupported expression: {0}")]
    Unsupported(String),
    /// An identifier didn't resolve against the input schema.
    #[error("unknown column: {0}")]
    UnknownColumn(String),
}

/// Compile `expr_sql` into a boolean predicate. Non-bool / null
/// results count as `false`, matching `WHERE` semantics.
///
/// # Errors
/// Returns [`EvalError`] on parse failure, unknown columns, or
/// unsupported operator kinds.
pub fn compile_predicate(expr_sql: &str, schema: &ScalarSchema) -> Result<PredicateFn, EvalError> {
    let scalar = compile_scalar(expr_sql, schema)?;
    Ok(Box::new(move |row| {
        matches!(scalar(row), Datum::Bool(true))
    }))
}

/// Compile `expr_sql` into a scalar closure.
///
/// # Errors
/// See [`compile_predicate`].
pub fn compile_scalar(expr_sql: &str, schema: &ScalarSchema) -> Result<ScalarFn, EvalError> {
    let expr = parse_expr(expr_sql)?;
    compile_inner(&expr, schema)
}

/// Compile `expr_sql` into a scalar closure together with its inferred
/// output [`ColumnType`]. Used for projection entries and aggregate
/// arguments that are full expressions (casts, `coalesce`,
/// arithmetic) rather than plain column references — the caller needs
/// a type to advertise in the output schema. `ColumnType::Unknown`
/// when inference has nothing to go on (e.g. a bare `NULL` literal);
/// unknown-typed columns are advertised permissively on the wire.
pub fn compile_typed_scalar(
    expr_sql: &str,
    schema: &ScalarSchema,
) -> Result<(ScalarFn, ColumnType), EvalError> {
    let expr = parse_expr(expr_sql)?;
    let scalar = compile_inner(&expr, schema)?;
    let ty = infer_type(&expr, schema);
    // Reconcile runtime values with the advertised type: an
    // expression like `coalesce(uuid_col, '<literal>')` infers `Uuid`
    // from the column, but the fallback branch would produce a text
    // datum at runtime — and the schema mismatch would kill the
    // subscription client-side. Coerce every produced datum to the
    // inferred type so the wire always matches the advertisement.
    let scalar = match ty {
        ColumnType::Unknown | ColumnType::Text => scalar,
        concrete => Box::new(move |row: &Row| cast_datum(scalar(row), concrete)),
    };
    Ok((scalar, ty))
}

/// Output-column label for a projection entry that is a cast of a
/// simple column (`id::text`, `CAST(posts.id AS text)`): Postgres
/// names that column after the inner identifier. `None` for anything
/// else — the caller falls back to the raw expression text.
#[must_use]
pub fn cast_label(expr_sql: &str) -> Option<String> {
    fn inner(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Nested(nested) => inner(nested),
            Expr::Cast { expr: source, .. } => match source.as_ref() {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
                _ => None,
            },
            _ => None,
        }
    }
    inner(&parse_expr(expr_sql).ok()?)
}

/// Best-effort static type of `expr` against `schema`. `Unknown` when
/// inference has nothing to go on (bare `NULL`, unsupported shapes).
fn infer_type(expr: &Expr, schema: &ScalarSchema) -> ColumnType {
    match expr {
        Expr::Nested(inner) => infer_type(inner, schema),
        Expr::Identifier(ident) => schema
            .column_type(&ident.value)
            .unwrap_or(ColumnType::Unknown),
        Expr::CompoundIdentifier(parts) => {
            let qualified = match parts.as_slice() {
                [relation, column] => schema
                    .index_of_qualified(&relation.value, &column.value)
                    .map(|idx| schema.columns()[idx].1),
                _ => None,
            };
            qualified
                .or_else(|| {
                    parts
                        .last()
                        .and_then(|last| schema.column_type(&last.value))
                })
                .unwrap_or(ColumnType::Unknown)
        }
        Expr::Value(SqlValue::Boolean(_)) => ColumnType::Bool,
        Expr::Value(SqlValue::Number(n, _)) => {
            if n.parse::<i64>().is_ok() {
                ColumnType::Int
            } else {
                ColumnType::Float
            }
        }
        Expr::Value(SqlValue::SingleQuotedString(_) | SqlValue::DoubleQuotedString(_)) => {
            ColumnType::Text
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::And
            | BinaryOperator::Or => ColumnType::Bool,
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                match (infer_type(left, schema), infer_type(right, schema)) {
                    (ColumnType::Int, ColumnType::Int) => ColumnType::Int,
                    (l, r) if l.is_numeric() && r.is_numeric() => ColumnType::Float,
                    _ => ColumnType::Unknown,
                }
            }
            BinaryOperator::StringConcat => ColumnType::Text,
            _ => ColumnType::Unknown,
        },
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Not => ColumnType::Bool,
            UnaryOperator::Minus | UnaryOperator::Plus => infer_type(inner, schema),
            _ => ColumnType::Unknown,
        },
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsDistinctFrom(..)
        | Expr::IsNotDistinctFrom(..)
        | Expr::AnyOp { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::ILike { .. } => ColumnType::Bool,
        Expr::Case {
            results,
            else_result,
            ..
        } => results
            .iter()
            .chain(else_result.as_deref())
            .map(|branch| infer_type(branch, schema))
            .find(|ty| *ty != ColumnType::Unknown)
            .unwrap_or(ColumnType::Unknown),
        Expr::Cast { data_type, .. } => cast_target_type(data_type).unwrap_or(ColumnType::Unknown),
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_lowercase();
            match name.as_str() {
                "cardinality" => ColumnType::Int,
                "coalesce" => coalesce_args(function)
                    .into_iter()
                    .map(|arg| infer_type(arg, schema))
                    .find(|ty| *ty != ColumnType::Unknown)
                    .unwrap_or(ColumnType::Unknown),
                _ => ColumnType::Unknown,
            }
        }
        _ => ColumnType::Unknown,
    }
}

fn coalesce_args(function: &sqlparser::ast::Function) -> Vec<&Expr> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    match &function.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Convenience: compile a single column reference into an `i64`
/// extractor. Used by aggregate input expressions like `SUM(value)`,
/// where the argument is a simple identifier. Also accepts `*` as
/// a sentinel for `COUNT(*)`, returning a constant `0` (the aggregate
/// only inspects the diff multiplicity in that case).
///
/// # Errors
/// Returns `EvalError::UnknownColumn` if the named column isn't in
/// `schema`, or `EvalError::Unsupported` for non-identifier inputs.
pub fn compile_int_extractor(
    arg_sql: &str,
    schema: &ScalarSchema,
) -> Result<IntExtractor, EvalError> {
    let trimmed = arg_sql.trim();
    if trimmed == "*" {
        return Ok(Box::new(|_| 0));
    }
    let scalar = compile_scalar(trimmed, schema)?;
    Ok(Box::new(move |row| match scalar(row) {
        Datum::I64(v) => v,
        Datum::I32(v) => i64::from(v),
        Datum::I16(v) => i64::from(v),
        _ => 0,
    }))
}

fn parse_expr(sql: &str) -> Result<Expr, EvalError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|err| EvalError::Parse(err.to_string()))?;
    parser
        .parse_expr()
        .map_err(|err| EvalError::Parse(err.to_string()))
}

fn compile_inner(expr: &Expr, schema: &ScalarSchema) -> Result<ScalarFn, EvalError> {
    match expr {
        Expr::Nested(inner) => compile_inner(inner, schema),
        Expr::Identifier(ident) => identifier_scalar(&ident.value, schema),
        Expr::CompoundIdentifier(parts) => {
            // Bind `relation.column` through provenance first: a join
            // can carry the same bare name on both sides, and the
            // qualifier is what tells them apart. Fall back to the
            // trailing segment for schemas without provenance (or
            // qualifiers that don't attribute, e.g. derived-table
            // aliases).
            if let [relation, column] = parts.as_slice() {
                if let Some(idx) = schema.index_of_qualified(&relation.value, &column.value) {
                    return Ok(Box::new(move |row| {
                        row.get(idx).cloned().unwrap_or(Datum::Null)
                    }));
                }
            }
            let last = parts
                .last()
                .ok_or_else(|| EvalError::Unsupported("empty compound identifier".to_owned()))?;
            identifier_scalar(&last.value, schema)
        }
        Expr::Value(value) => value_scalar(value),
        Expr::BinaryOp { left, op, right } => binary_scalar(left, op.clone(), right, schema),
        Expr::UnaryOp { op, expr: inner } => unary_scalar(op.clone(), inner, schema),
        Expr::IsNull(inner) => {
            let target = compile_inner(inner, schema)?;
            Ok(Box::new(move |row| {
                Datum::Bool(matches!(target(row), Datum::Null))
            }))
        }
        Expr::IsNotNull(inner) => {
            let target = compile_inner(inner, schema)?;
            Ok(Box::new(move |row| {
                Datum::Bool(!matches!(target(row), Datum::Null))
            }))
        }
        Expr::IsTrue(inner) => {
            let target = compile_inner(inner, schema)?;
            Ok(Box::new(move |row| {
                Datum::Bool(matches!(target(row), Datum::Bool(true)))
            }))
        }
        Expr::IsFalse(inner) => {
            let target = compile_inner(inner, schema)?;
            Ok(Box::new(move |row| {
                Datum::Bool(matches!(target(row), Datum::Bool(false)))
            }))
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => any_scalar(left, compare_op, right, schema),
        Expr::Cast {
            kind: CastKind::Cast | CastKind::DoubleColon,
            expr: inner,
            data_type,
            ..
        } => {
            let target = cast_target_type(data_type)
                .ok_or_else(|| EvalError::Unsupported(format!("cast target type {data_type}")))?;
            let source = compile_inner(inner, schema)?;
            Ok(Box::new(move |row| cast_datum(source(row), target)))
        }
        Expr::InList {
            expr: needle,
            list,
            negated,
        } => {
            let needle = compile_inner(needle, schema)?;
            let elements: Vec<ScalarFn> = list
                .iter()
                .map(|element| compile_inner(element, schema))
                .collect::<Result<_, _>>()?;
            let negated = *negated;
            Ok(Box::new(move |row| {
                let value = needle(row);
                // SQL three-valued logic collapsed onto WHERE
                // semantics: a NULL needle (or, for NOT IN, a NULL
                // list element) yields UNKNOWN, which filters the row.
                if matches!(value, Datum::Null) {
                    return Datum::Bool(false);
                }
                let mut saw_null = false;
                let mut hit = false;
                for element in &elements {
                    let candidate = element(row);
                    if matches!(candidate, Datum::Null) {
                        saw_null = true;
                    } else if datum_eq(&value, &candidate) {
                        hit = true;
                        break;
                    }
                }
                if negated {
                    Datum::Bool(!hit && !saw_null)
                } else {
                    Datum::Bool(hit)
                }
            }))
        }
        Expr::Between {
            expr: needle,
            negated,
            low,
            high,
        } => {
            let needle = compile_inner(needle, schema)?;
            let low = compile_inner(low, schema)?;
            let high = compile_inner(high, schema)?;
            let negated = *negated;
            Ok(Box::new(move |row| {
                let value = needle(row);
                let low_value = low(row);
                let high_value = high(row);
                // A NULL anywhere makes the comparison UNKNOWN → the
                // row is filtered whether or not the test is negated.
                if matches!(value, Datum::Null)
                    || matches!(low_value, Datum::Null)
                    || matches!(high_value, Datum::Null)
                {
                    return Datum::Bool(false);
                }
                let within = matches!(
                    datum_cmp_bool(&low_value, &value, |o| o.is_le()),
                    Datum::Bool(true)
                ) && matches!(
                    datum_cmp_bool(&value, &high_value, |o| o.is_le()),
                    Datum::Bool(true)
                );
                Datum::Bool(within != negated)
            }))
        }
        Expr::Like {
            negated,
            any: false,
            expr: subject,
            pattern,
            escape_char,
        } => like_scalar(
            subject,
            pattern,
            escape_char.as_deref(),
            *negated,
            false,
            schema,
        ),
        Expr::ILike {
            negated,
            any: false,
            expr: subject,
            pattern,
            escape_char,
        } => like_scalar(
            subject,
            pattern,
            escape_char.as_deref(),
            *negated,
            true,
            schema,
        ),
        Expr::IsDistinctFrom(left, right) => {
            let l = compile_inner(left, schema)?;
            let r = compile_inner(right, schema)?;
            Ok(Box::new(move |row| {
                Datum::Bool(!null_safe_eq(&l(row), &r(row)))
            }))
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let l = compile_inner(left, schema)?;
            let r = compile_inner(right, schema)?;
            Ok(Box::new(move |row| {
                Datum::Bool(null_safe_eq(&l(row), &r(row)))
            }))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => case_scalar(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            schema,
        ),
        Expr::Array(array) => {
            let elements: Vec<ScalarFn> = array
                .elem
                .iter()
                .map(|element| compile_inner(element, schema))
                .collect::<Result<_, _>>()?;
            Ok(Box::new(move |row| {
                Datum::Array(elements.iter().map(|element| element(row)).collect())
            }))
        }
        Expr::Function(function) => function_scalar(function, schema),
        other => Err(EvalError::Unsupported(format!("{other:?}"))),
    }
}

/// SQL `IS [NOT] DISTINCT FROM`: null-safe equality — two NULLs are
/// "not distinct", a NULL and a value are distinct.
fn null_safe_eq(a: &Datum, b: &Datum) -> bool {
    match (matches!(a, Datum::Null), matches!(b, Datum::Null)) {
        (true, true) => true,
        (true, false) | (false, true) => false,
        (false, false) => datum_eq(a, b),
    }
}

/// `[NOT] [I]LIKE` with `%` / `_` wildcards and an escape character
/// (Postgres defaults to backslash). NULL subject or pattern filters
/// the row, matching WHERE semantics.
fn like_scalar(
    subject: &Expr,
    pattern: &Expr,
    escape_char: Option<&str>,
    negated: bool,
    case_insensitive: bool,
    schema: &ScalarSchema,
) -> Result<ScalarFn, EvalError> {
    let subject = compile_inner(subject, schema)?;
    let pattern = compile_inner(pattern, schema)?;
    let escape = match escape_char {
        None => Some('\\'),
        Some(text) => {
            let mut chars = text.chars();
            let first = chars.next();
            if chars.next().is_some() {
                return Err(EvalError::Unsupported(format!(
                    "multi-character LIKE escape {text:?}"
                )));
            }
            // `ESCAPE ''` disables escaping in Postgres.
            first
        }
    };
    Ok(Box::new(move |row| {
        let (Datum::Text(subject), Datum::Text(pattern)) = (subject(row), pattern(row)) else {
            return Datum::Bool(false);
        };
        let (Ok(subject), Ok(pattern)) =
            (std::str::from_utf8(&subject), std::str::from_utf8(&pattern))
        else {
            return Datum::Bool(false);
        };
        let matched = if case_insensitive {
            like_match(&subject.to_lowercase(), &pattern.to_lowercase(), escape)
        } else {
            like_match(subject, pattern, escape)
        };
        Datum::Bool(matched != negated)
    }))
}

/// SQL LIKE matching: `%` matches any sequence, `_` any single
/// character, and `escape` makes the following character literal.
fn like_match(subject: &str, pattern: &str, escape: Option<char>) -> bool {
    #[derive(PartialEq)]
    enum Token {
        AnyRun,
        AnyOne,
        Literal(char),
    }
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if Some(c) == escape {
            match chars.next() {
                Some(escaped) => tokens.push(Token::Literal(escaped)),
                // Trailing escape matches nothing in Postgres; treat
                // it as a literal escape character.
                None => tokens.push(Token::Literal(c)),
            }
        } else if c == '%' {
            if tokens.last() != Some(&Token::AnyRun) {
                tokens.push(Token::AnyRun);
            }
        } else if c == '_' {
            tokens.push(Token::AnyOne);
        } else {
            tokens.push(Token::Literal(c));
        }
    }

    // Classic two-pointer wildcard match with backtracking on `%`.
    let subject: Vec<char> = subject.chars().collect();
    let (mut s, mut p) = (0_usize, 0_usize);
    let (mut star, mut star_s) = (None::<usize>, 0_usize);
    while s < subject.len() {
        match tokens.get(p) {
            Some(Token::Literal(c)) if *c == subject[s] => {
                s += 1;
                p += 1;
            }
            Some(Token::AnyOne) => {
                s += 1;
                p += 1;
            }
            Some(Token::AnyRun) => {
                star = Some(p);
                star_s = s;
                p += 1;
            }
            _ => {
                let Some(star_p) = star else { return false };
                star_s += 1;
                s = star_s;
                p = star_p + 1;
            }
        }
    }
    while tokens.get(p) == Some(&Token::AnyRun) {
        p += 1;
    }
    p == tokens.len()
}

/// `CASE` in both forms: `CASE x WHEN v THEN ...` compares the operand
/// against each branch value; `CASE WHEN cond THEN ...` evaluates each
/// condition as a predicate. No matching branch yields the ELSE value
/// or NULL.
fn case_scalar(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    schema: &ScalarSchema,
) -> Result<ScalarFn, EvalError> {
    let operand = operand
        .map(|expr| compile_inner(expr, schema))
        .transpose()?;
    let conditions: Vec<ScalarFn> = conditions
        .iter()
        .map(|expr| compile_inner(expr, schema))
        .collect::<Result<_, _>>()?;
    let results: Vec<ScalarFn> = results
        .iter()
        .map(|expr| compile_inner(expr, schema))
        .collect::<Result<_, _>>()?;
    let else_result = else_result
        .map(|expr| compile_inner(expr, schema))
        .transpose()?;
    Ok(Box::new(move |row| {
        for (condition, result) in conditions.iter().zip(&results) {
            let hit = match &operand {
                Some(operand) => datum_eq(&operand(row), &condition(row)),
                None => matches!(condition(row), Datum::Bool(true)),
            };
            if hit {
                return result(row);
            }
        }
        else_result
            .as_ref()
            .map_or(Datum::Null, |result| result(row))
    }))
}

/// SQL `ORDER BY` comparison across datums. `NULL` compares greater
/// than everything (so ascending order puts NULLs last and a reversed
/// comparison puts them first — Postgres' defaults for `ASC` / `DESC`).
/// Numerics compare across widths; other cross-type pairs fall back to
/// the derived `Ord`, which is arbitrary but total and deterministic.
#[must_use]
pub fn compare_datums_sort(a: &Datum, b: &Datum) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    use Datum::{Null, Text, Uuid};

    fn as_i128(datum: &Datum) -> Option<i128> {
        match datum {
            Datum::I64(v) => Some(i128::from(*v)),
            Datum::I32(v) => Some(i128::from(*v)),
            Datum::I16(v) => Some(i128::from(*v)),
            _ => None,
        }
    }
    #[allow(clippy::cast_precision_loss)]
    fn as_f64(datum: &Datum) -> Option<f64> {
        match datum {
            Datum::F64(bits) => Some(f64::from_bits(*bits)),
            Datum::F32(bits) => Some(f64::from(f32::from_bits(*bits))),
            Datum::I64(v) => Some(*v as f64),
            Datum::I32(v) => Some(f64::from(*v)),
            Datum::I16(v) => Some(f64::from(*v)),
            _ => None,
        }
    }

    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Greater,
        (_, Null) => Ordering::Less,
        _ => {
            if let (Some(x), Some(y)) = (as_i128(a), as_i128(b)) {
                return x.cmp(&y);
            }
            if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
                return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
            }
            match (a, b) {
                (Text(x), Text(y)) => x.cmp(y),
                (Uuid(x), Uuid(y)) => x.as_bytes().cmp(&y.as_bytes()),
                _ => a.cmp(b),
            }
        }
    }
}

/// Maps a SQL cast target onto the engine's coarse [`ColumnType`]
/// taxonomy — shared with the parser's expression gate so the
/// accepted cast surface and the evaluable one stay identical.
fn cast_target_type(data_type: &DataType) -> Option<ColumnType> {
    ColumnType::from_cast_target(data_type)
}

/// Runtime cast with Postgres-flavoured conversions. Total: values a
/// cast cannot convert become `Datum::Null` (the evaluator's stand-in
/// for a runtime error, consistent with the rest of this module).
#[allow(clippy::cast_possible_truncation)]
fn cast_datum(datum: Datum, target: ColumnType) -> Datum {
    use Datum::{Bool, Json, Jsonb, Null, Numeric, Text, Uuid, F32, F64, I16, I32, I64};

    fn text_of(datum: &Datum) -> Option<String> {
        match datum {
            Text(bytes) => std::str::from_utf8(bytes).ok().map(str::to_owned),
            I64(v) => Some(v.to_string()),
            I32(v) => Some(v.to_string()),
            I16(v) => Some(v.to_string()),
            F64(bits) => Some(f64::from_bits(*bits).to_string()),
            F32(bits) => Some(f32::from_bits(*bits).to_string()),
            Bool(v) => Some(v.to_string()),
            Uuid(v) => Some(v.to_string()),
            Numeric(v) => Some(v.as_str().to_owned()),
            Jsonb(bytes) | Json(bytes) => std::str::from_utf8(bytes).ok().map(str::to_owned),
            _ => None,
        }
    }

    if matches!(datum, Null) {
        return Null;
    }
    match target {
        ColumnType::Text | ColumnType::Enum => {
            text_of(&datum).map_or(Null, |text| Text(text.into_bytes().into()))
        }
        ColumnType::Int => match &datum {
            I64(v) => I64(*v),
            I32(v) => I64(i64::from(*v)),
            I16(v) => I64(i64::from(*v)),
            // Postgres rounds float → int casts to the nearest integer.
            F64(bits) => I64(f64::from_bits(*bits).round() as i64),
            F32(bits) => I64(f32::from_bits(*bits).round() as i64),
            Bool(v) => I64(i64::from(*v)),
            Text(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.trim().parse::<i64>().ok())
                .map_or(Null, I64),
            _ => Null,
        },
        ColumnType::Float => match &datum {
            F64(bits) => F64(*bits),
            F32(bits) => F64(f64::from(f32::from_bits(*bits)).to_bits()),
            I64(v) => F64((*v as f64).to_bits()),
            I32(v) => F64(f64::from(*v).to_bits()),
            I16(v) => F64(f64::from(*v).to_bits()),
            Text(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.trim().parse::<f64>().ok())
                .map_or(Null, |v| F64(v.to_bits())),
            _ => Null,
        },
        ColumnType::Bool => match &datum {
            Bool(v) => Bool(*v),
            I64(v) => Bool(*v != 0),
            I32(v) => Bool(*v != 0),
            I16(v) => Bool(*v != 0),
            Text(bytes) => match std::str::from_utf8(bytes)
                .map(|text| text.trim().to_ascii_lowercase())
            {
                Ok(text) if ["t", "true", "yes", "on", "1"].contains(&text.as_str()) => Bool(true),
                Ok(text) if ["f", "false", "no", "off", "0"].contains(&text.as_str()) => {
                    Bool(false)
                }
                _ => Null,
            },
            _ => Null,
        },
        ColumnType::Uuid => match &datum {
            Uuid(v) => Uuid(*v),
            Text(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(palimpsest_wal::Uuid::parse_text)
                .map_or(Null, Uuid),
            _ => Null,
        },
        ColumnType::Jsonb => match datum {
            Jsonb(bytes) | Json(bytes) => Jsonb(bytes),
            Text(bytes) => Jsonb(bytes),
            _ => Null,
        },
        ColumnType::Timestamp | ColumnType::TimestampTz => match datum {
            timestamp @ (Datum::Timestamp(_) | Datum::TimestampTz(_)) => timestamp,
            Datum::Date(date) => Datum::Timestamp(palimpsest_wal::Timestamp {
                micros_since_unix_epoch: i64::from(date.days_since_unix_epoch) * 86_400_000_000,
            }),
            Text(_) => parse_temporal_text(
                &datum,
                if target == ColumnType::TimestampTz {
                    &palimpsest_wal::DatumType::TimestampTz
                } else {
                    &palimpsest_wal::DatumType::Timestamp
                },
            ),
            _ => Null,
        },
        ColumnType::Date => match datum {
            date @ Datum::Date(_) => date,
            Datum::Timestamp(ts) => Datum::Date(palimpsest_wal::Date {
                days_since_unix_epoch: ts.micros_since_unix_epoch.div_euclid(86_400_000_000) as i32,
            }),
            Datum::TimestampTz(ts) => Datum::Date(palimpsest_wal::Date {
                days_since_unix_epoch: ts.micros_since_unix_epoch.div_euclid(86_400_000_000) as i32,
            }),
            Text(_) => parse_temporal_text(&datum, &palimpsest_wal::DatumType::Date),
            _ => Null,
        },
        ColumnType::Time => match datum {
            time @ Datum::Time(_) => time,
            Text(_) => parse_temporal_text(&datum, &palimpsest_wal::DatumType::Time),
            _ => Null,
        },
        ColumnType::Interval => match datum {
            interval @ Datum::Interval(_) => interval,
            Text(_) => parse_temporal_text(&datum, &palimpsest_wal::DatumType::Interval),
            _ => Null,
        },
        ColumnType::Numeric => match &datum {
            Numeric(_) => datum,
            I64(_) | I32(_) | I16(_) | F64(_) | F32(_) | Text(_) => text_of(&datum)
                .map_or(Null, |text| {
                    Numeric(palimpsest_wal::BigDecimal::new(text))
                }),
            _ => Null,
        },
        ColumnType::Bytea => match datum {
            bytea @ Datum::Bytea(_) => bytea,
            Text(bytes) => Datum::Bytea(bytes),
            _ => Null,
        },
        ColumnType::Array => match datum {
            array @ Datum::Array(_) => array,
            _ => Null,
        },
        ColumnType::Unknown => datum,
    }
}

/// Parses a text datum into the requested temporal type via the WAL
/// crate's Postgres-format decoders. `Null` on any parse failure,
/// consistent with the evaluator's total-function contract.
fn parse_temporal_text(datum: &Datum, target: &palimpsest_wal::DatumType) -> Datum {
    let Datum::Text(bytes) = datum else {
        return Datum::Null;
    };
    palimpsest_wal::decode_column_value(
        target,
        palimpsest_wal::ColumnValue::Text(bytes.clone()),
    )
    .unwrap_or(Datum::Null)
}

/// `expr <op> ANY(array)` — true when the comparison holds for at least
/// one array element. This is the shape materialized permission
/// predicates take for list-valued user context
/// (`team_id = ANY(ARRAY[1, 2])`), as well as user queries using
/// `ANY`/`SOME` over an array literal.
fn any_scalar(
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    schema: &ScalarSchema,
) -> Result<ScalarFn, EvalError> {
    let l = compile_inner(left, schema)?;
    let elements: Vec<ScalarFn> = match right {
        Expr::Array(array) => array
            .elem
            .iter()
            .map(|element| compile_inner(element, schema))
            .collect::<Result<_, _>>()?,
        // Postgres array-literal text: `ANY('{1,2,3}')`.
        Expr::Value(SqlValue::SingleQuotedString(text)) => parse_array_literal(text)?
            .into_iter()
            .map(|datum| {
                let scalar: ScalarFn = Box::new(move |_| datum.clone());
                Ok(scalar)
            })
            .collect::<Result<_, EvalError>>()?,
        other => {
            return Err(EvalError::Unsupported(format!(
                "ANY over non-array expression {other:?}"
            )))
        }
    };
    let op = compare_op.clone();
    // Validate the operator once at compile time.
    compare_datums(&op, &Datum::Null, &Datum::Null)
        .ok_or_else(|| EvalError::Unsupported(format!("ANY with operator {op:?}")))?;
    Ok(Box::new(move |row| {
        let lv = l(row);
        let hit = elements
            .iter()
            .any(|element| compare_datums(&op, &lv, &element(row)) == Some(true));
        Datum::Bool(hit)
    }))
}

/// Parses a Postgres array-literal string (`{1,2,3}`, `{a,"b c"}`)
/// into constant datums: integers, floats, booleans, NULLs, or text.
fn parse_array_literal(text: &str) -> Result<Vec<Datum>, EvalError> {
    let inner = text
        .trim()
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| EvalError::Parse(format!("array literal {text:?}")))?;
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = inner.chars();
    let mut quoted = false;
    let flush = |current: &mut String, quoted: &mut bool, elements: &mut Vec<Datum>| {
        let raw = current.trim();
        if raw.is_empty() && !*quoted {
            return;
        }
        let datum = if *quoted {
            Datum::Text(current.clone().into_bytes().into())
        } else if raw.eq_ignore_ascii_case("null") {
            Datum::Null
        } else if let Ok(v) = raw.parse::<i64>() {
            Datum::I64(v)
        } else if let Ok(v) = raw.parse::<f64>() {
            Datum::F64(v.to_bits())
        } else if raw.eq_ignore_ascii_case("true") || raw == "t" {
            Datum::Bool(true)
        } else if raw.eq_ignore_ascii_case("false") || raw == "f" {
            Datum::Bool(false)
        } else {
            Datum::Text(raw.to_owned().into_bytes().into())
        };
        elements.push(datum);
        current.clear();
        *quoted = false;
    };
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                quoted = true;
            }
            '\\' if in_quotes => {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
            }
            ',' if !in_quotes => flush(&mut current, &mut quoted, &mut elements),
            _ => current.push(c),
        }
    }
    if in_quotes {
        return Err(EvalError::Parse(format!("array literal {text:?}")));
    }
    flush(&mut current, &mut quoted, &mut elements);
    Ok(elements)
}

/// Scalar function calls. The SQL frontend rejects functions outside
/// this set at parse time (`SqlError::UnsupportedFunction`); the match
/// here is the evaluation half of that same allowlist.
fn function_scalar(
    function: &sqlparser::ast::Function,
    schema: &ScalarSchema,
) -> Result<ScalarFn, EvalError> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

    let name = function.name.to_string().to_ascii_lowercase();
    let args: Vec<&Expr> = match &function.args {
        FunctionArguments::None => Vec::new(),
        FunctionArguments::Subquery(_) => {
            return Err(EvalError::Unsupported(
                "subquery as function argument".to_owned(),
            ))
        }
        FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
                other => Err(EvalError::Unsupported(format!(
                    "function argument {other:?}"
                ))),
            })
            .collect::<Result<_, _>>()?,
    };

    match name.as_str() {
        "coalesce" => {
            let compiled: Vec<ScalarFn> = args
                .iter()
                .map(|arg| compile_inner(arg, schema))
                .collect::<Result<_, _>>()?;
            Ok(Box::new(move |row| {
                for arg in &compiled {
                    let value = arg(row);
                    if !matches!(value, Datum::Null) {
                        return value;
                    }
                }
                Datum::Null
            }))
        }
        "cardinality" => {
            let [arg] = args.as_slice() else {
                return Err(EvalError::Unsupported(format!(
                    "cardinality expects 1 argument, got {}",
                    args.len()
                )));
            };
            let compiled = compile_inner(arg, schema)?;
            Ok(Box::new(move |row| match compiled(row) {
                Datum::Array(items) => Datum::I64(i64::try_from(items.len()).unwrap_or(i64::MAX)),
                _ => Datum::Null,
            }))
        }
        other => Err(EvalError::Unsupported(format!("function {other}"))),
    }
}

/// Applies a comparison operator with the module's three-valued
/// semantics. `None` means the operator itself is unsupported.
fn compare_datums(op: &BinaryOperator, a: &Datum, b: &Datum) -> Option<bool> {
    match op {
        BinaryOperator::Eq => Some(datum_eq(a, b)),
        BinaryOperator::NotEq => {
            // NULL on either side never matches, mirroring `datum_eq`.
            if matches!(a, Datum::Null) || matches!(b, Datum::Null) {
                Some(false)
            } else {
                Some(!datum_eq(a, b))
            }
        }
        BinaryOperator::Lt => Some(matches!(
            datum_cmp_bool(a, b, |o| o.is_lt()),
            Datum::Bool(true)
        )),
        BinaryOperator::LtEq => Some(matches!(
            datum_cmp_bool(a, b, |o| o.is_le()),
            Datum::Bool(true)
        )),
        BinaryOperator::Gt => Some(matches!(
            datum_cmp_bool(a, b, |o| o.is_gt()),
            Datum::Bool(true)
        )),
        BinaryOperator::GtEq => Some(matches!(
            datum_cmp_bool(a, b, |o| o.is_ge()),
            Datum::Bool(true)
        )),
        _ => None,
    }
}

fn identifier_scalar(name: &str, schema: &ScalarSchema) -> Result<ScalarFn, EvalError> {
    let idx = schema
        .index_of(name)
        .ok_or_else(|| EvalError::UnknownColumn(name.to_owned()))?;
    Ok(Box::new(move |row| {
        row.get(idx).cloned().unwrap_or(Datum::Null)
    }))
}

fn value_scalar(value: &SqlValue) -> Result<ScalarFn, EvalError> {
    match value {
        SqlValue::Boolean(b) => {
            let b = *b;
            Ok(Box::new(move |_| Datum::Bool(b)))
        }
        SqlValue::Number(n, _) => {
            if let Ok(v) = n.parse::<i64>() {
                Ok(Box::new(move |_| Datum::I64(v)))
            } else if let Ok(v) = n.parse::<f64>() {
                let bits = v.to_bits();
                Ok(Box::new(move |_| Datum::F64(bits)))
            } else {
                Err(EvalError::Parse(format!("number literal '{n}'")))
            }
        }
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s)
        | SqlValue::UnicodeStringLiteral(s)
        | SqlValue::NationalStringLiteral(s) => {
            let bytes: bytes::Bytes = s.clone().into_bytes().into();
            Ok(Box::new(move |_| Datum::Text(bytes.clone())))
        }
        SqlValue::Null => Ok(Box::new(|_| Datum::Null)),
        other => Err(EvalError::Unsupported(format!("literal {other:?}"))),
    }
}

fn binary_scalar(
    left: &Expr,
    op: BinaryOperator,
    right: &Expr,
    schema: &ScalarSchema,
) -> Result<ScalarFn, EvalError> {
    let l = compile_inner(left, schema)?;
    let r = compile_inner(right, schema)?;
    match op {
        BinaryOperator::Eq => Ok(Box::new(move |row| Datum::Bool(datum_eq(&l(row), &r(row))))),
        BinaryOperator::NotEq => Ok(Box::new(move |row| {
            Datum::Bool(!datum_eq(&l(row), &r(row)))
        })),
        BinaryOperator::Lt => Ok(Box::new(move |row| {
            datum_cmp_bool(&l(row), &r(row), |o| o.is_lt())
        })),
        BinaryOperator::LtEq => Ok(Box::new(move |row| {
            datum_cmp_bool(&l(row), &r(row), |o| o.is_le())
        })),
        BinaryOperator::Gt => Ok(Box::new(move |row| {
            datum_cmp_bool(&l(row), &r(row), |o| o.is_gt())
        })),
        BinaryOperator::GtEq => Ok(Box::new(move |row| {
            datum_cmp_bool(&l(row), &r(row), |o| o.is_ge())
        })),
        BinaryOperator::And => Ok(Box::new(move |row| {
            let lv = matches!(l(row), Datum::Bool(true));
            if !lv {
                return Datum::Bool(false);
            }
            Datum::Bool(matches!(r(row), Datum::Bool(true)))
        })),
        BinaryOperator::Or => Ok(Box::new(move |row| {
            let lv = matches!(l(row), Datum::Bool(true));
            if lv {
                return Datum::Bool(true);
            }
            Datum::Bool(matches!(r(row), Datum::Bool(true)))
        })),
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => Ok(Box::new(move |row| {
            arithmetic_datums(&op, &l(row), &r(row))
        })),
        // Postgres `text || anynonarray`: either side coerces to text;
        // NULL on either side yields NULL.
        BinaryOperator::StringConcat => Ok(Box::new(move |row| {
            let (left, right) = (l(row), r(row));
            if matches!(left, Datum::Null) || matches!(right, Datum::Null) {
                return Datum::Null;
            }
            match (datum_display_text(&left), datum_display_text(&right)) {
                (Some(mut text), Some(rest)) => {
                    text.push_str(&rest);
                    Datum::Text(text.into_bytes().into())
                }
                _ => Datum::Null,
            }
        })),
        other => Err(EvalError::Unsupported(format!("binary op {other:?}"))),
    }
}

/// Text rendering shared by `||` and text casts.
fn datum_display_text(datum: &Datum) -> Option<String> {
    match datum {
        Datum::Text(bytes) => std::str::from_utf8(bytes).ok().map(str::to_owned),
        Datum::I64(v) => Some(v.to_string()),
        Datum::I32(v) => Some(v.to_string()),
        Datum::I16(v) => Some(v.to_string()),
        Datum::F64(bits) => Some(f64::from_bits(*bits).to_string()),
        Datum::F32(bits) => Some(f32::from_bits(*bits).to_string()),
        Datum::Bool(v) => Some(v.to_string()),
        Datum::Uuid(v) => Some(v.to_string()),
        Datum::Numeric(v) => Some(v.as_str().to_owned()),
        Datum::Jsonb(bytes) | Datum::Json(bytes) => {
            std::str::from_utf8(bytes).ok().map(str::to_owned)
        }
        _ => None,
    }
}

/// Numeric arithmetic with SQL semantics: integer op integer stays
/// integer (Postgres integer division truncates), any float operand
/// promotes to float, NULL / non-numeric operands and division by
/// zero yield `NULL` (the evaluator's stand-in for a runtime error).
fn arithmetic_datums(op: &BinaryOperator, a: &Datum, b: &Datum) -> Datum {
    fn as_i64(datum: &Datum) -> Option<i64> {
        match datum {
            Datum::I64(v) => Some(*v),
            Datum::I32(v) => Some(i64::from(*v)),
            Datum::I16(v) => Some(i64::from(*v)),
            _ => None,
        }
    }
    #[allow(clippy::cast_precision_loss)]
    fn as_f64(datum: &Datum) -> Option<f64> {
        match datum {
            Datum::F64(bits) => Some(f64::from_bits(*bits)),
            Datum::F32(bits) => Some(f64::from(f32::from_bits(*bits))),
            Datum::I64(v) => Some(*v as f64),
            Datum::I32(v) => Some(f64::from(*v)),
            Datum::I16(v) => Some(f64::from(*v)),
            _ => None,
        }
    }

    if let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) {
        let result = match op {
            BinaryOperator::Plus => x.checked_add(y),
            BinaryOperator::Minus => x.checked_sub(y),
            BinaryOperator::Multiply => x.checked_mul(y),
            BinaryOperator::Divide => x.checked_div(y),
            BinaryOperator::Modulo => x.checked_rem(y),
            _ => None,
        };
        return result.map_or(Datum::Null, Datum::I64);
    }
    if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
        let result = match op {
            BinaryOperator::Plus => x + y,
            BinaryOperator::Minus => x - y,
            BinaryOperator::Multiply => x * y,
            BinaryOperator::Divide => {
                if y == 0.0 {
                    return Datum::Null;
                }
                x / y
            }
            BinaryOperator::Modulo => {
                if y == 0.0 {
                    return Datum::Null;
                }
                x % y
            }
            _ => return Datum::Null,
        };
        return Datum::F64(result.to_bits());
    }
    Datum::Null
}

fn unary_scalar(
    op: UnaryOperator,
    inner: &Expr,
    schema: &ScalarSchema,
) -> Result<ScalarFn, EvalError> {
    let e = compile_inner(inner, schema)?;
    match op {
        UnaryOperator::Not => Ok(Box::new(move |row| match e(row) {
            Datum::Bool(b) => Datum::Bool(!b),
            _ => Datum::Bool(false),
        })),
        UnaryOperator::Minus => Ok(Box::new(move |row| match e(row) {
            Datum::I64(v) => Datum::I64(-v),
            Datum::I32(v) => Datum::I32(-v),
            Datum::I16(v) => Datum::I16(-v),
            // `Datum::F{32,64}` store the bit pattern of the float
            // rather than the float itself, so negation has to round-
            // trip through the IEEE representation.
            Datum::F64(v) => Datum::F64((-f64::from_bits(v)).to_bits()),
            Datum::F32(v) => Datum::F32((-f32::from_bits(v)).to_bits()),
            other => other,
        })),
        UnaryOperator::Plus => Ok(e),
        other => Err(EvalError::Unsupported(format!("unary op {other:?}"))),
    }
}

/// SQL equality with three-valued logic: NULL on either side → false.
fn datum_eq(a: &Datum, b: &Datum) -> bool {
    use Datum::{Bool, Json, Jsonb, Null, Text, Uuid, F32, F64, I16, I32, I64};
    match (a, b) {
        (Null, _) | (_, Null) => false,
        (Bool(x), Bool(y)) => x == y,
        (I64(x), I64(y)) => x == y,
        (I32(x), I32(y)) => x == y,
        (I16(x), I16(y)) => x == y,
        // `F32`/`F64` store IEEE bit patterns; comparing the integer
        // backing types gives canonical-bit equality rather than the
        // float equality SQL expects. Decode before comparing.
        (F64(x), F64(y)) => f64::from_bits(*x) == f64::from_bits(*y),
        (F32(x), F32(y)) => f32::from_bits(*x) == f32::from_bits(*y),
        (I64(x), I32(y)) => *x == i64::from(*y),
        (I32(x), I64(y)) => i64::from(*x) == *y,
        (I64(x), I16(y)) => *x == i64::from(*y),
        (I16(x), I64(y)) => i64::from(*x) == *y,
        (I32(x), I16(y)) => *x == i32::from(*y),
        (I16(x), I32(y)) => i32::from(*x) == *y,
        (Text(x), Text(y)) => x == y,
        (Uuid(x), Uuid(y)) => x == y,
        // Permission rewriting materializes uuid user values as quoted
        // string literals, so uuid columns must compare against text.
        (Uuid(u), Text(t)) | (Text(t), Uuid(u)) => parse_uuid_text(t) == Some(*u),
        // jsonb equality is structural, both between documents and
        // against a text literal carrying a JSON document.
        (Jsonb(x) | Json(x), Jsonb(y) | Json(y)) => json_eq(x, y),
        (Jsonb(x) | Json(x), Text(t)) | (Text(t), Jsonb(x) | Json(x)) => json_eq(x, t),
        _ => false,
    }
}

fn parse_uuid_text(raw: &[u8]) -> Option<palimpsest_wal::Uuid> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(palimpsest_wal::Uuid::parse_text)
}

/// Structural JSON equality; malformed input on either side → false.
fn json_eq(a: &[u8], b: &[u8]) -> bool {
    match (
        serde_json::from_slice::<serde_json::Value>(a),
        serde_json::from_slice::<serde_json::Value>(b),
    ) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// SQL ordering with three-valued logic: NULL on either side → false.
fn datum_cmp_bool<F>(a: &Datum, b: &Datum, pick: F) -> Datum
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    use std::cmp::Ordering;
    use Datum::{Null, Text, Uuid, F64, I16, I32, I64};
    let ord = match (a, b) {
        (Null, _) | (_, Null) => return Datum::Bool(false),
        (I64(x), I64(y)) => x.cmp(y),
        (I32(x), I32(y)) => x.cmp(y),
        (I16(x), I16(y)) => x.cmp(y),
        (F64(x), F64(y)) => f64::from_bits(*x)
            .partial_cmp(&f64::from_bits(*y))
            .unwrap_or(Ordering::Equal),
        (I64(x), I32(y)) => x.cmp(&i64::from(*y)),
        (I32(x), I64(y)) => i64::from(*x).cmp(y),
        (Text(x), Text(y)) => x.cmp(y),
        // Bytewise, matching Postgres uuid ordering.
        (Uuid(x), Uuid(y)) => x.as_bytes().cmp(&y.as_bytes()),
        (Uuid(u), Text(t)) => match parse_uuid_text(t) {
            Some(parsed) => u.as_bytes().cmp(&parsed.as_bytes()),
            None => return Datum::Bool(false),
        },
        (Text(t), Uuid(u)) => match parse_uuid_text(t) {
            Some(parsed) => parsed.as_bytes().cmp(&u.as_bytes()),
            None => return Datum::Bool(false),
        },
        _ => return Datum::Bool(false),
    };
    Datum::Bool(pick(ord))
}

impl fmt::Display for ScalarSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        for (i, (name, ty)) in self.columns.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{name}: {ty:?}")?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn posts_schema() -> ScalarSchema {
        ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("title".to_owned(), ColumnType::Text),
            ("published".to_owned(), ColumnType::Bool),
        ])
    }

    fn text(s: &str) -> Datum {
        Datum::Text(s.as_bytes().to_vec().into())
    }

    #[test]
    fn column_ref_extracts_value() {
        let schema = posts_schema();
        let f = compile_scalar("published", &schema).unwrap();
        let r: Row = smallvec![Datum::I64(1), text("hi"), Datum::Bool(true)];
        assert_eq!(f(&r), Datum::Bool(true));
    }

    #[test]
    fn predicate_equality_against_literal() {
        let schema = posts_schema();
        let p = compile_predicate("published = true", &schema).unwrap();
        let r_pub: Row = smallvec![Datum::I64(1), text("a"), Datum::Bool(true)];
        let r_draft: Row = smallvec![Datum::I64(2), text("b"), Datum::Bool(false)];
        assert!(p(&r_pub));
        assert!(!p(&r_draft));
    }

    #[test]
    fn predicate_or_short_circuits() {
        let schema = posts_schema();
        let p = compile_predicate("published = true OR id = 99", &schema).unwrap();
        let draft_99: Row = smallvec![Datum::I64(99), text("c"), Datum::Bool(false)];
        assert!(p(&draft_99));
    }

    #[test]
    fn predicate_with_inlined_admin_literal() {
        // After permission rewriting, $user.is_admin becomes a literal —
        // this is exactly the predicate dataflow operators see.
        let schema = posts_schema();
        let p = compile_predicate("published = true OR true = true", &schema).unwrap();
        let r: Row = smallvec![Datum::I64(1), text("x"), Datum::Bool(false)];
        assert!(p(&r));
    }

    #[test]
    fn predicate_ordering() {
        let schema = posts_schema();
        let p = compile_predicate("id < 5", &schema).unwrap();
        let small: Row = smallvec![Datum::I64(3), text(""), Datum::Bool(true)];
        let large: Row = smallvec![Datum::I64(7), text(""), Datum::Bool(true)];
        assert!(p(&small));
        assert!(!p(&large));
    }

    #[test]
    fn uuid_column_compares_against_text_literal() {
        // This is the exact shape a materialized `$user.tenant_id`
        // permission filter takes: uuid column vs quoted literal.
        let schema = ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("tenant_id".to_owned(), ColumnType::Uuid),
        ]);
        let tenant = palimpsest_wal::Uuid::parse_text("67e55044-10b1-426f-9247-bb680e5fe0c8")
            .expect("valid uuid");
        let p = compile_predicate(
            "tenant_id = '67e55044-10b1-426f-9247-bb680e5fe0c8'",
            &schema,
        )
        .unwrap();
        let matching: Row = smallvec![Datum::I64(1), Datum::Uuid(tenant)];
        let other = palimpsest_wal::Uuid::from_bytes([9; 16]);
        let non_matching: Row = smallvec![Datum::I64(2), Datum::Uuid(other)];
        assert!(p(&matching));
        assert!(!p(&non_matching));
    }

    #[test]
    fn jsonb_column_compares_structurally_against_text_literal() {
        let schema = ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("prefs".to_owned(), ColumnType::Jsonb),
        ]);
        let p = compile_predicate(r#"prefs = '{"a":1,"b":2}'"#, &schema).unwrap();
        // Key order and whitespace in the stored document don't matter.
        let stored = Datum::Jsonb(br#"{ "b": 2, "a": 1 }"#.to_vec().into());
        let matching: Row = smallvec![Datum::I64(1), stored];
        let non_matching: Row =
            smallvec![Datum::I64(2), Datum::Jsonb(br#"{"a":1}"#.to_vec().into())];
        assert!(p(&matching));
        assert!(!p(&non_matching));
    }

    #[test]
    fn enum_labels_travel_as_text_and_compare_against_literals() {
        let schema = ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("role".to_owned(), ColumnType::Enum),
        ]);
        let p = compile_predicate("role = 'admin'", &schema).unwrap();
        let admin: Row = smallvec![Datum::I64(1), text("admin")];
        let viewer: Row = smallvec![Datum::I64(2), text("viewer")];
        assert!(p(&admin));
        assert!(!p(&viewer));
    }

    #[test]
    fn any_over_array_literal_tests_membership() {
        // Materialized form of `team_id = ANY($user.team_ids)` with
        // `team_ids = [1, 3]`.
        let schema = ScalarSchema::from_pairs([
            ("id".to_owned(), ColumnType::Int),
            ("team_id".to_owned(), ColumnType::Int),
        ]);
        let p = compile_predicate("team_id = ANY(ARRAY[1, 3])", &schema).unwrap();
        let team1: Row = smallvec![Datum::I64(10), Datum::I64(1)];
        let team2: Row = smallvec![Datum::I64(11), Datum::I64(2)];
        let team3: Row = smallvec![Datum::I64(12), Datum::I64(3)];
        assert!(p(&team1));
        assert!(!p(&team2));
        assert!(p(&team3));
    }

    #[test]
    fn any_over_empty_array_matches_nothing() {
        let schema = posts_schema();
        let p = compile_predicate("id = ANY(ARRAY[])", &schema).unwrap();
        let r: Row = smallvec![Datum::I64(1), text(""), Datum::Bool(true)];
        assert!(!p(&r));
    }

    #[test]
    fn any_with_ordering_operator() {
        let schema = posts_schema();
        let p = compile_predicate("id < ANY(ARRAY[5, 2])", &schema).unwrap();
        let three: Row = smallvec![Datum::I64(3), text(""), Datum::Bool(true)];
        let nine: Row = smallvec![Datum::I64(9), text(""), Datum::Bool(true)];
        assert!(p(&three));
        assert!(!p(&nine));
    }

    #[test]
    fn any_over_text_array() {
        let schema = ScalarSchema::from_pairs([("role".to_owned(), ColumnType::Text)]);
        let p = compile_predicate("role = ANY(ARRAY['admin', 'editor'])", &schema).unwrap();
        let admin: Row = smallvec![text("admin")];
        let viewer: Row = smallvec![text("viewer")];
        assert!(p(&admin));
        assert!(!p(&viewer));
    }

    #[test]
    fn coalesce_returns_first_non_null() {
        let schema = posts_schema();
        let f = compile_scalar("coalesce(title, 'fallback')", &schema).unwrap();
        let named: Row = smallvec![Datum::I64(1), text("hello"), Datum::Bool(true)];
        let unnamed: Row = smallvec![Datum::I64(2), Datum::Null, Datum::Bool(true)];
        assert_eq!(f(&named), text("hello"));
        assert_eq!(f(&unnamed), text("fallback"));
        let all_null = compile_scalar("coalesce(NULL, NULL)", &schema).unwrap();
        assert_eq!(all_null(&named), Datum::Null);
    }

    #[test]
    fn coalesce_fallback_literal_coerces_to_the_inferred_column_type() {
        // Regression: `coalesce(uuid_col, '<literal>')` advertised
        // `Uuid` but served a text datum when the fallback fired,
        // killing the subscription with a client-side schema
        // mismatch. The adopter workaround was a `::uuid` cast written
        // into the query text.
        let schema = ScalarSchema::from_pairs([("id".to_owned(), ColumnType::Uuid)]);
        let (scalar, ty) = compile_typed_scalar(
            "coalesce(id, 'a5e9e2c0-0000-4000-8000-000000000042')",
            &schema,
        )
        .unwrap();
        assert_eq!(ty, ColumnType::Uuid);

        let missing: Row = smallvec![Datum::Null];
        match scalar(&missing) {
            Datum::Uuid(uuid) => assert_eq!(
                uuid.to_string(),
                "a5e9e2c0-0000-4000-8000-000000000042",
                "fallback literal must be served as the advertised type"
            ),
            other => panic!("expected a uuid datum, got {other:?}"),
        }

        let present: Row = smallvec![Datum::Uuid(palimpsest_wal::Uuid::from_bytes([7; 16]))];
        assert!(matches!(scalar(&present), Datum::Uuid(_)));
    }

    #[test]
    fn unsupported_function_rejected_at_compile_time() {
        let schema = posts_schema();
        let Err(err) = compile_scalar("upper(title)", &schema) else {
            panic!("expected compile failure on unsupported function");
        };
        assert!(matches!(err, EvalError::Unsupported(_)));
    }

    #[test]
    fn unknown_column_rejected_at_compile_time() {
        let schema = posts_schema();
        // PredicateFn isn't Debug, so we destructure the Err arm manually.
        let Err(err) = compile_predicate("ghost = 1", &schema) else {
            panic!("expected compile failure on unknown column");
        };
        assert!(matches!(err, EvalError::UnknownColumn(_)));
    }

    #[test]
    fn int_extractor_handles_star() {
        let schema = posts_schema();
        let f = compile_int_extractor("*", &schema).unwrap();
        let r: Row = smallvec![Datum::I64(42), text(""), Datum::Bool(true)];
        assert_eq!(f(&r), 0);
    }

    #[test]
    fn int_extractor_reads_named_column() {
        let schema = posts_schema();
        let f = compile_int_extractor("id", &schema).unwrap();
        let r: Row = smallvec![Datum::I64(42), text(""), Datum::Bool(true)];
        assert_eq!(f(&r), 42);
    }
}
