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
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};
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
        Self { columns, index }
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
            // Treat `table.column` as just `column` for our flat row
            // model. The MIR's BaseTable.project already pinned
            // column ordering, so qualification is informational.
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
        other => Err(EvalError::Unsupported(format!("{other:?}"))),
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
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
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
        other => Err(EvalError::Unsupported(format!("binary op {other:?}"))),
    }
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
