// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Compile a [`PermissionRule`] predicate into a canonical MIR fragment.
//!
//! The user-facing predicate is a SQL boolean expression that may
//! reference `$user.<field>` placeholders. Compilation does three things:
//!
//! 1. Substitutes each `$user.<field>` with a sentinel identifier so the
//!    text parses as ordinary SQL.
//! 2. Lowers `SELECT 1 FROM <table> WHERE <predicate>` through
//!    [`palimpsest_sql::parse_and_lower`] and extracts the canonicalized
//!    predicate from the resulting `Filter` node.
//! 3. Validates that every column reference matches the catalog and every
//!    `$user.*` reference is declared in the schema.
//!
//! The canonical predicate string contains the sentinels and is later
//! materialized against a concrete `UserContext` by the rewriter.

use std::collections::BTreeSet;

use palimpsest_sql::{
    lower::parse_and_lower,
    mir::{MirGraph, MirNodeKind},
    Catalog,
};

use crate::{
    error::PermissionError,
    rule::PermissionRule,
    user::{UserContext, UserContextSchema},
};

/// Sentinel prefix used to encode `$user.<field>` references as plain
/// SQL identifiers. The rewriter substitutes each occurrence with the
/// caller-provided `UserContext` value before installing the filter.
pub const USER_PLACEHOLDER_PREFIX: &str = "__palimpsest_user_";
const USER_PLACEHOLDER_SUFFIX: &str = "__";

/// Maximum AST depth permitted in a compiled rule predicate.
///
/// Depth is the longest path through nested expressions (binary ops,
/// `NOT`, parenthesised subexpressions). Set generously enough that
/// real predicates pass; anything beyond this budget is more likely to
/// be the product of a typo or a malicious rule than a useful policy.
pub const MAX_PREDICATE_DEPTH: usize = 32;

/// Compiled form of a permission rule predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPredicate {
    /// Canonical predicate text with `$user.<field>` references encoded
    /// as `__palimpsest_user_<field>__` sentinels.
    pub canonical: String,
    /// Distinct user-context fields the predicate reads, in alphabetical order.
    pub user_fields: BTreeSet<String>,
}

impl CompiledPredicate {
    /// Returns true when the predicate is a tautology and the rewriter
    /// should elide the filter entirely.
    #[must_use]
    pub fn is_tautology(&self) -> bool {
        let trimmed = self.canonical.trim();
        trimmed.eq_ignore_ascii_case("true")
    }

    /// Materializes the canonical predicate against a concrete
    /// `UserContext`. Each `__palimpsest_user_<field>__` sentinel is
    /// replaced by the SQL literal of the corresponding value.
    pub fn materialize(&self, context: &UserContext) -> Result<String, PermissionError> {
        let mut output = String::with_capacity(self.canonical.len());
        let mut remaining = self.canonical.as_str();
        while let Some(start) = remaining.find(USER_PLACEHOLDER_PREFIX) {
            output.push_str(&remaining[..start]);
            let after_prefix = &remaining[start + USER_PLACEHOLDER_PREFIX.len()..];
            let suffix = after_prefix.find(USER_PLACEHOLDER_SUFFIX).ok_or_else(|| {
                PermissionError::MalformedUserReference {
                    rule: String::new(),
                    fragment: format!("{USER_PLACEHOLDER_PREFIX}{after_prefix}"),
                }
            })?;
            let field = &after_prefix[..suffix];
            let value = context
                .get(field)
                .ok_or_else(|| PermissionError::MissingUserValue(field.to_owned()))?;
            output.push_str(&value.to_sql_literal());
            remaining = &after_prefix[suffix + USER_PLACEHOLDER_SUFFIX.len()..];
        }
        output.push_str(remaining);
        Ok(output)
    }

    /// Stable canonical representation used by the canonical-key hash.
    /// Includes the predicate template plus the values bound to each
    /// referenced user field, so two users with identical bindings
    /// share canonical keys.
    pub fn canonical_with_context(&self, context: &UserContext) -> Result<String, PermissionError> {
        let mut bindings = String::new();
        for field in &self.user_fields {
            let value = context
                .get(field)
                .ok_or_else(|| PermissionError::MissingUserValue(field.clone()))?;
            bindings.push('|');
            bindings.push_str(field);
            bindings.push('=');
            bindings.push_str(&value.canonical_repr());
        }
        Ok(format!("{}{bindings}", self.canonical))
    }
}

/// Compiled permission rule, keyed by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledRule {
    /// Stable rule name.
    pub name: String,
    /// Target table.
    pub table: String,
    /// Mode the rule is authoritative for.
    pub mode: crate::rule::Mode,
    /// Compiled predicate.
    pub predicate: CompiledPredicate,
}

/// Compile a single rule against the catalog and user-context schema.
///
/// # Errors
/// Surfaces the first [`PermissionError`] encountered.
pub fn compile_rule(
    rule: &PermissionRule,
    catalog: &Catalog,
    schema: &UserContextSchema,
) -> Result<CompiledRule, PermissionError> {
    let table = catalog
        .table(&rule.table)
        .ok_or_else(|| PermissionError::UnknownTable {
            rule: rule.name.clone(),
            table: rule.table.clone(),
        })?;

    let (rewritten, user_fields) = encode_user_placeholders(&rule.predicate, &rule.name, schema)?;
    enforce_predicate_depth(&rewritten, &rule.name)?;

    let lowered = parse_and_lower(&format!("SELECT 1 FROM {} WHERE ({rewritten})", rule.table))?;

    let canonical = extract_filter_predicate(&lowered).ok_or_else(|| {
        PermissionError::InvalidConfig(format!(
            "rule {:?} predicate failed to lower into a Filter node",
            rule.name
        ))
    })?;

    validate_columns(&canonical, &rule.name, table)?;

    Ok(CompiledRule {
        name: rule.name.clone(),
        table: rule.table.clone(),
        mode: rule.mode,
        predicate: CompiledPredicate {
            canonical,
            user_fields,
        },
    })
}

/// Compile each rule in `rules`, rejecting duplicates and ambiguous pairs.
///
/// Two rules over the same `table` whose canonical predicates match are
/// rejected: the loader cannot tell whether they were intended as
/// alternatives or duplicates, so it errs on the side of safety.
///
/// # Errors
/// Surfaces the first [`PermissionError`] encountered.
pub fn compile_rules(
    rules: &[PermissionRule],
    catalog: &Catalog,
    schema: &UserContextSchema,
) -> Result<Vec<CompiledRule>, PermissionError> {
    let mut compiled = Vec::with_capacity(rules.len());
    let mut names = BTreeSet::new();
    for rule in rules {
        if !names.insert(rule.name.clone()) {
            return Err(PermissionError::DuplicateRuleName(rule.name.clone()));
        }
        compiled.push(compile_rule(rule, catalog, schema)?);
    }

    for index in 0..compiled.len() {
        for other in (index + 1)..compiled.len() {
            if compiled[index].table == compiled[other].table
                && compiled[index].predicate.canonical == compiled[other].predicate.canonical
            {
                return Err(PermissionError::AmbiguousRule {
                    first: compiled[index].name.clone(),
                    second: compiled[other].name.clone(),
                    table: compiled[index].table.clone(),
                    predicate: compiled[index].predicate.canonical.clone(),
                });
            }
        }
    }

    Ok(compiled)
}

fn encode_user_placeholders(
    predicate: &str,
    rule: &str,
    schema: &UserContextSchema,
) -> Result<(String, BTreeSet<String>), PermissionError> {
    let mut output = String::with_capacity(predicate.len());
    let mut user_fields = BTreeSet::new();
    let mut chars = predicate.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        match ch {
            '\'' => {
                output.push('\'');
                while let Some((_, inner)) = chars.next() {
                    output.push(inner);
                    if inner == '\'' {
                        // Doubled `''` escape: stay inside the string literal.
                        if matches!(chars.peek(), Some((_, '\''))) {
                            let (_, next) = chars.next().expect("peeked single quote");
                            output.push(next);
                        } else {
                            break;
                        }
                    }
                }
            }
            '$' if predicate[index..].starts_with("$user.") => {
                let after_dot = index + "$user.".len();
                let field_end = predicate[after_dot..]
                    .char_indices()
                    .take_while(|(_, c)| c.is_ascii_alphanumeric() || *c == '_')
                    .map(|(offset, c)| offset + c.len_utf8())
                    .last()
                    .unwrap_or(0);
                if field_end == 0 {
                    return Err(PermissionError::MalformedUserReference {
                        rule: rule.to_owned(),
                        fragment: predicate[index..].to_owned(),
                    });
                }
                let field = &predicate[after_dot..after_dot + field_end];
                if !schema.contains(field) {
                    return Err(PermissionError::UnknownUserField {
                        rule: rule.to_owned(),
                        field: field.to_owned(),
                    });
                }
                output.push_str(USER_PLACEHOLDER_PREFIX);
                output.push_str(field);
                output.push_str(USER_PLACEHOLDER_SUFFIX);
                user_fields.insert(field.to_owned());
                // Consume "user." (5 ASCII chars) plus the field-name chars
                // we already located. The leading '$' was consumed by the
                // outer `match` arm.
                for _ in 0.."user.".len() + field_end {
                    chars.next();
                }
            }
            _ => output.push(ch),
        }
    }

    Ok((output, user_fields))
}

/// Bounds the AST depth of `predicate` (already encoded with
/// `__palimpsest_user_*__` sentinels) so the rewriter can't be made to
/// recurse into pathologically nested boolean expressions.
fn enforce_predicate_depth(predicate: &str, rule: &str) -> Result<(), PermissionError> {
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    let dialect = PostgreSqlDialect {};
    let expr = Parser::new(&dialect)
        .try_with_sql(predicate)
        .and_then(|mut parser| parser.parse_expr())
        .map_err(|err| PermissionError::InvalidConfig(format!("rule {rule:?}: {err}")))?;
    let depth = expr_depth(&expr);
    if depth > MAX_PREDICATE_DEPTH {
        return Err(PermissionError::PredicateTooDeep {
            rule: rule.to_owned(),
            depth,
            limit: MAX_PREDICATE_DEPTH,
        });
    }
    Ok(())
}

fn expr_depth(expr: &sqlparser::ast::Expr) -> usize {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => 1 + expr_depth(inner),
        Expr::BinaryOp { left, right, .. } => 1 + expr_depth(left).max(expr_depth(right)),
        Expr::Between {
            expr, low, high, ..
        } => 1 + expr_depth(expr).max(expr_depth(low)).max(expr_depth(high)),
        Expr::InList { expr, list, .. } => {
            let inner = list.iter().map(expr_depth).max().unwrap_or(0);
            1 + expr_depth(expr).max(inner)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            1 + expr_depth(expr).max(expr_depth(pattern))
        }
        _ => 1,
    }
}

fn extract_filter_predicate(graph: &MirGraph) -> Option<String> {
    for node in graph.node_kinds() {
        if let MirNodeKind::Filter { predicate } = node {
            return Some(predicate.clone());
        }
    }
    None
}

fn validate_columns(
    predicate: &str,
    rule: &str,
    table: &palimpsest_sql::TableSchema,
) -> Result<(), PermissionError> {
    for token in extract_identifiers(predicate) {
        if token.starts_with(USER_PLACEHOLDER_PREFIX) {
            continue;
        }
        if matches!(
            token.to_ascii_lowercase().as_str(),
            "true" | "false" | "null" | "and" | "or" | "not" | "is" | "in" | "between" | "like"
        ) {
            continue;
        }
        if token == table.name {
            continue;
        }
        if let Some((relation, column)) = token.split_once('.') {
            if relation != table.name {
                return Err(PermissionError::UnknownColumn {
                    rule: rule.to_owned(),
                    table: table.name.clone(),
                    column: token,
                });
            }
            if table.column(column).is_none() {
                return Err(PermissionError::UnknownColumn {
                    rule: rule.to_owned(),
                    table: table.name.clone(),
                    column: column.to_owned(),
                });
            }
            continue;
        }
        if table.column(&token).is_none() {
            return Err(PermissionError::UnknownColumn {
                rule: rule.to_owned(),
                table: table.name.clone(),
                column: token,
            });
        }
    }
    Ok(())
}

fn extract_identifiers(predicate: &str) -> Vec<String> {
    let mut identifiers = Vec::new();
    let bytes = predicate.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'\'' {
            index += 1;
            while index < bytes.len() {
                if bytes[index] == b'\'' {
                    if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                        index += 2;
                    } else {
                        index += 1;
                        break;
                    }
                } else {
                    index += 1;
                }
            }
            continue;
        }
        if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            while index < bytes.len() {
                let next = bytes[index];
                if next.is_ascii_alphanumeric() || next == b'_' || next == b'.' {
                    index += 1;
                } else {
                    break;
                }
            }
            let token = &predicate[start..index];
            if !token.chars().all(|ch| ch.is_ascii_digit()) {
                identifiers.push(token.to_owned());
            }
            continue;
        }
        index += 1;
    }
    identifiers
}

/// Stable canonical key for a `(rule, user_context)` pairing.
///
/// The rewriter folds this string into the MIR `Filter` predicate so
/// the canonical-key hash naturally distinguishes users that bind
/// different `$user.*` values.
///
/// # Errors
/// Surfaces [`PermissionError::MissingUserValue`] when `context` is
/// missing a referenced field.
pub fn predicate_key(
    rule: &CompiledRule,
    context: &UserContext,
) -> Result<String, PermissionError> {
    rule.predicate.canonical_with_context(context)
}

#[cfg(test)]
mod tests {
    use palimpsest_sql::{Catalog, ColumnType};

    use super::{compile_rule, compile_rules, encode_user_placeholders, CompiledPredicate};
    use crate::{
        rule::PermissionRule,
        user::{UserContext, UserContextSchema, UserValue},
    };

    fn schema() -> UserContextSchema {
        UserContextSchema::new([
            ("id".to_owned(), ColumnType::Int),
            ("org_id".to_owned(), ColumnType::Int),
            ("is_admin".to_owned(), ColumnType::Bool),
        ])
    }

    #[test]
    fn encode_collects_user_fields_and_replaces_inline() {
        let (rewritten, fields) =
            encode_user_placeholders("author_id = $user.id", "rule", &schema()).unwrap();
        assert!(rewritten.contains("__palimpsest_user_id__"));
        assert_eq!(
            fields.iter().map(String::as_str).collect::<Vec<_>>(),
            ["id"]
        );
    }

    #[test]
    fn encode_skips_user_inside_string_literal() {
        let (rewritten, fields) = encode_user_placeholders(
            "title = '$user.id literal' AND author_id = $user.id",
            "r",
            &schema(),
        )
        .unwrap();
        assert!(rewritten.contains("'$user.id literal'"));
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn encode_rejects_unknown_field() {
        let err = encode_user_placeholders("id = $user.bogus", "r", &schema()).unwrap_err();
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn encode_rejects_malformed_user_reference() {
        let err = encode_user_placeholders("id = $user.", "r", &schema()).unwrap_err();
        assert!(err.to_string().contains("malformed"));
    }

    #[test]
    fn compile_rule_extracts_filter_and_validates_columns() {
        let rule = PermissionRule::new("posts_in_org", "posts", "author_id = $user.id");
        let compiled = compile_rule(&rule, &Catalog::demo(), &schema()).unwrap();
        assert_eq!(compiled.name, "posts_in_org");
        assert_eq!(compiled.table, "posts");
        assert!(compiled
            .predicate
            .canonical
            .contains("__palimpsest_user_id__"));
        assert!(compiled
            .predicate
            .user_fields
            .iter()
            .any(|field| field == "id"));
    }

    #[test]
    fn compile_rule_rejects_unknown_table() {
        let rule = PermissionRule::new("p", "no_such_table", "true");
        let err = compile_rule(&rule, &Catalog::demo(), &schema()).unwrap_err();
        assert!(err.to_string().contains("no_such_table"));
    }

    #[test]
    fn compile_rule_rejects_unknown_column() {
        let rule = PermissionRule::new("p", "posts", "missing_col = 1");
        let err = compile_rule(&rule, &Catalog::demo(), &schema()).unwrap_err();
        assert!(err.to_string().contains("missing_col"));
    }

    #[test]
    fn compile_rule_accepts_tautology() {
        let rule = PermissionRule::new("everything", "posts", "true");
        let compiled = compile_rule(&rule, &Catalog::demo(), &schema()).unwrap();
        assert!(compiled.predicate.is_tautology());
    }

    #[test]
    fn compile_rules_rejects_duplicate_names() {
        let rules = vec![
            PermissionRule::new("dup", "posts", "id = 1"),
            PermissionRule::new("dup", "posts", "id = 2"),
        ];
        let err = compile_rules(&rules, &Catalog::demo(), &schema()).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn compile_rules_rejects_ambiguous_predicate() {
        let rules = vec![
            PermissionRule::new("a", "posts", "author_id = $user.id"),
            PermissionRule::new("b", "posts", "author_id = $user.id"),
        ];
        let err = compile_rules(&rules, &Catalog::demo(), &schema()).unwrap_err();
        assert!(err.to_string().contains("ambiguous"));
    }

    #[test]
    fn materialize_substitutes_user_values() {
        let rule = PermissionRule::new("p", "posts", "author_id = $user.id");
        let compiled = compile_rule(&rule, &Catalog::demo(), &schema()).unwrap();
        let context = UserContext::new([("id".to_owned(), UserValue::Int(42))]);
        let materialized = compiled.predicate.materialize(&context).unwrap();
        assert!(materialized.contains("42"));
        assert!(!materialized.contains("__palimpsest_user_id__"));
    }

    #[test]
    fn rejects_excessively_deep_predicate() {
        let mut predicate = "id = 1".to_owned();
        for _ in 0..super::MAX_PREDICATE_DEPTH + 4 {
            predicate = format!("({predicate} AND id = 1)");
        }
        let rule = PermissionRule::new("deep", "posts", predicate);
        let err = compile_rule(&rule, &Catalog::demo(), &schema()).unwrap_err();
        assert!(matches!(
            err,
            crate::PermissionError::PredicateTooDeep { rule, .. } if rule == "deep"
        ));
    }

    #[test]
    fn canonical_with_context_includes_user_bindings() {
        let predicate = CompiledPredicate {
            canonical: "author_id = __palimpsest_user_id__".to_owned(),
            user_fields: std::iter::once("id".to_owned()).collect(),
        };
        let alice = UserContext::new([("id".to_owned(), UserValue::Int(1))]);
        let bob = UserContext::new([("id".to_owned(), UserValue::Int(2))]);
        assert_ne!(
            predicate.canonical_with_context(&alice).unwrap(),
            predicate.canonical_with_context(&bob).unwrap(),
        );
    }
}
