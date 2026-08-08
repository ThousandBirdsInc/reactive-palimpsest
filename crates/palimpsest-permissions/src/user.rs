// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Typed `UserContext` carried alongside each subscription.
//!
//! Permission predicates may reference fields like `$user.org_id` or
//! `$user.is_admin`. Operators declare the schema of those fields up
//! front; subscribers attach a `UserContext` whose values are validated
//! against that schema before any predicate is evaluated.

use std::collections::BTreeMap;

use palimpsest_sql::ColumnType;
use serde::{Deserialize, Serialize};

use crate::error::PermissionError;

/// Concrete value bound to a `$user.*` field at subscribe time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum UserValue {
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 text.
    Text(String),
    /// ISO-8601 timestamp, opaque to the rewriter (compared as text).
    Timestamp(String),
    /// RFC 4122 UUID, stored as its string form. Prefer the
    /// [`UserValue::uuid`] constructor, which validates and normalizes;
    /// values built directly are validated by [`UserContext::validate`].
    Uuid(String),
    /// JSON document bound to a `jsonb` field. Rendered as a compact,
    /// key-sorted literal so equal documents share canonical keys.
    Jsonb(serde_json::Value),
    /// Enum label, compared as text. All Postgres enum types collapse
    /// into this one variant — the label carries no enum-type name.
    Enum(String),
    /// List of scalar values, referenced from predicates via
    /// `column = ANY($user.field)`. Elements must all be scalars of the
    /// field's declared type (nested lists are rejected by
    /// [`UserContext::validate`]). Materializes as an `ARRAY[...]`
    /// literal; the canonical representation sorts and dedupes
    /// elements, so `[2, 1, 1]` and `[1, 2]` share dataflows.
    List(Vec<Self>),
    /// SQL `NULL`. Always validates against any declared field type.
    Null,
}

impl UserValue {
    /// Builds a [`UserValue::Uuid`] after validating and normalizing
    /// `value` (lowercase, hyphenated `8-4-4-4-12` form). Accepts the
    /// hyphenated form, the plain 32-hex-digit form, and an optional
    /// surrounding `{...}` brace pair.
    ///
    /// # Errors
    /// Returns [`PermissionError::InvalidUserValue`] when `value` is not
    /// a well-formed UUID.
    pub fn uuid(value: impl AsRef<str>) -> Result<Self, PermissionError> {
        let raw = value.as_ref();
        normalized_uuid(raw)
            .map(Self::Uuid)
            .ok_or_else(|| PermissionError::InvalidUserValue {
                field: String::new(),
                reason: format!("'{raw}' is not a valid UUID"),
            })
    }

    /// Returns the catalog type that this value satisfies. A list
    /// reports its element type (the declared field type is the
    /// *element* type; list-ness is a property of the value), or
    /// [`ColumnType::Unknown`] when empty.
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        match self {
            Self::Bool(_) => ColumnType::Bool,
            Self::Int(_) => ColumnType::Int,
            Self::Float(_) => ColumnType::Float,
            Self::Text(_) => ColumnType::Text,
            Self::Timestamp(_) => ColumnType::Timestamp,
            Self::Uuid(_) => ColumnType::Uuid,
            Self::Jsonb(_) => ColumnType::Jsonb,
            Self::Enum(_) => ColumnType::Enum,
            Self::List(items) => items.first().map_or(ColumnType::Unknown, Self::column_type),
            Self::Null => ColumnType::Unknown,
        }
    }

    /// Renders the value as a SQL literal suitable for splicing into a
    /// canonicalized predicate string.
    #[must_use]
    pub fn to_sql_literal(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::Float(value) => format!("{value}"),
            Self::Text(value) | Self::Timestamp(value) | Self::Enum(value) => {
                format!("'{}'", value.replace('\'', "''"))
            }
            Self::Uuid(value) => format!("'{}'", normalize_or_raw(value).replace('\'', "''")),
            Self::Jsonb(value) => format!("'{}'", value.to_string().replace('\'', "''")),
            Self::List(items) => {
                let rendered: Vec<String> = items.iter().map(Self::to_sql_literal).collect();
                format!("ARRAY[{}]", rendered.join(", "))
            }
            Self::Null => "NULL".to_owned(),
        }
    }

    /// Stable string used for canonical-key hashing.
    ///
    /// Includes a type tag so `Int(0)` and `Bool(false)` never collide.
    #[must_use]
    pub fn canonical_repr(&self) -> String {
        match self {
            Self::Bool(value) => format!("bool:{value}"),
            Self::Int(value) => format!("int:{value}"),
            Self::Float(value) => format!("float:{value}"),
            Self::Text(value) => format!("text:{value}"),
            Self::Timestamp(value) => format!("ts:{value}"),
            Self::Uuid(value) => format!("uuid:{}", normalize_or_raw(value)),
            // `serde_json::Value` objects are backed by a sorted map, so
            // `to_string` is deterministic for structurally equal docs.
            Self::Jsonb(value) => format!("jsonb:{value}"),
            Self::Enum(value) => format!("enum:{value}"),
            // Order-insensitive and deduplicated: membership in a set is
            // what `ANY($user.field)` tests, so `[2, 1, 1]` and `[1, 2]`
            // must produce the same canonical key (and therefore share a
            // dataflow) — only genuinely different sets fork.
            //
            // Each element repr is length-prefixed so a crafted element
            // that *contains* the separator (e.g. `["1,text:2"]`) can
            // never collide with a different list (`["1", "2"]`).
            // Canonical keys decide dataflow sharing across users, so a
            // collision would leak one user's permitted rows to another.
            Self::List(items) => {
                let mut reprs: Vec<String> = items.iter().map(Self::canonical_repr).collect();
                reprs.sort();
                reprs.dedup();
                let framed: Vec<String> = reprs
                    .iter()
                    .map(|repr| format!("{}:{repr}", repr.len()))
                    .collect();
                format!("list:[{}]", framed.join(","))
            }
            Self::Null => "null".to_owned(),
        }
    }
}

/// Validates `raw` as an RFC 4122 UUID and returns the normalized
/// lowercase hyphenated form. Accepts `8-4-4-4-12` hex, plain 32 hex
/// digits, and an optional surrounding `{...}` pair.
fn normalized_uuid(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let trimmed = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .unwrap_or(trimmed);

    let hex: Vec<char> = match trimmed.len() {
        36 => {
            let bytes = trimmed.as_bytes();
            if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
                return None;
            }
            trimmed.chars().filter(|ch| *ch != '-').collect()
        }
        32 => trimmed.chars().collect(),
        _ => return None,
    };
    if hex.len() != 32 || !hex.iter().all(char::is_ascii_hexdigit) {
        return None;
    }

    let lower: String = hex.iter().collect::<String>().to_ascii_lowercase();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &lower[0..8],
        &lower[8..12],
        &lower[12..16],
        &lower[16..20],
        &lower[20..32],
    ))
}

/// Normalizes a UUID string, falling back to the raw text when it does
/// not parse (rendering must stay total; `validate` reports the error).
fn normalize_or_raw(raw: &str) -> String {
    normalized_uuid(raw).unwrap_or_else(|| raw.to_owned())
}

/// Declared shape of a `UserContext`: ordered field name → type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserContextSchema {
    fields: BTreeMap<String, ColumnType>,
}

impl UserContextSchema {
    /// Builds a schema from `(name, type)` pairs.
    #[must_use]
    pub fn new(fields: impl IntoIterator<Item = (String, ColumnType)>) -> Self {
        Self {
            fields: fields.into_iter().collect(),
        }
    }

    /// Returns the type declared for `field`, if any.
    #[must_use]
    pub fn field(&self, field: &str) -> Option<ColumnType> {
        self.fields.get(field).copied()
    }

    /// Iterates over `(field_name, type)` pairs in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, ColumnType)> {
        self.fields.iter().map(|(name, ty)| (name.as_str(), *ty))
    }

    /// Returns true when the schema declares `field`.
    #[must_use]
    pub fn contains(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    /// Number of declared fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// True when the schema has no declared fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Inserts (or replaces) a field declaration.
    pub fn insert(&mut self, name: impl Into<String>, ty: ColumnType) {
        self.fields.insert(name.into(), ty);
    }
}

/// Concrete `UserContext` attached to a subscription.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserContext {
    values: BTreeMap<String, UserValue>,
}

impl UserContext {
    /// Builds a context from `(field, value)` pairs. No validation is
    /// performed; use [`UserContext::validate`] to check against a schema.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = (String, UserValue)>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    /// Returns the value bound to `field`, if any.
    #[must_use]
    pub fn get(&self, field: &str) -> Option<&UserValue> {
        self.values.get(field)
    }

    /// Inserts (or replaces) a field value.
    pub fn insert(&mut self, field: impl Into<String>, value: UserValue) {
        self.values.insert(field.into(), value);
    }

    /// Iterates over `(field, value)` pairs in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &UserValue)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    /// Number of bound fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// True when no fields are bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Validates each value against the schema's declared type. `Null` is
    /// permitted regardless of the declared type. Beyond the coarse type
    /// check, UUID values (and text bound to a declared `uuid` field)
    /// must parse as well-formed UUIDs, and a declared `jsonb` field
    /// only accepts [`UserValue::Jsonb`]. A [`UserValue::List`] validates
    /// each element against the declared field type (the field type is
    /// the *element* type); nested lists are rejected.
    pub fn validate(&self, schema: &UserContextSchema) -> Result<(), PermissionError> {
        for (field, value) in &self.values {
            let expected = schema.field(field);
            if let UserValue::List(items) = value {
                for item in items {
                    match item {
                        UserValue::List(_) => {
                            return Err(PermissionError::InvalidUserValue {
                                field: field.clone(),
                                reason: "nested lists are not supported".to_owned(),
                            });
                        }
                        UserValue::Jsonb(_) => {
                            return Err(PermissionError::InvalidUserValue {
                                field: field.clone(),
                                reason: "jsonb documents inside lists are not supported".to_owned(),
                            });
                        }
                        _ => validate_scalar(field, item, expected)?,
                    }
                }
                continue;
            }
            validate_scalar(field, value, expected)?;
        }
        Ok(())
    }
}

/// Validates one scalar value against an optionally-declared field type.
/// Shared by top-level fields and list elements.
fn validate_scalar(
    field: &str,
    value: &UserValue,
    expected: Option<ColumnType>,
) -> Result<(), PermissionError> {
    if let UserValue::Uuid(raw) = value {
        if normalized_uuid(raw).is_none() {
            return Err(PermissionError::InvalidUserValue {
                field: field.to_owned(),
                reason: format!("'{raw}' is not a valid UUID"),
            });
        }
    }
    let Some(expected) = expected else {
        return Ok(());
    };
    if matches!(value, UserValue::Null) {
        return Ok(());
    }
    let actual = value.column_type();
    if !expected.is_compatible_with(actual)
        || (expected == ColumnType::Jsonb && !matches!(value, UserValue::Jsonb(_)))
    {
        return Err(PermissionError::UserValueTypeMismatch {
            field: field.to_owned(),
            expected,
            actual,
        });
    }
    if expected == ColumnType::Uuid {
        if let UserValue::Text(raw) = value {
            if normalized_uuid(raw).is_none() {
                return Err(PermissionError::InvalidUserValue {
                    field: field.to_owned(),
                    reason: format!("'{raw}' is not a valid UUID"),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ColumnType, UserContext, UserContextSchema, UserValue};

    #[test]
    fn schema_round_trip() {
        let mut schema = UserContextSchema::default();
        schema.insert("org_id", ColumnType::Int);
        schema.insert("is_admin", ColumnType::Bool);
        assert_eq!(schema.len(), 2);
        assert_eq!(schema.field("org_id"), Some(ColumnType::Int));
        assert!(schema.contains("is_admin"));
    }

    #[test]
    fn validate_rejects_type_mismatch() {
        let schema = UserContextSchema::new([
            ("org_id".to_owned(), ColumnType::Int),
            ("is_admin".to_owned(), ColumnType::Bool),
        ]);
        let context = UserContext::new([("org_id".to_owned(), UserValue::Text("nope".to_owned()))]);
        let err = context.validate(&schema).unwrap_err();
        assert!(err.to_string().contains("org_id"));
    }

    #[test]
    fn validate_accepts_null_and_compatible_numerics() {
        let schema = UserContextSchema::new([
            ("org_id".to_owned(), ColumnType::Int),
            ("balance".to_owned(), ColumnType::Float),
        ]);
        let context = UserContext::new([
            ("org_id".to_owned(), UserValue::Null),
            ("balance".to_owned(), UserValue::Int(7)),
        ]);
        context.validate(&schema).expect("compatible types");
    }

    #[test]
    fn sql_literal_escapes_single_quotes() {
        assert_eq!(
            UserValue::Text("o'brien".to_owned()).to_sql_literal(),
            "'o''brien'"
        );
        assert_eq!(UserValue::Int(-5).to_sql_literal(), "-5");
        assert_eq!(UserValue::Bool(true).to_sql_literal(), "true");
        assert_eq!(UserValue::Null.to_sql_literal(), "NULL");
    }

    #[test]
    fn canonical_repr_distinguishes_types() {
        assert_ne!(
            UserValue::Bool(false).canonical_repr(),
            UserValue::Int(0).canonical_repr(),
        );
        assert_ne!(
            UserValue::Text("admin".to_owned()).canonical_repr(),
            UserValue::Enum("admin".to_owned()).canonical_repr(),
        );
    }

    #[test]
    fn uuid_constructor_normalizes_all_accepted_forms() {
        let canonical = "67e55044-10b1-426f-9247-bb680e5fe0c8";
        for raw in [
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "67E55044-10B1-426F-9247-BB680E5FE0C8",
            "67e5504410b1426f9247bb680e5fe0c8",
            "{67e55044-10b1-426f-9247-bb680e5fe0c8}",
        ] {
            assert_eq!(
                UserValue::uuid(raw).expect("valid uuid"),
                UserValue::Uuid(canonical.to_owned()),
            );
        }
    }

    #[test]
    fn uuid_constructor_rejects_malformed_input() {
        for raw in [
            "",
            "not-a-uuid",
            "67e55044-10b1-426f-9247-bb680e5fe0c", // one digit short
            "67e55044x10b1x426fx9247xbb680e5fe0c8", // wrong separators
            "67e55044-10b1-426f-9247-bb680e5fe0c8ff", // too long
        ] {
            assert!(UserValue::uuid(raw).is_err(), "accepted {raw:?}");
        }
    }

    #[test]
    fn validate_accepts_uuid_jsonb_and_enum_fields() {
        let schema = UserContextSchema::new([
            ("tenant_id".to_owned(), ColumnType::Uuid),
            ("prefs".to_owned(), ColumnType::Jsonb),
            ("role".to_owned(), ColumnType::Enum),
        ]);
        let context = UserContext::new([
            (
                "tenant_id".to_owned(),
                UserValue::uuid("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap(),
            ),
            (
                "prefs".to_owned(),
                UserValue::Jsonb(serde_json::json!({"theme": "dark"})),
            ),
            ("role".to_owned(), UserValue::Enum("admin".to_owned())),
        ]);
        context.validate(&schema).expect("all fields validate");
    }

    #[test]
    fn validate_accepts_text_for_uuid_and_enum_fields() {
        // JWT claims arrive as plain strings; textual values satisfy
        // uuid (when well-formed) and enum declarations.
        let schema = UserContextSchema::new([
            ("tenant_id".to_owned(), ColumnType::Uuid),
            ("role".to_owned(), ColumnType::Enum),
        ]);
        let context = UserContext::new([
            (
                "tenant_id".to_owned(),
                UserValue::Text("67e55044-10b1-426f-9247-bb680e5fe0c8".to_owned()),
            ),
            ("role".to_owned(), UserValue::Text("admin".to_owned())),
        ]);
        context.validate(&schema).expect("textual values validate");
    }

    #[test]
    fn validate_rejects_malformed_uuid_text() {
        let schema = UserContextSchema::new([("tenant_id".to_owned(), ColumnType::Uuid)]);
        let context = UserContext::new([(
            "tenant_id".to_owned(),
            UserValue::Text("not-a-uuid".to_owned()),
        )]);
        let err = context.validate(&schema).unwrap_err();
        assert!(err.to_string().contains("not a valid UUID"));
    }

    #[test]
    fn validate_rejects_malformed_uuid_value_even_without_declaration() {
        let context =
            UserContext::new([("anything".to_owned(), UserValue::Uuid("garbage".to_owned()))]);
        let err = context.validate(&UserContextSchema::default()).unwrap_err();
        assert!(err.to_string().contains("not a valid UUID"));
    }

    #[test]
    fn validate_rejects_text_for_jsonb_field() {
        let schema = UserContextSchema::new([("prefs".to_owned(), ColumnType::Jsonb)]);
        let context = UserContext::new([(
            "prefs".to_owned(),
            UserValue::Text("{\"theme\":\"dark\"}".to_owned()),
        )]);
        let err = context.validate(&schema).unwrap_err();
        assert!(err.to_string().contains("prefs"));
    }

    #[test]
    fn uuid_sql_literal_and_canonical_repr_are_normalized() {
        let value = UserValue::Uuid("67E55044-10B1-426F-9247-BB680E5FE0C8".to_owned());
        assert_eq!(
            value.to_sql_literal(),
            "'67e55044-10b1-426f-9247-bb680e5fe0c8'"
        );
        assert_eq!(
            value.canonical_repr(),
            "uuid:67e55044-10b1-426f-9247-bb680e5fe0c8"
        );
    }

    #[test]
    fn jsonb_sql_literal_is_compact_and_escaped() {
        let value = UserValue::Jsonb(serde_json::json!({"note": "o'brien", "level": 3}));
        // serde_json's map is key-sorted, so rendering is deterministic.
        assert_eq!(value.to_sql_literal(), r#"'{"level":3,"note":"o''brien"}'"#);
        assert_eq!(
            value.canonical_repr(),
            r#"jsonb:{"level":3,"note":"o'brien"}"#
        );
    }

    #[test]
    fn enum_sql_literal_escapes_quotes() {
        assert_eq!(
            UserValue::Enum("it's-a-label".to_owned()).to_sql_literal(),
            "'it''s-a-label'"
        );
    }

    #[test]
    fn list_sql_literal_renders_array() {
        let value = UserValue::List(vec![UserValue::Int(3), UserValue::Int(1)]);
        assert_eq!(value.to_sql_literal(), "ARRAY[3, 1]");
        let texts = UserValue::List(vec![UserValue::Text("o'brien".to_owned())]);
        assert_eq!(texts.to_sql_literal(), "ARRAY['o''brien']");
        assert_eq!(UserValue::List(Vec::new()).to_sql_literal(), "ARRAY[]");
    }

    #[test]
    fn list_canonical_repr_resists_separator_injection() {
        // Without element framing, ["1", "2"] and ["1,text:2"] would
        // render identical reprs — and canonical keys decide dataflow
        // sharing across users, so the collision would let a crafted
        // context piggyback on another user's permission-filtered plan.
        let honest = UserValue::List(vec![
            UserValue::Text("1".to_owned()),
            UserValue::Text("2".to_owned()),
        ]);
        let crafted = UserValue::List(vec![UserValue::Text("1,text:2".to_owned())]);
        assert_ne!(honest.canonical_repr(), crafted.canonical_repr());
    }

    #[test]
    fn list_canonical_repr_is_order_insensitive_and_deduped() {
        let a = UserValue::List(vec![
            UserValue::Int(2),
            UserValue::Int(1),
            UserValue::Int(1),
        ]);
        let b = UserValue::List(vec![UserValue::Int(1), UserValue::Int(2)]);
        assert_eq!(a.canonical_repr(), b.canonical_repr());
        let c = UserValue::List(vec![UserValue::Int(1), UserValue::Int(3)]);
        assert_ne!(a.canonical_repr(), c.canonical_repr());
        // Type tags still matter inside lists.
        assert_ne!(
            UserValue::List(vec![UserValue::Int(0)]).canonical_repr(),
            UserValue::List(vec![UserValue::Bool(false)]).canonical_repr(),
        );
    }

    #[test]
    fn validate_accepts_list_of_declared_element_type() {
        let schema = UserContextSchema::new([("team_ids".to_owned(), ColumnType::Int)]);
        let context = UserContext::new([(
            "team_ids".to_owned(),
            UserValue::List(vec![UserValue::Int(1), UserValue::Int(2)]),
        )]);
        context.validate(&schema).expect("int list validates");
    }

    #[test]
    fn validate_rejects_list_with_mismatched_element() {
        let schema = UserContextSchema::new([("team_ids".to_owned(), ColumnType::Int)]);
        let context = UserContext::new([(
            "team_ids".to_owned(),
            UserValue::List(vec![UserValue::Int(1), UserValue::Text("nope".to_owned())]),
        )]);
        let err = context.validate(&schema).unwrap_err();
        assert!(err.to_string().contains("team_ids"));
    }

    #[test]
    fn validate_rejects_nested_lists() {
        let schema = UserContextSchema::new([("team_ids".to_owned(), ColumnType::Int)]);
        let context = UserContext::new([(
            "team_ids".to_owned(),
            UserValue::List(vec![UserValue::List(vec![UserValue::Int(1)])]),
        )]);
        let err = context.validate(&schema).unwrap_err();
        assert!(err.to_string().contains("nested lists"));
    }

    #[test]
    fn serde_round_trips_list() {
        let value = UserValue::List(vec![UserValue::Int(1), UserValue::Text("a".to_owned())]);
        let encoded = serde_json::to_string(&value).expect("serializes");
        let decoded: UserValue = serde_json::from_str(&encoded).expect("deserializes");
        assert_eq!(decoded, value);
    }

    #[test]
    fn serde_round_trips_new_variants() {
        for value in [
            UserValue::uuid("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap(),
            UserValue::Jsonb(serde_json::json!({"a": [1, 2, {"b": null}]})),
            UserValue::Enum("admin".to_owned()),
        ] {
            let encoded = serde_json::to_string(&value).expect("serializes");
            let decoded: UserValue = serde_json::from_str(&encoded).expect("deserializes");
            assert_eq!(decoded, value);
        }
        assert_eq!(
            serde_json::to_string(&UserValue::Enum("admin".to_owned())).unwrap(),
            r#"{"type":"enum","value":"admin"}"#
        );
    }
}
