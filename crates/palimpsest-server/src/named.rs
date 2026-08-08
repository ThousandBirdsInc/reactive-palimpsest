// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Server-side policy around the named prepared-query registry.
//!
//! [`NamedQueries`] wraps a [`QueryRegistry`] (built at startup — see
//! [`crate::embed::PalimpsestBuilder::with_query_registry`]) together
//! with the raw-SQL admission policy. The posture is fail-closed:
//!
//! * a `Subscribe{query_name}` for an unregistered name is refused;
//! * configuring a registry disables raw-SQL subscribes unless the
//!   embedder explicitly re-enables them, so the set of registered
//!   queries becomes the authorization boundary — tables reachable
//!   only through registered queries need no row-visibility rule of
//!   their own.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_sql::prepared::{BindError, BoundQuery, ParamValue, QueryRegistry};

/// Named-query policy shared by every connection.
///
/// Clones share the underlying registry slot, so
/// [`Self::replace_registry`] on any clone is immediately visible to
/// every connection — mirroring how permission rules hot-swap.
#[derive(Clone)]
pub struct NamedQueries {
    registry: Arc<RwLock<Option<Arc<QueryRegistry>>>>,
    inline_sql_enabled: bool,
}

impl Default for NamedQueries {
    /// No registry; raw SQL stays enabled (the pre-registry behavior).
    fn default() -> Self {
        Self {
            registry: Arc::new(RwLock::new(None)),
            inline_sql_enabled: true,
        }
    }
}

impl NamedQueries {
    /// Policy backed by `registry`. Raw-SQL subscribes are disabled —
    /// registered queries become the only reachable query surface.
    /// Call [`Self::with_inline_sql`] to relax that.
    #[must_use]
    pub fn new(registry: QueryRegistry) -> Self {
        Self {
            registry: Arc::new(RwLock::new(Some(Arc::new(registry)))),
            inline_sql_enabled: false,
        }
    }

    /// Explicitly enables or disables raw-SQL subscribes.
    #[must_use]
    pub const fn with_inline_sql(mut self, enabled: bool) -> Self {
        self.inline_sql_enabled = enabled;
        self
    }

    /// Whether `Subscribe{sql}` is admitted at all.
    #[must_use]
    pub const fn inline_sql_enabled(&self) -> bool {
        self.inline_sql_enabled
    }

    /// The backing registry, when one is configured.
    #[must_use]
    pub fn registry(&self) -> Option<Arc<QueryRegistry>> {
        self.registry.read().expect("registry lock").clone()
    }

    /// Hot-swaps the registry on a running server. Future subscribes
    /// bind against the new set immediately; subscriptions already
    /// streaming keep their bound plan (removing a query does not tear
    /// down its active subscriptions — drop the affected permission
    /// grants or restart to revoke live streams).
    pub fn replace_registry(&self, registry: QueryRegistry) {
        *self.registry.write().expect("registry lock") = Some(Arc::new(registry));
    }

    /// Resolves a named subscribe: converts the wire params and binds
    /// them into the registered template.
    ///
    /// # Errors
    /// `(code, message)` ready for the wire `Error` frame. Unknown
    /// names (including "no registry configured") map to
    /// `unknown_query`; parameter problems map to `invalid_params`.
    pub fn bind_named(
        &self,
        name: &str,
        vars: &HashMap<String, proto::VarValue>,
    ) -> Result<BoundQuery, (&'static str, String)> {
        let Some(registry) = self.registry() else {
            // Fail closed: a server without a registry has no named
            // queries, so every name is unknown.
            return Err((
                "unknown_query",
                format!("unknown query '{name}': no named queries are registered"),
            ));
        };
        let mut params = HashMap::with_capacity(vars.len());
        for (key, value) in vars {
            let value = param_value_from_proto(value)
                .map_err(|reason| ("invalid_params", format!("parameter '{key}': {reason}")))?;
            params.insert(key.clone(), value);
        }
        registry.bind(name, &params).map_err(|err| match err {
            BindError::UnknownQuery(_) => ("unknown_query", err.to_string()),
            other => ("invalid_params", other.to_string()),
        })
    }
}

/// Converts one wire `VarValue` into the registry's [`ParamValue`].
fn param_value_from_proto(value: &proto::VarValue) -> Result<ParamValue, String> {
    use proto::var_value::Kind;
    match &value.kind {
        None => Err("empty value".to_owned()),
        Some(Kind::BoolValue(b)) => Ok(ParamValue::Bool(*b)),
        Some(Kind::IntValue(v)) => Ok(ParamValue::Int(*v)),
        Some(Kind::FloatValue(v)) => Ok(ParamValue::Float(*v)),
        Some(Kind::StringValue(s)) => Ok(ParamValue::Text(s.clone())),
        Some(Kind::NullValue(_)) => Ok(ParamValue::Null),
        Some(Kind::BytesValue(_)) => Err("bytes values are not supported as parameters".to_owned()),
        Some(Kind::ListValue(list)) => list
            .values
            .iter()
            .map(|element| match param_value_from_proto(element)? {
                ParamValue::List(_) => Err("nested lists are not supported".to_owned()),
                scalar => Ok(scalar),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(ParamValue::List),
    }
}

#[cfg(test)]
mod tests {
    use super::NamedQueries;
    use palimpsest_proto::palimpsest::sync::v1 as proto;
    use palimpsest_sql::prepared::{ParamDecl, QueryRegistry};
    use palimpsest_sql::ColumnType;
    use std::collections::HashMap;

    fn registry() -> QueryRegistry {
        let mut registry = QueryRegistry::new();
        registry
            .register(
                "PostById",
                "SELECT id FROM posts WHERE id = $1",
                &[ParamDecl::new("id", ColumnType::Int)],
            )
            .expect("register");
        registry
    }

    fn string_var(value: &str) -> proto::VarValue {
        proto::VarValue {
            kind: Some(proto::var_value::Kind::StringValue(value.to_owned())),
        }
    }

    #[test]
    fn default_policy_allows_inline_sql_and_knows_no_names() {
        let policy = NamedQueries::default();
        assert!(policy.inline_sql_enabled());
        let err = policy
            .bind_named("PostById", &HashMap::new())
            .expect_err("no registry");
        assert_eq!(err.0, "unknown_query");
    }

    #[test]
    fn registry_policy_disables_inline_sql_by_default() {
        let policy = NamedQueries::new(registry());
        assert!(!policy.inline_sql_enabled());
        assert!(NamedQueries::new(registry())
            .with_inline_sql(true)
            .inline_sql_enabled());
    }

    #[test]
    fn replace_registry_is_visible_to_clones() {
        let policy = NamedQueries::new(registry());
        let connection_view = policy.clone();

        let mut swapped = QueryRegistry::new();
        swapped
            .register(
                "PostsByAuthor",
                "SELECT id FROM posts WHERE author_id = $1",
                &[ParamDecl::new("author_id", ColumnType::Int)],
            )
            .expect("register");
        policy.replace_registry(swapped);

        let mut vars = HashMap::new();
        vars.insert("author_id".to_owned(), string_var("3"));
        connection_view
            .bind_named("PostsByAuthor", &vars)
            .expect("clone sees the swapped registry");
        let err = connection_view
            .bind_named("PostById", &HashMap::new())
            .expect_err("old name is gone after the swap");
        assert_eq!(err.0, "unknown_query");
    }

    #[test]
    fn binds_wire_params() {
        let policy = NamedQueries::new(registry());
        let mut vars = HashMap::new();
        vars.insert("id".to_owned(), string_var("7"));
        let bound = policy.bind_named("PostById", &vars).expect("bind");
        assert!(bound.sql.contains('7'), "{}", bound.sql);

        let err = policy
            .bind_named("Missing", &HashMap::new())
            .expect_err("unknown name");
        assert_eq!(err.0, "unknown_query");

        let mut vars = HashMap::new();
        vars.insert("id".to_owned(), string_var("not-a-number"));
        let err = policy.bind_named("PostById", &vars).expect_err("bad value");
        assert_eq!(err.0, "invalid_params");
    }
}
