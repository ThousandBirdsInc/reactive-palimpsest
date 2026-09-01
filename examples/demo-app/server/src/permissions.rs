//! Live permissions-DSL playground state.
//!
//! The demo boots with [`DEFAULT_PERMISSIONS_TOML`] and exposes the
//! same TOML DSL the production config loader consumes
//! (`palimpsest_permissions::parse_config`) through the write API:
//! `GET /api/permissions` returns the currently-applied source and
//! `PUT /api/permissions` compiles a replacement against the demo
//! catalog and hot-swaps it onto the running `SyncEngine` via
//! [`PalimpsestHandle::update_permissions`]. Every active subscription
//! receives `Resync(PermissionsChanged)` and resubscribes under the
//! new rules, so edits are visible in the browser immediately.

use std::sync::Mutex;

use palimpsest_permissions::{parse_config, PermissionError, RuleConfig};
use palimpsest_server::PalimpsestHandle;
use palimpsest_sql::Catalog;

/// Rule set the demo boots with. Kept as TOML (rather than inline
/// `PermissionRule` constructors) so the playground editor starts from
/// the exact source of truth it will round-trip through
/// `parse_config`.
///
/// The `[[user_context]]` fields must stay in sync with
/// `auth::jwt_auth_config`: the JWT lifts `sub` → `$user.id` (text)
/// and `is_admin` → `$user.is_admin` (bool). Rules referencing fields
/// the JWT does not supply compile fine but fail at subscribe time.
pub const DEFAULT_PERMISSIONS_TOML: &str = r#"[[user_context]]
name = "id"
type = "text"

[[user_context]]
name = "is_admin"
type = "bool"

[[rule]]
name = "issues_visibility"
table = "issues"
predicate = "project != 'security' OR $user.is_admin = true"
"#;

/// Currently-applied DSL source plus its parsed rule list, guarded
/// together so `GET` never observes a source/summary mismatch.
struct Current {
    toml: String,
    rules: Vec<RuleConfig>,
}

/// Shared playground state: the catalog rules compile against, the
/// handle used to hot-swap them, and the applied source.
pub struct PermissionsState {
    catalog: Catalog,
    handle: PalimpsestHandle,
    current: Mutex<Current>,
}

impl PermissionsState {
    /// Builds the state from the TOML the server booted with.
    ///
    /// # Panics
    /// Panics if `toml` does not parse — the caller already compiled
    /// it to boot the server, so this is unreachable in practice.
    pub fn new(catalog: Catalog, handle: PalimpsestHandle, toml: &str) -> Self {
        let config = parse_config(toml).expect("boot permissions TOML parses");
        Self {
            catalog,
            handle,
            current: Mutex::new(Current {
                toml: toml.to_owned(),
                rules: config.rules,
            }),
        }
    }

    /// Returns the applied TOML source and its rule summaries.
    pub fn snapshot(&self) -> (String, Vec<RuleConfig>) {
        let current = self.current.lock().expect("permissions lock");
        (current.toml.clone(), current.rules.clone())
    }

    /// Parses + compiles `toml` against the demo catalog and, on
    /// success, hot-swaps the running server's rule set.
    ///
    /// # Errors
    /// Returns the first [`PermissionError`] from parsing or
    /// compilation; the running rule set is left untouched.
    pub fn apply(&self, toml: &str) -> Result<Vec<RuleConfig>, PermissionError> {
        let config = parse_config(toml)?;
        let compiled = config.compile(&self.catalog)?;
        self.handle.update_permissions(compiled);
        let mut current = self.current.lock().expect("permissions lock");
        toml.clone_into(&mut current.toml);
        current.rules.clone_from(&config.rules);
        Ok(config.rules)
    }
}
