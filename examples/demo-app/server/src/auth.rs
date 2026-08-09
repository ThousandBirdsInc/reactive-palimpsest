//! Demo auth config: HMAC secret, well-known users, JWT minter.
//!
//! In a real deployment the HMAC secret would come from a secret
//! manager and tokens would be minted by an identity provider. For the
//! demo we hardcode a development secret and mint short-ish-lived
//! tokens on-demand from `/api/token?user=<id>` — the SyncEngine's
//! `JwtAuthenticator` decodes those same tokens on the inbound side.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, EncodingKey, Header};
use palimpsest_server::JwtAuthConfig;
use serde::Serialize;

/// HMAC secret used to sign + verify JWTs. Same value is consumed by
/// the SyncEngine's `JwtAuthenticator`. Never use in production.
pub const DEV_SECRET: &str = "demo-secret-dev-only-not-for-production";

/// Token lifetime. 24h is plenty for a demo session and short enough
/// that nobody mistakes a leaked token for a forever credential.
const TOKEN_TTL_SECS: u64 = 24 * 60 * 60;

/// A demo persona. The picker shows one chip per `User` and each chip
/// resolves to a fresh JWT signed by [`mint_token`].
#[derive(Debug, Clone)]
pub struct User {
    pub id: &'static str,
    pub display_name: &'static str,
    pub is_admin: bool,
}

/// Three demo personas: one admin, two readers. Bob and Carol differ
/// only by name so it's obvious that swapping between them keeps the
/// same row-visibility filter (`is_admin = false`).
pub const USERS: &[User] = &[
    User {
        id: "alice",
        display_name: "Alice (admin)",
        is_admin: true,
    },
    User {
        id: "bob",
        display_name: "Bob (reader)",
        is_admin: false,
    },
    User {
        id: "carol",
        display_name: "Carol (reader)",
        is_admin: false,
    },
];

/// Find a persona by id, case-sensitive.
pub fn lookup(id: &str) -> Option<&'static User> {
    USERS.iter().find(|u| u.id == id)
}

/// JWT claim set the demo signs. Field names line up with
/// [`jwt_auth_config`]'s `claim_to_field` map.
#[derive(Debug, Serialize)]
struct Claims {
    sub: String,
    is_admin: bool,
    exp: u64,
}

/// Mint a fresh JWT for `user`. Token is HS256-signed with
/// [`DEV_SECRET`] and expires `TOKEN_TTL_SECS` from now.
///
/// # Errors
/// Surfaces `jsonwebtoken::errors::Error` if encoding fails (only
/// realistic cause is an OOM — the inputs here are well-formed).
pub fn mint_token(user: &User) -> Result<String, jsonwebtoken::errors::Error> {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
        + TOKEN_TTL_SECS;
    let claims = Claims {
        sub: user.id.to_owned(),
        is_admin: user.is_admin,
        exp,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(DEV_SECRET.as_bytes()),
    )
}

/// JWT config the SyncEngine's `JwtAuthenticator` uses to verify
/// inbound tokens. The `claim_to_field` map lifts the `sub` claim onto
/// `$user.id` and `is_admin` onto `$user.is_admin` — those are the
/// `$user.*` field references the permission rule depends on.
pub fn jwt_auth_config() -> JwtAuthConfig {
    let mut claim_to_field = BTreeMap::new();
    claim_to_field.insert("sub".to_owned(), "id".to_owned());
    claim_to_field.insert("is_admin".to_owned(), "is_admin".to_owned());
    JwtAuthConfig {
        secret: Some(DEV_SECRET.to_owned()),
        claim_to_field,
        ..JwtAuthConfig::default()
    }
}
