// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Pluggable gRPC authentication.
//!
//! The transport layer (§18.8) hands every inbound `Subscribe` stream a
//! [`MetadataMap`] of headers. An [`Authenticator`] inspects those
//! headers and yields a [`UserContext`] (or rejects the connection).
//!
//! Two implementations ship in this crate:
//!
//! * [`AnonymousAuthenticator`] — always succeeds, returns an empty
//!   context. Useful for dev/CI and the embedded `palimpsest-cli` when
//!   no `[auth]` section is configured.
//! * [`JwtAuthenticator`] — decodes a signed JWT from
//!   `Authorization: Bearer <token>` and maps the configured claims onto
//!   `UserContext` fields.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use palimpsest_permissions::{UserContext, UserValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::metadata::MetadataMap;

/// Authentication failure surfaced to the gRPC layer.
#[derive(Debug, Error)]
pub enum AuthError {
    /// `Authorization` header missing or malformed.
    #[error("missing or malformed authorization header")]
    MissingAuthorization,
    /// JWT decode/verify failed.
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// Claim referenced by the mapping is absent from the token.
    #[error("required claim '{0}' missing")]
    MissingClaim(String),
    /// Claim is present but not coercible to the declared `UserContext`
    /// shape.
    #[error("claim '{0}' has unexpected shape")]
    InvalidClaimShape(String),
}

/// Trait every gRPC auth backend implements.
///
/// Implementations must be `Send + Sync`: tonic clones the
/// `Arc<dyn Authenticator>` into the per-stream task.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Resolves an inbound stream's headers into a [`UserContext`].
    async fn authenticate(&self, headers: &MetadataMap) -> Result<UserContext, AuthError>;
}

/// Authenticator that always succeeds with an empty context.
#[derive(Debug, Default, Clone, Copy)]
pub struct AnonymousAuthenticator;

#[async_trait]
impl Authenticator for AnonymousAuthenticator {
    async fn authenticate(&self, _headers: &MetadataMap) -> Result<UserContext, AuthError> {
        Ok(UserContext::default())
    }
}

/// JWT-based authenticator.
///
/// Reads `Authorization: Bearer <token>` from the inbound metadata,
/// decodes with the configured secret + algorithm, and projects a
/// configured list of claims into `UserContext` fields.
pub struct JwtAuthenticator {
    decoding_key: DecodingKey,
    validation: Validation,
    /// Maps a JWT claim name to the `UserContext` field it should
    /// populate. The empty map yields an empty `UserContext` (the JWT
    /// is still verified, the resulting context just carries no
    /// fields).
    claim_to_field: BTreeMap<String, String>,
}

/// Configuration for [`JwtAuthenticator`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtAuthConfig {
    /// HMAC shared secret. v1 only ships HS256.
    pub secret: String,
    /// Optional issuer (`iss`) the token must declare.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Optional audience (`aud`) the token must declare.
    #[serde(default)]
    pub audience: Option<String>,
    /// Map from JWT claim name → `UserContext` field name.
    ///
    /// Example: `{ "sub" = "id", "org" = "org_id" }` lifts the
    /// token's subject onto `$user.id` and the `org` claim onto
    /// `$user.org_id`.
    #[serde(default)]
    pub claim_to_field: BTreeMap<String, String>,
}

impl JwtAuthenticator {
    /// Builds an authenticator from a [`JwtAuthConfig`].
    #[must_use]
    pub fn new(config: JwtAuthConfig) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        if let Some(iss) = &config.issuer {
            validation.set_issuer(&[iss.as_str()]);
        }
        if let Some(aud) = &config.audience {
            validation.set_audience(&[aud.as_str()]);
        }
        Self {
            decoding_key: DecodingKey::from_secret(config.secret.as_bytes()),
            validation,
            claim_to_field: config.claim_to_field,
        }
    }

    fn parse_bearer(headers: &MetadataMap) -> Result<&str, AuthError> {
        let value = headers
            .get("authorization")
            .ok_or(AuthError::MissingAuthorization)?;
        let raw = value
            .to_str()
            .map_err(|_| AuthError::MissingAuthorization)?;
        let token = raw
            .strip_prefix("Bearer ")
            .or_else(|| raw.strip_prefix("bearer "))
            .ok_or(AuthError::MissingAuthorization)?;
        Ok(token)
    }
}

#[async_trait]
impl Authenticator for JwtAuthenticator {
    async fn authenticate(&self, headers: &MetadataMap) -> Result<UserContext, AuthError> {
        let token = Self::parse_bearer(headers)?;
        let data = decode::<serde_json::Value>(token, &self.decoding_key, &self.validation)
            .map_err(|err| AuthError::InvalidToken(err.to_string()))?;
        let claims = data.claims;
        let mut ctx = UserContext::default();
        for (claim, field) in &self.claim_to_field {
            let value = claims
                .get(claim)
                .ok_or_else(|| AuthError::MissingClaim(claim.clone()))?;
            let user_value = json_to_user_value(claim, value)?;
            ctx.insert(field.clone(), user_value);
        }
        Ok(ctx)
    }
}

fn json_to_user_value(claim: &str, value: &serde_json::Value) -> Result<UserValue, AuthError> {
    match value {
        serde_json::Value::Bool(b) => Ok(UserValue::Bool(*b)),
        serde_json::Value::Number(n) => n.as_i64().map_or_else(
            || {
                n.as_f64().map_or_else(
                    || Err(AuthError::InvalidClaimShape(claim.to_owned())),
                    |f| Ok(UserValue::Float(f)),
                )
            },
            |i| Ok(UserValue::Int(i)),
        ),
        serde_json::Value::String(s) => Ok(UserValue::Text(s.clone())),
        serde_json::Value::Null => Ok(UserValue::Null),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(AuthError::InvalidClaimShape(claim.to_owned()))
        }
    }
}

/// Convenience type alias for shared, dyn-dispatched authenticators.
pub type DynAuthenticator = Arc<dyn Authenticator>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use jsonwebtoken::{encode, EncodingKey, Header};
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::{
        AnonymousAuthenticator, AuthError, Authenticator, JwtAuthConfig, JwtAuthenticator,
    };

    fn now_plus(secs: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs()
            + secs
    }

    fn token_with(claims: &serde_json::Value, secret: &str) -> String {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("encode")
    }

    fn metadata_with_bearer(token: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).expect("metadata"),
        );
        md
    }

    #[tokio::test]
    async fn anonymous_returns_empty_user_context() {
        let auth = AnonymousAuthenticator;
        let ctx = auth.authenticate(&MetadataMap::new()).await.unwrap();
        assert!(ctx.is_empty());
    }

    #[tokio::test]
    async fn jwt_decodes_and_lifts_claims() {
        let secret = "topsecret";
        let claims = serde_json::json!({
            "sub": "user-1",
            "org": 7,
            "exp": now_plus(60),
        });
        let token = token_with(&claims, secret);

        let mut claim_to_field = BTreeMap::new();
        claim_to_field.insert("sub".to_owned(), "id".to_owned());
        claim_to_field.insert("org".to_owned(), "org_id".to_owned());

        let auth = JwtAuthenticator::new(JwtAuthConfig {
            secret: secret.to_owned(),
            issuer: None,
            audience: None,
            claim_to_field,
        });

        let ctx = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap();
        assert!(ctx.get("id").is_some());
        assert!(ctx.get("org_id").is_some());
    }

    #[tokio::test]
    async fn jwt_missing_authorization_header_is_rejected() {
        let auth = JwtAuthenticator::new(JwtAuthConfig {
            secret: "x".to_owned(),
            issuer: None,
            audience: None,
            claim_to_field: BTreeMap::new(),
        });
        let err = auth.authenticate(&MetadataMap::new()).await.unwrap_err();
        assert!(matches!(err, AuthError::MissingAuthorization));
    }

    #[tokio::test]
    async fn jwt_invalid_signature_is_rejected() {
        let token = token_with(
            &serde_json::json!({ "sub": "x", "exp": now_plus(60) }),
            "right",
        );
        let auth = JwtAuthenticator::new(JwtAuthConfig {
            secret: "wrong".to_owned(),
            issuer: None,
            audience: None,
            claim_to_field: BTreeMap::new(),
        });
        let err = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)));
    }

    #[tokio::test]
    async fn jwt_missing_mapped_claim_is_reported() {
        let token = token_with(
            &serde_json::json!({ "sub": "x", "exp": now_plus(60) }),
            "secret",
        );
        let mut mapping = BTreeMap::new();
        mapping.insert("absent".to_owned(), "field".to_owned());
        let auth = JwtAuthenticator::new(JwtAuthConfig {
            secret: "secret".to_owned(),
            issuer: None,
            audience: None,
            claim_to_field: mapping,
        });
        let err = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::MissingClaim(name) if name == "absent"));
    }
}
