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
//!   `UserContext` fields. Verification is config, not code: HMAC
//!   secrets, RSA/EC/EdDSA public keys in PEM, an inline JWKS
//!   document, or a JWKS URL (fetched at startup and refreshed on
//!   unknown key ids). Standard claims (`iss`, `aud`, `exp`, `nbf`)
//!   validate with configurable leeway, and list-valued claims map
//!   onto list user-context fields so a multi-team user is one
//!   subscription rather than N.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
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
    /// The `[auth]` configuration itself is unusable (bad PEM, bad
    /// JWKS, conflicting key sources). Raised at startup, not per
    /// request.
    #[error("invalid auth configuration: {0}")]
    InvalidConfig(String),
    /// No verification key matches the token's key id.
    #[error("no verification key matches the token (kid {0:?})")]
    UnknownKey(Option<String>),
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
/// verifies it against the configured key material, validates the
/// standard claims, and projects a configured list of claims into
/// `UserContext` fields.
pub struct JwtAuthenticator {
    keys: Keys,
    issuer: Option<String>,
    audience: Option<String>,
    leeway_secs: u64,
    /// Maps a JWT claim name to the `UserContext` field it should
    /// populate. The empty map yields an empty `UserContext` (the JWT
    /// is still verified, the resulting context just carries no
    /// fields).
    claim_to_field: BTreeMap<String, String>,
}

/// Configuration for [`JwtAuthenticator`] — declaratively selects the
/// key material, the standard-claim policy, and the claim mapping.
/// Exactly one of `secret`, `public_key_pem`, `jwks`, or `jwks_url`
/// must be set.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JwtAuthConfig {
    /// HMAC shared secret (HS256/HS384/HS512).
    #[serde(default)]
    pub secret: Option<String>,
    /// PEM-encoded public key (RS*/PS*/ES*/EdDSA verification).
    #[serde(default)]
    pub public_key_pem: Option<String>,
    /// Inline JWKS document (RFC 7517 `{"keys": [...]}`).
    #[serde(default)]
    pub jwks: Option<String>,
    /// JWKS URL, fetched at startup and refreshed when a token
    /// presents an unknown `kid` (rate-limited).
    #[serde(default)]
    pub jwks_url: Option<String>,
    /// Signature algorithm. Defaults to `HS256` with a secret and
    /// `RS256` with a PEM key; JWKS keys carry their own `alg`.
    #[serde(default)]
    pub algorithm: Option<String>,
    /// Optional issuer (`iss`) the token must declare.
    #[serde(default)]
    pub issuer: Option<String>,
    /// Optional audience (`aud`) the token must declare.
    #[serde(default)]
    pub audience: Option<String>,
    /// Clock-skew leeway in seconds applied to `exp` / `nbf`.
    /// Defaults to 60.
    #[serde(default)]
    pub leeway_secs: Option<u64>,
    /// Map from JWT claim name → `UserContext` field name.
    ///
    /// Example: `{ "sub" = "id", "team_ids" = "team_ids" }` lifts the
    /// token's subject onto `$user.id` and a list-valued `team_ids`
    /// claim onto a list field for `= ANY($user.team_ids)` rules.
    #[serde(default)]
    pub claim_to_field: BTreeMap<String, String>,
}

/// Verification key material.
enum Keys {
    /// One key, fixed algorithm set (secret or PEM).
    Fixed {
        key: DecodingKey,
        algorithms: Vec<Algorithm>,
    },
    /// A JWKS key set, optionally refreshable from a URL.
    Jwks(JwksKeys),
}

struct CachedJwk {
    kid: Option<String>,
    algorithm: Option<Algorithm>,
    key: DecodingKey,
}

struct JwksKeys {
    url: Option<String>,
    cached: tokio::sync::RwLock<Vec<CachedJwk>>,
    /// Last refresh time; refetches are rate-limited so a flood of
    /// unknown-kid tokens cannot hammer the issuer.
    last_refresh: tokio::sync::Mutex<Option<Instant>>,
}

const JWKS_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(30);

fn parse_jwk_set(raw: &str) -> Result<Vec<CachedJwk>, AuthError> {
    let set: JwkSet = serde_json::from_str(raw)
        .map_err(|err| AuthError::InvalidConfig(format!("unparseable JWKS: {err}")))?;
    let mut keys = Vec::with_capacity(set.keys.len());
    for jwk in &set.keys {
        let key = match DecodingKey::from_jwk(jwk) {
            Ok(key) => key,
            Err(err) => {
                tracing::warn!(
                    kid = ?jwk.common.key_id,
                    error = %err,
                    "skipping unsupported JWKS key"
                );
                continue;
            }
        };
        let algorithm = jwk
            .common
            .key_algorithm
            .and_then(|alg| alg.to_string().parse::<Algorithm>().ok());
        keys.push(CachedJwk {
            kid: jwk.common.key_id.clone(),
            algorithm,
            key,
        });
    }
    if keys.is_empty() {
        return Err(AuthError::InvalidConfig(
            "JWKS contains no usable verification keys".to_owned(),
        ));
    }
    Ok(keys)
}

/// HMAC algorithms must never be admitted for JWKS-sourced keys: JWKS
/// material is public, and accepting `HS*` there would let a client
/// sign tokens with the public key bytes.
const fn is_hmac(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
    )
}

impl JwtAuthenticator {
    /// Builds an authenticator from a [`JwtAuthConfig`], validating
    /// key material up front so misconfiguration fails at startup.
    ///
    /// # Errors
    /// [`AuthError::InvalidConfig`] naming the problem.
    pub async fn from_config(config: JwtAuthConfig) -> Result<Self, AuthError> {
        let sources = [
            config.secret.is_some(),
            config.public_key_pem.is_some(),
            config.jwks.is_some(),
            config.jwks_url.is_some(),
        ]
        .iter()
        .filter(|set| **set)
        .count();
        if sources != 1 {
            return Err(AuthError::InvalidConfig(
                "configure exactly one of: secret, public_key_pem, jwks, jwks_url".to_owned(),
            ));
        }

        let algorithm = config
            .algorithm
            .as_deref()
            .map(|raw| {
                raw.parse::<Algorithm>()
                    .map_err(|_| AuthError::InvalidConfig(format!("unknown algorithm '{raw}'")))
            })
            .transpose()?;

        let keys = if let Some(secret) = &config.secret {
            let algorithm = algorithm.unwrap_or(Algorithm::HS256);
            if !is_hmac(algorithm) {
                return Err(AuthError::InvalidConfig(format!(
                    "algorithm {algorithm:?} needs a public key, not an HMAC secret"
                )));
            }
            Keys::Fixed {
                key: DecodingKey::from_secret(secret.as_bytes()),
                algorithms: vec![algorithm],
            }
        } else if let Some(pem) = &config.public_key_pem {
            let algorithm = algorithm.unwrap_or(Algorithm::RS256);
            let key = match algorithm {
                Algorithm::RS256
                | Algorithm::RS384
                | Algorithm::RS512
                | Algorithm::PS256
                | Algorithm::PS384
                | Algorithm::PS512 => DecodingKey::from_rsa_pem(pem.as_bytes()),
                Algorithm::ES256 | Algorithm::ES384 => DecodingKey::from_ec_pem(pem.as_bytes()),
                Algorithm::EdDSA => DecodingKey::from_ed_pem(pem.as_bytes()),
                Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
                    return Err(AuthError::InvalidConfig(
                        "HMAC algorithms take a secret, not a PEM key".to_owned(),
                    ));
                }
            }
            .map_err(|err| AuthError::InvalidConfig(format!("unusable public key PEM: {err}")))?;
            Keys::Fixed {
                key,
                algorithms: vec![algorithm],
            }
        } else if let Some(raw) = &config.jwks {
            Keys::Jwks(JwksKeys {
                url: None,
                cached: tokio::sync::RwLock::new(parse_jwk_set(raw)?),
                last_refresh: tokio::sync::Mutex::new(None),
            })
        } else {
            let url = config.jwks_url.clone().expect("checked above");
            let initial = fetch_jwks(&url).await?;
            Keys::Jwks(JwksKeys {
                url: Some(url),
                cached: tokio::sync::RwLock::new(initial),
                last_refresh: tokio::sync::Mutex::new(Some(Instant::now())),
            })
        };

        Ok(Self {
            keys,
            issuer: config.issuer,
            audience: config.audience,
            leeway_secs: config.leeway_secs.unwrap_or(60),
            claim_to_field: config.claim_to_field,
        })
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

    fn validation(&self, algorithm: Algorithm) -> Validation {
        let mut validation = Validation::new(algorithm);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.leeway = self.leeway_secs;
        if let Some(iss) = &self.issuer {
            validation.set_issuer(&[iss.as_str()]);
        }
        if let Some(aud) = &self.audience {
            validation.set_audience(&[aud.as_str()]);
        }
        validation
    }

    /// Resolves the verification key + algorithm for one token.
    async fn resolve_key(
        &self,
        header: &jsonwebtoken::Header,
    ) -> Result<(DecodingKey, Algorithm), AuthError> {
        match &self.keys {
            Keys::Fixed { key, algorithms } => {
                if !algorithms.contains(&header.alg) {
                    return Err(AuthError::InvalidToken(format!(
                        "token algorithm {:?} is not allowed",
                        header.alg
                    )));
                }
                Ok((key.clone(), header.alg))
            }
            Keys::Jwks(jwks) => {
                if is_hmac(header.alg) {
                    return Err(AuthError::InvalidToken(
                        "HMAC tokens are not accepted against a JWKS".to_owned(),
                    ));
                }
                if let Some(hit) = Self::jwks_lookup(jwks, header).await {
                    return Ok(hit);
                }
                // Unknown kid: the issuer may have rotated keys.
                // Refresh (rate-limited) and retry once.
                if let Some(url) = &jwks.url {
                    let mut last = jwks.last_refresh.lock().await;
                    let due = last.is_none_or(|at| at.elapsed() >= JWKS_REFRESH_MIN_INTERVAL);
                    if due {
                        let fresh = fetch_jwks(url).await?;
                        *jwks.cached.write().await = fresh;
                        *last = Some(Instant::now());
                    }
                    drop(last);
                    if let Some(hit) = Self::jwks_lookup(jwks, header).await {
                        return Ok(hit);
                    }
                }
                Err(AuthError::UnknownKey(header.kid.clone()))
            }
        }
    }

    async fn jwks_lookup(
        jwks: &JwksKeys,
        header: &jsonwebtoken::Header,
    ) -> Option<(DecodingKey, Algorithm)> {
        let cached = jwks.cached.read().await;
        let candidate = match &header.kid {
            Some(kid) => cached.iter().find(|key| key.kid.as_ref() == Some(kid)),
            // No kid on the token: unambiguous only when the set has
            // exactly one key.
            None => (cached.len() == 1).then(|| &cached[0]),
        }?;
        // A key that pins an algorithm only verifies tokens using it.
        if candidate
            .algorithm
            .is_some_and(|algorithm| algorithm != header.alg)
        {
            return None;
        }
        Some((candidate.key.clone(), header.alg))
    }
}

/// Fetches and parses a JWKS document.
async fn fetch_jwks(url: &str) -> Result<Vec<CachedJwk>, AuthError> {
    let response = reqwest::get(url)
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|err| AuthError::InvalidConfig(format!("JWKS fetch failed: {err}")))?;
    let body = response
        .text()
        .await
        .map_err(|err| AuthError::InvalidConfig(format!("JWKS fetch failed: {err}")))?;
    parse_jwk_set(&body)
}

#[async_trait]
impl Authenticator for JwtAuthenticator {
    async fn authenticate(&self, headers: &MetadataMap) -> Result<UserContext, AuthError> {
        let token = Self::parse_bearer(headers)?;
        let header =
            decode_header(token).map_err(|err| AuthError::InvalidToken(err.to_string()))?;
        let (key, algorithm) = self.resolve_key(&header).await?;
        let validation = self.validation(algorithm);
        let data = decode::<serde_json::Value>(token, &key, &validation)
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
        // Arrays of scalars (e.g. a `team_ids` claim) map onto list
        // user-context fields, consumed by `column = ANY($user.field)`
        // predicates. Arrays containing structured values fall back to
        // `jsonb`, like objects.
        serde_json::Value::Array(items) => {
            if items.iter().all(|item| {
                !matches!(
                    item,
                    serde_json::Value::Array(_) | serde_json::Value::Object(_)
                )
            }) {
                let elements = items
                    .iter()
                    .map(|item| json_to_user_value(claim, item))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(UserValue::List(elements))
            } else {
                Ok(UserValue::Jsonb(value.clone()))
            }
        }
        // Structured claims map onto `jsonb` user-context fields.
        // Validation against the declared schema happens downstream.
        serde_json::Value::Object(_) => Ok(UserValue::Jsonb(value.clone())),
    }
}

/// Convenience type alias for shared, dyn-dispatched authenticators.
pub type DynAuthenticator = Arc<dyn Authenticator>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use tonic::metadata::{MetadataMap, MetadataValue};

    use super::{
        AnonymousAuthenticator, AuthError, Authenticator, JwtAuthConfig, JwtAuthenticator,
    };

    const RSA_PRIVATE_PEM: &str = include_str!("../testdata/jwt_rsa_private.pem");
    const RSA_PUBLIC_PEM: &str = include_str!("../testdata/jwt_rsa_public.pem");
    const EC_PRIVATE_PEM: &str = include_str!("../testdata/jwt_ec_private.pem");
    const EC_PUBLIC_PEM: &str = include_str!("../testdata/jwt_ec_public.pem");
    const JWKS_JSON: &str = include_str!("../testdata/jwt_jwks.json");

    fn now_plus(secs: i64) -> i64 {
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_secs(),
        )
        .expect("epoch fits")
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

    async fn hs256(secret: &str, claim_to_field: BTreeMap<String, String>) -> JwtAuthenticator {
        JwtAuthenticator::from_config(JwtAuthConfig {
            secret: Some(secret.to_owned()),
            claim_to_field,
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config")
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
        let auth = hs256(secret, claim_to_field).await;

        let ctx = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap();
        assert!(ctx.get("id").is_some());
        assert!(ctx.get("org_id").is_some());
    }

    #[tokio::test]
    async fn jwt_lifts_list_claims_as_lists() {
        // A multi-team user is one subscription: the list claim maps
        // onto a list user-context field for `= ANY($user.team_ids)`.
        let secret = "topsecret";
        let claims = serde_json::json!({
            "team_ids": [3, 5, 9],
            "exp": now_plus(60),
        });
        let token = token_with(&claims, secret);
        let mut mapping = BTreeMap::new();
        mapping.insert("team_ids".to_owned(), "team_ids".to_owned());
        let auth = hs256(secret, mapping).await;
        let ctx = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap();
        assert_eq!(
            ctx.get("team_ids"),
            Some(&palimpsest_permissions::UserValue::List(vec![
                palimpsest_permissions::UserValue::Int(3),
                palimpsest_permissions::UserValue::Int(5),
                palimpsest_permissions::UserValue::Int(9),
            ]))
        );
    }

    #[tokio::test]
    async fn jwt_lifts_structured_claims_as_jsonb() {
        let secret = "topsecret";
        let claims = serde_json::json!({
            "sub": "user-1",
            "prefs": {"theme": "dark", "tags": [1, 2]},
            "exp": now_plus(60),
        });
        let token = token_with(&claims, secret);

        let mut claim_to_field = BTreeMap::new();
        claim_to_field.insert("prefs".to_owned(), "prefs".to_owned());
        let auth = hs256(secret, claim_to_field).await;

        let ctx = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap();
        assert_eq!(
            ctx.get("prefs"),
            Some(&palimpsest_permissions::UserValue::Jsonb(
                serde_json::json!({"theme": "dark", "tags": [1, 2]})
            ))
        );
    }

    #[tokio::test]
    async fn jwt_missing_authorization_header_is_rejected() {
        let auth = hs256("x", BTreeMap::new()).await;
        let err = auth.authenticate(&MetadataMap::new()).await.unwrap_err();
        assert!(matches!(err, AuthError::MissingAuthorization));
    }

    #[tokio::test]
    async fn jwt_invalid_signature_is_rejected() {
        let token = token_with(
            &serde_json::json!({ "sub": "x", "exp": now_plus(60) }),
            "right",
        );
        let auth = hs256("wrong", BTreeMap::new()).await;
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
        let auth = hs256("secret", mapping).await;
        let err = auth
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::MissingClaim(name) if name == "absent"));
    }

    #[tokio::test]
    async fn rs256_pem_verifies_and_rejects_wrong_family() {
        let claims = serde_json::json!({ "sub": "user-1", "exp": now_plus(60) });
        let token = encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(RSA_PRIVATE_PEM.as_bytes()).expect("rsa key"),
        )
        .expect("sign");

        let auth = JwtAuthenticator::from_config(JwtAuthConfig {
            public_key_pem: Some(RSA_PUBLIC_PEM.to_owned()),
            algorithm: Some("RS256".to_owned()),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        auth.authenticate(&metadata_with_bearer(&token))
            .await
            .expect("RS256 verifies");

        // An HS256 token signed with the *public key bytes* must be
        // refused — the classic key-confusion attack.
        let forged = token_with(&claims, RSA_PUBLIC_PEM);
        let err = auth
            .authenticate(&metadata_with_bearer(&forged))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)));
    }

    #[tokio::test]
    async fn es256_pem_verifies() {
        let claims = serde_json::json!({ "sub": "user-1", "exp": now_plus(60) });
        let token = encode(
            &Header::new(Algorithm::ES256),
            &claims,
            &EncodingKey::from_ec_pem(EC_PRIVATE_PEM.as_bytes()).expect("ec key"),
        )
        .expect("sign");
        let auth = JwtAuthenticator::from_config(JwtAuthConfig {
            public_key_pem: Some(EC_PUBLIC_PEM.to_owned()),
            algorithm: Some("ES256".to_owned()),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        auth.authenticate(&metadata_with_bearer(&token))
            .await
            .expect("ES256 verifies");
    }

    #[tokio::test]
    async fn inline_jwks_verifies_by_kid_and_refuses_hmac() {
        let claims = serde_json::json!({ "sub": "user-1", "exp": now_plus(60) });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-rsa".to_owned());
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(RSA_PRIVATE_PEM.as_bytes()).expect("rsa key"),
        )
        .expect("sign");

        let auth = JwtAuthenticator::from_config(JwtAuthConfig {
            jwks: Some(JWKS_JSON.to_owned()),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        auth.authenticate(&metadata_with_bearer(&token))
            .await
            .expect("JWKS kid match verifies");

        let forged = token_with(&claims, "whatever");
        let err = auth
            .authenticate(&metadata_with_bearer(&forged))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)), "{err:?}");
    }

    #[tokio::test]
    async fn jwks_url_fetches_at_startup() {
        // Serve the JWKS document from a local HTTP endpoint.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/.well-known/jwks.json",
            axum::routing::get(|| async { JWKS_JSON }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let claims = serde_json::json!({ "sub": "user-1", "exp": now_plus(60) });
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-rsa".to_owned());
        let token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(RSA_PRIVATE_PEM.as_bytes()).expect("rsa key"),
        )
        .expect("sign");

        let auth = JwtAuthenticator::from_config(JwtAuthConfig {
            jwks_url: Some(format!("http://{addr}/.well-known/jwks.json")),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("startup fetch succeeds");
        auth.authenticate(&metadata_with_bearer(&token))
            .await
            .expect("JWKS-URL key verifies");
    }

    #[tokio::test]
    async fn nbf_validates_with_configurable_leeway() {
        let secret = "topsecret";
        let claims = serde_json::json!({
            "sub": "user-1",
            "exp": now_plus(300),
            "nbf": now_plus(30),
        });
        let token = token_with(&claims, secret);

        // Not yet valid with zero leeway.
        let strict = JwtAuthenticator::from_config(JwtAuthConfig {
            secret: Some(secret.to_owned()),
            leeway_secs: Some(0),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        let err = strict
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)));

        // Accepted inside the configured clock-skew window.
        let lenient = JwtAuthenticator::from_config(JwtAuthConfig {
            secret: Some(secret.to_owned()),
            leeway_secs: Some(60),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        lenient
            .authenticate(&metadata_with_bearer(&token))
            .await
            .expect("nbf inside leeway");
    }

    #[tokio::test]
    async fn issuer_and_audience_validate() {
        let secret = "topsecret";
        let claims = serde_json::json!({
            "sub": "user-1",
            "exp": now_plus(60),
            "iss": "https://issuer.example",
            "aud": "palimpsest",
        });
        let token = token_with(&claims, secret);

        let auth = JwtAuthenticator::from_config(JwtAuthConfig {
            secret: Some(secret.to_owned()),
            issuer: Some("https://issuer.example".to_owned()),
            audience: Some("palimpsest".to_owned()),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        auth.authenticate(&metadata_with_bearer(&token))
            .await
            .expect("iss+aud match");

        let wrong_audience = JwtAuthenticator::from_config(JwtAuthConfig {
            secret: Some(secret.to_owned()),
            audience: Some("other".to_owned()),
            ..JwtAuthConfig::default()
        })
        .await
        .expect("config");
        let err = wrong_audience
            .authenticate(&metadata_with_bearer(&token))
            .await
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken(_)));
    }

    #[tokio::test]
    async fn conflicting_key_sources_are_refused() {
        let Err(err) = JwtAuthenticator::from_config(JwtAuthConfig {
            secret: Some("x".to_owned()),
            jwks: Some(JWKS_JSON.to_owned()),
            ..JwtAuthConfig::default()
        })
        .await
        else {
            panic!("two key sources must be refused");
        };
        assert!(matches!(err, AuthError::InvalidConfig(_)));

        let Err(err) = JwtAuthenticator::from_config(JwtAuthConfig::default()).await else {
            panic!("no key source must be refused");
        };
        assert!(matches!(err, AuthError::InvalidConfig(_)));
    }
}
