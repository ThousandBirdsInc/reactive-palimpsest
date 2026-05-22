// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs::File,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use palimpsest_paas_gateway::{
    gateway_router, GatewayRoute, GatewayRouteMtlsBundle, GatewayState, SharedGatewayState,
    TlsPolicy,
};
use serde::{de::DeserializeOwned, Deserialize};
use tokio::{net::TcpListener, time};

const DEFAULT_GATEWAY_ADDR: &str = "127.0.0.1:8089";
const DEFAULT_GATEWAY_ROUTE_REFRESH_SECS: u64 = 15;

type BoxError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let addr =
        env::var("PALIMPSEST_GATEWAY_ADDR").unwrap_or_else(|_| DEFAULT_GATEWAY_ADDR.to_owned());
    let state = Arc::new(Mutex::new(GatewayState::default()));

    if let Ok(secrets_path) = env::var("PALIMPSEST_GATEWAY_MTLS_SECRETS") {
        load_mtls_secrets_into_state(&secrets_path, &state)?;
    }
    if let Ok(routes_path) = env::var("PALIMPSEST_GATEWAY_ROUTES") {
        load_routes_into_state(&routes_path, &state)?;
    }
    let control_plane = env::var("PALIMPSEST_GATEWAY_CONTROL_PLANE_URL")
        .ok()
        .map(|url| (url, env::var("PALIMPSEST_GATEWAY_CONTROL_PLANE_TOKEN").ok()));
    if let Some((control_plane_url, token)) = control_plane.as_ref() {
        let routes = fetch_routes_from_control_plane(control_plane_url, token.as_deref()).await?;
        let mtls_secret_pems =
            fetch_mtls_secret_pems_from_control_plane(control_plane_url, token.as_deref(), &routes)
                .await?;
        replace_mtls_secrets_in_state(mtls_secret_pems, &state)?;
        replace_routes_in_state(routes, &state)?;
    }
    if let Some((control_plane_url, token)) = control_plane {
        start_gateway_route_refresh(control_plane_url, token, Arc::clone(&state))?;
    }

    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "palimpsest paas gateway listening");
    axum::serve(listener, gateway_router(state)).await?;
    Ok(())
}

fn load_routes_into_state(routes_path: &str, state: &SharedGatewayState) -> Result<(), BoxError> {
    let routes = load_routes(routes_path)?;
    upsert_routes_into_state(routes, state)
}

fn upsert_routes_into_state(
    routes: Vec<GatewayRoute>,
    state: &SharedGatewayState,
) -> Result<(), BoxError> {
    let route_count = routes.len();
    let mut state = state
        .lock()
        .map_err(|_| std::io::Error::other("gateway state lock is poisoned"))?;
    for route in routes {
        state.upsert_route(route)?;
    }
    tracing::info!(route_count, "loaded gateway routes");
    Ok(())
}

fn replace_routes_in_state(
    routes: Vec<GatewayRoute>,
    state: &SharedGatewayState,
) -> Result<(), BoxError> {
    let route_count = routes.len();
    let mut state = state
        .lock()
        .map_err(|_| std::io::Error::other("gateway state lock is poisoned"))?;
    state.replace_routes(routes)?;
    tracing::info!(route_count, "replaced gateway routes from control plane");
    Ok(())
}

fn replace_mtls_secrets_in_state(
    secrets: BTreeMap<String, String>,
    state: &SharedGatewayState,
) -> Result<(), BoxError> {
    let secret_count = secrets.len();
    state
        .lock()
        .map_err(|_| std::io::Error::other("gateway state lock is poisoned"))?
        .replace_mtls_secret_pems(secrets)?;
    tracing::info!(
        secret_count,
        "replaced gateway mTLS secrets from control plane"
    );
    Ok(())
}

fn load_routes(routes_path: &str) -> Result<Vec<GatewayRoute>, BoxError> {
    let value: serde_json::Value = serde_json::from_reader(File::open(routes_path)?)?;
    if value.is_array() {
        Ok(serde_json::from_value(value)?)
    } else {
        Ok(vec![serde_json::from_value(value)?])
    }
}

fn load_mtls_secrets_into_state(
    secrets_path: &str,
    state: &SharedGatewayState,
) -> Result<(), BoxError> {
    let secrets: BTreeMap<String, String> = serde_json::from_reader(File::open(secrets_path)?)?;
    let secret_count = secrets.len();
    state
        .lock()
        .map_err(|_| std::io::Error::other("gateway state lock is poisoned"))?
        .replace_mtls_secret_pems(secrets)?;
    tracing::info!(secret_count, "loaded gateway mTLS secret material");
    Ok(())
}

#[derive(Debug, Deserialize)]
struct GatewayRoutesResponse {
    routes: Vec<GatewayRoute>,
}

async fn fetch_routes_from_control_plane(
    control_plane_url: &str,
    bearer_token: Option<&str>,
) -> Result<Vec<GatewayRoute>, BoxError> {
    let endpoint = format!(
        "{}/v1/gateway-routes",
        control_plane_url.trim_end_matches('/')
    );
    let routes: GatewayRoutesResponse = get_control_plane_json(&endpoint, bearer_token).await?;
    Ok(routes.routes)
}

async fn fetch_mtls_secret_pems_from_control_plane(
    control_plane_url: &str,
    bearer_token: Option<&str>,
    routes: &[GatewayRoute],
) -> Result<BTreeMap<String, String>, BoxError> {
    let mut secrets = BTreeMap::new();
    for route in routes {
        if !matches!(route.tls_policy, TlsPolicy::MutualTlsToSync { .. }) {
            continue;
        }
        let endpoint = format!(
            "{}/v1/gateway-routes/{}/mtls-bundle",
            control_plane_url.trim_end_matches('/'),
            route.host
        );
        let bundle: GatewayRouteMtlsBundle =
            get_control_plane_json(&endpoint, bearer_token).await?;
        secrets.insert(bundle.ca_secret_ref, bundle.ca_pem);
        if let (Some(secret_ref), Some(pem)) = (
            bundle.client_certificate_secret_ref,
            bundle.client_certificate_pem,
        ) {
            secrets.insert(secret_ref, pem);
        }
        if let (Some(secret_ref), Some(pem)) = (
            bundle.client_private_key_secret_ref,
            bundle.client_private_key_pem,
        ) {
            secrets.insert(secret_ref, pem);
        }
    }
    Ok(secrets)
}

async fn get_control_plane_json<T: DeserializeOwned>(
    endpoint: &str,
    bearer_token: Option<&str>,
) -> Result<T, BoxError> {
    let uri: Uri = endpoint.parse()?;
    if uri.scheme_str() != Some("http") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PALIMPSEST_GATEWAY_CONTROL_PLANE_URL must use http://",
        )
        .into());
    }

    let mut request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("accept", "application/json");
    if let Some(token) = bearer_token
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        request = request.header("authorization", format!("Bearer {token}"));
    }

    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let response = client
        .request(request.body(Full::new(Bytes::new()))?)
        .await?;
    let status = response.status();
    let body = response.into_body().collect().await?.to_bytes();
    if !status.is_success() {
        return Err(io::Error::other(format!(
            "control plane returned HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        ))
        .into());
    }

    Ok(serde_json::from_slice(&body)?)
}

fn start_gateway_route_refresh(
    control_plane_url: String,
    token: Option<String>,
    state: SharedGatewayState,
) -> Result<(), BoxError> {
    let refresh_interval = gateway_route_refresh_interval()?;
    tokio::spawn(async move {
        loop {
            time::sleep(refresh_interval).await;
            match fetch_routes_from_control_plane(&control_plane_url, token.as_deref()).await {
                Ok(routes) => {
                    match fetch_mtls_secret_pems_from_control_plane(
                        &control_plane_url,
                        token.as_deref(),
                        &routes,
                    )
                    .await
                    {
                        Ok(secrets) => {
                            if let Err(err) = replace_mtls_secrets_in_state(secrets, &state) {
                                tracing::warn!(error = %err, "gateway mTLS secret refresh failed");
                                continue;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "gateway mTLS secret fetch failed");
                            continue;
                        }
                    }
                    if let Err(err) = replace_routes_in_state(routes, &state) {
                        tracing::warn!(error = %err, "gateway route refresh failed");
                    }
                }
                Err(err) => tracing::warn!(error = %err, "gateway route fetch failed"),
            }
        }
    });
    Ok(())
}

fn gateway_route_refresh_interval() -> Result<Duration, BoxError> {
    gateway_route_refresh_interval_from_env(env::var("PALIMPSEST_GATEWAY_ROUTE_REFRESH_SECS").ok())
}

fn gateway_route_refresh_interval_from_env(value: Option<String>) -> Result<Duration, BoxError> {
    let seconds = value
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(DEFAULT_GATEWAY_ROUTE_REFRESH_SECS)
        .max(1);
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_route_refresh_interval_defaults_to_15_seconds() {
        assert_eq!(
            gateway_route_refresh_interval_from_env(None).unwrap(),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn gateway_route_refresh_interval_has_one_second_floor() {
        assert_eq!(
            gateway_route_refresh_interval_from_env(Some("0".to_owned())).unwrap(),
            Duration::from_secs(1)
        );
    }
}
