// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fs::File,
    io,
    time::Duration,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Uri};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use palimpsest_paas_core::ManagedPostgresEndpointCertificateBundle;
use palimpsest_paas_gateway::{
    DatabaseProxyRoute, DatabaseProxyRouteHandle, DatabaseProxyServer, DatabaseProxyServerConfig,
    DatabaseProxyTlsMaterial,
};
use serde::Deserialize;
use tokio::{task::JoinHandle, time};

struct DatabaseProxyListener {
    handle: DatabaseProxyRouteHandle,
    task: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let mut routes = Vec::new();
    if let Ok(routes_path) = env::var("PALIMPSEST_DB_PROXY_ROUTES") {
        routes.extend(load_database_proxy_routes(&routes_path)?);
    }
    let control_plane = env::var("PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL")
        .ok()
        .map(|url| {
            (
                url,
                env::var("PALIMPSEST_DB_PROXY_CONTROL_PLANE_TOKEN").ok(),
            )
        });
    if let Some((control_plane_url, token)) = control_plane.as_ref() {
        routes.extend(fetch_routes_from_control_plane(control_plane_url, token.as_deref()).await?);
    }
    if routes.is_empty() {
        routes.extend(load_database_proxy_routes(
            "paas/examples/database-proxy-route.json",
        )?);
    }
    if routes.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "database proxy route file must contain at least one route",
        )
        .into());
    }

    let mut route_handles = BTreeMap::new();
    let configs = route_configs(routes, control_plane.as_ref()).await?;
    apply_database_proxy_routes(configs, &mut route_handles).await?;

    let Some((control_plane_url, token)) = control_plane else {
        std::future::pending::<()>().await;
        return Ok(());
    };

    let refresh_interval = database_proxy_route_refresh_interval()?;
    loop {
        time::sleep(refresh_interval).await;
        match fetch_routes_from_control_plane(&control_plane_url, token.as_deref()).await {
            Ok(routes) => {
                match route_configs(routes, Some(&(control_plane_url.clone(), token.clone()))).await
                {
                    Ok(configs) => {
                        if let Err(err) =
                            apply_database_proxy_routes(configs, &mut route_handles).await
                        {
                            tracing::warn!(error = %err, "database proxy route refresh failed");
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "database proxy TLS material fetch failed");
                    }
                }
            }
            Err(err) => tracing::warn!(error = %err, "database proxy route fetch failed"),
        }
    }
}

async fn apply_database_proxy_routes(
    configs: Vec<DatabaseProxyServerConfig>,
    route_handles: &mut BTreeMap<String, DatabaseProxyListener>,
) -> Result<(), Box<dyn Error>> {
    let active_listeners = configs
        .iter()
        .map(|config| config.route.listen_addr.clone())
        .collect::<BTreeSet<_>>();
    let stale_listeners = route_handles
        .keys()
        .filter(|listen_addr| !active_listeners.contains(*listen_addr))
        .cloned()
        .collect::<Vec<_>>();
    for listen_addr in stale_listeners {
        if let Some(listener) = route_handles.remove(&listen_addr) {
            listener.task.abort();
            tracing::info!(%listen_addr, "stopped removed database proxy route");
        }
    }

    for config in configs {
        let route = config.route.clone();
        if let Some(listener) = route_handles.get(&route.listen_addr) {
            listener.handle.update_config(config).await?;
            tracing::info!(
                listen_addr = %route.listen_addr,
                upstream_addr = %route.upstream_addr,
                environment_id = %route.environment_id,
                cluster_id = %route.cluster_id,
                "updated database proxy route"
            );
            continue;
        }

        let server = DatabaseProxyServer::bind_config(config).await?;
        let handle = server.route_handle();
        let task = tokio::spawn(async move {
            if let Err(err) = server.serve().await {
                tracing::error!(error = %err, "database proxy listener stopped");
            }
        });
        route_handles.insert(
            route.listen_addr.clone(),
            DatabaseProxyListener { handle, task },
        );
        tracing::info!(
            listen_addr = %route.listen_addr,
            upstream_addr = %route.upstream_addr,
            environment_id = %route.environment_id,
            cluster_id = %route.cluster_id,
            "started database proxy listener"
        );
    }
    Ok(())
}

fn database_proxy_route_refresh_interval() -> Result<Duration, Box<dyn Error>> {
    let seconds = env::var("PALIMPSEST_DB_PROXY_ROUTE_REFRESH_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(15)
        .max(1);
    Ok(Duration::from_secs(seconds))
}

async fn route_configs(
    routes: Vec<DatabaseProxyRoute>,
    control_plane: Option<&(String, Option<String>)>,
) -> Result<Vec<DatabaseProxyServerConfig>, Box<dyn Error>> {
    let mut configs = Vec::with_capacity(routes.len());
    for route in routes {
        let tls = if let Some(tls) = route.tls.as_ref() {
            let Some((control_plane_url, token)) = control_plane else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "TLS database proxy routes require control-plane bundle discovery",
                )
                .into());
            };
            let bundle = fetch_certificate_bundle_from_control_plane(
                control_plane_url,
                token.as_deref(),
                &route.environment_id,
                &tls.certificate_id,
            )
            .await?;
            Some(DatabaseProxyTlsMaterial {
                certificate_pem: bundle.certificate_pem,
                private_key_pem: bundle.private_key_pem,
            })
        } else {
            None
        };
        configs.push(DatabaseProxyServerConfig { route, tls });
    }
    Ok(configs)
}

fn load_database_proxy_routes(
    routes_path: &str,
) -> Result<Vec<DatabaseProxyRoute>, Box<dyn Error>> {
    let value: serde_json::Value = serde_json::from_reader(File::open(routes_path)?)?;
    if value.is_array() {
        Ok(serde_json::from_value(value)?)
    } else {
        Ok(vec![serde_json::from_value(value)?])
    }
}

#[derive(Debug, Deserialize)]
struct DatabaseProxyRoutesResponse {
    routes: Vec<DatabaseProxyRoute>,
}

async fn fetch_routes_from_control_plane(
    control_plane_url: &str,
    bearer_token: Option<&str>,
) -> Result<Vec<DatabaseProxyRoute>, Box<dyn Error>> {
    let endpoint = format!(
        "{}/v1/database-proxy-routes",
        control_plane_url.trim_end_matches('/')
    );
    let uri: Uri = endpoint.parse()?;
    if uri.scheme_str() != Some("http") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PALIMPSEST_DB_PROXY_CONTROL_PLANE_URL must use http://",
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

    let routes: DatabaseProxyRoutesResponse = serde_json::from_slice(&body)?;
    Ok(routes.routes)
}

async fn fetch_certificate_bundle_from_control_plane(
    control_plane_url: &str,
    bearer_token: Option<&str>,
    environment_id: &str,
    certificate_id: &str,
) -> Result<ManagedPostgresEndpointCertificateBundle, Box<dyn Error>> {
    let endpoint = format!(
        "{}/v1/environments/{}/managed-postgres-endpoint/certificates/{}/bundle",
        control_plane_url.trim_end_matches('/'),
        environment_id,
        certificate_id
    );
    let body = get_control_plane_json(&endpoint, bearer_token).await?;
    Ok(serde_json::from_slice(&body)?)
}

async fn get_control_plane_json(
    endpoint: &str,
    bearer_token: Option<&str>,
) -> Result<Bytes, Box<dyn Error>> {
    let uri: Uri = endpoint.parse()?;
    if uri.scheme_str() != Some("http") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "control-plane URLs must use http://",
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
    Ok(body)
}
