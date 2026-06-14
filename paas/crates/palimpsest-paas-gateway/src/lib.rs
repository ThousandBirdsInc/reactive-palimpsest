// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Hosted gateway primitives for the Palimpsest `PaaS`.

use std::{
    collections::BTreeMap,
    io::{self, BufReader, Cursor},
    sync::{Arc, Mutex},
};

use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{
        header::HOST, HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode,
        Uri,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
pub use palimpsest_paas_core::{
    DatabaseProxyPolicy, DatabaseProxyRoute, GatewayRoute, GatewayRouteMtlsBundle, RateLimitPolicy,
    TlsPolicy, UsageEvent,
};
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore, ServerConfig};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{copy_bidirectional, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub type SharedGatewayState = Arc<Mutex<GatewayState>>;
const MAX_PROXY_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const POSTGRES_PROTOCOL_VERSION_3: u32 = 196_608;
const POSTGRES_CANCEL_REQUEST_CODE: u32 = 80_877_102;
const POSTGRES_SSL_REQUEST_CODE: u32 = 80_877_103;
const MAX_POSTGRES_STARTUP_PACKET_BYTES: usize = 64 * 1024;
const MAX_POSTGRES_PROXY_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

pub struct DatabaseProxyServer {
    state: Arc<RwLock<DatabaseProxyRouteState>>,
    listener: TcpListener,
}

impl DatabaseProxyServer {
    pub async fn bind(route: DatabaseProxyRoute) -> Result<Self, DatabaseProxyError> {
        Self::bind_config(DatabaseProxyServerConfig { route, tls: None }).await
    }

    pub async fn bind_config(
        config: DatabaseProxyServerConfig,
    ) -> Result<Self, DatabaseProxyError> {
        let state = DatabaseProxyRouteState::try_from_config(config)?;
        let listener = TcpListener::bind(&state.route.listen_addr).await?;
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            listener,
        })
    }

    pub async fn route(&self) -> DatabaseProxyRoute {
        self.state.read().await.route.clone()
    }

    pub fn route_handle(&self) -> DatabaseProxyRouteHandle {
        DatabaseProxyRouteHandle {
            state: Arc::clone(&self.state),
        }
    }

    pub async fn serve(self) -> Result<(), DatabaseProxyError> {
        loop {
            let (client, peer_addr) = self.listener.accept().await?;
            let state = Arc::clone(&self.state);
            tokio::spawn(async move {
                let state = state.read().await.clone();
                let proxy_result = if let Some(acceptor) = state.tls_acceptor {
                    proxy_tls_database_connection(
                        client,
                        acceptor,
                        &state.route.upstream_addr,
                        state.route.policy.as_ref(),
                    )
                    .await
                } else {
                    proxy_database_route_connection(
                        client,
                        &state.route.upstream_addr,
                        state.route.policy.as_ref(),
                    )
                    .await
                };
                match proxy_result {
                    Ok((client_to_upstream, upstream_to_client)) => tracing::info!(
                        listen_addr = %state.route.listen_addr,
                        upstream_addr = %state.route.upstream_addr,
                        environment_id = %state.route.environment_id,
                        cluster_id = %state.route.cluster_id,
                        %peer_addr,
                        client_to_upstream,
                        upstream_to_client,
                        "database proxy connection closed"
                    ),
                    Err(err) => tracing::warn!(
                        listen_addr = %state.route.listen_addr,
                        upstream_addr = %state.route.upstream_addr,
                        environment_id = %state.route.environment_id,
                        cluster_id = %state.route.cluster_id,
                        %peer_addr,
                        error = %err,
                        "database proxy connection failed"
                    ),
                }
            });
        }
    }
}

#[derive(Clone)]
pub struct DatabaseProxyRouteHandle {
    state: Arc<RwLock<DatabaseProxyRouteState>>,
}

impl DatabaseProxyRouteHandle {
    pub async fn listen_addr(&self) -> String {
        self.state.read().await.route.listen_addr.clone()
    }

    pub async fn route(&self) -> DatabaseProxyRoute {
        self.state.read().await.route.clone()
    }

    pub async fn update_route(&self, route: DatabaseProxyRoute) -> Result<(), DatabaseProxyError> {
        self.update_config(DatabaseProxyServerConfig { route, tls: None })
            .await
    }

    pub async fn update_config(
        &self,
        config: DatabaseProxyServerConfig,
    ) -> Result<(), DatabaseProxyError> {
        let state = DatabaseProxyRouteState::try_from_config(config)?;
        let listen_addr = self.listen_addr().await;
        if state.route.listen_addr != listen_addr {
            return Err(DatabaseProxyError::InvalidRoute(
                "updated route listen_addr must match existing listener",
            ));
        }
        *self.state.write().await = state;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseProxyServerConfig {
    pub route: DatabaseProxyRoute,
    pub tls: Option<DatabaseProxyTlsMaterial>,
}

#[derive(Debug, Clone)]
pub struct DatabaseProxyTlsMaterial {
    pub certificate_pem: String,
    pub private_key_pem: String,
}

#[derive(Clone)]
struct DatabaseProxyRouteState {
    route: DatabaseProxyRoute,
    tls_acceptor: Option<TlsAcceptor>,
}

impl DatabaseProxyRouteState {
    fn try_from_config(config: DatabaseProxyServerConfig) -> Result<Self, DatabaseProxyError> {
        validate_database_proxy_route(&config.route)?;
        let tls_acceptor = config
            .tls
            .as_ref()
            .map(database_proxy_tls_acceptor)
            .transpose()?;
        Ok(Self {
            route: config.route,
            tls_acceptor,
        })
    }
}

pub async fn proxy_database_connection(
    mut client: TcpStream,
    upstream_addr: &str,
) -> Result<(u64, u64), DatabaseProxyError> {
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    proxy_database_streams(&mut client, &mut upstream).await
}

async fn proxy_database_route_connection(
    mut client: TcpStream,
    upstream_addr: &str,
    policy: Option<&DatabaseProxyPolicy>,
) -> Result<(u64, u64), DatabaseProxyError> {
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    if let Some(policy) = policy {
        proxy_validated_postgres_streams_with_policy(&mut client, &mut upstream, Some(policy)).await
    } else {
        proxy_database_streams(&mut client, &mut upstream).await
    }
}

pub async fn proxy_tls_database_connection(
    mut client: TcpStream,
    acceptor: TlsAcceptor,
    upstream_addr: &str,
    policy: Option<&DatabaseProxyPolicy>,
) -> Result<(u64, u64), DatabaseProxyError> {
    let mut request = [0_u8; 8];
    client.read_exact(&mut request).await?;
    if request != postgres_ssl_request() {
        return Err(DatabaseProxyError::InvalidPostgresTlsRequest);
    }
    client.write_all(b"S").await?;
    let mut client = acceptor
        .accept(client)
        .await
        .map_err(|err| DatabaseProxyError::Tls(err.to_string()))?;
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    proxy_validated_postgres_streams_with_policy(&mut client, &mut upstream, policy).await
}

async fn proxy_database_streams<C, U>(
    client: &mut C,
    upstream: &mut U,
) -> Result<(u64, u64), DatabaseProxyError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    copy_bidirectional(client, upstream)
        .await
        .map_err(DatabaseProxyError::Io)
}

async fn proxy_validated_postgres_streams_with_policy<C, U>(
    client: &mut C,
    upstream: &mut U,
    policy: Option<&DatabaseProxyPolicy>,
) -> Result<(u64, u64), DatabaseProxyError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let startup_packet = read_postgres_startup_packet(client).await?;
    let startup = validate_postgres_startup_packet(&startup_packet)?;
    if let Some(policy) = policy {
        enforce_database_proxy_policy(policy, &startup)?;
    }
    upstream.write_all(&startup_packet).await?;
    let (client_to_upstream, upstream_to_client) = if let Some(policy) = policy {
        if startup == PostgresStartup::CancelRequest {
            proxy_database_streams(client, upstream).await?
        } else if database_proxy_policy_inspects_messages(policy) {
            proxy_postgres_messages_with_policy(client, upstream, policy).await?
        } else {
            proxy_database_streams(client, upstream).await?
        }
    } else {
        proxy_database_streams(client, upstream).await?
    };
    Ok((
        client_to_upstream + u64::try_from(startup_packet.len()).unwrap_or(u64::MAX),
        upstream_to_client,
    ))
}

async fn proxy_postgres_messages_with_policy<C, U>(
    client: &mut C,
    upstream: &mut U,
    policy: &DatabaseProxyPolicy,
) -> Result<(u64, u64), DatabaseProxyError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let mut client_to_upstream = 0_u64;
    let mut upstream_to_client = 0_u64;
    let mut pending_client = Vec::new();
    let mut client_buf = [0_u8; 8192];
    let mut upstream_buf = [0_u8; 8192];
    let mut client_closed = false;
    let mut upstream_closed = false;

    loop {
        if client_closed && upstream_closed {
            break;
        }

        tokio::select! {
            read = client.read(&mut client_buf), if !client_closed => {
                let read = read.map_err(DatabaseProxyError::Io)?;
                if read == 0 {
                    client_closed = true;
                    if !pending_client.is_empty() {
                        return Err(DatabaseProxyError::InvalidPostgresMessage);
                    }
                    upstream.shutdown().await.map_err(DatabaseProxyError::Io)?;
                    continue;
                }
                pending_client.extend_from_slice(&client_buf[..read]);
                if pending_client.len() > MAX_POSTGRES_PROXY_MESSAGE_BYTES {
                    return Err(DatabaseProxyError::InvalidPostgresMessage);
                }
                let frames = drain_validated_postgres_client_messages(&mut pending_client, policy)?;
                for frame in frames {
                    upstream.write_all(&frame).await.map_err(DatabaseProxyError::Io)?;
                    client_to_upstream = client_to_upstream
                        .saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX));
                }
            }
            read = upstream.read(&mut upstream_buf), if !upstream_closed => {
                let read = read.map_err(DatabaseProxyError::Io)?;
                if read == 0 {
                    upstream_closed = true;
                    client.shutdown().await.map_err(DatabaseProxyError::Io)?;
                    continue;
                }
                client.write_all(&upstream_buf[..read]).await.map_err(DatabaseProxyError::Io)?;
                upstream_to_client = upstream_to_client
                    .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
            }
        }
    }

    Ok((client_to_upstream, upstream_to_client))
}

async fn read_postgres_startup_packet<C>(client: &mut C) -> Result<Vec<u8>, DatabaseProxyError>
where
    C: AsyncRead + Unpin,
{
    let mut length_bytes = [0_u8; 4];
    client.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes);
    let length = usize::try_from(length).map_err(|_| DatabaseProxyError::InvalidPostgresStartup)?;
    if !(8..=MAX_POSTGRES_STARTUP_PACKET_BYTES).contains(&length) {
        return Err(DatabaseProxyError::InvalidPostgresStartup);
    }
    let mut packet = Vec::with_capacity(length);
    packet.extend_from_slice(&length_bytes);
    packet.resize(length, 0);
    client.read_exact(&mut packet[4..]).await?;
    Ok(packet)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PostgresStartup {
    Startup {
        parameters: BTreeMap<String, String>,
    },
    CancelRequest,
}

fn validate_postgres_startup_packet(packet: &[u8]) -> Result<PostgresStartup, DatabaseProxyError> {
    if packet.len() < 8 {
        return Err(DatabaseProxyError::InvalidPostgresStartup);
    }
    let length = u32::from_be_bytes(packet[..4].try_into().expect("slice has 4 bytes"));
    if usize::try_from(length).ok() != Some(packet.len()) {
        return Err(DatabaseProxyError::InvalidPostgresStartup);
    }
    let protocol = u32::from_be_bytes(packet[4..8].try_into().expect("slice has 4 bytes"));
    match protocol {
        POSTGRES_PROTOCOL_VERSION_3 => validate_postgres_startup_parameters(&packet[8..]),
        POSTGRES_CANCEL_REQUEST_CODE => {
            if packet.len() == 16 {
                Ok(PostgresStartup::CancelRequest)
            } else {
                Err(DatabaseProxyError::InvalidPostgresStartup)
            }
        }
        POSTGRES_SSL_REQUEST_CODE => Err(DatabaseProxyError::NestedPostgresTlsRequest),
        _ => Err(DatabaseProxyError::InvalidPostgresStartup),
    }
}

fn validate_postgres_startup_parameters(
    parameters: &[u8],
) -> Result<PostgresStartup, DatabaseProxyError> {
    if parameters.last().copied() != Some(0) {
        return Err(DatabaseProxyError::InvalidPostgresStartup);
    }
    let mut parts = parameters.split(|byte| *byte == 0);
    let mut parsed = BTreeMap::new();
    while let Some(key) = parts.next() {
        if key.is_empty() {
            if parts.any(|part| !part.is_empty()) {
                return Err(DatabaseProxyError::InvalidPostgresStartup);
            }
            break;
        }
        let Some(value) = parts.next() else {
            return Err(DatabaseProxyError::InvalidPostgresStartup);
        };
        if value.is_empty() {
            return Err(DatabaseProxyError::InvalidPostgresStartup);
        }
        let key =
            std::str::from_utf8(key).map_err(|_| DatabaseProxyError::InvalidPostgresStartup)?;
        let value =
            std::str::from_utf8(value).map_err(|_| DatabaseProxyError::InvalidPostgresStartup)?;
        parsed.insert(key.to_owned(), value.to_owned());
    }
    if parsed.contains_key("user") {
        Ok(PostgresStartup::Startup { parameters: parsed })
    } else {
        Err(DatabaseProxyError::InvalidPostgresStartup)
    }
}

fn enforce_database_proxy_policy(
    policy: &DatabaseProxyPolicy,
    startup: &PostgresStartup,
) -> Result<(), DatabaseProxyError> {
    let PostgresStartup::Startup { parameters } = startup else {
        return Ok(());
    };
    if let Some(user) = parameters.get("user") {
        if !policy.allowed_users.is_empty()
            && !policy.allowed_users.iter().any(|allowed| allowed == user)
        {
            return Err(DatabaseProxyError::PolicyRejected {
                field: "user",
                value: user.clone(),
            });
        }
    }
    if let Some(database) = parameters.get("database") {
        if !policy.allowed_databases.is_empty()
            && !policy
                .allowed_databases
                .iter()
                .any(|allowed| allowed == database)
        {
            return Err(DatabaseProxyError::PolicyRejected {
                field: "database",
                value: database.clone(),
            });
        }
    }
    for parameter in &policy.forbidden_startup_parameters {
        if parameters.contains_key(parameter) {
            return Err(DatabaseProxyError::PolicyRejected {
                field: "forbidden_parameter",
                value: parameter.clone(),
            });
        }
    }
    for (parameter, expected_value) in &policy.required_startup_parameters {
        if parameters.get(parameter) != Some(expected_value) {
            return Err(DatabaseProxyError::PolicyRejected {
                field: "required_parameter",
                value: parameter.clone(),
            });
        }
    }
    Ok(())
}

fn database_proxy_policy_inspects_messages(policy: &DatabaseProxyPolicy) -> bool {
    !policy.forbidden_simple_query_verbs.is_empty() || policy.max_simple_query_bytes.is_some()
}

fn drain_validated_postgres_client_messages(
    pending: &mut Vec<u8>,
    policy: &DatabaseProxyPolicy,
) -> Result<Vec<Vec<u8>>, DatabaseProxyError> {
    let mut frames = Vec::new();
    loop {
        if pending.is_empty() {
            break;
        }
        if pending.len() < 5 {
            break;
        }

        let message_type = pending[0];
        if !is_known_postgres_frontend_message_type(message_type) {
            return Err(DatabaseProxyError::InvalidPostgresMessage);
        }

        let length = u32::from_be_bytes(
            pending[1..5]
                .try_into()
                .expect("slice has exactly four bytes"),
        );
        let length =
            usize::try_from(length).map_err(|_| DatabaseProxyError::InvalidPostgresMessage)?;
        if length < 4 {
            return Err(DatabaseProxyError::InvalidPostgresMessage);
        }
        let frame_len = length
            .checked_add(1)
            .ok_or(DatabaseProxyError::InvalidPostgresMessage)?;
        if frame_len > MAX_POSTGRES_PROXY_MESSAGE_BYTES {
            return Err(DatabaseProxyError::InvalidPostgresMessage);
        }
        if pending.len() < frame_len {
            break;
        }

        let frame: Vec<u8> = pending.drain(..frame_len).collect();
        validate_postgres_client_message(&frame, policy)?;
        frames.push(frame);
    }
    Ok(frames)
}

fn validate_postgres_client_message(
    frame: &[u8],
    policy: &DatabaseProxyPolicy,
) -> Result<(), DatabaseProxyError> {
    if frame.len() < 5 {
        return Err(DatabaseProxyError::InvalidPostgresMessage);
    }
    if frame[0] != b'Q' {
        return Ok(());
    }

    let query_bytes = &frame[5..];
    if let Some(max_bytes) = policy.max_simple_query_bytes {
        if query_bytes.len() > usize::try_from(max_bytes).unwrap_or(usize::MAX) {
            return Err(DatabaseProxyError::PolicyRejected {
                field: "simple_query_bytes",
                value: query_bytes.len().to_string(),
            });
        }
    }
    if query_bytes.last().copied() != Some(0) {
        return Err(DatabaseProxyError::InvalidPostgresMessage);
    }
    if policy.forbidden_simple_query_verbs.is_empty() {
        return Ok(());
    }

    let query = std::str::from_utf8(&query_bytes[..query_bytes.len().saturating_sub(1)])
        .map_err(|_| DatabaseProxyError::InvalidPostgresMessage)?;
    let verb = query
        .trim_start()
        .split(|ch: char| !ch.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    if !verb.is_empty()
        && policy
            .forbidden_simple_query_verbs
            .iter()
            .any(|forbidden| forbidden.eq_ignore_ascii_case(verb))
    {
        return Err(DatabaseProxyError::PolicyRejected {
            field: "simple_query_verb",
            value: verb.to_ascii_lowercase(),
        });
    }

    Ok(())
}

const fn is_known_postgres_frontend_message_type(message_type: u8) -> bool {
    matches!(
        message_type,
        b'B' | b'C' | b'c' | b'd' | b'D' | b'E' | b'F' | b'H' | b'P' | b'p' | b'Q' | b'S' | b'X'
    )
}

fn database_proxy_tls_acceptor(
    material: &DatabaseProxyTlsMaterial,
) -> Result<TlsAcceptor, DatabaseProxyError> {
    let mut cert_reader = BufReader::new(Cursor::new(material.certificate_pem.as_bytes()));
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| DatabaseProxyError::Tls(format!("parse certificate PEM: {err}")))?;
    if certs.is_empty() {
        return Err(DatabaseProxyError::Tls(
            "certificate PEM contained no certificates".to_owned(),
        ));
    }

    let mut key_reader = BufReader::new(Cursor::new(material.private_key_pem.as_bytes()));
    let private_key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|err| DatabaseProxyError::Tls(format!("parse private key PEM: {err}")))?
        .ok_or_else(|| DatabaseProxyError::Tls("private key PEM contained no key".to_owned()))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .map_err(|err| DatabaseProxyError::Tls(format!("load TLS config: {err}")))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn postgres_ssl_request() -> [u8; 8] {
    let mut request = [0_u8; 8];
    request[..4].copy_from_slice(&8_u32.to_be_bytes());
    request[4..].copy_from_slice(&POSTGRES_SSL_REQUEST_CODE.to_be_bytes());
    request
}

fn validate_database_proxy_route(route: &DatabaseProxyRoute) -> Result<(), DatabaseProxyError> {
    if route.listen_addr.trim().is_empty() {
        return Err(DatabaseProxyError::InvalidRoute("listen_addr is empty"));
    }
    if route.upstream_addr.trim().is_empty() {
        return Err(DatabaseProxyError::InvalidRoute("upstream_addr is empty"));
    }
    if route.environment_id.trim().is_empty() {
        return Err(DatabaseProxyError::InvalidRoute("environment_id is empty"));
    }
    if route.cluster_id.trim().is_empty() {
        return Err(DatabaseProxyError::InvalidRoute("cluster_id is empty"));
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct GatewayRouter {
    routes_by_host: BTreeMap<String, GatewayRoute>,
}

impl GatewayRouter {
    pub fn upsert_route(&mut self, route: GatewayRoute) -> Result<(), GatewayError> {
        let route = normalize_route(route)?;
        self.routes_by_host.insert(route.host.clone(), route);
        Ok(())
    }

    pub fn replace_routes(&mut self, routes: Vec<GatewayRoute>) -> Result<(), GatewayError> {
        let mut routes_by_host = BTreeMap::new();
        for route in routes {
            let route = normalize_route(route)?;
            routes_by_host.insert(route.host.clone(), route);
        }
        self.routes_by_host = routes_by_host;
        Ok(())
    }

    pub fn resolve(&self, host: &str) -> Result<&GatewayRoute, GatewayError> {
        let host = normalize_host(host)?;
        self.routes_by_host
            .get(&host)
            .ok_or(GatewayError::RouteNotFound(host))
    }
}

#[derive(Debug, Default)]
pub struct GatewayLimiter {
    state_by_environment: BTreeMap<String, EnvironmentLimitState>,
}

impl GatewayLimiter {
    pub fn admit_connection(&mut self, route: &GatewayRoute) -> Result<(), GatewayError> {
        let state = self
            .state_by_environment
            .entry(route.environment_id.clone())
            .or_default();
        if state.open_connections >= route.rate_limit.max_connections {
            return Err(GatewayError::RateLimited {
                environment_id: route.environment_id.clone(),
                reason: "connection_limit",
            });
        }
        state.open_connections += 1;
        Ok(())
    }

    pub fn close_connection(&mut self, environment_id: &str) {
        if let Some(state) = self.state_by_environment.get_mut(environment_id) {
            state.open_connections = state.open_connections.saturating_sub(1);
        }
    }

    pub fn record_request(&mut self, route: &GatewayRoute) -> Result<(), GatewayError> {
        let state = self
            .state_by_environment
            .entry(route.environment_id.clone())
            .or_default();
        if state.requests_this_minute >= route.rate_limit.max_requests_per_minute {
            return Err(GatewayError::RateLimited {
                environment_id: route.environment_id.clone(),
                reason: "request_limit",
            });
        }
        state.requests_this_minute += 1;
        Ok(())
    }

    pub fn reset_minute_window(&mut self) {
        for state in self.state_by_environment.values_mut() {
            state.requests_this_minute = 0;
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct EnvironmentLimitState {
    open_connections: u32,
    requests_this_minute: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressUsageEvent {
    pub environment_id: String,
    pub bytes: u64,
}

#[derive(Debug, Default)]
pub struct EgressAccountant {
    bytes_by_environment: BTreeMap<String, u64>,
    pending_by_identity: BTreeMap<UsageIdentity, u64>,
    next_usage_event_id: u64,
}

impl EgressAccountant {
    pub fn record(&mut self, event: EgressUsageEvent) {
        *self
            .bytes_by_environment
            .entry(event.environment_id)
            .or_default() += event.bytes;
    }

    pub fn record_route_egress(&mut self, route: &GatewayRoute, bytes: u64) {
        self.record(EgressUsageEvent {
            environment_id: route.environment_id.clone(),
            bytes,
        });
        *self
            .pending_by_identity
            .entry(UsageIdentity::from_route(route))
            .or_default() += bytes;
    }

    #[must_use]
    pub fn bytes_for_environment(&self, environment_id: &str) -> u64 {
        self.bytes_by_environment
            .get(environment_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn drain_usage_events(&mut self, occurred_at: impl Into<String>) -> Vec<UsageEvent> {
        let occurred_at = occurred_at.into();
        let pending = std::mem::take(&mut self.pending_by_identity);
        pending
            .into_iter()
            .map(|(identity, quantity)| {
                self.next_usage_event_id += 1;
                let sequence = self.next_usage_event_id;
                UsageEvent {
                    event_id: format!("gateway_usage_{sequence}"),
                    idempotency_key: format!(
                        "gateway:{}:{}:{sequence}",
                        identity.environment_id, identity.metric
                    ),
                    organization_id: identity.organization_id,
                    project_id: identity.project_id,
                    environment_id: identity.environment_id,
                    metric: identity.metric,
                    quantity,
                    occurred_at: occurred_at.clone(),
                    signature: None,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UsageIdentity {
    organization_id: String,
    project_id: String,
    environment_id: String,
    metric: String,
}

impl UsageIdentity {
    fn from_route(route: &GatewayRoute) -> Self {
        Self {
            organization_id: route.organization_id.clone(),
            project_id: route.project_id.clone(),
            environment_id: route.environment_id.clone(),
            metric: "sync_egress_bytes".to_owned(),
        }
    }
}

#[derive(Debug, Default)]
pub struct GatewayMetrics {
    total_requests: u64,
    routed_requests: u64,
    rejected_requests: u64,
    rate_limited_requests: u64,
}

impl GatewayMetrics {
    fn record_routed(&mut self) {
        self.total_requests += 1;
        self.routed_requests += 1;
    }

    fn record_rejected(&mut self, err: &GatewayError) {
        self.total_requests += 1;
        match err {
            GatewayError::RateLimited { .. } => self.rate_limited_requests += 1,
            GatewayError::RouteNotFound(_)
            | GatewayError::InvalidRoute(_)
            | GatewayError::Proxy(_) => {
                self.rejected_requests += 1;
            }
            GatewayError::StateUnavailable => {}
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GatewayMetricsSnapshot {
    pub total_requests: u64,
    pub routed_requests: u64,
    pub rejected_requests: u64,
    pub rate_limited_requests: u64,
    pub egress_bytes_by_environment: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
pub struct GatewayState {
    router: GatewayRouter,
    limiter: GatewayLimiter,
    accountant: EgressAccountant,
    metrics: GatewayMetrics,
    proxy_mode: GatewayProxyMode,
    mtls_secrets: GatewayMtlsSecretStore,
}

impl GatewayState {
    pub fn with_static_upstream_response(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            proxy_mode: GatewayProxyMode::Static {
                status,
                body: body.into(),
            },
            ..Self::default()
        }
    }

    pub fn upsert_route(&mut self, route: GatewayRoute) -> Result<(), GatewayError> {
        self.router.upsert_route(route)
    }

    pub fn replace_routes(&mut self, routes: Vec<GatewayRoute>) -> Result<(), GatewayError> {
        self.router.replace_routes(routes)
    }

    pub fn upsert_mtls_secret_pem(
        &mut self,
        secret_ref: impl Into<String>,
        pem: impl Into<String>,
    ) -> Result<(), GatewayError> {
        self.mtls_secrets.upsert(secret_ref, pem)
    }

    pub fn replace_mtls_secret_pems(
        &mut self,
        secrets: BTreeMap<String, String>,
    ) -> Result<(), GatewayError> {
        self.mtls_secrets.replace(secrets)
    }

    pub fn egress_bytes_for_environment(&self, environment_id: &str) -> u64 {
        self.accountant.bytes_for_environment(environment_id)
    }

    pub fn drain_usage_events(&mut self, occurred_at: impl Into<String>) -> Vec<UsageEvent> {
        self.accountant.drain_usage_events(occurred_at)
    }

    pub fn metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        GatewayMetricsSnapshot {
            total_requests: self.metrics.total_requests,
            routed_requests: self.metrics.routed_requests,
            rejected_requests: self.metrics.rejected_requests,
            rate_limited_requests: self.metrics.rate_limited_requests,
            egress_bytes_by_environment: self.accountant.bytes_by_environment.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
enum GatewayProxyMode {
    #[default]
    Http,
    Static {
        status: StatusCode,
        body: Bytes,
    },
}

#[derive(Debug)]
struct GatewayAdmission {
    route: GatewayRoute,
    proxy_mode: GatewayProxyMode,
    mtls_secrets: GatewayMtlsSecretStore,
}

#[derive(Debug, Default, Clone)]
pub struct GatewayMtlsSecretStore {
    pems_by_ref: BTreeMap<String, String>,
}

impl GatewayMtlsSecretStore {
    pub fn upsert(
        &mut self,
        secret_ref: impl Into<String>,
        pem: impl Into<String>,
    ) -> Result<(), GatewayError> {
        let secret_ref = secret_ref.into();
        if secret_ref.trim().is_empty() {
            return Err(GatewayError::InvalidRoute("mTLS secret ref is empty"));
        }
        let pem = pem.into();
        if pem.trim().is_empty() {
            return Err(GatewayError::InvalidRoute("mTLS secret PEM is empty"));
        }
        self.pems_by_ref.insert(secret_ref, pem);
        Ok(())
    }

    pub fn replace(&mut self, secrets: BTreeMap<String, String>) -> Result<(), GatewayError> {
        let mut replacement = Self::default();
        for (secret_ref, pem) in secrets {
            replacement.upsert(secret_ref, pem)?;
        }
        *self = replacement;
        Ok(())
    }

    fn pem(&self, secret_ref: &str) -> Result<&str, GatewayError> {
        self.pems_by_ref
            .get(secret_ref)
            .map(String::as_str)
            .ok_or_else(|| GatewayError::Proxy(format!("mTLS secret {secret_ref} is not loaded")))
    }
}

pub fn gateway_router(state: SharedGatewayState) -> Router {
    Router::new()
        .route("/healthz", get(gateway_health))
        .route("/metrics", get(gateway_metrics))
        .route("/usage-events/drain", post(gateway_drain_usage_events))
        .fallback(gateway_fallback)
        .with_state(state)
}

async fn gateway_health() -> &'static str {
    "ok"
}

async fn gateway_metrics(State(state): State<SharedGatewayState>) -> Response<Body> {
    match state.lock() {
        Ok(state) => (
            StatusCode::OK,
            render_gateway_metrics(&state.metrics_snapshot()),
        )
            .into_response(),
        Err(_) => gateway_error_response(GatewayError::StateUnavailable),
    }
}

async fn gateway_drain_usage_events(
    State(state): State<SharedGatewayState>,
    Json(payload): Json<DrainUsageEventsRequest>,
) -> Response<Body> {
    match state.lock() {
        Ok(mut state) => Json(DrainUsageEventsResponse {
            events: state.drain_usage_events(payload.occurred_at),
        })
        .into_response(),
        Err(_) => gateway_error_response(GatewayError::StateUnavailable),
    }
}

#[derive(Debug, Deserialize)]
struct DrainUsageEventsRequest {
    occurred_at: String,
}

#[derive(Debug, Serialize)]
struct DrainUsageEventsResponse {
    events: Vec<UsageEvent>,
}

async fn gateway_fallback(
    State(state): State<SharedGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    let Some(host) = headers.get(HOST).and_then(|value| value.to_str().ok()) else {
        let err = GatewayError::InvalidRoute("host header is missing");
        record_rejected_request(&state, &err);
        tracing::warn!(status = status_for_error(&err).as_u16(), error = %err, "gateway request rejected");
        return gateway_error_response(err);
    };
    let host = host_without_port(host).to_owned();

    let admission = (|| {
        let mut state = state.lock().map_err(|_| GatewayError::StateUnavailable)?;
        let route = match state.router.resolve(&host) {
            Ok(route) => route.clone(),
            Err(err) => {
                state.metrics.record_rejected(&err);
                return Err(err);
            }
        };

        if let Err(err) = state.limiter.admit_connection(&route) {
            state.metrics.record_rejected(&err);
            return Err(err);
        }
        if let Err(err) = state.limiter.record_request(&route) {
            state.limiter.close_connection(&route.environment_id);
            state.metrics.record_rejected(&err);
            return Err(err);
        }

        Ok(GatewayAdmission {
            route,
            proxy_mode: state.proxy_mode.clone(),
            mtls_secrets: state.mtls_secrets.clone(),
        })
    })();

    let admission = match admission {
        Ok(admission) => admission,
        Err(err) => {
            tracing::warn!(
                host,
                status = status_for_error(&err).as_u16(),
                error = %err,
                "gateway request rejected"
            );
            return gateway_error_response(err);
        }
    };

    let proxy_result = match admission.proxy_mode {
        GatewayProxyMode::Http => {
            proxy_to_upstream(
                &admission.route,
                &admission.mtls_secrets,
                method,
                uri,
                headers,
                body,
            )
            .await
        }
        GatewayProxyMode::Static { status, body } => Ok(ProxiedResponse {
            status,
            headers: HeaderMap::new(),
            body,
        }),
    };
    close_admitted_connection(&state, &admission.route.environment_id);

    match proxy_result {
        Ok(proxied) => {
            record_routed_response(&state, &admission.route, proxied.body.len() as u64);
            tracing::info!(
                host,
                environment_id = %admission.route.environment_id,
                status = proxied.status.as_u16(),
                egress_bytes = proxied.body.len(),
                "gateway request routed"
            );
            gateway_success_response(&admission.route, proxied)
        }
        Err(err) => {
            record_rejected_request(&state, &err);
            tracing::warn!(
                host,
                status = status_for_error(&err).as_u16(),
                error = %err,
                "gateway request rejected"
            );
            gateway_error_response(err)
        }
    }
}

async fn proxy_to_upstream(
    route: &GatewayRoute,
    mtls_secrets: &GatewayMtlsSecretStore,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<ProxiedResponse, GatewayError> {
    match &route.tls_policy {
        TlsPolicy::TerminateAtGateway => {
            proxy_to_http_upstream(route, method, uri, headers, body).await
        }
        TlsPolicy::MutualTlsToSync {
            ca_secret_ref,
            client_certificate_secret_ref,
            client_private_key_secret_ref,
            server_name,
        } => {
            proxy_to_mtls_upstream(
                route,
                mtls_secrets,
                MtlsPolicyRefs {
                    ca_secret_ref,
                    client_certificate_secret_ref: client_certificate_secret_ref.as_deref(),
                    client_private_key_secret_ref: client_private_key_secret_ref.as_deref(),
                    server_name: server_name.as_deref(),
                },
                method,
                uri,
                headers,
                body,
            )
            .await
        }
    }
}

async fn proxy_to_http_upstream(
    route: &GatewayRoute,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<ProxiedResponse, GatewayError> {
    let upstream_uri = upstream_uri_for_scheme(&route.sync_endpoint, &uri, "http")?;
    let request_body = to_bytes(body, MAX_PROXY_REQUEST_BYTES)
        .await
        .map_err(|err| GatewayError::Proxy(format!("read request body: {err}")))?;
    let mut builder = Request::builder().method(method).uri(upstream_uri.clone());
    for (name, value) in &headers {
        if !skip_request_header(name) {
            builder = builder.header(name, value);
        }
    }
    if let Some(authority) = upstream_uri.authority() {
        builder = builder.header(HOST, authority.as_str());
    }
    let request = builder
        .body(Full::new(request_body))
        .map_err(|err| GatewayError::Proxy(format!("build upstream request: {err}")))?;

    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(HttpConnector::new());
    let response = client
        .request(request)
        .await
        .map_err(|err| GatewayError::Proxy(format!("upstream request failed: {err}")))?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|err| GatewayError::Proxy(format!("read upstream response: {err}")))?
        .to_bytes();
    Ok(ProxiedResponse {
        status,
        headers,
        body,
    })
}

struct MtlsPolicyRefs<'a> {
    ca_secret_ref: &'a str,
    client_certificate_secret_ref: Option<&'a str>,
    client_private_key_secret_ref: Option<&'a str>,
    server_name: Option<&'a str>,
}

async fn proxy_to_mtls_upstream(
    route: &GatewayRoute,
    mtls_secrets: &GatewayMtlsSecretStore,
    policy: MtlsPolicyRefs<'_>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<ProxiedResponse, GatewayError> {
    let upstream_uri = upstream_uri_for_scheme(&route.sync_endpoint, &uri, "https")?;
    let request_body = to_bytes(body, MAX_PROXY_REQUEST_BYTES)
        .await
        .map_err(|err| GatewayError::Proxy(format!("read request body: {err}")))?;
    let authority = upstream_uri.authority().ok_or_else(|| {
        GatewayError::Proxy("upstream endpoint is missing an authority".to_owned())
    })?;
    let host = authority.host();
    let port = authority.port_u16().unwrap_or(443);
    let server_name = policy.server_name.unwrap_or(host);
    let addr = format!("{host}:{port}");

    let config = mtls_client_config(
        mtls_secrets.pem(policy.ca_secret_ref)?,
        policy
            .client_certificate_secret_ref
            .map(|secret_ref| mtls_secrets.pem(secret_ref))
            .transpose()?,
        policy
            .client_private_key_secret_ref
            .map(|secret_ref| mtls_secrets.pem(secret_ref))
            .transpose()?,
    )?;
    let server_name = ServerName::try_from(server_name.to_owned())
        .map_err(|err| GatewayError::Proxy(format!("invalid mTLS server name: {err}")))?;
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|err| GatewayError::Proxy(format!("connect mTLS upstream: {err}")))?;
    let mut stream = TlsConnector::from(Arc::new(config))
        .connect(server_name, stream)
        .await
        .map_err(|err| GatewayError::Proxy(format!("mTLS handshake failed: {err}")))?;

    let request = encode_http1_request(method, &upstream_uri, headers, &request_body)?;
    stream
        .write_all(&request)
        .await
        .map_err(|err| GatewayError::Proxy(format!("write mTLS request: {err}")))?;
    stream
        .flush()
        .await
        .map_err(|err| GatewayError::Proxy(format!("flush mTLS request: {err}")))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|err| GatewayError::Proxy(format!("read mTLS response: {err}")))?;
    decode_http1_response(&response)
}

#[derive(Debug)]
struct ProxiedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

fn mtls_client_config(
    ca_pem: &str,
    client_certificate_pem: Option<&str>,
    client_private_key_pem: Option<&str>,
) -> Result<ClientConfig, GatewayError> {
    let mut ca_reader = BufReader::new(Cursor::new(ca_pem.as_bytes()));
    let ca_certs = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| GatewayError::Proxy(format!("parse mTLS CA PEM: {err}")))?;
    if ca_certs.is_empty() {
        return Err(GatewayError::Proxy(
            "mTLS CA PEM contained no certificates".to_owned(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for cert in ca_certs {
        roots.add(cert).map_err(|err| {
            GatewayError::Proxy(format!("load mTLS CA certificate into root store: {err}"))
        })?;
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    match (client_certificate_pem, client_private_key_pem) {
        (Some(certificate_pem), Some(private_key_pem)) => {
            let mut cert_reader = BufReader::new(Cursor::new(certificate_pem.as_bytes()));
            let certs = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| GatewayError::Proxy(format!("parse mTLS client cert PEM: {err}")))?;
            if certs.is_empty() {
                return Err(GatewayError::Proxy(
                    "mTLS client cert PEM contained no certificates".to_owned(),
                ));
            }
            let mut key_reader = BufReader::new(Cursor::new(private_key_pem.as_bytes()));
            let private_key = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|err| GatewayError::Proxy(format!("parse mTLS client key PEM: {err}")))?
                .ok_or_else(|| {
                    GatewayError::Proxy("mTLS client key PEM contained no key".to_owned())
                })?;
            builder
                .with_client_auth_cert(certs, private_key)
                .map_err(|err| GatewayError::Proxy(format!("load mTLS client identity: {err}")))
        }
        (None, None) => Ok(builder.with_no_client_auth()),
        _ => Err(GatewayError::Proxy(
            "mTLS client certificate and private key refs must be configured together".to_owned(),
        )),
    }
}

fn encode_http1_request(
    method: Method,
    upstream_uri: &Uri,
    headers: HeaderMap,
    request_body: &[u8],
) -> Result<Vec<u8>, GatewayError> {
    let path_and_query = upstream_uri
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    let authority = upstream_uri.authority().ok_or_else(|| {
        GatewayError::Proxy("upstream endpoint is missing an authority".to_owned())
    })?;
    let mut request = format!(
        "{method} {path_and_query} HTTP/1.1\r\nhost: {authority}\r\ncontent-length: {}\r\nconnection: close\r\n",
        request_body.len()
    )
    .into_bytes();
    for (name, value) in &headers {
        if skip_request_header(name) || name.as_str() == "content-length" {
            continue;
        }
        request.extend_from_slice(name.as_str().as_bytes());
        request.extend_from_slice(b": ");
        request.extend_from_slice(value.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(request_body);
    Ok(request)
}

fn decode_http1_response(response: &[u8]) -> Result<ProxiedResponse, GatewayError> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| GatewayError::Proxy("mTLS upstream response has no headers".to_owned()))?;
    let header_block = std::str::from_utf8(&response[..header_end])
        .map_err(|err| GatewayError::Proxy(format!("mTLS upstream response headers: {err}")))?;
    let mut lines = header_block.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| GatewayError::Proxy("mTLS upstream response is empty".to_owned()))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| GatewayError::Proxy("mTLS upstream response has no status".to_owned()))?
        .parse::<u16>()
        .map_err(|err| GatewayError::Proxy(format!("parse mTLS upstream status: {err}")))?;
    let status = StatusCode::from_u16(status)
        .map_err(|err| GatewayError::Proxy(format!("invalid mTLS upstream status: {err}")))?;
    let mut headers = HeaderMap::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|err| GatewayError::Proxy(format!("parse mTLS response header: {err}")))?;
        let value = HeaderValue::from_str(value.trim())
            .map_err(|err| GatewayError::Proxy(format!("parse mTLS response header: {err}")))?;
        headers.insert(name, value);
    }
    Ok(ProxiedResponse {
        status,
        headers,
        body: Bytes::copy_from_slice(&response[header_end + 4..]),
    })
}

fn upstream_uri_for_scheme(
    sync_endpoint: &str,
    incoming_uri: &Uri,
    expected_scheme: &str,
) -> Result<Uri, GatewayError> {
    let base: Uri = sync_endpoint
        .parse()
        .map_err(|err| GatewayError::Proxy(format!("invalid upstream endpoint: {err}")))?;
    if base.scheme_str() != Some(expected_scheme) {
        return Err(GatewayError::Proxy(format!(
            "{expected_scheme} sync endpoint is required for this gateway TLS policy"
        )));
    }
    let Some(authority) = base.authority() else {
        return Err(GatewayError::Proxy(
            "upstream endpoint is missing an authority".to_owned(),
        ));
    };
    let path_and_query = incoming_uri
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    format!("{expected_scheme}://{authority}{path_and_query}")
        .parse()
        .map_err(|err| GatewayError::Proxy(format!("invalid upstream request URI: {err}")))
}

fn gateway_success_response(route: &GatewayRoute, proxied: ProxiedResponse) -> Response<Body> {
    let mut response = (proxied.status, Body::from(proxied.body)).into_response();
    for (name, value) in &proxied.headers {
        if !skip_response_header(name) {
            response.headers_mut().insert(name.clone(), value.clone());
        }
    }
    response.headers_mut().insert(
        "x-palimpsest-environment-id",
        HeaderValue::from_str(&route.environment_id)
            .unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    response.headers_mut().insert(
        "x-palimpsest-upstream",
        HeaderValue::from_str(&route.sync_endpoint)
            .unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    response
}

fn gateway_error_response(err: GatewayError) -> Response<Body> {
    let status = status_for_error(&err);
    (status, err.to_string()).into_response()
}

const fn status_for_error(err: &GatewayError) -> StatusCode {
    match err {
        GatewayError::RouteNotFound(_) | GatewayError::InvalidRoute(_) => StatusCode::NOT_FOUND,
        GatewayError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
        GatewayError::Proxy(_) => StatusCode::BAD_GATEWAY,
        GatewayError::StateUnavailable => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn close_admitted_connection(state: &SharedGatewayState, environment_id: &str) {
    if let Ok(mut state) = state.lock() {
        state.limiter.close_connection(environment_id);
    }
}

fn record_routed_response(state: &SharedGatewayState, route: &GatewayRoute, bytes: u64) {
    if let Ok(mut state) = state.lock() {
        state.accountant.record_route_egress(route, bytes);
        state.metrics.record_routed();
    }
}

fn record_rejected_request(state: &SharedGatewayState, err: &GatewayError) {
    if let Ok(mut state) = state.lock() {
        state.metrics.record_rejected(err);
    }
}

fn render_gateway_metrics(snapshot: &GatewayMetricsSnapshot) -> String {
    let mut metrics = format!(
        "palimpsest_gateway_requests_total {}\n\
         palimpsest_gateway_routed_requests_total {}\n\
         palimpsest_gateway_rejected_requests_total {}\n\
         palimpsest_gateway_rate_limited_requests_total {}\n",
        snapshot.total_requests,
        snapshot.routed_requests,
        snapshot.rejected_requests,
        snapshot.rate_limited_requests
    );
    for (environment_id, bytes) in &snapshot.egress_bytes_by_environment {
        metrics.push_str(&format!(
            "palimpsest_gateway_egress_bytes_total{{environment_id=\"{environment_id}\"}} {bytes}\n"
        ));
    }
    metrics
}

fn skip_request_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn skip_response_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn normalize_host(host: &str) -> Result<String, GatewayError> {
    let host = host_without_port(host).trim().to_ascii_lowercase();
    if host.is_empty() {
        return Err(GatewayError::InvalidRoute("host is empty"));
    }
    Ok(host)
}

fn normalize_route(mut route: GatewayRoute) -> Result<GatewayRoute, GatewayError> {
    route.host = normalize_host(&route.host)?;
    if route.sync_endpoint.trim().is_empty() {
        return Err(GatewayError::InvalidRoute("sync endpoint is empty"));
    }
    let endpoint: Uri = route
        .sync_endpoint
        .parse()
        .map_err(|_| GatewayError::InvalidRoute("sync endpoint is invalid"))?;
    match &route.tls_policy {
        TlsPolicy::TerminateAtGateway => {
            if endpoint.scheme_str() != Some("http") {
                return Err(GatewayError::InvalidRoute(
                    "terminate_at_gateway routes require http sync endpoints",
                ));
            }
        }
        TlsPolicy::MutualTlsToSync {
            ca_secret_ref,
            client_certificate_secret_ref,
            client_private_key_secret_ref,
            ..
        } => {
            if endpoint.scheme_str() != Some("https") {
                return Err(GatewayError::InvalidRoute(
                    "mutual_tls_to_sync routes require https sync endpoints",
                ));
            }
            if ca_secret_ref.trim().is_empty() {
                return Err(GatewayError::InvalidRoute("mTLS CA secret ref is empty"));
            }
            if client_certificate_secret_ref.is_some() != client_private_key_secret_ref.is_some() {
                return Err(GatewayError::InvalidRoute(
                    "mTLS client certificate and key refs must be configured together",
                ));
            }
        }
    }
    Ok(route)
}

fn host_without_port(host: &str) -> &str {
    let host = host.trim();
    if host.starts_with('[') {
        return host
            .find(']')
            .map_or(host, |end| host.get(1..end).unwrap_or(host));
    }

    match host.rsplit_once(':') {
        Some((hostname, _port)) if !hostname.contains(':') => hostname,
        _ => host,
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GatewayError {
    #[error("gateway route not found for host {0}")]
    RouteNotFound(String),
    #[error("invalid gateway route: {0}")]
    InvalidRoute(&'static str),
    #[error("environment {environment_id} is rate limited: {reason}")]
    RateLimited {
        environment_id: String,
        reason: &'static str,
    },
    #[error("gateway upstream proxy failed: {0}")]
    Proxy(String),
    #[error("gateway state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Error)]
pub enum DatabaseProxyError {
    #[error("invalid database proxy route: {0}")]
    InvalidRoute(&'static str),
    #[error("database proxy received a non-TLS PostgreSQL startup packet on a TLS route")]
    InvalidPostgresTlsRequest,
    #[error("database proxy received an invalid PostgreSQL startup packet")]
    InvalidPostgresStartup,
    #[error("database proxy received an invalid PostgreSQL frontend message")]
    InvalidPostgresMessage,
    #[error("database proxy policy rejected PostgreSQL startup {field}={value}")]
    PolicyRejected { field: &'static str, value: String },
    #[error("database proxy received a nested PostgreSQL SSLRequest inside TLS")]
    NestedPostgresTlsRequest,
    #[error("database proxy TLS failed: {0}")]
    Tls(String),
    #[error("database proxy I/O failed: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::header::HOST,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn router_resolves_route_by_host() {
        let mut router = GatewayRouter::default();
        let route = route();

        router.upsert_route(route.clone()).expect("route is valid");

        assert_eq!(router.resolve("env-123.palimpsest.dev"), Ok(&route));
    }

    #[test]
    fn router_replace_routes_removes_stale_hosts() {
        let mut router = GatewayRouter::default();
        let mut replacement = route();
        replacement.host = "env-456.palimpsest.dev".to_owned();

        router.upsert_route(route()).expect("route is valid");
        router
            .replace_routes(vec![replacement.clone()])
            .expect("replacement route is valid");

        assert_eq!(
            router.resolve("env-123.palimpsest.dev"),
            Err(GatewayError::RouteNotFound(
                "env-123.palimpsest.dev".to_owned()
            ))
        );
        assert_eq!(router.resolve("env-456.palimpsest.dev"), Ok(&replacement));
    }

    #[test]
    fn limiter_is_scoped_by_environment() {
        let mut limiter = GatewayLimiter::default();
        let mut route = route();
        route.rate_limit.max_connections = 1;

        limiter.admit_connection(&route).expect("first connection");
        let err = limiter
            .admit_connection(&route)
            .expect_err("second connection is limited");

        assert_eq!(
            err,
            GatewayError::RateLimited {
                environment_id: "env_123".to_owned(),
                reason: "connection_limit"
            }
        );

        limiter.close_connection("env_123");
        limiter
            .admit_connection(&route)
            .expect("connection reopened");
    }

    #[test]
    fn egress_accounting_accumulates_per_environment() {
        let mut accountant = EgressAccountant::default();

        accountant.record(EgressUsageEvent {
            environment_id: "env_123".to_owned(),
            bytes: 10,
        });
        accountant.record(EgressUsageEvent {
            environment_id: "env_123".to_owned(),
            bytes: 15,
        });

        assert_eq!(accountant.bytes_for_environment("env_123"), 25);
        assert_eq!(accountant.bytes_for_environment("env_other"), 0);
    }

    #[test]
    fn egress_accounting_drains_usage_events_by_route_identity() {
        let mut accountant = EgressAccountant::default();
        let route = route();

        accountant.record_route_egress(&route, 10);
        accountant.record_route_egress(&route, 15);
        let events = accountant.drain_usage_events("2026-05-17T00:00:00Z");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].organization_id, "org_123");
        assert_eq!(events[0].project_id, "project_123");
        assert_eq!(events[0].environment_id, "env_123");
        assert_eq!(events[0].metric, "sync_egress_bytes");
        assert_eq!(events[0].quantity, 25);
        assert_eq!(events[0].occurred_at, "2026-05-17T00:00:00Z");
        assert!(accountant
            .drain_usage_events("2026-05-17T00:01:00Z")
            .is_empty());
    }

    #[tokio::test]
    async fn gateway_http_surface_routes_by_host_and_records_egress() {
        let state = Arc::new(Mutex::new(GatewayState::with_static_upstream_response(
            StatusCode::OK,
            Bytes::from_static(b"upstream response"),
        )));
        state
            .lock()
            .expect("gateway state")
            .upsert_route(route())
            .expect("route is valid");

        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("env-123.palimpsest.dev:443"));
        let response = gateway_fallback(
            State(state.clone()),
            Method::GET,
            Uri::from_static("/sync?query=1"),
            headers,
            Body::empty(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("x-palimpsest-environment-id"),
            Some(&HeaderValue::from_static("env_123"))
        );
        let body = to_bytes(response.into_body(), MAX_PROXY_REQUEST_BYTES)
            .await
            .expect("response body");
        assert_eq!(body.as_ref(), b"upstream response");
        assert!(
            state
                .lock()
                .expect("gateway state")
                .egress_bytes_for_environment("env_123")
                > 0
        );
        let snapshot = state.lock().expect("gateway state").metrics_snapshot();
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.routed_requests, 1);
        assert_eq!(snapshot.rejected_requests, 0);
        let usage_events = state
            .lock()
            .expect("gateway state")
            .drain_usage_events("2026-05-17T00:00:00Z");
        assert_eq!(usage_events.len(), 1);
        assert_eq!(usage_events[0].metric, "sync_egress_bytes");
        assert!(usage_events[0].quantity > 0);
    }

    #[tokio::test]
    async fn gateway_http_surface_rate_limits_requests() {
        let state = Arc::new(Mutex::new(GatewayState::with_static_upstream_response(
            StatusCode::OK,
            Bytes::from_static(b"upstream response"),
        )));
        let mut route = route();
        route.rate_limit.max_requests_per_minute = 1;
        state
            .lock()
            .expect("gateway state")
            .upsert_route(route)
            .expect("route is valid");

        let first_response = gateway_fallback(
            State(state.clone()),
            Method::GET,
            Uri::from_static("/sync"),
            hosted_headers(),
            Body::empty(),
        )
        .await;
        let second_response = gateway_fallback(
            State(state.clone()),
            Method::GET,
            Uri::from_static("/sync"),
            hosted_headers(),
            Body::empty(),
        )
        .await;

        assert_eq!(first_response.status(), StatusCode::OK);
        assert_eq!(second_response.status(), StatusCode::TOO_MANY_REQUESTS);
        let snapshot = state.lock().expect("gateway state").metrics_snapshot();
        assert_eq!(snapshot.total_requests, 2);
        assert_eq!(snapshot.routed_requests, 1);
        assert_eq!(snapshot.rate_limited_requests, 1);
    }

    #[tokio::test]
    async fn gateway_http_surface_rejects_unknown_host() {
        let state = Arc::new(Mutex::new(GatewayState::default()));
        state
            .lock()
            .expect("gateway state")
            .upsert_route(route())
            .expect("route is valid");

        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("missing.palimpsest.dev"));

        let response = gateway_fallback(
            State(state),
            Method::GET,
            Uri::from_static("/sync"),
            headers,
            Body::empty(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn gateway_metrics_surface_reports_request_and_egress_counts() {
        let state = Arc::new(Mutex::new(GatewayState::default()));
        state
            .lock()
            .expect("gateway state")
            .upsert_route(route())
            .expect("route is valid");
        let _response = gateway_fallback(
            State(state.clone()),
            Method::GET,
            Uri::from_static("/sync"),
            hosted_headers(),
            Body::empty(),
        )
        .await;

        let response = gateway_metrics(State(state)).await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn gateway_usage_event_drain_surface_returns_and_clears_buffered_events() {
        let state = Arc::new(Mutex::new(GatewayState::with_static_upstream_response(
            StatusCode::OK,
            Bytes::from_static(b"upstream response"),
        )));
        state
            .lock()
            .expect("gateway state")
            .upsert_route(route())
            .expect("route is valid");
        let _response = gateway_fallback(
            State(state.clone()),
            Method::GET,
            Uri::from_static("/sync"),
            hosted_headers(),
            Body::empty(),
        )
        .await;

        let response = gateway_drain_usage_events(
            State(state.clone()),
            Json(DrainUsageEventsRequest {
                occurred_at: "2026-05-17T00:00:00Z".to_owned(),
            }),
        )
        .await;
        let body = to_bytes(response.into_body(), MAX_PROXY_REQUEST_BYTES)
            .await
            .expect("drain response body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("drain json");

        assert_eq!(value["events"].as_array().expect("events").len(), 1);
        assert_eq!(value["events"][0]["metric"], "sync_egress_bytes");

        let response = gateway_drain_usage_events(
            State(state),
            Json(DrainUsageEventsRequest {
                occurred_at: "2026-05-17T00:01:00Z".to_owned(),
            }),
        )
        .await;
        let body = to_bytes(response.into_body(), MAX_PROXY_REQUEST_BYTES)
            .await
            .expect("drain response body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("drain json");
        assert!(value["events"].as_array().expect("events").is_empty());
    }

    fn hosted_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, HeaderValue::from_static("env-123.palimpsest.dev"));
        headers
    }

    #[test]
    fn upstream_uri_preserves_incoming_path_and_query() {
        let uri = upstream_uri_for_scheme(
            "http://127.0.0.1:50051",
            &Uri::from_static("/sync?query=1"),
            "http",
        )
        .expect("upstream uri");

        assert_eq!(uri.to_string(), "http://127.0.0.1:50051/sync?query=1");
    }

    #[test]
    fn mtls_gateway_route_requires_https_and_complete_client_refs() {
        let mut route = route();
        route.sync_endpoint = "https://sync.internal:8443".to_owned();
        route.tls_policy = TlsPolicy::MutualTlsToSync {
            ca_secret_ref: "sync-ca".to_owned(),
            client_certificate_secret_ref: Some("gateway-client-cert".to_owned()),
            client_private_key_secret_ref: Some("gateway-client-key".to_owned()),
            server_name: Some("sync.internal".to_owned()),
        };
        let mut router = GatewayRouter::default();

        router
            .upsert_route(route.clone())
            .expect("mTLS route is valid");
        assert_eq!(router.resolve("env-123.palimpsest.dev"), Ok(&route));

        route.sync_endpoint = "http://sync.internal:8080".to_owned();
        assert!(matches!(
            router.upsert_route(route.clone()),
            Err(GatewayError::InvalidRoute(_))
        ));

        route.sync_endpoint = "https://sync.internal:8443".to_owned();
        route.tls_policy = TlsPolicy::MutualTlsToSync {
            ca_secret_ref: "sync-ca".to_owned(),
            client_certificate_secret_ref: Some("gateway-client-cert".to_owned()),
            client_private_key_secret_ref: None,
            server_name: None,
        };
        assert!(matches!(
            router.upsert_route(route),
            Err(GatewayError::InvalidRoute(_))
        ));
    }

    #[tokio::test]
    async fn mtls_gateway_route_requires_loaded_secret_material() {
        let mut route = route();
        route.sync_endpoint = "https://sync.internal:8443".to_owned();
        route.tls_policy = TlsPolicy::MutualTlsToSync {
            ca_secret_ref: "sync-ca".to_owned(),
            client_certificate_secret_ref: Some("gateway-client-cert".to_owned()),
            client_private_key_secret_ref: Some("gateway-client-key".to_owned()),
            server_name: Some("sync.internal".to_owned()),
        };
        let err = proxy_to_upstream(
            &route,
            &GatewayMtlsSecretStore::default(),
            Method::GET,
            Uri::from_static("/sync"),
            HeaderMap::new(),
            Body::empty(),
        )
        .await
        .expect_err("mTLS route needs loaded PEM material");

        assert!(matches!(err, GatewayError::Proxy(message) if message.contains("sync-ca")));
    }

    #[test]
    fn database_proxy_route_requires_addresses() {
        let mut route = database_proxy_route();
        route.upstream_addr.clear();

        let err = validate_database_proxy_route(&route).expect_err("route should be invalid");

        assert!(matches!(err, DatabaseProxyError::InvalidRoute(_)));
    }

    #[tokio::test]
    async fn database_proxy_forwards_bytes_between_client_and_upstream() {
        let (mut client, mut proxy_client) = tokio::io::duplex(64);
        let (mut proxy_upstream, mut upstream) = tokio::io::duplex(64);
        let upstream_task = tokio::spawn(async move {
            let mut request = [0_u8; 4];
            upstream
                .read_exact(&mut request)
                .await
                .expect("read request");
            assert_eq!(&request, b"ping");
            upstream.write_all(b"pong").await.expect("write response");
            upstream.shutdown().await.expect("shutdown upstream");
        });

        let proxy_task = tokio::spawn(async move {
            proxy_database_streams(&mut proxy_client, &mut proxy_upstream)
                .await
                .expect("proxy streams");
        });

        client.write_all(b"ping").await.expect("write to proxy");
        let mut response = [0_u8; 4];
        client
            .read_exact(&mut response)
            .await
            .expect("read from proxy");
        assert_eq!(&response, b"pong");
        client.shutdown().await.expect("shutdown client");

        upstream_task.await.expect("upstream task");
        proxy_task.await.expect("proxy task");
    }

    #[tokio::test]
    async fn database_proxy_validates_and_forwards_postgres_startup_packet() {
        let (mut client, mut proxy_client) = tokio::io::duplex(256);
        let (mut proxy_upstream, mut upstream) = tokio::io::duplex(256);
        let startup = postgres_startup_packet("cluster_123_app", "postgres");
        let expected_startup = startup.clone();
        let upstream_task = tokio::spawn(async move {
            let mut request = vec![0_u8; expected_startup.len()];
            upstream
                .read_exact(&mut request)
                .await
                .expect("read startup");
            assert_eq!(request, expected_startup);
            upstream.write_all(b"R").await.expect("write response");
            upstream.shutdown().await.expect("shutdown upstream");
        });

        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                None,
            )
            .await
            .expect("proxy streams")
        });

        client.write_all(&startup).await.expect("write startup");
        let mut response = [0_u8; 1];
        client
            .read_exact(&mut response)
            .await
            .expect("read response");
        assert_eq!(&response, b"R");
        client.shutdown().await.expect("shutdown client");

        upstream_task.await.expect("upstream task");
        let (client_to_upstream, upstream_to_client) = proxy_task.await.expect("proxy task");
        assert_eq!(client_to_upstream, startup.len() as u64);
        assert_eq!(upstream_to_client, 1);
    }

    #[tokio::test]
    async fn database_proxy_rejects_invalid_postgres_startup_packet() {
        let (mut client, mut proxy_client) = tokio::io::duplex(64);
        let (mut proxy_upstream, _upstream) = tokio::io::duplex(64);
        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                None,
            )
            .await
        });

        client
            .write_all(&postgres_ssl_request())
            .await
            .expect("write nested SSLRequest");

        let err = proxy_task
            .await
            .expect("proxy task")
            .expect_err("nested SSLRequest should fail");
        assert!(matches!(err, DatabaseProxyError::NestedPostgresTlsRequest));
    }

    #[tokio::test]
    async fn database_proxy_policy_rejects_disallowed_startup_user() {
        let (mut client, mut proxy_client) = tokio::io::duplex(256);
        let (mut proxy_upstream, _upstream) = tokio::io::duplex(256);
        let policy = DatabaseProxyPolicy {
            allowed_users: vec!["cluster_123_app".to_owned()],
            allowed_databases: vec!["postgres".to_owned()],
            forbidden_startup_parameters: Vec::new(),
            required_startup_parameters: BTreeMap::new(),
            forbidden_simple_query_verbs: Vec::new(),
            max_simple_query_bytes: None,
        };
        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                Some(&policy),
            )
            .await
        });

        client
            .write_all(&postgres_startup_packet(
                "cluster_123_replication",
                "postgres",
            ))
            .await
            .expect("write startup");

        let err = proxy_task
            .await
            .expect("proxy task")
            .expect_err("disallowed startup user should fail");
        assert!(matches!(
            err,
            DatabaseProxyError::PolicyRejected { field: "user", .. }
        ));
    }

    #[tokio::test]
    async fn database_proxy_policy_rejects_forbidden_startup_parameter() {
        let (mut client, mut proxy_client) = tokio::io::duplex(512);
        let (mut proxy_upstream, _upstream) = tokio::io::duplex(512);
        let policy = DatabaseProxyPolicy {
            allowed_users: vec!["cluster_123_app".to_owned()],
            allowed_databases: vec!["postgres".to_owned()],
            forbidden_startup_parameters: vec!["replication".to_owned(), "options".to_owned()],
            required_startup_parameters: BTreeMap::new(),
            forbidden_simple_query_verbs: Vec::new(),
            max_simple_query_bytes: None,
        };
        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                Some(&policy),
            )
            .await
        });

        client
            .write_all(&postgres_startup_packet_with_parameters(&[
                ("user", "cluster_123_app"),
                ("database", "postgres"),
                ("options", "-c search_path=public"),
            ]))
            .await
            .expect("write startup");

        let err = proxy_task
            .await
            .expect("proxy task")
            .expect_err("forbidden startup parameter should fail");
        assert!(matches!(
            err,
            DatabaseProxyError::PolicyRejected {
                field: "forbidden_parameter",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn database_proxy_policy_enforces_required_startup_parameter() {
        let (mut client, mut proxy_client) = tokio::io::duplex(512);
        let (mut proxy_upstream, _upstream) = tokio::io::duplex(512);
        let policy = DatabaseProxyPolicy {
            allowed_users: vec!["cluster_123_app".to_owned()],
            allowed_databases: vec!["postgres".to_owned()],
            forbidden_startup_parameters: Vec::new(),
            required_startup_parameters: BTreeMap::from([(
                "application_name".to_owned(),
                "palimpsest".to_owned(),
            )]),
            forbidden_simple_query_verbs: Vec::new(),
            max_simple_query_bytes: None,
        };
        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                Some(&policy),
            )
            .await
        });

        client
            .write_all(&postgres_startup_packet("cluster_123_app", "postgres"))
            .await
            .expect("write startup");

        let err = proxy_task
            .await
            .expect("proxy task")
            .expect_err("missing required startup parameter should fail");
        assert!(matches!(
            err,
            DatabaseProxyError::PolicyRejected {
                field: "required_parameter",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn database_proxy_policy_rejects_forbidden_simple_query_verb() {
        let (mut client, mut proxy_client) = tokio::io::duplex(512);
        let (mut proxy_upstream, _upstream) = tokio::io::duplex(512);
        let policy = DatabaseProxyPolicy {
            allowed_users: vec!["cluster_123_app".to_owned()],
            allowed_databases: vec!["postgres".to_owned()],
            forbidden_startup_parameters: Vec::new(),
            required_startup_parameters: BTreeMap::new(),
            forbidden_simple_query_verbs: vec!["copy".to_owned()],
            max_simple_query_bytes: None,
        };
        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                Some(&policy),
            )
            .await
        });

        client
            .write_all(&postgres_startup_packet("cluster_123_app", "postgres"))
            .await
            .expect("write startup");
        client
            .write_all(&postgres_simple_query_packet("COPY users TO STDOUT"))
            .await
            .expect("write query");

        let err = proxy_task
            .await
            .expect("proxy task")
            .expect_err("forbidden simple query verb should fail");
        assert!(matches!(
            err,
            DatabaseProxyError::PolicyRejected {
                field: "simple_query_verb",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn database_proxy_forwards_allowed_simple_query_frames() {
        let (mut client, mut proxy_client) = tokio::io::duplex(1024);
        let (mut proxy_upstream, mut upstream) = tokio::io::duplex(1024);
        let startup = postgres_startup_packet("cluster_123_app", "postgres");
        let query = postgres_simple_query_packet("SELECT 1");
        let expected_startup = startup.clone();
        let expected_query = query.clone();
        let policy = DatabaseProxyPolicy {
            allowed_users: vec!["cluster_123_app".to_owned()],
            allowed_databases: vec!["postgres".to_owned()],
            forbidden_startup_parameters: Vec::new(),
            required_startup_parameters: BTreeMap::new(),
            forbidden_simple_query_verbs: vec!["copy".to_owned()],
            max_simple_query_bytes: Some(1024),
        };
        let upstream_task = tokio::spawn(async move {
            let mut startup_request = vec![0_u8; expected_startup.len()];
            upstream
                .read_exact(&mut startup_request)
                .await
                .expect("read startup");
            assert_eq!(startup_request, expected_startup);
            let mut query_request = vec![0_u8; expected_query.len()];
            upstream
                .read_exact(&mut query_request)
                .await
                .expect("read query");
            assert_eq!(query_request, expected_query);
            upstream.write_all(b"Z").await.expect("write response");
            upstream.shutdown().await.expect("shutdown upstream");
        });

        let proxy_task = tokio::spawn(async move {
            proxy_validated_postgres_streams_with_policy(
                &mut proxy_client,
                &mut proxy_upstream,
                Some(&policy),
            )
            .await
            .expect("proxy streams")
        });

        client.write_all(&startup).await.expect("write startup");
        client.write_all(&query).await.expect("write query");
        let mut response = [0_u8; 1];
        client
            .read_exact(&mut response)
            .await
            .expect("read response");
        assert_eq!(&response, b"Z");
        client.shutdown().await.expect("shutdown client");

        upstream_task.await.expect("upstream task");
        let (client_to_upstream, upstream_to_client) = proxy_task.await.expect("proxy task");
        assert_eq!(client_to_upstream, (startup.len() + query.len()) as u64);
        assert_eq!(upstream_to_client, 1);
    }

    #[tokio::test]
    async fn database_proxy_route_handle_updates_upstream_in_place() {
        let mut route = database_proxy_route();
        let handle = DatabaseProxyRouteHandle {
            state: Arc::new(RwLock::new(DatabaseProxyRouteState {
                route: route.clone(),
                tls_acceptor: None,
            })),
        };

        route.upstream_addr = "127.0.0.1:56000".to_owned();
        route.cluster_id = "cluster_456".to_owned();
        handle
            .update_route(route.clone())
            .await
            .expect("update route");

        assert_eq!(handle.route().await.upstream_addr, "127.0.0.1:56000");
        assert_eq!(handle.route().await.cluster_id, "cluster_456");

        route.listen_addr = "127.0.0.1:55433".to_owned();
        let err = handle
            .update_route(route)
            .await
            .expect_err("changed listen address should be rejected");
        assert!(matches!(err, DatabaseProxyError::InvalidRoute(_)));
    }

    fn route() -> GatewayRoute {
        GatewayRoute {
            host: "env-123.palimpsest.dev".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            sync_endpoint: "http://127.0.0.1:50051".to_owned(),
            tls_policy: TlsPolicy::TerminateAtGateway,
            rate_limit: RateLimitPolicy {
                max_connections: 100,
                max_requests_per_minute: 1_000,
            },
        }
    }

    fn database_proxy_route() -> DatabaseProxyRoute {
        DatabaseProxyRoute {
            listen_addr: "127.0.0.1:55432".to_owned(),
            upstream_addr: "127.0.0.1:55000".to_owned(),
            organization_id: "org_123".to_owned(),
            project_id: "project_123".to_owned(),
            environment_id: "env_123".to_owned(),
            cluster_id: "cluster_123".to_owned(),
            policy: None,
            tls: None,
        }
    }

    fn postgres_startup_packet(user: &str, database: &str) -> Vec<u8> {
        postgres_startup_packet_with_parameters(&[("user", user), ("database", database)])
    }

    fn postgres_startup_packet_with_parameters(parameters: &[(&str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&POSTGRES_PROTOCOL_VERSION_3.to_be_bytes());
        for (key, value) in parameters {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        let length = u32::try_from(body.len() + 4).expect("startup packet length fits");
        let mut packet = length.to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        packet
    }

    fn postgres_simple_query_packet(query: &str) -> Vec<u8> {
        let length = u32::try_from(query.len() + 5).expect("query packet length fits");
        let mut packet = vec![b'Q'];
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(query.as_bytes());
        packet.push(0);
        packet
    }
}
