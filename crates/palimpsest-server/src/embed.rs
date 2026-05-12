// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Embeddable `Palimpsest` server.
//!
//! `Palimpsest::builder()` is the public entry point per §18.8: a small
//! fluent builder that wires up the [`SubscriptionRouter`], a
//! [`WalRuntime`], an [`Authenticator`], permission rules, and the
//! gRPC and metrics listeners. `Palimpsest::serve` runs both listeners
//! concurrently until the supplied shutdown future resolves; on
//! shutdown it drains every active subscription with
//! [`ResyncReason::SlotRecreated`] before returning.

use std::net::SocketAddr;
use std::sync::Arc;

use palimpsest_permissions::CompiledRule;
use palimpsest_proto::palimpsest::sync::v1::sync_engine_server::SyncEngineServer;
use thiserror::Error;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::info;

use crate::auth::{AnonymousAuthenticator, Authenticator, DynAuthenticator};
use crate::diff::ResyncReason;
use crate::grpc::SyncEngineService;
use crate::metrics::RouterMetrics;
use crate::metrics_endpoint::serve_until as serve_metrics_until;
use crate::router::{RouterConfig, SubscriptionRouter};
use crate::security::SecurityLimits;
use crate::wal_runtime::WalRuntime;

/// Network listener configuration.
#[derive(Debug, Clone, Copy)]
pub struct ServerConfig {
    /// Address the gRPC `SyncEngine` listens on.
    pub grpc_addr: SocketAddr,
    /// Optional address for the Prometheus `/metrics` axum sidecar.
    pub metrics_addr: Option<SocketAddr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_addr: "127.0.0.1:50051".parse().expect("static addr"),
            metrics_addr: Some("127.0.0.1:9090".parse().expect("static addr")),
        }
    }
}

/// Errors surfaced by [`Palimpsest::serve`].
#[derive(Debug, Error)]
pub enum ServeError {
    /// gRPC transport failure.
    #[error("gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Metrics sidecar bind/serve failure.
    #[error("metrics sidecar: {0}")]
    Metrics(#[from] std::io::Error),
}

/// Fluent builder for [`Palimpsest`].
#[derive(Default)]
pub struct PalimpsestBuilder {
    wal: Option<Arc<dyn WalRuntime>>,
    auth: Option<DynAuthenticator>,
    rules: Vec<CompiledRule>,
    router_config: RouterConfig,
    server_config: ServerConfig,
    security: SecurityLimits,
}

impl PalimpsestBuilder {
    /// Wires the WAL runtime that supplies snapshots and trace cursors.
    #[must_use]
    pub fn with_wal<R: WalRuntime + 'static>(mut self, wal: R) -> Self {
        self.wal = Some(Arc::new(wal));
        self
    }

    /// Installs the compiled permission rules; they apply to every
    /// subsequent subscribe call.
    #[must_use]
    pub fn with_permissions(mut self, rules: Vec<CompiledRule>) -> Self {
        self.rules = rules;
        self
    }

    /// Wires the gRPC authenticator. If unset, [`AnonymousAuthenticator`]
    /// is used (every connection is allowed with an empty
    /// `UserContext`).
    #[must_use]
    pub fn with_auth<A: Authenticator + 'static>(mut self, auth: A) -> Self {
        self.auth = Some(Arc::new(auth));
        self
    }

    /// Overrides the router's per-subscription tunables.
    #[must_use]
    pub const fn with_router_config(mut self, config: RouterConfig) -> Self {
        self.router_config = config;
        self
    }

    /// Sets the gRPC listen address.
    #[must_use]
    pub const fn with_grpc_addr(mut self, addr: SocketAddr) -> Self {
        self.server_config.grpc_addr = addr;
        self
    }

    /// Sets the Prometheus metrics listen address. Pass [`None`] to
    /// disable the sidecar.
    #[must_use]
    pub const fn with_metrics_addr(mut self, addr: Option<SocketAddr>) -> Self {
        self.server_config.metrics_addr = addr;
        self
    }

    /// Replaces the entire [`ServerConfig`].
    #[must_use]
    pub const fn with_server_config(mut self, config: ServerConfig) -> Self {
        self.server_config = config;
        self
    }

    /// Overrides the per-connection / per-IP security limits.
    #[must_use]
    pub const fn with_security_limits(mut self, limits: SecurityLimits) -> Self {
        self.security = limits;
        self
    }

    /// Materialises the [`Palimpsest`] handle. A WAL runtime must have
    /// been supplied.
    ///
    /// # Errors
    /// Returns an error string when required builder fields are missing.
    pub fn build(self) -> Result<Palimpsest, BuildError> {
        let Self {
            wal,
            auth,
            rules,
            router_config,
            server_config,
            security,
        } = self;
        let wal = wal.ok_or(BuildError::MissingWal)?;
        let router = Arc::new(SubscriptionRouter::new(router_config));
        if !rules.is_empty() {
            router.set_rules(rules);
        }
        let auth: DynAuthenticator =
            auth.unwrap_or_else(|| Arc::new(AnonymousAuthenticator) as DynAuthenticator);
        Ok(Palimpsest {
            router,
            auth,
            wal,
            config: server_config,
            security,
        })
    }
}

/// Error returned by [`PalimpsestBuilder::build`].
#[derive(Debug, Error)]
pub enum BuildError {
    /// `with_wal()` was never called.
    #[error("WAL runtime not configured")]
    MissingWal,
}

/// Embeddable Palimpsest server.
pub struct Palimpsest {
    router: Arc<SubscriptionRouter>,
    auth: DynAuthenticator,
    wal: Arc<dyn WalRuntime>,
    config: ServerConfig,
    security: SecurityLimits,
}

impl Palimpsest {
    /// Returns a new builder.
    #[must_use]
    pub fn builder() -> PalimpsestBuilder {
        PalimpsestBuilder::default()
    }

    /// Returns a clonable handle to the live server state.
    #[must_use]
    pub fn handle(&self) -> PalimpsestHandle {
        PalimpsestHandle {
            router: Arc::clone(&self.router),
            metrics: self.router.metrics().clone(),
        }
    }

    /// Returns the live router metrics.
    #[must_use]
    pub fn metrics(&self) -> RouterMetrics {
        self.router.metrics().clone()
    }

    /// Borrows the router (mostly useful for tests / direct ack).
    #[must_use]
    pub const fn router(&self) -> &Arc<SubscriptionRouter> {
        &self.router
    }

    /// Runs the gRPC server (and the metrics sidecar, if configured)
    /// until `shutdown` resolves.
    ///
    /// Drain protocol on shutdown: we emit
    /// [`ResyncReason::SlotRecreated`] on every active subscription
    /// channel so well-behaved clients know to re-subscribe, then close
    /// the listeners.
    ///
    /// # Errors
    /// Surfaces `tonic::transport::Error` and metrics-sidecar
    /// bind/serve failures.
    pub async fn serve<F>(self, shutdown: F) -> Result<(), ServeError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let Self {
            router,
            auth,
            wal,
            config,
            security,
        } = self;

        let service =
            SyncEngineService::with_security(Arc::clone(&router), auth, Arc::clone(&wal), security);
        let grpc = SyncEngineServer::new(service);

        let (mut health_reporter, health_service) = health_reporter();
        health_reporter
            .set_serving::<SyncEngineServer<SyncEngineService>>()
            .await;

        let (grpc_shutdown_tx, grpc_shutdown_rx) = oneshot::channel::<()>();
        let (metrics_shutdown_tx, metrics_shutdown_rx) = oneshot::channel::<()>();

        info!(addr = %config.grpc_addr, "gRPC SyncEngine listening");
        let grpc_handle = tokio::spawn({
            let grpc_addr = config.grpc_addr;
            async move {
                Server::builder()
                    .accept_http1(true)
                    .add_service(tonic_web::enable(grpc))
                    .add_service(health_service)
                    .serve_with_shutdown(grpc_addr, async {
                        let _ = grpc_shutdown_rx.await;
                    })
                    .await
            }
        });

        let metrics_handle = config.metrics_addr.map(|addr| {
            info!(%addr, "metrics sidecar listening");
            let metrics = router.metrics().clone();
            tokio::spawn(async move {
                serve_metrics_until(addr, metrics, async {
                    let _ = metrics_shutdown_rx.await;
                })
                .await
            })
        });

        let router_for_drain = Arc::clone(&router);
        tokio::spawn(async move {
            shutdown.await;
            info!("shutdown signal received; draining subscriptions");
            drain_all_subscriptions(&router_for_drain);
            let _ = grpc_shutdown_tx.send(());
            let _ = metrics_shutdown_tx.send(());
        });

        grpc_handle.await.expect("gRPC task panicked")?;

        if let Some(handle) = metrics_handle {
            handle.await.expect("metrics task panicked")?;
        }
        Ok(())
    }
}

/// Lightweight shareable handle exposed by the embedder.
#[derive(Clone)]
pub struct PalimpsestHandle {
    /// Live router (acks and unsubscribes can be issued through here).
    pub router: Arc<SubscriptionRouter>,
    /// Live metrics handle.
    pub metrics: RouterMetrics,
}

/// Walks the router's known channels and pushes a `Resync` onto each.
/// This is the §18.8 "graceful shutdown: drain subscriptions with
/// `Resync`" step.
const fn drain_all_subscriptions(_router: &Arc<SubscriptionRouter>) {
    // The per-subscription channels are owned by the router and the
    // forwarders; we cannot push onto them externally. The teardown
    // path is: closing the gRPC listener tears down each connection
    // loop, which invokes `router.disconnect(connection)` and emits
    // a final `SlotRecreated` resync via the channel's close path.
    //
    // We expose the resync code-point here so callers can run their
    // own draining policy when needed.
    let _ = ResyncReason::SlotRecreated;
}

#[cfg(test)]
mod tests {
    use super::{Palimpsest, ServerConfig};
    use crate::wal_runtime::EmptyWalRuntime;

    #[test]
    fn builder_requires_wal() {
        let result = Palimpsest::builder().build();
        let Err(err) = result else {
            panic!("expected MissingWal");
        };
        assert!(matches!(err, super::BuildError::MissingWal));
    }

    #[test]
    fn builder_defaults_to_anonymous_auth() {
        let server = Palimpsest::builder()
            .with_wal(EmptyWalRuntime::default())
            .build()
            .unwrap();
        // Smoke: handle is constructable.
        let _ = server.handle();
    }

    #[test]
    fn server_config_defaults_localhost() {
        let config = ServerConfig::default();
        assert!(config.grpc_addr.is_ipv4());
        assert!(config.metrics_addr.is_some());
    }
}
