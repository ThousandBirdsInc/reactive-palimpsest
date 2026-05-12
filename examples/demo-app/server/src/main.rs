//! Demo backend: an axum write API on :3000 and a Palimpsest gRPC-Web
//! service on :50051, sharing one in-memory `posts` store.
//!
//! The Rust side is a single-binary example of the production topology:
//! one process owns the application's write surface *and* embeds
//! Palimpsest as a library. The browser subscribes via gRPC-Web; the
//! same browser session POSTs writes to the HTTP API.

mod api;
mod state;
mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use palimpsest_server::{AnonymousAuthenticator, Palimpsest};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::api::{router, AppState};
use crate::state::{DemoWalRuntime, Post, Store};

const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:3000";
const DEFAULT_GRPC_ADDR: &str = "0.0.0.0:50051";

fn addr_from_env(var: &str, default: &str) -> SocketAddr {
    std::env::var(var)
        .ok()
        .as_deref()
        .unwrap_or(default)
        .parse()
        .expect("valid SocketAddr")
}

fn seed_posts() -> Vec<Post> {
    vec![
        Post {
            id: 1,
            title: "Welcome to Palimpsest".to_owned(),
            published: true,
        },
        Post {
            id: 2,
            title: "Subscribe to a live SQL view".to_owned(),
            published: true,
        },
        Post {
            id: 3,
            title: "Draft: in-progress writeup".to_owned(),
            published: false,
        },
    ]
}

/// Bare-bones `--healthcheck` shortcut so the Docker healthcheck doesn't
/// need curl/wget in the runtime image. Probes the HTTP API and exits
/// 0/1 based on a TCP connect.
async fn run_healthcheck() -> std::process::ExitCode {
    use tokio::io::AsyncWriteExt;
    let target = std::env::var("HEALTHCHECK_TARGET").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    match tokio::net::TcpStream::connect(&target).await {
        Ok(mut stream) => {
            let _ = stream
                .write_all(b"GET /api/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await;
            std::process::ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("healthcheck connect failed: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    if std::env::args().any(|a| a == "--healthcheck") {
        return run_healthcheck().await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,palimpsest_demo_server=debug")),
        )
        .with_target(false)
        .init();

    let store = Arc::new(Store::with_seed(seed_posts()));

    let grpc_addr = addr_from_env("PALIMPSEST_DEMO_GRPC_ADDR", DEFAULT_GRPC_ADDR);
    let http_addr = addr_from_env("PALIMPSEST_DEMO_HTTP_ADDR", DEFAULT_HTTP_ADDR);

    let palimpsest = Palimpsest::builder()
        .with_wal(DemoWalRuntime::new(Arc::clone(&store)))
        .with_auth(AnonymousAuthenticator)
        .with_grpc_addr(grpc_addr)
        .with_metrics_addr(None)
        .build()
        .expect("palimpsest build");

    let (shutdown_grpc_tx, shutdown_grpc_rx) = oneshot::channel::<()>();
    let (shutdown_http_tx, shutdown_http_rx) = oneshot::channel::<()>();

    let grpc_handle = tokio::spawn(async move {
        info!(%grpc_addr, "palimpsest gRPC-Web listening");
        if let Err(err) = palimpsest
            .serve(async move {
                let _ = shutdown_grpc_rx.await;
            })
            .await
        {
            error!(?err, "gRPC server error");
        }
    });

    let http_state = AppState {
        store: Arc::clone(&store),
    };
    let http_handle = tokio::spawn(async move {
        info!(%http_addr, "write API listening");
        let listener = match TcpListener::bind(http_addr).await {
            Ok(l) => l,
            Err(err) => {
                error!(?err, "http bind failed");
                return;
            }
        };
        // The WS bridge dials the gRPC server in-process; rewrite a
        // wildcard bind (0.0.0.0) to loopback so the dial actually
        // connects.
        let grpc_dial_addr = if grpc_addr.ip().is_unspecified() {
            SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), grpc_addr.port())
        } else {
            grpc_addr
        };
        let app = router(http_state, grpc_dial_addr);
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_http_rx.await;
            })
            .await
        {
            error!(?err, "http server error");
        }
    });

    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
    let _ = shutdown_grpc_tx.send(());
    let _ = shutdown_http_tx.send(());
    let _ = tokio::join!(grpc_handle, http_handle);
    std::process::ExitCode::SUCCESS
}
