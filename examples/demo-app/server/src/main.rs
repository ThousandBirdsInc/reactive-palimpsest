//! Demo backend: an axum write API on :3000 and a Palimpsest gRPC-Web
//! service on :50051, both driven by a Postgres logical-replication
//! slot.
//!
//! Topology (single binary, three concurrent tasks):
//!
//! 1. **Postgres consumer** — tails the `palimpsest_demo` slot,
//!    decodes pgoutput frames, mirrors `posts`, `orders`, and `accounts`
//!    in-memory,
//!    and appends to the per-table journals the WAL runtime cursor
//!    consumes.
//! 2. **Write API (axum)** — issues SQL through tokio-postgres;
//!    Postgres writes the WAL, the consumer picks the changes up
//!    after a ~100ms hop, and subscribers see them via the dataflow.
//! 3. **gRPC-Web (Palimpsest)** — reads from the same in-memory
//!    mirror via the `WalRuntime` trait. Permissions + dataflow
//!    execution are unchanged from the in-memory demo.

mod api;
mod auth;
mod db;
mod state;
mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use palimpsest_permissions::{compile_rules, PermissionRule, UserContextSchema};
use palimpsest_server::{JwtAuthenticator, Palimpsest};
use palimpsest_sql::{Catalog, ColumnSchema, ColumnType, TableSchema};
use palimpsest_wal::TableId;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::api::{router, AppState};
use crate::state::{AccountStore, DemoWalRuntime, OrderStore, Store};

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

/// Bare-bones `--healthcheck` shortcut so the Docker healthcheck
/// doesn't need curl/wget in the runtime image.
async fn run_healthcheck() -> std::process::ExitCode {
    use tokio::io::AsyncWriteExt;
    let target =
        std::env::var("HEALTHCHECK_TARGET").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
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

    // -------------------------------------------------------------
    // Postgres bootstrap: schema, publication, slot, seed, snapshot.
    // Everything downstream depends on this being done before we
    // start accepting subscribers.
    // -------------------------------------------------------------
    let pg = match db::bootstrap().await {
        Ok(pg) => pg,
        Err(err) => {
            error!(%err, "postgres bootstrap failed");
            return std::process::ExitCode::FAILURE;
        }
    };

    let posts_snapshot = match db::snapshot_posts(&pg.client).await {
        Ok(rows) => rows,
        Err(err) => {
            error!(%err, "posts snapshot failed");
            return std::process::ExitCode::FAILURE;
        }
    };
    let orders_snapshot = match db::snapshot_orders(&pg.client).await {
        Ok(rows) => rows,
        Err(err) => {
            error!(%err, "orders snapshot failed");
            return std::process::ExitCode::FAILURE;
        }
    };
    let accounts_snapshot = match db::snapshot_accounts(&pg.client).await {
        Ok(rows) => rows,
        Err(err) => {
            error!(%err, "accounts snapshot failed");
            return std::process::ExitCode::FAILURE;
        }
    };
    info!(
        posts = posts_snapshot.len(),
        orders = orders_snapshot.len(),
        accounts = accounts_snapshot.len(),
        "initial snapshot loaded from postgres",
    );

    let store = Arc::new(Store::from_snapshot(posts_snapshot));
    let order_store = Arc::new(OrderStore::from_snapshot(orders_snapshot));
    let account_store = Arc::new(AccountStore::from_snapshot(accounts_snapshot));

    // -------------------------------------------------------------
    // Start consuming the replication slot. The consumer owns its
    // own Postgres connection so the write client isn't blocked.
    // -------------------------------------------------------------
    let _consumer = db::spawn_consumer(
        pg.settings.clone(),
        pg.posts_oid,
        pg.orders_oid,
        pg.accounts_oid,
        Arc::clone(&store),
        Arc::clone(&order_store),
        Arc::clone(&account_store),
    );

    let grpc_addr = addr_from_env("PALIMPSEST_DEMO_GRPC_ADDR", DEFAULT_GRPC_ADDR);
    let http_addr = addr_from_env("PALIMPSEST_DEMO_HTTP_ADDR", DEFAULT_HTTP_ADDR);

    let user_schema = UserContextSchema::new([
        ("id".to_owned(), ColumnType::Text),
        ("is_admin".to_owned(), ColumnType::Bool),
    ]);
    let permission_rules = compile_rules(
        &[
            PermissionRule::new(
                "posts_visibility",
                "posts",
                "published = true OR $user.is_admin = true",
            ),
            PermissionRule::new(
                "accounts_visibility",
                "accounts",
                "owner_user_id = $user.id OR $user.is_admin = true",
            ),
        ],
        &demo_catalog(),
        &user_schema,
    )
    .expect("compile permission rules");

    let palimpsest = Palimpsest::builder()
        .with_wal(DemoWalRuntime::new(
            Arc::clone(&store),
            Arc::clone(&order_store),
            Arc::clone(&account_store),
            TableId::new(pg.posts_oid),
            TableId::new(pg.orders_oid),
            TableId::new(pg.accounts_oid),
        ))
        .with_auth(
            JwtAuthenticator::from_config(auth::jwt_auth_config())
                .await
                .expect("jwt auth config"),
        )
        .with_permissions(permission_rules)
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
        pg: Arc::clone(&pg.client),
        store: Arc::clone(&store),
        orders: Arc::clone(&order_store),
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

fn demo_catalog() -> Catalog {
    let mut tables: Vec<TableSchema> = Catalog::demo().tables().cloned().collect();
    tables.push(TableSchema::new(
        "accounts",
        vec![
            ColumnSchema::new("id", ColumnType::Int),
            ColumnSchema::new("owner_user_id", ColumnType::Text),
            ColumnSchema::new("display_name", ColumnType::Text),
            ColumnSchema::new("balance_cents", ColumnType::Int),
        ],
    ));
    Catalog::new(tables)
}
