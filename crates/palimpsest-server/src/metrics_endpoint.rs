// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Prometheus exposition + health/readiness sidecar.
//!
//! `axum` listens on its own port (per §18.8: "Prometheus endpoint via
//! `axum` sidecar on a separate port") and exposes:
//!
//! - `GET /metrics` — renders [`RouterMetrics::snapshot`] in the
//!   [Prometheus text format][exposition].
//! - `GET /healthz` — returns 200 OK once the process is live (always,
//!   for now; intended as a kubelet liveness probe).
//! - `GET /readyz` — returns 200 OK if the most-recently reported WAL
//!   lag is below [`HealthConfig::readiness_lag_bytes`], 503 otherwise.
//!   Surfaces the lag in the body so kubectl-describe shows useful
//!   diagnostics.
//!
//! [exposition]: https://prometheus.io/docs/instrumenting/exposition_formats/

use std::fmt::Write;
use std::net::SocketAddr;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use crate::metrics::{MetricsSnapshot, RouterMetrics};

/// Tunables for the health/readiness endpoints.
#[derive(Debug, Clone, Copy)]
pub struct HealthConfig {
    /// Maximum WAL lag (in bytes) at which `/readyz` still reports 200.
    /// Above this, the endpoint returns 503. Defaults to 16 MiB.
    pub readiness_lag_bytes: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            readiness_lag_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Builds the `axum` router that serves `/metrics`, `/healthz`, and
/// `/readyz` with default health tunables.
pub fn build_router(metrics: &RouterMetrics) -> Router {
    build_router_with(metrics, HealthConfig::default())
}

/// Same as [`build_router`] but with custom [`HealthConfig`].
pub fn build_router_with(metrics: &RouterMetrics, health: HealthConfig) -> Router {
    let metrics_for_metrics = metrics.clone();
    let metrics_for_readyz = metrics.clone();
    Router::new()
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics_for_metrics.clone();
                async move { render_prometheus(&metrics.snapshot()) }
            }),
        )
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/readyz",
            get(move || {
                let metrics = metrics_for_readyz.clone();
                async move { render_readyz(&metrics.snapshot(), health) }
            }),
        )
}

fn render_readyz(snapshot: &MetricsSnapshot, health: HealthConfig) -> Response {
    if snapshot.wal_lag_bytes <= health.readiness_lag_bytes {
        (
            StatusCode::OK,
            format!(
                "ready (wal_lag_bytes={} threshold={})",
                snapshot.wal_lag_bytes, health.readiness_lag_bytes,
            ),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "not ready (wal_lag_bytes={} > threshold={})",
                snapshot.wal_lag_bytes, health.readiness_lag_bytes,
            ),
        )
            .into_response()
    }
}

/// Renders a [`MetricsSnapshot`] as Prometheus exposition text.
#[must_use]
pub fn render_prometheus(snapshot: &MetricsSnapshot) -> String {
    let mut out = String::new();

    write_counter(
        &mut out,
        "palimpsest_subscriptions_in_flight",
        "Currently-active subscriptions",
        snapshot.subscriptions_in_flight,
    );
    write_counter(
        &mut out,
        "palimpsest_subscriptions_total",
        "Subscriptions opened since startup",
        snapshot.subscriptions_total,
    );
    write_counter(
        &mut out,
        "palimpsest_channel_full_events_total",
        "Per-subscription channel saturations",
        snapshot.channel_full_events,
    );
    write_counter(
        &mut out,
        "palimpsest_resyncs_emitted_total",
        "Resync events emitted (any reason)",
        snapshot.resyncs_emitted,
    );
    write_labelled_counter_block(
        &mut out,
        "palimpsest_resyncs_by_reason_total",
        "Resync events emitted, broken down by reason",
        &[
            ("lsn_compacted", snapshot.resyncs_by_reason.lsn_compacted),
            ("schema_changed", snapshot.resyncs_by_reason.schema_changed),
            ("backpressure", snapshot.resyncs_by_reason.backpressure),
            ("slot_recreated", snapshot.resyncs_by_reason.slot_recreated),
            (
                "permissions_changed",
                snapshot.resyncs_by_reason.permissions_changed,
            ),
        ],
    );
    write_counter(
        &mut out,
        "palimpsest_diffs_sent_total",
        "Diff payloads enqueued onto subscription streams",
        snapshot.diffs_sent,
    );
    write_counter(
        &mut out,
        "palimpsest_bytes_sent_total",
        "Encoded diff payload bytes sent over the wire",
        snapshot.bytes_sent,
    );

    write_gauge(
        &mut out,
        "palimpsest_channel_depth_max",
        "High-water mark of any per-subscription channel depth observed",
        snapshot.channel_depth_max,
    );
    write_gauge(
        &mut out,
        "palimpsest_wal_lag_bytes",
        "Current WAL lag (current LSN − applied LSN), in bytes",
        snapshot.wal_lag_bytes,
    );
    write_gauge(
        &mut out,
        "palimpsest_operator_memory_bytes",
        "Aggregate per-operator memory footprint, in bytes",
        snapshot.operator_memory_bytes,
    );

    write_gauge_us(
        &mut out,
        "palimpsest_fanout_latency_p50_microseconds",
        "Estimated p50 fan-out latency in microseconds",
        snapshot.fanout_latency_p50_us,
    );
    write_gauge_us(
        &mut out,
        "palimpsest_fanout_latency_p99_microseconds",
        "Estimated p99 fan-out latency in microseconds",
        snapshot.fanout_latency_p99_us,
    );

    write_counter(
        &mut out,
        "palimpsest_permission_rule_updates_total",
        "Permission rule-set swaps applied to the running router",
        snapshot.permission_rule_updates,
    );
    write_counter(
        &mut out,
        "palimpsest_permission_resyncs_forced_total",
        "Resync(PermissionsChanged) events forced onto active subscriptions by rule swaps",
        snapshot.permission_resyncs_forced,
    );
    write_gauge_us(
        &mut out,
        "palimpsest_permission_revocation_lag_p50_microseconds",
        "Estimated p50 server-side revocation lag (rule swap to resync enqueued) in microseconds",
        snapshot.permission_revocation_lag_p50_us,
    );
    write_gauge_us(
        &mut out,
        "palimpsest_permission_revocation_lag_p99_microseconds",
        "Estimated p99 server-side revocation lag (rule swap to resync enqueued) in microseconds",
        snapshot.permission_revocation_lag_p99_us,
    );
    out
}

fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
    writeln!(out, "# HELP {name} {help}").expect("write");
    writeln!(out, "# TYPE {name} counter").expect("write");
    writeln!(out, "{name} {value}").expect("write");
}

fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    writeln!(out, "# HELP {name} {help}").expect("write");
    writeln!(out, "# TYPE {name} gauge").expect("write");
    writeln!(out, "{name} {value}").expect("write");
}

fn write_gauge_us(out: &mut String, name: &str, help: &str, value: f64) {
    writeln!(out, "# HELP {name} {help}").expect("write");
    writeln!(out, "# TYPE {name} gauge").expect("write");
    writeln!(out, "{name} {value}").expect("write");
}

fn write_labelled_counter_block(out: &mut String, name: &str, help: &str, rows: &[(&str, u64)]) {
    writeln!(out, "# HELP {name} {help}").expect("write");
    writeln!(out, "# TYPE {name} counter").expect("write");
    for (reason, value) in rows {
        writeln!(out, "{name}{{reason=\"{reason}\"}} {value}").expect("write");
    }
}

/// Binds the metrics sidecar on `addr` and runs until `shutdown`
/// resolves.
///
/// # Errors
/// Surfaces `axum`/`tokio::net` bind failures.
pub async fn serve_until<F>(
    addr: SocketAddr,
    metrics: RouterMetrics,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let app = build_router(&metrics);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::{render_prometheus, render_readyz, HealthConfig, RouterMetrics};
    use axum::http::StatusCode;

    #[test]
    fn render_includes_help_and_type_lines() {
        let metrics = RouterMetrics::new();
        metrics.subscribe();
        metrics.record_channel_full();
        let body = render_prometheus(&metrics.snapshot());
        assert!(body.contains("# HELP palimpsest_subscriptions_in_flight"));
        assert!(body.contains("# TYPE palimpsest_subscriptions_in_flight counter"));
        assert!(body.contains("palimpsest_subscriptions_in_flight 1"));
        assert!(body.contains("palimpsest_channel_full_events_total 1"));
    }

    #[test]
    fn render_emits_zero_baselines_when_idle() {
        let body = render_prometheus(&RouterMetrics::new().snapshot());
        assert!(body.contains("palimpsest_subscriptions_total 0"));
        assert!(body.contains("palimpsest_diffs_sent_total 0"));
        assert!(body.contains("palimpsest_fanout_latency_p50_microseconds 0"));
        assert!(body.contains(r#"palimpsest_resyncs_by_reason_total{reason="backpressure"} 0"#));
        assert!(body.contains("palimpsest_bytes_sent_total 0"));
        assert!(body.contains("palimpsest_wal_lag_bytes 0"));
    }

    #[test]
    fn readyz_reports_200_below_threshold() {
        let metrics = RouterMetrics::new();
        metrics.set_wal_lag_bytes(1024);
        let response = render_readyz(&metrics.snapshot(), HealthConfig::default());
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn readyz_reports_503_above_threshold() {
        let metrics = RouterMetrics::new();
        metrics.set_wal_lag_bytes(64 * 1024 * 1024);
        let response = render_readyz(
            &metrics.snapshot(),
            HealthConfig {
                readiness_lag_bytes: 1024,
            },
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn build_router_with_returns_a_router() {
        let metrics = RouterMetrics::new();
        let _router = super::build_router_with(&metrics, HealthConfig::default());
    }
}
