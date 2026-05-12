// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `tracing-subscriber` bootstrap.
//!
//! Splits the format depending on whether stdout is a TTY: humans get
//! `tracing-subscriber::fmt::Layer` with timestamp + ANSI; production
//! pipes get JSON so connection/subscription IDs survive into log
//! aggregation.
//!
//! Optional `OTel`: build with `--features otel` and set the standard
//! `OTEL_EXPORTER_OTLP_ENDPOINT` env var to also export spans over OTLP
//! gRPC. The exporter uses the Tokio runtime and a batching span
//! processor so it does not block the tracing hot path.

use std::io::IsTerminal;

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Installs the global `tracing-subscriber`.
///
/// Honours `RUST_LOG`; falls back to `palimpsest=info`. Idempotent —
/// safe to call multiple times (the first install wins, subsequent
/// calls return without error).
pub fn install() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("palimpsest_server=info,palimpsest=info"));

    let fmt_layer = if std::io::stdout().is_terminal() {
        fmt::layer().with_target(true).boxed()
    } else {
        fmt::layer()
            .json()
            .with_target(true)
            .with_current_span(true)
            .with_span_list(false)
            .boxed()
    };

    #[cfg(feature = "otel")]
    let init = tracing_subscriber::registry()
        .with(filter)
        .with(otel_layer())
        .with(fmt_layer)
        .try_init()
        .is_ok();
    #[cfg(not(feature = "otel"))]
    let init = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init()
        .is_ok();
    let _ = init;
}

#[cfg(feature = "otel")]
fn otel_layer<S>(
) -> Option<tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use opentelemetry::trace::TracerProvider;

    std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT")?;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .ok()?;
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();
    let tracer = provider.tracer("palimpsest");
    opentelemetry::global::set_tracer_provider(provider);
    Some(tracing_opentelemetry::layer().with_tracer(tracer))
}

#[cfg(test)]
mod tests {
    #[test]
    fn install_is_idempotent() {
        super::install();
        super::install();
    }
}
