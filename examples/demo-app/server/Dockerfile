# Build context expected: workspace root (../..). The Dockerfile path is
# relative to that, so `COPY . .` brings the entire workspace (palimpsest
# crates + the demo) into the image. The runtime image only ships the
# compiled binary.

# syntax=docker/dockerfile:1.7
# Stays compatible with the rust-version=1.82.0 MSRV of the workspace,
# but uses 1.88 to match the web Dockerfile (which needs 1.88+ for
# wasm-bindgen-cli's locked deps) and to reuse Docker layer cache.
ARG RUST_VERSION=1.88-bookworm

FROM rust:${RUST_VERSION} AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY . .
WORKDIR /workspace/examples/demo-app/server
RUN cargo build --release --bin palimpsest-demo-server \
    && strip target/release/palimpsest-demo-server

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder \
    /workspace/examples/demo-app/server/target/release/palimpsest-demo-server \
    /usr/local/bin/palimpsest-demo-server
EXPOSE 3000 50051
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/palimpsest-demo-server"]
