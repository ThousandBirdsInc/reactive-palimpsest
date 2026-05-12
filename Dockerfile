# syntax=docker/dockerfile:1.7
# Multi-stage build for the `palimpsest` CLI (§18.14).
#
# Stage 1 (planner): runs `cargo chef prepare` to capture the workspace
# dependency graph as a recipe.json so subsequent rebuilds reuse the
# dependency cache when only application code changes.
#
# Stage 2 (cacher): builds *only* the dependencies based on recipe.json.
#
# Stage 3 (builder): compiles the actual binary on top of the cached
# dependency layer.
#
# Stage 4 (runtime): copies the static, stripped binary into a Distroless
# base. We deliberately ship the binary in a near-empty image (no shell,
# no package manager) to keep the attack surface minimal — the §18.14
# target is ≤ 30 MB final image.
#
# Build:
#   docker build -t palimpsest:dev .
# Run (with a mounted config + ports):
#   docker run --rm -p 50051:50051 -p 9090:9090 \
#     -v $(pwd)/palimpsest.toml:/etc/palimpsest/palimpsest.toml \
#     palimpsest:dev /etc/palimpsest/palimpsest.toml

ARG RUST_VERSION=1.82-bookworm

FROM rust:${RUST_VERSION} AS chef
RUN cargo install cargo-chef --locked --version 0.1.68
WORKDIR /workspace

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS cacher
COPY --from=planner /workspace/recipe.json recipe.json
# protoc is required for `palimpsest-proto` build script.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN cargo chef cook --release --recipe-path recipe.json --bin palimpsest

FROM chef AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY --from=cacher /workspace/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
COPY . .
RUN cargo build --release --bin palimpsest \
    && strip target/release/palimpsest

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
COPY --from=builder /workspace/target/release/palimpsest /usr/local/bin/palimpsest
EXPOSE 50051 9090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/palimpsest"]
CMD ["serve", "/etc/palimpsest/palimpsest.toml"]
