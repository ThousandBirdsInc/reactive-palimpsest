# Build context expected: workspace root (../..).
#
# Stage 1 (wasm-builder): build the wasm bundle (Rust + wasm-bindgen-cli).
# Stage 2 (ts-builder):  build the @palimpsest/client TS package (tsc).
# Stage 3 (web-builder): build the Vite static bundle.
# Stage 4 (runtime):     serve the static bundle with nginx.

# syntax=docker/dockerfile:1.7
# Rust toolchain needs to be ≥1.88 to install `wasm-bindgen-cli 0.2.121`
# with --locked: the CLI itself requires ≥1.86, and its locked
# transitive deps (`time@0.3.47`, …) require ≥1.88. The codebase's MSRV
# is still 1.82 — we're just using a newer compiler at *build* time.
ARG RUST_VERSION=1.88-bookworm
ARG NODE_VERSION=20-bookworm-slim
ARG WASM_BINDGEN_VERSION=0.2.121

FROM rust:${RUST_VERSION} AS wasm-builder
ARG WASM_BINDGEN_VERSION
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler binaryen \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add wasm32-unknown-unknown \
    && cargo install wasm-bindgen-cli --locked --version ${WASM_BINDGEN_VERSION}
WORKDIR /workspace
COPY . .
RUN cd /workspace && \
    cargo build -p palimpsest-client-js --target wasm32-unknown-unknown --profile release-wasm && \
    mkdir -p /pkg && \
    wasm-bindgen \
        target/wasm32-unknown-unknown/release-wasm/palimpsest_client_js.wasm \
        --target web --out-dir /pkg --out-name palimpsest_client_js && \
    wasm-opt -Oz \
        --enable-bulk-memory --enable-sign-ext \
        --enable-mutable-globals --enable-nontrapping-float-to-int \
        --enable-reference-types \
        /pkg/palimpsest_client_js_bg.wasm \
        -o /pkg/palimpsest_client_js_bg.wasm.tmp && \
    mv /pkg/palimpsest_client_js_bg.wasm.tmp /pkg/palimpsest_client_js_bg.wasm

FROM node:${NODE_VERSION} AS ts-builder
WORKDIR /pkg-src
COPY packages/palimpsest-client-typescript/package.json \
     packages/palimpsest-client-typescript/package-lock.json* \
     packages/palimpsest-client-typescript/tsconfig.json \
     packages/palimpsest-client-typescript/tsconfig.build.json \
     ./
RUN npm install --no-audit --no-fund
COPY packages/palimpsest-client-typescript/src ./src
RUN npm run build

FROM node:${NODE_VERSION} AS web-builder
WORKDIR /web
# Bring in the @palimpsest/client package at the resolution point the
# demo's package.json expects (file:../../../packages/palimpsest-client-typescript).
# Copy ONLY the runtime artifacts (package.json + dist/) — not the
# ts-builder's nested node_modules, which would otherwise poison the
# demo's npm install with a duplicate `react` and trigger the
# `Cannot read properties of undefined (reading 'extraneous')` npm v10 bug.
COPY --from=ts-builder /pkg-src/package.json /packages/palimpsest-client-typescript/package.json
COPY --from=ts-builder /pkg-src/dist /packages/palimpsest-client-typescript/dist
COPY examples/demo-app/web/package.json ./
RUN npm install --no-audit --no-fund
COPY examples/demo-app/web/ ./
COPY --from=wasm-builder /pkg /web/pkg
RUN npm run build

FROM nginx:1.27-alpine AS runtime
COPY --from=web-builder /web/dist /usr/share/nginx/html
COPY examples/demo-app/web/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
