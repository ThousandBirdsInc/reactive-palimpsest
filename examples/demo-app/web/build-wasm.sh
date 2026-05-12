#!/usr/bin/env bash
# Build palimpsest-client-js for the browser into ./pkg.
# Requires `cargo install wasm-bindgen-cli` and the wasm32 target
# (`rustup target add wasm32-unknown-unknown`).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"

cd "$ROOT"

cargo build \
  -p palimpsest-client-js \
  --target wasm32-unknown-unknown \
  --profile release-wasm

WASM=target/wasm32-unknown-unknown/release-wasm/palimpsest_client_js.wasm
OUT="$HERE/pkg"
rm -rf "$OUT"
mkdir -p "$OUT"

wasm-bindgen "$WASM" --target web --out-dir "$OUT" --out-name palimpsest_client_js

if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Oz \
    "$OUT/palimpsest_client_js_bg.wasm" \
    -o "$OUT/palimpsest_client_js_bg.wasm.tmp"
  mv "$OUT/palimpsest_client_js_bg.wasm.tmp" "$OUT/palimpsest_client_js_bg.wasm"
fi

echo "wasm bundle written to $OUT"
ls -lh "$OUT"
