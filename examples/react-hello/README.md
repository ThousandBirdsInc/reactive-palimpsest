# palimpsest-client React example

A minimal Vite + React app that consumes `palimpsest-client-js` (the
`wasm-bindgen`-built browser SDK) over gRPC-Web.

## Layout

```
react-hello/
├── README.md           ← you are here
├── package.json        ← Vite + React
├── vite.config.ts
├── tsconfig.json
├── index.html
├── build-wasm.sh       ← cargo build + wasm-bindgen → ./pkg
├── pkg/                ← generated; gitignored
└── src/
    ├── main.tsx
    ├── App.tsx
    └── usePalimpsestSubscription.ts   ← the React hook (§18.11)
```

## One-time setup

```bash
# Rust toolchain + wasm target
rustup target add wasm32-unknown-unknown

# wasm-bindgen-cli (matches the wasm-bindgen version your crate uses)
cargo install wasm-bindgen-cli --version 0.2.95   # or whatever Cargo.lock says

# Node deps
npm install

# Build the wasm bundle into ./pkg
./build-wasm.sh
```

## Run

```bash
npm run dev
# open http://localhost:5173
```

Point the URL in `App.tsx` at a Palimpsest server reachable via
gRPC-Web. (Native gRPC won't work in the browser — you need the
server's gRPC-Web sidecar enabled.)

## The hook

`usePalimpsestSubscription(url, sql, { token })` returns
`{ rows, status, error }`:

* `status: "connecting" | "open" | "closed" | "error"`
* `rows`: latest snapshot of the result set (cache-applied locally)
* `error`: the most recent `{ code, message }` event, if any

Internally the hook owns one `Client` per `url+token` and one
`Subscription` per `(url, sql, vars)` combo, calling `client.subscribe`
on mount and `subscription.unsubscribe()` on unmount.

The cache is enabled by default in `palimpsest-client`, so the hook
just listens for events and rebuilds the row list whenever a `diff`
arrives.
