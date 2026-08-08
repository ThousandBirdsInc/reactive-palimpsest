# WASM Client Quickstart

This is the minimal HTML + JS app that subscribes to a Palimpsest
query from a browser.

## Build the WASM bundle

The `palimpsest-client-js` crate is the wasm-bindgen wrapper around
the native Rust client. From the repo root:

```sh
# Install wasm-pack if needed:
#   cargo install wasm-pack
wasm-pack build crates/palimpsest-client-js \
    --target web \
    --out-dir ../../examples/browser/pkg
```

This produces `pkg/palimpsest_client_js.js`,
`pkg/palimpsest_client_js_bg.wasm`, and TypeScript typings in
`examples/browser/pkg/`.

## Server-side requirements

The server must have `tonic-web` enabled (the embedded server already
does this — see `embed.rs`). Browsers can only speak HTTP/1.1 and
HTTP/2 cleartext if the page allows mixed content; in production put
the server behind a TLS proxy and point the client at `https://…`.

CORS: the proxy must allow the origin of the page that loads the
WASM. For nginx that's `add_header Access-Control-Allow-Origin …;`.

## Minimal HTML + JS

`examples/browser/index.html`:

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Palimpsest WASM demo</title>
  </head>
  <body>
    <h1>Live posts</h1>
    <ul id="posts"></ul>

    <script type="module">
      import init, { Client, UserValue } from "./pkg/palimpsest_client_js.js";

      const list = document.getElementById("posts");
      const rows = new Map();

      function render() {
        list.innerHTML = "";
        for (const [id, row] of rows) {
          const li = document.createElement("li");
          li.textContent = `#${id} — ${row.title}`;
          list.appendChild(li);
        }
      }

      async function main() {
        await init();

        const client = await Client.connect("https://api.example.com", {
          token: "<jwt-from-your-auth>",
          userContext: {
            org_id: UserValue.int(42),
            is_admin: UserValue.bool(false),
          },
        });

        const sub = await client.subscribe(`
          SELECT id, title
          FROM posts
          WHERE published = true
        `);

        sub.on_snapshot((batch) => {
          for (const row of batch) rows.set(row.id, row);
          render();
        });

        sub.on_diff((event) => {
          for (const change of event.changes) {
            switch (change.op) {
              case "insert":
              case "update":
                rows.set(change.row.id, change.row);
                break;
              case "delete":
                rows.delete(change.row.id);
                break;
            }
          }
          render();
          sub.ack(event.lsn);
        });

        sub.on_resync(() => {
          rows.clear();
          render();
        });

        sub.on_error((err) => {
          console.error("subscription error:", err);
        });
      }

      main().catch(console.error);
    </script>
  </body>
</html>
```

Serve it from any static-file host (or `python3 -m http.server` from
the `examples/browser/` directory) and load it in a browser.

## What you'll see

1. **Snapshot**: `on_snapshot` fires once with every row currently
   matching the predicate (subject to permission rules).
2. **Diffs**: `on_diff` fires every time the upstream Postgres
   commits a change that affects the result set. `change.op` is
   `"insert"`, `"update"`, or `"delete"`; `event.lsn` is the
   monotonic LSN you should ack once durably consumed.
3. **Resync**: `on_resync` fires if the server decides to restart
   the stream (channel saturation, slot recreated, compaction
   window exceeded). Clear local state and wait for the next
   snapshot.

## Connection lifecycle

The `Client` keeps the gRPC stream alive across subscriptions.
`Client.disconnect()` closes everything. To resume after a network
blip with the same LSN cursor, store the last acked LSN in
`localStorage` and pass it to `client.subscribe(sql, { resumeFromLsn })`.

## Named prepared queries

Shipping SQL in the page exposes schema internals and couples SQL
changes to frontend deploys. If the server registers queries by name
([NAMED-QUERIES.md](NAMED-QUERIES.md)), subscribe with the name and
typed params instead:

```js
const sub = await client.subscribeNamed("BoardCards", {
  board_id: boardId,
});
```

The browser never holds SQL; unknown names and bad params are refused
server-side (`unknown_query` / `invalid_params` error events).

## Production checklist

- [ ] Serve over HTTPS (gRPC-Web requires either TLS or `localhost`).
- [ ] Set CORS headers on the proxy.
- [ ] Ship a stable JWT minting flow (don't embed long-lived tokens
      in the page).
- [ ] Prefer named prepared queries over raw SQL in the bundle.
- [ ] Persist `lastAckedLsn` so reconnects skip the snapshot.
- [ ] Wire `on_resync` to clear local state cleanly.
- [ ] Handle `on_error` — `rate_limited`, `connection_saturated`, and
      `query_too_large` are recoverable; `permission_denied` is not.

## Bundle size

A release build of `palimpsest-client-js` is ~250 KiB gzipped. If
you're shipping it on the critical path:

- Build with `--release` (the `wasm-pack build` invocation above
  already does).
- Enable the `wee_alloc` feature via
  `wasm-pack build … --features palimpsest-client-js/wee_alloc` for
  a smaller allocator.
- Lazy-load the module so it's not in the initial render path.
