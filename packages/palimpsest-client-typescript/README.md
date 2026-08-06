# `@1kbirds/palimpsest-client`

Type-safe TypeScript wrapper around
[`palimpsest-client-js`](../../crates/palimpsest-client-js) (the
`wasm-pack --target web` bundle), with React hooks.

The wasm bundle handles transport, codec, and reconnection. This
package adds:

- Strongly-typed subscriptions: declare a row type once,
  `client.subscribe<Post>(sql)` projects each diff into typed
  objects.
- Schema-aware row decoder with optional per-column overrides
  (custom JSON parsing, `bigint → number` coercion, etc.).
- React hooks (`usePalimpsestClient`, `usePalimpsestSubscription`)
  that handle wasm init, connection caching, and tear-down — no
  manual lifecycle in your components.

The package has **zero runtime coupling** to a specific wasm bundle
location. You import the wasm-pack output yourself and pass the
module to `PalimpsestClient.connect`, which keeps the package
bundler-agnostic (Vite, webpack, esbuild, Rollup all work).

## Install

In a monorepo with the wasm crate built into a sibling directory:

```json
{
  "dependencies": {
    "@1kbirds/palimpsest-client": "^0.1.0",
    "react": "^18.3.1"
  }
}
```

The wasm bundle (built via `wasm-pack build crates/palimpsest-client-js --target web`)
is imported directly from your app.

## Quick start (vanilla TS)

```ts
import { PalimpsestClient } from "@1kbirds/palimpsest-client";
import * as wasm from "./pkg/palimpsest_client_js";

interface Post {
  id: bigint;
  title: string;
  published: boolean;
}

const client = await PalimpsestClient.connect({
  url: "http://localhost:50051",
  wasm,
});

const sub = await client.subscribe<Post>(
  "SELECT id, title, published FROM posts",
);

sub.onEvent((event) => {
  switch (event.kind) {
    case "accepted":
      console.log("schema:", event.schema);
      break;
    case "diff":
      console.log(event.op, event.rows);  // Post[]
      break;
    case "resync":
      console.warn("resync:", event.reason);
      break;
    case "error":
      console.error(event.code, event.message);
      break;
  }
});

// later
await sub.unsubscribe();
await client.shutdown();
```

## Quick start (React)

```tsx
import { useMemo } from "react";
import {
  usePalimpsestClient,
  usePalimpsestSubscription,
} from "@1kbirds/palimpsest-client/react";
import * as wasm from "./pkg/palimpsest_client_js";

interface Post {
  id: bigint;
  title: string;
  published: boolean;
}

export function App() {
  const opts = useMemo(
    () => ({ url: "http://localhost:50051", wasm }),
    [],
  );
  const { client } = usePalimpsestClient(opts);

  const { status, rows, error } = usePalimpsestSubscription<Post>(
    client,
    "SELECT id, title, published FROM posts",
  );

  if (status !== "open") return <p>status: {status}</p>;
  if (error) return <pre>{error.message}</pre>;
  return (
    <ul>
      {rows.map((p) => (
        <li key={String(p.id)}>
          #{String(p.id)} {p.title}
        </li>
      ))}
    </ul>
  );
}
```

## Row decoders

The wasm side widens Postgres `bigint`/`numeric` columns to JS
`bigint` and `string` respectively to avoid silent precision loss.
If your app prefers plain `number` for small integers, pass a
column-level decoder:

```ts
const sub = await client.subscribe<Post>(sql, {
  decoder: {
    coerceSafeIntegersToNumber: true,
    decoders: {
      created_at: (v) => new Date(String(v)),
    },
  },
});
```

Decoders attached on the client (`PalimpsestClient.connect({ decoder
})`) apply to every subscription; per-subscription decoders override
them.

## Live diffs

The wire protocol delivers a snapshot (`accepted` + initial rows)
followed by live diffs on the same subscription: a committed write in
Postgres flows through the server's WAL cursor pumps and arrives as a
`diff` / `transaction` event, which the hook folds into `rows` by
primary key. No client-side action is needed after a write.

## Driving a host cache with `onDiff`

If your app already owns a cache (TanStack Query, Redux, a custom
store), pass `onDiff` to observe every event and fold it yourself, and
`trackRows: false` so the hook doesn't keep a second copy of the rows:

```ts
usePalimpsestSubscription<Post>(client, sql, {
  trackRows: false, // hook keeps no row map; `rows` stays []
  onDiff: (event) => {
    switch (event.kind) {
      case "accepted":
        queryClient.setQueryData(["posts"], []);
        break;
      case "diff":
      case "transaction":
        queryClient.setQueryData(["posts"], (old: Post[] = []) =>
          applyDiff(old, event),
        );
        break;
      case "resync":
        queryClient.invalidateQueries({ queryKey: ["posts"] });
        break;
    }
  },
});
```

`onDiff` fires for every event (`accepted`, `diff`, `transaction`,
`resync`, `error`) before the hook's own bookkeeping, and the latest
callback identity is always used — you can pass an inline closure
without causing re-subscribes.

## Subpaths

- `@1kbirds/palimpsest-client` — core (`PalimpsestClient`, `TypedSubscription`,
  types). No React dependency.
- `@1kbirds/palimpsest-client/react` — hooks. Pulls `react` as a peer.
