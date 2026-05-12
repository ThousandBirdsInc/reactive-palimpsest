# Migrating from polling REST to Palimpsest

Most apps that end up reaching for Palimpsest started with a REST
endpoint that the client polls every few seconds — sometimes with
ETags, sometimes with `?since=…`, often without. This guide is the
side-by-side mapping for that transition.

## The shape of the change

```
BEFORE                                   AFTER

  setInterval(() => fetch('/posts'),       client.subscribe(`SELECT … FROM posts`)
             5000)                              .on_snapshot(initial)
                                              .on_diff(applyDiff)
                                              .on_resync(reload)
```

The fetch loop becomes a subscription. Polling cadence becomes the
upstream Postgres commit cadence. Manual diffing on the client
becomes a structured `op: insert | update | delete` event.

## Common REST → Palimpsest mappings

### "Fetch the list, periodically"

```
GET /api/posts            →    SELECT id, title, body, published_at
                                FROM posts
                                WHERE published = true
                                ORDER BY published_at DESC
                                LIMIT 100
```

You can keep your existing pagination cursor on the client; the
subscription delivers the *whole window*, and the client maintains a
sorted prefix.

### "Fetch by ID, when needed"

Don't subscribe per row. Subscribe to the **scope** the page needs and
let the client read from the in-memory result set:

```
GET /api/posts/:id        →    SELECT * FROM posts WHERE author_id = $user.id
```

Then `posts.find(p => p.id === current)` in JS. One subscription,
many lookups.

### "Filter via query string"

```
GET /api/posts?org=42&    →    SELECT id, title
    published=true                  FROM posts
                                    WHERE org_id = $user.org_id
                                      AND published = true
```

Don't pass user identity through query strings — declare `org_id` as a
`$user.*` field and let permission rules enforce the scope. Removes a
class of "forgot the WHERE clause" bugs.

### "Aggregate dashboard endpoint"

```
GET /api/dashboard        →    SELECT org_id,
                                       count(*) AS posts,
                                       count(*) FILTER (WHERE published) AS live
                                FROM posts
                                GROUP BY org_id
```

The aggregate is computed incrementally — the dashboard updates
without polling and without recomputing the whole table. You ship one
SQL query instead of ten REST endpoints.

### "Webhook on change"

If your app currently fires webhooks per row change, those typically
become a server-side subscription with a backing service:

```
ON UPDATE posts → POST     →    let sub = palimpsest.subscribe(
   /webhooks/post-changed                "SELECT id, status FROM posts");
                                  for change in sub.diffs() {
                                      enqueue_webhook(change);
                                  }
```

The subscription replaces the trigger. Backpressure is built in: if
the webhook queue gets behind, the channel saturates, and you receive
a `Resync` rather than silently dropping events.

## Things you don't need to do anymore

- **ETag plumbing.** The protocol is built around LSN watermarks; ack
  the LSN and you're done.
- **Coalescing duplicate fetches.** Two clients with the same SQL
  share state on the server; you can't accidentally double-fetch.
- **Handcrafted "what changed since" diffing on the client.**
- **Cache invalidation by table name.** The dataflow knows what each
  subscription depends on; an update to `authors` invalidates exactly
  the subscriptions that joined `authors`.
- **Polling fallbacks for "missed" updates.** At-least-once + in-order
  + ack is the core guarantee.

## Things that change for you operationally

- **Long-lived gRPC streams.** Configure idle timeouts on your proxy
  to a generous value (60+ seconds). Many ingress configs default to
  closing idle streams after 10s, which causes spurious resyncs.
- **Backpressure surfaces as `Resync`.** If a client falls far enough
  behind that the per-subscription channel saturates, the server
  emits a `Resync` rather than buffer unboundedly. Treat it like a
  cache invalidation: clear local state, wait for the next snapshot.
- **Permissions move from middleware to rules.** You no longer write
  `if (req.user.org_id !== row.org_id) reject()` per endpoint — you
  write one TOML rule and Palimpsest folds it into every query.
- **Schema changes need a publication update.** Adding a column to a
  table requires updating the Postgres `PUBLICATION` if you want
  Palimpsest to see it. Removing a column is automatic.

## What still belongs as REST

Palimpsest is read-only. Writes still go through your existing API:

- `POST /api/posts` (creating a row) → still REST.
- `PUT /api/posts/:id` (updating a row) → still REST.
- `DELETE /api/posts/:id` → still REST.

The flow is: client writes through your write API, Postgres commits,
Palimpsest sees the commit on its replication stream, every relevant
subscription gets the diff. Average end-to-end latency from "POST
returned" to "subscription emits the new row" is the upstream WAL
flush latency plus a single dataflow step — typically <50ms in
healthy deployments.

## Migration ordering

1. Pick one read-heavy endpoint. (List endpoints are best.)
2. Stand up Palimpsest in front of the same Postgres.
3. Add the SQL surface to your client behind a feature flag.
4. Run both for a release; compare divergences.
5. Cut over. Decommission the REST endpoint.
6. Repeat.

Don't rewrite everything at once. The wins compound — every endpoint
you migrate makes the next one easier because the
client-side subscription primitives are already in place.
