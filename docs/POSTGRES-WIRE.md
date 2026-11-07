# Postgres wire protocol

`palimpsest-server` can expose its live queries over the Postgres wire
protocol. Any standard Postgres driver (`psql`, JDBC, psycopg,
`tokio-postgres`, `pgx`, ...) connects to the listener, runs a `SELECT`,
and gets the rows of the materialized view the dataflow maintains for
that query. This is the integration path for other backend services:
Palimpsest acts as a high-performance materialized view in front of the
database, and the service keeps using the driver and connection pool it
already has.

The gRPC/WebSocket transports push diffs; this one answers a statement
with the current view. The views are the same: a query executed over
the wire shares the dataflow plan, the permission rewrite and the
cursor pump with every gRPC subscriber of the same canonical query.

## Enabling it

```toml
[pgwire]
addr = "0.0.0.0:5433"
```

Omit the section to leave the listener off. The container image and
the Helm chart expose port `5433` by default (`service.pgwirePort`).

TLS is not terminated here, as with the gRPC listener: SSL and GSSAPI
startup probes are answered with "not supported" so drivers fall back
to plaintext, and production deployments should front the port with a
proxy (see [TLS.md](TLS.md)) or keep it on a private network.

## Connecting

```sh
psql "host=palimpsest port=5433 user=app sslmode=disable"
```

The `user` and `dbname` startup parameters are accepted but carry no
meaning. Authentication follows the server's `[auth]` section:

| `[auth] kind` | Driver side |
| --- | --- |
| `anonymous` | No password prompt. Every connection runs with an empty user context. |
| `jwt` | The server asks for a cleartext password; send the bearer token (the JWT) as the password. Claims map onto `$user.*` exactly as over gRPC. A missing or invalid token fails with SQLSTATE `28P01`. |

Because the password travels in cleartext, a JWT deployment needs the
proxy-terminated TLS described above or a trusted network.

## How a statement is answered

The first execution of a statement on a connection subscribes to it
the way a gRPC subscribe would: the SQL is parsed and lowered, the
permission rules for the connection's user are folded in, the plan is
registered with the shared dataflow host (or attached to an already
registered identical plan) and the snapshot is taken. The rows of that
snapshot are the result.

The subscription is then kept as an *anchor* for the rest of the
connection. The anchor keeps the plan registered and its cursor pump
folding WAL diffs into the cached view, so the next execution of the
same statement on that connection reads the view directly and answers
without a snapshot or a compile. The view is as current as the last
WAL diff the pump folded in (the pump polls the cursor every 50 ms).

Consequences worth knowing:

- **Repeat the same text.** The anchor is keyed by the statement text
  after the parameters are bound, so a prepared statement executed with
  the same values hits the cache; different values are different
  views, each anchored.
- **Anchors are bounded.** A connection holds at most
  `max_subscriptions_per_connection` anchors (the same security limit
  that bounds gRPC subscriptions). The oldest is evicted when the cap
  is reached; a later execution of an evicted statement re-subscribes.
- **Disconnecting releases everything.** All anchors are released and
  the plan is dropped once no subscriber over any transport holds it.
- **Pooled connections amortize the work.** A connection pool that
  runs the same handful of statements keeps those views warm; a
  service that opens a fresh connection per request pays the subscribe
  each time.

## Supported statements

- `SELECT` in the SQL subset documented in
  [supported-sql.md](supported-sql.md), including `$1`-style
  parameters through the extended protocol.
- `EXECUTE name(args)` runs a registered named query
  ([NAMED-QUERIES.md](NAMED-QUERIES.md)) with literal arguments. On a
  server with `[queries]` files and `inline_sql = false`, this is the
  only way to query, as over gRPC (inline SQL fails with `42501`).
- `SET ...`, `BEGIN`, `COMMIT`, `ROLLBACK`, `DISCARD ...` are accepted as
  no-ops so driver and pool connect-time setup succeeds. Transaction
  status is reported (`BEGIN` moves the session to "in transaction");
  nothing is actually transactional.
- `SHOW <parameter>` answers the parameters drivers probe
  (`server_version`, `transaction_isolation`, `transaction_read_only`,
  `timezone`, `client_encoding`, `search_path`, `session_authorization`
  and a few more); unknown parameters fail with `42704`.
- Empty statements and multi-statement simple queries (`a; b`) are
  handled per the protocol.

`INSERT`, `UPDATE`, `DELETE` and DDL fail with `25006` (read-only).
`PREPARE` / `DEALLOCATE` as SQL fail with `0A000`; use the driver's
prepared statements (the extended protocol) instead. `COPY`, cursors
(`DECLARE`/`FETCH`), `LISTEN` and catalog introspection (`pg_catalog`)
are not provided, so GUI tools that browse the schema will show an
empty catalog while still running queries.

Errors carry Postgres SQLSTATEs (`42601` syntax, `42P01` unknown
relation, `42703` unknown column, `42501` permission, `0A000`
unsupported construct, `54000` query too large or deep, `57P01` server
shutdown) and never poison the connection: the session returns to
ready after `Sync`, as a real server does.

## Protocol coverage

- Startup with protocol 3.0; SSL and GSSAPI requests answered `N`.
- `AuthenticationOk` or `AuthenticationCleartextPassword`, then the
  usual `ParameterStatus` set (`server_version 16.0`,
  `client_encoding UTF8`, `DateStyle ISO, MDY`, `integer_datetimes on`,
  `TimeZone UTC`, `default_transaction_read_only on`),
  `BackendKeyData` and `ReadyForQuery`.
- Simple query cycle (`Query`).
- Extended query cycle: `Parse`, `Bind`, `Describe` (statement and
  portal), `Execute` with row limits (`PortalSuspended` and resume),
  `Close`, `Flush`, `Sync`, `Terminate`. Named and unnamed statements
  and portals.
- Text and binary formats for parameters and results, chosen per column
  by the client's format codes.
- `CancelRequest` is accepted and closes that connection without
  cancelling anything; statements complete quickly enough that
  cancellation is not needed.

## Types

Result columns are described with the Postgres type OID matching the
view's declared column type, so drivers decode them natively:

| View column type | Wire type |
| --- | --- |
| bool | `bool` |
| int (any width) | `int8` |
| float (any width) | `float8` |
| numeric | `numeric` |
| text | `text` |
| bytea | `bytea` |
| timestamp / timestamptz | `timestamp` / `timestamptz` (microseconds, UTC) |
| date / time / interval | `date` / `time` / `interval` |
| uuid | `uuid` |
| jsonb | `jsonb` |
| array of T | `T[]` |

Parameters (`$1` ...) are typed from the query, the way the named-query
registry types them: `ParameterDescription` reports the OID, and a
client that declares its own parameter OIDs is decoded by what it
declared. Postgres' extended-query rule that an untyped parameter is
`unknown` is honoured: text is parsed into the inferred type.

NULLs are sent as NULL; every output column is declared nullable since
aggregates and outer joins can produce them.

## Example: a service reading a live aggregate

```rust
let (client, connection) = tokio_postgres::connect(
    "host=palimpsest port=5433 user=orders-svc password=<jwt> sslmode=disable",
    NoTls,
).await?;
tokio::spawn(connection);

// Prepared once; each execution reads the materialized view.
let stmt = client
    .prepare("SELECT category_id, SUM(value) AS total FROM events \
              WHERE active = $1 GROUP BY category_id")
    .await?;
loop {
    let rows = client.query(&stmt, &[&true]).await?;
    for row in &rows {
        let category: i64 = row.get("category_id");
        let total: i64 = row.get("total");
        // ...
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
}
```

The same statement over gRPC would stream each change instead of
polling; use that when the consumer can hold a stream, and the wire
protocol when it cannot.

## Observability

Wire-protocol subscriptions appear in the existing metrics
(`palimpsest_subscriptions_in_flight`, `palimpsest_subscriptions_total`
and friends) alongside gRPC ones, and their client subscription ids
are prefixed `pgwire-`. Connections
are counted against the same reconnect tracker and
`max_subscriptions_per_connection` limit as gRPC connections, and an
admin shutdown closes them with `57P01` after answering in-flight work.
