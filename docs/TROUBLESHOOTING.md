# Troubleshooting Matrix

Symptom → metric → action. Use this when the system is misbehaving and
you need to triage quickly. For deep-dive recovery procedures, see
[`RUNBOOK.md`](RUNBOOK.md).

## Server-side symptoms

| Symptom | Metric to check | Likely cause | Action |
| --- | --- | --- | --- |
| Subscriptions feel "stuck" — last diff was minutes ago. | `palimpsest_wal_lag_bytes` climbing | Replication slot stalled (slow consumer or stuck slot). | [`RUNBOOK.md`](RUNBOOK.md) §1 — restart the slot; if Postgres still shows `active = true` after the server died, run `pg_terminate_backend` on the orphan walsender. |
| Many clients receiving `Resync`. | `palimpsest_resyncs_by_reason_total{reason="channel_saturation"}` spiking | At least one downstream is too slow; the channel filled. | [`RUNBOOK.md`](RUNBOOK.md) §2 — bound diffs/sec per subscription, raise `router.channel_capacity`, or rate-limit the upstream. |
| `Resync` with reason `slot_recreated`. | `palimpsest_resyncs_by_reason_total{reason="slot_recreated"}` non-zero | Slot was lost (Postgres restart, manual `pg_drop_replication_slot`, or in-process slot rebuild). | Confirm the slot exists; if intentional, expect a one-shot resync wave. If unintentional, see [`RUNBOOK.md`](RUNBOOK.md) §1. |
| Steady stream of `permission_denied` errors. | `palimpsest_subscribe_rejected_total{reason="permissions"}` | Recent rule edit tightened a predicate further than intended, or `UserContext` shape changed. | Diff `permissions.toml`; verify clients are sending the expected `$user.*` fields. |
| Memory grows linearly with subscriber count, not query count. | `palimpsest_unique_canonical_keys` matches subscriber count | Queries are over-personalised — `$user.id` directly in the WHERE clause without a sharing dimension. | Move per-user predicates into permission rules instead of inline; clients with the same `org_id` should share canonical keys. |
| `subscribe` calls returning `rate_limited` from one IP. | `palimpsest_subscribe_rejected_total{code="rate_limited"}` per-IP | Reconnect storm or hostile client. | If legitimate (mobile fleet, etc.) raise `reconnect_rate.max_attempts`. If hostile, block at the proxy. |
| `subscribe` calls returning `connection_saturated`. | `palimpsest_subscribe_rejected_total{code="connection_saturated"}` | Client opening more subscriptions than `max_subscriptions_per_connection`. | Either raise the limit or have the client multiplex through fewer connections. |
| `subscribe` returning `query_too_large` / `query_too_complex`. | `palimpsest_subscribe_rejected_total{code="query_too_*"}` | Generated SQL exceeds 64 KiB / 256 MIR nodes. | Rewrite the query; if legitimately needed, raise `query_limits.max_input_bytes` / `max_mir_nodes`. |
| gRPC stream closes immediately after subscribe. | tonic logs at INFO; `palimpsest_grpc_errors_total{code="unauthenticated"}` | JWT missing / expired / wrong audience / malformed. | Check token issuance; default `exp` leeway is 60s. |
| Server CPU pegged but throughput low. | `palimpsest_dataflow_compaction_seconds` high | Compaction can't keep up — typically late-acking clients hold back the frontier. | Identify slow ackers via `palimpsest_subscription_ack_lag_seconds`; nudge or drop them. |

## Client-side symptoms

| Symptom | Metric / signal | Likely cause | Action |
| --- | --- | --- | --- |
| Connect succeeds but subscribe never returns `Accepted`. | gRPC keep-alive timing out | Proxy idle timeout shorter than first-snapshot latency. | Raise the proxy idle timeout (60s+) and the gRPC keepalive on the client. |
| Diffs stop arriving but no `Resync`. | last received diff timestamp old | Network blip; gRPC stream wedged. | Restart the subscription; pass `resume_from_lsn = last_acked` to skip the snapshot. |
| Snapshot batch much larger than expected. | size of `on_snapshot` payload | Permission rule didn't bind, or `UserContext` was empty. | Inspect the request — confirm `$user.*` fields are populated. |
| Numbers match REST/SQL one moment and diverge the next. | `palimpsest_subscription_ack_lag_seconds` climbing for this sub | Client isn't acking; server can't compact for this slot. | Confirm `ack(lsn)` is being called after every `on_diff`. |
| WASM client: `Failed to fetch` on connect. | browser devtools network tab | Mixed content (HTTPS page, HTTP server) or CORS missing. | Serve via TLS; add `Access-Control-Allow-Origin` on the proxy. |
| Browser tab shows `Resync` looping. | `on_resync` fires repeatedly | Tab too slow to drain diffs (rendering on every event). | Batch UI updates (e.g. `requestAnimationFrame`); subscribe to a smaller scope. |

## "I changed permissions" diagnostic flow

1. `palimpsest_permission_rule_version` ticked up? If not, the server
   didn't pick up the file.
2. Run `palimpsest validate-permissions <file>` (CLI subcommand) — it
   reproduces the same compile-time errors the server raises.
3. Check `palimpsest_subscribe_rejected_total{reason="permissions"}` —
   any sudden surge in the minute after the change is the new rule
   biting.
4. Roll back to the previous file if needed; rule changes need a
   server restart in v1, but the previous compiled rule set is held
   in `permissions.toml.bak` (operator convention, not enforced).

## "Postgres looks fine but Palimpsest is unhappy"

1. `pg_replication_slots` — slot exists, `active = true`,
   `confirmed_flush_lsn` advancing.
2. `pg_publication_tables` — every table the server's catalog expects
   is in the publication.
3. `pg_stat_replication` — Palimpsest's walsender is connected,
   `state = streaming`, `sync_state = async`.

If all three are healthy and the server still reports lag, the
problem is downstream — usually channel saturation; see
[`RUNBOOK.md`](RUNBOOK.md) §2.

## When to file a bug

If a symptom doesn't map to anything in this table, or the suggested
action doesn't resolve it, capture:

- The full output of `/metrics` (or at least the relevant counters).
- The relevant timeframe in the server logs (info level).
- The SQL of any subscriptions involved.
- The Postgres version and the output of `pg_replication_slots`.

File at the project's issue tracker with that bundle attached. The
core team's first ask will be those four artifacts, so you save a
round-trip by including them up front.
