# Palimpsest operations runbook

This runbook covers the three operational failure modes oncall is most
likely to hit (§18.14). It assumes the standard deployment shape:
upstream Postgres, one or more `palimpsest serve` replicas, and the
metrics sidecar reachable on port 9090.

For each scenario: how to detect it, what's actually happening, and the
recovery procedure. Commands assume the `palimpsest` CLI is on `$PATH`
(see the multi-stage Dockerfile in the repo root) and that `psql` is
available on a jump host with a superuser-equivalent role on the
upstream.

## 1. Slot stuck (`pg_replication_slots.active = false`)

### What you'll see

- `palimpsest_wal_lag_bytes` flatlines, then climbs without bound.
- `pg_xlogdump` / `SELECT * FROM pg_replication_slots WHERE slot_name =
  '<slot>'` shows `active = false` and `confirmed_flush_lsn` no longer
  advances.
- `palimpsest_resyncs_by_reason_total{reason="slot_recreated"}` may
  start ticking once the server gives up and resnapshots subscriptions.

### What's happening

The replication slot is no longer being consumed. Possible causes:

1. The server crashed or was killed mid-stream and the slot is still
   marked active by Postgres' deadlock-avoidance window — Postgres
   eventually clears `active` on its own.
2. A long-running consumer (or the server itself) holds the slot but
   isn't draining records, so Postgres can't recycle WAL.
3. The slot was created, then the publication that backed it was
   dropped — the server can no longer decode anything.

The dangerous failure mode is #2 + #3 combined: WAL piles up under the
slot until the upstream's `pg_wal` partition fills.

### Recovery

1. **Confirm the slot is not actually being read.** From a psql
   session against the upstream:
   ```sql
   SELECT slot_name, active, active_pid, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots WHERE slot_name = 'palimpsest';
   ```
   If `active_pid` is set and the matching backend is alive (`SELECT *
   FROM pg_stat_activity WHERE pid = <pid>`), you have a slow consumer,
   not a stuck slot — see §2 (slow consumer) instead.

2. **Drop and recreate the slot.** From the same psql:
   ```sql
   SELECT pg_drop_replication_slot('palimpsest');
   SELECT pg_create_logical_replication_slot('palimpsest', 'pgoutput');
   ```
   Drop+create resets `restart_lsn` to the current WAL position. **All
   live subscriptions will receive a `Resync{reason: SlotRecreated}`
   event** and will resnapshot.

3. **Restart the server.** This forces every subscription to take a
   fresh snapshot at the new restart LSN:
   ```sh
   kubectl -n palimpsest rollout restart deploy/palimpsest
   ```

4. **Verify recovery.** Watch:
   ```promql
   palimpsest_wal_lag_bytes
   palimpsest_resyncs_by_reason_total{reason="slot_recreated"}
   ```
   Lag should drop within minutes. The slot-recreated counter rises
   once during the rollout — that's the cohort of subscriptions that
   was draining when the slot died. Sustained increase past the
   rollout means clients are reconnecting in a tight loop; see §2.

## 2. Slow consumer (channel saturation)

### What you'll see

- `palimpsest_channel_full_events_total` rises monotonically.
- `palimpsest_resyncs_by_reason_total{reason="backpressure"}` advances.
- `palimpsest_channel_depth_max` sits at or near
  [`BoundedDiffChannel::DEFAULT_CAPACITY`][bdc] (256).
- gRPC clients report repeated `Resync(Backpressure)` messages on
  particular subscriptions.

[bdc]: ../crates/palimpsest-server/src/backpressure.rs

### What's happening

A specific subscription's channel filled because the client cannot
read fast enough (network or CPU bound on the consumer side, or the
client is stuck). The router emitted
`Resync{reason: Backpressure}` instead of dropping diffs silently
— the contract from §15.6 is preserved, but the client must take a
fresh snapshot.

### Recovery

1. **Identify the noisy subscription.** Backpressure is typically
   localized to a small number of clients. Cross-reference logs (the
   server logs the `client_subscription_id` whenever a forwarder
   exits) with `palimpsest_subscriptions_in_flight` / per-connection
   IDs in the access log.

2. **If it's a single client/tenant looping:** rate-limit them or
   ask product to push them onto pagination instead of a live
   subscription. The router does not currently per-tenant rate-limit;
   that's a backlog item.

3. **If many clients are affected:** increase `RouterConfig`'s
   `channel_capacity` (or override the per-client capacity in the
   config TOML once that field lands) and roll a new release. Default
   is 256; doubling to 512 or 1024 is safe and roughly linear in
   memory cost (each slot is a small `DiffEvent`).

4. **If the upstream itself is slow** (lag growing across the board,
   not just one subscription): see §1 first; backpressure may just be
   the symptom.

## 3. Schema drift

### What you'll see

- `palimpsest_resyncs_by_reason_total{reason="schema_changed"}` ticks.
- Server logs include `permission_filter_drop` or schema-validation
  warnings.
- Clients report `Resync{reason: SchemaChanged}` followed by
  decode-error spikes if they didn't refresh their `SchemaDefinition`.

### What's happening

Either:
- A DDL ran on the upstream (e.g. `ALTER TABLE … ADD COLUMN …`) and
  the new column shape no longer matches the cached
  `SchemaDefinition` the server handed out at subscribe time.
- The compiled permission rule set was reloaded with a shape change
  that affects existing predicates.

The router proactively emits `Resync{reason: SchemaChanged}` when it
detects a mismatch; well-behaved clients refetch the schema (the
`Accepted` message at re-subscribe time carries the new
`SchemaDefinition`) and continue.

### Recovery

1. **Confirm the DDL.** From the upstream:
   ```sql
   SELECT * FROM pg_stat_activity WHERE state != 'idle';
   SELECT now() - pg_last_xact_replay_timestamp();
   ```
   For recent DDL specifically, check the schema-change audit log
   (or `pg_stat_user_tables.last_autovacuum` as a coarse signal).

2. **If clients support automatic re-subscribe** (the official
   Palimpsest client does — re-subscribe on `Resync(SchemaChanged)`
   is the SDK default), no operator action is required. Confirm the
   resync counter plateaus within ~1 minute.

3. **If clients are old / static**, you'll see decode errors continue.
   Either pin the server to the old schema (revert the DDL), or
   coordinate a client release — the schema is part of the wire
   contract.

4. **If the schema change was unintended** (a developer ran a DDL on
   prod accidentally), revert the DDL on the upstream **before** any
   write traffic touches the new shape; otherwise you'll need a
   full snapshot resync of every subscription, which the slot-stuck
   procedure (§1) is the heavy hammer for.

## Reference: useful queries

```sql
-- How far behind is the slot?
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
       AS behind
FROM pg_replication_slots;

-- Current connections grouped by application_name.
SELECT application_name, count(*)
FROM pg_stat_activity
GROUP BY 1 ORDER BY 2 DESC;
```

```promql
# Lag in bytes (raw); spikes correlate with §1.
palimpsest_wal_lag_bytes

# Saturation rate per minute; spikes correlate with §2.
rate(palimpsest_channel_full_events_total[1m])

# Resync breakdown; useful to triage 1 vs 2 vs 3.
sum by (reason) (rate(palimpsest_resyncs_by_reason_total[5m]))
```
