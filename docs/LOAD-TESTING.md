# Load Testing

Palimpsest ships a load-test suite (`crates/palimpsest-soak`, binary
`palimpsest-loadsuite`) that models realistic large-scale workloads
against an in-process `SubscriptionRouter`, per §15.8 / §18.12 of
[`../DESIGN.md`](../DESIGN.md). It complements the existing
`palimpsest-soak` (72 h random-workload soak) and `palimpsest-load`
(fixed-trace replay) binaries with scenario coverage of the workload
shapes production fleets actually produce.

Every scenario is deterministic under a fixed seed — same seed, same
transaction stream — so latency numbers are comparable across runs
and regressions are attributable.

## Running

```sh
# Everything, full scale (several minutes):
cargo run --release -p palimpsest-soak --bin palimpsest-loadsuite

# One or more scenarios:
cargo run --release -p palimpsest-soak --bin palimpsest-loadsuite -- steady-state churn

# CI-sized:
PALIMPSEST_SUITE_SMOKE=1 cargo run -p palimpsest-soak --bin palimpsest-loadsuite
```

Always use `--release` for numbers you intend to compare; debug
builds shift every percentile.

### Environment knobs

| Variable | Effect |
|---|---|
| `PALIMPSEST_SUITE_SMOKE=1` | Tiny CI-scale configurations. |
| `PALIMPSEST_SUITE_SUBSCRIBERS` | Override subscriber count / target population. |
| `PALIMPSEST_SUITE_TXNS` | Override transaction count (churn ops for `churn`, per-round txns for `permission-storm`). |
| `PALIMPSEST_SUITE_TPS` | Override write pacing; `0` = unpaced (as fast as possible). |
| `PALIMPSEST_SUITE_SEED` | Master seed override. |
| `PALIMPSEST_SUITE_JSON=1` | Emit one JSON report object per scenario (for dashboards). |
| `PALIMPSEST_SUITE_MAX_P99_US` | Fail (non-zero exit) if any scenario's headline p99 exceeds this budget — usable as a CI/nightly gate. |

## Workload model

The write side models a large collaborative app rather than a
uniform firehose:

- **Zipf-skewed shards.** Writes and subscriber interest both target
  "shards" (documents / channels / boards) drawn from a Zipf
  distribution (`s ≈ 1.05`): a few shards are hot, most are cold.
- **Mixed operations with exact retractions.** 60/30/10
  insert/update/delete against each shard's live row set; updates and
  deletes retract the exact previously-emitted row image, matching
  differential set semantics.
- **Head-heavy transaction sizes.** 70% single-row transactions, 25%
  multi-row saves (2–8 rows), 5% large batches (16–64 rows).
- **Subscriber placement.** ~10% of subscribers follow a global feed
  (receive every transaction); the rest follow one Zipf-popular
  shard, so hot shards also have the most followers — fan-out load
  concentrates the way it does in production.
- **Embedded send timestamps.** Every row carries a `sent_at_ns`
  column stamped at generation, so consumers compute commit-to-client
  latency without a shared side-channel.
- **Concurrent consumers.** One task per subscriber drains its
  stream while the producer runs, and consumers ack periodically —
  queueing, backpressure, and ack-tracker costs are all real.

## Scenarios

| Scenario | Models | Key outputs |
|---|---|---|
| `steady-state` | The §15.8 "Figma envelope": 1k subscribers, 10k paced writes/s of mixed traffic. | Commit-to-client p50/p90/p99/p99.9, deliveries/s, per-subscriber RSS. |
| `fanout-burst` | Thundering herd: periodic back-to-back bursts on the hottest (most-subscribed) shard over a steady baseline. | Burst drain latency, saturation containment. |
| `slow-consumer` | A slow population (throttled tabs, mobile radios) draining with per-event think time. | Isolation: healthy-sub latency and zero healthy resyncs while slow channels saturate and resync. |
| `bulk-backfill` | Periodic multi-thousand-row transactions (migration / import) interleaved with interactive traffic. | Interactive tail-latency degradation during bulk windows. |
| `churn` | Continuous subscribe/unsubscribe (tab opens, roaming clients, deploys) under write load, with realistically-sized bootstrap snapshots. | Subscribe-call and subscribe-to-`Initial` latency, registry consistency, stale-pump races. |
| `wal-pipeline` | The full byte path: logical events → `pgoutput` frames → decode → typed diffs → router fan-out, over a multi-table stream where most tables are off-query. | Decode throughput (frames/s, MB/s), end-to-end generate-to-client latency. |
| `permission-storm` | Operator swaps the global permission rule set; every compiled-plan subscription is force-resynced and the whole fleet resubscribes (rewrite + plan compile + snapshot refetch). | Server-side revocation lag, revocation drain time, resubscribe-storm wall clock. |

## Reading the reports

Each scenario prints a human-readable report (or JSON with
`PALIMPSEST_SUITE_JSON=1`). Things worth watching:

- **`deliveries/s`** is the router's real fan-out throughput
  (transactions × matched subscribers). When achieved tps falls
  below the target, the configured rate exceeds what the current
  single-producer pump path sustains — that ceiling is a primary
  number to track release-over-release.
- **`saturation drops` vs `resyncs`.** A saturated channel drops the
  diff and force-sends `Resync(Backpressure)` — but the force-send is
  itself best-effort on a full channel, so slow subscribers can
  saturate many times while observing few resyncs. Healthy
  subscribers must show zero resyncs in every scenario except
  `permission-storm`.
- **Latency segmentation.** `slow-sub latency` is reported separately
  so a deliberately-slow population doesn't pollute the healthy
  percentiles.
- **RSS.** Before/after-subscribe and peak RSS come from
  `/proc/self/status`; per-subscriber cost is the subscribe-phase
  delta divided by the population.

## CI integration

`crates/palimpsest-soak/tests/smoke.rs` runs every scenario at tiny
scale in the normal workspace test run, asserting *structural*
invariants only (delivery accounting matches, slow consumers actually
saturate, healthy subscribers see no resyncs, the registry stays
consistent, every rule swap resyncs the whole fleet) — never absolute
latency, which would flake on shared CI hosts.

For a nightly latency gate, run the suite with a budget, e.g.:

```sh
PALIMPSEST_SUITE_MAX_P99_US=50000 \
  cargo run --release -p palimpsest-soak --bin palimpsest-loadsuite
```
