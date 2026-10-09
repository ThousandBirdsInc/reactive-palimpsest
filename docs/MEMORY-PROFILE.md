# Memory Profile: Multi-User, High-Volume Fan-Out

Measured 2026-10-09 on the `palimpsest-loadsuite` scenarios
([`LOAD-TESTING.md`](LOAD-TESTING.md)), release builds, 4 vCPU / 16 GiB
Linux host. Memory is process RSS from `/proc` (the suite's own
`rss_*` fields plus `crates/palimpsest-soak/memprofile.sh` sampling at
250 ms); allocation attribution is valgrind DHAT on a release build
with line tables.

The workload is the suite's model of a large collaborative app:
Zipf-skewed shards (a few hot, most cold), 60/30/10
insert/update/delete with exact retractions, head-heavy transaction
sizes, 10% of subscribers on a global feed and the rest following
popular shards, one consumer task per subscriber acking
periodically. `permission-storm` adds 500 distinct `$user` contexts
with compiled permission plans and rule swaps.

## Headline

Before this profile, fan-out re-paired every transaction once **per
subscriber** and each subscriber's channel held its own copy of every
row image. That was 97% of peak heap and the throughput ceiling. The
router now pairs once per transaction and shares the result
(`Arc<[RowChange]>`) across subscribers. Same scenarios, same host,
before → after:

| Scenario | Subscribers | Peak RSS | Fan-out (deliveries/s) | Achieved writes/s (target) | Commit-to-client p99 |
| --- | --- | --- | --- | --- | --- |
| steady-state | 1,000 | 71 → **29 MiB** | 172k → **1.30M** | 967 → 7,340 (10,000) | 14.5 → 4.9 ms |
| steady-state | 4,000 | 110 → **78 MiB** | 146k → **802k** | 213 → 1,166 (10,000) | 29.8 → 5.5 ms |
| permission-storm (500 users, 5 rule swaps) | 500 | 50 → **25 MiB** | wall 82 → 9.7 s | | 25.6 → 5.2 ms |
| churn (1k population, 2k sub/unsub) | 1,000 | 43 → **30 MiB** | | | 14.7 → 12.8 ms |
| bulk-backfill (8k-row batches) | 1,000 | 69 → **50 MiB** | 121k → 534k | 683 → 3,000 (3,000) | 1,566 → 24.8 ms |
| wal-pipeline (pgoutput decode → fan-out) | 500 | 58 → **19 MiB** | wall 95 → 14.6 s | 525 → 3,476 (3,500) | 7.7 → 5.0 ms |
| slow-consumer (50 throttled) | 1,000 | 107 → **31 MiB** | 157k → 865k | 830 → 4,997 (5,000) | 11.4 → 2.7 ms |

DHAT, steady-state at 200 subscribers / 3,000 transactions:

| | Before | After |
| --- | --- | --- |
| Bytes allocated over the run | 1,650 MB | 78 MB |
| Heap at peak | 104 MB | 6.7 MB |
| Heap at exit | 24 KB | 24 KB |

The baseline 1k-subscriber run overlapped two fuzzing campaigns on the
same host, so its throughput is pessimistic; the 4k run and the five
scenarios in the lower rows ran uncontended and show the same 4–6×
fan-out gain. The memory numbers are not sensitive to contention.

## Scaling with subscribers (baseline, before the fix)

Steady-state, 100k transactions, varying subscriber count:

| Subscribers | RSS after subscribe | Peak RSS | RSS at end | Growth in 2nd half of run |
| --- | --- | --- | --- | --- |
| 1,000 | 11 MiB (7 KiB/sub) | 71 MiB | 54 MiB | +0.2 MiB/min |
| 4,000 | 23 MiB (5 KiB/sub) | 111 MiB | 88 MiB | +0.7 MiB/min |
| 8,000 | 41 MiB (4 KiB/sub) | 179 MiB | 162 MiB | +0.7 MiB/min |
| 1,000, 400k transactions | 11 MiB | 100 MiB | 58 MiB | +0.0 MiB/min |

Memory is linear in subscribers (roughly 20 KiB per subscriber at
steady state on a ~10 MiB base) and flat in transactions: four times
the transactions at 1k subscribers ends at the same RSS, and DHAT
reports 24 KB live at exit. There is no leak. Static per-subscription
cost is ~5–7 KiB (registry record, ack tracker, a 256-slot bounded
channel); the rest is in-flight events waiting in channels.

After the fix the same 1k and 4k runs peak at 29 and 78 MiB, so the
per-subscriber steady-state cost drops to ~17 KiB and the base to
~12 MiB.

## What dominated the heap, and what dominates now

Before (DHAT at peak, 200 subscribers):

- 97%: `Vec<RowChange>` built in `SubscriptionRouter::pump_transaction`
  for each subscriber. The server's canonical pump cloned the raw
  delta per subscriber and `pair_changes` re-ran per subscriber, so a
  transaction touching a shard with N followers was paired N times and
  its row images existed N times, each copy sitting in a channel
  until that consumer drained it.
- 1.3%: tokio mpsc blocks for the per-subscriber channels.
- Below 1%: subscription registry, consumer tasks, harness recorders.

Allocation volume told the same story: 36% `RowChange` vectors (grown
by `push`, so reallocating), 52% `RawDiff` vectors (the per-subscriber
delta clone plus the pairing bins).

After (same run):

- 59%: one `Arc<[RowChange]>` per transaction (`pair_transaction`),
  shared by every subscriber's channel.
- 24%: tokio mpsc channel blocks.
- 6%: channel setup at subscribe time.
- Remaining: harness.

Allocation volume is 21× lower; what is left is the raw-diff
materialization of each transaction (the workload generator and the
pairing bins) and the one shared change vector.

## Harness correction

The first measurement pass showed RSS climbing ~4 MiB per 1,000
transactions with no plateau. That was the load suite itself: every
consumer kept the `LatencyRecorder` default of 256 Ki samples (2 MiB)
before decimating, and a thousand consumers made that the largest
allocation in the process. The per-consumer budget is now 1 Ki
samples; the suite's `rss_*` fields measure the engine. Keep this in
mind when comparing against reports from before this change.

## Operational guidance

- Budget roughly **12 MiB + 20 KiB × subscriptions** at steady state,
  plus headroom for in-flight events: peak is bounded by
  `router.channel_capacity` (256 events) × subscriptions × transaction
  size, and only approached when consumers lag.
- The `slow-consumer` scenario is the one to watch: throttled
  subscribers hold events in their channels until saturation forces a
  `Resync`. Lower `channel_capacity` to cap memory under slow clients
  at the cost of earlier resyncs.
- `bulk-backfill` (8k-row transactions) is the largest single-event
  shape: one `Arc<[RowChange]>` of 8k row images is shared across all
  followers, so memory no longer multiplies with fan-out there either.
- Permission-personalised queries (`$user.*` in the predicate) do not
  share plans across users; `permission-storm` with 500 distinct users
  peaks at 25 MiB, ~50 KiB per user including the compiled plan.
  The `palimpsest_unique_canonical_keys` gauge is the signal that a
  query shape is over-personalised.

## Reproducing

```sh
cargo build --release -p palimpsest-soak --bin palimpsest-loadsuite
export PALIMPSEST_SUITE_JSON=1
for s in steady-state permission-storm churn bulk-backfill wal-pipeline slow-consumer; do
  crates/palimpsest-soak/memprofile.sh "$s" /tmp/memprof -- target/release/palimpsest-loadsuite "$s"
done
PALIMPSEST_SUITE_SUBSCRIBERS=4000 \
  crates/palimpsest-soak/memprofile.sh steady-4k /tmp/memprof -- target/release/palimpsest-loadsuite steady-state
```

For attribution, see the DHAT recipe in
[`LOAD-TESTING.md`](LOAD-TESTING.md#memory-profiling).
