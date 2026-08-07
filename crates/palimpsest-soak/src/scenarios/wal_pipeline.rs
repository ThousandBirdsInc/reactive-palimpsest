// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Full `pgoutput` pipeline scenario.
//!
//! The router-path scenarios inject pre-decoded diffs; this one
//! pushes each transaction through the same byte-level path a real
//! deployment uses: logical events → `pgoutput` frames
//! (`WalGenerator`) → `decode_pgoutput_message` → typed diffs →
//! router fan-out. Traffic spans several tables while subscribers
//! follow only `posts`, so the decoder also pays for events the
//! query discards — as it does in production.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::{
    ConnectionId, QueryTransactionDelta, RawDiff, RouterConfig, RouterError, SubscriptionRouter,
};
use palimpsest_test_harness::{LogicalEvent, WalGenerator};
use palimpsest_wal::{decode_pgoutput_message, Catalog as WalCatalog, Datum, DecodedEvent, RowOp};
use serde::Serialize;
use smallvec::smallvec;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::driver::{spawn_consumer, Consumer, ConsumerStats, RouterCounters};
use crate::fixture::{now_ns, subscribe_pass_through, SNAPSHOT_LSN};
use crate::rng::Rng;
use crate::stats::{LatencyRecorder, LatencySummary};

/// Table id carrying the served query (`posts`).
const POSTS: u32 = 1;

/// WAL-pipeline configuration.
#[derive(Debug, Clone)]
pub struct WalPipelineConfig {
    /// Subscribers, all following `posts`.
    pub subscribers: usize,
    /// Transactions to run through the pipeline.
    pub total_txns: u64,
    /// Pacing (transactions per second); `0` = unpaced.
    pub target_tps: u64,
    /// Number of distinct tables in the WAL stream.
    pub tables: usize,
    /// Percent of row events that land on `posts`.
    pub posts_share_pct: u8,
    /// Max rows per transaction (sizes are uniform in `1..=max`).
    pub max_rows_per_txn: usize,
    /// Bootstrap snapshot rows per subscriber.
    pub snapshot_rows: usize,
    /// Per-subscription channel depth.
    pub channel_capacity: usize,
    /// Master seed.
    pub seed: u64,
}

impl WalPipelineConfig {
    /// Full-scale defaults.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            subscribers: 500,
            total_txns: 50_000,
            target_tps: 5_000,
            tables: 4,
            posts_share_pct: 40,
            max_rows_per_txn: 8,
            snapshot_rows: 64,
            channel_capacity: 256,
            seed: 0x0dec_0de5,
        }
    }

    /// Tiny CI-friendly configuration.
    #[must_use]
    pub const fn smoke() -> Self {
        Self {
            subscribers: 12,
            total_txns: 300,
            target_tps: 0,
            snapshot_rows: 8,
            channel_capacity: 1_024,
            ..Self::full()
        }
    }
}

/// WAL-pipeline outcome.
#[derive(Debug, Clone, Serialize)]
pub struct WalPipelineReport {
    /// Scenario name (`wal-pipeline`).
    pub scenario: String,
    /// Subscribers.
    pub subscribers: usize,
    /// Transactions pushed through the pipeline.
    pub txns_generated: u64,
    /// `pgoutput` frames decoded.
    pub frames_decoded: u64,
    /// `pgoutput` bytes decoded.
    pub bytes_decoded: u64,
    /// Decode-only throughput, frames per second.
    pub decode_frames_per_sec: f64,
    /// Decode-only throughput, megabytes per second.
    pub decode_mb_per_sec: f64,
    /// Row events decoded on non-served tables (discarded by the query).
    pub offquery_rows: u64,
    /// Successful per-subscriber deliveries.
    pub deliveries: u64,
    /// Wall-clock seconds for the pipeline phase.
    pub wall_secs: f64,
    /// Achieved transactions per second.
    pub achieved_tps: f64,
    /// Generate→encode→decode→route→client latency.
    pub end_to_end_latency: LatencySummary,
    /// Pumps dropped by channel saturation.
    pub saturation_drops: u64,
    /// Router counters.
    pub router: RouterCounters,
}

impl fmt::Display for WalPipelineReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "[{}] subs={} txns={} wall={:.2}s tps={:.0} deliveries={}",
            self.scenario,
            self.subscribers,
            self.txns_generated,
            self.wall_secs,
            self.achieved_tps,
            self.deliveries,
        )?;
        writeln!(
            f,
            "  decode: frames={} bytes={} ({:.0} frames/s, {:.2} MB/s) offquery_rows={}",
            self.frames_decoded,
            self.bytes_decoded,
            self.decode_frames_per_sec,
            self.decode_mb_per_sec,
            self.offquery_rows,
        )?;
        write!(
            f,
            "  end-to-end latency: {} (saturation drops={})",
            self.end_to_end_latency, self.saturation_drops
        )
    }
}

/// Per-table live rows tracked as the string tuples `WalGenerator`
/// encodes: `(id, author_id, sent_at_ns)`.
struct TableState {
    live: Vec<(i64, i64)>,
}

fn tuple(id: i64, author: i64, ts: i64) -> Vec<String> {
    vec![id.to_string(), author.to_string(), ts.to_string()]
}

fn parse_i64(datum: &Datum) -> i64 {
    match datum {
        Datum::I64(v) => *v,
        Datum::I32(v) => i64::from(*v),
        Datum::Text(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

/// Runs the WAL-pipeline scenario.
///
/// # Errors
///
/// Returns a description of the first decode or router error.
pub async fn run(cfg: WalPipelineConfig) -> Result<WalPipelineReport, String> {
    let epoch = Instant::now();
    let router = Arc::new(SubscriptionRouter::new(RouterConfig {
        channel_capacity: cfg.channel_capacity,
        ..RouterConfig::default()
    }));

    // Subscribers all follow `posts` (global fan-out).
    let mut handles: Vec<JoinHandle<ConsumerStats>> = Vec::with_capacity(cfg.subscribers);
    let mut expected_txs: Vec<oneshot::Sender<u64>> = Vec::with_capacity(cfg.subscribers);
    let mut subs = Vec::with_capacity(cfg.subscribers);
    for idx in 0..cfg.subscribers {
        let subscribed_at = Instant::now();
        let response = subscribe_pass_through(
            &router,
            ConnectionId::new(idx as u64 + 1),
            &format!("wal-{idx}"),
            cfg.snapshot_rows,
        )?;
        let (tx, rx) = oneshot::channel();
        handles.push(spawn_consumer(Consumer {
            stream: response.stream,
            router: Arc::clone(&router),
            sub: response.subscription_id,
            delay: None,
            ack_every: 16,
            epoch,
            subscribed_at,
            expected: rx,
            stop_on_resync: false,
        }));
        expected_txs.push(tx);
        subs.push(response.subscription_id);
    }
    let mut delivered = vec![0_u64; cfg.subscribers];

    let mut rng = Rng::new(cfg.seed);
    let mut generator = WalGenerator::new();
    let mut wal_catalog = WalCatalog::new();
    let mut tables: Vec<TableState> = (0..cfg.tables.max(1))
        .map(|_| TableState { live: Vec::new() })
        .collect();
    let mut next_id: i64 = 1;

    let mut frames_decoded: u64 = 0;
    let mut bytes_decoded: u64 = 0;
    let mut decode_ns: u64 = 0;
    let mut offquery_rows: u64 = 0;
    let mut deliveries: u64 = 0;
    let mut saturation_drops: u64 = 0;

    let interval = (cfg.target_tps > 0)
        .then(|| Duration::from_nanos(1_000_000_000_u64.checked_div(cfg.target_tps).unwrap_or(1)));
    let started = Instant::now();
    let mut next_tick = started;

    for txn_seq in 0..cfg.total_txns {
        if let Some(interval) = interval {
            next_tick += interval;
            let now = Instant::now();
            if next_tick > now {
                tokio::time::sleep(next_tick - now).await;
            }
        } else if txn_seq % 8 == 0 {
            tokio::task::yield_now().await;
        }

        // --- Generate one transaction's logical events ---------------
        let rows = rng.between(1, cfg.max_rows_per_txn.max(1));
        let mut events = Vec::with_capacity(rows + 2);
        events.push(LogicalEvent::Begin {
            xid: txn_seq as u32 + 1,
        });
        for _ in 0..rows {
            let table_idx = if rng.chance_pct(cfg.posts_share_pct) {
                0
            } else {
                1 + rng.below(tables.len().saturating_sub(1).max(1))
            };
            let table_idx = table_idx.min(tables.len() - 1);
            let table = palimpsest_test_harness::TableId::new(POSTS + table_idx as u32);
            let author = table_idx as i64;
            let ts = now_ns(epoch);
            let state = &mut tables[table_idx];
            let roll = rng.below(100);
            if state.live.is_empty() || roll < 60 {
                let id = next_id;
                next_id += 1;
                state.live.push((id, ts));
                events.push(LogicalEvent::Insert {
                    table,
                    new: tuple(id, author, ts),
                });
            } else if roll < 90 {
                let idx = rng.below(state.live.len());
                let (id, old_ts) = state.live[idx];
                state.live[idx].1 = ts;
                events.push(LogicalEvent::Update {
                    table,
                    old: Some(tuple(id, author, old_ts)),
                    new: tuple(id, author, ts),
                });
            } else {
                let idx = rng.below(state.live.len());
                let (id, old_ts) = state.live.swap_remove(idx);
                events.push(LogicalEvent::Delete {
                    table,
                    old: tuple(id, author, old_ts),
                });
            }
        }
        events.push(LogicalEvent::Commit);

        // --- Encode + decode the pgoutput frames ---------------------
        let frames = generator.encode_pgoutput(&events);
        let decode_started = Instant::now();
        let mut posts_diffs: Vec<RawDiff> = Vec::new();
        let lsn = Lsn::new(SNAPSHOT_LSN + 1 + txn_seq);
        for frame in frames {
            bytes_decoded += frame.len() as u64;
            frames_decoded += 1;
            let event = decode_pgoutput_message(&mut wal_catalog, frame)
                .map_err(|e| format!("decode failed at txn {txn_seq}: {e}"))?;
            match event {
                DecodedEvent::Row {
                    table,
                    op,
                    old,
                    new,
                    ..
                } if table.get() == POSTS => {
                    let to_row = |t: &palimpsest_wal::Tuple| {
                        smallvec![
                            Datum::I64(parse_i64(&t[0])),
                            Datum::I64(parse_i64(&t[1])),
                            Datum::I64(parse_i64(&t[2])),
                        ]
                    };
                    match op {
                        RowOp::Insert => {
                            if let Some(new) = &new {
                                posts_diffs.push(RawDiff {
                                    table: None,
                                    row: to_row(new),
                                    lsn,
                                    diff: 1,
                                });
                            }
                        }
                        RowOp::Update => {
                            if let Some(old) = &old {
                                posts_diffs.push(RawDiff {
                                    table: None,
                                    row: to_row(old),
                                    lsn,
                                    diff: -1,
                                });
                            }
                            if let Some(new) = &new {
                                posts_diffs.push(RawDiff {
                                    table: None,
                                    row: to_row(new),
                                    lsn,
                                    diff: 1,
                                });
                            }
                        }
                        RowOp::Delete => {
                            if let Some(old) = &old {
                                posts_diffs.push(RawDiff {
                                    table: None,
                                    row: to_row(old),
                                    lsn,
                                    diff: -1,
                                });
                            }
                        }
                    }
                }
                DecodedEvent::Row { .. } => offquery_rows += 1,
                _ => {}
            }
        }
        decode_ns += u64::try_from(decode_started.elapsed().as_nanos()).unwrap_or(u64::MAX);

        // --- Route to subscribers ------------------------------------
        if posts_diffs.is_empty() {
            continue;
        }
        for (idx, sub) in subs.iter().enumerate() {
            let delta = QueryTransactionDelta::new(
                Some(txn_seq as u32 + 1),
                None,
                lsn,
                None,
                posts_diffs.clone(),
            );
            match router.pump_transaction(*sub, delta, &[0]) {
                Ok(()) => {
                    delivered[idx] += 1;
                    deliveries += 1;
                }
                Err(RouterError::ChannelSaturated) => saturation_drops += 1,
                Err(other) => return Err(format!("pump failed at txn {txn_seq}: {other}")),
            }
        }
    }
    let wall_secs = started.elapsed().as_secs_f64();

    // --- Drain -----------------------------------------------------------
    for (tx, count) in expected_txs.into_iter().zip(&delivered) {
        let _ = tx.send(*count);
    }
    let mut latency = LatencyRecorder::default();
    for handle in handles {
        let stats = handle
            .await
            .map_err(|e| format!("consumer panicked: {e}"))?;
        latency.merge(&stats.latency);
    }

    let decode_secs = decode_ns as f64 / 1e9;
    let metrics = router.metrics().snapshot();
    Ok(WalPipelineReport {
        scenario: "wal-pipeline".to_owned(),
        subscribers: cfg.subscribers,
        txns_generated: cfg.total_txns,
        frames_decoded,
        bytes_decoded,
        decode_frames_per_sec: frames_decoded as f64 / decode_secs.max(f64::EPSILON),
        decode_mb_per_sec: bytes_decoded as f64 / 1e6 / decode_secs.max(f64::EPSILON),
        offquery_rows,
        deliveries,
        wall_secs,
        achieved_tps: cfg.total_txns as f64 / wall_secs.max(f64::EPSILON),
        end_to_end_latency: latency.summary(),
        saturation_drops,
        router: RouterCounters::from(&metrics),
    })
}
