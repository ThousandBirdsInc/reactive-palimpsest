// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Production-shaped synthetic write workload.
//!
//! Models the write side of a large collaborative app:
//!
//! * writes target **shards** (documents / channels / boards) drawn
//!   from a Zipf distribution — a few shards are hot, most are cold;
//! * transactions mix inserts, updates, and deletes against the
//!   shard's live row set, so retractions carry exact old images;
//! * transaction sizes follow a head-heavy mixture: mostly single-row
//!   writes, some multi-row saves, occasional large batches.

use std::time::Instant;

use palimpsest_dataflow::palimpsest::Lsn;
use palimpsest_server::RawDiff;
use palimpsest_wal::Datum;
use smallvec::smallvec;

use crate::fixture::now_ns;
use crate::rng::Rng;

/// Operation and transaction-size mixture.
#[derive(Debug, Clone, Copy)]
pub struct WorkloadMix {
    /// Percent of row operations that insert a new row.
    pub insert_pct: u8,
    /// Percent of row operations that update a live row.
    pub update_pct: u8,
    /// Percent of transactions touching exactly one row.
    pub single_row_txn_pct: u8,
    /// Percent of transactions in the medium band.
    pub medium_txn_pct: u8,
    /// Inclusive row-count range for medium transactions.
    pub medium_rows: (usize, usize),
    /// Inclusive row-count range for large transactions.
    pub large_rows: (usize, usize),
}

impl Default for WorkloadMix {
    /// OLTP-ish default: 60/30/10 insert/update/delete; 70% single-row
    /// transactions, 25% multi-row saves (2–8 rows), 5% large batches
    /// (16–64 rows).
    fn default() -> Self {
        Self {
            insert_pct: 60,
            update_pct: 30,
            single_row_txn_pct: 70,
            medium_txn_pct: 25,
            medium_rows: (2, 8),
            large_rows: (16, 64),
        }
    }
}

/// Stateful workload generator over `shards` independent row sets.
#[derive(Debug)]
pub struct Workload {
    mix: WorkloadMix,
    /// Per shard: live `(row_id, sent_at_ns)` images, needed so
    /// deletes and updates retract the exact previously-sent row.
    live: Vec<Vec<(i64, i64)>>,
    next_row_id: i64,
}

impl Workload {
    /// Creates an empty workload over `shards` shards.
    #[must_use]
    pub fn new(shards: usize, mix: WorkloadMix) -> Self {
        Self {
            mix,
            live: vec![Vec::new(); shards.max(1)],
            next_row_id: 1,
        }
    }

    /// Samples a transaction row count from the configured mixture.
    pub fn sample_txn_rows(&self, rng: &mut Rng) -> usize {
        let roll = rng.below(100) as u8;
        if roll < self.mix.single_row_txn_pct {
            1
        } else if roll < self.mix.single_row_txn_pct + self.mix.medium_txn_pct {
            rng.between(self.mix.medium_rows.0, self.mix.medium_rows.1)
        } else {
            rng.between(self.mix.large_rows.0, self.mix.large_rows.1)
        }
    }

    /// Generates one transaction of `rows` operations against `shard`,
    /// stamping fresh send-timestamps relative to `epoch`.
    ///
    /// Returned diffs all share `lsn` (the commit LSN) and use exact
    /// old images for retractions, matching differential set
    /// semantics.
    pub fn transaction(
        &mut self,
        rng: &mut Rng,
        shard: usize,
        rows: usize,
        lsn: Lsn,
        epoch: Instant,
    ) -> Vec<RawDiff> {
        let shard = shard % self.live.len();
        let author = shard as i64;
        let mut diffs = Vec::with_capacity(rows * 2);
        for _ in 0..rows {
            let ts = now_ns(epoch);
            let shard_live = &mut self.live[shard];
            let roll = rng.below(100) as u8;
            if shard_live.is_empty() || roll < self.mix.insert_pct {
                let id = self.next_row_id;
                self.next_row_id += 1;
                shard_live.push((id, ts));
                diffs.push(RawDiff {
                    table: None,
                    row: smallvec![Datum::I64(id), Datum::I64(author), Datum::I64(ts)],
                    lsn,
                    diff: 1,
                });
            } else if roll < self.mix.insert_pct + self.mix.update_pct {
                let idx = rng.below(shard_live.len());
                let (id, old_ts) = shard_live[idx];
                shard_live[idx].1 = ts;
                diffs.push(RawDiff {
                    table: None,
                    row: smallvec![Datum::I64(id), Datum::I64(author), Datum::I64(old_ts)],
                    lsn,
                    diff: -1,
                });
                diffs.push(RawDiff {
                    table: None,
                    row: smallvec![Datum::I64(id), Datum::I64(author), Datum::I64(ts)],
                    lsn,
                    diff: 1,
                });
            } else {
                let idx = rng.below(shard_live.len());
                let (id, old_ts) = shard_live.swap_remove(idx);
                diffs.push(RawDiff {
                    table: None,
                    row: smallvec![Datum::I64(id), Datum::I64(author), Datum::I64(old_ts)],
                    lsn,
                    diff: -1,
                });
            }
        }
        diffs
    }

    /// Total live rows across all shards.
    #[must_use]
    pub fn live_rows(&self) -> usize {
        self.live.iter().map(Vec::len).sum()
    }
}
