// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Optimistic-mutation ledger and rebase engine.
//!
//! The local database holds the *visible* state: the server's
//! authoritative permissioned subset with unconfirmed optimistic
//! mutations replayed on top. The reconciler keeps just enough shadow
//! state to rebuild that view incrementally:
//!
//! * `base` — the authoritative row image, tracked **only** for keys
//!   with pending mutations (so shadow memory is `O(pending writes)`,
//!   never `O(table)`).
//! * `pending` — ordered unconfirmed mutations per table.
//!
//! When a server change arrives for a key with pending mutations, the
//! base image is updated, mutations the change *confirms* are settled,
//! and the survivors are replayed over the new base — producing the
//! minimal set of local writes. Keys without pending mutations pass
//! through untouched, so steady-state sync does no extra work.
//!
//! Settlement policy:
//! * A server change whose post-image agrees with a pending mutation's
//!   written columns **settles** it (the write round-tripped through
//!   the WAL).
//! * A non-matching change for the key while the remote write is still
//!   in flight **rebases** the mutation on top (concurrent writer
//!   elsewhere; ours is still coming).
//! * A non-matching change after the remote write was acknowledged
//!   **conflicts**: the server won, the optimistic overlay for that
//!   mutation is dropped and the caller is told.
//! * A failed remote write **rolls back**: the mutation is removed and
//!   the key is rebuilt from base + surviving mutations.

// `local::mod` re-exports selectively; the ledger internals stay
// crate-private on purpose.
#![allow(clippy::redundant_pub_crate)]

use std::collections::{HashMap, HashSet};

use palimpsest_proto::palimpsest::sync::v1::DiffOp;
use palimpsest_proto::wire::{WireDatum, WireRow, WireRowChange};

use crate::cache::PrimaryKey;

use super::db::LocalWrite;

/// Opaque handle identifying one optimistic mutation.
pub type MutationToken = u64;

/// A staged optimistic mutation, resolved to column indices.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ResolvedKind {
    /// Full-row insert; `set_columns` are the columns the caller
    /// actually provided (settlement compares only those, so
    /// server-side defaults/triggers don't block confirmation).
    Insert {
        row: WireRow,
        set_columns: Vec<usize>,
    },
    /// Partial update of an existing row.
    Update { set: Vec<(usize, WireDatum)> },
    /// Delete by key.
    Delete,
}

#[derive(Debug)]
struct PendingOp {
    token: MutationToken,
    key: PrimaryKey,
    kind: ResolvedKind,
    /// The remote writer reported success; the next server change for
    /// this key either confirms it or wins as a conflict.
    writer_done: bool,
}

#[derive(Debug, Default)]
struct TableLedger {
    /// Authoritative row image per key **with pending mutations only**.
    base: HashMap<PrimaryKey, Option<WireRow>>,
    /// Unconfirmed mutations in application order.
    pending: Vec<PendingOp>,
}

impl TableLedger {
    fn replay(&self, key: &PrimaryKey) -> Option<WireRow> {
        let mut row = self.base.get(key).cloned().flatten();
        for op in self.pending.iter().filter(|op| &op.key == key) {
            row = apply_kind(row, &op.kind);
        }
        row
    }

    fn has_pending(&self, key: &PrimaryKey) -> bool {
        self.pending.iter().any(|op| &op.key == key)
    }

    /// Emit the local write that makes the visible row for `key` match
    /// the ledger, dropping base tracking when nothing is pending.
    fn write_for(&mut self, key: &PrimaryKey) -> LocalWrite {
        let visible = self.replay(key);
        if !self.has_pending(key) {
            self.base.remove(key);
        }
        match visible {
            Some(row) => LocalWrite::Upsert { row },
            None => LocalWrite::Delete { key: key.clone() },
        }
    }
}

fn apply_kind(row: Option<WireRow>, kind: &ResolvedKind) -> Option<WireRow> {
    match kind {
        ResolvedKind::Insert { row: new, .. } => Some(new.clone()),
        ResolvedKind::Update { set } => row.map(|mut r| {
            for (idx, value) in set {
                if let Some(slot) = r.get_mut(*idx) {
                    *slot = value.clone();
                }
            }
            r
        }),
        ResolvedKind::Delete => None,
    }
}

/// Does this server post-image confirm the pending mutation?
fn confirms(kind: &ResolvedKind, new_row: Option<&WireRow>) -> bool {
    match kind {
        ResolvedKind::Insert { row, set_columns } => {
            new_row.is_some_and(|new| set_columns.iter().all(|idx| new.get(*idx) == row.get(*idx)))
        }
        ResolvedKind::Update { set } => {
            new_row.is_some_and(|new| set.iter().all(|(idx, value)| new.get(*idx) == Some(value)))
        }
        ResolvedKind::Delete => new_row.is_none(),
    }
}

/// Result of folding one server transaction into the ledger.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ServerApply {
    /// Local writes that bring the visible state up to date.
    pub writes: Vec<LocalWrite>,
    /// Mutations confirmed by this transaction.
    pub settled: Vec<MutationToken>,
    /// Mutations dropped because the server change disagreed after the
    /// remote write had already been acknowledged.
    pub conflicted: Vec<MutationToken>,
}

/// Result of a remote-write completion.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct WriterApply {
    /// Rollback writes (only on failure).
    pub writes: Vec<LocalWrite>,
    /// Table the mutation targeted, when it was still pending.
    pub table: Option<String>,
}

/// Per-replica reconciliation state across all mirrored tables.
#[derive(Debug, Default)]
pub(crate) struct Reconciler {
    tables: HashMap<String, TableLedger>,
}

impl Reconciler {
    /// Stage an optimistic mutation.
    ///
    /// `current_local_row` is the visible row for `key` *before* this
    /// mutation; when the key has no pending mutations it doubles as
    /// the authoritative base image to capture.
    pub(crate) fn stage(
        &mut self,
        table: &str,
        token: MutationToken,
        key: PrimaryKey,
        kind: ResolvedKind,
        current_local_row: Option<WireRow>,
    ) -> Vec<LocalWrite> {
        let ledger = self.tables.entry(table.to_owned()).or_default();
        if !ledger.has_pending(&key) {
            ledger.base.insert(key.clone(), current_local_row);
        }
        ledger.pending.push(PendingOp {
            token,
            key: key.clone(),
            kind,
            writer_done: false,
        });
        vec![ledger.write_for(&key)]
    }

    /// Fold one committed server transaction into the ledger, returning
    /// the local writes plus settlement outcomes.
    pub(crate) fn apply_server_changes(
        &mut self,
        table: &str,
        changes: &[WireRowChange],
        key_of: impl Fn(&WireRow) -> PrimaryKey,
    ) -> ServerApply {
        let mut out = ServerApply::default();
        let ledger = self.tables.entry(table.to_owned()).or_default();
        for change in changes {
            let old_key = change.old.as_ref().map(&key_of);
            let new_key = change.new.as_ref().map(&key_of);
            // A primary-key-changing update is a delete of the old key
            // plus an insert of the new one.
            if let (Some(old_key), Some(new_key)) = (&old_key, &new_key) {
                if old_key != new_key {
                    fold_change(ledger, old_key, None, &mut out);
                    fold_change(ledger, new_key, change.new.as_ref(), &mut out);
                    continue;
                }
            }
            let (key, post_image) = match change.op {
                DiffOp::Delete => (old_key.or(new_key), None),
                _ => match (&new_key, &old_key) {
                    (Some(k), _) => (Some(k.clone()), change.new.as_ref()),
                    (None, Some(k)) => (Some(k.clone()), None),
                    (None, None) => (None, None),
                },
            };
            let Some(key) = key else { continue };
            fold_change(ledger, &key, post_image, &mut out);
        }
        out
    }

    /// Fold a fresh full snapshot for `table` into the ledger and
    /// return the complete visible row set to install locally.
    pub(crate) fn apply_initial(
        &mut self,
        table: &str,
        rows: Vec<WireRow>,
        key_of: impl Fn(&WireRow) -> PrimaryKey,
    ) -> Vec<WireRow> {
        let ledger = self.tables.entry(table.to_owned()).or_default();
        let pending_keys: HashSet<PrimaryKey> =
            ledger.pending.iter().map(|op| op.key.clone()).collect();
        // Reset base images from the snapshot: any tracked key not in
        // the snapshot has an authoritative image of "absent".
        ledger.base = pending_keys.iter().map(|k| (k.clone(), None)).collect();
        let mut visible = Vec::with_capacity(rows.len());
        for row in rows {
            let key = key_of(&row);
            if pending_keys.contains(&key) {
                ledger.base.insert(key, Some(row));
            } else {
                visible.push(row);
            }
        }
        for key in &pending_keys {
            if let Some(row) = ledger.replay(key) {
                visible.push(row);
            }
        }
        visible
    }

    /// Record the remote writer's outcome for one mutation.
    ///
    /// On success the mutation stays pending until the WAL round-trip
    /// confirms it. On failure it is rolled back and the visible state
    /// for its key rebuilt.
    pub(crate) fn writer_finished(&mut self, token: MutationToken, ok: bool) -> WriterApply {
        for (name, ledger) in &mut self.tables {
            let Some(idx) = ledger.pending.iter().position(|op| op.token == token) else {
                continue;
            };
            if ok {
                ledger.pending[idx].writer_done = true;
                return WriterApply {
                    writes: Vec::new(),
                    table: Some(name.clone()),
                };
            }
            let op = ledger.pending.remove(idx);
            return WriterApply {
                writes: vec![ledger.write_for(&op.key)],
                table: Some(name.clone()),
            };
        }
        WriterApply::default()
    }

    /// Number of unconfirmed mutations for `table`.
    pub(crate) fn pending_count(&self, table: &str) -> usize {
        self.tables.get(table).map_or(0, |l| l.pending.len())
    }
}

/// Fold one (key, post-image) server change into the ledger.
fn fold_change(
    ledger: &mut TableLedger,
    key: &PrimaryKey,
    post_image: Option<&WireRow>,
    out: &mut ServerApply,
) {
    if !ledger.has_pending(key) {
        // Fast path: no optimistic overlay for this key.
        match post_image {
            Some(row) => out.writes.push(LocalWrite::Upsert { row: row.clone() }),
            None => out.writes.push(LocalWrite::Delete { key: key.clone() }),
        }
        return;
    }
    ledger.base.insert(key.clone(), post_image.cloned());
    // Settle confirmations / drop acknowledged-but-contradicted
    // mutations, oldest first, until one survives.
    loop {
        let Some(idx) = ledger.pending.iter().position(|op| &op.key == key) else {
            break;
        };
        let op = &ledger.pending[idx];
        if confirms(&op.kind, post_image) {
            out.settled.push(op.token);
            ledger.pending.remove(idx);
        } else if op.writer_done {
            out.conflicted.push(op.token);
            ledger.pending.remove(idx);
        } else {
            break;
        }
    }
    out.writes.push(ledger.write_for(key));
}

#[cfg(test)]
mod tests {
    use palimpsest_proto::palimpsest::sync::v1::DiffOp;
    use palimpsest_proto::wire::{WireDatum, WireRow, WireRowChange};

    use super::super::db::LocalWrite;
    use super::{Reconciler, ResolvedKind};
    use crate::cache::PrimaryKey;

    fn key_of(row: &WireRow) -> PrimaryKey {
        vec![row[0].clone()]
    }

    fn row(id: i64, title: &str) -> WireRow {
        vec![
            WireDatum::I64(id),
            WireDatum::Text(title.as_bytes().to_vec()),
        ]
    }

    fn insert_change(new: WireRow) -> WireRowChange {
        WireRowChange {
            op: DiffOp::Insert,
            old: None,
            new: Some(new),
        }
    }

    #[test]
    fn passthrough_when_no_pending() {
        let mut rec = Reconciler::default();
        let out = rec.apply_server_changes("posts", &[insert_change(row(1, "a"))], key_of);
        assert_eq!(out.writes, vec![LocalWrite::Upsert { row: row(1, "a") }]);
        assert!(out.settled.is_empty() && out.conflicted.is_empty());
    }

    #[test]
    fn stage_update_then_settle_on_matching_change() {
        let mut rec = Reconciler::default();
        let writes = rec.stage(
            "posts",
            1,
            vec![WireDatum::I64(1)],
            ResolvedKind::Update {
                set: vec![(1, WireDatum::Text(b"new".to_vec()))],
            },
            Some(row(1, "old")),
        );
        assert_eq!(writes, vec![LocalWrite::Upsert { row: row(1, "new") }]);

        // Server confirms the write.
        let out = rec.apply_server_changes(
            "posts",
            &[WireRowChange {
                op: DiffOp::Update,
                old: Some(row(1, "old")),
                new: Some(row(1, "new")),
            }],
            key_of,
        );
        assert_eq!(out.settled, vec![1]);
        assert_eq!(out.writes, vec![LocalWrite::Upsert { row: row(1, "new") }]);
        assert_eq!(rec.pending_count("posts"), 0);
    }

    #[test]
    fn concurrent_change_rebases_pending_update() {
        let mut rec = Reconciler::default();
        // Optimistically set title while the row's other column (here:
        // a different title from another writer) changes server-side.
        rec.stage(
            "posts",
            7,
            vec![WireDatum::I64(1)],
            ResolvedKind::Update {
                set: vec![(1, WireDatum::Text(b"mine".to_vec()))],
            },
            Some(row(1, "old")),
        );
        let out = rec.apply_server_changes(
            "posts",
            &[WireRowChange {
                op: DiffOp::Update,
                old: Some(row(1, "old")),
                new: Some(row(1, "theirs")),
            }],
            key_of,
        );
        // Not settled, not conflicted (writer still in flight): the
        // pending update is rebased on the new base.
        assert!(out.settled.is_empty() && out.conflicted.is_empty());
        assert_eq!(
            out.writes,
            vec![LocalWrite::Upsert {
                row: row(1, "mine")
            }]
        );
        assert_eq!(rec.pending_count("posts"), 1);
    }

    #[test]
    fn acked_mutation_conflicts_when_server_disagrees() {
        let mut rec = Reconciler::default();
        rec.stage(
            "posts",
            3,
            vec![WireDatum::I64(1)],
            ResolvedKind::Update {
                set: vec![(1, WireDatum::Text(b"mine".to_vec()))],
            },
            Some(row(1, "old")),
        );
        assert_eq!(rec.writer_finished(3, true).table.as_deref(), Some("posts"));
        let out = rec.apply_server_changes(
            "posts",
            &[WireRowChange {
                op: DiffOp::Update,
                old: Some(row(1, "old")),
                new: Some(row(1, "server-won")),
            }],
            key_of,
        );
        assert_eq!(out.conflicted, vec![3]);
        // Server wins: visible row is the authoritative one.
        assert_eq!(
            out.writes,
            vec![LocalWrite::Upsert {
                row: row(1, "server-won")
            }]
        );
        assert_eq!(rec.pending_count("posts"), 0);
    }

    #[test]
    fn failed_writer_rolls_back_to_base() {
        let mut rec = Reconciler::default();
        rec.stage(
            "posts",
            9,
            vec![WireDatum::I64(1)],
            ResolvedKind::Delete,
            Some(row(1, "keep")),
        );
        let apply = rec.writer_finished(9, false);
        assert_eq!(
            apply.writes,
            vec![LocalWrite::Upsert {
                row: row(1, "keep")
            }]
        );
        assert_eq!(rec.pending_count("posts"), 0);
    }

    #[test]
    fn insert_settles_on_matching_server_insert_with_defaults() {
        let mut rec = Reconciler::default();
        // Caller only provided column 0; column 1 defaults server-side.
        rec.stage(
            "posts",
            4,
            vec![WireDatum::I64(5)],
            ResolvedKind::Insert {
                row: vec![WireDatum::I64(5), WireDatum::Null],
                set_columns: vec![0],
            },
            None,
        );
        let out = rec.apply_server_changes("posts", &[insert_change(row(5, "default"))], key_of);
        assert_eq!(out.settled, vec![4]);
        assert_eq!(
            out.writes,
            vec![LocalWrite::Upsert {
                row: row(5, "default")
            }]
        );
    }

    #[test]
    fn initial_snapshot_rebases_pending_mutations() {
        let mut rec = Reconciler::default();
        rec.stage(
            "posts",
            2,
            vec![WireDatum::I64(1)],
            ResolvedKind::Update {
                set: vec![(1, WireDatum::Text(b"mine".to_vec()))],
            },
            Some(row(1, "old")),
        );
        // Pending insert of a brand-new row too.
        rec.stage(
            "posts",
            5,
            vec![WireDatum::I64(99)],
            ResolvedKind::Insert {
                row: row(99, "draft"),
                set_columns: vec![0, 1],
            },
            None,
        );
        let visible = rec.apply_initial("posts", vec![row(1, "fresh"), row(2, "b")], key_of);
        // Row 1 keeps the optimistic title over the fresh base; row 2
        // passes through; row 99 (pending insert) is still visible.
        assert!(visible.contains(&row(1, "mine")));
        assert!(visible.contains(&row(2, "b")));
        assert!(visible.contains(&row(99, "draft")));
        assert_eq!(visible.len(), 3);
    }

    #[test]
    fn pk_changing_update_splits_into_delete_and_insert() {
        let mut rec = Reconciler::default();
        let out = rec.apply_server_changes(
            "posts",
            &[WireRowChange {
                op: DiffOp::Update,
                old: Some(row(1, "a")),
                new: Some(row(2, "a")),
            }],
            key_of,
        );
        assert_eq!(
            out.writes,
            vec![
                LocalWrite::Delete {
                    key: vec![WireDatum::I64(1)]
                },
                LocalWrite::Upsert { row: row(2, "a") },
            ]
        );
    }

    #[test]
    fn stacked_mutations_on_one_key_settle_in_order() {
        let mut rec = Reconciler::default();
        rec.stage(
            "posts",
            1,
            vec![WireDatum::I64(1)],
            ResolvedKind::Update {
                set: vec![(1, WireDatum::Text(b"first".to_vec()))],
            },
            Some(row(1, "base")),
        );
        rec.stage(
            "posts",
            2,
            vec![WireDatum::I64(1)],
            ResolvedKind::Update {
                set: vec![(1, WireDatum::Text(b"second".to_vec()))],
            },
            Some(row(1, "first")),
        );
        // First write confirmed; second still pending and rebased.
        let out = rec.apply_server_changes(
            "posts",
            &[WireRowChange {
                op: DiffOp::Update,
                old: Some(row(1, "base")),
                new: Some(row(1, "first")),
            }],
            key_of,
        );
        assert_eq!(out.settled, vec![1]);
        assert_eq!(
            out.writes,
            vec![LocalWrite::Upsert {
                row: row(1, "second")
            }]
        );
        assert_eq!(rec.pending_count("posts"), 1);
    }
}
