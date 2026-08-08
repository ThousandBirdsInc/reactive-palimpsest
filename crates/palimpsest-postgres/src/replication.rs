// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Connection lifecycle: introspection, replica-identity and
//! publication ownership, slot management, the fenced initial
//! snapshot, the change-poll loop, and reconnect-with-reconciliation.

use std::collections::BTreeMap;

use bytes::Bytes;
use palimpsest_dataflow::palimpsest::Row;
use palimpsest_wal::{
    decode_column_value, decode_pgoutput_message, Catalog, ColumnValue, DecodedEvent,
    ReconnectBackoff, ReplicaIdentity, RowOp, TableId, Tuple,
};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::error::PostgresRuntimeError;
use crate::introspect::{introspect_tables, IntrospectedTable};
use crate::runtime::{PostgresRuntimeConfig, PostgresWalRuntime};

/// Handle to the background replication task. Dropping it leaves the
/// task running; call [`Self::shutdown`] for a clean stop.
pub struct ReplicationHandle {
    shutdown: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl ReplicationHandle {
    /// Signals the replication task to stop and waits for it.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

impl PostgresWalRuntime {
    /// Connects to Postgres and brings up the full runtime:
    /// introspects the streamed tables, verifies (or sets) replica
    /// identity, creates or reconciles the publication, creates or
    /// resumes the replication slot, takes the fenced initial
    /// snapshot, and spawns the change-ingest task.
    ///
    /// # Errors
    /// Every failure names the exact problem — a missing table, the
    /// remedial `ALTER TABLE`, the missing replication grant — rather
    /// than surfacing as a bare connection error. The DSN never
    /// appears in errors or logs.
    pub async fn connect(
        config: PostgresRuntimeConfig,
    ) -> Result<(Self, ReplicationHandle), PostgresRuntimeError> {
        let client = open_client(&config.dsn).await?;

        let tables = introspect_tables(&client, &config.tables).await?;
        ensure_replica_identity(&client, &tables, config.manage_replica_identity).await?;
        ensure_publication(&client, &config.publication, &tables).await?;
        ensure_slot(&client, &config.slot).await?;

        let runtime = Self::from_tables(tables, config.journal_capacity);

        // Snapshot AFTER the slot exists so no change can fall between
        // them; the transaction-id fence below keeps changes that are
        // both in the snapshot and in the slot from double-applying.
        let (lsn, fence, rows) = fenced_snapshot(&client, &runtime).await?;
        runtime.seed_snapshot(lsn, rows);
        tracing::info!(
            snapshot_lsn = lsn,
            tables = runtime.tables().len(),
            "postgres runtime seeded"
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(ingest_task(
            config.clone(),
            runtime.clone(),
            client,
            fence,
            shutdown_rx,
        ));

        Ok((
            runtime,
            ReplicationHandle {
                shutdown: shutdown_tx,
                task,
            },
        ))
    }
}

/// Opens a client and spawns its connection driver.
async fn open_client(dsn: &str) -> Result<Client, PostgresRuntimeError> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .map_err(|err| PostgresRuntimeError::Connect(err.to_string()))?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::warn!(error = %err, "postgres connection closed");
        }
    });
    Ok(client)
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn qualified_name(table: &IntrospectedTable) -> String {
    format!(
        "{}.{}",
        quote_ident(&table.namespace),
        quote_ident(&table.name)
    )
}

const fn identity_label(identity: ReplicaIdentity) -> &'static str {
    match identity {
        ReplicaIdentity::Default => "DEFAULT",
        ReplicaIdentity::Nothing => "NOTHING",
        ReplicaIdentity::Full => "FULL",
        ReplicaIdentity::Index => "USING INDEX",
    }
}

/// Retractions need the full old row, so every streamed table needs
/// `REPLICA IDENTITY FULL`. Verify, set where permitted, refuse with
/// the exact remedial DDL where not.
async fn ensure_replica_identity(
    client: &Client,
    tables: &[IntrospectedTable],
    manage: bool,
) -> Result<(), PostgresRuntimeError> {
    for table in tables {
        if table.replica_identity == ReplicaIdentity::Full {
            continue;
        }
        let refusal = || PostgresRuntimeError::ReplicaIdentity {
            table: format!("{}.{}", table.namespace, table.name),
            current: identity_label(table.replica_identity),
        };
        if !manage {
            return Err(refusal());
        }
        let ddl = format!(
            "ALTER TABLE {} REPLICA IDENTITY FULL",
            qualified_name(table)
        );
        if let Err(err) = client.simple_query(&ddl).await {
            tracing::warn!(
                table = %table.name,
                error = %err,
                "could not set REPLICA IDENTITY FULL; refusing to start"
            );
            return Err(refusal());
        }
        tracing::info!(table = %table.name, "set REPLICA IDENTITY FULL");
    }
    Ok(())
}

/// Creates the publication from the streamed set, or reconciles an
/// existing one to exactly that set.
async fn ensure_publication(
    client: &Client,
    publication: &str,
    tables: &[IntrospectedTable],
) -> Result<(), PostgresRuntimeError> {
    let table_list = tables
        .iter()
        .map(qualified_name)
        .collect::<Vec<_>>()
        .join(", ");

    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .map_err(|source| PostgresRuntimeError::query("publication lookup", source))?
        .is_some();

    if exists {
        let published: Vec<(String, String)> = client
            .query(
                "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1",
                &[&publication],
            )
            .await
            .map_err(|source| PostgresRuntimeError::query("publication tables", source))?
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        let mut wanted: Vec<(String, String)> = tables
            .iter()
            .map(|table| (table.namespace.clone(), table.name.clone()))
            .collect();
        wanted.sort();
        let mut current = published;
        current.sort();
        if current != wanted {
            let ddl = format!(
                "ALTER PUBLICATION {} SET TABLE {}",
                quote_ident(publication),
                table_list
            );
            client.simple_query(&ddl).await.map_err(|err| {
                PostgresRuntimeError::Publication {
                    publication: publication.to_owned(),
                    detail: err.to_string(),
                }
            })?;
            tracing::info!(%publication, "publication reconciled to the derived table set");
        }
    } else {
        let ddl = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            quote_ident(publication),
            table_list
        );
        client.simple_query(&ddl).await.map_err(|err| {
            PostgresRuntimeError::Publication {
                publication: publication.to_owned(),
                detail: err.to_string(),
            }
        })?;
        tracing::info!(%publication, "publication created");
    }
    Ok(())
}

/// Creates the logical replication slot if it doesn't exist. An
/// existing slot resumes from its own position.
async fn ensure_slot(client: &Client, slot: &str) -> Result<(), PostgresRuntimeError> {
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(|source| PostgresRuntimeError::query("slot lookup", source))?
        .is_some();
    if exists {
        return Ok(());
    }
    if let Err(err) = client
        .query_one(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
    {
        let detail = err.to_string();
        // Name the missing grant instead of surfacing a bare error.
        if detail.contains("replication") || detail.contains("42501") {
            return Err(PostgresRuntimeError::MissingReplicationPrivilege { detail });
        }
        return Err(PostgresRuntimeError::Slot {
            slot: slot.to_owned(),
            detail,
        });
    }
    tracing::info!(%slot, "replication slot created");
    Ok(())
}

// ---------------------------------------------------------------------
// Fenced snapshot
// ---------------------------------------------------------------------

/// Transaction-id visibility fence captured with the snapshot. Slot
/// transactions whose xid the snapshot already saw as committed are
/// skipped so they don't double-apply.
#[derive(Debug, Clone, Default)]
pub(crate) struct SnapshotFence {
    xmax: u64,
    in_progress: Vec<u64>,
}

impl SnapshotFence {
    /// Parses `pg_current_snapshot()` text form: `xmin:xmax:xip1,...`.
    fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split(':');
        let _xmin = parts.next()?;
        let xmax: u64 = parts.next()?.parse().ok()?;
        let xip = parts.next().unwrap_or("");
        let in_progress = if xip.is_empty() {
            Vec::new()
        } else {
            xip.split(',').filter_map(|x| x.parse().ok()).collect()
        };
        Some(Self { xmax, in_progress })
    }

    /// True when a slot transaction with `xid` was already visible to
    /// the snapshot (committed before it) and must not re-apply.
    pub(crate) fn already_in_snapshot(&self, xid: u32) -> bool {
        if self
            .in_progress
            .iter()
            .any(|x| u32::try_from(x & 0xFFFF_FFFF).unwrap_or(u32::MAX) == xid)
        {
            return false;
        }
        let xmax_low = u32::try_from(self.xmax & 0xFFFF_FFFF).unwrap_or(u32::MAX);
        // Wraparound-aware TransactionIdPrecedes(xid, xmax): committed
        // before the snapshot's horizon and not in-progress means the
        // snapshot already saw its rows.
        (xid.wrapping_sub(xmax_low) as i32) < 0
    }
}

/// Takes a repeatable-read snapshot of every streamed table, returning
/// the WAL position, the xid fence, and the decoded rows.
async fn fenced_snapshot(
    client: &Client,
    runtime: &PostgresWalRuntime,
) -> Result<(u64, SnapshotFence, BTreeMap<TableId, Vec<Row>>), PostgresRuntimeError> {
    let phase = "snapshot";
    client
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .map_err(|source| PostgresRuntimeError::query(phase, source))?;

    let result = snapshot_in_transaction(client, runtime).await;

    // Always close the transaction, even on failure.
    let _ = client.simple_query("COMMIT").await;
    result
}

async fn snapshot_in_transaction(
    client: &Client,
    runtime: &PostgresWalRuntime,
) -> Result<(u64, SnapshotFence, BTreeMap<TableId, Vec<Row>>), PostgresRuntimeError> {
    let phase = "snapshot";
    // One statement establishes the snapshot AND reads the fence +
    // WAL position from it, minimizing the race window to intra-
    // statement.
    let header = client
        .simple_query(
            "SELECT pg_current_snapshot()::text AS fence, \
             (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint::text AS lsn",
        )
        .await
        .map_err(|source| PostgresRuntimeError::query(phase, source))?;
    let (fence_text, lsn_text) = first_row(&header)
        .and_then(|row| Some((row.get(0)?.to_owned(), row.get(1)?.to_owned())))
        .ok_or_else(|| PostgresRuntimeError::Snapshot {
            table: "<fence>".to_owned(),
            detail: "pg_current_snapshot() returned no row".to_owned(),
        })?;
    let fence = SnapshotFence::parse(&fence_text).ok_or_else(|| PostgresRuntimeError::Snapshot {
        table: "<fence>".to_owned(),
        detail: format!("unparseable snapshot '{fence_text}'"),
    })?;
    let lsn: u64 = lsn_text
        .parse()
        .map_err(|_| PostgresRuntimeError::Snapshot {
            table: "<fence>".to_owned(),
            detail: format!("unparseable wal position '{lsn_text}'"),
        })?;

    let mut rows = BTreeMap::new();
    for table in runtime.tables() {
        rows.insert(table.id, snapshot_table(client, &table).await?);
    }
    Ok((lsn, fence, rows))
}

fn first_row(messages: &[SimpleQueryMessage]) -> Option<&tokio_postgres::SimpleQueryRow> {
    messages.iter().find_map(|message| match message {
        SimpleQueryMessage::Row(row) => Some(row),
        _ => None,
    })
}

/// Reads one table via the simple (text) protocol and decodes values
/// with the same decoders the WAL path uses, so snapshot rows and
/// streamed rows are byte-identical for identical data.
async fn snapshot_table(
    client: &Client,
    table: &IntrospectedTable,
) -> Result<Vec<Row>, PostgresRuntimeError> {
    let column_list = table
        .columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {column_list} FROM {}", qualified_name(table));
    let messages = client
        .simple_query(&sql)
        .await
        .map_err(|err| PostgresRuntimeError::Snapshot {
            table: table.name.clone(),
            detail: err.to_string(),
        })?;

    let mut rows = Vec::new();
    for message in &messages {
        let SimpleQueryMessage::Row(row) = message else {
            continue;
        };
        let mut decoded: Row = Row::with_capacity(table.columns.len());
        for (index, column) in table.columns.iter().enumerate() {
            let value = row.get(index).map_or(ColumnValue::Null, |text| {
                ColumnValue::Text(Bytes::copy_from_slice(text.as_bytes()))
            });
            let datum = decode_column_value(&column.datum_type, value).map_err(|err| {
                PostgresRuntimeError::Snapshot {
                    table: table.name.clone(),
                    detail: format!("column '{}': {err}", column.name),
                }
            })?;
            decoded.push(datum);
        }
        rows.push(decoded);
    }
    Ok(rows)
}

// ---------------------------------------------------------------------
// Ingest loop
// ---------------------------------------------------------------------

/// One in-flight transaction being assembled from pgoutput frames.
#[derive(Default)]
struct PendingTransaction {
    xid: u32,
    changes: Vec<(TableId, RowOp, Option<Tuple>, Option<Tuple>)>,
    truncates: Vec<TableId>,
}

/// Background task: poll the slot, decode frames, apply transactions;
/// on connection failure, reconnect with backoff, re-snapshot, and
/// reconcile the difference.
async fn ingest_task(
    config: PostgresRuntimeConfig,
    runtime: PostgresWalRuntime,
    client: Client,
    fence: SnapshotFence,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    // The wal catalog is seeded from introspection so pgoutput tuples
    // decode with the real types (enums as labels, arrays with their
    // element type) — a stock Relation-frame mapping would coarsen
    // them.
    let mut catalog = Catalog::new();
    for table in runtime.tables() {
        catalog.upsert_relation(table.relation_schema());
    }

    let mut client = Some(client);
    let mut fence = fence;
    let mut backoff = ReconnectBackoff::default();

    loop {
        if *shutdown.borrow() {
            return;
        }
        let Some(active) = client.as_ref() else {
            // Reconnect path: backoff, dial, re-snapshot, reconcile.
            let delay = backoff.next_delay();
            tokio::select! {
                _ = shutdown.changed() => return,
                () = tokio::time::sleep(delay) => {}
            }
            match reconnect(&config, &runtime).await {
                Ok((fresh_client, fresh_fence)) => {
                    tracing::info!("postgres runtime reconnected and reconciled");
                    backoff = ReconnectBackoff::default();
                    fence = fresh_fence;
                    client = Some(fresh_client);
                }
                Err(err) => {
                    tracing::warn!(error = %err, "postgres reconnect failed; backing off");
                }
            }
            continue;
        };

        match poll_once(active, &config, &runtime, &mut catalog, &fence).await {
            Ok(applied) => {
                if !applied {
                    tokio::select! {
                        _ = shutdown.changed() => return,
                        () = tokio::time::sleep(config.poll_interval) => {}
                    }
                }
            }
            Err(PollError::Fatal(message)) => {
                runtime.fail(message);
                return;
            }
            Err(PollError::Connection(err)) => {
                tracing::warn!(error = %err, "postgres poll failed; will reconnect");
                client = None;
            }
        }
    }
}

enum PollError {
    /// Connection-level failure — reconnect and reconcile.
    Connection(String),
    /// Unrecoverable (schema drift) — latch the runtime failed.
    Fatal(String),
}

/// Drains one batch of slot changes. Returns whether anything applied.
async fn poll_once(
    client: &Client,
    config: &PostgresRuntimeConfig,
    runtime: &PostgresWalRuntime,
    catalog: &mut Catalog,
    fence: &SnapshotFence,
) -> Result<bool, PollError> {
    let rows = client
        .query(
            "SELECT data FROM pg_logical_slot_get_binary_changes($1, NULL, NULL, \
             'proto_version', '1', 'publication_names', $2)",
            &[&config.slot, &config.publication],
        )
        .await
        .map_err(|err| PollError::Connection(err.to_string()))?;

    let mut pending: Option<PendingTransaction> = None;
    let mut applied = false;

    for row in rows {
        let data: &[u8] = row.get(0);
        let event = decode_pgoutput_message(catalog, Bytes::copy_from_slice(data))
            .map_err(|err| PollError::Connection(format!("pgoutput decode: {err}")))?;
        match event {
            DecodedEvent::Begin { xid, .. } => {
                pending = Some(PendingTransaction {
                    xid,
                    ..PendingTransaction::default()
                });
            }
            DecodedEvent::Row {
                table, op, old, new
            } => {
                if runtime.table_by_id(table).is_some() {
                    if let Some(pending) = pending.as_mut() {
                        pending.changes.push((table, op, old, new));
                    }
                }
            }
            DecodedEvent::Truncate(truncate) => {
                if let Some(pending) = pending.as_mut() {
                    pending.truncates.extend(
                        truncate
                            .tables
                            .into_iter()
                            .filter(|table| runtime.table_by_id(*table).is_some()),
                    );
                }
            }
            DecodedEvent::Commit { commit_lsn, .. } => {
                if let Some(transaction) = pending.take() {
                    if fence.already_in_snapshot(transaction.xid) {
                        continue;
                    }
                    if !transaction.truncates.is_empty() {
                        runtime.apply_truncate(commit_lsn.get(), &transaction.truncates);
                        applied = true;
                    }
                    if !transaction.changes.is_empty() {
                        runtime.apply_transaction(commit_lsn.get(), transaction.changes);
                        applied = true;
                    }
                }
            }
            DecodedEvent::Schema { table, columns } => {
                // Drift check: a Relation frame that no longer matches
                // the introspected shape means rows would mis-decode.
                // Stop loudly instead.
                if let Some(known) = runtime.table_by_id(table) {
                    let streamed: Vec<&str> =
                        columns.iter().map(|column| column.name.as_str()).collect();
                    let introspected: Vec<&str> = known
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect();
                    if streamed == introspected {
                        // Re-assert the richer introspected types over
                        // the stock mapping the decoder just stored.
                        catalog.upsert_relation(known.relation_schema());
                    } else {
                        return Err(PollError::Fatal(
                            crate::error::PostgresRuntimeError::SchemaDrift {
                                table: known.name.clone(),
                                detail: format!(
                                    "columns changed from [{}] to [{}]",
                                    introspected.join(", "),
                                    streamed.join(", ")
                                ),
                            }
                            .to_string(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(applied)
}

/// Re-dials Postgres and reconciles: fresh snapshot, bag-diff against
/// the mirror, one reconciliation transaction.
async fn reconnect(
    config: &PostgresRuntimeConfig,
    runtime: &PostgresWalRuntime,
) -> Result<(Client, SnapshotFence), PostgresRuntimeError> {
    let client = open_client(&config.dsn).await?;
    let (lsn, fence, rows) = fenced_snapshot(&client, runtime).await?;
    runtime.reconcile_snapshot(lsn, rows);
    Ok((client, fence))
}

#[cfg(test)]
mod tests {
    use super::SnapshotFence;

    #[test]
    fn parses_snapshot_fence_text() {
        let fence = SnapshotFence::parse("100:105:101,103").expect("parse");
        assert_eq!(fence.xmax, 105);
        assert_eq!(fence.in_progress, vec![101, 103]);
        assert!(SnapshotFence::parse("100:105:").is_some());
        assert!(SnapshotFence::parse("garbage").is_none());
    }

    #[test]
    fn fence_skips_committed_and_applies_in_progress() {
        let fence = SnapshotFence::parse("100:105:101,103").expect("parse");
        // Committed before the horizon and not in-progress: visible in
        // the snapshot, must be skipped.
        assert!(fence.already_in_snapshot(99));
        assert!(fence.already_in_snapshot(102));
        // In-progress at snapshot time: not visible, must apply.
        assert!(!fence.already_in_snapshot(101));
        assert!(!fence.already_in_snapshot(103));
        // At or beyond the horizon: not visible, must apply.
        assert!(!fence.already_in_snapshot(105));
        assert!(!fence.already_in_snapshot(200));
    }

    #[test]
    fn fence_comparison_survives_xid_wraparound() {
        // Horizon just past wraparound: an xid just below u32::MAX
        // precedes it.
        let fence = SnapshotFence {
            xmax: 5,
            in_progress: Vec::new(),
        };
        assert!(fence.already_in_snapshot(u32::MAX - 1));
        assert!(!fence.already_in_snapshot(6));
    }
}
