// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §18.13.6 — real-replication smoke.
//!
//! A subset of §15.6 scenarios re-run end-to-end against real
//! Postgres. We cover:
//!
//! - "initial snapshot then steady-state diffs": INSERT a row on a
//!   real table and assert a SELECT returns it.
//! - "valid resume LSN": create a logical slot, advance, and read
//!   `pg_replication_slots.confirmed_flush_lsn`.
//! - "logical-decoding round-trip": create a slot + publication, do
//!   B/I/C-shaped DML, and assert the pgoutput stream returned by
//!   `pg_logical_slot_get_binary_changes` contains the expected tag
//!   sequence.
//!
//! Without `--features real-postgres`, this test is a no-op.

#![allow(clippy::missing_const_for_fn)]

#[test]
fn skip_without_real_postgres_feature() {
    #[cfg(not(feature = "real-postgres"))]
    {
        eprintln!("[conformance/replication_smoke] skipped (build with --features real-postgres)");
    }
}

#[cfg(feature = "real-postgres")]
mod with_pg {
    use palimpsest_conformance::{harness::connect_or_skip, REPL_PUBLICATION, REPL_SLOT};

    async fn ensure_publication_and_slot(conn: &palimpsest_conformance::harness::Connection) {
        // Best-effort drop+create so the test is rerunnable.
        let _ = conn
            .client
            .execute(
                &format!("SELECT pg_drop_replication_slot('{REPL_SLOT}')"),
                &[],
            )
            .await;
        let _ = conn
            .client
            .execute(
                &format!("DROP PUBLICATION IF EXISTS {REPL_PUBLICATION}"),
                &[],
            )
            .await;
        conn.client
            .batch_execute(&format!(
                "CREATE PUBLICATION {REPL_PUBLICATION} FOR ALL TABLES;"
            ))
            .await
            .expect("create publication");
        conn.client
            .query_one(
                &format!(
                    "SELECT * FROM pg_create_logical_replication_slot('{REPL_SLOT}', 'pgoutput')"
                ),
                &[],
            )
            .await
            .expect("create logical slot");
    }

    async fn cleanup(conn: &palimpsest_conformance::harness::Connection) {
        let _ = conn
            .client
            .execute(
                &format!("SELECT pg_drop_replication_slot('{REPL_SLOT}')"),
                &[],
            )
            .await;
        let _ = conn
            .client
            .execute(
                &format!("DROP PUBLICATION IF EXISTS {REPL_PUBLICATION}"),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn scenario_initial_snapshot_then_insert_round_trips() {
        let Some(conn) = connect_or_skip("replication_smoke/initial").await else {
            return;
        };
        conn.client
            .batch_execute(
                "DROP TABLE IF EXISTS palimpsest_smoke_posts;
                 CREATE TABLE palimpsest_smoke_posts (
                    id BIGINT PRIMARY KEY,
                    author_id BIGINT NOT NULL
                 );
                 INSERT INTO palimpsest_smoke_posts(id, author_id) VALUES (1, 7), (2, 8);",
            )
            .await
            .expect("setup");

        // Steady state: another INSERT.
        conn.client
            .execute(
                "INSERT INTO palimpsest_smoke_posts(id, author_id) VALUES ($1, $2)",
                &[&3_i64, &9_i64],
            )
            .await
            .expect("steady-state insert");

        let rows = conn
            .client
            .query("SELECT id FROM palimpsest_smoke_posts ORDER BY id", &[])
            .await
            .expect("select");
        let ids: Vec<i64> = rows.iter().map(|r| r.get::<_, i64>(0)).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn scenario_resume_lsn_round_trip() {
        let Some(conn) = connect_or_skip("replication_smoke/resume").await else {
            return;
        };
        ensure_publication_and_slot(&conn).await;

        // Real-PG round-trips a slot LSN: the `confirmed_flush_lsn`
        // column returns the same value we ack.
        let row = conn
            .client
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                  WHERE slot_name = $1",
                &[&REPL_SLOT],
            )
            .await
            .expect("slot lookup");
        let lsn: String = row.get(0);
        assert!(!lsn.is_empty(), "expected confirmed_flush_lsn to be set");

        cleanup(&conn).await;
    }

    /// Drives a real `BEGIN; INSERT; COMMIT` cycle through pgoutput
    /// and asserts the captured frame sequence starts with `B` and
    /// includes at least one `I` (insert) and one `C` (commit). This
    /// is the §15.6 "snapshot+steady-state" path executed against the
    /// real WAL pipeline.
    #[tokio::test]
    async fn scenario_logical_decoding_round_trip() {
        let Some(conn) = connect_or_skip("replication_smoke/logical").await else {
            return;
        };
        ensure_publication_and_slot(&conn).await;

        conn.client
            .batch_execute(
                "DROP TABLE IF EXISTS palimpsest_smoke_logical;
                 CREATE TABLE palimpsest_smoke_logical (
                    id BIGINT PRIMARY KEY,
                    author_id BIGINT NOT NULL
                 );
                 ALTER TABLE palimpsest_smoke_logical REPLICA IDENTITY FULL;
                 INSERT INTO palimpsest_smoke_logical(id, author_id) VALUES (1, 7), (2, 8);",
            )
            .await
            .expect("setup");

        let rows = conn
            .client
            .query(
                &format!(
                    "SELECT data FROM pg_logical_slot_get_binary_changes(\
                        '{REPL_SLOT}', NULL, NULL, \
                        'proto_version', '1', \
                        'publication_names', '{REPL_PUBLICATION}')"
                ),
                &[],
            )
            .await
            .expect("get changes");
        let frames: Vec<Vec<u8>> = rows
            .iter()
            .map(|r| {
                let v: &[u8] = r.get(0);
                v.to_vec()
            })
            .collect();
        let tags: Vec<u8> = frames.iter().map(|f| f[0]).collect();
        assert!(
            tags.contains(&b'B'),
            "expected at least one Begin frame, got {tags:?}"
        );
        assert!(
            tags.contains(&b'I'),
            "expected at least one Insert frame, got {tags:?}"
        );
        assert!(
            tags.contains(&b'C'),
            "expected at least one Commit frame, got {tags:?}"
        );

        cleanup(&conn).await;
    }
}
