// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! §18.13.3 — wire-byte conformance.
//!
//! For a fixed corpus of operations, we drive real Postgres logical
//! decoding and capture pgoutput frames, then compare against
//! `WalGenerator::encode_pgoutput` for the same logical events.
//!
//! Bytes that depend on real-PG state (LSNs, XIDs, commit timestamps,
//! type OIDs, the auto-assigned relation OID) cannot match exactly
//! across runs; we normalize those out and assert the **shape** —
//! message tags + per-message stable bytes — matches.
//!
//! Without `--features real-postgres`, this test is a compile-only
//! skip.

#![allow(clippy::missing_const_for_fn)]

#[test]
fn skip_without_real_postgres_feature() {
    #[cfg(not(feature = "real-postgres"))]
    {
        eprintln!("[conformance/wire_bytes] skipped (build with --features real-postgres)");
    }
}

#[cfg(feature = "real-postgres")]
mod with_pg {
    use palimpsest_conformance::harness::{connect_or_skip, Connection};
    use palimpsest_test_harness::{
        Catalog, ColumnDef, LogicalEvent, TableDef, TableId, Tuple, WalGenerator,
    };

    /// One normalized pgoutput message. Variable bytes (LSN, XID,
    /// commit timestamp, real-PG-assigned relid, type OIDs) are
    /// stripped; what remains is what both real-PG and `WalGenerator`
    /// must agree on byte-for-byte.
    #[derive(Debug, PartialEq, Eq)]
    enum Normalized {
        Begin,
        Commit,
        Relation {
            namespace: Vec<u8>,
            replica_identity: u8,
            columns: Vec<NormalizedColumn>,
        },
        Insert {
            tuple: NormalizedTuple,
        },
        Update {
            old: Option<NormalizedTuple>,
            new: NormalizedTuple,
        },
        Delete {
            tuple: NormalizedTuple,
        },
        Other(u8),
    }

    #[derive(Debug, PartialEq, Eq)]
    struct NormalizedColumn {
        flags: u8,
        // We keep the *count* of columns and their key-flag, but
        // not the names/oids — column names differ between
        // `WalGenerator` ("c1", "c2") and the real schema
        // ("id", "author_id", ...).
    }

    /// Per-column tuple datum, with `(kind, payload)` where:
    /// - `b'n'` (null) and `b'u'` (unchanged-toast) carry no payload,
    /// - `b't'` carries text bytes — we keep the full payload.
    /// - `b'b'` (binary) is mapped to `b't'` for the comparison since
    ///   both encoders may pick either representation; the underlying
    ///   bytes for our int8/text corpus are identical in text form.
    #[derive(Debug, PartialEq, Eq)]
    struct NormalizedTuple {
        cells: Vec<NormalizedCell>,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum NormalizedCell {
        Null,
        UnchangedToast,
        Text(Vec<u8>),
    }

    /// Parse a single pgoutput message and return its normalized form.
    /// Skips the variable bytes that depend on runtime state.
    fn normalize(frame: &[u8]) -> Normalized {
        let tag = frame[0];
        let body = &frame[1..];
        match tag {
            b'B' => Normalized::Begin,
            b'C' => Normalized::Commit,
            b'R' => parse_relation(body),
            b'I' => parse_insert(body),
            b'U' => parse_update(body),
            b'D' => parse_delete(body),
            other => Normalized::Other(other),
        }
    }

    fn parse_relation(body: &[u8]) -> Normalized {
        // skip relid (4)
        let mut p = 4;
        let (namespace, used) = read_cstr(&body[p..]);
        p += used;
        let (_relname, used) = read_cstr(&body[p..]);
        p += used;
        let replica_identity = body[p];
        p += 1;
        let column_count = u16::from_be_bytes([body[p], body[p + 1]]) as usize;
        p += 2;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let flags = body[p];
            p += 1;
            let (_name, used) = read_cstr(&body[p..]);
            p += used;
            // skip type oid (4) + typmod (4)
            p += 8;
            columns.push(NormalizedColumn { flags });
        }
        Normalized::Relation {
            namespace,
            replica_identity,
            columns,
        }
    }

    fn parse_insert(body: &[u8]) -> Normalized {
        // skip relid (4)
        let mut p = 4;
        // tuple type marker — must be 'N' for INSERT
        assert_eq!(body[p], b'N', "INSERT tuple marker");
        p += 1;
        let (tuple, _) = read_tuple(&body[p..]);
        Normalized::Insert { tuple }
    }

    fn parse_update(body: &[u8]) -> Normalized {
        let mut p = 4;
        let mut old: Option<NormalizedTuple> = None;
        loop {
            match body[p] {
                b'K' | b'O' => {
                    p += 1;
                    let (t, used) = read_tuple(&body[p..]);
                    old = Some(t);
                    p += used;
                }
                b'N' => {
                    p += 1;
                    let (new, _) = read_tuple(&body[p..]);
                    return Normalized::Update { old, new };
                }
                other => panic!("unexpected UPDATE tuple marker: {other:#x}"),
            }
        }
    }

    fn parse_delete(body: &[u8]) -> Normalized {
        let mut p = 4;
        let marker = body[p];
        assert!(matches!(marker, b'K' | b'O'), "DELETE tuple marker");
        p += 1;
        let (tuple, _) = read_tuple(&body[p..]);
        Normalized::Delete { tuple }
    }

    fn read_cstr(buf: &[u8]) -> (Vec<u8>, usize) {
        let nul = buf
            .iter()
            .position(|&b| b == 0)
            .expect("cstring is null-terminated");
        (buf[..nul].to_vec(), nul + 1)
    }

    fn read_tuple(buf: &[u8]) -> (NormalizedTuple, usize) {
        let column_count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        let mut p = 2;
        let mut cells = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let kind = buf[p];
            p += 1;
            match kind {
                b'n' => cells.push(NormalizedCell::Null),
                b'u' => cells.push(NormalizedCell::UnchangedToast),
                b't' | b'b' => {
                    let len =
                        u32::from_be_bytes([buf[p], buf[p + 1], buf[p + 2], buf[p + 3]]) as usize;
                    p += 4;
                    cells.push(NormalizedCell::Text(buf[p..p + len].to_vec()));
                    p += len;
                }
                other => panic!("unexpected tuple cell kind: {other:#x}"),
            }
        }
        (NormalizedTuple { cells }, p)
    }

    /// Fixed corpus shared between the real-PG side and the
    /// `WalGenerator` side. Returns:
    ///   - the SQL DDL/DML the real-PG side runs, in order,
    ///   - the equivalent logical events the generator should encode.
    fn fixed_corpus() -> (Vec<&'static str>, (Catalog, Vec<LogicalEvent>)) {
        let table_id = TableId::new(1);
        let columns = vec![
            ColumnDef {
                name: "id".into(),
                type_oid: 20,
                nullable: false,
            },
            ColumnDef {
                name: "author_id".into(),
                type_oid: 20,
                nullable: false,
            },
            ColumnDef {
                name: "title".into(),
                type_oid: 25,
                nullable: false,
            },
        ];
        let table = TableDef::new(table_id, "posts", columns);
        let catalog = Catalog::with_tables([table]);

        let row = |id: &str, author: &str, title: &str| -> Tuple {
            vec![id.into(), author.into(), title.into()]
        };

        let events = vec![
            LogicalEvent::Begin { xid: 100 },
            LogicalEvent::Insert {
                table: table_id,
                new: row("1", "7", "first"),
            },
            LogicalEvent::Insert {
                table: table_id,
                new: row("2", "8", "second"),
            },
            LogicalEvent::Update {
                table: table_id,
                old: Some(row("1", "7", "first")),
                new: row("1", "7", "first-edited"),
            },
            LogicalEvent::Delete {
                table: table_id,
                old: row("2", "8", "second"),
            },
            LogicalEvent::Commit,
        ];

        let ddl_dml = vec![
            "DROP TABLE IF EXISTS palimpsest_wire_posts;",
            "CREATE TABLE palimpsest_wire_posts (\
                id BIGINT PRIMARY KEY, \
                author_id BIGINT NOT NULL, \
                title TEXT NOT NULL\
             );",
            // FULL replica identity so UPDATE/DELETE emit old-tuple
            // bytes that we can compare against `Some(old)` in the
            // logical events. WalGenerator emits 'O' (old full) under
            // FULL identity.
            "ALTER TABLE palimpsest_wire_posts REPLICA IDENTITY FULL;",
            "INSERT INTO palimpsest_wire_posts(id, author_id, title) VALUES (1, 7, 'first');",
            "INSERT INTO palimpsest_wire_posts(id, author_id, title) VALUES (2, 8, 'second');",
            "UPDATE palimpsest_wire_posts SET title = 'first-edited' WHERE id = 1;",
            "DELETE FROM palimpsest_wire_posts WHERE id = 2;",
        ];

        (ddl_dml, (catalog, events))
    }

    /// Drop+recreate the slot/publication used by this test.
    async fn reset_slot(conn: &Connection) {
        let _ = conn
            .client
            .execute("SELECT pg_drop_replication_slot('palimpsest_wire')", &[])
            .await;
        let _ = conn
            .client
            .execute("DROP PUBLICATION IF EXISTS palimpsest_wire_pub", &[])
            .await;
        conn.client
            .batch_execute("CREATE PUBLICATION palimpsest_wire_pub FOR ALL TABLES;")
            .await
            .expect("create publication");
        conn.client
            .query_one(
                "SELECT * FROM pg_create_logical_replication_slot('palimpsest_wire', 'pgoutput')",
                &[],
            )
            .await
            .expect("create slot");
    }

    /// Drain pgoutput frames from `pg_logical_slot_get_binary_changes`.
    async fn drain_frames(conn: &Connection) -> Vec<Vec<u8>> {
        let rows = conn
            .client
            .query(
                "SELECT data FROM pg_logical_slot_get_binary_changes(\
                    'palimpsest_wire', NULL, NULL, \
                    'proto_version', '1', \
                    'publication_names', 'palimpsest_wire_pub')",
                &[],
            )
            .await
            .expect("drain frames");
        rows.iter()
            .map(|r| {
                let v: &[u8] = r.get(0);
                v.to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn pgoutput_frame_shape_matches_real_pg() {
        let Some(conn) = connect_or_skip("wire_bytes").await else {
            return;
        };

        // Real-PG side.
        reset_slot(&conn).await;
        let (ddl_dml, (catalog, events)) = fixed_corpus();
        for stmt in &ddl_dml {
            conn.client.batch_execute(stmt).await.expect(stmt);
        }
        let real_frames = drain_frames(&conn).await;
        assert!(!real_frames.is_empty(), "real-PG produced no frames");

        // Generator side.
        let mut gen = WalGenerator::with_catalog(catalog);
        let our_frames = gen.encode_pgoutput(&events);

        let real_normalized: Vec<Normalized> = real_frames.iter().map(|f| normalize(f)).collect();
        let our_normalized: Vec<Normalized> = our_frames.iter().map(|f| normalize(f)).collect();

        // Tag-by-tag sequence must match.
        let real_tags: Vec<u8> = real_frames.iter().map(|f| f[0]).collect();
        let our_tags: Vec<u8> = our_frames.iter().map(|f| f[0]).collect();
        assert_eq!(
            real_tags, our_tags,
            "pgoutput tag sequence diverged\n  real: {real_tags:?}\n  ours: {our_tags:?}"
        );

        // For each tag class, assert the normalized form matches.
        for (i, (real, ours)) in real_normalized.iter().zip(&our_normalized).enumerate() {
            // Relation messages: namespace + replica identity +
            // column-count + per-column flags.
            match (real, ours) {
                (
                    Normalized::Relation {
                        namespace: r_ns,
                        replica_identity: r_ri,
                        columns: r_cols,
                    },
                    Normalized::Relation {
                        namespace: o_ns,
                        replica_identity: o_ri,
                        columns: o_cols,
                    },
                ) => {
                    assert_eq!(r_ns, o_ns, "frame {i}: relation namespace");
                    assert_eq!(r_ri, o_ri, "frame {i}: replica identity");
                    assert_eq!(r_cols.len(), o_cols.len(), "frame {i}: column count");
                    // The first column is the primary key in both sides;
                    // remaining columns have flags=0. Compare flags only.
                    for (j, (rc, oc)) in r_cols.iter().zip(o_cols).enumerate() {
                        assert_eq!(rc.flags, oc.flags, "frame {i} col {j}: flags");
                    }
                }
                _ => assert_eq!(real, ours, "frame {i} normalized payload"),
            }
        }
    }
}
