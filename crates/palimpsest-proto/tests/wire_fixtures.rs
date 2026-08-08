// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Wire-format snapshot tests (§18.9).
//!
//! Each canonical message is encoded with prost / the manual `Row`
//! codec and compared to a captured hex fixture. The fixtures live
//! inline as `const &str` so a regression shows up immediately as a
//! diff in this file: any change to field numbers, default values, or
//! the bincode row encoding will fail one of these tests.
//!
//! When intentionally rolling the wire (additive only — see
//! `VERSIONING.md`) regenerate fixtures with:
//!
//! ```bash
//! cargo test -p palimpsest-proto --test wire_fixtures -- --nocapture print_
//! ```
//!
//! The `print_*` helpers below dump the current encoding so the
//! captured constants can be replaced wholesale.

use palimpsest_proto::palimpsest::sync::v1 as proto;
use palimpsest_proto::wire::{decode_diff, encode_rows, SchemaRegistry, WireDatum};
use prost::Message;

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{byte:02x}").expect("write to String");
    }
    out
}

fn unhex(hex: &str) -> Vec<u8> {
    assert!(hex.len() % 2 == 0, "odd hex length");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digit"))
        .collect()
}

fn posts_schema() -> proto::Schema {
    proto::Schema {
        columns: vec![
            proto::Column {
                name: "id".into(),
                r#type: proto::DatumType::I64.into(),
                nullable: false,
            },
            proto::Column {
                name: "title".into(),
                r#type: proto::DatumType::Text.into(),
                nullable: true,
            },
        ],
        primary_key_columns: vec![0],
    }
}

fn canonical_subscribe() -> proto::ClientMessage {
    proto::ClientMessage {
        kind: Some(proto::client_message::Kind::Subscribe(
            proto::SubscribeRequest {
                client_subscription_id: "posts".into(),
                sql: "SELECT id FROM posts".into(),
                vars: std::collections::HashMap::new(),
                resume_lsn: Some(42),
                query_name: String::new(),
            },
        )),
    }
}

fn canonical_named_subscribe() -> proto::ClientMessage {
    let mut vars = std::collections::HashMap::new();
    vars.insert(
        "board_id".to_owned(),
        proto::VarValue {
            kind: Some(proto::var_value::Kind::StringValue(
                "00000000-0000-0000-0000-000000000042".into(),
            )),
        },
    );
    proto::ClientMessage {
        kind: Some(proto::client_message::Kind::Subscribe(
            proto::SubscribeRequest {
                client_subscription_id: "board".into(),
                sql: String::new(),
                vars,
                resume_lsn: None,
                query_name: "BoardCards".into(),
            },
        )),
    }
}

fn canonical_accepted() -> proto::ServerMessage {
    proto::ServerMessage {
        kind: Some(proto::server_message::Kind::Accepted(proto::Accepted {
            subscription_id: "posts".into(),
            schema_id: 7,
            snapshot_lsn: 100,
            schema: Some(posts_schema()),
        })),
    }
}

fn canonical_diff_payload() -> Vec<u8> {
    let rows = vec![
        vec![WireDatum::I64(1), WireDatum::Text(b"hello".to_vec())],
        vec![WireDatum::I64(2), WireDatum::Null],
    ];
    encode_rows(&rows).expect("encode rows")
}

fn canonical_diff() -> proto::ServerMessage {
    proto::ServerMessage {
        kind: Some(proto::server_message::Kind::Diff(proto::Diff {
            subscription_id: "posts".into(),
            lsn: 101,
            op: proto::DiffOp::Insert.into(),
            schema_id: 7,
            rows: canonical_diff_payload(),
        })),
    }
}

fn canonical_resync() -> proto::ServerMessage {
    proto::ServerMessage {
        kind: Some(proto::server_message::Kind::Resync(proto::Resync {
            subscription_id: "posts".into(),
            reason: proto::ResyncReason::LsnCompacted.into(),
            message: "fell off the trace".into(),
        })),
    }
}

// =====================================================================
// Captured byte fixtures.
//
// To regenerate, see the file-level docs and run with --nocapture.
// =====================================================================

const SUBSCRIBE_HEX: &str = "0a1f0a05706f737473121453454c4543542069642046524f4d20706f737473202a";

const NAMED_SUBSCRIBE_HEX: &str = concat!(
    "0a470a05626f6172641a320a08626f6172645f6964122622243030303030303030",
    "2d303030302d303030302d303030302d3030303030303030303034322a0a426f61",
    "72644361726473",
);

const ACCEPTED_HEX: &str =
    "0a250a05706f7374731007186422180a060a02696410040a0b0a057469746c6510081801120100";

const DIFF_HEX: &str = concat!(
    "12540a05706f7374731065180220072a45",
    "0200000000000000",
    "0200000000000000",
    "030000000100000000000000",
    "07000000",
    "050000000000000068656c6c6f",
    "0200000000000000",
    "0300000002000000000000000c000000",
);

const RESYNC_HEX: &str = "1a1d0a05706f73747310011a1266656c6c206f666620746865207472616365";

const ROW_PAYLOAD_HEX: &str = concat!(
    "0200000000000000",
    "0200000000000000",
    "030000000100000000000000",
    "07000000",
    "050000000000000068656c6c6f",
    "0200000000000000",
    "0300000002000000000000000c000000",
);

#[test]
fn client_message_subscribe_matches_fixture() {
    let actual = canonical_subscribe().encode_to_vec();
    let expected = unhex(&strip_ws(SUBSCRIBE_HEX));
    assert_eq!(
        hex(&actual),
        hex(&expected),
        "ClientMessage::Subscribe wire format drift"
    );
}

#[test]
fn client_message_named_subscribe_matches_fixture() {
    let actual = canonical_named_subscribe().encode_to_vec();
    let expected = unhex(&strip_ws(NAMED_SUBSCRIBE_HEX));
    assert_eq!(
        hex(&actual),
        hex(&expected),
        "ClientMessage::Subscribe (named) wire format drift"
    );
}

#[test]
fn named_subscribe_round_trips_name_and_params() {
    let bytes = canonical_named_subscribe().encode_to_vec();
    let decoded = proto::ClientMessage::decode(bytes.as_slice()).expect("decode");
    let Some(proto::client_message::Kind::Subscribe(subscribe)) = decoded.kind else {
        panic!("expected Subscribe");
    };
    assert_eq!(subscribe.query_name, "BoardCards");
    assert!(subscribe.sql.is_empty());
    let value = subscribe.vars.get("board_id").expect("param");
    assert!(matches!(
        &value.kind,
        Some(proto::var_value::Kind::StringValue(s))
            if s == "00000000-0000-0000-0000-000000000042"
    ));
}

#[test]
fn var_list_round_trips() {
    let list = proto::VarValue {
        kind: Some(proto::var_value::Kind::ListValue(proto::VarList {
            values: vec![
                proto::VarValue {
                    kind: Some(proto::var_value::Kind::IntValue(1)),
                },
                proto::VarValue {
                    kind: Some(proto::var_value::Kind::IntValue(2)),
                },
            ],
        })),
    };
    let bytes = list.encode_to_vec();
    let decoded = proto::VarValue::decode(bytes.as_slice()).expect("decode");
    let Some(proto::var_value::Kind::ListValue(decoded_list)) = decoded.kind else {
        panic!("expected ListValue");
    };
    assert_eq!(decoded_list.values.len(), 2);
}

#[test]
fn server_message_accepted_matches_fixture() {
    let actual = canonical_accepted().encode_to_vec();
    let expected = unhex(&strip_ws(ACCEPTED_HEX));
    assert_eq!(
        hex(&actual),
        hex(&expected),
        "ServerMessage::Accepted wire format drift"
    );
}

#[test]
fn server_message_diff_matches_fixture() {
    let actual = canonical_diff().encode_to_vec();
    let expected = unhex(&strip_ws(DIFF_HEX));
    assert_eq!(
        hex(&actual),
        hex(&expected),
        "ServerMessage::Diff wire format drift"
    );
}

#[test]
fn server_message_resync_matches_fixture() {
    let actual = canonical_resync().encode_to_vec();
    let expected = unhex(&strip_ws(RESYNC_HEX));
    assert_eq!(
        hex(&actual),
        hex(&expected),
        "ServerMessage::Resync wire format drift"
    );
}

#[test]
fn row_payload_matches_fixture() {
    let actual = canonical_diff_payload();
    let expected = unhex(&strip_ws(ROW_PAYLOAD_HEX));
    assert_eq!(
        hex(&actual),
        hex(&expected),
        "Row payload (bincode) wire format drift"
    );
}

#[test]
fn fixtures_round_trip_through_decode() {
    let bytes = unhex(&strip_ws(DIFF_HEX));
    let decoded = proto::ServerMessage::decode(bytes.as_slice()).expect("decode");
    let proto::server_message::Kind::Diff(diff) = decoded.kind.expect("kind") else {
        panic!("expected Diff");
    };

    let mut registry = SchemaRegistry::new();
    registry.register(7, posts_schema());
    let rows = registry.decode(&diff).expect("decode diff");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], WireDatum::I64(1));
    assert_eq!(rows[1][1], WireDatum::Null);

    // decode_diff direct path
    let rows = decode_diff(&diff, &posts_schema()).expect("decode");
    assert_eq!(rows.len(), 2);
}

fn strip_ws(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

// ---------------------------------------------------------------------
// Regeneration helpers — run with --nocapture to print fresh hex.
// ---------------------------------------------------------------------

#[test]
fn print_fixtures() {
    if std::env::var_os("PALIMPSEST_PRINT_FIXTURES").is_none() {
        return;
    }
    println!(
        "SUBSCRIBE   = {}",
        hex(&canonical_subscribe().encode_to_vec())
    );
    println!(
        "NAMED_SUB   = {}",
        hex(&canonical_named_subscribe().encode_to_vec())
    );
    println!(
        "ACCEPTED    = {}",
        hex(&canonical_accepted().encode_to_vec())
    );
    println!("DIFF        = {}", hex(&canonical_diff().encode_to_vec()));
    println!("RESYNC      = {}", hex(&canonical_resync().encode_to_vec()));
    println!("ROW_PAYLOAD = {}", hex(&canonical_diff_payload()));
}
