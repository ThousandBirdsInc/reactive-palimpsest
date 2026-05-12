# Wire-protocol versioning policy

Status: **stable, additive-only within a major version.**

This document is the binding contract for changes to:

- `proto/palimpsest/sync/v1/sync.proto`
- the manual `Row` codec in `src/wire.rs`
- the constants exported from `src/version.rs`

The Palimpsest server and every official client (browser, native, CLI)
share this single proto definition. Long-lived subscriptions hold open
streams that may outlast multiple server deploys, so wire compatibility
is *not* optional.

## Versioning constants

The current major version is exposed as

```rust
palimpsest_proto::WIRE_PROTOCOL_VERSION   // "v1"
palimpsest_proto::WIRE_PROTOCOL_PACKAGE   // "palimpsest.sync.v1"
```

Both ride the proto package and the Rust module path
(`palimpsest::sync::v1`). Bumping either constant is a major change and
follows the [Major bump (`v2/`)](#major-bump-v2) section below.

## Allowed changes within `v1`

The following are all backwards-compatible and can land in any release:

- **Adding a new field** to an existing message. Pick a fresh tag number
  larger than any existing tag in that message; never re-use a number
  freed by [removal](#disallowed-changes-within-v1).
- **Adding a new variant** to a `oneof`. Existing peers will see it as
  `None` (unset) and route to the default arm.
- **Adding a new enum value** at the *end* of the enum. Existing peers
  decode it as the unspecified default.
- **Appending a new variant** to [`WireDatum`](src/wire.rs). New
  variants must go at the *end* — bincode tags are positional, so any
  reordering breaks every previously-serialized payload.
- **Adding a new RPC** to the `SyncEngine` service.
- Doc comments, formatting, and renames of generated Rust identifiers
  that do not change the proto on the wire.

Every additive change must come with:

1. An updated entry (or a new file) in `tests/wire_fixtures.rs`
   anchoring the bytes for at least one canonical example.
2. The matching server- and client-side code that handles the new
   field/variant gracefully when the peer omits it.

## Disallowed changes within `v1`

- **Renumbering** existing fields, removing fields, or repurposing a
  freed tag number for a different type.
- **Reordering** a `oneof`'s arms. The arm tag numbers are stable, but
  re-ordering invites mistakes and obscures diffs.
- **Reordering** an existing enum's values, or changing the numeric
  value of an existing variant.
- **Reordering** [`WireDatum`](src/wire.rs) variants — bincode would
  re-tag every previously-encoded payload.
- Changing field types (`uint64` → `int64`, `bytes` → `string`, etc.).
  These are silent corruptions on the wire.
- Changing the bincode encoding configuration (varints,
  little/big-endian, length prefix). The codec uses the bincode default
  config; that choice is part of the contract.
- Adding required validation that rejects payloads previously accepted.

If a change is required that violates any of the above, it is a major
bump.

## Major bump (`v2/`)

A major bump means the wire is no longer backwards-compatible. The
process:

1. Add `proto/palimpsest/sync/v2/sync.proto` (copy v1, then mutate).
2. Generate it under `crate::palimpsest::sync::v2`.
3. Update `WIRE_PROTOCOL_VERSION` to `"v2"` (and
   `WIRE_PROTOCOL_PACKAGE` to `"palimpsest.sync.v2"`).
4. The server registers both `SyncEngine` services side-by-side for an
   overlap window.
5. Clients connecting to the old (`v1`) service receive a synthetic
   `Resync { reason: SCHEMA_CHANGED, message: "wire-version: please
   reconnect on v2" }` and disconnect.
6. After the deprecation window the v1 service is unregistered and
   `proto/palimpsest/sync/v1/` is deleted.

In short: **every breaking change forces every connected client to
re-subscribe.** The v1 path stays alive only long enough for clients to
upgrade.

## Field reservations

When a field *must* be removed (e.g. it was a misdesign caught before
GA), reserve its tag number so it can never be re-used:

```proto
message Diff {
  reserved 9;
  reserved "old_field_name";
  // ... existing fields ...
}
```

`tonic-build` propagates `reserved` and `protoc` will fail any future
attempt to re-allocate the tag.

## Reviewer checklist

Wire-protocol PRs must answer all of:

- [ ] Does this change touch any existing field/variant tag number, or
      only add new ones?
- [ ] Are `WireDatum` variants append-only?
- [ ] Are the captured fixtures in `tests/wire_fixtures.rs` updated to
      cover the new shape?
- [ ] Does the server tolerate peers that still send the *old* shape?
- [ ] Does the native client tolerate servers that still emit the old
      shape (forward compat)?

If any answer is "no", this is a major bump.
