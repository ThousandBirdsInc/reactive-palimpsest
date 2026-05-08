# Palimpsest pg-walstream Fork

This directory vendors the `pg-walstream` source referenced by the design doc.
It is kept as upstream fork material rather than as the compiled
`palimpsest-wal` implementation.

Palimpsest-specific changes made at vendoring time:

- Removed the libpq backend and `libpq-sys` feature path.
- Made the pure Rust `rustls-tls` backend the default in the vendored
  manifest.
- Removed integration-test manifest entries that point outside this vendored
  source snapshot.

The compiled crate in `../../src` owns the current Phase 1 API:

- `Tuple = SmallVec<[Datum; 8]>`
- `Datum::Unchanged` for TOAST unchanged markers
- catalog-aware OID to `DatumType` decoding
- backpressure spill handling
