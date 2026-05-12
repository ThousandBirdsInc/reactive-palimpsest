# Fuzz targets

Per §15.5 / §18.12, three nightly fuzz targets:

- `pgoutput_decoder` — bytes → `decode_pgoutput_message` should never panic.
- `sql_parser` — UTF-8 → `parse_select` + `parse_and_lower` should never panic.
- `wire_decoder` — bytes → `decode_rows`; on success, encode and decode again must round-trip.

## Running locally

```bash
cargo +nightly install cargo-fuzz --locked
cargo +nightly fuzz run pgoutput_decoder -- -max_total_time=60
cargo +nightly fuzz run sql_parser       -- -max_total_time=60
cargo +nightly fuzz run wire_decoder     -- -max_total_time=60
```

Crash artifacts land in `fuzz/artifacts/<target>/`. Replay a specific
crash with:

```bash
cargo +nightly fuzz run sql_parser fuzz/artifacts/sql_parser/crash-...
```

The nightly GitHub Actions workflow at `.github/workflows/fuzz.yml`
runs each target for 30 minutes and auto-files a P1 issue with the
seed bytes if anything crashes.

## Corpus

Hand-seeded inputs live under `corpus/<target>/`. Add new seeds when
you find an interesting input you want preserved across runs (libFuzzer
will continue to mutate them).
