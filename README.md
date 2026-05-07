# Palimpsest

Palimpsest is a Rust service for maintaining live SQL query result sets from a
Postgres logical replication stream and pushing row-level diffs to clients.

The current repository is in scaffold phase. The architecture and implementation
backlog live in [DESIGN.md](DESIGN.md).

## Workspace

| Path | Purpose |
| --- | --- |
| `crates/palimpsest-proto` | Shared gRPC and wire protocol types generated from protobuf definitions. |
| `xtask` | Project-specific maintenance commands such as fixture regeneration and size checks. |

## Development

```sh
cargo fmt --all --check
cargo build --all-features
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo deny check
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
