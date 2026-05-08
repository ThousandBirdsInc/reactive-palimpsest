# palimpsest-dataflow fork notes

This crate vendors `differential-dataflow` as `palimpsest-dataflow` so
Palimpsest can adapt the execution engine for WAL-driven live query
maintenance, custom row containers, partial materialization, and
Postgres-specific state management.

Initial import:

- Upstream crate: `differential-dataflow`
- Upstream version: `0.18.0`
- Upstream license: MIT
- Local package: `palimpsest-dataflow`
- Local library crate: `palimpsest_dataflow`

The first fork patch keeps upstream behavior intact while moving the
crate onto the local package name and the newer timely/columnar dependency
line used by this workspace. The upstream README and changelog are kept
as `UPSTREAM-README.md` and `UPSTREAM-CHANGELOG.md`.
