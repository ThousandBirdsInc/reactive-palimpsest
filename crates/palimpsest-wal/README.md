# palimpsest-wal

WAL ingest for Palimpsest.

This crate owns the typed boundary between pgoutput row data and the rest of
the engine. pgoutput tuple fields arrive as text, binary, null, or TOAST
unchanged markers; the decoder maps Postgres type OIDs to `DatumType` once and
then emits `Datum` values inside a `Tuple`.

## pgoutput message reference

| Tag | Message | Phase 1 handling |
| --- | --- | --- |
| `B` | Begin | Decodes transaction id and final LSN. |
| `C` | Commit | Decodes commit and end LSN. |
| `R` | Relation | Refreshes table schema metadata. |
| `I` | Insert | Decodes a new tuple. |
| `U` | Update | Decodes optional old tuple plus new tuple. |
| `D` | Delete | Decodes old tuple. |
| `T` | Truncate | Decodes affected relation ids. |
| `Y` | Type | Reserved for domain/type metadata. |
| `O` | Origin | Parsed as replication metadata. |
| `M` | Logical message | Surfaced as an unsupported protocol message. |
| `S` | Stream start | Starts streamed transaction buffering. |
| `E` | Stream stop | Closes the current streamed segment. |
| `c` | Stream commit | Replays buffered streamed segments. |
| `A` | Stream abort | Drops buffered streamed segments. |
| `b` | Begin prepare | Decodes two-phase transaction prepare start. |
| `P` | Prepare | Decodes two-phase prepare completion. |
| `K` | Commit prepared | Decodes two-phase commit completion. |
| `r` | Rollback prepared | Decodes two-phase rollback completion. |
