# PaaS extraction reference

The managed-platform (PaaS) code, designs, and deployment assets were removed
from this repository. This engine repo is now the sync engine, its clients, and
its docs — nothing else. The PaaS is to be reconstituted in its own repository,
and this note records exactly where to pull it from.

## Reference commit

The last commit on `main` where the PaaS still existed in full:

```
46cd0dc175117b7746c69a16a086724542d85e51
```

- Date: 2026-08-01
- Subject: `Fix CI (nightly clippy + coverage), remove dead code, add paas cleanup runbooks (#6)`
- PR: #6

Everything listed below is present and intact at that commit. Nothing was
modified before deletion; the removal commit is a pure delete plus the
follow-on cleanup of references from the surviving engine code.

## What was removed

### `paas/` (250 files)

| Path | Contents |
|---|---|
| `paas/README.md` | Entry point and current state of the PaaS implementation. |
| `paas/IMPLEMENTATION-PLAN.md` | Phase-by-phase build plan with per-deliverable status annotations. |
| `paas/MANAGED-POSTGRES-DESIGN.md` | PostgreSQL 18+ lifecycle management design. |
| `paas/PRODUCTION-READINESS-DESIGN.md` | Readiness checklist, control loops, launch blockers. |
| `paas/BRANCHING-API-DESIGN.md` | Copy-on-write and point-in-time database branching API. |
| `paas/PAAS-UI-DESIGN.md` | Routed multi-page operator console design. |
| `paas/adr/` | ADRs 0001–0003 (Postgres version floor, owned Rust runtime, CloudNativePG runtime). |
| `paas/ci/` | JSON schema/example validation script. |
| `paas/control-plane/` | Flyway config, compose file, and the `V*.sql` migration set. |
| `paas/crates/` | The five Rust crates listed below. |
| `paas/deploy/` | PaaS Helm chart. |
| `paas/examples/` | Control-plane request/response examples. |
| `paas/local/` | Local PostgreSQL 18 + Palimpsest docker-compose dev stack, config, migrations, seeds. |
| `paas/observability/` | Alert rules and dashboards. |
| `paas/specs/` | JSON schemas for control-plane payloads. |
| `paas/ui/` | Operator console (source + built `dist/`). |

### Rust crates (were workspace members)

- `palimpsest-paas-control-plane`
- `palimpsest-paas-core`
- `palimpsest-paas-gateway`
- `palimpsest-paas-runtime`
- `palimpsest-paas-sync-wrapper`

### Docs under `docs/`

- `docs/PAAS-DESIGN.md` — top-level product/systems design for managed Postgres + live sync.
- `docs/PAAS-HOST-AGENT-REMOVAL.md` — staged runbook for removing the obsolete host-agent / node-command subsystem.
- `docs/PAAS-SYNC-DEPLOYMENT-DISPOSITION.md` — decision + runbook for the orphaned `palimpsest-paas-sync-wrapper` / SyncDeployment feature.

### Build and CI assets

- `Dockerfile.paas`, `Dockerfile.paas-migrations`
- `.github/workflows/paas.yml`
- `mise.toml` (only ever held the `paas:up` / `paas:down` Tilt tasks)

### Engine-side references cleaned up

These files stayed, with their PaaS-facing pieces removed:

- `crates/palimpsest-cli` — dropped the `palimpsest-paas-core` dependency and
  the `dev up|down|reset|status|env` and `db create|psql|branch ...` subcommand
  trees, along with their hand-rolled HTTP client, the `DevStack` / `Db` /
  `ControlPlane` error variants and their exit-code mappings, the matching
  tests, and the PaaS lines in the embedded agent skill and README.
  Exit codes are now `0` success, `2` usage, `1` everything else — the
  `3`/`4`/`5`/`6`/`7` control-plane classes went with the `db` commands.
- `Cargo.toml` — dropped the five `paas/crates/*` workspace members.
- `Cargo.lock` — regenerated.
- `.github/workflows/coverage.yml` — dropped the per-crate PaaS excludes.
- `deny.toml` — dropped the `RUSTSEC-2025-0134` (`rustls-pemfile`) ignore,
  which existed only for the PaaS TLS stack and no longer resolves.
- `publish.sh` — dropped `palimpsest-paas-core` from the publish set.
- `README.md`, `docs/README.md`, `docs/ARCHITECTURE.md` — dropped PaaS rows,
  sections, and links.

## Recovering the code

Read a single file without checking anything out:

```sh
git show 46cd0dc175117b7746c69a16a086724542d85e51:paas/README.md
```

Restore the whole tree into a working directory:

```sh
git checkout 46cd0dc175117b7746c69a16a086724542d85e51 -- paas docs/PAAS-DESIGN.md
```

Extract `paas/` with its full history into a standalone repo (requires
[`git-filter-repo`](https://github.com/newren/git-filter-repo)):

```sh
git clone <this-repo> palimpsest-paas
cd palimpsest-paas
git checkout 46cd0dc175117b7746c69a16a086724542d85e51
git filter-repo --path paas/ --path docs/PAAS-DESIGN.md \
                --path Dockerfile.paas --path Dockerfile.paas-migrations \
                --path-rename paas/:
```

## Notes for the new repo

- The PaaS crates depended on `palimpsest-paas-core` by path within this
  workspace, and `palimpsest-cli` depended on it too. In a standalone repo
  those become intra-workspace paths again; the CLI dependency is gone
  entirely and does not need to be recreated unless the `dev`/`db` commands
  are wanted there.
- `paas/local/docker-compose.yaml` was the target of `palimpsest dev up` and
  of the `PALIMPSEST_PAAS_COMPOSE_FILE` override. Any replacement CLI in the
  new repo needs to resolve that path relative to its own root.
- The coverage floor in `.github/workflows/coverage.yml` was set with the PaaS
  crates excluded, so the surviving number carried over unchanged.
