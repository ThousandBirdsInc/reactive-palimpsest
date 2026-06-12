# Palimpsest PaaS Operator UI Design

**Status:** Redesigned (v2); updated to match shipped console (Sync,
Permissions, SQL console, and Clones pages have landed since the v2 rewrite).
**Scope:** Routed multi-page console for operating the Palimpsest PaaS
control plane, with a path to first-class live updates via Palimpsest itself.

## 1. Purpose

The PaaS UI is the browser surface for the managed platform. It is an
**operator console first**, with developer and support workflows folded in as
secondary modes. The user opening it most often is mid-incident, mid-migration,
or about to take an action with blast radius — the UI should optimize for
*finding the thing*, *understanding its state right now*, and *acting on it
with attribution*.

The previous (v1) UI was a single 1680px page that put every panel on the home
screen. That model held until the control-plane API surface grew past ~10
endpoints; today the control plane exposes ~80 endpoints across backups, PITR,
failovers, standbys, certificates/ACME, runtime checks, audit, secrets, SSO,
quota policies, maintenance windows, domains, IP allowlists, and more — none
of which had a place to live. This redesign gives them one.

## 2. Primary Users

- **Operators on-call.** Need health, incidents, capacity, audit trail in seconds.
- **Developers.** Provisioning a managed Postgres, connecting to it, running
  queries through the SQL console, rotating roles.
- **Support engineers.** Read-only investigation of a customer environment
  under a redaction policy, with support-access-session enforcement.

These overlap heavily, so the UI is **one app with shared IA**, not three
"modes." Permissions and a top-bar scope picker narrow what each user sees.

## 3. Information Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Palimpsest │  [org ▾]  [project ▾]  [env ▾]    ⌘K   ● live   alice ▾   │
├──────────┬───────────────────────────────────────────────────────────────┤
│ Overview │                                                               │
│ Clusters │   Routed page contents (per nav item)                         │
│ Hosts    │                                                               │
│ Incidents│                                                               │
│ Quota    │                                                               │
│ Routes   │                                                               │
│ Sync     │                                                               │
│ Perms    │                                                               │
│ Audit    │                                                               │
│ Settings │                                                               │
└──────────┴───────────────────────────────────────────────────────────────┘
```

**Top bar (always visible)**

- `org / project / env` pickers, populated from `/v1/organizations`,
  `/v1/projects?organization_id=…`, `/v1/environments?project_id=…`. The
  current scope is the source of truth — every nav page reads from it.
- Connection-status pill: shows the live-data transport state (Palimpsest
  connected / reconnecting / closed, or "polling" when REST-only).
- Actor display: pulled from the API key / session, not free-typed.
- ⌘K command palette: navigate, jump to cluster by id, run common ops.

**Sidebar nav (collapsible)**

| Route prefix | Page | Primary data |
|---|---|---|
| `/` | Overview | env health, counts, recent audit, incidents, capacity |
| `/clusters` | Clusters list | `managed_postgres_clusters` |
| `/clusters/:id` | Cluster detail (tabs) | one cluster + its SQL console, clones, backups, PITR, roles, operations, audit |
| `/hosts` | Hosts | `node_hosts`, capacity, hardening checks, observed clusters/deployments |
| `/incidents` | Incidents | `incidents` |
| `/quota` | Quota | `quota_policies` + `quota_alerts` |
| `/routes` | Routes | `gateway_routes` + `database_proxy_routes` + `domains` |
| `/sync-deployments` | Sync | `sync_deployments` (deploy state + history) |
| `/permissions` | Permissions | `permission_rule_documents` (Rule DSL) + `query_permission_policies` |
| `/audit` | Audit log | `audit_events` (filterable) |
| `/settings` | Settings | api keys, sso, webhooks, secret-encryption-keys |

The nav order itself is owned by `src/shell/nav.ts` (`NAV_ITEMS`), and the
cluster-detail tab order by `CLUSTER_TABS` in the same file, so the shell,
command palette, and pages stay in sync from one source.

The **Cluster detail** tabs are: Overview, **SQL** (read-only console +
schema browser), **Clones** (database-clone / restore-drill history), Backups,
PITR, Roles, Operations, Audit.

The **Permissions** page hosts two related-but-distinct models as tabs:

- **Rule DSL** — a single `palimpsest-permissions` TOML document per
  environment, edited in a syntax-highlighted code editor and run through the
  verifier (`POST /v1/permissions/verify`), which compiles the rules against a
  catalog (a built-in demo catalog or a live cluster schema) and reports
  per-rule compilation, user-context fields, and tautology elision. The
  document is persisted independently of verification via the
  permission-rule-document API; verification is an explicit author-time check,
  not yet a save-time gate.
- **Query policies** — individual per-table read/subscribe predicates stored as
  `query_permission_policies` rows, each with a draft/active status and a
  dry-run check against a sample JSON user context.

Each detail page owns the entire vertical slice for that resource. Today's v1
"resource panels" become **summary widgets on Overview that link into their
respective detail pages** — no more rows that link nowhere.

## 4. Density & Visual System

The v1 doc claimed "dense, operational." The v1 implementation used hero
numbers (24px), 118px tile heights, and 220px panel minimums. v2 actually
ships density.

- **Base font:** 13px. Tables: 12.5px. IDs and LSNs: 12px monospace (ui-monospace).
- **Row height:** 28–32px in tables. No min-heights on panels.
- **Color is reserved for state**, not chrome:
  - green: `ready`, `healthy`, `active`, `succeeded`
  - amber: `degraded`, `reconciling`, `running`, `pending`
  - red: `failed`, `firing`, `unhealthy`, `deleting`
  - neutral: everything else (do not invent a tone for unknown states)
- **State pill** rendering is centralized; the mapping from state-string to
  tone lives in one place (`src/lib/state.ts`). Stop adding string literals to
  it — instead, ask the API to use one of the four buckets.
- **Layout grid:** 12-column at the page level; sidebar is a fixed 200px;
  topbar 48px. No responsive collapse below 1024px (this is a desktop console).
- **Icons:** `lucide-react` already in deps. Use 14–16px in inline contexts.
- **No marketing surfaces:** no hero sections, no decorative cards, no
  background gradients, no animation beyond Tailwind-style transition on
  hover/focus.

## 5. Data layer

The data layer is split so live updates can move from polling to Palimpsest
subscriptions per-resource, without touching the UI components.

```
                ┌──────────────────────────────────┐
                │  React component                 │
                │  const { rows } = usePaasResource│
                └──────────────┬───────────────────┘
                               │
                ┌──────────────▼───────────────────┐
                │  usePaasResource("clusters", …) │
                │  Picks live source per resource │
                └──────────┬───────────────┬───────┘
                           │               │
                ┌──────────▼────┐  ┌───────▼─────────────┐
                │ Polling       │  │ Palimpsest          │
                │ LiveSource    │  │ LiveSource          │
                │ (REST + poll) │  │ (usePalimpsest      │
                │               │  │  Subscription)      │
                └───────────────┘  └─────────────────────┘
```

- **PaasApi** (`src/lib/api.ts`): thin REST client for one-shot reads and
  mutations. Mutations send `x-actor-id`.
- **PollingLiveSource** (`src/lib/live/polling.ts`): default for every
  resource. Re-fetches per-panel on a configurable interval (default 10s),
  with manual refresh and `revalidate-on-focus`.
- **PalimpsestLiveSource** (`src/lib/live/palimpsest.ts`): opt-in. When the
  control plane is fronted by a Palimpsest server, set
  `VITE_PAAS_PALIMPSEST_URL` and the SQL map in `src/lib/live/sql.ts` — the
  resource transparently switches to push-based subscriptions via
  `@palimpsest/client/react`.
- **`usePaasResource(name, scope)`**: the only thing components call.
  Components don't know whether the data came from REST or Palimpsest.

This keeps the redesign useful **today** (REST works out of the box) while
opening the door to the dogfood story below.

## 6. The dogfood path

The PaaS UI should — eventually — be a Palimpsest client. Today the control
plane writes to its own Postgres (`palimpsest_control`) and exposes REST.
To close the loop:

1. Stand up a `palimpsest-server` instance in front of `palimpsest_control`.
2. Define SQL views the UI can subscribe to (one per resource, e.g.
   `SELECT * FROM managed_postgres_clusters WHERE environment_id = $env`).
3. Register the UI as a Palimpsest client; flip `PalimpsestLiveSource` on
   per-resource by populating `src/lib/live/sql.ts`.
4. The UI now receives push updates with sub-second latency, eats its own
   dogfood, and proves the operator-console use case for the platform.

The redesign is shaped to make step 3 a one-file change per resource.

## 7. API contract (used today)

Default local API base: `/api`, proxied by Vite to `http://127.0.0.1:18088`.

REST endpoints the UI reads:

- `GET /v1/organizations`, `/v1/projects`, `/v1/environments` — scope pickers
- `GET /v1/environments/{id}/overview` — Overview page bootstrap
- `GET /v1/managed-postgres/clusters?environment_id=…`
- `GET /v1/managed-postgres/clusters/{id}` (+ `/operations`, `/backups`,
  `/pitr-checks`, `/failovers`, `/standbys`, `/restores`, `/restore-drills`,
  `/agent-commands`, `/runtime-checks`)
- `GET /v1/managed-postgres/clusters/{id}/schema` and
  `POST /v1/managed-postgres/clusters/{id}/sql-console/query` — SQL tab
- `POST /v1/managed-postgres/clusters/{id}/database-clones` — Clones tab
- `GET /v1/node-hosts` (+ `/hardening-checks`, `/commands`) — host detail also
  surfaces observed clusters/sync-deployments reported on heartbeat
- `GET /v1/sync-deployments?environment_id=…`
- `GET`/`PUT /v1/environments/{env}/permission-rule-document` and
  `POST /v1/permissions/verify` — Permissions · Rule DSL
- `GET /v1/query-permission-policies?environment_id=…`,
  `POST /v1/query-permission-policies`, and
  `POST /v1/query-permission-policies/{id}/dry-run` — Permissions · Query policies
- `GET /v1/incidents?environment_id=…`
- `GET /v1/quota-policies`, `/v1/quota-alerts?environment_id=…`
- `GET /v1/gateway-routes?environment_id=…`, `/v1/database-proxy-routes?environment_id=…`,
  `/v1/domains`
- `GET /v1/audit-events?…` — Audit page + Overview recent-audit widget
- `GET /metrics` — Prometheus text for header indicators

Mutations (POST) for managed-postgres lifecycle, role rotation, backups,
restores, resize, pause/resume, delete, failovers, support-access-session
approve/revoke. All carry `x-actor-id` from the session.

## 8. Out-of-scope for this redesign (tracked for follow-up)

- Authentication / login. The UI today assumes the API is reachable; the
  session and `x-actor-id` are stubbed. Wire up API-key login + actor display
  before going past internal use.
- Certificate/ACME workflow UI.
- Form flows for backup retention, IP allowlist, maintenance windows,
  static egress, domains.
- Prometheus-backed metric charts (the `/metrics` scrape today gives one
  scalar at a time; real charts need a metrics backend).
- Mobile / narrow viewport: explicitly out of scope. Console is desktop.

### Shipped since the v2 redesign (no longer out of scope)

- **Schema browser + read-only SQL console** as the Cluster detail **SQL** tab,
  over `/schema` and `/sql-console/query`.
- **Database clones / restore-drill history** as the Cluster detail **Clones**
  tab.
- **Permissions** page (Rule DSL editor + verifier, and Query policies with
  dry-run), backed by a shared `CodeEditor` component (`components/CodeEditor.tsx`)
  with lightweight TOML/SQL highlighting in `lib/highlight.ts`.
- **Sync** page for SyncDeployment state and history.

The query-explorer `inspect` endpoint exists on the backend but is not yet a
dedicated UI surface.

## 9. Local development

```sh
cd paas/ui
npm install
npm run dev -- --host 127.0.0.1
```

Run the SQL control plane separately:

```sh
cargo run -p palimpsest-paas-control-plane -- serve-sql-api \
  127.0.0.1:18088 \
  postgres://palimpsest_control:palimpsest_control@127.0.0.1:54330/palimpsest_control
```

The UI renders without a live control plane (empty states everywhere), but
data and actions require the API.

## 10. File layout

```
paas/ui/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
└── src/
    ├── main.tsx                 // mounts <BrowserRouter><App/></BrowserRouter>
    ├── App.tsx                  // routes + AppShell wrapper
    ├── styles.css               // design tokens + density rules
    ├── shell/
    │   ├── AppShell.tsx         // sidebar + topbar + <Outlet/>
    │   ├── ScopePicker.tsx
    │   ├── CommandPalette.tsx
    │   └── nav.ts               // NAV_ITEMS + CLUSTER_TABS (single source)
    ├── lib/
    │   ├── api.ts               // PaasApi (one-shot reads + mutations)
    │   ├── scope.ts             // useScope() context
    │   ├── state.ts             // stateTone() + status labels
    │   ├── format.ts            // bytes, durations, relative time
    │   ├── highlight.ts         // highlightToml() / highlightSql()
    │   └── live/
    │       ├── index.ts         // usePaasResource()
    │       ├── polling.ts       // PollingLiveSource
    │       ├── palimpsest.ts    // PalimpsestLiveSource (opt-in)
    │       └── sql.ts           // resource → SQL map for Palimpsest path
    ├── pages/
    │   ├── Overview.tsx
    │   ├── ClustersList.tsx
    │   ├── ClusterDetail.tsx    // tabs: Overview/SQL/Clones/Backups/PITR/Roles/Operations/Audit
    │   ├── ClusterConsole.tsx   // SQL tab: schema browser + read-only console
    │   ├── ClusterClones.tsx    // Clones tab: clone / restore-drill history
    │   ├── Hosts.tsx
    │   ├── Incidents.tsx
    │   ├── Quota.tsx
    │   ├── Routes.tsx
    │   ├── SyncDeployments.tsx
    │   ├── Permissions.tsx      // tabs: Rule DSL (verifier) + Query policies
    │   ├── Audit.tsx
    │   └── Settings.tsx
    ├── components/
    │   ├── DataTable.tsx
    │   ├── StatePill.tsx
    │   ├── PageHeader.tsx
    │   ├── Empty.tsx
    │   ├── CodeEditor.tsx       // highlighted textarea for TOML/SQL editing
    │   └── ConfirmButton.tsx    // wraps destructive actions
    └── types.ts
```
