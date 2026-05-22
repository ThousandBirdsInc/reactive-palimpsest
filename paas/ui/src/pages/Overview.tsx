import { useCallback, useMemo } from "react";
import { Link } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi, useScope } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { metricValue } from "../lib/api";
import { formatRelative } from "../lib/format";
import { stateTone } from "../lib/state";
import type {
  AuditEvent,
  EnvironmentHealth,
  EnvironmentOverview,
  Incident,
  ManagedPostgresCluster,
  NodeHost,
  QuotaAlert,
} from "../types";

export function Overview() {
  const scope = useScope();
  const api = useApi();
  const env = scope.environmentId;

  const overview = usePaasResource<EnvironmentOverview | null>(
    useCallback(() => api.getOverview(), [api]),
    null,
    { deps: [env] },
  );
  const clusters = usePaasResource<ManagedPostgresCluster[]>(
    useCallback(() => api.listClusters(), [api]),
    [],
    { deps: [env] },
  );
  const hosts = usePaasResource<NodeHost[]>(
    useCallback(() => api.listNodeHosts(), [api]),
    [],
  );
  const incidents = usePaasResource<Incident[]>(
    useCallback(() => api.listIncidents(), [api]),
    [],
    { deps: [env] },
  );
  const alerts = usePaasResource<QuotaAlert[]>(
    useCallback(() => api.listQuotaAlerts(), [api]),
    [],
    { deps: [env] },
  );
  const audit = usePaasResource<AuditEvent[]>(
    useCallback(() => api.listAuditEvents(20), [api]),
    [],
    { deps: [env] },
  );
  const metrics = usePaasResource<string>(
    useCallback(() => api.getMetricsText(), [api]),
    "",
    { intervalMs: 30_000 },
  );

  const refreshAll = () => {
    overview.refresh();
    clusters.refresh();
    hosts.refresh();
    incidents.refresh();
    alerts.refresh();
    audit.refresh();
    metrics.refresh();
  };

  const health = overview.data?.health;
  const firingAlerts = alerts.data.filter((a) => a.state === "firing");
  const activeIncidents = incidents.data.filter((i) => i.status !== "resolved");
  const failedAgentCommands = metricValue(
    metrics.data,
    "palimpsest_paas_agent_commands_failed_total",
  );
  const loading =
    overview.loading || clusters.loading || hosts.loading || incidents.loading;

  return (
    <div>
      <PageHeader
        title="Overview"
        subtitle={`environment ${env}`}
        onRefresh={refreshAll}
        loading={loading}
      />

      {overview.error && <div className="error-banner">{overview.error}</div>}

      <section className="overview-grid">
        <Kpi
          label="Health"
          value={health?.state ?? "unknown"}
          detail={health?.summary ?? "no health data"}
          tone={kpiTone(health?.state)}
        />
        <Kpi
          label="Clusters"
          value={String(clusters.data.length)}
          detail={`${clusters.data.filter((c) => c.lifecycle_state === "ready").length} ready`}
        />
        <Kpi
          label="Hosts"
          value={String(hosts.data.length)}
          detail={`${hosts.data.filter((h) => h.state === "active").length} active`}
        />
        <Kpi
          label="Firing alerts"
          value={String(firingAlerts.length)}
          detail={`${alerts.data.length} configured`}
          tone={firingAlerts.length ? "bad" : "good"}
        />
      </section>

      <section className="overview-cols">
        <div className="panel">
          <h2>Active incidents</h2>
          {activeIncidents.length === 0 ? (
            <Empty title="No active incidents" />
          ) : (
            <ul>
              {activeIncidents.slice(0, 6).map((i) => (
                <li key={i.incident_id} className="panel-row">
                  <Link to={`/incidents`} className="row-link">
                    {i.title}
                  </Link>
                  <span className="row">
                    <span className="muted">{i.severity}</span>
                    <StatePill state={i.status} />
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="panel">
          <h2>Capacity</h2>
          <div className="panel-row">
            <span className="panel-row-key">Storage in use</span>
            <span className="panel-row-value">
              {clusters.data.reduce((sum, c) => sum + c.storage_gib, 0)} GiB across{" "}
              {clusters.data.length} clusters
            </span>
          </div>
          <div className="panel-row">
            <span className="panel-row-key">Hosts active</span>
            <span className="panel-row-value">
              {hosts.data.filter((h) => h.state === "active").length} / {hosts.data.length}
            </span>
          </div>
          <div className="panel-row">
            <span className="panel-row-key">Agent failures</span>
            <span className="panel-row-value">
              {failedAgentCommands === null ? "—" : failedAgentCommands}
            </span>
          </div>
        </div>
      </section>

      <HealthComponentsPanel health={health ?? null} />

      <div className="panel" style={{ marginTop: 12 }}>
        <h2>Recent audit</h2>
        {audit.data.length === 0 ? (
          <Empty title="No audit events" hint={<Link to="/audit" className="row-link">Open audit log →</Link>} />
        ) : (
          <ul>
            {audit.data.slice(0, 8).map((e) => (
              <li key={e.event_id} className="panel-row">
                <span className="mono">{e.action}</span>
                <span className="row">
                  <span className="muted">{e.actor_id ?? "—"}</span>
                  <span className="muted">{formatRelative(e.occurred_at)}</span>
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

// ---------- health components ----------
//
// The API emits one component per (cluster × concern) plus one per sync
// deployment. Rendering that as a flat list shows the same concern name
// 3+ times with no context. Pivot into rows = resource (cluster_id or
// deployment_id), columns = concern. Each cell is a small colored dot
// with the full detail string in a tooltip. Concerns that don't apply
// to a given resource render as "—".

const CONCERN_ORDER = [
  "database",
  "storage",
  "backup",
  "restore_drill",
  "wal_archive",
  "pitr",
  "standby",
  "sync",
];

const CONCERN_LABEL: Record<string, string> = {
  database: "db",
  storage: "storage",
  backup: "backup",
  restore_drill: "drill",
  wal_archive: "wal",
  pitr: "pitr",
  standby: "stdby",
  sync: "sync",
};

function HealthComponentsPanel({ health }: { health: EnvironmentHealth | null }) {
  const pivot = useMemo(() => pivotHealthComponents(health?.components ?? []), [health]);

  return (
    <div className="panel" style={{ marginTop: 12 }}>
      <h2>Health components</h2>
      {pivot.concerns.length === 0 ? (
        <Empty title="No component data" />
      ) : (
        <div className="health-grid-wrap">
          <table className="health-grid">
            <thead>
              <tr>
                <th scope="col">Resource</th>
                {pivot.concerns.map((c) => (
                  <th key={c} scope="col" title={c}>
                    {CONCERN_LABEL[c] ?? c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pivot.rows.map((row) => (
                <tr key={row.resource}>
                  <th scope="row" className="mono health-resource">
                    {row.resource}
                  </th>
                  {pivot.concerns.map((c) => {
                    const cell = row.byConcern.get(c);
                    if (!cell) return <td key={c} className="health-cell empty">—</td>;
                    return (
                      <td key={c} className="health-cell" title={`${cell.state} · ${cell.detail}`}>
                        <span
                          className={`health-dot state-${stateTone(cell.state)}`}
                          aria-label={`${c}: ${cell.state}`}
                        />
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

interface HealthPivot {
  concerns: string[];
  rows: Array<{
    resource: string;
    byConcern: Map<string, { state: string; detail: string }>;
  }>;
}

function pivotHealthComponents(
  components: NonNullable<EnvironmentHealth["components"]>,
): HealthPivot {
  const byResource = new Map<string, Map<string, { state: string; detail: string }>>();
  const seenConcerns = new Set<string>();
  for (const c of components) {
    const resource = extractResourceId(c.detail) ?? "(environment)";
    const inner = byResource.get(resource) ?? new Map();
    inner.set(c.name, { state: String(c.state), detail: c.detail });
    byResource.set(resource, inner);
    seenConcerns.add(c.name);
  }
  const concerns: string[] = [];
  for (const name of CONCERN_ORDER) {
    if (seenConcerns.has(name)) {
      concerns.push(name);
      seenConcerns.delete(name);
    }
  }
  for (const name of Array.from(seenConcerns).sort()) {
    concerns.push(name);
  }
  const rows = Array.from(byResource.entries())
    .sort((a, b) => {
      if (a[0] === "(environment)") return -1;
      if (b[0] === "(environment)") return 1;
      return a[0].localeCompare(b[0]);
    })
    .map(([resource, byConcern]) => ({ resource, byConcern }));
  return { concerns, rows };
}

// All detail strings from the control plane follow one of two patterns
// (see sql_store.rs::*_health_for_cluster + sync_health_for_deployment):
//   "... cluster <id> ..."          → cluster id
//   "... SyncDeployment <id> ..."   → deployment id
// Fall back to null if neither matches (e.g. the bootstrap
// "environment has no managed Postgres cluster" message).
function extractResourceId(detail: string): string | null {
  const cluster = /cluster\s+([A-Za-z0-9_-]+)/.exec(detail);
  if (cluster) return cluster[1];
  const sync = /SyncDeployment\s+([A-Za-z0-9_-]+)/.exec(detail);
  if (sync) return sync[1];
  return null;
}

function Kpi({
  label,
  value,
  detail,
  tone,
}: {
  label: string;
  value: string;
  detail: string;
  tone?: "good" | "warn" | "bad";
}) {
  return (
    <div className={`kpi ${tone ?? ""}`}>
      <span className="kpi-label">{label}</span>
      <span className="kpi-value">{value}</span>
      <span className="kpi-detail">{detail}</span>
    </div>
  );
}

function kpiTone(state: string | undefined): "good" | "warn" | "bad" | undefined {
  if (state === "healthy") return "good";
  if (state === "degraded") return "warn";
  if (state === "unhealthy") return "bad";
  return undefined;
}
