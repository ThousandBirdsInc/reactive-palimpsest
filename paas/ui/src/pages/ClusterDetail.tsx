// Cluster detail page — five tabs around one cluster_id. Each tab owns its
// own data fetch so leaving the page tears one resource down at a time.

import { useCallback, useEffect, useState } from "react";
import { NavLink, Route, Routes, useParams } from "react-router-dom";
import { HardDrive, KeyRound, Pause, Play, RotateCcw } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { ConfirmButton } from "../components/ConfirmButton";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { formatGiB, formatRelative, formatTimestamp } from "../lib/format";
import { CLUSTER_TABS } from "../shell/nav";
import { ClusterConsoleTab } from "./ClusterConsole";
import { ClusterClonesTab } from "./ClusterClones";
import { ConnectionInfo } from "../components/ConnectionInfo";
import { useClusterConnection } from "../lib/connection";
import type {
  AuditEvent,
  ClusterBackup,
  ClusterOperation,
  ManagedPostgresCluster,
  PitrCheck,
} from "../types";

export function ClusterDetail() {
  const { clusterId = "" } = useParams<{ clusterId: string }>();
  const api = useApi();
  const cluster = usePaasResource<ManagedPostgresCluster | null>(
    useCallback(() => api.getCluster(clusterId), [api, clusterId]),
    null,
    { deps: [clusterId] },
  );
  const [opError, setOpError] = useState<string | null>(null);

  async function run(label: string, fn: () => Promise<unknown>) {
    setOpError(null);
    try {
      await fn();
      cluster.refresh();
    } catch (e) {
      setOpError(`${label}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  const c = cluster.data;
  return (
    <div>
      <PageHeader
        title="Cluster"
        subtitle={
          c
            ? `${c.region} · pg ${c.postgres_version} · ${c.tier}`
            : `loading ${clusterId}…`
        }
        onRefresh={cluster.refresh}
        loading={cluster.loading}
        actions={
          c ? (
            <div className="row">
              <button
                type="button"
                className="btn-secondary"
                onClick={() => run("reconcile", () => api.reconcileCluster(clusterId))}
              >
                <RotateCcw size={13} /> Reconcile
              </button>
              <button
                type="button"
                className="btn-secondary"
                onClick={() => run("pause", () => api.pauseCluster(clusterId))}
              >
                <Pause size={13} /> Pause
              </button>
              <button
                type="button"
                className="btn-secondary"
                onClick={() => run("resume", () => api.resumeCluster(clusterId))}
              >
                <Play size={13} /> Resume
              </button>
              <button
                type="button"
                className="btn-secondary"
                onClick={() => run("backup", () => api.runBackup(clusterId))}
              >
                <HardDrive size={13} /> Backup
              </button>
              <button
                type="button"
                className="btn-secondary"
                onClick={() => run("rotate", () => api.rotateRoles(clusterId))}
              >
                <KeyRound size={13} /> Rotate
              </button>
            </div>
          ) : null
        }
      />

      {opError && <div className="error-banner">{opError}</div>}
      {cluster.error && <div className="error-banner">{cluster.error}</div>}

      <div className="cluster-detail-header" style={{ marginBottom: 12 }}>
        <span className="cluster-id">{clusterId}</span>
        {c && <StatePill state={c.lifecycle_state} />}
        {c?.host_assignment && (
          <span className="muted">
            on <span className="mono">{c.host_assignment.host_id}</span>
            {" · "}port <span className="mono">{c.host_assignment.port}</span>
          </span>
        )}
      </div>

      <nav className="tabs">
        {CLUSTER_TABS.map((tab) => (
          <NavLink
            key={tab.slug}
            to={tab.slug ? `/clusters/${encodeURIComponent(clusterId)}/${tab.slug}` : `/clusters/${encodeURIComponent(clusterId)}`}
            end={!tab.slug}
            className={({ isActive }) => (isActive ? "tab active" : "tab")}
          >
            {tab.label}
          </NavLink>
        ))}
      </nav>

      <Routes>
        <Route index element={<ClusterOverviewTab cluster={c} onAction={run} />} />
        <Route path="sql" element={<ClusterConsoleTab clusterId={clusterId} />} />
        <Route path="clones" element={<ClusterClonesTab clusterId={clusterId} />} />
        <Route path="backups" element={<ClusterBackupsTab clusterId={clusterId} />} />
        <Route path="pitr" element={<ClusterPitrTab clusterId={clusterId} />} />
        <Route path="roles" element={<ClusterRolesTab onRotate={() => run("rotate", () => api.rotateRoles(clusterId))} />} />
        <Route path="operations" element={<ClusterOpsTab clusterId={clusterId} />} />
        <Route path="audit" element={<ClusterAuditTab clusterId={clusterId} />} />
      </Routes>
    </div>
  );
}

function ClusterOverviewTab({
  cluster,
  onAction,
}: {
  cluster: ManagedPostgresCluster | null;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
}) {
  const api = useApi();
  const [storageGib, setStorageGib] = useState<number>(cluster?.storage_gib ?? 20);
  // A final backup can only be taken when the cluster is fully provisioned
  // and ready — see sql_api_delete_managed_postgres_cluster, which rejects
  // both unassigned clusters and any lifecycle_state ≠ Ready when
  // final_backup=true.
  const canTakeFinalBackup =
    cluster?.host_assignment != null && cluster?.lifecycle_state === "ready";
  const [finalBackup, setFinalBackup] = useState(canTakeFinalBackup);

  useEffect(() => {
    if (!canTakeFinalBackup && finalBackup) setFinalBackup(false);
  }, [canTakeFinalBackup, finalBackup]);

  if (!cluster) return <Empty title="No cluster loaded" />;

  return (
    <div className="cluster-overview">
      <div className="overview-cols">
        <div className="panel">
          <h2>Configuration</h2>
          <FactRow label="cluster_id" value={cluster.cluster_id} mono />
          <FactRow label="region" value={cluster.region} />
          <FactRow label="postgres_version" value={cluster.postgres_version} mono />
          <FactRow label="tier" value={cluster.tier} />
          <FactRow label="storage" value={formatGiB(cluster.storage_gib)} />
          <FactRow label="state" value={<StatePill state={cluster.lifecycle_state} />} />
          <FactRow
            label="host"
            value={cluster.host_assignment ? cluster.host_assignment.host_id : "unassigned"}
            mono
          />
        </div>
        <div className="panel">
          <h2>Connection</h2>
          <ClusterDefaultConnection cluster={cluster} />
        </div>
      </div>
      <div className="panel" style={{ marginTop: 12 }}>
        <h2>Operations</h2>
        <div className="stack">
          <label className="field">
            <span>Resize storage GiB</span>
            <input
              type="number"
              min={cluster.storage_gib}
              value={storageGib}
              onChange={(e) => setStorageGib(Number(e.target.value))}
            />
          </label>
          <ConfirmButton
            label={<><HardDrive size={13} /> Resize storage</>}
            confirmLabel={`Resize to ${storageGib} GiB`}
            confirmHint="This is online but may block writes briefly."
            onConfirm={() =>
              onAction("resize", () => api.resizeCluster(cluster.cluster_id, storageGib))
            }
          />
          <label
            className="check-row"
            title={
              canTakeFinalBackup
                ? undefined
                : "Available only for ready clusters with a host assignment."
            }
          >
            <input
              type="checkbox"
              checked={finalBackup}
              disabled={!canTakeFinalBackup}
              onChange={(e) => setFinalBackup(e.target.checked)}
            />
            Final backup before delete
            {!canTakeFinalBackup && (
              <span className="muted" style={{ marginLeft: 6, fontSize: 11 }}>
                (cluster not ready)
              </span>
            )}
          </label>
          <ConfirmButton
            label={canTakeFinalBackup ? "Delete cluster" : "Cancel cluster request"}
            confirmLabel={canTakeFinalBackup ? "Delete permanently" : "Cancel & remove"}
            confirmHint={
              canTakeFinalBackup
                ? `Type-equivalent confirm: deletes ${cluster.cluster_id}.`
                : `Removes the un-provisioned request for ${cluster.cluster_id}. No host work scheduled.`
            }
            danger
            onConfirm={() =>
              onAction("delete", () =>
                api.deleteCluster(cluster.cluster_id, finalBackup && canTakeFinalBackup),
              )
            }
          />
        </div>
      </div>
    </div>
  );
}

function ClusterBackupsTab({ clusterId }: { clusterId: string }) {
  const api = useApi();
  const backups = usePaasResource<ClusterBackup[]>(
    useCallback(() => api.listClusterBackups(clusterId), [api, clusterId]),
    [],
    { deps: [clusterId] },
  );
  if (backups.error) return <div className="error-banner">{backups.error}</div>;
  if (backups.data.length === 0) return <Empty title="No backups yet" hint="Trigger one via the action bar above." />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Backup</th>
            <th>State</th>
            <th>Started</th>
            <th>Completed</th>
          </tr>
        </thead>
        <tbody>
          {backups.data.map((b) => (
            <tr key={b.backup_id}>
              <td className="mono">{b.backup_id}</td>
              <td>
                <StatePill state={b.state} />
              </td>
              <td>{formatTimestamp(b.started_at)}</td>
              <td>{formatTimestamp(b.completed_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ClusterPitrTab({ clusterId }: { clusterId: string }) {
  const api = useApi();
  const checks = usePaasResource<PitrCheck[]>(
    useCallback(() => api.listClusterPitrChecks(clusterId), [api, clusterId]),
    [],
    { deps: [clusterId] },
  );
  if (checks.error) return <div className="error-banner">{checks.error}</div>;
  if (checks.data.length === 0) return <Empty title="No PITR checks recorded" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Check</th>
            <th>State</th>
            <th>Latest restorable LSN</th>
            <th>Observed</th>
          </tr>
        </thead>
        <tbody>
          {checks.data.map((c) => (
            <tr key={c.check_id}>
              <td className="mono">{c.check_id}</td>
              <td>
                <StatePill state={c.state} />
              </td>
              <td className="mono">{c.latest_restorable_lsn ?? "—"}</td>
              <td>{formatRelative(c.observed_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ClusterRolesTab({ onRotate }: { onRotate: () => void }) {
  return (
    <div className="panel">
      <h2>Role credentials</h2>
      <p className="muted" style={{ marginBottom: 10 }}>
        Listing per-role credentials and rotation history is a follow-up screen. Use the
        rotate button to issue a credential rotation now.
      </p>
      <ConfirmButton
        label={<><KeyRound size={13} /> Rotate credentials</>}
        confirmLabel="Rotate now"
        confirmHint="Existing connections will reconnect with the new credentials."
        onConfirm={onRotate}
      />
    </div>
  );
}

function ClusterOpsTab({ clusterId }: { clusterId: string }) {
  const api = useApi();
  const ops = usePaasResource<ClusterOperation[]>(
    useCallback(() => api.listClusterOperations(clusterId), [api, clusterId]),
    [],
    { deps: [clusterId] },
  );
  if (ops.error) return <div className="error-banner">{ops.error}</div>;
  if (ops.data.length === 0) return <Empty title="No operations recorded" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Operation</th>
            <th>Kind</th>
            <th>Target</th>
            <th>Status</th>
            <th>Step</th>
            <th>Leased by</th>
          </tr>
        </thead>
        <tbody>
          {ops.data.map((o) => (
            <tr key={o.operation_id}>
              <td className="mono">{o.operation_id}</td>
              <td>{o.kind}</td>
              <td className="mono">{o.target_resource_id}</td>
              <td>
                <StatePill state={o.status} />
              </td>
              <td className="mono">{o.current_step}</td>
              <td className="mono muted">{o.lease_owner ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ClusterAuditTab({ clusterId }: { clusterId: string }) {
  const api = useApi();
  const events = usePaasResource<AuditEvent[]>(
    useCallback(() => api.listAuditEvents(100), [api]),
    [],
  );
  const rows = events.data.filter((e) => e.resource?.includes(clusterId));
  if (events.error) return <div className="error-banner">{events.error}</div>;
  if (rows.length === 0) return <Empty title="No audit events for this cluster" />;
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>When</th>
            <th>Actor</th>
            <th>Action</th>
            <th>Outcome</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr key={e.event_id}>
              <td>{formatRelative(e.occurred_at)}</td>
              <td className="mono">{e.actor_id ?? "—"}</td>
              <td className="mono">{e.action}</td>
              <td>
                <StatePill state={e.outcome} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ClusterDefaultConnection({ cluster }: { cluster: ManagedPostgresCluster }) {
  const connection = useClusterConnection(cluster);
  return (
    <ConnectionInfo
      params={connection.params}
      reason={connection.reason}
      loading={connection.loading}
    />
  );
}

function FactRow({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="panel-row">
      <span className="panel-row-key">{label}</span>
      <span className={mono ? "panel-row-value mono" : "panel-row-value"}>{value}</span>
    </div>
  );
}
