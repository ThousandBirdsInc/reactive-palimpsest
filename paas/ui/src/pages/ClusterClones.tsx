// Cluster CoW database clones.
//
// The control plane exposes one mutation endpoint today:
//   POST /v1/managed-postgres/clusters/:id/database-clones
//     { source_database, target_database, terminate_source_connections }
//
// There is no dedicated list / get / delete for clones — every clone
// request creates an OperationRecord with kind == "create_database_clone"
// and target_resource_id == "<cluster_id>:<target_database>". We list
// those operations to show in-flight and historical clone requests.
//
// What you can't do here yet:
//   - Drop a clone database from the UI (no backend endpoint).
//   - See clone storage size or page-cache footprint (no metric exposed).
// Both are tracked as follow-ups; the surface is shaped to absorb them
// when the backend lands.

import React, { FormEvent, useCallback, useMemo, useState } from "react";
import { ChevronDown, ChevronRight, GitBranch, Info, Link as LinkIcon, Plus } from "lucide-react";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { ConnectionInfo } from "../components/ConnectionInfo";
import { useClusterConnection } from "../lib/connection";
import type {
  ClusterOperation,
  DatabaseCloneRequest,
  ManagedPostgresCluster,
} from "../types";

interface Props {
  clusterId: string;
}

const CLONE_OP_KIND = "create_database_clone";
const DEFAULT_SOURCE_DB = "postgres";

export function ClusterClonesTab({ clusterId }: Props) {
  const api = useApi();
  const ops = usePaasResource<ClusterOperation[]>(
    useCallback(() => api.listClusterOperations(clusterId), [api, clusterId]),
    [],
    { deps: [clusterId] },
  );
  // Cluster lookup is needed so per-clone ConnectionInfo can resolve the
  // proxy listen address + role-name pattern.
  const cluster = usePaasResource<ManagedPostgresCluster | null>(
    useCallback(() => api.getCluster(clusterId), [api, clusterId]),
    null,
    { deps: [clusterId] },
  );
  const [createOpen, setCreateOpen] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const cloneRows = useMemo(() => deriveCloneRows(ops.data, clusterId), [ops.data, clusterId]);

  function toggleExpand(operationId: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(operationId)) next.delete(operationId);
      else next.add(operationId);
      return next;
    });
  }

  async function submitClone(request: DatabaseCloneRequest) {
    setCreating(true);
    setCreateError(null);
    try {
      await api.createDatabaseClone(clusterId, request);
      setCreateOpen(false);
      ops.refresh();
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="stack">
      <div className="row" style={{ justifyContent: "space-between" }}>
        <div className="muted" style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <Info size={13} aria-hidden="true" />
          <span>
            Copy-on-write database clones. New clones share storage with the source until
            they're written to. Requires PostgreSQL 18+ and a ready source cluster.
          </span>
        </div>
        <button
          className="btn-primary"
          type="button"
          onClick={() => setCreateOpen((v) => !v)}
        >
          <Plus size={13} />
          New clone
        </button>
      </div>

      {createOpen && (
        <CreateCloneForm
          clusterId={clusterId}
          existingTargets={cloneRows.map((r) => r.targetDatabase)}
          busy={creating}
          error={createError}
          onCancel={() => {
            setCreateOpen(false);
            setCreateError(null);
          }}
          onSubmit={submitClone}
        />
      )}

      {ops.error && <div className="error-banner">{ops.error}</div>}

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th></th>
              <th>Clone (target db)</th>
              <th>Status</th>
              <th>Step</th>
              <th>Operation</th>
              <th>Leased by</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {cloneRows.map((row) => {
              const isOpen = expanded.has(row.operation.operation_id);
              return (
                <React.Fragment key={row.operation.operation_id}>
                  <tr>
                    <td style={{ width: 26 }}>
                      <button
                        type="button"
                        className="btn-ghost icon-only"
                        aria-label={isOpen ? "Hide connection info" : "Show connection info"}
                        aria-expanded={isOpen}
                        onClick={() => toggleExpand(row.operation.operation_id)}
                      >
                        {isOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                      </button>
                    </td>
                    <td className="mono">
                      <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                        <GitBranch size={12} aria-hidden="true" />
                        {row.targetDatabase}
                      </span>
                    </td>
                    <td>
                      <StatePill state={row.operation.status} />
                    </td>
                    <td className="mono">{row.operation.current_step}</td>
                    <td className="mono">{row.operation.operation_id}</td>
                    <td className="mono muted">{row.operation.lease_owner ?? "—"}</td>
                    <td>
                      <button
                        type="button"
                        className="btn-secondary"
                        onClick={() => toggleExpand(row.operation.operation_id)}
                      >
                        <LinkIcon size={12} />
                        Connect
                      </button>
                    </td>
                  </tr>
                  {isOpen && (
                    <tr className="clone-conn-row">
                      <td></td>
                      <td colSpan={6}>
                        <CloneConnection
                          cluster={cluster.data}
                          targetDatabase={row.targetDatabase}
                          operationStatus={row.operation.status}
                        />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
        {cloneRows.length === 0 && (
          <Empty
            title="No clones requested yet"
            hint="Click New clone to copy-on-write the source database into a new database on this cluster."
          />
        )}
      </div>
    </div>
  );
}

function CloneConnection({
  cluster,
  targetDatabase,
  operationStatus,
}: {
  cluster: ManagedPostgresCluster | null;
  targetDatabase: string;
  operationStatus: string;
}) {
  const connection = useClusterConnection(cluster, { database: targetDatabase });
  const stillProvisioning = operationStatus !== "succeeded";
  return (
    <div className="clone-conn">
      {stillProvisioning && (
        <div className="clone-conn-warn">
          <Info size={11} aria-hidden="true" /> Clone operation is{" "}
          <strong>{operationStatus}</strong> — the target database may not accept
          connections yet.
        </div>
      )}
      <ConnectionInfo
        params={connection.params}
        reason={connection.reason}
        loading={connection.loading}
        label={
          <span>
            Connection · <span className="mono">{targetDatabase}</span>
          </span>
        }
      />
    </div>
  );
}

interface CloneRow {
  targetDatabase: string;
  operation: ClusterOperation;
}

function deriveCloneRows(
  operations: ClusterOperation[],
  clusterId: string,
): CloneRow[] {
  const prefix = `${clusterId}:`;
  return operations
    .filter((o) => o.kind === CLONE_OP_KIND)
    .map((o) => {
      // target_resource_id format set by the control plane:
      //   "<cluster_id>:<sanitized_target_database>"
      // Fall back to the full id if the prefix isn't there.
      const targetDatabase = o.target_resource_id.startsWith(prefix)
        ? o.target_resource_id.slice(prefix.length)
        : o.target_resource_id;
      return { targetDatabase, operation: o };
    });
}

function CreateCloneForm({
  clusterId,
  existingTargets,
  busy,
  error,
  onCancel,
  onSubmit,
}: {
  clusterId: string;
  existingTargets: string[];
  busy: boolean;
  error: string | null;
  onCancel: () => void;
  onSubmit: (request: DatabaseCloneRequest) => void | Promise<void>;
}) {
  const [sourceDatabase, setSourceDatabase] = useState(DEFAULT_SOURCE_DB);
  const [targetDatabase, setTargetDatabase] = useState(() =>
    suggestCloneName(existingTargets),
  );
  const [terminateConnections, setTerminateConnections] = useState(false);

  const sameAsSource = sourceDatabase.trim() === targetDatabase.trim();
  const invalidTarget = !DB_NAME_RE.test(targetDatabase);
  const collides = existingTargets.includes(targetDatabase);
  const submitDisabled = busy || sameAsSource || invalidTarget;

  function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    void onSubmit({
      source_database: sourceDatabase.trim(),
      target_database: targetDatabase.trim(),
      terminate_source_connections: terminateConnections,
    });
  }

  return (
    <form className="panel" onSubmit={submit}>
      <h2>Create copy-on-write clone</h2>
      {error && <div className="error-banner">{error}</div>}
      <div className="form-grid">
        <label className="field">
          <span>Source database</span>
          <input
            value={sourceDatabase}
            onChange={(e) => setSourceDatabase(e.target.value)}
            placeholder={DEFAULT_SOURCE_DB}
          />
        </label>
        <label className="field">
          <span>Target database</span>
          <input
            value={targetDatabase}
            onChange={(e) => setTargetDatabase(e.target.value)}
            placeholder={`${DEFAULT_SOURCE_DB}_clone`}
            aria-invalid={invalidTarget || sameAsSource || undefined}
          />
        </label>
      </div>
      <label className="check-row" style={{ marginTop: 8 }}>
        <input
          type="checkbox"
          checked={terminateConnections}
          onChange={(e) => setTerminateConnections(e.target.checked)}
        />
        Terminate active connections to source before snapshotting
      </label>
      {(sameAsSource || invalidTarget || collides) && (
        <p className="muted" style={{ marginTop: 8, fontSize: 12 }}>
          {sameAsSource
            ? "Target must differ from source."
            : invalidTarget
              ? "Target must match [A-Za-z_][A-Za-z0-9_]{0,62}."
              : "A clone with this target name was already requested — the new request will queue alongside it."}
        </p>
      )}
      <p className="muted" style={{ marginTop: 8, fontSize: 12 }}>
        Cluster: <span className="mono">{clusterId}</span>
      </p>
      <div className="row" style={{ marginTop: 12, justifyContent: "flex-end" }}>
        <button type="button" className="btn-ghost" onClick={onCancel} disabled={busy}>
          Cancel
        </button>
        <button type="submit" className="btn-primary" disabled={submitDisabled}>
          {busy ? "Requesting…" : "Create clone"}
        </button>
      </div>
    </form>
  );
}

const DB_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

function suggestCloneName(existingTargets: string[]): string {
  const base = `${DEFAULT_SOURCE_DB}_clone`;
  if (!existingTargets.includes(base)) return base;
  for (let i = 2; i < 100; i += 1) {
    const candidate = `${base}_${i}`;
    if (!existingTargets.includes(candidate)) return candidate;
  }
  return `${base}_${Date.now().toString().slice(-4)}`;
}
