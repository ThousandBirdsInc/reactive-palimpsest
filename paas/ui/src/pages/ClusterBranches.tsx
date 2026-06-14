// Managed Postgres database branches.
//
// The control plane exposes a full CRUD-ish surface for branches:
//   POST   /v1/managed-postgres/clusters/:id/branches
//   GET    /v1/managed-postgres/clusters/:id/branches
//   GET    /v1/managed-postgres/clusters/:id/branches/:branch_id
//   DELETE /v1/managed-postgres/clusters/:id/branches/:branch_id
//
// A branch is either a copy-on-write fork of HEAD ("head_cow") or a
// point-in-time fork pinned to a recovery LSN ("point_in_time"). Branches
// can chain off one another via parent_branch_id, so we render the list as
// a parent → child lineage tree.
//
// Modeled on ClusterClones.tsx — same state/loading/error conventions.

import { FormEvent, useCallback, useMemo, useState } from "react";
import { GitBranch, Info, Plus, Trash2 } from "lucide-react";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { ConfirmButton } from "../components/ConfirmButton";
import type {
  CreateBranchRequest,
  ManagedPostgresBranch,
  ManagedPostgresBranchMode,
} from "../types";

interface Props {
  clusterId: string;
}

const DEFAULT_SOURCE_DB = "postgres";

export function ClusterBranchesTab({ clusterId }: Props) {
  const api = useApi();
  const branches = usePaasResource<ManagedPostgresBranch[]>(
    useCallback(() => api.listBranches(clusterId), [api, clusterId]),
    [],
    { deps: [clusterId] },
  );
  const [createOpen, setCreateOpen] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);
  const [opError, setOpError] = useState<string | null>(null);

  const rows = useMemo(() => deriveBranchRows(branches.data), [branches.data]);

  async function submitBranch(request: CreateBranchRequest) {
    setCreating(true);
    setCreateError(null);
    try {
      await api.createBranch(clusterId, request);
      setCreateOpen(false);
      branches.refresh();
    } catch (e) {
      setCreateError(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  }

  async function deleteBranch(branchId: string) {
    setOpError(null);
    try {
      await api.deleteBranch(clusterId, branchId);
      branches.refresh();
    } catch (e) {
      setOpError(`delete ${branchId}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  return (
    <div className="stack">
      <div className="row" style={{ justifyContent: "space-between" }}>
        <div className="muted" style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <Info size={13} aria-hidden="true" />
          <span>
            Database branches fork a source database — copy-on-write from HEAD or pinned
            to a point in time. Branches can chain off one another.
          </span>
        </div>
        <button
          className="btn-primary"
          type="button"
          onClick={() => setCreateOpen((v) => !v)}
        >
          <Plus size={13} />
          New branch
        </button>
      </div>

      {createOpen && (
        <CreateBranchForm
          clusterId={clusterId}
          existingBranches={branches.data}
          busy={creating}
          error={createError}
          onCancel={() => {
            setCreateOpen(false);
            setCreateError(null);
          }}
          onSubmit={submitBranch}
        />
      )}

      {opError && <div className="error-banner">{opError}</div>}
      {branches.error && <div className="error-banner">{branches.error}</div>}

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Branch</th>
              <th>State</th>
              <th>Mode</th>
              <th>Source db</th>
              <th>Branch db</th>
              <th>From LSN</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.branch.branch_id}>
                <td className="mono">
                  <span
                    style={{ display: "inline-flex", alignItems: "center", gap: 6 }}
                    title={row.branch.branch_id}
                  >
                    {row.depth > 0 && (
                      <span className="muted" aria-hidden="true">
                        {"  ".repeat(row.depth)}
                        {"└─"}
                      </span>
                    )}
                    <GitBranch size={12} aria-hidden="true" />
                    {row.branch.name}
                  </span>
                </td>
                <td>
                  <StatePill state={row.branch.lifecycle_state} />
                  {row.branch.error_message && (
                    <div className="muted" style={{ fontSize: 11, marginTop: 2 }}>
                      {row.branch.error_message}
                    </div>
                  )}
                </td>
                <td>{modeLabel(row.branch.mode)}</td>
                <td className="mono">{row.branch.source_database}</td>
                <td className="mono muted">{row.branch.branch_database ?? "—"}</td>
                <td className="mono muted">{row.branch.created_from_lsn ?? "—"}</td>
                <td>
                  <ConfirmButton
                    label={<><Trash2 size={12} /> Delete</>}
                    confirmLabel="Delete branch"
                    confirmHint={`Deletes branch ${row.branch.name}. This cannot be undone.`}
                    danger
                    disabled={
                      row.branch.lifecycle_state === "deleting" ||
                      row.branch.lifecycle_state === "deleted"
                    }
                    onConfirm={() => void deleteBranch(row.branch.branch_id)}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length === 0 && (
          <Empty
            title="No branches yet"
            hint="Click New branch to fork a source database from HEAD or a point in time."
          />
        )}
      </div>
    </div>
  );
}

interface BranchRow {
  branch: ManagedPostgresBranch;
  depth: number;
}

// Order branches as a parent → child lineage tree. Roots (no parent, or a
// parent not present in this cluster's list) come first, each followed by
// its descendants. Falls back to flat order for any branch we can't place
// (e.g. a cycle), so nothing is ever dropped.
function deriveBranchRows(branches: ManagedPostgresBranch[]): BranchRow[] {
  const byId = new Map<string, ManagedPostgresBranch>();
  for (const b of branches) byId.set(b.branch_id, b);

  const childrenOf = new Map<string | null, ManagedPostgresBranch[]>();
  for (const b of branches) {
    const parent =
      b.parent_branch_id && byId.has(b.parent_branch_id) ? b.parent_branch_id : null;
    const list = childrenOf.get(parent) ?? [];
    list.push(b);
    childrenOf.set(parent, list);
  }

  const rows: BranchRow[] = [];
  const seen = new Set<string>();
  const walk = (parent: string | null, depth: number) => {
    for (const b of childrenOf.get(parent) ?? []) {
      if (seen.has(b.branch_id)) continue;
      seen.add(b.branch_id);
      rows.push({ branch: b, depth });
      walk(b.branch_id, depth + 1);
    }
  };
  walk(null, 0);

  // Safety net: append anything not reached (e.g. a parent cycle).
  for (const b of branches) {
    if (!seen.has(b.branch_id)) rows.push({ branch: b, depth: 0 });
  }
  return rows;
}

function modeLabel(mode: ManagedPostgresBranchMode): string {
  return mode === "head_cow" ? "HEAD (copy-on-write)" : "Point in time";
}

function CreateBranchForm({
  clusterId,
  existingBranches,
  busy,
  error,
  onCancel,
  onSubmit,
}: {
  clusterId: string;
  existingBranches: ManagedPostgresBranch[];
  busy: boolean;
  error: string | null;
  onCancel: () => void;
  onSubmit: (request: CreateBranchRequest) => void | Promise<void>;
}) {
  const [name, setName] = useState(() => suggestBranchName(existingBranches));
  const [mode, setMode] = useState<ManagedPostgresBranchMode>("head_cow");
  const [parentBranchId, setParentBranchId] = useState("");
  const [sourceDatabase, setSourceDatabase] = useState("");
  const [recoveryTargetLsn, setRecoveryTargetLsn] = useState("");
  const [terminateConnections, setTerminateConnections] = useState(false);

  const trimmedName = name.trim();
  const invalidName = !BRANCH_NAME_RE.test(trimmedName);
  const collides = existingBranches.some((b) => b.name === trimmedName);
  const isPitr = mode === "point_in_time";
  const missingLsn = isPitr && recoveryTargetLsn.trim() === "";
  const submitDisabled = busy || invalidName || collides || missingLsn;

  function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const request: CreateBranchRequest = {
      name: trimmedName,
      mode,
      terminate_source_connections: terminateConnections,
    };
    if (parentBranchId) request.parent_branch_id = parentBranchId;
    if (sourceDatabase.trim()) request.source_database = sourceDatabase.trim();
    if (isPitr && recoveryTargetLsn.trim()) {
      request.recovery_target_lsn = recoveryTargetLsn.trim();
    }
    void onSubmit(request);
  }

  return (
    <form className="panel" onSubmit={submit}>
      <h2>Create database branch</h2>
      {error && <div className="error-banner">{error}</div>}
      <div className="form-grid">
        <label className="field">
          <span>Branch name</span>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="branch_1"
            aria-invalid={invalidName || collides || undefined}
          />
        </label>
        <label className="field">
          <span>Mode</span>
          <select
            value={mode}
            onChange={(e) => setMode(e.target.value as ManagedPostgresBranchMode)}
          >
            <option value="head_cow">HEAD (copy-on-write)</option>
            <option value="point_in_time">Point in time</option>
          </select>
        </label>
        <label className="field">
          <span>Parent branch</span>
          <select
            value={parentBranchId}
            onChange={(e) => setParentBranchId(e.target.value)}
          >
            <option value="">(none — fork from cluster)</option>
            {existingBranches.map((b) => (
              <option key={b.branch_id} value={b.branch_id}>
                {b.name}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Source database</span>
          <input
            value={sourceDatabase}
            onChange={(e) => setSourceDatabase(e.target.value)}
            placeholder={DEFAULT_SOURCE_DB}
          />
        </label>
        {isPitr && (
          <label className="field">
            <span>Recovery target LSN</span>
            <input
              value={recoveryTargetLsn}
              onChange={(e) => setRecoveryTargetLsn(e.target.value)}
              placeholder="0/16B3748"
              aria-invalid={missingLsn || undefined}
            />
          </label>
        )}
      </div>
      <label className="check-row" style={{ marginTop: 8 }}>
        <input
          type="checkbox"
          checked={terminateConnections}
          onChange={(e) => setTerminateConnections(e.target.checked)}
        />
        Terminate active connections to source before snapshotting
      </label>
      {(invalidName || collides || missingLsn) && (
        <p className="muted" style={{ marginTop: 8, fontSize: 12 }}>
          {invalidName
            ? "Name must match [A-Za-z_][A-Za-z0-9_]{0,62}."
            : collides
              ? "A branch with this name already exists on this cluster."
              : "Point-in-time mode requires a recovery target LSN."}
        </p>
      )}
      <p className="muted" style={{ marginTop: 8, fontSize: 12 }}>
        Point-in-time mode requires a recovery target LSN. Cluster:{" "}
        <span className="mono">{clusterId}</span>
      </p>
      <div className="row" style={{ marginTop: 12, justifyContent: "flex-end" }}>
        <button type="button" className="btn-ghost" onClick={onCancel} disabled={busy}>
          Cancel
        </button>
        <button type="submit" className="btn-primary" disabled={submitDisabled}>
          {busy ? "Creating…" : "Create branch"}
        </button>
      </div>
    </form>
  );
}

const BRANCH_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

function suggestBranchName(existing: ManagedPostgresBranch[]): string {
  const names = new Set(existing.map((b) => b.name));
  const base = "branch";
  for (let i = 1; i < 1000; i += 1) {
    const candidate = `${base}_${i}`;
    if (!names.has(candidate)) return candidate;
  }
  return `${base}_${Date.now().toString().slice(-4)}`;
}
