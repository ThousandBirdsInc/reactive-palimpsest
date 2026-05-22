import { FormEvent, useCallback, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { Plus, Search } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { formatGiB } from "../lib/format";
import type { ManagedPostgresCluster } from "../types";

export function ClustersList() {
  const api = useApi();
  const clusters = usePaasResource<ManagedPostgresCluster[]>(
    useCallback(() => api.listClusters(), [api]),
    [],
  );
  const [filter, setFilter] = useState("");
  const [createOpen, setCreateOpen] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  const rows = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return clusters.data;
    return clusters.data.filter(
      (c) =>
        c.cluster_id.toLowerCase().includes(q) ||
        c.lifecycle_state.toLowerCase().includes(q) ||
        c.tier.toLowerCase().includes(q) ||
        c.region.toLowerCase().includes(q),
    );
  }, [clusters.data, filter]);

  return (
    <div>
      <PageHeader
        title="Clusters"
        subtitle={`${clusters.data.length} total`}
        onRefresh={clusters.refresh}
        loading={clusters.loading}
        actions={
          <button className="btn-primary" type="button" onClick={() => setCreateOpen((v) => !v)}>
            <Plus size={14} />
            New cluster
          </button>
        }
      />

      {clusters.error && <div className="error-banner">{clusters.error}</div>}

      {createOpen && (
        <CreateClusterForm
          onCancel={() => setCreateOpen(false)}
          onSubmit={async (input) => {
            try {
              await api.createCluster(input);
              setCreateOpen(false);
              setCreateError(null);
              clusters.refresh();
            } catch (e) {
              setCreateError(e instanceof Error ? e.message : String(e));
            }
          }}
          error={createError}
        />
      )}

      <div className="row" style={{ marginBottom: 10 }}>
        <label className="field" style={{ flex: "0 0 280px" }}>
          <span>
            <Search size={11} style={{ verticalAlign: "-2px", marginRight: 4 }} />
            Filter
          </span>
          <input
            value={filter}
            placeholder="id, state, tier, region"
            onChange={(e) => setFilter(e.target.value)}
          />
        </label>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Cluster</th>
              <th>State</th>
              <th>Version</th>
              <th>Tier</th>
              <th>Storage</th>
              <th>Region</th>
              <th>Host</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((c) => (
              <tr key={c.cluster_id}>
                <td>
                  <Link to={`/clusters/${encodeURIComponent(c.cluster_id)}`} className="row-link">
                    {c.cluster_id}
                  </Link>
                </td>
                <td>
                  <StatePill state={c.lifecycle_state} />
                </td>
                <td className="mono">{c.postgres_version}</td>
                <td>{c.tier}</td>
                <td>{formatGiB(c.storage_gib)}</td>
                <td>{c.region}</td>
                <td className="mono">{c.host_assignment?.host_id ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length === 0 && (
          <Empty
            title={clusters.data.length === 0 ? "No clusters" : "No clusters match the filter"}
            hint={
              clusters.data.length === 0
                ? "Click New cluster to create the first managed Postgres."
                : undefined
            }
          />
        )}
      </div>
    </div>
  );
}

interface CreateInput {
  clusterId: string;
  region: string;
  postgresVersion: string;
  tier: string;
  storageGib: number;
}

function CreateClusterForm({
  onCancel,
  onSubmit,
  error,
}: {
  onCancel: () => void;
  onSubmit: (input: CreateInput) => void | Promise<void>;
  error: string | null;
}) {
  const [clusterId, setClusterId] = useState(`cluster_${Date.now().toString().slice(-6)}`);
  const [region, setRegion] = useState("us-east-1");
  const [postgresVersion, setPostgresVersion] = useState("18");
  const [tier, setTier] = useState("dev");
  const [storageGib, setStorageGib] = useState(20);

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void onSubmit({ clusterId, region, postgresVersion, tier, storageGib });
  }

  return (
    <form className="panel" style={{ marginBottom: 12 }} onSubmit={submit}>
      <h2>Create cluster</h2>
      {error && <div className="error-banner">{error}</div>}
      <div className="form-grid">
        <label className="field">
          <span>Cluster ID</span>
          <input value={clusterId} onChange={(e) => setClusterId(e.target.value)} />
        </label>
        <label className="field">
          <span>Region</span>
          <input value={region} onChange={(e) => setRegion(e.target.value)} />
        </label>
        <label className="field">
          <span>Postgres version</span>
          <input value={postgresVersion} onChange={(e) => setPostgresVersion(e.target.value)} />
        </label>
        <label className="field">
          <span>Tier</span>
          <select value={tier} onChange={(e) => setTier(e.target.value)}>
            <option value="dev">dev</option>
            <option value="prod">prod</option>
            <option value="bench">bench</option>
          </select>
        </label>
        <label className="field">
          <span>Storage GiB</span>
          <input
            type="number"
            min={1}
            value={storageGib}
            onChange={(e) => setStorageGib(Number(e.target.value))}
          />
        </label>
      </div>
      <div className="row" style={{ marginTop: 12, justifyContent: "flex-end" }}>
        <button type="button" className="btn-ghost" onClick={onCancel}>
          Cancel
        </button>
        <button type="submit" className="btn-primary">
          Create intent
        </button>
      </div>
    </form>
  );
}
