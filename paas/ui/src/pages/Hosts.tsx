import { useCallback } from "react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { formatGiB, formatRelative } from "../lib/format";
import type { NodeHost } from "../types";

export function Hosts() {
  const api = useApi();
  const hosts = usePaasResource<NodeHost[]>(
    useCallback(() => api.listNodeHosts(), [api]),
    [],
  );

  return (
    <div>
      <PageHeader
        title="Node hosts"
        subtitle={`${hosts.data.length} total`}
        onRefresh={hosts.refresh}
        loading={hosts.loading}
      />
      {hosts.error && <div className="error-banner">{hosts.error}</div>}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Host</th>
              <th>Region</th>
              <th>Failure domain</th>
              <th>State</th>
              <th>Max clusters</th>
              <th>Storage</th>
              <th>Reported</th>
            </tr>
          </thead>
          <tbody>
            {hosts.data.map((h) => (
              <tr key={h.host_id}>
                <td className="mono">{h.host_id}</td>
                <td>{h.region}</td>
                <td className="muted">{h.failure_domain ?? "—"}</td>
                <td>
                  <StatePill state={h.state} />
                </td>
                <td>{h.capacity?.max_clusters ?? "—"}</td>
                <td>{h.capacity?.storage_gib != null ? formatGiB(h.capacity.storage_gib) : "—"}</td>
                <td>{formatRelative(h.reported_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {hosts.data.length === 0 && <Empty title="No hosts reported" />}
      </div>
    </div>
  );
}
