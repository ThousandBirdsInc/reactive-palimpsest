import { useCallback } from "react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { formatRelative } from "../lib/format";
import type { Incident } from "../types";

export function Incidents() {
  const api = useApi();
  const incidents = usePaasResource<Incident[]>(
    useCallback(() => api.listIncidents(), [api]),
    [],
  );

  return (
    <div>
      <PageHeader
        title="Incidents"
        subtitle={`${incidents.data.length} total`}
        onRefresh={incidents.refresh}
        loading={incidents.loading}
      />
      {incidents.error && <div className="error-banner">{incidents.error}</div>}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Severity</th>
              <th>Status</th>
              <th>Started</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {incidents.data.map((i) => (
              <tr key={i.incident_id}>
                <td>{i.title}</td>
                <td>{i.severity}</td>
                <td>
                  <StatePill state={i.status} />
                </td>
                <td>{formatRelative(i.started_at)}</td>
                <td>{formatRelative(i.updated_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {incidents.data.length === 0 && <Empty title="No incidents" />}
      </div>
    </div>
  );
}
