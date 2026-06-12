// Sync deployments for the active environment. These are the live-sync
// processes fronting each managed cluster; previously this data only appeared
// rolled up on Overview, with no dedicated screen.

import { useCallback } from "react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import type { SyncDeployment } from "../types";

export function SyncDeployments() {
  const api = useApi();
  const deployments = usePaasResource<SyncDeployment[]>(
    useCallback(() => api.listSyncDeployments(), [api]),
    [],
  );

  return (
    <div>
      <PageHeader
        title="Sync deployments"
        subtitle={`${deployments.data.length} in this environment`}
        onRefresh={deployments.refresh}
        loading={deployments.loading}
      />
      {deployments.error && <div className="error-banner">{deployments.error}</div>}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Deployment</th>
              <th>Region</th>
              <th>Config version</th>
              <th>State</th>
            </tr>
          </thead>
          <tbody>
            {deployments.data.map((d) => (
              <tr key={d.deployment_id}>
                <td className="mono">{d.deployment_id}</td>
                <td>{d.region}</td>
                <td className="mono">{d.config_version ?? "—"}</td>
                <td>
                  <StatePill state={d.lifecycle_state} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {deployments.data.length === 0 && <Empty title="No sync deployments" />}
      </div>
    </div>
  );
}
