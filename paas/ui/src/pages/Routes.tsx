import { useCallback } from "react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import type { DatabaseProxyRoute, Domain, GatewayRoute } from "../types";

export function Routes() {
  const api = useApi();
  const gateway = usePaasResource<GatewayRoute[]>(
    useCallback(() => api.listGatewayRoutes(), [api]),
    [],
  );
  const dbProxy = usePaasResource<DatabaseProxyRoute[]>(
    useCallback(() => api.listDatabaseProxyRoutes(), [api]),
    [],
  );
  const domains = usePaasResource<Domain[]>(
    useCallback(() => api.listDomains(), [api]),
    [],
  );

  return (
    <div>
      <PageHeader
        title="Routes"
        subtitle={`${gateway.data.length} gateway · ${dbProxy.data.length} db proxy · ${domains.data.length} domains`}
        onRefresh={() => {
          gateway.refresh();
          dbProxy.refresh();
          domains.refresh();
        }}
        loading={gateway.loading || dbProxy.loading || domains.loading}
      />

      <div className="section-label">Gateway routes</div>
      <div className="table-wrap" style={{ marginBottom: 14 }}>
        <table>
          <thead>
            <tr>
              <th>Host</th>
              <th>Sync endpoint</th>
              <th>Max connections</th>
              <th>Max RPS/min</th>
            </tr>
          </thead>
          <tbody>
            {gateway.data.map((r) => (
              <tr key={r.host}>
                <td className="mono">{r.host}</td>
                <td className="mono">{r.sync_endpoint}</td>
                <td>{r.rate_limit?.max_connections ?? "—"}</td>
                <td>{r.rate_limit?.max_requests_per_minute ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {gateway.data.length === 0 && <Empty title="No gateway routes" />}
      </div>

      <div className="section-label">Database proxy routes</div>
      <div className="table-wrap" style={{ marginBottom: 14 }}>
        <table>
          <thead>
            <tr>
              <th>Listen</th>
              <th>Upstream</th>
              <th>Cluster</th>
            </tr>
          </thead>
          <tbody>
            {dbProxy.data.map((r) => (
              <tr key={r.listen_addr}>
                <td className="mono">{r.listen_addr}</td>
                <td className="mono">{r.upstream_addr}</td>
                <td className="mono">{r.cluster_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {dbProxy.data.length === 0 && <Empty title="No database proxy routes" />}
      </div>

      <div className="section-label">Domains</div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Hostname</th>
              <th>State</th>
              <th>Certificate</th>
            </tr>
          </thead>
          <tbody>
            {domains.data.map((d) => (
              <tr key={d.hostname}>
                <td className="mono">{d.hostname}</td>
                <td>
                  <StatePill state={d.state} />
                </td>
                <td className="mono">{d.certificate_id ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {domains.data.length === 0 && <Empty title="No domains" />}
      </div>
    </div>
  );
}
