import { useCallback } from "react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import type { QuotaAlert, QuotaPolicy } from "../types";

export function Quota() {
  const api = useApi();
  const alerts = usePaasResource<QuotaAlert[]>(
    useCallback(() => api.listQuotaAlerts(), [api]),
    [],
  );
  const policies = usePaasResource<QuotaPolicy[]>(
    useCallback(() => api.listQuotaPolicies(), [api]),
    [],
  );

  return (
    <div>
      <PageHeader
        title="Quota"
        subtitle={`${alerts.data.length} environment alerts · ${policies.data.length} organization policies`}
        onRefresh={() => {
          alerts.refresh();
          policies.refresh();
        }}
        loading={alerts.loading || policies.loading}
      />
      {(alerts.error || policies.error) && (
        <div className="error-banner">{alerts.error ?? policies.error}</div>
      )}

      <div className="section-label">Alerts (environment)</div>
      <div className="table-wrap" style={{ marginBottom: 14 }}>
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>State</th>
              <th>Current</th>
              <th>Limit</th>
              <th>Threshold (bp)</th>
            </tr>
          </thead>
          <tbody>
            {alerts.data.map((a) => (
              <tr key={a.alert_id}>
                <td className="mono">{a.metric}</td>
                <td>
                  <StatePill state={a.state} />
                </td>
                <td className="mono">{a.current_quantity}</td>
                <td className="mono">{a.limit_quantity}</td>
                <td className="mono">{a.threshold_basis_points}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {alerts.data.length === 0 && <Empty title="No quota alerts" />}
      </div>

      <div className="section-label">Policies (organization)</div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Policy</th>
              <th>Metric</th>
              <th>Limit</th>
              <th>Scope</th>
            </tr>
          </thead>
          <tbody>
            {policies.data.map((p) => (
              <tr key={p.policy_id}>
                <td className="mono">{p.policy_id}</td>
                <td className="mono">{p.metric}</td>
                <td className="mono">{p.limit_quantity}</td>
                <td className="muted">{p.scope ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {policies.data.length === 0 && <Empty title="No quota policies" />}
      </div>
    </div>
  );
}
