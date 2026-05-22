import { useCallback, useMemo, useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { formatRelative, formatTimestamp } from "../lib/format";
import type { AuditEvent } from "../types";

export function Audit() {
  const api = useApi();
  const [limit, setLimit] = useState(200);
  const [filter, setFilter] = useState("");
  const events = usePaasResource<AuditEvent[]>(
    useCallback(() => api.listAuditEvents(limit), [api, limit]),
    [],
    { deps: [limit] },
  );

  const rows = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return events.data;
    return events.data.filter((e) =>
      [e.action, e.actor_id, e.resource, e.outcome]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q)),
    );
  }, [events.data, filter]);

  return (
    <div>
      <PageHeader
        title="Audit log"
        subtitle={`${rows.length} of ${events.data.length} shown`}
        onRefresh={events.refresh}
        loading={events.loading}
      />

      <div className="row" style={{ marginBottom: 10 }}>
        <label className="field" style={{ flex: "0 0 280px" }}>
          <span>Filter</span>
          <input
            value={filter}
            placeholder="actor, action, resource, outcome"
            onChange={(e) => setFilter(e.target.value)}
          />
        </label>
        <label className="field" style={{ flex: "0 0 110px" }}>
          <span>Limit</span>
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            <option value={50}>50</option>
            <option value={200}>200</option>
            <option value={500}>500</option>
            <option value={1000}>1000</option>
          </select>
        </label>
      </div>

      {events.error && <div className="error-banner">{events.error}</div>}

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>When</th>
              <th>Actor</th>
              <th>Action</th>
              <th>Resource</th>
              <th>Outcome</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => (
              <tr key={e.event_id} title={formatTimestamp(e.occurred_at)}>
                <td>{formatRelative(e.occurred_at)}</td>
                <td className="mono">{e.actor_id ?? "—"}</td>
                <td className="mono">{e.action}</td>
                <td className="mono">{e.resource ?? "—"}</td>
                <td>
                  <StatePill state={e.outcome} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length === 0 && <Empty title="No audit events match" />}
      </div>
    </div>
  );
}
