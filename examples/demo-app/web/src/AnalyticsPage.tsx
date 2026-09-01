// Live analytics over the issue tracker. Every panel is a SQL
// aggregate the server compiles into an incrementally-maintained
// dataflow — the browser receives aggregate *rows*, never raw issues,
// and each WAL commit arrives as a retract/assert pair for exactly the
// group keys it touched. The charts are plain inline SVG.
//
// Chart conventions (see the repo's dataviz guidance): workflow stages
// wear one blue ramp stepped light→dark (an ordered scale, not a
// rainbow); single-measure charts use the single accent hue; marks are
// thin with rounded data-ends; text stays in ink tokens, never the
// series color; every multi-series chart keeps a legend and a data
// table.

import { ReactNode, useMemo, useState } from "react";
import { usePalimpsestSubscription } from "@palimpsest/client/react";
import { useSession } from "./session";
import {
  epochDayLabel,
  PROJECT_BY_ID,
  Status,
  STATUS_COLOR,
  STATUS_LABEL,
  todayEpochDay,
} from "./issues";

// ---------------------------------------------------------------------------
// Subscriptions

// One aggregate row per *single* group key: the compiled plan
// advertises output column 0 as the row identity, so multi-column
// GROUP BY keys would collide in the client-side row cache. Each
// chart therefore groups by exactly one key (the workload chart uses
// one subscription per stage — the dataflow shares the base-table
// scan between them anyway).
const STATUS_SQL = `SELECT status, COUNT(*) AS n
FROM issues
GROUP BY status`;

const URGENT_SQL = `SELECT COUNT(*) AS n
FROM issues
WHERE priority = 4
  AND status IN ('backlog', 'todo', 'in_progress', 'in_review')`;

const THROUGHPUT_SQL = `SELECT completed_day, COUNT(*) AS n, SUM(estimate) AS points
FROM issues
WHERE status = 'done'
GROUP BY completed_day
ORDER BY completed_day DESC
LIMIT 42`;

const workloadSql = (stage: string) => `SELECT assignee, COUNT(*) AS n
FROM issues
WHERE status = '${stage}'
GROUP BY assignee`;

const CYCLE_SQL = `WITH completed AS (
  SELECT project, cycle_days
  FROM issues
  WHERE status = 'done'
)
SELECT project, COUNT(*) AS n, AVG(cycle_days) AS avg_cycle_days
FROM completed
GROUP BY project`;

interface StatusRow {
  status: Status;
  n: number;
}

interface CountRow {
  n: number;
}

interface ThroughputRow {
  completed_day: number;
  n: number;
  points: number;
}

interface WorkloadRow {
  assignee: string;
  n: number;
}

interface CycleRow {
  project: string;
  n: number;
  avg_cycle_days: number;
}

/** Ink + chrome tokens (light surface). */
const INK = "#0b0b0b";
const INK_SECONDARY = "#52514e";
const INK_MUTED = "#898781";
const GRID = "#e1e0d9";
const BASELINE = "#c3c2b7";
const ACCENT = "#2a78d6";
const GOOD_TEXT = "#006300";

const OPEN_STATUSES: Status[] = ["backlog", "todo", "in_progress", "in_review"];
const WORK_STATUSES: Status[] = ["todo", "in_progress", "in_review"];

export default function AnalyticsPage() {
  const { client, users } = useSession();

  const { rows: statusRows, error: statusError } =
    usePalimpsestSubscription<StatusRow>(client, STATUS_SQL, {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: urgentRows, error: urgentError } =
    usePalimpsestSubscription<CountRow>(client, URGENT_SQL, {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: throughputRows, error: throughputError } =
    usePalimpsestSubscription<ThroughputRow>(client, THROUGHPUT_SQL, {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: todoRows, error: todoError } =
    usePalimpsestSubscription<WorkloadRow>(client, workloadSql("todo"), {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: inProgressRows, error: inProgressError } =
    usePalimpsestSubscription<WorkloadRow>(client, workloadSql("in_progress"), {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: inReviewRows, error: inReviewError } =
    usePalimpsestSubscription<WorkloadRow>(client, workloadSql("in_review"), {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: cycleRows, error: cycleError } =
    usePalimpsestSubscription<CycleRow>(client, CYCLE_SQL, {
      decoder: { coerceSafeIntegersToNumber: true },
    });

  const error =
    statusError ??
    urgentError ??
    throughputError ??
    todoError ??
    inProgressError ??
    inReviewError ??
    cycleError;

  // ------- KPI row (derived client-side from the aggregates) -------
  const kpis = useMemo(() => {
    const today = todayEpochDay();
    let open = 0;
    let inProgress = 0;
    for (const row of statusRows) {
      if (OPEN_STATUSES.includes(row.status)) open += row.n;
      if (row.status === "in_progress") inProgress += row.n;
    }
    const urgentOpen = urgentRows[0]?.n ?? 0;
    let done7 = 0;
    let donePrev7 = 0;
    for (const row of throughputRows) {
      if (row.completed_day > today - 7) done7 += row.n;
      else if (row.completed_day > today - 14) donePrev7 += row.n;
    }
    let cycleSum = 0;
    let cycleCount = 0;
    for (const row of cycleRows) {
      cycleSum += row.avg_cycle_days * row.n;
      cycleCount += row.n;
    }
    return {
      open,
      inProgress,
      urgentOpen,
      done7,
      done7Delta: done7 - donePrev7,
      avgCycle: cycleCount > 0 ? cycleSum / cycleCount : null,
    };
  }, [statusRows, urgentRows, throughputRows, cycleRows]);

  // ------- Status distribution -------
  const statusBars = useMemo(() => {
    const byStatus = new Map(statusRows.map((r) => [r.status, r.n]));
    const order: Status[] = [...OPEN_STATUSES, "done", "cancelled"];
    return order
      .filter((s) => byStatus.has(s))
      .map((status) => ({
        label: STATUS_LABEL[status],
        value: byStatus.get(status) ?? 0,
        color: STATUS_COLOR[status],
      }));
  }, [statusRows]);

  // ------- Throughput (zero-filled last 6 weeks) -------
  const throughput = useMemo(() => {
    const today = todayEpochDay();
    const byDay = new Map(throughputRows.map((r) => [r.completed_day, r]));
    const days: { day: number; n: number; points: number }[] = [];
    for (let day = today - 41; day <= today; day += 1) {
      const row = byDay.get(day);
      days.push({ day, n: row?.n ?? 0, points: row?.points ?? 0 });
    }
    return days;
  }, [throughputRows]);
  const [throughputMeasure, setThroughputMeasure] = useState<"n" | "points">(
    "n",
  );

  // ------- Workload by assignee (stacked by stage) -------
  const workload = useMemo(() => {
    const byStage: Record<Status, Map<string, number>> = {
      todo: new Map(todoRows.map((r) => [r.assignee, r.n])),
      in_progress: new Map(inProgressRows.map((r) => [r.assignee, r.n])),
      in_review: new Map(inReviewRows.map((r) => [r.assignee, r.n])),
    } as Record<Status, Map<string, number>>;
    const ids = [...users.map((u) => u.id), ""];
    return ids
      .map((id) => {
        const segments = WORK_STATUSES.map((status) => ({
          key: status,
          label: STATUS_LABEL[status],
          color: STATUS_COLOR[status],
          value: byStage[status]?.get(id) ?? 0,
        }));
        return {
          label: id === "" ? "Unassigned" : id,
          segments,
          total: segments.reduce((sum, s) => sum + s.value, 0),
        };
      })
      .filter((row) => row.total > 0 || row.label !== "Unassigned");
  }, [todoRows, inProgressRows, inReviewRows, users]);

  // ------- Cycle time by project -------
  const cycleBars = useMemo(
    () =>
      [...cycleRows]
        .sort((a, b) => a.avg_cycle_days - b.avg_cycle_days)
        .map((row) => ({
          label: PROJECT_BY_ID.get(row.project)?.label ?? row.project,
          value: row.avg_cycle_days,
          sublabel: `${row.n} done`,
        })),
    [cycleRows],
  );

  return (
    <>
      <h1>Analytics</h1>
      <p className="tagline">
        Four live aggregate subscriptions. The dataflow maintains each{" "}
        <code>GROUP BY</code> incrementally and ships only the aggregate rows
        that changed — flip on team activity in the top bar and watch the
        charts move.
      </p>

      {error && (
        <div className="error-banner">
          {error.code}: {error.message}
        </div>
      )}

      <div className="kpi-row">
        <StatTile label="Open issues" value={kpis.open} />
        <StatTile label="In progress" value={kpis.inProgress} />
        <StatTile
          label="Done, last 7 days"
          value={kpis.done7}
          delta={kpis.done7Delta}
          deltaHint="vs prior 7 days"
        />
        <StatTile
          label="Avg cycle time"
          value={kpis.avgCycle === null ? "—" : kpis.avgCycle.toFixed(1)}
          suffix={kpis.avgCycle === null ? "" : " days"}
        />
        <StatTile label="Urgent open" value={kpis.urgentOpen} alert={kpis.urgentOpen > 0} />
      </div>

      <ChartPanel
        title="Issues by workflow stage"
        subtitle="one blue step per stage, light → dark in workflow order"
        sql={STATUS_SQL}
      >
        <BarChartH
          rows={statusBars}
          valueLabel={(v) => v.toLocaleString()}
          colored
        />
      </ChartPanel>

      <ChartPanel
        title="Throughput"
        subtitle="completed per day, last 6 weeks"
        sql={THROUGHPUT_SQL}
        controls={
          <div className="filters" role="group" aria-label="Measure">
            <button
              type="button"
              className={`chip ${throughputMeasure === "n" ? "chip-active" : ""}`}
              onClick={() => setThroughputMeasure("n")}
            >
              Issues
            </button>
            <button
              type="button"
              className={`chip ${
                throughputMeasure === "points" ? "chip-active" : ""
              }`}
              onClick={() => setThroughputMeasure("points")}
            >
              Points
            </button>
          </div>
        }
      >
        <ColumnChart
          points={throughput.map((d) => ({
            x: d.day,
            label: epochDayLabel(d.day),
            value: throughputMeasure === "n" ? d.n : d.points,
          }))}
          unit={throughputMeasure === "n" ? "issues" : "points"}
        />
      </ChartPanel>

      <ChartPanel
        title="Workload by assignee"
        subtitle="open issues in todo / in progress / in review"
        sql={workloadSql("<stage>")}
      >
        <StackedBarH
          rows={workload}
          legend={WORK_STATUSES.map((s) => ({
            label: STATUS_LABEL[s],
            color: STATUS_COLOR[s],
          }))}
        />
      </ChartPanel>

      <ChartPanel
        title="Cycle time by project"
        subtitle="average days from created to done"
        sql={CYCLE_SQL}
      >
        <BarChartH
          rows={cycleBars}
          valueLabel={(v) => `${v.toFixed(1)}d`}
        />
      </ChartPanel>

      <footer>
        Aggregates are computed server-side by the compiled dataflow
        (BaseTable → Filter → Aggregate → TopK); the browser only ever holds
        the grouped rows above.
      </footer>
    </>
  );
}

// ---------------------------------------------------------------------------
// Panels & tiles

function ChartPanel({
  title,
  subtitle,
  sql,
  controls,
  children,
}: {
  title: string;
  subtitle: string;
  sql: string;
  controls?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="panel">
      <header className="panel-head">
        <div>
          <h2>{title}</h2>
          <span className="panel-subtitle">{subtitle}</span>
        </div>
        {controls}
      </header>
      {children}
      <details className="board-sql">
        <summary>SQL</summary>
        <pre className="sql">{sql}</pre>
      </details>
    </section>
  );
}

function StatTile({
  label,
  value,
  suffix = "",
  delta,
  deltaHint,
  alert = false,
}: {
  label: string;
  value: number | string;
  suffix?: string;
  delta?: number;
  deltaHint?: string;
  alert?: boolean;
}) {
  return (
    <div className="stat-tile">
      <span className="stat-label">{label}</span>
      <span className={`stat-value ${alert ? "stat-alert" : ""}`}>
        {typeof value === "number" ? value.toLocaleString() : value}
        {suffix && <span className="stat-suffix">{suffix}</span>}
      </span>
      {delta !== undefined && (
        <span
          className="stat-delta"
          style={{ color: delta >= 0 ? GOOD_TEXT : "#d03b3b" }}
          title={deltaHint}
        >
          {delta >= 0 ? "▲" : "▼"} {Math.abs(delta).toLocaleString()}
        </span>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Charts (inline SVG, light surface)

interface HBar {
  label: string;
  value: number;
  color?: string;
  sublabel?: string;
}

/** Horizontal bars: ≤16px thick, 4px rounded data-end, value at the
 *  tip in ink, category labels in the left gutter. */
function BarChartH({
  rows,
  valueLabel,
  colored = false,
}: {
  rows: HBar[];
  valueLabel: (v: number) => string;
  colored?: boolean;
}) {
  const width = 760;
  const barH = 16;
  const rowH = 30;
  const labelW = 120;
  // Right gutter must fit "12.3d" plus the widest muted sublabel.
  const valueW = rows.some((r) => r.sublabel) ? 130 : 76;
  const height = rows.length * rowH + 8;
  const max = Math.max(1, ...rows.map((r) => r.value));
  const innerW = width - labelW - valueW;
  if (rows.length === 0) {
    return <p className="empty">Waiting for the first aggregate snapshot…</p>;
  }
  return (
    <svg
      className="chart"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Bar chart"
    >
      {rows.map((row, i) => {
        const y = i * rowH + 6;
        const w = Math.max(2, (row.value / max) * innerW);
        return (
          <g key={row.label}>
            <text
              x={labelW - 10}
              y={y + barH / 2 + 4}
              textAnchor="end"
              fill={INK_SECONDARY}
              fontSize={12.5}
            >
              {row.label}
            </text>
            {/* square at the baseline, 4px rounded data-end */}
            <path
              d={roundedRightBar(labelW, y, w, barH, 4)}
              fill={colored ? (row.color ?? ACCENT) : ACCENT}
            >
              <title>
                {row.label}: {valueLabel(row.value)}
                {row.sublabel ? ` (${row.sublabel})` : ""}
              </title>
            </path>
            <text
              x={labelW + w + 8}
              y={y + barH / 2 + 4}
              fill={INK}
              fontSize={12.5}
              fontWeight={600}
            >
              {valueLabel(row.value)}
            </text>
            {row.sublabel && (
              <text
                x={labelW + w + 8 + valueLabelWidth(valueLabel(row.value))}
                y={y + barH / 2 + 4}
                fill={INK_MUTED}
                fontSize={11}
              >
                {row.sublabel}
              </text>
            )}
          </g>
        );
      })}
      <line
        x1={labelW}
        x2={labelW}
        y1={2}
        y2={height - 2}
        stroke={BASELINE}
        strokeWidth={1}
      />
    </svg>
  );
}

/** Crude width estimate to place a muted sublabel after the value. */
function valueLabelWidth(text: string): number {
  return text.length * 7.4 + 8;
}

/** Bar path: square left edge (baseline), rounded right data-end. */
function roundedRightBar(
  x: number,
  y: number,
  w: number,
  h: number,
  r: number,
): string {
  const radius = Math.min(r, w / 2, h / 2);
  return [
    `M ${x} ${y}`,
    `H ${x + w - radius}`,
    `Q ${x + w} ${y} ${x + w} ${y + radius}`,
    `V ${y + h - radius}`,
    `Q ${x + w} ${y + h} ${x + w - radius} ${y + h}`,
    `H ${x}`,
    "Z",
  ].join(" ");
}

/** Column path: square bottom (baseline), rounded top data-end. */
function roundedTopBar(
  x: number,
  y: number,
  w: number,
  h: number,
  r: number,
): string {
  const radius = Math.min(r, w / 2, h / 2);
  return [
    `M ${x} ${y + h}`,
    `V ${y + radius}`,
    `Q ${x} ${y} ${x + radius} ${y}`,
    `H ${x + w - radius}`,
    `Q ${x + w} ${y} ${x + w} ${y + radius}`,
    `V ${y + h}`,
    "Z",
  ].join(" ");
}

interface ColumnPoint {
  x: number;
  label: string;
  value: number;
}

/** Daily columns: single hue, hairline gridlines at clean ticks,
 *  hover tooltip, sparse x labels (weekly). */
function ColumnChart({
  points,
  unit,
}: {
  points: ColumnPoint[];
  unit: string;
}) {
  const width = 760;
  const height = 220;
  const pad = { top: 14, right: 8, bottom: 26, left: 40 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;
  const [hover, setHover] = useState<ColumnPoint | null>(null);

  const max = Math.max(1, ...points.map((p) => p.value));
  const yMax = niceCeil(max);
  const ticks = [0, yMax / 2, yMax];
  const slot = innerW / Math.max(points.length, 1);
  const barW = Math.min(24, Math.max(4, slot - 2));

  return (
    <div className="chart-wrap">
      <svg
        className="chart"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`Throughput columns (${unit} per day)`}
        onMouseLeave={() => setHover(null)}
      >
        {ticks.map((t) => (
          <g key={t}>
            <line
              x1={pad.left}
              x2={width - pad.right}
              y1={pad.top + innerH - (t / yMax) * innerH}
              y2={pad.top + innerH - (t / yMax) * innerH}
              stroke={t === 0 ? BASELINE : GRID}
              strokeWidth={1}
            />
            <text
              x={pad.left - 8}
              y={pad.top + innerH - (t / yMax) * innerH + 4}
              textAnchor="end"
              fill={INK_MUTED}
              fontSize={11}
            >
              {t.toLocaleString()}
            </text>
          </g>
        ))}
        {points.map((p, i) => {
          const h = (p.value / yMax) * innerH;
          const x = pad.left + i * slot + (slot - barW) / 2;
          const y = pad.top + innerH - h;
          const isHover = hover?.x === p.x;
          return (
            <g key={p.x}>
              {/* full-height hit target so hover works on short bars */}
              <rect
                x={pad.left + i * slot}
                y={pad.top}
                width={slot}
                height={innerH}
                fill="transparent"
                onMouseEnter={() => setHover(p)}
              />
              {p.value > 0 && (
                <path
                  d={roundedTopBar(x, y, barW, h, 4)}
                  fill={ACCENT}
                  opacity={hover && !isHover ? 0.45 : 1}
                  pointerEvents="none"
                />
              )}
            </g>
          );
        })}
        {/* weekly x labels */}
        {points.map((p, i) =>
          i % 7 === 0 ? (
            <text
              key={`x-${p.x}`}
              x={pad.left + i * slot + slot / 2}
              y={height - 8}
              textAnchor="middle"
              fill={INK_MUTED}
              fontSize={11}
            >
              {p.label}
            </text>
          ) : null,
        )}
      </svg>
      {hover && (
        <div className="chart-tooltip">
          <strong>{hover.label}</strong> · {hover.value.toLocaleString()}{" "}
          {unit}
        </div>
      )}
    </div>
  );
}

function niceCeil(v: number): number {
  const pow = 10 ** Math.floor(Math.log10(Math.max(v, 1)));
  for (const m of [1, 2, 4, 5, 10]) {
    if (m * pow >= v) return m * pow;
  }
  return 10 * pow;
}

interface StackRow {
  label: string;
  segments: { key: string; label: string; color: string; value: number }[];
  total: number;
}

/** Horizontal stacked bars with 2px surface gaps between segments, a
 *  legend, per-segment tooltips, and a data table. */
function StackedBarH({
  rows,
  legend,
}: {
  rows: StackRow[];
  legend: { label: string; color: string }[];
}) {
  const width = 760;
  const barH = 16;
  const rowH = 30;
  const labelW = 120;
  const valueW = 56;
  const height = rows.length * rowH + 8;
  const max = Math.max(1, ...rows.map((r) => r.total));
  const innerW = width - labelW - valueW;
  if (rows.length === 0) {
    return <p className="empty">Waiting for the first aggregate snapshot…</p>;
  }
  return (
    <>
      <div className="chart-legend">
        {legend.map((item) => (
          <span key={item.label} className="chart-legend-item">
            <span
              className="chart-legend-swatch"
              style={{ background: item.color }}
            />
            {item.label}
          </span>
        ))}
      </div>
      <svg
        className="chart"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="Workload stacked bars"
      >
        {rows.map((row, i) => {
          const y = i * rowH + 6;
          let x = labelW;
          return (
            <g key={row.label}>
              <text
                x={labelW - 10}
                y={y + barH / 2 + 4}
                textAnchor="end"
                fill={INK_SECONDARY}
                fontSize={12.5}
              >
                {row.label}
              </text>
              {row.segments.map((seg, j) => {
                if (seg.value === 0) return null;
                const w = (seg.value / max) * innerW;
                const isLast =
                  row.segments.slice(j + 1).every((s) => s.value === 0);
                const el = (
                  <path
                    key={seg.key}
                    d={
                      isLast
                        ? roundedRightBar(x, y, Math.max(w - 2, 2), barH, 4)
                        : `M ${x} ${y} H ${x + Math.max(w - 2, 2)} V ${y + barH} H ${x} Z`
                    }
                    fill={seg.color}
                  >
                    <title>
                      {row.label} · {seg.label}: {seg.value}
                    </title>
                  </path>
                );
                x += w;
                return el;
              })}
              <text
                x={x + 8}
                y={y + barH / 2 + 4}
                fill={INK}
                fontSize={12.5}
                fontWeight={600}
              >
                {row.total}
              </text>
            </g>
          );
        })}
        <line
          x1={labelW}
          x2={labelW}
          y1={2}
          y2={height - 2}
          stroke={BASELINE}
          strokeWidth={1}
        />
      </svg>
      <details className="board-sql">
        <summary>Data table</summary>
        <table className="lf-table">
          <thead>
            <tr>
              <th>Assignee</th>
              {legend.map((l) => (
                <th key={l.label}>{l.label}</th>
              ))}
              <th>Total</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.label}>
                <td>{row.label}</td>
                {row.segments.map((s) => (
                  <td key={s.key}>{s.value}</td>
                ))}
                <td>{row.total}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </>
  );
}
