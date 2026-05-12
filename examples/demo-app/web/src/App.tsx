import { FormEvent, useEffect, useMemo, useState } from "react";
import {
  usePalimpsestClient,
  usePalimpsestSubscription,
} from "@palimpsest/client/react";
import * as wasm from "../pkg/palimpsest_client_js";
import { ApiClient, DemoUser, PALIMPSEST_URL } from "./api";

interface Post {
  id: bigint;
  title: string;
  published: boolean;
}

/// Per-row shape the server emits for the events-aggregate
/// subscription. After the v1 → v2 router rewrite the dataflow does
/// the aggregation server-side, so the client receives ~50 rows of
/// `(category_id, n, total, avg_value)` instead of the 300k raw
/// events.
interface EventAggregateRow {
  category_id: bigint;
  n: bigint;
  total: bigint;
  avg_value: number;
}

/// Per-row shape the server emits for the posts-by-status chart
/// subscription (`GROUP BY published`). The dataflow emits one row
/// per status — `(published: bool, n: bigint)` — not raw posts.
interface PostsByStatusRow {
  published: boolean;
  n: bigint;
}

/// Default bulk-add payload size for the "inject events into
/// category N" button. Same-LSN coalescing means one click = one
/// Diff event with `BULK_ADD_BATCH` rows, regardless of how big the
/// batch is.
const BULK_ADD_BATCH = 1_000;

type Filter = "all" | "published" | "draft";

const FILTERS: { id: Filter; label: string; predicate: string | null }[] = [
  { id: "all", label: "All", predicate: null },
  { id: "published", label: "Published", predicate: "published = true" },
  { id: "draft", label: "Drafts", predicate: "published = false" },
];

const CHART_SQL = `WITH stats AS (
  SELECT published, COUNT(*) AS n
  FROM posts
  GROUP BY published
)
SELECT published, n
FROM stats
ORDER BY published`;

/// Multi-CTE aggregate against the 300k-row `events` table. Chains a
/// per-category rollup through a sort-and-limit. The server compiles
/// this into a differential-dataflow pipeline (BaseTable → Aggregate
/// → TopK) and ships only the ~50 aggregate rows the query produces,
/// not the 300k raw events.
const EVENT_STATS_SQL = `WITH per_category AS (
  SELECT category_id,
         COUNT(*) AS n,
         SUM(value) AS total,
         AVG(value) AS avg_value
  FROM events
  GROUP BY category_id
)
SELECT category_id, n, total, avg_value
FROM per_category
ORDER BY total DESC
LIMIT 8`;

/// Permission rule as configured on the server (see
/// `examples/demo-app/server/src/main.rs::permission_rules`). Shown
/// verbatim so the user can read the same predicate the SyncEngine's
/// rewriter compiles into the query graph.
const PERMISSION_RULE = {
  table: "posts",
  predicate: "published = true OR $user.is_admin = true",
};

function listSqlFor(filter: Filter): string {
  const where = FILTERS.find((f) => f.id === filter)?.predicate;
  return where
    ? `SELECT id, title, published\nFROM posts\nWHERE ${where}`
    : "SELECT id, title, published\nFROM posts";
}

export default function App() {
  const api = useMemo(() => new ApiClient(), []);
  const [draftTitle, setDraftTitle] = useState("");
  const [writeError, setWriteError] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("all");

  // User picker state. `users` is populated once from /api/users;
  // `currentUser` drives which token we request from /api/token. The
  // hook below re-keys on `token`, so a switch triggers a fresh
  // wasm Client + fresh subscriptions under the new user context.
  const [users, setUsers] = useState<DemoUser[]>([]);
  const [currentUser, setCurrentUser] = useState<DemoUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);

  // Load the persona list once; default to the first entry so the
  // page has a connection on first paint.
  useEffect(() => {
    let alive = true;
    api
      .listUsers()
      .then((list) => {
        if (!alive) return;
        setUsers(list);
        if (list.length > 0 && !currentUser) {
          setCurrentUser(list[0]);
        }
      })
      .catch((e) => {
        if (!alive) return;
        setAuthError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
    // Mount-only: we want a single initial list fetch regardless of
    // `api` / `currentUser` identity churn.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // When the selected persona changes, request a fresh JWT. The
  // wasm client will reconnect with the new token via the WS
  // `?token=…` query the bridge re-presents to gRPC.
  useEffect(() => {
    if (!currentUser) return;
    let alive = true;
    setAuthError(null);
    api
      .fetchToken(currentUser.id)
      .then((res) => {
        if (!alive) return;
        setToken(res.token);
      })
      .catch((e) => {
        if (!alive) return;
        setAuthError(e instanceof Error ? e.message : String(e));
        setToken(null);
      });
    return () => {
      alive = false;
    };
  }, [api, currentUser]);

  // Don't initiate the wasm client until we have a token. With an
  // empty URL the parser refuses synchronously, so no `ConnectionTask`
  // spawns and no WS-without-auth retry loop materializes against the
  // bridge. Once `token` flips from null → string, the deps change
  // and a real client connects.
  const clientOpts = useMemo(
    () =>
      token
        ? { url: PALIMPSEST_URL, wasm, token }
        : { url: "", wasm, token: undefined },
    [token],
  );
  const {
    client,
    status: clientStatus,
    error: clientError,
  } = usePalimpsestClient(clientOpts);

  const listSql = useMemo(() => listSqlFor(filter), [filter]);

  const {
    status: listStatus,
    rows: listRows,
    error: listError,
  } = usePalimpsestSubscription<Post>(client, listSql, {
    decoder: { coerceSafeIntegersToNumber: false },
  });

  const { rows: chartRows, error: chartError } =
    usePalimpsestSubscription<PostsByStatusRow>(client, CHART_SQL, {
      decoder: { coerceSafeIntegersToNumber: false },
    });

  // Subscription 3: aggregate over the 300k-row `events` table. The
  // server compiles the CTE+aggregate+TopK SQL into a dataflow and
  // ships only the ~50 aggregate rows; the client just renders them.
  // Live diffs flow through the persistent host's
  // `push_table_batch`: each WAL mutation produces aggregate
  // retract+assert deltas the client merges by PK.
  const { rows: eventRows, error: eventError } =
    usePalimpsestSubscription<EventAggregateRow>(client, EVENT_STATS_SQL, {
      decoder: { coerceSafeIntegersToNumber: false },
    });

  const overallStatus = client ? listStatus : clientStatus;
  const connectionError = clientError ?? listError ?? chartError ?? eventError;

  // The server now runs the permission filter through the compiled
  // dataflow, so `listRows` is *already* filtered down to what this
  // persona is allowed to see. We just narrow further when the user
  // clicks a filter chip.
  const visiblePosts = useMemo(() => {
    if (filter === "all") return listRows;
    const wantPublished = filter === "published";
    return listRows.filter((p) => p.published === wantPublished);
  }, [listRows, filter]);

  // `chartRows` is the *aggregate* — one row per `published` value
  // with `n` already computed by the dataflow. Read `n` directly;
  // don't recount.
  const counts = useMemo(() => {
    let published = 0;
    let draft = 0;
    for (const row of chartRows) {
      const n = Number(row.n);
      if (row.published) published = n;
      else draft = n;
    }
    return { published, draft };
  }, [chartRows]);
  const total = counts.published + counts.draft;

  // Server-side aggregation: `eventRows` already contains exactly
  // the top categories, ordered + limited per the SQL's
  // `ORDER BY total DESC LIMIT 8`. No client-side aggregation needed.
  const eventTopCategories = useMemo(
    () =>
      eventRows.map((row) => ({
        categoryId: String(row.category_id),
        count: Number(row.n),
        total: row.total,
        avg: row.avg_value,
      })),
    [eventRows],
  );

  /// Sum of the aggregate's `n` column — i.e. the total number of
  /// events across the categories the server sent us. The aggregate
  /// only ships the top-N categories, so for >N categories this is a
  /// lower bound; for the demo's 50 categories vs LIMIT 8 it is.
  const eventTotalRows = eventTopCategories.reduce(
    (sum, row) => sum + row.count,
    0,
  );

  async function withWrite<T>(fn: () => Promise<T>): Promise<void> {
    setWriteError(null);
    try {
      await fn();
      // Live diffs flow through the persistent host on the server.
    } catch (e) {
      setWriteError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onCreate(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const title = draftTitle.trim();
    if (!title) return;
    setDraftTitle("");
    await withWrite(() => api.createPost(title, true));
  }

  async function onBulkAddEvents(categoryId: number) {
    await withWrite(() => api.bulkAddEvents(categoryId, BULK_ADD_BATCH));
  }

  // Background traffic generator — every 5 seconds, push a random
  // batch into a random category through the regular write API.
  // Keeps the bubble chart visibly moving so the live-diff path is
  // obvious without the user clicking anything.
  const [autoWrite, setAutoWrite] = useState(true);
  useEffect(() => {
    if (!autoWrite || overallStatus !== "open") return;
    const tick = async () => {
      const categoryId = 1 + Math.floor(Math.random() * 50);
      const count = 200 + Math.floor(Math.random() * 600);
      const baseValue = 50 + Math.floor(Math.random() * 500);
      try {
        await api.bulkAddEvents(categoryId, count, baseValue);
      } catch (err) {
        // Don't surface auto-write errors into the user-facing banner;
        // a brief network hiccup shouldn't look like a write failure.
        console.warn("auto-write failed", err);
      }
    };
    const handle = window.setInterval(tick, 5000);
    return () => window.clearInterval(handle);
  }, [api, autoWrite, overallStatus]);

  return (
    <>
      <h1>Palimpsest demo</h1>
      <p className="tagline">
        Two live subscriptions over the same <code>posts</code> table,
        multiplexed onto one WebSocket. Each persona authenticates with
        its own JWT and the SyncEngine compiles row-visibility filters
        into the query graph per <code>$user.*</code> context.
      </p>

      <section className="panel">
        <header className="panel-head">
          <h2>Persona</h2>
          <div className="filters" role="group" aria-label="Choose user">
            {users.map((u) => (
              <button
                key={u.id}
                type="button"
                className={`chip ${
                  currentUser?.id === u.id ? "chip-active" : ""
                }`}
                onClick={() => setCurrentUser(u)}
              >
                {u.display_name}
              </button>
            ))}
          </div>
        </header>
        <p className="rule-text">
          Active permission rule on <code>{PERMISSION_RULE.table}</code>:{" "}
          <code>{PERMISSION_RULE.predicate}</code>
        </p>
        <p className="rule-text">
          User context: <code>$user.id = "{currentUser?.id ?? "—"}"</code>
          {" · "}
          <code>
            $user.is_admin = {currentUser?.is_admin ? "true" : "false"}
          </code>
        </p>
        {authError && <div className="error-banner">{authError}</div>}
      </section>

      <p>
        connection:{" "}
        <span className={`status ${overallStatus}`}>{overallStatus}</span>
        {" · "}
        <strong>{listRows.length}</strong> rows after rule + filter
        {connectionError && (
          <span className="status error">
            {" "}
            {"code" in connectionError
              ? `${connectionError.code}: ${connectionError.message}`
              : connectionError.message}
          </span>
        )}
      </p>

      <form className="create" onSubmit={onCreate}>
        <input
          type="text"
          placeholder="New post title…"
          value={draftTitle}
          onChange={(e) => setDraftTitle(e.target.value)}
        />
        <button
          type="submit"
          disabled={overallStatus !== "open" || draftTitle.trim() === ""}
        >
          Create
        </button>
      </form>

      {writeError && <div className="error-banner">{writeError}</div>}

      <section className="panel">
        <header className="panel-head">
          <h2>Subscription 1 — filtered list</h2>
          <div className="filters" role="group" aria-label="Filter posts">
            {FILTERS.map((f) => (
              <button
                key={f.id}
                type="button"
                className={`chip ${filter === f.id ? "chip-active" : ""}`}
                onClick={() => setFilter(f.id)}
              >
                {f.label}
              </button>
            ))}
          </div>
        </header>
        <pre className="sql" data-active-filter={filter}>
          {listSql}
        </pre>

        {overallStatus === "open" && visiblePosts.length === 0 && (
          <p className="empty">No posts match this filter for {currentUser?.display_name ?? "you"}.</p>
        )}

        <ul className="posts">
          {visiblePosts.map((post) => (
            <li key={String(post.id)}>
              <span className="post-id">#{String(post.id)}</span>
              <span className="post-title">{post.title}</span>
              <span
                className={`badge ${post.published ? "published" : "draft"}`}
              >
                {post.published ? "published" : "draft"}
              </span>
              <button
                className="row-action"
                onClick={() =>
                  withWrite(() =>
                    api.setPublished(Number(post.id), !post.published),
                  )
                }
              >
                {post.published ? "Unpublish" : "Publish"}
              </button>
              <button
                className="row-action"
                onClick={() => withWrite(() => api.deletePost(Number(post.id)))}
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>Subscription 2 — counts by status (CTE)</h2>
        </header>
        <pre className="sql">{CHART_SQL}</pre>

        <Chart
          label="published"
          value={counts.published}
          total={total}
          accent="published"
        />
        <Chart
          label="draft"
          value={counts.draft}
          total={total}
          accent="draft"
        />
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>
            Subscription 3 — top categories over{" "}
            <strong>{eventTotalRows.toLocaleString()}</strong> events
          </h2>
          <div className="filters" role="group" aria-label="Inject events">
            <label className="auto-toggle">
              <input
                type="checkbox"
                checked={autoWrite}
                onChange={(e) => setAutoWrite(e.target.checked)}
              />
              auto-write 5s
            </label>
            {[3, 17, 42].map((categoryId) => (
              <button
                key={categoryId}
                type="button"
                className="chip"
                disabled={overallStatus !== "open"}
                onClick={() => onBulkAddEvents(categoryId)}
              >
                +{BULK_ADD_BATCH.toLocaleString()} → cat {categoryId}
              </button>
            ))}
          </div>
        </header>
        <pre className="sql">{EVENT_STATS_SQL}</pre>

        {eventTopCategories.length === 0 && overallStatus === "open" && (
          <p className="empty">Waiting for first aggregate snapshot…</p>
        )}

        {eventTopCategories.length > 0 && (
          <EventBubbleChart rows={eventTopCategories} />
        )}
      </section>

      <footer>
        Transport: WebSocket (with <code>?token=…</code>) → in-process
        gRPC bridge → Palimpsest SyncEngine. Auth: HS256 JWT verified
        by <code>JwtAuthenticator</code>.
      </footer>
    </>
  );
}

interface ChartProps {
  label: string;
  value: number;
  total: number;
  accent: "published" | "draft";
}

function Chart({ label, value, total, accent }: ChartProps) {
  const pct = total === 0 ? 0 : (value / total) * 100;
  return (
    <div className="bar-row">
      <span className="bar-label">{label}</span>
      <div className="bar-track">
        <div className={`bar-fill bar-${accent}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="bar-value">{value}</span>
    </div>
  );
}

interface BubbleRow {
  categoryId: string;
  count: number;
  total: bigint;
  avg: number;
}

/// Multi-axis bubble chart over the live aggregate. Each bubble is
/// one of the top categories returned by the dataflow's TopK; its
/// position encodes count + average value, its area encodes total
/// (= count × avg, the same column the SQL orders by), and its hue
/// is a stable hash of the category id so a category keeps the same
/// color as it slides around on incoming diffs.
///
/// Pure inline SVG: no chart library. SVG `cx`/`cy`/`r` transitions
/// are CSS properties so the bubble animates smoothly when the
/// dataflow ships a retract+assert pair at a single LSN.
function EventBubbleChart({ rows }: { rows: BubbleRow[] }) {
  const width = 760;
  const height = 360;
  const pad = { top: 24, right: 28, bottom: 48, left: 64 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;

  const counts = rows.map((r) => r.count);
  const avgs = rows.map((r) => r.avg);
  const totals = rows.map((r) => Number(r.total));

  const xLo = Math.min(...counts);
  const xHi = Math.max(...counts);
  const yLo = Math.min(...avgs);
  const yHi = Math.max(...avgs);
  const tMax = Math.max(...totals, 1);

  // Pad the data range so points never sit exactly on the axis.
  const xRange = Math.max(xHi - xLo, 1);
  const yRange = Math.max(yHi - yLo, 1);
  const xMin = xLo - xRange * 0.12;
  const xMax = xHi + xRange * 0.12;
  const yMin = Math.max(0, yLo - yRange * 0.15);
  const yMax = yHi + yRange * 0.15;

  const sx = (v: number) =>
    pad.left + ((v - xMin) / (xMax - xMin || 1)) * innerW;
  const sy = (v: number) =>
    pad.top + innerH - ((v - yMin) / (yMax - yMin || 1)) * innerH;
  const sr = (v: number) => 10 + Math.sqrt(v / tMax) * 32;

  // Golden-angle hue mapping → distinct, stable colors per category.
  const hue = (id: string) => (Number(id) * 137.508) % 360;

  const xTicks = Array.from({ length: 4 }, (_, i) =>
    Math.round(xMin + ((xMax - xMin) * i) / 3),
  );
  const yTicks = Array.from({ length: 4 }, (_, i) =>
    yMin + ((yMax - yMin) * i) / 3,
  );

  return (
    <svg
      className="bubble-chart"
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Bubble chart of top categories"
    >
      {/* horizontal gridlines */}
      {yTicks.map((y, i) => (
        <line
          key={`gy-${i}`}
          x1={pad.left}
          x2={width - pad.right}
          y1={sy(y)}
          y2={sy(y)}
          className="bubble-grid"
        />
      ))}
      {/* axes */}
      <line
        x1={pad.left}
        x2={width - pad.right}
        y1={height - pad.bottom}
        y2={height - pad.bottom}
        className="bubble-axis"
      />
      <line
        x1={pad.left}
        x2={pad.left}
        y1={pad.top}
        y2={height - pad.bottom}
        className="bubble-axis"
      />
      {/* x tick labels */}
      {xTicks.map((x, i) => (
        <text
          key={`tx-${i}`}
          x={sx(x)}
          y={height - pad.bottom + 18}
          className="bubble-tick"
          textAnchor="middle"
        >
          {x.toLocaleString()}
        </text>
      ))}
      {/* y tick labels */}
      {yTicks.map((y, i) => (
        <text
          key={`ty-${i}`}
          x={pad.left - 10}
          y={sy(y) + 4}
          className="bubble-tick"
          textAnchor="end"
        >
          {y.toFixed(0)}
        </text>
      ))}
      {/* axis titles */}
      <text
        x={pad.left + innerW / 2}
        y={height - 8}
        className="bubble-axis-label"
        textAnchor="middle"
      >
        rows per category (COUNT(*))
      </text>
      <text
        x={16}
        y={pad.top + innerH / 2}
        className="bubble-axis-label"
        textAnchor="middle"
        transform={`rotate(-90 16 ${pad.top + innerH / 2})`}
      >
        avg value (AVG(value))
      </text>
      {/* bubbles */}
      {rows.map((row) => {
        const h = hue(row.categoryId);
        const fill = `hsl(${h}deg 75% 55% / 0.55)`;
        const stroke = `hsl(${h}deg 70% 40%)`;
        return (
          <g key={row.categoryId} className="bubble">
            <circle
              cx={sx(row.count)}
              cy={sy(row.avg)}
              r={sr(Number(row.total))}
              style={{ fill, stroke }}
            />
            <text
              x={sx(row.count)}
              y={sy(row.avg) + 4}
              className="bubble-label"
              textAnchor="middle"
            >
              {row.categoryId}
            </text>
            <title>
              cat {row.categoryId} · {row.count.toLocaleString()} rows · avg{" "}
              {row.avg.toFixed(1)} · total {row.total.toLocaleString()}
            </title>
          </g>
        );
      })}
    </svg>
  );
}
