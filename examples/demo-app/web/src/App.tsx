import { FormEvent, useMemo, useState } from "react";
import {
  usePalimpsestClient,
  usePalimpsestSubscription,
} from "@palimpsest/client/react";
import * as wasm from "../pkg/palimpsest_client_js";
import { ApiClient, PALIMPSEST_URL } from "./api";

interface Post {
  id: bigint;
  title: string;
  published: boolean;
}

type Filter = "all" | "published" | "draft";

const FILTERS: { id: Filter; label: string; predicate: string | null }[] = [
  { id: "all", label: "All", predicate: null },
  { id: "published", label: "Published", predicate: "published = true" },
  { id: "draft", label: "Drafts", predicate: "published = false" },
];

/// SQL the chart subscription registers. Demonstrates a CTE +
/// aggregate. The server in v1 still ships the raw `posts` rows back
/// regardless of the query graph (router.rs:218 — projection /
/// filter / aggregation aren't wired through dataflow yet); the
/// browser applies the aggregation client-side. The SQL text below
/// is, however, exactly what the wasm client subscribes with, so the
/// query is parsed + planned server-side end-to-end.
const CHART_SQL = `WITH stats AS (
  SELECT published, COUNT(*) AS n
  FROM posts
  GROUP BY published
)
SELECT published, n
FROM stats
ORDER BY published`;

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

  const clientOpts = useMemo(() => ({ url: PALIMPSEST_URL, wasm }), []);
  const {
    client,
    status: clientStatus,
    error: clientError,
  } = usePalimpsestClient(clientOpts);

  const listSql = useMemo(() => listSqlFor(filter), [filter]);

  // Subscription 1: filtered list. SQL changes when `filter` changes
  // — the hook tears the old subscription down and opens a fresh one
  // with the new text. Watch the server log on filter change to see
  // both events fly past.
  const {
    status: listStatus,
    rows: listRows,
    error: listError,
  } = usePalimpsestSubscription<Post>(client, listSql, {
    decoder: { coerceSafeIntegersToNumber: false },
  });

  // Subscription 2: counts-by-status. Stays open for the lifetime of
  // the page; live diffs flow in independently of the list view.
  const {
    rows: chartRows,
    error: chartError,
  } = usePalimpsestSubscription<Post>(client, CHART_SQL, {
    decoder: { coerceSafeIntegersToNumber: false },
  });

  const overallStatus = client ? listStatus : clientStatus;
  const connectionError = clientError ?? listError ?? chartError;

  // Client-side filter: the router ships every row regardless of the
  // WHERE clause in v1, so apply the predicate here. The SQL the user
  // sees on the left is the actual subscription text — when v2 wires
  // dataflow execution this branch goes away.
  const visiblePosts = useMemo(() => {
    if (filter === "all") return listRows;
    const wantPublished = filter === "published";
    return listRows.filter((p) => p.published === wantPublished);
  }, [listRows, filter]);

  // Client-side aggregation for the chart subscription.
  const counts = useMemo(() => {
    let published = 0;
    let draft = 0;
    for (const p of chartRows) {
      if (p.published) published += 1;
      else draft += 1;
    }
    return { published, draft };
  }, [chartRows]);
  const total = counts.published + counts.draft;

  async function withWrite<T>(fn: () => Promise<T>): Promise<void> {
    setWriteError(null);
    try {
      await fn();
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

  return (
    <>
      <h1>Palimpsest demo</h1>
      <p className="tagline">
        Two live subscriptions over the same <code>posts</code> table,
        multiplexed onto one WebSocket. Writes round-trip through the
        Rust write API on <code>localhost:3000</code> and propagate
        back as diff events.
      </p>

      <p>
        connection:{" "}
        <span className={`status ${overallStatus}`}>{overallStatus}</span>
        {" · "}
        <strong>{listRows.length}</strong> rows in list,{" "}
        <strong>{total}</strong> in chart
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
          <p className="empty">No posts match this filter.</p>
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

      <footer>
        Transport: WebSocket → in-process gRPC bridge → Palimpsest
        SyncEngine.
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
