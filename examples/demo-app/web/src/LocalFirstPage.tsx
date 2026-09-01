// Local-first replica demo page.
//
// A real Postgres-in-WASM engine (pglite) runs inside this tab. The
// Palimpsest `LocalReplica` streams the server's *permissioned* subset
// of `posts` into it — one atomic local transaction per remote commit —
// so the same SQL that powers the live subscriptions on the other page
// runs here against local storage: instant answers, no round trip.
//
// Writes are optimistic: `replica.mutate(...)` updates the local
// engine immediately, forwards the mutation through the demo's regular
// HTTP write API, and reconciles when the authoritative change comes
// back through the WAL (settle / rebase / conflict / rollback). The
// event log at the bottom shows each step happening.

import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { PGlite } from "@electric-sql/pglite";
import {
  postgresWasmDriver,
  type LocalReplicaHandle,
  type ReplicaEvent,
  type TableSyncStatus,
  type WriteRequest,
} from "@palimpsest/client";
import {
  subscribeReplicaEvents,
  useLocalQuery,
  usePalimpsestClient,
  usePalimpsestSubscription,
} from "@palimpsest/client/react";
import * as wasm from "../pkg/palimpsest_client_js";
import { ApiClient, DemoUser, PALIMPSEST_URL } from "./api";

// ---------------------------------------------------------------------------
// Local engine (one per tab)

// One pglite instance per tab, shared across StrictMode double-mounts
// and persona switches. In-memory: reloading the tab restarts from a
// fresh snapshot, which keeps the demo deterministic.
let pgPromise: Promise<PGlite> | null = null;
function getLocalPostgres(): Promise<PGlite> {
  if (!pgPromise) pgPromise = PGlite.create();
  return pgPromise;
}

// ---------------------------------------------------------------------------
// Queries

/// Queries runnable against BOTH engines: the server compiles them into
/// live dataflows; pglite executes them locally against the mirror.
const QUERY_PRESETS: { id: string; label: string; sql: string }[] = [
  {
    id: "list",
    label: "Filtered list",
    sql: `SELECT id, title, published
FROM posts
WHERE published = true
ORDER BY id`,
  },
  {
    id: "counts",
    label: "Counts by status (CTE)",
    sql: `WITH stats AS (
  SELECT published, COUNT(*) AS n
  FROM posts
  GROUP BY published
)
SELECT published, n
FROM stats
ORDER BY published`,
  },
  {
    id: "search",
    label: "Title search",
    sql: `SELECT id, title, published
FROM posts
WHERE title LIKE '%o%'
ORDER BY id`,
  },
];

const POSTS_SQL = "SELECT id, title, published FROM posts ORDER BY id";

interface PostRow {
  id: bigint | number;
  title: string;
  published: boolean;
}

// ---------------------------------------------------------------------------
// Page

export default function LocalFirstPage() {
  const api = useMemo(() => new ApiClient(), []);

  // Persona/token handling — same flow as the live-queries page: the
  // replica's mirror runs under this persona's permission rules, so
  // switching personas re-snapshots the local database to a different
  // permissioned subset.
  const [users, setUsers] = useState<DemoUser[]>([]);
  const [currentUser, setCurrentUser] = useState<DemoUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    api
      .listUsers()
      .then((list) => {
        if (!alive) return;
        setUsers(list);
        setCurrentUser((prev) => prev ?? list[0] ?? null);
      })
      .catch((e) => {
        if (alive) setAuthError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
  }, [api]);

  useEffect(() => {
    if (!currentUser) return;
    let alive = true;
    setAuthError(null);
    api
      .fetchToken(currentUser.id)
      .then((res) => {
        if (alive) setToken(res.token);
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

  const clientOpts = useMemo(
    () =>
      token
        ? { url: PALIMPSEST_URL, wasm, token }
        : { url: "", wasm, token: undefined },
    [token],
  );
  const { client, connection, error: clientError } =
    usePalimpsestClient(clientOpts);

  // -------------------------------------------------------------------------
  // Replica lifecycle: (client, pglite) -> LocalReplica

  const [replica, setReplica] = useState<LocalReplicaHandle | null>(null);
  const [replicaError, setReplicaError] = useState<string | null>(null);

  useEffect(() => {
    if (!client) {
      setReplica(null);
      return;
    }
    let alive = true;
    let started: LocalReplicaHandle | null = null;

    // The writer is the demo's ordinary HTTP write API — Palimpsest
    // never inserts itself into the write path. Rejecting rolls the
    // optimistic local change back.
    const writer = async (req: WriteRequest) => {
      if (req.table !== "posts") {
        throw new Error(`no write path for table \`${req.table}\``);
      }
      if (req.kind === "insert") {
        await api.createPost(
          String(req.values.title ?? ""),
          Boolean(req.values.published),
          Number(req.values.id),
        );
      } else if (req.kind === "update") {
        const keys = Object.keys(req.set);
        if (keys.length !== 1 || keys[0] !== "published") {
          throw new Error("demo write API only supports toggling `published`");
        }
        await api.setPublished(Number(req.key.id), Boolean(req.set.published));
      } else {
        await api.deletePost(Number(req.key.id));
      }
    };

    (async () => {
      const pg = await getLocalPostgres();
      const handle = await client.localReplica({
        database: postgresWasmDriver(pg),
        mirrors: ["posts"],
        writer,
      });
      if (!alive) {
        void handle.stop();
        return;
      }
      started = handle;
      setReplicaError(null);
      setReplica(handle);
    })().catch((e) => {
      if (alive) setReplicaError(e instanceof Error ? e.message : String(e));
    });

    return () => {
      alive = false;
      setReplica(null);
      if (started) void started.stop();
    };
  }, [client, api]);

  // -------------------------------------------------------------------------
  // Replica events → sync status, pending count, event log

  const [postsState, setPostsState] = useState<TableSyncStatus | null>(null);
  const [pending, setPending] = useState(0);
  const [events, setEvents] = useState<
    { at: string; nonce: number; event: ReplicaEvent }[]
  >([]);
  const eventNonce = useRef(0);

  useEffect(() => {
    if (!replica) {
      setPostsState(null);
      setPending(0);
      setEvents([]);
      return;
    }
    setPostsState(replica.tableStates()["posts"] ?? null);
    const unsubscribe = subscribeReplicaEvents(replica, (event) => {
      if (event.kind === "tableState" && event.table === "posts") {
        setPostsState(event.state);
      }
      eventNonce.current += 1;
      const entry = {
        at: new Date().toLocaleTimeString(undefined, { hour12: false }),
        nonce: eventNonce.current,
        event,
      };
      setEvents((prev) => [entry, ...prev].slice(0, 14));
      void replica.pendingMutations("posts").then(setPending);
    });
    return unsubscribe;
  }, [replica]);

  // -------------------------------------------------------------------------
  // Panel 1 — same query against both engines

  const [presetId, setPresetId] = useState(QUERY_PRESETS[0].id);
  const sql = QUERY_PRESETS.find((p) => p.id === presetId)!.sql;

  // Server side: an ordinary live subscription.
  const { rows: serverRows, status: serverStatus } = usePalimpsestSubscription<
    Record<string, unknown>
  >(client, sql, { decoder: { coerceSafeIntegersToNumber: false } });

  // Local side: the same SQL against pglite, re-run on every applied
  // batch. Timed so the "no round trip" point is visible.
  const [local, setLocal] = useState<{
    columns: string[];
    rows: unknown[][];
    ms: number;
  } | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  useEffect(() => {
    if (!replica) {
      setLocal(null);
      return;
    }
    let alive = true;
    const run = async () => {
      try {
        const begin = performance.now();
        const result = (await replica.query(sql)) as {
          rows: unknown[][];
          fields?: { name: string }[];
        };
        const ms = performance.now() - begin;
        if (!alive) return;
        setLocalError(null);
        setLocal({
          columns: (result.fields ?? []).map((f) => f.name),
          rows: result.rows,
          ms,
        });
      } catch (e) {
        if (alive) setLocalError(e instanceof Error ? e.message : String(e));
      }
    };
    void run();
    const unsubscribe = subscribeReplicaEvents(replica, (event) => {
      if (
        event.kind === "applied" ||
        event.kind === "mutationFailed" ||
        event.kind === "mutationConflicted"
      ) {
        void run();
      }
    });
    return () => {
      alive = false;
      unsubscribe();
    };
  }, [replica, sql]);

  // -------------------------------------------------------------------------
  // Panel 2 — optimistic mutations over the mirrored posts

  const postsQuery = useLocalQuery(replica, POSTS_SQL, { tables: ["posts"] });
  const posts: PostRow[] = postsQuery.rows.map((row) => ({
    id: row[0] as bigint | number,
    title: String(row[1] ?? ""),
    published: Boolean(row[2]),
  }));

  const [draftTitle, setDraftTitle] = useState("");
  const [mutateError, setMutateError] = useState<string | null>(null);

  async function mutate(mutation: Parameters<LocalReplicaHandle["mutate"]>[0]) {
    if (!replica) return;
    setMutateError(null);
    try {
      await replica.mutate(mutation);
      void replica.pendingMutations("posts").then(setPending);
    } catch (e) {
      setMutateError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onCreate(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const title = draftTitle.trim();
    if (!title) return;
    setDraftTitle("");
    // Client-chosen id (timestamp-scale, far above the server's
    // BIGSERIAL sequence) so the optimistic row and the WAL row share
    // a primary key — that's what lets the insert settle.
    await mutate({
      table: "posts",
      insert: { id: Date.now(), title, published: true },
    });
  }

  const live = postsState?.kind === "live";

  return (
    <>
      <h1>Local-first replica</h1>
      <p className="tagline">
        A Postgres-in-WASM engine (pglite) runs in this tab. Palimpsest
        streams the <em>permissioned subset</em> of <code>posts</code> into it
        and reconciles optimistic writes against the WAL — reads are local,
        writes are instant, the server stays authoritative.
      </p>

      <section className="panel">
        <header className="panel-head">
          <h2>Persona &amp; sync state</h2>
          <div className="filters" role="group" aria-label="Choose user">
            {users.map((u) => (
              <button
                key={u.id}
                type="button"
                className={`chip ${currentUser?.id === u.id ? "chip-active" : ""}`}
                onClick={() => setCurrentUser(u)}
              >
                {u.display_name}
              </button>
            ))}
          </div>
        </header>
        <p className="rule-text">
          Switching personas re-subscribes the mirror under the new
          user&apos;s permission rules — watch the local snapshot get replaced
          with a different subset. The permissions playground on the other
          page applies here too: rule changes resync this mirror live.
        </p>
        <p className="lf-status-row">
          <StatusChip
            label="connection"
            tone={connection?.kind === "connected" ? "ok" : "warn"}
            value={connection?.kind ?? "starting"}
          />
          <StatusChip
            label="posts mirror"
            tone={live ? "ok" : postsState?.kind === "errored" ? "err" : "warn"}
            value={
              postsState
                ? postsState.kind === "live"
                  ? `live @ lsn ${postsState.lsn.toString()}`
                  : postsState.kind === "errored"
                    ? `errored — ${postsState.message}`
                    : postsState.kind
                : replica
                  ? "starting"
                  : "waiting for local engine"
            }
          />
          <StatusChip
            label="pending optimistic writes"
            tone={pending > 0 ? "warn" : "ok"}
            value={String(pending)}
          />
        </p>
        {authError && <div className="error-banner">{authError}</div>}
        {clientError && (
          <div className="error-banner">
            {"code" in clientError
              ? `${clientError.code}: ${clientError.message}`
              : clientError.message}
          </div>
        )}
        {replicaError && <div className="error-banner">{replicaError}</div>}
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>Same query, two engines</h2>
          <div className="filters" role="group" aria-label="Query presets">
            {QUERY_PRESETS.map((preset) => (
              <button
                key={preset.id}
                type="button"
                className={`chip ${presetId === preset.id ? "chip-active" : ""}`}
                onClick={() => setPresetId(preset.id)}
              >
                {preset.label}
              </button>
            ))}
          </div>
        </header>
        <pre className="sql">{sql}</pre>
        <div className="lf-compare">
          <div className="lf-compare-col">
            <h3 className="lf-col-title">
              server — live subscription{" "}
              <span className="lf-col-hint">({serverStatus})</span>
            </h3>
            <ResultTable
              columns={serverRows[0] ? Object.keys(serverRows[0]) : []}
              rows={serverRows.map((row) => Object.values(row))}
            />
          </div>
          <div className="lf-compare-col">
            <h3 className="lf-col-title">
              local — pglite mirror{" "}
              {local && (
                <span className="lf-col-hint">
                  answered in {local.ms.toFixed(1)} ms, zero round trips
                </span>
              )}
            </h3>
            {localError ? (
              <div className="error-banner">{localError}</div>
            ) : (
              <ResultTable
                columns={local?.columns ?? []}
                rows={local?.rows ?? []}
              />
            )}
          </div>
        </div>
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>Optimistic writes</h2>
        </header>
        <p className="rule-text">
          Every action lands in the local engine <em>immediately</em>, then
          flows through the ordinary HTTP write API. When the change comes
          back through the WAL the pending mutation <strong>settles</strong>;
          a failed write <strong>rolls back</strong>; a disagreement after
          the API accepted it means the <strong>server wins</strong>.
        </p>

        <form className="create" onSubmit={onCreate}>
          <input
            type="text"
            placeholder="New post title (optimistic insert)…"
            value={draftTitle}
            onChange={(e) => setDraftTitle(e.target.value)}
          />
          <button type="submit" disabled={!live || draftTitle.trim() === ""}>
            Create locally
          </button>
        </form>
        {mutateError && <div className="error-banner">{mutateError}</div>}

        {postsQuery.status === "error" && (
          <div className="error-banner">{postsQuery.error}</div>
        )}
        {posts.length === 0 && live && (
          <p className="empty">
            No posts visible for {currentUser?.display_name ?? "you"}.
          </p>
        )}
        <ul className="posts">
          {posts.map((post) => (
            <li key={String(post.id)}>
              <span className="post-id">#{String(post.id)}</span>
              <span className="post-title">{post.title}</span>
              <span className={`badge ${post.published ? "published" : "draft"}`}>
                {post.published ? "published" : "draft"}
              </span>
              <button
                className="row-action"
                disabled={!live}
                onClick={() =>
                  mutate({
                    table: "posts",
                    key: { id: post.id },
                    set: { published: !post.published },
                  })
                }
              >
                {post.published ? "Unpublish" : "Publish"}
              </button>
              <button
                className="row-action"
                disabled={!live}
                onClick={() =>
                  mutate({ table: "posts", key: { id: post.id }, delete: true })
                }
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>Reconciliation event log</h2>
        </header>
        {events.length === 0 && <p className="empty">No events yet.</p>}
        <ul className="lf-events">
          {events.map(({ at, nonce, event }) => (
            <li key={nonce}>
              <span className="lf-event-time">{at}</span>
              <span className={`lf-event-kind lf-event-${event.kind}`}>
                {event.kind}
              </span>
              <span className="lf-event-detail">{describeEvent(event)}</span>
            </li>
          ))}
        </ul>
      </section>

      <footer>
        Local engine: pglite (Postgres compiled to WASM) ·
        mirror transport: the same WebSocket/gRPC subscription stream as the
        live-queries page · write path: the demo&apos;s HTTP API.
      </footer>
    </>
  );
}

// ---------------------------------------------------------------------------
// Presentational helpers

function describeEvent(event: ReplicaEvent): string {
  switch (event.kind) {
    case "tableState":
      return `\`${event.table}\` → ${
        event.state.kind === "live"
          ? `live @ lsn ${event.state.lsn.toString()}`
          : event.state.kind === "errored"
            ? `errored — ${event.state.message}`
            : event.state.kind
      }`;
    case "applied":
      return `\`${event.table}\` applied batch @ lsn ${event.lsn.toString()}`;
    case "mutationSettled":
      return `mutation #${event.token} confirmed by the WAL round-trip`;
    case "mutationConflicted":
      return `mutation #${event.token} lost to the server's version`;
    case "mutationFailed":
      return `mutation #${event.token} rolled back — ${event.error}`;
    case "mirrorError":
      return `\`${event.table}\` ${event.code}: ${event.message}`;
  }
}

function StatusChip({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "ok" | "warn" | "err";
}) {
  return (
    <span className={`lf-chip lf-chip-${tone}`}>
      <span className="lf-chip-label">{label}</span>
      {value}
    </span>
  );
}

function ResultTable({
  columns,
  rows,
}: {
  columns: string[];
  rows: unknown[][];
}) {
  if (rows.length === 0) {
    return <p className="empty">0 rows</p>;
  }
  return (
    <table className="lf-table">
      {columns.length > 0 && (
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
      )}
      <tbody>
        {rows.map((row, i) => (
          <tr key={i}>
            {row.map((cell, j) => (
              <td key={j}>{formatCell(cell)}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function formatCell(cell: unknown): string {
  if (cell === null || cell === undefined) return "∅";
  if (typeof cell === "boolean") return cell ? "true" : "false";
  return String(cell);
}
