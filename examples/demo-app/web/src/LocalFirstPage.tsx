// Local-first replica demo page.
//
// A real Postgres-in-WASM engine (pglite) runs inside this tab. The
// Palimpsest `LocalReplica` streams the server's *permissioned* subset
// of `issues` into it — one atomic local transaction per remote commit
// — so the same SQL that powers the board and the analytics runs here
// against local storage: instant answers, no round trip.
//
// Writes are optimistic: `replica.mutate(...)` updates the local
// engine immediately, forwards the mutation through the demo's regular
// HTTP write API, and reconciles when the change comes back through
// the WAL (settle / rebase / conflict / rollback). The event log at
// the bottom shows each step happening.

import { FormEvent, useEffect, useState } from "react";
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
  usePalimpsestSubscription,
} from "@palimpsest/client/react";
import { useSession } from "./session";
import {
  IssueRow,
  nextStatus,
  PRIORITIES,
  PROJECT_BY_ID,
  Status,
  STATUS_LABEL,
  todayEpochDay,
} from "./issues";

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
    id: "urgent",
    label: "Urgent & high, open",
    sql: `SELECT id, title, status, priority, assignee
FROM issues
WHERE priority >= 3 AND status IN ('todo', 'in_progress', 'in_review')
ORDER BY priority DESC, id DESC
LIMIT 15`,
  },
  {
    id: "counts",
    label: "Counts by stage (CTE)",
    sql: `WITH stages AS (
  SELECT status, COUNT(*) AS n
  FROM issues
  GROUP BY status
)
SELECT status, n
FROM stages
ORDER BY n DESC`,
  },
  {
    id: "workload",
    label: "Workload by assignee",
    sql: `SELECT assignee, COUNT(*) AS open_issues, SUM(estimate) AS points
FROM issues
WHERE status IN ('todo', 'in_progress', 'in_review')
GROUP BY assignee
ORDER BY points DESC`,
  },
];

const MY_QUEUE_SQL = `SELECT id, title, status, priority, project, estimate
FROM issues
WHERE status IN ('todo', 'in_progress', 'in_review')
ORDER BY id DESC
LIMIT 20`;

// ---------------------------------------------------------------------------
// Page

export default function LocalFirstPage() {
  const { api, client, currentUser } = useSession();

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
      if (req.table !== "issues") {
        throw new Error(`no write path for table \`${req.table}\``);
      }
      if (req.kind === "insert") {
        await api.createIssue({
          id: Number(req.values.id),
          title: String(req.values.title ?? ""),
          status: String(req.values.status ?? "todo"),
          priority: Number(req.values.priority ?? 2),
          assignee: String(req.values.assignee ?? ""),
          project: String(req.values.project ?? "sync-engine"),
          estimate: Number(req.values.estimate ?? 3),
        });
      } else if (req.kind === "update") {
        const body: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(req.set)) {
          if (key === "status") body.status = String(value);
          else if (key === "priority") body.priority = Number(value);
          else if (key === "assignee") body.assignee = String(value);
          else if (key === "title") body.title = String(value);
          // completed_day / cycle_days are derived server-side on the
          // status transition; skip forwarding them.
        }
        if (Object.keys(body).length === 0) {
          throw new Error("no forwardable columns in update");
        }
        await api.updateIssue(Number(req.key.id), body);
      } else {
        await api.deleteIssue(Number(req.key.id));
      }
    };

    (async () => {
      const pg = await getLocalPostgres();
      const handle = await client.localReplica({
        database: postgresWasmDriver(pg),
        mirrors: ["issues"],
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

  const [issuesState, setIssuesState] = useState<TableSyncStatus | null>(null);
  const [pending, setPending] = useState(0);
  const [events, setEvents] = useState<
    { at: string; nonce: number; event: ReplicaEvent }[]
  >([]);

  useEffect(() => {
    if (!replica) {
      setIssuesState(null);
      setPending(0);
      setEvents([]);
      return;
    }
    let nonce = 0;
    setIssuesState(replica.tableStates()["issues"] ?? null);
    const unsubscribe = subscribeReplicaEvents(replica, (event) => {
      if (event.kind === "tableState" && event.table === "issues") {
        setIssuesState(event.state);
      }
      nonce += 1;
      const entry = {
        at: new Date().toLocaleTimeString(undefined, { hour12: false }),
        nonce,
        event,
      };
      setEvents((prev) => [entry, ...prev].slice(0, 12));
      void replica.pendingMutations("issues").then(setPending);
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
  >(client, sql, { decoder: { coerceSafeIntegersToNumber: true } });

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
  // Panel 2 — optimistic mutations over the mirrored queue

  const queueQuery = useLocalQuery(replica, MY_QUEUE_SQL, {
    tables: ["issues"],
  });
  const queue: IssueRow[] = queueQuery.rows.map((row) => ({
    id: row[0] as bigint | number,
    title: String(row[1] ?? ""),
    status: String(row[2] ?? "todo") as Status,
    priority: row[3] as bigint | number,
    project: String(row[4] ?? ""),
    estimate: row[5] as bigint | number,
    assignee: "",
  }));

  const [draftTitle, setDraftTitle] = useState("");
  const [mutateError, setMutateError] = useState<string | null>(null);

  async function mutate(mutation: Parameters<LocalReplicaHandle["mutate"]>[0]) {
    if (!replica) return;
    setMutateError(null);
    try {
      await replica.mutate(mutation);
      void replica.pendingMutations("issues").then(setPending);
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
      table: "issues",
      insert: {
        id: Date.now(),
        title,
        status: "todo",
        priority: 2,
        assignee: currentUser?.id ?? "",
        project: "clients",
        estimate: 3,
        created_day: todayEpochDay(),
        completed_day: 0,
        cycle_days: 0,
      },
    });
  }

  const live = issuesState?.kind === "live";

  return (
    <>
      <h1>Local-first replica</h1>
      <p className="tagline">
        A Postgres-in-WASM engine (pglite) runs in this tab. Palimpsest
        streams the <em>permissioned subset</em> of <code>issues</code> into
        it and reconciles optimistic writes against the WAL — reads are
        local, writes are instant, the server stays authoritative.
      </p>

      <section className="panel">
        <header className="panel-head">
          <h2>Mirror state</h2>
        </header>
        <p className="rule-text">
          Switching personas in the top bar re-subscribes the mirror under
          the new user&apos;s permission rules — watch the local snapshot get
          replaced with a different subset. Rule changes from the board
          page&apos;s permissions playground resync this mirror live.
        </p>
        <p className="lf-status-row">
          <StatusChip
            label="issues mirror"
            tone={live ? "ok" : issuesState?.kind === "errored" ? "err" : "warn"}
            value={
              issuesState
                ? issuesState.kind === "live"
                  ? `live @ lsn ${issuesState.lsn.toString()}`
                  : issuesState.kind === "errored"
                    ? `errored — ${issuesState.message}`
                    : issuesState.kind
                : replica
                  ? "starting"
                  : "starting local engine"
            }
          />
          <StatusChip
            label="pending optimistic writes"
            tone={pending > 0 ? "warn" : "ok"}
            value={String(pending)}
          />
        </p>
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
          <h2>Optimistic writes — the 20 newest open issues</h2>
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
            placeholder="New issue title (optimistic insert)…"
            value={draftTitle}
            onChange={(e) => setDraftTitle(e.target.value)}
          />
          <button type="submit" disabled={!live || draftTitle.trim() === ""}>
            Create locally
          </button>
        </form>
        {mutateError && <div className="error-banner">{mutateError}</div>}
        {queueQuery.status === "error" && (
          <div className="error-banner">{queueQuery.error}</div>
        )}

        {queue.length === 0 && live && (
          <p className="empty">No open issues visible for this persona.</p>
        )}
        <ul className="posts">
          {queue.map((issue) => {
            const forward = nextStatus(issue.status);
            return (
              <li key={String(issue.id)}>
                <span className="post-id">PAL-{String(issue.id)}</span>
                <span className="post-title">{issue.title}</span>
                <span className="badge lf-stage" data-status={issue.status}>
                  {STATUS_LABEL[issue.status]}
                </span>
                <span className="post-id">
                  {PRIORITIES[Number(issue.priority)]?.short ?? "?"} ·{" "}
                  {PROJECT_BY_ID.get(issue.project)?.label ?? issue.project}
                </span>
                <button
                  className="row-action"
                  disabled={!forward}
                  onClick={() =>
                    forward &&
                    mutate({
                      table: "issues",
                      key: { id: issue.id },
                      set: { status: forward },
                    })
                  }
                >
                  {forward ? `→ ${STATUS_LABEL[forward]}` : "—"}
                </button>
                <button
                  className="row-action"
                  onClick={() =>
                    mutate({
                      table: "issues",
                      key: { id: issue.id },
                      delete: true,
                    })
                  }
                >
                  Delete
                </button>
              </li>
            );
          })}
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
        Local engine: pglite (Postgres compiled to WASM) · mirror transport:
        the same WebSocket/gRPC subscription stream as the board · write
        path: the tracker&apos;s HTTP API.
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
  if (typeof cell === "number" && !Number.isInteger(cell)) {
    return cell.toFixed(1);
  }
  return String(cell);
}
