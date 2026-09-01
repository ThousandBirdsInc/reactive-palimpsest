// Linear-style kanban board over two live subscriptions:
//
// * open issues — every non-terminal state, one subscription; the
//   client groups rows into columns, and each WAL commit lands as a
//   row-level diff that moves/updates exactly the cards it touched.
// * recently done — an `ORDER BY completed_day DESC LIMIT n` TopK the
//   dataflow maintains incrementally, so the Done column always shows
//   the freshest completions without shipping the whole history.
//
// Every card action (advance, reprioritize, assign, delete) goes
// through the HTTP write API → Postgres → WAL → dataflow → diff, the
// same path the activity simulator uses.

import { FormEvent, useMemo, useState } from "react";
import { usePalimpsestSubscription } from "@palimpsest/client/react";
import PermissionsPlayground from "./PermissionsPlayground";
import { useSession } from "./session";
import {
  BOARD_STATUSES,
  IssueRow,
  nextStatus,
  prevStatus,
  PRIORITIES,
  PROJECT_BY_ID,
  PROJECTS,
  Status,
  STATUS_LABEL,
} from "./issues";

const OPEN_SQL = `SELECT id, title, status, priority, assignee, project, estimate
FROM issues
WHERE status IN ('backlog', 'todo', 'in_progress', 'in_review')`;

const DONE_SQL = `SELECT id, title, status, priority, assignee, project, estimate, completed_day
FROM issues
WHERE status = 'done'
ORDER BY completed_day DESC, id DESC
LIMIT 12`;

/** Rows shown per open column before the "+N more" footer. */
const COLUMN_CAP = 30;

export default function BoardPage() {
  const { api, client, users, currentUser } = useSession();
  const [writeError, setWriteError] = useState<string | null>(null);
  const [projectFilter, setProjectFilter] = useState<string | null>(null);
  const [onlyMine, setOnlyMine] = useState(false);

  const { rows: openRows, error: openError } =
    usePalimpsestSubscription<IssueRow>(client, OPEN_SQL, {
      decoder: { coerceSafeIntegersToNumber: true },
    });
  const { rows: doneRows, error: doneError } =
    usePalimpsestSubscription<IssueRow>(client, DONE_SQL, {
      decoder: { coerceSafeIntegersToNumber: true },
    });

  const filtered = useMemo(() => {
    const match = (row: IssueRow) =>
      (!projectFilter || row.project === projectFilter) &&
      (!onlyMine || row.assignee === currentUser?.id);
    return {
      open: openRows.filter(match),
      done: doneRows.filter(match),
    };
  }, [openRows, doneRows, projectFilter, onlyMine, currentUser]);

  const columns = useMemo(() => {
    const byStatus = new Map<Status, IssueRow[]>(
      BOARD_STATUSES.map((s) => [s, []]),
    );
    for (const row of filtered.open) {
      byStatus.get(row.status)?.push(row);
    }
    for (const rows of byStatus.values()) {
      // Urgent first, then newest — a Linear-ish default ordering.
      rows.sort(
        (a, b) => Number(b.priority) - Number(a.priority) || Number(b.id) - Number(a.id),
      );
    }
    byStatus.set("done", filtered.done);
    return byStatus;
  }, [filtered]);

  async function withWrite<T>(fn: () => Promise<T>): Promise<void> {
    setWriteError(null);
    try {
      await fn();
      // The confirming update streams back through the WAL diff.
    } catch (e) {
      setWriteError(e instanceof Error ? e.message : String(e));
    }
  }

  const subscriptionError = openError ?? doneError;

  return (
    <>
      <h1>Palimpsest Tracker</h1>
      <p className="tagline">
        A Linear-style issue tracker where every view is a live SQL
        subscription: the columns below are row-level diffs from the WAL,
        the Done column is an incrementally-maintained TopK, and the
        permission rules decide which issues each persona's subscriptions
        may contain.
      </p>

      <Composer onError={setWriteError} />

      <div className="board-filters">
        <div className="filters" role="group" aria-label="Filter by project">
          <button
            type="button"
            className={`chip ${projectFilter === null ? "chip-active" : ""}`}
            onClick={() => setProjectFilter(null)}
          >
            All projects
          </button>
          {PROJECTS.map((p) => (
            <button
              key={p.id}
              type="button"
              className={`chip ${projectFilter === p.id ? "chip-active" : ""}`}
              onClick={() =>
                setProjectFilter(projectFilter === p.id ? null : p.id)
              }
            >
              <span className="project-dot" style={{ background: p.color }} />
              {p.label}
            </button>
          ))}
        </div>
        <label className="auto-toggle">
          <input
            type="checkbox"
            checked={onlyMine}
            onChange={(e) => setOnlyMine(e.target.checked)}
          />
          only mine
        </label>
      </div>

      {writeError && <div className="error-banner">{writeError}</div>}
      {subscriptionError && (
        <div className="error-banner">
          {subscriptionError.code}: {subscriptionError.message}
        </div>
      )}

      <div className="board" role="list" aria-label="Issue board">
        {BOARD_STATUSES.map((status) => {
          const rows = columns.get(status) ?? [];
          const capped = status === "done" ? rows : rows.slice(0, COLUMN_CAP);
          return (
            <section className="board-col" key={status} role="listitem">
              <header className="board-col-head">
                <span
                  className="status-dot"
                  data-status={status}
                  aria-hidden="true"
                />
                <h2>{STATUS_LABEL[status]}</h2>
                <span className="board-col-count">
                  {status === "done" ? `last ${rows.length}` : rows.length}
                </span>
              </header>
              <div className="board-col-cards">
                {capped.map((issue) => (
                  <IssueCard
                    key={String(issue.id)}
                    issue={issue}
                    users={users.map((u) => u.id)}
                    onMove={(to) =>
                      withWrite(() =>
                        api.updateIssue(Number(issue.id), { status: to }),
                      )
                    }
                    onPriority={(priority) =>
                      withWrite(() =>
                        api.updateIssue(Number(issue.id), { priority }),
                      )
                    }
                    onAssign={(assignee) =>
                      withWrite(() =>
                        api.updateIssue(Number(issue.id), { assignee }),
                      )
                    }
                    onDelete={() =>
                      withWrite(() => api.deleteIssue(Number(issue.id)))
                    }
                  />
                ))}
                {rows.length > capped.length && (
                  <div className="board-more">
                    +{rows.length - capped.length} more
                  </div>
                )}
                {rows.length === 0 && <div className="board-empty">—</div>}
              </div>
            </section>
          );
        })}
      </div>

      <details className="board-sql">
        <summary>The SQL behind this board</summary>
        <pre className="sql">{OPEN_SQL}</pre>
        <pre className="sql">{DONE_SQL}</pre>
      </details>

      <PermissionsPlayground />

      <footer>
        Transport: WebSocket (with <code>?token=…</code>) → in-process gRPC
        bridge → Palimpsest SyncEngine. Writes: HTTP API → Postgres → WAL →
        logical replication → dataflow diffs.
      </footer>
    </>
  );
}

/** New-issue composer: title + project + priority. */
function Composer({ onError }: { onError: (e: string | null) => void }) {
  const { api } = useSession();
  const [title, setTitle] = useState("");
  const [project, setProject] = useState<string>(PROJECTS[0].id);
  const [priority, setPriority] = useState(2);

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const trimmed = title.trim();
    if (!trimmed) return;
    setTitle("");
    onError(null);
    try {
      await api.createIssue({ title: trimmed, project, priority });
    } catch (err) {
      onError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <form className="composer" onSubmit={onSubmit}>
      <input
        type="text"
        placeholder="New issue title…"
        value={title}
        onChange={(e) => setTitle(e.target.value)}
      />
      <select
        value={project}
        onChange={(e) => setProject(e.target.value)}
        aria-label="Project"
      >
        {PROJECTS.map((p) => (
          <option key={p.id} value={p.id}>
            {p.label}
          </option>
        ))}
      </select>
      <select
        value={priority}
        onChange={(e) => setPriority(Number(e.target.value))}
        aria-label="Priority"
      >
        {PRIORITIES.map((p) => (
          <option key={p.value} value={p.value}>
            {p.label}
          </option>
        ))}
      </select>
      <button type="submit" disabled={title.trim() === ""}>
        Create
      </button>
    </form>
  );
}

interface IssueCardProps {
  issue: IssueRow;
  users: string[];
  onMove: (to: Status) => void;
  onPriority: (priority: number) => void;
  onAssign: (assignee: string) => void;
  onDelete: () => void;
}

function IssueCard({
  issue,
  users,
  onMove,
  onPriority,
  onAssign,
  onDelete,
}: IssueCardProps) {
  const project = PROJECT_BY_ID.get(issue.project);
  const priority = Number(issue.priority);
  const back = prevStatus(issue.status);
  const forward = nextStatus(issue.status);
  return (
    <article className="issue-card">
      <div className="issue-card-top">
        <PriorityIcon priority={priority} />
        <span className="issue-key">PAL-{String(issue.id)}</span>
        <span className="issue-estimate" title="Estimate (points)">
          {String(issue.estimate)}
        </span>
      </div>
      <div className="issue-title">{issue.title}</div>
      <div className="issue-card-bottom">
        <span className="issue-project">
          <span
            className="project-dot"
            style={{ background: project?.color ?? "#898781" }}
          />
          {project?.label ?? issue.project}
        </span>
        <Avatar assignee={issue.assignee} />
      </div>
      <div className="issue-actions">
        <button
          type="button"
          className="icon-btn"
          disabled={!back}
          title={back ? `Move to ${STATUS_LABEL[back]}` : undefined}
          onClick={() => back && onMove(back)}
        >
          ◀
        </button>
        <button
          type="button"
          className="icon-btn"
          disabled={!forward}
          title={forward ? `Move to ${STATUS_LABEL[forward]}` : undefined}
          onClick={() => forward && onMove(forward)}
        >
          ▶
        </button>
        <select
          className="mini-select"
          value={priority}
          title="Priority"
          onChange={(e) => onPriority(Number(e.target.value))}
        >
          {PRIORITIES.map((p) => (
            <option key={p.value} value={p.value}>
              {p.short}
            </option>
          ))}
        </select>
        <select
          className="mini-select"
          value={issue.assignee}
          title="Assignee"
          onChange={(e) => onAssign(e.target.value)}
        >
          <option value="">unassigned</option>
          {users.map((id) => (
            <option key={id} value={id}>
              {id}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="icon-btn icon-btn-danger"
          title="Delete issue"
          onClick={onDelete}
        >
          ✕
        </button>
      </div>
    </article>
  );
}

/** Linear-style priority glyph: three bars for low/med/high, an
 *  exclamation diamond for urgent, a dash for none. Inline SVG so it
 *  inherits crisp rendering at 14px. */
function PriorityIcon({ priority }: { priority: number }) {
  const label = PRIORITIES[priority]?.label ?? "Unknown";
  if (priority === 0) {
    return (
      <svg className="prio" viewBox="0 0 14 14" role="img" aria-label={label}>
        <title>{label}</title>
        <rect x="2" y="6.25" width="10" height="1.5" rx="0.75" fill="#898781" />
      </svg>
    );
  }
  if (priority === 4) {
    return (
      <svg className="prio" viewBox="0 0 14 14" role="img" aria-label={label}>
        <title>{label}</title>
        <path
          d="M7 1 13 7 7 13 1 7Z"
          fill="#ec835a"
        />
        <rect x="6.3" y="4" width="1.4" height="4" rx="0.7" fill="#fff" />
        <rect x="6.3" y="9" width="1.4" height="1.4" rx="0.7" fill="#fff" />
      </svg>
    );
  }
  const active = "#52514e";
  const idle = "#e1e0d9";
  return (
    <svg className="prio" viewBox="0 0 14 14" role="img" aria-label={label}>
      <title>{label}</title>
      <rect x="1.5" y="8" width="2.6" height="4.5" rx="1" fill={active} />
      <rect
        x="5.7"
        y="5.5"
        width="2.6"
        height="7"
        rx="1"
        fill={priority >= 2 ? active : idle}
      />
      <rect
        x="9.9"
        y="2.5"
        width="2.6"
        height="10"
        rx="1"
        fill={priority >= 3 ? active : idle}
      />
    </svg>
  );
}

/** Assignee chip: initial in a tinted circle, or a dashed outline when
 *  unassigned. The full id rides the tooltip + adjacent selects, so
 *  identity never relies on the tint. */
function Avatar({ assignee }: { assignee: string }) {
  if (!assignee) {
    return <span className="avatar avatar-empty" title="Unassigned" />;
  }
  return (
    <span className="avatar" title={assignee}>
      {assignee[0]?.toUpperCase()}
    </span>
  );
}
