// Shared issue-tracker domain: statuses, priorities, projects, and
// row shapes. The server-side twin of these lists lives in
// `server/src/db.rs` (`STATUSES` / `PROJECTS`); keep them in sync.

export type Status =
  | "backlog"
  | "todo"
  | "in_progress"
  | "in_review"
  | "done"
  | "cancelled";

/** Workflow states in board order (cancelled never gets a column). */
export const BOARD_STATUSES: Status[] = [
  "backlog",
  "todo",
  "in_progress",
  "in_review",
  "done",
];

export const STATUS_LABEL: Record<Status, string> = {
  backlog: "Backlog",
  todo: "Todo",
  in_progress: "In progress",
  in_review: "In review",
  done: "Done",
  cancelled: "Cancelled",
};

/**
 * Workflow stages are an *ordered* scale, so they wear one hue
 * stepped light→dark (an ordinal ramp — validated, lightest step
 * clears the surface) rather than a categorical rainbow. Cancelled
 * is an off-ramp state and stays neutral gray.
 */
export const STATUS_COLOR: Record<Status, string> = {
  backlog: "#86b6ef",
  todo: "#5598e7",
  in_progress: "#2a78d6",
  in_review: "#1c5cab",
  done: "#104281",
  cancelled: "#898781",
};

export function nextStatus(status: Status): Status | null {
  const idx = BOARD_STATUSES.indexOf(status);
  if (idx < 0 || idx === BOARD_STATUSES.length - 1) return null;
  return BOARD_STATUSES[idx + 1];
}

export function prevStatus(status: Status): Status | null {
  const idx = BOARD_STATUSES.indexOf(status);
  if (idx <= 0) return null;
  return BOARD_STATUSES[idx - 1];
}

/** Priorities, index = stored value (0 none … 4 urgent). */
export const PRIORITIES = [
  { value: 0, label: "No priority", short: "—" },
  { value: 1, label: "Low", short: "Low" },
  { value: 2, label: "Medium", short: "Med" },
  { value: 3, label: "High", short: "High" },
  { value: 4, label: "Urgent", short: "Urg" },
] as const;

/**
 * Projects with their identity hues — fixed categorical slot order
 * (validated adjacent-safe); the dot always sits beside the project
 * name, so identity is never color-alone.
 */
export const PROJECTS = [
  { id: "sync-engine", label: "Sync Engine", color: "#2a78d6" },
  { id: "dataflow", label: "Dataflow", color: "#eb6834" },
  { id: "clients", label: "Clients", color: "#1baf7a" },
  { id: "infra", label: "Infra", color: "#eda100" },
  { id: "security", label: "Security", color: "#e87ba4" },
] as const;

export const PROJECT_BY_ID = new Map(PROJECTS.map((p) => [p.id as string, p]));

/** Row shape of `SELECT * FROM issues` (and the board subscription). */
export interface IssueRow {
  id: bigint | number;
  title: string;
  status: Status;
  priority: bigint | number;
  assignee: string;
  project: string;
  estimate: bigint | number;
  created_day?: bigint | number;
  completed_day?: bigint | number;
  cycle_days?: bigint | number;
}

/** Days since the Unix epoch, from the browser clock (UTC). */
export function todayEpochDay(): number {
  return Math.floor(Date.now() / 86_400_000);
}

/** Short label ("Mar 4") for an epoch-day. */
export function epochDayLabel(day: number): string {
  return new Date(day * 86_400_000).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}
