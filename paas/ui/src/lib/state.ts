export type Tone = "good" | "warn" | "bad" | "neutral";

const GOOD = new Set([
  "ready",
  "healthy",
  "active",
  "ok",
  "succeeded",
  "available",
  "live",
  "connected",
]);
const WARN = new Set([
  "requested",
  "running",
  "degraded",
  "reconciling",
  "initializing_postgres",
  "configuring_replication",
  "resizing",
  "updating_postgres",
  "restoring",
  "draining",
  "maintenance",
  "pending",
  "reconnecting",
  "polling",
  "at_risk",
]);
const BAD = new Set([
  "failed",
  "unhealthy",
  "firing",
  "deleting",
  "offline",
  "error",
  "closed",
  "expired",
  "unavailable",
]);

export function stateTone(state: string | null | undefined): Tone {
  if (!state) return "neutral";
  const s = state.toLowerCase();
  if (GOOD.has(s)) return "good";
  if (WARN.has(s)) return "warn";
  if (BAD.has(s)) return "bad";
  return "neutral";
}

export function statusLabel(state: string | null | undefined): string {
  if (!state) return "unknown";
  return state.replace(/_/g, " ");
}
