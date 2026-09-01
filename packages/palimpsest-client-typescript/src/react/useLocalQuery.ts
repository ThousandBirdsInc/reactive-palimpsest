// React hook for querying a local-first replica.
//
// Runs `sql` against the replica's local Postgres-compatible engine and
// re-runs it whenever the replica applies a batch (a remote commit, a
// snapshot, an optimistic mutation settling, rolling back, or losing a
// conflict). Because the local database already holds the permissioned
// subset with optimistic writes on top, results are instant and stay
// live without any per-query server round-trips.

import { useEffect, useMemo, useRef, useState } from "react";
import type {
  LocalReplicaHandle,
  ReplicaEvent,
  SqlDriverResult,
} from "../local.js";

export type LocalQueryStatus = "loading" | "ready" | "error";

export interface UseLocalQueryOptions {
  /** Bind parameters for `$1..$n` placeholders. */
  params?: unknown[];
  /**
   * Only re-run when one of these tables changes. Defaults to re-running
   * on any applied batch (safe, since queries are local and cheap).
   */
  tables?: string[];
}

export interface UseLocalQueryResult {
  status: LocalQueryStatus;
  /** Rows exactly as the local driver returned them. */
  rows: unknown[][];
  error: string | null;
}

/**
 * Live local query against a {@link LocalReplicaHandle}.
 *
 * Note: the replica delivers events to a single consumer. This hook
 * registers that consumer on first use per replica instance and fans
 * out to every mounted hook, so it composes with itself but not with a
 * separate manual `onEvent` registration on the same replica.
 */
export function useLocalQuery(
  replica: LocalReplicaHandle | null,
  sql: string,
  options: UseLocalQueryOptions = {},
): UseLocalQueryResult {
  const [status, setStatus] = useState<LocalQueryStatus>("loading");
  const [rows, setRows] = useState<unknown[][]>([]);
  const [error, setError] = useState<string | null>(null);
  const paramsKey = JSON.stringify(options.params ?? []);
  const tablesKey = JSON.stringify(options.tables ?? null);
  const params = useMemo<unknown[]>(
    () => (options.params ? [...options.params] : []),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [paramsKey],
  );
  const generation = useRef(0);

  useEffect(() => {
    if (!replica) return;
    const tables = options.tables ? new Set(options.tables) : null;
    let disposed = false;

    const run = async () => {
      const ticket = ++generation.current;
      try {
        const result: SqlDriverResult = await replica.query(sql, params);
        if (disposed || ticket !== generation.current) return;
        setRows(result.rows);
        setStatus("ready");
        setError(null);
      } catch (err) {
        if (disposed || ticket !== generation.current) return;
        setStatus("error");
        setError(err instanceof Error ? err.message : String(err));
      }
    };

    void run();
    const unsubscribe = subscribeReplicaEvents(replica, (event) => {
      if (disposed) return;
      const affectsUs =
        !("table" in event) || tables === null || tables.has(event.table);
      const dataChanged =
        event.kind === "applied" ||
        event.kind === "mutationFailed" ||
        event.kind === "mutationConflicted";
      if (dataChanged && affectsUs) void run();
    });

    return () => {
      disposed = true;
      unsubscribe();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [replica, sql, paramsKey, tablesKey]);

  return { status, rows, error };
}

// The wasm replica delivers events to a single `onEvent` consumer; fan
// them out so many hooks can share one replica.
type Listener = (event: ReplicaEvent) => void;
const listeners = new WeakMap<LocalReplicaHandle, Set<Listener>>();

function subscribeReplicaEvents(
  replica: LocalReplicaHandle,
  listener: Listener,
): () => void {
  let set = listeners.get(replica);
  if (!set) {
    set = new Set();
    listeners.set(replica, set);
    replica.onEvent((event) => {
      const current = listeners.get(replica);
      if (!current) return;
      for (const fn of current) fn(event);
    });
  }
  set.add(listener);
  return () => {
    set.delete(listener);
  };
}
