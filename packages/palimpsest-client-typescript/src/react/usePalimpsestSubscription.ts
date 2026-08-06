// React hook for a single typed subscription.
//
// Lifecycle:
//   - On mount (or when sql/vars/refreshKey change): open a fresh
//     subscription; tear the previous one down.
//   - For each event:
//       * "accepted" → stash schema, clear local cache, status = "open".
//       * "diff"     → apply rows by primary key into the local cache.
//       * "transaction" → apply all row changes, then publish once.
//       * "resync"   → clear the cache, wait for the next "accepted".
//       * "error"    → surface code+message; status = "error".
//   - On unmount: call .unsubscribe() and stop reacting to events.
//
// Live diffs stream end-to-end (server WAL → dataflow → Diff /
// TransactionUpdate), so `refreshKey` is only an escape hatch for
// callers that want to force a full re-subscribe for their own
// reasons — it is never required after a write.
//
// Callers that maintain their own cache (e.g. TanStack Query) can pass
// `onDiff` to observe every event as it arrives and fold it into that
// cache directly, and `trackRows: false` to switch off this hook's
// internal row map so rows aren't held twice.

import { useEffect, useMemo, useRef, useState } from "react";
import type { PalimpsestClient, TypedSubscription } from "../client.js";
import type { RowDecoderOptions } from "../codec.js";
import type {
  DiffEvent,
  ErrorEvent,
  Schema,
  SubscribeOptions,
} from "../types.js";

export type SubscriptionStatus =
  | "idle"
  | "connecting"
  | "open"
  | "closed"
  | "error";

export interface UseSubscriptionOptions<T> extends SubscribeOptions {
  /**
   * Bump to force a fresh subscribe. Live diffs stream automatically,
   * so this is an escape hatch, not a requirement after writes.
   */
  refreshKey?: number | string;
  /** Called when a `resync` lands; defaults to clearing local rows. */
  onResync?: (event: Extract<DiffEvent<T>, { kind: "resync" }>) => void;
  /**
   * Called for **every** event (`accepted`, `diff`, `transaction`,
   * `resync`, `error`) before the hook applies it to its own row map.
   * Use this to fold diffs into a host cache (e.g. TanStack Query's
   * `setQueryData`) instead of copying `rows`. The latest callback is
   * always invoked — changing its identity does not re-subscribe.
   */
  onDiff?: (event: DiffEvent<T>) => void;
  /**
   * Maintain the hook's internal primary-key row map and publish
   * `rows` (default `true`). Pass `false` when a host cache driven by
   * `onDiff` is the single source of truth — the hook then keeps no
   * second copy of the rows and `rows` stays `[]`.
   */
  trackRows?: boolean;
  /** Per-row decoder override (overrides the client-level decoder). */
  decoder?: RowDecoderOptions;
}

export interface UseSubscriptionResult<T> {
  status: SubscriptionStatus;
  /** Rolling result set; always `[]` when `trackRows: false`. */
  rows: T[];
  schema: Schema | null;
  error: Pick<ErrorEvent, "code" | "message"> | null;
  /** Last server LSN seen (snapshot or diff). */
  lsn: bigint | null;
}

/**
 * Subscribe to `sql` against `client`. Returns the rolling result-set
 * as a typed `T[]`, keyed by the schema's primary-key columns.
 *
 * The hook owns one wasm `Subscription` per (sql, vars, refreshKey)
 * combo; changing any of those tears the old one down and opens a
 * fresh one.
 */
export function usePalimpsestSubscription<T>(
  client: PalimpsestClient | null,
  sql: string,
  options: UseSubscriptionOptions<T> = {},
): UseSubscriptionResult<T> {
  const [status, setStatus] = useState<SubscriptionStatus>("idle");
  const [rows, setRows] = useState<T[]>([]);
  const [schema, setSchema] = useState<Schema | null>(null);
  const [error, setError] = useState<UseSubscriptionResult<T>["error"]>(null);
  const [lsn, setLsn] = useState<bigint | null>(null);
  // Bumped from inside the event callback on every `resync` so the
  // effect tears the current subscription down and opens a fresh one.
  // The server emits Resync when a per-subscription channel saturates
  // (DropAndResync policy) expecting the client to refetch; without
  // this the subscription would appear "stuck" after a write burst.
  const [resyncEpoch, setResyncEpoch] = useState(0);

  // Stable string key for vars — re-subscribe when it changes.
  const varsKey = useMemo(
    () => JSON.stringify(options.vars ?? {}),
    [options.vars],
  );

  // Latest-callback refs so changing handler identity never tears the
  // subscription down; the event callback always sees the current one.
  const onDiffRef = useRef(options.onDiff);
  onDiffRef.current = options.onDiff;
  const onResyncRef = useRef(options.onResync);
  onResyncRef.current = options.onResync;
  const trackRows = options.trackRows !== false;

  // eslint-disable-next-line react-hooks/exhaustive-deps -- explicit deps below
  useEffect(() => {
    if (!client) {
      setStatus("idle");
      return;
    }

    let alive = true;
    setStatus("connecting");
    setRows([]);
    setError(null);

    // Per-subscription state kept in closure so successive events can
    // build on each other without going through React state (which is
    // async).
    let currentSchema: Schema | null = null;
    const rowsByPk = new Map<string, T>();

    let openedSub: TypedSubscription<T> | null = null;
    (async () => {
      try {
        const sub = await client.subscribe<T>(sql, {
          vars: options.vars,
          decoder: options.decoder,
        });
        if (!alive) {
          await sub.unsubscribe();
          return;
        }
        openedSub = sub;

        sub.onEvent((event) => {
          if (!alive) return;
          // Host-cache hook point: the caller sees every event before
          // the hook's own bookkeeping touches it.
          onDiffRef.current?.(event);
          switch (event.kind) {
            case "accepted":
              currentSchema = event.schema;
              setSchema(event.schema);
              setLsn(event.snapshotLsn);
              rowsByPk.clear();
              if (trackRows) setRows([]);
              setStatus("open");
              break;
            case "diff": {
              setLsn(event.lsn);
              if (!currentSchema || !trackRows) break;
              for (const row of event.rows) {
                const pk = pkOf(row, currentSchema);
                if (event.op === "delete") rowsByPk.delete(pk);
                else rowsByPk.set(pk, row);
              }
              setRows([...rowsByPk.values()]);
              break;
            }
            case "transaction": {
              setLsn(event.commitLsn);
              if (!currentSchema || !trackRows) break;
              for (const change of event.changes) {
                if (change.op === "delete") {
                  if (change.old) rowsByPk.delete(pkOf(change.old, currentSchema));
                } else if (change.op === "update") {
                  if (change.old) rowsByPk.delete(pkOf(change.old, currentSchema));
                  if (change.new) rowsByPk.set(pkOf(change.new, currentSchema), change.new);
                } else if (change.new) {
                  rowsByPk.set(pkOf(change.new, currentSchema), change.new);
                }
              }
              setRows([...rowsByPk.values()]);
              break;
            }
            case "resync":
              rowsByPk.clear();
              if (trackRows) setRows([]);
              onResyncRef.current?.(event);
              // Force a fresh subscribe so the client refetches —
              // matches the server's DropAndResync contract.
              setResyncEpoch((n) => n + 1);
              break;
            case "error":
              setError({ code: event.code, message: event.message });
              setStatus("error");
              break;
          }
        });
      } catch (e) {
        if (!alive) return;
        setError({
          code: "client",
          message: e instanceof Error ? e.message : String(e),
        });
        setStatus("error");
      }
    })();

    return () => {
      alive = false;
      const sub = openedSub;
      openedSub = null;
      if (sub) {
        sub.unsubscribe().catch(() => {
          /* already torn down */
        });
      }
      setStatus("closed");
    };
  }, [client, sql, varsKey, options.refreshKey, resyncEpoch, trackRows]);

  return { status, rows, schema, error, lsn };
}

/**
 * Compose the primary-key string from a typed row using the schema's
 * `primaryKeyColumns`. Falls back to the first own property when the
 * schema has no PK declared.
 */
function pkOf<T>(row: T, schema: Schema): string {
  if (typeof row !== "object" || row === null) return String(row);
  const obj = row as Record<string, unknown>;
  if (schema.primaryKeyColumns.length === 0) {
    const keys = Object.keys(obj);
    if (keys.length === 0) return "";
    const v = obj[keys[0]!];
    return typeof v === "bigint" ? v.toString() : String(v);
  }
  return schema.primaryKeyColumns
    .map((i) => {
      const colName = schema.columns[i]?.name;
      const v = colName ? obj[colName] : undefined;
      return typeof v === "bigint" ? v.toString() : String(v);
    })
    .join("|");
}
