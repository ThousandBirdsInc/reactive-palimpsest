// React hook for a single typed subscription.
//
// Lifecycle:
//   - On mount (or when sql/vars/refreshKey change): open a fresh
//     subscription; tear the previous one down.
//   - For each event:
//       * "accepted" → stash schema, clear local cache, status = "open".
//       * "diff"     → apply rows by primary key into the local cache.
//       * "resync"   → clear the cache, wait for the next "accepted".
//       * "error"    → surface code+message; status = "error".
//   - On unmount: call .unsubscribe() and stop reacting to events.
//
// The `refreshKey` dependency is a thin escape hatch: bump it from
// caller-land to force a fresh subscribe after a write. Once the wire
// protocol streams diffs end-to-end this becomes unnecessary, but it
// lets demo apps work today.

import { useEffect, useMemo, useState } from "react";
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
  /** Bump to force a fresh subscribe (e.g. after a write). */
  refreshKey?: number | string;
  /** Called when a `resync` lands; defaults to clearing local rows. */
  onResync?: (event: Extract<DiffEvent<T>, { kind: "resync" }>) => void;
  /** Per-row decoder override (overrides the client-level decoder). */
  decoder?: RowDecoderOptions;
}

export interface UseSubscriptionResult<T> {
  status: SubscriptionStatus;
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

  // Stable string key for vars — re-subscribe when it changes.
  const varsKey = useMemo(
    () => JSON.stringify(options.vars ?? {}),
    [options.vars],
  );

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
          switch (event.kind) {
            case "accepted":
              currentSchema = event.schema;
              setSchema(event.schema);
              setLsn(event.snapshotLsn);
              rowsByPk.clear();
              setRows([]);
              setStatus("open");
              break;
            case "diff": {
              setLsn(event.lsn);
              if (!currentSchema) break;
              for (const row of event.rows) {
                const pk = pkOf(row, currentSchema);
                if (event.op === "delete") rowsByPk.delete(pk);
                else rowsByPk.set(pk, row);
              }
              setRows([...rowsByPk.values()]);
              break;
            }
            case "resync":
              rowsByPk.clear();
              setRows([]);
              if (options.onResync) options.onResync(event);
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
  }, [client, sql, varsKey, options.refreshKey]);

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
