// React hook over palimpsest-client-js (§18.11).
//
// The wasm bundle is loaded once, lazily. The hook owns one
// Subscription whose lifecycle is tied to the component — on unmount
// or on a `sql`/`vars`/`url` change we tear it down and start a fresh
// one. Cache is enabled by default in palimpsest-client, so we just
// listen for events and rebuild the row list whenever a `diff` arrives.

import { useEffect, useRef, useState } from "react";
import init, {
  Client,
  type Subscription,
} from "../pkg/palimpsest_client_js";

type Status = "loading" | "connecting" | "open" | "closed" | "error";

type AcceptedSchema = {
  columns: Array<{
    name: string;
    type: number;
    typeName: string;
    nullable: boolean;
  }>;
  primaryKeyColumns: number[];
};

type DiffEvent =
  | {
      kind: "accepted";
      schemaId: number;
      snapshotLsn: bigint;
      schema: AcceptedSchema;
    }
  | { kind: "diff"; lsn: bigint; op: string; rows: unknown[][] }
  | { kind: "transaction"; commitLsn: bigint;
      changes: Array<{ op: string; old: unknown[] | null; new: unknown[] | null }> }
  | { kind: "resync"; reason: string; message: string }
  | { kind: "error"; code: string; message: string };

export interface PalimpsestSubscriptionResult {
  status: Status;
  rows: unknown[][];
  schema: AcceptedSchema | null;
  error: { code: string; message: string } | null;
}

export interface PalimpsestSubscriptionOptions {
  token?: string | null;
  vars?: Record<string, string>;
}

let initPromise: Promise<unknown> | null = null;
function ensureInit() {
  if (!initPromise) {
    initPromise = init();
  }
  return initPromise;
}

const clientCache = new Map<string, Promise<Client>>();
function getClient(url: string, token?: string | null) {
  const key = `${url}::${token ?? ""}`;
  let c = clientCache.get(key);
  if (!c) {
    c = (async () => {
      await ensureInit();
      return await Client.connect(url, token ?? null);
    })();
    clientCache.set(key, c);
  }
  return c;
}

export function usePalimpsestSubscription(
  url: string,
  sql: string,
  options: PalimpsestSubscriptionOptions = {},
): PalimpsestSubscriptionResult {
  const [status, setStatus] = useState<Status>("loading");
  const [rows, setRows] = useState<unknown[][]>([]);
  const [schema, setSchema] = useState<
    PalimpsestSubscriptionResult["schema"] | null
  >(null);
  const [error, setError] = useState<
    PalimpsestSubscriptionResult["error"]
  >(null);
  const subRef = useRef<Subscription | null>(null);
  const aliveRef = useRef(true);

  // Stable string key for vars — re-subscribe when it changes.
  const varsKey = JSON.stringify(options.vars ?? {});
  const token = options.token ?? null;

  useEffect(() => {
    aliveRef.current = true;
    setStatus("connecting");
    setRows([]);
    setSchema(null);
    setError(null);

    let sub: Subscription | null = null;
    const rowsByPk = new Map<string, unknown[]>();

    (async () => {
      try {
        const client = await getClient(url, token);
        if (!aliveRef.current) return;
        sub = await client.subscribe(sql, options.vars ?? {});
        if (!aliveRef.current) {
          await sub.unsubscribe();
          return;
        }
        subRef.current = sub;
        sub.onDiff((event: DiffEvent) => {
          if (!aliveRef.current) return;
          switch (event.kind) {
            case "accepted":
              setSchema(event.schema as never);
              rowsByPk.clear();
              setRows([]);
              setStatus("open");
              break;
            case "diff": {
              const pkCols =
                schema?.primaryKeyColumns ??
                (event as { schema?: { primaryKeyColumns: number[] } })
                  .schema?.primaryKeyColumns ??
                [0];
              for (const row of event.rows) {
                const pk = pkCols.map((i: number) => row[i]).join("|");
                if (event.op === "DIFF_OP_DELETE") {
                  rowsByPk.delete(pk);
                } else {
                  rowsByPk.set(pk, row);
                }
              }
              setRows([...rowsByPk.values()]);
              break;
            }
            case "transaction": {
              const pkCols = schema?.primaryKeyColumns ?? [0];
              for (const change of event.changes) {
                if (change.op === "DIFF_OP_DELETE") {
                  if (change.old) {
                    rowsByPk.delete(pkCols.map((i) => change.old![i]).join("|"));
                  }
                } else if (change.op === "DIFF_OP_UPDATE") {
                  if (change.old) {
                    rowsByPk.delete(pkCols.map((i) => change.old![i]).join("|"));
                  }
                  if (change.new) {
                    rowsByPk.set(pkCols.map((i) => change.new![i]).join("|"), change.new);
                  }
                } else if (change.new) {
                  rowsByPk.set(pkCols.map((i) => change.new![i]).join("|"), change.new);
                }
              }
              setRows([...rowsByPk.values()]);
              break;
            }
            case "resync":
              rowsByPk.clear();
              setRows([]);
              break;
            case "error":
              setError({ code: event.code, message: event.message });
              setStatus("error");
              break;
          }
        });
      } catch (e) {
        if (!aliveRef.current) return;
        setStatus("error");
        setError({
          code: "client",
          message: e instanceof Error ? e.message : String(e),
        });
      }
    })();

    return () => {
      aliveRef.current = false;
      const s = subRef.current;
      subRef.current = null;
      if (s) {
        s.unsubscribe().catch(() => {});
      }
      setStatus("closed");
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url, sql, varsKey, token]);

  return { status, rows, schema, error };
}
