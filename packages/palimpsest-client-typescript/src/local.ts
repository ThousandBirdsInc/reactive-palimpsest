// Local-first replica: typed wrapper over the wasm `localReplica`
// surface.
//
// The app supplies a SQL driver over a Postgres-compatible WASM engine
// (a pgrust/pglite-style build) and, optionally, a writer callback for
// optimistic mutations. The replica streams the server's *permissioned*
// subset of each mirrored query into the local engine, so the same SQL
// that runs against the server runs — with local latency and offline —
// against the mirror.

import type {
  RawLocalMutation,
  RawReplicaEvent,
  RawTableSyncStatus,
  RawWriteRequest,
  WasmLocalReplica,
} from "./wasm.js";

/** Result shape a {@link SqlDriver} must produce. */
export interface SqlDriverResult {
  /** Result rows in **array** mode (one array of cells per row). */
  rows: unknown[][];
}

/**
 * Minimal driver contract over the local Postgres-compatible engine.
 *
 * Statements must execute sequentially on one logical session —
 * `BEGIN`/`COMMIT` framing arrives as plain statements. `params` bind
 * to `$1..$n` placeholders.
 */
export interface SqlDriver {
  exec(sql: string, params: unknown[]): Promise<SqlDriverResult>;
}

/**
 * Adapt a pgrust/pglite-style instance (`query(sql, params, options)`
 * returning `{rows}`) to the {@link SqlDriver} contract, forcing
 * array-mode rows.
 */
export function postgresWasmDriver(pg: {
  query(
    sql: string,
    params?: unknown[],
    options?: { rowMode?: string },
  ): Promise<{ rows: unknown[][] }>;
}): SqlDriver {
  return {
    exec: (sql, params) => pg.query(sql, params, { rowMode: "array" }),
  };
}

/** One mirror: a table, a raw SQL query, or a named prepared query. */
export type MirrorSpec =
  | string
  | { table: string }
  | { sql: string; as: string }
  | {
      name: string;
      params?: Record<
        string,
        string | number | boolean | null | Array<string | number | boolean | null>
      >;
    };

/**
 * Optimistic mutation payload handed to {@link LocalReplicaOptions.writer}.
 * Mirrors `write_request_to_js` in `crates/palimpsest-client-js/src/local.rs`.
 */
export type WriteRequest = RawWriteRequest;

/** Mutation accepted by {@link LocalReplicaHandle.mutate}. */
export type LocalMutation = RawLocalMutation;

/** Per-table sync status. */
export type TableSyncStatus = RawTableSyncStatus;

/** Replica notification events. */
export type ReplicaEvent = RawReplicaEvent;

export interface LocalReplicaOptions {
  /** Driver over the local Postgres-compatible WASM engine. */
  database: SqlDriver;
  /** Queries/tables to mirror. */
  mirrors: MirrorSpec[];
  /**
   * The app's write path: perform the mutation against the real
   * backend (HTTP API, RPC, direct SQL). Reject/throw to roll the
   * optimistic local change back. Omit for a read-only replica.
   */
  writer?: (request: WriteRequest) => Promise<void> | void;
}

/**
 * A running local-first replica. Obtain via
 * `PalimpsestClient.localReplica(...)`.
 */
export class LocalReplicaHandle {
  constructor(private readonly wasmReplica: WasmLocalReplica) {}

  /**
   * Run a read-only SQL query against the local mirror (same
   * Postgres-dialect SQL the server accepts). Results come back
   * exactly as the driver produced them.
   */
  query(sql: string, params: unknown[] = []): Promise<SqlDriverResult> {
    return this.wasmReplica.query(sql, params) as Promise<SqlDriverResult>;
  }

  /**
   * Apply an optimistic mutation: visible locally immediately,
   * forwarded to the writer, reconciled when the authoritative change
   * streams back through the WAL. Resolves to a token identifying the
   * mutation in {@link onEvent} notifications.
   */
  mutate(mutation: LocalMutation): Promise<number> {
    return this.wasmReplica.mutate(mutation);
  }

  /**
   * Register a callback for replica events. Only the first
   * registration receives events; typical UIs re-run local queries on
   * `applied` and surface `mutationConflicted`/`mutationFailed`.
   */
  onEvent(callback: (event: ReplicaEvent) => void): void {
    this.wasmReplica.onEvent(callback);
  }

  /** Current sync state of every mirrored table. */
  tableStates(): Record<string, TableSyncStatus> {
    return this.wasmReplica.tableStates();
  }

  /** Optimistic mutations not yet confirmed for `table`. */
  pendingMutations(table: string): Promise<number> {
    return this.wasmReplica.pendingMutations(table);
  }

  /** Stop the mirrors; the underlying client connection stays open. */
  stop(): Promise<void> {
    return this.wasmReplica.stop();
  }
}
