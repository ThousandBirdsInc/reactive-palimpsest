// Type-only contract that mirrors the shape of `palimpsest-client-js`
// (the `wasm-pack build --target web` output of `crates/palimpsest-client-js`).
//
// We deliberately re-declare the surface here so the TypeScript wrapper
// has zero runtime coupling to a specific wasm bundle path. Callers
// import the wasm module themselves and pass it to `PalimpsestClient`,
// which keeps the package bundler-agnostic.

/**
 * Raw event shape emitted by the wasm `Subscription.onDiff` callback.
 *
 * Matches `event_to_js` in `crates/palimpsest-client-js/src/bindings.rs`.
 */
export type RawDiffEvent =
  | RawAcceptedEvent
  | RawDiffPayloadEvent
  | RawTransactionEvent
  | RawResyncEvent
  | RawErrorEvent;

export interface RawAcceptedEvent {
  kind: "accepted";
  schemaId: number;
  snapshotLsn: bigint;
  schema: RawSchema;
}

export interface RawSchema {
  columns: RawColumn[];
  primaryKeyColumns: number[];
}

export interface RawColumn {
  /** Column name as projected by the canonical query. */
  name: string;
  /** Numeric `DatumType` from `palimpsest-wal`. */
  type: number;
  /** Human-readable name, e.g. `"i64"`, `"text"`, `"bool"`. */
  typeName: string;
  /** Whether the column may carry SQL `NULL`. */
  nullable: boolean;
}

export interface RawDiffPayloadEvent {
  kind: "diff";
  lsn: bigint;
  /** One of `DIFF_OP_INITIAL`, `DIFF_OP_INSERT`, `DIFF_OP_UPDATE`, `DIFF_OP_DELETE`. */
  op: RawDiffOp;
  rows: unknown[][];
}

export interface RawTransactionEvent {
  kind: "transaction";
  commitLsn: bigint;
  beginLsn?: bigint;
  endLsn?: bigint;
  transactionId?: number;
  changes: RawRowChange[];
}

export interface RawRowChange {
  op: RawDiffOp;
  old: unknown[] | null;
  new: unknown[] | null;
}

export type RawDiffOp =
  | "DIFF_OP_INITIAL"
  | "DIFF_OP_INSERT"
  | "DIFF_OP_UPDATE"
  | "DIFF_OP_DELETE";

export interface RawResyncEvent {
  kind: "resync";
  reason: string;
  message: string;
}

export interface RawErrorEvent {
  kind: "error";
  code: string;
  message: string;
}

/**
 * Shape of the wasm module after `wasm-bindgen --target web` codegen.
 *
 * The default export is the `init` function; named exports are the
 * `Client` and `Subscription` constructors.
 */
export interface WasmModule {
  default: WasmInit;
  Client: WasmClientCtor;
}

export type WasmInit = (
  input?:
    | string
    | URL
    | Request
    | BufferSource
    | WebAssembly.Module
    | { module_or_path?: string | URL | Request | BufferSource | WebAssembly.Module },
) => Promise<unknown>;

export interface WasmClientCtor {
  /** `Client.connect(url, token)` — bearer token or null for anonymous. */
  connect(url: string, token: string | null): Promise<WasmClient>;
}

export interface WasmClient {
  subscribe(sql: string, vars: Record<string, string>): Promise<WasmSubscription>;
  /**
   * Start a local-first replica: mirror the server's permissioned
   * subset into a local Postgres-compatible WASM engine. See
   * `local_replica` in `crates/palimpsest-client-js/src/local.rs`.
   */
  localReplica(options: RawLocalReplicaOptions): Promise<WasmLocalReplica>;
  /**
   * Subscribe to a server-registered named prepared query. The browser
   * never holds or sends SQL on this path; `params` is keyed by the
   * registered parameter names (or `$N` positions).
   */
  subscribeNamed(
    name: string,
    params: Record<string, NamedQueryParam>,
  ): Promise<WasmSubscription>;
  shutdown(): Promise<void>;
  /**
   * Register a callback that fires once with the current transport
   * state and then on every transition until the client is shut down.
   * The payload matches `RawConnectionStatus` below; the typed wrapper
   * surfaces it as {@link ConnectionStatus}.
   */
  onConnectionStatus(callback: (status: RawConnectionStatus) => void): void;
}

/**
 * Accepted value shapes for named-query parameters. Converted by JS
 * type in the wasm bindings: string → string, boolean → bool, integral
 * number → int, other number → float, `null` → SQL null, array of the
 * above → list (for `= ANY($n)` parameters).
 */
export type NamedQueryParam =
  | string
  | number
  | boolean
  | null
  | Array<string | number | boolean | null>;

/** Raw connection-state payload — mirrors `connection_state_to_js`
 *  in `crates/palimpsest-client-js/src/bindings.rs`. */
export type RawConnectionStatus =
  | { kind: "connecting" }
  | { kind: "connected" }
  | { kind: "reconnecting"; attempt: number; delayMs: number }
  | { kind: "closed"; reason: string };

export interface WasmSubscription {
  onDiff(callback: (event: RawDiffEvent) => void): void;
  update(vars: Record<string, string>): Promise<void>;
  ack(lsn: bigint | number): Promise<void>;
  unsubscribe(): Promise<void>;
}

// ---------------------------------------------------------------------------
// Local-first replica raw contract — mirrors
// `crates/palimpsest-client-js/src/local.rs`.

/** Options accepted by `WasmClient.localReplica`. */
export interface RawLocalReplicaOptions {
  /** Driver object with `exec(sql, params) => Promise<{rows}>`. */
  database: { exec(sql: string, params: unknown[]): Promise<{ rows: unknown[][] }> };
  /** Mirror specs: table name, `{table}`, `{sql, as}`, or `{name, params?}`. */
  mirrors: Array<
    | string
    | { table: string }
    | { sql: string; as: string }
    | { name: string; params?: Record<string, NamedQueryParam> }
  >;
  /** Optional optimistic-write forwarder; reject to roll back. */
  writer?: (request: RawWriteRequest) => Promise<void> | void;
}

/** Optimistic write handed to the writer callback. */
export type RawWriteRequest =
  | { token: number; table: string; kind: "insert"; values: Record<string, unknown> }
  | {
      token: number;
      table: string;
      kind: "update";
      key: Record<string, unknown>;
      set: Record<string, unknown>;
    }
  | { token: number; table: string; kind: "delete"; key: Record<string, unknown> };

/** Mutation payload accepted by `WasmLocalReplica.mutate`. */
export type RawLocalMutation =
  | { table: string; insert: Record<string, unknown> }
  | { table: string; key: Record<string, unknown>; set: Record<string, unknown> }
  | { table: string; key: Record<string, unknown>; delete: true };

/** Per-table sync status from `tableStates()` / `tableState` events. */
export type RawTableSyncStatus =
  | { kind: "connecting" }
  | { kind: "snapshotting" }
  | { kind: "live"; lsn: bigint }
  | { kind: "resyncing" }
  | { kind: "errored"; message: string };

/** Replica notification events delivered to `onEvent`. */
export type RawReplicaEvent =
  | { kind: "tableState"; table: string; state: RawTableSyncStatus }
  | { kind: "applied"; table: string; lsn: bigint }
  | { kind: "mutationSettled"; token: number; table: string }
  | { kind: "mutationConflicted"; token: number; table: string }
  | { kind: "mutationFailed"; token: number; table: string; error: string }
  | { kind: "mirrorError"; table: string; code: string; message: string };

/** Wasm handle to a running local-first replica. */
export interface WasmLocalReplica {
  query(sql: string, params: unknown[]): Promise<unknown>;
  mutate(mutation: RawLocalMutation): Promise<number>;
  onEvent(callback: (event: RawReplicaEvent) => void): void;
  tableStates(): Record<string, RawTableSyncStatus>;
  pendingMutations(table: string): Promise<number>;
  stop(): Promise<void>;
}
