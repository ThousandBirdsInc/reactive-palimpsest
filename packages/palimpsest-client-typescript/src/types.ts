// Public types of the typed wrapper.

import type { RawDiffOp, RawSchema } from "./wasm.js";

/**
 * Numeric datum types reported by the server schema. Mirrors
 * `palimpsest_wal::DatumType`.
 *
 * These constants are kept in sync with the wire enum; missing or
 * unknown values flow through as the raw number from the server.
 */
export const DatumType = {
  Bool: 0,
  I16: 1,
  I32: 2,
  I64: 3,
  F32: 4,
  F64: 5,
  Numeric: 6,
  Text: 7,
  Bytea: 8,
  Date: 9,
  Time: 10,
  Timestamp: 11,
  TimestampTz: 12,
  Interval: 13,
  Uuid: 14,
  Json: 15,
  Jsonb: 16,
} as const;

/** Per-column metadata from the server's `Accepted` message. */
export interface Column {
  name: string;
  /** Raw datum type discriminator. */
  type: number;
  /** Human-readable name. */
  typeName: string;
  nullable: boolean;
}

/** Result-set schema attached to every subscription. */
export interface Schema {
  columns: Column[];
  /** Indexes (into `columns`) that form the primary key. */
  primaryKeyColumns: number[];
}

export function schemaFromRaw(raw: RawSchema): Schema {
  return {
    columns: raw.columns.map((c) => ({
      name: c.name,
      type: c.type,
      typeName: c.typeName,
      nullable: c.nullable,
    })),
    primaryKeyColumns: [...raw.primaryKeyColumns],
  };
}

/** Diff-payload operation, mapped from the wire constant. */
export type DiffOp = "initial" | "insert" | "update" | "delete";

export function diffOpFromRaw(raw: RawDiffOp): DiffOp {
  switch (raw) {
    case "DIFF_OP_INITIAL":
      return "initial";
    case "DIFF_OP_INSERT":
      return "insert";
    case "DIFF_OP_UPDATE":
      return "update";
    case "DIFF_OP_DELETE":
      return "delete";
  }
}

/**
 * Typed event delivered to subscribers. `T` is the row type the caller
 * declared on `subscribe<T>(...)`.
 */
export type DiffEvent<T> =
  | AcceptedEvent
  | DiffPayloadEvent<T>
  | TransactionEvent<T>
  | ResyncEvent
  | ErrorEvent;

export interface AcceptedEvent {
  kind: "accepted";
  schemaId: number;
  snapshotLsn: bigint;
  schema: Schema;
}

export interface DiffPayloadEvent<T> {
  kind: "diff";
  /** Server LSN at which this batch became visible. */
  lsn: bigint;
  op: DiffOp;
  rows: T[];
}

export interface TransactionEvent<T> {
  kind: "transaction";
  /** Commit LSN for the complete transaction. */
  commitLsn: bigint;
  beginLsn?: bigint;
  endLsn?: bigint;
  transactionId?: number;
  changes: RowChange<T>[];
}

export interface RowChange<T> {
  op: DiffOp;
  old: T | null;
  new: T | null;
}

export interface ResyncEvent {
  kind: "resync";
  /** One of `slot_recreated`, `channel_saturation`, … */
  reason: string;
  message: string;
}

export interface ErrorEvent {
  kind: "error";
  /** `permission_denied`, `query_too_large`, `rate_limited`, …; or `"client"` for client-side errors. */
  code: string;
  message: string;
}

/** Connection identity passed to `PalimpsestClient.connect`. */
export interface ConnectOptions {
  url: string;
  token?: string | null;
}

/**
 * Live status of the underlying transport. The wasm client's
 * connection manager handles reconnect-with-backoff transparently;
 * subscribe to this stream via `PalimpsestClient.onConnectionStatus`
 * to surface "disconnected, retrying in 800 ms" in the UI.
 *
 * Mirrors `ConnectionState` in `crates/palimpsest-client/src/connection.rs`.
 */
export type ConnectionStatus =
  | { kind: "connecting" }
  | { kind: "connected" }
  | {
      kind: "reconnecting";
      /** 1-based consecutive failure count since the last `connected`. */
      attempt: number;
      /** Backoff sleep before the next attempt, in milliseconds. */
      delayMs: number;
    }
  | {
      kind: "closed";
      /** Human-readable cause — `"client shutdown"`, `"auth failure: …"`, etc. */
      reason: string;
    };

/** Options for a single subscription. */
export interface SubscribeOptions {
  vars?: Record<string, string>;
}
