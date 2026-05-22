// Public entry point. Importing from "@palimpsest/client" gets you the
// core (no React dependency); React hooks live in "@palimpsest/client/react".

export { PalimpsestClient, TypedSubscription } from "./client.js";
export type { ClientOptions } from "./client.js";
export {
  DatumType,
  diffOpFromRaw,
  schemaFromRaw,
} from "./types.js";
export type {
  AcceptedEvent,
  Column,
  ConnectOptions,
  ConnectionStatus,
  DiffEvent,
  DiffOp,
  DiffPayloadEvent,
  ErrorEvent,
  ResyncEvent,
  RowChange,
  Schema,
  SubscribeOptions,
  TransactionEvent,
} from "./types.js";
export { decodeRow, decodeRows, primaryKey } from "./codec.js";
export type { ColumnDecoder, RowDecoderOptions } from "./codec.js";
export type {
  RawAcceptedEvent,
  RawColumn,
  RawConnectionStatus,
  RawDiffEvent,
  RawDiffOp,
  RawDiffPayloadEvent,
  RawErrorEvent,
  RawRowChange,
  RawResyncEvent,
  RawSchema,
  RawTransactionEvent,
  WasmClient,
  WasmClientCtor,
  WasmInit,
  WasmModule,
  WasmSubscription,
} from "./wasm.js";
