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
  DiffEvent,
  DiffOp,
  DiffPayloadEvent,
  ErrorEvent,
  ResyncEvent,
  Schema,
  SubscribeOptions,
} from "./types.js";
export { decodeRow, decodeRows, primaryKey } from "./codec.js";
export type { ColumnDecoder, RowDecoderOptions } from "./codec.js";
export type {
  RawAcceptedEvent,
  RawColumn,
  RawDiffEvent,
  RawDiffOp,
  RawDiffPayloadEvent,
  RawErrorEvent,
  RawResyncEvent,
  RawSchema,
  WasmClient,
  WasmClientCtor,
  WasmInit,
  WasmModule,
  WasmSubscription,
} from "./wasm.js";
