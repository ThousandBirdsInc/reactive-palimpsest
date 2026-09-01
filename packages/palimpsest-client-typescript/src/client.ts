// Typed wrapper over the wasm-bindgen `Client`/`Subscription`.
//
// One PalimpsestClient = one underlying gRPC-Web connection. The client
// is cheap to share; pass it around components rather than calling
// connect() in every hook.

import { decodeRow, decodeRows, type RowDecoderOptions } from "./codec.js";
import { LocalReplicaHandle, type LocalReplicaOptions } from "./local.js";
import {
  diffOpFromRaw,
  schemaFromRaw,
  type ConnectOptions,
  type ConnectionStatus,
  type DiffEvent,
  type Schema,
  type SubscribeOptions,
} from "./types.js";
import type {
  NamedQueryParam,
  RawConnectionStatus,
  RawDiffEvent,
  WasmClient,
  WasmModule,
  WasmSubscription,
} from "./wasm.js";

let initPromise: Promise<unknown> | null = null;

async function ensureInit(wasm: WasmModule): Promise<void> {
  if (!initPromise) {
    initPromise = wasm.default();
  }
  await initPromise;
}

/**
 * Subscribed live query. `T` is the row type the caller declared on
 * `client.subscribe<T>(...)`.
 *
 * The subscription owns one wasm `Subscription` handle. `unsubscribe()`
 * is idempotent; calling it twice is safe.
 */
export class TypedSubscription<T> {
  private decodedSchema: Schema | null = null;
  private closed = false;

  constructor(
    private readonly wasmSub: WasmSubscription,
    private readonly decoderOptions: RowDecoderOptions,
  ) {}

  /**
   * Register a callback for every event (accepted / diff / resync / error).
   *
   * Calling `onEvent` twice replaces the previous callback — only the
   * most recent registration receives events.
   */
  onEvent(callback: (event: DiffEvent<T>) => void): void {
    this.wasmSub.onDiff((raw: RawDiffEvent) => {
      callback(this.translate(raw));
    });
  }

  /** Push a fresh `vars` map; triggers a server-side resubscribe. */
  async update(vars: Record<string, string>): Promise<void> {
    await this.wasmSub.update(vars);
  }

  /** Ack the given server LSN. Used as `resume_lsn` after reconnect. */
  async ack(lsn: bigint): Promise<void> {
    await this.wasmSub.ack(lsn);
  }

  /** Tear the subscription down. Safe to call multiple times. */
  async unsubscribe(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.wasmSub.unsubscribe();
  }

  private translate(raw: RawDiffEvent): DiffEvent<T> {
    switch (raw.kind) {
      case "accepted": {
        const schema = schemaFromRaw(raw.schema);
        this.decodedSchema = schema;
        return {
          kind: "accepted",
          schemaId: raw.schemaId,
          snapshotLsn: raw.snapshotLsn,
          schema,
        };
      }
      case "diff": {
        if (!this.decodedSchema) {
          // Defensive: a diff before accepted shouldn't happen, but if
          // it does we surface it as an error rather than corrupt the
          // typed output.
          return {
            kind: "error",
            code: "protocol",
            message: "diff received before accepted",
          };
        }
        return {
          kind: "diff",
          lsn: raw.lsn,
          op: diffOpFromRaw(raw.op),
          rows: decodeRows<T>(raw.rows, this.decodedSchema, this.decoderOptions),
        };
      }
      case "transaction": {
        if (!this.decodedSchema) {
          return {
            kind: "error",
            code: "protocol",
            message: "transaction received before accepted",
          };
        }
        return {
          kind: "transaction",
          commitLsn: raw.commitLsn,
          beginLsn: raw.beginLsn,
          endLsn: raw.endLsn,
          transactionId: raw.transactionId,
          changes: raw.changes.map((change) => ({
            op: diffOpFromRaw(change.op),
            old: change.old
              ? decodeRow<T>(change.old, this.decodedSchema!, this.decoderOptions)
              : null,
            new: change.new
              ? decodeRow<T>(change.new, this.decodedSchema!, this.decoderOptions)
              : null,
          })),
        };
      }
      case "resync":
        return { kind: "resync", reason: raw.reason, message: raw.message };
      case "error":
        return { kind: "error", code: raw.code, message: raw.message };
    }
  }
}

export interface ClientOptions extends ConnectOptions {
  /** wasm-bindgen module produced by `wasm-pack build --target web`. */
  wasm: WasmModule;
  /** Row-decoder overrides applied to every subscription on this client. */
  decoder?: RowDecoderOptions;
}

/**
 * Type-safe wrapper around the wasm `Client`. Construct via
 * `PalimpsestClient.connect(...)`; reuse for many subscriptions.
 */
export class PalimpsestClient {
  private constructor(
    private readonly wasmClient: WasmClient,
    private readonly defaultDecoder: RowDecoderOptions,
  ) {}

  static async connect(options: ClientOptions): Promise<PalimpsestClient> {
    await ensureInit(options.wasm);
    const token = options.token ?? null;
    const wasmClient = await options.wasm.Client.connect(options.url, token);
    return new PalimpsestClient(wasmClient, options.decoder ?? {});
  }

  /**
   * Open a subscription. `T` is the row shape the caller expects.
   *
   * The returned subscription doesn't deliver events until you call
   * `.onEvent(...)`.
   */
  async subscribe<T>(
    sql: string,
    options: SubscribeOptions & { decoder?: RowDecoderOptions } = {},
  ): Promise<TypedSubscription<T>> {
    const wasmSub = await this.wasmClient.subscribe(sql, options.vars ?? {});
    return new TypedSubscription<T>(wasmSub, {
      ...this.defaultDecoder,
      ...(options.decoder ?? {}),
    });
  }

  /**
   * Open a subscription to a server-registered named prepared query.
   *
   * The client sends only `{name, params}` — no SQL ships in the
   * bundle or over the socket. `params` is keyed by the registered
   * parameter names (or `$N` positions); an unknown name, missing
   * param, or ill-typed value is refused by the server with an
   * `error` event (`unknown_query` / `invalid_params`).
   */
  async subscribeNamed<T>(
    name: string,
    params: Record<string, NamedQueryParam> = {},
    options: { decoder?: RowDecoderOptions } = {},
  ): Promise<TypedSubscription<T>> {
    const wasmSub = await this.wasmClient.subscribeNamed(name, params);
    return new TypedSubscription<T>(wasmSub, {
      ...this.defaultDecoder,
      ...(options.decoder ?? {}),
    });
  }

  /**
   * Start a local-first replica: stream the server's permissioned
   * subset of each mirror into a local Postgres-compatible WASM engine
   * (pgrust/pglite-style), run the same SQL against it locally, and
   * apply optimistic mutations that reconcile when the authoritative
   * change comes back through the WAL.
   */
  async localReplica(options: LocalReplicaOptions): Promise<LocalReplicaHandle> {
    const wasmReplica = await this.wasmClient.localReplica({
      database: options.database,
      mirrors: options.mirrors,
      writer: options.writer,
    });
    return new LocalReplicaHandle(wasmReplica);
  }

  /** Close the underlying connection. */
  async shutdown(): Promise<void> {
    await this.wasmClient.shutdown();
  }

  /**
   * Subscribe to transport state transitions. The callback fires once
   * immediately with the current status, then on every change. Returns
   * an unsubscribe function — call it to stop receiving updates (the
   * underlying connection manager keeps running).
   *
   * The wasm side keeps a single forwarding task per registration; the
   * unsubscribe just toggles a local flag, so calling it from an effect
   * cleanup is cheap.
   */
  onConnectionStatus(
    callback: (status: ConnectionStatus) => void,
  ): () => void {
    let active = true;
    this.wasmClient.onConnectionStatus((raw: RawConnectionStatus) => {
      if (!active) return;
      callback(raw satisfies ConnectionStatus);
    });
    return () => {
      active = false;
    };
  }
}
