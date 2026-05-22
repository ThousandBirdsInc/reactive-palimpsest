// React hook that owns a single PalimpsestClient and shares it across
// mounted subscriptions. The client survives StrictMode double-mounts
// via an idempotent connect promise keyed on (url, token, wasm).
//
// The hook tracks two concerns:
//   1. The initial `PalimpsestClient.connect(...)` — the wasm `init` /
//      module load. This is one-shot per (url, token); if it throws we
//      surface "error" and let the caller decide whether to bump deps.
//   2. The live transport state from `client.onConnectionStatus(...)`.
//      The wasm connection manager already reconnects with exponential
//      backoff transparently; this hook just forwards each transition
//      so UIs can render a "reconnecting in 800 ms" badge and disable
//      writes while disconnected.

import { useEffect, useState } from "react";
import { PalimpsestClient, type ClientOptions } from "../client.js";
import type { ConnectionStatus } from "../types.js";

type CacheKey = string;
const clientCache = new Map<CacheKey, Promise<PalimpsestClient>>();

function cacheKey(options: ClientOptions): CacheKey {
  return `${options.url}::${options.token ?? ""}`;
}

/**
 * Coarse status surfaced to React callers. Layered on top of the wasm
 * connection manager's transitions:
 *  - `connecting`: initial wasm load + first handshake, **or** a
 *    backoff sleep between retries.
 *  - `open`: stream is up and events flow.
 *  - `reconnecting`: stream dropped; manager is sleeping before the
 *    next attempt. Writes should be disabled but rendered state stays.
 *  - `closed`: terminal (manual shutdown, auth rejection). Will not
 *    recover without bumping the hook deps.
 *  - `error`: the wasm `connect()` itself threw — never opened.
 */
export type ClientStatus =
  | "connecting"
  | "open"
  | "reconnecting"
  | "closed"
  | "error";

export interface UsePalimpsestClientResult {
  client: PalimpsestClient | null;
  status: ClientStatus;
  /** Last raw status from the wasm manager. `null` until the first
   *  transition is observed. UIs can read `connection.delayMs` /
   *  `connection.attempt` to render countdowns. */
  connection: ConnectionStatus | null;
  error: Error | null;
}

/**
 * Return a (shared, cached) PalimpsestClient for the given connection
 * options. The same `(url, token)` always returns the same client
 * instance, even across re-renders / re-mounts.
 *
 * Mount the client at a high level (App root) and pass it down via
 * context or props rather than calling this in every component.
 */
export function usePalimpsestClient(
  options: ClientOptions,
): UsePalimpsestClientResult {
  const [client, setClient] = useState<PalimpsestClient | null>(null);
  const [status, setStatus] = useState<ClientStatus>("connecting");
  const [connection, setConnection] = useState<ConnectionStatus | null>(null);
  const [error, setError] = useState<Error | null>(null);

  // eslint-disable-next-line react-hooks/exhaustive-deps -- key derived from options
  useEffect(() => {
    let alive = true;
    setStatus("connecting");
    setConnection(null);
    setError(null);

    const key = cacheKey(options);
    let promise = clientCache.get(key);
    if (!promise) {
      promise = PalimpsestClient.connect(options);
      clientCache.set(key, promise);
    }

    // `unsubscribe` from onConnectionStatus when the effect tears down.
    let cancelConnectionListener: (() => void) | null = null;

    promise
      .then((c) => {
        if (!alive) return;
        setClient(c);
        cancelConnectionListener = c.onConnectionStatus((status) => {
          if (!alive) return;
          setConnection(status);
          setStatus(mapStatus(status));
        });
      })
      .catch((e: unknown) => {
        if (!alive) return;
        clientCache.delete(key);
        setError(e instanceof Error ? e : new Error(String(e)));
        setStatus("error");
      });

    return () => {
      alive = false;
      cancelConnectionListener?.();
    };
  }, [options.url, options.token, options.wasm]);

  return { client, status, connection, error };
}

function mapStatus(s: ConnectionStatus): ClientStatus {
  switch (s.kind) {
    case "connecting":
      return "connecting";
    case "connected":
      return "open";
    case "reconnecting":
      return "reconnecting";
    case "closed":
      return "closed";
  }
}
