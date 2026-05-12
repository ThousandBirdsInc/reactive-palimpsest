// React hook that owns a single PalimpsestClient and shares it across
// mounted subscriptions. The client survives StrictMode double-mounts
// via an idempotent connect promise keyed on (url, token, wasm).

import { useEffect, useState } from "react";
import { PalimpsestClient, type ClientOptions } from "../client.js";

type CacheKey = string;
const clientCache = new Map<CacheKey, Promise<PalimpsestClient>>();

function cacheKey(options: ClientOptions): CacheKey {
  return `${options.url}::${options.token ?? ""}`;
}

export type ClientStatus = "connecting" | "open" | "error";

export interface UsePalimpsestClientResult {
  client: PalimpsestClient | null;
  status: ClientStatus;
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
  const [error, setError] = useState<Error | null>(null);

  // eslint-disable-next-line react-hooks/exhaustive-deps -- key derived from options
  useEffect(() => {
    let alive = true;
    setStatus("connecting");
    setError(null);

    const key = cacheKey(options);
    let promise = clientCache.get(key);
    if (!promise) {
      promise = PalimpsestClient.connect(options);
      clientCache.set(key, promise);
    }

    promise
      .then((c) => {
        if (!alive) return;
        setClient(c);
        setStatus("open");
      })
      .catch((e: unknown) => {
        if (!alive) return;
        clientCache.delete(key);
        setError(e instanceof Error ? e : new Error(String(e)));
        setStatus("error");
      });

    return () => {
      alive = false;
    };
  }, [options.url, options.token, options.wasm]);

  return { client, status, error };
}
