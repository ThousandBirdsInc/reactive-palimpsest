// Per-resource live data hook.
//
// Components call usePaasResource(name, loader). Today every call goes
// through PollingLiveSource (REST + setInterval). Once the control plane
// is fronted by a palimpsest-server, swap in PalimpsestLiveSource for
// individual resources via `src/lib/live/sql.ts` — components don't change.
//
// See `paas/PAAS-UI-DESIGN.md` §5–§6 for the architecture and dogfood path.

import { useCallback, useEffect, useRef, useState } from "react";

export interface LiveResult<T> {
  data: T;
  loading: boolean;
  error: string | null;
  /** Manually re-fetch / re-subscribe. */
  refresh: () => void;
  /** Source identifier for the connection-status pill. */
  source: "polling" | "palimpsest";
}

export interface UseResourceOptions {
  intervalMs?: number;
  /** Pass a stable list of values that, when changed, force a re-fetch. */
  deps?: ReadonlyArray<unknown>;
}

const DEFAULT_INTERVAL_MS = 10_000;

/**
 * Subscribe to a PaaS resource. `loader` is called once on mount and then
 * every `intervalMs`. When deps change the loader runs immediately.
 */
export function usePaasResource<T>(
  loader: () => Promise<T>,
  initial: T,
  options: UseResourceOptions = {},
): LiveResult<T> {
  const { intervalMs = DEFAULT_INTERVAL_MS, deps = [] } = options;
  const [data, setData] = useState<T>(initial);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loaderRef = useRef(loader);
  loaderRef.current = loader;

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const next = await loaderRef.current();
      setData(next);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refresh, ...deps]);

  useEffect(() => {
    if (intervalMs <= 0) return;
    const timer = window.setInterval(() => void refresh(), intervalMs);
    return () => window.clearInterval(timer);
  }, [refresh, intervalMs]);

  return { data, loading, error, refresh, source: "polling" };
}
