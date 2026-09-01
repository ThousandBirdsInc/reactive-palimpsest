// Shared session context: persona list, current persona + JWT, the
// Palimpsest client bound to that JWT, and the write-API client.
//
// All three pages (board / analytics / local-first) subscribe under
// the same persona, so this lives above the router. Switching persona
// swaps the token, which re-keys the wasm client — every page's
// subscriptions reconnect and re-filter under the new `$user.*`
// context automatically.

import {
  createContext,
  ReactNode,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { PalimpsestClient, ConnectionStatus } from "@palimpsest/client";
import { usePalimpsestClient } from "@palimpsest/client/react";
import * as wasm from "../pkg/palimpsest_client_js";
import { ApiClient, DemoUser, PALIMPSEST_URL } from "./api";

export interface Session {
  api: ApiClient;
  users: DemoUser[];
  currentUser: DemoUser | null;
  setCurrentUser: (user: DemoUser) => void;
  client: PalimpsestClient | null;
  connection: ConnectionStatus | null;
  /** First auth/connect error worth surfacing, if any. */
  error: string | null;
  /** Activity simulator (synthetic team events via the write API). */
  simulate: {
    running: boolean;
    setRunning: (on: boolean) => void;
    eventsPerMinute: number;
    setEventsPerMinute: (n: number) => void;
    /** Bumps after every applied batch — pages can show a pulse. */
    tick: number;
  };
}

const SessionContext = createContext<Session | null>(null);

export function useSession(): Session {
  const session = useContext(SessionContext);
  if (!session) throw new Error("useSession outside <SessionProvider>");
  return session;
}

/** Events applied per simulator tick (one tick = one HTTP call). */
const SIM_BATCH = 5;

export function SessionProvider({ children }: { children: ReactNode }) {
  const api = useMemo(() => new ApiClient(), []);
  const [users, setUsers] = useState<DemoUser[]>([]);
  const [currentUser, setCurrentUser] = useState<DemoUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    api
      .listUsers()
      .then((list) => {
        if (!alive) return;
        setUsers(list);
        setCurrentUser((prev) => prev ?? list[0] ?? null);
      })
      .catch((e) => {
        if (alive) setAuthError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
  }, [api]);

  useEffect(() => {
    if (!currentUser) return;
    let alive = true;
    setAuthError(null);
    api
      .fetchToken(currentUser.id)
      .then((res) => {
        if (alive) setToken(res.token);
      })
      .catch((e) => {
        if (!alive) return;
        setAuthError(e instanceof Error ? e.message : String(e));
        setToken(null);
      });
    return () => {
      alive = false;
    };
  }, [api, currentUser]);

  // Don't initiate the wasm client until we have a token; with an
  // empty URL the parser refuses synchronously, so no connection task
  // spawns until `token` flips to a real value.
  const clientOpts = useMemo(
    () =>
      token
        ? { url: PALIMPSEST_URL, wasm, token }
        : { url: "", wasm, token: undefined },
    [token],
  );
  const { client, connection, error: clientError } =
    usePalimpsestClient(clientOpts);

  // ---------------------------------------------------------------
  // Activity simulator: a background timer POSTing synthetic team
  // events through the ordinary write API, so the board and the
  // analytics both visibly move without anyone clicking.
  // ---------------------------------------------------------------
  const [running, setRunning] = useState(true);
  const [eventsPerMinute, setEventsPerMinute] = useState(60);
  const [tick, setTick] = useState(0);
  const inFlight = useRef(false);
  useEffect(() => {
    if (!running || !client) return;
    const intervalMs = Math.max(500, (60_000 * SIM_BATCH) / eventsPerMinute);
    const handle = window.setInterval(() => {
      if (inFlight.current) return;
      inFlight.current = true;
      api
        .simulate(SIM_BATCH)
        .then(() => setTick((t) => t + 1))
        .catch((err) => console.warn("simulate failed", err))
        .finally(() => {
          inFlight.current = false;
        });
    }, intervalMs);
    return () => window.clearInterval(handle);
  }, [api, client, running, eventsPerMinute]);

  const error =
    authError ??
    (clientError
      ? "code" in clientError
        ? `${clientError.code}: ${clientError.message}`
        : clientError.message
      : null);

  const session: Session = {
    api,
    users,
    currentUser,
    setCurrentUser,
    client,
    connection,
    error,
    simulate: {
      running,
      setRunning,
      eventsPerMinute,
      setEventsPerMinute,
      tick,
    },
  };

  return (
    <SessionContext.Provider value={session}>
      {children}
    </SessionContext.Provider>
  );
}

/** Persona chips + connection badge, rendered in the top bar. */
export function PersonaBar() {
  const { users, currentUser, setCurrentUser, connection, error } =
    useSession();
  return (
    <div className="persona-bar">
      <div className="filters" role="group" aria-label="Choose persona">
        {users.map((u) => (
          <button
            key={u.id}
            type="button"
            className={`chip ${currentUser?.id === u.id ? "chip-active" : ""}`}
            title={u.is_admin ? "Admin — sees every project" : "Member"}
            onClick={() => setCurrentUser(u)}
          >
            {u.display_name}
          </button>
        ))}
      </div>
      <span
        className={`conn-badge ${
          connection?.kind === "connected" ? "conn-ok" : "conn-warn"
        }`}
      >
        {connection?.kind ?? "starting"}
        {connection?.kind === "reconnecting" &&
          ` (retry in ${connection.delayMs} ms)`}
      </span>
      {error && <span className="conn-badge conn-err">{error}</span>}
    </div>
  );
}

/** Simulator toggle + pace slider, rendered in the top bar. */
export function SimulatorControl() {
  const { simulate } = useSession();
  return (
    <div className="sim-control" aria-label="Activity simulator">
      <label className="auto-toggle">
        <input
          type="checkbox"
          checked={simulate.running}
          onChange={(e) => simulate.setRunning(e.target.checked)}
        />
        team activity
      </label>
      <input
        type="range"
        className="pace-slider"
        min={15}
        max={300}
        step={15}
        value={simulate.eventsPerMinute}
        disabled={!simulate.running}
        onChange={(e) => simulate.setEventsPerMinute(Number(e.target.value))}
        aria-label="Simulated events per minute"
      />
      <span className="pace-value">
        <strong>{simulate.eventsPerMinute}</strong> events/min
      </span>
    </div>
  );
}
