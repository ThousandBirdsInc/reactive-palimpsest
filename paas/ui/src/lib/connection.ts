// Resolve client-facing Postgres connection info for a cluster.
//
// The control plane fronts every cluster with a database-proxy listener.
// Clients should connect to the proxy's listen_addr, not the cluster's
// host_assignment.port (which is the upstream backend port and is not
// publicly reachable).
//
// Source of truth for host:port lookup:
//   1. /v1/database-proxy-routes?environment_id=… — per-cluster routes.
//      Preferred because a single environment can host multiple clusters.
//   2. /v1/environments/:id/managed-postgres-endpoint — the env-wide
//      active route. Fallback when no per-cluster proxy route is set.
//
// Username convention is the per-cluster role name set by the node agent
// (`{sanitized_cluster_id}_{app|migration|support}` — see
// palimpsest-paas-node-agent::render_role_sql). Passwords are never
// exposed; the UI renders a `<password>` placeholder and points users at
// the Roles tab.

import { useCallback, useMemo } from "react";
import { useApi } from "./scope";
import { usePaasResource } from "./live";
import type {
  DatabaseProxyRoute,
  ManagedPostgresCluster,
  ManagedPostgresEndpoint,
} from "../types";

export type ConnectionRole = "app" | "migration" | "support";

export interface ConnectionParams {
  host: string;
  port: number;
  database: string;
  username: string;
  sslmode: "require" | "verify-ca" | "verify-full" | "disable";
  /**
   * Where we learned host:port from, surfaced for debugging in the UI.
   * `direct-backend` means no public proxy route is published yet, so
   * we've fallen back to the cluster's host_assignment.port — that
   * works in single-host dev where 127.0.0.1 reaches the backend, but
   * is NOT how clients should connect in production. The UI surfaces
   * an advisory when this source is used.
   */
  source: "database-proxy-route" | "managed-postgres-endpoint" | "direct-backend";
}

/** Host fallback for direct-backend connections; mirrors the server's
 * `PALIMPSEST_PAAS_SQL_CONSOLE_HOST` default in sql_store.rs. */
const DIRECT_BACKEND_HOST = "127.0.0.1";

export interface ConnectionResolution {
  /** Null while loading or when we don't have enough info to render. */
  params: ConnectionParams | null;
  loading: boolean;
  /** Set when neither source has the cluster, so the UI can explain why. */
  reason: string | null;
  refresh: () => void;
}

interface UseClusterConnectionOptions {
  /** Defaults to `postgres`; pass a clone's target_database. */
  database?: string;
  /** Defaults to `app`. */
  role?: ConnectionRole;
}

export function useClusterConnection(
  cluster: ManagedPostgresCluster | null,
  options: UseClusterConnectionOptions = {},
): ConnectionResolution {
  const api = useApi();
  const database = options.database ?? "postgres";
  const role = options.role ?? "app";

  const routes = usePaasResource<DatabaseProxyRoute[]>(
    useCallback(() => api.listDatabaseProxyRoutes(), [api]),
    [],
  );
  const endpoint = usePaasResource<ManagedPostgresEndpoint | null>(
    useCallback(() => api.getManagedPostgresEndpoint(), [api]),
    null,
  );

  const params = useMemo<ConnectionParams | null>(() => {
    if (!cluster) return null;

    const hostPort = pickHostPort(cluster, routes.data, endpoint.data);
    if (!hostPort) return null;

    // Only the proxy route negotiates TLS; the direct-backend fallback
    // talks to the upstream Postgres which (in dev) doesn't have TLS
    // configured. Reflect that so the connection string is accurate.
    const sslmode =
      hostPort.source === "direct-backend"
        ? "disable"
        : endpoint.data?.active_certificate_id
          ? "require"
          : "disable";

    return {
      host: hostPort.host,
      port: hostPort.port,
      database,
      username: roleName(cluster.cluster_id, role),
      sslmode,
      source: hostPort.source,
    };
    // routes.data and endpoint.data identity changes on every poll; we
    // only care about value identity, but for simplicity re-derive every
    // render — it's cheap.
  }, [cluster, routes.data, endpoint.data, database, role]);

  const refresh = useCallback(() => {
    routes.refresh();
    endpoint.refresh();
  }, [routes, endpoint]);

  const loading = routes.loading || endpoint.loading;
  const reason = computeReason(cluster, routes.data, endpoint.data);

  return { params, loading, reason, refresh };
}

interface PickedHostPort {
  host: string;
  port: number;
  source: ConnectionParams["source"];
}

function pickHostPort(
  cluster: ManagedPostgresCluster,
  routes: DatabaseProxyRoute[],
  endpoint: ManagedPostgresEndpoint | null,
): PickedHostPort | null {
  const route = routes.find((r) => r.cluster_id === cluster.cluster_id);
  if (route) {
    const parsed = parseHostPort(route.listen_addr);
    if (parsed) return { ...parsed, source: "database-proxy-route" };
  }
  if (endpoint?.database_proxy_listen_addr && endpoint.active_cluster_id === cluster.cluster_id) {
    const parsed = parseHostPort(endpoint.database_proxy_listen_addr);
    if (parsed) return { ...parsed, source: "managed-postgres-endpoint" };
  }
  // Fallback for dev / pre-cert clusters: the cluster's backend port is
  // reachable on the control-plane host. The server's SQL console takes
  // this same path (sql_store.rs::managed_postgres_role_endpoint).
  if (cluster.host_assignment?.port) {
    return {
      host: DIRECT_BACKEND_HOST,
      port: cluster.host_assignment.port,
      source: "direct-backend",
    };
  }
  return null;
}

// Accepts "host:port" and "[ipv6]:port". Returns null if either piece is
// missing or the port doesn't parse — better to show "no route" than to
// emit a broken connection string.
export function parseHostPort(addr: string): { host: string; port: number } | null {
  const trimmed = addr.trim();
  if (!trimmed) return null;
  const ipv6 = /^\[(.+)\]:(\d+)$/.exec(trimmed);
  if (ipv6) {
    const port = Number(ipv6[2]);
    return Number.isInteger(port) && port > 0 ? { host: ipv6[1]!, port } : null;
  }
  const idx = trimmed.lastIndexOf(":");
  if (idx <= 0) return null;
  const host = trimmed.slice(0, idx);
  const port = Number(trimmed.slice(idx + 1));
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  // Localhost loopback shorthand: surface 127.0.0.1 as "localhost" only
  // if the user explicitly wrote it — leave the raw value otherwise.
  return { host, port };
}

function roleName(clusterId: string, role: ConnectionRole): string {
  return `${sanitizeIdentifierComponent(clusterId)}_${role}`;
}

// Mirrors palimpsest-paas-control-plane::sanitize_identifier_component:
// replace anything outside [A-Za-z0-9_] with `_`.
function sanitizeIdentifierComponent(value: string): string {
  return value.replace(/[^A-Za-z0-9_]/g, "_");
}

function computeReason(
  cluster: ManagedPostgresCluster | null,
  _routes: DatabaseProxyRoute[],
  _endpoint: ManagedPostgresEndpoint | null,
): string | null {
  if (!cluster) return "no cluster loaded";
  if (!cluster.host_assignment && cluster.lifecycle_state !== "ready") {
    return `cluster is ${cluster.lifecycle_state} — connection address is published once the cluster is ready`;
  }
  return null;
}

/** True when params came from the direct-backend fallback. The UI surfaces
 * an advisory in that case so users know this isn't the production path. */
export function isDirectBackend(params: ConnectionParams | null): boolean {
  return params?.source === "direct-backend";
}

// ---------- connection string formatters ----------

const PASSWORD_PLACEHOLDER = "<password>";

export function buildPsqlCommand(params: ConnectionParams): string {
  // `psql "host=… port=… …"` is the most portable shape and the one we
  // recommend in docs.
  const inner = [
    `host=${shellEscape(params.host)}`,
    `port=${params.port}`,
    `dbname=${shellEscape(params.database)}`,
    `user=${shellEscape(params.username)}`,
    `sslmode=${params.sslmode}`,
  ].join(" ");
  return `PGPASSWORD=${PASSWORD_PLACEHOLDER} psql "${inner}"`;
}

export function buildLibpqUri(params: ConnectionParams): string {
  const auth = `${encodeURIComponent(params.username)}:${PASSWORD_PLACEHOLDER}`;
  const host = needsIpv6Brackets(params.host) ? `[${params.host}]` : encodeURIComponent(params.host);
  return `postgresql://${auth}@${host}:${params.port}/${encodeURIComponent(params.database)}?sslmode=${params.sslmode}`;
}

export function buildKeywordDsn(params: ConnectionParams): string {
  return [
    `host=${params.host}`,
    `port=${params.port}`,
    `dbname=${params.database}`,
    `user=${params.username}`,
    `password=${PASSWORD_PLACEHOLDER}`,
    `sslmode=${params.sslmode}`,
  ].join(" ");
}

export function buildJdbcUrl(params: ConnectionParams): string {
  // JDBC takes user+password as query args; sslmode maps to ssl=true +
  // sslmode=… (the modern PostgreSQL JDBC driver accepts the latter
  // directly).
  const host = needsIpv6Brackets(params.host) ? `[${params.host}]` : params.host;
  return `jdbc:postgresql://${host}:${params.port}/${params.database}?user=${encodeURIComponent(params.username)}&password=${PASSWORD_PLACEHOLDER}&sslmode=${params.sslmode}`;
}

function needsIpv6Brackets(host: string): boolean {
  return host.includes(":") && !host.startsWith("[");
}

function shellEscape(value: string): string {
  // Embed in `"…"` so spaces/=/, are fine; escape only " and \.
  return value.replace(/(["\\])/g, "\\$1");
}
