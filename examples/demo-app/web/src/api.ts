// Thin fetch wrapper around the tracker's write API.

// Default to same-origin so we go through whatever reverse proxy is in
// front of the SPA (nginx in the docker-compose setup). Override with
// VITE_API_URL=http://localhost:3000 for `npm run dev` against a bare
// `cargo run` server.
const DEFAULT_API_URL =
  (import.meta.env.VITE_API_URL as string | undefined) ?? "";

/** JSON shape the write API returns for one issue. */
export interface ApiIssue {
  id: number;
  title: string;
  status: string;
  priority: number;
  assignee: string;
  project: string;
  estimate: number;
  created_day: number;
  completed_day: number;
  cycle_days: number;
}

export interface CreateIssueBody {
  title: string;
  /** Client-chosen id — used by the local-first page so its
   *  optimistic insert and the WAL row share a primary key. */
  id?: number;
  status?: string;
  priority?: number;
  assignee?: string;
  project?: string;
  estimate?: number;
}

export interface UpdateIssueBody {
  title?: string;
  status?: string;
  priority?: number;
  assignee?: string;
  estimate?: number;
}

export interface SimulateResponse {
  created: number;
  progressed: number;
  triaged: number;
  cancelled: number;
}

export interface DemoUser {
  id: string;
  display_name: string;
  is_admin: boolean;
}

export interface TokenResponse {
  token: string;
  user: DemoUser;
}

export type PermissionRuleMode = "row_visibility" | "subscribe" | "both";

/// One `[[rule]]` block as the server parsed it — echoed back so the
/// UI can show what's actually active rather than what was typed.
export interface PermissionRuleSummary {
  name: string;
  table: string;
  predicate: string;
  mode: PermissionRuleMode;
}

export interface PermissionsSnapshot {
  /// TOML source currently applied to the running SyncEngine.
  toml: string;
  /// TOML the server booted with (used by the reset button).
  default_toml: string;
  rules: PermissionRuleSummary[];
}

export class ApiClient {
  constructor(public readonly base = DEFAULT_API_URL) {}

  async listUsers(): Promise<DemoUser[]> {
    const res = await fetch(`${this.base}/api/users`);
    if (!res.ok) throw new Error(`listUsers: ${res.status}`);
    const body = (await res.json()) as { users: DemoUser[] };
    return body.users;
  }

  async fetchToken(userId: string): Promise<TokenResponse> {
    const res = await fetch(
      `${this.base}/api/token?user=${encodeURIComponent(userId)}`,
    );
    if (!res.ok) throw new Error(`fetchToken(${userId}): ${res.status}`);
    return (await res.json()) as TokenResponse;
  }

  async createIssue(body: CreateIssueBody): Promise<ApiIssue> {
    const res = await fetch(`${this.base}/api/issues`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`createIssue: ${res.status}`);
    return (await res.json()) as ApiIssue;
  }

  async updateIssue(id: number, body: UpdateIssueBody): Promise<ApiIssue> {
    const res = await fetch(`${this.base}/api/issues/${id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`updateIssue: ${res.status}`);
    return (await res.json()) as ApiIssue;
  }

  async deleteIssue(id: number): Promise<void> {
    const res = await fetch(`${this.base}/api/issues/${id}`, {
      method: "DELETE",
    });
    if (!res.ok && res.status !== 404)
      throw new Error(`deleteIssue: ${res.status}`);
  }

  /** Apply `events` synthetic team events server-side. */
  async simulate(events: number): Promise<SimulateResponse> {
    const res = await fetch(`${this.base}/api/simulate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ events }),
    });
    if (!res.ok) throw new Error(`simulate: ${res.status}`);
    return (await res.json()) as SimulateResponse;
  }

  async getPermissions(): Promise<PermissionsSnapshot> {
    const res = await fetch(`${this.base}/api/permissions`);
    if (!res.ok) throw new Error(`getPermissions: ${res.status}`);
    return (await res.json()) as PermissionsSnapshot;
  }

  /// Submit edited DSL source. On a 422 the server's parse/compile
  /// error message is surfaced as the thrown Error's message so the
  /// editor can display it verbatim.
  async updatePermissions(toml: string): Promise<PermissionRuleSummary[]> {
    const res = await fetch(`${this.base}/api/permissions`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ toml }),
    });
    if (!res.ok) {
      let detail = `updatePermissions: ${res.status}`;
      try {
        const body = (await res.json()) as { error?: string };
        if (body.error) detail = body.error;
      } catch {
        // Non-JSON error body; keep the status-code message.
      }
      throw new Error(detail);
    }
    const body = (await res.json()) as { rules: PermissionRuleSummary[] };
    return body.rules;
  }
}

// Same-origin gRPC-Web: nginx reverse-proxies /palimpsest.sync.v1.*
// paths to the server. Browser code can pass any absolute URL the
// gRPC-Web client treats as a base; using window.location.origin keeps
// the request same-origin (no CORS preflight). In SSR contexts this
// falls back to "".
export const PALIMPSEST_URL =
  (import.meta.env.VITE_PALIMPSEST_URL as string | undefined) ??
  (typeof window !== "undefined" ? window.location.origin : "");
