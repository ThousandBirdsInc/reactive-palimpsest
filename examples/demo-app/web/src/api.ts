// Thin fetch wrapper around the write API.

// Default to same-origin so we go through whatever reverse proxy is in
// front of the SPA (nginx in the docker-compose setup). Override with
// VITE_API_URL=http://localhost:3000 for `npm run dev` against a bare
// `cargo run` server.
const DEFAULT_API_URL =
  (import.meta.env.VITE_API_URL as string | undefined) ?? "";

export interface Post {
  id: number;
  title: string;
  published: boolean;
}

export interface BulkAddOrdersResponse {
  inserted: number;
  category_id: number;
  total_rows: number;
}

export interface AccountWriteResponse {
  updated?: number;
  debited?: number;
  credited?: number;
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

  async createPost(title: string, published = true): Promise<Post> {
    const res = await fetch(`${this.base}/api/posts`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ title, published }),
    });
    if (!res.ok) throw new Error(`createPost: ${res.status}`);
    return (await res.json()) as Post;
  }

  async setPublished(id: number, published: boolean): Promise<Post> {
    const res = await fetch(`${this.base}/api/posts/${id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ published }),
    });
    if (!res.ok) throw new Error(`setPublished: ${res.status}`);
    return (await res.json()) as Post;
  }

  async deletePost(id: number): Promise<void> {
    const res = await fetch(`${this.base}/api/posts/${id}`, {
      method: "DELETE",
    });
    if (!res.ok && res.status !== 404) throw new Error(`deletePost: ${res.status}`);
  }

  async bulkAddOrders(
    categoryId: number,
    count: number,
    floorCents: number,
    spreadCents: number,
  ): Promise<BulkAddOrdersResponse> {
    const res = await fetch(`${this.base}/api/orders/bulk-add`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        category_id: categoryId,
        count,
        floor_cents: floorCents,
        spread_cents: spreadCents,
      }),
    });
    if (!res.ok) throw new Error(`bulkAddOrders: ${res.status}`);
    return (await res.json()) as BulkAddOrdersResponse;
  }

  async deposit(actorUserId: string, amountCents: number): Promise<AccountWriteResponse> {
    return this.accountWrite("/api/accounts/deposit", {
      actor_user_id: actorUserId,
      amount_cents: amountCents,
    });
  }

  async withdraw(actorUserId: string, amountCents: number): Promise<AccountWriteResponse> {
    return this.accountWrite("/api/accounts/withdraw", {
      actor_user_id: actorUserId,
      amount_cents: amountCents,
    });
  }

  async transfer(
    actorUserId: string,
    toUserId: string,
    amountCents: number,
  ): Promise<AccountWriteResponse> {
    return this.accountWrite("/api/accounts/transfer", {
      actor_user_id: actorUserId,
      to_user_id: toUserId,
      amount_cents: amountCents,
    });
  }

  private async accountWrite(
    path: string,
    body: Record<string, string | number>,
  ): Promise<AccountWriteResponse> {
    const res = await fetch(`${this.base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`${path}: ${res.status}`);
    return (await res.json()) as AccountWriteResponse;
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
