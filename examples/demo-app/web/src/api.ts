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

export class ApiClient {
  constructor(public readonly base = DEFAULT_API_URL) {}

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
}

// Same-origin gRPC-Web: nginx reverse-proxies /palimpsest.sync.v1.*
// paths to the server. Browser code can pass any absolute URL the
// gRPC-Web client treats as a base; using window.location.origin keeps
// the request same-origin (no CORS preflight). In SSR contexts this
// falls back to "".
export const PALIMPSEST_URL =
  (import.meta.env.VITE_PALIMPSEST_URL as string | undefined) ??
  (typeof window !== "undefined" ? window.location.origin : "");
