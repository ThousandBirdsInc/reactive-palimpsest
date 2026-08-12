import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { motion } from "motion/react";
import {
  usePalimpsestClient,
  usePalimpsestSubscription,
} from "@palimpsest/client/react";
import * as wasm from "../pkg/palimpsest_client_js";
import {
  ApiClient,
  DemoUser,
  PALIMPSEST_URL,
  PermissionRuleSummary,
  PermissionsSnapshot,
} from "./api";

interface Post {
  id: bigint;
  title: string;
  published: boolean;
}

/// Per-row shape the server emits for the orders-aggregate
/// subscription. The dataflow rolls up the 600k-row orders table
/// into one row per category — `(category_id, n, total_cents,
/// avg_cents)` — so the bubble chart only ever sees ~20 rows of
/// aggregate, never the raw orders.
interface OrderAggregateRow {
  category_id: bigint;
  n: bigint;
  total_cents: bigint;
  avg_cents: number;
}

/// Per-row shape the server emits for the posts-by-status chart
/// subscription (`GROUP BY published`). The dataflow emits one row
/// per status — `(published: bool, n: bigint)` — not raw posts.
interface PostsByStatusRow {
  published: boolean;
  n: bigint;
}

interface AccountRow {
  id: bigint;
  owner_user_id: string;
  display_name: string;
  balance_cents: bigint;
}

/// Default bulk-add payload size for the "inject events into
/// category N" button. Same-LSN coalescing means one click = one
/// Diff event with `BULK_ADD_BATCH` rows, regardless of how big the
/// batch is.
const BULK_ADD_BATCH = 1_000;

/// Rows written per auto-write tick. Fixed (not randomized) so the
/// number visible in the UI matches what's actually hitting Postgres
/// and so demo behavior is reproducible across runs.
const AUTO_WRITE_BATCH = 500;

type Filter = "all" | "published" | "draft";

const FILTERS: { id: Filter; label: string; predicate: string | null }[] = [
  { id: "all", label: "All", predicate: null },
  { id: "published", label: "Published", predicate: "published = true" },
  { id: "draft", label: "Drafts", predicate: "published = false" },
];

const CHART_SQL = `WITH stats AS (
  SELECT published, COUNT(*) AS n
  FROM posts
  GROUP BY published
)
SELECT published, n
FROM stats
ORDER BY published`;

/// Multi-CTE aggregate against the 600k-row `orders` table. Chains a
/// per-category rollup through a sort-and-limit. The server compiles
/// this into a differential-dataflow pipeline (BaseTable → Aggregate
/// → TopK) and ships only the top-12 aggregate rows the query
/// produces, not the 600k raw orders.
const ORDER_STATS_SQL = `WITH per_category AS (
  SELECT category_id,
         COUNT(*)          AS n,
         SUM(amount_cents) AS total_cents,
         AVG(amount_cents) AS avg_cents
  FROM orders
  GROUP BY category_id
)
SELECT category_id, n, total_cents, avg_cents
FROM per_category
ORDER BY total_cents DESC
LIMIT 12`;

const ACCOUNTS_SQL = `SELECT id, owner_user_id, display_name, balance_cents
FROM accounts
ORDER BY owner_user_id`;

/// 20-category e-commerce catalog, mirrored from `db.rs::CATEGORY_PROFILES`.
/// `floorCents` and `spreadCents` define the (uniform) price distribution
/// for each category; the auto-write timer + bulk-add buttons use them so
/// new orders fit the category's existing pricing shape. `sector` groups
/// related categories so the bubble chart can color them consistently.
type Sector = "tech" | "apparel" | "grocery" | "home" | "media";
interface Category {
  id: number;
  name: string;
  short: string;
  sector: Sector;
  floorCents: number;
  spreadCents: number;
}
const CATEGORIES: Category[] = [
  { id: 1,  name: "Smartphones", short: "Phones",   sector: "tech",    floorCents: 40_000, spreadCents:  80_000 },
  { id: 2,  name: "Laptops",     short: "Laptops",  sector: "tech",    floorCents: 60_000, spreadCents: 140_000 },
  { id: 3,  name: "Headphones",  short: "Audio",    sector: "tech",    floorCents:  5_000, spreadCents:  12_000 },
  { id: 4,  name: "Cameras",     short: "Cams",     sector: "tech",    floorCents: 30_000, spreadCents:  90_000 },
  { id: 5,  name: "Tees",        short: "Tees",     sector: "apparel", floorCents:  1_500, spreadCents:   3_000 },
  { id: 6,  name: "Jeans",       short: "Jeans",    sector: "apparel", floorCents:  4_000, spreadCents:   7_000 },
  { id: 7,  name: "Sneakers",    short: "Shoes",    sector: "apparel", floorCents:  6_000, spreadCents:  15_000 },
  { id: 8,  name: "Jackets",     short: "Jackets",  sector: "apparel", floorCents:  8_000, spreadCents:  20_000 },
  { id: 9,  name: "Snacks",      short: "Snacks",   sector: "grocery", floorCents:    300, spreadCents:     600 },
  { id: 10, name: "Beverages",   short: "Drinks",   sector: "grocery", floorCents:    400, spreadCents:     800 },
  { id: 11, name: "Dairy",       short: "Dairy",    sector: "grocery", floorCents:    500, spreadCents:   1_200 },
  { id: 12, name: "Pantry",      short: "Pantry",   sector: "grocery", floorCents:    800, spreadCents:   1_800 },
  { id: 13, name: "Cookware",    short: "Cook",     sector: "home",    floorCents:  4_000, spreadCents:  12_000 },
  { id: 14, name: "Bedding",     short: "Bedding",  sector: "home",    floorCents:  5_000, spreadCents:   9_000 },
  { id: 15, name: "Decor",       short: "Decor",    sector: "home",    floorCents:  2_500, spreadCents:   6_000 },
  { id: 16, name: "Furniture",   short: "Furn",     sector: "home",    floorCents: 15_000, spreadCents:  60_000 },
  { id: 17, name: "Books",       short: "Books",    sector: "media",   floorCents:  1_500, spreadCents:   2_500 },
  { id: 18, name: "Games",       short: "Games",    sector: "media",   floorCents:  4_000, spreadCents:   4_000 },
  { id: 19, name: "Movies",      short: "Movies",   sector: "media",   floorCents:  2_000, spreadCents:   1_500 },
  { id: 20, name: "Vinyl",       short: "Vinyl",    sector: "media",   floorCents:  2_500, spreadCents:   4_000 },
];
const CATEGORIES_BY_ID = new Map(CATEGORIES.map((c) => [c.id, c]));

const SECTOR_COLOR: Record<Sector, string> = {
  tech:    "#1a73e8",
  apparel: "#a64ac9",
  grocery: "#15a86b",
  home:    "#e07f23",
  media:   "#d63a5a",
};
const SECTOR_LABEL: Record<Sector, string> = {
  tech: "Tech",
  apparel: "Apparel",
  grocery: "Grocery",
  home: "Home",
  media: "Media",
};

/// Example rule sets for the permissions playground. Each chip loads
/// its TOML into the editor (it is NOT auto-applied — the user clicks
/// Apply so the compile step stays visible). The `[[user_context]]`
/// block always declares `id`/`is_admin` because those are the only
/// fields the demo JWT supplies.
const DSL_USER_CONTEXT = `[[user_context]]
name = "id"
type = "text"

[[user_context]]
name = "is_admin"
type = "bool"`;

const DSL_ACCOUNTS_RULE = `[[rule]]
name = "accounts_visibility"
table = "accounts"
predicate = "owner_user_id = $user.id OR $user.is_admin = true"`;

const DSL_PRESETS: { id: string; label: string; toml: string }[] = [
  {
    id: "published-only",
    label: "Hide drafts from everyone",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "posts_published_only"
table = "posts"
predicate = "published = true"

${DSL_ACCOUNTS_RULE}
`,
  },
  {
    id: "admins-only",
    label: "Posts: admins only",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "posts_admins_only"
table = "posts"
predicate = "$user.is_admin = true"

${DSL_ACCOUNTS_RULE}
`,
  },
  {
    id: "open-accounts",
    label: "Drop the accounts rule",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "posts_visibility"
table = "posts"
predicate = "published = true OR $user.is_admin = true"
`,
  },
  {
    id: "broken",
    label: "Compile error demo",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "posts_by_author"
table = "posts"
predicate = "author_id = $user.id"

${DSL_ACCOUNTS_RULE}
`,
  },
];

function listSqlFor(filter: Filter): string {
  const where = FILTERS.find((f) => f.id === filter)?.predicate;
  return where
    ? `SELECT id, title, published\nFROM posts\nWHERE ${where}`
    : "SELECT id, title, published\nFROM posts";
}

export default function App() {
  const api = useMemo(() => new ApiClient(), []);
  const [draftTitle, setDraftTitle] = useState("");
  const [writeError, setWriteError] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("all");
  const [accountAmount, setAccountAmount] = useState(25);
  const [transferTo, setTransferTo] = useState("bob");

  // User picker state. `users` is populated once from /api/users;
  // `currentUser` drives which token we request from /api/token. The
  // hook below re-keys on `token`, so a switch triggers a fresh
  // wasm Client + fresh subscriptions under the new user context.
  const [users, setUsers] = useState<DemoUser[]>([]);
  const [currentUser, setCurrentUser] = useState<DemoUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);

  // Permissions playground state. `perms` mirrors what the server has
  // actually applied (source + parsed rules); the editor keeps its own
  // draft and reports successful applies back through `onPermsApplied`.
  const [perms, setPerms] = useState<PermissionsSnapshot | null>(null);
  const [permsLoadError, setPermsLoadError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    api
      .getPermissions()
      .then((snap) => {
        if (alive) setPerms(snap);
      })
      .catch((e) => {
        if (alive) setPermsLoadError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
  }, [api]);

  const rulesFor = (table: string): PermissionRuleSummary[] =>
    (perms?.rules ?? []).filter((r) => r.table === table);

  // Load the persona list once; default to the first entry so the
  // page has a connection on first paint.
  useEffect(() => {
    let alive = true;
    api
      .listUsers()
      .then((list) => {
        if (!alive) return;
        setUsers(list);
        if (list.length > 0 && !currentUser) {
          setCurrentUser(list[0]);
        }
      })
      .catch((e) => {
        if (!alive) return;
        setAuthError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
    // Mount-only: we want a single initial list fetch regardless of
    // `api` / `currentUser` identity churn.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // When the selected persona changes, request a fresh JWT. The
  // wasm client will reconnect with the new token via the WS
  // `?token=…` query the bridge re-presents to gRPC.
  useEffect(() => {
    if (!currentUser) return;
    let alive = true;
    setAuthError(null);
    api
      .fetchToken(currentUser.id)
      .then((res) => {
        if (!alive) return;
        setToken(res.token);
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

  // Don't initiate the wasm client until we have a token. With an
  // empty URL the parser refuses synchronously, so no `ConnectionTask`
  // spawns and no WS-without-auth retry loop materializes against the
  // bridge. Once `token` flips from null → string, the deps change
  // and a real client connects.
  const clientOpts = useMemo(
    () =>
      token
        ? { url: PALIMPSEST_URL, wasm, token }
        : { url: "", wasm, token: undefined },
    [token],
  );
  const {
    client,
    status: clientStatus,
    connection: clientConnection,
    error: clientError,
  } = usePalimpsestClient(clientOpts);

  const listSql = useMemo(() => listSqlFor(filter), [filter]);

  const {
    status: listStatus,
    rows: listRows,
    error: listError,
  } = usePalimpsestSubscription<Post>(client, listSql, {
    decoder: { coerceSafeIntegersToNumber: false },
  });

  const { rows: chartRows, error: chartError } =
    usePalimpsestSubscription<PostsByStatusRow>(client, CHART_SQL, {
      decoder: { coerceSafeIntegersToNumber: false },
    });

  // Subscription 3: aggregate over the 600k-row `orders` table. The
  // server compiles the CTE+aggregate+TopK SQL into a dataflow and
  // ships only the top-12 aggregate rows; the client renders them
  // as a multi-axis bubble chart. Live diffs flow through the
  // persistent host's `push_table_batch`: each WAL mutation produces
  // aggregate retract+assert deltas the client merges by PK.
  const { rows: orderRows, error: orderError } =
    usePalimpsestSubscription<OrderAggregateRow>(client, ORDER_STATS_SQL, {
      decoder: { coerceSafeIntegersToNumber: false },
    });

  const { rows: accountRows, error: accountError } =
    usePalimpsestSubscription<AccountRow>(client, ACCOUNTS_SQL, {
      decoder: { coerceSafeIntegersToNumber: false },
    });

  const overallStatus = client ? listStatus : clientStatus;
  const connectionError =
    clientError ?? listError ?? chartError ?? orderError ?? accountError;

  // The server now runs the permission filter through the compiled
  // dataflow, so `listRows` is *already* filtered down to what this
  // persona is allowed to see. We just narrow further when the user
  // clicks a filter chip.
  const visiblePosts = useMemo(() => {
    if (filter === "all") return listRows;
    const wantPublished = filter === "published";
    return listRows.filter((p) => p.published === wantPublished);
  }, [listRows, filter]);

  // `chartRows` is the *aggregate* — one row per `published` value
  // with `n` already computed by the dataflow. Read `n` directly;
  // don't recount.
  const counts = useMemo(() => {
    let published = 0;
    let draft = 0;
    for (const row of chartRows) {
      const n = Number(row.n);
      if (row.published) published = n;
      else draft = n;
    }
    return { published, draft };
  }, [chartRows]);
  const total = counts.published + counts.draft;

  // The dataflow already returns the top-12 categories ordered by
  // `total_cents DESC`. Map into the shape the bubble chart consumes —
  // dollars instead of cents, with the resolved Category descriptor
  // attached so the chart can show names + sector colors.
  const orderTopCategories = useMemo(
    () =>
      orderRows.map((row) => {
        const id = Number(row.category_id);
        const category = CATEGORIES_BY_ID.get(id);
        return {
          id,
          name: category?.name ?? `cat ${id}`,
          short: category?.short ?? `${id}`,
          sector: category?.sector ?? "media",
          count: Number(row.n),
          avgDollars: row.avg_cents / 100,
          totalDollars: Number(row.total_cents) / 100,
        };
      }),
    [orderRows],
  );

  /// Sum of the aggregate's `n` column across the categories the
  /// server sent us. With 20 categories and LIMIT 12 this is a
  /// lower bound, but the visible chart only covers those 12 anyway.
  const orderTotalRows = orderTopCategories.reduce(
    (sum, row) => sum + row.count,
    0,
  );

  const visibleAccountTotal = accountRows.reduce(
    (sum, row) => sum + Number(row.balance_cents),
    0,
  );
  const transferOptions = users.filter((u) => u.id !== currentUser?.id);

  useEffect(() => {
    if (currentUser && transferTo === currentUser.id) {
      setTransferTo(users.find((u) => u.id !== currentUser.id)?.id ?? "");
    } else if (!transferTo && users.length > 1) {
      setTransferTo(users.find((u) => u.id !== currentUser?.id)?.id ?? "");
    }
  }, [currentUser, transferTo, users]);

  async function withWrite<T>(fn: () => Promise<T>): Promise<void> {
    setWriteError(null);
    try {
      await fn();
      // Live diffs flow through the persistent host on the server.
    } catch (e) {
      setWriteError(e instanceof Error ? e.message : String(e));
    }
  }

  async function onCreate(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const title = draftTitle.trim();
    if (!title) return;
    setDraftTitle("");
    await withWrite(() => api.createPost(title, true));
  }

  async function onBulkAddOrders(category: Category) {
    await withWrite(() =>
      api.bulkAddOrders(
        category.id,
        BULK_ADD_BATCH,
        category.floorCents,
        category.spreadCents,
      ),
    );
  }

  async function onDeposit() {
    if (!currentUser) return;
    await withWrite(() =>
      api.deposit(currentUser.id, Math.round(accountAmount * 100)),
    );
  }

  async function onWithdraw() {
    if (!currentUser) return;
    await withWrite(() =>
      api.withdraw(currentUser.id, Math.round(accountAmount * 100)),
    );
  }

  async function onTransfer() {
    if (!currentUser || !transferTo) return;
    await withWrite(() =>
      api.transfer(currentUser.id, transferTo, Math.round(accountAmount * 100)),
    );
  }

  // Background traffic generator — every N ms, push a random batch
  // into a random category through the regular write API, priced
  // from that category's own distribution. Keeps the bubble chart
  // visibly moving so the live-diff path is obvious without the
  // user clicking anything. The pace slider drives the cadence in
  // writes-per-minute (12 = one every 5s, 240 = one every 250ms).
  const [autoWrite, setAutoWrite] = useState(true);
  const [writesPerMinute, setWritesPerMinute] = useState(12);
  const autoWriteIntervalMs = Math.max(
    250,
    Math.round(60_000 / writesPerMinute),
  );
  // Most recent auto-write tick — surfaced in the UI so it's obvious
  // how many rows each timer fire actually pushes through. `nonce`
  // re-keys the badge so it can re-mount and replay the pop animation
  // even when consecutive ticks happen to hit the same category.
  const [lastAutoWrite, setLastAutoWrite] = useState<{
    category: string;
    nonce: number;
  } | null>(null);
  useEffect(() => {
    if (!autoWrite || overallStatus !== "open") return;
    const tick = async () => {
      const category =
        CATEGORIES[Math.floor(Math.random() * CATEGORIES.length)];
      try {
        await api.bulkAddOrders(
          category.id,
          AUTO_WRITE_BATCH,
          category.floorCents,
          category.spreadCents,
        );
        setLastAutoWrite((prev) => ({
          category: category.name,
          nonce: (prev?.nonce ?? 0) + 1,
        }));
      } catch (err) {
        // Don't surface auto-write errors into the user-facing banner;
        // a brief network hiccup shouldn't look like a write failure.
        console.warn("auto-write failed", err);
      }
    };
    const handle = window.setInterval(tick, autoWriteIntervalMs);
    return () => window.clearInterval(handle);
  }, [api, autoWrite, autoWriteIntervalMs, overallStatus]);

  return (
    <>
      <h1>Palimpsest demo</h1>
      <p className="tagline">
        Two live subscriptions over the same <code>posts</code> table,
        multiplexed onto one WebSocket. Each persona authenticates with
        its own JWT and the SyncEngine compiles row-visibility filters
        into the query graph per <code>$user.*</code> context.
      </p>

      <section className="panel">
        <header className="panel-head">
          <h2>Persona</h2>
          <div className="filters" role="group" aria-label="Choose user">
            {users.map((u) => (
              <button
                key={u.id}
                type="button"
                className={`chip ${
                  currentUser?.id === u.id ? "chip-active" : ""
                }`}
                onClick={() => setCurrentUser(u)}
              >
                {u.display_name}
              </button>
            ))}
          </div>
        </header>
        <p className="rule-text">
          Active rule{rulesFor("posts").length === 1 ? "" : "s"} on{" "}
          <code>posts</code>: <RuleList rules={rulesFor("posts")} />
        </p>
        <p className="rule-text">
          User context: <code>$user.id = "{currentUser?.id ?? "—"}"</code>
          {" · "}
          <code>
            $user.is_admin = {currentUser?.is_admin ? "true" : "false"}
          </code>
        </p>
        {authError && <div className="error-banner">{authError}</div>}
      </section>

      <PermissionsPlayground
        perms={perms}
        loadError={permsLoadError}
        onApply={async (toml) => {
          const rules = await api.updatePermissions(toml);
          setPerms((prev) =>
            prev ? { ...prev, toml, rules } : prev,
          );
          return rules;
        }}
      />

      <p>
        connection:{" "}
        <span className={`status ${overallStatus}`}>
          {overallStatus}
          {clientConnection?.kind === "reconnecting" && (
            <>
              {" "}attempt {clientConnection.attempt}, retry in{" "}
              {clientConnection.delayMs} ms
            </>
          )}
          {clientConnection?.kind === "closed" && (
            <> — {clientConnection.reason}</>
          )}
        </span>
        {" · "}
        <strong>{listRows.length}</strong> rows after rule + filter
        {connectionError && (
          <span className="status error">
            {" "}
            {"code" in connectionError
              ? `${connectionError.code}: ${connectionError.message}`
              : connectionError.message}
          </span>
        )}
      </p>

      <form className="create" onSubmit={onCreate}>
        <input
          type="text"
          placeholder="New post title…"
          value={draftTitle}
          onChange={(e) => setDraftTitle(e.target.value)}
        />
        <button
          type="submit"
          disabled={overallStatus !== "open" || draftTitle.trim() === ""}
        >
          Create
        </button>
      </form>

      {writeError && <div className="error-banner">{writeError}</div>}

      <section className="panel">
        <header className="panel-head">
          <h2>Subscription 1 — filtered list</h2>
          <div className="filters" role="group" aria-label="Filter posts">
            {FILTERS.map((f) => (
              <button
                key={f.id}
                type="button"
                className={`chip ${filter === f.id ? "chip-active" : ""}`}
                onClick={() => setFilter(f.id)}
              >
                {f.label}
              </button>
            ))}
          </div>
        </header>
        <pre className="sql" data-active-filter={filter}>
          {listSql}
        </pre>

        {overallStatus === "open" && visiblePosts.length === 0 && (
          <p className="empty">No posts match this filter for {currentUser?.display_name ?? "you"}.</p>
        )}

        <ul className="posts">
          {visiblePosts.map((post) => (
            <li key={String(post.id)}>
              <span className="post-id">#{String(post.id)}</span>
              <span className="post-title">{post.title}</span>
              <span
                className={`badge ${post.published ? "published" : "draft"}`}
              >
                {post.published ? "published" : "draft"}
              </span>
              <button
                className="row-action"
                onClick={() =>
                  withWrite(() =>
                    api.setPublished(Number(post.id), !post.published),
                  )
                }
              >
                {post.published ? "Unpublish" : "Publish"}
              </button>
              <button
                className="row-action"
                onClick={() => withWrite(() => api.deletePost(Number(post.id)))}
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>Subscription 2 — counts by status (CTE)</h2>
        </header>
        <pre className="sql">{CHART_SQL}</pre>

        <Chart
          label="published"
          value={counts.published}
          total={total}
          accent="published"
        />
        <Chart
          label="draft"
          value={counts.draft}
          total={total}
          accent="draft"
        />
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>Subscription 3 — account balances as one transaction</h2>
          <div className="filters" role="group" aria-label="Account actions">
            <button
              type="button"
              className="chip"
              disabled={overallStatus !== "open" || !currentUser}
              onClick={onDeposit}
            >
              +${accountAmount}
            </button>
            <button
              type="button"
              className="chip"
              disabled={overallStatus !== "open" || !currentUser}
              onClick={onWithdraw}
            >
              -${accountAmount}
            </button>
            <button
              type="button"
              className="chip"
              disabled={overallStatus !== "open" || !currentUser || !transferTo}
              onClick={onTransfer}
            >
              transfer ${accountAmount}
            </button>
          </div>
        </header>
        <pre className="sql">{ACCOUNTS_SQL}</pre>
        <p className="rule-text">
          Account visibility rule{rulesFor("accounts").length === 1 ? "" : "s"}:{" "}
          <RuleList rules={rulesFor("accounts")} />. Writes are stricter: each
          persona can deposit to, withdraw from, and transfer out of only their
          own account.
        </p>

        <div className="account-controls">
          <label>
            Amount
            <input
              type="number"
              min={1}
              max={10000}
              value={accountAmount}
              onChange={(e) => setAccountAmount(Number(e.target.value))}
            />
          </label>
          <label>
            Transfer recipient
            <select
              value={transferTo}
              onChange={(e) => setTransferTo(e.target.value)}
            >
              {transferOptions.map((user) => (
                <option key={user.id} value={user.id}>
                  {user.display_name}
                </option>
              ))}
            </select>
          </label>
          <span className="account-total">
            visible total: {formatMoney(visibleAccountTotal)}
          </span>
        </div>

        <AccountBalances rows={accountRows} currentUserId={currentUser?.id} />
      </section>

      <section className="panel">
        <header className="panel-head">
          <h2>
            Subscription 4 — top categories over{" "}
            <strong>{orderTotalRows.toLocaleString()}</strong> orders
          </h2>
          <div className="filters" role="group" aria-label="Inject orders">
            {[
              CATEGORIES_BY_ID.get(9)!,   // Snacks (high vol, low ticket)
              CATEGORIES_BY_ID.get(2)!,   // Laptops (low vol, high ticket)
              CATEGORIES_BY_ID.get(7)!,   // Sneakers (mid)
            ].map((category) => (
              <button
                key={category.id}
                type="button"
                className="chip"
                disabled={overallStatus !== "open"}
                title={`Bulk-insert ${BULK_ADD_BATCH} ${category.name.toLowerCase()} orders priced from the same distribution`}
                onClick={() => onBulkAddOrders(category)}
              >
                +{BULK_ADD_BATCH.toLocaleString()} → {category.name}
              </button>
            ))}
          </div>
        </header>
        <pre className="sql">{ORDER_STATS_SQL}</pre>

        <div className="auto-write-row" aria-label="Auto-write controls">
          <label className="auto-toggle">
            <input
              type="checkbox"
              checked={autoWrite}
              onChange={(e) => setAutoWrite(e.target.checked)}
            />
            auto-write
          </label>
          <input
            type="range"
            className="pace-slider"
            min={12}
            max={240}
            step={12}
            value={writesPerMinute}
            disabled={!autoWrite}
            onChange={(e) => setWritesPerMinute(Number(e.target.value))}
            aria-label="Auto-write pace (writes per minute)"
          />
          <span className="pace-value">
            <strong>{writesPerMinute}</strong>/min · every{" "}
            {(autoWriteIntervalMs / 1000).toFixed(2)}s ·{" "}
            <strong>
              {(writesPerMinute * AUTO_WRITE_BATCH).toLocaleString()}
            </strong>{" "}
            rows / min
          </span>
          {lastAutoWrite && (
            <motion.span
              key={lastAutoWrite.nonce}
              className="auto-write-last"
              initial={{ scale: 0.85, opacity: 0.4 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={{
                type: "spring",
                stiffness: 320,
                damping: 22,
              }}
              aria-live="polite"
            >
              last:{" "}
              <strong>+{AUTO_WRITE_BATCH.toLocaleString()}</strong> →{" "}
              {lastAutoWrite.category}
            </motion.span>
          )}
        </div>

        {orderTopCategories.length === 0 && overallStatus === "open" && (
          <p className="empty">Waiting for first aggregate snapshot…</p>
        )}

        {orderTopCategories.length > 0 && (
          <OrderBubbleChart rows={orderTopCategories} />
        )}
      </section>

      <footer>
        Transport: WebSocket (with <code>?token=…</code>) → in-process
        gRPC bridge → Palimpsest SyncEngine. Auth: HS256 JWT verified
        by <code>JwtAuthenticator</code>.
      </footer>
    </>
  );
}

/// Renders the predicates guarding one table. Rules over a table are
/// disjunctive (a row is visible iff at least one predicate passes),
/// so multiple predicates join with "OR". No rule means no policy —
/// the table is visible to everyone.
function RuleList({ rules }: { rules: PermissionRuleSummary[] }) {
  if (rules.length === 0) {
    return <em>none — table visible to every persona</em>;
  }
  return (
    <>
      {rules.map((rule, i) => (
        <span key={rule.name}>
          {i > 0 && " OR "}
          <code>{rule.predicate}</code>
        </span>
      ))}
    </>
  );
}

interface PermissionsPlaygroundProps {
  perms: PermissionsSnapshot | null;
  loadError: string | null;
  /// Submits the draft to the server; resolves with the parsed rules
  /// on success, throws with the compiler's message on rejection.
  onApply: (toml: string) => Promise<PermissionRuleSummary[]>;
}

/// Editor + controls for the permission-rule TOML DSL. Apply compiles
/// the draft server-side and hot-swaps the rule set on the running
/// SyncEngine; every live subscription is sent
/// `Resync(PermissionsChanged)` and resubscribes under the new rules,
/// so the other panels re-filter immediately. A rejected draft leaves
/// the active rules untouched and shows the compile error inline.
function PermissionsPlayground({
  perms,
  loadError,
  onApply,
}: PermissionsPlaygroundProps) {
  const [draft, setDraft] = useState("");
  const [applying, setApplying] = useState(false);
  const [applyError, setApplyError] = useState<string | null>(null);
  const [applied, setApplied] = useState<{
    count: number;
    nonce: number;
  } | null>(null);

  // Seed the editor once the first snapshot lands; after that the
  // draft belongs to the user and applies flow back through `perms`.
  const seeded = useRef(false);
  useEffect(() => {
    if (perms && !seeded.current) {
      seeded.current = true;
      setDraft(perms.toml);
    }
  }, [perms]);

  const dirty = perms !== null && draft !== perms.toml;

  function loadIntoEditor(toml: string) {
    setDraft(toml);
    setApplyError(null);
  }

  async function apply() {
    if (!perms || applying) return;
    setApplying(true);
    setApplyError(null);
    try {
      const rules = await onApply(draft);
      setApplied((prev) => ({
        count: rules.length,
        nonce: (prev?.nonce ?? 0) + 1,
      }));
    } catch (e) {
      setApplied(null);
      setApplyError(e instanceof Error ? e.message : String(e));
    } finally {
      setApplying(false);
    }
  }

  return (
    <section className="panel">
      <header className="panel-head">
        <h2>Permissions DSL — live rule editor</h2>
        <div className="filters" role="group" aria-label="Permissions actions">
          <button
            type="button"
            className="chip"
            disabled={!perms || draft === perms.default_toml}
            onClick={() => perms && loadIntoEditor(perms.default_toml)}
          >
            Reset to default
          </button>
          <button
            type="button"
            className="chip chip-active"
            disabled={!perms || applying || !dirty}
            onClick={apply}
          >
            {applying ? "Compiling…" : "Apply rules"}
          </button>
        </div>
      </header>

      <p className="rule-text">
        This is the same TOML DSL the server loads from configuration:{" "}
        <code>[[user_context]]</code> declares the fields{" "}
        <code>$user.*</code> may reference, and each <code>[[rule]]</code>{" "}
        guards one table. Applying compiles the rules against the catalog and
        hot-swaps them onto the running SyncEngine — every live subscription
        gets <code>Resync(PermissionsChanged)</code> and resubscribes under
        the new rules, so the panels below re-filter without a reload. Try a
        preset, tweak it, and switch personas to see the effect.
      </p>

      <div className="filters dsl-presets" role="group" aria-label="Example rule sets">
        {DSL_PRESETS.map((preset) => (
          <button
            key={preset.id}
            type="button"
            className="chip"
            disabled={!perms}
            title="Load this rule set into the editor (does not apply it)"
            onClick={() => loadIntoEditor(preset.toml)}
          >
            {preset.label}
          </button>
        ))}
      </div>

      <textarea
        className="dsl-editor"
        spellCheck={false}
        value={perms ? draft : "loading current rules…"}
        disabled={!perms}
        onChange={(e) => setDraft(e.target.value)}
        aria-label="Permissions DSL source"
      />

      {loadError && (
        <div className="error-banner">
          failed to load current rules: {loadError}
        </div>
      )}
      {applyError && (
        <div className="error-banner">
          <strong>rejected — </strong>
          {applyError}
        </div>
      )}
      {applied && !applyError && (
        <motion.div
          key={applied.nonce}
          className="dsl-flash"
          initial={{ opacity: 0.4, y: -2 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ type: "spring", stiffness: 320, damping: 24 }}
          aria-live="polite"
        >
          applied — {applied.count} rule{applied.count === 1 ? "" : "s"}{" "}
          compiled, active subscriptions resynced
        </motion.div>
      )}

      <div className="dsl-rules" aria-label="Active compiled rules">
        <span className="dsl-rules-title">active rules</span>
        {(perms?.rules ?? []).length === 0 && (
          <p className="empty">
            No rules — every table is visible to every persona.
          </p>
        )}
        {(perms?.rules ?? []).map((rule) => (
          <div className="dsl-rule" key={rule.name}>
            <span className="dsl-rule-name">{rule.name}</span>
            <span className="dsl-rule-table">{rule.table}</span>
            <span className="dsl-rule-mode">{rule.mode}</span>
            <code className="dsl-rule-pred">{rule.predicate}</code>
          </div>
        ))}
      </div>
    </section>
  );
}

interface ChartProps {
  label: string;
  value: number;
  total: number;
  accent: "published" | "draft";
}

function formatMoney(cents: number): string {
  return (cents / 100).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function AccountBalances({
  rows,
  currentUserId,
}: {
  rows: AccountRow[];
  currentUserId?: string;
}) {
  const maxBalance = Math.max(
    1,
    ...rows.map((row) => Number(row.balance_cents)),
  );
  if (rows.length === 0) {
    return <p className="empty">No account rows are visible for this persona.</p>;
  }
  return (
    <div className="accounts-grid">
      {rows.map((row) => {
        const balance = Number(row.balance_cents);
        const pct = Math.max(6, (balance / maxBalance) * 100);
        const isMine = row.owner_user_id === currentUserId;
        return (
          <div
            key={String(row.id)}
            className={`account-card ${isMine ? "account-mine" : ""}`}
          >
            <div className="account-card-head">
              <span>{row.display_name}</span>
              <span className="account-owner">{row.owner_user_id}</span>
            </div>
            <div className="account-bar-track">
              <div className="account-bar-fill" style={{ width: `${pct}%` }} />
            </div>
            <div className="account-balance">{formatMoney(balance)}</div>
          </div>
        );
      })}
    </div>
  );
}

function Chart({ label, value, total, accent }: ChartProps) {
  const pct = total === 0 ? 0 : (value / total) * 100;
  return (
    <div className="bar-row">
      <span className="bar-label">{label}</span>
      <div className="bar-track">
        <div className={`bar-fill bar-${accent}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="bar-value">{value}</span>
    </div>
  );
}

interface BubbleRow {
  id: number;
  name: string;
  short: string;
  sector: Sector;
  count: number;
  avgDollars: number;
  totalDollars: number;
}

/// Multi-axis bubble chart over the live aggregate. Each bubble is
/// one of the top categories returned by the dataflow's TopK:
///
/// * **X axis** — orders shipped (COUNT(*) per category). Log-scaled
///   because volume spans Snacks (~100k) to Furniture (~3k), and a
///   linear axis would crowd everything against the right edge.
/// * **Y axis** — average order value (AVG(amount_cents) / 100, in
///   dollars). Also log-scaled — Snacks averages ~$5, Laptops ~$1300.
/// * **Area** — total revenue (SUM(amount_cents) / 100). √-scaled so
///   the visual area is proportional to dollars.
/// * **Color** — sector (Tech, Apparel, Grocery, Home, Media), shared
///   across the 3–4 categories in each sector so visual groupings
///   pop without overloading the eye with 20 distinct hues.
///
/// Inline SVG (no chart library) wrapped in `motion` so each bubble
/// + its labels spring into their new position when the dataflow
/// retracts the old aggregate row and asserts the new one. Same
/// row id across frames means motion reuses the DOM node and
/// interpolates `cx`/`cy`/`r`/`x`/`y` instead of teleporting.
function OrderBubbleChart({ rows }: { rows: BubbleRow[] }) {
  const width = 780;
  const height = 420;
  const pad = { top: 24, right: 28, bottom: 56, left: 72 };
  const innerW = width - pad.left - pad.right;
  const innerH = height - pad.top - pad.bottom;

  const counts = rows.map((r) => r.count);
  const avgs = rows.map((r) => r.avgDollars);
  const totals = rows.map((r) => r.totalDollars);
  const tMax = Math.max(...totals, 1);

  // Log scales for both axes so the realistic e-commerce spread
  // (high-volume / low-ticket vs low-volume / high-ticket) reads
  // clearly. Padding kept tight (~6 % per side) so small live-diff
  // movements use as many pixels as possible.
  const xLogLo = Math.log10(Math.max(Math.min(...counts), 1));
  const xLogHi = Math.log10(Math.max(...counts, 10));
  const yLogLo = Math.log10(Math.max(Math.min(...avgs), 1));
  const yLogHi = Math.log10(Math.max(...avgs, 10));
  const xPad = (xLogHi - xLogLo) * 0.06 || 0.15;
  const yPad = (yLogHi - yLogLo) * 0.06 || 0.15;
  const xMin = xLogLo - xPad;
  const xMax = xLogHi + xPad;
  const yMin = yLogLo - yPad;
  const yMax = yLogHi + yPad;

  const sx = (v: number) =>
    pad.left + ((Math.log10(Math.max(v, 1)) - xMin) / (xMax - xMin)) * innerW;
  const sy = (v: number) =>
    pad.top +
    innerH -
    ((Math.log10(Math.max(v, 1)) - yMin) / (yMax - yMin)) * innerH;
  const sr = (totalDollars: number) =>
    14 + Math.sqrt(totalDollars / tMax) * 42;

  // Major ticks at decades (1, 10, 100, 1k, …); minor ticks at 2× and
  // 5× of each decade (e.g. 1, 2, 5, 10, 20, 50, 100, …). The minor
  // gridlines double the eye's reference density so small movements
  // between major ticks are easier to spot.
  const decadeTicks = (lo: number, hi: number, multipliers: number[]) => {
    const start = Math.floor(lo);
    const end = Math.ceil(hi);
    const out: number[] = [];
    for (let p = start; p <= end; p += 1) {
      for (const m of multipliers) {
        const v = m * Math.pow(10, p);
        if (Math.log10(v) >= lo && Math.log10(v) <= hi) out.push(v);
      }
    }
    return out;
  };
  const fmt = (n: number) => {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(0)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(0)}k`;
    if (n >= 1) return n.toFixed(0);
    return n.toString();
  };

  const xMajor = decadeTicks(xMin, xMax, [1]);
  const xMinor = decadeTicks(xMin, xMax, [2, 5]);
  const yMajor = decadeTicks(yMin, yMax, [1]);
  const yMinor = decadeTicks(yMin, yMax, [2, 5]);

  // Sector legend — only show sectors that are actually present in
  // the current top-K so we don't advertise hues that aren't drawn.
  const sectorsShown = Array.from(new Set(rows.map((r) => r.sector)));

  // Pulse tracking: for every row that changed since the last frame,
  // bump its counter so React remounts the SMIL halo with a fresh
  // animation. Even one-row diffs from the dataflow visibly ping
  // their bubble.
  const prevByIdRef = useRef<Map<number, BubbleRow>>(new Map());
  const [pulseCounters, setPulseCounters] = useState<Map<number, number>>(
    () => new Map(),
  );
  useEffect(() => {
    const prev = prevByIdRef.current;
    let updated: Map<number, number> | null = null;
    for (const row of rows) {
      const last = prev.get(row.id);
      const changed =
        last !== undefined &&
        (last.count !== row.count ||
          Math.abs(last.totalDollars - row.totalDollars) > 0.005 ||
          Math.abs(last.avgDollars - row.avgDollars) > 0.005);
      if (changed) {
        if (!updated) updated = new Map(pulseCounters);
        updated.set(row.id, (updated.get(row.id) ?? 0) + 1);
      }
    }
    if (updated) setPulseCounters(updated);
    // Refresh the snapshot regardless so newly-appearing rows
    // don't get a spurious pulse on their next frame.
    const next = new Map<number, BubbleRow>();
    for (const row of rows) next.set(row.id, row);
    prevByIdRef.current = next;
  }, [rows]);

  // For text decisions below.
  const yDollarFmt = (n: number) =>
    n >= 1_000_000
      ? `$${(n / 1_000_000).toFixed(1)}M`
      : n >= 1_000
      ? `$${(n / 1_000).toFixed(0)}k`
      : `$${n.toFixed(0)}`;

  return (
    <>
      <svg
        className="bubble-chart"
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label="Bubble chart of top order categories"
      >
        {/* minor gridlines (2× and 5× of each decade) — drawn first
            so the major lines render on top */}
        {yMinor.map((y, i) => (
          <line
            key={`gym-${i}`}
            x1={pad.left}
            x2={width - pad.right}
            y1={sy(y)}
            y2={sy(y)}
            className="bubble-grid-minor"
          />
        ))}
        {xMinor.map((x, i) => (
          <line
            key={`gxm-${i}`}
            x1={sx(x)}
            x2={sx(x)}
            y1={pad.top}
            y2={height - pad.bottom}
            className="bubble-grid-minor"
          />
        ))}
        {/* major gridlines at decades */}
        {yMajor.map((y, i) => (
          <line
            key={`gy-${i}`}
            x1={pad.left}
            x2={width - pad.right}
            y1={sy(y)}
            y2={sy(y)}
            className="bubble-grid"
          />
        ))}
        {xMajor.map((x, i) => (
          <line
            key={`gx-${i}`}
            x1={sx(x)}
            x2={sx(x)}
            y1={pad.top}
            y2={height - pad.bottom}
            className="bubble-grid"
          />
        ))}
        {/* axes */}
        <line
          x1={pad.left}
          x2={width - pad.right}
          y1={height - pad.bottom}
          y2={height - pad.bottom}
          className="bubble-axis"
        />
        <line
          x1={pad.left}
          x2={pad.left}
          y1={pad.top}
          y2={height - pad.bottom}
          className="bubble-axis"
        />
        {/* minor x tick labels — lighter so the eye groups them as
            references, not as primary readings */}
        {xMinor.map((x, i) => (
          <text
            key={`txm-${i}`}
            x={sx(x)}
            y={height - pad.bottom + 18}
            className="bubble-tick bubble-tick-minor"
            textAnchor="middle"
          >
            {fmt(x)}
          </text>
        ))}
        {/* major x tick labels */}
        {xMajor.map((x, i) => (
          <text
            key={`tx-${i}`}
            x={sx(x)}
            y={height - pad.bottom + 18}
            className="bubble-tick"
            textAnchor="middle"
          >
            {fmt(x)}
          </text>
        ))}
        {/* minor y tick labels — dollars */}
        {yMinor.map((y, i) => (
          <text
            key={`tym-${i}`}
            x={pad.left - 10}
            y={sy(y) + 4}
            className="bubble-tick bubble-tick-minor"
            textAnchor="end"
          >
            ${fmt(y)}
          </text>
        ))}
        {/* major y tick labels */}
        {yMajor.map((y, i) => (
          <text
            key={`ty-${i}`}
            x={pad.left - 10}
            y={sy(y) + 4}
            className="bubble-tick"
            textAnchor="end"
          >
            ${fmt(y)}
          </text>
        ))}
        {/* axis titles */}
        <text
          x={pad.left + innerW / 2}
          y={height - 14}
          className="bubble-axis-label"
          textAnchor="middle"
        >
          orders per category (log scale)
        </text>
        <text
          x={20}
          y={pad.top + innerH / 2}
          className="bubble-axis-label"
          textAnchor="middle"
          transform={`rotate(-90 20 ${pad.top + innerH / 2})`}
        >
          avg ticket ($, log scale)
        </text>
        {/* bubbles */}
        {rows.map((row) => {
          const color = SECTOR_COLOR[row.sector];
          const cx = sx(row.count);
          const cy = sy(row.avgDollars);
          const r = sr(row.totalDollars);
          const pulse = pulseCounters.get(row.id) ?? 0;
          // Three lines of text fit comfortably for r >= 26 (≈ 52px
          // diameter). Smaller bubbles drop to two lines so we don't
          // overflow the circle.
          const showCount = r >= 26;
          // Tuned spring: stiff enough that fast write bursts catch
          // up before the next aggregate frame lands, soft enough
          // that small moves don't snap. One config drives the
          // parent transform and the radius/label offset tweens so
          // every element of a bubble shares the same easing curve.
          const tween = {
            type: "spring",
            stiffness: 180,
            damping: 24,
            mass: 0.7,
          } as const;
          // Position lives on the parent <motion.g> — animating the
          // group's transform once keeps the circle, halo, and every
          // text label locked together as the bubble moves. Children
          // use offsets relative to the group's origin, so they can
          // never desync from the circle the way independently-
          // animated `cx`/`x` attributes did.
          return (
            <motion.g
              key={row.id}
              className="bubble"
              initial={false}
              animate={{ x: cx, y: cy }}
              transition={tween}
            >
              <motion.circle
                initial={false}
                cx={0}
                cy={0}
                animate={{ r }}
                transition={tween}
                style={{
                  fill: `${color}99`, // ~60 % alpha hex suffix
                  stroke: color,
                }}
              />
              {pulse > 0 && (
                <circle
                  key={`halo-${row.id}-${pulse}`}
                  cx={0}
                  cy={0}
                  r={r}
                  className="bubble-halo"
                  stroke={color}
                  fill="none"
                >
                  <animate
                    attributeName="r"
                    from={r}
                    to={r + 22}
                    dur="800ms"
                    fill="freeze"
                  />
                  <animate
                    attributeName="stroke-opacity"
                    from="0.95"
                    to="0"
                    dur="800ms"
                    fill="freeze"
                  />
                  <animate
                    attributeName="stroke-width"
                    from="3"
                    to="0.5"
                    dur="800ms"
                    fill="freeze"
                  />
                </circle>
              )}
              {/* Texts get animated y so the showCount threshold cross
                  (r ≈ 26) eases between layouts instead of snapping. */}
              <motion.text
                initial={false}
                x={0}
                animate={{ y: showCount ? -10 : -1 }}
                transition={tween}
                className="bubble-label"
                textAnchor="middle"
              >
                {row.short}
              </motion.text>
              {showCount && (
                <motion.text
                  initial={false}
                  x={0}
                  animate={{ y: 4 }}
                  transition={tween}
                  className="bubble-sublabel"
                  textAnchor="middle"
                >
                  {row.count.toLocaleString()}
                </motion.text>
              )}
              <motion.text
                initial={false}
                x={0}
                animate={{ y: showCount ? 17 : 12 }}
                transition={tween}
                className="bubble-sublabel bubble-sublabel-strong"
                textAnchor="middle"
              >
                {yDollarFmt(row.totalDollars)}
              </motion.text>
              <title>
                {row.name} · {row.count.toLocaleString()} orders · avg $
                {row.avgDollars.toFixed(2)} · revenue $
                {row.totalDollars.toLocaleString(undefined, {
                  maximumFractionDigits: 0,
                })}
              </title>
            </motion.g>
          );
        })}
      </svg>
      <div className="bubble-legend">
        {sectorsShown.map((sector) => (
          <span key={sector} className="bubble-legend-item">
            <span
              className="bubble-legend-swatch"
              style={{ background: SECTOR_COLOR[sector] }}
            />
            {SECTOR_LABEL[sector]}
          </span>
        ))}
        <span className="bubble-legend-hint">
          area = revenue · size of bubble grows with{" "}
          <code>SUM(amount_cents)</code>
        </span>
      </div>
    </>
  );
}
