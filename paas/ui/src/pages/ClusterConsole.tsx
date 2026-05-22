// SQL console tab for a managed Postgres cluster.
//
// The control plane enforces:
//   - read-only (SET TRANSACTION READ ONLY, single statement)
//   - 5s statement timeout
//   - 1..100 row limit (default 50)
//
// We respect those constraints in the UI: limit selector caps at 100, a
// banner makes the read-only-ness obvious, and we surface server error
// messages verbatim so callers can fix their SQL.

import { ChangeEvent, KeyboardEvent, MouseEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Copy,
  Database,
  Info,
  Lock,
  Play,
  RefreshCw,
  Search,
  Sprout,
  Telescope,
} from "lucide-react";
import { useApi } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { Empty } from "../components/Empty";
import { ConfirmButton } from "../components/ConfirmButton";
import type {
  ClusterOperation,
  ClusterQueryExplain,
  ClusterSchema,
  ClusterSchemaResponse,
  ClusterSchemaTable,
  ClusterSqlResult,
  SampleDataSeedResponse,
  SqlCellValue,
} from "../types";

const HISTORY_KEY = "palimpsest-paas-ui-sql-history";
const HISTORY_LIMIT = 20;
const DEFAULT_SQL = "SELECT current_database(), current_user, version();";
const ROW_LIMITS = [10, 25, 50, 100];
const MAX_COMPLETIONS = 12;
const DEFAULT_DATABASE = "postgres";
const CLONE_OP_KIND = "create_database_clone";

// SELECT we drop into the editor after a successful seed so the user can
// see the dataset immediately. Matches the schema created by the seed
// endpoint (sample_users / sample_posts / sample_comments).
const SAMPLE_DEMO_SQL = `SELECT u.handle,
       count(p.id)                       AS posts,
       count(c.id)                       AS comments
FROM   sample_users  u
LEFT JOIN sample_posts    p ON p.author_id = u.id
LEFT JOIN sample_comments c ON c.author_id = u.id
GROUP BY u.handle
ORDER BY posts DESC, comments DESC;`;

const POSTGRES_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT JOIN",
  "RIGHT JOIN",
  "INNER JOIN",
  "FULL JOIN",
  "ON",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "WITH",
  "AS",
  "DISTINCT",
  "AND",
  "OR",
  "NOT",
  "NULL",
  "IS NULL",
  "IS NOT NULL",
  "BETWEEN",
  "IN",
  "LIKE",
  "ILIKE",
  "EXISTS",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  "TRUE",
  "FALSE",
  "ASC",
  "DESC",
];

const POSTGRES_FUNCTIONS = [
  "avg",
  "count",
  "current_database",
  "current_date",
  "current_timestamp",
  "current_user",
  "date_trunc",
  "jsonb_agg",
  "jsonb_build_object",
  "lower",
  "max",
  "min",
  "now",
  "sum",
  "to_char",
  "upper",
  "version",
];

interface Props {
  clusterId: string;
}

type CompletionKind = "keyword" | "function" | "schema" | "table" | "column";

interface SqlCompletion {
  label: string;
  insertText: string;
  detail: string;
  kind: CompletionKind;
  cursorOffset?: number;
}

interface CompletionState {
  open: boolean;
  items: SqlCompletion[];
  active: number;
  range: { start: number; end: number };
  anchor: { top: number; left: number };
}

interface CompletionContext {
  sqlBeforeCaret: string;
  prefix: string;
  qualifier: string | null;
  replaceRange: { start: number; end: number };
  insideString: boolean;
}

interface CompletionCatalog {
  keywords: SqlCompletion[];
  functions: SqlCompletion[];
  schemas: SqlCompletion[];
  tables: SqlCompletion[];
  columns: SqlCompletion[];
  columnsByTable: Map<string, SqlCompletion[]>;
}

export function ClusterConsoleTab({ clusterId }: Props) {
  const api = useApi();
  const [sql, setSql] = useState<string>(() => loadLastSql(clusterId) ?? DEFAULT_SQL);
  const [limit, setLimit] = useState<number>(50);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ClusterSqlResult | null>(null);
  const [explain, setExplain] = useState<ClusterQueryExplain | null>(null);
  const [explainOpen, setExplainOpen] = useState(false);
  const [history, setHistory] = useState<string[]>(() => loadHistory(clusterId));
  const [browserOpen, setBrowserOpen] = useState(true);
  const [elapsedMs, setElapsedMs] = useState<number | null>(null);
  const [seedResult, setSeedResult] = useState<SampleDataSeedResponse | null>(null);
  const [seeding, setSeeding] = useState(false);
  const [database, setDatabase] = useState(DEFAULT_DATABASE);
  const [completion, setCompletion] = useState<CompletionState>({
    open: false,
    items: [],
    active: 0,
    range: { start: 0, end: 0 },
    anchor: { top: 0, left: 0 },
  });

  const textareaRef = useRef<HTMLTextAreaElement | null>(null);

  // Persist the most recent SQL per-cluster so coming back to the page
  // doesn't lose what you were writing.
  useEffect(() => {
    saveLastSql(clusterId, sql);
  }, [clusterId, sql]);

  const operations = usePaasResource<ClusterOperation[]>(
    useCallback(() => api.listClusterOperations(clusterId), [api, clusterId]),
    [],
    { deps: [clusterId] },
  );
  const databaseOptions = useMemo(
    () => [DEFAULT_DATABASE, ...deriveCloneDatabases(operations.data, clusterId)],
    [operations.data, clusterId],
  );
  const schema = useClusterSchema(clusterId, database);
  const completionCatalog = useMemo(() => buildCompletionCatalog(schema.data), [schema.data]);

  const runQuery = useCallback(async () => {
    const trimmed = sql.trim();
    if (!trimmed) return;
    setRunning(true);
    setError(null);
    setExplain(null);
    setExplainOpen(false);
    const startedAt = performance.now();
    try {
      const next = await api.runClusterQuery(clusterId, trimmed, limit, database);
      setResult(next);
      setHistory((prev) => pushHistory(clusterId, prev, trimmed));
    } catch (e) {
      setResult(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setElapsedMs(Math.round(performance.now() - startedAt));
      setRunning(false);
    }
  }, [api, clusterId, sql, limit, database]);

  const seedSampleData = useCallback(async () => {
    setSeeding(true);
    setError(null);
    setSeedResult(null);
    try {
      const next = await api.seedSampleData(clusterId);
      setSeedResult(next);
      // Refresh the schema browser so the new tables appear, and put a
      // ready-to-run demo query in the editor.
      schema.refresh();
      setSql(SAMPLE_DEMO_SQL);
      setResult(null);
      setExplain(null);
      setExplainOpen(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSeeding(false);
    }
    // schema.refresh is stable from useClusterSchema (useCallback)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, clusterId]);

  const explainQuery = useCallback(async () => {
    const trimmed = sql.trim();
    if (!trimmed) return;
    setRunning(true);
    setError(null);
    try {
      const next = await api.explainClusterQuery(clusterId, trimmed, database);
      setExplain(next);
      setExplainOpen(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  }, [api, clusterId, sql, database]);

  function onKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    const mod = e.metaKey || e.ctrlKey;
    if (completion.open) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setCompletion((prev) => ({
          ...prev,
          active: prev.items.length === 0 ? 0 : (prev.active + 1) % prev.items.length,
        }));
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setCompletion((prev) => ({
          ...prev,
          active: prev.items.length === 0 ? 0 : (prev.active - 1 + prev.items.length) % prev.items.length,
        }));
        return;
      }
      if (e.key === "Enter" || e.key === "Tab") {
        const item = completion.items[completion.active];
        if (item) {
          e.preventDefault();
          applyCompletion(item);
        }
        return;
      }
      if (e.key === "Escape") {
        e.preventDefault();
        closeCompletion();
        return;
      }
    }
    if (mod && e.key === " ") {
      e.preventDefault();
      openCompletion(true);
    } else if (mod && e.key === "Enter") {
      e.preventDefault();
      void runQuery();
    } else if (mod && e.key.toLowerCase() === "e") {
      e.preventDefault();
      void explainQuery();
    }
  }

  function onSqlChange(e: ChangeEvent<HTMLTextAreaElement>) {
    const next = e.target.value;
    setSql(next);
    const caret = e.target.selectionStart;
    requestAnimationFrame(() => updateCompletion(next, caret, false));
  }

  function openCompletion(includeEmptyPrefix: boolean) {
    const ta = textareaRef.current;
    if (!ta) return;
    updateCompletion(sql, ta.selectionStart, includeEmptyPrefix);
  }

  function updateCompletion(sourceSql: string, caret: number, includeEmptyPrefix: boolean) {
    const ta = textareaRef.current;
    if (!ta) return;
    const context = completionContext(sourceSql, caret);
    if (!includeEmptyPrefix && context.prefix.length === 0) {
      closeCompletion();
      return;
    }
    const items = completeSql(context, completionCatalog, includeEmptyPrefix);
    if (items.length === 0) {
      closeCompletion();
      return;
    }
    setCompletion({
      open: true,
      items,
      active: 0,
      range: context.replaceRange,
      anchor: textareaCaretAnchor(ta, caret),
    });
  }

  function closeCompletion() {
    setCompletion((prev) => (prev.open ? { ...prev, open: false } : prev));
  }

  function applyCompletion(item: SqlCompletion) {
    const ta = textareaRef.current;
    if (!ta) return;
    const { start, end } = completion.range;
    const next = sql.slice(0, start) + item.insertText + sql.slice(end);
    const cursor = start + item.insertText.length + (item.cursorOffset ?? 0);
    setSql(next);
    closeCompletion();
    requestAnimationFrame(() => {
      ta.focus();
      ta.setSelectionRange(cursor, cursor);
    });
  }

  function insertAtCursor(text: string) {
    const ta = textareaRef.current;
    if (!ta) {
      setSql((prev) => (prev.endsWith("\n") || prev.length === 0 ? prev + text : `${prev}\n${text}`));
      return;
    }
    const start = ta.selectionStart;
    const end = ta.selectionEnd;
    const next = sql.slice(0, start) + text + sql.slice(end);
    setSql(next);
    requestAnimationFrame(() => {
      ta.focus();
      const caret = start + text.length;
      ta.setSelectionRange(caret, caret);
    });
  }

  return (
    <div className="console">
      <div className={browserOpen ? "console-grid" : "console-grid collapsed"}>
        <aside className="schema-pane">
          <header className="schema-pane-header">
            <button
              type="button"
              className="btn-ghost icon-only"
              aria-label={browserOpen ? "Collapse schema browser" : "Expand schema browser"}
              onClick={() => setBrowserOpen((v) => !v)}
            >
              {browserOpen ? <ChevronRight size={14} /> : <Database size={14} />}
            </button>
            {browserOpen && (
              <>
                <span className="schema-pane-title">Schema</span>
                <button
                  type="button"
                  className="btn-ghost icon-only"
                  title="Refresh schema"
                  onClick={schema.refresh}
                  aria-label="Refresh schema"
                >
                  <RefreshCw size={13} className={schema.loading ? "spin" : undefined} />
                </button>
              </>
            )}
          </header>
          {browserOpen && (
            <SchemaBrowser
              data={schema.data}
              error={schema.error}
              loading={schema.loading}
              onInsert={insertAtCursor}
            />
          )}
        </aside>

        <section className="editor-pane">
          <div className="editor-toolbar">
            <button
              type="button"
              className="btn-primary"
              disabled={running || sql.trim().length === 0}
              onClick={() => void runQuery()}
              title="Run (⌘/Ctrl + Enter)"
            >
              <Play size={13} />
              Run
            </button>
            <button
              type="button"
              className="btn-secondary"
              disabled={running || sql.trim().length === 0}
              onClick={() => void explainQuery()}
              title="Explain (⌘/Ctrl + E)"
            >
              <Telescope size={13} />
              Explain
            </button>
            <label className="field inline">
              <span>Database</span>
              <select
                value={database}
                onChange={(e: ChangeEvent<HTMLSelectElement>) => {
                  setDatabase(e.target.value);
                  setResult(null);
                  setExplain(null);
                  setExplainOpen(false);
                }}
              >
                {databaseOptions.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </label>
            <label className="field inline">
              <span>Limit</span>
              <select
                value={limit}
                onChange={(e: ChangeEvent<HTMLSelectElement>) => setLimit(Number(e.target.value))}
              >
                {ROW_LIMITS.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
            <span className="console-readonly" title="Server enforces SET TRANSACTION READ ONLY">
              <Lock size={12} /> read-only · 5s timeout · single statement
            </span>
            <span className="toolbar-spacer" />
            <ConfirmButton
              label={<><Sprout size={13} /> Seed sample data</>}
              confirmLabel={seeding ? "Seeding…" : "Drop & seed"}
              confirmHint="Drops & recreates sample_users / sample_posts / sample_comments."
              disabled={seeding}
              onConfirm={() => void seedSampleData()}
            />
            {elapsedMs !== null && (
              <span className="muted console-elapsed">{elapsedMs} ms</span>
            )}
          </div>

          {seedResult && (
            <div className="seed-banner">
              <Sprout size={13} aria-hidden="true" />
              <span>
                Seeded <strong>{seedResult.total_rows}</strong> rows:{" "}
                {seedResult.tables.map((t, i) => (
                  <span key={t.name}>
                    {i > 0 ? " · " : null}
                    <span className="mono">{t.name}</span> ({t.rows})
                  </span>
                ))}
              </span>
              <button
                type="button"
                className="btn-ghost"
                onClick={() => setSeedResult(null)}
                aria-label="Dismiss"
              >
                Dismiss
              </button>
            </div>
          )}
          <div className="sql-editor-wrap">
            <textarea
              ref={textareaRef}
              className="sql-editor"
              spellCheck={false}
              value={sql}
              onChange={onSqlChange}
              onKeyDown={onKeyDown}
              onBlur={() => window.setTimeout(closeCompletion, 120)}
              onClick={() => closeCompletion()}
              placeholder="SELECT 1;"
              rows={8}
            />
            {completion.open && (
              <CompletionMenu
                completion={completion}
                onHover={(active) => setCompletion((prev) => ({ ...prev, active }))}
                onPick={applyCompletion}
              />
            )}
          </div>

          {error && <div className="error-banner">{error}</div>}

          {explain && explainOpen && (
            <ExplainPanel explain={explain} onClose={() => setExplainOpen(false)} />
          )}

          <ResultsView result={result} running={running} />

          <HistoryPanel
            history={history}
            current={sql}
            onPick={(s) => {
              setSql(s);
              textareaRef.current?.focus();
            }}
            onClear={() => {
              setHistory([]);
              clearHistory(clusterId);
            }}
          />
        </section>
      </div>
    </div>
  );
}

function CompletionMenu({
  completion,
  onHover,
  onPick,
}: {
  completion: CompletionState;
  onHover: (active: number) => void;
  onPick: (item: SqlCompletion) => void;
}) {
  return (
    <div
      className="sql-completion-menu"
      style={{ top: completion.anchor.top, left: completion.anchor.left }}
      role="listbox"
      aria-label="SQL completions"
    >
      {completion.items.map((item, i) => (
        <button
          key={`${item.kind}:${item.insertText}:${i}`}
          type="button"
          className={i === completion.active ? "sql-completion active" : "sql-completion"}
          role="option"
          aria-selected={i === completion.active}
          onMouseEnter={() => onHover(i)}
          onMouseDown={(e: MouseEvent<HTMLButtonElement>) => {
            e.preventDefault();
            onPick(item);
          }}
        >
          <span className={`sql-completion-kind kind-${item.kind}`}>{completionKindLabel(item.kind)}</span>
          <span className="sql-completion-label mono">{item.label}</span>
          <span className="sql-completion-detail">{item.detail}</span>
        </button>
      ))}
    </div>
  );
}

function buildCompletionCatalog(data: ClusterSchemaResponse | null): CompletionCatalog {
  const columnsByTable = new Map<string, SqlCompletion[]>();
  const schemas: SqlCompletion[] = [];
  const tables: SqlCompletion[] = [];
  const columns: SqlCompletion[] = [];
  const seenTables = new Set<string>();
  const seenSchemas = new Set<string>();

  for (const schema of data?.schemas ?? []) {
    if (!seenSchemas.has(schema.name)) {
      schemas.push({
        label: schema.name,
        insertText: schema.name,
        detail: "schema",
        kind: "schema",
      });
      seenSchemas.add(schema.name);
    }
    for (const table of schema.tables) {
      const qualified = schema.name === "public" ? table.name : `${schema.name}.${table.name}`;
      const tableItems: SqlCompletion[] = [];
      const tableCompletion: SqlCompletion = {
        label: qualified,
        insertText: qualified,
        detail: `table · ${table.columns.length} cols`,
        kind: "table",
      };
      if (!seenTables.has(qualified)) {
        tables.push(tableCompletion);
        seenTables.add(qualified);
      }
      tableItems.push(tableCompletion);
      if (schema.name !== "public" && !seenTables.has(table.name)) {
        tables.push({
          label: table.name,
          insertText: table.name,
          detail: `table · ${schema.name}`,
          kind: "table",
        });
        seenTables.add(table.name);
      }

      const columnItems = table.columns.map<SqlCompletion>((column) => ({
        label: column.name,
        insertText: column.name,
        detail: `column · ${qualified} · ${column.data_type}`,
        kind: "column",
      }));
      columns.push(...columnItems);
      for (const key of [table.name, qualified, `${schema.name}.${table.name}`]) {
        columnsByTable.set(key.toLowerCase(), columnItems);
      }
      for (const tableItem of tableItems) {
        columnsByTable.set(tableItem.insertText.toLowerCase(), columnItems);
      }
    }
  }

  return {
    keywords: POSTGRES_KEYWORDS.map((keyword) => ({
      label: keyword,
      insertText: keyword,
      detail: "keyword",
      kind: "keyword",
    })),
    functions: POSTGRES_FUNCTIONS.map((fn) => ({
      label: `${fn}()`,
      insertText: `${fn}()`,
      detail: "function",
      kind: "function",
      cursorOffset: -1,
    })),
    schemas,
    tables,
    columns,
    columnsByTable,
  };
}

function completionContext(sql: string, caret: number): CompletionContext {
  const sqlBeforeCaret = sql.slice(0, caret);
  const token = sqlBeforeCaret.match(/[A-Za-z0-9_$.]*$/)?.[0] ?? "";
  const tokenStart = caret - token.length;
  const dotIndex = token.lastIndexOf(".");
  const qualifier = dotIndex >= 0 ? token.slice(0, dotIndex) : null;
  const prefix = dotIndex >= 0 ? token.slice(dotIndex + 1) : token;
  const start = dotIndex >= 0 ? tokenStart + dotIndex + 1 : tokenStart;
  return {
    sqlBeforeCaret,
    prefix,
    qualifier: qualifier && qualifier.length > 0 ? qualifier : null,
    replaceRange: { start, end: caret },
    insideString: isInsideSingleQuotedString(sqlBeforeCaret),
  };
}

function completeSql(
  context: CompletionContext,
  catalog: CompletionCatalog,
  includeEmptyPrefix: boolean,
): SqlCompletion[] {
  if (context.insideString) return [];
  const prefix = context.prefix.toLowerCase();
  if (!includeEmptyPrefix && prefix.length === 0) return [];

  if (context.qualifier) {
    const aliases = tableAliases(context.sqlBeforeCaret);
    const qualifier = unquoteIdentifier(context.qualifier).toLowerCase();
    const tableKey = aliases.get(qualifier) ?? qualifier;
    const columns = catalog.columnsByTable.get(tableKey) ?? [];
    return rankCompletions(columns, prefix, includeEmptyPrefix);
  }

  const hint = clauseHint(context.sqlBeforeCaret);
  const source =
    hint === "table"
      ? [...catalog.tables, ...catalog.schemas, ...catalog.keywords]
      : hint === "column"
        ? [...catalog.columns, ...catalog.functions, ...catalog.keywords, ...catalog.tables]
        : [
            ...catalog.tables,
            ...catalog.columns,
            ...catalog.functions,
            ...catalog.keywords,
            ...catalog.schemas,
          ];
  return rankCompletions(source, prefix, includeEmptyPrefix);
}

function rankCompletions(
  completions: SqlCompletion[],
  prefix: string,
  includeEmptyPrefix: boolean,
): SqlCompletion[] {
  const seen = new Set<string>();
  return completions
    .filter((item) => includeEmptyPrefix || item.label.toLowerCase().startsWith(prefix))
    .filter((item) => {
      const key = `${item.kind}:${item.insertText}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => completionScore(a, prefix) - completionScore(b, prefix) || a.label.localeCompare(b.label))
    .slice(0, MAX_COMPLETIONS);
}

function completionScore(item: SqlCompletion, prefix: string): number {
  const label = item.label.toLowerCase();
  const kindRank: Record<CompletionKind, number> = {
    table: 0,
    column: 1,
    function: 2,
    keyword: 3,
    schema: 4,
  };
  if (prefix.length === 0) return kindRank[item.kind];
  if (label === prefix) return -10 + kindRank[item.kind];
  if (label.startsWith(prefix)) return kindRank[item.kind];
  return 100 + kindRank[item.kind];
}

function tableAliases(sqlBeforeCaret: string): Map<string, string> {
  const aliases = new Map<string, string>();
  const re = /\b(?:from|join)\s+("?[\w]+"?(?:\."?[\w]+"?)?)\s+(?:as\s+)?("?[\w]+"?)\b/gi;
  for (const match of sqlBeforeCaret.matchAll(re)) {
    const table = normalizeSqlIdentifierPath(match[1]);
    const alias = normalizeSqlIdentifierPath(match[2]);
    if (table && alias && !isJoinKeyword(alias)) {
      aliases.set(alias.toLowerCase(), table.toLowerCase());
    }
  }
  return aliases;
}

function clauseHint(sqlBeforeCaret: string): "table" | "column" | "any" {
  const normalized = sqlBeforeCaret.replace(/\s+/g, " ").trim().toLowerCase();
  const tail = normalized.split(" ").slice(-4).join(" ");
  if (/\b(from|join)$/.test(tail)) return "table";
  if (/\b(select|where|on|having|by|and|or)$/.test(tail)) return "column";
  return "any";
}

function isInsideSingleQuotedString(sqlBeforeCaret: string): boolean {
  let inString = false;
  for (let i = 0; i < sqlBeforeCaret.length; i += 1) {
    if (sqlBeforeCaret[i] !== "'") continue;
    if (sqlBeforeCaret[i + 1] === "'") {
      i += 1;
      continue;
    }
    inString = !inString;
  }
  return inString;
}

function normalizeSqlIdentifierPath(value: string): string {
  return value
    .split(".")
    .map((part) => unquoteIdentifier(part))
    .join(".");
}

function unquoteIdentifier(value: string): string {
  return value.replace(/^"|"$/g, "");
}

function isJoinKeyword(value: string): boolean {
  return ["on", "where", "left", "right", "inner", "full", "cross", "join"].includes(value.toLowerCase());
}

function completionKindLabel(kind: CompletionKind): string {
  switch (kind) {
    case "keyword":
      return "kw";
    case "function":
      return "fn";
    case "schema":
      return "sch";
    case "table":
      return "tbl";
    case "column":
      return "col";
  }
}

function textareaCaretAnchor(textarea: HTMLTextAreaElement, caret: number): { top: number; left: number } {
  const style = window.getComputedStyle(textarea);
  const mirror = document.createElement("div");
  const span = document.createElement("span");
  const properties = [
    "boxSizing",
    "width",
    "fontFamily",
    "fontSize",
    "fontWeight",
    "letterSpacing",
    "lineHeight",
    "paddingTop",
    "paddingRight",
    "paddingBottom",
    "paddingLeft",
    "borderTopWidth",
    "borderRightWidth",
    "borderBottomWidth",
    "borderLeftWidth",
  ] as const;
  for (const property of properties) {
    mirror.style[property] = style[property];
  }
  mirror.style.position = "absolute";
  mirror.style.visibility = "hidden";
  mirror.style.whiteSpace = "pre-wrap";
  mirror.style.overflowWrap = "break-word";
  mirror.style.top = "0";
  mirror.style.left = "-9999px";
  mirror.textContent = textarea.value.slice(0, caret);
  span.textContent = textarea.value.slice(caret, caret + 1) || " ";
  mirror.appendChild(span);
  document.body.appendChild(mirror);
  const lineHeight = Number.parseFloat(style.lineHeight) || 18;
  const left = textarea.offsetLeft + span.offsetLeft - textarea.scrollLeft;
  const top = textarea.offsetTop + span.offsetTop - textarea.scrollTop + lineHeight + 6;
  document.body.removeChild(mirror);
  return {
    left: Math.max(8, Math.min(left, textarea.clientWidth - 280)),
    top: Math.max(8, top),
  };
}

// ----- schema browser -----

function useClusterSchema(clusterId: string, database: string) {
  const api = useApi();
  const [data, setData] = useState<ClusterSchemaResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const next = await api.getClusterSchema(clusterId, database);
      setData(next);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [api, clusterId, database]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { data, loading, error, refresh };
}

function deriveCloneDatabases(operations: ClusterOperation[], clusterId: string): string[] {
  const prefix = `${clusterId}:`;
  const databases = new Set<string>();
  for (const operation of operations) {
    if (operation.kind !== CLONE_OP_KIND || operation.status !== "succeeded") continue;
    databases.add(
      operation.target_resource_id.startsWith(prefix)
        ? operation.target_resource_id.slice(prefix.length)
        : operation.target_resource_id,
    );
  }
  return [...databases].sort((a, b) => a.localeCompare(b));
}

function SchemaBrowser({
  data,
  error,
  loading,
  onInsert,
}: {
  data: ClusterSchemaResponse | null;
  error: string | null;
  loading: boolean;
  onInsert: (text: string) => void;
}) {
  const [query, setQuery] = useState("");

  const schemas = useMemo<ClusterSchema[]>(() => {
    const all = data?.schemas ?? [];
    const q = query.trim().toLowerCase();
    if (!q) return all;
    return all
      .map((s) => ({
        ...s,
        tables: s.tables.filter(
          (t) =>
            t.name.toLowerCase().includes(q) ||
            t.columns.some((c) => c.name.toLowerCase().includes(q)),
        ),
      }))
      .filter((s) => s.tables.length > 0);
  }, [data, query]);

  if (error) {
    return <div className="schema-pane-body"><div className="error-banner">{error}</div></div>;
  }
  if (loading && !data) {
    return <div className="schema-pane-body"><p className="muted">Loading schema…</p></div>;
  }
  if (!data || data.schemas.length === 0) {
    return (
      <div className="schema-pane-body">
        <Empty title="No schema returned" hint="The cluster may still be initializing." />
      </div>
    );
  }
  return (
    <div className="schema-pane-body">
      <label className="schema-search">
        <Search size={11} aria-hidden="true" />
        <input
          value={query}
          placeholder="filter tables / columns"
          onChange={(e) => setQuery(e.target.value)}
        />
      </label>
      <ul className="schema-list">
        {schemas.map((s) => (
          <SchemaNode key={s.name} schema={s} onInsert={onInsert} forceOpen={query.length > 0} />
        ))}
      </ul>
    </div>
  );
}

function SchemaNode({
  schema,
  onInsert,
  forceOpen,
}: {
  schema: ClusterSchema;
  onInsert: (text: string) => void;
  forceOpen: boolean;
}) {
  const [open, setOpen] = useState(true);
  const isOpen = forceOpen || open;
  return (
    <li>
      <button
        type="button"
        className="schema-row schema-schema"
        onClick={() => setOpen((v) => !v)}
      >
        {isOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        <span className="mono">{schema.name}</span>
        <span className="muted schema-count">{schema.tables.length}</span>
      </button>
      {isOpen && (
        <ul className="schema-children">
          {schema.tables.map((t) => (
            <TableNode key={t.name} schemaName={schema.name} table={t} onInsert={onInsert} forceOpen={forceOpen} />
          ))}
        </ul>
      )}
    </li>
  );
}

function TableNode({
  schemaName,
  table,
  onInsert,
  forceOpen,
}: {
  schemaName: string;
  table: ClusterSchemaTable;
  onInsert: (text: string) => void;
  forceOpen: boolean;
}) {
  const [open, setOpen] = useState(false);
  const isOpen = forceOpen || open;
  const qualified = schemaName === "public" ? table.name : `${schemaName}.${table.name}`;
  return (
    <li>
      <div className="schema-row schema-table">
        <button
          type="button"
          className="schema-toggle"
          aria-label={isOpen ? "Collapse" : "Expand"}
          onClick={() => setOpen((v) => !v)}
        >
          {isOpen ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
        </button>
        <button
          type="button"
          className="schema-table-label mono"
          title={`Insert SELECT * FROM ${qualified}`}
          onClick={() => onInsert(`SELECT * FROM ${qualified} LIMIT 50;`)}
        >
          {table.name}
        </button>
      </div>
      {isOpen && (
        <ul className="schema-children">
          {table.columns.map((c) => (
            <li key={c.name}>
              <button
                type="button"
                className="schema-row schema-column"
                title={`Insert ${c.name}`}
                onClick={() => onInsert(c.name)}
              >
                <span className="mono">{c.name}</span>
                <span className="muted schema-col-type">{c.data_type}{c.nullable ? "" : "·NN"}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

// ----- results -----

function ResultsView({ result, running }: { result: ClusterSqlResult | null; running: boolean }) {
  if (running && !result) {
    return <div className="results muted">Running…</div>;
  }
  if (!result) {
    return null;
  }
  if (result.rows.length === 0) {
    return (
      <div className="results">
        <header className="results-header">
          <span>0 rows</span>
        </header>
        <Empty title="Query returned no rows" />
      </div>
    );
  }
  const columns = deriveColumns(result.rows);
  return (
    <div className="results">
      <header className="results-header">
        <span>
          {result.row_count} row{result.row_count === 1 ? "" : "s"}
        </span>
        {result.truncated && (
          <span className="results-truncated">
            <Info size={11} /> truncated to limit
          </span>
        )}
      </header>
      <div className="results-scroll">
        <table className="results-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, idx) => (
              <tr key={idx}>
                {columns.map((col) => (
                  <td key={col}>
                    <CellValue value={row[col] ?? null} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function deriveColumns(rows: Array<Record<string, unknown>>): string[] {
  // Union of keys across rows in insertion order (first row wins ordering;
  // later rows can contribute keys the first row didn't have).
  const seen = new Set<string>();
  const out: string[] = [];
  for (const row of rows) {
    for (const k of Object.keys(row)) {
      if (!seen.has(k)) {
        seen.add(k);
        out.push(k);
      }
    }
  }
  return out;
}

function CellValue({ value }: { value: SqlCellValue }) {
  if (value === null || value === undefined) {
    return <span className="cell-null">null</span>;
  }
  if (typeof value === "boolean") {
    return <span className={value ? "cell-bool true" : "cell-bool false"}>{String(value)}</span>;
  }
  if (typeof value === "number") {
    return <span className="cell-number">{value}</span>;
  }
  if (typeof value === "string") {
    return <span className="cell-text">{value}</span>;
  }
  return <span className="cell-json mono">{JSON.stringify(value)}</span>;
}

// ----- explain -----

function ExplainPanel({
  explain,
  onClose,
}: {
  explain: ClusterQueryExplain;
  onClose: () => void;
}) {
  return (
    <div className="explain-panel">
      <header className="explain-header">
        <strong>Explain</strong>
        <span className="muted">{explain.statement_kind}</span>
        {explain.estimated_rows != null && (
          <span className="muted">~{Math.round(explain.estimated_rows)} rows</span>
        )}
        {explain.estimated_total_cost != null && (
          <span className="muted">cost {explain.estimated_total_cost.toFixed(2)}</span>
        )}
        <button type="button" className="btn-ghost" onClick={onClose}>
          Close
        </button>
      </header>
      {explain.referenced_tables.length > 0 && (
        <p className="muted">
          tables: {explain.referenced_tables.map((t) => <code key={t} className="mono">{t}</code>).reduce<JSX.Element[]>((acc, el, i) => (i === 0 ? [el] : [...acc, <span key={`s${i}`}>, </span>, el]), [])}
        </p>
      )}
      <pre className="explain-plan">{JSON.stringify(explain.explain_plan, null, 2)}</pre>
    </div>
  );
}

// ----- history -----

function HistoryPanel({
  history,
  current,
  onPick,
  onClear,
}: {
  history: string[];
  current: string;
  onPick: (sql: string) => void;
  onClear: () => void;
}) {
  if (history.length === 0) return null;
  return (
    <div className="history-panel">
      <header className="history-header">
        <span>History</span>
        <button type="button" className="btn-ghost" onClick={onClear}>
          Clear
        </button>
      </header>
      <ul className="history-list">
        {history.map((sql, i) => (
          <li key={i} className={sql === current ? "history-row current" : "history-row"}>
            <button type="button" className="history-pick mono" onClick={() => onPick(sql)}>
              {oneLine(sql)}
            </button>
            <button
              type="button"
              className="btn-ghost icon-only"
              title="Copy"
              aria-label="Copy"
              onClick={() => {
                if (typeof navigator !== "undefined" && navigator.clipboard) {
                  void navigator.clipboard.writeText(sql);
                }
              }}
            >
              <Copy size={12} />
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

function oneLine(sql: string): string {
  const collapsed = sql.replace(/\s+/g, " ").trim();
  return collapsed.length > 140 ? `${collapsed.slice(0, 140)}…` : collapsed;
}

// ----- per-cluster persistence -----

function lastSqlKey(clusterId: string): string {
  return `palimpsest-paas-ui-sql-last:${clusterId}`;
}
function historyKey(clusterId: string): string {
  return `${HISTORY_KEY}:${clusterId}`;
}
function loadLastSql(clusterId: string): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(lastSqlKey(clusterId));
}
function saveLastSql(clusterId: string, sql: string): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(lastSqlKey(clusterId), sql);
}
function loadHistory(clusterId: string): string[] {
  if (typeof window === "undefined") return [];
  const raw = window.localStorage.getItem(historyKey(clusterId));
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as string[]).slice(0, HISTORY_LIMIT) : [];
  } catch {
    return [];
  }
}
function pushHistory(clusterId: string, prev: string[], sql: string): string[] {
  const trimmed = sql.trim();
  if (!trimmed) return prev;
  const deduped = prev.filter((s) => s !== trimmed);
  const next = [trimmed, ...deduped].slice(0, HISTORY_LIMIT);
  if (typeof window !== "undefined") {
    window.localStorage.setItem(historyKey(clusterId), JSON.stringify(next));
  }
  return next;
}
function clearHistory(clusterId: string): void {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(historyKey(clusterId));
}
