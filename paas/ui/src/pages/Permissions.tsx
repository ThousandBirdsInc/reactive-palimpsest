// Permissions. Two related-but-distinct models live here, one per tab:
//
//   Rule DSL       — a single palimpsest-permissions TOML document for the
//                    environment, compiled by the verifier against a catalog.
//   Query policies — individual per-table read/subscribe predicates stored as
//                    rows, each with a draft/active status and a dry-run check.
//
// They are co-located so the distinction is explicit rather than split across
// two unrelated nav entries.

import { ChangeEvent, useCallback, useEffect, useMemo, useState } from "react";
import { CheckCircle2, FlaskConical, Play, Plus, Save, ShieldCheck, XCircle } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/Empty";
import { StatePill } from "../components/StatePill";
import { CodeEditor } from "../components/CodeEditor";
import { useApi, useScope } from "../lib/scope";
import { usePaasResource } from "../lib/live";
import { highlightToml, highlightSql } from "../lib/highlight";
import type {
  ManagedPostgresCluster,
  PermissionVerifyCatalogTable,
  PermissionVerifyResponse,
  QueryPermissionOperation,
  QueryPermissionPolicy,
  QueryPermissionPolicyDryRunResponse,
  QueryPermissionPolicyStatus,
} from "../types";

type Tab = "dsl" | "policies";

export function Permissions() {
  const [tab, setTab] = useState<Tab>("dsl");
  return (
    <div>
      <nav className="tabs">
        <button
          type="button"
          className={tab === "dsl" ? "tab active" : "tab"}
          onClick={() => setTab("dsl")}
        >
          Rule DSL
        </button>
        <button
          type="button"
          className={tab === "policies" ? "tab active" : "tab"}
          onClick={() => setTab("policies")}
        >
          Query policies
        </button>
      </nav>
      {tab === "dsl" ? <RuleDslTab /> : <QueryPolicies />}
    </div>
  );
}

// ----- Rule DSL tab -----

const DEMO_CATALOG = "__demo__";

const STARTER_DSL = `# Permission rules — palimpsest-permissions DSL (TOML).
#
# Declare the fields carried on the authenticated user. Predicates reference
# them as $user.<name>.
[[user_context]]
name = "id"
type = "int"

[[user_context]]
name = "org_id"
type = "int"

# A rule filters a table to the rows a user may see (and, by default, may
# subscribe to). mode = "row_visibility" | "subscribe" | "both" (default both).
[[rule]]
name = "posts_owner"
table = "posts"
mode = "both"
predicate = "author_id = $user.id"
`;

function RuleDslTab() {
  const api = useApi();
  const scope = useScope();
  const storageKey = `palimpsest-paas-ui-permissions-dsl:${scope.environmentId}`;

  const [dsl, setDsl] = useState<string>("");
  const [savedDsl, setSavedDsl] = useState<string>("");
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<PermissionVerifyResponse | null>(null);
  const [catalogSource, setCatalogSource] = useState<string>(DEMO_CATALOG);

  const clusters = usePaasResource<ManagedPostgresCluster[]>(
    useCallback(() => api.listClusters(), [api]),
    [],
  );

  const loadDocument = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const doc = await api.getPermissionRuleDocument();
      const draft = typeof window !== "undefined" ? window.localStorage.getItem(storageKey) : null;
      const saved = doc?.dsl ?? "";
      setSavedDsl(saved);
      setUpdatedAt(doc?.updated_at ?? null);
      // Prefer an unsaved local draft, then the saved document, then a starter.
      setDsl(draft ?? (saved.trim().length > 0 ? saved : STARTER_DSL));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setDsl(STARTER_DSL);
    } finally {
      setLoading(false);
    }
  }, [api, storageKey]);

  useEffect(() => {
    void loadDocument();
  }, [loadDocument]);

  // Persist the working draft locally so navigating away doesn't lose edits.
  useEffect(() => {
    if (loading || typeof window === "undefined") return;
    window.localStorage.setItem(storageKey, dsl);
  }, [dsl, loading, storageKey]);

  const dirty = dsl !== savedDsl;

  const buildCatalog = useCallback(async (): Promise<PermissionVerifyCatalogTable[] | undefined> => {
    if (catalogSource === DEMO_CATALOG) return undefined;
    const schema = await api.getClusterSchema(catalogSource);
    if (!schema) return undefined;
    const tables: PermissionVerifyCatalogTable[] = [];
    for (const s of schema.schemas) {
      for (const t of s.tables) {
        tables.push({
          name: t.name,
          columns: t.columns.map((c) => ({ name: c.name, type: c.data_type })),
        });
      }
    }
    return tables;
  }, [api, catalogSource]);

  const verify = useCallback(async () => {
    setVerifying(true);
    setError(null);
    try {
      const catalog = await buildCatalog();
      const next = await api.verifyPermissions(dsl, catalog);
      setResult(next);
    } catch (e) {
      setResult(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setVerifying(false);
    }
  }, [api, buildCatalog, dsl]);

  const save = useCallback(async () => {
    setSaving(true);
    setError(null);
    try {
      const doc = await api.savePermissionRuleDocument(dsl);
      setSavedDsl(doc.dsl);
      setUpdatedAt(doc.updated_at ?? null);
      if (typeof window !== "undefined") window.localStorage.removeItem(storageKey);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [api, dsl, storageKey]);

  const subtitle = useMemo(() => {
    const parts: string[] = [`environment ${scope.environmentId}`];
    if (updatedAt) parts.push(`saved ${updatedAt}`);
    if (dirty) parts.push("unsaved changes");
    return parts.join(" · ");
  }, [scope.environmentId, updatedAt, dirty]);

  return (
    <div>
      <PageHeader
        title="Permissions · Rule DSL"
        subtitle={subtitle}
        onRefresh={() => void loadDocument()}
        loading={loading}
        actions={
          <>
            <button
              type="button"
              className="btn-primary"
              disabled={verifying || dsl.trim().length === 0}
              onClick={() => void verify()}
              title="Run the permissions verifier"
            >
              <Play size={13} /> {verifying ? "Verifying…" : "Verify"}
            </button>
            <button
              type="button"
              className="btn-secondary"
              disabled={saving || !dirty}
              onClick={() => void save()}
              title="Save the DSL document"
            >
              <Save size={13} /> {saving ? "Saving…" : "Save"}
            </button>
          </>
        }
      />

      <div className="editor-toolbar" style={{ marginBottom: 10 }}>
        <label className="field inline">
          <span>Verify against</span>
          <select
            value={catalogSource}
            onChange={(e: ChangeEvent<HTMLSelectElement>) => setCatalogSource(e.target.value)}
          >
            <option value={DEMO_CATALOG}>Demo catalog (posts, authors, comments)</option>
            {clusters.data.map((c) => (
              <option key={c.cluster_id} value={c.cluster_id}>
                Cluster · {c.cluster_id}
              </option>
            ))}
          </select>
        </label>
        <span className="muted" style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <ShieldCheck size={12} /> rules compile against the selected catalog
        </span>
      </div>

      <CodeEditor
        value={dsl}
        highlight={highlightToml}
        onChange={(e) => setDsl(e.target.value)}
        placeholder={STARTER_DSL}
        rows={18}
      />

      {error && <div className="error-banner">{error}</div>}

      <VerifyResult result={result} />
    </div>
  );
}

function VerifyResult({ result }: { result: PermissionVerifyResponse | null }) {
  if (!result) {
    return (
      <div style={{ marginTop: 14 }}>
        <Empty title="Not verified yet" hint="Run Verify to compile the rules against the catalog." />
      </div>
    );
  }
  return (
    <div style={{ marginTop: 14 }}>
      <div className={result.ok ? "seed-banner" : "error-banner"}>
        {result.ok ? <CheckCircle2 size={14} /> : <XCircle size={14} />}
        <span>
          {result.ok
            ? `Verified · ${result.rules.length} rule${result.rules.length === 1 ? "" : "s"} compiled against the ${result.catalog_source} catalog`
            : result.error}
        </span>
      </div>

      {result.user_context.length > 0 && (
        <p className="muted" style={{ margin: "10px 0" }}>
          User context:{" "}
          {result.user_context.map((f, i) => (
            <span key={f.name}>
              {i > 0 ? ", " : ""}
              <code className="mono">
                {f.name}: {f.type}
              </code>
            </span>
          ))}
        </p>
      )}

      <div className="section-label">Rules</div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Table</th>
              <th>Mode</th>
              <th>Predicate</th>
              <th>User fields</th>
              <th>Compiled</th>
            </tr>
          </thead>
          <tbody>
            {result.rules.map((rule) => (
              <tr key={rule.name}>
                <td className="mono">{rule.name}</td>
                <td className="mono">{rule.table}</td>
                <td>{rule.mode}</td>
                <td className="mono">{rule.predicate}</td>
                <td className="mono">{rule.user_fields.join(", ") || "—"}</td>
                <td>
                  {rule.canonical ? (
                    <span style={{ color: "var(--ok, #2e7d32)" }}>
                      {rule.tautology ? "tautology (elided)" : "ok"}
                    </span>
                  ) : (
                    "—"
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {result.rules.length === 0 && <Empty title="No rules defined" />}
      </div>
    </div>
  );
}

// ----- Query policies tab -----

interface PolicyForm {
  name: string;
  table_schema: string;
  table_name: string;
  operation: QueryPermissionOperation;
  principal_claim: string;
  predicate_sql: string;
  status: QueryPermissionPolicyStatus;
  sampleContextText: string;
}

function emptyForm(): PolicyForm {
  return {
    name: "",
    table_schema: "public",
    table_name: "",
    operation: "read",
    principal_claim: "sub",
    predicate_sql: "",
    status: "draft",
    sampleContextText: "{}",
  };
}

function formFromPolicy(policy: QueryPermissionPolicy): PolicyForm {
  return {
    name: policy.name,
    table_schema: policy.table_schema,
    table_name: policy.table_name,
    operation: policy.operation,
    principal_claim: policy.principal_claim,
    predicate_sql: policy.predicate_sql,
    status: policy.status,
    sampleContextText: JSON.stringify(policy.sample_context ?? {}, null, 2),
  };
}

function parseObject(text: string): { value?: Record<string, unknown>; error?: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text || "{}");
  } catch (e) {
    return { error: `sample context is not valid JSON: ${e instanceof Error ? e.message : String(e)}` };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { error: "sample context must be a JSON object" };
  }
  return { value: parsed as Record<string, unknown> };
}

function newPolicyId(): string {
  return `qpp_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function QueryPolicies() {
  const api = useApi();
  const scope = useScope();

  const policies = usePaasResource<QueryPermissionPolicy[]>(
    useCallback(() => api.listQueryPermissionPolicies(), [api]),
    [],
  );

  const [editingId, setEditingId] = useState<string | null>(null);
  const [form, setForm] = useState<PolicyForm>(emptyForm);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dryRun, setDryRun] = useState<QueryPermissionPolicyDryRunResponse | null>(null);
  const [dryRunning, setDryRunning] = useState(false);

  function startNew() {
    setEditingId(null);
    setForm(emptyForm());
    setDryRun(null);
    setError(null);
  }

  function selectPolicy(policy: QueryPermissionPolicy) {
    setEditingId(policy.policy_id);
    setForm(formFromPolicy(policy));
    setDryRun(null);
    setError(null);
  }

  function patch<K extends keyof PolicyForm>(key: K, value: PolicyForm[K]) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  const save = useCallback(async () => {
    const ctx = parseObject(form.sampleContextText);
    if (ctx.error) {
      setError(ctx.error);
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const policyId = editingId ?? newPolicyId();
      const saved = await api.upsertQueryPermissionPolicy({
        policy_id: policyId,
        organization_id: scope.organizationId,
        project_id: scope.projectId,
        environment_id: scope.environmentId,
        name: form.name,
        table_schema: form.table_schema,
        table_name: form.table_name,
        operation: form.operation,
        principal_claim: form.principal_claim,
        predicate_sql: form.predicate_sql,
        sample_context: ctx.value as Record<string, unknown>,
        status: form.status,
      });
      setEditingId(saved.policy_id);
      setForm(formFromPolicy(saved));
      policies.refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }, [api, editingId, form, policies, scope]);

  const runDryRun = useCallback(async () => {
    if (!editingId) return;
    const ctx = parseObject(form.sampleContextText);
    if (ctx.error) {
      setError(ctx.error);
      return;
    }
    setDryRunning(true);
    setError(null);
    try {
      const next = await api.dryRunQueryPermissionPolicy(editingId, ctx.value);
      setDryRun(next);
    } catch (e) {
      setDryRun(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDryRunning(false);
    }
  }, [api, editingId, form.sampleContextText]);

  const canSave = form.name.trim() && form.table_name.trim() && form.predicate_sql.trim();

  return (
    <div>
      <PageHeader
        title="Permissions · Query policies"
        subtitle={`${policies.data.length} polic${policies.data.length === 1 ? "y" : "ies"} · environment ${scope.environmentId}`}
        onRefresh={policies.refresh}
        loading={policies.loading}
        actions={
          <button type="button" className="btn-secondary" onClick={startNew}>
            <Plus size={13} /> New policy
          </button>
        }
      />
      {policies.error && <div className="error-banner">{policies.error}</div>}

      <div className="table-wrap" style={{ marginBottom: 14 }}>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Table</th>
              <th>Operation</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {policies.data.map((p) => (
              <tr
                key={p.policy_id}
                onClick={() => selectPolicy(p)}
                className={editingId === p.policy_id ? "row-selectable selected" : "row-selectable"}
                style={{ cursor: "pointer" }}
              >
                <td className="mono">{p.name}</td>
                <td className="mono">
                  {p.table_schema}.{p.table_name}
                </td>
                <td>{p.operation}</td>
                <td>
                  <StatePill state={p.status} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {policies.data.length === 0 && (
          <Empty title="No query policies" hint="Create one with “New policy”." />
        )}
      </div>

      <div className="panel">
        <h2>{editingId ? "Edit policy" : "New policy"}</h2>
        <div className="overview-cols">
          <label className="field">
            <span>Name</span>
            <input value={form.name} onChange={(e) => patch("name", e.target.value)} />
          </label>
          <label className="field">
            <span>Principal claim</span>
            <input
              value={form.principal_claim}
              onChange={(e) => patch("principal_claim", e.target.value)}
            />
          </label>
          <label className="field">
            <span>Table schema</span>
            <input
              value={form.table_schema}
              onChange={(e) => patch("table_schema", e.target.value)}
            />
          </label>
          <label className="field">
            <span>Table name</span>
            <input value={form.table_name} onChange={(e) => patch("table_name", e.target.value)} />
          </label>
          <label className="field">
            <span>Operation</span>
            <select
              value={form.operation}
              onChange={(e) => patch("operation", e.target.value as QueryPermissionOperation)}
            >
              <option value="read">read</option>
              <option value="subscribe">subscribe</option>
            </select>
          </label>
          <label className="field">
            <span>Status</span>
            <select
              value={form.status}
              onChange={(e) => patch("status", e.target.value as QueryPermissionPolicyStatus)}
            >
              <option value="draft">draft</option>
              <option value="active">active</option>
            </select>
          </label>
        </div>

        <div className="section-label" style={{ marginTop: 12 }}>
          Predicate SQL
        </div>
        <CodeEditor
          value={form.predicate_sql}
          highlight={highlightSql}
          onChange={(e) => patch("predicate_sql", e.target.value)}
          placeholder="author_id = current_setting('palimpsest.user_id')::int"
          rows={4}
        />

        <div className="section-label" style={{ marginTop: 12 }}>
          Sample context (JSON)
        </div>
        <textarea
          className="sql-editor"
          spellCheck={false}
          value={form.sampleContextText}
          onChange={(e) => patch("sampleContextText", e.target.value)}
          rows={5}
        />

        {error && <div className="error-banner" style={{ marginTop: 10 }}>{error}</div>}

        <div className="editor-toolbar" style={{ marginTop: 12 }}>
          <button
            type="button"
            className="btn-primary"
            disabled={saving || !canSave}
            onClick={() => void save()}
          >
            <Save size={13} /> {saving ? "Saving…" : "Save policy"}
          </button>
          <button
            type="button"
            className="btn-secondary"
            disabled={!editingId || dryRunning}
            onClick={() => void runDryRun()}
            title={editingId ? "Evaluate the saved policy against the sample context" : "Save the policy first"}
          >
            <FlaskConical size={13} /> {dryRunning ? "Running…" : "Dry-run"}
          </button>
          {!editingId && (
            <span className="muted">Dry-run evaluates the last saved version of a policy.</span>
          )}
        </div>

        {dryRun && (
          <div style={{ marginTop: 12 }}>
            <div className={dryRun.accepted ? "seed-banner" : "error-banner"}>
              {dryRun.accepted ? <CheckCircle2 size={14} /> : <XCircle size={14} />}
              <span>{dryRun.decision_detail}</span>
            </div>
            <p className="muted" style={{ margin: "8px 0 0" }}>
              checked predicate: <code className="mono">{dryRun.checked_predicate_sql}</code>
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
