// Live permissions-DSL editor. Apply compiles the draft server-side
// and hot-swaps the rule set on the running SyncEngine; every live
// subscription is sent `Resync(PermissionsChanged)` and resubscribes
// under the new rules, so the board, the analytics, and the local-first
// mirror all re-filter immediately. A rejected draft leaves the active
// rules untouched and shows the compile error inline.

import { useEffect, useRef, useState } from "react";
import { motion } from "motion/react";
import { PermissionRuleSummary, PermissionsSnapshot } from "./api";
import { useSession } from "./session";

/// Example rule sets. Each chip loads its TOML into the editor (it is
/// NOT auto-applied — the user clicks Apply so the compile step stays
/// visible). The `[[user_context]]` block always declares
/// `id`/`is_admin` because those are the only fields the demo JWT
/// supplies.
const DSL_USER_CONTEXT = `[[user_context]]
name = "id"
type = "text"

[[user_context]]
name = "is_admin"
type = "bool"`;

const DSL_PRESETS: { id: string; label: string; toml: string }[] = [
  {
    id: "default",
    label: "Security project: admins only",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "issues_visibility"
table = "issues"
predicate = "project != 'security' OR $user.is_admin = true"
`,
  },
  {
    id: "mine-only",
    label: "Only my issues",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "issues_mine_only"
table = "issues"
predicate = "assignee = $user.id OR $user.is_admin = true"
`,
  },
  {
    id: "urgent-only",
    label: "Members see urgent work only",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "issues_urgent_only"
table = "issues"
predicate = "priority >= 3 OR $user.is_admin = true"
`,
  },
  {
    id: "open",
    label: "Everything visible",
    toml: `${DSL_USER_CONTEXT}
`,
  },
  {
    id: "broken",
    label: "Compile error demo",
    toml: `${DSL_USER_CONTEXT}

[[rule]]
name = "issues_by_team"
table = "issues"
predicate = "team_id = $user.id"
`,
  },
];

export default function PermissionsPlayground() {
  const { api } = useSession();
  const [perms, setPerms] = useState<PermissionsSnapshot | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [applying, setApplying] = useState(false);
  const [applyError, setApplyError] = useState<string | null>(null);
  const [applied, setApplied] = useState<{
    count: number;
    nonce: number;
  } | null>(null);

  useEffect(() => {
    let alive = true;
    api
      .getPermissions()
      .then((snap) => {
        if (alive) setPerms(snap);
      })
      .catch((e) => {
        if (alive) setLoadError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      alive = false;
    };
  }, [api]);

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
      const rules = await api.updatePermissions(draft);
      setPerms((prev) => (prev ? { ...prev, toml: draft, rules } : prev));
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
        The same TOML DSL the server loads from configuration:{" "}
        <code>[[user_context]]</code> declares the fields <code>$user.*</code>{" "}
        may reference, and each <code>[[rule]]</code> guards one table.
        Applying compiles the rules against the catalog and hot-swaps them
        onto the running SyncEngine — every live subscription gets{" "}
        <code>Resync(PermissionsChanged)</code> and resubscribes, so the
        board, the analytics, and the local-first mirror re-filter without a
        reload. Try a preset, then switch personas.
      </p>

      <div
        className="filters dsl-presets"
        role="group"
        aria-label="Example rule sets"
      >
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
            No rules — every issue is visible to every persona.
          </p>
        )}
        {(perms?.rules ?? []).map((rule: PermissionRuleSummary) => (
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
