import { FormEvent, useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { useScope, useSetScope, persistScope } from "../lib/scope";
import type { Scope } from "../types";

export function SettingsPage() {
  const scope = useScope();
  const setScope = useSetScope();
  const [draft, setDraft] = useState<Scope>(scope);
  const [saved, setSaved] = useState(false);

  function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    persistScope(draft);
    setScope(draft);
    setSaved(true);
    window.setTimeout(() => setSaved(false), 1500);
  }

  return (
    <div>
      <PageHeader title="Settings" subtitle="Local connection + identity. No remote write." />
      <form onSubmit={submit} className="panel" style={{ maxWidth: 640 }}>
        <h2>Connection</h2>
        <div className="form-grid" style={{ marginBottom: 12 }}>
          <label className="field">
            <span>API base</span>
            <input
              value={draft.apiBase}
              onChange={(e) => setDraft({ ...draft, apiBase: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Actor id</span>
            <input
              value={draft.actorId}
              onChange={(e) => setDraft({ ...draft, actorId: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Organization id</span>
            <input
              value={draft.organizationId}
              onChange={(e) => setDraft({ ...draft, organizationId: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Project id</span>
            <input
              value={draft.projectId}
              onChange={(e) => setDraft({ ...draft, projectId: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Environment id</span>
            <input
              value={draft.environmentId}
              onChange={(e) => setDraft({ ...draft, environmentId: e.target.value })}
            />
          </label>
        </div>
        <div className="row" style={{ justifyContent: "flex-end" }}>
          {saved && <span className="muted">Saved.</span>}
          <button type="submit" className="btn-primary">
            Save
          </button>
        </div>
      </form>

      <div className="panel" style={{ maxWidth: 640, marginTop: 12 }}>
        <h2>API keys, SSO, webhooks, secrets</h2>
        <p className="muted">
          These management surfaces are tracked in <code>PAAS-UI-DESIGN.md §8</code> and ship
          incrementally. The control plane already exposes the endpoints; the UI work is the
          form + table + confirm flow.
        </p>
      </div>
    </div>
  );
}
