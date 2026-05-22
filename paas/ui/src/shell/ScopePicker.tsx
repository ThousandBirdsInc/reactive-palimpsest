// Top-bar scope picker. Loads organizations / projects / environments from
// the control plane and renders selects. If the calls fail (no API yet, or
// the env doesn't exist), falls back to a free-text id input so the user
// can still drive the UI in local dev.

import { useEffect, useState } from "react";
import type { Environment, Organization, Project, Scope } from "../types";
import { PaasApi } from "../lib/api";
import { persistScope, useScope, useSetScope } from "../lib/scope";

export function ScopePicker() {
  const scope = useScope();
  const setScope = useSetScope();
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [envs, setEnvs] = useState<Environment[]>([]);

  // Load orgs once. Projects/envs reload when their parent changes.
  useEffect(() => {
    const api = new PaasApi(scope);
    api.listOrganizations().then(setOrgs).catch(() => setOrgs([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope.apiBase]);

  useEffect(() => {
    const api = new PaasApi(scope);
    api.listProjects().then(setProjects).catch(() => setProjects([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope.apiBase, scope.organizationId]);

  useEffect(() => {
    const api = new PaasApi(scope);
    api.listEnvironments().then(setEnvs).catch(() => setEnvs([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scope.apiBase, scope.projectId]);

  function apply(next: Scope) {
    persistScope(next);
    setScope(next);
  }

  return (
    <div className="scope-picker">
      <ScopeSelect
        label="org"
        value={scope.organizationId}
        items={orgs.map((o) => ({ id: o.organization_id, label: o.display_name ?? o.organization_id }))}
        onChange={(organizationId) => apply({ ...scope, organizationId })}
      />
      <span className="scope-sep">/</span>
      <ScopeSelect
        label="project"
        value={scope.projectId}
        items={projects.map((p) => ({ id: p.project_id, label: p.display_name ?? p.project_id }))}
        onChange={(projectId) => apply({ ...scope, projectId })}
      />
      <span className="scope-sep">/</span>
      <ScopeSelect
        label="env"
        value={scope.environmentId}
        items={envs.map((e) => ({ id: e.environment_id, label: e.display_name ?? e.environment_id }))}
        onChange={(environmentId) => apply({ ...scope, environmentId })}
      />
    </div>
  );
}

interface SelectProps {
  label: string;
  value: string;
  items: Array<{ id: string; label: string }>;
  onChange: (next: string) => void;
}

function ScopeSelect({ label, value, items, onChange }: SelectProps) {
  // If the API returned nothing, fall back to a text input so local dev
  // works against a control plane that may not have anything seeded yet.
  if (items.length === 0) {
    return (
      <label className="scope-field">
        <span className="scope-label">{label}</span>
        <input value={value} onChange={(e) => onChange(e.target.value)} aria-label={label} />
      </label>
    );
  }
  const known = items.some((item) => item.id === value);
  return (
    <label className="scope-field">
      <span className="scope-label">{label}</span>
      <select value={known ? value : ""} onChange={(e) => onChange(e.target.value)} aria-label={label}>
        {!known && (
          <option value="" disabled>
            {value || `select ${label}`}
          </option>
        )}
        {items.map((item) => (
          <option key={item.id} value={item.id}>
            {item.label}
          </option>
        ))}
      </select>
    </label>
  );
}
