import { createContext, useContext, useMemo } from "react";
import type { Scope } from "../types";
import { PaasApi } from "./api";

export const DEFAULT_SCOPE: Scope = {
  apiBase: import.meta.env.VITE_PAAS_API_BASE ?? "/api",
  organizationId: "org_123",
  projectId: "project_123",
  environmentId: "env_123",
  actorId: "paas-ui-local",
};

interface ScopeContextValue {
  scope: Scope;
  setScope: (next: Scope) => void;
}

export const ScopeContext = createContext<ScopeContextValue>({
  scope: DEFAULT_SCOPE,
  setScope: () => {
    /* no-op default */
  },
});

export function useScope(): Scope {
  return useContext(ScopeContext).scope;
}

export function useSetScope(): (next: Scope) => void {
  return useContext(ScopeContext).setScope;
}

export function useApi(): PaasApi {
  const scope = useScope();
  return useMemo(() => new PaasApi(scope), [scope]);
}

const SCOPE_STORAGE_KEY = "palimpsest-paas-ui-scope";

export function loadScope(): Scope {
  if (typeof window === "undefined") return DEFAULT_SCOPE;
  const raw = window.localStorage.getItem(SCOPE_STORAGE_KEY);
  if (!raw) return DEFAULT_SCOPE;
  try {
    return { ...DEFAULT_SCOPE, ...(JSON.parse(raw) as Partial<Scope>) };
  } catch {
    return DEFAULT_SCOPE;
  }
}

export function persistScope(scope: Scope): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(SCOPE_STORAGE_KEY, JSON.stringify(scope));
}
