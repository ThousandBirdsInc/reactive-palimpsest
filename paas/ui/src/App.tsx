import { useCallback, useState } from "react";
import { Route, Routes } from "react-router-dom";
import { AppShell } from "./shell/AppShell";
import { ScopeContext, loadScope, persistScope } from "./lib/scope";
import type { Scope } from "./types";
import { Overview } from "./pages/Overview";
import { ClustersList } from "./pages/ClustersList";
import { ClusterDetail } from "./pages/ClusterDetail";
import { Hosts } from "./pages/Hosts";
import { Incidents } from "./pages/Incidents";
import { Quota } from "./pages/Quota";
import { Routes as RoutesPage } from "./pages/Routes";
import { Audit } from "./pages/Audit";
import { SettingsPage } from "./pages/Settings";

export function App() {
  const [scope, setScopeState] = useState<Scope>(() => loadScope());
  const setScope = useCallback((next: Scope) => {
    persistScope(next);
    setScopeState(next);
  }, []);

  return (
    <ScopeContext.Provider value={{ scope, setScope }}>
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<Overview />} />
          <Route path="clusters" element={<ClustersList />} />
          <Route path="clusters/:clusterId/*" element={<ClusterDetail />} />
          <Route path="hosts" element={<Hosts />} />
          <Route path="incidents" element={<Incidents />} />
          <Route path="quota" element={<Quota />} />
          <Route path="routes" element={<RoutesPage />} />
          <Route path="audit" element={<Audit />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
      </Routes>
    </ScopeContext.Provider>
  );
}
