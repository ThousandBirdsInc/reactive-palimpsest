import { lazy, StrictMode, Suspense, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import BoardPage from "./BoardPage";
import { PersonaBar, SessionProvider, SimulatorControl } from "./session";

// Three pages behind a dependency-free hash router:
//   #/            — the board (Linear-style kanban, default)
//   #/analytics   — live aggregate dashboards
//   #/local-first — the local-first replica demo (pglite mirror,
//                   optimistic writes, WAL reconciliation)
type Route = "board" | "analytics" | "local-first";

// Lazy: analytics is chart-heavy and the local-first page pulls in
// pglite (a full Postgres compiled to WASM); keep both off the board
// route's critical path.
const AnalyticsPage = lazy(() => import("./AnalyticsPage"));
const LocalFirstPage = lazy(() => import("./LocalFirstPage"));

function routeFromHash(): Route {
  if (window.location.hash.startsWith("#/local-first")) return "local-first";
  if (window.location.hash.startsWith("#/analytics")) return "analytics";
  return "board";
}

function useHashRoute(): Route {
  const [route, setRoute] = useState<Route>(routeFromHash);
  useEffect(() => {
    const onChange = () => setRoute(routeFromHash());
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return route;
}

const TABS: { route: Route; href: string; label: string }[] = [
  { route: "board", href: "#/", label: "Board" },
  { route: "analytics", href: "#/analytics", label: "Analytics" },
  { route: "local-first", href: "#/local-first", label: "Local-first" },
];

function Root() {
  const route = useHashRoute();
  return (
    <SessionProvider>
      <nav className="top-nav" aria-label="Pages">
        {TABS.map((tab) => (
          <a
            key={tab.route}
            href={tab.href}
            className={route === tab.route ? "top-nav-active" : ""}
          >
            {tab.label}
          </a>
        ))}
        <div className="top-nav-spacer" />
        <SimulatorControl />
      </nav>
      <PersonaBar />
      {route === "board" && <BoardPage />}
      {route === "analytics" && (
        <Suspense fallback={<p className="empty">Loading analytics…</p>}>
          <AnalyticsPage />
        </Suspense>
      )}
      {route === "local-first" && (
        <Suspense fallback={<p className="empty">Loading local engine…</p>}>
          <LocalFirstPage />
        </Suspense>
      )}
    </SessionProvider>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
