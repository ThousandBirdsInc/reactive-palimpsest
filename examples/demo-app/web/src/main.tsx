import { lazy, StrictMode, Suspense, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

// Lazy: the local-first page pulls in pglite (a full Postgres compiled
// to WASM); keep that ~10 MB of assets off the live-queries route.
const LocalFirstPage = lazy(() => import("./LocalFirstPage"));

// Two demo pages behind a dependency-free hash router:
//   #/            — the live-subscription demo (default)
//   #/local-first — the local-first replica demo (pglite mirror,
//                   optimistic writes, WAL reconciliation)
type Route = "live" | "local-first";

function routeFromHash(): Route {
  return window.location.hash.startsWith("#/local-first")
    ? "local-first"
    : "live";
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

function Root() {
  const route = useHashRoute();
  return (
    <>
      <nav className="top-nav" aria-label="Demo pages">
        <a href="#/" className={route === "live" ? "top-nav-active" : ""}>
          Live queries
        </a>
        <a
          href="#/local-first"
          className={route === "local-first" ? "top-nav-active" : ""}
        >
          Local-first replica
        </a>
      </nav>
      {route === "local-first" ? (
        <Suspense fallback={<p className="empty">Loading local engine…</p>}>
          <LocalFirstPage />
        </Suspense>
      ) : (
        <App />
      )}
    </>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
