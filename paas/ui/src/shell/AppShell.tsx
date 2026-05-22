import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";
import { CircleDot } from "lucide-react";
import { CommandPalette } from "./CommandPalette";
import { ScopePicker } from "./ScopePicker";
import { NAV_ITEMS } from "./nav";
import { useScope } from "../lib/scope";

export function AppShell() {
  const scope = useScope();
  const [shortcut] = useState(() => (isMac() ? "⌘K" : "Ctrl+K"));

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">◆</span>
          <span className="brand-name">Palimpsest</span>
          <span className="brand-sub">PaaS</span>
        </div>
        <ScopePicker />
        <div className="topbar-right">
          <ConnectionPill />
          <kbd className="kbd">{shortcut}</kbd>
          <span className="actor" title={`actor=${scope.actorId}`}>
            {scope.actorId}
          </span>
        </div>
      </header>

      <aside className="sidebar">
        <nav>
          <ul>
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;
              return (
                <li key={item.to}>
                  <NavLink
                    to={item.to}
                    end={item.end}
                    className={({ isActive }) => (isActive ? "nav-link active" : "nav-link")}
                  >
                    <Icon size={15} aria-hidden="true" />
                    <span>{item.label}</span>
                  </NavLink>
                </li>
              );
            })}
          </ul>
        </nav>
      </aside>

      <main className="content">
        <Outlet />
      </main>

      <CommandPalette />
    </div>
  );
}

function ConnectionPill() {
  // Hard-coded "polling" today. Once PalimpsestLiveSource lights up, this
  // should read from the live-source registry instead.
  return (
    <span className="conn-pill conn-warn" title="Live updates: polling REST every 10s">
      <CircleDot size={10} aria-hidden="true" /> polling
    </span>
  );
}

function isMac(): boolean {
  if (typeof navigator === "undefined") return false;
  return /Mac|iPhone|iPad/.test(navigator.platform);
}
