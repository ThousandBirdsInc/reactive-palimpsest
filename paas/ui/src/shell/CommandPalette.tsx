// Minimal ⌘K / ctrl+K palette. Lists nav targets + opens cluster detail
// pages by id. Intentionally not search-as-you-type — flat list, arrow
// keys, enter. Keep it boring; expand later if it earns its keep.

import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { NAV_ITEMS } from "./nav";

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const isMod = e.metaKey || e.ctrlKey;
      if (isMod && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((v) => !v);
        setQuery("");
        setSelected(0);
      } else if (e.key === "Escape" && open) {
        setOpen(false);
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const items = useMemo(() => {
    const q = query.trim().toLowerCase();
    const base = NAV_ITEMS.map((n) => ({
      label: `Go to ${n.label}`,
      to: n.to,
    }));
    if (q.startsWith("cluster ") || q.startsWith("c ")) {
      const id = q.split(/\s+/)[1] ?? "";
      if (id) base.unshift({ label: `Open cluster ${id}`, to: `/clusters/${id}` });
    }
    if (!q) return base;
    return base.filter((it) => it.label.toLowerCase().includes(q));
  }, [query]);

  useEffect(() => {
    if (selected >= items.length) setSelected(0);
  }, [items, selected]);

  if (!open) return null;
  return (
    <div className="palette-overlay" onMouseDown={(e) => e.target === e.currentTarget && setOpen(false)}>
      <div className="palette" role="dialog" aria-label="Command palette">
        <input
          ref={inputRef}
          className="palette-input"
          value={query}
          placeholder="Jump to… (try 'cluster cluster_abc')"
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") {
              e.preventDefault();
              setSelected((s) => Math.min(items.length - 1, s + 1));
            } else if (e.key === "ArrowUp") {
              e.preventDefault();
              setSelected((s) => Math.max(0, s - 1));
            } else if (e.key === "Enter") {
              const item = items[selected];
              if (item) {
                navigate(item.to);
                setOpen(false);
              }
            }
          }}
        />
        <ul className="palette-list">
          {items.length === 0 && <li className="palette-empty">No matches</li>}
          {items.map((it, i) => (
            <li
              key={it.to + it.label}
              className={i === selected ? "palette-item selected" : "palette-item"}
              onMouseEnter={() => setSelected(i)}
              onMouseDown={(e) => {
                e.preventDefault();
                navigate(it.to);
                setOpen(false);
              }}
            >
              {it.label}
              <span className="palette-route">{it.to}</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
