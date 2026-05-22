// Renders connection parameters + four canonical connection strings for
// one (cluster, database, role) tuple. Tooltip-free copy buttons go on
// each string. Passwords are never exposed by the control plane — we
// emit `<password>` placeholders and direct users to the Roles tab.

import { ReactNode, useState } from "react";
import {
  AlertTriangle,
  Check,
  Copy,
  KeyRound,
  Server,
  ShieldCheck,
  ShieldOff,
} from "lucide-react";
import {
  buildJdbcUrl,
  buildKeywordDsn,
  buildLibpqUri,
  buildPsqlCommand,
  ConnectionParams,
} from "../lib/connection";

interface Props {
  params: ConnectionParams | null;
  /** Why params are null. Shown in place of the table when no params. */
  reason?: string | null;
  loading?: boolean;
  /** Optional label above the panel, e.g. "Connection · postgres". */
  label?: ReactNode;
  /** Hide the "where to find the password" hint (e.g. on roles tab). */
  hidePasswordHint?: boolean;
}

export function ConnectionInfo({ params, reason, loading, label, hidePasswordHint }: Props) {
  return (
    <div className="conn-panel">
      {label && <div className="conn-panel-label">{label}</div>}
      {!params ? (
        <div className="conn-panel-empty">
          {loading ? "Loading connection info…" : reason ?? "No connection info available."}
        </div>
      ) : (
        <>
          {params.source === "direct-backend" && (
            <div className="conn-direct-warn">
              <AlertTriangle size={11} aria-hidden="true" />
              <span>
                <strong>Dev fallback.</strong> No public database-proxy route is published
                for this cluster yet, so this string targets the backend port directly on{" "}
                <span className="mono">{params.host}</span>. In production clients should
                connect via the proxy listen address with TLS.
              </span>
            </div>
          )}
          <ConnectionParamsTable params={params} />
          <ConnectionStrings params={params} />
          {!hidePasswordHint && (
            <p className="conn-password-note">
              <KeyRound size={11} aria-hidden="true" /> Passwords are issued by the control plane
              and never returned over the API. Rotate via the <strong>Roles</strong> tab and read
              the new secret from your secret store.
            </p>
          )}
        </>
      )}
    </div>
  );
}

function ConnectionParamsTable({ params }: { params: ConnectionParams }) {
  return (
    <dl className="conn-params">
      <ConnRow label="Host" value={params.host} icon={<Server size={11} />} mono />
      <ConnRow label="Port" value={String(params.port)} mono />
      <ConnRow label="Database" value={params.database} mono />
      <ConnRow label="User" value={params.username} mono />
      <ConnRow
        label="SSL"
        value={params.sslmode}
        icon={
          params.sslmode === "disable" ? (
            <ShieldOff size={11} aria-hidden="true" />
          ) : (
            <ShieldCheck size={11} aria-hidden="true" />
          )
        }
      />
      <ConnRow label="Route source" value={params.source} mono muted />
    </dl>
  );
}

function ConnRow({
  label,
  value,
  icon,
  mono,
  muted,
}: {
  label: string;
  value: string;
  icon?: ReactNode;
  mono?: boolean;
  muted?: boolean;
}) {
  return (
    <>
      <dt>{label}</dt>
      <dd className={`${mono ? "mono" : ""} ${muted ? "muted" : ""}`.trim()}>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          {icon}
          <span>{value}</span>
          {!muted && (
            <CopyChip text={value} ariaLabel={`Copy ${label.toLowerCase()}`} />
          )}
        </span>
      </dd>
    </>
  );
}

function ConnectionStrings({ params }: { params: ConnectionParams }) {
  return (
    <div className="conn-strings">
      <ConnStringRow label="psql" value={buildPsqlCommand(params)} />
      <ConnStringRow label="URI" value={buildLibpqUri(params)} />
      <ConnStringRow label="DSN" value={buildKeywordDsn(params)} />
      <ConnStringRow label="JDBC" value={buildJdbcUrl(params)} />
    </div>
  );
}

function ConnStringRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="conn-string-row">
      <span className="conn-string-label">{label}</span>
      <code className="conn-string-value mono">{value}</code>
      <CopyChip text={value} ariaLabel={`Copy ${label} connection string`} />
    </div>
  );
}

function CopyChip({ text, ariaLabel }: { text: string; ariaLabel: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="conn-copy"
      aria-label={ariaLabel}
      title={ariaLabel}
      onClick={() => {
        if (typeof navigator === "undefined" || !navigator.clipboard) return;
        void navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1200);
        });
      }}
    >
      {copied ? <Check size={11} /> : <Copy size={11} />}
    </button>
  );
}
