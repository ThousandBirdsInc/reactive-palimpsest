import { useState } from "react";
import { usePalimpsestSubscription } from "./usePalimpsestSubscription";

const DEFAULT_URL = "http://127.0.0.1:50052";
const DEFAULT_SQL = "SELECT id, title FROM posts";

export default function App() {
  const [url, setUrl] = useState(DEFAULT_URL);
  const [sql, setSql] = useState(DEFAULT_SQL);
  const { status, rows, schema, error } = usePalimpsestSubscription(url, sql);

  const columns = schema?.columns.map((c) => c.name) ?? [];

  return (
    <div style={{ fontFamily: "monospace", padding: 24, maxWidth: 900 }}>
      <h1>Palimpsest live query</h1>

      <p>
        <a href="https://github.com/thousandbirds/palimpsest">
          palimpsest-client-js
        </a>{" "}
        + React. Edit a row in Postgres and watch this table refresh.
      </p>

      <fieldset style={{ marginBottom: 16 }}>
        <legend>connection</legend>
        <div style={{ display: "flex", gap: 8 }}>
          <label style={{ flex: 1 }}>
            URL{" "}
            <input
              style={{ width: "100%" }}
              value={url}
              onChange={(e) => setUrl(e.target.value)}
            />
          </label>
        </div>
        <div style={{ marginTop: 8 }}>
          <label>
            SQL{" "}
            <input
              style={{ width: "100%" }}
              value={sql}
              onChange={(e) => setSql(e.target.value)}
            />
          </label>
        </div>
      </fieldset>

      <p>
        status: <strong>{status}</strong>
        {error && <span style={{ color: "crimson" }}> — {error.message}</span>}
      </p>

      {schema && (
        <table border={1} cellPadding={6} style={{ borderCollapse: "collapse" }}>
          <thead>
            <tr>
              {columns.map((c) => (
                <th key={c}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i}>
                {row.map((cell, j) => (
                  <td key={j}>{String(cell)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {rows.length === 0 && status === "open" && (
        <p>(empty result set)</p>
      )}
    </div>
  );
}
