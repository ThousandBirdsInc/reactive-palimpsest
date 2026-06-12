// Tiny dependency-free syntax highlighters used by the overlay code editor.
//
// Each function turns source text into an HTML string of <span class="tok-*">
// tokens that a <pre> renders underneath a transparent <textarea>. The output
// MUST preserve every input character (including whitespace) verbatim so the
// highlighted layer stays glyph-for-glyph aligned with the textarea.

function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function span(kind: string, innerHtml: string): string {
  return `<span class="tok-${kind}">${innerHtml}</span>`;
}

// ----- TOML (permissions DSL) -----

export function highlightToml(src: string): string {
  return src.split("\n").map(highlightTomlLine).join("\n");
}

function highlightTomlLine(line: string): string {
  // A `#` only starts a comment when it is outside a double-quoted string.
  let inString = false;
  let commentIdx = -1;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') inString = !inString;
    else if (ch === "#" && !inString) {
      commentIdx = i;
      break;
    }
  }
  const code = commentIdx >= 0 ? line.slice(0, commentIdx) : line;
  const comment = commentIdx >= 0 ? line.slice(commentIdx) : "";
  let html = highlightTomlCode(code);
  if (comment) html += span("comment", escapeHtml(comment));
  return html;
}

function highlightTomlCode(code: string): string {
  if (code.trimStart().startsWith("[")) {
    // Table / array-of-tables header, e.g. [[rule]] or [section].
    return span("section", escapeHtml(code));
  }
  const eq = code.indexOf("=");
  if (eq >= 0) {
    return (
      span("key", escapeHtml(code.slice(0, eq))) +
      span("op", "=") +
      highlightTomlValue(code.slice(eq + 1))
    );
  }
  return escapeHtml(code);
}

function highlightTomlValue(value: string): string {
  let out = "";
  let i = 0;
  while (i < value.length) {
    if (value[i] === '"') {
      let j = i + 1;
      while (j < value.length && value[j] !== '"') j += 1;
      const end = j < value.length ? j + 1 : value.length;
      out += span("string", highlightUserRefs(value.slice(i, end)));
      i = end;
    } else {
      let j = i;
      while (j < value.length && value[j] !== '"') j += 1;
      out += highlightTomlScalars(value.slice(i, j));
      i = j;
    }
  }
  return out;
}

function highlightTomlScalars(text: string): string {
  return escapeHtml(text)
    .replace(/\b(?:true|false)\b/g, (m) => span("bool", m))
    .replace(/\b-?\d+(?:\.\d+)?\b/g, (m) => span("number", m));
}

// Inside a permission predicate, `$user.<field>` references are the bridge to
// the request context, so call them out distinctly.
function highlightUserRefs(quoted: string): string {
  return escapeHtml(quoted).replace(/\$user\.[A-Za-z_][A-Za-z0-9_]*/g, (m) => span("user", m));
}

// ----- SQL -----

const SQL_KEYWORDS = new Set([
  "select", "from", "where", "join", "left", "right", "inner", "full", "outer",
  "cross", "on", "using", "group", "by", "order", "having", "limit", "offset",
  "with", "as", "distinct", "and", "or", "not", "null", "is", "between", "in",
  "like", "ilike", "exists", "case", "when", "then", "else", "end", "true",
  "false", "asc", "desc", "union", "all", "insert", "into", "values", "update",
  "set", "delete", "returning", "set", "transaction", "read", "only",
]);

const SQL_FUNCTIONS = new Set([
  "avg", "count", "current_database", "current_date", "current_timestamp",
  "current_user", "date_trunc", "jsonb_agg", "jsonb_build_object", "lower",
  "max", "min", "now", "sum", "to_char", "upper", "version", "coalesce",
]);

const DIGIT = /[0-9]/;
const NUMERIC = /[0-9.]/;
const IDENT_START = /[A-Za-z_]/;
const IDENT_CHAR = /[A-Za-z0-9_]/;

export function highlightSql(src: string): string {
  let out = "";
  let i = 0;
  const n = src.length;
  while (i < n) {
    const ch = src[i];
    const two = src.slice(i, i + 2);
    if (two === "--") {
      let j = i;
      while (j < n && src[j] !== "\n") j += 1;
      out += span("comment", escapeHtml(src.slice(i, j)));
      i = j;
    } else if (two === "/*") {
      let j = i + 2;
      while (j < n && src.slice(j, j + 2) !== "*/") j += 1;
      const end = j < n ? j + 2 : n;
      out += span("comment", escapeHtml(src.slice(i, end)));
      i = end;
    } else if (ch === "'") {
      let j = i + 1;
      while (j < n) {
        if (src[j] === "'" && src[j + 1] === "'") {
          j += 2;
          continue;
        }
        if (src[j] === "'") break;
        j += 1;
      }
      const end = j < n ? j + 1 : n;
      out += span("string", escapeHtml(src.slice(i, end)));
      i = end;
    } else if (ch === '"') {
      let j = i + 1;
      while (j < n && src[j] !== '"') j += 1;
      const end = j < n ? j + 1 : n;
      out += span("ident", escapeHtml(src.slice(i, end)));
      i = end;
    } else if (DIGIT.test(ch)) {
      let j = i;
      while (j < n && NUMERIC.test(src[j])) j += 1;
      out += span("number", escapeHtml(src.slice(i, j)));
      i = j;
    } else if (IDENT_START.test(ch)) {
      let j = i;
      while (j < n && IDENT_CHAR.test(src[j])) j += 1;
      const word = src.slice(i, j);
      const lower = word.toLowerCase();
      if (SQL_KEYWORDS.has(lower)) out += span("keyword", escapeHtml(word));
      else if (SQL_FUNCTIONS.has(lower)) out += span("function", escapeHtml(word));
      else out += escapeHtml(word);
      i = j;
    } else {
      out += escapeHtml(ch);
      i += 1;
    }
  }
  return out;
}
