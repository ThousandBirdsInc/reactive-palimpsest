// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resource bounds applied to inbound SQL.
//!
//! v1 enforces two limits, both at parse/lower time:
//!
//! * `max_input_bytes` — how big the SQL string can be. Stops a runaway
//!   client from forcing the parser to chew through megabytes of text.
//! * `max_mir_nodes` — how big the lowered MIR can be. Stops cleverly
//!   short queries (deep set-op chains, big CTE webs) from expanding
//!   into a graph the planner has to walk N² over.
//! * `max_nesting_depth` — how deeply parentheses may nest, checked
//!   with an O(n) scan before the parser runs, and also applied as the
//!   parser's own recursion limit. The parser backtracks across
//!   alternatives at every `(`, so unbounded nesting lets a few hundred
//!   bytes of input burn seconds of CPU (nightly fuzz timeouts on
//!   `sql_parser` and `named_query_registrar`).
//!
//! Both are advisory: callers explicitly invoke
//! [`enforce_input_size`] / [`enforce_graph_size`] (or use the
//! `*_with_limits` helpers in [`lower`](crate::lower)). The default
//! limits are deliberately generous enough for the conformance suite to
//! pass unmodified.

use crate::SqlError;

/// Resource bounds applied to inbound SQL, surfaced to the gRPC layer
/// so it can refuse oversized queries before parsing.
#[derive(Debug, Clone, Copy)]
pub struct QueryLimits {
    /// Maximum byte length of the SQL input.
    pub max_input_bytes: usize,
    /// Maximum node count in the lowered MIR.
    pub max_mir_nodes: usize,
    /// Maximum parenthesis nesting depth of the SQL input, also used
    /// as the parser's recursion limit.
    pub max_nesting_depth: usize,
}

impl QueryLimits {
    /// Default budget: 64 KiB of SQL, 256 MIR nodes, 32 levels of
    /// nesting. Set generously enough that real-world dashboards do
    /// not bump into them: the conformance suite's deepest query nests
    /// well under half the depth budget.
    pub const DEFAULT: Self = Self {
        max_input_bytes: 64 * 1024,
        max_mir_nodes: 256,
        max_nesting_depth: 32,
    };
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Returns [`SqlError::QueryTooLarge`] when `sql.len()` exceeds
/// `limits.max_input_bytes`.
///
/// # Errors
/// As above.
pub const fn enforce_input_size(sql: &str, limits: QueryLimits) -> Result<(), SqlError> {
    let len = sql.len();
    if len > limits.max_input_bytes {
        Err(SqlError::QueryTooLarge {
            len,
            limit: limits.max_input_bytes,
        })
    } else {
        Ok(())
    }
}

/// Returns [`SqlError::QueryTooDeep`] when parentheses in `sql` nest
/// deeper than `limits.max_nesting_depth`.
///
/// Parentheses inside string literals (`'…'`, with `''` escapes),
/// quoted identifiers (`"…"`), dollar-quoted strings (`$$…$$`,
/// `$tag$…$tag$`), and comments (`-- …`, `/* … */`) do not count, so
/// a text column full of brackets cannot trip the limit. Unbalanced
/// closers are ignored here; the parser reports them.
///
/// # Errors
/// As above.
pub fn enforce_nesting_depth(sql: &str, limits: QueryLimits) -> Result<(), SqlError> {
    let depth = max_paren_depth(sql);
    if depth > limits.max_nesting_depth {
        Err(SqlError::QueryTooDeep {
            depth,
            limit: limits.max_nesting_depth,
        })
    } else {
        Ok(())
    }
}

/// Deepest parenthesis nesting outside literals and comments.
fn max_paren_depth(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut max = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                max = max.max(depth);
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'\'' => i = skip_quoted(bytes, i, b'\''),
            b'"' => i = skip_quoted(bytes, i, b'"'),
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = bytes[i..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(bytes.len(), |n| i + n + 1);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = find(bytes, i + 2, b"*/").map_or(bytes.len(), |n| n + 2);
            }
            b'$' => i = skip_dollar_quoted(bytes, i),
            _ => i += 1,
        }
    }
    max
}

/// Skips a `'…'` or `"…"` literal starting at `start`, honouring
/// doubled-quote escapes. Returns the index just past the closer, or
/// the end of input when unterminated.
fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Skips a `$tag$…$tag$` dollar-quoted string starting at `start`
/// when one is present; otherwise steps over the `$` (a positional
/// parameter such as `$1`).
fn skip_dollar_quoted(bytes: &[u8], start: usize) -> usize {
    let mut end = start + 1;
    while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        end += 1;
    }
    if bytes.get(end) != Some(&b'$')
        || bytes[start + 1..end]
            .first()
            .is_some_and(u8::is_ascii_digit)
    {
        return start + 1;
    }
    let tag = &bytes[start..=end];
    find(bytes, end + 1, tag).map_or(bytes.len(), |n| n + tag.len())
}

fn find(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|n| from + n)
}

/// Returns [`SqlError::QueryTooComplex`] when `nodes` exceeds
/// `limits.max_mir_nodes`.
///
/// # Errors
/// As above.
pub const fn enforce_graph_size(nodes: usize, limits: QueryLimits) -> Result<(), SqlError> {
    if nodes > limits.max_mir_nodes {
        Err(SqlError::QueryTooComplex {
            nodes,
            limit: limits.max_mir_nodes,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        enforce_graph_size, enforce_input_size, enforce_nesting_depth, max_paren_depth, QueryLimits,
    };
    use crate::SqlError;

    #[test]
    fn input_at_limit_passes() {
        let limits = QueryLimits {
            max_input_bytes: 4,
            max_mir_nodes: 8,
            ..QueryLimits::DEFAULT
        };
        assert!(enforce_input_size("abcd", limits).is_ok());
    }

    #[test]
    fn input_above_limit_rejects() {
        let limits = QueryLimits {
            max_input_bytes: 3,
            max_mir_nodes: 8,
            ..QueryLimits::DEFAULT
        };
        match enforce_input_size("abcd", limits) {
            Err(SqlError::QueryTooLarge { len: 4, limit: 3 }) => {}
            other => panic!("expected QueryTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn graph_above_limit_rejects() {
        let limits = QueryLimits {
            max_input_bytes: 1024,
            max_mir_nodes: 5,
            ..QueryLimits::DEFAULT
        };
        match enforce_graph_size(10, limits) {
            Err(SqlError::QueryTooComplex {
                nodes: 10,
                limit: 5,
            }) => {}
            other => panic!("expected QueryTooComplex, got {other:?}"),
        }
    }

    #[test]
    fn paren_depth_counts_nesting_not_total() {
        assert_eq!(max_paren_depth("SELECT id FROM t"), 0);
        assert_eq!(max_paren_depth("SELECT (1), (2), (3) FROM t"), 1);
        assert_eq!(
            max_paren_depth("SELECT ((1)) FROM (SELECT id FROM (SELECT 1) a) b"),
            2
        );
        assert_eq!(max_paren_depth("SELECT id FROM ((((x"), 4);
    }

    #[test]
    fn paren_depth_ignores_literals_and_comments() {
        assert_eq!(max_paren_depth("SELECT '((((' FROM t"), 0);
        assert_eq!(max_paren_depth("SELECT 'it''s ((' FROM t"), 0);
        assert_eq!(max_paren_depth(r#"SELECT "we(ird(" FROM t"#), 0);
        assert_eq!(
            max_paren_depth(
                "SELECT $$((((((
$$ FROM t"
            ),
            0
        );
        assert_eq!(max_paren_depth("SELECT $q$(((($q$ FROM t"), 0);
        assert_eq!(
            max_paren_depth(
                "SELECT id -- ((((
FROM t"
            ),
            0
        );
        assert_eq!(
            max_paren_depth(
                "SELECT id /* ((((
(( */ FROM t"
            ),
            0
        );
        // Positional parameters are not dollar quotes.
        assert_eq!(
            max_paren_depth("SELECT id FROM t WHERE id = $1 AND (x = $2)"),
            1
        );
        // Unterminated literal: nothing after it counts.
        assert_eq!(max_paren_depth("SELECT '(((("), 0);
    }

    #[test]
    fn nesting_above_limit_rejects() {
        let limits = QueryLimits {
            max_nesting_depth: 3,
            ..QueryLimits::DEFAULT
        };
        assert!(enforce_nesting_depth("SELECT id FROM (((x)))", limits).is_ok());
        match enforce_nesting_depth("SELECT id FROM ((((x))))", limits) {
            Err(SqlError::QueryTooDeep { depth: 4, limit: 3 }) => {}
            other => panic!("expected QueryTooDeep, got {other:?}"),
        }
    }
}
