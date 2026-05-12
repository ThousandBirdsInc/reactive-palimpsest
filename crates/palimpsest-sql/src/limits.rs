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
}

impl QueryLimits {
    /// Default budget: 64 KiB of SQL, 256 MIR nodes. Set generously
    /// enough that real-world dashboards do not bump into them.
    pub const DEFAULT: Self = Self {
        max_input_bytes: 64 * 1024,
        max_mir_nodes: 256,
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
    use super::{enforce_graph_size, enforce_input_size, QueryLimits};
    use crate::SqlError;

    #[test]
    fn input_at_limit_passes() {
        let limits = QueryLimits {
            max_input_bytes: 4,
            max_mir_nodes: 8,
        };
        assert!(enforce_input_size("abcd", limits).is_ok());
    }

    #[test]
    fn input_above_limit_rejects() {
        let limits = QueryLimits {
            max_input_bytes: 3,
            max_mir_nodes: 8,
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
        };
        match enforce_graph_size(10, limits) {
            Err(SqlError::QueryTooComplex {
                nodes: 10,
                limit: 5,
            }) => {}
            other => panic!("expected QueryTooComplex, got {other:?}"),
        }
    }
}
