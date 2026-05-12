// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Resume-LSN handling.
//!
//! Reconnecting clients hand back the LSN of the last diff they
//! applied; the router decides whether to (a) replay diffs from that
//! LSN forward — possible only if the trace's compaction frontier has
//! not advanced past it — or (b) issue a fresh `Initial`.
//!
//! The decision is captured by [`ResumeDecision`] so the router orchestrator
//! can branch without scattering equality checks.

use palimpsest_dataflow::palimpsest::Lsn;

use crate::diff::ResyncReason;

/// Range `[earliest_replayable, latest_known]` of LSNs the trace can
/// still produce diffs for.
///
/// `earliest_replayable` is the trace's logical compaction frontier;
/// any LSN strictly less than this value has been collapsed and cannot
/// be replayed verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionWindow {
    /// Inclusive lower bound; LSN < `earliest_replayable` triggers a
    /// fresh `Initial`.
    pub earliest_replayable: Lsn,
    /// Inclusive upper bound; the router won't be asked for LSNs above
    /// this until the trace observes them.
    pub latest_known: Lsn,
}

impl CompactionWindow {
    /// Builds a window. Caller is responsible for ensuring
    /// `earliest_replayable <= latest_known`.
    #[must_use]
    pub const fn new(earliest_replayable: Lsn, latest_known: Lsn) -> Self {
        Self {
            earliest_replayable,
            latest_known,
        }
    }

    /// Returns true when `lsn` is inside the replayable window
    /// (inclusive of `earliest_replayable`).
    #[must_use]
    pub fn contains(&self, lsn: Lsn) -> bool {
        lsn >= self.earliest_replayable
    }
}

/// What the router should do for a given resume request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeDecision {
    /// Send a fresh `Initial`; the client will discard their stale
    /// state.
    FreshInitial {
        /// Why a fresh `Initial` is required.
        reason: ResyncReason,
    },
    /// Replay diffs from `from_lsn` forward (exclusive).
    Replay {
        /// First LSN the client has not yet acked.
        from_lsn: Lsn,
    },
}

/// Resolves a resume request against the trace's compaction window.
///
/// `last_acked_lsn` is the LSN the client claims to have applied. If
/// it is `None` (a brand-new subscription), the router always issues
/// `Initial`.
#[must_use]
pub fn resolve(last_acked_lsn: Option<Lsn>, window: CompactionWindow) -> ResumeDecision {
    let Some(last) = last_acked_lsn else {
        return ResumeDecision::FreshInitial {
            reason: ResyncReason::LsnCompacted,
        };
    };
    if window.contains(last) {
        ResumeDecision::Replay { from_lsn: last }
    } else {
        ResumeDecision::FreshInitial {
            reason: ResyncReason::LsnCompacted,
        }
    }
}

#[cfg(test)]
mod tests {
    use palimpsest_dataflow::palimpsest::Lsn;

    use super::{resolve, CompactionWindow, ResumeDecision};
    use crate::diff::ResyncReason;

    #[test]
    fn lsn_inside_window_is_replayable() {
        let window = CompactionWindow::new(Lsn::new(10), Lsn::new(50));
        assert_eq!(
            resolve(Some(Lsn::new(20)), window),
            ResumeDecision::Replay {
                from_lsn: Lsn::new(20),
            }
        );
    }

    #[test]
    fn lsn_at_lower_bound_is_replayable() {
        let window = CompactionWindow::new(Lsn::new(10), Lsn::new(50));
        assert_eq!(
            resolve(Some(Lsn::new(10)), window),
            ResumeDecision::Replay {
                from_lsn: Lsn::new(10),
            }
        );
    }

    #[test]
    fn lsn_below_compaction_frontier_triggers_initial() {
        let window = CompactionWindow::new(Lsn::new(10), Lsn::new(50));
        assert_eq!(
            resolve(Some(Lsn::new(5)), window),
            ResumeDecision::FreshInitial {
                reason: ResyncReason::LsnCompacted,
            }
        );
    }

    #[test]
    fn missing_resume_lsn_triggers_initial() {
        let window = CompactionWindow::new(Lsn::new(10), Lsn::new(50));
        assert_eq!(
            resolve(None, window),
            ResumeDecision::FreshInitial {
                reason: ResyncReason::LsnCompacted,
            }
        );
    }
}
