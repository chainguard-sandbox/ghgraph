//! Transport: the `gh` CLI as a subprocess. gh owns auth, SSO, TLS, and host
//! selection; ghgraph carries no HTTP or TLS dependencies because of it.
//! `gh` is a documented runtime prerequisite.
//!
//! Invariants:
//!   * The GraphQL document goes to gh on stdin (`-F query=@-`) — argv size
//!     limits can never apply, regardless of query growth.
//!   * Environment hygiene: GH_PAGER is cleared and GH_PROMPT_DISABLED=1 so an
//!     unattended run can never block on a pager or prompt.
//!   * Subprocess contract (mechanism lands milestone 2): both pipes are
//!     drained concurrently (no pipe deadlock on multi-MB responses) and
//!     the child is always reaped — killed by a watchdog thread within a
//!     deadline, so no gh can wedge an unattended sync. Command::output()
//!     was the first design and was abandoned: it blocks forever on a
//!     stalled child and nothing inside a no-signal-handler process can
//!     unstick it. Kill-anytime safety rests on replay idempotence (a
//!     killed window's redo is a no-op); a mid-walk kill marks truncated,
//!     never sweeps — the completeness witness guarantees it.
//!   * Retry policy is owned here, bounded, and configured (fields land
//!     milestone 2): attempts per call, per-repo budget. Primary rate
//!     limits fold into the floor's defer-record-exit path — one budget,
//!     one mechanism. gh stderr is redacted for token shapes (gh[pousr]_…)
//!     before it reaches any envelope; the scrubber lands with the watchdog
//!     in milestone 2, beside the classification table it protects.
//!   * gh does not retry rate limits, and its exit code cannot distinguish
//!     them; the failure class is parsed from stderr:
//!
//! ```text
//! "secondary rate limit"      → TRANSIENT, backoff with jitter
//! "API rate limit exceeded"   → TRANSIENT, sleep toward resetAt
//! exit code 4                 → CONFIGURATION (gh auth login needed)
//! gh binary absent            → CONFIGURATION
//! anything else               → TRANSIENT with first ~1KB of stderr
//! ```
//!
//!   * An HTTP 200 whose body carries a top-level "errors" array is still a
//!     failure; the body parse decides, not the exit code alone.
//!   * Every query appends `rateLimit { cost remaining resetAt }` (costs 0);
//!     callers accumulate cost for the sync summary.
//!
//! The coupling is a seam, not a marriage: `graphql()` is the entire
//! transport surface, and nothing outside this module knows gh exists —
//! documents are strings, responses are Values, rate-limit data is in-band.
//! Swapping transports is a rewrite of this module behind this signature.
//! What would trigger it: post-batching telemetry showing subprocess
//! overhead still dominating sync wall time; gh breaking the `api graphql`
//! contract; a deployment context where gh cannot exist. The stderr table
//! above is heuristic, not contract — its default class is TRANSIENT, so
//! version skew degrades retry efficiency, never correctness.

use serde::Deserialize;

use crate::error::Result;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    pub cost: u32,
    pub remaining: u32,
    pub reset_at: String,
}

pub struct Response {
    pub data: serde_json::Value,
    pub rate_limit: Option<RateLimit>,
}

/// One GraphQL round trip. `vars` become string variables; typed variables
/// are not needed by any current query.
pub fn graphql(_query: &str, _vars: &[(&str, &str)]) -> Result<Response> {
    todo!("Command::new(\"gh\") api graphql, query on stdin, classify per module docs")
}
