//! The sync pipeline.
//!
//! Shape: K worker threads (config.workers, default 3) pull repos from a
//! shared job list ordered by the scheduler (starved-first; see below), run
//! discovery + hydration through gh, and stream typed messages to the
//! single writer thread — the only thread that ever touches the write
//! connection.
//!
//!     workers ── sync_channel(16) ──▶ writer (owns the Connection)
//!
//! Messages (see Msg): Page for intermediate rows; Done for one COMPLETED
//! discovery window — its rows, its quarantine records, and the watermark
//! advance in one message, one transaction, both-or-neither, so state cannot
//! lead data and a quarantined id is passed only by the record that
//! resurfaces it; Deferred when the rate-limit floor stops a stream, typed
//! so the summary never string-sniffs; Failed is recorded in the summary,
//! never carries a watermark, and holds the watermark at the last completed
//! window boundary. Windows are processed oldest-first and hydration
//! ascends by updatedAt, so the hydrated prefix is contiguous, a window
//! boundary is a sound checkpoint, two consecutive floor-deferred runs never
//! re-hydrate the same PR, and an item bumped mid-walk moves toward the
//! unvisited end — seen twice and deduped, never skipped.
//!
//! Invariants, in order of importance:
//!
//!   * The watermark is server-side time — max updatedAt actually ingested —
//!     never the local clock (clock skew and search-index lag both lose
//!     updates otherwise). Queries overlap the watermark by ~10 minutes;
//!     upserts make the overlap free.
//!
//!   * sync_state advances only on Done: state never leads data. This is also
//!     the entire cancellation story — SIGINT kills the process group (gh
//!     children included), SQLite rolls back the open transaction on next
//!     open, and the next run redoes the window. No signal handler, on
//!     purpose.
//!
//!   * Mark-and-sweep applies soft deletes (deleted_at) to comments/threads
//!     absent from a COMPLETELY fetched connection — never from a truncated
//!     one, or truncation reads as deletion. Deleted rows are kept: upstream
//!     deletion is signal for a memory tool.
//!
//!   * Upserts: ON CONFLICT(repo, number) for prs (node ids are data, not
//!     identity), ON CONFLICT(id) elsewhere; never INSERT OR REPLACE (rowid
//!     churn breaks FTS). The field diff each upsert computes is recorded to
//!     observations — that diff is the "what changed since yesterday" answer.
//!
//!   * Workers return Result on every fallible path (no unwrap on external
//!     data); a panic is a ghgraph bug and crashes loudly. No catch_unwind.
//!
//!   * The writer's original Sender is dropped before the recv loop, or the
//!     loop never terminates.
//!
//!   * Every discovered id resolves to exactly one outcome — hydrated,
//!     filtered, quarantined, or deferred. The stream watermark is
//!     max(updatedAt) over hydrated ∪ filtered (filtered is declined, not
//!     unfetched — a bot-only window must still advance, or re-discovery
//!     grows without bound); a deferred id caps the watermark below it; a
//!     quarantined id is passed only with its quarantine row in the same
//!     transaction.
//!
//!   * Re-verify (tiered, or --full) gives a PR a complete refetch to catch
//!     quiet mutations — comment edits and deletions, thread resolves — that
//!     never bump updatedAt past the watermark: open PRs every
//!     reverify_open_days REGARDLESS of lookback (OPEN is the relevance
//!     signal, not recency — otherwise a quiet open PR closed upstream sits
//!     in waiting_on_me forever), closed/merged every reverify_closed_days
//!     within lookback of closed_at/merged_at. The schedule derives from
//!     state + verified_at.
//!
//!   * One scheduler function decides "hydrate this now?" and orders all
//!     work: quarantine backoff dominates every hydration cause (only an
//!     explicit --pr consumes a retry attempt through backoff); repos run
//!     starved-first (runs_since_advance desc, size desc as tiebreaker —
//!     largest-first retired to tiebreaker because it optimizes wall clock
//!     at the cost of "every repo eventually advances"); within a repo,
//!     discovery and new-activity hydration precede re-verify; re-verify is
//!     capped per run, oldest-verified_at first, deterministically jittered
//!     (hash(repo, number) mod period — no RNG, golden summaries stay
//!     byte-stable), and sheds first at the floor, with shed volume counted.
//!
//!   * verified_at is a witnessed write: set only in a transaction holding a
//!     completeness witness for EVERY connection of the PR, which also
//!     recomputes truncated = 0. A witness is constructible iff pagination
//!     of the connection's live id set terminated — ids suffice, bodies are
//!     not part of completeness — so a full skeleton walk earns witnesses
//!     and may sweep, a tail fetch never can, a truncated hydration never
//!     stamps, and a witness-complete sync --pr does.
//!
//!   * Rehydration of a touched PR is layered. The tail-first fetch
//!     (last: K, walked back to id overlap) under count conservation —
//!     archived live rows + new tail == totalCount, else full walk —
//!     applies ONLY to the top-level comments connection, by name: never
//!     reviewThreads (a reply mutates an old thread and the count balances
//!     while the archive is wrong) and never thread-comment connections
//!     (pending-review submission inserts middle-positioned comments).
//!     Those are skeleton-walked in full: cheap mutable fields (isResolved,
//!     lastEditedAt, isMinimized) every time, bodies fetched only for new
//!     or edited ids; when totalCount fits one page the skeleton IS the
//!     naive document, so single-page PRs cost one call. The check's
//!     preconditions are named where it lives (counting universe; stable
//!     creation order — a proof dependency, not a coincidence; induction
//!     from the last witnessed hydration; count and tail from one document;
//!     minimized rows counted live). Tolerance, correctly scoped: a
//!     deletion paired with tail-visible adds is CAUGHT (the arithmetic
//!     overshoots and forces a full walk); only a deletion masked by an
//!     equal count of non-tail-visible adds in one window escapes, and the
//!     re-verify tier catches it.
//!
//!   * Rate-limit floor: every GraphQL document returns rateLimit cost and
//!     remaining; when remaining < config.rate_limit_floor, the run defers —
//!     per-repo "deferred at floor" in the summary, watermark unadvanced
//!     past unfetched data. Sync shares the point budget with the operator's
//!     interactive gh use and must never drain it.
//!
//! Summary document: repos sorted by name, per-repo fields grouped —
//!
//!     counts   fetched, upserted, unchanged (diff-gate skips), filtered
//!              (bots / exclude_authors skips — configured absence is
//!              visible), observations, soft_deleted
//!     refresh  reverified, quiet_mutations_found, tail_hits, full_walks,
//!              bodies_skipped
//!     cost     subprocess_count, subprocess_seconds, bytes_parsed,
//!              rate_cost, sleeps, sleep_seconds
//!     health   truncated, quarantined, discovery_truncated,
//!              deferred_at_floor, watchdog_kills, errors
//!
//! plus run-level rate remaining. Per-call overhead is the intercept of a
//! regression over the run's real (bytes_parsed, subprocess_seconds) pairs;
//! the batching decision reads a median over trailing sync_runs rows, never
//! one run. (A per-run calibration call was designed and cut by the
//! telemetry rule itself: the field's consumer has a superior zero-spend
//! source.) Deterministic modulo the enumerated timing and rate fields,
//! which golden tests mask.
//!
//! Telemetry rule: every field names the decision it feeds — batching (the
//! overhead intercept), re-verify tiers (quiet_mutations_found, split by
//! tier, makes the defaults falsifiable), tail size (tail_hits vs
//! full_walks), retry and floor defaults — or the regression it detects (an
//! unchanged remote with nonzero deltas is replay idempotence failing
//! live). A field with no consumer is deleted. Per-repo detail lives here
//! only: sync_runs persists one flat row per run and grows no child tables.
//! The fence, precisely: per-repo STATE with a scheduling consumer
//! (runs_since_advance) is state and lives on sync_state; per-repo HISTORY
//! is telemetry and stays ephemeral.

use std::sync::mpsc::{Receiver, Sender};

use crate::config::{Config, RepoConfig};
use crate::error::Result;

/// Rows for one writer transaction, already parsed and typed.
/// One value per hydrated PR: the PR row plus its complete child sets, so the
/// writer can upsert and sweep in a single transaction without re-reading.
pub struct PrBundle {
    // pr row, threads, comments, review_requests, refs, linked issues;
    // per-connection completeness witnesses (constructible only when the
    // live id set's pagination terminated) gate the sweep and, jointly,
    // the verified_at write.
}

/// The two discovery streams. Closed on purpose: per-flavor watermarks were
/// considered and rejected — flavor-set identity lives in the sync_state
/// fingerprint, not the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Pr,
    Issue,
}

/// A hydration failure recorded durably. Committed only inside the Done
/// transaction of the window that passes the id: the watermark's advance is
/// licensed by the record that resurfaces the item, so no exit can turn
/// "quarantined" into "forgotten".
pub struct QuarantineRecord {
    pub id: String,
    pub attempts: u32,
    pub next_retry_at: String,
    pub error_class: String,
}

/// A witness that every id in one discovery window resolved to an outcome
/// (hydrated, filtered, quarantined, deferred-free). Constructible only by
/// the window-walk path — sync --pr runs no discovery, so it can never
/// build one, and therefore can never advance a watermark. The same idiom
/// as the sweep witness: an unearned watermark is a type error.
pub struct WindowComplete(());

pub enum Msg {
    Page(String, Vec<PrBundle>),
    /// One completed discovery window: rows, quarantine records, and the
    /// watermark advance in a single transaction — both-or-neither, at
    /// window grain, so a mid-run floor deferral banks every window it
    /// finished and a cold start larger than one run's budget still
    /// converges across runs.
    Done {
        repo: String,
        stream: Stream,
        witness: WindowComplete,
        rows: Vec<PrBundle>,
        quarantine: Vec<QuarantineRecord>,
        watermark: String,
    },
    /// The rate-limit floor stopped this stream; typed so the summary and
    /// stats never string-sniff. The watermark holds at the last completed
    /// window boundary.
    Deferred {
        repo: String,
        stream: Stream,
        reset_at: String,
    },
    Failed(String, crate::error::Error),
}

/// `pr`: targeted hydration of one PR through the ordinary pipeline — same
/// bundle, same writer, same invariants; only discovery is skipped, which
/// is why it can never construct a WindowComplete or advance a watermark.
/// Terminates in exactly one typed outcome: hydrated (witnessed, sets
/// verified_at), already_running, filtered_refused (USER_INPUT naming the
/// filter — honoring would create archive states no config explains),
/// not_in_config (USER_INPUT — with discovery skipped this is the only
/// enforcement point of "discovery scope is the config"), or quarantined
/// (an explicit demand consumes one retry attempt through backoff).
/// Floor-exempt, with the rationale recorded: the floor exists to protect
/// the operator's interactive use, and --pr IS the operator's interactive
/// use — one mechanism, one principled exemption, never a second floor.
pub fn run(_cfg: &Config, _full: bool, _pr: Option<&str>) -> Result<serde_json::Value> {
    todo!("thread::scope: K workers over Mutex<IntoIter<repo>>, writer on the main thread")
}

/// Discovery (scope-dependent flavors, deduped by id) + hydration for one
/// configured repo, oldest window first, ascending updatedAt within a
/// window. Every result — Page, per-window Done with its watermark and
/// quarantine rows, Deferred at the floor, Failed — flows through the
/// channel, never a return value: a String return could carry at most one
/// watermark, and a project repo has two streams. Reads config for scope,
/// people, filters, and the fingerprint.
fn sync_repo(_cfg: &Config, _repo: &RepoConfig, _tx: &Sender<Msg>) -> Result<()> {
    todo!()
}

/// Single writer: owns the Connection, applies the Msg stream in order, and
/// produces the summary document.
fn writer(_rx: Receiver<Msg>) -> Result<serde_json::Value> {
    todo!()
}
