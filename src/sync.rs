//! The sync pipeline.
//!
//! Shape: K worker threads (config.workers, default 3) pull repos from a
//! shared job list ordered by the scheduler (starved-first; see below), run
//! discovery + hydration through gh, and stream typed messages to the
//! single writer thread — the only thread that ever touches the write
//! connection.
//!
//! ```text
//!     workers ── sync_channel(16) ──▶ writer (owns the Connection)
//! ```
//!
//! Messages (see Msg): Page for intermediate rows; Done for one COMPLETED
//! discovery window — its rows, its quarantine records, and the watermark
//! advance in one message, one transaction, both-or-neither, so state cannot
//! lead data and a quarantined id is passed only by the record that
//! resurfaces it; Retries for quarantine-retry outcomes (outside any
//! window — their ids were already passed by the watermark that quarantined
//! them); Deferred when the rate-limit floor stops a stream, typed so the
//! summary never string-sniffs; Failed is recorded in the summary, never
//! carries a watermark, and holds the watermark at the last completed
//! window boundary. Windows are processed oldest-first and hydration
//! ascends by updatedAt, so the hydrated prefix is contiguous, a window
//! boundary is a sound checkpoint, two consecutive floor-deferred runs never
//! re-hydrate the same PR, and an item bumped mid-walk moves toward the
//! unvisited end — seen twice and deduped, never skipped.
//!
//! Invariants, in order of importance:
//!
//!   * The watermark is server-side time — max DISCOVERY-time updatedAt
//!     over the window's resolved ids — never the local clock (clock skew
//!     and search-index lag both lose updates otherwise), and never the
//!     hydration-time value, which an item touched between the two calls
//!     can carry past a closed window's right edge (the fold in
//!     walk_window carries the full argument). Queries overlap the
//!     watermark by ~10 minutes; upserts make the overlap free. The writer
//!     takes max(stored, offered), so a targeted backfill over old windows
//!     can never regress it.
//!
//!   * sync_state advances only on Done: state never leads data. This is also
//!     the entire cancellation story — SIGINT kills the process group (gh
//!     children included), SQLite rolls back the open transaction on next
//!     open, and the next run redoes the window. No signal handler, on
//!     purpose. (Data may lead state: Page rows commit before their window's
//!     watermark, which replay idempotence makes safe.)
//!
//!   * Mark-and-sweep applies soft deletes (deleted_at) to comments/threads
//!     absent from a COMPLETELY fetched connection — never from a truncated
//!     one, or truncation reads as deletion. Deleted rows are kept: upstream
//!     deletion is signal for a memory tool. Review rows (comments
//!     kind='review') sweep under the same rule against the
//!     latestOpinionatedReviews set: a row leaving that set was superseded
//!     by its reviewer's newer verdict, and deleted_at uniformly means
//!     "left the observed set" — one sweep rule, not two.
//!
//!   * Upserts: ON CONFLICT(repo, number) for prs (node ids are data, not
//!     identity), ON CONFLICT(id) elsewhere; never INSERT OR REPLACE (rowid
//!     churn breaks FTS). The field diff each upsert computes is recorded to
//!     observations — that diff is the "what changed since yesterday" answer.
//!     EVERY write is diff-gated, children and wholesale-replaced sets
//!     included: replaying an unchanged remote writes no row, no
//!     observation, and no FTS churn, which is what makes a killed window's
//!     redo a no-op. The diff is load-bearing twice; it is never refactored
//!     to compute after the write.
//!
//!   * verified_at moves only when it says something new: it is stamped when
//!     the hydration was witness-complete AND (anything changed, or the row
//!     was truncated/never-verified, or the hydration was the re-verify
//!     tier's explicit refetch). An overlap-window re-hydration of an
//!     unchanged PR touches nothing — otherwise the ~10-minute overlap would
//!     rewrite a row per PR per run and replay idempotence would be a lie —
//!     while the re-verify tier's refetch always re-stamps, so its schedule
//!     (which reads verified_at) keeps advancing.
//!
//!   * Workers return control on every fallible path (no unwrap on external
//!     data); a panic is a ghgraph bug and crashes loudly. No catch_unwind.
//!
//!   * The writer's original Sender is dropped before the recv loop, or the
//!     loop never terminates.
//!
//!   * Every discovered id resolves to exactly one outcome — hydrated,
//!     filtered, quarantined, or deferred. The stream watermark is
//!     max(updatedAt) over hydrated ∪ filtered (filtered is declined, not
//!     unfetched — a bot-only window must still advance, or re-discovery
//!     grows without bound); a deferred id caps the watermark below it (a
//!     floor stop mid-window sends no Done); a quarantined id is passed only
//!     with its quarantine row in the same transaction. A masked search hit
//!     (item-level null: a visibility domain the viewer cannot see into)
//!     has no id and no updatedAt — it is counted and disclosed
//!     (health.masked_hits) as its defined outcome. An unsplittable window
//!     that still overflows the search cap records discovery_truncated,
//!     keeps what it saw as Page rows, and HALTS the stream, because any
//!     later window's Done would advance the watermark past the lost tail.
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
//!     discovery and new-activity hydration precede quarantine retries,
//!     which precede re-verify; re-verify is capped per run,
//!     oldest-verified_at first (never-verified rows lead), deterministically
//!     jittered (FNV-1a hash(repo, number) mod period — no RNG, golden
//!     summaries stay byte-stable), and sheds first at the floor, with shed
//!     volume counted (refresh.reverify_shed).
//!
//!   * verified_at is a witnessed write: set only in a transaction holding a
//!     completeness witness for EVERY connection of the PR, which also
//!     recomputes truncated = 0. A witness is constructible iff pagination
//!     of the connection's live id set terminated — ids suffice, bodies are
//!     not part of completeness — so a full skeleton walk earns witnesses
//!     and may sweep, a truncated hydration never stamps, and a
//!     witness-complete sync --pr does.
//!
//!   * Rehydration of a touched PR is LAYERED (the round-0-audited design;
//!     DESIGN.md's refresh section carries the argument): a
//!     discovery-origin rehydration of a PR with a witnessed baseline
//!     (verified_at set, truncated 0, not deleted — the structural,
//!     caller-side dispatch gate in StreamCtx::hydrate) fetches
//!     REFRESH_PR — the tail (comments last: K WITH totalCount, one
//!     document per page, a two-round-trip count/tail split being a
//!     TOCTOU) plus skeleton threads (cheap mutable fields, no bodies) —
//!     and runs the count-conservation check (tail_conservation, where
//!     the two masked cases and the fully-observed refinement are
//!     documented). Every arm that cannot license the skip escalates to
//!     the full walk, restarting from the top; every mid-walk abort
//!     (floor, transport) lands the bundle Incomplete instead. A tail hit
//!     is the third completeness state: it upserts the tail, resolves
//!     thread bodies from the archive when id + lastEditedAt match
//!     (bodies_skipped) or refetches whole changed threads
//!     (THREAD_BODIES), and carries stored truncated/verified_at forward
//!     untouched — the tail can never sweep COMMENTS and never stamps,
//!     while a complete skeleton walk in the same bundle still earns the
//!     thread sweeps (per-connection witnesses, as everywhere).
//!     Re-verify, retries, --pr,
//!     and first contact always FULL-WALK: the first-page document plus
//!     follow-up pages for both big connections, the
//!     strictly-more-complete form the refresh specializes — and
//!     re-verify's full walk is what catches the masked cases, within its
//!     tier-bounded reach (unconditional for OPEN; within lookback of
//!     closed_at/merged_at for CLOSED/MERGED — beyond that a masked case
//!     is permanently stale, the same accepted scope limit as closed-tier
//!     discovery). The enablement gate the audit demanded holds as a
//!     fixture: minimized comments COUNT in totalCount
//!     (tests/fixtures/comments_minimized.json; parse.rs pins it
//!     offline). One asymmetry, disclosed: a review thread whose comment
//!     count exceeds the full walk's nested first:30 stays truncated=1 on
//!     full-walk paths (counted, re-verified, never swept) until the
//!     nested `last: K` tail lands (ROADMAP defers sizing it to real
//!     totalCount data) — but the refresh path's per-thread refetch
//!     covers 50, so a thread that grows past 30 on an already-verified
//!     PR can still be completed by a refresh. A later full walk of that
//!     PR cannot re-witness the >30 thread (its stamp stalls and, when
//!     fields changed, it marks truncated) — conservative, disclosed,
//!     and retired by the nested tail when it lands.
//!
//!   * Rate-limit floor: every GraphQL document returns rateLimit cost and
//!     remaining; when remaining < config.rate_limit_floor, the run defers —
//!     per-repo "deferred at floor" in the summary, watermark unadvanced
//!     past unfetched data. Sync shares the point budget with the operator's
//!     interactive gh use and must never drain it. The floor state is a
//!     run-wide flag: one worker tripping it defers every repo behind it.
//!     A missing rateLimit envelope on a successful call yields no new
//!     budget information; the floor keeps its last observation and the
//!     call is counted (health.rate_limit_unknown) — deferring on a benign
//!     envelope hiccup would stall syncs on nothing, and true exhaustion
//!     still arrives typed (FailureKind::RateExhausted folds into this same
//!     defer path).
//!
//! Summary document: repos sorted by name, per-repo fields grouped —
//!
//! ```text
//!     counts   fetched, upserted, unchanged (diff-gate skips), filtered
//!              (bots / exclude_authors skips — configured absence is
//!              visible), observations, soft_deleted
//!     refresh  reverified, quiet_mutations_found, reverify_shed,
//!              tail_hits, full_walks (partitioning concluded refresh
//!              attempts — the hit rate that sizes TAIL_K), escalations
//!              (full_walks split by reason: which response would change
//!              the outcome is a per-reason question), bodies_skipped
//!     cost     subprocess_count, subprocess_seconds, bytes_parsed,
//!              rate_cost, sleeps, sleep_seconds
//!     health   truncated, quarantined, discovery_truncated,
//!              deferred_at_floor, watchdog_kills, masked_hits,
//!              rate_limit_unknown, errors
//! ```
//!
//! plus run-level rate remaining. Per-call overhead is the intercept of a
//! regression over the run's real (bytes_parsed, subprocess_seconds) pairs;
//! the batching decision reads a median over trailing sync_runs rows, never
//! one run. (A per-run calibration call was designed and cut by the
//! telemetry rule itself: the field's consumer has a superior zero-spend
//! source.) Deterministic modulo the enumerated timing fields
//! (subprocess_seconds, sleep_seconds), which golden tests mask.
//!
//! Telemetry rule: every field names the decision it feeds — batching (the
//! overhead intercept), re-verify tiers (quiet_mutations_found, makes the
//! defaults falsifiable), tail size (tail_hits vs full_walks, with their
//! mechanism), retry and floor defaults — or the regression it detects
//! (unchanged remote with nonzero deltas is replay idempotence failing
//! live; rate_limit_unknown is the envelope regressing). A field with no
//! consumer is deleted. Per-repo detail lives here only: sync_runs persists
//! one flat row per run and grows no child tables. The fence, precisely:
//! per-repo STATE with a scheduling consumer (runs_since_advance) is state
//! and lives on sync_state; per-repo HISTORY is telemetry and stays
//! ephemeral. runs_since_advance semantics, refined where the mechanism
//! landed: it resets when the stream COMPLETES (all windows Done — an
//! empty-but-checked repo is not starved) and increments on a deferred or
//! failed run; the starvation it detects is "never gets to finish", not
//! "finds nothing".
//!
//! The sync run lock: one OS file lock (`File::try_lock`, ghgraph.db.lock
//! beside the archive), released by the OS on any death including SIGKILL —
//! never a run-long transaction, which would destroy per-window commits. A
//! second sync exits promptly with a typed already-running envelope,
//! TRANSIENT: the actor who fixes it is time.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;

use serde_json::{Value, json};

use crate::config::{Config, RepoConfig, Scope};
use crate::db::{self, RwArchive};
use crate::error::{Error, Result};
use crate::gh::{self, FailureKind, GhCtx};
use crate::identity::{self, Login};
use crate::parse;
use crate::queries;
use crate::refs;
use crate::time::Rfc3339Utc;

/// Discovery windows overlap the watermark by this much: GitHub's search
/// index lags and re-sorts live, so a query from the exact watermark can
/// miss an item the index surfaced late. Upserts make the overlap free.
const OVERLAP_SECS: u64 = 600;

/// Below this width a capped window is unsplittable: the search grammar is
/// second-granular, so a 2s window is already at the smallest range whose
/// halves are expressible.
const MIN_WINDOW_SECS: i64 = 2;

/// Backstop on discovery pagination: GitHub search caps near 1,000 results
/// (20 pages of 50); a cursor still walking past 40 pages means the cap
/// heuristic failed and the window must split rather than trust the walk.
const MAX_DISCOVERY_PAGES: u32 = 40;

/// Backstop on hydration follow-up pagination: 100 pages of 100 comments is
/// an order of magnitude past any real PR; hitting it marks the PR
/// truncated (disclosed) rather than walking forever on a broken cursor.
const MAX_CONNECTION_PAGES: u32 = 100;

/// Bundles per Page message: bounds worker memory on a 1,000-item window
/// while keeping writer transactions usefully sized. Known-equivalent
/// mutants, and they stay: the flush threshold is a tuning constant — ANY
/// positive batching is correct (rows reach the writer either in Page
/// batches or in the window's Done), so comparison mutants here change
/// message shapes, never archive content. The same argument as gh.rs's
/// drain chunk size.
const PAGE_BATCH: usize = 8;

/// Re-verify hydrations per repo per run. A cap, not a config: it exists so
/// a cold archive's backlog cannot starve discovery, and the jitter spreads
/// the steady state; sync_runs telemetry (milestone 5) is what would
/// promote it.
const REVERIFY_CAP: usize = 25;

/// node:null must repeat this many attempts before draining to
/// prs.deleted_at — one null can be replication lag; three spaced by
/// backoff is a deletion. Conservative ship; ROADMAP defers tuning.
const QUARANTINE_DRAIN_ATTEMPTS: u32 = 3;

/// Quarantine backoff: BASE doubled per attempt, capped. An hour rides out
/// transient API trouble; a week bounds how stale a persistently failing
/// id's retry cadence can get.
const QUARANTINE_BACKOFF_BASE_SECS: u64 = 3_600;
const QUARANTINE_BACKOFF_CAP_SECS: u64 = 7 * 86_400;

/// The two discovery streams. Closed on purpose: per-flavor watermarks were
/// considered and rejected — flavor-set identity lives in the sync_state
/// fingerprint, not the key. Both walk on the same machinery: per-stream
/// watermarks (sync_state is keyed (repo, stream)), stream-typed discovery
/// terms (queries.rs), and stream-typed hydration dispatch — the quarantine
/// table's `stream` column carries the dispatch fact across runs, since a
/// node id is opaque and cannot say which document resurrects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stream {
    Pr,
    Issue,
}

impl Stream {
    fn as_str(self) -> &'static str {
        match self {
            Stream::Pr => "pr",
            Stream::Issue => "issue",
        }
    }

    /// Parse a stored stream tag (quarantine.stream). `None` for anything
    /// else — the caller classifies it INTERNAL, since only ghgraph writes
    /// the column and an unknown tag means this code drifted from its own
    /// archive.
    fn parse(s: &str) -> Option<Stream> {
        match s {
            "pr" => Some(Stream::Pr),
            "issue" => Some(Stream::Issue),
            _ => None,
        }
    }
}

/// What caused a hydration — the writer attributes refresh counters and
/// verified_at policy by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Discovery,
    Reverify,
    Retry,
    Targeted,
}

/// The top-level comments connection's completeness, three-state by the
/// round-0 spec audit's decree. Two states cannot say "the tail sufficed":
/// with a boolean, a tail hit either claims complete (falsely licensing
/// the sweep and the verified_at stamp — the middle was inferred, not
/// witnessed) or claims incomplete (flipping truncated 0↔1 per alternating
/// tail/full runs on an UNCHANGED PR — an UPDATE per tail hit, replay
/// idempotence broken). TailHit is the third horn: the writer carries the
/// STORED truncated and verified_at forward untouched, sweeps nothing,
/// stamps nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommentsCompleteness {
    /// Pagination of the live id set terminated: the witness, as in B2.
    Complete,
    /// The conservation check concluded the un-fetched middle is unchanged;
    /// the bundle's comment nodes are the tail only. An inference, never a
    /// witness: it can neither sweep nor stamp, and the stored
    /// truncated/verified_at pass through unmodified.
    TailHit,
    /// Pagination did not terminate (cap, floor, mid-walk failure): the
    /// row lands truncated, as in B2.
    Incomplete,
}

/// Rows for one writer transaction, already parsed and typed: the PR node
/// with its comments/reviewThreads vectors REPLACED by the merged
/// all-pages sets, plus body-extracted refs and the per-connection
/// completeness verdicts. Constructed only by [`hydrate_one`] and the
/// refresh path (`refresh_one`), the only places a completeness claim can
/// be earned.
pub struct PrBundle {
    repo: String,
    pr: parse::PrNode,
    refs: Vec<refs::ExtractedRef>,
    origin: Origin,
    /// Top-level comments: the one three-state connection (see
    /// [`CommentsCompleteness`]); every other connection is witnessed or
    /// not, with no inference form.
    comments: CommentsCompleteness,
    /// Thread pagination terminated AND every thread's nested comment
    /// selection was complete (nodes == totalCount). The skeleton walk
    /// earns this the same way a full walk does: ids suffice, bodies are
    /// not part of completeness.
    threads_complete: bool,
    /// The three schema-nullable connections: present (not error-masked)
    /// and complete (nodes == totalCount).
    requests_complete: bool,
    reviews_complete: bool,
    closing_complete: bool,
}

impl PrBundle {
    /// Witness-complete: every connection of the PR paginated to its end.
    /// The gate for verified_at and for sweeps. A TailHit is deliberately
    /// NOT verified — the comments middle was inferred, and an inference
    /// licenses carrying state forward, never advancing it.
    fn verified(&self) -> bool {
        self.comments == CommentsCompleteness::Complete
            && self.threads_complete
            && self.requests_complete
            && self.reviews_complete
            && self.closing_complete
    }
}

/// Rows for one writer transaction, issue-stream form: the issue node with
/// its comments vector REPLACED by the merged all-pages set, plus the
/// per-connection completeness verdicts. Constructed only by
/// [`hydrate_issue_one`] — issues have no refresh/tail path (queries.rs
/// HYDRATE_ISSUE records the cut), so completeness is two-state: witnessed
/// or not.
pub struct IssueBundle {
    repo: String,
    issue: parse::IssueNode,
    origin: Origin,
    /// Pagination of the comments connection's live id set terminated.
    comments_complete: bool,
    /// The two small counted connections: nodes cover totalCount.
    labels_complete: bool,
    assignees_complete: bool,
}

impl IssueBundle {
    /// Witness-complete: the gate for issues.verified_at and the comments
    /// sweep, same rule as [`PrBundle::verified`].
    fn verified(&self) -> bool {
        self.comments_complete && self.labels_complete && self.assignees_complete
    }
}

/// One hydrated item, stream-typed: the walk, the channel messages, and the
/// writer dispatch all carry this, so a PR row and an issue row cannot take
/// each other's upsert path — the write-side mirror of the stream-typed
/// discovery terms. Boxed variants: a PrBundle is ~750 bytes and rides in
/// Vecs and channel messages; the box keeps those moves pointer-sized.
pub enum Bundle {
    Pr(Box<PrBundle>),
    Issue(Box<IssueBundle>),
}

impl Bundle {
    fn id(&self) -> &str {
        match self {
            Bundle::Pr(b) => &b.pr.id,
            Bundle::Issue(b) => &b.issue.id,
        }
    }
}

/// A hydration failure recorded durably. Committed only inside the Done (or
/// Retries) transaction that passes the id: the watermark's advance is
/// licensed by the record that resurfaces the item, so no exit can turn
/// "quarantined" into "forgotten".
pub struct QuarantineRecord {
    pub id: String,
    /// Which hydration document resurrects this id (schema.sql records why
    /// the id itself cannot say).
    pub stream: Stream,
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
    /// Intermediate rows, committed ahead of their window's watermark (data
    /// may lead state; replay idempotence makes the redo free).
    Page { repo: String, rows: Vec<Bundle> },
    /// One completed discovery window: rows, quarantine records, and the
    /// watermark advance in a single transaction — both-or-neither, at
    /// window grain, so a mid-run floor deferral banks every window it
    /// finished and a cold start larger than one run's budget still
    /// converges across runs.
    Done {
        repo: String,
        stream: Stream,
        witness: WindowComplete,
        rows: Vec<Bundle>,
        quarantine: Vec<QuarantineRecord>,
        /// None: an empty window (nothing to advance over) or an
        /// unsplittable-truncated one (advancing would pass the lost tail).
        watermark: Option<Rfc3339Utc>,
        fingerprint: String,
        /// The stream's final window: stamps last_checked_at and resets
        /// runs_since_advance. Backfill windows never set it.
        completes_stream: bool,
        discovery_truncated: u64,
        masked: u64,
    },
    /// Quarantine-retry outcomes, outside any window: their ids were
    /// already passed by the watermark that quarantined them.
    Retries {
        repo: String,
        resolved: Vec<Bundle>,
        requeued: Vec<QuarantineRecord>,
        /// node:null reached the drain threshold: soft-delete + row removal.
        /// Stream-tagged — the drain writes the stream's own table.
        drained: Vec<(String, Stream)>,
    },
    /// The rate-limit floor stopped this stream; typed so the summary and
    /// stats never string-sniff. The watermark holds at the last completed
    /// window boundary. reset_at feeds `retry_after` on the TRANSIENT
    /// envelope where an envelope exists (the targeted `sync --pr` path);
    /// a full run's deferral is a summary disclosure, not an error, so
    /// here the field waits for a summary consumer — carried in the
    /// message because its shape is the workers' contract and adding it
    /// later would touch every send site.
    Deferred {
        repo: String,
        stream: Stream,
        reset_at: Option<String>,
    },
    /// Repo-scoped failure. Carries the streams THIS RUN was configured to
    /// walk: the starvation increment must not touch a residual row for a
    /// stream the config no longer runs (issues flipped off leaves the
    /// (repo,'issue') row behind, and only its own stream's completing
    /// Done can ever reset it — an unconditional increment would read the
    /// repo as permanently starved and bias starved-first ordering
    /// forever).
    Failed(String, Vec<Stream>, Error),
    /// Worker-side accounting, once per repo, after its last data message.
    Stats {
        repo: String,
        tel: gh::Telemetry,
        fetched: u64,
        filtered: u64,
        reverified: u64,
        reverify_shed: u64,
        /// Refresh attempts whose conservation check concluded (the
        /// dispatch gate passed): tail_hits and full_walks PARTITION them
        /// — an escalated check counts as full_walks only — so
        /// tail_hits/(tail_hits+full_walks) is the true hit rate that
        /// sizes TAIL_K (the telemetry rule's named consumer). First
        /// contact, re-verify, retries, and --pr are not attempts: they
        /// never reach the gate. A walk-back aborted BEFORE the verdict
        /// (floor, transport) counts as neither and lands in
        /// health.truncated; post-verdict thread-phase failures still
        /// count as tail_hits — the tail mechanism concluded, and the
        /// incompleteness is disclosed through the withheld threads
        /// witness.
        tail_hits: u64,
        full_walks: u64,
        /// Escalations by reason (the strings RefreshEnd::Escalate
        /// names): the diagnostic split behind full_walks — which
        /// response would change the outcome (raise K, accept deletion
        /// traffic, distrust the walk) is a per-reason question.
        escalations: BTreeMap<&'static str, u64>,
        /// Thread-comment bodies resolved from the archive instead of the
        /// wire (id known and lastEditedAt unmoved): the skeleton's saved
        /// bytes, and the field that would expose a body-skip that stops
        /// firing (a regression detector, its other named consumer).
        bodies_skipped: u64,
    },
}

// ---------------------------------------------------------------------------
// run(): lock, gates, scheduler, threads

/// `pr`: targeted hydration of one PR through the ordinary pipeline — same
/// bundle, same writer helpers, same invariants; only discovery is skipped,
/// which is why it can never construct a WindowComplete or advance a
/// watermark. Terminates in exactly one typed outcome: hydrated (witnessed,
/// sets verified_at), already_running, filtered_refused (USER_INPUT naming
/// the filter — honoring would create archive states no config explains),
/// not_in_config (USER_INPUT — with discovery skipped this is the only
/// enforcement point of "discovery scope is the config"), or quarantined
/// (an explicit demand consumes one retry attempt through backoff).
/// Floor-exempt, with the rationale recorded: the floor exists to protect
/// the operator's interactive use, and --pr IS the operator's interactive
/// use — one mechanism, one principled exemption, never a second floor.
pub fn run(cfg: &Config, full: bool, pr: Option<&str>) -> Result<Value> {
    let db_path = cfg.db_path()?;
    let mut archive = db::open_rw(&db_path)?;
    let _lock = RunLock::acquire(&db_path)?;
    gh::version_gate()?;
    let authenticated = gh::viewer_login()?;
    if !identity::login_eq(cfg.viewer.as_str(), &authenticated) {
        // The authenticated login is API text and stays out of the message;
        // the config value's echo is licensed (it is the operator's own).
        return Err(Error::config(format!(
            "gh is authenticated as a different account than config viewer {:?} — \
             fix `viewer` or switch accounts (gh auth login)",
            cfg.viewer.as_str()
        )));
    }
    if let Some(reference) = pr {
        return run_targeted(cfg, &mut archive, reference);
    }

    let now = Rfc3339Utc::now();
    let plans = plan(cfg, &archive, &now, full)?;
    let repo_names: Vec<String> = {
        let mut names: Vec<String> = plans.iter().map(|p| p.repo.clone()).collect();
        names.sort();
        names
    };

    let (tx, rx) = std::sync::mpsc::sync_channel::<Msg>(16);
    let jobs = Mutex::new(plans.into_iter());
    let floor = AtomicBool::new(false);

    thread::scope(|scope| {
        for _ in 0..cfg.workers.max(1) {
            let tx = tx.clone();
            let jobs = &jobs;
            let floor = &floor;
            scope.spawn(move || worker(cfg, jobs, tx, floor));
        }
        // The writer's original Sender is dropped before the recv loop, or
        // the loop never terminates (module invariant).
        drop(tx);
        writer(&mut archive, rx, &now, &repo_names)
    })
}

/// One sync per archive: an OS file lock beside the db, released by the OS
/// on any death including SIGKILL. Never a run-long transaction — that
/// would destroy per-window commits. The lock FILE persists between runs
/// (unlinking a held lock's path would race a third process); only the lock
/// itself is scoped to this value's lifetime.
struct RunLock {
    _file: std::fs::File,
}

impl RunLock {
    fn acquire(db_path: &std::path::Path) -> Result<RunLock> {
        use std::os::unix::fs::OpenOptionsExt;
        let path = db_path.with_extension("db.lock");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .map_err(|e| Error::config(format!("cannot open run lock {}: {e}", path.display())))?;
        match file.try_lock() {
            Ok(()) => Ok(RunLock { _file: file }),
            Err(std::fs::TryLockError::WouldBlock) => Err(Error::transient(
                "another sync is already running on this archive; wait for it or check for a \
                 stuck process",
            )),
            Err(std::fs::TryLockError::Error(e)) => Err(Error::config(format!(
                "cannot lock {}: {e}",
                path.display()
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// The scheduler: one function decides and orders all work.

struct RepoPlan {
    rc: RepoConfig,
    repo: String,
    /// PR-stream main-walk discovery start, overlap already applied.
    since: Rfc3339Utc,
    /// Issue-stream main-walk start; `Some` iff the repo runs the issue
    /// stream (project scope with issues on — config.rs rejects every
    /// other combination). Each stream transitions against its OWN stored
    /// fingerprint row, so a scope flip cold-starts both while an
    /// unchanged config leaves both incremental.
    issue_since: Option<Rfc3339Utc>,
    /// This walk restarts from the lookback floor (a relaxation ColdStart,
    /// or --full): its intermediate windows commit the STORED fingerprint,
    /// so a kill mid-walk reads "unequal → cold-start again" next run
    /// instead of "equal → incremental", which would silently skip the
    /// region the walk never reached — the F1 argument, generalized from
    /// backfill to every restart-from-floor (D1 panel). The price,
    /// recorded: an interrupted cold start banks no discovery progress
    /// (the watermark stays pinned at the old high by max()) and redoes
    /// its windows next run; it converges when any single run completes
    /// the walk, and the redo is idempotent. Under --full with an
    /// UNCHANGED config the stored and new fingerprints are byte-equal
    /// (canonical serialization), so the hold degenerates to a no-op.
    /// Per stream, like everything else here.
    cold_start: bool,
    issue_cold_start: bool,
    /// The issue stream's own stored fingerprint (see stored_fingerprint;
    /// the rows can disagree — e.g. a kill mid-cold-start of one stream).
    issue_stored_fingerprint: Option<String>,
    /// People added since the stored fingerprint: targeted backfill of just
    /// their involves: flavors over the lookback. PR-stream only — people
    /// never shape project-scope discovery, and the issue stream exists
    /// only at project scope, so an issue backfill is unconstructible.
    backfill: Vec<Login>,
    fingerprint: String,
    /// The fingerprint currently in the PR stream's sync_state, verbatim.
    /// Backfill windows commit THIS one: a kill mid-backfill must leave
    /// the stored inputs unchanged, or the next run reads "equal →
    /// incremental" and the rest of the backfill silently never happens
    /// (closure-pass F1). Only the main walk's windows write the new
    /// fingerprint, and by then the backfill has completed — a kill after
    /// that point redoes a completed (idempotent) backfill, which
    /// converges.
    stored_fingerprint: Option<String>,
    /// Due quarantine retries: (id, prior attempts, stream). Ordered by id.
    quarantine_due: Vec<(String, u32, Stream)>,
    /// Every quarantined id, due or not: backoff dominates every hydration
    /// cause, so windows and re-verify skip these entirely. One set for
    /// both streams — node ids are globally unique.
    quarantined: HashSet<String>,
    /// Capped, ordered re-verify list (never-verified first, then oldest),
    /// stream-tagged for hydration dispatch.
    reverify: Vec<(String, i64, Stream)>,
}

/// The streams a repo's config runs, in walk order (PR first: the demand
/// surface's stream; deterministic either way).
fn streams(plan: &RepoPlan) -> Vec<Stream> {
    if plan.issue_since.is_some() {
        vec![Stream::Pr, Stream::Issue]
    } else {
        vec![Stream::Pr]
    }
}

/// Build every repo's plan and order them starved-first. The only reader of
/// archive state before the writer takes the connection.
fn plan(cfg: &Config, archive: &RwArchive, now: &Rfc3339Utc, full: bool) -> Result<Vec<RepoPlan>> {
    let conn = archive.conn();
    let mut plans = Vec::new();
    let mut order_keys: HashMap<String, (i64, i64)> = HashMap::new();
    for entry in &cfg.repos {
        let rc = entry.resolved();
        let repo = rc.repo.as_str().to_string();
        let state: Option<(String, String)> = conn
            .query_row(
                "SELECT last_item_updated_at, fingerprint FROM sync_state \
                 WHERE repo = ?1 AND stream = 'pr'",
                [&repo],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map(Some)
            .or_else(none_if_no_rows)
            .map_err(|e| classify_sql(&e))?;
        let fp_new = fingerprint(cfg, &rc);
        let fp_json = serde_json::to_string(&fp_new).expect("fingerprint is plain data");

        let lookback_start = lookback_start(now, &rc, cfg);
        let (since, backfill, cold_start) = if full {
            // --full ignores watermarks and refetches the whole lookback;
            // it subsumes any pending backfill (every flavor re-runs).
            (lookback_start.clone(), Vec::new(), true)
        } else {
            match transition(state.as_ref(), &fp_new) {
                Transition::ColdStart => (lookback_start.clone(), Vec::new(), true),
                Transition::Incremental => (
                    incremental_since(state.as_ref(), &lookback_start),
                    Vec::new(),
                    false,
                ),
                Transition::Backfill(added) => (
                    incremental_since(state.as_ref(), &lookback_start),
                    added,
                    false,
                ),
            }
        };

        // One gate, computed once: the issue-since decision and the
        // issue-re-verify decision must agree (a divergence would re-verify
        // a stream the walk never runs, or vice versa), so they read the
        // same binding.
        let issue_stream_on = rc.scope == Scope::Project && rc.issues();

        // The issue stream's own transition, against its own state row.
        // Backfill folds to Incremental by construction (project-scope
        // fingerprints pin people empty, so `added` cannot be non-empty);
        // the arm is written rather than unreachable-marked because its
        // failure mode — a people edit backfilling a stream whose search
        // never mentions them — is exactly what the pinned-empty rule
        // exists to prevent, and Incremental is that rule's meaning here.
        let (issue_since, issue_cold_start, issue_stored_fingerprint) = if issue_stream_on {
            let issue_state: Option<(String, String)> = conn
                .query_row(
                    "SELECT last_item_updated_at, fingerprint FROM sync_state \
                     WHERE repo = ?1 AND stream = 'issue'",
                    [&repo],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map(Some)
                .or_else(none_if_no_rows)
                .map_err(|e| classify_sql(&e))?;
            let (issue_since, issue_cold) = if full {
                (lookback_start.clone(), true)
            } else {
                match transition(issue_state.as_ref(), &fp_new) {
                    Transition::ColdStart => (lookback_start.clone(), true),
                    Transition::Incremental | Transition::Backfill(_) => (
                        incremental_since(issue_state.as_ref(), &lookback_start),
                        false,
                    ),
                }
            };
            (Some(issue_since), issue_cold, issue_state.map(|(_, fp)| fp))
        } else {
            (None, false, None)
        };

        let mut quarantine_due = Vec::new();
        let mut quarantined = HashSet::new();
        {
            let mut stmt = conn
                .prepare(
                    "SELECT id, stream, attempts, next_retry_at FROM quarantine \
                     WHERE repo = ?1 ORDER BY id",
                )
                .map_err(|e| classify_sql(&e))?;
            let rows = stmt
                .query_map([&repo], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                })
                .map_err(|e| classify_sql(&e))?;
            for row in rows {
                let (id, stream, attempts, next_retry_at) = row.map_err(|e| classify_sql(&e))?;
                let Some(stream) = Stream::parse(&stream) else {
                    // Only ghgraph writes this column; an unknown tag is
                    // this code drifting from its own archive.
                    return Err(Error::internal(format!(
                        "quarantine row carries unknown stream tag {stream:?}"
                    )));
                };
                if next_retry_at.as_str() <= now.as_str() {
                    quarantine_due.push((id.clone(), u32::try_from(attempts).unwrap_or(0), stream));
                }
                quarantined.insert(id);
            }
        }

        let reverify = reverify_due(conn, &repo, cfg, now, &quarantined, issue_stream_on)?;

        // Starvation is per stream in the schema but starved-FIRST ordering
        // is per repo (a repo is the unit a worker takes): the max over its
        // streams, so either stream starving pulls the repo forward.
        let runs_since_advance: i64 = conn
            .query_row(
                "SELECT coalesce(max(runs_since_advance), 0) FROM sync_state WHERE repo = ?1",
                [&repo],
                |r| r.get(0),
            )
            .map_err(|e| classify_sql(&e))?;
        let size: i64 = conn
            .query_row(
                "SELECT (SELECT count(*) FROM prs WHERE repo = ?1) + \
                        (SELECT count(*) FROM issues WHERE repo = ?1)",
                [&repo],
                |r| r.get(0),
            )
            .map_err(|e| classify_sql(&e))?;
        order_keys.insert(repo.clone(), (runs_since_advance, size));

        plans.push(RepoPlan {
            rc,
            repo,
            since,
            issue_since,
            cold_start,
            issue_cold_start,
            issue_stored_fingerprint,
            backfill,
            fingerprint: fp_json,
            stored_fingerprint: state.as_ref().map(|(_, fp)| fp.clone()),
            quarantine_due,
            quarantined,
            reverify,
        });
    }
    // Starved-first: runs_since_advance desc, then size desc, then name for
    // a total, deterministic order.
    plans.sort_by(|a, b| {
        let ka = order_keys[&a.repo];
        let kb = order_keys[&b.repo];
        kb.0.cmp(&ka.0)
            .then(kb.1.cmp(&ka.1))
            .then(a.repo.cmp(&b.repo))
    });
    Ok(plans)
}

fn lookback_start(now: &Rfc3339Utc, rc: &RepoConfig, cfg: &Config) -> Rfc3339Utc {
    let days = rc.lookback_days.unwrap_or(cfg.lookback_days);
    now.checked_sub_days(days)
        .unwrap_or_else(|| Rfc3339Utc::parse("1970-01-01T00:00:00Z").expect("epoch parses"))
}

/// Incremental start: watermark minus the overlap, clamped to the lookback
/// floor. The clamp is a policy, recorded: lookback bounds how far
/// DISCOVERY reaches even after a long-unsynced gap — items updated inside
/// the gap but outside the lookback were once archived, and the re-verify
/// tier (which schedules by state, not recency) keeps the open ones honest.
fn incremental_since(state: Option<&(String, String)>, lookback_start: &Rfc3339Utc) -> Rfc3339Utc {
    let wm = state
        .and_then(|(wm, _)| Rfc3339Utc::parse(wm).ok())
        .and_then(|wm| wm.checked_sub_secs(OVERLAP_SECS));
    match wm {
        Some(wm) if wm > *lookback_start => wm,
        _ => lookback_start.clone(),
    }
}

/// The structured discovery fingerprint (schema.sql records why it is
/// structured, not hashed). It carries exactly the inputs that shape THIS
/// stream's discovery and ingest: viewer and people shape working-scope
/// flavors only, so at project scope they are pinned empty — a people edit
/// must not backfill a stream whose search never mentions them.
pub(crate) fn fingerprint(cfg: &Config, rc: &RepoConfig) -> Value {
    let (viewer, mut people) = match rc.scope {
        Scope::Working => (
            cfg.viewer.as_str().to_string(),
            cfg.people
                .iter()
                .map(|p| p.as_str().to_string())
                .collect::<Vec<_>>(),
        ),
        Scope::Project => (String::new(), Vec::new()),
    };
    people.sort();
    let mut exclude: Vec<String> = rc
        .exclude_authors
        .iter()
        .map(|p| {
            let mut s = p.login().as_str().to_string();
            if p.bot_only() {
                s.push_str("[bot]");
            }
            s
        })
        .collect();
    exclude.sort();
    json!({
        "scope": match rc.scope { Scope::Working => "working", Scope::Project => "project" },
        "viewer": viewer,
        "people": people,
        "bots": rc.bots(),
        "exclude_authors": exclude,
        "lookback_days": rc.lookback_days.unwrap_or(cfg.lookback_days),
    })
}

enum Transition {
    Incremental,
    Backfill(Vec<Login>),
    ColdStart,
}

/// The config-transition rules (DESIGN.md, Config): equal → incremental;
/// person added → targeted backfill of just the new involves: flavor; any
/// other relaxation (scope flip, filter relaxed, lookback increased, viewer
/// changed) → stream cold-start, because history the old inputs never
/// discovered cannot be incrementally recovered; tightening → nothing
/// (filters govern ingest, never deletion — a person removed keeps their
/// rows and only the fingerprint updates).
fn transition(state: Option<&(String, String)>, new: &Value) -> Transition {
    let Some((_, old_json)) = state else {
        return Transition::ColdStart;
    };
    let Ok(old) = serde_json::from_str::<Value>(old_json) else {
        // An unreadable stored fingerprint cannot prove the inputs match;
        // cold-start rather than guess.
        return Transition::ColdStart;
    };
    if old == *new {
        return Transition::Incremental;
    }
    let relaxed = old["scope"] != new["scope"]
        || old["viewer"] != new["viewer"]
        || as_u64(&old["lookback_days"]) < as_u64(&new["lookback_days"])
        || (!as_bool(&old["bots"]) && as_bool(&new["bots"]))
        // Relaxed = an exclusion the old inputs enforced is gone from the
        // new ones (old ⊄ new); ADDING an exclusion is a tightening.
        || !subset(&old["exclude_authors"], &new["exclude_authors"]);
    if relaxed {
        return Transition::ColdStart;
    }
    let added: Vec<Login> = str_items(&new["people"])
        .filter(|p| !str_items(&old["people"]).any(|o| o == *p))
        .filter_map(|p| Login::new(p).ok())
        .collect();
    if added.is_empty() {
        Transition::Incremental
    } else {
        Transition::Backfill(added)
    }
}

fn as_u64(v: &Value) -> u64 {
    v.as_u64().unwrap_or(0)
}
fn as_bool(v: &Value) -> bool {
    v.as_bool().unwrap_or(false)
}
fn str_items(v: &Value) -> impl Iterator<Item = &str> {
    v.as_array().into_iter().flatten().filter_map(Value::as_str)
}
/// Every item of `a` appears in `b`.
fn subset(a: &Value, b: &Value) -> bool {
    str_items(a).all(|x| str_items(b).any(|y| y == x))
}

/// The re-verify schedule: due = verified_at is NULL (never witnessed) or
/// older than the tier period plus a deterministic per-item jitter
/// (FNV-1a(repo, number) mod period — no RNG), which spreads a cold
/// archive's re-verifies across [period, 2·period) instead of a thundering
/// herd on day N. Open tier is lookback-exempt (OPEN is the relevance
/// signal); the PR closed tier is bounded by lookback of
/// closed_at/merged_at. Quarantined ids are excluded — backoff dominates
/// every hydration cause.
///
/// Issues re-verify under the same two tiers when the repo runs the issue
/// stream, with two deviations, both decisions:
///   * Only hydration_source='stream' rows: a working-scope linked row is
///     a fill-only context cache with no witness to keep honest — putting
///     it in the schedule would hydrate it into a full row, exactly the
///     scope widening DESIGN.md's "stays a skinny context cache" forbids.
///   * The closed tier bounds by updated_at, not closed_at (the issues
///     table stores none — queries.rs HYDRATE_ISSUE records the cut).
///     Containment: updated_at ≥ close time always (closing bumps it), so
///     the updated_at bound re-verifies a superset of the closed_at rule's
///     rows — wider by exactly the issues still discussed after closing,
///     never narrower, so no closed-within-lookback issue is missed.
///
/// The cap applies per stream, not shared: a PR backlog must not starve
/// issue re-verify or vice versa, and two bounded lists stay bounded.
fn reverify_due(
    conn: &rusqlite::Connection,
    repo: &str,
    cfg: &Config,
    now: &Rfc3339Utc,
    quarantined: &HashSet<String>,
    issue_stream_on: bool,
) -> Result<Vec<(String, i64, Stream)>> {
    let closed_floor = lookback_floor_str(now, cfg);

    // NULL verified_at leads (never witnessed), then oldest; number breaks
    // ties for a total order.
    let mut pr_due: Vec<(String, i64)> = Vec::new();
    reverify_tier(
        conn,
        repo,
        now,
        quarantined,
        "SELECT id, number, verified_at FROM prs \
         WHERE repo = ?1 AND deleted_at IS NULL AND state = 'OPEN' \
         ORDER BY verified_at IS NOT NULL, verified_at ASC, number ASC",
        &[&repo],
        cfg.reverify_open_days,
        &mut pr_due,
    )?;
    reverify_tier(
        conn,
        repo,
        now,
        quarantined,
        "SELECT id, number, verified_at FROM prs \
         WHERE repo = ?1 AND deleted_at IS NULL AND state != 'OPEN' \
           AND coalesce(merged_at, closed_at, '') >= ?2 \
         ORDER BY verified_at IS NOT NULL, verified_at ASC, number ASC",
        &[&repo, &closed_floor],
        cfg.reverify_closed_days,
        &mut pr_due,
    )?;
    pr_due.truncate(REVERIFY_CAP);
    let mut out: Vec<(String, i64, Stream)> = pr_due
        .into_iter()
        .map(|(id, n)| (id, n, Stream::Pr))
        .collect();

    if issue_stream_on {
        let mut issue_due: Vec<(String, i64)> = Vec::new();
        reverify_tier(
            conn,
            repo,
            now,
            quarantined,
            "SELECT id, number, verified_at FROM issues \
             WHERE repo = ?1 AND deleted_at IS NULL AND state = 'OPEN' \
               AND hydration_source = 'stream' \
             ORDER BY verified_at IS NOT NULL, verified_at ASC, number ASC",
            &[&repo],
            cfg.reverify_open_days,
            &mut issue_due,
        )?;
        reverify_tier(
            conn,
            repo,
            now,
            quarantined,
            "SELECT id, number, verified_at FROM issues \
             WHERE repo = ?1 AND deleted_at IS NULL AND state != 'OPEN' \
               AND hydration_source = 'stream' AND updated_at >= ?2 \
             ORDER BY verified_at IS NOT NULL, verified_at ASC, number ASC",
            &[&repo, &closed_floor],
            cfg.reverify_closed_days,
            &mut issue_due,
        )?;
        issue_due.truncate(REVERIFY_CAP);
        out.extend(issue_due.into_iter().map(|(id, n)| (id, n, Stream::Issue)));
    }
    Ok(out)
}

/// One re-verify tier query: append due (id, number) pairs. The due rule
/// (NULL verified_at, or older than period + deterministic jitter) is
/// documented at [`reverify_due`].
#[allow(clippy::too_many_arguments)]
fn reverify_tier(
    conn: &rusqlite::Connection,
    repo: &str,
    now: &Rfc3339Utc,
    quarantined: &HashSet<String>,
    sql: &str,
    args: &[&dyn rusqlite::ToSql],
    period_days: u32,
    due: &mut Vec<(String, i64)>,
) -> Result<()> {
    let mut stmt = conn.prepare(sql).map_err(|e| classify_sql(&e))?;
    let rows = stmt
        .query_map(args, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|e| classify_sql(&e))?;
    for row in rows {
        let (id, number, verified_at) = row.map_err(|e| classify_sql(&e))?;
        if quarantined.contains(&id) {
            continue;
        }
        let period = i64::from(period_days) * 86_400;
        let is_due = match verified_at
            .as_deref()
            .and_then(|v| Rfc3339Utc::parse(v).ok())
        {
            None => true,
            Some(v) => {
                let jitter = (fnv1a(repo, number) % period.unsigned_abs()) as i64;
                v.epoch() + period + jitter <= now.epoch()
            }
        };
        if is_due {
            due.push((id, number));
        }
    }
    Ok(())
}

fn lookback_floor_str(now: &Rfc3339Utc, cfg: &Config) -> String {
    now.checked_sub_days(cfg.lookback_days)
        .map(|t| t.as_str().to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

/// FNV-1a over (repo, number): a stable, dependency-free hash. RandomState
/// would re-jitter every process and make the schedule (and any golden
/// summary derived from it) nondeterministic.
fn fnv1a(repo: &str, number: i64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in repo.bytes().chain(number.to_le_bytes()) {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ---------------------------------------------------------------------------
// Workers: discovery windows, hydration, retries, re-verify.

fn worker(
    cfg: &Config,
    jobs: &Mutex<std::vec::IntoIter<RepoPlan>>,
    tx: SyncSender<Msg>,
    floor: &AtomicBool,
) {
    let ro = cfg.db_path().ok().and_then(|path| db::open_ro(&path).ok());
    loop {
        let Some(plan) = jobs.lock().expect("job list mutex poisoned").next() else {
            return;
        };
        if floor.load(Ordering::Relaxed) {
            // The run-wide floor tripped before this repo started: defer it
            // whole, zero calls — the summary shows why nothing moved.
            // Every configured stream deferred: the starvation counter is
            // per stream, and both were denied this run.
            let mut deferred = Ok(());
            for stream in streams(&plan) {
                deferred = deferred.and(tx.send(Msg::Deferred {
                    repo: plan.repo.clone(),
                    stream,
                    reset_at: None,
                }));
            }
            let stats = tx.send(Msg::Stats {
                repo: plan.repo,
                tel: gh::Telemetry::default(),
                fetched: 0,
                filtered: 0,
                reverified: 0,
                reverify_shed: 0,
                tail_hits: 0,
                full_walks: 0,
                escalations: BTreeMap::new(),
                bodies_skipped: 0,
            });
            if deferred.is_err() || stats.is_err() {
                return; // writer gone: cancellation
            }
            continue;
        }
        let mut s = StreamCtx {
            cfg,
            plan: &plan,
            tx: &tx,
            floor,
            ro: ro.as_ref(),
            gh: GhCtx::new(cfg.retry_attempts, cfg.retry_budget),
            fetched: 0,
            filtered: 0,
            reverified: 0,
            reverify_shed: 0,
            tail_hits: 0,
            full_walks: 0,
            escalations: BTreeMap::new(),
            bodies_skipped: 0,
            windows_done: 0,
            full_walked: HashSet::new(),
        };
        let end = s.sync_repo();
        let stats = Msg::Stats {
            repo: plan.repo.clone(),
            tel: s.gh.tel.clone(),
            fetched: s.fetched,
            filtered: s.filtered,
            reverified: s.reverified,
            reverify_shed: s.reverify_shed,
            tail_hits: s.tail_hits,
            full_walks: s.full_walks,
            escalations: s.escalations.clone(),
            bodies_skipped: s.bodies_skipped,
        };
        match end {
            Err(Stop::Writer) => return,
            Err(Stop::Floor) => {
                floor.store(true, Ordering::Relaxed);
                let reset = s.gh.tel.reset_at.as_ref().map(|t| t.as_str().to_string());
                // Every configured stream: the floor stopped the RUN, and
                // the module docs' rule is "increments on a deferred or
                // failed run" — a stream that completed before the trip
                // still eats the increment (its Done reset to 0 first, so
                // the count reads "runs since it last finished", which is
                // the starvation the scheduler orders by).
                let mut deferred = Ok(());
                for stream in streams(&plan) {
                    deferred = deferred.and(tx.send(Msg::Deferred {
                        repo: plan.repo.clone(),
                        stream,
                        reset_at: reset.clone(),
                    }));
                }
                if deferred.is_err() {
                    return;
                }
                let _ = tx.send(stats);
            }
            Err(Stop::Repo(error)) => {
                if tx
                    .send(Msg::Failed(plan.repo.clone(), streams(&plan), error))
                    .is_err()
                {
                    return;
                }
                let _ = tx.send(stats);
            }
            Ok(()) => {
                let _ = tx.send(stats);
            }
        }
    }
}

/// Why a repo's walk stopped early.
enum Stop {
    /// Rate-limit floor (or a typed RateExhausted): defer, watermarks hold.
    Floor,
    /// Repo-scoped failure (rename detected, discovery drift, exhausted
    /// stream retries): recorded in the summary, other repos continue.
    Repo(Error),
    /// The writer dropped the receiver: treat send failure as cancellation.
    Writer,
}

struct StreamCtx<'a> {
    cfg: &'a Config,
    plan: &'a RepoPlan,
    tx: &'a SyncSender<Msg>,
    floor: &'a AtomicBool,
    /// The worker's own read-only view of the archive, for the refresh
    /// dispatch gate and baseline. WAL exists exactly so readers coexist
    /// with the writer thread ("read commands stay usable mid-sync",
    /// db.rs); the writer's RW connection stays the only writer. None —
    /// the archive could not be opened read-only — degrades every
    /// hydration to the full walk: correct, costlier, and the writer
    /// surfaces any real archive fault loudly.
    ro: Option<&'a db::RoArchive>,
    gh: GhCtx,
    fetched: u64,
    filtered: u64,
    reverified: u64,
    reverify_shed: u64,
    tail_hits: u64,
    full_walks: u64,
    escalations: BTreeMap<&'static str, u64>,
    bodies_skipped: u64,
    windows_done: u64,
    /// Ids whose hydration this run was a genuine full walk. Re-verify
    /// skips THESE, not `hydrated`: a tail-hit rehydration is an
    /// inference, and letting it stand in for the tier's complete refetch
    /// would disarm the masked-case catcher on exactly the PRs active
    /// enough to be rediscovered every run (B3; the round-0 context's
    /// "catcher unreachable" blocker class).
    full_walked: HashSet<String>,
}

impl StreamCtx<'_> {
    fn sync_repo(&mut self) -> std::result::Result<(), Stop> {
        // Targeted backfill first (old windows), then each stream's main
        // walk, whose final window completes that stream. The writer's
        // max() keeps the backfill's older watermarks from regressing
        // anything. PR stream first (walk order is a determinism choice,
        // recorded at `streams`); a halt in one stream does NOT skip the
        // other — watermarks are per stream and a lost PR tail says
        // nothing about the issue stream — but any halt skips retries and
        // re-verify, the existing posture (a capped unsplittable window is
        // repo-level trouble; spending the budget's remainder on
        // maintenance calls before the operator sees the disclosure buys
        // staleness polish at freshness cost).
        if !self.plan.backfill.is_empty() {
            let start = lookback_start(&Rfc3339Utc::now(), &self.plan.rc, self.cfg);
            let added = self.plan.backfill.clone();
            let completed =
                self.walk_window(&start, None, &TermSource::Backfill(added), Stream::Pr)?;
            if !completed {
                return Ok(()); // halted: discovery_truncated disclosed
            }
        }
        let since = self.plan.since.clone();
        let pr_completed = self.walk_window(&since, None, &TermSource::Full, Stream::Pr)?;
        let issue_completed = match self.plan.issue_since.clone() {
            Some(issue_since) => {
                self.walk_window(&issue_since, None, &TermSource::Full, Stream::Issue)?
            }
            None => true,
        };
        if !(pr_completed && issue_completed) {
            return Ok(());
        }
        self.retry_quarantined()?;
        self.reverify()
    }

    /// The run-wide floor gate, checked before every gh call.
    fn gate(&self) -> std::result::Result<(), Stop> {
        if self.floor.load(Ordering::Relaxed) {
            return Err(Stop::Floor);
        }
        if let Some(remaining) = self.gh.tel.remaining
            && remaining < self.cfg.rate_limit_floor
        {
            return Err(Stop::Floor);
        }
        Ok(())
    }

    fn call(
        &mut self,
        query: &str,
        vars: &[(&str, &str)],
    ) -> std::result::Result<gh::Response, Stop> {
        self.gate()?;
        match gh::graphql(query, vars, &mut self.gh) {
            Ok(resp) => Ok(resp),
            Err(e) if e.kind == FailureKind::RateExhausted => Err(Stop::Floor),
            Err(e) => Err(Stop::Repo(e.error)),
        }
    }

    /// One discovery window, splitting on the search cap. Returns false when
    /// the stream HALTED on an unsplittable truncated window (later windows
    /// would advance the watermark past the lost tail).
    fn walk_window(
        &mut self,
        since: &Rfc3339Utc,
        until: Option<&Rfc3339Utc>,
        terms: &TermSource,
        stream: Stream,
    ) -> std::result::Result<bool, Stop> {
        let d = self.discover(since, until, terms, stream)?;
        if !d.complete
            && let Some(mid) = split_point(since, until)
        {
            // Oldest half first: its Done banks before the newer half runs,
            // so a mid-run stop is a sound checkpoint. Depth is bounded by
            // construction: halving from a lookback-sized window to the
            // MIN_WINDOW_SECS floor is ~22 levels.
            let right_start = Rfc3339Utc::from_epoch(mid.epoch() + 1)
                .expect("split point is far from the representable range");
            if !self.walk_window(since, Some(&mid), terms, stream)? {
                return Ok(false);
            }
            return self.walk_window(&right_start, until, terms, stream);
        }

        self.windows_done += 1;
        self.heartbeat(stream, d.hits.len());

        // Hydration ascends by updatedAt (then id for a total order): the
        // hydrated prefix is contiguous, so a boundary is a checkpoint.
        let mut ordered: Vec<&Hit> = d.hits.values().collect();
        ordered.sort_by(|a, b| a.updated_at.cmp(&b.updated_at).then(a.id.cmp(&b.id)));

        let mut rows: Vec<Bundle> = Vec::new();
        let mut quarantine: Vec<QuarantineRecord> = Vec::new();
        let mut watermark: Option<Rfc3339Utc> = None;
        // Known-equivalent mutant on the comparison (> vs >=), and it
        // stays: at equality the replacement is the same instant, so both
        // orderings fold to the same watermark.
        let extend = |wm: &mut Option<Rfc3339Utc>, t: &Rfc3339Utc| {
            if wm.as_ref().is_none_or(|cur| t > cur) {
                *wm = Some(t.clone());
            }
        };
        for hit in ordered {
            if self.plan.quarantined.contains(&hit.id) {
                // Quarantined: backoff dominates. Its durable row already
                // licenses the watermark to pass it; nothing to do here.
                continue;
            }
            if self.excluded(hit.author.as_ref()) {
                // Filtered is declined, not unfetched: it advances the fold
                // (a bot-only window must still advance) and is counted.
                self.filtered += 1;
                extend(&mut watermark, &hit.updated_at);
                continue;
            }
            // The floor gates every hydration, mid-window included: a
            // deferral here sends no Done, so the watermark holds at the
            // last completed window — the mid-run banking the module docs
            // promise.
            self.gate()?;
            let hydrated = match stream {
                Stream::Pr => self.hydrate(&hit.id, Origin::Discovery)?,
                Stream::Issue => self.hydrate_issue_item(&hit.id, Origin::Discovery)?,
            };
            match hydrated {
                Hydrated::Bundle(bundle) => {
                    // The fold takes the DISCOVERY-time updatedAt — the
                    // value the `updated:` window bounded — not the
                    // hydration-time one, which an item touched between
                    // the two calls can carry past a closed window's right
                    // edge. Unbounded, that overshoot commits a watermark
                    // covering the sibling right window before it runs; a
                    // floor stop then resumes PAST an item whose only home
                    // was that window — the one shape "watermark never
                    // leads data" did not cover (D1 panel). The drifted
                    // item itself is safe either way: its new updatedAt
                    // exceeds the folded one, so the next run's window
                    // rediscovers it and the upsert dedups. Same rule as
                    // the filtered arm below — one fold, one time base.
                    extend(&mut watermark, &hit.updated_at);
                    self.fetched += 1;
                    rows.push(bundle);
                    if rows.len() >= PAGE_BATCH {
                        self.send(Msg::Page {
                            repo: self.plan.repo.clone(),
                            rows: std::mem::take(&mut rows),
                        })?;
                    }
                }
                Hydrated::Quarantine(class) => {
                    quarantine.push(quarantine_record(&hit.id, stream, 1, class));
                }
            }
        }

        let halt = !d.complete; // unsplittable and still capped
        self.send(Msg::Done {
            repo: self.plan.repo.clone(),
            stream,
            witness: WindowComplete(()),
            rows,
            quarantine,
            // A truncated window advances nothing: the lost tail is OLDER
            // than everything seen (sort is updated-desc), so any advance
            // would pass it permanently.
            watermark: if halt { None } else { watermark },
            fingerprint: {
                let stored = match stream {
                    Stream::Pr => &self.plan.stored_fingerprint,
                    Stream::Issue => &self.plan.issue_stored_fingerprint,
                };
                let cold_start = match stream {
                    Stream::Pr => self.plan.cold_start,
                    Stream::Issue => self.plan.issue_cold_start,
                };
                let completes = until.is_none() && matches!(terms, TermSource::Full) && !halt;
                match terms {
                    // A cold start's intermediate windows commit the STORED
                    // inputs; only the completing window claims the new ones
                    // (RepoPlan::cold_start carries the argument). First
                    // contact has nothing stored and writes the new
                    // fingerprint from the first window — its convergence
                    // is the banked watermark, which needs the row.
                    TermSource::Full if cold_start && !completes => stored
                        .clone()
                        .unwrap_or_else(|| self.plan.fingerprint.clone()),
                    TermSource::Full => self.plan.fingerprint.clone(),
                    // See RepoPlan::stored_fingerprint. A backfill only
                    // exists when a stored row does.
                    TermSource::Backfill(_) => stored
                        .clone()
                        .unwrap_or_else(|| self.plan.fingerprint.clone()),
                }
            },
            completes_stream: until.is_none() && matches!(terms, TermSource::Full) && !halt,
            discovery_truncated: u64::from(halt),
            masked: d.masked,
        })?;
        Ok(!halt)
    }

    /// All configured terms over one window, paged to exhaustion, deduped by
    /// id. Complete iff every term's pagination terminated with nodes_seen
    /// covering issueCount — counted BEFORE any filter, or client-side
    /// filters would read incomplete forever on a bot-heavy repo.
    fn discover(
        &mut self,
        since: &Rfc3339Utc,
        until: Option<&Rfc3339Utc>,
        source: &TermSource,
        stream: Stream,
    ) -> std::result::Result<Discovered, Stop> {
        let terms = match source {
            TermSource::Full => queries::discovery_terms(
                &self.plan.rc,
                &self.cfg.viewer,
                &self.cfg.people,
                since,
                until,
                stream,
            ),
            TermSource::Backfill(added) => {
                queries::backfill_terms(&self.plan.rc, added, since, until)
            }
        };
        let mut hits: BTreeMap<String, Hit> = BTreeMap::new();
        let mut complete = true;
        let mut masked: u64 = 0;
        for term in &terms {
            let mut after: Option<String> = None;
            let mut seen: i64 = 0;
            let mut issue_count;
            let mut pages: u32 = 0;
            loop {
                let mut vars: Vec<(&str, &str)> = vec![("q", term.as_str())];
                if let Some(after) = &after {
                    vars.push(("after", after.as_str()));
                }
                let resp = self.call(queries::DISCOVERY, &vars)?;
                let page = parse::discovery(&resp.data).map_err(|e| {
                    Stop::Repo(Error::transient(format!(
                        "discovery failed for {}: {e}",
                        self.plan.repo
                    )))
                })?;
                pages += 1;
                seen += i64::try_from(page.nodes.len()).unwrap_or(0);
                issue_count = page.issue_count;
                for node in page.nodes {
                    match node {
                        None => masked += 1,
                        Some(hit) => {
                            hits.entry(hit.id.clone()).or_insert(Hit {
                                id: hit.id,
                                updated_at: hit.updated_at,
                                author: hit.author,
                            });
                        }
                    }
                }
                if !page.page_info.has_next_page {
                    break;
                }
                match page.page_info.end_cursor {
                    // A non-advancing cursor cannot be walked; treat the
                    // term as capped so the window splits instead of
                    // trusting a stuck page (tested: the stuck-cursor
                    // pipeline case). The page-count backstop beside it is
                    // defense-in-depth against a server that advances
                    // cursors forever — its counter's mutants survive
                    // hermetic tests (the fake terminates) and stay:
                    // exercising it would mean a 40-page fixture chain
                    // proving a bound the cursor guard already witnesses
                    // one level down.
                    Some(c)
                        if after.as_deref() != Some(c.as_str()) && pages < MAX_DISCOVERY_PAGES =>
                    {
                        after = Some(c);
                    }
                    _ => {
                        complete = false;
                        break;
                    }
                }
            }
            if seen < issue_count {
                complete = false;
            }
        }
        Ok(Discovered {
            hits,
            complete,
            masked,
        })
    }

    /// The one filter judgment, on discovery-carried author data: a null
    /// author is ordinary data and never a filter match; bot-ness is
    /// structural; exclude_authors goes through the one login equivalence.
    fn excluded(&self, author: Option<&parse::Author>) -> bool {
        let Some(author) = author else {
            return false;
        };
        if author.is_bot() && !self.plan.rc.bots() {
            return true;
        }
        self.plan
            .rc
            .exclude_authors
            .iter()
            .any(|p| p.matches(author.login.as_str(), &author.typename))
    }

    /// Hydrate one PR, routing through the layered refresh when the
    /// dispatch gate passes. The gate is structural and caller-side
    /// (round-0 obligation): only a discovery-origin rehydration of a PR
    /// with a witnessed, untruncated, undeleted baseline may take the tail
    /// path — first contact full-walks (no baseline row), re-verify
    /// full-walks (it is the named catcher for the check's masked cases,
    /// so routing it through the check would disarm it), retries
    /// full-walk (a quarantined PR's last hydration failed; its baseline
    /// is stale in an unbounded way), and --pr full-walks (it stamps
    /// verified_at, which only a witness may do).
    ///
    /// Cost bound of check-then-escalate, acknowledged: a refresh that
    /// escalates has spent one REFRESH_PR document and at most
    /// TAIL_WALKBACK_PAGES cheap tail pages more than the plain full walk
    /// it then runs. The hit rate that justifies that overhead is exactly
    /// what refresh.tail_hits / full_walks measures.
    fn hydrate(&mut self, id: &str, origin: Origin) -> std::result::Result<Hydrated, Stop> {
        if origin == Origin::Discovery
            && let Some(baseline) = self.refresh_baseline(id)
        {
            match refresh_one(
                &mut self.gh,
                &self.plan.repo,
                id,
                origin,
                self.cfg.rate_limit_floor,
                || self.floor.load(Ordering::Relaxed),
                &baseline,
                &mut self.bodies_skipped,
            ) {
                RefreshEnd::End(end) => {
                    if let HydrateEnd::Bundle(Bundle::Pr(b)) = &end
                        && b.comments == CommentsCompleteness::TailHit
                    {
                        self.tail_hits += 1;
                    }
                    return self.hydrate_end(end);
                }
                RefreshEnd::Escalate(reason) => {
                    // The check could not license the tail: discard the
                    // refresh's view entirely and restart from the top —
                    // never resume from suspect state. Partitioned into
                    // full_walks only (never also tail_hits), with the
                    // reason split kept for the K-sizing diagnosis.
                    self.full_walks += 1;
                    *self.escalations.entry(reason).or_default() += 1;
                }
            }
        }
        let end = hydrate_one(
            &mut self.gh,
            &self.plan.repo,
            id,
            origin,
            self.cfg.rate_limit_floor,
            || self.floor.load(Ordering::Relaxed),
        );
        if matches!(end, HydrateEnd::Bundle(_)) {
            self.full_walked.insert(id.to_string());
        }
        self.hydrate_end(end)
    }

    /// Hydrate one issue by node id: always the full walk — issues have no
    /// refresh/tail path (queries.rs HYDRATE_ISSUE records the cut), so the
    /// dispatch gate has nothing to decide here. Same outcome discipline as
    /// [`Self::hydrate`], and successful walks enter `full_walked` so the
    /// re-verify tier skips them identically.
    fn hydrate_issue_item(
        &mut self,
        id: &str,
        origin: Origin,
    ) -> std::result::Result<Hydrated, Stop> {
        let end = hydrate_issue_one(
            &mut self.gh,
            &self.plan.repo,
            id,
            origin,
            self.cfg.rate_limit_floor,
            || self.floor.load(Ordering::Relaxed),
        );
        if matches!(end, HydrateEnd::Bundle(_)) {
            self.full_walked.insert(id.to_string());
        }
        self.hydrate_end(end)
    }

    /// Map a walk's terminal outcome onto the stream's typed results —
    /// shared by the full walk and the refresh so both classify at the
    /// same call site.
    fn hydrate_end(&self, end: HydrateEnd) -> std::result::Result<Hydrated, Stop> {
        match end {
            HydrateEnd::Bundle(b) => Ok(Hydrated::Bundle(b)),
            HydrateEnd::Vanished => Ok(Hydrated::Quarantine("node_null")),
            HydrateEnd::ParseDrift => Ok(Hydrated::Quarantine("parse")),
            HydrateEnd::Retryable => Ok(Hydrated::Quarantine("transient")),
            HydrateEnd::Renamed => Err(Stop::Repo(Error::config(format!(
                "repo {}: a hydrated item reports a different repository — renamed or \
                 transferred upstream; update the config entry",
                self.plan.repo
            )))),
            HydrateEnd::RateExhausted => Err(Stop::Floor),
            HydrateEnd::Fatal(error) => Err(Stop::Repo(error)),
        }
    }

    /// Read the archived state the refresh inducts from, gating as it
    /// goes. `None` means "no tail path" — no RO connection, no row, no
    /// witnessed baseline (verified_at NULL), stored truncation, a
    /// soft-deleted row, or an unreadable archive — and the caller
    /// full-walks, the strictly-more-complete form. Conflating a read
    /// error with absence is deliberate: the degradation is correctness-
    /// neutral (cost only), and a genuinely broken archive fails loudly
    /// in the writer, which holds the RW connection.
    fn refresh_baseline(&self, id: &str) -> Option<RefreshBaseline> {
        let conn = self.ro?.conn();
        let (pk, verified_at, truncated, deleted_at): (i64, Option<String>, i64, Option<String>) =
            conn.query_row(
                "SELECT pk, verified_at, truncated, deleted_at FROM prs \
                 WHERE repo = ?1 AND id = ?2",
                rusqlite::params![self.plan.repo, id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .ok()?;
        if verified_at.is_none() || truncated != 0 || deleted_at.is_some() {
            return None;
        }
        // The archived side of the counting universe: LIVE rows only
        // (deleted_at IS NULL) — a soft-deleted row is not in GitHub's
        // totalCount, and a tail id that resurrects one counts as new on
        // both sides of the equation. Minimized rows are counted: the
        // universe the enablement fixture pins (parse.rs,
        // minimized_comments_count_in_total_count).
        let mut live_comments = HashSet::new();
        let mut thread_comments = HashMap::new();
        let mut stmt = conn
            .prepare(
                "SELECT id, kind, updated_at, body FROM comments \
                 WHERE parent_kind = 'pr' AND parent = ?1 \
                   AND kind IN ('comment', 'review_comment') \
                   AND deleted_at IS NULL",
            )
            .ok()?;
        let rows = stmt
            .query_map([pk], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .ok()?;
        for row in rows {
            let (cid, kind, updated_at, body) = row.ok()?;
            if kind == "comment" {
                live_comments.insert(cid);
            } else {
                thread_comments.insert(cid, (updated_at, body));
            }
        }
        Some(RefreshBaseline {
            live_comments,
            thread_comments,
        })
    }

    fn retry_quarantined(&mut self) -> std::result::Result<(), Stop> {
        if self.plan.quarantine_due.is_empty() {
            return Ok(());
        }
        let now = Rfc3339Utc::now();
        let mut resolved = Vec::new();
        let mut requeued = Vec::new();
        let mut drained = Vec::new();
        for (id, attempts, stream) in &self.plan.quarantine_due {
            // The floor gates each retry; undone retries simply stay due —
            // their rows are durable, no Deferred bookkeeping needed.
            if self.gate().is_err() {
                break;
            }
            let hydrated = match stream {
                Stream::Pr => self.hydrate(id, Origin::Retry)?,
                Stream::Issue => self.hydrate_issue_item(id, Origin::Retry)?,
            };
            match hydrated {
                Hydrated::Bundle(bundle) => {
                    self.fetched += 1;
                    resolved.push(bundle);
                }
                Hydrated::Quarantine(class) => {
                    let next = attempts + 1;
                    if class == "node_null" && next >= QUARANTINE_DRAIN_ATTEMPTS {
                        drained.push((id.clone(), *stream));
                    } else {
                        requeued.push(quarantine_record_at(&now, id, *stream, next, class));
                    }
                }
            }
        }
        self.send(Msg::Retries {
            repo: self.plan.repo.clone(),
            resolved,
            requeued,
            drained,
        })
    }

    fn reverify(&mut self) -> std::result::Result<(), Stop> {
        let mut rows: Vec<Bundle> = Vec::new();
        let mut requeued: Vec<QuarantineRecord> = Vec::new();
        let now = Rfc3339Utc::now();
        for (i, (id, _number, stream)) in self.plan.reverify.iter().enumerate() {
            if self.full_walked.contains(id) {
                // Discovery already gave it a complete refetch. `hydrated`
                // would be the wrong set here: a tail-hit rehydration is
                // an inference, and the tier's whole job is the complete
                // refetch that catches what inference masks.
                continue;
            }
            if self.gate().is_err() {
                // Re-verify sheds first at the floor, and the shed volume
                // is counted — it is the deferrable tier by design.
                self.reverify_shed += (self.plan.reverify.len() - i) as u64;
                break;
            }
            let hydrated = match stream {
                Stream::Pr => self.hydrate(id, Origin::Reverify)?,
                Stream::Issue => self.hydrate_issue_item(id, Origin::Reverify)?,
            };
            match hydrated {
                Hydrated::Bundle(bundle) => {
                    self.reverified += 1;
                    self.fetched += 1;
                    rows.push(bundle);
                    if rows.len() >= PAGE_BATCH {
                        self.send(Msg::Page {
                            repo: self.plan.repo.clone(),
                            rows: std::mem::take(&mut rows),
                        })?;
                    }
                }
                Hydrated::Quarantine(class) => {
                    requeued.push(quarantine_record_at(&now, id, *stream, 1, class));
                }
            }
        }
        if !rows.is_empty() {
            self.send(Msg::Page {
                repo: self.plan.repo.clone(),
                rows,
            })?;
        }
        if !requeued.is_empty() {
            self.send(Msg::Retries {
                repo: self.plan.repo.clone(),
                resolved: Vec::new(),
                requeued,
                drained: Vec::new(),
            })?;
        }
        Ok(())
    }

    fn send(&self, msg: Msg) -> std::result::Result<(), Stop> {
        self.tx.send(msg).map_err(|_| Stop::Writer)
    }

    /// Operators kill healthy multi-hour first runs; say what is happening.
    /// stderr stays non-contract — which is also why mutants on this
    /// counter and format survive mutation testing, and stay: a test
    /// asserting heartbeat text would promote noise space into contract.
    fn heartbeat(&self, stream: Stream, window_items: usize) {
        let points = match self.gh.tel.remaining {
            Some(r) => r.to_string(),
            None => "?".to_string(),
        };
        eprintln!(
            "ghgraph: {}: {} window {} ({window_items} items), {points} points remaining",
            self.plan.repo,
            stream.as_str(),
            self.windows_done
        );
    }
}

enum TermSource {
    Full,
    Backfill(Vec<Login>),
}

struct Discovered {
    hits: BTreeMap<String, Hit>,
    complete: bool,
    masked: u64,
}

struct Hit {
    id: String,
    updated_at: Rfc3339Utc,
    author: Option<parse::Author>,
}

enum Hydrated {
    Bundle(Bundle),
    /// Quarantine with this error class; the caller owns attempts/backoff.
    Quarantine(&'static str),
}

/// Halve a window; None when it is too narrow to split (the search grammar
/// is second-granular). An open right edge splits against "now".
/// Known-equivalent mutants, and they stay: the split boundary arithmetic
/// (the +1 on the right half's start, the MIN_WINDOW comparison's exact
/// edge) tolerates any perturbation that produces OVERLAP — upserts make
/// overlap free, exactly like the deliberate 10-minute watermark overlap —
/// and no ±1/×1 mutant of a midpoint can produce a GAP, which is the only
/// wrong direction. Only the halving itself (progress toward the floor)
/// is load-bearing, and a non-halving mutant times out against the
/// recursion.
fn split_point(since: &Rfc3339Utc, until: Option<&Rfc3339Utc>) -> Option<Rfc3339Utc> {
    let end = match until {
        Some(u) => u.clone(),
        None => Rfc3339Utc::now(),
    };
    let width = end.epoch() - since.epoch();
    if width < MIN_WINDOW_SECS {
        return None;
    }
    Rfc3339Utc::from_epoch(since.epoch() + width / 2)
}

fn quarantine_record(id: &str, stream: Stream, attempts: u32, class: &str) -> QuarantineRecord {
    quarantine_record_at(&Rfc3339Utc::now(), id, stream, attempts, class)
}

fn quarantine_record_at(
    now: &Rfc3339Utc,
    id: &str,
    stream: Stream,
    attempts: u32,
    class: &str,
) -> QuarantineRecord {
    let backoff = QUARANTINE_BACKOFF_BASE_SECS
        .saturating_mul(1u64 << (attempts.saturating_sub(1)).min(20))
        .min(QUARANTINE_BACKOFF_CAP_SECS);
    let next = Rfc3339Utc::from_epoch(now.epoch().saturating_add(backoff as i64))
        .unwrap_or_else(|| now.clone());
    QuarantineRecord {
        id: id.to_string(),
        stream,
        attempts,
        next_retry_at: next.as_str().to_string(),
        error_class: class.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Hydration: one PR, full walk, witnesses earned here and nowhere else.

enum HydrateEnd {
    Bundle(Bundle),
    /// node: null — the id no longer resolves (deleted or access lost).
    Vanished,
    /// The response did not match the parse type: quarantine class 'parse',
    /// the disclosed, retried outcome for schema drift on one PR.
    ParseDrift,
    /// Transient failure that exhausted its retries: quarantine.
    Retryable,
    /// repository.nameWithOwner disagrees with config: rename/transfer.
    Renamed,
    RateExhausted,
    /// Operator-fixable transport failure: abort the repo.
    Fatal(Error),
}

/// Hydrate one PR by node id: the first-page document, then follow-up pages
/// for the two big connections. Every completeness claim in the returned
/// bundle is earned by pagination TERMINATING (ids suffice; bodies are not
/// part of completeness); any failure or backstop along a connection's walk
/// withholds that connection's witness and the bundle lands truncated —
/// never silently smaller. `floor_tripped` lets a run-wide floor stop
/// follow-up paging between calls; the partial connection simply loses its
/// witness.
fn hydrate_one(
    gh_ctx: &mut GhCtx,
    repo: &str,
    id: &str,
    origin: Origin,
    floor: u32,
    floor_tripped: impl Fn() -> bool,
) -> HydrateEnd {
    // Between follow-up pages the budget check reads this call context's
    // own telemetry (the run-wide flag arrives via the closure): a floor
    // trip mid-walk stops paging and the connection simply loses its
    // witness — truncated, disclosed, never wedged.
    let floor_hit = |ctx: &GhCtx| ctx.tel.remaining.is_some_and(|r| r < floor) || floor_tripped();
    let resp = match gh::graphql(queries::HYDRATE_PR, &[("id", id)], gh_ctx) {
        Ok(resp) => resp,
        Err(e) => return hydrate_failure(e),
    };
    let mut node = match parse::hydrate_pr(&resp.data) {
        Err(_) => return HydrateEnd::ParseDrift,
        Ok(None) => return HydrateEnd::Vanished,
        Ok(Some(node)) => node,
    };
    // Rename detection: the PR's own view of its repo, case-folded
    // (queries.rs records why), against the canonical config key.
    if node.repository.name_with_owner.to_ascii_lowercase() != repo {
        return HydrateEnd::Renamed;
    }

    // Follow-up pages for the top-level comments connection.
    let mut comments_complete = !node.comments.page_info.has_next_page;
    let mut cursor = node.comments.page_info.end_cursor.clone();
    let mut pages: u32 = 0;
    while !comments_complete {
        if floor_hit(gh_ctx) || pages >= MAX_CONNECTION_PAGES {
            break;
        }
        let Some(after) = cursor.clone() else { break };
        let resp = match gh::graphql(
            queries::COMMENTS_PAGE,
            &[("id", id), ("after", &after)],
            gh_ctx,
        ) {
            Ok(resp) => resp,
            Err(_) => break, // witness withheld; the bundle lands truncated
        };
        let page = match parse::comments_page(&resp.data) {
            Ok(Some(page)) => page,
            // Vanished or drifted mid-walk: keep what we have, no witness.
            Ok(None) | Err(_) => break,
        };
        pages += 1;
        node.comments.nodes.extend(page.comments.nodes);
        comments_complete = !page.comments.page_info.has_next_page;
        match page.comments.page_info.end_cursor {
            Some(c) if Some(&c) != cursor.as_ref() => cursor = Some(c),
            _ if comments_complete => {}
            _ => break, // non-advancing cursor: stop, no witness
        }
    }

    // Follow-up pages for reviewThreads.
    let mut threads_paged = !node.review_threads.page_info.has_next_page;
    let mut cursor = node.review_threads.page_info.end_cursor.clone();
    let mut pages: u32 = 0;
    while !threads_paged {
        if floor_hit(gh_ctx) || pages >= MAX_CONNECTION_PAGES {
            break;
        }
        let Some(after) = cursor.clone() else { break };
        let resp = match gh::graphql(
            queries::THREADS_PAGE,
            &[("id", id), ("after", &after)],
            gh_ctx,
        ) {
            Ok(resp) => resp,
            Err(_) => break,
        };
        let page = match parse::threads_page(&resp.data) {
            Ok(Some(page)) => page,
            Ok(None) | Err(_) => break,
        };
        pages += 1;
        node.review_threads.nodes.extend(page.review_threads.nodes);
        threads_paged = !page.review_threads.page_info.has_next_page;
        match page.review_threads.page_info.end_cursor {
            Some(c) if Some(&c) != cursor.as_ref() => cursor = Some(c),
            _ if threads_paged => {}
            _ => break,
        }
    }
    // Thread completeness includes every thread's nested comments: the
    // first:30 selection has no follow-up document yet (module docs — the
    // deferred tail), so an overflowing thread withholds the witness.
    let threads_complete = threads_paged
        && node.review_threads.nodes.iter().all(|t| {
            i64::try_from(t.comments.nodes.len()).is_ok_and(|n| n >= t.comments.total_count)
        });

    let requests_complete = counted_complete(&node.review_requests);
    let reviews_complete = counted_complete(&node.latest_opinionated_reviews);
    let closing_complete = counted_complete(&node.closing_issues_references);

    let body_refs = refs::extract(&node.body, repo).unwrap_or_default();

    HydrateEnd::Bundle(Bundle::Pr(Box::new(PrBundle {
        repo: repo.to_string(),
        pr: node,
        refs: body_refs,
        origin,
        // The full walk is two-state: it witnesses or it doesn't. TailHit
        // is constructible only by the refresh path's conservation check.
        comments: if comments_complete {
            CommentsCompleteness::Complete
        } else {
            CommentsCompleteness::Incomplete
        },
        threads_complete,
        requests_complete,
        reviews_complete,
        closing_complete,
    })))
}

/// Hydrate one issue by node id: the first-page document, then follow-up
/// pages for the comments connection. The same witness discipline as
/// [`hydrate_one`]: every completeness claim is earned by pagination
/// TERMINATING; any failure or backstop withholds the witness and the row
/// lands truncated — never silently smaller. The two small counted
/// connections (labels, assignees) have no follow-up document (queries.rs
/// records the cut), so nodes covering totalCount IS their termination.
fn hydrate_issue_one(
    gh_ctx: &mut GhCtx,
    repo: &str,
    id: &str,
    origin: Origin,
    floor: u32,
    floor_tripped: impl Fn() -> bool,
) -> HydrateEnd {
    let floor_hit = |ctx: &GhCtx| ctx.tel.remaining.is_some_and(|r| r < floor) || floor_tripped();
    let resp = match gh::graphql(queries::HYDRATE_ISSUE, &[("id", id)], gh_ctx) {
        Ok(resp) => resp,
        Err(e) => return hydrate_failure(e),
    };
    let mut node = match parse::hydrate_issue(&resp.data) {
        Err(_) => return HydrateEnd::ParseDrift,
        Ok(None) => return HydrateEnd::Vanished,
        Ok(Some(node)) => node,
    };
    // Rename detection: the issue's own view of its repo, case-folded
    // (queries.rs records why), against the canonical config key.
    if node.repository.name_with_owner.to_ascii_lowercase() != repo {
        return HydrateEnd::Renamed;
    }

    // Follow-up pages for the comments connection — the same walk as
    // hydrate_one's top-level comments, against the issue-rooted document.
    let mut comments_complete = !node.comments.page_info.has_next_page;
    let mut cursor = node.comments.page_info.end_cursor.clone();
    let mut pages: u32 = 0;
    while !comments_complete {
        if floor_hit(gh_ctx) || pages >= MAX_CONNECTION_PAGES {
            break;
        }
        let Some(after) = cursor.clone() else { break };
        let resp = match gh::graphql(
            queries::ISSUE_COMMENTS_PAGE,
            &[("id", id), ("after", &after)],
            gh_ctx,
        ) {
            Ok(resp) => resp,
            Err(_) => break, // witness withheld; the row lands truncated
        };
        let page = match parse::issue_comments_page(&resp.data) {
            Ok(Some(page)) => page,
            // Vanished or drifted mid-walk: keep what we have, no witness.
            Ok(None) | Err(_) => break,
        };
        pages += 1;
        node.comments.nodes.extend(page.comments.nodes);
        comments_complete = !page.comments.page_info.has_next_page;
        // Known-equivalent mutants here, and they stay: `pages` feeds only
        // the MAX_CONNECTION_PAGES backstop (defense-in-depth against a
        // server advancing cursors forever — the non-advancing guard below
        // is the live witness one level down, same argument as discovery's
        // page counter), and the second arm's guard only chooses between
        // exiting via {} and exiting via break when the walk is already
        // complete — every input converges to the same state either way.
        match page.comments.page_info.end_cursor {
            Some(c) if Some(&c) != cursor.as_ref() => cursor = Some(c),
            _ if comments_complete => {}
            _ => break, // non-advancing cursor: stop, no witness
        }
    }

    // labels is schema-nullable (error-masked; parse.rs), so it takes the
    // same judgment as the PR's three nullable connections; assignees is
    // non-null and judges its own coverage.
    let labels_complete = counted_complete(&node.labels);
    let assignees_complete =
        i64::try_from(node.assignees.nodes.len()).is_ok_and(|n| n >= node.assignees.total_count);

    HydrateEnd::Bundle(Bundle::Issue(Box::new(IssueBundle {
        repo: repo.to_string(),
        issue: node,
        origin,
        comments_complete,
        labels_complete,
        assignees_complete,
    })))
}

/// Present (not error-masked) and complete (every node the count claims).
/// The judgment on parse::Counted the hydrator owns (parse.rs carries, this
/// decides).
fn counted_complete<T>(c: &Option<parse::Counted<T>>) -> bool {
    // An unrepresentable length fails CLOSED (no witness): this gates
    // sweeps, and the permissive direction was the alarming one even while
    // physically unreachable (B2 panel, S5).
    c.as_ref()
        .is_some_and(|c| i64::try_from(c.nodes.len()).is_ok_and(|n| n >= c.total_count))
}

fn hydrate_failure(e: gh::GhError) -> HydrateEnd {
    match e.kind {
        FailureKind::RateExhausted => HydrateEnd::RateExhausted,
        FailureKind::Config => HydrateEnd::Fatal(e.error),
        FailureKind::SecondaryLimit | FailureKind::Watchdog | FailureKind::Other => {
            HydrateEnd::Retryable
        }
    }
}

// ---------------------------------------------------------------------------
// The layered refresh: tail-first comments under count conservation,
// skeleton-walked threads with archive-resolved bodies. DESIGN.md's refresh
// section carries the argument; the two masked cases and their catcher are
// documented at tail_conservation below.

/// Walk-back bound for the tail: pages of TAIL_K fetched beyond the first
/// while hunting the induction anchor (id overlap with the archived set).
/// (1 + TAIL_WALKBACK_PAGES) × TAIL_K = 60 newest comments; a PR that
/// gained more than that between two syncs deserves the full walk the
/// escalation buys, and the bound is what keeps check-then-escalate within
/// a constant of the plain walk (see the dispatch's cost note).
const TAIL_WALKBACK_PAGES: u32 = 2;

/// The archived state one refresh inducts from, read through the worker's
/// RO connection at dispatch time. The read cannot race a write to the
/// same PR: repos are worker-exclusive, the run lock excludes other
/// syncs, and within a repo the only hydration that can FOLLOW a tail hit
/// of the same PR is the re-verify full walk, which reads no baseline.
/// (Windows dedupe among themselves; retries exclude quarantined ids from
/// windows entirely.)
struct RefreshBaseline {
    /// Live (deleted_at IS NULL) top-level comment ids — the archived
    /// side of the conservation equation, minimized rows included.
    live_comments: HashSet<String>,
    /// Live review-thread comment id → stored (updated_at, body): the
    /// body-skip signal and the text that replaces the un-fetched body
    /// when the signal says "unmoved".
    thread_comments: HashMap<String, (Option<String>, String)>,
}

/// A refresh attempt's outcome: a terminal end (bundle, vanish, transport
/// failure — the same shapes the full walk produces, classified at the
/// same call site), or an escalation to the full walk. Escalation is not
/// an error: it is the check declining to license the skip.
enum RefreshEnd {
    End(HydrateEnd),
    /// The reason names the arm for the summary's per-reason escalation
    /// counters (refresh.escalations): the flat tail_hits/full_walks
    /// ratio sizes K, but only the reason split says WHICH response
    /// raising K would change — "no induction anchor" means the tail is
    /// too short, "count imbalance" means deletion traffic no K fixes,
    /// "duplicate tail id"/"non-advancing cursor" mean the API is
    /// unstable under the walk (B3 panel, S1).
    Escalate(&'static str),
}

/// The conservation check's verdict. The reason string names the arm for
/// tests and future disclosure; it never reaches output today.
#[derive(Debug, PartialEq, Eq)]
enum TailVerdict {
    Hit,
    Escalate(&'static str),
}

/// The count-conservation decision, pure so it can be enumerated against a
/// model. Inputs: the archived live top-level id set, the totalCount all
/// tail pages agreed on (a moved count escalates before this is called),
/// the walked tail's ids (ascending, all pages), and whether the walk
/// reached the connection's start (`fully_observed` — the oldest page had
/// hasPreviousPage == false).
///
/// The inference: stable creation order makes the un-fetched region a
/// PREFIX of the connection. If some tail id is archived (the anchor) and
/// |archived_live| + |tail ∖ archived_live| == totalCount, the prefix is
/// exactly the archived remainder — nothing was added or deleted there.
/// Preconditions, each used here or upstream: minimized rows count in
/// totalCount (enablement fixture); the archived side counts live rows
/// only; count and tail come from one document per page; top-level
/// comments only (a review-thread reply mutates an OLD thread — the count
/// balances while the archive is wrong — and pending-review submission
/// inserts middle-positioned thread comments, so stable creation order
/// does not even hold there).
///
/// Zero overlap with an un-fetched middle has NO induction anchor and
/// escalates regardless of balance (round-0 obligation). One refinement,
/// decided here: when the walk reached the connection's start there IS no
/// un-fetched middle — the check degenerates to direct observation, where
/// balance alone proves archived ⊆ fetched (|fetched ∩ archived| =
/// totalCount − |new| = |archived|) — so a fully-observed balanced tail
/// hits without an anchor. This is the arm that keeps zero-comment and
/// first-comment PRs on the cheap path; it inducts over nothing. If the
/// panel refutes the degenerate-case argument, tighten this arm, not the
/// anchor rule.
///
/// The two masked cases, disclosed (DESIGN.md refresh): (1) a deletion
/// offset by an equal count of non-tail-visible adds inside one window —
/// REFINED by the model enumeration below: under the stated creation-
/// order precondition an anchored suffix covers every appended add, so
/// this shape is unreachable and the id arithmetic is EXACT; it survives
/// as the disclosed blast radius of an order violation (an imported or
/// backdated comment landing mid-connection), where the enumeration
/// confines every false pass to exactly this shape; (2) a body edit to a
/// top-level comment in the un-fetched middle (the count is conserved
/// and the ids overlap). Both fall to the re-verify tier — unconditional
/// for OPEN PRs, within lookback of closed_at/merged_at for
/// CLOSED/MERGED, beyond which a masked case is permanently stale, the
/// same accepted scope limit as closed-tier discovery.
fn tail_conservation(
    archived_live: &HashSet<String>,
    total_count: i64,
    tail_ids: &[&str],
    fully_observed: bool,
) -> TailVerdict {
    if total_count < 0 {
        return TailVerdict::Escalate("negative totalCount");
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut new: i64 = 0;
    let mut anchored = false;
    for id in tail_ids {
        if !seen.insert(id) {
            // A duplicate across pages means the connection reordered
            // under the walk: the pages are not one snapshot.
            return TailVerdict::Escalate("duplicate tail id");
        }
        if archived_live.contains(*id) {
            anchored = true;
        } else {
            new += 1;
        }
    }
    let Ok(archived) = i64::try_from(archived_live.len()) else {
        return TailVerdict::Escalate("unrepresentable archive size");
    };
    let balanced = archived.checked_add(new) == Some(total_count);
    match (fully_observed, anchored, balanced) {
        (true, _, true) => TailVerdict::Hit,
        (false, true, true) => TailVerdict::Hit,
        (false, false, _) => TailVerdict::Escalate("no induction anchor"),
        (_, _, false) => TailVerdict::Escalate("count imbalance"),
    }
}

/// One layered refresh: REFRESH_PR, the tail walk-back, the conservation
/// verdict, the skeleton thread walk, and body resolution. Every arm where
/// the check cannot run or cannot be trusted degrades AWAY from the skip:
/// a moved count, a missing anchor, an imbalance, a duplicate id, or a
/// REFRESH parse drift escalates to the full walk (RefreshEnd::Escalate —
/// note drift here is NOT the parse-class quarantine: only the full walk's
/// own document proves schema drift against the PR); a floor trip or
/// transport wobble mid-walk keeps what was fetched and lands the bundle
/// Incomplete, exactly like the full walk's partial arm — never a skip.
///
/// A returned bundle's comment nodes are the TAIL ONLY (plus thread
/// comments); the writer's diff gate makes re-upserting the already-
/// archived overlap free, and the un-fetched middle is never visited —
/// which is why a TailHit can neither sweep nor stamp (PrBundle docs).
#[allow(clippy::too_many_arguments)]
fn refresh_one(
    gh_ctx: &mut GhCtx,
    repo: &str,
    id: &str,
    origin: Origin,
    floor: u32,
    floor_tripped: impl Fn() -> bool,
    baseline: &RefreshBaseline,
    bodies_skipped: &mut u64,
) -> RefreshEnd {
    let floor_hit = |ctx: &GhCtx| ctx.tel.remaining.is_some_and(|r| r < floor) || floor_tripped();
    let resp = match gh::graphql(&queries::refresh_pr_document(), &[("id", id)], gh_ctx) {
        Ok(resp) => resp,
        Err(e) => return RefreshEnd::End(hydrate_failure(e)),
    };
    let node = match parse::refresh_pr(&resp.data) {
        Err(_) => return RefreshEnd::Escalate("refresh parse drift"),
        Ok(None) => return RefreshEnd::End(HydrateEnd::Vanished),
        Ok(Some(node)) => node,
    };
    if node.repository.name_with_owner.to_ascii_lowercase() != repo {
        return RefreshEnd::End(HydrateEnd::Renamed);
    }

    // The tail walk-back: pages accumulate (ascending; older pages
    // prepend) until an anchor exists, the connection's start is reached,
    // or a bound stops the hunt. Count and tail arrive in ONE document on
    // every iteration; a count that moved between pages is a live
    // mutation mid-walk and escalates — restart from the top, never
    // resume across the seam.
    let total = node.comments.total_count;
    let mut tail = node.comments.nodes;
    let mut has_prev = node.comments.page_info.has_previous_page;
    let mut cursor = node.comments.page_info.start_cursor.clone();
    let mut pages: u32 = 0;
    // `aborted`: the floor or a transport wobble stopped the walk-back
    // BEFORE a verdict; the bundle lands Incomplete (witness-free), the
    // same shape as the full walk's partial arm. Post-verdict failures
    // (the thread phase) never touch this: comments completeness
    // reflects the comments connection alone, and a thread-phase floor
    // or transport failure withholds the THREADS witness instead (B3
    // panel, S2 — the earlier draft demoted a concluded Hit to
    // Incomplete on a thread-phase floor, which made the accounting
    // asymmetric with the transport arm below).
    let mut aborted = false;
    while has_prev && !tail.iter().any(|c| baseline.live_comments.contains(&c.id)) {
        if pages >= TAIL_WALKBACK_PAGES {
            // Anchor not found within the bound: no induction base.
            return RefreshEnd::Escalate("no anchor within walk-back bound");
        }
        if floor_hit(gh_ctx) {
            aborted = true;
            break;
        }
        let Some(before) = cursor.clone() else {
            // hasPreviousPage with no startCursor is schema nonsense;
            // treat it as an untrustworthy snapshot.
            return RefreshEnd::Escalate("missing walk-back cursor");
        };
        let resp = match gh::graphql(
            &queries::tail_comments_document(),
            &[("id", id), ("before", &before)],
            gh_ctx,
        ) {
            Ok(resp) => resp,
            Err(_) => {
                aborted = true;
                break;
            }
        };
        let page = match parse::tail_comments(&resp.data) {
            Ok(Some(page)) => page,
            // Vanished or drifted mid-walk: the snapshot is suspect.
            Ok(None) | Err(_) => return RefreshEnd::Escalate("tail page vanished or drifted"),
        };
        if page.comments.total_count != total {
            return RefreshEnd::Escalate("count moved between pages");
        }
        pages += 1;
        has_prev = page.comments.page_info.has_previous_page;
        // Guard-mutant note (kin of the B2 cursor-guard survivors): with
        // the middle arm's guard forced true, a non-advancing terminal
        // page nulls the cursor and the next iteration escalates through
        // the `let Some` — same verdict, same call count, so the mutant
        // is observationally equivalent; the arms stay split for the
        // costlier non-terminal case, which the non-advancing-cursor
        // pipeline test pins by exact subprocess count.
        match page.comments.page_info.start_cursor {
            Some(c) if Some(&c) != cursor.as_ref() => cursor = Some(c),
            _ if !has_prev => cursor = None,
            _ => return RefreshEnd::Escalate("non-advancing cursor"),
        }
        let mut walked = page.comments.nodes;
        walked.extend(tail);
        tail = walked;
    }

    if !aborted {
        let tail_ids: Vec<&str> = tail.iter().map(|c| c.id.as_str()).collect();
        match tail_conservation(&baseline.live_comments, total, &tail_ids, !has_prev) {
            TailVerdict::Hit => {}
            TailVerdict::Escalate(reason) => return RefreshEnd::Escalate(reason),
        }
    }

    // The skeleton thread walk: same loop discipline as the full walk's
    // (backstop, floor, non-advancing-cursor guard); a stopped walk
    // withholds the threads witness, never shrinks silently.
    let threads_total = node.review_threads.total_count;
    let mut skeletons = node.review_threads.nodes;
    let mut threads_paged = !node.review_threads.page_info.has_next_page;
    let mut cursor = node.review_threads.page_info.end_cursor.clone();
    let mut pages: u32 = 0;
    // Mutant notes, same classes B2 documented at hydrate_one's walks:
    // the MAX_CONNECTION_PAGES half of the guard and the `pages` counter
    // it consumes are a 100-page backstop no fixture reaches (their
    // mutants are equivalent below that bound); the cursor-guard arms
    // below degrade a pathological repeating-cursor remote to a withheld
    // witness either way. The floor half is pinned by the
    // skeleton-floor pipeline test.
    while !threads_paged {
        if floor_hit(gh_ctx) || pages >= MAX_CONNECTION_PAGES {
            break;
        }
        let Some(after) = cursor.clone() else { break };
        let resp = match gh::graphql(
            queries::SKELETON_THREADS_PAGE,
            &[("id", id), ("after", &after)],
            gh_ctx,
        ) {
            Ok(resp) => resp,
            Err(_) => break,
        };
        let page = match parse::skeleton_threads_page(&resp.data) {
            Ok(Some(page)) => page,
            Ok(None) | Err(_) => break,
        };
        pages += 1;
        skeletons.extend(page.review_threads.nodes);
        threads_paged = !page.review_threads.page_info.has_next_page;
        match page.review_threads.page_info.end_cursor {
            Some(c) if Some(&c) != cursor.as_ref() => cursor = Some(c),
            _ if threads_paged => {}
            _ => break,
        }
    }

    // Body resolution, per thread: a thread whose every nested comment is
    // known (id archived live) with an unmoved edit signal (lastEditedAt
    // == stored updated_at) — and whose selection covered its count —
    // resolves from the archive, no call; anything else refetches the
    // WHOLE thread with bodies (the newer snapshot replaces the
    // skeleton's view). A refetch that fails or comes back short just
    // withholds the threads witness via the ordinary count rule.
    let mut threads: Vec<parse::ThreadNode> = Vec::with_capacity(skeletons.len());
    for sk in skeletons {
        let covered =
            i64::try_from(sk.comments.nodes.len()).is_ok_and(|n| n >= sk.comments.total_count);
        let resolvable = covered
            && sk.comments.nodes.iter().all(|c| {
                baseline
                    .thread_comments
                    .get(&c.id)
                    .is_some_and(|(stored_edit, _)| {
                        stored_edit.as_deref() == c.last_edited_at.as_ref().map(|t| t.as_str())
                    })
            });
        if resolvable {
            *bodies_skipped += sk.comments.nodes.len() as u64;
            threads.push(thread_from_skeleton(sk, baseline));
            continue;
        }
        if floor_hit(gh_ctx) {
            // Same degradation as the transport arm below: the thread
            // keeps its cheap fields, loses its comments, and the
            // withheld threads witness lands the row truncated. The
            // comments verdict (already concluded) stands.
            threads.push(thread_without_comments(sk));
            continue;
        }
        let refetched = gh::graphql(queries::THREAD_BODIES, &[("id", &sk.id)], gh_ctx)
            .ok()
            .and_then(|resp| parse::thread_bodies(&resp.data).ok().flatten())
            .filter(|tb| tb.id == sk.id);
        match refetched {
            Some(tb) => threads.push(parse::ThreadNode {
                id: sk.id,
                is_resolved: sk.is_resolved,
                is_outdated: sk.is_outdated,
                path: sk.path,
                line: sk.line,
                comments: tb.comments,
            }),
            // Vanished/drifted/failed: keep the thread's cheap fields,
            // drop its comments — total_count > 0 with zero nodes is what
            // withholds the witness below. (A thread that vanished with
            // total 0 reads complete-and-empty, which is also true.)
            None => threads.push(thread_without_comments(sk)),
        }
    }

    let threads_complete = threads_paged
        && threads.iter().all(|t| {
            i64::try_from(t.comments.nodes.len()).is_ok_and(|n| n >= t.comments.total_count)
        });
    let requests_complete = counted_complete(&node.review_requests);
    let reviews_complete = counted_complete(&node.latest_opinionated_reviews);
    let closing_complete = counted_complete(&node.closing_issues_references);
    let body_refs = refs::extract(&node.body, repo).unwrap_or_default();

    // page_info on the assembled connections is fabricated-inert: it is
    // pagination transport, consumed above, never read by the writer.
    let done = parse::PageInfo {
        has_next_page: false,
        end_cursor: None,
    };
    let pr = parse::PrNode {
        id: node.id,
        number: node.number,
        title: node.title,
        body: node.body,
        state: node.state,
        is_draft: node.is_draft,
        url: node.url,
        author: node.author,
        author_association: node.author_association,
        repository: node.repository,
        head_ref_name: node.head_ref_name,
        base_ref_name: node.base_ref_name,
        review_decision: node.review_decision,
        created_at: node.created_at,
        updated_at: node.updated_at,
        merged_at: node.merged_at,
        closed_at: node.closed_at,
        commits: node.commits,
        review_requests: node.review_requests,
        latest_opinionated_reviews: node.latest_opinionated_reviews,
        closing_issues_references: node.closing_issues_references,
        comments: parse::Paged {
            total_count: total,
            page_info: done.clone(),
            nodes: tail,
        },
        review_threads: parse::Paged {
            total_count: threads_total,
            page_info: done,
            nodes: threads,
        },
    };
    RefreshEnd::End(HydrateEnd::Bundle(Bundle::Pr(Box::new(PrBundle {
        repo: repo.to_string(),
        pr,
        refs: body_refs,
        origin,
        comments: if aborted {
            CommentsCompleteness::Incomplete
        } else {
            CommentsCompleteness::TailHit
        },
        threads_complete,
        requests_complete,
        reviews_complete,
        closing_complete,
    }))))
}

/// A skeleton thread whose every comment resolved from the archive.
fn thread_from_skeleton(
    sk: parse::SkeletonThreadNode,
    baseline: &RefreshBaseline,
) -> parse::ThreadNode {
    let nodes = sk
        .comments
        .nodes
        .into_iter()
        .map(|c| {
            let body = baseline
                .thread_comments
                .get(&c.id)
                .map(|(_, body)| body.clone())
                .unwrap_or_default(); // unreachable: resolvable checked membership
            parse::CommentNode {
                id: c.id,
                body,
                created_at: c.created_at,
                last_edited_at: c.last_edited_at,
                url: c.url,
                is_minimized: c.is_minimized,
                author_association: c.author_association,
                author: c.author,
            }
        })
        .collect();
    parse::ThreadNode {
        id: sk.id,
        is_resolved: sk.is_resolved,
        is_outdated: sk.is_outdated,
        path: sk.path,
        line: sk.line,
        comments: parse::Counted {
            total_count: sk.comments.total_count,
            nodes,
        },
    }
}

/// A thread kept for its cheap fields with its comments dropped (body
/// fetch failed or the floor stopped it): total_count survives, so the
/// ordinary nodes-vs-count rule withholds the threads witness.
fn thread_without_comments(sk: parse::SkeletonThreadNode) -> parse::ThreadNode {
    parse::ThreadNode {
        id: sk.id,
        is_resolved: sk.is_resolved,
        is_outdated: sk.is_outdated,
        path: sk.path,
        line: sk.line,
        comments: parse::Counted {
            total_count: sk.comments.total_count,
            nodes: Vec::new(),
        },
    }
}

// ---------------------------------------------------------------------------
// Writer: the only thread that touches the write connection.

/// Per-repo write-side counters. The worker-side ones arrive in Msg::Stats.
#[derive(Default)]
struct RepoTally {
    fetched: u64,
    upserted: u64,
    unchanged: u64,
    filtered: u64,
    observations: u64,
    soft_deleted: u64,
    reverified: u64,
    quiet_mutations_found: u64,
    reverify_shed: u64,
    tail_hits: u64,
    full_walks: u64,
    escalations: BTreeMap<&'static str, u64>,
    bodies_skipped: u64,
    truncated: u64,
    quarantined: u64,
    discovery_truncated: u64,
    deferred_at_floor: bool,
    masked_hits: u64,
    watchdog_kills: u64,
    rate_limit_unknown: u64,
    subprocess_count: u64,
    subprocess_ms: u64,
    bytes_parsed: u64,
    rate_cost: u64,
    sleeps: u64,
    sleep_ms: u64,
    remaining: Option<u32>,
    errors: Vec<String>,
}

fn writer(
    archive: &mut RwArchive,
    rx: Receiver<Msg>,
    now: &Rfc3339Utc,
    repos: &[String],
) -> Result<Value> {
    let mut tallies: BTreeMap<String, RepoTally> = BTreeMap::new();
    for repo in repos {
        tallies.insert(repo.clone(), RepoTally::default());
    }
    // A message can only name a configured repo, but stay total.
    fn tally<'a>(t: &'a mut BTreeMap<String, RepoTally>, repo: &str) -> &'a mut RepoTally {
        t.entry(repo.to_string()).or_default()
    }

    for msg in rx {
        match msg {
            Msg::Page { repo, rows } => {
                apply_rows(archive, now, &rows, tally(&mut tallies, &repo))?;
            }
            Msg::Done {
                repo,
                stream,
                witness: _witness,
                rows,
                quarantine,
                watermark,
                fingerprint,
                completes_stream,
                discovery_truncated,
                masked,
            } => {
                let t = tally(&mut tallies, &repo);
                t.discovery_truncated += discovery_truncated;
                t.masked_hits += masked;
                t.quarantined += quarantine.len() as u64;
                let tx = archive
                    .conn_mut()
                    .transaction()
                    .map_err(|e| classify_sql(&e))?;
                for bundle in &rows {
                    apply_bundle(&tx, now, bundle, t)?;
                }
                for q in &quarantine {
                    upsert_quarantine(&tx, &repo, q)?;
                }
                advance_state(
                    &tx,
                    &repo,
                    stream,
                    watermark.as_ref(),
                    &fingerprint,
                    completes_stream,
                    now,
                )?;
                tx.commit().map_err(|e| classify_sql(&e))?;
            }
            Msg::Retries {
                repo,
                resolved,
                requeued,
                drained,
            } => {
                let t = tally(&mut tallies, &repo);
                t.quarantined += requeued.len() as u64;
                let tx = archive
                    .conn_mut()
                    .transaction()
                    .map_err(|e| classify_sql(&e))?;
                for bundle in &resolved {
                    apply_bundle(&tx, now, bundle, t)?;
                    exec(
                        &tx,
                        "DELETE FROM quarantine WHERE id = ?1",
                        rusqlite::params![bundle.id()],
                    )?;
                }
                for q in &requeued {
                    upsert_quarantine(&tx, &repo, q)?;
                }
                for (id, stream) in &drained {
                    // Repeated node:null drains to deleted_at: the id is
                    // gone upstream; the row (if any) becomes a soft delete
                    // in the STREAM'S table and the quarantine entry
                    // retires with it.
                    let sql = match stream {
                        Stream::Pr => {
                            "UPDATE prs SET deleted_at = ?1 WHERE id = ?2 AND deleted_at IS NULL"
                        }
                        Stream::Issue => {
                            "UPDATE issues SET deleted_at = ?1 WHERE id = ?2 AND deleted_at IS NULL"
                        }
                    };
                    let n = exec(&tx, sql, rusqlite::params![now.as_str(), id])?;
                    t.soft_deleted += n as u64;
                    exec(
                        &tx,
                        "DELETE FROM quarantine WHERE id = ?1",
                        rusqlite::params![id],
                    )?;
                }
                tx.commit().map_err(|e| classify_sql(&e))?;
            }
            Msg::Deferred {
                repo,
                stream,
                reset_at: _,
            } => {
                let t = tally(&mut tallies, &repo);
                t.deferred_at_floor = true;
                // Starvation bookkeeping: a deferred stream did not
                // complete. Only an existing row increments — a
                // never-synced stream has no watermark to starve.
                let conn = archive.conn();
                conn.execute(
                    "UPDATE sync_state SET runs_since_advance = runs_since_advance + 1 \
                     WHERE repo = ?1 AND stream = ?2",
                    rusqlite::params![repo, stream.as_str()],
                )
                .map_err(|e| classify_sql(&e))?;
            }
            Msg::Failed(repo, streams, error) => {
                let t = tally(&mut tallies, &repo);
                t.errors.push(error.to_string());
                // A repo failure is a failed RUN for every stream this run
                // was configured to walk (the module docs' "increments on
                // a deferred or failed run") — and only those (the message
                // doc carries the residual-row argument). Only existing
                // rows move, as with Deferred: a never-synced stream has
                // nothing to starve.
                for stream in streams {
                    archive
                        .conn()
                        .execute(
                            "UPDATE sync_state SET runs_since_advance = runs_since_advance + 1 \
                             WHERE repo = ?1 AND stream = ?2",
                            rusqlite::params![repo, stream.as_str()],
                        )
                        .map_err(|e| classify_sql(&e))?;
                }
            }
            Msg::Stats {
                repo,
                tel,
                fetched,
                filtered,
                reverified,
                reverify_shed,
                tail_hits,
                full_walks,
                escalations,
                bodies_skipped,
            } => {
                let t = tally(&mut tallies, &repo);
                t.fetched += fetched;
                t.filtered += filtered;
                t.reverified += reverified;
                t.reverify_shed += reverify_shed;
                t.tail_hits += tail_hits;
                t.full_walks += full_walks;
                for (reason, n) in escalations {
                    *t.escalations.entry(reason).or_default() += n;
                }
                t.bodies_skipped += bodies_skipped;
                t.subprocess_count += tel.subprocess_count;
                t.subprocess_ms += tel.subprocess_ms;
                t.bytes_parsed += tel.bytes_parsed;
                t.rate_cost += tel.rate_cost;
                t.sleeps += tel.sleeps;
                t.sleep_ms += tel.sleep_ms;
                // watchdog_kills' merge is witnessed only by the ignored
                // heavy test (`make check-heavy` waits out the shipped
                // 120s deadline once); fast sweeps see it as a survivor.
                t.watchdog_kills += tel.watchdog_kills;
                t.rate_limit_unknown += tel.rate_limit_unknown;
                t.remaining = tel.remaining;
            }
        }
    }

    Ok(summary(&tallies))
}

/// One repo's rows in one transaction (the Page path).
fn apply_rows(
    archive: &mut RwArchive,
    now: &Rfc3339Utc,
    rows: &[Bundle],
    t: &mut RepoTally,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let tx = archive
        .conn_mut()
        .transaction()
        .map_err(|e| classify_sql(&e))?;
    for bundle in rows {
        apply_bundle(&tx, now, bundle, t)?;
    }
    tx.commit().map_err(|e| classify_sql(&e))
}

/// Watermark and freshness, inside the same transaction as the window's
/// rows. max() keeps the advance monotone under backfills; last_checked_at
/// and runs_since_advance move only when the stream COMPLETED.
fn advance_state(
    tx: &rusqlite::Transaction,
    repo: &str,
    stream: Stream,
    watermark: Option<&Rfc3339Utc>,
    fingerprint: &str,
    completes_stream: bool,
    now: &Rfc3339Utc,
) -> Result<()> {
    let stored: Option<String> = tx
        .query_row(
            "SELECT last_item_updated_at FROM sync_state WHERE repo = ?1 AND stream = ?2",
            rusqlite::params![repo, stream.as_str()],
            |r| r.get(0),
        )
        .map(Some)
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;
    let offered = watermark.map(|w| w.as_str().to_string());
    let new_wm = match (&stored, &offered) {
        (Some(s), Some(o)) => Some(if o.as_str() > s.as_str() {
            o.clone()
        } else {
            s.clone()
        }),
        (Some(s), None) => Some(s.clone()),
        (None, Some(o)) => Some(o.clone()),
        // First contact and an empty window: the epoch sentinel keeps the
        // row present (fingerprint + freshness need one) without claiming
        // any item was seen.
        (None, None) => Some("1970-01-01T00:00:00Z".to_string()),
    };
    if completes_stream {
        exec(
            tx,
            "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                     runs_since_advance, fingerprint) \
             VALUES (?1, ?2, ?3, ?4, 0, ?5) \
             ON CONFLICT(repo, stream) DO UPDATE SET \
               last_item_updated_at = excluded.last_item_updated_at, \
               last_checked_at = excluded.last_checked_at, \
               runs_since_advance = 0, \
               fingerprint = excluded.fingerprint",
            rusqlite::params![repo, stream.as_str(), new_wm, now.as_str(), fingerprint],
        )?;
    } else {
        exec(
            tx,
            "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                     runs_since_advance, fingerprint) \
             VALUES (?1, ?2, ?3, NULL, 0, ?4) \
             ON CONFLICT(repo, stream) DO UPDATE SET \
               last_item_updated_at = excluded.last_item_updated_at, \
               fingerprint = excluded.fingerprint",
            rusqlite::params![repo, stream.as_str(), new_wm, fingerprint],
        )?;
    }
    Ok(())
}

fn upsert_quarantine(tx: &rusqlite::Transaction, repo: &str, q: &QuarantineRecord) -> Result<()> {
    // `stream` updates on conflict for completeness, but it cannot actually
    // flip: node ids are globally unique, so an id re-quarantined by the
    // other stream would mean discovery itself mislabeled it.
    exec(
        tx,
        "INSERT INTO quarantine (id, repo, stream, attempts, next_retry_at, error_class) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(id) DO UPDATE SET \
           stream = excluded.stream, \
           attempts = excluded.attempts, \
           next_retry_at = excluded.next_retry_at, \
           error_class = excluded.error_class",
        rusqlite::params![
            q.id,
            repo,
            q.stream.as_str(),
            q.attempts,
            q.next_retry_at,
            q.error_class
        ],
    )?;
    Ok(())
}

// --- the bundle upsert: diff-gated everywhere, witnesses gate the sweeps ---

/// Everything the archive records about one hydrated item, one transaction
/// scope (the caller's) — dispatched by stream, so a PR bundle and an issue
/// bundle can never take each other's write path.
fn apply_bundle(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    b: &Bundle,
    t: &mut RepoTally,
) -> Result<()> {
    match b {
        Bundle::Pr(b) => apply_pr_bundle(tx, now, b, t),
        Bundle::Issue(b) => apply_issue_bundle(tx, now, b, t),
    }
}

/// One hydrated PR's writes. Returns nothing; effects land in the tally.
fn apply_pr_bundle(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    b: &PrBundle,
    t: &mut RepoTally,
) -> Result<()> {
    // Two flags, deliberately distinct: `changed` is CONTENT (fields,
    // children, sweeps — what replay idempotence forbids on unchanged
    // input); the verified_at re-stamp a re-verify performs is a row write
    // but not a mutation FOUND, so it feeds neither quiet_mutations_found
    // nor upserted.
    let mut changed = false;

    let (pk, landed_truncated) = upsert_pr(tx, now, b, t, &mut changed)?;
    upsert_children(tx, now, b, pk, t, &mut changed)?;

    if changed {
        t.upserted += 1;
    } else {
        t.unchanged += 1;
    }
    // health.truncated counts rows the run LEFT truncated. A TailHit
    // carries the stored value, so an ordinary hit (stored 0) is not
    // truncation — counting the bundle's !verified() here would report
    // every tail hit as an incomplete archive row, which it is not.
    if landed_truncated {
        t.truncated += 1;
    }
    if b.origin == Origin::Reverify && changed {
        // The tier exists to catch quiet mutations; a re-verify whose
        // refetch changed CONTENT found one. Feeds the tier defaults.
        t.quiet_mutations_found += 1;
    }
    Ok(())
}

/// One hydrated issue's writes: the row (labels/assignees as canonical
/// JSON), its comments under parent_kind='issue', and the witness-gated
/// comment sweep. Same tally semantics as [`apply_pr_bundle`]; no
/// observations rows ever — that table is PR-fields only in both scopes
/// (DESIGN.md's fence), and no refs — refs are extracted from PRs
/// (queries.rs HYDRATE_ISSUE records the cut).
fn apply_issue_bundle(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    b: &IssueBundle,
    t: &mut RepoTally,
) -> Result<()> {
    let mut changed = false;

    let (pk, landed_truncated) = upsert_issue_stream(tx, now, b, &mut changed)?;

    let mut seen: HashSet<String> = HashSet::new();
    for c in &b.issue.comments.nodes {
        seen.insert(c.id.clone());
        upsert_comment(
            tx,
            "issue",
            pk,
            None,
            "comment",
            None,
            &CommentFields::from_node(c),
            &mut changed,
        )?;
    }
    // The sweep is witness-gated, as everywhere: truncation must never
    // read as deletion.
    if b.comments_complete {
        t.soft_deleted += sweep(
            tx,
            now,
            "SELECT id FROM comments WHERE parent_kind='issue' AND parent=?1 \
             AND kind='comment' AND deleted_at IS NULL",
            pk,
            &seen,
            &mut changed,
        )?;
    }

    if changed {
        t.upserted += 1;
    } else {
        t.unchanged += 1;
    }
    if landed_truncated {
        t.truncated += 1;
    }
    if b.origin == Origin::Reverify && changed {
        t.quiet_mutations_found += 1;
    }
    Ok(())
}

/// The observed PR fields whose transitions are the changelog. author_id
/// and author_assoc are deliberately not observed (schema.sql records why);
/// last_pushed_at has no writer yet — the OPEN QUESTION at queries.rs
/// HYDRATE_PR is DECIDED as: no API source exists (Commit.pushedDate is
/// deprecated); the staleness signal's replacement is the observations
/// table's own head_sha flip row (observed_at, local time, disclosed as
/// such) as the fresh-side bound plus prs.head_committed_at as the
/// stale-side bound (schema v2 — the v1 prose claimed committedDate was
/// stored while the upsert dropped it; db.rs SCHEMA_VERSION records the
/// repair). head_committed_at is diffed, not observed: it rides with
/// head_sha (same oid ⇒ same date), so observing it would double-log every
/// push. attention.rs combines the two bounds under its polarity rule;
/// prs.last_pushed_at stays NULL, which fails closed.
const OBSERVED: &[&str] = &["state", "review_decision", "head_sha", "is_draft", "author"];

/// The one SELECT whose column ORDER feeds a decision: old_cols[17] must
/// be `truncated` for the TailHit carry (upsert_pr). The list is a const
/// so the position test pins the string the query actually runs, not a
/// copy (B3 panel, S3).
const PR_SELECT: &str = "SELECT pk, id, title, body, state, is_draft, author, author_id, \
                    author_assoc, head_ref, base_ref, head_sha, review_decision, created_at, \
                    updated_at, merged_at, closed_at, url, truncated, deleted_at, \
                    head_committed_at, verified_at \
             FROM prs WHERE repo = ?1 AND number = ?2";

fn upsert_pr(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    b: &PrBundle,
    t: &mut RepoTally,
    changed: &mut bool,
) -> Result<(i64, bool)> {
    let pr = &b.pr;
    let head = pr.commits.nodes.first().map(|c| &c.commit);
    let author = pr.author.as_ref();

    let existing: Option<(i64, Vec<Option<String>>, Option<String>)> = tx
        .query_row(PR_SELECT, rusqlite::params![b.repo, pr.number], |r| {
            let pk: i64 = r.get(0)?;
            let mut cols = Vec::new();
            for i in 1..21 {
                // Numeric columns read back as text for a uniform diff;
                // SQLite's CAST of INTEGER to TEXT is exact.
                cols.push(
                    r.get::<_, Option<String>>(i).or_else(|_| {
                        r.get::<_, Option<i64>>(i).map(|v| v.map(|v| v.to_string()))
                    })?,
                );
            }
            let verified_at: Option<String> = r.get(21)?;
            Ok((pk, cols, verified_at))
        })
        .map(Some)
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;

    // The truncated value this bundle lands. A TailHit CARRIES the stored
    // value (round-0 decree: the middle was inferred, so the bundle may
    // neither heal truncation nor introduce it — a boolean here is what
    // oscillated); with truncated inside the field diff, carrying the old
    // value is also what keeps an unchanged tail-hit replay write-free.
    // The carry is licensed ONLY when every other connection witnessed:
    // a TailHit whose thread walk lost its witness may be missing rows,
    // and carrying stored 0 through that would hide real truncation. A
    // TailHit with no stored row cannot claim anything either (the
    // dispatch gate requires a witnessed baseline, so that arm is
    // defensive): both land truncated, disclosed, healed by the next
    // full walk.
    let others_complete =
        b.threads_complete && b.requests_complete && b.reviews_complete && b.closing_complete;
    let landed_truncated = match (b.comments, &existing) {
        (CommentsCompleteness::TailHit, Some((_, old_cols, _))) if others_complete => {
            // old_cols[17] is `truncated` (the SELECT order above).
            old_cols[17].as_deref() == Some("1")
        }
        // This defensive arm is explicit rather than folded into the
        // fallback: for every TailHit input `!b.verified()` also computes
        // true (a TailHit is never verified), so deleting the arm is a
        // behavior-preserving mutant — it stays because the truth of the
        // fallback depends on verified()'s definition and this line's
        // correctness must not.
        (CommentsCompleteness::TailHit, _) => true,
        _ => !b.verified(),
    };

    let new: Vec<(&str, Option<String>)> = vec![
        ("id", Some(pr.id.clone())),
        ("title", Some(pr.title.clone())),
        ("body", Some(pr.body.clone())),
        ("state", Some(pr.state.clone())),
        ("is_draft", Some(i64::from(pr.is_draft).to_string())),
        ("author", author.map(|a| a.login.as_str().to_string())),
        (
            "author_id",
            author.and_then(|a| a.database_id).map(|i| i.to_string()),
        ),
        ("author_assoc", Some(pr.author_association.clone())),
        ("head_ref", Some(pr.head_ref_name.clone())),
        ("base_ref", Some(pr.base_ref_name.clone())),
        ("head_sha", head.map(|c| c.oid.clone())),
        ("review_decision", pr.review_decision.clone()),
        ("created_at", Some(pr.created_at.as_str().to_string())),
        ("updated_at", Some(pr.updated_at.as_str().to_string())),
        (
            "merged_at",
            pr.merged_at.as_ref().map(|x| x.as_str().to_string()),
        ),
        (
            "closed_at",
            pr.closed_at.as_ref().map(|x| x.as_str().to_string()),
        ),
        ("url", Some(pr.url.clone())),
        ("truncated", Some(i64::from(landed_truncated).to_string())),
        ("deleted_at", None),
        // Diffed but not OBSERVED: it cannot change without head_sha
        // changing (same oid ⇒ same committedDate), so observing it would
        // double-log every push. In the diff it must stay: that is what
        // backfills NULL on a migrated-v1 row's next hydration (db.rs
        // SCHEMA_VERSION) while keeping the unchanged-replay write-free.
        (
            "head_committed_at",
            head.map(|c| c.committed_date.as_str().to_string()),
        ),
    ];

    match existing {
        None => {
            *changed = true;
            let verified_at = if b.verified() {
                Some(now.as_str())
            } else {
                None
            };
            exec(
                tx,
                "INSERT INTO prs (id, repo, number, title, body, state, is_draft, author, \
                                  author_id, author_assoc, head_ref, base_ref, head_sha, \
                                  last_pushed_at, review_decision, created_at, updated_at, \
                                  merged_at, closed_at, url, truncated, verified_at, deleted_at, \
                                  head_committed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, NULL, ?14, \
                         ?15, ?16, ?17, ?18, ?19, ?20, ?21, NULL, ?22)",
                rusqlite::params![
                    pr.id,
                    b.repo,
                    pr.number,
                    pr.title,
                    pr.body,
                    pr.state,
                    pr.is_draft,
                    author.map(|a| a.login.as_str()),
                    author.and_then(|a| a.database_id),
                    pr.author_association,
                    pr.head_ref_name,
                    pr.base_ref_name,
                    head.map(|c| c.oid.as_str()),
                    pr.review_decision,
                    pr.created_at.as_str(),
                    pr.updated_at.as_str(),
                    pr.merged_at.as_ref().map(|x| x.as_str()),
                    pr.closed_at.as_ref().map(|x| x.as_str()),
                    pr.url,
                    landed_truncated,
                    verified_at,
                    head.map(|c| c.committed_date.as_str()),
                ],
            )?;
            Ok((tx.last_insert_rowid(), landed_truncated))
        }
        Some((pk, old_cols, old_verified_at)) => {
            let names = [
                "id",
                "title",
                "body",
                "state",
                "is_draft",
                "author",
                "author_id",
                "author_assoc",
                "head_ref",
                "base_ref",
                "head_sha",
                "review_decision",
                "created_at",
                "updated_at",
                "merged_at",
                "closed_at",
                "url",
                "truncated",
                "deleted_at",
                "head_committed_at",
            ];
            let old: HashMap<&str, &Option<String>> =
                names.iter().copied().zip(old_cols.iter()).collect();
            let mut field_changed = false;
            for (name, new_val) in &new {
                let old_val = old[*name];
                if old_val != new_val {
                    field_changed = true;
                    if OBSERVED.contains(name) {
                        exec(
                            tx,
                            "INSERT INTO observations (pr, observed_at, field, old, new) \
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![pk, now.as_str(), name, old_val, new_val],
                        )?;
                        t.observations += 1;
                    }
                }
            }
            // verified_at moves only when it says something new (module
            // docs): witnessed AND (changed | was-truncated | never |
            // explicit re-verify). Keeping it still on a no-op overlap
            // re-hydration is what makes replay write nothing (pinned by
            // the replay test's backdated stamps). The never-verified arm
            // is defensive: this writer cannot produce verified_at NULL
            // with truncated=0 (insert stamps or marks truncated), so the
            // arm is unreachable until some future writer relaxes that —
            // its mutants are equivalent by that precondition, and stay.
            let stamp = b.verified()
                && (field_changed
                    || old_verified_at.is_none()
                    || old["truncated"].as_deref() == Some("1")
                    || b.origin == Origin::Reverify
                    || b.origin == Origin::Targeted);
            if field_changed {
                *changed = true;
            }
            if field_changed || stamp {
                let verified_at = if stamp {
                    Some(now.as_str().to_string())
                } else {
                    old_verified_at
                };
                exec(
                    tx,
                    "UPDATE prs SET id=?1, title=?2, body=?3, state=?4, is_draft=?5, author=?6, \
                       author_id=?7, author_assoc=?8, head_ref=?9, base_ref=?10, head_sha=?11, \
                       review_decision=?12, created_at=?13, updated_at=?14, merged_at=?15, \
                       closed_at=?16, url=?17, truncated=?18, verified_at=?19, deleted_at=NULL, \
                       head_committed_at=?20 \
                     WHERE pk = ?21",
                    rusqlite::params![
                        pr.id,
                        pr.title,
                        pr.body,
                        pr.state,
                        pr.is_draft,
                        author.map(|a| a.login.as_str()),
                        author.and_then(|a| a.database_id),
                        pr.author_association,
                        pr.head_ref_name,
                        pr.base_ref_name,
                        head.map(|c| c.oid.as_str()),
                        pr.review_decision,
                        pr.created_at.as_str(),
                        pr.updated_at.as_str(),
                        pr.merged_at.as_ref().map(|x| x.as_str()),
                        pr.closed_at.as_ref().map(|x| x.as_str()),
                        pr.url,
                        landed_truncated,
                        verified_at,
                        head.map(|c| c.committed_date.as_str()),
                        pk,
                    ],
                )?;
            }
            Ok((pk, landed_truncated))
        }
    }
}

/// Threads, comments (top-level, review, thread), review requests, refs,
/// linked issues, and the witness-gated sweeps.
fn upsert_children(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    b: &PrBundle,
    pk: i64,
    t: &mut RepoTally,
    changed: &mut bool,
) -> Result<()> {
    let pr = &b.pr;

    // Threads and their comments.
    let mut seen_threads: HashSet<String> = HashSet::new();
    let mut seen_review_comments: HashSet<String> = HashSet::new();
    for thread in &pr.review_threads.nodes {
        seen_threads.insert(thread.id.clone());
        let thread_pk = upsert_thread(tx, pk, thread, changed)?;
        for c in &thread.comments.nodes {
            seen_review_comments.insert(c.id.clone());
            upsert_comment(
                tx,
                "pr",
                pk,
                Some(thread_pk),
                "review_comment",
                None,
                &CommentFields::from_node(c),
                changed,
            )?;
        }
    }

    // Top-level comments.
    let mut seen_comments: HashSet<String> = HashSet::new();
    for c in &pr.comments.nodes {
        seen_comments.insert(c.id.clone());
        upsert_comment(
            tx,
            "pr",
            pk,
            None,
            "comment",
            None,
            &CommentFields::from_node(c),
            changed,
        )?;
    }

    // Reviews land as comments rows (kind='review'). A review without a
    // submittedAt cannot satisfy comments.created_at NOT NULL and an
    // opinionated review always carries one; skipping is recorded at the
    // parse type.
    let mut seen_reviews: HashSet<String> = HashSet::new();
    if let Some(reviews) = &pr.latest_opinionated_reviews {
        for r in &reviews.nodes {
            let Some(submitted) = &r.submitted_at else {
                continue;
            };
            seen_reviews.insert(r.id.clone());
            upsert_comment(
                tx,
                "pr",
                pk,
                None,
                "review",
                Some(r.state.as_str()),
                &CommentFields {
                    id: &r.id,
                    body: &r.body,
                    created_at: submitted.as_str().to_string(),
                    updated_at: None,
                    url: Some(r.url.clone()),
                    is_minimized: false,
                    author_assoc: &r.author_association,
                    author: r.author.as_ref(),
                },
                changed,
            )?;
        }
    }

    // Sweeps: soft deletes, gated on the connection's completeness witness
    // — a sweep on an incomplete connection is a type error by discipline
    // (truncation must never read as deletion).
    if b.comments == CommentsCompleteness::Complete {
        t.soft_deleted += sweep(
            tx,
            now,
            "SELECT id FROM comments WHERE parent_kind='pr' AND parent=?1 AND kind='comment' \
             AND deleted_at IS NULL",
            pk,
            &seen_comments,
            changed,
        )?;
    }
    if b.threads_complete {
        t.soft_deleted += sweep_threads(tx, now, pk, &seen_threads, changed)?;
        t.soft_deleted += sweep(
            tx,
            now,
            "SELECT id FROM comments WHERE parent_kind='pr' AND parent=?1 \
             AND kind='review_comment' AND deleted_at IS NULL",
            pk,
            &seen_review_comments,
            changed,
        )?;
    }
    if b.reviews_complete {
        t.soft_deleted += sweep(
            tx,
            now,
            "SELECT id FROM comments WHERE parent_kind='pr' AND parent=?1 AND kind='review' \
             AND deleted_at IS NULL",
            pk,
            &seen_reviews,
            changed,
        )?;
    }

    // Review requests: replaced wholesale — but only under the witness, and
    // only when the set actually changed (an incomplete connection must not
    // delete requests it merely failed to see: waiting_on_me is fail-open,
    // and a dropped row can only under-fill a demand).
    if b.requests_complete {
        let mut new_reqs: Vec<(String, String)> = Vec::new();
        if let Some(requests) = &pr.review_requests {
            for r in &requests.nodes {
                match &r.requested_reviewer {
                    Some(parse::RequestedReviewer::User(u)) => {
                        new_reqs.push((u.login.as_str().to_string(), "user".to_string()));
                    }
                    Some(parse::RequestedReviewer::Team(team)) => {
                        new_reqs.push((team.name.clone(), "team".to_string()));
                    }
                    // Invisible (None) or fragment-less (Unresolved: Bot,
                    // Mannequin, EnterpriseTeam) reviewers have no name to
                    // store. The request is real — totalCount counts it —
                    // but a row needs an identity; the viewer's own USER
                    // requests are always visible to the viewer, and a
                    // team request under-fills only for a team the viewer
                    // cannot see, which a truthfully-declared config.teams
                    // membership rules out — so the demand surface
                    // (attention) cannot under-fill from this skip.
                    Some(parse::RequestedReviewer::Unresolved(_)) | None => {}
                }
            }
        }
        new_reqs.sort();
        new_reqs.dedup();
        let mut old_reqs: Vec<(String, String)> = {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT reviewer, kind FROM review_requests WHERE pr = ?1 ORDER BY reviewer, kind",
                )
                .map_err(|e| classify_sql(&e))?;
            let rows = stmt
                .query_map([pk], |r| Ok((r.get(0)?, r.get(1)?)))
                .map_err(|e| classify_sql(&e))?;
            rows.collect::<std::result::Result<_, _>>()
                .map_err(|e| classify_sql(&e))?
        };
        old_reqs.sort();
        if old_reqs != new_reqs {
            *changed = true;
            exec(
                tx,
                "DELETE FROM review_requests WHERE pr = ?1",
                rusqlite::params![pk],
            )?;
            for (reviewer, kind) in &new_reqs {
                exec(
                    tx,
                    "INSERT OR IGNORE INTO review_requests (pr, reviewer, kind) VALUES (?1, ?2, ?3)",
                    rusqlite::params![pk, reviewer, kind],
                )?;
            }
        }
    }

    // Refs: recomputed per PR from what was OBSERVED — body refs always
    // (the body came with the hydration), api refs only under the closing
    // witness (a masked connection must not delete evidence it failed to
    // carry).
    let mut new_refs: Vec<(String, String, String, i64)> = b
        .refs
        .iter()
        .map(|r| {
            (
                r.kind.as_str().to_string(),
                "body".to_string(),
                r.repo.clone(),
                i64::try_from(r.number).unwrap_or(i64::MAX),
            )
        })
        .collect();
    if let Some(closing) = &pr.closing_issues_references {
        for issue in &closing.nodes {
            new_refs.push((
                "fixes".to_string(),
                "api".to_string(),
                issue.repository.name_with_owner.to_ascii_lowercase(),
                issue.number,
            ));
        }
    }
    new_refs.sort();
    new_refs.dedup();
    let mut old_refs: Vec<(String, String, String, i64)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT kind, source, target_repo, target_number FROM refs WHERE src_pr = ?1 \
                 ORDER BY kind, source, target_repo, target_number",
            )
            .map_err(|e| classify_sql(&e))?;
        let rows = stmt
            .query_map([pk], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .map_err(|e| classify_sql(&e))?;
        rows.collect::<std::result::Result<_, _>>()
            .map_err(|e| classify_sql(&e))?
    };
    // Under a missing closing witness, keep stored api rows out of the
    // comparison and the delete — they are not up for recomputation.
    if !b.closing_complete {
        old_refs.retain(|(_, source, _, _)| source == "body");
        new_refs.retain(|(_, source, _, _)| source == "body");
    }
    old_refs.sort();
    if old_refs != new_refs {
        *changed = true;
        if b.closing_complete {
            exec(
                tx,
                "DELETE FROM refs WHERE src_pr = ?1",
                rusqlite::params![pk],
            )?;
        } else {
            exec(
                tx,
                "DELETE FROM refs WHERE src_pr = ?1 AND source = 'body'",
                rusqlite::params![pk],
            )?;
        }
        for (kind, source, target_repo, target_number) in &new_refs {
            exec(
                tx,
                "INSERT OR IGNORE INTO refs (src_pr, kind, source, target_repo, target_number) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![pk, kind, source, target_repo, target_number],
            )?;
        }
    }

    // Linked issues: the working-scope FILL-ONLY writer (schema.sql) — it
    // inserts a missing row and freshens only rows it owns
    // (hydration_source='linked'), diff-gated like everything else so a
    // replay writes nothing (synced_at moves only with real changes; the
    // gate is the same honesty rule).
    if let Some(closing) = &pr.closing_issues_references {
        for issue in &closing.nodes {
            upsert_linked_issue(tx, now, issue, changed)?;
        }
    }
    Ok(())
}

/// (pk, path, line, is_resolved, is_outdated, deleted_at)
type ThreadRow = (i64, Option<String>, Option<i64>, i64, i64, Option<String>);

fn upsert_thread(
    tx: &rusqlite::Transaction,
    pr_pk: i64,
    thread: &parse::ThreadNode,
    changed: &mut bool,
) -> Result<i64> {
    let existing: Option<ThreadRow> = tx
        .query_row(
            "SELECT pk, path, line, is_resolved, is_outdated, deleted_at \
             FROM review_threads WHERE id = ?1",
            [&thread.id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .map(Some)
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;
    match existing {
        None => {
            *changed = true;
            exec(
                tx,
                "INSERT INTO review_threads (id, pr, path, line, is_resolved, is_outdated, deleted_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
                rusqlite::params![
                    thread.id,
                    pr_pk,
                    thread.path,
                    thread.line,
                    thread.is_resolved,
                    thread.is_outdated
                ],
            )?;
            Ok(tx.last_insert_rowid())
        }
        Some((pk, path, line, is_resolved, is_outdated, deleted_at)) => {
            let same = path.as_deref() == Some(thread.path.as_str())
                && line == thread.line
                && (is_resolved != 0) == thread.is_resolved
                && (is_outdated != 0) == thread.is_outdated
                && deleted_at.is_none();
            if !same {
                *changed = true;
                exec(
                    tx,
                    "UPDATE review_threads SET path=?1, line=?2, is_resolved=?3, is_outdated=?4, \
                     deleted_at=NULL WHERE pk=?5",
                    rusqlite::params![
                        thread.path,
                        thread.line,
                        thread.is_resolved,
                        thread.is_outdated,
                        pk
                    ],
                )?;
            }
            Ok(pk)
        }
    }
}

/// The comment column set shared by the three kinds; reviews adapt into it.
struct CommentFields<'a> {
    id: &'a str,
    body: &'a str,
    created_at: String,
    updated_at: Option<String>,
    url: Option<String>,
    is_minimized: bool,
    author_assoc: &'a str,
    author: Option<&'a parse::Author>,
}

impl<'a> CommentFields<'a> {
    fn from_node(c: &'a parse::CommentNode) -> CommentFields<'a> {
        CommentFields {
            id: &c.id,
            body: &c.body,
            created_at: c.created_at.as_str().to_string(),
            updated_at: c.last_edited_at.as_ref().map(|t| t.as_str().to_string()),
            url: Some(c.url.clone()),
            is_minimized: c.is_minimized,
            author_assoc: &c.author_association,
            author: c.author.as_ref(),
        }
    }
}

/// Keyed on the upstream comment id, which GitHub guarantees globally
/// unique — the one place this writer leans on an upstream invariant with
/// no local witness: if an id ever named both a PR comment and an issue
/// comment, the UPDATE would re-home it whole (parent_kind and parent move
/// in the same statement — atomic, never a cross-table orphan) but
/// silently, each hydration flipping it back. The milestone-5 stats
/// consistency audit is the named place to add the oscillation detector if
/// that invariant ever cracks.
#[allow(clippy::too_many_arguments)]
fn upsert_comment(
    tx: &rusqlite::Transaction,
    parent_kind: &str,
    parent_pk: i64,
    thread_pk: Option<i64>,
    kind: &str,
    state: Option<&str>,
    c: &CommentFields<'_>,
    changed: &mut bool,
) -> Result<()> {
    let author_login = c.author.map(|a| a.login.as_str());
    let author_id = c.author.and_then(|a| a.database_id);
    let existing: Option<(i64, Vec<Option<String>>)> = tx
        .query_row(
            "SELECT pk, parent_kind, parent, thread, kind, state, author, author_id, \
                    author_assoc, body, is_minimized, created_at, updated_at, url, deleted_at \
             FROM comments WHERE id = ?1",
            [c.id],
            |r| {
                let pk: i64 = r.get(0)?;
                let mut cols = Vec::new();
                for i in 1..15 {
                    cols.push(r.get::<_, Option<String>>(i).or_else(|_| {
                        r.get::<_, Option<i64>>(i).map(|v| v.map(|v| v.to_string()))
                    })?);
                }
                Ok((pk, cols))
            },
        )
        .map(Some)
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;
    let new: Vec<Option<String>> = vec![
        Some(parent_kind.to_string()),
        Some(parent_pk.to_string()),
        thread_pk.map(|x| x.to_string()),
        Some(kind.to_string()),
        state.map(str::to_string),
        author_login.map(str::to_string),
        author_id.map(|x| x.to_string()),
        Some(c.author_assoc.to_string()),
        Some(c.body.to_string()),
        Some(i64::from(c.is_minimized).to_string()),
        Some(c.created_at.clone()),
        c.updated_at.clone(),
        c.url.clone(),
        None,
    ];
    match existing {
        None => {
            *changed = true;
            exec(
                tx,
                "INSERT INTO comments (id, parent_kind, parent, thread, kind, state, author, \
                                       author_id, author_assoc, body, is_minimized, created_at, \
                                       updated_at, url, deleted_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL)",
                rusqlite::params![
                    c.id,
                    parent_kind,
                    parent_pk,
                    thread_pk,
                    kind,
                    state,
                    author_login,
                    author_id,
                    c.author_assoc,
                    c.body,
                    c.is_minimized,
                    c.created_at,
                    c.updated_at,
                    c.url
                ],
            )?;
        }
        Some((pk, old)) if old != new => {
            *changed = true;
            exec(
                tx,
                "UPDATE comments SET parent_kind=?1, parent=?2, thread=?3, kind=?4, state=?5, \
                   author=?6, author_id=?7, author_assoc=?8, body=?9, is_minimized=?10, \
                   created_at=?11, updated_at=?12, url=?13, deleted_at=NULL WHERE pk=?14",
                rusqlite::params![
                    parent_kind,
                    parent_pk,
                    thread_pk,
                    kind,
                    state,
                    author_login,
                    author_id,
                    c.author_assoc,
                    c.body,
                    c.is_minimized,
                    c.created_at,
                    c.updated_at,
                    c.url,
                    pk
                ],
            )?;
        }
        Some(_) => {}
    }
    Ok(())
}

/// Soft-delete live child rows whose id left the (witnessed-complete)
/// observed set. Deterministic order; returns the count.
fn sweep(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    select_live: &str,
    pk: i64,
    seen: &HashSet<String>,
    changed: &mut bool,
) -> Result<u64> {
    let live: Vec<String> = {
        let mut stmt = tx
            .prepare_cached(select_live)
            .map_err(|e| classify_sql(&e))?;
        let rows = stmt
            .query_map([pk], |r| r.get::<_, String>(0))
            .map_err(|e| classify_sql(&e))?;
        rows.collect::<std::result::Result<_, _>>()
            .map_err(|e| classify_sql(&e))?
    };
    let mut gone: Vec<&String> = live.iter().filter(|id| !seen.contains(*id)).collect();
    gone.sort();
    let mut n = 0;
    for id in gone {
        n += exec(
            tx,
            "UPDATE comments SET deleted_at = ?1 WHERE id = ?2",
            rusqlite::params![now.as_str(), id],
        )? as u64;
    }
    if n > 0 {
        *changed = true;
    }
    Ok(n)
}

fn sweep_threads(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    pk: i64,
    seen: &HashSet<String>,
    changed: &mut bool,
) -> Result<u64> {
    let live: Vec<String> = {
        let mut stmt = tx
            .prepare_cached("SELECT id FROM review_threads WHERE pr = ?1 AND deleted_at IS NULL")
            .map_err(|e| classify_sql(&e))?;
        let rows = stmt
            .query_map([pk], |r| r.get::<_, String>(0))
            .map_err(|e| classify_sql(&e))?;
        rows.collect::<std::result::Result<_, _>>()
            .map_err(|e| classify_sql(&e))?
    };
    let mut gone: Vec<&String> = live.iter().filter(|id| !seen.contains(*id)).collect();
    gone.sort();
    let mut n = 0;
    for id in gone {
        n += exec(
            tx,
            "UPDATE review_threads SET deleted_at = ?1 WHERE id = ?2",
            rusqlite::params![now.as_str(), id],
        )? as u64;
    }
    if n > 0 {
        *changed = true;
    }
    Ok(n)
}

/// The issue-stream row upsert: diff-gated like upsert_pr, minus the
/// PR-only machinery (no observations — the fence — and no TailHit carry,
/// since issues have no inference path: truncated is always this bundle's
/// own !verified()). Writing hydration_source='stream' takes ownership of
/// a fill-only 'linked' row — the upgrade direction; the linked writer
/// cannot take it back (its UPDATE is gated on hydration_source='linked',
/// upsert_linked_issue below).
///
/// labels and assignees are stored as CANONICAL JSON: name/login-sorted,
/// deduped. GitHub's response order is presentation, not data, and a
/// canonical form is what keeps the diff gate honest — an order shuffle
/// upstream must not read as a change (replay idempotence) — and the
/// stored value deterministic for golden output.
///
/// A masked labels connection (schema-nullable; parse.rs) is not up for
/// recomputation — the same rule as the refs writer under a missing
/// closing witness: the stored value carries through the diff and the
/// UPDATE untouched, and only a fresh insert lands NULL (an unknown,
/// disclosed by the truncated flag the withheld witness already set).
fn upsert_issue_stream(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    b: &IssueBundle,
    changed: &mut bool,
) -> Result<(i64, bool)> {
    let issue = &b.issue;
    let author = issue.author.as_ref();
    let landed_truncated = !b.verified();

    let labels: Option<String> = issue.labels.as_ref().map(|c| {
        let mut names: Vec<&str> = c.nodes.iter().map(|l| l.name.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        serde_json::to_string(&names).expect("labels are plain data")
    });
    let mut assignees: Vec<&str> = issue
        .assignees
        .nodes
        .iter()
        .map(|a| a.login.as_str())
        .collect();
    assignees.sort_unstable();
    assignees.dedup();
    let assignees = serde_json::to_string(&assignees).expect("assignees are plain data");

    let existing: Option<(i64, Vec<Option<String>>, Option<String>)> = tx
        .query_row(
            "SELECT pk, id, title, state, body, author, author_id, author_assoc, labels, \
                    assignees, url, created_at, updated_at, hydration_source, truncated, \
                    deleted_at, verified_at \
             FROM issues WHERE repo = ?1 AND number = ?2",
            rusqlite::params![b.repo, issue.number],
            |r| {
                let pk: i64 = r.get(0)?;
                let mut cols = Vec::new();
                for i in 1..16 {
                    // Numeric columns read back as text for a uniform diff;
                    // SQLite's CAST of INTEGER to TEXT is exact.
                    cols.push(r.get::<_, Option<String>>(i).or_else(|_| {
                        r.get::<_, Option<i64>>(i).map(|v| v.map(|v| v.to_string()))
                    })?);
                }
                let verified_at: Option<String> = r.get(16)?;
                Ok((pk, cols, verified_at))
            },
        )
        .map(Some)
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;

    match existing {
        None => {
            *changed = true;
            let verified_at = if b.verified() {
                Some(now.as_str())
            } else {
                None
            };
            exec(
                tx,
                "INSERT INTO issues (id, repo, number, title, state, body, author, author_id, \
                                     author_assoc, labels, assignees, url, created_at, \
                                     updated_at, hydration_source, truncated, verified_at, \
                                     synced_at, deleted_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
                         'stream', ?15, ?16, ?17, NULL)",
                rusqlite::params![
                    issue.id,
                    b.repo,
                    issue.number,
                    issue.title,
                    issue.state,
                    issue.body,
                    author.map(|a| a.login.as_str()),
                    author.and_then(|a| a.database_id),
                    issue.author_association,
                    labels,
                    assignees,
                    issue.url,
                    issue.created_at.as_str(),
                    issue.updated_at.as_str(),
                    landed_truncated,
                    verified_at,
                    now.as_str(),
                ],
            )?;
            Ok((tx.last_insert_rowid(), landed_truncated))
        }
        Some((pk, old_cols, old_verified_at)) => {
            let names = [
                "id",
                "title",
                "state",
                "body",
                "author",
                "author_id",
                "author_assoc",
                "labels",
                "assignees",
                "url",
                "created_at",
                "updated_at",
                "hydration_source",
                "truncated",
                "deleted_at",
            ];
            let old: HashMap<&str, &Option<String>> =
                names.iter().copied().zip(old_cols.iter()).collect();
            // The masked-labels carry (doc above): the stored value stands
            // in for the unfetchable one, in the diff and the UPDATE both.
            let labels_row = labels.clone().or_else(|| old["labels"].clone());
            let new: Vec<(&str, Option<String>)> = vec![
                ("id", Some(issue.id.clone())),
                ("title", Some(issue.title.clone())),
                ("state", Some(issue.state.clone())),
                ("body", Some(issue.body.clone())),
                ("author", author.map(|a| a.login.as_str().to_string())),
                (
                    "author_id",
                    author.and_then(|a| a.database_id).map(|i| i.to_string()),
                ),
                ("author_assoc", Some(issue.author_association.clone())),
                ("labels", labels_row.clone()),
                ("assignees", Some(assignees.clone())),
                ("url", Some(issue.url.clone())),
                ("created_at", Some(issue.created_at.as_str().to_string())),
                ("updated_at", Some(issue.updated_at.as_str().to_string())),
                ("hydration_source", Some("stream".to_string())),
                ("truncated", Some(i64::from(landed_truncated).to_string())),
                ("deleted_at", None),
            ];
            let mut field_changed = false;
            for (name, new_val) in &new {
                if old[*name] != new_val {
                    field_changed = true;
                }
            }
            // verified_at moves only when it says something new — the
            // upsert_pr rule verbatim (module docs). The never-verified
            // arm is defensive here exactly as it is there: this writer's
            // own insert stamps or marks truncated, and the one other
            // writer ('linked' fill) leaves verified_at NULL only on rows
            // whose upgrade must flip hydration_source — which trips
            // field_changed first. Its mutants are equivalent by that
            // precondition, and stay; the arm itself stays because the
            // precondition lives in ANOTHER function's SQL. The truncated
            // arm co-fires with field_changed for the same reason (a
            // healing walk flips the stored truncated column, a diffed
            // field) — load-bearing only on the PR side, where the
            // TailHit carry can hold the column still; kept for rule
            // parity, mutants equivalent. Targeted is unreachable on the
            // issue path today (--pr is PR-only by verb contract) —
            // written for rule parity, so a future --issue lands on the
            // decided policy instead of re-deriving it; its mutants are
            // equivalent by the same argument.
            let stamp = b.verified()
                && (field_changed
                    || old_verified_at.is_none()
                    || old["truncated"].as_deref() == Some("1")
                    || b.origin == Origin::Reverify
                    || b.origin == Origin::Targeted);
            if field_changed {
                *changed = true;
            }
            if field_changed || stamp {
                let verified_at = if stamp {
                    Some(now.as_str().to_string())
                } else {
                    old_verified_at
                };
                exec(
                    tx,
                    "UPDATE issues SET id=?1, title=?2, state=?3, body=?4, author=?5, \
                       author_id=?6, author_assoc=?7, labels=?8, assignees=?9, url=?10, \
                       created_at=?11, updated_at=?12, hydration_source='stream', \
                       truncated=?13, verified_at=?14, synced_at=?15, deleted_at=NULL \
                     WHERE pk = ?16",
                    rusqlite::params![
                        issue.id,
                        issue.title,
                        issue.state,
                        issue.body,
                        author.map(|a| a.login.as_str()),
                        author.and_then(|a| a.database_id),
                        issue.author_association,
                        labels_row,
                        assignees,
                        issue.url,
                        issue.created_at.as_str(),
                        issue.updated_at.as_str(),
                        landed_truncated,
                        verified_at,
                        now.as_str(),
                        pk,
                    ],
                )?;
            }
            Ok((pk, landed_truncated))
        }
    }
}

fn upsert_linked_issue(
    tx: &rusqlite::Transaction,
    now: &Rfc3339Utc,
    issue: &parse::LinkedIssueNode,
    changed: &mut bool,
) -> Result<()> {
    let repo = issue.repository.name_with_owner.to_ascii_lowercase();
    let author = issue.author.as_ref();
    // A linked reference is a live API sighting: GitHub just rendered this
    // issue's node inside a PR's closingIssuesReferences, standing evidence
    // the id resolves to something the viewer can see — exactly what a
    // node:null drain concluded was gone. Presence is not content, so the
    // resurrect deliberately crosses the hydration_source boundary (a
    // drained STREAM row revives here too; its content refreshes on its
    // next discovery or re-verify) while the content freshen below keeps
    // its 'linked'-only gate. Conflicting evidence can alternate this
    // across runs (direct lookup null, connection rendering it) — each
    // flip is evidence-driven and counted (soft_deleted), never a guess.
    // Diff-gated by the WHERE, like every write here.
    let revived = exec(
        tx,
        "UPDATE issues SET deleted_at = NULL \
         WHERE repo = ?1 AND number = ?2 AND deleted_at IS NOT NULL",
        rusqlite::params![repo, issue.number],
    )?;
    if revived > 0 {
        *changed = true;
    }
    let inserted = exec(
        tx,
        "INSERT INTO issues (id, repo, number, title, state, body, author, author_id, \
                             author_assoc, labels, assignees, url, created_at, updated_at, \
                             hydration_source, truncated, verified_at, synced_at, deleted_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, NULL, ?10, NULL, ?11, 'linked', \
                 0, NULL, ?12, NULL) \
         ON CONFLICT(repo, number) DO NOTHING",
        rusqlite::params![
            issue.id,
            repo,
            issue.number,
            issue.title,
            issue.state,
            issue.body,
            author.map(|a| a.login.as_str()),
            author.and_then(|a| a.database_id),
            issue.author_association,
            issue.url,
            issue.updated_at.as_str(),
            now.as_str()
        ],
    )?;
    // The early return's comparison mutants are known-equivalent, and
    // stay: an insert-as-the-run's-only-change would need an absent row
    // beside an otherwise-unchanged PR, which no flow constructs (a PR's
    // first contact is itself a change, and rows never hard-delete), and
    // the mutant's fall-through freshen no-ops against the just-inserted
    // values.
    if inserted > 0 {
        *changed = true;
        return Ok(());
    }
    // Freshen only rows this writer owns, only when something moved.
    let n = exec(
        tx,
        "UPDATE issues SET title=?1, state=?2, body=?3, updated_at=?4, synced_at=?5 \
         WHERE repo=?6 AND number=?7 AND hydration_source='linked' \
           AND (title IS NOT ?1 OR state IS NOT ?2 OR body IS NOT ?3 OR updated_at IS NOT ?4)",
        rusqlite::params![
            issue.title,
            issue.state,
            issue.body,
            issue.updated_at.as_str(),
            now.as_str(),
            repo,
            issue.number
        ],
    )?;
    if n > 0 {
        *changed = true;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Summary

fn summary(tallies: &BTreeMap<String, RepoTally>) -> Value {
    let repos: Vec<Value> = tallies
        .iter()
        .map(|(repo, t)| {
            let mut errors = t.errors.clone();
            errors.sort();
            json!({
                "repo": repo,
                "counts": {
                    "fetched": t.fetched,
                    "upserted": t.upserted,
                    "unchanged": t.unchanged,
                    "filtered": t.filtered,
                    "observations": t.observations,
                    "soft_deleted": t.soft_deleted,
                },
                "refresh": {
                    "reverified": t.reverified,
                    "quiet_mutations_found": t.quiet_mutations_found,
                    "reverify_shed": t.reverify_shed,
                    "tail_hits": t.tail_hits,
                    "full_walks": t.full_walks,
                    "escalations": t.escalations,
                    "bodies_skipped": t.bodies_skipped,
                },
                "cost": {
                    "subprocess_count": t.subprocess_count,
                    "subprocess_seconds": t.subprocess_ms / 1_000,
                    "bytes_parsed": t.bytes_parsed,
                    "rate_cost": t.rate_cost,
                    "sleeps": t.sleeps,
                    "sleep_seconds": t.sleep_ms / 1_000,
                },
                "health": {
                    "truncated": t.truncated,
                    "quarantined": t.quarantined,
                    "discovery_truncated": t.discovery_truncated,
                    "deferred_at_floor": t.deferred_at_floor,
                    "watchdog_kills": t.watchdog_kills,
                    "masked_hits": t.masked_hits,
                    "rate_limit_unknown": t.rate_limit_unknown,
                    "errors": errors,
                },
            })
        })
        .collect();
    let rate_remaining = tallies.values().filter_map(|t| t.remaining).min();
    json!({
        "sync": {
            "repos": repos,
            "rate_remaining": rate_remaining,
        }
    })
}

/// Did this run leave the archive incomplete? The `sync --strict` gate
/// predicate: main.rs derives the exit code from this over the summary that
/// was emitted, with no flag in scope on the writing path — gate flags
/// change the exit code, never a byte of JSON, and reading the DISCLOSED
/// document means the gate and a consumer parsing stdout cannot disagree.
///
/// Gating health tallies: `truncated`, `quarantined`, `discovery_truncated`,
/// `deferred_at_floor`, and a non-empty `errors` — each names data the
/// archive is known to be missing after the run. Excluded on purpose:
/// `watchdog_kills` and `rate_limit_unknown` (an event retried to success
/// leaves the archive complete — if it wasn't, one of the gating tallies
/// already fired) and `masked_hits` (masking is the discovery mechanism
/// working, not data missing). The targeted form (`sync --pr`) is incomplete
/// iff its one hydration reports truncated. A document this function cannot
/// read counts as incomplete — fail-open, the report::attention_has_demands
/// posture; unreachable from [`run`]'s output by construction, pinned by
/// test against drift.
pub fn incomplete(doc: &Value) -> bool {
    if let Some(pr) = doc.pointer("/sync/pr") {
        return pr.get("truncated").and_then(Value::as_bool) != Some(false);
    }
    match doc.pointer("/sync/repos").and_then(Value::as_array) {
        None => true,
        Some(repos) => repos.iter().any(|r| {
            let health = &r["health"];
            // Tally counts; deferred_at_floor is the per-repo FLAG (the
            // floor is run-wide, so the summary carries a bool, not an
            // event count).
            ["truncated", "quarantined", "discovery_truncated"]
                .iter()
                .any(|k| health.get(*k).and_then(Value::as_u64) != Some(0))
                || health.get("deferred_at_floor").and_then(Value::as_bool) != Some(false)
                || health
                    .get("errors")
                    .and_then(Value::as_array)
                    .is_none_or(|e| !e.is_empty())
        }),
    }
}

// ---------------------------------------------------------------------------
// sync --pr: the targeted path (doc on `run`)

fn run_targeted(cfg: &Config, archive: &mut RwArchive, reference: &str) -> Result<Value> {
    let (repo, number) = refs::parse_pr_ref(reference).ok_or_else(|| {
        Error::user(format!(
            "cannot parse PR reference {reference:?} — use owner/name#123 or a \
             https://github.com/owner/name/pull/123 URL"
        ))
    })?;
    let rc = cfg
        .repos
        .iter()
        .map(|e| e.resolved())
        .find(|rc| rc.repo == repo)
        .ok_or_else(|| {
            Error::user(format!(
                "repo {:?} is not in the config — discovery scope is the config, and --pr \
                 does not widen it; add the repo to sync it",
                repo.as_str()
            ))
        })?;

    let now = Rfc3339Utc::now();
    let mut gh_ctx = GhCtx::new(cfg.retry_attempts, cfg.retry_budget);

    // Resolve the node id: the archive first, then the PR_ID lookup.
    let known_id: Option<String> = archive
        .conn()
        .query_row(
            // deleted_at IS NULL: a drained row's stale node id would just
            // re-run the null→quarantine→drain cycle forever; falling
            // through to the live PR_ID lookup lets GitHub say whether the
            // PR is truly gone (USER_INPUT, immediately) or was reborn
            // under a new id (closure-pass S2).
            "SELECT id FROM prs WHERE repo = ?1 AND number = ?2 AND deleted_at IS NULL",
            rusqlite::params![repo.as_str(), i64::try_from(number).unwrap_or(i64::MAX)],
            |r| r.get(0),
        )
        .map(Some)
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;
    let id = match known_id {
        Some(id) => id,
        None => {
            let (owner, name) = repo
                .as_str()
                .split_once('/')
                .expect("RepoName always carries one slash");
            let doc = queries::pr_id_document(number);
            let resp = gh::graphql(&doc, &[("owner", owner), ("name", name)], &mut gh_ctx)
                .map_err(|e| e.error)?;
            let repo_node = parse::pr_id(&resp.data)
                .map_err(|e| Error::transient(format!("PR lookup failed: {e}")))?;
            match repo_node.and_then(|r| r.pull_request) {
                Some(pr) => pr.id,
                None => {
                    return Err(Error::user(format!(
                        "{}#{number} does not exist or is not visible to this account",
                        repo.as_str()
                    )));
                }
            }
        }
    };

    let quarantine_attempts: Option<u32> = archive
        .conn()
        .query_row(
            "SELECT attempts FROM quarantine WHERE id = ?1",
            [&id],
            |r| r.get::<_, i64>(0),
        )
        .map(|a| Some(u32::try_from(a).unwrap_or(0)))
        .or_else(none_if_no_rows)
        .map_err(|e| classify_sql(&e))?;

    // Floor-exempt on purpose (doc on `run`); the closure never trips.
    // Floor-exempt on purpose (doc on `run`): u32::MIN floor, inert closure.
    match hydrate_one(&mut gh_ctx, repo.as_str(), &id, Origin::Targeted, 0, || {
        false
    }) {
        HydrateEnd::Bundle(bundle) => {
            // hydrate_one constructs Pr bundles only; the match keeps the
            // targeted path total if that ever changes.
            let Bundle::Pr(bundle) = bundle else {
                return Err(Error::internal(
                    "targeted PR hydration produced a non-PR bundle",
                ));
            };
            if let Some(author) = &bundle.pr.author {
                let bot_excluded = author.is_bot() && !rc.bots();
                let listed = rc
                    .exclude_authors
                    .iter()
                    .any(|p| p.matches(author.login.as_str(), &author.typename));
                if bot_excluded || listed {
                    let filter = if bot_excluded {
                        "bots"
                    } else {
                        "exclude_authors"
                    };
                    return Err(Error::user(format!(
                        "{}#{number} is excluded by this repo's `{filter}` filter — honoring \
                         the demand would create archive states no config explains; relax the \
                         filter to sync it",
                        repo.as_str()
                    )));
                }
            }
            let mut t = RepoTally::default();
            let tx = archive
                .conn_mut()
                .transaction()
                .map_err(|e| classify_sql(&e))?;
            apply_pr_bundle(&tx, &now, &bundle, &mut t)?;
            exec(
                &tx,
                "DELETE FROM quarantine WHERE id = ?1",
                rusqlite::params![id],
            )?;
            tx.commit().map_err(|e| classify_sql(&e))?;
            Ok(json!({
                "sync": {
                    "pr": {
                        "repo": repo.as_str(),
                        "number": number,
                        "outcome": "hydrated",
                        "verified": bundle.verified(),
                        "truncated": !bundle.verified(),
                    }
                }
            }))
        }
        HydrateEnd::Vanished => {
            // An explicit demand consumes one retry attempt through backoff.
            let attempts = quarantine_attempts.unwrap_or(0) + 1;
            if attempts >= QUARANTINE_DRAIN_ATTEMPTS {
                let tx = archive
                    .conn_mut()
                    .transaction()
                    .map_err(|e| classify_sql(&e))?;
                exec(
                    &tx,
                    "UPDATE prs SET deleted_at = ?1 WHERE id = ?2 AND deleted_at IS NULL",
                    rusqlite::params![now.as_str(), id],
                )?;
                exec(
                    &tx,
                    "DELETE FROM quarantine WHERE id = ?1",
                    rusqlite::params![id],
                )?;
                tx.commit().map_err(|e| classify_sql(&e))?;
                return Err(Error::user(format!(
                    "{}#{number} no longer resolves upstream (deleted or access lost); the \
                     archived row, if any, is now marked deleted",
                    repo.as_str()
                )));
            }
            quarantine_targeted(archive, repo.as_str(), &now, &id, attempts, "node_null")?;
            Err(Error::transient(format!(
                "{}#{number} did not resolve (attempt {attempts}); quarantined for retry",
                repo.as_str()
            )))
        }
        HydrateEnd::ParseDrift => {
            let attempts = quarantine_attempts.unwrap_or(0) + 1;
            quarantine_targeted(archive, repo.as_str(), &now, &id, attempts, "parse")?;
            Err(Error::transient(format!(
                "{}#{number} hydration did not match the expected response shape \
                 (attempt {attempts}); quarantined for retry",
                repo.as_str()
            )))
        }
        HydrateEnd::Retryable => {
            let attempts = quarantine_attempts.unwrap_or(0) + 1;
            quarantine_targeted(archive, repo.as_str(), &now, &id, attempts, "transient")?;
            Err(Error::transient(format!(
                "{}#{number} hydration failed (attempt {attempts}); quarantined for retry",
                repo.as_str()
            )))
        }
        HydrateEnd::Renamed => Err(Error::config(format!(
            "repo {}: the PR reports a different repository — renamed or transferred \
             upstream; update the config entry",
            repo.as_str()
        ))),
        HydrateEnd::RateExhausted => Err(Error::transient(
            "the GraphQL point budget is exhausted; retry after it resets",
        )
        // The freeze batch's TRANSIENT disclosure: resetAt is API data the
        // telemetry already carries when any call in this run returned a
        // rateLimit envelope; absent one, the envelope simply omits it
        // ("when gh returned one" — DESIGN.md, command surface).
        .with_retry_after(gh_ctx.tel.reset_at.as_ref().map(|t| t.as_str().to_string()))),
        HydrateEnd::Fatal(error) => Err(error),
    }
}

fn quarantine_targeted(
    archive: &mut RwArchive,
    repo: &str,
    now: &Rfc3339Utc,
    id: &str,
    attempts: u32,
    class: &str,
) -> Result<()> {
    // --pr is PR-only by its verb contract; the record's stream follows.
    let record = quarantine_record_at(now, id, Stream::Pr, attempts, class);
    let tx = archive
        .conn_mut()
        .transaction()
        .map_err(|e| classify_sql(&e))?;
    upsert_quarantine(&tx, repo, &record)?;
    tx.commit().map_err(|e| classify_sql(&e))
}

// ---------------------------------------------------------------------------
// SQL plumbing

fn exec(tx: &rusqlite::Transaction, sql: &str, params: impl rusqlite::Params) -> Result<usize> {
    let mut stmt = tx.prepare_cached(sql).map_err(|e| classify_sql(&e))?;
    stmt.execute(params).map_err(|e| classify_sql(&e))
}

fn none_if_no_rows<T>(e: rusqlite::Error) -> std::result::Result<Option<T>, rusqlite::Error> {
    match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        other => Err(other),
    }
}

fn if_no_rows<T>(e: rusqlite::Error, v: T) -> std::result::Result<T, rusqlite::Error> {
    match e {
        rusqlite::Error::QueryReturnedNoRows => Ok(v),
        other => Err(other),
    }
}

/// Classify a writer-path SQLite failure by the actor who can fix it (the
/// no-blanket-From rule): busy/locked → TRANSIENT; a full disk →
/// CONFIGURATION with the disposable-cache remedy; anything else in OUR
/// prepared statements is a ghgraph bug → INTERNAL.
fn classify_sql(e: &rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(err, _) = e {
        if matches!(
            err.code,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
        ) {
            return Error::transient(format!("archive is busy: {e}"));
        }
        if err.code == rusqlite::ErrorCode::DiskFull {
            return Error::config(format!(
                "disk full while writing the archive: {e} — the archive is a disposable \
                 cache; free space or remove it and resync"
            ));
        }
    }
    Error::internal(format!("archive write failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse as parse_config;

    fn cfg(json: &str) -> Config {
        parse_config(json, "<test>").unwrap()
    }

    fn fp(cfg_json: &str) -> Value {
        let c = cfg(cfg_json);
        fingerprint(&c, &c.repos[0].resolved())
    }

    // --- fingerprint + transition: the config-change contract ---

    #[test]
    fn fingerprint_is_canonical_and_scope_aware() {
        let working = fp(r#"{"viewer":"Viewer","repos":["o/n"],"people":["Bob","alice"]}"#);
        assert_eq!(working["viewer"], "viewer", "canonical fold");
        assert_eq!(
            working["people"],
            json!(["alice", "bob"]),
            "sorted, folded — list order is not identity"
        );
        // Project scope pins viewer/people empty: they do not shape its
        // discovery, so their edits must not cold-start or backfill it.
        let project =
            fp(r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project"}],"people":["x"]}"#);
        assert_eq!(project["viewer"], "");
        assert_eq!(project["people"], json!([]));
        assert_eq!(project["bots"], json!(false), "project default");
    }

    #[test]
    fn transitions_follow_the_relaxation_rules() {
        let base = r#"{"viewer":"v","repos":[{"repo":"o/n","lookback_days":30}],"people":["a"]}"#;
        let old = serde_json::to_string(&fp(base)).unwrap();
        let state = ("2026-07-01T00:00:00Z".to_string(), old);

        // Equal → incremental.
        assert!(matches!(
            transition(Some(&state), &fp(base)),
            Transition::Incremental
        ));
        // Person added → targeted backfill of exactly the new flavor.
        let added = fp(
            r#"{"viewer":"v","repos":[{"repo":"o/n","lookback_days":30}],"people":["a","NewB"]}"#,
        );
        match transition(Some(&state), &added) {
            Transition::Backfill(people) => {
                assert_eq!(people.len(), 1);
                assert_eq!(people[0].as_str(), "newb");
            }
            _ => panic!("person added must backfill"),
        }
        // Person removed → tightening, nothing.
        let removed = fp(r#"{"viewer":"v","repos":[{"repo":"o/n","lookback_days":30}]}"#);
        assert!(matches!(
            transition(Some(&state), &removed),
            Transition::Incremental
        ));
        // Lookback increased → cold start; decreased → nothing.
        let longer =
            fp(r#"{"viewer":"v","repos":[{"repo":"o/n","lookback_days":60}],"people":["a"]}"#);
        assert!(matches!(
            transition(Some(&state), &longer),
            Transition::ColdStart
        ));
        let shorter =
            fp(r#"{"viewer":"v","repos":[{"repo":"o/n","lookback_days":7}],"people":["a"]}"#);
        assert!(matches!(
            transition(Some(&state), &shorter),
            Transition::Incremental
        ));
        // Viewer changed → cold start.
        let other_viewer =
            fp(r#"{"viewer":"w","repos":[{"repo":"o/n","lookback_days":30}],"people":["a"]}"#);
        assert!(matches!(
            transition(Some(&state), &other_viewer),
            Transition::ColdStart
        ));
        // Filter relaxed (an exclusion dropped) → cold start; tightened →
        // nothing.
        let excl = r#"{"viewer":"v","repos":[{"repo":"o/n","lookback_days":30,"exclude_authors":["x"]}],"people":["a"]}"#;
        let excl_state = (
            "2026-07-01T00:00:00Z".to_string(),
            serde_json::to_string(&fp(excl)).unwrap(),
        );
        assert!(matches!(
            transition(Some(&excl_state), &fp(base)),
            Transition::ColdStart
        ));
        assert!(matches!(
            transition(Some(&state), &fp(excl)),
            Transition::Incremental
        ));
        // No stored state → cold start. Unreadable stored state → cold start.
        assert!(matches!(transition(None, &fp(base)), Transition::ColdStart));
        let garbage = ("x".to_string(), "not json".to_string());
        assert!(matches!(
            transition(Some(&garbage), &fp(base)),
            Transition::ColdStart
        ));
    }

    #[test]
    fn bots_and_exclusion_swaps_classify_correctly() {
        // bots false→true is a relaxation (cold start); true→false is a
        // tightening (nothing). Project scope so the default is false.
        let off = r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project","issues":false}]}"#;
        let on = r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project","issues":false,"bots":true}]}"#;
        let off_state = (
            "2026-07-01T00:00:00Z".to_string(),
            serde_json::to_string(&fp(off)).unwrap(),
        );
        assert!(matches!(
            transition(Some(&off_state), &fp(on)),
            Transition::ColdStart
        ));
        let on_state = (
            "2026-07-01T00:00:00Z".to_string(),
            serde_json::to_string(&fp(on)).unwrap(),
        );
        assert!(matches!(
            transition(Some(&on_state), &fp(off)),
            Transition::Incremental
        ));

        // Swapping one exclusion for another both removes and adds: the
        // removal is the relaxation, and it dominates.
        let x = r#"{"viewer":"v","repos":[{"repo":"o/n","exclude_authors":["x"]}]}"#;
        let y = r#"{"viewer":"v","repos":[{"repo":"o/n","exclude_authors":["y"]}]}"#;
        let x_state = (
            "2026-07-01T00:00:00Z".to_string(),
            serde_json::to_string(&fp(x)).unwrap(),
        );
        assert!(matches!(
            transition(Some(&x_state), &fp(y)),
            Transition::ColdStart
        ));
    }

    #[test]
    fn person_added_and_filter_relaxed_together_cold_starts() {
        // Backfill covers only the new involves: flavor; a simultaneous
        // relaxation needs history no backfill reaches, so cold start wins.
        let old =
            r#"{"viewer":"v","repos":[{"repo":"o/n","exclude_authors":["x"]}],"people":["a"]}"#;
        let state = (
            "2026-07-01T00:00:00Z".to_string(),
            serde_json::to_string(&fp(old)).unwrap(),
        );
        let new = fp(r#"{"viewer":"v","repos":["o/n"],"people":["a","b"]}"#);
        assert!(matches!(
            transition(Some(&state), &new),
            Transition::ColdStart
        ));
    }

    // --- window splitting ---

    #[test]
    fn split_halves_and_respects_the_floor() {
        let since = Rfc3339Utc::parse("2026-07-01T00:00:00Z").unwrap();
        let until = Rfc3339Utc::parse("2026-07-03T00:00:00Z").unwrap();
        let mid = split_point(&since, Some(&until)).expect("splittable");
        assert_eq!(mid.as_str(), "2026-07-02T00:00:00Z");
        // Too narrow to split.
        let narrow = Rfc3339Utc::from_epoch(since.epoch() + 1).unwrap();
        assert!(split_point(&since, Some(&narrow)).is_none());
        // Open right edge splits against now.
        assert!(split_point(&since, None).is_some());
    }

    // --- the deterministic jitter ---

    #[test]
    fn fnv_jitter_is_stable_and_spreads() {
        // FNV-1a is a published algorithm; the vector below was computed
        // independently of this implementation, so a mutation of the hash
        // (or of its byte feed) cannot self-consistently pass.
        assert_eq!(fnv1a("o/n", 1), 15_678_916_660_073_372_886);
        assert_ne!(fnv1a("o/n", 1), fnv1a("o/n", 2));
        assert_ne!(fnv1a("o/n", 1), fnv1a("o/m", 1));
    }

    // The re-verify due boundary, pinned to the second: due at exactly
    // verified_at + period + (fnv1a(repo, number) mod period). The jitter
    // constant is computed independently of fnv1a (the vector above mod
    // 604800 = 457686), so the schedule arithmetic cannot drift
    // self-consistently. NULL verified_at leads; quarantined ids never
    // re-verify, whatever their age.
    #[test]
    fn reverify_due_boundary_and_exclusions() {
        let dir = std::env::temp_dir().join(format!("ghgraph-reverify-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let archive = crate::db::open_rw(&dir.join("g.db")).unwrap();
        let conn = archive.conn();
        let insert = |id: &str, number: i64, verified_at: Option<&str>| {
            conn.execute(
                "INSERT INTO prs (id, repo, number, title, body, state, created_at, \
                                  updated_at, url, verified_at) \
                 VALUES (?1, 'o/n', ?2, 't', '', 'OPEN', '2026-06-01T00:00:00Z', \
                         '2026-06-01T00:00:00Z', 'u', ?3)",
                rusqlite::params![id, number, verified_at],
            )
            .unwrap();
        };
        insert("PR_1", 1, Some("2026-07-01T00:00:00Z")); // epoch 1782864000
        insert("PR_2", 2, None); // never witnessed: always due, and first
        insert("PR_Q", 3, None); // quarantined: excluded

        let cfg = cfg(r#"{"viewer":"v","repos":["o/n"]}"#); // open tier: 7d
        let quarantined: HashSet<String> = [String::from("PR_Q")].into();
        let boundary = 1_782_864_000 + 7 * 86_400 + 457_686;

        let before = Rfc3339Utc::from_epoch(boundary - 1).unwrap();
        let due = reverify_due(conn, "o/n", &cfg, &before, &quarantined, false).unwrap();
        assert_eq!(
            due,
            vec![("PR_2".to_string(), 2, Stream::Pr)],
            "one second early: only the never-verified row is due"
        );

        let at = Rfc3339Utc::from_epoch(boundary).unwrap();
        let due = reverify_due(conn, "o/n", &cfg, &at, &quarantined, false).unwrap();
        assert_eq!(
            due,
            vec![
                ("PR_2".to_string(), 2, Stream::Pr),
                ("PR_1".to_string(), 1, Stream::Pr)
            ],
            "at the jittered boundary the aged row joins, after the NULL"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // The issue tiers: only stream-owned rows enter the schedule (a linked
    // row is a fill-only cache — re-verifying it would widen working scope
    // into the issue hydration DESIGN.md forbids), quarantine excludes as
    // for PRs, and the closed tier bounds by updated_at (the containment
    // argument at reverify_due). Off means off: no issue rows, whatever
    // the table holds.
    #[test]
    fn reverify_due_issue_tiers_respect_source_and_gate() {
        let dir = std::env::temp_dir().join(format!("ghgraph-reverify-iss-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let archive = crate::db::open_rw(&dir.join("g.db")).unwrap();
        let conn = archive.conn();
        let insert = |id: &str, number: i64, state: &str, source: &str, updated: &str| {
            conn.execute(
                "INSERT INTO issues (id, repo, number, title, state, body, url, updated_at, \
                                     hydration_source, synced_at) \
                 VALUES (?1, 'o/n', ?2, 't', ?3, '', 'u', ?4, ?5, '2026-06-01T00:00:00Z')",
                rusqlite::params![id, number, state, updated, source],
            )
            .unwrap();
        };
        insert("I_1", 1, "OPEN", "stream", "2026-06-01T00:00:00Z");
        insert("I_2", 2, "OPEN", "linked", "2026-06-01T00:00:00Z"); // cache: never
        insert("I_3", 3, "OPEN", "stream", "2026-06-01T00:00:00Z"); // quarantined
        insert("I_4", 4, "CLOSED", "stream", "2026-07-01T00:00:00Z"); // in lookback
        insert("I_5", 5, "CLOSED", "stream", "2020-01-01T00:00:00Z"); // beyond it

        let cfg = cfg(r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project"}]}"#);
        let quarantined: HashSet<String> = [String::from("I_3")].into();
        let now = Rfc3339Utc::parse("2026-07-10T00:00:00Z").unwrap();

        let due = reverify_due(conn, "o/n", &cfg, &now, &quarantined, true).unwrap();
        assert_eq!(
            due,
            vec![
                ("I_1".to_string(), 1, Stream::Issue),
                ("I_4".to_string(), 4, Stream::Issue)
            ],
            "stream-owned, unquarantined, and (for CLOSED) within lookback only"
        );
        let due = reverify_due(conn, "o/n", &cfg, &now, &quarantined, false).unwrap();
        assert_eq!(due, Vec::new(), "issue stream off: no issue re-verify");
        drop(archive);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- quarantine backoff ---

    #[test]
    fn quarantine_backoff_doubles_and_caps() {
        let now = Rfc3339Utc::parse("2026-07-01T00:00:00Z").unwrap();
        let r1 = quarantine_record_at(&now, "X", Stream::Pr, 1, "transient");
        let r2 = quarantine_record_at(&now, "X", Stream::Pr, 2, "transient");
        let r9 = quarantine_record_at(&now, "X", Stream::Pr, 9, "transient");
        assert_eq!(r1.next_retry_at, "2026-07-01T01:00:00Z", "base 1h");
        assert_eq!(r2.next_retry_at, "2026-07-01T02:00:00Z", "doubled");
        assert_eq!(r9.next_retry_at, "2026-07-08T00:00:00Z", "capped at 7d");
        assert_eq!(r1.attempts, 1);
    }

    // --- the writer's SQL failure classification ---

    // One case per arm: busy retries (TRANSIENT), a full disk is
    // operator-fixable with the disposable-cache remedy (CONFIGURATION),
    // and anything else in our own statements is a ghgraph bug (INTERNAL).
    #[test]
    fn classify_sql_names_the_actor_per_arm() {
        use crate::error::Code;
        let sqlite = |code: std::os::raw::c_int| {
            rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None)
        };
        let busy = classify_sql(&sqlite(rusqlite::ffi::SQLITE_BUSY));
        assert_eq!(busy.code, Code::Transient);
        let full = classify_sql(&sqlite(rusqlite::ffi::SQLITE_FULL));
        assert_eq!(full.code, Code::Configuration);
        assert!(full.message.contains("disposable"), "{}", full.message);
        let other = classify_sql(&rusqlite::Error::QueryReturnedNoRows);
        assert_eq!(other.code, Code::Internal);
    }

    // --- incremental since: overlap + lookback clamp ---

    #[test]
    fn incremental_since_applies_overlap_and_clamp() {
        let lookback = Rfc3339Utc::parse("2026-04-01T00:00:00Z").unwrap();
        let state = ("2026-07-01T00:10:00Z".to_string(), String::new());
        let since = incremental_since(Some(&state), &lookback);
        assert_eq!(since.as_str(), "2026-07-01T00:00:00Z", "10min overlap");
        // A watermark older than the lookback clamps to the lookback.
        let old_state = ("2026-01-01T00:00:00Z".to_string(), String::new());
        let since = incremental_since(Some(&old_state), &lookback);
        assert_eq!(since.as_str(), lookback.as_str());
        assert_eq!(
            incremental_since(None, &lookback).as_str(),
            lookback.as_str()
        );
    }

    // --- tail conservation: exhaustive model enumeration ---

    // The check is a pure decision function, so its safety property is
    // provable by cases over a model of upstream histories: every archived
    // list up to 4 ids, every deletion subset, up to 2 adds, every tail
    // length (the walked tail is a suffix; deletions strike anywhere).
    // Two models, one theorem each:
    //
    //   STRICT (adds append — the creation-order precondition the check
    //   states): ZERO false hits. This is stronger than the round-0
    //   disclosure: an anchored suffix necessarily covers every appended
    //   add, so "deletion offset by non-tail-visible adds" cannot balance
    //   while anchored — at the id level, under the preconditions, the
    //   inference is EXACT, and the only residual masked case is the body
    //   edit (invisible to ids by construction; pipeline-tested catcher).
    //
    //   WEAKENED (adds at ANY position — the order precondition broken,
    //   e.g. an imported comment backdated into the middle): false hits
    //   exist but are confined EXACTLY to the documented shape — partial
    //   observation, deletions == non-tail-visible adds. This pins the
    //   blast radius of a precondition failure to the disclosed tolerance;
    //   any false hit outside it is the third masked case the round-0
    //   panel hunted, and fails this test.
    //
    // Completeness rides along in the strict model: no deletions and an
    // anchored (or fully-observed) tail always Hits, so unchanged PRs,
    // appended comments, and zero-comment PRs never over-escalate.
    #[test]
    fn tail_conservation_enumerated_against_the_model() {
        let archived_ids = ["a0", "a1", "a2", "a3"];
        let add_ids = ["n0", "n1"];

        // All live lists reachable by inserting `adds` ids into
        // `survivors`: at the end only (strict) or anywhere (weakened).
        fn arrangements<'a>(
            survivors: &[&'a str],
            adds: &[&'a str],
            append_only: bool,
        ) -> Vec<Vec<&'a str>> {
            let mut acc = vec![survivors.to_vec()];
            for add in adds {
                let mut next = Vec::new();
                for s1 in &acc {
                    let positions = if append_only {
                        s1.len()..=s1.len()
                    } else {
                        0..=s1.len()
                    };
                    for pos in positions {
                        let mut v = s1.clone();
                        v.insert(pos, add);
                        next.push(v);
                    }
                }
                acc = next;
            }
            acc
        }

        for append_only in [true, false] {
            let mut cases = 0u32;
            let mut masked = 0u32;
            for archived_len in 0..=archived_ids.len() {
                let s0 = &archived_ids[..archived_len];
                let archived_live: HashSet<String> = s0.iter().map(|s| s.to_string()).collect();
                for deleted_mask in 0u32..(1 << archived_len) {
                    let survivors: Vec<&str> = s0
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| deleted_mask & (1 << i) == 0)
                        .map(|(_, id)| *id)
                        .collect();
                    let deletions = archived_len - survivors.len();
                    for adds in 0..=add_ids.len() {
                        for s1 in arrangements(&survivors, &add_ids[..adds], append_only) {
                            for k in 0..=s1.len() {
                                cases += 1;
                                let tail = &s1[s1.len() - k..];
                                let fully_observed = k == s1.len();
                                let verdict = tail_conservation(
                                    &archived_live,
                                    s1.len() as i64,
                                    tail,
                                    fully_observed,
                                );
                                // Ground truth: a tail-hit apply keeps every
                                // archived row (no sweep) and upserts the tail.
                                let mut after: HashSet<&str> = s0.iter().copied().collect();
                                after.extend(tail);
                                let live: HashSet<&str> = s1.iter().copied().collect();
                                let sound = after == live;
                                let tail_set: HashSet<&str> = tail.iter().copied().collect();
                                let adds_outside_tail = add_ids[..adds]
                                    .iter()
                                    .filter(|id| !tail_set.contains(**id))
                                    .count();
                                match verdict {
                                    TailVerdict::Hit if !sound => {
                                        masked += 1;
                                        assert!(
                                            !append_only,
                                            "STRICT-MODEL false hit: archived={s0:?} \
                                             deleted_mask={deleted_mask:b} s1={s1:?} k={k}"
                                        );
                                        assert!(
                                            !fully_observed
                                                && deletions > 0
                                                && deletions == adds_outside_tail,
                                            "UNDOCUMENTED false hit: archived={s0:?} \
                                             deleted_mask={deleted_mask:b} s1={s1:?} k={k}"
                                        );
                                    }
                                    TailVerdict::Escalate(reason)
                                        if append_only
                                            && deletions == 0
                                            && (fully_observed
                                                || tail
                                                    .iter()
                                                    .any(|t| archived_live.contains(*t))) =>
                                    {
                                        panic!(
                                            "over-escalation ({reason}): archived={s0:?} \
                                             s1={s1:?} k={k}"
                                        );
                                    }
                                    TailVerdict::Hit if deletions > 0 && adds == 0 => {
                                        panic!(
                                            "a pure deletion balanced?! archived={s0:?} \
                                             deleted_mask={deleted_mask:b} k={k}"
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            if append_only {
                assert_eq!(cases, 333, "the strict model space ran, none skipped");
                assert_eq!(masked, 0, "under the preconditions the inference is exact");
            } else {
                assert_eq!(cases, 2080, "the weakened model space ran, none skipped");
                assert!(
                    masked > 0,
                    "the documented tolerance is reachable, not vacuous"
                );
            }
        }
    }

    // upsert_pr reads old_cols[17] as `truncated` for the TailHit carry —
    // the one place a positional column index feeds a DECISION rather
    // than the uniform diff. Pin the position against the SELECT text
    // itself so a column reorder fails here, not silently in the carry
    // (B3 panel, S3).
    #[test]
    fn upsert_pr_truncated_column_position_is_pinned() {
        let cols: Vec<&str> = PR_SELECT
            .trim_start_matches("SELECT ")
            .split(" FROM ")
            .next()
            .unwrap()
            .split(',')
            .map(str::trim)
            .collect();
        // cols[0] is pk (not part of old_cols); old_cols[i] = cols[i + 1].
        assert_eq!(cols.len(), 22, "the 1..21 read loop spans every column");
        assert_eq!(cols[18], "truncated", "old_cols[17] must be truncated");
    }

    // The check's degenerate arms, pinned individually: the discriminating
    // inputs are named so a regression names its arm.
    #[test]
    fn tail_conservation_arm_pins() {
        let arch: HashSet<String> = ["a".to_string()].into();
        // Duplicate id across pages: the connection moved under the walk.
        assert_eq!(
            tail_conservation(&arch, 2, &["n", "n"], false),
            TailVerdict::Escalate("duplicate tail id")
        );
        // Negative count is API nonsense, not arithmetic fodder.
        assert_eq!(
            tail_conservation(&arch, -1, &[], true),
            TailVerdict::Escalate("negative totalCount")
        );
        // Partial observation without an anchor never hits, even balanced:
        // 1 archived + 1 new == 2, but nothing ties the suffix to the
        // archive (round-0: no induction base).
        assert_eq!(
            tail_conservation(&arch, 2, &["n"], false),
            TailVerdict::Escalate("no induction anchor")
        );
        // The fully-observed degenerate case: same shape, but the walk
        // reached the connection's start — balance alone proves
        // archived ⊆ fetched, and there is no middle to induct over.
        assert_eq!(
            tail_conservation(&arch, 2, &["a", "n"], true),
            TailVerdict::Hit
        );
        // Zero comments before and after: fully observed, trivially
        // balanced — the arm that keeps comment-less PRs on one call.
        assert_eq!(
            tail_conservation(&HashSet::new(), 0, &[], true),
            TailVerdict::Hit
        );
        // An archived id deleted upstream, fully observed: the count
        // drops, the imbalance forces the walk that sweeps.
        assert_eq!(
            tail_conservation(&arch, 0, &[], true),
            TailVerdict::Escalate("count imbalance")
        );
    }

    /// The --strict classification table: which disclosed fields gate and
    /// which deliberately do not (the doc on `incomplete` argues each).
    #[test]
    fn strict_incompleteness_reads_the_disclosed_summary() {
        let full_run = |health: Value| {
            json!({"sync": {"repos": [
                {"repo": "o/n", "health": health},
            ], "rate_remaining": 4000}})
        };
        let clean = json!({
            "truncated": 0, "quarantined": 0, "discovery_truncated": 0,
            "deferred_at_floor": false, "watchdog_kills": 0, "masked_hits": 0,
            "rate_limit_unknown": 0, "errors": [],
        });
        assert!(!incomplete(&full_run(clean.clone())));
        // Each gating field fires alone; the floor is the summary's one
        // BOOLEAN health field (run-wide flag, not an event count), so it
        // gets its own arm — and the type mismatch either way must read
        // incomplete, not clean (the shape-drift pin).
        for k in ["truncated", "quarantined", "discovery_truncated"] {
            let mut h = clean.clone();
            h[k] = json!(1);
            assert!(incomplete(&full_run(h)), "{k} must gate");
        }
        let mut h = clean.clone();
        h["deferred_at_floor"] = json!(true);
        assert!(incomplete(&full_run(h)), "deferred_at_floor must gate");
        let mut h = clean.clone();
        h["deferred_at_floor"] = json!(0);
        assert!(
            incomplete(&full_run(h)),
            "a number where the flag belongs is shape drift, never all-clear"
        );
        let mut h = clean.clone();
        h["errors"] = json!(["boom"]);
        assert!(incomplete(&full_run(h)), "errors must gate");
        // The deliberate exclusions: disclosed, never gating alone.
        for k in ["watchdog_kills", "masked_hits", "rate_limit_unknown"] {
            let mut h = clean.clone();
            h[k] = json!(3);
            assert!(!incomplete(&full_run(h)), "{k} must not gate");
        }
        // The targeted form gates on its one hydration's completeness.
        let targeted = |truncated: bool| {
            json!({"sync": {"pr": {
                "repo": "o/n", "number": 1, "outcome": "hydrated",
                "verified": !truncated, "truncated": truncated,
            }}})
        };
        assert!(!incomplete(&targeted(false)));
        assert!(incomplete(&targeted(true)));
        // Unreadable documents fail open (drift pin: reachable only if
        // run()'s output shape moves without this predicate moving).
        assert!(incomplete(&json!({})));
        assert!(incomplete(&json!({"sync": {"repos": "not an array"}})));
        assert!(incomplete(&full_run(
            json!({"truncated": "NaN", "errors": []})
        )));
    }
}
