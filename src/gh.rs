//! Transport: the `gh` CLI as a subprocess. gh owns auth, SSO, TLS, and host
//! selection; ghgraph carries no HTTP or TLS dependencies because of it.
//! `gh` is a documented runtime prerequisite, gated at sync start by
//! [`version_gate`].
//!
//! Invariants:
//!   * The GraphQL document goes to gh on stdin (`-F query=@-`) — argv size
//!     limits can never apply, regardless of query growth. The write happens
//!     on its own thread: a child that never reads stdin cannot wedge the
//!     caller, and a child that exits before reading turns the write into an
//!     ignored EPIPE — the exit status and body decide the outcome, never
//!     the write.
//!   * Environment hygiene: GH_PAGER is cleared and GH_PROMPT_DISABLED=1 so an
//!     unattended run can never block on a pager or prompt.
//!   * Subprocess contract: both pipes are drained by dedicated threads (no
//!     pipe deadlock on multi-MB responses) and the child is always reaped.
//!     The watchdog is a `try_wait` poll under an `Instant` deadline — kill
//!     on expiry, then `wait` — because `Child::wait` cannot be interrupted
//!     from another thread in a process whose cancellation story is the
//!     absence of signal handlers. Command::output() was the first design
//!     and was abandoned: it blocks forever on a stalled child and nothing
//!     inside a no-signal-handler process can unstick it. Once the child
//!     is reaped — normal exit included — the wait for the drains is
//!     bounded (DRAIN_GRACE) and keeps what arrived: neither a kill nor
//!     the child's own exit can close a pipe end a grandchild inherited,
//!     so an unbounded post-exit read is a wedge on ANY path. Kill-anytime
//!     safety rests on replay idempotence (a killed window's redo is a
//!     no-op); a mid-walk kill marks truncated, never sweeps — the
//!     completeness witness guarantees it. The deadline is a constant
//!     (WATCHDOG_DEADLINE) until telemetry names a config consumer:
//!     subprocess_seconds tails from real syncs are the evidence that would
//!     promote it.
//!   * Success is decided by the body, not the exit code, in both
//!     directions. gh exits nonzero whenever the response carries a
//!     top-level "errors" array, even beside usable partial data — and
//!     GraphQL error-masking bubbles a failed sub-resolver to the nearest
//!     nullable field, which is exactly the set of spots parse.rs types for
//!     it (the three Option connections, nullable search hits, node: null).
//!     So: `data` present and non-null → Ok, and the masked nulls resolve
//!     downstream to defined outcomes (truncation, quarantine, deleted) —
//!     never silently empty; `data` null or absent → failure, classified
//!     from exit status and stderr. An HTTP 200 whose body carries errors
//!     and no data is still a failure even when the exit code says 0.
//!     Reversal: a masked-null case parse.rs cannot express as a defined
//!     outcome would force errors-array inspection here — none is known.
//!   * Retry policy is owned here, bounded, and configured (config.rs:
//!     `retry_attempts` per call, `retry_budget` per repo): the caller hands
//!     [`graphql`] a per-repo [`GhCtx`] and gh.rs decides what retries.
//!     Only transient classes retry — secondary rate limits on a long
//!     backoff, watchdog kills and unclassified failures on a short one.
//!     A PRIMARY rate limit never retries: it folds into the floor's
//!     defer-record-exit path (one budget, one mechanism), which is why the
//!     failure carries a typed kind ([`FailureKind::RateExhausted`]) the
//!     scheduler can match without string-sniffing.
//!   * gh's output is redacted for token shapes before any of it reaches
//!     an envelope: `gh[pousr]_` and `github_pat_` prefixes followed by
//!     8+ `[A-Za-z0-9_]` (see [`scrub_tokens`]), then capped at ~1KB.
//!     Two admission points exist, both scrub-then-cap: stderr on the
//!     classification table's default row, and `gh --version` stdout on
//!     the gate's parse-failure path.
//!   * gh does not retry rate limits, and its exit code cannot distinguish
//!     them; the failure class is parsed from stderr:
//!
//! ```text
//! "secondary rate limit"      → TRANSIENT, retried on a long backoff
//! "API rate limit exceeded"   → TRANSIENT, RateExhausted: defer, never retry
//! "Bad credentials"           → CONFIGURATION (token invalid/expired)
//! exit code 4                 → CONFIGURATION (gh auth login needed)
//! gh binary absent            → CONFIGURATION
//! anything else               → TRANSIENT with first ~1KB of stderr
//! ```
//!
//! The rows are checked in table order (stderr strings before the exit
//! code, ASCII-case-insensitively); classification never inspects stdout,
//! whose failure modes the body-decides rule above already owns. Exit 4 is
//! strictly "no credentials at all" (gh refuses before calling); a REJECTED
//! token is exit 1 with the API's "Bad credentials" relayed on stderr —
//! both probed live — and without its own row it would read TRANSIENT,
//! sending a retry loop after a failure only the operator can fix. The two
//! rate-limit strings are likewise API text relayed by gh, so they are
//! stable across gh versions; the sync-time viewer identity check
//! (milestone 2) catches dead auth up front, and these rows catch it
//! mid-run (tokens expire while syncs run).
//!
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
//! version skew degrades retry efficiency, never correctness. The version
//! gate below keeps that heuristic honest.

use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::time::Rfc3339Utc;

/// Kill a gh call that produces no exit within this deadline. A healthy
/// hydration is single-digit MB and completes in seconds even on a slow
/// link; 120s sits comfortably above any observed healthy call while
/// bounding a wedged one. A constant, not config — promoted only when
/// subprocess_seconds telemetry names the consumer.
const WATCHDOG_DEADLINE: Duration = Duration::from_secs(120);

/// `gh --version` prints instantly or something is wrong with the install.
const VERSION_DEADLINE: Duration = Duration::from_secs(10);

/// `gh api user` is one tiny REST round trip; a healthy call is
/// sub-second, and 30s absorbs a slow link without letting a wedged
/// credential helper stall the run's first step for the full watchdog span.
const IDENTITY_DEADLINE: Duration = Duration::from_secs(30);

/// Backoff bases for the two retried classes. Fixed schedules, no jitter:
/// jitter decorrelates fleets, and a sync is one client (the no-RNG rule
/// keeps summaries byte-stable anyway — sleep timing is not output, but a
/// second mechanism needs a reason). Constants until sync_runs telemetry
/// names a consumer, like WATCHDOG_DEADLINE.
const SECONDARY_BACKOFF: Duration = Duration::from_secs(30);
const TRANSIENT_BACKOFF: Duration = Duration::from_secs(1);

/// try_wait poll granularity: cheap enough to be negligible against a
/// network round trip, fine enough that the deadline error is small.
const POLL_INTERVAL: Duration = Duration::from_millis(15);

/// stderr detail admitted into a TRANSIENT envelope, after scrubbing.
const STDERR_CAP: usize = 1024;

/// Once the child is reaped (normal exit, watchdog kill, or wait error),
/// the TOTAL further wall time the collector may spend on both pipes (one
/// deadline spans them). The child's own exit
/// leaves at most a pipe buffer of residue, delivered in milliseconds; only
/// a descendant that inherited the pipe fds (gh spawns credential helpers)
/// can hold EOF open past that, on ANY exit path — so the bound is
/// unconditional, not a kill-path special case. On expiry the collector
/// keeps every chunk that arrived (a complete body written before a
/// lingering grandchild still parses; a truncated one fails parse and
/// classifies TRANSIENT — both defined) and the drain thread is left
/// blocked until the pipe finally closes — a leak bounded by the number of
/// affected calls, which quarantine backoff bounds in turn.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The oldest gh this build claims its exit-code taxonomy (4 = requires
/// auth, probed live at 2.96.0 alongside the bad-token and
/// partial-data-relay behaviors), stdin document passing (`-F query=@-`),
/// and prompt-disable env for. 2.40.0 (2023-11) comfortably postdates every
/// mechanism used here. The stderr strings themselves are GitHub API text
/// relayed by gh, so they barely depend on gh's version — the floor is for
/// the RELAY behaviors (exit codes, body-to-stdout passthrough). Raising it
/// is cheap (CONFIGURATION with an upgrade remedy); lowering it requires
/// re-verifying those behaviors against the older release.
pub const MIN_GH_VERSION: (u32, u32, u32) = (2, 40, 0);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    pub cost: u32,
    pub remaining: u32,
    /// A GraphQL DateTime scalar, validated at ingest like every other
    /// timestamp (parse.rs invariant): the milestone-2 scheduler sleeps
    /// toward this value, and a bare String would hand it unvalidated
    /// text. Extraction is missing-tolerant (a malformed envelope —
    /// missing keys, wrong types — yields `rate_limit: None`; extra keys
    /// are silently ignored for forward compatibility — deliberately no
    /// `deny_unknown_fields`, unlike the parse.rs types whose
    /// type=selection contract needs it), so validation here narrows,
    /// never breaks.
    pub reset_at: Rfc3339Utc,
}

// Debug exists for test diagnostics (unwrap_err needs it); no shipped code
// formats a Response — the parse.rs Debug caveat applies here too.
#[derive(Debug)]
pub struct Response {
    /// Must be produced by default-config serde_json (its byte parser caps
    /// nesting at 128): parse.rs's totality over this Value leans on that
    /// cap — the Value-to-typed deserializer recurses per level with no
    /// depth guard of its own, so an unbounded-depth Value could overflow
    /// the stack. Holds by construction in [`body_success`]; keep it true
    /// if the parsing path ever changes.
    pub data: serde_json::Value,
    /// The in-band `rateLimit` envelope, when the document selected it and
    /// it parsed; `None` otherwise — deliberately missing-tolerant, like
    /// parse.rs's own rate_limit fields. What the floor does about a `None`
    /// (fly blind vs. defer) is the scheduler's policy call, not transport's.
    pub rate_limit: Option<RateLimit>,
}

/// Why a call failed, typed for the scheduler — the summary and the defer
/// path match on this, never on message text (the "never string-sniffs"
/// rule). `error` carries the operator-facing classification (its `code`
/// already names the actor); `kind` carries the retry semantics.
#[derive(Debug)]
pub struct GhError {
    pub kind: FailureKind,
    pub error: Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// Primary point budget exhausted: never retried here — the scheduler
    /// folds it into the rate-limit floor's defer path (one mechanism).
    RateExhausted,
    /// Secondary (abuse) rate limit: retried on the long backoff.
    SecondaryLimit,
    /// The watchdog killed a stalled gh: retried on the short backoff,
    /// counted per call into [`Telemetry::watchdog_kills`].
    Watchdog,
    /// Operator-fixable (auth, missing binary): never retried.
    Config,
    /// Everything else on the classification table's default row: retried
    /// on the short backoff.
    Other,
}

/// Per-call accounting, accumulated per repo in [`GhCtx`] and folded into
/// the sync summary's `cost`/`health` groups. Every field feeds a named
/// summary field (the telemetry rule); durations are integer milliseconds
/// internally and print as whole seconds (no floats in output).
#[derive(Debug, Default, Clone)]
pub struct Telemetry {
    pub subprocess_count: u64,
    pub subprocess_ms: u64,
    pub bytes_parsed: u64,
    /// Per data-bearing, un-killed call: (stdout bytes, wall ms). Consumer:
    /// the per-run overhead-intercept regression written to sync_runs
    /// (sync.rs) — per-call pairs, because a regression over per-repo
    /// aggregates would conflate spawn overhead with payload size. Intra-run
    /// only; never persisted as-is.
    pub samples: Vec<(u64, u64)>,
    pub rate_cost: u64,
    pub sleeps: u64,
    pub sleep_ms: u64,
    pub watchdog_kills: u64,
    /// Successful responses whose rateLimit envelope was missing or
    /// malformed — the floor flew blind for that call. Nonzero detects a
    /// transport/envelope regression (its named consumer).
    pub rate_limit_unknown: u64,
    /// Latest observed budget state; the floor check reads these.
    pub remaining: Option<u32>,
    pub reset_at: Option<Rfc3339Utc>,
}

/// Per-repo call context: the configured retry policy, the repo's remaining
/// retry budget, and the accumulated telemetry. gh.rs owns what happens
/// between attempts; the caller owns what a final failure means.
#[derive(Debug)]
pub struct GhCtx {
    pub attempts_per_call: u32,
    pub retry_budget: u32,
    pub tel: Telemetry,
}

impl GhCtx {
    pub fn new(attempts_per_call: u32, retry_budget: u32) -> GhCtx {
        GhCtx {
            attempts_per_call,
            retry_budget,
            tel: Telemetry::default(),
        }
    }

    /// One attempt, no budget: the version-gate/test form.
    fn single() -> GhCtx {
        GhCtx::new(1, 0)
    }
}

/// One GraphQL round trip with the configured retry policy. `vars` become
/// string variables; typed variables are not needed by any current query
/// (PR_ID inlines its one Int — queries.rs records why).
pub fn graphql(
    query: &str,
    vars: &[(&str, &str)],
    ctx: &mut GhCtx,
) -> std::result::Result<Response, GhError> {
    graphql_ctx(Path::new("gh"), WATCHDOG_DEADLINE, query, vars, ctx)
}

/// [`graphql`] with the binary and deadline injectable, so the tests can run
/// a fake gh from a scratch directory without mutating process env (set_var
/// is `unsafe` in edition 2024; unsafe is forbidden crate-wide).
fn graphql_ctx(
    bin: &Path,
    deadline: Duration,
    query: &str,
    vars: &[(&str, &str)],
    ctx: &mut GhCtx,
) -> std::result::Result<Response, GhError> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let err = match graphql_once(bin, deadline, query, vars, &mut ctx.tel) {
            Ok(resp) => return Ok(resp),
            Err(err) => err,
        };
        let retryable = matches!(
            err.kind,
            FailureKind::SecondaryLimit | FailureKind::Watchdog | FailureKind::Other
        );
        if !retryable || attempt >= ctx.attempts_per_call.max(1) || ctx.retry_budget == 0 {
            return Err(err);
        }
        ctx.retry_budget -= 1;
        let pause = backoff(err.kind, attempt);
        ctx.tel.sleeps += 1;
        ctx.tel.sleep_ms += u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
        thread::sleep(pause);
    }
}

/// The pause before retry `attempt + 1`, by failure class: secondary rate
/// limits get the long linear schedule (GitHub's guidance is a generous
/// pause, and hammering converts throttling into a ban); everything else
/// gets a short doubling schedule capped at 8s (a blip either clears fast
/// or is not a blip).
fn backoff(kind: FailureKind, attempt: u32) -> Duration {
    match kind {
        FailureKind::SecondaryLimit => SECONDARY_BACKOFF * attempt,
        FailureKind::Watchdog | FailureKind::Other => {
            TRANSIENT_BACKOFF * (1u32 << (attempt - 1).min(3))
        }
        // Never retried (graphql_ctx filters them before any backoff): a
        // wildcard here would hand a future caller a plausible schedule for
        // a class that must not sleep-and-retry; a panic is a ghgraph bug
        // announcing itself (B2 panel, S4).
        FailureKind::RateExhausted | FailureKind::Config => {
            unreachable!("non-retryable FailureKind reached backoff()")
        }
    }
}

/// One spawn-to-reap round trip, telemetry recorded, no retry.
fn graphql_once(
    bin: &Path,
    deadline: Duration,
    query: &str,
    vars: &[(&str, &str)],
    tel: &mut Telemetry,
) -> std::result::Result<Response, GhError> {
    let mut args = vec![
        "api".to_string(),
        "graphql".to_string(),
        "-F".to_string(),
        "query=@-".to_string(),
    ];
    for (k, v) in vars {
        args.push("-f".to_string());
        args.push(format!("{k}={v}"));
    }
    let start = Instant::now();
    let out = run_gh(bin, &args, Some(query), deadline).map_err(|error| GhError {
        kind: FailureKind::Config,
        error,
    })?;
    let call_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    tel.subprocess_count += 1;
    // Wall-clock values are the contract's enumerated nondeterminism —
    // no test may PIN them — but a lower bound is contract-safe:
    // Instant::elapsed can never undershoot a child's sleep, and the
    // subprocess_ms_accumulates test asserts exactly that bound (killing
    // the *=-from-zero mutant deliberately; the -= sibling already dies
    // by debug underflow, now on purpose rather than by luck).
    tel.subprocess_ms += call_ms;
    tel.bytes_parsed += out.stdout.len() as u64;
    if out.killed {
        tel.watchdog_kills += 1;
    }
    // Body first, unconditionally: a complete, data-bearing response is a
    // success even from a child the watchdog had to kill after it wrote one.
    if let Some(resp) = body_success(&out.stdout) {
        if !out.killed {
            // One (bytes, ms) pair per data-bearing, un-killed call — the
            // raw material for sync_runs' per-run overhead-intercept
            // regression (sync.rs). Failed attempts are excluded on the
            // same argument as watchdog kills: a throttle wait or 5xx
            // measures the failure, not the bytes-to-time relationship —
            // and error points sit near x = 0, where they would steer the
            // intercept directly rather than average out.
            tel.samples.push((out.stdout.len() as u64, call_ms));
        }
        match &resp.rate_limit {
            Some(rl) => {
                tel.rate_cost += u64::from(rl.cost);
                tel.remaining = Some(rl.remaining);
                tel.reset_at = Some(rl.reset_at.clone());
            }
            None => tel.rate_limit_unknown += 1,
        }
        return Ok(resp);
    }
    if out.killed {
        return Err(GhError {
            kind: FailureKind::Watchdog,
            error: Error::transient(format!(
                "gh produced no exit within {}s and was killed by the watchdog",
                deadline.as_secs()
            )),
        });
    }
    Err(classify(out.status, &out.stderr))
}

/// The single-shot form the unit tests drive (one attempt, classification
/// flattened to the envelope error). Shipped code goes through [`graphql`].
#[cfg(test)]
fn graphql_with(
    bin: &Path,
    deadline: Duration,
    query: &str,
    vars: &[(&str, &str)],
) -> Result<Response> {
    graphql_ctx(bin, deadline, query, vars, &mut GhCtx::single()).map_err(|e| e.error)
}

/// The authenticated account's login, via `gh api user` (REST) — run once at
/// sync start beside the version gate. A viewer/config mismatch means every
/// working-scope search returns someone else's involvement (or nothing),
/// silently; the caller compares via `identity::login_eq` and refuses. The
/// returned login is API text: compared, bound, never interpolated into an
/// error message (the caller's mismatch envelope echoes only the config
/// value, whose echo is licensed).
pub fn viewer_login() -> Result<String> {
    viewer_login_with(Path::new("gh"), IDENTITY_DEADLINE)
}

fn viewer_login_with(bin: &Path, deadline: Duration) -> Result<String> {
    let args = ["api".to_string(), "user".to_string()];
    let out = run_gh(bin, &args, None, deadline)?;
    if out.killed {
        return Err(Error::transient(format!(
            "gh api user produced no exit within {}s and was killed by the watchdog",
            deadline.as_secs()
        )));
    }
    // Body decides, REST edition: the success body is the user object
    // itself (no `data` wrapper), and `login` is its required key.
    if out.status.is_some_and(|s| s.success())
        && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&out.stdout)
        && let Some(login) = v.get("login").and_then(|l| l.as_str())
    {
        return Ok(login.to_string());
    }
    Err(classify(out.status, &out.stderr).error)
}

/// The body-decides rule (module docs): Some iff stdout parses as JSON and
/// carries a non-null `data`. Uses default-config serde_json — the depth-cap
/// precondition [`Response::data`] documents.
fn body_success(stdout: &[u8]) -> Option<Response> {
    let mut body: serde_json::Value = serde_json::from_slice(stdout).ok()?;
    let data = body.get_mut("data")?.take();
    if data.is_null() {
        return None;
    }
    let rate_limit = data
        .get("rateLimit")
        .and_then(|v| RateLimit::deserialize(v).ok());
    Some(Response { data, rate_limit })
}

/// The stderr classification table (module docs), applied in table order.
/// stderr is scrubbed before any of it reaches an envelope; the two
/// rate-limit rows emit fixed strings and admit no stderr text at all.
/// Each row carries its typed [`FailureKind`] so retry and defer decisions
/// never re-derive the class from the message.
fn classify(status: Option<ExitStatus>, stderr: &[u8]) -> GhError {
    let text = String::from_utf8_lossy(stderr);
    // `lower` is classification-only; the scrub runs on the original case
    // (token prefixes are already lowercase, and admitting case-folded
    // text would distort the diagnostic).
    let lower = text.to_ascii_lowercase();
    if lower.contains("secondary rate limit") {
        return GhError {
            kind: FailureKind::SecondaryLimit,
            error: Error::transient("gh: secondary rate limit hit; back off before retrying"),
        };
    }
    if lower.contains("api rate limit exceeded") {
        return GhError {
            kind: FailureKind::RateExhausted,
            error: Error::transient("gh: API rate limit exceeded; defer until the limit resets"),
        };
    }
    if lower.contains("bad credentials") {
        return GhError {
            kind: FailureKind::Config,
            error: Error::config("gh token was rejected (bad credentials) — run: gh auth login"),
        };
    }
    if status.and_then(|s| s.code()) == Some(4) {
        return GhError {
            kind: FailureKind::Config,
            error: Error::config("gh is not authenticated — run: gh auth login"),
        };
    }
    let scrubbed = scrub_tokens(&text);
    let detail = match cap(scrubbed.trim_end()) {
        "" => "<no stderr>",
        d => d,
    };
    let suffix = match status {
        Some(s) => format!(" ({s})"),
        None => String::new(),
    };
    GhError {
        kind: FailureKind::Other,
        error: Error::transient(format!("gh failed{suffix}: {detail}")),
    }
}

/// First STDERR_CAP bytes, backed off to a char boundary. The backoff is a
/// bounded search over 4 offsets, not a decrement loop: a UTF-8 boundary
/// occurs at least every 4 bytes, so non-termination is unrepresentable
/// rather than merely avoided (the same discipline as scrub_tokens'
/// progress assert).
fn cap(s: &str) -> &str {
    if s.len() <= STDERR_CAP {
        return s;
    }
    // Known-equivalent mutant: lowering the range's floor (e.g. `- 3` →
    // `/ 3`) survives, and stays. The floor is a proof bound — the
    // descending search always terminates within 4 offsets of the top, so
    // any lower floor is behavior-identical; only raising it above
    // STDERR_CAP - 3 could change results, and that direction is caught.
    let end = (STDERR_CAP - 3..=STDERR_CAP)
        .rev()
        .find(|&e| s.is_char_boundary(e))
        .expect("any 4 consecutive byte offsets contain a UTF-8 char boundary");
    &s[..end]
}

/// Redact token shapes: `gh[pousr]_` or `github_pat_` followed by 8 or more
/// `[A-Za-z0-9_]`, replaced (maximal run, prefix included) with
/// `[REDACTED]`. Deliberately no word-boundary requirement on the left: a
/// token abutting a word character would leak under one, and the cost of
/// the aggressive rule is over-redacting diagnostic text ("laughs_padpadpad"
/// loses its tail), which is the cheap side. 8 is far below any real token
/// length (36+); a shorter fragment is not a usable credential. Idempotent:
/// the replacement contains no token shape. Public for the fuzz harness
/// (fuzz/fuzz_targets/scrub_tokens.rs), which witnesses no-shape-survives,
/// clean-text-identity, and idempotence; not part of the transport surface.
pub fn scrub_tokens(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match token_at(&b[i..]) {
            Some(len) => {
                out.extend_from_slice(b"[REDACTED]");
                i += len.get();
            }
            None => {
                out.push(b[i]);
                i += 1;
            }
        }
        // Progress invariant: every consumed input byte contributes at most
        // 10 output bytes ("[REDACTED]".len()), so out.len() <= 10*i at
        // every loop head. NonZeroUsize makes a zero-length match
        // unrepresentable; this witnesses the rest — a loop that stops
        // advancing i is an unbounded allocator (observed as an OOM under
        // mutation testing), and this converts that into an instant panic
        // in debug/test builds at zero release cost.
        debug_assert!(out.len() <= 10 * i, "scrub loop stopped advancing");
    }
    // Only ASCII runs are replaced, with ASCII; every other byte is copied
    // verbatim in order, so a valid-UTF-8 input yields a valid-UTF-8 output.
    String::from_utf8(out).expect("scrub replaces ASCII with ASCII")
}

fn is_word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Length of the token shape starting at `rest[0]`, if one does. NonZero by
/// type, not by luck: the scrub loop advances by this value, so a zero here
/// is an infinite loop that appends "[REDACTED]" at memory-bandwidth speed —
/// a mutation-testing run demonstrated it as an OOM, not a hang. A match is
/// always at least the prefix (4+), and the signature makes the
/// non-advancing case unrepresentable rather than merely absent.
fn token_at(rest: &[u8]) -> Option<NonZeroUsize> {
    const MIN_RUN: usize = 8;
    let prefix = if rest.starts_with(b"github_pat_") {
        b"github_pat_".len()
    } else if rest.len() > 3
        && rest[0] == b'g'
        && rest[1] == b'h'
        && matches!(rest[2], b'p' | b'o' | b'u' | b's' | b'r')
        && rest[3] == b'_'
    {
        4
    } else {
        return None;
    };
    let run = rest[prefix..].iter().take_while(|&&c| is_word(c)).count();
    (run >= MIN_RUN)
        .then_some(prefix + run)
        .and_then(NonZeroUsize::new)
}

/// The minimum-version gate, run once at sync start (sync::run wires the
/// call). Below MIN_GH_VERSION the stderr heuristic and exit-code taxonomy
/// are unverified claims, so the run refuses with the remedy instead of
/// degrading silently.
///
/// Known-equivalent mutant: replacing this body with `Ok(())` survives
/// mutation testing, and stays. The wrapper's only content is the real
/// binary name and deadline, so a hermetic test cannot distinguish it from
/// `Ok(())` without a real gh on PATH; the mechanism lives in
/// [`version_gate_with`], which the tests cover including both boundary
/// sides. The same applies to [`graphql`]'s wrapper.
pub fn version_gate() -> Result<()> {
    version_gate_with(Path::new("gh"), VERSION_DEADLINE)
}

fn version_gate_with(bin: &Path, deadline: Duration) -> Result<()> {
    let args = ["--version".to_string()];
    let out = run_gh(bin, &args, None, deadline)?;
    if out.killed {
        return Err(Error::transient(format!(
            "gh --version produced no exit within {}s and was killed by the watchdog",
            deadline.as_secs()
        )));
    }
    if !out.status.is_some_and(|s| s.success()) {
        return Err(classify(out.status, &out.stderr).error);
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let v = parse_gh_version(&text).ok_or_else(|| {
        Error::config(format!(
            "cannot parse `gh --version` output: {}",
            cap(scrub_tokens(text.trim()).as_str())
        ))
    })?;
    if v < MIN_GH_VERSION {
        let (a, b, c) = v;
        let (x, y, z) = MIN_GH_VERSION;
        return Err(Error::config(format!(
            "gh {a}.{b}.{c} is older than the minimum {x}.{y}.{z} — upgrade gh (https://cli.github.com)"
        )));
    }
    Ok(())
}

/// First line is "gh version X.Y.Z (date)"; distro builds append suffixes
/// ("2.4.0+dfsg1"), so each component parses its leading digits and requires
/// at least one. Public for the fuzz harness (fuzz_targets/gh_version.rs),
/// which witnesses totality over arbitrary input; not part of the transport
/// surface.
pub fn parse_gh_version(text: &str) -> Option<(u32, u32, u32)> {
    let mut words = text.lines().next()?.split_whitespace();
    if words.next()? != "gh" || words.next()? != "version" {
        return None;
    }
    let mut parts = words.next()?.split('.');
    let mut component = || -> Option<u32> {
        let part = parts.next()?;
        let digits = &part[..part.chars().take_while(char::is_ascii_digit).count()];
        digits.parse().ok()
    };
    Some((component()?, component()?, component()?))
}

struct RunOutput {
    /// None only when even the post-kill reap failed to report one.
    status: Option<ExitStatus>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    killed: bool,
}

/// Spawn `bin args…`, feed `stdin_doc` if any, drain both pipes
/// concurrently, and reap the child — killing it at `deadline`. Every
/// mechanism invariant in the module docs lives here.
fn run_gh(
    bin: &Path,
    args: &[String],
    stdin_doc: Option<&str>,
    deadline: Duration,
) -> Result<RunOutput> {
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .env("GH_PAGER", "")
        .env("GH_PROMPT_DISABLED", "1")
        .stdin(if stdin_doc.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| spawn_error(bin, &e))?;

    // Fire-and-forget: nothing consumes the writer's result (EPIPE from a
    // child that exited before reading is ignored — the exit status and
    // body decide the outcome, not this write), and joining it could block
    // behind a stdin pipe a grandchild still holds after a kill.
    // SIGPIPE precondition: "EPIPE is an Err, not a signal" holds because
    // Rust's runtime sets SIGPIPE to SIG_IGN before main — in a C host
    // that write would kill the thread instead.
    if let Some(doc) = stdin_doc {
        let mut pipe = child.stdin.take().expect("stdin was piped above");
        let doc = doc.to_string();
        thread::spawn(move || {
            let _ = pipe.write_all(doc.as_bytes());
        });
    }
    let stdout_rx = drain(child.stdout.take().expect("stdout was piped above"));
    let stderr_rx = drain(child.stderr.take().expect("stderr was piped above"));

    let start = Instant::now();
    let (status, killed) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (Some(status), false),
            Ok(None) if start.elapsed() >= deadline => {
                let _ = child.kill();
                // Rare, high-consequence: the operator watching a stalled
                // sync deserves more than the final envelope. stderr is
                // non-contract noise space; the counted telemetry field
                // (watchdog_kills) lands with the milestone-2 summary.
                eprintln!(
                    "ghgraph: gh produced no exit within {}s; killed by the watchdog",
                    deadline.as_secs()
                );
                break (child.wait().ok(), true);
            }
            Ok(None) => thread::sleep(POLL_INTERVAL),
            Err(_) => {
                // An OS-level wait failure is neither a user typo nor a
                // ghgraph bug; kill best-effort and let classification
                // treat the statusless outcome as TRANSIENT.
                let _ = child.kill();
                break (child.wait().ok(), false);
            }
        }
    };
    // Normal exit closes the child's pipe ends, so the drains hit EOF and
    // deliver promptly. On ANY exit path a grandchild may still hold the
    // pipes open (see DRAIN_GRACE) — collect what arrived, bounded, and
    // never block unconditionally: an early `killed=false` bug here once
    // routed the wait-error kill path into an unbounded recv (caught in
    // review); the bound being unconditional makes that class of routing
    // mistake unrepresentable. ONE deadline spans both pipes, so
    // DRAIN_GRACE is the total post-reap bound, not per-pipe (the closure
    // lens caught the doubled per-pipe reading).
    let deadline = Instant::now() + DRAIN_GRACE;
    let collect = |rx: mpsc::Receiver<Vec<u8>>| {
        let mut buf = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(left) {
                Ok(chunk) => buf.extend_from_slice(&chunk),
                // EOF or read error: complete, the normal case.
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                // A held-open pipe: bounded prefix, and a drain thread
                // just leaked (until the pipe closes) — worth a line.
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    eprintln!(
                        "ghgraph: a gh pipe was still open {}s after exit; \
                         keeping what arrived",
                        DRAIN_GRACE.as_secs()
                    );
                    break;
                }
            }
        }
        buf
    };
    let stdout = collect(stdout_rx);
    let stderr = collect(stderr_rx);
    Ok(RunOutput {
        status,
        stdout,
        stderr,
        killed,
    })
}

/// Drain a pipe on its own thread, streaming chunks through a channel so
/// the caller can bound its wait (a JoinHandle cannot be joined with a
/// timeout) AND keep everything that arrived before a held-open pipe
/// stopped progress — an all-at-EOF send would forfeit a complete body
/// whenever a grandchild delays EOF past the grace. A send fails only if
/// the collector already gave up; the thread then exits on its next read.
fn drain(mut pipe: impl Read + Send + 'static) -> mpsc::Receiver<Vec<u8>> {
    // Bounded (1024 chunks × 64KB = 64MB per pipe): the channel is the only
    // unbounded allocation a misbehaving child could drive (the collector
    // reads nothing until the child is reaped), and 64MB is an order of
    // magnitude above any legitimate response (document shapes bound
    // hydration to single-digit MB). A legit child never feels the cap; a
    // firehose child blocks on a full channel, stops draining its pipe,
    // and stalls into the watchdog. A blocked send exits cleanly when the
    // collector drops the receiver.
    let (tx, rx) = mpsc::sync_channel(1024);
    thread::spawn(move || {
        // Chunk size is a throughput tuning constant: ANY positive size is
        // correct (smaller sizes just mean more sends), so mutants on this
        // expression are equivalent by construction.
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match pipe.read(&mut chunk) {
                // EOF, or a read error (the prefix already sent stands):
                // dropping the sender tells the collector "complete".
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.send(chunk[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });
    rx
}

fn spawn_error(bin: &Path, e: &std::io::Error) -> Error {
    if e.kind() == std::io::ErrorKind::NotFound {
        Error::config(format!(
            "gh not found ({}) — ghgraph's only transport; install it from https://cli.github.com",
            bin.display()
        ))
    } else {
        Error::config(format!("cannot run gh ({}): {e}", bin.display()))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::error::Code;

    /// A fake gh: a scratch directory holding an executable `gh` shell
    /// script (plus any side files), passed to the `_with` entry points so
    /// no test mutates process env or PATH.
    struct FakeGh {
        dir: PathBuf,
    }

    impl FakeGh {
        fn new(script_body: &str) -> FakeGh {
            static N: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "ghgraph-gh-test-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&dir).unwrap();
            let bin = dir.join("gh");
            fs::write(&bin, format!("#!/bin/sh\n{script_body}\n")).unwrap();
            fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
            FakeGh { dir }
        }

        /// A fake that drains stdin, then prints `body` (written to a side
        /// file — no shell quoting of JSON) and exits with `code`.
        fn with_body(body: &str, code: i32) -> FakeGh {
            let fake = FakeGh::new("");
            fs::write(fake.dir.join("body.json"), body).unwrap();
            let script = format!(
                "cat > /dev/null\ncat '{}'\nexit {code}",
                fake.dir.join("body.json").display()
            );
            fs::write(fake.bin(), format!("#!/bin/sh\n{script}\n")).unwrap();
            fake
        }

        fn bin(&self) -> PathBuf {
            self.dir.join("gh")
        }

        fn side(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }
    }

    impl Drop for FakeGh {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    fn deadline() -> Duration {
        Duration::from_secs(30)
    }

    // --- the success path ---

    // The wiring contract in one shot: the document arrives on stdin (so
    // argv limits can never apply), vars become -f string variables after
    // `api graphql -F query=@-`, and the env hygiene vars are set on the
    // child. The fake records everything to side files and returns a
    // canned body.
    #[test]
    fn success_wires_argv_stdin_env_and_extracts_rate_limit() {
        let fake = FakeGh::new(""); // placeholder; rewritten below with paths
        let script = format!(
            "printf '%s\\n' \"$@\" > '{argv}'\ncat > '{stdin}'\nprintf '%s\\n' \"$GH_PAGER\" \"$GH_PROMPT_DISABLED\" > '{env}'\ncat '{body}'",
            argv = fake.side("argv").display(),
            stdin = fake.side("stdin").display(),
            env = fake.side("env").display(),
            body = fake.side("body.json").display(),
        );
        fs::write(fake.bin(), format!("#!/bin/sh\n{script}\n")).unwrap();
        let fixture = include_str!("../tests/fixtures/discovery_page.json");
        fs::write(fake.side("body.json"), fixture).unwrap();

        let resp = graphql_with(
            &fake.bin(),
            deadline(),
            "query($q:String!){...}",
            &[("q", "repo:o/n is:pr")],
        )
        .expect("fixture body must succeed");

        let argv = fs::read_to_string(fake.side("argv")).unwrap();
        assert_eq!(
            argv.lines().collect::<Vec<_>>(),
            vec!["api", "graphql", "-F", "query=@-", "-f", "q=repo:o/n is:pr"],
        );
        let stdin = fs::read_to_string(fake.side("stdin")).unwrap();
        assert_eq!(stdin, "query($q:String!){...}");
        let env = fs::read_to_string(fake.side("env")).unwrap();
        assert_eq!(env, "\n1\n", "GH_PAGER cleared, GH_PROMPT_DISABLED=1");

        assert!(resp.data.get("search").is_some(), "data is the data object");
        let rl = resp.rate_limit.expect("fixture selects rateLimit");
        assert_eq!((rl.cost, rl.remaining), (1, 4823));
        assert_eq!(rl.reset_at.as_str(), "2026-07-30T22:01:39Z");
    }

    // The ghost-author fixture through the gh path and into the typed parse:
    // ordinary-but-odd live data (a deleted account's `ghost` author) flows
    // as data, not as an error any From impl could launder into INTERNAL —
    // the call-site classification witness on the gh path (ROADMAP m2).
    #[test]
    fn ghost_fixture_flows_through_gh_to_typed_parse() {
        let fake = FakeGh::with_body(include_str!("../tests/fixtures/hydrate_pr_ghost.json"), 0);
        let resp = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap();
        let node = crate::parse::hydrate_pr(&resp.data)
            .expect("ghost fixture parses")
            .expect("node is present");
        assert_eq!(
            node.author.expect("ghost, not null").login.as_str(),
            "ghost"
        );
    }

    // Timing telemetry carries no pinnable value (enumerated
    // nondeterminism), but its ACCUMULATION direction is a contract: a
    // child that provably slept 50ms must leave at least 50ms behind —
    // Instant::elapsed cannot undershoot the sleep — so a broken
    // accumulator (stuck at zero, or subtracting) fails here without any
    // test asserting noise.
    #[test]
    fn subprocess_ms_accumulates_a_wall_clock_lower_bound() {
        let fake = FakeGh::new("sleep 0.05\ncat > /dev/null\nprintf '%s' '{\"data\":{}}'");
        let mut ctx = GhCtx::single();
        graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap();
        assert!(
            ctx.tel.subprocess_ms >= 50,
            "a 50ms child sleep must accumulate >= 50ms, got {}",
            ctx.tel.subprocess_ms
        );
    }

    // Partial data beside a top-level errors array is a SUCCESS carrying
    // masked nulls (here node:null): gh exits 1 on any errors array, but the
    // body decides. parse.rs types the masked spots and milestone-2 sync
    // resolves each to a defined outcome — failing here instead would turn
    // every permanently-masked PR (e.g. a private team reviewer) into an
    // eternal quarantine loop.
    #[test]
    fn partial_data_with_errors_array_is_success() {
        let fake = FakeGh::with_body(
            r#"{"data":{"node":null},"errors":[{"type":"NOT_FOUND","message":"boom"}]}"#,
            1,
        );
        let resp = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap();
        assert!(resp.data.get("node").unwrap().is_null());
        assert!(resp.rate_limit.is_none());
    }

    // The other direction of body-decides: a null `data` is a failure even
    // when gh exits 0.
    #[test]
    fn null_data_is_failure_despite_exit_zero() {
        let fake = FakeGh::with_body(r#"{"data":null,"errors":[{"message":"x"}]}"#, 0);
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
    }

    #[test]
    fn non_json_stdout_is_transient() {
        let fake = FakeGh::with_body("gh: flagrant nonsense", 0);
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
    }

    // A multi-MB payload on BOTH pipes at once: only concurrent drains
    // survive this (a 64KB pipe buffer wedges any sequential read), and the
    // padded body still parses. Pins the no-pipe-deadlock mechanism.
    #[test]
    fn multi_mb_on_both_pipes_does_not_deadlock() {
        let fake = FakeGh::new(concat!(
            "cat > /dev/null\n",
            "dd if=/dev/zero bs=1024 count=2048 2>/dev/null | tr '\\0' 'e' >&2\n",
            "printf '{\"data\":{\"pad\":\"'\n",
            "dd if=/dev/zero bs=1024 count=2048 2>/dev/null | tr '\\0' 'e'\n",
            "printf '\"}}'",
        ));
        let resp = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap();
        assert_eq!(
            resp.data.get("pad").unwrap().as_str().unwrap().len(),
            2048 * 1024
        );
    }

    // --- classification, one test per table row ---

    #[test]
    fn secondary_rate_limit_is_transient() {
        let fake = FakeGh::new(
            "echo 'You have exceeded a secondary rate limit. Please wait.' >&2\nexit 1",
        );
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
        assert!(err.message.contains("secondary rate limit"), "{err}");
    }

    #[test]
    fn primary_rate_limit_is_transient() {
        let fake = FakeGh::new("echo 'API rate limit exceeded for user ID 1.' >&2\nexit 1");
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
        assert!(err.message.contains("rate limit exceeded"), "{err}");
    }

    // A REJECTED token is not exit 4: gh makes the call, the API answers
    // 401, gh exits 1 relaying "Bad credentials" on stderr (probed live).
    // Without this row the failure would read TRANSIENT and retry forever
    // against a token only the operator can replace. The fake reproduces
    // the probed shape: REST error body on stdout, relay line on stderr.
    #[test]
    fn bad_credentials_is_configuration() {
        let fake = FakeGh::new(concat!(
            "cat > /dev/null\n",
            "printf '{\"message\":\"Bad credentials\",\"status\":\"401\"}'\n",
            "echo 'gh: Bad credentials (HTTP 401)' >&2\n",
            "exit 1",
        ));
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Configuration);
        assert!(err.message.contains("gh auth login"), "{err}");
        assert!(
            !err.message.contains("(HTTP 401)"),
            "fixed string, not stderr echo: {err}"
        );
    }

    #[test]
    fn exit_code_4_is_configuration_auth() {
        let fake = FakeGh::new("cat > /dev/null\nexit 4");
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Configuration);
        assert!(err.message.contains("gh auth login"), "{err}");
    }

    #[test]
    fn absent_binary_is_configuration() {
        let err = graphql_with(
            Path::new("/nonexistent/ghgraph-test/gh"),
            deadline(),
            "q",
            &[],
        )
        .unwrap_err();
        assert_eq!(err.code, Code::Configuration);
        assert!(err.message.contains("gh not found"), "{err}");
    }

    // The default row: TRANSIENT, carrying scrubbed stderr capped at ~1KB.
    // The token leads the output so the cap cannot be what hid it.
    #[test]
    fn default_row_scrubs_tokens_and_caps_stderr() {
        let fake = FakeGh::new(concat!(
            "printf 'fatal: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 rejected ' >&2\n",
            "dd if=/dev/zero bs=1024 count=4 2>/dev/null | tr '\\0' 'x' >&2\n",
            "exit 1",
        ));
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
        assert!(err.message.contains("[REDACTED]"), "{err}");
        assert!(!err.message.contains("ghp_A"), "token must not leak: {err}");
        assert!(
            err.message.len() < STDERR_CAP + 100,
            "cap holds: {} bytes",
            err.message.len()
        );
    }

    #[test]
    fn empty_stderr_default_row_names_the_absence() {
        let fake = FakeGh::new("cat > /dev/null\nexit 1");
        let err = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
        assert!(err.message.contains("<no stderr>"), "{err}");
    }

    // The grace window's semantics, pinned by its discriminating input: a
    // chunk written entirely AFTER the child exited, within DRAIN_GRACE,
    // is kept (the deadline is real waiting, not just take-what's-queued —
    // the mutant that collapses it to zero fails here).
    #[test]
    fn straggler_chunk_within_grace_is_kept() {
        let fake = FakeGh::new(concat!(
            "( sleep 0.5; printf '{\"data\":{\"late\":true}}' ) &\n",
            "cat > /dev/null\n",
            "exit 0",
        ));
        let resp = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap();
        assert_eq!(
            resp.data.get("late"),
            Some(&serde_json::Value::Bool(true)),
            "a body written 0.5s after exit, inside the 2s grace, must arrive"
        );
    }

    // A malformed rateLimit envelope degrades to None (missing-tolerant),
    // never to a failed call — including a timestamp Rfc3339Utc rejects.
    #[test]
    fn malformed_rate_limit_envelope_is_none() {
        let fake = FakeGh::with_body(
            r#"{"data":{"ok":true,"rateLimit":{"cost":1,"remaining":2,"resetAt":"not a time"}}}"#,
            0,
        );
        let resp = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap();
        assert!(resp.rate_limit.is_none());
        assert_eq!(resp.data.get("ok"), Some(&serde_json::Value::Bool(true)));
    }

    // --- the watchdog ---

    // A grandchild that inherits the pipes and outlives a NORMALLY-exited
    // gh must not wedge the caller, and the fully-written body must
    // survive: EOF never arrives, so an unbounded post-exit read blocks
    // 30s here, while the chunked drain returns the complete body within
    // DRAIN_GRACE of the exit. The review panel caught the kill-path
    // variant of this; the bound is unconditional now.
    #[test]
    fn lingering_grandchild_after_normal_exit_is_bounded_and_keeps_body() {
        let fake = FakeGh::new(concat!(
            "sleep 30 &\n",
            "cat > /dev/null\n",
            "printf '{\"data\":{\"ok\":true}}'\n",
            "exit 0",
        ));
        let start = Instant::now();
        let resp = graphql_with(&fake.bin(), deadline(), "q", &[]).unwrap();
        assert_eq!(resp.data.get("ok"), Some(&serde_json::Value::Bool(true)));
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "bounded by DRAIN_GRACE, not the grandchild's 30s: {:?}",
            start.elapsed()
        );
    }

    // A stalled gh (never reads stdin, never writes, never exits) is killed
    // within the deadline and reaped; the caller gets TRANSIENT promptly
    // rather than hanging an unattended sync.
    #[test]
    fn watchdog_kills_stalled_gh() {
        let fake = FakeGh::new("sleep 30");
        let start = Instant::now();
        let err = graphql_with(&fake.bin(), Duration::from_millis(300), "q", &[]).unwrap_err();
        assert_eq!(err.code, Code::Transient);
        assert!(err.message.contains("watchdog"), "{err}");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "killed promptly, not after the child's 30s: {:?}",
            start.elapsed()
        );
    }

    // --- retry policy and telemetry ---

    /// A fake gh that fails with `stderr_line`/exit 1 until the counter file
    /// records `fails` prior runs, then succeeds with `body`.
    fn flaky(fails: u32, stderr_line: &str, body: &str) -> FakeGh {
        let fake = FakeGh::new("");
        fs::write(fake.side("body.json"), body).unwrap();
        let script = format!(
            "cat > /dev/null\n\
             n=$(cat '{count}' 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > '{count}'\n\
             if [ \"$n\" -lt {fails} ]; then echo '{stderr_line}' >&2; exit 1; fi\n\
             cat '{body}'",
            count = fake.side("count").display(),
            body = fake.side("body.json").display(),
        );
        fs::write(fake.bin(), format!("#!/bin/sh\n{script}\n")).unwrap();
        fake
    }

    #[test]
    fn transient_failure_retries_to_success_and_counts() {
        let fake = flaky(1, "flagrant blip", r#"{"data":{"ok":true}}"#);
        let mut ctx = GhCtx::new(3, 10);
        let resp = graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap();
        assert_eq!(resp.data.get("ok"), Some(&serde_json::Value::Bool(true)));
        assert_eq!(ctx.tel.subprocess_count, 2, "one failure, one success");
        assert_eq!(ctx.tel.sleeps, 1);
        assert_eq!(ctx.retry_budget, 9, "one retry consumed");
        // No rateLimit selected: the blind-call counter must say so.
        assert_eq!(ctx.tel.rate_limit_unknown, 1);
    }

    #[test]
    fn attempts_per_call_bounds_the_retries() {
        let fake = flaky(99, "always down", r#"{"data":{}}"#);
        let mut ctx = GhCtx::new(2, 10);
        let err = graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap_err();
        assert_eq!(err.kind, FailureKind::Other);
        assert_eq!(err.error.code, Code::Transient);
        assert_eq!(ctx.tel.subprocess_count, 2, "attempts_per_call=2");
        assert_eq!(ctx.retry_budget, 9);
    }

    #[test]
    fn exhausted_budget_means_single_attempts() {
        let fake = flaky(99, "always down", r#"{"data":{}}"#);
        let mut ctx = GhCtx::new(3, 0);
        let _ = graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap_err();
        assert_eq!(ctx.tel.subprocess_count, 1, "no budget, no retry");
        assert_eq!(ctx.tel.sleeps, 0);
    }

    #[test]
    fn rate_exhausted_and_config_never_retry() {
        // Primary rate limit: typed RateExhausted, exactly one attempt even
        // with attempts and budget to spare — the scheduler defers, gh must
        // not burn the budget the defer exists to protect.
        let fake = flaky(99, "API rate limit exceeded for user ID 1.", "{}");
        let mut ctx = GhCtx::new(3, 10);
        let err = graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap_err();
        assert_eq!(err.kind, FailureKind::RateExhausted);
        assert_eq!(ctx.tel.subprocess_count, 1);

        let fake = flaky(99, "gh: Bad credentials (HTTP 401)", "{}");
        let mut ctx = GhCtx::new(3, 10);
        let err = graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap_err();
        assert_eq!(err.kind, FailureKind::Config);
        assert_eq!(err.error.code, Code::Configuration);
        assert_eq!(ctx.tel.subprocess_count, 1);
    }

    #[test]
    fn successful_calls_accumulate_rate_telemetry() {
        let fixture = include_str!("../tests/fixtures/discovery_page.json");
        let fake = FakeGh::with_body(fixture, 0);
        let mut ctx = GhCtx::new(1, 0);
        graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap();
        graphql_ctx(&fake.bin(), deadline(), "q", &[], &mut ctx).unwrap();
        assert_eq!(ctx.tel.rate_cost, 2, "fixture costs 1, twice");
        assert_eq!(ctx.tel.remaining, Some(4823));
        assert_eq!(
            ctx.tel.reset_at.as_ref().map(|t| t.as_str()),
            Some("2026-07-30T22:01:39Z")
        );
        assert_eq!(ctx.tel.rate_limit_unknown, 0);
        assert!(ctx.tel.bytes_parsed > 0);
    }

    #[test]
    fn backoff_schedule_shape() {
        use super::backoff;
        // Secondary: long and linear. Others: short doubling, capped at 8s.
        assert_eq!(backoff(FailureKind::SecondaryLimit, 1).as_secs(), 30);
        assert_eq!(backoff(FailureKind::SecondaryLimit, 2).as_secs(), 60);
        assert_eq!(backoff(FailureKind::Other, 1).as_secs(), 1);
        assert_eq!(backoff(FailureKind::Other, 2).as_secs(), 2);
        assert_eq!(backoff(FailureKind::Watchdog, 4).as_secs(), 8);
        assert_eq!(backoff(FailureKind::Other, 40).as_secs(), 8, "cap holds");
    }

    // --- the viewer identity call ---

    #[test]
    fn viewer_login_parses_the_user_object() {
        let fake = FakeGh::with_body(r#"{"login":"OctoCat","id":1,"type":"User"}"#, 0);
        let login = viewer_login_with(&fake.bin(), deadline()).unwrap();
        assert_eq!(login, "OctoCat", "as received — folding is the caller's");
    }

    #[test]
    fn viewer_login_failures_classify() {
        let fake = FakeGh::new("cat > /dev/null 2>/dev/null\nexit 4");
        let err = viewer_login_with(&fake.bin(), deadline()).unwrap_err();
        assert_eq!(err.code, Code::Configuration);

        // Exit 0 with a non-JSON body (or JSON without login) is not a
        // success — it classifies from stderr's default row.
        let fake = FakeGh::with_body("not json", 0);
        let err = viewer_login_with(&fake.bin(), deadline()).unwrap_err();
        assert_eq!(err.code, Code::Transient);
    }

    // --- the version gate ---

    fn version_fake(line: &str) -> FakeGh {
        FakeGh::new(&format!("printf '%s\\n' '{line}'"))
    }

    #[test]
    fn version_gate_accepts_current_and_rejects_old() {
        let ok = version_fake("gh version 2.96.0 (2026-07-02)");
        version_gate_with(&ok.bin(), deadline()).expect("2.96.0 passes");

        let old = version_fake("gh version 2.4.0 (2022-01-26)");
        let err = version_gate_with(&old.bin(), deadline()).unwrap_err();
        assert_eq!(err.code, Code::Configuration);
        assert!(err.message.contains("2.4.0"), "{err}");
        assert!(err.message.contains("minimum 2.40.0"), "{err}");
    }

    #[test]
    fn version_gate_handles_distro_suffix_and_garbage() {
        let deb = version_fake("gh version 2.96.0+dfsg1 (2026-07-02)");
        version_gate_with(&deb.bin(), deadline()).expect("+dfsg1 suffix parses");

        let garbage = version_fake("definitely not gh");
        let err = version_gate_with(&garbage.bin(), deadline()).unwrap_err();
        assert_eq!(err.code, Code::Configuration);
        assert!(err.message.contains("cannot parse"), "{err}");
    }

    // The no-echo pin for the gate's one output-admitting error path
    // (mirrors the pins in time.rs/identity.rs/parse.rs): a token shape in
    // `gh --version` output must reach the envelope only as [REDACTED].
    #[test]
    fn version_gate_parse_error_scrubs_tokens() {
        let bad = version_fake("broken ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 wrapper");
        let err = version_gate_with(&bad.bin(), deadline()).unwrap_err();
        assert_eq!(err.code, Code::Configuration);
        assert!(err.message.contains("[REDACTED]"), "{err}");
        assert!(!err.message.contains("ghp_A"), "token must not leak: {err}");
    }

    #[test]
    fn version_gate_boundary_is_inclusive() {
        let at = version_fake("gh version 2.40.0 (2023-11-01)");
        version_gate_with(&at.bin(), deadline()).expect("the floor itself passes");
        let below = version_fake("gh version 2.39.9 (2023-10-01)");
        assert!(version_gate_with(&below.bin(), deadline()).is_err());
    }

    #[test]
    fn parse_gh_version_shapes() {
        assert_eq!(
            parse_gh_version("gh version 2.96.0 (2026-07-02)\nhttps://x\n"),
            Some((2, 96, 0))
        );
        assert_eq!(
            parse_gh_version("gh version 2.4.0+dfsg1 (2022-01-26)"),
            Some((2, 4, 0))
        );
        assert_eq!(
            parse_gh_version("gh version 2.96 (x)"),
            None,
            "two components"
        );
        assert_eq!(parse_gh_version("zsh version 5.9"), None);
        assert_eq!(parse_gh_version(""), None);
        assert_eq!(parse_gh_version("gh version x.y.z"), None);
    }

    // --- the scrubber ---

    #[test]
    fn scrub_redacts_every_prefix_family() {
        for prefix in ["ghp", "gho", "ghu", "ghs", "ghr"] {
            let input = format!("token {prefix}_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 end");
            assert_eq!(
                scrub_tokens(&input),
                "token [REDACTED] end",
                "family {prefix}"
            );
        }
        assert_eq!(
            scrub_tokens("x github_pat_11AAAAAAA0abcdefghijklmnop y"),
            "x [REDACTED] y"
        );
    }

    #[test]
    fn scrub_short_runs_and_clean_text_pass_through() {
        for clean in [
            "ghp_1234567",             // 7 < MIN_RUN: not a usable credential
            "gh version 2.96.0",       // no prefix match
            "naïve — unicode stays ✓", // multibyte untouched
            "",
        ] {
            assert_eq!(scrub_tokens(clean), clean);
        }
    }

    // The no-left-boundary rule, pinned as a decision: a token glued to a
    // word char is still redacted (leak side), at the cost of eating the
    // tail of an innocent word (cheap side).
    #[test]
    fn scrub_has_no_left_boundary() {
        assert_eq!(
            scrub_tokens("Bearerghp_ABCDEFGHIJKLMNOP"),
            "Bearer[REDACTED]"
        );
        assert_eq!(scrub_tokens("laughs_padpadpad"), "lau[REDACTED]");
    }

    #[test]
    fn scrub_is_idempotent_and_handles_edges() {
        let once = scrub_tokens("ghs_AAAAAAAAAAAA and ghr_BBBBBBBBBBBB");
        assert_eq!(once, "[REDACTED] and [REDACTED]");
        assert_eq!(scrub_tokens(&once), once);
        // token at the very start and very end of the input
        assert_eq!(scrub_tokens("ghp_ABCDEFGH"), "[REDACTED]");
        assert_eq!(scrub_tokens("x ghp_ABCDEFGH"), "x [REDACTED]");
        // A bare prefix as the FINAL bytes of the input: the discriminating
        // case for token_at's length guard (`len > 3`), whose off-by-one
        // reads rest[3] past the end and panics instead of passing through.
        assert_eq!(scrub_tokens("ghp"), "ghp");
        assert_eq!(scrub_tokens("trailing ghs"), "trailing ghs");
        assert_eq!(scrub_tokens("gh"), "gh");
    }

    #[test]
    fn cap_backs_off_to_char_boundary() {
        // 1023 ASCII bytes then a 3-byte char straddling the 1024 limit.
        let s = format!("{}€tail", "x".repeat(1023));
        let capped = cap(&s);
        assert!(capped.len() <= STDERR_CAP);
        assert_eq!(capped, &"x".repeat(1023)[..]);
        assert_eq!(cap("short"), "short");
    }
}
