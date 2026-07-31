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
//!     inside a no-signal-handler process can unstick it. After a kill the
//!     wait for the drains is bounded (DRAIN_GRACE): killing the direct
//!     child cannot close a pipe end a grandchild inherited. Kill-anytime
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
//!   * Retry policy is owned here, bounded, and configured: attempts per
//!     call, per-repo budget. PLANNED (milestone 2, landing with the
//!     scheduler that consumes it — a config field with no consumer is the
//!     telemetry rule's counterexample). Primary rate limits fold into the
//!     floor's defer-record-exit path — one budget, one mechanism.
//!   * gh stderr is redacted for token shapes before it reaches any
//!     envelope: `gh[pousr]_` and `github_pat_` prefixes followed by 8+
//!     `[A-Za-z0-9_]` (see [`scrub_tokens`]), then capped at ~1KB.
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
//! The rows are checked in table order (stderr strings before the exit
//! code, ASCII-case-insensitively); classification never inspects stdout,
//! whose failure modes the body-decides rule above already owns.
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

/// Kill a gh call that produces no exit within this deadline. A healthy
/// hydration is single-digit MB and completes in seconds even on a slow
/// link; 120s sits comfortably above any observed healthy call while
/// bounding a wedged one. A constant, not config — promoted only when
/// subprocess_seconds telemetry names the consumer.
const WATCHDOG_DEADLINE: Duration = Duration::from_secs(120);

/// `gh --version` prints instantly or something is wrong with the install.
const VERSION_DEADLINE: Duration = Duration::from_secs(10);

/// try_wait poll granularity: cheap enough to be negligible against a
/// network round trip, fine enough that the deadline error is small.
const POLL_INTERVAL: Duration = Duration::from_millis(15);

/// stderr detail admitted into a TRANSIENT envelope, after scrubbing.
const STDERR_CAP: usize = 1024;

/// After a watchdog kill, how long to wait for the pipe drains. Killing the
/// direct child does not close a pipe a grandchild inherited (gh spawns
/// credential helpers), so an unconditional join could re-wedge the exact
/// path the watchdog exists to unwedge. On expiry the output is treated as
/// empty and the drain thread is left blocked until the pipe finally closes
/// — a leak bounded by the number of watchdog kills, which quarantine
/// backoff bounds in turn.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The oldest gh this build claims its stderr classification table, exit-code
/// taxonomy (4 = auth), stdin document passing, and prompt-disable env for.
/// 2.40.0 (2023-11) comfortably postdates every mechanism used here; the
/// table strings were verified live against 2.96.0. Raising the floor is
/// cheap (CONFIGURATION with an upgrade remedy); lowering it requires
/// re-verifying the table against the older release.
pub const MIN_GH_VERSION: (u32, u32, u32) = (2, 40, 0);

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    pub cost: u32,
    pub remaining: u32,
    pub reset_at: String,
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

/// One GraphQL round trip. `vars` become string variables; typed variables
/// are not needed by any current query.
pub fn graphql(query: &str, vars: &[(&str, &str)]) -> Result<Response> {
    graphql_with(Path::new("gh"), WATCHDOG_DEADLINE, query, vars)
}

/// [`graphql`] with the binary and deadline injectable, so the tests can run
/// a fake gh from a scratch directory without mutating process env (set_var
/// is `unsafe` in edition 2024; unsafe is forbidden crate-wide).
fn graphql_with(
    bin: &Path,
    deadline: Duration,
    query: &str,
    vars: &[(&str, &str)],
) -> Result<Response> {
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
    let out = run_gh(bin, &args, Some(query), deadline)?;
    // Body first, unconditionally: a complete, data-bearing response is a
    // success even from a child the watchdog had to kill after it wrote one.
    if let Some(resp) = body_success(&out.stdout) {
        return Ok(resp);
    }
    if out.killed {
        return Err(Error::transient(format!(
            "gh produced no exit within {}s and was killed by the watchdog",
            deadline.as_secs()
        )));
    }
    Err(classify(out.status, &out.stderr))
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
fn classify(status: Option<ExitStatus>, stderr: &[u8]) -> Error {
    let text = String::from_utf8_lossy(stderr);
    let lower = text.to_ascii_lowercase();
    if lower.contains("secondary rate limit") {
        return Error::transient("gh: secondary rate limit hit; back off before retrying");
    }
    if lower.contains("api rate limit exceeded") {
        return Error::transient("gh: API rate limit exceeded; defer until the limit resets");
    }
    if status.and_then(|s| s.code()) == Some(4) {
        return Error::config("gh is not authenticated — run: gh auth login");
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
    Error::transient(format!("gh failed{suffix}: {detail}"))
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

/// The minimum-version gate, run once at sync start (PLANNED: milestone 2
/// wires the call when sync::run lands). Below MIN_GH_VERSION the stderr
/// heuristic and exit-code taxonomy are unverified claims, so the run
/// refuses with the remedy instead of degrading silently.
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
        return Err(classify(out.status, &out.stderr));
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
/// at least one.
fn parse_gh_version(text: &str) -> Option<(u32, u32, u32)> {
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
    // deliver promptly. After a kill, a grandchild may still hold the pipes
    // open (see DRAIN_GRACE) — wait bounded, then settle for empty.
    let collect = |rx: mpsc::Receiver<Vec<u8>>| {
        if killed {
            rx.recv_timeout(DRAIN_GRACE).unwrap_or_default()
        } else {
            rx.recv().expect("drain thread panicked")
        }
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

/// Drain a pipe to EOF on its own thread, delivering through a channel so
/// the caller can bound its wait (a JoinHandle cannot be joined with a
/// timeout). The send fails only if the caller already gave up — ignored.
fn drain(mut pipe: impl Read + Send + 'static) -> mpsc::Receiver<Vec<u8>> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        // A mid-read error keeps the prefix; the classification path caps
        // and scrubs whatever arrived.
        let _ = pipe.read_to_end(&mut buf);
        let _ = tx.send(buf);
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
        assert_eq!(rl.reset_at, "2026-07-30T22:01:39Z");
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

    // --- the watchdog ---

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
