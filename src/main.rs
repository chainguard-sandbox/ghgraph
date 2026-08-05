#![forbid(unsafe_code)]
// A thin shell over the `ghgraph` library crate (src/lib.rs), which owns the
// modules, types, invariants, and the Unix-only fence. The split exists so
// verification harnesses can reach the library; the binary just wires argv to
// it.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use ghgraph::config;
use ghgraph::error::{self, Result};
use ghgraph::{report, sync};

/// Sync your GitHub work — pull requests, review threads, issues — into
/// local SQLite; query it offline from the CLI, in JSON.
///
/// I/O contract: stdout is always exactly one JSON document. Progress goes to
/// stderr, prefixed "ghgraph: ". Exit 0 = ok; exit 2 = error, as a typed
/// envelope {"error":{"code","message"}} on stdout; exit 1 is reserved for
/// opt-in gate flags. Output is deterministic for identical archive state
/// (modulo timing fields in _meta).
#[derive(Parser)]
#[command(name = "ghgraph", version)]
struct Cli {
    /// Config file (default: $XDG_CONFIG_HOME/ghgraph/config.json).
    #[arg(long, global = true, env = "GHGRAPH_CONFIG")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch configured repos into the archive.
    Sync {
        /// Ignore watermarks; refetch the whole lookback window.
        #[arg(long)]
        full: bool,
        /// Hydrate one PR now ("owner/name#123" or URL) — the read-time
        /// freshness path. `_meta` says when; the reader says which.
        #[arg(long, conflicts_with = "full")]
        pr: Option<String>,
        /// Exit 1 when the run left the archive incomplete (truncation,
        /// quarantine, capped discovery, floor deferral, errors) — the CI
        /// gate. Gate flags change the exit code, never a byte of JSON.
        #[arg(long)]
        strict: bool,
    },
    /// What needs the operator's attention, derived from archive state.
    Attention {
        /// Exit 1 when anything is waiting — the cron gate. Gate flags
        /// change the exit code, never a byte of JSON.
        #[arg(long)]
        fail_if_any: bool,
        /// Cap the rows returned per bucket. Totals stay disclosed: limits
        /// govern presentation, never derivation.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// List PRs in the archive.
    Prs {
        #[arg(long)]
        repo: Option<String>,
        /// Include merged, closed, and upstream-deleted PRs.
        #[arg(long)]
        all: bool,
        /// Only PRs authored by this login (the tracked-person one-liner:
        /// a WHERE clause, not a monitor verb).
        #[arg(long)]
        author: Option<String>,
        /// Cap the rows returned. The matching total is always disclosed:
        /// limits govern presentation, never derivation.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// One PR with reviews, threads, comments, and linked issues.
    Pr {
        /// "owner/name#123", a GitHub PR URL, or a bare number.
        /// Bare numbers resolve via --repo, then the cwd git remote; the
        /// canonical repo/number/url are echoed in the output so consumers
        /// never re-parse. (MCP consumers must pass the qualified form —
        /// cwd inference is a convenience, never the only path.)
        reference: String,
        #[arg(long)]
        repo: Option<String>,
        /// Truncate each body field to at most this many bytes (never
        /// splitting a UTF-8 code point); elided bodies say so via
        /// body_elided. Opt-in: a property of the request, distinct from
        /// `truncated`, which is a property of the archive.
        #[arg(long)]
        max_body_bytes: Option<usize>,
    },
    /// Full-text search over PR titles/bodies and comments.
    Search {
        query: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Read-only SQL against the archive. "-" (or a piped stdin with no
    /// argument) reads the statement from stdin.
    Query {
        sql: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Archive counts, size, and per-repo sync state.
    Stats,
}

fn main() {
    // clap owns --help/--version: it prints them to stdout and we exit 0.
    // Any other parse failure is a user typo — USER_INPUT, not the empty-
    // stdout-exit-2 that reads as INTERNAL. clap's own default would do the
    // latter, so we intercept.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => match e.kind() {
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => {
                let _ = e.print();
                std::process::exit(0);
            }
            _ => {
                emit(&error::Error::user(e.to_string()).envelope());
                std::process::exit(2);
            }
        },
    };
    match run(cli) {
        Ok((doc, exit)) => {
            emit(&format!("{doc:#}"));
            if exit != 0 {
                std::process::exit(exit);
            }
        }
        Err(e) => {
            emit(&e.envelope());
            std::process::exit(2);
        }
    }
}

/// The single stdout writer. A closed pipe (`ghgraph … | head`) is a silent
/// success, never a panic — `println!` would panic on EPIPE, violating the
/// stdout contract every consumer depends on.
fn emit(doc: &str) {
    use std::io::Write;
    match writeln!(std::io::stdout().lock(), "{doc}") {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => std::process::exit(0),
        Err(_) => std::process::exit(2),
    }
}

/// Dispatch to (document, exit code). Gate flags (--fail-if-any, --strict)
/// change the exit code, never a byte of JSON — mechanically: the document
/// builders never receive the flag (their signatures cannot express it),
/// and the exit derives from the DISCLOSED fields of the document about to
/// be emitted, so the gate and a stdout consumer read the same numbers.
/// Exit 1 is reserved for these opt-in gates (struct Cli docs).
fn run(cli: Cli) -> Result<(serde_json::Value, i32)> {
    let cfg = config::load(cli.config.as_deref())?;
    let gated = |doc: serde_json::Value, flag: bool, trips: fn(&serde_json::Value) -> bool| {
        let exit = if flag && trips(&doc) { 1 } else { 0 };
        (doc, exit)
    };
    match cli.command {
        Command::Sync { full, pr, strict } => Ok(gated(
            sync::run(&cfg, full, pr.as_deref())?,
            strict,
            sync::incomplete,
        )),
        Command::Attention { fail_if_any, limit } => Ok(gated(
            report::attention(&cfg, limit)?,
            fail_if_any,
            report::attention_has_demands,
        )),
        Command::Prs {
            repo,
            all,
            author,
            limit,
        } => Ok((
            report::prs(&cfg, repo.as_deref(), all, author.as_deref(), limit)?,
            0,
        )),
        Command::Pr {
            reference,
            repo,
            max_body_bytes,
        } => Ok((
            report::pr(&cfg, &reference, repo.as_deref(), max_body_bytes)?,
            0,
        )),
        Command::Search { query, limit } => Ok((report::search(&cfg, &query, limit)?, 0)),
        Command::Query { sql, limit } => Ok((report::query(&cfg, sql.as_deref(), limit)?, 0)),
        Command::Stats => Ok((report::stats(&cfg)?, 0)),
    }
}
