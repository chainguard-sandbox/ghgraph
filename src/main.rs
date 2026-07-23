#![forbid(unsafe_code)]
// Design-phase scaffold: the command surface, module boundaries, types, and
// invariants are the deliverable. Bodies are todo!() stubs.
#![allow(dead_code)]

// Unix only, by declaration: cancellation is process-group SIGINT semantics
// and archive protection is mode bits (0700/0600). A port would need a second
// mechanism — and a second proof — for each. Not a missing feature; a fence.
#[cfg(windows)]
compile_error!(
    "ghgraph is Unix-only: its cancellation and file-mode invariants are Unix semantics (see DESIGN.md)"
);

mod attention;
mod config;
mod db;
mod error;
mod gh;
mod queries;
mod refs;
mod report;
mod sync;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::error::Result;

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
    },
    /// What needs the operator's attention, derived from archive state.
    Attention {
        /// Exit 1 when anything is waiting — the cron gate. (Reserved; the
        /// flag exists so the contract is fixed before an implementation.)
        #[arg(long)]
        fail_if_any: bool,
    },
    /// List PRs in the archive.
    Prs {
        #[arg(long)]
        repo: Option<String>,
        /// Include merged and closed PRs.
        #[arg(long)]
        all: bool,
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
    let cli = Cli::parse();
    match run(cli) {
        Ok(doc) => println!("{doc:#}"),
        Err(e) => {
            println!("{}", e.envelope());
            std::process::exit(2);
        }
    }
}

fn run(cli: Cli) -> Result<serde_json::Value> {
    let cfg = config::load(cli.config.as_deref())?;
    match cli.command {
        Command::Sync { full, pr } => sync::run(&cfg, full, pr.as_deref()),
        Command::Attention { fail_if_any } => report::attention(&cfg, fail_if_any),
        Command::Prs { repo, all } => report::prs(&cfg, repo.as_deref(), all),
        Command::Pr { reference, repo } => report::pr(&cfg, &reference, repo.as_deref()),
        Command::Search { query, limit } => report::search(&cfg, &query, limit),
        Command::Query { sql, limit } => report::query(&cfg, sql.as_deref(), limit),
        Command::Stats => report::stats(&cfg),
    }
}
