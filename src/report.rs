//! Read commands. Contract shared by every one of them:
//!
//!   * One JSON document with a `_meta` header:
//!
//! ```text
//! "_meta": {
//!   "generated_at": "...",
//!   "archive": [ { "repo", "last_synced", "age_seconds", "hint"? } ]
//! }
//! ```
//!
//!   `hint` appears when an archive is older than 24h ("stale — run:
//!   ghgraph sync"). In-band staleness is the defense against consumers
//!   trusting old data; there is no --max-age flag.
//!   * Deterministic: every SELECT carries a total order (trailing unique
//!     id column breaks ties); FTS rank floats are never printed; JSON is
//!     serialized from structs/sorted keys, never HashMap order.
//!     CI runs reads under PRAGMA reverse_unordered_selects=ON to catch
//!     missing ORDER BYs.
//!   * List commands are flat arrays of records under one key. `pr` is the
//!     exception: one nested document, because its job is full context in
//!     one call:
//!
//! ```text
//! { "_meta": ...,
//!   "pr": { repo, number, url, title, author, state, draft, base, head,
//!           created_at, updated_at, review_decision, effective_review_state,
//!           reviews:  [ { reviewer, state, submitted_at, stale } ],
//!           threads:  [ { id, path, line, resolved, outdated, waiting_on,
//!                         comments: [ { author, body, created_at, url } ] } ],
//!           comments: [ { author, body, created_at, url } ],
//!           linked_issues: [ { repo, number, title, state, url } ],
//!           refs: [ { kind, source, repo, number } ] } }
//! ```
//!
//!   `waiting_on` ∈ "me" | "them" | null is derived; it is what makes
//!   attention and jq one-liners trivial.

use crate::config::Config;
use crate::error::Result;

pub fn attention(_cfg: &Config, _fail_if_any: bool) -> Result<serde_json::Value> {
    todo!("buckets per attention.rs module docs; --fail-if-any reserved, unimplemented")
}

pub fn prs(_cfg: &Config, _repo: Option<&str>, _all: bool) -> Result<serde_json::Value> {
    todo!("flat array; default open only; ORDER BY repo, number")
}

/// Reference forms, tried in order: "owner/name#123" · GitHub PR URL ·
/// bare number (--repo, then cwd git remote, else USER_INPUT error naming
/// both remedies). Canonical repo/number/url echoed in output.
pub fn pr(_cfg: &Config, _reference: &str, _repo: Option<&str>) -> Result<serde_json::Value> {
    todo!()
}

pub fn search(_cfg: &Config, _query: &str, _limit: usize) -> Result<serde_json::Value> {
    todo!("prs_fts UNION comments_fts; ORDER BY rank, id; rank not printed")
}

/// SQL from the positional arg, or stdin when it is "-" or absent-and-piped.
/// Connection is read-only twice over (open flag + query_only pragma).
pub fn query(_cfg: &Config, _sql: Option<&str>, _limit: usize) -> Result<serde_json::Value> {
    todo!()
}

pub fn stats(_cfg: &Config) -> Result<serde_json::Value> {
    todo!("table counts, db size, sync_state rows; ORDER BY repo")
}
