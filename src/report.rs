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

use rusqlite::Connection;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::config::Config;
use crate::db;
use crate::error::{Error, Result};
use crate::time::Rfc3339Utc;

/// The archive shape every read verb speaks. Bumped only additively after the
/// milestone-3 freeze; a consumer keys on it to know which fields exist.
const SCHEMA_VERSION: u32 = 1;

/// Age past which an archive is called stale in `_meta` (24h). Advisory: reads
/// never fail on staleness, they disclose it.
const STALE_AFTER_SECS: i64 = 24 * 60 * 60;

/// The `_meta` header shared by every read verb. `generated_at` and each
/// entry's `age_seconds` are the enumerated timing fields — masked in golden
/// tests, everything else is byte-stable for identical archive state.
#[derive(Serialize)]
struct Meta {
    generated_at: String,
    schema_version: u32,
    /// One entry per configured repo, ordered by repo (deterministic).
    archive: Vec<ArchiveFreshness>,
}

/// Per-repo freshness. `last_synced` is null and `hint` names the remedy when a
/// repo has never been swept — the `sync --pr` path hydrates PRs without ever
/// writing a `sync_state` watermark, so "populated but never swept" is a real
/// and honest state to report, not an error.
#[derive(Serialize)]
struct ArchiveFreshness {
    repo: String,
    last_synced: Option<String>,
    age_seconds: Option<i64>,
    stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

/// The typed body of `query`: the contract shape is a struct, not an ad-hoc
/// `json!`, so a verb cannot drift from the contract without a compile error.
#[derive(Serialize)]
struct QueryResult {
    #[serde(rename = "_meta")]
    meta: Meta,
    /// Column names in SQL order (the row objects sort keys, so this is the
    /// only order-preserving view of the projection).
    columns: Vec<String>,
    rows: Vec<Map<String, Value>>,
    returned: usize,
    /// True when more rows existed than `limit` returned — presentation cap,
    /// disclosed, never silent.
    truncated: bool,
}

/// The pure core of `query`, split from connection/stdin/`_meta` wiring so the
/// SQL→JSON mapping and the limit/truncation rule are testable against an
/// in-memory connection.
#[derive(Debug)]
struct QueryOutcome {
    columns: Vec<String>,
    rows: Vec<Map<String, Value>>,
    truncated: bool,
}

/// Run one read-only statement, mapping its result set to JSON. Exactly one
/// prepared statement per invocation (the write-immunity boundary is the RO
/// open + `query_only`; single-statement is the contract). Returns `limit`
/// rows and sets `truncated` when a further row existed.
fn run_sql(conn: &Connection, sql: &str, limit: usize) -> Result<QueryOutcome> {
    // A SQL typo, a bad table, a write under query_only — all the user's to
    // fix, so USER_INPUT, never the INTERNAL that a blanket From would launder
    // it into (see error.rs).
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| Error::user(format!("invalid SQL: {e}")))?;
    let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
    let mut result = stmt
        .query([])
        .map_err(|e| Error::user(format!("invalid SQL: {e}")))?;

    let mut rows = Vec::new();
    let mut truncated = false;
    while let Some(row) = result
        .next()
        .map_err(|e| Error::user(format!("query failed: {e}")))?
    {
        // Fetch one row past the limit to distinguish "exactly limit" from
        // "more existed" — the truncation flag is honest, never a silent cap.
        if rows.len() == limit {
            truncated = true;
            break;
        }
        let mut obj = Map::new();
        for (i, col) in columns.iter().enumerate() {
            let cell = row
                .get_ref(i)
                .map_err(|e| Error::internal(format!("reading column {i}: {e}")))?;
            obj.insert(col.clone(), value_to_json(cell)?);
        }
        rows.push(obj);
    }

    Ok(QueryOutcome {
        columns,
        rows,
        truncated,
    })
}

/// Map one SQLite cell to JSON. Text must be UTF-8 (archive invariant); a blob
/// renders as a lowercase-hex string.
fn value_to_json(v: rusqlite::types::ValueRef<'_>) -> Result<Value> {
    use rusqlite::types::ValueRef;
    Ok(match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::from(i),
        ValueRef::Real(f) => Value::from(f),
        ValueRef::Text(bytes) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| Error::internal(format!("non-UTF-8 text in archive: {e}")))?;
            Value::from(s)
        }
        ValueRef::Blob(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2);
            for b in bytes {
                hex.push_str(&format!("{b:02x}"));
            }
            Value::from(hex)
        }
    })
}

/// Build the `_meta` freshness header from `sync_state`, one entry per
/// configured repo, ordered by repo name.
fn build_meta(conn: &Connection, cfg: &Config) -> Result<Meta> {
    let now = Rfc3339Utc::now();

    // Sort by repo so the archive block is deterministic regardless of config
    // order (golden-file discipline; the timing fields are the only variance).
    let mut repos: Vec<String> = cfg
        .repos
        .iter()
        .map(|e| e.resolved().repo.as_str().to_string())
        .collect();
    repos.sort_unstable();

    let mut archive = Vec::with_capacity(repos.len());
    for repo in &repos {
        let repo = repo.as_str();
        // max() over zero matching rows still yields one NULL row, so this is
        // always Ok and None means "never swept" — the sync --pr case.
        let last_synced: Option<String> = conn
            .query_row(
                "SELECT max(last_checked_at) FROM sync_state WHERE repo = ?1",
                [repo],
                |r| r.get::<_, Option<String>>(0),
            )
            .map_err(|e| Error::internal(format!("reading sync_state for {repo}: {e}")))?;

        let entry = match &last_synced {
            None => ArchiveFreshness {
                repo: repo.to_string(),
                last_synced: None,
                age_seconds: None,
                // We hold no timestamp, so we make no age claim (stale is an
                // age claim); the hint carries the remedy instead.
                stale: false,
                hint: Some("never swept — run `ghgraph sync`".to_string()),
            },
            Some(ts) => {
                let checked = Rfc3339Utc::parse(ts).map_err(|e| {
                    Error::internal(format!("corrupt last_checked_at {ts:?} for {repo}: {e:?}"))
                })?;
                let age = now.epoch() - checked.epoch();
                let stale = age >= STALE_AFTER_SECS;
                ArchiveFreshness {
                    repo: repo.to_string(),
                    last_synced: Some(ts.clone()),
                    age_seconds: Some(age),
                    stale,
                    hint: stale.then(|| "stale — run `ghgraph sync`".to_string()),
                }
            }
        };
        archive.push(entry);
    }

    Ok(Meta {
        generated_at: now.as_str().to_string(),
        schema_version: SCHEMA_VERSION,
        archive,
    })
}

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
pub fn query(cfg: &Config, sql: Option<&str>, limit: usize) -> Result<serde_json::Value> {
    let sql = resolve_sql(sql)?;
    let archive = db::open_ro(&cfg.db_path()?)?;
    let conn = archive.conn();
    let meta = build_meta(conn, cfg)?;
    let outcome = run_sql(conn, &sql, limit)?;
    let result = QueryResult {
        meta,
        columns: outcome.columns,
        returned: outcome.rows.len(),
        rows: outcome.rows,
        truncated: outcome.truncated,
    };
    // to_value on a plain struct of owned data cannot fail; classify as
    // INTERNAL rather than launder it — nothing here is the user's to fix.
    serde_json::to_value(&result).map_err(|e| Error::internal(format!("serializing result: {e}")))
}

/// The statement to run: the positional arg, or stdin when the arg is absent
/// or the literal "-". An empty statement is USER_INPUT, naming both sources.
fn resolve_sql(sql: Option<&str>) -> Result<String> {
    let text = match sql {
        Some("-") | None => {
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| Error::user(format!("cannot read SQL from stdin: {e}")))?;
            buf
        }
        Some(s) => s.to_string(),
    };
    if text.trim().is_empty() {
        return Err(Error::user(
            "no SQL provided — pass a statement as an argument or pipe one on stdin",
        ));
    }
    Ok(text)
}

pub fn stats(_cfg: &Config) -> Result<serde_json::Value> {
    todo!("table counts, db size, sync_state rows; ORDER BY repo")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn mem_with_rows() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (n INTEGER, name TEXT, r REAL, z);
             INSERT INTO t VALUES (1, 'alice', 1.5, NULL);
             INSERT INTO t VALUES (2, 'bob',   2.5, NULL);
             INSERT INTO t VALUES (3, 'carol', 3.5, NULL);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn run_sql_maps_columns_and_values() {
        let conn = mem_with_rows();
        let out = run_sql(&conn, "SELECT n, name, r, z FROM t ORDER BY n", 100).unwrap();
        assert_eq!(out.columns, vec!["n", "name", "r", "z"]);
        assert_eq!(out.rows.len(), 3);
        assert!(!out.truncated);
        let first = &out.rows[0];
        assert_eq!(first["n"], Value::from(1_i64));
        assert_eq!(first["name"], Value::from("alice"));
        assert_eq!(first["r"], Value::from(1.5_f64));
        assert_eq!(first["z"], Value::Null);
    }

    #[test]
    fn run_sql_caps_at_limit_and_flags_truncation() {
        let conn = mem_with_rows();
        let out = run_sql(&conn, "SELECT n FROM t ORDER BY n", 2).unwrap();
        assert_eq!(out.rows.len(), 2);
        assert!(
            out.truncated,
            "a third row existed, so truncated must be true"
        );
    }

    #[test]
    fn run_sql_exact_limit_is_not_truncated() {
        let conn = mem_with_rows();
        let out = run_sql(&conn, "SELECT n FROM t ORDER BY n", 3).unwrap();
        assert_eq!(out.rows.len(), 3);
        assert!(
            !out.truncated,
            "exactly limit rows and no more is not truncation"
        );
    }

    #[test]
    fn run_sql_bad_sql_is_user_input_not_internal() {
        let conn = mem_with_rows();
        let err = run_sql(&conn, "SELECT * FROM no_such_table", 100).unwrap_err();
        assert_eq!(
            err.code,
            crate::error::Code::UserInput,
            "a SQL typo is the user's to fix, never INTERNAL"
        );
    }

    fn cfg(json: &str) -> Config {
        crate::config::parse(json, "<test>").unwrap()
    }

    fn mem_with_sync_state() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE sync_state (
                 repo TEXT, stream TEXT, last_item_updated_at TEXT,
                 last_checked_at TEXT, runs_since_advance INTEGER, fingerprint TEXT);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn build_meta_reports_fresh_and_stale_ordered_by_repo() {
        let conn = mem_with_sync_state();
        let fresh = Rfc3339Utc::now().as_str().to_string();
        // Insert out of repo order to prove build_meta sorts.
        conn.execute(
            "INSERT INTO sync_state VALUES ('o/b', 'pr', NULL, '2000-01-01T00:00:00Z', 0, '')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sync_state VALUES ('o/a', 'pr', NULL, ?1, 0, '')",
            [&fresh],
        )
        .unwrap();

        let meta = build_meta(&conn, &cfg(r#"{"viewer":"me","repos":["o/b","o/a"]}"#)).unwrap();

        assert_eq!(meta.schema_version, SCHEMA_VERSION);
        let repos: Vec<&str> = meta.archive.iter().map(|a| a.repo.as_str()).collect();
        assert_eq!(repos, vec!["o/a", "o/b"], "archive entries sort by repo");

        let a = &meta.archive[0];
        assert_eq!(a.last_synced.as_deref(), Some(fresh.as_str()));
        assert!(!a.stale, "just-synced repo is not stale");
        assert!(a.hint.is_none());

        let b = &meta.archive[1];
        assert!(b.stale, "a year-2000 sync is stale");
        assert!(b.hint.is_some(), "stale carries a remedy hint");
        assert!(b.age_seconds.unwrap() > STALE_AFTER_SECS);
    }

    #[test]
    fn build_meta_never_swept_repo_is_honest_not_an_error() {
        // The `sync --pr` path populates PRs but writes no sync_state row.
        let conn = mem_with_sync_state();
        let meta = build_meta(&conn, &cfg(r#"{"viewer":"me","repos":["o/x"]}"#)).unwrap();
        let x = &meta.archive[0];
        assert_eq!(x.repo, "o/x");
        assert!(x.last_synced.is_none(), "never swept → no last_synced");
        assert!(x.age_seconds.is_none(), "no age we can honestly claim");
        assert!(!x.stale, "stale is an age claim we cannot make here");
        assert!(x.hint.is_some(), "hint names the remedy: run sync");
    }
}
