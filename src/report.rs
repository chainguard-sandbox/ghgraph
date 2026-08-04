//! Read commands. Contract shared by every one of them:
//!
//!   * One JSON document with a `_meta` header (shape below). Keys serialize
//!     sorted (serde_json's default map; the `preserve_order` feature is
//!     deliberately absent) — sorted keys are the determinism contract, so
//!     turning that feature on would break every golden test, not just style.
//!   * Deterministic: every SELECT carries a total order (trailing unique
//!     id column breaks ties); FTS rank floats are never printed; identical
//!     archive state yields byte-identical documents modulo the enumerated
//!     timing fields (`generated_at`, `age_seconds` — the list golden tests
//!     mask, and the ONLY nondeterminism the contract allows). CI runs reads
//!     under PRAGMA reverse_unordered_selects=ON to catch missing ORDER BYs
//!     (db.rs, the GHGRAPH_TEST_REVERSE_SELECTS hook).
//!   * Reads never touch the network and never fail stale: `_meta` freshness
//!     is advisory, in-band, and derives from sync_state.last_checked_at —
//!     never the watermark, which is discovery state and lags on quiet repos
//!     (DESIGN.md, command surface).
//!
//! ```text
//! "_meta": {
//!   "schema_version": 1,
//!   "generated_at": "...",
//!   "archive": [ { "repo", "config_pending", "fingerprint",
//!                  "streams": [ { "stream", "last_checked_at",
//!                                 "age_seconds", "stale" } ],
//!                  "hint"? } ]
//! }
//! ```
//!
//!   `schema_version` is the OUTPUT-CONTRACT version ([`CONTRACT_VERSION`]),
//!   not the archive's PRAGMA user_version (db.rs) — the archive migrates
//!   without the contract moving. `stale` is a per-stream boolean (age >
//!   24h, or never synced); `hint` appears alongside it with the remedy
//!   prose. In-band staleness is the defense against consumers trusting old
//!   data; there is no --max-age flag. `fingerprint` is the STORED discovery
//!   fingerprint — what produced the archive, never a live config echo,
//!   which would lie immediately after any config change — and
//!   `config_pending` says the loaded config would produce something else
//!   (sync.rs owns the fingerprint definition; this module only compares).
//!   Every emitted PR row carries its own `verified_at` and `truncated`:
//!   repo-level age cannot bound per-PR staleness under layered refresh.
//!   * Body-carrying fields always ride with provenance (`is_minimized`,
//!     `deleted_at`) and an elision marker: `body_elided` is a property of
//!     the REQUEST (--max-body-bytes), `truncated` of the ARCHIVE — never
//!     conflated. Untrusted text is data: bodies reach the document as JSON
//!     string values and nothing here interprets them.
//!   * List commands are flat arrays of records under one key, with
//!     disclosed totals: limits govern presentation, never derivation
//!     (attention.rs owns the polarity argument). `pr` is the exception:
//!     one nested document, because its job is full context in one call:
//!
//! ```text
//! { "_meta": ...,
//!   "pr": { repo, number, url, title, body, body_elided, author,
//!           author_assoc, state, draft, base_ref, head_ref, head_sha,
//!           created_at, updated_at, merged_at, closed_at, deleted_at,
//!           review_decision, effective_review_state, truncated, verified_at,
//!           reviews:  [ { reviewer, state, submitted_at, stale } ],
//!           review_requests: [ { reviewer, kind } ],
//!           threads:  [ { id, path, line, resolved, outdated, waiting_on,
//!                         comments: [ ... ] } ],
//!           comments: [ { author, author_assoc, body, body_elided,
//!                         created_at, updated_at, is_minimized, deleted_at,
//!                         url } ],
//!           linked_issues: [ { repo, number, title, state, url, resolved } ],
//!           refs: [ { kind, source, repo, number, resolved } ] } }
//! ```
//!
//!   `waiting_on` ∈ "me" | "them" | null and `effective_review_state` /
//!   `reviews[].stale` (true/false/null — null is honest unknown) are
//!   attention.rs derivations; this module only queries and serializes.
//!   `author_id` stays internal everywhere: identity plumbing, not a display
//!   field (ROADMAP, freeze batch). `head_committed_at` also stays internal:
//!   the derived staleness fields carry its meaning.
//!   * `search` regroups hits by their parent PR/issue: bm25 ranks from
//!     separate FTS indexes (prs_fts, comments_fts, issues_fts) are not
//!     comparable, so no cross-index rank exists to sort by, honest or
//!     otherwise. Groups order by recency (updated_at DESC — the meaningful
//!     axis for work memory), tiebroken to total by (repo, number, kind).
//!     Results are LOCATORS (repo#number + where it matched), not bodies:
//!     the `pr` verb is the context call. Rejected: emitting FTS snippets —
//!     snippet() output depends on tokenizer internals, couples goldens to
//!     the porter stemmer, and re-ships third-party text a locator already
//!     names; revisit if captured MCP sessions show a search→pr round trip
//!     dominating token budgets.
//!   * `query` runs arbitrary read-only SQL. "Cannot write" is carried by
//!     the file-layer read-only open plus query_only (db.rs) and by rusqlite
//!     itself refusing multi-statement text in `prepare`
//!     (Error::MultipleStatement) — one prepared statement per invocation is
//!     a stated precondition of that proof, so the refusal is load-bearing,
//!     not pedantry. SQL parameters are refused (SQLite silently NULLs
//!     unbound parameters — a silent surprise, not an error, so the gate is
//!     here). Values map losslessly: REAL prints via serde_json's shortest
//!     round-trip (the "no floats" determinism rule bans DERIVED floats like
//!     bm25 rank; a stored REAL is stable data — though no current column is
//!     REAL, `query` must represent what SQL can produce), non-UTF-8 TEXT
//!     and BLOB print as {"$blob": "<hex>"} rather than lossy-mangling
//!     (incompleteness is never silent), and NaN/±Inf (constructible in SQL,
//!     unstorable in JSON numbers) print as {"$real": "nan"|"inf"|"-inf"}.

use std::io::IsTerminal;

use rusqlite::Connection;
use rusqlite::types::ValueRef;
use serde_json::{Map, Value, json};

use crate::attention::{self, PushBounds, ReviewFreshness, ReviewSignal, ThreadComment};
use crate::config::Config;
use crate::db::{self, RoArchive};
use crate::error::{Error, Result};
use crate::identity::RepoName;
use crate::refs;
use crate::time::Rfc3339Utc;

/// The OUTPUT-CONTRACT version stamped into `_meta.schema_version`. Distinct
/// from db::SCHEMA_VERSION (the archive's storage version): the archive can
/// migrate without a consumer-visible change, which is exactly what archive
/// v2 was. Version 1 freezes at the end of milestone 3 (all seven verbs
/// golden); from that point changes are additive-only and anything else
/// bumps this.
pub const CONTRACT_VERSION: u64 = 1;

/// A stream is stale when unchecked for longer than this (24h), or never
/// checked. Advisory only — reads never fail stale.
const STALE_AFTER_SECS: i64 = 86_400;

pub fn attention(_cfg: &Config, _fail_if_any: bool) -> Result<Value> {
    todo!("PLANNED (milestone 3): buckets per attention.rs module docs; --fail-if-any reserved")
}

pub fn prs(
    cfg: &Config,
    repo: Option<&str>,
    all: bool,
    author: Option<&str>,
    limit: Option<usize>,
) -> Result<Value> {
    // Both filters validate before touching SQL (config-grade identifiers,
    // config-grade gate — identity.rs): a typo'd repo/author becomes a named
    // USER_INPUT here, not a silently-empty result below. RepoName also
    // case-folds, matching the archive's folded storage.
    let repo = repo
        .map(|r| RepoName::new(r).map_err(|e| Error::user(format!("--repo: {e}"))))
        .transpose()?;
    let author = author
        .map(|a| crate::identity::Login::new(a).map_err(|e| Error::user(format!("--author: {e}"))))
        .transpose()?;
    let archive = open(cfg)?;
    let conn = archive.conn();

    // Filters and defaults in ONE where-clause, shared by the count and the
    // page, so the disclosed total can never disagree with the rows. The
    // default hides soft-deleted rows (an upstream-deleted PR is not open
    // work); --all shows everything, deleted_at disclosed. --author matches
    // by login_eq semantics: logins are ASCII, so COLLATE NOCASE (ASCII-only
    // in stock SQLite) is the same equivalence (identity.rs).
    const WHERE: &str = "(?1 IS NULL OR repo = ?1) \
         AND (?2 IS NULL OR author = ?2 COLLATE NOCASE) \
         AND (?3 OR (state = 'OPEN' AND deleted_at IS NULL))";
    let params = rusqlite::params![
        repo.as_ref().map(|r| r.as_str()),
        author.as_ref().map(|a| a.as_str()),
        all,
    ];

    let total: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM prs WHERE {WHERE}"),
            params,
            |r| r.get(0),
        )
        .map_err(classify_ours)?;

    let cap = limit_to_sql(limit);
    let mut stmt = conn
        .prepare(&format!(
            "SELECT repo, number, title, state, is_draft, author, author_assoc, \
                    review_decision, created_at, updated_at, merged_at, closed_at, url, \
                    truncated, verified_at, deleted_at \
             FROM prs WHERE {WHERE} ORDER BY repo, number LIMIT ?4"
        ))
        .map_err(classify_ours)?;
    let rows = stmt
        .query_map(
            rusqlite::params![
                repo.as_ref().map(|r| r.as_str()),
                author.as_ref().map(|a| a.as_str()),
                all,
                cap
            ],
            |r| {
                Ok(json!({
                    "repo": r.get::<_, String>(0)?,
                    "number": r.get::<_, i64>(1)?,
                    "title": r.get::<_, String>(2)?,
                    "state": r.get::<_, String>(3)?,
                    "draft": r.get::<_, bool>(4)?,
                    "author": r.get::<_, Option<String>>(5)?,
                    "author_assoc": r.get::<_, Option<String>>(6)?,
                    "review_decision": r.get::<_, Option<String>>(7)?,
                    "created_at": r.get::<_, String>(8)?,
                    "updated_at": r.get::<_, String>(9)?,
                    "merged_at": r.get::<_, Option<String>>(10)?,
                    "closed_at": r.get::<_, Option<String>>(11)?,
                    "url": r.get::<_, String>(12)?,
                    "truncated": r.get::<_, bool>(13)?,
                    "verified_at": r.get::<_, Option<String>>(14)?,
                    "deleted_at": r.get::<_, Option<String>>(15)?,
                }))
            },
        )
        .map_err(classify_ours)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(classify_ours)?;

    Ok(json!({
        "_meta": meta(cfg, conn)?,
        "total": total,
        "returned": rows.len(),
        "prs": rows,
    }))
}

/// Reference forms, tried in order: "owner/name#123" · GitHub PR URL ·
/// bare number (--repo, then cwd git remote, else USER_INPUT error naming
/// both remedies). Canonical repo/number/url echoed in output.
pub fn pr(
    cfg: &Config,
    reference: &str,
    repo: Option<&str>,
    max_body_bytes: Option<usize>,
) -> Result<Value> {
    let (repo, number) = resolve_pr_ref(reference, repo)?;
    let archive = open(cfg)?;
    let conn = archive.conn();

    let row = conn
        .query_row(
            "SELECT pk, title, body, state, is_draft, author, author_assoc, head_ref, \
                    base_ref, head_sha, review_decision, created_at, updated_at, merged_at, \
                    closed_at, url, truncated, verified_at, deleted_at, head_committed_at \
             FROM prs WHERE repo = ?1 AND number = ?2",
            // parse_pr_ref numbers are u64 by type; GitHub numbers fit i64
            // (SQLite INTEGER) with 2^63 to spare, so the saturation arm is
            // unreachable in practice and merely total.
            rusqlite::params![repo.as_str(), i64::try_from(number).unwrap_or(i64::MAX)],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    json!({
                        "repo": repo.as_str(),
                        "number": number,
                        "title": r.get::<_, String>(1)?,
                        "state": r.get::<_, String>(3)?,
                        "draft": r.get::<_, bool>(4)?,
                        "author": r.get::<_, Option<String>>(5)?,
                        "author_assoc": r.get::<_, Option<String>>(6)?,
                        "head_ref": r.get::<_, Option<String>>(7)?,
                        "base_ref": r.get::<_, Option<String>>(8)?,
                        "head_sha": r.get::<_, Option<String>>(9)?,
                        "review_decision": r.get::<_, Option<String>>(10)?,
                        "created_at": r.get::<_, String>(11)?,
                        "updated_at": r.get::<_, String>(12)?,
                        "merged_at": r.get::<_, Option<String>>(13)?,
                        "closed_at": r.get::<_, Option<String>>(14)?,
                        "url": r.get::<_, String>(15)?,
                        "truncated": r.get::<_, bool>(16)?,
                        "verified_at": r.get::<_, Option<String>>(17)?,
                        "deleted_at": r.get::<_, Option<String>>(18)?,
                    }),
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<String>>(19)?,
                ))
            },
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(classify_ours(e)),
        })?;
    let Some((pk, mut doc, body, pr_author, head_committed_at)) = row else {
        return Err(Error::user(format!(
            "{}#{number} is not in the archive — sync the repo (`ghgraph sync`), or pull \
             just this PR with `ghgraph sync --pr {}#{number}`",
            repo.as_str(),
            repo.as_str(),
        )));
    };
    let obj = doc.as_object_mut().expect("built as an object above");

    let (body, elided) = elide(&body, max_body_bytes);
    obj.insert("body".into(), Value::String(body));
    obj.insert("body_elided".into(), Value::Bool(elided));

    // The push bounds feeding every staleness derivation below. Latest
    // head_sha flip: freshness must be proven against the CURRENT head
    // (attention.rs PushBounds); seq breaks the tie among equal times.
    let head_flip: Option<String> = conn
        .query_row(
            "SELECT observed_at FROM observations \
             WHERE pr = ?1 AND field = 'head_sha' \
             ORDER BY observed_at DESC, seq DESC LIMIT 1",
            [pk],
            |r| r.get(0),
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            e => Err(classify_ours(e)),
        })?;
    let bounds = PushBounds {
        head_committed_at: head_committed_at.as_deref(),
        head_flip_observed_at: head_flip.as_deref(),
    };

    // Reviews: kind='review' rows are latest-per-reviewer (the sync sweeps
    // superseded ones — the effective_review_state precondition). Deleted
    // rows are superseded-or-removed reviews, not display rows.
    let mut stmt = conn
        .prepare(
            "SELECT author, state, created_at FROM comments \
             WHERE parent_kind = 'pr' AND parent = ?1 AND kind = 'review' \
               AND deleted_at IS NULL \
             ORDER BY created_at, id",
        )
        .map_err(classify_ours)?;
    let reviews: Vec<(Option<String>, Option<String>, String)> = stmt
        .query_map([pk], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;
    let signals: Vec<ReviewSignal<'_>> = reviews
        .iter()
        .filter_map(|(author, state, at)| {
            state.as_ref().map(|s| ReviewSignal {
                reviewer: author.as_deref().unwrap_or(""),
                state: s,
                submitted_at: at,
            })
        })
        .collect();
    obj.insert(
        "effective_review_state".into(),
        Value::String(
            attention::effective_review_state(&signals, &bounds)
                .as_str()
                .to_string(),
        ),
    );
    obj.insert(
        "reviews".into(),
        Value::Array(
            reviews
                .iter()
                .map(|(author, state, at)| {
                    let stale = match attention::review_freshness(at, &bounds) {
                        ReviewFreshness::Fresh => Value::Bool(false),
                        ReviewFreshness::Stale => Value::Bool(true),
                        ReviewFreshness::Unknown => Value::Null,
                    };
                    json!({
                        "reviewer": author,
                        "state": state,
                        "submitted_at": at,
                        "stale": stale,
                    })
                })
                .collect(),
        ),
    );

    let mut stmt = conn
        .prepare(
            "SELECT reviewer, kind FROM review_requests WHERE pr = ?1 \
             ORDER BY kind, reviewer",
        )
        .map_err(classify_ours)?;
    let requests: Vec<Value> = stmt
        .query_map([pk], |r| {
            Ok(json!({
                "reviewer": r.get::<_, String>(0)?,
                "kind": r.get::<_, String>(1)?,
            }))
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;
    obj.insert("review_requests".into(), Value::Array(requests));

    // Threads, with their comments and the waiting_on derivation.
    let mut stmt = conn
        .prepare(
            "SELECT pk, id, path, line, is_resolved, is_outdated FROM review_threads \
             WHERE pr = ?1 AND deleted_at IS NULL \
             ORDER BY path, line, id",
        )
        .map_err(classify_ours)?;
    // (pk, node id, path, line, resolved, outdated)
    type ThreadRow = (i64, String, Option<String>, Option<i64>, bool, bool);
    let threads: Vec<ThreadRow> = stmt
        .query_map([pk], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;
    let mut thread_docs = Vec::with_capacity(threads.len());
    for (tpk, tid, path, line, resolved, outdated) in threads {
        let comments = comment_rows(
            conn,
            "SELECT author, author_assoc, body, created_at, updated_at, is_minimized, \
                    deleted_at, url \
             FROM comments WHERE thread = ?1 ORDER BY created_at, id",
            tpk,
            max_body_bytes,
        )?;
        let waiting = comments
            .derivation
            .iter()
            .map(|(author, minimized, deleted)| ThreadComment {
                author: author.as_deref(),
                is_minimized: *minimized,
                deleted: *deleted,
            })
            .collect::<Vec<_>>();
        let waiting_on = attention::waiting_on(
            cfg.viewer.as_str(),
            pr_author.as_deref(),
            resolved,
            &waiting,
        );
        thread_docs.push(json!({
            "id": tid,
            "path": path,
            "line": line,
            "resolved": resolved,
            "outdated": outdated,
            "waiting_on": waiting_on.map(|w| w.as_str()),
            "comments": comments.docs,
        }));
    }
    obj.insert("threads".into(), Value::Array(thread_docs));

    let top_level = comment_rows(
        conn,
        "SELECT author, author_assoc, body, created_at, updated_at, is_minimized, \
                deleted_at, url \
         FROM comments WHERE parent_kind = 'pr' AND parent = ?1 AND kind = 'comment' \
         ORDER BY created_at, id",
        pk,
        max_body_bytes,
    )?;
    obj.insert("comments".into(), Value::Array(top_level.docs));

    // Refs resolve lazily at read time (schema.sql): a dangling target is
    // signal, never an error — resolved: false IS the disclosure. linked
    // issues are the fixes-refs joined against the issues cache, deduped on
    // (repo, number) since one edge can arrive by both api and body.
    let mut stmt = conn
        .prepare(
            "SELECT r.kind, r.source, r.target_repo, r.target_number, \
                    EXISTS (SELECT 1 FROM prs t \
                            WHERE t.repo = r.target_repo AND t.number = r.target_number) \
                    OR EXISTS (SELECT 1 FROM issues t \
                               WHERE t.repo = r.target_repo AND t.number = r.target_number) \
             FROM refs r WHERE r.src_pr = ?1 \
             ORDER BY r.kind, r.source, r.target_repo, r.target_number",
        )
        .map_err(classify_ours)?;
    let ref_docs: Vec<Value> = stmt
        .query_map([pk], |r| {
            Ok(json!({
                "kind": r.get::<_, String>(0)?,
                "source": r.get::<_, String>(1)?,
                "repo": r.get::<_, String>(2)?,
                "number": r.get::<_, i64>(3)?,
                "resolved": r.get::<_, bool>(4)?,
            }))
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;
    obj.insert("refs".into(), Value::Array(ref_docs));

    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT r.target_repo, r.target_number, i.title, i.state, i.url, \
                    i.repo IS NOT NULL \
             FROM refs r LEFT JOIN issues i \
               ON i.repo = r.target_repo AND i.number = r.target_number \
             WHERE r.src_pr = ?1 AND r.kind = 'fixes' \
             ORDER BY r.target_repo, r.target_number",
        )
        .map_err(classify_ours)?;
    let linked: Vec<Value> = stmt
        .query_map([pk], |r| {
            Ok(json!({
                "repo": r.get::<_, String>(0)?,
                "number": r.get::<_, i64>(1)?,
                "title": r.get::<_, Option<String>>(2)?,
                "state": r.get::<_, Option<String>>(3)?,
                "url": r.get::<_, Option<String>>(4)?,
                "resolved": r.get::<_, bool>(5)?,
            }))
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;
    obj.insert("linked_issues".into(), Value::Array(linked));

    Ok(json!({ "_meta": meta(cfg, conn)?, "pr": doc }))
}

pub fn search(cfg: &Config, query: &str, limit: usize) -> Result<Value> {
    let archive = open(cfg)?;
    let conn = archive.conn();

    // (kind, repo, number) → group. BTreeMap gives the dedupe; the final
    // order is recency (module docs) applied after collection.
    struct Group {
        head: Value,
        updated_at: String,
        self_match: bool,
        comment_matches: i64,
    }
    let mut groups: std::collections::BTreeMap<(String, String, i64), Group> =
        std::collections::BTreeMap::new();

    let mut collect = |kind: &str, sql: &str, self_match: bool| -> Result<()> {
        let mut stmt = conn.prepare(sql).map_err(classify_ours)?;
        let rows = stmt
            .query_map([query], |r| {
                Ok((
                    r.get::<_, String>(0)?,          // repo
                    r.get::<_, i64>(1)?,             // number
                    r.get::<_, String>(2)?,          // title
                    r.get::<_, String>(3)?,          // state
                    r.get::<_, String>(4)?,          // updated_at
                    r.get::<_, String>(5)?,          // url
                    r.get::<_, Option<String>>(6)?,  // author
                    r.get::<_, Option<String>>(7)?,  // author_assoc
                    r.get::<_, bool>(8)?,            // truncated
                    r.get::<_, Option<String>>(9)?,  // verified_at
                    r.get::<_, Option<String>>(10)?, // deleted_at
                ))
            })
            .map_err(classify_user_query)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(classify_user_query)?;
        for (
            repo,
            number,
            title,
            state,
            updated_at,
            url,
            author,
            assoc,
            truncated,
            verified,
            deleted,
        ) in rows
        {
            let key = (kind.to_string(), repo.clone(), number);
            let g = groups.entry(key).or_insert_with(|| Group {
                head: json!({
                    "kind": kind,
                    "repo": repo,
                    "number": number,
                    "title": title,
                    "state": state,
                    "updated_at": updated_at.clone(),
                    "url": url,
                    "author": author,
                    "author_assoc": assoc,
                    "truncated": truncated,
                    "verified_at": verified,
                    "deleted_at": deleted,
                }),
                updated_at,
                self_match: false,
                comment_matches: 0,
            });
            if self_match {
                g.self_match = true;
            } else {
                g.comment_matches += 1;
            }
        }
        Ok(())
    };

    const PR_COLS: &str = "p.repo, p.number, p.title, p.state, p.updated_at, p.url, \
                           p.author, p.author_assoc, p.truncated, p.verified_at, p.deleted_at";
    const ISSUE_COLS: &str = "i.repo, i.number, i.title, i.state, i.updated_at, i.url, \
                              i.author, i.author_assoc, i.truncated, i.verified_at, i.deleted_at";
    collect(
        "pr",
        &format!(
            "SELECT {PR_COLS} FROM prs_fts f JOIN prs p ON p.pk = f.rowid \
                  WHERE prs_fts MATCH ?1"
        ),
        true,
    )?;
    collect(
        "issue",
        &format!(
            "SELECT {ISSUE_COLS} FROM issues_fts f JOIN issues i ON i.pk = f.rowid \
                  WHERE issues_fts MATCH ?1"
        ),
        true,
    )?;
    // One row per MATCHED COMMENT (not per parent): the per-group count is
    // the fold, done here rather than in SQL so the parent lookup branches
    // on parent_kind exactly the way the schema demands (comments join by
    // parent_kind — schema.sql).
    collect(
        "pr",
        &format!(
            "SELECT {PR_COLS} FROM comments_fts f \
             JOIN comments c ON c.pk = f.rowid AND c.parent_kind = 'pr' \
             JOIN prs p ON p.pk = c.parent \
             WHERE comments_fts MATCH ?1"
        ),
        false,
    )?;
    collect(
        "issue",
        &format!(
            "SELECT {ISSUE_COLS} FROM comments_fts f \
             JOIN comments c ON c.pk = f.rowid AND c.parent_kind = 'issue' \
             JOIN issues i ON i.pk = c.parent \
             WHERE comments_fts MATCH ?1"
        ),
        false,
    )?;

    // Recency DESC, then (repo, number, kind) — a total order: (repo,
    // number, kind) is the map key, hence unique (kind disambiguates
    // nothing on GitHub, where PRs and issues share a number space, but the
    // schema does not enforce that, so it stays in the key).
    let mut ordered: Vec<((String, String, i64), Group)> = groups.into_iter().collect();
    ordered.sort_by(|((ka, ra, na), ga), ((kb, rb, nb), gb)| {
        gb.updated_at
            .cmp(&ga.updated_at)
            .then_with(|| ra.cmp(rb))
            .then_with(|| na.cmp(nb))
            .then_with(|| ka.cmp(kb))
    });
    let total = ordered.len();
    let results: Vec<Value> = ordered
        .into_iter()
        .take(limit)
        .map(|(_, g)| {
            let mut head = g.head;
            let obj = head.as_object_mut().expect("built as an object above");
            obj.insert(
                "matched".into(),
                json!({ "self": g.self_match, "comments": g.comment_matches }),
            );
            head
        })
        .collect();

    Ok(json!({
        "_meta": meta(cfg, conn)?,
        "total": total,
        "returned": results.len(),
        "results": results,
    }))
}

/// SQL from the positional arg, or stdin when it is "-" or absent-and-piped.
/// Connection is read-only twice over (open flag + query_only pragma), and
/// rusqlite's prepare refuses multi-statement text — the module docs carry
/// why that refusal is load-bearing.
pub fn query(cfg: &Config, sql: Option<&str>, limit: usize) -> Result<Value> {
    let sql = match sql {
        Some("-") | None => {
            let mut stdin = std::io::stdin();
            if sql.is_none() && stdin.is_terminal() {
                return Err(Error::user(
                    "no SQL: pass a statement as the argument, or pipe one to stdin",
                ));
            }
            let mut buf = String::new();
            std::io::Read::read_to_string(&mut stdin, &mut buf)
                .map_err(|e| Error::user(format!("cannot read SQL from stdin: {e}")))?;
            buf
        }
        Some(s) => s.to_string(),
    };
    if sql.trim().is_empty() {
        return Err(Error::user("empty SQL"));
    }

    let archive = open(cfg)?;
    let conn = archive.conn();
    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(rusqlite::Error::MultipleStatement) => {
            return Err(Error::user(
                "multiple SQL statements — query runs exactly one per invocation",
            ));
        }
        Err(e) => return Err(classify_user_query(e)),
    };
    if stmt.parameter_count() > 0 {
        // SQLite runs unbound parameters as NULL — a silent surprise, so a
        // refusal here, not a footnote in the output.
        return Err(Error::user(
            "SQL parameters (?, :name) are not supported — inline the value",
        ));
    }
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let n_cols = columns.len();

    let mut rows_out: Vec<Value> = Vec::new();
    let mut truncated_at_limit = false;
    let mut rows = stmt.query([]).map_err(classify_user_query)?;
    loop {
        match rows.next().map_err(classify_user_query)? {
            None => break,
            Some(row) => {
                if rows_out.len() == limit {
                    truncated_at_limit = true;
                    break;
                }
                let mut out = Vec::with_capacity(n_cols);
                for i in 0..n_cols {
                    out.push(sql_value_to_json(
                        row.get_ref(i).map_err(classify_user_query)?,
                    ));
                }
                rows_out.push(Value::Array(out));
            }
        }
    }

    Ok(json!({
        "_meta": meta(cfg, conn)?,
        "columns": columns,
        "row_count": rows_out.len(),
        "rows": rows_out,
        "truncated_at_limit": truncated_at_limit,
    }))
}

pub fn stats(cfg: &Config) -> Result<Value> {
    let archive = open(cfg)?;
    let conn = archive.conn();

    // Audits (orphans, observation chain, FTS integrity, watermark
    // assertion) are PLANNED (milestone 5, hardening) — this is the count
    // surface they will land beside.
    let mut counts = Map::new();
    for table in [
        "comments",
        "issues",
        "observations",
        "prs",
        "quarantine",
        "refs",
        "review_requests",
        "review_threads",
    ] {
        let n: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .map_err(classify_ours)?;
        counts.insert(table.to_string(), Value::Number(n.into()));
    }

    let db_bytes: i64 = conn
        .query_row(
            "SELECT (SELECT * FROM pragma_page_count()) * (SELECT * FROM pragma_page_size())",
            [],
            |r| r.get(0),
        )
        .map_err(classify_ours)?;

    let mut stmt = conn
        .prepare(
            "SELECT repo, stream, last_item_updated_at, last_checked_at, \
                    runs_since_advance, fingerprint \
             FROM sync_state ORDER BY repo, stream",
        )
        .map_err(classify_ours)?;
    let sync_state: Vec<Value> = stmt
        .query_map([], |r| {
            let fp: String = r.get(5)?;
            Ok(json!({
                "repo": r.get::<_, String>(0)?,
                "stream": r.get::<_, String>(1)?,
                "last_item_updated_at": r.get::<_, String>(2)?,
                "last_checked_at": r.get::<_, Option<String>>(3)?,
                "runs_since_advance": r.get::<_, i64>(4)?,
                // Stored as JSON text; disclosed structured, like _meta.
                // Unparseable ⇒ null (the cold-start posture — sync.rs
                // transition), never a raw-string leak of undefined shape.
                "fingerprint": serde_json::from_str::<Value>(&fp).unwrap_or(Value::Null),
            }))
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;

    let mut stmt = conn
        .prepare(
            "SELECT repo, COUNT(*), MIN(next_retry_at) FROM quarantine \
             GROUP BY repo ORDER BY repo",
        )
        .map_err(classify_ours)?;
    let quarantine: Vec<Value> = stmt
        .query_map([], |r| {
            Ok(json!({
                "repo": r.get::<_, String>(0)?,
                "count": r.get::<_, i64>(1)?,
                "next_retry_at": r.get::<_, Option<String>>(2)?,
            }))
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;

    let archive_version: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(classify_ours)?;

    Ok(json!({
        "_meta": meta(cfg, conn)?,
        "stats": {
            "archive_schema_version": archive_version,
            "counts": counts,
            "db_bytes": db_bytes,
            "quarantine": quarantine,
            "sync_state": sync_state,
        },
    }))
}

// ---------------------------------------------------------------------------
// Shared plumbing

fn open(cfg: &Config) -> Result<RoArchive> {
    db::open_ro(&cfg.db_path()?)
}

/// The `_meta` header (module docs). One entry per repo in the UNION of the
/// loaded config and sync_state: a configured-but-never-synced repo must
/// show as such rather than vanish, and an archived repo dropped from the
/// config keeps disclosing what produced its rows (config_pending: true says
/// the config no longer would).
fn meta(cfg: &Config, conn: &Connection) -> Result<Value> {
    let now = Rfc3339Utc::now();

    struct StreamRow {
        stream: String,
        last_checked_at: Option<String>,
        fingerprint: String,
    }
    let mut stmt = conn
        .prepare(
            "SELECT repo, stream, last_checked_at, fingerprint \
             FROM sync_state ORDER BY repo, stream",
        )
        .map_err(classify_ours)?;
    let mut by_repo: std::collections::BTreeMap<String, Vec<StreamRow>> =
        std::collections::BTreeMap::new();
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                StreamRow {
                    stream: r.get(1)?,
                    last_checked_at: r.get(2)?,
                    fingerprint: r.get(3)?,
                },
            ))
        })
        .map_err(classify_ours)?;
    for row in rows {
        let (repo, sr) = row.map_err(classify_ours)?;
        by_repo.entry(repo).or_default().push(sr);
    }

    let configured: std::collections::BTreeMap<String, crate::config::RepoConfig> = cfg
        .repos
        .iter()
        .map(|e| {
            let rc = e.resolved();
            (rc.repo.as_str().to_string(), rc)
        })
        .collect();

    let mut all_repos: std::collections::BTreeSet<&String> = by_repo.keys().collect();
    all_repos.extend(configured.keys());

    let mut archive = Vec::with_capacity(all_repos.len());
    for repo in all_repos {
        let streams = by_repo.get(repo).map(Vec::as_slice).unwrap_or(&[]);
        let rc = configured.get(repo);

        // config_pending: would the loaded config produce this archive
        // slice? Fingerprints are compared as parsed JSON (they are
        // structured for field-level comparison — schema.sql); the expected
        // stream set is part of the answer, since `issues: true` with no
        // issue stream yet means the next sync adds one.
        let computed = rc.map(|rc| crate::sync::fingerprint(cfg, rc));
        let expected_streams: Vec<&str> = match rc {
            None => Vec::new(),
            Some(rc) if rc.issues() => vec!["issue", "pr"],
            Some(_) => vec!["pr"],
        };
        let present: Vec<&str> = streams.iter().map(|s| s.stream.as_str()).collect();
        let fp_match = |stored: &str| -> bool {
            match (&computed, serde_json::from_str::<Value>(stored)) {
                (Some(c), Ok(s)) => *c == s,
                _ => false,
            }
        };
        let config_pending = rc.is_none()
            || present != expected_streams
            || streams.iter().any(|s| !fp_match(&s.fingerprint));

        // The disclosed fingerprint is the STORED one (what produced the
        // archive). Streams share a fingerprint in every steady state; in a
        // transitional one the 'pr' stream's stands for the repo and
        // config_pending is already true. Never synced ⇒ null.
        let disclosed = streams
            .iter()
            .find(|s| s.stream == "pr")
            .or(streams.first())
            .map(|s| serde_json::from_str::<Value>(&s.fingerprint).unwrap_or(Value::Null))
            .unwrap_or(Value::Null);

        let mut any_stale = streams.is_empty();
        let stream_docs: Vec<Value> = streams
            .iter()
            .map(|s| {
                let age = s.last_checked_at.as_deref().and_then(|t| {
                    Rfc3339Utc::parse(t)
                        .ok()
                        // A checked-in-the-future stamp is clock skew, not
                        // negative age; clamp and let stale stay false.
                        .map(|t| (now.epoch() - t.epoch()).max(0))
                });
                let stale = age.is_none_or(|a| a > STALE_AFTER_SECS);
                any_stale |= stale;
                json!({
                    "stream": s.stream,
                    "last_checked_at": s.last_checked_at,
                    "age_seconds": age,
                    "stale": stale,
                })
            })
            .collect();

        let mut entry = json!({
            "repo": repo,
            "config_pending": config_pending,
            "fingerprint": disclosed,
            "streams": stream_docs,
        });
        if any_stale {
            entry
                .as_object_mut()
                .expect("built as an object above")
                .insert(
                    "hint".into(),
                    Value::String(if streams.is_empty() {
                        "never synced — run: ghgraph sync".into()
                    } else {
                        "stale — run: ghgraph sync".into()
                    }),
                );
        }
        archive.push(entry);
    }

    Ok(json!({
        "schema_version": CONTRACT_VERSION,
        "generated_at": now.as_str(),
        "archive": archive,
    }))
}

/// `--limit` → SQL LIMIT: absent means every row (LIMIT -1 is SQLite's
/// "none"). usize→i64 saturates rather than wraps — a limit beyond i64 is
/// "all" in effect anyway.
fn limit_to_sql(limit: Option<usize>) -> i64 {
    match limit {
        None => -1,
        Some(n) => i64::try_from(n).unwrap_or(i64::MAX),
    }
}

/// Truncate `body` to at most `max` BYTES on a char boundary. UTF-8 safety
/// is the point: a byte budget must never split a code point (the output is
/// a JSON string, which cannot carry half of one).
fn elide(body: &str, max: Option<usize>) -> (String, bool) {
    let Some(max) = max else {
        return (body.to_string(), false);
    };
    if body.len() <= max {
        return (body.to_string(), false);
    }
    let mut end = max;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    (body[..end].to_string(), true)
}

struct CommentDocs {
    docs: Vec<Value>,
    /// (author, is_minimized, deleted) per row, for the waiting_on
    /// derivation — kept beside the docs so both views come from ONE query
    /// and cannot disagree on order or membership.
    derivation: Vec<(Option<String>, bool, bool)>,
}

/// Comments under one parent, in thread order, with body provenance and
/// elision applied. The SQL is caller-supplied because the three comment
/// surfaces (thread, PR top-level, issue top-level later) differ only in
/// their WHERE; the column list is fixed here.
fn comment_rows(
    conn: &Connection,
    sql: &str,
    parent_pk: i64,
    max_body_bytes: Option<usize>,
) -> Result<CommentDocs> {
    let mut stmt = conn.prepare(sql).map_err(classify_ours)?;
    let mut docs = Vec::new();
    let mut derivation = Vec::new();
    let rows = stmt
        .query_map([parent_pk], |r| {
            Ok((
                r.get::<_, Option<String>>(0)?, // author
                r.get::<_, Option<String>>(1)?, // author_assoc
                r.get::<_, String>(2)?,         // body
                r.get::<_, String>(3)?,         // created_at
                r.get::<_, Option<String>>(4)?, // updated_at
                r.get::<_, bool>(5)?,           // is_minimized
                r.get::<_, Option<String>>(6)?, // deleted_at
                r.get::<_, Option<String>>(7)?, // url
            ))
        })
        .map_err(classify_ours)?;
    for row in rows {
        let (author, assoc, body, created, updated, minimized, deleted, url) =
            row.map_err(classify_ours)?;
        let (body, elided) = elide(&body, max_body_bytes);
        derivation.push((author.clone(), minimized, deleted.is_some()));
        docs.push(json!({
            "author": author,
            "author_assoc": assoc,
            "body": body,
            "body_elided": elided,
            "created_at": created,
            "updated_at": updated,
            "is_minimized": minimized,
            "deleted_at": deleted,
            "url": url,
        }));
    }
    Ok(CommentDocs { docs, derivation })
}

/// Resolve a PR reference to (repo, number). Forms in order: qualified /
/// URL (refs.rs — host pinned there), bare number via --repo, then the cwd
/// git remote. The remote URL is attacker-chosen content (DESIGN.md,
/// security posture): it gets the same host pinning and RepoName validation
/// as any other identifier, here, before crossing any module boundary.
fn resolve_pr_ref(reference: &str, repo_flag: Option<&str>) -> Result<(RepoName, u64)> {
    if let Some((repo, number)) = refs::parse_pr_ref(reference) {
        return Ok((repo, number));
    }
    let number: u64 = reference.parse().map_err(|_| {
        Error::user(format!(
            "cannot parse PR reference {reference:?} — use owner/name#123, a \
             https://github.com/owner/name/pull/123 URL, or a bare number with --repo \
             (or from inside a github.com clone)"
        ))
    })?;
    if let Some(repo) = repo_flag {
        let repo = RepoName::new(repo).map_err(|e| Error::user(format!("--repo: {e}")))?;
        return Ok((repo, number));
    }
    match cwd_github_repo() {
        Some(repo) => Ok((repo, number)),
        None => Err(Error::user(format!(
            "bare PR number {number} needs a repo: pass --repo owner/name, or run inside \
             a github.com clone"
        ))),
    }
}

/// The cwd git remote → owner/name, if it names github.com. Tried remotes:
/// `upstream` first, then `origin` — in a fork clone upstream is where PRs
/// live and origin is the fork; in a plain clone only origin exists. The
/// reversal evidence: an operator whose `upstream` names something other
/// than the PR home (they pass --repo, which always wins). git here is a
/// LOCAL config read (`git remote get-url` touches no network) — the
/// no-HTTP rule is about transports, and the runtime git prerequisite is no
/// heavier than the gh one. None (git missing, not a repo, non-GitHub
/// remote) falls back to the USER_INPUT error naming both remedies.
fn cwd_github_repo() -> Option<RepoName> {
    for remote in ["upstream", "origin"] {
        let out = std::process::Command::new("git")
            .args(["remote", "get-url", remote])
            .stderr(std::process::Stdio::null())
            .output()
            .ok()?;
        if !out.status.success() {
            continue;
        }
        let url = String::from_utf8(out.stdout).ok()?;
        if let Some(repo) = github_repo_from_remote_url(url.trim()) {
            return Some(repo);
        }
    }
    None
}

/// Parse owner/name out of a github.com remote URL. Host PINNED to
/// github.com (DESIGN.md: URL host policy — GHES waits for an operator);
/// recognized forms are the three git ships: scp-like
/// `git@github.com:owner/name`, `ssh://git@github.com/owner/name`, and
/// `https://github.com/owner/name`, each with an optional `.git`. Anything
/// else is None, never a guess — the value is validated by RepoName like
/// every identifier.
fn github_repo_from_remote_url(url: &str) -> Option<RepoName> {
    let path = url
        .strip_prefix("git@github.com:")
        .or_else(|| url.strip_prefix("ssh://git@github.com/"))
        .or_else(|| url.strip_prefix("https://github.com/"))?;
    let path = path.strip_suffix(".git").unwrap_or(path);
    let path = path.strip_suffix('/').unwrap_or(path);
    RepoName::new(path).ok()
}

/// One SQL value → JSON, losslessly (module docs carry the rationale for
/// the $blob / $real escape hatches).
fn sql_value_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Number(i.into()),
        ValueRef::Real(f) => match serde_json::Number::from_f64(f) {
            Some(n) => Value::Number(n),
            None => json!({ "$real": if f.is_nan() {
                "nan"
            } else if f > 0.0 {
                "inf"
            } else {
                "-inf"
            }}),
        },
        ValueRef::Text(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Value::String(s.to_string()),
            Err(_) => json!({ "$blob": hex(bytes) }),
        },
        ValueRef::Blob(bytes) => json!({ "$blob": hex(bytes) }),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Failures of OUR fixed statements: busy → TRANSIENT; a corrupt or foreign
/// file → CONFIGURATION with the disposable-cache remedy; anything else is
/// a ghgraph bug (the read twin of sync.rs classify_sql).
fn classify_ours(e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(err, _) = &e {
        if matches!(
            err.code,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
        ) {
            return Error::transient(format!("archive is busy: {e}"));
        }
        if matches!(
            err.code,
            rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
        ) {
            return Error::config(format!(
                "archive is corrupt: {e} — the archive is a disposable cache; \
                 remove it and resync"
            ));
        }
    }
    Error::internal(format!("archive read failed: {e}"))
}

/// Failures while running USER-supplied text (`query` SQL, `search` MATCH):
/// the user can fix their statement, so USER_INPUT — except busy, which is
/// the environment's to fix. The sqlite message names the syntax problem
/// without this code ever interpolating archive content.
fn classify_user_query(e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(err, _) = &e
        && matches!(
            err.code,
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
        )
    {
        return Error::transient(format!("archive is busy: {e}"));
    }
    Error::user(format!("query failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- elide: the byte budget never splits a code point ----------------

    /// Exhaustive over a multibyte-dense string × every budget: the domain
    /// (each boundary of each width 1..4) is small enough that the loop is
    /// the proof. Properties: output is a prefix, fits the budget, is valid
    /// UTF-8 by construction (the slice would panic otherwise), and elided
    /// ⟺ shortened.
    #[test]
    fn elide_exhaustive_over_budgets() {
        let s = "a£€🦀b£€🦀"; // widths 1,2,3,4 interleaved twice
        for max in 0..=s.len() + 2 {
            let (out, elided) = elide(s, Some(max));
            assert!(out.len() <= max, "budget {max}: {} > {max}", out.len());
            assert!(s.starts_with(&out), "budget {max}: not a prefix");
            assert_eq!(elided, out.len() < s.len(), "budget {max}: flag wrong");
            // Maximality: the next byte would split a code point or exceed.
            if elided {
                let longer = out.len() + 1;
                assert!(
                    longer > max || !s.is_char_boundary(longer),
                    "budget {max}: gave up {longer} early"
                );
            }
        }
        assert_eq!(elide(s, None), (s.to_string(), false));
    }

    // ---- reference resolution --------------------------------------------

    #[test]
    fn remote_url_forms_parse_and_pin_host() {
        for url in [
            "git@github.com:Owner/Repo.git",
            "git@github.com:Owner/Repo",
            "ssh://git@github.com/Owner/Repo.git",
            "https://github.com/Owner/Repo",
            "https://github.com/Owner/Repo.git",
        ] {
            let repo = github_repo_from_remote_url(url).expect(url);
            assert_eq!(repo.as_str(), "owner/repo", "case folds: {url}");
        }
        for url in [
            "https://github.evil.com/owner/repo",     // host suffix spoof
            "git@github.com.evil.com:owner/repo.git", // scp-like spoof
            "https://gitlab.com/owner/repo",          // wrong forge
            "https://github.com/owner",               // no repo
            "https://github.com/owner/repo/extra",    // trailing path
            "git@github.com:owner/repo;rm -rf /",     // injection shape
            "",
        ] {
            assert!(
                github_repo_from_remote_url(url).is_none(),
                "must reject {url:?}"
            );
        }
    }

    #[test]
    fn resolve_ref_prefers_qualified_then_flag() {
        let (repo, n) = resolve_pr_ref("octo/repo#7", None).unwrap();
        assert_eq!((repo.as_str(), n), ("octo/repo", 7));
        let (repo, n) = resolve_pr_ref("https://github.com/octo/repo/pull/8", None).unwrap();
        assert_eq!((repo.as_str(), n), ("octo/repo", 8));
        let (repo, n) = resolve_pr_ref("9", Some("octo/repo")).unwrap();
        assert_eq!((repo.as_str(), n), ("octo/repo", 9));
        // A bad --repo is a named USER_INPUT, not a silent miss.
        let err = resolve_pr_ref("9", Some("not a repo")).unwrap_err();
        assert_eq!(err.code, crate::error::Code::UserInput);
        // Garbage reference: the error names every remedy.
        let err = resolve_pr_ref("nonsense", None).unwrap_err();
        assert_eq!(err.code, crate::error::Code::UserInput);
        assert!(err.message.contains("--repo"), "{}", err.message);
    }

    // ---- sql_value_to_json ------------------------------------------------

    #[test]
    fn sql_values_map_losslessly() {
        assert_eq!(sql_value_to_json(ValueRef::Null), Value::Null);
        assert_eq!(sql_value_to_json(ValueRef::Integer(-7)), json!(-7));
        assert_eq!(sql_value_to_json(ValueRef::Real(1.5)), json!(1.5));
        assert_eq!(
            sql_value_to_json(ValueRef::Real(f64::INFINITY)),
            json!({"$real": "inf"})
        );
        assert_eq!(
            sql_value_to_json(ValueRef::Real(f64::NEG_INFINITY)),
            json!({"$real": "-inf"})
        );
        assert_eq!(
            sql_value_to_json(ValueRef::Real(f64::NAN)),
            json!({"$real": "nan"})
        );
        assert_eq!(
            sql_value_to_json(ValueRef::Text("ok".as_bytes())),
            json!("ok")
        );
        assert_eq!(
            sql_value_to_json(ValueRef::Text(&[0xff, 0x00])),
            json!({"$blob": "ff00"}),
            "invalid UTF-8 TEXT discloses bytes rather than lossy-mangling"
        );
        assert_eq!(
            sql_value_to_json(ValueRef::Blob(&[0xde, 0xad])),
            json!({"$blob": "dead"})
        );
    }

    #[test]
    fn limit_to_sql_boundaries() {
        assert_eq!(limit_to_sql(None), -1);
        assert_eq!(limit_to_sql(Some(0)), 0);
        assert_eq!(limit_to_sql(Some(100)), 100);
        assert_eq!(limit_to_sql(Some(usize::MAX)), i64::MAX);
    }
}
