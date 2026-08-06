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
//!     mask, and the ONLY nondeterminism the contract allows). `stale` and
//!     `hint` are quantized functions of that same masked clock: they flip
//!     only when age crosses the 24h boundary between two runs, never
//!     independently of it, so a consumer masking the two enumerated fields
//!     must read a boundary flip as clock movement, not an archive change.
//!     CI runs reads under
//!     PRAGMA reverse_unordered_selects=ON to catch missing ORDER BYs
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
//!           reviews:  [ { reviewer, state, submitted_at, freshness } ],
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
//!   `reviews[].freshness` ("fresh" | "stale" | "unknown" — an explicit
//!   enum, never a nullable boolean; the unknown case must not be
//!   truth-testable by accident) are attention.rs derivations; this module
//!   only queries and serializes.
//!   `author_id` stays internal everywhere: identity plumbing, not a display
//!   field (ROADMAP, freeze batch). `head_committed_at` also stays internal:
//!   the derived staleness fields carry its meaning.
//!   * `attention` emits the buckets as an ARRAY — the one place key
//!     order matters (fixed order IS priority, attention.rs) and sorted-key
//!     objects cannot carry it:
//!
//! ```text
//! { "_meta": ...,
//!   "attention": [ { "bucket": "waiting_on_me" | "they_replied" |
//!                              "ready_to_merge" | "people_prs" |
//!                              "needs_reviewer" | "untriaged",
//!                    "total", "returned",
//!                    "prs": [ { repo, number, title, draft, author,
//!                               author_assoc, updated_at, url, truncated,
//!                               verified_at,
//!                               requested_via? threads_waiting?      (waiting_on_me)
//!                               last_other_activity_at? } ]          (they_replied)
//!                    "issues": [ { same minus draft } ] } ] }        (untriaged only)
//! ```
//!
//!   Rows are locators (search's argument), recency-ordered (updated_at
//!   DESC, tiebroken total by repo, number); every emitted bucket appears
//!   even when empty — an empty array IS the "checked, nothing"
//!   disclosure. The maintainer pair (needs_reviewer, untriaged) is
//!   emitted only when the loaded config has a project-scope repo, and is
//!   ABSENT otherwise — "checked, nothing" would claim a sweep the config
//!   never asked for (attention.rs owns the argument and the per-row
//!   project_scope gate). untriaged rows are issues and ride under an
//!   "issues" key — same locator fields minus `draft`, which an issue does
//!   not have. Both additions are additive under schema_version 1: no
//!   pre-existing bucket, field, or ordering moved.
//!   `--fail-if-any` never reaches this module: main.rs derives the exit
//!   code from [`attention_has_demands`] over the emitted document, built
//!   with no flag in scope — gate flags change the exit code, never a
//!   byte of JSON, and the signature is the mechanism.
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

use crate::attention::{self, PushBounds, ReviewSignal, ThreadComment};
use crate::config::Config;
use crate::db::{self, RoArchive};
use crate::error::{Error, Result};
use crate::identity::{RepoName, login_eq};
use crate::refs;
use crate::time::Rfc3339Utc;

/// The OUTPUT-CONTRACT version stamped into `_meta.schema_version`. Distinct
/// from db::SCHEMA_VERSION (the archive's storage version): the archive can
/// migrate without a consumer-visible change, which is exactly what archive
/// v2 was. Version 1 FROZE with milestone 3 — all seven verbs golden
/// (tests/read_surface.rs holds the read verbs' byte-level record,
/// tests/sync_pipeline.rs the sync summary's). From here changes
/// are additive-only: a new field or a new always-present key is a golden
/// regeneration; renaming, removing, retyping, or re-meaning anything bumps
/// this and is a design event, not an edit.
pub const CONTRACT_VERSION: u64 = 1;

/// A stream is stale when unchecked for longer than this (24h), or never
/// checked. Advisory only — reads never fail stale.
const STALE_AFTER_SECS: i64 = 86_400;

pub fn attention(cfg: &Config, limit: Option<usize>) -> Result<Value> {
    let archive = open(cfg)?;
    let conn = read_snapshot(&archive)?;
    let conn = &*conn;
    let viewer = cfg.viewer.as_str();

    // The maintainer-bucket gate: repos the LOADED config puts at project
    // scope. Config, never the archive's stored fingerprint — archive
    // contents never create a bucket (DESIGN.md; attention.rs module docs
    // carry the absent-vs-empty output argument). Repo names are folded to
    // lowercase on both sides of this lookup (RepoName at the config
    // boundary, ingest folding to match — config.rs).
    let project_repos: std::collections::BTreeSet<String> = cfg
        .repos
        .iter()
        .map(|e| e.resolved())
        .filter(|rc| rc.scope == crate::config::Scope::Project)
        .map(|rc| rc.repo.as_str().to_string())
        .collect();
    let maintainer_scope = !project_repos.is_empty();

    // Candidates: every open, not-upstream-deleted PR (the bucket scope —
    // attention.rs module docs own the argument). Iteration order (repo,
    // number) is a total order; the per-bucket recency sort below re-orders
    // deterministically on top of it.
    let mut cand_stmt = conn
        .prepare(
            "SELECT pk, repo, number, title, is_draft, author, author_assoc, \
                    review_decision, created_at, updated_at, url, truncated, verified_at, \
                    head_committed_at \
             FROM prs WHERE state = 'OPEN' AND deleted_at IS NULL ORDER BY repo, number",
        )
        .map_err(classify_ours)?;
    struct Cand {
        pk: i64,
        repo: String,
        number: i64,
        title: String,
        draft: bool,
        author: Option<String>,
        author_assoc: Option<String>,
        review_decision: Option<String>,
        created_at: String,
        updated_at: String,
        url: String,
        truncated: bool,
        verified_at: Option<String>,
        head_committed_at: Option<String>,
    }
    let cands: Vec<Cand> = cand_stmt
        .query_map([], |r| {
            Ok(Cand {
                pk: r.get(0)?,
                repo: r.get(1)?,
                number: r.get(2)?,
                title: r.get(3)?,
                draft: r.get(4)?,
                author: r.get(5)?,
                author_assoc: r.get(6)?,
                review_decision: r.get(7)?,
                created_at: r.get(8)?,
                updated_at: r.get(9)?,
                url: r.get(10)?,
                truncated: r.get(11)?,
                verified_at: r.get(12)?,
                head_committed_at: r.get(13)?,
            })
        })
        .map_err(classify_ours)?
        .collect::<std::result::Result<_, _>>()
        .map_err(classify_ours)?;

    // One prepared statement per signal, reused across candidates.
    let mut rq_stmt = conn
        .prepare("SELECT reviewer, kind FROM review_requests WHERE pr = ?1 ORDER BY kind, reviewer")
        .map_err(classify_ours)?;
    let mut rev_stmt = conn
        .prepare(
            "SELECT author, state, created_at FROM comments \
             WHERE parent_kind = 'pr' AND parent = ?1 AND kind = 'review' \
               AND deleted_at IS NULL \
             ORDER BY created_at, id",
        )
        .map_err(classify_ours)?;
    let mut flip_stmt = conn
        .prepare(
            "SELECT observed_at FROM observations \
             WHERE pr = ?1 AND field = 'head_sha' \
             ORDER BY observed_at DESC, seq DESC LIMIT 1",
        )
        .map_err(classify_ours)?;
    let mut thread_stmt = conn
        .prepare(
            "SELECT pk FROM review_threads \
             WHERE pr = ?1 AND deleted_at IS NULL AND is_resolved = 0 ORDER BY pk",
        )
        .map_err(classify_ours)?;
    let mut tc_stmt = conn
        .prepare(
            "SELECT author, is_minimized, deleted_at IS NOT NULL FROM comments \
             WHERE thread = ?1 ORDER BY created_at, id",
        )
        .map_err(classify_ours)?;
    // Substantive activity split by party. Minimized and deleted comments
    // are neither activity nor participation (attention.rs); logins compare
    // by login_eq semantics — ASCII logins make COLLATE NOCASE the same
    // equivalence (identity.rs, the prs --author precedent). MAX over
    // RFC 3339 "Z" ingest text; attention.rs re-parses before judging.
    let mut viewer_last_stmt = conn
        .prepare(
            "SELECT MAX(created_at) FROM comments \
             WHERE parent_kind = 'pr' AND parent = ?1 AND deleted_at IS NULL \
               AND is_minimized = 0 AND author = ?2 COLLATE NOCASE",
        )
        .map_err(classify_ours)?;
    // An APPROVED review verdict is not a reply (attention.rs module docs:
    // counting it would starve ready_to_merge behind they_replied). `IS`,
    // not `=`: a review row with a NULL state must COUNT as activity —
    // under `=`, three-valued logic makes the NOT arm NULL and drops the
    // row, silently suppressing a demand (fail-closed, the wrong
    // polarity). Unreachable from ghgraph's own writer, but a derivation
    // input is validated where it is consumed (attention.rs).
    let mut other_last_stmt = conn
        .prepare(
            "SELECT MAX(created_at) FROM comments \
             WHERE parent_kind = 'pr' AND parent = ?1 AND deleted_at IS NULL \
               AND is_minimized = 0 \
               AND (author IS NULL OR author <> ?2 COLLATE NOCASE) \
               AND NOT (kind = 'review' AND state IS 'APPROVED')",
        )
        .map_err(classify_ours)?;

    // (updated_at, repo, number, row) per bucket, sorted after collection.
    let mut buckets: Vec<Vec<(String, String, i64, Value)>> =
        attention::Bucket::ALL.iter().map(|_| Vec::new()).collect();

    for cand in &cands {
        let viewers_pr = cand.author.as_deref().is_some_and(|a| login_eq(a, viewer));
        let person_pr = cand
            .author
            .as_deref()
            .is_some_and(|a| cfg.people.iter().any(|p| login_eq(p.as_str(), a)));

        // Review requests addressing the viewer: user rows by login, team
        // rows by a declared config.teams name (config.rs owns why).
        let requests: Vec<(String, String)> = rq_stmt
            .query_map([cand.pk], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(classify_ours)?
            .collect::<std::result::Result<_, _>>()
            .map_err(classify_ours)?;
        let requested_via: Vec<&(String, String)> = requests
            .iter()
            .filter(|(reviewer, kind)| match kind.as_str() {
                "team" => cfg.teams.iter().any(|t| login_eq(t.as_str(), reviewer)),
                // 'user' — and, deliberately, any UNRECOGNIZED kind: sync
                // writes only 'user'/'team' (schema.sql), so an unknown
                // kind is shape drift, and one naming the viewer escalates
                // rather than silently dropping a request addressed to
                // them (uncertainty may add to waiting_on_me). One arm for
                // both, or the 'user' arm is a mutant-shaped duplicate.
                _ => login_eq(reviewer, viewer),
            })
            .collect();

        let reviews: Vec<(Option<String>, Option<String>, String)> = rev_stmt
            .query_map([cand.pk], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map_err(classify_ours)?
            .collect::<std::result::Result<_, _>>()
            .map_err(classify_ours)?;
        let viewer_reviewed = reviews
            .iter()
            .any(|(author, _, _)| author.as_deref().is_some_and(|a| login_eq(a, viewer)));
        let head_flip: Option<String> = flip_stmt
            .query_row([cand.pk], |r| r.get(0))
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(classify_ours(e)),
            })?;
        let bounds = PushBounds {
            head_committed_at: cand.head_committed_at.as_deref(),
            head_flip_observed_at: head_flip.as_deref(),
        };
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
        let effective = attention::effective_review_state(&signals, &bounds);

        let unresolved: Vec<i64> = thread_stmt
            .query_map([cand.pk], |r| r.get(0))
            .map_err(classify_ours)?
            .collect::<std::result::Result<_, _>>()
            .map_err(classify_ours)?;
        let mut threads_waiting = 0u64;
        for tpk in &unresolved {
            let tc: Vec<(Option<String>, bool, bool)> = tc_stmt
                .query_map([tpk], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .map_err(classify_ours)?
                .collect::<std::result::Result<_, _>>()
                .map_err(classify_ours)?;
            let tc: Vec<ThreadComment<'_>> = tc
                .iter()
                .map(|(author, minimized, deleted)| ThreadComment {
                    author: author.as_deref(),
                    is_minimized: *minimized,
                    deleted: *deleted,
                })
                .collect();
            if attention::waiting_on(viewer, cand.author.as_deref(), false, &tc)
                == Some(attention::WaitingOn::Me)
            {
                threads_waiting += 1;
            }
        }

        let viewer_spoke_at: Option<String> = viewer_last_stmt
            .query_row(rusqlite::params![cand.pk, viewer], |r| r.get(0))
            .map_err(classify_ours)?;
        // Authorship is participation, at the PR's created_at; both stamps
        // are canonical "Z" ingest text, so lexicographic max agrees with
        // time order (and a non-canonical stray fails open in attention.rs).
        let viewer_last = match (viewer_spoke_at, viewers_pr) {
            (Some(spoke), true) => Some(spoke.max(cand.created_at.clone())),
            (Some(spoke), false) => Some(spoke),
            (None, true) => Some(cand.created_at.clone()),
            (None, false) => None,
        };
        let other_last: Option<String> = other_last_stmt
            .query_row(rusqlite::params![cand.pk, viewer], |r| r.get(0))
            .map_err(classify_ours)?;

        let placed = attention::bucket(&attention::PrSignals {
            viewers_pr,
            person_pr,
            draft: cand.draft,
            truncated: cand.truncated,
            review_decision: cand.review_decision.as_deref(),
            requested_of_viewer: !requested_via.is_empty(),
            thread_demands_viewer: threads_waiting > 0,
            viewer_last_activity_at: viewer_last.as_deref(),
            last_other_activity_at: other_last.as_deref(),
            effective,
            has_unresolved_threads: !unresolved.is_empty(),
            viewer_reviewed,
            project_scope: project_repos.contains(&cand.repo),
            // Anyone asked / anyone reviewed — the raw row sets, before the
            // viewer-specific narrowings above (attention.rs owns why an
            // undeclared team or a COMMENTED review still counts here).
            has_review_requests: !requests.is_empty(),
            has_reviews: !reviews.is_empty(),
        });
        let Some(placed) = placed else { continue };

        let mut row = json!({
            "repo": cand.repo,
            "number": cand.number,
            "title": cand.title,
            "draft": cand.draft,
            "author": cand.author,
            "author_assoc": cand.author_assoc,
            "updated_at": cand.updated_at,
            "url": cand.url,
            "truncated": cand.truncated,
            "verified_at": cand.verified_at,
        });
        let obj = row.as_object_mut().expect("built as an object above");
        match placed {
            attention::Bucket::WaitingOnMe => {
                // The evidence for each arm of the bucket rule: which
                // requests address the viewer, and how many unresolved
                // threads on the viewer's own PR wait on them (the own-PR
                // restriction is the bucket's, so the disclosed count
                // matches what qualified the row; participated threads on
                // others' PRs surface via they_replied instead).
                obj.insert(
                    "requested_via".into(),
                    Value::Array(
                        requested_via
                            .iter()
                            .map(|(reviewer, kind)| json!({ "kind": kind, "reviewer": reviewer }))
                            .collect(),
                    ),
                );
                obj.insert(
                    "threads_waiting".into(),
                    json!(if viewers_pr { threads_waiting } else { 0 }),
                );
            }
            attention::Bucket::TheyReplied => {
                obj.insert("last_other_activity_at".into(), json!(other_last));
            }
            attention::Bucket::ReadyToMerge
            | attention::Bucket::PeoplePrs
            | attention::Bucket::NeedsReviewer => {}
            // bucket() never returns the issue bucket for a PR — pinned
            // over the whole signal cube (attention.rs oracle test).
            attention::Bucket::Untriaged => unreachable!("bucket() is PR-only"),
        }
        let idx = attention::Bucket::ALL
            .iter()
            .position(|b| *b == placed)
            .expect("ALL enumerates every bucket");
        buckets[idx].push((cand.updated_at.clone(), cand.repo.clone(), cand.number, row));
    }

    // The issue sweep — untriaged, the one issue-shaped bucket. Same
    // candidate scope as the PR loop (OPEN, not upstream-deleted), any
    // hydration_source: a fill-only linked row's NULL labels read as
    // unwitnessed and fail open (attention.rs owns that argument). Skipped
    // entirely when no repo is at project scope: the gate is per-row
    // anyway (project_scope in the signals), so the skip only saves the
    // scan — it can't change the outcome.
    if maintainer_scope {
        let mut issue_stmt = conn
            .prepare(
                "SELECT pk, repo, number, title, author, author_assoc, labels, assignees, \
                        updated_at, url, truncated, verified_at \
                 FROM issues WHERE state = 'OPEN' AND deleted_at IS NULL ORDER BY repo, number",
            )
            .map_err(classify_ours)?;
        // Association values of everyone who substantively spoke; the
        // maintainer judgment over them is attention.rs's
        // (is_maintainer_assoc), so the WHERE stays structural — deleted
        // and minimized rows are not speech, everything else is. No ORDER
        // BY: the set is consumed by any(), so row order cannot reach
        // output (held under reverse_unordered_selects like every read).
        let mut assoc_stmt = conn
            .prepare(
                "SELECT DISTINCT author_assoc FROM comments \
                 WHERE parent_kind = 'issue' AND parent = ?1 AND deleted_at IS NULL \
                   AND is_minimized = 0",
            )
            .map_err(classify_ours)?;
        struct IssueCand {
            pk: i64,
            repo: String,
            number: i64,
            title: String,
            author: Option<String>,
            author_assoc: Option<String>,
            labels: Option<String>,
            assignees: Option<String>,
            updated_at: String,
            url: String,
            truncated: bool,
            verified_at: Option<String>,
        }
        let issues: Vec<IssueCand> = issue_stmt
            .query_map([], |r| {
                Ok(IssueCand {
                    pk: r.get(0)?,
                    repo: r.get(1)?,
                    number: r.get(2)?,
                    title: r.get(3)?,
                    author: r.get(4)?,
                    author_assoc: r.get(5)?,
                    labels: r.get(6)?,
                    assignees: r.get(7)?,
                    updated_at: r.get(8)?,
                    url: r.get(9)?,
                    truncated: r.get(10)?,
                    verified_at: r.get(11)?,
                })
            })
            .map_err(classify_ours)?
            .collect::<std::result::Result<_, _>>()
            .map_err(classify_ours)?;
        let untriaged_idx = attention::Bucket::ALL
            .iter()
            .position(|b| *b == attention::Bucket::Untriaged)
            .expect("ALL enumerates every bucket");
        for issue in issues {
            let assocs: Vec<Option<String>> = assoc_stmt
                .query_map([issue.pk], |r| r.get(0))
                .map_err(classify_ours)?
                .collect::<std::result::Result<_, _>>()
                .map_err(classify_ours)?;
            let placed = attention::untriaged(&attention::IssueSignals {
                project_scope: project_repos.contains(&issue.repo),
                labeled: attention::json_array_nonempty(issue.labels.as_deref()),
                assigned: attention::json_array_nonempty(issue.assignees.as_deref()),
                maintainer_replied: assocs
                    .iter()
                    .any(|a| attention::is_maintainer_assoc(a.as_deref())),
            });
            if !placed {
                continue;
            }
            let row = json!({
                "repo": issue.repo,
                "number": issue.number,
                "title": issue.title,
                "author": issue.author,
                "author_assoc": issue.author_assoc,
                "updated_at": issue.updated_at,
                "url": issue.url,
                "truncated": issue.truncated,
                "verified_at": issue.verified_at,
            });
            buckets[untriaged_idx].push((issue.updated_at.clone(), issue.repo, issue.number, row));
        }
    }

    // Per-bucket recency order (updated_at DESC — search's argument for the
    // meaningful work-memory axis), tiebroken to total by (repo, number).
    // --limit caps rows per bucket; totals stay disclosed (limits govern
    // presentation, polarity governs derivation — attention.rs). The
    // maintainer buckets exist in the document only when some repo is at
    // project scope (attention.rs, Bucket::maintainer — absent, not empty:
    // an empty array claims "checked, nothing", and no maintainer sweep
    // was configured). untriaged rows are issues and serialize under an
    // "issues" key — calling them "prs" would be a quiet lie in the frozen
    // contract's vocabulary.
    let cap = limit.unwrap_or(usize::MAX);
    let out: Vec<Value> = attention::Bucket::ALL
        .iter()
        .zip(buckets)
        .filter(|(b, _)| maintainer_scope || !b.maintainer())
        .map(|(b, mut rows)| {
            rows.sort_by(|(ua, ra, na, _), (ub, rb, nb, _)| {
                ub.cmp(ua).then_with(|| ra.cmp(rb)).then_with(|| na.cmp(nb))
            });
            let total = rows.len();
            let items: Vec<Value> = rows.into_iter().take(cap).map(|(_, _, _, v)| v).collect();
            let key = if *b == attention::Bucket::Untriaged {
                "issues"
            } else {
                "prs"
            };
            json!({
                "bucket": b.as_str(),
                "total": total,
                "returned": items.len(),
                key: items,
            })
        })
        .collect();

    Ok(json!({ "_meta": meta(cfg, conn)?, "attention": out }))
}

/// Does an `attention` document carry any demand? Reads the DISCLOSED
/// per-bucket totals of the document that was (or will be) emitted, so the
/// gate and a consumer parsing stdout can never disagree. A document this
/// function cannot read counts as demanding (fail-open: a gate that cannot
/// prove "all clear" must not report it) — unreachable from [`attention`]'s
/// output by construction, pinned by test against drift.
pub fn attention_has_demands(doc: &Value) -> bool {
    match doc.get("attention").and_then(Value::as_array) {
        None => true,
        Some(buckets) => buckets
            .iter()
            .any(|b| b.get("total").and_then(Value::as_u64) != Some(0)),
    }
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
    let conn = read_snapshot(&archive)?;
    let conn = &*conn;

    // Filters and defaults in ONE where-clause, shared by the count and the
    // page, AND one snapshot around both (read_snapshot) — the predicate
    // makes the total and the rows agree in space, the transaction makes
    // them agree in time; either alone over-promises. The default hides
    // soft-deleted rows (an upstream-deleted PR is not open work); --all
    // shows everything, deleted_at disclosed. --author matches by login_eq
    // semantics: logins are ASCII, so COLLATE NOCASE (ASCII-only in stock
    // SQLite) is the same equivalence (identity.rs).
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
    let conn = read_snapshot(&archive)?;
    let conn = &*conn;

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
                    // A string enum, not a nullable boolean (C1 panel, S3):
                    // `_meta.streams[].stale` is a plain boolean, and a
                    // same-named tri-state field one verb over invites a
                    // consumer to treat null as falsy — parsing "unknown"
                    // as "fresh", the polarity inversion at their layer
                    // instead of ours. The explicit third value cannot be
                    // truth-tested by accident.
                    json!({
                        "reviewer": author,
                        "state": state,
                        "submitted_at": at,
                        "freshness": attention::review_freshness(at, &bounds).as_str(),
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
    let conn = read_snapshot(&archive)?;
    let conn = &*conn;

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
    let conn = read_snapshot(&archive)?;
    let conn = &*conn;
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
    let conn = read_snapshot(&archive)?;
    let conn = &*conn;

    // Audits (orphans, observation chain, FTS integrity, watermark
    // assertion) are PLANNED (milestone 5, hardening) — this is the count
    // surface they will land beside.
    //
    // The format! below interpolates TABLE NAMES — admissible only because
    // the names come from this literal array and nowhere else. Never add a
    // computed or user-supplied entry here; user-named tables go through
    // `query`, whose gates exist for exactly that.
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

/// One WAL snapshot for one whole document (C1 panel, S1). Outside a
/// transaction every statement acquires its own WAL read mark, so a
/// concurrent sync could commit between a verb's statements and the
/// document would disagree with itself — returned > total is the smallest
/// counterexample, a count-vs-page drift under an MCP background sync the
/// realistic one. Every verb therefore runs ALL its reads (counts, pages,
/// children, `_meta`) inside one deferred transaction: readers don't block
/// the writer under WAL, and the writer doesn't move this snapshot.
/// `unchecked_transaction` because RoArchive hands out `&Connection` —
/// rusqlite's `&mut` requirement guards writer misuse this connection
/// cannot express (read-only + query_only, db.rs). Dropped un-committed on
/// every path: a read transaction has nothing to commit.
fn read_snapshot(archive: &RoArchive) -> Result<rusqlite::Transaction<'_>> {
    archive
        .conn()
        .unchecked_transaction()
        .map_err(classify_ours)
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
                let stale = stale_at(age);
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
    // Bounded search, not a mutable countdown: boundary 0 always exists, so
    // find() is total by construction and the mutant class that breaks loop
    // progress (Makefile, the mutants target's OOM note) has nothing to
    // break.
    let end = (0..=max)
        .rev()
        .find(|&i| body.is_char_boundary(i))
        .expect("0 is always a char boundary");
    (body[..end].to_string(), true)
}

/// A stream is stale strictly beyond [`STALE_AFTER_SECS`], or when never
/// checked (None). Pure, so the boundary second is pinnable — `meta`'s ages
/// come from now(), which no test can hold still.
fn stale_at(age_seconds: Option<i64>) -> bool {
    age_seconds.is_none_or(|a| a > STALE_AFTER_SECS)
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
///
/// `pub` as a verification seam only (like gh::scrub_tokens): the input is
/// attacker-chosen (a cloned repo's .git/config), so the fuzz harness
/// hammers host pinning and totality from outside the crate. Not CLI
/// surface.
pub fn github_repo_from_remote_url(url: &str) -> Option<RepoName> {
    let path = url
        .strip_prefix("git@github.com:")
        .or_else(|| url.strip_prefix("ssh://git@github.com/"))
        .or_else(|| url.strip_prefix("https://github.com/"))?;
    let path = path.strip_suffix('/').unwrap_or(path);
    // The .git strip is case-INsensitive, and a name that still ends in
    // ".git" after it is rejected outright: github.com forbids repo names
    // ending in .git, so such a path names nothing real — and admitting it
    // would break round-trip identity (the fuzz counterexample that forced
    // this: "Owner/x…giT" folds to a .git-suffixed name that re-parses
    // differently). That naming rule is enforced HERE, at the URL boundary,
    // not in RepoName: the identity gate is the injection charset, not
    // github.com's full naming policy (identity.rs).
    let path = match path.len().checked_sub(4) {
        Some(cut) if path.is_char_boundary(cut) && path[cut..].eq_ignore_ascii_case(".git") => {
            &path[..cut]
        }
        _ => path,
    };
    let repo = RepoName::new(path).ok()?;
    if repo.as_str().ends_with(".git") {
        return None;
    }
    Some(repo)
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
/// the user can fix their statement, so USER_INPUT — except busy (the
/// environment's to fix) and a corrupt archive (the operator's; C1 panel,
/// S2). The corrupt arm matters because execution-time errors are a MIXED
/// stream: an FTS5 syntax error in the user's MATCH surfaces at step time —
/// i.e. inside the row-collect loop, exactly where a corruption error would
/// — so the site cannot classify by position, only by code. (The panel's
/// synthesis proposed flipping the collect site to classify_ours wholesale;
/// that would relabel the user's own FTS typo "file a ghgraph bug" and
/// break the pinned search_syntax_error_is_user_input — the per-code arm
/// here is the fix that survives both tests.) The sqlite message names the
/// syntax problem without this code ever interpolating archive content.
fn classify_user_query(e: rusqlite::Error) -> Error {
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
            "git@github.com:Owner/Repo.GIT", // the suffix strips per case-fold
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
            // github.com forbids repo names ending in .git; admitting one
            // breaks round-trip identity (the fuzz counterexample — see the
            // parser). The .giT form is the exact crash input's shape.
            "git@github.com:owner/repo.git.git",
            "git@github.com:owner/repo.giT.git",
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

    /// The mechanism behind read_snapshot (C1 panel, S1): inside one
    /// deferred transaction on the read connection, a concurrent writer's
    /// committed rows stay invisible — so a verb's count and page cannot
    /// disagree in time. Two real connections on one WAL archive; the
    /// interleaving is explicit where an integration test could only race.
    #[test]
    fn read_snapshot_pins_one_wal_view_across_statements() {
        let dir = std::env::temp_dir().join(format!("ghgraph-snap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("a/ghgraph.db");
        let rw = db::open_rw(&path).unwrap();
        rw.conn()
            .execute_batch(
                "INSERT INTO prs (id, repo, number, title, state, created_at, updated_at, url) \
                 VALUES ('n1', 'o/r', 1, 't', 'OPEN', '2026-01-01T00:00:00Z', \
                         '2026-01-01T00:00:00Z', 'u1')",
            )
            .unwrap();

        let ro = db::open_ro(&path).unwrap();
        let tx = read_snapshot(&ro).unwrap();
        let count = |c: &Connection| -> i64 {
            c.query_row("SELECT COUNT(*) FROM prs", [], |r| r.get(0))
                .unwrap()
        };
        // First read materializes the snapshot; the writer then commits.
        assert_eq!(count(&tx), 1);
        rw.conn()
            .execute_batch(
                "INSERT INTO prs (id, repo, number, title, state, created_at, updated_at, url) \
                 VALUES ('n2', 'o/r', 2, 't', 'OPEN', '2026-01-01T00:00:00Z', \
                         '2026-01-01T00:00:00Z', 'u2')",
            )
            .unwrap();
        assert_eq!(count(&tx), 1, "the snapshot must not move mid-document");
        drop(tx);
        // A NEW snapshot (the next verb invocation) sees the commit.
        let tx = read_snapshot(&ro).unwrap();
        assert_eq!(count(&tx), 2);
        drop(tx);
        drop(ro);
        drop(rw);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The mixed execution-time stream behind classify_user_query (C1
    /// panel, S2): one arm per actor, pinned like sync.rs's classifier.
    #[test]
    fn classify_user_query_names_the_actor_per_arm() {
        use crate::error::Code;
        let sqlite = |code: std::os::raw::c_int| {
            rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None)
        };
        assert_eq!(
            classify_user_query(sqlite(rusqlite::ffi::SQLITE_BUSY)).code,
            Code::Transient
        );
        let corrupt = classify_user_query(sqlite(rusqlite::ffi::SQLITE_CORRUPT));
        assert_eq!(corrupt.code, Code::Configuration);
        assert!(corrupt.message.contains("resync"), "{}", corrupt.message);
        assert_eq!(
            classify_user_query(sqlite(rusqlite::ffi::SQLITE_NOTADB)).code,
            Code::Configuration
        );
        // The default arm is the user's own statement — an FTS5 syntax
        // error surfaces as a plain SqliteFailure at step time.
        assert_eq!(
            classify_user_query(sqlite(rusqlite::ffi::SQLITE_ERROR)).code,
            Code::UserInput
        );
    }

    /// The --fail-if-any gate reads the disclosed totals; anything it
    /// cannot read is a demand (fail-open). The unreadable arms are drift
    /// pins: reachable only if attention()'s output shape moves without
    /// this predicate moving.
    #[test]
    fn fail_if_any_reads_the_disclosed_totals() {
        // The six-bucket project-scope shape, untriaged under its "issues"
        // key: the gate reads totals and must be bucket-name- and
        // rows-key-agnostic — a maintainer demand gates exactly like an
        // operator demand.
        let doc = |totals: [u64; 6]| {
            json!({"attention": [
                {"bucket": "waiting_on_me", "total": totals[0], "returned": 0, "prs": []},
                {"bucket": "they_replied", "total": totals[1], "returned": 0, "prs": []},
                {"bucket": "ready_to_merge", "total": totals[2], "returned": 0, "prs": []},
                {"bucket": "people_prs", "total": totals[3], "returned": 0, "prs": []},
                {"bucket": "needs_reviewer", "total": totals[4], "returned": 0, "prs": []},
                {"bucket": "untriaged", "total": totals[5], "returned": 0, "issues": []},
            ]})
        };
        assert!(!attention_has_demands(&doc([0, 0, 0, 0, 0, 0])));
        for i in 0..6 {
            let mut t = [0u64; 6];
            t[i] = 1;
            assert!(attention_has_demands(&doc(t)), "bucket {i} must trip");
        }
        assert!(attention_has_demands(&json!({})), "unreadable fails open");
        assert!(
            attention_has_demands(&json!({"attention": [{"bucket": "waiting_on_me"}]})),
            "a missing total is never all-clear"
        );
    }

    #[test]
    fn stale_boundary_is_exact() {
        // Strictly beyond 24h — checked at 86400s even is NOT stale (the
        // discriminating second for the > vs >= mutant); never checked is.
        assert!(!stale_at(Some(0)));
        assert!(!stale_at(Some(STALE_AFTER_SECS)));
        assert!(stale_at(Some(STALE_AFTER_SECS + 1)));
        assert!(stale_at(None));
    }

    #[test]
    fn limit_to_sql_boundaries() {
        assert_eq!(limit_to_sql(None), -1);
        assert_eq!(limit_to_sql(Some(0)), 0);
        assert_eq!(limit_to_sql(Some(100)), 100);
        assert_eq!(limit_to_sql(Some(usize::MAX)), i64::MAX);
    }
}
