//! The read-surface suite (milestone 3): golden files for prs / pr /
//! search / query / stats (C1) and attention (C2), held under PRAGMA
//! reverse_unordered_selects=ON
//! (via db.rs's GHGRAPH_TEST_REVERSE_SELECTS hook, so the reversal applies
//! to the very connection the verbs use), plus the error-classification
//! table for the read path.
//!
//! The archive is SEEDED directly through the library (db::open_rw + bound
//! SQL) rather than replayed through sync fixtures: the read contract wants
//! states the write path makes deliberately hard to reach (truncated rows,
//! minimized and deleted comments, dangling refs, a stale approval next to
//! a fresh one), and the write path has its own load-bearing suite
//! (tests/sync_pipeline.rs). What this trades away: drift between this
//! seed and what sync actually writes would go unnoticed here — the seam
//! is the schema, which both sides compile against.
//!
//! Timing fields are masked before comparison — `generated_at` and
//! `age_seconds`, the contract's ENUMERATED nondeterminism list (report.rs)
//! and nothing else. Every other byte must hold, or the golden fails.
//! Seed timestamps are fixed in the past, so `stale` is deterministically
//! true; the stale:false arm is asserted separately with a now() stamp.
//!
//! Goldens regenerate with GHGRAPH_UPDATE_GOLDENS=1 — review the diff like
//! code; a golden change IS a contract change.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::{Value, json};

use ghgraph::db;

struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new() -> Scratch {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ghgraph-read-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Scratch { dir }
    }

    fn db_path(&self) -> PathBuf {
        self.dir.join("archive/ghgraph.db")
    }

    fn config_path(&self) -> PathBuf {
        self.dir.join("config.json")
    }

    fn write_config(&self, body: &Value) {
        let mut v = body.clone();
        v["db_path"] = json!(self.db_path().to_str().unwrap());
        std::fs::write(self.config_path(), v.to_string()).unwrap();
    }

    /// Run a read verb; cwd is the scratch dir (NOT this checkout), so the
    /// `pr` verb's git-remote fallback can never see a real remote and the
    /// suite stays hermetic.
    fn run(&self, args: &[&str]) -> (i32, Option<Value>, String) {
        let out = Command::new(env!("CARGO_BIN_EXE_ghgraph"))
            .arg("--config")
            .arg(self.config_path())
            .args(args)
            .current_dir(&self.dir)
            .env("GHGRAPH_TEST_REVERSE_SELECTS", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("spawn ghgraph");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let doc = serde_json::from_str(&stdout).ok();
        (
            out.status.code().unwrap_or(-1),
            doc,
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    }

    fn run_ok(&self, args: &[&str]) -> Value {
        let (code, doc, stderr) = self.run(args);
        assert_eq!(code, 0, "{args:?} must exit 0; stderr:\n{stderr}");
        doc.expect("one JSON document on stdout")
    }

    fn run_err(&self, args: &[&str]) -> Value {
        let (code, doc, stderr) = self.run(args);
        assert_eq!(code, 2, "{args:?} must exit 2; stderr:\n{stderr}");
        doc.expect("a typed error envelope on stdout")
    }
}

/// The standard config the goldens assume: `me` at the keyboard, alpha at
/// working scope (synced, fingerprint matching), beta at project scope
/// (synced under an OLD fingerprint → config_pending).
fn standard_config(s: &Scratch) {
    s.write_config(&json!({
        "viewer": "me",
        "repos": ["octo/alpha", {"repo": "octo/beta", "scope": "project"}],
    }));
}

/// The attention goldens' config: alice is a tracked person and the team
/// list is the test's variable. `people` is a DISCOVERY input, so these
/// goldens honestly show config_pending: true — the state after editing the
/// config and before the next sync; people_prs derives from the CURRENT
/// config regardless of what ingested the row (DESIGN.md). `teams` is
/// read-side only and moves no fingerprint.
fn attention_config(s: &Scratch, teams: &[&str]) {
    s.write_config(&json!({
        "viewer": "me",
        "repos": ["octo/alpha", {"repo": "octo/beta", "scope": "project"}],
        "people": ["alice"],
        "teams": teams,
    }));
}

/// Seed the archive every golden reads. One deliberate state per contract
/// clause — the comments beside each block name the clause it exists for.
fn seed(s: &Scratch) {
    let arch = db::open_rw(&s.db_path()).unwrap();
    let c = arch.conn();
    let exec = |sql: &str, params: &[&dyn rusqlite::ToSql]| {
        c.execute(sql, params).unwrap();
    };

    // sync_state: alpha matches the loaded config (config_pending false);
    // beta was synced at WORKING scope before its config flipped to project
    // (config_pending true, stored fingerprint disclosed as-was); gone/old
    // is in the archive but no longer configured (still disclosed).
    let alpha_fp = r#"{"bots":true,"exclude_authors":[],"lookback_days":90,"people":[],"scope":"working","viewer":"me"}"#;
    let beta_old_fp = r#"{"bots":true,"exclude_authors":[],"lookback_days":90,"people":[],"scope":"working","viewer":"me"}"#;
    exec(
        "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                 runs_since_advance, fingerprint) \
         VALUES ('octo/alpha', 'pr', '2026-01-05T00:00:00Z', '2026-01-05T00:10:00Z', 0, ?1)",
        &[&alpha_fp],
    );
    exec(
        "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                 runs_since_advance, fingerprint) \
         VALUES ('octo/beta', 'pr', '2026-01-04T00:00:00Z', '2026-01-04T00:10:00Z', 2, ?1)",
        &[&beta_old_fp],
    );
    exec(
        "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                 runs_since_advance, fingerprint) \
         VALUES ('octo/gone', 'pr', '2026-01-03T00:00:00Z', '2026-01-03T00:10:00Z', 5, ?1)",
        &[&alpha_fp],
    );

    let insert_pr = "INSERT INTO prs (pk, id, repo, number, title, body, state, is_draft, \
            author, author_id, author_assoc, head_ref, base_ref, head_sha, review_decision, \
            created_at, updated_at, merged_at, closed_at, url, truncated, verified_at, \
            deleted_at, head_committed_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, \
                 ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24)";

    // #1 — the rich PR: stale approval next to a fresh one (effective
    // stale_approval), threads in every waiting_on state, hostile bodies,
    // refs both resolved and dangling. Multibyte body for the elision
    // golden. Note the body carries an FTS hit for "retry" too.
    exec(
        insert_pr,
        &[
            &1i64,
            &"PR_a1",
            &"octo/alpha",
            &1i64,
            &"Harden the retry loop",
            &"héllo 🦀 — retry budgets; see also 'DROP TABLE prs' as text",
            &"OPEN",
            &false,
            &"alice",
            &1001i64,
            &"CONTRIBUTOR",
            &"alice/retry",
            &"main",
            &"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            &"REVIEW_REQUIRED",
            &"2026-01-01T00:00:00Z",
            &"2026-01-05T00:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/1",
            &false,
            &"2026-01-05T00:00:00Z",
            &None::<String>,
            &"2026-01-01T12:00:00Z",
        ],
    );
    // The head_sha flip observation: the fresh-side bound (attention.rs).
    exec(
        "INSERT INTO observations (pr, observed_at, field, old, new) \
         VALUES (1, '2026-01-02T00:00:00Z', 'head_sha', 'old', \
                 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
        &[],
    );
    // bob approved AFTER flip+margin (fresh, stale:false); carol approved
    // BEFORE committedDate (provably stale, stale:true) — one stale
    // approval degrades the PR to stale_approval ("none stale" is the
    // Approved contract).
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, state, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('RV_a1_bob', 'pr', 1, 'review', 'APPROVED', 'bob', 'MEMBER', '', \
                 '2026-01-02T01:00:00Z', 'https://github.com/octo/alpha/pull/1#r1')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, state, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('RV_a1_carol', 'pr', 1, 'review', 'APPROVED', 'carol', 'MEMBER', '', \
                 '2026-01-01T06:00:00Z', 'https://github.com/octo/alpha/pull/1#r2')",
        &[],
    );
    exec(
        "INSERT INTO review_requests (pr, reviewer, kind) VALUES (1, 'dave', 'user')",
        &[],
    );
    exec(
        "INSERT INTO review_requests (pr, reviewer, kind) VALUES (1, 'platform', 'team')",
        &[],
    );
    // Thread 1 (unresolved): viewer spoke, alice answered last → waiting_on
    // "me". Thread 2 (resolved) → null. Thread 3 (unresolved): viewer spoke
    // last; the minimized latecomer must NOT flip it back → "them".
    exec(
        "INSERT INTO review_threads (pk, id, pr, path, line, is_resolved, is_outdated) \
         VALUES (11, 'TH_a1_1', 1, 'src/a.rs', 5, 0, 0)",
        &[],
    );
    exec(
        "INSERT INTO review_threads (pk, id, pr, path, line, is_resolved, is_outdated) \
         VALUES (12, 'TH_a1_2', 1, 'src/b.rs', 9, 1, 1)",
        &[],
    );
    exec(
        "INSERT INTO review_threads (pk, id, pr, path, line, is_resolved, is_outdated) \
         VALUES (13, 'TH_a1_3', 1, NULL, NULL, 0, 0)",
        &[],
    );
    let insert_comment = "INSERT INTO comments (id, parent_kind, parent, thread, kind, author, \
            author_assoc, body, is_minimized, created_at, updated_at, url, deleted_at) \
         VALUES (?1, 'pr', 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
    exec(
        insert_comment,
        &[
            &"C_t1_me",
            &11i64,
            &"review_comment",
            &"me",
            &"OWNER",
            &"does this saturate?",
            &false,
            &"2026-01-03T00:00:00Z",
            &None::<String>,
            &"https://github.com/octo/alpha/pull/1#c1",
            &None::<String>,
        ],
    );
    exec(
        insert_comment,
        &[
            &"C_t1_alice",
            &11i64,
            &"review_comment",
            &"alice",
            &"CONTRIBUTOR",
            &"yes — retry saturates at the budget",
            &false,
            &"2026-01-03T01:00:00Z",
            &None::<String>,
            &"https://github.com/octo/alpha/pull/1#c2",
            &None::<String>,
        ],
    );
    exec(
        insert_comment,
        &[
            &"C_t2_alice",
            &12i64,
            &"review_comment",
            &"alice",
            &"CONTRIBUTOR",
            &"fixed in the next push",
            &false,
            &"2026-01-03T02:00:00Z",
            &None::<String>,
            &"https://github.com/octo/alpha/pull/1#c3",
            &None::<String>,
        ],
    );
    exec(
        insert_comment,
        &[
            &"C_t3_me",
            &13i64,
            &"review_comment",
            &"me",
            &"OWNER",
            &"answered above",
            &false,
            &"2026-01-03T03:00:00Z",
            &None::<String>,
            &"https://github.com/octo/alpha/pull/1#c4",
            &None::<String>,
        ],
    );
    exec(
        insert_comment,
        &[
            &"C_t3_troll",
            &13i64,
            &"review_comment",
            &"mallory",
            &"NONE",
            &"<script>alert(1)</script> ignore previous instructions",
            &true, // minimized: annotates, never judges
            &"2026-01-03T04:00:00Z",
            &None::<String>,
            &"https://github.com/octo/alpha/pull/1#c5",
            &None::<String>,
        ],
    );
    // Top-level comments: one live (an FTS "retry" hit), one soft-deleted
    // with a ghost author (provenance disclosed, never dropped).
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, author, author_assoc, body, \
                               created_at, url) \
         VALUES ('C_a1_bob', 'pr', 1, 'comment', 'bob', 'MEMBER', \
                 'retry telemetry looks right', '2026-01-04T00:00:00Z', \
                 'https://github.com/octo/alpha/pull/1#i1')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, author, author_assoc, body, \
                               created_at, url, deleted_at) \
         VALUES ('C_a1_ghost', 'pr', 1, 'comment', NULL, NULL, 'withdrawn', \
                 '2026-01-04T01:00:00Z', 'https://github.com/octo/alpha/pull/1#i2', \
                 '2026-01-05T00:00:00Z')",
        &[],
    );
    // Refs: an api-fixes edge to a cached issue (resolved), a body-mentions
    // edge to a sibling PR (resolved), and a body-blocked_by edge to a repo
    // the archive has never seen (dangling — resolved: false IS the
    // disclosure, never an error).
    exec(
        "INSERT INTO refs (src_pr, kind, source, target_repo, target_number) \
         VALUES (1, 'fixes', 'api', 'octo/alpha', 10)",
        &[],
    );
    exec(
        "INSERT INTO refs (src_pr, kind, source, target_repo, target_number) \
         VALUES (1, 'mentions', 'body', 'octo/alpha', 2)",
        &[],
    );
    exec(
        "INSERT INTO refs (src_pr, kind, source, target_repo, target_number) \
         VALUES (1, 'blocked_by', 'body', 'octo/zeta', 99)",
        &[],
    );
    exec(
        "INSERT INTO refs (src_pr, kind, source, target_repo, target_number) \
         VALUES (1, 'fixes', 'body', 'octo/gone', 7)",
        &[],
    );
    // The linked-issue cache row behind the api-fixes edge.
    exec(
        "INSERT INTO issues (pk, id, repo, number, title, state, author, url, updated_at, \
                             hydration_source, synced_at) \
         VALUES (100, 'IS_a10', 'octo/alpha', 10, 'Retries hammer the API', 'OPEN', 'alice', \
                 'https://github.com/octo/alpha/issues/10', '2026-01-02T00:00:00Z', \
                 'linked', '2026-01-05T00:00:00Z')",
        &[],
    );

    // #2 — viewer's PR, effectively approved (fresh approval, no threads):
    // an FTS title hit for "retry".
    exec(
        insert_pr,
        &[
            &2i64,
            &"PR_a2",
            &"octo/alpha",
            &2i64,
            &"Retry the sync watchdog",
            &"",
            &"OPEN",
            &false,
            &"me",
            &1000i64,
            &"OWNER",
            &"me/watchdog",
            &"main",
            &"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            &"APPROVED",
            &"2026-01-02T00:00:00Z",
            &"2026-01-06T00:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/2",
            &false,
            &"2026-01-06T00:00:00Z",
            &None::<String>,
            &"2026-01-02T12:00:00Z",
        ],
    );
    exec(
        "INSERT INTO observations (pr, observed_at, field, old, new) \
         VALUES (2, '2026-01-03T00:00:00Z', 'head_sha', 'old', \
                 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, state, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('RV_a2_bob', 'pr', 2, 'review', 'APPROVED', 'bob', 'MEMBER', '', \
                 '2026-01-03T01:00:00Z', 'https://github.com/octo/alpha/pull/2#r1')",
        &[],
    );

    // #3 — a draft with standing changes_requested.
    exec(
        insert_pr,
        &[
            &3i64,
            &"PR_a3",
            &"octo/alpha",
            &3i64,
            &"Sketch the issue stream",
            &"",
            &"OPEN",
            &true,
            &"bob",
            &1002i64,
            &"MEMBER",
            &"bob/issues",
            &"main",
            &"cccccccccccccccccccccccccccccccccccccccc",
            &None::<String>,
            &"2026-01-03T00:00:00Z",
            &"2026-01-04T00:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/3",
            &false,
            &"2026-01-04T00:00:00Z",
            &None::<String>,
            &"2026-01-03T12:00:00Z",
        ],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, state, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('RV_a3_carol', 'pr', 3, 'review', 'CHANGES_REQUESTED', 'carol', 'MEMBER', \
                 '', '2026-01-03T06:00:00Z', 'https://github.com/octo/alpha/pull/3#r1')",
        &[],
    );

    // #4 — merged (hidden by default, shown under --all).
    exec(
        insert_pr,
        &[
            &4i64,
            &"PR_a4",
            &"octo/alpha",
            &4i64,
            &"Land the schema",
            &"",
            &"MERGED",
            &false,
            &"me",
            &1000i64,
            &"OWNER",
            &"me/schema",
            &"main",
            &"dddddddddddddddddddddddddddddddddddddddd",
            &None::<String>,
            &"2025-12-20T00:00:00Z",
            &"2025-12-28T00:00:00Z",
            &"2025-12-28T00:00:00Z",
            &"2025-12-28T00:00:00Z",
            &"https://github.com/octo/alpha/pull/4",
            &false,
            &"2026-01-01T00:00:00Z",
            &None::<String>,
            &"2025-12-27T00:00:00Z",
        ],
    );

    // #5 — upstream-deleted (soft): hidden by default even though OPEN,
    // disclosed under --all via deleted_at.
    exec(
        insert_pr,
        &[
            &5i64,
            &"PR_a5",
            &"octo/alpha",
            &5i64,
            &"Deleted upstream",
            &"",
            &"OPEN",
            &false,
            &"mallory",
            &None::<i64>,
            &"NONE",
            &None::<String>,
            &None::<String>,
            &None::<String>,
            &None::<String>,
            &"2026-01-01T00:00:00Z",
            &"2026-01-02T00:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/5",
            &false,
            &None::<String>,
            &"2026-01-04T00:00:00Z",
            &None::<String>,
        ],
    );

    // #6 — truncated hydration in octo/beta (per-PR incompleteness rides
    // every emitted row: truncated true, verified_at null).
    exec(
        insert_pr,
        &[
            &6i64,
            &"PR_b1",
            &"octo/beta",
            &1i64,
            &"Big migration",
            &"",
            &"OPEN",
            &false,
            &"erin",
            &1005i64,
            &"COLLABORATOR",
            &"erin/mig",
            &"main",
            &"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            &None::<String>,
            &"2026-01-01T00:00:00Z",
            &"2026-01-03T00:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/beta/pull/1",
            &true,
            &None::<String>,
            &None::<String>,
            &"2026-01-01T00:00:00Z",
        ],
    );

    // A project-scope issue in beta: the third search group.
    exec(
        "INSERT INTO issues (pk, id, repo, number, title, state, body, author, author_assoc, \
                             labels, assignees, url, created_at, updated_at, hydration_source, \
                             synced_at) \
         VALUES (101, 'IS_b3', 'octo/beta', 3, 'Flaky retry logic in CI', 'OPEN', \
                 'retries flake under load', 'erin', 'COLLABORATOR', '[\"bug\"]', '[]', \
                 'https://github.com/octo/beta/issues/3', '2026-01-02T00:00:00Z', \
                 '2026-01-04T12:00:00Z', 'stream', '2026-01-04T12:30:00Z')",
        &[],
    );

    // ---- untriaged fixtures (milestone 4 / D2): the issue bucket's wiring
    // pins. beta#3 above is labeled → absent (the labels-JSON wire); the
    // alpha linked-cache issue (pk 100) is at WORKING scope → absent (the
    // per-row config gate). Each row here discriminates one signal wire.

    // beta#4 — bare: no labels, no assignees, no comments → untriaged.
    exec(
        "INSERT INTO issues (pk, id, repo, number, title, state, body, author, author_assoc, \
                             labels, assignees, url, created_at, updated_at, hydration_source, \
                             synced_at) \
         VALUES (102, 'IS_b4', 'octo/beta', 4, 'Crash on empty config', 'OPEN', '', 'frank', \
                 'NONE', NULL, NULL, 'https://github.com/octo/beta/issues/4', \
                 '2026-01-03T00:00:00Z', '2026-01-05T06:00:00Z', 'stream', \
                 '2026-01-05T06:30:00Z')",
        &[],
    );
    // beta#5 — a COLLABORATOR replied → cleared by the maintainer-reply
    // wire alone (no labels, no assignees).
    exec(
        "INSERT INTO issues (pk, id, repo, number, title, state, body, author, author_assoc, \
                             labels, assignees, url, created_at, updated_at, hydration_source, \
                             synced_at) \
         VALUES (103, 'IS_b5', 'octo/beta', 5, 'Docs typo in the install guide', 'OPEN', '', \
                 'grace', 'NONE', NULL, NULL, 'https://github.com/octo/beta/issues/5', \
                 '2026-01-03T00:00:00Z', '2026-01-04T18:00:00Z', 'stream', \
                 '2026-01-05T06:30:00Z')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, author, author_assoc, body, \
                               created_at, url) \
         VALUES ('C_b5_erin', 'issue', 103, 'comment', 'erin', 'COLLABORATOR', \
                 'taking a look', '2026-01-04T18:00:00Z', \
                 'https://github.com/octo/beta/issues/5#c1')",
        &[],
    );
    // beta#6 — the only maintainer comment is MINIMIZED: not speech, so
    // the demand stands (untriaged; the structural filter's pin).
    exec(
        "INSERT INTO issues (pk, id, repo, number, title, state, body, author, author_assoc, \
                             labels, assignees, url, created_at, updated_at, hydration_source, \
                             synced_at) \
         VALUES (104, 'IS_b6', 'octo/beta', 6, 'Wrong exit code on EPIPE', 'OPEN', '', \
                 'mallory', 'NONE', NULL, NULL, 'https://github.com/octo/beta/issues/6', \
                 '2026-01-04T00:00:00Z', '2026-01-05T09:00:00Z', 'stream', \
                 '2026-01-05T09:30:00Z')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, author, author_assoc, body, \
                               is_minimized, created_at, url) \
         VALUES ('C_b6_erin', 'issue', 104, 'comment', 'erin', 'COLLABORATOR', \
                 'duplicate spam', 1, '2026-01-05T00:00:00Z', \
                 'https://github.com/octo/beta/issues/6#c1')",
        &[],
    );
    // beta#7 — assigned (no labels, no reply) → cleared by the assignees
    // wire alone.
    exec(
        "INSERT INTO issues (pk, id, repo, number, title, state, body, author, author_assoc, \
                             labels, assignees, url, created_at, updated_at, hydration_source, \
                             synced_at) \
         VALUES (105, 'IS_b7', 'octo/beta', 7, 'Tune the backoff curve', 'OPEN', '', 'frank', \
                 'NONE', NULL, '[\"erin\"]', 'https://github.com/octo/beta/issues/7', \
                 '2026-01-04T00:00:00Z', '2026-01-04T20:00:00Z', 'stream', \
                 '2026-01-05T06:30:00Z')",
        &[],
    );

    // One quarantined hydration, for stats.
    exec(
        "INSERT INTO quarantine (id, repo, attempts, next_retry_at, error_class) \
         VALUES ('PR_bad', 'octo/beta', 2, '2026-01-05T00:00:00Z', 'transient')",
        &[],
    );

    // ---- attention fixtures (milestone 3 / C2): one PR per bucket arm ----

    // #7 — alice's PR with a user review request stored as 'Me' (API case;
    // the match is login_eq) → waiting_on_me, request arm. The 'security'
    // team request must NOT surface: no config declares that team.
    exec(
        insert_pr,
        &[
            &7i64,
            &"PR_a7",
            &"octo/alpha",
            &7i64,
            &"Wire the config loader",
            &"",
            &"OPEN",
            &false,
            &"alice",
            &1001i64,
            &"CONTRIBUTOR",
            &"alice/loader",
            &"main",
            &"ffffffffffffffffffffffffffffffffffffffff",
            &"REVIEW_REQUIRED",
            &"2026-01-02T00:00:00Z",
            &"2026-01-05T12:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/7",
            &false,
            &"2026-01-05T12:00:00Z",
            &None::<String>,
            &"2026-01-02T12:00:00Z",
        ],
    );
    exec(
        "INSERT INTO review_requests (pr, reviewer, kind) VALUES (7, 'Me', 'user')",
        &[],
    );
    exec(
        "INSERT INTO review_requests (pr, reviewer, kind) VALUES (7, 'security', 'team')",
        &[],
    );

    // #8 — alice's bare PR, no viewer involvement → people_prs when alice
    // is a configured person.
    exec(
        insert_pr,
        &[
            &8i64,
            &"PR_a8",
            &"octo/alpha",
            &8i64,
            &"Document the archive layout",
            &"",
            &"OPEN",
            &false,
            &"alice",
            &1001i64,
            &"CONTRIBUTOR",
            &"alice/docs",
            &"main",
            &"1111111111111111111111111111111111111111",
            &None::<String>,
            &"2026-01-03T00:00:00Z",
            &"2026-01-04T12:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/8",
            &false,
            &"2026-01-04T12:00:00Z",
            &None::<String>,
            &"2026-01-03T12:00:00Z",
        ],
    );

    // #9 — the viewer's PR with an unresolved thread where alice spoke
    // last → waiting_on_me, thread arm (threads_waiting 1).
    exec(
        insert_pr,
        &[
            &9i64,
            &"PR_a9",
            &"octo/alpha",
            &9i64,
            &"Split the report module",
            &"",
            &"OPEN",
            &false,
            &"me",
            &1000i64,
            &"OWNER",
            &"me/split",
            &"main",
            &"2222222222222222222222222222222222222222",
            &None::<String>,
            &"2026-01-03T00:00:00Z",
            &"2026-01-06T12:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/9",
            &false,
            &"2026-01-06T12:00:00Z",
            &None::<String>,
            &"2026-01-03T12:00:00Z",
        ],
    );
    exec(
        "INSERT INTO review_threads (pk, id, pr, path, line, is_resolved, is_outdated) \
         VALUES (91, 'TH_a9_1', 9, 'src/r.rs', 3, 0, 0)",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, thread, kind, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('C_t91_me', 'pr', 9, 91, 'review_comment', 'me', 'OWNER', \
                 'should this live in the library crate?', '2026-01-04T00:00:00Z', \
                 'https://github.com/octo/alpha/pull/9#c1')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, thread, kind, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('C_t91_alice', 'pr', 9, 91, 'review_comment', 'alice', 'CONTRIBUTOR', \
                 'either way — your call', '2026-01-04T01:00:00Z', \
                 'https://github.com/octo/alpha/pull/9#c2')",
        &[],
    );

    // #10 — the viewer's PR, freshly approved (decision agrees), but
    // TRUNCATED: ready_to_merge is fail-closed, so this row appears in no
    // bucket at all — its absence from the attention goldens is the pin.
    // The approval is excluded from they_replied by the verdict rule
    // (attention.rs: an approval is not a reply).
    exec(
        insert_pr,
        &[
            &10i64,
            &"PR_a10",
            &"octo/alpha",
            &10i64,
            &"Gate the fuzz harness",
            &"",
            &"OPEN",
            &false,
            &"me",
            &1000i64,
            &"OWNER",
            &"me/gate",
            &"main",
            &"3333333333333333333333333333333333333333",
            &"APPROVED",
            &"2026-01-02T00:00:00Z",
            &"2026-01-06T18:00:00Z",
            &None::<String>,
            &None::<String>,
            &"https://github.com/octo/alpha/pull/10",
            &true,
            &None::<String>,
            &None::<String>,
            &"2026-01-02T12:00:00Z",
        ],
    );
    exec(
        "INSERT INTO observations (pr, observed_at, field, old, new) \
         VALUES (10, '2026-01-03T00:00:00Z', 'head_sha', 'old', \
                 '3333333333333333333333333333333333333333')",
        &[],
    );
    exec(
        "INSERT INTO comments (id, parent_kind, parent, kind, state, author, author_assoc, \
                               body, created_at, url) \
         VALUES ('RV_a10_bob', 'pr', 10, 'review', 'APPROVED', 'bob', 'MEMBER', '', \
                 '2026-01-03T01:00:00Z', 'https://github.com/octo/alpha/pull/10#r1')",
        &[],
    );
}

// ---------------------------------------------------------------------------
// Golden machinery

/// Mask the ENUMERATED timing fields (report.rs module docs) — and nothing
/// else. A new nondeterministic field must be argued into that list, not
/// silently added here.
fn mask(doc: &mut Value) {
    if let Some(meta) = doc.get_mut("_meta") {
        meta["generated_at"] = json!("<TIME>");
        if let Some(archive) = meta.get_mut("archive").and_then(Value::as_array_mut) {
            for entry in archive {
                if let Some(streams) = entry.get_mut("streams").and_then(Value::as_array_mut) {
                    for s in streams {
                        if s["age_seconds"].is_number() {
                            s["age_seconds"] = json!("<AGE>");
                        }
                    }
                }
            }
        }
    }
}

fn golden(name: &str, doc: &Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/goldens")
        .join(name);
    let got = serde_json::to_string_pretty(doc).unwrap() + "\n";
    if std::env::var_os("GHGRAPH_UPDATE_GOLDENS").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &got).unwrap();
        return;
    }
    let want = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing golden {name} — generate with GHGRAPH_UPDATE_GOLDENS=1 and review")
    });
    assert_eq!(
        got, want,
        "golden {name} diverged — if intended, regenerate and review the diff"
    );
}

/// Run a verb twice, assert the two documents agree byte-for-byte after
/// masking (the determinism contract), and pin the result to its golden.
fn golden_verb(s: &Scratch, name: &str, args: &[&str]) {
    let mut a = s.run_ok(args);
    let mut b = s.run_ok(args);
    mask(&mut a);
    mask(&mut b);
    assert_eq!(
        serde_json::to_string(&a).unwrap(),
        serde_json::to_string(&b).unwrap(),
        "{args:?} must be deterministic modulo masked timing"
    );
    golden(name, &a);
}

// ---------------------------------------------------------------------------
// The goldens

#[test]
fn golden_prs_default() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    golden_verb(&s, "prs_default.json", &["prs"]);
}

#[test]
fn golden_prs_all_limited() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    // --all admits merged/deleted rows; --limit 3 truncates while the total
    // stays disclosed (limits govern presentation, never derivation).
    golden_verb(
        &s,
        "prs_all_limited.json",
        &["prs", "--all", "--limit", "3"],
    );
}

#[test]
fn golden_prs_author_and_repo() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    // Case-insensitive login and case-folded repo: the flags accept what
    // GitHub accepts, and both spellings must yield identical bytes.
    golden_verb(
        &s,
        "prs_author.json",
        &["prs", "--repo", "OCTO/Alpha", "--author", "ME"],
    );
    let mut upper = s.run_ok(&["prs", "--repo", "OCTO/Alpha", "--author", "ME"]);
    let mut lower = s.run_ok(&["prs", "--repo", "octo/alpha", "--author", "me"]);
    mask(&mut upper);
    mask(&mut lower);
    assert_eq!(upper, lower, "identifier case must not change the document");
}

#[test]
fn golden_pr_full() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    golden_verb(&s, "pr_full.json", &["pr", "octo/alpha#1"]);
    // The three reference forms converge on one canonical document.
    let mut by_url = s.run_ok(&["pr", "https://github.com/octo/alpha/pull/1"]);
    let mut by_number = s.run_ok(&["pr", "1", "--repo", "octo/alpha"]);
    let mut qualified = s.run_ok(&["pr", "octo/alpha#1"]);
    mask(&mut by_url);
    mask(&mut by_number);
    mask(&mut qualified);
    assert_eq!(qualified, by_url);
    assert_eq!(qualified, by_number);
}

#[test]
fn golden_pr_elided() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    // 8 bytes lands inside the 🦀 (4 bytes at offset 7 after "héllo ") —
    // the boundary must back off, never split. body_elided flips per field;
    // truncated (archive property) is untouched.
    golden_verb(
        &s,
        "pr_elided.json",
        &["pr", "octo/alpha#1", "--max-body-bytes", "8"],
    );
}

#[test]
fn golden_attention_default() {
    let s = Scratch::new();
    seed(&s);
    attention_config(&s, &[]);
    // No teams declared: #9 (own PR, thread waits on me) and #7 (user
    // request, case-flipped) fill waiting_on_me; #1 falls through to
    // they_replied (alice's PR, viewer participated, others spoke since —
    // and it therefore never reaches people_prs: priority dedup); #2 is
    // ready_to_merge; #8 is alice's untouched PR → people_prs. #10 (own,
    // freshly approved, TRUNCATED) appears nowhere: fail-closed out of
    // ready_to_merge, and its approval is not a reply.
    golden_verb(&s, "attention.json", &["attention"]);
}

#[test]
fn golden_attention_teams() {
    let s = Scratch::new();
    seed(&s);
    attention_config(&s, &["platform"]);
    // Declaring the team moves #1 up into waiting_on_me via its 'platform'
    // team request; the undeclared 'security' request on #7 still surfaces
    // nothing. they_replied empties — same archive, config-only change.
    golden_verb(&s, "attention_teams.json", &["attention"]);
}

#[test]
fn golden_attention_limited() {
    let s = Scratch::new();
    seed(&s);
    attention_config(&s, &[]);
    // --limit 1 caps each bucket's rows; totals stay disclosed.
    golden_verb(&s, "attention_limited.json", &["attention", "--limit", "1"]);
}

/// Polarity edges the golden seed can't carry: states ghgraph's own writer
/// never produces, hand-planted because `query` proves the archive is
/// reachable by arbitrary SQL — a derivation input is validated where it
/// is consumed (attention.rs), and each of these pins a failure DIRECTION.
#[test]
fn attention_probes_polarity_edges() {
    let s = Scratch::new();
    seed(&s);
    {
        let arch = db::open_rw(&s.db_path()).unwrap();
        let c = arch.conn();
        // #11 — viewer's PR whose only other-party act is a review row
        // with a NULL verdict: must read as a reply (fail-open), not
        // vanish through SQL three-valued logic (`state IS 'APPROVED'`).
        c.execute(
            "INSERT INTO prs (pk, id, repo, number, title, state, is_draft, author, \
                              created_at, updated_at, url) \
             VALUES (11, 'PR_a11', 'octo/alpha', 11, 'Probe null verdict', 'OPEN', 0, \
                     'me', '2026-01-02T00:00:00Z', '2026-01-05T00:00:00Z', 'u11')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO comments (id, parent_kind, parent, kind, state, author, body, \
                                   created_at) \
             VALUES ('RV_a11', 'pr', 11, 'review', NULL, 'bob', 'please split this', \
                     '2026-01-03T00:00:00Z')",
            [],
        )
        .unwrap();
        // #12 — requests of an UNRECOGNIZED kind: one naming the viewer
        // (escalates into waiting_on_me), one naming someone else (never
        // matches). Sync writes only user/team; this is the shape-drift
        // guard's pin.
        c.execute(
            "INSERT INTO prs (pk, id, repo, number, title, state, is_draft, author, \
                              created_at, updated_at, url) \
             VALUES (12, 'PR_a12', 'octo/alpha', 12, 'Probe unknown kind', 'OPEN', 0, \
                     'alice', '2026-01-02T00:00:00Z', '2026-01-05T00:00:00Z', 'u12')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO review_requests (pr, reviewer, kind) VALUES (12, 'ME', 'mannequin')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO review_requests (pr, reviewer, kind) VALUES (12, 'dave', 'copilot')",
            [],
        )
        .unwrap();
        // #13 — a MERGED PR of the viewer's with a fresh other-party
        // reply: excluded from every bucket. The working-set narrowing is
        // recorded in attention.rs with its reversal trigger; this test is
        // the narrowing's named witness, so reversing it is an edit here,
        // not an accident.
        c.execute(
            "INSERT INTO prs (pk, id, repo, number, title, state, is_draft, author, \
                              created_at, updated_at, merged_at, url) \
             VALUES (13, 'PR_a13', 'octo/alpha', 13, 'Probe merged reply', 'MERGED', 0, \
                     'me', '2026-01-02T00:00:00Z', '2026-01-05T00:00:00Z', \
                     '2026-01-04T00:00:00Z', 'u13')",
            [],
        )
        .unwrap();
        c.execute(
            "INSERT INTO comments (id, parent_kind, parent, kind, author, body, created_at) \
             VALUES ('C_a13', 'pr', 13, 'comment', 'alice', 'does this regress?', \
                     '2026-01-04T12:00:00Z')",
            [],
        )
        .unwrap();
    }
    attention_config(&s, &[]);
    let doc = s.run_ok(&["attention"]);
    let buckets = doc["attention"].as_array().unwrap();
    let numbers = |name: &str| -> Vec<i64> {
        buckets.iter().find(|b| b["bucket"] == name).unwrap()["prs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["number"].as_i64().unwrap())
            .collect()
    };
    assert!(
        numbers("they_replied").contains(&11),
        "a verdict-less review row is a reply — only a PROVEN approval is excluded"
    );
    assert!(
        numbers("waiting_on_me").contains(&12),
        "an unrecognized request kind naming the viewer escalates"
    );
    for name in [
        "waiting_on_me",
        "they_replied",
        "ready_to_merge",
        "people_prs",
    ] {
        assert!(
            !numbers(name).contains(&13),
            "a merged PR is outside the working set (recorded narrowing): {name}"
        );
    }
}

/// EPIPE must not clobber an earned gate exit (main.rs emit). The consumer
/// closes the pipe before reading; either the write EPIPEs (earned exit
/// preserved) or it landed in the pipe buffer first (gate exit path runs) —
/// both must yield 1, so the assertion is race-free even though which arm
/// fires is not.
#[test]
fn gate_exit_survives_a_closed_pipe() {
    let s = Scratch::new();
    seed(&s);
    attention_config(&s, &[]);
    let mut child = Command::new(env!("CARGO_BIN_EXE_ghgraph"))
        .arg("--config")
        .arg(s.config_path())
        .args(["attention", "--fail-if-any"])
        .current_dir(&s.dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let status = child.wait().unwrap();
    assert_eq!(
        status.code(),
        Some(1),
        "a closed pipe means the consumer went away, never all-clear"
    );
}

#[test]
fn attention_fail_if_any_gates_exit_never_bytes() {
    let s = Scratch::new();
    seed(&s);
    attention_config(&s, &[]);
    let (code, doc, stderr) = s.run(&["attention"]);
    assert_eq!(code, 0, "no flag, no gate; stderr:\n{stderr}");
    let (gated_code, gated_doc, stderr) = s.run(&["attention", "--fail-if-any"]);
    assert_eq!(gated_code, 1, "demands trip the gate; stderr:\n{stderr}");
    let (mut a, mut b) = (doc.unwrap(), gated_doc.unwrap());
    mask(&mut a);
    mask(&mut b);
    assert_eq!(a, b, "the gate flag must never change a byte of JSON");

    // Maintainer demands are viewer-INDEPENDENT: a hermit viewer with no
    // involvement anywhere still trips the gate while a project repo has
    // unreviewed PRs or untriaged issues — the maintainer sweep is a
    // property of the scope, not the seat.
    s.write_config(&json!({
        "viewer": "hermit",
        "repos": ["octo/alpha", {"repo": "octo/beta", "scope": "project"}],
    }));
    let (code, doc, stderr) = s.run(&["attention", "--fail-if-any"]);
    assert_eq!(
        code, 1,
        "maintainer demands trip the gate; stderr:\n{stderr}"
    );
    let buckets = doc.unwrap()["attention"].as_array().unwrap().clone();
    assert_eq!(buckets.len(), 6, "project scope emits the maintainer pair");

    // All clear: the same hermit with beta back at working scope — the
    // operator buckets are empty and the maintainer pair is ABSENT, not
    // empty (no maintainer sweep was configured; report.rs module docs).
    s.write_config(&json!({
        "viewer": "hermit",
        "repos": ["octo/alpha", "octo/beta"],
    }));
    let (code, doc, stderr) = s.run(&["attention", "--fail-if-any"]);
    assert_eq!(code, 0, "all-clear exits 0; stderr:\n{stderr}");
    let buckets = doc.unwrap()["attention"].as_array().unwrap().clone();
    assert_eq!(
        buckets.len(),
        4,
        "every operator bucket appears even when empty"
    );
    for b in &buckets {
        assert_eq!(b["total"], json!(0), "{b}");
        assert_eq!(b["prs"], json!([]));
    }
}

#[test]
fn golden_search() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    // "retry" hits PR #2 by title (self), PR #1 by body AND two comments,
    // and issue beta#3 by title+body — three groups, recency order.
    golden_verb(&s, "search.json", &["search", "retry"]);
}

#[test]
fn golden_search_limited() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    golden_verb(
        &s,
        "search_limited.json",
        &["search", "retry", "--limit", "1"],
    );
}

#[test]
fn golden_query() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    golden_verb(
        &s,
        "query.json",
        &[
            "query",
            "SELECT repo, number, title FROM prs ORDER BY repo, number",
            "--limit",
            "3",
        ],
    );
}

#[test]
fn golden_stats() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    golden_verb(&s, "stats.json", &["stats"]);
}

// ---------------------------------------------------------------------------
// Meta behaviors the fixed-past seed cannot golden

#[test]
fn meta_fresh_stream_is_not_stale() {
    let s = Scratch::new();
    {
        let arch = db::open_rw(&s.db_path()).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // A checked-just-now stream: stale must read false and no hint
        // appears. Formatting via SQLite keeps this test free of a second
        // RFC 3339 writer.
        arch.conn()
            .execute(
                "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                         runs_since_advance, fingerprint) \
                 VALUES ('octo/alpha', 'pr', '2026-01-05T00:00:00Z', \
                         strftime('%Y-%m-%dT%H:%M:%SZ', ?1, 'unixepoch'), 0, \
                         '{\"bots\":true,\"exclude_authors\":[],\"lookback_days\":90,\
                           \"people\":[],\"scope\":\"working\",\"viewer\":\"me\"}')",
                [now as i64],
            )
            .unwrap();
    }
    s.write_config(&json!({"viewer": "me", "repos": ["octo/alpha"]}));
    let doc = s.run_ok(&["prs"]);
    let entry = &doc["_meta"]["archive"][0];
    assert_eq!(entry["streams"][0]["stale"], json!(false));
    assert!(entry.get("hint").is_none(), "no hint when fresh: {entry}");
    assert_eq!(entry["config_pending"], json!(false));
}

#[test]
fn meta_never_synced_repo_is_disclosed() {
    let s = Scratch::new();
    {
        let _ = db::open_rw(&s.db_path()).unwrap();
    }
    s.write_config(&json!({"viewer": "me", "repos": ["octo/alpha"]}));
    let doc = s.run_ok(&["prs"]);
    let entry = &doc["_meta"]["archive"][0];
    assert_eq!(entry["repo"], json!("octo/alpha"));
    assert_eq!(entry["config_pending"], json!(true));
    assert_eq!(entry["fingerprint"], Value::Null);
    assert_eq!(entry["streams"], json!([]));
    assert!(
        entry["hint"].as_str().unwrap().contains("never synced"),
        "{entry}"
    );
}

// ---------------------------------------------------------------------------
// Error classification: the code names the actor who can fix it

#[test]
fn query_cannot_write_and_says_so_as_user_input() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["query", "DELETE FROM prs"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"), "{err}");
    // And nothing was deleted — the read-only pair held.
    let doc = s.run_ok(&["query", "SELECT COUNT(*) FROM prs"]);
    assert_eq!(doc["rows"][0][0], json!(10));
}

#[test]
fn query_refuses_multiple_statements() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["query", "SELECT 1; SELECT 2"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("one per invocation"),
        "{err}"
    );
    // Trailing whitespace/comments are NOT a second statement.
    let doc = s.run_ok(&["query", "SELECT 1 -- trailing note"]);
    assert_eq!(doc["rows"], json!([[1]]));
}

#[test]
fn query_refuses_parameters() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["query", "SELECT * FROM prs WHERE repo = ?1"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
    assert!(
        err["error"]["message"].as_str().unwrap().contains("inline"),
        "{err}"
    );
}

#[test]
fn query_syntax_error_is_user_input() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["query", "SELEC typo"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
}

#[test]
fn query_reads_stdin_when_dashed() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let out = {
        use std::io::Write;
        let mut child = Command::new(env!("CARGO_BIN_EXE_ghgraph"))
            .arg("--config")
            .arg(s.config_path())
            .args(["query", "-"])
            .current_dir(&s.dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"SELECT COUNT(*) FROM issues")
            .unwrap();
        child.wait_with_output().unwrap()
    };
    assert_eq!(out.status.code(), Some(0));
    let doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(doc["rows"], json!([[6]]));
}

#[test]
fn query_reads_stdin_when_piped_without_argument() {
    // Absent argument + piped stdin is the second stdin form ("-" is the
    // first); the terminal check must only fire when there is genuinely no
    // SQL anywhere (the && ↔ || mutant this discriminates).
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let out = {
        use std::io::Write;
        let mut child = Command::new(env!("CARGO_BIN_EXE_ghgraph"))
            .arg("--config")
            .arg(s.config_path())
            .arg("query")
            .current_dir(&s.dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"SELECT COUNT(*) FROM prs")
            .unwrap();
        child.wait_with_output().unwrap()
    };
    assert_eq!(out.status.code(), Some(0));
    let doc: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(doc["rows"], json!([[10]]));
}

#[test]
fn harness_reverse_selects_pragma_is_live() {
    // The determinism harness itself: every run in this suite sets
    // GHGRAPH_TEST_REVERSE_SELECTS=1, and the pragma must actually be ON in
    // the connection the verbs use — otherwise every golden here is passing
    // by physical row order, not by ORDER BY, and the suite proves nothing.
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let doc = s.run_ok(&["query", "PRAGMA reverse_unordered_selects"]);
    assert_eq!(doc["rows"], json!([[1]]), "the hook must reach the reader");
}

#[test]
fn meta_project_repo_missing_issue_stream_is_pending() {
    // A project-scope repo whose stored PR-stream fingerprint MATCHES the
    // loaded config but whose issue stream has not synced yet: the next
    // sync adds a stream, so the config is pending — the expected-stream
    // set is part of the fingerprint comparison, not a separate truth.
    let s = Scratch::new();
    {
        let arch = db::open_rw(&s.db_path()).unwrap();
        arch.conn()
            .execute(
                "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                         runs_since_advance, fingerprint) \
                 VALUES ('octo/beta', 'pr', '2026-01-04T00:00:00Z', '2026-01-04T00:10:00Z', 0, \
                         '{\"bots\":false,\"exclude_authors\":[],\"lookback_days\":90,\
                           \"people\":[],\"scope\":\"project\",\"viewer\":\"\"}')",
                [],
            )
            .unwrap();
    }
    s.write_config(&json!({
        "viewer": "me",
        "repos": [{"repo": "octo/beta", "scope": "project"}],
    }));
    let doc = s.run_ok(&["prs"]);
    let entry = &doc["_meta"]["archive"][0];
    assert_eq!(entry["config_pending"], json!(true), "{entry}");
    // And the same repo with BOTH streams present reads settled.
    {
        let arch = db::open_rw(&s.db_path()).unwrap();
        arch.conn()
            .execute(
                "INSERT INTO sync_state (repo, stream, last_item_updated_at, last_checked_at, \
                                         runs_since_advance, fingerprint) \
                 VALUES ('octo/beta', 'issue', '2026-01-04T00:00:00Z', '2026-01-04T00:10:00Z', \
                         0, '{\"bots\":false,\"exclude_authors\":[],\"lookback_days\":90,\
                              \"people\":[],\"scope\":\"project\",\"viewer\":\"\"}')",
                [],
            )
            .unwrap();
    }
    let doc = s.run_ok(&["prs"]);
    let entry = &doc["_meta"]["archive"][0];
    assert_eq!(entry["config_pending"], json!(false), "{entry}");
}

#[test]
fn meta_disclosed_fingerprint_prefers_pr_stream() {
    // Streams disagree only in transitional states; the 'pr' stream's
    // stored fingerprint stands for the repo (and config_pending is already
    // true). The lookback_days value is the discriminator.
    let s = Scratch::new();
    {
        let arch = db::open_rw(&s.db_path()).unwrap();
        for (stream, days) in [("issue", 30), ("pr", 90)] {
            arch.conn()
                .execute(
                    "INSERT INTO sync_state (repo, stream, last_item_updated_at, \
                                             last_checked_at, runs_since_advance, fingerprint) \
                     VALUES ('octo/beta', ?1, '2026-01-04T00:00:00Z', '2026-01-04T00:10:00Z', \
                             0, ?2)",
                    rusqlite::params![
                        stream,
                        format!(
                            "{{\"bots\":false,\"exclude_authors\":[],\"lookback_days\":{days},\
                              \"people\":[],\"scope\":\"project\",\"viewer\":\"\"}}"
                        )
                    ],
                )
                .unwrap();
        }
    }
    s.write_config(&json!({
        "viewer": "me",
        "repos": [{"repo": "octo/beta", "scope": "project"}],
    }));
    let doc = s.run_ok(&["prs"]);
    let entry = &doc["_meta"]["archive"][0];
    assert_eq!(entry["fingerprint"]["lookback_days"], json!(90), "{entry}");
}

#[test]
fn pr_bare_number_resolves_via_cwd_remote() {
    // The success path of the git-remote fallback: a clone whose origin
    // names github.com resolves a bare number without --repo. The remote
    // value is attacker-chosen content — this test is the benign half; the
    // hostile half lives in the remote_url fuzz target and unit table.
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let git = |args: &[&str]| {
        let ok = Command::new("git")
            .args(args)
            .current_dir(&s.dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("git present in dev/CI environments")
            .success();
        assert!(ok, "git {args:?} failed");
    };
    git(&["init", "-q"]);
    git(&["remote", "add", "origin", "git@github.com:OCTO/Alpha.git"]);
    let doc = s.run_ok(&["pr", "1"]);
    assert_eq!(
        doc["pr"]["repo"],
        json!("octo/alpha"),
        "case-folded via the remote"
    );
    assert_eq!(doc["pr"]["number"], json!(1));
}

#[test]
fn search_syntax_error_is_user_input() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["search", "\"unterminated"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"), "{err}");
}

#[test]
fn pr_bare_number_without_repo_names_both_remedies() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    // cwd is the scratch dir: no git repo, so the remote fallback yields
    // nothing and the error must name --repo AND the clone alternative.
    let err = s.run_err(&["pr", "7"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
    let msg = err["error"]["message"].as_str().unwrap();
    assert!(msg.contains("--repo") && msg.contains("clone"), "{msg}");
}

#[test]
fn pr_not_in_archive_is_user_input_with_remedy() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["pr", "octo/alpha#999"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("sync --pr"),
        "{err}"
    );
}

#[test]
fn missing_archive_is_configuration() {
    let s = Scratch::new();
    s.write_config(&json!({"viewer": "me", "repos": ["octo/alpha"]}));
    let err = s.run_err(&["prs"]);
    assert_eq!(err["error"]["code"], json!("CONFIGURATION"));
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("ghgraph sync"),
        "{err}"
    );
}

#[test]
fn invalid_author_flag_is_user_input() {
    let s = Scratch::new();
    seed(&s);
    standard_config(&s);
    let err = s.run_err(&["prs", "--author", "not a login!"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
    let err = s.run_err(&["prs", "--repo", "not-a-repo"]);
    assert_eq!(err["error"]["code"], json!("USER_INPUT"));
}
