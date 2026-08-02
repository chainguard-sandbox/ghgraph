// The deeply nested json! fixture builders exceed the default macro
// recursion limit; raising it is cosmetic, not architectural.
#![recursion_limit = "256"]

//! The milestone-2 load-bearing suite (ROADMAP): fixture replay with zero
//! row and zero FTS deltas (including a metadata-only flip proving the FTS
//! WHEN guards), SIGKILL at arbitrary points (watermark never leads data;
//! the redo converges), two-process lock contention, floor-injection
//! deferral across runs (window banking, monotone watermark, no double
//! hydration of banked windows), a config-transition test per fingerprint
//! case including person removal, and the quarantine lifecycle.
//!
//! Every test drives the REAL binary (CARGO_BIN_EXE_ghgraph) end to end
//! with a scripted fake `gh` reached through the child's PATH — the same
//! seam the FakeGh unit tests use, but across the process boundary, so the
//! run lock, the process-group kill story, and the stdout contract are all
//! in the tested path. The fake serves canned JSON keyed by document kind
//! (discovery responses optionally per run+sequence, hydrations per node
//! id) and appends one line per call to calls.log, which is how tests
//! assert "hydrated exactly once".
//!
//! Determinism: workers=1 in every config here (the suite asserts call
//! sequences); rateLimit.remaining is embedded per fixture, which is how
//! the floor tests inject exhaustion at exact points.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Harness

struct Fake {
    dir: PathBuf,
}

const GH_SCRIPT: &str = r#"#!/bin/sh
dir="$(dirname "$0")"
if [ "$1" = "--version" ]; then
  n=$(cat "$dir/run_n" 2>/dev/null || echo 0); echo $((n+1)) > "$dir/run_n"
  echo "gh version 2.96.0 (2026-01-01)"; exit 0
fi
if [ "$1" = "api" ] && [ "$2" = "user" ]; then cat "$dir/user.json"; exit 0; fi
doc=$(cat)
q=""; id=""; owner=""; name=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-f" ]; then
    case "$a" in
      q=*) q="${a#q=}";;
      id=*) id="${a#id=}";;
      owner=*) owner="${a#owner=}";;
      name=*) name="${a#name=}";;
    esac
  fi
  prev="$a"
done
run=$(cat "$dir/run_n" 2>/dev/null || echo 1)
case "$doc" in
  *'search(type: ISSUE'*)
    seqf="$dir/disc_seq_$run"; s=$(cat "$seqf" 2>/dev/null || echo 0); echo $((s+1)) > "$seqf"
    echo "DISC|run=$run|seq=$s|q=$q" >> "$dir/calls.log"
    resp="$dir/disc-$run-$s.json"
    [ -f "$resp" ] || resp="$dir/disc-default.json"
    ;;
  *'pullRequest(number:'*)
    echo "PRID|run=$run|owner=$owner|name=$name" >> "$dir/calls.log"
    resp="$dir/prid.json"
    ;;
  *)
    echo "HYD|run=$run|id=$id" >> "$dir/calls.log"
    resp="$dir/hyd-$id.json"
    ;;
esac
if [ -f "$dir/sleep_every_call" ]; then sleep "$(cat "$dir/sleep_every_call")"; fi
if [ ! -f "$resp" ]; then echo "fake gh: no fixture $resp" >&2; exit 1; fi
cat "$resp"
"#;

impl Fake {
    fn new() -> Fake {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "ghgraph-pipeline-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let fake = Fake { dir };
        fake.write_exec("gh", GH_SCRIPT);
        fake.write("user.json", r#"{"login":"viewer"}"#);
        fake
    }

    fn write(&self, name: &str, content: &str) {
        std::fs::write(self.dir.join(name), content).unwrap();
    }

    fn write_exec(&self, name: &str, content: &str) {
        use std::os::unix::fs::PermissionsExt;
        let p = self.dir.join(name);
        std::fs::write(&p, content).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn remove(&self, name: &str) {
        let _ = std::fs::remove_file(self.dir.join(name));
    }

    /// Write the config, injecting db_path into this scratch.
    fn config(&self, body: &Value) {
        let mut v = body.clone();
        v["db_path"] = json!(self.dir.join("archive/ghgraph.db").to_str().unwrap());
        self.write("config.json", &v.to_string());
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ghgraph"));
        cmd.arg("--config")
            .arg(self.dir.join("config.json"))
            .args(args)
            .env("PATH", format!("{}:/usr/bin:/bin", self.dir.display()))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd
    }

    /// Run to completion; (exit code, stdout JSON if any, stderr).
    fn run(&self, args: &[&str]) -> (i32, Option<Value>, String) {
        let out = self.command(args).output().expect("spawn ghgraph");
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let doc = serde_json::from_str(&stdout).ok();
        (out.status.code().unwrap_or(-1), doc, stderr)
    }

    fn sync_ok(&self) -> Value {
        let (code, doc, stderr) = self.run(&["sync"]);
        assert_eq!(code, 0, "sync must exit 0; stderr:\n{stderr}");
        doc.expect("sync emits one JSON document")
    }

    fn calls(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.join("calls.log"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn hydrations(&self, run: u32) -> Vec<String> {
        self.calls()
            .iter()
            .filter_map(|l| {
                l.strip_prefix(&format!("HYD|run={run}|id="))
                    .map(str::to_string)
            })
            .collect()
    }

    fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open_with_flags(
            self.dir.join("archive/ghgraph.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )
        .expect("open archive for assertions")
    }

    fn query_one<T: rusqlite::types::FromSql>(&self, sql: &str) -> T {
        self.db().query_row(sql, [], |r| r.get(0)).expect(sql)
    }

    /// A deterministic full-content dump of every data table plus the raw
    /// FTS index blobs, with the wall-clock columns masked (verified_at,
    /// observed_at, synced_at, next_retry_at, deleted_at) — the enumerated
    /// nondeterminism, nothing else. sync_state is dumped separately by the
    /// tests that assert on it.
    fn dump(&self) -> String {
        let conn = self.db();
        let mut out = String::new();
        let tables = [
            ("prs", "repo, number"),
            ("issues", "repo, number"),
            ("review_threads", "id"),
            ("comments", "id"),
            ("review_requests", "pr, reviewer, kind"),
            ("refs", "src_pr, kind, source, target_repo, target_number"),
            ("observations", "seq"),
            ("quarantine", "id"),
            ("prs_fts_data", "id"),
            ("comments_fts_data", "id"),
            ("issues_fts_data", "id"),
        ];
        let masked = [
            "verified_at",
            "observed_at",
            "synced_at",
            "next_retry_at",
            "deleted_at",
        ];
        for (table, order) in tables {
            out.push_str(&format!("== {table}\n"));
            let mut stmt = conn
                .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))
                .unwrap();
            let names: Vec<String> = stmt
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect();
            let mut rows = stmt.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                for (i, name) in names.iter().enumerate() {
                    let cell: String = if masked.contains(&name.as_str()) {
                        let v: Option<String> = row.get(i).unwrap_or(None);
                        if v.is_some() {
                            "<T>".into()
                        } else {
                            "-".into()
                        }
                    } else {
                        match row.get_ref(i).unwrap() {
                            rusqlite::types::ValueRef::Null => "-".into(),
                            rusqlite::types::ValueRef::Integer(v) => v.to_string(),
                            rusqlite::types::ValueRef::Real(v) => v.to_string(),
                            rusqlite::types::ValueRef::Text(t) => {
                                String::from_utf8_lossy(t).into_owned()
                            }
                            rusqlite::types::ValueRef::Blob(b) => {
                                // FTS index bytes: a stable digest is enough
                                // to witness "unchanged".
                                format!("blob:{}:{}", b.len(), fnv(b))
                            }
                        }
                    };
                    out.push_str(&cell);
                    out.push('|');
                }
                out.push('\n');
            }
        }
        out
    }

    fn repo_summary<'a>(&self, doc: &'a Value, repo: &str) -> &'a Value {
        doc["sync"]["repos"]
            .as_array()
            .expect("repos array")
            .iter()
            .find(|r| r["repo"] == repo)
            .expect("repo in summary")
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn fnv(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &x in b {
        h ^= u64::from(x);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// --- fixture builders: JSON the strict parse types accept ---

fn rate_limit(remaining: u32) -> Value {
    json!({"cost": 1, "remaining": remaining, "resetAt": "2026-08-01T00:00:00Z"})
}

fn author(login: &str, typename: &str) -> Value {
    json!({"login": login, "__typename": typename, "databaseId": 7})
}

struct Pr {
    id: &'static str,
    number: i64,
    updated_at: &'static str,
    title: String,
    body: String,
    state: &'static str,
    author_login: String,
    author_type: &'static str,
    comment_ids: Vec<String>,
    minimized: bool,
    remaining: u32,
    repo: String,
}

impl Pr {
    fn new(id: &'static str, number: i64, updated_at: &'static str) -> Pr {
        Pr {
            id,
            number,
            updated_at,
            title: format!("title {number}"),
            body: format!("body {number}"),
            state: "OPEN",
            author_login: "alice".into(),
            author_type: "User",
            comment_ids: vec![format!("C_{id}")],
            minimized: false,
            remaining: 4000,
            repo: "o/n".into(),
        }
    }

    fn hit(&self) -> Value {
        json!({
            "id": self.id,
            "updatedAt": self.updated_at,
            "author": {"login": self.author_login, "__typename": self.author_type}
        })
    }

    fn hydration(&self) -> String {
        let comments: Vec<Value> = self
            .comment_ids
            .iter()
            .map(|cid| {
                json!({
                    "id": cid, "body": format!("comment {cid}"),
                    "createdAt": "2026-07-10T00:00:00Z", "lastEditedAt": null,
                    "url": "https://github.com/x", "isMinimized": self.minimized,
                    "authorAssociation": "NONE", "author": author("carol", "User")
                })
            })
            .collect();
        json!({
            "data": {
                "node": {
                    "id": self.id, "number": self.number, "title": self.title,
                    "body": self.body, "state": self.state, "isDraft": false,
                    "url": format!("https://github.com/{}/pull/{}", self.repo, self.number),
                    "author": author(&self.author_login, self.author_type),
                    "authorAssociation": "MEMBER",
                    "repository": {"nameWithOwner": self.repo},
                    "headRefName": "feature", "baseRefName": "main",
                    "reviewDecision": null,
                    "createdAt": "2026-07-01T00:00:00Z",
                    "updatedAt": self.updated_at,
                    "mergedAt": null,
                    "closedAt": if self.state == "CLOSED" { json!("2026-07-21T00:00:00Z") } else { Value::Null },
                    "commits": {"nodes": [{"commit": {
                        "oid": "0123456789012345678901234567890123456789",
                        "committedDate": "2026-07-09T00:00:00Z"}}]},
                    "reviewRequests": {"totalCount": 1, "nodes": [
                        {"requestedReviewer": {"login": "rev"}}]},
                    "latestOpinionatedReviews": {"totalCount": 1, "nodes": [{
                        "id": format!("REV_{}", self.id), "state": "APPROVED",
                        "submittedAt": "2026-07-11T00:00:00Z", "body": "lgtm",
                        "url": "https://github.com/r", "authorAssociation": "MEMBER",
                        "author": author("rev", "User")}]},
                    "closingIssuesReferences": {"totalCount": 1, "nodes": [{
                        "id": format!("I_{}", self.id), "number": self.number + 100,
                        "title": "linked issue", "state": "OPEN", "body": "issue body",
                        "updatedAt": "2026-07-08T00:00:00Z",
                        "author": author("dora", "User"), "authorAssociation": "NONE",
                        "url": "https://github.com/i",
                        "repository": {"nameWithOwner": self.repo}}]},
                    "comments": {"totalCount": comments.len(),
                        "pageInfo": {"hasNextPage": false, "endCursor": null},
                        "nodes": comments},
                    "reviewThreads": {"totalCount": 1,
                        "pageInfo": {"hasNextPage": false, "endCursor": null},
                        "nodes": [{
                            "id": format!("T_{}", self.id), "isResolved": false,
                            "isOutdated": false, "path": "src/x.rs", "line": 10,
                            "comments": {"totalCount": 1, "nodes": [{
                                "id": format!("TC_{}", self.id), "body": "thread comment",
                                "createdAt": "2026-07-10T01:00:00Z", "lastEditedAt": null,
                                "url": "https://github.com/t", "isMinimized": false,
                                "authorAssociation": "NONE", "author": author("erin", "User")}]}}]}
                },
                "rateLimit": rate_limit(self.remaining)
            }
        })
        .to_string()
    }
}

fn discovery(hits: &[&Pr], issue_count: Option<i64>, remaining: u32) -> String {
    let nodes: Vec<Value> = hits.iter().map(|p| p.hit()).collect();
    json!({
        "data": {
            "search": {
                "issueCount": issue_count.unwrap_or(nodes.len() as i64),
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": nodes
            },
            "rateLimit": rate_limit(remaining)
        }
    })
    .to_string()
}

fn base_config() -> Value {
    json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project", "issues": false}],
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    })
}

fn install_prs(fake: &Fake, prs: &[&Pr]) {
    fake.write("disc-default.json", &discovery(prs, None, 4000));
    for pr in prs {
        fake.write(&format!("hyd-{}.json", pr.id), &pr.hydration());
    }
}

// ---------------------------------------------------------------------------
// 1. Fixture replay: an unchanged remote twice → zero row, zero FTS deltas.

#[test]
fn replay_of_unchanged_remote_writes_nothing() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let b = Pr::new("PR_2", 2, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a, &b]);

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["fetched"], 2);
    assert_eq!(s["counts"]["upserted"], 2);
    assert_eq!(s["counts"]["unchanged"], 0);
    assert_eq!(s["health"]["truncated"], 0, "single-page fixtures verify");
    let dump1 = fake.dump();

    // Second run against the byte-identical remote: the diff gate must
    // skip every row, every observation, every FTS write.
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 0, "zero row deltas: {s}");
    assert_eq!(s["counts"]["unchanged"], 2);
    assert_eq!(s["counts"]["observations"], 0);
    assert_eq!(s["counts"]["soft_deleted"], 0);
    let dump2 = fake.dump();
    assert_eq!(
        dump1, dump2,
        "replay must be byte-identical incl. FTS blobs"
    );

    // The watermark is server-side time: exactly the newest updatedAt.
    let wm: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(wm, "2026-07-20T11:00:00Z");
    let checked: Option<String> =
        fake.query_one("SELECT last_checked_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(checked.is_some(), "completed stream stamps freshness");
    let starved: i64 = fake
        .query_one("SELECT runs_since_advance FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(starved, 0, "a completed stream is not starved");
}

// ---------------------------------------------------------------------------
// 2. Metadata-only flips: the FTS WHEN guards are enforcement, not comments.

#[test]
fn metadata_only_flip_updates_rows_but_never_fts() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    let fts_before: String = fake
        .query_one("SELECT group_concat(id || ':' || length(block)) FROM prs_fts_data ORDER BY id");
    let cfts_before: String = fake.query_one(
        "SELECT group_concat(id || ':' || length(block)) FROM comments_fts_data ORDER BY id",
    );

    // State flips CLOSED and the one comment flips is_minimized — the
    // exact quiet-mutation shapes the skeleton walk exists to record.
    // Title and body are byte-identical, so FTS must not move.
    a.state = "CLOSED";
    a.minimized = true;
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 1);
    assert_eq!(
        s["counts"]["observations"], 1,
        "state OPEN→CLOSED is the one observed-field diff: {s}"
    );

    let state: String = fake.query_one("SELECT state FROM prs WHERE number=1");
    assert_eq!(state, "CLOSED");
    let minimized: i64 = fake.query_one("SELECT is_minimized FROM comments WHERE id='C_PR_1'");
    assert_eq!(minimized, 1);
    let (field, old, new): (String, String, String) = fake
        .db()
        .query_row(
            "SELECT field, old, new FROM observations ORDER BY seq DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (field.as_str(), old.as_str(), new.as_str()),
        ("state", "OPEN", "CLOSED")
    );

    let fts_after: String = fake
        .query_one("SELECT group_concat(id || ':' || length(block)) FROM prs_fts_data ORDER BY id");
    let cfts_after: String = fake.query_one(
        "SELECT group_concat(id || ':' || length(block)) FROM comments_fts_data ORDER BY id",
    );
    assert_eq!(
        fts_before, fts_after,
        "prs_fts must not rewrite on a state flip"
    );
    assert_eq!(
        cfts_before, cfts_after,
        "comments_fts must not rewrite on a minimize flip"
    );
}

// A comment deleted upstream sweeps (soft) under the completeness witness.
#[test]
fn upstream_comment_deletion_sweeps_softly() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into(), "C_b".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["soft_deleted"], 1, "{s}");
    let deleted: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_b'");
    assert!(deleted.is_some(), "swept, not erased");
    let body: String = fake.query_one("SELECT body FROM comments WHERE id='C_b'");
    assert_eq!(body, "comment C_b", "deleted rows keep their content");
}

// ---------------------------------------------------------------------------
// 3. SIGKILL at arbitrary points: the redo converges to the control state.

#[test]
fn sigkill_mid_run_converges_and_watermark_never_leads_data() {
    // Control: one uninterrupted run.
    let prs: Vec<Pr> = (1..=5)
        .map(|n| {
            let id: &'static str = Box::leak(format!("PR_{n}").into_boxed_str());
            let up: &'static str = Box::leak(format!("2026-07-20T0{n}:00:00Z").into_boxed_str());
            Pr::new(id, n, up)
        })
        .collect();
    let control = Fake::new();
    control.config(&base_config());
    install_prs(&control, &prs.iter().collect::<Vec<_>>());
    control.sync_ok();
    let want = control.dump();

    for kill_after in [2u32, 4, 6] {
        let fake = Fake::new();
        fake.config(&base_config());
        install_prs(&fake, &prs.iter().collect::<Vec<_>>());
        // Slow every call slightly so the kill lands mid-run, not after.
        fake.write("sleep_every_call", "0.3");

        let mut child = fake.command(&["sync"]).spawn().expect("spawn sync");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if fake.calls().len() as u32 >= kill_after {
                break;
            }
            if child.try_wait().expect("try_wait").is_some() {
                break; // finished before the target call count: still a case
            }
            assert!(
                Instant::now() < deadline,
                "fake gh never reached call {kill_after}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = child.kill(); // SIGKILL: no handler runs, by design
        let _ = child.wait();

        // Watermark never leads data, even in the killed wreckage: every
        // fixture PR at or below the stored watermark must be present.
        if fake.dir.join("archive/ghgraph.db").exists() {
            let conn = fake.db();
            let wm: Option<String> = conn
                .query_row(
                    "SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'",
                    [],
                    |r| r.get(0),
                )
                .ok();
            if let Some(wm) = wm {
                for pr in &prs {
                    if pr.updated_at <= wm.as_str() {
                        let n: i64 = conn
                            .query_row("SELECT count(*) FROM prs WHERE id=?1", [pr.id], |r| {
                                r.get(0)
                            })
                            .unwrap();
                        assert_eq!(
                            n, 1,
                            "watermark {wm} passed {} without its row (kill@{kill_after})",
                            pr.id
                        );
                    }
                }
            }
        }

        // The redo: a fresh run must converge to the control state.
        fake.remove("sleep_every_call");
        fake.sync_ok();
        assert_eq!(
            fake.dump(),
            want,
            "post-kill resync diverged (kill after {kill_after} calls)"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Two-process lock contention: the second sync exits promptly, typed.

#[test]
fn second_sync_refuses_promptly_while_first_runs() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.write("sleep_every_call", "1");

    let mut first = fake.command(&["sync"]).spawn().expect("spawn first sync");
    // Wait until the first sync holds the lock (it logs its first call).
    let deadline = Instant::now() + Duration::from_secs(30);
    while fake.calls().is_empty() {
        assert!(Instant::now() < deadline, "first sync never started");
        std::thread::sleep(Duration::from_millis(20));
    }

    let started = Instant::now();
    let (code, doc, _) = fake.run(&["sync"]);
    let elapsed = started.elapsed();
    assert_eq!(code, 2, "second sync is a typed refusal");
    let doc = doc.expect("error envelope on stdout");
    assert_eq!(doc["error"]["code"], "TRANSIENT");
    assert!(
        doc["error"]["message"]
            .as_str()
            .unwrap()
            .contains("already running"),
        "{doc}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the refusal must be prompt, not queued behind the run: {elapsed:?}"
    );

    let status = first.wait().expect("first sync exits");
    assert!(status.success(), "the running sync must be unaffected");
    // And the lock releases with the process: a third sync now proceeds.
    fake.remove("sleep_every_call");
    fake.sync_ok();
}

// ---------------------------------------------------------------------------
// 5. Floor injection: banked windows never re-hydrate; watermark monotone.

#[test]
fn floor_deferral_banks_windows_and_never_rehydrates_them() {
    let fake = Fake::new();
    let mut cfg = base_config();
    cfg["rate_limit_floor"] = json!(500);
    fake.config(&cfg);

    let a1 = Pr::new("PR_A1", 1, "2026-07-10T00:00:00Z");
    let mut a2 = Pr::new("PR_A2", 2, "2026-07-10T06:00:00Z");
    a2.remaining = 100; // trips the floor after window A hydrates
    let b1 = Pr::new("PR_B1", 3, "2026-07-19T00:00:00Z");
    let b2 = Pr::new("PR_B2", 4, "2026-07-19T06:00:00Z");
    for pr in [&a1, &a2, &b1, &b2] {
        fake.write(&format!("hyd-{}.json", pr.id), &pr.hydration());
    }
    // Run 1: the full window reports capped (issueCount far above the two
    // nodes returned) → split; left half completes (A1, A2 — A2's response
    // drains the budget); right half's discovery defers at the floor.
    fake.write("disc-1-0.json", &discovery(&[&b1, &b2], Some(1500), 4000));
    fake.write("disc-1-1.json", &discovery(&[&a1, &a2], None, 4000));
    fake.write("disc-default.json", &discovery(&[&b1, &b2], None, 4000));

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["deferred_at_floor"], true, "{s}");
    assert_eq!(s["counts"]["fetched"], 2, "window A only");
    let wm: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(wm, "2026-07-10T06:00:00Z", "banked at window A's boundary");
    assert_eq!(
        fake.hydrations(1),
        vec!["PR_A1", "PR_A2"],
        "ascending updatedAt, window A only"
    );
    let starved: i64 = fake
        .query_one("SELECT runs_since_advance FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(starved, 1, "a deferred stream counts toward starvation");

    // Run 2: budget restored; discovery (from the banked watermark) serves
    // window B. The banked window's PRs are NEVER re-hydrated.
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["deferred_at_floor"], false);
    assert_eq!(fake.hydrations(2), vec!["PR_B1", "PR_B2"]);
    let wm2: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(wm2, "2026-07-19T06:00:00Z");
    assert!(wm2.as_str() > wm.as_str(), "monotone watermark");

    let all: Vec<String> = fake
        .calls()
        .iter()
        .filter(|l| l.starts_with("HYD"))
        .map(|l| l.rsplit('=').next().unwrap().to_string())
        .collect();
    assert_eq!(
        all.len(),
        4,
        "no PR hydrated twice across the deferral: {all:?}"
    );
}

// ---------------------------------------------------------------------------
// 6. Fingerprint transitions, one per case (incl. person removal).

fn working_config(people: &[&str], lookback: u32) -> Value {
    json!({
        "viewer": "viewer",
        "repos": ["o/n"],
        "people": people,
        "lookback_days": lookback,
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    })
}

/// The `updated:` bound of every discovery call in one run, by flavor.
fn discovery_bounds(fake: &Fake, run: u32) -> Vec<(String, String)> {
    fake.calls()
        .iter()
        .filter_map(|l| {
            let rest = l.strip_prefix(&format!("DISC|run={run}|"))?;
            let q = rest.split("|q=").nth(1)?;
            let since = q
                .split("updated:>=")
                .nth(1)
                .map(|s| s.split_whitespace().next().unwrap_or(""))?;
            let flavor = if q.contains("involves:")
                || q.contains("requested:")
                || q.contains("reviewed-by:")
            {
                q.split("is:pr ").nth(1).unwrap_or("").to_string()
            } else {
                String::new()
            };
            Some((flavor, since.to_string()))
        })
        .collect()
}

#[test]
fn fingerprint_transitions_drive_discovery_reach() {
    let fake = Fake::new();
    fake.config(&working_config(&[], 90));
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);

    // Run 1, cold start: the three viewer flavors, since ≈ lookback.
    fake.sync_ok();
    let b1 = discovery_bounds(&fake, 1);
    assert_eq!(b1.len(), 3, "three viewer flavors: {b1:?}");
    let cold_since = b1[0].1.clone();
    assert!(b1.iter().all(|(_, s)| *s == cold_since));

    // Run 2, unchanged config: incremental — since jumps to the watermark
    // minus the overlap, far past the lookback start.
    fake.sync_ok();
    let b2 = discovery_bounds(&fake, 2);
    assert_eq!(b2.len(), 3);
    assert_eq!(b2[0].1, "2026-07-20T09:50:00Z", "watermark − 10min overlap");

    // Run 3, person added: the regular incremental flavors PLUS a backfill
    // involves:bob over the full lookback (the cheaper-than-cold path).
    fake.config(&working_config(&["bob"], 90));
    fake.sync_ok();
    let b3 = discovery_bounds(&fake, 3);
    let backfill: Vec<_> = b3
        .iter()
        .filter(|(f, _)| f.contains("involves:bob"))
        .collect();
    assert_eq!(
        backfill.len(),
        2,
        "backfill + regular flavor for bob: {b3:?}"
    );
    assert!(
        backfill.iter().any(|(_, s)| *s < b2[0].1),
        "the backfill reaches back to the lookback: {b3:?}"
    );

    // Run 4, person removed: pure tightening — incremental, no backfill,
    // no cold start (since stays at watermark − overlap).
    fake.config(&working_config(&[], 90));
    fake.sync_ok();
    let b4 = discovery_bounds(&fake, 4);
    assert_eq!(b4.len(), 3, "no extra flavors: {b4:?}");
    assert_eq!(b4[0].1, "2026-07-20T09:50:00Z");

    // Run 5, lookback increased: a relaxation — the stream cold-starts.
    fake.config(&working_config(&[], 120));
    fake.sync_ok();
    let b5 = discovery_bounds(&fake, 5);
    assert!(
        b5[0].1 < cold_since,
        "cold start from the WIDER lookback: {} vs {cold_since}",
        b5[0].1
    );
}

// ---------------------------------------------------------------------------
// 7. Filters skip at discovery — and still advance the watermark.

#[test]
fn filtered_authors_cost_no_hydration_and_still_advance() {
    let fake = Fake::new();
    let mut cfg = base_config();
    cfg["repos"][0]["exclude_authors"] = json!(["spammer", "botty[bot]"]);
    fake.config(&cfg);

    let ok = Pr::new("PR_OK", 1, "2026-07-20T01:00:00Z");
    let mut bot = Pr::new("PR_BOT", 2, "2026-07-20T02:00:00Z");
    bot.author_login = "dependabot".into();
    bot.author_type = "Bot"; // project scope: bots default out
    let mut spam = Pr::new("PR_SPAM", 3, "2026-07-20T03:00:00Z");
    spam.author_login = "Spammer".into(); // case-insensitive match
    let mut botty = Pr::new("PR_BOTTY", 4, "2026-07-20T04:00:00Z");
    botty.author_login = "botty".into();
    botty.author_type = "Bot";
    install_prs(&fake, &[&ok, &bot, &spam, &botty]);

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["fetched"], 1);
    assert_eq!(s["counts"]["filtered"], 3, "{s}");
    assert_eq!(
        fake.hydrations(1),
        vec!["PR_OK"],
        "filtered PRs cost discovery only"
    );

    // A filtered item is declined, not unfetched: the newest activity here
    // is all filtered, and the watermark must still advance over it.
    let wm: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(wm, "2026-07-20T04:00:00Z");
}

// ---------------------------------------------------------------------------
// 8. The sync-time viewer identity check.

#[test]
fn viewer_mismatch_is_a_configuration_refusal() {
    let fake = Fake::new();
    fake.config(&base_config());
    fake.write("user.json", r#"{"login":"someone-else"}"#);
    let (code, doc, _) = fake.run(&["sync"]);
    assert_eq!(code, 2);
    let doc = doc.expect("error envelope");
    assert_eq!(doc["error"]["code"], "CONFIGURATION");
    let msg = doc["error"]["message"].as_str().unwrap();
    assert!(msg.contains("viewer"), "{msg}");
    assert!(
        !msg.contains("someone-else"),
        "the authenticated login is API text and stays out of envelopes: {msg}"
    );
    assert!(fake.calls().is_empty(), "refused before any data call");
}

// ---------------------------------------------------------------------------
// 9. Quarantine: backoff dominates, retries resolve, node:null drains.

#[test]
fn quarantine_lifecycle_backoff_retry_and_drain() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let broken = Pr::new("PR_X", 9, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a, &broken]);
    // PR_X's hydration fails outright (the fake exits 1 on a missing file).
    fake.remove("hyd-PR_X.json");

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 1, "{s}");
    assert_eq!(s["counts"]["fetched"], 1);
    let (attempts, class): (i64, String) = fake
        .db()
        .query_row(
            "SELECT attempts, error_class FROM quarantine WHERE id='PR_X'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((attempts, class.as_str()), (1, "transient"));
    // The watermark folds over hydrated ∪ filtered only: the quarantine
    // row LICENSES passing the id (a newer hydrated item may advance over
    // it), but nothing here forces an advance — it holds at the newest
    // hydrated item, so the quarantined id keeps being re-surfaced.
    let wm: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(wm, "2026-07-20T10:00:00Z");

    // Run 2, fixture healed but backoff not elapsed: quarantine dominates —
    // no retry call, even though discovery re-surfaces the id.
    fake.write("hyd-PR_X.json", &broken.hydration());
    fake.sync_ok();
    assert!(
        !fake.hydrations(2).contains(&"PR_X".to_string()),
        "backoff dominates every hydration cause: {:?}",
        fake.calls()
    );

    // Run 3, backoff aged out: the retry resolves, the row retires.
    fake.db()
        .execute(
            "UPDATE quarantine SET next_retry_at='2020-01-01T00:00:00Z' WHERE id='PR_X'",
            [],
        )
        .unwrap();
    fake.sync_ok();
    assert!(fake.hydrations(3).contains(&"PR_X".to_string()));
    let left: i64 = fake.query_one("SELECT count(*) FROM quarantine");
    assert_eq!(left, 0, "resolved retry deletes the record");
    let present: i64 = fake.query_one("SELECT count(*) FROM prs WHERE id='PR_X'");
    assert_eq!(present, 1);

    // node:null drain: PR_1 vanishes upstream. Each aged retry re-nulls;
    // the third attempt drains to deleted_at and retires the record.
    fake.write("hyd-PR_1.json", r#"{"data":{"node":null}}"#);
    fake.sync_ok(); // rediscovered → attempts=1 (node_null)
    for _ in 0..2 {
        fake.db()
            .execute(
                "UPDATE quarantine SET next_retry_at='2020-01-01T00:00:00Z' WHERE id='PR_1'",
                [],
            )
            .unwrap();
        fake.sync_ok();
    }
    let deleted: Option<String> = fake.query_one("SELECT deleted_at FROM prs WHERE id='PR_1'");
    assert!(deleted.is_some(), "repeated node:null drains to deleted_at");
    let left: i64 = fake.query_one("SELECT count(*) FROM quarantine WHERE id='PR_1'");
    assert_eq!(left, 0);
}

// ---------------------------------------------------------------------------
// 10. sync --pr: the typed outcomes.

#[test]
fn targeted_pr_hydrates_and_refuses_by_type() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": {"id": "PR_1"}},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );

    // Unknown archive, resolved through PR_ID, hydrated and witnessed.
    let (code, doc, stderr) = fake.run(&["sync", "--pr", "o/n#1"]);
    assert_eq!(code, 0, "{stderr}");
    let doc = doc.unwrap();
    assert_eq!(doc["sync"]["pr"]["outcome"], "hydrated");
    assert_eq!(doc["sync"]["pr"]["verified"], true);
    let verified: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert!(
        verified.is_some(),
        "a witness-complete --pr stamps verified_at"
    );
    // No discovery ran, and no watermark exists: --pr can never advance one.
    let states: i64 = fake.query_one("SELECT count(*) FROM sync_state");
    assert_eq!(states, 0, "no WindowComplete, no watermark, ever");

    // Not in config: USER_INPUT, the one enforcement point.
    let (code, doc, _) = fake.run(&["sync", "--pr", "other/repo#5"]);
    assert_eq!(code, 2);
    assert_eq!(doc.unwrap()["error"]["code"], "USER_INPUT");

    // Nonexistent number: both PR_ID nulls are USER_INPUT data.
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": null},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let (code, doc, _) = fake.run(&["sync", "--pr", "o/n#999"]);
    assert_eq!(code, 2);
    let doc = doc.unwrap();
    assert_eq!(doc["error"]["code"], "USER_INPUT");
    assert!(doc["error"]["message"].as_str().unwrap().contains("999"));

    // Filter-excluded: refused, and the archive is untouched.
    let mut bot = Pr::new("PR_B", 7, "2026-07-20T11:00:00Z");
    bot.author_type = "Bot";
    fake.write("hyd-PR_B.json", &bot.hydration());
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": {"id": "PR_B"}},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let (code, doc, _) = fake.run(&["sync", "--pr", "o/n#7"]);
    assert_eq!(code, 2);
    let doc = doc.unwrap();
    assert_eq!(doc["error"]["code"], "USER_INPUT");
    assert!(doc["error"]["message"].as_str().unwrap().contains("bots"));
    let stored: i64 = fake.query_one("SELECT count(*) FROM prs WHERE number=7");
    assert_eq!(stored, 0, "a refused --pr writes nothing");
}

// ---------------------------------------------------------------------------
// 11. The stalled-gh watchdog at sync level. Heavy: the deadline is the
// shipped 120s constant (a constant on purpose — gh.rs records the
// telemetry that would promote it), so this run takes ~2 minutes and lives
// behind --ignored / `make check-heavy`. The mechanism itself is pinned
// fast in gh.rs unit tests with an injected deadline; THIS test pins the
// pipeline consequence: a stalled call becomes a quarantined PR and a
// counted watchdog kill, never a hung sync.

#[test]
#[ignore = "~2min: waits out the shipped 120s watchdog deadline once"]
fn stalled_gh_is_killed_quarantined_and_counted() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let stall = Pr::new("PR_S", 2, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a, &stall]);
    // The fake sleeps forever on PR_S's hydration (the script sleeps when
    // a marker file exists for the id — emulate by replacing the fixture
    // with a very long sleep via sleep_every_call only for this call is
    // not expressible; instead serve PR_S through a stalling wrapper).
    fake.write_exec("gh-stall-helper", "#!/bin/sh\nsleep 300\n");
    // Replace PR_S's fixture with a name the fake cannot find, and wrap the
    // fake so that missing fixtures stall instead of failing:
    let script = GH_SCRIPT.replace(
        "if [ ! -f \"$resp\" ]; then echo \"fake gh: no fixture $resp\" >&2; exit 1; fi",
        "if [ ! -f \"$resp\" ]; then sleep 300; exit 1; fi",
    );
    fake.write_exec("gh", &script);
    fake.remove("hyd-PR_S.json");

    let started = Instant::now();
    let doc = fake.sync_ok();
    let elapsed = started.elapsed();
    assert!(
        elapsed > Duration::from_secs(115) && elapsed < Duration::from_secs(200),
        "one watchdog deadline, not a hang: {elapsed:?}"
    );
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["watchdog_kills"], 1, "{s}");
    assert_eq!(s["health"]["quarantined"], 1);
    assert_eq!(s["counts"]["fetched"], 1);
}
