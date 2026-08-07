// The deeply nested json! fixture builders exceed the default macro
// recursion limit; raising it is cosmetic, not architectural.
#![recursion_limit = "256"]

//! The sync pipeline's load-bearing suite (ROADMAP milestones 2 and 4):
//! fixture replay with zero row and zero FTS deltas (including a
//! metadata-only flip proving the FTS WHEN guards), SIGKILL at arbitrary
//! points (watermark never leads data; the redo converges), two-process
//! lock contention, floor-injection deferral across runs (window banking,
//! monotone watermark, no double hydration of banked windows), a
//! config-transition test per fingerprint case including person removal,
//! the quarantine lifecycle, and the issue stream (stream-typed dispatch,
//! per-stream watermarks, the linked-cache ownership rule).
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
q=""; id=""; owner=""; name=""; after=""; before=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-f" ]; then
    case "$a" in
      q=*) q="${a#q=}";;
      id=*) id="${a#id=}";;
      owner=*) owner="${a#owner=}";;
      name=*) name="${a#name=}";;
      after=*) after="${a#after=}";;
      before=*) before="${a#before=}";;
    esac
  fi
  prev="$a"
done
run=$(cat "$dir/run_n" 2>/dev/null || echo 1)
case "$doc" in
  *'search(type: ISSUE'*)
    case "$q" in
      *'is:issue'*)
        seqf="$dir/idisc_seq_$run"; s=$(cat "$seqf" 2>/dev/null || echo 0); echo $((s+1)) > "$seqf"
        echo "IDISC|run=$run|seq=$s|q=$q" >> "$dir/calls.log"
        resp="$dir/idisc-$run-$s.json"
        [ -f "$resp" ] || resp="$dir/idisc-default.json"
        ;;
      *)
        seqf="$dir/disc_seq_$run"; s=$(cat "$seqf" 2>/dev/null || echo 0); echo $((s+1)) > "$seqf"
        echo "DISC|run=$run|seq=$s|q=$q" >> "$dir/calls.log"
        resp="$dir/disc-$run-$s.json"
        [ -f "$resp" ] || resp="$dir/disc-default.json"
        ;;
    esac
    ;;
  *'... on Issue'*'comments(first: 100'*)
    echo "ICPAGE|run=$run|id=$id|after=$after" >> "$dir/calls.log"
    resp="$dir/icpage-$id-$after.json"
    [ -f "$resp" ] || resp="$dir/icpage-$id.json"
    ;;
  *'labels(first: 100'*)
    echo "IHYD|run=$run|id=$id" >> "$dir/calls.log"
    if [ -f "$dir/stderr-$id" ]; then cat "$dir/stderr-$id" >&2; exit 1; fi
    resp="$dir/ihyd-$id.json"
    ;;
  *'PullRequestReviewThread'*)
    echo "TBODIES|run=$run|id=$id" >> "$dir/calls.log"
    resp="$dir/tbodies-$id.json"
    ;;
  *', before: $before)'*)
    echo "TAIL|run=$run|id=$id|before=$before" >> "$dir/calls.log"
    resp="$dir/tail-$id-$before.json"
    [ -f "$resp" ] || resp="$dir/tail-$id.json"
    ;;
  *'comments(first: 100'*)
    echo "CPAGE|run=$run|id=$id|after=$after" >> "$dir/calls.log"
    resp="$dir/cpage-$id.json"
    ;;
  *'comments(last: '*)
    echo "REFRESH|run=$run|id=$id" >> "$dir/calls.log"
    resp="$dir/refresh-$id.json"
    [ -f "$resp" ] || resp="$dir/hyd-$id.json"
    ;;
  *'nodes { id createdAt'*)
    echo "SKELPAGE|run=$run|id=$id|after=$after" >> "$dir/calls.log"
    resp="$dir/skelpage-$id.json"
    ;;
  *'reviewThreads(first: 100'*)
    echo "TPAGE|run=$run|id=$id|after=$after" >> "$dir/calls.log"
    resp="$dir/tpage-$id.json"
    ;;
  *'pullRequest(number:'*)
    echo "PRID|run=$run|owner=$owner|name=$name" >> "$dir/calls.log"
    resp="$dir/prid.json"
    ;;
  *)
    echo "HYD|run=$run|id=$id" >> "$dir/calls.log"
    if [ -f "$dir/stderr-$id" ]; then cat "$dir/stderr-$id" >&2; exit 1; fi
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
        // The issue stream's discovery default: empty. Present from the
        // start so a project-scope test that only exercises PRs gets an
        // empty issue walk instead of a missing-fixture transport failure;
        // issue tests overwrite it via install_issues.
        fake.write("idisc-default.json", &discovery(&[], None, 4000));
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

    fn refreshes(&self, run: u32) -> Vec<String> {
        self.calls()
            .iter()
            .filter_map(|l| {
                l.strip_prefix(&format!("REFRESH|run={run}|id="))
                    .map(str::to_string)
            })
            .collect()
    }

    fn issue_hydrations(&self, run: u32) -> Vec<String> {
        self.calls()
            .iter()
            .filter_map(|l| {
                l.strip_prefix(&format!("IHYD|run={run}|id="))
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
    /// tests that assert on it. sync_runs is EXCLUDED on purpose: it gains
    /// one row per run BY DESIGN, so it can never sit inside a "replay
    /// writes nothing" comparison — the replay test asserts its delta
    /// columns are zero instead.
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
    /// reviewRequests arrives error-masked (null): the schema-nullable
    /// connection GraphQL bubbles a failed sub-resolver into.
    mask_requests: bool,
    /// comments pageInfo claims another page (with no cursor to walk):
    /// the witness-withholding shape.
    comments_has_next: bool,
    /// A real follow-up cursor: the walkable multi-page shape.
    comments_cursor: Option<&'static str>,
    threads_has_next: bool,
    threads_cursor: Option<&'static str>,
    thread_resolved: bool,
    no_thread: bool,
    /// closingIssuesReferences arrives error-masked (null).
    mask_closing: bool,
    closed_at: Option<&'static str>,
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
            mask_requests: false,
            comments_has_next: false,
            comments_cursor: None,
            threads_has_next: false,
            threads_cursor: None,
            thread_resolved: false,
            no_thread: false,
            mask_closing: false,
            closed_at: None,
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
                    "closedAt": if self.state == "CLOSED" {
                        json!(self.closed_at.unwrap_or("2026-07-21T00:00:00Z"))
                    } else { Value::Null },
                    "commits": {"nodes": [{"commit": {
                        "oid": "0123456789012345678901234567890123456789",
                        "committedDate": "2026-07-09T00:00:00Z"}}]},
                    "reviewRequests": if self.mask_requests { Value::Null } else {
                        json!({"totalCount": 1, "nodes": [
                            {"requestedReviewer": {"login": "rev"}}]})
                    },
                    "latestOpinionatedReviews": {"totalCount": 1, "nodes": [{
                        "id": format!("REV_{}", self.id), "state": "APPROVED",
                        "submittedAt": "2026-07-11T00:00:00Z", "body": "lgtm",
                        "url": "https://github.com/r", "authorAssociation": "MEMBER",
                        "author": author("rev", "User")}]},
                    "closingIssuesReferences": if self.mask_closing { Value::Null } else { json!({"totalCount": 1, "nodes": [{
                        "id": format!("I_{}", self.id), "number": self.number + 100,
                        "title": "linked issue", "state": "OPEN", "body": "issue body",
                        "updatedAt": "2026-07-08T00:00:00Z",
                        "author": author("dora", "User"), "authorAssociation": "NONE",
                        "url": "https://github.com/i",
                        "repository": {"nameWithOwner": self.repo}}]}) },
                    "comments": {"totalCount": comments.len() + usize::from(self.comments_has_next),
                        "pageInfo": {"hasNextPage": self.comments_has_next,
                                     "endCursor": self.comments_cursor},
                        "nodes": comments},
                    "reviewThreads": {
                        "totalCount": (1 - usize::from(self.no_thread)) + usize::from(self.threads_has_next),
                        "pageInfo": {"hasNextPage": self.threads_has_next,
                                     "endCursor": self.threads_cursor},
                        "nodes": if self.no_thread { json!([]) } else { json!([{
                            "id": format!("T_{}", self.id), "isResolved": self.thread_resolved,
                            "isOutdated": false, "path": "src/x.rs", "line": 10,
                            "comments": {"totalCount": 1, "nodes": [{
                                "id": format!("TC_{}", self.id), "body": "thread comment",
                                "createdAt": "2026-07-10T01:00:00Z", "lastEditedAt": null,
                                "url": "https://github.com/t", "isMinimized": false,
                                "authorAssociation": "NONE", "author": author("erin", "User")}]}}]) }}
                },
                "rateLimit": rate_limit(self.remaining)
            }
        })
        .to_string()
    }
}

impl Pr {
    /// The REFRESH_PR response for this PR: the hydration's node with the
    /// two big connections in their refresh forms — comments as a fully-
    /// observed tail (backward pageInfo, hasPreviousPage false) and
    /// skeleton threads (nested comments without bodies). A test that
    /// needs a walk-back or an un-fetched middle writes its own
    /// refresh-/tail- fixtures instead.
    fn refresh(&self) -> String {
        let mut v: Value = serde_json::from_str(&self.hydration()).unwrap();
        let node = &mut v["data"]["node"];
        node["comments"]["pageInfo"] = json!({
            "hasPreviousPage": false,
            "startCursor": if self.comment_ids.is_empty() { Value::Null } else { json!("cur0") }
        });
        if let Some(threads) = node["reviewThreads"]["nodes"].as_array_mut() {
            for t in threads {
                if let Some(cs) = t["comments"]["nodes"].as_array_mut() {
                    for c in cs {
                        c.as_object_mut().unwrap().remove("body");
                    }
                }
            }
        }
        v.to_string()
    }
}

fn discovery(hits: &[&Pr], issue_count: Option<i64>, remaining: u32) -> String {
    discovery_nodes(
        hits.iter().map(|p| p.hit()).collect(),
        issue_count,
        remaining,
    )
}

fn discovery_nodes(nodes: Vec<Value>, issue_count: Option<i64>, remaining: u32) -> String {
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

/// The issue-stream sibling of [`Pr`]: builds DISCOVERY hits and
/// HYDRATE_ISSUE responses shaped exactly like the live documents
/// (parse.rs pins the shape offline; capture.rs re-captures it live).
struct Issue {
    id: &'static str,
    number: i64,
    updated_at: &'static str,
    title: String,
    body: String,
    state: &'static str,
    author_login: String,
    author_type: &'static str,
    labels: Vec<&'static str>,
    assignees: Vec<&'static str>,
    comment_ids: Vec<String>,
    remaining: u32,
    repo: String,
    /// comments pageInfo claims another page (with no cursor to walk):
    /// the witness-withholding shape.
    comments_has_next: bool,
    /// A real follow-up cursor: the walkable multi-page shape.
    comments_cursor: Option<&'static str>,
    /// labels arrives error-masked (null): the schema-nullable connection
    /// GraphQL bubbles a failed sub-resolver into (unlike assignees).
    mask_labels: bool,
}

impl Issue {
    fn new(id: &'static str, number: i64, updated_at: &'static str) -> Issue {
        Issue {
            id,
            number,
            updated_at,
            title: format!("issue title {number}"),
            body: format!("issue body {number}"),
            state: "OPEN",
            author_login: "alice".into(),
            author_type: "User",
            labels: vec!["bug"],
            assignees: vec![],
            comment_ids: vec![format!("IC_{id}")],
            remaining: 4000,
            repo: "o/n".into(),
            comments_has_next: false,
            comments_cursor: None,
            mask_labels: false,
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
                    "url": "https://github.com/x", "isMinimized": false,
                    "authorAssociation": "NONE", "author": author("carol", "User")
                })
            })
            .collect();
        let labels: Value = if self.mask_labels {
            Value::Null
        } else {
            let nodes: Vec<Value> = self.labels.iter().map(|l| json!({"name": l})).collect();
            json!({"totalCount": nodes.len(), "nodes": nodes})
        };
        let assignees: Vec<Value> = self.assignees.iter().map(|a| json!({"login": a})).collect();
        json!({
            "data": {
                "node": {
                    "id": self.id, "number": self.number, "title": self.title,
                    "body": self.body, "state": self.state,
                    "url": format!("https://github.com/{}/issues/{}", self.repo, self.number),
                    "author": author(&self.author_login, self.author_type),
                    "authorAssociation": "NONE",
                    "repository": {"nameWithOwner": self.repo},
                    "createdAt": "2026-07-01T00:00:00Z",
                    "updatedAt": self.updated_at,
                    "labels": labels,
                    "assignees": {"totalCount": assignees.len(), "nodes": assignees},
                    "comments": {"totalCount": comments.len() + usize::from(self.comments_has_next),
                        "pageInfo": {"hasNextPage": self.comments_has_next,
                                     "endCursor": self.comments_cursor},
                        "nodes": comments}
                },
                "rateLimit": rate_limit(self.remaining)
            }
        })
        .to_string()
    }
}

fn install_issues(fake: &Fake, issues: &[&Issue]) {
    fake.write(
        "idisc-default.json",
        &discovery_nodes(issues.iter().map(|i| i.hit()).collect(), None, 4000),
    );
    for issue in issues {
        fake.write(&format!("ihyd-{}.json", issue.id), &issue.hydration());
    }
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
        // The refresh form beside every hydration: a rehydration of a PR
        // this suite verified in an earlier run dispatches to the tail
        // path, and a missing refresh fixture would read as a transport
        // failure (quarantine), not a cost fallback. Tests that never
        // re-verify a PR simply never serve it.
        fake.write(&format!("refresh-{}.json", pr.id), &pr.refresh());
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
    // Backdate the stamps two days (well inside the 7d+jitter re-verify
    // period, so nothing is due): the replay must not move them — an
    // unchanged, already-verified overlap re-hydration that re-stamped
    // would be exactly the per-PR-per-run row churn the stamp rule forbids.
    let recent = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(2)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE prs SET verified_at = ?1",
            rusqlite::params![recent.as_str()],
        )
        .unwrap();

    // Second run against the byte-identical remote: the diff gate must
    // skip every row, every observation, every FTS write.
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 0, "zero row deltas: {s}");
    assert_eq!(s["counts"]["unchanged"], 2);
    assert_eq!(s["counts"]["observations"], 0);
    assert_eq!(s["counts"]["soft_deleted"], 0);
    // The cost group is deterministic with fixture responses: one
    // discovery page and two REFRESH documents (both PRs carry a
    // witnessed baseline from run 1, and the fully-observed tail
    // balances), each costing 1 point — the single-page-PR-costs-one-call
    // claim, pinned.
    assert_eq!(s["cost"]["subprocess_count"], 3);
    assert_eq!(s["cost"]["rate_cost"], 3);
    assert_eq!(
        s["refresh"]["tail_hits"], 2,
        "both rehydrations tail-hit: {s}"
    );
    assert_eq!(s["refresh"]["full_walks"], 0);
    assert_eq!(
        s["refresh"]["bodies_skipped"], 2,
        "each PR's one thread comment resolved from the archive"
    );
    let stamps: i64 = fake.query_one(&format!(
        "SELECT count(*) FROM prs WHERE verified_at = '{}'",
        recent.as_str()
    ));
    assert_eq!(stamps, 2, "replay must not move verified_at");
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

    // sync_runs: one flat row per completed run, and the second row IS the
    // replay-idempotence detector at rest — an unchanged remote with any
    // nonzero delta column here is the regression the table exists to catch.
    let runs: i64 = fake.query_one("SELECT count(*) FROM sync_runs");
    assert_eq!(runs, 2, "one sync_runs row per completed run");
    let (upserted, unchanged, observations, errors, intercept): (i64, i64, i64, i64, Option<i64>) =
        fake.db()
            .query_row(
                "SELECT upserted, unchanged, observations, errors, overhead_intercept_ms \
             FROM sync_runs ORDER BY seq DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
    assert_eq!(
        (upserted, unchanged, observations, errors),
        (0, 2, 0, 0),
        "the replay run's sync_runs row shows zero deltas"
    );
    // The intercept VALUE is wall-clock and unassertable, but its
    // PRESENCE is structural: the run made three data-bearing calls
    // whose response sizes differ (discovery page vs hydration docs),
    // so a regression exists and the column must be non-NULL — a
    // sample-collection regression (e.g. sampling only killed calls)
    // yields zero samples and NULL here.
    assert!(
        intercept.is_some(),
        "three differing-size fixture calls must yield an intercept"
    );
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

    // State flips CLOSED, the one comment flips is_minimized, and the
    // review thread resolves — the exact quiet-mutation shapes the
    // skeleton walk exists to record. Title and body are byte-identical,
    // so FTS must not move.
    a.state = "CLOSED";
    a.minimized = true;
    a.thread_resolved = true;
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
    let resolved: i64 = fake.query_one("SELECT is_resolved FROM review_threads WHERE id='T_PR_1'");
    assert_eq!(resolved, 1, "the thread resolve landed");
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

    // The whole thread vanishes upstream: the (witnessed) thread sweep
    // soft-deletes the thread AND its review comment.
    a.no_thread = true;
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["soft_deleted"], 2, "thread + its comment: {s}");
    let t_gone: Option<String> =
        fake.query_one("SELECT deleted_at FROM review_threads WHERE id='T_PR_1'");
    assert!(t_gone.is_some());
    let tc_gone: Option<String> =
        fake.query_one("SELECT deleted_at FROM comments WHERE id='TC_PR_1'");
    assert!(tc_gone.is_some());
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
    // A2 is touched upstream BETWEEN discovery and hydration: its
    // hydration response carries a far-future updatedAt. The watermark
    // fold must take the discovery-time value — folding this one would
    // bank window A past window B before B ever runs, and the deferral
    // below would then skip B's items forever (D1 panel finding).
    {
        let mut drifted: Value = serde_json::from_str(&a2.hydration()).unwrap();
        drifted["data"]["node"]["updatedAt"] = json!("2026-07-25T00:00:00Z");
        fake.write("hyd-PR_A2.json", &drifted.to_string());
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
    assert_eq!(
        wm, "2026-07-10T06:00:00Z",
        "banked at window A's boundary — A2's drifted hydration time never folds"
    );
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
    // Splice in a masked hit (item-level null: a visibility domain the
    // viewer cannot see into): it has no id, counts as seen, and is
    // disclosed — never an error, never a watermark contribution.
    let mut d: Value =
        serde_json::from_str(&std::fs::read_to_string(fake.dir.join("disc-default.json")).unwrap())
            .unwrap();
    d["data"]["search"]["nodes"]
        .as_array_mut()
        .unwrap()
        .push(Value::Null);
    d["data"]["search"]["issueCount"] = json!(5);
    fake.write("disc-default.json", &d.to_string());

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["fetched"], 1);
    assert_eq!(s["counts"]["filtered"], 3, "{s}");
    assert_eq!(s["health"]["masked_hits"], 1, "{s}");
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
    let mut cfg = base_config();
    cfg["retry_attempts"] = json!(2); // one in-call retry: sleeps become visible
    fake.config(&cfg);
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let broken = Pr::new("PR_X", 9, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a, &broken]);
    // PR_X's hydration fails outright (the fake exits 1 on a missing file).
    fake.remove("hyd-PR_X.json");

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 1, "{s}");
    assert_eq!(s["counts"]["fetched"], 1);
    assert_eq!(
        s["cost"]["sleeps"], 1,
        "the one transient retry slept once, and the writer merged it: {s}"
    );
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
    let doc = fake.sync_ok();
    assert!(fake.hydrations(3).contains(&"PR_X".to_string()));
    // Two fetches: PR_1's window rehydration and PR_X's resolved retry —
    // the retry path's own counter increment, pinned.
    assert_eq!(fake.repo_summary(&doc, "o/n")["counts"]["fetched"], 2);
    let left: i64 = fake.query_one("SELECT count(*) FROM quarantine");
    assert_eq!(left, 0, "resolved retry deletes the record");
    let present: i64 = fake.query_one("SELECT count(*) FROM prs WHERE id='PR_X'");
    assert_eq!(present, 1);

    // node:null drain: PR_1 vanishes upstream. Each aged retry re-nulls;
    // the third attempt drains to deleted_at and retires the record. Both
    // documents must agree it vanished: PR_1 is verified, so the
    // rediscovery routes through REFRESH first (node:null there is the
    // same Vanished outcome), and the aged retries full-walk HYDRATE.
    fake.write("hyd-PR_1.json", r#"{"data":{"node":null}}"#);
    fake.write("refresh-PR_1.json", r#"{"data":{"node":null}}"#);
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

    // A transport-broken hydration quarantines with the transient class
    // and one consumed attempt.
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": {"id": "PR_T"}},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let (code, doc, _) = fake.run(&["sync", "--pr", "o/n#9"]); // no hyd-PR_T.json
    assert_eq!(code, 2);
    assert_eq!(doc.unwrap()["error"]["code"], "TRANSIENT");
    let (attempts, class): (i64, String) = fake
        .db()
        .query_row(
            "SELECT attempts, error_class FROM quarantine WHERE id='PR_T'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((attempts, class.as_str()), (1, "transient"));

    // The vanished-PR arc: each explicit demand consumes one retry attempt
    // through backoff; the third node:null drains.
    fake.write("hyd-PR_D.json", r#"{"data":{"node":null}}"#);
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": {"id": "PR_D"}},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    for expected_attempts in [1i64, 2] {
        let (code, doc, _) = fake.run(&["sync", "--pr", "o/n#8"]);
        assert_eq!(code, 2);
        let doc = doc.unwrap();
        assert_eq!(doc["error"]["code"], "TRANSIENT");
        let attempts: i64 = fake.query_one("SELECT attempts FROM quarantine WHERE id='PR_D'");
        assert_eq!(attempts, expected_attempts, "{doc}");
    }
    let (code, doc, _) = fake.run(&["sync", "--pr", "o/n#8"]);
    assert_eq!(code, 2);
    let doc = doc.unwrap();
    assert_eq!(
        doc["error"]["code"], "USER_INPUT",
        "the third null drains: {doc}"
    );
    let rows: i64 = fake.query_one("SELECT count(*) FROM quarantine WHERE id='PR_D'");
    assert_eq!(rows, 0, "the drained record retires");

    // A drained row's stale node id must not restart the cycle: the next
    // --pr consults the live lookup, which now says the PR is gone —
    // USER_INPUT immediately, no fresh quarantine (closure-pass S2).
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": null},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let (code, doc, _) = fake.run(&["sync", "--pr", "o/n#8"]);
    assert_eq!(code, 2);
    assert_eq!(doc.unwrap()["error"]["code"], "USER_INPUT");
    let rows: i64 = fake.query_one("SELECT count(*) FROM quarantine WHERE id='PR_D'");
    assert_eq!(rows, 0, "no cycle restart after the drain");
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

// ---------------------------------------------------------------------------
// 12. Truncation lifecycle: a withheld witness never stamps, never sweeps,
// never drops demand rows — and a later complete refetch heals it all.

#[test]
fn truncation_never_stamps_sweeps_or_drops_requests_and_heals() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into(), "C_b".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();
    let stamped: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert!(stamped.is_some(), "run 1 is witness-complete");
    let req: i64 = fake.query_one("SELECT count(*) FROM review_requests");
    assert_eq!(req, 1);
    // Backdate the stamp to a sentinel: second-granular wall clocks make
    // same-second re-stamps invisible, and the point of the next three
    // runs is exactly WHEN the stamp moves. Recent (two days), not epoch:
    // an old stamp would make re-verify due, and this test isolates the
    // witness rules from the tier (the tail-then-reverify interplay has
    // its own test).
    let v1_ts = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(2)
        .unwrap();
    let v1 = v1_ts.as_str();
    fake.db()
        .execute("UPDATE prs SET verified_at = ?1", rusqlite::params![v1])
        .unwrap();

    // Run 2: the requests connection arrives error-masked (null). The PR
    // lands truncated; the stored request row is NOT deleted (fail-open —
    // a dropped row could only under-fill a demand); verified_at holds.
    a.mask_requests = true;
    a.mask_closing = true;
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    assert_eq!(fake.repo_summary(&doc, "o/n")["health"]["truncated"], 1);
    let req: i64 = fake.query_one("SELECT count(*) FROM review_requests");
    assert_eq!(req, 1, "a masked connection must not delete demand rows");
    let api_refs: i64 = fake.query_one("SELECT count(*) FROM refs WHERE source='api'");
    assert_eq!(
        api_refs, 1,
        "a masked closing connection must not delete api refs"
    );
    let v2: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert_eq!(v2.as_deref(), Some(v1), "no witness, no stamp");

    // Run 3: comments claim another page that cannot be walked; C_b is
    // absent from the visible page. Truncation must never read as
    // deletion: C_b stays live.
    a.mask_requests = false;
    a.mask_closing = false;
    a.comments_has_next = true;
    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    assert_eq!(fake.repo_summary(&doc, "o/n")["health"]["truncated"], 1);
    let gone: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_b'");
    assert!(gone.is_none(), "an incomplete connection must not sweep");

    // Run 4: the connection completes with C_b really gone — NOW it
    // sweeps, truncated heals to 0, and the witness re-stamps.
    a.comments_has_next = false;
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 0, "{s}");
    assert_eq!(s["counts"]["soft_deleted"], 1);
    let gone: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_b'");
    assert!(gone.is_some(), "the witnessed refetch sweeps");
    let v4: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert_ne!(v4.as_deref(), Some(v1), "the heal re-stamps");
}

// ---------------------------------------------------------------------------
// 13. Re-verify: the tier catches quiet mutations discovery cannot see,
// and the closed tier is bounded by the lookback.

#[test]
fn reverify_catches_quiet_mutations_within_its_tiers() {
    let fake = Fake::new();
    fake.config(&base_config());
    let open = Pr::new("PR_O", 1, "2026-07-20T10:00:00Z");
    let mut recent = Pr::new("PR_RC", 2, "2026-07-20T11:00:00Z");
    recent.state = "CLOSED"; // closed_at 2026-07-21: inside the lookback
    let mut ancient = Pr::new("PR_AC", 3, "2026-07-20T12:00:00Z");
    ancient.state = "CLOSED";
    ancient.closed_at = Some("2020-01-01T00:00:00Z"); // far outside it
    install_prs(&fake, &[&open, &recent, &ancient]);
    fake.sync_ok();

    // Age every verified_at past both tiers' period + max jitter (7+7,
    // 30+30 days), then edit the open PR's comment WITHOUT bumping its
    // updatedAt — the exact quiet-mutation shape — and make run 2's
    // discovery come back empty so re-verify is the only path to it.
    let aged = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(61)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE prs SET verified_at = ?1",
            rusqlite::params![aged.as_str()],
        )
        .unwrap();
    let mut edited = Pr::new("PR_O", 1, "2026-07-20T10:00:00Z");
    edited.comment_ids = vec!["C_PR_O".into()];
    let hydration = edited
        .hydration()
        .replace("comment C_PR_O", "comment C_PR_O, edited");
    fake.write("hyd-PR_O.json", &hydration);
    fake.write("disc-2-0.json", &discovery(&[], Some(0), 4000));

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["reverified"], 2, "open + recent-closed: {s}");
    assert_eq!(
        s["refresh"]["quiet_mutations_found"], 1,
        "only the edited PR changed: {s}"
    );
    let mut got = fake.hydrations(2);
    got.sort();
    assert_eq!(
        got,
        vec!["PR_O", "PR_RC"],
        "the ancient closed PR is outside the lookback tier"
    );
    let body: String = fake.query_one("SELECT body FROM comments WHERE id='C_PR_O'");
    assert!(body.ends_with("edited"), "the quiet edit landed: {body}");
    let v: String = fake
        .query_one::<Option<String>>("SELECT verified_at FROM prs WHERE number=1")
        .unwrap();
    assert_ne!(v, aged.as_str(), "an explicit re-verify always re-stamps");
}

// ---------------------------------------------------------------------------
// 14. Typed stream endings: a hard discovery failure is Failed (recorded,
// not deferred); a primary rate limit mid-stream folds into the floor's
// defer path (deferred, not an error).

#[test]
fn discovery_failure_is_failed_not_deferred() {
    let fake = Fake::new();
    fake.config(&base_config());
    // No disc-default.json: discovery exits 1 with noise on stderr.
    let (code, doc, _) = fake.run(&["sync"]);
    assert_eq!(code, 0, "per-repo failures are summary content, not exits");
    let doc = doc.unwrap();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["deferred_at_floor"], false);
    let errors = s["health"]["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1, "{s}");
    assert!(
        errors[0].as_str().unwrap().starts_with("TRANSIENT"),
        "classified, never string-matched: {errors:?}"
    );
}

#[test]
fn rate_exhausted_mid_stream_defers_typed() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let b = Pr::new("PR_2", 2, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a, &b]);
    // PR_2's call relays the API's primary-limit text: typed RateExhausted,
    // never retried, folded into the floor's defer path.
    fake.write("stderr-PR_2", "API rate limit exceeded for user ID 1.\n");
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["deferred_at_floor"], true, "{s}");
    assert_eq!(
        s["health"]["errors"].as_array().unwrap().len(),
        0,
        "a deferral is not an error: {s}"
    );
    let calls = fake.hydrations(1);
    assert_eq!(
        calls.iter().filter(|id| *id == "PR_2").count(),
        1,
        "RateExhausted must not burn retries: {calls:?}"
    );
}

// ---------------------------------------------------------------------------
// 15. The unsplittable-capped endgame: the stream HALTS so no later
// window's Done can advance the watermark past the lost tail.

#[test]
fn unsplittable_capped_window_halts_the_stream() {
    let fake = Fake::new();
    let mut cfg = base_config();
    cfg["lookback_days"] = json!(1); // bounds the halving depth (~16)
    fake.config(&cfg);
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    fake.write("hyd-PR_1.json", &a.hydration());
    // Every window, at every depth, reports far more hits than it returns:
    // capped, splittable until the 2s floor, then halted.
    fake.write("disc-default.json", &discovery(&[&a], Some(1500), 4000));

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["discovery_truncated"], 1, "{s}");
    assert_eq!(fake.hydrations(1), vec!["PR_1"], "the leaf still hydrates");
    let checked: Option<String> =
        fake.query_one("SELECT last_checked_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(checked.is_none(), "a halted stream never claims freshness");
    let wm: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert_eq!(
        wm, "1970-01-01T00:00:00Z",
        "the watermark pins below the lost tail (first-contact sentinel)"
    );
}

// ---------------------------------------------------------------------------
// 16. The floor boundary is strict: remaining == floor proceeds,
// remaining < floor defers.

#[test]
fn floor_boundary_is_strict() {
    for (remaining, defers) in [(500u32, false), (499, true)] {
        let fake = Fake::new();
        fake.config(&base_config()); // rate_limit_floor: 500 default
        let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
        a.remaining = remaining; // observed after the FIRST hydration
        let b = Pr::new("PR_2", 2, "2026-07-20T11:00:00Z");
        install_prs(&fake, &[&a, &b]);
        let doc = fake.sync_ok();
        let s = fake.repo_summary(&doc, "o/n");
        assert_eq!(
            s["health"]["deferred_at_floor"],
            json!(defers),
            "remaining={remaining}: {s}"
        );
        let expected = if defers { 1 } else { 2 };
        assert_eq!(s["counts"]["fetched"], expected, "remaining={remaining}");
    }
}

// ---------------------------------------------------------------------------
// 17. Stream-typed dispatch, both directions (the B2 panel's S1, upgraded
// for milestone 4): the PR walk's terms never carry is:issue and an Issue
// id never reaches HYDRATE_PR — before the typed terms, every issue in a
// project repo became an eternal parse-class quarantine row — and,
// symmetrically, a PR id never reaches HYDRATE_ISSUE.

#[test]
fn default_project_scope_never_discovers_issues_into_pr_hydration() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}], // issues default ON
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    let i = Issue::new("I_9", 9, "2026-07-20T11:00:00Z");
    install_issues(&fake, &[&i]);

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 0, "{s}");
    assert_eq!(s["counts"]["fetched"], 2, "one PR + one issue");
    assert_eq!(fake.hydrations(1), vec!["PR_1"], "PR walk: PR ids only");
    assert_eq!(
        fake.issue_hydrations(1),
        vec!["I_9"],
        "issue walk: issue ids only, through HYDRATE_ISSUE"
    );
    for call in fake.calls() {
        if call.starts_with("DISC|") {
            assert!(
                !call.contains("is:issue"),
                "the PR walk must never emit an issue term: {call}"
            );
        }
        if call.starts_with("IDISC|") {
            assert!(
                call.contains("is:issue"),
                "the issue walk's terms are issue-typed: {call}"
            );
        }
    }
}

// 17a. The issue stream end to end: hydration writes the row (labels and
// assignees canonical), its comments land under parent_kind='issue', FTS
// serves them, the (repo,'issue') watermark advances independently of the
// PR stream's, and an unchanged replay writes nothing — the same replay
// idempotence bar the PR stream clears.

#[test]
fn issue_stream_hydrates_populates_fts_and_replays_clean() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    let pr = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&pr]);
    let mut a = Issue::new("I_1", 11, "2026-07-20T12:00:00Z");
    a.labels = vec!["zeta", "bug", "bug"]; // canonicalized: sorted, deduped
    a.assignees = vec!["bob"];
    a.body = "the frobnicator misfires".into();
    let b = Issue::new("I_2", 12, "2026-07-19T09:00:00Z");
    install_issues(&fake, &[&a, &b]);

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["fetched"], 3, "{s}");
    assert_eq!(
        s["health"],
        json!({
            "truncated": 0, "quarantined": 0, "discovery_truncated": 0,
            "deferred_at_floor": false, "watchdog_kills": 0, "masked_hits": 0,
            "rate_limit_unknown": 0, "errors": []
        })
    );

    let labels: String = fake.query_one(
        "SELECT labels FROM issues WHERE repo='o/n' AND number=11 AND hydration_source='stream'",
    );
    assert_eq!(labels, r#"["bug","zeta"]"#, "canonical: sorted, deduped");
    let assignees: String =
        fake.query_one("SELECT assignees FROM issues WHERE repo='o/n' AND number=11");
    assert_eq!(assignees, r#"["bob"]"#);
    let verified: i64 = fake.query_one(
        "SELECT count(*) FROM issues WHERE repo='o/n' AND hydration_source='stream' \
         AND verified_at IS NOT NULL AND truncated = 0",
    );
    assert_eq!(verified, 2, "witnessed hydrations stamp");
    let issue_comments: i64 = fake.query_one(
        "SELECT count(*) FROM comments c JOIN issues i ON c.parent = i.pk \
         WHERE c.parent_kind = 'issue' AND i.repo = 'o/n' AND i.number IN (11, 12)",
    );
    assert_eq!(issue_comments, 2);
    let fts_hits: i64 =
        fake.query_one("SELECT count(*) FROM issues_fts WHERE issues_fts MATCH 'frobnicator'");
    assert_eq!(fts_hits, 1, "the stream writer feeds issues_fts");

    // Per-stream watermarks: each stream's max updatedAt, independently.
    let pr_wm: String = fake
        .query_one("SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    let issue_wm: String = fake.query_one(
        "SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='issue'",
    );
    assert_eq!(pr_wm, "2026-07-20T10:00:00Z");
    assert_eq!(issue_wm, "2026-07-20T12:00:00Z");

    assert_eq!(s["counts"]["upserted"], 3, "first contact writes each row");

    // Backdate the issue stamps (well inside the re-verify period, so
    // nothing is due): the replay must not move them — the PR-side stamp
    // rule, held on the issue path (upsert_issue_stream).
    let recent = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(2)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE issues SET verified_at = ?1 WHERE hydration_source = 'stream'",
            rusqlite::params![recent.as_str()],
        )
        .unwrap();

    // Replay: an unchanged remote writes no row, no observation, no FTS
    // churn — and re-hydrates nothing it already full-walked... it DOES
    // re-hydrate (the overlap window rediscovers), but the diff gate makes
    // every write a no-op.
    let dump1 = fake.dump();
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 0, "replay upserts nothing: {s}");
    assert_eq!(s["counts"]["unchanged"], 3, "every replayed row is a no-op");
    let stamps: i64 = fake.query_one(&format!(
        "SELECT count(*) FROM issues WHERE hydration_source = 'stream' AND verified_at = '{}'",
        recent.as_str()
    ));
    assert_eq!(stamps, 2, "replay must not move issue verified_at");
    assert_eq!(dump1, fake.dump(), "byte-identical archive after replay");

    // A real change on a verified, untruncated row: the witnessed refetch
    // both writes the field and MOVES the stamp — the field_changed arm of
    // the stamp rule standing alone (no truncation, no re-verify origin).
    let mut a2 = Issue::new("I_1", 11, "2026-07-23T00:00:00Z");
    a2.labels = vec!["zeta", "bug", "bug"];
    a2.assignees = vec!["bob"];
    a2.body = "the frobnicator misfires".into();
    a2.title = "retitled upstream".into();
    install_issues(&fake, &[&a2, &b]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 1, "{s}");
    let (title, verified_at): (String, Option<String>) = fake
        .db()
        .query_row(
            "SELECT title, verified_at FROM issues WHERE repo='o/n' AND number=11",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(title, "retitled upstream");
    assert_ne!(
        verified_at.as_deref(),
        Some(recent.as_str()),
        "a changed witnessed refetch re-stamps"
    );
}

// 17a'. Off means off, at both gates: a project repo with issues: false
// runs no issue discovery (zero IDISC calls — the walk gate) and grows no
// (repo,'issue') sync_state row (the plan gate; an empty walk would still
// commit one, flipping _meta config_pending forever). The discriminating
// input for the plan-gate's && — either operand alone would pass a
// project-with-issues-off config through.

#[test]
fn issues_off_project_repo_runs_no_issue_stream() {
    let fake = Fake::new();
    fake.config(&base_config()); // project scope, issues: false
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    let i = Issue::new("I_1", 11, "2026-07-20T11:00:00Z");
    install_issues(&fake, &[&i]); // present but must never be asked for

    fake.sync_ok();
    assert!(
        fake.calls().iter().all(|c| !c.starts_with("IDISC|")),
        "issues off: no issue discovery call"
    );
    let issue_rows: i64 =
        fake.query_one("SELECT count(*) FROM sync_state WHERE repo='o/n' AND stream='issue'");
    assert_eq!(issue_rows, 0, "issues off: no issue stream state row");
}

// 17b. Issue-stream filters: a bot-authored issue is skipped at discovery
// (no hydration subprocess), counted as filtered, and STILL advances the
// issue watermark — declined, not unfetched, exactly the PR-stream rule.

#[test]
fn issue_stream_filters_bots_and_still_advances() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}], // bots default OFF
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let mut bot = Issue::new("I_B", 21, "2026-07-22T00:00:00Z");
    bot.author_login = "dependabot".into();
    bot.author_type = "Bot";
    let human = Issue::new("I_H", 22, "2026-07-21T00:00:00Z");
    install_issues(&fake, &[&bot, &human]);

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["filtered"], 1, "{s}");
    assert_eq!(s["counts"]["fetched"], 1);
    assert_eq!(
        fake.issue_hydrations(1),
        vec!["I_H"],
        "the filtered issue costs discovery only"
    );
    let issue_wm: String = fake.query_one(
        "SELECT last_item_updated_at FROM sync_state WHERE repo='o/n' AND stream='issue'",
    );
    assert_eq!(
        issue_wm, "2026-07-22T00:00:00Z",
        "the bot issue's updatedAt advances the fold: filtered is declined, not unfetched"
    );
}

// 17c. The linked-cache upgrade, both directions of the ownership rule:
// stream hydration takes over a fill-only 'linked' row (labels land,
// verified_at stamps), and the linked writer can never downgrade it back —
// a later PR sync carrying staler linked data leaves the stream row
// untouched.

#[test]
fn issue_stream_upgrades_linked_rows_and_linked_never_downgrades() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    // Run 1: PRs only. PR_1's closingIssuesReferences plants the linked
    // cache row for issue number 101 (Pr::hydration's fixture shape).
    let pr = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&pr]);
    fake.write("idisc-1-0.json", &discovery_nodes(vec![], None, 4000));
    fake.sync_ok();
    let source: String =
        fake.query_one("SELECT hydration_source FROM issues WHERE repo='o/n' AND number=101");
    assert_eq!(source, "linked");

    // Run 1b: the linked TARGET changes upstream while the PR itself does
    // not — the freshen is the run's only write, and it must both land
    // and count (a linked-owned row is this writer's to keep fresh).
    {
        let mut refreshed: Value = serde_json::from_str(&pr.refresh()).unwrap();
        refreshed["data"]["node"]["closingIssuesReferences"]["nodes"][0]["title"] =
            json!("fresher linked title");
        fake.write("refresh-PR_1.json", &refreshed.to_string());
    }
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 1, "the freshen counts: {s}");
    let title: String = fake.query_one("SELECT title FROM issues WHERE repo='o/n' AND number=101");
    assert_eq!(title, "fresher linked title");
    fake.write("refresh-PR_1.json", &pr.refresh());

    // Run 2: the issue stream discovers the same issue; hydration upgrades.
    let mut upstream = Issue::new("I_PR_1", 101, "2026-07-22T00:00:00Z");
    upstream.title = "stream-owned title".into();
    upstream.labels = vec!["triaged"];
    install_issues(&fake, &[&upstream]);
    fake.sync_ok();
    let (source, title, labels, verified_at): (String, String, String, Option<String>) = fake
        .db()
        .query_row(
            "SELECT hydration_source, title, labels, verified_at FROM issues \
             WHERE repo='o/n' AND number=101",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(source, "stream", "hydration takes ownership");
    assert_eq!(title, "stream-owned title");
    assert_eq!(labels, r#"["triaged"]"#);
    assert!(verified_at.is_some(), "witnessed upgrade stamps");

    // Final run: PRs only again (issue stream empty this run); the PR's
    // linked reference still carries the OLD title — the fill-only writer
    // must not clobber the stream row with it. (Run 4: run 1b above
    // shifted the numbering.)
    fake.write("idisc-4-0.json", &discovery_nodes(vec![], None, 4000));
    fake.sync_ok();
    let title: String = fake.query_one("SELECT title FROM issues WHERE repo='o/n' AND number=101");
    assert_eq!(
        title, "stream-owned title",
        "linked data never downgrades a stream row"
    );
}

// 17d. Issue truncation discipline: a comments walk that cannot terminate
// lands the row truncated with no verified_at and sweeps nothing; the next
// run's complete walk heals it — witnessed, stamped, swept.

#[test]
fn issue_truncation_never_sweeps_and_heals_on_the_complete_walk() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    // Run 1: complete — two comments land.
    let mut a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    a.comment_ids = vec!["IC_1".into(), "IC_2".into()];
    install_issues(&fake, &[&a]);
    fake.sync_ok();
    let live: i64 = fake.query_one(
        "SELECT count(*) FROM comments WHERE parent_kind='issue' AND deleted_at IS NULL",
    );
    assert_eq!(live, 2);
    // Pin the run-1 stamp to a known marker: run 2 changes fields but is
    // unwitnessed, and an unwitnessed write must never move verified_at —
    // exactly (not just "still set"), or the stamp gate's `verified() &&`
    // could silently become `||`.
    let marker = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(2)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE issues SET verified_at = ?1",
            rusqlite::params![marker.as_str()],
        )
        .unwrap();

    // Run 2: the remote claims another page but offers no cursor — the
    // witness-withholding shape — and one comment is missing from the
    // nodes. Truncation must not read as deletion.
    let mut t = Issue::new("I_1", 11, "2026-07-21T00:00:00Z");
    t.comment_ids = vec!["IC_1".into()];
    t.comments_has_next = true;
    install_issues(&fake, &[&t]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 1, "{s}");
    let (truncated, verified_at): (i64, Option<String>) = fake
        .db()
        .query_row(
            "SELECT truncated, verified_at FROM issues WHERE repo='o/n' AND number=11",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(truncated, 1);
    assert_eq!(
        verified_at.as_deref(),
        Some(marker.as_str()),
        "an unwitnessed hydration carries the stamp, to the byte"
    );
    let live: i64 = fake.query_one(
        "SELECT count(*) FROM comments WHERE parent_kind='issue' AND deleted_at IS NULL",
    );
    assert_eq!(live, 2, "an incomplete connection sweeps nothing");

    // Run 3: complete again, and IC_2 is genuinely gone upstream — the
    // witnessed walk heals truncated and sweeps softly.
    let mut h = Issue::new("I_1", 11, "2026-07-22T00:00:00Z");
    h.comment_ids = vec!["IC_1".into()];
    install_issues(&fake, &[&h]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["soft_deleted"], 1, "{s}");
    let truncated: i64 =
        fake.query_one("SELECT truncated FROM issues WHERE repo='o/n' AND number=11");
    assert_eq!(truncated, 0, "the complete refetch heals truncation");
    let deleted: i64 = fake.query_one(
        "SELECT count(*) FROM comments WHERE parent_kind='issue' AND deleted_at IS NOT NULL",
    );
    assert_eq!(deleted, 1, "soft delete: the row stays, dated");
}

// 17d'. A masked labels connection (schema-nullable; the D1 panel finding)
// is a withheld witness, not data: the row lands truncated with the STORED
// labels carried — never overwritten with "no labels", never a parse-class
// quarantine — and the next unmasked walk heals it.

#[test]
fn masked_labels_carry_stored_value_and_heal() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let mut a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    a.labels = vec!["bug", "triaged"];
    install_issues(&fake, &[&a]);
    fake.sync_ok();

    let mut m = Issue::new("I_1", 11, "2026-07-21T00:00:00Z");
    m.mask_labels = true;
    install_issues(&fake, &[&m]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 0, "a mask is not drift: {s}");
    assert_eq!(s["health"]["truncated"], 1, "{s}");
    let labels: String = fake.query_one("SELECT labels FROM issues WHERE repo='o/n' AND number=11");
    assert_eq!(
        labels, r#"["bug","triaged"]"#,
        "the stored labels carry through the mask"
    );

    let mut h = Issue::new("I_1", 11, "2026-07-22T00:00:00Z");
    h.labels = vec!["bug"];
    install_issues(&fake, &[&h]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 0, "{s}");
    let (labels, truncated): (String, i64) = fake
        .db()
        .query_row(
            "SELECT labels, truncated FROM issues WHERE repo='o/n' AND number=11",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(labels, r#"["bug"]"#, "the unmasked walk recomputes");
    assert_eq!(truncated, 0, "and heals the truncation");
}

// 17e. Issue quarantine round-trips through the stream column: a failed
// issue hydration quarantines with stream='issue', and the retry
// resurrects it through HYDRATE_ISSUE — never HYDRATE_PR, which would
// parse-fail forever (the drain would also hit the wrong table).

#[test]
fn issue_quarantine_retries_through_the_issue_document() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1,
        "retry_attempts": 1,
        "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    install_issues(&fake, &[&a]);
    fake.write("stderr-I_1", "boom: transient trouble");
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 1, "{s}");
    let stream: String = fake.query_one("SELECT stream FROM quarantine WHERE id = 'I_1'");
    assert_eq!(stream, "issue");

    // Make the retry due, clear the fault, and re-run: the retry must
    // dispatch through HYDRATE_ISSUE and the row must land whole.
    fake.remove("stderr-I_1");
    fake.db()
        .execute(
            "UPDATE quarantine SET next_retry_at = '2020-01-01T00:00:00Z' WHERE id = 'I_1'",
            [],
        )
        .unwrap();
    fake.sync_ok();
    assert_eq!(
        fake.issue_hydrations(2),
        vec!["I_1"],
        "the window walk skips quarantined ids (backoff dominates); the one \
         hydration is the retry, through the issue document"
    );
    assert_eq!(fake.hydrations(2), Vec::<String>::new());
    let quarantined: i64 = fake.query_one("SELECT count(*) FROM quarantine");
    assert_eq!(quarantined, 0, "resolution deletes the record");
    let landed: i64 = fake.query_one(
        "SELECT count(*) FROM issues WHERE repo='o/n' AND number=11 \
         AND hydration_source='stream' AND verified_at IS NOT NULL",
    );
    assert_eq!(landed, 1);
}

// 17f. Issue multi-page hydration: the follow-up page merges and the
// witness is earned by TERMINATED pagination — the issue-side mirror of
// test 18.

#[test]
fn issue_follow_up_pages_merge_and_earn_the_witness() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let mut a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    a.comments_has_next = true;
    a.comments_cursor = Some("c1");
    install_issues(&fake, &[&a]);
    fake.write(
        "icpage-I_1.json",
        &json!({"data": {"node": {"comments": {
            "totalCount": 2,
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": [{
                "id": "IC_p2", "body": "second-page comment",
                "createdAt": "2026-07-10T02:00:00Z", "lastEditedAt": null,
                "url": "https://github.com/x2", "isMinimized": false,
                "authorAssociation": "NONE", "author": author("carol", "User")}]}},
            "rateLimit": rate_limit(4000)}})
        .to_string(),
    );

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 0, "{s}");
    assert!(
        fake.calls()
            .iter()
            .any(|c| c.starts_with("ICPAGE|run=1|id=I_1|after=c1")),
        "the follow-up page walks the cursor: {:?}",
        fake.calls()
    );
    let comments: i64 = fake.query_one("SELECT count(*) FROM comments WHERE parent_kind='issue'");
    assert_eq!(comments, 2, "pages merge");
    let verified: i64 = fake.query_one(
        "SELECT count(*) FROM issues WHERE number=11 AND verified_at IS NOT NULL AND truncated=0",
    );
    assert_eq!(verified, 1, "terminated pagination earns the witness");
}

// 17f'. Three pages: the mid-walk cursor ADVANCE (c1 → c2) is the arm the
// two-page shape never exercises — a walk that cannot take it reads every
// multi-follow-up issue as truncated.

#[test]
fn issue_walk_advances_through_a_mid_walk_cursor() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let mut a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    a.comments_has_next = true;
    a.comments_cursor = Some("c1");
    install_issues(&fake, &[&a]);
    let page = |cid: &str, cursor: Value, has_next: bool| {
        json!({"data": {"node": {"comments": {
            "totalCount": 3,
            "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
            "nodes": [{
                "id": cid, "body": format!("comment {cid}"),
                "createdAt": "2026-07-10T02:00:00Z", "lastEditedAt": null,
                "url": "https://github.com/x", "isMinimized": false,
                "authorAssociation": "NONE", "author": author("carol", "User")}]}},
            "rateLimit": rate_limit(4000)}})
        .to_string()
    };
    fake.write("icpage-I_1-c1.json", &page("IC_p2", json!("c2"), true));
    fake.write("icpage-I_1-c2.json", &page("IC_p3", Value::Null, false));

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 0, "{s}");
    let pages: Vec<String> = fake
        .calls()
        .iter()
        .filter(|c| c.starts_with("ICPAGE|run=1"))
        .cloned()
        .collect();
    assert_eq!(
        pages,
        vec![
            "ICPAGE|run=1|id=I_1|after=c1".to_string(),
            "ICPAGE|run=1|id=I_1|after=c2".to_string(),
        ],
        "the walk advances through the mid-walk cursor exactly once each"
    );
    let comments: i64 = fake.query_one("SELECT count(*) FROM comments WHERE parent_kind='issue'");
    assert_eq!(comments, 3, "all three pages merge");
}

// 17g. An issue follow-up page whose cursor does not advance reads as a
// withheld witness (truncated), never an infinite walk and never a sweep.

#[test]
fn issue_non_advancing_cursor_withholds_the_witness() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let mut a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    a.comments_has_next = true;
    a.comments_cursor = Some("c1");
    install_issues(&fake, &[&a]);
    // The follow-up claims yet another page behind the SAME cursor.
    fake.write(
        "icpage-I_1.json",
        &json!({"data": {"node": {"comments": {
            "totalCount": 3,
            "pageInfo": {"hasNextPage": true, "endCursor": "c1"},
            "nodes": [{
                "id": "IC_p2", "body": "second-page comment",
                "createdAt": "2026-07-10T02:00:00Z", "lastEditedAt": null,
                "url": "https://github.com/x2", "isMinimized": false,
                "authorAssociation": "NONE", "author": author("carol", "User")}]}},
            "rateLimit": rate_limit(4000)}})
        .to_string(),
    );

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 1, "{s}");
    let icpages = fake
        .calls()
        .iter()
        .filter(|c| c.starts_with("ICPAGE|run=1"))
        .count();
    assert_eq!(icpages, 1, "a stuck cursor is walked exactly once");
    let verified: i64 =
        fake.query_one("SELECT count(*) FROM issues WHERE number=11 AND verified_at IS NOT NULL");
    assert_eq!(verified, 0, "no witness from a walk that could not end");
}

// 17h. The floor gates issue follow-up paging, strictly: remaining == floor
// keeps paging, remaining < floor stops mid-walk with the witness withheld
// — the issue-side floor_boundary_is_strict.

#[test]
fn issue_floor_boundary_gates_follow_up_pages() {
    for (remaining, pages) in [(500u32, true), (499, false)] {
        let fake = Fake::new();
        fake.config(&json!({
            "viewer": "viewer",
            "repos": [{"repo": "o/n", "scope": "project"}], // floor: 500 default
            "workers": 1, "retry_attempts": 1, "retry_budget": 5
        }));
        install_prs(&fake, &[]);
        let mut a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
        a.comments_has_next = true;
        a.comments_cursor = Some("c1");
        a.remaining = remaining; // observed after the first-page document
        install_issues(&fake, &[&a]);
        fake.write(
            "icpage-I_1.json",
            &json!({"data": {"node": {"comments": {
                "totalCount": 2,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": [{
                    "id": "IC_p2", "body": "second-page comment",
                    "createdAt": "2026-07-10T02:00:00Z", "lastEditedAt": null,
                    "url": "https://github.com/x2", "isMinimized": false,
                    "authorAssociation": "NONE", "author": author("carol", "User")}]}},
                "rateLimit": rate_limit(4000)}})
            .to_string(),
        );

        let doc = fake.sync_ok();
        let s = fake.repo_summary(&doc, "o/n");
        let paged = fake.calls().iter().any(|c| c.starts_with("ICPAGE|run=1"));
        assert_eq!(paged, pages, "remaining={remaining}: {s}");
        assert_eq!(
            s["health"]["truncated"],
            json!(u64::from(!pages)),
            "remaining={remaining}: an aborted walk lands truncated"
        );
    }
}

// 17i. A halted stream skips the maintenance phases: an issue-stream halt
// (unsplittable capped window) must leave a due quarantine retry unrun,
// exactly as a PR-stream halt does — either stream failing to complete
// gates retries and re-verify.

#[test]
fn issue_stream_halt_skips_quarantine_retries() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "lookback_days": 1, // bounds the halving depth
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    // Run 1: PR_1 fails hydration → quarantined.
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.write("stderr-PR_1", "boom");
    install_issues(&fake, &[]);
    fake.sync_ok();
    fake.remove("stderr-PR_1");
    let q: i64 = fake.query_one("SELECT count(*) FROM quarantine WHERE id='PR_1'");
    assert_eq!(q, 1);
    fake.db()
        .execute(
            "UPDATE quarantine SET next_retry_at = '2020-01-01T00:00:00Z'",
            [],
        )
        .unwrap();

    // Run 2: every issue window, at every depth, reports far more hits
    // than it returns — capped, splittable to the 2s floor, then halted.
    let i = Issue::new("I_1", 11, "2026-07-20T11:00:00Z");
    fake.write("ihyd-I_1.json", &i.hydration());
    fake.write(
        "idisc-default.json",
        &discovery_nodes(vec![i.hit()], Some(1500), 4000),
    );
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["discovery_truncated"], 1, "{s}");
    assert_eq!(
        fake.hydrations(2),
        Vec::<String>::new(),
        "the due retry must NOT run behind a halted stream"
    );
    let q: i64 = fake.query_one("SELECT count(*) FROM quarantine WHERE id='PR_1'");
    assert_eq!(q, 1, "the quarantine row stays due for a completing run");
}

// 17j. A relaxation cold start interrupted by the floor restarts as a cold
// start: its intermediate windows commit the STORED fingerprint, so the
// next run reads "unequal" and re-walks from the lookback floor instead of
// resuming incrementally from the old watermark — which would silently
// skip everything the relaxation existed to ingest (the F1 argument,
// generalized; D1 panel finding).

#[test]
fn interrupted_cold_start_restarts_instead_of_resuming() {
    let fake = Fake::new();
    let cfg = |bots: bool| {
        json!({
            "viewer": "viewer",
            "repos": [{"repo": "o/n", "scope": "project", "issues": false, "bots": bots}],
            "workers": 1, "retry_attempts": 1, "retry_budget": 5
        })
    };
    // Run 1: bots off; the human PR syncs, watermark lands at 07-20.
    fake.config(&cfg(false));
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    // Run 2: bots on — a relaxation, so a cold start from the lookback
    // floor. The walk splits (capped top window), banks its left half —
    // which hydrates the bot PR_B the relaxation revealed, at 499
    // remaining — and floors before the right half.
    fake.config(&cfg(true));
    let mut b = Pr::new("PR_B", 2, "2026-07-01T00:00:00Z");
    b.author_login = "dependabot".into();
    b.author_type = "Bot";
    b.remaining = 499; // below the default 500 floor after B hydrates
    fake.write("disc-2-0.json", &discovery(&[&a], Some(1500), 4000));
    fake.write("disc-2-1.json", &discovery(&[&b], None, 4000));
    fake.write("hyd-PR_B.json", &b.hydration());
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["deferred_at_floor"], true, "{s}");
    let banked: i64 = fake.query_one("SELECT count(*) FROM prs WHERE number=2");
    assert_eq!(banked, 1, "the completed left window banked its rows");
    let fp: String =
        fake.query_one("SELECT fingerprint FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(
        fp.contains("\"bots\":false"),
        "an interrupted cold start must keep the STORED inputs: {fp}"
    );

    // Run 3: same config, budget restored. The stored fingerprint still
    // says bots:false, so the walk cold-starts again and completes; only
    // now do the new inputs land.
    b.remaining = 4000;
    fake.write("refresh-PR_B.json", &b.refresh());
    fake.write("hyd-PR_B.json", &b.hydration());
    fake.write("disc-3-0.json", &discovery(&[&a, &b], None, 4000));
    fake.sync_ok();
    let fp: String =
        fake.query_one("SELECT fingerprint FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(
        fp.contains("\"bots\":true"),
        "the completing walk claims the new inputs: {fp}"
    );
    let q3 = fake
        .calls()
        .iter()
        .find(|c| c.starts_with("DISC|run=3|seq=0"))
        .expect("run 3 discovers")
        .clone();
    assert!(
        !q3.contains("2026-07-20"),
        "run 3 walks from the lookback floor, not from the old watermark: {q3}"
    );
}

// 17k. A drained issue resurrects on a linked sighting: node:null drained
// it, but a later PR's closingIssuesReferences RENDERING the node is live
// evidence it resolves again — presence data that crosses the ownership
// boundary while content stays stream-owned.

#[test]
fn drained_issue_resurrects_on_linked_sighting() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    // Run 1: the stream owns issue 101 (the number PR_1's linked ref names).
    install_prs(&fake, &[]);
    let mut i = Issue::new("I_PR_1", 101, "2026-07-20T00:00:00Z");
    i.title = "stream title".into();
    install_issues(&fake, &[&i]);
    fake.sync_ok();

    // The issue vanishes upstream: node:null on every later hydration.
    fake.write(
        "ihyd-I_PR_1.json",
        &json!({"data": {"node": null, "rateLimit": rate_limit(4000)}}).to_string(),
    );
    // Run 2: rediscovered (same window overlap), hydration nulls →
    // quarantined. Runs 3 and 4: made due, retried, null again — the third
    // null drains to deleted_at and retires the quarantine row.
    fake.sync_ok();
    for _ in 0..2 {
        fake.db()
            .execute(
                "UPDATE quarantine SET next_retry_at = '2020-01-01T00:00:00Z'",
                [],
            )
            .unwrap();
        fake.sync_ok();
    }
    let (deleted, source): (Option<String>, String) = fake
        .db()
        .query_row(
            "SELECT deleted_at, hydration_source FROM issues WHERE repo='o/n' AND number=101",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(deleted.is_some(), "three nulls drain to deleted_at");
    assert_eq!(source, "stream");
    let q: i64 = fake.query_one("SELECT count(*) FROM quarantine");
    assert_eq!(q, 0, "the drain retires the quarantine row");

    // Run 5: a PR arrives whose closingIssuesReferences renders issue 101
    // (Pr::hydration's linked fixture names number+100 with id I_PR_1).
    // The sighting clears deleted_at; content stays stream-owned.
    let pr = Pr::new("PR_1", 1, "2026-07-21T00:00:00Z");
    install_prs(&fake, &[&pr]);
    fake.write("idisc-5-0.json", &discovery_nodes(vec![], None, 4000));
    fake.sync_ok();
    let (deleted, source, title): (Option<String>, String, String) = fake
        .db()
        .query_row(
            "SELECT deleted_at, hydration_source, title FROM issues \
             WHERE repo='o/n' AND number=101",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert!(deleted.is_none(), "a live sighting clears the drain");
    assert_eq!(source, "stream", "ownership does not move");
    assert_eq!(title, "stream title", "content does not move either");

    // The resurrect is a CONTENT change and must count as one: re-delete
    // the row directly, replay the otherwise-unchanged PR — the only write
    // in the run is the sighting's deleted_at clear, and upserted must say
    // so (a silent resurrect would hide the flip from the summary).
    fake.db()
        .execute(
            "UPDATE issues SET deleted_at = '2026-01-01T00:00:00Z' WHERE number = 101",
            [],
        )
        .unwrap();
    fake.write("idisc-6-0.json", &discovery_nodes(vec![], None, 4000));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["counts"]["upserted"], 1, "the resurrect counts: {s}");
    let deleted: Option<String> =
        fake.query_one("SELECT deleted_at FROM issues WHERE repo='o/n' AND number=101");
    assert!(deleted.is_none());
}

// 17l. A repo failure increments starvation only for the streams THIS RUN
// walked: after issues flips off, the residual (repo,'issue') row must not
// keep climbing (nothing can ever reset it), or the repo reads permanently
// starved and biases starved-first ordering forever.

#[test]
fn failed_run_increments_only_the_configured_streams() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    let i = Issue::new("I_1", 11, "2026-07-20T11:00:00Z");
    install_issues(&fake, &[&i]);
    fake.sync_ok();

    // Issues off; the PR stream's run-2 discovery drifts (wrong shape) →
    // a repo-scoped failure.
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project", "issues": false}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    fake.write(
        "disc-2-0.json",
        &json!({"data": {"search": {"unexpected": true}}}).to_string(),
    );
    let (code, doc, _stderr) = fake.run(&["sync"]);
    assert_eq!(code, 0, "a repo failure is a summary disclosure");
    let doc = doc.expect("summary emitted");
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(
        s["health"]["errors"].as_array().map(Vec::len),
        Some(1),
        "{s}"
    );
    let pr_runs: i64 = fake
        .query_one("SELECT runs_since_advance FROM sync_state WHERE repo='o/n' AND stream='pr'");
    let issue_runs: i64 = fake
        .query_one("SELECT runs_since_advance FROM sync_state WHERE repo='o/n' AND stream='issue'");
    assert_eq!(pr_runs, 1, "the walked stream failed to complete");
    assert_eq!(issue_runs, 0, "the unconfigured stream is not starving");
}

// 17m. Issue re-verify: the tier's complete refetch catches quiet
// mutations (a comment edit that never bumps the issue's updatedAt),
// counts them, and re-stamps verified_at even when nothing changed — the
// re-stamp is what keeps the schedule advancing.

#[test]
fn issue_reverify_catches_quiet_mutations_and_restamps() {
    let fake = Fake::new();
    fake.config(&json!({
        "viewer": "viewer",
        "repos": [{"repo": "o/n", "scope": "project"}],
        "workers": 1, "retry_attempts": 1, "retry_budget": 5
    }));
    install_prs(&fake, &[]);
    let a = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    install_issues(&fake, &[&a]);
    fake.sync_ok();

    // Age the stamp past the open tier's period; quiet-edit the comment
    // body upstream WITHOUT bumping the issue's updatedAt; empty the
    // discovery windows so only re-verify can find it.
    let backdate = |fake: &Fake| {
        fake.db()
            .execute("UPDATE issues SET verified_at = '2026-01-01T00:00:00Z'", [])
            .unwrap();
    };
    backdate(&fake);
    let mut quiet = Issue::new("I_1", 11, "2026-07-20T00:00:00Z");
    quiet.comment_ids = vec!["IC_I_1".into()];
    let mut hyd: Value = serde_json::from_str(&quiet.hydration()).unwrap();
    hyd["data"]["node"]["comments"]["nodes"][0]["body"] = json!("edited quietly");
    fake.write("ihyd-I_1.json", &hyd.to_string());
    fake.write("idisc-2-0.json", &discovery_nodes(vec![], None, 4000));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["reverified"], 1, "{s}");
    assert_eq!(s["refresh"]["quiet_mutations_found"], 1, "{s}");
    assert_eq!(s["counts"]["fetched"], 1, "a re-verify is a fetch");
    assert_eq!(
        fake.issue_hydrations(2),
        vec!["I_1"],
        "re-verify dispatches through the issue document"
    );
    let body: String =
        fake.query_one("SELECT body FROM comments WHERE id='IC_I_1' AND parent_kind='issue'");
    assert_eq!(body, "edited quietly");
    let moved: i64 =
        fake.query_one("SELECT count(*) FROM issues WHERE verified_at > '2026-01-01T00:00:00Z'");
    assert_eq!(moved, 1, "the refetch re-stamps");

    // Round 2, nothing changed upstream: the re-verify still re-stamps
    // (the schedule reads verified_at) while counting no quiet mutation
    // and writing no content.
    backdate(&fake);
    fake.write("idisc-3-0.json", &discovery_nodes(vec![], None, 4000));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["reverified"], 1, "{s}");
    assert_eq!(s["refresh"]["quiet_mutations_found"], 0, "{s}");
    assert_eq!(s["counts"]["upserted"], 0);
    let moved: i64 =
        fake.query_one("SELECT count(*) FROM issues WHERE verified_at > '2026-01-01T00:00:00Z'");
    assert_eq!(moved, 1, "an unchanged re-verify still re-stamps");
}

// ---------------------------------------------------------------------------
// 18. Multi-page hydration: follow-up pages merge, the witness is earned by
// TERMINATED pagination, and a broken follow-up withholds it.

#[test]
fn follow_up_pages_merge_and_earn_the_witness() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comments_has_next = true;
    a.comments_cursor = Some("c1");
    a.threads_has_next = true;
    a.threads_cursor = Some("t1");
    install_prs(&fake, &[&a]);
    fake.write(
        "cpage-PR_1.json",
        &json!({"data": {"node": {"comments": {
            "totalCount": 2,
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": [{
                "id": "C_p2", "body": "second-page comment",
                "createdAt": "2026-07-10T02:00:00Z", "lastEditedAt": null,
                "url": "https://github.com/x2", "isMinimized": false,
                "authorAssociation": "NONE", "author": author("carol", "User")}]}},
            "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    fake.write(
        "tpage-PR_1.json",
        &json!({"data": {"node": {"reviewThreads": {
            "totalCount": 2,
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": [{
                "id": "T_p2", "isResolved": false, "isOutdated": false,
                "path": "src/y.rs", "line": 4,
                "comments": {"totalCount": 1, "nodes": [{
                    "id": "TC_p2", "body": "second-page thread comment",
                    "createdAt": "2026-07-10T03:00:00Z", "lastEditedAt": null,
                    "url": "https://github.com/t2", "isMinimized": false,
                    "authorAssociation": "NONE", "author": author("erin", "User")}]}}]}},
            "rateLimit": rate_limit(4000)}})
        .to_string(),
    );

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(
        s["health"]["truncated"], 0,
        "terminated pagination = witness: {s}"
    );
    let comments: i64 =
        fake.query_one("SELECT count(*) FROM comments WHERE kind='comment' AND deleted_at IS NULL");
    assert_eq!(comments, 2, "both pages' comments stored");
    let threads: i64 = fake.query_one("SELECT count(*) FROM review_threads");
    assert_eq!(threads, 2, "both pages' threads stored");
    let paged: Vec<String> = fake
        .calls()
        .iter()
        .filter(|l| l.starts_with("CPAGE") || l.starts_with("TPAGE"))
        .cloned()
        .collect();
    assert_eq!(paged.len(), 2, "one follow-up per connection: {paged:?}");
    assert!(paged[0].contains("after=c1") || paged[1].contains("after=c1"));
    let verified: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert!(verified.is_some());

    // A later refetch whose comments follow-up BREAKS: the witness is
    // withheld, the PR lands truncated, and the second-page comment
    // gathered earlier is NOT swept (no witness, no sweep).
    fake.remove("cpage-PR_1.json");
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["truncated"], 1, "{s}");
    let gone: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_p2'");
    assert!(gone.is_none(), "a broken walk must not sweep the tail");
}

// ---------------------------------------------------------------------------
// 19. A stuck discovery cursor reads as capped (never as complete): the
// window splits to the floor and the stream halts, exactly like the
// count-capped case.

#[test]
fn non_advancing_discovery_cursor_reads_as_capped() {
    let fake = Fake::new();
    let mut cfg = base_config();
    cfg["lookback_days"] = json!(1);
    fake.config(&cfg);
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    fake.write("hyd-PR_1.json", &a.hydration());
    let mut d: Value = serde_json::from_str(&discovery(&[&a], None, 4000)).unwrap();
    d["data"]["search"]["pageInfo"] = json!({"hasNextPage": true, "endCursor": "X"});
    fake.write("disc-default.json", &d.to_string());

    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["discovery_truncated"], 1, "{s}");
    let checked: Option<String> =
        fake.query_one("SELECT last_checked_at FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(checked.is_none(), "a halted stream never claims freshness");
}

// ---------------------------------------------------------------------------
// 20. Re-verify sheds first at the floor, and the shed volume is counted —
// shedding is graceful degradation, not a deferral.

#[test]
fn reverify_sheds_at_the_floor_and_counts_it() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    let aged = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(61)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE prs SET verified_at = ?1",
            rusqlite::params![aged.as_str()],
        )
        .unwrap();
    // Run 2: an empty discovery window whose response leaves the budget one
    // point below the floor. The window completes (no more calls needed);
    // re-verify would be next and sheds instead.
    fake.write("disc-2-0.json", &discovery(&[], Some(0), 499));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["reverify_shed"], 1, "{s}");
    assert_eq!(s["refresh"]["reverified"], 0);
    assert_eq!(
        s["health"]["deferred_at_floor"], false,
        "shedding the deferrable tier is not a stream deferral: {s}"
    );
}

// ---------------------------------------------------------------------------
// 21. A backfill that does not finish leaves the STORED fingerprint alone,
// so the next run re-detects the person and re-runs the (idempotent)
// backfill — a kill or failure between backfill windows must not strand it
// (closure-pass F1).

#[test]
fn interrupted_backfill_keeps_the_old_fingerprint_and_reruns() {
    let fake = Fake::new();
    fake.config(&working_config(&[], 90));
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    // Run 1 (cold, three flavors = seq 0..2): explicit per-seq responses,
    // no default — later runs control exactly which discovery calls live.
    for seq in 0..3 {
        fake.write(&format!("disc-1-{seq}.json"), &discovery(&[&a], None, 4000));
    }
    fake.write("hyd-PR_1.json", &a.hydration());
    fake.sync_ok();
    let fp1: String =
        fake.query_one("SELECT fingerprint FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(!fp1.contains("bob"));

    // Run 2, person added: the backfill window (seq 0) succeeds; the main
    // walk's first discovery (seq 1) has no fixture and fails the repo.
    fake.config(&working_config(&["bob"], 90));
    fake.write("disc-2-0.json", &discovery(&[&a], None, 4000));
    let (code, doc, _) = fake.run(&["sync"]);
    assert_eq!(code, 0);
    let doc = doc.unwrap();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["errors"].as_array().unwrap().len(), 1, "{s}");
    let fp2: String =
        fake.query_one("SELECT fingerprint FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(
        !fp2.contains("bob"),
        "an unfinished backfill must not commit the new inputs: {fp2}"
    );

    // Run 3, healthy: the person is re-detected, the backfill re-runs, and
    // only a COMPLETED main walk commits the new fingerprint.
    fake.write("disc-default.json", &discovery(&[&a], None, 4000));
    fake.sync_ok();
    let backfills: Vec<String> = fake
        .calls()
        .iter()
        .filter(|l| l.starts_with("DISC|run=3") && l.contains("involves:bob"))
        .cloned()
        .collect();
    assert!(
        backfills.len() >= 2,
        "run 3 re-runs the backfill flavor plus the regular flavor: {backfills:?}"
    );
    let fp3: String =
        fake.query_one("SELECT fingerprint FROM sync_state WHERE repo='o/n' AND stream='pr'");
    assert!(fp3.contains("bob"), "the completed walk commits the inputs");
}

// ---------------------------------------------------------------------------
// 15. The layered refresh: dispatch, conservation arms, skeleton bodies, and
// the masked-case/re-verify interplay. (The tail-hit replay and its cost
// pin live in test 1.)

/// An upstream deletion unbalances the count (2 archived + 0 new != 1):
/// the check escalates, the full walk runs from the top, and ONLY the full
/// walk sweeps — the deletion becomes a soft delete with a witness, never
/// an inference.
#[test]
fn refresh_escalates_on_deletion_and_the_full_walk_sweeps() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into(), "C_b".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();
    assert!(
        fake.refreshes(1).is_empty(),
        "first contact never refreshes"
    );

    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["full_walks"], 1, "escalated: {s}");
    assert_eq!(s["refresh"]["tail_hits"], 0);
    assert_eq!(s["refresh"]["escalations"]["count imbalance"], 1, "{s}");
    assert_eq!(s["counts"]["soft_deleted"], 1);
    assert_eq!(fake.refreshes(2), vec!["PR_1".to_string()]);
    assert_eq!(
        fake.hydrations(2),
        vec!["PR_1".to_string()],
        "restarted from the top"
    );
    let gone: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_b'");
    assert!(
        gone.is_some(),
        "the walk's witness sweeps what the tail could not"
    );
}

/// An appended comment tail-hits: the new row lands, nothing sweeps, and
/// the stamp does not move — a tail hit is an inference, and verified_at
/// only ever records witnesses.
#[test]
fn refresh_tail_hit_upserts_without_stamp() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();
    let sentinel = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(2)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE prs SET verified_at = ?1",
            rusqlite::params![sentinel.as_str()],
        )
        .unwrap();

    a.comment_ids = vec!["C_a".into(), "C_b".into()];
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "{s}");
    assert_eq!(s["refresh"]["full_walks"], 0);
    assert_eq!(
        s["counts"]["upserted"], 1,
        "the new comment is a content change"
    );
    assert_eq!(
        s["health"]["truncated"], 0,
        "a tail hit carries stored truncated=0"
    );
    assert!(fake.hydrations(2).is_empty(), "no full walk");
    let present: i64 = fake.query_one("SELECT count(*) FROM comments WHERE id='C_b'");
    assert_eq!(present, 1);
    let stamp: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert_eq!(
        stamp.as_deref(),
        Some(sentinel.as_str()),
        "inference never stamps"
    );
}

/// The walk-back: the first tail page is all-new, the second reaches the
/// archived set (the anchor), and the balanced whole hits — count and tail
/// from one document on every iteration.
#[test]
fn refresh_walks_back_to_the_anchor() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into(), "C_b".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    // Hand-built pages: the fully-observed suffix is [C_a, C_b, C_c] with
    // totalCount 3; the refresh's own page carries only the new C_c.
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["comments"] = json!({
        "totalCount": 3,
        "pageInfo": {"hasPreviousPage": true, "startCursor": "curA"},
        "nodes": [comment_node("C_c")]
    });
    fake.write("refresh-PR_1.json", &refresh.to_string());
    // The terminal page's startCursor is null on purpose: the walk-back's
    // cursor bookkeeping must read "no previous page" as termination, not
    // as a non-advancing cursor (which escalates).
    fake.write(
        "tail-PR_1-curA.json",
        &json!({"data": {"node": {"comments": {
            "totalCount": 3,
            "pageInfo": {"hasPreviousPage": false, "startCursor": null},
            "nodes": [comment_node("C_a"), comment_node("C_b")]
        }}, "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "{s}");
    assert!(
        fake.calls()
            .iter()
            .any(|l| l.starts_with("TAIL|run=2|id=PR_1|before=curA")),
        "the walk-back paged before the anchor: {:?}",
        fake.calls()
    );
    let present: i64 = fake.query_one("SELECT count(*) FROM comments WHERE id='C_c'");
    assert_eq!(present, 1);
    assert_eq!(
        s["cost"]["subprocess_count"], 3,
        "disc + refresh + one tail page"
    );
}

/// No anchor within the walk-back bound: no induction base, escalate —
/// and a totalCount that moves between pages escalates immediately (the
/// pages are not one snapshot).
#[test]
fn refresh_escalates_unanchored_and_moved_counts() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    let tail_page = |total: i64, id: &str, cursor: &str, more: bool| {
        json!({"data": {"node": {"comments": {
            "totalCount": total,
            "pageInfo": {"hasPreviousPage": more, "startCursor": cursor},
            "nodes": [comment_node(id)]
        }}, "rateLimit": rate_limit(4000)}})
        .to_string()
    };
    // Nine comments upstream, every walked page all-new: after the two
    // bound walk-back pages there is still no anchor.
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["comments"] = json!({
        "totalCount": 9,
        "pageInfo": {"hasPreviousPage": true, "startCursor": "curA"},
        "nodes": [comment_node("N_1")]
    });
    fake.write("refresh-PR_1.json", &refresh.to_string());
    fake.write("tail-PR_1-curA.json", &tail_page(9, "N_2", "curB", true));
    fake.write("tail-PR_1-curB.json", &tail_page(9, "N_3", "curC", true));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["full_walks"], 1, "unanchored: {s}");
    assert_eq!(
        s["refresh"]["escalations"]["no anchor within walk-back bound"], 1,
        "the reason split feeds the K-sizing diagnosis: {s}"
    );
    let tails: Vec<String> = fake
        .calls()
        .iter()
        .filter(|l| l.starts_with("TAIL|run=2"))
        .cloned()
        .collect();
    assert_eq!(tails.len(), 2, "the bound stopped the hunt: {tails:?}");
    assert_eq!(fake.hydrations(2), vec!["PR_1".to_string()]);

    // Run 3: the count moves between page one and the walk-back page.
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["comments"] = json!({
        "totalCount": 2,
        "pageInfo": {"hasPreviousPage": true, "startCursor": "curA"},
        "nodes": [comment_node("N_1")]
    });
    fake.write("refresh-PR_1.json", &refresh.to_string());
    fake.write("tail-PR_1-curA.json", &tail_page(3, "C_a", "cur0", false));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["full_walks"], 1, "moved count: {s}");
    assert_eq!(
        s["refresh"]["escalations"]["count moved between pages"], 1,
        "{s}"
    );
    assert_eq!(fake.hydrations(3), vec!["PR_1".to_string()]);
}

/// The skeleton's edit signal: a moved lastEditedAt refetches the WHOLE
/// thread with bodies; an is_minimized flip with the signal unmoved is a
/// cheap-field update resolved from the archive — no body fetch at all.
#[test]
fn skeleton_fetches_bodies_only_on_the_edit_signal() {
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    // Run 2: the thread comment was edited (lastEditedAt moved).
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["reviewThreads"]["nodes"][0]["comments"]["nodes"][0]["lastEditedAt"] =
        json!("2026-07-21T00:00:00Z");
    fake.write("refresh-PR_1.json", &refresh.to_string());
    fake.write(
        "tbodies-T_PR_1.json",
        &json!({"data": {"node": {
            "id": "T_PR_1",
            "comments": {"totalCount": 1, "nodes": [{
                "id": "TC_PR_1", "body": "edited thread comment",
                "createdAt": "2026-07-10T01:00:00Z",
                "lastEditedAt": "2026-07-21T00:00:00Z",
                "url": "https://github.com/t", "isMinimized": false,
                "authorAssociation": "NONE",
                "author": author("erin", "User")}]}
        }, "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "{s}");
    assert_eq!(
        s["refresh"]["bodies_skipped"], 0,
        "an edited thread skips nothing"
    );
    assert!(
        fake.calls()
            .iter()
            .any(|l| l.starts_with("TBODIES|run=2|id=T_PR_1")),
        "{:?}",
        fake.calls()
    );
    let body: String = fake.query_one("SELECT body FROM comments WHERE id='TC_PR_1'");
    assert_eq!(body, "edited thread comment");

    // Run 3: minimize flip only — the signal is unmoved, so the body
    // comes from the archive and no thread refetch happens.
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["reviewThreads"]["nodes"][0]["comments"]["nodes"][0]["lastEditedAt"] =
        json!("2026-07-21T00:00:00Z");
    refresh["data"]["node"]["reviewThreads"]["nodes"][0]["comments"]["nodes"][0]["isMinimized"] =
        json!(true);
    fake.write("refresh-PR_1.json", &refresh.to_string());
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["bodies_skipped"], 1, "{s}");
    assert!(
        !fake.calls().iter().any(|l| l.starts_with("TBODIES|run=3")),
        "a cheap-field flip must not fetch bodies: {:?}",
        fake.calls()
    );
    let (minimized, body): (i64, String) = fake
        .db()
        .query_row(
            "SELECT is_minimized, body FROM comments WHERE id='TC_PR_1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(minimized, 1, "the flip landed");
    assert_eq!(
        body, "edited thread comment",
        "the archived body survived the skip"
    );
}

/// The masked case and its catcher, end to end. An order-violating
/// deletion+add that balances (the disclosed tolerance) tail-hits and
/// leaves the archive wrong with clean paperwork; the re-verify tier's
/// full walk catches it — which requires that a tail hit NOT stand in for
/// the tier's complete refetch (the full_walked skip set, not `hydrated`).
#[test]
fn masked_case_is_tolerated_then_caught_by_reverify() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into(), "C_b".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    // Upstream: C_a deleted, C_c added mid-connection (a backdated
    // import — the creation-order violation the tolerance covers). The
    // observed tail [C_b] anchors, 2 + 0 == 2 balances, the middle is
    // never fetched: a masked hit.
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["comments"] = json!({
        "totalCount": 2,
        "pageInfo": {"hasPreviousPage": true, "startCursor": "curX"},
        "nodes": [comment_node("C_b")]
    });
    fake.write("refresh-PR_1.json", &refresh.to_string());
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "the mask holds: {s}");
    let stale: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_a'");
    assert!(
        stale.is_none(),
        "the deleted row survives, stale (the tolerance)"
    );
    let missed: i64 = fake.query_one("SELECT count(*) FROM comments WHERE id='C_c'");
    assert_eq!(missed, 0, "the middle add is invisible to the tail");

    // Run 3: the tier is due (stamp aged past reverify_open_days). The
    // window STILL rediscovers the PR and the mask STILL holds — and the
    // tier must full-walk anyway: a tail hit is an inference, not the
    // complete refetch the tier exists to perform.
    let old = ghgraph::time::Rfc3339Utc::now()
        .checked_sub_days(40)
        .unwrap();
    fake.db()
        .execute(
            "UPDATE prs SET verified_at = ?1",
            rusqlite::params![old.as_str()],
        )
        .unwrap();
    a.comment_ids = vec!["C_b".into(), "C_c".into()];
    fake.write("hyd-PR_1.json", &a.hydration()); // the truth, for the walk
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "the window still masks: {s}");
    assert_eq!(s["refresh"]["reverified"], 1, "the tier walked anyway: {s}");
    assert_eq!(
        s["refresh"]["quiet_mutations_found"], 1,
        "and found the mutation"
    );
    assert_eq!(fake.hydrations(3), vec!["PR_1".to_string()]);
    let stale: Option<String> = fake.query_one("SELECT deleted_at FROM comments WHERE id='C_a'");
    assert!(stale.is_some(), "the catcher swept the masked deletion");
    let caught: i64 = fake.query_one("SELECT count(*) FROM comments WHERE id='C_c'");
    assert_eq!(caught, 1, "the catcher landed the masked add");
    let stamp: Option<String> = fake.query_one("SELECT verified_at FROM prs WHERE number=1");
    assert_ne!(
        stamp.as_deref(),
        Some(old.as_str()),
        "the witness re-stamped"
    );
}

/// The comment-node shape every hand-built refresh page uses.
fn comment_node(id: &str) -> Value {
    json!({
        "id": id, "body": format!("comment {id}"),
        "createdAt": "2026-07-10T00:00:00Z", "lastEditedAt": null,
        "url": "https://github.com/x", "isMinimized": false,
        "authorAssociation": "NONE", "author": author("carol", "User")
    })
}

/// The dispatch gate's other two arms: a stored-truncated row and the
/// carry license. A TailHit whose refresh lost a non-comments witness
/// lands truncated=1 (carrying stored 0 through a lost witness would hide
/// real truncation) — and that stored truncation then bars the tail path
/// entirely on the next run.
#[test]
fn refresh_gate_requires_an_untruncated_baseline() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    // Run 2: the refresh's reviewRequests connection arrives error-masked.
    // The tail concludes, but the carry is licensed by ALL other
    // witnesses: this bundle lands truncated.
    a.mask_requests = true;
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(
        s["refresh"]["tail_hits"], 1,
        "the check itself concluded: {s}"
    );
    assert_eq!(
        s["health"]["truncated"], 1,
        "a lost witness is not carried over: {s}"
    );
    let trunc: i64 = fake.query_one("SELECT truncated FROM prs WHERE number=1");
    assert_eq!(trunc, 1);

    // Run 3: the stored truncation bars the tail path — no trustworthy
    // counting universe — and the full walk (fixture still masked) keeps
    // the row truncated rather than healing it blind.
    let doc = fake.sync_ok();
    assert!(
        fake.refreshes(3).is_empty(),
        "a truncated baseline must never refresh: {:?}",
        fake.calls()
    );
    assert_eq!(fake.hydrations(3), vec!["PR_1".to_string()]);
    assert_eq!(fake.repo_summary(&doc, "o/n")["refresh"]["tail_hits"], 0);
}

/// The floor stops a refresh mid-walk-back: the check never concludes,
/// the bundle lands Incomplete (witness-free), and the stream defers —
/// never a skip, never an extra call. At exactly the floor the walk
/// proceeds ("below", not "at": the boundary, pinned).
#[test]
fn refresh_floor_aborts_the_walk_back() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    let refresh_page = |remaining: u32| {
        let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
        refresh["data"]["node"]["comments"] = json!({
            "totalCount": 2,
            "pageInfo": {"hasPreviousPage": true, "startCursor": "curA"},
            "nodes": [comment_node("C_b")]
        });
        refresh["data"]["rateLimit"] = rate_limit(remaining);
        refresh.to_string()
    };
    fake.write(
        "tail-PR_1-curA.json",
        &json!({"data": {"node": {"comments": {
            "totalCount": 2,
            "pageInfo": {"hasPreviousPage": false, "startCursor": null},
            "nodes": [comment_node("C_a")]
        }}, "rateLimit": rate_limit(4000)}})
        .to_string(),
    );

    // Run 2: the refresh document's own response drops remaining below
    // the floor (400 < 500). The anchor hunt must NOT spend another call.
    fake.write("refresh-PR_1.json", &refresh_page(400));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 0, "no verdict, no hit: {s}");
    assert_eq!(
        s["health"]["truncated"], 1,
        "the abort lands Incomplete: {s}"
    );
    // No Deferred here: PR_1 was the run's last work, so nothing behind
    // it needed the budget — the floor's evidence is the withheld
    // witness and the un-spent tail page, not a defer marker.
    assert_eq!(
        s["cost"]["subprocess_count"], 2,
        "disc + refresh only — the floor stopped the tail page: {s}"
    );

    // Run 3: the abort left the row truncated, which bars the tail path
    // (no trustworthy universe) — the full walk heals the witness. Only
    // then can run 4 test the boundary.
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert!(
        fake.refreshes(3).is_empty(),
        "truncated baseline: {:?}",
        fake.calls()
    );
    assert_eq!(s["health"]["truncated"], 0, "the walk healed it: {s}");

    // Run 4: remaining is exactly AT the floor — "below" does not fire,
    // the walk-back proceeds, anchors, and hits (resurrecting C_b, which
    // run 3's heal swept as absent from its complete fixture).
    fake.write("refresh-PR_1.json", &refresh_page(500));
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "at-the-floor proceeds: {s}");
    assert_eq!(s["cost"]["subprocess_count"], 3);
}

/// The fully-observed degenerate arm carries its weight: a PR verified
/// with ZERO comments gains its first — no anchor can exist, but the walk
/// reached the connection's start, so balance alone licenses the hit and
/// the PR stays on the cheap path.
#[test]
fn refresh_first_comment_hits_without_an_anchor() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec![];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    a.comment_ids = vec!["C_first".into()];
    install_prs(&fake, &[&a]);
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "{s}");
    assert!(fake.hydrations(2).is_empty(), "no escalation");
    let present: i64 = fake.query_one("SELECT count(*) FROM comments WHERE id='C_first'");
    assert_eq!(present, 1);
}

/// A tail page that repeats its cursor is a non-progress remote: escalate
/// immediately — exactly one wasted page, never a spin toward the bound.
#[test]
fn refresh_escalates_on_a_non_advancing_cursor() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    a.comment_ids = vec!["C_a".into()];
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["node"]["comments"] = json!({
        "totalCount": 5,
        "pageInfo": {"hasPreviousPage": true, "startCursor": "curA"},
        "nodes": [comment_node("N_1")]
    });
    fake.write("refresh-PR_1.json", &refresh.to_string());
    // The walk-back page hands back the cursor it was asked to page
    // before: no progress.
    fake.write(
        "tail-PR_1-curA.json",
        &json!({"data": {"node": {"comments": {
            "totalCount": 5,
            "pageInfo": {"hasPreviousPage": true, "startCursor": "curA"},
            "nodes": [comment_node("N_2")]
        }}, "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["full_walks"], 1, "{s}");
    assert_eq!(
        s["cost"]["subprocess_count"], 4,
        "disc + refresh + ONE tail page + the full walk: {s}"
    );
}

/// The skeleton walk pages like any other connection — and earns the
/// threads witness by ids across pages. Then under a floor, the follow-up
/// page is not spent and the witness is withheld instead.
#[test]
fn skeleton_walk_pages_and_respects_the_floor() {
    let fake = Fake::new();
    fake.config(&base_config());
    let mut a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    fake.sync_ok();

    // Run 2: two thread pages; the second carries a comment-less thread
    // and a null endCursor (termination, not non-progress).
    a.threads_has_next = true;
    a.threads_cursor = Some("tcurA");
    fake.write("refresh-PR_1.json", &a.refresh());
    fake.write(
        "skelpage-PR_1.json",
        &json!({"data": {"node": {"reviewThreads": {
            "totalCount": 2,
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": [{
                "id": "T2_PR_1", "isResolved": false, "isOutdated": false,
                "path": "src/y.rs", "line": 3,
                "comments": {"totalCount": 0, "nodes": []}
            }]
        }}, "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["refresh"]["tail_hits"], 1, "{s}");
    assert_eq!(
        s["health"]["truncated"], 0,
        "the paged walk earned the witness: {s}"
    );
    assert!(
        fake.calls()
            .iter()
            .any(|l| l.starts_with("SKELPAGE|run=2|id=PR_1")),
        "{:?}",
        fake.calls()
    );
    let t2: i64 = fake.query_one("SELECT count(*) FROM review_threads WHERE id='T2_PR_1'");
    assert_eq!(t2, 1);

    // Run 3: same shape, but the refresh response leaves remaining below
    // the floor — the follow-up skeleton page must not be spent, and the
    // withheld witness lands the row truncated.
    let mut refresh: Value = serde_json::from_str(&a.refresh()).unwrap();
    refresh["data"]["rateLimit"] = rate_limit(400);
    fake.write("refresh-PR_1.json", &refresh.to_string());
    let doc = fake.sync_ok();
    let s = fake.repo_summary(&doc, "o/n");
    assert!(
        !fake.calls().iter().any(|l| l.starts_with("SKELPAGE|run=3")),
        "the floor stops the skeleton walk: {:?}",
        fake.calls()
    );
    assert_eq!(s["health"]["truncated"], 1, "witness withheld: {s}");
    // The comments verdict concluded BEFORE the thread-phase floor: the
    // hit stands, and the incompleteness is the withheld threads
    // witness, not a demoted verdict (panel S2).
    assert_eq!(s["refresh"]["tail_hits"], 1, "{s}");
}

// ---------------------------------------------------------------------------
// 12. sync --strict: the gate flag changes the exit code, never a byte.

#[test]
fn strict_gates_the_exit_code_on_disclosed_incompleteness() {
    // Complete run: --strict exits 0 and the summary is the ordinary one.
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    install_prs(&fake, &[&a]);
    let (code, doc, stderr) = fake.run(&["sync", "--strict"]);
    assert_eq!(
        code, 0,
        "complete sync under --strict; stderr:\n{stderr}\ndoc: {doc:?}"
    );
    let doc = doc.expect("one JSON document");
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 0);
    assert!(!ghgraph::sync::incomplete(&doc));

    // Incomplete run: a discovered PR whose hydration fixture is missing
    // reads as a transport failure and quarantines (harness docs) — the
    // summary discloses it, and --strict turns that disclosure into exit 1
    // while stdout still carries the full document (a gate is not an
    // error: the envelope path stays exit 2).
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let b = Pr::new("PR_2", 2, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a]);
    fake.write("disc-default.json", &discovery(&[&a, &b], None, 4000));
    let (code, doc, stderr) = fake.run(&["sync", "--strict"]);
    assert_eq!(
        code, 1,
        "quarantine under --strict is exit 1; stderr:\n{stderr}"
    );
    let doc = doc.expect("the gate still emits the full summary");
    let s = fake.repo_summary(&doc, "o/n");
    assert_eq!(s["health"]["quarantined"], 1, "{s}");
    assert!(ghgraph::sync::incomplete(&doc));
}

// ---------------------------------------------------------------------------
// 13. The sync summary golden — the seventh verb's byte-level record.

/// Same contract as tests/read_surface.rs's golden(): regenerate with
/// GHGRAPH_UPDATE_GOLDENS=1 and review the diff like code.
fn golden(name: &str, doc: &Value) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/goldens")
        .join(name);
    let got = serde_json::to_string_pretty(doc).unwrap() + "\n";
    if std::env::var_os("GHGRAPH_UPDATE_GOLDENS").is_some() {
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

#[test]
fn golden_sync_summary() {
    // Fixture responses make every summary field deterministic except the
    // enumerated timing fields (sync.rs module docs: subprocess_seconds,
    // sleep_seconds), masked here exactly as read_surface masks _meta
    // timing — and nothing else, or the golden proves less than it claims.
    let fake = Fake::new();
    fake.config(&base_config());
    let a = Pr::new("PR_1", 1, "2026-07-20T10:00:00Z");
    let b = Pr::new("PR_2", 2, "2026-07-20T11:00:00Z");
    install_prs(&fake, &[&a, &b]);
    let mut doc = fake.sync_ok();
    for repo in doc["sync"]["repos"].as_array_mut().unwrap() {
        repo["cost"]["subprocess_seconds"] = json!("<SECS>");
        repo["cost"]["sleep_seconds"] = json!("<SECS>");
    }
    golden("sync.json", &doc);
}

// ---------------------------------------------------------------------------
// 14. retry_after rides the TRANSIENT envelope when gh returned a reset.

#[test]
fn targeted_rate_exhaustion_envelope_carries_retry_after() {
    let fake = Fake::new();
    fake.config(&base_config());
    // The PR_ID lookup succeeds (its rateLimit envelope teaches resetAt);
    // the hydration then hits the primary rate limit — typed RateExhausted,
    // one attempt, no quarantine (budget exhaustion is not the PR's fault).
    fake.write(
        "prid.json",
        &json!({"data": {"repository": {"pullRequest": {"id": "PR_1"}},
                 "rateLimit": rate_limit(4000)}})
        .to_string(),
    );
    fake.write(
        "stderr-PR_1",
        "gh: API rate limit exceeded for user (HTTP 403)",
    );
    let (code, doc, stderr) = fake.run(&["sync", "--pr", "o/n#1"]);
    assert_eq!(code, 2, "stderr:\n{stderr}");
    let err = &doc.expect("typed envelope on stdout")["error"];
    assert_eq!(err["code"], "TRANSIENT", "{err}");
    assert_eq!(
        err["retry_after"], "2026-08-01T00:00:00Z",
        "the reset the run learned rides the envelope: {err}"
    );
}
