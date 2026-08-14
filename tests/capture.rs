//! Live-capture harness for the parse fixtures. Ignored by default: it
//! talks to real GitHub through `gh` (read-only) and rewrites
//! tests/fixtures/*.json. Re-capture:
//!
//! ```text
//! cargo test --test capture -- --ignored --nocapture
//! ```
//!
//! The harness runs the document consts from queries.rs VERBATIM — the same
//! strings sync will send — so a fixture can only exist if the live schema
//! accepted the real document. That is the pin: parse.rs's
//! deny-both-directions types against these captures make document, parse
//! type, and live schema agree or fail loudly. When a parse.rs fixture test
//! breaks after a re-capture, the schema moved; diff the fixture.
//!
//! Capture targets are pinned, with what each was chosen to exercise:
//!
//! * discovery_page.json — DISCOVERY over a real working-scope term built by
//!   `discovery_terms` (repo:cli/cli, viewer williammartin, since
//!   2026-07-01), so the term builder is in the captured path too.
//! * hydrate_pr_threads.json — cli/cli#13864: review threads, an
//!   opinionated review, a closing-issue reference (empty comments).
//! * hydrate_pr_comments.json — cli/cli#13987: top-level comments and a
//!   review request (empty threads).
//! * hydrate_pr_ghost.json — worldpay-saml/vantiv-sdk-for-python#44, whose
//!   author deleted their account: pins how the platform actually renders a
//!   deleted author (the `ghost` User; the schema-permitted `author: null`
//!   is pinned by hand in parse.rs instead).
//! * threads_page.json — THREADS_PAGE against cli/cli#13864, no cursor.
//! * comments_page.json — COMMENTS_PAGE against cli/cli#13987, no cursor.
//! * pr_id.json — PR_ID for cli/cli#13864; the capture asserts it returns
//!   the node id the hydration fixtures pin, tying the two documents to
//!   one PR.
//! * refresh_pr.json — REFRESH_PR against cli/cli#13987: a populated
//!   comments tail (small enough for last: K to cover fully) beside the
//!   same review request the hydration fixture pins.
//! * tail_comments.json — TAIL_COMMENTS against cli/cli#13987, no cursor.
//! * skeleton_threads_page.json — SKELETON_THREADS_PAGE against
//!   cli/cli#13864: a populated skeleton thread whose nested comment
//!   carries no body.
//! * thread_bodies.json — THREAD_BODIES against cli/cli#13864's one review
//!   thread (PRRT_kwDODKw3uc6QWWXy), tying the thread-rooted document to
//!   the same thread the hydration fixture carries.
//! * hydrate_issue_assigned.json — HYDRATE_ISSUE against cli/cli issue
//!   #13016: two assignees and three labels on one page (the counted
//!   connections populated), one comment — the single-page shape.
//! * hydrate_issue_paged.json — HYDRATE_ISSUE against cli/cli issue
//!   #13840: 100+ comments, so the first page reports hasNextPage — the
//!   shape whose walk earns (or withholds) the comments witness.
//! * issue_comments_page.json — ISSUE_COMMENTS_PAGE against the same
//!   issue, no cursor, tying the follow-up document to the same anchor.
//! * comments_minimized.json — TAIL_COMMENTS against cli/cli#13918, whose
//!   ONLY top-level comment is minimized (spam). The enablement gate for
//!   the layered-refresh conservation check: the capture asserts that
//!   totalCount counts the minimized node (totalCount == nodes.len(), one
//!   node isMinimized) — if GitHub excluded minimized comments from
//!   totalCount, this PR would read totalCount 0 and the check's counting
//!   universe would bias toward false passes.
//!
//! One `#[ignore]` test per fixture group, so a single fixture can be
//! re-captured without churning the others — the pinned targets were chosen
//! for properties (a bot in the discovery page, a private-team request) a
//! later full re-capture is not guaranteed to reproduce.
//!
//! All public data. `gh` must be authenticated (`make doctor`).

use std::io::Write as _;
use std::process::{Command, Stdio};

fn gh_graphql(document: &str, vars: &[(&str, &str)]) -> String {
    // Query on stdin (`-F query=@-`, the gh.rs invariant: argv limits can
    // never apply to a document). Variables use `-f` — raw strings, no `-F`
    // type magic.
    let mut cmd = Command::new("gh");
    cmd.args(["api", "graphql", "-F", "query=@-"]);
    for (k, v) in vars {
        cmd.args(["-f", &format!("{k}={v}")]);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn gh");
    child
        .stdin
        .take()
        .expect("stdin piped")
        .write_all(document.as_bytes())
        .expect("write document");
    let out = child.wait_with_output().expect("gh exit");
    assert!(
        out.status.success(),
        "gh api graphql failed for vars {vars:?}"
    );
    String::from_utf8(out.stdout).expect("gh output is UTF-8")
}

fn write_fixture(name: &str, raw: &str) {
    // Pretty-printed so a re-capture diffs line-by-line in git.
    let v: serde_json::Value = serde_json::from_str(raw).expect("gh output is JSON");
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let mut pretty = serde_json::to_string_pretty(&v).expect("re-serialize");
    pretty.push('\n');
    std::fs::write(&path, pretty).expect("write fixture");
    println!("captured {path}");
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_discovery_page() {
    // The discovery term goes through the real builder, not a hand-written
    // string: config::parse + discovery_terms are part of what is captured.
    let cfg = ghgraph::config::parse(
        r#"{"viewer":"williammartin","repos":["cli/cli"]}"#,
        "<capture>",
    )
    .expect("capture config parses");
    let since = ghgraph::time::Rfc3339Utc::parse("2026-07-01T00:00:00Z").unwrap();
    let terms = ghgraph::queries::discovery_terms(
        &cfg.repos[0].resolved(),
        &cfg.viewer,
        &cfg.people,
        &since,
        None,
        ghgraph::sync::Stream::Pr,
    );
    // terms[0] is the involves: flavor (queries.rs pins the order).
    write_fixture(
        "discovery_page.json",
        &gh_graphql(ghgraph::queries::DISCOVERY, &[("q", &terms[0])]),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_hydrations() {
    for (name, id) in [
        ("hydrate_pr_threads.json", "PR_kwDODKw3uc7xAi-o"),
        ("hydrate_pr_comments.json", "PR_kwDODKw3uc73afq3"),
        ("hydrate_pr_ghost.json", "PR_kwDOSbh2Nc7av-g3"),
    ] {
        write_fixture(
            name,
            &gh_graphql(ghgraph::queries::HYDRATE_PR, &[("id", id)]),
        );
    }
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_issue_hydrations() {
    // #13016: assignees and labels populated, single-page comments.
    write_fixture(
        "hydrate_issue_assigned.json",
        &gh_graphql(
            ghgraph::queries::HYDRATE_ISSUE,
            &[("id", "I_kwDODKw3uc71-pak")],
        ),
    );
    // #13840: a 100+-comment issue — the first page must report another
    // page, or the multi-page witness shape has no live pin.
    let raw = gh_graphql(
        ghgraph::queries::HYDRATE_ISSUE,
        &[("id", "I_kwDODKw3uc8AAAABIWsa1Q")],
    );
    let v: serde_json::Value = serde_json::from_str(&raw).expect("JSON");
    assert_eq!(
        v["data"]["node"]["comments"]["pageInfo"]["hasNextPage"], true,
        "the pinned issue no longer overflows one page — pick a new anchor"
    );
    write_fixture("hydrate_issue_paged.json", &raw);
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_issue_comments_page() {
    write_fixture(
        "issue_comments_page.json",
        &gh_graphql(
            ghgraph::queries::ISSUE_COMMENTS_PAGE,
            &[("id", "I_kwDODKw3uc8AAAABIWsa1Q")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_threads_page() {
    write_fixture(
        "threads_page.json",
        &gh_graphql(
            ghgraph::queries::THREADS_PAGE,
            &[("id", "PR_kwDODKw3uc7xAi-o")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_comments_page() {
    write_fixture(
        "comments_page.json",
        &gh_graphql(
            ghgraph::queries::COMMENTS_PAGE,
            &[("id", "PR_kwDODKw3uc73afq3")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_refresh_pr() {
    write_fixture(
        "refresh_pr.json",
        &gh_graphql(
            &ghgraph::queries::refresh_pr_document(),
            &[("id", "PR_kwDODKw3uc73afq3")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_tail_comments() {
    write_fixture(
        "tail_comments.json",
        &gh_graphql(
            &ghgraph::queries::tail_comments_document(),
            &[("id", "PR_kwDODKw3uc73afq3")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_skeleton_threads_page() {
    write_fixture(
        "skeleton_threads_page.json",
        &gh_graphql(
            ghgraph::queries::SKELETON_THREADS_PAGE,
            &[("id", "PR_kwDODKw3uc7xAi-o")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_thread_bodies() {
    write_fixture(
        "thread_bodies.json",
        &gh_graphql(
            ghgraph::queries::THREAD_BODIES,
            &[("id", "PRRT_kwDODKw3uc6QWWXy")],
        ),
    );
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_comments_minimized() {
    let raw = gh_graphql(
        &ghgraph::queries::tail_comments_document(),
        &[("id", "PR_kwDODKw3uc7zWRTx")],
    );
    // The enablement gate (round-0 spec audit): the conservation check's
    // counting universe must include minimized comments, live-witnessed,
    // not assumed. This PR's ONLY top-level comment is minimized, so an
    // excluded-from-count regime would read totalCount 0 here.
    let v: serde_json::Value = serde_json::from_str(&raw).expect("JSON");
    let comments = &v["data"]["node"]["comments"];
    let nodes = comments["nodes"].as_array().expect("nodes array");
    assert_eq!(
        comments["totalCount"].as_i64(),
        Some(nodes.len() as i64),
        "tail must cover the whole connection for the count proof to read"
    );
    assert!(
        nodes.iter().any(|n| n["isMinimized"] == true),
        "the pinned PR's minimized comment is gone — pick a new anchor"
    );
    write_fixture("comments_minimized.json", &raw);
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_pr_id() {
    let raw = gh_graphql(
        &ghgraph::queries::pr_id_document(13864),
        &[("owner", "cli"), ("name", "cli")],
    );
    // The lookup and the hydration fixtures must name the same PR: the id
    // returned here is the id capture_threads_page hydrates.
    let v: serde_json::Value = serde_json::from_str(&raw).expect("JSON");
    assert_eq!(
        v["data"]["repository"]["pullRequest"]["id"], "PR_kwDODKw3uc7xAi-o",
        "PR_ID must resolve cli/cli#13864 to the pinned hydration id"
    );
    write_fixture("pr_id.json", &raw);
}

#[test]
#[ignore = "talks to live GitHub and rewrites tests/fixtures/ — run explicitly"]
fn capture_type_backfill() {
    // Ids come from the already-captured hydration fixture — the same
    // comment node ids the archive would store — so the capture exercises
    // the lane's real input shape: known ids in, scalar typenames out.
    // The null-element outcome is NOT capturable on demand (an undecodable
    // id is a GraphQL error, not a null node; a decodable-but-dead one
    // requires a deleted comment to exist) — the synthetic parse test
    // carries that shape instead.
    let hydrate: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/hydrate_pr_comments.json"))
            .expect("hydrate fixture parses");
    let ids: Vec<String> = hydrate["data"]["node"]["comments"]["nodes"]
        .as_array()
        .expect("fixture carries comments")
        .iter()
        .filter_map(|c| c["id"].as_str().map(str::to_string))
        .take(2)
        .collect();
    assert!(
        !ids.is_empty(),
        "fixture must carry at least one comment id"
    );
    let vars: Vec<(&str, &str)> = ids.iter().map(|i| ("ids[]", i.as_str())).collect();
    write_fixture(
        "type_backfill.json",
        &gh_graphql(ghgraph::queries::TYPE_BACKFILL, &vars),
    );
}
