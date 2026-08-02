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
