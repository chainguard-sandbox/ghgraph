#![no_main]
//! Fuzz the response parse boundary (parse.rs) — the first code that touches
//! bytes gh brought back from the network.
//!
//! Properties, over arbitrary bytes:
//!   1. Totality: JSON or not, matched shape or not, no parse function
//!      panics — accept or reject, never abort. Precondition, named: the
//!      Value comes from serde_json's byte parser, whose default 128-level
//!      nesting cap is what bounds the typed deserializer's recursion — the
//!      same precondition production holds (gh.rs, Response.data). A
//!      hand-built Value deeper than a few thousand levels would overflow;
//!      none is constructible through either path.
//!   2. Round-trip stability, the direction a fixture cannot witness: every
//!      ACCEPTED value re-serializes (the harness-only Serialize) and
//!      re-parses to the same value. With deny_unknown_fields + required
//!      fields this pins type = shape from the parser's side: nothing
//!      accepted is dropped or invented in flight, and normalization
//!      (timestamp canonicalization) is idempotent.
//!   3. Rejection carries no input: a ParseError's entire output is one of
//!      nine fixed strings — a pure function of which document failed —
//!      so no fragment of the (untrusted) document can ride along.
//!
//! Corpus: seed with tests/fixtures/*.json so mutation starts from real
//! GitHub shapes instead of discovering JSON syntax from zeroes:
//!
//! ```text
//! cargo fuzz run response_parse fuzz/corpus/response_parse ../tests/fixtures
//! ```

use libfuzzer_sys::fuzz_target;

use ghgraph::parse;

fuzz_target!(|data: &[u8]| {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };

    match parse::discovery(&v) {
        Ok(page) => {
            let re = serde_json::json!({ "search": page });
            assert_eq!(
                parse::discovery(&re).as_ref(),
                Ok(&page),
                "discovery round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::hydrate_pr(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::hydrate_pr(&re).as_ref(),
                Ok(&node),
                "hydrate_pr round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::threads_page(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::threads_page(&re).as_ref(),
                Ok(&node),
                "threads_page round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::comments_page(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::comments_page(&re).as_ref(),
                Ok(&node),
                "comments_page round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::refresh_pr(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::refresh_pr(&re).as_ref(),
                Ok(&node),
                "refresh_pr round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::tail_comments(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::tail_comments(&re).as_ref(),
                Ok(&node),
                "tail_comments round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::skeleton_threads_page(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::skeleton_threads_page(&re).as_ref(),
                Ok(&node),
                "skeleton_threads_page round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::thread_bodies(&v) {
        Ok(node) => {
            let re = serde_json::json!({ "node": node });
            assert_eq!(
                parse::thread_bodies(&re).as_ref(),
                Ok(&node),
                "thread_bodies round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }

    match parse::pr_id(&v) {
        Ok(repo) => {
            let re = serde_json::json!({ "repository": repo });
            assert_eq!(
                parse::pr_id(&re).as_ref(),
                Ok(&repo),
                "pr_id round-trip must be identity"
            );
        }
        Err(e) => assert_fixed_message(&e),
    }
});

/// The error's entire Display and Debug output must be the fixed string its
/// document determines — input-independent by exact equality, which is the
/// sound form of "never echoes" (substring scans false-positive whenever
/// the input happens to contain a piece of the fixed message).
fn assert_fixed_message(e: &parse::ParseError) {
    assert_eq!(
        e.to_string(),
        format!(
            "response does not match the {} document's parse type \
             (ghgraph's selection and GitHub's live schema disagree)",
            e.doc
        )
    );
    assert_eq!(format!("{e:?}"), format!("ParseError {{ doc: {:?} }}", e.doc));
}
