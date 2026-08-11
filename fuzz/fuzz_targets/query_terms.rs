#![no_main]
//! The GraphQL search-term builders (queries::discovery_terms and
//! backfill_terms) — the point where config-supplied identifiers become
//! `gh` argv. DESIGN's hard constraint is that untrusted text never reaches
//! argv; the builders honor it by interpolating NEWTYPES ONLY (RepoName,
//! Login), so the gate is really identity.rs's and this target witnesses
//! that the gate actually holds at the interpolation site.
//!
//! The load-bearing property is TERM ARITY. Every term these functions
//! build is a fixed number of whitespace-separated qualifiers:
//!
//!   repo:<r> updated:<...> sort:updated-desc is:pr involves:<login>
//!
//! A validated identifier contains no whitespace, so the arity is a
//! constant of the term's shape. If a RepoName or Login could ever carry a
//! space, the interpolation would silently ADD a qualifier — GitHub search
//! would then answer a different question than the caller asked, and the
//! archive would be quietly wrong rather than loudly broken. Arity is the
//! observable that catches that, and it catches it without this target
//! having to know what a legal login looks like: it re-derives the bound
//! from the newtypes' own output.
//!
//! Witnesses:
//!   1. Totality: neither builder panics on any config the deserializer
//!      accepts, any login the newtype accepts, or any stamp time.rs parses.
//!   2. Arity: each emitted term has exactly the token count its shape
//!      dictates — no identifier smuggles in a qualifier.
//!   3. No control characters: a term is a single argv word; a newline or
//!      NUL in it would be a shell/argv boundary violation even if the
//!      arity held.
//!   4. Provenance: every `involves:`/`review-requested:`/`reviewed-by:`
//!      value is one the caller actually passed (viewer or a member of
//!      people/added) — the builder never invents a subject.
//!   5. Determinism: same inputs, same terms.
//!
//! ```text
//! cargo fuzz run query_terms
//! ```

use libfuzzer_sys::fuzz_target;

use ghgraph::config::RepoConfig;
use ghgraph::identity::Login;
use ghgraph::queries::{backfill_terms, discovery_terms};
use ghgraph::sync::Stream;
use ghgraph::time::Rfc3339Utc;

/// Qualifiers whose value is an identifier the caller supplied. Used by
/// witness (4) to check the builder never names a subject of its own.
const SUBJECT_QUALIFIERS: [&str; 3] = ["involves:", "review-requested:", "reviewed-by:"];

fuzz_target!(|input: (&str, &str, Vec<&str>, &str, Option<&str>, bool)| {
    let (repo_json, viewer_s, people_s, since_s, until_s, is_issue) = input;

    // RepoConfig's `issues`/`bots` fields are private, so build it the way
    // production does — through Deserialize, which runs the identity gate.
    // A rejected config is not interesting here; identity_gate/config_gate
    // already own that boundary.
    let Ok(rc) = serde_json::from_str::<RepoConfig>(repo_json) else {
        return;
    };
    let Ok(viewer) = Login::new(viewer_s) else {
        return;
    };
    let Ok(since) = Rfc3339Utc::parse(since_s) else {
        return;
    };
    let until = match until_s {
        Some(u) => match Rfc3339Utc::parse(u) {
            Ok(t) => Some(t),
            Err(_) => return,
        },
        None => None,
    };
    let people: Vec<Login> = people_s.iter().filter_map(|p| Login::new(p).ok()).collect();

    let stream = if is_issue { Stream::Issue } else { Stream::Pr };

    // (1) Totality.
    let terms = discovery_terms(&rc, &viewer, &people, &since, until.as_ref(), stream);
    // (5) Determinism.
    let again = discovery_terms(&rc, &viewer, &people, &since, until.as_ref(), stream);
    assert_eq!(terms, again, "discovery_terms is not deterministic");

    // The set of subjects the caller authorized the builder to name.
    let mut allowed: Vec<&str> = people.iter().map(Login::as_str).collect();
    allowed.push(viewer.as_str());

    check_terms(&terms, rc.repo.as_str(), &allowed, "discovery");

    // backfill_terms: same injection boundary, one term per added login.
    let added = people;
    let back = backfill_terms(&rc, &added, &since, until.as_ref());
    let back_again = backfill_terms(&rc, &added, &since, until.as_ref());
    assert_eq!(back, back_again, "backfill_terms is not deterministic");
    assert_eq!(
        back.len(),
        added.len(),
        "backfill emits exactly one term per added login"
    );
    let back_allowed: Vec<&str> = added.iter().map(Login::as_str).collect();
    check_terms(&back, rc.repo.as_str(), &back_allowed, "backfill");
});

fn check_terms(terms: &[String], repo: &str, allowed: &[&str], which: &str) {
    for t in terms {
        // (3) A term is one argv word per qualifier — never a control byte.
        assert!(
            !t.chars().any(|c| c.is_control()),
            "{which}: control character in term {t:?}"
        );

        let tokens: Vec<&str> = t.split(' ').collect();

        // (2) Arity. Splitting on a single space (not split_whitespace,
        // which would silently collapse a doubled space and hide exactly
        // the injection this checks) must yield no empty token, and every
        // token must be a qualifier of the expected shape.
        assert!(
            tokens.iter().all(|tok| !tok.is_empty()),
            "{which}: empty token — doubled space in {t:?}"
        );

        // The base is always: repo:<r> updated:<...> sort:updated-desc
        // followed by is:pr / is:issue, then zero or one subject qualifier.
        assert!(
            (4..=5).contains(&tokens.len()),
            "{which}: unexpected arity {} in {t:?}",
            tokens.len()
        );
        assert_eq!(
            tokens[0],
            format!("repo:{repo}"),
            "{which}: repo qualifier not first or not the configured repo: {t:?}"
        );
        assert!(
            tokens[1].starts_with("updated:"),
            "{which}: expected updated: qualifier, got {:?}",
            tokens[1]
        );
        assert_eq!(tokens[2], "sort:updated-desc", "{which}: sort clobbered");
        assert!(
            tokens[3] == "is:pr" || tokens[3] == "is:issue",
            "{which}: expected a stream qualifier, got {:?}",
            tokens[3]
        );

        // (4) Provenance: a subject qualifier may only name a login the
        // caller passed in.
        if let Some(last) = tokens.get(4) {
            let Some(q) = SUBJECT_QUALIFIERS.iter().find(|q| last.starts_with(**q)) else {
                panic!("{which}: unknown trailing qualifier {last:?} in {t:?}");
            };
            let subject = &last[q.len()..];
            assert!(
                allowed.contains(&subject),
                "{which}: term names a subject the caller never supplied: {subject:?}"
            );
        }
    }
}
