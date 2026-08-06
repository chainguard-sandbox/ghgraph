#![no_main]
//! Fuzz attention.rs's derivation-input readers — the functions that meet
//! arbitrary archive text (the `query` verb proves the archive is reachable
//! by arbitrary SQL, so a derivation input is validated where it is
//! consumed, and these are the consumers).
//!
//! The load-bearing property is directional: UNCERTAINTY NEVER PROVES.
//! Over arbitrary input:
//!   1. Totality: no reader panics — accept or degrade, never abort.
//!   2. review_freshness returns Fresh/Stale only with parsed proof in
//!      hand (the submitted stamp AND the deciding bound both re-parse);
//!      everything else must degrade to Unknown, because a Fresh the input
//!      cannot prove is a lie in ready_to_merge — the expensive failure.
//!   3. json_array_nonempty returns true only for input an independent
//!      parse re-recognizes as a non-empty JSON array (over-acceptance
//!      would silently clear the untriaged demand).
//!   4. is_maintainer_assoc accepts exactly the three proven-affiliation
//!      tokens — anything else must read false (drift escalates, never
//!      triages).
//!
//! ```text
//! cargo fuzz run attention_inputs
//! ```

use libfuzzer_sys::fuzz_target;

use ghgraph::attention::{
    PushBounds, ReviewFreshness, is_maintainer_assoc, json_array_nonempty, review_freshness,
};
use ghgraph::time::Rfc3339Utc;

fuzz_target!(|input: (&str, Option<&str>, Option<&str>)| {
    let (text, commit, flip) = input;

    // (3) Only a re-recognized non-empty array proves triage.
    if json_array_nonempty(Some(text)) {
        let independent = serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|v| v.as_array().map(|a| !a.is_empty()))
            .unwrap_or(false);
        assert!(independent, "json_array_nonempty over-accepted: {text:?}");
    }
    assert!(!json_array_nonempty(None), "NULL can never prove triage");

    // (4) Exactly the three tokens.
    if is_maintainer_assoc(Some(text)) {
        assert!(
            matches!(text, "OWNER" | "MEMBER" | "COLLABORATOR"),
            "is_maintainer_assoc over-accepted: {text:?}"
        );
    }
    assert!(!is_maintainer_assoc(None));

    // (2) Fresh and Stale each require their proof to re-parse; a bound
    // (or stamp) this module cannot read must degrade to Unknown.
    let bounds = PushBounds {
        head_committed_at: commit,
        head_flip_observed_at: flip,
    };
    match review_freshness(text, &bounds) {
        ReviewFreshness::Fresh => {
            assert!(Rfc3339Utc::parse(text).is_ok(), "Fresh without a stamp");
            assert!(
                flip.is_some_and(|f| Rfc3339Utc::parse(f).is_ok()),
                "Fresh without a parsed fresh-side bound"
            );
        }
        ReviewFreshness::Stale => {
            assert!(Rfc3339Utc::parse(text).is_ok(), "Stale without a stamp");
            assert!(
                commit.is_some_and(|c| Rfc3339Utc::parse(c).is_ok()),
                "Stale without a parsed stale-side bound"
            );
        }
        ReviewFreshness::Unknown => {}
    }
});
