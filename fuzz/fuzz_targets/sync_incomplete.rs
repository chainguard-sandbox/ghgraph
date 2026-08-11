#![no_main]
//! sync::incomplete — the run's disclosure gate. It reads a finished run
//! document and answers "is the archive known to be missing something?",
//! which is what turns DESIGN's "incompleteness is never silent" from a
//! claim into an output field.
//!
//! The load-bearing property is directional, and it is the opposite of the
//! usual one: FAIL OPEN. A document this function cannot read must count as
//! incomplete. A false `true` costs a spurious disclosure; a false `false`
//! silently claims a partial archive is whole — the failure the project says
//! it cannot tolerate. So the witness is one-sided: `false` must be earned,
//! `true` never needs justification.
//!
//! Witnesses:
//!   1. Totality: no panic on arbitrary JSON. `incomplete` indexes with
//!      `&r["health"]` on array elements it does not control; serde_json's
//!      immutable Index yields Null rather than panicking, and this target
//!      is what holds that assumption still (IndexMut would panic — a
//!      later edit to `&mut` would be caught here, not in production).
//!   2. Fail-open on shape: any document without a readable /sync/pr or
//!      /sync/repos array is incomplete.
//!   3. Earned completeness: if it returns false, an independent re-read
//!      confirms every gating tally is present AND zero — the check is
//!      written positively here (all fields present and clear) against
//!      incomplete's negative form (any field missing or nonzero), so a
//!      polarity slip in either shows up as a disagreement.
//!   4. Determinism.
//!
//! ```text
//! cargo fuzz run sync_incomplete
//! ```

use libfuzzer_sys::fuzz_target;
use serde_json::Value;

/// Per-repo tallies that must all be present and zero for a repo to be
/// complete. Mirrors incomplete()'s list, restated positively.
const ZERO_TALLIES: [&str; 3] = ["truncated", "quarantined", "discovery_truncated"];

/// Independent re-derivation of "this repo is complete", written in the
/// positive: every gate must affirmatively read clear.
fn repo_is_complete(r: &Value) -> bool {
    let Some(health) = r.get("health") else {
        return false;
    };
    for k in ZERO_TALLIES {
        if health.get(k).and_then(Value::as_u64) != Some(0) {
            return false;
        }
    }
    if health.get("deferred_at_floor").and_then(Value::as_bool) != Some(false) {
        return false;
    }
    match health.get("errors").and_then(Value::as_array) {
        Some(e) => e.is_empty(),
        None => false,
    }
}

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(doc) = serde_json::from_str::<Value>(s) else {
        return;
    };

    // (1) Totality and (4) determinism.
    let got = ghgraph::sync::incomplete(&doc);
    assert_eq!(got, ghgraph::sync::incomplete(&doc), "not deterministic");

    // Independent re-derivation of the same judgment.
    let expected = match doc.pointer("/sync/pr") {
        Some(pr) => pr.get("truncated").and_then(Value::as_bool) != Some(false),
        None => match doc.pointer("/sync/repos").and_then(Value::as_array) {
            // (2) Fail open: unreadable shape is incomplete.
            None => true,
            Some(repos) => !repos.iter().all(repo_is_complete),
        },
    };

    // (3) The one-sided witness. Equality is the strong form; if the two
    // ever disagree, report which direction — a spurious `true` is a cost,
    // a spurious `false` is the silent-partial-archive bug.
    assert_eq!(
        got, expected,
        "incomplete disagreed with the positive re-derivation \
         (got={got}, expected={expected}) on: {s:?}"
    );
    if !got {
        assert!(
            expected == got,
            "completeness claimed but not earned on: {s:?}"
        );
    }
});
