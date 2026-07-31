#![no_main]
//! Totality witness for gh::parse_gh_version: the version gate's parser
//! runs over `gh --version` output — operator-controlled binary output,
//! but adversarial by posture — and must never panic (slicing is by
//! counted leading ASCII digits; a boundary mistake there would be a ⊥).
//! Accepted output is additionally sane: components round-trip through the
//! same "gh version X.Y.Z" shape the parser claims to recognize.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Some((a, b, c)) = ghgraph::gh::parse_gh_version(s) {
            // Re-render and re-parse: an accepted input's components must
            // survive the parser's own canonical shape (adjunction pin).
            let canon = format!("gh version {a}.{b}.{c}");
            assert_eq!(
                ghgraph::gh::parse_gh_version(&canon),
                Some((a, b, c)),
                "canonical form must re-parse identically"
            );
        }
    }
});
