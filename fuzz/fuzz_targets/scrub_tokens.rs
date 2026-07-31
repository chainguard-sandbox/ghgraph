#![no_main]
//! Both directions for the token scrubber (gh::scrub_tokens), plus totality:
//!   1. no token shape survives scrubbing — re-recognized by an independent
//!      implementation of the same shape definition (prefix + >=8 word
//!      chars, no left boundary), so an over-acceptance in the scrubber's
//!      own scanner cannot hide itself;
//!   2. clean input passes through byte-identical (no over-eager rewriting);
//!   3. idempotence: scrubbing a scrubbed string is a fixed point;
//!   4. totality: no panic on arbitrary UTF-8 (the from_utf8 expect inside
//!      scrub_tokens is a proof obligation this target hammers).

use libfuzzer_sys::fuzz_target;

/// Independent re-recognizer for the token shape. Deliberately naive
/// (every position, every prefix) — different code path from the scrubber's
/// single-pass scanner.
fn has_token_shape(s: &str) -> bool {
    let b = s.as_bytes();
    let prefixes: [&[u8]; 6] = [b"ghp_", b"gho_", b"ghu_", b"ghs_", b"ghr_", b"github_pat_"];
    (0..b.len()).any(|i| {
        prefixes.iter().any(|p| {
            b[i..].starts_with(p)
                && b[i + p.len()..]
                    .iter()
                    .take_while(|&&c| c.is_ascii_alphanumeric() || c == b'_')
                    .count()
                    >= 8
        })
    })
}

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let out = ghgraph::gh::scrub_tokens(s);
        assert!(!has_token_shape(&out), "token shape survived: {out:?}");
        assert_eq!(ghgraph::gh::scrub_tokens(&out), out, "not idempotent");
        if !has_token_shape(s) {
            assert_eq!(out, s, "clean text must pass through identically");
        }
    }
});
