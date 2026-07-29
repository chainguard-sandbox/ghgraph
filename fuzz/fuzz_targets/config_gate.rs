#![no_main]
//! Fuzz the config injection gate end-to-end (`config::parse`).
//!
//! The gate now lives in the identity newtypes' Deserialize impls
//! (identity.rs; identity_gate fuzzes the constructors directly) — this
//! target proves the whole config surface still composes to the same two
//! properties, over arbitrary UTF-8 input:
//!   1. `parse` never panics — total over every input.
//!   2. The load-bearing one: no identifier a parsed Config CARRIES may
//!      contain a space or ':'. Those are the characters that could smuggle a
//!      second gh search qualifier ("owner/name involves:someone-else"), and
//!      the whole point of the types is that a carried value is safe to
//!      interpolate. If serde plumbing ever routes around the constructors,
//!      this target produces the counterexample.

use libfuzzer_sys::fuzz_target;

use ghgraph::config;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(cfg) = config::parse(s, "<fuzz>") else {
        return;
    };
    let clean = |id: &str| {
        assert!(
            !id.contains(' ') && !id.contains(':'),
            "config carries an identifier with a qualifier separator: {id:?}"
        );
    };
    // repo / viewer / people are interpolated into search qualifiers.
    for login in cfg.people.iter().chain([&cfg.viewer]) {
        clean(login.as_str());
    }
    for entry in &cfg.repos {
        let rc = entry.resolved();
        clean(rc.repo.as_str());
        // exclude_authors is a filter today, not interpolated — checked as
        // defense in depth ahead of the milestone-4 move to server-side
        // `-author:x` terms.
        for a in &rc.exclude_authors {
            clean(a.login().as_str());
        }
    }
});
