#![no_main]
//! Fuzz the config injection gate (`config::parse`).
//!
//! Two properties, over arbitrary UTF-8 input:
//!   1. `parse` never panics — total over every input.
//!   2. The load-bearing one: no identifier the gate ACCEPTS may contain a
//!      space or ':'. Those are the characters that could smuggle a second gh
//!      search qualifier ("owner/name involves:someone-else"), and the whole
//!      point of the gate is to make an accepted value safe to interpolate. If
//!      the gate ever lets one through, this target produces the counterexample.

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
            "gate accepted an identifier carrying a qualifier separator: {id:?}"
        );
    };
    // repo / viewer / people are interpolated into search qualifiers.
    for login in cfg.people.iter().chain([&cfg.viewer]) {
        clean(login);
    }
    for entry in &cfg.repos {
        let rc = entry.resolved();
        clean(&rc.repo);
        // exclude_authors is a filter today, not interpolated — checked as
        // defense in depth. The [bot] suffix's brackets are allowed; only a
        // space or ':' in the login core would be smuggling material.
        for a in &rc.exclude_authors {
            clean(a.strip_suffix("[bot]").unwrap_or(a));
        }
    }
});
