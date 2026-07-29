#![no_main]
//! Fuzz `Rfc3339Utc::parse` for totality over arbitrary UTF-8:
//!   1. `parse` never panics, indexes out of bounds, or overflows — it is total,
//!      returning a typed `ParseError` for every input it does not accept.
//!   2. Every value it DOES accept round-trips: re-parsing its canonical string
//!      succeeds and yields the same instant. This is the parse/render adjunction
//!      the exhaustive civil-date test asserts on the civil side; here it is
//!      checked against the parser's own accepted set.
//!   2b. The SAFETY direction (the module's primary claim): everything `parse`
//!      accepts is a structurally valid Zulu string. Re-derived from the input
//!      independently of `parse`, so an over-acceptance that normalized a
//!      malformed input through `format_epoch` — which would round-trip cleanly
//!      and hide from property 2 — is caught here.
//!   3. The arithmetic on an accepted value (`checked_sub_secs`,
//!      `checked_sub_days`, `from_epoch`) never panics — saturation/`Option`,
//!      never overflow — so the harness covers the whole surface, not just the
//!      reject path.

use libfuzzer_sys::fuzz_target;

use ghgraph::time::Rfc3339Utc;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(t) = Rfc3339Utc::parse(s) {
        // Round-trip: a canonical string must re-parse to the same instant.
        // unwrap is the property — a value parse just produced that its own
        // renderer cannot round-trip is the counterexample this target hunts.
        let back = Rfc3339Utc::parse(t.as_str()).expect("canonical string must re-parse");
        assert_eq!(t.epoch(), back.epoch(), "round-trip changed the instant");

        // Safety direction: the accepted input must be EXACTLY the canonical form,
        // or the canonical form with a fractional part (`. 1*DIGIT`) spliced in
        // before the `Z`. This re-recognizes validity from `s` without calling
        // `parse` again, so an over-acceptance (a string parse admits but the RFC
        // forbids) becomes a counterexample here rather than passing silently.
        let canon = t.as_str().as_bytes(); // "YYYY-MM-DDTHH:MM:SSZ", always 20 bytes
        let sb = s.as_bytes();
        let structurally_valid = sb == canon
            || (sb.len() >= 22                            // 19 + '.' + >=1 digit + 'Z'
                && sb[..19] == canon[..19]                // date + 'T' + HH:MM:SS agree
                && sb[19] == b'.'                         // fractional marker
                && sb[sb.len() - 1] == b'Z'               // Zulu terminator
                && sb[20..sb.len() - 1].iter().all(u8::is_ascii_digit)); // 1*DIGIT
        assert!(
            structurally_valid,
            "parse accepted a non-Zulu-structured input: {s:?} (canonical {:?})",
            t.as_str()
        );

        // Arithmetic on an accepted value must saturate/Option, never panic.
        let _ = t.checked_sub_secs(u64::MAX);
        let _ = t.checked_sub_days(u32::MAX);
        let _ = Rfc3339Utc::from_epoch(t.epoch());
    }
});
