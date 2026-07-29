#![no_main]
//! Fuzz `Rfc3339Utc::parse` for totality over arbitrary UTF-8:
//!   1. `parse` never panics, indexes out of bounds, or overflows — it is total,
//!      returning a typed `ParseError` for every input it does not accept.
//!   2. Every value it DOES accept round-trips: re-parsing its canonical string
//!      succeeds and yields the same instant. This is the parse/render adjunction
//!      the exhaustive civil-date test asserts on the civil side; here it is
//!      checked against the parser's own accepted set.
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
        // Arithmetic on an accepted value must saturate/Option, never panic.
        let _ = t.checked_sub_secs(u64::MAX);
        let _ = t.checked_sub_days(u32::MAX);
        let _ = Rfc3339Utc::from_epoch(t.epoch());
    }
});
