#![no_main]
//! Fuzz the epoch->value direction and the checked arithmetic — the surface
//! rfc3339_parse only touches at fixed extremes on parse-derived (in-range)
//! instants. Here the scalars are drawn from the input, so `from_epoch` is
//! exercised over ALL of i64, not just the 0001..=9999 band `parse` yields.
//!
//! Properties, over an arbitrary (i64, u64, u32) drawn from the bytes:
//!   1. `from_epoch` is total over every i64 — no panic or overflow in the
//!      unchecked civil conversion that runs BEFORE the range guard.
//!   2. Every value `from_epoch` accepts round-trips: its canonical string
//!      re-parses to the same epoch (the epoch->string->epoch direction, the
//!      mirror of rfc3339_parse's string->epoch->string one).
//!   3. `checked_sub_secs`/`checked_sub_days` never panic and never turn
//!      "minus" into "plus": any accepted result is no later than the original.

use libfuzzer_sys::fuzz_target;

use ghgraph::time::Rfc3339Utc;

fuzz_target!(|data: &[u8]| {
    // Draw fixed-width scalars from the front of the input, zero-padded so short
    // inputs still exercise the path — no dependency on the `arbitrary` crate.
    let mut buf = [0u8; 20];
    let n = data.len().min(20);
    buf[..n].copy_from_slice(&data[..n]);
    let secs = i64::from_le_bytes(buf[0..8].try_into().unwrap());
    let sub_secs = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let sub_days = u32::from_le_bytes(buf[16..20].try_into().unwrap());

    // 1 + 2: from_epoch is total; whatever it accepts must round-trip exactly.
    if let Some(t) = Rfc3339Utc::from_epoch(secs) {
        assert_eq!(t.epoch(), secs, "from_epoch must preserve the epoch it accepts");
        let back = Rfc3339Utc::parse(t.as_str()).expect("canonical string must re-parse");
        assert_eq!(back.epoch(), secs, "epoch -> string -> epoch must round-trip");

        // 3: the subtractions saturate to None, never panic, and never run
        // the clock forward (the whole point of the unsigned-argument design).
        if let Some(u) = t.checked_sub_secs(sub_secs) {
            assert!(u.epoch() <= t.epoch(), "checked_sub_secs moved forward in time");
        }
        if let Some(u) = t.checked_sub_days(sub_days) {
            assert!(u.epoch() <= t.epoch(), "checked_sub_days moved forward in time");
        }
    }
});
