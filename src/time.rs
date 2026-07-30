//! RFC 3339 UTC timestamps, in std alone (no time crate — see Cargo.toml).
//!
//! GitHub emits timestamps as RFC 3339 "Zulu" strings, e.g.
//! `2026-07-24T13:59:00Z`, occasionally with a fractional-second part. This
//! module accepts exactly that shape and nothing else: [`Rfc3339Utc::parse`] is
//! Z-only and total — every input either produces a canonical value or a
//! [`ParseError`], never a mis-parse. A non-`Z` terminator, a space separator,
//! out-of-range or calendar-invalid fields, and the RFC-permitted forms listed
//! under "Divergences" below are all refused. A fractional part is accepted and
//! truncated to whole seconds (the archive's granularity; the watermark overlap
//! window is minutes wide).
//!
//! Divergences from RFC 3339 §5.6, every one in the SAFE direction: `parse`
//! accepts a strict SUBSET of the grammar — for each field, the values it
//! admits are a subset of what the RFC admits — so it can never accept a string
//! the RFC rejects; it only declines valid-but-unwanted forms. Each narrowing
//! is sound only because the sole input is GitHub's API, which emits
//! uppercase-`Z`, offset-free, leap-second-free, four-digit-year timestamps. If
//! that input source ever broadens, revisit these — the offset and leap-second
//! cases especially:
//!   * Lowercase `t`/`z`: RFC §5.6 permits them ("the 'T' and 'Z' characters
//!     ... may alternatively be lower case"); we require uppercase (`t` falls
//!     out as `Malformed`, `z` as `NotZulu`).
//!   * Numeric offsets (`±HH:MM`), including `+00:00` and the semantically
//!     distinct `-00:00` ("offset to local time is unknown"): RFC permits them;
//!     we take `Z` only. A general parser would normalize an offset to UTC; we
//!     do not, because we never receive one.
//!   * Leap second `:60`: RFC §5.6 explicitly permits `time-second` = 60 "at
//!     the end of months in which a leap second occurs"; we reject it as
//!     `OutOfRange`. Unix epoch seconds cannot represent a leap second and
//!     GitHub does not emit one — declining is a policy choice, not the value
//!     being invalid per the RFC.
//!   * Year `0000`: the RFC's `4DIGIT` admits it syntactically; we require
//!     `0001..=9999` so the canonical form stays fixed-width and
//!     lexicographically ordered (see below).
//!
//! Why a newtype and not a bare String:
//!   * The stored form is CANONICAL and fixed-width (`YYYY-MM-DDTHH:MM:SSZ`, 20
//!     chars), so lexicographic order == chronological order — the property the
//!     schema's `ORDER BY` on timestamp columns relies on. Year is bounded to
//!     `0001..=9999` precisely so the width (and therefore that order) holds.
//!   * [`as_str`](Rfc3339Utc::as_str) is charset-bounded to `[0-9:TZ-]` — no
//!     whitespace, no `:` outside the fixed `HH:MM:SS` positions — so
//!     interpolating a watermark into a `gh` search qualifier cannot smuggle a
//!     second qualifier. `discovery_terms`' signature (queries.rs) completes
//!     that injection-safety argument by admitting only this type and the
//!     identity.rs newtypes; this module guarantees the charset of the
//!     canonical form.
//!
//! Errors: `parse` returns a [`ParseError`] leaf, not a classified
//! `crate::Error`. The actor who can fix a bad timestamp depends on where it
//! came from — malformed `gh` output is TRANSIENT, a corrupt stored watermark
//! is something else — so classification happens at the call site, per the
//! no-blanket-`From` discipline. Crucially, `ParseError` never carries the
//! offending text: the input is untrusted and must not reach an error message.

use std::fmt;

/// A canonical RFC 3339 UTC timestamp: `YYYY-MM-DDTHH:MM:SSZ`, second-granular,
/// year `0001..=9999`. Ordering and equality are by instant (the cached epoch),
/// so two inputs that differ only in a truncated fractional part compare equal.
#[derive(Clone, Debug)]
pub struct Rfc3339Utc {
    /// The canonical, normalized string. Private: the only ways to obtain one
    /// are `parse`, `from_epoch`, and `now`, each of which normalizes.
    canonical: String,
    /// Seconds since the Unix epoch. Cached so arithmetic and ordering do not
    /// re-parse; kept consistent with `canonical` by construction.
    epoch: i64,
}

/// Why a timestamp was rejected. Deliberately carries NO copy of the offending
/// input — the input is untrusted and must never reach an error message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Not terminated by a `Z` (an offset, a lowercase `z`, or trailing junk).
    NotZulu,
    /// Wrong length or shape (bad separators, non-digits, missing fields).
    Malformed,
    /// A field is out of range, or the date is calendar-invalid (e.g. Feb 29 in
    /// a non-leap year, month 13, second 60), or the year is outside 1..=9999.
    OutOfRange,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ParseError::NotZulu => "not a Z-terminated UTC (RFC 3339) timestamp",
            ParseError::Malformed => "malformed RFC 3339 timestamp",
            ParseError::OutOfRange => "RFC 3339 timestamp field outside the accepted range",
        };
        f.write_str(s)
    }
}

impl Rfc3339Utc {
    /// Parse a Z-only RFC 3339 UTC timestamp. Total: every input maps to a
    /// canonical value or a [`ParseError`], never a silent mis-parse. A
    /// fractional-second part is accepted and dropped.
    pub fn parse(s: &str) -> Result<Rfc3339Utc, ParseError> {
        let b = s.as_bytes();
        // Shortest valid form is exactly "YYYY-MM-DDTHH:MM:SSZ" (20 bytes).
        if b.len() < 20 {
            return Err(ParseError::Malformed);
        }
        if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return Err(ParseError::Malformed);
        }
        let year = digits(&b[0..4]).ok_or(ParseError::Malformed)?;
        let month = digits(&b[5..7]).ok_or(ParseError::Malformed)?;
        let day = digits(&b[8..10]).ok_or(ParseError::Malformed)?;
        let hour = digits(&b[11..13]).ok_or(ParseError::Malformed)?;
        let minute = digits(&b[14..16]).ok_or(ParseError::Malformed)?;
        let second = digits(&b[17..19]).ok_or(ParseError::Malformed)?;

        // Position 19 onward: either "Z", or ".<one-or-more-digits>Z".
        if !ends_in_zulu(&b[19..]) {
            return Err(ParseError::NotZulu);
        }

        if !(1..=9999).contains(&year)
            || !(1..=12).contains(&month)
            || day < 1
            || day > days_in_month(year, month)
            || hour > 23
            || minute > 59
            || second > 59
        {
            return Err(ParseError::OutOfRange);
        }

        let epoch =
            days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
        Ok(Rfc3339Utc {
            canonical: format_epoch(epoch),
            epoch,
        })
    }

    /// The current instant, truncated to whole seconds. Uses the system clock
    /// (std, no time crate). A clock set before 1970 or past year 9999 is not a
    /// representable timestamp; rather than panic, `now` falls back to the Unix
    /// epoch in that (practically unreachable) case.
    pub fn now() -> Rfc3339Utc {
        // try_from (not `as i64`) so a u64 second-count above i64::MAX does not
        // wrap to a small/negative residue — an unrepresentable clock resolves
        // to the Unix-epoch fallback, matching the doc above.
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok())
            .unwrap_or(0);
        Rfc3339Utc::from_epoch(secs).unwrap_or_else(Rfc3339Utc::unix_epoch)
    }

    /// Build from seconds-since-Unix-epoch, or `None` if the instant falls
    /// outside the representable year range `0001..=9999` (which would break the
    /// fixed-width, lexicographically-ordered canonical form).
    pub fn from_epoch(secs: i64) -> Option<Rfc3339Utc> {
        let (year, _, _) = civil_from_days(secs.div_euclid(86_400));
        if !(1..=9999).contains(&year) {
            return None;
        }
        Some(Rfc3339Utc {
            canonical: format_epoch(secs),
            epoch: secs,
        })
    }

    /// The canonical `YYYY-MM-DDTHH:MM:SSZ` form. Charset is `[0-9:TZ-]` with no
    /// whitespace (see module docs).
    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    /// Seconds since the Unix epoch.
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// This instant minus `secs` seconds (the watermark overlap window is built
    /// this way). `secs` is unsigned so "minus" cannot silently become "plus".
    /// `None` if the result falls out of the representable range.
    pub fn checked_sub_secs(&self, secs: u64) -> Option<Rfc3339Utc> {
        let secs = i64::try_from(secs).ok()?;
        Rfc3339Utc::from_epoch(self.epoch.checked_sub(secs)?)
    }

    /// This instant minus `days` days (the re-verify tiers are built this way).
    /// `None` on overflow or if the result falls out of the representable range.
    pub fn checked_sub_days(&self, days: u32) -> Option<Rfc3339Utc> {
        let delta = i64::from(days).checked_mul(86_400)?;
        Rfc3339Utc::from_epoch(self.epoch.checked_sub(delta)?)
    }

    fn unix_epoch() -> Rfc3339Utc {
        Rfc3339Utc {
            canonical: String::from("1970-01-01T00:00:00Z"),
            epoch: 0,
        }
    }
}

impl fmt::Display for Rfc3339Utc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.canonical)
    }
}

// Equality and ordering are by instant. Two values with the same epoch always
// carry the same canonical string (it is a pure function of the epoch), so this
// is consistent with structural equality without depending on it.
impl PartialEq for Rfc3339Utc {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch
    }
}
impl Eq for Rfc3339Utc {}
impl PartialOrd for Rfc3339Utc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Rfc3339Utc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.epoch.cmp(&other.epoch)
    }
}

/// Parse a run of ASCII digits into an `i64`, or `None` if empty or non-digit.
fn digits(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for &c in bytes {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + i64::from(c - b'0');
    }
    Some(v)
}

/// True iff `tail` is `Z` or `.<one-or-more-digits>Z` and nothing else.
fn ends_in_zulu(tail: &[u8]) -> bool {
    match tail.first() {
        Some(b'Z') => tail.len() == 1,
        Some(b'.') => {
            let frac = &tail[1..];
            // Need at least one fractional digit and the terminating Z.
            frac.len() >= 2
                && *frac.last().unwrap() == b'Z'
                && frac[..frac.len() - 1].iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 0,
    }
}

// Days between the civil date and 1970-01-01, and its inverse. Howard Hinnant's
// public-domain algorithms (chrono-compatible), proleptic Gregorian. Exact in
// i64 for every representable year; no floating point.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    // The `else` (y < 0) arm is Hinnant's negative-year handling, kept for
    // fidelity to the published algorithm — but it is UNREACHABLE here:
    // `parse` range-checks year ∈ 1..=9999 before calling this, so y is 0 (only
    // for Jan/Feb of year 1) or positive, never negative. `civil_from_days` is
    // the direction that meets negative inputs (via `from_epoch`), and its own
    // negative arm IS exercised (from_epoch_rejects_every_instant_below_year_one).
    // Mutations to `y - 399` therefore survive as EQUIVALENT mutants: no
    // representable date reaches this branch, and the exhaustive civil test
    // covers only y ≥ 0. Left documented rather than annotated-away.
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if month > 2 { month - 3 } else { month + 9 }; // Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + day - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn format_epoch(epoch: i64) -> String {
    let days = epoch.div_euclid(86_400);
    let sod = epoch.rem_euclid(86_400); // seconds of day, always [0, 86399]
    let (year, month, day) = civil_from_days(days);
    let (hh, mm, ss) = (sod / 3_600, (sod % 3_600) / 60, sod % 60);
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_normalizes() {
        let t = Rfc3339Utc::parse("2026-07-24T13:59:00Z").unwrap();
        assert_eq!(t.as_str(), "2026-07-24T13:59:00Z");
    }

    #[test]
    fn epoch_is_correct_at_known_points() {
        assert_eq!(
            Rfc3339Utc::parse("1970-01-01T00:00:00Z").unwrap().epoch(),
            0
        );
        // 2000-01-01T00:00:00Z is 946684800 (verified against the civil algo).
        assert_eq!(
            Rfc3339Utc::parse("2000-01-01T00:00:00Z").unwrap().epoch(),
            946_684_800
        );
    }

    #[test]
    fn round_trips_epoch_and_string() {
        for s in [
            "1970-01-01T00:00:00Z",
            "2000-02-29T23:59:59Z", // leap day
            "2026-07-24T13:59:00Z",
            "9999-12-31T23:59:59Z",
            "0001-01-01T00:00:00Z",
        ] {
            let t = Rfc3339Utc::parse(s).unwrap();
            assert_eq!(t.as_str(), s, "canonical must equal a canonical input");
            let back = Rfc3339Utc::from_epoch(t.epoch()).unwrap();
            assert_eq!(back.as_str(), s, "epoch -> string must round-trip");
            assert_eq!(back, t);
        }
    }

    #[test]
    fn accepts_and_truncates_fractional_seconds() {
        let t = Rfc3339Utc::parse("2026-07-24T13:59:00.123456Z").unwrap();
        assert_eq!(t.as_str(), "2026-07-24T13:59:00Z");
        assert_eq!(
            t,
            Rfc3339Utc::parse("2026-07-24T13:59:00Z").unwrap(),
            "fractional part must not change the instant"
        );
    }

    #[test]
    fn rejects_non_zulu_forms() {
        for s in [
            "2026-07-24T13:59:00+00:00", // offset
            "2026-07-24T13:59:00z",      // lowercase z
            "2026-07-24T13:59:00X",      // wrong terminator (same length)
            "2026-07-24T13:59:00Z ",     // trailing space
            "2026-07-24T13:59:00.Z",     // empty fraction
        ] {
            assert_eq!(Rfc3339Utc::parse(s), Err(ParseError::NotZulu), "input: {s}");
        }
    }

    #[test]
    fn rejects_malformed() {
        // Each of these is specifically Malformed (too short, or bad separators)
        // — pinned exactly so a NotZulu/Malformed misclassification is caught.
        for s in [
            "",
            "2026",
            "2026-07-24",
            "not-a-date-at-all!!",
            "20260724T135900Z",     // missing separators
            "2026-07-24 13:59:00Z", // space where the 'T' separator must be (pos 10)
            "2026-07-24T13:59:00",  // no zone at all (too short)
            "2026-07-24t13:59:00Z", // lowercase 't': RFC §5.6 permits it, we
            // decline -> Malformed (b[10] != 'T'). The z side of this documented
            // narrowing is pinned in rejects_non_zulu_forms; this is the t side.
            // Exactly ONE separator wrong, the rest correct — each isolates a
            // single clause of the separator check (pos 10 covered above), so a
            // mutation that weakens one `||` to `&&` produces a mis-parse here.
            "2026x07-24T13:59:00Z", // pos 4: '-' -> 'x'
            "2026-07x24T13:59:00Z", // pos 7
            "2026-07-24T13x59:00Z", // pos 13
            "2026-07-24T13:59x00Z", // pos 16
        ] {
            assert_eq!(
                Rfc3339Utc::parse(s),
                Err(ParseError::Malformed),
                "input: {s}"
            );
        }
    }

    #[test]
    fn rejects_out_of_range_and_calendar_invalid() {
        for s in [
            "2026-13-01T00:00:00Z", // month 13
            "2026-00-01T00:00:00Z", // month 0
            "2026-07-32T00:00:00Z", // day 32
            "2026-07-00T00:00:00Z", // day 0 (the only input caught solely by `day < 1`)
            "2026-02-29T00:00:00Z", // Feb 29 in a non-leap year
            "2026-07-24T24:00:00Z", // hour 24
            "2026-07-24T13:60:00Z", // minute 60
            "2026-07-24T13:59:60Z", // second 60: a leap second, permitted by RFC 3339 §5.6 but deliberately declined (see module docs)
            "0000-01-01T00:00:00Z", // year 0
        ] {
            assert_eq!(
                Rfc3339Utc::parse(s),
                Err(ParseError::OutOfRange),
                "input: {s}"
            );
        }
    }

    #[test]
    fn ordering_matches_time_and_lexicographic() {
        let a = Rfc3339Utc::parse("2026-07-24T13:59:00Z").unwrap();
        let b = Rfc3339Utc::parse("2026-07-24T14:00:00Z").unwrap();
        assert!(a < b);
        // Canonical form is fixed-width, so byte order agrees with time order.
        assert!(a.as_str() < b.as_str());
    }

    #[test]
    fn checked_arithmetic_windows() {
        let t = Rfc3339Utc::parse("2026-07-24T00:10:00Z").unwrap();
        // Watermark overlap: 10 minutes back.
        assert_eq!(
            t.checked_sub_secs(600).unwrap().as_str(),
            "2026-07-24T00:00:00Z"
        );
        // Re-verify window: 7 days back.
        assert_eq!(
            t.checked_sub_days(7).unwrap().as_str(),
            "2026-07-17T00:10:00Z"
        );
        // Underflow past year 1 yields None, never a wrapped instant.
        assert!(
            Rfc3339Utc::parse("0001-01-01T00:00:00Z")
                .unwrap()
                .checked_sub_days(1)
                .is_none()
        );
    }

    #[test]
    fn now_round_trips() {
        let n = Rfc3339Utc::now();
        // now() is a valid canonical timestamp that re-parses to the same instant.
        assert_eq!(Rfc3339Utc::parse(n.as_str()).unwrap(), n);
    }

    #[test]
    fn canonical_charset_is_bounded() {
        // The injection-safety argument (discovery_terms, queries.rs)
        // depends on the canonical form
        // staying within [0-9:TZ-] — no whitespace, no ':' outside HH:MM:SS.
        // Check representative values AND the domain boundaries, where an extreme
        // epoch could otherwise widen or sign-prefix a field. (epoch_arith fuzzes
        // this same predicate over accepted values across all of i64; this pins
        // the boundaries as a fast, always-run unit assertion.)
        let samples = [
            Rfc3339Utc::parse("2026-07-24T13:59:00Z").unwrap(),
            Rfc3339Utc::from_epoch(0).unwrap(), // 1970-01-01
            Rfc3339Utc::parse("0001-01-01T00:00:00Z").unwrap(), // min year
            Rfc3339Utc::parse("9999-12-31T23:59:59Z").unwrap(), // max year
        ];
        for t in samples {
            assert!(
                t.as_str()
                    .bytes()
                    .all(|c| matches!(c, b'0'..=b'9' | b':' | b'T' | b'Z' | b'-')),
                "canonical form must stay within [0-9:TZ-], got {:?}",
                t.as_str()
            );
        }
    }

    #[test]
    fn civil_inverse_holds_for_every_representable_date() {
        // Exhaustive proof-by-enumeration over all ~3.65M valid dates in
        // 0001..=9999. Three properties, total over the whole domain:
        //   * civil_from_days is the exact inverse of days_from_civil;
        //   * the day count advances by exactly 1 per calendar day;
        //   * the canonical rendering is byte-monotonic in the day count — i.e.
        //     lexicographic order == chronological order, the property the
        //     schema's ORDER BY on timestamp columns depends on (declared at the
        //     top of this module, and what makes Eq/Ord-by-instant consistent
        //     with the string). Witnessed here, not just argued in prose.
        // This is the algebraic core the epoch<->string round-trip rests on, and
        // what a Kani proof would establish; Kani can't run here yet (its
        // toolchain predates our 1.95 MSRV), so exhaustion stands in. Runs in
        // well under a couple seconds (the monotonicity check formats each date).
        let mut prev = i64::MIN;
        let mut prev_str: Option<String> = None;
        for year in 1..=9999i64 {
            for month in 1..=12i64 {
                for day in 1..=days_in_month(year, month) {
                    let z = days_from_civil(year, month, day);
                    assert_eq!(
                        civil_from_days(z),
                        (year, month, day),
                        "round-trip failed at {year:04}-{month:02}-{day:02}"
                    );
                    if prev != i64::MIN {
                        assert_eq!(
                            z,
                            prev + 1,
                            "days must advance by exactly 1 per calendar day"
                        );
                    }
                    // A strictly-later day must render to a strictly-greater
                    // canonical string (compared bytewise, as SQLite's default
                    // collation does): this ties lexicographic to chronological
                    // order across the whole domain.
                    let s = format_epoch(z * 86_400);
                    if let Some(ps) = &prev_str {
                        assert!(
                            s.as_bytes() > ps.as_bytes(),
                            "canonical byte order must track day order: {ps:?} !< {s:?}"
                        );
                    }
                    prev = z;
                    prev_str = Some(s);
                }
            }
        }
        assert_eq!(days_from_civil(1, 1, 1), -719_162);
        assert_eq!(days_from_civil(9999, 12, 31), 2_932_896);
    }

    #[test]
    fn parse_error_never_echoes_input() {
        // Untrusted text must not reach an error message.
        let sneaky = "owner/repo involves:someone-else";
        let err = Rfc3339Utc::parse(sneaky).unwrap_err();
        assert!(
            !format!("{err}").contains("involves"),
            "error must not carry the offending input"
        );
        // Debug is an error surface too (tracing events, unwrap panics); it must
        // not leak the input either. Fieldless enum today, so this holds by
        // construction — pin it against a future hand-rolled Debug.
        assert!(
            !format!("{err:?}").contains("involves"),
            "Debug must not carry the offending input either"
        );
    }

    #[test]
    fn display_renders_and_eq_discriminates() {
        // Display must render the canonical string, not a write-nothing no-op:
        // as_str() is what the tests usually read, so `{}` needs its own witness.
        let t = Rfc3339Utc::parse("2026-07-24T13:59:00Z").unwrap();
        assert_eq!(format!("{t}"), "2026-07-24T13:59:00Z");
        assert_eq!(format!("{t}"), t.as_str());
        // Each ParseError variant Displays a specific, non-empty message.
        assert_eq!(
            format!("{}", ParseError::NotZulu),
            "not a Z-terminated UTC (RFC 3339) timestamp"
        );
        assert_eq!(
            format!("{}", ParseError::Malformed),
            "malformed RFC 3339 timestamp"
        );
        assert_eq!(
            format!("{}", ParseError::OutOfRange),
            "RFC 3339 timestamp field outside the accepted range"
        );
        // Equality is by instant: distinct instants must NOT compare equal. Every
        // other test asserts equality of EQUAL values, which an eq that always
        // returned true would satisfy; this is the discriminating case.
        let other = Rfc3339Utc::parse("2026-07-24T14:00:00Z").unwrap();
        assert_ne!(t, other);
    }

    #[test]
    fn from_epoch_rejects_every_instant_below_year_one() {
        // Every instant before 0001-01-01 is unrepresentable and must yield None
        // — the year guard decides this regardless of the civil-conversion
        // internals. The scan crosses the point where civil_from_days's day
        // count goes negative (~306 days before year 1), exercising its
        // negative-day branch: no arithmetic slip there may turn a below-range
        // instant into an accepted (Some) one.
        let year_one = Rfc3339Utc::parse("0001-01-01T00:00:00Z").unwrap().epoch();
        for d in 1..=1000i64 {
            let secs = year_one - d * 86_400;
            assert!(
                Rfc3339Utc::from_epoch(secs).is_none(),
                "instant {d} days before year 1 must be unrepresentable"
            );
        }
    }
}
