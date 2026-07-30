#![no_main]
//! Fuzz the identity constructors directly (`Login::new`, `RepoName::new`,
//! `AuthorPattern::parse`) — the injection gate itself, without the JSON
//! plumbing config_gate goes through.
//!
//! Properties, over arbitrary UTF-8 input:
//!   1. Totality: no constructor panics, accept or reject.
//!   2. Every ACCEPTED value is structurally valid by an INDEPENDENT
//!      recognizer (chars-based here vs the byte-based constructors) — the
//!      direction that catches over-acceptance, which a round-trip alone
//!      would hide. In particular: no space, no ':' (the qualifier-smuggling
//!      characters), and the documented length bounds.
//!   3. Canonicalization is sound: the canonical form is equivalent to the
//!      input under login_eq, and re-constructing from the canonical form is
//!      identity (idempotence) — so folding can never widen the admitted set.
//!   4. AuthorPattern::matches is consistent with its parts: it fires exactly
//!      when login_eq fires and, for [bot] patterns, only on the Bot type.

use libfuzzer_sys::fuzz_target;

use ghgraph::identity::{AuthorPattern, Login, RepoName, is_bot, login_eq};

fn valid_login_chars(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 39
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        && !s.starts_with('-')
}

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(l) = Login::new(s) {
        let c = l.as_str();
        assert!(valid_login_chars(c), "accepted login not re-recognized: {c:?}");
        assert!(login_eq(c, s), "canonical form must be login_eq to the input");
        assert_eq!(Login::new(c).ok().as_ref(), Some(&l), "canonicalization idempotent");
    }

    if let Ok(r) = RepoName::new(s) {
        let c = r.as_str();
        let (owner, name) = c.split_once('/').expect("accepted repo carries a '/'");
        assert!(valid_login_chars(owner), "owner half not re-recognized: {owner:?}");
        assert!(
            !name.is_empty()
                && name.len() <= 100
                && name.chars().all(|ch| {
                    ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_' | '.')
                }),
            "name half not re-recognized: {name:?}"
        );
        assert_eq!(RepoName::new(c).ok().as_ref(), Some(&r), "canonicalization idempotent");
    }

    if let Ok(p) = AuthorPattern::parse(s) {
        let core = p.login().as_str();
        assert!(valid_login_chars(core), "accepted pattern core not re-recognized: {core:?}");
        // matches ≡ login_eq ∧ (bot_only → is_bot), probed on both type sides.
        assert!(p.matches(core, "Bot"), "self-match on a Bot author");
        assert_eq!(p.matches(core, "User"), !p.bot_only(), "[bot] narrows to Bot");
        assert!(is_bot("Bot") && !is_bot("User"));
    }
});
