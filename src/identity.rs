//! Validated identity types: the injection boundary and the one login
//! equivalence. This module makes two discipline rules structural:
//!
//!   * Injection is unrepresentable by type. Every identifier that reaches a
//!     GitHub search qualifier (queries::discovery_terms) arrives as a
//!     validated newtype whose admitted charset excludes a space and ':' —
//!     the two characters that could smuggle a second qualifier
//!     ("owner/name involves:someone-else"). Validation runs inside
//!     `Deserialize`, so even a caller that bypasses `config::parse` cannot
//!     obtain an unvalidated value: the charset gate that was discipline
//!     before milestone 1 (config.rs's fenced gap) is now the constructor.
//!   * Logins compare in one place. [`login_eq`] is the equivalence
//!     (ASCII-case-insensitive, as GitHub treats logins) and
//!     [`AuthorPattern::matches`] is the author-filter judgment built on it;
//!     [`is_bot`] is the one structural bot check (`__typename`, never a
//!     login pattern). Nothing else compares logins or sniffs bot-ness.
//!
//! Canonical form: both newtypes fold to ASCII lowercase at construction
//! (GitHub treats logins and repo names case-insensitively, and search
//! qualifiers accept either case), so the derived `Eq`/`Ord` on the canonical
//! string agree with [`login_eq`] by construction — the same idiom as
//! `time::Rfc3339Utc`, where a canonical form is what makes derived
//! comparisons sound. [`login_eq`] exists for the boundary where one side is
//! raw API text: API-returned logins are data, stored as received, never
//! folded.
//!
//! Errors are fieldless (the `time::ParseError` idiom): a constructor cannot
//! echo its input, so untrusted text can never reach an error message through
//! this module. The `Deserialize` impls DO name the offending value — the
//! config file is the operator's own text, and config errors name the entry
//! and field by contract (DESIGN.md, Config). That echo is licensed ONLY by
//! that precondition: ingest parse types (milestone 2) must carry API logins
//! as plain data fields — stored as received, compared via [`login_eq`] —
//! never deserialized through these impls, or third-party text lands in a
//! CONFIGURATION message.

use std::fmt;

use serde::Deserialize;
use serde::de::Error as _;

/// Why an identifier was rejected. Fieldless on purpose: no copy of the
/// offending input, so the rejection can never echo it (see module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityError {
    /// Not a valid GitHub login.
    Login,
    /// Not of the form `owner/name`.
    Repo,
    /// Not a valid `exclude_authors` pattern (`login` or `login[bot]`).
    AuthorPattern,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            IdentityError::Login => {
                "not a valid GitHub login (letters, digits, '-' or '_', \
                 not starting with '-'; 1\u{2013}39 chars)"
            }
            IdentityError::Repo => "not of the form owner/name",
            IdentityError::AuthorPattern => "not a valid login (optionally with a [bot] suffix)",
        };
        f.write_str(s)
    }
}

/// A validated GitHub login, folded to ASCII lowercase (canonical form; see
/// module docs). The only constructor is [`Login::new`], and `Deserialize`
/// goes through it, so an unvalidated login is unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Login(String);

impl Login {
    pub fn new(s: &str) -> Result<Login, IdentityError> {
        if is_login(s) {
            Ok(Login(s.to_ascii_lowercase()))
        } else {
            Err(IdentityError::Login)
        }
    }

    /// The canonical (lowercase) login. Charset `[a-z0-9-_]` — no whitespace,
    /// no ':' — so interpolating it into a search qualifier cannot smuggle a
    /// second qualifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Login {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Login {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Login::new(&s).map_err(|e| D::Error::custom(format!("login {s:?} is {e}")))
    }
}

/// A validated `owner/name` repository key, folded to ASCII lowercase — the
/// canonical form of the archive's `(repo, number)` business key, so
/// `Foo/Bar` and `foo/bar` can never split it or trip rename detection
/// against the canonical `nameWithOwner` (folded to match at ingest).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RepoName(String);

impl RepoName {
    pub fn new(s: &str) -> Result<RepoName, IdentityError> {
        if is_repo(s) {
            Ok(RepoName(s.to_ascii_lowercase()))
        } else {
            Err(IdentityError::Repo)
        }
    }

    /// The canonical (lowercase) `owner/name`. Charset `[a-z0-9-_./]` with
    /// exactly one `/` — no whitespace, no ':'.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RepoName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RepoName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        RepoName::new(&s).map_err(|e| D::Error::custom(format!("repo {s:?} is {e}")))
    }
}

/// THE login equivalence: ASCII-case-insensitive, as GitHub treats logins.
/// Called everywhere a login meets raw API text (which is data, stored as
/// received, never folded). Two [`Login`] values need no function — their
/// canonical form makes derived `Eq` agree with this by construction.
pub fn login_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// THE structural bot check: GraphQL `__typename == "Bot"`. Bot-ness is
/// author type, never a login pattern (DESIGN.md) — logins like
/// "dependabot" are not evidence, and GitHub attaches no "[bot]" suffix in
/// API data (that suffix is a UI and ghgraph-config affordance only).
pub fn is_bot(author_typename: &str) -> bool {
    author_typename == "Bot"
}

/// One `exclude_authors` entry: `login` or `login[bot]`.
///
/// Matching semantics (decided 2026-07-24, replacing an earlier draft where a
/// bare login matched only Users):
///
///   * a BARE login matches by [`login_eq`] regardless of author type —
///     `exclude_authors: ["dependabot"]` must filter the dependabot bot,
///     because that is the common maintainer intent and the earlier draft
///     made it a silent under-filter (uncertainty resolved the wrong way);
///   * the `[bot]` suffix NARROWS the match to author type `Bot` (structural,
///     via [`is_bot`]) — the affordance for shielding a same-named human.
///
/// What would reverse the bare-matches-either rule: a real operator needing
/// the other one-sided match (filter a User while keeping a same-named Bot).
/// The narrowing for that direction would be a new suffix, not a change to
/// bare semantics — bare-means-User cannot come back without reintroducing
/// the silent under-filter.
///
/// A deleted account (`author: null`) has no login and matches no pattern:
/// that is ordinary data, never a filter hit (queries.rs, hydration notes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorPattern {
    login: Login,
    bot_only: bool,
}

impl AuthorPattern {
    pub fn parse(s: &str) -> Result<AuthorPattern, IdentityError> {
        let (core, bot_only) = match s.strip_suffix("[bot]") {
            Some(core) => (core, true),
            None => (s, false),
        };
        match Login::new(core) {
            Ok(login) => Ok(AuthorPattern { login, bot_only }),
            Err(_) => Err(IdentityError::AuthorPattern),
        }
    }

    /// Does this pattern exclude an author with `api_login` and GraphQL
    /// `api_typename`? Both arrive as raw API text; the login side goes
    /// through [`login_eq`], the type side through [`is_bot`].
    pub fn matches(&self, api_login: &str, api_typename: &str) -> bool {
        login_eq(self.login.as_str(), api_login) && (!self.bot_only || is_bot(api_typename))
    }

    pub fn login(&self) -> &Login {
        &self.login
    }

    pub fn bot_only(&self) -> bool {
        self.bot_only
    }
}

impl<'de> Deserialize<'de> for AuthorPattern {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        AuthorPattern::parse(&s)
            .map_err(|e| D::Error::custom(format!("exclude_authors entry {s:?} is {e}")))
    }
}

fn is_login(s: &str) -> bool {
    // GitHub's LIVE login set, not the signup form's: 1–39 chars of
    // [A-Za-z0-9-_]. The signup rule ("alphanumeric or single interior
    // hyphens") is narrower than what exists — trailing-hyphen accounts
    // (`a-`, `b-`, org `Test-`) and consecutive-hyphen accounts
    // (`foo--bar`) are live grandfathered users, and Enterprise Managed
    // Users get `_` in their `IDP-USERNAME_SHORT-CODE` logins (probed and
    // cited 2026-07-29). A gate narrower than GitHub hard-rejects a valid
    // config; a gate slightly wider only trades a clean CONFIGURATION
    // message for an empty search result. Injection defense needs none of
    // this: space and ':' are excluded by the charset regardless.
    // A LEADING hyphen stays rejected: no live account was found by probe,
    // and '-' is the search syntax's negation prefix (`-involves:x`) —
    // revisit on the first real leading-hyphen login.
    !s.is_empty()
        && s.len() <= 39
        && s.bytes().next() != Some(b'-')
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn is_repo(s: &str) -> bool {
    match s.split_once('/') {
        Some((owner, name)) => {
            // owner is bounded to 39 by is_login; GitHub caps the name at 100.
            // Bounding it here keeps the gate's admitted set aligned with
            // GitHub's, same rationale as is_login's length cap.
            is_login(owner)
                && !name.is_empty()
                && name.len() <= 100
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthorPattern, IdentityError, Login, RepoName, is_bot, login_eq};

    // The injection counterexamples live on as unit tests (ROADMAP milestone
    // 1): the strings that motivated the gate must stay rejected forever.
    #[test]
    fn login_rejects_qualifier_injection() {
        for bad in [
            "me involves:target",
            "a:b",
            "has space",
            "",
            "-foo", // leading hyphen: search's negation prefix; no live account found
            &"x".repeat(40),
        ] {
            assert!(Login::new(bad).is_err(), "should reject {bad:?}");
        }
        assert!(Login::new("octocat").is_ok());
        assert!(Login::new("a-b-1").is_ok()); // interior hyphens are fine
        // Live GitHub accounts the signup-form rule would deny (probed
        // 2026-07-29): trailing hyphen (`b-`, org `Test-`), consecutive
        // hyphens (`foo--bar`), and EMU underscore logins. The gate admits
        // what exists, not what the form allows.
        assert!(Login::new("b-").is_ok());
        assert!(Login::new("foo--bar").is_ok());
        assert!(Login::new("mona-cat_octo").is_ok());
    }

    // is_repo must reject malformed forms ("/owner/name", "owner//name",
    // "owner/name/") and the ':'/space forms that could smuggle a second
    // search qualifier — the reason the validation exists. A charset
    // allowlist enforces both; a slash-count check would not.
    #[test]
    fn repo_rejects_malformed_and_injection() {
        for bad in [
            "/owner/name",
            "owner//name",
            "owner/name/",
            "owner",
            "owner/",
            "/name",
            "",
            "owner/na me",
            "owner/na:me",
            "owner/name involves:x",
            "a/b is:issue",
        ] {
            assert!(RepoName::new(bad).is_err(), "should reject {bad:?}");
        }
        for ok in ["owner/name", "o/n", "owner/name.rs", "o-w/n_1"] {
            assert!(RepoName::new(ok).is_ok(), "should accept {ok:?}");
        }
    }

    // Pin the length boundary (GitHub logins max 39): a 39-char login is
    // accepted, 40 is not. Without the lower assertion, relaxing `<= 39` to
    // `< 39` would go unnoticed.
    #[test]
    fn login_length_boundary() {
        assert!(Login::new(&"x".repeat(39)).is_ok(), "39 chars accepted");
        assert!(Login::new(&"x".repeat(40)).is_err(), "40 chars rejected");
    }

    // Pin the repo-name length boundary (GitHub caps names at 100): a 100-char
    // name is accepted, 101 is not. The owner half is already covered by
    // login_length_boundary via is_login.
    #[test]
    fn repo_name_length_boundary() {
        assert!(RepoName::new(&format!("o/{}", "x".repeat(100))).is_ok());
        assert!(RepoName::new(&format!("o/{}", "x".repeat(101))).is_err());
    }

    // Canonical form: construction folds to lowercase, so derived Eq agrees
    // with login_eq and the (repo, number) key can never split on case.
    #[test]
    fn construction_folds_to_canonical_lowercase() {
        assert_eq!(Login::new("OctoCat").unwrap().as_str(), "octocat");
        assert_eq!(
            Login::new("OctoCat").unwrap(),
            Login::new("octocat").unwrap()
        );
        assert_eq!(RepoName::new("Foo/Bar").unwrap().as_str(), "foo/bar");
        assert_eq!(
            RepoName::new("Foo/Bar").unwrap(),
            RepoName::new("foo/bar").unwrap()
        );
    }

    #[test]
    fn login_eq_is_ascii_case_insensitive() {
        assert!(login_eq("OctoCat", "octocat"));
        assert!(login_eq("dependabot", "dependabot"));
        assert!(!login_eq("octocat", "octodog"));
        // ASCII-only folding: a non-ASCII byte never folds, so lookalikes
        // in other scripts do not become equal.
        assert!(!login_eq("octo", "octö"));
    }

    // The rejection reason never echoes the input (fieldless enum) — the
    // property that lets identity errors carry untrusted text nowhere. The
    // type system enforces it; this test documents it as load-bearing.
    #[test]
    fn errors_are_fieldless_and_copy() {
        let e: IdentityError = Login::new("has space").unwrap_err();
        let copied: IdentityError = e; // Copy: no room for captured input
        assert!(!copied.to_string().contains("has space"));
    }

    // The Display strings are the operator-facing rule text the Deserialize
    // impls splice into CONFIGURATION errors; pin them so the remedy a user
    // reads never silently degrades.
    #[test]
    fn error_display_states_the_rule() {
        assert_eq!(
            IdentityError::Login.to_string(),
            "not a valid GitHub login (letters, digits, '-' or '_', \
             not starting with '-'; 1\u{2013}39 chars)"
        );
        assert_eq!(
            IdentityError::Repo.to_string(),
            "not of the form owner/name"
        );
        assert_eq!(
            IdentityError::AuthorPattern.to_string(),
            "not a valid login (optionally with a [bot] suffix)"
        );
    }

    // Exhaustive over every 1- and 2-byte ASCII string: acceptance equals an
    // independently written recognizer (chars-based, not the byte-based
    // constructor path). A proof-by-cases over the whole small domain — the
    // boundary shapes (single char, edge hyphens, the empty-adjacent cases)
    // all live here.
    #[test]
    fn login_acceptance_exhaustive_over_short_ascii() {
        fn reference(s: &str) -> bool {
            !s.is_empty()
                && s.len() <= 39
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
                && !s.starts_with('-')
        }
        for a in 0u8..=127 {
            let s1 = String::from_utf8(vec![a]).unwrap();
            assert_eq!(Login::new(&s1).is_ok(), reference(&s1), "1-byte {s1:?}");
            for b in 0u8..=127 {
                let s2 = String::from_utf8(vec![a, b]).unwrap();
                assert_eq!(Login::new(&s2).is_ok(), reference(&s2), "2-byte {s2:?}");
            }
        }
    }

    // The captured GraphQL bot-actor fixture (gh api graphql, 2026-07-29;
    // DISCOVERY's `author { login __typename }` shape). The load-bearing API
    // fact it witnesses: GitHub returns the BARE login for bots —
    // "dependabot" with __typename Bot, never "dependabot[bot]" — so a
    // literal bracket match would never fire.
    #[test]
    fn author_pattern_against_captured_bot_actor() {
        let v: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/bot_actor.json")).unwrap();
        let author = |stream: &str| &v["data"][stream]["nodes"][0]["author"];

        let bot = author("bot");
        assert_eq!(bot["__typename"], "Bot", "fixture must carry a Bot actor");
        assert_eq!(bot["login"], "dependabot", "and the API's bare login");
        let (bot_login, bot_tn) = (
            bot["login"].as_str().unwrap(),
            bot["__typename"].as_str().unwrap(),
        );

        // Bare pattern matches the Bot (the 2026-07-24 decision: excluding
        // "dependabot" must filter the dependabot bot).
        assert!(
            AuthorPattern::parse("dependabot")
                .unwrap()
                .matches(bot_login, bot_tn)
        );
        // [bot] matches the Bot too...
        let narrowed = AuthorPattern::parse("dependabot[bot]").unwrap();
        assert!(narrowed.matches(bot_login, bot_tn));
        // ...but NOT a same-named User: the suffix narrows by author type.
        assert!(!narrowed.matches("dependabot", "User"));

        // A bare pattern matches a real User node, case-insensitively.
        let human = author("human");
        assert_eq!(human["__typename"], "User");
        assert!(AuthorPattern::parse("WilliamMartin").unwrap().matches(
            human["login"].as_str().unwrap(),
            human["__typename"].as_str().unwrap(),
        ));

        // The structural check is the __typename, nothing else.
        assert!(is_bot(bot_tn));
        assert!(!is_bot("User"));
        assert!(!is_bot("Mannequin"));
        assert!(!is_bot("Organization"));
    }

    #[test]
    fn author_pattern_parse_shapes() {
        let p = AuthorPattern::parse("dependabot[bot]").unwrap();
        assert_eq!(p.login().as_str(), "dependabot");
        assert!(p.bot_only());
        let q = AuthorPattern::parse("Alice").unwrap();
        assert_eq!(q.login().as_str(), "alice"); // canonical fold
        assert!(!q.bot_only());
        // The suffix is not part of the login: a bracket in the core rejects.
        assert_eq!(
            AuthorPattern::parse("bad login[bot]").unwrap_err(),
            IdentityError::AuthorPattern
        );
        assert!(
            AuthorPattern::parse("a[bot]x").is_err(),
            "suffix must be terminal"
        );
        assert!(AuthorPattern::parse("[bot]").is_err(), "empty core rejects");
    }

    // Deserialize IS validation: serde paths cannot yield unvalidated values,
    // and the error names the offending value (the config file is the
    // operator's own text — naming it is the contract, not a leak).
    #[test]
    fn deserialize_validates_and_names_value() {
        let e = serde_json::from_str::<Login>(r#""me involves:target""#)
            .unwrap_err()
            .to_string();
        assert!(e.contains("me involves:target"), "names the value: {e}");
        assert!(serde_json::from_str::<RepoName>(r#""owner/name involves:x""#).is_err());
        assert!(serde_json::from_str::<AuthorPattern>(r#""bad login[bot]""#).is_err());
        assert_eq!(
            serde_json::from_str::<Login>(r#""OctoCat""#)
                .unwrap()
                .as_str(),
            "octocat"
        );
    }
}
