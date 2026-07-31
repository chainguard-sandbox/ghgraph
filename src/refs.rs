//! Reference extraction from PR body text. Pure — no I/O, no allocation
//! beyond the result. The natural first Kani harness if verification is ever
//! wanted: no panic on arbitrary input, deterministic ordered output.
//!
//! Recognized grammar (case-insensitive keywords, `#N` or `owner/name#N`
//! targets):
//!
//! ```text
//!     fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved  → fixes
//!     depends on                                                     → depends_on
//!     blocked by                                                     → blocked_by
//!     blocks                                                         → blocks
//!     bare #N / owner/name#N                                         → mentions
//! ```
//!
//! Scanner decisions, each with its reason:
//!   * A keyword binds only to an ADJACENT target — an optional `:` and
//!     whitespace may intervene, nothing else ("fixes the #1" is a mention,
//!     not a fixes). GitHub's own closing-keyword grammar has the same
//!     adjacency rule, and this is extraction of intent, not prose parsing.
//!   * Keywords and targets are recognized at word boundaries only:
//!     "suffixes #1" must not match `fixes`, "abc#1" is not a reference
//!     (GitHub does not autolink it either).
//!   * Number parsing is checked u64 with a trailing-boundary requirement
//!     (`#12abc` is text) and `#0` is refused (GitHub numbers start at 1);
//!     an overflowing digit run is text, never a saturated reference.
//!   * `owner/name` segments take the ASCII identifier charset
//!     `[A-Za-z0-9._-]` and fold to lowercase at emission, matching the
//!     archive's canonical `(repo, number)` key (identity.rs). Anything
//!     that fails the shape is left as text — extraction annotates, it
//!     never errors.
//!   * Markdown structure is deliberately ignored: a reference inside a
//!     code fence still extracts. Refs can annotate but never suppress
//!     attention (DESIGN.md), so an over-extraction is a spare row in a
//!     view, and fence tracking would buy that back with a state machine
//!     over third-party text.
//!   * URL forms (https://github.com/o/n/pull/N) are NOT extracted here —
//!     the grammar above is the contract. The CLI-argument reference
//!     parser [`parse_pr_ref`] handles the URL form, where the operator
//!     typed it on purpose.
//!
//! Body-extracted `fixes` is source='body'; the API's closingIssuesReferences
//! lands as source='api' (sync ingests both — same fact, different trust).
//! `blocks` is stored exactly as observed, never flipped to blocked_by: the
//! blocked_edges view canonicalizes direction, the table preserves evidence.

use crate::error::Result;
use crate::identity::RepoName;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RefKind {
    Fixes,
    DependsOn,
    BlockedBy,
    Blocks,
    Mentions,
}

impl RefKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RefKind::Fixes => "fixes",
            RefKind::DependsOn => "depends_on",
            RefKind::BlockedBy => "blocked_by",
            RefKind::Blocks => "blocks",
            RefKind::Mentions => "mentions",
        }
    }
}

/// `repo` is always the resolved owner/name — bare `#N` resolves against the
/// source PR's repo before storage; no same-repo sentinel exists.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExtractedRef {
    pub kind: RefKind,
    pub repo: String,
    pub number: u64,
}

/// Deterministic: sorted by (kind, repo, number), deduped. Never errors on
/// arbitrary text; the Result exists for signature stability only.
pub fn extract(body: &str, src_repo: &str) -> Result<Vec<ExtractedRef>> {
    let b = body.as_bytes();
    let src = src_repo.to_ascii_lowercase();
    let mut out = Vec::new();
    let mut i = 0;
    // Progress: every arm below advances `i` by at least 1 (target and word
    // lengths are nonzero by construction), so the debug_assert at the loop
    // foot witnesses termination the same way gh::scrub_tokens does.
    while i < b.len() {
        let before = i;
        if b[i] == b'#' && at_boundary(b, i) {
            // A bare target with no keyword: a mention.
            if let Some((repo, number, len)) = target_at(&b[i..], &src) {
                out.push(ExtractedRef {
                    kind: RefKind::Mentions,
                    repo,
                    number,
                });
                i += len;
            } else {
                i += 1;
            }
        } else if is_word(b[i]) && at_boundary(b, i) {
            let word_len = b[i..].iter().take_while(|&&c| is_word(c)).count();
            let word = &b[i..i + word_len];
            match keyword(word) {
                Some(Keyword::Single(kind)) => {
                    // Keyword, optional ':', whitespace, then an ADJACENT
                    // target — or the keyword was plain text.
                    match bind_target(b, i + word_len, &src) {
                        Some((repo, number, end)) => {
                            out.push(ExtractedRef { kind, repo, number });
                            i = end;
                        }
                        None => i += word_len,
                    }
                }
                Some(Keyword::Pair(second, kind)) => {
                    // "depends on" / "blocked by": the second word, then the
                    // target, both adjacency-bound.
                    match second_word(b, i + word_len, second)
                        .and_then(|after| bind_target(b, after, &src))
                    {
                        Some((repo, number, end)) => {
                            out.push(ExtractedRef { kind, repo, number });
                            i = end;
                        }
                        None => i += word_len,
                    }
                }
                None => {
                    // Not a keyword — but possibly the owner segment of an
                    // owner/name#N mention.
                    match target_at(&b[i..], &src) {
                        Some((repo, number, len)) => {
                            out.push(ExtractedRef {
                                kind: RefKind::Mentions,
                                repo,
                                number,
                            });
                            i += len;
                        }
                        None => i += word_len,
                    }
                }
            }
        } else {
            i += 1;
        }
        debug_assert!(i > before, "extract scanner stopped advancing");
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// A CLI PR reference: `owner/name#123` or a github.com PR URL. Returns the
/// validated (repo, number) pair or `None`; the caller owns the USER_INPUT
/// message (the input is the operator's own argv, so echoing it there is
/// licensed — the same rule as config errors, identity.rs).
///
/// The URL host is pinned to github.com — the recorded extension point for a
/// GHES operator (ROADMAP: deferred until one materializes). Accepted URL
/// shape: `https://github.com/owner/name/pull/N`, optional trailing `/`;
/// anything longer (files tab, diff anchors) is refused rather than guessed.
pub fn parse_pr_ref(s: &str) -> Option<(RepoName, u64)> {
    let (repo_part, number_part) = if let Some(rest) = s.strip_prefix("https://github.com/") {
        let mut segs = rest.trim_end_matches('/').split('/');
        let owner = segs.next()?;
        let name = segs.next()?;
        if segs.next()? != "pull" {
            return None;
        }
        let num = segs.next()?;
        if segs.next().is_some() {
            return None;
        }
        (format!("{owner}/{name}"), num.to_string())
    } else {
        let (r, n) = s.split_once('#')?;
        (r.to_string(), n.to_string())
    };
    let repo = RepoName::new(&repo_part).ok()?;
    let number: u64 = number_part.parse().ok()?;
    if number == 0 {
        return None;
    }
    Some((repo, number))
}

enum Keyword {
    Single(RefKind),
    /// First word matched; the required second word and the resulting kind.
    Pair(&'static [u8], RefKind),
}

fn keyword(word: &[u8]) -> Option<Keyword> {
    const FIXES: &[&[u8]] = &[
        b"fix",
        b"fixes",
        b"fixed",
        b"close",
        b"closes",
        b"closed",
        b"resolve",
        b"resolves",
        b"resolved",
    ];
    if FIXES.iter().any(|k| word.eq_ignore_ascii_case(k)) {
        return Some(Keyword::Single(RefKind::Fixes));
    }
    if word.eq_ignore_ascii_case(b"blocks") {
        return Some(Keyword::Single(RefKind::Blocks));
    }
    if word.eq_ignore_ascii_case(b"depends") {
        return Some(Keyword::Pair(b"on", RefKind::DependsOn));
    }
    if word.eq_ignore_ascii_case(b"blocked") {
        return Some(Keyword::Pair(b"by", RefKind::BlockedBy));
    }
    None
}

/// After a pair keyword's first word: whitespace, then exactly `second` at a
/// word boundary. Returns the offset just past it.
fn second_word(b: &[u8], mut i: usize, second: &[u8]) -> Option<usize> {
    let ws = b[i..]
        .iter()
        .take_while(|&&c| c.is_ascii_whitespace())
        .count();
    if ws == 0 {
        return None;
    }
    i += ws;
    let len = b[i..].iter().take_while(|&&c| is_word(c)).count();
    if b[i..i + len].eq_ignore_ascii_case(second) {
        Some(i + len)
    } else {
        None
    }
}

/// After a keyword: optional `:`, whitespace, then a target starting there.
/// Returns (repo, number, offset-just-past-target).
fn bind_target(b: &[u8], mut i: usize, src: &str) -> Option<(String, u64, usize)> {
    if i < b.len() && b[i] == b':' {
        i += 1;
    }
    let ws = b[i..]
        .iter()
        .take_while(|&&c| c.is_ascii_whitespace())
        .count();
    // Whitespace is required unless the colon already separated ("fixes#1"
    // is not GitHub's grammar; "fixes:#1" is unusual but unambiguous).
    if ws == 0 && (i == 0 || b[i - 1] != b':') {
        return None;
    }
    i += ws;
    let (repo, number, len) = target_at(&b[i..], src)?;
    Some((repo, number, i + len))
}

/// A target at `rest[0]`: `#N` or `owner/name#N`. Returns the canonical
/// (lowercase) repo, the number, and the consumed length.
fn target_at(rest: &[u8], src: &str) -> Option<(String, u64, usize)> {
    if rest.first() == Some(&b'#') {
        let (number, digits) = number_at(&rest[1..])?;
        return Some((src.to_string(), number, 1 + digits));
    }
    // owner/name#N
    let owner = rest.iter().take_while(|&&c| is_repo_seg(c)).count();
    if owner == 0 || rest.get(owner) != Some(&b'/') {
        return None;
    }
    let name_start = owner + 1;
    let name = rest[name_start..]
        .iter()
        .take_while(|&&c| is_repo_seg(c))
        .count();
    if name == 0 || rest.get(name_start + name) != Some(&b'#') {
        return None;
    }
    let num_start = name_start + name + 1;
    let (number, digits) = number_at(&rest[num_start..])?;
    let repo = std::str::from_utf8(&rest[..name_start + name])
        .ok()?
        .to_ascii_lowercase();
    Some((repo, number, num_start + digits))
}

/// A checked digit run with a trailing word boundary. `#0` and overflow are
/// text, not references.
fn number_at(rest: &[u8]) -> Option<(u64, usize)> {
    let digits = rest.iter().take_while(|&&c| c.is_ascii_digit()).count();
    if digits == 0 || rest.get(digits).is_some_and(|&c| is_word(c)) {
        return None;
    }
    let number: u64 = std::str::from_utf8(&rest[..digits]).ok()?.parse().ok()?;
    if number == 0 {
        return None;
    }
    Some((number, digits))
}

/// True when position `i` starts a token: at the string start or after a
/// non-word byte. Multibyte UTF-8 continuation bytes are non-word (>= 0x80),
/// so the check never lands inside a char in a way that matters — every
/// keyword and target byte tested is ASCII.
fn at_boundary(b: &[u8], i: usize) -> bool {
    i == 0 || !is_word(b[i - 1])
}

fn is_word(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn is_repo_seg(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(body: &str) -> Vec<(RefKind, String, u64)> {
        extract(body, "src/repo")
            .unwrap()
            .into_iter()
            .map(|r| (r.kind, r.repo, r.number))
            .collect()
    }

    #[test]
    fn every_fixes_keyword_maps() {
        for kw in [
            "fix", "fixes", "fixed", "close", "closes", "closed", "resolve", "resolves",
            "resolved", "FIXES", "Closes",
        ] {
            assert_eq!(
                refs(&format!("{kw} #7")),
                vec![(RefKind::Fixes, "src/repo".into(), 7)],
                "keyword {kw}"
            );
        }
    }

    #[test]
    fn colon_and_cross_repo_forms() {
        assert_eq!(
            refs("Fixes: #12"),
            vec![(RefKind::Fixes, "src/repo".into(), 12)]
        );
        assert_eq!(
            refs("fixes Other/Repo.js#9"),
            vec![(RefKind::Fixes, "other/repo.js".into(), 9)]
        );
        assert_eq!(
            refs("fixes:#3"),
            vec![(RefKind::Fixes, "src/repo".into(), 3)]
        );
    }

    #[test]
    fn pair_keywords() {
        assert_eq!(
            refs("depends on #4"),
            vec![(RefKind::DependsOn, "src/repo".into(), 4)]
        );
        assert_eq!(
            refs("Blocked   by o/n#5"),
            vec![(RefKind::BlockedBy, "o/n".into(), 5)]
        );
        assert_eq!(
            refs("blocked\nby #6"),
            vec![(RefKind::BlockedBy, "src/repo".into(), 6)]
        );
        assert_eq!(
            refs("blocks #8"),
            vec![(RefKind::Blocks, "src/repo".into(), 8)]
        );
        // The pair's first word alone binds nothing; the target is a mention.
        assert_eq!(
            refs("depends heavily on #4"),
            vec![(RefKind::Mentions, "src/repo".into(), 4)]
        );
    }

    #[test]
    fn adjacency_is_required() {
        // A word between keyword and target demotes the target to a mention.
        assert_eq!(
            refs("fixes the #1"),
            vec![(RefKind::Mentions, "src/repo".into(), 1)]
        );
        // No whitespace and no colon: not the grammar — and the glued '#'
        // has no left boundary either, so it is not even a mention (the
        // same rule that makes "abc#1" text).
        assert_eq!(refs("fixes#1"), vec![]);
    }

    #[test]
    fn word_boundaries_hold() {
        // "suffixes" must not match `fixes`; "abc#1" is not a reference.
        assert_eq!(
            refs("suffixes #1"),
            vec![(RefKind::Mentions, "src/repo".into(), 1)]
        );
        assert_eq!(refs("abc#1"), vec![]);
        // "#12abc" has no trailing boundary.
        assert_eq!(refs("#12abc"), vec![]);
    }

    #[test]
    fn bare_mentions_and_rejects() {
        assert_eq!(
            refs("see #22 and o/n#3"),
            vec![
                (RefKind::Mentions, "o/n".into(), 3),
                (RefKind::Mentions, "src/repo".into(), 22),
            ]
        );
        assert_eq!(refs("# 1"), vec![]);
        assert_eq!(refs("#0"), vec![], "GitHub numbers start at 1");
        assert_eq!(refs("#99999999999999999999999"), vec![], "overflow is text");
        assert_eq!(refs(""), vec![]);
        assert_eq!(refs("no refs here"), vec![]);
    }

    #[test]
    fn output_is_sorted_and_deduped() {
        let got = refs("fixes #2 fixes #1 fixes #2 #9 #9");
        assert_eq!(
            got,
            vec![
                (RefKind::Fixes, "src/repo".into(), 1),
                (RefKind::Fixes, "src/repo".into(), 2),
                (RefKind::Mentions, "src/repo".into(), 9),
            ]
        );
    }

    #[test]
    fn consumed_target_is_not_also_a_mention() {
        assert_eq!(
            refs("fixes #1"),
            vec![(RefKind::Fixes, "src/repo".into(), 1)]
        );
    }

    #[test]
    fn multibyte_text_is_safe_and_inert() {
        assert_eq!(
            refs("naïve — fixes #1 ✓"),
            vec![(RefKind::Fixes, "src/repo".into(), 1)]
        );
        assert_eq!(
            refs("é#1"),
            vec![(RefKind::Mentions, "src/repo".into(), 1)],
            "a continuation byte is a boundary; the ref is standalone-ish"
        );
    }

    #[test]
    fn punctuation_wrapping() {
        assert_eq!(
            refs("(see #5, and [#6])"),
            vec![
                (RefKind::Mentions, "src/repo".into(), 5),
                (RefKind::Mentions, "src/repo".into(), 6),
            ]
        );
    }

    // --- parse_pr_ref: the CLI form ---

    #[test]
    fn pr_ref_shorthand_and_url() {
        let (repo, n) = parse_pr_ref("Owner/Name#12").unwrap();
        assert_eq!((repo.as_str(), n), ("owner/name", 12));
        let (repo, n) = parse_pr_ref("https://github.com/cli/cli/pull/13864").unwrap();
        assert_eq!((repo.as_str(), n), ("cli/cli", 13864));
        let (repo, n) = parse_pr_ref("https://github.com/cli/cli/pull/1/").unwrap();
        assert_eq!((repo.as_str(), n), ("cli/cli", 1));
    }

    #[test]
    fn pr_ref_rejects_wrong_hosts_and_shapes() {
        for bad in [
            "http://github.com/o/n/pull/1",        // scheme not pinned form
            "https://evil.example/o/n/pull/1",     // host pinned to github.com
            "https://github.com/o/n/issues/1",     // not a PR path
            "https://github.com/o/n/pull/1/files", // trailing segment
            "o/n",                                 // no number
            "o/n#0",                               // zero
            "o/n#x",                               // not digits
            "o n#1",                               // invalid repo
            "#12", // bare number is the pr verb's cwd form, not --pr's
        ] {
            assert!(parse_pr_ref(bad).is_none(), "{bad} must be refused");
        }
    }
}
