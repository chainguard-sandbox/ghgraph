//! Reference extraction from PR body text. Pure — no I/O, no allocation
//! beyond the result. The natural first Kani harness if verification is ever
//! wanted: no panic on arbitrary input, deterministic ordered output.
//!
//! Recognized grammar (case-insensitive keywords, `#N` or `owner/name#N`
//! targets):
//!
//!     fix|fixes|fixed|close|closes|closed|resolve|resolves|resolved  → fixes
//!     depends on                                                     → depends_on
//!     blocked by                                                     → blocked_by
//!     blocks                                                         → blocks
//!     bare #N / owner/name#N                                         → mentions
//!
//! Body-extracted `fixes` is source='body'; the API's closingIssuesReferences
//! lands as source='api' (sync ingests both — same fact, different trust).
//! `blocks` is stored exactly as observed, never flipped to blocked_by: the
//! blocked_edges view canonicalizes direction, the table preserves evidence.

use crate::error::Result;

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
pub fn extract(_body: &str, _src_repo: &str) -> Result<Vec<ExtractedRef>> {
    todo!("single-pass scanner; no regex dependency")
}
