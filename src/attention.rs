//! Derived review state — the highest-value, most error-prone derivation in
//! the tool, encoded once here instead of re-derived (differently) by every
//! consumer prompt.
//!
//! effective_review_state per PR, from three raw signals:
//!   1. latestOpinionatedReviews — per-reviewer APPROVED / CHANGES_REQUESTED
//!   2. review_requests          — who is currently asked
//!   3. last head push time      — a review older than the last push is
//!      stale. Its source is an OPEN QUESTION (queries.rs, HYDRATE_PR:
//!      Commit.pushedDate is deprecated upstream); until resolved the signal
//!      is NULL, which this module's polarity degrades OUT of
//!      ready_to_merge — the bucket under-fills, it never lies.
//!
//! reviewDecision (the raw API field) is stored but never trusted alone:
//! branch protection makes it read REVIEW_REQUIRED over a human approval, and
//! it mislabels PRs that were reviewed and then pushed to.
//!
//! `attention` output buckets, in fixed order:
//!   waiting_on_me   — review requested of viewer; or an unresolved thread on
//!                     viewer's PR where the other party spoke last
//!   they_replied    — activity since viewer's last comment/review on PRs
//!                     viewer participates in
//!   ready_to_merge  — viewer's PRs, effectively approved, no unresolved
//!                     threads
//!   people_prs      — open PRs by config.people with no review from viewer
//!                     yet. The collaboration demand only: monitoring tracked
//!                     people is served by the archive (search, query,
//!                     observations), never by attention — attention is for
//!                     demands.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectiveReviewState {
    /// Approved by current reviews, none stale.
    Approved,
    /// At least one CHANGES_REQUESTED newer than the last push.
    ChangesRequested,
    /// An approval exists but predates the last push.
    StaleApproval,
    Unreviewed,
}

pub struct ReviewSignal<'a> {
    pub reviewer: &'a str,
    pub state: &'a str,
    pub submitted_at: &'a str,
}

/// Pure: (latest opinionated reviews, last push time) → effective state.
pub fn effective_review_state(
    _reviews: &[ReviewSignal<'_>],
    _last_pushed_at: Option<&str>,
) -> EffectiveReviewState {
    todo!("timestamp comparison is lexicographic — RFC 3339 Z strings")
}
