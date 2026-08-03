//! Derived review state — the highest-value, most error-prone derivation in
//! the tool, encoded once here instead of re-derived (differently) by every
//! consumer prompt.
//!
//! effective_review_state per PR, from three raw signals:
//!   1. latestOpinionatedReviews — per-reviewer APPROVED / CHANGES_REQUESTED
//!      (stored as comments rows, kind='review', already latest-per-reviewer:
//!      the sync sweeps superseded rows — sync.rs)
//!   2. review_requests          — who is currently asked
//!   3. the push bounds          — Commit.pushedDate is deprecated upstream,
//!      so "did this review see the current head?" is answered by two stored
//!      bounds (DECIDED at queries.rs HYDRATE_PR, sync.rs OBSERVED):
//!        * prs.head_committed_at — server time; push ≥ commit, so a review
//!          older than it provably PREDATES the push (stale-side, no skew).
//!        * the observations head_sha flip row's observed_at — local time;
//!          the head was already current when we recorded the flip, so a
//!          review submitted after it (plus a skew margin, owned here)
//!          provably POSTDATES the push (fresh-side).
//!
//!      Between the bounds, ordering is UNKNOWN, and the polarity rule
//!      degrades unknown OUT of ready_to_merge — the bucket under-fills, it
//!      never lies. Two under-fills are structural and accepted, recorded
//!      here as the posture rather than papered over:
//!        * a PR approved before its first sync has no flip row, so its
//!          approval can never prove freshness (reads StaleApproval);
//!        * head_committed_at is author-controlled clock (an amended date
//!          travels with the oid), so a future-dated commit widens
//!          staleness.
//!
//!      Both err toward demanding a re-review — the cheap failure. The
//!      promoting evidence for either: a real operator's ready_to_merge
//!      starving on live archives with timestamps in hand (ROADMAP would
//!      then weigh storing first-seen-head time, a defined-writer schema
//!      step).
//!
//! reviewDecision (the raw API field) is stored but never trusted alone:
//! branch protection makes it read REVIEW_REQUIRED over a human approval, and
//! it mislabels PRs that were reviewed and then pushed to.
//!
//! `attention` output buckets, in fixed order — PLANNED (milestone 3, the
//! attention verb; this module currently carries the derivations it will
//! share with the `pr` verb):
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

use crate::identity::login_eq;
use crate::time::Rfc3339Utc;

/// The margin (seconds) a review must clear the LOCAL fresh-side bound by
/// before local-vs-server clock comparison counts as proof. Owned here (the
/// one place that compares across clocks). 300s covers NTP-sane skew with
/// two orders of magnitude to spare; the cost of oversizing is a few minutes
/// of StaleApproval right after a push, the cost of undersizing is a lie in
/// ready_to_merge — so it is sized for the second. Tuning evidence: a live
/// false-stale with both timestamps in hand.
pub const CLOCK_SKEW_MARGIN_SECS: u64 = 300;

/// The two stored push bounds (module docs). Both optional: a v1-migrated or
/// never-pushed-since-ingest row has neither, and every consumer must treat
/// that as unknown ordering, never as fresh.
#[derive(Debug, Clone, Copy, Default)]
pub struct PushBounds<'a> {
    /// prs.head_committed_at — server time, stale-side proof.
    pub head_committed_at: Option<&'a str>,
    /// observed_at of the LATEST observations row with field='head_sha' —
    /// local time, fresh-side proof. Latest, because freshness must be
    /// proven against the current head, not a superseded one.
    pub head_flip_observed_at: Option<&'a str>,
}

/// Did a review see the current head? Three-valued on purpose: the honest
/// answer between the bounds is "unknown", and collapsing it either way is
/// how a staleness derivation lies. Serialized by the `pr` verb as
/// stale: true / false / null.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewFreshness {
    /// Provably postdates the last push (fresh-side bound + skew margin).
    Fresh,
    /// Provably predates the last push (stale-side bound, server-only).
    Stale,
    /// Neither bound applies — degraded out of ready_to_merge by polarity.
    Unknown,
}

/// One review's freshness against the push bounds.
///
/// Stale wins over Fresh when both fire: both firing means the bounds
/// contradict each other (we observed the head flip before its commit
/// existed — clock skew beyond the margin, or an author-backdated
/// committedDate), and a contradiction is exactly the uncertainty the
/// polarity rule sends OUT of ready_to_merge.
///
/// Timestamps are RFC 3339 UTC "Z" at ingest, so the stale-side compare is
/// lexicographic; both are re-parsed here anyway because `query` proves the
/// archive is reachable by arbitrary SQL, so a derivation input is validated
/// where it is consumed, not assumed from the writer. Unparseable ⇒ Unknown
/// (fails closed).
pub fn review_freshness(submitted_at: &str, bounds: &PushBounds<'_>) -> ReviewFreshness {
    let Ok(submitted) = Rfc3339Utc::parse(submitted_at) else {
        return ReviewFreshness::Unknown;
    };
    if let Some(committed) = bounds.head_committed_at
        && let Ok(committed) = Rfc3339Utc::parse(committed)
        && submitted.epoch() < committed.epoch()
    {
        return ReviewFreshness::Stale;
    }
    if let Some(observed) = bounds.head_flip_observed_at
        && let Ok(observed) = Rfc3339Utc::parse(observed)
        && submitted
            .epoch()
            .checked_sub(observed.epoch())
            .is_some_and(|d| d >= 0 && d.unsigned_abs() >= CLOCK_SKEW_MARGIN_SECS)
    {
        return ReviewFreshness::Fresh;
    }
    ReviewFreshness::Unknown
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectiveReviewState {
    /// Approved by current reviews, none stale: at least one approval, every
    /// approval provably Fresh, and no CHANGES_REQUESTED standing (see
    /// [`effective_review_state`] for what "standing" means).
    Approved,
    /// At least one CHANGES_REQUESTED not provably stale.
    ChangesRequested,
    /// An approval exists but is not provably fresh — it predates the last
    /// push, or the ordering is unknown (the fail-closed collapse; module
    /// docs record why unknown lands here and not in Approved).
    StaleApproval,
    /// No standing opinionated review. NOT "never reviewed": a
    /// CHANGES_REQUESTED that provably predates the push lands here too —
    /// the push invalidated the review context, so the PR needs review
    /// again, which is what this state tells attention.
    Unreviewed,
}

impl EffectiveReviewState {
    /// The wire spelling (`pr` verb, and the attention buckets later).
    pub fn as_str(self) -> &'static str {
        match self {
            EffectiveReviewState::Approved => "approved",
            EffectiveReviewState::ChangesRequested => "changes_requested",
            EffectiveReviewState::StaleApproval => "stale_approval",
            EffectiveReviewState::Unreviewed => "unreviewed",
        }
    }
}

/// One latestOpinionatedReviews row (a comments row, kind='review').
pub struct ReviewSignal<'a> {
    pub reviewer: &'a str,
    /// Raw review state. Only APPROVED and CHANGES_REQUESTED are opinionated;
    /// anything else (COMMENTED, DISMISSED, PENDING) carries no verdict and
    /// is ignored by the state machine.
    pub state: &'a str,
    pub submitted_at: &'a str,
}

/// (latest opinionated reviews, push bounds) → effective state. Pure.
///
/// Precondition: `reviews` is latest-per-reviewer (the archive's kind='review'
/// rows are, because the sync sweeps superseded ones — sync.rs). Feeding all
/// historical reviews would let a reviewer's superseded CHANGES_REQUESTED
/// veto their own later approval.
///
/// The polarity rule, mechanically:
///   * a CHANGES_REQUESTED counts unless PROVABLY stale — uncertainty
///     escalates attention (Fresh and Unknown both stand);
///   * an approval counts toward Approved only when PROVABLY fresh —
///     uncertainty degrades out of ready_to_merge;
///   * both directions collapse Unknown toward "needs attention", never
///     toward "ready".
pub fn effective_review_state(
    reviews: &[ReviewSignal<'_>],
    bounds: &PushBounds<'_>,
) -> EffectiveReviewState {
    let mut approvals = 0u32;
    let mut fresh_approvals = 0u32;
    for r in reviews {
        match r.state {
            "CHANGES_REQUESTED" => {
                if review_freshness(r.submitted_at, bounds) != ReviewFreshness::Stale {
                    return EffectiveReviewState::ChangesRequested;
                }
            }
            "APPROVED" => {
                approvals += 1;
                if review_freshness(r.submitted_at, bounds) == ReviewFreshness::Fresh {
                    fresh_approvals += 1;
                }
            }
            _ => {}
        }
    }
    if approvals > 0 && fresh_approvals == approvals {
        EffectiveReviewState::Approved
    } else if approvals > 0 {
        EffectiveReviewState::StaleApproval
    } else {
        EffectiveReviewState::Unreviewed
    }
}

/// Who an unresolved review thread waits on, from the viewer's seat.
/// Serialized by the `pr` verb as waiting_on: "me" / "them" / null.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitingOn {
    Me,
    Them,
}

impl WaitingOn {
    pub fn as_str(self) -> &'static str {
        match self {
            WaitingOn::Me => "me",
            WaitingOn::Them => "them",
        }
    }
}

/// One thread comment, reduced to what the waiting_on derivation may see.
pub struct ThreadComment<'a> {
    /// None is a deleted account (author: null is ordinary data — parse.rs);
    /// it can never equal the viewer, so it counts as the other party.
    pub author: Option<&'a str>,
    /// GitHub's own hostile-content label. A minimized comment never DRIVES
    /// waiting_on (it is skipped when finding the last speaker) — untrusted
    /// text may annotate, never judge (DESIGN.md, security posture).
    pub is_minimized: bool,
    /// Soft-deleted upstream: still archived, no longer a demand.
    pub deleted: bool,
}

/// Pure: (viewer, PR author, thread state, comments in thread order) →
/// waiting_on. `None` means "this thread demands nothing from the viewer's
/// seat": resolved, no substantive comments, or a thread between third
/// parties on a PR the viewer neither authored nor participated in.
///
/// The derivation reads the LAST substantive comment (not minimized, not
/// deleted; a deleted-account author is substantive — see [`ThreadComment`]):
///   * last speaker is the viewer → Them (the viewer has answered; the ball
///     left their court, whoever's court it lands in);
///   * last speaker is someone else, and the viewer is party to the thread
///     (their PR, or they spoke in it) → Me.
///
/// Login comparison is [`login_eq`] everywhere — the one equivalence.
pub fn waiting_on(
    viewer: &str,
    pr_author: Option<&str>,
    is_resolved: bool,
    comments: &[ThreadComment<'_>],
) -> Option<WaitingOn> {
    if is_resolved {
        return None;
    }
    let last_speaker = comments
        .iter()
        .rev()
        .find(|c| !c.is_minimized && !c.deleted)
        .map(|c| c.author)?;
    let viewer_spoke = comments
        .iter()
        .filter(|c| !c.is_minimized && !c.deleted)
        .any(|c| c.author.is_some_and(|a| login_eq(a, viewer)));
    let viewers_pr = pr_author.is_some_and(|a| login_eq(a, viewer));
    if last_speaker.is_some_and(|a| login_eq(a, viewer)) {
        Some(WaitingOn::Them)
    } else if viewers_pr || viewer_spoke {
        Some(WaitingOn::Me)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: &str = "2026-01-01T00:00:00Z"; // before every bound
    const COMMIT: &str = "2026-02-01T00:00:00Z";
    const FLIP: &str = "2026-02-01T01:00:00Z";
    const AFTER: &str = "2026-02-01T02:00:00Z"; // clears FLIP + margin

    fn bounds<'a>(commit: Option<&'a str>, flip: Option<&'a str>) -> PushBounds<'a> {
        PushBounds {
            head_committed_at: commit,
            head_flip_observed_at: flip,
        }
    }

    // ---- review_freshness: proof-by-cases over the bound lattice --------

    #[test]
    fn freshness_no_bounds_is_unknown() {
        assert_eq!(
            review_freshness(AFTER, &bounds(None, None)),
            ReviewFreshness::Unknown
        );
    }

    #[test]
    fn freshness_before_commit_is_stale() {
        assert_eq!(
            review_freshness(T0, &bounds(Some(COMMIT), None)),
            ReviewFreshness::Stale
        );
    }

    #[test]
    fn freshness_at_commit_is_not_stale() {
        // push ≥ commit only bounds one side: equality proves nothing, and
        // an unproven review must read Unknown, not Stale — the strictness
        // of the < is what this pins.
        assert_eq!(
            review_freshness(COMMIT, &bounds(Some(COMMIT), None)),
            ReviewFreshness::Unknown
        );
    }

    #[test]
    fn freshness_after_flip_plus_margin_is_fresh() {
        assert_eq!(
            review_freshness(AFTER, &bounds(Some(COMMIT), Some(FLIP))),
            ReviewFreshness::Fresh
        );
    }

    #[test]
    fn freshness_margin_boundary_is_exact() {
        // Exactly at flip + margin ⇒ Fresh (≥); one second inside ⇒ Unknown.
        // The margin is the mechanism that makes local-vs-server comparison
        // a proof, so its boundary is pinned to the second.
        let at_margin = "2026-02-01T01:05:00Z";
        let inside = "2026-02-01T01:04:59Z";
        assert_eq!(
            review_freshness(at_margin, &bounds(None, Some(FLIP))),
            ReviewFreshness::Fresh
        );
        assert_eq!(
            review_freshness(inside, &bounds(None, Some(FLIP))),
            ReviewFreshness::Unknown
        );
    }

    #[test]
    fn freshness_contradicting_bounds_reads_stale() {
        // Flip observed before the commit existed: bounds contradict; the
        // conflict degrades out of ready_to_merge (Stale), never in.
        let flip_before_commit = "2026-01-31T00:00:00Z";
        let submitted = "2026-01-31T12:00:00Z"; // clears flip+margin, < commit
        assert_eq!(
            review_freshness(submitted, &bounds(Some(COMMIT), Some(flip_before_commit))),
            ReviewFreshness::Stale
        );
    }

    #[test]
    fn freshness_unparseable_inputs_are_unknown() {
        assert_eq!(
            review_freshness("not-a-time", &bounds(Some(COMMIT), Some(FLIP))),
            ReviewFreshness::Unknown
        );
        assert_eq!(
            review_freshness(AFTER, &bounds(Some("junk"), Some("junk"))),
            ReviewFreshness::Unknown
        );
    }

    // ---- effective_review_state: exhaustive over the 1-2 reviewer model --

    fn sig<'a>(state: &'a str, at: &'a str) -> ReviewSignal<'a> {
        ReviewSignal {
            reviewer: "r",
            state,
            submitted_at: at,
        }
    }

    /// ∀ proof by enumeration: every (state × freshness) pair for a single
    /// reviewer, against the doc table. The domain is small enough that the
    /// loop IS the spec.
    #[test]
    fn single_review_exhaustive() {
        // (state, submitted_at, bounds, expected)
        let full = bounds(Some(COMMIT), Some(FLIP));
        let none = bounds(None, None);
        let cases: &[(&str, &str, &PushBounds, EffectiveReviewState)] = &[
            // APPROVED × {Fresh, Stale, Unknown}
            ("APPROVED", AFTER, &full, EffectiveReviewState::Approved),
            ("APPROVED", T0, &full, EffectiveReviewState::StaleApproval),
            (
                "APPROVED",
                AFTER,
                &none,
                EffectiveReviewState::StaleApproval,
            ),
            // CHANGES_REQUESTED × {Fresh, Stale, Unknown}
            (
                "CHANGES_REQUESTED",
                AFTER,
                &full,
                EffectiveReviewState::ChangesRequested,
            ),
            (
                "CHANGES_REQUESTED",
                T0,
                &full,
                EffectiveReviewState::Unreviewed,
            ),
            (
                "CHANGES_REQUESTED",
                AFTER,
                &none,
                EffectiveReviewState::ChangesRequested,
            ),
            // Non-opinionated states carry no verdict regardless of bounds.
            ("COMMENTED", AFTER, &full, EffectiveReviewState::Unreviewed),
            ("DISMISSED", AFTER, &full, EffectiveReviewState::Unreviewed),
            ("PENDING", AFTER, &full, EffectiveReviewState::Unreviewed),
        ];
        for (state, at, b, expected) in cases {
            assert_eq!(
                effective_review_state(&[sig(state, at)], b),
                *expected,
                "state={state} at={at}"
            );
        }
    }

    /// The pairwise interactions the polarity rule decides: CR dominance and
    /// the all-approvals-fresh requirement.
    #[test]
    fn review_pairs_pin_polarity() {
        let full = bounds(Some(COMMIT), Some(FLIP));
        // Fresh approval + unknown-freshness CR: the CR stands (uncertainty
        // escalates), approval cannot outvote it.
        let cr_unknown_bounds = bounds(Some(COMMIT), None);
        assert_eq!(
            effective_review_state(
                &[sig("APPROVED", AFTER), sig("CHANGES_REQUESTED", AFTER)],
                &cr_unknown_bounds
            ),
            EffectiveReviewState::ChangesRequested
        );
        // Fresh approval + provably stale CR: the stale CR falls away.
        assert_eq!(
            effective_review_state(
                &[sig("APPROVED", AFTER), sig("CHANGES_REQUESTED", T0)],
                &full
            ),
            EffectiveReviewState::Approved
        );
        // Fresh approval + stale approval: ONE stale approval degrades the
        // whole PR ("none stale" is the Approved contract).
        let two = [
            ReviewSignal {
                reviewer: "a",
                state: "APPROVED",
                submitted_at: AFTER,
            },
            ReviewSignal {
                reviewer: "b",
                state: "APPROVED",
                submitted_at: T0,
            },
        ];
        assert_eq!(
            effective_review_state(&two, &full),
            EffectiveReviewState::StaleApproval
        );
        // No reviews at all.
        assert_eq!(
            effective_review_state(&[], &full),
            EffectiveReviewState::Unreviewed
        );
    }

    // ---- waiting_on ------------------------------------------------------

    fn c(author: Option<&'static str>, minimized: bool, deleted: bool) -> ThreadComment<'static> {
        ThreadComment {
            author,
            is_minimized: minimized,
            deleted,
        }
    }

    #[test]
    fn waiting_on_resolved_is_none() {
        assert_eq!(
            waiting_on("me", Some("me"), true, &[c(Some("them"), false, false)]),
            None
        );
    }

    #[test]
    fn waiting_on_my_pr_other_spoke_last_is_me() {
        assert_eq!(
            waiting_on("me", Some("me"), false, &[c(Some("them"), false, false)]),
            Some(WaitingOn::Me)
        );
    }

    #[test]
    fn waiting_on_i_spoke_last_is_them() {
        assert_eq!(
            waiting_on(
                "me",
                Some("me"),
                false,
                &[c(Some("them"), false, false), c(Some("ME"), false, false)]
            ),
            Some(WaitingOn::Them),
            "login comparison is login_eq: case-insensitive"
        );
    }

    #[test]
    fn waiting_on_minimized_never_drives() {
        // The only unminimized-and-undeleted history is the viewer's own; a
        // minimized latecomer cannot flip the thread back to Me (untrusted
        // text annotates, never judges).
        assert_eq!(
            waiting_on(
                "me",
                Some("me"),
                false,
                &[c(Some("me"), false, false), c(Some("them"), true, false)]
            ),
            Some(WaitingOn::Them)
        );
        // All comments minimized/deleted: no substantive speaker, no demand.
        assert_eq!(
            waiting_on(
                "me",
                Some("me"),
                false,
                &[c(Some("them"), true, false), c(Some("them"), false, true)]
            ),
            None
        );
    }

    #[test]
    fn waiting_on_third_party_thread_is_none() {
        // Someone else's PR, viewer never spoke: no seat in this thread.
        assert_eq!(
            waiting_on(
                "me",
                Some("author"),
                false,
                &[c(Some("them"), false, false)]
            ),
            None
        );
        // But once the viewer participated, the reply demand is theirs.
        assert_eq!(
            waiting_on(
                "me",
                Some("author"),
                false,
                &[c(Some("me"), false, false), c(Some("them"), false, false)]
            ),
            Some(WaitingOn::Me)
        );
    }

    #[test]
    fn waiting_on_ghost_author_counts_as_other_party() {
        // A deleted account can never equal the viewer; polarity says the
        // demand stands (uncertainty escalates, on the viewer's own PR).
        assert_eq!(
            waiting_on("me", Some("me"), false, &[c(None, false, false)]),
            Some(WaitingOn::Me)
        );
    }
}
