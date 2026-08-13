#![no_main]
//! attention.rs's DERIVATIONS — bucket, untriaged, effective_review_state
//! and waiting_on. attention_inputs covers the readers that meet archive
//! text; these are the judgments those readers feed, and they are where
//! DESIGN's directional rule actually bites:
//!
//!   uncertainty only escalates: it can add to `waiting_on_me`, it can
//!   never qualify a PR as `ready_to_merge`.
//!
//! That rule is asymmetric, so the witnesses are too. A spurious
//! `waiting_on_me` costs the operator a glance. A spurious
//! `ready_to_merge` tells them a PR is safe to land when the evidence did
//! not say so — and it is silent, because the archive looks complete. So
//! `ready_to_merge` must be EARNED from every proof at once, while the
//! escalating buckets need no justification.
//!
//! These are properties, not a mirror of the implementation: a
//! reimplementation of `bucket` here would drift into the same mistake the
//! original made and agree with it. Each witness instead states something
//! that must hold whatever the classification code looks like.
//!
//! Witnesses:
//!   1. Totality and determinism across all four derivations.
//!   2. EARNED: ready_to_merge implies every one of its proofs — the
//!      viewer's own PR, not draft, not truncated, an Approved effective
//!      state, no unresolved threads, and a review_decision that is either
//!      absent or APPROVED.
//!   3. ESCALATION IS ONE-WAY: re-deriving the same PR with truncation
//!      forced on (strictly less certainty) can never yield
//!      ready_to_merge — and neither can forcing unresolved threads on.
//!      This is the differential form of the rule, and it holds for inputs
//!      that were never ready to begin with, so it tests the boundary and
//!      not just the happy path.
//!   4. PRIORITY: an explicit review request of the viewer outranks every
//!      other signal — Bucket::ALL is documented as classification
//!      priority as well as wire order, and WaitingOnMe leads it.
//!   5. MAINTAINER SCOPE: a maintainer bucket can only appear for a
//!      project-scope repo, so the absent-vs-empty contract report.rs
//!      gates on cannot be violated from here.
//!   6. TRIAGE MUST BE PROVEN: untriaged is exactly "project scope and
//!      nothing proves triage"; any single proof clears it.
//!   7. FAIL-CLOSED REVIEW STATE: Approved requires an APPROVED review to
//!      exist; an unreadable stamp collapses toward StaleApproval, never
//!      up into Approved.
//!
//! ```text
//! cargo fuzz run attention_derive
//! ```

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use ghgraph::attention::{
    Bucket, EffectiveReviewState, IssueSignals, PrSignals, PushBounds, ReviewSignal, ThreadComment,
    bucket, effective_review_state, untriaged, waiting_on,
};

#[derive(Arbitrary, Debug)]
struct Input {
    viewers_pr: bool,
    person_pr: bool,
    draft: bool,
    truncated: bool,
    review_decision: Option<String>,
    requested_of_viewer: bool,
    thread_demands_viewer: bool,
    viewer_last_activity_at: Option<String>,
    last_other_activity_at: Option<String>,
    effective_sel: u8,
    has_unresolved_threads: bool,
    viewer_reviewed: bool,
    triage_scope: bool,
    has_review_requests: bool,
    has_reviews: bool,

    issue_triage_scope: bool,
    labeled: bool,
    assigned: bool,
    maintainer_replied: bool,

    reviews: Vec<(String, String, String)>,
    head_committed_at: Option<String>,
    head_flip_observed_at: Option<String>,

    viewer: String,
    pr_author: Option<String>,
    is_resolved: bool,
    comments: Vec<(Option<String>, bool, bool)>,
}

/// The four states, selected by byte so the fuzzer can reach each one
/// without having to guess a discriminant encoding.
fn effective_of(sel: u8) -> EffectiveReviewState {
    match sel % 4 {
        0 => EffectiveReviewState::Approved,
        1 => EffectiveReviewState::ChangesRequested,
        2 => EffectiveReviewState::StaleApproval,
        _ => EffectiveReviewState::Unreviewed,
    }
}

/// Rebuild the signals, optionally forcing a field that represents LESS
/// certainty. Used by witness (3): the same PR, strictly less proven.
fn signals<'a>(i: &'a Input, force_truncated: bool, force_unresolved: bool) -> PrSignals<'a> {
    PrSignals {
        viewers_pr: i.viewers_pr,
        person_pr: i.person_pr,
        draft: i.draft,
        truncated: i.truncated || force_truncated,
        review_decision: i.review_decision.as_deref(),
        requested_of_viewer: i.requested_of_viewer,
        thread_demands_viewer: i.thread_demands_viewer,
        viewer_last_activity_at: i.viewer_last_activity_at.as_deref(),
        last_other_activity_at: i.last_other_activity_at.as_deref(),
        effective: effective_of(i.effective_sel),
        has_unresolved_threads: i.has_unresolved_threads || force_unresolved,
        viewer_reviewed: i.viewer_reviewed,
        triage_scope: i.triage_scope,
        has_review_requests: i.has_review_requests,
        has_reviews: i.has_reviews,
    }
}

fuzz_target!(|i: Input| {
    let s = signals(&i, false, false);

    // (1) Totality and determinism.
    let got = bucket(&s);
    assert_eq!(got, bucket(&signals(&i, false, false)), "bucket not deterministic");

    // (2) ready_to_merge must be EARNED — every proof, not a majority.
    if got == Some(Bucket::ReadyToMerge) {
        assert!(s.viewers_pr, "ready_to_merge on a PR that is not the viewer's");
        assert!(!s.draft, "ready_to_merge on a draft");
        assert!(
            !s.truncated,
            "ready_to_merge on truncated data — the archive did not prove this"
        );
        assert_eq!(
            s.effective,
            EffectiveReviewState::Approved,
            "ready_to_merge without an Approved effective review state"
        );
        assert!(
            !s.has_unresolved_threads,
            "ready_to_merge with unresolved threads"
        );
        assert!(
            matches!(s.review_decision, None | Some("APPROVED")),
            "ready_to_merge with a review_decision that does not approve: {:?}",
            s.review_decision
        );
    }

    // (3) Escalation is one-way. Strictly less certainty, re-judged: the
    // answer may change, but it may never become ready_to_merge.
    assert_ne!(
        bucket(&signals(&i, true, false)),
        Some(Bucket::ReadyToMerge),
        "truncation produced ready_to_merge — uncertainty must never prove"
    );
    assert_ne!(
        bucket(&signals(&i, false, true)),
        Some(Bucket::ReadyToMerge),
        "an unresolved thread produced ready_to_merge"
    );

    // (4) An explicit request of the viewer outranks everything else —
    // on someone else's PR. On the viewer's OWN PR the request demands
    // the rest of the team (the author cannot review it), so the same
    // signal must NOT read as the viewer's demand: both polarity
    // directions witnessed, so neither regression direction is silent.
    if s.requested_of_viewer && !s.viewers_pr {
        assert_eq!(
            got,
            Some(Bucket::WaitingOnMe),
            "a review requested of the viewer on someone else's PR must \
             lead the priority order"
        );
    }
    if s.viewers_pr && s.requested_of_viewer && !s.thread_demands_viewer {
        assert_ne!(
            got,
            Some(Bucket::WaitingOnMe),
            "a request on the viewer's own PR demands the team, not the viewer"
        );
    }

    // (5) A maintainer bucket implies project scope — report.rs gates
    // serialization on exactly this.
    if let Some(b) = got {
        if b.maintainer() {
            assert!(
                s.triage_scope,
                "maintainer bucket {:?} outside triage scope",
                b.as_str()
            );
        }
        // Bucket::ALL is the wire order AND the priority order; every
        // returned bucket must be in it, or serialization has a hole.
        assert!(Bucket::ALL.contains(&b), "bucket outside Bucket::ALL");
    }

    // (6) Triage must be proven: any single proof clears untriaged, and
    // untriaged never fires outside project scope.
    let issue = IssueSignals {
        triage_scope: i.issue_triage_scope,
        labeled: i.labeled,
        assigned: i.assigned,
        maintainer_replied: i.maintainer_replied,
    };
    let u = untriaged(&issue);
    assert_eq!(u, untriaged(&issue), "untriaged not deterministic");
    if u {
        assert!(issue.triage_scope, "untriaged outside triage scope");
        assert!(!issue.labeled, "untriaged despite a label");
        assert!(!issue.assigned, "untriaged despite an assignee");
        assert!(
            !issue.maintainer_replied,
            "untriaged despite a maintainer reply"
        );
    }

    // (7) effective_review_state: fail-closed. Approved is only reachable
    // when an APPROVED review actually exists.
    let reviews: Vec<ReviewSignal<'_>> = i
        .reviews
        .iter()
        .map(|(reviewer, state, submitted_at)| ReviewSignal {
            reviewer,
            state,
            submitted_at,
        })
        .collect();
    let bounds = PushBounds {
        head_committed_at: i.head_committed_at.as_deref(),
        head_flip_observed_at: i.head_flip_observed_at.as_deref(),
    };
    let eff = effective_review_state(&reviews, &bounds);
    assert_eq!(
        eff,
        effective_review_state(&reviews, &bounds),
        "effective_review_state not deterministic"
    );
    if eff == EffectiveReviewState::Approved {
        assert!(
            reviews.iter().any(|r| r.state == "APPROVED"),
            "Approved without any APPROVED review"
        );
        assert!(
            !reviews.is_empty(),
            "Approved from an empty review set — silence is not approval"
        );
    }
    // as_str must be total over whatever it returns (the wire spelling).
    assert!(!eff.as_str().is_empty());

    // (8) waiting_on: totality, determinism, and the resolved short-circuit.
    let comments: Vec<ThreadComment<'_>> = i
        .comments
        .iter()
        .map(|(author, is_minimized, deleted)| ThreadComment {
            author: author.as_deref(),
            is_minimized: *is_minimized,
            deleted: *deleted,
        })
        .collect();
    let w = waiting_on(&i.viewer, i.pr_author.as_deref(), i.is_resolved, &comments);
    assert_eq!(
        w,
        waiting_on(&i.viewer, i.pr_author.as_deref(), i.is_resolved, &comments),
        "waiting_on not deterministic"
    );
    if i.is_resolved {
        assert!(w.is_none(), "a resolved thread demands nobody");
    }
    if let Some(w) = w {
        assert!(!w.as_str().is_empty());
    }
});
