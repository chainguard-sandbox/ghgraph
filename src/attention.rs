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
//! `attention` output buckets, in fixed order ([`Bucket::ALL`]). The order
//! IS the priority: [`bucket`] places a PR in the FIRST bucket it qualifies
//! for and no other — one demand list, not four overlapping ones, and the
//! ordering is why (a new comment on an approved PR must read as "look
//! first", not "merge"). report.rs serializes the buckets as an ARRAY
//! because its sorted-key objects cannot carry this order.
//!   waiting_on_me   — review requested of viewer: a kind='user' request
//!                     matching the viewer by login_eq, or a kind='team'
//!                     request matching a declared config.teams name
//!                     (membership is declared, not verified — config.rs
//!                     records why the empty default surfaces no team
//!                     requests rather than all of them; a request of an
//!                     UNRECOGNIZED kind naming the viewer escalates —
//!                     shape drift must not drop a request). A request
//!                     reaching the viewer on their OWN PR is excluded:
//!                     an author cannot review their own PR, so a team
//!                     request there is a demand on the rest of the team
//!                     — the exclusion rests on [`login_eq`](author,
//!                     viewer), structural certainty, not uncertainty,
//!                     so it suppresses no demand addressed to the
//!                     viewer. Or an unresolved
//!                     thread on viewer's OWN PR whose last substantive
//!                     speaker is the other party ([`waiting_on`] == Me);
//!                     the same state on someone else's PR is a reply, not
//!                     a request, and falls through to they_replied.
//!   they_replied    — substantive activity by another HUMAN party since
//!                     the viewer's last, on PRs the viewer participates
//!                     in. Authorship is participation (it counts as
//!                     activity at the PR's created_at); minimized and
//!                     deleted comments are neither activity nor
//!                     participation, and neither is a Bot-typed author's
//!                     comment — a stale-bot warning or a CI summary
//!                     demands nothing — UNLESS the config names the bot
//!                     in reply_bots: a review bot the operator answers
//!                     daily is conversation, and the explicit list is
//!                     the honest tool (config.rs owns the default's
//!                     argument; the ambient population stays machinery).
//!                     The judgment is the structural
//!                     __typename, never a login pattern; an UNTYPED
//!                     author (NULL — ingested before the column existed
//!                     — or '', unresolvable at backfill: schema.sql)
//!                     fails OPEN as a human until re-hydration types it,
//!                     so uncertainty escalates here like everywhere
//!                     else. A third known narrowing joins the two above:
//!                     the exclusion — and its reply_bots opt-in — is
//!                     PR-seat only. A bot speaking LAST in a review
//!                     thread still flips waiting_on, listed or not
//!                     (ThreadComment carries no type). Promote it on the
//!                     first real bot thread-reply that fabricates a
//!                     demand, like the others.
//!                     An APPROVED review verdict is NOT a reply: it
//!                     demands nothing ready_to_merge doesn't already say,
//!                     and counting it would shunt every freshly-approved
//!                     PR into this bucket ahead of that one (priority
//!                     order) — starving ready_to_merge structurally. A
//!                     CHANGES_REQUESTED or COMMENTED review IS a reply
//!                     (COMMENTED arrives only in archives whose review
//!                     rows carry it — today's ingest query is
//!                     opinionated-only), and so does a review row whose
//!                     verdict is missing: only a PROVEN approval is
//!                     excluded, absence of a verdict fails open like
//!                     every other derivation input here. Two known
//!                     narrowings, PR-level recency and push-is-not-
//!                     activity (a head flip is an observation, not a
//!                     comment, and its stamp is local time — cross-clock
//!                     comparison is review_freshness's hard-won domain):
//!                     promote either on the first real demand it misses.
//!   ready_to_merge  — viewer's PRs, effectively approved, no unresolved
//!                     threads. Fail-closed, mechanically: draft (cannot
//!                     merge), truncated (threads may be missing — the
//!                     "complete data" precondition), or a stored
//!                     reviewDecision other than APPROVED (branch
//!                     protection saying "not yet" is never overridden;
//!                     the decision is distrusted alone, but distrust only
//!                     ever degrades OUT of this bucket) each disqualify.
//!   people_prs      — open PRs by config.people with no review from viewer
//!                     yet. The collaboration demand only: monitoring tracked
//!                     people is served by the archive (search, query,
//!                     observations), never by attention — attention is for
//!                     demands. Not drafts (the demand starts when the PR
//!                     asks for review — the needs_reviewer rule, applied
//!                     early), and never the viewer's own row (a
//!                     self-tracked viewer is config noise, not a
//!                     collaboration demand).
//!
//! The maintainer buckets (DESIGN.md, project scope) follow, gated on
//! `triage_scope`: a READ-time fact of the LOADED config — project scope
//! with `triage` enabled (config.rs owns the default and the
//! working-scope refusal) — computed by report.rs and carried in as a
//! structural signal, never the archive's stored fingerprint, so archive
//! contents cannot create a bucket, and the judgment (with its oracle)
//! still lives here. `triage: false` narrows the DEMAND surface only:
//! the whole-stream archive keeps project breadth for search, query, and
//! people; what turns off is the duty. For a config with no
//! triage-licensed repo the buckets are absent from output entirely, not
//! empty: an empty array is the "checked, nothing" disclosure, and no
//! maintainer sweep was asked for or performed (report.rs serializes;
//! the gate is [`Bucket::maintainer`]).
//!   needs_reviewer  — open, not draft, nobody asked (no review_requests
//!                     row at all — an undeclared team is still somebody)
//!                     and nobody has reviewed (any kind='review' row, any
//!                     verdict: a COMMENTED review is a reviewer already
//!                     engaging). The maintainer demand is "find this PR a
//!                     reviewer", so the viewer's own PRs qualify — at
//!                     project scope the viewer is the one who can assign.
//!                     Ordered after people_prs: a tracked person's
//!                     unreviewed PR is the operator's collaboration demand
//!                     first (one bucket per PR; the maintainer sweep
//!                     catches everyone else's).
//!   untriaged       — the one issue-shaped bucket. Open issue, no labels,
//!                     no assignees, no maintainer reply. Judged by
//!                     [`untriaged`] over [`IssueSignals`]; [`bucket`]
//!                     never returns it (a PR is not an issue; the oracle
//!                     test pins the impossibility). "Maintainer reply" is
//!                     structural — a non-deleted, non-minimized issue
//!                     comment whose author_assoc is proven affiliation
//!                     ([`is_maintainer_assoc`]): GitHub's own association
//!                     enum, never a judgment derived from text. Labels and
//!                     assignees are stored JSON read by
//!                     [`json_array_nonempty`]; NULL (a linked-cache or
//!                     masked-labels row that never witnessed them), an
//!                     empty array, and text that will not read as an array
//!                     all count as ABSENT — absence of proof of triage
//!                     keeps the demand standing. A stale fill-only row can
//!                     therefore over-fill this bucket; that is the
//!                     fail-open price, and the reversing evidence is a
//!                     real operator drowning in rows whose upstream issues
//!                     are in fact triaged.
//!
//! Bucket scope, all four operator buckets: state OPEN and not
//! upstream-deleted. A deleted
//! PR's demands died with it; a reply on a merged/closed PR is excluded as
//! out of the working set — the narrowing to reverse on the first real
//! demand it hides (the evidence would be an operator missing a post-merge
//! question). Drafts stay IN waiting_on_me and they_replied: a question on
//! a draft is a live demand. Truncated rows stay IN the demand buckets too
//! (fail-open — truncation flags a row, it never suppresses a demand) and
//! OUT of ready_to_merge (fail-closed), which is the polarity rule made
//! mechanical.

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
/// freshness: "fresh" | "stale" | "unknown" (a string enum, never a
/// nullable boolean — report.rs records why).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewFreshness {
    /// Provably postdates the last push (fresh-side bound + skew margin).
    Fresh,
    /// Provably predates the last push (stale-side bound, server-only).
    Stale,
    /// Neither bound applies — degraded out of ready_to_merge by polarity.
    Unknown,
}

impl ReviewFreshness {
    /// The wire spelling (`pr` verb).
    pub fn as_str(self) -> &'static str {
        match self {
            ReviewFreshness::Fresh => "fresh",
            ReviewFreshness::Stale => "stale",
            ReviewFreshness::Unknown => "unknown",
        }
    }
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
    // Two Nones with different meanings meet at this `?`: find() returning
    // None (no substantive comment at all → the thread demands nothing,
    // return) vs a substantive comment whose AUTHOR is None (a ghost spoke
    // last → last_speaker = None proceeds, and a ghost can never login_eq
    // the viewer, so it counts as the other party below).
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

/// The attention buckets, in output-and-priority order (module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    WaitingOnMe,
    TheyReplied,
    ReadyToMerge,
    PeoplePrs,
    NeedsReviewer,
    Untriaged,
}

impl Bucket {
    /// The fixed order: serialization order AND classification priority —
    /// one constant so the two cannot drift. The first four predate
    /// schema_version 1 and their relative order is frozen; the maintainer
    /// pair was appended (additive-only), and any future bucket appends
    /// here too.
    pub const ALL: [Bucket; 6] = [
        Bucket::WaitingOnMe,
        Bucket::TheyReplied,
        Bucket::ReadyToMerge,
        Bucket::PeoplePrs,
        Bucket::NeedsReviewer,
        Bucket::Untriaged,
    ];

    /// The wire spelling (`attention` verb).
    pub fn as_str(self) -> &'static str {
        match self {
            Bucket::WaitingOnMe => "waiting_on_me",
            Bucket::TheyReplied => "they_replied",
            Bucket::ReadyToMerge => "ready_to_merge",
            Bucket::PeoplePrs => "people_prs",
            Bucket::NeedsReviewer => "needs_reviewer",
            Bucket::Untriaged => "untriaged",
        }
    }

    /// A maintainer bucket exists in output only when the loaded config has
    /// a project-scope repo (module docs carry the absent-vs-empty
    /// argument); report.rs gates serialization on this.
    pub fn maintainer(self) -> bool {
        matches!(self, Bucket::NeedsReviewer | Bucket::Untriaged)
    }
}

/// One open, not-upstream-deleted PR, reduced to the structural signals the
/// bucket judgment may see. The caller (report.rs) queries; this type is the
/// fence that keeps the judgment here — every field is a structural fact,
/// none is derived text.
pub struct PrSignals<'a> {
    /// PR author is the viewer ([`login_eq`]).
    pub viewers_pr: bool,
    /// PR author is in config.people ([`login_eq`]).
    pub person_pr: bool,
    pub draft: bool,
    /// prs.truncated — hydration known incomplete.
    pub truncated: bool,
    /// Raw stored reviewDecision. Consumed ONLY as a negative gate on
    /// ready_to_merge (module docs); never trusted toward readiness.
    pub review_decision: Option<&'a str>,
    /// Some review_requests row addresses the viewer: kind='user' matching
    /// the viewer login, or kind='team' matching a declared team name.
    /// Computed on any PR; [`bucket`] itself applies the own-PR
    /// restriction (an author cannot review their own PR, so a request
    /// reaching the viewer there demands someone else), mirroring
    /// [`Self::thread_demands_viewer`] — the spec lives here, not in the
    /// caller's SQL.
    pub requested_of_viewer: bool,
    /// Some unresolved thread has [`waiting_on`] == Me from the viewer's
    /// seat. Computed on any PR; [`bucket`] itself applies the own-PR
    /// restriction, so the spec lives here and not in the caller's SQL.
    pub thread_demands_viewer: bool,
    /// The viewer's last substantive act on this PR: max created_at over
    /// their unminimized, undeleted comments and reviews, and the PR's own
    /// created_at when they authored it. None ⟺ the viewer neither
    /// authored nor spoke — i.e. does not participate.
    pub viewer_last_activity_at: Option<&'a str>,
    /// The other parties' last substantive act: same definition over
    /// comments whose author is not the viewer (a ghost author counts as
    /// the other party, as in [`waiting_on`]), EXCLUDING APPROVED review
    /// verdicts — module docs carry why an approval is not a reply — and
    /// EXCLUDING Bot-typed authors not named in config.reply_bots
    /// (module docs; NULL type fails open as human).
    pub last_other_activity_at: Option<&'a str>,
    pub effective: EffectiveReviewState,
    /// Any review_threads row with is_resolved = 0 (structural — comment
    /// content never resolves a thread).
    pub has_unresolved_threads: bool,
    /// Any kind='review' row by the viewer, any verdict: a COMMENTED review
    /// is still "a review from viewer" for people_prs.
    pub viewer_reviewed: bool,
    /// The repo's maintainer sweep is licensed by the LOADED config:
    /// project scope with `triage` enabled (config.rs owns the default
    /// and the working-scope refusal) — a read-time fact computed by
    /// report.rs from config, never from the archive (archive contents
    /// never create a bucket — DESIGN.md).
    pub triage_scope: bool,
    /// Any review_requests row at all, anyone, user or team, declared or
    /// not: an undeclared team request still means somebody was asked.
    pub has_review_requests: bool,
    /// Any kind='review' row at all, any verdict, anyone (module docs:
    /// a COMMENTED review is a reviewer already engaging).
    pub has_reviews: bool,
}

/// (signals) → at most one bucket, the first qualifying in [`Bucket::ALL`]
/// order. Pure; report.rs only queries and serializes.
pub fn bucket(s: &PrSignals<'_>) -> Option<Bucket> {
    if (s.requested_of_viewer && !s.viewers_pr) || (s.viewers_pr && s.thread_demands_viewer) {
        return Some(Bucket::WaitingOnMe);
    }
    if replied_since(s.viewer_last_activity_at, s.last_other_activity_at) {
        return Some(Bucket::TheyReplied);
    }
    if s.viewers_pr
        && !s.draft
        && !s.truncated
        && s.effective == EffectiveReviewState::Approved
        && !s.has_unresolved_threads
        && matches!(s.review_decision, None | Some("APPROVED"))
    {
        return Some(Bucket::ReadyToMerge);
    }
    if s.person_pr && !s.viewers_pr && !s.draft && !s.viewer_reviewed {
        return Some(Bucket::PeoplePrs);
    }
    if s.triage_scope && !s.draft && !s.has_review_requests && !s.has_reviews {
        return Some(Bucket::NeedsReviewer);
    }
    None
}

/// One open, not-upstream-deleted issue, reduced to the structural signals
/// [`untriaged`] may see — the issue-shaped mirror of [`PrSignals`], and the
/// same fence: the caller (report.rs) queries, the judgment lives here.
pub struct IssueSignals {
    /// The repo's maintainer sweep is licensed by the LOADED config (see
    /// [`PrSignals::triage_scope`] — same fact, same source).
    pub triage_scope: bool,
    /// issues.labels holds a non-empty array ([`json_array_nonempty`]).
    pub labeled: bool,
    /// issues.assignees holds a non-empty array ([`json_array_nonempty`]).
    pub assigned: bool,
    /// Some non-deleted, non-minimized comment on the issue carries a
    /// proven maintainer association ([`is_maintainer_assoc`]).
    pub maintainer_replied: bool,
}

/// (signals) → does the issue land in `untriaged`? The only issue bucket,
/// so this returns bool where [`bucket`] returns an enum. A demand: every
/// input that cannot PROVE triage leaves the issue in (module docs).
pub fn untriaged(s: &IssueSignals) -> bool {
    s.triage_scope && !s.labeled && !s.assigned && !s.maintainer_replied
}

/// Is a stored JSON value a non-empty array? The triage-mark reading for
/// issues.labels / issues.assignees: NULL (never witnessed — the
/// linked-cache and masked-labels shapes), an empty array, and text this
/// function cannot read as an array all read FALSE, because only a
/// witnessed non-empty array PROVES triage — the demand stands on anything
/// less. Re-parsed here like every derivation input: `query` proves the
/// archive is reachable by arbitrary SQL, so the input is validated where
/// it is consumed, not assumed from the writer.
pub fn json_array_nonempty(raw: Option<&str>) -> bool {
    raw.and_then(|r| serde_json::from_str::<serde_json::Value>(r).ok())
        .as_ref()
        .and_then(serde_json::Value::as_array)
        .is_some_and(|a| !a.is_empty())
}

/// A proven maintainer association — GitHub's own CommentAuthorAssociation,
/// a structural fact, never derived from comment text. Only the three
/// proven-affiliation values count; CONTRIBUTOR, FIRST_TIME_CONTRIBUTOR,
/// FIRST_TIMER, NONE, MANNEQUIN, a ghost author's NULL, and any value this
/// module does not recognize all read FALSE — a maintainer reply must be
/// proven to clear the untriaged demand, so unrecognized association drift
/// escalates (keeps the issue in) rather than silently triaging it.
pub fn is_maintainer_assoc(assoc: Option<&str>) -> bool {
    matches!(assoc, Some("OWNER" | "MEMBER" | "COLLABORATOR"))
}

/// Did another party act after the viewer's last act? False without both
/// timestamps (no participation, or no other activity — no demand either
/// way); with both, strictly-after on parsed times. Unparseable input on
/// either side reads TRUE: they_replied is a demand, demands fail open, and
/// a timestamp this module cannot order is exactly the uncertainty that
/// escalates (the inverse of [`review_freshness`], where the same doubt
/// degrades OUT of ready_to_merge). Re-parsed here like every derivation
/// input: `query` proves the archive is reachable by arbitrary SQL.
fn replied_since(viewer_last: Option<&str>, other_last: Option<&str>) -> bool {
    let (Some(viewer_last), Some(other_last)) = (viewer_last, other_last) else {
        return false;
    };
    match (
        Rfc3339Utc::parse(viewer_last),
        Rfc3339Utc::parse(other_last),
    ) {
        (Ok(v), Ok(o)) => o.epoch() > v.epoch(),
        _ => true,
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

    /// ∀ over ordered two-reviewer panels: 3 states × 3 freshness classes
    /// per reviewer = 81 pairs, checked against an independent count-based
    /// oracle restating the doc rules. The implementation short-circuits on
    /// the first standing CR (order-dependent structure); the oracle is
    /// order-independent — that structural difference is what makes this a
    /// check and not a restatement, and multi-reviewer ordering bugs are
    /// exactly what the four hand-picked pairs above cannot see.
    #[test]
    fn review_pairs_exhaustive_two_reviewers() {
        let full = bounds(Some(COMMIT), Some(FLIP));
        // MID: after COMMIT (not provably stale), before FLIP+margin (not
        // provably fresh) — the Unknown class.
        const MID: &str = "2026-02-01T00:30:00Z";
        let states = ["APPROVED", "CHANGES_REQUESTED", "COMMENTED"];
        let classes = [
            (T0, ReviewFreshness::Stale),
            (MID, ReviewFreshness::Unknown),
            (AFTER, ReviewFreshness::Fresh),
        ];
        fn oracle(reviews: &[(&str, ReviewFreshness)]) -> EffectiveReviewState {
            let standing_cr = reviews
                .iter()
                .any(|(s, f)| *s == "CHANGES_REQUESTED" && *f != ReviewFreshness::Stale);
            let approvals = reviews.iter().filter(|(s, _)| *s == "APPROVED").count();
            let fresh = reviews
                .iter()
                .filter(|(s, f)| *s == "APPROVED" && *f == ReviewFreshness::Fresh)
                .count();
            if standing_cr {
                EffectiveReviewState::ChangesRequested
            } else if approvals > 0 && fresh == approvals {
                EffectiveReviewState::Approved
            } else if approvals > 0 {
                EffectiveReviewState::StaleApproval
            } else {
                EffectiveReviewState::Unreviewed
            }
        }
        for s1 in states {
            for (t1, f1) in classes {
                for s2 in states {
                    for (t2, f2) in classes {
                        let reviews = [
                            ReviewSignal {
                                reviewer: "a",
                                state: s1,
                                submitted_at: t1,
                            },
                            ReviewSignal {
                                reviewer: "b",
                                state: s2,
                                submitted_at: t2,
                            },
                        ];
                        // The freshness classes themselves are pinned by
                        // the review_freshness cases; assert them anyway so
                        // a timestamp typo here fails loudly, not quietly.
                        assert_eq!(review_freshness(t1, &full), f1);
                        assert_eq!(review_freshness(t2, &full), f2);
                        assert_eq!(
                            effective_review_state(&reviews, &full),
                            oracle(&[(s1, f1), (s2, f2)]),
                            "({s1},{f1:?}) + ({s2},{f2:?})"
                        );
                    }
                }
            }
        }
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
        // Participation must be SUBSTANTIVE: a viewer comment that was
        // minimized (or deleted) does not seat them in the thread — the
        // filter applies to viewer_spoke exactly as it does to last-speaker
        // (the && ↔ || mutant this discriminates).
        assert_eq!(
            waiting_on(
                "me",
                Some("author"),
                false,
                &[c(Some("me"), true, false), c(Some("them"), false, false)]
            ),
            None
        );
        assert_eq!(
            waiting_on(
                "me",
                Some("author"),
                false,
                &[c(Some("me"), false, true), c(Some("them"), false, false)]
            ),
            None
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

    // ---- bucket: the PR-side classification -------------------------------

    const B_V1: &str = "2026-03-01T00:00:00Z";
    const B_BEFORE: &str = "2026-02-01T00:00:00Z";
    const B_AFTER: &str = "2026-03-02T00:00:00Z";

    #[test]
    fn replied_since_pins_polarity_and_strictness() {
        assert!(!replied_since(None, None));
        assert!(
            !replied_since(None, Some(B_AFTER)),
            "no participation, no demand"
        );
        assert!(
            !replied_since(Some(B_V1), None),
            "no other activity, no demand"
        );
        assert!(replied_since(Some(B_V1), Some(B_AFTER)));
        assert!(!replied_since(Some(B_V1), Some(B_BEFORE)));
        assert!(
            !replied_since(Some(B_V1), Some(B_V1)),
            "strictly after: simultaneity is not a reply"
        );
        // Unparseable escalates (fail-open): a demand this module cannot
        // order is surfaced, never dropped — the inverse of
        // review_freshness, where the same doubt degrades out.
        assert!(replied_since(Some("junk"), Some(B_AFTER)));
        assert!(replied_since(Some(B_V1), Some("junk")));
    }

    /// ∀ by enumeration over the full signal cube: 11 bools × 4 effective
    /// states × 4 decision values × 8 activity classes = 262,144 cases,
    /// against an oracle that restates the per-bucket doc rules
    /// order-INDEPENDENTLY (collect every qualifying bucket, take the first
    /// in Bucket::ALL order) where the implementation short-circuits — a
    /// priority-order bug is visible to exactly one of the two. The
    /// polarity implications are asserted separately, independent of both
    /// formulations.
    #[test]
    fn bucket_exhaustive_against_oracle() {
        use EffectiveReviewState as E;
        let effectives = [
            E::Approved,
            E::ChangesRequested,
            E::StaleApproval,
            E::Unreviewed,
        ];
        let decisions = [
            None,
            Some("APPROVED"),
            Some("REVIEW_REQUIRED"),
            Some("CHANGES_REQUESTED"),
        ];
        // (viewer_last, other_last, replied_since) — the classes pinned by
        // replied_since_pins_polarity_and_strictness above.
        let activity: &[(Option<&str>, Option<&str>, bool)] = &[
            (None, None, false),
            (None, Some(B_AFTER), false),
            (Some(B_V1), None, false),
            (Some(B_V1), Some(B_AFTER), true),
            (Some(B_V1), Some(B_BEFORE), false),
            (Some(B_V1), Some(B_V1), false),
            (Some("junk"), Some(B_AFTER), true),
            (Some(B_V1), Some("junk"), true),
        ];
        for bits in 0u32..2048 {
            let f = |i: u32| bits & (1 << i) != 0;
            for effective in effectives {
                for decision in decisions {
                    for (viewer_last, other_last, replied) in activity {
                        let s = PrSignals {
                            viewers_pr: f(0),
                            person_pr: f(1),
                            draft: f(2),
                            truncated: f(3),
                            review_decision: decision,
                            requested_of_viewer: f(4),
                            thread_demands_viewer: f(5),
                            viewer_last_activity_at: *viewer_last,
                            last_other_activity_at: *other_last,
                            effective,
                            has_unresolved_threads: f(6),
                            viewer_reviewed: f(7),
                            triage_scope: f(8),
                            has_review_requests: f(9),
                            has_reviews: f(10),
                        };
                        let got = bucket(&s);
                        let waiting = (s.requested_of_viewer && !s.viewers_pr)
                            || (s.viewers_pr && s.thread_demands_viewer);
                        let ready = s.viewers_pr
                            && !s.draft
                            && !s.truncated
                            && s.effective == E::Approved
                            && !s.has_unresolved_threads
                            && matches!(s.review_decision, None | Some("APPROVED"));
                        let people = s.person_pr && !s.viewers_pr && !s.draft && !s.viewer_reviewed;
                        let needs =
                            s.triage_scope && !s.draft && !s.has_review_requests && !s.has_reviews;
                        let want = [
                            (waiting, Bucket::WaitingOnMe),
                            (*replied, Bucket::TheyReplied),
                            (ready, Bucket::ReadyToMerge),
                            (people, Bucket::PeoplePrs),
                            (needs, Bucket::NeedsReviewer),
                        ]
                        .into_iter()
                        .find(|(q, _)| *q)
                        .map(|(_, b)| b);
                        assert_eq!(
                            got, want,
                            "bits={bits:011b} effective={effective:?} decision={decision:?} \
                             viewer_last={viewer_last:?} other_last={other_last:?}"
                        );
                        // Polarity, independent of either formulation:
                        if s.requested_of_viewer && !s.viewers_pr {
                            assert_eq!(
                                got,
                                Some(Bucket::WaitingOnMe),
                                "a review request on someone else's PR is never \
                                 suppressed by any other signal"
                            );
                        }
                        if s.viewers_pr && s.requested_of_viewer && !s.thread_demands_viewer {
                            assert_ne!(
                                got,
                                Some(Bucket::WaitingOnMe),
                                "a request reaching the viewer on their own PR is a \
                                 demand on someone else — the author cannot review it"
                            );
                        }
                        if got == Some(Bucket::ReadyToMerge) {
                            assert!(
                                s.viewers_pr
                                    && !s.draft
                                    && !s.truncated
                                    && s.effective == E::Approved
                                    && !s.has_unresolved_threads,
                                "ready_to_merge admitted incomplete or unapproved state"
                            );
                        }
                        // A PR is not an issue: the issue bucket is
                        // unreachable from PR signals, over the whole cube.
                        assert_ne!(got, Some(Bucket::Untriaged));
                        if got == Some(Bucket::NeedsReviewer) {
                            assert!(
                                s.triage_scope,
                                "a maintainer bucket leaked outside triage scope"
                            );
                        }
                    }
                }
            }
        }
    }

    /// The named discriminating cases behind the priority order — each pair
    /// is a real workflow moment, kept legible where the cube above is not.
    #[test]
    fn bucket_priority_named_cases() {
        let base = |viewers_pr: bool| PrSignals {
            viewers_pr,
            person_pr: false,
            draft: false,
            truncated: false,
            review_decision: None,
            requested_of_viewer: false,
            thread_demands_viewer: false,
            viewer_last_activity_at: None,
            last_other_activity_at: None,
            effective: EffectiveReviewState::Unreviewed,
            has_unresolved_threads: false,
            viewer_reviewed: false,
            triage_scope: false,
            has_review_requests: false,
            has_reviews: false,
        };
        // A request on a tracked person's PR is MY demand, not a people row.
        let s = PrSignals {
            person_pr: true,
            requested_of_viewer: true,
            ..base(false)
        };
        assert_eq!(bucket(&s), Some(Bucket::WaitingOnMe));
        // A fresh reply on my approved PR reads "look first", never "merge".
        let s = PrSignals {
            effective: EffectiveReviewState::Approved,
            review_decision: Some("APPROVED"),
            viewer_last_activity_at: Some(B_V1),
            last_other_activity_at: Some(B_AFTER),
            ..base(true)
        };
        assert_eq!(bucket(&s), Some(Bucket::TheyReplied));
        // A thread waiting on me on SOMEONE ELSE'S PR is a reply, not a
        // request: it reaches they_replied by recency, never waiting_on_me.
        let s = PrSignals {
            thread_demands_viewer: true,
            viewer_last_activity_at: Some(B_V1),
            last_other_activity_at: Some(B_AFTER),
            ..base(false)
        };
        assert_eq!(bucket(&s), Some(Bucket::TheyReplied));
        // Third-party PR, no involvement: no bucket at all.
        assert_eq!(bucket(&base(false)), None);
        // The same PR at project scope IS the maintainer's demand: nobody
        // asked, nobody reviewed — find it a reviewer.
        let s = PrSignals {
            triage_scope: true,
            ..base(false)
        };
        assert_eq!(bucket(&s), Some(Bucket::NeedsReviewer));
        // The viewer's own fresh PR qualifies too (the demand is
        // assignment, not review — module docs).
        let s = PrSignals {
            triage_scope: true,
            ..base(true)
        };
        assert_eq!(bucket(&s), Some(Bucket::NeedsReviewer));
        // A tracked person's unreviewed PR is the collaboration demand
        // first — people_prs outranks needs_reviewer (one bucket per PR).
        let s = PrSignals {
            triage_scope: true,
            person_pr: true,
            ..base(false)
        };
        assert_eq!(bucket(&s), Some(Bucket::PeoplePrs));
        // Somebody was asked, or somebody reviewed (any verdict — a
        // COMMENTED review is engagement): no needs_reviewer demand.
        let s = PrSignals {
            triage_scope: true,
            has_review_requests: true,
            ..base(false)
        };
        assert_eq!(bucket(&s), None);
        let s = PrSignals {
            triage_scope: true,
            has_reviews: true,
            ..base(false)
        };
        assert_eq!(bucket(&s), None);
        // Drafts don't need a reviewer yet — the demand starts when the PR
        // asks for review (the same rule people_prs applies).
        let s = PrSignals {
            triage_scope: true,
            draft: true,
            ..base(false)
        };
        assert_eq!(bucket(&s), None);
    }

    // ---- untriaged: the issue-shaped judgment ----------------------------

    /// ∀ by enumeration over the 4-bool signal space, against the doc rule
    /// stated as implications rather than restated as a conjunction: the
    /// bucket requires project scope, and each proof of triage
    /// independently clears it.
    #[test]
    fn untriaged_exhaustive() {
        for bits in 0u32..16 {
            let f = |i: u32| bits & (1 << i) != 0;
            let s = IssueSignals {
                triage_scope: f(0),
                labeled: f(1),
                assigned: f(2),
                maintainer_replied: f(3),
            };
            let got = untriaged(&s);
            assert_eq!(
                got,
                s.triage_scope && !s.labeled && !s.assigned && !s.maintainer_replied,
                "bits={bits:04b}"
            );
            if !s.triage_scope {
                assert!(!got, "a maintainer bucket leaked outside triage scope");
            }
            if s.labeled || s.assigned || s.maintainer_replied {
                assert!(!got, "a proven triage mark must clear the demand");
            }
        }
    }

    #[test]
    fn json_array_nonempty_reads_only_proven_triage() {
        // Proof of triage: a witnessed, non-empty array — element type is
        // irrelevant (foreign SQL can store anything; presence is presence).
        assert!(json_array_nonempty(Some(r#"["bug"]"#)));
        assert!(json_array_nonempty(Some("[0]")));
        // Everything short of proof keeps the demand: never witnessed
        // (NULL — the linked-cache and masked-labels shapes), witnessed
        // empty, JSON that is not an array, and text that is not JSON.
        assert!(!json_array_nonempty(None));
        assert!(!json_array_nonempty(Some("[]")));
        assert!(!json_array_nonempty(Some("null")));
        assert!(!json_array_nonempty(Some("{}")));
        assert!(!json_array_nonempty(Some(r#""bug""#)));
        assert!(!json_array_nonempty(Some("junk")));
    }

    /// ∀ over GitHub's CommentAuthorAssociation enum plus the null and
    /// drift cases — the domain is small enough that the loop is the spec.
    #[test]
    fn maintainer_assoc_exhaustive() {
        for (assoc, expected) in [
            (Some("OWNER"), true),
            (Some("MEMBER"), true),
            (Some("COLLABORATOR"), true),
            (Some("CONTRIBUTOR"), false),
            (Some("FIRST_TIME_CONTRIBUTOR"), false),
            (Some("FIRST_TIMER"), false),
            (Some("NONE"), false),
            (Some("MANNEQUIN"), false),
            // A ghost author proves nothing; unrecognized drift (or a
            // case-mangled value — the API enum is exact) must not triage.
            (None, false),
            (Some("owner"), false),
            (Some("ADMIN"), false),
        ] {
            assert_eq!(is_maintainer_assoc(assoc), expected, "assoc={assoc:?}");
        }
    }
}
