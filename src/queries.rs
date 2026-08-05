//! GraphQL documents. Design rules these encode:
//!
//!   * Discovery and hydration are split. Discovery finds ids cheaply;
//!     hydration fetches one item's full context by node id. Never page a
//!     whole repo's pullRequests connection — `updated:`-windowed search is
//!     what bounds a high-traffic monorepo's cold start by the lookback.
//!   * Discovery is `search()`, scope-dependent. Working: three qualifier
//!     flavors for the viewer, deduped by id — `involves:` does NOT cover
//!     review-requested and does not reliably cover reviewed-by — plus one
//!     `involves:` per tracked person (config.people); for tracked people
//!     the involves: gaps are accepted, since their pending review queue is
//!     not the operator's demand. Project: one unqualified PR search and one
//!     issue search; broader results, fewer queries.
//!   * Discovery selects each hit's author, so per-repo filters (bots,
//!     exclude_authors) skip excluded items before hydration — a filtered
//!     PR costs discovery only, never a hydration subprocess.
//!   * Every connection selects totalCount + pageInfo. Any hasNextPage on a
//!     hydration connection triggers a follow-up page query rooted at the
//!     node id; if follow-ups don't complete, the PR row is marked truncated.
//!     No silent caps.
//!   * Search results lag and re-sort live; the caller overlaps the watermark
//!     window (~10 min) and treats upserts as idempotent.
//!   * Every document has a 1:1 response parse type in parse.rs, and the two
//!     are pinned to each other mechanically (deny_unknown_fields both
//!     directions) and empirically (captured live fixtures, re-captured by
//!     tests/capture.rs running these consts verbatim). Change a document
//!     and its parse type together.

use crate::config::{RepoConfig, Scope};
use crate::identity::Login;
use crate::sync::Stream;
use crate::time::Rfc3339Utc;

/// Discovery: ids, updatedAt, and author (for filter skips) only.
/// Variables: $q, $after. One document for both streams — the search
/// string's is:pr / is:issue decides which fragment matches. `typename` on
/// author distinguishes Bot accounts structurally, not by login pattern.
pub const DISCOVERY: &str = r#"
query($q: String!, $after: String) {
  search(type: ISSUE, first: 50, query: $q, after: $after) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest { id updatedAt author { login __typename } }
      ... on Issue { id updatedAt author { login __typename } }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// The discovery search strings for one configured repo and ONE stream —
/// the caller walks per (repo, stream) and each stream has its own
/// watermark, so a stream-typed term set is what keeps an Issue node id
/// out of PR hydration (the B2 panel's S1: an untyped term list fed
/// project-scope issue hits into HYDRATE_PR, where every one became an
/// eternal parse-class quarantine row). Stream::Issue emits terms only at
/// project scope with the issue stream on; sync.rs walks each configured
/// stream against its own watermark.
/// `since` is the caller's watermark with the overlap window already applied;
/// `until`, when present, closes the window (`updated:since..until`) — the
/// cap-splitting walk (sync.rs) halves a window that GitHub's ~1,000-result
/// search cap truncated, and the halves need a bounded range. `None` is the
/// open form (`updated:>=since`), the ordinary single-window case.
/// This signature is the injection boundary: every interpolated value is a
/// validating newtype — RepoName and Login (identity.rs) admit no space or
/// ':', Rfc3339Utc's canonical form is charset-bounded to [0-9:TZ-] with ':'
/// only in the fixed HH:MM:SS positions — so a value that could smuggle a
/// second qualifier ("owner/name involves:someone-else") is unrepresentable
/// here, not filtered here (the ".." separator is this function's own
/// literal, not data). The counterexample strings live on as unit tests
/// (identity.rs, config.rs). Do not add interpolation sites that take raw
/// strings.
pub fn discovery_terms(
    rc: &RepoConfig,
    viewer: &Login,
    people: &[Login],
    since: &Rfc3339Utc,
    until: Option<&Rfc3339Utc>,
    stream: Stream,
) -> Vec<String> {
    let updated = match until {
        Some(until) => format!("updated:{since}..{until}"),
        None => format!("updated:>={since}"),
    };
    let base = format!("repo:{} {updated} sort:updated-desc", rc.repo);
    match (stream, rc.scope) {
        (Stream::Issue, Scope::Project) if rc.issues() => {
            vec![format!("{base} is:issue")]
        }
        // Working scope has no issue stream (config.rs rejects the
        // combination); a project repo with issues off has none either.
        (Stream::Issue, _) => Vec::new(),
        (Stream::Pr, Scope::Project) => vec![format!("{base} is:pr")],
        (Stream::Pr, Scope::Working) => {
            let pr = format!("{base} is:pr");
            let mut terms = vec![
                format!("{pr} involves:{viewer}"),
                format!("{pr} review-requested:{viewer}"),
                format!("{pr} reviewed-by:{viewer}"),
            ];
            terms.extend(people.iter().map(|p| format!("{pr} involves:{p}")));
            terms
        }
    }
}

/// The targeted-backfill terms for people ADDED to a working-scope repo's
/// config (the fingerprint transition that does not cold-start the stream):
/// just the new `involves:` flavors, over the caller's window. Same
/// injection boundary and same interpolation discipline as
/// [`discovery_terms`] — newtypes only.
pub fn backfill_terms(
    rc: &RepoConfig,
    added: &[Login],
    since: &Rfc3339Utc,
    until: Option<&Rfc3339Utc>,
) -> Vec<String> {
    let updated = match until {
        Some(until) => format!("updated:{since}..{until}"),
        None => format!("updated:>={since}"),
    };
    added
        .iter()
        .map(|p| {
            format!(
                "repo:{} {updated} sort:updated-desc is:pr involves:{p}",
                rc.repo
            )
        })
        .collect()
}

/// Hydration: one PR's full working-set context by node id. Variables: $id.
///
/// Shape notes:
///   * closingIssuesReferences is GitHub's own parse of closing keywords
///     (cross-repo included) — ingested as refs kind='fixes', source='api'.
///     Body extraction (refs.rs) covers the rest. "Related to #N" is missed
///     by design and documented as such.
///   * latestOpinionatedReviews gives per-reviewer APPROVED/CHANGES_REQUESTED
///     without paging review history; with reviewRequests this feeds
///     attention::effective_review_state. reviewDecision is stored raw and
///     never trusted alone.
///   * commits(last:1) carries head oid + committedDate. It does NOT carry
///     push time: Commit.pushedDate — the field prs.last_pushed_at was
///     designed around — is deprecated upstream ("no longer supported") and
///     returns null on current PRs; selecting it buys nothing and breaks
///     every hydration whenever GitHub drops the field. DECIDED (milestone
///     2, recorded at sync.rs OBSERVED): the approval-staleness signal's
///     replacement is two stored bounds — prs.head_committed_at as the
///     stale-side proof (push ≥ commit, so approval < committedDate proves
///     staleness, server time, no skew; schema v2 added the column the v1
///     prose had claimed) and the observations table's own head_sha flip
///     row (observed_at ≥ push, local time, so approval ≥ observed_at
///     proves freshness modulo clock skew); the force-push timeline event
///     stays rejected with the timeline (DESIGN.md). prs.last_pushed_at
///     stays NULL, and attention's polarity contract degrades NULL/unknown
///     ordering OUT of ready_to_merge (attention.rs, which owns the skew
///     margin) — the bucket under-fills, it never lies.
///   * Every author selection in this hydration document carries __typename
///     (structural Bot detection at ingest, since sync --pr skips discovery)
///     plus databaseId via User/Bot fragments (NULL when neither fragment
///     matches — schema-possible for Mannequin; the Organization-author
///     case observed live nulls the whole actor instead). DISCOVERY fetches
///     only login + __typename: its results decide skip-or-hydrate and are
///     never stored, so databaseId there would be waste. databaseId is a
///     stable id captured now so identity matching could move off logins
///     (deferred; ROADMAP names the deciding evidence) — it is stored, not
///     yet consulted (matching is login-keyed, identity.rs). author is
///     Option everywhere in the parse types: author:null is ordinary, live
///     data (deleted users render as the `ghost` User, but legacy accounts
///     converted to Organizations null the actor — observed on
///     rails/rails#2), ingested NULL, never an error or a filter match. All
///     scalars on nodes already traversed, so zero extra rate-limit points —
///     only a few response bytes.
///   * authorAssociation on every author-bearing node — the PR, its reviews,
///     comments, and linked issues — is the reliable external-vs-insider axis
///     (OWNER/MEMBER/CONTRIBUTOR/FIRST_TIME_CONTRIBUTOR/…), the triage filter
///     GitHub actually backs, unlike "service account", which has no API
///     signal (use exclude_authors). Reviews carry it too, so the
///     comments.kind='review' rows never leave author_assoc silently NULL.
///   * repository { nameWithOwner } is the PR's own view of its repo:
///     a mismatch against config — compared case-folded, since repo identity
///     is case-insensitive (RepoName folds at construction, identity.rs;
///     fold nameWithOwner to match) —
///     detects a rename/transfer and surfaces as CONFIGURATION ("repo
///     renamed — update config"), never a silent follow or empty stream.
pub const HYDRATE_PR: &str = r#"
query($id: ID!) {
  node(id: $id) {
    ... on PullRequest {
      id number title body state isDraft url
      author { login __typename ... on User { databaseId } ... on Bot { databaseId } }
      authorAssociation
      repository { nameWithOwner }
      headRefName baseRefName
      reviewDecision
      createdAt updatedAt mergedAt closedAt
      commits(last: 1) { nodes { commit { oid committedDate } } }
      reviewRequests(first: 20) {
        totalCount
        nodes { requestedReviewer { ... on User { login } ... on Team { name } } }
      }
      latestOpinionatedReviews(first: 20) {
        totalCount
        nodes { id state submittedAt body url authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
      closingIssuesReferences(first: 10) {
        totalCount
        nodes { id number title state body updatedAt
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } }
                authorAssociation url repository { nameWithOwner } }
      }
      comments(first: 50) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
      reviewThreads(first: 50) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 30) {
            totalCount
            nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                    author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Follow-up page for any overflowed hydration connection, rooted at the
/// parent node id. Variables: $id, $after. One document per connection kind:
/// THREADS_PAGE here, COMMENTS_PAGE below.
///
/// totalCount is re-selected on every page, not just the first: the count
/// can move mid-walk (a thread lands during pagination), and a follow-up
/// page that re-reads it lets the walker see the drift instead of trusting
/// a stale first-page count — a scalar on a node already traversed, zero
/// extra points. It also keeps the page shape identical to the first page's
/// (parse::Paged), one parse type for both.
pub const THREADS_PAGE: &str = r#"
query($id: ID!, $after: String) {
  node(id: $id) {
    ... on PullRequest {
      reviewThreads(first: 100, after: $after) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 50) {
            totalCount
            nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                    author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Follow-up page for the top-level comments connection — same shape rules
/// as THREADS_PAGE (totalCount re-selected every page; parse::Paged both
/// pages). Until this document existed, a PR with more than 50 top-level
/// comments could only be marked truncated (the second-round B1 panel
/// caught the "analogous" comment standing in for the const).
pub const COMMENTS_PAGE: &str = r#"
query($id: ID!, $after: String) {
  node(id: $id) {
    ... on PullRequest {
      comments(first: 100, after: $after) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Hydration: one issue's full context by node id — the project-scope
/// stream's document. Variables: $id.
///
/// Deliberately lighter than HYDRATE_PR, and the cuts are decisions:
///
///   * No review machinery — issues have none — and no timeline: the same
///     event-system fence as everywhere.
///   * No closedAt: the issues table has no column for it, and a selection
///     without a consumer is waste (the telemetry rule's sibling). The
///     re-verify closed tier bounds by updated_at instead (sync.rs records
///     the containment argument).
///   * labels and assignees are small counted connections, first: 20 with
///     totalCount, like reviewRequests on the PR side: no follow-up
///     document — an overflow withholds the witness and the row lands
///     truncated, disclosed, like every other incompleteness. assignees
///     cannot overflow (GitHub caps assignees at 10); labels can on a
///     heavy-triage repo, and such a row is unhealable by design until a
///     page document exists — never-verified, so the re-verify tier
///     refetches it every run, one of the REVERIFY_CAP slots. That cost
///     is the accepted interim (disclosed via health.truncated each run);
///     a repo that routinely overflows 20 is the evidence that would add
///     the page document. labels is also schema-nullable (error-masking,
///     unlike assignees/comments — live-introspected): parse.rs carries
///     it Option, and a masked connection withholds the witness while
///     the writer carries the stored value forward (upsert_issue_stream).
///   * No refresh/tail layer: every issue hydration is a full walk. The
///     tail exists because PR hydration pays for review threads; an issue
///     is one comments connection, and a single-page issue costs exactly
///     one call already. The telemetry that would earn an issue tail is
///     the same tail_hits/full_walks pair, measured on real archives.
///   * No refs extraction from issue bodies: refs.src_pr is PR-keyed by
///     schema and the reference graph is a PR working-set feature; an
///     issue-sourced edge has no consumer. Revisit when a read verb wants
///     issue-to-issue links, not before.
///
/// Author selections carry the full identity discipline (__typename +
/// databaseId fragments, authorAssociation) — the shape notes at HYDRATE_PR
/// apply verbatim. repository { nameWithOwner } feeds the same
/// rename/transfer refusal.
pub const HYDRATE_ISSUE: &str = r#"
query($id: ID!) {
  node(id: $id) {
    ... on Issue {
      id number title body state url
      author { login __typename ... on User { databaseId } ... on Bot { databaseId } }
      authorAssociation
      repository { nameWithOwner }
      createdAt updatedAt
      labels(first: 20) {
        totalCount
        nodes { name }
      }
      assignees(first: 20) {
        totalCount
        nodes { login }
      }
      comments(first: 50) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Follow-up page for an overflowed issue comments connection — COMMENTS_PAGE
/// with the fragment retyped (`... on Issue`), same shape rules (totalCount
/// re-selected every page; parse::Paged both pages). A separate const, not a
/// parameterized template: the fragment type is the whole difference, and a
/// string-substituted type name would trade a grep-able document for a
/// render path the fixture pin cannot see.
pub const ISSUE_COMMENTS_PAGE: &str = r#"
query($id: ID!, $after: String) {
  node(id: $id) {
    ... on Issue {
      comments(first: 100, after: $after) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Tail size for the layered-refresh comments fetch: `comments(last: K)`.
/// Conservative constant, not config: it must only be large enough to cover
/// the new-comment burst between two syncs of one PR (the walk-back covers
/// modest overshoot; anything larger escalates to the full walk, which is
/// correct, just costlier). ROADMAP defers final sizing to the telemetry
/// this layer itself emits (refresh.tail_hits vs full_walks against real
/// totalCount distributions); its value is an input to that decision, not
/// a finding.
pub const TAIL_K: u32 = 20;

/// Render `__K__` in a refresh-layer document template. TAIL_K is a
/// compile-time u32, so its rendering is digits-only — the pr_id_document
/// argument. The marker form (not `format!`) keeps the GraphQL braces
/// literal; the no-residual-marker test pins that every marker was
/// replaced.
fn render_tail_k(template: &str) -> String {
    template.replace("__K__", &TAIL_K.to_string())
}

/// Layered refresh: the first document for a PR that already has a
/// witnessed baseline (sync.rs owns that dispatch gate). Identical to
/// HYDRATE_PR — same scalars, same small connections, the shared shape
/// notes above apply — except the two big connections:
///
///   * comments(last: K): the tail, selected WITH totalCount in this same
///     document. Count and tail from one response is a correctness
///     obligation, not a preference — a two-round-trip split is a TOCTOU
///     on a live connection (round-0 spec audit). Backward pagination
///     reads the mirror pageInfo pair (hasPreviousPage/startCursor);
///     walk-back pages go through TAIL_COMMENTS.
///   * reviewThreads: the skeleton — every cheap mutable field every time
///     (isResolved, isOutdated, isMinimized, lastEditedAt,
///     authorAssociation can all change without bumping PR.updatedAt),
///     bodies omitted. GitHub prices by node count, not field, so the
///     skeleton saves bytes while the tail saves points; sync.rs fills
///     bodies from the archive for unchanged ids and refetches whole
///     threads (THREAD_BODIES) for new or edited ones.
///
/// A single-page PR — tail covers the comments, threads fit one page —
/// costs exactly one call.
pub fn refresh_pr_document() -> String {
    render_tail_k(REFRESH_PR_TEMPLATE)
}

const REFRESH_PR_TEMPLATE: &str = r#"
query($id: ID!) {
  node(id: $id) {
    ... on PullRequest {
      id number title body state isDraft url
      author { login __typename ... on User { databaseId } ... on Bot { databaseId } }
      authorAssociation
      repository { nameWithOwner }
      headRefName baseRefName
      reviewDecision
      createdAt updatedAt mergedAt closedAt
      commits(last: 1) { nodes { commit { oid committedDate } } }
      reviewRequests(first: 20) {
        totalCount
        nodes { requestedReviewer { ... on User { login } ... on Team { name } } }
      }
      latestOpinionatedReviews(first: 20) {
        totalCount
        nodes { id state submittedAt body url authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
      closingIssuesReferences(first: 10) {
        totalCount
        nodes { id number title state body updatedAt
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } }
                authorAssociation url repository { nameWithOwner } }
      }
      comments(last: __K__) {
        totalCount
        pageInfo { hasPreviousPage startCursor }
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
      reviewThreads(first: 50) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 30) {
            totalCount
            nodes { id createdAt lastEditedAt url isMinimized authorAssociation
                    author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Walk-back page for the refresh tail: the next K comments BEFORE the
/// oldest already fetched, still selecting totalCount in the same response
/// (the one-document rule holds on every iteration — each page's count is
/// compared against the first's, and a moved count escalates to the full
/// walk). Nodes carry bodies: tail rows are upserted verbatim, they are
/// the rows the refresh writes.
pub fn tail_comments_document() -> String {
    render_tail_k(TAIL_COMMENTS_TEMPLATE)
}

const TAIL_COMMENTS_TEMPLATE: &str = r#"
query($id: ID!, $before: String) {
  node(id: $id) {
    ... on PullRequest {
      comments(last: __K__, before: $before) {
        totalCount
        pageInfo { hasPreviousPage startCursor }
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// Follow-up skeleton page for the refresh thread walk — THREADS_PAGE with
/// bodies omitted from the nested selection (same shape rules: totalCount
/// re-selected every page, one parse type for first and follow-up pages).
/// The skeleton walk's pagination terminating over ids is what earns the
/// threads witness; bodies are not part of completeness.
pub const SKELETON_THREADS_PAGE: &str = r#"
query($id: ID!, $after: String) {
  node(id: $id) {
    ... on PullRequest {
      reviewThreads(first: 100, after: $after) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 50) {
            totalCount
            nodes { id createdAt lastEditedAt url isMinimized authorAssociation
                    author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// One thread refetched WITH bodies, rooted at the thread's own node id:
/// the body fetch for a thread whose skeleton showed a new or edited
/// comment id. Whole-thread on purpose — a review burst lands its replies
/// in one thread, so grouping by thread bounds calls by changed threads,
/// not changed comments — and the response REPLACES the skeleton's view of
/// that thread (its comment set may have moved between the two calls; the
/// newer snapshot wins, and a count its selection cannot cover just
/// withholds the threads witness, exactly like an overflowing thread in
/// HYDRATE_PR). first: 50 matches THREADS_PAGE's nested budget, not
/// HYDRATE_PR's 30: a single-thread document has node room to spare.
pub const THREAD_BODIES: &str = r#"
query($id: ID!) {
  node(id: $id) {
    ... on PullRequestReviewThread {
      id
      comments(first: 50) {
        totalCount
        nodes { id body createdAt lastEditedAt url isMinimized authorAssociation
                author { login __typename ... on User { databaseId } ... on Bot { databaseId } } }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;

/// The node-id lookup for `sync --pr`: hydration is by node id, but the
/// operator names a (repo, number). Variables: $owner, $name — the number is
/// inlined into the document text instead of passed as a variable, because
/// `gh -f` sends string variables only and `pullRequest(number:)` takes an
/// Int; the inlined value is a validated `u64`, so its rendering is
/// digits-only — the same argument that lets `discovery_terms` interpolate
/// its newtypes. Do not extend this precedent to any non-numeric type.
/// `repository` and `pullRequest` are both schema-nullable: null means "not
/// found or not visible", which is data for the caller (USER_INPUT naming
/// the reference), never a parse error.
pub fn pr_id_document(number: u64) -> String {
    format!(
        r#"
query($owner: String!, $name: String!) {{
  repository(owner: $owner, name: $name) {{ pullRequest(number: {number}) {{ id }} }}
  rateLimit {{ cost remaining resetAt }}
}}"#
    )
}

#[cfg(test)]
mod tests {
    use super::discovery_terms;
    use crate::config::parse;
    use crate::identity::Login;
    use crate::sync::Stream;
    use crate::time::Rfc3339Utc;

    fn since() -> Rfc3339Utc {
        Rfc3339Utc::parse("2026-07-01T00:00:00Z").unwrap()
    }

    // Working scope: exactly the three viewer flavors (involves: does not
    // cover review-requested and does not reliably cover reviewed-by) plus
    // one involves: per tracked person, in config order — the flavor set the
    // sync fingerprint identifies.
    #[test]
    fn working_scope_emits_viewer_flavors_plus_people() {
        let cfg = parse(
            r#"{"viewer":"Viewer","repos":["O/N"],"people":["Alice"]}"#,
            "<test>",
        )
        .unwrap();
        let rc = cfg.repos[0].resolved();
        let terms = discovery_terms(&rc, &cfg.viewer, &cfg.people, &since(), None, Stream::Pr);
        let base = "repo:o/n updated:>=2026-07-01T00:00:00Z sort:updated-desc is:pr";
        assert_eq!(
            terms,
            vec![
                format!("{base} involves:viewer"),
                format!("{base} review-requested:viewer"),
                format!("{base} reviewed-by:viewer"),
                format!("{base} involves:alice"),
            ],
            "canonical (folded) identifiers, fixed flavor order"
        );
    }

    // Project scope: one unqualified PR term, plus the issue term iff the
    // issue stream is on (default at project scope; off explicitly here).
    #[test]
    fn project_scope_emits_pr_and_issue_terms() {
        let cfg = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project"}]}"#,
            "<test>",
        )
        .unwrap();
        let rc = cfg.repos[0].resolved();
        let people: Vec<Login> = Vec::new();
        let base = "repo:o/n updated:>=2026-07-01T00:00:00Z sort:updated-desc";
        // The PR stream never carries the issue term, whatever the issue
        // setting: stream-typed terms are what keep an Issue node id out of
        // PR hydration (the B2 panel's S1).
        let terms = discovery_terms(&rc, &cfg.viewer, &people, &since(), None, Stream::Pr);
        assert_eq!(terms, vec![format!("{base} is:pr")]);
        let terms = discovery_terms(&rc, &cfg.viewer, &people, &since(), None, Stream::Issue);
        assert_eq!(terms, vec![format!("{base} is:issue")]);

        let no_issues = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project","issues":false}]}"#,
            "<test>",
        )
        .unwrap();
        let rc = no_issues.repos[0].resolved();
        let terms = discovery_terms(&rc, &no_issues.viewer, &people, &since(), None, Stream::Pr);
        assert_eq!(terms, vec![format!("{base} is:pr")]);
        let terms = discovery_terms(
            &rc,
            &no_issues.viewer,
            &people,
            &since(),
            None,
            Stream::Issue,
        );
        assert_eq!(terms, Vec::<String>::new(), "issues off: no issue term");
        // Working scope: no issue stream exists at all.
        let working = parse(r#"{"viewer":"v","repos":["o/n"]}"#, "<test>").unwrap();
        let rc = working.repos[0].resolved();
        let terms = discovery_terms(&rc, &working.viewer, &people, &since(), None, Stream::Issue);
        assert_eq!(terms, Vec::<String>::new());
    }

    // The bounded window form the cap-splitting walk uses: a closed
    // updated:since..until range, same injection-safe interpolation.
    #[test]
    fn bounded_window_emits_closed_range() {
        let cfg = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project","issues":false}]}"#,
            "<test>",
        )
        .unwrap();
        let rc = cfg.repos[0].resolved();
        let until = Rfc3339Utc::parse("2026-07-15T12:00:00Z").unwrap();
        let terms = discovery_terms(&rc, &cfg.viewer, &[], &since(), Some(&until), Stream::Pr);
        assert_eq!(
            terms,
            vec![
                "repo:o/n updated:2026-07-01T00:00:00Z..2026-07-15T12:00:00Z \
                 sort:updated-desc is:pr"
                    .to_string()
            ]
        );
    }

    // The backfill flavor's exact rendered string, pinned like its
    // siblings: a future edit adding a non-newtype interpolation must fail
    // a string-level test, not just an end-to-end count.
    #[test]
    fn backfill_terms_render_exactly() {
        let cfg = parse(r#"{"viewer":"v","repos":["o/n"]}"#, "<test>").unwrap();
        let rc = cfg.repos[0].resolved();
        let added = vec![Login::new("Bob").unwrap()];
        let terms = super::backfill_terms(&rc, &added, &since(), None);
        assert_eq!(
            terms,
            vec![
                "repo:o/n updated:>=2026-07-01T00:00:00Z sort:updated-desc is:pr involves:bob"
                    .to_string()
            ]
        );
    }

    // pr_id_document inlines only a validated u64 (digits); the two string
    // identifiers stay variables. Pin the rendered shape.
    #[test]
    fn pr_id_document_inlines_digits_only() {
        let doc = super::pr_id_document(13864);
        assert!(doc.contains("pullRequest(number: 13864)"), "{doc}");
        assert!(doc.contains("$owner: String!"), "{doc}");
        assert!(doc.contains("rateLimit"), "{doc}");
    }

    // The refresh documents render TAIL_K wherever the template says
    // __K__, and no marker survives — a half-rendered document would be a
    // live GraphQL syntax error, caught here instead.
    #[test]
    fn tail_documents_render_k_completely() {
        let k = format!("(last: {}", super::TAIL_K);
        for doc in [
            super::refresh_pr_document(),
            super::tail_comments_document(),
        ] {
            assert!(doc.contains(&k), "{doc}");
            assert!(!doc.contains("__K__"), "{doc}");
        }
        // The backward-pagination pair, in both tail documents: a forward
        // pageInfo here would make every walk-back terminate instantly and
        // read as "overlap reached" (the round-0 context's claim 6).
        for doc in [
            super::refresh_pr_document(),
            super::tail_comments_document(),
        ] {
            assert!(doc.contains("hasPreviousPage startCursor"), "{doc}");
        }
        // The skeleton selections carry no bodies on thread comments; the
        // tail selection does (its rows are written verbatim).
        assert!(
            !super::SKELETON_THREADS_PAGE.contains(" body "),
            "skeleton must not fetch bodies"
        );
        assert!(super::tail_comments_document().contains("id body createdAt"));
    }
}
