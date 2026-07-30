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

/// The discovery search strings for one configured repo.
/// `since` is the caller's watermark with the overlap window already applied.
/// This signature is the injection boundary: every interpolated value is a
/// validating newtype — RepoName and Login (identity.rs) admit no space or
/// ':', Rfc3339Utc's canonical form is charset-bounded to [0-9:TZ-] with ':'
/// only in the fixed HH:MM:SS positions — so a value that could smuggle a
/// second qualifier ("owner/name involves:someone-else") is unrepresentable
/// here, not filtered here. The counterexample strings live on as unit tests
/// (identity.rs, config.rs). Do not add interpolation sites that take raw
/// strings.
pub fn discovery_terms(
    rc: &RepoConfig,
    viewer: &Login,
    people: &[Login],
    since: &Rfc3339Utc,
) -> Vec<String> {
    let base = format!("repo:{} updated:>={since} sort:updated-desc", rc.repo);
    let pr = format!("{base} is:pr");
    match rc.scope {
        Scope::Project => {
            let mut terms = vec![pr];
            if rc.issues() {
                terms.push(format!("{base} is:issue"));
            }
            terms
        }
        Scope::Working => {
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
///     every hydration whenever GitHub drops the field. OPEN QUESTION
///     (milestone 2): the approval-staleness signal needs a replacement
///     source — candidates are the force-push timeline event (in tension
///     with the timeline standing rejection, DESIGN.md), the sync's own
///     observed head_sha flip time (local, not server time), or
///     committedDate as a lower bound (push ≥ commit, so approval <
///     committedDate proves staleness but the converse proves nothing).
///     Interim guarantee: last_pushed_at stays NULL, and attention's
///     polarity contract degrades NULL/unknown ordering OUT of
///     ready_to_merge (PLANNED, milestone 3 — attention.rs) — the bucket
///     under-fills, it never lies.
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
        nodes { state submittedAt authorAssociation
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
/// parent node id. Variables: $id, $after. One document per connection kind;
/// THREADS_PAGE shown, COMMENTS_PAGE analogous.
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

#[cfg(test)]
mod tests {
    use super::discovery_terms;
    use crate::config::parse;
    use crate::identity::Login;
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
        let terms = discovery_terms(&rc, &cfg.viewer, &cfg.people, &since());
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
        let terms = discovery_terms(&rc, &cfg.viewer, &people, &since());
        let base = "repo:o/n updated:>=2026-07-01T00:00:00Z sort:updated-desc";
        assert_eq!(
            terms,
            vec![format!("{base} is:pr"), format!("{base} is:issue")]
        );

        let no_issues = parse(
            r#"{"viewer":"v","repos":[{"repo":"o/n","scope":"project","issues":false}]}"#,
            "<test>",
        )
        .unwrap();
        let rc = no_issues.repos[0].resolved();
        let terms = discovery_terms(&rc, &no_issues.viewer, &people, &since());
        assert_eq!(terms, vec![format!("{base} is:pr")]);
    }
}
