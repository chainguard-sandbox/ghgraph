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

use crate::config::{RepoConfig, Scope};

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
/// `since` is RFC 3339; the caller has already applied the overlap window.
/// PLANNED (milestone 1): every interpolated value becomes a validating
/// newtype (RepoName, Login, Rfc3339Utc), enforced by this signature. Today
/// they are raw strings and the slash-count config check is the only guard —
/// "a/b is:issue" passes it and injects a qualifier. Do not add
/// interpolation sites before the newtypes land.
pub fn discovery_terms(
    rc: &RepoConfig,
    viewer: &str,
    people: &[String],
    since: &str,
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
///     without paging review history; with reviewRequests and the last
///     commit's push time this feeds attention::effective_review_state.
///     reviewDecision is stored raw and never trusted alone.
///   * commits(last:1) carries head oid + pushedDate: an approval that
///     predates the last push is a stale approval.
///   * Every author selection carries __typename: filters need structural
///     Bot detection at ingest (not just discovery — sync --pr skips
///     discovery entirely), and a unit test parses these documents to
///     assert it. author is Option everywhere in the parse types: a deleted
///     account (author: null) is ordinary data, ingested with author NULL —
///     never an error, never a filter match.
///   * repository { nameWithOwner } is the PR's own view of its repo:
///     mismatch against config detects a rename/transfer and surfaces as
///     CONFIGURATION ("repo renamed — update config"), never a silent
///     follow and never a silently empty stream.
pub const HYDRATE_PR: &str = r#"
query($id: ID!) {
  node(id: $id) {
    ... on PullRequest {
      id number title body state isDraft url
      author { login __typename }
      repository { nameWithOwner }
      headRefName baseRefName
      reviewDecision
      createdAt updatedAt mergedAt closedAt
      commits(last: 1) { nodes { commit { oid committedDate pushedDate } } }
      reviewRequests(first: 20) {
        totalCount
        nodes { requestedReviewer { ... on User { login } ... on Team { name } } }
      }
      latestOpinionatedReviews(first: 20) {
        totalCount
        nodes { author { login __typename } state submittedAt }
      }
      closingIssuesReferences(first: 10) {
        totalCount
        nodes { id number title state body author { login __typename } url updatedAt
                repository { nameWithOwner } }
      }
      comments(first: 50) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes { id author { login __typename } body createdAt lastEditedAt url isMinimized }
      }
      reviewThreads(first: 50) {
        totalCount
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 30) {
            totalCount
            nodes { id author { login __typename } body createdAt lastEditedAt url isMinimized }
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
pub const THREADS_PAGE: &str = r#"
query($id: ID!, $after: String) {
  node(id: $id) {
    ... on PullRequest {
      reviewThreads(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line
          comments(first: 50) {
            totalCount
            nodes { id author { login __typename } body createdAt lastEditedAt url isMinimized }
          }
        }
      }
    }
  }
  rateLimit { cost remaining resetAt }
}"#;
