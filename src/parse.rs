//! Typed views of the GraphQL response documents in queries.rs — the parse
//! side of the discovery/hydration split. One parse type per document,
//! field-for-field; change a document and its type together.
//!
//! Invariants, and the mechanism carrying each:
//!
//!   * Type = selection, mechanically. Every struct is
//!     `deny_unknown_fields`, so a selection the type does not carry is a
//!     parse error (selection ⊆ type). In the other direction every field
//!     is required — including Option fields, which serde would otherwise
//!     treat as silently missing-tolerant: each nullable-but-always-selected
//!     field carries `deserialize_with = "nullable"`, which restores
//!     key-required (absent key = parse error, `null` = `None`), so a typed
//!     field the document stopped selecting is a parse error (type ⊆
//!     selection). The two deliberately missing-tolerant fields
//!     (`Author::database_id`, the `rate_limit` envelopes) are exactly the
//!     Option fields WITHOUT the marker, each justified where it stands.
//!     The captured fixtures (tests/fixtures/*.json, re-captured by
//!     tests/capture.rs running the real documents against live GitHub)
//!     witness the equality — a document/type drift cannot land silently.
//!     One caveat, accepted as a trade: these functions take an
//!     already-parsed `&Value`, and serde_json's Value collapses duplicate
//!     keys last-wins before the typed parse can reject them. The input
//!     source (GitHub's GraphQL serializer via gh) does not emit duplicate
//!     keys, and rejecting them would mean parsing typed structs from raw
//!     bytes, forfeiting the shared in-band rateLimit handling — revisit if
//!     the transport ever hands over bytes.
//!   * API text is plain data. Logins arrive as [`ApiLogin`] — stored as
//!     received, compared only via `identity::login_eq` /
//!     `AuthorPattern::matches` — never through `identity::Login`'s
//!     validating `Deserialize`, whose error message echoes its input under
//!     a license (config is the operator's own text) that API responses do
//!     not hold. `ApiLogin` deliberately implements no `Display`, so
//!     `format!`-interpolating one into a search term or an error message
//!     is a type error. The derived `Debug` remains a formatting route
//!     ({:?} prints the login); it is kept because the fixture and
//!     round-trip harnesses need legible diagnostics, and no shipped code
//!     formats a parse type — reweigh if one ever does.
//!   * `author` is `Option` everywhere. The GraphQL schema keeps every
//!     `author` field nullable, and a null author is ordinary, live data —
//!     the blanket-`From` counterexample recorded in error.rs. GitHub
//!     materializes a deleted user as the `ghost` User
//!     (tests/fixtures/hydrate_pr_ghost.json) rather than null, but
//!     `author: null` occurs in production for other causes (observed on
//!     legacy accounts converted to Organizations, e.g. rails/rails#2);
//!     `None` must stay an ordinary ingest value, never an error or a
//!     filter match.
//!   * Errors are shape-only. [`ParseError`] names the document, never the
//!     content: serde's own messages echo scalar values on type mismatch,
//!     and response text is third-party — it must not reach an error
//!     envelope. Classification (TRANSIENT retry vs quarantine vs INTERNAL)
//!     happens at the call site per the no-blanket-`From` rule; the fixtures
//!     and the re-capture harness are the diagnostic for a shape mismatch,
//!     not the error message.
//!   * API-owned enumerations stay raw strings (`state`,
//!     `authorAssociation`, `reviewDecision`, `__typename`). GitHub adds
//!     variants (MANNEQUIN did not always exist); a closed enum would turn
//!     each addition into a quarantine storm. The schema stores them raw and
//!     judgment lives with the judge: `identity::is_bot` for `__typename`,
//!     attention.rs for the rest. Reversed only by a consumer needing
//!     exhaustive matching at ingest — none exists, and judgment reads the
//!     archive, so none is expected.
//!   * Timestamps validate at ingest. Every DateTime field is
//!     `time::Rfc3339Utc` (validated inside a non-echoing `Deserialize`;
//!     time.rs), so a malformed timestamp is a loud parse error here and
//!     unrepresentable past this boundary — the watermark fold and the
//!     schema's lexicographic-order convention both rely on it. The Z-only
//!     grammar is safe because the documents select only `DateTime` scalars
//!     (UTC-normalized, Z-terminated); GitHub's non-normalized git-time
//!     scalar is a different type, `GitTimestamp`, which no document
//!     selects — that is the evidence boundary for Z-only.
//!
//! Nullability is the introspected schema's, not a guess — with one
//! deliberate narrowing, recorded here with its reversal condition. The
//! GraphQL convention makes every connection's `nodes` list and items
//! nullable so a failed sub-resolver can bubble to the nearest nullable
//! field instead of failing the whole query. Carried faithfully that is
//! `Option<Vec<Option<T>>>` on every connection, and every consumer drowns.
//! The line drawn instead:
//!
//!   * Search hits are `Vec<Option<DiscoveryHit>>` — item-level null is kept
//!     because search spans visibility domains and a masked item is a real
//!     production case; the discovery walk must resolve a `None` hit to a
//!     defined outcome like any other id (PLANNED, milestone 2).
//!   * The three connections the schema itself marks nullable
//!     (`reviewRequests`, `latestOpinionatedReviews`,
//!     `closingIssuesReferences`) are `Option<_>` — `None` means that
//!     connection's resolver failed and was masked, so the hydrator must
//!     treat it as truncation, never as empty (PLANNED, milestone 2).
//!   * Everything else parses strict. A null where this module is strict
//!     fails the one PR's parse, and the quarantine row (error_class
//!     'parse') is the disclosed, retried outcome — the correct failure
//!     unit, and also the detector: a quarantined PR that heals on retry is
//!     the evidence that would loosen that spot to Option.
//!
//! What this module never does: judge. No filtering (the bot/author skip is
//! the discovery walk's, on data carried here), no folding (`nameWithOwner`
//! is compared case-folded at the comparison site), no derived fields.
//! Parse carries; sync.rs decides.

use std::fmt;

use serde::Deserialize;
#[cfg(feature = "harness")]
use serde::Serialize;

use crate::identity;
use crate::time::Rfc3339Utc;

// `Serialize` on the types here exists for the verification harness — the
// fuzz round-trip witness (parse → serialize → parse must be identity) needs
// a render direction. It is feature-gated (`harness`, enabled only by the
// fuzz workspace) so "nothing shipped serializes these types" is a compile
// error rather than a promise.

/// Restores key-required on a nullable field: serde treats every `Option`
/// field as missing-tolerant (an absent key silently becomes `None`), which
/// would let a document drop a selection without any parse failing — the
/// silent-drift direction `deny_unknown_fields` cannot see. Attached as
/// `deserialize_with` (which makes the key required), it accepts `null` as
/// `None` and nothing else by absence.
fn nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

/// Which document a response failed to match. The whole content of a
/// [`ParseError`] — deliberately, see the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Doc {
    Discovery,
    HydratePr,
    ThreadsPage,
    CommentsPage,
    PrId,
    RefreshPr,
    TailComments,
    SkeletonThreadsPage,
    ThreadBodies,
}

impl fmt::Display for Doc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Doc::Discovery => "DISCOVERY",
            Doc::HydratePr => "HYDRATE_PR",
            Doc::ThreadsPage => "THREADS_PAGE",
            Doc::CommentsPage => "COMMENTS_PAGE",
            Doc::PrId => "PR_ID",
            Doc::RefreshPr => "REFRESH_PR",
            Doc::TailComments => "TAIL_COMMENTS",
            Doc::SkeletonThreadsPage => "SKELETON_THREADS_PAGE",
            Doc::ThreadBodies => "THREAD_BODIES",
        })
    }
}

/// A response that does not match its document's parse type. Carries the
/// document name and nothing else: serde's message would name the offending
/// value, and response text never reaches an error message. To diagnose one,
/// re-capture the fixture (tests/capture.rs) and diff — the shape moved,
/// either under ghgraph (a bug) or under GitHub (a schema change).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseError {
    pub doc: Doc,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "response does not match the {} document's parse type \
             (ghgraph's selection and GitHub's live schema disagree)",
            self.doc
        )
    }
}

/// An API-returned login: plain data, stored as received. Compared only via
/// `identity::login_eq` / `AuthorPattern::matches` — never folded, never
/// validated (validation is for the operator's own identifiers;
/// identity.rs). No `Display` impl, on purpose: an API login must never be
/// interpolated into a search qualifier, an error message, or SQL text, and
/// the missing impl makes `format!("{}", login)` a compile error.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(transparent)]
pub struct ApiLogin(String);

impl ApiLogin {
    /// The login exactly as the API returned it. Sinks are bound SQL
    /// parameters and `login_eq` comparisons; see the type docs.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An author selection: `login`, structural `__typename`, and (hydration
/// only) the stable `databaseId`. DISCOVERY omits `databaseId` — its hits
/// decide skip-or-hydrate and are never stored — so the field defaults to
/// `None` there rather than forcing two author types.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Author {
    pub login: ApiLogin,
    #[serde(rename = "__typename")]
    pub typename: String,
    /// Stable numeric id; survives a login rename. `None` for an author
    /// matching neither databaseId-carrying fragment (schema-possible for
    /// Mannequin; the one Organization author observed live nulls the whole
    /// actor instead) and in discovery, which does not select it — this is
    /// one of the two fields deliberately WITHOUT the `nullable` marker
    /// (module docs): serde's plain-Option missing-tolerance is the wanted
    /// behavior. Stored, not yet consulted — ROADMAP names the evidence
    /// that would move matching onto it.
    pub database_id: Option<i64>,
}

impl Author {
    /// The one structural bot check, delegated to identity.rs: `__typename`
    /// only, never a login pattern.
    pub fn is_bot(&self) -> bool {
        identity::is_bot(&self.typename)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PageInfo {
    pub has_next_page: bool,
    #[serde(deserialize_with = "nullable")]
    pub end_cursor: Option<String>,
}

/// A connection selected with totalCount + pageInfo + nodes: the two big
/// per-PR connections (comments, reviewThreads), whose overflow triggers
/// follow-up pages. The three connection shapes below are distinct types on
/// purpose: which of totalCount/pageInfo a document selects is part of the
/// contract (pageInfo on comments is what makes "no silent caps" checkable),
/// so dropping one is a parse error, not a silent narrowing.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Paged<T> {
    pub total_count: i64,
    pub page_info: PageInfo,
    pub nodes: Vec<T>,
}

/// A connection selected with totalCount + nodes, no pageInfo: the bounded
/// selections (reviewRequests, latestOpinionatedReviews,
/// closingIssuesReferences, a thread's comments). Truncation is detectable
/// as `nodes.len() < total_count`; the hydrator owns that judgment.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Counted<T> {
    pub total_count: i64,
    pub nodes: Vec<T>,
}

/// The backward-pagination mirror of [`PageInfo`], for `last:`-selected
/// connections (the refresh tail). A distinct type on purpose: mixing the
/// forward pair into a backward walk typically terminates the walk
/// instantly (hasNextPage is false at the connection's end) and would read
/// as "overlap reached" — the mixup must be a parse error, not a quiet
/// wrong loop.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BackPageInfo {
    pub has_previous_page: bool,
    #[serde(deserialize_with = "nullable")]
    pub start_cursor: Option<String>,
}

/// A `last:`-selected connection: totalCount + the backward pageInfo pair +
/// nodes. The refresh tail's shape — count and tail in one value because
/// they arrive in one response (the one-document rule; sync.rs owns the
/// conservation judgment this feeds).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BackPaged<T> {
    pub total_count: i64,
    pub page_info: BackPageInfo,
    pub nodes: Vec<T>,
}

/// A connection selected as nodes only: commits(last: 1), where the one
/// node is the head commit and no count is wanted.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NodesOnly<T> {
    pub nodes: Vec<T>,
}

// ---------------------------------------------------------------------------
// DISCOVERY

/// One page of search results: ids, updatedAt, and the author needed for
/// filter skips. Nothing here is ever stored.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiscoveryPage {
    /// Total hits for the term across all pages — the discovery-cap
    /// heuristic's counting side (`nodes_seen == issueCount`).
    pub issue_count: i64,
    pub page_info: PageInfo,
    /// Item-level `Option` kept from the schema: search spans visibility
    /// domains, and a masked hit is real. A `None` still counts as seen —
    /// the walk must resolve it to a defined outcome (PLANNED, milestone 2).
    pub nodes: Vec<Option<DiscoveryHit>>,
}

/// One search hit. Both fragments (PullRequest, Issue) select these same
/// three fields, so one type parses both streams — the search term's
/// `is:pr` / `is:issue` already decided which arrives.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DiscoveryHit {
    pub id: String,
    pub updated_at: Rfc3339Utc,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DiscoveryData {
    search: DiscoveryPage,
    /// Typed and consumed in gh.rs (every document appends it); carried
    /// loose here so parse neither re-validates nor depends on whether the
    /// transport already stripped it — deliberately missing-tolerant, the
    /// other field class WITHOUT the `nullable` marker (module docs).
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one DISCOVERY response's `data`.
pub fn discovery(data: &serde_json::Value) -> Result<DiscoveryPage, ParseError> {
    DiscoveryData::deserialize(data)
        .map(|d| d.search)
        .map_err(|_| ParseError {
            doc: Doc::Discovery,
        })
}

// ---------------------------------------------------------------------------
// HYDRATE_PR

/// One hydrated PR: the full working-set context the writer turns into a
/// PrBundle. Field set = the HYDRATE_PR selection, exactly.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PrNode {
    pub id: String,
    pub number: i64,
    pub title: String,
    pub body: String,
    /// PullRequestState (OPEN | CLOSED | MERGED); raw on purpose (module
    /// docs).
    pub state: String,
    pub is_draft: bool,
    pub url: String,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
    pub author_association: String,
    pub repository: RepoRef,
    pub head_ref_name: String,
    pub base_ref_name: String,
    /// Raw API value; never trusted alone (queries.rs).
    #[serde(deserialize_with = "nullable")]
    pub review_decision: Option<String>,
    pub created_at: Rfc3339Utc,
    pub updated_at: Rfc3339Utc,
    #[serde(deserialize_with = "nullable")]
    pub merged_at: Option<Rfc3339Utc>,
    #[serde(deserialize_with = "nullable")]
    pub closed_at: Option<Rfc3339Utc>,
    pub commits: NodesOnly<CommitEdge>,
    /// `Option` on the connection itself: the schema marks these three
    /// nullable (unlike comments/reviewThreads/commits), which is GraphQL's
    /// error-masking — a failed sub-resolver bubbles null here instead of
    /// failing the query. `None` is that mask, and the hydrator must treat
    /// it as truncation, never as empty (PLANNED, milestone 2).
    #[serde(deserialize_with = "nullable")]
    pub review_requests: Option<Counted<ReviewRequestNode>>,
    #[serde(deserialize_with = "nullable")]
    pub latest_opinionated_reviews: Option<Counted<ReviewNode>>,
    #[serde(deserialize_with = "nullable")]
    pub closing_issues_references: Option<Counted<LinkedIssueNode>>,
    pub comments: Paged<CommentNode>,
    pub review_threads: Paged<ThreadNode>,
}

/// The PR's own view of its repo. Raw `nameWithOwner`; rename detection
/// compares it case-folded against config at the comparison site
/// (queries.rs records why).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RepoRef {
    pub name_with_owner: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CommitEdge {
    pub commit: Commit,
}

/// The head commit. `committedDate` is NOT push time: Commit.pushedDate is
/// deprecated upstream and returns null, so the approval-staleness signal
/// `prs.last_pushed_at` wanted has no API source in this document. The open
/// question and its interim guarantee are recorded at the document
/// (queries.rs) and the column (schema.sql).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Commit {
    pub oid: String,
    pub committed_date: Rfc3339Utc,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReviewRequestNode {
    /// Null means the reviewer NODE is not visible to the viewer — a
    /// private Team the viewer is not a member of is the common live case
    /// (tests/fixtures/hydrate_pr_comments.json carries one), deletion
    /// another. It does NOT mean "no reviewer": the request is real and
    /// totalCount counts it, so a consumer must never read `None` as
    /// gone/deleted (PLANNED, milestone 2 owns that judgment).
    #[serde(deserialize_with = "nullable")]
    pub requested_reviewer: Option<RequestedReviewer>,
}

/// The requestedReviewer union, parsed by shape: the document's fragments
/// select `login` for a User and `name` for a Team, and the two never
/// collide. The union has more members than the fragments cover (Bot,
/// Mannequin, EnterpriseTeam), and a VISIBLE member matching neither
/// fragment renders `{}` — [`RequestedReviewer::Unresolved`], live-reachable
/// (a pending Copilot request is a Bot). Adding `__typename` to name the
/// unresolved kind was considered and declined: the schema stores only
/// user/team requests, so the name would be carried judgment-free into
/// nothing; revisit if a real archive shows Bot/Mannequin requests
/// mattering to attention.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(untagged)]
pub enum RequestedReviewer {
    User(UserRef),
    Team(TeamRef),
    Unresolved(EmptyObject),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields)]
pub struct UserRef {
    pub login: ApiLogin,
}

/// A team name is org-controlled third-party text like any other API
/// string; it stays a plain field (bound-parameter sink) rather than an
/// `ApiLogin` because no equivalence discipline exists for team names —
/// `ApiLogin` enforces the login_eq rule, not a general text taint.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields)]
pub struct TeamRef {
    pub name: String,
}

/// The no-fragment-matched shape. `deny_unknown_fields` keeps it from
/// swallowing anything but a literal `{}`, so untagged dispatch stays
/// honest.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields)]
pub struct EmptyObject {}

/// One row of latestOpinionatedReviews: per-reviewer verdict without paging
/// review history. Becomes a comments row (kind='review'; schema.sql) —
/// which is why it selects `id` (comments.id is NOT NULL UNIQUE), `body`
/// (review summaries join comments_fts like any other comment text), and
/// `url`. `submittedAt` is schema-nullable; a `None` cannot become a
/// comments row (created_at is NOT NULL) and the writer skips it — an
/// opinionated review always carries one in practice, so the skip is
/// disclosure-free (sync.rs records the decision).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReviewNode {
    pub id: String,
    /// APPROVED | CHANGES_REQUESTED | … — raw (module docs).
    pub state: String,
    #[serde(deserialize_with = "nullable")]
    pub submitted_at: Option<Rfc3339Utc>,
    pub body: String,
    pub url: String,
    pub author_association: String,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
}

/// A linked issue from closingIssuesReferences — GitHub's own parse of the
/// closing keywords, cross-repo included; becomes a refs row
/// (kind='fixes', source='api') and a fill-only issues row.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LinkedIssueNode {
    pub id: String,
    pub number: i64,
    pub title: String,
    /// OPEN | CLOSED — raw (module docs).
    pub state: String,
    pub body: String,
    pub updated_at: Rfc3339Utc,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
    pub author_association: String,
    pub url: String,
    pub repository: RepoRef,
}

/// A comment, top-level or review-thread — the two selections are
/// identical, so one type serves both (`comments.kind` is the writer's).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CommentNode {
    pub id: String,
    pub body: String,
    pub created_at: Rfc3339Utc,
    #[serde(deserialize_with = "nullable")]
    pub last_edited_at: Option<Rfc3339Utc>,
    pub url: String,
    /// GitHub's own hostile-content label; load-bearing in attention.rs.
    pub is_minimized: bool,
    pub author_association: String,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ThreadNode {
    pub id: String,
    pub is_resolved: bool,
    pub is_outdated: bool,
    /// Non-null in the schema (a thread is anchored to a file), unlike
    /// `line`, which nulls for file-level and outdated-position threads.
    pub path: String,
    #[serde(deserialize_with = "nullable")]
    pub line: Option<i64>,
    pub comments: Counted<CommentNode>,
}

/// [`CommentNode`] minus `body`: the refresh skeleton's nested selection.
/// Everything mutable-without-bumping-PR.updatedAt is here (lastEditedAt,
/// isMinimized, authorAssociation), so a cheap-field flip never needs a
/// body fetch; sync.rs resolves bodies — from the archive when id +
/// lastEditedAt match the stored row, from THREAD_BODIES otherwise.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SkeletonCommentNode {
    pub id: String,
    pub created_at: Rfc3339Utc,
    #[serde(deserialize_with = "nullable")]
    pub last_edited_at: Option<Rfc3339Utc>,
    pub url: String,
    pub is_minimized: bool,
    pub author_association: String,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
}

/// [`ThreadNode`] with the skeleton nested selection.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SkeletonThreadNode {
    pub id: String,
    pub is_resolved: bool,
    pub is_outdated: bool,
    pub path: String,
    #[serde(deserialize_with = "nullable")]
    pub line: Option<i64>,
    pub comments: Counted<SkeletonCommentNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HydratePrData {
    #[serde(deserialize_with = "nullable")]
    node: Option<PrNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one HYDRATE_PR response's `data`. `Ok(None)` is `node: null` — the
/// id no longer resolves (deleted or access lost). That is data, not an
/// error: repeated `node: null` drains to `prs.deleted_at` (sync.rs), and
/// conflating it with a parse failure is the exact blanket-`From`
/// counterexample error.rs records. A non-null node that is not a
/// PullRequest arrives as `{}` and fails the parse — a ghgraph bug by
/// construction (only discovery-produced PR ids reach hydration).
pub fn hydrate_pr(data: &serde_json::Value) -> Result<Option<PrNode>, ParseError> {
    HydratePrData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::HydratePr,
        })
}

// ---------------------------------------------------------------------------
// THREADS_PAGE

/// A follow-up reviewThreads page, rooted at the PR node id.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ThreadsPageNode {
    pub review_threads: Paged<ThreadNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ThreadsPageData {
    #[serde(deserialize_with = "nullable")]
    node: Option<ThreadsPageNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one THREADS_PAGE response's `data`. `Ok(None)` = the PR vanished
/// mid-walk (`node: null`); the walk's outcome discipline owns it.
pub fn threads_page(data: &serde_json::Value) -> Result<Option<ThreadsPageNode>, ParseError> {
    ThreadsPageData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::ThreadsPage,
        })
}

// ---------------------------------------------------------------------------
// COMMENTS_PAGE

/// A follow-up top-level-comments page, rooted at the PR node id.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CommentsPageNode {
    pub comments: Paged<CommentNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CommentsPageData {
    #[serde(deserialize_with = "nullable")]
    node: Option<CommentsPageNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one COMMENTS_PAGE response's `data`. `Ok(None)` = the PR vanished
/// mid-walk (`node: null`); the walk's outcome discipline owns it.
pub fn comments_page(data: &serde_json::Value) -> Result<Option<CommentsPageNode>, ParseError> {
    CommentsPageData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::CommentsPage,
        })
}

// ---------------------------------------------------------------------------
// REFRESH_PR / TAIL_COMMENTS / SKELETON_THREADS_PAGE / THREAD_BODIES
// (the layered-refresh documents; sync.rs owns every judgment on these —
// this module only carries shapes)

/// [`PrNode`] with the refresh selections: a backward-paged comments tail
/// and skeleton threads. A distinct type, not a parameterized PrNode: which
/// connections carry bodies and which direction they page is document
/// contract, and deny_unknown_fields can only enforce it on a type that
/// states it.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RefreshPrNode {
    pub id: String,
    pub number: i64,
    pub title: String,
    pub body: String,
    pub state: String,
    pub is_draft: bool,
    pub url: String,
    #[serde(deserialize_with = "nullable")]
    pub author: Option<Author>,
    pub author_association: String,
    pub repository: RepoRef,
    pub head_ref_name: String,
    pub base_ref_name: String,
    #[serde(deserialize_with = "nullable")]
    pub review_decision: Option<String>,
    pub created_at: Rfc3339Utc,
    pub updated_at: Rfc3339Utc,
    #[serde(deserialize_with = "nullable")]
    pub merged_at: Option<Rfc3339Utc>,
    #[serde(deserialize_with = "nullable")]
    pub closed_at: Option<Rfc3339Utc>,
    pub commits: NodesOnly<CommitEdge>,
    /// Nullable-connection masking: same reading as PrNode's (None is a
    /// failed sub-resolver, treated as truncation, never as empty).
    #[serde(deserialize_with = "nullable")]
    pub review_requests: Option<Counted<ReviewRequestNode>>,
    #[serde(deserialize_with = "nullable")]
    pub latest_opinionated_reviews: Option<Counted<ReviewNode>>,
    #[serde(deserialize_with = "nullable")]
    pub closing_issues_references: Option<Counted<LinkedIssueNode>>,
    pub comments: BackPaged<CommentNode>,
    pub review_threads: Paged<SkeletonThreadNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RefreshPrData {
    #[serde(deserialize_with = "nullable")]
    node: Option<RefreshPrNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one REFRESH_PR response's `data`. `Ok(None)` is `node: null`,
/// same reading as [`hydrate_pr`]. A parse failure here is NOT quarantined
/// by the caller: the full walk is the strictly-more-complete form, so a
/// refresh that stops parsing escalates to it (sync.rs) — only a HYDRATE_PR
/// drift earns the parse-class quarantine row.
pub fn refresh_pr(data: &serde_json::Value) -> Result<Option<RefreshPrNode>, ParseError> {
    RefreshPrData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::RefreshPr,
        })
}

/// A walk-back tail page, rooted at the PR node id.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TailCommentsNode {
    pub comments: BackPaged<CommentNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TailCommentsData {
    #[serde(deserialize_with = "nullable")]
    node: Option<TailCommentsNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one TAIL_COMMENTS response's `data`. `Ok(None)` = the PR vanished
/// mid-walk; the refresh escalation discipline owns it.
pub fn tail_comments(data: &serde_json::Value) -> Result<Option<TailCommentsNode>, ParseError> {
    TailCommentsData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::TailComments,
        })
}

/// A follow-up skeleton reviewThreads page, rooted at the PR node id.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SkeletonThreadsPageNode {
    pub review_threads: Paged<SkeletonThreadNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SkeletonThreadsPageData {
    #[serde(deserialize_with = "nullable")]
    node: Option<SkeletonThreadsPageNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one SKELETON_THREADS_PAGE response's `data`.
pub fn skeleton_threads_page(
    data: &serde_json::Value,
) -> Result<Option<SkeletonThreadsPageNode>, ParseError> {
    SkeletonThreadsPageData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::SkeletonThreadsPage,
        })
}

/// One thread refetched with bodies, rooted at the THREAD's node id.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ThreadBodiesNode {
    pub id: String,
    pub comments: Counted<CommentNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ThreadBodiesData {
    #[serde(deserialize_with = "nullable")]
    node: Option<ThreadBodiesNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one THREAD_BODIES response's `data`. `Ok(None)` = the thread
/// vanished between the skeleton and the body fetch; the caller withholds
/// the threads witness (deletion evidence arrives on the next full walk or
/// re-verify, never from a refresh).
pub fn thread_bodies(data: &serde_json::Value) -> Result<Option<ThreadBodiesNode>, ParseError> {
    ThreadBodiesData::deserialize(data)
        .map(|d| d.node)
        .map_err(|_| ParseError {
            doc: Doc::ThreadBodies,
        })
}

// ---------------------------------------------------------------------------
// PR_ID

/// The `repository { pullRequest { id } }` lookup for `sync --pr`. Both
/// levels are schema-nullable and both nulls are data: "no such repo (or not
/// visible)" and "no such PR" respectively — the caller renders each as
/// USER_INPUT naming the reference, never a parse error.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RepoIdNode {
    #[serde(deserialize_with = "nullable")]
    pub pull_request: Option<PrIdNode>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "harness", derive(Serialize))]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PrIdNode {
    pub id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PrIdData {
    #[serde(deserialize_with = "nullable")]
    repository: Option<RepoIdNode>,
    /// See DiscoveryData::rate_limit.
    #[allow(dead_code)]
    rate_limit: Option<serde_json::Value>,
}

/// Parse one PR_ID response's `data`.
pub fn pr_id(data: &serde_json::Value) -> Result<Option<RepoIdNode>, ParseError> {
    PrIdData::deserialize(data)
        .map(|d| d.repository)
        .map_err(|_| ParseError { doc: Doc::PrId })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn data(fixture: &str) -> Value {
        let v: Value = serde_json::from_str(fixture).unwrap();
        v["data"].clone()
    }

    // ------------------------------------------------------------------
    // Captured fixtures: the empirical half of "type = selection". Each of
    // these parses a verbatim live response to the verbatim document
    // (tests/capture.rs), so a pass means document, parse type, and GitHub's
    // live schema agreed at capture time.

    #[test]
    fn discovery_fixture_parses() {
        let page = discovery(&data(include_str!("../tests/fixtures/discovery_page.json"))).unwrap();
        assert!(page.issue_count >= 1);
        assert!(!page.nodes.is_empty());
        for hit in &page.nodes {
            let hit = hit.as_ref().expect("no masked hits in this capture");
            assert!(!hit.id.is_empty());
            let author = hit.author.as_ref().expect("capture has no ghost authors");
            assert!(!author.login.as_str().is_empty());
            // DISCOVERY does not select databaseId; the shared Author type
            // must default it, never invent it.
            assert_eq!(author.database_id, None);
        }
        // The capture crosses both author kinds (dependabot is in the page),
        // and the judgment is structural.
        assert!(
            page.nodes
                .iter()
                .flatten()
                .any(|h| h.author.as_ref().is_some_and(Author::is_bot))
        );
        assert!(
            page.nodes
                .iter()
                .flatten()
                .any(|h| h.author.as_ref().is_some_and(|a| !a.is_bot()))
        );
    }

    #[test]
    fn bot_actor_fixture_hits_parse_as_discovery_hits() {
        // The bot-actor fixture (identity.rs) is discovery-shaped; parsing
        // its hits here ties AuthorPattern's evidence to the type that
        // carries it at ingest.
        let v: Value =
            serde_json::from_str(include_str!("../tests/fixtures/bot_actor.json")).unwrap();
        let bots: Vec<Option<DiscoveryHit>> =
            serde_json::from_value(v["data"]["bot"]["nodes"].clone()).unwrap();
        let humans: Vec<Option<DiscoveryHit>> =
            serde_json::from_value(v["data"]["human"]["nodes"].clone()).unwrap();
        assert!(
            bots.iter()
                .flatten()
                .all(|h| h.author.as_ref().unwrap().is_bot())
        );
        assert!(
            humans
                .iter()
                .flatten()
                .all(|h| !h.author.as_ref().unwrap().is_bot())
        );
    }

    #[test]
    fn hydrate_threads_fixture_parses() {
        let pr = hydrate_pr(&data(include_str!(
            "../tests/fixtures/hydrate_pr_threads.json"
        )))
        .unwrap()
        .expect("node resolves");
        assert_eq!(pr.state, "MERGED");
        assert!(pr.repository.name_with_owner.contains('/'));

        // Exactly one head commit, a 40-hex oid, committedDate validated.
        assert_eq!(pr.commits.nodes.len(), 1);
        let head = &pr.commits.nodes[0].commit;
        assert_eq!(head.oid.len(), 40);
        assert!(head.oid.bytes().all(|b| b.is_ascii_hexdigit()));

        let threads = &pr.review_threads;
        assert!(threads.total_count >= 1);
        let t = &threads.nodes[0];
        assert!(!t.path.is_empty(), "path is schema-non-null");
        assert!(t.comments.total_count >= 1);
        // A live Bot author WITH databaseId — the User/Bot fragments both
        // carry it, and this capture proves the Bot arm.
        let bot = t.comments.nodes[0].author.as_ref().unwrap();
        assert!(bot.is_bot());
        assert!(bot.database_id.is_some());

        let reviews = pr.latest_opinionated_reviews.as_ref().unwrap();
        assert!(reviews.total_count >= 1);
        assert_eq!(reviews.nodes[0].state, "APPROVED");
        assert!(reviews.nodes[0].submitted_at.is_some());

        let closing = pr.closing_issues_references.as_ref().unwrap();
        assert!(closing.total_count >= 1);
        assert!(closing.nodes[0].repository.name_with_owner.contains('/'));
    }

    #[test]
    fn hydrate_comments_fixture_parses() {
        let pr = hydrate_pr(&data(include_str!(
            "../tests/fixtures/hydrate_pr_comments.json"
        )))
        .unwrap()
        .expect("node resolves");
        assert!(pr.comments.total_count >= 1);
        assert!(!pr.comments.nodes.is_empty());
        assert!(!pr.comments.nodes[0].is_minimized);

        // Live fact, pinned: a request whose reviewer node the viewer cannot
        // see (here a private team, per the REST cross-check) arrives as
        // requestedReviewer: null — while totalCount still counts it. None
        // is "invisible", never "no request".
        let requests = pr.review_requests.as_ref().unwrap();
        assert!(requests.total_count >= 1);
        assert!(
            requests
                .nodes
                .iter()
                .any(|r| r.requested_reviewer.is_none())
        );
    }

    #[test]
    fn ghost_author_fixture_pins_the_platform_rendering() {
        // GitHub materializes a deleted user as the `ghost` User (with
        // ghost's own databaseId) rather than author:null. The
        // schema-permitted author:null is pinned separately below — both
        // must stay ordinary data.
        let pr = hydrate_pr(&data(include_str!(
            "../tests/fixtures/hydrate_pr_ghost.json"
        )))
        .unwrap()
        .expect("node resolves");
        let author = pr
            .author
            .expect("deleted account renders as ghost, not null");
        assert_eq!(author.login.as_str(), "ghost");
        assert!(!author.is_bot());
        assert!(author.database_id.is_some());
        // Nullable scalars exercised by this capture.
        assert_eq!(pr.review_decision, None);
    }

    #[test]
    fn threads_page_fixture_parses() {
        let node = threads_page(&data(include_str!("../tests/fixtures/threads_page.json")))
            .unwrap()
            .expect("node resolves");
        // totalCount re-selected on follow-up pages (queries.rs records why)
        // — the Paged shape holds for page one and page N alike.
        assert!(node.review_threads.total_count >= 1);
        assert!(!node.review_threads.nodes.is_empty());
        // Independent structural evidence, not just shape-parses: this is a
        // real follow-up page, so pin the same load-bearing facts the
        // first-page fixture pins.
        let t = &node.review_threads.nodes[0];
        assert!(!t.path.is_empty(), "path is schema-non-null");
        assert!(t.comments.total_count >= 1);
        assert!(!t.comments.nodes.is_empty());
        assert!(t.comments.nodes[0].author.is_some());
    }

    // ------------------------------------------------------------------
    // Hand-pinned shapes the captures cannot show.

    /// A minimal HYDRATE_PR node with every required field, for shape
    /// surgery in the tests below.
    fn minimal_pr() -> Value {
        json!({
            "id": "PR_x", "number": 1, "title": "t", "body": "b",
            "state": "OPEN", "isDraft": false, "url": "https://example.invalid/1",
            "author": {"login": "someone", "__typename": "User", "databaseId": 1},
            "authorAssociation": "NONE",
            "repository": {"nameWithOwner": "o/n"},
            "headRefName": "h", "baseRefName": "b",
            "reviewDecision": null,
            "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z",
            "mergedAt": null, "closedAt": null,
            "commits": {"nodes": [{"commit": {"oid": "0123456789012345678901234567890123456789", "committedDate": "2026-01-01T00:00:00Z"}}]},
            "reviewRequests": {"totalCount": 0, "nodes": []},
            "latestOpinionatedReviews": {"totalCount": 0, "nodes": []},
            "closingIssuesReferences": {"totalCount": 0, "nodes": []},
            "comments": {"totalCount": 0, "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []},
            "reviewThreads": {"totalCount": 0, "pageInfo": {"hasNextPage": false, "endCursor": null}, "nodes": []}
        })
    }

    #[test]
    fn node_null_is_data_not_an_error() {
        // The blanket-`From` counterexample (error.rs): an id that no longer
        // resolves is ordinary data draining to deleted_at, never a failure.
        assert_eq!(hydrate_pr(&json!({"node": null})).unwrap(), None);
        assert_eq!(threads_page(&json!({"node": null})).unwrap(), None);
    }

    #[test]
    fn author_null_is_data_not_an_error() {
        // author is schema-nullable on every node; it must parse as None,
        // never fail, never later match a filter.
        let mut pr = minimal_pr();
        pr["author"] = Value::Null;
        let parsed = hydrate_pr(&json!({"node": pr})).unwrap().unwrap();
        assert_eq!(parsed.author, None);
    }

    #[test]
    fn non_pr_node_is_a_parse_error() {
        // A node id of the wrong type matches no fragment and arrives as {}
        // — distinct from node:null, and a ghgraph bug by construction, so
        // it must fail the parse rather than read as deleted.
        assert!(hydrate_pr(&json!({"node": {}})).is_err());
    }

    #[test]
    fn nullable_connections_mask_as_none_strict_ones_do_not() {
        // The schema marks exactly three connections nullable; a masked
        // (error-bubbled) one parses as None for the hydrator to treat as
        // truncation. The non-null ones stay strict: null there is schema
        // drift and must fail loudly.
        let mut pr = minimal_pr();
        pr["reviewRequests"] = Value::Null;
        pr["latestOpinionatedReviews"] = Value::Null;
        pr["closingIssuesReferences"] = Value::Null;
        let parsed = hydrate_pr(&json!({"node": pr.clone()})).unwrap().unwrap();
        assert_eq!(parsed.review_requests, None);
        assert_eq!(parsed.latest_opinionated_reviews, None);
        assert_eq!(parsed.closing_issues_references, None);

        pr["comments"] = Value::Null;
        assert!(hydrate_pr(&json!({"node": pr})).is_err());
    }

    #[test]
    fn type_equals_selection_in_both_directions() {
        // selection ⊆ type: a field the document selects but the type does
        // not carry fails (deny_unknown_fields).
        let mut extra = minimal_pr();
        extra["somethingNew"] = json!(1);
        assert!(hydrate_pr(&json!({"node": extra})).is_err());

        // type ⊆ selection: a field the type carries but the document
        // stopped selecting fails — for a required field (serde's own
        // missing-field error)...
        let mut missing = minimal_pr();
        missing.as_object_mut().unwrap().remove("updatedAt");
        assert!(hydrate_pr(&json!({"node": missing})).is_err());

        // ...and, the direction serde would silently forgive, for Option
        // fields: the `nullable` marker makes the KEY required while null
        // stays None, so dropping a nullable selection is just as loud.
        for key in [
            "author",
            "mergedAt",
            "closedAt",
            "reviewDecision",
            "reviewRequests",
            "latestOpinionatedReviews",
            "closingIssuesReferences",
        ] {
            let mut dropped = minimal_pr();
            dropped.as_object_mut().unwrap().remove(key);
            assert!(
                hydrate_pr(&json!({"node": dropped})).is_err(),
                "dropping the {key} selection must not parse"
            );
        }
        let mut page_info_narrowed = minimal_pr();
        page_info_narrowed["comments"]["pageInfo"] = json!({"hasNextPage": false});
        assert!(hydrate_pr(&json!({"node": page_info_narrowed})).is_err());

        // The deliberate exception: databaseId is absent in DISCOVERY, so
        // Author tolerates the missing key (and only this field does).
        let mut no_db_id = minimal_pr();
        no_db_id["author"] = json!({"login": "a", "__typename": "User"});
        assert!(hydrate_pr(&json!({"node": no_db_id})).is_ok());
    }

    #[test]
    fn missing_node_key_is_a_parse_error_not_a_deletion() {
        // Ok(None) drains to deleted_at, so it must be producible only by a
        // literal node:null — a response missing the key entirely (spec
        // violation or transport mangling) must fail, not read as deleted.
        assert!(hydrate_pr(&json!({})).is_err());
        assert!(threads_page(&json!({})).is_err());
    }

    #[test]
    fn rate_limit_at_the_parser_depth_cap_parses() {
        // The rateLimit envelope is carried as a loose Value; its
        // deserialization recurses per nesting level, bounded in production
        // by serde_json's 128-level byte-parser cap (gh.rs, Response.data).
        // Pin that a Value at that cap parses with margin to spare.
        let mut deep = json!(1);
        for _ in 0..127 {
            deep = json!([deep]);
        }
        let doc = json!({
            "search": {
                "issueCount": 0,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": []
            },
            "rateLimit": deep
        });
        assert!(discovery(&doc).is_ok());
    }

    #[test]
    fn requested_reviewer_parses_by_shape() {
        let parse = |rr: Value| {
            let mut pr = minimal_pr();
            pr["reviewRequests"] = json!({"totalCount": 1, "nodes": [{"requestedReviewer": rr}]});
            hydrate_pr(&json!({"node": pr}))
                .unwrap()
                .unwrap()
                .review_requests
                .unwrap()
                .nodes[0]
                .requested_reviewer
                .clone()
        };
        match parse(json!({"login": "alice"})) {
            Some(RequestedReviewer::User(u)) => assert_eq!(u.login.as_str(), "alice"),
            other => panic!("expected User, got {other:?}"),
        }
        match parse(json!({"name": "platform"})) {
            Some(RequestedReviewer::Team(t)) => assert_eq!(t.name, "platform"),
            other => panic!("expected Team, got {other:?}"),
        }
        // A visible union member matching neither fragment (Bot, Mannequin,
        // EnterpriseTeam) — live-reachable, e.g. a pending Copilot request.
        assert_eq!(
            parse(json!({})),
            Some(RequestedReviewer::Unresolved(EmptyObject {}))
        );
        assert_eq!(parse(Value::Null), None);
        // A shape matching no variant is drift, not data.
        let mut pr = minimal_pr();
        pr["reviewRequests"] =
            json!({"totalCount": 1, "nodes": [{"requestedReviewer": {"login": "a", "name": "b"}}]});
        assert!(hydrate_pr(&json!({"node": pr})).is_err());
    }

    #[test]
    fn search_hits_keep_item_level_null() {
        // Search spans visibility domains; a masked hit is data the walk
        // must resolve to an outcome, not a page-fatal error.
        let page = discovery(&json!({
            "search": {
                "issueCount": 2,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": [
                    null,
                    {"id": "PR_y", "updatedAt": "2026-01-01T00:00:00Z", "author": null}
                ]
            }
        }))
        .unwrap();
        assert_eq!(page.nodes.len(), 2);
        assert_eq!(page.nodes[0], None);
        let hit = page.nodes[1].as_ref().unwrap();
        assert_eq!(hit.author, None);
    }

    #[test]
    fn timestamps_validate_at_ingest() {
        let mut pr = minimal_pr();
        pr["updatedAt"] = json!("2026-13-40T99:99:99Z");
        assert!(hydrate_pr(&json!({"node": pr})).is_err());
        let mut pr = minimal_pr();
        pr["updatedAt"] = json!("2026-01-01T00:00:00+02:00"); // offset form: not Z
        assert!(hydrate_pr(&json!({"node": pr})).is_err());
    }

    #[test]
    fn rate_limit_key_is_tolerated_present_or_absent() {
        // gh.rs owns rateLimit; parse must accept data whether or not the
        // transport already stripped it.
        let search = json!({
            "issueCount": 0,
            "pageInfo": {"hasNextPage": false, "endCursor": null},
            "nodes": []
        });
        let rl = json!({"cost": 1, "remaining": 4999, "resetAt": "2026-01-01T00:00:00Z"});
        assert!(discovery(&json!({"search": search})).is_ok());
        assert!(discovery(&json!({"search": search, "rateLimit": rl})).is_ok());
        // Same leniency on the other two wrappers — one test per envelope,
        // so a refactor cannot narrow one of them unnoticed.
        assert!(hydrate_pr(&json!({"node": null})).is_ok());
        assert!(hydrate_pr(&json!({"node": null, "rateLimit": rl})).is_ok());
        assert!(threads_page(&json!({"node": null})).is_ok());
        assert!(threads_page(&json!({"node": null, "rateLimit": rl})).is_ok());
    }

    #[test]
    fn parse_error_never_echoes_response_text() {
        // The error is shape-only: serde's messages echo scalar values, and
        // response text is third-party — it must not reach an envelope.
        let marker = "EVIL_MARKER_c0ffee";
        let mut pr = minimal_pr();
        pr["number"] = json!(marker); // type mismatch at a scalar
        let err = hydrate_pr(&json!({"node": pr})).unwrap_err();
        let shown = format!("{err} / {err:?}");
        assert!(!shown.contains(marker));
        assert_eq!(err.doc, Doc::HydratePr);
        assert_eq!(
            err.to_string(),
            "response does not match the HYDRATE_PR document's parse type \
             (ghgraph's selection and GitHub's live schema disagree)"
        );
    }

    #[test]
    fn comments_page_fixture_parses() {
        let node = comments_page(&data(include_str!("../tests/fixtures/comments_page.json")))
            .unwrap()
            .expect("node resolves");
        assert!(node.comments.total_count >= 1);
        assert!(!node.comments.nodes.is_empty());
        let c = &node.comments.nodes[0];
        assert!(!c.id.is_empty());
        assert!(c.author.is_some());
    }

    #[test]
    fn comments_page_node_null_and_shape_errors() {
        assert_eq!(comments_page(&json!({"node": null})).unwrap(), None);
        assert!(comments_page(&json!({})).is_err(), "missing key is drift");
        // A node that is not a PullRequest ({} from an unmatched fragment)
        // must fail, not read as an empty page.
        assert!(comments_page(&json!({"node": {}})).is_err());
    }

    #[test]
    fn refresh_pr_fixture_parses() {
        let pr = refresh_pr(&data(include_str!("../tests/fixtures/refresh_pr.json")))
            .unwrap()
            .expect("node resolves");
        // The tail on cli/cli#13987 covers the whole small connection: the
        // backward pair reads "no un-fetched middle".
        assert!(pr.comments.total_count >= 1);
        assert_eq!(pr.comments.nodes.len() as i64, pr.comments.total_count);
        assert!(!pr.comments.page_info.has_previous_page);
        assert!(pr.comments.page_info.start_cursor.is_some());
        // Same PR the hydration fixture pins: the review request is there.
        assert!(
            pr.review_requests
                .as_ref()
                .is_some_and(|r| r.total_count >= 1)
        );
        assert!(!pr.comments.nodes[0].body.is_empty());
    }

    #[test]
    fn refresh_pr_node_null_and_shape_errors() {
        assert_eq!(refresh_pr(&json!({"node": null})).unwrap(), None);
        assert!(refresh_pr(&json!({})).is_err(), "missing key is drift");
        assert!(refresh_pr(&json!({"node": {}})).is_err());
    }

    #[test]
    fn tail_comments_fixture_parses() {
        let node = tail_comments(&data(include_str!("../tests/fixtures/tail_comments.json")))
            .unwrap()
            .expect("node resolves");
        assert!(node.comments.total_count >= 1);
        assert_eq!(node.comments.nodes.len() as i64, node.comments.total_count);
        assert!(!node.comments.page_info.has_previous_page);
    }

    #[test]
    fn tail_comments_node_null_and_shape_errors() {
        assert_eq!(tail_comments(&json!({"node": null})).unwrap(), None);
        assert!(tail_comments(&json!({})).is_err());
        assert!(tail_comments(&json!({"node": {}})).is_err());
        // A FORWARD pageInfo in a tail response is drift, not tolerable
        // variation: the walk-back loop reads the backward pair, and a
        // mixup must die at parse, not terminate the loop early (the
        // round-0 context's claim 6).
        assert!(
            tail_comments(&json!({"node": {"comments": {
                "totalCount": 1,
                "pageInfo": {"hasNextPage": false, "endCursor": null},
                "nodes": []
            }}}))
            .is_err()
        );
    }

    #[test]
    fn skeleton_threads_page_fixture_parses() {
        let node = skeleton_threads_page(&data(include_str!(
            "../tests/fixtures/skeleton_threads_page.json"
        )))
        .unwrap()
        .expect("node resolves");
        assert!(node.review_threads.total_count >= 1);
        let t = &node.review_threads.nodes[0];
        assert_eq!(t.id, "PRRT_kwDODKw3uc6QWWXy");
        assert!(t.comments.total_count >= 1);
        // The skeleton carries the edit signal and the cheap mutable
        // fields; body's absence is enforced by the type (deny_unknown
        // would reject a body key), so this only spot-checks the signal.
        assert!(!t.comments.nodes[0].id.is_empty());
        assert!(t.comments.nodes[0].author.is_some());
    }

    #[test]
    fn skeleton_threads_page_rejects_bodies() {
        // deny_unknown_fields in the live direction: a response that DOES
        // carry a body (someone pointed the skeleton parser at a full
        // THREADS_PAGE response) is drift, not silently-extra data.
        let node =
            skeleton_threads_page(&data(include_str!("../tests/fixtures/threads_page.json")));
        assert!(
            node.is_err(),
            "a bodied response must not parse as skeleton"
        );
        assert_eq!(skeleton_threads_page(&json!({"node": null})).unwrap(), None);
        assert!(skeleton_threads_page(&json!({})).is_err());
    }

    #[test]
    fn thread_bodies_fixture_parses() {
        let node = thread_bodies(&data(include_str!("../tests/fixtures/thread_bodies.json")))
            .unwrap()
            .expect("node resolves");
        assert_eq!(node.id, "PRRT_kwDODKw3uc6QWWXy");
        assert_eq!(node.comments.nodes.len() as i64, node.comments.total_count);
        assert!(!node.comments.nodes[0].body.is_empty());
    }

    #[test]
    fn thread_bodies_node_null_and_shape_errors() {
        assert_eq!(thread_bodies(&json!({"node": null})).unwrap(), None);
        assert!(thread_bodies(&json!({})).is_err());
        assert!(thread_bodies(&json!({"node": {}})).is_err());
    }

    // The enablement gate, pinned offline (round-0 spec audit): minimized
    // comments COUNT in the comments connection's totalCount, witnessed
    // live on cli/cli#13918, whose only top-level comment is minimized as
    // spam. The conservation check's counting universe (sync.rs) leans on
    // this: archived live rows INCLUDE is_minimized=1 rows, and if GitHub
    // excluded them upstream the arithmetic would bias toward false
    // passes. If a re-capture breaks this test, the universe moved — stop
    // and re-derive the check before touching the assertion.
    #[test]
    fn minimized_comments_count_in_total_count() {
        let node = tail_comments(&data(include_str!(
            "../tests/fixtures/comments_minimized.json"
        )))
        .unwrap()
        .expect("node resolves");
        assert_eq!(node.comments.nodes.len() as i64, node.comments.total_count);
        assert!(node.comments.nodes.iter().any(|c| c.is_minimized));
    }

    #[test]
    fn pr_id_fixture_parses() {
        let repo = pr_id(&data(include_str!("../tests/fixtures/pr_id.json")))
            .unwrap()
            .expect("repository resolves");
        let pr = repo.pull_request.expect("PR resolves");
        assert!(pr.id.starts_with("PR_"), "{}", pr.id);
    }

    #[test]
    fn pr_id_nulls_are_data() {
        // Repo invisible and PR absent are USER_INPUT-grade data, never
        // parse errors; a missing key stays loud.
        assert_eq!(pr_id(&json!({"repository": null})).unwrap(), None);
        let repo = pr_id(&json!({"repository": {"pullRequest": null}}))
            .unwrap()
            .expect("repo resolves");
        assert_eq!(repo.pull_request, None);
        assert!(pr_id(&json!({})).is_err());
    }

    #[test]
    fn api_login_is_stored_as_received() {
        // Never folded (fold is for the operator's validated identifiers);
        // equality goes through login_eq. The no-Display property is
        // compile-time: an interpolation site simply does not build.
        let l: ApiLogin = serde_json::from_value(json!("MiXeD")).unwrap();
        assert_eq!(l.as_str(), "MiXeD");
        assert!(crate::identity::login_eq(l.as_str(), "mixed"));
        assert!(!crate::identity::login_eq(l.as_str(), "other"));
    }
}
