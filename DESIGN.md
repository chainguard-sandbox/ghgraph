# ghgraph design

Local GitHub work memory. Syncs GitHub conversation into SQLite and answers
from the archive instead of the API. Two scopes, chosen per repo in config:
**working** — the operator's working set (PRs authored, PRs under review,
review threads, linked issue context) — and **project** — a repo's whole PR
and issue stream, for a maintainer who needs triage over a large active
project. Between them, working-scope repos can track named **people**:
collaborators or contributors the operator opts in, whose involvement is
archived alongside the operator's own without widening to the whole repo.

Status: design scaffold. Types, schema, command surface, and invariants are
final-draft; function bodies are stubs. ROADMAP.md sequences the build.
Mechanism and its rationale live as module comments where the code is
(src/sync.rs, src/schema.sql, src/gh.rs, src/queries.rs); this document
carries the architecture and the arguments between modules, deliberately
not everything.

## Non-goals

- Org- or forge-wide mirroring. Every synced repo is named in the config —
  discovery scope is the config, not a search — and project scope archives a
  repo's conversation (PRs, issues, comments), never the forge around it:
  no CI runs, no releases, no git objects.
- An event system. `observations` is the sync's own field-diff changelog,
  PR fields only — comment edits are visible via `comments.updated_at`,
  never as observation rows. This line holds forever; an event system
  returns by accretion, one granularity at a time.
- Graph machinery. Relationships are rows in `refs`; rendering (dot, kanban,
  DAG views) belongs to consumers reading the archive.

## Dependencies

Strict by policy; every entry justified in Cargo.toml; tree audited with
cargo vet. rusqlite (bundled — the only C), serde, serde_json, clap. No HTTP,
TLS, or async crates: network transport is the `gh` CLI as a subprocess,
which owns auth, SSO, and TLS. `gh` is a documented runtime prerequisite.
`unsafe` is forbidden crate-wide. No time crate: timestamps are strict
RFC 3339 UTC "Z" strings, validated at ingest by ~40 lines of std. The MSRV
is 1.95, set by the dependency tree: rusqlite and libsqlite3-sys use std's
`cfg_select!` (stable since 1.95). Our own code needs only 1.89
(`std::fs::File::try_lock`, the sync run lock); the deps dominate.

The gh coupling is a seam, not a marriage: `gh::graphql()` is the entire
transport surface, nothing else knows gh exists, and every semantic signal
(rate limit, GraphQL errors) rides in-band rather than in gh's exit codes or
stderr. Swapping transports would be a one-module rewrite; the observations
that would trigger it are recorded in src/gh.rs. A minimum-version gate at
sync start keeps the one heuristic surface (stderr classification) honest.

Unix only, enforced by a `compile_error!` in main.rs: the cancellation
invariant is process-group SIGINT semantics and archive protection is mode
bits, neither of which has a Windows expression — a port would mean two
mechanisms and two proofs where the design wants one.

A future MCP server starts as an external per-call wrapper spawning the CLI —
zero new dependencies, inherits every CLI invariant. A feature-gated resident
binary lands only on measured spawn latency, never before.

## Sync

Mechanism and its preconditions live in src/sync.rs and src/queries.rs;
this section carries the architecture and the arguments between the layers.

### Discovery

Discovery/hydration split (src/queries.rs): `search()` finds ids cheaply,
then each item hydrates by node id. Working-scope repos run three qualifier
flavors for the viewer (`involves:`, `review-requested:`, `reviewed-by:` —
deduped; no single qualifier covers the set) plus one `involves:` per
tracked person. The review-requested gap is accepted for tracked people —
their pending queue is not the operator's demand — and monitoring them is
an archive query, never an attention bucket; attention stays a demand
surface. Project-scope repos run one unqualified PR search and one issue
search per window: broader results, fewer queries, people subsumed. Never
page a repo's pullRequests connection — `updated:`-windowed search is what
bounds a busy monorepo's cold start by the lookback.

Project scope defaults defensively, because a busy repo's stream is mostly
machinery: bot-authored PRs (author type `Bot` — structural, never a login
pattern) are excluded unless `bots: true`; `exclude_authors` drops named
accounts through one login-equivalence function (`x[bot]` means login `x`
with author type `Bot`; GitHub returns bare logins for bots); per-repo
`lookback_days` makes a huge archive opt-in rather than the price of the
scope; and the issue stream is on by default at project scope only —
`issues: true` at working scope is a configuration error, never a silent
no-op. Discovery selects each hit's author, so an excluded PR is skipped
before hydration and costs discovery only. Filters govern ingest, never
deletion, and per-repo `filtered` counts appear in the sync summary —
configured absence is visible, like every other absence. Hydration also
records each author's `authorAssociation` (OWNER/MEMBER/CONTRIBUTOR/
FIRST_TIME_CONTRIBUTOR/…) and stable `databaseId` — both scalars on nodes
already fetched, so zero extra rate-limit points. The association is a
reliable external-vs-insider triage axis. The `databaseId` is stored now so
identity matching can move off logins later (PLANNED, milestone 1): today
`people`/`exclude_authors` match by login, so a login rename still breaks
those matches — the stable id is captured, not yet consulted. A "service
account" filter is deliberately absent: GitHub exposes no signal for it, so
`exclude_authors` (explicit) is the honest tool, never a login heuristic.

### Watermarks

Every discovered id resolves to exactly one outcome — hydrated, filtered,
quarantined, or deferred — and the per-repo, per-stream watermark is
max `updatedAt` over hydrated ∪ filtered. Filtered counts because filtered
is declined, not unfetched: a window whose newest activity is all bot PRs
must still advance, or re-discovery grows without bound. A deferred id caps
the watermark below it; a quarantined id is passed only with its durable
quarantine row in the same transaction — the advance is licensed by the
record that resurfaces the item. Discovery windows run oldest-first,
hydration ascends by `updatedAt`, and `Done` commits one completed window —
rows, quarantine, watermark, one transaction — so a watermark exists only
with a window-completion witness, a floor deferral banks every window it
finished, and a cold start larger than one run's budget converges across
runs. Windows are queried with a ~10-minute overlap, and upserts are
diff-gated: the field diff computed for `observations` also gates the row
UPDATE, so replaying an unchanged remote writes nothing and a killed run's
redo is a no-op. The diff is load-bearing twice; it is never refactored to
compute after the write.

### Refresh

Refresh is layered, so cost scales with what changed, not with archive
size. A connection's completeness witness is defined once — constructible
iff pagination of its live id set terminated; ids suffice, bodies are not
part of completeness — and `verified_at` is a witnessed write: set only in
a transaction holding witnesses for every connection, which also recomputes
`truncated = 0`. Within a touched PR, comments fetch tail-first under a
count-conservation check, and the tail applies only to the top-level
comments connection — never review threads, where a reply mutates an old
thread and the count balances while the archive is wrong. Threads are
skeleton-walked in full: cheap mutable fields every time, bodies only for
new or edited ids (GitHub prices GraphQL by node count, not field — the
skeleton saves bytes, the tail saves points, and a single-page PR costs
exactly one call). A tail fetch never constructs a witness, so it can never
sweep or stamp `verified_at`; the check's preconditions and its one masked
case are stated beside the check in src/sync.rs. Quiet mutations the
watermark cannot see (edits, resolves) fall to the tiered re-verify: open
PRs refetch completely on a short period regardless of lookback — OPEN is
the relevance signal, not recency, else a quiet open PR closed upstream
sits in `waiting_on_me` forever — and closed or merged PRs on a long period
within it. For read-time relevance, `sync --pr` hydrates one PR through the
ordinary pipeline: floor-exempt (the floor protects interactive use, and
`--pr` is interactive use), refused for repos outside the config — with
discovery skipped, this is the only enforcement of "discovery scope is the
config" — and for filter-excluded PRs, and it can never advance a
watermark, because it runs no discovery and so cannot construct the
witness. The notifications and timeline APIs stay out — a second discovery
loop, and the event system by the back door.

### Incompleteness

Incompleteness is never silent, at every layer. GitHub search caps near
1,000 results, so discovery checks completeness per search term —
pagination-exhausted as the primary signal, `nodes_seen == issueCount` as
the cap heuristic, counted before the filter branch, because client-side
filters would otherwise read incomplete forever on a bot-heavy repo — and
splits the window when short; an unsplittable window records
`discovery_truncated` and pins the stream watermark below the lost tail. A
PR whose hydration exhausts its retry budget is quarantined, retried under
backoff, surfaced in the summary; repeated `node: null` drains to
`prs.deleted_at`. `prs.truncated` is recomputed on every witnessed
hydration, so a complete refetch heals it. Sweeps are soft deletes, gated
on the completeness witness — a sweep on an incomplete connection is a type
error.

### Concurrency

K worker threads and one writer thread that owns the connection; std only —
the topology is literally MPSC. One scheduler function decides "hydrate
this now?" and orders all work: starved-first repos, discovery before
re-verify, re-verify capped and deterministically jittered and shed first
at the floor, quarantine backoff dominating every hydration cause (the
ordering rationale lives in src/sync.rs). One sync per archive: a run-level
OS file lock (`File::try_lock`), released by the OS on any death including
SIGKILL — never a run-long transaction, which would destroy per-window
commits. A second sync exits promptly with a typed "already running"
envelope. Every gh subprocess runs under a watchdog deadline; retry
attempts and budgets become config fields in milestone 2 — ghgraph owns
retry policy, not gh and not the operator. A configured floor bounds
rate-limit consumption: below it the run defers with a typed `Deferred`,
watermarks holding at the last completed window, so sync never drains the
point budget the operator's interactive gh use shares. Shutdown works in
both directions: workers ending drop the Sender; a writer error drops the
Receiver and workers treat send failure as cancellation. Cancellation
itself is the absence of a handler: SIGINT kills the process group, SQLite
rolls back, state never leads data.

## Storage

See src/schema.sql — conventions and rationale are comments there.
Load-bearing choices: `(repo, number)` is the PR business key and child
tables key on `prs.pk` — node ids are data everywhere, never join keys
(node-id formats have migrated before; a migration must strand nothing).
Explicit `pk INTEGER PRIMARY KEY` on every FTS content table (implicit
rowids desync FTS under VACUUM). Reviews are `comments.kind='review'` rows.
Soft deletes everywhere, and every `deleted_at` column has a defined writer.
No foreign keys: sync-order independence, and PrBundle writes a parent and
its children in one transaction, so orphans are unrepresentable in the write
path; a `stats` orphan audit is the backstop. Under project scope, issues are
first-class: the same shape as PRs (pk, truncated, deleted_at), labels and
assignees stored for triage, their comments in the comments table under a
typed parent, their text in FTS. An issue referenced from a working-scope PR
stays a skinny context cache. `observations` remains PR-fields only in both
scopes — that fence does not move with this one. Migrations only increase
`user_version`; an archive newer than the binary is a typed error, never a
silent write.

## Command surface

The verbs — sync, attention, prs, pr, search, query, stats — are the whole
surface, and each is one operation on the archive that maps 1:1 to a future
MCP tool. The mapping is the constraint, not the count: the CLI and the MCP
server are one surface, so there is one contract to specify, test, and reason
about rather than two that can diverge. A verb is an archive operation an
agent would call; the count is incidental, and an additional verb is
admissible when it meets that test. Anything else — human setup, config
scaffolding, tooling — belongs in a flag (the `--help`/`--version` family) or
the Makefile, not in a command that would have no MCP counterpart. stdout is
always one JSON document (carve-outs:
`--help`/`--version`, and abnormal exit, where empty stdout plus nonzero exit
reads as INTERNAL); progress goes to stderr; a closed pipe is a silent
exit 0, never a panic. Errors are typed envelopes, and the code names the
actor who can fix it: a typo in user SQL is USER_INPUT, a full disk is
CONFIGURATION with remedy text (the archive is a disposable cache — remove
and resync), INTERNAL means file a ghgraph bug and nothing else. Every read
carries in-band `_meta` freshness — numeric age, boolean stale, prose hint —
advisory only; reads never fail stale and never touch the network.
Freshness derives from `sync_state.last_checked_at`, never the watermark:
the watermark is discovery state and lags on quiet repos, so it cannot
express "checked five minutes ago, nothing new". `_meta.archive[]` also
discloses each repo's sync fingerprint (scope, filter digest, per-stream
freshness) plus a `config_pending` flag when the loaded config differs —
what produced the archive, never a live config echo, which would lie
immediately after any config change. Every emitted PR row carries its own
`verified_at` and `truncated`; repo-level age cannot bound per-PR staleness
under layered refresh, and TRANSIENT envelopes carry `retry_after` when gh
returned one. Output is deterministic: identical archive state yields
byte-identical documents modulo `_meta` timing.

`attention` exists because "waiting on me / they replied / ready to merge" is
the most error-prone derivation in the domain and must be encoded once, not
re-derived per consumer. Its polarity rule: uncertainty only escalates
attention. `ready_to_merge` is fail-closed — it requires complete data and an
approval newer than the last push, and when the ordering is unknown the PR
degrades out (a false "stale" costs a click; a false "ready" costs the tool's
credibility). `waiting_on_me` is fail-open — never suppressed by truncated
data, body-derived references, or minimized comments, only flagged.
Project scope adds maintainer buckets under the same rule: `needs_reviewer`
(open, not draft, no request and no review) and `untriaged` (open issue, no
labels, no assignee, no maintainer reply) are demands, so they fail open.
Buckets derive from archive state at read time: `people_prs` membership is
`author ∈ config.people` regardless of which scope or flavor ingested the
row, and maintainer buckets are emitted only for repos configured at
project scope when read — archive contents never create a bucket. Limits
(`--limit`, with disclosed totals, always) govern presentation; polarity
governs derivation — a limit is never precedent for suppressing a demand.
Derivations have one home (src/attention.rs); SQL views may encode structure,
never judgment.

## Config

The config file is the eighth verb — a public interface, mapped and
constrained like one (src/config.rs). A `repos` entry is a bare "owner/name"
(working scope, all defaults) or an object: `scope`, `issues`,
`lookback_days`, `bots`, `exclude_authors`. The shape is closed
(`deny_unknown_fields`), a malformed entry fails the whole config as
CONFIGURATION naming the entry and field — never skip-and-continue, and
never serde's opaque untagged-enum message — every identifier becomes a
validating newtype before it can reach a search qualifier (milestone 1),
and every default resolves in one place in code, so "what will this sync?"
always has one checkable answer. Identifiers are case-insensitive, as
GitHub treats them: repo names fold to lowercase at the config boundary
(and API-ingested repos fold to match), so `Foo/Bar` and `foo/bar` never
split the `(repo, number)` key or trip rename detection against the
canonical `nameWithOwner`.

Config changes over an existing archive are defined, not accidental. Each
`sync_state` row carries the structured fingerprint of the discovery inputs
that produced it (scope, viewer, people, filters, lookback). Equal →
incremental. A person added → targeted backfill of just the new `involves:`
flavor over the lookback. Any other relaxation — scope flip, filter
relaxed, lookback increased, viewer changed — cold-starts the stream,
because history the old inputs never discovered cannot be incrementally
recovered. Tightening changes nothing: filters govern ingest, never
deletion. A person removed keeps their rows (re-verify keeps open ones
honest; the cost decays with their open count) while `people_prs` empties
instantly, since it derives from current config.

## Security posture

Untrusted text is data everywhere. PR and comment bodies are third-party
content: bound parameters only, never concatenated into SQL, argv, error
text, or hints. Project scope widens the author pool from people the operator
engaged to everyone who shows up, which changes nothing: the posture is not
scope-conditional; derived fields come only from structural signals, and the one
body→structure path (src/refs.rs) can annotate but never suppress attention.
The config file is an interface: repos and logins become validating
newtypes (milestone 1), making search-qualifier injection unrepresentable
in `discovery_terms`' signature — until then the injection counterexamples
are recorded there and no new interpolation sites are added. PR references
(`owner/name#123`, URL, bare number via the cwd git remote) parse to a
validated (repo, number) pair before crossing any module boundary; URL
parsing pins the host, and the cwd remote is attacker-chosen content that
gets the same validation. No code path reads tokens; gh stderr is redacted
for token shapes before it lands in an envelope (the scrubber lands in
milestone 2, beside the classification table it protects). The archive
directory is created 0700 and the database 0600 at creation time — mode
bits at open, never a chmod after. Tracking `people` is an opt-in over
public GitHub data, stored locally under those modes; that is the privacy
posture, recorded here so it is a decision rather than an accident. `query`
cannot write because the connection is opened read-only at the file layer
and each invocation runs exactly one prepared statement — `query_only` is
defense-in-depth, not the boundary.

## Verification

Invariants land as mechanisms, not comments: a type that makes the violation
unrepresentable where possible, a named test where not. The load-bearing
suite: fixture replay (sync an unchanged remote twice, assert zero row and
zero FTS deltas), SIGKILL at random points (watermark never leads data),
two-process lock contention, a stalled-gh watchdog kill, golden files for all
seven verbs under `PRAGMA reverse_unordered_selects=ON`, and EXPLAIN QUERY
PLAN gates on the hot reads — stable in CI because bundled SQLite pins the
version to the lockfile. Cargo.lock is committed; cargo vet and cargo audit
run in CI. `stats` ships the same audits (orphans, observation chain, FTS
integrity, watermark assertion), so every operator is a CI runner.

Telemetry follows one rule: every measured field names the decision it
feeds or the regression it detects, and a field with no consumer is deleted
(the fields and their consumers live in src/sync.rs, beside the summary
they populate). Two homes, deliberately unequal: the sync summary carries
per-repo detail and is ephemeral; `sync_runs` persists one flat row per
run. Per-repo *state* with a scheduling consumer lives on `sync_state`;
per-repo *history* is telemetry and stays ephemeral. Read verbs carry no
runtime timing (latency is bench territory), there is no external sink
ever, and the determinism exemption for timing fields is an enumerated list
masked in golden tests. MCP spawn latency is the wrapper's observation, not
ghgraph's — self-timing could not answer that question.

## License

Apache-2.0.
