# ghgraph roadmap

The order is load-bearing: schema decisions are cheap before v1 data exists
and table rebuilds after; the output-contract freeze must follow the fixes
that change the contract; MCP follows the freeze.

## 1 — schema and identity, before any v1 data

The schema is final and lives in `src/schema.sql`: child tables keyed on the
parent's `pk` (node ids are data everywhere); comments carry a typed parent
(PR or issue) and `is_minimized`; issues share the PR shape (`pk`, `truncated`,
`deleted_at`, `verified_at`, `labels`, `assignees`, `hydration_source`, and
`issues_fts`); `prs.last_pushed_at` and `prs.verified_at`; `review_requests.kind`;
`sync_state` keyed `(repo, stream)` with the discovery fingerprint,
`last_checked_at`, and `runs_since_advance`; the `quarantine` table; and FTS
`WHEN` guards. Rationale is recorded at each definition. What remains for this
milestone is the identity code that validates and populates against it:

- Promote config's charset gate (config.rs `is_login`/`is_repo`) to validating
  `RepoName`/`Login`/`Rfc3339Utc` newtypes enforced by `discovery_terms`'
  signature, so injection is unrepresentable by type; the counterexample
  string lives on as a unit test.
- Login equivalence as one function, called everywhere logins compare, tested
  against a captured GraphQL bot-actor fixture; `x[bot]` means login `x` with
  author type `Bot`.
- A manual `Deserialize` for the `repos` entry (string or object) so a config
  typo names the failing entry and field instead of serde's opaque
  untagged-variant message; it rejects duplicate keys explicitly, or closure
  regresses while diagnosability improves.
- `__typename` on every author selection is in the queries; the parse types
  carry `author` as `Option` (a deleted account is data, never an error) and
  read structural Bot type for ingest filtering (`sync --pr` skips discovery).
- An `involves:` discovery flavor per tracked person; `refs.source` exposed on
  `blocked_edges` (in schema) consumed by the reader.
- Archive dir/db created 0700/0600 at creation time (mode bits at open, no
  chmod-after race) — pulled forward from hardening.

Done when the identity code validates against the final schema and the
injection, bot-equivalence, and config-diagnosability tests pass.

## 2 — sync pipeline bodies

The stubs become code. The machinery is specified in DESIGN.md's sync
section and src/sync.rs's module comment — this milestone implements it
rather than re-describing it here. Sequencing notes that live in the
roadmap:

- PR sync only at this point; the issue stream is milestone 4, on machinery
  this milestone proves.
- Pulled forward from milestone 3: the sync-time viewer identity check
  (`gh api user` once per run) — a typo'd viewer syncs nothing, silently,
  in exactly the milestone people first run sync.
- The minimum-gh-version gate and the stderr token scrubber land beside the
  watchdog; the blanket `From` error impls are already deleted, so
  classification is call-site from the first body written, and the
  ghost-author fixture proves it on the gh path.
- A stderr progress heartbeat (current repo, window N of M, points
  remaining) so operators don't kill healthy multi-hour first runs; stderr
  stays non-contract.

Done when the load-bearing suite passes: fixture replay with zero deltas
(including a metadata-only-flip case proving the FTS guards), SIGKILL at
random points, two-process lock contention, stalled-gh watchdog, the
floor-injection deferral fixture (two deferred runs, monotone watermark, no
double hydration), and a config-transition test per fingerprint case
including person removal.

## 3 — read surface and contract freeze

- The freeze batch: `_meta` stale boolean and `schema_version`; freshness
  derived from `last_checked_at`, with the per-repo sync fingerprint and a
  `config_pending` flag disclosed (what produced the archive, never a live
  config echo); `verified_at` and `truncated` on every emitted PR row
  (repo-level age cannot bound per-PR staleness under layered refresh);
  `resolved: false` on dangling refs and minimized/deleted provenance on
  every body-carrying field; search results regrouped by PR (bm25 ranks
  from separate FTS indexes are not comparable); `body_elided` distinct
  from `truncated` — one is a property of the archive, the other of the
  request; `retry_after` on TRANSIENT envelopes.
- Stamp `schema_version: 1`, additive-only from then on — strictly after
  milestone 1, whose fixes change the contract.
- Opt-in `--max-body-bytes` elision. `--limit` on `prs`/`attention` with
  mandatory disclosed totals — limits govern presentation, polarity governs
  derivation, and that distinction is written in attention.rs so a limit is
  never precedent for suppressing a demand. `prs --author`, so the
  tracked-person workflow is a one-liner (a WHERE clause, not a monitor
  verb). `sync --pr` for read-time freshness: the `_meta` hint says when,
  the reader says which. `attention --fail-if-any` and `sync --strict`
  under the rule that gate flags change the exit code, never a byte of
  JSON.

Done when golden files exist for all seven verbs and hold under
`PRAGMA reverse_unordered_selects=ON`.

## 4 — project scope

The maintainer half of the tool, additive on top of the frozen contract:

- Issue discovery and hydration for project repos — one `is:issue` search
  per window, a lighter hydration document (no review threads), per-stream
  watermarks so a repo's PR and issue progress advance independently.
- Issues join FTS. The deferral condition ("a real query wants it") is met:
  "where did we discuss X" lands in issue bodies for any active project.
- Maintainer attention buckets, `needs_reviewer` and `untriaged` — demands,
  so fail-open, under the same polarity rule as the rest.
- The stream filters with their defensive defaults: bot-authored PRs out
  unless opted in, `exclude_authors`, per-repo lookback; filtered counts in
  the summary, and discovery-side skips so filtered PRs never cost a
  hydration. Once the newtypes exist, `exclude_authors` moves server-side
  (`-author:x`) with the tradeoff recorded where the term is built:
  `filtered` counts degrade to a lower bound, and "configured absence is
  visible" is served by the fingerprint disclosure instead. `bots` stays
  client-side forever — structural `Bot` has no search qualifier.
- Volume proof-out: cap splitting and quarantine were designed for the
  project-scope load; this milestone is where their tests meet a large
  active repo's real cold start.

Done when a project repo's 90-day cold start completes on a busy monorepo
with truncation and discovery-cap counts at zero, completes across runs
when interrupted at the floor without refetching, and an unchanged resync
writes nothing.

## 5 — hardening

Committed Cargo.lock; cargo vet, cargo audit, and a `cargo tree` diff in CI;
a written policy for SQLite CVEs arriving through rusqlite; a fuzz harness
for `refs::extract` outside the default build. Best-effort WAL truncate at
writer close. The `sync_runs` health row — one flat row per run, no child
tables, so trends are a `query` away without a telemetry store growing
underneath; its column list is written beside its named consumers (the
batching-overhead median, run-level tail ratios, floor counters) when
milestone 2 defines them, so this milestone implements rather than
reverse-engineers. `stats` gains the audits, plus per-repo
runs-since-advance and the oldest open-PR `verified_at`, so starvation and
staleness are visible, not inferred. EXPLAIN QUERY PLAN gates on the hot
reads.

## 6 — MCP

In order: contract-honesty fixes (1), freeze (3), then an external per-call
CLI-spawning wrapper as v0 — zero new dependencies, every CLI invariant
inherited. The feature-gated resident binary lands only on measured spawn
latency from real sessions, with the long-lived-reader/WAL-checkpoint
interaction as a named design input at that point.

## Deferred, with the evidence that decides each

- Batched `nodes(ids:)` hydration — from the overhead-intercept median over
  trailing `sync_runs` rows: batch only if spawn overhead dominates, and
  only after the quarantine exists, or the failure unit becomes the batch.
- Tail size `last: K` and the 50×30 nested thread-comment request (most of
  a hydration's point cost) — from the connection `totalCount` distribution
  once real syncs run.
- The ~10-minute watermark overlap at project-scope volume — the
  unchanged-remote replay check is the live detector of an insufficient
  overlap (a missed item resurfaces as a spurious delta).
- FTS tokenizer for identifiers — ship stock unicode61; decide from captured
  queries. A tokenizer change is a rebuild migration, not a data migration.
- Retry deadlines, budgets, quarantine threshold — ship conservative
  defaults; tune from `sync_runs` and summary data.
- Pinning tracked `people` to node ids (login rename/reuse defense) — the
  posture is recorded in DESIGN.md; it becomes an invariant only once
  "mismatch → what?" is defined by a captured rename fixture or the first
  real mismatch.
- URL host policy for PR references — pin github.com until a GHES operator
  materializes; the extension point is the reference parser.
- Repo-rename row migration — detection ships in milestone 2; whether to
  rewrite `prs.repo` on a confirmed rename waits for what the first real
  rename does to `refs` integrity.
