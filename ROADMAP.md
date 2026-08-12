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
  request; `retry_after` on TRANSIENT envelopes; `author_assoc` surfaced on
  every emitted PR/issue row (the triage axis is read-facing, and its
  absence from output cannot be added back after `schema_version: 1`).
  `author_id` stays internal — identity plumbing, not a display field.
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
  hydration. The planned server-side move (`-author:x`) is REJECTED on
  live probes (this milestone): GitHub search silently returns ZERO
  results — no error — when any negated author qualifier fails to
  resolve, which happens for a deleted or renamed account (the natural
  lifecycle of an excluded author) and for the `-author:app/x` spelling
  of any login without a matching GitHub App — and a bare pattern needs
  that spelling for parity with the client rule (bare matches either
  kind), so excluding any human zeroes the stream. A zeroed window is
  indistinguishable from a quiet repo: discovery starves silently, and
  the replay detector cannot see items that were never discovered.
  queries.rs records the mechanism at the term builder. Reversing
  evidence: search erroring (or excluding nothing) on unresolvable
  authors. The client-side skip already delivers the material half —
  filtered PRs never cost a hydration, and `filtered` counts stay exact.
  `bots` stays client-side forever — structural `Bot` has no search
  qualifier.
- Volume proof-out: cap splitting and quarantine were designed for the
  project-scope load; this milestone is where their tests meet a large
  active repo's real cold start.

Done when a project repo's 90-day cold start completes on a busy monorepo
with truncation and discovery-cap counts at zero, completes across runs
when interrupted at the floor without refetching, and an unchanged resync
writes nothing.

## 5 — hardening

Committed Cargo.lock; cargo vet, cargo audit, and a `cargo tree` diff in
CI; a fuzz harness
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

In order: contract-honesty fixes (1), freeze (3), then the external
per-call CLI-spawning wrapper as v0 — zero new dependencies, every CLI
invariant inherited. Built: `ghgraph-mcp` (src/bin/mcp.rs), the wrapper's
protocol decisions recorded in its module docs and the one-surface
invariant proven by test against the live CLI. The resident binary moved
to the deferred list below with its deciding evidence.

## Deferred, with the evidence that decides each

- The feature-gated resident MCP binary — the evidence so far says no.
  Measured (2026-08, Apple M4 Pro, 118 MB working archive): the
  spawn+open floor is ~7 ms, the wrapper adds nothing observable over the
  direct CLI, and the slow verbs are query work a resident would not
  reduce (search ~36 ms is FTS hydration, stats ~2.8 s is the audit
  sweep; SQLite answers the raw queries in under 1 ms either way). Reopen
  only if a real session shows the spawn floor dominating — order tens of
  calls per second sustained. Two preconditions if it ever lands: it must
  not hold a read connection across a sync (a long-lived reader is
  exactly the WAL-snapshot holder db.rs's truncate-at-close names as its
  defeat), and it needs a crash-containment answer that does not reopen
  the catch_unwind rejection (mcp.rs records the process boundary as the
  only containment the crate's rules permit).
- MCP structuredContent/outputSchema on tool results — the documents are
  already structured JSON delivered as text, and a declared output schema
  is a second encoding of the `schema_version: 1` contract that can drift
  from it. Adopt only when a real target client measurably parses or
  routes better with it; the text block stays regardless, so adoption is
  purely additive.
- MCP progress notifications for sync — the spec's answer to client tool
  timeouts on long calls, but emitting them means parsing the child's
  stderr heartbeat, which is deliberately non-contract on both surfaces.
  Adopt only when a real client that honors progress-based timeout
  renewal is observed timing out a sync wanted over MCP; until then a
  cold sync belongs in an operator shell (mcp.rs records the posture).
- Batched `nodes(ids:)` hydration — from the overhead-intercept median over
  trailing `sync_runs` rows: batch only if spawn overhead dominates, and
  only after the quarantine exists, or the failure unit becomes the batch.
- Tail size `last: K` and the 50×30 nested thread-comment request (most of
  a hydration's point cost) — from the connection `totalCount` distribution
  once real syncs run.
- The ~10-minute watermark overlap at project-scope volume — the
  unchanged-remote replay check is the live detector of an insufficient
  overlap (a missed item resurfaces as a spurious delta).
- `--print-default-config` — a flag (like `--help`) that emits the example
  config to stdout, for a released binary whose users have no checkout. The
  `make config` target covers source checkouts today; a verb is ruled out,
  since config scaffolding has no MCP counterpart.
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
  rename does to `refs` integrity. One concrete failure the migration must
  fix, observed by review (milestone 4): after the operator follows the
  rename remedy ("update the config entry"), the old rows keep the old repo
  key under the same node ids, so the first upsert of a renamed item hits
  the `UNIQUE(id)` constraint — classified INTERNAL (a lie; the actor is
  the operator) and aborting every run until the archive is rebuilt. The
  disposable-cache remedy covers it today; the migration retires it.
