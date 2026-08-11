# CLAUDE.md

ghgraph: local GitHub work memory in SQLite — the operator's working set, or
a whole project's PR and issue stream, chosen per repo in config. The `gh`
CLI is the only transport. Unix only, enforced by a `compile_error!`.
Read DESIGN.md before changing anything; ROADMAP.md sequences the build.
Working through milestone 4; the output contract is frozen at
`schema_version: 1`, additive-only.

## The discipline

- An invariant is a mechanism, not a comment. Prefer, in order: a type that
  makes the violation unrepresentable, a transaction boundary, a named test.
  If you can only assert a property in prose, say so explicitly and open the
  question rather than papering over it.
- Record rationale where the code is: schema decisions as schema.sql
  comments, protocol decisions in the module that implements them. When you
  decide something, write down why and what evidence would reverse it.
  When you reject something, record the rejection — cuts that lose their
  context get re-proposed.
- DESIGN.md carries architecture and the arguments between modules; module
  comments carry mechanism and its rationale. Don't restate one in the
  other — add a pointer.
- Never assert an unbuilt mechanism in the present tense: mark it
  `PLANNED (milestone N)` — as db.rs does — and remove the marker in the
  change that builds it.
- Proofs have preconditions. "Watermark never leads data" holds only if
  every discovered id resolves to a defined outcome (hydrated, filtered,
  quarantined-with-a-durable-row, deferred) and the watermark folds over
  that enum; "query cannot write" holds only under
  one-prepared-statement-per-invocation. When you touch code near a stated
  invariant, find its preconditions before editing.

## Hard constraints

- Dependencies are exactly rusqlite, serde, serde_json, clap. No HTTP, TLS,
  async, or time crates; std does it (the RFC 3339 parser is ~40 lines, the
  run lock is `File::try_lock`). `unsafe` is forbidden crate-wide.
- stdout is one JSON document; progress and noise go to stderr. Error codes
  name the actor who can fix them: USER_INPUT, CONFIGURATION, TRANSIENT,
  INTERNAL — and INTERNAL means "file a ghgraph bug", nothing else. Don't
  let a blanket `From` impl launder a user's typo into INTERNAL.
- Untrusted text is data. PR and comment bodies never reach SQL text, argv,
  error messages, or derived judgments; bound parameters only.
- Incompleteness is never silent. If data might be partial — truncated
  hydration, capped discovery, an unresolvable ref — record it and disclose
  it in output. In `attention`, uncertainty only escalates: it can add to
  `waiting_on_me`, it can never qualify a PR as `ready_to_merge`.
- Output is deterministic: total ORDER BY with unique tiebreakers, sorted
  keys, no floats printed. Golden tests must survive
  `PRAGMA reverse_unordered_selects=ON`.

## Standing rejections

Signal handlers (cancellation is the absence of a handler), an event system
(observations stay PR-fields only), FKs with cascades, `catch_unwind`, a
panic hook, run-long transactions for locking, views that encode judgment,
org- or forge-wide discovery (every synced repo is named in the config), a
Windows port, a CLI command with no MCP counterpart (human setup or config
scaffolding is a flag or a Makefile target, not a verb), and a fifth
verification instrument without a property that needs it — proptest, Miri,
Loom, unbounded provers, a chrono oracle (per-instrument arguments in
DESIGN.md Verification; Kani is deferred there, not rejected). Each was
argued down in DESIGN.md; meet the argument, not the absence.
