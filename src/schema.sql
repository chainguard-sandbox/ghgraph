-- ghgraph archive schema.
--
-- Conventions:
--   * Every FTS content table keys on `pk INTEGER PRIMARY KEY` — a true rowid
--     alias, stable under VACUUM. Implicit rowids are renumbered by VACUUM and
--     would silently desync the FTS indexes.
--   * GraphQL node ids are stored UNIQUE but are not the key: GitHub has
--     migrated node-id formats before, so (repo, number) is the business key.
--   * Upserts use INSERT ... ON CONFLICT, never INSERT OR REPLACE (which
--     deletes + reinserts and churns the rowid under the FTS index).
--   * Upstream deletions are soft (deleted_at): a deleted comment is signal
--     for a memory tool, not garbage. Deleted rows stay in FTS.
--   * Timestamps are RFC 3339 UTC "Z" strings; lexicographic order = time order.
--
-- No FOREIGN KEYs, decided: PrBundle writes a parent and its children in one
-- transaction, so orphans are unrepresentable in the write path, and ON
-- DELETE CASCADE would amplify hard deletes against the soft-delete audit
-- trail; a `stats` orphan audit is the backstop. Child tables re-key on
-- prs.pk before v1 data exists (ROADMAP milestone 1) so a node-id format
-- migration can strand nothing.

CREATE TABLE IF NOT EXISTS prs (
  pk              INTEGER PRIMARY KEY,
  id              TEXT NOT NULL UNIQUE,  -- GraphQL node id (data, not key)
  repo            TEXT NOT NULL,         -- owner/name
  number          INTEGER NOT NULL,
  title           TEXT NOT NULL,
  body            TEXT NOT NULL DEFAULT '',
  state           TEXT NOT NULL,         -- OPEN | CLOSED | MERGED
  is_draft        INTEGER NOT NULL DEFAULT 0,
  author          TEXT,
  head_ref        TEXT,
  base_ref        TEXT,
  head_sha        TEXT,                  -- last commit; reviews older than its
                                         -- push time are stale approvals
  review_decision TEXT,                  -- raw API value; never trusted alone
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  merged_at       TEXT,
  closed_at       TEXT,
  url             TEXT NOT NULL,
  truncated       INTEGER NOT NULL DEFAULT 0,  -- hydration incomplete; no silent caps
  deleted_at      TEXT,
  UNIQUE (repo, number)
);
CREATE INDEX IF NOT EXISTS idx_prs_repo_state ON prs (repo, state);
CREATE INDEX IF NOT EXISTS idx_prs_author_state ON prs (author, state);
CREATE INDEX IF NOT EXISTS idx_prs_updated ON prs (updated_at);

-- Skinny issue cache: populated for issues that synced PRs reference, so the
-- context behind a PR is available offline. Not a standalone sync loop.
CREATE TABLE IF NOT EXISTS issues (
  id         TEXT NOT NULL UNIQUE,
  repo       TEXT NOT NULL,
  number     INTEGER NOT NULL,
  title      TEXT NOT NULL,
  state      TEXT NOT NULL,
  body       TEXT NOT NULL DEFAULT '',
  author     TEXT,
  url        TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  synced_at  TEXT NOT NULL,              -- staleness is part of the record
  UNIQUE (repo, number)
);

CREATE TABLE IF NOT EXISTS review_threads (
  id          TEXT PRIMARY KEY,
  pr_id       TEXT NOT NULL,             -- prs.id
  path        TEXT,
  line        INTEGER,
  is_resolved INTEGER NOT NULL DEFAULT 0,
  is_outdated INTEGER NOT NULL DEFAULT 0,
  deleted_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_threads_pr ON review_threads (pr_id);

-- Reviews live here as kind='review' rows (state = APPROVED | CHANGES_REQUESTED
-- | COMMENTED | DISMISSED); no separate reviews table.
CREATE TABLE IF NOT EXISTS comments (
  pk         INTEGER PRIMARY KEY,
  id         TEXT NOT NULL UNIQUE,
  pr_id      TEXT NOT NULL,              -- prs.id
  thread_id  TEXT,                       -- review_threads.id; NULL otherwise
  kind       TEXT NOT NULL DEFAULT 'comment',  -- comment | review_comment | review
  state      TEXT,                       -- review verdict when kind='review'
  author     TEXT,
  body       TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT,                       -- edit detection; keeps the FTS copy honest
  url        TEXT,
  deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_comments_pr ON comments (pr_id);
CREATE INDEX IF NOT EXISTS idx_comments_thread ON comments (thread_id);

-- Current review requests, replaced wholesale per PR per sync. Role ("mine" vs
-- "reviewing") is derived at query time from author + this table — a stored
-- role column would go stale and mislabel author-and-reviewer PRs.
CREATE TABLE IF NOT EXISTS review_requests (
  pr_id    TEXT NOT NULL,
  reviewer TEXT NOT NULL,
  PRIMARY KEY (pr_id, reviewer)
);

-- Cross-references, recomputed per PR on every sync: a view over observed
-- text and API links, not a source of truth. target_repo is always the real
-- owner/name. Targets resolve lazily at query time (LEFT JOIN prs/issues);
-- a dangling target is signal, not an error. `blocks` is stored as observed,
-- never flipped — blocked_edges below canonicalizes direction.
CREATE TABLE IF NOT EXISTS refs (
  src           TEXT NOT NULL,           -- prs.id
  kind          TEXT NOT NULL,           -- fixes | depends_on | blocked_by | blocks | mentions
  source        TEXT NOT NULL,           -- body | api (closingIssuesReferences → fixes/api)
  target_repo   TEXT NOT NULL,
  target_number INTEGER NOT NULL,
  PRIMARY KEY (src, kind, source, target_repo, target_number)
);
CREATE INDEX IF NOT EXISTS idx_refs_target ON refs (target_repo, target_number);

CREATE VIEW IF NOT EXISTS blocked_edges AS
SELECT p.repo AS blocked_repo, p.number AS blocked_number,
       r.target_repo AS blocker_repo, r.target_number AS blocker_number
  FROM refs r JOIN prs p ON p.id = r.src
 WHERE r.kind = 'blocked_by'
UNION
SELECT r.target_repo, r.target_number, p.repo, p.number
  FROM refs r JOIN prs p ON p.id = r.src
 WHERE r.kind = 'blocks';

-- The sync's own changelog: the field-by-field diff the upsert already
-- computes, recorded. "What changed since yesterday" = WHERE observed_at > ?.
-- Not an event system; GitHub's timeline API is deliberately not fetched.
CREATE TABLE IF NOT EXISTS observations (
  seq         INTEGER PRIMARY KEY,
  pr_id       TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  field       TEXT NOT NULL,             -- state | review_decision | head_sha | is_draft | ...
  old         TEXT,
  new         TEXT
);
CREATE INDEX IF NOT EXISTS idx_observations_at ON observations (observed_at);

-- Watermarks are server-side time — max updatedAt over items hydrated OR
-- deliberately filtered (filtered is declined, not unfetched; a bot-only
-- window must still advance or re-discovery grows without bound) — never
-- the local clock; advanced only inside the transaction committing a
-- completed discovery window. GraphQL cursors are intra-run only and never
-- persisted. Keyed per (repo, stream): a project repo's PR and issue
-- watermarks advance independently. The fingerprint is the structured
-- discovery input (scope, viewer, people, bots, exclude_authors, lookback);
-- on load, equal → incremental, person added → targeted backfill of the new
-- involves: flavor over the lookback, any other relaxation → stream
-- cold-start, tightening → nothing (filters govern ingest, never deletion).
-- It is structured rather than hashed because those transitions need
-- field-level comparison. last_checked_at (local wall time, written in the
-- Done transaction) is the _meta freshness source — the watermark is
-- discovery state and lags on quiet repos, so it can never express "checked
-- five minutes ago, nothing new". runs_since_advance feeds starved-first
-- scheduling and the stats starvation line: per-repo state with a
-- scheduling consumer is state, not telemetry.
CREATE TABLE IF NOT EXISTS sync_state (
  repo                 TEXT NOT NULL,
  stream               TEXT NOT NULL,          -- 'pr' | 'issue'
  last_item_updated_at TEXT NOT NULL,
  last_checked_at      TEXT,
  runs_since_advance   INTEGER NOT NULL DEFAULT 0,
  fingerprint          TEXT NOT NULL,          -- JSON, structured on purpose
  PRIMARY KEY (repo, stream)
);

-- Hydration failures, recorded durably so the watermark may pass them: a
-- quarantine row commits in the same transaction as the watermark that
-- passes its id — the advance is licensed by the record that resurfaces the
-- item, so no exit can turn "quarantined" into "forgotten". Retried under
-- backoff by the scheduler (backoff dominates every hydration cause);
-- node:null after repeated attempts drains to prs.deleted_at.
CREATE TABLE IF NOT EXISTS quarantine (
  id            TEXT PRIMARY KEY,              -- GraphQL node id
  repo          TEXT NOT NULL,
  attempts      INTEGER NOT NULL DEFAULT 0,
  next_retry_at TEXT NOT NULL,
  error_class   TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS prs_fts USING fts5(
  title, body,
  content='prs', content_rowid='pk',
  tokenize='porter unicode61'
);
CREATE TRIGGER IF NOT EXISTS prs_ai AFTER INSERT ON prs BEGIN
  INSERT INTO prs_fts(rowid, title, body) VALUES (new.pk, new.title, new.body);
END;
CREATE TRIGGER IF NOT EXISTS prs_ad AFTER DELETE ON prs BEGIN
  INSERT INTO prs_fts(prs_fts, rowid, title, body)
    VALUES ('delete', old.pk, old.title, old.body);
END;
-- WHEN guard: without it, ANY column change — a state flip, an updated_at
-- bump, the is_minimized flip the skeleton walk exists to record — rewrites
-- the full tokenization, and at project scope the whole archive's FTS is
-- rewritten once per PR lifecycle. The diff gate cannot see this (it hides
-- only no-op updates), so the guard is the enforcement, not a comment.
CREATE TRIGGER IF NOT EXISTS prs_au AFTER UPDATE ON prs
WHEN old.title IS NOT new.title OR old.body IS NOT new.body
BEGIN
  INSERT INTO prs_fts(prs_fts, rowid, title, body)
    VALUES ('delete', old.pk, old.title, old.body);
  INSERT INTO prs_fts(rowid, title, body) VALUES (new.pk, new.title, new.body);
END;

CREATE VIRTUAL TABLE IF NOT EXISTS comments_fts USING fts5(
  body,
  content='comments', content_rowid='pk',
  tokenize='porter unicode61'
);
CREATE TRIGGER IF NOT EXISTS comments_ai AFTER INSERT ON comments BEGIN
  INSERT INTO comments_fts(rowid, body) VALUES (new.pk, new.body);
END;
CREATE TRIGGER IF NOT EXISTS comments_ad AFTER DELETE ON comments BEGIN
  INSERT INTO comments_fts(comments_fts, rowid, body)
    VALUES ('delete', old.pk, old.body);
END;
-- Same WHEN rationale as prs_au.
CREATE TRIGGER IF NOT EXISTS comments_au AFTER UPDATE ON comments
WHEN old.body IS NOT new.body
BEGIN
  INSERT INTO comments_fts(comments_fts, rowid, body)
    VALUES ('delete', old.pk, old.body);
  INSERT INTO comments_fts(rowid, body) VALUES (new.pk, new.body);
END;
