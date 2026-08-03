//! Archive open + migrate.
//!
//! Two connection kinds, two types. `open_rw` returns [`RwArchive`]; `open_ro`
//! returns [`RoArchive`]. They are distinct types so a read verb cannot be
//! handed a writable connection, or a writer a read-only one — that mixup is a
//! compile error. That is the *only* guarantee the types give. Write-immunity
//! on the read path is entirely runtime: SQLITE_OPEN_READ_ONLY *plus* PRAGMA
//! query_only=ON (belt and suspenders — the pragma also blocks ATTACH-based
//! writes). `RoArchive::conn` returns `&Connection`, whose `execute`/
//! `execute_batch` are still callable and simply fail at runtime; the type does
//! not withhold them. Do not mistake the wrapper for a compile-time write
//! guard.
//!
//! Write connection (exactly one, owned by the sync writer thread):
//! journal_mode=WAL, synchronous=NORMAL, busy_timeout=5000. WAL matters even
//! single-writer: read commands stay usable mid-sync. journal_mode is set
//! BEFORE the migration transaction — a WAL switch cannot happen inside one.
//!
//! Modes at creation, never a chmod-after: directories WE create are born 0700
//! (DirBuilderExt::mode), the db file 0600 (OpenOptionsExt::mode with
//! create_new). create_new is O_CREAT|O_EXCL, which fails on an existing
//! symlink, so first creation cannot be redirected through one. The reopen path
//! does NOT use SQLITE_OPEN_NOFOLLOW: that flag refuses a database whose path
//! merely *traverses* a symlinked directory (macOS's /var is one, so is many an
//! operator's data dir), turning a legitimate archive into a false refusal. The
//! symlink-swap threat on reopen is closed by the 0700 archive directory
//! instead — planting a symlink inside it requires write access an attacker only
//! has if they already own the operator. That same 0700 directory is the
//! confidentiality boundary for SQLite's -wal/-shm sidecars, created at the
//! process umask (we cannot set their mode at their creation without a libc dep
//! we do not take).
//!
//! Two preconditions of the mode guarantee, stated rather than papered over:
//!   * It covers only directories ghgraph creates. A pre-existing archive
//!     directory (an operator's custom `db_path` pointed at a shared or
//!     world-readable dir) keeps its own mode; the -wal/-shm confidentiality
//!     boundary is then absent. Refusing a group/other-writable parent is
//!     PLANNED (milestone 5, hardening) — the enforcement is a mechanism, and
//!     until it exists this is a documented gap, not a silent one.
//!   * mode() is masked by the process umask, so 0700/0600 is a ceiling, not a
//!     floor. umask can only tighten, never loosen, so confidentiality is never
//!     regressed; an exotic umask that clears owner bits could make the archive
//!     unwritable, which surfaces as a CONFIGURATION open error.
//!
//! Migrations: PRAGMA user_version. 0 → apply schema.sql (always the CURRENT
//! shape) → SCHEMA_VERSION, schema apply and the version bump in ONE
//! rusqlite-managed transaction, so a crash mid-apply rolls back to 0 and the
//! next open retries from clean — the archive is never half-migrated. Every
//! user_version value has a defined outcome (see `migrate`); a value we do
//! not understand is refused, never guessed. Older versions step forward
//! through numbered fn(&mut Connection) migrations (v1→v2 is the first),
//! each bumping the pragma inside its own transaction. No schema_version
//! table; the pragma is the record.

use std::fs::{DirBuilder, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, ErrorCode, OpenFlags};

use crate::error::{Error, Result};

// Not pub: the only legitimate way to apply the schema is through open_rw's
// migration, which also sets WAL, the file mode, and the version stamp. A
// public SCHEMA would let a caller execute_batch it onto a connection that
// skipped all of that.
const SCHEMA: &str = include_str!("schema.sql");

/// Current schema version, written to PRAGMA user_version after migration.
/// This is the ARCHIVE version (a storage fact), not the output contract's
/// `_meta.schema_version` (report.rs) — the archive can migrate without the
/// output contract moving, which is exactly what v2 did (an added column
/// feeds a derivation; no emitted field changed shape).
///
/// v2: prs.head_committed_at — the stale-side approval-staleness bound. The
/// v1 schema's own comments claimed staleness "derives from committedDate",
/// but v1 never stored it: parse.rs validated the field and the upsert
/// dropped it. Migrated v1 rows hold NULL (freshness reads Unknown, which
/// fails closed) and heal on their next hydration, since the column joins
/// the diff-gated upsert.
pub const SCHEMA_VERSION: i64 = 2;

const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

/// A read-write archive connection. Exactly one exists per sync run, owned by
/// the writer thread (see sync.rs). Distinct from [`RoArchive`] by type.
pub struct RwArchive(Connection);

impl RwArchive {
    /// The underlying connection, for the writer's prepared statements and
    /// transactions.
    pub fn conn(&self) -> &Connection {
        &self.0
    }

    /// Mutable access, for `Connection::transaction`.
    pub fn conn_mut(&mut self) -> &mut Connection {
        &mut self.0
    }

    /// Consume the wrapper for code that needs to own the `Connection`.
    pub fn into_inner(self) -> Connection {
        self.0
    }
}

/// A read-only archive connection: SQLITE_OPEN_READ_ONLY + PRAGMA query_only.
/// Distinct from [`RwArchive`] by type. The write-immunity is the runtime pair,
/// not this wrapper (see module docs).
pub struct RoArchive(Connection);

impl RoArchive {
    /// The underlying connection, for the reader's prepared `SELECT`s. Exposes
    /// the full `&Connection` API; writes through it fail at runtime.
    pub fn conn(&self) -> &Connection {
        &self.0
    }
}

/// Open (creating if absent) the read-write archive and migrate it to
/// [`SCHEMA_VERSION`].
///
/// Errors are classified at the call site — there is no blanket `From` (see
/// error.rs). A busy/locked archive is TRANSIENT (retry); a bad path, a full or
/// read-only filesystem, a corrupt or foreign archive are CONFIGURATION.
pub fn open_rw(path: &Path) -> Result<RwArchive> {
    if let Some(parent) = path.parent() {
        ensure_dir_0700(parent)?;
    }
    create_0600_if_absent(path)?;

    // create_0600_if_absent already birthed the file at 0600, so in the normal
    // path SQLITE_OPEN_CREATE never sets a mode. It is kept only to survive the
    // vanishing-file race between our create and this open — and in that race
    // branch O_CREAT recreates the file at umask-default (not 0600). That narrow
    // gap is undefended (PLANNED milestone 5: re-verify the fd's mode after
    // open), but reaching it requires already controlling the 0700 archive
    // directory. No NOFOLLOW — it false-refuses archives under symlinked parent
    // dirs; the 0700 directory is the symlink-swap defense (module docs).
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let mut conn = Connection::open_with_flags(path, flags)
        .map_err(|e| sqlite_err(path, "cannot open archive", e))?;
    configure_conn(&conn, path)?;
    set_wal(&conn, path)?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| sqlite_err(path, "cannot set synchronous on", e))?;
    migrate(&mut conn, path)?;
    Ok(RwArchive(conn))
}

/// Open the archive read-only. The archive must already exist and be at exactly
/// [`SCHEMA_VERSION`]: a missing archive means "run sync first", a foreign or
/// half-initialized one means "this is not a ghgraph archive I understand" —
/// both CONFIGURATION, and neither is answered against.
pub fn open_ro(path: &Path) -> Result<RoArchive> {
    // try_exists distinguishes "not there" (run sync) from an access error
    // (e.g. the parent lost its 0700 traverse bit) — the latter must not be
    // laundered into the friendly "run sync first" message.
    match path.try_exists() {
        Ok(true) => {}
        Ok(false) => {
            return Err(Error::config(format!(
                "no archive at {} — run `ghgraph sync` first",
                path.display()
            )));
        }
        Err(e) => {
            return Err(Error::config(format!(
                "cannot access archive {}: {e}",
                path.display()
            )));
        }
    }
    // No NOFOLLOW (see module docs); the 0700 dir is the symlink defense.
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags)
        .map_err(|e| sqlite_err(path, "cannot open archive", e))?;
    configure_conn(&conn, path)?;
    // query_only blocks writes the READ_ONLY flag alone would miss (ATTACH).
    conn.pragma_update(None, "query_only", true)
        .map_err(|e| sqlite_err(path, "cannot set query_only on", e))?;
    // The determinism harness (golden tests, DESIGN.md Verification): with
    // this env var set, SQLite reverses the row order of any SELECT that
    // lacks a total ORDER BY, so a missing ORDER BY fails the golden diff
    // instead of passing by physical-row-order luck. Live on the shipped
    // path on purpose — the hook must exercise the very connection the read
    // verbs use, and for contract-correct output the pragma is a no-op (it
    // can reorder only what the contract never promised an order for), so
    // an operator setting it can confuse only themselves, not the archive.
    if std::env::var_os("GHGRAPH_TEST_REVERSE_SELECTS").is_some_and(|v| v == "1") {
        conn.pragma_update(None, "reverse_unordered_selects", true)
            .map_err(|e| sqlite_err(path, "cannot set reverse_unordered_selects on", e))?;
    }
    let version = user_version(&conn, path)?;
    if version != SCHEMA_VERSION {
        return Err(wrong_version(path, version));
    }
    Ok(RoArchive(conn))
}

/// Create the parent chain, with any directory WE create born 0700. A
/// pre-existing directory is left as-is — its mode is the operator's (see the
/// precondition in the module docs; enforcing 0700 on a pre-existing parent is
/// PLANNED milestone 5).
fn ensure_dir_0700(dir: &Path) -> Result<()> {
    if dir.as_os_str().is_empty() {
        return Ok(());
    }
    // try_exists (not exists()) so an access error surfaces with an accurate
    // message rather than falling through to a create that fails vaguer.
    match dir.try_exists() {
        Ok(true) => return Ok(()),
        Ok(false) => {}
        Err(e) => {
            return Err(Error::config(format!(
                "cannot access archive dir {}: {e}",
                dir.display()
            )));
        }
    }
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| Error::config(format!("cannot create archive dir {}: {e}", dir.display())))
}

/// Birth the db file at 0600 if it does not exist. create_new is O_CREAT|O_EXCL:
/// it fails on an existing regular file OR symlink, so it never follows a symlink
/// and never truncates existing data. An AlreadyExists is the ordinary reopen
/// case — we leave the file alone; on reopen the symlink-swap defense is the
/// 0700 archive directory, NOT NOFOLLOW (which is deliberately not set; see
/// module docs).
fn create_0600_if_absent(path: &Path) -> Result<()> {
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(_file) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(Error::config(format!(
            "cannot create archive {}: {e}",
            path.display()
        ))),
    }
}

/// The busy_timeout every connection gets, RW and RO alike (shared so a timeout
/// policy change is one edit). Failure is a configuration problem, not INTERNAL.
/// rusqlite already defaults this to 5000ms, so setting it here is explicit
/// intent, version-independent of that default — which also makes dropping the
/// call a behaviorally-equivalent mutation the tests cannot (and need not) kill.
fn configure_conn(conn: &Connection, path: &Path) -> Result<()> {
    conn.busy_timeout(BUSY_TIMEOUT)
        .map_err(|e| sqlite_err(path, "cannot configure archive", e))
}

/// Set WAL and verify it took. A WAL switch cannot happen inside a transaction,
/// so this runs before `migrate`. SQLite answers the pragma with the mode it
/// actually adopted; anything but "wal" is a configuration problem (e.g. the
/// filesystem cannot support the shared-memory WAL index), surfaced, never
/// silently accepted as a rollback-journal archive.
fn set_wal(conn: &Connection, path: &Path) -> Result<()> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .map_err(|e| sqlite_err(path, "cannot set WAL on", e))?;
    if mode.eq_ignore_ascii_case("wal") {
        Ok(())
    } else {
        Err(Error::config(format!(
            "archive {} would not enter WAL mode (got {mode:?}); \
             the filesystem may not support it",
            path.display()
        )))
    }
}

fn user_version(conn: &Connection, path: &Path) -> Result<i64> {
    conn.query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| sqlite_err(path, "cannot read schema version of", e))
}

/// Bring the archive to [`SCHEMA_VERSION`]. Each arm is a defined outcome; an
/// unrecognized version is refused, not guessed.
fn migrate(conn: &mut Connection, path: &Path) -> Result<()> {
    match user_version(conn, path)? {
        0 => apply_full(conn, path),
        1 => migrate_v1_to_v2(conn, path),
        v if v == SCHEMA_VERSION => Ok(()),
        // Everything else (a newer archive, or a negative/foreign sentinel —
        // SQLite accepts any i64 user_version) is refused, never guessed.
        v => Err(wrong_version(path, v)),
    }
}

/// v1 → v2: add prs.head_committed_at (rationale at [`SCHEMA_VERSION`]).
/// ALTER TABLE ADD COLUMN appends, and schema.sql declares the column last
/// for exactly that reason: a migrated archive and a fresh one must agree on
/// column order, or `query` SELECT * output forks by archive provenance.
/// Step and stamp share one transaction, like every migration here: a crash
/// between them re-runs the step from v1 cleanly.
fn migrate_v1_to_v2(conn: &mut Connection, path: &Path) -> Result<()> {
    let cannot = |e: rusqlite::Error| sqlite_err(path, "cannot migrate archive", e);
    let tx = conn.transaction().map_err(cannot)?;
    tx.execute_batch("ALTER TABLE prs ADD COLUMN head_committed_at TEXT")
        .map_err(cannot)?;
    tx.pragma_update(None, "user_version", 2).map_err(cannot)?;
    tx.commit().map_err(cannot)?;
    Ok(())
}

/// The CONFIGURATION error for an archive whose `user_version` is not the
/// current [`SCHEMA_VERSION`] and cannot be migrated to it. Shared by `open_ro`
/// (any non-current version) and `migrate` (its refusal arms) so the two never
/// drift. `migrate` handles v == 0 by applying the schema, so the v == 0 message
/// here is reached only from `open_ro`.
fn wrong_version(path: &Path, v: i64) -> Error {
    let detail = if v == 0 {
        "empty or not a ghgraph archive — run `ghgraph sync` first".to_string()
    } else if v < 0 {
        // SQLite stores any i64; a negative user_version is not a version this
        // (or any) ghgraph ever wrote — a corrupt or foreign sentinel. Say so,
        // rather than "no migration path", which implies a real intermediate
        // version. This arm must come before the > / catch-all below.
        "a negative sentinel — the archive is corrupt or not a ghgraph archive".to_string()
    } else if v > SCHEMA_VERSION {
        format!("newer than this ghgraph (v{SCHEMA_VERSION}); upgrade ghgraph")
    } else {
        // Only 0 < v < SCHEMA_VERSION reaches here, and only from open_ro:
        // migrate handles every such version, but a read-only connection
        // cannot run it. The remedy is the writer, which migrates on open.
        format!("older than this ghgraph (v{SCHEMA_VERSION}); run `ghgraph sync` to migrate it")
    };
    Error::config(format!(
        "archive {} is at schema version {v}: {detail}",
        path.display()
    ))
}

/// Apply the full current schema and stamp user_version=[`SCHEMA_VERSION`]
/// atomically (schema.sql always describes the CURRENT shape; migrations exist
/// for archives born under older ones). The schema apply and the version bump
/// run inside ONE rusqlite-managed transaction (schema.sql carries no
/// BEGIN/COMMIT of its own; the only BEGINs there are trigger bodies), and
/// PRAGMA user_version is transactional — so a crash between the last CREATE
/// and the stamp rolls back to user_version=0 and the next open retries from
/// clean.
fn apply_full(conn: &mut Connection, path: &Path) -> Result<()> {
    let cannot = |e: rusqlite::Error| sqlite_err(path, "cannot initialize archive", e);
    let tx = conn.transaction().map_err(cannot)?;
    tx.execute_batch(SCHEMA).map_err(cannot)?;
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(cannot)?;
    tx.commit().map_err(cannot)?;
    Ok(())
}

/// Classify a rusqlite failure by the actor who can fix it, at every call site
/// — open, a pragma, or the migration. A busy or locked database is TRANSIENT
/// (retry), whichever operation hit it; this one classifier is why the
/// "busy is TRANSIENT" promise in `open_rw` holds on every path, not just open.
/// Everything else is operator-fixable configuration: a corrupt archive is
/// removable-and-rebuildable, a full/read-only filesystem or a permission
/// problem is a path the operator controls. `ctx` is the leading clause of the
/// CONFIGURATION message, e.g. "cannot set WAL on".
fn sqlite_err(path: &Path, ctx: &str, e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(err, _) = &e
        && matches!(
            err.code,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
        )
    {
        return Error::transient(format!("archive {} is busy: {e}", path.display()));
    }
    Error::config(format!("{ctx} {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A unique scratch dir that removes itself on drop — panic-safe, so a
    /// failing assertion does not leak into temp_dir(). No tempfile crate (the
    /// four-dep floor); pid + a counter is unique across parallel test binaries
    /// (distinct pids) and reruns.
    struct Scratch {
        dir: PathBuf,
    }

    impl Scratch {
        fn new() -> Scratch {
            static N: AtomicU32 = AtomicU32::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("ghgraph-db-test-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            Scratch { dir }
        }

        fn join(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn set_raw_user_version(path: &Path, v: i64) {
        let conn = Connection::open(path).unwrap();
        conn.pragma_update(None, "user_version", v).unwrap();
    }

    #[test]
    fn open_rw_creates_schema_and_stamps_version() {
        let s = Scratch::new();
        let path = s.join("nested/ghgraph.db");
        let arc = open_rw(&path).unwrap();
        assert_eq!(user_version(arc.conn(), &path).unwrap(), SCHEMA_VERSION);
        // A representative table, an FTS virtual table (proving fts5 vtable
        // creation succeeds inside the migration transaction), and a trigger.
        for (kind, name) in [
            ("table", "prs"),
            ("table", "prs_fts"),
            ("trigger", "prs_ai"),
        ] {
            let count: i64 = arc
                .conn()
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type=?1 AND name=?2",
                    (kind, name),
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "{kind} {name} should exist");
        }
    }

    #[test]
    fn modes_are_0700_dir_0600_file_at_creation() {
        let s = Scratch::new();
        let path = s.join("sub/ghgraph.db");
        let _arc = open_rw(&path).unwrap();
        let file_mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600, "db file should be 0600");
        let dir_mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "archive dir should be 0700");
        // Note: this reads back the final bits under the ambient umask; since
        // umask only clears bits, an unset .mode() call could still pass here
        // under a benign umask. The explicit-mode guarantee is argued in the
        // module docs; a umask-injecting harness is PLANNED (milestone 5).
    }

    #[test]
    fn reopen_is_idempotent_and_preserves_data() {
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let arc = open_rw(&path).unwrap();
            // A schema-independent durability probe: a write here must survive
            // a close and reopen (and proves the migration did not re-run
            // destructively).
            arc.conn()
                .execute_batch("CREATE TABLE _probe(x); INSERT INTO _probe VALUES (42)")
                .unwrap();
        }
        let b = open_rw(&path).unwrap();
        assert_eq!(user_version(b.conn(), &path).unwrap(), SCHEMA_VERSION);
        let x: i64 = b
            .conn()
            .query_row("SELECT x FROM _probe", [], |r| r.get(0))
            .unwrap();
        assert_eq!(x, 42, "data must survive reopen");
    }

    #[test]
    fn open_rw_enters_wal() {
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        let arc = open_rw(&path).unwrap();
        let mode: String = arc
            .conn()
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert!(
            mode.eq_ignore_ascii_case("wal"),
            "expected wal, got {mode:?}"
        );
    }

    #[test]
    fn open_rw_sets_synchronous_normal() {
        // The default is FULL (2); open_rw sets NORMAL (1), the right pairing
        // with WAL. Without this, dropping the pragma is an undetected change.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        let arc = open_rw(&path).unwrap();
        let sync: i64 = arc
            .conn()
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            sync, 1,
            "synchronous must be NORMAL (1), not the FULL default"
        );
    }

    #[test]
    fn open_rw_sets_busy_timeout() {
        // configure_conn sets 5000ms; the default is 0. Without this, skipping
        // configure_conn (or its being replaced with a no-op) goes undetected.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        let arc = open_rw(&path).unwrap();
        let ms: i64 = arc
            .conn()
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ms, 5000, "busy_timeout must be 5000ms");
    }

    #[test]
    fn ro_rejects_writes_with_readonly_code() {
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        let ro = open_ro(&path).unwrap();
        // The write must be refused specifically for read-only reasons — not
        // for a parse error or a lock — or the test passes for the wrong reason.
        let err = ro.conn().execute_batch("CREATE TABLE t(x)").unwrap_err();
        match err {
            rusqlite::Error::SqliteFailure(e, _) => {
                assert_eq!(e.code, ErrorCode::ReadOnly, "expected read-only rejection")
            }
            other => panic!("expected SqliteFailure(ReadOnly), got {other:?}"),
        }
        // An ATTACH-based write is what query_only (not the READ_ONLY flag)
        // closes; it too must be refused, and specifically as read-only — a bare
        // is_err() would pass even if the statement failed for some other reason.
        let attach_err = ro
            .conn()
            .execute_batch("ATTACH DATABASE ':memory:' AS side; CREATE TABLE side.t(x)")
            .unwrap_err();
        match attach_err {
            rusqlite::Error::SqliteFailure(e, _) => assert_eq!(
                e.code,
                ErrorCode::ReadOnly,
                "ATTACH write must be refused as read-only"
            ),
            other => panic!("expected SqliteFailure(ReadOnly) on ATTACH path, got {other:?}"),
        }
        // Reads still work.
        let n: i64 = ro
            .conn()
            .query_row("SELECT count(*) FROM prs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn ro_refuses_missing_archive() {
        let s = Scratch::new();
        let path = s.join("does-not-exist.db");
        let err = open_ro(&path).err().expect("missing archive must error");
        assert_eq!(err.code, crate::error::Code::Configuration);
    }

    #[test]
    fn ro_refuses_foreign_version_zero_db() {
        // A valid SQLite file that is not a ghgraph archive (user_version 0).
        let s = Scratch::new();
        std::fs::create_dir_all(&s.dir).unwrap();
        let path = s.join("foreign.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE unrelated(x)").unwrap();
        }
        let err = open_ro(&path)
            .err()
            .expect("a version-0 foreign db must be refused");
        assert_eq!(err.code, crate::error::Code::Configuration);
    }

    /// Reconstruct a v1 archive from a v2 one: drop the one column v2 added
    /// and reset the stamp. Dropping the LAST column preserves the order of
    /// the rest, so the reconstruction is structurally faithful — that
    /// last-position choice is load-bearing (schema.sql, head_committed_at)
    /// and this helper depends on it like the migration does.
    fn make_v1(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch("ALTER TABLE prs DROP COLUMN head_committed_at")
            .unwrap();
        conn.pragma_update(None, "user_version", 1).unwrap();
    }

    #[test]
    fn migrates_v1_to_v2_and_matches_fresh_column_order() {
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        make_v1(&path);
        // A row born under v1 must survive the migration with NULL in the new
        // column (Unknown freshness, failing closed — attention.rs).
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "INSERT INTO prs (id, repo, number, title, state, created_at, updated_at, url) \
                 VALUES ('n1', 'o/r', 1, 't', 'OPEN', '2026-01-01T00:00:00Z', \
                         '2026-01-01T00:00:00Z', 'https://github.com/o/r/pull/1')",
            )
            .unwrap();
        }
        let migrated = open_rw(&path).unwrap();
        assert_eq!(user_version(migrated.conn(), &path).unwrap(), 2);
        let committed: Option<String> = migrated
            .conn()
            .query_row(
                "SELECT head_committed_at FROM prs WHERE number=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            committed, None,
            "v1 rows migrate to NULL (Unknown, fails closed)"
        );

        // Column ORDER must agree with a fresh v2 archive, or `query`
        // SELECT * output would fork by archive provenance.
        let fresh_path = s.join("fresh.db");
        let fresh = open_rw(&fresh_path).unwrap();
        let cols = |conn: &Connection| -> Vec<String> {
            let mut stmt = conn.prepare("PRAGMA table_info(prs)").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(
            cols(migrated.conn()),
            cols(fresh.conn()),
            "migrated and fresh archives must agree on prs column order"
        );
    }

    #[test]
    fn ro_refuses_v1_with_migrate_remedy() {
        // open_ro cannot migrate; it must name the writer as the remedy.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        make_v1(&path);
        let err = open_ro(&path).err().expect("open_ro must refuse v1");
        assert_eq!(err.code, crate::error::Code::Configuration);
        assert!(
            err.message.contains("ghgraph sync"),
            "message must direct the operator to sync (which migrates), got: {}",
            err.message
        );
    }

    #[test]
    fn refuses_newer_schema_version() {
        // An archive written by a hypothetical newer ghgraph (user_version > 1).
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        set_raw_user_version(&path, SCHEMA_VERSION + 1);
        // Both paths must refuse as CONFIGURATION, and the open_rw message must
        // carry the actionable remedy — pin it so a refactor can't drop it.
        let rw = open_rw(&path).err().expect("open_rw must refuse newer");
        assert_eq!(rw.code, crate::error::Code::Configuration);
        assert!(
            rw.message.contains("upgrade ghgraph"),
            "message must direct the operator to upgrade, got: {}",
            rw.message
        );
        let ro = open_ro(&path).err().expect("open_ro must refuse newer");
        assert_eq!(ro.code, crate::error::Code::Configuration);
        assert!(
            ro.message.contains("upgrade ghgraph"),
            "open_ro message must direct the operator to upgrade, got: {}",
            ro.message
        );
    }

    #[test]
    fn refuses_negative_schema_version() {
        // SQLite accepts any i64 user_version; a corrupt or foreign archive can
        // carry a negative sentinel that clears the 0 / ==VERSION / >VERSION
        // guards and hits migrate's catch-all. Both opens must refuse it.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        set_raw_user_version(&path, -1);
        // Refuse as CONFIGURATION on both paths, AND the message must flag the
        // archive as corrupt/foreign — not "no migration path", which would
        // imply a real intermediate version. Pin the wording so a refactor of
        // wrong_version cannot silently fold negatives back into the else arm
        // (mirrors refuses_newer_schema_version pinning "upgrade ghgraph").
        let rw = open_rw(&path).err().expect("open_rw must refuse negative");
        assert_eq!(rw.code, crate::error::Code::Configuration);
        assert!(
            rw.message.contains("corrupt"),
            "negative-version message must flag corruption, got: {}",
            rw.message
        );
        let ro = open_ro(&path).err().expect("open_ro must refuse negative");
        assert_eq!(ro.code, crate::error::Code::Configuration);
        assert!(
            ro.message.contains("corrupt"),
            "open_ro negative-version message must flag corruption, got: {}",
            ro.message
        );
    }
}
