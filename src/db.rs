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
//! Migrations: PRAGMA user_version. 0 → apply schema.sql → 1, schema apply and
//! the version bump in ONE rusqlite-managed transaction, so a crash mid-apply
//! rolls back to 0 and the next open retries from clean — the archive is never
//! half-migrated. Every user_version value has a defined outcome (see
//! `migrate`); a value we do not understand is refused, never guessed. Later
//! versions are numbered fn(&mut Connection) steps, each bumping the pragma
//! inside its own transaction. No schema_version table; the pragma is the record.

use std::fs::{DirBuilder, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, ErrorCode, OpenFlags};

use crate::error::{Error, Result};

pub const SCHEMA: &str = include_str!("schema.sql");

/// Current schema version, written to PRAGMA user_version after migration.
pub const SCHEMA_VERSION: i64 = 1;

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
    // gap is undefended, but reaching it requires already controlling the 0700
    // archive directory. No NOFOLLOW — it false-refuses archives under symlinked
    // parent dirs; the 0700 directory is the symlink-swap defense (module docs).
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let mut conn = Connection::open_with_flags(path, flags).map_err(|e| open_error(path, e))?;
    configure_conn(&conn, path)?;
    set_wal(&conn, path)?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| Error::config(format!("cannot set synchronous on {}: {e}", path.display())))?;
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
    let conn = Connection::open_with_flags(path, flags).map_err(|e| open_error(path, e))?;
    configure_conn(&conn, path)?;
    // query_only blocks writes the READ_ONLY flag alone would miss (ATTACH).
    conn.pragma_update(None, "query_only", true)
        .map_err(|e| Error::config(format!("cannot set query_only on {}: {e}", path.display())))?;
    let version = user_version(&conn, path)?;
    if version != SCHEMA_VERSION {
        return Err(Error::config(format!(
            "archive {} is at schema version {version}, expected {SCHEMA_VERSION} \
             (a version 0 archive is empty or foreign; a higher version was written \
             by a newer ghgraph)",
            path.display()
        )));
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
fn configure_conn(conn: &Connection, path: &Path) -> Result<()> {
    conn.busy_timeout(BUSY_TIMEOUT)
        .map_err(|e| Error::config(format!("cannot configure archive {}: {e}", path.display())))
}

/// Set WAL and verify it took. A WAL switch cannot happen inside a transaction,
/// so this runs before `migrate`. SQLite answers the pragma with the mode it
/// actually adopted; anything but "wal" is a configuration problem (e.g. the
/// filesystem cannot support the shared-memory WAL index), surfaced, never
/// silently accepted as a rollback-journal archive.
fn set_wal(conn: &Connection, path: &Path) -> Result<()> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .map_err(|e| Error::config(format!("cannot set WAL on {}: {e}", path.display())))?;
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
        .map_err(|e| {
            Error::config(format!(
                "cannot read schema version of {}: {e}",
                path.display()
            ))
        })
}

/// Bring the archive to [`SCHEMA_VERSION`]. Each arm is a defined outcome; an
/// unrecognized version is refused, not guessed.
fn migrate(conn: &mut Connection, path: &Path) -> Result<()> {
    let version = user_version(conn, path)?;
    match version {
        0 => apply_v1(conn, path),
        v if v == SCHEMA_VERSION => Ok(()),
        v if v > SCHEMA_VERSION => Err(Error::config(format!(
            "archive {} is schema version {v}, newer than this ghgraph (v{SCHEMA_VERSION}); \
             upgrade ghgraph",
            path.display()
        ))),
        // 0 < v < SCHEMA_VERSION: reachable only once numbered migration steps
        // exist. PLANNED (milestone: whenever SCHEMA_VERSION first exceeds 1) —
        // dispatch the ordered steps from v to SCHEMA_VERSION here, each in its
        // own transaction. Until then this arm is unreachable (the only value
        // below 1 is 0) and refusing is the safe default.
        v => Err(Error::config(format!(
            "archive {} is at schema version {v}, which this ghgraph has no migration path for",
            path.display()
        ))),
    }
}

/// Apply the v1 schema and stamp user_version=1 atomically. The schema apply
/// and the version bump run inside ONE rusqlite-managed transaction (schema.sql
/// carries no BEGIN/COMMIT of its own; the only BEGINs there are trigger
/// bodies), and PRAGMA user_version is transactional — so a crash between the
/// last CREATE and the stamp rolls back to user_version=0 and the next open
/// retries from clean.
fn apply_v1(conn: &mut Connection, path: &Path) -> Result<()> {
    let cannot = |e: rusqlite::Error| {
        Error::config(format!("cannot initialize archive {}: {e}", path.display()))
    };
    let tx = conn.transaction().map_err(cannot)?;
    tx.execute_batch(SCHEMA).map_err(cannot)?;
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(cannot)?;
    tx.commit().map_err(cannot)?;
    Ok(())
}

/// Classify a rusqlite open failure. A busy or locked archive is TRANSIENT —
/// the fix is to retry, not to change a path. Everything else here is
/// operator-fixable configuration: a corrupt archive is removable-and-
/// rebuildable, a symlink or permission problem is a path the operator controls.
fn open_error(path: &Path, e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(err, _) = &e
        && matches!(
            err.code,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
        )
    {
        return Error::transient(format!("archive {} is busy: {e}", path.display()));
    }
    Error::config(format!("cannot open archive {}: {e}", path.display()))
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

    #[test]
    fn refuses_newer_schema_version() {
        // An archive written by a hypothetical newer ghgraph (user_version > 1).
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        set_raw_user_version(&path, SCHEMA_VERSION + 1);
        // Both paths must refuse, as CONFIGURATION ("upgrade ghgraph").
        let rw = open_rw(&path).err().expect("open_rw must refuse newer");
        assert_eq!(rw.code, crate::error::Code::Configuration);
        let ro = open_ro(&path).err().expect("open_ro must refuse newer");
        assert_eq!(ro.code, crate::error::Code::Configuration);
    }
}
