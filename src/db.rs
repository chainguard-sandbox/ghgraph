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
//! Two preconditions of the mode guarantee, and their enforcement:
//!   * A pre-existing archive directory (an operator's custom `db_path`
//!     pointed at a shared dir) keeps its own mode — but open_rw REFUSES a
//!     group/other-WRITABLE parent unless it is root-owned AND sticky (the
//!     /tmp shape): a writable parent is exactly the access the symlink-swap
//!     defense assumes the attacker lacks; sticky denies NON-OWNERS the
//!     unlink+replace a swap needs, and the root-owned narrowing keeps a
//!     user-owned 1777 dir (whose owner retains both) refused. The residue
//!     sticky cannot cover — planting a symlink at a not-yet-existing
//!     archive path needs only create — is closed by an lstat refusal of a
//!     symlink at the path itself (refuse_symlink_archive). Immediate
//!     parent only, with the limit stated: a writable ANCESTOR can rename
//!     the parent dir itself aside and substitute one that passes every
//!     check here — the same unsafe-traversal exposure the module accepts
//!     by rejecting NOFOLLOW/openat chains, and an operator's path choice,
//!     not a mode this code can vet. Write bits only, decided: a
//!     world-READABLE parent weakens the -wal/-shm confidentiality
//!     boundary but forges nothing — refusing it would false-refuse every
//!     home directory more open than 0700, so readability stays the
//!     operator's call and integrity does not.
//!   * mode() is masked by the process umask, so 0700/0600 is a ceiling, not a
//!     floor. umask can only tighten, never loosen, so confidentiality is never
//!     regressed; an exotic umask that clears owner bits could make the archive
//!     unwritable, which surfaces as a CONFIGURATION open error. The tests
//!     re-exec themselves under `sh -c 'umask 0; ...'` to prove the explicit
//!     .mode() calls carry the guarantee rather than an inherited umask.
//!
//! Versioning: PRAGMA user_version. 0 → apply schema.sql (always the CURRENT
//! shape) → SCHEMA_VERSION, schema apply and the version bump in ONE
//! rusqlite-managed transaction, so a crash mid-apply rolls back to 0 and the
//! next open retries from clean — the archive is never half-initialized.
//! Every user_version value has a defined outcome (see `migrate`); a value
//! we do not understand is refused, never guessed, and pre-release stamps
//! are refused rather than migrated — the policy and its reversal point
//! (the first released binary) are recorded at SCHEMA_VERSION. No
//! schema_version table; the pragma is the record.

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
/// `_meta.schema_version` (report.rs) — the archive can move without the
/// output contract moving, which is exactly what v2 did (an added column
/// feeds a derivation; no emitted field changed shape).
///
/// v2: prs.head_committed_at — the stale-side approval-staleness bound. The
/// v1 schema's own comments claimed staleness "derives from committedDate",
/// but v1 never stored it: parse.rs validated the field and the upsert
/// dropped it.
///
/// v3: quarantine.stream — retry dispatch by hydration document (schema.sql
/// records why an opaque node id cannot carry that fact itself).
///
/// v4: the sync_runs run-telemetry table (hardening milestone) — one flat
/// row per completed run, so trends are a `query` away without a telemetry
/// store growing underneath. An archive fact only; no emitted field changed
/// shape (`stats` gained additive keys under the additive-only contract).
///
/// MIGRATION POLICY, decided here so the machinery is not re-proposed every
/// schema change: migrations begin at the first RELEASED binary — an
/// archive someone cannot cheaply rebuild. Until then the archive is a
/// disposable cache (that is already the corruption remedy's prose), so a
/// pre-release schema change bumps this version and REFUSES older stamps
/// with the remove-and-resync remedy, rather than carrying ALTER TABLE
/// steps, their column-order constraints (an appended column must then stay
/// last forever or `query` SELECT * forks by archive provenance), and the
/// fixture archaeology their tests need. The bump itself is NOT optional:
/// amending the schema in place under an unchanged version would let an
/// old archive pass the version gate and then fail mid-verb with "no such
/// column" classified INTERNAL — a lie about the actor. A v1→v2 ALTER
/// TABLE migration existed briefly and was verified correct; it was deleted
/// by this policy, not by a defect (the git history holds it if the first
/// release ever needs the pattern back).
pub const SCHEMA_VERSION: i64 = 4;

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

    // No into_inner, DECIDED: an escaped Connection would outlive the
    // truncate-on-close mechanism below, making "the WAL is truncated at
    // writer close" hold on some paths and not others. Nothing needs to own
    // the raw Connection; if something ever does, it must also own the close
    // behavior it is opting out of.
}

/// Best-effort WAL truncate at writer close (hardening milestone): a busy
/// steady-state sync can leave a WAL comparable to the archive itself, and
/// the next writer may be days away. TRUNCATE (not PASSIVE) returns the disk
/// space. Best-effort is load-bearing: a reader holding a WAL snapshot makes
/// the truncate report busy, and a run that synced correctly must not turn
/// into an error over housekeeping — so the result is deliberately ignored
/// (the next close retries by existing). Drop, not an explicit close method:
/// every writer path — sync's run, the targeted form, a mid-run error unwind,
/// every test — closes through here, so the mechanism cannot be forgotten.
impl Drop for RwArchive {
    fn drop(&mut self) {
        // wal_checkpoint returns a (busy, log, checkpointed) row; both the
        // row and any error are non-actionable here by the argument above.
        let _ = self
            .0
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
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

/// The `stats`-audit connection: SQLITE_OPEN_READ_ONLY, deliberately WITHOUT
/// query_only — see [`open_ro_audit`] for the argument and its bounds. A
/// distinct type so the weakened belt cannot be handed to a verb that runs
/// operator SQL by accident of sharing [`RoArchive`]'s signature.
pub struct RoAuditArchive(Connection);

impl RoAuditArchive {
    /// The underlying connection. Archive writes fail at runtime via the
    /// READ_ONLY open flag; temp-schema writes (the fts5vocab audit tables)
    /// succeed, which is this type's whole reason to exist.
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
        refuse_writable_parent(parent)?;
    }
    create_0600_if_absent(path)?;
    refuse_symlink_archive(path)?;

    // create_0600_if_absent already birthed the file at 0600, so in the normal
    // path SQLITE_OPEN_CREATE never sets a mode. It is kept only to survive the
    // vanishing-file race between our create and this open — and in that race
    // branch O_CREAT recreates the file at umask-default (not 0600). The mode
    // re-check after open (below) closes that branch: whatever file this open
    // landed on must still be owner-only. No NOFOLLOW — it false-refuses
    // archives under symlinked parent dirs; the 0700 directory is the
    // symlink-swap defense (module docs).
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let mut conn = Connection::open_with_flags(path, flags)
        .map_err(|e| sqlite_err(path, "cannot open archive", e))?;
    refuse_loose_archive_mode(path)?;
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

/// Open the archive for the `stats` audits: SQLITE_OPEN_READ_ONLY but NO
/// PRAGMA query_only, because the FTS integrity audit introspects the index
/// through `fts5vocab` TEMP virtual tables — the only read-only ENUMERATION
/// of the index's per-rowid contents (a plain fts5 full scan answers from
/// the CONTENT table so it cannot witness a desync, and a MATCH reads the
/// index but only for terms you already know) — and query_only refuses even
/// temp-schema writes. What this trades, precisely: archive write-immunity
/// remains a mechanism (the VFS-level READ_ONLY flag), but the belt
/// query_only adds — refusing ATTACH-based writes to OTHER files — is off.
/// Admissible only because stats executes exclusively its own literal SQL;
/// the `query` verb, which runs operator SQL, keeps the full pair and must
/// never move to this open. The guard is at the OPEN, not at execute():
/// RoAuditArchive::conn is a raw &Connection like every wrapper here, so
/// the type prevents handing the weakened connection around, not misusing
/// one already held. Same existence and version gates as [`open_ro`].
/// Reversal evidence: an fts5 mechanism that lets a query_only connection
/// enumerate index rowids would retire this open.
pub fn open_ro_audit(path: &Path) -> Result<RoAuditArchive> {
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
    // Mutation notes, shared with open_ro's identical lines: the flag `|`
    // has an equivalent `^` mutant (the two flags are disjoint bit sets, so
    // OR and XOR coincide); the hook's `== "1"` has an equivalent `!=`
    // mutant under the test suite, because for contract-correct queries the
    // reversal pragma is observably a no-op either way — the hook exists to
    // catch INCORRECT queries, and a test that could see the flip would
    // have to ship one. Documented per the triage rule, not chased.
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags)
        .map_err(|e| sqlite_err(path, "cannot open archive", e))?;
    configure_conn(&conn, path)?;
    // The same determinism hook as open_ro: the audit statements are
    // contract output too and must hold under reversed unordered selects.
    if std::env::var_os("GHGRAPH_TEST_REVERSE_SELECTS").is_some_and(|v| v == "1") {
        conn.pragma_update(None, "reverse_unordered_selects", true)
            .map_err(|e| sqlite_err(path, "cannot set reverse_unordered_selects on", e))?;
    }
    let version = user_version(&conn, path)?;
    if version != SCHEMA_VERSION {
        return Err(wrong_version(path, version));
    }
    Ok(RoAuditArchive(conn))
}

/// Create the parent chain, with any directory WE create born 0700. A
/// pre-existing directory keeps its mode — the integrity floor it must still
/// meet is `refuse_writable_parent`, checked by open_rw right after this.
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

/// Refuse a group/other-WRITABLE archive directory without the sticky bit
/// (module docs carry the full argument): the 0700-directory symlink-swap
/// defense is void exactly when someone else can write the directory, so
/// that state is a refusal, not a footnote. Sticky (the /tmp shape) is
/// exempt — non-owners cannot unlink or rename our entries there, which is
/// the operation a swap needs. Checked on the IMMEDIATE parent only: an
/// ancestor's mode governs reaching the directory, not replacing entries
/// inside it. RW-side only: the reader creates nothing and inherits a
/// directory the writer already vetted; refusing reads of an archive that
/// synced fine yesterday would punish the reader for the writer's problem.
fn refuse_writable_parent(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    // A bare-filename db_path ("ghgraph.db") has parent Some("") — that is
    // the current directory, so check it as such rather than ENOENT-ing on
    // the empty path (ensure_dir_0700 early-returns on the same input).
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let meta = std::fs::metadata(dir)
        .map_err(|e| Error::config(format!("cannot access archive dir {}: {e}", dir.display())))?;
    let mode = meta.permissions().mode();
    // The sticky exemption is narrowed to ROOT-owned dirs (the actual /tmp
    // shape): sticky denies non-owners the unlink+rename a swap needs, but
    // the directory's OWNER keeps both — so a user-owned 1777 dir is still
    // a swap venue for its owner and is refused (its owner can chmod go-w;
    // the remedy applies). The plant-before-create residue a sticky dir
    // still permits (anyone may CREATE a symlink at a path that does not
    // exist yet) is closed by refuse_symlink_archive, not here.
    //
    // Mutation note: the sticky-mask operators (& 0o1000) have surviving
    // mutants (|, ^) whose only discriminating fixture is a ROOT-owned
    // writable NON-sticky directory — unprivileged tests cannot create
    // one. The testable arms are covered: user-owned sticky refused,
    // root-owned sticky (/tmp itself) exempt, plain writable refused.
    if mode & 0o022 != 0 && !(mode & 0o1000 != 0 && meta.uid() == 0) {
        return Err(Error::config(format!(
            "archive dir {} is group/other-writable (mode {:03o}) — anyone with write \
             access could swap the archive through a symlink; chmod go-w the directory \
             or point db_path somewhere private",
            dir.display(),
            mode & 0o7777
        )));
    }
    Ok(())
}

/// Refuse a symlink at the archive path itself, checked with lstat AFTER
/// `create_0600_if_absent` and immediately before the SQLite open. This is
/// the plant-before-create defense for shared sticky dirs (/tmp): anyone
/// may create a symlink at a not-yet-existing path — no unlink needed, so
/// the sticky bit does not help — and the open (deliberately NOFOLLOW-less,
/// module docs) would follow it. Post-create ordering bounds the race: if
/// WE created the file, a non-owner cannot replace it under sticky; if it
/// pre-existed, whatever is there is what lstat sees.
fn refuse_symlink_archive(path: &Path) -> Result<()> {
    let meta = std::fs::symlink_metadata(path)
        .map_err(|e| Error::config(format!("cannot stat archive {}: {e}", path.display())))?;
    if meta.file_type().is_symlink() {
        return Err(Error::config(format!(
            "archive path {} is a symlink — ghgraph does not follow one where the \
             archive should be; remove it and resync",
            path.display()
        )));
    }
    Ok(())
}

/// Refuse an archive file with group/other permission bits, re-checked AFTER
/// the SQLite open so it covers whatever file the open actually landed on —
/// this closes the vanishing-file race in open_rw (a file recreated by
/// O_CREAT at umask default between our 0600 birth and the open) and, the
/// common case, a pre-existing archive born looser than ghgraph ever creates
/// (a chmod'd file, or one copied in from elsewhere). A refusal with the
/// remedy, never a repair chmod: modes are set at creation by design, and a
/// silent chmod here would paper over whoever loosened it. Path re-stat, not
/// literally the fd (rusqlite does not expose it): a swap between open and
/// stat requires writing the parent, which `refuse_writable_parent` already
/// bounds. One known benign trigger: a Linux directory with a default POSIX
/// ACL can surface ACL-mask bits in st_mode group bits at creation despite
/// our 0600 request — the refusal and its remedy still apply (the archive
/// really is group-accessible there), it is just the operator's ACL rather
/// than a chmod that loosened it.
fn refuse_loose_archive_mode(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path)
        .map_err(|e| Error::config(format!("cannot stat archive {}: {e}", path.display())))?;
    let mode = meta.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(Error::config(format!(
            "archive {} has group/other permission bits (mode {:03o}) — ghgraph creates \
             it 0600; chmod 600 the file, or remove it and resync",
            path.display(),
            mode & 0o7777
        )));
    }
    Ok(())
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
        v if v == SCHEMA_VERSION => Ok(()),
        // Everything else — an older pre-release stamp (refused with the
        // remove-and-resync remedy; migrations begin at the first release,
        // see SCHEMA_VERSION), a newer archive, or a negative/foreign
        // sentinel (SQLite accepts any i64 user_version) — is refused,
        // never guessed.
        v => Err(wrong_version(path, v)),
    }
}

/// The CONFIGURATION error for an archive whose `user_version` is not the
/// current [`SCHEMA_VERSION`] and cannot be migrated to it. Shared by `open_ro`
/// (any non-current version) and `migrate` (its refusal arms) so the two never
/// drift. `migrate` handles v == 0 by applying the schema, so the v == 0 message
/// here is reached only from `open_ro`.
fn wrong_version(path: &Path, v: i64) -> Error {
    // Mutation note: the < and > below have equivalent mutants (<= / >=):
    // their boundary values are unreachable — v == 0 is consumed by the arm
    // above, and v == SCHEMA_VERSION never reaches this function (open_ro
    // calls it only on a version mismatch; migrate's arms consume the rest).
    // Documented per the triage rule rather than chased with a test that
    // could only assert the unreachable.
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
        // 0 < v < SCHEMA_VERSION: a pre-release schema. Not migrated,
        // by policy (SCHEMA_VERSION) — the archive is a disposable cache.
        format!(
            "a pre-release schema this ghgraph (v{SCHEMA_VERSION}) does not migrate — \
             the archive is a disposable cache; remove it and resync"
        )
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
        // This reads back the final bits under the ambient umask; since umask
        // only clears bits, an unset .mode() call could still pass here under
        // a benign umask. umask_zero_still_births_0700_0600 below closes that
        // hole by re-running the creation under `umask 0`.
    }

    /// The umask-injection harness: re-exec this test binary under
    /// `sh -c 'umask 0'` and prove creation is STILL 0700/0600 — under a
    /// zero umask an unset .mode() would leak 0755/0644, so this test dies
    /// exactly when the explicit-mode call is dropped. Re-exec because umask
    /// is process-global: setting it in-process would race sibling tests,
    /// and std exposes no umask API anyway (the libc dep is off the floor).
    #[test]
    fn umask_zero_still_births_0700_0600() {
        let s = Scratch::new();
        std::fs::create_dir_all(&s.dir).unwrap();
        let exe = std::env::current_exe().unwrap();
        let out = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(r#"umask 0; exec "$0" --exact db::tests::umask_child_open_rw --ignored"#)
            .arg(&exe)
            .env("GHGRAPH_TEST_UMASK_DIR", &s.dir)
            .output()
            .expect("re-exec test binary under umask 0");
        assert!(
            out.status.success(),
            "umask-0 child failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        // Assert from the parent side too — the child ran under umask 0, so
        // these bits can only be here because .mode() put them here.
        let db = s.dir.join("sub/ghgraph.db");
        let file_mode = std::fs::metadata(&db).unwrap().permissions().mode() & 0o7777;
        assert_eq!(file_mode, 0o600, "db must be 0600 even under umask 0");
        let dir_mode = std::fs::metadata(db.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            dir_mode, 0o700,
            "archive dir must be 0700 even under umask 0"
        );
    }

    /// Child half of umask_zero_still_births_0700_0600 — ignored so the
    /// normal sweep never runs it directly; when it IS invoked without the
    /// parent's env (e.g. `make check-heavy` runs everything ignored), it
    /// has nothing to assert and exits clean.
    #[test]
    #[ignore = "child of umask_zero_still_births_0700_0600, spawned by it"]
    fn umask_child_open_rw() {
        let Some(dir) = std::env::var_os("GHGRAPH_TEST_UMASK_DIR") else {
            return;
        };
        let path = PathBuf::from(dir).join("sub/ghgraph.db");
        let _arc = open_rw(&path).unwrap();
    }

    #[test]
    fn refuses_group_writable_parent_dir() {
        let s = Scratch::new();
        std::fs::create_dir_all(&s.dir).unwrap();
        std::fs::set_permissions(&s.dir, std::fs::Permissions::from_mode(0o770)).unwrap();
        let err = open_rw(&s.join("ghgraph.db"))
            .err()
            .expect("a group-writable parent must be refused");
        assert_eq!(err.code, crate::error::Code::Configuration);
        assert!(
            err.message.contains("chmod go-w"),
            "message must carry the remedy, got: {}",
            err.message
        );
    }

    #[test]
    fn user_owned_sticky_writable_parent_is_refused() {
        // Sticky alone is not the exemption: the directory's OWNER keeps
        // unlink+rename, so a user-owned 1777 dir is still a swap venue
        // for its owner. Only root-owned sticky (the real /tmp) is exempt.
        let s = Scratch::new();
        std::fs::create_dir_all(&s.dir).unwrap();
        std::fs::set_permissions(&s.dir, std::fs::Permissions::from_mode(0o1777)).unwrap();
        let err = open_rw(&s.join("ghgraph.db"))
            .err()
            .expect("a user-owned sticky writable dir must be refused");
        assert_eq!(err.code, crate::error::Code::Configuration);
    }

    #[test]
    fn root_owned_sticky_dir_is_allowed() {
        // The exemption itself, exercised against the real thing: /tmp is
        // root-owned 1777 on every supported platform. Skip (not fail) if
        // some exotic host shapes /tmp differently — the exemption is then
        // simply untested here, and the refusal tests above still hold.
        use std::os::unix::fs::MetadataExt;
        let tmp = Path::new("/tmp");
        let meta = std::fs::metadata(tmp).unwrap();
        if meta.uid() != 0 || meta.permissions().mode() & 0o1000 == 0 {
            eprintln!("skipping: /tmp is not root-owned sticky on this host");
            return;
        }
        let path = std::env::temp_dir().join(format!(
            "ghgraph-sticky-{}-{}.db",
            std::process::id(),
            line!()
        ));
        // temp_dir may not be /tmp (macOS): target /tmp explicitly.
        let path = tmp.join(path.file_name().unwrap());
        let _ = std::fs::remove_file(&path);
        open_rw(&path).expect("root-owned sticky /tmp must open");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db.lock"));
    }

    #[test]
    fn refuses_symlink_at_archive_path() {
        // The plant-before-create vector: a symlink waiting at the archive
        // path (create needs no unlink, so sticky dirs permit it) must be
        // refused by the lstat check, never followed by the open.
        let s = Scratch::new();
        std::fs::create_dir_all(&s.dir).unwrap();
        let target = s.join("elsewhere.db");
        let path = s.join("ghgraph.db");
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let err = open_rw(&path)
            .err()
            .expect("a symlink at the archive path must be refused");
        assert_eq!(err.code, crate::error::Code::Configuration);
        assert!(
            err.message.contains("symlink"),
            "message must name the finding, got: {}",
            err.message
        );
        assert!(!target.exists(), "the symlink target must never be created");
    }

    #[test]
    fn refuses_loose_archive_file_mode() {
        // A pre-existing archive with group/other bits — chmod'd, or copied
        // in — is refused with the remedy on the next writer open; the same
        // check is what closes open_rw's vanishing-file race branch.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let err = open_rw(&path)
            .err()
            .expect("a group-readable archive must be refused");
        assert_eq!(err.code, crate::error::Code::Configuration);
        assert!(
            err.message.contains("chmod 600"),
            "message must carry the remedy, got: {}",
            err.message
        );
    }

    #[test]
    fn wal_truncates_at_writer_close() {
        // The discriminating setup is the SECOND, idle connection held open
        // across the writer's drop: with it, SQLite's own last-connection
        // close checkpoint never runs (it needs exclusive access), so an
        // empty -wal afterward can only be OUR Drop-side TRUNCATE — the
        // very case the mechanism exists for (an MCP reader keeping the
        // archive open between syncs). Without the reader, a no-op Drop
        // passes this test by riding SQLite's close behavior.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        let reader = {
            let arc = open_rw(&path).unwrap();
            let reader = open_ro(&path).unwrap();
            arc.conn()
                .execute_batch(
                    "CREATE TABLE _bulk(x); \
                     INSERT INTO _bulk (x) \
                       WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n LIMIT 500) \
                       SELECT zeroblob(1024) FROM n",
                )
                .unwrap();
            let wal = std::fs::metadata(path.with_extension("db-wal"))
                .expect("-wal exists while the writer is open");
            assert!(wal.len() > 0, "WAL must be non-empty before close");
            reader
        };
        // The reader outlives the writer, so the -wal file must still
        // exist — expect() rather than a defaulted 0, or a deleted file
        // would pass as truncated.
        let wal_len = std::fs::metadata(path.with_extension("db-wal"))
            .expect("-wal persists while a reader holds the archive")
            .len();
        assert_eq!(wal_len, 0, "writer close must truncate the WAL");
        drop(reader);
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

    #[test]
    fn refuses_pre_release_schema_with_resync_remedy() {
        // Pre-release stamps are refused, not migrated (the policy at
        // SCHEMA_VERSION), and BOTH open paths must name the actionable
        // remedy — the archive is a disposable cache, so the remedy is
        // remove-and-resync, never a migration that does not exist. The
        // shape behind the stamp is irrelevant: refusal happens at the
        // version gate, before any statement could notice the columns.
        let s = Scratch::new();
        let path = s.join("ghgraph.db");
        {
            let _a = open_rw(&path).unwrap();
        }
        set_raw_user_version(&path, 1);
        for (name, err) in [
            (
                "open_rw",
                open_rw(&path).err().expect("open_rw must refuse v1"),
            ),
            (
                "open_ro",
                open_ro(&path).err().expect("open_ro must refuse v1"),
            ),
        ] {
            assert_eq!(err.code, crate::error::Code::Configuration, "{name}");
            assert!(
                err.message.contains("remove it and resync"),
                "{name} must carry the disposable-cache remedy, got: {}",
                err.message
            );
        }
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
