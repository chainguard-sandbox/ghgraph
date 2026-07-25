//! Archive open + migrate.
//!
//! Write connection (exactly one, owned by the sync writer thread):
//!   journal_mode=WAL, synchronous=NORMAL, busy_timeout=5000. WAL matters even
//!   single-writer: read commands stay usable mid-sync.
//!
//! Read connections: SQLITE_OPEN_READ_ONLY *plus* PRAGMA query_only=ON —
//! belt and suspenders; the pragma also blocks ATTACH-based writes.
//!
//! Migrations: PRAGMA user_version. 0 → apply schema.sql → 1. Later versions
//! are numbered fn(&Transaction) steps, each bumping the pragma inside its own
//! transaction. No schema_version table; the pragma is the record.

use std::path::Path;

use rusqlite::Connection;

use crate::error::Result;

pub const SCHEMA: &str = include_str!("schema.sql");

/// Current schema version, written to PRAGMA user_version after migration.
pub const SCHEMA_VERSION: i64 = 1;

pub fn open_rw(_path: &Path) -> Result<Connection> {
    todo!("create parent dirs; open; pragmas; migrate to SCHEMA_VERSION")
}

pub fn open_ro(_path: &Path) -> Result<Connection> {
    todo!("OPEN_READ_ONLY | query_only=ON; error CONFIGURATION if archive missing")
}
