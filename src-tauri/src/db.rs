/// DuckDB connection management.
///
/// DuckDB's Connection is NOT thread-safe (like a psycopg2 connection in Python).
/// Tauri commands run on multiple threads, so we wrap it in a Mutex.
///
/// Mutex = mutual exclusion lock. Think of it like Python's threading.Lock() —
/// only one thread can access the connection at a time. When one thread is
/// using it, others wait. This prevents data races (two threads writing to
/// the same connection simultaneously).
///
/// In Tauri, this struct is stored as "managed state" — similar to Flask's
/// `app.config` or Django's `settings`. Any command can access it.
use std::sync::Mutex;

use duckdb::Connection;

use crate::error::DiffDonkeyError;

pub struct DuckDbState {
    pub conn: Mutex<Connection>,
}

impl DuckDbState {
    /// Create a new in-memory DuckDB connection.
    /// Equivalent to: `conn = duckdb.connect(':memory:')` in Python.
    pub fn new() -> Result<Self, DiffDonkeyError> {
        let conn = Connection::open_in_memory()?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}
