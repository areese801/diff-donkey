/// SQL activity logging — tracks every query DuckDB executes.
///
/// Think of this like DataGrip's query console or DBeaver's SQL log —
/// it records what ran, how long it took, and whether it succeeded.
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use duckdb::Connection;
use serde::Serialize;

use crate::error::DiffDonkeyError;

#[derive(Debug, Clone, Serialize)]
pub struct QueryLogEntry {
    /// Milliseconds since Unix epoch — frontend formats this
    pub timestamp: u64,
    /// Human-readable operation name, e.g. "load_csv", "build_diff_join"
    pub operation: String,
    /// The actual SQL that was executed
    pub sql: String,
    /// How long the query took in milliseconds
    pub duration_ms: u64,
    /// Number of rows affected/returned, if known
    pub rows_affected: Option<i64>,
    /// Error message if the query failed
    pub error: Option<String>,
}

pub struct ActivityLog {
    entries: Mutex<Vec<QueryLogEntry>>,
}

impl ActivityLog {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn log_query(
        &self,
        operation: &str,
        sql: &str,
        duration_ms: u64,
        rows_affected: Option<i64>,
        error: Option<String>,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = QueryLogEntry {
            timestamp,
            operation: operation.to_string(),
            sql: sql.to_string(),
            duration_ms,
            rows_affected,
            error,
        };
        self.entries.lock().unwrap().push(entry);
    }

    pub fn get_entries(&self) -> Vec<QueryLogEntry> {
        self.entries.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }
}

/// Execute a SQL batch and log it to the activity log.
pub fn execute_logged(
    conn: &Connection,
    sql: &str,
    operation: &str,
    log: &ActivityLog,
) -> Result<(), DiffDonkeyError> {
    let start = Instant::now();
    let result = conn.execute_batch(sql);
    let duration = start.elapsed().as_millis() as u64;
    log.log_query(
        operation,
        sql,
        duration,
        None,
        result.as_ref().err().map(|e| e.to_string()),
    );
    result.map_err(DiffDonkeyError::from)
}

/// Execute a single-row query and log it.
pub fn query_row_logged<T>(
    conn: &Connection,
    sql: &str,
    operation: &str,
    log: &ActivityLog,
    f: impl FnOnce(&duckdb::Row) -> Result<T, duckdb::Error>,
) -> Result<T, DiffDonkeyError> {
    let start = Instant::now();
    let result = conn.query_row(sql, [], f);
    let duration = start.elapsed().as_millis() as u64;
    log.log_query(
        operation,
        sql,
        duration,
        Some(1),
        result.as_ref().err().map(|e| e.to_string()),
    );
    result.map_err(DiffDonkeyError::from)
}
