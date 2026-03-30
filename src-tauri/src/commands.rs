/// Tauri IPC commands — the bridge between frontend and Rust.
///
/// Each `#[tauri::command]` function is callable from the frontend via:
///   const result = await invoke("command_name", { arg1: value1 });
///
/// Think of these like Flask route handlers or Django views — they receive
/// a request (args), do work, and return a response (JSON).
///
/// SECURITY: All inputs from the IPC boundary are untrusted. Even though
/// the Svelte frontend constrains values (e.g., label is "a" | "b"),
/// any JavaScript in the webview can call invoke() directly with arbitrary
/// strings. Every IPC parameter must be validated before use in SQL.
use tauri::State;

use crate::activity::{self, ActivityLog};
use crate::connections::{self, SavedConnection};
use crate::db::DuckDbState;
use crate::db_loader;
use crate::diff;
use crate::error::DiffDonkeyError;
use crate::loader;
use crate::types::{
    ColumnTolerance, DiffConfig, OverviewResult, PagedRows, SchemaComparison, TableMeta,
};

/// Maximum rows per page to prevent memory exhaustion via large page_size.
const MAX_PAGE_SIZE: usize = 1000;

/// Validate that a string is a safe SQL identifier (alphanumeric + underscore).
/// Rejects anything with quotes, semicolons, spaces, or other SQL-special chars.
fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Validate that a column name exists in the given table's schema.
/// Returns an error if the column doesn't exist — prevents injection via
/// column names that aren't actually in the data.
fn validate_column_exists(
    conn: &duckdb::Connection,
    table_name: &str,
    column_name: &str,
) -> Result<(), String> {
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM information_schema.columns \
             WHERE table_name = ? AND column_name = ?",
            [table_name, column_name],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;

    if !exists {
        return Err(format!(
            "Column '{}' does not exist in table '{}'",
            column_name, table_name
        ));
    }
    Ok(())
}

/// Load a file (CSV or Parquet) into DuckDB as either source_a or source_b.
///
/// Called from frontend after user picks a file via the file dialog.
/// The `label` parameter is "a" or "b" — determines the table name.
#[tauri::command]
pub fn load_source(
    path: String,
    label: String,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<TableMeta, String> {
    // SECURITY: Validate label is exactly "a" or "b" — prevents table name injection.
    // Without this, a label like "a; DROP TABLE source_b; --" would inject SQL.
    if label != "a" && label != "b" {
        return Err("Invalid label: must be 'a' or 'b'".to_string());
    }

    let table_name = format!("source_{}", label);

    // Lock the mutex to get exclusive access to the DuckDB connection.
    // .lock() returns a MutexGuard — when it goes out of scope, the lock
    // is automatically released. Similar to Python's `with lock:` context manager.
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    // Detect file type by extension
    let result = if path.ends_with(".parquet") || path.ends_with(".pq") {
        loader::load_parquet(&conn, &path, &table_name, &log)
    } else {
        loader::load_csv(&conn, &path, &table_name, &log)
    };

    result.map_err(|e: DiffDonkeyError| e.into())
}

/// Load data from a remote database into DuckDB as either source_a or source_b.
///
/// Uses DuckDB's postgres or mysql extension to query the remote database
/// and materialize the result as a local table.
#[tauri::command]
pub fn load_database_source(
    conn_string: String,
    query: String,
    label: String,
    db_type: db_loader::DatabaseType,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<TableMeta, String> {
    // SECURITY: Validate label is exactly "a" or "b"
    if label != "a" && label != "b" {
        return Err("Invalid label: must be 'a' or 'b'".to_string());
    }

    let table_name = format!("source_{}", label);

    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    db_loader::load_from_database(&conn, &conn_string, &query, &table_name, &db_type, &log)
        .map_err(|e: DiffDonkeyError| {
            // SECURITY: Sanitize connection errors to avoid leaking credentials.
            // The full error is logged to stderr for debugging.
            let msg = e.to_string();
            eprintln!("Database load error: {}", msg);
            if msg.contains("connection")
                || msg.contains("authentication")
                || msg.contains("password")
            {
                "Database connection failed. Check your connection string and credentials."
                    .to_string()
            } else {
                e.into()
            }
        },
    )
}

/// Compare schemas of the two loaded sources.
/// Both source_a and source_b must be loaded first.
#[tauri::command]
pub fn get_schema_comparison(state: State<DuckDbState>) -> Result<SchemaComparison, String> {
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    diff::schema::compare_schemas(&conn).map_err(|e: DiffDonkeyError| e.into())
}

/// Run the full diff on both sources.
///
/// Accepts a `DiffConfig` with the primary key column, optional default tolerance,
/// and optional per-column tolerance overrides.
#[tauri::command]
pub fn run_diff(
    config: DiffConfig,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<OverviewResult, String> {
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    let pk_columns = &config.pk_columns;

    // SECURITY: Validate each PK column exists in both source tables.
    for pk in pk_columns {
        validate_column_exists(&conn, "source_a", pk)?;
        validate_column_exists(&conn, "source_b", pk)?;
    }

    if pk_columns.is_empty() {
        return Err("At least one primary key column is required".to_string());
    }

    // Validate global precision (negative values are valid — ROUND(x, -1) rounds to nearest 10)
    // No range restriction needed; DuckDB handles all integer precision values.

    // Validate per-column tolerances
    let column_tolerances = config.column_tolerances.unwrap_or_default();
    for (col, tol) in &column_tolerances {
        match tol {
            // Precision can be negative (ROUND(x, -1) rounds to nearest 10)
            ColumnTolerance::Seconds { seconds }
                if *seconds < 0.0 || seconds.is_nan() || seconds.is_infinite() =>
            {
                return Err(format!(
                    "Seconds tolerance for column '{}' must be a non-negative finite number",
                    col
                ));
            }
            _ => {}
        }
    }

    // Get shared columns (excluding PKs) to compare
    let schema = diff::schema::compare_schemas(&conn).map_err(|e| e.to_string())?;
    let compare_columns: Vec<String> = schema
        .shared
        .iter()
        .filter(|c| !pk_columns.contains(&c.name))
        .map(|c| c.name.clone())
        .collect();

    // Build column type map from shared columns (use type_a)
    let column_types: std::collections::HashMap<String, String> = schema
        .shared
        .iter()
        .filter(|c| !pk_columns.contains(&c.name))
        .map(|c| (c.name.clone(), c.type_a.clone()))
        .collect();

    // Store PK columns as JSON array in _diff_meta
    let pk_json = serde_json::to_string(pk_columns).map_err(|e| e.to_string())?;
    conn.execute(
        "CREATE OR REPLACE TEMPORARY TABLE _diff_meta AS SELECT ? as pk_columns",
        [&pk_json],
    )
    .map_err(|e| e.to_string())?;

    diff::stats::run_diff(
        &conn,
        pk_columns,
        &compare_columns,
        &column_types,
        config.tolerance,
        &column_tolerances,
        &log,
    )
    .map_err(|e: DiffDonkeyError| e.into())
}

/// Helper: get PK column names from the stored diff metadata.
fn get_pk_columns(conn: &duckdb::Connection) -> Result<Vec<String>, String> {
    let json_str: String = conn
        .query_row("SELECT pk_columns FROM _diff_meta", [], |row| row.get(0))
        .map_err(|_| "Diff not run yet — run diff first".to_string())?;
    serde_json::from_str(&json_str).map_err(|e| format!("Failed to parse PK columns: {}", e))
}

/// Get exclusive rows — rows that exist only in one side.
#[tauri::command]
pub fn get_exclusive_rows(
    side: String,
    page: usize,
    page_size: usize,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<PagedRows, String> {
    let page_size = page_size.min(MAX_PAGE_SIZE);
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;
    let pks = get_pk_columns(&conn)?;

    diff::keys::get_exclusive_rows(&conn, &side, &pks, page, page_size, &log)
        .map_err(|e: DiffDonkeyError| e.into())
}

/// Get duplicate primary keys for one side.
#[tauri::command]
pub fn get_duplicate_pks(
    side: String,
    page: usize,
    page_size: usize,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<PagedRows, String> {
    let page_size = page_size.min(MAX_PAGE_SIZE);
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;
    let pks = get_pk_columns(&conn)?;

    diff::keys::get_duplicate_pks(&conn, &side, &pks, page, page_size, &log)
        .map_err(|e: DiffDonkeyError| e.into())
}

/// Get diff rows — matched rows where values differ, with pagination.
/// Optional column_filter limits to rows where a specific column differs.
/// Optional row_filter: "all" | "diffs" | "minor" | "same" (default: "diffs").
#[tauri::command]
pub fn get_diff_rows(
    page: usize,
    page_size: usize,
    column_filter: Option<String>,
    row_filter: Option<String>,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<PagedRows, String> {
    let page_size = page_size.min(MAX_PAGE_SIZE);
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    // SECURITY: Validate row_filter against known values
    if let Some(ref rf) = row_filter {
        if !["all", "diffs", "minor", "same"].contains(&rf.as_str()) {
            return Err(format!("Invalid row filter: '{}'", rf));
        }
    }

    // Get compare column names from _diff_join (is_diff_* columns)
    let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '_diff_join' AND column_name LIKE 'is_diff_%' \
             ORDER BY ordinal_position",
        )
        .map_err(|e| e.to_string())?;

    let compare_cols: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    // Get is_raw_diff_* columns
    let mut raw_stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '_diff_join' AND column_name LIKE 'is_raw_diff_%' \
             ORDER BY ordinal_position",
        )
        .map_err(|e| e.to_string())?;

    let raw_diff_cols: Vec<String> = raw_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    // SECURITY: Validate column_filter against the known column list.
    if let Some(ref col) = column_filter {
        let expected_is_diff = format!("is_diff_{}", col);
        if !compare_cols.contains(&expected_is_diff) {
            return Err(format!("Invalid column filter: '{}'", col));
        }
    }

    // Build composite PK NOT NULL check from _diff_join schema
    let mut pk_stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '_diff_join' AND column_name LIKE 'pk_%_a' \
             ORDER BY ordinal_position",
        )
        .map_err(|e| e.to_string())?;

    let pk_a_cols: Vec<String> = pk_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    let pk_not_null = pk_a_cols
        .iter()
        .map(|c| format!("\"{}\" IS NOT NULL", c))
        .chain(
            pk_a_cols
                .iter()
                .map(|c| format!("\"{}\" IS NOT NULL", c.replace("_a", "_b"))),
        )
        .collect::<Vec<_>>()
        .join(" AND ");

    // Build WHERE clause based on row_filter
    let where_clause = match row_filter.as_deref() {
        Some("all") => {
            // All matched rows
            pk_not_null.clone()
        }
        Some("minor") => {
            // All is_diff_* = 0 AND at least one is_raw_diff_* = 1
            let no_diffs = compare_cols
                .iter()
                .map(|c| format!("\"{}\" = 0", c))
                .collect::<Vec<_>>()
                .join(" AND ");
            let any_raw = raw_diff_cols
                .iter()
                .map(|c| format!("\"{}\" = 1", c))
                .collect::<Vec<_>>()
                .join(" OR ");

            if let Some(ref col) = column_filter {
                // Minor diff on this specific column
                let is_diff_col = format!("is_diff_{}", col);
                let is_raw_diff_col = format!("is_raw_diff_{}", col);
                format!(
                    "{} AND \"{}\" = 0 AND \"{}\" = 1",
                    pk_not_null, is_diff_col, is_raw_diff_col
                )
            } else {
                format!("{} AND ({}) AND ({})", pk_not_null, no_diffs, any_raw)
            }
        }
        Some("same") => {
            // All is_raw_diff_* = 0 (truly identical)
            let no_raw = raw_diff_cols
                .iter()
                .map(|c| format!("\"{}\" = 0", c))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("{} AND ({})", pk_not_null, no_raw)
        }
        _ => {
            // Default: "diffs" — at least one is_diff_* = 1
            let diff_filter = if let Some(ref col) = column_filter {
                format!("\"is_diff_{}\" = 1", col)
            } else {
                compare_cols
                    .iter()
                    .map(|c| format!("\"{}\" = 1", c))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            };
            format!("{} AND ({})", pk_not_null, diff_filter)
        }
    };

    // Count total matching rows
    let count_sql = format!("SELECT COUNT(*) FROM _diff_join WHERE {}", where_clause);
    let total: i64 = activity::query_row_logged(
        &conn,
        &count_sql,
        "get_diff_rows_count",
        &log,
        |row| row.get(0),
    )
    .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

    // Get all non-metadata columns (pk + value columns, excluding is_diff_* and is_raw_diff_*)
    let mut col_stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '_diff_join' \
             AND column_name NOT LIKE 'is_diff_%' \
             AND column_name NOT LIKE 'is_raw_diff_%' \
             ORDER BY ordinal_position",
        )
        .map_err(|e| e.to_string())?;

    let columns: Vec<String> = col_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    // Include is_diff and is_raw_diff columns so frontend can highlight cells
    let all_columns: Vec<String> = {
        let mut all = columns.clone();
        all.extend(compare_cols.iter().cloned());
        all.extend(raw_diff_cols.iter().cloned());
        all
    };

    // SECURITY: Column names come from information_schema (trusted), so quoting
    // them is defensive rather than strictly necessary. But we quote them anyway
    // to handle column names with spaces or reserved words.
    let select_cols = all_columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "")))
        .collect::<Vec<_>>()
        .join(", ");
    let offset = page * page_size;

    let order_by = pk_a_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT {} FROM _diff_join WHERE {} ORDER BY {} LIMIT {} OFFSET {}",
        select_cols, where_clause, order_by, page_size, offset
    );

    let start = std::time::Instant::now();
    let rows =
        diff::keys::query_to_rows_public(&conn, &sql, &all_columns).map_err(|e| e.to_string())?;
    let duration = start.elapsed().as_millis() as u64;
    log.log_query(
        "get_diff_rows",
        &sql,
        duration,
        Some(rows.len() as i64),
        None,
    );

    Ok(PagedRows {
        columns: all_columns,
        rows,
        total,
        page,
        page_size,
    })
}

// ─── Connection Management Commands ─────────────────────────────────────────

/// List all saved database connections.
#[tauri::command]
pub fn list_saved_connections(
    app_handle: tauri::AppHandle,
) -> Result<Vec<SavedConnection>, String> {
    let path = connections::get_connections_path(&app_handle);
    connections::list_connections(&path).map_err(|e| e.to_string())
}

/// Save (create or update) a database connection.
/// Password is stored separately in the OS keychain.
#[tauri::command]
pub fn save_connection(
    app_handle: tauri::AppHandle,
    conn: SavedConnection,
    password: Option<String>,
) -> Result<(), String> {
    let path = connections::get_connections_path(&app_handle);
    connections::save_connection(&path, conn, password).map_err(|e| {
        eprintln!("Save connection error: {}", e);
        e.to_string()
    })
}

/// Delete a saved connection by ID.
#[tauri::command]
pub fn delete_connection(app_handle: tauri::AppHandle, id: String) -> Result<(), String> {
    let path = connections::get_connections_path(&app_handle);
    connections::delete_connection(&path, &id).map_err(|e| e.to_string())
}

/// Test a database connection by attempting a simple query.
#[tauri::command]
pub fn test_connection(
    conn: SavedConnection,
    password: Option<String>,
    state: State<DuckDbState>,
) -> Result<String, String> {
    let duck_conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    connections::test_connection(&duck_conn, &conn, password.as_deref()).map_err(|e| {
        let msg = e.to_string();
        eprintln!("Test connection error: {}", msg);
        if msg.contains("connection") || msg.contains("authentication") || msg.contains("password")
        {
            "Connection failed. Check your host, credentials, and network.".to_string()
        } else {
            msg
        }
    })
}

/// Load data from a saved connection into DuckDB as source_a or source_b.
///
/// Retrieves the password from the OS keychain, builds the connection string,
/// and delegates to the existing db_loader infrastructure.
#[tauri::command]
pub fn load_from_saved_connection(
    id: String,
    query: String,
    label: String,
    app_handle: tauri::AppHandle,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<TableMeta, String> {
    // SECURITY: Validate label
    if label != "a" && label != "b" {
        return Err("Invalid label: must be 'a' or 'b'".to_string());
    }

    let table_name = format!("source_{}", label);

    // Look up the saved connection
    let path = connections::get_connections_path(&app_handle);
    let all_connections = connections::list_connections(&path).map_err(|e| e.to_string())?;
    let saved = all_connections
        .iter()
        .find(|c| c.id == id)
        .ok_or_else(|| format!("Connection '{}' not found", id))?;

    // Retrieve password from keychain
    let password = connections::get_password(&id).map_err(|e| {
        eprintln!("Keyring error: {}", e);
        "Failed to retrieve stored password. You may need to re-enter it.".to_string()
    })?;

    // Build connection string
    let conn_string = connections::build_connection_string(saved, password.as_deref());
    if conn_string.trim().is_empty() {
        return Err("Could not build connection string — check connection settings.".to_string());
    }

    // Determine database type for DuckDB extension
    let db_type = match saved.db_type.as_str() {
        "postgres" => db_loader::DatabaseType::Postgres,
        "mysql" => db_loader::DatabaseType::MySQL,
        _ => {
            return Err(format!(
                "Database type '{}' not yet supported for loading",
                saved.db_type
            ))
        }
    };

    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    db_loader::load_from_database(&conn, &conn_string, &query, &table_name, &db_type, &log)
        .map_err(|e: DiffDonkeyError| {
            let msg = e.to_string();
            eprintln!("Database load error: {}", msg);
            if msg.contains("connection")
                || msg.contains("authentication")
                || msg.contains("password")
            {
                "Database connection failed. Check your connection settings and credentials."
                    .to_string()
            } else {
                e.into()
            }
        })
}

// ─── Activity Log Commands ──────────────────────────────────────────────────

/// Get all SQL query log entries.
#[tauri::command]
pub fn get_activity_log(log: State<ActivityLog>) -> Vec<activity::QueryLogEntry> {
    log.get_entries()
}

/// Clear the SQL query log.
#[tauri::command]
pub fn clear_activity_log(log: State<ActivityLog>) {
    log.clear();
}
