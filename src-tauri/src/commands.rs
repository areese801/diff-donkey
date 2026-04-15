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
use crate::query_history::{self, QueryHistoryEntry};
use crate::remote_loader::{self, RemoteCredentials};
use crate::remote_profiles::{self, RemoteSecrets, SavedRemoteProfile};
use crate::snowflake;
use crate::types::{
    ColumnTolerance, DiffConfig, OverviewResult, PagedRows, PkMode, SchemaComparison, TableMeta,
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
    app_handle: tauri::AppHandle,
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

    let result = db_loader::load_from_database(&conn, &conn_string, &query, &table_name, &db_type, &log).map_err(
        |e: DiffDonkeyError| {
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
    );

    // Auto-save query to history on success (no connection_id for manual mode)
    if result.is_ok() {
        let history_path = query_history::get_history_path(&app_handle);
        let _ = query_history::add_to_history(&history_path, None, &query);
    }

    result
}

/// Load a remote file (S3, GCS, or HTTP URL) into DuckDB as source_a or source_b.
///
/// Uses DuckDB's httpfs extension to stream remote Parquet/CSV files directly.
#[tauri::command]
pub fn load_remote_source(
    uri: String,
    label: String,
    credentials: Option<RemoteCredentials>,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<TableMeta, String> {
    // SECURITY: Validate label is exactly "a" or "b"
    if label != "a" && label != "b" {
        return Err("Invalid label: must be 'a' or 'b'".to_string());
    }

    let table_name = format!("source_{}", label);
    let creds = credentials.unwrap_or_default();

    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    remote_loader::load_remote(&conn, &uri, &table_name, &creds, &log)
        .map_err(|e: DiffDonkeyError| {
            let msg = e.to_string();
            eprintln!("Remote load error: {}", msg);
            if msg.contains("authentication") || msg.contains("credentials") || msg.contains("Access Denied") {
                "Remote access failed. Check your credentials and permissions.".to_string()
            } else {
                e.into()
            }
        })
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

    // Resolve PK mode: expression takes priority if provided
    let has_expression = config
        .pk_expression
        .as_ref()
        .map_or(false, |e| !e.trim().is_empty());
    let has_columns = !config.pk_columns.is_empty();

    let pk_mode = if has_expression && has_columns {
        return Err(
            "Provide either pk_columns or pk_expression, not both".to_string()
        );
    } else if has_expression {
        let expr = config.pk_expression.as_ref().unwrap().trim().to_string();
        // Basic validation: reject semicolons to prevent multi-statement injection
        if expr.contains(';') {
            return Err("PK expression must not contain semicolons".to_string());
        }
        // Validate the expression by running it against both source tables
        let test_sql = format!("SELECT ({}) FROM source_a LIMIT 0", expr);
        conn.execute(&test_sql, []).map_err(|e| {
            format!("Invalid PK expression against source_a: {}", e)
        })?;
        let test_sql_b = format!("SELECT ({}) FROM source_b LIMIT 0", expr);
        conn.execute(&test_sql_b, []).map_err(|e| {
            format!("Invalid PK expression against source_b: {}", e)
        })?;
        PkMode::Expression { expression: expr }
    } else if has_columns {
        // SECURITY: Validate each PK column exists in both source tables.
        for pk in &config.pk_columns {
            validate_column_exists(&conn, "source_a", pk)?;
            validate_column_exists(&conn, "source_b", pk)?;
        }
        PkMode::Columns {
            columns: config.pk_columns.clone(),
        }
    } else {
        return Err("At least one primary key column or a PK expression is required".to_string());
    };

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

    // Get the raw PK column names for filtering shared columns
    let pk_col_names: Vec<String> = match &pk_mode {
        PkMode::Columns { columns } => columns.clone(),
        PkMode::Expression { .. } => vec![], // expression doesn't exclude any columns
    };

    // Get shared columns (excluding PKs and ignored columns) to compare
    let schema = diff::schema::compare_schemas(&conn).map_err(|e| e.to_string())?;
    let ignored = config.ignored_columns.unwrap_or_default();
    let compare_columns: Vec<String> = schema
        .shared
        .iter()
        .filter(|c| !pk_col_names.contains(&c.name) && !ignored.contains(&c.name))
        .map(|c| c.name.clone())
        .collect();

    // Build column type map from shared columns (use type_a)
    let column_types: std::collections::HashMap<String, String> = schema
        .shared
        .iter()
        .filter(|c| !pk_col_names.contains(&c.name) && !ignored.contains(&c.name))
        .map(|c| (c.name.clone(), c.type_a.clone()))
        .collect();

    // Store PK mode as JSON in _diff_meta for later retrieval by get_exclusive_rows etc.
    let pk_mode_json = serde_json::to_string(&pk_mode).map_err(|e| e.to_string())?;
    conn.execute(
        "CREATE OR REPLACE TEMPORARY TABLE _diff_meta AS SELECT ? as pk_columns",
        [&pk_mode_json],
    )
    .map_err(|e| e.to_string())?;

    diff::stats::run_diff(
        &conn,
        &pk_mode,
        &compare_columns,
        &column_types,
        config.tolerance,
        &column_tolerances,
        &config.where_clause,
        &ignored,
        &log,
    )
    .map_err(|e: DiffDonkeyError| e.into())
}

/// Helper: get PK mode from the stored diff metadata.
///
/// Handles both the new PkMode format and the legacy Vec<String> format
/// (for backward compatibility with any in-flight sessions).
fn get_pk_mode(conn: &duckdb::Connection) -> Result<PkMode, String> {
    let json_str: String = conn
        .query_row("SELECT pk_columns FROM _diff_meta", [], |row| row.get(0))
        .map_err(|_| "Diff not run yet — run diff first".to_string())?;

    // Try parsing as PkMode first, fall back to legacy Vec<String>
    if let Ok(mode) = serde_json::from_str::<PkMode>(&json_str) {
        return Ok(mode);
    }
    let columns: Vec<String> =
        serde_json::from_str(&json_str).map_err(|e| format!("Failed to parse PK metadata: {}", e))?;
    Ok(PkMode::Columns { columns })
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
    let pk_mode = get_pk_mode(&conn)?;

    diff::keys::get_exclusive_rows(&conn, &side, &pk_mode, page, page_size, &log)
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
    let pk_mode = get_pk_mode(&conn)?;

    diff::keys::get_duplicate_pks(&conn, &side, &pk_mode, page, page_size, &log)
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
    let total: i64 =
        activity::query_row_logged(&conn, &count_sql, "get_diff_rows_count", &log, |row| {
            row.get(0)
        })
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

/// Load data from Snowflake into DuckDB as source_a or source_b.
///
/// Uses the Snowflake REST API (not DuckDB extensions) to authenticate,
/// execute a query, and load results into a local DuckDB table.
#[tauri::command]
pub async fn load_snowflake_source(
    account_url: String,
    username: String,
    auth_method: String,
    password: Option<String>,
    private_key_path: Option<String>,
    warehouse: Option<String>,
    role: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    query: String,
    label: String,
    app_handle: tauri::AppHandle,
    state: State<'_, DuckDbState>,
    log: State<'_, ActivityLog>,
) -> Result<TableMeta, String> {
    // SECURITY: Validate label is exactly "a" or "b"
    if label != "a" && label != "b" {
        return Err("Invalid label: must be 'a' or 'b'".to_string());
    }

    let table_name = format!("source_{}", label);

    let config = snowflake::SnowflakeConfig {
        account_url,
        warehouse,
        role,
        database,
        schema,
    };

    let auth = match auth_method.as_str() {
        "keypair" => {
            let key_path = private_key_path
                .ok_or("Private key path is required for key-pair authentication")?;
            let pem = std::fs::read_to_string(&key_path).map_err(|e| {
                eprintln!("Failed to read private key: {}", e);
                "Failed to read private key file. Check the file path and permissions.".to_string()
            })?;
            snowflake::SnowflakeAuth::KeyPair {
                username,
                private_key_pem: pem,
            }
        }
        _ => snowflake::SnowflakeAuth::Password {
            username,
            password: password.unwrap_or_default(),
        },
    };

    // Phase 1: HTTP — authenticate and fetch results (no DuckDB lock held)
    let result = snowflake::fetch_snowflake(config, auth, &query)
        .await
        .map_err(|e: DiffDonkeyError| {
            let msg = e.to_string();
            eprintln!("Snowflake fetch error: {}", msg);
            if msg.contains("authentication") || msg.contains("401") || msg.contains("token") {
                "Snowflake authentication failed. Check your credentials.".to_string()
            } else {
                e.into()
            }
        })?;

    // Phase 2: DuckDB — lock and load results into local table
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    let load_result = snowflake::load_results_to_duckdb(&conn, &result, &table_name, &log)
        .map_err(|e: DiffDonkeyError| e.into());

    // Auto-save query to history on success (no connection_id for manual Snowflake)
    if load_result.is_ok() {
        let history_path = query_history::get_history_path(&app_handle);
        let _ = query_history::add_to_history(&history_path, None, &query);
    }

    load_result
}

// ─── Export Commands ────────────────────────────────────────────────────────

/// Export diff rows to a file (CSV, Parquet, or JSON) using DuckDB's COPY TO.
///
/// Reuses the same WHERE clause logic as get_diff_rows so the export
/// respects the current row_filter and column_filter selections.
#[tauri::command]
pub fn export_diff_rows(
    filepath: String,
    format: String,
    column_filter: Option<String>,
    row_filter: Option<String>,
    state: State<DuckDbState>,
    log: State<ActivityLog>,
) -> Result<i64, String> {
    // Validate format
    if !["csv", "parquet", "json"].contains(&format.as_str()) {
        return Err(format!(
            "Invalid export format: '{}'. Must be csv, parquet, or json.",
            format
        ));
    }

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

    // Get is_diff_* columns
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

    // Validate column_filter
    if let Some(ref col) = column_filter {
        let expected_is_diff = format!("is_diff_{}", col);
        if !compare_cols.contains(&expected_is_diff) {
            return Err(format!("Invalid column filter: '{}'", col));
        }
    }

    // Build PK NOT NULL check
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

    // Build WHERE clause (same logic as get_diff_rows)
    let where_clause = match row_filter.as_deref() {
        Some("all") => pk_not_null.clone(),
        Some("minor") => {
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
            let no_raw = raw_diff_cols
                .iter()
                .map(|c| format!("\"{}\" = 0", c))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("{} AND ({})", pk_not_null, no_raw)
        }
        _ => {
            // Default: "diffs"
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

    // Build SELECT columns — exclude is_diff_* and is_raw_diff_* metadata
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

    let select_cols = columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "")))
        .collect::<Vec<_>>()
        .join(", ");

    let order_by = pk_a_cols
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // SECURITY: Escape filepath (double single quotes)
    let escaped_path = filepath.replace('\'', "''");

    // Build format options — HEADER only applies to CSV
    let format_options = match format.as_str() {
        "csv" => "FORMAT CSV, HEADER true".to_string(),
        "parquet" => "FORMAT PARQUET".to_string(),
        "json" => "FORMAT JSON".to_string(),
        _ => unreachable!(),
    };

    let copy_sql = format!(
        "COPY (SELECT {} FROM _diff_join WHERE {} ORDER BY {}) TO '{}' ({})",
        select_cols, where_clause, order_by, escaped_path, format_options
    );

    activity::execute_logged(&conn, &copy_sql, "export_diff_rows", &log)
        .map_err(|e| e.to_string())?;

    // Count exported rows
    let count_sql = format!("SELECT COUNT(*) FROM _diff_join WHERE {}", where_clause);
    let count: i64 =
        activity::query_row_logged(&conn, &count_sql, "export_diff_rows_count", &log, |row| {
            row.get(0)
        })
        .map_err(|e| e.to_string())?;

    Ok(count)
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
/// SSH password is stored under a separate keychain entry.
#[tauri::command]
pub fn save_connection(
    app_handle: tauri::AppHandle,
    conn: SavedConnection,
    password: Option<String>,
    ssh_password: Option<String>,
) -> Result<(), String> {
    let path = connections::get_connections_path(&app_handle);
    connections::save_connection(&path, conn, password, ssh_password).map_err(|e| {
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
/// If SSH tunneling is enabled, establishes a tunnel first.
#[tauri::command]
pub fn test_connection(
    conn: SavedConnection,
    password: Option<String>,
    ssh_password: Option<String>,
    state: State<DuckDbState>,
) -> Result<String, String> {
    let duck_conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    connections::test_connection(
        &duck_conn,
        &conn,
        password.as_deref(),
        ssh_password.as_deref(),
    )
    .map_err(|e| {
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
/// For Snowflake connections, uses the REST API client instead.
#[tauri::command]
pub async fn load_from_saved_connection(
    id: String,
    query: String,
    label: String,
    app_handle: tauri::AppHandle,
    state: State<'_, DuckDbState>,
    log: State<'_, ActivityLog>,
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

    // Snowflake uses its own REST API — not DuckDB extensions
    if saved.db_type == "snowflake" {
        let account_url = saved
            .account_url
            .as_deref()
            .ok_or("Account URL is required for Snowflake connections")?;
        let username = saved
            .username
            .as_deref()
            .ok_or("Username is required for Snowflake connections")?;

        let sf_config = snowflake::SnowflakeConfig {
            account_url: account_url.to_string(),
            warehouse: saved.warehouse.clone(),
            role: saved.role.clone(),
            database: saved.database.clone(),
            schema: saved.schema.clone(),
        };

        let sf_auth = connections::build_snowflake_auth(saved, username, password.as_deref())
            .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

        // Phase 1: HTTP — fetch results without holding DuckDB lock
        let result = snowflake::fetch_snowflake(sf_config, sf_auth, &query)
            .await
            .map_err(|e: DiffDonkeyError| {
                let msg = e.to_string();
                eprintln!("Snowflake fetch error: {}", msg);
                if msg.contains("authentication") || msg.contains("401") {
                    "Snowflake authentication failed. Check your credentials.".to_string()
                } else {
                    e.into()
                }
            })?;

        // Phase 2: DuckDB — lock and load
        let conn = state
            .conn
            .lock()
            .map_err(|e| format!("Lock error: {}", e))?;

        let load_result = snowflake::load_results_to_duckdb(&conn, &result, &table_name, &log)
            .map_err(|e: DiffDonkeyError| e.into());

        if load_result.is_ok() {
            let history_path = query_history::get_history_path(&app_handle);
            let _ = query_history::add_to_history(&history_path, Some(&id), &query);
        }

        return load_result;
    }

    // If SSH tunneling is enabled, establish tunnel and rewrite host/port
    let _tunnel: Option<crate::ssh_tunnel::SshTunnel>;
    let effective_conn: SavedConnection;

    if saved.ssh_enabled {
        // Retrieve SSH password from keychain
        let ssh_password = connections::get_ssh_password(&id).map_err(|e| {
            eprintln!("Keyring error (SSH): {}", e);
            "Failed to retrieve stored SSH password.".to_string()
        })?;

        let tunnel_config = crate::ssh_tunnel::build_tunnel_config(saved, ssh_password)
            .map_err(|e: DiffDonkeyError| -> String { e.into() })?;
        let tunnel = crate::ssh_tunnel::start_tunnel(&tunnel_config)
            .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

        let mut tunneled = saved.clone();
        tunneled.host = Some("127.0.0.1".to_string());
        tunneled.port = Some(tunnel.local_port);
        _tunnel = Some(tunnel);
        effective_conn = tunneled;
    } else {
        _tunnel = None;
        effective_conn = saved.clone();
    }

    // Build connection string for postgres/mysql
    let conn_string = connections::build_connection_string(&effective_conn, password.as_deref());
    if conn_string.trim().is_empty() {
        return Err("Could not build connection string — check connection settings.".to_string());
    }

    // Determine database type for DuckDB extension
    let db_type = match effective_conn.db_type.as_str() {
        "postgres" => db_loader::DatabaseType::Postgres,
        "mysql" => db_loader::DatabaseType::MySQL,
        _ => {
            return Err(format!(
                "Database type '{}' not yet supported for loading",
                effective_conn.db_type
            ))
        }
    };

    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    let result =
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
            });

    // Auto-save query to history on success
    if result.is_ok() {
        let history_path = query_history::get_history_path(&app_handle);
        let _ = query_history::add_to_history(&history_path, Some(&id), &query);
    }

    // _tunnel is dropped here, which signals the background thread to stop
    result
}

// ─── Connection Import / Export Commands ─────────────────────────────────────

/// Export all saved connections to a JSON file (no passwords, no IDs).
/// The frontend provides the file path via the save dialog.
#[tauri::command]
pub fn export_connections_to_file(
    path: String,
    app_handle: tauri::AppHandle,
) -> Result<usize, String> {
    let connections_path = connections::get_connections_path(&app_handle);
    let export = connections::export_connections(&connections_path).map_err(|e| e.to_string())?;
    let count = export.connections.len();
    connections::write_export_file(&export, std::path::Path::new(&path))
        .map_err(|e| e.to_string())?;
    Ok(count)
}

/// Import connections from a JSON file. Skips duplicates by name.
/// The frontend provides the file path via the open dialog.
#[tauri::command]
pub fn import_connections_from_file(
    path: String,
    app_handle: tauri::AppHandle,
) -> Result<connections::ImportResult, String> {
    let export_data =
        connections::read_export_file(std::path::Path::new(&path)).map_err(|e| e.to_string())?;
    let connections_path = connections::get_connections_path(&app_handle);
    connections::import_connections(&connections_path, &export_data).map_err(|e| e.to_string())
}
// ─── Catalog Browsing Commands ───────────────────────────────────────────────

/// A catalog item returned by list_catalog (database, schema, or table name).
#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogItem {
    pub name: String,
}

/// List databases, schemas, or tables for a saved connection.
///
/// This enables the "Browse Tables" UI — cascading dropdowns that let users
/// pick a table instead of typing SQL manually.
///
/// Supports SSH tunnels using the same pattern as load_from_saved_connection.
#[tauri::command]
pub async fn list_catalog(
    connection_id: String,
    catalog_type: String,
    database: Option<String>,
    schema: Option<String>,
    app_handle: tauri::AppHandle,
    state: State<'_, DuckDbState>,
) -> Result<Vec<CatalogItem>, String> {
    // Validate catalog_type
    let valid_types = ["schemas", "tables", "databases"];
    if !valid_types.contains(&catalog_type.as_str()) {
        return Err(format!(
            "Invalid catalog type '{}'. Must be one of: {}",
            catalog_type,
            valid_types.join(", ")
        ));
    }

    // Look up the saved connection
    let path = connections::get_connections_path(&app_handle);
    let all_connections = connections::list_connections(&path).map_err(|e| e.to_string())?;
    let saved = all_connections
        .iter()
        .find(|c| c.id == connection_id)
        .ok_or_else(|| format!("Connection '{}' not found", connection_id))?;

    // Retrieve password from keychain
    let password = connections::get_password(&connection_id).map_err(|e| {
        eprintln!("Keyring error: {}", e);
        "Failed to retrieve stored password.".to_string()
    })?;

    // Snowflake uses its own REST API
    if saved.db_type == "snowflake" {
        let account_url = saved
            .account_url
            .as_deref()
            .ok_or("Account URL is required for Snowflake connections")?;
        let username = saved
            .username
            .as_deref()
            .ok_or("Username is required for Snowflake connections")?;

        let sf_config = snowflake::SnowflakeConfig {
            account_url: account_url.to_string(),
            warehouse: saved.warehouse.clone(),
            role: saved.role.clone(),
            database: saved.database.clone(),
            schema: saved.schema.clone(),
        };

        let sf_auth = connections::build_snowflake_auth(saved, username, password.as_deref())
            .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

        let query = snowflake::build_snowflake_catalog_query(
            &catalog_type,
            database.as_deref(),
            schema.as_deref(),
        )
        .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

        let names = snowflake::fetch_snowflake_metadata(sf_config, sf_auth, &query)
            .await
            .map_err(|e: DiffDonkeyError| {
                let msg = e.to_string();
                eprintln!("Snowflake catalog error: {}", msg);
                if msg.contains("authentication") || msg.contains("401") {
                    "Snowflake authentication failed. Check your credentials.".to_string()
                } else {
                    "Failed to browse catalog. Check the console for details.".to_string()
                }
            })?;

        return Ok(names.into_iter().map(|name| CatalogItem { name }).collect());
    }

    // Postgres/MySQL path — uses DuckDB extensions

    // If SSH tunneling is enabled, establish tunnel and rewrite host/port
    let _tunnel: Option<crate::ssh_tunnel::SshTunnel>;
    let effective_conn: connections::SavedConnection;

    if saved.ssh_enabled {
        let ssh_password = connections::get_ssh_password(&connection_id).map_err(|e| {
            eprintln!("Keyring error (SSH): {}", e);
            "Failed to retrieve stored SSH password.".to_string()
        })?;

        let tunnel_config = crate::ssh_tunnel::build_tunnel_config(saved, ssh_password)
            .map_err(|e: DiffDonkeyError| -> String { e.into() })?;
        let tunnel = crate::ssh_tunnel::start_tunnel(&tunnel_config)
            .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

        let mut tunneled = saved.clone();
        tunneled.host = Some("127.0.0.1".to_string());
        tunneled.port = Some(tunnel.local_port);
        _tunnel = Some(tunnel);
        effective_conn = tunneled;
    } else {
        _tunnel = None;
        effective_conn = saved.clone();
    }

    // Build connection string
    let conn_string = connections::build_connection_string(&effective_conn, password.as_deref());
    if conn_string.trim().is_empty() {
        return Err("Could not build connection string — check connection settings.".to_string());
    }

    // Determine database type
    let db_type = match effective_conn.db_type.as_str() {
        "postgres" => db_loader::DatabaseType::Postgres,
        "mysql" => db_loader::DatabaseType::MySQL,
        _ => {
            return Err(format!(
                "Database type '{}' not supported for catalog browsing",
                effective_conn.db_type
            ))
        }
    };

    // Build and execute the metadata query
    let query = db_loader::build_catalog_query(
        &db_type,
        &catalog_type,
        database.as_deref(),
        schema.as_deref(),
    )
    .map_err(|e: DiffDonkeyError| -> String { e.into() })?;

    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

    let names = db_loader::query_metadata(&conn, &db_type, &conn_string, &query).map_err(
        |e: DiffDonkeyError| {
            let msg = e.to_string();
            eprintln!("Catalog query error: {}", msg);
            "Failed to browse catalog. Check connection and permissions.".to_string()
        },
    )?;

    // _tunnel is dropped here
    Ok(names.into_iter().map(|name| CatalogItem { name }).collect())
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

#[cfg(test)]
mod tests {
    use crate::activity::ActivityLog;
    use crate::diff;
    use crate::loader;
    use crate::types::PkMode;
    use duckdb::Connection;
    use std::collections::HashMap;

    fn test_log() -> ActivityLog {
        ActivityLog::new()
    }

    /// Set up a DuckDB connection with test data loaded and diff already run,
    /// so _diff_join and _diff_meta tables exist for export tests.
    fn setup_diff_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        let log = test_log();
        loader::load_csv(&conn, "../test-data/orders_a.csv", "source_a", &log).unwrap();
        loader::load_csv(&conn, "../test-data/orders_b.csv", "source_b", &log).unwrap();

        let pk_columns = vec!["id".to_string()];
        let compare_cols = vec![
            "customer_name".to_string(),
            "amount".to_string(),
            "status".to_string(),
            "created_at".to_string(),
        ];
        let column_types: HashMap<String, String> = [
            ("customer_name", "VARCHAR"),
            ("amount", "DOUBLE"),
            ("status", "VARCHAR"),
            ("created_at", "DATE"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        // Store PK metadata
        let pk_json = serde_json::to_string(&pk_columns).unwrap();
        conn.execute(
            "CREATE OR REPLACE TEMPORARY TABLE _diff_meta AS SELECT ? as pk_columns",
            [&pk_json],
        )
        .unwrap();

        let no_col_tol = HashMap::new();
        diff::stats::run_diff(
            &conn,
            &PkMode::Columns { columns: pk_columns.clone() },
            &compare_cols,
            &column_types,
            None,
            &no_col_tol,
            &None,
            &[],
            &log,
        )
        .unwrap();

        conn
    }

    /// Helper: run export_diff_rows logic directly against a connection
    /// (bypasses Tauri State wrapper, tests the SQL logic).
    fn run_export(
        conn: &Connection,
        filepath: &str,
        format: &str,
        column_filter: Option<&str>,
        row_filter: Option<&str>,
    ) -> Result<i64, String> {
        let log = test_log();

        // Validate format
        if !["csv", "parquet", "json"].contains(&format) {
            return Err(format!(
                "Invalid export format: '{}'. Must be csv, parquet, or json.",
                format
            ));
        }

        if let Some(rf) = row_filter {
            if !["all", "diffs", "minor", "same"].contains(&rf) {
                return Err(format!("Invalid row filter: '{}'", rf));
            }
        }

        // Get is_diff_* columns
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

        let where_clause = match row_filter {
            Some("all") => pk_not_null.clone(),
            Some("diffs") | None => {
                let diff_filter = if let Some(col) = column_filter {
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
            Some("minor") => {
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
                format!("{} AND ({}) AND ({})", pk_not_null, no_diffs, any_raw)
            }
            Some("same") => {
                let no_raw = raw_diff_cols
                    .iter()
                    .map(|c| format!("\"{}\" = 0", c))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("{} AND ({})", pk_not_null, no_raw)
            }
            _ => unreachable!(),
        };

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

        let select_cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "")))
            .collect::<Vec<_>>()
            .join(", ");
        let order_by = pk_a_cols
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let escaped_path = filepath.replace('\'', "''");
        let format_options = match format {
            "csv" => "FORMAT CSV, HEADER true",
            "parquet" => "FORMAT PARQUET",
            "json" => "FORMAT JSON",
            _ => unreachable!(),
        };

        let copy_sql = format!(
            "COPY (SELECT {} FROM _diff_join WHERE {} ORDER BY {}) TO '{}' ({})",
            select_cols, where_clause, order_by, escaped_path, format_options
        );

        crate::activity::execute_logged(conn, &copy_sql, "export_diff_rows", &log)
            .map_err(|e| e.to_string())?;

        let count_sql = format!("SELECT COUNT(*) FROM _diff_join WHERE {}", where_clause);
        let count: i64 = conn
            .query_row(&count_sql, [], |row| row.get(0))
            .map_err(|e| e.to_string())?;

        Ok(count)
    }

    #[test]
    fn test_export_csv() {
        let conn = setup_diff_conn();
        let dir = std::env::temp_dir();
        let path = dir.join("test_export.csv");
        let path_str = path.to_str().unwrap();

        let count = run_export(&conn, path_str, "csv", None, Some("all")).unwrap();
        assert!(count > 0, "Should export at least one row");
        assert!(path.exists(), "CSV file should exist");
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains(","), "CSV should contain commas");
        // Header line + data lines
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len() as i64, count + 1, "Lines should be header + row count");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_export_parquet() {
        let conn = setup_diff_conn();
        let dir = std::env::temp_dir();
        let path = dir.join("test_export.parquet");
        let path_str = path.to_str().unwrap();

        let count = run_export(&conn, path_str, "parquet", None, Some("all")).unwrap();
        assert!(count > 0, "Should export at least one row");
        assert!(path.exists(), "Parquet file should exist");
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0, "Parquet file should have content");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_export_invalid_format() {
        let result = run_export(
            &Connection::open_in_memory().unwrap(),
            "/tmp/test.xlsx",
            "xlsx",
            None,
            Some("all"),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid export format"));
    }

    #[test]
    fn test_export_with_row_filter() {
        let conn = setup_diff_conn();
        let dir = std::env::temp_dir();

        // Export all rows
        let path_all = dir.join("test_export_all.csv");
        let count_all = run_export(&conn, path_all.to_str().unwrap(), "csv", None, Some("all")).unwrap();

        // Export only diffs
        let path_diffs = dir.join("test_export_diffs.csv");
        let count_diffs =
            run_export(&conn, path_diffs.to_str().unwrap(), "csv", None, Some("diffs")).unwrap();

        // Diffs should be a subset of all
        assert!(count_diffs <= count_all, "Diff rows should be <= all rows");
        assert!(count_diffs > 0, "Should have some diff rows in test data");

        std::fs::remove_file(&path_all).ok();
        std::fs::remove_file(&path_diffs).ok();
    }
}

// ─── Query History Commands ─────────────────────────────────────────────────

/// Get query history entries, optionally filtered by connection ID.
#[tauri::command]
pub fn get_query_history(
    connection_id: Option<String>,
    app_handle: tauri::AppHandle,
) -> Result<Vec<QueryHistoryEntry>, String> {
    let path = query_history::get_history_path(&app_handle);
    query_history::list_history(&path, connection_id.as_deref()).map_err(|e| e.to_string())
}

/// Delete a single query history entry by ID.
#[tauri::command]
pub fn delete_query_history_entry(
    id: String,
    app_handle: tauri::AppHandle,
) -> Result<(), String> {
    let path = query_history::get_history_path(&app_handle);
    query_history::delete_history_entry(&path, &id).map_err(|e| e.to_string())
}

/// Clear query history, optionally for a specific connection.
#[tauri::command]
pub fn clear_query_history(
    connection_id: Option<String>,
    app_handle: tauri::AppHandle,
) -> Result<(), String> {
    let path = query_history::get_history_path(&app_handle);
    query_history::clear_history(&path, connection_id.as_deref()).map_err(|e| e.to_string())
}

// ─── Remote profiles ────────────────────────────────────────────────

#[tauri::command]
pub fn list_remote_profiles(
    app_handle: tauri::AppHandle,
) -> Result<Vec<SavedRemoteProfile>, String> {
    let path = remote_profiles::get_remote_profiles_path(&app_handle);
    remote_profiles::list_profiles(&path).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn save_remote_profile(
    app_handle: tauri::AppHandle,
    profile: SavedRemoteProfile,
    secrets: RemoteSecrets,
) -> Result<(), String> {
    let path = remote_profiles::get_remote_profiles_path(&app_handle);
    remote_profiles::save_profile(&path, profile, secrets).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn delete_remote_profile(
    app_handle: tauri::AppHandle,
    id: String,
) -> Result<(), String> {
    let path = remote_profiles::get_remote_profiles_path(&app_handle);
    remote_profiles::delete_profile(&path, &id).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn get_remote_profile_secrets(
    app_handle: tauri::AppHandle,
    id: String,
) -> Result<RemoteSecrets, String> {
    let path = remote_profiles::get_remote_profiles_path(&app_handle);
    remote_profiles::get_profile_secrets(&path, &id).map_err(|e| e.to_string())
}
