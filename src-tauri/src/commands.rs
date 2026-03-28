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

use crate::db::DuckDbState;
use crate::diff;
use crate::error::DiffDonkeyError;
use crate::loader;
use crate::types::{ColumnTolerance, DiffConfig, OverviewResult, PagedRows, SchemaComparison, TableMeta};

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
        loader::load_parquet(&conn, &path, &table_name)
    } else {
        loader::load_csv(&conn, &path, &table_name)
    };

    result.map_err(|e: DiffDonkeyError| e.into())
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
pub fn run_diff(config: DiffConfig, state: State<DuckDbState>) -> Result<OverviewResult, String> {
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

    // Validate global precision
    if let Some(prec) = config.tolerance {
        if prec < 0 {
            return Err("Decimal places must be a non-negative integer".to_string());
        }
    }

    // Validate per-column tolerances
    let column_tolerances = config.column_tolerances.unwrap_or_default();
    for (col, tol) in &column_tolerances {
        match tol {
            ColumnTolerance::Precision { precision } if *precision < 0 => {
                return Err(format!(
                    "Precision for column '{}' must be non-negative",
                    col
                ));
            }
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
    )
    .map_err(|e: DiffDonkeyError| e.into())
}

/// Helper: get PK column names from the stored diff metadata.
fn get_pk_columns(conn: &duckdb::Connection) -> Result<Vec<String>, String> {
    let json_str: String = conn
        .query_row("SELECT pk_columns FROM _diff_meta", [], |row| row.get(0))
        .map_err(|_| "Diff not run yet — run diff first".to_string())?;
    serde_json::from_str(&json_str)
        .map_err(|e| format!("Failed to parse PK columns: {}", e))
}

/// Get exclusive rows — rows that exist only in one side.
#[tauri::command]
pub fn get_exclusive_rows(
    side: String,
    page: usize,
    page_size: usize,
    state: State<DuckDbState>,
) -> Result<PagedRows, String> {
    let page_size = page_size.min(MAX_PAGE_SIZE);
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;
    let pks = get_pk_columns(&conn)?;

    diff::keys::get_exclusive_rows(&conn, &side, &pks, page, page_size)
        .map_err(|e: DiffDonkeyError| e.into())
}

/// Get duplicate primary keys for one side.
#[tauri::command]
pub fn get_duplicate_pks(
    side: String,
    page: usize,
    page_size: usize,
    state: State<DuckDbState>,
) -> Result<PagedRows, String> {
    let page_size = page_size.min(MAX_PAGE_SIZE);
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;
    let pks = get_pk_columns(&conn)?;

    diff::keys::get_duplicate_pks(&conn, &side, &pks, page, page_size)
        .map_err(|e: DiffDonkeyError| e.into())
}

/// Get diff rows — matched rows where values differ, with pagination.
/// Optional column_filter limits to rows where a specific column differs.
#[tauri::command]
pub fn get_diff_rows(
    page: usize,
    page_size: usize,
    column_filter: Option<String>,
    state: State<DuckDbState>,
) -> Result<PagedRows, String> {
    let page_size = page_size.min(MAX_PAGE_SIZE);
    let conn = state
        .conn
        .lock()
        .map_err(|e| format!("Lock error: {}", e))?;

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

    // SECURITY: Validate column_filter against the known column list.
    // Without this, a crafted column_filter like "x = 1; DROP TABLE source_a; --"
    // would be interpolated directly into the WHERE clause.
    let diff_filter = if let Some(ref col) = column_filter {
        let expected_is_diff = format!("is_diff_{}", col);
        if !compare_cols.contains(&expected_is_diff) {
            return Err(format!("Invalid column filter: '{}'", col));
        }
        // Safe: col is validated to be a known column name from the schema
        format!("\"{}\" = 1", expected_is_diff)
    } else {
        compare_cols
            .iter()
            .map(|c| format!("\"{}\" = 1", c))
            .collect::<Vec<_>>()
            .join(" OR ")
    };

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

    let where_clause = format!("{} AND ({})", pk_not_null, diff_filter);

    // Count total matching rows
    let total: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM _diff_join WHERE {}", where_clause),
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;

    // Get all non-is_diff columns (pk + value columns)
    let mut col_stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = '_diff_join' AND column_name NOT LIKE 'is_diff_%' \
             ORDER BY ordinal_position",
        )
        .map_err(|e| e.to_string())?;

    let columns: Vec<String> = col_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    // Also include is_diff columns so frontend knows which cells to highlight
    let all_columns: Vec<String> = {
        let mut all = columns.clone();
        all.extend(compare_cols.iter().cloned());
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

    let rows = diff::keys::query_to_rows_public(&conn, &sql, &all_columns)
        .map_err(|e| e.to_string())?;

    Ok(PagedRows {
        columns: all_columns,
        rows,
        total,
        page,
        page_size,
    })
}
