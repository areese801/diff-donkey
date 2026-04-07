/// Primary key analysis — exclusive rows, duplicates, and null PKs.
///
/// These queries run against the _diff_join materialized table
/// (created by stats.rs) and the original source tables.
use duckdb::Connection;

use crate::activity::ActivityLog;
use crate::error::DiffDonkeyError;
use crate::types::{PagedRows, PkMode};

/// Get rows exclusive to one side (exist in A but not B, or vice versa).
///
/// In SQL terms:
///   Exclusive to A: WHERE pk_b IS NULL (LEFT side of FULL OUTER JOIN)
///   Exclusive to B: WHERE pk_a IS NULL (RIGHT side of FULL OUTER JOIN)
pub fn get_exclusive_rows(
    conn: &Connection,
    side: &str,
    pk_mode: &PkMode,
    page: usize,
    page_size: usize,
    log: &ActivityLog,
) -> Result<PagedRows, DiffDonkeyError> {
    let (source_table, null_side) = match side {
        "a" => ("source_a", "b"),
        "b" => ("source_b", "a"),
        _ => return Err(DiffDonkeyError::Validation(format!("Invalid side: {}", side))),
    };

    let join_keys = pk_mode.join_key_names();

    // All opposite-side PK columns must be NULL
    let null_check = join_keys.iter()
        .map(|k| format!("\"{}_{}\" IS NULL", k, null_side))
        .collect::<Vec<_>>().join(" AND ");

    let total: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM _diff_join WHERE {}", null_check),
        [], |row| row.get(0),
    )?;

    let offset = page * page_size;
    let columns = get_table_columns(conn, source_table)?;

    let sql = match pk_mode {
        PkMode::Columns { columns: pk_columns } => {
            // Select PK columns from _diff_join, join back to source
            let pk_select = pk_columns.iter()
                .map(|pk| format!("\"pk_{}_{}\"", pk, side))
                .collect::<Vec<_>>().join(", ");

            let join_conds = pk_columns.iter()
                .map(|pk| format!("s.\"{}\" = excl.\"pk_{}_{}\"", pk, pk, side))
                .collect::<Vec<_>>().join(" AND ");

            let order_by = pk_columns.iter()
                .map(|pk| format!("s.\"{}\"", pk))
                .collect::<Vec<_>>().join(", ");

            format!(
                "SELECT s.* FROM {} s \
                 INNER JOIN (SELECT {} FROM _diff_join WHERE {} LIMIT {} OFFSET {}) excl \
                 ON {} ORDER BY {}",
                source_table, pk_select, null_check, page_size, offset, join_conds, order_by
            )
        }
        PkMode::Expression { expression } => {
            // In expression mode, use the expression to join back to source.
            // Use ROW_NUMBER to preserve pagination ordering.
            format!(
                "SELECT s.* FROM {src} s \
                 INNER JOIN (\
                   SELECT \"{key}_{side}\" FROM _diff_join WHERE {null_check} LIMIT {limit} OFFSET {offset}\
                 ) excl \
                 ON ({expr}) = excl.\"{key}_{side}\" \
                 ORDER BY ({expr})",
                src = source_table,
                key = join_keys[0],
                side = side,
                null_check = null_check,
                limit = page_size,
                offset = offset,
                expr = expression,
            )
        }
    };

    let start = std::time::Instant::now();
    let rows = query_to_rows(conn, &sql, &columns)?;
    let duration = start.elapsed().as_millis() as u64;
    log.log_query("get_exclusive_rows", &sql, duration, Some(rows.len() as i64), None);

    Ok(PagedRows { columns, rows, total, page, page_size })
}

/// Get duplicate primary keys in a source table.
///
/// Shows PKs that appear more than once — a data quality issue.
/// In SQL: SELECT pk, COUNT(*) FROM source GROUP BY pk HAVING COUNT(*) > 1
pub fn get_duplicate_pks(
    conn: &Connection,
    side: &str,
    pk_mode: &PkMode,
    page: usize,
    page_size: usize,
    log: &ActivityLog,
) -> Result<PagedRows, DiffDonkeyError> {
    let source_table = match side {
        "a" => "source_a",
        "b" => "source_b",
        _ => return Err(DiffDonkeyError::Validation(format!("Invalid side: {}", side))),
    };

    let (total, columns, sql) = match pk_mode {
        PkMode::Columns { columns: pk_columns } => {
            let group_cols = pk_columns.iter()
                .map(|pk| format!("\"{}\"", pk))
                .collect::<Vec<_>>().join(", ");

            let t: i64 = conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM (SELECT {gc} FROM {src} GROUP BY {gc} HAVING COUNT(*) > 1)",
                    gc = group_cols, src = source_table
                ),
                [], |row| row.get(0),
            )?;

            let offset = page * page_size;
            let mut cols: Vec<String> = pk_columns.to_vec();
            cols.push("count".to_string());

            let order_cols = pk_columns.iter()
                .map(|pk| format!("\"{}\"", pk))
                .collect::<Vec<_>>().join(", ");

            let q = format!(
                "SELECT {gc}, COUNT(*) as count FROM {src} GROUP BY {gc} HAVING COUNT(*) > 1 \
                 ORDER BY count DESC, {oc} LIMIT {lim} OFFSET {off}",
                gc = group_cols, src = source_table, oc = order_cols, lim = page_size, off = offset
            );

            (t, cols, q)
        }
        PkMode::Expression { expression } => {
            let t: i64 = conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM (SELECT ({expr}) AS pk_expr FROM {src} GROUP BY pk_expr HAVING COUNT(*) > 1)",
                    expr = expression, src = source_table
                ),
                [], |row| row.get(0),
            )?;

            let offset = page * page_size;
            let cols = vec!["pk_expr".to_string(), "count".to_string()];

            let q = format!(
                "SELECT ({expr}) AS pk_expr, COUNT(*) as count FROM {src} GROUP BY pk_expr HAVING COUNT(*) > 1 \
                 ORDER BY count DESC, pk_expr LIMIT {lim} OFFSET {off}",
                expr = expression, src = source_table, lim = page_size, off = offset
            );

            (t, cols, q)
        }
    };

    let start = std::time::Instant::now();
    let rows = query_to_rows(conn, &sql, &columns)?;
    let duration = start.elapsed().as_millis() as u64;
    log.log_query("get_duplicate_pks", &sql, duration, Some(rows.len() as i64), None);

    Ok(PagedRows { columns, rows, total, page, page_size })
}

/// Helper: get column names for a table.
fn get_table_columns(conn: &Connection, table_name: &str) -> Result<Vec<String>, DiffDonkeyError> {
    let mut stmt = conn.prepare(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
    )?;

    let columns = stmt
        .query_map([table_name], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(columns)
}

/// Helper: execute a query and return rows as Vec<HashMap>.
///
/// DuckDB returns strongly-typed columns, but we convert everything to
/// JSON-friendly strings/numbers for the frontend. This is like doing
/// `cursor.fetchall()` in Python but converting to dicts.
/// Public version of query_to_rows for use from commands.rs.
pub fn query_to_rows_public(
    conn: &Connection,
    sql: &str,
    columns: &[String],
) -> Result<Vec<std::collections::HashMap<String, serde_json::Value>>, DiffDonkeyError> {
    query_to_rows(conn, sql, columns)
}

fn query_to_rows(
    conn: &Connection,
    sql: &str,
    columns: &[String],
) -> Result<Vec<std::collections::HashMap<String, serde_json::Value>>, DiffDonkeyError> {
    let mut stmt = conn.prepare(sql)?;
    let col_count = columns.len();

    let mut rows_out = Vec::new();
    let mut db_rows = stmt.query([])?;

    while let Some(row) = db_rows.next()? {
        let mut map = std::collections::HashMap::new();
        for i in 0..col_count {
            let value = row_value_to_json(row, i);
            map.insert(columns[i].clone(), value);
        }
        rows_out.push(map);
    }

    Ok(rows_out)
}

/// Convert a DuckDB row value at index to a serde_json::Value.
///
/// Tries multiple types since DuckDB columns can be various types.
/// Falls back to string representation.
fn row_value_to_json(row: &duckdb::Row, idx: usize) -> serde_json::Value {
    // Try f64 first (covers DOUBLE, FLOAT, DECIMAL, and also integers).
    // If the value has no fractional part, emit as i64 for cleaner display.
    // We try f64 before i64 because DuckDB will truncate 150.1234 to 150
    // when reading as i64, losing decimal precision.
    if let Ok(v) = row.get::<_, f64>(idx) {
        if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
            return serde_json::Value::Number((v as i64).into());
        }
        if let Some(n) = serde_json::Number::from_f64(v) {
            return serde_json::Value::Number(n);
        }
    }
    // Try i64 as fallback (covers BIGINT values that can't be represented as f64)
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    // Try bool
    if let Ok(v) = row.get::<_, bool>(idx) {
        return serde_json::Value::Bool(v);
    }
    // Try string (covers VARCHAR, DATE, TIMESTAMP as strings)
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }
    // NULL or unknown type
    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::ActivityLog;
    use crate::diff::stats;
    use crate::loader;
    use duckdb::Connection;

    fn test_log() -> ActivityLog {
        ActivityLog::new()
    }

    fn setup_diff_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        let log = test_log();
        loader::load_csv(&conn, "../test-data/orders_a.csv", "source_a", &log).unwrap();
        loader::load_csv(&conn, "../test-data/orders_b.csv", "source_b", &log).unwrap();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];
        let pk_mode = PkMode::Columns { columns: vec!["id".to_string()] };
        stats::run_diff(&conn, &pk_mode, &compare_cols, &std::collections::HashMap::new(), None, &std::collections::HashMap::<String, crate::types::ColumnTolerance>::new(), &None, &[], &log).unwrap();
        conn
    }

    fn pk_columns_id() -> PkMode {
        PkMode::Columns { columns: vec!["id".to_string()] }
    }

    #[test]
    fn test_exclusive_rows_a() {
        let conn = setup_diff_conn();
        let result = get_exclusive_rows(&conn, "a", &pk_columns_id(), 0, 50, &test_log()).unwrap();

        // Row 8 (Henry Wilson) only exists in A
        assert_eq!(result.total, 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_exclusive_rows_b() {
        let conn = setup_diff_conn();
        let result = get_exclusive_rows(&conn, "b", &pk_columns_id(), 0, 50, &test_log()).unwrap();

        // Row 11 (Karen Martinez) only exists in B
        assert_eq!(result.total, 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_no_duplicate_pks() {
        let conn = setup_diff_conn();
        let result = get_duplicate_pks(&conn, "a", &pk_columns_id(), 0, 50, &test_log()).unwrap();

        assert_eq!(result.total, 0);
        assert_eq!(result.rows.len(), 0);
    }

    // ── PK Expression mode tests ─────────────────────────────────────────

    fn setup_expr_diff_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                ('Alice', 'Smith', 100.0),
                ('Bob', 'Jones', 200.0),
                ('Carol', 'Lee', 300.0)
            ) AS t(first_name, last_name, amount);

            CREATE TABLE source_b AS SELECT * FROM (VALUES
                ('Alice', 'Smith', 100.5),
                ('Bob', 'Jones', 200.0),
                ('Dave', 'Kim', 400.0)
            ) AS t(first_name, last_name, amount);"
        ).unwrap();

        let log = test_log();
        let compare_cols: Vec<String> = vec!["amount".into()];
        let col_types: std::collections::HashMap<String, String> = [("amount", "DOUBLE")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let pk_mode = PkMode::Expression {
            expression: "CONCAT(first_name, '_', last_name)".to_string(),
        };
        stats::run_diff(
            &conn, &pk_mode, &compare_cols, &col_types,
            None, &std::collections::HashMap::<String, crate::types::ColumnTolerance>::new(),
            &None, &[], &log,
        ).unwrap();
        conn
    }

    fn pk_expr_concat() -> PkMode {
        PkMode::Expression {
            expression: "CONCAT(first_name, '_', last_name)".to_string(),
        }
    }

    #[test]
    fn test_pk_expression_exclusive_rows() {
        let conn = setup_expr_diff_conn();

        // Carol_Lee is exclusive to A
        let result_a = get_exclusive_rows(&conn, "a", &pk_expr_concat(), 0, 50, &test_log()).unwrap();
        assert_eq!(result_a.total, 1);
        assert_eq!(result_a.rows.len(), 1);

        // Dave_Kim is exclusive to B
        let result_b = get_exclusive_rows(&conn, "b", &pk_expr_concat(), 0, 50, &test_log()).unwrap();
        assert_eq!(result_b.total, 1);
        assert_eq!(result_b.rows.len(), 1);
    }

    #[test]
    fn test_pk_expression_duplicate_pks() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                ('Alice', 'X', 1.0),
                ('Alice', 'X', 2.0),
                ('Bob', 'Y', 3.0)
            ) AS t(name, tag, amount);

            CREATE TABLE source_b AS SELECT * FROM (VALUES
                ('Alice', 'X', 1.5),
                ('Bob', 'Y', 3.0)
            ) AS t(name, tag, amount);"
        ).unwrap();

        let log = test_log();
        let compare_cols: Vec<String> = vec!["amount".into()];
        let col_types: std::collections::HashMap<String, String> = [("amount", "DOUBLE")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let pk_mode = PkMode::Expression {
            expression: "CONCAT(name, '_', tag)".to_string(),
        };
        stats::run_diff(
            &conn, &pk_mode, &compare_cols, &col_types,
            None, &std::collections::HashMap::<String, crate::types::ColumnTolerance>::new(),
            &None, &[], &log,
        ).unwrap();

        // source_a has Alice_X twice
        let result = get_duplicate_pks(&conn, "a", &pk_mode, 0, 50, &test_log()).unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.rows.len(), 1);

        // source_b has no duplicates
        let result_b = get_duplicate_pks(&conn, "b", &pk_mode, 0, 50, &test_log()).unwrap();
        assert_eq!(result_b.total, 0);
    }
}
