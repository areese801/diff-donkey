/// Primary key analysis — exclusive rows, duplicates, and null PKs.
///
/// These queries run against the _diff_join materialized table
/// (created by stats.rs) and the original source tables.
use duckdb::Connection;

use crate::error::DiffDonkeyError;
use crate::types::PagedRows;

/// Get rows exclusive to one side (exist in A but not B, or vice versa).
///
/// In SQL terms:
///   Exclusive to A: WHERE pk_b IS NULL (LEFT side of FULL OUTER JOIN)
///   Exclusive to B: WHERE pk_a IS NULL (RIGHT side of FULL OUTER JOIN)
pub fn get_exclusive_rows(
    conn: &Connection,
    side: &str,
    pk_columns: &[String],
    page: usize,
    page_size: usize,
) -> Result<PagedRows, DiffDonkeyError> {
    let (source_table, null_side) = match side {
        "a" => ("source_a", "b"),
        "b" => ("source_b", "a"),
        _ => return Err(DiffDonkeyError::Validation(format!("Invalid side: {}", side))),
    };

    // All opposite-side PK columns must be NULL
    let null_check = pk_columns.iter()
        .map(|pk| format!("\"pk_{}_{}\" IS NULL", pk, null_side))
        .collect::<Vec<_>>().join(" AND ");

    let total: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM _diff_join WHERE {}", null_check),
        [], |row| row.get(0),
    )?;

    let offset = page * page_size;
    let columns = get_table_columns(conn, source_table)?;

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

    let sql = format!(
        "SELECT s.* FROM {} s \
         INNER JOIN (SELECT {} FROM _diff_join WHERE {} LIMIT {} OFFSET {}) excl \
         ON {} ORDER BY {}",
        source_table, pk_select, null_check, page_size, offset, join_conds, order_by
    );

    let rows = query_to_rows(conn, &sql, &columns)?;

    Ok(PagedRows { columns, rows, total, page, page_size })
}

/// Get duplicate primary keys in a source table.
///
/// Shows PKs that appear more than once — a data quality issue.
/// In SQL: SELECT pk, COUNT(*) FROM source GROUP BY pk HAVING COUNT(*) > 1
pub fn get_duplicate_pks(
    conn: &Connection,
    side: &str,
    pk_columns: &[String],
    page: usize,
    page_size: usize,
) -> Result<PagedRows, DiffDonkeyError> {
    let source_table = match side {
        "a" => "source_a",
        "b" => "source_b",
        _ => return Err(DiffDonkeyError::Validation(format!("Invalid side: {}", side))),
    };

    let group_cols = pk_columns.iter()
        .map(|pk| format!("\"{}\"", pk))
        .collect::<Vec<_>>().join(", ");

    let total: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM (SELECT {} FROM {} GROUP BY {} HAVING COUNT(*) > 1)",
            group_cols, source_table, group_cols
        ),
        [], |row| row.get(0),
    )?;

    let offset = page * page_size;
    let mut columns: Vec<String> = pk_columns.to_vec();
    columns.push("count".to_string());

    let order_cols = pk_columns.iter()
        .map(|pk| format!("\"{}\"", pk))
        .collect::<Vec<_>>().join(", ");

    let sql = format!(
        "SELECT {}, COUNT(*) as count FROM {} GROUP BY {} HAVING COUNT(*) > 1 \
         ORDER BY count DESC, {} LIMIT {} OFFSET {}",
        group_cols, source_table, group_cols, order_cols, page_size, offset
    );

    let rows = query_to_rows(conn, &sql, &columns)?;

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
    // Try i64 first (covers INTEGER, BIGINT)
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }
    // Try f64 (covers DOUBLE, FLOAT, DECIMAL)
    if let Ok(v) = row.get::<_, f64>(idx) {
        if let Some(n) = serde_json::Number::from_f64(v) {
            return serde_json::Value::Number(n);
        }
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
    use crate::diff::stats;
    use crate::loader;
    use duckdb::Connection;

    fn setup_diff_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        loader::load_csv(&conn, "../test-data/orders_a.csv", "source_a").unwrap();
        loader::load_csv(&conn, "../test-data/orders_b.csv", "source_b").unwrap();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];
        stats::run_diff(&conn, &["id".to_string()], &compare_cols, &std::collections::HashMap::new(), None, &std::collections::HashMap::<String, crate::types::ColumnTolerance>::new()).unwrap();
        conn
    }

    #[test]
    fn test_exclusive_rows_a() {
        let conn = setup_diff_conn();
        let result = get_exclusive_rows(&conn, "a", &["id".to_string()], 0, 50).unwrap();

        // Row 8 (Henry Wilson) only exists in A
        assert_eq!(result.total, 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_exclusive_rows_b() {
        let conn = setup_diff_conn();
        let result = get_exclusive_rows(&conn, "b", &["id".to_string()], 0, 50).unwrap();

        // Row 11 (Karen Martinez) only exists in B
        assert_eq!(result.total, 1);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_no_duplicate_pks() {
        let conn = setup_diff_conn();
        let result = get_duplicate_pks(&conn, "a", &["id".to_string()], 0, 50).unwrap();

        assert_eq!(result.total, 0);
        assert_eq!(result.rows.len(), 0);
    }
}
