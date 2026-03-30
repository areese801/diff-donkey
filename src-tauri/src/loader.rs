/// File loading — gets CSV/Parquet files into DuckDB tables.
///
/// DuckDB's `read_csv_auto` is like Snowflake's `INFER_SCHEMA` + `COPY INTO`
/// combined — it auto-detects column types, delimiters, and headers.
use duckdb::Connection;

use crate::activity::{self, ActivityLog};
use crate::error::DiffDonkeyError;
use crate::types::{ColumnInfo, TableMeta};

/// SECURITY: Escape a string for use in a SQL single-quoted literal.
/// Doubles any single quotes to prevent SQL injection via file paths.
/// A file named `it's.csv` becomes `it''s.csv` in SQL.
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Load a CSV file into a DuckDB table.
///
/// Under the hood this runs:
///   CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{path}')
///
/// `read_csv_auto` handles type inference automatically — integers, floats,
/// dates, strings are all detected. Similar to pandas `read_csv()` but
/// running inside a SQL engine.
pub fn load_csv(conn: &Connection, path: &str, table_name: &str, log: &ActivityLog) -> Result<TableMeta, DiffDonkeyError> {
    // Validate the file exists before asking DuckDB to read it
    if !std::path::Path::new(path).exists() {
        return Err(DiffDonkeyError::Validation("File not found".to_string()));
    }

    // SECURITY: Escape single quotes in path to prevent SQL injection.
    // A path like "/tmp/x'); DROP TABLE y; --" would break out of the string literal.
    let escaped_path = escape_sql_string(path);

    // CREATE OR REPLACE so reloading the same source works without errors
    let sql = format!(
        "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}')",
        table_name, escaped_path
    );
    activity::execute_logged(conn, &sql, "load_csv", log)?;

    get_table_meta(conn, table_name)
}

/// Load a Parquet file into a DuckDB table.
/// DuckDB reads Parquet natively — no extra dependencies needed.
pub fn load_parquet(conn: &Connection, path: &str, table_name: &str, log: &ActivityLog) -> Result<TableMeta, DiffDonkeyError> {
    if !std::path::Path::new(path).exists() {
        return Err(DiffDonkeyError::Validation("File not found".to_string()));
    }

    let escaped_path = escape_sql_string(path);

    let sql = format!(
        "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_parquet('{}')",
        table_name, escaped_path
    );
    activity::execute_logged(conn, &sql, "load_parquet", log)?;

    get_table_meta(conn, table_name)
}

/// Query table metadata — row count and column info.
/// Similar to running DESCRIBE + COUNT(*) in Snowflake.
fn get_table_meta(conn: &Connection, table_name: &str) -> Result<TableMeta, DiffDonkeyError> {
    // Get row count
    let row_count: usize = conn.query_row(
        &format!("SELECT COUNT(*) FROM {}", table_name),
        [],
        |row| row.get(0),
    )?;

    // Get column info from information_schema
    // This is the same system table Snowflake/Postgres use.
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
    )?;

    let columns: Vec<ColumnInfo> = stmt
        .query_map([table_name], |row| {
            Ok(ColumnInfo {
                name: row.get(0)?,
                data_type: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TableMeta {
        table_name: table_name.to_string(),
        row_count,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    fn test_log() -> ActivityLog {
        ActivityLog::new()
    }

    #[test]
    fn test_load_csv() {
        let conn = Connection::open_in_memory().unwrap();
        let meta = load_csv(&conn, "../test-data/orders_a.csv", "source_a", &test_log()).unwrap();

        assert_eq!(meta.table_name, "source_a");
        assert_eq!(meta.row_count, 10);
        assert_eq!(meta.columns.len(), 5);
        assert_eq!(meta.columns[0].name, "id");
        assert_eq!(meta.columns[1].name, "customer_name");
    }

    #[test]
    fn test_load_csv_file_not_found() {
        let conn = Connection::open_in_memory().unwrap();
        let result = load_csv(&conn, "nonexistent.csv", "source_a", &test_log());

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("File not found"));
    }

    #[test]
    fn test_load_csv_both_sources() {
        let conn = Connection::open_in_memory().unwrap();
        let log = test_log();

        let meta_a = load_csv(&conn, "../test-data/orders_a.csv", "source_a", &log).unwrap();
        let meta_b = load_csv(&conn, "../test-data/orders_b.csv", "source_b", &log).unwrap();

        assert_eq!(meta_a.row_count, 10);
        assert_eq!(meta_b.row_count, 10);

        // Both tables exist in the same connection — we can query across them
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_a a FULL OUTER JOIN source_b b ON a.id = b.id",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // 10 rows in A, 10 in B, but IDs 1-7,9,10 overlap and 8 is A-only, 11 is B-only
        assert_eq!(count, 11);
    }
}
