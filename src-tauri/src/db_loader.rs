/// Database loading — executes a SQL query against a remote database via DuckDB extensions
/// and materializes the result into a local DuckDB table.
///
/// DuckDB's postgres and mysql extensions allow querying remote databases directly:
///   postgres_query(conn_string, query) → table
///   mysql_query(conn_string, query) → table
///
/// The result is stored as source_a or source_b, and then the existing diff engine
/// works unchanged — it only cares that the tables exist with columns.
use duckdb::Connection;

use crate::activity::{self, ActivityLog};
use crate::error::DiffDonkeyError;
use crate::types::{ColumnInfo, TableMeta};

/// Supported database types for remote loading.
#[derive(Debug, serde::Deserialize, Clone)]
pub enum DatabaseType {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "mysql")]
    MySQL,
}

impl DatabaseType {
    /// Returns the DuckDB extension name to INSTALL and LOAD.
    pub fn extension_name(&self) -> &'static str {
        match self {
            DatabaseType::Postgres => "postgres",
            DatabaseType::MySQL => "mysql",
        }
    }

    /// Returns the DuckDB function name for querying the remote database.
    pub fn query_function(&self) -> &'static str {
        match self {
            DatabaseType::Postgres => "postgres_query",
            DatabaseType::MySQL => "mysql_query",
        }
    }
}

/// SECURITY: Escape a string for use in a SQL single-quoted literal.
/// Doubles any single quotes to prevent SQL injection.
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Load data from a remote database into a local DuckDB table.
///
/// Steps:
/// 1. Install and load the appropriate DuckDB extension (postgres or mysql)
/// 2. Execute the user's query against the remote database
/// 3. Materialize the result as a local table (source_a or source_b)
/// 4. Return table metadata (row count, columns, types)
pub fn load_from_database(
    conn: &Connection,
    conn_string: &str,
    query: &str,
    table_name: &str,
    db_type: &DatabaseType,
    log: &ActivityLog,
) -> Result<TableMeta, DiffDonkeyError> {
    // Validate inputs are not empty
    if conn_string.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Connection string cannot be empty".to_string(),
        ));
    }
    if query.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "SQL query cannot be empty".to_string(),
        ));
    }

    let ext = db_type.extension_name();
    let query_fn = db_type.query_function();

    // Install and load the DuckDB extension
    let install_sql = format!("INSTALL {}; LOAD {};", ext, ext);
    activity::execute_logged(conn, &install_sql, "install_db_extension", log)?;

    // SECURITY: Escape single quotes in connection string and query to prevent
    // SQL injection. The connection string and query come from the local desktop
    // user (not a web browser), but we still sanitize as defense in depth.
    let escaped_conn = escape_sql_string(conn_string);
    let escaped_query = escape_sql_string(query);

    let create_sql = format!(
        "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM {}('{}', '{}')",
        table_name, query_fn, escaped_conn, escaped_query
    );
    activity::execute_logged(conn, &create_sql, "load_from_database", log)?;

    get_table_meta(conn, table_name)
}

/// Query table metadata — row count and column info.
pub fn get_table_meta(conn: &Connection, table_name: &str) -> Result<TableMeta, DiffDonkeyError> {
    let row_count: usize = conn.query_row(
        &format!("SELECT COUNT(*) FROM {}", table_name),
        [],
        |row| row.get(0),
    )?;

    let mut stmt = conn.prepare(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = ? ORDER BY ordinal_position",
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
    fn test_load_from_database_empty_conn_string() {
        let conn = Connection::open_in_memory().unwrap();
        let result = load_from_database(&conn, "", "SELECT 1", "source_a", &DatabaseType::Postgres, &test_log());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Connection string cannot be empty"));
    }

    #[test]
    fn test_load_from_database_whitespace_conn_string() {
        let conn = Connection::open_in_memory().unwrap();
        let result =
            load_from_database(&conn, "   ", "SELECT 1", "source_a", &DatabaseType::Postgres, &test_log());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Connection string cannot be empty"));
    }

    #[test]
    fn test_load_from_database_empty_query() {
        let conn = Connection::open_in_memory().unwrap();
        let result = load_from_database(
            &conn,
            "host=localhost dbname=test",
            "",
            "source_a",
            &DatabaseType::Postgres,
            &test_log(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("SQL query cannot be empty"));
    }

    #[test]
    fn test_load_from_database_whitespace_query() {
        let conn = Connection::open_in_memory().unwrap();
        let result = load_from_database(
            &conn,
            "host=localhost dbname=test",
            "   ",
            "source_a",
            &DatabaseType::MySQL,
            &test_log(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("SQL query cannot be empty"));
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("no quotes"), "no quotes");
        assert_eq!(escape_sql_string("it's"), "it''s");
        assert_eq!(escape_sql_string("a'b'c"), "a''b''c");
    }

    #[test]
    fn test_database_type_extension_names() {
        assert_eq!(DatabaseType::Postgres.extension_name(), "postgres");
        assert_eq!(DatabaseType::MySQL.extension_name(), "mysql");
    }

    #[test]
    fn test_database_type_query_functions() {
        assert_eq!(DatabaseType::Postgres.query_function(), "postgres_query");
        assert_eq!(DatabaseType::MySQL.query_function(), "mysql_query");
    }
}
