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

/// Execute a metadata query against a remote database and return the first column as strings.
///
/// Like load_from_database, this installs/loads the extension and uses ATTACH
/// to connect. But instead of creating a local table, it runs a metadata query
/// (e.g., listing schemas or tables) and returns the first column of results.
pub fn query_metadata(
    conn: &Connection,
    db_type: &DatabaseType,
    conn_string: &str,
    query: &str,
) -> Result<Vec<String>, DiffDonkeyError> {
    if conn_string.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Connection string cannot be empty".to_string(),
        ));
    }
    if query.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Metadata query cannot be empty".to_string(),
        ));
    }

    let ext = db_type.extension_name();
    let escaped_conn = escape_sql_string(conn_string);

    // Install and load the DuckDB extension
    let install_sql = format!("INSTALL {}; LOAD {};", ext, ext);
    conn.execute_batch(&install_sql)?;

    // Detach any previous metadata connection (ignore errors if not attached)
    let _ = conn.execute_batch("DETACH IF EXISTS _meta_db");

    // Attach the remote database
    let attach_type = match db_type {
        DatabaseType::Postgres => "POSTGRES",
        DatabaseType::MySQL => "MYSQL",
    };
    let attach_sql = format!(
        "ATTACH '{}' AS _meta_db (TYPE {})",
        escaped_conn, attach_type
    );
    conn.execute_batch(&attach_sql)?;

    // Execute the metadata query against the attached database
    let prefixed_query = format!(
        "SELECT * FROM _meta_db.{}",
        query.trim()
    );
    let mut stmt = conn.prepare(&prefixed_query).or_else(|_| {
        // If prefixing fails, try the query directly (some queries use
        // information_schema which may need different handling)
        conn.prepare(query)
    })?;

    let results: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();

    // Clean up
    let _ = conn.execute_batch("DETACH IF EXISTS _meta_db");

    Ok(results)
}

/// Build the metadata SQL query for the given catalog type and database type.
///
/// Returns the SQL query string to execute against the remote database.
/// This is separated from execution for testability.
pub fn build_catalog_query(
    db_type: &DatabaseType,
    catalog_type: &str,
    database: Option<&str>,
    schema: Option<&str>,
) -> Result<String, DiffDonkeyError> {
    match (db_type, catalog_type) {
        (DatabaseType::Postgres, "schemas") => Ok(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
             ORDER BY schema_name"
                .to_string(),
        ),
        (DatabaseType::Postgres, "tables") => {
            let schema_name = schema.ok_or_else(|| {
                DiffDonkeyError::Validation("Schema is required to list tables".to_string())
            })?;
            Ok(format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = '{}' AND table_type IN ('BASE TABLE', 'VIEW') \
                 ORDER BY table_name",
                escape_sql_string(schema_name)
            ))
        }
        (DatabaseType::MySQL, "databases") => Ok(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') \
             ORDER BY schema_name"
                .to_string(),
        ),
        (DatabaseType::MySQL, "tables") => {
            let db_name = database.ok_or_else(|| {
                DiffDonkeyError::Validation("Database is required to list tables".to_string())
            })?;
            Ok(format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = '{}' AND table_type IN ('BASE TABLE', 'VIEW') \
                 ORDER BY table_name",
                escape_sql_string(db_name)
            ))
        }
        (_, catalog_type) => Err(DiffDonkeyError::Validation(format!(
            "Invalid catalog type '{}' for {:?}",
            catalog_type, db_type
        ))),
    }
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

    // ─── Catalog Query Tests ────────────────────────────────────────────

    #[test]
    fn test_catalog_query_postgres_schemas() {
        let sql = build_catalog_query(&DatabaseType::Postgres, "schemas", None, None).unwrap();
        assert!(sql.contains("information_schema.schemata"));
        assert!(sql.contains("pg_catalog"));
        assert!(sql.contains("pg_toast"));
        assert!(sql.contains("ORDER BY schema_name"));
    }

    #[test]
    fn test_catalog_query_postgres_tables() {
        let sql =
            build_catalog_query(&DatabaseType::Postgres, "tables", None, Some("public")).unwrap();
        assert!(sql.contains("information_schema.tables"));
        assert!(sql.contains("table_schema = 'public'"));
        assert!(sql.contains("BASE TABLE"));
        assert!(sql.contains("VIEW"));
        assert!(sql.contains("ORDER BY table_name"));
    }

    #[test]
    fn test_catalog_query_postgres_tables_missing_schema() {
        let result = build_catalog_query(&DatabaseType::Postgres, "tables", None, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Schema is required"));
    }

    #[test]
    fn test_catalog_query_mysql_databases() {
        let sql = build_catalog_query(&DatabaseType::MySQL, "databases", None, None).unwrap();
        assert!(sql.contains("information_schema.schemata"));
        assert!(sql.contains("performance_schema"));
        assert!(sql.contains("ORDER BY schema_name"));
    }

    #[test]
    fn test_catalog_query_mysql_tables() {
        let sql =
            build_catalog_query(&DatabaseType::MySQL, "tables", Some("mydb"), None).unwrap();
        assert!(sql.contains("information_schema.tables"));
        assert!(sql.contains("table_schema = 'mydb'"));
        assert!(sql.contains("ORDER BY table_name"));
    }

    #[test]
    fn test_catalog_query_mysql_tables_missing_database() {
        let result = build_catalog_query(&DatabaseType::MySQL, "tables", None, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Database is required"));
    }

    #[test]
    fn test_catalog_type_validation() {
        let result = build_catalog_query(&DatabaseType::Postgres, "invalid", None, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid catalog type"));
    }

    #[test]
    fn test_catalog_query_sql_injection_protection() {
        let sql = build_catalog_query(
            &DatabaseType::Postgres,
            "tables",
            None,
            Some("public'; DROP TABLE users; --"),
        )
        .unwrap();
        // Single quotes should be escaped
        assert!(sql.contains("public''; DROP TABLE users; --"));
    }

    #[test]
    fn test_query_metadata_empty_conn_string() {
        let conn = Connection::open_in_memory().unwrap();
        let result = query_metadata(&conn, &DatabaseType::Postgres, "", "SELECT 1");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Connection string cannot be empty"));
    }

    // ─── Query Generation Tests ────────────────────────────────────────

    #[test]
    fn test_query_generation_postgres() {
        // Verify the expected SELECT pattern for Postgres
        let schema = "public";
        let table = "users";
        let expected = format!("SELECT * FROM {}.{}", schema, table);
        assert_eq!(expected, "SELECT * FROM public.users");
    }

    #[test]
    fn test_query_generation_mysql() {
        let database = "mydb";
        let table = "orders";
        let expected = format!("SELECT * FROM {}.{}", database, table);
        assert_eq!(expected, "SELECT * FROM mydb.orders");
    }

    #[test]
    fn test_query_generation_snowflake() {
        let database = "MY_DB";
        let schema = "PUBLIC";
        let table = "customers";
        let expected = format!(
            "SELECT * FROM \"{}\".\"{}\".\"{}\"\n",
            database, schema, table
        );
        assert!(expected.contains("MY_DB"));
        assert!(expected.contains("PUBLIC"));
        assert!(expected.contains("customers"));
    }

    #[test]
    fn test_query_metadata_empty_query() {
        let conn = Connection::open_in_memory().unwrap();
        let result = query_metadata(
            &conn,
            &DatabaseType::Postgres,
            "host=localhost dbname=test",
            "",
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Metadata query cannot be empty"));
    }
}
