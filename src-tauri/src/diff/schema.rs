/// Schema comparison — analyzes column overlap between two loaded tables.
///
/// This is the equivalent of comparing two `DESCRIBE TABLE` outputs in
/// Snowflake to see which columns exist in both, which are exclusive,
/// and whether the types match.
use duckdb::Connection;

use crate::error::DiffDonkeyError;
use crate::types::{ColumnInfo, SchemaComparison, SharedColumnInfo};

/// Compare schemas of source_a and source_b.
///
/// Returns lists of shared columns (with type match info), columns only in A,
/// and columns only in B. This drives the Columns tab in the UI.
pub fn compare_schemas(conn: &Connection) -> Result<SchemaComparison, DiffDonkeyError> {
    let cols_a = get_columns(conn, "source_a")?;
    let cols_b = get_columns(conn, "source_b")?;

    // Build lookup maps: column_name → data_type
    // In Python this would be: {col.name: col.data_type for col in cols_a}
    let map_a: std::collections::HashMap<&str, &str> = cols_a
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();
    let map_b: std::collections::HashMap<&str, &str> = cols_b
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();

    let mut shared = Vec::new();
    let mut only_in_a = Vec::new();
    let mut only_in_b = Vec::new();

    // Check each column in A
    for col in &cols_a {
        if let Some(&type_b) = map_b.get(col.name.as_str()) {
            shared.push(SharedColumnInfo {
                name: col.name.clone(),
                type_a: col.data_type.clone(),
                type_b: type_b.to_string(),
                types_match: col.data_type == type_b,
            });
        } else {
            only_in_a.push(col.clone());
        }
    }

    // Check columns only in B
    for col in &cols_b {
        if !map_a.contains_key(col.name.as_str()) {
            only_in_b.push(col.clone());
        }
    }

    Ok(SchemaComparison {
        shared,
        only_in_a,
        only_in_b,
    })
}

/// Get columns for a single table from information_schema.
pub fn get_columns(conn: &Connection, table_name: &str) -> Result<Vec<ColumnInfo>, DiffDonkeyError> {
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
    )?;

    let columns = stmt
        .query_map([table_name], |row| {
            Ok(ColumnInfo {
                name: row.get(0)?,
                data_type: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::ActivityLog;
    use crate::loader;
    use duckdb::Connection;

    fn setup_test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        let log = ActivityLog::new();
        loader::load_csv(&conn, "../test-data/orders_a.csv", "source_a", &log).unwrap();
        loader::load_csv(&conn, "../test-data/orders_b.csv", "source_b", &log).unwrap();
        conn
    }

    #[test]
    fn test_compare_schemas_identical_columns() {
        let conn = setup_test_conn();
        let comparison = compare_schemas(&conn).unwrap();

        // Both CSVs have the same 5 columns
        assert_eq!(comparison.shared.len(), 5);
        assert!(comparison.only_in_a.is_empty());
        assert!(comparison.only_in_b.is_empty());

        // All types should match since the CSVs have the same structure
        assert!(comparison.shared.iter().all(|c| c.types_match));
    }

    #[test]
    fn test_compare_schemas_column_names() {
        let conn = setup_test_conn();
        let comparison = compare_schemas(&conn).unwrap();

        let names: Vec<&str> = comparison.shared.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"customer_name"));
        assert!(names.contains(&"amount"));
        assert!(names.contains(&"status"));
        assert!(names.contains(&"created_at"));
    }
}
