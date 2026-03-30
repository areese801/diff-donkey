/// Core diff engine — the heart of Diff Donkey.
///
/// This implements the Datafold algorithm:
/// 1. FULL OUTER JOIN source_a and source_b on the primary key
/// 2. For each column, compute a diff expression → 0/1
/// 3. Materialize as a temp table (_diff_join)
/// 4. Aggregate: SUM each is_diff column → per-column diff counts
///
/// IS DISTINCT FROM is crucial — unlike `!=`, it handles NULLs correctly:
///   NULL IS DISTINCT FROM NULL  → false (they're "equal")
///   NULL IS DISTINCT FROM 'x'  → true  (they differ)
///
/// Tolerance modes override IS DISTINCT FROM with type-specific comparisons:
///   Precision  → ROUND(a, N) = ROUND(b, N)
///   Seconds    → ABS(EPOCH(a) - EPOCH(b)) <= N
///   CaseInsensitive → LOWER(a) IS DISTINCT FROM LOWER(b)
///   Whitespace → TRIM(a) IS DISTINCT FROM TRIM(b)
use std::collections::HashMap;

use duckdb::Connection;

use crate::error::DiffDonkeyError;
use crate::types::{
    ColumnDiffStats, ColumnTolerance, DiffStats, OverviewResult, PkSummary, ValuesSummary,
};

/// Check whether a DuckDB data type is numeric (eligible for precision tolerance).
pub fn is_numeric_type(data_type: &str) -> bool {
    let upper = data_type.to_uppercase();
    matches!(
        upper.as_str(),
        "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "BIGINT"
            | "HUGEINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
            | "UBIGINT"
            | "FLOAT"
            | "DOUBLE"
    ) || upper.starts_with("DECIMAL")
}

/// Check whether a DuckDB data type is a timestamp (eligible for seconds tolerance).
pub fn is_timestamp_type(data_type: &str) -> bool {
    data_type.to_uppercase().starts_with("TIMESTAMP")
}

/// Run the full diff: build the join table and compute all stats.
///
/// This is the main entry point called by the `run_diff` Tauri command.
pub fn run_diff(
    conn: &Connection,
    pk_columns: &[String],
    compare_columns: &[String],
    column_types: &HashMap<String, String>,
    default_precision: Option<i32>,
    column_tolerances: &HashMap<String, ColumnTolerance>,
) -> Result<OverviewResult, DiffDonkeyError> {
    // Step 1: Build the materialized join table
    build_diff_join(
        conn,
        pk_columns,
        compare_columns,
        column_types,
        default_precision,
        column_tolerances,
    )?;

    // Step 2: Compute aggregate stats from the join table
    let diff_stats = compute_diff_stats(conn, compare_columns)?;

    // Step 3: Compute PK-level summary
    let pk_summary = compute_pk_summary(conn, pk_columns)?;

    // Step 4: Compute values-level summary
    let total_rows: i64 = conn.query_row("SELECT COUNT(*) FROM _diff_join", [], |row| row.get(0))?;

    let rows_with_diffs: i64 = if compare_columns.is_empty() {
        0
    } else {
        let any_diff_clause = compare_columns
            .iter()
            .map(|c| format!("\"is_diff_{}\" = 1", c))
            .collect::<Vec<_>>()
            .join(" OR ");

        // Matched rows: all PK columns NOT NULL on both sides
        let matched_filter = pk_columns
            .iter()
            .map(|pk| format!("\"pk_{}_a\" IS NOT NULL", pk))
            .chain(pk_columns.iter().map(|pk| format!("\"pk_{}_b\" IS NOT NULL", pk)))
            .collect::<Vec<_>>()
            .join(" AND ");

        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM _diff_join WHERE {} AND ({})",
                matched_filter, any_diff_clause
            ),
            [],
            |row| row.get(0),
        )?
    };

    let matched_rows = total_rows - pk_summary.exclusive_a - pk_summary.exclusive_b;

    // Compute rows_minor: tolerance-suppressed diffs (no real diffs, but at least one raw diff)
    let rows_minor: i64 = if compare_columns.is_empty() {
        0
    } else {
        let matched_filter = pk_columns
            .iter()
            .map(|pk| format!("\"pk_{}_a\" IS NOT NULL", pk))
            .chain(pk_columns.iter().map(|pk| format!("\"pk_{}_b\" IS NOT NULL", pk)))
            .collect::<Vec<_>>()
            .join(" AND ");

        let no_real_diffs = compare_columns
            .iter()
            .map(|c| format!("\"is_diff_{}\" = 0", c))
            .collect::<Vec<_>>()
            .join(" AND ");
        let any_raw_diffs = compare_columns
            .iter()
            .map(|c| format!("\"is_raw_diff_{}\" = 1", c))
            .collect::<Vec<_>>()
            .join(" OR ");

        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM _diff_join WHERE {} AND ({}) AND ({})",
                matched_filter, no_real_diffs, any_raw_diffs
            ),
            [],
            |row| row.get(0),
        )?
    };

    let values_summary = ValuesSummary {
        total_compared: matched_rows,
        rows_with_diffs,
        rows_minor,
        rows_identical: matched_rows - rows_with_diffs - rows_minor,
    };

    Ok(OverviewResult {
        diff_stats,
        pk_summary,
        values_summary,
        total_rows_a: conn.query_row("SELECT COUNT(*) FROM source_a", [], |row| row.get(0))?,
        total_rows_b: conn.query_row("SELECT COUNT(*) FROM source_b", [], |row| row.get(0))?,
    })
}

/// Build the materialized diff join table.
fn build_diff_join(
    conn: &Connection,
    pk_columns: &[String],
    compare_columns: &[String],
    column_types: &HashMap<String, String>,
    default_precision: Option<i32>,
    column_tolerances: &HashMap<String, ColumnTolerance>,
) -> Result<(), DiffDonkeyError> {
    // PK columns: pk_{name}_a, pk_{name}_b for each PK column
    let mut select_parts = Vec::new();
    for pk in pk_columns {
        select_parts.push(format!("a.\"{}\" as \"pk_{}_a\"", pk, pk));
        select_parts.push(format!("b.\"{}\" as \"pk_{}_b\"", pk, pk));
    }

    for col in compare_columns {
        select_parts.push(format!("a.\"{}\" as \"{}_a\"", col, col));
        select_parts.push(format!("b.\"{}\" as \"{}_b\"", col, col));

        let col_type = column_types
            .get(col.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");

        // Resolve effective tolerance for this column
        let explicit_tol = column_tolerances.get(col.as_str());
        let is_diff_expr = if let Some(tol) = explicit_tol {
            tolerance_sql(col, col_type, tol)
        } else if let Some(prec) = default_precision {
            if is_numeric_type(col_type) {
                tolerance_sql(col, col_type, &ColumnTolerance::Precision { precision: prec })
            } else {
                default_diff_sql(col)
            }
        } else {
            default_diff_sql(col)
        };

        select_parts.push(format!("{} as \"is_diff_{}\"", is_diff_expr, col));

        // Always add raw diff (pure IS DISTINCT FROM, no tolerance)
        select_parts.push(format!(
            "(a.\"{}\" IS DISTINCT FROM b.\"{}\")::INTEGER as \"is_raw_diff_{}\"",
            col, col, col
        ));
    }

    // JOIN clause: ON a."col1" = b."col1" AND a."col2" = b."col2"
    let join_conditions = pk_columns
        .iter()
        .map(|pk| format!("a.\"{}\" = b.\"{}\"", pk, pk))
        .collect::<Vec<_>>()
        .join(" AND ");

    let sql = format!(
        "CREATE OR REPLACE TEMPORARY TABLE _diff_join AS \
         SELECT {} \
         FROM source_a a \
         FULL OUTER JOIN source_b b ON {}",
        select_parts.join(", "),
        join_conditions
    );

    conn.execute_batch(&sql)?;
    Ok(())
}

/// Generate the default IS DISTINCT FROM expression.
fn default_diff_sql(col: &str) -> String {
    format!(
        "(a.\"{}\" IS DISTINCT FROM b.\"{}\")::INTEGER",
        col, col
    )
}

/// Generate a tolerance-aware diff expression for the given column and mode.
fn tolerance_sql(col: &str, col_type: &str, tol: &ColumnTolerance) -> String {
    match tol {
        ColumnTolerance::Precision { precision } if is_numeric_type(col_type) => {
            format!(
                "CASE WHEN a.\"{}\" IS NULL AND b.\"{}\" IS NULL THEN 0 \
                 WHEN a.\"{}\" IS NULL OR b.\"{}\" IS NULL THEN 1 \
                 WHEN ROUND(a.\"{}\"::DOUBLE, {}) = ROUND(b.\"{}\"::DOUBLE, {}) THEN 0 ELSE 1 END",
                col, col, col, col, col, precision, col, precision
            )
        }
        ColumnTolerance::Seconds { seconds } if is_timestamp_type(col_type) => {
            format!(
                "CASE WHEN a.\"{}\" IS NULL AND b.\"{}\" IS NULL THEN 0 \
                 WHEN a.\"{}\" IS NULL OR b.\"{}\" IS NULL THEN 1 \
                 WHEN ABS(EPOCH(a.\"{}\") - EPOCH(b.\"{}\")) <= {} THEN 0 ELSE 1 END",
                col, col, col, col, col, col, seconds
            )
        }
        ColumnTolerance::CaseInsensitive => {
            format!(
                "(LOWER(a.\"{}\") IS DISTINCT FROM LOWER(b.\"{}\"))::INTEGER",
                col, col
            )
        }
        ColumnTolerance::Whitespace => {
            format!(
                "(TRIM(a.\"{}\") IS DISTINCT FROM TRIM(b.\"{}\"))::INTEGER",
                col, col
            )
        }
        ColumnTolerance::CaseInsensitiveWhitespace => {
            format!(
                "(LOWER(TRIM(a.\"{}\")) IS DISTINCT FROM LOWER(TRIM(b.\"{}\")))::INTEGER",
                col, col
            )
        }
        // Type mismatch (e.g., Precision on VARCHAR) — fall through to default
        _ => default_diff_sql(col),
    }
}

/// Compute per-column diff statistics from the materialized join table.
fn compute_diff_stats(
    conn: &Connection,
    compare_columns: &[String],
) -> Result<DiffStats, DiffDonkeyError> {
    if compare_columns.is_empty() {
        return Ok(DiffStats { columns: vec![] });
    }

    let mut agg_parts = Vec::new();
    for col in compare_columns {
        agg_parts.push(format!("SUM(\"is_diff_{col}\")"));
        agg_parts.push(format!("COUNT(*) - SUM(\"is_diff_{col}\")"));
    }

    // Use information_schema to find PK columns dynamically for matched-row filter
    let mut pk_stmt = conn.prepare(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = '_diff_join' AND column_name LIKE 'pk_%_a' \
         ORDER BY ordinal_position",
    )?;
    let pk_a_cols: Vec<String> = pk_stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    let matched_filter = pk_a_cols
        .iter()
        .map(|c| format!("\"{}\" IS NOT NULL", c))
        .chain(pk_a_cols.iter().map(|c| format!("\"{}\" IS NOT NULL", c.replace("_a", "_b"))))
        .collect::<Vec<_>>()
        .join(" AND ");

    let sql = format!(
        "SELECT {} FROM _diff_join WHERE {}",
        agg_parts.join(", "),
        matched_filter
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let row = rows.next()?.ok_or(DiffDonkeyError::Validation(
        "No results from diff stats query".to_string(),
    ))?;

    let mut columns = Vec::new();
    for (i, col) in compare_columns.iter().enumerate() {
        let diff_count: i64 = row.get(i * 2)?;
        let match_count: i64 = row.get(i * 2 + 1)?;
        let total = diff_count + match_count;

        columns.push(ColumnDiffStats {
            name: col.clone(),
            diff_count,
            match_count,
            total,
            match_pct: if total > 0 {
                (match_count as f64 / total as f64) * 100.0
            } else {
                100.0
            },
        });
    }

    Ok(DiffStats { columns })
}

/// Compute primary key summary: exclusive rows and duplicates.
fn compute_pk_summary(conn: &Connection, pk_columns: &[String]) -> Result<PkSummary, DiffDonkeyError> {
    // Exclusive to A: all B-side PK columns are NULL
    let b_null = pk_columns.iter()
        .map(|pk| format!("\"pk_{}_b\" IS NULL", pk))
        .collect::<Vec<_>>().join(" AND ");
    let exclusive_a: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM _diff_join WHERE {}", b_null),
        [],
        |row| row.get(0),
    )?;

    // Exclusive to B: all A-side PK columns are NULL
    let a_null = pk_columns.iter()
        .map(|pk| format!("\"pk_{}_a\" IS NULL", pk))
        .collect::<Vec<_>>().join(" AND ");
    let exclusive_b: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM _diff_join WHERE {}", a_null),
        [],
        |row| row.get(0),
    )?;

    // Duplicate PKs: GROUP BY all PK columns
    let group_cols = pk_columns.iter()
        .map(|pk| format!("\"{}\"", pk))
        .collect::<Vec<_>>().join(", ");

    let duplicate_pks_a: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM (SELECT {} FROM source_a GROUP BY {} HAVING COUNT(*) > 1)",
            group_cols, group_cols
        ),
        [],
        |row| row.get(0),
    )?;

    let duplicate_pks_b: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM (SELECT {} FROM source_b GROUP BY {} HAVING COUNT(*) > 1)",
            group_cols, group_cols
        ),
        [],
        |row| row.get(0),
    )?;

    // Null PKs: any PK column is NULL
    let any_null = pk_columns.iter()
        .map(|pk| format!("\"{}\" IS NULL", pk))
        .collect::<Vec<_>>().join(" OR ");

    let null_pks_a: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM source_a WHERE {}", any_null),
        [],
        |row| row.get(0),
    )?;

    let null_pks_b: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM source_b WHERE {}", any_null),
        [],
        |row| row.get(0),
    )?;

    Ok(PkSummary {
        exclusive_a,
        exclusive_b,
        duplicate_pks_a,
        duplicate_pks_b,
        null_pks_a,
        null_pks_b,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loader;
    use duckdb::Connection;

    fn setup_test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        loader::load_csv(&conn, "../test-data/orders_a.csv", "source_a").unwrap();
        loader::load_csv(&conn, "../test-data/orders_b.csv", "source_b").unwrap();
        conn
    }

    fn get_test_column_types() -> HashMap<String, String> {
        [
            ("customer_name", "VARCHAR"),
            ("amount", "DOUBLE"),
            ("status", "VARCHAR"),
            ("created_at", "DATE"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    // ── Baseline tests (no tolerance) ─────────────────────────────────────

    #[test]
    fn test_run_diff_overview() {
        let conn = setup_test_conn();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];
        let col_types = get_test_column_types();
        let no_col_tol = HashMap::new();

        let result = run_diff(&conn, &["id".to_string()], &compare_cols, &col_types, None, &no_col_tol).unwrap();

        assert_eq!(result.total_rows_a, 10);
        assert_eq!(result.total_rows_b, 10);
        assert_eq!(result.pk_summary.exclusive_a, 1);
        assert_eq!(result.pk_summary.exclusive_b, 1);
        assert_eq!(result.pk_summary.duplicate_pks_a, 0);
        assert_eq!(result.pk_summary.duplicate_pks_b, 0);
    }

    #[test]
    fn test_per_column_diff_counts() {
        let conn = setup_test_conn();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];
        let col_types = get_test_column_types();
        let no_col_tol = HashMap::new();

        let result = run_diff(&conn, &["id".to_string()], &compare_cols, &col_types, None, &no_col_tol).unwrap();

        let name_stats = result.diff_stats.columns.iter().find(|c| c.name == "customer_name").unwrap();
        assert_eq!(name_stats.diff_count, 1); // "Eve Davis" vs "eve davis"

        let amount_stats = result.diff_stats.columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(amount_stats.diff_count, 1); // 275.50 vs 280.00

        let status_stats = result.diff_stats.columns.iter().find(|c| c.name == "status").unwrap();
        assert_eq!(status_stats.diff_count, 2); // pending→shipped, pending→completed

        let date_stats = result.diff_stats.columns.iter().find(|c| c.name == "created_at").unwrap();
        assert_eq!(date_stats.diff_count, 0);
    }

    #[test]
    fn test_values_summary() {
        let conn = setup_test_conn();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];
        let col_types = get_test_column_types();
        let no_col_tol = HashMap::new();

        let result = run_diff(&conn, &["id".to_string()], &compare_cols, &col_types, None, &no_col_tol).unwrap();

        assert_eq!(result.values_summary.total_compared, 9);
        assert_eq!(result.values_summary.rows_with_diffs, 4);
        assert_eq!(result.values_summary.rows_identical, 5);
    }

    // ── Type detection ────────────────────────────────────────────────────

    #[test]
    fn test_is_numeric_type() {
        assert!(is_numeric_type("INTEGER"));
        assert!(is_numeric_type("BIGINT"));
        assert!(is_numeric_type("FLOAT"));
        assert!(is_numeric_type("DOUBLE"));
        assert!(is_numeric_type("TINYINT"));
        assert!(is_numeric_type("SMALLINT"));
        assert!(is_numeric_type("HUGEINT"));
        assert!(is_numeric_type("UTINYINT"));
        assert!(is_numeric_type("USMALLINT"));
        assert!(is_numeric_type("UINTEGER"));
        assert!(is_numeric_type("UBIGINT"));
        assert!(is_numeric_type("DECIMAL(10,2)"));
        assert!(is_numeric_type("decimal(18,4)"));

        assert!(!is_numeric_type("VARCHAR"));
        assert!(!is_numeric_type("DATE"));
        assert!(!is_numeric_type("BOOLEAN"));
        assert!(!is_numeric_type("TIMESTAMP"));
        assert!(!is_numeric_type("BLOB"));
    }

    #[test]
    fn test_is_timestamp_type() {
        assert!(is_timestamp_type("TIMESTAMP"));
        assert!(is_timestamp_type("TIMESTAMPTZ"));
        assert!(is_timestamp_type("TIMESTAMP WITH TIME ZONE"));
        assert!(is_timestamp_type("TIMESTAMP_S"));
        assert!(is_timestamp_type("TIMESTAMP_MS"));

        assert!(!is_timestamp_type("DATE"));
        assert!(!is_timestamp_type("VARCHAR"));
        assert!(!is_timestamp_type("INTEGER"));
    }

    // ── Numeric precision tests ───────────────────────────────────────────

    fn setup_tolerance_tables(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 'Alice', 100.0, 'active'),
                (2, 'Bob',   200.5, 'active'),
                (3, 'Carol', 300.0, 'pending')
            ) AS t(id, name, amount, status);

            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 'Alice', 100.3, 'active'),
                (2, 'Bob',   205.0, 'active'),
                (3, 'Carol', 300.0, 'shipped')
            ) AS t(id, name, amount, status);"
        ).unwrap();
    }

    fn tolerance_column_types() -> HashMap<String, String> {
        [("name", "VARCHAR"), ("amount", "DOUBLE"), ("status", "VARCHAR")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_precision_suppresses_small_diff() {
        let conn = Connection::open_in_memory().unwrap();
        setup_tolerance_tables(&conn);
        let cols: Vec<String> = vec!["name".into(), "amount".into(), "status".into()];
        let col_types = tolerance_column_types();
        let no_col_tol = HashMap::new();

        // precision=0 → round to integer: 100.0→100, 100.3→100 (match); 200.5→201, 205.0→205 (diff)
        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &no_col_tol).unwrap();

        let amount = result.diff_stats.columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(amount.diff_count, 1); // only id=2 differs at integer precision

        // String columns unaffected
        let status = result.diff_stats.columns.iter().find(|c| c.name == "status").unwrap();
        assert_eq!(status.diff_count, 1);
    }

    #[test]
    fn test_precision_round_comparison() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 0.1234),
                (2, 0.1234)
            ) AS t(id, val);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 0.1235),
                (2, 0.1244)
            ) AS t(id, val);"
        ).unwrap();

        let cols: Vec<String> = vec!["val".into()];
        let col_types: HashMap<String, String> =
            [("val".to_string(), "DOUBLE".to_string())].into_iter().collect();
        let no_col_tol = HashMap::new();

        // precision=3 → ROUND(0.1234,3)=0.123, ROUND(0.1235,3)=0.124 (diff!)
        //               ROUND(0.1234,3)=0.123, ROUND(0.1244,3)=0.124 (diff)
        // Actually: ROUND(0.1235,3) in DuckDB uses banker's rounding → 0.124
        // Let's use precision=2 for a clearer test:
        // ROUND(0.1234,2)=0.12, ROUND(0.1235,2)=0.12 → match
        // ROUND(0.1234,2)=0.12, ROUND(0.1244,2)=0.12 → match
        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(2), &no_col_tol).unwrap();
        let val = result.diff_stats.columns.iter().find(|c| c.name == "val").unwrap();
        assert_eq!(val.diff_count, 0); // both match at 2dp

        // Now recreate and test with precision=3
        conn.execute_batch("DROP TABLE source_a; DROP TABLE source_b;").unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 0.1234),
                (2, 0.1234)
            ) AS t(id, val);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 0.1239),
                (2, 0.1254)
            ) AS t(id, val);"
        ).unwrap();

        // precision=3: ROUND(0.1234,3)=0.123, ROUND(0.1239,3)=0.124 → diff
        //              ROUND(0.1234,3)=0.123, ROUND(0.1254,3)=0.125 → diff
        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(3), &no_col_tol).unwrap();
        let val = result.diff_stats.columns.iter().find(|c| c.name == "val").unwrap();
        assert_eq!(val.diff_count, 2);
    }

    #[test]
    fn test_precision_not_applied_to_strings() {
        let conn = Connection::open_in_memory().unwrap();
        setup_tolerance_tables(&conn);
        let cols: Vec<String> = vec!["name".into(), "amount".into(), "status".into()];
        let col_types = tolerance_column_types();
        let no_col_tol = HashMap::new();

        // Even with high precision, string columns use IS DISTINCT FROM
        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(10), &no_col_tol).unwrap();

        let status = result.diff_stats.columns.iter().find(|c| c.name == "status").unwrap();
        assert_eq!(status.diff_count, 1);
    }

    #[test]
    fn test_precision_null_handling() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, NULL),
                (2, NULL),
                (3, 100.0),
                (4, 100.0)
            ) AS t(id, val);

            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, NULL),
                (2, 100.0),
                (3, NULL),
                (4, 100.4)
            ) AS t(id, val);"
        ).unwrap();

        let cols: Vec<String> = vec!["val".into()];
        let col_types: HashMap<String, String> =
            [("val".to_string(), "DOUBLE".to_string())].into_iter().collect();
        let no_col_tol = HashMap::new();

        // precision=0: ROUND(100.0,0)=100, ROUND(100.4,0)=100 → match
        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &no_col_tol).unwrap();
        let val = result.diff_stats.columns.iter().find(|c| c.name == "val").unwrap();
        // id=1: NULL vs NULL → match
        // id=2: NULL vs 100.0 → diff
        // id=3: 100.0 vs NULL → diff
        // id=4: 100.0 vs 100.4 → match (both round to 100)
        assert_eq!(val.diff_count, 2);
        assert_eq!(val.match_count, 2);
    }

    #[test]
    fn test_per_column_precision_override() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 100.0, 500.0),
                (2, 200.0, 600.0)
            ) AS t(id, price, weight);

            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 100.4, 505.0),
                (2, 208.0, 615.0)
            ) AS t(id, price, weight);"
        ).unwrap();

        let cols: Vec<String> = vec!["price".into(), "weight".into()];
        let col_types: HashMap<String, String> = [
            ("price".to_string(), "DOUBLE".to_string()),
            ("weight".to_string(), "DOUBLE".to_string()),
        ].into_iter().collect();

        // Default precision=0 (integer rounding), but weight gets precision=2
        let col_tol: HashMap<String, ColumnTolerance> =
            [("weight".to_string(), ColumnTolerance::Precision { precision: 2 })].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &col_tol).unwrap();

        let price = result.diff_stats.columns.iter().find(|c| c.name == "price").unwrap();
        // precision=0: ROUND(100.0,0)=100, ROUND(100.4,0)=100 → match
        //              ROUND(200.0,0)=200, ROUND(208.0,0)=208 → diff
        assert_eq!(price.diff_count, 1);

        let weight = result.diff_stats.columns.iter().find(|c| c.name == "weight").unwrap();
        // precision=2: ROUND(500.0,2)=500.0, ROUND(505.0,2)=505.0 → diff
        //              ROUND(600.0,2)=600.0, ROUND(615.0,2)=615.0 → diff
        assert_eq!(weight.diff_count, 2);
    }

    #[test]
    fn test_column_tolerance_overrides_default() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 100.0)
            ) AS t(id, amount);

            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 100.4)
            ) AS t(id, amount);"
        ).unwrap();

        let cols: Vec<String> = vec!["amount".into()];
        let col_types: HashMap<String, String> =
            [("amount".to_string(), "DOUBLE".to_string())].into_iter().collect();

        // Default precision=0 (would match: both round to 100), but per-column precision=1 (strict)
        // ROUND(100.0,1)=100.0, ROUND(100.4,1)=100.4 → diff
        let col_tol: HashMap<String, ColumnTolerance> =
            [("amount".to_string(), ColumnTolerance::Precision { precision: 1 })].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &col_tol).unwrap();

        let amount = result.diff_stats.columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(amount.diff_count, 1);
    }

    // ── Timestamp tolerance tests ─────────────────────────────────────────

    #[test]
    fn test_timestamp_tolerance() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, TIMESTAMP '2024-01-01 12:00:00'),
                (2, TIMESTAMP '2024-01-01 12:00:00')
            ) AS t(id, ts);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, TIMESTAMP '2024-01-01 12:00:03'),
                (2, TIMESTAMP '2024-01-01 12:00:10')
            ) AS t(id, ts);"
        ).unwrap();

        let cols: Vec<String> = vec!["ts".into()];
        let col_types: HashMap<String, String> =
            [("ts".to_string(), "TIMESTAMP".to_string())].into_iter().collect();
        let col_tol: HashMap<String, ColumnTolerance> =
            [("ts".to_string(), ColumnTolerance::Seconds { seconds: 5.0 })].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, None, &col_tol).unwrap();
        let ts = result.diff_stats.columns.iter().find(|c| c.name == "ts").unwrap();
        assert_eq!(ts.diff_count, 1);  // id=1 within 5s, id=2 exceeds
        assert_eq!(ts.match_count, 1);
    }

    // ── String tolerance tests ────────────────────────────────────────────

    #[test]
    fn test_case_insensitive() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 'Alice'),
                (2, 'Bob')
            ) AS t(id, name);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 'alice'),
                (2, 'Bobby')
            ) AS t(id, name);"
        ).unwrap();

        let cols: Vec<String> = vec!["name".into()];
        let col_types: HashMap<String, String> =
            [("name".to_string(), "VARCHAR".to_string())].into_iter().collect();
        let col_tol: HashMap<String, ColumnTolerance> =
            [("name".to_string(), ColumnTolerance::CaseInsensitive)].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, None, &col_tol).unwrap();
        let name = result.diff_stats.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name.diff_count, 1);  // "Bob" vs "Bobby" still differs
        assert_eq!(name.match_count, 1); // "Alice" vs "alice" matches
    }

    #[test]
    fn test_whitespace_tolerance() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, '  Alice  '),
                (2, 'Bob')
            ) AS t(id, name);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 'Alice'),
                (2, 'Bobby')
            ) AS t(id, name);"
        ).unwrap();

        let cols: Vec<String> = vec!["name".into()];
        let col_types: HashMap<String, String> =
            [("name".to_string(), "VARCHAR".to_string())].into_iter().collect();
        let col_tol: HashMap<String, ColumnTolerance> =
            [("name".to_string(), ColumnTolerance::Whitespace)].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, None, &col_tol).unwrap();
        let name = result.diff_stats.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name.diff_count, 1);  // "Bob" vs "Bobby"
        assert_eq!(name.match_count, 1); // "  Alice  " vs "Alice" matches after trim
    }

    #[test]
    fn test_case_insensitive_whitespace() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, '  Alice  '),
                (2, 'BOB')
            ) AS t(id, name);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 'alice'),
                (2, 'bob')
            ) AS t(id, name);"
        ).unwrap();

        let cols: Vec<String> = vec!["name".into()];
        let col_types: HashMap<String, String> =
            [("name".to_string(), "VARCHAR".to_string())].into_iter().collect();
        let col_tol: HashMap<String, ColumnTolerance> =
            [("name".to_string(), ColumnTolerance::CaseInsensitiveWhitespace)].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, None, &col_tol).unwrap();
        let name = result.diff_stats.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name.diff_count, 0);  // both match with case+trim
    }

    // ── Type mismatch fallthrough ─────────────────────────────────────────

    #[test]
    fn test_type_mismatch_fallthrough() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES (1, 'hello')) AS t(id, val);
             CREATE TABLE source_b AS SELECT * FROM (VALUES (1, 'hello')) AS t(id, val);"
        ).unwrap();

        let cols: Vec<String> = vec!["val".into()];
        let col_types: HashMap<String, String> =
            [("val".to_string(), "VARCHAR".to_string())].into_iter().collect();
        let col_tol: HashMap<String, ColumnTolerance> =
            [("val".to_string(), ColumnTolerance::Precision { precision: 3 })].into_iter().collect();

        let result = run_diff(&conn, &["id".to_string()], &cols, &col_types, None, &col_tol).unwrap();
        let val = result.diff_stats.columns.iter().find(|c| c.name == "val").unwrap();
        assert_eq!(val.diff_count, 0);
    }

    // ── Composite primary key tests ───────────────────────────────────────

    #[test]
    fn test_composite_pk_basic_diff() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 10, 'Alice', 100.0),
                (1, 20, 'Bob', 200.0),
                (2, 10, 'Carol', 300.0)
            ) AS t(order_id, line_id, name, amount);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 10, 'Alice', 100.0),
                (1, 20, 'Bob', 250.0),
                (2, 20, 'Dave', 400.0)
            ) AS t(order_id, line_id, name, amount);"
        ).unwrap();

        let cols: Vec<String> = vec!["name".into(), "amount".into()];
        let col_types: HashMap<String, String> = [
            ("name".to_string(), "VARCHAR".to_string()),
            ("amount".to_string(), "DOUBLE".to_string()),
        ].into_iter().collect();
        let no_tol = HashMap::new();

        let result = run_diff(
            &conn, &["order_id".to_string(), "line_id".to_string()],
            &cols, &col_types, None, &no_tol,
        ).unwrap();

        assert_eq!(result.pk_summary.exclusive_a, 1);
        assert_eq!(result.pk_summary.exclusive_b, 1);
        assert_eq!(result.values_summary.total_compared, 2);
        assert_eq!(result.values_summary.rows_with_diffs, 1);

        let amount = result.diff_stats.columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(amount.diff_count, 1);
    }

    #[test]
    fn test_composite_pk_duplicates() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 10, 'Alice'),
                (1, 10, 'Alice2'),
                (2, 20, 'Bob')
            ) AS t(order_id, line_id, name);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 10, 'Alice'),
                (2, 20, 'Bob')
            ) AS t(order_id, line_id, name);"
        ).unwrap();
        let cols: Vec<String> = vec!["name".into()];
        let col_types = HashMap::new();
        let no_tol = HashMap::new();
        let result = run_diff(&conn, &["order_id".to_string(), "line_id".to_string()], &cols, &col_types, None, &no_tol).unwrap();
        assert_eq!(result.pk_summary.duplicate_pks_a, 1);
        assert_eq!(result.pk_summary.duplicate_pks_b, 0);
    }

    #[test]
    fn test_composite_pk_nulls() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 10, 'Alice'),
                (NULL, 20, 'Bob'),
                (3, NULL, 'Carol')
            ) AS t(order_id, line_id, name);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 10, 'Alice')
            ) AS t(order_id, line_id, name);"
        ).unwrap();
        let cols: Vec<String> = vec!["name".into()];
        let col_types = HashMap::new();
        let no_tol = HashMap::new();
        let result = run_diff(&conn, &["order_id".to_string(), "line_id".to_string()], &cols, &col_types, None, &no_tol).unwrap();
        assert_eq!(result.pk_summary.null_pks_a, 2);
        assert_eq!(result.pk_summary.null_pks_b, 0);
    }

    #[test]
    fn test_three_column_composite_pk() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 10, 'US', 100.0),
                (1, 10, 'UK', 200.0)
            ) AS t(order_id, line_id, region, amount);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 10, 'US', 100.0),
                (1, 10, 'UK', 250.0)
            ) AS t(order_id, line_id, region, amount);"
        ).unwrap();
        let cols: Vec<String> = vec!["amount".into()];
        let col_types: HashMap<String, String> =
            [("amount".to_string(), "DOUBLE".to_string())].into_iter().collect();
        let no_tol = HashMap::new();
        let result = run_diff(
            &conn,
            &["order_id".to_string(), "line_id".to_string(), "region".to_string()],
            &cols, &col_types, None, &no_tol,
        ).unwrap();
        assert_eq!(result.values_summary.total_compared, 2);
        assert_eq!(result.values_summary.rows_with_diffs, 1);
    }

    // ── Row minor / raw diff tests ───────────────────────────────────────

    #[test]
    fn test_rows_minor_count() {
        // Tolerance suppresses one diff → it becomes "minor"
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 100.0, 'active'),
                (2, 200.5, 'active'),
                (3, 300.0, 'pending')
            ) AS t(id, amount, status);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 100.3, 'active'),
                (2, 200.5, 'active'),
                (3, 300.0, 'shipped')
            ) AS t(id, amount, status);"
        )
        .unwrap();

        let cols: Vec<String> = vec!["amount".into(), "status".into()];
        let col_types: HashMap<String, String> = [
            ("amount".to_string(), "DOUBLE".to_string()),
            ("status".to_string(), "VARCHAR".to_string()),
        ]
        .into_iter()
        .collect();
        let no_col_tol = HashMap::new();

        // precision=0: 100.0 rounds to 100, 100.3 rounds to 100 → match (minor diff)
        // id=3: status differs (pending vs shipped) → real diff
        let result =
            run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &no_col_tol).unwrap();

        assert_eq!(result.values_summary.rows_with_diffs, 1); // id=3
        assert_eq!(result.values_summary.rows_minor, 1); // id=1 (amount diff suppressed)
        assert_eq!(result.values_summary.rows_identical, 1); // id=2
    }

    #[test]
    fn test_no_tolerance_minor_zero() {
        // No tolerance → rows_minor should always be 0
        let conn = setup_test_conn();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];
        let col_types = get_test_column_types();
        let no_col_tol = HashMap::new();

        let result = run_diff(
            &conn,
            &["id".to_string()],
            &compare_cols,
            &col_types,
            None,
            &no_col_tol,
        )
        .unwrap();

        assert_eq!(result.values_summary.rows_minor, 0);
        // Verify existing assertions still hold
        assert_eq!(result.values_summary.rows_with_diffs, 4);
        assert_eq!(result.values_summary.rows_identical, 5);
    }

    #[test]
    fn test_raw_diff_columns_exist() {
        let conn = Connection::open_in_memory().unwrap();
        setup_tolerance_tables(&conn);
        let cols: Vec<String> = vec!["name".into(), "amount".into(), "status".into()];
        let col_types = tolerance_column_types();
        let no_col_tol = HashMap::new();

        run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &no_col_tol).unwrap();

        // Verify is_raw_diff_* columns exist in _diff_join
        let mut stmt = conn
            .prepare(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = '_diff_join' AND column_name LIKE 'is_raw_diff_%' \
                 ORDER BY ordinal_position",
            )
            .unwrap();
        let raw_cols: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(raw_cols.len(), 3);
        assert!(raw_cols.contains(&"is_raw_diff_name".to_string()));
        assert!(raw_cols.contains(&"is_raw_diff_amount".to_string()));
        assert!(raw_cols.contains(&"is_raw_diff_status".to_string()));
    }

    #[test]
    fn test_minor_row_classification() {
        // A row where amount diff is suppressed by tolerance:
        // is_diff_amount = 0 (tolerance), is_raw_diff_amount = 1 (raw)
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 100.3)
            ) AS t(id, amount);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 100.0)
            ) AS t(id, amount);"
        )
        .unwrap();

        let cols: Vec<String> = vec!["amount".into()];
        let col_types: HashMap<String, String> =
            [("amount".to_string(), "DOUBLE".to_string())].into_iter().collect();
        let no_col_tol = HashMap::new();

        // precision=0 suppresses 100.3 vs 100.0
        run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &no_col_tol).unwrap();

        let is_diff: i32 = conn
            .query_row("SELECT \"is_diff_amount\" FROM _diff_join", [], |row| row.get(0))
            .unwrap();
        let is_raw_diff: i32 = conn
            .query_row("SELECT \"is_raw_diff_amount\" FROM _diff_join", [], |row| row.get(0))
            .unwrap();

        assert_eq!(is_diff, 0); // tolerance suppressed
        assert_eq!(is_raw_diff, 1); // raw diff detected
    }

    #[test]
    fn test_mixed_row() {
        // Row with one real diff (status) and one minor diff (amount with tolerance):
        // classified as "diffs" not "minor" (any real diff = diffs category)
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_a AS SELECT * FROM (VALUES
                (1, 100.3, 'active')
            ) AS t(id, amount, status);
            CREATE TABLE source_b AS SELECT * FROM (VALUES
                (1, 100.0, 'inactive')
            ) AS t(id, amount, status);"
        )
        .unwrap();

        let cols: Vec<String> = vec!["amount".into(), "status".into()];
        let col_types: HashMap<String, String> = [
            ("amount".to_string(), "DOUBLE".to_string()),
            ("status".to_string(), "VARCHAR".to_string()),
        ]
        .into_iter()
        .collect();
        let no_col_tol = HashMap::new();

        let result =
            run_diff(&conn, &["id".to_string()], &cols, &col_types, Some(0), &no_col_tol).unwrap();

        // Row has a real diff (status), so it's "diffs", not "minor"
        assert_eq!(result.values_summary.rows_with_diffs, 1);
        assert_eq!(result.values_summary.rows_minor, 0);
        assert_eq!(result.values_summary.rows_identical, 0);
    }
}
