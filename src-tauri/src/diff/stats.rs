/// Core diff engine — the heart of Diff Donkey.
///
/// This implements the Datafold algorithm:
/// 1. FULL OUTER JOIN source_a and source_b on the primary key
/// 2. For each column, compute `col_a IS DISTINCT FROM col_b` → 0/1
/// 3. Materialize as a temp table (_diff_join)
/// 4. Aggregate: SUM each is_diff column → per-column diff counts
///
/// IS DISTINCT FROM is crucial — unlike `!=`, it handles NULLs correctly:
///   NULL IS DISTINCT FROM NULL  → false (they're "equal")
///   NULL IS DISTINCT FROM 'x'  → true  (they differ)
///
/// In Snowflake terms, this is like:
///   SELECT col_a, col_b, (col_a IS DISTINCT FROM col_b)::INT as is_diff
///   FROM table_a FULL OUTER JOIN table_b ON a.pk = b.pk
use duckdb::Connection;

use crate::error::DiffDonkeyError;
use crate::types::{ColumnDiffStats, DiffStats, OverviewResult, PkSummary, ValuesSummary};

/// Run the full diff: build the join table and compute all stats.
///
/// This is the main entry point called by the `run_diff` Tauri command.
/// It orchestrates the entire diff pipeline.
pub fn run_diff(
    conn: &Connection,
    pk_column: &str,
    compare_columns: &[String],
) -> Result<OverviewResult, DiffDonkeyError> {
    // Step 1: Build the materialized join table
    build_diff_join(conn, pk_column, compare_columns)?;

    // Step 2: Compute aggregate stats from the join table
    let diff_stats = compute_diff_stats(conn, compare_columns)?;

    // Step 3: Compute PK-level summary
    let pk_summary = compute_pk_summary(conn, pk_column)?;

    // Step 4: Compute values-level summary
    let total_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM _diff_join",
        [],
        |row| row.get(0),
    )?;

    let rows_with_diffs: i64 = if compare_columns.is_empty() {
        0
    } else {
        let any_diff_clause = compare_columns
            .iter()
            .map(|c| format!("\"is_diff_{}\" = 1", c))
            .collect::<Vec<_>>()
            .join(" OR ");

        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM _diff_join WHERE pk_a IS NOT NULL AND pk_b IS NOT NULL AND ({})",
                any_diff_clause
            ),
            [],
            |row| row.get(0),
        )?
    };

    let matched_rows = total_rows - pk_summary.exclusive_a - pk_summary.exclusive_b;

    let values_summary = ValuesSummary {
        total_compared: matched_rows,
        rows_with_diffs,
        rows_identical: matched_rows - rows_with_diffs,
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
///
/// Creates a temporary table `_diff_join` with:
/// - pk_a, pk_b: the primary key from each side (NULL if exclusive)
/// - {col}_a, {col}_b: the column values from each side
/// - is_diff_{col}: 1 if values differ, 0 if they match
///
/// This is the SQL equivalent of Datafold's `_create_outer_join()`.
fn build_diff_join(
    conn: &Connection,
    pk_column: &str,
    compare_columns: &[String],
) -> Result<(), DiffDonkeyError> {
    // Build the SELECT clause:
    //   a.pk as pk_a, b.pk as pk_b,
    //   a.col1 as col1_a, b.col1 as col1_b,
    //   (a.col1 IS DISTINCT FROM b.col1)::INTEGER as is_diff_col1,
    //   ...
    let mut select_parts = vec![
        format!("a.\"{}\" as pk_a", pk_column),
        format!("b.\"{}\" as pk_b", pk_column),
    ];

    for col in compare_columns {
        // Value columns from each side
        select_parts.push(format!("a.\"{}\" as \"{}_a\"", col, col));
        select_parts.push(format!("b.\"{}\" as \"{}_b\"", col, col));

        // IS DISTINCT FROM check — the core of the diff algorithm
        // Cast to INTEGER: true→1, false→0 (DuckDB supports this natively)
        // SECURITY: Quote the alias to handle column names with spaces/special chars.
        select_parts.push(format!(
            "(a.\"{}\" IS DISTINCT FROM b.\"{}\")::INTEGER as \"is_diff_{}\"",
            col, col, col
        ));
    }

    let sql = format!(
        "CREATE OR REPLACE TEMPORARY TABLE _diff_join AS \
         SELECT {} \
         FROM source_a a \
         FULL OUTER JOIN source_b b ON a.\"{}\" = b.\"{}\"",
        select_parts.join(", "),
        pk_column,
        pk_column
    );

    conn.execute_batch(&sql)?;
    Ok(())
}

/// Compute per-column diff statistics from the materialized join table.
///
/// For each column, counts how many rows differ vs match.
/// This is the equivalent of Datafold's `_count_diff_per_column()`.
fn compute_diff_stats(
    conn: &Connection,
    compare_columns: &[String],
) -> Result<DiffStats, DiffDonkeyError> {
    if compare_columns.is_empty() {
        return Ok(DiffStats { columns: vec![] });
    }

    // Build aggregate query:
    //   SELECT SUM(is_diff_col1), COUNT(*) - SUM(is_diff_col1), ...
    //   FROM _diff_join
    //   WHERE pk_a IS NOT NULL AND pk_b IS NOT NULL  -- only matched rows
    let mut agg_parts = Vec::new();
    for col in compare_columns {
        agg_parts.push(format!("SUM(\"is_diff_{col}\")"));
        agg_parts.push(format!("COUNT(*) - SUM(\"is_diff_{col}\")"));
    }

    let sql = format!(
        "SELECT {} FROM _diff_join WHERE pk_a IS NOT NULL AND pk_b IS NOT NULL",
        agg_parts.join(", ")
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
fn compute_pk_summary(conn: &Connection, pk_column: &str) -> Result<PkSummary, DiffDonkeyError> {
    // Exclusive to A: rows where pk_b is NULL (no match in B)
    let exclusive_a: i64 = conn.query_row(
        "SELECT COUNT(*) FROM _diff_join WHERE pk_b IS NULL",
        [],
        |row| row.get(0),
    )?;

    // Exclusive to B: rows where pk_a is NULL (no match in A)
    let exclusive_b: i64 = conn.query_row(
        "SELECT COUNT(*) FROM _diff_join WHERE pk_a IS NULL",
        [],
        |row| row.get(0),
    )?;

    // Duplicate PKs in A
    let duplicate_pks_a: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM (SELECT \"{}\" FROM source_a GROUP BY \"{}\" HAVING COUNT(*) > 1)",
            pk_column, pk_column
        ),
        [],
        |row| row.get(0),
    )?;

    // Duplicate PKs in B
    let duplicate_pks_b: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM (SELECT \"{}\" FROM source_b GROUP BY \"{}\" HAVING COUNT(*) > 1)",
            pk_column, pk_column
        ),
        [],
        |row| row.get(0),
    )?;

    // Null PKs
    let null_pks_a: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM source_a WHERE \"{}\" IS NULL", pk_column),
        [],
        |row| row.get(0),
    )?;

    let null_pks_b: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM source_b WHERE \"{}\" IS NULL", pk_column),
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

    #[test]
    fn test_run_diff_overview() {
        let conn = setup_test_conn();
        let compare_cols: Vec<String> = vec![
            "customer_name".into(),
            "amount".into(),
            "status".into(),
            "created_at".into(),
        ];

        let result = run_diff(&conn, "id", &compare_cols).unwrap();

        // 10 rows in A, 10 rows in B
        assert_eq!(result.total_rows_a, 10);
        assert_eq!(result.total_rows_b, 10);

        // Row 8 is exclusive to A, row 11 is exclusive to B
        assert_eq!(result.pk_summary.exclusive_a, 1);
        assert_eq!(result.pk_summary.exclusive_b, 1);

        // No duplicate PKs in our test data
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

        let result = run_diff(&conn, "id", &compare_cols).unwrap();

        // Find the customer_name column stats
        let name_stats = result.diff_stats.columns.iter().find(|c| c.name == "customer_name").unwrap();
        // Row 5: "Eve Davis" vs "eve davis" — IS DISTINCT FROM catches case diff
        assert_eq!(name_stats.diff_count, 1);

        // Find the amount column stats
        let amount_stats = result.diff_stats.columns.iter().find(|c| c.name == "amount").unwrap();
        // Row 2: 275.50 vs 280.00
        assert_eq!(amount_stats.diff_count, 1);

        // Find the status column stats
        let status_stats = result.diff_stats.columns.iter().find(|c| c.name == "status").unwrap();
        // Row 3: pending vs shipped, Row 7: pending vs completed
        assert_eq!(status_stats.diff_count, 2);

        // created_at should have no diffs
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

        let result = run_diff(&conn, "id", &compare_cols).unwrap();

        // 9 matched rows (IDs 1-7, 9, 10 are in both)
        assert_eq!(result.values_summary.total_compared, 9);

        // 4 rows have at least one diff: rows 2 (amount), 3 (status), 5 (name), 7 (status)
        assert_eq!(result.values_summary.rows_with_diffs, 4);
        assert_eq!(result.values_summary.rows_identical, 5);
    }
}
