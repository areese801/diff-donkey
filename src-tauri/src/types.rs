/// Shared types that flow between Rust and the frontend via JSON.
///
/// The `#[derive(Serialize)]` attribute is like adding a `to_dict()` method
/// in Python — it lets serde automatically convert these structs to JSON.
/// The TypeScript frontend will have matching interfaces.
use serde::Serialize;

/// Metadata about a loaded table — returned after loading a CSV/Parquet file.
/// Like a simplified version of `DESCRIBE TABLE` in Snowflake.
#[derive(Debug, Serialize, Clone)]
pub struct TableMeta {
    pub table_name: String,
    pub row_count: usize,
    pub columns: Vec<ColumnInfo>,
}

/// Info about a single column — name and DuckDB type.
/// Similar to what you'd get from `information_schema.columns`.
#[derive(Debug, Serialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Schema comparison result — shows column overlap between two tables.
/// Like running DESCRIBE on both tables and diffing the output.
#[derive(Debug, Serialize, Clone)]
pub struct SchemaComparison {
    pub shared: Vec<SharedColumnInfo>,
    pub only_in_a: Vec<ColumnInfo>,
    pub only_in_b: Vec<ColumnInfo>,
}

/// A column that exists in both tables — with type match info.
#[derive(Debug, Serialize, Clone)]
pub struct SharedColumnInfo {
    pub name: String,
    pub type_a: String,
    pub type_b: String,
    pub types_match: bool,
}

/// The complete diff result — everything needed for the Overview tab.
#[derive(Debug, Serialize, Clone)]
pub struct OverviewResult {
    pub diff_stats: DiffStats,
    pub pk_summary: PkSummary,
    pub values_summary: ValuesSummary,
    pub total_rows_a: i64,
    pub total_rows_b: i64,
}

/// Per-column diff statistics.
#[derive(Debug, Serialize, Clone)]
pub struct DiffStats {
    pub columns: Vec<ColumnDiffStats>,
}

/// Stats for a single column's comparison.
#[derive(Debug, Serialize, Clone)]
pub struct ColumnDiffStats {
    pub name: String,
    pub diff_count: i64,
    pub match_count: i64,
    pub total: i64,
    pub match_pct: f64,
}

/// Primary key analysis summary.
#[derive(Debug, Serialize, Clone)]
pub struct PkSummary {
    pub exclusive_a: i64,
    pub exclusive_b: i64,
    pub duplicate_pks_a: i64,
    pub duplicate_pks_b: i64,
    pub null_pks_a: i64,
    pub null_pks_b: i64,
}

/// Values-level summary for matched rows.
#[derive(Debug, Serialize, Clone)]
pub struct ValuesSummary {
    pub total_compared: i64,
    pub rows_with_diffs: i64,
    pub rows_identical: i64,
}

/// Tolerance mode for a single column comparison.
/// Each variant maps to a different SQL comparison strategy.
#[derive(Debug, serde::Deserialize, Clone)]
#[serde(tag = "mode")]
pub enum ColumnTolerance {
    /// Numeric precision: ROUND(a, N) = ROUND(b, N)
    #[serde(rename = "precision")]
    Precision { precision: i32 },

    /// Timestamp tolerance: values within N seconds are a match
    #[serde(rename = "seconds")]
    Seconds { seconds: f64 },

    /// Case-insensitive string comparison: LOWER(a) = LOWER(b)
    #[serde(rename = "case_insensitive")]
    CaseInsensitive,

    /// Whitespace-normalized comparison: TRIM(a) = TRIM(b)
    #[serde(rename = "whitespace")]
    Whitespace,

    /// Both case-insensitive and whitespace-normalized
    #[serde(rename = "case_insensitive_whitespace")]
    CaseInsensitiveWhitespace,
}

/// Diff configuration sent from the frontend.
///
/// `tolerance` sets a default numeric precision (decimal places) for all numeric columns.
/// `column_tolerances` overrides specific columns with any tolerance mode.
/// Resolution: column_tolerances[col] → auto-apply Precision(tolerance) if numeric → IS DISTINCT FROM.
#[derive(Debug, serde::Deserialize)]
pub struct DiffConfig {
    pub pk_column: String,
    pub tolerance: Option<i32>,
    pub column_tolerances: Option<std::collections::HashMap<String, ColumnTolerance>>,
}

/// Paginated row data — used for exclusive rows, duplicates, and diff rows.
/// Generic enough to hold any tabular data.
#[derive(Debug, Serialize, Clone)]
pub struct PagedRows {
    pub columns: Vec<String>,
    pub rows: Vec<std::collections::HashMap<String, serde_json::Value>>,
    pub total: i64,
    pub page: usize,
    pub page_size: usize,
}
