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
    pub minor_count: i64,
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
    pub rows_minor: i64,
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
    pub pk_columns: Vec<String>,
    pub pk_expression: Option<String>,
    pub tolerance: Option<i32>,
    pub column_tolerances: Option<std::collections::HashMap<String, ColumnTolerance>>,
    pub ignored_columns: Option<Vec<String>>,
    pub where_clause: Option<String>,
}

/// Resolved primary key mode — either named columns or a SQL expression.
///
/// Think of this like Python's Union type: it's either a list of column names
/// or a single SQL expression string. The diff engine uses this to decide
/// how to build JOIN conditions and PK aliases in the _diff_join table.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
#[serde(tag = "mode")]
pub enum PkMode {
    #[serde(rename = "columns")]
    Columns { columns: Vec<String> },
    #[serde(rename = "expression")]
    Expression { expression: String },
}

impl PkMode {
    /// Column name prefixes used in _diff_join (without the _a/_b suffix).
    /// In column mode: ["pk_col1", "pk_col2", ...].
    /// In expression mode: ["pk_expr"].
    pub fn join_key_names(&self) -> Vec<String> {
        match self {
            PkMode::Columns { columns } => columns.iter().map(|c| format!("pk_{}", c)).collect(),
            PkMode::Expression { .. } => vec!["pk_expr".to_string()],
        }
    }

    /// The raw PK column names (for column mode) or a placeholder for expression mode.
    /// Used for queries against source tables (GROUP BY, NULL checks).
    pub fn source_column_names(&self) -> Vec<String> {
        match self {
            PkMode::Columns { columns } => columns.clone(),
            PkMode::Expression { .. } => vec!["_pk_expr".to_string()],
        }
    }
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
