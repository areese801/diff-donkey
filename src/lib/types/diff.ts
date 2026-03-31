/**
 * TypeScript interfaces mirroring the Rust types in src-tauri/src/types.rs.
 *
 * These must stay in sync with the Rust structs — when you add a field
 * in Rust, add it here too. Serde serializes Rust struct fields as
 * camelCase by default (matching JS conventions).
 */

/** Metadata about a loaded table — matches Rust's TableMeta */
export interface TableMeta {
  table_name: string;
  row_count: number;
  columns: ColumnInfo[];
}

/** Info about a single column — matches Rust's ColumnInfo */
export interface ColumnInfo {
  name: string;
  data_type: string;
}

/** Schema comparison between two tables */
export interface SchemaComparison {
  shared: SharedColumnInfo[];
  only_in_a: ColumnInfo[];
  only_in_b: ColumnInfo[];
}

/** A column present in both tables */
export interface SharedColumnInfo {
  name: string;
  type_a: string;
  type_b: string;
  types_match: boolean;
}

/** Complete diff result — everything for the Overview tab */
export interface OverviewResult {
  diff_stats: DiffStats;
  pk_summary: PkSummary;
  values_summary: ValuesSummary;
  total_rows_a: number;
  total_rows_b: number;
}

/** Per-column diff statistics */
export interface DiffStats {
  columns: ColumnDiffStats[];
}

/** Stats for a single column comparison */
export interface ColumnDiffStats {
  name: string;
  diff_count: number;
  minor_count: number;
  match_count: number;
  total: number;
  match_pct: number;
}

/** Primary key analysis summary */
export interface PkSummary {
  exclusive_a: number;
  exclusive_b: number;
  duplicate_pks_a: number;
  duplicate_pks_b: number;
  null_pks_a: number;
  null_pks_b: number;
}

/** Values-level summary */
export interface ValuesSummary {
  total_compared: number;
  rows_with_diffs: number;
  rows_minor: number;
  rows_identical: number;
}

/** Tolerance mode for a single column */
export type ColumnTolerance =
  | { mode: "precision"; precision: number }
  | { mode: "seconds"; seconds: number }
  | { mode: "case_insensitive" }
  | { mode: "whitespace" }
  | { mode: "case_insensitive_whitespace" };

/** Diff configuration sent to the backend */
export interface DiffConfig {
  pk_columns: string[];
  tolerance: number | null;
  column_tolerances: Record<string, ColumnTolerance> | null;
  ignored_columns?: string[];
  where_clause?: string | null;
}

/** Supported database types for remote loading */
export type DatabaseType = "postgres" | "mysql" | "snowflake";

/** Paginated row data returned from backend */
export interface PagedRows {
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  page: number;
  page_size: number;
}

/** A single SQL query log entry from the activity log */
export interface QueryLogEntry {
  timestamp: number;
  operation: string;
  sql: string;
  duration_ms: number;
  rows_affected: number | null;
  error: string | null;
}
