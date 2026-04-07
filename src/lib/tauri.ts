/**
 * Typed wrappers around Tauri's invoke() function.
 *
 * Instead of calling invoke() directly with string command names,
 * use these functions for type safety. If the Rust command signature
 * changes, TypeScript will catch the mismatch at compile time.
 */
import { invoke } from "@tauri-apps/api/core";
import type { TableMeta, SchemaComparison, OverviewResult, PagedRows, DiffConfig, DatabaseType, QueryLogEntry, RemoteCredentials } from "./types/diff";
import type { SavedConnection } from "./types/connections";

/** Load a file into DuckDB as source_a or source_b */
export async function loadSource(
  path: string,
  label: "a" | "b"
): Promise<TableMeta> {
  return invoke<TableMeta>("load_source", { path, label });
}

/** Load data from a remote database into DuckDB as source_a or source_b */
export async function loadDatabaseSource(
  connString: string,
  query: string,
  label: "a" | "b",
  dbType: DatabaseType
): Promise<TableMeta> {
  return invoke<TableMeta>("load_database_source", { connString, query, label, dbType });
}

/** Compare schemas of the two loaded sources */
export async function getSchemaComparison(): Promise<SchemaComparison> {
  return invoke<SchemaComparison>("get_schema_comparison");
}

/** Run the full diff with the given config (PK + optional tolerance) */
export async function runDiff(config: DiffConfig): Promise<OverviewResult> {
  return invoke<OverviewResult>("run_diff", { config });
}

/** Get exclusive rows for a given side (a or b) */
export async function getExclusiveRows(
  side: "a" | "b",
  page: number,
  pageSize: number
): Promise<PagedRows> {
  return invoke<PagedRows>("get_exclusive_rows", { side, page, pageSize });
}

/** Get duplicate PKs for a given side (a or b) */
export async function getDuplicatePks(
  side: "a" | "b",
  page: number,
  pageSize: number
): Promise<PagedRows> {
  return invoke<PagedRows>("get_duplicate_pks", { side, page, pageSize });
}

/** Get diff rows with optional column filter and row filter */
export async function getDiffRows(
  page: number,
  pageSize: number,
  columnFilter?: string,
  rowFilter?: string
): Promise<PagedRows> {
  return invoke<PagedRows>("get_diff_rows", {
    page,
    pageSize,
    columnFilter: columnFilter ?? null,
    rowFilter: rowFilter ?? null,
  });
}

/** Load a remote file (S3, GCS, HTTP) into DuckDB as source_a or source_b */
export async function loadRemoteSource(
  uri: string,
  label: "a" | "b",
  credentials?: RemoteCredentials | null,
): Promise<TableMeta> {
  return invoke<TableMeta>("load_remote_source", {
    uri,
    label,
    credentials: credentials ?? null,
  });
}

/** Load data from Snowflake into DuckDB as source_a or source_b */
export async function loadSnowflakeSource(
  accountUrl: string,
  username: string,
  authMethod: "password" | "keypair",
  password: string | null,
  privateKeyPath: string | null,
  warehouse: string | null,
  role: string | null,
  database: string | null,
  schema: string | null,
  query: string,
  label: "a" | "b"
): Promise<TableMeta> {
  return invoke<TableMeta>("load_snowflake_source", {
    accountUrl,
    username,
    authMethod,
    password,
    privateKeyPath,
    warehouse,
    role,
    database,
    schema,
    query,
    label,
  });
}

// ─── Connection Management ──────────────────────────────────────────────────

/** List all saved database connections */
export async function listSavedConnections(): Promise<SavedConnection[]> {
  return invoke<SavedConnection[]>("list_saved_connections");
}

/** Save (create or update) a database connection */
export async function saveConnection(
  conn: SavedConnection,
  password: string | null,
  sshPassword: string | null = null
): Promise<void> {
  return invoke<void>("save_connection", { conn, password, sshPassword });
}

/** Delete a saved connection by ID */
export async function deleteConnection(id: string): Promise<void> {
  return invoke<void>("delete_connection", { id });
}

/** Test a database connection */
export async function testConnection(
  conn: SavedConnection,
  password: string | null,
  sshPassword: string | null = null
): Promise<string> {
  return invoke<string>("test_connection", { conn, password, sshPassword });
}

/** Load data from a saved connection into DuckDB */
export async function loadFromSavedConnection(
  id: string,
  query: string,
  label: "a" | "b"
): Promise<TableMeta> {
  return invoke<TableMeta>("load_from_saved_connection", { id, query, label });
}

// ─── Activity Log ────────────────────────────────────────────────────────────

/** Get all SQL query log entries */
export async function getActivityLog(): Promise<QueryLogEntry[]> {
  return invoke<QueryLogEntry[]>("get_activity_log");
}

/** Clear the SQL query log */
export async function clearActivityLog(): Promise<void> {
  return invoke<void>("clear_activity_log");
}
