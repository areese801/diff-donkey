# Diff Donkey — TODO

## Done

- [x] v0.1.1: Tolerance modes (TRUNC precision, seconds, case-insensitive, whitespace, combo)
- [x] Composite primary keys
- [x] Postgres/MySQL via DuckDB extensions
- [x] Saved connections with OS keychain (cross-platform)
- [x] Row filter toggles (All / Diffs / Minor / Same)
- [x] Character-level diff highlighting (toggleable)
- [x] Ignore Case global toggle
- [x] Auto-PK detection, file picker with persistence + auto-load
- [x] Per-column minor counts in Overview table
- [x] DB Activity Log (collapsible bottom panel)
- [x] Snowflake connectivity via REST API (PR #6)
- [x] SSH tunneling via russh (PR #7)

## Manual Testing Needed

- [ ] **Postgres connectivity** — test with a real Postgres instance (save connection, test connection, load data, run diff)
- [ ] **MySQL connectivity** — same as Postgres with MySQL instance
- [ ] **Snowflake connectivity** — test with real Snowflake account (password auth, key-pair auth, query execution, data loading)
- [ ] **SSH tunneling** — test with a real bastion host (Postgres or MySQL behind SSH, verify tunnel + query works)

## Up Next

### UX Improvements

- [ ] **Ignored columns** — per-column "Ignore" toggle to exclude from diff
- [ ] **WHERE clause filtering** — restrict comparison to subset of rows
- [ ] **Query history** — remember recent queries per saved connection
- [ ] **Connection colors** — visual tagging for prod vs dev
- [ ] **Connection import/export** — share profiles minus passwords

## v0.2.0: Export & Reporting

- [ ] **Export diff results** — save to CSV, Parquet, or DuckDB table

## Cross-Platform Testing

- [ ] **Windows** — test saved connections with Windows Credential Manager
- [ ] **Linux** — test saved connections with Secret Service (D-Bus)

## Future / Research

- [ ] **Snowflake-native diff engine** — push diff to Snowflake for 100M+ rows
- [ ] **Auto-populate table picker** — SHOW DATABASES → SCHEMAS → TABLES
- [ ] **PK expression mode** — synthetic join keys via DuckDB expressions
- [ ] **Remote Parquet/CSV on S3/GCS** — DuckDB httpfs extension
- [ ] **BigQuery** — gcp-bigquery-client crate
- [ ] **Generic ODBC** — odbc-api + arrow-odbc (stretch)
