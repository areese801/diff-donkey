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
- [x] SSH tunneling via russh (PR #8)
- [x] Ignored columns + WHERE clause filtering (PR #9)
- [x] Export diff results — CSV, Parquet, JSON (PR #10)
- [x] Query history — remember recent queries per connection (PR #11)
- [x] Connection import/export — share profiles minus passwords (PR #12)
- [x] Auto-populate table picker — cascading dropdowns (PR #13)
- [x] PK expression mode — synthetic join keys (PR #14)
- [x] Remote Parquet/CSV on S3/GCS/HTTP — DuckDB httpfs (PR #15)
- [x] Connection colors — visual tagging for prod vs dev

## Manual Testing Needed

- [ ] **Postgres connectivity** — test with a real Postgres instance (save connection, test connection, load data, run diff, table picker)
- [ ] **MySQL connectivity** — same as Postgres with MySQL instance
- [ ] **Snowflake connectivity** — test with real Snowflake account (password auth, key-pair auth, query execution, data loading, table picker)
- [ ] **SSH tunneling** — test with a real bastion host (Postgres or MySQL behind SSH, verify tunnel + query works)
- [ ] **Remote files** — test S3/GCS with real credentials, test public HTTP Parquet URL
- [ ] **Query history** — verify auto-save on successful load, deduplication, deletion
- [ ] **Connection import/export** — export, edit JSON, re-import, verify duplicate skipping
- [ ] **PK expression mode** — test CONCAT, LOWER, CAST expressions with real data

## Cross-Platform Testing

- [ ] **Windows** — test saved connections with Windows Credential Manager
- [ ] **Linux** — test saved connections with Secret Service (D-Bus)

## UI Phase 2

- [ ] **Collapsed config summary bar width shrinks** — should maintain full container width when collapsed
- [ ] **Flatten database source screens** — Saved Connection, SQL Query, Import/Export are too vertical/stacked; make horizontal like file source rows
- [ ] **Connections management** — consider moving to a proper app-level settings/config menu instead of inline
- [ ] **Results tabs layout and visual treatment** — review after Phase 1 settles
- [ ] **Color scheme and theming refinements**
- [ ] **ColumnsTab redundancy** — slim down or repurpose (shared config strip already shows column info)

## Future / Research

- [ ] **Snowflake-native diff engine** — push diff to Snowflake for 100M+ rows
- [ ] **BigQuery** — gcp-bigquery-client crate
- [ ] **Generic ODBC** — odbc-api + arrow-odbc (stretch)
