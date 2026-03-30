# Diff Donkey — TODO

## v0.1.1: Column-Level Tolerance (DONE)

- [x] Numeric precision — `ROUND(a, N) = ROUND(b, N)`
- [x] Timestamp tolerance — within N seconds
- [x] Case-insensitive strings, whitespace trim, combo
- [x] Per-column mode dropdowns filtered by column type
- [x] 24 tests passing

## v0.1.2: Database Connectivity + Composite Keys

### Database Sources

- [ ] **Postgres via DuckDB extension** — `INSTALL postgres; LOAD postgres; ATTACH ...`
      Connection string dialog in UI. Zero new Rust crates needed.
- [ ] **MySQL via DuckDB extension** — same pattern as Postgres
- [ ] **Snowflake** — REST API (`/api/v2/statements`) via `reqwest` + JWT auth.
      Fetch results as JSON/Arrow, load into DuckDB temp table, diff as usual.
- [ ] **Remote Parquet/CSV on S3/GCS** — DuckDB `httpfs` extension
- [ ] **BigQuery** — `gcp-bigquery-client` crate, same load-into-DuckDB pattern
- [ ] **Generic ODBC** — `odbc-api` + `arrow-odbc` for SQL Server, Oracle, etc. (stretch)

### Composite Primary Keys

- [ ] Multi-column PK support — update DiffConfig to accept `Vec<String>`,
      join ON multiple columns, update PK analysis

## v0.1.3: UX Improvements

### Row Filter Toggles

- [ ] Quick-filter buttons on Values tab: All / Diffs Only / Matches Only / Orphans Only
      Filter the _diff_join table in SQL, re-render

### Cell-Level Highlighting

- [ ] In Values tab, highlight individual cells that differ within a matched row
      (not just flag the row). Biggest UX gap vs Beyond Compare.

### Unimportant / Ignored Columns

- [ ] Per-column "Ignore" toggle — exclude columns from diff entirely
      (e.g., random GUIDs, ETL timestamps, surrogate keys).
      Inspired by Beyond Compare and Kaleidoscope.
      Already have the per-column UI; add an "Ignore" option to the mode dropdown.

### WHERE Clause Filtering

- [ ] Let users restrict comparison to a subset of rows via SQL WHERE clause
      (e.g., `status = 'active'`). DuckDB handles the SQL natively.

## v0.2.0: Export & Reporting

- [ ] **Export diff results** — save to CSV, Parquet, or DuckDB table
      Useful for sharing results or downstream consumption.

## Cross-Platform Testing

- [ ] **Windows** — test saved connections with Windows Credential Manager (`keyring` crate).
      Verify credential store/retrieve, connection form, SSH tunnel. Build with `cargo tauri build`.
- [ ] **Linux** — test saved connections with Secret Service (D-Bus) via `keyring` crate.
      May need `libdbus` dev package installed. Verify on Ubuntu/Fedora.

## Future / Research

- [ ] **Snowflake-native diff engine** — for very large datasets (100M+ rows),
      push the diff query to Snowflake rather than pulling data into DuckDB.
      Use Snowflake as the compute backend instead of local DuckDB.
      Investigate at what data size the pull-into-DuckDB approach breaks down.

- [ ] **Auto-populate Snowflake table picker** — after connecting, query
      `SHOW DATABASES` → `SHOW SCHEMAS` → `SHOW TABLES` to fill cascading
      dropdowns instead of manual text input.

- [ ] **PK expression mode** — let users type a DuckDB expression
      (e.g., `region || '-' || store_id`) as a synthetic join key,
      for cases where no clean PK exists.
