# v0.1.2: Database Connectivity + Composite Primary Keys

## Overview

Extend Diff Donkey to load data from Postgres and Snowflake in addition to CSV/Parquet files, and support composite (multi-column) primary keys. The diff engine itself is unchanged — all sources ultimately create `source_a` / `source_b` tables in DuckDB.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Source type selection | Dropdown per source panel | Scales to many source types, compact |
| Connection form | Modal dialog | Keeps source panels clean, room for fields |
| Postgres connectivity | DuckDB `postgres` extension | Zero new Rust crates, just SQL strings |
| Snowflake connectivity | Rust REST API client | No DuckDB Snowflake extension exists |
| Snowflake auth | Username/password (with MFA) + key-pair (JWT) | Both common in practice |
| Snowflake query mode | Table picker + Custom SQL toggle | Simple default, power-user escape hatch |
| Table picker fields | Manual text inputs (database/schema/table) | No round-trips, no permission issues |
| Composite PK UI | Multi-select dropdown | Compact, well-understood pattern |
| Large dataset strategy | Pull into DuckDB (for now) | Snowflake-native diff deferred to future |

## Architecture

### Data Flow

```
Source Panel (dropdown: File | Postgres | Snowflake)
    │
    ├─ File → existing pickFile() → load_source IPC → loader::load_csv/parquet
    ├─ Postgres → modal → load_postgres IPC → DuckDB ATTACH + SELECT INTO
    └─ Snowflake → modal → load_snowflake IPC → REST API fetch → DuckDB INSERT
    │
    ▼
source_a / source_b tables in DuckDB (same as today)
    │
    ▼
Existing diff engine (schema compare → build_diff_join → compute stats)
```

### Postgres via DuckDB Extension

No new Rust crates. The `load_postgres` Tauri command runs SQL through the existing DuckDB connection:

```sql
INSTALL postgres;
LOAD postgres;
ATTACH 'host=localhost port=5432 dbname=mydb user=admin password=secret' AS pg (TYPE postgres);
CREATE OR REPLACE TABLE source_a AS SELECT * FROM pg.public.orders;
-- or with custom SQL:
CREATE OR REPLACE TABLE source_a AS SELECT * FROM postgres_query('...conn...', 'SELECT * FROM orders WHERE status = ''active''');
```

DuckDB handles connection management, type mapping, and query pushdown. The Rust code just builds and executes SQL strings.

**Extension installation**: `INSTALL` and `LOAD` are idempotent — safe to call every time. The `load_from_postgres` function calls both before every ATTACH to ensure the extension is available.

**Connection string escaping**: All connection parameters (host, database, username, password) are escaped using the existing `escape_sql_string()` helper before interpolation into the ATTACH string. A password like `it's_secret` becomes `it''s_secret` in the SQL literal. Parameters are also validated to reject empty strings.

**Connection fields**: host, port, database, username, password, schema, table (or custom SQL).

### Snowflake via REST API

New Rust crates: `reqwest` (HTTP), `jsonwebtoken` + `rsa` (JWT/key-pair auth).

**Auth flow (key-pair)**:
1. User provides account URL, username, path to private key `.p8` file
2. Rust reads the key file, generates a JWT with `sub` = `ACCOUNT.USERNAME`
3. JWT is sent as `Authorization: Bearer <jwt>` to Snowflake REST API

**Auth flow (username/password with MFA)**:
1. User provides account URL, username, password
2. POST to Snowflake `/session/v1/login-request` with `LOGIN_NAME`, `PASSWORD`
3. If Snowflake returns `"code": "390318"` (MFA required), prompt the user to approve the Duo push or enter the TOTP code
4. For Duo push: Snowflake handles asynchronously — poll the login endpoint until MFA is approved or timeout
5. For TOTP: add `passcode` field to the login request with the user-entered code
6. On success, extract the `token` from the response for subsequent API calls

**Query flow**:
1. POST to `https://<account>.snowflakecomputing.com/api/v2/statements`
2. Poll for completion (async execution)
3. Fetch result pages as JSON
4. Parse into rows, insert into DuckDB: `CREATE TABLE source_a AS SELECT ...`

**Row limit**: The connection modal includes an optional "Row Limit" field (default: empty = no limit). When set, a `LIMIT N` clause is appended to the query. For table picker mode, the generated SQL becomes `SELECT * FROM db.schema.table LIMIT N`. For custom SQL, the user is responsible for scoping their query. The UI shows a warning when loading more than 1M rows: "Large dataset — consider adding a WHERE clause or row limit."

**Connection fields**: account URL, auth method dropdown, username, password OR key file, warehouse (optional), role (optional), database, schema, table (or custom SQL), row limit (optional).

### Composite Primary Keys

**DiffConfig change**:
```rust
pub struct DiffConfig {
    pub pk_columns: Vec<String>,  // was pk_column: String
    // ... tolerance fields unchanged
}
```

**Composite PKs are always joined by column name** — `a."order_id" = b."order_id" AND a."line_item_id" = b."line_item_id"`. Column order in the multi-select doesn't affect the join semantics. Both sources must have columns with the same names for the PK.

**Query mode validation**: Exactly one of `table` or `custom_sql` must be `Some`. If both are `Some` or both are `None`, the command returns an error: "Specify either a table name or custom SQL, not both."

#### Composite PK Implementation Detail

The `pk_columns: Vec<String>` parameter flows through the entire diff pipeline. Here is how each function adapts:

**`build_diff_join` — column naming**:

Each PK column gets its own `pk_{name}_a` / `pk_{name}_b` alias:

```sql
-- Single PK (today): a."id" as pk_a, b."id" as pk_b
-- Composite PK:
a."order_id" as "pk_order_id_a", b."order_id" as "pk_order_id_b",
a."line_item_id" as "pk_line_item_id_a", b."line_item_id" as "pk_line_item_id_b"
```

JOIN clause:
```sql
FROM source_a a FULL OUTER JOIN source_b b
  ON a."order_id" = b."order_id" AND a."line_item_id" = b."line_item_id"
```

**`compute_pk_summary`**:

```sql
-- Exclusive to A: all B-side PK columns are NULL
SELECT COUNT(*) FROM _diff_join WHERE "pk_order_id_b" IS NULL AND "pk_line_item_id_b" IS NULL

-- Exclusive to B: all A-side PK columns are NULL
SELECT COUNT(*) FROM _diff_join WHERE "pk_order_id_a" IS NULL AND "pk_line_item_id_a" IS NULL

-- Duplicate PKs in A:
SELECT COUNT(*) FROM (
  SELECT "order_id", "line_item_id" FROM source_a
  GROUP BY "order_id", "line_item_id" HAVING COUNT(*) > 1
)

-- Null PKs in A (any PK column is NULL):
SELECT COUNT(*) FROM source_a WHERE "order_id" IS NULL OR "line_item_id" IS NULL
```

**`get_exclusive_rows` (keys.rs)** — full rewrite for composite PKs:

```sql
-- Old (single PK):
SELECT s.* FROM source_a s
  INNER JOIN (SELECT pk_a as pk FROM _diff_join WHERE pk_b IS NULL ...) excl
  ON s."id" = excl.pk

-- New (composite PK):
SELECT s.* FROM source_a s
  INNER JOIN (
    SELECT "pk_order_id_a", "pk_line_item_id_a"
    FROM _diff_join
    WHERE "pk_order_id_b" IS NULL AND "pk_line_item_id_b" IS NULL
    LIMIT ? OFFSET ?
  ) excl
  ON s."order_id" = excl."pk_order_id_a" AND s."line_item_id" = excl."pk_line_item_id_a"
  ORDER BY s."order_id", s."line_item_id"
```

**`get_duplicate_pks` (keys.rs)** — similar multi-column GROUP BY and join.

**`_diff_meta`** — stores PK columns as JSON array:
```sql
CREATE OR REPLACE TEMPORARY TABLE _diff_meta AS
  SELECT '["order_id","line_item_id"]' as pk_columns
```

The `get_pk_column` helper becomes `get_pk_columns` and returns `Vec<String>` by parsing the JSON.

**Values summary — `rows_with_diffs`**: The `WHERE pk_a IS NOT NULL AND pk_b IS NOT NULL` clause becomes `WHERE "pk_{name}_a" IS NOT NULL AND ... for all PK columns`.

**`get_diff_rows` (commands.rs)**: The `ORDER BY pk_a, pk_b` becomes multi-column: `ORDER BY "pk_order_id_a", "pk_line_item_id_a"`.

**`compare_columns` filter**: PK columns are excluded from comparison. The filter changes from `.filter(|c| c.name != pk_column)` to `.filter(|c| !pk_columns.contains(&c.name))`.

## Frontend Components

### SourceSelector.svelte (modified)

Each source panel gets a source type dropdown at the top:

```
┌─────────────────────────────┐
│ Source A                    │
│ [▼ CSV/Parquet           ]  │  ← dropdown: File | Postgres | Snowflake
│ ┌─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐  │
│   Select File / Change File │  ← or "Connect to Postgres..." button
│ └─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  │
│ 10 rows                    │
│ id BIGINT                  │
│ name VARCHAR               │
└─────────────────────────────┘
```

When a database type is selected, the "Select File" button changes to "Connect to [Database]..." which opens the modal.

### ConnectionModal.svelte (new)

Shared modal component parameterized by database type. Props determine which fields to show:

- **Postgres**: host, port, database, username, password, schema, table/SQL toggle
- **Snowflake**: account URL, auth method, username, password/key file, warehouse, role, database, schema, table/SQL toggle, row limit

Modal has "Connect & Load" button. On success, modal closes and source panel shows table metadata.

### DiffConfig.svelte (modified)

PK selector changes from `<select>` to a multi-select. When multiple columns are selected, they appear as a comma-separated list or chips.

## Tauri IPC Commands

### New commands

```rust
#[tauri::command]
fn load_postgres(config: PostgresConfig, label: String, state: State<DuckDbState>) -> Result<TableMeta, String>

#[tauri::command]
fn load_snowflake(config: SnowflakeConfig, label: String, state: State<DuckDbState>) -> Result<TableMeta, String>
```

### New types

```rust
struct PostgresConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    schema: String,
    table: Option<String>,       // table picker mode — mutually exclusive with custom_sql
    custom_sql: Option<String>,  // custom SQL mode — mutually exclusive with table
}

struct SnowflakeConfig {
    account_url: String,
    auth_method: SnowflakeAuthMethod,
    username: String,
    warehouse: Option<String>,
    role: Option<String>,
    database: String,
    schema: String,
    table: Option<String>,
    custom_sql: Option<String>,
    row_limit: Option<u64>,
}

enum SnowflakeAuthMethod {
    Password { password: String },
    KeyPair { private_key_path: String },
}
```

### Modified commands

`run_diff` already accepts `DiffConfig`. Change `pk_column: String` to `pk_columns: Vec<String>`.

## File Changes

| File | Change |
|------|--------|
| `src-tauri/Cargo.toml` | Add `reqwest`, `jsonwebtoken`, `rsa`, `base64`, `serde_json` (if not already) |
| `src-tauri/src/types.rs` | Add `PostgresConfig`, `SnowflakeConfig`, `SnowflakeAuthMethod`; change `DiffConfig.pk_column` to `pk_columns: Vec<String>` |
| `src-tauri/src/loader.rs` | Add `load_from_postgres()` with extension install/load, connection string escaping |
| `src-tauri/src/snowflake.rs` | New module: REST API client, JWT auth, password+MFA auth, query execution, result parsing |
| `src-tauri/src/commands.rs` | Add `load_postgres`, `load_snowflake` commands; update `run_diff` for composite PKs; update `get_pk_column` → `get_pk_columns` |
| `src-tauri/src/lib.rs` | Register new commands, declare `snowflake` module |
| `src-tauri/src/diff/stats.rs` | Update `build_diff_join` for multi-column ON clause and `pk_{name}_a/b` aliases; update `compute_pk_summary` for multi-column NULL checks |
| `src-tauri/src/diff/keys.rs` | Rewrite `get_exclusive_rows`, `get_duplicate_pks` for multi-column joins and GROUP BY |
| `src/lib/types/diff.ts` | Add `PostgresConfig`, `SnowflakeConfig` types; change `pk_column` to `pk_columns` |
| `src/lib/tauri.ts` | Add `loadPostgres()`, `loadSnowflake()` wrappers |
| `src/lib/components/SourceSelector.svelte` | Add source type dropdown, "Connect" button for DB types |
| `src/lib/components/ConnectionModal.svelte` | New: shared modal for database connections |
| `src/lib/components/DiffConfig.svelte` | Multi-select PK dropdown |
| `src/routes/+page.svelte` | Wire new components, update `handleRunDiff` for `pk_columns` |

## Security

- Database credentials are never persisted — they exist only in memory for the duration of the connection
- Snowflake private key files are read once to generate JWT, then the key material is dropped from memory
- Postgres connection string parameters are escaped via `escape_sql_string()` to prevent SQL injection through values containing single quotes
- Custom SQL from users is executed via DuckDB (Postgres) or Snowflake REST API — it runs with the permissions of the provided credentials, not elevated
- Label validation ("a" or "b") remains in place for all load commands
- Query mode validation: exactly one of `table` or `custom_sql` must be provided; both or neither is rejected

## Testing Strategy

- **Postgres**: Unit tests using inline DuckDB tables (mock the postgres extension path by creating `source_a` directly). Integration test requires a running Postgres instance (CI or local).
- **Snowflake**: Unit tests for JWT generation, request building, response parsing (mock HTTP responses). Integration tests require Snowflake credentials (manual/CI with secrets).
- **Composite PKs**: Full unit test coverage — can test entirely with inline DuckDB tables. Update all existing diff tests that use single PK to pass `vec!["id".to_string()]`. Add new tests with 2-column and 3-column composite keys covering: basic diff, exclusive rows, duplicate PKs, null PKs, and `get_diff_rows`.
