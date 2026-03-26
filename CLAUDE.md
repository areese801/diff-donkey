# Diff Donkey

Desktop data diff tool — compare datasets visually using DuckDB as the analytical backend.

## Tech Stack

- **Tauri v2** — native desktop shell (Rust + web frontend)
- **Rust** — backend: DuckDB integration, diff engine, file loading
- **SvelteKit** — frontend: Datafold-style 4-tab UI (Overview, Columns, Primary Keys, Values)
- **DuckDB** — in-memory SQL engine for all diff operations
- **TypeScript** — frontend type safety

## Build Commands

```bash
# Development (opens native window with hot reload)
cargo tauri dev

# Frontend only (no Tauri window)
npm run dev

# Production build
cargo tauri build

# Type check frontend
npm run check
```

## Project Structure

```
src/                    # SvelteKit frontend
  routes/               # Pages
  lib/                  # Shared components, stores, types
    components/         # Svelte components
    stores/             # Svelte stores
    types/              # TypeScript interfaces
src-tauri/              # Rust backend
  src/
    lib.rs              # Tauri app setup, command registration
    main.rs             # Entry point
    commands.rs         # Tauri IPC commands
    db.rs               # DuckDB connection management
    loader.rs           # File → DuckDB loading
    types.rs            # Shared types (serde)
    error.rs            # Error types
    diff/               # Diff engine
      mod.rs
      schema.rs         # Schema comparison
      stats.rs          # Core diff (FULL OUTER JOIN + aggregation)
      keys.rs           # PK analysis
test-data/              # Sample CSVs for development
```

## Conventions

### Rust Explanations
When explaining Rust concepts, compare to Python/Snowflake/Postgres equivalents.
Example: "Rust's `Result<T, E>` is like Python's try/except — the compiler forces you to handle the error case."

### Implementation Style
- Small, functional increments — each piece should compile and be testable
- Explain new Rust concepts as they're introduced
- Types flow: Rust structs (serde) → JSON → TypeScript interfaces

### Tauri IPC Pattern
Frontend calls Rust via `invoke()`:
```typescript
// Frontend
const result = await invoke<TableMeta>("load_source", { path: filePath });
```
```rust
// Backend
#[tauri::command]
fn load_source(path: String, state: State<DuckDbState>) -> Result<TableMeta, String> { ... }
```

## Run Tests

```bash
# Rust tests (11 tests covering loader, schema, diff engine, keys)
cargo test --manifest-path src-tauri/Cargo.toml

# Frontend build check
npm run build
```

## Security

All IPC inputs are validated before use in SQL:
- Label allowlist ("a" or "b") prevents table name injection
- PK column validated against `information_schema.columns`
- Column filter validated against known diff columns
- File paths escaped (single quotes doubled)
- CSP enabled (`default-src 'self'`)
- Page size capped at 1000 rows
- Error messages sanitized (no path/SQL leakage to frontend)

## GitHub

- Repo: https://github.com/areese801/diff-donkey
- Branch protection on `main` — PRs required, no force push
- Renamed from `duck-diff` → `diff-donkey` on 2026-03-25

## Design Spec

Full specification: `docs/superpowers/specs/2026-03-25-diff-donkey-design.md`
