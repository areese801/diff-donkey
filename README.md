<p align="center">
  <img src="diff-donkey-icon.png" alt="Diff Donkey" width="200" />
</p>

<h1 align="center">Diff Donkey</h1>

<p align="center">
  <strong>A free, local desktop app for comparing datasets — powered by DuckDB.</strong>
</p>

<p align="center">
  <a href="#features">Features</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#how-it-works">How It Works</a> &middot;
  <a href="#roadmap">Roadmap</a>
</p>

---

## Why Diff Donkey?

Datafold deprecated their open-source data diff tool, leaving a gap for data engineers who need to compare datasets across environments — prod vs staging, pre/post migration, dbt model changes. Existing tools are either CLI-only, cloud-dependent, or lack column-level granularity.

Diff Donkey fills that gap: a free, local desktop app with a Datafold-inspired tabbed UI and DuckDB as a blazing-fast analytical backend. No cloud accounts. No subscriptions. Just load two files and diff.

## Features

- **CSV and Parquet support** — drag in any CSV or Parquet file, DuckDB handles type inference automatically
- **Column-level diff statistics** — per-column match percentages, diff counts, and mini progress bars
- **Primary key analysis** — exclusive rows (only in A / only in B), duplicate PK detection, null PK counts
- **Row-level drill-down** — paginated view of differing rows with cell-level pink highlighting
- **Schema comparison** — side-by-side column listing with type match indicators
- **4-tab Datafold-style UI**:
  - **Overview** — match score, summary cards, per-column stats table
  - **Columns** — shared/exclusive columns with type comparison
  - **Primary Keys** — sub-tabs for exclusive rows and duplicate PKs
  - **Values** — per-column progress bars, filterable diff row viewer

## Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) (1.70+)
- [Node.js](https://nodejs.org/) (18+)

### Build & Run

```bash
# Clone the repo
git clone https://github.com/areese801/diff-donkey.git
cd diff-donkey

# Install frontend dependencies
npm install

# Launch dev mode (opens native window with hot reload)
npx tauri dev
```

### Try It Out

The repo includes sample test data with known differences:

1. Click **Select File** under Source A → choose `test-data/orders_a.csv`
2. Click **Select File** under Source B → choose `test-data/orders_b.csv`
3. Select `id` as the Primary Key
4. Click **Run Diff**

You'll see: 4 rows with differences, 1 row exclusive to each side, and per-column match percentages.

## How It Works

Diff Donkey implements the [Datafold diff algorithm](https://github.com/datafold/data-diff):

1. **Load** both files into DuckDB as in-memory tables
2. **FULL OUTER JOIN** on the primary key
3. **IS DISTINCT FROM** per column — correctly handles NULLs (`NULL IS DISTINCT FROM NULL` = false)
4. **Materialize** the join result as a temp table
5. **Aggregate** per-column diff counts via `SUM(is_diff_column)`

```sql
-- Core of the diff engine (simplified)
CREATE TEMP TABLE _diff_join AS
SELECT
    a."id" as pk_a, b."id" as pk_b,
    a."amount" as "amount_a", b."amount" as "amount_b",
    (a."amount" IS DISTINCT FROM b."amount")::INTEGER as "is_diff_amount"
FROM source_a a
FULL OUTER JOIN source_b b ON a."id" = b."id"
```

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Desktop Shell | [Tauri v2](https://v2.tauri.app/) | Native window, ~5MB bundle |
| Frontend | [SvelteKit](https://svelte.dev/) + TypeScript | Reactive 4-tab UI |
| Backend | [Rust](https://www.rust-lang.org/) | Diff engine, file loading, IPC |
| Data Engine | [DuckDB](https://duckdb.org/) | In-memory SQL for all diff operations |

## Project Structure

```
src/                        # SvelteKit frontend
  lib/components/           # Svelte components (8 total)
  lib/stores/               # Reactive state (config, diff results)
  lib/types/                # TypeScript interfaces mirroring Rust types
src-tauri/                  # Rust backend
  src/commands.rs           # 6 Tauri IPC commands
  src/diff/stats.rs         # Core diff engine (FULL OUTER JOIN + IS DISTINCT FROM)
  src/diff/schema.rs        # Schema comparison
  src/diff/keys.rs          # PK analysis + pagination
  src/loader.rs             # CSV/Parquet → DuckDB loading
test-data/                  # Sample CSVs with known differences
```

## Roadmap

| Version | Feature |
|---------|---------|
| **v0.1.0** | Column-level variance stats, 4-tab GUI (current) |
| v0.1.1 | Numeric tolerance (`ABS(a - b) < threshold`) |
| v0.1.2 | Composite keys (multi-column JOIN) |
| v0.1.3 | Row-level detail drill-down |
| v0.1.4 | CLI/headless mode (`diff-donkey compare a.csv b.csv --key id --output json`) |
| v0.1.5 | JSON-aware comparison (order-insensitive object matching) |
| Future | Case-insensitive comparison toggle |
| Future | Remote compute offload (Snowflake/BigQuery) for large datasets |

## License

MIT
