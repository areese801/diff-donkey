# UI Redesign Phase 1 — Layout, Config Strip, Progressive Disclosure

**Date:** 2026-04-07
**Status:** Draft

## Overview

Redesign the Diff Donkey UI to better use screen real estate, consolidate column configuration into a single shared strip, and introduce progressive disclosure so the results area dominates after running a diff.

**Phase 1 scope only.** After implementing these changes, reconvene for a second UI pass covering the results tabs, color scheme, and any remaining polish.

## Problem

On a widescreen monitor, the current UI has several issues:

1. **Wasted horizontal space** — hard-capped at 1100px, leaving huge empty margins
2. **Source panels list columns vertically** — takes excessive vertical space, columns listed 3x (Source A, Source B, and the config bar's PK selector)
3. **Config bar is cluttered** — PK multi-select, precision, ignore case, WHERE clause, per-column tolerances, and Run Diff all crammed into one horizontal row
4. **No visual hierarchy** — setup and results have equal visual weight with no separation between configure and analyze modes

## Design Decisions

### 1. Fluid Layout

- Remove the `max-width: 1100px` constraint on `.container`
- Use percentage-based padding: `padding: 0 3%`
- Add a safety-rail max-width of `~1800px` to prevent absurdly wide lines on ultra-wide monitors
- Same width for all sections (no hybrid complexity)
- Horizontal scroll on data tables when columns exceed available width (expected behavior, not a problem to solve)

### 2. Simplified Source Panels

Source panels become compact — just the data source picker and summary stats:

- Mode toggle (File / Database / Remote)
- File picker or database connection UI
- Summary line: **10 rows** · 5 columns

Column metadata (names, types) is **removed** from source panels entirely. It moves to the shared config strip (see below).

### 3. Shared Config Strip

A new component that appears **between the source panels and the results tabs**, only after both sources are loaded. This is the single place to see all columns and configure the diff.

**Layout:** A horizontal table (spreadsheet-style) that scrolls horizontally when there are many columns.

**Column headers:** Each column has a presence badge above the name:
- `A · B` (blue badge) — column exists in both sources; full config available
- `A` (orange badge) — column only in Source A; shown grayed out, non-configurable
- `B` (orange badge) — column only in Source B; shown grayed out, non-configurable

**Table rows:**
| Row | Description |
|-----|-------------|
| Type | Data type from DuckDB (read-only) |
| PK | Checkbox — mark as primary key (shared columns only) |
| Ignore | Checkbox — exclude from diff (shared columns only, not available on PK columns) |
| Tolerance | Type-appropriate control: dropdown for VARCHAR/DATE types (None, Case, Trim, Seconds), numeric input for DOUBLE/FLOAT (decimal places). Shared columns only, not available on PK columns. |

**Header area** (above the table):
- Label: "Diff Configuration"
- Ignore Case toggle (global default — applies to all VARCHAR columns unless overridden by a per-column tolerance setting)
- WHERE clause text input

**Footer area** (below the table):
- Columns / Expression toggle (for PK expression mode)
- Run Diff button

**What this replaces:**
- Column list in Source A panel → gone
- Column list in Source B panel → gone
- DiffConfig bar (PK multi-select, precision, per-column tolerances) → gone
- The "Show Per-Column Tolerances" expandable section → gone (tolerances are inline now)
- Global precision input → gone (replaced by per-column tolerance on numeric columns)

### 4. Progressive Disclosure — Setup Collapse

After clicking Run Diff, the setup area (source panels + shared config strip) collapses to a **compact summary bar** with a disclosure arrow toggle:

**Collapsed state:**
```
▶ orders_a.csv (10 rows) vs orders_b.csv (10 rows) · PK: id · 5 cols compared   [▼]
```

- Disclosure arrow (`▶` / `▼`) toggles expand/collapse, consistent with the existing Activity Log pattern
- Summary shows: source names, row counts, PK column(s), number of compared columns
- Clicking the arrow or bar expands back to full setup for reconfiguration

**Expanded state:** Full source panels + shared config strip, same as before running the diff.

**Behavior:**
- Before first diff: setup is fully expanded, no summary bar
- After Run Diff succeeds: setup collapses, results tabs take over the viewport
- After Run Diff fails (validation error, no PK selected, etc.): setup stays expanded, error displayed inline in the config strip
- User can expand/collapse freely after that
- Re-running diff (after changing config) collapses again on success

### 5. Impact on Existing Components

**SourceSelector.svelte** — Simplified: remove column metadata display. Keep mode toggle, file picker, database source, remote source, and summary stats.

**DiffConfig.svelte** — Replaced by the new shared config strip component. This component is removed or gutted.

**New component: DiffConfigStrip.svelte** — The shared horizontal config strip. Receives column info from both sources, emits DiffConfig on Run Diff.

**ColumnsTab.svelte** — The "Shared Columns" table becomes partially redundant since the config strip already shows column presence, types, and match info. Consider slimming this tab or repurposing it (deferred to Phase 2 UI pass).

**+page.svelte** — Orchestrates the collapse/expand behavior. Manages state: `setupCollapsed: boolean`.

## Out of Scope (Phase 2)

After Phase 1 is implemented, reconvene for a second UI pass covering:

- Results tabs layout and visual treatment
- Color scheme and theming refinements
- Typography and spacing polish
- ColumnsTab redundancy (slim down or repurpose)
- Any issues discovered during Phase 1 implementation
