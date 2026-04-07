# UI Redesign Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign the Diff Donkey UI to use fluid layout, consolidate column config into a shared horizontal strip, and collapse setup after running a diff.

**Architecture:** Four sequential changes to the SvelteKit frontend — no Rust backend changes. (1) Widen the layout, (2) create a new DiffConfigStrip component that replaces both the column metadata in SourceSelector and the DiffConfig component, (3) simplify SourceSelector by removing column lists, (4) add progressive disclosure so setup collapses after diff runs.

**Tech Stack:** SvelteKit 5 (runes mode — `$state`, `$derived`, `$effect`), TypeScript, Tauri v2 IPC

**Spec:** `docs/superpowers/specs/2026-04-07-ui-redesign-phase1-design.md`

**Verification commands:**
- Type check: `npm run check` (expect only 3 pre-existing errors in `+page.svelte`)
- Rust tests: `cargo test --manifest-path src-tauri/Cargo.toml` (expect 167 passing — no backend changes)
- Visual: `npx tauri dev` to launch and inspect

---

## Chunk 1: Layout + DiffConfigStrip Component

### Task 1: Fluid layout

Change the `.container` CSS from a fixed 1100px cap to fluid with a generous safety rail.

**Files:**
- Modify: `src/routes/+page.svelte` (lines 136–141)

- [ ] **Step 1: Update `.container` CSS**

In `src/routes/+page.svelte`, replace lines 136–141:

```css
/* Before */
.container {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px;
  flex: 1;
}

/* After */
.container {
  max-width: 1800px;
  margin: 0 auto;
  padding: 24px 3%;
  flex: 1;
}
```

- [ ] **Step 2: Run type check**

Run: `npm run check`
Expected: same 3 pre-existing errors, no new ones.

- [ ] **Step 3: Commit**

```bash
git add src/routes/+page.svelte
git commit -m "style: fluid layout — widen container to 1800px with 3% padding"
```

---

### Task 2: Create DiffConfigStrip component

This is the core new component — a horizontal spreadsheet-style table showing all columns with presence badges, PK checkboxes, Ignore checkboxes, and tolerance controls.

**Files:**
- Create: `src/lib/components/DiffConfigStrip.svelte`

**Props interface:**
```typescript
interface Props {
  sourceA: TableMeta | null;
  sourceB: TableMeta | null;
  schemaComparison: SchemaComparison | null;
  isLoading: boolean;
  onRunDiff: (
    pkColumns: string[],
    tolerance: number | null,
    columnTolerances: Record<string, ColumnTolerance> | null,
    ignoredColumns: string[],
    whereClause: string | null,
    pkExpression: string | null,
  ) => void;
}
```

The component must:
1. Derive a merged column list from both sources + schemaComparison, categorized as "shared", "a_only", or "b_only"
2. Display columns horizontally in a `<table>` that scrolls with `overflow-x: auto`
3. Show presence badges: `A · B` (blue) for shared, `A` or `B` (orange) for single-source
4. Render rows: Type, PK (checkbox), Ignore (checkbox), Tolerance (type-appropriate control)
5. Gray out and disable controls for single-source columns
6. Auto-detect PK columns (name === "id" or ends with "_id") — same logic as current DiffConfig
7. Header: "Diff Configuration" label, global Ignore Case checkbox, WHERE clause input
8. Footer: Columns/Expression toggle, Run Diff button
9. Emit the same `onRunDiff` callback signature as the current DiffConfig

- [ ] **Step 1: Create the component file**

Write `src/lib/components/DiffConfigStrip.svelte`. This is a large component. Key sections:

**Script section** — port all logic from `DiffConfig.svelte` (lines 1–143), adapting:
- Instead of receiving `columns: ColumnInfo[]` (shared only), derive the full column list from `sourceA`, `sourceB`, and `schemaComparison`
- Each column entry: `{ name, type_a, type_b, presence: "shared" | "a_only" | "b_only" }`
- PK checkboxes only enabled for shared columns
- Ignore checkboxes only enabled for shared non-PK columns
- Tolerance controls only enabled for shared non-PK columns
- Keep `isNumericType`, `isTimestampType`, `isStringType`, `modesForType` helpers
- Keep auto-PK detection `$effect`
- Keep `handleRun()` logic that builds `colTols`, `ignoredCols`, applies global ignoreCase
- **Important:** When porting `handleRun()`, the merged column list uses `type_a`/`type_b` fields, not `data_type`. Update references like `isStringType(col.data_type)` to `isStringType(col.type_a)`. The tolerance UI template already uses `col.type_a` correctly — just ensure `handleRun` matches.
- **Important:** The old `modesForType` includes `"default"` and `"precision"` modes from the global precision flow. Simplify: numeric columns now use a direct number input (no mode dropdown), so `handleRun` should treat a non-empty numeric value as `{ mode: "precision", precision: N }` directly.

**Template section** — the horizontal table:
```svelte
<div class="config-strip">
  <!-- Header -->
  <div class="strip-header">
    <span class="strip-title">Diff Configuration</span>
    <div class="strip-controls">
      <label class="global-toggle">
        <input type="checkbox" bind:checked={ignoreCase}> Ignore Case
      </label>
      <input class="where-input" type="text" bind:value={whereClause}
        placeholder="WHERE clause (e.g. status = 'active')" />
    </div>
  </div>

  <!-- Scrollable table -->
  <div class="strip-table-wrap">
    <table class="strip-table">
      <thead>
        <tr>
          <th class="row-label"></th>
          {#each allColumns as col}
            <th class="col-header" class:dimmed={col.presence !== "shared"}>
              <span class="presence-badge" class:shared={col.presence === "shared"}
                class:single={col.presence !== "shared"}>
                {col.presence === "shared" ? "A · B"
                  : col.presence === "a_only" ? "A" : "B"}
              </span>
              <span class="col-name">{col.name}</span>
            </th>
          {/each}
        </tr>
      </thead>
      <tbody>
        <!-- Type row -->
        <tr>
          <td class="row-label">Type</td>
          {#each allColumns as col}
            <td class="col-cell" class:dimmed={col.presence !== "shared"}>
              {col.presence === "shared" ? col.type_a
                : col.presence === "a_only" ? col.type_a : col.type_b}
            </td>
          {/each}
        </tr>
        <!-- PK row -->
        <tr>
          <td class="row-label">PK</td>
          {#each allColumns as col}
            <td class="col-cell" class:dimmed={col.presence !== "shared"}>
              {#if col.presence === "shared"}
                <input type="checkbox"
                  checked={selectedPks.includes(col.name)}
                  onchange={() => togglePk(col.name)}
                  disabled={isLoading} />
              {:else}
                <span class="na">—</span>
              {/if}
            </td>
          {/each}
        </tr>
        <!-- Ignore row -->
        <tr>
          <td class="row-label">Ignore</td>
          {#each allColumns as col}
            <td class="col-cell" class:dimmed={col.presence !== "shared"}>
              {#if col.presence === "shared" && !selectedPks.includes(col.name)}
                <input type="checkbox"
                  checked={perColumnMode[col.name] === "ignore"}
                  onchange={() => toggleIgnore(col.name)}
                  disabled={isLoading} />
              {:else}
                <span class="na">—</span>
              {/if}
            </td>
          {/each}
        </tr>
        <!-- Tolerance row -->
        <tr>
          <td class="row-label">Tolerance</td>
          {#each allColumns as col}
            <td class="col-cell" class:dimmed={col.presence !== "shared"}>
              {#if col.presence === "shared" && !selectedPks.includes(col.name)
                   && perColumnMode[col.name] !== "ignore"}
                <!-- Type-appropriate control -->
                {#if isNumericType(col.type_a)}
                  <input type="number" class="tol-input" placeholder="dp"
                    bind:value={perColumnValue[col.name]}
                    disabled={isLoading} />
                {:else if isTimestampType(col.type_a)}
                  <select class="tol-select" bind:value={perColumnMode[col.name]}
                    disabled={isLoading}>
                    <option value="exact">None</option>
                    <option value="seconds">Seconds</option>
                  </select>
                  {#if perColumnMode[col.name] === "seconds"}
                    <input type="number" class="tol-input" placeholder="s"
                      bind:value={perColumnValue[col.name]}
                      disabled={isLoading} />
                  {/if}
                {:else if isStringType(col.type_a)}
                  <select class="tol-select" bind:value={perColumnMode[col.name]}
                    disabled={isLoading}>
                    <option value="exact">None</option>
                    <option value="case_insensitive">Case</option>
                    <option value="whitespace">Trim</option>
                    <option value="case_insensitive_whitespace">Case+Trim</option>
                  </select>
                {:else}
                  <span class="na">—</span>
                {/if}
              {:else}
                <span class="na">—</span>
              {/if}
            </td>
          {/each}
        </tr>
      </tbody>
    </table>
  </div>

  <!-- Footer -->
  <div class="strip-footer">
    <div class="pk-mode-toggle">
      <button class:active={pkMode === "columns"} onclick={() => pkMode = "columns"}
        disabled={isLoading}>Columns</button>
      <button class:active={pkMode === "expression"} onclick={() => pkMode = "expression"}
        disabled={isLoading}>Expression</button>
    </div>
    {#if pkMode === "expression"}
      <input class="expr-input" type="text" bind:value={pkExpression}
        placeholder="e.g. CONCAT(first_name, '_', last_name)" disabled={isLoading} />
    {/if}
    <button class="run-btn" onclick={handleRun} disabled={isLoading}>
      {isLoading ? "Running..." : "Run Diff"}
    </button>
  </div>

  {#if error}
    <p class="strip-error">{error}</p>
  {/if}
</div>
```

**Style section** — scoped CSS covering:
- `.config-strip`: border, border-radius, padding, background tint (light blue in light mode, dark blue-gray in dark mode)
- `.strip-table-wrap`: `overflow-x: auto`
- `.strip-table`: `border-collapse: collapse; min-width: max-content; width: 100%`
- `.col-header`: centered, with badge above name
- `.presence-badge.shared`: blue badge (`background: #1a2a4a; color: #8ab4f8; border: 1px solid #3a5a8a`)
- `.presence-badge.single`: orange badge (`background: #3a2a0a; color: #f39c12; border: 1px solid #5a4a1a`)
- `.dimmed`: `opacity: 0.4`
- `.row-label`: left-aligned, muted color, small caps
- `.tol-input`: small number input, ~40px wide
- `.tol-select`: small select dropdown
- `.run-btn`: styled like current Run Diff (`background: #396cd8; color: white`)
- `.pk-mode-toggle`: button group matching current style
- Dark mode overrides via `@media (prefers-color-scheme: dark)`

- [ ] **Step 2: Run type check**

Run: `npm run check`
Expected: same 3 pre-existing errors. The new component isn't wired in yet, but it should compile.

- [ ] **Step 3: Commit**

```bash
git add src/lib/components/DiffConfigStrip.svelte
git commit -m "feat: add DiffConfigStrip — horizontal column config with badges"
```

---

### Task 3: Simplify SourceSelector — remove column metadata

Remove the `<ul class="columns">` blocks from all four places in SourceSelector (file mode A, remote mode A, file mode B, remote mode B). Keep only the row count summary, changing it to include column count.

**Files:**
- Modify: `src/lib/components/SourceSelector.svelte`

- [ ] **Step 1: Replace column metadata blocks**

In `src/lib/components/SourceSelector.svelte`, find all four instances of the column list pattern:

```svelte
{#if metaX && modeX === "file"|"remote"}
  <div class="meta">
    <p class="row-count">{metaX.row_count.toLocaleString()} rows</p>
    <ul class="columns">
      {#each metaX.columns as col}
        <li><code>{col.name}</code> <span class="type">{col.data_type}</span></li>
      {/each}
    </ul>
  </div>
{/if}
```

Replace each with a compact summary:

```svelte
{#if metaX && modeX === "file"|"remote"}
  <div class="meta">
    <p class="row-count">
      <strong>{metaX.row_count.toLocaleString()} rows</strong> · {metaX.columns.length} columns
    </p>
  </div>
{/if}
```

There are four instances to change in SourceSelector.svelte:
- Source A file mode (lines ~252–261)
- Source A remote mode (lines ~300–309)
- Source B file mode (lines ~355–364)
- Source B remote mode (lines ~403–412)

Additionally, `DatabaseSource.svelte` has the same column list pattern (around line 580). Apply the same simplification there — replace the `<ul class="columns">` block with the compact summary.

- [ ] **Step 2: Remove unused CSS**

Remove `.columns`, `.columns li`, `.columns code`, and `.type` styles from SourceSelector since they're no longer used.

- [ ] **Step 3: Run type check**

Run: `npm run check`
Expected: same 3 pre-existing errors, no new ones.

- [ ] **Step 4: Commit**

```bash
git add src/lib/components/SourceSelector.svelte src/lib/components/DatabaseSource.svelte
git commit -m "style: simplify source panels — remove column lists, show compact summary"
```

---

## Chunk 2: Wire Up + Progressive Disclosure

### Task 4: Wire DiffConfigStrip into +page.svelte, replace DiffConfig

Replace the `<DiffConfig>` component with `<DiffConfigStrip>` in the page, passing the new props.

**Files:**
- Modify: `src/routes/+page.svelte`

- [ ] **Step 1: Update imports and component**

In `src/routes/+page.svelte`:

Replace the import (line 4):
```typescript
// Before
import DiffConfig from "$lib/components/DiffConfig.svelte";
// After
import DiffConfigStrip from "$lib/components/DiffConfigStrip.svelte";
```

Replace the component usage (lines 82–86):
```svelte
<!-- Before -->
<DiffConfig
  columns={sharedColumns}
  onRunDiff={handleRunDiff}
  isLoading={$isLoading}
/>

<!-- After -->
<DiffConfigStrip
  sourceA={$sourceA}
  sourceB={$sourceB}
  schemaComparison={schemaComparison}
  onRunDiff={handleRunDiff}
  isLoading={$isLoading}
/>
```

The `sharedColumns` derived variable (lines 21–24) can be removed since DiffConfigStrip derives its own column list internally.

- [ ] **Step 2: Run type check**

Run: `npm run check`
Expected: same 3 pre-existing errors.

- [ ] **Step 3: Visual test**

Run: `npx tauri dev`
- Load two CSVs (test-data/orders_a.csv and orders_b.csv)
- Verify: source panels show compact summary (row count + column count, no column list)
- Verify: shared config strip appears with horizontal column table
- Verify: presence badges show `A · B` for all shared columns
- Verify: PK checkbox auto-selects `id`
- Verify: Run Diff works and produces results
- Verify: horizontal scroll works if window is narrowed

- [ ] **Step 4: Commit**

```bash
git add src/routes/+page.svelte
git commit -m "feat: wire DiffConfigStrip into page, replace old DiffConfig"
```

---

### Task 5: Progressive disclosure — setup collapse after diff

Add collapse/expand behavior: after Run Diff succeeds, source panels + config strip collapse to a summary bar with a disclosure arrow.

**Files:**
- Modify: `src/routes/+page.svelte`

- [ ] **Step 1: Add collapse state and summary**

In `src/routes/+page.svelte` script section, add state:

```typescript
let setupCollapsed = $state(false);
```

In `handleRunDiff`, after `diffResult.set(result)` succeeds (line ~62), add:
```typescript
setupCollapsed = true;
```

On error, setup stays expanded (already the case — `setupCollapsed` isn't set on the error path).

- [ ] **Step 2: Add summary bar template**

Derive a summary string. Note: `TableMeta.table_name` is the internal DuckDB alias (e.g. `"source_a"`), not the original filename — so the summary shows "Source A" / "Source B" as labels. Showing actual filenames would require plumbing path info from SourceSelector, which is deferred.

```typescript
let setupSummary = $derived.by(() => {
  if (!$sourceA || !$sourceB) return "";
  const pkDisplay = $pkColumn || "none";
  const colCount = schemaComparison?.shared.length ?? 0;
  return `Source A (${$sourceA.row_count.toLocaleString()} rows) vs Source B (${$sourceB.row_count.toLocaleString()} rows) · PK: ${pkDisplay} · ${colCount} cols compared`;
});
```

Reference in template as `{setupSummary}` (not `{setupSummary()}` — `$derived.by` produces a value, not a function).

In the template, wrap the setup area with the collapse toggle:

```svelte
{#if bothLoaded}
  <!-- Setup area: collapsible -->
  <div class="setup-section">
    <button class="setup-handle" onclick={() => setupCollapsed = !setupCollapsed}>
      <span class="handle-icon">{setupCollapsed ? "▶" : "▼"}</span>
      {#if setupCollapsed}
        <span class="setup-summary">{setupSummary}</span>
      {:else}
        <span class="setup-label">Configuration</span>
      {/if}
    </button>

    {#if !setupCollapsed}
      <SourceSelector />
      <DiffConfigStrip
        sourceA={$sourceA}
        sourceB={$sourceB}
        schemaComparison={schemaComparison}
        onRunDiff={handleRunDiff}
        isLoading={$isLoading}
      />
    {/if}
  </div>
{:else}
  <SourceSelector />
{/if}
```

Note: `<SourceSelector />` moves inside the collapsible section when both are loaded. Before both are loaded, it renders normally (no collapse handle).

- [ ] **Step 3: Add styles for setup collapse**

```css
.setup-section {
  margin-bottom: 16px;
}

.setup-handle {
  width: 100%;
  padding: 8px 12px;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  background: transparent;
  cursor: pointer;
  font-size: 0.85em;
  font-weight: 600;
  color: #888;
  text-align: left;
  display: flex;
  align-items: center;
  gap: 8px;
}

.setup-handle:hover {
  color: #555;
  background: #f0f0f0;
}

.setup-summary {
  font-weight: 400;
  color: #666;
}

.setup-label {
  font-weight: 600;
}

@media (prefers-color-scheme: dark) {
  .setup-handle {
    border-color: #444;
    color: #999;
  }

  .setup-handle:hover {
    color: #ccc;
    background: #3a3a3a;
  }

  .setup-summary {
    color: #aaa;
  }
}
```

- [ ] **Step 4: Run type check**

Run: `npm run check`
Expected: same 3 pre-existing errors.

- [ ] **Step 5: Visual test**

Run: `npx tauri dev`
- Load two CSVs → setup visible, no collapse bar
- Click Run Diff → setup collapses to summary bar
- Summary shows: file names, row counts, PK, column count
- Click `▶` → setup expands back
- Click `▼` → collapses again
- Change config, re-run → collapses again on success

- [ ] **Step 6: Commit**

```bash
git add src/routes/+page.svelte
git commit -m "feat: progressive disclosure — collapse setup after diff runs"
```

---

### Task 6: Cleanup

Remove the old DiffConfig component since it's been fully replaced.

**Files:**
- Delete: `src/lib/components/DiffConfig.svelte`

- [ ] **Step 1: Delete old component**

```bash
rm src/lib/components/DiffConfig.svelte
```

- [ ] **Step 2: Run type check**

Run: `npm run check`
Expected: same 3 pre-existing errors. If DiffConfig is still imported anywhere, fix those imports.

- [ ] **Step 3: Run Rust tests to verify no backend regressions**

Run: `cargo test --manifest-path src-tauri/Cargo.toml`
Expected: 167 passed, 0 failed.

- [ ] **Step 4: Final visual test**

Run: `npx tauri dev`
Full flow: load sources → config strip appears → configure PK/tolerance/ignore → Run Diff → setup collapses → browse results → expand setup → reconfigure → re-run.

- [ ] **Step 5: Commit**

```bash
git rm src/lib/components/DiffConfig.svelte
git commit -m "chore: remove old DiffConfig component, replaced by DiffConfigStrip"
```
