<script lang="ts">
  import type { TableMeta, SchemaComparison, ColumnTolerance } from "$lib/types/diff";

  interface Props {
    sourceA: TableMeta | null;
    sourceB: TableMeta | null;
    schemaComparison: SchemaComparison | null;
    isLoading: boolean;
    charDiffColumns?: Record<string, boolean>;
    onCharDiffChange?: (columns: Record<string, boolean>) => void;
    onRunDiff: (
      pkColumns: string[],
      tolerance: number | null,
      columnTolerances: Record<string, ColumnTolerance> | null,
      ignoredColumns: string[],
      whereClause: string | null,
      pkExpression: string | null,
    ) => void;
  }

  let { sourceA, sourceB, schemaComparison, isLoading, charDiffColumns = {}, onCharDiffChange, onRunDiff }: Props = $props();

  // --- Column merging ---

  interface MergedColumn {
    name: string;
    type_a: string;
    type_b: string;
    presence: "shared" | "a_only" | "b_only";
  }

  let allColumns: MergedColumn[] = $derived.by(() => {
    if (!schemaComparison) return [];
    const cols: MergedColumn[] = [];
    for (const c of schemaComparison.shared) {
      cols.push({ name: c.name, type_a: c.type_a, type_b: c.type_b, presence: "shared" });
    }
    for (const c of schemaComparison.only_in_a) {
      cols.push({ name: c.name, type_a: c.data_type, type_b: "", presence: "a_only" });
    }
    for (const c of schemaComparison.only_in_b) {
      cols.push({ name: c.name, type_a: "", type_b: c.data_type, presence: "b_only" });
    }
    return cols;
  });

  let sharedColumns = $derived(allColumns.filter(c => c.presence === "shared"));

  // --- PK state ---

  let pkMode: "columns" | "expression" = $state("columns");
  let pkExpression = $state("");
  let selectedPks: string[] = $state([]);

  // Auto-detect PK columns
  $effect(() => {
    if (sharedColumns.length > 0 && selectedPks.length === 0) {
      const autoDetected = sharedColumns
        .filter(c => {
          const lower = c.name.toLowerCase();
          return lower === "id" || lower.endsWith("_id");
        })
        .map(c => c.name);
      if (autoDetected.length > 0) {
        selectedPks = autoDetected;
      }
    }
  });

  function togglePk(name: string) {
    if (selectedPks.includes(name)) {
      selectedPks = selectedPks.filter(n => n !== name);
    } else {
      selectedPks = [...selectedPks, name];
    }
  }

  // --- Tolerance / Ignore state ---

  let whereClause = $state("");
  let perColumnMode: Record<string, string> = $state({});
  let perColumnValue: Record<string, string> = $state({});
  let perColumnCharDiff: Record<string, boolean> = $state({});
  let error: string | null = $state(null);

  // Initialize charDiff to true for all shared columns
  $effect(() => {
    for (const col of sharedColumns) {
      if (!(col.name in perColumnCharDiff)) {
        perColumnCharDiff[col.name] = true;
      }
    }
  });

  function toggleCharDiff(name: string) {
    perColumnCharDiff[name] = !perColumnCharDiff[name];
    onCharDiffChange?.({ ...perColumnCharDiff });
  }

  function toggleIgnore(name: string) {
    if (perColumnMode[name] === "ignore") {
      perColumnMode[name] = "exact";
    } else {
      perColumnMode[name] = "ignore";
    }
  }

  // --- Type helpers ---

  const NUMERIC_PREFIXES = [
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "DOUBLE", "DECIMAL",
  ];

  function isNumericType(dataType: string): boolean {
    const upper = dataType.toUpperCase();
    return NUMERIC_PREFIXES.some(p => upper === p || upper.startsWith(p + "("));
  }

  function isTimestampType(dataType: string): boolean {
    return dataType.toUpperCase().startsWith("TIMESTAMP");
  }

  function isStringType(dataType: string): boolean {
    const upper = dataType.toUpperCase();
    return upper === "VARCHAR" || upper === "TEXT" || upper.startsWith("VARCHAR(") || upper === "STRING";
  }

  // --- Run handler ---

  function handleRun() {
    const colTols: Record<string, ColumnTolerance> = {};
    const ignoredCols: string[] = [];

    for (const col of sharedColumns) {
      if (selectedPks.includes(col.name)) continue;

      const mode = perColumnMode[col.name] || "exact";
      if (mode === "ignore") {
        ignoredCols.push(col.name);
        continue;
      }
      if (mode === "exact") {
        // Check if there's a numeric precision value set directly
        const v = String(perColumnValue[col.name] || "").trim();
        if (v !== "" && isNumericType(col.type_a)) {
          const p = parseInt(v, 10);
          if (!isNaN(p) && p >= 0) {
            colTols[col.name] = { mode: "precision", precision: p };
          }
        }
        continue;
      }

      if (mode === "seconds") {
        const v = String(perColumnValue[col.name] || "").trim();
        if (v === "") continue;
        const s = parseFloat(v);
        if (isNaN(s) || s < 0) return;
        colTols[col.name] = { mode: "seconds", seconds: s };
      } else if (mode === "case_insensitive") {
        colTols[col.name] = { mode: "case_insensitive" };
      } else if (mode === "whitespace") {
        colTols[col.name] = { mode: "whitespace" };
      } else if (mode === "case_insensitive_whitespace") {
        colTols[col.name] = { mode: "case_insensitive_whitespace" };
      }
    }

    const hasTols = Object.keys(colTols).length > 0;
    const trimmedWhere = whereClause.trim();
    const expr = pkMode === "expression" ? (pkExpression.trim() || null) : null;
    const pks = pkMode === "columns" ? selectedPks : [];
    onRunDiff(pks, null, hasTols ? colTols : null, ignoredCols, trimmedWhere || null, expr);
  }
</script>

<div class="config-strip">
  <!-- Header -->
  <div class="strip-header">
    <span class="strip-title">Diff Configuration</span>
    <div class="strip-controls">
      <input
        class="where-input"
        type="text"
        bind:value={whereClause}
        placeholder="WHERE clause (e.g. status = 'active')"
        disabled={isLoading}
      />
    </div>
  </div>

  <!-- Scrollable column table -->
  {#if allColumns.length > 0}
    <div class="strip-table-wrap">
      <table class="strip-table">
        <thead>
          <tr>
            <th class="row-label"></th>
            {#each allColumns as col}
              <th class="col-header" class:dimmed={col.presence !== "shared"}>
                <span
                  class="presence-badge"
                  class:shared={col.presence === "shared"}
                  class:single={col.presence !== "shared"}
                >
                  {col.presence === "shared" ? "A \u00B7 B"
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
                  <input
                    type="checkbox"
                    checked={selectedPks.includes(col.name)}
                    onchange={() => togglePk(col.name)}
                    disabled={isLoading}
                  />
                {:else}
                  <span class="na">&mdash;</span>
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
                  <input
                    type="checkbox"
                    checked={perColumnMode[col.name] === "ignore"}
                    onchange={() => toggleIgnore(col.name)}
                    disabled={isLoading}
                  />
                {:else}
                  <span class="na">&mdash;</span>
                {/if}
              </td>
            {/each}
          </tr>
          <!-- Tolerance row -->
          <tr>
            <td class="row-label">Tolerance</td>
            {#each allColumns as col}
              <td class="col-cell" class:dimmed={col.presence !== "shared"}>
                {#if col.presence === "shared" && !selectedPks.includes(col.name) && perColumnMode[col.name] !== "ignore"}
                  {#if isNumericType(col.type_a)}
                    <input
                      type="number"
                      class="tol-input"
                      placeholder="dp"
                      bind:value={perColumnValue[col.name]}
                      disabled={isLoading}
                    />
                  {:else if isTimestampType(col.type_a)}
                    <select class="tol-select" bind:value={perColumnMode[col.name]} disabled={isLoading}>
                      <option value="exact">None</option>
                      <option value="seconds">Sec</option>
                    </select>
                    {#if perColumnMode[col.name] === "seconds"}
                      <input
                        type="number"
                        class="tol-input"
                        placeholder="s"
                        bind:value={perColumnValue[col.name]}
                        disabled={isLoading}
                      />
                    {/if}
                  {:else if isStringType(col.type_a)}
                    <select class="tol-select" bind:value={perColumnMode[col.name]} disabled={isLoading}>
                      <option value="exact">None</option>
                      <option value="case_insensitive">Case</option>
                      <option value="whitespace">Trim</option>
                      <option value="case_insensitive_whitespace">Case+Trim</option>
                    </select>
                  {:else}
                    <span class="na">&mdash;</span>
                  {/if}
                {:else}
                  <span class="na">&mdash;</span>
                {/if}
              </td>
            {/each}
          </tr>
          <!-- Char Diff row (string columns only) -->
          <tr>
            <td class="row-label">Highlight</td>
            {#each allColumns as col}
              <td class="col-cell" class:dimmed={col.presence !== "shared"}>
                {#if col.presence === "shared" && !selectedPks.includes(col.name) && perColumnMode[col.name] !== "ignore"}
                  <input
                    type="checkbox"
                    checked={perColumnCharDiff[col.name] !== false}
                    onchange={() => toggleCharDiff(col.name)}
                    disabled={isLoading}
                  />
                {:else}
                  <span class="na">&mdash;</span>
                {/if}
              </td>
            {/each}
          </tr>
        </tbody>
      </table>
    </div>
  {/if}

  <!-- Footer -->
  <div class="strip-footer">
    <div class="pk-mode-toggle">
      <button class="mode-btn" class:active={pkMode === "columns"} onclick={() => pkMode = "columns"} disabled={isLoading}>
        Columns
      </button>
      <button class="mode-btn" class:active={pkMode === "expression"} onclick={() => pkMode = "expression"} disabled={isLoading}>
        Expression
      </button>
    </div>
    {#if pkMode === "expression"}
      <input
        class="expr-input"
        type="text"
        bind:value={pkExpression}
        placeholder="e.g. CONCAT(first_name, '_', last_name)"
        disabled={isLoading}
      />
    {/if}
    <button
      class="run-btn"
      onclick={handleRun}
      disabled={(pkMode === "columns" ? selectedPks.length === 0 : pkExpression.trim() === "") || isLoading}
    >
      {isLoading ? "Running..." : "Run Diff"}
    </button>
  </div>

  {#if error}
    <p class="strip-error">{error}</p>
  {/if}
</div>

<style>
  .config-strip {
    border: 1px solid #b8cce8;
    border-radius: 8px;
    padding: 12px;
    background: #eef3fb;
    margin: 16px 0;
  }

  .strip-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 10px;
    flex-wrap: wrap;
    gap: 8px;
  }

  .strip-title {
    font-size: 0.9em;
    font-weight: 600;
    color: #396cd8;
  }

  .strip-controls {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .where-input {
    font-size: 0.8em;
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: white;
    color: inherit;
    min-width: 200px;
  }

  /* Scrollable table */
  .strip-table-wrap {
    overflow-x: auto;
    margin-bottom: 10px;
  }

  .strip-table {
    border-collapse: collapse;
    min-width: max-content;
    width: 100%;
    font-size: 0.8em;
  }

  .strip-table th,
  .strip-table td {
    padding: 4px 12px;
    text-align: center;
    white-space: nowrap;
  }

  .strip-table thead tr {
    border-bottom: 2px solid #b8cce8;
  }

  .strip-table tbody tr {
    border-bottom: 1px solid #d8e4f0;
  }

  .col-header {
    vertical-align: bottom;
  }

  .presence-badge {
    display: inline-block;
    font-size: 0.75em;
    font-weight: 700;
    border-radius: 3px;
    padding: 0 4px;
    margin-bottom: 2px;
    letter-spacing: 0.5px;
  }

  .presence-badge.shared {
    color: #396cd8;
    background: #dce8f8;
    border: 1px solid #b8cce8;
  }

  .presence-badge.single {
    color: #b87a00;
    background: #fdf0d5;
    border: 1px solid #e8d5a0;
  }

  .col-name {
    display: block;
    font-weight: 600;
    color: #333;
  }

  .row-label {
    text-align: left !important;
    color: #888;
    font-size: 0.85em;
    text-transform: uppercase;
    font-weight: 600;
    min-width: 65px;
    letter-spacing: 0.3px;
  }

  .col-cell {
    color: #666;
    font-size: 0.9em;
  }

  .dimmed {
    opacity: 0.4;
  }

  .na {
    color: #ccc;
  }

  .tol-input {
    width: 40px;
    padding: 2px 4px;
    font-size: 0.9em;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: white;
    color: inherit;
    text-align: center;
  }

  .tol-select {
    font-size: 0.9em;
    padding: 1px 4px;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: white;
    color: inherit;
  }

  /* Footer */
  .strip-footer {
    display: flex;
    align-items: center;
    gap: 10px;
    flex-wrap: wrap;
  }

  .pk-mode-toggle {
    display: flex;
    gap: 0;
    border-radius: 4px;
    overflow: hidden;
    border: 1px solid #ccc;
  }

  .mode-btn {
    padding: 4px 10px;
    font-size: 0.8em;
    font-weight: 500;
    border: none;
    border-radius: 0;
    background: #e8e8e8;
    color: #555;
    cursor: pointer;
  }

  .mode-btn.active {
    background: #396cd8;
    color: white;
  }

  .mode-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .expr-input {
    flex: 1;
    min-width: 200px;
    padding: 4px 8px;
    font-size: 0.8em;
    font-family: monospace;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: white;
    color: inherit;
  }

  .run-btn {
    margin-left: auto;
    padding: 6px 24px;
    border-radius: 6px;
    border: none;
    background: #396cd8;
    color: white;
    font-weight: 600;
    cursor: pointer;
    font-size: 0.9em;
  }

  .run-btn:hover:not(:disabled) {
    background: #2d5bbf;
  }

  .run-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .strip-error {
    color: #e74c3c;
    font-size: 0.85em;
    margin-top: 8px;
    padding: 6px 10px;
    background: #ffeaea;
    border-radius: 4px;
  }

  /* Dark mode */
  @media (prefers-color-scheme: dark) {
    .config-strip {
      background: #1a2233;
      border-color: #3a5a8a;
    }

    .strip-title {
      color: #8ab4f8;
    }

    .strip-table thead tr {
      border-bottom-color: #3a4a6a;
    }

    .strip-table tbody tr {
      border-bottom-color: #2a3a5a;
    }

    .presence-badge.shared {
      color: #8ab4f8;
      background: #1a2a4a;
      border-color: #3a5a8a;
    }

    .presence-badge.single {
      color: #f39c12;
      background: #3a2a0a;
      border-color: #5a4a1a;
    }

    .col-name {
      color: #e0e0e0;
    }

    .col-cell {
      color: #888;
    }

    .na {
      color: #555;
    }

    .where-input,
    .tol-input,
    .tol-select,
    .expr-input {
      background: #2a2a2a;
      border-color: #555;
      color: #ccc;
    }

    .pk-mode-toggle {
      border-color: #555;
    }

    .mode-btn {
      background: #444;
      color: #aaa;
    }

    .mode-btn.active {
      background: #396cd8;
      color: white;
    }

    .strip-error {
      background: #3a2020;
    }
  }
</style>
