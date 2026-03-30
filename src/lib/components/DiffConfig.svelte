<script lang="ts">
  import type { ColumnInfo } from "$lib/types/diff";
  import type { ColumnTolerance } from "$lib/types/diff";

  interface Props {
    columns: ColumnInfo[];
    onRunDiff: (pkColumns: string[], tolerance: number | null, columnTolerances: Record<string, ColumnTolerance> | null) => void;
    isLoading: boolean;
  }

  let { columns, onRunDiff, isLoading }: Props = $props();

  let selectedPks: string[] = $state([]);
  let precisionInput = $state("");

  // Auto-select PK columns: first column named "id" or ending in "_id"
  $effect(() => {
    if (columns.length > 0 && selectedPks.length === 0) {
      const autoDetected = columns
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
  let ignoreCase = $state(false);
  let showPerColumn = $state(false);
  let perColumnMode: Record<string, string> = $state({});
  let perColumnValue: Record<string, string> = $state({});

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

  let nonPkColumns = $derived(
    columns.filter(c => !selectedPks.includes(c.name))
  );

  function modesForType(dataType: string): { value: string; label: string }[] {
    if (isNumericType(dataType)) {
      return [
        { value: "default", label: "Default" },
        { value: "precision", label: "Custom Precision" },
        { value: "exact", label: "Exact" },
      ];
    }
    if (isTimestampType(dataType)) {
      return [
        { value: "exact", label: "Exact" },
        { value: "seconds", label: "Within N Seconds" },
      ];
    }
    return [
      { value: "exact", label: "Exact" },
      { value: "case_insensitive", label: "Case Insensitive" },
      { value: "whitespace", label: "Trim Whitespace" },
      { value: "case_insensitive_whitespace", label: "Case + Trim" },
    ];
  }

  function handleRun() {
    const raw = String(precisionInput).trim();
    const prec = raw === "" ? null : parseInt(raw, 10);
    if (prec !== null && isNaN(prec)) return;

    const colTols: Record<string, ColumnTolerance> = {};
    for (const col of nonPkColumns) {
      const mode = perColumnMode[col.name] || "default";
      if (mode === "default" || mode === "exact") continue;

      if (mode === "precision") {
        const v = String(perColumnValue[col.name] || "").trim();
        if (v === "") continue;
        const p = parseInt(v, 10);
        if (isNaN(p) || p < 0) return;
        colTols[col.name] = { mode: "precision", precision: p };
      } else if (mode === "seconds") {
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

    // Apply global case-insensitive toggle to string columns not already overridden
    if (ignoreCase) {
      for (const col of nonPkColumns) {
        if (col.name in colTols) continue; // per-column override takes precedence
        if (!isNumericType(col.data_type) && !isTimestampType(col.data_type)) {
          colTols[col.name] = { mode: "case_insensitive" };
        }
      }
    }

    const hasTols = Object.keys(colTols).length > 0;
    onRunDiff(selectedPks, prec, hasTols ? colTols : null);
  }

  function needsValueInput(mode: string): boolean {
    return mode === "precision" || mode === "seconds";
  }
</script>

<div class="diff-config">
  <div class="config-row">
    <label for="pk-select">Primary Key:</label>
    <select id="pk-select" multiple bind:value={selectedPks} disabled={isLoading} class="pk-multi-select">
      {#each columns as col}
        <option value={col.name}>{col.name} ({col.data_type})</option>
      {/each}
    </select>

    <label for="precision-input" title="Positive = decimal places (2 → hundredths). Negative = integer rounding (-1 → nearest 10, -2 → nearest 100).">Precision:</label>
    <input
      id="precision-input"
      type="number"
      step="1"
      placeholder="e.g. 2"
      bind:value={precisionInput}
      disabled={isLoading}
      class="tolerance-input"
    />

    <label class="ignore-case-toggle">
      <input type="checkbox" bind:checked={ignoreCase} disabled={isLoading} />
      Ignore Case
    </label>

    <button
      onclick={handleRun}
      disabled={selectedPks.length === 0 || isLoading}
    >
      {isLoading ? "Running..." : "Run Diff"}
    </button>
  </div>

  {#if nonPkColumns.length > 0}
    <button
      class="toggle-per-column"
      onclick={() => showPerColumn = !showPerColumn}
      disabled={isLoading}
    >
      {showPerColumn ? "Hide" : "Show"} Per-Column Tolerances ({nonPkColumns.length})
    </button>

    {#if showPerColumn}
      <div class="per-column-section">
        {#each nonPkColumns as col}
          <div class="per-column-row">
            <span class="col-name" title={col.data_type}>{col.name}</span>
            <span class="col-type">{col.data_type}</span>
            <select
              bind:value={perColumnMode[col.name]}
              disabled={isLoading}
              class="mode-select"
            >
              {#each modesForType(col.data_type) as opt}
                <option value={opt.value}>{opt.label}</option>
              {/each}
            </select>
            {#if needsValueInput(perColumnMode[col.name])}
              <input
                type="number"
                min="0"
                step={perColumnMode[col.name] === "precision" ? "1" : "any"}
                placeholder={perColumnMode[col.name] === "precision" ? "decimal places" : "seconds"}
                bind:value={perColumnValue[col.name]}
                disabled={isLoading}
                class="tolerance-input"
              />
            {/if}
          </div>
        {/each}
      </div>
    {/if}
  {/if}
</div>

<style>
  .diff-config {
    padding: 12px 16px;
    background: #f0f0f0;
    border-radius: 8px;
    margin-bottom: 16px;
  }

  .config-row {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .pk-multi-select {
    min-height: 60px;
    max-height: 100px;
    min-width: 180px;
  }

  .ignore-case-toggle {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 0.85em;
    cursor: pointer;
    user-select: none;
  }

  .ignore-case-toggle input {
    cursor: pointer;
  }

  label {
    font-weight: 500;
    font-size: 0.9em;
    white-space: nowrap;
  }

  select {
    padding: 6px 10px;
    border-radius: 4px;
    border: 1px solid #ccc;
    font-size: 0.9em;
    background: white;
    color: inherit;
  }

  .tolerance-input {
    width: 100px;
    padding: 6px 10px;
    border-radius: 4px;
    border: 1px solid #ccc;
    font-size: 0.9em;
    background: white;
    color: inherit;
  }

  button {
    padding: 8px 20px;
    border-radius: 6px;
    border: none;
    background: #396cd8;
    color: white;
    font-weight: 600;
    cursor: pointer;
    font-size: 0.9em;
  }

  button:hover:not(:disabled) {
    background: #2d5bbf;
  }

  button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .toggle-per-column {
    margin-top: 8px;
    padding: 4px 12px;
    font-size: 0.8em;
    font-weight: 500;
    background: transparent;
    color: #396cd8;
    border: 1px solid #396cd8;
  }

  .toggle-per-column:hover:not(:disabled) {
    background: rgba(57, 108, 216, 0.1);
  }

  .per-column-section {
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .per-column-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .col-name {
    font-size: 0.85em;
    min-width: 120px;
    text-align: right;
    font-weight: 500;
  }

  .col-type {
    font-size: 0.75em;
    color: #888;
    min-width: 80px;
  }

  .mode-select {
    font-size: 0.85em;
    min-width: 140px;
  }

  @media (prefers-color-scheme: dark) {
    .diff-config {
      background: #383838;
    }

    select,
    .tolerance-input {
      background: #2f2f2f;
      border-color: #555;
    }

    .col-name {
      color: #ddd;
    }

    .col-type {
      color: #888;
    }

    .toggle-per-column {
      color: #6b9aff;
      border-color: #6b9aff;
    }

    .toggle-per-column:hover:not(:disabled) {
      background: rgba(107, 154, 255, 0.1);
    }
  }
</style>
