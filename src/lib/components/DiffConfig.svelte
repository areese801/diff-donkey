<script lang="ts">
  import type { ColumnInfo } from "$lib/types/diff";

  interface Props {
    columns: ColumnInfo[];
    onRunDiff: (pkColumn: string, tolerance: number | null, columnTolerances: Record<string, number> | null) => void;
    isLoading: boolean;
  }

  let { columns, onRunDiff, isLoading }: Props = $props();

  let selectedPk = $state("");
  let toleranceInput = $state("");
  let showPerColumn = $state(false);
  let perColumnInputs: Record<string, string> = $state({});

  const NUMERIC_PREFIXES = [
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "DOUBLE", "DECIMAL",
  ];

  function isNumericType(dataType: string): boolean {
    const upper = dataType.toUpperCase();
    return NUMERIC_PREFIXES.some(p => upper === p || upper.startsWith(p + "("));
  }

  let numericColumns = $derived(
    columns.filter(c => c.name !== selectedPk && isNumericType(c.data_type))
  );

  function handleRun() {
    // toleranceInput may be a number (from <input type="number">) or string
    const raw = String(toleranceInput).trim();
    const tol = raw === "" ? null : parseFloat(raw);
    if (tol !== null && (isNaN(tol) || tol < 0)) return;

    const colTols: Record<string, number> = {};
    for (const [col, val] of Object.entries(perColumnInputs)) {
      const v = String(val).trim();
      if (v === "") continue;
      const parsed = parseFloat(v);
      if (isNaN(parsed) || parsed < 0) return;
      colTols[col] = parsed;
    }

    const hasTols = Object.keys(colTols).length > 0;
    onRunDiff(selectedPk, tol, hasTols ? colTols : null);
  }
</script>

<div class="diff-config">
  <div class="config-row">
    <label for="pk-select">Primary Key Column:</label>
    <select id="pk-select" bind:value={selectedPk} disabled={isLoading}>
      <option value="" disabled>Select a column...</option>
      {#each columns as col}
        <option value={col.name}>{col.name} ({col.data_type})</option>
      {/each}
    </select>

    <label for="tolerance-input">Tolerance:</label>
    <input
      id="tolerance-input"
      type="number"
      min="0"
      step="any"
      placeholder="e.g. 0.01"
      bind:value={toleranceInput}
      disabled={isLoading}
      class="tolerance-input"
    />

    <button
      onclick={handleRun}
      disabled={!selectedPk || isLoading}
    >
      {isLoading ? "Running..." : "Run Diff"}
    </button>
  </div>

  {#if numericColumns.length > 0}
    <button
      class="toggle-per-column"
      onclick={() => showPerColumn = !showPerColumn}
      disabled={isLoading}
    >
      {showPerColumn ? "Hide" : "Show"} Per-Column Tolerances ({numericColumns.length})
    </button>

    {#if showPerColumn}
      <div class="per-column-section">
        {#each numericColumns as col}
          <div class="per-column-row">
            <span class="col-name">{col.name}</span>
            <input
              type="number"
              min="0"
              step="any"
              placeholder={toleranceInput.trim() || "default"}
              bind:value={perColumnInputs[col.name]}
              disabled={isLoading}
              class="tolerance-input"
            />
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
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 6px;
  }

  .per-column-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .col-name {
    font-size: 0.85em;
    min-width: 100px;
    text-align: right;
    color: #555;
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
      color: #aaa;
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
