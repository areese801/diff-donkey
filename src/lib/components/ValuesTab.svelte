<script lang="ts">
  import DataTable from "./DataTable.svelte";
  import type { ColumnDiffStats, ValuesSummary, OverviewResult, SchemaComparison, PagedRows } from "$lib/types/diff";
  import { getDiffRows, getExclusiveRows, getDuplicatePks, exportDiffRows } from "$lib/tauri";
  import { save } from "@tauri-apps/plugin-dialog";

  interface Props {
    columnStats: ColumnDiffStats[];
    valuesSummary?: ValuesSummary;
    precision?: number | null;
    result?: OverviewResult | null;
    schemaComparison?: SchemaComparison | null;
  }

  let { columnStats, valuesSummary, precision = null, result = null, schemaComparison = null }: Props = $props();

  let selectedColumn: string | null = $state(null);
  let rowFilter: string = $state("all");
  let charDiffs = $state(true);
  let data: PagedRows | null = $state(null);
  let loading = $state(false);
  let exportMessage: string | null = $state(null);
  let exporting = $state(false);
  const PAGE_SIZE = 50;

  /** Fetch diff rows when column selection, row filter, or diff results change */
  $effect(() => {
    // Read columnStats to establish dependency — re-fetch when diff is re-run
    void columnStats;
    // Trigger on selectedColumn and rowFilter changes
    void rowFilter;
    fetchData(0);
  });

  async function fetchData(page: number) {
    loading = true;
    try {
      if (rowFilter === "exclusive_a") {
        data = await getExclusiveRows("a", page, PAGE_SIZE);
      } else if (rowFilter === "exclusive_b") {
        data = await getExclusiveRows("b", page, PAGE_SIZE);
      } else if (rowFilter === "duplicates_a") {
        data = await getDuplicatePks("a", page, PAGE_SIZE);
      } else if (rowFilter === "duplicates_b") {
        data = await getDuplicatePks("b", page, PAGE_SIZE);
      } else {
        data = await getDiffRows(page, PAGE_SIZE, selectedColumn ?? undefined, rowFilter);
      }
    } catch (e) {
      console.error("Values tab fetch error:", e);
      data = null;
    } finally {
      loading = false;
    }
  }

  async function handleExport() {
    const filepath = await save({
      filters: [
        { name: "CSV", extensions: ["csv"] },
        { name: "Parquet", extensions: ["parquet", "pq"] },
        { name: "JSON", extensions: ["json"] },
      ],
    });
    if (!filepath) return;

    let format: "csv" | "parquet" | "json";
    if (filepath.endsWith(".parquet") || filepath.endsWith(".pq")) {
      format = "parquet";
    } else if (filepath.endsWith(".json")) {
      format = "json";
    } else {
      format = "csv";
    }

    exporting = true;
    try {
      const count = await exportDiffRows(filepath, format, selectedColumn ?? undefined, rowFilter);
      const filename = filepath.split(/[/\\]/).pop() ?? filepath;
      exportMessage = `Exported ${count.toLocaleString()} rows to ${filename}`;
      setTimeout(() => { exportMessage = null; }, 3000);
    } catch (e) {
      console.error("Export error:", e);
      exportMessage = `Export failed: ${e}`;
      setTimeout(() => { exportMessage = null; }, 5000);
    } finally {
      exporting = false;
    }
  }

  /** Columns with diffs, sorted by diff count descending */
  let sortedStats = $derived(
    [...columnStats].sort((a, b) => b.diff_count - a.diff_count)
  );

  /** Row filter button label and count */
  let filterCounts = $derived(() => {
    if (!valuesSummary) return { all: 0, diffs: 0, minor: 0, same: 0 };
    return {
      all: valuesSummary.total_compared,
      diffs: valuesSummary.rows_with_diffs,
      minor: valuesSummary.rows_minor,
      same: valuesSummary.rows_identical,
    };
  });
</script>

{#if columnStats.length === 0}
  <p class="empty">Run a diff to see value comparisons.</p>
{:else}
  <div class="values-tab">
    <!-- Summary stats bar -->
    {#if result}
      <section class="stats-bar">
        <span class="stat">
          <strong>{result.total_rows_a.toLocaleString()}</strong> rows A
        </span>
        <span class="stat-sep">&middot;</span>
        <span class="stat">
          <strong>{result.total_rows_b.toLocaleString()}</strong> rows B
        </span>
        <span class="stat-sep">&middot;</span>
        <span class="stat matched">
          <strong>{result.values_summary.total_compared.toLocaleString()}</strong> matched
        </span>
        {#if result.pk_summary.exclusive_a > 0}
          <span class="stat-sep">&middot;</span>
          <span class="stat warn">
            <strong>{result.pk_summary.exclusive_a}</strong> only in A
          </span>
        {/if}
        {#if result.pk_summary.exclusive_b > 0}
          <span class="stat-sep">&middot;</span>
          <span class="stat warn">
            <strong>{result.pk_summary.exclusive_b}</strong> only in B
          </span>
        {/if}
        {#if result.pk_summary.duplicate_pks_a > 0 || result.pk_summary.duplicate_pks_b > 0}
          <span class="stat-sep">&middot;</span>
          <span class="stat warn">
            <strong>{result.pk_summary.duplicate_pks_a + result.pk_summary.duplicate_pks_b}</strong> duplicate PKs
          </span>
        {/if}
        {#if schemaComparison}
          <span class="stat-sep">&middot;</span>
          <span class="stat">
            <strong>{schemaComparison.shared.length}</strong> shared cols
          </span>
          {#if schemaComparison.only_in_a.length > 0}
            <span class="stat-sep">&middot;</span>
            <span class="stat muted">{schemaComparison.only_in_a.length} only in A</span>
          {/if}
          {#if schemaComparison.only_in_b.length > 0}
            <span class="stat-sep">&middot;</span>
            <span class="stat muted">{schemaComparison.only_in_b.length} only in B</span>
          {/if}
        {/if}
      </section>
    {/if}

    <!-- Per-column chips -->
    <section class="column-bars">
      <button
        class="filter-btn"
        class:active={selectedColumn === null}
        onclick={() => selectedColumn = null}
      >
        All columns
      </button>
      {#each sortedStats as col}
        <button
          class="column-chip"
          class:active={selectedColumn === col.name}
          onclick={() => selectedColumn = col.name}
          title="{col.name}: {col.match_pct.toFixed(1)}% match, {col.diff_count} diffs"
        >
          <span class="chip-name">{col.name}</span>
          {#if col.diff_count > 0}
            <span class="chip-count-diffs">{col.diff_count}</span>
          {/if}
        </button>
      {/each}
    </section>

    <!-- Row filter toggles -->
    <section class="row-filters">
      <button
        class="row-filter-btn"
        class:active={rowFilter === "all"}
        onclick={() => rowFilter = "all"}
      >
        All ({filterCounts().all.toLocaleString()})
      </button>
      <button
        class="row-filter-btn"
        class:active={rowFilter === "diffs"}
        onclick={() => rowFilter = "diffs"}
      >
        Diffs ({filterCounts().diffs.toLocaleString()})
      </button>
      <button
        class="row-filter-btn"
        class:active={rowFilter === "minor"}
        disabled={filterCounts().minor === 0}
        onclick={() => rowFilter = "minor"}
      >
        Minor ({filterCounts().minor.toLocaleString()})
      </button>
      <button
        class="row-filter-btn"
        class:active={rowFilter === "same"}
        onclick={() => rowFilter = "same"}
      >
        Same ({filterCounts().same.toLocaleString()})
      </button>

      {#if result && result.pk_summary.exclusive_a > 0}
        <button
          class="row-filter-btn pk-filter"
          class:active={rowFilter === "exclusive_a"}
          onclick={() => rowFilter = "exclusive_a"}
        >
          Only A ({result.pk_summary.exclusive_a})
        </button>
      {/if}
      {#if result && result.pk_summary.exclusive_b > 0}
        <button
          class="row-filter-btn pk-filter"
          class:active={rowFilter === "exclusive_b"}
          onclick={() => rowFilter = "exclusive_b"}
        >
          Only B ({result.pk_summary.exclusive_b})
        </button>
      {/if}

      <label class="char-diff-toggle">
        <input type="checkbox" bind:checked={charDiffs} />
        Char diffs
      </label>

      <button
        class="export-btn"
        onclick={handleExport}
        disabled={!data || data.total === 0 || exporting}
      >
        {exporting ? "Exporting..." : "Export"}
      </button>

      {#if exportMessage}
        <span class="export-message">{exportMessage}</span>
      {/if}
    </section>

    <!-- Diff rows table -->
    <section class="diff-rows">
      <h3>
        {#if rowFilter === "all"}
          All matched rows
        {:else if rowFilter === "minor"}
          {#if selectedColumn}
            Minor diffs in <code>{selectedColumn}</code>
          {:else}
            Rows with minor differences (tolerance-suppressed)
          {/if}
        {:else if rowFilter === "same"}
          Identical rows
        {:else if selectedColumn}
          Rows where <code>{selectedColumn}</code> differs
        {:else}
          All rows with differences
        {/if}
      </h3>
      <DataTable
        {data}
        {loading}
        onPageChange={(page) => fetchData(page)}
        highlightDiffs={true}
        {charDiffs}
        {precision}
      />
    </section>
  </div>
{/if}

<style>
  .empty {
    color: #888;
    text-align: center;
    padding: 40px;
  }

  .values-tab {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .stats-bar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 2px;
    font-size: 0.82em;
    color: #666;
    padding: 8px 12px;
    background: #f8f8f8;
    border-radius: 6px;
    border: 1px solid #eee;
  }

  .stat strong {
    color: #333;
  }

  .stat.matched strong {
    color: #27ae60;
  }

  .stat.warn strong {
    color: #e67e22;
  }

  .stat.muted {
    color: #999;
  }

  .stat-sep {
    color: #ccc;
    margin: 0 2px;
  }

  .column-bars {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
  }

  .filter-btn {
    padding: 2px 8px;
    border: 1px solid #b8cce8;
    border-radius: 4px;
    background: #dce8f8;
    cursor: pointer;
    font-size: 0.75em;
    font-weight: 700;
    color: #396cd8;
    white-space: nowrap;
  }

  .filter-btn:hover {
    background: #cddcf4;
  }

  .filter-btn.active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .column-chip {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 2px 8px;
    border: 1px solid #b8cce8;
    border-radius: 4px;
    background: #dce8f8;
    cursor: pointer;
    font-size: 0.75em;
    font-weight: 600;
    color: #396cd8;
    white-space: nowrap;
  }

  .column-chip:hover {
    background: #cddcf4;
  }

  .column-chip.active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .column-chip.active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .chip-name {
    letter-spacing: 0.2px;
  }

  .chip-count-diffs {
    font-size: 0.85em;
    padding: 0 4px;
    border-radius: 3px;
    background: rgba(231, 76, 60, 0.15);
    color: #c0392b;
    font-weight: 700;
  }

  .column-chip.active .chip-count-diffs {
    background: rgba(255,255,255,0.2);
    color: white;
  }

  .row-filters {
    display: flex;
    gap: 6px;
  }

  .row-filter-btn {
    padding: 5px 12px;
    border: 1px solid #ddd;
    border-radius: 16px;
    background: transparent;
    cursor: pointer;
    font-size: 0.82em;
    font-weight: 500;
    color: inherit;
  }

  .row-filter-btn:hover:not(:disabled) {
    background: #f0f0f0;
  }

  .row-filter-btn.active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .row-filter-btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .pk-filter {
    border-color: #e67e22;
    color: #e67e22;
  }

  .pk-filter.active {
    background: #e67e22;
    color: white;
    border-color: #e67e22;
  }

  .char-diff-toggle {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 0.82em;
    color: #888;
    cursor: pointer;
    margin-left: auto;
    user-select: none;
  }

  .char-diff-toggle input {
    cursor: pointer;
  }

  .export-btn {
    padding: 5px 12px;
    border: 1px solid #ddd;
    border-radius: 16px;
    background: transparent;
    cursor: pointer;
    font-size: 0.82em;
    font-weight: 500;
    color: inherit;
    margin-left: 4px;
  }

  .export-btn:hover:not(:disabled) {
    background: #f0f0f0;
  }

  .export-btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .export-message {
    font-size: 0.8em;
    color: #27ae60;
    margin-left: 8px;
    white-space: nowrap;
  }

  .diff-rows h3 {
    margin: 0 0 8px 0;
    font-size: 0.95em;
  }

  code {
    background: #f0f0f0;
    padding: 1px 4px;
    border-radius: 3px;
    font-size: 0.9em;
  }

  @media (prefers-color-scheme: dark) {
    .filter-btn {
      border-color: #555;
    }

    .filter-btn.active {
      background: #24c8db;
      color: #1a1a1a;
      border-color: #24c8db;
    }

    .row-filter-btn {
      border-color: #555;
    }

    .row-filter-btn:hover:not(:disabled) {
      background: #383838;
    }

    .row-filter-btn.active {
      background: #24c8db;
      color: #1a1a1a;
      border-color: #24c8db;
    }

    .export-btn {
      border-color: #555;
    }

    .export-btn:hover:not(:disabled) {
      background: #383838;
    }

    .column-chip {
      background: #1a2a4a;
      border-color: #3a5a8a;
      color: #8ab4f8;
    }

    .column-chip:hover {
      background: #253a5a;
    }

    .column-chip.active {
      background: #396cd8;
      color: white;
    }

    .chip-count-diffs {
      background: rgba(231, 76, 60, 0.2);
      color: #f08080;
    }

    code {
      background: #3a3a3a;
    }
  }
</style>
