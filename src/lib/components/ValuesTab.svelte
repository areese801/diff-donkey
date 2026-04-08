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
    charDiffColumns?: Record<string, boolean>;
  }

  let { columnStats, valuesSummary, precision = null, result = null, schemaComparison = null, charDiffColumns = {} }: Props = $props();

  let selectedColumn: string | null = $state(null);
  let rowFilter: string = $state("all");
  let data: PagedRows | null = $state(null);
  let allRows: Record<string, unknown>[] = $state([]);
  let loading = $state(false);
  let loadingMore = $state(false);
  let currentPage = $state(0);
  let hasMore = $state(false);
  let exportMessage: string | null = $state(null);
  let exporting = $state(false);
  const PAGE_SIZE = 50;

  /** Combined data object with accumulated rows */
  let combinedData = $derived.by(() => {
    if (!data) return null;
    return { ...data, rows: allRows };
  });

  /** Reset and fetch first page when filters change */
  $effect(() => {
    void columnStats;
    void rowFilter;
    void selectedColumn;
    resetAndFetch();
  });

  async function resetAndFetch() {
    allRows = [];
    currentPage = 0;
    hasMore = false;
    loading = true;
    try {
      const result = await fetchPage(0);
      if (result) {
        data = result;
        allRows = [...result.rows];
        currentPage = 0;
        hasMore = result.rows.length >= PAGE_SIZE && allRows.length < result.total;
      }
    } catch (e) {
      console.error("Values tab fetch error:", e);
      data = null;
      allRows = [];
    } finally {
      loading = false;
    }
  }

  async function loadMore() {
    if (loadingMore || !hasMore) return;
    loadingMore = true;
    try {
      const nextPage = currentPage + 1;
      const result = await fetchPage(nextPage);
      if (result && result.rows.length > 0) {
        allRows = [...allRows, ...result.rows];
        currentPage = nextPage;
        hasMore = allRows.length < result.total;
      } else {
        hasMore = false;
      }
    } catch (e) {
      console.error("Values tab load more error:", e);
    } finally {
      loadingMore = false;
    }
  }

  async function fetchPage(page: number): Promise<PagedRows | null> {
    if (rowFilter === "exclusive_a") {
      return await getExclusiveRows("a", page, PAGE_SIZE);
    } else if (rowFilter === "exclusive_b") {
      return await getExclusiveRows("b", page, PAGE_SIZE);
    } else if (rowFilter === "duplicates_a") {
      return await getDuplicatePks("a", page, PAGE_SIZE);
    } else if (rowFilter === "duplicates_b") {
      return await getDuplicatePks("b", page, PAGE_SIZE);
    } else {
      return await getDiffRows(page, PAGE_SIZE, selectedColumn ?? undefined, rowFilter);
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
        data={combinedData}
        {loading}
        onLoadMore={loadMore}
        {loadingMore}
        {hasMore}
        highlightDiffs={true}
        {charDiffColumns}
        {precision}
        {columnStats}
        {selectedColumn}
        onColumnSelect={(col) => selectedColumn = col}
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

    code {
      background: #3a3a3a;
    }
  }
</style>
