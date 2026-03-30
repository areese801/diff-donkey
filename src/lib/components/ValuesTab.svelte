<script lang="ts">
  import ProgressBar from "./ProgressBar.svelte";
  import DataTable from "./DataTable.svelte";
  import type { ColumnDiffStats, ValuesSummary, PagedRows } from "$lib/types/diff";
  import { getDiffRows } from "$lib/tauri";

  interface Props {
    columnStats: ColumnDiffStats[];
    valuesSummary?: ValuesSummary;
    precision?: number | null;
  }

  let { columnStats, valuesSummary, precision = null }: Props = $props();

  let selectedColumn: string | null = $state(null);
  let rowFilter: string = $state("all");
  let charDiffs = $state(true);
  let data: PagedRows | null = $state(null);
  let loading = $state(false);
  const PAGE_SIZE = 50;

  /** Fetch diff rows when column selection, row filter, or diff results change */
  $effect(() => {
    // Read columnStats to establish dependency — re-fetch when diff is re-run
    void columnStats;
    // Trigger on selectedColumn and rowFilter changes
    void rowFilter;
    fetchDiffRows(0);
  });

  async function fetchDiffRows(page: number) {
    loading = true;
    try {
      data = await getDiffRows(page, PAGE_SIZE, selectedColumn ?? undefined, rowFilter);
    } catch (e) {
      console.error("Values tab fetch error:", e);
      data = null;
    } finally {
      loading = false;
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
    <!-- Per-column progress bars -->
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
          class="column-row"
          class:active={selectedColumn === col.name}
          onclick={() => selectedColumn = col.name}
        >
          <ProgressBar matchPct={col.match_pct} label={col.name} />
          <span class="diff-badge" class:has-diffs={col.diff_count > 0}>
            {col.diff_count}
          </span>
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

      <label class="char-diff-toggle">
        <input type="checkbox" bind:checked={charDiffs} />
        Char diffs
      </label>
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
        onPageChange={(page) => fetchDiffRows(page)}
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
    gap: 20px;
  }

  .column-bars {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .filter-btn {
    padding: 6px 12px;
    border: 1px solid #ddd;
    border-radius: 6px;
    background: transparent;
    cursor: pointer;
    font-size: 0.85em;
    text-align: left;
    color: inherit;
  }

  .filter-btn.active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .column-row {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 12px;
    border: 1px solid transparent;
    border-radius: 6px;
    background: transparent;
    cursor: pointer;
    width: 100%;
    text-align: left;
    color: inherit;
  }

  .column-row:hover {
    background: #f0f0f0;
  }

  .column-row.active {
    border-color: #396cd8;
    background: #f0f5ff;
  }

  .diff-badge {
    font-size: 0.75em;
    padding: 2px 8px;
    border-radius: 10px;
    background: #f0f0f0;
    font-weight: 600;
    min-width: 30px;
    text-align: center;
  }

  .diff-badge.has-diffs {
    background: #ffeaea;
    color: #e74c3c;
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

    .column-row:hover {
      background: #383838;
    }

    .column-row.active {
      border-color: #24c8db;
      background: #1a2a30;
    }

    .diff-badge {
      background: #3a3a3a;
    }

    .diff-badge.has-diffs {
      background: #4a2020;
    }

    code {
      background: #3a3a3a;
    }
  }
</style>
