<script lang="ts">
  import { diffChars } from "diff";
  import type { PagedRows, ColumnDiffStats } from "$lib/types/diff";

  interface Props {
    data: PagedRows | null;
    loading: boolean;
    onPageChange: (page: number) => void;
    highlightDiffs?: boolean;
    charDiffs?: boolean;
    precision?: number | null;
    columnStats?: ColumnDiffStats[];
    selectedColumn?: string | null;
    onColumnSelect?: (col: string | null) => void;
  }

  let { data, loading, onPageChange, highlightDiffs = false, charDiffs = true, precision = null, columnStats = [], selectedColumn = null, onColumnSelect }: Props = $props();

  /** Map column base names to their diff stats */
  let statsMap = $derived.by(() => {
    const map = new Map<string, ColumnDiffStats>();
    for (const s of columnStats) {
      map.set(s.name, s);
    }
    return map;
  });

  /**
   * Format a cell value for display. When precision is set and the value
   * is numeric, truncate to that many decimal places to match the comparison.
   */
  function formatValue(val: unknown): string {
    if (val === null || val === undefined) return "NULL";
    if (precision !== null && precision >= 0 && typeof val === "number" && !Number.isInteger(val)) {
      // Truncate (not round) to match TRUNC behavior
      const factor = Math.pow(10, precision);
      const truncated = Math.trunc(val * factor) / factor;
      return truncated.toFixed(precision);
    }
    return String(val);
  }

  let totalPages = $derived(data ? Math.ceil(data.total / data.page_size) : 0);
  /** Columns to display (filter out is_diff_* and is_raw_diff_* columns) */
  let displayColumns = $derived(
    data?.columns.filter(c => !c.startsWith("is_diff_") && !c.startsWith("is_raw_diff_")) ?? []
  );

  /** Set of base column names that are ignored (have values but no is_diff_* flags) */
  let ignoredBaseColumns = $derived(() => {
    if (!data) return new Set<string>();
    const diffCols = new Set(data.columns.filter(c => c.startsWith("is_diff_")).map(c => c.replace("is_diff_", "")));
    const ignored = new Set<string>();
    for (const col of displayColumns) {
      const base = col.replace(/_[ab]$/, '');
      if ((col.endsWith("_a") || col.endsWith("_b")) && !diffCols.has(base)) {
        ignored.add(base);
      }
    }
    return ignored;
  });

  /**
   * Compute character-level diff parts for a cell value.
   * Returns an array of { text, type } where type is "same", "removed", or "added".
   * - For _a columns: shows "same" and "removed" parts
   * - For _b columns: shows "same" and "added" parts
   */
  function charDiffParts(
    row: Record<string, unknown>,
    col: string,
  ): { text: string; highlight: boolean }[] | null {
    // Only compute for paired _a / _b columns with a diff
    const isA = col.endsWith("_a");
    const isB = col.endsWith("_b");
    if (!isA && !isB) return null;

    const baseCol = col.replace(/_[ab]$/, "");
    const isDiffCol = `is_diff_${baseCol}`;
    const isRawDiffCol = `is_raw_diff_${baseCol}`;
    const hasDiff = row[isDiffCol] === 1 || row[isRawDiffCol] === 1;
    if (!hasDiff) return null;

    const valA = formatValue(row[`${baseCol}_a`]);
    const valB = formatValue(row[`${baseCol}_b`]);

    const changes = diffChars(valA, valB);
    const parts: { text: string; highlight: boolean }[] = [];

    for (const change of changes) {
      if (change.added) {
        // This part only exists in B
        if (isB) parts.push({ text: change.value, highlight: true });
      } else if (change.removed) {
        // This part only exists in A
        if (isA) parts.push({ text: change.value, highlight: true });
      } else {
        // Same in both
        parts.push({ text: change.value, highlight: false });
      }
    }

    return parts.length > 0 ? parts : null;
  }
</script>

{#if loading}
  <div class="loading">Loading...</div>
{:else if !data || data.rows.length === 0}
  <div class="empty">No data to display.</div>
{:else}
  <div class="data-table-wrapper">
    <table>
      <thead>
        {#if columnStats.length > 0}
          <tr class="stats-row">
            {#each displayColumns as col}
              {@const baseCol = col.replace(/_[ab]$/, '')}
              {@const stat = statsMap.get(baseCol)}
              {#if stat && col.endsWith("_a")}
                {@const diffPct = stat.total > 0 ? ((stat.diff_count / stat.total) * 100) : 0}
                <td
                  class="stat-cell"
                  class:stat-active={selectedColumn === baseCol}
                  class:stat-has-diffs={stat.diff_count > 0}
                  colspan="2"
                  onclick={() => onColumnSelect?.(selectedColumn === baseCol ? null : baseCol)}
                  title="{baseCol}: {stat.diff_count} diffs out of {stat.total} rows ({diffPct.toFixed(1)}%)"
                >
                  {#if stat.diff_count > 0}
                    {diffPct.toFixed(1)}% diff
                  {:else}
                    <span class="stat-ok">&check;</span> match
                  {/if}
                </td>
              {:else if stat && col.endsWith("_b")}
                <!-- spanned by colspan=2 from _a cell -->
              {:else if col.startsWith("pk_")}
                <td class="stat-cell-spacer"></td>
              {:else}
                <td class="stat-cell-spacer"></td>
              {/if}
            {/each}
          </tr>
        {/if}
        <tr>
          {#each displayColumns as col}
            {@const baseCol = col.replace(/_[ab]$/, '')}
            <th class:ignored-header={highlightDiffs && ignoredBaseColumns().has(baseCol)}>{col}</th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each data.rows as row}
          <tr>
            {#each displayColumns as col}
              {@const baseCol = col.replace(/_[ab]$/, '')}
              {@const isDiffCol = `is_diff_${baseCol}`}
              {@const isRawDiffCol = `is_raw_diff_${baseCol}`}
              {@const isIgnored = highlightDiffs && (col.endsWith("_a") || col.endsWith("_b")) && row[isDiffCol] === undefined && row[isRawDiffCol] === undefined}
              {@const hasDiff = highlightDiffs && row[isDiffCol] === 1}
              {@const hasMinorDiff = highlightDiffs && row[isDiffCol] !== 1 && row[isRawDiffCol] === 1}
              {@const parts = highlightDiffs && charDiffs && !isIgnored ? charDiffParts(row, col) : null}
              <td class:diff-cell={hasDiff} class:minor-diff-cell={hasMinorDiff} class:ignored-cell={isIgnored}>
                {#if parts}
                  {#each parts as part}
                    {#if part.highlight}
                      <span class="char-diff">{part.text}</span>
                    {:else}
                      {part.text}
                    {/if}
                  {/each}
                {:else}
                  {formatValue(row[col])}
                {/if}
              </td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>

    <!-- Pagination -->
    {#if totalPages > 1}
      <div class="pagination">
        <button
          onclick={() => onPageChange(data!.page - 1)}
          disabled={data!.page === 0}
        >
          Prev
        </button>
        <span>Page {data!.page + 1} of {totalPages} ({data!.total.toLocaleString()} rows)</span>
        <button
          onclick={() => onPageChange(data!.page + 1)}
          disabled={data!.page >= totalPages - 1}
        >
          Next
        </button>
      </div>
    {:else}
      <div class="pagination">
        <span>{data.total.toLocaleString()} rows</span>
      </div>
    {/if}
  </div>
{/if}

<style>
  .loading, .empty {
    color: #888;
    text-align: center;
    padding: 40px;
  }

  .data-table-wrapper {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85em;
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
  }

  th {
    text-align: left;
    padding: 6px 10px;
    border-bottom: 2px solid #e0e0e0;
    font-weight: 600;
    font-size: 0.85em;
    color: #888;
    white-space: nowrap;
    position: sticky;
    top: 0;
    background: #f6f6f6;
  }

  .stats-row {
    position: sticky;
    top: 0;
    z-index: 1;
  }

  .stat-cell {
    text-align: center;
    padding: 2px 6px;
    font-size: 0.75em;
    font-weight: 700;
    cursor: pointer;
    white-space: nowrap;
    background: #dce8f8;
    border: 1px solid #b8cce8;
    border-radius: 3px;
    color: #396cd8;
  }

  .stat-cell:hover {
    background: #cddcf4;
  }

  .stat-cell.stat-active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .stat-cell.stat-has-diffs {
    color: #c0392b;
  }

  .stat-cell.stat-has-diffs.stat-active {
    background: #396cd8;
    color: white;
  }

  .stat-ok {
    color: #27ae60;
    font-size: 1.1em;
  }

  .stat-cell-spacer {
    padding: 0;
    border: none;
  }

  td {
    padding: 4px 10px;
    border-bottom: 1px solid #f0f0f0;
    white-space: nowrap;
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .diff-cell {
    background: #ffe0e0;
    color: #c0392b;
    font-weight: 500;
  }

  .minor-diff-cell {
    background: #fff8e1;
    color: #e67e22;
    font-weight: 500;
  }

  .ignored-cell {
    opacity: 0.35;
    text-decoration: line-through;
  }

  .ignored-header {
    opacity: 0.35;
    text-decoration: line-through;
  }

  .char-diff {
    background: rgba(0, 0, 0, 0.15);
    border-radius: 2px;
    padding: 0 1px;
    font-weight: 700;
  }

  .diff-cell .char-diff {
    background: #ffb3b3;
  }

  .minor-diff-cell .char-diff {
    background: #ffe0a0;
  }

  .pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    padding: 12px;
    font-size: 0.85em;
    color: #888;
  }

  .pagination button {
    padding: 4px 12px;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.9em;
    color: inherit;
  }

  .pagination button:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  @media (prefers-color-scheme: dark) {
    th {
      border-bottom-color: #444;
      background: #2f2f2f;
    }

    td {
      border-bottom-color: #3a3a3a;
    }

    .diff-cell {
      background: #4a2020;
      color: #ff8888;
    }

    .minor-diff-cell {
      background: #4a4020;
      color: #ffcc66;
    }

    .char-diff {
      background: rgba(255, 255, 255, 0.15);
    }

    .diff-cell .char-diff {
      background: #7a3030;
    }

    .minor-diff-cell .char-diff {
      background: #7a6030;
    }

    .pagination button {
      border-color: #555;
    }
  }
</style>
