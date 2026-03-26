<script lang="ts">
  import type { PagedRows } from "$lib/types/diff";

  interface Props {
    data: PagedRows | null;
    loading: boolean;
    onPageChange: (page: number) => void;
    highlightDiffs?: boolean;
  }

  let { data, loading, onPageChange, highlightDiffs = false }: Props = $props();

  let totalPages = $derived(data ? Math.ceil(data.total / data.page_size) : 0);
  /** Columns to display (filter out is_diff_* columns) */
  let displayColumns = $derived(
    data?.columns.filter(c => !c.startsWith("is_diff_")) ?? []
  );
</script>

{#if loading}
  <div class="loading">Loading...</div>
{:else if !data || data.rows.length === 0}
  <div class="empty">No data to display.</div>
{:else}
  <div class="data-table-wrapper">
    <table>
      <thead>
        <tr>
          {#each displayColumns as col}
            <th>{col}</th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each data.rows as row}
          <tr>
            {#each displayColumns as col}
              {@const isDiffCol = `is_diff_${col.replace(/_[ab]$/, '')}`}
              {@const hasDiff = highlightDiffs && row[isDiffCol] === 1}
              <td class:diff-cell={hasDiff}>
                {row[col] ?? "NULL"}
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

    .pagination button {
      border-color: #555;
    }
  }
</style>
