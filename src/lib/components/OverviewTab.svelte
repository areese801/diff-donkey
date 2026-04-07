<script lang="ts">
  import type { OverviewResult } from "$lib/types/diff";
  import { exportDiffRows } from "$lib/tauri";
  import { save } from "@tauri-apps/plugin-dialog";

  interface Props {
    result: OverviewResult | null;
    ignoredColumns?: string[];
  }

  let { result, ignoredColumns = [] }: Props = $props();

  let exportMessage: string | null = $state(null);
  let exporting = $state(false);

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
      const count = await exportDiffRows(filepath, format, undefined, "all");
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

  /** Overall match percentage across all columns */
  let overallMatchPct = $derived(() => {
    if (!result) return 0;
    const cols = result.diff_stats.columns;
    if (cols.length === 0) return 100;
    const totalMatch = cols.reduce((sum, c) => sum + c.match_count, 0);
    const totalAll = cols.reduce((sum, c) => sum + c.total, 0);
    return totalAll > 0 ? (totalMatch / totalAll) * 100 : 100;
  });
</script>

{#if !result}
  <p class="empty">Run a diff to see the overview.</p>
{:else}
  <div class="overview">
    <!-- Summary cards row -->
    <div class="summary-cards">
      <div class="card">
        <div class="card-value match">{overallMatchPct().toFixed(1)}%</div>
        <div class="card-label">Overall Match</div>
      </div>
      <div class="card">
        <div class="card-value">{result.total_rows_a.toLocaleString()}</div>
        <div class="card-label">Rows in A</div>
      </div>
      <div class="card">
        <div class="card-value">{result.total_rows_b.toLocaleString()}</div>
        <div class="card-label">Rows in B</div>
      </div>
      <div class="card">
        <div class="card-value">{result.values_summary.total_compared.toLocaleString()}</div>
        <div class="card-label">Matched Rows</div>
      </div>
    </div>

    <!-- PK Summary -->
    <section class="section">
      <h3>Primary Key Summary</h3>
      <div class="pk-grid">
        <div class="pk-stat">
          <span class="pk-value" class:warn={result.pk_summary.exclusive_a > 0}>
            {result.pk_summary.exclusive_a.toLocaleString()}
          </span>
          <span class="pk-label">Only in A</span>
        </div>
        <div class="pk-stat">
          <span class="pk-value" class:warn={result.pk_summary.exclusive_b > 0}>
            {result.pk_summary.exclusive_b.toLocaleString()}
          </span>
          <span class="pk-label">Only in B</span>
        </div>
        <div class="pk-stat">
          <span class="pk-value" class:warn={result.pk_summary.duplicate_pks_a > 0}>
            {result.pk_summary.duplicate_pks_a.toLocaleString()}
          </span>
          <span class="pk-label">Duplicate PKs (A)</span>
        </div>
        <div class="pk-stat">
          <span class="pk-value" class:warn={result.pk_summary.duplicate_pks_b > 0}>
            {result.pk_summary.duplicate_pks_b.toLocaleString()}
          </span>
          <span class="pk-label">Duplicate PKs (B)</span>
        </div>
      </div>
    </section>

    <!-- Values Summary -->
    <section class="section">
      <h3>Values Summary</h3>
      <div class="values-bar">
        {#if result.values_summary.total_compared > 0}
          {@const total = result.values_summary.total_compared}
          {@const identicalPct = (result.values_summary.rows_identical / total) * 100}
          {@const minorPct = (result.values_summary.rows_minor / total) * 100}
          {@const diffPct = (result.values_summary.rows_with_diffs / total) * 100}
          <div class="bar-identical" style="width: {identicalPct}%"></div>
          {#if minorPct > 0}
            <div class="bar-minor" style="width: {minorPct}%"></div>
          {/if}
          <div class="bar-diff" style="width: {diffPct}%"></div>
        {/if}
      </div>
      <div class="values-legend">
        <span class="legend-item identical">
          {result.values_summary.rows_identical.toLocaleString()} identical
        </span>
        {#if result.values_summary.rows_minor > 0}
          <span class="legend-item minor">
            {result.values_summary.rows_minor.toLocaleString()} minor
          </span>
        {/if}
        <span class="legend-item diff">
          {result.values_summary.rows_with_diffs.toLocaleString()} with differences
        </span>
      </div>
    </section>

    <!-- Per-column stats table -->
    <section class="section">
      <h3>Per-Column Comparison</h3>
      <table>
        <thead>
          <tr>
            <th>Column</th>
            <th>Match %</th>
            <th>Diffs</th>
            <th>Minor</th>
            <th>Matches</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each result.diff_stats.columns as col}
            <tr>
              <td><code>{col.name}</code></td>
              <td class:perfect={col.match_pct === 100} class:has-diffs={col.match_pct < 100}>
                {col.match_pct.toFixed(1)}%
              </td>
              <td class:has-diffs={col.diff_count > 0}>{col.diff_count.toLocaleString()}</td>
              <td class:has-minor={col.minor_count > 0}>{col.minor_count.toLocaleString()}</td>
              <td>{col.match_count.toLocaleString()}</td>
              <td>
                <div class="mini-bar">
                  <div class="mini-bar-fill" style="width: {col.match_pct}%"></div>
                </div>
              </td>
            </tr>
          {/each}
          {#each ignoredColumns as col}
            <tr class="ignored-row">
              <td><code>{col}</code></td>
              <td colspan="4" class="ignored-label">Ignored</td>
              <td></td>
            </tr>
          {/each}
        </tbody>
      </table>
      <div class="export-row">
        <button
          class="export-btn"
          onclick={handleExport}
          disabled={exporting}
        >
          {exporting ? "Exporting..." : "Export Results"}
        </button>
        {#if exportMessage}
          <span class="export-message">{exportMessage}</span>
        {/if}
      </div>
    </section>
  </div>
{/if}

<style>
  .empty {
    color: #888;
    text-align: center;
    padding: 40px;
  }

  .overview {
    display: flex;
    flex-direction: column;
    gap: 24px;
  }

  /* Summary cards */
  .summary-cards {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
  }

  .card {
    text-align: center;
    padding: 16px;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
  }

  .card-value {
    font-size: 1.8em;
    font-weight: 700;
  }

  .card-value.match {
    color: #27ae60;
  }

  .card-label {
    color: #888;
    font-size: 0.85em;
    margin-top: 4px;
  }

  /* Sections */
  .section h3 {
    margin: 0 0 12px 0;
    font-size: 1em;
    font-weight: 600;
  }

  /* PK grid */
  .pk-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
  }

  .pk-stat {
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 12px;
    border-radius: 6px;
    background: #f8f8f8;
  }

  .pk-value {
    font-size: 1.3em;
    font-weight: 600;
  }

  .pk-value.warn {
    color: #e67e22;
  }

  .pk-label {
    color: #888;
    font-size: 0.8em;
    margin-top: 4px;
  }

  /* Values bar */
  .values-bar {
    display: flex;
    height: 24px;
    border-radius: 4px;
    overflow: hidden;
  }

  .bar-identical {
    background: #27ae60;
  }

  .bar-minor {
    background: #f39c12;
  }

  .bar-diff {
    background: #e74c3c;
  }

  .values-legend {
    display: flex;
    gap: 16px;
    margin-top: 6px;
    font-size: 0.85em;
  }

  .legend-item::before {
    content: "";
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 2px;
    margin-right: 4px;
  }

  .legend-item.identical::before {
    background: #27ae60;
  }

  .legend-item.minor::before {
    background: #f39c12;
  }

  .legend-item.diff::before {
    background: #e74c3c;
  }

  /* Per-column table */
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9em;
  }

  th {
    text-align: left;
    padding: 8px 12px;
    border-bottom: 2px solid #e0e0e0;
    font-weight: 600;
    font-size: 0.85em;
    text-transform: uppercase;
    color: #888;
  }

  td {
    padding: 8px 12px;
    border-bottom: 1px solid #f0f0f0;
  }

  .perfect {
    color: #27ae60;
    font-weight: 500;
  }

  .has-diffs {
    color: #e74c3c;
    font-weight: 500;
  }

  .has-minor {
    color: #e67e22;
    font-weight: 500;
  }

  .ignored-row {
    opacity: 0.45;
  }

  .ignored-row code {
    text-decoration: line-through;
  }

  .ignored-label {
    font-style: italic;
    color: #999;
  }

  .mini-bar {
    width: 80px;
    height: 6px;
    background: #f0f0f0;
    border-radius: 3px;
    overflow: hidden;
  }

  .mini-bar-fill {
    height: 100%;
    background: #27ae60;
    border-radius: 3px;
  }

  .export-row {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-top: 12px;
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
    white-space: nowrap;
  }

  @media (prefers-color-scheme: dark) {
    .card {
      border-color: #444;
    }

    .pk-stat {
      background: #383838;
    }

    th {
      border-bottom-color: #444;
    }

    td {
      border-bottom-color: #3a3a3a;
    }

    .mini-bar {
      background: #3a3a3a;
    }

    .export-btn {
      border-color: #555;
    }

    .export-btn:hover:not(:disabled) {
      background: #383838;
    }
  }
</style>
