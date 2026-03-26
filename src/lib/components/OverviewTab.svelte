<script lang="ts">
  import type { OverviewResult } from "$lib/types/diff";

  interface Props {
    result: OverviewResult | null;
  }

  let { result }: Props = $props();

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
          {@const identicalPct = (result.values_summary.rows_identical / result.values_summary.total_compared) * 100}
          <div class="bar-identical" style="width: {identicalPct}%"></div>
          <div class="bar-diff" style="width: {100 - identicalPct}%"></div>
        {/if}
      </div>
      <div class="values-legend">
        <span class="legend-item identical">
          {result.values_summary.rows_identical.toLocaleString()} identical
        </span>
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
              <td>{col.match_count.toLocaleString()}</td>
              <td>
                <div class="mini-bar">
                  <div class="mini-bar-fill" style="width: {col.match_pct}%"></div>
                </div>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
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
  }
</style>
