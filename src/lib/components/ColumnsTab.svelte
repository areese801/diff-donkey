<script lang="ts">
  import type { SchemaComparison } from "$lib/types/diff";

  interface Props {
    comparison: SchemaComparison | null;
  }

  let { comparison }: Props = $props();
</script>

{#if !comparison}
  <p class="empty">Load both sources to see column comparison.</p>
{:else}
  <div class="columns-tab">
    {#if comparison.shared.length > 0}
      <section>
        <h3>Shared Columns ({comparison.shared.length})</h3>
        <table>
          <thead>
            <tr>
              <th>Column</th>
              <th>Type (A)</th>
              <th>Type (B)</th>
              <th>Match</th>
            </tr>
          </thead>
          <tbody>
            {#each comparison.shared as col}
              <tr>
                <td><code>{col.name}</code></td>
                <td>{col.type_a}</td>
                <td>{col.type_b}</td>
                <td class:match={col.types_match} class:mismatch={!col.types_match}>
                  {col.types_match ? "Yes" : "No"}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </section>
    {/if}

    {#if comparison.only_in_a.length > 0}
      <section>
        <h3>Only in Source A ({comparison.only_in_a.length})</h3>
        <table>
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {#each comparison.only_in_a as col}
              <tr>
                <td><code>{col.name}</code></td>
                <td>{col.data_type}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </section>
    {/if}

    {#if comparison.only_in_b.length > 0}
      <section>
        <h3>Only in Source B ({comparison.only_in_b.length})</h3>
        <table>
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {#each comparison.only_in_b as col}
              <tr>
                <td><code>{col.name}</code></td>
                <td>{col.data_type}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </section>
    {/if}
  </div>
{/if}

<style>
  .empty {
    color: #888;
    text-align: center;
    padding: 40px;
  }

  .columns-tab {
    display: flex;
    flex-direction: column;
    gap: 20px;
  }

  h3 {
    margin: 0 0 8px 0;
    font-size: 1em;
  }

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
    padding: 6px 12px;
    border-bottom: 1px solid #f0f0f0;
  }

  code {
    font-size: 0.95em;
  }

  .match {
    color: #27ae60;
    font-weight: 500;
  }

  .mismatch {
    color: #e74c3c;
    font-weight: 500;
  }

  @media (prefers-color-scheme: dark) {
    th {
      border-bottom-color: #444;
    }

    td {
      border-bottom-color: #3a3a3a;
    }
  }
</style>
