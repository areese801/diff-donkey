<script lang="ts">
  import { loadDatabaseSource } from "$lib/tauri";
  import type { TableMeta, DatabaseType } from "$lib/types/diff";

  interface Props {
    label: "a" | "b";
    onLoaded: (meta: TableMeta) => void;
  }

  let { label, onLoaded }: Props = $props();

  let dbType: DatabaseType = $state("postgres");
  let connString = $state("");
  let query = $state("");
  let loading = $state(false);
  let error: string | null = $state(null);
  let meta: TableMeta | null = $state(null);

  async function handleLoad() {
    if (!connString.trim() || !query.trim()) {
      error = "Connection string and query are required.";
      return;
    }

    loading = true;
    error = null;

    try {
      meta = await loadDatabaseSource(connString, query, label, dbType);
      onLoaded(meta);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }
</script>

<div class="db-source">
  <div class="field">
    <label for="db-type-{label}">Database Type</label>
    <select id="db-type-{label}" bind:value={dbType}>
      <option value="postgres">PostgreSQL</option>
      <option value="mysql">MySQL</option>
    </select>
  </div>

  <div class="field">
    <label for="conn-{label}">Connection String</label>
    <input
      id="conn-{label}"
      type="password"
      bind:value={connString}
      placeholder={dbType === "postgres"
        ? "host=localhost port=5432 dbname=mydb user=me password=secret"
        : "host=localhost port=3306 user=me password=secret database=mydb"}
    />
  </div>

  <div class="field">
    <label for="query-{label}">SQL Query</label>
    <textarea
      id="query-{label}"
      bind:value={query}
      placeholder="SELECT * FROM my_table WHERE ..."
      rows="3"
    ></textarea>
  </div>

  <button onclick={handleLoad} disabled={loading}>
    {loading ? "Loading..." : "Load from Database"}
  </button>

  {#if error}
    <p class="error">{error}</p>
  {/if}

  {#if meta}
    <div class="meta">
      <p class="row-count">{meta.row_count.toLocaleString()} rows</p>
      <ul class="columns">
        {#each meta.columns as col}
          <li><code>{col.name}</code> <span class="type">{col.data_type}</span></li>
        {/each}
      </ul>
    </div>
  {/if}
</div>

<style>
  .db-source {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .field label {
    font-size: 0.85em;
    font-weight: 600;
  }

  select, input, textarea {
    padding: 8px;
    border-radius: 4px;
    border: 1px solid #ccc;
    font-family: inherit;
    font-size: 0.9em;
    background: inherit;
    color: inherit;
  }

  textarea {
    resize: vertical;
    font-family: monospace;
  }

  button {
    width: 100%;
    padding: 10px;
    border-radius: 6px;
    border: 2px solid #396cd8;
    background: #396cd8;
    color: white;
    cursor: pointer;
    font-size: 0.95em;
  }

  button:hover:not(:disabled) {
    background: #2a5ab8;
  }

  button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .error {
    color: #e74c3c;
    font-size: 0.85em;
    margin: 0;
  }

  .meta {
    margin-top: 4px;
  }

  .row-count {
    font-weight: 600;
    margin: 0 0 8px 0;
  }

  .columns {
    list-style: none;
    padding: 0;
    margin: 0;
    font-size: 0.85em;
  }

  .columns li {
    padding: 2px 0;
  }

  .type {
    color: #888;
    font-size: 0.85em;
  }

  @media (prefers-color-scheme: dark) {
    select, input, textarea {
      border-color: #555;
    }

    button {
      background: #24c8db;
      border-color: #24c8db;
      color: #1a1a1a;
    }

    button:hover:not(:disabled) {
      background: #1db0c0;
    }
  }
</style>
