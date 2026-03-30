<script lang="ts">
  import { loadDatabaseSource, loadFromSavedConnection } from "$lib/tauri";
  import { savedConnections, loadConnections } from "$lib/stores/connections";
  import ConnectionForm from "$lib/components/ConnectionForm.svelte";
  import type { TableMeta, DatabaseType } from "$lib/types/diff";
  import type { SavedConnection } from "$lib/types/connections";

  interface Props {
    label: "a" | "b";
    onLoaded: (meta: TableMeta) => void;
  }

  let { label, onLoaded }: Props = $props();

  // Connection selection
  let selectedConnectionId = $state("");
  let showNewForm = $state(false);

  // Manual mode (fallback for users who don't want to save)
  let manualMode = $state(false);
  let dbType: DatabaseType = $state("postgres");
  let connString = $state("");

  // Shared
  let query = $state("");
  let loading = $state(false);
  let error: string | null = $state(null);
  let meta: TableMeta | null = $state(null);

  // Load connections on first render
  $effect(() => {
    loadConnections();
  });

  async function handleLoad() {
    if (!query.trim()) {
      error = "SQL query is required.";
      return;
    }

    loading = true;
    error = null;

    try {
      if (selectedConnectionId) {
        // Load from saved connection
        meta = await loadFromSavedConnection(selectedConnectionId, query, label);
      } else if (manualMode && connString.trim()) {
        // Load from manual connection string
        meta = await loadDatabaseSource(connString, query, label, dbType);
      } else {
        error = "Select a saved connection or enter a connection string.";
        loading = false;
        return;
      }
      onLoaded(meta);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  function handleNewFormClose() {
    showNewForm = false;
    // Refresh connections in case a new one was saved
    loadConnections();
  }

  function selectedConnection(): SavedConnection | undefined {
    return $savedConnections.find((c) => c.id === selectedConnectionId);
  }
</script>

<div class="db-source">
  {#if showNewForm}
    <ConnectionForm connection={null} onClose={handleNewFormClose} />
  {:else}
    <div class="field">
      <label for="saved-conn-{label}">Saved Connection</label>
      <div class="conn-select-row">
        <select
          id="saved-conn-{label}"
          bind:value={selectedConnectionId}
          onchange={() => { manualMode = false; }}
        >
          <option value="">-- Select a connection --</option>
          {#each $savedConnections as conn (conn.id)}
            <option value={conn.id}>{conn.name} ({conn.db_type})</option>
          {/each}
        </select>
        <button class="btn-small" onclick={() => (showNewForm = true)} title="New Connection">+</button>
      </div>
    </div>

    {#if selectedConnectionId}
      {@const conn = selectedConnection()}
      {#if conn}
        <div class="conn-summary">
          {#if conn.color}
            <span class="color-dot" style="background: {conn.color}"></span>
          {/if}
          <span class="conn-summary-text">
            {conn.host ?? ""}
            {#if conn.database} / {conn.database}{/if}
          </span>
        </div>
      {/if}
    {/if}

    <div class="divider-row">
      <span class="divider-line"></span>
      <button
        class="divider-toggle"
        onclick={() => { manualMode = !manualMode; if (manualMode) selectedConnectionId = ""; }}
      >
        {manualMode ? "Use saved connection" : "Use connection string"}
      </button>
      <span class="divider-line"></span>
    </div>

    {#if manualMode}
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
    {/if}

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

  .conn-select-row {
    display: flex;
    gap: 6px;
  }

  .conn-select-row select {
    flex: 1;
  }

  .btn-small {
    padding: 6px 12px;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 1em;
    font-weight: 600;
    color: inherit;
  }

  .btn-small:hover {
    background: rgba(57, 108, 216, 0.1);
  }

  .conn-summary {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
    background: rgba(57, 108, 216, 0.05);
    border-radius: 4px;
    font-size: 0.85em;
    color: #666;
  }

  .color-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }

  .divider-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .divider-line {
    flex: 1;
    height: 1px;
    background: #e0e0e0;
  }

  .divider-toggle {
    background: none;
    border: none;
    font-size: 0.8em;
    color: #888;
    cursor: pointer;
    white-space: nowrap;
  }

  .divider-toggle:hover {
    color: #396cd8;
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

    .btn-small {
      border-color: #555;
    }

    .divider-line {
      background: #444;
    }

    .conn-summary {
      background: rgba(107, 154, 255, 0.1);
    }
  }
</style>
