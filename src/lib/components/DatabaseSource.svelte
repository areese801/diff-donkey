<script lang="ts">
  import { loadDatabaseSource, loadFromSavedConnection, loadSnowflakeSource, getQueryHistory, deleteQueryHistoryEntry, clearQueryHistory } from "$lib/tauri";
  import { savedConnections, loadConnections } from "$lib/stores/connections";
  import { open } from "@tauri-apps/plugin-dialog";
  import ConnectionForm from "$lib/components/ConnectionForm.svelte";
  import type { TableMeta, DatabaseType, QueryHistoryEntry } from "$lib/types/diff";
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

  // Snowflake manual mode fields
  let sfAccountUrl = $state("");
  let sfUsername = $state("");
  let sfAuthMethod = $state<"password" | "keypair">("password");
  let sfPassword = $state("");
  let sfPrivateKeyPath = $state("");
  let sfPrivateKeyFilename = $state("");
  let sfWarehouse = $state("");
  let sfRole = $state("");
  let sfDatabase = $state("");
  let sfSchema = $state("");

  // Shared
  let query = $state("");
  let loading = $state(false);
  let error: string | null = $state(null);
  let meta: TableMeta | null = $state(null);

  // Query history
  let history: QueryHistoryEntry[] = $state([]);
  let showHistory = $state(false);

  // Load connections on first render
  $effect(() => {
    loadConnections();
  });

  async function refreshHistory() {
    try {
      const cid = selectedConnectionId || undefined;
      history = await getQueryHistory(cid);
    } catch {
      history = [];
    }
  }

  function toggleHistory() {
    if (!showHistory) {
      refreshHistory();
    }
    showHistory = !showHistory;
  }

  function selectQuery(q: string) {
    query = q;
    showHistory = false;
  }

  async function deleteEntry(id: string) {
    try {
      await deleteQueryHistoryEntry(id);
      history = history.filter((e) => e.id !== id);
    } catch {
      // ignore
    }
  }

  async function handleClearHistory() {
    try {
      const cid = selectedConnectionId || undefined;
      await clearQueryHistory(cid);
      history = [];
      showHistory = false;
    } catch {
      // ignore
    }
  }

  function truncate(s: string, max: number): string {
    const oneLine = s.replace(/\s+/g, " ").trim();
    return oneLine.length <= max ? oneLine : oneLine.slice(0, max) + "...";
  }

  function formatRelativeTime(iso: string): string {
    const now = Date.now();
    const then = new Date(iso).getTime();
    const diffSec = Math.floor((now - then) / 1000);
    if (diffSec < 60) return "just now";
    if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
    if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
    if (diffSec < 604800) return `${Math.floor(diffSec / 86400)}d ago`;
    return new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric" });
  }

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
      } else if (manualMode && dbType === "snowflake") {
        // Load from manual Snowflake fields
        if (!sfAccountUrl.trim() || !sfUsername.trim()) {
          error = "Account URL and username are required for Snowflake.";
          loading = false;
          return;
        }
        meta = await loadSnowflakeSource(
          sfAccountUrl.trim(),
          sfUsername.trim(),
          sfAuthMethod,
          sfAuthMethod === "password" ? sfPassword : null,
          sfAuthMethod === "keypair" ? sfPrivateKeyPath : null,
          sfWarehouse.trim() || null,
          sfRole.trim() || null,
          sfDatabase.trim() || null,
          sfSchema.trim() || null,
          query,
          label,
        );
      } else if (manualMode && connString.trim()) {
        // Load from manual connection string
        meta = await loadDatabaseSource(connString, query, label, dbType);
      } else {
        error = "Select a saved connection or enter a connection string.";
        loading = false;
        return;
      }
      onLoaded(meta);
      // Refresh history count after successful load (backend auto-saved the query)
      refreshHistory();
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

  async function handleSelectKeyFile() {
    const selected = await open({
      multiple: false,
      filters: [{ name: "Private Key", extensions: ["p8", "pem"] }],
    });
    if (selected) {
      sfPrivateKeyPath = selected as string;
      sfPrivateKeyFilename = sfPrivateKeyPath.split(/[/\\]/).pop() ?? "";
    }
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
            {#if conn.db_type === "snowflake"}
              {conn.account_url ?? ""}
              {#if conn.warehouse} / {conn.warehouse}{/if}
            {:else}
              {conn.host ?? ""}
              {#if conn.database} / {conn.database}{/if}
            {/if}
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
          <option value="snowflake">Snowflake</option>
        </select>
      </div>

      {#if dbType === "snowflake"}
        <div class="field">
          <label for="sf-account-{label}">Account URL</label>
          <input id="sf-account-{label}" type="text" bind:value={sfAccountUrl} placeholder="https://myorg-myaccount.snowflakecomputing.com" />
        </div>
        <div class="field-row">
          <div class="field">
            <label for="sf-auth-{label}">Auth Method</label>
            <select id="sf-auth-{label}" bind:value={sfAuthMethod}>
              <option value="password">Password</option>
              <option value="keypair">Key Pair</option>
            </select>
          </div>
          <div class="field">
            <label for="sf-user-{label}">Username</label>
            <input id="sf-user-{label}" type="text" bind:value={sfUsername} placeholder="MYUSER" />
          </div>
        </div>
        {#if sfAuthMethod === "password"}
          <div class="field">
            <label for="sf-pass-{label}">Password</label>
            <input id="sf-pass-{label}" type="password" bind:value={sfPassword} placeholder="Enter password" />
          </div>
        {:else}
          <div class="field">
            <label for="sf-keyfile-{label}">Private Key (.p8 / .pem)</label>
            <div class="key-file-row">
              <button id="sf-keyfile-{label}" type="button" class="btn-small" onclick={handleSelectKeyFile}>Select Key File</button>
              {#if sfPrivateKeyFilename}
                <span class="key-filename">{sfPrivateKeyFilename}</span>
              {/if}
            </div>
          </div>
        {/if}
        <div class="field-row">
          <div class="field">
            <label for="sf-wh-{label}">Warehouse</label>
            <input id="sf-wh-{label}" type="text" bind:value={sfWarehouse} placeholder="COMPUTE_WH" />
          </div>
          <div class="field">
            <label for="sf-role-{label}">Role</label>
            <input id="sf-role-{label}" type="text" bind:value={sfRole} placeholder="SYSADMIN" />
          </div>
        </div>
        <div class="field-row">
          <div class="field">
            <label for="sf-db-{label}">Database</label>
            <input id="sf-db-{label}" type="text" bind:value={sfDatabase} placeholder="MY_DB" />
          </div>
          <div class="field">
            <label for="sf-schema-{label}">Schema</label>
            <input id="sf-schema-{label}" type="text" bind:value={sfSchema} placeholder="PUBLIC" />
          </div>
        </div>
      {:else}
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
    {/if}

    <div class="field">
      <div class="query-label-row">
        <label for="query-{label}">SQL Query</label>
        {#if history.length > 0 || selectedConnectionId}
          <button class="history-btn" onclick={toggleHistory}>
            Recent{history.length > 0 ? ` (${history.length})` : ""}
          </button>
        {/if}
      </div>
      {#if showHistory && history.length > 0}
        <div class="history-dropdown">
          {#each history as entry (entry.id)}
            <div class="history-entry">
              <button class="history-entry-btn" onclick={() => selectQuery(entry.query)} title={entry.query}>
                <span class="history-query">{truncate(entry.query, 60)}</span>
                <span class="history-time">{formatRelativeTime(entry.last_used_at)}</span>
              </button>
              <button class="history-delete" onclick={() => deleteEntry(entry.id)} title="Remove from history">&times;</button>
            </div>
          {/each}
          <button class="history-clear" onclick={handleClearHistory}>Clear History</button>
        </div>
      {/if}
      {#if showHistory && history.length === 0}
        <div class="history-dropdown history-empty">No query history yet.</div>
      {/if}
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

  .field-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
  }

  .key-file-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .key-filename {
    font-size: 0.85em;
    color: #888;
    font-family: monospace;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .query-label-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
  }

  .history-btn {
    all: unset;
    font-size: 0.78em;
    color: #888;
    cursor: pointer;
    padding: 2px 6px;
    border-radius: 3px;
  }

  .history-btn:hover {
    color: #396cd8;
    background: rgba(57, 108, 216, 0.08);
  }

  .history-dropdown {
    border: 1px solid #ddd;
    border-radius: 4px;
    max-height: 200px;
    overflow-y: auto;
    background: inherit;
    font-size: 0.85em;
  }

  .history-empty {
    padding: 10px;
    color: #999;
    text-align: center;
  }

  .history-entry {
    display: flex;
    align-items: center;
    border-bottom: 1px solid #eee;
  }

  .history-entry:last-of-type {
    border-bottom: none;
  }

  .history-entry-btn {
    all: unset;
    flex: 1;
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 8px;
    cursor: pointer;
    min-width: 0;
    gap: 8px;
  }

  .history-entry-btn:hover {
    background: rgba(57, 108, 216, 0.06);
  }

  .history-query {
    font-family: monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    min-width: 0;
  }

  .history-time {
    color: #999;
    font-size: 0.85em;
    white-space: nowrap;
    flex-shrink: 0;
  }

  .history-delete {
    all: unset;
    padding: 4px 8px;
    cursor: pointer;
    color: #ccc;
    font-size: 1.1em;
    line-height: 1;
    flex-shrink: 0;
  }

  .history-delete:hover {
    color: #e74c3c;
  }

  .history-clear {
    all: unset;
    display: block;
    width: 100%;
    text-align: center;
    padding: 6px;
    font-size: 0.85em;
    color: #999;
    cursor: pointer;
    border-top: 1px solid #eee;
  }

  .history-clear:hover {
    color: #e74c3c;
    background: rgba(231, 76, 60, 0.05);
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

    .history-dropdown {
      border-color: #444;
    }

    .history-entry {
      border-bottom-color: #333;
    }

    .history-delete {
      color: #666;
    }

    .history-clear {
      border-top-color: #333;
    }
  }
</style>
