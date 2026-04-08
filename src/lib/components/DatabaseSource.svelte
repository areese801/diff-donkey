<script lang="ts">
  import { loadDatabaseSource, loadFromSavedConnection, loadSnowflakeSource, getQueryHistory, deleteQueryHistoryEntry, clearQueryHistory, exportConnectionsToFile, importConnectionsFromFile, listCatalog } from "$lib/tauri";
  import { savedConnections, loadConnections } from "$lib/stores/connections";
  import { open, save } from "@tauri-apps/plugin-dialog";
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

  // Catalog browser state
  let showBrowser = $state(false);
  let catalogLoading = $state(false);
  let catalogError: string | null = $state(null);
  let databases: string[] = $state([]);
  let schemas: string[] = $state([]);
  let tables: string[] = $state([]);
  let selectedDatabase = $state("");
  let selectedSchema = $state("");
  let selectedTable = $state("");

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

  function connDbType(): string {
    const conn = selectedConnection();
    return conn?.db_type ?? "postgres";
  }

  function resetBrowser() {
    databases = [];
    schemas = [];
    tables = [];
    selectedDatabase = "";
    selectedSchema = "";
    selectedTable = "";
    catalogError = null;
  }

  async function handleBrowseToggle() {
    showBrowser = !showBrowser;
    if (!showBrowser || !selectedConnectionId) return;

    resetBrowser();
    const dt = connDbType();

    catalogLoading = true;
    catalogError = null;
    try {
      if (dt === "snowflake") {
        // Snowflake: start with databases
        const items = await listCatalog(selectedConnectionId, "databases");
        databases = items.map((i) => i.name);
      } else if (dt === "mysql") {
        // MySQL: start with databases (no schema concept)
        const items = await listCatalog(selectedConnectionId, "databases");
        databases = items.map((i) => i.name);
      } else {
        // Postgres: start with schemas
        const items = await listCatalog(selectedConnectionId, "schemas");
        schemas = items.map((i) => i.name);
      }
    } catch (e) {
      catalogError = e instanceof Error ? e.message : String(e);
    } finally {
      catalogLoading = false;
    }
  }

  async function onDatabaseChange() {
    // Reset downstream selections
    schemas = [];
    tables = [];
    selectedSchema = "";
    selectedTable = "";

    if (!selectedDatabase) return;

    const dt = connDbType();
    catalogLoading = true;
    catalogError = null;
    try {
      if (dt === "snowflake") {
        // Snowflake: after database, load schemas
        const items = await listCatalog(selectedConnectionId, "schemas", selectedDatabase);
        schemas = items.map((i) => i.name);
      } else if (dt === "mysql") {
        // MySQL: after database, load tables directly
        const items = await listCatalog(selectedConnectionId, "tables", selectedDatabase);
        tables = items.map((i) => i.name);
      }
    } catch (e) {
      catalogError = e instanceof Error ? e.message : String(e);
    } finally {
      catalogLoading = false;
    }
  }

  async function onSchemaChange() {
    tables = [];
    selectedTable = "";

    if (!selectedSchema) return;

    catalogLoading = true;
    catalogError = null;
    try {
      const dt = connDbType();
      if (dt === "snowflake") {
        const items = await listCatalog(selectedConnectionId, "tables", selectedDatabase, selectedSchema);
        tables = items.map((i) => i.name);
      } else {
        // Postgres
        const items = await listCatalog(selectedConnectionId, "tables", undefined, selectedSchema);
        tables = items.map((i) => i.name);
      }
    } catch (e) {
      catalogError = e instanceof Error ? e.message : String(e);
    } finally {
      catalogLoading = false;
    }
  }

  function onTableSelect() {
    if (!selectedTable) return;

    const dt = connDbType();
    if (dt === "snowflake") {
      query = `SELECT * FROM "${selectedDatabase}"."${selectedSchema}"."${selectedTable}"`;
    } else if (dt === "mysql") {
      query = `SELECT * FROM ${selectedDatabase}.${selectedTable}`;
    } else {
      // Postgres
      query = `SELECT * FROM ${selectedSchema}.${selectedTable}`;
    }
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

  // Import/Export
  let importExportStatus: string | null = $state(null);

  async function handleExport() {
    try {
      const path = await save({
        defaultPath: "diff-donkey-connections.json",
        filters: [{ name: "JSON", extensions: ["json"] }],
      });
      if (!path) return;
      const count = await exportConnectionsToFile(path);
      importExportStatus = `Exported ${count} connection${count !== 1 ? "s" : ""}.`;
    } catch (e) {
      importExportStatus = `Export failed: ${e instanceof Error ? e.message : String(e)}`;
    }
  }

  async function handleImport() {
    try {
      const path = await open({
        multiple: false,
        filters: [{ name: "JSON", extensions: ["json"] }],
      });
      if (!path) return;
      const result = await importConnectionsFromFile(path as string);
      await loadConnections();
      const parts: string[] = [];
      if (result.imported > 0) parts.push(`Imported ${result.imported}`);
      if (result.skipped > 0) parts.push(`Skipped ${result.skipped} (${result.skipped_names.join(", ")})`);
      importExportStatus = parts.join(". ") || "No connections to import.";
    } catch (e) {
      importExportStatus = `Import failed: ${e instanceof Error ? e.message : String(e)}`;
    }
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
    <!-- Primary row: connection + query + load -->
    <div class="db-primary-row">
      {#if !manualMode}
        <select
          class="conn-select"
          bind:value={selectedConnectionId}
          onchange={() => { manualMode = false; }}
        >
          <option value="">-- Connection --</option>
          {#each $savedConnections as conn (conn.id)}
            <option value={conn.id}>{conn.name} ({conn.db_type})</option>
          {/each}
        </select>
        <button class="btn-icon" onclick={() => (showNewForm = true)} title="New Connection">+</button>
      {:else}
        <select class="db-type-select" bind:value={dbType}>
          <option value="postgres">Postgres</option>
          <option value="mysql">MySQL</option>
          <option value="snowflake">Snowflake</option>
        </select>
      {/if}

      <input
        class="query-input"
        type="text"
        bind:value={query}
        placeholder="SELECT * FROM my_table WHERE ..."
      />

      {#if history.length > 0}
        <button class="btn-icon" onclick={toggleHistory} title="Query History">&#128337;</button>
      {/if}

      {#if selectedConnectionId}
        <button class="btn-icon" onclick={handleBrowseToggle} title="Browse Tables">&#128269;</button>
      {/if}

      <button class="load-btn" onclick={handleLoad} disabled={loading}>
        {loading ? "..." : "Load"}
      </button>
    </div>

    <!-- Toggle: saved vs manual -->
    <div class="mode-row">
      <button class="link-btn" onclick={() => { manualMode = !manualMode; if (manualMode) selectedConnectionId = ""; }}>
        {manualMode ? "Use saved connection" : "Use connection string"}
      </button>
      {#if meta}
        <span class="meta-inline"><strong>{meta.row_count.toLocaleString()}</strong> rows &middot; {meta.columns.length} cols</span>
      {/if}
      {#if error}
        <span class="error-inline">{error}</span>
      {/if}
    </div>

    <!-- Expandable sections -->
    {#if showHistory && history.length > 0}
      <div class="history-dropdown">
        {#each history as entry (entry.id)}
          <div class="history-entry">
            <button class="history-entry-btn" onclick={() => selectQuery(entry.query)} title={entry.query}>
              <span class="history-query">{truncate(entry.query, 80)}</span>
              <span class="history-time">{formatRelativeTime(entry.last_used_at)}</span>
            </button>
            <button class="history-delete" onclick={() => deleteEntry(entry.id)} title="Remove">&times;</button>
          </div>
        {/each}
      </div>
    {/if}

    {#if showBrowser && selectedConnectionId}
      {@const conn = selectedConnection()}
      {#if conn}
      <div class="catalog-browser">
        {#if conn.db_type === "snowflake" || conn.db_type === "mysql"}
          <select class="cat-select" bind:value={selectedDatabase} onchange={onDatabaseChange}>
            <option value="">-- Database --</option>
            {#each databases as db}<option value={db}>{db}</option>{/each}
          </select>
        {/if}
        {#if conn && conn.db_type !== "mysql"}
          <select class="cat-select" bind:value={selectedSchema} onchange={onSchemaChange}
            disabled={conn?.db_type === "snowflake" && !selectedDatabase}>
            <option value="">-- Schema --</option>
            {#each schemas as s}<option value={s}>{s}</option>{/each}
          </select>
        {/if}
        <select class="cat-select" bind:value={selectedTable} onchange={onTableSelect} disabled={tables.length === 0}>
          <option value="">-- Table --</option>
          {#each tables as t}<option value={t}>{t}</option>{/each}
        </select>
        {#if catalogLoading}<span class="cat-loading">Loading...</span>{/if}
        {#if catalogError}<span class="cat-error">{catalogError}</span>{/if}
      </div>
      {/if}
    {/if}

    {#if manualMode}
      <div class="manual-fields">
        {#if dbType === "snowflake"}
          <input type="text" bind:value={sfAccountUrl} placeholder="Account URL" class="manual-input" />
          <input type="text" bind:value={sfUsername} placeholder="Username" class="manual-input" />
          <input type="password" bind:value={sfPassword} placeholder="Password" class="manual-input" />
          <input type="text" bind:value={sfWarehouse} placeholder="Warehouse" class="manual-input" />
          <input type="text" bind:value={sfRole} placeholder="Role" class="manual-input" />
          <input type="text" bind:value={sfDatabase} placeholder="Database" class="manual-input" />
          <input type="text" bind:value={sfSchema} placeholder="Schema" class="manual-input" />
        {:else}
          <input
            type="password"
            bind:value={connString}
            placeholder={dbType === "postgres"
              ? "host=localhost port=5432 dbname=mydb user=me password=secret"
              : "host=localhost port=3306 user=me password=secret database=mydb"}
            class="manual-input manual-input-wide"
          />
        {/if}
      </div>
    {/if}
  {/if}
</div>

<style>
  .db-source {
    display: flex;
    flex-direction: column;
    gap: 6px;

  }

  .db-primary-row {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
  }

  .conn-select {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.8em;
    background: white;
    color: inherit;
    min-width: 140px;
  }

  .db-type-select {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.8em;
    background: white;
    color: inherit;
  }

  .query-input {
    flex: 1;
    min-width: 180px;
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.8em;
    font-family: monospace;
    background: white;
    color: inherit;
  }

  .btn-icon {
    padding: 3px 7px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: transparent;
    cursor: pointer;
    font-size: 0.85em;
    color: #888;
    line-height: 1;
  }

  .btn-icon:hover {
    color: #396cd8;
    border-color: #396cd8;
  }

  .load-btn {
    padding: 4px 12px;
    border: none;
    border-radius: 4px;
    background: #396cd8;
    color: white;
    cursor: pointer;
    font-size: 0.8em;
    font-weight: 600;
    white-space: nowrap;
  }

  .load-btn:hover:not(:disabled) {
    background: #2d5bbf;
  }

  .load-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .mode-row {
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 0.75em;
  }

  .link-btn {
    background: none;
    border: none;
    color: #888;
    cursor: pointer;
    font-size: 1em;
    padding: 0;
    text-decoration: underline;
  }

  .link-btn:hover {
    color: #396cd8;
  }

  .meta-inline {
    color: #888;
  }

  .error-inline {
    color: #e74c3c;
  }

  .catalog-browser {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
    padding: 4px 0;
  }

  .cat-select {
    padding: 3px 6px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.8em;
    background: white;
    color: inherit;
  }

  .cat-loading {
    font-size: 0.75em;
    color: #888;
  }

  .cat-error {
    font-size: 0.75em;
    color: #e74c3c;
  }

  .manual-fields {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }

  .manual-input {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.8em;
    background: white;
    color: inherit;
    min-width: 100px;
  }

  .manual-input-wide {
    flex: 1;
    min-width: 300px;
  }

  .history-dropdown {
    border: 1px solid #ddd;
    border-radius: 4px;
    max-height: 160px;
    overflow-y: auto;
    background: inherit;
    font-size: 0.8em;
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
    padding: 4px 8px;
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
    padding: 2px 6px;
    cursor: pointer;
    color: #ccc;
    font-size: 1em;
    line-height: 1;
    flex-shrink: 0;
  }

  .history-delete:hover {
    color: #e74c3c;
  }

  @media (prefers-color-scheme: dark) {
    .conn-select, .db-type-select, .query-input, .cat-select, .manual-input {
      border-color: #555;
      background: #2a2a2a;
    }

    .btn-icon {
      border-color: #555;
      color: #999;
    }

    .btn-icon:hover {
      color: #8ab4f8;
      border-color: #8ab4f8;
    }

    .load-btn {
      background: #6b9aff;
      color: #1a1a1a;
    }

    .link-btn {
      color: #999;
    }

    .link-btn:hover {
      color: #8ab4f8;
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
  }
</style>
