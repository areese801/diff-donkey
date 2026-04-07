<script lang="ts">
  import { loadDatabaseSource, loadFromSavedConnection, loadSnowflakeSource, listCatalog } from "$lib/tauri";
  import { savedConnections, loadConnections } from "$lib/stores/connections";
  import { open } from "@tauri-apps/plugin-dialog";
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

      <div class="browse-section">
        <button class="browse-toggle" onclick={handleBrowseToggle}>
          {showBrowser ? "Hide Browser" : "Browse Tables"}
        </button>

        {#if showBrowser}
          <div class="catalog-browser">
            {#if conn && (conn.db_type === "snowflake" || conn.db_type === "mysql")}
              <!-- Database dropdown (Snowflake + MySQL) -->
              <div class="catalog-field">
                <label for="cat-db-{label}">Database</label>
                <select id="cat-db-{label}" bind:value={selectedDatabase} onchange={onDatabaseChange}>
                  <option value="">-- Select database --</option>
                  {#each databases as db}
                    <option value={db}>{db}</option>
                  {/each}
                </select>
              </div>
            {/if}

            {#if conn && conn.db_type !== "mysql"}
              <!-- Schema dropdown (Postgres + Snowflake) -->
              <div class="catalog-field">
                <label for="cat-schema-{label}">Schema</label>
                <select id="cat-schema-{label}" bind:value={selectedSchema} onchange={onSchemaChange}
                  disabled={conn.db_type === "snowflake" && !selectedDatabase}>
                  <option value="">-- Select schema --</option>
                  {#each schemas as s}
                    <option value={s}>{s}</option>
                  {/each}
                </select>
              </div>
            {/if}

            <!-- Table dropdown (all types) -->
            <div class="catalog-field">
              <label for="cat-table-{label}">Table</label>
              <select id="cat-table-{label}" bind:value={selectedTable} onchange={onTableSelect}
                disabled={tables.length === 0}>
                <option value="">-- Select table --</option>
                {#each tables as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
            </div>

            {#if catalogLoading}
              <span class="catalog-loading">Loading...</span>
            {/if}
            {#if catalogError}
              <p class="catalog-error">{catalogError}</p>
            {/if}
          </div>
        {/if}
      </div>
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

  .browse-section {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .browse-toggle {
    align-self: flex-start;
    padding: 4px 10px;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.8em;
    color: inherit;
  }

  .browse-toggle:hover {
    background: rgba(57, 108, 216, 0.1);
  }

  .catalog-browser {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 8px;
    border: 1px solid #e0e0e0;
    border-radius: 4px;
    background: rgba(57, 108, 216, 0.02);
  }

  .catalog-field {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .catalog-field label {
    font-size: 0.8em;
    font-weight: 600;
  }

  .catalog-loading {
    font-size: 0.8em;
    color: #888;
    font-style: italic;
  }

  .catalog-error {
    color: #e74c3c;
    font-size: 0.8em;
    margin: 0;
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

    .browse-toggle {
      border-color: #555;
    }

    .catalog-browser {
      border-color: #444;
      background: rgba(107, 154, 255, 0.05);
    }
  }
</style>
