<script lang="ts">
  import { saveConnection, testConnection } from "$lib/tauri";
  import { loadConnections } from "$lib/stores/connections";
  import { open } from "@tauri-apps/plugin-dialog";
  import type { SavedConnection } from "$lib/types/connections";

  interface Props {
    /** Pre-populated connection for editing, or null for new */
    connection?: SavedConnection | null;
    /** Called when save succeeds or form is cancelled */
    onClose: () => void;
  }

  let { connection = null, onClose }: Props = $props();

  type FormMode = "builder" | "raw";

  // Form state
  let mode: FormMode = $state("builder");
  let name = $state("");
  let dbType = $state("postgres");
  let host = $state("");
  let port = $state<number | string>(5432);
  let database = $state("");
  let username = $state("");
  let password = $state("");
  let schema = $state("");
  let ssl = $state(false);
  let color = $state("#396cd8");
  let rawConnString = $state("");
  let initialId = $state("");
  let initialCreatedAt = $state("");
  // Snowflake-specific fields
  let accountUrl = $state("");
  let warehouse = $state("");
  let sfRole = $state("");
  let authMethod = $state("password");
  let privateKeyPath = $state("");
  let privateKeyFilename = $state("");
  let initialized = false;

  // Populate form from connection prop on mount
  $effect(() => {
    if (!initialized && connection) {
      name = connection.name;
      dbType = connection.db_type;
      host = connection.host ?? "";
      port = connection.port ?? defaultPort(connection.db_type);
      database = connection.database ?? "";
      username = connection.username ?? "";
      schema = connection.schema ?? "";
      ssl = connection.ssl;
      color = connection.color ?? "#396cd8";
      accountUrl = connection.account_url ?? "";
      warehouse = connection.warehouse ?? "";
      sfRole = connection.role ?? "";
      authMethod = connection.auth_method ?? "password";
      privateKeyPath = connection.private_key_path ?? "";
      if (privateKeyPath) {
        privateKeyFilename = privateKeyPath.split(/[/\\]/).pop() ?? "";
      }
      initialId = connection.id;
      initialCreatedAt = connection.created_at;
      initialized = true;
    }
  });

  // UI state
  let saving = $state(false);
  let testing = $state(false);
  let error: string | null = $state(null);
  let testResult: { success: boolean; message: string } | null = $state(null);

  function defaultPort(type: string): number {
    return type === "mysql" ? 3306 : 5432;
  }

  function handleDbTypeChange() {
    if (dbType !== "snowflake") {
      port = defaultPort(dbType);
    }
  }

  async function handleSelectKeyFile() {
    const selected = await open({
      multiple: false,
      filters: [{ name: "Private Key", extensions: ["p8", "pem"] }],
    });
    if (selected) {
      privateKeyPath = selected as string;
      privateKeyFilename = privateKeyPath.split(/[/\\]/).pop() ?? "";
    }
  }

  function buildConnection(): SavedConnection {
    const now = new Date().toISOString();
    return {
      id: initialId || crypto.randomUUID(),
      name: name.trim(),
      db_type: dbType,
      host: host.trim() || null,
      port: typeof port === "string" ? parseInt(port) || null : port || null,
      database: database.trim() || null,
      username: username.trim() || null,
      schema: schema.trim() || null,
      ssl,
      color: color || null,
      account_url: dbType === "snowflake" ? (accountUrl.trim() || null) : null,
      warehouse: dbType === "snowflake" ? (warehouse.trim() || null) : null,
      role: dbType === "snowflake" ? (sfRole.trim() || null) : null,
      auth_method: dbType === "snowflake" ? authMethod : null,
      private_key_path: dbType === "snowflake" && authMethod === "keypair" ? (privateKeyPath || null) : null,
      ssh_enabled: false,
      ssh_host: null,
      ssh_port: null,
      ssh_username: null,
      ssh_auth_method: null,
      ssh_key_path: null,
      created_at: initialCreatedAt || now,
      updated_at: now,
    };
  }

  async function handleTest() {
    testing = true;
    testResult = null;
    error = null;

    try {
      const conn = buildConnection();
      const msg = await testConnection(conn, password || null);
      testResult = { success: true, message: msg };
    } catch (e) {
      testResult = {
        success: false,
        message: e instanceof Error ? e.message : String(e),
      };
    } finally {
      testing = false;
    }
  }

  async function handleSave() {
    if (!name.trim()) {
      error = "Connection name is required.";
      return;
    }

    saving = true;
    error = null;

    try {
      const conn = buildConnection();
      await saveConnection(conn, password || null);
      await loadConnections();
      onClose();
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      saving = false;
    }
  }
</script>

<div class="connection-form">
  <div class="form-header">
    <h3>{initialId ? "Edit Connection" : "New Connection"}</h3>
    <button class="close-btn" onclick={onClose}>&#x2715;</button>
  </div>

  <div class="mode-toggle">
    <button
      class="toggle-btn"
      class:active={mode === "builder"}
      onclick={() => (mode = "builder")}
    >Builder</button>
    <button
      class="toggle-btn"
      class:active={mode === "raw"}
      onclick={() => (mode = "raw")}
    >Raw</button>
  </div>

  {#if mode === "builder"}
    <div class="form-grid">
      <div class="field full-width">
        <label for="conn-name">Name</label>
        <input id="conn-name" type="text" bind:value={name} placeholder="Prod Postgres" />
      </div>

      <div class="field">
        <label for="conn-type">Type</label>
        <select id="conn-type" bind:value={dbType} onchange={handleDbTypeChange}>
          <option value="postgres">PostgreSQL</option>
          <option value="mysql">MySQL</option>
          <option value="snowflake">Snowflake</option>
        </select>
      </div>

      <div class="field">
        <label for="conn-color">Color</label>
        <div class="color-row">
          <input id="conn-color" type="color" bind:value={color} />
          <span class="color-hex">{color}</span>
        </div>
      </div>

      {#if dbType === "snowflake"}
        <div class="field full-width">
          <label for="conn-account-url">Account URL</label>
          <input id="conn-account-url" type="text" bind:value={accountUrl} placeholder="https://myorg-myaccount.snowflakecomputing.com" />
        </div>

        <div class="field">
          <label for="conn-auth-method">Auth Method</label>
          <select id="conn-auth-method" bind:value={authMethod}>
            <option value="password">Password</option>
            <option value="keypair">Key Pair</option>
          </select>
        </div>

        <div class="field">
          <label for="conn-username">Username</label>
          <input id="conn-username" type="text" bind:value={username} placeholder="MYUSER" />
        </div>

        {#if authMethod === "password"}
          <div class="field">
            <label for="conn-password">Password</label>
            <input id="conn-password" type="password" bind:value={password} placeholder="Enter password" />
          </div>
        {:else}
          <div class="field">
            <label for="conn-keyfile">Private Key (.p8 / .pem)</label>
            <div class="key-file-row">
              <button id="conn-keyfile" type="button" class="btn-secondary btn-small-file" onclick={handleSelectKeyFile}>
                Select Key File
              </button>
              {#if privateKeyFilename}
                <span class="key-filename">{privateKeyFilename}</span>
              {/if}
            </div>
          </div>
        {/if}

        <div class="field">
          <label for="conn-warehouse">Warehouse</label>
          <input id="conn-warehouse" type="text" bind:value={warehouse} placeholder="COMPUTE_WH" />
        </div>

        <div class="field">
          <label for="conn-role">Role</label>
          <input id="conn-role" type="text" bind:value={sfRole} placeholder="SYSADMIN" />
        </div>

        <div class="field">
          <label for="conn-database">Database</label>
          <input id="conn-database" type="text" bind:value={database} placeholder="MY_DB" />
        </div>

        <div class="field">
          <label for="conn-schema">Schema</label>
          <input id="conn-schema" type="text" bind:value={schema} placeholder="PUBLIC" />
        </div>
      {:else}
        <div class="field">
          <label for="conn-host">Host</label>
          <input id="conn-host" type="text" bind:value={host} placeholder="localhost" />
        </div>

        <div class="field">
          <label for="conn-port">Port</label>
          <input id="conn-port" type="number" bind:value={port} />
        </div>

        <div class="field">
          <label for="conn-database">Database</label>
          <input id="conn-database" type="text" bind:value={database} placeholder="mydb" />
        </div>

        <div class="field">
          <label for="conn-schema">Schema</label>
          <input id="conn-schema" type="text" bind:value={schema} placeholder="public" />
        </div>

        <div class="field">
          <label for="conn-username">Username</label>
          <input id="conn-username" type="text" bind:value={username} placeholder="postgres" />
        </div>

        <div class="field">
          <label for="conn-password">Password</label>
          <input id="conn-password" type="password" bind:value={password} placeholder="Enter password" />
        </div>

        <div class="field full-width">
          <label class="checkbox-label">
            <input type="checkbox" bind:checked={ssl} />
            Require SSL
          </label>
        </div>
      {/if}
    </div>
  {:else}
    <div class="field">
      <label for="raw-conn">Connection String</label>
      <textarea
        id="raw-conn"
        bind:value={rawConnString}
        placeholder="host=localhost port=5432 dbname=mydb user=postgres password=secret"
        rows="4"
      ></textarea>
      <p class="help-text">Paste a full connection string. This will be used directly — saved connections store structured fields instead.</p>
    </div>
  {/if}

  {#if testResult}
    <div class="test-result" class:success={testResult.success} class:failure={!testResult.success}>
      {testResult.success ? "Connected successfully" : testResult.message}
    </div>
  {/if}

  {#if error}
    <p class="error">{error}</p>
  {/if}

  <div class="form-actions">
    <button class="btn-secondary" onclick={onClose}>Cancel</button>
    <button class="btn-secondary" onclick={handleTest} disabled={testing}>
      {testing ? "Testing..." : "Test Connection"}
    </button>
    <button class="btn-primary" onclick={handleSave} disabled={saving || mode === "raw"}>
      {saving ? "Saving..." : "Save"}
    </button>
  </div>
</div>

<style>
  .connection-form {
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 16px;
    max-width: 520px;
  }

  .form-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .form-header h3 {
    margin: 0;
    font-size: 1.1em;
  }

  .close-btn {
    background: none;
    border: none;
    font-size: 1.2em;
    cursor: pointer;
    color: inherit;
    padding: 4px 8px;
  }

  .mode-toggle {
    display: flex;
    border: 1px solid #ccc;
    border-radius: 6px;
    overflow: hidden;
  }

  .toggle-btn {
    flex: 1;
    padding: 6px 12px;
    border: none;
    background: transparent;
    cursor: pointer;
    font-size: 0.85em;
    font-weight: 500;
    color: inherit;
  }

  .toggle-btn.active {
    background: #396cd8;
    color: white;
  }

  .form-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
  }

  .full-width {
    grid-column: 1 / -1;
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

  input[type="text"],
  input[type="password"],
  input[type="number"],
  select,
  textarea {
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

  input[type="color"] {
    width: 32px;
    height: 32px;
    padding: 2px;
    border: 1px solid #ccc;
    border-radius: 4px;
    cursor: pointer;
    background: inherit;
  }

  .color-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .color-hex {
    font-size: 0.85em;
    color: #888;
    font-family: monospace;
  }

  .key-file-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .btn-small-file {
    padding: 6px 12px;
    font-size: 0.85em;
    white-space: nowrap;
  }

  .key-filename {
    font-size: 0.85em;
    color: #888;
    font-family: monospace;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .checkbox-label {
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    font-weight: normal !important;
  }

  .help-text {
    font-size: 0.8em;
    color: #888;
    margin: 0;
  }

  .test-result {
    padding: 8px 12px;
    border-radius: 4px;
    font-size: 0.9em;
  }

  .test-result.success {
    background: rgba(46, 204, 113, 0.15);
    color: #27ae60;
    border: 1px solid rgba(46, 204, 113, 0.3);
  }

  .test-result.failure {
    background: rgba(231, 76, 60, 0.1);
    color: #e74c3c;
    border: 1px solid rgba(231, 76, 60, 0.3);
  }

  .error {
    color: #e74c3c;
    font-size: 0.85em;
    margin: 0;
  }

  .form-actions {
    display: flex;
    gap: 8px;
    justify-content: flex-end;
    margin-top: 4px;
  }

  .btn-primary,
  .btn-secondary {
    padding: 8px 16px;
    border-radius: 6px;
    cursor: pointer;
    font-size: 0.9em;
    border: 1px solid #ccc;
  }

  .btn-primary {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  .btn-primary:hover:not(:disabled) {
    background: #2a5ab8;
  }

  .btn-secondary {
    background: transparent;
    color: inherit;
  }

  .btn-secondary:hover:not(:disabled) {
    background: rgba(57, 108, 216, 0.1);
  }

  .btn-primary:disabled,
  .btn-secondary:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  @media (prefers-color-scheme: dark) {
    input[type="text"],
    input[type="password"],
    input[type="number"],
    select,
    textarea {
      border-color: #555;
    }

    .mode-toggle {
      border-color: #555;
    }

    .toggle-btn.active {
      background: #6b9aff;
      color: #1a1a1a;
    }

    .btn-primary {
      background: #24c8db;
      border-color: #24c8db;
      color: #1a1a1a;
    }

    .btn-primary:hover:not(:disabled) {
      background: #1db0c0;
    }

    .btn-secondary {
      border-color: #555;
    }
  }
</style>
