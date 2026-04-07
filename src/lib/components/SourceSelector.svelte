<script lang="ts">
  import { open } from "@tauri-apps/plugin-dialog";
  import { loadSource, loadRemoteSource } from "$lib/tauri";
  import { sourceA, sourceB } from "$lib/stores/config";
  import DatabaseSource from "$lib/components/DatabaseSource.svelte";
  import ConnectionManager from "$lib/components/ConnectionManager.svelte";
  import type { TableMeta, RemoteCredentials } from "$lib/types/diff";

  type SourceMode = "file" | "database" | "remote";

  /** Current state for each source panel */
  let modeA: SourceMode = $state("file");
  let modeB: SourceMode = $state("file");
  let pathA: string = $state(localStorage.getItem("diff-donkey:pathA") ?? "");
  let pathB: string = $state(localStorage.getItem("diff-donkey:pathB") ?? "");
  let metaA: TableMeta | null = $state(null);
  let metaB: TableMeta | null = $state(null);
  let errorA: string | null = $state(null);
  let errorB: string | null = $state(null);
  let loadingA = $state(false);
  let loadingB = $state(false);
  let showConnectionManager = $state(false);

  /** Remote source state */
  let remoteUriA = $state("");
  let remoteUriB = $state("");
  let accessKeyA = $state("");
  let accessKeyB = $state("");
  let secretKeyA = $state("");
  let secretKeyB = $state("");
  let regionA = $state("");
  let regionB = $state("");
  let endpointA = $state("");
  let endpointB = $state("");

  function needsCredentials(uri: string): boolean {
    return uri.startsWith("s3://") || uri.startsWith("gs://");
  }

  function getProvider(uri: string): string | null {
    if (uri.startsWith("s3://")) return "s3";
    if (uri.startsWith("gs://")) return "gcs";
    return null;
  }

  async function loadRemote(label: "a" | "b") {
    const uri = label === "a" ? remoteUriA : remoteUriB;
    if (!uri.trim()) return;

    if (label === "a") { loadingA = true; errorA = null; }
    else { loadingB = true; errorB = null; }

    const credentials: RemoteCredentials = {
      provider: getProvider(uri),
      access_key: (label === "a" ? accessKeyA : accessKeyB) || null,
      secret_key: (label === "a" ? secretKeyA : secretKeyB) || null,
      region: (label === "a" ? regionA : regionB) || null,
      endpoint: (label === "a" ? endpointA : endpointB) || null,
    };

    try {
      const meta = await loadRemoteSource(uri, label, credentials);
      if (label === "a") {
        metaA = meta;
        sourceA.set(meta);
      } else {
        metaB = meta;
        sourceB.set(meta);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") errorA = msg;
      else errorB = msg;
    } finally {
      if (label === "a") loadingA = false;
      else loadingB = false;
    }
  }

  /** Extract just the filename from a full path */
  function filename(path: string): string {
    return path.split("/").pop()?.split("\\").pop() ?? path;
  }

  /** Extract the directory from a full path for defaultPath */
  function dirname(path: string): string {
    const sep = path.includes("\\") ? "\\" : "/";
    const parts = path.split(sep);
    parts.pop();
    return parts.join(sep);
  }

  async function pickFile(label: "a" | "b") {
    const lastPath = label === "a" ? pathA : pathB;
    const selected = await open({
      multiple: false,
      defaultPath: lastPath ? dirname(lastPath) : undefined,
      filters: [
        { name: "Data Files", extensions: ["csv", "parquet", "pq"] },
      ],
    });

    if (!selected) return;

    const path = typeof selected === "string" ? selected : selected;

    if (label === "a") {
      pathA = path;
      localStorage.setItem("diff-donkey:pathA", path);
      loadingA = true;
      errorA = null;
    } else {
      pathB = path;
      localStorage.setItem("diff-donkey:pathB", path);
      loadingB = true;
      errorB = null;
    }

    try {
      const meta = await loadSource(path, label);
      if (label === "a") {
        metaA = meta;
        sourceA.set(meta);
      } else {
        metaB = meta;
        sourceB.set(meta);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") {
        errorA = msg;
      } else {
        errorB = msg;
      }
    } finally {
      if (label === "a") {
        loadingA = false;
      } else {
        loadingB = false;
      }
    }
  }

  /** Load a file by path (without opening dialog) */
  async function loadFileByPath(path: string, label: "a" | "b") {
    if (!path) return;

    if (label === "a") {
      loadingA = true;
      errorA = null;
    } else {
      loadingB = true;
      errorB = null;
    }

    try {
      const meta = await loadSource(path, label);
      if (label === "a") {
        metaA = meta;
        sourceA.set(meta);
      } else {
        metaB = meta;
        sourceB.set(meta);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") {
        errorA = msg;
        pathA = ""; // clear invalid saved path
        localStorage.removeItem("diff-donkey:pathA");
      } else {
        errorB = msg;
        pathB = "";
        localStorage.removeItem("diff-donkey:pathB");
      }
    } finally {
      if (label === "a") {
        loadingA = false;
      } else {
        loadingB = false;
      }
    }
  }

  // Auto-load saved files on startup
  $effect(() => {
    if (pathA && !metaA) loadFileByPath(pathA, "a");
    if (pathB && !metaB) loadFileByPath(pathB, "b");
  });

  function handleDbLoaded(label: "a" | "b", meta: TableMeta) {
    if (label === "a") {
      metaA = meta;
      sourceA.set(meta);
    } else {
      metaB = meta;
      sourceB.set(meta);
    }
  }
</script>

<div class="source-selector">
  <div class="manage-row">
    <button class="manage-btn" onclick={() => (showConnectionManager = true)}>
      Manage Connections
    </button>
  </div>

  {#if showConnectionManager}
    <ConnectionManager onClose={() => (showConnectionManager = false)} />
  {/if}

  <div class="source-panel">
    <h3>Source A</h3>
    <div class="mode-toggle">
      <button
        class="toggle-btn"
        class:active={modeA === "file"}
        onclick={() => modeA = "file"}
      >File</button>
      <button
        class="toggle-btn"
        class:active={modeA === "database"}
        onclick={() => modeA = "database"}
      >Database</button>
      <button
        class="toggle-btn"
        class:active={modeA === "remote"}
        onclick={() => modeA = "remote"}
      >Remote</button>
    </div>

    {#if modeA === "file"}
      <div class="file-picker">
        <input
          type="text"
          class="file-path"
          value={pathA ? filename(pathA) : ""}
          placeholder="No file selected"
          readonly
          title={pathA || "No file selected"}
        />
        <button class="browse-btn" onclick={() => pickFile("a")} disabled={loadingA}>
          {loadingA ? "..." : "Browse"}
        </button>
      </div>

      {#if errorA}
        <p class="error">{errorA}</p>
      {/if}

      {#if metaA && modeA === "file"}
        <div class="meta">
          <p class="row-count">{metaA.row_count.toLocaleString()} rows</p>
          <ul class="columns">
            {#each metaA.columns as col}
              <li><code>{col.name}</code> <span class="type">{col.data_type}</span></li>
            {/each}
          </ul>
        </div>
      {/if}
    {:else if modeA === "remote"}
      <div class="remote-source">
        <div class="field">
          <label for="remote-uri-a">Remote URL</label>
          <input id="remote-uri-a" type="text" bind:value={remoteUriA}
            placeholder="s3://bucket/path/file.parquet or https://..." />
        </div>

        {#if needsCredentials(remoteUriA)}
          <details class="credentials-section">
            <summary>Credentials (optional — uses env/IAM if empty)</summary>
            <div class="field">
              <label for="access-key-a">Access Key</label>
              <input id="access-key-a" type="text" bind:value={accessKeyA} placeholder="AWS_ACCESS_KEY_ID" />
            </div>
            <div class="field">
              <label for="secret-key-a">Secret Key</label>
              <input id="secret-key-a" type="password" bind:value={secretKeyA} placeholder="AWS_SECRET_ACCESS_KEY" />
            </div>
            <div class="field">
              <label for="region-a">Region</label>
              <input id="region-a" type="text" bind:value={regionA} placeholder="us-east-1" />
            </div>
            <div class="field">
              <label for="endpoint-a">Endpoint (optional)</label>
              <input id="endpoint-a" type="text" bind:value={endpointA} placeholder="For MinIO, R2, etc." />
            </div>
          </details>
        {/if}

        <button class="load-btn" onclick={() => loadRemote("a")} disabled={!remoteUriA.trim() || loadingA}>
          {loadingA ? "Loading..." : "Load Remote File"}
        </button>

        {#if errorA}
          <p class="error">{errorA}</p>
        {/if}

        {#if metaA && modeA === "remote"}
          <div class="meta">
            <p class="row-count">{metaA.row_count.toLocaleString()} rows</p>
            <ul class="columns">
              {#each metaA.columns as col}
                <li><code>{col.name}</code> <span class="type">{col.data_type}</span></li>
              {/each}
            </ul>
          </div>
        {/if}
      </div>
    {:else}
      <DatabaseSource label="a" onLoaded={(meta) => handleDbLoaded("a", meta)} />
    {/if}
  </div>

  <div class="source-panel">
    <h3>Source B</h3>
    <div class="mode-toggle">
      <button
        class="toggle-btn"
        class:active={modeB === "file"}
        onclick={() => modeB = "file"}
      >File</button>
      <button
        class="toggle-btn"
        class:active={modeB === "database"}
        onclick={() => modeB = "database"}
      >Database</button>
      <button
        class="toggle-btn"
        class:active={modeB === "remote"}
        onclick={() => modeB = "remote"}
      >Remote</button>
    </div>

    {#if modeB === "file"}
      <div class="file-picker">
        <input
          type="text"
          class="file-path"
          value={pathB ? filename(pathB) : ""}
          placeholder="No file selected"
          readonly
          title={pathB || "No file selected"}
        />
        <button class="browse-btn" onclick={() => pickFile("b")} disabled={loadingB}>
          {loadingB ? "..." : "Browse"}
        </button>
      </div>

      {#if errorB}
        <p class="error">{errorB}</p>
      {/if}

      {#if metaB && modeB === "file"}
        <div class="meta">
          <p class="row-count">{metaB.row_count.toLocaleString()} rows</p>
          <ul class="columns">
            {#each metaB.columns as col}
              <li><code>{col.name}</code> <span class="type">{col.data_type}</span></li>
            {/each}
          </ul>
        </div>
      {/if}
    {:else if modeB === "remote"}
      <div class="remote-source">
        <div class="field">
          <label for="remote-uri-b">Remote URL</label>
          <input id="remote-uri-b" type="text" bind:value={remoteUriB}
            placeholder="s3://bucket/path/file.parquet or https://..." />
        </div>

        {#if needsCredentials(remoteUriB)}
          <details class="credentials-section">
            <summary>Credentials (optional — uses env/IAM if empty)</summary>
            <div class="field">
              <label for="access-key-b">Access Key</label>
              <input id="access-key-b" type="text" bind:value={accessKeyB} placeholder="AWS_ACCESS_KEY_ID" />
            </div>
            <div class="field">
              <label for="secret-key-b">Secret Key</label>
              <input id="secret-key-b" type="password" bind:value={secretKeyB} placeholder="AWS_SECRET_ACCESS_KEY" />
            </div>
            <div class="field">
              <label for="region-b">Region</label>
              <input id="region-b" type="text" bind:value={regionB} placeholder="us-east-1" />
            </div>
            <div class="field">
              <label for="endpoint-b">Endpoint (optional)</label>
              <input id="endpoint-b" type="text" bind:value={endpointB} placeholder="For MinIO, R2, etc." />
            </div>
          </details>
        {/if}

        <button class="load-btn" onclick={() => loadRemote("b")} disabled={!remoteUriB.trim() || loadingB}>
          {loadingB ? "Loading..." : "Load Remote File"}
        </button>

        {#if errorB}
          <p class="error">{errorB}</p>
        {/if}

        {#if metaB && modeB === "remote"}
          <div class="meta">
            <p class="row-count">{metaB.row_count.toLocaleString()} rows</p>
            <ul class="columns">
              {#each metaB.columns as col}
                <li><code>{col.name}</code> <span class="type">{col.data_type}</span></li>
              {/each}
            </ul>
          </div>
        {/if}
      </div>
    {:else}
      <DatabaseSource label="b" onLoaded={(meta) => handleDbLoaded("b", meta)} />
    {/if}
  </div>
</div>

<style>
  .source-selector {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    padding: 16px;
  }

  .manage-row {
    grid-column: 1 / -1;
    display: flex;
    justify-content: flex-end;
  }

  .manage-btn {
    padding: 4px 12px;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.8em;
    color: #888;
  }

  .manage-btn:hover {
    color: #396cd8;
    border-color: #396cd8;
  }

  .source-panel {
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 16px;
  }

  h3 {
    margin: 0 0 12px 0;
    font-size: 1.1em;
  }

  .mode-toggle {
    display: flex;
    gap: 0;
    margin-bottom: 12px;
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

  .toggle-btn:hover:not(.active) {
    background: rgba(57, 108, 216, 0.1);
  }

  .file-picker {
    display: flex;
    gap: 0;
    border: 1px solid #ccc;
    border-radius: 6px;
    overflow: hidden;
  }

  .file-path {
    flex: 1;
    padding: 8px 10px;
    border: none;
    background: transparent;
    font-size: 0.9em;
    color: inherit;
    outline: none;
    cursor: default;
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
  }

  .file-path::placeholder {
    color: #aaa;
  }

  .browse-btn {
    padding: 8px 16px;
    border: none;
    border-left: 1px solid #ccc;
    background: #f0f0f0;
    cursor: pointer;
    font-size: 0.85em;
    font-weight: 500;
    color: inherit;
    white-space: nowrap;
  }

  .browse-btn:hover:not(:disabled) {
    background: #e0e0e0;
  }

  .browse-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .error {
    color: #e74c3c;
    font-size: 0.85em;
    margin: 8px 0;
  }

  .meta {
    margin-top: 12px;
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

  .remote-source {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .remote-source .field {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .remote-source .field label {
    font-size: 0.8em;
    font-weight: 500;
    color: #666;
  }

  .remote-source .field input {
    padding: 8px 10px;
    border: 1px solid #ccc;
    border-radius: 6px;
    font-size: 0.9em;
    background: transparent;
    color: inherit;
  }

  .remote-source .field input:focus {
    outline: none;
    border-color: #396cd8;
  }

  .credentials-section {
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    padding: 8px;
  }

  .credentials-section summary {
    cursor: pointer;
    font-size: 0.8em;
    color: #888;
    user-select: none;
  }

  .credentials-section[open] summary {
    margin-bottom: 8px;
  }

  .credentials-section .field {
    margin-top: 6px;
  }

  .load-btn {
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    background: #396cd8;
    color: white;
    cursor: pointer;
    font-size: 0.85em;
    font-weight: 500;
  }

  .load-btn:hover:not(:disabled) {
    background: #2d5ab8;
  }

  .load-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  @media (prefers-color-scheme: dark) {
    .source-panel {
      border-color: #444;
    }

    .mode-toggle {
      border-color: #555;
    }

    .toggle-btn.active {
      background: #6b9aff;
      color: #1a1a1a;
    }

    .file-picker {
      border-color: #555;
    }

    .browse-btn {
      border-left-color: #555;
      background: #3a3a3a;
    }

    .browse-btn:hover:not(:disabled) {
      background: #4a4a4a;
    }

    .remote-source .field label {
      color: #aaa;
    }

    .remote-source .field input {
      border-color: #555;
    }

    .remote-source .field input:focus {
      border-color: #6b9aff;
    }

    .credentials-section {
      border-color: #444;
    }

    .load-btn {
      background: #6b9aff;
      color: #1a1a1a;
    }

    .load-btn:hover:not(:disabled) {
      background: #5a89ee;
    }
  }
</style>
