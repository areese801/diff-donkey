<script lang="ts">
  import { open } from "@tauri-apps/plugin-dialog";
  import { loadSource, loadRemoteSource } from "$lib/tauri";
  import { sourceA, sourceB } from "$lib/stores/config";
  import DatabaseSource from "$lib/components/DatabaseSource.svelte";
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
  <!-- Source A row -->
  <div class="source-row">
    <span class="source-label">Source A</span>
    <div class="mode-toggle">
      <button class="toggle-btn" class:active={modeA === "file"} onclick={() => modeA = "file"}>File</button>
      <button class="toggle-btn" class:active={modeA === "database"} onclick={() => modeA = "database"}>Database</button>
      <button class="toggle-btn" class:active={modeA === "remote"} onclick={() => modeA = "remote"}>Remote</button>
    </div>

    {#if modeA === "file"}
      <div class="file-picker">
        <input type="text" class="file-path" value={pathA ? filename(pathA) : ""} placeholder="No file selected" readonly title={pathA || "No file selected"} />
        <button class="browse-btn" onclick={() => pickFile("a")} disabled={loadingA}>{loadingA ? "..." : "Browse"}</button>
      </div>
      {#if metaA && modeA === "file"}
        <span class="meta-summary"><strong>{metaA.row_count.toLocaleString()}</strong> rows &middot; {metaA.columns.length} cols</span>
      {/if}
      {#if errorA}<span class="error">{errorA}</span>{/if}
    {:else if modeA === "remote"}
      <input class="remote-uri" type="text" bind:value={remoteUriA} placeholder="s3://bucket/path/file.parquet or https://..." />
      <button class="load-btn" onclick={() => loadRemote("a")} disabled={!remoteUriA.trim() || loadingA}>{loadingA ? "..." : "Load"}</button>
      {#if metaA && modeA === "remote"}
        <span class="meta-summary"><strong>{metaA.row_count.toLocaleString()}</strong> rows &middot; {metaA.columns.length} cols</span>
      {/if}
      {#if errorA}<span class="error">{errorA}</span>{/if}
    {:else}
      <DatabaseSource label="a" onLoaded={(meta) => handleDbLoaded("a", meta)} />
    {/if}
  </div>

  <!-- Source B row -->
  <div class="source-row source-row-right">
    <span class="source-label">Source B</span>
    <div class="mode-toggle">
      <button class="toggle-btn" class:active={modeB === "file"} onclick={() => modeB = "file"}>File</button>
      <button class="toggle-btn" class:active={modeB === "database"} onclick={() => modeB = "database"}>Database</button>
      <button class="toggle-btn" class:active={modeB === "remote"} onclick={() => modeB = "remote"}>Remote</button>
    </div>

    {#if modeB === "file"}
      <div class="file-picker">
        <input type="text" class="file-path" value={pathB ? filename(pathB) : ""} placeholder="No file selected" readonly title={pathB || "No file selected"} />
        <button class="browse-btn" onclick={() => pickFile("b")} disabled={loadingB}>{loadingB ? "..." : "Browse"}</button>
      </div>
      {#if metaB && modeB === "file"}
        <span class="meta-summary"><strong>{metaB.row_count.toLocaleString()}</strong> rows &middot; {metaB.columns.length} cols</span>
      {/if}
      {#if errorB}<span class="error">{errorB}</span>{/if}
    {:else if modeB === "remote"}
      <input class="remote-uri" type="text" bind:value={remoteUriB} placeholder="s3://bucket/path/file.parquet or https://..." />
      <button class="load-btn" onclick={() => loadRemote("b")} disabled={!remoteUriB.trim() || loadingB}>{loadingB ? "..." : "Load"}</button>
      {#if metaB && modeB === "remote"}
        <span class="meta-summary"><strong>{metaB.row_count.toLocaleString()}</strong> rows &middot; {metaB.columns.length} cols</span>
      {/if}
      {#if errorB}<span class="error">{errorB}</span>{/if}
    {:else}
      <DatabaseSource label="b" onLoaded={(meta) => handleDbLoaded("b", meta)} />
    {/if}
  </div>
</div>

<style>
  .source-selector {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px 16px;
  }

  .source-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 0;
    flex-wrap: wrap;
  }

  .source-row-right {
    justify-content: flex-end;
  }

  .source-label {
    font-weight: 700;
    font-size: 0.85em;
    min-width: 60px;
    white-space: nowrap;
  }

  .mode-toggle {
    display: flex;
    gap: 0;
    border: 1px solid #ccc;
    border-radius: 4px;
    overflow: hidden;
  }

  .toggle-btn {
    padding: 4px 10px;
    border: none;
    background: transparent;
    cursor: pointer;
    font-size: 0.8em;
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
    border-radius: 4px;
    overflow: hidden;
    min-width: 180px;
  }

  .file-path {
    flex: 1;
    padding: 4px 8px;
    border: none;
    background: transparent;
    font-size: 0.85em;
    color: inherit;
    outline: none;
    cursor: default;
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
    min-width: 100px;
  }

  .file-path::placeholder {
    color: #aaa;
  }

  .browse-btn {
    padding: 4px 10px;
    border: none;
    border-left: 1px solid #ccc;
    background: #f0f0f0;
    cursor: pointer;
    font-size: 0.8em;
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

  .remote-uri {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 0.85em;
    color: inherit;
    background: transparent;
    min-width: 200px;
    flex: 1;
  }

  .load-btn {
    padding: 4px 10px;
    border: none;
    border-radius: 4px;
    background: #396cd8;
    color: white;
    cursor: pointer;
    font-size: 0.8em;
    font-weight: 500;
  }

  .load-btn:hover:not(:disabled) {
    background: #2d5bbf;
  }

  .load-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .error {
    color: #e74c3c;
    font-size: 0.8em;
  }

  .meta-summary {
    font-size: 0.8em;
    color: #888;
    white-space: nowrap;
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

    .remote-uri {
      border-color: #555;
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
