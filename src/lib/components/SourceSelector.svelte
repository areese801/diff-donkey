<script lang="ts">
  import { open } from "@tauri-apps/plugin-dialog";
  import { loadSource } from "$lib/tauri";
  import { sourceA, sourceB } from "$lib/stores/config";
  import DatabaseSource from "$lib/components/DatabaseSource.svelte";
  import ConnectionManager from "$lib/components/ConnectionManager.svelte";
  import type { TableMeta } from "$lib/types/diff";

  type SourceMode = "file" | "database";

  /** Current state for each source panel */
  let modeA: SourceMode = $state("file");
  let modeB: SourceMode = $state("file");
  let metaA: TableMeta | null = $state(null);
  let metaB: TableMeta | null = $state(null);
  let errorA: string | null = $state(null);
  let errorB: string | null = $state(null);
  let loadingA = $state(false);
  let loadingB = $state(false);
  let showConnectionManager = $state(false);

  async function pickFile(label: "a" | "b") {
    const selected = await open({
      multiple: false,
      filters: [
        { name: "Data Files", extensions: ["csv", "parquet", "pq"] },
      ],
    });

    if (!selected) return; // User cancelled

    const path = typeof selected === "string" ? selected : selected;

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
    </div>

    {#if modeA === "file"}
      <button class="pick-btn" onclick={() => pickFile("a")} disabled={loadingA}>
        {loadingA ? "Loading..." : metaA ? "Change File" : "Select File"}
      </button>

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
    </div>

    {#if modeB === "file"}
      <button class="pick-btn" onclick={() => pickFile("b")} disabled={loadingB}>
        {loadingB ? "Loading..." : metaB ? "Change File" : "Select File"}
      </button>

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

  .pick-btn {
    width: 100%;
    padding: 10px;
    border-radius: 6px;
    border: 2px dashed #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.95em;
    color: inherit;
  }

  .pick-btn:hover:not(:disabled) {
    border-color: #396cd8;
    color: #396cd8;
  }

  .pick-btn:disabled {
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

    .pick-btn {
      border-color: #555;
    }

    .pick-btn:hover:not(:disabled) {
      border-color: #24c8db;
      color: #24c8db;
    }
  }
</style>
