<script lang="ts">
  import { open } from "@tauri-apps/plugin-dialog";
  import { loadSource } from "$lib/tauri";
  import { sourceA, sourceB } from "$lib/stores/config";
  import type { TableMeta } from "$lib/types/diff";

  /** Current state for each source panel */
  let metaA: TableMeta | null = $state(null);
  let metaB: TableMeta | null = $state(null);
  let errorA: string | null = $state(null);
  let errorB: string | null = $state(null);
  let loadingA = $state(false);
  let loadingB = $state(false);

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
</script>

<div class="source-selector">
  <div class="source-panel">
    <h3>Source A</h3>
    <button onclick={() => pickFile("a")} disabled={loadingA}>
      {loadingA ? "Loading..." : metaA ? "Change File" : "Select File"}
    </button>

    {#if errorA}
      <p class="error">{errorA}</p>
    {/if}

    {#if metaA}
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

  <div class="source-panel">
    <h3>Source B</h3>
    <button onclick={() => pickFile("b")} disabled={loadingB}>
      {loadingB ? "Loading..." : metaB ? "Change File" : "Select File"}
    </button>

    {#if errorB}
      <p class="error">{errorB}</p>
    {/if}

    {#if metaB}
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
</div>

<style>
  .source-selector {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
    padding: 16px;
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

  button {
    width: 100%;
    padding: 10px;
    border-radius: 6px;
    border: 2px dashed #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.95em;
    color: inherit;
  }

  button:hover:not(:disabled) {
    border-color: #396cd8;
    color: #396cd8;
  }

  button:disabled {
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

    button {
      border-color: #555;
    }

    button:hover:not(:disabled) {
      border-color: #24c8db;
      color: #24c8db;
    }
  }
</style>
