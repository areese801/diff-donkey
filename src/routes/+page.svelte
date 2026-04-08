<script lang="ts">
  import SourceSelector from "$lib/components/SourceSelector.svelte";
  import DiffConfigStrip from "$lib/components/DiffConfigStrip.svelte";
  import ValuesTab from "$lib/components/ValuesTab.svelte";
  import ConnectionManager from "$lib/components/ConnectionManager.svelte";
  import ActivityTab from "$lib/components/ActivityTab.svelte";
  import { sourceA, sourceB } from "$lib/stores/config";
  import { diffResult, isLoading, pkColumn, diffPrecision, ignoredColumns as ignoredColumnsStore } from "$lib/stores/diff";
  import { getSchemaComparison, runDiff } from "$lib/tauri";
  import type { SchemaComparison } from "$lib/types/diff";

  let schemaComparison: SchemaComparison | null = $state(null);
  let diffError: string | null = $state(null);
  let activityOpen = $state(false);
  let setupCollapsed = $state(false);
  let showConnectionManager = $state(false);

  let bothLoaded = $derived(!!$sourceA && !!$sourceB);

  let setupSummary = $derived.by(() => {
    if (!$sourceA || !$sourceB) return "";
    const pkDisplay = $pkColumn || "none";
    const colCount = schemaComparison?.shared.length ?? 0;
    return `Source A (${$sourceA.row_count.toLocaleString()} rows) vs Source B (${$sourceB.row_count.toLocaleString()} rows) \u00B7 PK: ${pkDisplay} \u00B7 ${colCount} cols compared`;
  });
  $effect(() => {
    if (bothLoaded) {
      fetchSchemaComparison();
    }
  });

  async function fetchSchemaComparison() {
    try {
      schemaComparison = await getSchemaComparison();
    } catch (e) {
      console.error("Schema comparison failed:", e);
    }
  }

  async function handleRunDiff(
    selectedPks: string[],
    tolerance: number | null,
    columnTolerances: Record<string, import("$lib/types/diff").ColumnTolerance> | null,
    ignoredColumns: string[],
    whereClause: string | null,
    pkExpression: string | null = null,
  ) {
    isLoading.set(true);
    diffError = null;
    pkColumn.set(pkExpression ? `expr: ${pkExpression}` : selectedPks.join(", "));

    try {
      const isFirstRun = !$diffResult;
      const result = await runDiff({
        pk_columns: selectedPks,
        pk_expression: pkExpression,
        tolerance,
        column_tolerances: columnTolerances,
        ignored_columns: ignoredColumns.length > 0 ? ignoredColumns : undefined,
        where_clause: whereClause,
      });
      diffResult.set(result);
      diffPrecision.set(tolerance);
      ignoredColumnsStore.set(ignoredColumns);
      setupCollapsed = true;
      // no-op: values tab is always visible
    } catch (e) {
      diffError = e instanceof Error ? e.message : String(e);
    } finally {
      isLoading.set(false);
    }
  }
</script>

<div class="app-layout">
  <main class="container">
    <div class="page-header">
      <div>
        <h1>Diff Donkey</h1>
        <p class="subtitle">Dataset comparison powered by DuckDB</p>
      </div>
      <button class="settings-btn" onclick={() => showConnectionManager = true} title="Manage Connections">
        &#9881;
      </button>
    </div>

    {#if showConnectionManager}
      <ConnectionManager onClose={() => showConnectionManager = false} />
    {/if}

    {#if !bothLoaded}
      <SourceSelector />
    {/if}

    {#if bothLoaded}
      <!-- Setup area: collapsible after diff runs -->
      <div class="setup-section" class:collapsed={setupCollapsed}>
        <button class="setup-handle" onclick={() => setupCollapsed = !setupCollapsed}>
          <span class="handle-icon">{setupCollapsed ? "▶" : "▼"}</span>
          {#if setupCollapsed}
            <span class="setup-summary">{setupSummary}</span>
          {:else}
            <span class="setup-label">Configuration</span>
          {/if}
        </button>

        {#if !setupCollapsed}
          <div class="setup-content">
            <SourceSelector />
            <DiffConfigStrip
              sourceA={$sourceA}
              sourceB={$sourceB}
              {schemaComparison}
              onRunDiff={handleRunDiff}
              isLoading={$isLoading}
            />
          </div>
        {/if}
      </div>

      {#if diffError}
        <p class="error">{diffError}</p>
      {/if}

      <ValuesTab
        columnStats={$diffResult?.diff_stats.columns ?? []}
        valuesSummary={$diffResult?.values_summary}
        precision={$diffPrecision}
        result={$diffResult}
        {schemaComparison}
      />
    {/if}
  </main>

  <!-- Bottom panel: Activity Log -->
  <div class="activity-panel" class:open={activityOpen}>
    <button class="activity-handle" onclick={() => activityOpen = !activityOpen}>
      <span class="handle-icon">{activityOpen ? "▼" : "▲"}</span>
      Activity Log
    </button>
    {#if activityOpen}
      <div class="activity-content">
        <ActivityTab />
      </div>
    {/if}
  </div>
</div>

<style>
  :root {
    font-family: Inter, Avenir, Helvetica, Arial, sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 400;
    color: #0f0f0f;
    background-color: #f6f6f6;
  }

  .app-layout {
    display: flex;
    flex-direction: column;
    min-height: 100vh;
  }

  .container {
    max-width: 1800px;
    margin: 0 auto;
    padding: 24px 3%;
    flex: 1;
  }

  .setup-section {
    margin-bottom: 16px;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    overflow: hidden;
    width: 100%;
  }

  .setup-section.collapsed {
    border-radius: 6px;
  }

  .setup-handle {
    width: 100%;
    padding: 8px 12px;
    border: none;
    background: #f5f5f5;
    cursor: pointer;
    font-size: 0.85em;
    font-weight: 600;
    color: #888;
    text-align: left;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .setup-handle:hover {
    color: #555;
    background: #eeeeee;
  }

  .setup-content {
    padding: 12px;
  }

  .setup-summary {
    font-weight: 400;
    color: #666;
  }

  .setup-label {
    font-weight: 600;
  }

  .page-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-bottom: 8px;
  }

  h1 {
    margin: 0;
    font-size: 1.8em;
  }

  .settings-btn {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 6px;
    background: transparent;
    cursor: pointer;
    font-size: 1.2em;
    color: #888;
    line-height: 1;
  }

  .settings-btn:hover {
    color: #396cd8;
    border-color: #396cd8;
  }

  .subtitle {
    color: #666;
    margin: 4px 0 24px 0;
    font-size: 0.95em;
  }

  .error {
    color: #e74c3c;
    padding: 8px 16px;
    background: #ffeaea;
    border-radius: 6px;
  }

  /* Bottom activity panel */
  .activity-panel {
    position: sticky;
    bottom: 0;
    border-top: 1px solid #e0e0e0;
    background: #f9f9f9;
    z-index: 10;
  }

  .activity-handle {
    width: 100%;
    padding: 6px 16px;
    border: none;
    background: transparent;
    cursor: pointer;
    font-size: 0.82em;
    font-weight: 600;
    color: #888;
    text-align: left;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .activity-handle:hover {
    color: #555;
    background: #f0f0f0;
  }

  .handle-icon {
    font-size: 0.75em;
  }

  .activity-content {
    max-height: 300px;
    overflow-y: auto;
    padding: 0 16px 8px;
  }

  @media (prefers-color-scheme: dark) {
    :root {
      color: #f6f6f6;
      background-color: #2f2f2f;
    }

    .subtitle {
      color: #aaa;
    }

    .error {
      background: #3a2020;
    }

    .activity-panel {
      border-top-color: #444;
      background: #333;
    }

    .activity-handle {
      color: #999;
    }

    .activity-handle:hover {
      color: #ccc;
      background: #3a3a3a;
    }

    .settings-btn {
      border-color: #555;
      color: #999;
    }

    .settings-btn:hover {
      color: #8ab4f8;
      border-color: #8ab4f8;
    }

    .setup-section {
      border-color: #444;
    }

    .setup-handle {
      background: #333;
      color: #999;
    }

    .setup-handle:hover {
      color: #ccc;
      background: #3a3a3a;
    }

    .setup-summary {
      color: #aaa;
    }
  }
</style>
