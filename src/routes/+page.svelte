<script lang="ts">
  import SourceSelector from "$lib/components/SourceSelector.svelte";
  import TabNav from "$lib/components/TabNav.svelte";
  import DiffConfig from "$lib/components/DiffConfig.svelte";
  import ColumnsTab from "$lib/components/ColumnsTab.svelte";
  import OverviewTab from "$lib/components/OverviewTab.svelte";
  import PrimaryKeysTab from "$lib/components/PrimaryKeysTab.svelte";
  import ValuesTab from "$lib/components/ValuesTab.svelte";
  import ActivityTab from "$lib/components/ActivityTab.svelte";
  import { sourceA, sourceB } from "$lib/stores/config";
  import { diffResult, isLoading, pkColumn, diffPrecision } from "$lib/stores/diff";
  import { getSchemaComparison, runDiff } from "$lib/tauri";
  import type { SchemaComparison } from "$lib/types/diff";

  let activeTab = $state("columns");
  let schemaComparison: SchemaComparison | null = $state(null);
  let diffError: string | null = $state(null);
  let activityOpen = $state(false);

  let bothLoaded = $derived(!!$sourceA && !!$sourceB);
  let sharedColumns = $derived(schemaComparison?.shared.map(c => ({
    name: c.name,
    data_type: c.type_a,
  })) ?? []);

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
  ) {
    isLoading.set(true);
    diffError = null;
    pkColumn.set(selectedPks.join(", "));

    try {
      const isFirstRun = !$diffResult;
      const result = await runDiff({
        pk_columns: selectedPks,
        tolerance,
        column_tolerances: columnTolerances,
      });
      diffResult.set(result);
      diffPrecision.set(tolerance);
      if (isFirstRun) activeTab = "overview";
    } catch (e) {
      diffError = e instanceof Error ? e.message : String(e);
    } finally {
      isLoading.set(false);
    }
  }
</script>

<div class="app-layout">
  <main class="container">
    <h1>Diff Donkey</h1>
    <p class="subtitle">Dataset comparison powered by DuckDB</p>

    <SourceSelector />

    {#if bothLoaded}
      <DiffConfig
        columns={sharedColumns}
        onRunDiff={handleRunDiff}
        isLoading={$isLoading}
      />

      <TabNav {activeTab} onTabChange={(tab) => activeTab = tab} />

      {#if diffError}
        <p class="error">{diffError}</p>
      {/if}

      {#if activeTab === "overview"}
        <OverviewTab result={$diffResult} />
      {:else if activeTab === "columns"}
        <ColumnsTab comparison={schemaComparison} />
      {:else if activeTab === "primary-keys"}
        <PrimaryKeysTab pkSummary={$diffResult?.pk_summary ?? null} />
      {:else if activeTab === "values"}
        <ValuesTab columnStats={$diffResult?.diff_stats.columns ?? []} valuesSummary={$diffResult?.values_summary} precision={$diffPrecision} />
      {/if}
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
    max-width: 1100px;
    margin: 0 auto;
    padding: 24px;
    flex: 1;
  }

  h1 {
    margin: 0;
    font-size: 1.8em;
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
  }
</style>
