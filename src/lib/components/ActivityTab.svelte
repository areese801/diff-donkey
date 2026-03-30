<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import { getActivityLog, clearActivityLog } from "$lib/tauri";
  import type { QueryLogEntry } from "$lib/types/diff";

  let entries: QueryLogEntry[] = $state([]);
  let expandedIndex: number | null = $state(null);
  let intervalId: ReturnType<typeof setInterval> | null = null;

  async function refresh() {
    try {
      entries = await getActivityLog();
    } catch (e) {
      console.error("Failed to fetch activity log:", e);
    }
  }

  async function handleClear() {
    await clearActivityLog();
    entries = [];
  }

  onMount(() => {
    refresh();
    intervalId = setInterval(refresh, 2000);
  });

  onDestroy(() => {
    if (intervalId) clearInterval(intervalId);
  });

  function toggleExpand(i: number) {
    expandedIndex = expandedIndex === i ? null : i;
  }

  function formatTime(epochMs: number): string {
    const d = new Date(epochMs);
    const h = d.getHours().toString().padStart(2, "0");
    const m = d.getMinutes().toString().padStart(2, "0");
    const s = d.getSeconds().toString().padStart(2, "0");
    const ms = d.getMilliseconds().toString().padStart(3, "0");
    return `${h}:${m}:${s}.${ms}`;
  }

  function durationClass(ms: number): string {
    if (ms < 100) return "badge-green";
    if (ms < 1000) return "badge-yellow";
    return "badge-red";
  }

  function truncateSql(sql: string, max: number = 80): string {
    const oneLine = sql.replace(/\s+/g, " ").trim();
    if (oneLine.length <= max) return oneLine;
    return oneLine.slice(0, max) + "...";
  }

  let sortOrder: "newest" | "oldest" = $state("newest");
  let sortedEntries = $derived(
    sortOrder === "newest" ? [...entries].reverse() : [...entries]
  );
</script>

<div class="activity">
  <div class="toolbar">
    <span class="count">{entries.length} {entries.length === 1 ? "query" : "queries"}</span>
    <div class="toolbar-actions">
      <button class="btn" onclick={() => sortOrder = sortOrder === "newest" ? "oldest" : "newest"}>
        {sortOrder === "newest" ? "↓ Newest" : "↑ Oldest"}
      </button>
      <button class="btn btn-clear" onclick={handleClear} disabled={entries.length === 0}>Clear Log</button>
    </div>
  </div>

  {#if sortedEntries.length === 0}
    <p class="empty">No queries logged yet. Load data or run a diff to see activity.</p>
  {:else}
    <div class="log-list">
      {#each sortedEntries as entry, i}
        <div class="log-entry" class:has-error={!!entry.error}>
          <div class="entry-header" onclick={() => toggleExpand(i)} role="button" tabindex="0" onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') toggleExpand(i); }}>
            <span class="timestamp">{formatTime(entry.timestamp)}</span>
            <span class="badge badge-op">{entry.operation}</span>
            <span class="badge {durationClass(entry.duration_ms)}">{entry.duration_ms}ms</span>
            {#if entry.rows_affected !== null}
              <span class="rows">{entry.rows_affected} rows</span>
            {/if}
            {#if entry.error}
              <span class="badge badge-red">ERROR</span>
            {/if}
            <span class="sql-preview">{truncateSql(entry.sql)}</span>
          </div>

          {#if expandedIndex === i}
            <div class="entry-detail">
              {#if entry.error}
                <div class="error-msg">{entry.error}</div>
              {/if}
              <pre><code>{entry.sql}</code></pre>
            </div>
          {/if}
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .activity {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }

  .toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .toolbar-actions {
    display: flex;
    gap: 8px;
  }

  .count {
    font-size: 0.9em;
    color: #666;
  }

  .btn {
    padding: 6px 14px;
    border: 1px solid #ddd;
    border-radius: 6px;
    background: #fff;
    cursor: pointer;
    font-size: 0.85em;
    transition: background 0.15s;
  }

  .btn:hover {
    background: #f0f0f0;
  }

  .btn:disabled {
    opacity: 0.5;
    cursor: default;
  }

  .btn-clear {
    color: #e74c3c;
    border-color: #e74c3c;
  }

  .btn-clear:hover:not(:disabled) {
    background: #ffeaea;
  }

  .empty {
    color: #999;
    text-align: center;
    padding: 32px;
  }

  .log-list {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .log-entry {
    border: 1px solid #e0e0e0;
    border-radius: 6px;
    overflow: hidden;
  }

  .log-entry.has-error {
    border-color: #e74c3c;
  }

  .entry-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    cursor: pointer;
    font-size: 0.88em;
    overflow: hidden;
  }

  .entry-header:hover {
    background: #f8f8f8;
  }

  .timestamp {
    font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
    color: #888;
    white-space: nowrap;
    font-size: 0.9em;
  }

  .badge {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.8em;
    font-weight: 600;
    white-space: nowrap;
  }

  .badge-op {
    background: #e8eaf6;
    color: #3949ab;
  }

  .badge-green {
    background: #e8f5e9;
    color: #2e7d32;
  }

  .badge-yellow {
    background: #fff8e1;
    color: #f57f17;
  }

  .badge-red {
    background: #ffeaea;
    color: #c62828;
  }

  .rows {
    color: #888;
    font-size: 0.85em;
    white-space: nowrap;
  }

  .sql-preview {
    font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
    color: #666;
    font-size: 0.85em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex: 1;
    min-width: 0;
  }

  .entry-detail {
    padding: 8px 12px;
    border-top: 1px solid #e0e0e0;
    background: #fafafa;
  }

  .entry-detail pre {
    margin: 0;
    padding: 8px;
    background: #f5f5f5;
    border-radius: 4px;
    overflow-x: auto;
    font-size: 0.85em;
    line-height: 1.5;
  }

  .entry-detail code {
    font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
  }

  .error-msg {
    color: #e74c3c;
    font-size: 0.88em;
    padding: 4px 0 8px;
  }

  @media (prefers-color-scheme: dark) {
    .count {
      color: #aaa;
    }

    .btn {
      background: #3a3a3a;
      border-color: #555;
      color: #ddd;
    }

    .btn:hover {
      background: #4a4a4a;
    }

    .btn-clear {
      color: #ff6b6b;
      border-color: #ff6b6b;
    }

    .btn-clear:hover:not(:disabled) {
      background: #4a2020;
    }

    .log-entry {
      border-color: #444;
    }

    .log-entry.has-error {
      border-color: #ff6b6b;
    }

    .entry-header:hover {
      background: #383838;
    }

    .timestamp {
      color: #888;
    }

    .badge-op {
      background: #2a2d4a;
      color: #8c9eff;
    }

    .badge-green {
      background: #1b3a1b;
      color: #66bb6a;
    }

    .badge-yellow {
      background: #3a3018;
      color: #ffa726;
    }

    .badge-red {
      background: #3a1818;
      color: #ff6b6b;
    }

    .rows {
      color: #888;
    }

    .sql-preview {
      color: #aaa;
    }

    .entry-detail {
      border-top-color: #444;
      background: #333;
    }

    .entry-detail pre {
      background: #2a2a2a;
    }

    .error-msg {
      color: #ff6b6b;
    }

    .empty {
      color: #777;
    }
  }
</style>
