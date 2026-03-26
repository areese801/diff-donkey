<script lang="ts">
  import type { ColumnInfo } from "$lib/types/diff";

  interface Props {
    columns: ColumnInfo[];
    onRunDiff: (pkColumn: string) => void;
    isLoading: boolean;
  }

  let { columns, onRunDiff, isLoading }: Props = $props();

  let selectedPk = $state("");
</script>

<div class="diff-config">
  <label for="pk-select">Primary Key Column:</label>
  <select id="pk-select" bind:value={selectedPk} disabled={isLoading}>
    <option value="" disabled>Select a column...</option>
    {#each columns as col}
      <option value={col.name}>{col.name} ({col.data_type})</option>
    {/each}
  </select>

  <button
    onclick={() => onRunDiff(selectedPk)}
    disabled={!selectedPk || isLoading}
  >
    {isLoading ? "Running..." : "Run Diff"}
  </button>
</div>

<style>
  .diff-config {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 12px 16px;
    background: #f0f0f0;
    border-radius: 8px;
    margin-bottom: 16px;
  }

  label {
    font-weight: 500;
    font-size: 0.9em;
    white-space: nowrap;
  }

  select {
    padding: 6px 10px;
    border-radius: 4px;
    border: 1px solid #ccc;
    font-size: 0.9em;
    background: white;
    color: inherit;
  }

  button {
    padding: 8px 20px;
    border-radius: 6px;
    border: none;
    background: #396cd8;
    color: white;
    font-weight: 600;
    cursor: pointer;
    font-size: 0.9em;
  }

  button:hover:not(:disabled) {
    background: #2d5bbf;
  }

  button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  @media (prefers-color-scheme: dark) {
    .diff-config {
      background: #383838;
    }

    select {
      background: #2f2f2f;
      border-color: #555;
    }
  }
</style>
