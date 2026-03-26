<script lang="ts">
  import DataTable from "./DataTable.svelte";
  import type { PagedRows, PkSummary } from "$lib/types/diff";
  import { getExclusiveRows, getDuplicatePks } from "$lib/tauri";

  interface Props {
    pkSummary: PkSummary | null;
  }

  let { pkSummary }: Props = $props();

  let subTab = $state<"exclusive_a" | "exclusive_b" | "duplicates_a" | "duplicates_b">("exclusive_a");

  let data: PagedRows | null = $state(null);
  let loading = $state(false);
  const PAGE_SIZE = 50;

  /** Fetch data when sub-tab changes */
  $effect(() => {
    if (pkSummary) {
      fetchData(subTab, 0);
    }
  });

  async function fetchData(tab: string, page: number) {
    loading = true;
    try {
      if (tab === "exclusive_a") {
        data = await getExclusiveRows("a", page, PAGE_SIZE);
      } else if (tab === "exclusive_b") {
        data = await getExclusiveRows("b", page, PAGE_SIZE);
      } else if (tab === "duplicates_a") {
        data = await getDuplicatePks("a", page, PAGE_SIZE);
      } else if (tab === "duplicates_b") {
        data = await getDuplicatePks("b", page, PAGE_SIZE);
      }
    } catch (e) {
      console.error("PK tab fetch error:", e);
      data = null;
    } finally {
      loading = false;
    }
  }
</script>

{#if !pkSummary}
  <p class="empty">Run a diff to see primary key analysis.</p>
{:else}
  <div class="pk-tab">
    <nav class="sub-tabs">
      <button
        class:active={subTab === "exclusive_a"}
        onclick={() => subTab = "exclusive_a"}
      >
        Only in A ({pkSummary.exclusive_a})
      </button>
      <button
        class:active={subTab === "exclusive_b"}
        onclick={() => subTab = "exclusive_b"}
      >
        Only in B ({pkSummary.exclusive_b})
      </button>
      <button
        class:active={subTab === "duplicates_a"}
        onclick={() => subTab = "duplicates_a"}
      >
        Duplicate PKs A ({pkSummary.duplicate_pks_a})
      </button>
      <button
        class:active={subTab === "duplicates_b"}
        onclick={() => subTab = "duplicates_b"}
      >
        Duplicate PKs B ({pkSummary.duplicate_pks_b})
      </button>
    </nav>

    <DataTable
      {data}
      {loading}
      onPageChange={(page) => fetchData(subTab, page)}
    />
  </div>
{/if}

<style>
  .empty {
    color: #888;
    text-align: center;
    padding: 40px;
  }

  .sub-tabs {
    display: flex;
    gap: 4px;
    margin-bottom: 16px;
  }

  .sub-tabs button {
    padding: 6px 14px;
    border: 1px solid #ddd;
    border-radius: 16px;
    background: transparent;
    cursor: pointer;
    font-size: 0.85em;
    color: #666;
  }

  .sub-tabs button.active {
    background: #396cd8;
    color: white;
    border-color: #396cd8;
  }

  @media (prefers-color-scheme: dark) {
    .sub-tabs button {
      border-color: #555;
      color: #999;
    }

    .sub-tabs button.active {
      background: #24c8db;
      color: #1a1a1a;
      border-color: #24c8db;
    }
  }
</style>
