<script lang="ts">
  interface Props {
    activeTab: string;
    onTabChange: (tab: string) => void;
  }

  let { activeTab, onTabChange }: Props = $props();

  const tabs = [
    { id: "overview", label: "Summary" },
    { id: "columns", label: "Columns" },
    { id: "primary-keys", label: "Primary Keys" },
    { id: "values", label: "Values" },
  ];
</script>

<nav class="tab-nav">
  {#each tabs as tab}
    <button
      class="tab"
      class:active={activeTab === tab.id}
      onclick={() => onTabChange(tab.id)}
    >
      {tab.label}
    </button>
  {/each}
</nav>

<style>
  .tab-nav {
    display: flex;
    border-bottom: 2px solid #e0e0e0;
    gap: 0;
    margin-bottom: 16px;
  }

  .tab {
    padding: 10px 20px;
    border: none;
    background: transparent;
    cursor: pointer;
    font-size: 0.95em;
    font-weight: 500;
    color: #666;
    border-bottom: 2px solid transparent;
    margin-bottom: -2px;
    transition: color 0.15s, border-color 0.15s;
  }

  .tab:hover {
    color: #333;
  }

  .tab.active {
    color: #396cd8;
    border-bottom-color: #396cd8;
  }

  @media (prefers-color-scheme: dark) {
    .tab-nav {
      border-bottom-color: #444;
    }

    .tab {
      color: #999;
    }

    .tab:hover {
      color: #ccc;
    }

    .tab.active {
      color: #24c8db;
      border-bottom-color: #24c8db;
    }
  }
</style>
