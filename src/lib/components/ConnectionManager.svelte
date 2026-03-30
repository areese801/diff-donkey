<script lang="ts">
  import { savedConnections, loadConnections } from "$lib/stores/connections";
  import { deleteConnection, testConnection } from "$lib/tauri";
  import ConnectionForm from "$lib/components/ConnectionForm.svelte";
  import type { SavedConnection } from "$lib/types/connections";

  interface Props {
    onClose: () => void;
  }

  let { onClose }: Props = $props();

  let editingConnection: SavedConnection | null = $state(null);
  let showForm = $state(false);
  let confirmDeleteId: string | null = $state(null);
  let testingId: string | null = $state(null);
  let testResults: Record<string, { success: boolean; message: string }> = $state({});

  function handleNew() {
    editingConnection = null;
    showForm = true;
  }

  function handleEdit(conn: SavedConnection) {
    editingConnection = conn;
    showForm = true;
  }

  function handleFormClose() {
    showForm = false;
    editingConnection = null;
  }

  async function handleDelete(id: string) {
    try {
      await deleteConnection(id);
      await loadConnections();
      confirmDeleteId = null;
    } catch (e) {
      console.error("Failed to delete connection:", e);
    }
  }

  async function handleTest(conn: SavedConnection) {
    testingId = conn.id;
    try {
      const msg = await testConnection(conn, null);
      testResults = { ...testResults, [conn.id]: { success: true, message: msg } };
    } catch (e) {
      testResults = {
        ...testResults,
        [conn.id]: {
          success: false,
          message: e instanceof Error ? e.message : String(e),
        },
      };
    } finally {
      testingId = null;
    }
  }

  function handleDuplicate(conn: SavedConnection) {
    editingConnection = {
      ...conn,
      id: crypto.randomUUID(),
      name: `${conn.name} (copy)`,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    showForm = true;
  }

  function dbTypeLabel(type: string): string {
    switch (type) {
      case "postgres":
        return "PostgreSQL";
      case "mysql":
        return "MySQL";
      case "snowflake":
        return "Snowflake";
      default:
        return type;
    }
  }
</script>

<div class="manager-overlay" onclick={onClose} role="presentation">
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="manager-panel" onclick={(e) => e.stopPropagation()}>
    {#if showForm}
      <ConnectionForm connection={editingConnection} onClose={handleFormClose} />
    {:else}
      <div class="manager-header">
        <h3>Manage Connections</h3>
        <button class="close-btn" onclick={onClose}>&#x2715;</button>
      </div>

      <button class="btn-new" onclick={handleNew}>+ New Connection</button>

      {#if $savedConnections.length === 0}
        <p class="empty">No saved connections yet. Create one to get started.</p>
      {:else}
        <div class="connections-list">
          {#each $savedConnections as conn (conn.id)}
            <div class="connection-entry">
              <div class="conn-info">
                {#if conn.color}
                  <span class="color-dot" style="background: {conn.color}"></span>
                {/if}
                <div class="conn-details">
                  <span class="conn-name">{conn.name}</span>
                  <span class="conn-meta">
                    {dbTypeLabel(conn.db_type)}
                    {#if conn.host} &middot; {conn.host}{/if}
                    {#if conn.database} &middot; {conn.database}{/if}
                  </span>
                </div>
              </div>

              <div class="conn-actions">
                <button class="action-btn" onclick={() => handleEdit(conn)} title="Edit">Edit</button>
                <button
                  class="action-btn"
                  onclick={() => handleTest(conn)}
                  disabled={testingId === conn.id}
                  title="Test"
                >{testingId === conn.id ? "..." : "Test"}</button>
                <button class="action-btn" onclick={() => handleDuplicate(conn)} title="Duplicate">Dup</button>
                {#if confirmDeleteId === conn.id}
                  <button class="action-btn danger" onclick={() => handleDelete(conn.id)}>Confirm</button>
                  <button class="action-btn" onclick={() => (confirmDeleteId = null)}>Cancel</button>
                {:else}
                  <button class="action-btn danger" onclick={() => (confirmDeleteId = conn.id)} title="Delete">Del</button>
                {/if}
              </div>

              {#if testResults[conn.id]}
                <div
                  class="test-result"
                  class:success={testResults[conn.id].success}
                  class:failure={!testResults[conn.id].success}
                >
                  {testResults[conn.id].success ? "Connected" : testResults[conn.id].message}
                </div>
              {/if}
            </div>
          {/each}
        </div>
      {/if}
    {/if}
  </div>
</div>

<style>
  .manager-overlay {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.4);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 100;
  }

  .manager-panel {
    background: white;
    border-radius: 12px;
    max-width: 560px;
    width: 90%;
    max-height: 80vh;
    overflow-y: auto;
    padding: 20px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
  }

  .manager-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
  }

  .manager-header h3 {
    margin: 0;
    font-size: 1.1em;
  }

  .close-btn {
    background: none;
    border: none;
    font-size: 1.2em;
    cursor: pointer;
    color: inherit;
    padding: 4px 8px;
  }

  .btn-new {
    width: 100%;
    padding: 10px;
    border-radius: 6px;
    border: 2px dashed #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.9em;
    color: inherit;
    margin-bottom: 12px;
  }

  .btn-new:hover {
    border-color: #396cd8;
    color: #396cd8;
  }

  .empty {
    text-align: center;
    color: #888;
    font-size: 0.9em;
    padding: 20px;
  }

  .connections-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .connection-entry {
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 10px 12px;
  }

  .conn-info {
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .color-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
  }

  .conn-details {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-width: 0;
  }

  .conn-name {
    font-weight: 600;
    font-size: 0.95em;
  }

  .conn-meta {
    font-size: 0.8em;
    color: #888;
  }

  .conn-actions {
    display: flex;
    gap: 4px;
    margin-top: 8px;
  }

  .action-btn {
    padding: 4px 10px;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.8em;
    color: inherit;
  }

  .action-btn:hover:not(:disabled) {
    background: rgba(57, 108, 216, 0.1);
  }

  .action-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .action-btn.danger {
    color: #e74c3c;
    border-color: #e74c3c;
  }

  .action-btn.danger:hover {
    background: rgba(231, 76, 60, 0.1);
  }

  .test-result {
    margin-top: 6px;
    padding: 4px 8px;
    border-radius: 4px;
    font-size: 0.8em;
  }

  .test-result.success {
    background: rgba(46, 204, 113, 0.15);
    color: #27ae60;
  }

  .test-result.failure {
    background: rgba(231, 76, 60, 0.1);
    color: #e74c3c;
  }

  @media (prefers-color-scheme: dark) {
    .manager-panel {
      background: #2a2a2a;
    }

    .connection-entry {
      border-color: #444;
    }

    .btn-new {
      border-color: #555;
    }

    .btn-new:hover {
      border-color: #24c8db;
      color: #24c8db;
    }

    .action-btn {
      border-color: #555;
    }
  }
</style>
