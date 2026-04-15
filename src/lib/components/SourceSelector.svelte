<script lang="ts">
  import { open } from "@tauri-apps/plugin-dialog";
  import { loadSource, loadRemoteSource, listRemoteProfiles, saveRemoteProfile, deleteRemoteProfile, getRemoteProfileSecrets } from "$lib/tauri";
  import { sourceA, sourceB } from "$lib/stores/config";
  import DatabaseSource from "$lib/components/DatabaseSource.svelte";
  import ConnectionManager from "$lib/components/ConnectionManager.svelte";
  import type { TableMeta, RemoteCredentials } from "$lib/types/diff";
  import type { SavedRemoteProfile, RemoteSecrets } from "$lib/types/connections";

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
  let showConnectionManager = $state(false);

  /** Remote source state */
  let remoteUriA = $state(localStorage.getItem("diff-donkey:remoteUriA") ?? "");
  let remoteUriB = $state(localStorage.getItem("diff-donkey:remoteUriB") ?? "");
  let accessKeyA = $state("");
  let accessKeyB = $state("");
  let secretKeyA = $state("");
  let secretKeyB = $state("");
  let regionA = $state("us-east-1");
  let regionB = $state("us-east-1");
  let endpointA = $state("");
  let endpointB = $state("");
  let sessionTokenA = $state("");
  let sessionTokenB = $state("");
  let urlStyleA = $state("");
  let urlStyleB = $state("");
  let useSslA = $state<boolean | null>(null);
  let useSslB = $state<boolean | null>(null);
  let bearerTokenA = $state("");
  let bearerTokenB = $state("");
  let showCredsA = $state(false);
  let showCredsB = $state(false);

  /** Remote profile state */
  let remoteProfiles: SavedRemoteProfile[] = $state([]);
  let selectedProfileA = $state(localStorage.getItem("diff-donkey:remoteProfileA") ?? "");
  let selectedProfileB = $state(localStorage.getItem("diff-donkey:remoteProfileB") ?? "");
  let profileNameA = $state("");
  let profileNameB = $state("");
  let savingA = $state(false);
  let savingB = $state(false);

  async function refreshProfiles() {
    try { remoteProfiles = await listRemoteProfiles(); } catch { remoteProfiles = []; }
  }

  async function selectProfile(label: "a" | "b", profileId: string) {
    if (label === "a") {
      selectedProfileA = profileId;
      if (profileId) localStorage.setItem("diff-donkey:remoteProfileA", profileId);
      else localStorage.removeItem("diff-donkey:remoteProfileA");
    } else {
      selectedProfileB = profileId;
      if (profileId) localStorage.setItem("diff-donkey:remoteProfileB", profileId);
      else localStorage.removeItem("diff-donkey:remoteProfileB");
    }

    if (!profileId) return;
    const profile = remoteProfiles.find(p => p.id === profileId);
    if (!profile) return;

    try {
      const secrets = await getRemoteProfileSecrets(profileId);
      if (label === "a") {
        regionA = profile.region ?? "";
        endpointA = profile.endpoint ?? "";
        urlStyleA = profile.url_style ?? "";
        useSslA = profile.use_ssl ?? null;
        accessKeyA = secrets.access_key ?? "";
        secretKeyA = secrets.secret_key ?? "";
        sessionTokenA = secrets.session_token ?? "";
        bearerTokenA = secrets.bearer_token ?? "";
        showCredsA = true;
      } else {
        regionB = profile.region ?? "";
        endpointB = profile.endpoint ?? "";
        urlStyleB = profile.url_style ?? "";
        useSslB = profile.use_ssl ?? null;
        accessKeyB = secrets.access_key ?? "";
        secretKeyB = secrets.secret_key ?? "";
        sessionTokenB = secrets.session_token ?? "";
        bearerTokenB = secrets.bearer_token ?? "";
        showCredsB = true;
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") errorA = msg; else errorB = msg;
    }
  }

  async function saveCurrentProfile(label: "a" | "b") {
    const existingId = label === "a" ? selectedProfileA : selectedProfileB;
    const existing = remoteProfiles.find(p => p.id === existingId);
    const name = existing?.name ?? (label === "a" ? profileNameA : profileNameB).trim();
    if (!name) {
      if (label === "a") errorA = "Enter a profile name before saving";
      else errorB = "Enter a profile name before saving";
      return;
    }

    if (label === "a") savingA = true; else savingB = true;

    const now = new Date().toISOString();
    const profile: SavedRemoteProfile = {
      id: existing?.id ?? crypto.randomUUID(),
      name,
      provider: getProvider(label === "a" ? remoteUriA : remoteUriB),
      region: (label === "a" ? regionA : regionB) || null,
      endpoint: (label === "a" ? endpointA : endpointB) || null,
      url_style: (label === "a" ? urlStyleA : urlStyleB) || null,
      use_ssl: label === "a" ? useSslA : useSslB,
      created_at: existing?.created_at ?? now,
      updated_at: now,
    };
    const secrets: RemoteSecrets = {
      access_key: (label === "a" ? accessKeyA : accessKeyB) || null,
      secret_key: (label === "a" ? secretKeyA : secretKeyB) || null,
      session_token: (label === "a" ? sessionTokenA : sessionTokenB) || null,
      bearer_token: (label === "a" ? bearerTokenA : bearerTokenB) || null,
    };

    try {
      await saveRemoteProfile(profile, secrets);
      await refreshProfiles();
      if (label === "a") { selectedProfileA = profile.id; profileNameA = ""; errorA = null; localStorage.setItem("diff-donkey:remoteProfileA", profile.id); }
      else { selectedProfileB = profile.id; profileNameB = ""; errorB = null; localStorage.setItem("diff-donkey:remoteProfileB", profile.id); }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") errorA = msg; else errorB = msg;
    } finally {
      if (label === "a") savingA = false; else savingB = false;
    }
  }

  async function deleteCurrentProfile(label: "a" | "b") {
    const id = label === "a" ? selectedProfileA : selectedProfileB;
    if (!id) return;
    try {
      await deleteRemoteProfile(id);
      if (label === "a") { selectedProfileA = ""; localStorage.removeItem("diff-donkey:remoteProfileA"); }
      else { selectedProfileB = ""; localStorage.removeItem("diff-donkey:remoteProfileB"); }
      await refreshProfiles();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") errorA = msg; else errorB = msg;
    }
  }

  // Load profiles on mount and restore saved selections
  $effect(() => {
    refreshProfiles().then(() => {
      if (selectedProfileA) selectProfile("a", selectedProfileA);
      if (selectedProfileB) selectProfile("b", selectedProfileB);
    });
  });

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
      session_token: (label === "a" ? sessionTokenA : sessionTokenB) || null,
      url_style: (label === "a" ? urlStyleA : urlStyleB) || null,
      use_ssl: label === "a" ? useSslA : useSslB,
      bearer_token: (label === "a" ? bearerTokenA : bearerTokenB) || null,
    };

    try {
      const meta = await loadRemoteSource(uri, label, credentials);
      if (label === "a") {
        metaA = meta;
        sourceA.set(meta);
        localStorage.setItem("diff-donkey:remoteUriA", uri);
      } else {
        metaB = meta;
        sourceB.set(meta);
        localStorage.setItem("diff-donkey:remoteUriB", uri);
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (label === "a") {
        errorA = msg;
        remoteUriA = "";
        localStorage.removeItem("diff-donkey:remoteUriA");
      } else {
        errorB = msg;
        remoteUriB = "";
        localStorage.removeItem("diff-donkey:remoteUriB");
      }
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
  {#if showConnectionManager}
    <ConnectionManager onClose={() => (showConnectionManager = false)} />
  {/if}

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
      <input class="remote-uri" type="text" bind:value={remoteUriA} placeholder="s3://bucket/path/file.csv or https://..." />
      <button class="load-btn" onclick={() => loadRemote("a")} disabled={!remoteUriA.trim() || loadingA}>{loadingA ? "..." : "Load"}</button>
      <button class="creds-toggle" class:open={showCredsA} onclick={() => showCredsA = !showCredsA} title="Credentials">{showCredsA ? "▾" : "▸"} Auth</button>
      {#if metaA && modeA === "remote"}
        <span class="meta-summary"><strong>{metaA.row_count.toLocaleString()}</strong> rows &middot; {metaA.columns.length} cols</span>
      {/if}
      {#if errorA}<span class="error">{errorA}</span>{/if}
      {#if showCredsA}
        <div class="creds-profile-bar">
          <select class="profile-select" value={selectedProfileA} onchange={(e) => selectProfile("a", (e.target as HTMLSelectElement).value)}>
            <option value="">-- No saved profile --</option>
            {#each remoteProfiles as p}
              <option value={p.id}>{p.name}</option>
            {/each}
          </select>
          {#if !selectedProfileA}
            <input class="profile-name-input" type="text" bind:value={profileNameA} placeholder="Profile name" />
          {/if}
          <button class="profile-btn" onclick={() => saveCurrentProfile("a")} disabled={savingA}>{savingA ? "..." : selectedProfileA ? "Update" : "Save"}</button>
          {#if selectedProfileA}
            <button class="profile-btn delete" onclick={() => deleteCurrentProfile("a")}>Delete</button>
          {/if}
        </div>
        <div class="creds-grid">
          <label>Access Key <input type="text" bind:value={accessKeyA} placeholder="AKIAIOSFODNN7EXAMPLE" /></label>
          <label>Secret Key <input type="password" bind:value={secretKeyA} placeholder="wJalrXUtnFEMI/K7MDENG/..." /></label>
          <label>Region <input type="text" bind:value={regionA} placeholder="us-east-1" /></label>
          <label>Endpoint <input type="text" bind:value={endpointA} placeholder="127.0.0.1:9000" /></label>
          <label>Session Token <input type="text" bind:value={sessionTokenA} placeholder="Optional STS token" /></label>
          <label>URL Style
            <select bind:value={urlStyleA}>
              <option value="">Default</option>
              <option value="path">Path</option>
              <option value="vhost">Virtual Host</option>
            </select>
          </label>
          <label>Use SSL
            <select value={useSslA === null ? "" : useSslA ? "true" : "false"} onchange={(e) => { const v = (e.target as HTMLSelectElement).value; useSslA = v === "" ? null : v === "true"; }}>
              <option value="">Default</option>
              <option value="true">Yes</option>
              <option value="false">No</option>
            </select>
          </label>
          <label>Bearer Token <input type="password" bind:value={bearerTokenA} placeholder="For private HTTP endpoints" /></label>
        </div>
      {/if}
    {:else}
      <DatabaseSource label="a" onLoaded={(meta) => handleDbLoaded("a", meta)} />
      <button class="manage-btn" onclick={() => (showConnectionManager = true)}>Connections</button>
    {/if}
  </div>

  <!-- Source B row -->
  <div class="source-row">
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
      <input class="remote-uri" type="text" bind:value={remoteUriB} placeholder="s3://bucket/path/file.csv or https://..." />
      <button class="load-btn" onclick={() => loadRemote("b")} disabled={!remoteUriB.trim() || loadingB}>{loadingB ? "..." : "Load"}</button>
      <button class="creds-toggle" class:open={showCredsB} onclick={() => showCredsB = !showCredsB} title="Credentials">{showCredsB ? "▾" : "▸"} Auth</button>
      {#if metaB && modeB === "remote"}
        <span class="meta-summary"><strong>{metaB.row_count.toLocaleString()}</strong> rows &middot; {metaB.columns.length} cols</span>
      {/if}
      {#if errorB}<span class="error">{errorB}</span>{/if}
      {#if showCredsB}
        <div class="creds-profile-bar">
          <select class="profile-select" value={selectedProfileB} onchange={(e) => selectProfile("b", (e.target as HTMLSelectElement).value)}>
            <option value="">-- No saved profile --</option>
            {#each remoteProfiles as p}
              <option value={p.id}>{p.name}</option>
            {/each}
          </select>
          {#if !selectedProfileB}
            <input class="profile-name-input" type="text" bind:value={profileNameB} placeholder="Profile name" />
          {/if}
          <button class="profile-btn" onclick={() => saveCurrentProfile("b")} disabled={savingB}>{savingB ? "..." : selectedProfileB ? "Update" : "Save"}</button>
          {#if selectedProfileB}
            <button class="profile-btn delete" onclick={() => deleteCurrentProfile("b")}>Delete</button>
          {/if}
        </div>
        <div class="creds-grid">
          <label>Access Key <input type="text" bind:value={accessKeyB} placeholder="AKIAIOSFODNN7EXAMPLE" /></label>
          <label>Secret Key <input type="password" bind:value={secretKeyB} placeholder="wJalrXUtnFEMI/K7MDENG/..." /></label>
          <label>Region <input type="text" bind:value={regionB} placeholder="us-east-1" /></label>
          <label>Endpoint <input type="text" bind:value={endpointB} placeholder="127.0.0.1:9000" /></label>
          <label>Session Token <input type="text" bind:value={sessionTokenB} placeholder="Optional STS token" /></label>
          <label>URL Style
            <select bind:value={urlStyleB}>
              <option value="">Default</option>
              <option value="path">Path</option>
              <option value="vhost">Virtual Host</option>
            </select>
          </label>
          <label>Use SSL
            <select value={useSslB === null ? "" : useSslB ? "true" : "false"} onchange={(e) => { const v = (e.target as HTMLSelectElement).value; useSslB = v === "" ? null : v === "true"; }}>
              <option value="">Default</option>
              <option value="true">Yes</option>
              <option value="false">No</option>
            </select>
          </label>
          <label>Bearer Token <input type="password" bind:value={bearerTokenB} placeholder="For private HTTP endpoints" /></label>
        </div>
      {/if}
    {:else}
      <DatabaseSource label="b" onLoaded={(meta) => handleDbLoaded("b", meta)} />
    {/if}
  </div>
</div>

<style>
  .source-selector {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    column-gap: 24px;
    row-gap: 8px;
  }

  .source-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 0;
    flex-wrap: wrap;
    flex: 0 1 auto;
  }

  .source-label {
    font-weight: 700;
    font-size: 0.85em;
    min-width: 60px;
    white-space: nowrap;
  }

  .manage-btn {
    padding: 4px 10px;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: transparent;
    cursor: pointer;
    font-size: 0.75em;
    color: #888;
    white-space: nowrap;
  }

  .manage-btn:hover {
    color: #396cd8;
    border-color: #396cd8;
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

  .creds-toggle {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: transparent;
    cursor: pointer;
    font-size: 0.75em;
    color: #888;
    white-space: nowrap;
  }

  .creds-toggle:hover {
    color: #396cd8;
    border-color: #396cd8;
  }

  .creds-toggle.open {
    color: #396cd8;
    border-color: #396cd8;
  }

  .creds-profile-bar {
    display: flex;
    align-items: center;
    gap: 4px;
    flex-basis: 100%;
  }

  .profile-select {
    flex: 1;
    padding: 3px 6px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 0.8em;
    color: inherit;
    background: transparent;
  }

  .profile-name-input {
    padding: 3px 6px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 0.8em;
    color: inherit;
    background: transparent;
    min-width: 120px;
  }

  .profile-btn {
    padding: 3px 8px;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: transparent;
    cursor: pointer;
    font-size: 0.75em;
    color: #888;
    white-space: nowrap;
  }

  .profile-btn:hover:not(:disabled) {
    color: #396cd8;
    border-color: #396cd8;
  }

  .profile-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .profile-btn.delete:hover {
    color: #e74c3c;
    border-color: #e74c3c;
  }

  .creds-grid {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr 1fr;
    gap: 4px 8px;
    padding: 6px 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
    background: rgba(0, 0, 0, 0.02);
    flex-basis: 100%;
  }

  .creds-grid label {
    display: flex;
    flex-direction: column;
    font-size: 0.7em;
    color: #888;
    gap: 2px;
  }

  .creds-grid input,
  .creds-grid select {
    padding: 3px 6px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 1.1em;
    color: inherit;
    background: transparent;
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

    .creds-toggle {
      border-color: #555;
    }

    .profile-select {
      border-color: #555;
    }

    .profile-name-input {
      border-color: #555;
    }

    .profile-btn {
      border-color: #555;
    }

    .creds-grid {
      border-color: #444;
      background: rgba(255, 255, 255, 0.03);
    }

    .creds-grid input,
    .creds-grid select {
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
