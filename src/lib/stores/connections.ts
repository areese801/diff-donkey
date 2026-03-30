/**
 * Svelte store for saved database connections.
 *
 * Loads the connection list from the backend on app start and keeps it
 * in sync as connections are added, edited, or deleted.
 */
import { writable } from "svelte/store";
import type { SavedConnection } from "../types/connections";
import { listSavedConnections } from "../tauri";

/** The list of saved connections */
export const savedConnections = writable<SavedConnection[]>([]);

/** Load saved connections from the backend */
export async function loadConnections(): Promise<void> {
  try {
    const connections = await listSavedConnections();
    savedConnections.set(connections);
  } catch (e) {
    console.error("Failed to load saved connections:", e);
  }
}
