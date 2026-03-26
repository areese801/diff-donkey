/**
 * Svelte stores for source configuration.
 *
 * Stores in Svelte are like reactive variables — when you update a store,
 * any component that reads it automatically re-renders. Similar concept
 * to React's useState but shared across components.
 *
 * writable() creates a store you can both read and write to.
 */
import { writable } from "svelte/store";
import type { TableMeta } from "../types/diff";

/** Metadata for source A (left side of diff) */
export const sourceA = writable<TableMeta | null>(null);

/** Metadata for source B (right side of diff) */
export const sourceB = writable<TableMeta | null>(null);
