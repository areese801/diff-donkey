import { writable } from "svelte/store";
import type { OverviewResult } from "../types/diff";

/** The current diff result (null before diff is run) */
export const diffResult = writable<OverviewResult | null>(null);

/** Whether a diff operation is currently running */
export const isLoading = writable(false);

/** The primary key column used for the current diff */
export const pkColumn = writable<string | null>(null);

/** The precision (decimal places) used for the current diff */
export const diffPrecision = writable<number | null>(null);

/** Columns ignored in the current diff */
export const ignoredColumns = writable<string[]>([]);
