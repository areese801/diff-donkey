/**
 * TypeScript interfaces for saved database connections.
 * Mirrors the Rust SavedConnection struct in src-tauri/src/connections.rs.
 */

export interface SavedConnection {
  id: string;
  name: string;
  db_type: string;
  host: string | null;
  port: number | null;
  database: string | null;
  username: string | null;
  schema: string | null;
  ssl: boolean;
  color: string | null;
  // Snowflake-specific
  account_url: string | null;
  warehouse: string | null;
  role: string | null;
  auth_method: string | null;        // "password" | "keypair"
  private_key_path: string | null;   // Path to .p8/.pem file
  // SSH tunnel (Phase 3)
  ssh_enabled: boolean;
  ssh_host: string | null;
  ssh_port: number | null;
  ssh_username: string | null;
  ssh_auth_method: string | null;
  ssh_key_path: string | null;
  // Metadata
  created_at: string;
  updated_at: string;
}

export interface ImportResult {
  imported: number;
  skipped: number;
  skipped_names: string[];
}
