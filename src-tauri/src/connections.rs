/// Saved database connections — persistent storage for connection profiles.
///
/// Connections are stored as JSON in the app's data directory (platform-specific):
///   - macOS: ~/Library/Application Support/com.diff-donkey/connections.json
///   - Linux: ~/.local/share/com.diff-donkey/connections.json
///   - Windows: %APPDATA%/com.diff-donkey/connections.json
///
/// Passwords are stored separately in the OS keychain via the `keyring` crate
/// (macOS Keychain, Windows Credential Manager, Linux Secret Service).
/// This keeps credentials out of the JSON file on disk.
use std::path::{Path, PathBuf};

use crate::error::DiffDonkeyError;

/// The keyring service name used for all stored passwords.
const KEYRING_SERVICE: &str = "diff-donkey";

/// A saved database connection profile.
///
/// All fields except `id`, `name`, `db_type`, `ssl`, and `ssh_enabled` are optional
/// to support different database types and connection modes.
/// Password is NOT stored here — it lives in the OS keychain, keyed by `id`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SavedConnection {
    pub id: String,
    pub name: String,
    pub db_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub schema: Option<String>,
    pub ssl: bool,
    pub color: Option<String>,
    // Snowflake-specific
    pub account_url: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub auth_method: Option<String>,      // "password" | "keypair"
    pub private_key_path: Option<String>, // Path to .p8/.pem file
    // SSH tunnel (placeholder for Phase 3)
    pub ssh_enabled: bool,
    pub ssh_host: Option<String>,
    pub ssh_port: Option<u16>,
    pub ssh_username: Option<String>,
    pub ssh_auth_method: Option<String>,
    pub ssh_key_path: Option<String>,
    // Metadata
    pub created_at: String,
    pub updated_at: String,
}

/// Get the path to the connections.json file in the app's data directory.
///
/// Uses Tauri's `app_data_dir()` resolver to get the platform-appropriate path.
/// Like Python's `platformdirs.user_data_dir()`.
pub fn get_connections_path(app_handle: &tauri::AppHandle) -> PathBuf {
    use tauri::Manager;
    let data_dir = app_handle
        .path()
        .app_data_dir()
        .expect("Failed to resolve app data directory");
    data_dir.join("connections.json")
}

/// Read all saved connections from the JSON file.
/// Returns an empty vec if the file doesn't exist yet (first run).
pub fn list_connections(path: &PathBuf) -> Result<Vec<SavedConnection>, DiffDonkeyError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(path)?;
    let connections: Vec<SavedConnection> =
        serde_json::from_str(&data).map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    Ok(connections)
}

/// Save (upsert) a connection to the JSON file and store its password in the keychain.
///
/// If a connection with the same `id` exists, it's replaced. Otherwise, it's appended.
/// The password is stored separately in the OS keychain, keyed by the connection's UUID.
/// SSH password is stored under a separate keychain entry: `diff-donkey/{id}/ssh`.
pub fn save_connection(
    path: &PathBuf,
    conn: SavedConnection,
    password: Option<String>,
    ssh_password: Option<String>,
) -> Result<(), DiffDonkeyError> {
    // Validate required fields
    validate_connection(&conn)?;

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Read existing connections (or empty vec)
    let mut connections = list_connections(path)?;

    // Upsert: replace if exists, append if new
    if let Some(pos) = connections.iter().position(|c| c.id == conn.id) {
        connections[pos] = conn.clone();
    } else {
        connections.push(conn.clone());
    }

    // Write back to file
    let json = serde_json::to_string_pretty(&connections)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(path, json)?;

    // Store DB password in keychain (if provided)
    if let Some(pw) = password {
        if !pw.is_empty() {
            store_password(&conn.id, &pw)?;
        }
    }

    // Store SSH password in keychain (if provided)
    if let Some(ssh_pw) = ssh_password {
        if !ssh_pw.is_empty() {
            let ssh_key = format!("{}/ssh", conn.id);
            store_password(&ssh_key, &ssh_pw)?;
        }
    }

    Ok(())
}

/// Delete a connection from the JSON file and remove its password from the keychain.
pub fn delete_connection(path: &PathBuf, id: &str) -> Result<(), DiffDonkeyError> {
    let mut connections = list_connections(path)?;
    connections.retain(|c| c.id != id);

    // Write back
    let json = serde_json::to_string_pretty(&connections)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(path, json)?;

    // Remove passwords from keychain (ignore errors — may not exist)
    let _ = delete_password(id);
    let ssh_key = format!("{}/ssh", id);
    let _ = delete_password(&ssh_key);

    Ok(())
}

/// Retrieve the SSH password from the OS keychain by connection ID.
pub fn get_ssh_password(id: &str) -> Result<Option<String>, DiffDonkeyError> {
    let ssh_key = format!("{}/ssh", id);
    get_password(&ssh_key)
}

/// Retrieve a password from the OS keychain by connection ID.
pub fn get_password(id: &str) -> Result<Option<String>, DiffDonkeyError> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, id)
        .map_err(|e| DiffDonkeyError::Validation(format!("Keyring error: {}", e)))?;
    match entry.get_password() {
        Ok(pw) => Ok(Some(pw)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(DiffDonkeyError::Validation(format!(
            "Failed to retrieve password: {}",
            e
        ))),
    }
}

/// Store a password in the OS keychain.
fn store_password(id: &str, password: &str) -> Result<(), DiffDonkeyError> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, id)
        .map_err(|e| DiffDonkeyError::Validation(format!("Keyring error: {}", e)))?;
    entry
        .set_password(password)
        .map_err(|e| DiffDonkeyError::Validation(format!("Failed to store password: {}", e)))
}

/// Delete a password from the OS keychain.
fn delete_password(id: &str) -> Result<(), DiffDonkeyError> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, id)
        .map_err(|e| DiffDonkeyError::Validation(format!("Keyring error: {}", e)))?;
    entry
        .delete_credential()
        .map_err(|e| DiffDonkeyError::Validation(format!("Failed to delete password: {}", e)))
}

/// Validate required fields on a SavedConnection.
pub fn validate_connection(conn: &SavedConnection) -> Result<(), DiffDonkeyError> {
    if conn.name.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Connection name cannot be empty".to_string(),
        ));
    }
    let valid_types = ["postgres", "mysql", "snowflake"];
    if !valid_types.contains(&conn.db_type.as_str()) {
        return Err(DiffDonkeyError::Validation(format!(
            "Invalid database type '{}'. Must be one of: {}",
            conn.db_type,
            valid_types.join(", ")
        )));
    }

    // Snowflake-specific validation
    if conn.db_type == "snowflake" {
        if conn.account_url.as_deref().unwrap_or("").trim().is_empty() {
            return Err(DiffDonkeyError::Validation(
                "Account URL is required for Snowflake connections".to_string(),
            ));
        }
        if conn.username.as_deref().unwrap_or("").trim().is_empty() {
            return Err(DiffDonkeyError::Validation(
                "Username is required for Snowflake connections".to_string(),
            ));
        }
        if conn.auth_method.as_deref() == Some("keypair") {
            if conn
                .private_key_path
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
            {
                return Err(DiffDonkeyError::Validation(
                    "Private key path is required for key-pair authentication".to_string(),
                ));
            }
        }
    }

    // Validate SSH fields when SSH is enabled
    if conn.ssh_enabled {
        if conn.ssh_host.as_deref().unwrap_or("").trim().is_empty() {
            return Err(DiffDonkeyError::Validation(
                "SSH host is required when SSH tunneling is enabled".to_string(),
            ));
        }
        if conn.ssh_username.as_deref().unwrap_or("").trim().is_empty() {
            return Err(DiffDonkeyError::Validation(
                "SSH username is required when SSH tunneling is enabled".to_string(),
            ));
        }
        if conn.ssh_auth_method.as_deref() == Some("key") {
            if conn.ssh_key_path.as_deref().unwrap_or("").trim().is_empty() {
                return Err(DiffDonkeyError::Validation(
                    "SSH key file path is required for key authentication".to_string(),
                ));
            }
        }
    }

    Ok(())
}

/// Build a database-specific connection string from structured fields.
///
/// Assembles the connection parameters into the format expected by DuckDB's
/// postgres_query() or mysql_query() functions.
///
/// Postgres format: host=X port=N dbname=X user=X password=X
/// MySQL format:    host=X port=N user=X password=X database=X
pub fn build_connection_string(conn: &SavedConnection, password: Option<&str>) -> String {
    match conn.db_type.as_str() {
        "postgres" => build_postgres_connection_string(conn, password),
        "mysql" => build_mysql_connection_string(conn, password),
        _ => String::new(),
    }
}

fn build_postgres_connection_string(conn: &SavedConnection, password: Option<&str>) -> String {
    let mut parts = Vec::new();

    if let Some(ref host) = conn.host {
        if !host.is_empty() {
            parts.push(format!("host={}", host));
        }
    }
    if let Some(port) = conn.port {
        parts.push(format!("port={}", port));
    }
    if let Some(ref database) = conn.database {
        if !database.is_empty() {
            parts.push(format!("dbname={}", database));
        }
    }
    if let Some(ref username) = conn.username {
        if !username.is_empty() {
            parts.push(format!("user={}", username));
        }
    }
    if let Some(pw) = password {
        if !pw.is_empty() {
            parts.push(format!("password={}", pw));
        }
    }
    if conn.ssl {
        parts.push("sslmode=require".to_string());
    }

    parts.join(" ")
}

fn build_mysql_connection_string(conn: &SavedConnection, password: Option<&str>) -> String {
    let mut parts = Vec::new();

    if let Some(ref host) = conn.host {
        if !host.is_empty() {
            parts.push(format!("host={}", host));
        }
    }
    if let Some(port) = conn.port {
        parts.push(format!("port={}", port));
    }
    if let Some(ref username) = conn.username {
        if !username.is_empty() {
            parts.push(format!("user={}", username));
        }
    }
    if let Some(pw) = password {
        if !pw.is_empty() {
            parts.push(format!("password={}", pw));
        }
    }
    if let Some(ref database) = conn.database {
        if !database.is_empty() {
            parts.push(format!("database={}", database));
        }
    }
    if conn.ssl {
        parts.push("ssl-mode=REQUIRED".to_string());
    }

    parts.join(" ")
}

/// Test a database connection by attempting a simple query via DuckDB.
///
/// Uses the same DuckDB extension mechanism as `db_loader::load_from_database`,
/// but only runs `SELECT 1` to verify connectivity without loading data.
/// For Snowflake, uses the REST API client instead.
/// If SSH tunneling is enabled, establishes a tunnel first and tests through it.
pub fn test_connection(
    duck_conn: &duckdb::Connection,
    conn: &SavedConnection,
    password: Option<&str>,
    ssh_password: Option<&str>,
) -> Result<String, DiffDonkeyError> {
    // Snowflake uses its own REST API — not DuckDB extensions
    if conn.db_type == "snowflake" {
        return test_snowflake_connection(conn, password);
    }

    // If SSH tunneling is enabled, establish tunnel and rewrite host/port
    let _tunnel: Option<crate::ssh_tunnel::SshTunnel>;
    let effective_conn: std::borrow::Cow<'_, SavedConnection>;

    if conn.ssh_enabled {
        let tunnel_config =
            crate::ssh_tunnel::build_tunnel_config(conn, ssh_password.map(|s| s.to_string()))?;
        let tunnel = crate::ssh_tunnel::start_tunnel(&tunnel_config)?;
        let mut tunneled = conn.clone();
        tunneled.host = Some("127.0.0.1".to_string());
        tunneled.port = Some(tunnel.local_port);
        _tunnel = Some(tunnel);
        effective_conn = std::borrow::Cow::Owned(tunneled);
    } else {
        _tunnel = None;
        effective_conn = std::borrow::Cow::Borrowed(conn);
    }

    let conn_string = build_connection_string(&effective_conn, password);

    if conn_string.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Connection string is empty — fill in at least host and database".to_string(),
        ));
    }

    let db_type = match effective_conn.db_type.as_str() {
        "postgres" => crate::db_loader::DatabaseType::Postgres,
        "mysql" => crate::db_loader::DatabaseType::MySQL,
        _ => {
            return Err(DiffDonkeyError::Validation(format!(
                "Testing not yet supported for '{}'",
                effective_conn.db_type
            )));
        }
    };

    let ext = db_type.extension_name();
    let query_fn = db_type.query_function();

    // Install and load extension
    let install_sql = format!("INSTALL {}; LOAD {};", ext, ext);
    duck_conn.execute_batch(&install_sql)?;

    // Escape connection string for SQL
    let escaped_conn = conn_string.replace('\'', "''");
    let test_sql = format!("SELECT * FROM {}('{}', 'SELECT 1')", query_fn, escaped_conn);

    duck_conn.execute_batch(&test_sql)?;

    // _tunnel is dropped here, which signals the background thread to stop
    Ok("Connection successful".to_string())
}

/// Test a Snowflake connection by authenticating and running SELECT 1.
fn test_snowflake_connection(
    conn: &SavedConnection,
    password: Option<&str>,
) -> Result<String, DiffDonkeyError> {
    let account_url = conn
        .account_url
        .as_deref()
        .ok_or_else(|| DiffDonkeyError::Validation("Account URL is required".to_string()))?;

    let username = conn
        .username
        .as_deref()
        .ok_or_else(|| DiffDonkeyError::Validation("Username is required".to_string()))?;

    let config = crate::snowflake::SnowflakeConfig {
        account_url: account_url.to_string(),
        warehouse: conn.warehouse.clone(),
        role: conn.role.clone(),
        database: conn.database.clone(),
        schema: conn.schema.clone(),
    };

    let auth = build_snowflake_auth(conn, username, password)?;

    // Run async code from sync context using tokio runtime
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| DiffDonkeyError::Snowflake(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async {
        let client_config = crate::snowflake::SnowflakeConfig {
            account_url: config.account_url,
            warehouse: config.warehouse,
            role: config.role,
            database: config.database,
            schema: config.schema,
        };
        // Just authenticate and run SELECT 1 — we don't need to load results
        let duck_conn =
            duckdb::Connection::open_in_memory().map_err(|e| DiffDonkeyError::DuckDb(e))?;
        let log = crate::activity::ActivityLog::new();
        crate::snowflake::load_snowflake(
            client_config,
            auth,
            "SELECT 1 AS test",
            &duck_conn,
            "_snowflake_test",
            &log,
        )
        .await?;
        Ok("Connection successful".to_string())
    })
}

/// Build a SnowflakeAuth from a SavedConnection.
pub fn build_snowflake_auth(
    conn: &SavedConnection,
    username: &str,
    password: Option<&str>,
) -> Result<crate::snowflake::SnowflakeAuth, DiffDonkeyError> {
    match conn.auth_method.as_deref() {
        Some("keypair") => {
            let key_path = conn.private_key_path.as_deref().ok_or_else(|| {
                DiffDonkeyError::Validation("Private key path is required".to_string())
            })?;

            let private_key_pem = std::fs::read_to_string(key_path).map_err(|e| {
                DiffDonkeyError::Snowflake(format!(
                    "Failed to read private key file '{}': {}",
                    key_path, e
                ))
            })?;

            Ok(crate::snowflake::SnowflakeAuth::KeyPair {
                username: username.to_string(),
                private_key_pem,
            })
        }
        _ => {
            // Default to password auth
            let pw = password.unwrap_or("").to_string();
            Ok(crate::snowflake::SnowflakeAuth::Password {
                username: username.to_string(),
                password: pw,
            })
        }
    }
}

// ─── Import / Export ─────────────────────────────────────────────────────────

/// A connection profile stripped of IDs, passwords, and timestamps for export.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExportedConnection {
    pub name: String,
    pub db_type: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub schema: Option<String>,
    pub ssl: bool,
    pub color: Option<String>,
    pub account_url: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub auth_method: Option<String>,
    pub private_key_path: Option<String>,
    pub ssh_enabled: bool,
    pub ssh_host: Option<String>,
    pub ssh_port: Option<u16>,
    pub ssh_username: Option<String>,
    pub ssh_auth_method: Option<String>,
    pub ssh_key_path: Option<String>,
}

/// Top-level export envelope with version and timestamp.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectionExport {
    pub version: u32,
    pub exported_at: String,
    pub connections: Vec<ExportedConnection>,
}

/// Summary returned after importing connections.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportResult {
    pub imported: usize,
    pub skipped: usize,
    pub skipped_names: Vec<String>,
}

impl From<&SavedConnection> for ExportedConnection {
    fn from(c: &SavedConnection) -> Self {
        ExportedConnection {
            name: c.name.clone(),
            db_type: c.db_type.clone(),
            host: c.host.clone(),
            port: c.port,
            database: c.database.clone(),
            username: c.username.clone(),
            schema: c.schema.clone(),
            ssl: c.ssl,
            color: c.color.clone(),
            account_url: c.account_url.clone(),
            warehouse: c.warehouse.clone(),
            role: c.role.clone(),
            auth_method: c.auth_method.clone(),
            private_key_path: c.private_key_path.clone(),
            ssh_enabled: c.ssh_enabled,
            ssh_host: c.ssh_host.clone(),
            ssh_port: c.ssh_port,
            ssh_username: c.ssh_username.clone(),
            ssh_auth_method: c.ssh_auth_method.clone(),
            ssh_key_path: c.ssh_key_path.clone(),
        }
    }
}

/// Export all saved connections to a JSON file. Passwords and IDs are stripped.
pub fn export_connections(connections_path: &Path) -> Result<ConnectionExport, DiffDonkeyError> {
    let connections = list_connections(&connections_path.to_path_buf())?;
    let exported: Vec<ExportedConnection> = connections.iter().map(ExportedConnection::from).collect();
    let now = chrono::Utc::now().to_rfc3339();
    Ok(ConnectionExport {
        version: 1,
        exported_at: now,
        connections: exported,
    })
}

/// Write a ConnectionExport to a file as pretty-printed JSON.
pub fn write_export_file(
    export_data: &ConnectionExport,
    output_path: &Path,
) -> Result<(), DiffDonkeyError> {
    let json = serde_json::to_string_pretty(export_data)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(output_path, json)?;
    Ok(())
}

/// Import connections from a JSON file. Skips connections whose name already exists.
/// Returns a summary of how many were imported vs. skipped.
pub fn import_connections(
    connections_path: &Path,
    export_data: &ConnectionExport,
) -> Result<ImportResult, DiffDonkeyError> {
    if export_data.version != 1 {
        return Err(DiffDonkeyError::Validation(format!(
            "Unsupported export version: {}. Expected 1.",
            export_data.version
        )));
    }

    let mut existing = list_connections(&connections_path.to_path_buf())?;
    let existing_names: std::collections::HashSet<String> =
        existing.iter().map(|c| c.name.clone()).collect();

    let mut imported = 0;
    let mut skipped_names = Vec::new();

    for ec in &export_data.connections {
        if existing_names.contains(&ec.name) {
            skipped_names.push(ec.name.clone());
            continue;
        }

        let now = chrono::Utc::now().to_rfc3339();
        let conn = SavedConnection {
            id: uuid::Uuid::new_v4().to_string(),
            name: ec.name.clone(),
            db_type: ec.db_type.clone(),
            host: ec.host.clone(),
            port: ec.port,
            database: ec.database.clone(),
            username: ec.username.clone(),
            schema: ec.schema.clone(),
            ssl: ec.ssl,
            color: ec.color.clone(),
            account_url: ec.account_url.clone(),
            warehouse: ec.warehouse.clone(),
            role: ec.role.clone(),
            auth_method: ec.auth_method.clone(),
            private_key_path: ec.private_key_path.clone(),
            ssh_enabled: ec.ssh_enabled,
            ssh_host: ec.ssh_host.clone(),
            ssh_port: ec.ssh_port,
            ssh_username: ec.ssh_username.clone(),
            ssh_auth_method: ec.ssh_auth_method.clone(),
            ssh_key_path: ec.ssh_key_path.clone(),
            created_at: now.clone(),
            updated_at: now,
        };
        existing.push(conn);
        imported += 1;
    }

    // Ensure parent directory exists
    if let Some(parent) = connections_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write back
    let json = serde_json::to_string_pretty(&existing)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(connections_path, json)?;

    Ok(ImportResult {
        imported,
        skipped: skipped_names.len(),
        skipped_names,
    })
}

/// Read and parse a ConnectionExport from a JSON file.
pub fn read_export_file(path: &Path) -> Result<ConnectionExport, DiffDonkeyError> {
    let data = std::fs::read_to_string(path)?;
    let export: ConnectionExport =
        serde_json::from_str(&data).map_err(|e| DiffDonkeyError::Validation(format!(
            "Invalid connection export file: {}",
            e
        )))?;
    Ok(export)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a minimal SavedConnection for testing.
    fn test_conn(db_type: &str) -> SavedConnection {
        SavedConnection {
            id: "test-id".to_string(),
            name: "Test Connection".to_string(),
            db_type: db_type.to_string(),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("mydb".to_string()),
            username: Some("user".to_string()),
            schema: None,
            ssl: false,
            color: None,
            account_url: None,
            warehouse: None,
            role: None,
            auth_method: None,
            private_key_path: None,
            ssh_enabled: false,
            ssh_host: None,
            ssh_port: None,
            ssh_username: None,
            ssh_auth_method: None,
            ssh_key_path: None,
            created_at: "2026-03-30T00:00:00Z".to_string(),
            updated_at: "2026-03-30T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn test_build_postgres_connection_string() {
        let conn = test_conn("postgres");
        let result = build_connection_string(&conn, Some("secret"));
        assert_eq!(
            result,
            "host=localhost port=5432 dbname=mydb user=user password=secret"
        );
    }

    #[test]
    fn test_build_postgres_with_ssl() {
        let mut conn = test_conn("postgres");
        conn.ssl = true;
        let result = build_connection_string(&conn, Some("secret"));
        assert_eq!(
            result,
            "host=localhost port=5432 dbname=mydb user=user password=secret sslmode=require"
        );
    }

    #[test]
    fn test_build_mysql_connection_string() {
        let mut conn = test_conn("mysql");
        conn.port = Some(3306);
        let result = build_connection_string(&conn, Some("secret"));
        assert_eq!(
            result,
            "host=localhost port=3306 user=user password=secret database=mydb"
        );
    }

    #[test]
    fn test_build_mysql_with_ssl() {
        let mut conn = test_conn("mysql");
        conn.port = Some(3306);
        conn.ssl = true;
        let result = build_connection_string(&conn, Some("secret"));
        assert_eq!(
            result,
            "host=localhost port=3306 user=user password=secret database=mydb ssl-mode=REQUIRED"
        );
    }

    #[test]
    fn test_build_connection_string_no_password() {
        let conn = test_conn("postgres");
        let result = build_connection_string(&conn, None);
        assert_eq!(result, "host=localhost port=5432 dbname=mydb user=user");
    }

    #[test]
    fn test_build_connection_string_empty_password() {
        let conn = test_conn("postgres");
        let result = build_connection_string(&conn, Some(""));
        assert_eq!(result, "host=localhost port=5432 dbname=mydb user=user");
    }

    #[test]
    fn test_build_connection_string_minimal() {
        let mut conn = test_conn("postgres");
        conn.host = None;
        conn.port = None;
        conn.username = None;
        let result = build_connection_string(&conn, None);
        assert_eq!(result, "dbname=mydb");
    }

    #[test]
    fn test_validate_connection_empty_name() {
        let mut conn = test_conn("postgres");
        conn.name = "".to_string();
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("name cannot be empty"));
    }

    #[test]
    fn test_validate_connection_whitespace_name() {
        let mut conn = test_conn("postgres");
        conn.name = "   ".to_string();
        let result = validate_connection(&conn);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_connection_invalid_db_type() {
        let mut conn = test_conn("sqlite");
        conn.name = "Test".to_string();
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid database type"));
    }

    #[test]
    fn test_validate_connection_valid_types() {
        for db_type in &["postgres", "mysql", "snowflake"] {
            let mut conn = test_conn(db_type);
            if *db_type == "snowflake" {
                conn.account_url =
                    Some("https://myorg-myaccount.snowflakecomputing.com".to_string());
            }
            assert!(
                validate_connection(&conn).is_ok(),
                "Failed for type: {}",
                db_type
            );
        }
    }

    #[test]
    fn test_json_roundtrip() {
        let conn = test_conn("postgres");
        let json = serde_json::to_string(&conn).unwrap();
        let deserialized: SavedConnection = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, conn.id);
        assert_eq!(deserialized.name, conn.name);
        assert_eq!(deserialized.db_type, conn.db_type);
        assert_eq!(deserialized.host, conn.host);
        assert_eq!(deserialized.port, conn.port);
        assert_eq!(deserialized.ssl, conn.ssl);
        assert_eq!(deserialized.ssh_enabled, conn.ssh_enabled);
    }

    #[test]
    fn test_json_vec_roundtrip() {
        let connections = vec![test_conn("postgres"), test_conn("mysql")];
        let json = serde_json::to_string_pretty(&connections).unwrap();
        let deserialized: Vec<SavedConnection> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0].db_type, "postgres");
        assert_eq!(deserialized[1].db_type, "mysql");
    }

    #[test]
    fn test_list_connections_nonexistent_file() {
        let path = PathBuf::from("/tmp/nonexistent_connections.json");
        let result = list_connections(&path).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_save_and_list_connections() {
        let path = PathBuf::from("/tmp/test_diff_donkey_connections.json");
        // Clean up from any previous test run
        let _ = std::fs::remove_file(&path);

        let conn = test_conn("postgres");
        // save_connection calls keyring which may fail in CI — test file operations only
        // Write directly to test list_connections
        let connections = vec![conn.clone()];
        let json = serde_json::to_string_pretty(&connections).unwrap();
        std::fs::write(&path, json).unwrap();

        let loaded = list_connections(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Test Connection");

        // Clean up
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_build_unknown_db_type() {
        let conn = test_conn("sqlite");
        let result = build_connection_string(&conn, Some("pw"));
        assert_eq!(result, "");
    }

    #[test]
    fn test_validate_snowflake_missing_account_url() {
        let mut conn = test_conn("snowflake");
        conn.account_url = None;
        conn.username = Some("user".to_string());
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Account URL is required"));
    }

    #[test]
    fn test_validate_snowflake_missing_username() {
        let mut conn = test_conn("snowflake");
        conn.account_url = Some("https://org-acct.snowflakecomputing.com".to_string());
        conn.username = None;
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Username is required"));
    }

    #[test]
    fn test_validate_snowflake_keypair_missing_key_path() {
        let mut conn = test_conn("snowflake");
        conn.account_url = Some("https://org-acct.snowflakecomputing.com".to_string());
        conn.username = Some("user".to_string());
        conn.auth_method = Some("keypair".to_string());
        conn.private_key_path = None;
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Private key path is required"));
    }

    #[test]
    fn test_validate_snowflake_valid_password_auth() {
        let mut conn = test_conn("snowflake");
        conn.account_url = Some("https://org-acct.snowflakecomputing.com".to_string());
        conn.username = Some("user".to_string());
        conn.auth_method = Some("password".to_string());
        assert!(validate_connection(&conn).is_ok());
    }

    #[test]
    fn test_validate_snowflake_valid_keypair_auth() {
        let mut conn = test_conn("snowflake");
        conn.account_url = Some("https://org-acct.snowflakecomputing.com".to_string());
        conn.username = Some("user".to_string());
        conn.auth_method = Some("keypair".to_string());
        conn.private_key_path = Some("/path/to/key.p8".to_string());
        assert!(validate_connection(&conn).is_ok());
    }

    // ─── SSH Validation Tests ────────────────────────────────────────────

    #[test]
    fn test_validate_ssh_enabled_missing_host() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = true;
        conn.ssh_host = None;
        conn.ssh_username = Some("sshuser".to_string());
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH host is required"));
    }

    #[test]
    fn test_validate_ssh_enabled_empty_host() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = true;
        conn.ssh_host = Some("  ".to_string());
        conn.ssh_username = Some("sshuser".to_string());
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH host is required"));
    }

    #[test]
    fn test_validate_ssh_enabled_missing_username() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = true;
        conn.ssh_host = Some("bastion.example.com".to_string());
        conn.ssh_username = None;
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH username is required"));
    }

    #[test]
    fn test_validate_ssh_key_auth_missing_path() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = true;
        conn.ssh_host = Some("bastion.example.com".to_string());
        conn.ssh_username = Some("sshuser".to_string());
        conn.ssh_auth_method = Some("key".to_string());
        conn.ssh_key_path = None;
        let result = validate_connection(&conn);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH key file path is required"));
    }

    #[test]
    fn test_validate_ssh_disabled_no_requirements() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = false;
        conn.ssh_host = None;
        conn.ssh_username = None;
        conn.ssh_auth_method = None;
        conn.ssh_key_path = None;
        assert!(validate_connection(&conn).is_ok());
    }

    #[test]
    fn test_validate_ssh_enabled_valid_password_auth() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = true;
        conn.ssh_host = Some("bastion.example.com".to_string());
        conn.ssh_username = Some("sshuser".to_string());
        conn.ssh_auth_method = Some("password".to_string());
        assert!(validate_connection(&conn).is_ok());
    }

    #[test]
    fn test_validate_ssh_enabled_valid_key_auth() {
        let mut conn = test_conn("postgres");
        conn.ssh_enabled = true;
        conn.ssh_host = Some("bastion.example.com".to_string());
        conn.ssh_username = Some("sshuser".to_string());
        conn.ssh_auth_method = Some("key".to_string());
        conn.ssh_key_path = Some("/home/user/.ssh/id_rsa".to_string());
        assert!(validate_connection(&conn).is_ok());
    }

    // ─── Import / Export Tests ──────────────────────────────────────────

    #[test]
    fn test_export_strips_passwords_and_ids() {
        let path = PathBuf::from("/tmp/test_dd_export_strip.json");
        let _ = std::fs::remove_file(&path);

        let conn = test_conn("postgres");
        let json = serde_json::to_string_pretty(&vec![conn]).unwrap();
        std::fs::write(&path, json).unwrap();

        let export = export_connections(&path).unwrap();
        assert_eq!(export.connections.len(), 1);

        let exported_json = serde_json::to_string(&export.connections[0]).unwrap();
        assert!(!exported_json.contains("\"id\""));
        assert!(!exported_json.contains("\"created_at\""));
        assert!(!exported_json.contains("\"updated_at\""));
        assert!(!exported_json.contains("password"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_export_format() {
        let path = PathBuf::from("/tmp/test_dd_export_format.json");
        let _ = std::fs::remove_file(&path);

        let connections = vec![test_conn("postgres"), test_conn("mysql")];
        let json = serde_json::to_string_pretty(&connections).unwrap();
        std::fs::write(&path, json).unwrap();

        let export = export_connections(&path).unwrap();
        assert_eq!(export.version, 1);
        assert!(!export.exported_at.is_empty());
        assert_eq!(export.connections.len(), 2);
        assert_eq!(export.connections[0].db_type, "postgres");
        assert_eq!(export.connections[1].db_type, "mysql");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_import_creates_new_connections() {
        let path = PathBuf::from("/tmp/test_dd_import_new.json");
        let _ = std::fs::remove_file(&path);

        let export = ConnectionExport {
            version: 1,
            exported_at: "2026-04-07T00:00:00Z".to_string(),
            connections: vec![ExportedConnection {
                name: "Import Test".to_string(),
                db_type: "postgres".to_string(),
                host: Some("db.example.com".to_string()),
                port: Some(5432),
                database: Some("testdb".to_string()),
                username: Some("user".to_string()),
                schema: None,
                ssl: false,
                color: None,
                account_url: None,
                warehouse: None,
                role: None,
                auth_method: None,
                private_key_path: None,
                ssh_enabled: false,
                ssh_host: None,
                ssh_port: None,
                ssh_username: None,
                ssh_auth_method: None,
                ssh_key_path: None,
            }],
        };

        let result = import_connections(&path, &export).unwrap();
        assert_eq!(result.imported, 1);
        assert_eq!(result.skipped, 0);

        let loaded = list_connections(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "Import Test");
        assert!(!loaded[0].id.is_empty());
        assert!(!loaded[0].created_at.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_import_skips_duplicates() {
        let path = PathBuf::from("/tmp/test_dd_import_dup.json");
        let _ = std::fs::remove_file(&path);

        // Pre-populate with an existing connection
        let existing = test_conn("postgres");
        let json = serde_json::to_string_pretty(&vec![existing]).unwrap();
        std::fs::write(&path, json).unwrap();

        let export = ConnectionExport {
            version: 1,
            exported_at: "2026-04-07T00:00:00Z".to_string(),
            connections: vec![
                ExportedConnection {
                    name: "Test Connection".to_string(), // same name as existing
                    db_type: "postgres".to_string(),
                    host: Some("other.example.com".to_string()),
                    port: Some(5432),
                    database: None,
                    username: None,
                    schema: None,
                    ssl: false,
                    color: None,
                    account_url: None,
                    warehouse: None,
                    role: None,
                    auth_method: None,
                    private_key_path: None,
                    ssh_enabled: false,
                    ssh_host: None,
                    ssh_port: None,
                    ssh_username: None,
                    ssh_auth_method: None,
                    ssh_key_path: None,
                },
                ExportedConnection {
                    name: "New Connection".to_string(),
                    db_type: "mysql".to_string(),
                    host: Some("new.example.com".to_string()),
                    port: Some(3306),
                    database: None,
                    username: None,
                    schema: None,
                    ssl: false,
                    color: None,
                    account_url: None,
                    warehouse: None,
                    role: None,
                    auth_method: None,
                    private_key_path: None,
                    ssh_enabled: false,
                    ssh_host: None,
                    ssh_port: None,
                    ssh_username: None,
                    ssh_auth_method: None,
                    ssh_key_path: None,
                },
            ],
        };

        let result = import_connections(&path, &export).unwrap();
        assert_eq!(result.imported, 1);
        assert_eq!(result.skipped, 1);
        assert_eq!(result.skipped_names, vec!["Test Connection"]);

        let loaded = list_connections(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_import_invalid_json() {
        let result: Result<ConnectionExport, _> =
            serde_json::from_str("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_import_invalid_version() {
        let path = PathBuf::from("/tmp/test_dd_import_ver.json");
        let _ = std::fs::remove_file(&path);

        let export = ConnectionExport {
            version: 99,
            exported_at: "2026-04-07T00:00:00Z".to_string(),
            connections: vec![],
        };

        let result = import_connections(&path, &export);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported export version"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_roundtrip_export_import() {
        let src_path = PathBuf::from("/tmp/test_dd_roundtrip_src.json");
        let dst_path = PathBuf::from("/tmp/test_dd_roundtrip_dst.json");
        let _ = std::fs::remove_file(&src_path);
        let _ = std::fs::remove_file(&dst_path);

        // Create source connections
        let mut pg = test_conn("postgres");
        pg.name = "Roundtrip PG".to_string();
        pg.color = Some("#e74c3c".to_string());
        let mut my = test_conn("mysql");
        my.name = "Roundtrip MySQL".to_string();
        my.port = Some(3306);

        let json = serde_json::to_string_pretty(&vec![pg, my]).unwrap();
        std::fs::write(&src_path, json).unwrap();

        // Export
        let export = export_connections(&src_path).unwrap();
        assert_eq!(export.connections.len(), 2);

        // Import into empty target
        let result = import_connections(&dst_path, &export).unwrap();
        assert_eq!(result.imported, 2);
        assert_eq!(result.skipped, 0);

        // Verify imported connections
        let loaded = list_connections(&dst_path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "Roundtrip PG");
        assert_eq!(loaded[0].color, Some("#e74c3c".to_string()));
        assert_eq!(loaded[1].name, "Roundtrip MySQL");
        assert_eq!(loaded[1].port, Some(3306));

        // IDs should be different from source
        let source = list_connections(&src_path).unwrap();
        assert_ne!(loaded[0].id, source[0].id);

        let _ = std::fs::remove_file(&src_path);
        let _ = std::fs::remove_file(&dst_path);
    }
}
