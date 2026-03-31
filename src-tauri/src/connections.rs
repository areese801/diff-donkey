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
use std::path::PathBuf;

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
}
