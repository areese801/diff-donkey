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
    // Snowflake-specific (placeholder for Phase 2)
    pub account_url: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
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
pub fn save_connection(
    path: &PathBuf,
    conn: SavedConnection,
    password: Option<String>,
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

    // Store password in keychain (if provided)
    if let Some(pw) = password {
        if !pw.is_empty() {
            store_password(&conn.id, &pw)?;
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

    // Remove password from keychain (ignore errors — may not exist)
    let _ = delete_password(id);

    Ok(())
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
pub fn test_connection(
    duck_conn: &duckdb::Connection,
    conn: &SavedConnection,
    password: Option<&str>,
) -> Result<String, DiffDonkeyError> {
    let conn_string = build_connection_string(conn, password);

    if conn_string.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Connection string is empty — fill in at least host and database".to_string(),
        ));
    }

    let db_type = match conn.db_type.as_str() {
        "postgres" => crate::db_loader::DatabaseType::Postgres,
        "mysql" => crate::db_loader::DatabaseType::MySQL,
        _ => {
            return Err(DiffDonkeyError::Validation(format!(
                "Testing not yet supported for '{}'",
                conn.db_type
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

    Ok("Connection successful".to_string())
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
            let conn = test_conn(db_type);
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
}
