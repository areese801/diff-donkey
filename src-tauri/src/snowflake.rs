/// Snowflake connectivity via REST API.
///
/// Unlike Postgres/MySQL (which use DuckDB extensions), Snowflake requires
/// a Rust-native HTTP client. This module handles:
///   1. Authentication (password or key-pair/JWT)
///   2. Query execution via the Snowflake SQL API v2
///   3. Loading results into DuckDB tables
///
/// The Snowflake SQL API docs:
///   POST /api/v2/statements — submit a statement
///   GET  /api/v2/statements/{handle} — check status / fetch results
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Client;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::activity::{self, ActivityLog};
use crate::db_loader;
use crate::error::DiffDonkeyError;
use crate::types::TableMeta;

// ─── Public Types ───────────────────────────────────────────────────────────

pub struct SnowflakeConfig {
    pub account_url: String,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

pub enum SnowflakeAuth {
    Password { username: String, password: String },
    KeyPair { username: String, private_key_pem: String },
}

pub struct SnowflakeColumn {
    pub name: String,
    pub sf_type: String,
}

pub struct SnowflakeQueryResult {
    pub columns: Vec<SnowflakeColumn>,
    pub rows: Vec<Vec<Option<String>>>,
}

// ─── Internal Types ─────────────────────────────────────────────────────────

struct SnowflakeClient {
    config: SnowflakeConfig,
    http: Client,
    token: String,
    is_keypair: bool,
}

#[derive(Serialize)]
struct PasswordLoginRequest {
    data: PasswordLoginData,
}

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct PasswordLoginData {
    login_name: String,
    password: String,
    account_name: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    data: Option<LoginResponseData>,
    message: Option<String>,
    code: Option<String>,
    success: Option<bool>,
}

#[derive(Deserialize)]
struct LoginResponseData {
    token: Option<String>,
}

#[derive(Serialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

#[derive(Serialize)]
struct StatementRequest {
    statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    warehouse: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    timeout: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct StatementResponse {
    #[serde(default)]
    statement_handle: Option<String>,
    #[serde(default)]
    statement_status_url: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    result_set_meta_data: Option<ResultSetMetaData>,
    #[serde(default)]
    data: Option<Vec<Vec<Option<String>>>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ResultSetMetaData {
    #[serde(default)]
    row_type: Vec<ColumnMetaData>,
    #[serde(default)]
    num_rows: Option<u64>,
    #[serde(default)]
    partition_info: Option<Vec<PartitionInfo>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ColumnMetaData {
    name: String,
    #[serde(rename = "type")]
    type_name: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PartitionInfo {
    row_count: u64,
}

// ─── Account Extraction ─────────────────────────────────────────────────────

/// Extract the Snowflake account identifier from a full URL.
///
/// "https://myorg-myaccount.snowflakecomputing.com" → "MYORG-MYACCOUNT"
/// "https://myorg-myaccount.snowflakecomputing.com/" → "MYORG-MYACCOUNT"
pub fn extract_account(account_url: &str) -> Result<String, DiffDonkeyError> {
    let url = account_url
        .trim()
        .trim_end_matches('/');

    // Strip protocol
    let host = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);

    // Take everything before the first dot
    let account = host
        .split('.')
        .next()
        .ok_or_else(|| DiffDonkeyError::Snowflake("Invalid account URL".to_string()))?;

    if account.is_empty() {
        return Err(DiffDonkeyError::Snowflake(
            "Could not extract account from URL".to_string(),
        ));
    }

    Ok(account.to_uppercase())
}

// ─── Authentication ─────────────────────────────────────────────────────────

/// Authenticate with Snowflake using username/password.
async fn authenticate_password(
    http: &Client,
    account_url: &str,
    username: &str,
    password: &str,
) -> Result<String, DiffDonkeyError> {
    let account = extract_account(account_url)?;
    let url = format!(
        "{}/session/v1/login-request",
        account_url.trim_end_matches('/')
    );

    let body = PasswordLoginRequest {
        data: PasswordLoginData {
            login_name: username.to_string(),
            password: password.to_string(),
            account_name: account,
        },
    };

    let resp = http
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| DiffDonkeyError::Snowflake(format!("HTTP request failed: {}", e)))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| DiffDonkeyError::Snowflake(format!("Failed to read response: {}", e)))?;

    let login_resp: LoginResponse = serde_json::from_str(&text).map_err(|e| {
        DiffDonkeyError::Snowflake(format!(
            "Failed to parse login response (HTTP {}): {}",
            status, e
        ))
    })?;

    // Check for MFA requirement
    if let Some(ref code) = login_resp.code {
        if code == "390318" {
            return Err(DiffDonkeyError::Snowflake(
                "MFA is required. Please use key-pair authentication instead.".to_string(),
            ));
        }
    }

    if login_resp.success != Some(true) {
        let msg = login_resp
            .message
            .unwrap_or_else(|| format!("authentication failed (HTTP {})", status));
        return Err(DiffDonkeyError::Snowflake(msg));
    }

    login_resp
        .data
        .and_then(|d| d.token)
        .ok_or_else(|| DiffDonkeyError::Snowflake("No token in login response".to_string()))
}

/// Authenticate with Snowflake using key-pair (JWT).
fn authenticate_keypair(
    account_url: &str,
    username: &str,
    private_key_pem: &str,
) -> Result<String, DiffDonkeyError> {
    let account = extract_account(account_url)?;

    // Parse the private key from PEM
    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem).map_err(|e| {
        DiffDonkeyError::Snowflake(format!(
            "Failed to parse private key: {}. Ensure it is unencrypted PKCS#8 PEM format.",
            e
        ))
    })?;

    // Extract public key and compute SHA-256 fingerprint
    let public_key = private_key.to_public_key();
    let pub_der = public_key
        .to_public_key_der()
        .map_err(|e| DiffDonkeyError::Snowflake(format!("Failed to encode public key: {}", e)))?;

    let mut hasher = Sha256::new();
    hasher.update(pub_der.as_bytes());
    let fingerprint = BASE64.encode(hasher.finalize());

    let qualified_username = format!("{}.{}", account, username.to_uppercase());
    let iss = format!("{}.SHA256:{}", qualified_username, fingerprint);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let claims = JwtClaims {
        iss,
        sub: qualified_username,
        iat: now,
        exp: now + 60,
    };

    let header = Header::new(Algorithm::RS256);
    let encoding_key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes()).map_err(|e| {
        DiffDonkeyError::Snowflake(format!("Failed to create encoding key: {}", e))
    })?;

    encode(&header, &claims, &encoding_key)
        .map_err(|e| DiffDonkeyError::Snowflake(format!("Failed to sign JWT: {}", e)))
}

// ─── Query Execution ────────────────────────────────────────────────────────

impl SnowflakeClient {
    /// Create a new authenticated Snowflake client.
    async fn new(config: SnowflakeConfig, auth: SnowflakeAuth) -> Result<Self, DiffDonkeyError> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| DiffDonkeyError::Snowflake(format!("Failed to create HTTP client: {}", e)))?;

        let (token, is_keypair) = match auth {
            SnowflakeAuth::Password { username, password } => {
                let token =
                    authenticate_password(&http, &config.account_url, &username, &password).await?;
                (token, false)
            }
            SnowflakeAuth::KeyPair {
                username,
                private_key_pem,
            } => {
                let token =
                    authenticate_keypair(&config.account_url, &username, &private_key_pem)?;
                (token, true)
            }
        };

        Ok(Self {
            config,
            http,
            token,
            is_keypair,
        })
    }

    /// Execute a SQL statement and return parsed results.
    async fn execute_query(&self, sql: &str) -> Result<SnowflakeQueryResult, DiffDonkeyError> {
        let url = format!(
            "{}/api/v2/statements",
            self.config.account_url.trim_end_matches('/')
        );

        let body = StatementRequest {
            statement: sql.to_string(),
            warehouse: self.config.warehouse.clone(),
            role: self.config.role.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
            timeout: 300,
        };

        let mut req = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .bearer_auth(&self.token);

        if self.is_keypair {
            req = req.header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT_TOKEN");
        }

        let resp = req
            .json(&body)
            .send()
            .await
            .map_err(|e| DiffDonkeyError::Snowflake(format!("Query request failed: {}", e)))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| DiffDonkeyError::Snowflake(format!("Failed to read response: {}", e)))?;

        let mut stmt_resp: StatementResponse = serde_json::from_str(&text).map_err(|e| {
            DiffDonkeyError::Snowflake(format!(
                "Failed to parse statement response (HTTP {}): {}",
                status, e
            ))
        })?;

        // Check for error codes
        if let Some(ref code) = stmt_resp.code {
            let code_num: u32 = code.parse().unwrap_or(0);
            // Codes >= 390000 are typically errors; 090001 = statement still running
            if code_num >= 390000 {
                let msg = stmt_resp
                    .message
                    .unwrap_or_else(|| format!("Snowflake error code {}", code));
                return Err(DiffDonkeyError::Snowflake(msg));
            }
        }

        // If the statement is still running, poll for completion
        if let Some(ref status_url) = stmt_resp.statement_status_url {
            // Check if we need to poll — the status URL is present when async
            if stmt_resp.data.is_none() || stmt_resp.result_set_meta_data.is_none() {
                stmt_resp = self.poll_statement(status_url).await?;
            }
        }

        // Also handle polling via statement handle if we got a handle but no data
        if stmt_resp.data.is_none() {
            if let Some(ref handle) = stmt_resp.statement_handle {
                let status_url = format!(
                    "{}/api/v2/statements/{}",
                    self.config.account_url.trim_end_matches('/'),
                    handle
                );
                stmt_resp = self.poll_statement(&status_url).await?;
            }
        }

        // Parse columns from metadata
        let meta = stmt_resp.result_set_meta_data.ok_or_else(|| {
            DiffDonkeyError::Snowflake("No result set metadata in response".to_string())
        })?;

        let columns: Vec<SnowflakeColumn> = meta
            .row_type
            .iter()
            .map(|c| SnowflakeColumn {
                name: c.name.clone(),
                sf_type: c.type_name.clone().unwrap_or_else(|| "TEXT".to_string()),
            })
            .collect();

        // Collect data from first partition
        let mut rows = stmt_resp.data.unwrap_or_default();

        // Fetch additional partitions if present
        if let (Some(ref partitions), Some(ref handle)) =
            (&meta.partition_info, &stmt_resp.statement_handle)
        {
            if partitions.len() > 1 {
                for partition_idx in 1..partitions.len() {
                    let partition_rows = self.fetch_partition(handle, partition_idx).await?;
                    rows.extend(partition_rows);
                }
            }
        }

        Ok(SnowflakeQueryResult { columns, rows })
    }

    /// Poll a statement status URL until completion with exponential backoff.
    async fn poll_statement(
        &self,
        status_url: &str,
    ) -> Result<StatementResponse, DiffDonkeyError> {
        let mut delay_ms: u64 = 500;
        let max_delay_ms: u64 = 30_000;
        let total_timeout = std::time::Duration::from_secs(300);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > total_timeout {
                return Err(DiffDonkeyError::Snowflake(
                    "Query timed out after 5 minutes".to_string(),
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;

            let mut req = self
                .http
                .get(status_url)
                .header("Accept", "application/json")
                .bearer_auth(&self.token);

            if self.is_keypair {
                req =
                    req.header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT_TOKEN");
            }

            let resp = req
                .send()
                .await
                .map_err(|e| DiffDonkeyError::Snowflake(format!("Poll request failed: {}", e)))?;

            let text = resp.text().await.map_err(|e| {
                DiffDonkeyError::Snowflake(format!("Failed to read poll response: {}", e))
            })?;

            let stmt_resp: StatementResponse =
                serde_json::from_str(&text).map_err(|e| {
                    DiffDonkeyError::Snowflake(format!("Failed to parse poll response: {}", e))
                })?;

            // Check for errors
            if let Some(ref code) = stmt_resp.code {
                let code_num: u32 = code.parse().unwrap_or(0);
                if code_num >= 390000 {
                    let msg = stmt_resp
                        .message
                        .unwrap_or_else(|| format!("Snowflake error code {}", code));
                    return Err(DiffDonkeyError::Snowflake(msg));
                }
            }

            // If data is present, the query is complete
            if stmt_resp.data.is_some() && stmt_resp.result_set_meta_data.is_some() {
                return Ok(stmt_resp);
            }

            delay_ms = (delay_ms * 2).min(max_delay_ms);
        }
    }

    /// Fetch a specific result partition.
    async fn fetch_partition(
        &self,
        handle: &str,
        partition: usize,
    ) -> Result<Vec<Vec<Option<String>>>, DiffDonkeyError> {
        let url = format!(
            "{}/api/v2/statements/{}?partition={}",
            self.config.account_url.trim_end_matches('/'),
            handle,
            partition
        );

        let mut req = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .bearer_auth(&self.token);

        if self.is_keypair {
            req = req.header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT_TOKEN");
        }

        let resp = req
            .send()
            .await
            .map_err(|e| DiffDonkeyError::Snowflake(format!("Partition fetch failed: {}", e)))?;

        let text = resp.text().await.map_err(|e| {
            DiffDonkeyError::Snowflake(format!("Failed to read partition response: {}", e))
        })?;

        let stmt_resp: StatementResponse = serde_json::from_str(&text).map_err(|e| {
            DiffDonkeyError::Snowflake(format!("Failed to parse partition response: {}", e))
        })?;

        Ok(stmt_resp.data.unwrap_or_default())
    }
}

// ─── Type Mapping ───────────────────────────────────────────────────────────

/// Map a Snowflake type name to a DuckDB type for CREATE TABLE.
pub fn map_sf_type_to_duckdb(sf_type: &str) -> &'static str {
    match sf_type.to_uppercase().as_str() {
        "FIXED" | "NUMBER" | "DECIMAL" | "NUMERIC" => "DOUBLE",
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => "DOUBLE",
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "BYTEINT" => "BIGINT",
        "BOOLEAN" => "BOOLEAN",
        "DATE" => "DATE",
        "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" | "TIMESTAMP" | "DATETIME" => {
            "TIMESTAMP"
        }
        "TIME" => "VARCHAR",
        "BINARY" | "VARBINARY" => "VARCHAR",
        "VARIANT" | "OBJECT" | "ARRAY" => "VARCHAR",
        // TEXT, VARCHAR, CHAR, STRING, and anything else → VARCHAR
        _ => "VARCHAR",
    }
}

// ─── DuckDB Loading ─────────────────────────────────────────────────────────

/// Load Snowflake query results into a DuckDB table.
pub fn load_results_to_duckdb(
    conn: &duckdb::Connection,
    result: &SnowflakeQueryResult,
    table_name: &str,
    log: &ActivityLog,
) -> Result<TableMeta, DiffDonkeyError> {
    if result.columns.is_empty() {
        return Err(DiffDonkeyError::Snowflake(
            "Query returned no columns".to_string(),
        ));
    }

    // Build CREATE TABLE statement with mapped types
    let col_defs: Vec<String> = result
        .columns
        .iter()
        .map(|c| {
            format!(
                "\"{}\" {}",
                c.name.replace('"', ""),
                map_sf_type_to_duckdb(&c.sf_type)
            )
        })
        .collect();

    let create_sql = format!(
        "CREATE OR REPLACE TABLE \"{}\" ({})",
        table_name,
        col_defs.join(", ")
    );
    activity::execute_logged(conn, &create_sql, "snowflake_create_table", log)?;

    // Insert rows in batches of 1000
    let batch_size = 1000;
    let num_cols = result.columns.len();

    for chunk in result.rows.chunks(batch_size) {
        if chunk.is_empty() {
            continue;
        }

        // Build a batch INSERT with placeholder values
        let placeholders_per_row = (0..num_cols).map(|_| "?").collect::<Vec<_>>().join(", ");
        let all_row_placeholders: Vec<String> = chunk
            .iter()
            .map(|_| format!("({})", placeholders_per_row))
            .collect();

        let insert_sql = format!(
            "INSERT INTO \"{}\" VALUES {}",
            table_name,
            all_row_placeholders.join(", ")
        );

        // Flatten all values into a single params vector
        let params: Vec<Option<String>> = chunk
            .iter()
            .flat_map(|row| {
                // Pad or truncate row to match column count
                (0..num_cols).map(move |i| {
                    row.get(i).cloned().unwrap_or(None)
                })
            })
            .collect();

        // Use duckdb's parameter binding — all values as strings, DuckDB casts
        let param_refs: Vec<&dyn duckdb::ToSql> = params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();

        let start = std::time::Instant::now();
        let mut stmt = conn.prepare(&insert_sql)?;
        stmt.execute(param_refs.as_slice())?;
        let duration = start.elapsed().as_millis() as u64;

        log.log_query(
            "snowflake_insert_batch",
            &format!("INSERT INTO {} ({} rows)", table_name, chunk.len()),
            duration,
            Some(chunk.len() as i64),
            None,
        );
    }

    db_loader::get_table_meta(conn, table_name)
}

// ─── Public Entry Point ─────────────────────────────────────────────────────

/// Execute a Snowflake query via REST API (HTTP only — no DuckDB access).
///
/// Returns the raw query result that can then be loaded into DuckDB separately.
/// This split allows callers to hold the DuckDB lock only during the load phase.
pub async fn fetch_snowflake(
    config: SnowflakeConfig,
    auth: SnowflakeAuth,
    sql: &str,
) -> Result<SnowflakeQueryResult, DiffDonkeyError> {
    if sql.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "SQL query cannot be empty".to_string(),
        ));
    }

    let client = SnowflakeClient::new(config, auth).await?;
    client.execute_query(sql).await
}

/// Execute a Snowflake metadata query and return the first column as strings.
///
/// Used for catalog browsing (listing databases, schemas, tables).
pub async fn fetch_snowflake_metadata(
    config: SnowflakeConfig,
    auth: SnowflakeAuth,
    sql: &str,
) -> Result<Vec<String>, DiffDonkeyError> {
    if sql.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Metadata query cannot be empty".to_string(),
        ));
    }

    let client = SnowflakeClient::new(config, auth).await?;
    let result = client.execute_query(sql).await?;

    Ok(result
        .rows
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.clone()))
        .collect())
}

/// Build a Snowflake catalog query for the given type.
pub fn build_snowflake_catalog_query(
    catalog_type: &str,
    database: Option<&str>,
    schema: Option<&str>,
) -> Result<String, DiffDonkeyError> {
    match catalog_type {
        "databases" => Ok(
            "SHOW DATABASES".to_string(),
        ),
        "schemas" => {
            let db = database.ok_or_else(|| {
                DiffDonkeyError::Validation("Database is required to list schemas".to_string())
            })?;
            Ok(format!("SHOW SCHEMAS IN DATABASE \"{}\"", db.replace('"', "")))
        }
        "tables" => {
            let db = database.ok_or_else(|| {
                DiffDonkeyError::Validation("Database is required to list tables".to_string())
            })?;
            let sch = schema.ok_or_else(|| {
                DiffDonkeyError::Validation("Schema is required to list tables".to_string())
            })?;
            Ok(format!(
                "SELECT table_name FROM \"{}\".information_schema.tables \
                 WHERE table_schema = '{}' AND table_type IN ('BASE TABLE', 'VIEW') \
                 ORDER BY table_name",
                db.replace('"', ""),
                sch.replace('\'', "''")
            ))
        }
        _ => Err(DiffDonkeyError::Validation(format!(
            "Invalid catalog type '{}' for Snowflake",
            catalog_type
        ))),
    }
}

/// Load data from Snowflake into a local DuckDB table.
///
/// Orchestrates: authenticate → execute query → load results → return metadata.
/// Note: This holds the DuckDB connection for the entire duration including HTTP.
/// Prefer `fetch_snowflake` + `load_results_to_duckdb` for better lock management.
pub async fn load_snowflake(
    config: SnowflakeConfig,
    auth: SnowflakeAuth,
    sql: &str,
    conn: &duckdb::Connection,
    table_name: &str,
    log: &ActivityLog,
) -> Result<TableMeta, DiffDonkeyError> {
    let result = fetch_snowflake(config, auth, sql).await?;
    load_results_to_duckdb(conn, &result, table_name, log)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_account_standard() {
        let result = extract_account("https://myorg-myaccount.snowflakecomputing.com").unwrap();
        assert_eq!(result, "MYORG-MYACCOUNT");
    }

    #[test]
    fn test_extract_account_trailing_slash() {
        let result =
            extract_account("https://myorg-myaccount.snowflakecomputing.com/").unwrap();
        assert_eq!(result, "MYORG-MYACCOUNT");
    }

    #[test]
    fn test_extract_account_no_protocol() {
        let result = extract_account("myorg-myaccount.snowflakecomputing.com").unwrap();
        assert_eq!(result, "MYORG-MYACCOUNT");
    }

    #[test]
    fn test_extract_account_http() {
        let result = extract_account("http://myorg-myaccount.snowflakecomputing.com").unwrap();
        assert_eq!(result, "MYORG-MYACCOUNT");
    }

    #[test]
    fn test_extract_account_empty_fails() {
        let result = extract_account("");
        assert!(result.is_err());
    }

    #[test]
    fn test_type_mapping_numeric() {
        assert_eq!(map_sf_type_to_duckdb("FIXED"), "DOUBLE");
        assert_eq!(map_sf_type_to_duckdb("NUMBER"), "DOUBLE");
        assert_eq!(map_sf_type_to_duckdb("REAL"), "DOUBLE");
        assert_eq!(map_sf_type_to_duckdb("FLOAT"), "DOUBLE");
        assert_eq!(map_sf_type_to_duckdb("DECIMAL"), "DOUBLE");
    }

    #[test]
    fn test_type_mapping_integer() {
        assert_eq!(map_sf_type_to_duckdb("INTEGER"), "BIGINT");
        assert_eq!(map_sf_type_to_duckdb("BIGINT"), "BIGINT");
        assert_eq!(map_sf_type_to_duckdb("SMALLINT"), "BIGINT");
    }

    #[test]
    fn test_type_mapping_boolean() {
        assert_eq!(map_sf_type_to_duckdb("BOOLEAN"), "BOOLEAN");
    }

    #[test]
    fn test_type_mapping_date_time() {
        assert_eq!(map_sf_type_to_duckdb("DATE"), "DATE");
        assert_eq!(map_sf_type_to_duckdb("TIMESTAMP_NTZ"), "TIMESTAMP");
        assert_eq!(map_sf_type_to_duckdb("TIMESTAMP_LTZ"), "TIMESTAMP");
        assert_eq!(map_sf_type_to_duckdb("TIMESTAMP_TZ"), "TIMESTAMP");
        assert_eq!(map_sf_type_to_duckdb("TIME"), "VARCHAR");
    }

    #[test]
    fn test_type_mapping_semi_structured() {
        assert_eq!(map_sf_type_to_duckdb("VARIANT"), "VARCHAR");
        assert_eq!(map_sf_type_to_duckdb("OBJECT"), "VARCHAR");
        assert_eq!(map_sf_type_to_duckdb("ARRAY"), "VARCHAR");
    }

    #[test]
    fn test_type_mapping_string() {
        assert_eq!(map_sf_type_to_duckdb("TEXT"), "VARCHAR");
        assert_eq!(map_sf_type_to_duckdb("VARCHAR"), "VARCHAR");
        assert_eq!(map_sf_type_to_duckdb("CHAR"), "VARCHAR");
        assert_eq!(map_sf_type_to_duckdb("STRING"), "VARCHAR");
    }

    #[test]
    fn test_type_mapping_case_insensitive() {
        assert_eq!(map_sf_type_to_duckdb("fixed"), "DOUBLE");
        assert_eq!(map_sf_type_to_duckdb("Boolean"), "BOOLEAN");
        assert_eq!(map_sf_type_to_duckdb("timestamp_ntz"), "TIMESTAMP");
    }

    // ─── Catalog Query Tests ────────────────────────────────────────────

    #[test]
    fn test_snowflake_catalog_query_databases() {
        let sql = build_snowflake_catalog_query("databases", None, None).unwrap();
        assert_eq!(sql, "SHOW DATABASES");
    }

    #[test]
    fn test_snowflake_catalog_query_schemas() {
        let sql = build_snowflake_catalog_query("schemas", Some("MY_DB"), None).unwrap();
        assert!(sql.contains("SHOW SCHEMAS IN DATABASE"));
        assert!(sql.contains("MY_DB"));
    }

    #[test]
    fn test_snowflake_catalog_query_schemas_missing_database() {
        let result = build_snowflake_catalog_query("schemas", None, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Database is required"));
    }

    #[test]
    fn test_snowflake_catalog_query_tables() {
        let sql =
            build_snowflake_catalog_query("tables", Some("MY_DB"), Some("PUBLIC")).unwrap();
        assert!(sql.contains("MY_DB"));
        assert!(sql.contains("table_schema = 'PUBLIC'"));
        assert!(sql.contains("information_schema.tables"));
        assert!(sql.contains("ORDER BY table_name"));
    }

    #[test]
    fn test_snowflake_catalog_query_tables_missing_schema() {
        let result = build_snowflake_catalog_query("tables", Some("MY_DB"), None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Schema is required"));
    }

    #[test]
    fn test_snowflake_catalog_query_tables_missing_database() {
        let result = build_snowflake_catalog_query("tables", None, Some("PUBLIC"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Database is required"));
    }

    #[test]
    fn test_snowflake_catalog_type_validation() {
        let result = build_snowflake_catalog_query("invalid", None, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid catalog type"));
    }

    #[test]
    fn test_snowflake_catalog_query_sql_injection_protection() {
        let sql = build_snowflake_catalog_query(
            "tables",
            Some("MY_DB"),
            Some("PUBLIC'; DROP TABLE x; --"),
        )
        .unwrap();
        // Single quotes should be escaped
        assert!(sql.contains("PUBLIC''; DROP TABLE x; --"));
    }

    #[test]
    fn test_type_mapping_binary() {
        assert_eq!(map_sf_type_to_duckdb("BINARY"), "VARCHAR");
        assert_eq!(map_sf_type_to_duckdb("VARBINARY"), "VARCHAR");
    }

    #[test]
    fn test_load_results_to_duckdb_basic() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let log = ActivityLog::new();

        let result = SnowflakeQueryResult {
            columns: vec![
                SnowflakeColumn {
                    name: "id".to_string(),
                    sf_type: "FIXED".to_string(),
                },
                SnowflakeColumn {
                    name: "name".to_string(),
                    sf_type: "TEXT".to_string(),
                },
                SnowflakeColumn {
                    name: "active".to_string(),
                    sf_type: "BOOLEAN".to_string(),
                },
            ],
            rows: vec![
                vec![
                    Some("1".to_string()),
                    Some("Alice".to_string()),
                    Some("true".to_string()),
                ],
                vec![
                    Some("2".to_string()),
                    Some("Bob".to_string()),
                    Some("false".to_string()),
                ],
                vec![
                    Some("3".to_string()),
                    None,
                    Some("true".to_string()),
                ],
            ],
        };

        let meta = load_results_to_duckdb(&conn, &result, "test_sf", &log).unwrap();

        assert_eq!(meta.table_name, "test_sf");
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.columns.len(), 3);
        assert_eq!(meta.columns[0].name, "id");
        assert_eq!(meta.columns[0].data_type, "DOUBLE");
        assert_eq!(meta.columns[1].name, "name");
        assert_eq!(meta.columns[1].data_type, "VARCHAR");
        assert_eq!(meta.columns[2].name, "active");
        assert_eq!(meta.columns[2].data_type, "BOOLEAN");

        // Verify data was loaded
        let count: usize = conn
            .query_row("SELECT COUNT(*) FROM test_sf", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_load_results_empty_columns_error() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let log = ActivityLog::new();

        let result = SnowflakeQueryResult {
            columns: vec![],
            rows: vec![],
        };

        let err = load_results_to_duckdb(&conn, &result, "test_sf", &log);
        assert!(err.is_err());
    }

    #[test]
    fn test_load_results_empty_rows() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let log = ActivityLog::new();

        let result = SnowflakeQueryResult {
            columns: vec![SnowflakeColumn {
                name: "id".to_string(),
                sf_type: "FIXED".to_string(),
            }],
            rows: vec![],
        };

        let meta = load_results_to_duckdb(&conn, &result, "test_sf", &log).unwrap();
        assert_eq!(meta.row_count, 0);
    }

    #[test]
    fn test_jwt_generation() {
        // Generate a test RSA key pair
        use rsa::pkcs8::EncodePrivateKey;

        let mut rng = rsa::rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = private_key
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap();

        let token = authenticate_keypair(
            "https://myorg-myaccount.snowflakecomputing.com",
            "testuser",
            pem.as_ref(),
        )
        .unwrap();

        // JWT should be three dot-separated base64 parts
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 parts");

        // Decode the payload (middle part) to verify claims
        let payload_bytes = BASE64
            .decode(format!(
                "{}{}",
                parts[1],
                // Add padding if needed
                match parts[1].len() % 4 {
                    2 => "==",
                    3 => "=",
                    _ => "",
                }
            ))
            // base64url uses - and _ instead of + and /
            .unwrap_or_else(|_| {
                use base64::engine::general_purpose::URL_SAFE_NO_PAD;
                URL_SAFE_NO_PAD.decode(parts[1]).unwrap()
            });

        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();

        // Verify claims structure
        assert!(payload["iss"].as_str().unwrap().starts_with("MYORG-MYACCOUNT.TESTUSER.SHA256:"));
        assert_eq!(
            payload["sub"].as_str().unwrap(),
            "MYORG-MYACCOUNT.TESTUSER"
        );
        assert!(payload["iat"].as_u64().is_some());
        assert!(payload["exp"].as_u64().is_some());
        assert!(
            payload["exp"].as_u64().unwrap() > payload["iat"].as_u64().unwrap(),
            "exp should be after iat"
        );
    }
}
