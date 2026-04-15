/// Remote file loading — loads CSV/Parquet files from S3, GCS, or HTTP URLs
/// using DuckDB's httpfs extension.
///
/// DuckDB's httpfs extension supports streaming remote files via range requests,
/// so Parquet files don't need to be fully downloaded before querying.
use duckdb::Connection;
use serde::Deserialize;

use crate::activity::{self, ActivityLog};
use crate::error::DiffDonkeyError;
use crate::types::{ColumnInfo, TableMeta};

/// Supported remote file types, detected from URI extension.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoteFileType {
    Parquet,
    Csv,
    Json,
}

/// Credentials for accessing remote storage (S3, GCS, private HTTPS).
/// All fields are optional — when omitted, DuckDB uses credential chain
/// (IAM roles, environment variables, instance metadata).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RemoteCredentials {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// AWS STS session token for temporary credentials
    #[serde(default)]
    pub session_token: Option<String>,
    /// URL style for S3-compatible endpoints (`"path"` or `"vhost"`)
    #[serde(default)]
    pub url_style: Option<String>,
    /// Override TLS for S3-compatible endpoints
    #[serde(default)]
    pub use_ssl: Option<bool>,
    /// Bearer token for private HTTP/HTTPS endpoints
    #[serde(default)]
    pub bearer_token: Option<String>,
}

/// SECURITY: Escape a string for use in a SQL single-quoted literal.
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Redact credential values for activity logging.
/// Replaces secret values with *** so they don't appear in the activity log.
///
/// Handles all occurrences of each keyword (not just the first) so that
/// multi-secret statements are fully redacted. Also redacts bearer tokens
/// embedded in EXTRA_HTTP_HEADERS.
fn redact_credentials(sql: &str) -> String {
    let mut result = sql.to_string();
    // KEY_ID 'v' / SECRET 'v' / SESSION_TOKEN 'v' / KEY 'v' → '***'
    // Keywords processed in order; each needle has a trailing space + quote to
    // avoid accidental overlap (e.g. KEY_ID does NOT match "KEY '").
    for keyword in &["KEY_ID", "SECRET", "SESSION_TOKEN", "KEY"] {
        let needle = format!("{} '", keyword);
        let mut search_from = 0;
        while let Some(rel) = result[search_from..].find(&needle) {
            let start = search_from + rel;
            let after_key = start + needle.len();
            if let Some(end_quote) = result[after_key..].find('\'') {
                let end = after_key + end_quote;
                result.replace_range(after_key..end, "***");
                // Advance past the replacement to avoid re-matching.
                search_from = after_key + 3;
            } else {
                break;
            }
        }
    }

    // Redact bearer tokens: 'Bearer XYZ' → 'Bearer ***'
    let needle = "'Bearer ";
    let mut out = String::with_capacity(result.len());
    let mut cursor = 0;
    while let Some(rel) = result[cursor..].find(needle) {
        let abs = cursor + rel;
        let after = abs + needle.len();
        match result[after..].find('\'') {
            Some(end_rel) => {
                let end = after + end_rel;
                out.push_str(&result[cursor..after]);
                out.push_str("***");
                cursor = end;
            }
            None => break,
        }
    }
    out.push_str(&result[cursor..]);
    out
}

/// Validate a remote URI — must use a supported scheme and have a recognized file extension.
///
/// Returns the detected file type on success.
pub fn validate_uri(uri: &str) -> Result<RemoteFileType, DiffDonkeyError> {
    let uri_trimmed = uri.trim();

    if uri_trimmed.is_empty() {
        return Err(DiffDonkeyError::Validation("URI cannot be empty".to_string()));
    }

    // SECURITY: No semicolons allowed — prevents SQL injection via URI
    if uri_trimmed.contains(';') {
        return Err(DiffDonkeyError::Validation(
            "URI must not contain semicolons".to_string(),
        ));
    }

    // Validate scheme
    let valid_schemes = ["s3://", "gs://", "http://", "https://"];
    if !valid_schemes.iter().any(|s| uri_trimmed.starts_with(s)) {
        return Err(DiffDonkeyError::Validation(
            "URI must start with s3://, gs://, http://, or https://".to_string(),
        ));
    }

    // Detect file type from extension
    detect_file_type(uri_trimmed)
}

/// Detect file type from URI extension.
/// Strips query parameters before checking the extension.
pub fn detect_file_type(uri: &str) -> Result<RemoteFileType, DiffDonkeyError> {
    // Strip query parameters for extension detection
    let path = uri.split('?').next().unwrap_or(uri);
    let lower = path.to_lowercase();

    if lower.ends_with(".parquet") || lower.ends_with(".pq") {
        Ok(RemoteFileType::Parquet)
    } else if lower.ends_with(".csv") {
        Ok(RemoteFileType::Csv)
    } else if lower.ends_with(".json") || lower.ends_with(".jsonl") {
        Ok(RemoteFileType::Json)
    } else {
        Err(DiffDonkeyError::Validation(
            "Unsupported file type. URI must end with .parquet, .pq, .csv, .json, or .jsonl"
                .to_string(),
        ))
    }
}

/// Generate the CREATE SECRET SQL for S3 credentials.
pub fn build_s3_credential_sql(creds: &RemoteCredentials) -> String {
    let has_keys = creds.access_key.as_ref().is_some_and(|k| !k.is_empty())
        && creds.secret_key.as_ref().is_some_and(|k| !k.is_empty());

    if has_keys {
        let access_key = escape_sql_string(creds.access_key.as_deref().unwrap_or(""));
        let secret_key = escape_sql_string(creds.secret_key.as_deref().unwrap_or(""));
        let region = creds
            .region
            .as_deref()
            .filter(|r| !r.is_empty())
            .unwrap_or("us-east-1");
        let region = escape_sql_string(region);

        let mut sql = format!(
            "CREATE OR REPLACE SECRET (\n    TYPE S3,\n    KEY_ID '{}',\n    SECRET '{}',\n    REGION '{}'",
            access_key, secret_key, region
        );

        if let Some(token) = creds.session_token.as_deref().filter(|t| !t.is_empty()) {
            let token = escape_sql_string(token);
            sql.push_str(&format!(",\n    SESSION_TOKEN '{}'", token));
        }

        if let Some(endpoint) = creds.endpoint.as_deref().filter(|e| !e.is_empty()) {
            let endpoint = escape_sql_string(endpoint);
            sql.push_str(&format!(",\n    ENDPOINT '{}'", endpoint));
        }

        if let Some(style) = creds.url_style.as_deref().filter(|s| !s.is_empty()) {
            let style = escape_sql_string(style);
            sql.push_str(&format!(",\n    URL_STYLE '{}'", style));
        }

        if let Some(use_ssl) = creds.use_ssl {
            sql.push_str(&format!(",\n    USE_SSL {}", use_ssl));
        }

        sql.push_str("\n)");
        sql
    } else {
        // No explicit credentials — use credential chain (IAM roles, env vars)
        "CREATE OR REPLACE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN)".to_string()
    }
}

/// Generate the CREATE SECRET SQL for GCS credentials.
pub fn build_gcs_credential_sql(creds: &RemoteCredentials) -> String {
    let has_keys = creds.access_key.as_ref().is_some_and(|k| !k.is_empty())
        && creds.secret_key.as_ref().is_some_and(|k| !k.is_empty());

    if has_keys {
        let access_key = escape_sql_string(creds.access_key.as_deref().unwrap_or(""));
        let secret_key = escape_sql_string(creds.secret_key.as_deref().unwrap_or(""));

        format!(
            "CREATE OR REPLACE SECRET (\n    TYPE GCS,\n    KEY_ID '{}',\n    SECRET '{}'\n)",
            access_key, secret_key
        )
    } else {
        "CREATE OR REPLACE SECRET (TYPE GCS, PROVIDER CREDENTIAL_CHAIN)".to_string()
    }
}

/// Generate the CREATE SECRET SQL for private HTTP/HTTPS endpoints.
///
/// Returns `None` when no bearer token is provided (public URL — no secret needed).
pub fn build_http_credential_sql(creds: &RemoteCredentials) -> Option<String> {
    let token = creds.bearer_token.as_deref().filter(|t| !t.is_empty())?;
    let token = escape_sql_string(token);
    Some(format!(
        "CREATE OR REPLACE SECRET (\n    TYPE HTTP,\n    EXTRA_HTTP_HEADERS MAP {{'Authorization': 'Bearer {}'}}\n)",
        token
    ))
}

/// Load a remote file (S3, GCS, or HTTP) into a DuckDB table.
///
/// Steps:
/// 1. Validate the URI and detect file type
/// 2. Install and load the httpfs extension
/// 3. Configure credentials if needed (S3/GCS)
/// 4. Execute CREATE TABLE AS SELECT * FROM read_parquet/read_csv_auto
/// 5. Return table metadata
pub fn load_remote(
    conn: &Connection,
    uri: &str,
    table_name: &str,
    credentials: &RemoteCredentials,
    log: &ActivityLog,
) -> Result<TableMeta, DiffDonkeyError> {
    let file_type = validate_uri(uri)?;

    // Install and load httpfs extension
    let install_sql = "INSTALL httpfs; LOAD httpfs;";
    activity::execute_logged(conn, install_sql, "install_httpfs", log)?;

    // Configure credentials based on URI scheme
    if uri.starts_with("s3://") {
        let cred_sql = build_s3_credential_sql(credentials);
        let redacted = redact_credentials(&cred_sql);
        // Log the redacted version, execute the real one
        log.log_query("configure_s3_credentials", &redacted, 0, None, None);
        conn.execute_batch(&cred_sql)
            .map_err(DiffDonkeyError::DuckDb)?;
    } else if uri.starts_with("gs://") {
        let cred_sql = build_gcs_credential_sql(credentials);
        let redacted = redact_credentials(&cred_sql);
        log.log_query("configure_gcs_credentials", &redacted, 0, None, None);
        conn.execute_batch(&cred_sql)
            .map_err(DiffDonkeyError::DuckDb)?;
    } else if uri.starts_with("http://") || uri.starts_with("https://") {
        if let Some(cred_sql) = build_http_credential_sql(credentials) {
            let redacted = redact_credentials(&cred_sql);
            log.log_query("configure_http_credentials", &redacted, 0, None, None);
            conn.execute_batch(&cred_sql)
                .map_err(DiffDonkeyError::DuckDb)?;
        }
    }

    // Build the load SQL based on file type
    let escaped_uri = escape_sql_string(uri);
    let read_fn = match file_type {
        RemoteFileType::Parquet => "read_parquet",
        RemoteFileType::Csv => "read_csv_auto",
        RemoteFileType::Json => "read_json_auto",
    };

    let sql = format!(
        "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM {}('{}')",
        table_name, read_fn, escaped_uri
    );
    activity::execute_logged(conn, &sql, "load_remote", log)?;

    get_table_meta(conn, table_name)
}

/// Query table metadata — row count and column info.
fn get_table_meta(conn: &Connection, table_name: &str) -> Result<TableMeta, DiffDonkeyError> {
    let row_count: usize = conn.query_row(
        &format!("SELECT COUNT(*) FROM \"{}\"", table_name),
        [],
        |row| row.get(0),
    )?;

    let mut stmt = conn.prepare(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = ? ORDER BY ordinal_position",
    )?;

    let columns: Vec<ColumnInfo> = stmt
        .query_map([table_name], |row| {
            Ok(ColumnInfo {
                name: row.get(0)?,
                data_type: row.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TableMeta {
        table_name: table_name.to_string(),
        row_count,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_validation_s3() {
        let result = validate_uri("s3://my-bucket/data/file.parquet");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RemoteFileType::Parquet);
    }

    #[test]
    fn test_uri_validation_s3_csv() {
        let result = validate_uri("s3://my-bucket/data/file.csv");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RemoteFileType::Csv);
    }

    #[test]
    fn test_uri_validation_gcs() {
        let result = validate_uri("gs://my-bucket/data/file.parquet");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RemoteFileType::Parquet);
    }

    #[test]
    fn test_uri_validation_http() {
        let result = validate_uri("https://example.com/data.parquet");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RemoteFileType::Parquet);
    }

    #[test]
    fn test_uri_validation_http_csv() {
        let result = validate_uri("https://example.com/data.csv");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RemoteFileType::Csv);
    }

    #[test]
    fn test_uri_validation_invalid_scheme() {
        let result = validate_uri("ftp://example.com/data.csv");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must start with"));
    }

    #[test]
    fn test_uri_validation_empty() {
        let result = validate_uri("");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be empty"));
    }

    #[test]
    fn test_uri_validation_unsupported_extension() {
        let result = validate_uri("s3://bucket/file.txt");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unsupported file type"));
    }

    #[test]
    fn test_uri_file_type_detection_parquet() {
        assert_eq!(
            detect_file_type("s3://b/file.parquet").unwrap(),
            RemoteFileType::Parquet
        );
        assert_eq!(
            detect_file_type("s3://b/file.pq").unwrap(),
            RemoteFileType::Parquet
        );
    }

    #[test]
    fn test_uri_file_type_detection_csv() {
        assert_eq!(
            detect_file_type("s3://b/file.csv").unwrap(),
            RemoteFileType::Csv
        );
    }

    #[test]
    fn test_uri_file_type_detection_json() {
        assert_eq!(
            detect_file_type("s3://b/file.json").unwrap(),
            RemoteFileType::Json
        );
        assert_eq!(
            detect_file_type("s3://b/file.jsonl").unwrap(),
            RemoteFileType::Json
        );
    }

    #[test]
    fn test_uri_file_type_detection_with_query_params() {
        let result = detect_file_type("https://example.com/data.parquet?token=abc123");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RemoteFileType::Parquet);
    }

    #[test]
    fn test_uri_semicolon_rejected() {
        let result = validate_uri("s3://bucket/file.parquet; DROP TABLE x;");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("semicolons"));
    }

    #[test]
    fn test_credential_sql_generation_s3() {
        let creds = RemoteCredentials {
            provider: Some("s3".to_string()),
            access_key: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            region: Some("us-west-2".to_string()),
            endpoint: None,
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("TYPE S3"));
        assert!(sql.contains("KEY_ID 'AKIAIOSFODNN7EXAMPLE'"));
        assert!(sql.contains("SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'"));
        assert!(sql.contains("REGION 'us-west-2'"));
        assert!(!sql.contains("ENDPOINT"));
    }

    #[test]
    fn test_credential_sql_gcs() {
        let creds = RemoteCredentials {
            provider: Some("gcs".to_string()),
            access_key: Some("GOOG1EEXAMPLE".to_string()),
            secret_key: Some("gcs-secret-key".to_string()),
            region: None,
            endpoint: None,
            ..Default::default()
        };

        let sql = build_gcs_credential_sql(&creds);
        assert!(sql.contains("TYPE GCS"));
        assert!(sql.contains("KEY_ID 'GOOG1EEXAMPLE'"));
        assert!(sql.contains("SECRET 'gcs-secret-key'"));
    }

    #[test]
    fn test_no_credentials_uses_chain() {
        let creds = RemoteCredentials::default();

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("PROVIDER CREDENTIAL_CHAIN"));
        assert!(!sql.contains("KEY_ID"));
    }

    #[test]
    fn test_no_credentials_uses_chain_gcs() {
        let creds = RemoteCredentials::default();

        let sql = build_gcs_credential_sql(&creds);
        assert!(sql.contains("PROVIDER CREDENTIAL_CHAIN"));
        assert!(!sql.contains("KEY_ID"));
    }

    #[test]
    fn test_custom_endpoint() {
        let creds = RemoteCredentials {
            provider: Some("s3".to_string()),
            access_key: Some("minioadmin".to_string()),
            secret_key: Some("minioadmin".to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some("localhost:9000".to_string()),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("ENDPOINT 'localhost:9000'"));
    }

    #[test]
    fn test_redact_credentials() {
        let sql = "CREATE OR REPLACE SECRET (\n    TYPE S3,\n    KEY_ID 'AKIAIOSFODNN7EXAMPLE',\n    SECRET 'wJalrXUtnFEMI',\n    REGION 'us-west-2'\n)";
        let redacted = redact_credentials(sql);
        assert!(redacted.contains("KEY_ID '***'"));
        assert!(redacted.contains("SECRET '***'"));
        assert!(!redacted.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!redacted.contains("wJalrXUtnFEMI"));
    }

    #[test]
    fn test_default_region() {
        let creds = RemoteCredentials {
            access_key: Some("key".to_string()),
            secret_key: Some("secret".to_string()),
            region: None,
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("REGION 'us-east-1'"));
    }

    #[test]
    fn test_escape_sql_in_credentials() {
        let creds = RemoteCredentials {
            access_key: Some("key'with'quotes".to_string()),
            secret_key: Some("secret".to_string()),
            region: None,
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("key''with''quotes"));
    }

    // ─── Session token ─────────────────────────────────────────────────

    #[test]
    fn test_s3_sql_with_session_token() {
        let creds = RemoteCredentials {
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            session_token: Some("FwoGZX...token".to_string()),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("SESSION_TOKEN 'FwoGZX...token'"));
        assert!(sql.contains("KEY_ID 'AKID'"));
    }

    #[test]
    fn test_s3_sql_without_session_token() {
        let creds = RemoteCredentials {
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(!sql.contains("SESSION_TOKEN"));
    }

    #[test]
    fn test_s3_sql_empty_session_token_ignored() {
        let creds = RemoteCredentials {
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            session_token: Some("".to_string()),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(!sql.contains("SESSION_TOKEN"));
    }

    // ─── S3-compatible endpoint flags ───────────────────────────────────

    #[test]
    fn test_s3_sql_with_url_style_path() {
        let creds = RemoteCredentials {
            access_key: Some("minioadmin".to_string()),
            secret_key: Some("minioadmin".to_string()),
            endpoint: Some("localhost:9000".to_string()),
            url_style: Some("path".to_string()),
            use_ssl: Some(false),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("URL_STYLE 'path'"));
        assert!(sql.contains("USE_SSL false"));
        assert!(sql.contains("ENDPOINT 'localhost:9000'"));
    }

    #[test]
    fn test_s3_sql_use_ssl_true() {
        let creds = RemoteCredentials {
            access_key: Some("key".to_string()),
            secret_key: Some("secret".to_string()),
            use_ssl: Some(true),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(sql.contains("USE_SSL true"));
    }

    #[test]
    fn test_s3_sql_no_ssl_flag_when_none() {
        let creds = RemoteCredentials {
            access_key: Some("key".to_string()),
            secret_key: Some("secret".to_string()),
            ..Default::default()
        };

        let sql = build_s3_credential_sql(&creds);
        assert!(!sql.contains("USE_SSL"));
    }

    // ─── HTTP bearer token ──────────────────────────────────────────────

    #[test]
    fn test_http_sql_with_bearer_token() {
        let creds = RemoteCredentials {
            bearer_token: Some("ghp_abc123XYZ".to_string()),
            ..Default::default()
        };

        let sql = build_http_credential_sql(&creds);
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.contains("TYPE HTTP"));
        assert!(sql.contains("'Bearer ghp_abc123XYZ'"));
    }

    #[test]
    fn test_http_sql_without_bearer_token_returns_none() {
        let creds = RemoteCredentials::default();
        assert!(build_http_credential_sql(&creds).is_none());
    }

    #[test]
    fn test_http_sql_empty_bearer_token_returns_none() {
        let creds = RemoteCredentials {
            bearer_token: Some("".to_string()),
            ..Default::default()
        };
        assert!(build_http_credential_sql(&creds).is_none());
    }

    // ─── Redaction ──────────────────────────────────────────────────────

    #[test]
    fn test_redact_session_token() {
        let creds = RemoteCredentials {
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            session_token: Some("FwoGZXtoken".to_string()),
            ..Default::default()
        };
        let sql = build_s3_credential_sql(&creds);
        let redacted = redact_credentials(&sql);
        assert!(redacted.contains("SESSION_TOKEN '***'"));
        assert!(!redacted.contains("FwoGZXtoken"));
    }

    #[test]
    fn test_redact_bearer_token() {
        let creds = RemoteCredentials {
            bearer_token: Some("ghp_secret123".to_string()),
            ..Default::default()
        };
        let sql = build_http_credential_sql(&creds).unwrap();
        let redacted = redact_credentials(&sql);
        assert!(redacted.contains("'Bearer ***'"));
        assert!(!redacted.contains("ghp_secret123"));
    }
}
