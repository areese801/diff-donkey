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

/// Credentials for accessing remote storage (S3, GCS).
/// All fields are optional — when omitted, DuckDB uses credential chain
/// (IAM roles, environment variables, instance metadata).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RemoteCredentials {
    pub provider: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

/// SECURITY: Escape a string for use in a SQL single-quoted literal.
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Redact credential values for activity logging.
/// Replaces key values with *** so secrets don't appear in the activity log.
fn redact_credentials(sql: &str) -> String {
    // Redact KEY_ID, SECRET, KEY values in CREATE SECRET statements
    let mut result = sql.to_string();
    for keyword in &["KEY_ID", "SECRET", "KEY"] {
        // Match pattern: KEY_ID 'value' -> KEY_ID '***'
        if let Some(start) = result.find(&format!("{} '", keyword)) {
            let key_prefix_len = keyword.len() + 2; // "KEY_ID '"
            let after_key = start + key_prefix_len;
            if let Some(end_quote) = result[after_key..].find('\'') {
                let end = after_key + end_quote;
                result.replace_range(after_key..end, "***");
            }
        }
    }
    result
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

    // Validate scheme (case-insensitive)
    let uri_lower = uri_trimmed.to_lowercase();
    let valid_schemes = ["s3://", "gs://", "http://", "https://"];
    if !valid_schemes.iter().any(|s| uri_lower.starts_with(s)) {
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

        if let Some(endpoint) = creds.endpoint.as_deref().filter(|e| !e.is_empty()) {
            let endpoint = escape_sql_string(endpoint);
            sql.push_str(&format!(",\n    ENDPOINT '{}'", endpoint));
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

    // Configure credentials based on URI scheme (case-insensitive)
    let uri_lower = uri.to_lowercase();
    if uri_lower.starts_with("s3://") {
        let cred_sql = build_s3_credential_sql(credentials);
        let redacted = redact_credentials(&cred_sql);
        // Log the redacted version, execute the real one
        log.log_query("configure_s3_credentials", &redacted, 0, None, None);
        conn.execute_batch(&cred_sql)
            .map_err(DiffDonkeyError::DuckDb)?;
    } else if uri_lower.starts_with("gs://") {
        let cred_sql = build_gcs_credential_sql(credentials);
        let redacted = redact_credentials(&cred_sql);
        log.log_query("configure_gcs_credentials", &redacted, 0, None, None);
        conn.execute_batch(&cred_sql)
            .map_err(DiffDonkeyError::DuckDb)?;
    }
    // HTTP/HTTPS: no credentials needed

    // Normalize scheme to lowercase for DuckDB compatibility
    let normalized_uri = if let Some(idx) = uri.find("://") {
        format!("{}{}", uri[..idx].to_lowercase(), &uri[idx..])
    } else {
        uri.to_string()
    };

    // Build the load SQL based on file type
    let escaped_uri = escape_sql_string(&normalized_uri);
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
}
