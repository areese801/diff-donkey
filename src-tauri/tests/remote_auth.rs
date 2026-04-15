//! Integration tests for remote file loading with authentication.
//!
//! These tests require Docker to be running and are gated behind the
//! `integration` feature flag. Run with:
//!
//! ```sh
//! cargo test --features integration --test remote_auth
//! ```
//!
//! Tests spin up MinIO (S3-compatible) via testcontainers and a hand-rolled
//! HTTP server to exercise:
//! - Static S3 credentials (happy path)
//! - STS session tokens
//! - Private HTTPS with bearer token auth
//! - S3-compatible endpoint flags (USE_SSL, URL_STYLE)

#[cfg(feature = "integration")]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;
    use duckdb::Connection;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::minio::MinIO;

    use diff_donkey_lib::activity::ActivityLog;
    use diff_donkey_lib::remote_loader::{self, RemoteCredentials};

    const MINIO_USER: &str = "minioadmin";
    const MINIO_PASS: &str = "minioadmin";
    const TEST_BUCKET: &str = "test-data";
    const TEST_CSV: &str = "id,name,value\n1,Alice,100\n2,Bob,200\n3,Carol,300\n";

    /// Upload a CSV fixture to a MinIO bucket and return the S3 URI.
    async fn upload_fixture(endpoint: &str, bucket: &str, key: &str, body: &[u8]) -> String {
        let creds = Credentials::new(MINIO_USER, MINIO_PASS, None, None, "test");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(creds)
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(config);

        // Create bucket (ignore if already exists)
        let _ = client.create_bucket().bucket(bucket).send().await;

        // Upload object
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .expect("Failed to upload fixture to MinIO");

        format!("s3://{}/{}", bucket, key)
    }

    /// Create a DuckDB in-memory connection for testing.
    fn test_conn() -> Connection {
        Connection::open_in_memory().expect("Failed to open in-memory DuckDB")
    }

    // ─── S3 with static keys via MinIO ──────────────────────────────────

    #[tokio::test]
    async fn test_minio_load_static_keys() {
        let container = MinIO::default().start().await.expect("MinIO container failed to start");
        let port = container.get_host_port_ipv4(9000).await.expect("Failed to get MinIO port");
        let endpoint = format!("http://127.0.0.1:{}", port);

        let s3_uri = upload_fixture(&endpoint, TEST_BUCKET, "orders.csv", TEST_CSV.as_bytes()).await;

        let conn = test_conn();
        let log = ActivityLog::new();

        let creds = RemoteCredentials {
            provider: Some("s3".to_string()),
            access_key: Some(MINIO_USER.to_string()),
            secret_key: Some(MINIO_PASS.to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some(format!("127.0.0.1:{}", port)),
            url_style: Some("path".to_string()),
            use_ssl: Some(false),
            ..Default::default()
        };

        let meta = remote_loader::load_remote(&conn, &s3_uri, "source_a", &creds, &log)
            .expect("load_remote should succeed against MinIO");

        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.columns.len(), 3);
        assert_eq!(meta.columns[0].name, "id");
    }

    // ─── Session token SQL acceptance ─────────────────────────────────

    /// Verify that DuckDB accepts the SESSION_TOKEN syntax in a CREATE SECRET.
    ///
    /// We can't test with a *real* session token against MinIO without calling
    /// MinIO's STS API (it validates tokens, unlike access_key/secret_key which
    /// are just the root creds). Instead we verify that:
    /// 1. The SQL we generate is syntactically valid — DuckDB executes it.
    /// 2. A subsequent load with the root creds (no token) still works.
    ///
    /// Unit tests in remote_loader.rs already verify the SQL string shape.
    #[tokio::test]
    async fn test_session_token_sql_accepted_by_duckdb() {
        let container = MinIO::default().start().await.expect("MinIO container failed to start");
        let port = container.get_host_port_ipv4(9000).await.expect("Failed to get MinIO port");
        let endpoint = format!("http://127.0.0.1:{}", port);

        upload_fixture(&endpoint, TEST_BUCKET, "orders_st.csv", TEST_CSV.as_bytes()).await;

        let conn = test_conn();

        // Install httpfs so we can test secret creation
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;").unwrap();

        // Build the SECRET SQL with a session token and verify DuckDB accepts it
        let creds = RemoteCredentials {
            provider: Some("s3".to_string()),
            access_key: Some(MINIO_USER.to_string()),
            secret_key: Some(MINIO_PASS.to_string()),
            session_token: Some("FwoGZXIvYXdz...example-session-token".to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some(format!("127.0.0.1:{}", port)),
            url_style: Some("path".to_string()),
            use_ssl: Some(false),
            ..Default::default()
        };

        let sql = remote_loader::build_s3_credential_sql(&creds);
        assert!(sql.contains("SESSION_TOKEN"));

        // DuckDB should accept this SQL without error (validates syntax)
        conn.execute_batch(&sql)
            .expect("DuckDB should accept CREATE SECRET with SESSION_TOKEN");

        // Now overwrite with valid root creds (no token) and do a real load
        // to confirm the connection still works after a token-bearing secret
        let log = ActivityLog::new();
        let valid_creds = RemoteCredentials {
            provider: Some("s3".to_string()),
            access_key: Some(MINIO_USER.to_string()),
            secret_key: Some(MINIO_PASS.to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some(format!("127.0.0.1:{}", port)),
            url_style: Some("path".to_string()),
            use_ssl: Some(false),
            ..Default::default()
        };

        let s3_uri = format!("s3://{}/orders_st.csv", TEST_BUCKET);
        let meta = remote_loader::load_remote(&conn, &s3_uri, "source_a", &valid_creds, &log)
            .expect("load_remote should succeed after session token secret was replaced");

        assert_eq!(meta.row_count, 3);
    }

    // ─── Private HTTP with bearer token ─────────────────────────────────

    /// Spawn a minimal HTTP server that requires a bearer token and serves CSV.
    /// Returns the port number. The server handles one request then shuts down.
    fn spawn_auth_http_server(csv_body: &'static str, expected_token: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
        let port = listener.local_addr().unwrap().port();

        thread::spawn(move || {
            // Accept up to 5 connections (DuckDB httpfs may probe with HEAD first)
            for _ in 0..5 {
                if let Ok(mut stream) = listener.accept().map(|(s, _)| s) {
                    handle_http_conn(&mut stream, csv_body.as_bytes(), expected_token);
                }
            }
        });

        port
    }

    fn handle_http_conn(stream: &mut TcpStream, csv: &[u8], expected_token: &str) {
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);

        // Check for Authorization header
        let auth_ok = req.lines().any(|line| {
            let lower = line.to_lowercase();
            lower.starts_with("authorization:")
                && line.contains(&format!("Bearer {}", expected_token))
        });

        if !auth_ok {
            let resp = b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(resp);
            return;
        }

        // Check if HEAD request (DuckDB probes with HEAD)
        let is_head = req.starts_with("HEAD ");

        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/csv\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
            csv.len()
        );
        let _ = stream.write_all(header.as_bytes());
        if !is_head {
            let _ = stream.write_all(csv);
        }
    }

    #[test]
    fn test_http_load_with_bearer_token() {
        let token = "ghp_testtoken123";
        let port = spawn_auth_http_server(TEST_CSV, "ghp_testtoken123");

        let uri = format!("http://127.0.0.1:{}/data.csv", port);

        let conn = test_conn();
        let log = ActivityLog::new();

        let creds = RemoteCredentials {
            bearer_token: Some(token.to_string()),
            ..Default::default()
        };

        let meta = remote_loader::load_remote(&conn, &uri, "source_a", &creds, &log)
            .expect("load_remote should succeed with bearer token");

        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.columns.len(), 3);
    }

    #[test]
    fn test_http_load_rejects_wrong_token() {
        let port = spawn_auth_http_server(TEST_CSV, "correct-token");

        let uri = format!("http://127.0.0.1:{}/data.csv", port);

        let conn = test_conn();
        let log = ActivityLog::new();

        let creds = RemoteCredentials {
            bearer_token: Some("wrong-token".to_string()),
            ..Default::default()
        };

        let result = remote_loader::load_remote(&conn, &uri, "source_a", &creds, &log);
        assert!(result.is_err(), "load_remote should fail with wrong bearer token");
    }
}
