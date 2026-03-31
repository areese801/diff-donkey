/// SSH tunnel — local port forwarding through a bastion host.
///
/// Implements the equivalent of `ssh -L local_port:remote_host:remote_port bastion`.
/// DuckDB's postgres/mysql extensions open their own TCP connections to the database,
/// so we bind a local port and forward all incoming connections through the SSH channel.
///
/// Uses `russh` (pure Rust, no C dependencies) for the SSH protocol implementation.
/// We chose russh over ssh2 because ssh2 requires libssh2-dev (C library).
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use russh::client;
use russh_keys::key;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use crate::error::DiffDonkeyError;

/// Configuration for establishing an SSH tunnel.
#[derive(Debug)]
pub struct SshTunnelConfig {
    pub ssh_host: String,
    pub ssh_port: u16,
    pub ssh_username: String,
    pub auth: SshAuth,
    pub remote_host: String,
    pub remote_port: u16,
}

/// SSH authentication method.
#[derive(Debug)]
pub enum SshAuth {
    Password(String),
    KeyFile {
        path: String,
        passphrase: Option<String>,
    },
}

/// A running SSH tunnel. The tunnel stays alive as long as this struct exists.
/// When dropped, the background forwarding task is signaled to stop.
pub struct SshTunnel {
    pub local_port: u16,
    shutdown: Arc<AtomicBool>,
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

/// Minimal russh client handler — accepts all host keys.
///
/// In a desktop app where the user explicitly configures the SSH host,
/// host key verification is less critical than in automated systems.
/// A future enhancement could store known host keys and verify them.
struct ClientHandler;

#[async_trait::async_trait]
impl client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &key::PublicKey,
    ) -> Result<bool, Self::Error> {
        // Accept all host keys (equivalent to StrictHostKeyChecking=no)
        Ok(true)
    }
}

/// Start an SSH tunnel, returning the local port to connect to.
///
/// This function:
/// 1. Binds a random local port on 127.0.0.1
/// 2. Connects to the SSH server and authenticates
/// 3. Spawns a background thread with its own tokio runtime that:
///    - Accepts local TCP connections
///    - Opens SSH direct-tcpip channels for each connection
///    - Bidirectionally copies data between local TCP and SSH channel
/// 4. Returns an SshTunnel holding the local port and a shutdown handle
pub fn start_tunnel(config: &SshTunnelConfig) -> Result<SshTunnel, DiffDonkeyError> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| DiffDonkeyError::Ssh(format!("Failed to create async runtime: {}", e)))?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    // Bind local listener (port 0 = OS picks a free port)
    let listener = rt.block_on(async {
        TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| DiffDonkeyError::Ssh(format!("Failed to bind local port: {}", e)))
    })?;

    let local_port = listener
        .local_addr()
        .map_err(|e| DiffDonkeyError::Ssh(format!("Failed to get local address: {}", e)))?
        .port();

    // Connect to SSH server and authenticate
    let ssh_config = Arc::new(russh::client::Config::default());
    let ssh_addr = format!("{}:{}", config.ssh_host, config.ssh_port);
    let username = config.ssh_username.clone();
    let remote_host = config.remote_host.clone();
    let remote_port = config.remote_port;

    let session = rt.block_on(async {
        let handler = ClientHandler;
        let mut session = client::connect(ssh_config, &ssh_addr, handler)
            .await
            .map_err(|e| DiffDonkeyError::Ssh(format!("SSH connection failed: {}", e)))?;

        match &config.auth {
            SshAuth::Password(password) => {
                let auth_ok = session
                    .authenticate_password(&username, password)
                    .await
                    .map_err(|e| {
                        DiffDonkeyError::Ssh(format!("SSH password auth failed: {}", e))
                    })?;
                if !auth_ok {
                    return Err(DiffDonkeyError::Ssh(
                        "SSH authentication rejected — check username and password".to_string(),
                    ));
                }
            }
            SshAuth::KeyFile { path, passphrase } => {
                let key_pair =
                    russh_keys::load_secret_key(path, passphrase.as_deref()).map_err(|e| {
                        DiffDonkeyError::Ssh(format!("Failed to load SSH key '{}': {}", path, e))
                    })?;
                let auth_ok = session
                    .authenticate_publickey(&username, Arc::new(key_pair))
                    .await
                    .map_err(|e| DiffDonkeyError::Ssh(format!("SSH key auth failed: {}", e)))?;
                if !auth_ok {
                    return Err(DiffDonkeyError::Ssh(
                        "SSH key authentication rejected — check username and key file".to_string(),
                    ));
                }
            }
        }

        Ok(session)
    })?;

    // Spawn background thread to accept and forward connections.
    // Each accepted connection gets its own SSH direct-tcpip channel.
    let session_handle = Arc::new(session);

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tunnel runtime");
        rt.block_on(async move {
            loop {
                if shutdown_clone.load(Ordering::Relaxed) {
                    break;
                }

                // Accept with a timeout so we can check the shutdown flag periodically
                let accept_result =
                    tokio::time::timeout(std::time::Duration::from_secs(1), listener.accept())
                        .await;

                match accept_result {
                    Ok(Ok((local_stream, _addr))) => {
                        let session = session_handle.clone();
                        let rhost = remote_host.clone();
                        let rport = remote_port;

                        tokio::spawn(async move {
                            if let Err(e) =
                                forward_connection(local_stream, session, &rhost, rport).await
                            {
                                eprintln!("SSH tunnel forward error: {}", e);
                            }
                        });
                    }
                    Ok(Err(e)) => {
                        eprintln!("SSH tunnel accept error: {}", e);
                    }
                    Err(_) => {
                        // Timeout — loop back to check shutdown flag
                        continue;
                    }
                }
            }
        });
    });

    Ok(SshTunnel {
        local_port,
        shutdown,
    })
}

/// Forward a single TCP connection through an SSH direct-tcpip channel.
///
/// Opens a channel to remote_host:remote_port and bidirectionally copies data
/// between the local TCP stream and the SSH channel using russh's stream API.
async fn forward_connection(
    local_stream: tokio::net::TcpStream,
    session: Arc<client::Handle<ClientHandler>>,
    remote_host: &str,
    remote_port: u16,
) -> Result<(), String> {
    let channel = session
        .channel_open_direct_tcpip(remote_host, remote_port as u32, "127.0.0.1", 0)
        .await
        .map_err(|e| format!("Failed to open SSH channel: {}", e))?;

    // russh's Channel::into_stream() gives us an AsyncRead+AsyncWrite stream
    let channel_stream = channel.into_stream();
    let (mut ch_read, mut ch_write) = tokio::io::split(channel_stream);
    let (mut local_read, mut local_write) = local_stream.into_split();

    // Bidirectional copy: local → SSH channel, SSH channel → local
    let l2r = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut local_read, &mut ch_write).await;
        let _ = ch_write.shutdown().await;
    });

    let r2l = tokio::spawn(async move {
        let _ = tokio::io::copy(&mut ch_read, &mut local_write).await;
        let _ = local_write.shutdown().await;
    });

    // Wait for both directions to finish (one side closes → other follows)
    let _ = tokio::join!(l2r, r2l);

    Ok(())
}

/// Build an SshTunnelConfig from a SavedConnection's SSH fields.
///
/// Called from commands.rs when a saved connection has ssh_enabled=true.
pub fn build_tunnel_config(
    conn: &crate::connections::SavedConnection,
    ssh_password: Option<String>,
) -> Result<SshTunnelConfig, DiffDonkeyError> {
    let ssh_host = conn.ssh_host.as_deref().unwrap_or("").to_string();

    if ssh_host.is_empty() {
        return Err(DiffDonkeyError::Validation(
            "SSH host is required when SSH tunneling is enabled".to_string(),
        ));
    }

    let ssh_username = conn.ssh_username.as_deref().unwrap_or("").to_string();

    if ssh_username.is_empty() {
        return Err(DiffDonkeyError::Validation(
            "SSH username is required when SSH tunneling is enabled".to_string(),
        ));
    }

    let auth = match conn.ssh_auth_method.as_deref() {
        Some("key") => {
            let path = conn.ssh_key_path.as_deref().unwrap_or("").to_string();
            if path.is_empty() {
                return Err(DiffDonkeyError::Validation(
                    "SSH key file path is required for key authentication".to_string(),
                ));
            }
            SshAuth::KeyFile {
                path,
                passphrase: ssh_password,
            }
        }
        _ => {
            // Default to password auth
            SshAuth::Password(ssh_password.unwrap_or_default())
        }
    };

    // The remote host/port is the database host as seen from the bastion
    let remote_host = conn.host.as_deref().unwrap_or("localhost").to_string();

    let remote_port = conn.port.unwrap_or(5432);

    Ok(SshTunnelConfig {
        ssh_host,
        ssh_port: conn.ssh_port.unwrap_or(22),
        ssh_username,
        auth,
        remote_host,
        remote_port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssh_tunnel_config_construction() {
        let config = SshTunnelConfig {
            ssh_host: "bastion.example.com".to_string(),
            ssh_port: 22,
            ssh_username: "deploy".to_string(),
            auth: SshAuth::Password("secret".to_string()),
            remote_host: "db.internal".to_string(),
            remote_port: 5432,
        };
        assert_eq!(config.ssh_host, "bastion.example.com");
        assert_eq!(config.ssh_port, 22);
        assert_eq!(config.remote_port, 5432);
    }

    #[test]
    fn test_ssh_tunnel_config_with_keyfile() {
        let config = SshTunnelConfig {
            ssh_host: "jump.example.com".to_string(),
            ssh_port: 2222,
            ssh_username: "admin".to_string(),
            auth: SshAuth::KeyFile {
                path: "/home/user/.ssh/id_rsa".to_string(),
                passphrase: Some("keypass".to_string()),
            },
            remote_host: "10.0.0.5".to_string(),
            remote_port: 3306,
        };
        assert_eq!(config.ssh_port, 2222);
        assert_eq!(config.remote_port, 3306);
        match &config.auth {
            SshAuth::KeyFile { path, passphrase } => {
                assert_eq!(path, "/home/user/.ssh/id_rsa");
                assert_eq!(passphrase.as_deref(), Some("keypass"));
            }
            _ => panic!("Expected KeyFile auth"),
        }
    }

    #[test]
    fn test_ssh_tunnel_shutdown_flag() {
        let shutdown = Arc::new(AtomicBool::new(false));
        assert!(!shutdown.load(Ordering::Relaxed));
        shutdown.store(true, Ordering::Relaxed);
        assert!(shutdown.load(Ordering::Relaxed));
    }

    #[test]
    fn test_build_tunnel_config_password_auth() {
        let conn = crate::connections::SavedConnection {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: "postgres".to_string(),
            host: Some("db.internal".to_string()),
            port: Some(5432),
            database: Some("mydb".to_string()),
            username: Some("dbuser".to_string()),
            schema: None,
            ssl: false,
            color: None,
            account_url: None,
            warehouse: None,
            role: None,
            auth_method: None,
            private_key_path: None,
            ssh_enabled: true,
            ssh_host: Some("bastion.example.com".to_string()),
            ssh_port: Some(22),
            ssh_username: Some("sshuser".to_string()),
            ssh_auth_method: Some("password".to_string()),
            ssh_key_path: None,
            created_at: "2026-03-31T00:00:00Z".to_string(),
            updated_at: "2026-03-31T00:00:00Z".to_string(),
        };

        let config = build_tunnel_config(&conn, Some("sshpass".to_string())).unwrap();
        assert_eq!(config.ssh_host, "bastion.example.com");
        assert_eq!(config.ssh_port, 22);
        assert_eq!(config.ssh_username, "sshuser");
        assert_eq!(config.remote_host, "db.internal");
        assert_eq!(config.remote_port, 5432);
        match &config.auth {
            SshAuth::Password(pw) => assert_eq!(pw, "sshpass"),
            _ => panic!("Expected Password auth"),
        }
    }

    #[test]
    fn test_build_tunnel_config_key_auth() {
        let conn = crate::connections::SavedConnection {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: "postgres".to_string(),
            host: Some("db.internal".to_string()),
            port: Some(5432),
            database: Some("mydb".to_string()),
            username: Some("dbuser".to_string()),
            schema: None,
            ssl: false,
            color: None,
            account_url: None,
            warehouse: None,
            role: None,
            auth_method: None,
            private_key_path: None,
            ssh_enabled: true,
            ssh_host: Some("bastion.example.com".to_string()),
            ssh_port: Some(2222),
            ssh_username: Some("sshuser".to_string()),
            ssh_auth_method: Some("key".to_string()),
            ssh_key_path: Some("/home/user/.ssh/id_ed25519".to_string()),
            created_at: "2026-03-31T00:00:00Z".to_string(),
            updated_at: "2026-03-31T00:00:00Z".to_string(),
        };

        let config = build_tunnel_config(&conn, Some("keypass".to_string())).unwrap();
        assert_eq!(config.ssh_port, 2222);
        match &config.auth {
            SshAuth::KeyFile { path, passphrase } => {
                assert_eq!(path, "/home/user/.ssh/id_ed25519");
                assert_eq!(passphrase.as_deref(), Some("keypass"));
            }
            _ => panic!("Expected KeyFile auth"),
        }
    }

    #[test]
    fn test_build_tunnel_config_missing_host() {
        let conn = crate::connections::SavedConnection {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: "postgres".to_string(),
            host: Some("db.internal".to_string()),
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
            ssh_enabled: true,
            ssh_host: None,
            ssh_port: None,
            ssh_username: Some("sshuser".to_string()),
            ssh_auth_method: None,
            ssh_key_path: None,
            created_at: "2026-03-31T00:00:00Z".to_string(),
            updated_at: "2026-03-31T00:00:00Z".to_string(),
        };

        let result = build_tunnel_config(&conn, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH host is required"));
    }

    #[test]
    fn test_build_tunnel_config_missing_username() {
        let conn = crate::connections::SavedConnection {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: "postgres".to_string(),
            host: Some("db.internal".to_string()),
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
            ssh_enabled: true,
            ssh_host: Some("bastion.example.com".to_string()),
            ssh_port: None,
            ssh_username: None,
            ssh_auth_method: None,
            ssh_key_path: None,
            created_at: "2026-03-31T00:00:00Z".to_string(),
            updated_at: "2026-03-31T00:00:00Z".to_string(),
        };

        let result = build_tunnel_config(&conn, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH username is required"));
    }

    #[test]
    fn test_build_tunnel_config_key_auth_missing_path() {
        let conn = crate::connections::SavedConnection {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: "postgres".to_string(),
            host: Some("db.internal".to_string()),
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
            ssh_enabled: true,
            ssh_host: Some("bastion.example.com".to_string()),
            ssh_port: None,
            ssh_username: Some("sshuser".to_string()),
            ssh_auth_method: Some("key".to_string()),
            ssh_key_path: None,
            created_at: "2026-03-31T00:00:00Z".to_string(),
            updated_at: "2026-03-31T00:00:00Z".to_string(),
        };

        let result = build_tunnel_config(&conn, None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SSH key file path is required"));
    }

    #[test]
    fn test_build_tunnel_config_defaults() {
        let conn = crate::connections::SavedConnection {
            id: "test".to_string(),
            name: "Test".to_string(),
            db_type: "postgres".to_string(),
            host: None, // defaults to "localhost"
            port: None, // defaults to 5432
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
            ssh_enabled: true,
            ssh_host: Some("bastion.example.com".to_string()),
            ssh_port: None, // defaults to 22
            ssh_username: Some("sshuser".to_string()),
            ssh_auth_method: None, // defaults to password
            ssh_key_path: None,
            created_at: "2026-03-31T00:00:00Z".to_string(),
            updated_at: "2026-03-31T00:00:00Z".to_string(),
        };

        let config = build_tunnel_config(&conn, None).unwrap();
        assert_eq!(config.ssh_port, 22);
        assert_eq!(config.remote_host, "localhost");
        assert_eq!(config.remote_port, 5432);
        match &config.auth {
            SshAuth::Password(pw) => assert_eq!(pw, ""),
            _ => panic!("Expected Password auth with default empty string"),
        }
    }
}
