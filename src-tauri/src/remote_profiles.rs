/// Saved remote connection profiles — persistent storage for S3/GCS/HTTP credentials.
///
/// Non-sensitive fields (region, endpoint, url_style, use_ssl) are stored in JSON:
///   - macOS: ~/Library/Application Support/com.diff-donkey/remote_profiles.json
///
/// Sensitive fields (access_key, secret_key, session_token, bearer_token) are stored
/// in the OS keychain via the `keyring` crate, keyed by `remote/{id}/{field}`.
use std::path::{Path, PathBuf};

use crate::error::DiffDonkeyError;

/// A saved remote connection profile — all fields stored in JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SavedRemoteProfile {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub url_style: Option<String>,
    #[serde(default)]
    pub use_ssl: Option<bool>,
    // Credentials stored inline (app data dir is user-private)
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub bearer_token: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// Credential fields sent separately from the frontend for save operations.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RemoteSecrets {
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub bearer_token: Option<String>,
}

/// Get the path to remote_profiles.json in the app's data directory.
pub fn get_remote_profiles_path(app_handle: &tauri::AppHandle) -> PathBuf {
    use tauri::Manager;
    let data_dir = app_handle
        .path()
        .app_data_dir()
        .expect("Failed to resolve app data directory");
    data_dir.join("remote_profiles.json")
}

/// Read all saved remote profiles from the JSON file.
pub fn list_profiles(path: &Path) -> Result<Vec<SavedRemoteProfile>, DiffDonkeyError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(path)?;
    let profiles: Vec<SavedRemoteProfile> =
        serde_json::from_str(&data).map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    Ok(profiles)
}

/// Save (upsert) a remote profile with secrets stored inline in JSON.
pub fn save_profile(
    path: &Path,
    mut profile: SavedRemoteProfile,
    secrets: RemoteSecrets,
) -> Result<(), DiffDonkeyError> {
    if profile.name.trim().is_empty() {
        return Err(DiffDonkeyError::Validation(
            "Profile name cannot be empty".to_string(),
        ));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Merge secrets into the profile
    profile.access_key = secrets.access_key;
    profile.secret_key = secrets.secret_key;
    profile.session_token = secrets.session_token;
    profile.bearer_token = secrets.bearer_token;

    let mut profiles = list_profiles(path)?;

    // Upsert
    if let Some(pos) = profiles.iter().position(|p| p.id == profile.id) {
        profiles[pos] = profile;
    } else {
        profiles.push(profile);
    }

    let json = serde_json::to_string_pretty(&profiles)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(path, json)?;

    Ok(())
}

/// Delete a remote profile.
pub fn delete_profile(path: &Path, id: &str) -> Result<(), DiffDonkeyError> {
    let mut profiles = list_profiles(path)?;
    profiles.retain(|p| p.id != id);

    let json = serde_json::to_string_pretty(&profiles)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(path, json)?;

    Ok(())
}

/// Retrieve secrets for a profile from the JSON file.
pub fn get_profile_secrets(path: &Path, id: &str) -> Result<RemoteSecrets, DiffDonkeyError> {
    let profiles = list_profiles(path)?;
    let profile = profiles.iter().find(|p| p.id == id);
    match profile {
        Some(p) => Ok(RemoteSecrets {
            access_key: p.access_key.clone(),
            secret_key: p.secret_key.clone(),
            session_token: p.session_token.clone(),
            bearer_token: p.bearer_token.clone(),
        }),
        None => Ok(RemoteSecrets::default()),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("diff-donkey-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir.join("remote_profiles.json")
    }

    fn test_profile(id: &str, name: &str) -> SavedRemoteProfile {
        SavedRemoteProfile {
            id: id.to_string(),
            name: name.to_string(),
            provider: Some("s3".to_string()),
            region: Some("us-east-1".to_string()),
            endpoint: Some("127.0.0.1:9000".to_string()),
            url_style: Some("path".to_string()),
            use_ssl: Some(false),
            access_key: None,
            secret_key: None,
            session_token: None,
            bearer_token: None,
            created_at: "2026-04-15T00:00:00Z".to_string(),
            updated_at: "2026-04-15T00:00:00Z".to_string(),
        }
    }

    fn test_secrets() -> RemoteSecrets {
        RemoteSecrets {
            access_key: Some("minioadmin".to_string()),
            secret_key: Some("minioadmin".to_string()),
            session_token: None,
            bearer_token: None,
        }
    }

    #[test]
    fn test_list_empty() {
        let path = temp_path();
        let profiles = list_profiles(&path).unwrap();
        assert!(profiles.is_empty());
    }

    #[test]
    fn test_save_and_list() {
        let path = temp_path();
        let profile = test_profile("test-1", "MinIO Local");

        save_profile(&path, profile.clone(), test_secrets()).unwrap();

        let profiles = list_profiles(&path).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "MinIO Local");
        assert_eq!(profiles[0].endpoint, Some("127.0.0.1:9000".to_string()));
        assert_eq!(profiles[0].use_ssl, Some(false));
        // Secrets stored inline
        assert_eq!(profiles[0].access_key, Some("minioadmin".to_string()));
        assert_eq!(profiles[0].secret_key, Some("minioadmin".to_string()));
    }

    #[test]
    fn test_secrets_roundtrip() {
        let path = temp_path();
        let profile = test_profile("test-secrets", "SecretsTest");
        let secrets = RemoteSecrets {
            access_key: Some("AKID".to_string()),
            secret_key: Some("SECRET".to_string()),
            session_token: Some("TOKEN".to_string()),
            bearer_token: None,
        };
        save_profile(&path, profile, secrets).unwrap();

        let retrieved = get_profile_secrets(&path, "test-secrets").unwrap();
        assert_eq!(retrieved.access_key, Some("AKID".to_string()));
        assert_eq!(retrieved.secret_key, Some("SECRET".to_string()));
        assert_eq!(retrieved.session_token, Some("TOKEN".to_string()));
        assert_eq!(retrieved.bearer_token, None);
    }

    #[test]
    fn test_secrets_missing_profile() {
        let path = temp_path();
        let retrieved = get_profile_secrets(&path, "nonexistent").unwrap();
        assert_eq!(retrieved.access_key, None);
    }

    #[test]
    fn test_upsert() {
        let path = temp_path();
        let profile = test_profile("test-2", "Original");
        save_profile(&path, profile.clone(), test_secrets()).unwrap();

        let mut profile2 = test_profile("test-2", "Updated");
        profile2.region = Some("eu-west-1".to_string());
        save_profile(&path, profile2, test_secrets()).unwrap();

        let profiles = list_profiles(&path).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "Updated");
        assert_eq!(profiles[0].region, Some("eu-west-1".to_string()));
    }

    #[test]
    fn test_delete() {
        let path = temp_path();
        save_profile(&path, test_profile("test-3", "ToDelete"), test_secrets()).unwrap();
        assert_eq!(list_profiles(&path).unwrap().len(), 1);

        delete_profile(&path, "test-3").unwrap();
        assert!(list_profiles(&path).unwrap().is_empty());
    }

    #[test]
    fn test_empty_name_rejected() {
        let path = temp_path();
        let mut profile = test_profile("test-4", "");
        profile.name = "  ".to_string();
        let result = save_profile(&path, profile, test_secrets());
        assert!(result.is_err());
    }

    #[test]
    fn test_serde_with_missing_optional_fields() {
        // Simulate what Tauri sends — some fields might be missing or null
        let json = r#"{
            "id": "test-5",
            "name": "Minimal",
            "created_at": "2026-04-15T00:00:00Z",
            "updated_at": "2026-04-15T00:00:00Z"
        }"#;
        let profile: SavedRemoteProfile = serde_json::from_str(json).unwrap();
        assert_eq!(profile.name, "Minimal");
        assert_eq!(profile.provider, None);
        assert_eq!(profile.region, None);
        assert_eq!(profile.use_ssl, None);
    }

    #[test]
    fn test_serde_secrets_with_nulls() {
        let json = r#"{
            "access_key": "AKID",
            "secret_key": null
        }"#;
        let secrets: RemoteSecrets = serde_json::from_str(json).unwrap();
        assert_eq!(secrets.access_key, Some("AKID".to_string()));
        assert_eq!(secrets.secret_key, None);
        assert_eq!(secrets.session_token, None);
        assert_eq!(secrets.bearer_token, None);
    }
}
