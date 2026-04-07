/// Query history — persistent storage for recently executed SQL queries.
///
/// Stores queries per connection so users can quickly re-use them.
/// History is stored as JSON in the app's data directory, same pattern as connections.json.
use std::path::PathBuf;

use crate::error::DiffDonkeyError;

/// Maximum entries per connection.
const MAX_PER_CONNECTION: usize = 50;
/// Maximum total entries across all connections.
const MAX_TOTAL: usize = 500;

/// A single query history entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryHistoryEntry {
    pub id: String,
    pub connection_id: Option<String>,
    pub query: String,
    pub created_at: String,
    pub last_used_at: String,
}

/// Get the path to query_history.json in the app's data directory.
pub fn get_history_path(app_handle: &tauri::AppHandle) -> PathBuf {
    use tauri::Manager;
    let data_dir = app_handle
        .path()
        .app_data_dir()
        .expect("Failed to resolve app data directory");
    data_dir.join("query_history.json")
}

/// Read all history entries from disk, optionally filtered by connection_id.
/// Returns entries sorted by last_used_at descending (most recent first).
pub fn list_history(
    path: &PathBuf,
    connection_id: Option<&str>,
) -> Result<Vec<QueryHistoryEntry>, DiffDonkeyError> {
    let mut entries = read_entries(path)?;

    if let Some(cid) = connection_id {
        entries.retain(|e| e.connection_id.as_deref() == Some(cid));
    }

    entries.sort_by(|a, b| b.last_used_at.cmp(&a.last_used_at));
    Ok(entries)
}

/// Add a query to history. If the same query+connection already exists,
/// update its last_used_at. Otherwise create a new entry. Enforces caps.
pub fn add_to_history(
    path: &PathBuf,
    connection_id: Option<&str>,
    query: &str,
) -> Result<(), DiffDonkeyError> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(());
    }

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut entries = read_entries(path)?;
    let now = chrono::Utc::now().to_rfc3339();

    // Check for existing entry with same query text + connection
    if let Some(existing) = entries.iter_mut().find(|e| {
        e.query == trimmed && e.connection_id.as_deref() == connection_id
    }) {
        existing.last_used_at = now;
    } else {
        entries.push(QueryHistoryEntry {
            id: uuid::Uuid::new_v4().to_string(),
            connection_id: connection_id.map(|s| s.to_string()),
            query: trimmed.to_string(),
            created_at: now.clone(),
            last_used_at: now,
        });
    }

    // Enforce per-connection cap: keep newest MAX_PER_CONNECTION per connection
    enforce_per_connection_cap(&mut entries, connection_id);

    // Enforce total cap: drop oldest entries globally
    enforce_total_cap(&mut entries);

    write_entries(path, &entries)
}

/// Delete a single history entry by ID.
pub fn delete_history_entry(
    path: &PathBuf,
    id: &str,
) -> Result<(), DiffDonkeyError> {
    let mut entries = read_entries(path)?;
    entries.retain(|e| e.id != id);
    write_entries(path, &entries)
}

/// Clear all history entries, optionally filtered by connection_id.
pub fn clear_history(
    path: &PathBuf,
    connection_id: Option<&str>,
) -> Result<(), DiffDonkeyError> {
    if let Some(cid) = connection_id {
        let mut entries = read_entries(path)?;
        entries.retain(|e| e.connection_id.as_deref() != Some(cid));
        write_entries(path, &entries)
    } else {
        write_entries(path, &Vec::new())
    }
}

// ─── Internal Helpers ────────────────────────────────────────────────────────

fn read_entries(path: &PathBuf) -> Result<Vec<QueryHistoryEntry>, DiffDonkeyError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read_to_string(path)?;
    let entries: Vec<QueryHistoryEntry> =
        serde_json::from_str(&data).map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    Ok(entries)
}

fn write_entries(
    path: &PathBuf,
    entries: &Vec<QueryHistoryEntry>,
) -> Result<(), DiffDonkeyError> {
    let json = serde_json::to_string_pretty(entries)
        .map_err(|e| DiffDonkeyError::Validation(e.to_string()))?;
    std::fs::write(path, json)?;
    Ok(())
}

fn enforce_per_connection_cap(entries: &mut Vec<QueryHistoryEntry>, connection_id: Option<&str>) {
    // Sort matching entries by last_used_at descending, mark excess for removal
    let mut matching: Vec<(usize, String)> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.connection_id.as_deref() == connection_id)
        .map(|(i, e)| (i, e.last_used_at.clone()))
        .collect();

    if matching.len() <= MAX_PER_CONNECTION {
        return;
    }

    // Sort by last_used_at descending (newest first)
    matching.sort_by(|a, b| b.1.cmp(&a.1));

    // Collect indices to remove (the oldest ones beyond the cap)
    let to_remove: std::collections::HashSet<usize> = matching[MAX_PER_CONNECTION..]
        .iter()
        .map(|(i, _)| *i)
        .collect();

    let mut idx = 0;
    entries.retain(|_| {
        let keep = !to_remove.contains(&idx);
        idx += 1;
        keep
    });
}

fn enforce_total_cap(entries: &mut Vec<QueryHistoryEntry>) {
    if entries.len() <= MAX_TOTAL {
        return;
    }

    // Sort by last_used_at descending, keep the newest MAX_TOTAL
    entries.sort_by(|a, b| b.last_used_at.cmp(&a.last_used_at));
    entries.truncate(MAX_TOTAL);
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path() -> PathBuf {
        let id = uuid::Uuid::new_v4();
        PathBuf::from(format!("/tmp/test_query_history_{}.json", id))
    }

    #[test]
    fn test_add_to_history() {
        let path = temp_path();
        add_to_history(&path, Some("conn1"), "SELECT 1").unwrap();

        let entries = list_history(&path, Some("conn1")).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].query, "SELECT 1");
        assert_eq!(entries[0].connection_id.as_deref(), Some("conn1"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_duplicate_query_updates_timestamp() {
        let path = temp_path();
        add_to_history(&path, Some("conn1"), "SELECT 1").unwrap();
        let first = list_history(&path, Some("conn1")).unwrap();
        let first_created = first[0].created_at.clone();

        // Small delay to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(10));
        add_to_history(&path, Some("conn1"), "SELECT 1").unwrap();

        let entries = list_history(&path, Some("conn1")).unwrap();
        assert_eq!(entries.len(), 1, "Should still be one entry");
        assert_eq!(entries[0].created_at, first_created, "created_at unchanged");
        assert!(
            entries[0].last_used_at >= first[0].last_used_at,
            "last_used_at should be updated"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_history_cap_per_connection() {
        let path = temp_path();

        // Add 51 entries for same connection
        for i in 0..51 {
            add_to_history(&path, Some("conn1"), &format!("SELECT {}", i)).unwrap();
        }

        let entries = list_history(&path, Some("conn1")).unwrap();
        assert_eq!(entries.len(), MAX_PER_CONNECTION);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_filter_by_connection() {
        let path = temp_path();
        add_to_history(&path, Some("conn1"), "SELECT 1").unwrap();
        add_to_history(&path, Some("conn2"), "SELECT 2").unwrap();
        add_to_history(&path, None, "SELECT 3").unwrap();

        let conn1 = list_history(&path, Some("conn1")).unwrap();
        assert_eq!(conn1.len(), 1);
        assert_eq!(conn1[0].query, "SELECT 1");

        let conn2 = list_history(&path, Some("conn2")).unwrap();
        assert_eq!(conn2.len(), 1);

        let all = list_history(&path, None).unwrap();
        assert_eq!(all.len(), 3);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete_entry() {
        let path = temp_path();
        add_to_history(&path, Some("conn1"), "SELECT 1").unwrap();
        let entries = list_history(&path, Some("conn1")).unwrap();
        let id = entries[0].id.clone();

        delete_history_entry(&path, &id).unwrap();

        let after = list_history(&path, Some("conn1")).unwrap();
        assert!(after.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_clear_history() {
        let path = temp_path();
        add_to_history(&path, Some("conn1"), "SELECT 1").unwrap();
        add_to_history(&path, Some("conn2"), "SELECT 2").unwrap();

        // Clear only conn1
        clear_history(&path, Some("conn1")).unwrap();
        let remaining = list_history(&path, None).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].connection_id.as_deref(), Some("conn2"));

        // Clear all
        clear_history(&path, None).unwrap();
        let empty = list_history(&path, None).unwrap();
        assert!(empty.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_empty_query_ignored() {
        let path = temp_path();
        add_to_history(&path, Some("conn1"), "  ").unwrap();
        let entries = list_history(&path, None).unwrap();
        assert!(entries.is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_nonexistent_file_returns_empty() {
        let path = PathBuf::from("/tmp/nonexistent_query_history_test.json");
        let entries = list_history(&path, None).unwrap();
        assert!(entries.is_empty());
    }
}
