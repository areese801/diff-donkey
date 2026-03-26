/// Central error type for Diff Donkey.
///
/// Think of this like a Python exception hierarchy — but the Rust compiler
/// forces every caller to handle (or propagate) the error. No silent failures.
///
/// `thiserror` auto-generates Display and From impls, similar to how Python's
/// exception classes auto-generate __str__ from args.
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DiffDonkeyError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Validation error: {0}")]
    Validation(String),
}

/// Tauri commands need to return `Result<T, String>` for the frontend.
/// This impl converts errors to user-safe messages — avoiding leaking
/// internal paths, SQL fragments, or stack traces to the UI.
///
/// In Python terms: this is like having a middleware that catches all custom
/// exceptions and converts them to a sanitized JSON error response.
impl From<DiffDonkeyError> for String {
    fn from(err: DiffDonkeyError) -> String {
        match &err {
            // DuckDB errors may contain file paths or SQL fragments — sanitize
            DiffDonkeyError::DuckDb(e) => {
                let msg = e.to_string();
                // Log the full error for debugging (visible in dev console)
                eprintln!("DuckDB error: {}", msg);
                // Return a safe summary to the frontend
                if msg.contains("does not exist") || msg.contains("not found") {
                    "Database error: table or column not found. Try reloading your sources.".to_string()
                } else if msg.contains("read_csv") || msg.contains("read_parquet") {
                    "Error reading file. Please check the file format and try again.".to_string()
                } else {
                    "A database error occurred. Check the console for details.".to_string()
                }
            }
            // IO errors may contain file paths
            DiffDonkeyError::Io(_) => {
                eprintln!("IO error: {}", err);
                "A file system error occurred.".to_string()
            }
            // Validation errors are user-facing by design — safe to pass through
            DiffDonkeyError::Validation(msg) => msg.clone(),
        }
    }
}
