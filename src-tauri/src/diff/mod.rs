/// Diff engine module — all the logic for comparing two datasets.
///
/// This is organized like a Python package with __init__.py:
///   diff/
///     mod.rs      ← this file (like __init__.py)
///     schema.rs   ← schema/column comparison
///     stats.rs    ← core diff statistics
///     keys.rs     ← primary key analysis
pub mod keys;
pub mod schema;
pub mod stats;
