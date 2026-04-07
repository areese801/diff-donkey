pub mod activity;
mod commands;
mod connections;
mod db;
mod db_loader;
mod diff;
mod error;
mod loader;
pub mod snowflake;
mod ssh_tunnel;
mod types;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let db_state = db::DuckDbState::new().expect("Failed to initialize DuckDB");

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(db_state)
        .manage(activity::ActivityLog::new())
        .invoke_handler(tauri::generate_handler![
            commands::load_source,
            commands::load_database_source,
            commands::get_schema_comparison,
            commands::run_diff,
            commands::get_exclusive_rows,
            commands::get_duplicate_pks,
            commands::get_diff_rows,
            commands::list_saved_connections,
            commands::save_connection,
            commands::delete_connection,
            commands::test_connection,
            commands::load_from_saved_connection,
            commands::load_snowflake_source,
            commands::export_connections_to_file,
            commands::import_connections_from_file,
            commands::get_activity_log,
            commands::clear_activity_log,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
