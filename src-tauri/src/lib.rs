mod commands;
mod db;
mod db_loader;
mod diff;
mod error;
mod loader;
mod types;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let db_state = db::DuckDbState::new().expect("Failed to initialize DuckDB");

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(db_state)
        .invoke_handler(tauri::generate_handler![
            commands::load_source,
            commands::load_database_source,
            commands::get_schema_comparison,
            commands::run_diff,
            commands::get_exclusive_rows,
            commands::get_duplicate_pks,
            commands::get_diff_rows,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
