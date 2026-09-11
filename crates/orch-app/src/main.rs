// Sin consola detrás de la ventana en Windows.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

//! Aplicación de escritorio de Orch.
//!
//! El motor corre dentro de este mismo proceso, como biblioteca: no hay
//! servidor, ni puerto, ni serialización de los datos. Lo único que cruza
//! hacia el webview son eventos y métricas, nunca los lotes de Arrow.

mod commands;
mod events;
mod state;

use orch_store::Store;
use state::{default_directory, default_store, full_registry, AppState};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("ORCH_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_target(false)
        .init();

    let store = match Store::open(default_store()) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("no se pudo abrir el historial: {err}");
            std::process::exit(1);
        }
    };

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(AppState::new(full_registry(), store, default_directory()))
        .invoke_handler(tauri::generate_handler![
            commands::workspace,
            commands::set_directory,
            commands::recent_runs,
            commands::run_detail,
            commands::start_run,
            commands::catalog,
        ])
        .run(tauri::generate_context!())
        .expect("no se pudo arrancar la ventana");
}
