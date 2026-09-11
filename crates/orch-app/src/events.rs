//! Puente entre el canal del ejecutor y la ventana.
//!
//! El motor publica en un `broadcast`; aquí se reenvía cada evento al
//! webview. Es el mismo canal que ya alimentaba al visor de la CLI y al
//! escritor de DuckDB: la UI no necesitó nada nuevo del motor, sólo otro
//! suscriptor.
//!
//! Que la UI se retrase no puede frenar los datos, así que si el puente
//! pierde eventos se avisa y se sigue.

use orch_core::RunEvent;
use tauri::{AppHandle, Emitter};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

/// Nombre del evento que escucha el frontend.
pub const CHANNEL: &str = "orch://event";

pub fn bridge(app: AppHandle, mut events: Receiver<RunEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    if app.emit(CHANNEL, &event).is_err() {
                        // La ventana se cerró: no hay a quién contárselo.
                        break;
                    }
                }
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "la ventana se retrasó y perdió eventos");
                }
            }
        }
    })
}
