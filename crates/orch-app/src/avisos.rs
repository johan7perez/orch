//! Las alertas como notificación del sistema.
//!
//! Dentro de la ventana, una alerta sale encima de la tabla de la ejecución.
//! Pero para eso hay que estar mirando: con la ventana detrás de otra o
//! minimizada, una ejecución puede llevar diez minutos colgada sin que nadie
//! se entere. Entonces avisa el sistema.
//!
//! Con la ventana al frente no se notifica: el aviso ya está en ella, y
//! repetirlo en una esquina de la pantalla sólo molesta. Sale desde aquí, el
//! lado de Rust, donde nace la alerta: llega aunque la interfaz no esté
//! pintando nada.

use orch_alert::{Alerta, Tipo};
use tauri::{AppHandle, Manager};
use tauri_plugin_notification::NotificationExt;

/// Avisa de `alerta` con una notificación del sistema, si la ventana no
/// está al frente.
pub fn notificar(app: &AppHandle, alerta: &Alerta) {
    let al_frente = app
        .get_webview_window("main")
        .and_then(|ventana| ventana.is_focused().ok())
        .unwrap_or(false);
    if al_frente {
        return;
    }

    let titulo = titulo(alerta);
    let cuerpo = alerta.mensaje.clone();
    let app = app.clone();
    // En Linux va por D-Bus y espera respuesta: fuera del bucle que vigila.
    tauri::async_runtime::spawn_blocking(move || {
        if let Err(err) = app
            .notification()
            .builder()
            .title(titulo)
            .body(cuerpo)
            .show()
        {
            tracing::warn!(error = %err, "no se pudo mostrar la notificación");
        }
    });
}

/// El título dice qué pasó y en qué pipeline; el cuerpo, el detalle.
fn titulo(alerta: &Alerta) -> String {
    let que = match alerta.tipo {
        Tipo::Fallo => "falló",
        Tipo::Colgada => "lleva rato sin moverse",
        Tipo::Lenta => "va más lento de lo normal",
    };
    format!("{} {que}", alerta.pipeline)
}
