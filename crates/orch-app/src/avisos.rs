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
#[cfg(not(target_os = "linux"))]
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
        if let Err(err) = mostrar(&app, titulo, cuerpo) {
            tracing::warn!(error = %err, "no se pudo mostrar la notificación");
        }
    });
}

/// En Windows y macOS, con el plugin: presenta la notificación como la
/// aplicación instalada (su identificador).
#[cfg(not(target_os = "linux"))]
fn mostrar(app: &AppHandle, titulo: String, cuerpo: String) -> Result<(), String> {
    app.notification()
        .builder()
        .title(titulo)
        .body(cuerpo)
        .show()
        .map_err(|err| err.to_string())
}

/// En Linux el plugin la presenta con el nombre del ejecutable (`orch-app`) y
/// no deja cambiarlo. Así sale como «Orch», con el icono que instala el
/// paquete y su entrada de escritorio (`Orch.desktop`), por la que el
/// escritorio la agrupa con la aplicación.
#[cfg(target_os = "linux")]
fn mostrar(app: &AppHandle, titulo: String, cuerpo: String) -> Result<(), String> {
    let nombre = app.package_info().name.clone();
    // El icono se instala con el nombre del ejecutable.
    let icono = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_default();
    linux::mostrar(&nombre, &icono, &titulo, &cuerpo)
}

#[cfg(target_os = "linux")]
mod linux {
    use notify_rust::{Hint, Notification};

    pub fn mostrar(nombre: &str, icono: &str, titulo: &str, cuerpo: &str) -> Result<(), String> {
        Notification::new()
            .appname(nombre)
            .icon(icono)
            .hint(Hint::DesktopEntry(nombre.to_string()))
            .summary(titulo)
            .body(cuerpo)
            .show()
            .map(drop)
            .map_err(|err| err.to_string())
    }

    /// Manda una notificación de verdad: se lanza a mano, con
    /// `dbus-monitor "interface='org.freedesktop.Notifications'"` mirando.
    #[test]
    #[ignore = "enseña una notificación en el escritorio"]
    fn sale_con_el_nombre_de_la_aplicacion() {
        mostrar("Orch", "orch-app", "Prueba de Orch", "Si se ve, va bien.").unwrap();
    }
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
