//! Lo que la aplicación recuerda entre arranques, y los ejemplos que trae.
//!
//! Compilada desde el repositorio, la aplicación arranca en una carpeta con
//! `examples/pipelines` al lado. Instalada no: arranca donde quiere el
//! sistema, y sin acordarse de nada abriría siempre una carpeta que no
//! existe. Así que recuerda la última carpeta que se abrió y, la primera
//! vez, abre unos ejemplos que lleva dentro del instalador, copiados a la
//! carpeta de datos del usuario (dentro del paquete no se puede escribir, y
//! un pipeline de ejemplo escribe sus salidas al lado).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
struct Preferencias {
    /// La carpeta de pipelines que se estaba mirando.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    carpeta: Option<PathBuf>,
}

/// El fichero donde se guardan: `~/.config/orch/aplicacion.json` en Linux,
/// `~/Library/Application Support/orch` en macOS, `%APPDATA%\orch` en
/// Windows.
fn fichero() -> Option<PathBuf> {
    dirs::config_dir().map(|config| config.join("orch").join("aplicacion.json"))
}

fn leer(fichero: &Path) -> Preferencias {
    std::fs::read(fichero)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Escribe al lado y renombra: un corte a mitad no deja un JSON roto que
/// haga olvidarlo todo.
fn guardar(fichero: &Path, preferencias: &Preferencias) -> std::io::Result<()> {
    if let Some(carpeta) = fichero.parent() {
        std::fs::create_dir_all(carpeta)?;
    }
    let temporal = fichero.with_extension("json.orch-tmp");
    let texto = serde_json::to_vec_pretty(preferencias).map_err(std::io::Error::other)?;
    std::fs::write(&temporal, texto)?;
    std::fs::rename(&temporal, fichero)
}

fn recordada_en(fichero: &Path) -> Option<PathBuf> {
    leer(fichero).carpeta.filter(|carpeta| carpeta.is_dir())
}

fn recordar_en(fichero: &Path, carpeta: &Path) -> std::io::Result<()> {
    let mut preferencias = leer(fichero);
    // Absoluta: la próxima vez puede arrancar desde otro sitio.
    preferencias.carpeta = Some(std::path::absolute(carpeta)?);
    guardar(fichero, &preferencias)
}

/// La última carpeta de pipelines que se abrió, si todavía existe.
pub fn carpeta_recordada() -> Option<PathBuf> {
    recordada_en(&fichero()?)
}

/// Recuerda `carpeta` para el próximo arranque. Si no se puede, se avisa en
/// el log y nada más: no es motivo para no abrirla.
pub fn recordar_carpeta(carpeta: &Path) {
    let Some(fichero) = fichero() else {
        return;
    };
    if let Err(err) = recordar_en(&fichero, carpeta) {
        tracing::warn!(error = %err, fichero = %fichero.display(), "no se pudo recordar la carpeta");
    }
}

/// La carpeta de pipelines de los ejemplos instalados, copiándolos desde
/// `origen` (los recursos del paquete) la primera vez.
///
/// Si ya estaban copiados se dejan como estén: pueden haberse editado.
pub fn ejemplos(origen: &Path) -> Option<PathBuf> {
    let destino = dirs::data_local_dir()?.join("orch").join("ejemplos");
    match instalar_ejemplos(origen, &destino) {
        Ok(pipelines) => Some(pipelines),
        Err(err) => {
            tracing::warn!(error = %err, destino = %destino.display(), "no se pudieron copiar los ejemplos");
            None
        }
    }
}

fn instalar_ejemplos(origen: &Path, destino: &Path) -> std::io::Result<PathBuf> {
    let pipelines = destino.join("pipelines");
    if destino.exists() {
        return Ok(pipelines);
    }
    if !origen.join("pipelines").is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "el paquete no trae `{}`",
                origen.join("pipelines").display()
            ),
        ));
    }
    // A una carpeta de al lado y luego de una vez: si se corta a mitad, la
    // próxima vez se copia entera en vez de quedarse con media.
    let temporal = destino.with_extension("orch-tmp");
    let _ = std::fs::remove_dir_all(&temporal);
    copiar(origen, &temporal)?;
    std::fs::rename(&temporal, destino)?;
    Ok(pipelines)
}

fn copiar(origen: &Path, destino: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(destino)?;
    for entrada in std::fs::read_dir(origen)? {
        let entrada = entrada?;
        let hacia = destino.join(entrada.file_name());
        if entrada.file_type()?.is_dir() {
            copiar(&entrada.path(), &hacia)?;
        } else {
            std::fs::copy(entrada.path(), &hacia)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn la_carpeta_recordada_vuelve_en_el_siguiente_arranque() {
        let dir = tempfile::tempdir().unwrap();
        let fichero = dir.path().join("config").join("aplicacion.json");
        let carpeta = dir.path().join("mis-pipelines");
        std::fs::create_dir(&carpeta).unwrap();

        assert_eq!(recordada_en(&fichero), None);
        recordar_en(&fichero, &carpeta).unwrap();
        assert_eq!(recordada_en(&fichero), Some(carpeta.clone()));

        // Borrada desde fuera: no se abre una carpeta que no existe.
        std::fs::remove_dir(&carpeta).unwrap();
        assert_eq!(recordada_en(&fichero), None);
    }

    #[test]
    fn un_fichero_roto_no_impide_arrancar() {
        let dir = tempfile::tempdir().unwrap();
        let fichero = dir.path().join("aplicacion.json");
        std::fs::write(&fichero, "{ esto no es json").unwrap();
        assert_eq!(recordada_en(&fichero), None);
        // Y se puede volver a recordar encima.
        recordar_en(&fichero, dir.path()).unwrap();
        assert_eq!(recordada_en(&fichero), Some(dir.path().to_path_buf()));
    }

    #[test]
    fn los_ejemplos_se_copian_una_vez_y_luego_se_respetan() {
        let dir = tempfile::tempdir().unwrap();
        let origen = dir.path().join("paquete");
        std::fs::create_dir_all(origen.join("pipelines")).unwrap();
        std::fs::create_dir_all(origen.join("data")).unwrap();
        std::fs::write(origen.join("pipelines/uno.yaml"), "name: uno\n").unwrap();
        std::fs::write(origen.join("data/datos.csv"), "a\n1\n").unwrap();
        let destino = dir.path().join("datos").join("ejemplos");

        let pipelines = instalar_ejemplos(&origen, &destino).unwrap();
        assert_eq!(pipelines, destino.join("pipelines"));
        assert_eq!(
            std::fs::read_to_string(pipelines.join("uno.yaml")).unwrap(),
            "name: uno\n"
        );
        assert!(destino.join("data/datos.csv").is_file());

        // Editado por el usuario: un arranque nuevo no lo pisa.
        std::fs::write(pipelines.join("uno.yaml"), "name: editado\n").unwrap();
        instalar_ejemplos(&origen, &destino).unwrap();
        assert_eq!(
            std::fs::read_to_string(pipelines.join("uno.yaml")).unwrap(),
            "name: editado\n"
        );
        assert!(!destino.with_extension("orch-tmp").exists());
    }

    #[test]
    fn sin_ejemplos_en_el_paquete_no_se_crea_nada() {
        let dir = tempfile::tempdir().unwrap();
        let destino = dir.path().join("ejemplos");
        assert!(instalar_ejemplos(&dir.path().join("no-existe"), &destino).is_err());
        assert!(!destino.exists());
    }
}
