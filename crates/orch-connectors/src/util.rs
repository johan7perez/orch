//! Utilidades compartidas por los conectores.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use orch_core::{OrchError, Result};

/// Un fichero que sólo aparece en su sitio si se termina de escribir.
///
/// Se escribe en un temporal junto al destino —en la misma carpeta, así que
/// en el mismo sistema de ficheros y renombrarlo es atómico— y se renombra
/// encima al [confirmar](Self::confirmar). Si no se confirma (el pipeline
/// falló, el escritor se rompió), el temporal se borra al soltarlo y el
/// destino se queda como estaba: con el resultado anterior, o sin fichero.
///
/// Quien escribe no confirma: devuelve esto, y confirma quien sabe si todo
/// fue bien, incluido lo que pasó aguas arriba.
pub(crate) struct FicheroAtomico {
    destino: PathBuf,
    temporal: PathBuf,
    confirmado: bool,
}

impl FicheroAtomico {
    pub(crate) fn crear(node: &str, destino: &Path) -> Result<(Self, File)> {
        static CUENTA: AtomicU64 = AtomicU64::new(0);
        let nombre = destino
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        // Empieza por punto y no acaba como el destino: nadie lo toma por un
        // resultado si llega a verlo.
        let temporal = destino.with_file_name(format!(
            ".{nombre}.{}-{}.orch-tmp",
            std::process::id(),
            CUENTA.fetch_add(1, Ordering::Relaxed)
        ));
        let fichero = File::create(&temporal).map_err(|e| {
            OrchError::node(
                node,
                format!("no se pudo crear `{}`: {e}", destino.display()),
            )
        })?;
        Ok((
            Self {
                destino: destino.to_path_buf(),
                temporal,
                confirmado: false,
            },
            fichero,
        ))
    }

    /// Lo pone en su sitio, de una vez.
    pub(crate) fn confirmar(mut self, node: &str) -> Result<()> {
        let fallo = |que: &str, e: std::io::Error| {
            OrchError::node(node, format!("{que} `{}`: {e}", self.destino.display()))
        };
        // Que esté en disco antes de que el nombre apunte a él: un corte de
        // luz no puede dejar un destino vacío.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&self.temporal)
            .and_then(|f| f.sync_all())
            .map_err(|e| fallo("no se pudo asegurar en disco", e))?;
        if let Ok(meta) = std::fs::metadata(&self.destino) {
            let _ = std::fs::set_permissions(&self.temporal, meta.permissions());
        }
        std::fs::rename(&self.temporal, &self.destino)
            .map_err(|e| fallo("no se pudo poner en su sitio", e))?;
        self.confirmado = true;
        // Y que el renombrado también lo esté.
        #[cfg(unix)]
        if let Some(carpeta) = self.destino.parent().filter(|p| !p.as_os_str().is_empty()) {
            let _ = File::open(carpeta).and_then(|d| d.sync_all());
        }
        Ok(())
    }
}

impl Drop for FicheroAtomico {
    fn drop(&mut self) {
        if !self.confirmado {
            let _ = std::fs::remove_file(&self.temporal);
        }
    }
}

/// Convierte un delimitador escrito como carácter en el byte que espera Arrow.
pub(crate) fn delimiter_byte(node: &str, delimiter: char) -> Result<u8> {
    if delimiter.is_ascii() {
        Ok(delimiter as u8)
    } else {
        Err(OrchError::config(
            node,
            format!("el delimitador `{delimiter}` debe ser un carácter ASCII"),
        ))
    }
}

pub(crate) const fn yes() -> bool {
    true
}

pub(crate) const fn comma() -> char {
    ','
}
