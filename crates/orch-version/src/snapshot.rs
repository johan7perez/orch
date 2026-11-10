//! Lo que se ejecutó: el texto exacto de un pipeline y de qué versión es.

use std::path::{Path, PathBuf};

use orch_core::{OrchError, PipelineSpec, Result};
use serde::Serialize;

use crate::git::{FileState, Repo};

/// Una versión concreta de un pipeline, leída una sola vez.
///
/// Se toma al arrancar una ejecución, y el pipeline que se ejecuta sale de
/// este mismo texto ([`Snapshot::spec`]). Así lo que se guarda como «la
/// versión que corrió» es exactamente lo que corrió, aunque el fichero cambie
/// un instante después.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    /// La ruta del fichero, absoluta.
    pub path: PathBuf,
    pub text: String,
    /// Huella del texto ([`orch_core::fingerprint`]): la misma con la que el
    /// lienzo identifica lo que dibuja.
    pub hash: String,
    /// Su estado en git. `None` sin git o fuera de un repositorio.
    pub git: Option<FileState>,
}

impl Snapshot {
    /// Lee el pipeline y mira su estado en git.
    ///
    /// Sólo falla si no se puede leer el fichero: un problema con git deja
    /// la versión sin commit, no impide ejecutar.
    pub fn take(path: &Path) -> Result<Snapshot> {
        let text = std::fs::read_to_string(path).map_err(|source| OrchError::SpecIo {
            path: path.display().to_string(),
            source,
        })?;
        let git = Repo::of(path).and_then(|repo| match repo.state(path) {
            Ok(state) => Some(state),
            Err(err) => {
                tracing::warn!(path = %path.display(), error = %err, "no se pudo mirar el estado en git");
                None
            }
        });
        Ok(Snapshot {
            path: path.canonicalize().unwrap_or_else(|_| path.to_path_buf()),
            hash: orch_core::fingerprint(&text),
            text,
            git,
        })
    }

    /// El pipeline listo para ejecutar, sacado de este mismo texto.
    pub fn spec(&self) -> Result<PipelineSpec> {
        PipelineSpec::from_text_at(&self.path, &self.text)
    }

    /// El commit del que es esta versión, si está guardada tal cual en git.
    pub fn commit(&self) -> Option<&crate::git::Commit> {
        self.git.as_ref().filter(|g| !g.modified)?.commit.as_ref()
    }

    /// Cómo se nombra esta versión: `a1b2c3d`, `a1b2c3d con cambios` o
    /// `sin git`.
    pub fn label(&self) -> String {
        label(
            &self.hash,
            self.git
                .as_ref()
                .and_then(|g| g.commit.as_ref())
                .map(|c| c.hash.as_str()),
            self.git.as_ref().is_none_or(|g| g.modified),
            self.git.is_some(),
        )
    }
}

/// El nombre corto de una versión, a partir de lo que se sabe de ella:
/// `a1b2c3d`, `a1b2c3d con cambios`, `sin guardar en git` o, fuera de git,
/// `sin git (#9e8f138)`, con el principio de la huella para distinguirlas.
pub fn label(hash: &str, commit: Option<&str>, modified: bool, en_git: bool) -> String {
    match (commit, modified) {
        (Some(commit), false) => crate::git::short(commit).to_string(),
        (Some(commit), true) => format!("{} con cambios", crate::git::short(commit)),
        (None, _) if en_git => "sin guardar en git".to_string(),
        (None, _) => format!("sin git (#{})", crate::git::short(hash)),
    }
}
