//! Lo que se sabe de las versiones de un pipeline, juntando git y el
//! historial de ejecuciones.
//!
//! Lo usan igual la CLI (`orch diff`, `orch history`) y la aplicación, así
//! que vive aquí y no en ninguna de las dos.

use std::path::{Path, PathBuf};

use orch_core::{OrchError, PipelineSpec, Result};
use orch_version::{Change, Commit, FileState, Repo};
use serde::{Deserialize, Serialize};

use crate::{Store, VersionRuns};

/// Una versión que se quiere ver o comparar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VersionRef {
    /// El fichero tal y como está en disco.
    Working,
    /// Cualquier cosa que git entienda: `HEAD`, `a1b2c3`, `main~2`.
    Commit { rev: String },
    /// Lo que corrió en una ejecución (basta el prefijo del id).
    Run { run_id: String },
}

impl VersionRef {
    /// Como se escribe en la línea de órdenes: `run:<id>` o una referencia
    /// de git.
    pub fn parse(texto: &str) -> Self {
        match texto.strip_prefix("run:") {
            Some(prefijo) => VersionRef::Run {
                run_id: prefijo.to_string(),
            },
            None => VersionRef::Commit {
                rev: texto.to_string(),
            },
        }
    }
}

/// Una versión ya cargada.
#[derive(Debug, Clone)]
pub struct LoadedVersion {
    /// Cómo se nombra: `a1b2c3d «Sube el umbral»`, `la copia de trabajo`…
    pub label: String,
    pub text: String,
    pub spec: PipelineSpec,
}

/// Carga una versión de un pipeline, tal y como está escrita.
pub fn load(
    version: &VersionRef,
    path: &Path,
    repo: Option<&Repo>,
    store: Option<&Store>,
) -> Result<LoadedVersion> {
    let leer = |label: String, text: String| -> Result<LoadedVersion> {
        let spec = PipelineSpec::from_yaml_str_as_written(&path.display().to_string(), &text)
            .map_err(|err| OrchError::Other(format!("{label} no se puede leer: {err}")))?;
        Ok(LoadedVersion { label, text, spec })
    };

    match version {
        VersionRef::Working => {
            let text = std::fs::read_to_string(path).map_err(|source| OrchError::SpecIo {
                path: path.display().to_string(),
                source,
            })?;
            leer("la copia de trabajo".into(), text)
        }
        VersionRef::Commit { rev } => {
            let repo = repo.ok_or_else(|| {
                OrchError::Other(format!(
                    "`{}` no está en git, así que `{rev}` no es una versión que se pueda buscar. \
                     Lo que corrió en una ejecución sí: `run:<id>`",
                    path.display()
                ))
            })?;
            let (commit, text) = repo.show(rev, path)?;
            leer(format!("{} «{}»", commit.short(), commit.subject), text)
        }
        VersionRef::Run { run_id } => {
            let store = store.ok_or_else(|| {
                OrchError::Other("no hay historial de ejecuciones en el que buscar".into())
            })?;
            let run_id = store.resolve_run(run_id)?;
            let corto = &run_id[..run_id.len().min(8)];
            let version = store.run_version(&run_id)?.ok_or_else(|| {
                OrchError::Other(format!(
                    "la ejecución {corto} es de antes de que se guardara la versión de cada \
                     ejecución"
                ))
            })?;
            if Path::new(&version.path) != canonical(path) {
                return Err(OrchError::Other(format!(
                    "la ejecución {corto} es de otro pipeline: `{}`",
                    version.path
                )));
            }
            let text = store.version_text(&version.hash)?.ok_or_else(|| {
                OrchError::Other(format!(
                    "el texto de la versión de {corto} ya no está en el historial"
                ))
            })?;
            leer(
                format!("lo que corrió en {corto} ({})", version.label),
                text,
            )
        }
    }
}

/// Qué cambió entre dos versiones.
#[derive(Debug, Clone, Serialize)]
pub struct VersionDiff {
    pub from: String,
    pub to: String,
    pub changes: Vec<Change>,
}

pub fn diff(
    path: &Path,
    from: &VersionRef,
    to: &VersionRef,
    store: Option<&Store>,
) -> Result<VersionDiff> {
    let repo = Repo::of(path);
    let antes = load(from, path, repo.as_ref(), store)?;
    let ahora = load(to, path, repo.as_ref(), store)?;
    Ok(VersionDiff {
        changes: orch_version::diff(&antes.spec, &ahora.spec),
        from: antes.label,
        to: ahora.label,
    })
}

/// Un commit del historial del fichero, con lo que corrió de él.
#[derive(Debug, Clone, Serialize)]
pub struct CommitRuns {
    pub commit: Commit,
    /// Huella del texto del fichero en ese commit.
    pub hash: Option<String>,
    /// Las ejecuciones que corrieron exactamente ese texto.
    pub runs: Option<VersionRuns>,
}

/// El historial de un pipeline: sus commits y sus ejecuciones.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineHistory {
    /// Ruta absoluta del fichero.
    pub path: String,
    /// Huella del texto que hay ahora en disco: la misma que la revisión del
    /// lienzo.
    pub hash: String,
    /// Su estado en git; `None` sin git o fuera de un repositorio.
    pub git: Option<FileState>,
    /// Las ejecuciones del texto que hay ahora.
    pub current: Option<VersionRuns>,
    /// Del más nuevo al más viejo, siguiendo al fichero en los renombrados.
    pub commits: Vec<CommitRuns>,
    /// Lo que corrió y no es lo de ahora ni ningún commit de la lista:
    /// cambios que nunca se guardaron así, o commits más viejos que el límite.
    pub others: Vec<VersionRuns>,
}

/// Monta el historial. Una ejecución se atribuye al commit **cuyo texto
/// corrió**, no al que había al ejecutarla: quien prueba un cambio y luego lo
/// guarda tal cual, corrió esa versión.
pub fn history(path: &Path, limit: usize, store: Option<&Store>) -> Result<PipelineHistory> {
    let texto = std::fs::read_to_string(path).map_err(|source| OrchError::SpecIo {
        path: path.display().to_string(),
        source,
    })?;
    let hash = orch_core::fingerprint(&texto);
    let absoluta = canonical(path);

    let repo = Repo::of(path);
    let (commits, git) = match &repo {
        Some(repo) => (repo.log(path, limit)?, Some(repo.state(path)?)),
        None => (Vec::new(), None),
    };
    let mut corridas = match store {
        Some(store) => juntar_por_texto(store.versions_of(&absoluta.to_string_lossy())?),
        None => Vec::new(),
    };

    let mut tomar = |h: Option<&str>| -> Option<VersionRuns> {
        let i = corridas
            .iter()
            .position(|v| Some(v.version.hash.as_str()) == h)?;
        Some(corridas.remove(i))
    };
    let current = tomar(Some(&hash));
    let commits = commits
        .into_iter()
        .map(|commit| {
            let huella = repo
                .as_ref()
                .and_then(|r| r.text_of(&commit).ok())
                .map(|t| orch_core::fingerprint(&t));
            CommitRuns {
                // Si el texto de ahora es el de este commit, sus ejecuciones
                // ya están en `current`; se repiten aquí para que la lista
                // de commits no parezca que no corrió nunca.
                runs: if huella.as_deref() == Some(hash.as_str()) {
                    current.clone()
                } else {
                    tomar(huella.as_deref())
                },
                hash: huella,
                commit,
            }
        })
        .collect();

    Ok(PipelineHistory {
        path: absoluta.to_string_lossy().into_owned(),
        hash,
        git,
        current,
        commits,
        others: corridas,
    })
}

/// Una versión es su texto. El historial guarda además cómo estaba git al
/// ejecutar —«a1b2c3d con cambios» antes de guardar, «e4f5a6b» después—, así
/// que el mismo texto puede llegar en varias filas: se juntan, con lo de la
/// más reciente. Vienen de la más reciente a la más antigua.
fn juntar_por_texto(corridas: Vec<VersionRuns>) -> Vec<VersionRuns> {
    let mut juntas: Vec<VersionRuns> = Vec::new();
    for v in corridas {
        match juntas.iter_mut().find(|j| j.version.hash == v.version.hash) {
            Some(j) => j.runs += v.runs,
            None => juntas.push(v),
        }
    }
    juntas
}

fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
