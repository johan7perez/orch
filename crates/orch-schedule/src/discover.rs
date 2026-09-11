//! Descubrimiento de pipelines en un directorio.

use std::path::{Path, PathBuf};

use orch_core::{OrchError, PipelineSpec, Result};

use crate::scheduler::Entry;

/// Un fichero que no se pudo cargar.
#[derive(Debug, Clone)]
pub struct Broken {
    pub path: PathBuf,
    pub error: String,
}

/// Lo que se encontró en el directorio.
///
/// Nada de esto es fatal por sí solo. Un fichero roto **no** puede esconder
/// a los demás: la primera versión abortaba el escaneo entero al primer
/// error y bastaba un pipeline con un secreto sin definir para que la
/// ventana apareciera vacía, como si no hubiera nada. Ahora cada problema se
/// reporta por separado y lo demás sigue funcionando.
#[derive(Debug, Clone, Default)]
pub struct Discovery {
    pub entries: Vec<Entry>,
    /// Ficheros que no se pudieron cargar, con su motivo.
    pub broken: Vec<Broken>,
    /// Problemas del conjunto: nombres repetidos, `after` colgando.
    pub problems: Vec<String>,
}

impl Discovery {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Carga todos los pipelines de un directorio, en orden de nombre de fichero.
pub fn discover(dir: &Path) -> Result<Discovery> {
    if !dir.is_dir() {
        return Err(OrchError::Other(format!(
            "`{}` no es un directorio",
            dir.display()
        )));
    }

    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| OrchError::Other(format!("no se pudo leer `{}`: {e}", dir.display())))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.is_file()
                && path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| {
                        ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml")
                    })
        })
        .collect();
    paths.sort();

    let mut found = Discovery::default();

    for path in paths {
        let spec = match PipelineSpec::from_path(&path) {
            Ok(spec) => spec,
            Err(err) => {
                found.broken.push(Broken {
                    path,
                    error: err.to_string(),
                });
                continue;
            }
        };

        // Dos pipelines con el mismo nombre harían ambiguos los
        // encadenamientos y el control de concurrencia. Se queda el primero.
        if found.entries.iter().any(|e| e.name == spec.name) {
            found.problems.push(format!(
                "hay dos pipelines llamados `{}`; se ignora `{}`",
                spec.name,
                path.display()
            ));
            continue;
        }

        match Entry::from_spec(&spec, &path) {
            Ok(entry) => found.entries.push(entry),
            Err(err) => found.broken.push(Broken {
                path,
                error: err.to_string(),
            }),
        }
    }

    // Un `after` que apunta a un pipeline que no existe nunca dispararía, y
    // callarlo sería peor que avisar.
    let names: Vec<&str> = found.entries.iter().map(|e| e.name.as_str()).collect();
    let dangling: Vec<String> = found
        .entries
        .iter()
        .flat_map(|entry| {
            entry
                .after
                .iter()
                .filter(|upstream| !names.contains(&upstream.as_str()))
                .map(|upstream| {
                    format!(
                        "`{}` espera a `{upstream}`, que no está en el directorio: no arrancará solo",
                        entry.name
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    found.problems.extend(dangling);

    Ok(found)
}
