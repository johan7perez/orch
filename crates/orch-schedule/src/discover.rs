//! Descubrimiento de pipelines en un directorio.

use std::path::Path;

use orch_core::{OrchError, PipelineSpec, Result};

use crate::scheduler::Entry;

/// Carga todos los pipelines de un directorio, en orden de nombre de fichero.
///
/// Un fichero que no se pueda leer detiene el arranque: es preferible que el
/// demonio se niegue a levantarse a que corra a medias sin que nadie lo note.
pub fn discover(dir: &Path) -> Result<Vec<Entry>> {
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

    let mut entries = Vec::with_capacity(paths.len());
    let mut seen: Vec<String> = Vec::new();

    for path in paths {
        let spec = PipelineSpec::from_path(&path)?;
        // Dos pipelines con el mismo nombre harían ambiguos los
        // encadenamientos y el control de concurrencia.
        if seen.contains(&spec.name) {
            return Err(OrchError::Other(format!(
                "hay dos pipelines llamados `{}`; los nombres tienen que ser únicos \
                 dentro del directorio",
                spec.name
            )));
        }
        seen.push(spec.name.clone());
        entries.push(Entry::from_spec(&spec, &path)?);
    }

    // Un `after` que apunta a un pipeline que no existe nunca dispararía, y
    // callarlo sería peor que fallar.
    for entry in &entries {
        for upstream in &entry.after {
            if !seen.contains(upstream) {
                return Err(OrchError::Other(format!(
                    "`{}` espera a `{upstream}`, que no está en el directorio",
                    entry.name
                )));
            }
        }
    }

    Ok(entries)
}
