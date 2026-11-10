//! Lo que se sabe de una carpeta de pipelines entera.

use std::path::Path;
use std::sync::Arc;

use orch_core::{Dag, Executor, NodeLineage, PipelineSpec, Registry, Result};

use crate::discover;

/// El lineage de un pipeline mirando también los demás de su carpeta, y el
/// de esos otros (para el impacto).
///
/// Lo usan igual `orch lineage` y la aplicación.
pub async fn lineage_en_carpeta(
    registry: &Arc<Registry>,
    ruta: &Path,
    dag: &Dag,
) -> Result<(Vec<NodeLineage>, Vec<(String, Vec<NodeLineage>)>)> {
    let carpeta = ruta
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let propia = ruta.canonicalize().ok();
    let mut pipelines = vec![(dag.spec().name.clone(), dag.clone())];
    // Los que no se pueden leer o no validan, simplemente no cuentan.
    if let Ok(encontrados) = discover(carpeta) {
        for entrada in encontrados.entries {
            if entrada.path.canonicalize().ok() == propia || entrada.name == dag.spec().name {
                continue;
            }
            if let Ok(otro) = PipelineSpec::from_path(&entrada.path).and_then(Dag::build) {
                pipelines.push((entrada.name.clone(), otro));
            }
        }
    }
    let mut todos = orch_core::lineage::de_varios(&Executor::new(Arc::clone(registry)), &pipelines)
        .await
        .into_iter();
    let propio = todos.next().expect("el propio va primero")?;
    let otros = pipelines
        .into_iter()
        .skip(1)
        .zip(todos)
        .filter_map(|((nombre, _), r)| r.ok().map(|l| (nombre, l)))
        .collect();
    Ok((propio, otros))
}
