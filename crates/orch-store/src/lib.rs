//! # orch-store
//!
//! Persistencia de ejecuciones, métricas y eventos en DuckDB.
//!
//! Hasta ahora todo lo que sabía el motor moría con el proceso. Aquí queda
//! guardado, y como DuckDB es columnar y embebido, consultarlo es rápido sin
//! montar ningún servidor: «cuánto tardó de media este pipeline el último
//! mes» es un `GROUP BY` sobre un fichero local.
//!
//! El escritor de eventos vive en su propia tarea y nunca frena la
//! ejecución: se engancha al canal `broadcast` del ejecutor, que descarta
//! eventos si el consumidor se retrasa en vez de aplicar contrapresión.

mod conexion;
mod schema;
mod store;
pub mod versions;
mod writer;

pub use store::{
    AlertRow, EventRow, NodeRow, ProgressRow, RunSummary, RunVersion, Store, VersionRuns,
};
pub use writer::EventWriter;

use std::path::PathBuf;

/// Dónde vive el historial si no se dice otra cosa (`--store` u
/// `ORCH_STORE`).
///
/// En la carpeta de datos del usuario —`~/.local/share/orch` en Linux,
/// `~/Library/Application Support/orch` en macOS, `%LOCALAPPDATA%\orch` en
/// Windows—, y la misma para la CLI, el demonio y la aplicación: así
/// comparten historial se lancen desde donde se lancen. Relativo al
/// directorio de trabajo no servía: una aplicación instalada arranca donde
/// quiere el sistema (en macOS, en `/`, donde no se puede escribir).
///
/// Si el sistema no tiene carpeta de datos (un usuario sin `HOME`), el de
/// siempre: `orch.duckdb` en el directorio de trabajo.
pub fn ruta_por_defecto() -> PathBuf {
    dirs::data_local_dir()
        .map(|datos| datos.join("orch").join("orch.duckdb"))
        .unwrap_or_else(|| PathBuf::from("orch.duckdb"))
}
