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
