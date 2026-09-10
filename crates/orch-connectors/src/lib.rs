//! # orch-connectors
//!
//! Conectores y transformaciones nativas de la Fase 0.
//!
//! Todo el I/O bloqueante (ficheros, y más adelante drivers de base de datos)
//! se ejecuta en `spawn_blocking` y se comunica con el runtime async por un
//! canal corto. El runtime de Tokio nunca se bloquea, que es lo que permite
//! que decenas de nodos convivan en unos pocos hilos.

pub mod csv;
pub mod generator;
pub mod memory;
pub mod null;
pub mod parquet;
pub mod transforms;

mod util;

use std::sync::Arc;

use orch_core::Registry;

/// Registro con todos los componentes nativos disponibles.
pub fn default_registry() -> Registry {
    let mut registry = Registry::new();
    csv::register(&mut registry);
    generator::register(&mut registry);
    null::register(&mut registry);
    parquet::register(&mut registry);
    transforms::register(&mut registry);
    registry
}

/// Igual que [`default_registry`], listo para pasarse al ejecutor.
pub fn default_registry_arc() -> Arc<Registry> {
    Arc::new(default_registry())
}
