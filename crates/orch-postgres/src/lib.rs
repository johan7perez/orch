//! # orch-postgres
//!
//! Conector PostgreSQL.
//!
//! Dos cosas que lo diferencian de leer un fichero:
//!
//! - **La lectura va por cursor**, dentro de una transacción, pidiendo
//!   `fetch_size` filas por vuelta. Una tabla de mil millones de filas no se
//!   trae entera a memoria.
//! - **La escritura es `COPY ... FORMAT binary`**, nunca `INSERT` fila a
//!   fila, y dentro de una transacción. Es el único destino del proyecto que
//!   sí es transaccional: si el pipeline falla a medias, la tabla queda como
//!   estaba.
//!
//! El esquema se obtiene preparando la sentencia, que no ejecuta nada, así
//! que `validate` conoce los tipos exactos del servidor sin leer datos.
//!
//! **Sin TLS todavía.** Un servidor que exija SSL rechazará la conexión con
//! un error claro. Está anotado en `docs/PLAN.md`.

mod conn;
mod sink;
mod source;
mod types;

use orch_core::Registry;

pub use sink::{PostgresSink, PostgresSinkConfig};
pub use source::{PostgresSource, PostgresSourceConfig};

/// Añade el origen y el destino PostgreSQL a un registro existente.
pub fn register(registry: &mut Registry) {
    source::register(registry);
    sink::register(registry);
}
