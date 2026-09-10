//! # orch-rest
//!
//! Conector REST: lee una API paginada y publica lotes contra otra.
//!
//! Va en su propio crate para que quien sólo mueva ficheros no arrastre un
//! cliente HTTP ni TLS. La misma razón por la que `orch-sql` está aparte, y
//! el camino natural hacia los plugins de la Fase 2.
//!
//! Lo que distingue a una API de un fichero, y que aquí está resuelto:
//! paginación, límite de tasa, y que un 503 es transitorio mientras que un
//! 401 no. Las cabeceras nunca se registran en los logs: llevan tokens.

mod http;
mod json;
mod sink;
mod source;

use orch_core::Registry;

pub use http::HttpRetryConfig;
pub use json::ColumnSpec;
pub use sink::{BodyFormat, RestSink, RestSinkConfig};
pub use source::{Pagination, RestSource, RestSourceConfig};

/// Añade el origen y el destino REST a un registro existente.
pub fn register(registry: &mut Registry) {
    source::register(registry);
    sink::register(registry);
}
