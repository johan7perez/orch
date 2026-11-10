//! # orch-version
//!
//! Versionado de pipelines.
//!
//! - [`Snapshot`]: la versión que corre en una ejecución, con su texto
//!   exacto y, si el pipeline está en git, el commit del que sale.
//! - [`git::Repo`]: el historial del fichero, su contenido en una versión,
//!   guardar una versión nueva y descartar cambios, a través del `git`
//!   instalado.
//! - [`diff`]: qué cambió entre dos versiones, dicho como pipeline (nodos,
//!   config, aristas) y no como líneas de YAML.
//!
//! Git es opcional en todo esto: sin él, una ejecución sigue guardando la
//! huella y el texto de lo que corrió, y el diff funciona igual entre dos
//! versiones guardadas en el historial.

pub mod diff;
pub mod git;
mod snapshot;

pub use diff::{diff, Change};
pub use git::{Commit, FileState, Repo};
pub use snapshot::{label, Snapshot};
