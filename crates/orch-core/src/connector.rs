//! Contratos que implementa todo conector o transformación.
//!
//! Los tres traits son deliberadamente simétricos: cada uno recibe un
//! [`NodeContext`] con los datos de la ejecución y trabaja contra [`Input`] /
//! [`Output`]. Nada en esta capa conoce el DAG, lo que permite probar un
//! conector aislado y, más adelante, ejecutar el mismo trait sobre un
//! transporte distinto (plugin WASM, nodo remoto).

use async_trait::async_trait;

use crate::error::Result;
use crate::io::{Input, Output};
use crate::spec::RunSettings;

/// Todo lo que un nodo necesita saber de la ejecución en curso.
#[derive(Debug, Clone)]
pub struct NodeContext {
    pub run_id: String,
    pub pipeline: String,
    pub node: String,
    /// Intento actual, 1-indexado.
    pub attempt: u32,
    pub settings: RunSettings,
}

/// Origen de datos: produce batches sin consumir ninguno.
#[async_trait]
pub trait Source: Send + Sync {
    /// Nombre del conector tal y como aparece en el YAML.
    fn connector(&self) -> &str;

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()>;
}

/// Transformación: consume batches y produce batches.
#[async_trait]
pub trait Transform: Send + Sync {
    fn op(&self) -> &str;

    async fn apply(&self, ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()>;
}

/// Destino de datos: consume batches sin producir ninguno.
#[async_trait]
pub trait Sink: Send + Sync {
    fn connector(&self) -> &str;

    async fn write(&self, ctx: &NodeContext, input: &mut Input) -> Result<()>;
}
