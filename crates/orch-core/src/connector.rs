//! Contratos que implementa todo conector o transformación.
//!
//! Los tres traits son deliberadamente simétricos: cada uno recibe un
//! [`NodeContext`] con los datos de la ejecución y trabaja contra [`Input`] /
//! [`Output`]. Nada en esta capa conoce el DAG, lo que permite probar un
//! conector aislado y, más adelante, ejecutar el mismo trait sobre un
//! transporte distinto (plugin WASM, nodo remoto).
//!
//! Además del método de ejecución, cada trait tiene un gancho de
//! **planificación** que corre en `orch validate`: declara el esquema que
//! produce o comprueba el que recibe. Es opcional —lo que no se sabe se
//! devuelve como `None`— pero cuanto más declare un conector, más errores se
//! detectan sin leer datos.

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::error::Result;
use crate::io::{Input, Output};
use crate::schema::InputSchemas;
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
    /// Esquemas de entrada resueltos en `prepare`, cuando se conocen.
    pub inputs: std::sync::Arc<InputSchemas>,
}

/// Origen de datos: produce batches sin consumir ninguno.
#[async_trait]
pub trait Source: Send + Sync {
    /// Nombre del conector tal y como aparece en el YAML.
    fn connector(&self) -> &str;

    /// Esquema que producirá, si puede saberse sin leer datos.
    ///
    /// Un fallo al averiguarlo (un fichero que todavía no existe porque lo
    /// genera un paso anterior) debe devolver `Ok(None)`, no `Err`: `validate`
    /// no puede exigir que las fuentes estén disponibles.
    async fn schema(&self) -> Result<Option<SchemaRef>> {
        Ok(None)
    }

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()>;
}

/// Transformación: consume batches y produce batches.
#[async_trait]
pub trait Transform: Send + Sync {
    fn op(&self) -> &str;

    /// Comprueba el cableado y declara el esquema de salida.
    ///
    /// Es el sitio donde una transformación rechaza una entrada que no sabe
    /// manejar (dos flujos donde espera uno) o una columna que no existe.
    async fn plan(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        let _ = inputs;
        Ok(None)
    }

    async fn apply(&self, ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()>;
}

/// Destino de datos: consume batches sin producir ninguno.
#[async_trait]
pub trait Sink: Send + Sync {
    fn connector(&self) -> &str;

    /// Comprueba que puede escribir lo que va a recibir.
    async fn plan(&self, inputs: &InputSchemas) -> Result<()> {
        let _ = inputs;
        Ok(())
    }

    async fn write(&self, ctx: &NodeContext, input: &mut Input) -> Result<()>;
}
