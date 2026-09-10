//! Esquemas conocidos antes de ejecutar.
//!
//! Cada nodo puede declarar qué columnas produce sin leer un solo dato. El
//! ejecutor recorre el DAG en orden topológico durante `prepare` y propaga
//! esos esquemas, de modo que `orch validate` detecta una columna inexistente
//! o un fan-in con esquemas incompatibles antes de tocar ninguna fuente.
//!
//! La propagación es *best effort*: un nodo que no sabe su esquema devuelve
//! `None` y la cadena se corta ahí sin invalidar el pipeline. Eso mantiene
//! utilizables los conectores que sólo descubren su forma al leer.

use arrow::datatypes::SchemaRef;

use crate::error::{OrchError, Result};
use crate::spec::NodeId;

/// Una entrada de un nodo: de dónde viene y qué forma tiene.
#[derive(Debug, Clone)]
pub struct PortSchema {
    /// Nombre del puerto. Por defecto, el id del nodo de origen.
    pub port: String,
    pub upstream: NodeId,
    /// `None` cuando el origen no puede declararlo sin leer datos.
    pub schema: Option<SchemaRef>,
}

/// Las entradas de un nodo, en el orden en que se declararon las aristas.
#[derive(Debug, Clone, Default)]
pub struct InputSchemas {
    ports: Vec<PortSchema>,
}

impl InputSchemas {
    pub fn new(ports: Vec<PortSchema>) -> Self {
        Self { ports }
    }

    pub fn ports(&self) -> &[PortSchema] {
        &self.ports
    }

    pub fn len(&self) -> usize {
        self.ports.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ports.is_empty()
    }

    pub fn get(&self, port: &str) -> Option<&PortSchema> {
        self.ports.iter().find(|p| p.port == port)
    }

    /// Todos los puertos declaran esquema. Requisito para planificar un join.
    pub fn all_known(&self) -> bool {
        !self.ports.is_empty() && self.ports.iter().all(|p| p.schema.is_some())
    }

    /// El único puerto de entrada, si el nodo tiene exactamente uno.
    pub fn single(&self) -> Option<&PortSchema> {
        match self.ports.as_slice() {
            [only] => Some(only),
            _ => None,
        }
    }

    /// Exige exactamente una entrada.
    ///
    /// Las operaciones que trabajan sobre una tabla (`filter`, `derive`,
    /// `aggregate`) no saben qué hacer con dos: fallan aquí, en `validate`,
    /// en vez de concatenar en silencio dos flujos que el usuario quería unir.
    pub fn require_single(&self, node: &str, op: &str) -> Result<&PortSchema> {
        match self.ports.as_slice() {
            [only] => Ok(only),
            [] => Err(OrchError::Validation(format!(
                "`{node}`: `{op}` necesita una entrada"
            ))),
            many => Err(OrchError::Validation(format!(
                "`{node}`: `{op}` sólo admite una entrada y tiene {} ({}). \
                 Usa `sql` si quieres combinarlas.",
                many.len(),
                many.iter()
                    .map(|p| p.port.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// Esquema resultante de concatenar todas las entradas.
    ///
    /// El fan-in del motor apila flujos, así que todos deben tener la misma
    /// forma. Si dos difieren, el error sale en `validate` y no a mitad de una
    /// escritura.
    pub fn concatenated(&self, node: &str) -> Result<Option<SchemaRef>> {
        let mut known = self
            .ports
            .iter()
            .filter_map(|p| p.schema.as_ref().map(|s| (p, s)));

        let Some((first_port, first)) = known.next() else {
            return Ok(None);
        };
        for (port, schema) in known {
            if schema.fields() != first.fields() {
                return Err(OrchError::Validation(format!(
                    "`{node}`: las entradas `{}` y `{}` tienen esquemas distintos y el \
                     fan-in las concatena.\n  {}: {}\n  {}: {}",
                    first_port.port,
                    port.port,
                    first_port.port,
                    describe(first),
                    port.port,
                    describe(schema),
                )));
            }
        }

        // Si algún puerto no declaró esquema, no podemos afirmar el resultado.
        if self.ports.iter().any(|p| p.schema.is_none()) {
            return Ok(None);
        }
        Ok(Some(SchemaRef::clone(first)))
    }
}

/// Descripción corta de un esquema, para mensajes de error legibles.
pub fn describe(schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("{}: {}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}
