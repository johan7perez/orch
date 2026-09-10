//! Origen sintético determinista.
//!
//! No es un juguete: es el banco de pruebas del motor. Permite medir el
//! throughput del orquestador aislado del disco y de la red, que es
//! exactamente lo que hay que vigilar en un proyecto cuyo requisito no
//! funcional es "ser el más rápido".

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{parse_config, NodeContext, OrchError, Output, Registry, Result, Source};
use serde::Deserialize;

pub fn register(registry: &mut Registry) {
    registry.register_source("generator", |node, config| {
        let source: Arc<dyn Source> =
            Arc::new(GeneratorSource::new(node, parse_config(node, config)?));
        Ok(source)
    });
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratorConfig {
    /// Filas totales a producir.
    pub rows: u64,
    /// Sobrescribe `settings.batch_size` sólo para este nodo.
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// Incluye una columna `label` de texto. Desactívala para medir el coste
    /// del motor sin la asignación de cadenas.
    #[serde(default = "crate::util::yes")]
    pub with_text: bool,
}

pub struct GeneratorSource {
    node: String,
    config: GeneratorConfig,
    schema: SchemaRef,
}

impl GeneratorSource {
    pub fn new(node: impl Into<String>, config: GeneratorConfig) -> Self {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ];
        if config.with_text {
            fields.push(Field::new("label", DataType::Utf8, false));
        }
        Self {
            node: node.into(),
            config,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    /// Esquema fijo que produce este generador.
    pub fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn make_batch(&self, start: u64, len: usize) -> Result<RecordBatch> {
        let ids = Int64Array::from_iter_values((0..len as u64).map(|i| (start + i) as i64));
        let values =
            Float64Array::from_iter_values((0..len as u64).map(|i| (start + i) as f64 * 1.5));

        let mut columns: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(values)];
        if self.config.with_text {
            let labels = StringArray::from_iter_values(
                (0..len as u64).map(|i| format!("row-{}", start + i)),
            );
            columns.push(Arc::new(labels));
        }

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(Into::into)
    }
}

#[async_trait]
impl Source for GeneratorSource {
    fn connector(&self) -> &str {
        "generator"
    }

    async fn schema(&self) -> Result<Option<SchemaRef>> {
        Ok(Some(self.output_schema()))
    }

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        let batch_size = self.config.batch_size.unwrap_or(ctx.settings.batch_size);
        if batch_size == 0 {
            return Err(OrchError::config(&self.node, "`batch_size` debe ser > 0"));
        }

        let mut produced: u64 = 0;
        while produced < self.config.rows && !output.is_closed() {
            let len = batch_size.min((self.config.rows - produced) as usize);
            output.send(self.make_batch(produced, len)?).await?;
            produced += len as u64;
        }
        Ok(())
    }
}
