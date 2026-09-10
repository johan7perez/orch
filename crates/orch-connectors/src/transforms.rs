//! Transformaciones nativas de la Fase 0.
//!
//! Son operaciones de metadatos o de recorte: proyectar, renombrar y limitar
//! no tocan los buffers de Arrow, sólo reordenan o comparten `Arc`s. Las
//! transformaciones con expresiones viven en `orch-sql`, sobre DataFusion.
//!
//! Las tres resuelven su esquema de salida en `validate`, así que una columna
//! mal escrita se detecta antes de leer un solo dato.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{
    parse_config, Input, InputSchemas, NodeContext, OrchError, Output, Registry, Result, Transform,
};
use serde::Deserialize;

pub fn register(registry: &mut Registry) {
    registry.register_transform("select", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(Select::new(node, parse_config(node, config)?));
        Ok(t)
    });
    registry.register_transform("rename", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(Rename::new(node, parse_config(node, config)?));
        Ok(t)
    });
    registry.register_transform("limit", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(Limit::new(node, parse_config(node, config)?));
        Ok(t)
    });
}

fn column_names(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

// --- select -----------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectConfig {
    /// Columnas a conservar, en el orden en que deben quedar.
    pub columns: Vec<String>,
}

pub struct Select {
    node: String,
    config: SelectConfig,
}

impl Select {
    pub fn new(node: impl Into<String>, config: SelectConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }

    /// Índices de las columnas pedidas, en el orden en que se pidieron.
    fn resolve(&self, schema: &Schema) -> Result<Vec<usize>> {
        self.config
            .columns
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    OrchError::node(
                        &self.node,
                        format!(
                            "la columna `{name}` no existe (disponibles: {})",
                            column_names(schema)
                        ),
                    )
                })
            })
            .collect()
    }
}

#[async_trait]
impl Transform for Select {
    fn op(&self) -> &str {
        "select"
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        if self.config.columns.is_empty() {
            return Err(OrchError::config(
                &self.node,
                "`columns` no puede estar vacío",
            ));
        }
        let Some(schema) = inputs.concatenated(&self.node)? else {
            return Ok(None);
        };
        // Resolver aquí los índices convierte "esa columna no existe" en un
        // error de `validate` en vez de uno a mitad de ejecución.
        let indices = self.resolve(&schema)?;
        Ok(Some(Arc::new(schema.project(&indices)?)))
    }

    async fn apply(&self, _ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        if self.config.columns.is_empty() {
            return Err(OrchError::config(
                &self.node,
                "`columns` no puede estar vacío",
            ));
        }
        // El esquema no cambia entre batches, así que los índices se resuelven
        // una sola vez.
        let mut indices: Option<Vec<usize>> = None;

        while let Some(batch) = input.recv().await? {
            if indices.is_none() {
                indices = Some(self.resolve(&batch.schema())?);
            }
            let indices = indices.as_deref().expect("resuelto justo arriba");
            output.send(batch.project(indices)?).await?;
        }
        Ok(())
    }
}

// --- rename -----------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenameConfig {
    /// Mapa `nombre_actual: nombre_nuevo`.
    pub columns: BTreeMap<String, String>,
}

pub struct Rename {
    node: String,
    config: RenameConfig,
}

impl Rename {
    pub fn new(node: impl Into<String>, config: RenameConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }

    /// Esquema con las columnas renombradas.
    fn renamed(&self, source: &Schema) -> Result<SchemaRef> {
        for old in self.config.columns.keys() {
            if source.index_of(old).is_err() {
                return Err(OrchError::node(
                    &self.node,
                    format!(
                        "la columna `{old}` no existe (disponibles: {})",
                        column_names(source)
                    ),
                ));
            }
        }
        let fields: Vec<Field> = source
            .fields()
            .iter()
            .map(|field| match self.config.columns.get(field.name()) {
                Some(new_name) => field.as_ref().clone().with_name(new_name.clone()),
                None => field.as_ref().clone(),
            })
            .collect();
        Ok(Arc::new(
            Schema::new(fields).with_metadata(source.metadata().clone()),
        ))
    }
}

#[async_trait]
impl Transform for Rename {
    fn op(&self) -> &str {
        "rename"
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        match inputs.concatenated(&self.node)? {
            Some(schema) => Ok(Some(self.renamed(&schema)?)),
            None => Ok(None),
        }
    }

    async fn apply(&self, _ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        let mut renamed_schema = None;

        while let Some(batch) = input.recv().await? {
            if renamed_schema.is_none() {
                renamed_schema = Some(self.renamed(&batch.schema())?);
            }
            let schema = Arc::clone(renamed_schema.as_ref().expect("resuelto justo arriba"));

            // Sólo cambia el esquema: las columnas se reutilizan tal cual.
            let renamed = RecordBatch::try_new(schema, batch.columns().to_vec())?;
            output.send(renamed).await?;
        }
        Ok(())
    }
}

// --- limit ------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitConfig {
    pub rows: u64,
}

pub struct Limit {
    node: String,
    config: LimitConfig,
}

impl Limit {
    pub fn new(node: impl Into<String>, config: LimitConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }
}

#[async_trait]
impl Transform for Limit {
    fn op(&self) -> &str {
        "limit"
    }

    /// Recortar filas no cambia las columnas.
    async fn plan(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        inputs.concatenated(&self.node)
    }

    async fn apply(&self, _ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        let mut remaining = self.config.rows;

        while remaining > 0 {
            let Some(batch) = input.recv().await? else {
                break;
            };
            let take = remaining.min(batch.num_rows() as u64) as usize;
            let batch = if take == batch.num_rows() {
                batch
            } else {
                batch.slice(0, take)
            };
            remaining -= take as u64;
            output.send(batch).await?;
        }

        // Alcanzado el límite se deja de leer. Al soltar el `Input`, los nodos
        // de arriba dejan de producir en cuanto llenan su canal: un `limit`
        // sobre una tabla enorme no la lee entera.
        Ok(())
    }
}
