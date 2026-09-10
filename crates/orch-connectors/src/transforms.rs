//! Transformaciones nativas de la Fase 0.
//!
//! Son operaciones de metadatos o de recorte: proyectar, renombrar y limitar
//! no tocan los buffers de Arrow, sólo reordenan o comparten `Arc`s. Las
//! transformaciones con expresiones (filtros, agregaciones, joins, SQL) entran
//! en la Fase 0.2 sobre DataFusion, que reutilizará este mismo trait.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{parse_config, Input, NodeContext, OrchError, Output, Registry, Result, Transform};
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

fn available_columns(batch: &RecordBatch) -> String {
    batch
        .schema()
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
}

#[async_trait]
impl Transform for Select {
    fn op(&self) -> &str {
        "select"
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
                indices = Some(
                    self.config
                        .columns
                        .iter()
                        .map(|name| {
                            batch.schema().index_of(name).map_err(|_| {
                                OrchError::node(
                                    &self.node,
                                    format!(
                                        "la columna `{name}` no existe (disponibles: {})",
                                        available_columns(&batch)
                                    ),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
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
}

#[async_trait]
impl Transform for Rename {
    fn op(&self) -> &str {
        "rename"
    }

    async fn apply(&self, _ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        let mut renamed_schema = None;

        while let Some(batch) = input.recv().await? {
            if renamed_schema.is_none() {
                let source_schema = batch.schema();
                for old in self.config.columns.keys() {
                    if source_schema.index_of(old).is_err() {
                        return Err(OrchError::node(
                            &self.node,
                            format!(
                                "la columna `{old}` no existe (disponibles: {})",
                                available_columns(&batch)
                            ),
                        ));
                    }
                }
                let fields: Vec<Field> = source_schema
                    .fields()
                    .iter()
                    .map(|field| match self.config.columns.get(field.name()) {
                        Some(new_name) => field.as_ref().clone().with_name(new_name.clone()),
                        None => field.as_ref().clone(),
                    })
                    .collect();
                renamed_schema = Some(Arc::new(
                    Schema::new(fields).with_metadata(source_schema.metadata().clone()),
                ));
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
    #[allow(dead_code)]
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
