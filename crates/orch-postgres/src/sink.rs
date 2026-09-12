//! Destino PostgreSQL: carga masiva con `COPY ... FORMAT binary`.
//!
//! Nunca `INSERT` fila a fila. Y binario, no CSV, por una razón de
//! corrección y no de velocidad: en `COPY ... FORMAT csv` una cadena vacía
//! sin comillas significa NULL, y el escritor CSV de Arrow emite exactamente
//! lo mismo para un NULL que para un `""`. Cualquier columna de texto
//! nullable se corrompería en silencio. El formato binario no tiene esa
//! ambigüedad.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::pin_mut;
use orch_core::{Input, NodeContext, OrchError, Registry, Result, Sink};
use postgres_types::{ToSql, Type};
use serde::Deserialize;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::Transaction;

use crate::conn::{connect, describe, quote_ident, quote_qualified, TlsConfig};
use crate::types::{arrow_type, value_at, SqlValue};

pub fn register(registry: &mut Registry) {
    registry.register_sink("postgres", |node, config| {
        let sink: Arc<dyn Sink> = Arc::new(PostgresSink::new(node, config)?);
        Ok(sink)
    });
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresSinkConfig {
    pub dsn: String,
    /// Tabla de destino, opcionalmente cualificada.
    pub table: String,
    /// Columnas a escribir, en el orden en que llegan los datos. Por defecto,
    /// las del propio lote.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Vacía la tabla antes de cargar, dentro de la misma transacción: o se
    /// sustituye entera o no se toca.
    #[serde(default)]
    pub truncate: bool,
    #[serde(default)]
    pub tls: TlsConfig,
}

pub struct PostgresSink {
    node: String,
    config: PostgresSinkConfig,
}

impl PostgresSink {
    pub fn new(node: impl Into<String>, config: PostgresSinkConfig) -> Result<Self> {
        let node = node.into();
        if config.table.trim().is_empty() {
            return Err(OrchError::config(&node, "`table` no puede estar vacía"));
        }
        if matches!(&config.columns, Some(columns) if columns.is_empty()) {
            return Err(OrchError::config(&node, "`columns` no puede estar vacío"));
        }
        Ok(Self { node, config })
    }

    /// Tipos de las columnas de destino, preguntados al servidor.
    ///
    /// Se obtienen preparando un `SELECT ... WHERE false`: no lee ninguna
    /// fila y devuelve los tipos exactos, que es lo que necesita el formato
    /// binario.
    async fn destination_types(
        &self,
        transaction: &Transaction<'_>,
        columns: &[String],
    ) -> Result<Vec<Type>> {
        let projection = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let probe = format!(
            "SELECT {projection} FROM {} WHERE false",
            quote_qualified(&self.config.table)
        );

        let statement = transaction.prepare(&probe).await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!(
                    "no se pudo consultar la tabla `{}`: {}",
                    self.config.table,
                    describe(&e)
                ),
            )
        })?;

        statement
            .columns()
            .iter()
            .map(|column| {
                // Se valida aquí para fallar antes de abrir el COPY.
                arrow_type(&self.node, column.name(), column.type_())?;
                Ok(column.type_().clone())
            })
            .collect()
    }
}

#[async_trait]
impl Sink for PostgresSink {
    fn connector(&self) -> &str {
        "postgres"
    }

    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> Result<()> {
        // No se abre nada hasta saber qué columnas llegan.
        let Some(first) = input.recv().await? else {
            tracing::debug!(node = %self.node, "no llegó ninguna fila: nada que cargar");
            return Ok(());
        };

        let columns: Vec<String> = match &self.config.columns {
            Some(columns) => columns.clone(),
            None => first
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        };
        if columns.len() != first.num_columns() {
            return Err(OrchError::node(
                &self.node,
                format!(
                    "llegan {} columnas y `columns` declara {}",
                    first.num_columns(),
                    columns.len()
                ),
            ));
        }

        let mut client = connect(&self.node, &self.config.dsn, &self.config.tls).await?;
        let transaction = client.transaction().await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!("no se pudo abrir la transacción: {}", describe(&e)),
            )
        })?;

        let types = self.destination_types(&transaction, &columns).await?;

        if self.config.truncate {
            let sql = format!("TRUNCATE {}", quote_qualified(&self.config.table));
            transaction.execute(&sql, &[]).await.map_err(|e| {
                OrchError::node(
                    &self.node,
                    format!("no se pudo vaciar la tabla: {}", describe(&e)),
                )
            })?;
        }

        let copy = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
            quote_qualified(&self.config.table),
            columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let sink = transaction
            .copy_in::<_, Bytes>(copy.as_str())
            .await
            .map_err(|e| {
                OrchError::node(
                    &self.node,
                    format!("no se pudo iniciar el COPY: {}", describe(&e)),
                )
            })?;

        let writer = BinaryCopyInWriter::new(sink, &types);
        pin_mut!(writer);

        let mut batch = Some(first);
        let mut rows_written: u64 = 0;

        loop {
            let Some(current) = batch.take() else { break };
            rows_written += self
                .write_batch(writer.as_mut(), &current, &columns, &types)
                .await?;

            batch = match input.recv().await {
                Ok(next) => next,
                // Sin `finish`, el COPY se aborta y la transacción no llega a
                // confirmarse: la tabla queda como estaba.
                Err(err) => return Err(err),
            };
        }

        let copied = writer.finish().await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!("el COPY falló al cerrarse: {}", describe(&e)),
            )
        })?;

        transaction.commit().await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!("no se pudo confirmar la carga: {}", describe(&e)),
            )
        })?;

        tracing::debug!(
            node = %self.node,
            table = %self.config.table,
            rows = copied,
            "carga confirmada"
        );
        debug_assert_eq!(copied, rows_written);
        Ok(())
    }
}

impl PostgresSink {
    /// Vuelca un lote en el flujo del COPY.
    async fn write_batch(
        &self,
        writer: std::pin::Pin<&mut BinaryCopyInWriter>,
        batch: &RecordBatch,
        columns: &[String],
        types: &[Type],
    ) -> Result<u64> {
        // Convertir la columna entera de una vez deja que Arrow resuelva el
        // ensanchado de tipos y dé buenos errores; después sólo hay que leer.
        let converted: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .zip(types)
            .zip(columns)
            .map(|((array, pg), name)| {
                let target = arrow_type(&self.node, name, pg)?;
                if array.data_type() == &target {
                    return Ok(Arc::clone(array));
                }
                arrow::compute::cast(array, &target).map_err(|e| {
                    OrchError::node(
                        &self.node,
                        format!(
                            "la columna `{name}` llega como {} y la tabla la espera como \
                             {target}: {e}",
                            array.data_type()
                        ),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut writer = writer;
        let mut values: Vec<SqlValue> = Vec::with_capacity(converted.len());

        for row in 0..batch.num_rows() {
            values.clear();
            for ((array, pg), name) in converted.iter().zip(types).zip(columns) {
                values.push(value_at(&self.node, name, array, row, pg)?);
            }
            let refs: Vec<&(dyn ToSql + Sync)> = values
                .iter()
                .map(|value| value as &(dyn ToSql + Sync))
                .collect();

            writer.as_mut().write(&refs).await.map_err(|e| {
                OrchError::node(
                    &self.node,
                    format!("fallo al escribir una fila: {}", describe(&e)),
                )
            })?;
        }

        Ok(batch.num_rows() as u64)
    }
}
