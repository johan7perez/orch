//! Origen PostgreSQL: lee por cursor, sin traerse la tabla entera a memoria.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{
    parse_config, NodeContext, OrchError, Output, PushdownOp, Registry, Result, Source,
};
use serde::Deserialize;
use serde_json::json;
use tokio_postgres::Statement;

use crate::conn::{connect, describe, quote_ident, quote_qualified, TlsConfig};
use crate::types::{arrow_type, ColumnBuilder};

pub fn register(registry: &mut Registry) {
    registry.register_source("postgres", |node, config| {
        let source: Arc<dyn Source> =
            Arc::new(PostgresSource::new(node, parse_config(node, config)?)?);
        Ok(source)
    });

    // Lo que se empuja aquí lo resuelve el servidor, y por la red viaja sólo
    // el resultado. Es el salto que más se nota de todos.
    //
    // Sólo con la forma `table`: con `query` habría que envolverla en una
    // subconsulta y eso cambia cómo la planifica PostgreSQL. Quien escribe
    // su propio SQL ya puede poner ahí el WHERE.
    registry.register_pushdown("postgres", |config, op| {
        let Some(map) = config.as_object_mut() else {
            return false;
        };
        if !map.get("table").is_some_and(|t| t.is_string()) {
            return false;
        }

        match op {
            PushdownOp::Select { columns } => {
                if map.get("columns").is_some_and(|c| !c.is_null()) {
                    return false;
                }
                map.insert("columns".to_string(), json!(columns));
                true
            }
            PushdownOp::Filter { predicate } => {
                // Dos filtros se combinan con AND, que es justo lo que
                // significaba encadenarlos.
                let combined = match map.get("where").and_then(|w| w.as_str()) {
                    Some(existing) if !existing.trim().is_empty() => {
                        format!("({existing}) AND ({predicate})")
                    }
                    _ => predicate.clone(),
                };
                map.insert("where".to_string(), json!(combined));
                true
            }
        }
    });
}

fn default_fetch_size() -> usize {
    10_000
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    /// Cadena de conexión de libpq. La contraseña debería venir de
    /// `${env:...}`, no escrita en el YAML.
    pub dsn: String,
    /// Consulta a ejecutar. Alternativa a `table`.
    #[serde(default)]
    pub query: Option<String>,
    /// Tabla a leer, opcionalmente cualificada (`ventas.pedidos`).
    #[serde(default)]
    pub table: Option<String>,
    /// Columnas a leer de `table`. Por defecto, todas.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Filtro para `table`, sin la palabra `WHERE`.
    #[serde(rename = "where", default)]
    pub filter: Option<String>,
    /// Filas que el servidor entrega por vuelta del cursor.
    #[serde(default = "default_fetch_size")]
    pub fetch_size: usize,
    #[serde(default)]
    pub tls: TlsConfig,
}

pub struct PostgresSource {
    node: String,
    config: PostgresSourceConfig,
    sql: String,
}

impl PostgresSource {
    pub fn new(node: impl Into<String>, config: PostgresSourceConfig) -> Result<Self> {
        let node = node.into();
        let sql = build_sql(&node, &config)?;
        if config.fetch_size == 0 {
            return Err(OrchError::config(
                &node,
                "`fetch_size` debe ser mayor que 0",
            ));
        }
        Ok(Self { node, config, sql })
    }

    /// La consulta que se va a ejecutar. Útil para depurar y para los tests.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Esquema Arrow a partir de los tipos que declara la sentencia.
    fn schema_of(&self, statement: &Statement) -> Result<SchemaRef> {
        let fields = statement
            .columns()
            .iter()
            .map(|column| {
                Ok(Field::new(
                    column.name(),
                    arrow_type(&self.node, column.name(), column.type_())?,
                    // PostgreSQL no dice si una columna de un resultado puede
                    // ser nula, así que se asume que sí.
                    true,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Convierte un grupo de filas en un `RecordBatch`.
    fn build_batch(&self, schema: &SchemaRef, rows: &[tokio_postgres::Row]) -> Result<RecordBatch> {
        let mut builders: Vec<ColumnBuilder> = schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(field.data_type()))
            .collect();

        for row in rows {
            for (index, builder) in builders.iter_mut().enumerate() {
                builder.append(&self.node, schema.field(index).name(), row, index)?;
            }
        }

        let columns = builders.iter_mut().map(|b| b.finish()).collect();
        RecordBatch::try_new(SchemaRef::clone(schema), columns).map_err(Into::into)
    }
}

fn build_sql(node: &str, config: &PostgresSourceConfig) -> Result<String> {
    match (&config.query, &config.table) {
        (Some(_), Some(_)) => Err(OrchError::config(node, "usa `query` o `table`, no las dos")),
        (None, None) => Err(OrchError::config(node, "hace falta `query` o `table`")),

        (Some(query), None) => {
            if config.columns.is_some() || config.filter.is_some() {
                return Err(OrchError::config(
                    node,
                    "`columns` y `where` sólo valen con `table`; con `query` van dentro del SQL",
                ));
            }
            if query.trim().is_empty() {
                return Err(OrchError::config(node, "`query` no puede estar vacía"));
            }
            Ok(query.clone())
        }

        (None, Some(table)) => {
            let projection = match &config.columns {
                Some(columns) if columns.is_empty() => {
                    return Err(OrchError::config(node, "`columns` no puede estar vacío"))
                }
                Some(columns) => columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                None => "*".to_string(),
            };
            let mut sql = format!("SELECT {projection} FROM {}", quote_qualified(table));
            if let Some(filter) = &config.filter {
                if !filter.trim().is_empty() {
                    sql.push_str(&format!(" WHERE ({filter})"));
                }
            }
            Ok(sql)
        }
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn connector(&self) -> &str {
        "postgres"
    }

    /// Prepara la sentencia y lee los tipos que declara el servidor.
    ///
    /// Preparar no ejecuta nada, así que `validate` obtiene el esquema real
    /// —con los tipos exactos— sin tocar un solo dato. Si la base no está
    /// disponible se devuelve `None`: la validación no puede exigir que lo
    /// esté.
    async fn schema(&self) -> Result<Option<SchemaRef>> {
        let client = match connect(&self.node, &self.config.dsn, &self.config.tls).await {
            Ok(client) => client,
            Err(err) => {
                tracing::debug!(
                    node = %self.node,
                    error = %err,
                    "no se pudo consultar el esquema todavía"
                );
                return Ok(None);
            }
        };

        match client.prepare(&self.sql).await {
            // Un SQL mal escrito sí es un error del pipeline y debe salir en
            // `validate`, no a mitad de la ejecución.
            Err(err) => Err(OrchError::node(
                &self.node,
                format!("la consulta no es válida: {}", describe(&err)),
            )),
            Ok(statement) => self.schema_of(&statement).map(Some),
        }
    }

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        let mut client = connect(&self.node, &self.config.dsn, &self.config.tls).await?;

        // El cursor vive dentro de una transacción; al ser sólo lectura, se
        // deshace sola al terminar.
        let transaction = client.transaction().await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!("no se pudo abrir la transacción: {}", describe(&e)),
            )
        })?;

        let statement = transaction.prepare(&self.sql).await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!("la consulta no es válida: {}", describe(&e)),
            )
        })?;
        let schema = self.schema_of(&statement)?;

        let portal = transaction.bind(&statement, &[]).await.map_err(|e| {
            OrchError::node(
                &self.node,
                format!("no se pudo abrir el cursor: {}", describe(&e)),
            )
        })?;

        // Cada vuelta del cursor trae `fetch_size` filas como mucho; nunca
        // está el resultado entero en memoria.
        let fetch = self.config.fetch_size.min(i32::MAX as usize) as i32;
        let batch_size = ctx.settings.batch_size.max(1);

        // Las filas se acumulan entre vueltas del cursor: el tamaño de lote
        // de Arrow no tiene por qué coincidir con el del cursor, y trocear
        // cada vuelta por separado dejaría un lote corto al final de cada
        // una.
        let mut pending: Vec<tokio_postgres::Row> = Vec::with_capacity(batch_size);

        loop {
            let rows = transaction
                .query_portal(&portal, fetch)
                .await
                .map_err(|e| {
                    OrchError::node(&self.node, format!("fallo al leer: {}", describe(&e)))
                })?;
            let exhausted = rows.is_empty();
            pending.extend(rows);

            while pending.len() >= batch_size {
                let rest = pending.split_off(batch_size);
                output.send(self.build_batch(&schema, &pending)?).await?;
                pending = rest;
            }

            if exhausted || output.is_closed() {
                break;
            }
        }

        if !pending.is_empty() {
            output.send(self.build_batch(&schema, &pending)?).await?;
        }

        Ok(())
    }
}
