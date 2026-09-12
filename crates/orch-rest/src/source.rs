//! Origen REST: pagina una API y entrega sus registros como lotes de Arrow.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use orch_core::{NodeContext, OrchError, Output, Registry, Result, Source};
use reqwest::Method;
use serde::Deserialize;
use serde_json::Value;

use crate::http::{build_url, default_timeout_ms, parse_method, HttpClient, HttpRetryConfig};
use crate::json::{self, ColumnSpec, JsonBatcher};

pub fn register(registry: &mut Registry) {
    registry.register_source("rest", |node, config| {
        let source: Arc<dyn Source> = Arc::new(RestSource::new(node, config)?);
        Ok(source)
    });
}

/// Cómo pedir la siguiente página.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Pagination {
    /// Una sola petición.
    #[default]
    None,
    /// `?page=1`, `?page=2`… hasta que una página vuelve vacía.
    Page {
        #[serde(default = "page_param")]
        param: String,
        #[serde(default = "one")]
        start: u64,
        #[serde(default)]
        max_pages: Option<u64>,
    },
    /// `?offset=0`, `?offset=100`… incrementando por los registros recibidos.
    Offset {
        #[serde(default = "offset_param")]
        param: String,
        #[serde(default)]
        max_pages: Option<u64>,
    },
    /// El cursor de la siguiente página sale de un campo de la respuesta.
    Cursor {
        #[serde(default = "cursor_param")]
        param: String,
        /// Ruta con puntos hasta el cursor (`meta.next_cursor`).
        next_path: String,
        #[serde(default)]
        max_pages: Option<u64>,
    },
}

impl Pagination {
    fn max_pages(&self) -> Option<u64> {
        match self {
            Pagination::None => Some(1),
            Pagination::Page { max_pages, .. }
            | Pagination::Offset { max_pages, .. }
            | Pagination::Cursor { max_pages, .. } => *max_pages,
        }
    }
}

fn page_param() -> String {
    "page".to_string()
}
fn offset_param() -> String {
    "offset".to_string()
}
fn cursor_param() -> String {
    "cursor".to_string()
}
fn one() -> u64 {
    1
}
fn get() -> String {
    "GET".to_string()
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RestSourceConfig {
    pub url: String,
    #[serde(default = "get")]
    pub method: String,
    /// Cabeceras fijas. Aquí es donde va `Authorization: Bearer ${env:TOKEN}`.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    /// Ruta con puntos hasta la lista de registros (`data.items`). Sin ella,
    /// la respuesta debe ser un array.
    #[serde(default)]
    pub records_path: Option<String>,
    /// Esquema declarado. Sin él se deduce de la primera página, pero
    /// entonces `validate` no puede comprobar nada aguas abajo: una API no se
    /// puede llamar durante la validación.
    #[serde(default)]
    pub schema: Option<Vec<ColumnSpec>>,
    #[serde(default)]
    pub pagination: Pagination,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub rate_limit_per_second: Option<f64>,
    #[serde(default)]
    pub retry: HttpRetryConfig,
    #[serde(default)]
    pub batch_size: Option<usize>,
}

pub struct RestSource {
    node: String,
    config: RestSourceConfig,
    method: Method,
    declared: Option<SchemaRef>,
    client: HttpClient,
}

impl RestSource {
    pub fn new(node: impl Into<String>, config: RestSourceConfig) -> Result<Self> {
        let node = node.into();
        let method = parse_method(&node, &config.method)?;
        let declared = match &config.schema {
            Some(columns) => Some(json::schema_from(&node, columns)?),
            None => None,
        };
        let client = HttpClient::new(
            &node,
            &config.headers,
            config.timeout_ms,
            config.rate_limit_per_second,
            config.retry.clone(),
        )?;
        Ok(Self {
            node,
            config,
            method,
            declared,
            client,
        })
    }

    /// Pide una página y devuelve el cuerpo ya parseado.
    async fn fetch(&self, page: u64, offset: u64, cursor: Option<&str>) -> Result<Value> {
        let paginated: Option<(String, String)> = match &self.config.pagination {
            Pagination::None => None,
            Pagination::Page { param, .. } => Some((param.clone(), page.to_string())),
            Pagination::Offset { param, .. } => Some((param.clone(), offset.to_string())),
            Pagination::Cursor { param, .. } => {
                cursor.map(|value| (param.clone(), value.to_string()))
            }
        };

        let params = self
            .config
            .query
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .chain(
                paginated
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            );
        let url = build_url(&self.node, &self.config.url, params)?;

        let describe = format!("{} {}", self.method, url);
        let response = self
            .client
            .send(&describe, || self.client.request(&self.method, url.clone()))
            .await?;

        response.json::<Value>().await.map_err(|e| {
            OrchError::node(&self.node, format!("la respuesta no es JSON válido: {e}"))
        })
    }
}

#[async_trait]
impl Source for RestSource {
    fn connector(&self) -> &str {
        "rest"
    }

    /// Sólo si está declarado: llamar a la API durante `validate` tendría
    /// efectos secundarios y exigiría red.
    async fn schema(&self) -> Result<Option<SchemaRef>> {
        Ok(self.declared.clone())
    }

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        let batch_size = self.config.batch_size.unwrap_or(ctx.settings.batch_size);
        if batch_size == 0 {
            return Err(OrchError::config(&self.node, "`batch_size` debe ser > 0"));
        }

        let mut batcher = match &self.declared {
            Some(schema) => Some(JsonBatcher::new(
                &self.node,
                SchemaRef::clone(schema),
                batch_size,
            )?),
            None => None,
        };

        let mut page = match &self.config.pagination {
            Pagination::Page { start, .. } => *start,
            _ => 0,
        };
        let mut offset: u64 = 0;
        let mut cursor: Option<String> = None;
        let mut pages: u64 = 0;

        loop {
            let body = self.fetch(page, offset, cursor.as_deref()).await?;
            let records = json::records_at(&self.node, &body, self.config.records_path.as_deref())?;
            let received = records.len();
            pages += 1;

            if batcher.is_none() && !records.is_empty() {
                let schema = json::infer(&self.node, &records)?;
                tracing::debug!(
                    node = %self.node,
                    columns = schema.fields().len(),
                    "esquema deducido de la primera página"
                );
                batcher = Some(JsonBatcher::new(&self.node, schema, batch_size)?);
            }
            if let Some(batcher) = batcher.as_mut() {
                for batch in batcher.push(&self.node, &records)? {
                    output.send(batch).await?;
                }
            }

            tracing::debug!(node = %self.node, pages, received, "página leída");

            if output.is_closed() {
                break;
            }
            if let Some(max) = self.config.pagination.max_pages() {
                if pages >= max {
                    break;
                }
            }

            match &self.config.pagination {
                Pagination::None => break,
                // Una página vacía es el final: la mayoría de APIs
                // paginadas no dicen cuántas hay.
                Pagination::Page { .. } => {
                    if received == 0 {
                        break;
                    }
                    page += 1;
                }
                Pagination::Offset { .. } => {
                    if received == 0 {
                        break;
                    }
                    offset += received as u64;
                }
                Pagination::Cursor { next_path, .. } => match json::string_at(&body, next_path) {
                    Some(next) if !next.is_empty() => cursor = Some(next),
                    _ => break,
                },
            }
        }

        Ok(())
    }
}
