//! Destino REST: publica los lotes contra una API.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{parse_config, Input, NodeContext, OrchError, Registry, Result, Sink};
use reqwest::Method;
use serde::Deserialize;

use crate::http::{build_url, default_timeout_ms, parse_method, HttpClient, HttpRetryConfig};
use crate::json;

pub fn register(registry: &mut Registry) {
    registry.register_sink("rest", |node, config| {
        let sink: Arc<dyn Sink> = Arc::new(RestSink::new(node, parse_config(node, config)?)?);
        Ok(sink)
    });
}

/// Formato del cuerpo de cada petición.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyFormat {
    /// `[{...}, {...}]` con `Content-Type: application/json`.
    #[default]
    JsonArray,
    /// Un objeto JSON por línea, con `application/x-ndjson`.
    Ndjson,
}

fn post() -> String {
    "POST".to_string()
}

fn default_rows_per_request() -> usize {
    500
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestSinkConfig {
    pub url: String,
    #[serde(default = "post")]
    pub method: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    /// Filas por petición. Los lotes de entrada se trocean para respetarlo.
    #[serde(default = "default_rows_per_request")]
    pub rows_per_request: usize,
    #[serde(default)]
    pub body: BodyFormat,
    /// Envuelve el array en un objeto: `{ "records": [...] }`.
    #[serde(default)]
    pub wrap_in: Option<String>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub rate_limit_per_second: Option<f64>,
    #[serde(default)]
    pub retry: HttpRetryConfig,
}

pub struct RestSink {
    node: String,
    config: RestSinkConfig,
    method: Method,
    client: HttpClient,
}

impl RestSink {
    pub fn new(node: impl Into<String>, config: RestSinkConfig) -> Result<Self> {
        let node = node.into();
        if config.rows_per_request == 0 {
            return Err(OrchError::config(
                &node,
                "`rows_per_request` debe ser mayor que 0",
            ));
        }
        if config.wrap_in.is_some() && config.body == BodyFormat::Ndjson {
            return Err(OrchError::config(
                &node,
                "`wrap_in` no tiene sentido con `body: ndjson`, que no lleva array",
            ));
        }
        let method = parse_method(&node, &config.method)?;
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
            client,
        })
    }

    /// Cuerpo y `Content-Type` de un trozo de lote.
    fn body_of(&self, batch: &RecordBatch) -> Result<(Vec<u8>, &'static str)> {
        match self.config.body {
            BodyFormat::Ndjson => Ok((json::to_ndjson(&self.node, batch)?, "application/x-ndjson")),
            BodyFormat::JsonArray => {
                let array = json::to_json_array(&self.node, batch)?;
                match &self.config.wrap_in {
                    None => Ok((array, "application/json")),
                    Some(field) => {
                        // El array ya está serializado: envolverlo es
                        // concatenar, sin volver a recorrer los datos.
                        let key = serde_json::to_string(field).map_err(|e| {
                            OrchError::node(&self.node, format!("`wrap_in` no es válido: {e}"))
                        })?;
                        let mut body = Vec::with_capacity(array.len() + key.len() + 3);
                        body.push(b'{');
                        body.extend_from_slice(key.as_bytes());
                        body.push(b':');
                        body.extend_from_slice(&array);
                        body.push(b'}');
                        Ok((body, "application/json"))
                    }
                }
            }
        }
    }

    async fn post_chunk(&self, batch: &RecordBatch) -> Result<()> {
        let (body, content_type) = self.body_of(batch)?;
        let params = self
            .config
            .query
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()));
        let url = build_url(&self.node, &self.config.url, params)?;
        let rows = batch.num_rows();

        let describe = format!("{} {url} ({rows} filas)", self.method);
        self.client
            .send(&describe, || {
                self.client
                    .request(&self.method, url.clone())
                    .header(reqwest::header::CONTENT_TYPE, content_type)
                    .body(body.clone())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for RestSink {
    fn connector(&self) -> &str {
        "rest"
    }

    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> Result<()> {
        while let Some(batch) = input.recv().await? {
            // Un lote de Arrow puede ser mucho más grande de lo que acepta
            // una API; se trocea sin copiar datos (`slice` comparte buffers).
            let mut sent = 0;
            while sent < batch.num_rows() {
                let len = self.config.rows_per_request.min(batch.num_rows() - sent);
                self.post_chunk(&batch.slice(sent, len)).await?;
                sent += len;
            }
        }
        Ok(())
    }
}
