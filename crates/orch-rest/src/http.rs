//! Cliente HTTP compartido: reintentos por código de estado y límite de tasa.
//!
//! Nada de esto registra cabeceras: llevan tokens de autenticación y acabarían
//! en los logs.

use std::collections::BTreeMap;
use std::time::Duration;

use orch_core::{OrchError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER};
use reqwest::{Method, Response, Url};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// Códigos que merece la pena reintentar: saturación o fallo transitorio del
/// servidor. Un 4xx distinto de 408/429 es culpa de la petición y reintentarlo
/// sólo gasta tiempo.
fn default_retry_on() -> Vec<u16> {
    vec![408, 429, 500, 502, 503, 504]
}

fn default_max_attempts() -> u32 {
    3
}

fn default_backoff_ms() -> u64 {
    500
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_max_backoff_ms() -> u64 {
    30_000
}

pub fn default_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpRetryConfig {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    /// Códigos de estado que se reintentan.
    #[serde(default = "default_retry_on")]
    pub retry_on: Vec<u16>,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: default_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            max_backoff_ms: default_max_backoff_ms(),
            retry_on: default_retry_on(),
        }
    }
}

impl HttpRetryConfig {
    fn backoff(&self, attempt: u32) -> Duration {
        let factor = self.backoff_multiplier.max(1.0).powi(attempt as i32 - 1);
        let millis = (self.backoff_ms as f64 * factor).min(self.max_backoff_ms as f64);
        Duration::from_millis(millis as u64)
    }
}

pub struct HttpClient {
    node: String,
    client: reqwest::Client,
    retry: HttpRetryConfig,
    /// Separación mínima entre peticiones, si hay límite de tasa.
    min_interval: Option<Duration>,
    last_sent: Mutex<Option<Instant>>,
}

impl HttpClient {
    pub fn new(
        node: &str,
        headers: &BTreeMap<String, String>,
        timeout_ms: u64,
        rate_limit_per_second: Option<f64>,
        retry: HttpRetryConfig,
    ) -> Result<Self> {
        if retry.max_attempts == 0 {
            return Err(OrchError::config(
                node,
                "`retry.max_attempts` debe ser al menos 1",
            ));
        }

        let mut header_map = HeaderMap::new();
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                OrchError::config(node, format!("`{name}` no es un nombre de cabecera válido"))
            })?;
            // El valor puede ser un token: el error no lo incluye.
            let value = HeaderValue::from_str(value).map_err(|_| {
                OrchError::config(
                    node,
                    format!("el valor de la cabecera `{name}` no es válido"),
                )
            })?;
            header_map.insert(name, value);
        }

        let client = reqwest::Client::builder()
            .default_headers(header_map)
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|e| OrchError::config(node, format!("no se pudo crear el cliente: {e}")))?;

        let min_interval = match rate_limit_per_second {
            Some(rate) if rate > 0.0 => Some(Duration::from_secs_f64(1.0 / rate)),
            Some(_) => {
                return Err(OrchError::config(
                    node,
                    "`rate_limit_per_second` debe ser mayor que 0",
                ))
            }
            None => None,
        };

        Ok(Self {
            node: node.to_string(),
            client,
            retry,
            min_interval,
            last_sent: Mutex::new(None),
        })
    }

    pub fn request(&self, method: &Method, url: Url) -> reqwest::RequestBuilder {
        self.client.request(method.clone(), url)
    }

    /// Espera lo necesario para no pasarse del límite de tasa.
    ///
    /// El cerrojo se mantiene durante la espera a propósito: así las
    /// peticiones concurrentes se serializan en vez de salir todas juntas en
    /// cuanto pasa el intervalo.
    async fn throttle(&self) {
        let Some(interval) = self.min_interval else {
            return;
        };
        let mut last = self.last_sent.lock().await;
        if let Some(previous) = *last {
            let elapsed = previous.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
        *last = Some(Instant::now());
    }

    /// Envía con reintentos.
    ///
    /// Recibe un constructor y no una petición ya montada porque cada intento
    /// necesita una petición nueva: el cuerpo puede consumirse al enviarse.
    pub async fn send<F>(&self, describe: &str, build: F) -> Result<Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut attempt: u32 = 1;
        loop {
            self.throttle().await;

            let outcome = build().send().await;
            let last_attempt = attempt >= self.retry.max_attempts;

            match outcome {
                Ok(response) if response.status().is_success() => return Ok(response),

                Ok(response) => {
                    let status = response.status();
                    let retryable = self.retry.retry_on.contains(&status.as_u16());
                    if retryable && !last_attempt {
                        // Un servidor que dice cuándo volver sabe más que
                        // nuestro backoff.
                        let wait =
                            retry_after(&response).unwrap_or_else(|| self.retry.backoff(attempt));
                        tracing::warn!(
                            node = %self.node,
                            %status,
                            attempt,
                            wait_ms = wait.as_millis() as u64,
                            "{describe}: se reintentará"
                        );
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    let body = response.text().await.unwrap_or_default();
                    return Err(OrchError::node(
                        &self.node,
                        format!("{describe}: HTTP {status}. {}", truncate(&body, 500)),
                    ));
                }

                // Un fallo de red o un timeout sí es transitorio por defecto.
                Err(err) => {
                    if !last_attempt {
                        let wait = self.retry.backoff(attempt);
                        tracing::warn!(
                            node = %self.node,
                            attempt,
                            error = %err,
                            "{describe}: fallo de red, se reintentará"
                        );
                        tokio::time::sleep(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(OrchError::node(
                        &self.node,
                        format!("{describe}: la petición falló tras {attempt} intento(s): {err}"),
                    ));
                }
            }
        }
    }
}

/// Lee `Retry-After` en su forma de segundos.
fn retry_after(response: &Response) -> Option<Duration> {
    let value = response.headers().get(RETRY_AFTER)?.to_str().ok()?;
    let seconds: u64 = value.trim().parse().ok()?;
    // Un servidor que pide esperar una hora no debe colgar el pipeline.
    Some(Duration::from_secs(seconds.min(300)))
}

fn truncate(text: &str, max: usize) -> String {
    let trimmed = text.trim();
    if trimmed.len() <= max {
        trimmed.to_string()
    } else {
        let mut end = max;
        while !trimmed.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &trimmed[..end])
    }
}

/// Monta la URL con sus parámetros de query.
///
/// Se arma aquí, y no con `RequestBuilder::query`, para que cada reintento
/// reutilice exactamente la misma URL en vez de reconstruirla.
pub fn build_url<'a, I>(node: &str, base: &str, params: I) -> Result<Url>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut url = Url::parse(base)
        .map_err(|e| OrchError::config(node, format!("`{base}` no es una URL válida: {e}")))?;
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in params {
            pairs.append_pair(key, value);
        }
    }
    Ok(url)
}

/// Convierte el método escrito en el YAML.
pub fn parse_method(node: &str, method: &str) -> Result<Method> {
    Method::from_bytes(method.to_ascii_uppercase().as_bytes())
        .map_err(|_| OrchError::config(node, format!("método HTTP `{method}` no válido")))
}
