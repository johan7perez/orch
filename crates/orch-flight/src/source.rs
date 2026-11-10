//! Origen `flight`: lee un flujo de un servidor Arrow Flight.
//!
//! Lo normal es que el servidor sea otro pipeline de Orch con un sink
//! `flight`, en otra máquina; pero vale cualquier servidor que sirva un flujo
//! con DoGet.
//!
//! Como los dos pipelines arrancan cada uno por su cuenta, el servidor puede
//! no estar escuchando todavía: se insiste durante `wait_for_server`. Lo que
//! no se arregla esperando (un token que no vale, un flujo que no existe)
//! falla al momento.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightClient, Ticket};
use async_trait::async_trait;
use futures::StreamExt;
use orch_core::{NodeContext, OrchError, Output, Plazo, Registry, Result, Source};
use serde::Deserialize;
use tokio::time::Instant;
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::Code;

use crate::tls::ClientTls;

fn un_minuto() -> Plazo {
    Plazo(Duration::from_secs(60))
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FlightSourceConfig {
    /// El servidor: `http://host:puerto`, o `https://` para ir cifrado.
    pub url: String,
    /// Qué flujo pedir (el «ticket» de Flight).
    pub ticket: String,
    /// Se manda como `Authorization: Bearer <token>`. Lo normal es
    /// `${env:...}`.
    #[serde(default)]
    pub token: Option<String>,
    /// Cuánto insistir si el servidor todavía no escucha: el pipeline que lo
    /// sirve puede arrancar después que éste.
    #[serde(default = "un_minuto")]
    #[schemars(with = "String")]
    pub wait_for_server: Plazo,
    /// Con `https://`: con qué CA verificar al servidor (sin ella, las del
    /// sistema) y el certificado propio, si el servidor pide TLS mutuo.
    #[serde(default)]
    pub tls: Option<ClientTls>,
}

pub struct FlightSource {
    node: String,
    config: FlightSourceConfig,
    endpoint: Endpoint,
}

/// Por qué no se pudo empezar a leer.
enum Fallo {
    /// El servidor aún no está: vale la pena volver a intentarlo.
    Todavia(String),
    Definitivo(OrchError),
}

impl FlightSource {
    pub fn new(node: impl Into<String>, config: FlightSourceConfig) -> Result<Self> {
        let node = node.into();
        let cifrado = config.url.starts_with("https://");
        let tls = match (&config.tls, cifrado) {
            (Some(_), false) => {
                return Err(OrchError::config(
                    &node,
                    "hay bloque `tls` pero la `url` es `http://`: para ir cifrado, `https://`",
                ))
            }
            (tls, true) => Some(tls.clone().unwrap_or_default().config(&node)?),
            (None, false) => None,
        };
        Self::construir(node, config, tls)
    }

    /// Con el TLS ya decidido: el del propio proceso, para las aristas que
    /// crea el coordinador de un clúster.
    pub(crate) fn con_tls(
        node: impl Into<String>,
        config: FlightSourceConfig,
        tls: ClientTlsConfig,
    ) -> Result<Self> {
        Self::construir(node.into(), config, Some(tls))
    }

    fn construir(
        node: String,
        config: FlightSourceConfig,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Self> {
        let cifrado = config.url.starts_with("https://");
        let mut endpoint = Endpoint::from_shared(config.url.clone())
            .ok()
            .filter(|_| cifrado || config.url.starts_with("http://"))
            .ok_or_else(|| {
                OrchError::config(
                    &node,
                    format!(
                        "`url` es `http://host:puerto` o `https://host:puerto`, no `{}`",
                        config.url
                    ),
                )
            })?
            .connect_timeout(Duration::from_secs(5));
        if let Some(tls) = tls {
            if !cifrado {
                return Err(OrchError::config(&node, "con TLS, la `url` es `https://`"));
            }
            endpoint = endpoint.tls_config(tls).map_err(|err| {
                OrchError::config(&node, format!("la configuración TLS no vale: {err}"))
            })?;
        }
        if config.ticket.trim().is_empty() {
            return Err(OrchError::config(&node, "`ticket` no puede estar vacío"));
        }
        Ok(Self {
            node,
            config,
            endpoint,
        })
    }

    fn error(&self, contexto: &str, err: impl std::fmt::Display) -> OrchError {
        OrchError::node(&self.node, format!("{contexto}: {err}"))
    }

    async fn empezar(
        &self,
    ) -> std::result::Result<arrow_flight::decode::FlightRecordBatchStream, Fallo> {
        let canal = self.endpoint.connect().await.map_err(|err| {
            let motivo = cadena(&err);
            // Que el servidor no esté todavía se arregla esperando; que su
            // certificado no valga, no.
            let minusculas = motivo.to_lowercase();
            if ["certificate", "tls", "handshake", "unknownissuer"]
                .iter()
                .any(|p| minusculas.contains(p))
            {
                Fallo::Definitivo(self.error(
                    &format!("no se pudo establecer TLS con `{}`", self.config.url),
                    motivo,
                ))
            } else {
                Fallo::Todavia(motivo)
            }
        })?;
        // Un servidor ajeno puede mandar mensajes más grandes que el tope
        // por defecto de tonic (4 MB).
        let interno = FlightServiceClient::new(canal).max_decoding_message_size(usize::MAX);
        let mut cliente = FlightClient::new_from_inner(interno);
        if let Some(token) = self.config.token.as_deref().filter(|t| !t.is_empty()) {
            cliente
                .add_header("authorization", &format!("Bearer {token}"))
                .map_err(|_| {
                    Fallo::Definitivo(OrchError::config(
                        &self.node,
                        "el `token` lleva caracteres que no pueden ir en una cabecera",
                    ))
                })?;
        }
        cliente
            .do_get(Ticket::new(self.config.ticket.clone()))
            .await
            .map_err(|err| match &err {
                FlightError::Tonic(status) if status.code() == Code::Unavailable => {
                    Fallo::Todavia(status.message().to_string())
                }
                _ => Fallo::Definitivo(self.error(
                    &format!(
                        "`{}` no sirvió el flujo `{}`",
                        self.config.url, self.config.ticket
                    ),
                    describir(&err),
                )),
            })
    }
}

/// Un error con todas sus causas: el de transporte de tonic sólo dice
/// «transport error» y lo útil va debajo.
pub(crate) fn cadena(err: &dyn std::error::Error) -> String {
    let mut partes = vec![err.to_string()];
    let mut causa = err.source();
    while let Some(c) = causa {
        let texto = c.to_string();
        if !partes.contains(&texto) {
            partes.push(texto);
        }
        causa = c.source();
    }
    partes.join(": ")
}

/// El mensaje de un error de Flight, sin el envoltorio de gRPC.
fn describir(err: &FlightError) -> String {
    match err {
        FlightError::Tonic(status) => format!("{} ({:?})", status.message(), status.code()),
        otro => otro.to_string(),
    }
}

#[async_trait]
impl Source for FlightSource {
    fn connector(&self) -> &str {
        "flight"
    }

    async fn read(&self, _ctx: &NodeContext, output: &Output) -> Result<()> {
        let plazo = self.config.wait_for_server;
        let limite = Instant::now() + plazo.0;
        let mut espera = Duration::from_millis(100);
        let mut flujo = loop {
            match self.empezar().await {
                Ok(flujo) => break flujo,
                Err(Fallo::Definitivo(err)) => return Err(err),
                Err(Fallo::Todavia(motivo)) => {
                    if Instant::now() + espera > limite {
                        return Err(self.error(
                            &format!(
                                "`{}` no respondió en {plazo} (¿está corriendo el pipeline que \
                                 sirve `{}`?)",
                                self.config.url, self.config.ticket
                            ),
                            motivo,
                        ));
                    }
                    tracing::debug!(node = %self.node, %motivo, "el servidor aún no escucha");
                    tokio::time::sleep(espera).await;
                    espera = (espera * 2).min(Duration::from_secs(1));
                }
            }
        };

        while let Some(lote) = flujo.next().await {
            let lote = lote.map_err(|err| self.error("el flujo remoto falló", describir(&err)))?;
            output.send(lote).await?;
            // Si lo de detrás ya no quiere más (un `limit`), soltar el
            // flujo corta la conexión y el servidor deja de mandar.
            if output.is_closed() {
                break;
            }
        }
        Ok(())
    }
}

pub(crate) fn register(registry: &mut Registry) {
    registry.register_source("flight", |node, config: FlightSourceConfig| {
        let s: Arc<dyn orch_core::Source> = Arc::new(FlightSource::new(node, config)?);
        Ok(s)
    });
}
