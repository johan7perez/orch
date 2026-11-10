//! Sink `flight`: sirve el flujo que le llega para que otro lo lea.
//!
//! Escucha en `listen` mientras dura el nodo y entrega su entrada al primer
//! consumidor que pida `ticket`: otro pipeline de Orch con un origen
//! `flight`, o cualquier cliente de Arrow Flight (pyarrow, por ejemplo).
//!
//! No guarda nada: los lotes pasan del pipeline al consumidor con la misma
//! contrapresión que entre dos nodos de la misma máquina. Hasta que alguien
//! viene a leer, el pipeline espera; si nadie viene en `wait_for_client`,
//! falla. Y si el pipeline falla a mitad, el consumidor recibe el fallo, no
//! un final de flujo que parecería bueno.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use orch_core::{Input, InputSchemas, NodeContext, OrchError, Plazo, Registry, Result, Sink};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::ServerTlsConfig;

use crate::servidor::{Flujo, Lote};
use crate::tls::ServerTls;

fn diez_minutos() -> Plazo {
    Plazo(Duration::from_secs(600))
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FlightSinkConfig {
    /// Dónde escuchar, `host:puerto`. `127.0.0.1:50051` sólo acepta a esta
    /// máquina; `0.0.0.0:50051`, a cualquiera que llegue al puerto, y por eso
    /// exige autenticar a quien lee: un `token` o `tls.client_ca`.
    pub listen: String,
    /// Nombre del flujo: lo que pide quien lo lee (el «ticket» de Flight).
    pub ticket: String,
    /// Si está, quien lee tiene que mandar `Authorization: Bearer <token>`.
    /// Lo normal es `${env:...}`, para que no quede escrito en el fichero.
    #[serde(default)]
    pub token: Option<String>,
    /// Cuánto esperar a que alguien venga a leer antes de fallar: `30s`,
    /// `10m`, `1h`.
    #[serde(default = "diez_minutos")]
    #[schemars(with = "String")]
    pub wait_for_client: Plazo,
    /// Servir cifrado. Con `client_ca`, además, sólo a quien presente un
    /// certificado de esa CA.
    #[serde(default)]
    pub tls: Option<ServerTls>,
}

pub struct FlightSink {
    node: String,
    config: FlightSinkConfig,
    direccion: SocketAddr,
    /// Leído al crear el nodo: un certificado que no está falla en
    /// `validate`, no al ejecutar.
    tls: Option<ServerTlsConfig>,
}

impl FlightSink {
    pub fn new(node: impl Into<String>, config: FlightSinkConfig) -> Result<Self> {
        let node = node.into();
        let direccion: SocketAddr = config.listen.parse().map_err(|_| {
            OrchError::config(
                &node,
                format!(
                    "`listen` es `host:puerto` con una IP (`127.0.0.1:50051`), no `{}`",
                    config.listen
                ),
            )
        })?;
        if config.ticket.trim().is_empty() {
            return Err(OrchError::config(&node, "`ticket` no puede estar vacío"));
        }
        // Escuchar fuera de esta máquina sin autenticar a quien lee sería
        // servir los datos a cualquiera que llegue al puerto. No se permite
        // por descuido.
        let con_token = config.token.as_deref().is_some_and(|t| !t.is_empty());
        let con_certificado = config.tls.as_ref().is_some_and(ServerTls::mutual);
        if !direccion.ip().is_loopback() {
            if !con_token && !con_certificado {
                return Err(OrchError::config(
                    &node,
                    format!(
                        "`{}` acepta conexiones de otras máquinas: autentica a quien lee con \
                         un `token` (`${{env:...}}`) o con `tls.client_ca`, o escucha en \
                         `127.0.0.1`",
                        config.listen
                    ),
                ));
            }
            if config.tls.is_none() {
                tracing::warn!(
                    node = %node,
                    listen = %config.listen,
                    "se sirve sin TLS a otras máquinas: el token y los datos viajan sin cifrar"
                );
            }
        }
        let tls = config.tls.as_ref().map(|t| t.config(&node)).transpose()?;
        Ok(Self {
            node,
            config,
            direccion,
            tls,
        })
    }
}

#[async_trait]
impl Sink for FlightSink {
    fn connector(&self) -> &str {
        "flight"
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<()> {
        // Flight lleva un esquema por flujo: varias entradas tienen que
        // apilarse en una sola forma.
        inputs.concatenated(&self.node).map(|_| ())
    }

    async fn write(&self, ctx: &NodeContext, input: &mut Input) -> Result<()> {
        let escucha = TcpListener::bind(self.direccion).await.map_err(|err| {
            OrchError::node(
                &self.node,
                format!("no se pudo escuchar en `{}`: {err}", self.config.listen),
            )
        })?;
        tracing::info!(
            node = %self.node,
            listen = %self.config.listen,
            ticket = %self.config.ticket,
            "sirviendo el flujo por Flight"
        );

        let (lotes, pendiente) = mpsc::channel::<Lote>(ctx.settings.channel_capacity.max(1));
        let (aviso, tomado) = oneshot::channel();
        let flujo = Flujo::nuevo(
            self.config.ticket.clone(),
            self.config.token.clone().filter(|t| !t.is_empty()),
            ctx.inputs.concatenated(&self.node).ok().flatten(),
            pendiente,
            aviso,
        );
        let (parar, parada) = oneshot::channel::<()>();
        let mut constructor = tonic::transport::Server::builder();
        if let Some(tls) = &self.tls {
            constructor = constructor.tls_config(tls.clone()).map_err(|err| {
                OrchError::node(
                    &self.node,
                    format!("el certificado no vale para TLS: {err}"),
                )
            })?;
        }
        let servidor = tokio::spawn(
            constructor
                .add_service(FlightServiceServer::new(flujo))
                .serve_with_incoming_shutdown(TcpListenerStream::new(escucha), async {
                    let _ = parada.await;
                }),
        );
        let apagar = |servidor: tokio::task::JoinHandle<_>, parar: oneshot::Sender<()>| async move {
            let _ = parar.send(());
            // El apagado ordenado deja terminar lo que está en curso: el
            // consumidor recibe lo que ya se le mandó.
            if tokio::time::timeout(Duration::from_secs(60), servidor)
                .await
                .is_err()
            {
                tracing::warn!("el consumidor no terminó de leer en un minuto; se corta");
            }
        };

        // 1. Esperar a que alguien venga a leer.
        let plazo = self.config.wait_for_client;
        let quien = match tokio::time::timeout(plazo.0, tomado).await {
            Ok(Ok(quien)) => quien,
            Ok(Err(_)) | Err(_) => {
                apagar(servidor, parar).await;
                return Err(OrchError::node(
                    &self.node,
                    format!(
                        "nadie vino a leer `{}` en {plazo} (escuchando en `{}`)",
                        self.config.ticket, self.config.listen
                    ),
                ));
            }
        };
        tracing::info!(node = %self.node, consumidor = %quien, "el flujo tiene consumidor");

        // 2. Pasarle los lotes, con contrapresión: si el consumidor va lento,
        //    el canal se llena y el pipeline espera.
        let mut fallo = None;
        loop {
            match input.recv().await {
                Ok(Some(lote)) => {
                    if lotes.send(Ok(lote)).await.is_err() {
                        fallo = Some(OrchError::node(
                            &self.node,
                            format!("el consumidor ({quien}) se desconectó a mitad del flujo"),
                        ));
                        break;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    // Que el otro lado se entere de que esto no terminó bien.
                    let motivo = format!("el pipeline que sirve el flujo falló: {err}");
                    let _ = lotes
                        .send(Err(FlightError::ExternalError(Box::new(
                            std::io::Error::other(motivo),
                        ))))
                        .await;
                    fallo = Some(err);
                    break;
                }
            }
        }
        drop(lotes);

        // 3. Cerrar cuando el consumidor tenga todo.
        apagar(servidor, parar).await;
        match fallo {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

pub(crate) fn register(registry: &mut Registry) {
    registry.register_sink("flight", |node, config: FlightSinkConfig| {
        let s: Arc<dyn Sink> = Arc::new(FlightSink::new(node, config)?);
        Ok(s)
    });
}
