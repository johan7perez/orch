//! Las aristas que cruzan de una máquina a otra dentro de un clúster.
//!
//! Son como el sink y el origen `flight`, con dos diferencias: no abren un
//! puerto cada una —los flujos se publican en el servicio Flight que ya tiene
//! el proceso ([`Aristas`])— y el TLS no se escribe en el pipeline: es el del
//! propio proceso, el del clúster. Nadie las escribe a mano; las pone el
//! coordinador al partir el pipeline.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_flight::error::FlightError;
use async_trait::async_trait;
use orch_core::arrow::datatypes::SchemaRef;
use orch_core::{
    Input, InputSchemas, NodeContext, OrchError, Plazo, Registry, Result, Sink, Source,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::ClientTlsConfig;

use crate::servidor::Lote;
use crate::source::{FlightSource, FlightSourceConfig};

/// Un flujo publicado, esperando a que lo pidan.
pub(crate) struct Publicado {
    pub lotes: mpsc::Receiver<Lote>,
    pub schema: Option<SchemaRef>,
    /// Avisa de que alguien empezó a leer, y quién.
    pub tomado: oneshot::Sender<String>,
    /// Avisa de que el consumidor recibió el final (o se fue).
    pub terminado: oneshot::Sender<()>,
}

/// Los flujos que este proceso sirve a otras máquinas del clúster, por su
/// ticket.
#[derive(Default)]
pub struct Aristas {
    flujos: Mutex<HashMap<String, Publicado>>,
}

impl Aristas {
    fn publicar(&self, ticket: &str, flujo: Publicado) -> std::result::Result<(), String> {
        let mut flujos = self.flujos.lock().expect("mutex de aristas");
        if flujos.contains_key(ticket) {
            return Err(format!("ya hay un flujo publicado como `{ticket}`"));
        }
        flujos.insert(ticket.to_string(), flujo);
        Ok(())
    }

    /// Se lo lleva quien lo pide: cada flujo se lee una vez.
    pub(crate) fn tomar(&self, ticket: &str) -> Option<Publicado> {
        self.flujos.lock().expect("mutex de aristas").remove(ticket)
    }

    fn retirar(&self, ticket: &str) {
        self.flujos.lock().expect("mutex de aristas").remove(ticket);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SalidaConfig {
    pub ticket: String,
    #[schemars(with = "String")]
    pub wait: Plazo,
}

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntradaConfig {
    pub url: String,
    pub ticket: String,
    #[schemars(with = "String")]
    pub wait: Plazo,
}

/// Sink `cluster_out`: publica su entrada para el worker que la lee.
struct Salida {
    node: String,
    config: SalidaConfig,
    aristas: Arc<Aristas>,
}

#[async_trait]
impl Sink for Salida {
    fn connector(&self) -> &str {
        "cluster_out"
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<()> {
        inputs.concatenated(&self.node).map(|_| ())
    }

    async fn write(&self, ctx: &NodeContext, input: &mut Input) -> Result<()> {
        let ticket = &self.config.ticket;
        let (lotes, pendiente) = mpsc::channel::<Lote>(ctx.settings.channel_capacity.max(1));
        let (aviso, tomado) = oneshot::channel();
        let (fin, terminado) = oneshot::channel();
        self.aristas
            .publicar(
                ticket,
                Publicado {
                    lotes: pendiente,
                    schema: ctx.inputs.concatenated(&self.node).ok().flatten(),
                    tomado: aviso,
                    terminado: fin,
                },
            )
            .map_err(|e| OrchError::node(&self.node, e))?;

        let plazo = self.config.wait;
        let quien = match tokio::time::timeout(plazo.0, tomado).await {
            Ok(Ok(quien)) => quien,
            _ => {
                self.aristas.retirar(ticket);
                return Err(OrchError::node(
                    &self.node,
                    format!("la máquina que tenía que leer esta arista no vino en {plazo}"),
                ));
            }
        };

        let mut fallo = None;
        loop {
            match input.recv().await {
                Ok(Some(lote)) => {
                    if lotes.send(Ok(lote)).await.is_err() {
                        fallo = Some(OrchError::node(
                            &self.node,
                            format!("la máquina que leía ({quien}) se desconectó a mitad"),
                        ));
                        break;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    let motivo = format!("falló la parte que la alimentaba: {err}");
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
        // Que llegue el final antes de dar la arista por cerrada.
        if fallo.is_none()
            && tokio::time::timeout(Duration::from_secs(60), terminado)
                .await
                .is_err()
        {
            tracing::warn!(node = %self.node, "el final de la arista no se confirmó en un minuto");
        }
        match fallo {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// Registra `cluster_out` y `cluster_in` con la identidad del clúster de
/// este proceso.
pub fn registrar_aristas(registry: &mut Registry, aristas: Arc<Aristas>, tls: ClientTlsConfig) {
    registry.register_sink("cluster_out", move |node, config: SalidaConfig| {
        let s: Arc<dyn Sink> = Arc::new(Salida {
            node: node.to_string(),
            config,
            aristas: Arc::clone(&aristas),
        });
        Ok(s)
    });
    registry.register_source("cluster_in", move |node, config: EntradaConfig| {
        let s: Arc<dyn Source> = Arc::new(FlightSource::con_tls(
            node,
            FlightSourceConfig {
                url: config.url,
                ticket: config.ticket,
                token: None,
                wait_for_server: config.wait,
                tls: None,
            },
            tls.clone(),
        )?);
        Ok(s)
    });
}
