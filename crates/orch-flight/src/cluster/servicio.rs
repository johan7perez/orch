//! El servicio Flight de cada máquina del clúster.
//!
//! - **DoGet**: sirve las aristas que salen de este proceso ([`Aristas`]).
//! - **DoAction `orch.run`** (sólo en un worker): ejecuta una parte de un
//!   pipeline y devuelve, según ocurren, sus eventos y al final su informe.
//!   Si quien la pidió se va —el coordinador cae, o cancela porque otra
//!   parte no arrancó—, la ejecución se cancela: no se queda corriendo sola.
//!
//! Quién puede entrar lo decide el TLS mutuo, antes de llegar aquí.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{Future, StreamExt, TryStreamExt};
use orch_core::{Dag, Executor, OrchError, PipelineSpec, Registry, RunEvent, RunReport};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::aristas::{registrar_aristas, Aristas};
use super::coordinador::reenviar;
use super::ClusterTls;

/// Lo que el coordinador le pide a un worker.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Encargo {
    pub run_id: String,
    /// La parte del pipeline, en YAML y tal y como está escrita: los
    /// secretos y las rutas se resuelven en el worker.
    pub pipeline: String,
}

/// Lo que un worker le cuenta al coordinador mientras ejecuta su parte.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Mensaje {
    Evento(RunEvent),
    Informe(RunReport),
    /// La parte ni siquiera pudo arrancar.
    Error(String),
}

pub(crate) const ACCION_EJECUTAR: &str = "orch.run";

/// Ejecuta partes de pipelines en este worker.
struct Ejecutor {
    registry: Arc<Registry>,
    /// Contra qué carpeta se resuelven las rutas relativas.
    dir: PathBuf,
}

impl Ejecutor {
    fn preparar(&self, encargo: &Encargo) -> orch_core::Result<Dag> {
        let mut spec = PipelineSpec::from_yaml_str("(parte del coordinador)", &encargo.pipeline)?;
        spec.resolve_paths(&self.dir);
        orch_core::pushdown::apply(&mut spec, &self.registry);
        Dag::build(spec)
    }

    /// Los mensajes de una parte según ocurren. Soltar el flujo cancela la
    /// ejecución.
    fn ejecutar(
        &self,
        encargo: Encargo,
    ) -> BoxStream<'static, Result<arrow_flight::Result, Status>> {
        let (tx, rx) = mpsc::channel::<Mensaje>(256);
        let dag = self.preparar(&encargo);
        let registry = Arc::clone(&self.registry);

        let tarea = tokio::spawn(async move {
            let dag = match dag {
                Ok(dag) => dag,
                Err(err) => {
                    let _ = tx.send(Mensaje::Error(err.to_string())).await;
                    return;
                }
            };
            let ejecutor = Executor::new(registry);
            let (eventos_tx, mut eventos_rx) = mpsc::channel(256);
            let reenvio = {
                let tx = tx.clone();
                let escucha = tokio::spawn(reenviar(ejecutor.subscribe(), eventos_tx));
                tokio::spawn(async move {
                    while let Some(evento) = eventos_rx.recv().await {
                        if tx.send(Mensaje::Evento(evento)).await.is_err() {
                            break;
                        }
                    }
                    let _ = escucha.await;
                })
            };
            let resultado = ejecutor.run_with_id(&dag, encargo.run_id).await;
            drop(ejecutor);
            let _ = reenvio.await;
            let _ = tx
                .send(match resultado {
                    Ok(informe) => Mensaje::Informe(informe),
                    Err(err) => Mensaje::Error(err.to_string()),
                })
                .await;
        });

        // Si el coordinador suelta el flujo, la tarea se aborta, y con ella
        // la ejecución y sus nodos.
        let guarda = AbortarAlSoltar(tarea);
        ReceiverStream::new(rx)
            .map(move |mensaje| {
                let _ = &guarda;
                serde_json::to_vec(&mensaje)
                    .map(|body| arrow_flight::Result { body: body.into() })
                    .map_err(|e| Status::internal(e.to_string()))
            })
            .boxed()
    }
}

struct AbortarAlSoltar(tokio::task::JoinHandle<()>);

impl Drop for AbortarAlSoltar {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct Servicio {
    pub aristas: Arc<Aristas>,
    ejecutor: Option<Ejecutor>,
}

impl Servicio {
    /// Sólo sirve aristas: el del coordinador.
    pub(crate) fn solo_aristas(aristas: Arc<Aristas>) -> Self {
        Self {
            aristas,
            ejecutor: None,
        }
    }
}

fn no_aqui<T>(que: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!(
        "{que}: un nodo de un clúster de Orch sólo sirve aristas y ejecuta partes"
    )))
}

#[tonic::async_trait]
impl FlightService for Servicio {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn do_get(
        &self,
        peticion: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = String::from_utf8_lossy(&peticion.get_ref().ticket).into_owned();
        // Quien lee puede llegar antes de que la parte que sirve haya
        // publicado la arista: «todavía no», y vuelve a intentarlo.
        let flujo = self.aristas.tomar(&ticket).ok_or_else(|| {
            Status::unavailable(format!("la arista `{ticket}` todavía no está publicada"))
        })?;
        let quien = peticion
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "?".into());
        let _ = flujo.tomado.send(quien);

        let mut codificador = FlightDataEncoderBuilder::new();
        if let Some(schema) = flujo.schema {
            codificador = codificador.with_schema(schema);
        }
        // Cuando el flujo se agota (o se suelta), la arista lo sabe.
        let mut fin = Some(flujo.terminado);
        let datos = codificador
            .build(ReceiverStream::new(flujo.lotes))
            .map_err(Status::from)
            .chain(futures::stream::poll_fn(move |_| {
                if let Some(fin) = fin.take() {
                    let _ = fin.send(());
                }
                std::task::Poll::Ready(None)
            }));
        Ok(Response::new(datos.boxed()))
    }

    async fn do_action(
        &self,
        peticion: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let accion = peticion.into_inner();
        if accion.r#type != ACCION_EJECUTAR {
            return no_aqui(&format!("la acción `{}`", accion.r#type));
        }
        let ejecutor = self
            .ejecutor
            .as_ref()
            .ok_or_else(|| Status::unimplemented("esto es un coordinador, no un worker"))?;
        let encargo: Encargo = serde_json::from_slice(&accion.body)
            .map_err(|e| Status::invalid_argument(format!("encargo ilegible: {e}")))?;
        tracing::info!(run_id = %encargo.run_id, "ejecutando una parte");
        Ok(Response::new(ejecutor.ejecutar(encargo)))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        no_aqui("Handshake")
    }

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        no_aqui("ListFlights")
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        no_aqui("GetFlightInfo")
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        no_aqui("PollFlightInfo")
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        no_aqui("GetSchema")
    }

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        no_aqui("DoPut")
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        no_aqui("DoExchange")
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        no_aqui("ListActions")
    }
}

/// Pone en marcha el servicio de una máquina del clúster sobre `escucha`.
pub(crate) fn servir(
    servicio: Servicio,
    tls: &ClusterTls,
    escucha: TcpListener,
    parada: impl Future<Output = ()> + Send + 'static,
) -> orch_core::Result<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>> {
    let servidor = tonic::transport::Server::builder()
        .tls_config(tls.servidor()?)
        .map_err(|e| OrchError::Other(format!("el certificado del clúster no vale: {e}")))?
        .add_service(FlightServiceServer::new(servicio))
        .serve_with_incoming_shutdown(TcpListenerStream::new(escucha), parada);
    Ok(tokio::spawn(servidor))
}

/// `orch worker`: ejecuta las partes que le mande un coordinador y sirve sus
/// aristas, hasta que `parada` termine.
///
/// `registry` es lo que hay disponible en esta máquina (conectores, plugins);
/// aquí se le añaden las aristas del clúster. Las rutas relativas de lo que
/// llegue cuelgan de `dir`.
pub async fn servir_worker(
    escucha: TcpListener,
    tls: &ClusterTls,
    mut registry: Registry,
    dir: PathBuf,
    parada: impl Future<Output = ()> + Send + 'static,
) -> orch_core::Result<()> {
    let aristas = Arc::new(Aristas::default());
    registrar_aristas(&mut registry, Arc::clone(&aristas), tls.cliente()?);
    let servicio = Servicio {
        aristas,
        ejecutor: Some(Ejecutor {
            registry: Arc::new(registry),
            dir,
        }),
    };
    let direccion: Option<SocketAddr> = escucha.local_addr().ok();
    tracing::info!(?direccion, "worker escuchando");
    servir(servicio, tls, escucha, parada)?
        .await
        .map_err(|e| OrchError::Other(e.to_string()))?
        .map_err(|e| OrchError::Other(format!("el worker se paró: {e}")))
}
