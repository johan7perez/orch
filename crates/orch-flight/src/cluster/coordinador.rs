//! El coordinador: reparte un pipeline entre las máquinas del clúster y lo
//! junta en una sola ejecución.
//!
//! Para quien escucha sus eventos (el visor de `--follow`, el historial, las
//! alertas) es como un ejecutor más: un `RunStarted`, los eventos de todos los
//! nodos de todas las máquinas con el mismo `run_id`, el progreso de todos en
//! un solo evento, y un `RunFinished`.
//!
//! Que una parte **falle** no cancela nada: el fallo viaja por las aristas
//! como en una sola máquina. Que una parte **no pueda correr** (el worker no
//! responde, no tiene un conector, se cae a mitad) sí: las demás se cancelan
//! en vez de quedarse esperando una arista que no va a llegar.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use orch_core::event::EVENT_CHANNEL_CAPACITY;
use orch_core::{
    Dag, Executor, NodeKind, NodeProgress, NodeReport, OrchError, PipelineSpec, Registry, Result,
    RunEvent, RunReport,
};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;
use tonic::transport::{ClientTlsConfig, Endpoint};

use super::aristas::{registrar_aristas, Aristas};
use super::division::{dividir, Donde};
use super::servicio::{servir, Encargo, Mensaje, Servicio, ACCION_EJECUTAR};
use super::ClusterConfig;

/// Cómo se presentan las aristas entre máquinas en eventos e informes.
pub const ARISTA: &str = "edge";

pub struct Coordinador {
    registry: Arc<Registry>,
    aristas: Arc<Aristas>,
    cluster: ClusterConfig,
    cliente: ClientTlsConfig,
    eventos: broadcast::Sender<RunEvent>,
}

impl Coordinador {
    /// `registry` es lo que hay disponible en esta máquina; aquí se le
    /// añaden las aristas del clúster.
    pub fn new(mut registry: Registry, cluster: ClusterConfig) -> Result<Self> {
        let aristas = Arc::new(Aristas::default());
        let cliente = cluster.tls.cliente()?;
        registrar_aristas(&mut registry, Arc::clone(&aristas), cliente.clone());
        let (eventos, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Ok(Self {
            registry: Arc::new(registry),
            aristas,
            cluster,
            cliente,
            eventos,
        })
    }

    /// Los eventos de la ejecución entera, como los de un [`Executor`].
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.eventos.subscribe()
    }

    /// Ejecuta `spec` —tal y como está escrito— repartido por el clúster.
    /// `base` es la carpeta del pipeline: contra ella se resuelven las rutas
    /// de lo que corre aquí.
    pub async fn run(&self, spec: &PipelineSpec, base: &Path) -> Result<RunReport> {
        let run_id = uuid::Uuid::new_v4().to_string();
        let inicio = Instant::now();
        let partes = dividir(
            spec,
            &run_id,
            &self.cluster.workers,
            self.cluster.url.as_deref(),
            self.cluster.wait,
        )?;

        // Lo de aquí se prepara antes de arrancar nada: un error de config
        // sale sin haber movido una fila en ninguna máquina.
        let mut local: Option<Dag> = None;
        let mut sirve_aqui = false;
        for parte in &partes {
            if parte.donde != Donde::Local {
                continue;
            }
            sirve_aqui = parte.spec.nodes.iter().any(|n| {
                matches!(&n.kind, NodeKind::Sink { connector, .. } if connector == "cluster_out")
            });
            let mut aqui = parte.spec.clone();
            aqui.expand_secrets()?;
            aqui.resolve_paths(base);
            orch_core::pushdown::apply(&mut aqui, &self.registry);
            local = Some(Dag::build(aqui)?);
        }

        let servidor = if sirve_aqui {
            let listen = self.cluster.listen.as_deref().ok_or_else(|| {
                OrchError::Validation(
                    "algún nodo que corre aquí alimenta a un worker: el coordinador tiene que \
                     escuchar (`listen` en el fichero del clúster)"
                        .into(),
                )
            })?;
            let escucha = TcpListener::bind(listen).await.map_err(|err| {
                OrchError::Other(format!(
                    "el coordinador no pudo escuchar en `{listen}`: {err}"
                ))
            })?;
            let (parar, parada) = oneshot::channel::<()>();
            let tarea = servir(
                Servicio::solo_aristas(Arc::clone(&self.aristas)),
                &self.cluster.tls,
                escucha,
                async {
                    let _ = parada.await;
                },
            )?;
            Some((tarea, parar))
        } else {
            None
        };

        let nodos: usize = partes.iter().map(|p| p.spec.nodes.len()).sum();
        let _ = self.eventos.send(RunEvent::RunStarted {
            run_id: run_id.clone(),
            pipeline: spec.name.clone(),
            nodes: nodos,
        });

        // Los eventos de todas las partes pasan por aquí: se quitan los
        // arranques y finales de cada parte y se junta el progreso.
        let (tx, mut rx) = mpsc::channel::<RunEvent>(EVENT_CHANNEL_CAPACITY);
        let eventos = self.eventos.clone();
        let id = run_id.clone();
        // Los nodos que no están en el fichero son las aristas entre
        // máquinas: se presentan como `edge`, no como orígenes o sinks, para
        // que nadie cuente como escritas las filas que sólo viajan.
        let originales: std::collections::HashSet<String> =
            spec.nodes.iter().map(|n| n.id.clone()).collect();
        let es_arista = {
            let originales = originales.clone();
            move |nodo: &str| !originales.contains(nodo)
        };
        let juntar = tokio::spawn(async move {
            let mut progreso: BTreeMap<String, NodeProgress> = BTreeMap::new();
            while let Some(evento) = rx.recv().await {
                match evento {
                    RunEvent::RunStarted { .. } | RunEvent::RunFinished { .. } => {}
                    RunEvent::Progress { nodes, .. } => {
                        for n in nodes {
                            progreso.insert(n.node.clone(), n);
                        }
                        let _ = eventos.send(RunEvent::Progress {
                            run_id: id.clone(),
                            elapsed_ms: inicio.elapsed().as_millis() as u64,
                            nodes: progreso.values().cloned().collect(),
                        });
                    }
                    RunEvent::NodeStarted {
                        run_id,
                        node,
                        kind,
                        component,
                        attempt,
                    } => {
                        let kind = if es_arista(&node) {
                            ARISTA.to_string()
                        } else {
                            kind
                        };
                        let _ = eventos.send(RunEvent::NodeStarted {
                            run_id,
                            node,
                            kind,
                            component,
                            attempt,
                        });
                    }
                    otro => {
                        let _ = eventos.send(otro);
                    }
                }
            }
        });

        let mut tareas = JoinSet::new();
        for parte in partes {
            let tx = tx.clone();
            let run_id = run_id.clone();
            match parte.donde {
                Donde::Local => {
                    let dag = local.take().expect("preparada arriba");
                    let registry = Arc::clone(&self.registry);
                    tareas.spawn(async move {
                        (Donde::Local, parte_local(registry, dag, run_id, tx).await)
                    });
                }
                Donde::Worker(nombre) => {
                    let url = self.cluster.workers[&nombre].clone();
                    let tls = self.cliente.clone();
                    let pipeline = serde_yaml::to_string(&parte.spec)
                        .map_err(|e| OrchError::Other(e.to_string()))?;
                    tareas.spawn(async move {
                        let resultado =
                            parte_remota(&url, tls, Encargo { run_id, pipeline }, tx).await;
                        (Donde::Worker(nombre), resultado)
                    });
                }
            }
        }
        drop(tx);

        let mut informes: Vec<RunReport> = Vec::new();
        let mut fallo: Option<String> = None;
        while let Some(terminada) = tareas.join_next().await {
            match terminada {
                Ok((_, Ok(informe))) => informes.push(informe),
                Ok((donde, Err(err))) => {
                    let quien = match donde {
                        Donde::Local => "la parte de esta máquina".to_string(),
                        Donde::Worker(n) => format!("la parte del worker `{n}`"),
                    };
                    fallo.get_or_insert(format!("{quien} no pudo correr: {err}"));
                    // Las demás esperarían una arista que no va a llegar.
                    tareas.abort_all();
                }
                Err(err) if err.is_cancelled() => {}
                Err(err) => {
                    fallo.get_or_insert(format!("una parte se rompió: {err}"));
                    tareas.abort_all();
                }
            }
        }
        let _ = juntar.await;
        if let Some((tarea, parar)) = servidor {
            let _ = parar.send(());
            let _ = tokio::time::timeout(Duration::from_secs(10), tarea).await;
        }

        let elapsed_ms = inicio.elapsed().as_millis() as u64;
        let succeeded = fallo.is_none() && informes.iter().all(|i| i.succeeded);
        let _ = self.eventos.send(RunEvent::RunFinished {
            run_id: run_id.clone(),
            pipeline: spec.name.clone(),
            succeeded,
            elapsed_ms,
        });
        if let Some(fallo) = fallo {
            return Err(OrchError::Other(fallo));
        }

        // Los nodos en el orden del pipeline, y detrás las aristas del
        // clúster, que no estaban en el fichero.
        let mut por_id: BTreeMap<String, NodeReport> = informes
            .into_iter()
            .flat_map(|i| i.nodes)
            .map(|mut n| {
                if !originales.contains(&n.id) {
                    n.kind = ARISTA.to_string();
                }
                (n.id.clone(), n)
            })
            .collect();
        let mut nodes: Vec<NodeReport> = spec
            .nodes
            .iter()
            .filter_map(|n| por_id.remove(&n.id))
            .collect();
        nodes.extend(por_id.into_values());

        Ok(RunReport {
            run_id,
            pipeline: spec.name.clone(),
            succeeded,
            elapsed_ms,
            nodes,
        })
    }
}

/// Reenvía los eventos de un ejecutor hasta que se cierre su canal. Un
/// oyente que se retrasa pierde eventos, pero no deja de escuchar.
pub(crate) async fn reenviar(
    mut eventos: broadcast::Receiver<RunEvent>,
    tx: mpsc::Sender<RunEvent>,
) {
    loop {
        match eventos.recv().await {
            Ok(evento) => {
                if tx.send(evento).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

async fn parte_local(
    registry: Arc<Registry>,
    dag: Dag,
    run_id: String,
    tx: mpsc::Sender<RunEvent>,
) -> Result<RunReport> {
    let ejecutor = Executor::new(registry);
    let reenvio = tokio::spawn(reenviar(ejecutor.subscribe(), tx));
    let resultado = ejecutor.run_with_id(&dag, run_id).await;
    drop(ejecutor);
    let _ = reenvio.await;
    resultado
}

async fn parte_remota(
    url: &str,
    tls: ClientTlsConfig,
    encargo: Encargo,
    tx: mpsc::Sender<RunEvent>,
) -> Result<RunReport> {
    let fallo = |que: &str, err: &dyn std::fmt::Display| OrchError::Other(format!("{que}: {err}"));
    let canal = Endpoint::from_shared(url.to_string())
        .map_err(|e| fallo("la url del worker no vale", &e))?
        .connect_timeout(Duration::from_secs(10))
        .tls_config(tls)
        .map_err(|e| fallo("el TLS del clúster no vale", &e))?
        .connect()
        .await
        .map_err(|e| {
            fallo(
                &format!("no se pudo conectar con `{url}`"),
                &crate::source::cadena(&e),
            )
        })?;
    let mut cliente = FlightServiceClient::new(canal).max_decoding_message_size(usize::MAX);
    let body = serde_json::to_vec(&encargo).map_err(|e| fallo("encargo", &e))?;
    let mut flujo = cliente
        .do_action(Action {
            r#type: ACCION_EJECUTAR.into(),
            body: body.into(),
        })
        .await
        .map_err(|s| fallo(&format!("`{url}` no aceptó la parte"), &s.message()))?
        .into_inner();

    loop {
        let mensaje = flujo
            .message()
            .await
            .map_err(|s| fallo(&format!("se perdió la conexión con `{url}`"), &s.message()))?;
        let Some(mensaje) = mensaje else {
            return Err(OrchError::Other(format!(
                "`{url}` cerró sin terminar su parte"
            )));
        };
        match serde_json::from_slice::<Mensaje>(&mensaje.body)
            .map_err(|e| fallo("mensaje ilegible del worker", &e))?
        {
            Mensaje::Evento(evento) => {
                let _ = tx.send(evento).await;
            }
            Mensaje::Informe(informe) => return Ok(informe),
            Mensaje::Error(error) => return Err(OrchError::Other(error)),
        }
    }
}
