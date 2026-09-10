//! Ejecución del DAG.
//!
//! Modelo: **dataflow por streaming**. Todos los nodos arrancan a la vez, cada
//! arista es un canal acotado de `RecordBatch` y el orden lo impone el propio
//! flujo de datos, no un planificador por capas. Un sink empieza a escribir
//! mientras su source todavía está leyendo, y la memoria queda acotada por
//! `channel_capacity * batch_size`, no por el tamaño del dataset.

use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;
use tracing::Instrument;

use crate::connector::{NodeContext, Sink, Source, Transform};
use crate::dag::Dag;
use crate::error::{OrchError, Result};
use crate::event::{RunEvent, EVENT_CHANNEL_CAPACITY};
use crate::io::{batch_channel, Input, IoStats, NodeSignal, Output};
use crate::registry::Registry;
use crate::schema::{InputSchemas, PortSchema};
use crate::spec::{NodeKind, RetryPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Succeeded,
    /// El nodo falló por sí mismo.
    Failed,
    /// El nodo no llegó a hacer trabajo útil porque un nodo del que depende no
    /// completó.
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeReport {
    pub id: String,
    pub kind: String,
    pub component: String,
    pub status: NodeStatus,
    pub attempts: u32,
    pub input: IoStats,
    pub output: IoStats,
    pub elapsed_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub run_id: String,
    pub pipeline: String,
    pub succeeded: bool,
    pub elapsed_ms: u64,
    pub nodes: Vec<NodeReport>,
}

impl RunReport {
    /// Filas escritas por todos los sinks del pipeline.
    pub fn rows_written(&self) -> u64 {
        self.nodes
            .iter()
            .filter(|n| n.kind == "sink")
            .map(|n| n.input.rows)
            .sum()
    }

    pub fn failed_nodes(&self) -> impl Iterator<Item = &NodeReport> {
        self.nodes.iter().filter(|n| n.status == NodeStatus::Failed)
    }
}

enum Runner {
    Source(Arc<dyn Source>),
    Transform(Arc<dyn Transform>),
    Sink(Arc<dyn Sink>),
}

pub struct Executor {
    registry: Arc<Registry>,
    events: broadcast::Sender<RunEvent>,
}

impl Executor {
    pub fn new(registry: Arc<Registry>) -> Self {
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self { registry, events }
    }

    /// Suscripción al flujo de eventos de ejecución.
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.events.subscribe()
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Construye todos los conectores del DAG y propaga los esquemas, sin
    /// ejecutar nada.
    ///
    /// Es lo que usa `orch validate`: detecta conectores desconocidos, configs
    /// mal formadas, columnas inexistentes y fan-in con esquemas
    /// incompatibles, todo antes de abrir un solo fichero.
    pub async fn prepare(&self, dag: &Dag) -> Result<Vec<InputSchemas>> {
        let runners = self.build_runners(dag)?;
        plan_schemas(dag, &runners).await
    }

    fn build_runners(&self, dag: &Dag) -> Result<Vec<Runner>> {
        dag.nodes()
            .iter()
            .map(|node| match &node.kind {
                NodeKind::Source { connector, config } => self
                    .registry
                    .build_source(connector, &node.id, config)
                    .map(Runner::Source),
                NodeKind::Transform { op, config } => self
                    .registry
                    .build_transform(op, &node.id, config)
                    .map(Runner::Transform),
                NodeKind::Sink { connector, config } => self
                    .registry
                    .build_sink(connector, &node.id, config)
                    .map(Runner::Sink),
            })
            .collect()
    }

    /// Ejecuta el pipeline hasta que todos los nodos terminan.
    ///
    /// Sólo devuelve `Err` si el pipeline no pudo ni arrancar. Un fallo durante
    /// la ejecución llega como `Ok(report)` con `succeeded == false`, para que
    /// quien llama tenga las métricas de todos los nodos y no sólo el primer
    /// error.
    pub async fn run(&self, dag: &Dag) -> Result<RunReport> {
        let runners = self.build_runners(dag)?;
        let input_schemas: Vec<Arc<InputSchemas>> = plan_schemas(dag, &runners)
            .await?
            .into_iter()
            .map(Arc::new)
            .collect();
        let run_id = uuid::Uuid::new_v4().to_string();
        let pipeline = dag.spec().name.clone();
        let settings = dag.spec().settings.clone();
        let n = dag.len();
        let started = Instant::now();

        let _ = self.events.send(RunEvent::RunStarted {
            run_id: run_id.clone(),
            pipeline: pipeline.clone(),
            nodes: n,
        });

        // Señal de estado por nodo, que sus vecinos observan. El emisor se
        // mueve a la tarea del nodo (`watch::Sender` no es clonable); los
        // receptores sí se clonan para cada vecino interesado.
        let mut signal_txs: Vec<Option<watch::Sender<NodeSignal>>> = Vec::with_capacity(n);
        let mut signal_rxs: Vec<watch::Receiver<NodeSignal>> = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = watch::channel(NodeSignal::Pending);
            signal_txs.push(Some(tx));
            signal_rxs.push(rx);
        }

        // Un canal por arista de datos. Se recorren las entradas de cada nodo
        // para que el orden de los receptores coincida con el orden en que se
        // declararon las aristas en el YAML.
        type OutEdge = (crate::io::BatchSender, String, watch::Receiver<NodeSignal>);
        let mut out_edges: Vec<Vec<OutEdge>> = (0..n).map(|_| Vec::new()).collect();
        let mut inputs: Vec<Option<Input>> = (0..n).map(|_| None).collect();

        for to in 0..n {
            let ups = dag.upstream(to);
            let mut receivers = Vec::with_capacity(ups.len());
            let mut upstream_ids = Vec::with_capacity(ups.len());
            let mut upstream_signals = Vec::with_capacity(ups.len());
            for &from in ups {
                let (tx, rx) = batch_channel(settings.channel_capacity);
                out_edges[from].push((tx, dag.node(to).id.clone(), signal_rxs[to].clone()));
                receivers.push(rx);
                upstream_ids.push(dag.node(from).id.clone());
                upstream_signals.push(signal_rxs[from].clone());
            }
            inputs[to] = Some(Input::new(
                dag.node(to).id.clone(),
                receivers,
                dag.upstream_ports(to).to_vec(),
                upstream_ids,
                upstream_signals,
            ));
        }

        let mut tasks: JoinSet<(usize, NodeReport)> = JoinSet::new();
        let mut out_edges = out_edges.into_iter();
        let mut inputs = inputs.into_iter();

        for (i, runner) in runners.into_iter().enumerate() {
            let node = dag.node(i).clone();
            let (senders, downstream_ids, downstream_signals) =
                unzip3(out_edges.next().expect("un slot por nodo"));
            let output = Output::new(node.id.clone(), senders, downstream_ids, downstream_signals);
            let input = inputs
                .next()
                .expect("un slot por nodo")
                .expect("input inicializado");
            let barriers: Vec<(String, watch::Receiver<NodeSignal>)> = dag
                .barriers(i)
                .iter()
                .map(|&d| (dag.node(d).id.clone(), signal_rxs[d].clone()))
                .collect();
            let signal_tx = signal_txs[i].take().expect("un emisor de señal por nodo");

            let ctx_base = NodeContext {
                run_id: run_id.clone(),
                pipeline: pipeline.clone(),
                node: node.id.clone(),
                attempt: 1,
                settings: settings.clone(),
                inputs: Arc::clone(&input_schemas[i]),
            };
            let events = self.events.clone();
            let span = tracing::info_span!(
                "node",
                run_id = %run_id,
                node = %node.id,
                kind = node.kind.label(),
                component = node.kind.component(),
            );

            tasks.spawn(
                async move {
                    let report = execute_node(
                        runner,
                        ctx_base,
                        node.kind.label().to_string(),
                        node.kind.component().to_string(),
                        node.retry.clone(),
                        input,
                        output,
                        barriers,
                        signal_tx,
                        events,
                    )
                    .await;
                    (i, report)
                }
                .instrument(span),
            );
        }

        let mut reports: Vec<Option<NodeReport>> = (0..n).map(|_| None).collect();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((i, report)) => reports[i] = Some(report),
                // Un panic en un nodo no debe llevarse la ejecución entera por
                // delante: se reporta como fallo de ese nodo.
                Err(join_err) => {
                    tracing::error!(error = %join_err, "una tarea de nodo terminó de forma anómala");
                }
            }
        }

        let nodes: Vec<NodeReport> = dag
            .topological_order()
            .iter()
            .map(|&i| {
                reports[i].clone().unwrap_or_else(|| NodeReport {
                    id: dag.node(i).id.clone(),
                    kind: dag.node(i).kind.label().to_string(),
                    component: dag.node(i).kind.component().to_string(),
                    status: NodeStatus::Failed,
                    attempts: 0,
                    input: IoStats::default(),
                    output: IoStats::default(),
                    elapsed_ms: 0,
                    error: Some("la tarea del nodo terminó de forma anómala".to_string()),
                })
            })
            .collect();

        let succeeded = nodes.iter().all(|r| r.status == NodeStatus::Succeeded);
        let elapsed_ms = started.elapsed().as_millis() as u64;

        let _ = self.events.send(RunEvent::RunFinished {
            run_id: run_id.clone(),
            pipeline: pipeline.clone(),
            succeeded,
            elapsed_ms,
        });

        Ok(RunReport {
            run_id,
            pipeline,
            succeeded,
            elapsed_ms,
            nodes,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_node(
    runner: Runner,
    ctx_base: NodeContext,
    kind: String,
    component: String,
    retry: RetryPolicy,
    mut input: Input,
    output: Output,
    mut barriers: Vec<(String, watch::Receiver<NodeSignal>)>,
    signal_tx: watch::Sender<NodeSignal>,
    events: broadcast::Sender<RunEvent>,
) -> NodeReport {
    let started = Instant::now();
    let node_id = ctx_base.node.clone();
    let run_id = ctx_base.run_id.clone();

    // Dependencias de orden puro antes de tocar nada.
    if let Some(blocker) = wait_for_barriers(&mut barriers).await {
        let reason = format!("el nodo `{blocker}` del que depende no completó");
        tracing::warn!(blocker = %blocker, "nodo omitido");
        let _ = events.send(RunEvent::NodeSkipped {
            run_id,
            node: node_id.clone(),
            reason: reason.clone(),
        });
        let _ = signal_tx.send(NodeSignal::Skipped);
        return make_report(
            &node_id,
            &kind,
            &component,
            NodeStatus::Skipped,
            0,
            Some(reason),
            &input,
            &output,
            started,
        );
    }

    let mut attempt: u32 = 1;
    let (status, error) = loop {
        if attempt > 1 {
            let backoff = retry.backoff_for(attempt);
            tracing::info!(
                attempt,
                backoff_ms = backoff.as_millis() as u64,
                "reintentando nodo"
            );
            tokio::time::sleep(backoff).await;
        }

        let ctx = NodeContext {
            attempt,
            ..ctx_base.clone()
        };
        let _ = events.send(RunEvent::NodeStarted {
            run_id: run_id.clone(),
            node: node_id.clone(),
            kind: kind.clone(),
            component: component.clone(),
            attempt,
        });

        let result = match &runner {
            Runner::Source(source) => source.read(&ctx, &output).await,
            Runner::Transform(transform) => transform.apply(&ctx, &mut input, &output).await,
            Runner::Sink(sink) => sink.write(&ctx, &mut input).await,
        };

        match result {
            Ok(()) => break (NodeStatus::Succeeded, None),
            Err(err) => {
                // Un fallo aguas arriba no es culpa de este nodo y no se reintenta.
                if let OrchError::UpstreamFailed { upstream, .. } = &err {
                    let reason = format!("el nodo `{upstream}` del que depende no completó");
                    let _ = events.send(RunEvent::NodeSkipped {
                        run_id: run_id.clone(),
                        node: node_id.clone(),
                        reason: reason.clone(),
                    });
                    break (NodeStatus::Skipped, Some(reason));
                }

                // Reintentar sólo mientras no haya datos en vuelo: repetir un
                // nodo que ya consumió o emitió batches duplicaría o perdería
                // filas.
                let in_flight = input.has_consumed() || output.has_emitted();
                let will_retry = attempt < retry.max_attempts && !in_flight;

                tracing::error!(attempt, will_retry, error = %err, "el nodo falló");
                let _ = events.send(RunEvent::NodeFailed {
                    run_id: run_id.clone(),
                    node: node_id.clone(),
                    attempt,
                    error: err.to_string(),
                    will_retry,
                });

                if will_retry {
                    attempt += 1;
                    continue;
                }

                let message = if in_flight && attempt < retry.max_attempts {
                    format!("{err} (sin reintento: el nodo ya tenía datos en vuelo)")
                } else {
                    err.to_string()
                };
                break (NodeStatus::Failed, Some(message));
            }
        }
    };

    let report = make_report(
        &node_id, &kind, &component, status, attempt, error, &input, &output, started,
    );

    if status == NodeStatus::Succeeded {
        let _ = events.send(RunEvent::NodeFinished {
            run_id,
            node: node_id,
            input: report.input,
            output: report.output,
            elapsed_ms: report.elapsed_ms,
        });
    }

    // La señal debe publicarse ANTES de soltar los canales: así, cuando un
    // vecino ve su canal cerrado, el estado que lee ya es el definitivo.
    let _ = signal_tx.send(match status {
        NodeStatus::Succeeded => NodeSignal::Finished,
        NodeStatus::Failed => NodeSignal::Failed,
        NodeStatus::Skipped => NodeSignal::Skipped,
    });
    drop(output);
    drop(input);

    report
}

/// Recorre el DAG en orden topológico propagando esquemas.
///
/// Cada nodo declara lo que produce a partir de lo que recibe. Un `None` no
/// invalida nada: significa "todavía no se puede saber", y a partir de ahí la
/// cadena deja de propagarse. Lo que sí se detiene aquí son los errores que
/// un nodo detecta al ver sus entradas: una columna que no existe, un fan-in
/// con esquemas incompatibles o un `filter` con dos entradas.
async fn plan_schemas(dag: &Dag, runners: &[Runner]) -> Result<Vec<InputSchemas>> {
    let n = dag.len();
    let mut produced: Vec<Option<arrow::datatypes::SchemaRef>> = vec![None; n];
    let mut inputs: Vec<InputSchemas> = vec![InputSchemas::default(); n];

    for &i in dag.topological_order() {
        let ports = dag
            .upstream(i)
            .iter()
            .zip(dag.upstream_ports(i))
            .map(|(&from, port)| PortSchema {
                port: port.clone(),
                upstream: dag.node(from).id.clone(),
                schema: produced[from].clone(),
            })
            .collect();
        inputs[i] = InputSchemas::new(ports);

        produced[i] = match &runners[i] {
            Runner::Source(source) => source.schema().await?,
            Runner::Transform(transform) => transform.plan(&inputs[i]).await?,
            Runner::Sink(sink) => {
                sink.plan(&inputs[i]).await?;
                None
            }
        };

        tracing::debug!(
            node = %dag.node(i).id,
            schema = produced[i].as_ref().map(crate::schema::describe),
            "esquema propagado"
        );
    }

    Ok(inputs)
}

fn unzip3<A, B, C>(items: Vec<(A, B, C)>) -> (Vec<A>, Vec<B>, Vec<C>) {
    let mut a = Vec::with_capacity(items.len());
    let mut b = Vec::with_capacity(items.len());
    let mut c = Vec::with_capacity(items.len());
    for (x, y, z) in items {
        a.push(x);
        b.push(y);
        c.push(z);
    }
    (a, b, c)
}

#[allow(clippy::too_many_arguments)]
fn make_report(
    id: &str,
    kind: &str,
    component: &str,
    status: NodeStatus,
    attempts: u32,
    error: Option<String>,
    input: &Input,
    output: &Output,
    started: Instant,
) -> NodeReport {
    NodeReport {
        id: id.to_string(),
        kind: kind.to_string(),
        component: component.to_string(),
        status,
        attempts,
        input: input.stats(),
        output: output.stats(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        error,
    }
}

/// Espera a que terminen las dependencias de orden puro.
///
/// Devuelve el id de la primera que no completó, o `None` si todas terminaron.
async fn wait_for_barriers(
    barriers: &mut [(String, watch::Receiver<NodeSignal>)],
) -> Option<String> {
    for (id, rx) in barriers.iter_mut() {
        loop {
            let signal = *rx.borrow();
            match signal {
                NodeSignal::Finished => break,
                NodeSignal::Failed | NodeSignal::Skipped => return Some(id.clone()),
                NodeSignal::Pending => {
                    if rx.changed().await.is_err() {
                        // El emisor desapareció sin publicar un estado final.
                        return match *rx.borrow() {
                            NodeSignal::Finished => break,
                            _ => Some(id.clone()),
                        };
                    }
                }
            }
        }
    }
    None
}
