//! Canales de `RecordBatch` entre nodos.
//!
//! Un nodo nunca ve el grafo: recibe un [`Input`] (0..n aristas entrantes) y
//! un [`Output`] (0..n aristas salientes). Los batches de Arrow son `Arc` por
//! dentro, así que el fan-out clona punteros, no datos.
//!
//! Cada arista entrante es un **puerto** con nombre —por defecto el id del
//! nodo de origen—. La mayoría de nodos los concatenan con [`Input::recv`] y
//! ni se enteran; los que necesitan distinguirlos (un join en SQL) los toman
//! por separado con [`Input::take_ports`].

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use tokio::sync::{mpsc, watch};

use crate::error::{OrchError, Result};
use crate::spec::NodeId;

pub type BatchSender = mpsc::Sender<RecordBatch>;
pub type BatchReceiver = mpsc::Receiver<RecordBatch>;

/// Estado publicado por cada nodo para que sus vecinos lo observen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeSignal {
    Pending,
    Finished,
    Failed,
    Skipped,
}

impl NodeSignal {
    pub fn is_terminal(self) -> bool {
        !matches!(self, NodeSignal::Pending)
    }
}

/// Crea el canal de una arista de datos.
pub fn batch_channel(capacity: usize) -> (BatchSender, BatchReceiver) {
    mpsc::channel(capacity)
}

#[derive(Debug, Default)]
struct Counters {
    rows: AtomicU64,
    batches: AtomicU64,
    bytes: AtomicU64,
    stalled_nanos: AtomicU64,
    touched: AtomicBool,
}

impl Counters {
    fn record(&self, batch: &RecordBatch) {
        self.rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
        self.touched.store(true, Ordering::Release);
    }

    fn stalled(&self, elapsed: std::time::Duration) {
        self.stalled_nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn stats(&self) -> IoStats {
        IoStats {
            rows: self.rows.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            stalled_ms: self.stalled_nanos.load(Ordering::Relaxed) / 1_000_000,
        }
    }

    fn touched(&self) -> bool {
        self.touched.load(Ordering::Acquire)
    }
}

/// Métricas acumuladas de un lado (entrada o salida) de un nodo.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IoStats {
    pub rows: u64,
    pub batches: u64,
    pub bytes: u64,
    /// Tiempo parado esperando al vecino.
    ///
    /// En la **salida** es contrapresión: el consumidor no daba abasto. En la
    /// **entrada** es hambre: el productor no traía datos. El nodo que no
    /// espera por ningún lado es el cuello de botella, y esta es la cifra que
    /// lo señala.
    pub stalled_ms: u64,
}

/// Aristas salientes de un nodo.
#[derive(Debug)]
pub struct Output {
    node: NodeId,
    senders: Vec<BatchSender>,
    downstream_ids: Vec<NodeId>,
    downstream_signals: Vec<watch::Receiver<NodeSignal>>,
    counters: Counters,
    closed: AtomicBool,
}

impl Output {
    pub fn new(
        node: impl Into<NodeId>,
        senders: Vec<BatchSender>,
        downstream_ids: Vec<NodeId>,
        downstream_signals: Vec<watch::Receiver<NodeSignal>>,
    ) -> Self {
        debug_assert_eq!(senders.len(), downstream_ids.len());
        debug_assert_eq!(senders.len(), downstream_signals.len());
        Self {
            node: node.into(),
            senders,
            downstream_ids,
            downstream_signals,
            counters: Counters::default(),
            closed: AtomicBool::new(false),
        }
    }

    /// Publica un batch a todos los consumidores, aplicando contrapresión.
    ///
    /// Los batches vacíos se descartan: no aportan filas y obligarían a cada
    /// sink a defenderse de ellos.
    ///
    /// Un consumidor que ya terminó **bien** (el caso de `limit`, que corta
    /// en cuanto junta las filas que pedía) no es un error: simplemente deja
    /// de recibir. Sólo si cerró sin completar se propaga el fallo. Cuando
    /// ningún consumidor sigue escuchando, [`Output::is_closed`] pasa a `true`
    /// para que el productor pueda parar en vez de generar datos que nadie
    /// leerá.
    pub async fn send(&self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut delivered = 0usize;
        for (i, tx) in self.senders.iter().enumerate() {
            // Se intenta sin esperar primero: cuando hay hueco —el caso
            // normal— no se lee el reloj ni una vez. El coste de medir la
            // contrapresión sólo se paga cuando de verdad la hay.
            match tx.try_send(batch.clone()) {
                Ok(()) => {
                    delivered += 1;
                    continue;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.check_downstream(i)?;
                    continue;
                }
                Err(mpsc::error::TrySendError::Full(batch)) => {
                    let waiting = Instant::now();
                    let outcome = tx.send(batch).await;
                    self.counters.stalled(waiting.elapsed());
                    match outcome {
                        Ok(()) => delivered += 1,
                        Err(_) => self.check_downstream(i)?,
                    }
                }
            }
        }

        if delivered > 0 {
            self.counters.record(&batch);
        } else if !self.senders.is_empty() {
            self.closed.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Un canal cerrado sólo es aceptable si el consumidor terminó bien.
    fn check_downstream(&self, i: usize) -> Result<()> {
        match *self.downstream_signals[i].borrow() {
            NodeSignal::Finished => Ok(()),
            _ => Err(OrchError::DownstreamFailed {
                node: self.node.clone(),
                downstream: self.downstream_ids[i].clone(),
            }),
        }
    }

    /// `true` cuando ningún consumidor sigue leyendo. Los orígenes deberían
    /// consultarlo para dejar de producir.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn has_emitted(&self) -> bool {
        self.counters.touched()
    }

    pub fn stats(&self) -> IoStats {
        self.counters.stats()
    }
}

/// Una arista entrante concreta.
///
/// Se obtiene con [`Input::take_ports`] cuando un nodo necesita tratar sus
/// entradas por separado. Cada puerto es independiente: pueden consumirse en
/// paralelo sin aliasing, que es justo lo que necesita un join.
#[derive(Debug)]
pub struct InputPort {
    node: NodeId,
    /// Nombre del puerto; por defecto, el id del nodo de origen.
    pub name: String,
    pub upstream: NodeId,
    receiver: BatchReceiver,
    signal: watch::Receiver<NodeSignal>,
    counters: Arc<Counters>,
}

impl InputPort {
    /// Siguiente batch de este puerto, o `None` si terminó bien.
    pub async fn recv(&mut self) -> Result<Option<RecordBatch>> {
        // Igual que en la salida: si ya hay un lote esperando, no se mide
        // nada. El reloj sólo entra cuando hay que quedarse esperando.
        let received = match self.receiver.try_recv() {
            Ok(batch) => Some(batch),
            Err(mpsc::error::TryRecvError::Disconnected) => None,
            Err(mpsc::error::TryRecvError::Empty) => {
                let waiting = Instant::now();
                let batch = self.receiver.recv().await;
                self.counters.stalled(waiting.elapsed());
                batch
            }
        };

        match received {
            Some(batch) => {
                self.counters.record(&batch);
                Ok(Some(batch))
            }
            None => {
                // El emisor se libera después de publicar su señal, así que
                // aquí ya es visible.
                match *self.signal.borrow() {
                    NodeSignal::Failed | NodeSignal::Skipped => Err(OrchError::UpstreamFailed {
                        node: self.node.clone(),
                        upstream: self.upstream.clone(),
                    }),
                    _ => Ok(None),
                }
            }
        }
    }
}

/// Aristas entrantes de un nodo.
///
/// [`Input::recv`] las presenta como un único flujo, drenándolas en el orden
/// en que se declararon las aristas (concatenación, no intercalado): el
/// resultado es determinista y, como el grafo es acíclico, ningún productor
/// puede quedarse bloqueado para siempre aunque su canal se llene mientras se
/// drena otro.
#[derive(Debug)]
pub struct Input {
    node: NodeId,
    ports: Vec<InputPort>,
    cursor: usize,
    counters: Arc<Counters>,
}

impl Input {
    pub fn new(
        node: impl Into<NodeId>,
        receivers: Vec<BatchReceiver>,
        port_names: Vec<String>,
        upstream_ids: Vec<NodeId>,
        upstream_signals: Vec<watch::Receiver<NodeSignal>>,
    ) -> Self {
        let node = node.into();
        debug_assert_eq!(receivers.len(), port_names.len());
        debug_assert_eq!(receivers.len(), upstream_ids.len());
        debug_assert_eq!(receivers.len(), upstream_signals.len());

        // Un único juego de contadores compartido: las métricas del nodo
        // siguen siendo correctas aunque los puertos se hayan repartido.
        let counters = Arc::new(Counters::default());
        let ports = receivers
            .into_iter()
            .zip(port_names)
            .zip(upstream_ids)
            .zip(upstream_signals)
            .map(|(((receiver, name), upstream), signal)| InputPort {
                node: node.clone(),
                name,
                upstream,
                receiver,
                signal,
                counters: Arc::clone(&counters),
            })
            .collect();

        Self {
            node,
            ports,
            cursor: 0,
            counters,
        }
    }

    /// Siguiente batch, o `None` cuando todas las entradas terminaron bien.
    ///
    /// Si una entrada se cerró porque su nodo falló, devuelve
    /// [`OrchError::UpstreamFailed`] en vez de fingir un fin de flujo limpio:
    /// de otro modo un sink escribiría un resultado truncado y lo reportaría
    /// como éxito.
    pub async fn recv(&mut self) -> Result<Option<RecordBatch>> {
        while self.cursor < self.ports.len() {
            match self.ports[self.cursor].recv().await? {
                Some(batch) => return Ok(Some(batch)),
                None => self.cursor += 1,
            }
        }
        Ok(None)
    }

    /// Toma las entradas por separado, dejando el `Input` vacío.
    ///
    /// Es lo que usa un join: cada puerto se convierte en una tabla distinta.
    /// Las métricas del nodo se siguen contabilizando.
    pub fn take_ports(&mut self) -> Vec<InputPort> {
        self.cursor = 0;
        std::mem::take(&mut self.ports)
    }

    pub fn port_names(&self) -> Vec<&str> {
        self.ports.iter().map(|p| p.name.as_str()).collect()
    }

    pub fn len(&self) -> usize {
        self.ports.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ports.is_empty()
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// `true` si el nodo ya recibió algún batch. El ejecutor lo consulta para
    /// decidir si un reintento sigue siendo seguro.
    pub fn has_consumed(&self) -> bool {
        self.counters.touched()
    }

    pub fn stats(&self) -> IoStats {
        self.counters.stats()
    }
}

/// Entrada desconectada, útil para probar sources y para nodos sin aristas.
pub fn empty_input(node: impl Into<NodeId>) -> Input {
    Input::new(node, Vec::new(), Vec::new(), Vec::new(), Vec::new())
}

/// Salida sin consumidores, útil para probar sinks.
pub fn discarding_output(node: impl Into<NodeId>) -> Output {
    Output::new(node, Vec::new(), Vec::new(), Vec::new())
}

/// Emparejador para tests: un [`Output`] y el [`Input`] que lo consume.
pub fn connected_pair(from: &str, to: &str, capacity: usize) -> (Output, Input) {
    let (tx, rx) = batch_channel(capacity);
    // El receptor de un `watch` sigue viendo el último valor aunque el emisor
    // ya no exista, así que basta con dejarlos caer.
    let (_from_tx, from_signal) = watch::channel(NodeSignal::Finished);
    let (_to_tx, to_signal) = watch::channel(NodeSignal::Finished);
    (
        Output::new(from, vec![tx], vec![to.to_string()], vec![to_signal]),
        Input::new(
            to,
            vec![rx],
            vec![from.to_string()],
            vec![from.to_string()],
            vec![from_signal],
        ),
    )
}
