//! Canales de `RecordBatch` entre nodos.
//!
//! Un nodo nunca ve el grafo: recibe un [`Input`] (0..n aristas entrantes ya
//! fusionadas) y un [`Output`] (0..n aristas salientes). Los batches de Arrow
//! son `Arc` por dentro, así que el fan-out clona punteros, no datos.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
}

impl Counters {
    fn record(&self, batch: &RecordBatch) {
        self.rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }
}

/// Métricas acumuladas de un lado (entrada o salida) de un nodo.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IoStats {
    pub rows: u64,
    pub batches: u64,
    pub bytes: u64,
}

/// Aristas salientes de un nodo.
#[derive(Debug)]
pub struct Output {
    node: NodeId,
    senders: Vec<BatchSender>,
    downstream_ids: Vec<NodeId>,
    downstream_signals: Vec<watch::Receiver<NodeSignal>>,
    counters: Counters,
    emitted: AtomicBool,
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
            emitted: AtomicBool::new(false),
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
            if tx.is_closed() {
                self.check_downstream(i)?;
                continue;
            }
            match tx.send(batch.clone()).await {
                Ok(()) => delivered += 1,
                Err(_) => self.check_downstream(i)?,
            }
        }

        if delivered > 0 {
            self.counters.record(&batch);
            self.emitted.store(true, Ordering::Release);
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
        self.emitted.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> IoStats {
        IoStats {
            rows: self.counters.rows.load(Ordering::Relaxed),
            batches: self.counters.batches.load(Ordering::Relaxed),
            bytes: self.counters.bytes.load(Ordering::Relaxed),
        }
    }
}

/// Aristas entrantes de un nodo, presentadas como un único flujo.
///
/// Las entradas se drenan en el orden en que se declararon las aristas
/// (concatenación, no intercalado): el resultado es determinista y, como el
/// grafo es acíclico, ningún productor puede quedarse bloqueado para siempre
/// aunque su canal se llene mientras se drena otro.
#[derive(Debug)]
pub struct Input {
    node: NodeId,
    receivers: Vec<BatchReceiver>,
    upstream_ids: Vec<NodeId>,
    upstream_signals: Vec<watch::Receiver<NodeSignal>>,
    cursor: usize,
    counters: Counters,
    consumed: AtomicBool,
}

impl Input {
    pub fn new(
        node: impl Into<NodeId>,
        receivers: Vec<BatchReceiver>,
        upstream_ids: Vec<NodeId>,
        upstream_signals: Vec<watch::Receiver<NodeSignal>>,
    ) -> Self {
        debug_assert_eq!(receivers.len(), upstream_ids.len());
        debug_assert_eq!(receivers.len(), upstream_signals.len());
        Self {
            node: node.into(),
            receivers,
            upstream_ids,
            upstream_signals,
            cursor: 0,
            counters: Counters::default(),
            consumed: AtomicBool::new(false),
        }
    }

    /// Siguiente batch, o `None` cuando todas las entradas terminaron bien.
    ///
    /// Si una entrada se cerró porque su nodo falló, devuelve
    /// [`OrchError::UpstreamFailed`] en vez de fingir un fin de flujo limpio:
    /// de otro modo un sink escribiría un resultado truncado y lo reportaría
    /// como éxito.
    pub async fn recv(&mut self) -> Result<Option<RecordBatch>> {
        while self.cursor < self.receivers.len() {
            match self.receivers[self.cursor].recv().await {
                Some(batch) => {
                    self.counters.record(&batch);
                    self.consumed.store(true, Ordering::Release);
                    return Ok(Some(batch));
                }
                None => {
                    // El emisor se libera después de publicar su señal, así que
                    // aquí ya es visible.
                    let signal = *self.upstream_signals[self.cursor].borrow();
                    if matches!(signal, NodeSignal::Failed | NodeSignal::Skipped) {
                        return Err(OrchError::UpstreamFailed {
                            node: self.node.clone(),
                            upstream: self.upstream_ids[self.cursor].clone(),
                        });
                    }
                    self.cursor += 1;
                }
            }
        }
        Ok(None)
    }

    pub fn has_consumed(&self) -> bool {
        self.consumed.load(Ordering::Acquire)
    }

    pub fn stats(&self) -> IoStats {
        IoStats {
            rows: self.counters.rows.load(Ordering::Relaxed),
            batches: self.counters.batches.load(Ordering::Relaxed),
            bytes: self.counters.bytes.load(Ordering::Relaxed),
        }
    }
}

/// Entrada desconectada, útil para probar sources y para nodos sin aristas.
pub fn empty_input(node: impl Into<NodeId>) -> Input {
    Input::new(node, Vec::new(), Vec::new(), Vec::new())
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
        Input::new(to, vec![rx], vec![from.to_string()], vec![from_signal]),
    )
}
