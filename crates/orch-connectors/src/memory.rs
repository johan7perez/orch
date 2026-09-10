//! Origen y destino en memoria.
//!
//! No se registran por nombre porque su configuración son batches de Arrow,
//! no YAML. Existen para construir pipelines desde Rust en tests y benchmarks
//! sin tocar el disco.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{Input, NodeContext, Output, Result, Sink, Source};

/// Origen que emite una lista fija de batches.
pub struct MemorySource {
    batches: Vec<RecordBatch>,
}

impl MemorySource {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl Source for MemorySource {
    fn connector(&self) -> &str {
        "memory"
    }

    async fn read(&self, _ctx: &NodeContext, output: &Output) -> Result<()> {
        for batch in &self.batches {
            if output.is_closed() {
                break;
            }
            output.send(batch.clone()).await?;
        }
        Ok(())
    }
}

/// Handle compartido con lo que un [`MemorySink`] ha recibido.
pub type Collected = Arc<Mutex<Vec<RecordBatch>>>;

/// Destino que acumula los batches recibidos.
pub struct MemorySink {
    collected: Collected,
}

impl MemorySink {
    /// Devuelve el sink y el handle desde el que leer el resultado.
    pub fn pair() -> (Self, Collected) {
        let collected: Collected = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                collected: Arc::clone(&collected),
            },
            collected,
        )
    }
}

#[async_trait]
impl Sink for MemorySink {
    fn connector(&self) -> &str {
        "memory"
    }

    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> Result<()> {
        while let Some(batch) = input.recv().await? {
            // Mutex de `std`: el bloqueo no cruza ningún `await`.
            self.collected
                .lock()
                .expect("el mutex del sink en memoria no debería envenenarse")
                .push(batch);
        }
        Ok(())
    }
}

/// Total de filas acumuladas en un [`Collected`].
pub fn total_rows(collected: &Collected) -> usize {
    collected
        .lock()
        .expect("el mutex del sink en memoria no debería envenenarse")
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}
