//! Adaptador entre el flujo de entrada de un nodo y una tabla de DataFusion.
//!
//! DataFusion necesita un `TableProvider`, y `StreamingTable` acepta uno
//! construido sobre [`PartitionStream`]. Eso deja al motor **tirando** de
//! nuestros batches en vez de exigir que el dataset esté materializado: un
//! `WHERE` sobre un CSV de 50 GB se resuelve con la memoria de unos pocos
//! lotes.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub(crate) type BatchResult = Result<RecordBatch, DataFusionError>;

/// Partición de una sola pasada alimentada por un canal.
///
/// El flujo de un nodo sólo se puede recorrer una vez, así que el receptor se
/// entrega al primer `execute`. Si el plan pidiera escanear la tabla dos veces
/// (un self-join, por ejemplo), el segundo escaneo falla con un mensaje claro
/// en vez de devolver un resultado vacío en silencio.
#[derive(Debug)]
pub(crate) struct InputPartition {
    schema: SchemaRef,
    table: String,
    receiver: Mutex<Option<mpsc::Receiver<BatchResult>>>,
}

impl InputPartition {
    pub(crate) fn new(
        schema: SchemaRef,
        table: impl Into<String>,
        receiver: mpsc::Receiver<BatchResult>,
    ) -> Self {
        Self {
            schema,
            table: table.into(),
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl PartitionStream for InputPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let taken = self
            .receiver
            .lock()
            .expect("el mutex de la partición no debería envenenarse")
            .take();

        match taken {
            Some(receiver) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                ReceiverStream::new(receiver),
            )),
            None => {
                let table = self.table.clone();
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    futures::stream::once(async move {
                        Err(DataFusionError::Execution(format!(
                            "la tabla `{table}` es un flujo de una sola pasada y el plan \
                             intentó escanearla más de una vez; materializa el paso previo \
                             en un fichero si necesitas leerla dos veces"
                        )))
                    }),
                ))
            }
        }
    }
}
