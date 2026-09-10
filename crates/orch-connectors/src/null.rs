//! Destino que descarta todo lo que recibe.
//!
//! Sirve para medir el coste del camino de lectura + transformación sin que
//! el destino contamine la medición, y para hacer un "dry run" de un pipeline
//! sin escribir nada.

use std::sync::Arc;

use async_trait::async_trait;
use orch_core::{Input, NodeContext, Registry, Result, Sink};

pub fn register(registry: &mut Registry) {
    registry.register_sink("null", |_node, _config| {
        let sink: Arc<dyn Sink> = Arc::new(NullSink);
        Ok(sink)
    });
}

pub struct NullSink;

#[async_trait]
impl Sink for NullSink {
    fn connector(&self) -> &str {
        "null"
    }

    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> Result<()> {
        // Las métricas de filas y bytes las contabiliza `Input` al recibir.
        while input.recv().await?.is_some() {}
        Ok(())
    }
}
