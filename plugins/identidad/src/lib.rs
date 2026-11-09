//! Plugin que devuelve cada lote tal cual.
//!
//! No hace nada a propósito: es la vara para medir lo que cuesta la frontera
//! de WebAssembly —copiar, serializar y llamar— sin trabajo que lo disimule.

use orch_plugin::{RecordBatch, SinConfig, Transformacion};

pub struct Identidad;

impl Transformacion for Identidad {
    const NOMBRE: &'static str = "identidad";
    type Config = SinConfig;

    fn nueva(_config: SinConfig) -> Result<Self, String> {
        Ok(Self)
    }

    fn procesar(&mut self, lote: RecordBatch) -> Result<Vec<RecordBatch>, String> {
        Ok(vec![lote])
    }
}

orch_plugin::exportar!(Identidad);
