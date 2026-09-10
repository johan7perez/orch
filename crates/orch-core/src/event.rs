//! Eventos de ejecución en tiempo real.
//!
//! El ejecutor los publica en un canal `broadcast`. Hoy los consume la CLI;
//! en la Fase 1 los consumirá el puente de Tauri hacia el frontend y el
//! escritor de DuckDB, sin que el ejecutor tenga que cambiar.

use serde::Serialize;

use crate::io::IoStats;

/// Capacidad del canal de eventos. Si un suscriptor se retrasa más de esto,
/// pierde eventos (`RecvError::Lagged`) pero nunca frena la ejecución: la
/// telemetría jamás debe aplicar contrapresión sobre los datos.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunEvent {
    RunStarted {
        run_id: String,
        pipeline: String,
        nodes: usize,
    },
    NodeStarted {
        run_id: String,
        node: String,
        kind: String,
        component: String,
        attempt: u32,
    },
    NodeFinished {
        run_id: String,
        node: String,
        input: IoStats,
        output: IoStats,
        elapsed_ms: u64,
    },
    NodeFailed {
        run_id: String,
        node: String,
        attempt: u32,
        error: String,
        will_retry: bool,
    },
    NodeSkipped {
        run_id: String,
        node: String,
        reason: String,
    },
    RunFinished {
        run_id: String,
        pipeline: String,
        succeeded: bool,
        elapsed_ms: u64,
    },
}

impl RunEvent {
    pub fn run_id(&self) -> &str {
        match self {
            RunEvent::RunStarted { run_id, .. }
            | RunEvent::NodeStarted { run_id, .. }
            | RunEvent::NodeFinished { run_id, .. }
            | RunEvent::NodeFailed { run_id, .. }
            | RunEvent::NodeSkipped { run_id, .. }
            | RunEvent::RunFinished { run_id, .. } => run_id,
        }
    }
}
