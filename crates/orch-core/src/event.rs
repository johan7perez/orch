//! Eventos de ejecución en tiempo real.
//!
//! El ejecutor los publica en un canal `broadcast`, y los consumen el visor
//! de la CLI, el escritor de DuckDB y el puente de Tauri hacia la ventana,
//! cada uno por su cuenta.

use serde::{Deserialize, Serialize};

use crate::io::IoStats;

/// Capacidad del canal de eventos. Si un suscriptor se retrasa más de esto,
/// pierde eventos (`RecvError::Lagged`) pero nunca frena la ejecución: la
/// telemetría jamás debe aplicar contrapresión sobre los datos.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Cómo va cada nodo. Se publica cada [`PROGRESS_EVERY`] mientras la
    /// ejecución sigue viva y algo se ha movido.
    ///
    /// Los contadores son acumulados desde el arranque: las filas por segundo
    /// salen de dos seguidos, y perder uno (un oyente que se retrasa) no
    /// desajusta nada.
    ///
    /// [`PROGRESS_EVERY`]: crate::executor::PROGRESS_EVERY
    Progress {
        run_id: String,
        elapsed_ms: u64,
        nodes: Vec<NodeProgress>,
    },
}

/// Lo que lleva movido un nodo hasta ahora.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProgress {
    pub node: String,
    pub input: IoStats,
    pub output: IoStats,
}

impl RunEvent {
    pub fn run_id(&self) -> &str {
        match self {
            RunEvent::RunStarted { run_id, .. }
            | RunEvent::NodeStarted { run_id, .. }
            | RunEvent::NodeFinished { run_id, .. }
            | RunEvent::NodeFailed { run_id, .. }
            | RunEvent::NodeSkipped { run_id, .. }
            | RunEvent::RunFinished { run_id, .. }
            | RunEvent::Progress { run_id, .. } => run_id,
        }
    }
}
