//! Visor de eventos en vivo para `orch run --follow`.
//!
//! Es uno de los consumidores del canal de telemetría; la ventana y el
//! historial se enganchan exactamente igual.

use std::collections::HashMap;

use orch_core::{NodeProgress, RunEvent};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

pub fn spawn(mut events: Receiver<RunEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut progreso = Progreso::default();
        loop {
            match events.recv().await {
                Ok(event) => {
                    let last = matches!(event, RunEvent::RunFinished { .. });
                    print(&event, &mut progreso);
                    if last {
                        break;
                    }
                }
                Err(RecvError::Closed) => break,
                // La telemetría nunca frena la ejecución: si el visor se
                // retrasa, pierde eventos y lo dice.
                Err(RecvError::Lagged(skipped)) => {
                    eprintln!("  … {skipped} evento(s) omitidos (visor por detrás)");
                }
            }
        }
    })
}

/// Cada cuánto, como mucho, una línea de progreso. El motor lo publica cuatro
/// veces por segundo; en una terminal eso es ruido.
const PROGRESO_CADA_MS: u64 = 2_000;

/// La última línea de progreso impresa: cuándo y cuántas filas llevaba cada
/// nodo, para sacar las filas por segundo desde entonces.
#[derive(Default)]
struct Progreso {
    impreso_ms: u64,
    filas: HashMap<String, u64>,
}

/// Las filas que ha movido un nodo: las que sacó, o las que recibió si no
/// saca nada (un destino).
fn movidas(nodo: &NodeProgress) -> u64 {
    if nodo.output.rows > 0 {
        nodo.output.rows
    } else {
        nodo.input.rows
    }
}

fn print(event: &RunEvent, progreso: &mut Progreso) {
    match event {
        RunEvent::RunStarted {
            pipeline, nodes, ..
        } => {
            eprintln!("▶ {pipeline} ({nodes} nodos)");
        }
        RunEvent::NodeStarted {
            node,
            kind,
            component,
            attempt,
            ..
        } => {
            let suffix = if *attempt > 1 {
                format!(" (intento {attempt})")
            } else {
                String::new()
            };
            eprintln!("  · {node} [{kind}:{component}] iniciado{suffix}");
        }
        RunEvent::NodeFinished {
            node,
            input,
            output,
            elapsed_ms,
            ..
        } => {
            eprintln!(
                "  ✓ {node} — in {} filas / out {} filas en {elapsed_ms} ms",
                input.rows, output.rows
            );
        }
        RunEvent::NodeFailed {
            node,
            attempt,
            error,
            will_retry,
            ..
        } => {
            let tail = if *will_retry {
                " (se reintentará)"
            } else {
                ""
            };
            eprintln!("  ✗ {node} — intento {attempt} falló{tail}: {error}");
        }
        RunEvent::NodeSkipped { node, reason, .. } => {
            eprintln!("  ⊘ {node} — omitido: {reason}");
        }
        RunEvent::RunFinished {
            pipeline,
            succeeded,
            elapsed_ms,
            ..
        } => {
            let mark = if *succeeded { "✓" } else { "✗" };
            eprintln!("{mark} {pipeline} terminó en {elapsed_ms} ms");
        }
        RunEvent::Progress {
            elapsed_ms, nodes, ..
        } => {
            let transcurrido = elapsed_ms.saturating_sub(progreso.impreso_ms);
            if transcurrido < PROGRESO_CADA_MS {
                return;
            }
            // Sólo lo que se movió desde la última línea: en un pipeline
            // grande, lo que importa es qué está trabajando ahora.
            let activos: Vec<String> = nodes
                .iter()
                .filter_map(|nodo| {
                    let ahora = movidas(nodo);
                    let antes = progreso.filas.get(&nodo.node).copied().unwrap_or(0);
                    (ahora > antes).then(|| {
                        let por_segundo = (ahora - antes) * 1_000 / transcurrido;
                        format!(
                            "{} {} filas ({}/s)",
                            nodo.node,
                            crate::report::thousands(ahora),
                            crate::report::thousands(por_segundo)
                        )
                    })
                })
                .collect();
            if !activos.is_empty() {
                eprintln!(
                    "  … {}  {}",
                    crate::report::human_ms(*elapsed_ms),
                    activos.join(" · ")
                );
            }
            progreso.impreso_ms = *elapsed_ms;
            progreso.filas = nodes.iter().map(|n| (n.node.clone(), movidas(n))).collect();
        }
    }
}
