//! Visor de eventos en vivo para `orch run --follow`.
//!
//! Es el primer consumidor del canal de telemetría; en la Fase 1 el frontend
//! de Tauri se engancha exactamente igual.

use orch_core::RunEvent;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

pub fn spawn(mut events: Receiver<RunEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    let last = matches!(event, RunEvent::RunFinished { .. });
                    print(&event);
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

fn print(event: &RunEvent) {
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
    }
}
