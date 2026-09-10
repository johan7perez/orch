//! Escritor de eventos.
//!
//! Se engancha al canal `broadcast` del ejecutor y va volcando a DuckDB en
//! lotes. Nunca frena la ejecución: el canal descarta eventos si el
//! consumidor se retrasa, que es exactamente lo que debe pasar — la
//! telemetría no puede ser la que decida el ritmo de los datos.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use orch_core::RunEvent;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;

use crate::store::{PendingEvent, Store};

/// Eventos acumulados antes de escribir.
const BATCH: usize = 256;
/// Tiempo máximo que un evento espera en el buffer.
const FLUSH_EVERY: Duration = Duration::from_millis(200);

pub struct EventWriter;

impl EventWriter {
    /// Arranca el escritor. El handle termina cuando el canal se cierra.
    pub fn spawn(store: Arc<Store>, mut events: Receiver<RunEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut buffer: Vec<PendingEvent> = Vec::with_capacity(BATCH);
            let mut seq: i64 = 0;
            let mut ticker = tokio::time::interval(FLUSH_EVERY);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    received = events.recv() => match received {
                        Ok(event) => {
                            buffer.push(to_pending(&event, seq));
                            seq += 1;
                            if buffer.len() >= BATCH {
                                flush(&store, &mut buffer).await;
                            }
                        }
                        Err(RecvError::Closed) => break,
                        Err(RecvError::Lagged(skipped)) => {
                            // Que se pierdan eventos es aceptable; que no se
                            // sepa, no.
                            tracing::warn!(
                                skipped,
                                "el almacén se retrasó y perdió eventos"
                            );
                        }
                    },
                    _ = ticker.tick() => flush(&store, &mut buffer).await,
                }
            }

            flush(&store, &mut buffer).await;
        })
    }
}

async fn flush(store: &Arc<Store>, buffer: &mut Vec<PendingEvent>) {
    if buffer.is_empty() {
        return;
    }
    let batch = std::mem::take(buffer);
    let store = Arc::clone(store);
    // DuckDB es sincrónico: se escribe en un hilo de bloqueo para no ocupar
    // uno del runtime, que es donde corren los nodos.
    let outcome = tokio::task::spawn_blocking(move || store.append_events(&batch)).await;

    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(error = %err, "no se pudieron guardar los eventos"),
        Err(err) => tracing::error!(error = %err, "el escritor de eventos se cayó"),
    }
}

fn to_pending(event: &RunEvent, seq: i64) -> PendingEvent {
    let (kind, node, detail) = describe(event);
    PendingEvent {
        run_id: event.run_id().to_string(),
        seq,
        at: Utc::now(),
        kind: kind.to_string(),
        node,
        detail,
        // El evento entero se guarda tal cual: mañana habrá campos nuevos y
        // las consultas viejas seguirán funcionando.
        payload: serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string()),
    }
}

fn describe(event: &RunEvent) -> (&'static str, Option<String>, Option<String>) {
    match event {
        RunEvent::RunStarted {
            pipeline, nodes, ..
        } => (
            "run_started",
            None,
            Some(format!("{pipeline} ({nodes} nodos)")),
        ),
        RunEvent::NodeStarted {
            node,
            kind,
            component,
            attempt,
            ..
        } => (
            "node_started",
            Some(node.clone()),
            Some(format!("{kind}:{component} intento {attempt}")),
        ),
        RunEvent::NodeFinished {
            node,
            input,
            output,
            elapsed_ms,
            ..
        } => (
            "node_finished",
            Some(node.clone()),
            Some(format!(
                "in {} / out {} filas en {elapsed_ms} ms",
                input.rows, output.rows
            )),
        ),
        RunEvent::NodeFailed {
            node,
            attempt,
            error,
            will_retry,
            ..
        } => (
            "node_failed",
            Some(node.clone()),
            Some(format!(
                "intento {attempt}{}: {error}",
                if *will_retry {
                    " (se reintentará)"
                } else {
                    ""
                }
            )),
        ),
        RunEvent::NodeSkipped { node, reason, .. } => {
            ("node_skipped", Some(node.clone()), Some(reason.clone()))
        }
        RunEvent::RunFinished {
            succeeded,
            elapsed_ms,
            ..
        } => (
            "run_finished",
            None,
            Some(format!(
                "{} en {elapsed_ms} ms",
                if *succeeded { "correcto" } else { "fallido" }
            )),
        ),
    }
}
