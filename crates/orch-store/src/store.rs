//! Lectura y escritura del almacén.

use std::path::Path;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use duckdb::{params, Connection};
use orch_core::{NodeStatus, OrchError, Result, RunReport};
use serde::Serialize;

use crate::schema::{MIGRATIONS, VERSION_TABLE};

fn failed(context: &str, err: impl std::fmt::Display) -> OrchError {
    OrchError::Other(format!("almacén: {context}: {err}"))
}

/// Un fichero DuckDB con el historial de ejecuciones.
///
/// La conexión va tras un `Mutex` porque la de DuckDB no es `Sync`. No es un
/// cuello de botella: sólo la usan el escritor de eventos y los comandos de
/// consulta, nunca el camino de los datos.
pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| failed(&format!("no se pudo crear `{}`", parent.display()), e))?;
            }
        }
        let conn = Connection::open(path)
            .map_err(|e| failed(&format!("no se pudo abrir `{}`", path.display()), e))?;
        Self::from_connection(conn)
    }

    /// Almacén en memoria, para tests.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| failed("no se pudo abrir", e))?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.lock();
        conn.execute_batch(VERSION_TABLE)
            .map_err(|e| failed("no se pudo preparar el control de versiones", e))?;

        let applied: i64 = conn
            .query_row(
                "SELECT coalesce(max(version), 0) FROM schema_version",
                [],
                |row| row.get(0),
            )
            .map_err(|e| failed("no se pudo leer la versión del esquema", e))?;

        for (index, statement) in MIGRATIONS.iter().enumerate() {
            let version = index as i64 + 1;
            if version <= applied {
                continue;
            }
            conn.execute_batch(statement)
                .map_err(|e| failed(&format!("falló la migración {version}"), e))?;
            conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, make_timestamp(?))",
                params![version, Utc::now().timestamp_micros()],
            )
            .map_err(|e| failed("no se pudo anotar la migración", e))?;
            tracing::debug!(version, "migración aplicada");
        }
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Guarda el resultado de una ejecución.
    ///
    /// Se reescribe si ya existía: una ejecución se anota como `running` en
    /// cuanto arranca y se completa al terminar.
    pub fn record_run(&self, report: &RunReport, started_at: DateTime<Utc>) -> Result<()> {
        let conn = self.lock();
        conn.execute("BEGIN", [])
            .map_err(|e| failed("no se pudo abrir la transacción", e))?;

        let outcome = (|| -> Result<()> {
            conn.execute("DELETE FROM runs WHERE run_id = ?", params![report.run_id])
                .map_err(|e| failed("no se pudo limpiar la ejecución previa", e))?;
            conn.execute(
                "DELETE FROM node_runs WHERE run_id = ?",
                params![report.run_id],
            )
            .map_err(|e| failed("no se pudo limpiar los nodos previos", e))?;

            conn.execute(
                "INSERT INTO runs
                 (run_id, pipeline, started_at_us, finished_at_us, status, elapsed_ms, nodes)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    report.run_id,
                    report.pipeline,
                    started_at.timestamp_micros(),
                    started_at.timestamp_micros() + report.elapsed_ms as i64 * 1_000,
                    if report.succeeded {
                        "succeeded"
                    } else {
                        "failed"
                    },
                    report.elapsed_ms as i64,
                    report.nodes.len() as i32,
                ],
            )
            .map_err(|e| failed("no se pudo guardar la ejecución", e))?;

            for (position, node) in report.nodes.iter().enumerate() {
                conn.execute(
                    "INSERT INTO node_runs
                     (run_id, node, position, kind, component, status, attempts,
                      rows_in, batches_in, bytes_in, stalled_in_ms,
                      rows_out, batches_out, bytes_out, stalled_out_ms,
                      elapsed_ms, error)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        report.run_id,
                        node.id,
                        position as i32,
                        node.kind,
                        node.component,
                        status_name(node.status),
                        node.attempts as i32,
                        node.input.rows as i64,
                        node.input.batches as i64,
                        node.input.bytes as i64,
                        node.input.stalled_ms as i64,
                        node.output.rows as i64,
                        node.output.batches as i64,
                        node.output.bytes as i64,
                        node.output.stalled_ms as i64,
                        node.elapsed_ms as i64,
                        node.error,
                    ],
                )
                .map_err(|e| failed(&format!("no se pudo guardar el nodo `{}`", node.id), e))?;
            }
            Ok(())
        })();

        match outcome {
            Ok(()) => conn
                .execute("COMMIT", [])
                .map(|_| ())
                .map_err(|e| failed("no se pudo confirmar", e)),
            Err(err) => {
                let _ = conn.execute("ROLLBACK", []);
                Err(err)
            }
        }
    }

    /// Añade un lote de eventos.
    pub fn append_events(&self, events: &[PendingEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let conn = self.lock();
        let mut appender = conn
            .appender("events")
            .map_err(|e| failed("no se pudo abrir el appender de eventos", e))?;
        for event in events {
            appender
                .append_row(params![
                    event.run_id,
                    event.seq,
                    event.at.timestamp_micros(),
                    event.kind,
                    event.node,
                    event.detail,
                    event.payload,
                ])
                .map_err(|e| failed("no se pudo añadir un evento", e))?;
        }
        appender
            .flush()
            .map_err(|e| failed("no se pudieron volcar los eventos", e))?;
        Ok(())
    }

    /// Últimas ejecuciones, de la más reciente a la más antigua.
    pub fn recent_runs(&self, limit: usize) -> Result<Vec<RunSummary>> {
        let conn = self.lock();
        let mut statement = conn
            .prepare(
                "SELECT run_id, pipeline, started_at_us, status, elapsed_ms, nodes,
                        (SELECT count(*) FROM node_runs n
                          WHERE n.run_id = r.run_id AND n.status = 'failed') AS failed_nodes
                 FROM runs r
                 ORDER BY started_at_us DESC
                 LIMIT ?",
            )
            .map_err(|e| failed("no se pudo preparar la consulta", e))?;

        let rows = statement
            .query_map(params![limit as i64], |row| {
                Ok(RunSummary {
                    run_id: row.get(0)?,
                    pipeline: row.get(1)?,
                    started_at: from_micros(row.get(2)?),
                    status: row.get(3)?,
                    elapsed_ms: row.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                    nodes: row.get::<_, i32>(5)? as usize,
                    failed_nodes: row.get::<_, i64>(6)? as usize,
                })
            })
            .map_err(|e| failed("no se pudieron leer las ejecuciones", e))?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| failed("no se pudieron leer las ejecuciones", e))
    }

    /// Detalle por nodo de una ejecución, con el throughput ya calculado.
    pub fn nodes_of(&self, run_id: &str) -> Result<Vec<NodeRow>> {
        let conn = self.lock();
        let mut statement = conn
            .prepare(
                "SELECT n.node, n.kind, n.component, n.status, n.attempts,
                        n.rows_in, n.rows_out, n.elapsed_ms,
                        n.stalled_in_ms, n.stalled_out_ms, n.error,
                        t.rows_per_second, t.busy_pct, t.busy_ms
                 FROM node_runs n
                 JOIN node_throughput t ON t.run_id = n.run_id AND t.node = n.node
                 WHERE n.run_id = ?
                 ORDER BY n.position",
            )
            .map_err(|e| failed("no se pudo preparar la consulta", e))?;

        let rows = statement
            .query_map(params![run_id], |row| {
                Ok(NodeRow {
                    node: row.get(0)?,
                    kind: row.get(1)?,
                    component: row.get(2)?,
                    status: row.get(3)?,
                    attempts: row.get::<_, i32>(4)? as u32,
                    rows_in: row.get::<_, i64>(5)? as u64,
                    rows_out: row.get::<_, i64>(6)? as u64,
                    elapsed_ms: row.get::<_, i64>(7)? as u64,
                    stalled_in_ms: row.get::<_, i64>(8)? as u64,
                    stalled_out_ms: row.get::<_, i64>(9)? as u64,
                    error: row.get(10)?,
                    rows_per_second: row.get::<_, Option<f64>>(11)?.unwrap_or(0.0),
                    busy_pct: row.get::<_, Option<f64>>(12)?.unwrap_or(0.0),
                    busy_ms: row.get::<_, i64>(13)? as u64,
                })
            })
            .map_err(|e| failed("no se pudieron leer los nodos", e))?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| failed("no se pudieron leer los nodos", e))
    }

    /// Eventos de una ejecución, en orden.
    pub fn events_of(&self, run_id: &str, limit: usize) -> Result<Vec<EventRow>> {
        let conn = self.lock();
        let mut statement = conn
            .prepare(
                "SELECT seq, at_us, kind, node, detail
                 FROM events WHERE run_id = ?
                 ORDER BY seq LIMIT ?",
            )
            .map_err(|e| failed("no se pudo preparar la consulta", e))?;

        let rows = statement
            .query_map(params![run_id, limit as i64], |row| {
                Ok(EventRow {
                    seq: row.get::<_, i64>(0)? as u64,
                    at: from_micros(row.get(1)?),
                    kind: row.get(2)?,
                    node: row.get(3)?,
                    detail: row.get(4)?,
                })
            })
            .map_err(|e| failed("no se pudieron leer los eventos", e))?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| failed("no se pudieron leer los eventos", e))
    }

    /// Resuelve un prefijo de `run_id` a la ejecución completa.
    ///
    /// Los identificadores son UUID: nadie va a teclear los 36 caracteres.
    pub fn resolve_run(&self, prefix: &str) -> Result<String> {
        let conn = self.lock();
        let mut statement = conn
            .prepare("SELECT run_id FROM runs WHERE run_id LIKE ? ORDER BY started_at_us DESC")
            .map_err(|e| failed("no se pudo preparar la consulta", e))?;
        let matches: Vec<String> = statement
            .query_map(params![format!("{prefix}%")], |row| row.get(0))
            .map_err(|e| failed("no se pudo buscar la ejecución", e))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| failed("no se pudo buscar la ejecución", e))?;

        match matches.as_slice() {
            [only] => Ok(only.clone()),
            [] => Err(OrchError::Other(format!(
                "no hay ninguna ejecución que empiece por `{prefix}`"
            ))),
            many => Err(OrchError::Other(format!(
                "`{prefix}` coincide con {} ejecuciones; alarga el identificador",
                many.len()
            ))),
        }
    }
}

/// Un evento pendiente de escribir.
#[derive(Debug, Clone)]
pub struct PendingEvent {
    pub run_id: String,
    pub seq: i64,
    pub at: DateTime<Utc>,
    pub kind: String,
    pub node: Option<String>,
    pub detail: Option<String>,
    pub payload: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub pipeline: String,
    pub started_at: DateTime<Utc>,
    pub status: String,
    pub elapsed_ms: u64,
    pub nodes: usize,
    pub failed_nodes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeRow {
    pub node: String,
    pub kind: String,
    pub component: String,
    pub status: String,
    pub attempts: u32,
    pub rows_in: u64,
    pub rows_out: u64,
    pub elapsed_ms: u64,
    pub stalled_in_ms: u64,
    pub stalled_out_ms: u64,
    pub error: Option<String>,
    pub rows_per_second: f64,
    /// Porcentaje del tiempo del nodo en que estaba trabajando de verdad.
    pub busy_pct: f64,
    /// Tiempo trabajando sin esperar a ningún vecino. El nodo con más
    /// `busy_ms` es el cuello de botella: el resto le espera.
    pub busy_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventRow {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub kind: String,
    pub node: Option<String>,
    pub detail: Option<String>,
}

fn status_name(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Succeeded => "succeeded",
        NodeStatus::Failed => "failed",
        NodeStatus::Skipped => "skipped",
    }
}

/// Microsegundos desde epoch, tal y como se guardan.
fn from_micros(micros: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(micros).unwrap_or_default()
}
