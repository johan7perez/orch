//! Lo que la ventana puede pedirle al motor.
//!
//! Ningún comando hace trabajo pesado en línea: ejecutar un pipeline lanza
//! una tarea y responde al momento, y los resultados llegan por eventos. La
//! UI no puede quedarse bloqueada porque alguien mueva diez millones de
//! filas.

use std::path::PathBuf;
use std::sync::Arc;

use orch_core::{Dag, Executor, PipelineSpec};
use orch_store::{EventRow, NodeRow, RunSummary};
use serde::Serialize;
use tauri::{AppHandle, Emitter, State};

use crate::state::AppState;

/// Los errores viajan a la UI como texto: es lo que se va a enseñar.
type Response<T> = std::result::Result<T, String>;

fn fail(err: impl std::fmt::Display) -> String {
    err.to_string()
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineInfo {
    pub name: String,
    pub path: String,
    pub description: Option<String>,
    pub nodes: usize,
    /// Descripción legible del disparador («cron `0 2 * * *` (UTC)»).
    pub trigger: String,
    pub scheduled: bool,
    /// Si no valida, el motivo. La UI lo enseña sin impedir lo demás.
    pub problem: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Workspace {
    pub directory: String,
    pub pipelines: Vec<PipelineInfo>,
    /// Problema del directorio entero (no existe, nombres duplicados…).
    pub problem: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunDetail {
    pub run_id: String,
    pub nodes: Vec<NodeRow>,
    pub events: Vec<EventRow>,
}

/// Pipelines del directorio actual.
#[tauri::command]
pub async fn workspace(state: State<'_, AppState>) -> Response<Workspace> {
    let directory = state.directory();
    let display = directory.display().to_string();

    let found = match orch_schedule::discover(&directory) {
        Ok(found) => found,
        Err(err) => {
            return Ok(Workspace {
                directory: display,
                pipelines: Vec::new(),
                problem: Some(err.to_string()),
            })
        }
    };

    let mut pipelines = Vec::with_capacity(found.entries.len() + found.broken.len());

    // Los ficheros que ni se pudieron leer también salen en la lista, con
    // su motivo: esconderlos haría parecer que no existen.
    for broken in found.broken {
        let name = broken
            .path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| broken.path.display().to_string());
        pipelines.push(PipelineInfo {
            name,
            path: broken.path.display().to_string(),
            description: None,
            nodes: 0,
            trigger: "no se pudo cargar".to_string(),
            scheduled: false,
            problem: Some(broken.error),
        });
    }

    for entry in found.entries {
        // Se valida cada uno por separado: que uno esté roto no puede
        // esconder los demás.
        let (nodes, description, problem) = match inspect(&state, &entry.path) {
            Ok((nodes, description)) => (nodes, description, None),
            Err(err) => (0, None, Some(err)),
        };
        pipelines.push(PipelineInfo {
            name: entry.name.clone(),
            path: entry.path.display().to_string(),
            description,
            nodes,
            trigger: entry.describe_trigger(),
            scheduled: entry.is_triggered(),
            problem,
        });
    }

    Ok(Workspace {
        directory: display,
        pipelines,
        problem: (!found.problems.is_empty()).then(|| found.problems.join("\n")),
    })
}

fn inspect(
    state: &AppState,
    path: &std::path::Path,
) -> std::result::Result<(usize, Option<String>), String> {
    let mut spec = PipelineSpec::from_path(path).map_err(fail)?;
    let description = spec.description.clone();
    orch_core::pushdown::apply(&mut spec, &state.registry);
    let dag = Dag::build(spec).map_err(fail)?;
    Ok((dag.len(), description))
}

/// Cambia el directorio que se está mirando.
#[tauri::command]
pub async fn set_directory(state: State<'_, AppState>, path: String) -> Response<Workspace> {
    state.set_directory(&PathBuf::from(path));
    workspace(state).await
}

#[tauri::command]
pub async fn recent_runs(state: State<'_, AppState>, limit: usize) -> Response<Vec<RunSummary>> {
    let store = Arc::clone(&state.store);
    // DuckDB es sincrónico: se consulta fuera del hilo de la UI.
    tokio::task::spawn_blocking(move || store.recent_runs(limit))
        .await
        .map_err(fail)?
        .map_err(fail)
}

#[tauri::command]
pub async fn run_detail(state: State<'_, AppState>, run_id: String) -> Response<RunDetail> {
    let store = Arc::clone(&state.store);
    tokio::task::spawn_blocking(move || {
        let run_id = store.resolve_run(&run_id)?;
        Ok::<_, orch_core::OrchError>(RunDetail {
            nodes: store.nodes_of(&run_id)?,
            events: store.events_of(&run_id, 500)?,
            run_id,
        })
    })
    .await
    .map_err(fail)?
    .map_err(fail)
}

/// Arranca un pipeline y devuelve al momento.
///
/// El identificador de la ejecución no se conoce hasta que el motor lo crea,
/// así que llega en el primer evento. La UI se engancha a `orch://event`.
#[tauri::command]
pub async fn start_run(app: AppHandle, state: State<'_, AppState>, path: String) -> Response<()> {
    let path = PathBuf::from(path);
    let registry = Arc::clone(&state.registry);
    let store = Arc::clone(&state.store);

    let mut spec = PipelineSpec::from_path(&path).map_err(fail)?;
    orch_core::pushdown::apply(&mut spec, &registry);
    let dag = Dag::build(spec).map_err(fail)?;

    tokio::spawn(async move {
        let executor = Executor::new(registry);

        // Dos oyentes del mismo canal: uno pinta y el otro guarda.
        let bridge = crate::events::bridge(app.clone(), executor.subscribe());
        let recorder = orch_store::EventWriter::spawn(Arc::clone(&store), executor.subscribe());

        let started_at = chrono::Utc::now();
        let outcome = executor.run(&dag).await;
        drop(executor);
        bridge.await.ok();
        recorder.await.ok();

        match outcome {
            Ok(report) => {
                if let Err(err) = store.record_run(&report, started_at) {
                    tracing::error!(error = %err, "no se pudo guardar la ejecución");
                }
                let _ = app.emit("orch://finished", &report);
            }
            Err(err) => {
                let _ = app.emit("orch://failed", err.to_string());
            }
        }
    });

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct Catalog {
    pub sources: Vec<String>,
    pub transforms: Vec<String>,
    pub sinks: Vec<String>,
}

#[tauri::command]
pub async fn catalog(state: State<'_, AppState>) -> Response<Catalog> {
    let owned = |names: Vec<&str>| names.into_iter().map(str::to_string).collect();
    Ok(Catalog {
        sources: owned(state.registry.source_names()),
        transforms: owned(state.registry.transform_names()),
        sinks: owned(state.registry.sink_names()),
    })
}
