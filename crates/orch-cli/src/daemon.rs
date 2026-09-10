//! Proceso residente que dispara los pipelines cuando toca.
//!
//! Es una capa fina sobre [`orch_schedule::Scheduler`]: le pasa el reloj,
//! arranca lo que diga y le avisa cuando algo termina. Toda la lógica de
//! concurrencia y encadenamientos vive allí, donde se puede probar sin
//! esperas.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use orch_core::{Executor, OrchError, Registry, Result, RunReport};
use orch_schedule::{discover, Launch, Reason, Scheduler};
use orch_store::{EventWriter, Store};

/// Cada cuánto se le pregunta al planificador.
///
/// Un segundo basta: el cron tiene resolución de minuto y así el apagado
/// responde rápido.
const TICK: Duration = Duration::from_secs(1);

pub struct DaemonOptions {
    pub dir: PathBuf,
    pub store: PathBuf,
    /// Días de historial a conservar. `None` = todo.
    pub keep_days: Option<i64>,
}

pub async fn run(registry: Arc<Registry>, options: DaemonOptions) -> Result<()> {
    let entries = discover(&options.dir)?;
    if entries.is_empty() {
        return Err(OrchError::Other(format!(
            "no hay ningún pipeline en `{}`",
            options.dir.display()
        )));
    }

    // Se validan todos al arrancar. Un demonio que descubre a las 3 de la
    // mañana que un pipeline no compila no sirve de nada.
    let mut paths: HashMap<String, PathBuf> = HashMap::new();
    for entry in &entries {
        let (dag, _) = crate::load(&entry.path, &registry)?;
        Executor::new(Arc::clone(&registry)).prepare(&dag).await?;
        paths.insert(entry.name.clone(), entry.path.clone());
    }

    println!(
        "vigilando {} pipeline(s) en {}",
        entries.len(),
        options.dir.display()
    );
    let width = entries.iter().map(|e| e.name.len()).max().unwrap_or(8);
    for entry in &entries {
        println!("  {:<width$}  {}", entry.name, entry.describe_trigger());
    }
    let watched = entries.iter().filter(|e| e.is_triggered()).count();
    if watched == 0 {
        println!("  (ninguno tiene disparadores: el demonio no hará nada)");
    }
    println!("Ctrl-C para parar");

    let store = Arc::new(Store::open(&options.store)?);
    prune(&store, options.keep_days);

    let mut scheduler = Scheduler::new(entries, Utc::now());
    // Las ejecuciones en vuelo, para poder esperarlas al apagar.
    let mut running: tokio::task::JoinSet<(String, bool)> = tokio::task::JoinSet::new();
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut stopping = false;

    loop {
        tokio::select! {
            _ = ticker.tick(), if !stopping => {
                for launch in scheduler.due(Utc::now()) {
                    spawn(&mut running, &registry, &store, &paths, launch);
                }
            }

            Some(joined) = running.join_next() => {
                match joined {
                    Ok((name, succeeded)) => {
                        for launch in scheduler.finished(&name, succeeded) {
                            if !stopping {
                                spawn(&mut running, &registry, &store, &paths, launch);
                            }
                        }
                    }
                    Err(err) => tracing::error!(error = %err, "una ejecución terminó de forma anómala"),
                }
            }

            _ = tokio::signal::ctrl_c(), if !stopping => {
                stopping = true;
                if running.is_empty() {
                    break;
                }
                println!("\nparando: se esperan {} ejecución(es) en curso", running.len());
            }
        }

        if stopping && running.is_empty() {
            break;
        }
    }

    println!("parado");
    Ok(())
}

fn spawn(
    running: &mut tokio::task::JoinSet<(String, bool)>,
    registry: &Arc<Registry>,
    store: &Arc<Store>,
    paths: &HashMap<String, PathBuf>,
    launch: Launch,
) {
    let Some(path) = paths.get(&launch.name).cloned() else {
        tracing::error!(pipeline = %launch.name, "no se encontró el fichero");
        return;
    };
    let registry = Arc::clone(registry);
    let store = Arc::clone(store);

    println!(
        "{}  ▶ {} ({})",
        Utc::now().format("%H:%M:%S"),
        launch.name,
        motivo(launch.reason)
    );

    running.spawn(async move {
        let succeeded = match execute(&registry, &store, &path).await {
            Ok(report) => {
                println!(
                    "{}  {} {} en {} ms",
                    Utc::now().format("%H:%M:%S"),
                    if report.succeeded { "✓" } else { "✗" },
                    launch.name,
                    report.elapsed_ms
                );
                report.succeeded
            }
            Err(err) => {
                // Un pipeline que no arranca no puede tumbar el demonio.
                eprintln!(
                    "{}  ✗ {} no pudo arrancar: {err}",
                    Utc::now().format("%H:%M:%S"),
                    launch.name
                );
                false
            }
        };
        (launch.name, succeeded)
    });
}

async fn execute(registry: &Arc<Registry>, store: &Arc<Store>, path: &Path) -> Result<RunReport> {
    let (dag, _) = crate::load(path, registry)?;
    let executor = Executor::new(Arc::clone(registry));
    let recorder = EventWriter::spawn(Arc::clone(store), executor.subscribe());

    let started_at = Utc::now();
    let result = executor.run(&dag).await;
    drop(executor);
    recorder.await.ok();

    let report = result?;
    if let Err(err) = store.record_run(&report, started_at) {
        tracing::error!(error = %err, "no se pudo guardar la ejecución");
    }
    Ok(report)
}

fn prune(store: &Store, keep_days: Option<i64>) {
    let Some(days) = keep_days else {
        return;
    };
    let cutoff = Utc::now() - chrono::Duration::days(days);
    match store.prune(cutoff) {
        Ok(0) => {}
        Ok(removed) => println!("podadas {removed} ejecución(es) de más de {days} día(s)"),
        Err(err) => tracing::error!(error = %err, "no se pudo podar el historial"),
    }
}

fn motivo(reason: Reason) -> &'static str {
    match reason {
        Reason::Cron => "cron",
        Reason::Upstream => "encadenado",
        Reason::Queued => "estaba en cola",
    }
}
