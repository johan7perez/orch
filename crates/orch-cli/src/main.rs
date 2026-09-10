//! CLI de Orch (Fase 0).

mod report;
mod watch;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use orch_core::{Dag, Executor, PipelineSpec};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "orch",
    about = "Orquestación y transporte de datos de alto rendimiento",
    version
)]
struct Cli {
    /// Nivel de log: error, warn, info, debug, trace. La variable
    /// `ORCH_LOG` tiene prioridad y admite filtros por módulo.
    #[arg(long, global = true, default_value = "warn")]
    log: String,

    /// Fichero DuckDB donde se guarda el historial de ejecuciones.
    #[arg(long, global = true, env = "ORCH_STORE", default_value = "orch.duckdb")]
    store: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Comprueba que el pipeline es un DAG válido y que sus conectores existen.
    Validate { pipeline: PathBuf },
    /// Muestra la estructura del pipeline sin ejecutarlo.
    Graph { pipeline: PathBuf },
    /// Ejecuta el pipeline.
    Run {
        pipeline: PathBuf,
        /// Formato del informe final.
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
        /// Imprime cada evento de ejecución según ocurre.
        #[arg(long)]
        follow: bool,
        /// No guarda nada en el historial.
        #[arg(long)]
        no_store: bool,
    },
    /// Lista los conectores y transformaciones disponibles.
    Connectors,
    /// Últimas ejecuciones guardadas.
    Runs {
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
    },
    /// Detalle de una ejecución: métricas por nodo y eventos.
    ///
    /// Basta con las primeras letras del identificador.
    Logs {
        run_id: String,
        /// Eventos a mostrar como mucho.
        #[arg(long, default_value_t = 200)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum Format {
    Text,
    Json,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("error: no se pudo iniciar el runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(dispatch(cli.command, &cli.store)) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing(level: &str) {
    let filter = EnvFilter::try_from_env("ORCH_LOG")
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

/// Todo lo que el binario sabe ejecutar.
fn full_registry() -> std::sync::Arc<orch_core::Registry> {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    orch_rest::register(&mut registry);
    orch_postgres::register(&mut registry);
    std::sync::Arc::new(registry)
}

async fn dispatch(command: Command, store_path: &std::path::Path) -> orch_core::Result<ExitCode> {
    let registry = full_registry();

    match command {
        Command::Runs { limit, format } => {
            let store = orch_store::Store::open(store_path)?;
            let runs = store.recent_runs(limit)?;
            match format {
                Format::Text => report::print_runs(&runs),
                Format::Json => println!("{}", to_json(&runs)?),
            }
            Ok(ExitCode::SUCCESS)
        }

        Command::Logs {
            run_id,
            limit,
            format,
        } => {
            let store = orch_store::Store::open(store_path)?;
            let run_id = store.resolve_run(&run_id)?;
            let nodes = store.nodes_of(&run_id)?;
            let events = store.events_of(&run_id, limit)?;
            match format {
                Format::Text => report::print_logs(&run_id, &nodes, &events),
                Format::Json => println!(
                    "{}",
                    to_json(&serde_json::json!({
                        "run_id": run_id,
                        "nodes": nodes,
                        "events": events,
                    }))?
                ),
            }
            Ok(ExitCode::SUCCESS)
        }

        Command::Connectors => {
            report::print_registry(&registry);
            Ok(ExitCode::SUCCESS)
        }

        Command::Validate { pipeline } => {
            let (dag, pushed) = load(&pipeline, &registry)?;
            Executor::new(registry).prepare(&dag).await?;
            println!(
                "✓ `{}` es válido: {} nodo(s), {} arista(s)",
                dag.spec().name,
                dag.len(),
                dag.spec().edges.len()
            );
            report::print_pushdown(&pushed);
            Ok(ExitCode::SUCCESS)
        }

        Command::Graph { pipeline } => {
            let (dag, pushed) = load(&pipeline, &registry)?;
            report::print_graph(&dag);
            report::print_pushdown(&pushed);
            Ok(ExitCode::SUCCESS)
        }

        Command::Run {
            pipeline,
            format,
            follow,
            no_store,
        } => {
            let (dag, _) = load(&pipeline, &registry)?;
            let executor = Executor::new(registry);

            // Suscribirse ANTES de arrancar: el canal es broadcast y los
            // eventos anteriores a la suscripción no se recuperan.
            let watcher = follow.then(|| watch::spawn(executor.subscribe()));
            let store = if no_store {
                None
            } else {
                Some(std::sync::Arc::new(orch_store::Store::open(store_path)?))
            };
            let recorder = store
                .as_ref()
                .map(|store| orch_store::EventWriter::spawn(store.clone(), executor.subscribe()));

            let started_at = chrono::Utc::now();
            let result = executor.run(&dag).await;
            // Soltar el ejecutor cierra el canal de eventos: sin esto, un
            // fallo previo a `RunFinished` dejaría esperando a los oyentes.
            drop(executor);

            if let Some(handle) = watcher {
                handle.await.ok();
            }
            // El escritor termina al cerrarse el canal, y vuelca lo que
            // quede pendiente antes de salir.
            if let Some(handle) = recorder {
                handle.await.ok();
            }

            let report = result?;
            if let Some(store) = &store {
                // Que falle el historial no puede tumbar una ejecución que
                // ya movió los datos: se avisa y se sigue.
                if let Err(err) = store.record_run(&report, started_at) {
                    eprintln!("aviso: no se pudo guardar la ejecución: {err}");
                }
            }

            match format {
                Format::Text => report::print_run(&report),
                Format::Json => println!("{}", to_json(&report)?),
            }

            Ok(if report.succeeded {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
    }
}

fn to_json<T: serde::Serialize>(value: &T) -> orch_core::Result<String> {
    serde_json::to_string_pretty(value).map_err(|e| orch_core::OrchError::Other(e.to_string()))
}

/// Carga el pipeline y empuja hacia los orígenes lo que acepten.
///
/// La reescritura va antes de construir el DAG, así que lo que se valida y
/// lo que se ejecuta es siempre el pipeline ya optimizado.
fn load(
    path: &PathBuf,
    registry: &orch_core::Registry,
) -> orch_core::Result<(Dag, Vec<orch_core::Pushed>)> {
    let mut spec = PipelineSpec::from_path(path)?;
    let pushed = orch_core::pushdown::apply(&mut spec, registry);
    Ok((Dag::build(spec)?, pushed))
}
