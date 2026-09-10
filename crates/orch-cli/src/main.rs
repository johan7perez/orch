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
    },
    /// Lista los conectores y transformaciones disponibles.
    Connectors,
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

    match runtime.block_on(dispatch(cli.command)) {
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

/// Todo lo que el binario sabe ejecutar: conectores nativos + SQL.
fn full_registry() -> std::sync::Arc<orch_core::Registry> {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    std::sync::Arc::new(registry)
}

async fn dispatch(command: Command) -> orch_core::Result<ExitCode> {
    let registry = full_registry();

    match command {
        Command::Connectors => {
            report::print_registry(&registry);
            Ok(ExitCode::SUCCESS)
        }

        Command::Validate { pipeline } => {
            let dag = load(&pipeline)?;
            Executor::new(registry).prepare(&dag)?;
            println!(
                "✓ `{}` es válido: {} nodo(s), {} arista(s)",
                dag.spec().name,
                dag.len(),
                dag.spec().edges.len()
            );
            Ok(ExitCode::SUCCESS)
        }

        Command::Graph { pipeline } => {
            let dag = load(&pipeline)?;
            report::print_graph(&dag);
            Ok(ExitCode::SUCCESS)
        }

        Command::Run {
            pipeline,
            format,
            follow,
        } => {
            let dag = load(&pipeline)?;
            let executor = Executor::new(registry);
            // Suscribirse ANTES de arrancar: el canal es broadcast y los
            // eventos anteriores a la suscripción no se recuperan.
            let watcher = follow.then(|| watch::spawn(executor.subscribe()));

            let result = executor.run(&dag).await;
            // Soltar el ejecutor cierra el canal de eventos: sin esto, un
            // fallo previo a `RunFinished` dejaría al visor esperando.
            drop(executor);

            if let Some(handle) = watcher {
                handle.await.ok();
            }

            let report = result?;
            match format {
                Format::Text => report::print_run(&report),
                Format::Json => println!(
                    "{}",
                    serde_json::to_string_pretty(&report)
                        .map_err(|e| orch_core::OrchError::Other(e.to_string()))?
                ),
            }

            Ok(if report.succeeded {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
    }
}

fn load(path: &PathBuf) -> orch_core::Result<Dag> {
    Dag::build(PipelineSpec::from_path(path)?)
}
