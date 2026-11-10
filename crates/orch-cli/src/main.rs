//! CLI de Orch (Fase 0).

mod daemon;
mod report;
mod versiones;
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

    /// Carpeta de plugins WASM. Por defecto, la subcarpeta `plugins/` de la
    /// carpeta del pipeline (o de `--dir`, con el demonio).
    #[arg(long, global = true, env = "ORCH_PLUGINS")]
    plugins: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Comprueba que el pipeline es un DAG válido y que sus conectores existen.
    Validate { pipeline: PathBuf },
    /// Muestra la estructura del pipeline sin ejecutarlo.
    Graph { pipeline: PathBuf },
    /// De qué columnas de origen sale cada columna de salida.
    ///
    /// Sin `--node`, enseña lo que escribe cada sink. Con un origen, a qué
    /// salidas llega cada una de sus columnas y dónde se usa: lo que se
    /// rompe si cambia. No lee datos, pero los orígenes miran su esquema.
    Lineage {
        pipeline: PathBuf,
        /// Nodo del que enseñar las columnas.
        #[arg(long)]
        node: Option<String>,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
    },
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
        /// Fichero del clúster: reparte los nodos con `worker:` entre las
        /// máquinas que nombra.
        #[arg(long, env = "ORCH_CLUSTER")]
        cluster: Option<PathBuf>,
    },
    /// Ejecuta las partes de pipelines que le mande un coordinador
    /// (`orch run --cluster`) y sirve sus aristas.
    ///
    /// Sólo acepta a quien presente un certificado de la CA del clúster: un
    /// worker ejecuta lo que le manden, y eso no se deja abierto.
    Worker {
        /// Dónde escuchar, `host:puerto`.
        #[arg(long)]
        listen: String,
        /// La CA del clúster.
        #[arg(long)]
        ca: PathBuf,
        /// El certificado de esta máquina, firmado por esa CA.
        #[arg(long)]
        cert: PathBuf,
        #[arg(long)]
        key: PathBuf,
        /// Contra qué carpeta se resuelven las rutas relativas de lo que
        /// llegue. Sus plugins, en `plugins/` dentro de ella.
        #[arg(long, default_value = ".")]
        dir: PathBuf,
    },
    /// Qué cambió entre dos versiones del pipeline, nodo a nodo.
    ///
    /// Una versión es un commit (`HEAD`, `a1b2c3`, `main~2`) o lo que corrió
    /// en una ejecución (`run:<id>`). Sin versiones, compara la última
    /// guardada en git con el fichero tal y como está.
    Diff {
        pipeline: PathBuf,
        /// Versión de partida. Por defecto, `HEAD`.
        desde: Option<String>,
        /// Versión de llegada. Por defecto, el fichero en disco.
        hasta: Option<String>,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
    },
    /// Versiones del pipeline en git, con las ejecuciones de cada una.
    History {
        pipeline: PathBuf,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
    },
    /// Lista los conectores y transformaciones disponibles.
    ///
    /// Con un nombre, enseña la config que acepta ese componente.
    Connectors {
        /// Componente del que enseñar la configuración (`csv`, `sql`…).
        component: Option<String>,
    },
    /// Últimas ejecuciones guardadas.
    Runs {
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, value_enum, default_value_t = Format::Text)]
        format: Format,
    },
    /// Vigila un directorio de pipelines y los dispara cuando toca.
    Daemon {
        /// Directorio con los ficheros de pipeline.
        #[arg(long, default_value = "pipelines")]
        dir: PathBuf,
        /// Días de historial a conservar. Sin esto, no se poda nada.
        #[arg(long)]
        keep_days: Option<i64>,
    },
    /// Borra del historial las ejecuciones más viejas.
    Prune {
        #[arg(long, default_value_t = 30)]
        keep_days: i64,
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

    let plugins = cli
        .plugins
        .clone()
        .or_else(|| carpeta_base(&cli.command).map(|base| base.join("plugins")));
    match runtime.block_on(dispatch(cli.command, &cli.store, plugins.as_deref())) {
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

/// Todo lo que el binario sabe ejecutar, más los plugins de la carpeta.
///
/// Un plugin que no carga no impide nada: se avisa y se sigue. Si un
/// pipeline lo usa, fallará al validarse con el motivo de siempre.
fn full_registry(plugins: Option<&std::path::Path>) -> std::sync::Arc<orch_core::Registry> {
    std::sync::Arc::new(base_registry(plugins))
}

fn base_registry(plugins: Option<&std::path::Path>) -> orch_core::Registry {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    orch_rest::register(&mut registry);
    orch_postgres::register(&mut registry);
    orch_flight::register(&mut registry);
    if let Some(carpeta) = plugins {
        for aviso in orch_wasm::registrar_carpeta(&mut registry, carpeta) {
            eprintln!("aviso: {aviso}");
        }
    }
    registry
}

/// Lo que ejecuta `orch run`: el motor de siempre, o el coordinador de un
/// clúster. Para quien escucha sus eventos son iguales.
enum Motor {
    Local {
        ejecutor: Executor,
        dag: Dag,
    },
    Cluster {
        coordinador: orch_flight::cluster::Coordinador,
        spec: PipelineSpec,
        base: PathBuf,
    },
}

impl Motor {
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<orch_core::RunEvent> {
        match self {
            Motor::Local { ejecutor, .. } => ejecutor.subscribe(),
            Motor::Cluster { coordinador, .. } => coordinador.subscribe(),
        }
    }

    fn spec(&self) -> &PipelineSpec {
        match self {
            Motor::Local { dag, .. } => dag.spec(),
            Motor::Cluster { spec, .. } => spec,
        }
    }

    async fn run(&self) -> orch_core::Result<orch_core::RunReport> {
        match self {
            Motor::Local { ejecutor, dag } => ejecutor.run(dag).await,
            Motor::Cluster {
                coordinador,
                spec,
                base,
            } => coordinador.run(spec, base).await,
        }
    }
}

/// La carpeta a la que pertenece lo que pide el comando: ahí se buscan sus
/// plugins. Los comandos que sólo leen el historial no los necesitan.
fn carpeta_base(command: &Command) -> Option<PathBuf> {
    let carpeta_de = |fichero: &std::path::Path| {
        fichero
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    };
    match command {
        Command::Validate { pipeline } | Command::Graph { pipeline } => Some(carpeta_de(pipeline)),
        Command::Lineage { pipeline, .. } => Some(carpeta_de(pipeline)),
        Command::Run { pipeline, .. } => Some(carpeta_de(pipeline)),
        Command::Worker { dir, .. } => Some(dir.clone()),
        Command::Daemon { dir, .. } => Some(dir.clone()),
        Command::Connectors { .. } => Some(PathBuf::from(".")),
        Command::Runs { .. }
        | Command::Prune { .. }
        | Command::Logs { .. }
        | Command::Diff { .. }
        | Command::History { .. } => None,
    }
}

async fn dispatch(
    command: Command,
    store_path: &std::path::Path,
    plugins: Option<&std::path::Path>,
) -> orch_core::Result<ExitCode> {
    let registry = full_registry(plugins);

    match command {
        Command::Daemon { dir, keep_days } => {
            daemon::run(
                registry,
                daemon::DaemonOptions {
                    dir,
                    store: store_path.to_path_buf(),
                    keep_days,
                },
            )
            .await?;
            Ok(ExitCode::SUCCESS)
        }

        Command::Prune { keep_days } => {
            let store = orch_store::Store::open(store_path)?;
            let cutoff = chrono::Utc::now() - chrono::Duration::days(keep_days);
            let removed = store.prune(cutoff)?;
            println!(
                "podadas {removed} ejecución(es) anteriores a {}",
                cutoff.format("%Y-%m-%d")
            );
            Ok(ExitCode::SUCCESS)
        }

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
            let version = store.run_version(&run_id)?;
            match format {
                Format::Text => report::print_logs(&run_id, version.as_ref(), &nodes, &events),
                Format::Json => println!(
                    "{}",
                    to_json(&serde_json::json!({
                        "run_id": run_id,
                        "version": version,
                        "nodes": nodes,
                        "events": events,
                    }))?
                ),
            }
            Ok(ExitCode::SUCCESS)
        }

        Command::Connectors { component } => match component {
            None => {
                report::print_registry(&registry);
                Ok(ExitCode::SUCCESS)
            }
            Some(name) => {
                if report::print_component(&registry, &name) {
                    Ok(ExitCode::SUCCESS)
                } else {
                    Err(orch_core::OrchError::Other(format!(
                        "no hay ningún componente `{name}`. `orch connectors` los lista todos"
                    )))
                }
            }
        },

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

        Command::Lineage {
            pipeline,
            node,
            format,
        } => {
            // Sin pushdown: el lineage habla de los nodos tal y como están
            // escritos, no de los que quedan tras absorber un `select`.
            let dag = Dag::build(PipelineSpec::from_path(&pipeline)?)?;
            if let Some(id) = &node {
                if dag.index_of(id).is_none() {
                    let ids: Vec<&str> = dag.nodes().iter().map(|n| n.id.as_str()).collect();
                    return Err(orch_core::OrchError::Other(format!(
                        "el pipeline no tiene ningún nodo `{id}` (tiene: {})",
                        ids.join(", ")
                    )));
                }
            }
            let lineage = Executor::new(registry).lineage(&dag).await?;
            match format {
                Format::Text => report::print_lineage(&dag, &lineage, node.as_deref()),
                Format::Json => {
                    let elegidos: Vec<&orch_core::NodeLineage> = lineage
                        .iter()
                        .filter(|l| node.as_ref().is_none_or(|id| &l.node == id))
                        .collect();
                    println!("{}", to_json(&elegidos)?);
                }
            }
            Ok(ExitCode::SUCCESS)
        }

        Command::Diff {
            pipeline,
            desde,
            hasta,
            format,
        } => {
            versiones::diff(&pipeline, desde, hasta, store_path, format == Format::Json)?;
            Ok(ExitCode::SUCCESS)
        }

        Command::History {
            pipeline,
            limit,
            format,
        } => {
            versiones::history(&pipeline, limit, store_path, format == Format::Json)?;
            Ok(ExitCode::SUCCESS)
        }

        Command::Graph { pipeline } => {
            let (dag, pushed) = load(&pipeline, &registry)?;
            report::print_graph(&dag);
            report::print_pushdown(&pushed);
            Ok(ExitCode::SUCCESS)
        }

        Command::Worker {
            listen,
            ca,
            cert,
            key,
            dir,
        } => {
            let tls = orch_flight::cluster::ClusterTls { ca, cert, key };
            let escucha = tokio::net::TcpListener::bind(&listen)
                .await
                .map_err(|err| {
                    orch_core::OrchError::Other(format!("no se pudo escuchar en `{listen}`: {err}"))
                })?;
            let registry = std::sync::Arc::try_unwrap(registry)
                .map_err(|_| orch_core::OrchError::Other("registro compartido".into()))?;
            println!(
                "worker escuchando en {} (rutas desde `{}`); Ctrl+C para parar",
                escucha
                    .local_addr()
                    .map(|a| a.to_string())
                    .unwrap_or(listen),
                dir.display()
            );
            orch_flight::cluster::servir_worker(escucha, &tls, registry, dir, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
            Ok(ExitCode::SUCCESS)
        }

        Command::Run {
            pipeline,
            format,
            follow,
            no_store,
            cluster,
        } => {
            // Se lee una vez: lo que corre y lo que se guarda como «la versión
            // que corrió» son el mismo texto.
            let version = orch_version::Snapshot::take(&pipeline)?;
            let executor = match &cluster {
                None => {
                    let (dag, _) = load_spec(version.spec()?, &registry)?;
                    Motor::Local {
                        ejecutor: Executor::new(registry),
                        dag,
                    }
                }
                // Repartido: el pipeline va tal y como está escrito, porque
                // cada máquina resuelve sus secretos y sus rutas.
                Some(fichero) => {
                    let config = orch_flight::cluster::ClusterConfig::from_path(fichero)?;
                    let spec = PipelineSpec::from_yaml_str_as_written(
                        &pipeline.display().to_string(),
                        &version.text,
                    )?;
                    let registry = std::sync::Arc::try_unwrap(registry)
                        .map_err(|_| orch_core::OrchError::Other("registro compartido".into()))?;
                    Motor::Cluster {
                        coordinador: orch_flight::cluster::Coordinador::new(registry, config)?,
                        spec,
                        base: version
                            .path
                            .parent()
                            .map(std::path::Path::to_path_buf)
                            .unwrap_or_else(|| PathBuf::from(".")),
                    }
                }
            };

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
            // Las alertas son otro oyente: avisan aquí, en la terminal, y van
            // al historial y al webhook si el pipeline lo pide.
            let vigilancia = orch_alert::Vigilancia::para(executor.spec(), store.as_deref());
            let entrega =
                orch_alert::Entrega::nueva(store.clone(), vigilancia.config.webhook.clone());
            let alertas = orch_alert::vigilar(
                vigilancia,
                executor.subscribe(),
                entrega,
                |alerta, problemas| {
                    eprintln!("⚠ {}", alerta.mensaje);
                    for problema in problemas {
                        eprintln!("  aviso: {problema}");
                    }
                },
            );

            let started_at = chrono::Utc::now();
            let result = executor.run().await;
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
            alertas.await.ok();

            let report = result?;
            if let Some(store) = &store {
                // Que falle el historial no puede tumbar una ejecución que
                // ya movió los datos: se avisa y se sigue.
                if let Err(err) = store.record_run(&report, started_at) {
                    eprintln!("aviso: no se pudo guardar la ejecución: {err}");
                } else if let Err(err) = store.record_run_version(&report.run_id, &version) {
                    eprintln!("aviso: no se pudo guardar qué versión corrió: {err}");
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
pub(crate) fn load(
    path: &std::path::Path,
    registry: &orch_core::Registry,
) -> orch_core::Result<(Dag, Vec<orch_core::Pushed>)> {
    load_spec(PipelineSpec::from_path(path)?, registry)
}

/// Como [`load`], con el pipeline ya leído.
pub(crate) fn load_spec(
    mut spec: PipelineSpec,
    registry: &orch_core::Registry,
) -> orch_core::Result<(Dag, Vec<orch_core::Pushed>)> {
    let pushed = orch_core::pushdown::apply(&mut spec, registry);
    Ok((Dag::build(spec)?, pushed))
}
