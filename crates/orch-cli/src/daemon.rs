//! Proceso residente que dispara los pipelines cuando toca.
//!
//! Es una capa fina sobre [`orch_schedule::Scheduler`]: le pasa el reloj,
//! arranca lo que diga y le avisa cuando algo termina. Toda la lógica de
//! concurrencia y encadenamientos vive allí, donde se puede probar sin
//! esperas.
//!
//! Vigila además su carpeta: un pipeline nuevo, cambiado o borrado se nota
//! en un par de segundos, sin reiniciar. Lo que esté corriendo termina con la
//! versión con la que arrancó.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use orch_core::{Executor, PipelineSpec, Registry, Result, RunReport};
use orch_flight::cluster::{Coordinador, Motor};
use orch_schedule::{discover, Launch, Reason, Scheduler};
use orch_store::{EventWriter, Store};

/// Cada cuánto se le pregunta al planificador.
///
/// Un segundo basta: el cron tiene resolución de minuto y así el apagado
/// responde rápido.
const TICK: Duration = Duration::from_secs(1);

/// Cada cuánto se mira si cambió algo en la carpeta.
///
/// Se compara el contenido de cada pipeline, no la fecha del fichero: tocarlo
/// sin cambiarlo no es un cambio. Leer unos YAML pequeños cada dos segundos
/// no cuesta nada, y no depende de que el sistema avise de los cambios (que
/// en una carpeta de red, por ejemplo, no siempre lo hace).
const RELECTURA: Duration = Duration::from_secs(2);

pub struct DaemonOptions {
    pub dir: PathBuf,
    pub store: PathBuf,
    /// Días de historial a conservar. `None` = todo.
    pub keep_days: Option<i64>,
    /// Con él, también los pipelines que reparten nodos entre máquinas.
    pub coordinador: Option<Arc<Coordinador>>,
}

/// Con qué se ejecuta: lo que hay en esta máquina y, si hay clúster, su
/// coordinador. Uno para todas las ejecuciones: cada una lleva sus propios
/// eventos, así que las alertas de una no se mezclan con las de otra.
#[derive(Clone)]
struct Ejecucion {
    registry: Arc<Registry>,
    coordinador: Option<Arc<Coordinador>>,
}

pub async fn run(registry: Arc<Registry>, options: DaemonOptions) -> Result<()> {
    let con = Ejecucion {
        registry,
        coordinador: options.coordinador.clone(),
    };
    let mut vistas = huellas(&options.dir);
    let found = discover(&options.dir)?;
    // Un fichero roto se reporta pero no impide arrancar con los demás: si
    // uno de veinte tiene un secreto sin definir, los otros diecinueve
    // tienen que seguir corriendo a su hora.
    for broken in &found.broken {
        eprintln!(
            "aviso: `{}` no se pudo cargar: {}",
            broken.path.display(),
            broken.error
        );
    }
    for problem in &found.problems {
        eprintln!("aviso: {problem}");
    }

    // Se validan todos al arrancar. Un demonio que descubre a las 3 de la
    // mañana que un pipeline no compila no sirve de nada.
    let mut paths: HashMap<String, PathBuf> = HashMap::new();
    let mut entries = Vec::with_capacity(found.entries.len());
    for entry in found.entries {
        match validate(&con, &entry.path).await {
            Ok(()) => {
                paths.insert(entry.name.clone(), entry.path.clone());
                entries.push(entry);
            }
            Err(err) => eprintln!("aviso: `{}` no es válido: {err}", entry.name),
        }
    }

    // `discover` ya avisa de un `after` que apunta a un nombre inexistente,
    // pero un pipeline puede haberse caído aquí, en la validación, después de
    // aquella comprobación. Un encadenamiento que cuelga no rompe nada: deja
    // de dispararse, y en silencio es peor.
    for entry in &entries {
        for esperado in &entry.after {
            if !paths.contains_key(esperado) {
                eprintln!(
                    "aviso: `{}` espera a `{esperado}`, que no está disponible: nunca se disparará por encadenamiento",
                    entry.name
                );
            }
        }
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
    if entries.is_empty() {
        println!("  (por ahora ninguno utilizable: se vigila la carpeta)");
    } else if !entries.iter().any(|e| e.is_triggered()) {
        println!("  (ninguno tiene disparadores: por ahora el demonio no hará nada)");
    }
    println!("Los cambios en la carpeta se recogen solos. Ctrl-C para parar");

    let store = Arc::new(Store::open(&options.store)?);
    prune(&store, options.keep_days);

    let mut scheduler = Scheduler::new(entries, Utc::now());
    // Las ejecuciones en vuelo, para poder esperarlas al apagar.
    let mut running: tokio::task::JoinSet<(String, bool)> = tokio::task::JoinSet::new();
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut relectura = tokio::time::interval(RELECTURA);
    relectura.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut stopping = false;

    loop {
        tokio::select! {
            _ = ticker.tick(), if !stopping => {
                for launch in scheduler.due(Utc::now()) {
                    spawn(&mut running, &con, &store, &paths, launch);
                }
            }

            Some(joined) = running.join_next() => {
                match joined {
                    Ok((name, succeeded)) => {
                        for launch in scheduler.finished(&name, succeeded) {
                            if !stopping {
                                spawn(&mut running, &con, &store, &paths, launch);
                            }
                        }
                    }
                    Err(err) => tracing::error!(error = %err, "una ejecución terminó de forma anómala"),
                }
            }

            _ = relectura.tick(), if !stopping => {
                let ahora = huellas(&options.dir);
                if ahora != vistas {
                    recargar(&con, &options.dir, &vistas, &ahora, &mut paths, &mut scheduler)
                        .await;
                    vistas = ahora;
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

/// Cómo enseña el demonio una alerta: con hora, como el resto de su salida.
fn imprimir_alerta(alerta: &orch_alert::Alerta, problemas: &[String]) {
    eprintln!("{}  ⚠ {}", Utc::now().format("%H:%M:%S"), alerta.mensaje);
    for problema in problemas {
        eprintln!("          aviso: {problema}");
    }
}

fn spawn(
    running: &mut tokio::task::JoinSet<(String, bool)>,
    con: &Ejecucion,
    store: &Arc<Store>,
    paths: &HashMap<String, PathBuf>,
    launch: Launch,
) {
    let Some(path) = paths.get(&launch.name).cloned() else {
        tracing::error!(pipeline = %launch.name, "no se encontró el fichero");
        return;
    };
    let con = con.clone();
    let store = Arc::clone(store);

    println!(
        "{}  ▶ {} ({})",
        Utc::now().format("%H:%M:%S"),
        launch.name,
        motivo(launch.reason)
    );

    running.spawn(async move {
        let succeeded = match execute(&con, &store, &path).await {
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
                orch_alert::avisar_arranque_fallido(
                    &path,
                    &launch.name,
                    &err.to_string(),
                    Some(Arc::clone(&store)),
                    imprimir_alerta,
                )
                .await;
                false
            }
        };
        (launch.name, succeeded)
    });
}

async fn execute(con: &Ejecucion, store: &Arc<Store>, path: &Path) -> Result<RunReport> {
    // Mirar el estado en git lanza un par de procesos: fuera del runtime.
    let ruta = path.to_path_buf();
    let version =
        tokio::task::spawn_blocking(move || orch_version::Snapshot::take(&ruta)).await??;
    let motor = Motor::para(
        path,
        &version.text,
        Arc::clone(&con.registry),
        con.coordinador.clone(),
    )?;
    let recorder = EventWriter::spawn(Arc::clone(store), motor.subscribe());
    let vigilancia = orch_alert::Vigilancia::para(motor.spec(), Some(store));
    let entrega =
        orch_alert::Entrega::nueva(Some(Arc::clone(store)), vigilancia.config.webhook.clone());
    let alertas = orch_alert::vigilar(vigilancia, motor.subscribe(), entrega, imprimir_alerta);

    let started_at = Utc::now();
    let result = motor.run().await;
    // Soltarlo cierra el canal de eventos de esta ejecución.
    drop(motor);
    recorder.await.ok();
    alertas.await.ok();

    let report = result?;
    if let Err(err) = store.record_run(&report, started_at) {
        tracing::error!(error = %err, "no se pudo guardar la ejecución");
    } else if let Err(err) = store.record_run_version(&report.run_id, &version) {
        tracing::error!(error = %err, "no se pudo guardar qué versión corrió");
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

/// El contenido de cada pipeline de la carpeta, por su huella.
fn huellas(dir: &Path) -> BTreeMap<PathBuf, String> {
    let Ok(entradas) = std::fs::read_dir(dir) else {
        return BTreeMap::new();
    };
    entradas
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension().and_then(|e| e.to_str()).is_some_and(|e| {
                    e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml")
                })
        })
        .filter_map(|p| {
            let texto = std::fs::read_to_string(&p).ok()?;
            Some((p, orch_core::fingerprint(&texto)))
        })
        .collect()
}

/// Vuelve a leer la carpeta y le cuenta al planificador lo que hay ahora.
///
/// Sólo se valida lo que cambió: lo demás ya se validó. Un pipeline que se
/// rompe (a medio editar, un secreto que falta) deja de dispararse con un
/// aviso, y vuelve en cuanto se arregla.
async fn recargar(
    con: &Ejecucion,
    dir: &Path,
    antes: &BTreeMap<PathBuf, String>,
    ahora: &BTreeMap<PathBuf, String>,
    paths: &mut HashMap<String, PathBuf>,
    scheduler: &mut Scheduler,
) {
    let hora = || Utc::now().format("%H:%M:%S");
    let tocados: BTreeSet<&PathBuf> = ahora
        .iter()
        .filter(|(p, h)| antes.get(*p) != Some(*h))
        .map(|(p, _)| p)
        .collect();

    let found = match discover(dir) {
        Ok(found) => found,
        Err(err) => {
            eprintln!("{}  aviso: no se pudo releer la carpeta: {err}", hora());
            return;
        }
    };
    for roto in found.broken.iter().filter(|b| tocados.contains(&b.path)) {
        eprintln!(
            "{}  ✗ `{}` no se puede cargar: {}",
            hora(),
            nombre_de(&roto.path),
            roto.error
        );
    }
    for problema in &found.problems {
        eprintln!("{}  aviso: {problema}", hora());
    }

    let mut entries = Vec::with_capacity(found.entries.len());
    let mut nuevos: HashMap<String, PathBuf> = HashMap::new();
    let mut actualizados = Vec::new();
    for entry in found.entries {
        let valido = if tocados.contains(&entry.path) {
            match validate(con, &entry.path).await {
                Ok(()) => {
                    actualizados.push(entry.name.clone());
                    true
                }
                Err(err) => {
                    eprintln!("{}  ✗ `{}` no es válido: {err}", hora(), entry.name);
                    false
                }
            }
        } else {
            // Sin cambios: vale lo que valía.
            paths.get(&entry.name) == Some(&entry.path)
        };
        if valido {
            nuevos.insert(entry.name.clone(), entry.path.clone());
            entries.push(entry);
        }
    }

    let cambios = scheduler.replace(entries, Utc::now());
    *paths = nuevos;
    let disparador = |nombre: &str| {
        scheduler
            .entries()
            .find(|e| e.name == nombre)
            .map(|e| e.describe_trigger())
            .unwrap_or_default()
    };
    for nombre in &cambios.added {
        println!("{}  + `{nombre}`: {}", hora(), disparador(nombre));
    }
    for nombre in &cambios.changed {
        println!("{}  ~ `{nombre}`: ahora {}", hora(), disparador(nombre));
    }
    for nombre in &cambios.removed {
        println!("{}  - `{nombre}` ya no se vigila", hora());
    }
    for nombre in actualizados
        .iter()
        .filter(|n| !cambios.added.contains(n) && !cambios.changed.contains(n))
    {
        println!(
            "{}  · `{nombre}` actualizado: la próxima ejecución usa la versión nueva",
            hora()
        );
    }
}

fn nombre_de(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

async fn validate(con: &Ejecucion, path: &Path) -> Result<()> {
    let texto = std::fs::read_to_string(path).map_err(|source| orch_core::OrchError::SpecIo {
        path: path.display().to_string(),
        source,
    })?;
    let escrito = PipelineSpec::from_yaml_str_as_written(&path.display().to_string(), &texto)?;
    if orch_flight::cluster::reparte(&escrito) {
        // Lo de aquí se comprueba aquí; lo de cada worker, allí, al ejecutar.
        let coordinador = con
            .coordinador
            .as_ref()
            .ok_or_else(|| orch_flight::cluster::sin_cluster(&escrito))?;
        let base = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        return coordinador.validar(&escrito, base).await;
    }
    let (dag, _) = crate::load(path, &con.registry)?;
    Executor::new(Arc::clone(&con.registry))
        .prepare(&dag)
        .await?;
    Ok(())
}

fn motivo(reason: Reason) -> &'static str {
    match reason {
        Reason::Cron => "cron",
        Reason::Upstream => "encadenado",
        Reason::Queued => "estaba en cola",
    }
}
