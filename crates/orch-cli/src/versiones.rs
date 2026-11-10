//! `orch diff` y `orch history`: versiones de un pipeline.
//!
//! La lógica (cargar una versión, atribuir ejecuciones a commits) está en
//! `orch_store::versions`, compartida con la aplicación; aquí sólo se
//! imprime.

use std::path::Path;

use orch_core::{OrchError, Result};
use orch_store::versions::{self, PipelineHistory, VersionRef};
use orch_store::{Store, VersionRuns};
use orch_version::diff::{line_diff, show_value};
use orch_version::{git, Change, Repo};
use serde_json::Value;

use crate::report::translate_status;

/// `orch diff`: qué cambió entre dos versiones.
pub fn diff(
    ruta: &Path,
    desde: Option<String>,
    hasta: Option<String>,
    store_path: &Path,
    json: bool,
) -> Result<()> {
    let desde = match desde {
        Some(texto) => VersionRef::parse(&texto),
        // Lo normal: qué hay sin guardar.
        None if Repo::of(ruta).is_some() => VersionRef::Commit { rev: "HEAD".into() },
        None => {
            return Err(OrchError::Other(format!(
                "`{}` no está en git: di desde qué versión comparar, por ejemplo \
                 `orch diff {} run:<id>`",
                ruta.display(),
                ruta.display()
            )))
        }
    };
    let hasta = hasta.map_or(VersionRef::Working, |t| VersionRef::parse(&t));

    let store = abrir_store(store_path)?;
    let resultado = versions::diff(ruta, &desde, &hasta, store.as_ref())?;

    if json {
        println!("{}", a_json(&resultado)?);
        return Ok(());
    }

    let nombre = versions::load(&VersionRef::Working, ruta, None, None)
        .map(|v| v.spec.name)
        .unwrap_or_else(|_| ruta.display().to_string());
    println!("{nombre}: de {} a {}", resultado.from, resultado.to);
    println!();
    if resultado.changes.is_empty() {
        println!("  sin cambios");
        return Ok(());
    }
    for cambio in &resultado.changes {
        imprimir_cambio(cambio);
    }
    Ok(())
}

fn imprimir_cambio(cambio: &Change) {
    match cambio {
        Change::Pipeline {
            field,
            before,
            after,
        } => imprimir_campo(field, before.as_ref(), after.as_ref()),
        Change::Node {
            node,
            field,
            before,
            after,
        } => imprimir_campo(&format!("{node}.{field}"), before.as_ref(), after.as_ref()),
        Change::NodeAdded {
            node,
            kind,
            component,
        } => println!("  + nodo {node} [{kind}:{component}]"),
        Change::NodeRemoved {
            node,
            kind,
            component,
        } => println!("  - nodo {node} [{kind}:{component}]"),
        Change::NodeRenamed { from, to } => println!("  ~ nodo {from} renombrado a {to}"),
        Change::EdgeAdded { from, to, port } => println!("  + arista {}", arista(from, to, port)),
        Change::EdgeRemoved { from, to, port } => {
            println!("  - arista {}", arista(from, to, port))
        }
        Change::Layout { nodes } => println!(
            "  · se movi{} en el lienzo: {}",
            if nodes.len() == 1 { "ó" } else { "eron" },
            nodes.join(", ")
        ),
    }
}

fn arista(from: &str, to: &str, port: &str) -> String {
    if port == from {
        format!("{from} → {to}")
    } else {
        format!("{from} → {to} (puerto {port})")
    }
}

fn imprimir_campo(campo: &str, antes: Option<&Value>, ahora: Option<&Value>) {
    match (antes, ahora) {
        (None, Some(v)) => println!("  + {campo}: {}", show_value(v)),
        (Some(v), None) => println!("  - {campo}: {}", show_value(v)),
        (Some(Value::String(a)), Some(Value::String(b)))
            if a.contains('\n') || b.contains('\n') =>
        {
            // Una query escrita en varias líneas: diff de líneas.
            println!("  ~ {campo}");
            for (marca, linea) in line_diff(a, b) {
                println!("      {marca} {linea}");
            }
        }
        (Some(a), Some(b)) => {
            let (a, b) = (show_value(a), show_value(b));
            if a.chars().count() + b.chars().count() > 70 {
                println!("  ~ {campo}");
                println!("      antes:   {a}");
                println!("      después: {b}");
            } else {
                println!("  ~ {campo}: {a} → {b}");
            }
        }
        (None, None) => {}
    }
}

/// `orch history`: los commits del fichero, con las ejecuciones de cada uno.
pub fn history(ruta: &Path, limit: usize, store_path: &Path, json: bool) -> Result<()> {
    let store = abrir_store(store_path)?;
    let historia = versions::history(ruta, limit, store.as_ref())?;

    if json {
        println!("{}", a_json(&historia)?);
        return Ok(());
    }

    let nombre = versions::load(&VersionRef::Working, ruta, None, None)
        .map(|v| v.spec.name)
        .unwrap_or_else(|_| ruta.display().to_string());
    println!("{nombre} · {}", ruta.display());
    println!();

    let Some(estado) = &historia.git else {
        historia_sin_git(&historia);
        return Ok(());
    };

    let autor = historia
        .commits
        .iter()
        .map(|c| c.commit.author.chars().count())
        .max()
        .unwrap_or(0);
    let mensaje = historia
        .commits
        .iter()
        .map(|c| c.commit.subject.chars().count())
        .max()
        .unwrap_or(0)
        .clamp(21, 48);

    if estado.modified {
        println!(
            "  {:<7}  {:<16}  {:<autor$}  {:<mensaje$}  {}",
            "*",
            "",
            "",
            if estado.commit.is_some() {
                "cambios sin guardar"
            } else {
                "nunca guardado en git"
            },
            ejecuciones(historia.current.as_ref()),
        );
    }
    for c in &historia.commits {
        println!(
            "  {:<7}  {:<16}  {:<autor$}  {:<mensaje$}  {}",
            c.commit.short(),
            c.commit.date.format("%Y-%m-%d %H:%M"),
            c.commit.author,
            recortar(&c.commit.subject, mensaje),
            ejecuciones(c.runs.as_ref()),
        );
    }
    if historia.commits.is_empty() && !estado.modified {
        println!("  sin commits");
    }

    if !historia.others.is_empty() {
        let total: usize = historia.others.iter().map(|v| v.runs).sum();
        println!();
        println!(
            "Además, {} con versiones que no están en esta lista. `orch diff {} run:<id>` las \
             compara.",
            plural(total),
            ruta.display()
        );
    }
    Ok(())
}

fn historia_sin_git(historia: &PipelineHistory) {
    println!("  no está en git. Versiones que han corrido, según el historial:");
    let todas: Vec<(&VersionRuns, bool)> = historia
        .current
        .iter()
        .map(|v| (v, true))
        .chain(historia.others.iter().map(|v| (v, false)))
        .collect();
    if todas.is_empty() {
        println!("  (ninguna todavía)");
        return;
    }
    println!();
    for (v, actual) in todas {
        println!(
            "  #{}  {:<11}  {}",
            git::short(&v.version.hash),
            if actual { "(la actual)" } else { "" },
            ejecuciones(Some(v))
        );
    }
}

fn ejecuciones(v: Option<&VersionRuns>) -> String {
    match v {
        None => "—".to_string(),
        Some(v) => format!(
            "{} · última {} {}",
            plural(v.runs),
            match v.last_status.as_str() {
                "succeeded" => "✓",
                "failed" => "✗",
                otro => translate_status(otro),
            },
            v.last_started.format("%Y-%m-%d %H:%M")
        ),
    }
}

fn plural(n: usize) -> String {
    if n == 1 {
        "1 ejecución".to_string()
    } else {
        format!("{n} ejecuciones")
    }
}

fn recortar(texto: &str, ancho: usize) -> String {
    if texto.chars().count() <= ancho {
        texto.to_string()
    } else {
        let corto: String = texto.chars().take(ancho.saturating_sub(1)).collect();
        format!("{corto}…")
    }
}

fn a_json<T: serde::Serialize>(valor: &T) -> Result<String> {
    serde_json::to_string_pretty(valor).map_err(|e| OrchError::Other(e.to_string()))
}

/// El historial, si existe. No se crea uno vacío sólo por mirar.
fn abrir_store(path: &Path) -> Result<Option<Store>> {
    if path.exists() {
        Store::open(path).map(Some)
    } else {
        Ok(None)
    }
}
