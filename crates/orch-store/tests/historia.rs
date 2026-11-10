//! El historial de un pipeline juntando git y las ejecuciones guardadas, y
//! las versiones que se pueden pedir: commits, la copia de trabajo y lo que
//! corrió en una ejecución.

use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::{Duration, Utc};
use orch_core::executor::RunReport;
use orch_store::versions::{self, VersionRef};
use orch_store::Store;
use orch_version::{Change, Repo, Snapshot};
use tempfile::TempDir;

const V1: &str = "name: demo\nnodes:\n  - { id: a, type: source, connector: generator, config: { rows: 1 } }\n  - { id: b, type: sink, connector: \"null\" }\nedges:\n  - { from: a, to: b }\n";

fn version(filas: u32) -> String {
    V1.replace("rows: 1", &format!("rows: {filas}"))
}

fn git(dir: &Path, args: &[&str]) {
    let salida = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        salida.status.success(),
        "{}",
        String::from_utf8_lossy(&salida.stderr)
    );
}

/// Un repositorio temporal con identidad propia, sin firma ni hooks.
fn repo() -> TempDir {
    let dir = TempDir::new().unwrap();
    let p = dir.path();
    git(p, &["init", "--quiet"]);
    git(p, &["config", "user.name", "Pruebas"]);
    git(p, &["config", "user.email", "pruebas@example.invalid"]);
    git(p, &["config", "commit.gpgsign", "false"]);
    git(
        p,
        &[
            "config",
            "core.hooksPath",
            &p.join(".sin-hooks").to_string_lossy(),
        ],
    );
    dir
}

/// Ejecuta «de mentira»: guarda una ejecución con la versión que hay ahora.
fn ejecutar(store: &Store, ruta: &Path, run_id: &str, hace_min: i64) {
    let foto = Snapshot::take(ruta).unwrap();
    let informe = RunReport {
        run_id: run_id.to_string(),
        pipeline: "demo".to_string(),
        succeeded: true,
        elapsed_ms: 10,
        nodes: vec![],
    };
    store
        .record_run(&informe, Utc::now() - Duration::minutes(hace_min))
        .unwrap();
    store.record_run_version(run_id, &foto).unwrap();
}

fn escribir(ruta: &PathBuf, texto: &str) {
    std::fs::write(ruta, texto).unwrap();
}

#[test]
fn cada_ejecucion_cuenta_para_el_commit_cuyo_texto_corrio() {
    let dir = repo();
    let ruta = dir.path().join("p.yaml");
    let store = Store::in_memory().unwrap();

    escribir(&ruta, &version(1));
    let repo = Repo::of(&ruta).unwrap();
    let primero = repo.commit(&ruta, "Una fila").unwrap();
    ejecutar(&store, &ruta, "run-a", 6);

    // Se prueba un cambio sin guardarlo, se guarda tal cual y se vuelve a
    // ejecutar: el mismo texto, con git en dos estados distintos.
    escribir(&ruta, &version(2));
    ejecutar(&store, &ruta, "run-b", 5);
    let segundo = repo.commit(&ruta, "Dos filas").unwrap();
    ejecutar(&store, &ruta, "run-b2", 4);

    // Un experimento que nunca se guardó.
    escribir(&ruta, &version(3));
    ejecutar(&store, &ruta, "run-c", 3);

    // Y lo que hay ahora, también ejecutado.
    escribir(&ruta, &version(4));
    ejecutar(&store, &ruta, "run-d", 2);

    let historia = versions::history(&ruta, 10, Some(&store)).unwrap();
    assert!(historia.git.as_ref().unwrap().modified);
    assert_eq!(historia.hash, orch_core::fingerprint(&version(4)));
    assert_eq!(historia.current.as_ref().unwrap().last_run, "run-d");

    let commits: Vec<(&str, Option<&str>)> = historia
        .commits
        .iter()
        .map(|c| {
            (
                c.commit.hash.as_str(),
                c.runs.as_ref().map(|r| r.last_run.as_str()),
            )
        })
        .collect();
    // `run-b` corrió con cambios sin guardar, pero son los que se guardaron
    // en el segundo commit: cuenta para él, junto con `run-b2`.
    assert_eq!(
        commits,
        [
            (segundo.hash.as_str(), Some("run-b2")),
            (primero.hash.as_str(), Some("run-a")),
        ]
    );
    assert_eq!(historia.commits[0].runs.as_ref().unwrap().runs, 2);
    let sueltas: Vec<&str> = historia
        .others
        .iter()
        .map(|v| v.last_run.as_str())
        .collect();
    assert_eq!(sueltas, ["run-c"]);
}

#[test]
fn las_versiones_se_piden_por_commit_por_ejecucion_o_la_de_ahora() {
    let dir = repo();
    let ruta = dir.path().join("p.yaml");
    let store = Store::in_memory().unwrap();

    escribir(&ruta, &version(1));
    let repo = Repo::of(&ruta).unwrap();
    repo.commit(&ruta, "Una fila").unwrap();
    escribir(&ruta, &version(2));
    ejecutar(&store, &ruta, "run-con-cambios", 1);
    escribir(&ruta, &version(3));

    let por_commit = versions::load(
        &VersionRef::Commit { rev: "HEAD".into() },
        &ruta,
        Some(&repo),
        Some(&store),
    )
    .unwrap();
    assert_eq!(por_commit.text, version(1));
    assert!(
        por_commit.label.ends_with("«Una fila»"),
        "{}",
        por_commit.label
    );

    let por_ejecucion = versions::load(
        &VersionRef::parse("run:run-con"),
        &ruta,
        Some(&repo),
        Some(&store),
    )
    .unwrap();
    assert_eq!(por_ejecucion.text, version(2));
    assert!(
        por_ejecucion.label.contains("con cambios"),
        "{}",
        por_ejecucion.label
    );

    let ahora = versions::load(&VersionRef::Working, &ruta, None, None).unwrap();
    assert_eq!(ahora.text, version(3));

    // Qué ha cambiado desde lo que corrió.
    let diff = versions::diff(
        &ruta,
        &VersionRef::Run {
            run_id: "run-con-cambios".into(),
        },
        &VersionRef::Working,
        Some(&store),
    )
    .unwrap();
    assert_eq!(diff.to, "la copia de trabajo");
    assert!(
        matches!(diff.changes.as_slice(), [Change::Node { node, field, .. }] if node == "a" && field == "config.rows"),
        "{:?}",
        diff.changes
    );
}

#[test]
fn no_se_compara_con_la_ejecucion_de_otro_pipeline() {
    let dir = TempDir::new().unwrap();
    let uno = dir.path().join("uno.yaml");
    let otro = dir.path().join("otro.yaml");
    escribir(&uno, &version(1));
    escribir(&otro, &version(1));
    let store = Store::in_memory().unwrap();
    ejecutar(&store, &uno, "run-de-uno", 1);

    let error = versions::load(
        &VersionRef::Run {
            run_id: "run-de-uno".into(),
        },
        &otro,
        None,
        Some(&store),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("otro pipeline"), "{error}");

    // Sin git, un commit no es una versión que se pueda pedir.
    let error = versions::load(&VersionRef::parse("HEAD"), &uno, None, Some(&store))
        .unwrap_err()
        .to_string();
    assert!(error.contains("no está en git"), "{error}");
}
