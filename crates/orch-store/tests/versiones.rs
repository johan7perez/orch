//! Qué versión del pipeline corrió en cada ejecución.

use std::path::PathBuf;

use chrono::{Duration, Utc};
use orch_core::executor::RunReport;
use orch_store::Store;
use orch_version::{Commit, FileState, Snapshot};

fn report(run_id: &str, succeeded: bool) -> RunReport {
    RunReport {
        run_id: run_id.to_string(),
        pipeline: "mi-pipeline".to_string(),
        succeeded,
        elapsed_ms: 100,
        nodes: vec![],
    }
}

fn foto(texto: &str, git: Option<FileState>) -> Snapshot {
    Snapshot {
        path: PathBuf::from("/pipelines/p.yaml"),
        text: texto.to_string(),
        hash: orch_core::fingerprint(texto),
        git,
    }
}

fn commit(hash: &str, asunto: &str) -> Commit {
    Commit {
        hash: hash.to_string(),
        author: "Pruebas".to_string(),
        date: Utc::now(),
        subject: asunto.to_string(),
        path: "p.yaml".to_string(),
    }
}

fn guardar(store: &Store, run_id: &str, ok: bool, hace_min: i64, version: &Snapshot) {
    store
        .record_run(
            &report(run_id, ok),
            Utc::now() - Duration::minutes(hace_min),
        )
        .unwrap();
    store.record_run_version(run_id, version).unwrap();
}

#[test]
fn cada_ejecucion_dice_que_version_corrio() {
    let store = Store::in_memory().unwrap();
    let limpia = foto(
        "name: v1\n",
        Some(FileState {
            commit: Some(commit("a1b2c3d4e5f6", "Primera versión")),
            modified: false,
        }),
    );
    let con_cambios = foto(
        "name: v2\n",
        Some(FileState {
            commit: Some(commit("a1b2c3d4e5f6", "Primera versión")),
            modified: true,
        }),
    );
    let sin_git = foto("name: v3\n", None);

    guardar(&store, "run-1", true, 3, &limpia);
    guardar(&store, "run-2", true, 2, &con_cambios);
    guardar(&store, "run-3", true, 1, &sin_git);
    // Una ejecución de antes de que se guardaran versiones.
    store
        .record_run(&report("run-0", true), Utc::now() - Duration::minutes(4))
        .unwrap();

    let runs = store.recent_runs(10).unwrap();
    let etiquetas: Vec<Option<String>> = runs
        .iter()
        .map(|r| r.version.as_ref().map(|v| v.label.clone()))
        .collect();
    assert_eq!(
        etiquetas,
        [
            Some(format!("sin git (#{})", &sin_git.hash[..7])),
            Some("a1b2c3d con cambios".to_string()),
            Some("a1b2c3d".to_string()),
            None,
        ]
    );

    let version = store.run_version("run-1").unwrap().unwrap();
    assert_eq!(version.git_subject.as_deref(), Some("Primera versión"));
    assert_eq!(version.path, "/pipelines/p.yaml");
    // Con la huella se recupera el texto exacto que corrió.
    assert_eq!(
        store.version_text(&version.hash).unwrap().as_deref(),
        Some("name: v1\n")
    );
    assert!(store.run_version("run-0").unwrap().is_none());
}

#[test]
fn el_texto_se_guarda_una_vez_por_version() {
    let store = Store::in_memory().unwrap();
    let v1 = foto("name: v1\n", None);
    let v2 = foto("name: v2\n", None);
    guardar(&store, "run-1", true, 5, &v1);
    guardar(&store, "run-2", false, 4, &v1);
    guardar(&store, "run-3", true, 3, &v1);
    guardar(&store, "run-4", true, 2, &v2);

    let versiones = store.versions_of("/pipelines/p.yaml").unwrap();
    assert_eq!(versiones.len(), 2);
    // La más reciente primero, con cuántas veces corrió y cómo fue la última.
    assert_eq!(versiones[0].version.hash, v2.hash);
    assert_eq!(versiones[0].runs, 1);
    assert_eq!(versiones[1].version.hash, v1.hash);
    assert_eq!(versiones[1].runs, 3);
    assert_eq!(versiones[1].last_status, "succeeded");
    assert_eq!(versiones[1].last_run, "run-3");

    assert!(store.versions_of("/otro/sitio.yaml").unwrap().is_empty());
}

#[test]
fn podar_se_lleva_los_textos_que_nadie_usa() {
    let store = Store::in_memory().unwrap();
    let vieja = foto("name: vieja\n", None);
    let compartida = foto("name: compartida\n", None);
    guardar(&store, "run-viejo", true, 60 * 24 * 10, &vieja);
    guardar(&store, "run-viejo-2", true, 60 * 24 * 10, &compartida);
    guardar(&store, "run-nuevo", true, 1, &compartida);

    let podadas = store.prune(Utc::now() - Duration::days(1)).unwrap();
    assert_eq!(podadas, 2);

    assert!(store.run_version("run-viejo").unwrap().is_none());
    assert!(store.version_text(&vieja.hash).unwrap().is_none());
    // La que aún usa una ejecución viva se queda.
    assert!(store.version_text(&compartida.hash).unwrap().is_some());
}
