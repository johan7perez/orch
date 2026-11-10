//! Varios procesos con el mismo historial: la aplicación abierta y un
//! `orch run`, el demonio y un `orch runs`.
//!
//! Tiene que ser con procesos de verdad: los bloqueos de fichero de DuckDB
//! no chocan dentro de un mismo proceso. El test se relanza a sí mismo como
//! segundo proceso.

use std::process::Command;
use std::time::{Duration, Instant};

use chrono::Utc;
use orch_core::executor::RunReport;
use orch_store::Store;
use tempfile::TempDir;

/// Con esto en el entorno, `otro_proceso` hace de segundo proceso.
const PAPEL: &str = "ORCH_STORE_TEST_OTRO_PROCESO";
/// Lo que tarda el escritor de eventos entre volcado y volcado.
const CADENCIA: Duration = Duration::from_millis(1000);
const VECES: usize = 4;

fn informe(run_id: &str) -> RunReport {
    RunReport {
        run_id: run_id.to_string(),
        pipeline: "compartido".to_string(),
        succeeded: true,
        elapsed_ms: 1,
        nodes: vec![],
    }
}

fn escribir(store: &Store, prefijo: &str) {
    for i in 0..VECES {
        store
            .record_run(&informe(&format!("{prefijo}-{i}")), Utc::now())
            .unwrap();
        std::thread::sleep(CADENCIA);
    }
}

/// El segundo proceso: no hace nada si no lo lanza el test de abajo.
#[test]
fn otro_proceso() {
    let Ok(ruta) = std::env::var(PAPEL) else {
        return;
    };
    let store = Store::open(&ruta).expect("el otro proceso abre el historial");
    escribir(&store, "otro");
}

#[test]
fn dos_procesos_escriben_a_la_vez_en_el_mismo_historial() {
    let dir = TempDir::new().unwrap();
    let ruta = dir.path().join("historial.duckdb");
    let store = Store::open(&ruta).unwrap();

    let mut hijo = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "otro_proceso", "--test-threads=1", "--nocapture"])
        .env(PAPEL, &ruta)
        .spawn()
        .expect("lanzar el segundo proceso");

    // Los dos escriben con la cadencia del escritor de eventos.
    escribir(&store, "este");
    let estado = hijo.wait().unwrap();
    assert!(
        estado.success(),
        "el segundo proceso no pudo usar el historial"
    );

    let runs = store.recent_runs(100).unwrap();
    let de = |p: &str| runs.iter().filter(|r| r.run_id.starts_with(p)).count();
    assert_eq!((de("este-"), de("otro-")), (VECES, VECES), "{runs:?}");
}

#[test]
fn un_proceso_ocioso_no_retiene_el_historial() {
    let dir = TempDir::new().unwrap();
    let ruta = dir.path().join("historial.duckdb");
    let store = Store::open(&ruta).unwrap();
    store.record_run(&informe("primero"), Utc::now()).unwrap();

    // Este proceso sigue vivo con el historial «abierto», como la aplicación
    // en segundo plano; el otro tiene que entrar sin esperar casi nada.
    std::thread::sleep(Duration::from_millis(500));
    let inicio = Instant::now();
    let estado = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "otro_proceso", "--test-threads=1", "--nocapture"])
        .env(PAPEL, &ruta)
        .status()
        .unwrap();
    assert!(estado.success());
    // Lo que tarda es lo que escribe (VECES × CADENCIA), no esperar turno.
    assert!(
        inicio.elapsed() < CADENCIA * VECES as u32 + Duration::from_secs(3),
        "{:?}",
        inicio.elapsed()
    );
    assert_eq!(store.recent_runs(100).unwrap().len(), 1 + VECES);
}
