//! Persistencia de ejecuciones y eventos.

use std::sync::Arc;

use chrono::Utc;
use orch_core::executor::{NodeReport, NodeStatus, RunReport};
use orch_core::{IoStats, NodeProgress, RunEvent};
use orch_store::{EventWriter, Store};
use tempfile::TempDir;

fn node(id: &str, kind: &str, status: NodeStatus, rows_in: u64, rows_out: u64) -> NodeReport {
    NodeReport {
        id: id.to_string(),
        kind: kind.to_string(),
        component: "prueba".to_string(),
        status,
        attempts: 1,
        input: IoStats {
            rows: rows_in,
            batches: 1,
            bytes: rows_in * 8,
            stalled_ms: 5,
        },
        output: IoStats {
            rows: rows_out,
            batches: 1,
            bytes: rows_out * 8,
            stalled_ms: 15,
        },
        elapsed_ms: 100,
        error: None,
    }
}

fn report(run_id: &str, succeeded: bool) -> RunReport {
    RunReport {
        run_id: run_id.to_string(),
        pipeline: "mi-pipeline".to_string(),
        succeeded,
        elapsed_ms: 250,
        nodes: vec![
            node("origen", "source", NodeStatus::Succeeded, 0, 1000),
            node("destino", "sink", NodeStatus::Succeeded, 1000, 0),
        ],
    }
}

#[test]
fn guarda_y_recupera_una_ejecucion() {
    let store = Store::in_memory().expect("almacén");
    store
        .record_run(&report("run-1", true), Utc::now())
        .expect("guardar");

    let runs = store.recent_runs(10).expect("leer");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id, "run-1");
    assert_eq!(runs[0].pipeline, "mi-pipeline");
    assert_eq!(runs[0].status, "succeeded");
    assert_eq!(runs[0].elapsed_ms, 250);
    assert_eq!(runs[0].nodes, 2);
    assert_eq!(runs[0].failed_nodes, 0);
}

#[test]
fn cuenta_los_nodos_que_fallaron() {
    let store = Store::in_memory().expect("almacén");
    let mut informe = report("run-fallo", false);
    informe.nodes[1].status = NodeStatus::Failed;
    informe.nodes[1].error = Some("se rompió".to_string());
    store.record_run(&informe, Utc::now()).expect("guardar");

    let runs = store.recent_runs(10).expect("leer");
    assert_eq!(runs[0].status, "failed");
    assert_eq!(runs[0].failed_nodes, 1);

    let nodes = store.nodes_of("run-fallo").expect("nodos");
    assert_eq!(nodes[1].error.as_deref(), Some("se rompió"));
}

#[test]
fn calcula_throughput_y_ocupacion() {
    let store = Store::in_memory().expect("almacén");
    store
        .record_run(&report("run-metricas", true), Utc::now())
        .expect("guardar");

    let nodes = store.nodes_of("run-metricas").expect("nodos");
    assert_eq!(nodes.len(), 2);

    // 1000 filas en 100 ms = 10 000 filas/s.
    assert_eq!(nodes[0].node, "origen");
    assert!((nodes[0].rows_per_second - 10_000.0).abs() < 0.01);

    // 100 ms de vida, 5 esperando datos y 15 por contrapresión: 80 ms de
    // trabajo real, el 80%.
    assert_eq!(nodes[0].busy_ms, 80);
    assert!((nodes[0].busy_pct - 80.0).abs() < 0.01);

    // Para un sink lo que cuenta son las filas que consumió.
    assert_eq!(nodes[1].node, "destino");
    assert!((nodes[1].rows_per_second - 10_000.0).abs() < 0.01);
}

#[test]
fn los_nodos_conservan_el_orden_del_pipeline() {
    let store = Store::in_memory().expect("almacén");
    store
        .record_run(&report("run-orden", true), Utc::now())
        .expect("guardar");

    let nodes = store.nodes_of("run-orden").expect("nodos");
    let ids: Vec<&str> = nodes.iter().map(|n| n.node.as_str()).collect();
    assert_eq!(ids, vec!["origen", "destino"]);
}

#[test]
fn reescribir_una_ejecucion_no_la_duplica() {
    let store = Store::in_memory().expect("almacén");
    let at = Utc::now();
    store.record_run(&report("run-1", true), at).expect("una");
    store.record_run(&report("run-1", false), at).expect("dos");

    let runs = store.recent_runs(10).expect("leer");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, "failed", "debe quedar la última");
    assert_eq!(store.nodes_of("run-1").expect("nodos").len(), 2);
}

#[test]
fn resuelve_un_prefijo_de_identificador() {
    let store = Store::in_memory().expect("almacén");
    store
        .record_run(&report("abcdef123456", true), Utc::now())
        .expect("guardar");

    assert_eq!(store.resolve_run("abcd").expect("resolver"), "abcdef123456");

    let err = store.resolve_run("zzzz").expect_err("no existe");
    assert!(err.to_string().contains("zzzz"), "{err}");
}

#[test]
fn un_prefijo_ambiguo_se_rechaza() {
    let store = Store::in_memory().expect("almacén");
    store
        .record_run(&report("aa-uno", true), Utc::now())
        .expect("una");
    store
        .record_run(&report("aa-dos", true), Utc::now())
        .expect("dos");

    let err = store.resolve_run("aa").expect_err("ambiguo");
    assert!(err.to_string().contains("alarga"), "{err}");
}

#[test]
fn el_esquema_sobrevive_a_reabrir_el_fichero() {
    // Las migraciones se aplican una vez y no se repiten.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("historial.duckdb");

    {
        let store = Store::open(&path).expect("abrir");
        store
            .record_run(&report("run-persistente", true), Utc::now())
            .expect("guardar");
    }

    let store = Store::open(&path).expect("reabrir");
    let runs = store.recent_runs(10).expect("leer");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id, "run-persistente");
}

#[test]
fn se_crea_el_directorio_del_fichero() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("sub/carpeta/historial.duckdb");
    Store::open(&path).expect("abrir");
    assert!(path.exists());
}

#[tokio::test]
async fn el_escritor_guarda_los_eventos_del_canal() {
    let store = Arc::new(Store::in_memory().expect("almacén"));
    let (sender, receiver) = tokio::sync::broadcast::channel(64);
    let writer = EventWriter::spawn(Arc::clone(&store), receiver);

    sender
        .send(RunEvent::RunStarted {
            run_id: "run-eventos".to_string(),
            pipeline: "p".to_string(),
            nodes: 2,
        })
        .expect("enviar");
    sender
        .send(RunEvent::NodeFailed {
            run_id: "run-eventos".to_string(),
            node: "n".to_string(),
            attempt: 2,
            error: "algo pasó".to_string(),
            will_retry: true,
        })
        .expect("enviar");
    sender
        .send(RunEvent::RunFinished {
            run_id: "run-eventos".to_string(),
            pipeline: "p".to_string(),
            succeeded: false,
            elapsed_ms: 12,
        })
        .expect("enviar");

    // Al cerrarse el canal, el escritor vuelca lo pendiente y termina.
    drop(sender);
    writer.await.expect("el escritor termina");

    let events = store.events_of("run-eventos", 100).expect("leer");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, "run_started");
    assert_eq!(events[1].kind, "node_failed");
    assert_eq!(events[1].node.as_deref(), Some("n"));
    assert!(
        events[1]
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("algo pasó"),
        "{:?}",
        events[1].detail
    );
    assert_eq!(events[2].kind, "run_finished");
    // La secuencia conserva el orden en que ocurrieron.
    assert_eq!(
        events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn el_escritor_termina_solo_sin_eventos() {
    let store = Arc::new(Store::in_memory().expect("almacén"));
    let (sender, receiver) = tokio::sync::broadcast::channel(8);
    let writer = EventWriter::spawn(store, receiver);
    drop(sender);
    writer.await.expect("termina limpio");
}

fn progreso(run_id: &str, ms: u64, filas: u64) -> RunEvent {
    let lado = |rows| IoStats {
        rows,
        batches: rows / 100,
        bytes: rows * 8,
        stalled_ms: 0,
    };
    RunEvent::Progress {
        run_id: run_id.to_string(),
        elapsed_ms: ms,
        nodes: vec![
            NodeProgress {
                node: "origen".to_string(),
                input: lado(0),
                output: lado(filas),
            },
            NodeProgress {
                node: "destino".to_string(),
                input: lado(filas),
                output: lado(0),
            },
        ],
    }
}

#[tokio::test]
async fn el_progreso_se_guarda_a_un_segundo_y_no_ensucia_el_registro() {
    let store = Arc::new(Store::in_memory().expect("almacén"));
    let (sender, receiver) = tokio::sync::broadcast::channel(64);
    let writer = EventWriter::spawn(Arc::clone(&store), receiver);

    // Diez muestras cada 250 ms, como las publica el motor: 2,5 segundos.
    for i in 1..=10u64 {
        sender
            .send(progreso("run-progreso", i * 250, i * 1_000))
            .expect("enviar");
    }
    drop(sender);
    writer.await.expect("el escritor termina");

    let muestras = store.progress_of("run-progreso").expect("leer");
    let tiempos: Vec<u64> = muestras
        .iter()
        .filter(|m| m.node == "destino")
        .map(|m| m.at_ms)
        .collect();
    // La primera de cada segundo: 250 (segundo 0), 1 000 y 2 000.
    assert_eq!(tiempos, [250, 1_000, 2_000]);
    let destino = muestras
        .iter()
        .find(|m| m.node == "destino" && m.at_ms == 2_000);
    assert_eq!(destino.map(|m| m.rows_in), Some(8_000));

    // En el registro de eventos no hay ni una.
    assert!(store
        .events_of("run-progreso", 100)
        .expect("leer")
        .is_empty());
}

#[test]
fn podar_se_lleva_tambien_el_progreso() {
    let store = Store::in_memory().expect("almacén");
    let viejo = Utc::now() - chrono::Duration::days(10);
    store
        .record_run(&report("run-viejo", true), viejo)
        .expect("guardar");
    store
        .append_progress(&[orch_store::ProgressRow {
            run_id: "run-viejo".into(),
            node: "origen".into(),
            at_ms: 1_000,
            rows_in: 0,
            rows_out: 10,
            bytes_in: 0,
            bytes_out: 80,
            stalled_in_ms: 0,
            stalled_out_ms: 0,
        }])
        .expect("guardar progreso");
    store
        .prune(Utc::now() - chrono::Duration::days(1))
        .expect("podar");
    assert!(store.progress_of("run-viejo").expect("leer").is_empty());
}
