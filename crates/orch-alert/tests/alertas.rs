//! Las alertas: cuándo se dan, cuándo no, y a dónde llegan.
//!
//! El detector se prueba con el reloj de Tokio simulado: diez minutos de
//! «colgada» pasan en milisegundos y el resultado no depende de la máquina.

use std::sync::Arc;
use std::time::Duration;

use orch_alert::{Alerta, Entrega, Tipo, Vigilancia};
use orch_core::{AlertsSpec, IoStats, NodeProgress, Plazo, RunEvent, RunReport};
use orch_store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;

const RUN: &str = "run-1";

fn vigilancia(config: AlertsSpec, tipica: Option<Duration>) -> Vigilancia {
    Vigilancia {
        pipeline: "nocturno".into(),
        config,
        tipica,
    }
}

fn empieza() -> RunEvent {
    RunEvent::RunStarted {
        run_id: RUN.into(),
        pipeline: "nocturno".into(),
        nodes: 2,
    }
}

fn avanza(filas: u64) -> RunEvent {
    let lado = |rows| IoStats {
        rows,
        batches: 1,
        bytes: rows * 8,
        stalled_ms: 0,
    };
    RunEvent::Progress {
        run_id: RUN.into(),
        elapsed_ms: 0,
        nodes: vec![NodeProgress {
            node: "leer".into(),
            input: lado(0),
            output: lado(filas),
        }],
    }
}

fn termina(bien: bool) -> RunEvent {
    RunEvent::RunFinished {
        run_id: RUN.into(),
        pipeline: "nocturno".into(),
        succeeded: bien,
        elapsed_ms: 0,
    }
}

/// Manda los eventos con sus esperas (en tiempo simulado) y recoge las
/// alertas que salen.
async fn alertas(v: Vigilancia, guion: Vec<(Duration, RunEvent)>) -> Vec<Alerta> {
    let (emisor, receptor) = broadcast::channel(64);
    let (tarea, mut salida) = v.detectar(receptor);
    for (espera, evento) in guion {
        tokio::time::sleep(espera).await;
        emisor.send(evento).unwrap();
    }
    drop(emisor);
    tarea.await.unwrap();
    let mut todas = Vec::new();
    while let Ok(alerta) = salida.try_recv() {
        todas.push(alerta);
    }
    todas
}

fn segundos(s: u64) -> Duration {
    Duration::from_secs(s)
}

#[tokio::test(start_paused = true)]
async fn un_fallo_avisa_con_el_nodo_y_el_error() {
    let fallo = RunEvent::NodeFailed {
        run_id: RUN.into(),
        node: "cargar".into(),
        attempt: 1,
        error: "la tabla no existe".into(),
        will_retry: false,
    };
    let salida = alertas(
        vigilancia(AlertsSpec::default(), None),
        vec![
            (segundos(0), empieza()),
            (segundos(2), fallo),
            (segundos(0), termina(false)),
        ],
    )
    .await;
    assert_eq!(salida.len(), 1, "{salida:?}");
    assert_eq!(salida[0].tipo, Tipo::Fallo);
    assert_eq!(salida[0].nodo.as_deref(), Some("cargar"));
    assert_eq!(
        salida[0].mensaje,
        "`nocturno` falló en el nodo `cargar`: la tabla no existe"
    );
}

#[tokio::test(start_paused = true)]
async fn un_fallo_que_se_reintenta_no_es_el_que_se_cuenta() {
    let reintento = RunEvent::NodeFailed {
        run_id: RUN.into(),
        node: "leer".into(),
        attempt: 1,
        error: "tiempo de espera".into(),
        will_retry: true,
    };
    let salida = alertas(
        vigilancia(AlertsSpec::default(), None),
        vec![
            (segundos(0), empieza()),
            (segundos(1), reintento),
            (segundos(1), termina(true)),
        ],
    )
    .await;
    assert!(salida.is_empty(), "{salida:?}");
}

#[tokio::test(start_paused = true)]
async fn sin_pedirlo_no_avisa_de_un_fallo() {
    let config = AlertsSpec {
        on_failure: false,
        ..AlertsSpec::default()
    };
    let salida = alertas(
        vigilancia(config, None),
        vec![(segundos(0), empieza()), (segundos(1), termina(false))],
    )
    .await;
    assert!(salida.is_empty(), "{salida:?}");
}

#[tokio::test(start_paused = true)]
async fn una_ejecucion_parada_avisa_una_vez_de_que_esta_colgada() {
    let config = AlertsSpec {
        stalled_after: Some(Plazo(segundos(5))),
        ..AlertsSpec::default()
    };
    // Se mueve durante tres segundos y luego se para medio minuto.
    let salida = alertas(
        vigilancia(config, None),
        vec![
            (segundos(0), empieza()),
            (segundos(1), avanza(100)),
            (segundos(2), avanza(200)),
            (segundos(30), termina(true)),
        ],
    )
    .await;
    assert_eq!(salida.len(), 1, "{salida:?}");
    assert_eq!(salida[0].tipo, Tipo::Colgada);
    assert_eq!(salida[0].mensaje, "`nocturno` lleva 5s sin mover una fila");
}

#[tokio::test(start_paused = true)]
async fn moverse_a_tiempo_no_es_estar_colgada() {
    let config = AlertsSpec {
        stalled_after: Some(Plazo(segundos(5))),
        ..AlertsSpec::default()
    };
    let mut guion = vec![(segundos(0), empieza())];
    for i in 1..=10 {
        guion.push((segundos(3), avanza(i * 100)));
    }
    guion.push((segundos(1), termina(true)));
    let salida = alertas(vigilancia(config, None), guion).await;
    assert!(salida.is_empty(), "{salida:?}");
}

#[tokio::test(start_paused = true)]
async fn avisa_en_cuanto_pasa_del_doble_de_lo_normal_sin_esperar_al_final() {
    let config = AlertsSpec {
        slower_than: Some(2.0),
        ..AlertsSpec::default()
    };
    let mut guion = vec![(segundos(0), empieza())];
    for i in 1..=40 {
        guion.push((segundos(1), avanza(i)));
    }
    guion.push((segundos(0), termina(true)));
    let salida = alertas(vigilancia(config, Some(segundos(10))), guion).await;
    assert_eq!(salida.len(), 1, "{salida:?}");
    assert_eq!(salida[0].tipo, Tipo::Lenta);
    assert_eq!(
        salida[0].mensaje,
        "`nocturno` lleva 21s, más de 2× lo que suele tardar (10s)"
    );
}

#[tokio::test(start_paused = true)]
async fn sin_historia_no_hay_lento() {
    let config = AlertsSpec {
        slower_than: Some(2.0),
        ..AlertsSpec::default()
    };
    let mut guion = vec![(segundos(0), empieza())];
    for i in 1..=60 {
        guion.push((segundos(1), avanza(i)));
    }
    guion.push((segundos(0), termina(true)));
    let salida = alertas(vigilancia(config, None), guion).await;
    assert!(salida.is_empty(), "{salida:?}");
}

// --- Lo que suele tardar -------------------------------------------------------

fn ejecucion(store: &Store, id: &str, ms: u64, bien: bool) {
    let informe = RunReport {
        run_id: id.into(),
        pipeline: "nocturno".into(),
        succeeded: bien,
        elapsed_ms: ms,
        nodes: Vec::new(),
    };
    store.record_run(&informe, chrono::Utc::now()).unwrap();
}

#[test]
fn lo_normal_es_la_mediana_de_las_ultimas_correctas() {
    let store = Store::in_memory().unwrap();
    let spec = |alerts: Option<AlertsSpec>| {
        let mut spec = orch_core::PipelineSpec::from_yaml_str(
            "t.yaml",
            "name: nocturno\nnodes: [{ id: a, type: source, connector: x }]\n",
        )
        .unwrap();
        spec.alerts = alerts;
        spec
    };
    let lenta = Some(AlertsSpec {
        slower_than: Some(2.0),
        ..AlertsSpec::default()
    });

    ejecucion(&store, "a", 1_000, true);
    ejecucion(&store, "b", 2_000, true);
    // Dos no bastan para saber qué es lo normal.
    assert_eq!(
        Vigilancia::para(&spec(lenta.clone()), Some(&store)).tipica,
        None
    );

    // Una rarísima no mueve la mediana, y las fallidas no cuentan.
    ejecucion(&store, "c", 90_000, true);
    ejecucion(&store, "d", 500, false);
    assert_eq!(
        Vigilancia::para(&spec(lenta), Some(&store)).tipica,
        Some(Duration::from_millis(2_000))
    );

    // Si no se pide `slower_than`, ni se pregunta.
    assert_eq!(Vigilancia::para(&spec(None), Some(&store)).tipica, None);
}

// --- La entrega ------------------------------------------------------------------

fn una_alerta() -> Alerta {
    Alerta {
        tipo: Tipo::Fallo,
        pipeline: "nocturno".into(),
        run_id: RUN.into(),
        nodo: Some("cargar".into()),
        mensaje: "`nocturno` falló en el nodo `cargar`: la tabla no existe".into(),
    }
}

/// Un receptor de webhooks de una sola petición: devuelve su URL y el cuerpo
/// que le llegue.
async fn receptor() -> (String, tokio::sync::oneshot::Receiver<serde_json::Value>) {
    let escucha = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/alertas", escucha.local_addr().unwrap());
    let (dar, recibido) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut conexion, _) = escucha.accept().await.unwrap();
        let mut leido = Vec::new();
        let mut trozo = [0u8; 4096];
        loop {
            let n = conexion.read(&mut trozo).await.unwrap();
            leido.extend_from_slice(&trozo[..n]);
            let texto = String::from_utf8_lossy(&leido);
            if let Some(fin) = texto.find("\r\n\r\n") {
                let largo = texto[..fin]
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if leido.len() >= fin + 4 + largo {
                    let cuerpo = serde_json::from_slice(&leido[fin + 4..fin + 4 + largo]).unwrap();
                    conexion
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                        .await
                        .unwrap();
                    let _ = dar.send(cuerpo);
                    return;
                }
            }
            if n == 0 {
                return;
            }
        }
    });
    (url, recibido)
}

#[tokio::test]
async fn la_alerta_llega_al_webhook_y_al_historial() {
    let store = Arc::new(Store::in_memory().unwrap());
    let (url, recibido) = receptor().await;
    let entrega = Entrega::nueva(Some(Arc::clone(&store)), Some(url));

    let problemas = entrega.entregar(&una_alerta()).await;
    assert!(problemas.is_empty(), "{problemas:?}");

    let cuerpo = recibido.await.unwrap();
    // `text` es lo que leen Slack y Teams.
    assert_eq!(
        cuerpo["text"],
        "⚠ `nocturno` falló en el nodo `cargar`: la tabla no existe"
    );
    assert_eq!(cuerpo["tipo"], "fallo");
    assert_eq!(cuerpo["nodo"], "cargar");

    let guardadas = store.alerts_of(RUN).unwrap();
    assert_eq!(guardadas.len(), 1);
    assert_eq!(guardadas[0].kind, "fallo");
}

#[tokio::test]
async fn un_webhook_caido_se_cuenta_pero_no_impide_guardarla() {
    let store = Arc::new(Store::in_memory().unwrap());
    // Un puerto en el que no escucha nadie.
    let libre = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", libre.local_addr().unwrap());
    drop(libre);
    let entrega = Entrega::nueva(Some(Arc::clone(&store)), Some(url));

    let problemas = entrega.entregar(&una_alerta()).await;
    assert_eq!(problemas.len(), 1, "{problemas:?}");
    assert!(problemas[0].contains("webhook"), "{problemas:?}");
    assert_eq!(store.alerts_of(RUN).unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn el_mensaje_no_repite_el_nodo_que_ya_dice_el_error() {
    let fallo = RunEvent::NodeFailed {
        run_id: RUN.into(),
        node: "leer".into(),
        attempt: 1,
        error: "el nodo `leer` falló: no se pudo abrir `x.csv`".into(),
        will_retry: false,
    };
    let salida = alertas(
        vigilancia(AlertsSpec::default(), None),
        vec![
            (segundos(0), empieza()),
            (segundos(0), fallo),
            (segundos(0), termina(false)),
        ],
    )
    .await;
    assert_eq!(
        salida[0].mensaje,
        "`nocturno` falló en el nodo `leer`: no se pudo abrir `x.csv`"
    );
}

#[tokio::test]
async fn un_pipeline_que_no_arranca_tambien_avisa() {
    let dir = std::env::temp_dir().join(format!("orch-alerta-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (url, recibido) = receptor().await;
    let ruta = dir.join("roto.yaml");
    // Un nodo sin `type`: no carga, pero su bloque `alerts` sí se lee.
    std::fs::write(
        &ruta,
        format!("name: roto\nalerts:\n  webhook: {url}\nnodes:\n  - id: a\n"),
    )
    .unwrap();

    let mut vistas = Vec::new();
    orch_alert::avisar_arranque_fallido(&ruta, "roto", "falta `type`", None, |a, _| {
        vistas.push(a.clone())
    })
    .await;

    assert_eq!(vistas.len(), 1);
    assert_eq!(vistas[0].mensaje, "`roto` no pudo arrancar: falta `type`");
    let cuerpo = tokio::time::timeout(Duration::from_secs(10), recibido)
        .await
        .expect("el webhook tenía que recibir la alerta")
        .unwrap();
    assert_eq!(cuerpo["tipo"], "fallo");
    std::fs::remove_dir_all(&dir).ok();
}
