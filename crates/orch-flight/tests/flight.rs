//! Aristas remotas de verdad: un servidor Flight escuchando en esta máquina y
//! un cliente que lo lee, cada uno en su pipeline.
//!
//! Son dos ejecutores independientes, como serían dos procesos en dos
//! máquinas: no comparten nada más que la conexión.

mod comun;

use std::sync::Arc;
use std::time::Duration;

use comun::{correr, ids, nodo, puerto, Recogido};
use orch_core::{Dag, Executor, NodeStatus, PipelineSpec, Registry};

fn registro(recogido: &Recogido) -> Arc<Registry> {
    Arc::new(comun::registro(recogido))
}

fn productor(puerto: u16, filas: u64, extra: &str) -> String {
    format!(
        r#"
name: productor
settings: {{ batch_size: 8192 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: {filas}, with_text: false }} }}
  - id: servir
    type: sink
    connector: flight
    config: {{ listen: "127.0.0.1:{puerto}", ticket: datos, wait_for_client: 10s {extra} }}
edges:
  - {{ from: generar, to: servir }}
"#
    )
}

fn consumidor(puerto: u16, extra: &str, despues: &str) -> String {
    format!(
        r#"
name: consumidor
nodes:
  - id: leer
    type: source
    connector: flight
    config: {{ url: "http://127.0.0.1:{puerto}", ticket: datos, wait_for_server: 10s {extra} }}
  {despues}
  - {{ id: guardar, type: sink, connector: guardar }}
edges:
  {aristas}
"#,
        aristas = if despues.is_empty() {
            "- { from: leer, to: guardar }"
        } else {
            "- { from: leer, to: medio }\n  - { from: medio, to: guardar }"
        }
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn el_flujo_llega_entero_y_en_orden_a_otro_pipeline() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();

    // El consumidor arranca primero: tiene que esperar a que el servidor
    // escuche, como pasaría con dos máquinas que arrancan cuando quieren.
    let (lee, sirve) = tokio::join!(correr(&registry, consumidor(p, "", "")), async {
        tokio::time::sleep(Duration::from_millis(400)).await;
        correr(&registry, productor(p, 100_000, "")).await
    });

    assert!(sirve.succeeded, "{sirve:?}");
    assert!(lee.succeeded, "{lee:?}");
    assert_eq!(nodo(&sirve, "servir").input.rows, 100_000);
    assert_eq!(nodo(&lee, "leer").output.rows, 100_000);
    assert_eq!(ids(&recogido), (0..100_000).collect::<Vec<i64>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn un_fallo_del_que_sirve_llega_como_fallo_al_que_lee() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();
    // Divide entre cero en la fila 50 000: falla a mitad, con lotes ya
    // enviados.
    let productor = format!(
        r#"
name: productor
settings: {{ batch_size: 8192 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 100000, with_text: false }} }}
  - {{ id: romper, type: transform, op: derive, config: {{ columns: {{ x: "10 / (id - 50000)" }} }} }}
  - {{ id: servir, type: sink, connector: flight, config: {{ listen: "127.0.0.1:{p}", ticket: datos }} }}
edges:
  - {{ from: generar, to: romper }}
  - {{ from: romper, to: servir }}
"#
    );

    let (lee, sirve) = tokio::join!(
        correr(&registry, consumidor(p, "", "")),
        correr(&registry, productor)
    );

    assert!(!sirve.succeeded);
    assert!(!lee.succeeded, "un flujo cortado no puede parecer completo");
    let error = nodo(&lee, "leer").error.clone().unwrap_or_default();
    assert!(error.contains("el flujo remoto falló"), "{error}");
    assert!(error.contains("falló"), "{error}");
    // Lo que llegó antes del fallo no se da por bueno aguas abajo.
    assert_ne!(nodo(&lee, "guardar").status, NodeStatus::Succeeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sin_el_token_bueno_no_se_lee_nada() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();

    let (lee, sirve) = tokio::join!(
        correr(&registry, consumidor(p, ", token: otro", "")),
        correr(
            &registry,
            productor(p, 1_000, ", token: secreto")
                .replace("wait_for_client: 10s", "wait_for_client: 2s")
        )
    );

    let error = nodo(&lee, "leer").error.clone().unwrap_or_default();
    assert!(error.contains("token"), "{error}");
    assert!(!lee.succeeded);
    // El servidor no entregó nada a nadie, y lo dice.
    assert!(!sirve.succeeded);
    let error = nodo(&sirve, "servir").error.clone().unwrap_or_default();
    assert!(error.contains("nadie vino a leer"), "{error}");
    assert!(recogido.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn con_el_token_bueno_si() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();

    let (lee, sirve) = tokio::join!(
        correr(&registry, consumidor(p, ", token: secreto", "")),
        correr(&registry, productor(p, 1_000, ", token: secreto"))
    );
    assert!(lee.succeeded && sirve.succeeded, "{lee:?}\n{sirve:?}");
    assert_eq!(ids(&recogido).len(), 1_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn si_el_que_lee_corta_antes_el_que_sirve_lo_sabe() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();

    // Un `limit` al otro lado: lee diez filas y suelta el flujo.
    let (lee, sirve) = tokio::join!(
        correr(
            &registry,
            consumidor(
                p,
                "",
                "- { id: medio, type: transform, op: limit, config: { rows: 10 } }"
            )
        ),
        correr(&registry, productor(p, 50_000_000, ""))
    );

    assert!(lee.succeeded, "{lee:?}");
    assert_eq!(ids(&recogido).len(), 10);
    // Desde el servidor no se distingue un `limit` de una caída: no se da
    // por bueno un flujo que nadie leyó entero. Y no se queda colgado
    // generando cincuenta millones de filas.
    assert!(!sirve.succeeded);
    let error = nodo(&sirve, "servir").error.clone().unwrap_or_default();
    assert!(error.contains("se desconectó"), "{error}");
    assert!(nodo(&sirve, "generar").output.rows < 50_000_000);
}

#[tokio::test]
async fn escuchar_hacia_fuera_sin_token_no_se_permite() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let yaml = productor(50999, 10, "").replace("127.0.0.1", "0.0.0.0");
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", &yaml).unwrap()).unwrap();
    let error = Executor::new(registry)
        .prepare(&dag)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("token"), "{error}");
}

#[tokio::test]
async fn un_servidor_que_no_aparece_se_dice_claro() {
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();
    let lee = correr(
        &registry,
        consumidor(p, "", "").replace("wait_for_server: 10s", "wait_for_server: 1s"),
    )
    .await;
    assert!(!lee.succeeded);
    let error = nodo(&lee, "leer").error.clone().unwrap_or_default();
    assert!(error.contains("no respondió en 1s"), "{error}");
}
