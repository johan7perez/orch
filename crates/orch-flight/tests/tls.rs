//! Aristas remotas cifradas, con certificados generados para cada test.
//!
//! Lo que tiene que pasar, y lo que tiene que fallar: un cliente sin
//! certificado no entra en un servidor con TLS mutuo, y un servidor de otra
//! CA no se acepta. Lo que no se arregla esperando falla al momento, en vez
//! de insistir hasta agotar el plazo.

mod comun;

use std::sync::Arc;
use std::time::{Duration, Instant};

use comun::{correr, error_de, ids, puerto, r, Pki, Recogido};
use orch_core::{Dag, Executor, PipelineSpec, Registry};

fn registro(recogido: &Recogido) -> Arc<Registry> {
    Arc::new(comun::registro(recogido))
}

/// Sirve 10 000 filas con TLS (y TLS mutuo si `mutuo`).
fn servidor(pki: &Pki, puerto: u16, mutuo: bool, escucha: &str) -> String {
    let client_ca = if mutuo {
        format!(", client_ca: \"{}\"", r(&pki.ca))
    } else {
        String::new()
    };
    format!(
        r#"
name: servidor
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 10000, with_text: false }} }}
  - id: servir
    type: sink
    connector: flight
    config:
      listen: "{escucha}:{puerto}"
      ticket: datos
      wait_for_client: 3s
      tls: {{ cert: "{}", key: "{}"{client_ca} }}
edges:
  - {{ from: generar, to: servir }}
"#,
        r(&pki.servidor_cert),
        r(&pki.servidor_key),
    )
}

/// Lee con `tls` como bloque YAML (o sin él).
fn cliente(puerto: u16, url: &str, tls: &str) -> String {
    format!(
        r#"
name: cliente
nodes:
  - id: leer
    type: source
    connector: flight
    config: {{ url: "{url}://127.0.0.1:{puerto}", ticket: datos, wait_for_server: 3s {tls} }}
  - {{ id: guardar, type: sink, connector: guardar }}
edges:
  - {{ from: leer, to: guardar }}
"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn con_tls_mutuo_el_flujo_llega_entero() {
    let pki = Pki::nueva();
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();
    let tls = format!(
        r#", tls: {{ ca: "{}", cert: "{}", key: "{}" }}"#,
        r(&pki.ca),
        r(&pki.cliente_cert),
        r(&pki.cliente_key)
    );

    let (lee, sirve) = tokio::join!(
        correr(&registry, cliente(p, "https", &tls)),
        correr(&registry, servidor(&pki, p, true, "127.0.0.1"))
    );
    assert!(sirve.succeeded, "{sirve:?}");
    assert!(lee.succeeded, "{lee:?}");
    assert_eq!(ids(&recogido), (0..10_000).collect::<Vec<i64>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sin_certificado_de_cliente_no_se_entra() {
    let pki = Pki::nueva();
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();
    // Verifica al servidor, pero no presenta certificado propio.
    let tls = format!(r#", tls: {{ ca: "{}" }}"#, r(&pki.ca));

    let (lee, sirve) = tokio::join!(
        correr(&registry, cliente(p, "https", &tls)),
        correr(&registry, servidor(&pki, p, true, "127.0.0.1"))
    );
    assert!(!lee.succeeded);
    assert!(!sirve.succeeded, "no se entregó a nadie: {sirve:?}");
    assert!(error_de(&sirve, "servir").contains("nadie vino a leer"));
    assert!(recogido.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn un_cliente_de_otra_ca_no_se_acepta() {
    let pki = Pki::nueva();
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();
    let tls = format!(
        r#", tls: {{ ca: "{}", cert: "{}", key: "{}" }}"#,
        r(&pki.ca),
        r(&pki.intruso_cert),
        r(&pki.intruso_key)
    );

    let (lee, sirve) = tokio::join!(
        correr(&registry, cliente(p, "https", &tls)),
        correr(&registry, servidor(&pki, p, true, "127.0.0.1"))
    );
    assert!(!lee.succeeded && !sirve.succeeded);
    assert!(recogido.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn un_servidor_que_no_es_de_la_ca_esperada_falla_al_momento() {
    let pki = Pki::nueva();
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();
    // El cliente sólo se fía de otra CA.
    let tls = format!(r#", tls: {{ ca: "{}" }}"#, r(&pki.otra_ca));

    let inicio = Instant::now();
    let (lee, _) = tokio::join!(
        async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            correr(&registry, cliente(p, "https", &tls).replace("3s", "30s")).await
        },
        correr(&registry, servidor(&pki, p, false, "127.0.0.1"))
    );
    assert!(!lee.succeeded);
    let error = error_de(&lee, "leer");
    assert!(error.contains("TLS"), "{error}");
    // No se queda insistiendo los 30 s: un certificado que no vale no se
    // arregla esperando.
    assert!(
        inicio.elapsed() < Duration::from_secs(10),
        "{:?}",
        inicio.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_sin_ca_se_fia_solo_de_las_del_sistema() {
    let pki = Pki::nueva();
    let recogido = Recogido::default();
    let registry = registro(&recogido);
    let p = puerto();

    let (lee, _) = tokio::join!(
        async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            correr(&registry, cliente(p, "https", "")).await
        },
        correr(&registry, servidor(&pki, p, false, "127.0.0.1"))
    );
    // Una CA privada no está entre las del sistema.
    assert!(!lee.succeeded);
    assert!(
        error_de(&lee, "leer").contains("TLS"),
        "{}",
        error_de(&lee, "leer")
    );
}

async fn validar(registry: &Arc<Registry>, yaml: &str) -> Result<(), String> {
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).unwrap()).unwrap();
    Executor::new(Arc::clone(registry))
        .prepare(&dag)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn escuchar_fuera_con_tls_mutuo_no_necesita_token() {
    let pki = Pki::nueva();
    let registry = registro(&Recogido::default());
    validar(&registry, &servidor(&pki, 50999, true, "0.0.0.0"))
        .await
        .expect("TLS mutuo autentica a quien lee");
    // Con TLS pero sin `client_ca` no se sabe quién lee: hace falta token.
    let error = validar(&registry, &servidor(&pki, 50999, false, "0.0.0.0"))
        .await
        .unwrap_err();
    assert!(error.contains("client_ca"), "{error}");
}

#[tokio::test]
async fn los_errores_de_configuracion_salen_en_validate() {
    let pki = Pki::nueva();
    let registry = registro(&Recogido::default());

    let http_con_tls = format!(r#", tls: {{ ca: "{}" }}"#, r(&pki.ca));
    let error = validar(&registry, &cliente(1, "http", &http_con_tls))
        .await
        .unwrap_err();
    assert!(error.contains("https://"), "{error}");

    let falta = servidor(&pki, 1, false, "127.0.0.1").replace("servidor.pem", "no-existe.pem");
    let error = validar(&registry, &falta).await.unwrap_err();
    assert!(error.contains("tls.cert"), "{error}");

    let medio = format!(
        r#", tls: {{ ca: "{}", cert: "{}" }}"#,
        r(&pki.ca),
        r(&pki.cliente_cert)
    );
    let error = validar(&registry, &cliente(1, "https", &medio))
        .await
        .unwrap_err();
    assert!(error.contains("van juntos"), "{error}");
}
