//! Conector REST contra un servidor local: paginación, reintentos, límite de
//! tasa y publicación por trozos.

mod common;

use std::sync::Arc;
use std::time::Instant;

use common::{Reply, TestServer};
use orch_core::{Dag, Executor, NodeStatus, PipelineSpec, Registry, RunReport};
use tempfile::TempDir;

fn registry() -> Arc<Registry> {
    let mut registry = orch_connectors::default_registry();
    orch_rest::register(&mut registry);
    Arc::new(registry)
}

fn dag(yaml: &str) -> Dag {
    Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido")
}

async fn run(yaml: &str) -> RunReport {
    Executor::new(registry())
        .run(&dag(yaml))
        .await
        .expect("la ejecución debería arrancar")
}

fn temp_path(dir: &TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().replace('\\', "/")
}

fn read_lines(path: &str) -> Vec<String> {
    std::fs::read_to_string(path)
        .expect("fichero de salida")
        .lines()
        .map(str::to_string)
        .collect()
}

/// Pipeline `rest -> csv` con la config de origen que se le pase.
fn read_pipeline(source_config: &str, out: &str) -> String {
    format!(
        r#"
name: rest
nodes:
  - id: api
    type: source
    connector: rest
    config:
{source_config}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: api, to: escribir }}
"#
    )
}

// --- lectura ----------------------------------------------------------------

#[tokio::test]
async fn lee_una_respuesta_sencilla() {
    let server = TestServer::always(Reply::ok(
        r#"[{"id":1,"nombre":"Ana"},{"id":2,"nombre":"Luis"}]"#,
    ))
    .await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!("      url: \"{}/items\"", server.url),
        &out,
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 2);
    let lines = read_lines(&out);
    assert_eq!(lines[0], "id,nombre");
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn extrae_los_registros_de_una_ruta_anidada() {
    let server = TestServer::always(Reply::ok(
        r#"{"meta":{"total":1},"data":{"items":[{"id":7,"ok":true}]}}"#,
    ))
    .await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      records_path: data.items",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 1);
    assert_eq!(read_lines(&out)[0], "id,ok");
}

#[tokio::test]
async fn una_ruta_de_registros_inexistente_da_un_error_util() {
    let server = TestServer::always(Reply::ok(r#"{"resultados":[]}"#)).await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      records_path: data.items",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(!report.succeeded);
    let api = report
        .nodes
        .iter()
        .find(|n| n.id == "api")
        .expect("informe");
    let message = api.error.as_deref().unwrap_or_default();
    assert!(message.contains("data.items"), "{message}");
    // El error muestra lo que sí llegó, para poder corregir la ruta.
    assert!(message.contains("resultados"), "{message}");
}

#[tokio::test]
async fn pagina_por_numero_de_pagina_hasta_una_vacia() {
    let server = TestServer::start(vec![
        Reply::ok(r#"[{"id":1},{"id":2}]"#),
        Reply::ok(r#"[{"id":3}]"#),
        Reply::ok("[]"),
    ])
    .await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      pagination: {{ kind: page, param: p, start: 1 }}",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 3);
    // Tres peticiones: la última descubre que ya no hay datos.
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].query("p").as_deref(), Some("1"));
    assert_eq!(requests[2].query("p").as_deref(), Some("3"));
}

#[tokio::test]
async fn pagina_por_cursor_hasta_que_no_hay_siguiente() {
    let server = TestServer::start(vec![
        Reply::ok(r#"{"items":[{"id":1}],"meta":{"next":"abc"}}"#),
        Reply::ok(r#"{"items":[{"id":2}],"meta":{"next":null}}"#),
    ])
    .await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      records_path: items\n      pagination: \
             {{ kind: cursor, param: c, next_path: meta.next }}",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 2);
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    // La primera va sin cursor; la segunda lleva el que devolvió la primera.
    assert_eq!(requests[0].query("c"), None);
    assert_eq!(requests[1].query("c").as_deref(), Some("abc"));
}

#[tokio::test]
async fn max_pages_corta_una_api_que_nunca_se_acaba() {
    let server = TestServer::always(Reply::ok(r#"[{"id":1}]"#)).await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      pagination: {{ kind: page, max_pages: 4 }}",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(server.request_count(), 4);
    assert_eq!(report.rows_written(), 4);
}

// --- reintentos y límite de tasa --------------------------------------------

#[tokio::test]
async fn reintenta_un_503_y_termina_bien() {
    let server = TestServer::start(vec![
        Reply::status(503, "no disponible"),
        Reply::status(503, "no disponible"),
        Reply::ok(r#"[{"id":1}]"#),
    ])
    .await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      retry: {{ max_attempts: 3, backoff_ms: 1 }}",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 1);
    assert_eq!(server.request_count(), 3);
}

#[tokio::test]
async fn un_401_no_se_reintenta() {
    // Un fallo de autenticación no mejora repitiéndolo.
    let server = TestServer::always(Reply::status(401, r#"{"error":"token inválido"}"#)).await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      retry: {{ max_attempts: 5, backoff_ms: 1 }}",
            server.url
        ),
        &out,
    ))
    .await;

    assert!(!report.succeeded);
    assert_eq!(server.request_count(), 1, "no debería reintentarse");
    let api = report
        .nodes
        .iter()
        .find(|n| n.id == "api")
        .expect("informe");
    let message = api.error.as_deref().unwrap_or_default();
    assert!(message.contains("401"), "{message}");
    // El cuerpo del error ayuda a entender qué rechazó la API.
    assert!(message.contains("token inválido"), "{message}");
}

#[tokio::test]
async fn respeta_retry_after_en_un_429() {
    let server = TestServer::start(vec![
        Reply::status(429, "despacio").with_header("Retry-After", "1"),
        Reply::ok(r#"[{"id":1}]"#),
    ])
    .await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let started = Instant::now();
    let report = run(&read_pipeline(
        &format!(
            // El backoff propio es de 1 ms: si se esperó un segundo, fue por
            // hacer caso a la cabecera.
            "      url: \"{}/items\"\n      retry: {{ max_attempts: 2, backoff_ms: 1 }}",
            server.url
        ),
        &out,
    ))
    .await;
    let elapsed = started.elapsed();

    assert!(report.succeeded, "{report:?}");
    assert!(
        elapsed.as_millis() >= 900,
        "debería haber esperado ~1 s, esperó {elapsed:?}"
    );
}

#[tokio::test]
async fn el_limite_de_tasa_separa_las_peticiones() {
    let server = TestServer::always(Reply::ok(r#"[{"id":1}]"#)).await;
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let started = Instant::now();
    let report = run(&read_pipeline(
        &format!(
            "      url: \"{}/items\"\n      rate_limit_per_second: 20\n      pagination: \
             {{ kind: page, max_pages: 4 }}",
            server.url
        ),
        &out,
    ))
    .await;
    let elapsed = started.elapsed();

    assert!(report.succeeded, "{report:?}");
    assert_eq!(server.request_count(), 4);
    // 20/s = 50 ms entre peticiones; tres esperas como mínimo.
    assert!(
        elapsed.as_millis() >= 140,
        "las peticiones salieron demasiado juntas: {elapsed:?}"
    );
}

// --- esquema ----------------------------------------------------------------

#[tokio::test]
async fn un_esquema_declarado_llega_a_validate() {
    // Sin declararlo no se puede validar nada aguas abajo: llamar a la API
    // durante `validate` tendría efectos secundarios.
    let server = TestServer::always(Reply::ok(r#"[{"id":1,"total":2.5}]"#)).await;
    let yaml = format!(
        r#"
name: esquema
nodes:
  - id: api
    type: source
    connector: rest
    config:
      url: "{}/items"
      schema:
        - {{ name: id, type: int64 }}
        - {{ name: total, type: float64 }}
  - {{ id: filtrar, type: transform, op: select, config: {{ columns: [id, fantasma] }} }}
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: api, to: filtrar }}
  - {{ from: filtrar, to: descartar }}
"#,
        server.url
    );

    let err = Executor::new(registry())
        .prepare(&dag(&yaml))
        .await
        .expect_err("`fantasma` no está en el esquema declarado");
    assert!(err.to_string().contains("fantasma"), "{err}");
    assert_eq!(
        server.request_count(),
        0,
        "validate no debe llamar a la API"
    );
}

#[tokio::test]
async fn un_tipo_desconocido_en_el_esquema_se_detecta_en_validate() {
    let yaml = r#"
name: tipo-malo
nodes:
  - id: api
    type: source
    connector: rest
    config:
      url: "http://localhost:1/items"
      schema:
        - { name: id, type: numerico }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: api, to: descartar }
"#;

    let err = Executor::new(registry())
        .prepare(&dag(yaml))
        .await
        .expect_err("`numerico` no es un tipo");
    assert!(err.to_string().contains("numerico"), "{err}");
    assert!(
        err.to_string().contains("int64"),
        "debería listarlos: {err}"
    );
}

// --- escritura --------------------------------------------------------------

#[tokio::test]
async fn el_destino_publica_en_trozos() {
    let server = TestServer::always(Reply::status(201, "{}")).await;

    let report = run(&format!(
        r#"
name: publicar
settings: {{ batch_size: 1000 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 250, with_text: false }} }}
  - id: publicar
    type: sink
    connector: rest
    config:
      url: "{}/ingest"
      rows_per_request: 100
edges:
  - {{ from: generar, to: publicar }}
"#,
        server.url
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    // 250 filas en trozos de 100 = 3 peticiones.
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert!(requests[0].body.starts_with('['), "{}", requests[0].body);
    assert!(
        requests[0].body.contains("\"id\":0"),
        "{}",
        requests[0].body
    );
}

#[tokio::test]
async fn el_destino_puede_envolver_y_usar_ndjson() {
    let server = TestServer::always(Reply::ok("{}")).await;

    let report = run(&format!(
        r#"
name: envuelto
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 2, with_text: false }} }}
  - id: publicar
    type: sink
    connector: rest
    config:
      url: "{}/ingest"
      wrap_in: registros
edges:
  - {{ from: generar, to: publicar }}
"#,
        server.url
    ))
    .await;
    assert!(report.succeeded, "{report:?}");
    let body = &server.requests()[0].body;
    assert!(body.starts_with(r#"{"registros":["#), "{body}");

    let ndjson_server = TestServer::always(Reply::ok("{}")).await;
    let report = run(&format!(
        r#"
name: ndjson
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 2, with_text: false }} }}
  - id: publicar
    type: sink
    connector: rest
    config:
      url: "{}/ingest"
      body: ndjson
edges:
  - {{ from: generar, to: publicar }}
"#,
        ndjson_server.url
    ))
    .await;
    assert!(report.succeeded, "{report:?}");
    let body = &ndjson_server.requests()[0].body;
    assert_eq!(body.trim().lines().count(), 2, "{body}");
}

#[tokio::test]
async fn wrap_in_con_ndjson_se_rechaza_en_validate() {
    let yaml = r#"
name: incompatible
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 1 } }
  - id: publicar
    type: sink
    connector: rest
    config:
      url: "http://localhost:1/x"
      body: ndjson
      wrap_in: registros
edges:
  - { from: generar, to: publicar }
"#;

    let err = Executor::new(registry())
        .prepare(&dag(yaml))
        .await
        .expect_err("wrap_in y ndjson son incompatibles");
    assert!(err.to_string().contains("wrap_in"), "{err}");
}

#[tokio::test]
async fn un_error_del_destino_marca_el_nodo_como_fallido() {
    let server = TestServer::always(Reply::status(400, r#"{"error":"campo faltante"}"#)).await;

    let report = run(&format!(
        r#"
name: rechazado
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 5, with_text: false }} }}
  - id: publicar
    type: sink
    connector: rest
    config:
      url: "{}/ingest"
      retry: {{ max_attempts: 2, backoff_ms: 1 }}
edges:
  - {{ from: generar, to: publicar }}
"#,
        server.url
    ))
    .await;

    assert!(!report.succeeded);
    let publicar = report
        .nodes
        .iter()
        .find(|n| n.id == "publicar")
        .expect("informe");
    assert_eq!(publicar.status, NodeStatus::Failed);
    assert!(
        publicar
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("campo faltante"),
        "{:?}",
        publicar.error
    );
    // Un 400 tampoco se reintenta.
    assert_eq!(server.request_count(), 1);
}
