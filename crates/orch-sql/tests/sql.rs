//! Transformaciones SQL de extremo a extremo, dentro de pipelines reales.

use std::sync::Arc;

use orch_core::{Dag, Executor, NodeStatus, PipelineSpec, Registry, RunReport};
use tempfile::TempDir;

fn registry() -> Arc<Registry> {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
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

fn write_customers(dir: &TempDir) -> String {
    let path = dir.path().join("customers.csv");
    std::fs::write(
        &path,
        "id,name,city,spend\n\
         1,Ana,Santo Domingo,1000.0\n\
         2,Luis,Santiago,800.0\n\
         3,Marta,Santo Domingo,2000.0\n\
         4,Pedro,Santiago,200.0\n\
         5,Rosa,La Romana,500.0\n",
    )
    .expect("escribir el CSV de entrada");
    path.to_string_lossy().replace('\\', "/")
}

fn read_lines(path: &str) -> Vec<String> {
    std::fs::read_to_string(path)
        .expect("fichero de salida")
        .lines()
        .map(str::to_string)
        .collect()
}

// --- generación de SQL ------------------------------------------------------

#[test]
fn aggregate_genera_la_query_esperada() {
    let config = serde_json::json!({
        "group_by": ["city"],
        "aggregates": { "total": "sum(spend)", "clientes": "count(*)" },
    });
    let transform = orch_sql::build_aggregate("agg", &config).expect("config válida");
    // El mapa de agregados es un BTreeMap: el orden de columnas es estable.
    assert_eq!(
        transform.query(),
        r#"SELECT "city", (count(*)) AS "clientes", (sum(spend)) AS "total" FROM "input" GROUP BY "city""#
    );
}

#[test]
fn filter_genera_la_query_esperada() {
    let config = serde_json::json!({ "where": "spend > 500" });
    let transform = orch_sql::build_filter("f", &config).expect("config válida");
    assert_eq!(
        transform.query(),
        r#"SELECT * FROM "input" WHERE (spend > 500)"#
    );
}

#[test]
fn un_identificador_con_comillas_se_escapa() {
    let config = serde_json::json!({
        "aggregates": { "raro\"alias": "count(*)" },
    });
    let transform = orch_sql::build_aggregate("agg", &config).expect("config válida");
    assert!(
        transform.query().contains(r#"AS "raro""alias""#),
        "{}",
        transform.query()
    );
}

// --- ejecución --------------------------------------------------------------

#[tokio::test]
async fn filter_descarta_las_filas_que_no_cumplen() {
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: filtro
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 100, with_text: false }} }}
  - {{ id: filtrar, type: transform, op: filter, config: {{ where: "id >= 90" }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: generar, to: filtrar }}
  - {{ from: filtrar, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 10);
    assert_eq!(read_lines(&out).len(), 11);
}

#[tokio::test]
async fn derive_anade_columnas_calculadas() {
    let dir = TempDir::new().expect("tempdir");
    let input = write_customers(&dir);
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: derivadas
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{input}" }} }}
  - id: calcular
    type: transform
    op: derive
    config:
      columns:
        con_itbis: "spend * 1.18"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: calcular }}
  - {{ from: calcular, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    let lines = read_lines(&out);
    assert_eq!(lines[0], "id,name,city,spend,con_itbis");
    assert_eq!(lines.len(), 6);
    assert!(
        lines[1].starts_with("1,Ana,Santo Domingo,1000.0,1180"),
        "{}",
        lines[1]
    );
}

#[tokio::test]
async fn aggregate_agrupa_y_suma() {
    let dir = TempDir::new().expect("tempdir");
    let input = write_customers(&dir);
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: agregado
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{input}" }} }}
  - id: agrupar
    type: transform
    op: aggregate
    config:
      group_by: [city]
      aggregates:
        total: "sum(spend)"
        clientes: "count(*)"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: agrupar }}
  - {{ from: agrupar, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    let lines = read_lines(&out);
    assert_eq!(lines[0], "city,clientes,total");
    // Tres ciudades distintas, en cualquier orden.
    assert_eq!(lines.len(), 4);
    assert!(
        lines.iter().any(|l| l.starts_with("Santo Domingo,2,3000")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.starts_with("La Romana,1,500")),
        "{lines:?}"
    );
}

#[tokio::test]
async fn sql_arbitrario_ordena_y_recorta() {
    let dir = TempDir::new().expect("tempdir");
    let input = write_customers(&dir);
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: top
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{input}" }} }}
  - id: top2
    type: transform
    op: sql
    config:
      query: "SELECT name, spend FROM input ORDER BY spend DESC LIMIT 2"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: top2 }}
  - {{ from: top2, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    let lines = read_lines(&out);
    assert_eq!(lines[0], "name,spend");
    assert_eq!(lines.len(), 3);
    assert!(lines[1].starts_with("Marta,2000"), "{}", lines[1]);
    assert!(lines[2].starts_with("Ana,1000"), "{}", lines[2]);
}

#[tokio::test]
async fn un_limit_en_sql_no_agota_el_origen() {
    // El plan termina antes que la entrada. El alimentador debe cortarse sin
    // que el origen lo interprete como un fallo.
    let report = run(r#"
name: limite-sql
settings: { batch_size: 1000 }
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 1000000, with_text: false } }
  - id: recortar
    type: transform
    op: sql
    config:
      query: "SELECT * FROM input LIMIT 5"
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: recortar }
  - { from: recortar, to: descartar }
"#)
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 5);

    let generado = report
        .nodes
        .iter()
        .find(|n| n.id == "generar")
        .expect("informe del generador");
    assert_eq!(generado.status, NodeStatus::Succeeded);
    assert!(
        generado.output.rows < 1_000_000,
        "el origen no debería agotar la fuente: {} filas",
        generado.output.rows
    );
}

#[tokio::test]
async fn una_entrada_vacia_produce_una_salida_vacia() {
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: vacio
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 1000, with_text: false }} }}
  - {{ id: nada, type: transform, op: limit, config: {{ rows: 0 }} }}
  - id: contar
    type: transform
    op: aggregate
    config:
      aggregates:
        filas: "count(*)"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: generar, to: nada }}
  - {{ from: nada, to: contar }}
  - {{ from: contar, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 0);
    // Sin esquema no hay plan: la salida es vacía en vez de una fila con 0.
    assert!(read_lines(&out).is_empty());
}

// --- errores ----------------------------------------------------------------

#[test]
fn una_query_con_sintaxis_invalida_se_detecta_en_validate() {
    let dag = dag(r#"
name: sintaxis-mala
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 1 } }
  - { id: q, type: transform, op: sql, config: { query: "SELCT * FROM input" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: q }
  - { from: q, to: descartar }
"#);

    let err = Executor::new(registry())
        .prepare(&dag)
        .expect_err("`SELCT` no es SQL válido");
    assert!(err.to_string().contains("SQL inválido"), "{err}");
}

#[tokio::test]
async fn una_columna_inexistente_falla_al_planificar() {
    let report = run(r#"
name: columna-mala
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 10, with_text: false } }
  - { id: q, type: transform, op: filter, config: { where: "no_existe > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: q }
  - { from: q, to: descartar }
"#)
    .await;

    assert!(!report.succeeded);
    let node = report
        .nodes
        .iter()
        .find(|n| n.id == "q")
        .expect("informe del nodo");
    assert_eq!(node.status, NodeStatus::Failed);
    let message = node.error.as_deref().unwrap_or_default();
    assert!(
        message.contains("no_existe"),
        "el error debería nombrar la columna: {message}"
    );
    // El sink no puede darse por bueno con una salida truncada.
    assert_eq!(
        report
            .nodes
            .iter()
            .find(|n| n.id == "descartar")
            .expect("informe del sink")
            .status,
        NodeStatus::Skipped
    );
}

#[test]
fn un_campo_desconocido_en_la_config_se_detecta_en_validate() {
    let dag = dag(r#"
name: config-mala
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 1 } }
  - { id: q, type: transform, op: filter, config: { wher: "id > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: q }
  - { from: q, to: descartar }
"#);

    let err = Executor::new(registry())
        .prepare(&dag)
        .expect_err("`wher` no es un campo válido");
    assert!(err.to_string().contains('q'), "{err}");
}
