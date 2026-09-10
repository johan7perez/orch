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
async fn una_entrada_vacia_con_esquema_conocido_sigue_contando() {
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
    // El esquema viene propagado desde el generador, así que hay plan aunque
    // no llegue ni un lote: `count(*)` devuelve una fila con 0, como en SQL.
    assert_eq!(report.rows_written(), 1);
    assert_eq!(read_lines(&out), vec!["filas".to_string(), "0".to_string()]);
}

#[tokio::test]
async fn la_cadena_de_tres_nodos_y_la_query_fusionada_dan_lo_mismo() {
    // Es lo que hace comparables `benchmark_sql_chained.yaml` y
    // `benchmark_sql_fused.yaml`: si los resultados no coincidieran, medir sus
    // tiempos no diría nada.
    let dir = TempDir::new().expect("tempdir");
    let encadenado = temp_path(&dir, "encadenado.csv");
    let fusionado = temp_path(&dir, "fusionado.csv");

    let chained = run(&format!(
        r#"
name: encadenado
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 1000, with_text: false }} }}
  - {{ id: filtrar, type: transform, op: filter, config: {{ where: "id >= 500" }} }}
  - id: derivar
    type: transform
    op: derive
    config:
      columns:
        grupo: "id % 10"
        ajustado: "value * 1.18"
  - id: agregar
    type: transform
    op: aggregate
    config:
      group_by: [grupo]
      aggregates:
        n: "count(*)"
        total: "sum(ajustado)"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{encadenado}" }} }}
edges:
  - {{ from: generar, to: filtrar }}
  - {{ from: filtrar, to: derivar }}
  - {{ from: derivar, to: agregar }}
  - {{ from: agregar, to: escribir }}
"#
    ))
    .await;
    assert!(chained.succeeded, "{chained:?}");

    let fused = run(&format!(
        r#"
name: fusionado
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 1000, with_text: false }} }}
  - id: todo
    type: transform
    op: sql
    config:
      query: >
        SELECT id % 10 AS grupo, count(*) AS n, sum(value * 1.18) AS total
        FROM input WHERE id >= 500 GROUP BY id % 10
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{fusionado}" }} }}
edges:
  - {{ from: generar, to: todo }}
  - {{ from: todo, to: escribir }}
"#
    ))
    .await;
    assert!(fused.succeeded, "{fused:?}");

    // El agregado no garantiza orden, así que se comparan ordenados.
    let mut a = read_lines(&encadenado);
    let mut b = read_lines(&fusionado);
    assert_eq!(a[0], "grupo,n,total");
    assert_eq!(a[0], b[0]);
    a.sort();
    b.sort();
    assert_eq!(a, b);
    assert_eq!(a.len(), 11, "10 grupos más la cabecera");
}

// --- errores ----------------------------------------------------------------

#[tokio::test]
async fn una_query_con_sintaxis_invalida_se_detecta_en_validate() {
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
        .await
        .expect_err("`SELCT` no es SQL válido");
    assert!(err.to_string().contains("SQL inválido"), "{err}");
}

#[tokio::test]
async fn una_columna_inexistente_impide_arrancar_la_ejecucion() {
    // `run` planifica antes de mover un solo dato, así que una columna que no
    // existe corta la ejecución en seco en vez de dejar el pipeline a medias.
    // La validación equivalente en `orch validate` está en `joins.rs`.
    let err = Executor::new(registry())
        .run(&dag(r#"
name: columna-mala
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 10, with_text: false } }
  - { id: q, type: transform, op: filter, config: { where: "no_existe > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: q }
  - { from: q, to: descartar }
"#))
        .await
        .expect_err("la propagación de esquemas debería rechazarlo");

    assert!(
        err.to_string().contains("no_existe"),
        "el error debería nombrar la columna: {err}"
    );
}

#[tokio::test]
async fn un_fallo_a_mitad_no_da_por_bueno_al_sink() {
    // El origen no existe, así que el fallo sólo aparece al ejecutar.
    let report = run(r#"
name: fuente-rota
nodes:
  - { id: leer, type: source, connector: csv, config: { path: "./no/existe.csv" } }
  - { id: q, type: transform, op: filter, config: { where: "id > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: leer, to: q }
  - { from: q, to: descartar }
"#)
    .await;

    assert!(!report.succeeded);
    assert_eq!(
        report
            .nodes
            .iter()
            .find(|n| n.id == "leer")
            .expect("informe del origen")
            .status,
        NodeStatus::Failed
    );
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

#[tokio::test]
async fn un_campo_desconocido_en_la_config_se_detecta_en_validate() {
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
        .await
        .expect_err("`wher` no es un campo válido");
    assert!(err.to_string().contains('q'), "{err}");
}
