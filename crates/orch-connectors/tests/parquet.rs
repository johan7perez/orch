//! Conector Parquet: ida y vuelta, proyección empujada y esquema exacto.

use orch_connectors::default_registry_arc;
use orch_core::{Dag, Executor, NodeStatus, PipelineSpec, RunReport};
use tempfile::TempDir;

fn dag(yaml: &str) -> Dag {
    Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido")
}

async fn run(yaml: &str) -> RunReport {
    Executor::new(default_registry_arc())
        .run(&dag(yaml))
        .await
        .expect("la ejecución debería arrancar")
}

async fn validation_error(yaml: &str) -> String {
    Executor::new(default_registry_arc())
        .prepare(&dag(yaml))
        .await
        .expect_err("se esperaba un error de validación")
        .to_string()
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

/// Genera un Parquet con columnas id, value y label.
async fn make_parquet(path: &str, rows: u64) {
    let report = run(&format!(
        r#"
name: generar-parquet
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: {rows} }} }}
  - {{ id: escribir, type: sink, connector: parquet, config: {{ path: "{path}" }} }}
edges:
  - {{ from: generar, to: escribir }}
"#
    ))
    .await;
    assert!(report.succeeded, "{report:?}");
}

#[tokio::test]
async fn ida_y_vuelta_conserva_las_filas() {
    let dir = TempDir::new().expect("tempdir");
    let parquet = temp_path(&dir, "datos.parquet");
    let csv = temp_path(&dir, "salida.csv");

    make_parquet(&parquet, 500).await;

    let report = run(&format!(
        r#"
name: leer-parquet
nodes:
  - {{ id: leer, type: source, connector: parquet, config: {{ path: "{parquet}" }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{csv}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 500);
    let lines = read_lines(&csv);
    assert_eq!(lines[0], "id,value,label");
    assert_eq!(lines.len(), 501);
    assert_eq!(lines[1], "0,0.0,row-0");
}

#[tokio::test]
async fn la_proyeccion_se_empuja_al_fichero() {
    let dir = TempDir::new().expect("tempdir");
    let parquet = temp_path(&dir, "datos.parquet");
    let csv = temp_path(&dir, "salida.csv");

    make_parquet(&parquet, 100).await;

    let report = run(&format!(
        r#"
name: proyeccion
nodes:
  - id: leer
    type: source
    connector: parquet
    config:
      path: "{parquet}"
      columns: [label, id]
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{csv}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    // `columns` es una proyección de lectura: conserva el orden del fichero,
    // no el de la lista. `value` no se lee.
    assert_eq!(read_lines(&csv)[0], "id,label");
}

#[tokio::test]
async fn una_columna_inexistente_se_detecta_en_validate() {
    let dir = TempDir::new().expect("tempdir");
    let parquet = temp_path(&dir, "datos.parquet");
    make_parquet(&parquet, 10).await;

    let err = validation_error(&format!(
        r#"
name: columna-mala
nodes:
  - id: leer
    type: source
    connector: parquet
    config:
      path: "{parquet}"
      columns: [id, fantasma]
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: descartar }}
"#
    ))
    .await;

    assert!(err.contains("fantasma"), "{err}");
    assert!(err.contains("disponibles"), "{err}");
}

#[tokio::test]
async fn el_esquema_del_parquet_llega_a_validate_con_los_tipos_reales() {
    // El pie del fichero da los tipos exactos, no inferidos: un `select`
    // sobre una columna que no está se detecta sin leer datos.
    let dir = TempDir::new().expect("tempdir");
    let parquet = temp_path(&dir, "datos.parquet");
    make_parquet(&parquet, 10).await;

    let err = validation_error(&format!(
        r#"
name: select-malo
nodes:
  - {{ id: leer, type: source, connector: parquet, config: {{ path: "{parquet}" }} }}
  - {{ id: recortar, type: transform, op: select, config: {{ columns: [id, no_existe] }} }}
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: recortar }}
  - {{ from: recortar, to: descartar }}
"#
    ))
    .await;
    assert!(err.contains("no_existe"), "{err}");
}

#[tokio::test]
async fn un_resultado_vacio_produce_un_parquet_legible() {
    // El esquema viene propagado desde el generador, así que el fichero se
    // escribe con su pie aunque no llegue ni una fila.
    let dir = TempDir::new().expect("tempdir");
    let vacio = temp_path(&dir, "vacio.parquet");
    let csv = temp_path(&dir, "salida.csv");

    let report = run(&format!(
        r#"
name: parquet-vacio
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 100, with_text: false }} }}
  - {{ id: nada, type: transform, op: limit, config: {{ rows: 0 }} }}
  - {{ id: escribir, type: sink, connector: parquet, config: {{ path: "{vacio}" }} }}
edges:
  - {{ from: generar, to: nada }}
  - {{ from: nada, to: escribir }}
"#
    ))
    .await;
    assert!(report.succeeded, "{report:?}");

    // Se puede volver a leer: tiene pie y esquema.
    let vuelta = run(&format!(
        r#"
name: releer
nodes:
  - {{ id: leer, type: source, connector: parquet, config: {{ path: "{vacio}" }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{csv}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
    ))
    .await;
    assert!(vuelta.succeeded, "{vuelta:?}");
    assert_eq!(vuelta.rows_written(), 0);
}

#[tokio::test]
async fn una_compresion_desconocida_se_detecta_en_validate() {
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "salida.parquet");

    let err = validation_error(&format!(
        r#"
name: compresion-mala
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 1 }} }}
  - id: escribir
    type: sink
    connector: parquet
    config:
      path: "{out}"
      compression: brotlix
edges:
  - {{ from: generar, to: escribir }}
"#
    ))
    .await;
    assert!(err.contains("brotlix"), "{err}");
    assert!(err.contains("zstd"), "debería listar las soportadas: {err}");
}

#[tokio::test]
async fn zstd_y_row_group_size_producen_un_fichero_legible() {
    let dir = TempDir::new().expect("tempdir");
    let parquet = temp_path(&dir, "comprimido.parquet");
    let csv = temp_path(&dir, "salida.csv");

    let report = run(&format!(
        r#"
name: comprimido
settings: {{ batch_size: 256 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 5000 }} }}
  - id: escribir
    type: sink
    connector: parquet
    config:
      path: "{parquet}"
      compression: zstd
      row_group_size: 1000
edges:
  - {{ from: generar, to: escribir }}
"#
    ))
    .await;
    assert!(report.succeeded, "{report:?}");

    let vuelta = run(&format!(
        r#"
name: releer
nodes:
  - {{ id: leer, type: source, connector: parquet, config: {{ path: "{parquet}" }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{csv}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
    ))
    .await;
    assert!(vuelta.succeeded, "{vuelta:?}");
    assert_eq!(vuelta.rows_written(), 5000);
}

#[tokio::test]
async fn un_parquet_que_no_existe_falla_al_ejecutar_no_al_validar() {
    let yaml = r#"
name: sin-fichero
nodes:
  - { id: leer, type: source, connector: parquet, config: { path: "./no/existe.parquet" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: leer, to: descartar }
"#;

    // `validate` pasa: el fichero puede generarlo una ejecución anterior.
    Executor::new(default_registry_arc())
        .prepare(&dag(yaml))
        .await
        .expect("un fichero ausente no invalida el pipeline");

    let report = run(yaml).await;
    assert!(!report.succeeded);
    let leer = report
        .nodes
        .iter()
        .find(|n| n.id == "leer")
        .expect("informe del lector");
    assert_eq!(leer.status, NodeStatus::Failed);
    assert!(
        leer.error
            .as_deref()
            .unwrap_or_default()
            .contains("no se pudo abrir"),
        "{:?}",
        leer.error
    );
}
