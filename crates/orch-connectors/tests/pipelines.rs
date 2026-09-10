//! Pipelines completos de extremo a extremo con los conectores nativos.

use orch_connectors::default_registry_arc;
use orch_core::{Dag, Executor, NodeStatus, PipelineSpec, RunReport};
use tempfile::TempDir;

async fn run(yaml: &str) -> RunReport {
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido");
    Executor::new(default_registry_arc())
        .run(&dag)
        .await
        .expect("la ejecución debería arrancar")
}

fn write_csv(dir: &TempDir, name: &str, contents: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, contents).expect("escribir el CSV de entrada");
    path.to_string_lossy().replace('\\', "/")
}

const CUSTOMERS: &str = "id,name,city,spend\n\
1,Ana,Santo Domingo,1200.5\n\
2,Luis,Santiago,830.0\n\
3,Marta,La Romana,2150.75\n\
4,Pedro,Santo Domingo,415.25\n";

#[tokio::test]
async fn csv_a_csv_conserva_todas_las_filas() {
    let dir = TempDir::new().expect("tempdir");
    let input = write_csv(&dir, "customers.csv", CUSTOMERS);
    let output = dir.path().join("out/customers.csv");
    let output_str = output.to_string_lossy().replace('\\', "/");

    let report = run(&format!(
        r#"
name: csv-a-csv
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{input}" }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{output_str}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 4);

    let written = std::fs::read_to_string(&output).expect("el sink debió crear el fichero");
    let lines: Vec<&str> = written.lines().collect();
    assert_eq!(lines[0], "id,name,city,spend");
    assert_eq!(lines.len(), 5);
    assert!(written.contains("Marta"));
}

#[tokio::test]
async fn select_y_rename_reescriben_el_esquema() {
    let dir = TempDir::new().expect("tempdir");
    let input = write_csv(&dir, "customers.csv", CUSTOMERS);
    let output = dir.path().join("resumen.csv");
    let output_str = output.to_string_lossy().replace('\\', "/");

    let report = run(&format!(
        r#"
name: proyeccion
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{input}" }} }}
  - {{ id: recortar, type: transform, op: select, config: {{ columns: [name, spend] }} }}
  - {{ id: renombrar, type: transform, op: rename, config: {{ columns: {{ name: cliente, spend: gasto }} }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{output_str}" }} }}
edges:
  - {{ from: leer, to: recortar }}
  - {{ from: recortar, to: renombrar }}
  - {{ from: renombrar, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    let written = std::fs::read_to_string(&output).expect("fichero de salida");
    assert_eq!(written.lines().next(), Some("cliente,gasto"));
    assert!(!written.contains("Santo Domingo"));
}

#[tokio::test]
async fn limit_corta_sin_marcar_el_pipeline_como_fallido() {
    // El caso interesante: `limit` deja de leer y el origen se encuentra el
    // canal cerrado. Eso es una parada limpia, no un fallo.
    let report = run(r#"
name: limite
settings: { batch_size: 1000 }
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 1000000 } }
  - { id: recortar, type: transform, op: limit, config: { rows: 10 } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: recortar }
  - { from: recortar, to: descartar }
"#)
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 10);

    let generado = report
        .nodes
        .iter()
        .find(|n| n.id == "generar")
        .expect("informe del generador");
    assert_eq!(generado.status, NodeStatus::Succeeded);
    // No se generó el millón de filas: la parada temprana se propagó hacia atrás.
    assert!(
        generado.output.rows < 1_000_000,
        "el origen no debería agotar la fuente: {} filas",
        generado.output.rows
    );
}

#[tokio::test]
async fn un_fichero_inexistente_falla_con_un_mensaje_util() {
    let report = run(r#"
name: sin-fichero
nodes:
  - { id: leer, type: source, connector: csv, config: { path: "./no/existe.csv" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: leer, to: descartar }
"#)
    .await;

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

#[tokio::test]
async fn una_config_invalida_se_detecta_en_validate() {
    let dag = Dag::build(
        PipelineSpec::from_yaml_str(
            "test.yaml",
            r#"
name: config-mala
nodes:
  - { id: leer, type: source, connector: csv, config: { ruta: "x.csv" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: leer, to: descartar }
"#,
        )
        .expect("YAML válido"),
    )
    .expect("DAG válido");

    let err = Executor::new(default_registry_arc())
        .prepare(&dag)
        .expect_err("`ruta` no es un campo válido");
    assert!(err.to_string().contains("leer"), "{err}");
}

#[tokio::test]
async fn el_generador_alimenta_dos_destinos() {
    let dir = TempDir::new().expect("tempdir");
    let a = dir
        .path()
        .join("a.csv")
        .to_string_lossy()
        .replace('\\', "/");
    let b = dir
        .path()
        .join("b.csv")
        .to_string_lossy()
        .replace('\\', "/");

    let report = run(&format!(
        r#"
name: fan-out
settings: {{ batch_size: 256 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 1000, with_text: false }} }}
  - {{ id: a, type: sink, connector: csv, config: {{ path: "{a}" }} }}
  - {{ id: b, type: sink, connector: csv, config: {{ path: "{b}" }} }}
edges:
  - {{ from: generar, to: a }}
  - {{ from: generar, to: b }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 2000);
    for path in [&a, &b] {
        let contents = std::fs::read_to_string(path).expect("fichero de salida");
        assert_eq!(contents.lines().count(), 1001);
    }
}
