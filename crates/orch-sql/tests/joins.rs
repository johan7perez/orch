//! Joins entre ramas del DAG y validación estática de esquemas.
//!
//! Cada arista entrante de un nodo `sql` se registra como una tabla con el
//! nombre del puerto (por defecto, el id del nodo de origen), así que unir dos
//! ramas no necesita sintaxis nueva.

use std::sync::Arc;

use orch_core::{Dag, Executor, PipelineSpec, Registry, RunReport};
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

async fn validation_error(yaml: &str) -> String {
    Executor::new(registry())
        .prepare(&dag(yaml))
        .await
        .expect_err("se esperaba un error de validación")
        .to_string()
}

fn write(dir: &TempDir, name: &str, contents: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, contents).expect("escribir el CSV");
    path.to_string_lossy().replace('\\', "/")
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

const CLIENTES: &str = "id,nombre\n1,Ana\n2,Luis\n3,Marta\n";
const PEDIDOS: &str = "cliente_id,importe\n1,100.0\n1,50.0\n2,300.0\n9,7.0\n";

// --- joins ------------------------------------------------------------------

#[tokio::test]
async fn une_dos_ramas_por_el_nombre_del_nodo() {
    let dir = TempDir::new().expect("tempdir");
    let clientes = write(&dir, "clientes.csv", CLIENTES);
    let pedidos = write(&dir, "pedidos.csv", PEDIDOS);
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: join
nodes:
  - {{ id: clientes, type: source, connector: csv, config: {{ path: "{clientes}" }} }}
  - {{ id: pedidos, type: source, connector: csv, config: {{ path: "{pedidos}" }} }}
  - id: unir
    type: transform
    op: sql
    config:
      query: >
        SELECT c.nombre, sum(p.importe) AS total
        FROM clientes c JOIN pedidos p ON c.id = p.cliente_id
        GROUP BY c.nombre
        ORDER BY total DESC
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: clientes, to: unir }}
  - {{ from: pedidos, to: unir }}
  - {{ from: unir, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    let lines = read_lines(&out);
    assert_eq!(lines[0], "nombre,total");
    // Marta no tiene pedidos y el pedido del cliente 9 no tiene cliente.
    assert_eq!(lines.len(), 3);
    assert!(lines[1].starts_with("Luis,300"), "{}", lines[1]);
    assert!(lines[2].starts_with("Ana,150"), "{}", lines[2]);
}

#[tokio::test]
async fn el_puerto_puede_renombrarse_con_port() {
    let dir = TempDir::new().expect("tempdir");
    let clientes = write(&dir, "clientes.csv", CLIENTES);
    let pedidos = write(&dir, "pedidos.csv", PEDIDOS);
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: join-con-alias
nodes:
  - {{ id: leer_clientes_v2, type: source, connector: csv, config: {{ path: "{clientes}" }} }}
  - {{ id: leer_pedidos_v2, type: source, connector: csv, config: {{ path: "{pedidos}" }} }}
  - id: unir
    type: transform
    op: sql
    config:
      query: "SELECT count(*) AS filas FROM c JOIN p ON c.id = p.cliente_id"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer_clientes_v2, to: unir, port: c }}
  - {{ from: leer_pedidos_v2, to: unir, port: p }}
  - {{ from: unir, to: escribir }}
"#
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(read_lines(&out), vec!["filas".to_string(), "3".to_string()]);
}

#[tokio::test]
async fn dos_aristas_con_el_mismo_puerto_se_rechazan() {
    // Sin `port:`, ambas aristas se llamarían igual y el join sería ambiguo.
    let err = Dag::build(
        PipelineSpec::from_yaml_str(
            "test.yaml",
            r#"
name: puertos-repetidos
nodes:
  - { id: origen, type: source, connector: generator, config: { rows: 1 } }
  - { id: recortar, type: transform, op: limit, config: { rows: 1 } }
  - { id: unir, type: transform, op: sql, config: { query: "SELECT 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: origen, to: unir, port: mismo }
  - { from: recortar, to: unir, port: mismo }
  - { from: unir, to: descartar }
"#,
        )
        .expect("YAML válido"),
    )
    .expect_err("puertos duplicados");
    assert!(err.to_string().contains("mismo"), "{err}");
}

// --- validación estática de esquemas ---------------------------------------

#[tokio::test]
async fn una_columna_inexistente_se_detecta_en_validate() {
    // Antes esto sólo fallaba al ejecutar, cuando llegaba el primer lote.
    let err = validation_error(
        r#"
name: columna-mala
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 10, with_text: false } }
  - { id: q, type: transform, op: filter, config: { where: "no_existe > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: q }
  - { from: q, to: descartar }
"#,
    )
    .await;
    assert!(err.contains("no_existe"), "{err}");
}

#[tokio::test]
async fn un_select_sobre_una_columna_inexistente_se_detecta_en_validate() {
    let err = validation_error(
        r#"
name: select-malo
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 10, with_text: false } }
  - { id: recortar, type: transform, op: select, config: { columns: [id, fantasma] } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: recortar }
  - { from: recortar, to: descartar }
"#,
    )
    .await;
    assert!(err.contains("fantasma"), "{err}");
    assert!(err.contains("disponibles"), "{err}");
}

#[tokio::test]
async fn el_esquema_se_propaga_a_traves_de_varias_transformaciones() {
    // El error tiene que venir del último nodo, lo que prueba que el esquema
    // llegó hasta ahí atravesando select y rename.
    let err = validation_error(
        r#"
name: cadena
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 10 } }
  - { id: recortar, type: transform, op: select, config: { columns: [id, label] } }
  - { id: renombrar, type: transform, op: rename, config: { columns: { label: etiqueta } } }
  - { id: q, type: transform, op: filter, config: { where: "label = 'x'" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: generar, to: recortar }
  - { from: recortar, to: renombrar }
  - { from: renombrar, to: q }
  - { from: q, to: descartar }
"#,
    )
    .await;
    // `label` ya no existe tras el rename: sólo se puede saber si el esquema
    // se propagó.
    assert!(err.contains("label"), "{err}");
}

#[tokio::test]
async fn un_fan_in_con_esquemas_distintos_se_detecta_en_validate() {
    // Un CSV tiene una sola cabecera: no puede recibir dos formas distintas.
    // (El sink `null` sí las acepta: descarta todo, así que le da igual.)
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let err = validation_error(&format!(
        r#"
name: fan-in-incompatible
nodes:
  - {{ id: con_texto, type: source, connector: generator, config: {{ rows: 5, with_text: true }} }}
  - {{ id: sin_texto, type: source, connector: generator, config: {{ rows: 5, with_text: false }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: con_texto, to: escribir }}
  - {{ from: sin_texto, to: escribir }}
"#
    ))
    .await;
    assert!(err.contains("esquemas distintos"), "{err}");
    assert!(err.contains("label"), "{err}");
}

#[tokio::test]
async fn filter_con_dos_entradas_se_rechaza_con_un_mensaje_util() {
    let err = validation_error(
        r#"
name: filter-con-dos
nodes:
  - { id: a, type: source, connector: generator, config: { rows: 5, with_text: false } }
  - { id: b, type: source, connector: generator, config: { rows: 5, with_text: false } }
  - { id: q, type: transform, op: filter, config: { where: "id > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: a, to: q }
  - { from: b, to: q }
  - { from: q, to: descartar }
"#,
    )
    .await;
    assert!(err.contains("sólo admite una entrada"), "{err}");
    assert!(err.contains("sql"), "sugerir la alternativa: {err}");
}

#[tokio::test]
async fn con_esquema_estatico_un_count_sobre_cero_filas_devuelve_una_fila() {
    // Sin propagación de esquemas esto daba salida vacía, porque no había con
    // qué planificar la query.
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: cuenta-vacia
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 1000, with_text: false }} }}
  - {{ id: nada, type: transform, op: filter, config: {{ where: "id < 0" }} }}
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
    assert_eq!(read_lines(&out), vec!["filas".to_string(), "0".to_string()]);
}

#[tokio::test]
async fn un_csv_que_no_existe_no_invalida_el_pipeline() {
    // `validate` no puede exigir que las fuentes estén disponibles: el
    // fichero puede generarlo una ejecución anterior.
    Executor::new(registry())
        .prepare(&dag(r#"
name: fuente-futura
nodes:
  - { id: leer, type: source, connector: csv, config: { path: "./todavia/no/existe.csv" } }
  - { id: q, type: transform, op: filter, config: { where: "lo_que_sea > 1" } }
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: leer, to: q }
  - { from: q, to: descartar }
"#))
        .await
        .expect("sin esquema, la validación se detiene pero no falla");
}
