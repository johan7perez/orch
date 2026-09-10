//! Validación del pipeline: lo que el motor debe rechazar antes de abrir un
//! solo fichero.

use orch_core::{Dag, PipelineSpec};

fn build(yaml: &str) -> orch_core::Result<Dag> {
    Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml)?)
}

fn error_of(yaml: &str) -> String {
    build(yaml)
        .expect_err("se esperaba un pipeline inválido")
        .to_string()
}

const LINEAL: &str = r#"
name: lineal
nodes:
  - id: src
    type: source
    connector: generator
    config: { rows: 10 }
  - id: dst
    type: sink
    connector: "null"
edges:
  - { from: src, to: dst }
"#;

#[test]
fn acepta_un_pipeline_lineal() {
    let dag = build(LINEAL).expect("debería ser válido");
    assert_eq!(dag.len(), 2);
    let order: Vec<&str> = dag
        .topological_order()
        .iter()
        .map(|&i| dag.node(i).id.as_str())
        .collect();
    assert_eq!(order, vec!["src", "dst"]);
}

#[test]
fn el_orden_topologico_respeta_las_dependencias() {
    let dag = build(
        r#"
name: cadena
nodes:
  - { id: dst, type: sink, connector: "null" }
  - { id: mid, type: transform, op: limit, config: { rows: 1 } }
  - { id: src, type: source, connector: generator, config: { rows: 10 } }
edges:
  - { from: mid, to: dst }
  - { from: src, to: mid }
"#,
    )
    .expect("debería ser válido");

    let order: Vec<&str> = dag
        .topological_order()
        .iter()
        .map(|&i| dag.node(i).id.as_str())
        .collect();
    // Declarados al revés en el YAML, pero ordenados por dependencia.
    assert_eq!(order, vec!["src", "mid", "dst"]);
}

#[test]
fn rechaza_ids_duplicados() {
    let err = error_of(
        r#"
name: duplicados
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 } }
  - { id: src, type: sink, connector: "null" }
edges:
  - { from: src, to: src }
"#,
    );
    assert!(err.contains("duplicado"), "mensaje inesperado: {err}");
}

#[test]
fn rechaza_aristas_a_nodos_inexistentes() {
    let err = error_of(
        r#"
name: fantasma
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 } }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: src, to: dst }
  - { from: src, to: nadie }
"#,
    );
    assert!(err.contains("nadie"), "mensaje inesperado: {err}");
}

#[test]
fn rechaza_ciclos() {
    let err = error_of(
        r#"
name: ciclo
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 } }
  - { id: a, type: transform, op: limit, config: { rows: 1 } }
  - { id: b, type: transform, op: limit, config: { rows: 1 } }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: src, to: a }
  - { from: a, to: b }
  - { from: b, to: a }
  - { from: b, to: dst }
"#,
    );
    assert!(err.contains("ciclo"), "mensaje inesperado: {err}");
}

#[test]
fn rechaza_un_source_con_entradas() {
    let err = error_of(
        r#"
name: source-con-entrada
nodes:
  - { id: a, type: source, connector: generator, config: { rows: 1 } }
  - { id: b, type: source, connector: generator, config: { rows: 1 } }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: a, to: b }
  - { from: b, to: dst }
"#,
    );
    assert!(
        err.contains("no puede tener entradas"),
        "mensaje inesperado: {err}"
    );
}

#[test]
fn rechaza_un_transform_que_no_va_a_ninguna_parte() {
    let err = error_of(
        r#"
name: colgando
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 } }
  - { id: mid, type: transform, op: limit, config: { rows: 1 } }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: src, to: mid }
  - { from: src, to: dst }
"#,
    );
    assert!(
        err.contains("no van a ninguna parte"),
        "mensaje inesperado: {err}"
    );
}

#[test]
fn rechaza_un_after_a_un_nodo_inexistente() {
    let err = error_of(
        r#"
name: barrera-rota
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 }, after: [fantasma] }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: src, to: dst }
"#,
    );
    assert!(err.contains("fantasma"), "mensaje inesperado: {err}");
}

#[test]
fn una_barrera_tambien_puede_formar_ciclo() {
    let err = error_of(
        r#"
name: barrera-ciclica
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 }, after: [dst] }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: src, to: dst }
"#,
    );
    assert!(err.contains("ciclo"), "mensaje inesperado: {err}");
}

#[test]
fn rechaza_una_version_de_formato_futura() {
    let err = error_of(
        r#"
version: 99
name: del-futuro
nodes:
  - { id: src, type: source, connector: generator, config: { rows: 1 } }
  - { id: dst, type: sink, connector: "null" }
edges:
  - { from: src, to: dst }
"#,
    );
    assert!(err.contains("99"), "mensaje inesperado: {err}");
}
