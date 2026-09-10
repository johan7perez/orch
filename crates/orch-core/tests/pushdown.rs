//! Empuje de operaciones hacia el origen.
//!
//! La regla que hay que no romper: el pipeline reescrito tiene que dar
//! exactamente el mismo resultado. Aquí se comprueba *cuándo* se reescribe;
//! que el resultado no cambie se comprueba en los tests de cada conector.

use std::sync::Arc;

use orch_core::pushdown::{self, PushdownOp};
use orch_core::{PipelineSpec, Registry, Source};
use serde_json::{json, Value};

/// Registro con un origen ficticio que acepta lo que se le empuje.
fn registry() -> Registry {
    let mut registry = Registry::new();
    registry.register_source("absorbente", |_node, _config| {
        unreachable!("estos tests no ejecutan nada")
    });
    registry.register_source("terco", |_node, _config| {
        unreachable!("estos tests no ejecutan nada")
    });
    registry.register_pushdown("absorbente", |config, op| {
        let Some(map) = config.as_object_mut() else {
            return false;
        };
        match op {
            PushdownOp::Select { columns } => {
                map.insert("columns".to_string(), json!(columns));
                true
            }
            PushdownOp::Filter { predicate } => {
                map.insert("where".to_string(), json!(predicate));
                true
            }
        }
    });
    registry
}

fn rewrite(yaml: &str) -> (PipelineSpec, Vec<orch_core::Pushed>) {
    let mut spec = PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido");
    let pushed = pushdown::apply(&mut spec, &registry());
    (spec, pushed)
}

fn config_of(spec: &PipelineSpec, id: &str) -> Value {
    spec.nodes
        .iter()
        .find(|n| n.id == id)
        .unwrap_or_else(|| panic!("no existe el nodo `{id}`"))
        .kind
        .config()
        .clone()
}

fn node_ids(spec: &PipelineSpec) -> Vec<&str> {
    spec.nodes.iter().map(|n| n.id.as_str()).collect()
}

// --- lo que sí se empuja ----------------------------------------------------

#[test]
fn un_select_pegado_al_origen_desaparece() {
    let (spec, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: recortar, type: transform, op: select, config: { columns: [a, b] } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: recortar, to: destino }
"#,
    );

    assert_eq!(node_ids(&spec), vec!["origen", "destino"]);
    assert_eq!(config_of(&spec, "origen")["columns"], json!(["a", "b"]));
    assert_eq!(pushed.len(), 1);
    assert_eq!(pushed[0].op, "select");
    assert_eq!(pushed[0].into, "origen");
    assert_eq!(pushed[0].removed, "recortar");
    // La arista del nodo eliminado pasa a salir del origen.
    assert_eq!(spec.edges.len(), 1);
    assert_eq!(spec.edges[0].from, "origen");
    assert_eq!(spec.edges[0].to, "destino");
}

#[test]
fn un_filter_pegado_al_origen_tambien() {
    let (spec, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: filtrar, type: transform, op: filter, config: { where: "a > 1" } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: filtrar }
  - { from: filtrar, to: destino }
"#,
    );

    assert_eq!(node_ids(&spec), vec!["origen", "destino"]);
    assert_eq!(config_of(&spec, "origen")["where"], "a > 1");
    assert_eq!(pushed.len(), 1);
}

#[test]
fn se_empujan_varios_en_cadena() {
    // Absorber el primero deja el segundo pegado al origen.
    let (spec, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: filtrar, type: transform, op: filter, config: { where: "a > 1" } }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: filtrar }
  - { from: filtrar, to: recortar }
  - { from: recortar, to: destino }
"#,
    );

    assert_eq!(node_ids(&spec), vec!["origen", "destino"]);
    assert_eq!(pushed.len(), 2);
    let config = config_of(&spec, "origen");
    assert_eq!(config["where"], "a > 1");
    assert_eq!(config["columns"], json!(["a"]));
}

#[test]
fn el_puerto_conserva_el_nombre_del_nodo_eliminado() {
    // Un nodo `sql` aguas abajo registra sus entradas por nombre de puerto;
    // si el puerto cambiara al desaparecer el nodo, su query se rompería.
    let (spec, _) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: unir, type: transform, op: sql, config: { query: "SELECT * FROM recortar" } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: recortar, to: unir }
  - { from: unir, to: destino }
"#,
    );

    let edge = spec
        .edges
        .iter()
        .find(|e| e.to == "unir")
        .expect("la arista hacia `unir`");
    assert_eq!(edge.from, "origen");
    assert_eq!(edge.port_name(), "recortar", "el puerto no debe cambiar");
}

// --- lo que no se empuja ----------------------------------------------------

#[test]
fn no_se_empuja_si_el_origen_alimenta_a_alguien_mas() {
    // Recortarle las columnas al origen cambiaría lo que ve la otra rama.
    let (spec, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: completo, type: sink, connector: "null" }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: origen, to: completo }
  - { from: recortar, to: destino }
"#,
    );

    assert!(pushed.is_empty());
    assert_eq!(
        node_ids(&spec),
        vec!["origen", "recortar", "completo", "destino"]
    );
}

#[test]
fn no_se_empuja_a_un_conector_que_no_lo_soporta() {
    let (spec, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: terco, config: {} }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: recortar, to: destino }
"#,
    );

    assert!(pushed.is_empty());
    assert_eq!(node_ids(&spec).len(), 3);
}

#[test]
fn no_se_empuja_lo_que_no_esta_pegado_al_origen() {
    let (_, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: medio, type: transform, op: limit, config: { rows: 10 } }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: medio }
  - { from: medio, to: recortar }
  - { from: recortar, to: destino }
"#,
    );

    // `limit` cambia cuántas filas pasan: adelantar el `select` es inocuo,
    // pero adelantarlo *a través* de un nodo no soportado no se intenta.
    assert!(pushed.is_empty());
}

#[test]
fn no_se_empuja_un_nodo_con_barrera() {
    // El `after` es una dependencia explícita: si el nodo desaparece, se
    // perdería el orden que el usuario pidió.
    let (_, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: previo, type: source, connector: absorbente, config: {} }
  - { id: aparte, type: sink, connector: "null" }
  - { id: origen, type: source, connector: absorbente, config: {} }
  - id: recortar
    type: transform
    op: select
    after: [aparte]
    config: { columns: [a] }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: previo, to: aparte }
  - { from: origen, to: recortar }
  - { from: recortar, to: destino }
"#,
    );

    assert!(pushed.is_empty());
}

#[test]
fn no_se_empuja_si_otro_nodo_depende_del_que_desaparece() {
    let (_, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: destino, type: sink, connector: "null" }
  - { id: otro, type: source, connector: terco, after: [recortar], config: {} }
  - { id: fin, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: recortar, to: destino }
  - { from: otro, to: fin }
"#,
    );

    assert!(pushed.is_empty());
}

#[test]
fn no_se_empuja_un_filter_con_tabla_propia() {
    // `table` significa que la query no habla de la entrada directa.
    let (_, pushed) = rewrite(
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: filtrar, type: transform, op: filter, config: { where: "a > 1", table: otra } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: filtrar }
  - { from: filtrar, to: destino }
"#,
    );

    assert!(pushed.is_empty());
}

#[test]
fn un_registro_sin_pushdown_no_reescribe_nada() {
    let mut spec = PipelineSpec::from_yaml_str(
        "test.yaml",
        r#"
name: p
nodes:
  - { id: origen, type: source, connector: absorbente, config: {} }
  - { id: recortar, type: transform, op: select, config: { columns: [a] } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: recortar }
  - { from: recortar, to: destino }
"#,
    )
    .expect("YAML válido");

    let pushed = pushdown::apply(&mut spec, &Registry::new());
    assert!(pushed.is_empty());
    assert_eq!(spec.nodes.len(), 3);
}

/// Silencia el aviso de import sin usar: `Source` sólo aparece en la firma
/// de los factories del registro de prueba.
#[allow(dead_code)]
fn _tipo_usado(_: Option<Arc<dyn Source>>) {}
