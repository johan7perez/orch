//! El diff de pipelines: qué cambió, dicho como pipeline.

use orch_core::PipelineSpec;
use orch_version::diff::{line_diff, show_value};
use orch_version::{diff, Change};
use serde_json::json;

fn spec(yaml: &str) -> PipelineSpec {
    PipelineSpec::from_yaml_str_as_written("test.yaml", yaml).expect("YAML válido")
}

const BASE: &str = r#"
name: pedidos
schedule: { cron: "0 2 * * *" }
nodes:
  - { id: leer, type: source, connector: csv, config: { path: pedidos.csv } }
  - { id: pagados, type: transform, op: filter, config: { where: "status = 'pagado'" } }
  - { id: escribir, type: sink, connector: csv, config: { path: out.csv } }
edges:
  - { from: leer, to: pagados }
  - { from: pagados, to: escribir }
"#;

#[test]
fn lo_mismo_escrito_de_otra_forma_no_es_un_cambio() {
    // Otro orden de claves, comentarios, y un valor por defecto escrito a mano.
    let otra_forma = r#"
# Comentario nuevo
name: pedidos
nodes:
  - id: leer
    connector: csv
    type: source
    config: { path: pedidos.csv }
  - id: pagados
    type: transform
    op: filter
    retry: { max_attempts: 1 }
    config: { where: "status = 'pagado'" }
  - { id: escribir, type: sink, connector: csv, config: { path: out.csv } }
edges:
  - { from: leer, to: pagados, port: leer }
  - { from: pagados, to: escribir }
schedule: { cron: "0 2 * * *" }
"#;
    assert_eq!(diff(&spec(BASE), &spec(otra_forma)), vec![]);
}

#[test]
fn un_campo_de_config_se_nombra_hasta_la_hoja() {
    let nuevo = BASE.replace("status = 'pagado'", "status IN ('pagado', 'enviado')");
    assert_eq!(
        diff(&spec(BASE), &spec(&nuevo)),
        vec![Change::Node {
            node: "pagados".into(),
            field: "config.where".into(),
            before: Some(json!("status = 'pagado'")),
            after: Some(json!("status IN ('pagado', 'enviado')")),
        }]
    );
}

#[test]
fn nodos_y_aristas_que_entran_y_salen() {
    let nuevo = r#"
name: pedidos
schedule: { cron: "0 2 * * *" }
nodes:
  - { id: leer, type: source, connector: csv, config: { path: pedidos.csv } }
  - { id: primeros, type: transform, op: limit, config: { rows: 10 } }
  - { id: escribir, type: sink, connector: csv, config: { path: out.csv } }
edges:
  - { from: leer, to: primeros }
  - { from: primeros, to: escribir }
"#;
    let cambios = diff(&spec(BASE), &spec(nuevo));
    assert_eq!(
        cambios,
        vec![
            Change::NodeRemoved {
                node: "pagados".into(),
                kind: "transform".into(),
                component: "filter".into()
            },
            Change::NodeAdded {
                node: "primeros".into(),
                kind: "transform".into(),
                component: "limit".into()
            },
            Change::EdgeRemoved {
                from: "leer".into(),
                to: "pagados".into(),
                port: "leer".into()
            },
            Change::EdgeRemoved {
                from: "pagados".into(),
                to: "escribir".into(),
                port: "pagados".into()
            },
            Change::EdgeAdded {
                from: "leer".into(),
                to: "primeros".into(),
                port: "leer".into()
            },
            Change::EdgeAdded {
                from: "primeros".into(),
                to: "escribir".into(),
                port: "primeros".into()
            },
        ]
    );
}

#[test]
fn un_renombrado_no_arrastra_sus_aristas() {
    let nuevo = BASE.replace("pagados", "solo_pagados");
    assert_eq!(
        diff(&spec(BASE), &spec(&nuevo)),
        vec![Change::NodeRenamed {
            from: "pagados".into(),
            to: "solo_pagados".into()
        }]
    );

    // Renombrado y además cambiado: ya no es el mismo nodo.
    let cambiado = nuevo.replace("status = 'pagado'", "status = 'enviado'");
    let cambios = diff(&spec(BASE), &spec(&cambiado));
    assert!(cambios
        .iter()
        .any(|c| matches!(c, Change::NodeRemoved { node, .. } if node == "pagados")));
    assert!(cambios
        .iter()
        .any(|c| matches!(c, Change::NodeAdded { node, .. } if node == "solo_pagados")));
}

#[test]
fn la_programacion_y_las_alertas_tambien_cuentan() {
    let nuevo = BASE.replace("0 2 * * *", "0 3 * * *").replace(
        "name: pedidos",
        "name: pedidos\nalerts: { stalled_after: 10m }",
    );
    let cambios = diff(&spec(BASE), &spec(&nuevo));
    assert!(
        cambios.contains(&Change::Pipeline {
            field: "schedule.cron".into(),
            before: Some(json!("0 2 * * *")),
            after: Some(json!("0 3 * * *")),
        }),
        "{cambios:?}"
    );
    // Un bloque que no estaba se da entero.
    assert!(
        cambios.iter().any(|c| matches!(
            c,
            Change::Pipeline { field, before: None, after: Some(_) } if field == "alerts"
        )),
        "{cambios:?}"
    );
}

#[test]
fn mover_nodos_en_el_lienzo_se_dice_aparte() {
    let movido = format!("{BASE}layout:\n  leer: [0, 0]\n  pagados: [200, 0]\n");
    let cambios = diff(&spec(BASE), &spec(&movido));
    assert_eq!(
        cambios,
        vec![Change::Layout {
            nodes: vec!["leer".into(), "pagados".into()]
        }]
    );
    assert!(!cambios[0].is_semantic());

    // Un nodo nuevo trae su posición: eso no es moverlo.
    let con_nuevo = movido
        .replace(
            "edges:",
            "  - { id: extra, type: sink, connector: \"null\" }\nedges:\n  - { from: pagados, to: extra }",
        )
        .replace("pagados: [200, 0]", "pagados: [200, 0]\n  extra: [400, 0]");
    let cambios = diff(&spec(&movido), &spec(&con_nuevo));
    assert!(
        !cambios.iter().any(|c| matches!(c, Change::Layout { .. })),
        "{cambios:?}"
    );
}

#[test]
fn una_query_de_varias_lineas_se_compara_por_lineas() {
    let antes = "SELECT a,\n       b\nFROM input\nWHERE a > 1";
    let ahora = "SELECT a,\n       b,\n       c\nFROM input\nWHERE a > 1";
    assert_eq!(
        line_diff(antes, ahora),
        vec![
            (' ', "SELECT a,"),
            ('-', "       b"),
            ('+', "       b,"),
            ('+', "       c"),
            (' ', "FROM input"),
            (' ', "WHERE a > 1"),
        ]
    );
    assert_eq!(show_value(&json!("x > 1")), "\"x > 1\"");
    assert_eq!(show_value(&json!([1, 2])), "[1,2]");
}
