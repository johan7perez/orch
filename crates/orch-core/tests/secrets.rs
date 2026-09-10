//! Referencias a secretos en la config.
//!
//! Los tests usan variables de entorno con nombres propios y únicos: el
//! entorno es global al proceso y los tests corren en paralelo.

use orch_core::{NodeKind, PipelineSpec};

fn spec_with_config(config: &str) -> orch_core::Result<PipelineSpec> {
    PipelineSpec::from_yaml_str(
        "test.yaml",
        &format!(
            r#"
name: secretos
nodes:
  - {{ id: origen, type: source, connector: postgres, config: {config} }}
  - {{ id: destino, type: sink, connector: "null" }}
edges:
  - {{ from: origen, to: destino }}
"#
        ),
    )
}

fn config_of(spec: &PipelineSpec, id: &str) -> serde_json::Value {
    let node = spec.nodes.iter().find(|n| n.id == id).expect("nodo");
    node.kind.config().clone()
}

#[test]
fn expande_una_variable_de_entorno() {
    std::env::set_var("ORCH_TEST_PASSWORD", "s3cr3t");
    let spec = spec_with_config(r#"{ dsn: "postgres://u:${env:ORCH_TEST_PASSWORD}@host/db" }"#)
        .expect("debería resolver");
    assert_eq!(
        config_of(&spec, "origen")["dsn"],
        serde_json::json!("postgres://u:s3cr3t@host/db")
    );
}

#[test]
fn expande_dentro_de_listas_y_mapas_anidados() {
    std::env::set_var("ORCH_TEST_TOKEN", "abc123");
    let spec = spec_with_config(
        r#"{ headers: { Authorization: "Bearer ${env:ORCH_TEST_TOKEN}" }, tags: ["${env:ORCH_TEST_TOKEN}"] }"#,
    )
    .expect("debería resolver");
    let config = config_of(&spec, "origen");
    assert_eq!(config["headers"]["Authorization"], "Bearer abc123");
    assert_eq!(config["tags"][0], "abc123");
}

#[test]
fn varias_referencias_en_la_misma_cadena() {
    std::env::set_var("ORCH_TEST_HOST", "db.local");
    std::env::set_var("ORCH_TEST_PORT", "6543");
    let spec = spec_with_config(r#"{ dsn: "${env:ORCH_TEST_HOST}:${env:ORCH_TEST_PORT}/x" }"#)
        .expect("debería resolver");
    assert_eq!(config_of(&spec, "origen")["dsn"], "db.local:6543/x");
}

#[test]
fn una_variable_ausente_es_un_error_al_cargar() {
    let err = spec_with_config(r#"{ dsn: "${env:ORCH_TEST_NO_DEFINIDA_JAMAS}" }"#)
        .expect_err("la variable no existe");
    let message = err.to_string();
    assert!(message.contains("ORCH_TEST_NO_DEFINIDA_JAMAS"), "{message}");
    // El error debe señalar el nodo, para saber dónde mirar.
    assert!(message.contains("origen"), "{message}");
}

#[test]
fn un_origen_de_secreto_desconocido_es_un_error() {
    // Un `${ENV:X}` mal escrito acabaría en una cadena de conexión y fallaría
    // de forma incomprensible; mejor rechazarlo aquí.
    let err = spec_with_config(r#"{ dsn: "${ENV:ALGO}" }"#).expect_err("esquema desconocido");
    let message = err.to_string();
    assert!(message.contains("desconocido"), "{message}");
    assert!(
        message.contains("env"),
        "debería listar los soportados: {message}"
    );
}

#[test]
fn una_cadena_sin_referencias_no_se_toca() {
    let spec = spec_with_config(r#"{ dsn: "postgres://localhost/db", precio: "100$" }"#)
        .expect("sin referencias");
    let config = config_of(&spec, "origen");
    assert_eq!(config["dsn"], "postgres://localhost/db");
    assert_eq!(config["precio"], "100$");
}

#[test]
fn una_llave_sin_forma_de_referencia_se_deja_literal() {
    // `${HOME}` no lleva esquema: no es una referencia de Orch y puede que la
    // interprete el destino (una plantilla de URL, por ejemplo).
    let spec = spec_with_config(r#"{ url: "http://host/${HOME}/x", raro: "${sin cerrar" }"#)
        .expect("literales");
    let config = config_of(&spec, "origen");
    assert_eq!(config["url"], "http://host/${HOME}/x");
    assert_eq!(config["raro"], "${sin cerrar");
}

#[test]
fn el_tipo_de_nodo_no_afecta_a_la_expansion() {
    std::env::set_var("ORCH_TEST_RUTA", "/datos/x.csv");
    let spec = PipelineSpec::from_yaml_str(
        "test.yaml",
        r#"
name: secretos-en-sink
nodes:
  - { id: origen, type: source, connector: generator, config: { rows: 1 } }
  - { id: destino, type: sink, connector: csv, config: { path: "${env:ORCH_TEST_RUTA}" } }
edges:
  - { from: origen, to: destino }
"#,
    )
    .expect("debería resolver");

    let destino = spec.nodes.iter().find(|n| n.id == "destino").expect("nodo");
    assert!(matches!(destino.kind, NodeKind::Sink { .. }));
    assert_eq!(destino.kind.config()["path"], "/datos/x.csv");
}
