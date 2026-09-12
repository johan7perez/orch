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
fn lee_una_credencial_del_almacen_del_sistema() {
    // Crea una credencial con un nombre propio, la lee por el pipeline y la
    // borra. Si el almacén no está disponible (contenedor sin sesión de
    // escritorio), se salta.
    let service = "orch-test-suite";
    let user = "pipeline";
    let entry = match keyring::Entry::new(service, user) {
        Ok(entry) => entry,
        Err(err) => {
            eprintln!("almacén de credenciales no disponible ({err}); se salta el test");
            return;
        }
    };
    if let Err(err) = entry.set_password("clave-de-prueba") {
        eprintln!("no se pudo escribir en el almacén ({err}); se salta el test");
        return;
    }

    let resolved = spec_with_config(&format!(r#"{{ dsn: "${{keyring:{service}/{user}}}" }}"#));
    let _ = entry.delete_credential();

    let spec = resolved.expect("debería resolver");
    assert_eq!(config_of(&spec, "origen")["dsn"], "clave-de-prueba");
}

#[test]
fn una_referencia_de_keyring_mal_formada_es_un_error() {
    let err = spec_with_config(r#"{ dsn: "${keyring:sin-barra}" }"#).expect_err("falta el usuario");
    assert!(
        err.to_string().contains("servicio/usuario"),
        "{}",
        err.to_string()
    );
}

#[test]
fn una_credencial_inexistente_es_un_error_al_cargar() {
    let err = spec_with_config(r#"{ dsn: "${keyring:orch-no-existe-jamas/nadie}" }"#)
        .expect_err("la credencial no existe");
    let message = err.to_string();
    assert!(message.contains("orch-no-existe-jamas/nadie"), "{message}");
    assert!(
        message.contains("origen"),
        "debería señalar el nodo: {message}"
    );
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

/// El diseñador carga con `from_path_as_written`, y esto es lo que separa
/// enseñar `${env:...}` de enseñar la contraseña en pantalla.
#[test]
fn el_disenador_no_expande_secretos_y_por_eso_abre_lo_que_no_se_puede_ejecutar() {
    // Una variable que no existe a propósito: es el caso real de un
    // `postgres.yaml` en una máquina sin el DSN configurado.
    let yaml = r#"
name: sin-el-secreto
nodes:
  - { id: origen, type: source, connector: postgres, config: { dsn: "${env:ORCH_TEST_QUE_NO_EXISTE}" } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: destino }
"#;
    let carpeta = std::env::temp_dir().join(format!("orch-disenador-{}", std::process::id()));
    std::fs::create_dir_all(&carpeta).expect("crear carpeta");
    let ruta = carpeta.join("p.yaml");
    std::fs::write(&ruta, yaml).expect("escribir pipeline");

    // Para ejecutar no sirve, y debe decirlo.
    assert!(PipelineSpec::from_path(&ruta).is_err());

    // Para dibujarlo sí, y lo que se ve es lo que el fichero dice.
    let spec = PipelineSpec::from_path_as_written(&ruta).expect("debería abrirse igual");
    assert_eq!(
        config_of(&spec, "origen")["dsn"],
        "${env:ORCH_TEST_QUE_NO_EXISTE}"
    );

    std::fs::remove_dir_all(&carpeta).ok();
}
