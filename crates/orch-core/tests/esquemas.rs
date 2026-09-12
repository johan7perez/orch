//! El esquema de config de un componente.
//!
//! Sale del mismo tipo que deserializa la config, así que estos tests fijan
//! sobre todo que las dos cosas siguen siendo la misma definición.

use std::sync::Arc;

use orch_core::{NoConfig, Registry, Source};

// Los campos no se leen: este struct existe para que salga un esquema de él,
// que es justo lo que se está probando.
#[allow(dead_code)]
#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConfigDePrueba {
    /// Fichero del que leer.
    path: String,
    /// Filas por lote.
    #[serde(default = "mil")]
    batch_size: usize,
}

fn mil() -> usize {
    1000
}

struct Falso;

#[async_trait::async_trait]
impl Source for Falso {
    fn connector(&self) -> &str {
        "falso"
    }
    async fn read(
        &self,
        _ctx: &orch_core::NodeContext,
        _out: &orch_core::Output,
    ) -> orch_core::Result<()> {
        Ok(())
    }
}

fn registro() -> Registry {
    let mut registry = Registry::new();
    registry.register_source("falso", |_node, _config: ConfigDePrueba| {
        let source: Arc<dyn Source> = Arc::new(Falso);
        Ok(source)
    });
    registry.register_source("sin_config", |_node, _config: NoConfig| {
        let source: Arc<dyn Source> = Arc::new(Falso);
        Ok(source)
    });
    registry
}

#[test]
fn el_esquema_lleva_tipos_obligatorios_y_valores_por_defecto() {
    let registry = registro();
    let esquema = registry.schema("source", "falso").expect("hay esquema");

    let props = esquema["properties"].as_object().expect("propiedades");
    assert_eq!(props["path"]["type"], "string");
    assert_eq!(props["batch_size"]["default"], 1000);

    let required = esquema["required"].as_array().expect("obligatorios");
    assert!(required.contains(&serde_json::json!("path")));
    assert!(
        !required.contains(&serde_json::json!("batch_size")),
        "un campo con `default` no es obligatorio"
    );
}

#[test]
fn la_descripcion_sale_de_los_comentarios_del_struct() {
    // Es lo que hace que no haya documentación separada que envejezca.
    let registry = registro();
    let esquema = registry.schema("source", "falso").expect("hay esquema");
    assert_eq!(
        esquema["properties"]["path"]["description"],
        "Fichero del que leer."
    );
}

#[test]
fn deny_unknown_fields_viaja_al_esquema() {
    // El inspector se apoya en esto para marcar una clave que no existe.
    let registry = registro();
    let esquema = registry.schema("source", "falso").expect("hay esquema");
    assert_eq!(esquema["additionalProperties"], false);
}

#[test]
fn un_componente_sin_registrar_no_tiene_esquema() {
    assert!(registro().schema("source", "inventado").is_none());
    // Y el tipo tampoco se confunde: `falso` es un origen, no un destino.
    assert!(registro().schema("sink", "falso").is_none());
}

#[test]
fn un_nodo_sin_config_se_construye() {
    // Antes llegaba como `null` y serde lo rechazaba donde esperaba un
    // struct, así que hasta un componente sin campos exigía `config: {}`.
    let registry = registro();
    assert!(registry
        .build_source("sin_config", "n", &serde_json::Value::Null)
        .is_ok());
}

#[test]
fn una_config_con_un_campo_que_no_existe_se_rechaza() {
    let registry = registro();
    let err = registry
        .build_source("sin_config", "n", &serde_json::json!({ "vaya": 1 }))
        .err()
        .expect("`sin_config` no acepta campos");
    // El error se atribuye al nodo, que es lo que el usuario puede localizar.
    assert!(err.to_string().contains('n'), "{err}");
}

#[test]
fn una_config_a_la_que_le_falta_un_obligatorio_se_rechaza() {
    let registry = registro();
    let err = registry
        .build_source("falso", "leer", &serde_json::json!({ "batch_size": 10 }))
        .err()
        .expect("falta `path`");
    assert!(err.to_string().contains("path"), "{err}");
}
