//! Resolución de rutas relativas del pipeline.
//!
//! Las rutas de `path` cuelgan de la carpeta del fichero YAML y no del
//! directorio de trabajo del proceso, que en una aplicación de escritorio es
//! cualquier cosa.

use std::path::{Path, PathBuf};

use orch_core::{NodeKind, PipelineSpec};

fn spec(config_origen: &str, config_destino: &str) -> PipelineSpec {
    PipelineSpec::from_yaml_str(
        "test.yaml",
        &format!(
            r#"
name: rutas
nodes:
  - {{ id: origen, type: source, connector: csv, config: {config_origen} }}
  - {{ id: destino, type: sink, connector: csv, config: {config_destino} }}
edges:
  - {{ from: origen, to: destino }}
"#
        ),
    )
    .expect("YAML válido")
}

fn ruta_de(spec: &PipelineSpec, id: &str) -> Option<PathBuf> {
    let nodo = spec.nodes.iter().find(|n| n.id == id)?;
    let (NodeKind::Source { config, .. }
    | NodeKind::Transform { config, .. }
    | NodeKind::Sink { config, .. }) = &nodo.kind;
    Some(PathBuf::from(config.get("path")?.as_str()?))
}

#[test]
fn una_ruta_relativa_cuelga_de_la_carpeta_del_pipeline() {
    let mut spec = spec(
        "{ path: ../data/customers.csv }",
        "{ path: ../out/salida.csv }",
    );
    spec.resolve_paths(Path::new("/proyecto/examples/pipelines"));

    assert_eq!(
        ruta_de(&spec, "origen").unwrap(),
        Path::new("/proyecto/examples/pipelines/../data/customers.csv"),
    );
    assert_eq!(
        ruta_de(&spec, "destino").unwrap(),
        Path::new("/proyecto/examples/pipelines/../out/salida.csv"),
    );
}

#[test]
fn una_ruta_absoluta_se_deja_como_esta() {
    // Quien escribe una ruta absoluta sabe lo que quiere: no se toca.
    let absoluta = if cfg!(windows) {
        r"C:\datos\entrada.csv"
    } else {
        "/datos/entrada.csv"
    };
    let mut spec = spec(&format!("{{ path: '{absoluta}' }}"), "{ path: salida.csv }");
    spec.resolve_paths(Path::new("/otra/carpeta"));

    assert_eq!(ruta_de(&spec, "origen").unwrap(), Path::new(absoluta));
}

#[test]
fn un_nodo_sin_ruta_no_estorba() {
    // `generator` no tiene `path`; resolver no debe inventarle uno.
    let mut spec = PipelineSpec::from_yaml_str(
        "test.yaml",
        r#"
name: sin-rutas
nodes:
  - { id: origen, type: source, connector: generator, config: { rows: 10 } }
  - { id: destino, type: sink, connector: csv, config: { path: salida.csv } }
edges:
  - { from: origen, to: destino }
"#,
    )
    .expect("YAML válido");
    spec.resolve_paths(Path::new("/base"));

    assert_eq!(ruta_de(&spec, "origen"), None);
    assert_eq!(
        ruta_de(&spec, "destino").unwrap(),
        Path::new("/base/salida.csv"),
    );
}

#[test]
fn from_path_resuelve_contra_el_fichero_y_no_contra_el_cwd() {
    // El test de verdad: cargar desde disco desde un cwd que no tiene nada
    // que ver con la carpeta del pipeline.
    let carpeta = std::env::temp_dir().join(format!("orch-rutas-{}", std::process::id()));
    let pipelines = carpeta.join("pipelines");
    std::fs::create_dir_all(&pipelines).expect("crear carpeta");
    let yaml = pipelines.join("p.yaml");
    std::fs::write(
        &yaml,
        r#"
name: desde-disco
nodes:
  - { id: origen, type: source, connector: csv, config: { path: ../data/entrada.csv } }
  - { id: destino, type: sink, connector: "null" }
edges:
  - { from: origen, to: destino }
"#,
    )
    .expect("escribir pipeline");

    let spec = PipelineSpec::from_path(&yaml).expect("pipeline válido");
    assert_eq!(
        ruta_de(&spec, "origen").unwrap(),
        pipelines.join("../data/entrada.csv"),
    );

    std::fs::remove_dir_all(&carpeta).ok();
}
