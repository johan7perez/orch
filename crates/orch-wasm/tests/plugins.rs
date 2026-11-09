//! Plugins de verdad, cargados desde sus `.wasm` y ejecutados en pipelines.
//!
//! Necesitan los plugins compilados:
//!
//!     cargo build --release --target wasm32-wasip2 \
//!         -p orch-plugin-identidad -p orch-plugin-mayusculas -p orch-plugin-fisgon
//!
//! Si no están, cada test se salta solo y lo dice (`-- --nocapture`), como
//! los de PostgreSQL sin servidor: compilar a WASM desde un test bloquearía el
//! mismo directorio `target` que está usando `cargo test`.

use std::path::PathBuf;
use std::sync::Arc;

use orch_core::{Dag, Executor, NodeStatus, PipelineSpec, Registry, RunReport};
use orch_plugin::Transformacion;
use orch_wasm::Plugin;
use tempfile::TempDir;

fn ruta_plugin(nombre: &str) -> Option<PathBuf> {
    let ruta = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/wasm32-wasip2/release")
        .join(format!("orch_plugin_{nombre}.wasm"));
    ruta.exists().then_some(ruta)
}

/// Un motor para todos los tests del proceso: así cada plugin se compila una
/// vez y no una por test, que en debug son decenas de segundos cada una.
fn motor() -> &'static wasmtime::Engine {
    static MOTOR: std::sync::OnceLock<wasmtime::Engine> = std::sync::OnceLock::new();
    MOTOR.get_or_init(|| orch_wasm::motor().expect("motor de WebAssembly"))
}

/// El registro nativo más los plugins pedidos, o `None` si falta alguno.
fn registro(nombres: &[&str]) -> Option<Arc<Registry>> {
    let mut registro = orch_connectors::default_registry();
    let mut plugins = Vec::new();
    for nombre in nombres {
        let Some(ruta) = ruta_plugin(nombre) else {
            eprintln!("el plugin `{nombre}` no está compilado a WASM; se salta el test");
            return None;
        };
        plugins.push(Plugin::cargar(motor(), &ruta).expect("el plugin debería cargar"));
    }
    orch_wasm::registrar(&mut registro, plugins);
    Some(Arc::new(registro))
}

async fn ejecutar(registro: Arc<Registry>, yaml: &str) -> RunReport {
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido");
    Executor::new(registro)
        .run(&dag)
        .await
        .expect("la ejecución debería arrancar")
}

fn csv(dir: &TempDir) -> (String, String) {
    let entrada = dir.path().join("clientes.csv");
    std::fs::write(
        &entrada,
        "id,name,city\n1,Ana,Santo Domingo\n2,Luis,Santiago\n",
    )
    .unwrap();
    let salida = dir.path().join("salida.csv");
    (
        entrada.to_string_lossy().replace('\\', "/"),
        salida.to_string_lossy().replace('\\', "/"),
    )
}

/// Un pipeline `csv → <transformaciones> → csv`, con las transformaciones
/// encadenadas en el orden dado.
fn pipeline(entrada: &str, salida: &str, pasos: &[(&str, &str, &str)]) -> String {
    let mut nodos = format!(
        "  - {{ id: leer, type: source, connector: csv, config: {{ path: \"{entrada}\" }} }}\n"
    );
    let mut aristas = String::new();
    let mut anterior = "leer".to_string();
    for (id, op, config) in pasos {
        nodos.push_str(&format!(
            "  - {{ id: {id}, type: transform, op: {op}, config: {config} }}\n"
        ));
        aristas.push_str(&format!("  - {{ from: {anterior}, to: {id} }}\n"));
        anterior = id.to_string();
    }
    nodos.push_str(&format!(
        "  - {{ id: escribir, type: sink, connector: csv, config: {{ path: \"{salida}\" }} }}\n"
    ));
    aristas.push_str(&format!("  - {{ from: {anterior}, to: escribir }}\n"));
    format!("name: con-plugins\nnodes:\n{nodos}edges:\n{aristas}")
}

#[test]
fn un_plugin_se_registra_con_su_nombre_y_el_esquema_de_su_config() {
    let Some(registro) = registro(&["mayusculas"]) else {
        return;
    };
    assert!(registro.transform_names().contains(&"mayusculas"));
    let esquema = registro
        .schema("transform", "mayusculas")
        .expect("trae esquema");
    let columna = &esquema["properties"]["columna"];
    assert_eq!(columna["type"], "string");
    // La descripción sale del comentario `///` del campo, dentro del plugin.
    assert!(
        columna["description"]
            .as_str()
            .unwrap_or("")
            .contains("mayúsculas"),
        "{esquema}"
    );
    assert_eq!(esquema["required"][0], "columna");
}

#[tokio::test]
async fn un_pipeline_con_plugins_funciona_de_extremo_a_extremo() {
    let Some(registro) = registro(&["identidad", "mayusculas"]) else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let (entrada, salida) = csv(&dir);
    // Dos nodos del mismo plugin, cada uno con su instancia y su config.
    let informe = ejecutar(
        registro,
        &pipeline(
            &entrada,
            &salida,
            &[
                ("nada", "identidad", "{}"),
                ("nombre", "mayusculas", "{ columna: name }"),
                ("ciudad", "mayusculas", "{ columna: city }"),
            ],
        ),
    )
    .await;
    assert!(informe.succeeded, "{informe:#?}");
    assert_eq!(informe.rows_written(), 2);
    let escrito = std::fs::read_to_string(dir.path().join("salida.csv")).unwrap();
    assert_eq!(
        escrito,
        "id,name,city\n1,ANA,SANTO DOMINGO\n2,LUIS,SANTIAGO\n"
    );
}

#[tokio::test]
async fn una_config_que_el_plugin_rechaza_es_un_error_de_config_del_nodo() {
    let Some(registro) = registro(&["mayusculas"]) else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let (entrada, salida) = csv(&dir);
    let informe = ejecutar(
        registro,
        &pipeline(
            &entrada,
            &salida,
            &[("nombre", "mayusculas", "{ columnas: name }")],
        ),
    )
    .await;
    assert!(!informe.succeeded);
    let nodo = informe.nodes.iter().find(|n| n.id == "nombre").unwrap();
    assert_eq!(nodo.status, NodeStatus::Failed);
    let error = nodo.error.as_deref().unwrap_or("");
    assert!(
        error.contains("config inválida en el nodo `nombre`"),
        "{error}"
    );
    assert!(error.contains("columnas"), "{error}");
}

#[tokio::test]
async fn el_error_de_un_plugin_se_atribuye_a_su_nodo() {
    let Some(registro) = registro(&["mayusculas"]) else {
        return;
    };
    let dir = TempDir::new().unwrap();
    let (entrada, salida) = csv(&dir);
    let informe = ejecutar(
        registro,
        &pipeline(
            &entrada,
            &salida,
            &[("nombre", "mayusculas", "{ columna: apellido }")],
        ),
    )
    .await;
    let nodo = informe.nodes.iter().find(|n| n.id == "nombre").unwrap();
    assert_eq!(nodo.status, NodeStatus::Failed);
    let error = nodo.error.as_deref().unwrap_or("");
    assert!(error.contains("el nodo `nombre` falló"), "{error}");
    assert!(error.contains("la columna `apellido` no existe"), "{error}");
}

#[tokio::test]
async fn un_plugin_no_puede_leer_ficheros_conectarse_ni_ver_el_entorno() {
    let Some(registro) = registro(&["fisgon"]) else {
        return;
    };
    // Un fichero que existe, un puerto que escucha y una variable que está:
    // fuera del sandbox, los tres intentos salen bien.
    let fichero = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("Cargo.toml")
        .to_string_lossy()
        .replace('\\', "/");
    let escucha = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let conexion = escucha.local_addr().unwrap().to_string();
    let variable = "PATH";

    let config = orch_plugin_fisgon::Config {
        fichero: fichero.clone(),
        conexion: conexion.clone(),
        variable: variable.to_string(),
    };
    let mut nativo = orch_plugin_fisgon::Fisgon::nueva(config).unwrap();
    let lote = orch_wasm::leer_ipc(
        &orch_wasm::escribir_ipc(&arrow::record_batch::RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )))
        .unwrap(),
    )
    .unwrap();
    let fuera = nativo.procesar(lote).unwrap_err();
    assert!(
        fuera.contains("leyó") && fuera.contains("se conectó") && fuera.contains("vio"),
        "las sondas tienen que funcionar fuera del sandbox para que la prueba valga: {fuera}"
    );

    // Dentro, ninguno.
    let dir = TempDir::new().unwrap();
    let (entrada, salida) = csv(&dir);
    let informe = ejecutar(
        registro,
        &pipeline(
            &entrada,
            &salida,
            &[(
                "curioso",
                "fisgon",
                &format!(
                    "{{ fichero: \"{fichero}\", conexion: \"{conexion}\", variable: {variable} }}"
                ),
            )],
        ),
    )
    .await;
    assert!(informe.succeeded, "{informe:#?}");
    assert_eq!(informe.rows_written(), 2);
}
