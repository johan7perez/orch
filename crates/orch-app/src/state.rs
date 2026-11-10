//! Estado compartido de la aplicación.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use orch_core::Registry;
use orch_flight::cluster::{ClusterConfig, Coordinador};
use orch_store::Store;

/// Todo lo que la ventana necesita para trabajar.
///
/// El almacén se abre una vez al arrancar: abrir DuckDB en cada clic se
/// notaría. El registro se rehace al cambiar de carpeta, porque los plugins
/// viven con los pipelines y cada carpeta trae los suyos; compilarlos sale de
/// la caché, así que cuesta unas decenas de milisegundos por plugin.
pub struct AppState {
    registry: RwLock<Arc<Registry>>,
    /// Los plugins de la carpeta que no se pudieron cargar, y por qué.
    plugin_warnings: Mutex<Vec<String>>,
    /// El coordinador del clúster de `ORCH_CLUSTER`, si lo hay: con él se
    /// ejecutan los pipelines que reparten nodos entre máquinas. Se rehace
    /// con el registro, porque lleva su propia copia.
    coordinador: RwLock<Option<Arc<Coordinador>>>,
    pub store: Arc<Store>,
    /// Directorio de pipelines que se está mirando. Cambiable desde la UI.
    directory: Mutex<PathBuf>,
}

impl AppState {
    pub fn new(store: Store, directory: PathBuf) -> Self {
        let (registry, mut avisos) = full_registry(&directory);
        let coordinador = coordinador(&directory, &mut avisos);
        Self {
            registry: RwLock::new(Arc::new(registry)),
            plugin_warnings: Mutex::new(avisos),
            coordinador: RwLock::new(coordinador),
            store: Arc::new(store),
            directory: Mutex::new(directory),
        }
    }

    /// El registro vigente. Una ejecución en curso se queda con el que tenía
    /// al arrancar aunque se cambie de carpeta a mitad.
    pub fn registry(&self) -> Arc<Registry> {
        Arc::clone(
            &self
                .registry
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    /// El coordinador vigente, si hay clúster.
    pub fn coordinador(&self) -> Option<Arc<Coordinador>> {
        self.coordinador
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn plugin_warnings(&self) -> Vec<String> {
        self.plugin_warnings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn directory(&self) -> PathBuf {
        self.directory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn set_directory(&self, path: &Path) {
        let mut current = self
            .directory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = path.to_path_buf();
        drop(current);

        let (registry, mut avisos) = full_registry(path);
        *self
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(registry);
        *self
            .coordinador
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = coordinador(path, &mut avisos);
        *self
            .plugin_warnings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = avisos;
    }
}

/// Todos los conectores y transformaciones que la aplicación sabe ejecutar,
/// más los plugins de la carpeta de pipelines, con los avisos de los que no
/// se pudieron cargar.
pub fn full_registry(directory: &Path) -> (Registry, Vec<String>) {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    orch_rest::register(&mut registry);
    orch_postgres::register(&mut registry);
    orch_flight::register(&mut registry);
    let avisos =
        orch_wasm::registrar_carpeta(&mut registry, &orch_wasm::carpeta_de_plugins(directory));
    (registry, avisos)
}

/// El coordinador del clúster que diga `ORCH_CLUSTER`. Un fichero de clúster
/// que no vale se avisa como un plugin que no carga: los pipelines que no
/// reparten nada siguen funcionando.
fn coordinador(directory: &Path, avisos: &mut Vec<String>) -> Option<Arc<Coordinador>> {
    let fichero = std::env::var_os("ORCH_CLUSTER")?;
    let fichero = PathBuf::from(fichero);
    let resultado = ClusterConfig::from_path(&fichero).and_then(|config| {
        let (registry, _) = full_registry(directory);
        Coordinador::new(registry, config)
    });
    match resultado {
        Ok(coordinador) => Some(Arc::new(coordinador)),
        Err(err) => {
            avisos.push(format!(
                "el clúster de `ORCH_CLUSTER` ({}) no se pudo cargar: {err}",
                fichero.display()
            ));
            None
        }
    }
}

/// Dónde buscar pipelines la primera vez.
///
/// Se prueba `ORCH_PIPELINES`, luego `./pipelines` y luego los ejemplos del
/// repositorio, para que la aplicación arranque enseñando algo en vez de una
/// pantalla vacía.
pub fn default_directory() -> PathBuf {
    if let Ok(from_env) = std::env::var("ORCH_PIPELINES") {
        return PathBuf::from(from_env);
    }
    for candidate in ["pipelines", "examples/pipelines"] {
        let path = PathBuf::from(candidate);
        if path.is_dir() {
            return path;
        }
    }
    PathBuf::from("pipelines")
}

/// Dónde vive el historial.
pub fn default_store() -> PathBuf {
    std::env::var("ORCH_STORE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("orch.duckdb"))
}
