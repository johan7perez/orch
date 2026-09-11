//! Estado compartido de la aplicación.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use orch_core::Registry;
use orch_store::Store;

/// Todo lo que la ventana necesita para trabajar.
///
/// El registro y el almacén se crean una vez al arrancar: montar el catálogo
/// de DataFusion o abrir DuckDB en cada clic se notaría.
pub struct AppState {
    pub registry: Arc<Registry>,
    pub store: Arc<Store>,
    /// Directorio de pipelines que se está mirando. Cambiable desde la UI.
    directory: Mutex<PathBuf>,
}

impl AppState {
    pub fn new(registry: Registry, store: Store, directory: PathBuf) -> Self {
        Self {
            registry: Arc::new(registry),
            store: Arc::new(store),
            directory: Mutex::new(directory),
        }
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
    }
}

/// Todos los conectores y transformaciones que la aplicación sabe ejecutar.
pub fn full_registry() -> Registry {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    orch_rest::register(&mut registry);
    orch_postgres::register(&mut registry);
    registry
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
