//! SDK para escribir transformaciones de Orch en Rust y compilarlas a
//! WebAssembly.
//!
//! Un plugin es un tipo que implementa [`Transformacion`] y una línea que lo
//! exporta:
//!
//! ```ignore
//! use orch_plugin::{RecordBatch, Transformacion};
//!
//! #[derive(serde::Deserialize, schemars::JsonSchema)]
//! #[serde(deny_unknown_fields)]
//! struct Config {
//!     /// Columna a tratar.
//!     columna: String,
//! }
//!
//! struct MiPlugin {
//!     config: Config,
//! }
//!
//! impl Transformacion for MiPlugin {
//!     const NOMBRE: &'static str = "mi_plugin";
//!     type Config = Config;
//!
//!     fn nueva(config: Config) -> Result<Self, String> {
//!         Ok(Self { config })
//!     }
//!
//!     fn procesar(&mut self, lote: RecordBatch) -> Result<Vec<RecordBatch>, String> {
//!         Ok(vec![lote])
//!     }
//! }
//!
//! orch_plugin::exportar!(MiPlugin);
//! ```
//!
//! Se compila con `cargo build --release --target wasm32-wasip2`, y el
//! `.wasm` resultante va a la carpeta de plugins de Orch.
//!
//! El SDK se ocupa de la frontera: deserializa la config, traduce los lotes
//! desde y hacia IPC de Arrow y guarda el estado entre llamadas. Las
//! descripciones de la config salen de los comentarios `///` de sus campos,
//! igual que en un conector nativo, y el inspector del lienzo las enseña.
//!
//! Fuera de WASM, `exportar!` no genera nada: el mismo crate se puede usar
//! como biblioteca nativa, que es como se mide cuánto cuesta la frontera.

use std::any::Any;
use std::cell::RefCell;

pub use arrow_array;
pub use arrow_array::RecordBatch;
pub use arrow_schema;

/// Una transformación: lotes entran, lotes salen.
pub trait Transformacion: Sized + 'static {
    /// Nombre con el que se usa en el YAML: `op: <NOMBRE>`.
    const NOMBRE: &'static str;

    /// La config del nodo. Su esquema JSON es el que enseña el inspector.
    type Config: serde::de::DeserializeOwned + schemars::JsonSchema;

    fn nueva(config: Self::Config) -> Result<Self, String>;

    /// Procesa un lote. Puede devolver cero, uno o varios.
    fn procesar(&mut self, lote: RecordBatch) -> Result<Vec<RecordBatch>, String>;

    /// La entrada terminó: lo que quede por emitir (un agregado, un búfer).
    fn terminar(&mut self) -> Result<Vec<RecordBatch>, String> {
        Ok(Vec::new())
    }
}

/// Para un plugin que no lleva configuración.
///
/// No es lo mismo que ignorarla: con `deny_unknown_fields`, un `config:` con
/// algo dentro se rechaza en vez de aceptarse en silencio.
#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SinConfig {}

/// Traducción de lotes al formato que cruza la frontera: un stream IPC de
/// Arrow con el esquema y un lote.
pub mod ipc {
    use arrow_array::RecordBatch;
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;

    pub fn leer(bytes: &[u8]) -> Result<RecordBatch, String> {
        let mut lector = StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| format!("el lote no es IPC de Arrow válido: {e}"))?;
        lector
            .next()
            .ok_or_else(|| "el stream IPC no trae ningún lote".to_string())?
            .map_err(|e| format!("no se pudo leer el lote: {e}"))
    }

    pub fn escribir(lote: &RecordBatch) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();
        let mut escritor = StreamWriter::try_new(&mut bytes, &lote.schema())
            .map_err(|e| format!("no se pudo escribir el esquema: {e}"))?;
        escritor
            .write(lote)
            .map_err(|e| format!("no se pudo escribir el lote: {e}"))?;
        escritor
            .finish()
            .map_err(|e| format!("no se pudo cerrar el stream: {e}"))?;
        drop(escritor);
        Ok(bytes)
    }
}

// --- La frontera -------------------------------------------------------------
//
// Lo que sigue lo usa `exportar!`. Cada instancia del componente es un nodo
// del pipeline y WebAssembly es de un solo hilo, así que el estado vive en un
// `thread_local`.

#[doc(hidden)]
pub mod bindings {
    wit_bindgen::generate!({
        world: "transformacion",
        path: "../../wit",
        pub_export_macro: true,
        default_bindings_module: "orch_plugin::bindings",
        export_macro_name: "__exportar_componente",
    });
}

thread_local! {
    static ESTADO: RefCell<Option<Box<dyn Any>>> = const { RefCell::new(None) };
}

#[doc(hidden)]
pub fn __esquema<T: Transformacion>() -> String {
    serde_json::to_string(&schemars::schema_for!(T::Config)).unwrap_or_default()
}

#[doc(hidden)]
pub fn __iniciar<T: Transformacion>(config: &str) -> Result<(), String> {
    // Un nodo sin `config:` llega como nulo, y significa «todo por defecto».
    let valor: serde_json::Value = if config.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(config).map_err(|e| format!("la config no es JSON: {e}"))?
    };
    let valor = if valor.is_null() {
        serde_json::Value::Object(Default::default())
    } else {
        valor
    };
    let config: T::Config =
        serde_json::from_value(valor).map_err(|e| format!("config inválida: {e}"))?;
    let transformacion = T::nueva(config)?;
    ESTADO.with(|estado| *estado.borrow_mut() = Some(Box::new(transformacion)));
    Ok(())
}

#[doc(hidden)]
pub fn __procesar<T: Transformacion>(lote: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
    let lote = ipc::leer(&lote)?;
    let salidas = con::<T, _>(|t| t.procesar(lote))?;
    salidas.iter().map(ipc::escribir).collect()
}

#[doc(hidden)]
pub fn __terminar<T: Transformacion>() -> Result<Vec<Vec<u8>>, String> {
    let salidas = con::<T, _>(|t| t.terminar())?;
    salidas.iter().map(ipc::escribir).collect()
}

fn con<T: Transformacion, R>(f: impl FnOnce(&mut T) -> Result<R, String>) -> Result<R, String> {
    ESTADO.with(|estado| {
        let mut estado = estado.borrow_mut();
        let transformacion = estado
            .as_mut()
            .and_then(|e| e.downcast_mut::<T>())
            .ok_or_else(|| "la transformación no se inició".to_string())?;
        f(transformacion)
    })
}

/// Exporta un tipo que implementa [`Transformacion`] como componente de Orch.
///
/// Fuera de WebAssembly no genera nada, de modo que el mismo crate se puede
/// usar como biblioteca nativa.
#[macro_export]
macro_rules! exportar {
    ($tipo:ty) => {
        #[cfg(target_arch = "wasm32")]
        const _: () = {
            struct Componente;

            impl $crate::bindings::Guest for Componente {
                fn nombre() -> String {
                    <$tipo as $crate::Transformacion>::NOMBRE.to_string()
                }
                fn esquema_config() -> String {
                    $crate::__esquema::<$tipo>()
                }
                fn iniciar(config: String) -> Result<(), String> {
                    $crate::__iniciar::<$tipo>(&config)
                }
                fn procesar(lote: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
                    $crate::__procesar::<$tipo>(lote)
                }
                fn terminar() -> Result<Vec<Vec<u8>>, String> {
                    $crate::__terminar::<$tipo>()
                }
            }

            $crate::bindings::__exportar_componente!(Componente);
        };
    };
}
