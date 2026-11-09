//! Plugins de Orch en WebAssembly.
//!
//! Un plugin es un componente de WebAssembly que cumple el contrato de
//! `wit/orch-plugin.wit`. Se carga una vez, se le pregunta su nombre y el
//! esquema de su config, y queda registrado como una transformación más: el
//! YAML lo usa con `op:`, el inspector enseña su config y `orch connectors`
//! lo lista, igual que a uno nativo.
//!
//! Cada nodo que lo usa recibe su propia instancia, aislada: sin ficheros,
//! sin red, sin variables de entorno, con un tope de memoria y un plazo por
//! llamada. Lo único que cruza la frontera son lotes de Arrow en formato IPC.
//!
//! Lo que cuesta, medido (`cargo run --release -p orch-wasm --example medir`):
//! cruzar son ~30 ns por fila en lotes de 8 192, y el código dentro del
//! sandbox va unas dos veces más lento que el nativo. Junto a cualquier E/S
//! real es poco; para el núcleo en memoria, que mueve una fila en 5 ns, no.
//! Por eso los conectores de Orch son nativos y los plugins son para lo que
//! Orch no trae.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{Input, NodeContext, OrchError, Output, Registry, Result, Transform};
use serde_json::Value;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Cache, CacheConfig, Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

wasmtime::component::bindgen!({
    world: "transformacion",
    path: "../../wit",
});

/// Memoria máxima de una instancia: un plugin que crece sin control falla él
/// solo en vez de llevarse el proceso.
const MEMORIA_MAXIMA: usize = 512 << 20;

/// Cada cuánto avanza el reloj que vigila a los plugins.
const TIC: Duration = Duration::from_millis(100);

/// Plazo de una llamada al plugin. Un lote de miles de filas se procesa en
/// milisegundos: si una llamada pasa de esto, el plugin está colgado, y es
/// mejor que falle su nodo que dejar el pipeline esperando para siempre.
const PLAZO_POR_LLAMADA: Duration = Duration::from_secs(30);

/// Filas por lote al cruzar la frontera.
///
/// Medido: por debajo de ~4 000 filas pesa el coste fijo de cada llamada, y
/// por encima de ~16 000 el lote serializado deja de caber en la caché del
/// procesador y cada fila cuesta casi el doble. Partir un lote grande no
/// copia nada: es una vista sobre los mismos buffers.
const FILAS_POR_CRUCE: usize = 8_192;

/// Un componente cargado y compilado, listo para instanciarse por nodo.
pub struct Plugin {
    pub nombre: String,
    /// Esquema JSON de su config, tal y como lo declara el propio plugin.
    pub esquema: Value,
    pub ruta: PathBuf,
    motor: Engine,
    componente: Component,
    enlazador: Linker<Estado>,
}

/// Lo que vive dentro de cada instancia: un WASI vacío y sus límites.
struct Estado {
    wasi: WasiCtx,
    tabla: ResourceTable,
    limites: StoreLimits,
}

impl WasiView for Estado {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.tabla,
        }
    }
}

/// Un motor de WebAssembly nuevo.
///
/// Lleva la caché de compilación de wasmtime: un plugin se compila la primera
/// vez que se ve (~1 s para uno de 2 MB) y los arranques siguientes lo leen
/// de disco en unas decenas de milisegundos. Si la caché no se puede crear,
/// se compila cada vez.
///
/// También arranca el reloj que hace cumplir el plazo por llamada; se para
/// solo cuando el motor deja de usarse.
pub fn motor() -> Result<Engine> {
    let mut config = Config::new();
    config.epoch_interruption(true);
    if let Ok(cache) = Cache::new(CacheConfig::new()) {
        config.cache(Some(cache));
    }
    let motor = Engine::new(&config).map_err(|e| {
        OrchError::Other(format!("no se pudo crear el motor de WebAssembly: {e:#}"))
    })?;

    let debil = motor.weak();
    std::thread::Builder::new()
        .name("orch-wasm-reloj".into())
        .spawn(move || {
            while let Some(motor) = debil.upgrade() {
                motor.increment_epoch();
                drop(motor);
                std::thread::sleep(TIC);
            }
        })
        .map_err(|e| {
            OrchError::Other(format!("no se pudo arrancar el reloj de los plugins: {e}"))
        })?;
    Ok(motor)
}

/// El motor del proceso: compilar es caro y lo compilado se comparte.
fn motor_compartido() -> Result<&'static Engine> {
    static MOTOR: OnceLock<Engine> = OnceLock::new();
    if let Some(motor) = MOTOR.get() {
        return Ok(motor);
    }
    let motor = motor()?;
    Ok(MOTOR.get_or_init(|| motor))
}

impl Plugin {
    /// Carga y compila un componente, y le pregunta su nombre y su esquema.
    pub fn cargar(motor: &Engine, ruta: &Path) -> Result<Self> {
        let fallo = |que: &str, e: wasmtime::Error| {
            OrchError::Other(format!("plugin `{}`: {que}: {e:#}", ruta.display()))
        };
        let componente = Component::from_file(motor, ruta)
            .map_err(|e| fallo("no es un componente válido", e))?;
        let mut enlazador = Linker::new(motor);
        // WASI está para que la biblioteca estándar del plugin funcione, no
        // para darle acceso a nada: el contexto va vacío.
        wasmtime_wasi::p2::add_to_linker_sync(&mut enlazador)
            .map_err(|e| fallo("no se pudo preparar WASI", e))?;

        let mut plugin = Self {
            nombre: String::new(),
            esquema: Value::Null,
            ruta: ruta.to_path_buf(),
            motor: motor.clone(),
            componente,
            enlazador,
        };
        let mut instancia = plugin.instanciar()?;
        let nombre = instancia.nombre()?;
        if nombre.trim().is_empty() {
            return Err(OrchError::Other(format!(
                "plugin `{}`: no dice cómo se llama",
                ruta.display()
            )));
        }
        plugin.nombre = nombre;
        let esquema = instancia.esquema()?;
        plugin.esquema = serde_json::from_str(&esquema).unwrap_or(Value::Null);
        Ok(plugin)
    }

    /// Una instancia nueva, con su memoria propia.
    pub fn instanciar(&self) -> Result<Instancia> {
        let estado = Estado {
            wasi: WasiCtx::builder().build(),
            tabla: ResourceTable::new(),
            limites: StoreLimitsBuilder::new()
                .memory_size(MEMORIA_MAXIMA)
                .build(),
        };
        let mut store = Store::new(&self.motor, estado);
        store.limiter(|estado| &mut estado.limites);
        store.set_epoch_deadline(ticks(PLAZO_POR_LLAMADA));
        let enlaces = Transformacion::instantiate(&mut store, &self.componente, &self.enlazador)
            .map_err(|e| {
                OrchError::Other(format!(
                    "plugin `{}`: no se pudo instanciar: {e:#}",
                    self.ruta.display()
                ))
            })?;
        Ok(Instancia {
            store,
            enlaces,
            nombre: if self.nombre.is_empty() {
                self.ruta.display().to_string()
            } else {
                self.nombre.clone()
            },
        })
    }
}

fn ticks(plazo: Duration) -> u64 {
    (plazo.as_millis() / TIC.as_millis()).max(1) as u64
}

/// Una instancia viva de un plugin: la de un nodo en una ejecución.
pub struct Instancia {
    store: Store<Estado>,
    enlaces: Transformacion,
    /// Para los mensajes de error.
    nombre: String,
}

impl Instancia {
    fn trampa(&self, e: wasmtime::Error) -> OrchError {
        if matches!(
            e.downcast_ref::<wasmtime::Trap>(),
            Some(wasmtime::Trap::Interrupt)
        ) {
            return OrchError::Other(format!(
                "plugin `{}`: no respondió en {} s y se interrumpió",
                self.nombre,
                PLAZO_POR_LLAMADA.as_secs()
            ));
        }
        OrchError::Other(format!("plugin `{}`: {e:#}", self.nombre))
    }

    /// Cada llamada empieza con el plazo entero.
    fn llamar<R>(
        &mut self,
        f: impl FnOnce(&Transformacion, &mut Store<Estado>) -> wasmtime::Result<R>,
    ) -> Result<R> {
        self.store.set_epoch_deadline(ticks(PLAZO_POR_LLAMADA));
        let resultado = f(&self.enlaces, &mut self.store);
        resultado.map_err(|e| self.trampa(e))
    }

    pub fn nombre(&mut self) -> Result<String> {
        self.llamar(|t, s| t.call_nombre(s))
    }

    pub fn esquema(&mut self) -> Result<String> {
        self.llamar(|t, s| t.call_esquema_config(s))
    }

    /// Prepara la instancia con la config del nodo. El error del plugin
    /// (una config que no le vale) es un error de config del nodo.
    pub fn iniciar(&mut self, nodo: &str, config: &Value) -> Result<()> {
        let config = serde_json::to_string(config).unwrap_or_default();
        self.llamar(|t, s| t.call_iniciar(s, &config))?
            .map_err(|mensaje| OrchError::config(nodo, mensaje))
    }

    pub fn procesar(&mut self, nodo: &str, lote: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let bytes = escribir_ipc(lote)?;
        let salidas = self.procesar_ipc(nodo, &bytes)?;
        salidas.iter().map(|s| leer_ipc(s)).collect()
    }

    /// La llamada en crudo, con los lotes ya en IPC. La usa la medición para
    /// separar lo que cuesta serializar de lo que cuesta cruzar.
    pub fn procesar_ipc(&mut self, nodo: &str, lote: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.llamar(|t, s| t.call_procesar(s, lote))?
            .map_err(|mensaje| OrchError::node(nodo, mensaje))
    }

    pub fn terminar(&mut self, nodo: &str) -> Result<Vec<RecordBatch>> {
        let salidas = self
            .llamar(|t, s| t.call_terminar(s))?
            .map_err(|mensaje| OrchError::node(nodo, mensaje))?;
        salidas.iter().map(|s| leer_ipc(s)).collect()
    }
}

/// Carga todos los `.wasm` de una carpeta.
///
/// Un plugin que no carga no impide cargar los demás: se devuelve su error
/// junto a los que sí, para enseñarlo sin esconder el resto.
pub fn cargar_carpeta(motor: &Engine, carpeta: &Path) -> Vec<Result<Plugin>> {
    let Ok(entradas) = std::fs::read_dir(carpeta) else {
        return Vec::new();
    };
    let mut rutas: Vec<PathBuf> = entradas
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wasm"))
        .collect();
    rutas.sort();
    rutas
        .iter()
        .map(|ruta| Plugin::cargar(motor, ruta))
        .collect()
}

/// Dónde buscar los plugins de los pipelines que viven en `base`.
///
/// En su subcarpeta `plugins/`, para que una carpeta de pipelines sea
/// portable con todo lo que necesita, igual que sus rutas se resuelven
/// contra ella. `ORCH_PLUGINS` apunta a otra.
pub fn carpeta_de_plugins(base: &Path) -> PathBuf {
    std::env::var_os("ORCH_PLUGINS")
        .map(PathBuf::from)
        .unwrap_or_else(|| base.join("plugins"))
}

/// Carga los plugins de una carpeta y los registra.
///
/// Devuelve un aviso por cada uno que no se pudo usar. Un plugin no puede
/// llamarse como un componente que ya existe: un `sql` de terceros que
/// sustituyera en silencio al de verdad sería un agujero, no una extensión.
pub fn registrar_carpeta(registro: &mut Registry, carpeta: &Path) -> Vec<String> {
    if !carpeta.is_dir() {
        return Vec::new();
    }
    let motor = match motor_compartido() {
        Ok(motor) => motor,
        Err(err) => return vec![err.to_string()],
    };
    let mut avisos = Vec::new();
    let mut validos: Vec<Plugin> = Vec::new();
    for cargado in cargar_carpeta(motor, carpeta) {
        match cargado {
            Err(err) => avisos.push(err.to_string()),
            Ok(plugin) => {
                if registro.transform_names().contains(&plugin.nombre.as_str())
                    || validos.iter().any(|v| v.nombre == plugin.nombre)
                {
                    avisos.push(format!(
                        "plugin `{}`: ya hay una transformación `{}`; no se carga",
                        plugin.ruta.display(),
                        plugin.nombre
                    ));
                } else {
                    validos.push(plugin);
                }
            }
        }
    }
    registrar(registro, validos);
    avisos
}

/// Registra los plugins como transformaciones.
pub fn registrar(registro: &mut Registry, plugins: Vec<Plugin>) {
    for plugin in plugins {
        let plugin = Arc::new(plugin);
        let nombre = plugin.nombre.clone();
        let esquema = plugin.esquema.clone();
        registro.register_transform_dyn(nombre, esquema, move |nodo, config| {
            let t: Arc<dyn Transform> = Arc::new(TransformacionWasm {
                nodo: nodo.to_string(),
                plugin: Arc::clone(&plugin),
                config: config.clone(),
            });
            Ok(t)
        });
    }
}

/// Un nodo cuyo trabajo hace un plugin.
struct TransformacionWasm {
    nodo: String,
    plugin: Arc<Plugin>,
    config: Value,
}

#[async_trait]
impl Transform for TransformacionWasm {
    fn op(&self) -> &str {
        &self.plugin.nombre
    }

    async fn apply(&self, _ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        // Una instancia por ejecución del nodo: un reintento empieza limpio,
        // sin el estado que dejó el intento anterior.
        let plugin = Arc::clone(&self.plugin);
        let nodo = self.nodo.clone();
        let config = self.config.clone();
        let mut instancia = fuera_del_motor(move || {
            let mut instancia = plugin.instanciar()?;
            instancia.iniciar(&nodo, &config)?;
            Ok(instancia)
        })
        .await?;

        while let Some(lote) = input.recv().await? {
            for trozo in trozos(&lote) {
                let nodo = self.nodo.clone();
                let (devuelta, salidas) = fuera_del_motor(move || {
                    let salidas = instancia.procesar(&nodo, &trozo);
                    salidas.map(|s| (instancia, s))
                })
                .await?;
                instancia = devuelta;
                for salida in salidas {
                    output.send(salida).await?;
                }
            }
        }

        let nodo = self.nodo.clone();
        let salidas = fuera_del_motor(move || instancia.terminar(&nodo)).await?;
        for salida in salidas {
            output.send(salida).await?;
        }
        Ok(())
    }
}

/// Ejecuta una llamada al plugin en un hilo aparte.
///
/// El código de un plugin es de terceros y puede tardar: en los hilos del
/// motor bloquearía a los demás nodos que comparten hilo con él. El salto de
/// hilo cuesta microsegundos y un lote, centenares.
async fn fuera_del_motor<R: Send + 'static>(
    f: impl FnOnce() -> Result<R> + Send + 'static,
) -> Result<R> {
    tokio::task::spawn_blocking(f).await?
}

/// El lote, en trozos de [`FILAS_POR_CRUCE`] filas como mucho.
fn trozos(lote: &RecordBatch) -> Vec<RecordBatch> {
    if lote.num_rows() <= FILAS_POR_CRUCE {
        return vec![lote.clone()];
    }
    (0..lote.num_rows())
        .step_by(FILAS_POR_CRUCE)
        .map(|desde| lote.slice(desde, FILAS_POR_CRUCE.min(lote.num_rows() - desde)))
        .collect()
}

pub fn escribir_ipc(lote: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut escritor = StreamWriter::try_new(&mut bytes, &lote.schema())?;
    escritor.write(lote)?;
    escritor.finish()?;
    drop(escritor);
    Ok(bytes)
}

pub fn leer_ipc(bytes: &[u8]) -> Result<RecordBatch> {
    let mut lector = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    lector
        .next()
        .ok_or_else(|| OrchError::Other("el plugin devolvió un stream IPC vacío".into()))?
        .map_err(Into::into)
}
