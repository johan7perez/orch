//! Plugin de prueba que intenta salirse del sandbox.
//!
//! No es un ejemplo a imitar: existe para que los tests comprueben que un
//! plugin no puede leer ficheros, abrir conexiones ni ver las variables de
//! entorno del proceso que lo carga. Si algún intento sale bien, lo dice en
//! el error, y el test falla.

use orch_plugin::{RecordBatch, Transformacion};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Fichero que intenta leer.
    pub fichero: String,
    /// `host:puerto` al que intenta conectarse.
    pub conexion: String,
    /// Variable de entorno que intenta leer.
    pub variable: String,
}

pub struct Fisgon {
    config: Config,
}

impl Transformacion for Fisgon {
    const NOMBRE: &'static str = "fisgon";
    type Config = Config;

    fn nueva(config: Config) -> Result<Self, String> {
        Ok(Self { config })
    }

    fn procesar(&mut self, lote: RecordBatch) -> Result<Vec<RecordBatch>, String> {
        let mut escapes = Vec::new();
        if std::fs::read(&self.config.fichero).is_ok() {
            escapes.push(format!("leyó `{}`", self.config.fichero));
        }
        if std::net::TcpStream::connect(&self.config.conexion).is_ok() {
            escapes.push(format!("se conectó a `{}`", self.config.conexion));
        }
        if std::env::var(&self.config.variable).is_ok() {
            escapes.push(format!("vio la variable `{}`", self.config.variable));
        }
        if escapes.is_empty() {
            Ok(vec![lote])
        } else {
            Err(format!("ESCAPÓ DEL SANDBOX: {}", escapes.join(", ")))
        }
    }
}

orch_plugin::exportar!(Fisgon);
