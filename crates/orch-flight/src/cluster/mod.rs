//! Un pipeline repartido entre varias máquinas.
//!
//! Cada nodo puede llevar `worker: <nombre>`; los que no, corren donde se
//! lanza el pipeline. El coordinador parte el DAG por las aristas que cruzan
//! de una máquina a otra ([`dividir`]), manda cada parte a su worker y junta
//! lo que pasa en una sola ejecución, con un solo `run_id`.
//!
//! Un pipeline repartido es varios pipelines locales unidos por aristas
//! remotas: cada arista que cruza se convierte en un sink `cluster_out` en
//! un lado y un origen `cluster_in` en el otro, sobre Arrow Flight. Así el
//! motor de cada máquina es el de siempre.
//!
//! Lo que lleva cada máquina:
//!
//! - `orch worker` escucha con TLS mutuo **obligatorio**: sólo entra quien
//!   presente un certificado de la CA del clúster. Un worker ejecuta lo que
//!   le manden, y un pipeline puede leer y escribir ficheros: no se deja
//!   abierto a cualquiera. Recibe su parte tal y como está escrita, así que
//!   los `${env:...}` se resuelven con sus variables y las rutas relativas
//!   cuelgan de su carpeta.
//! - Las aristas que salen de una máquina se sirven desde su propio puerto
//!   (el del worker, o el `listen` del coordinador), no uno por arista.

mod aristas;
mod coordinador;
mod division;
mod motor;
mod servicio;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use orch_core::{OrchError, Plazo, Result};
use serde::Deserialize;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

pub use aristas::{registrar_aristas, Aristas};
pub use coordinador::Coordinador;
pub use division::{dividir, Donde, Parte};
pub use motor::{reparte, sin_cluster, Motor};
pub use servicio::servir_worker;

/// Nombre con el que se llama a «donde se lanza el pipeline».
pub const LOCAL: &str = "local";

fn un_minuto() -> Plazo {
    Plazo(Duration::from_secs(60))
}

/// El fichero del clúster: cómo llegar a cada worker y con qué
/// certificados. Depende del despliegue, no del pipeline, y por eso va
/// aparte.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// La CA del clúster y el certificado de esta máquina.
    pub tls: ClusterTls,
    /// `nombre: https://host:puerto` de cada worker.
    pub workers: BTreeMap<String, String>,
    /// Dónde escucha el coordinador si algún nodo que corre aquí alimenta a
    /// un worker: el worker tiene que poder leerlo.
    #[serde(default)]
    pub listen: Option<String>,
    /// Cómo ven los workers al coordinador (`https://host:puerto`), si
    /// escucha.
    #[serde(default)]
    pub url: Option<String>,
    /// Cuánto espera cada parte a la otra en una arista: lo que puede tardar
    /// en arrancar la máquina de enfrente.
    #[serde(default = "un_minuto")]
    pub wait: Plazo,
}

impl ClusterConfig {
    /// Lee el fichero del clúster. Las rutas de los certificados cuelgan de
    /// su carpeta.
    pub fn from_path(path: &Path) -> Result<Self> {
        let texto = std::fs::read_to_string(path).map_err(|source| OrchError::SpecIo {
            path: path.display().to_string(),
            source,
        })?;
        let mut config: ClusterConfig = serde_yaml::from_str(&texto).map_err(|err| {
            OrchError::Other(format!(
                "el clúster `{}` no se puede leer: {err}",
                path.display()
            ))
        })?;
        let base = path.parent().unwrap_or(Path::new("."));
        for ruta in [
            &mut config.tls.ca,
            &mut config.tls.cert,
            &mut config.tls.key,
        ] {
            if ruta.is_relative() {
                *ruta = base.join(&*ruta);
            }
        }
        for (nombre, url) in &config.workers {
            if !url.starts_with("https://") {
                return Err(OrchError::Other(format!(
                    "el worker `{nombre}` es `{url}`: en un clúster todo va cifrado, `https://`"
                )));
            }
        }
        if config.workers.contains_key(LOCAL) {
            return Err(OrchError::Other(format!(
                "`{LOCAL}` es como se llama a la máquina que lanza el pipeline: elige otro \
                 nombre para ese worker"
            )));
        }
        Ok(config)
    }
}

/// TLS de un clúster: todos se autentican con certificados de la misma CA.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTls {
    /// La CA que firma a los workers y al coordinador.
    pub ca: PathBuf,
    /// El certificado de esta máquina. Sirve para presentarse al escuchar y
    /// al conectarse.
    pub cert: PathBuf,
    pub key: PathBuf,
}

fn leer(que: &str, ruta: &Path) -> Result<Vec<u8>> {
    std::fs::read(ruta).map_err(|err| {
        OrchError::Other(format!(
            "no se pudo leer el {que} del clúster en `{}`: {err}",
            ruta.display()
        ))
    })
}

impl ClusterTls {
    fn identidad(&self) -> Result<Identity> {
        Ok(Identity::from_pem(
            leer("certificado", &self.cert)?,
            leer("certificado (clave)", &self.key)?,
        ))
    }

    /// Para escuchar: certificado propio y sólo clientes de la CA.
    pub fn servidor(&self) -> Result<ServerTlsConfig> {
        Ok(ServerTlsConfig::new()
            .identity(self.identidad()?)
            .client_ca_root(Certificate::from_pem(leer("CA", &self.ca)?)))
    }

    /// Para conectarse: sólo servidores de la CA, presentando el propio.
    pub fn cliente(&self) -> Result<ClientTlsConfig> {
        Ok(ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(leer("CA", &self.ca)?))
            .identity(self.identidad()?))
    }
}
