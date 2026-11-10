//! TLS de las aristas remotas.
//!
//! Los dos lados llevan un bloque `tls` con ficheros PEM. Las rutas
//! relativas cuelgan de la carpeta del pipeline, como `path`.
//!
//! - Quien escucha (el sink) presenta su certificado y, con `client_ca`, pide
//!   a quien se conecta un certificado firmado por esa CA: TLS mutuo, que
//!   autentica al cliente sin necesidad de token.
//! - Quien se conecta (el origen) verifica al servidor con `ca` o, sin ella,
//!   con los certificados raíz del sistema; y presenta el suyo si el servidor
//!   lo pide.

use std::path::{Path, PathBuf};

use orch_core::{OrchError, Result};
use serde::Deserialize;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// TLS de quien escucha.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    /// Certificado del servidor (PEM, con la cadena intermedia si la hay).
    pub cert: PathBuf,
    /// Su clave privada (PEM).
    pub key: PathBuf,
    /// Con esto, sólo se acepta a quien presente un certificado firmado por
    /// esta CA (TLS mutuo). Autentica a quien lee sin necesidad de `token`.
    #[serde(default)]
    pub client_ca: Option<PathBuf>,
}

/// TLS de quien se conecta.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClientTls {
    /// CA con la que verificar al servidor. Sin ella, la del sistema.
    #[serde(default)]
    pub ca: Option<PathBuf>,
    /// Certificado propio, si el servidor pide TLS mutuo.
    #[serde(default)]
    pub cert: Option<PathBuf>,
    /// Clave del certificado propio.
    #[serde(default)]
    pub key: Option<PathBuf>,
    /// Nombre con el que verificar el certificado del servidor, si no es el
    /// de la `url`.
    #[serde(default)]
    pub domain: Option<String>,
}

fn leer(node: &str, que: &str, ruta: &Path) -> Result<Vec<u8>> {
    std::fs::read(ruta).map_err(|err| {
        OrchError::config(
            node,
            format!("no se pudo leer `tls.{que}` en `{}`: {err}", ruta.display()),
        )
    })
}

impl ServerTls {
    pub(crate) fn config(&self, node: &str) -> Result<ServerTlsConfig> {
        let identidad = Identity::from_pem(
            leer(node, "cert", &self.cert)?,
            leer(node, "key", &self.key)?,
        );
        let mut config = ServerTlsConfig::new().identity(identidad);
        if let Some(ca) = &self.client_ca {
            config = config.client_ca_root(Certificate::from_pem(leer(node, "client_ca", ca)?));
        }
        Ok(config)
    }

    /// Si autentica a quien se conecta.
    pub fn mutual(&self) -> bool {
        self.client_ca.is_some()
    }
}

impl ClientTls {
    pub(crate) fn config(&self, node: &str) -> Result<ClientTlsConfig> {
        let mut config = ClientTlsConfig::new();
        config = match &self.ca {
            Some(ca) => config.ca_certificate(Certificate::from_pem(leer(node, "ca", ca)?)),
            None => config.with_native_roots(),
        };
        match (&self.cert, &self.key) {
            (Some(cert), Some(key)) => {
                config = config.identity(Identity::from_pem(
                    leer(node, "cert", cert)?,
                    leer(node, "key", key)?,
                ));
            }
            (None, None) => {}
            _ => {
                return Err(OrchError::config(
                    node,
                    "`tls.cert` y `tls.key` van juntos: el certificado propio necesita su clave",
                ))
            }
        }
        if let Some(dominio) = &self.domain {
            config = config.domain_name(dominio.clone());
        }
        Ok(config)
    }
}
