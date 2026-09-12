//! Conexión a PostgreSQL, con o sin TLS.

use std::path::PathBuf;

use orch_core::{OrchError, Result};
use serde::Deserialize;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

/// Ajustes de TLS del conector.
///
/// El *modo* sale del `sslmode` de la cadena de conexión, como en libpq:
/// `disable` no cifra, `prefer` (el defecto) cifra si el servidor puede, y
/// `require` exige cifrado.
///
/// Lo que se decide aquí es si además se **verifica** el certificado, y por
/// defecto depende del modo:
///
/// - Con `prefer` no se verifica, igual que libpq. Es cifrado oportunista:
///   mejor que texto plano, pero no protege de un intermediario, y exigir
///   más rompería cualquier servidor con certificado propio sin que nadie
///   haya pedido garantías.
/// - Con `require` sí se verifica. **Aquí se diverge de libpq a propósito**:
///   allí `require` cifra sin comprobar nada, lo que da una falsa sensación
///   de seguridad. Si alguien pide TLS explícitamente, que sirva de algo.
///
/// `verify` fuerza cualquiera de los dos comportamientos. Un certificado
/// autofirmado con `require` necesita `root_cert` o `verify: false`.
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Sin indicar, se decide por el `sslmode`.
    #[serde(default)]
    pub verify: Option<bool>,
    /// CA con la que verificar, en PEM.
    #[serde(default)]
    pub root_cert: Option<PathBuf>,
}

impl TlsConfig {
    fn verifies(&self, mode: SslMode) -> bool {
        self.verify
            .unwrap_or(!matches!(mode, SslMode::Prefer) || self.root_cert.is_some())
    }
}

/// Abre una conexión y deja su tarea de protocolo corriendo.
///
/// `tokio-postgres` separa el `Client` (lo que se usa) de la `Connection`
/// (la que habla por el socket): sin lanzar la segunda, la primera se queda
/// esperando para siempre.
pub async fn connect(node: &str, dsn: &str, tls: &TlsConfig) -> Result<Client> {
    let config: Config = dsn.parse().map_err(|e| {
        // El DSN lleva la contraseña: nunca se incluye en el error.
        OrchError::config(node, format!("la cadena de conexión no es válida: {e}"))
    })?;

    // `sslmode=disable` es lo único que apaga TLS; `prefer` (el defecto de
    // libpq) lo intenta y se conforma con texto plano si el servidor no
    // puede.
    let mode = config.get_ssl_mode();
    if mode == SslMode::Disable {
        let (client, connection) = config
            .connect(NoTls)
            .await
            .map_err(|e| connection_error(node, &e))?;
        spawn_connection(node, connection);
        return Ok(client);
    }

    let connector = build_tls(node, tls, mode)?;
    let (client, connection) = config
        .connect(connector)
        .await
        .map_err(|e| connection_error(node, &e))?;
    spawn_connection(node, connection);
    Ok(client)
}

fn build_tls(
    node: &str,
    tls: &TlsConfig,
    mode: SslMode,
) -> Result<postgres_native_tls::MakeTlsConnector> {
    let mut builder = native_tls::TlsConnector::builder();

    if let Some(path) = &tls.root_cert {
        let pem = std::fs::read(path).map_err(|e| {
            OrchError::config(
                node,
                format!("no se pudo leer `root_cert` en `{}`: {e}", path.display()),
            )
        })?;
        let certificate = native_tls::Certificate::from_pem(&pem).map_err(|e| {
            OrchError::config(
                node,
                format!("`{}` no es un certificado PEM válido: {e}", path.display()),
            )
        })?;
        builder.add_root_certificate(certificate);
    }

    if !tls.verifies(mode) {
        // Cifra, pero no protege de un intermediario.
        tracing::debug!(
            node = %node,
            ?mode,
            "TLS sin verificar el certificado del servidor"
        );
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }

    let connector = builder
        .build()
        .map_err(|e| OrchError::config(node, format!("no se pudo preparar TLS: {e}")))?;
    Ok(postgres_native_tls::MakeTlsConnector::new(connector))
}

fn connection_error(node: &str, err: &tokio_postgres::Error) -> OrchError {
    // Sobre la cadena completa, no sobre `to_string()`: el motivo real del
    // fallo de TLS vive en la causa anidada.
    let detail = describe(err);
    let mut message = format!("no se pudo conectar a PostgreSQL: {detail}");
    // El tropiezo más habitual al estrenar TLS, y su solución.
    if detail.contains("certificate") || detail.contains("certificado") {
        message.push_str(
            ". Si el servidor usa un certificado autofirmado, indica su CA en \
             `tls.root_cert` o desactiva la comprobación con `tls.verify: false`",
        );
    }
    OrchError::node(node, message)
}

fn spawn_connection<F>(node: &str, connection: F)
where
    F: std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>
        + Send
        + 'static,
{
    let node = node.to_string();
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!(node = %node, error = %err, "la conexión con PostgreSQL se cortó");
        }
    });
}

/// Describe un error de PostgreSQL de forma legible.
///
/// El `Display` de `tokio_postgres::Error` sólo dice «db error»: el mensaje
/// que de verdad explica qué pasó, junto al detalle y la sugerencia del
/// servidor, está en el `DbError` que lleva dentro.
pub fn describe(err: &tokio_postgres::Error) -> String {
    let Some(db) = err.as_db_error() else {
        // Sin `DbError` (fallo de red o de TLS) la causa suele estar anidada.
        let mut text = err.to_string();
        let mut source = std::error::Error::source(err);
        while let Some(cause) = source {
            text.push_str(": ");
            text.push_str(&cause.to_string());
            source = cause.source();
        }
        return text;
    };
    let mut text = db.message().to_string();
    if let Some(detail) = db.detail() {
        text.push_str(" — ");
        text.push_str(detail);
    }
    if let Some(hint) = db.hint() {
        text.push_str(" (");
        text.push_str(hint);
        text.push(')');
    }
    text
}

/// Escapa un identificador para incrustarlo en SQL.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Escapa un nombre que puede venir cualificado (`esquema.tabla`).
pub fn quote_qualified(name: &str) -> String {
    name.split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}
