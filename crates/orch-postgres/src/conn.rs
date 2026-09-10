//! Conexión a PostgreSQL.

use orch_core::{OrchError, Result};
use tokio_postgres::{Client, NoTls};

/// Abre una conexión y deja su tarea de protocolo corriendo.
///
/// `tokio-postgres` separa el `Client` (lo que se usa) de la `Connection`
/// (la que habla por el socket): sin lanzar la segunda, la primera se queda
/// esperando para siempre.
///
/// Sin TLS por ahora. Un servidor que exija SSL rechazará la conexión con un
/// error claro, no en silencio.
pub async fn connect(node: &str, dsn: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.map_err(|e| {
        OrchError::node(
            node,
            // El DSN lleva la contraseña: nunca se incluye en el error.
            format!("no se pudo conectar a PostgreSQL: {}", describe(&e)),
        )
    })?;

    let node = node.to_string();
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::error!(node = %node, error = %err, "la conexión con PostgreSQL se cortó");
        }
    });

    Ok(client)
}

/// Describe un error de PostgreSQL de forma legible.
///
/// El `Display` de `tokio_postgres::Error` sólo dice «db error»: el mensaje
/// que de verdad explica qué pasó, junto al detalle y la sugerencia del
/// servidor, está en el `DbError` que lleva dentro.
pub fn describe(err: &tokio_postgres::Error) -> String {
    let Some(db) = err.as_db_error() else {
        return err.to_string();
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
