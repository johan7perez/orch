//! Referencias a secretos dentro de la config de un nodo.
//!
//! Una contraseña no debe vivir en el YAML: el YAML se versiona, se comparte
//! y acaba en un repositorio. En su lugar se escribe una referencia,
//! `${env:PGPASSWORD}`, que se resuelve al cargar el pipeline.
//!
//! El formato deja sitio para otros orígenes (`${file:...}`, `${keyring:...}`)
//! sin cambiar la sintaxis. Un esquema desconocido es un error, no un valor
//! literal: un `${ENV:X}` mal escrito acabaría en una cadena de conexión y
//! fallaría de forma incomprensible.

use serde_json::Value;

use crate::error::{OrchError, Result};

/// Esquemas soportados hoy.
const SCHEMES: &[&str] = &["env"];

/// Resuelve las referencias de todas las cadenas de un valor de config.
pub fn expand_value(node: &str, value: &mut Value) -> Result<()> {
    match value {
        Value::String(text) => {
            if let Some(expanded) = expand(node, text)? {
                *text = expanded;
            }
        }
        Value::Array(items) => {
            for item in items {
                expand_value(node, item)?;
            }
        }
        Value::Object(fields) => {
            for (_, field) in fields.iter_mut() {
                expand_value(node, field)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resuelve las referencias de una cadena.
///
/// Devuelve `None` si no había ninguna, para no reasignar sin necesidad.
pub fn expand(node: &str, text: &str) -> Result<Option<String>> {
    if !text.contains("${") {
        return Ok(None);
    }

    let mut out = String::with_capacity(text.len());
    let mut rest = text;

    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];

        let Some(end) = after.find('}') else {
            // Un `${` sin cerrar no es una referencia; se deja tal cual.
            out.push_str(&rest[start..]);
            return Ok(Some(out));
        };

        let body = &after[..end];
        match body.split_once(':') {
            Some(("env", name)) => {
                out.push_str(&lookup_env(node, name)?);
            }
            Some((scheme, _)) if is_identifier(scheme) => {
                return Err(OrchError::config(
                    node,
                    format!(
                        "`${{{body}}}`: origen de secreto `{scheme}` desconocido \
                         (soportados: {})",
                        SCHEMES.join(", ")
                    ),
                ));
            }
            // No tiene forma de referencia (`${HOME}`, `${1}`): literal.
            _ => {
                out.push_str("${");
                out.push_str(body);
                out.push('}');
            }
        }

        rest = &after[end + 1..];
    }

    out.push_str(rest);
    Ok(Some(out))
}

fn lookup_env(node: &str, name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(OrchError::config(
            node,
            "`${env:}` no indica ninguna variable",
        ));
    }
    std::env::var(name).map_err(|_| {
        OrchError::config(
            node,
            format!("la variable de entorno `{name}` no está definida"),
        )
    })
}

fn is_identifier(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
