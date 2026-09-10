//! Utilidades compartidas por los conectores.

use orch_core::{OrchError, Result};

/// Convierte un delimitador escrito como carácter en el byte que espera Arrow.
pub(crate) fn delimiter_byte(node: &str, delimiter: char) -> Result<u8> {
    if delimiter.is_ascii() {
        Ok(delimiter as u8)
    } else {
        Err(OrchError::config(
            node,
            format!("el delimitador `{delimiter}` debe ser un carácter ASCII"),
        ))
    }
}

pub(crate) const fn yes() -> bool {
    true
}

pub(crate) const fn comma() -> char {
    ','
}
