//! Plugin que pasa a mayúsculas una columna de texto.
//!
//! Hace trabajo de verdad en cada fila, así que sirve para medir cuánto pesa
//! la frontera de WebAssembly frente a una transformación con contenido. Y es
//! el ejemplo de plugin con config.

use std::sync::Arc;

use orch_plugin::arrow_array::{Array, ArrayRef, StringArray};
use orch_plugin::{RecordBatch, Transformacion};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Columna de texto que se pasa a mayúsculas. Las demás pasan tal cual.
    pub columna: String,
}

pub struct Mayusculas {
    config: Config,
}

impl Transformacion for Mayusculas {
    const NOMBRE: &'static str = "mayusculas";
    type Config = Config;

    fn nueva(config: Config) -> Result<Self, String> {
        if config.columna.is_empty() {
            return Err("`columna` no puede estar vacía".into());
        }
        Ok(Self { config })
    }

    fn procesar(&mut self, lote: RecordBatch) -> Result<Vec<RecordBatch>, String> {
        let indice = lote
            .schema()
            .index_of(&self.config.columna)
            .map_err(|_| format!("la columna `{}` no existe", self.config.columna))?;
        let texto = lote
            .column(indice)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("la columna `{}` no es de texto", self.config.columna))?;

        let nueva: StringArray = texto.iter().map(|v| v.map(str::to_uppercase)).collect();
        let mut columnas: Vec<ArrayRef> = lote.columns().to_vec();
        columnas[indice] = Arc::new(nueva);
        let salida = RecordBatch::try_new(lote.schema(), columnas).map_err(|e| e.to_string())?;
        Ok(vec![salida])
    }
}

orch_plugin::exportar!(Mayusculas);
