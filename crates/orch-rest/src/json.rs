//! Conversión entre JSON y Arrow.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::json::reader::{infer_json_schema_from_iterator, ReaderBuilder};
use arrow::json::{ArrayWriter, LineDelimitedWriter};
use arrow::record_batch::RecordBatch;
use orch_core::{OrchError, Result};
use serde::Deserialize;
use serde_json::Value;

/// Una columna declarada a mano en el YAML.
///
/// Se declara como lista y no como mapa para que el orden de las columnas sea
/// el que se escribe: un mapa de YAML pierde el orden al pasar por JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default = "yes")]
    pub nullable: bool,
}

fn yes() -> bool {
    true
}

pub fn schema_from(node: &str, columns: &[ColumnSpec]) -> Result<SchemaRef> {
    if columns.is_empty() {
        return Err(OrchError::config(node, "`schema` no puede estar vacío"));
    }
    let fields = columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                &column.name,
                parse_type(node, &column.data_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn parse_type(node: &str, name: &str) -> Result<DataType> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "int64" | "bigint" | "long" => DataType::Int64,
        "int32" | "int" | "integer" => DataType::Int32,
        "float64" | "double" => DataType::Float64,
        "float32" | "float" => DataType::Float32,
        "utf8" | "string" | "text" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        other => {
            return Err(OrchError::config(
                node,
                format!(
                    "tipo `{other}` desconocido (soportados: int64, int32, float64, float32, \
                     utf8, bool, date, timestamp)"
                ),
            ))
        }
    })
}

/// Deduce el esquema a partir de los registros de la primera página.
pub fn infer(node: &str, records: &[Value]) -> Result<SchemaRef> {
    let schema = infer_json_schema_from_iterator(records.iter().map(Ok)).map_err(|e| {
        OrchError::node(node, format!("no se pudo deducir el esquema del JSON: {e}"))
    })?;
    Ok(Arc::new(schema))
}

/// Baja por una ruta con puntos (`data.items`) dentro de la respuesta.
pub fn value_at<'a>(body: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = body;
    for segment in path.split('.').filter(|s| !s.is_empty()) {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Cadena en una ruta, aceptando también números (hay APIs que devuelven el
/// cursor como entero).
pub fn string_at(body: &Value, path: &str) -> Option<String> {
    match value_at(body, path)? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// Extrae la lista de registros de una respuesta.
pub fn records_at(node: &str, body: &Value, path: Option<&str>) -> Result<Vec<Value>> {
    let path = path.filter(|p| !p.is_empty());
    let located = match path {
        None => body,
        Some(path) => value_at(body, path).ok_or_else(|| {
            OrchError::node(
                node,
                format!("la respuesta no contiene `{path}`: {}", preview(body)),
            )
        })?,
    };

    match located {
        Value::Array(items) => Ok(items.clone()),
        // Una página sin resultados suele venir como `null`, no como `[]`.
        Value::Null => Ok(Vec::new()),
        other => Err(OrchError::node(
            node,
            format!(
                "`{}` no es una lista de registros: {}",
                path.unwrap_or("la respuesta"),
                preview(other)
            ),
        )),
    }
}

fn preview(value: &Value) -> String {
    let text = value.to_string();
    if text.len() <= 200 {
        text
    } else {
        format!("{}…", &text[..200])
    }
}

/// Acumula registros JSON y los va entregando como `RecordBatch`.
pub struct JsonBatcher {
    decoder: arrow::json::reader::Decoder,
    batch_size: usize,
}

impl JsonBatcher {
    pub fn new(node: &str, schema: SchemaRef, batch_size: usize) -> Result<Self> {
        let decoder = ReaderBuilder::new(schema)
            .with_batch_size(batch_size)
            .build_decoder()
            .map_err(|e| {
                OrchError::node(node, format!("no se pudo preparar el lector JSON: {e}"))
            })?;
        Ok(Self {
            decoder,
            batch_size,
        })
    }

    /// Convierte registros en lotes de como mucho `batch_size` filas.
    pub fn push(&mut self, node: &str, records: &[Value]) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for chunk in records.chunks(self.batch_size) {
            self.decoder.serialize(chunk).map_err(|e| {
                OrchError::node(
                    node,
                    format!("un registro no encaja con el esquema esperado: {e}"),
                )
            })?;
            if let Some(batch) = self
                .decoder
                .flush()
                .map_err(|e| OrchError::node(node, format!("no se pudo formar el lote: {e}")))?
            {
                batches.push(batch);
            }
        }
        Ok(batches)
    }
}

/// Serializa un lote como un array JSON.
pub fn to_json_array(node: &str, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = ArrayWriter::new(Vec::new());
    writer
        .write(batch)
        .and_then(|()| writer.finish())
        .map_err(|e| OrchError::node(node, format!("no se pudo serializar a JSON: {e}")))?;
    Ok(writer.into_inner())
}

/// Serializa un lote como JSON delimitado por líneas.
pub fn to_ndjson(node: &str, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = LineDelimitedWriter::new(Vec::new());
    writer
        .write(batch)
        .and_then(|()| writer.finish())
        .map_err(|e| OrchError::node(node, format!("no se pudo serializar a NDJSON: {e}")))?;
    Ok(writer.into_inner())
}
