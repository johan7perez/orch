//! Correspondencia entre los tipos de PostgreSQL y los de Arrow.
//!
//! Es la parte del conector donde de verdad se equivoca uno, así que está
//! aislada aquí y cubierta por tests de ida y vuelta contra una base real.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, Float32Array, Float32Builder, Float64Array, Float64Builder, Int16Array,
    Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, TimeUnit};
use bytes::BytesMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use orch_core::{OrchError, Result};
use postgres_types::{to_sql_checked, IsNull, ToSql, Type};
use tokio_postgres::Row;

use crate::conn::describe;

/// Día cero de Arrow (`Date32` cuenta días desde aquí).
fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 es una fecha válida")
}

/// Tipo Arrow equivalente a uno de PostgreSQL.
pub fn arrow_type(node: &str, column: &str, pg: &Type) -> Result<DataType> {
    Ok(match *pg {
        Type::BOOL => DataType::Boolean,
        Type::INT2 => DataType::Int16,
        Type::INT4 => DataType::Int32,
        Type::INT8 => DataType::Int64,
        Type::FLOAT4 => DataType::Float32,
        Type::FLOAT8 => DataType::Float64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => DataType::Utf8,
        Type::BYTEA => DataType::Binary,
        Type::DATE => DataType::Date32,
        Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        // Se transportan como texto: es su representación natural y evita
        // arrastrar tipos de Arrow que casi nada más entiende.
        Type::UUID | Type::JSON | Type::JSONB => DataType::Utf8,
        ref other => {
            return Err(OrchError::node(
                node,
                format!(
                    "la columna `{column}` es de tipo `{other}`, que todavía no está \
                     soportado. Conviértela en la propia consulta, por ejemplo \
                     `{column}::text`."
                ),
            ))
        }
    })
}

// --- lectura: filas de PostgreSQL -> columnas de Arrow ----------------------

/// Acumula los valores de una columna hasta formar el array de Arrow.
pub enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Text(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder, Option<Arc<str>>),
}

impl ColumnBuilder {
    pub fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => ColumnBuilder::Bool(BooleanBuilder::new()),
            DataType::Int16 => ColumnBuilder::Int16(Int16Builder::new()),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::new()),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::new()),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::new()),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::new()),
            DataType::Binary => ColumnBuilder::Binary(BinaryBuilder::new()),
            DataType::Date32 => ColumnBuilder::Date(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::new(), tz.clone())
            }
            // `arrow_type` sólo produce los de arriba y Utf8.
            _ => ColumnBuilder::Text(StringBuilder::new()),
        }
    }

    /// Lee el valor de una fila y lo añade.
    pub fn append(&mut self, node: &str, column: &str, row: &Row, index: usize) -> Result<()> {
        let fail = |e: tokio_postgres::Error| {
            OrchError::node(
                node,
                format!("no se pudo leer la columna `{column}`: {}", describe(&e)),
            )
        };

        match self {
            ColumnBuilder::Bool(builder) => {
                builder.append_option(row.try_get::<_, Option<bool>>(index).map_err(fail)?)
            }
            ColumnBuilder::Int16(builder) => {
                builder.append_option(row.try_get::<_, Option<i16>>(index).map_err(fail)?)
            }
            ColumnBuilder::Int32(builder) => {
                builder.append_option(row.try_get::<_, Option<i32>>(index).map_err(fail)?)
            }
            ColumnBuilder::Int64(builder) => {
                builder.append_option(row.try_get::<_, Option<i64>>(index).map_err(fail)?)
            }
            ColumnBuilder::Float32(builder) => {
                builder.append_option(row.try_get::<_, Option<f32>>(index).map_err(fail)?)
            }
            ColumnBuilder::Float64(builder) => {
                builder.append_option(row.try_get::<_, Option<f64>>(index).map_err(fail)?)
            }
            ColumnBuilder::Binary(builder) => {
                builder.append_option(row.try_get::<_, Option<&[u8]>>(index).map_err(fail)?)
            }
            ColumnBuilder::Date(builder) => {
                let date = row.try_get::<_, Option<NaiveDate>>(index).map_err(fail)?;
                builder.append_option(date.map(|date| (date - epoch()).num_days() as i32));
            }
            ColumnBuilder::Timestamp(builder, tz) => {
                let micros = match tz {
                    // `timestamptz` llega ya en UTC.
                    Some(_) => row
                        .try_get::<_, Option<DateTime<Utc>>>(index)
                        .map_err(fail)?
                        .map(|value| value.timestamp_micros()),
                    None => row
                        .try_get::<_, Option<NaiveDateTime>>(index)
                        .map_err(fail)?
                        .map(|value| value.and_utc().timestamp_micros()),
                };
                builder.append_option(micros);
            }
            ColumnBuilder::Text(builder) => {
                // Los tipos que no tienen equivalente directo viajan como su
                // representación textual.
                let pg_type = row.columns()[index].type_().clone();
                let text: Option<String> = match pg_type {
                    Type::UUID => row
                        .try_get::<_, Option<uuid::Uuid>>(index)
                        .map_err(fail)?
                        .map(|value| value.to_string()),
                    Type::JSON | Type::JSONB => row
                        .try_get::<_, Option<serde_json::Value>>(index)
                        .map_err(fail)?
                        .map(|value| value.to_string()),
                    _ => row
                        .try_get::<_, Option<&str>>(index)
                        .map_err(fail)?
                        .map(str::to_string),
                };
                builder.append_option(text);
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int16(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int64(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float64(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Text(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Binary(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Date(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Timestamp(builder, tz) => match tz {
                Some(tz) => Arc::new(builder.finish().with_timezone(Arc::clone(tz))),
                None => Arc::new(builder.finish()),
            },
        }
    }
}

// --- escritura: columnas de Arrow -> parámetros de PostgreSQL ---------------

/// Un valor listo para el protocolo binario de `COPY`.
///
/// Existe para poder pasar celdas de tipos distintos por la misma ranura de
/// `&dyn ToSql` sin una caja por celda.
#[derive(Debug)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Binary(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Uuid(uuid::Uuid),
    Json(serde_json::Value),
}

type SqlResult = std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>;

impl ToSql for SqlValue {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> SqlResult {
        match self {
            SqlValue::Null => Ok(IsNull::Yes),
            SqlValue::Bool(value) => value.to_sql(ty, out),
            SqlValue::Int16(value) => value.to_sql(ty, out),
            SqlValue::Int32(value) => value.to_sql(ty, out),
            SqlValue::Int64(value) => value.to_sql(ty, out),
            SqlValue::Float32(value) => value.to_sql(ty, out),
            SqlValue::Float64(value) => value.to_sql(ty, out),
            SqlValue::Text(value) => value.to_sql(ty, out),
            SqlValue::Binary(value) => value.to_sql(ty, out),
            SqlValue::Date(value) => value.to_sql(ty, out),
            SqlValue::Timestamp(value) => value.to_sql(ty, out),
            SqlValue::TimestampTz(value) => value.to_sql(ty, out),
            SqlValue::Uuid(value) => value.to_sql(ty, out),
            SqlValue::Json(value) => value.to_sql(ty, out),
        }
    }

    // La comprobación real la hace el `to_sql` del tipo concreto de dentro.
    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Extrae una celda ya convertida al tipo Arrow que corresponde a la columna
/// de destino.
///
/// El array llega convertido con `arrow::compute::cast`, así que aquí sólo
/// hay que leerlo: el ensanchado de tipos y sus errores los resuelve Arrow.
pub fn value_at(
    node: &str,
    column: &str,
    array: &ArrayRef,
    row: usize,
    pg: &Type,
) -> Result<SqlValue> {
    if array.is_null(row) {
        return Ok(SqlValue::Null);
    }

    let missing = |what: &str| {
        OrchError::node(
            node,
            format!("la columna `{column}` no se pudo leer como {what}"),
        )
    };

    Ok(match *pg {
        Type::BOOL => SqlValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| missing("bool"))?
                .value(row),
        ),
        Type::INT2 => SqlValue::Int16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| missing("int2"))?
                .value(row),
        ),
        Type::INT4 => SqlValue::Int32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| missing("int4"))?
                .value(row),
        ),
        Type::INT8 => SqlValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| missing("int8"))?
                .value(row),
        ),
        Type::FLOAT4 => SqlValue::Float32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| missing("float4"))?
                .value(row),
        ),
        Type::FLOAT8 => SqlValue::Float64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| missing("float8"))?
                .value(row),
        ),
        Type::BYTEA => SqlValue::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| missing("bytea"))?
                .value(row)
                .to_vec(),
        ),
        Type::DATE => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| missing("date"))?
                .value(row);
            SqlValue::Date(epoch() + chrono::Duration::days(days as i64))
        }
        Type::TIMESTAMP | Type::TIMESTAMPTZ => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| missing("timestamp"))?
                .value(row);
            let moment = DateTime::<Utc>::from_timestamp_micros(micros).ok_or_else(|| {
                OrchError::node(
                    node,
                    format!("la columna `{column}` tiene un instante fuera de rango: {micros} µs"),
                )
            })?;
            if *pg == Type::TIMESTAMPTZ {
                SqlValue::TimestampTz(moment)
            } else {
                SqlValue::Timestamp(moment.naive_utc())
            }
        }
        Type::UUID | Type::JSON | Type::JSONB => {
            let text = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| missing("texto"))?
                .value(row);
            match *pg {
                Type::UUID => SqlValue::Uuid(text.parse().map_err(|e| {
                    OrchError::node(
                        node,
                        format!("`{text}` no es un UUID válido para la columna `{column}`: {e}"),
                    )
                })?),
                _ => SqlValue::Json(serde_json::from_str(text).map_err(|e| {
                    OrchError::node(
                        node,
                        format!("la columna `{column}` no contiene JSON válido: {e}"),
                    )
                })?),
            }
        }
        _ => SqlValue::Text(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| missing("texto"))?
                .value(row)
                .to_string(),
        ),
    })
}
