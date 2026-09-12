//! Conector Parquet (origen y destino).
//!
//! Es el formato donde Arrow rinde mejor: columnar en disco, columnar en
//! memoria, sin conversión de por medio. Dos cosas que el CSV no puede dar:
//!
//! - **El esquema sale de los metadatos**, sin leer una sola fila, así que
//!   `orch validate` lo conoce siempre y con exactitud (tipos incluidos, no
//!   inferidos).
//! - **La proyección se empuja al fichero**: `columns:` no recorta después de
//!   leer, evita descomprimir las columnas que no se piden.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{
    Input, InputSchemas, NodeContext, OrchError, Output, PushdownOp, Registry, Result, Sink, Source,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::util::yes;

pub fn register(registry: &mut Registry) {
    registry.register_source("parquet", |node, config| {
        let source: Arc<dyn Source> = Arc::new(ParquetSource::new(node, config));
        Ok(source)
    });
    registry.register_sink("parquet", |node, config| {
        let sink: Arc<dyn Sink> = Arc::new(ParquetSink::new(node, config));
        Ok(sink)
    });

    // Un `select` justo detrás se convierte en no leer esas columnas. Un
    // `filter` no: Parquet puede saltarse grupos de filas por estadísticas,
    // pero eso exige evaluar la expresión y todavía no se hace.
    registry.register_pushdown("parquet", |config, op| {
        let PushdownOp::Select { columns } = op else {
            return false;
        };
        // Si ya venía con una proyección, la del `select` tiene que ser un
        // subconjunto suyo; comprobarlo aquí es más lío que dejarlo estar.
        if config.get("columns").is_some_and(|c| !c.is_null()) {
            return false;
        }
        let Some(map) = config.as_object_mut() else {
            return false;
        };
        map.insert("columns".to_string(), json!(columns));
        true
    });
}

// --- origen -----------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetSourceConfig {
    pub path: PathBuf,
    /// Columnas a leer. Las demás ni se descomprimen.
    ///
    /// Conserva el orden del fichero, no el de esta lista: es una proyección
    /// de lectura, no un `select`. Para reordenar, encadena un `select`.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Sobrescribe `settings.batch_size` sólo para este nodo.
    #[serde(default)]
    pub batch_size: Option<usize>,
}

pub struct ParquetSource {
    node: String,
    config: ParquetSourceConfig,
}

impl ParquetSource {
    pub fn new(node: impl Into<String>, config: ParquetSourceConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }
}

/// Cómo se recorta y se ordena lo que sale del fichero.
struct Projection {
    /// Esquema que produce el nodo, ya en el orden pedido.
    schema: SchemaRef,
    /// Reordenación a aplicar a cada lote, si el orden pedido no coincide
    /// con el del fichero. El lector siempre devuelve las columnas en el
    /// orden en que están escritas.
    reorder: Option<Vec<usize>>,
}

/// Índices de las columnas pedidas, en el orden en que se pidieron.
fn requested_indices(node: &str, schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                OrchError::node(
                    node,
                    format!(
                        "la columna `{name}` no existe en el fichero (disponibles: {})",
                        schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                )
            })
        })
        .collect()
}

/// Abre el fichero y lee su pie de metadatos.
///
/// Se separa de [`project`] a propósito: que el fichero no esté disponible
/// todavía no invalida el pipeline, pero que se pida una columna inexistente
/// sí. Sin la separación habría que distinguirlos por el texto del error.
fn open_builder(
    node: &str,
    config: &ParquetSourceConfig,
) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let file = File::open(&config.path).map_err(|e| {
        OrchError::node(
            node,
            format!("no se pudo abrir `{}`: {e}", config.path.display()),
        )
    })?;

    ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        OrchError::node(
            node,
            format!("`{}` no es un Parquet válido: {e}", config.path.display()),
        )
    })
}

/// Aplica la proyección y devuelve cómo queda la salida.
fn project(
    node: &str,
    mut builder: ParquetRecordBatchReaderBuilder<File>,
    config: &ParquetSourceConfig,
) -> Result<(ParquetRecordBatchReaderBuilder<File>, Projection)> {
    let file_schema = SchemaRef::clone(builder.schema());
    let Some(columns) = &config.columns else {
        return Ok((
            builder,
            Projection {
                schema: file_schema,
                reorder: None,
            },
        ));
    };

    let requested = requested_indices(node, &file_schema, columns)?;

    // La máscara del lector no entiende de orden ni de repeticiones: se le
    // pasa el conjunto, y luego se reordena el lote.
    let mut read: Vec<usize> = requested.clone();
    read.sort_unstable();
    read.dedup();

    let mask = ProjectionMask::roots(builder.parquet_schema(), read.iter().copied());
    builder = builder.with_projection(mask);

    let reorder: Vec<usize> = requested
        .iter()
        .map(|index| {
            read.binary_search(index)
                .expect("cada columna pedida está en el conjunto leído")
        })
        .collect();
    let identity = reorder.iter().copied().eq(0..read.len());

    Ok((
        builder,
        Projection {
            schema: Arc::new(file_schema.project(&requested)?),
            reorder: (!identity).then_some(reorder),
        },
    ))
}

#[async_trait]
impl Source for ParquetSource {
    fn connector(&self) -> &str {
        "parquet"
    }

    /// El esquema está en el pie del fichero: se lee sin tocar los datos, y
    /// con los tipos reales en vez de inferidos.
    async fn schema(&self) -> Result<Option<SchemaRef>> {
        let node = self.node.clone();
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || match open_builder(&node, &config) {
            Ok(builder) => {
                project(&node, builder, &config).map(|(_, projection)| Some(projection.schema))
            }
            Err(err) => {
                tracing::debug!(
                    node = %node,
                    path = %config.path.display(),
                    error = %err,
                    "no se pudo leer el esquema todavía"
                );
                Ok(None)
            }
        })
        .await?
    }

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        let batch_size = self.config.batch_size.unwrap_or(ctx.settings.batch_size);
        if batch_size == 0 {
            return Err(OrchError::config(&self.node, "`batch_size` debe ser > 0"));
        }

        let node = self.node.clone();
        let config = self.config.clone();
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch>>(2);

        let reader = tokio::task::spawn_blocking(move || {
            if let Err(err) = read_blocking(&node, &config, batch_size, &tx) {
                let _ = tx.blocking_send(Err(err));
            }
        });

        while let Some(item) = rx.recv().await {
            output.send(item?).await?;
            if output.is_closed() {
                break;
            }
        }
        drop(rx);

        reader.await?;
        Ok(())
    }
}

fn read_blocking(
    node: &str,
    config: &ParquetSourceConfig,
    batch_size: usize,
    tx: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let (builder, projection) = project(node, open_builder(node, config)?, config)?;
    tracing::debug!(
        path = %config.path.display(),
        columns = projection.schema.fields().len(),
        batch_size,
        "Parquet abierto"
    );

    let reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| OrchError::node(node, format!("no se pudo abrir el lector Parquet: {e}")))?;

    for batch in reader {
        let batch =
            batch.map_err(|e| OrchError::node(node, format!("Parquet mal formado: {e}")))?;
        let batch = match &projection.reorder {
            Some(reorder) => batch.project(reorder)?,
            None => batch,
        };
        if tx.blocking_send(Ok(batch)).is_err() {
            break;
        }
    }
    Ok(())
}

// --- destino ----------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParquetSinkConfig {
    pub path: PathBuf,
    #[serde(default = "yes")]
    pub create_dirs: bool,
    /// `snappy` (por defecto), `zstd`, `gzip`, `lz4` o `none`.
    #[serde(default = "default_compression")]
    pub compression: String,
    /// Filas por grupo de filas. Grupos grandes comprimen mejor y permiten
    /// saltar más al leer; grupos pequeños bajan la memoria del escritor.
    #[serde(default)]
    pub row_group_size: Option<usize>,
}

fn default_compression() -> String {
    "snappy".to_string()
}

fn compression_from(node: &str, name: &str) -> Result<Compression> {
    match name.to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        "gzip" => Ok(Compression::GZIP(GzipLevel::default())),
        "lz4" => Ok(Compression::LZ4),
        other => Err(OrchError::config(
            node,
            format!("compresión `{other}` desconocida (soportadas: none, snappy, zstd, gzip, lz4)"),
        )),
    }
}

pub struct ParquetSink {
    node: String,
    config: ParquetSinkConfig,
}

impl ParquetSink {
    pub fn new(node: impl Into<String>, config: ParquetSinkConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }
}

#[async_trait]
impl Sink for ParquetSink {
    fn connector(&self) -> &str {
        "parquet"
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<()> {
        compression_from(&self.node, &self.config.compression)?;
        inputs.concatenated(&self.node).map(|_| ())
    }

    async fn write(&self, ctx: &NodeContext, input: &mut Input) -> Result<()> {
        // Un Parquet lleva el esquema en el pie, así que hace falta conocerlo
        // aunque no llegue ni una fila. Si `prepare` lo resolvió, se usa y un
        // resultado vacío produce igualmente un fichero legible.
        let known = ctx.inputs.concatenated(&self.node)?;

        let node = self.node.clone();
        let config = self.config.clone();
        let (tx, rx) = mpsc::channel::<RecordBatch>(2);
        let writer = tokio::task::spawn_blocking(move || write_blocking(&node, &config, known, rx));

        let mut upstream_error = None;
        loop {
            match input.recv().await {
                Ok(Some(batch)) => {
                    if tx.send(batch).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    upstream_error = Some(err);
                    break;
                }
            }
        }
        drop(tx);

        let write_result = writer.await?;
        match upstream_error {
            Some(err) => Err(err),
            None => write_result,
        }
    }
}

fn write_blocking(
    node: &str,
    config: &ParquetSinkConfig,
    known_schema: Option<SchemaRef>,
    mut rx: mpsc::Receiver<RecordBatch>,
) -> Result<()> {
    let mut properties =
        WriterProperties::builder().set_compression(compression_from(node, &config.compression)?);
    if let Some(size) = config.row_group_size {
        properties = properties.set_max_row_group_row_count(Some(size));
    }
    let properties = properties.build();

    let mut writer: Option<ArrowWriter<File>> = None;

    // El fichero no se crea hasta conocer el esquema: un Parquet sin pie no
    // se puede leer, y dejar uno a medias sería peor que no dejar nada.
    let open_writer = |schema: SchemaRef| -> Result<ArrowWriter<File>> {
        if config.create_dirs {
            if let Some(parent) = config.path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        OrchError::node(
                            node,
                            format!("no se pudo crear `{}`: {e}", parent.display()),
                        )
                    })?;
                }
            }
        }
        let file = File::create(&config.path).map_err(|e| {
            OrchError::node(
                node,
                format!("no se pudo crear `{}`: {e}", config.path.display()),
            )
        })?;
        ArrowWriter::try_new(file, schema, Some(properties.clone())).map_err(|e| {
            OrchError::node(node, format!("no se pudo abrir el escritor Parquet: {e}"))
        })
    };

    if let Some(schema) = known_schema.clone() {
        writer = Some(open_writer(schema)?);
    }

    while let Some(batch) = rx.blocking_recv() {
        if writer.is_none() {
            writer = Some(open_writer(batch.schema())?);
        }
        writer
            .as_mut()
            .expect("abierto justo arriba")
            .write(&batch)
            .map_err(|e| OrchError::node(node, format!("no se pudo escribir el Parquet: {e}")))?;
    }

    match writer {
        // `close` escribe el pie con los metadatos; sin él no hay fichero.
        Some(writer) => writer
            .close()
            .map(|_| ())
            .map_err(|e| OrchError::node(node, format!("no se pudo cerrar el Parquet: {e}"))),
        None => Err(OrchError::node(
            node,
            format!(
                "no llegó ninguna fila y el esquema de entrada no se pudo determinar en \
                 `validate`, así que no hay un Parquet válido que escribir en `{}`",
                config.path.display()
            ),
        )),
    }
}
