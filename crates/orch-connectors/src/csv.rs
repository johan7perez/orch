//! Conector CSV (origen y destino), sobre el lector/escritor de `arrow-csv`.

use std::fs::File;
use std::io::Seek;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::csv::reader::{Format, ReaderBuilder};
use arrow::csv::WriterBuilder;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use orch_core::{
    parse_config, Input, InputSchemas, NodeContext, OrchError, Output, Registry, Result, Sink,
    Source,
};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::util::{comma, delimiter_byte, yes};

pub fn register(registry: &mut Registry) {
    registry.register_source("csv", |node, config| {
        let source: Arc<dyn Source> = Arc::new(CsvSource::new(node, parse_config(node, config)?));
        Ok(source)
    });
    registry.register_sink("csv", |node, config| {
        let sink: Arc<dyn Sink> = Arc::new(CsvSink::new(node, parse_config(node, config)?));
        Ok(sink)
    });
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvSourceConfig {
    pub path: PathBuf,
    #[serde(default = "yes")]
    pub has_header: bool,
    #[serde(default = "comma")]
    pub delimiter: char,
    /// Filas que se leen para inferir el esquema. `0` = escanear el fichero
    /// completo (más lento, pero seguro con columnas cuyo tipo sólo se
    /// distingue al final).
    #[serde(default = "default_infer_rows")]
    pub infer_rows: usize,
    /// Sobrescribe `settings.batch_size` sólo para este nodo.
    #[serde(default)]
    pub batch_size: Option<usize>,
}

fn default_infer_rows() -> usize {
    1_000
}

pub struct CsvSource {
    node: String,
    config: CsvSourceConfig,
}

impl CsvSource {
    pub fn new(node: impl Into<String>, config: CsvSourceConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }
}

#[async_trait]
impl Source for CsvSource {
    fn connector(&self) -> &str {
        "csv"
    }

    /// Infiere el esquema leyendo sólo la cabecera y las primeras filas.
    ///
    /// Si el fichero todavía no existe se devuelve `None`, no un error: es
    /// legítimo que lo produzca un paso anterior o una ejecución programada,
    /// y `validate` no debería exigir que las fuentes estén disponibles.
    async fn schema(&self) -> Result<Option<SchemaRef>> {
        let node = self.node.clone();
        let config = self.config.clone();
        let inferred = tokio::task::spawn_blocking(move || infer_schema(&node, &config)).await?;

        match inferred {
            Ok(schema) => Ok(Some(schema)),
            Err(err) => {
                tracing::debug!(
                    node = %self.node,
                    path = %self.config.path.display(),
                    error = %err,
                    "no se pudo inferir el esquema todavía"
                );
                Ok(None)
            }
        }
    }

    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        let batch_size = self.config.batch_size.unwrap_or(ctx.settings.batch_size);
        if batch_size == 0 {
            return Err(OrchError::config(&self.node, "`batch_size` debe ser > 0"));
        }

        let node = self.node.clone();
        let config = self.config.clone();
        // Un canal corto: la contrapresión real la impone `output.send`, este
        // buffer sólo evita que el hilo bloqueante se pare en cada batch.
        let (tx, mut rx) = mpsc::channel::<Result<RecordBatch>>(2);

        let reader = tokio::task::spawn_blocking(move || {
            if let Err(err) = read_blocking(&node, &config, batch_size, &tx) {
                let _ = tx.blocking_send(Err(err));
            }
        });

        while let Some(item) = rx.recv().await {
            output.send(item?).await?;
            // Si aguas abajo ya nadie escucha (por ejemplo un `limit` que
            // alcanzó su cuota), no tiene sentido seguir leyendo el fichero.
            if output.is_closed() {
                break;
            }
        }
        drop(rx);

        // Al cerrarse el canal, el hilo bloqueante sale de su bucle.
        reader.await?;
        Ok(())
    }
}

/// Abre el fichero y deduce su esquema, dejando el cursor al principio.
fn open_and_infer(node: &str, config: &CsvSourceConfig) -> Result<(File, Format, SchemaRef)> {
    let mut file = File::open(&config.path).map_err(|e| {
        OrchError::node(
            node,
            format!("no se pudo abrir `{}`: {e}", config.path.display()),
        )
    })?;

    let format = Format::default()
        .with_header(config.has_header)
        .with_delimiter(delimiter_byte(node, config.delimiter)?);

    let max_records = (config.infer_rows > 0).then_some(config.infer_rows);
    let (schema, _read) = format.infer_schema(&mut file, max_records).map_err(|e| {
        OrchError::node(
            node,
            format!(
                "no se pudo inferir el esquema de `{}`: {e}",
                config.path.display()
            ),
        )
    })?;
    file.rewind()?;

    Ok((file, format, Arc::new(schema)))
}

fn infer_schema(node: &str, config: &CsvSourceConfig) -> Result<SchemaRef> {
    open_and_infer(node, config).map(|(_, _, schema)| schema)
}

fn read_blocking(
    node: &str,
    config: &CsvSourceConfig,
    batch_size: usize,
    tx: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let (file, format, schema) = open_and_infer(node, config)?;

    tracing::debug!(
        path = %config.path.display(),
        columns = schema.fields().len(),
        batch_size,
        "esquema CSV inferido"
    );

    let reader = ReaderBuilder::new(schema)
        .with_format(format)
        .with_batch_size(batch_size)
        .build(file)
        .map_err(|e| OrchError::node(node, format!("no se pudo abrir el lector CSV: {e}")))?;

    for batch in reader {
        let batch = batch.map_err(|e| OrchError::node(node, format!("CSV mal formado: {e}")))?;
        // El receptor se cierra cuando el pipeline se detiene: no es un error.
        if tx.blocking_send(Ok(batch)).is_err() {
            break;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CsvSinkConfig {
    pub path: PathBuf,
    #[serde(default = "yes")]
    pub has_header: bool,
    #[serde(default = "comma")]
    pub delimiter: char,
    /// Crea los directorios intermedios si no existen.
    #[serde(default = "yes")]
    pub create_dirs: bool,
}

pub struct CsvSink {
    node: String,
    config: CsvSinkConfig,
}

impl CsvSink {
    pub fn new(node: impl Into<String>, config: CsvSinkConfig) -> Self {
        Self {
            node: node.into(),
            config,
        }
    }
}

#[async_trait]
impl Sink for CsvSink {
    fn connector(&self) -> &str {
        "csv"
    }

    /// Un CSV tiene una sola cabecera, así que todas las entradas deben traer
    /// las mismas columnas. `concatenated` lo comprueba en `validate`, antes
    /// de crear el fichero.
    async fn plan(&self, inputs: &InputSchemas) -> Result<()> {
        inputs.concatenated(&self.node).map(|_| ())
    }

    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> Result<()> {
        let node = self.node.clone();
        let config = self.config.clone();
        let (tx, rx) = mpsc::channel::<RecordBatch>(2);

        let writer = tokio::task::spawn_blocking(move || write_blocking(&node, &config, rx));

        // Un fallo aguas arriba no debe dejar el fichero a medio cerrar: se
        // recuerda el error, se cierra el escritor y se reporta después.
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
    config: &CsvSinkConfig,
    mut rx: mpsc::Receiver<RecordBatch>,
) -> Result<()> {
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

    let mut writer = WriterBuilder::new()
        .with_header(config.has_header)
        .with_delimiter(delimiter_byte(node, config.delimiter)?)
        .build(file);

    while let Some(batch) = rx.blocking_recv() {
        writer
            .write(&batch)
            .map_err(|e| OrchError::node(node, format!("no se pudo escribir el CSV: {e}")))?;
    }

    // El escritor de `arrow-csv` vuelca su buffer al soltarse; el `File` que
    // hay debajo no tiene buffer propio, así que aquí el fichero queda completo.
    drop(writer);
    Ok(())
}
