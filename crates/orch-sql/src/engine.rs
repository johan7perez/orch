//! Ejecución de una query de DataFusion sobre los flujos de un nodo.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use orch_core::{Input, InputPort, InputSchemas, NodeContext, OrchError, Output, Result};
use tokio::sync::mpsc;

use crate::table::{BatchResult, InputPartition};

/// Una transformación SQL lista para ejecutarse.
///
/// El `SessionContext` se construye una sola vez, al crear el nodo, para que
/// un pipeline preparado una vez y ejecutado muchas no repita el montaje del
/// catálogo de funciones de DataFusion. En release eso cuesta menos de 1 ms
/// por nodo; en debug, cientos. Lo único que ocurre por ejecución es
/// registrar las tablas de entrada.
pub(crate) struct Plan {
    pub node: String,
    /// Nombre con el que se registra la entrada cuando el nodo tiene una sola.
    pub table: String,
    pub query: String,
    pub session: SessionContext,
}

impl Plan {
    pub(crate) fn new(
        node: &str,
        table: String,
        query: String,
        memory_limit_mb: Option<usize>,
    ) -> Result<Self> {
        let config = SessionConfig::new();
        let session = match memory_limit_mb {
            None => SessionContext::new_with_config(config),
            Some(mb) => {
                let runtime = RuntimeEnvBuilder::new()
                    .with_memory_limit(mb * 1024 * 1024, 1.0)
                    .build_arc()
                    .map_err(|e| {
                        OrchError::config(
                            node,
                            format!("no se pudo aplicar `memory_limit_mb`: {e}"),
                        )
                    })?;
                SessionContext::new_with_config_rt(config, runtime)
            }
        };
        Ok(Self {
            node: node.to_string(),
            table,
            query,
            session,
        })
    }

    fn error(&self, context: &str, err: DataFusionError) -> OrchError {
        OrchError::node(&self.node, format!("{context}: {err}"))
    }

    /// (Re)registra una tabla en la sesión.
    ///
    /// `register_table` falla si el nombre ya existe, y aquí siempre existe:
    /// la sesión se comparte entre la planificación (tablas vacías) y cada
    /// intento de ejecución (tablas en streaming). Dar de baja primero es lo
    /// que hace idempotente el registro.
    fn register(&self, name: &str, provider: Arc<dyn TableProvider>) -> Result<()> {
        let reference = TableReference::bare(name.to_string());
        let _ = self.session.deregister_table(reference.clone());
        self.session
            .register_table(reference, provider)
            .map(|_| ())
            .map_err(|e| self.error("no se pudo registrar la tabla de entrada", e))
    }

    /// Nombres de tabla bajo los que se registra una entrada.
    ///
    /// Cada puerto se registra con su propio nombre —por defecto el id del
    /// nodo de origen, que es lo que permite escribir un join sin sintaxis
    /// nueva—. Cuando hay una sola entrada se registra además bajo `table`
    /// (`input` por defecto), que es como se escriben las queries de un solo
    /// flujo y lo que generan `filter`, `derive` y `aggregate`.
    fn table_names(&self, port: &str, single: bool) -> Vec<String> {
        let mut names = vec![port.to_string()];
        if single && port != self.table {
            names.push(self.table.clone());
        }
        names
    }

    /// Registra tablas vacías con el esquema conocido y planifica la query.
    ///
    /// Es lo que convierte una columna inexistente en un error de `validate`.
    /// Devuelve el esquema de salida.
    pub(crate) async fn plan_schema(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        if !inputs.all_known() {
            return Ok(None);
        }
        let single = inputs.len() == 1;
        for port in inputs.ports() {
            let schema = port.schema.as_ref().expect("all_known lo garantiza");
            let empty = Arc::new(
                MemTable::try_new(SchemaRef::clone(schema), vec![vec![]])
                    .map_err(|e| self.error("no se pudo preparar la tabla de entrada", e))?,
            );
            for name in self.table_names(&port.port, single) {
                self.register(&name, empty.clone())?;
            }
        }

        let frame = self
            .session
            .sql(&self.query)
            .await
            .map_err(|e| self.error("la query no se pudo planificar", e))?;
        Ok(Some(Arc::new(frame.schema().as_arrow().clone())))
    }
}

/// Ejecuta la query alimentándola con las entradas y publicando el resultado.
///
/// Cuando `prepare` pudo resolver los esquemas, se usan directamente. Si no
/// (un origen que no sabe declararlos), se toma el esquema del primer lote,
/// lo que obliga a que el nodo tenga una sola entrada: espiar varias en serie
/// podría bloquear el pipeline si comparten un origen aguas arriba.
pub(crate) async fn execute(
    plan: &Plan,
    ctx: &NodeContext,
    input: &mut Input,
    output: &Output,
) -> Result<()> {
    let ports = input.take_ports();
    if ports.is_empty() {
        return Err(OrchError::node(&plan.node, "el nodo no tiene entradas"));
    }
    let single = ports.len() == 1;

    // Resolver el esquema de cada puerto, espiando sólo si hace falta.
    let mut sources = Vec::with_capacity(ports.len());
    for mut port in ports {
        let (schema, first) = match ctx.inputs.get(&port.name).and_then(|p| p.schema.clone()) {
            Some(schema) => (schema, None),
            None if single => match port.recv().await? {
                Some(batch) => (batch.schema(), Some(batch)),
                // Entrada vacía y sin esquema declarado: no hay nada que
                // planificar. Con esquema estático sí habría plan, y un
                // COUNT(*) devolvería una fila con 0.
                None => {
                    tracing::debug!(
                        node = %plan.node,
                        "entrada vacía y sin esquema declarado: no hay query que ejecutar"
                    );
                    return Ok(());
                }
            },
            None => {
                return Err(OrchError::node(
                    &plan.node,
                    format!(
                        "la entrada `{}` no declara su esquema, y un nodo con varias entradas \
                         necesita conocerlos todos antes de ejecutar",
                        port.name
                    ),
                ))
            }
        };
        sources.push((port, schema, first));
    }

    // Registrar cada entrada como una tabla que DataFusion irá tirando.
    let mut feeders: FuturesUnordered<_> = FuturesUnordered::new();
    for (port, schema, first) in sources {
        let (tx, rx) = mpsc::channel::<BatchResult>(ctx.settings.channel_capacity.max(1));
        let partition: Arc<dyn PartitionStream> = Arc::new(InputPartition::new(
            SchemaRef::clone(&schema),
            port.name.clone(),
            rx,
        ));
        let provider = StreamingTable::try_new(schema, vec![partition])
            .map_err(|e| plan.error("no se pudo construir la tabla de entrada", e))?;
        let provider = Arc::new(provider);

        // La sesión se reutiliza entre intentos: el registro sustituye la
        // tabla anterior, que en ese punto ya está agotada.
        for name in plan.table_names(&port.name, single) {
            plan.register(&name, provider.clone())?;
        }

        feeders.push(feed(port, first, tx));
    }

    let frame = plan
        .session
        .sql(&plan.query)
        .await
        .map_err(|e| plan.error("la query no se pudo planificar", e))?;
    let mut results = frame
        .execute_stream()
        .await
        .map_err(|e| plan.error("la query no se pudo ejecutar", e))?;

    // Alimentadores y drenador tienen que avanzar a la vez: DataFusion pide
    // lotes de cualquier entrada mientras nosotros publicamos lo que produjo.
    let mut feed_error: Option<OrchError> = None;

    loop {
        let batch = if feeders.is_empty() {
            results.next().await
        } else {
            tokio::select! {
                item = results.next() => item,
                Some(outcome) = feeders.next() => {
                    if let Err(err) = outcome {
                        feed_error.get_or_insert(err);
                    }
                    continue;
                }
            }
        };

        match batch {
            Some(batch) => {
                let batch = batch.map_err(|e| plan.error("la query falló a mitad", e))?;
                output.send(batch).await?;
            }
            None => break,
        }
    }

    // Si el plan terminó sin agotar las entradas (un LIMIT, o un escaneo que
    // el optimizador podó), soltar los alimentadores deja de leer aguas
    // arriba en vez de quedarse esperando.
    drop(feeders);

    match feed_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Vuelca un puerto en el canal que consume DataFusion.
async fn feed(
    mut port: InputPort,
    first: Option<arrow::record_batch::RecordBatch>,
    tx: mpsc::Sender<BatchResult>,
) -> Result<()> {
    if let Some(batch) = first {
        if tx.send(Ok(batch)).await.is_err() {
            return Ok(());
        }
    }
    loop {
        match port.recv().await {
            // Un canal cerrado significa que el plan ya no quiere más datos
            // (un LIMIT, por ejemplo). No es un error.
            Ok(Some(batch)) => {
                if tx.send(Ok(batch)).await.is_err() {
                    return Ok(());
                }
            }
            Ok(None) => return Ok(()),
            Err(err) => return Err(err),
        }
    }
    // Al completarse, `tx` se libera y DataFusion ve el fin del flujo.
}

/// Comprueba que la query es sintácticamente válida sin ejecutarla ni conocer
/// el esquema. Es lo que permite que `orch validate` detecte un `SELCT`.
pub(crate) fn check_syntax(node: &str, query: &str) -> Result<()> {
    datafusion::sql::parser::DFParser::parse_sql(query)
        .map(|_| ())
        .map_err(|err| OrchError::config(node, format!("SQL inválido: {err}")))
}
