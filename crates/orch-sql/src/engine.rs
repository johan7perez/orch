//! Ejecución de una query de DataFusion sobre el flujo de un nodo.

use std::sync::Arc;

use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use orch_core::{Input, NodeContext, OrchError, Output, Result};
use tokio::sync::mpsc;

use crate::table::{BatchResult, InputPartition};

/// Una transformación SQL lista para ejecutarse.
///
/// El `SessionContext` se construye una sola vez, al crear el nodo, para que
/// un pipeline preparado una vez y ejecutado muchas no repita el montaje del
/// catálogo de funciones de DataFusion. En release eso cuesta menos de 1 ms
/// por nodo; en debug, cientos. Lo único que ocurre por ejecución es
/// registrar la tabla de entrada.
pub(crate) struct Plan {
    pub node: String,
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
}

/// Ejecuta la query alimentándola con `input` y publicando el resultado en
/// `output`.
///
/// El esquema de la tabla se toma del primer lote que llega: en la Fase 0 los
/// orígenes no declaran su esquema por adelantado. Consecuencia: una entrada
/// vacía produce una salida vacía, incluso para un `COUNT(*)` que en SQL puro
/// devolvería una fila con 0. Se resolverá cuando la propagación estática de
/// esquemas entre en `validate`.
pub(crate) async fn execute(
    plan: &Plan,
    ctx: &NodeContext,
    input: &mut Input,
    output: &Output,
) -> Result<()> {
    let Some(first) = input.recv().await? else {
        tracing::debug!(
            table = %plan.table,
            "entrada vacía: no hay esquema con el que planificar la query"
        );
        return Ok(());
    };
    let schema = first.schema();

    let (tx, rx) = mpsc::channel::<BatchResult>(ctx.settings.channel_capacity.max(1));
    let partition: Arc<dyn PartitionStream> = Arc::new(InputPartition::new(
        Arc::clone(&schema),
        plan.table.clone(),
        rx,
    ));
    let provider = StreamingTable::try_new(Arc::clone(&schema), vec![partition])
        .map_err(|e| plan.error("no se pudo construir la tabla de entrada", e))?;

    // La sesión se reutiliza entre intentos; volver a registrar sustituye la
    // tabla anterior, que en ese punto ya está agotada.
    plan.session
        .register_table(TableReference::bare(plan.table.clone()), Arc::new(provider))
        .map_err(|e| plan.error("no se pudo registrar la tabla de entrada", e))?;

    let frame = plan
        .session
        .sql(&plan.query)
        .await
        .map_err(|e| plan.error("la query no se pudo planificar", e))?;
    let mut results = frame
        .execute_stream()
        .await
        .map_err(|e| plan.error("la query no se pudo ejecutar", e))?;

    // El alimentador y el drenador tienen que avanzar a la vez: DataFusion
    // pide lotes mientras nosotros publicamos los que ya produjo.
    let mut feeder = Box::pin(async move {
        if tx.send(Ok(first)).await.is_err() {
            return Ok(());
        }
        loop {
            match input.recv().await {
                // Un canal cerrado significa que el plan ya no quiere más
                // datos (un LIMIT, por ejemplo). No es un error.
                Ok(Some(batch)) => {
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(());
                    }
                }
                Ok(None) => return Ok(()),
                Err(err) => return Err(err),
            }
        }
        // Al completarse el bloque, `tx` se libera y DataFusion ve el fin del
        // flujo.
    });

    let mut feeding = true;
    let mut feed_error: Option<OrchError> = None;

    loop {
        let batch = if feeding {
            tokio::select! {
                item = results.next() => item,
                outcome = &mut feeder => {
                    feeding = false;
                    if let Err(err) = outcome {
                        feed_error = Some(err);
                    }
                    continue;
                }
            }
        } else {
            results.next().await
        };

        match batch {
            Some(batch) => {
                let batch = batch.map_err(|e| plan.error("la query falló a mitad", e))?;
                output.send(batch).await?;
            }
            None => break,
        }
    }

    // Si el plan terminó sin agotar la entrada (un LIMIT, o un escaneo que el
    // optimizador podó), soltar el alimentador deja de leer aguas arriba.
    drop(feeder);

    match feed_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Comprueba que la query es sintácticamente válida sin ejecutarla ni conocer
/// el esquema. Es lo que permite que `orch validate` detecte un `SELCT`.
pub(crate) fn check_syntax(node: &str, query: &str) -> Result<()> {
    datafusion::sql::parser::DFParser::parse_sql(query)
        .map(|_| ())
        .map_err(|err| OrchError::config(node, format!("SQL inválido: {err}")))
}
