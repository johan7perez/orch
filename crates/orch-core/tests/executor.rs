//! Semántica de ejecución: flujo de datos, fallos, reintentos y barreras.
//!
//! Los conectores se definen aquí mismo: el motor no debe necesitar ningún
//! conector real para ser verificable.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use orch_core::arrow::array::Int64Array;
use orch_core::arrow::datatypes::{DataType, Field, Schema};
use orch_core::arrow::record_batch::RecordBatch;
use orch_core::{
    Dag, Executor, Input, NoConfig, NodeContext, NodeStatus, OrchError, Output, PipelineSpec,
    Registry, Result, RunReport, Sink, Source,
};

fn batch(rows: usize, start: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids = Int64Array::from_iter_values((0..rows as i64).map(|i| start + i));
    RecordBatch::try_new(schema, vec![Arc::new(ids)]).expect("batch válido")
}

// --- conectores de prueba ---------------------------------------------------

/// Emite `batches` lotes de `rows` filas.
struct Emitter {
    batches: usize,
    rows: usize,
}

#[async_trait]
impl Source for Emitter {
    fn connector(&self) -> &str {
        "emitter"
    }
    async fn read(&self, _ctx: &NodeContext, output: &Output) -> Result<()> {
        for i in 0..self.batches {
            output
                .send(batch(self.rows, (i * self.rows) as i64))
                .await?;
        }
        Ok(())
    }
}

/// Falla siempre, sin emitir nada.
struct Exploding;

#[async_trait]
impl Source for Exploding {
    fn connector(&self) -> &str {
        "exploding"
    }
    async fn read(&self, ctx: &NodeContext, _output: &Output) -> Result<()> {
        Err(OrchError::node(&ctx.node, "boom"))
    }
}

/// Falla en los primeros `fail_until - 1` intentos y luego funciona.
struct Flaky {
    fail_until: u32,
    attempts: Arc<AtomicU32>,
}

#[async_trait]
impl Source for Flaky {
    fn connector(&self) -> &str {
        "flaky"
    }
    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if ctx.attempt < self.fail_until {
            return Err(OrchError::node(&ctx.node, "todavía no"));
        }
        output.send(batch(3, 0)).await
    }
}

/// Emite un lote y después falla: ya hay datos en vuelo, así que reintentar
/// no es seguro.
struct EmitsThenFails;

#[async_trait]
impl Source for EmitsThenFails {
    fn connector(&self) -> &str {
        "emits-then-fails"
    }
    async fn read(&self, ctx: &NodeContext, output: &Output) -> Result<()> {
        output.send(batch(2, 0)).await?;
        Err(OrchError::node(&ctx.node, "se rompió a mitad"))
    }
}

/// Consume despacio, para provocar contrapresión aguas arriba.
struct SlowSink {
    per_batch: std::time::Duration,
    rows: Arc<AtomicU32>,
}

#[async_trait]
impl Sink for SlowSink {
    fn connector(&self) -> &str {
        "slow"
    }
    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> Result<()> {
        while let Some(batch) = input.recv().await? {
            tokio::time::sleep(self.per_batch).await;
            self.rows
                .fetch_add(batch.num_rows() as u32, Ordering::SeqCst);
        }
        Ok(())
    }
}

/// Cuenta las filas recibidas y anota su id al terminar.
struct Recorder {
    rows: Arc<AtomicU32>,
    finished: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Sink for Recorder {
    fn connector(&self) -> &str {
        "recorder"
    }
    async fn write(&self, ctx: &NodeContext, input: &mut Input) -> Result<()> {
        while let Some(batch) = input.recv().await? {
            self.rows
                .fetch_add(batch.num_rows() as u32, Ordering::SeqCst);
        }
        self.finished
            .lock()
            .expect("mutex sano")
            .push(ctx.node.clone());
        Ok(())
    }
}

// --- andamiaje --------------------------------------------------------------

#[derive(Clone, Default)]
struct Probe {
    rows: Arc<AtomicU32>,
    attempts: Arc<AtomicU32>,
    finished: Arc<Mutex<Vec<String>>>,
}

impl Probe {
    fn rows(&self) -> u32 {
        self.rows.load(Ordering::SeqCst)
    }
    fn attempts(&self) -> u32 {
        self.attempts.load(Ordering::SeqCst)
    }
    fn finish_order(&self) -> Vec<String> {
        self.finished.lock().expect("mutex sano").clone()
    }
}

// Configs de los componentes de prueba. Ahora que el registro deserializa por
// ti, declararlas sale más corto que hurgar en el `Value` a mano, y de paso
// los campos que faltan los reporta serde con el nombre del nodo.
#[derive(serde::Deserialize, schemars::JsonSchema)]
struct EmitterConfig {
    batches: usize,
    rows: usize,
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct FlakyConfig {
    fail_until: u32,
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct SlowConfig {
    ms_per_batch: u64,
}

fn registry(probe: &Probe) -> Arc<Registry> {
    let mut registry = Registry::new();

    registry.register_source("emitter", |_node, config: EmitterConfig| {
        let source: Arc<dyn Source> = Arc::new(Emitter {
            batches: config.batches,
            rows: config.rows,
        });
        Ok(source)
    });

    registry.register_source("exploding", |_node, _config: NoConfig| {
        let source: Arc<dyn Source> = Arc::new(Exploding);
        Ok(source)
    });

    let attempts = Arc::clone(&probe.attempts);
    registry.register_source("flaky", move |_node, config: FlakyConfig| {
        let source: Arc<dyn Source> = Arc::new(Flaky {
            fail_until: config.fail_until,
            attempts: Arc::clone(&attempts),
        });
        Ok(source)
    });

    registry.register_source("emits-then-fails", |_node, _config: NoConfig| {
        let source: Arc<dyn Source> = Arc::new(EmitsThenFails);
        Ok(source)
    });

    let slow_rows = Arc::clone(&probe.rows);
    registry.register_sink("slow", move |_node, config: SlowConfig| {
        let sink: Arc<dyn Sink> = Arc::new(SlowSink {
            per_batch: std::time::Duration::from_millis(config.ms_per_batch),
            rows: Arc::clone(&slow_rows),
        });
        Ok(sink)
    });

    let rows = Arc::clone(&probe.rows);
    let finished = Arc::clone(&probe.finished);
    registry.register_sink("recorder", move |_node, _config: NoConfig| {
        let sink: Arc<dyn Sink> = Arc::new(Recorder {
            rows: Arc::clone(&rows),
            finished: Arc::clone(&finished),
        });
        Ok(sink)
    });

    Arc::new(registry)
}

async fn run(yaml: &str, probe: &Probe) -> RunReport {
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido");
    Executor::new(registry(probe))
        .run(&dag)
        .await
        .expect("la ejecución debería arrancar")
}

fn status_of<'a>(report: &'a RunReport, id: &str) -> &'a orch_core::NodeReport {
    report
        .nodes
        .iter()
        .find(|n| n.id == id)
        .unwrap_or_else(|| panic!("no hay informe para `{id}`"))
}

// --- tests ------------------------------------------------------------------

#[tokio::test]
async fn un_pipeline_lineal_entrega_todas_las_filas() {
    let probe = Probe::default();
    let report = run(
        r#"
name: lineal
nodes:
  - { id: src, type: source, connector: emitter, config: { batches: 5, rows: 100 } }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: src, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(probe.rows(), 500);
    assert_eq!(report.rows_written(), 500);
    assert_eq!(status_of(&report, "src").output.rows, 500);
}

#[tokio::test]
async fn el_fan_out_entrega_a_todos_los_consumidores() {
    let probe = Probe::default();
    let report = run(
        r#"
name: fan-out
nodes:
  - { id: src, type: source, connector: emitter, config: { batches: 2, rows: 10 } }
  - { id: a, type: sink, connector: recorder }
  - { id: b, type: sink, connector: recorder }
edges:
  - { from: src, to: a }
  - { from: src, to: b }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    // Cada consumidor ve el flujo completo: 20 filas cada uno.
    assert_eq!(probe.rows(), 40);
}

#[tokio::test]
async fn el_fan_in_concatena_las_entradas() {
    let probe = Probe::default();
    let report = run(
        r#"
name: fan-in
nodes:
  - { id: a, type: source, connector: emitter, config: { batches: 1, rows: 7 } }
  - { id: b, type: source, connector: emitter, config: { batches: 1, rows: 3 } }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: a, to: dst }
  - { from: b, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(probe.rows(), 10);
}

#[tokio::test]
async fn un_source_que_falla_deja_el_sink_omitido_y_no_lo_da_por_bueno() {
    let probe = Probe::default();
    let report = run(
        r#"
name: fallo
nodes:
  - { id: src, type: source, connector: exploding }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: src, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(!report.succeeded);
    assert_eq!(status_of(&report, "src").status, NodeStatus::Failed);
    // Lo importante: el sink NO se reporta como correcto con 0 filas.
    assert_eq!(status_of(&report, "dst").status, NodeStatus::Skipped);
}

#[tokio::test]
async fn reintenta_hasta_que_el_nodo_tiene_exito() {
    let probe = Probe::default();
    let report = run(
        r#"
name: reintento
nodes:
  - id: src
    type: source
    connector: flaky
    config: { fail_until: 3 }
    retry: { max_attempts: 5, backoff_ms: 1 }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: src, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(probe.attempts(), 3);
    assert_eq!(status_of(&report, "src").attempts, 3);
    assert_eq!(probe.rows(), 3);
}

#[tokio::test]
async fn no_reintenta_si_el_nodo_ya_emitio_datos() {
    let probe = Probe::default();
    let report = run(
        r#"
name: sin-reintento
nodes:
  - id: src
    type: source
    connector: emits-then-fails
    retry: { max_attempts: 5, backoff_ms: 1 }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: src, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(!report.succeeded);
    let src = status_of(&report, "src");
    assert_eq!(src.status, NodeStatus::Failed);
    assert_eq!(
        src.attempts, 1,
        "reintentar duplicaría las filas ya emitidas"
    );
    assert!(src
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("datos en vuelo"));
}

#[tokio::test]
async fn after_ordena_ramas_independientes() {
    let probe = Probe::default();
    let report = run(
        r#"
name: barrera
nodes:
  - { id: src_a, type: source, connector: emitter, config: { batches: 1, rows: 1 } }
  - { id: sink_a, type: sink, connector: recorder }
  - { id: src_b, type: source, connector: emitter, config: { batches: 1, rows: 1 }, after: [sink_a] }
  - { id: sink_b, type: sink, connector: recorder }
edges:
  - { from: src_a, to: sink_a }
  - { from: src_b, to: sink_b }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(probe.finish_order(), vec!["sink_a", "sink_b"]);
}

#[tokio::test]
async fn una_barrera_incumplida_omite_la_rama_entera() {
    let probe = Probe::default();
    let report = run(
        r#"
name: barrera-rota
nodes:
  - { id: src_a, type: source, connector: exploding }
  - { id: sink_a, type: sink, connector: recorder }
  - { id: src_b, type: source, connector: emitter, config: { batches: 1, rows: 1 }, after: [sink_a] }
  - { id: sink_b, type: sink, connector: recorder }
edges:
  - { from: src_a, to: sink_a }
  - { from: src_b, to: sink_b }
"#,
        &probe,
    )
    .await;

    assert!(!report.succeeded);
    assert_eq!(status_of(&report, "src_b").status, NodeStatus::Skipped);
    assert_eq!(status_of(&report, "sink_b").status, NodeStatus::Skipped);
    assert_eq!(probe.rows(), 0);
}

#[tokio::test]
async fn la_contrapresion_queda_medida_en_el_informe() {
    // El productor va sobrado y el consumidor no da abasto: el tiempo que el
    // productor pasa esperando es lo que señala dónde está el cuello de
    // botella, y tiene que quedar registrado.
    let probe = Probe::default();
    let report = run(
        r#"
name: contrapresion
settings: { channel_capacity: 1 }
nodes:
  - { id: src, type: source, connector: emitter, config: { batches: 6, rows: 10 } }
  - { id: dst, type: sink, connector: slow, config: { ms_per_batch: 20 } }
edges:
  - { from: src, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(probe.rows(), 60);

    let src = status_of(&report, "src");
    assert!(
        src.output.stalled_ms >= 40,
        "el productor debería haber esperado al consumidor: {} ms",
        src.output.stalled_ms
    );

    // El consumidor nunca espera: siempre tiene trabajo pendiente.
    let dst = status_of(&report, "dst");
    assert!(
        dst.input.stalled_ms < src.output.stalled_ms,
        "el consumidor no debería ser el que espera (in {} ms, out {} ms)",
        dst.input.stalled_ms,
        src.output.stalled_ms
    );
}

#[tokio::test]
async fn sin_contrapresion_no_se_contabiliza_espera() {
    // El camino rápido no debe pagar nada por la instrumentación: si el
    // consumidor va sobrado, no hay espera que medir.
    let probe = Probe::default();
    let report = run(
        r#"
name: sin-espera
settings: { channel_capacity: 16 }
nodes:
  - { id: src, type: source, connector: emitter, config: { batches: 4, rows: 10 } }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: src, to: dst }
"#,
        &probe,
    )
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(status_of(&report, "src").output.stalled_ms, 0);
}

#[tokio::test]
async fn un_conector_desconocido_se_detecta_antes_de_ejecutar() {
    let probe = Probe::default();
    let dag = Dag::build(
        PipelineSpec::from_yaml_str(
            "test.yaml",
            r#"
name: desconocido
nodes:
  - { id: src, type: source, connector: no-existe }
  - { id: dst, type: sink, connector: recorder }
edges:
  - { from: src, to: dst }
"#,
        )
        .expect("YAML válido"),
    )
    .expect("DAG válido");

    let err = Executor::new(registry(&probe))
        .prepare(&dag)
        .await
        .expect_err("debería fallar");
    assert!(err.to_string().contains("no-existe"), "{err}");
}
