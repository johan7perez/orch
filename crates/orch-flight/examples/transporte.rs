//! Cuánto cuesta que una arista cruce la red.
//!
//! El mismo flujo del generador por tres caminos:
//!
//! 1. **En memoria**: generador → null, la arista de siempre (un canal de
//!    `Arc`s, sin copiar nada).
//! 2. **Sólo IPC**: los mismos lotes codificados a Arrow IPC como mensajes de
//!    Flight y decodificados otra vez, sin red. Es el coste de serializar.
//! 3. **Flight**: generador → sink `flight` → (red local) → origen `flight` →
//!    null, en el mismo proceso pero hablando gRPC por `127.0.0.1`.
//!
//! ```sh
//! cargo run --release -p orch-flight --example transporte -- 20000000
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use futures::{StreamExt, TryStreamExt};
use orch_core::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use orch_core::arrow::datatypes::{DataType, Field, Schema};
use orch_core::arrow::record_batch::RecordBatch;
use orch_core::{Dag, Executor, PipelineSpec, Registry, RunReport};

const LOTE: usize = 65_536;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let filas: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(20_000_000);

    let mut registry = orch_connectors::default_registry();
    orch_flight::register(&mut registry);
    let registry = Arc::new(registry);

    println!("{filas} filas, lotes de {LOTE}\n");
    println!(
        "  {:<26} {:>10} {:>14} {:>11}",
        "camino", "tiempo", "filas/s", "MB/s"
    );
    for texto in [false, true] {
        let etiqueta = if texto { "con texto" } else { "sin texto" };
        // El volumen es el de los datos en Arrow, el mismo para los tres: lo
        // que dice el sink del camino Flight cuenta de más, porque las
        // columnas decodificadas comparten el búfer del mensaje.
        let (ms, bytes) = en_memoria(&registry, filas, texto).await;
        fila(&format!("en memoria, {etiqueta}"), filas, ms, bytes);
        let ms = solo_ipc(filas, texto).await;
        fila(&format!("sólo IPC, {etiqueta}"), filas, ms, bytes);
        let ms = por_flight(&registry, filas, texto).await;
        fila(&format!("Flight local, {etiqueta}"), filas, ms, bytes);
        println!();
    }
}

fn fila(camino: &str, filas: u64, ms: f64, bytes: u64) {
    println!(
        "  {:<26} {:>8.0} ms {:>14.0} {:>11.0}",
        camino,
        ms,
        filas as f64 / (ms / 1000.0),
        bytes as f64 / 1e6 / (ms / 1000.0)
    );
}

fn generador(filas: u64, texto: bool) -> String {
    format!(
        "{{ id: generar, type: source, connector: generator, config: {{ rows: {filas}, with_text: {texto} }} }}"
    )
}

async fn correr(registry: &Arc<Registry>, yaml: &str) -> RunReport {
    let dag = Dag::build(PipelineSpec::from_yaml_str("bench.yaml", yaml).unwrap()).unwrap();
    let informe = Executor::new(Arc::clone(registry)).run(&dag).await.unwrap();
    assert!(informe.succeeded, "{informe:?}");
    informe
}

fn bytes_de(informe: &RunReport, nodo: &str) -> u64 {
    informe
        .nodes
        .iter()
        .find(|n| n.id == nodo)
        .unwrap()
        .input
        .bytes
}

async fn en_memoria(registry: &Arc<Registry>, filas: u64, texto: bool) -> (f64, u64) {
    let yaml = format!(
        "name: memoria\nsettings: {{ batch_size: {LOTE} }}\nnodes:\n  - {}\n  - {{ id: tirar, type: sink, connector: \"null\" }}\nedges:\n  - {{ from: generar, to: tirar }}\n",
        generador(filas, texto)
    );
    let inicio = Instant::now();
    let informe = correr(registry, &yaml).await;
    (
        inicio.elapsed().as_secs_f64() * 1000.0,
        bytes_de(&informe, "tirar"),
    )
}

async fn por_flight(registry: &Arc<Registry>, filas: u64, texto: bool) -> f64 {
    let puerto = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    // Las dos mitades en un mismo pipeline: dos componentes sin aristas
    // entre ellos, unidos sólo por la red.
    let yaml = format!(
        r#"
name: flight
settings: {{ batch_size: {LOTE} }}
nodes:
  - {}
  - {{ id: servir, type: sink, connector: flight, config: {{ listen: "127.0.0.1:{puerto}", ticket: bench }} }}
  - {{ id: leer, type: source, connector: flight, config: {{ url: "http://127.0.0.1:{puerto}", ticket: bench }} }}
  - {{ id: tirar, type: sink, connector: "null" }}
edges:
  - {{ from: generar, to: servir }}
  - {{ from: leer, to: tirar }}
"#,
        generador(filas, texto)
    );
    let inicio = Instant::now();
    correr(registry, &yaml).await;
    inicio.elapsed().as_secs_f64() * 1000.0
}

/// Codificar y decodificar como lo hace Flight, sin red de por medio.
async fn solo_ipc(filas: u64, texto: bool) -> f64 {
    let lotes = lotes(filas, texto);
    let inicio = Instant::now();
    let datos =
        FlightDataEncoderBuilder::new().build(futures::stream::iter(lotes.into_iter().map(Ok)));
    let mut decodificado = FlightRecordBatchStream::new_from_flight_data(datos.map_err(Into::into));
    let mut contadas = 0u64;
    while let Some(lote) = decodificado.next().await {
        contadas += lote.unwrap().num_rows() as u64;
    }
    assert_eq!(contadas, filas);
    inicio.elapsed().as_secs_f64() * 1000.0
}

/// Lotes como los del generador, ya hechos: aquí sólo se mide el transporte.
fn lotes(filas: u64, texto: bool) -> Vec<RecordBatch> {
    let mut campos = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ];
    if texto {
        campos.push(Field::new("label", DataType::Utf8, false));
    }
    let schema = Arc::new(Schema::new(campos));
    let mut lotes = Vec::new();
    let mut hechas = 0u64;
    while hechas < filas {
        let n = (filas - hechas).min(LOTE as u64);
        let ids: Vec<i64> = (hechas..hechas + n).map(|i| i as i64).collect();
        let mut columnas: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Float64Array::from_iter_values(
                ids.iter().map(|&i| i as f64 * 1.5),
            )),
        ];
        if texto {
            columnas.push(Arc::new(StringArray::from_iter_values(
                ids.iter().map(|i| format!("row-{i}")),
            )));
        }
        lotes.push(RecordBatch::try_new(Arc::clone(&schema), columnas).unwrap());
        hechas += n;
    }
    lotes
}
