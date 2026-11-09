//! Throughput del motor sin E/S, para comparar cambios en el ejecutor.
//!
//!     cargo run --release -p orch-connectors --example motor
//!
//! Dos pipelines del generador al sink nulo: el de las líneas base del
//! README (10 M filas) y uno de 1 000 M que dura lo bastante para que la
//! telemetría periódica actúe muchas veces. Se ejecutan con un oyente de
//! eventos enganchado, como lo tienen la CLI y la aplicación: lo que se mide
//! es lo que paga una ejecución de verdad.

use std::time::{Duration, Instant};

use orch_core::{Dag, Executor, PipelineSpec};

fn yaml(filas: u64) -> String {
    format!(
        "name: motor
settings: {{ batch_size: 65536 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: {filas}, with_text: false }} }}
  - {{ id: tirar, type: sink, connector: \"null\" }}
edges:
  - {{ from: generar, to: tirar }}
"
    )
}

#[tokio::main]
async fn main() {
    for filas in [10_000_000u64, 1_000_000_000] {
        let dag =
            Dag::build(PipelineSpec::from_yaml_str("motor.yaml", &yaml(filas)).unwrap()).unwrap();
        let mut tiempos = Vec::new();
        let mut eventos = 0usize;
        for _ in 0..5 {
            let executor = Executor::new(orch_connectors::default_registry_arc());
            let mut oyente = executor.subscribe();
            let contar = tokio::spawn(async move {
                let mut n = 0usize;
                while oyente.recv().await.is_ok() {
                    n += 1;
                }
                n
            });
            let t = Instant::now();
            let informe = executor.run(&dag).await.unwrap();
            tiempos.push(t.elapsed());
            assert!(informe.succeeded);
            drop(executor);
            eventos = contar.await.unwrap();
        }
        tiempos.sort();
        let mediana: Duration = tiempos[2];
        println!(
            "{:>11} filas  {:>8.1} ms  {:>6.1} M filas/s  ({} eventos por ejecución)",
            filas,
            mediana.as_secs_f64() * 1000.0,
            filas as f64 / mediana.as_secs_f64() / 1e6,
            eventos
        );
    }
}
