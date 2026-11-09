//! Cuánto cuesta la frontera de WebAssembly frente a la misma transformación
//! nativa.
//!
//!     cargo build --release --target wasm32-wasip2 \
//!         -p orch-plugin-identidad -p orch-plugin-mayusculas
//!     cargo run --release -p orch-wasm --example medir
//!
//! Es la medida que pedía el PLAN antes de comprometerse con los plugins: el
//! mismo código, compilado de las dos formas, sobre los mismos lotes. Con
//! `identidad` se ve el coste puro de cruzar (copiar, serializar, llamar);
//! con `mayusculas`, cuánto pesa eso frente a trabajo de verdad.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use orch_plugin::Transformacion;
use orch_wasm::{escribir_ipc, leer_ipc, Plugin};

const FILAS: usize = 2_000_000;

fn main() {
    let carpeta =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/wasm32-wasip2/release");
    let motor = orch_wasm::motor().unwrap();

    for nombre in ["identidad", "mayusculas"] {
        let ruta = carpeta.join(format!("orch_plugin_{nombre}.wasm"));
        let t = Instant::now();
        let plugin =
            Plugin::cargar(&motor, &ruta).expect("compila los plugins primero (ver arriba)");
        let carga = t.elapsed();
        // La segunda vez, de la caché de compilación: es lo que paga cada
        // arranque de la CLI o de la aplicación.
        let t = Instant::now();
        drop(Plugin::cargar(&motor, &ruta).unwrap());
        let recarga = t.elapsed();
        let t = Instant::now();
        let mut instancia = plugin.instanciar().unwrap();
        let instanciar = t.elapsed();
        println!(
            "\n## {nombre}  ({} KB · cargar {} ms, otra vez {} ms · instanciar {:.2} ms)",
            std::fs::metadata(&ruta).unwrap().len() / 1024,
            carga.as_millis(),
            recarga.as_millis(),
            instanciar.as_secs_f64() * 1000.0
        );
        println!(
            "{:>8} {:>12} {:>12} {:>9} {:>11} {:>11} {:>11}",
            "lote", "nativo", "wasm", "x", "serializar", "llamar", "deserial."
        );

        let config = match nombre {
            "mayusculas" => serde_json::json!({ "columna": "label" }),
            _ => serde_json::json!({}),
        };
        instancia.iniciar("medir", &config).unwrap();

        for tamano in [1_024usize, 4_096, 8_192, 16_384, 65_536] {
            let lotes = generar(tamano);

            // Nativo: el mismo tipo, llamado directamente.
            let nativo = mejor_de(3, || {
                let t = Instant::now();
                match nombre {
                    "mayusculas" => {
                        let mut p = orch_plugin_mayusculas::Mayusculas::nueva(
                            serde_json::from_value(config.clone()).unwrap(),
                        )
                        .unwrap();
                        for lote in &lotes {
                            std::hint::black_box(p.procesar(lote.clone()).unwrap());
                        }
                    }
                    _ => {
                        let mut p =
                            orch_plugin_identidad::Identidad::nueva(Default::default()).unwrap();
                        for lote in &lotes {
                            std::hint::black_box(p.procesar(lote.clone()).unwrap());
                        }
                    }
                }
                (t.elapsed(), Duration::ZERO, Duration::ZERO, Duration::ZERO)
            });

            // WASM, desglosado: lo que paga el host al serializar, la llamada
            // (que incluye leer y escribir IPC dentro del plugin y su trabajo)
            // y lo que paga al deserializar lo que vuelve.
            let wasm = mejor_de(3, || {
                let (mut ser, mut llamar, mut de) =
                    (Duration::ZERO, Duration::ZERO, Duration::ZERO);
                let total = Instant::now();
                for lote in &lotes {
                    let t = Instant::now();
                    let bytes = escribir_ipc(lote).unwrap();
                    ser += t.elapsed();
                    let t = Instant::now();
                    let salidas = instancia.procesar_ipc("medir", &bytes).unwrap();
                    llamar += t.elapsed();
                    let t = Instant::now();
                    for s in &salidas {
                        std::hint::black_box(leer_ipc(s).unwrap());
                    }
                    de += t.elapsed();
                }
                (total.elapsed(), ser, llamar, de)
            });

            let ns = |d: Duration| d.as_nanos() as f64 / FILAS as f64;
            println!(
                "{:>8} {:>9.1} ns {:>9.1} ns {:>8.1}x {:>8.1} ns {:>8.1} ns {:>8.1} ns   (por fila)",
                tamano,
                ns(nativo.0),
                ns(wasm.0),
                wasm.0.as_secs_f64() / nativo.0.as_secs_f64(),
                ns(wasm.1),
                ns(wasm.2),
                ns(wasm.3),
            );
        }
    }
}

/// El mejor de varios intentos: lo que cuesta de verdad, sin el ruido de la
/// máquina.
fn mejor_de(
    veces: usize,
    mut f: impl FnMut() -> (Duration, Duration, Duration, Duration),
) -> (Duration, Duration, Duration, Duration) {
    (0..veces).map(|_| f()).min_by_key(|r| r.0).unwrap()
}

/// Lotes como los del `generator`: un entero, un decimal y un texto.
fn generar(tamano: usize) -> Vec<RecordBatch> {
    let esquema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    (0..FILAS)
        .step_by(tamano)
        .map(|desde| {
            let hasta = (desde + tamano).min(FILAS);
            RecordBatch::try_new(
                Arc::clone(&esquema),
                vec![
                    Arc::new(Int64Array::from_iter_values(desde as i64..hasta as i64)),
                    Arc::new(Float64Array::from_iter_values(
                        (desde..hasta).map(|i| i as f64 * 0.5),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        (desde..hasta).map(|i| format!("cliente-{i}")),
                    )),
                ],
            )
            .unwrap()
        })
        .collect()
}
