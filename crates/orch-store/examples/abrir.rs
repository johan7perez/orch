//! Cuánto cuesta abrir el historial, escribir un lote de eventos y cerrarlo.
//!
//! ```sh
//! cargo run --release -p orch-store --example abrir -- /tmp/medir.duckdb
//! ```

use std::time::Instant;

use duckdb::{params, Connection};

fn main() {
    let ruta = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "medir.duckdb".into());
    let _ = std::fs::remove_file(&ruta);
    let _ = std::fs::remove_file(format!("{ruta}.wal"));
    {
        let c = Connection::open(&ruta).unwrap();
        c.execute_batch(
            "CREATE TABLE events (run_id VARCHAR, seq BIGINT, at_us BIGINT, kind VARCHAR, node VARCHAR, detail VARCHAR, payload VARCHAR);",
        )
        .unwrap();
        // Un historial con algo de vida: 200 000 eventos.
        let mut app = c.appender("events").unwrap();
        for i in 0..200_000i64 {
            app.append_row(params![
                format!("run-{}", i / 1000),
                i,
                i,
                "node_finished",
                "n",
                "d",
                "{}"
            ])
            .unwrap();
        }
    }

    for (nombre, cerrar) in [
        ("abriendo y cerrando en cada lote", true),
        ("con la conexión abierta", false),
    ] {
        let mut abierta = None;
        let inicio = Instant::now();
        let vueltas = 50;
        for v in 0..vueltas {
            if abierta.is_none() {
                abierta = Some(Connection::open(&ruta).unwrap());
            }
            let c = abierta.as_ref().unwrap();
            let mut app = c.appender("events").unwrap();
            for i in 0..256i64 {
                app.append_row(params!["nueva", v * 256 + i, i, "progress", "n", "d", "{}"])
                    .unwrap();
            }
            drop(app);
            if cerrar {
                abierta = None;
            }
        }
        let ms = inicio.elapsed().as_secs_f64() * 1000.0 / vueltas as f64;
        println!("{nombre}: {ms:.1} ms por lote de 256 eventos");
    }
}
