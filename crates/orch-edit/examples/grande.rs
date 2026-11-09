//! Un pipeline de cientos de nodos, para medir el editor y el lienzo.
//!
//!     cargo run -p orch-edit --example grande -- 500 /tmp/grande/grande.yaml
//!     cargo run --release -p orch-edit --example grande -- 500
//!
//! Escribe el pipeline —ramas de `generator → limit… → null`, con alguna
//! espera `after` entre ramas— y mide cuánto tarda cada operación del editor
//! sobre él. Es la referencia de la Fase 2.4: el lienzo tiene que seguir
//! siendo usable con un pipeline así, y esto dice dónde se va el tiempo.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use orch_core::{Dag, PipelineSpec};
use orch_edit::{apply, Edit, Kind};

/// Nodos por rama: un origen, ocho `limit` y un destino.
const POR_RAMA: usize = 10;

fn main() {
    let mut args = std::env::args().skip(1);
    let pedidos: usize = args.next().and_then(|n| n.parse().ok()).unwrap_or(500);
    let ramas = pedidos.div_ceil(POR_RAMA).max(2);
    let texto = generar(ramas);

    if let Some(ruta) = args.next() {
        std::fs::write(&ruta, &texto).expect("no se pudo escribir el pipeline");
        println!("escrito en {ruta}");
    }

    let spec = PipelineSpec::from_yaml_str_as_written("grande", &texto).unwrap();
    println!(
        "{} nodos, {} aristas, {} líneas, {} KB\n",
        spec.nodes.len(),
        spec.edges.len(),
        texto.lines().count(),
        texto.len() / 1024
    );

    medir("leer (serde)", || {
        PipelineSpec::from_yaml_str_as_written("grande", &texto).unwrap()
    });
    medir("validar (Dag::build)", || Dag::build(spec.clone()).unwrap());

    // Las operaciones, sobre el centro del grafo: ni al principio ni al
    // final del fichero, donde algunas serían más baratas.
    let medio = ramas / 2;
    let todos: BTreeMap<String, [f64; 2]> = spec
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            (
                n.id.clone(),
                [(i % POR_RAMA) as f64 * 244.0, (i / POR_RAMA) as f64 * 76.0],
            )
        })
        .collect();
    let con_layout = apply(
        &texto,
        &[Edit::SetLayout {
            positions: todos.clone(),
        }],
    )
    .unwrap();

    let casos: Vec<(&str, &str, Edit)> = vec![
        (
            "añadir nodo",
            &texto,
            Edit::AddNode {
                id: "nuevo".into(),
                kind: Kind::Transform,
                component: "limit".into(),
                config: Default::default(),
            },
        ),
        (
            "conectar",
            &texto,
            Edit::Connect {
                from: format!("paso_{medio}_3"),
                to: format!("destino_{}", medio + 1),
                port: None,
            },
        ),
        (
            "desconectar",
            &texto,
            Edit::Disconnect {
                from: format!("paso_{medio}_3"),
                to: format!("paso_{medio}_4"),
            },
        ),
        (
            "cambiar un campo",
            &texto,
            Edit::SetConfig {
                node: format!("paso_{medio}_3"),
                key: "rows".into(),
                value: Some(serde_json::json!(5)),
            },
        ),
        (
            "renombrar",
            &texto,
            Edit::RenameNode {
                from: format!("paso_{medio}_3"),
                to: "renombrado".into(),
            },
        ),
        (
            "quitar nodo",
            &texto,
            Edit::RemoveNode {
                id: format!("paso_{medio}_3"),
            },
        ),
        (
            "primer arrastre (fija todos)",
            &texto,
            Edit::SetLayout { positions: todos },
        ),
        (
            "mover un nodo",
            &con_layout,
            Edit::SetLayout {
                positions: [(format!("paso_{medio}_3"), [1.0, 2.0])].into(),
            },
        ),
        (
            "quitar nodo con layout",
            &con_layout,
            Edit::RemoveNode {
                id: format!("paso_{medio}_3"),
            },
        ),
        ("colocar solo", &con_layout, Edit::ClearLayout),
    ];

    for (nombre, base, edit) in casos {
        medir(nombre, || apply(base, std::slice::from_ref(&edit)).unwrap());
    }
}

/// La mediana de varias ejecuciones: la primera paga cachés frías.
fn medir<T>(nombre: &str, f: impl Fn() -> T) {
    let mut tiempos: Vec<Duration> = (0..5)
        .map(|_| {
            let t = Instant::now();
            std::hint::black_box(f());
            t.elapsed()
        })
        .collect();
    tiempos.sort();
    println!("{nombre:<30} {:>9.1} ms", tiempos[2].as_secs_f64() * 1000.0);
}

fn generar(ramas: usize) -> String {
    let mut nodos = String::new();
    let mut aristas = String::new();
    for b in 0..ramas {
        let ids: Vec<String> = (0..POR_RAMA)
            .map(|k| match k {
                0 => format!("origen_{b}"),
                k if k == POR_RAMA - 1 => format!("destino_{b}"),
                k => format!("paso_{b}_{k}"),
            })
            .collect();

        nodos.push_str(&format!(
            "  - id: {}\n    type: source\n    connector: generator\n",
            ids[0]
        ));
        // Cada diez ramas, una espera la anterior: aristas punteadas y capas
        // más profundas, como en un pipeline de verdad con fases.
        if b > 0 && b % 10 == 0 {
            nodos.push_str(&format!("    after: [destino_{}]\n", b - 1));
        }
        nodos.push_str("    config:\n      rows: 1000\n      with_text: false\n\n");
        for (k, id) in ids.iter().enumerate().take(POR_RAMA - 1).skip(1) {
            nodos.push_str(&format!(
                "  - id: {id}\n    type: transform\n    op: limit\n    config:\n      rows: {}\n\n",
                1000 - k
            ));
        }
        nodos.push_str(&format!(
            "  - id: {}\n    type: sink\n    connector: \"null\"\n\n",
            ids[POR_RAMA - 1]
        ));
        for par in ids.windows(2) {
            aristas.push_str(&format!("  - {{ from: {}, to: {} }}\n", par[0], par[1]));
        }
    }

    format!(
        "# Generado por `cargo run -p orch-edit --example grande`: un pipeline\n\
         # grande para medir el editor y el lienzo.\n\n\
         version: 1\n\
         name: grande-{n}\n\
         description: {ramas} ramas de {POR_RAMA} nodos para medir el lienzo.\n\n\
         nodes:\n{nodos}edges:\n{aristas}",
        n = ramas * POR_RAMA,
        nodos = nodos.trim_end_matches('\n').to_string() + "\n\n"
    )
}
