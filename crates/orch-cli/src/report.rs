//! Salida legible de la CLI.

use orch_core::{Dag, NodeStatus, Pushed, Registry, RunReport};

pub fn print_registry(registry: &Registry) {
    print_list("sources", &registry.source_names());
    print_list("transforms", &registry.transform_names());
    print_list("sinks", &registry.sink_names());
    print_list("con pushdown", &registry.pushdown_names());
}

/// Qué trabajo se empujó hasta el origen.
///
/// Se muestra siempre que ocurra: el pipeline que se ejecuta ya no es el que
/// está escrito en el YAML, y eso hay que decirlo.
pub fn print_pushdown(pushed: &[Pushed]) {
    if pushed.is_empty() {
        return;
    }
    println!();
    println!("empujado hasta el origen:");
    for item in pushed {
        println!("  {} de `{}` → `{}`", item.op, item.removed, item.into);
    }
}

fn print_list(title: &str, names: &[&str]) {
    println!("{title}:");
    if names.is_empty() {
        println!("  (ninguno)");
    }
    for name in names {
        println!("  {name}");
    }
}

pub fn print_graph(dag: &Dag) {
    let spec = dag.spec();
    println!("{}", spec.name);
    if let Some(description) = &spec.description {
        println!("{description}");
    }
    println!(
        "batch_size={} channel_capacity={}",
        spec.settings.batch_size, spec.settings.channel_capacity
    );
    println!();

    for &i in dag.topological_order() {
        let node = dag.node(i);
        println!(
            "{} [{}:{}]",
            node.id,
            node.kind.label(),
            node.kind.component()
        );

        let upstream: Vec<&str> = dag
            .upstream(i)
            .iter()
            .map(|&u| dag.node(u).id.as_str())
            .collect();
        if !upstream.is_empty() {
            println!("    ← datos de: {}", upstream.join(", "));
        }
        let barriers: Vec<&str> = dag
            .barriers(i)
            .iter()
            .map(|&b| dag.node(b).id.as_str())
            .collect();
        if !barriers.is_empty() {
            println!("    ⏱ espera a: {}", barriers.join(", "));
        }
        if node.retry.max_attempts > 1 {
            println!("    ↻ hasta {} intentos", node.retry.max_attempts);
        }
    }
}

pub fn print_run(report: &RunReport) {
    println!();
    println!("pipeline : {}", report.pipeline);
    println!("run_id   : {}", report.run_id);
    println!(
        "estado   : {}",
        if report.succeeded {
            "correcto"
        } else {
            "fallido"
        }
    );
    println!("duración : {}", human_ms(report.elapsed_ms));
    println!();

    let id_width = report
        .nodes
        .iter()
        .map(|n| n.id.len())
        .max()
        .unwrap_or(4)
        .max(4);

    println!(
        "  {:<id_width$}  {:<10}  {:>12}  {:>12}  {:>10}  {:>14}",
        "nodo",
        "estado",
        "filas in",
        "filas out",
        "tiempo",
        "filas/s",
        id_width = id_width
    );

    for node in &report.nodes {
        let status = match node.status {
            NodeStatus::Succeeded => "ok",
            NodeStatus::Failed => "fallo",
            NodeStatus::Skipped => "omitido",
        };
        // Para un sink lo relevante son las filas que consumió; para el resto,
        // las que produjo.
        let moved = if node.kind == "sink" {
            node.input.rows
        } else {
            node.output.rows
        };
        println!(
            "  {:<id_width$}  {:<10}  {:>12}  {:>12}  {:>10}  {:>14}",
            node.id,
            status,
            thousands(node.input.rows),
            thousands(node.output.rows),
            human_ms(node.elapsed_ms),
            rate(moved, node.elapsed_ms),
            id_width = id_width
        );
    }

    let failures: Vec<_> = report.nodes.iter().filter(|n| n.error.is_some()).collect();
    if !failures.is_empty() {
        println!();
        for node in failures {
            println!(
                "  {}: {}",
                node.id,
                node.error.as_deref().unwrap_or("error desconocido")
            );
        }
    }
    println!();
    println!("filas escritas: {}", thousands(report.rows_written()));
}

pub fn print_runs(runs: &[orch_store::RunSummary]) {
    if runs.is_empty() {
        println!("no hay ejecuciones guardadas todavía");
        return;
    }

    let pipeline_width = runs
        .iter()
        .map(|r| r.pipeline.len())
        .max()
        .unwrap_or(8)
        .max(8);

    println!(
        "  {:<8}  {:<pipeline_width$}  {:<19}  {:<9}  {:>10}  {:>7}",
        "id",
        "pipeline",
        "arrancó",
        "estado",
        "duración",
        "nodos",
        pipeline_width = pipeline_width
    );
    for run in runs {
        let nodes = if run.failed_nodes > 0 {
            format!("{}✗/{}", run.failed_nodes, run.nodes)
        } else {
            run.nodes.to_string()
        };
        println!(
            "  {:<8}  {:<pipeline_width$}  {:<19}  {:<9}  {:>10}  {:>7}",
            short_id(&run.run_id),
            run.pipeline,
            run.started_at.format("%Y-%m-%d %H:%M:%S"),
            translate_status(&run.status),
            human_ms(run.elapsed_ms),
            nodes,
            pipeline_width = pipeline_width
        );
    }
}

pub fn print_logs(run_id: &str, nodes: &[orch_store::NodeRow], events: &[orch_store::EventRow]) {
    println!("run_id : {run_id}");
    println!();

    let width = nodes.iter().map(|n| n.node.len()).max().unwrap_or(4).max(4);
    println!(
        "  {:<width$}  {:<9}  {:>12}  {:>14}  {:>10}  {:>10}",
        "nodo",
        "estado",
        "filas",
        "filas/s",
        "tiempo",
        "ocupado",
        width = width
    );

    // El que más tiempo pasa trabajando sin esperar a nadie es el que marca
    // el ritmo. Se mira en absoluto y no en porcentaje: un nodo que vive
    // 3 ms sin esperar da 100% y no es el cuello de botella de nada.
    let bottleneck = nodes
        .iter()
        .filter(|n| n.busy_ms > 0)
        .max_by_key(|n| n.busy_ms)
        .map(|n| n.node.clone());

    for node in nodes {
        let moved = if node.kind == "sink" {
            node.rows_in
        } else {
            node.rows_out
        };
        let marker = if Some(&node.node) == bottleneck.as_ref() {
            " ←"
        } else {
            ""
        };
        println!(
            "  {:<width$}  {:<9}  {:>12}  {:>14}  {:>10}  {:>10}{}",
            node.node,
            translate_status(&node.status),
            thousands(moved),
            thousands(node.rows_per_second as u64),
            human_ms(node.elapsed_ms),
            format!("{} ({:.0}%)", human_ms(node.busy_ms), node.busy_pct),
            marker,
            width = width
        );
    }
    if let Some(node) = &bottleneck {
        println!();
        println!("  ← `{node}` es el que marca el ritmo: el resto le espera");
    }

    let failures: Vec<_> = nodes.iter().filter(|n| n.error.is_some()).collect();
    if !failures.is_empty() {
        println!();
        for node in failures {
            println!(
                "  {}: {}",
                node.node,
                node.error.as_deref().unwrap_or_default()
            );
        }
    }

    if !events.is_empty() {
        println!();
        println!("eventos:");
        for event in events {
            println!(
                "  {}  {:<14} {}",
                event.at.format("%H:%M:%S%.3f"),
                event.kind,
                event.detail.as_deref().unwrap_or("")
            );
        }
    }
}

fn short_id(run_id: &str) -> &str {
    &run_id[..run_id.len().min(8)]
}

fn translate_status(status: &str) -> &str {
    match status {
        "succeeded" => "correcto",
        "failed" => "fallido",
        "skipped" => "omitido",
        "running" => "en curso",
        other => other,
    }
}

fn human_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms} ms")
    } else if ms < 60_000 {
        format!("{:.2} s", ms as f64 / 1_000.0)
    } else {
        format!("{}m {:02}s", ms / 60_000, (ms % 60_000) / 1_000)
    }
}

fn rate(rows: u64, ms: u64) -> String {
    if ms == 0 || rows == 0 {
        return "—".to_string();
    }
    thousands((rows as f64 / (ms as f64 / 1_000.0)) as u64)
}

/// Separa millares con espacio fino para que las cifras grandes se lean de un
/// vistazo sin depender de la localización del sistema.
fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(' ');
        }
        out.push(ch);
    }
    out
}

/// La config que acepta un componente, para quien escribe el YAML a mano.
///
/// Sale del mismo esquema que alimenta al inspector de la ventana, que a su
/// vez sale del struct que deserializa la config: los tres dicen lo mismo
/// porque los tres son la misma definición.
pub fn print_component(registry: &Registry, name: &str) -> bool {
    let kinds = [
        ("source", registry.source_names()),
        ("transform", registry.transform_names()),
        ("sink", registry.sink_names()),
    ];

    let mut encontrado = false;
    for (kind, names) in kinds {
        if !names.contains(&name) {
            continue;
        }
        let Some(schema) = registry.schema(kind, name) else {
            continue;
        };
        encontrado = true;
        println!();
        println!("{kind} `{name}`");

        let required: Vec<&str> = schema
            .get("required")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let Some(props) = schema.get("properties").and_then(|v| v.as_object()) else {
            println!("  (sin configuración)");
            continue;
        };
        if props.is_empty() {
            println!("  (sin configuración)");
            continue;
        }

        let ancho = props.keys().map(String::len).max().unwrap_or(4);
        for (campo, def) in props {
            let obligatorio = required.contains(&campo.as_str());
            let nota = if obligatorio {
                "obligatorio".to_string()
            } else {
                match def.get("default") {
                    Some(serde_json::Value::Null) | None => "opcional".to_string(),
                    Some(valor) => format!("por defecto: {valor}"),
                }
            };
            println!("  {campo:<ancho$}  {:<9}  {nota}", tipo(def));
            // La descripción viene de los comentarios `///` del struct.
            if let Some(texto) = def.get("description").and_then(|v| v.as_str()) {
                for linea in texto.lines() {
                    println!("  {:ancho$}  {linea}", "");
                }
            }
        }
    }
    encontrado
}

/// Nombre legible del tipo JSON Schema.
fn tipo(def: &serde_json::Value) -> String {
    let bruto = match def.get("type") {
        // Un campo opcional sale como `["integer", "null"]`: interesa el que
        // no es null.
        Some(serde_json::Value::Array(tipos)) => tipos
            .iter()
            .filter_map(|t| t.as_str())
            .find(|t| *t != "null")
            .unwrap_or("?"),
        Some(serde_json::Value::String(t)) => t.as_str(),
        _ if def.get("enum").is_some() || def.get("oneOf").is_some() => "enum",
        _ => "?",
    };
    match bruto {
        "string" => "texto",
        "integer" => "entero",
        "number" => "número",
        "boolean" => "booleano",
        "array" => "lista",
        "object" => "mapa",
        otro => otro,
    }
    .to_string()
}
