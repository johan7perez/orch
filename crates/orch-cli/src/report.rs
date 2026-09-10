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
