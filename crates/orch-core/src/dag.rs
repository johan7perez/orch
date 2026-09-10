//! Validación del pipeline y construcción del grafo ejecutable.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::{OrchError, Result};
use crate::spec::{NodeId, NodeSpec, PipelineSpec, SPEC_VERSION};

/// Un [`PipelineSpec`] validado, con las adyacencias ya resueltas a índices.
#[derive(Debug, Clone)]
pub struct Dag {
    spec: PipelineSpec,
    index: HashMap<NodeId, usize>,
    /// Aristas de datos entrantes, en el orden en que se declararon.
    upstream: Vec<Vec<usize>>,
    /// Aristas de datos salientes.
    downstream: Vec<Vec<usize>>,
    /// Dependencias de orden puro (`after`).
    barriers: Vec<Vec<usize>>,
    /// Orden topológico sobre datos + barreras.
    order: Vec<usize>,
}

impl Dag {
    pub fn build(spec: PipelineSpec) -> Result<Self> {
        if spec.version != SPEC_VERSION {
            return Err(OrchError::Validation(format!(
                "versión de formato {} no soportada (esperada {SPEC_VERSION})",
                spec.version
            )));
        }
        if spec.name.trim().is_empty() {
            return Err(OrchError::Validation("`name` no puede estar vacío".into()));
        }
        if spec.nodes.is_empty() {
            return Err(OrchError::Validation("el pipeline no tiene nodos".into()));
        }
        if spec.settings.batch_size == 0 {
            return Err(OrchError::Validation(
                "`settings.batch_size` debe ser mayor que 0".into(),
            ));
        }
        if spec.settings.channel_capacity == 0 {
            return Err(OrchError::Validation(
                "`settings.channel_capacity` debe ser mayor que 0".into(),
            ));
        }

        let mut index = HashMap::with_capacity(spec.nodes.len());
        for (i, node) in spec.nodes.iter().enumerate() {
            if node.id.trim().is_empty() {
                return Err(OrchError::Validation(format!(
                    "el nodo en la posición {i} no tiene `id`"
                )));
            }
            if node.retry.max_attempts == 0 {
                return Err(OrchError::Validation(format!(
                    "nodo `{}`: `retry.max_attempts` debe ser al menos 1",
                    node.id
                )));
            }
            if index.insert(node.id.clone(), i).is_some() {
                return Err(OrchError::Validation(format!(
                    "el id de nodo `{}` está duplicado",
                    node.id
                )));
            }
        }

        let n = spec.nodes.len();
        let mut upstream = vec![Vec::new(); n];
        let mut downstream = vec![Vec::new(); n];
        let mut seen_edges = HashSet::new();

        for edge in &spec.edges {
            let from = *index.get(&edge.from).ok_or_else(|| {
                OrchError::Validation(format!(
                    "la arista referencia un nodo inexistente: `{}`",
                    edge.from
                ))
            })?;
            let to = *index.get(&edge.to).ok_or_else(|| {
                OrchError::Validation(format!(
                    "la arista referencia un nodo inexistente: `{}`",
                    edge.to
                ))
            })?;
            if from == to {
                return Err(OrchError::Validation(format!(
                    "el nodo `{}` tiene una arista hacia sí mismo",
                    edge.from
                )));
            }
            if !seen_edges.insert((from, to)) {
                return Err(OrchError::Validation(format!(
                    "arista duplicada `{}` -> `{}`",
                    edge.from, edge.to
                )));
            }
            if !spec.nodes[from].kind.produces_output() {
                return Err(OrchError::Validation(format!(
                    "`{}` es un sink y no puede tener salidas de datos",
                    edge.from
                )));
            }
            if !spec.nodes[to].kind.accepts_input() {
                return Err(OrchError::Validation(format!(
                    "`{}` es un source y no puede tener entradas de datos",
                    edge.to
                )));
            }
            downstream[from].push(to);
            upstream[to].push(from);
        }

        let mut barriers = vec![Vec::new(); n];
        for (i, node) in spec.nodes.iter().enumerate() {
            for dep in &node.after {
                let d = *index.get(dep).ok_or_else(|| {
                    OrchError::Validation(format!(
                        "nodo `{}`: `after` referencia un nodo inexistente: `{dep}`",
                        node.id
                    ))
                })?;
                if d == i {
                    return Err(OrchError::Validation(format!(
                        "nodo `{}`: no puede depender de sí mismo en `after`",
                        node.id
                    )));
                }
                if !barriers[i].contains(&d) {
                    barriers[i].push(d);
                }
            }
        }

        // Cada nodo debe estar cableado de forma coherente con su tipo.
        for (i, node) in spec.nodes.iter().enumerate() {
            if node.kind.accepts_input() && upstream[i].is_empty() {
                return Err(OrchError::Validation(format!(
                    "`{}` es un {} y necesita al menos una entrada de datos",
                    node.id,
                    node.kind.label()
                )));
            }
            if node.kind.produces_output() && downstream[i].is_empty() {
                return Err(OrchError::Validation(format!(
                    "`{}` es un {} y sus datos no van a ninguna parte; conéctalo o quítalo",
                    node.id,
                    node.kind.label()
                )));
            }
        }

        let order = topological_order(&spec.nodes, &downstream, &barriers)?;

        Ok(Self {
            spec,
            index,
            upstream,
            downstream,
            barriers,
            order,
        })
    }

    pub fn spec(&self) -> &PipelineSpec {
        &self.spec
    }

    pub fn nodes(&self) -> &[NodeSpec] {
        &self.spec.nodes
    }

    pub fn len(&self) -> usize {
        self.spec.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spec.nodes.is_empty()
    }

    pub fn node(&self, i: usize) -> &NodeSpec {
        &self.spec.nodes[i]
    }

    pub fn index_of(&self, id: &str) -> Option<usize> {
        self.index.get(id).copied()
    }

    pub fn upstream(&self, i: usize) -> &[usize] {
        &self.upstream[i]
    }

    pub fn downstream(&self, i: usize) -> &[usize] {
        &self.downstream[i]
    }

    pub fn barriers(&self, i: usize) -> &[usize] {
        &self.barriers[i]
    }

    /// Índices de nodo en orden topológico. Sólo informativo: el ejecutor
    /// arranca todos los nodos a la vez y deja que los canales impongan el orden.
    pub fn topological_order(&self) -> &[usize] {
        &self.order
    }
}

fn topological_order(
    nodes: &[NodeSpec],
    downstream: &[Vec<usize>],
    barriers: &[Vec<usize>],
) -> Result<Vec<usize>> {
    let n = nodes.len();
    let mut succ: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut in_degree = vec![0usize; n];

    for (from, tos) in downstream.iter().enumerate() {
        for &to in tos {
            succ[from].push(to);
            in_degree[to] += 1;
        }
    }
    for (to, deps) in barriers.iter().enumerate() {
        for &from in deps {
            // Una barrera puede duplicar una arista de datos ya existente; en
            // ese caso no vuelve a contar para el grado de entrada.
            if !downstream[from].contains(&to) {
                succ[from].push(to);
                in_degree[to] += 1;
            }
        }
    }

    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    while let Some(i) = queue.pop_front() {
        order.push(i);
        for &j in &succ[i] {
            in_degree[j] -= 1;
            if in_degree[j] == 0 {
                queue.push_back(j);
            }
        }
    }

    if order.len() != n {
        let cycle: Vec<&str> = (0..n)
            .filter(|&i| in_degree[i] > 0)
            .map(|i| nodes[i].id.as_str())
            .collect();
        return Err(OrchError::Validation(format!(
            "el pipeline tiene un ciclo entre los nodos: {}",
            cycle.join(", ")
        )));
    }

    Ok(order)
}
