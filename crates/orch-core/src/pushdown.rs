//! Empuje de operaciones hacia el conector de origen.
//!
//! Un `filter` justo después de un origen tiene que leerlo todo para tirar la
//! mayor parte. Si el origen sabe hacerlo él —un `WHERE` que resuelve
//! PostgreSQL, unas columnas que Parquet ni descomprime—, el trabajo se hace
//! donde están los datos y por el canal viaja mucho menos.
//!
//! Esto es lo que un planificador global habría dado «gratis», y que en la
//! Fase 0.2 se midió que **no** valía la pena por la fusión entre
//! transformaciones: ahí no se ganaba nada. La ganancia real estaba aquí, en
//! el salto hasta el origen, y se consigue sin atar el motor a DataFusion.
//!
//! La reescritura se hace sobre el [`PipelineSpec`], antes de construir el
//! DAG: es un paso aparte, legible y con sus propios tests, y el ejecutor no
//! se entera.

use serde_json::Value;

use crate::registry::Registry;
use crate::spec::{NodeKind, PipelineSpec};

/// Una operación que un origen puede absorber.
#[derive(Debug, Clone)]
pub enum PushdownOp {
    /// Leer sólo estas columnas, en este orden.
    Select { columns: Vec<String> },
    /// Quedarse con las filas que cumplan el predicado SQL.
    Filter { predicate: String },
}

impl PushdownOp {
    pub fn label(&self) -> &'static str {
        match self {
            PushdownOp::Select { .. } => "select",
            PushdownOp::Filter { .. } => "filter",
        }
    }

    /// Extrae la operación de un nodo de transformación, si es de las que se
    /// pueden empujar.
    fn from_node(kind: &NodeKind) -> Option<Self> {
        let NodeKind::Transform { op, config } = kind else {
            return None;
        };
        match op.as_str() {
            "select" => {
                let columns = config.get("columns")?.as_array()?;
                // Cualquier cosa rara en la config y no se toca: ya fallará
                // en `validate` con su propio mensaje.
                let columns: Option<Vec<String>> = columns
                    .iter()
                    .map(|c| c.as_str().map(str::to_string))
                    .collect();
                Some(PushdownOp::Select { columns: columns? })
            }
            "filter" => {
                // Un `filter` con `table` o `memory_limit_mb` propios no es
                // un filtro corriente; se deja donde está.
                if config.get("table").is_some() || config.get("memory_limit_mb").is_some() {
                    return None;
                }
                Some(PushdownOp::Filter {
                    predicate: config.get("where")?.as_str()?.to_string(),
                })
            }
            _ => None,
        }
    }
}

/// Lo que un conector hace con una operación que se le ofrece.
///
/// Devuelve `true` si la absorbió, y en ese caso ya habrá modificado su
/// propia config.
pub type PushdownHandler = Box<dyn Fn(&mut Value, &PushdownOp) -> bool + Send + Sync>;

/// Una operación efectivamente empujada, para poder contarlo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pushed {
    /// Nodo de origen que la absorbió.
    pub into: String,
    /// Nodo de transformación que desapareció.
    pub removed: String,
    pub op: &'static str,
}

/// Empuja hacia los orígenes todo lo que se pueda y elimina los nodos que
/// sobran.
///
/// Sólo se empuja cuando el origen tiene **un único consumidor**: si tuviera
/// más, recortar sus columnas o sus filas cambiaría lo que ven los otros.
pub fn apply(spec: &mut PipelineSpec, registry: &Registry) -> Vec<Pushed> {
    let mut pushed = Vec::new();

    // Un solo paso puede habilitar el siguiente: al absorber un `select`, el
    // `filter` que venía detrás pasa a estar pegado al origen.
    while let Some(applied) = apply_once(spec, registry) {
        pushed.push(applied);
    }

    pushed
}

fn apply_once(spec: &mut PipelineSpec, registry: &Registry) -> Option<Pushed> {
    // Los candidatos se recogen primero, sin retener préstamos sobre `spec`:
    // absorber una operación modifica el nodo de origen y elimina otro.
    let candidates: Vec<(usize, String, PushdownOp)> = spec
        .nodes
        .iter()
        .enumerate()
        // Una barrera es una dependencia explícita del usuario: si el nodo
        // desaparece, se perdería.
        .filter(|(_, node)| node.after.is_empty())
        .filter_map(|(index, node)| {
            PushdownOp::from_node(&node.kind).map(|op| (index, node.id.clone(), op))
        })
        .collect();

    for (index, removed_id, op) in candidates {
        // Tiene que tener exactamente una entrada, y venir de un origen.
        let incoming: Vec<&str> = spec
            .edges
            .iter()
            .filter(|e| e.to == removed_id)
            .map(|e| e.from.as_str())
            .collect();
        let [source_id] = incoming.as_slice() else {
            continue;
        };
        let source_id = source_id.to_string();

        let Some(source_index) = spec.nodes.iter().position(|n| n.id == source_id) else {
            continue;
        };
        let NodeKind::Source { connector, .. } = &spec.nodes[source_index].kind else {
            continue;
        };
        let connector = connector.clone();

        // El origen no puede alimentar a nadie más: recortarle columnas o
        // filas cambiaría lo que ven los demás.
        if spec.edges.iter().filter(|e| e.from == source_id).count() != 1 {
            continue;
        }
        // Y nadie puede depender por `after` del nodo que va a desaparecer.
        if spec.nodes.iter().any(|n| n.after.contains(&removed_id)) {
            continue;
        }

        let Some(handler) = registry.pushdown_handler(&connector) else {
            continue;
        };

        let NodeKind::Source { config, .. } = &mut spec.nodes[source_index].kind else {
            continue;
        };
        // Se trabaja sobre una copia: si el conector la rechaza a medio
        // camino, la config original queda intacta.
        let mut candidate = config.clone();
        if !handler(&mut candidate, &op) {
            continue;
        }
        *config = candidate;

        // El nodo desaparece y el origen pasa a hablar con sus consumidores.
        spec.nodes.remove(index);
        spec.edges.retain(|e| e.to != removed_id);
        for edge in &mut spec.edges {
            if edge.from == removed_id {
                edge.from = source_id.clone();
                // El puerto llevaba el nombre del nodo eliminado.
                if edge.port.is_none() {
                    edge.port = Some(removed_id.clone());
                }
            }
        }

        return Some(Pushed {
            into: source_id,
            removed: removed_id,
            op: op.label(),
        });
    }

    None
}
