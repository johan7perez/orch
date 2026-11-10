//! Lo que cada cambio significa sobre el modelo.
//!
//! Es la mitad de la comprobación: el texto editado tiene que leerse
//! exactamente como lo que sale de aquí. También es donde se rechazan los
//! cambios sin sentido, antes de tocar una sola línea.

use std::collections::HashSet;

use orch_core::{EdgeSpec, NodeKind, NodeSpec, PipelineSpec, RetryPolicy};
use serde_json::{Map, Value};

use crate::{Edit, EditError, Kind};

/// Operaciones que leen el nombre del puerto de sus entradas.
///
/// Un `sql` registra cada entrada como una tabla con ese nombre (ver
/// `orch-sql`). Las demás concatenan o generan su propia query y lo ignoran.
const LEEN_PUERTOS: &[&str] = &["sql"];

pub(crate) fn aplicar(antes: &PipelineSpec, edit: &Edit) -> Result<PipelineSpec, EditError> {
    let mut spec = antes.clone();
    match edit {
        Edit::AddNode {
            id,
            kind,
            component,
            config,
        } => {
            id_nuevo(&spec, id)?;
            if component.trim().is_empty() {
                return Err(EditError::Invalid(
                    "falta el componente del nodo nuevo".into(),
                ));
            }
            let config = if config.is_empty() {
                Value::Null
            } else {
                Value::Object(config.clone())
            };
            let component = component.clone();
            let kind = match kind {
                Kind::Source => NodeKind::Source {
                    connector: component,
                    config,
                },
                Kind::Transform => NodeKind::Transform {
                    op: component,
                    config,
                },
                Kind::Sink => NodeKind::Sink {
                    connector: component,
                    config,
                },
            };
            spec.nodes.push(NodeSpec {
                id: id.clone(),
                after: Vec::new(),
                retry: RetryPolicy::default(),
                worker: None,
                kind,
            });
        }

        Edit::RemoveNode { id } => {
            let i = indice(&spec, id)?;
            spec.nodes.remove(i);
            spec.edges.retain(|e| e.from != *id && e.to != *id);
            for nodo in &mut spec.nodes {
                nodo.after.retain(|previo| previo != id);
            }
            spec.layout.remove(id);
        }

        Edit::Connect { from, to, port } => {
            indice(&spec, from)?;
            indice(&spec, to)?;
            // Lo demás —ciclos, un sink con salidas, puertos repetidos— es
            // cosa de la validación, que lo enseña sobre el lienzo. Esto no:
            // dos aristas iguales se dibujan una encima de otra y no habría
            // forma de saber cuál se quiere quitar.
            if spec.edges.iter().any(|e| e.from == *from && e.to == *to) {
                return Err(EditError::Invalid(format!(
                    "ya hay una arista de `{from}` a `{to}`"
                )));
            }
            if let Some(port) = port {
                comprobar_nombre(port, "nombre de puerto")?;
            }
            spec.edges.push(EdgeSpec {
                from: from.clone(),
                to: to.clone(),
                port: port.clone(),
            });
        }

        Edit::Disconnect { from, to } => {
            let antes = spec.edges.len();
            spec.edges.retain(|e| !(e.from == *from && e.to == *to));
            if spec.edges.len() == antes {
                return Err(EditError::Invalid(format!(
                    "no hay ninguna arista de `{from}` a `{to}`"
                )));
            }
        }

        Edit::SetConfig { node, key, value } => {
            let i = indice(&spec, node)?;
            if key.trim().is_empty() {
                return Err(EditError::Invalid("falta el nombre del campo".into()));
            }
            let config = config_mut(&mut spec.nodes[i].kind);
            let mut mapa = match std::mem::take(config) {
                Value::Null => Map::new(),
                Value::Object(mapa) => mapa,
                _ => {
                    return Err(EditError::Unsupported(format!(
                        "la config de `{node}` no es un mapa"
                    )))
                }
            };
            match value {
                Some(valor) => {
                    mapa.insert(key.clone(), valor.clone());
                }
                None => {
                    mapa.remove(key);
                }
            }
            *config = if mapa.is_empty() {
                Value::Null
            } else {
                Value::Object(mapa)
            };
        }

        Edit::SetAfter { node, after } => {
            let i = indice(&spec, node)?;
            let mut lista: Vec<String> = Vec::with_capacity(after.len());
            for previo in after {
                indice(&spec, previo)?;
                if previo == node {
                    return Err(EditError::Invalid(format!(
                        "`{node}` no puede esperarse a sí mismo"
                    )));
                }
                if !lista.contains(previo) {
                    lista.push(previo.clone());
                }
            }
            spec.nodes[i].after = lista;
        }

        Edit::RenameNode { from, to } => {
            let i = indice(&spec, from)?;
            if from == to {
                return Ok(spec);
            }
            id_nuevo(&spec, to)?;

            let leen: HashSet<String> = spec
                .nodes
                .iter()
                .filter(|n| lee_puertos(&spec, &n.id))
                .map(|n| n.id.clone())
                .collect();

            spec.nodes[i].id = to.clone();
            for arista in &mut spec.edges {
                if arista.from == *from {
                    // El puerto se llamaba como el nodo. Si el destino es un
                    // `sql`, ese nombre es una tabla de su query: se fija para
                    // que la query siga funcionando. Es lo mismo que hace el
                    // pushdown cuando absorbe un nodo.
                    if arista.port.is_none() && leen.contains(&arista.to) {
                        arista.port = Some(from.clone());
                    }
                    arista.from = to.clone();
                }
                if arista.to == *from {
                    arista.to = to.clone();
                }
            }
            for nodo in &mut spec.nodes {
                for previo in &mut nodo.after {
                    if previo == from {
                        *previo = to.clone();
                    }
                }
            }
            if let Some(punto) = spec.layout.remove(from) {
                spec.layout.insert(to.clone(), punto);
            }
        }

        Edit::SetLayout { positions } => {
            for (id, [x, y]) in positions {
                indice(&spec, id)?;
                if !(x.is_finite() && y.is_finite()) {
                    return Err(EditError::Invalid(format!(
                        "la posición de `{id}` no es un número"
                    )));
                }
                spec.layout.insert(id.clone(), [x.round(), y.round()]);
            }
        }

        Edit::ClearLayout => spec.layout.clear(),
    }
    Ok(spec)
}

/// Si el nodo `id` usa el nombre del puerto de sus entradas.
pub(crate) fn lee_puertos(spec: &PipelineSpec, id: &str) -> bool {
    spec.nodes.iter().any(|n| {
        n.id == id
            && matches!(&n.kind, NodeKind::Transform { op, .. } if LEEN_PUERTOS.contains(&op.as_str()))
    })
}

pub(crate) fn indice(spec: &PipelineSpec, id: &str) -> Result<usize, EditError> {
    spec.nodes
        .iter()
        .position(|n| n.id == id)
        .ok_or_else(|| EditError::Invalid(format!("no hay ningún nodo `{id}`")))
}

fn id_nuevo(spec: &PipelineSpec, id: &str) -> Result<(), EditError> {
    comprobar_nombre(id, "id de nodo")?;
    if spec.nodes.iter().any(|n| n.id == id) {
        return Err(EditError::Invalid(format!("ya hay un nodo `{id}`")));
    }
    Ok(())
}

/// Los nombres nuevos se limitan a lo que funciona como tabla en SQL sin
/// comillas.
///
/// El puerto de una arista se llama, por defecto, como su nodo de origen, y
/// un `sql` lo usa como nombre de tabla. Un guion obligaría a entrecomillarlo
/// en la query, y DataFusion pasa a minúsculas lo que no va entre comillas:
/// un nodo `LeerClientes` no se encontraría escribiendo `FROM LeerClientes`.
/// Los ids que ya están en el fichero no se tocan; esto sólo vale para los
/// que crea el lienzo.
fn comprobar_nombre(nombre: &str, que: &str) -> Result<(), EditError> {
    let mut letras = nombre.chars();
    let valido = matches!(letras.next(), Some(c) if c.is_ascii_lowercase() || c == '_')
        && letras.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valido {
        Ok(())
    } else {
        Err(EditError::Invalid(format!(
            "`{nombre}` no vale como {que}: usa minúsculas, números y `_`, empezando por \
             letra o `_` (se usa como nombre de tabla en SQL)"
        )))
    }
}

fn config_mut(kind: &mut NodeKind) -> &mut Value {
    match kind {
        NodeKind::Source { config, .. }
        | NodeKind::Transform { config, .. }
        | NodeKind::Sink { config, .. } => config,
    }
}
